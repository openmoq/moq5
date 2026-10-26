#if canImport(AVFoundation) && canImport(CoreMedia)

import AVFoundation
import CoreMedia
import MoQService
#if canImport(AVFAudio) && (os(iOS) || os(tvOS))
import AVFAudio
#endif

/// A renderer a sample is enqueued into.
@MainActor
protocol SampleRenderer: AnyObject {
    var isReadyForMoreMediaData: Bool { get }
    /// The renderer can no longer decode (a bad unit or configuration).
    var hasFailed: Bool { get }
    func enqueue(_ sample: CMSampleBuffer)
    func flush()
}

/// What enqueueing one sample came to.
enum EnqueueOutcome: Equatable {
    case enqueued
    /// The renderer failed, before or on this sample; it is not received.
    case failed
    case cancelled
}

/// Enqueue `sample` once `renderer` has room. Waiting here is the player's
/// backpressure: no further output is pulled until this returns. A renderer
/// that fails while it is waited on ends the wait: a failed renderer never
/// becomes ready again by itself.
@MainActor
func enqueueWhenReady(_ sample: CMSampleBuffer, into renderer: SampleRenderer) async
    -> EnqueueOutcome {
    while true {
        if renderer.hasFailed { return .failed }
        if Task.isCancelled { return .cancelled }
        if renderer.isReadyForMoreMediaData {
            renderer.enqueue(sample)
            return renderer.hasFailed ? .failed : .enqueued
        }
        try? await Task.sleep(for: .milliseconds(5))
    }
}

/// How waiting for a finite presentation to finish came out.
enum SettleOutcome: Equatable {
    /// The clock reached the end: everything enqueued has presented.
    case presented
    /// A renderer failed before the end presented.
    case failed
    /// The clock did not reach the end: it never started, stopped, or did
    /// not advance by the time the end was due.
    case notPresented
    case cancelled
}

/// The presentation clock a settlement watches.
@MainActor
protocol PresentationClock: AnyObject {
    /// The clock's media time, or nil when no timeline drives it.
    var mediaTime: Duration? { get }
    /// The host time (monotonic microseconds since boot) at which media
    /// time `end` presents on the clock's mapping; nil when no timeline
    /// drives it or its rate is not positive.
    func hostTime(presenting end: Duration) -> UInt64?
}

/// Wait until media time `end` has presented. It has once its host time on
/// the mapping has come and the clock reads `end` or later: a clock reading
/// alone is not enough, since a mapping just set takes effect on the
/// timebase later, and until then the clock still reads its old time. The
/// wait is bounded: it lasts until that host time plus `grace` for the
/// timebase to catch up. A renderer that fails meanwhile, or cancellation,
/// ends it first.
@MainActor
func settlePresentation(through end: Duration, clock: PresentationClock,
                        renderers: [SampleRenderer], grace: Duration = .milliseconds(250),
                        hostNow: () -> UInt64 = { DispatchTime.now().uptimeNanoseconds / 1_000 })
    async -> SettleOutcome {
    guard let due = clock.hostTime(presenting: end) else { return .notPresented }
    let graceUS = UInt64(grace.components.seconds) * 1_000_000 +
                  UInt64(grace.components.attoseconds / 1_000_000_000_000)
    let deadline = due > UInt64.max - graceUS ? UInt64.max : due + graceUS
    while true {
        if Task.isCancelled { return .cancelled }
        if renderers.contains(where: { $0.hasFailed }) { return .failed }
        let host = hostNow()
        if host >= due, let now = clock.mediaTime, now >= end { return .presented }
        if host >= deadline { return .notPresented }
        /* Sleep toward the end's due time, in steps short enough to notice
         * a failure or a cancellation promptly. */
        let wait = min(deadline - host, due > host ? due - host : 10_000, 10_000)
        try? await Task.sleep(for: .microseconds(Int64(min(wait, UInt64(Int64.max)))))
    }
}

/// Where the player's outputs are rendered.
@MainActor
protocol RenderSink: AnyObject {
    /// The renderer for a track kind; nil for audio until it is attached.
    func renderer(video: Bool) -> SampleRenderer?
    func attachAudio()
    /// Start the clock on a timeline's mapping; one timeline drives it.
    func apply(_ timeline: WatchTimeline)
    /// Whether `timelineID` is the timeline driving the clock.
    func drives(_ timelineID: UInt32) -> Bool
    /// Wait until media time `end` has been presented (settlePresentation).
    func settle(through end: Duration) async -> SettleOutcome
    func stop()
}

/// SimplePlayer's audio/video render layer, clocked by the media player.
///
/// One `AVSampleBufferRenderSynchronizer` owns the timebase shared by the
/// video display layer and, when the stream carries audio, an audio
/// renderer. The player decides *when* media presents: its `TIMELINE`
/// output maps media time onto the host clock (`anchorMedia` presents at
/// `anchorLocalMicroseconds`, at `rate`), and this pipeline hands that
/// mapping to the synchronizer, so every sample presents at its own
/// presentation time on that mapping. The player's cushion is the startup
/// buffer: the first unit is released ahead of its presentation time.
///
/// Running the shared timebase is also what keeps the display layer from
/// freezing on its first frame: its own control timebase starts stopped.
@available(iOS 16.0, macOS 13.0, tvOS 16.0, *)
@MainActor
final class RenderPipeline: RenderSink, PresentationClock {

    let displayLayer: AVSampleBufferDisplayLayer
    private let synchronizer = AVSampleBufferRenderSynchronizer()
    private let video: VideoRenderer
    private var audio: AudioRenderer?
    /// The timeline driving the synchronizer, and its mapping. The
    /// synchronizer has one clock, so a second, independent timeline is not
    /// applied.
    private var timelineID: UInt32?
    private var timeline: WatchTimeline?

    init(displayLayer: AVSampleBufferDisplayLayer) {
        self.displayLayer = displayLayer
        self.video = VideoRenderer(layer: displayLayer)
        // Clear any frame left over from a prior session before this one's
        // timebase takes over.
        if #available(iOS 17.0, macOS 14.0, tvOS 17.0, *) {
            displayLayer.sampleBufferRenderer.flush(
                removingDisplayedImage: true, completionHandler: nil)
        } else {
            displayLayer.flushAndRemoveImage()
        }
        /* The player decides when media presents: its TIMELINE, released a
         * cushion ahead, starts the clock. Renderer preroll must not hold
         * that start back: a finite stream's last units can leave a renderer
         * below its preroll with no more media coming. */
        synchronizer.delaysRateChangeUntilHasSufficientMediaData = false
        synchronizer.addRenderer(video.rendering)
    }

    func renderer(video isVideo: Bool) -> SampleRenderer? {
        isVideo ? video : audio
    }

    /// Attach an audio renderer; the synchronizer then follows the audio
    /// device's clock while keeping the player's mapping.
    func attachAudio() {
        guard audio == nil else { return }
        configureAudioSession()
        let renderer = AudioRenderer()
        audio = renderer
        synchronizer.addRenderer(renderer.renderer)
    }

    /// Start the clock on the player's mapping: at host time
    /// `anchorLocalMicroseconds` the timebase reads `anchorMedia`. The
    /// player's clock is the host clock (monotonic microseconds since boot).
    func apply(_ timeline: WatchTimeline) {
        guard timelineID == nil || timelineID == timeline.timelineID else { return }
        timelineID = timeline.timelineID
        self.timeline = timeline
        synchronizer.setRate(
            Float(timeline.rate),
            time: cmTime(timeline.anchorMedia),
            atHostTime: CMTime(value: Int64(timeline.anchorLocalMicroseconds),
                               timescale: 1_000_000))
    }

    func drives(_ timelineID: UInt32) -> Bool { self.timelineID == timelineID }

    func settle(through end: Duration) async -> SettleOutcome {
        var renderers: [SampleRenderer] = [video]
        if let audio { renderers.append(audio) }
        return await settlePresentation(through: end, clock: self, renderers: renderers)
    }

    var mediaTime: Duration? {
        guard timeline != nil else { return nil }
        let t = synchronizer.currentTime()
        guard t.isNumeric else { return nil }
        return .microseconds(CMTimeConvertScale(t, timescale: 1_000_000,
                                                method: .roundTowardNegativeInfinity).value)
    }

    func hostTime(presenting end: Duration) -> UInt64? {
        guard let timeline, timeline.rate > 0 else { return nil }
        let offset = Double((end - timeline.anchorMedia) / .microseconds(1)) / timeline.rate
        let host = Double(timeline.anchorLocalMicroseconds) + offset
        return host <= 0 ? 0 : UInt64(host)
    }

    /// Stop the clock and detach renderers so the shared display layer is free
    /// to join a fresh pipeline on the next connection.
    func stop() {
        synchronizer.setRate(0, time: .zero)
        video.flush()
        audio?.flush()
        synchronizer.removeRenderer(video.rendering, at: .invalid)
        if let audio {
            synchronizer.removeRenderer(audio.renderer, at: .invalid)
        }
    }

    private func cmTime(_ duration: Duration) -> CMTime {
        let (seconds, attoseconds) = duration.components
        return CMTime(value: seconds * 1_000_000 + attoseconds / 1_000_000_000_000,
                      timescale: 1_000_000)
    }

    private func configureAudioSession() {
        #if canImport(AVFAudio) && (os(iOS) || os(tvOS))
        // Without an active .playback session, iOS routes sample-renderer audio
        // nowhere — silent even when the buffers decode cleanly.
        let session = AVAudioSession.sharedInstance()
        try? session.setCategory(.playback, mode: .moviePlayback)
        try? session.setActive(true)
        #endif
    }
}

/// The display layer's renderer. iOS 17 / macOS 14 moved queued rendering
/// off the layer onto `sampleBufferRenderer`; gated to avoid the deprecated
/// path while still supporting the iOS 16 / macOS 13 floor.
@available(iOS 16.0, macOS 13.0, tvOS 16.0, *)
@MainActor
final class VideoRenderer: SampleRenderer {
    private let layer: AVSampleBufferDisplayLayer
    init(layer: AVSampleBufferDisplayLayer) { self.layer = layer }

    var rendering: AVQueuedSampleBufferRendering {
        if #available(iOS 17.0, macOS 14.0, tvOS 17.0, *) {
            return layer.sampleBufferRenderer
        }
        return layer
    }
    var isReadyForMoreMediaData: Bool { rendering.isReadyForMoreMediaData }
    var hasFailed: Bool {
        if #available(iOS 17.0, macOS 14.0, tvOS 17.0, *) {
            return layer.sampleBufferRenderer.status == .failed
        }
        return layer.status == .failed
    }
    func enqueue(_ sample: CMSampleBuffer) { rendering.enqueue(sample) }
    func flush() { rendering.flush() }
}

@available(iOS 16.0, macOS 13.0, tvOS 16.0, *)
@MainActor
final class AudioRenderer: SampleRenderer {
    let renderer = AVSampleBufferAudioRenderer()
    var isReadyForMoreMediaData: Bool { renderer.isReadyForMoreMediaData }
    var hasFailed: Bool { renderer.status == .failed }
    func enqueue(_ sample: CMSampleBuffer) { renderer.enqueue(sample) }
    func flush() { renderer.flush() }
}

#endif  /* canImport(AVFoundation) && canImport(CoreMedia) */
