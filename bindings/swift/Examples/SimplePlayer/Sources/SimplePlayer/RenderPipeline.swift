#if canImport(AVFoundation) && canImport(CoreMedia)

import AVFoundation
import CoreMedia
#if canImport(AVFAudio) && (os(iOS) || os(tvOS))
import AVFAudio
#endif

/// SimplePlayer's audio/video render + sync layer.
///
/// One `AVSampleBufferRenderSynchronizer` owns a single timebase shared by the
/// video display layer and — when the stream carries audio — an audio renderer:
///
///  - **Audio present → video is timed to audio.** Once an audio renderer is
///    attached, the synchronizer slaves its clock to the audio hardware, so
///    video presents against the audio timeline (the correct master for A/V).
///  - **No audio → video is timed independently.** The timebase runs on the
///    host clock, started at the first frame's PTS, so video plays paced by its
///    own stream timeline.
///
/// Running the shared timebase is also the proper cure for the "frozen on the
/// first frame" trap: `AVSampleBufferDisplayLayer`'s own control timebase starts
/// stopped, so a bare enqueue never advances — the synchronizer drives it.
///
/// Nothing here is hardcoded to a fixed track: renderers are keyed by media
/// role, each sample buffer carries its own per-object format description, and
/// the source video track can change without rebuilding the pipeline — the shape
/// a later ABR rendition switch will need.
@available(iOS 16.0, macOS 13.0, tvOS 16.0, *)
final class RenderPipeline {

    let displayLayer: AVSampleBufferDisplayLayer
    private let synchronizer = AVSampleBufferRenderSynchronizer()
    private var audioRenderer: AVSampleBufferAudioRenderer?
    private var started = false

    init(displayLayer: AVSampleBufferDisplayLayer) {
        self.displayLayer = displayLayer
        // Clear any frame left over from a prior session before this one's
        // timebase takes over.
        displayLayer.flushAndRemoveImage()
        synchronizer.addRenderer(videoRenderer)
    }

    /// Attach an audio renderer. The synchronizer then uses the audio clock as
    /// the master timebase, so video follows audio.
    func attachAudio() {
        guard audioRenderer == nil else { return }
        configureAudioSession()
        let renderer = AVSampleBufferAudioRenderer()
        audioRenderer = renderer
        synchronizer.addRenderer(renderer)
    }

    func enqueueVideo(_ sample: CMSampleBuffer) {
        startIfNeeded(at: CMSampleBufferGetPresentationTimeStamp(sample))
        let renderer = videoRenderer
        if renderer.isReadyForMoreMediaData { renderer.enqueue(sample) }
    }

    func enqueueAudio(_ sample: CMSampleBuffer) {
        guard let audioRenderer else { return }
        startIfNeeded(at: CMSampleBufferGetPresentationTimeStamp(sample))
        if audioRenderer.isReadyForMoreMediaData { audioRenderer.enqueue(sample) }
    }

    /// Stop the clock and detach renderers so the shared display layer is free
    /// to join a fresh pipeline on the next connection.
    func stop() {
        synchronizer.setRate(0, time: .zero)
        videoRenderer.flush()
        audioRenderer?.flush()
        synchronizer.removeRenderer(videoRenderer, at: .invalid)
        if let audioRenderer {
            synchronizer.removeRenderer(audioRenderer, at: .invalid)
        }
    }

    /// Start the shared clock at the first presented PTS, so playback is
    /// calibrated to the stream's own timeline regardless of its epoch.
    /// A small startup buffer. The shared clock starts this far BEHIND the
    /// first sample so arriving media lands ahead of it (see startIfNeeded).
    private let startupLead = CMTime(value: 300, timescale: 1000)  // 0.3 s

    private func startIfNeeded(at pts: CMTime) {
        guard !started, pts.isNumeric else { return }
        started = true
        // Start the clock a beat BEHIND the first sample, not exactly on it.
        // With zero lead the free-running clock immediately outpaces real-time
        // network delivery (~tens of ms of latency), so every later buffer
        // arrives a hair late and the renderer drops it — silent audio, most
        // visibly on a mid-stream rejoin where no initial burst builds headroom.
        // The lead is a playback buffer that gives arriving media room to land
        // ahead of the clock; it also absorbs jitter.
        synchronizer.setRate(1.0, time: pts - startupLead)
    }

    /// iOS 17 / macOS 14 moved queued rendering off the layer onto
    /// `sampleBufferRenderer`; gate to avoid the deprecated path while still
    /// supporting the iOS 16 / macOS 13 floor.
    private var videoRenderer: AVQueuedSampleBufferRendering {
        if #available(iOS 17.0, macOS 14.0, tvOS 17.0, *) {
            return displayLayer.sampleBufferRenderer
        }
        return displayLayer
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

#endif  /* canImport(AVFoundation) && canImport(CoreMedia) */
