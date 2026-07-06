import Foundation
import MoQService
#if canImport(CoreMedia)
import CoreMedia
#endif
#if canImport(AVFoundation)
import AVFoundation
#endif

/// Every UI-visible phase of a playback session, in the order a session
/// moves through them. This is the single source of truth the view renders —
/// nothing else decides what the user sees.
///
/// `waitingForMedia` is deliberately first-class and unalarming: "connected,
/// but the publisher has not produced a track/sample yet" is a normal live-
/// media condition, not an error.
enum PlayerState: Equatable {
    case idle
    case connecting
    case established
    case waitingForMedia
    case playing
    case ended
    case failed(String)
}

/// The app-level playback state and loop. `@MainActor` so the `@Published`
/// UI state and the display layer are touched only on the main thread; the
/// heavy lifting stays inside MoQService's own service thread, which this
/// loop merely awaits.
@MainActor
final class PlayerModel: ObservableObject {

    // -- Connection inputs (persisted across launches) ------------------

    @Published var urlText: String {
        didSet { defaults.set(urlText, forKey: "SimplePlayer.url") }
    }
    @Published var namespaceText: String {
        didSet { defaults.set(namespaceText, forKey: "SimplePlayer.namespace") }
    }

    // -- Advanced options (each maps 1:1 to MoQEndpoint.Configuration) --

    enum TransportChoice: String, CaseIterable, Identifiable {
        case automatic = "Auto"          // derives from the URL scheme
        case webTransport = "WebTransport"
        case rawQUIC = "Raw QUIC"
        var id: String { rawValue }
    }
    enum VersionChoice: String, CaseIterable, Identifiable {
        case automatic = "Auto"          // offer all supported versions
        case draft18 = "Draft 18"
        case draft16 = "Draft 16"
        var id: String { rawValue }
    }

    @Published var transport: TransportChoice = .automatic
    @Published var version: VersionChoice = .automatic
    /// Path to a PEM CA bundle. Required on iOS for OpenSSL verification
    /// (iOS ships no readable system PEM bundle); optional on macOS.
    @Published var caFilePath: String = ""
    @Published var insecureSkipVerify: Bool = false

    // -- Observable output ----------------------------------------------

    @Published private(set) var state: PlayerState = .idle
    @Published private(set) var sampleCount: Int = 0
    @Published private(set) var trackName: String?
    @Published private(set) var negotiatedVersion: String?
    @Published private(set) var isRunning: Bool = false

    #if canImport(AVFoundation)
    /// The render target the view hosts. Sample buffers are enqueued on
    /// iOS 17 / macOS 14 and newer (the sampleBufferRenderer API); older
    /// systems still count samples — this is an example, not a shipping
    /// decoder fallback chain.
    let displayLayer = AVSampleBufferDisplayLayer()
    #endif

    private var playbackTask: Task<Void, Never>?
    private let defaults = UserDefaults.standard

    #if canImport(AVFoundation) && canImport(CoreMedia)
    /// The A/V render + sync pipeline for the active session; nil while idle.
    private var pipeline: RenderPipeline?
    #endif

    /// Generation token. Bumped on every connect()/disconnect() so a stale
    /// task that is still unwinding cannot clobber a newer connection's UI
    /// state (terminal clears, status). A task's writes apply only while its
    /// captured `run` is still current.
    private var runID: Int = 0

    init() {
        urlText = defaults.string(forKey: "SimplePlayer.url")
            ?? "https://relay.example.com:4433/moq-relay"
        namespaceText = defaults.string(forKey: "SimplePlayer.namespace")
            ?? "live/cam1"
    }

    // -- Actions ---------------------------------------------------------

    func toggle() {
        isRunning ? disconnect() : connect()
    }

    func connect() {
        guard let url = URL(string: urlText), url.scheme != nil else {
            state = .failed("Invalid URL")
            return
        }
        let namespace = MediaNamespace(namespaceText)
        let configuration = makeConfiguration(url: url)

        runID &+= 1
        let run = runID
        isRunning = true
        sampleCount = 0
        trackName = nil
        negotiatedVersion = nil
        state = .connecting

        // Sessions are strictly serial: cancel any prior task and await its
        // full teardown (endpoint close + pipeline stop) BEFORE dialing, so a
        // disconnect→reconnect never runs two live sessions against the relay
        // at once — that overlap is what made the second play's audio stutter.
        let previous = playbackTask
        playbackTask = Task { [weak self] in
            previous?.cancel()
            _ = await previous?.value
            guard let self else { return }
            await self.play(run: run, configuration: configuration,
                            namespace: namespace)
        }
    }

    func disconnect() {
        // Supersede: bump the generation so the in-flight task's terminal
        // writes no-op, silence the pipeline immediately, and drop to idle. The
        // task is cancelled and left to unwind (closing its endpoint); it is
        // kept on `playbackTask` so the next connect() awaits it before dialing.
        runID &+= 1
        isRunning = false
        teardownPipeline()
        state = .idle
        playbackTask?.cancel()
    }

    // -- Configuration mapping --------------------------------------------

    /// The Advanced panel maps 1:1 onto `MoQEndpoint.Configuration`; nothing
    /// here is invented. `.automatic` transport derives from the URL scheme
    /// (moqt:// -> raw QUIC, https:// -> WebTransport, taking the WT path
    /// from the URL).
    private func makeConfiguration(url: URL) -> MoQEndpoint.Configuration {
        var configuration = MoQEndpoint.Configuration(url: url)
        switch transport {
        case .automatic:    break
        case .webTransport: configuration.transportProtocol = .webTransport
        case .rawQUIC:      configuration.transportProtocol = .rawQUIC
        }
        switch version {
        case .automatic: break
        case .draft18:   configuration.versions = .list([.draft18])
        case .draft16:   configuration.versions = .list([.draft16])
        }
        let trimmedCA = caFilePath.trimmingCharacters(in: .whitespaces)
        if !trimmedCA.isEmpty {
            configuration.caFileURL = URL(fileURLWithPath: trimmedCA)
        } else if let bundled = Bundle.main.url(
            forResource: "cacert", withExtension: "pem") {
            // Default trust roots, fetched into the app bundle at build time by
            // scripts/make-app.sh (never committed). iOS ships no readable
            // system PEM bundle for OpenSSL, so without a CA file every https://
            // relay fails verification; macOS falls back to its system roots
            // when this is absent, and any platform can override in Advanced.
            configuration.caFileURL = bundled
        }
        configuration.insecureSkipVerify = insecureSkipVerify
        return configuration
    }

    // -- The playback loop -------------------------------------------------

    /// True only while `run` is the newest connect()/disconnect() generation.
    private func isCurrent(_ run: Int) -> Bool { run == runID }

    private func play(run: Int, configuration: MoQEndpoint.Configuration,
                      namespace: MediaNamespace) async {
        let outcome: PlayerState
        do {
            let endpoint = try MoQEndpoint.connect(configuration: configuration)
            outcome = await receive(
                run: run, endpoint: endpoint, namespace: namespace)
        } catch is CancellationError {
            outcome = .idle
        } catch {
            outcome = .failed(describe(error))
        }
        // Terminal UI state: apply only if this task is still current. A
        // newer connect()/disconnect() has already installed its own state.
        guard isCurrent(run) else { return }
        playbackTask = nil
        isRunning = false
        teardownPipeline()
        state = outcome
    }

    /// established -> receiver -> catalog -> select video (+ audio) tracks ->
    /// format descriptions -> for-await objects routed by role -> render
    /// pipeline. The core MoQService shape, wrapped in app state. Owns the
    /// endpoint's teardown on every path and returns the terminal state.
    private func receive(
        run: Int, endpoint: MoQEndpoint, namespace: MediaNamespace
    ) async -> PlayerState {
        do {
            try await endpoint.established()
            // Bail before attaching if superseded/cancelled during the
            // establish await (the next cancellation-aware await is inside
            // the discovery/object loops, so guard the gap explicitly).
            guard isCurrent(run), !Task.isCancelled else {
                await endpoint.close()
                return .idle
            }
            state = .established
            negotiatedVersion = endpoint.negotiatedVersion
                .map(String.init(describing:))

            // .live(namespace:) auto-subscribes every discovered track and
            // drops to the latest keyframe under pressure — the right preset
            // for a live viewer.
            let receiver = try MediaReceiver.attach(
                to: endpoint, configuration: .live(namespace: namespace))
            state = .waitingForMedia

            let selection = try await selectTracks(receiver, run: run)
            guard let video = selection.video else {
                await receiver.close()
                await endpoint.close()
                return .failed("No video track in the stream catalog")
            }
            if isCurrent(run) {
                trackName = video.description.name
            }

            #if canImport(CoreMedia)
            let outcome = try await pump(
                run: run, receiver: receiver,
                video: video, audio: selection.audio)
            #else
            let outcome = try await countOnly(
                run: run, receiver: receiver, video: video)
            #endif

            await receiver.close()
            await endpoint.close()
            return outcome
        } catch is CancellationError {
            await endpoint.close()
            return .idle
        } catch {
            await endpoint.close()
            return .failed(describe(error))
        }
    }

    /// Drain discovery until the catalog is fully enumerated, then pick the
    /// first video track and the first audio track. Waiting for `catalogReady`
    /// (rather than returning on the first video event) is what lets a stream's
    /// audio be discovered before playback begins — an audio-only decision
    /// can't be made from a single video event. Selection lives here, apart
    /// from the render pipeline, so a later ABR rendition switch changes only
    /// this step.
    private func selectTracks(
        _ receiver: MediaReceiver, run: Int
    ) async throws -> (video: MediaTrack?, audio: MediaTrack?) {
        for try await event in receiver.trackEvents {
            guard isCurrent(run), !Task.isCancelled else { break }
            if case .catalogReady = event { break }
        }
        let tracks = receiver.tracks
        return (tracks.first { $0.description.mediaType == .video },
                tracks.first { $0.description.mediaType == .audio })
    }

    #if canImport(CoreMedia)
    /// The object loop: route each object to the video or audio renderer by
    /// track identity, building each renderer's format description once. Audio
    /// is best-effort — a stream can carry an audio track whose catalog lacks
    /// the fields CoreMedia needs, so we fall back to video-only rather than
    /// fail the whole session.
    private func pump(
        run: Int, receiver: MediaReceiver,
        video: MediaTrack, audio: MediaTrack?
    ) async throws -> PlayerState {
        let videoFormat = try video.description.makeFormatDescription()

        var audioTrack: MediaTrack?
        var audioFormat: CMFormatDescription?
        if let audio,
           let format = try? audio.description.makeFormatDescription() {
            audioTrack = audio
            audioFormat = format
        }

        #if canImport(AVFoundation)
        let pipeline = RenderPipeline(displayLayer: displayLayer)
        if audioFormat != nil { pipeline.attachAudio() }
        self.pipeline = pipeline
        #endif

        for try await object in receiver.objects {
            // Stop as soon as this task is superseded or cancelled; no
            // suspension follows before the state writes, so runID is stable
            // through the rest of the iteration.
            guard isCurrent(run), !Task.isCancelled else { break }
            if object.track === video {
                let sample = try object.makeSampleBuffer(
                    formatDescription: videoFormat)
                #if canImport(AVFoundation)
                pipeline.enqueueVideo(sample)
                #endif
                sampleCount += 1
                if state != .playing { state = .playing }
            } else if let audioTrack, object.track === audioTrack,
                      let audioFormat {
                let sample = try object.makeSampleBuffer(
                    formatDescription: audioFormat)
                #if canImport(AVFoundation)
                pipeline.enqueueAudio(sample)
                #endif
            }
        }
        return isCurrent(run) ? .ended : .idle
    }
    #else
    /// Non-CoreMedia platforms (Linux): count video access units so the UI has
    /// a liveness signal, without any rendering.
    private func countOnly(
        run: Int, receiver: MediaReceiver, video: MediaTrack
    ) async throws -> PlayerState {
        for try await object in receiver.objects where object.track === video {
            guard isCurrent(run), !Task.isCancelled else { break }
            if !object.mediaData.isEmpty { sampleCount += 1 }
            if state != .playing { state = .playing }
        }
        return isCurrent(run) ? .ended : .idle
    }
    #endif

    /// Tear down the active render pipeline, freeing the shared display layer
    /// to join a fresh pipeline on the next connection.
    private func teardownPipeline() {
        #if canImport(AVFoundation) && canImport(CoreMedia)
        pipeline?.stop()
        pipeline = nil
        #endif
    }

    /// Give the most common connection failures a human sentence; fall back
    /// to the error's own description for the rest.
    private func describe(_ error: Error) -> String {
        if let serviceError = error as? MoQServiceError {
            switch serviceError {
            case .interrupted:
                return "Connection interrupted"
            case .closed:
                return "Connection closed"
            case .unsupported:
                return "Unsupported media format (CMAF playback is not " +
                       "wired up yet — use a RAW/LOC stream)"
            // The SDK now classifies why a connection failed, so the app can
            // report it truthfully instead of guessing from an opaque code.
            case .connectionFailed(let failure):
                switch failure.kind {
                case .certificateUnverified:
                    return "The server's certificate could not be verified. " +
                           "On iOS, set a CA bundle in Advanced."
                case .tls:
                    return "TLS handshake failed (0x\(String(failure.code, radix: 16)))"
                case .transport:
                    return "Could not reach the relay"
                }
            case .invalidArgument(let message):
                return message
            default:
                return String(describing: serviceError)
            }
        }
        return String(describing: error)
    }

}
