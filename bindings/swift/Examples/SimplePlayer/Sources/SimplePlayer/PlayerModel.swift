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
/// UI state and the display layer are touched only on the main thread.
///
/// The whole connect → established → discover → select → receive lifecycle —
/// and the serialized-reconnect teardown that a disconnect→reconnect needs to
/// avoid running two live sessions at once — now lives in the SDK's
/// ``LiveMediaSession``. This model just maps ``WatchState`` onto `PlayerState`
/// for the UI and drives the app-only render pipeline from the session's
/// object stream. No generation tokens, no manual endpoint/receiver teardown.
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

    private let defaults = UserDefaults.standard

    /// The SDK watch handle: one reusable session for the model's life.
    /// `LiveMediaSession()` wires `MoQEndpoint.connect` as its endpoint factory.
    private let session = LiveMediaSession()

    /// Maps the session's `WatchState` onto `PlayerState`; recreated per
    /// connect so a superseded watch's buffered states never reach the UI.
    private var stateObserver: Task<Void, Never>?
    /// The per-watch object → render loop; nil while there is nothing to render.
    private var renderTask: Task<Void, Never>?
    /// The last render task after cancellation, so the next one waits for it to
    /// release the session's single-consumer object claim before iterating.
    private var previousRender: Task<Void, Never>?

    #if canImport(AVFoundation) && canImport(CoreMedia)
    /// The A/V render + sync pipeline for the active watch; nil while idle.
    private var pipeline: RenderPipeline?
    #endif

    /// Video is required, audio optional — expressed over the whole track
    /// snapshot so no-video is a clean `noMatchingTracks` (a "No video" failure)
    /// while a stream that carries audio gets both tracks selected.
    private static let videoRequiredSelector = TrackSelector { tracks in
        guard let video = tracks.first(where: { $0.description.mediaType == .video })
        else { return [] }
        var selection = [video]
        if let audio = tracks.first(where: { $0.description.mediaType == .audio }) {
            selection.append(audio)
        }
        return selection
    }

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
        let configuration = LiveMediaSession.Configuration(
            endpoint: makeConfiguration(url: url),
            namespace: MediaNamespace(namespaceText),
            selector: Self.videoRequiredSelector)

        isRunning = true
        // Immediate feedback; the observer confirms and advances it as the
        // session progresses.
        state = .connecting
        sampleCount = 0
        trackName = nil
        negotiatedVersion = nil

        // start() supersedes any prior watch AND serializes behind an in-flight
        // stop()'s teardown, so a disconnect→reconnect never runs two live
        // endpoints at once — the SDK owns what used to be hand-rolled here.
        session.start(configuration)
        observeState()
    }

    func disconnect() {
        isRunning = false
        state = .idle
        cancelObserver()
        cancelRender()
        teardownPipeline()
        // beginStop() applies the supersede SYNCHRONOUSLY (teardown proceeds on
        // its own), so an immediate reconnect's start() serializes behind it.
        // A deferred `Task { await stop() }` would instead let that start() run
        // first and then be cancelled by the late stop — the disconnect race.
        session.beginStop()
    }

    // -- State observation ------------------------------------------------

    private func observeState() {
        cancelObserver()
        stateObserver = Task { [weak self, session] in
            for await watchState in session.stateUpdates() {
                guard let self else { break }
                if self.apply(watchState) { break }   // stop on a terminal state
            }
        }
    }

    private func cancelObserver() {
        stateObserver?.cancel()
        stateObserver = nil
    }

    /// Map one `WatchState` onto the UI. Returns `true` for a terminal state,
    /// which ends the observer (a superseded watch's late states can't leak).
    private func apply(_ watchState: WatchState) -> Bool {
        switch watchState {
        case .connecting:
            state = .connecting
            sampleCount = 0
            trackName = nil
            negotiatedVersion = nil
            cancelRender()
            teardownPipeline()
            return false
        case .established:
            state = .established
            refreshVersion()
            return false
        case .awaitingCatalog:
            state = .waitingForMedia
            refreshVersion()
            return false
        case .awaitingFirstObject(let tracks):
            state = .waitingForMedia
            refreshVersion()
            trackName = trackName ?? videoName(in: tracks)
            startRender()
            return false
        case .receiving(let tracks):
            state = .playing
            trackName = trackName ?? videoName(in: tracks)
            startRender()
            return false
        case .noMatchingTracks:
            finishWatch(.failed("No video track in the stream catalog"))
            return true
        case .ended:
            finishWatch(.ended)
            return true
        case .failed(let error):
            finishWatch(.failed(describe(error)))
            return true
        }
    }

    private func finishWatch(_ terminal: PlayerState) {
        state = terminal
        isRunning = false
        cancelRender()
        teardownPipeline()
    }

    /// Read the negotiated version off the live endpoint (the durable fact),
    /// not the transient `.established` payload — a coalescing state stream may
    /// drop that state, but the endpoint keeps answering.
    private func refreshVersion() {
        if negotiatedVersion == nil,
           let version = session.endpoint?.negotiatedVersion {
            negotiatedVersion = String(describing: version)
        }
    }

    private func videoName(in tracks: [MediaTrack]) -> String? {
        tracks.first { $0.description.mediaType == .video }?.description.name
    }

    // -- Rendering (app-only; the SDK vends objects, not sample buffers) ---

    /// Start the object → render loop for the current watch (idempotent within
    /// a watch). It serializes behind the previous watch's render task so the
    /// session's single-consumer object claim is released before re-iterating.
    private func startRender() {
        guard renderTask == nil else { return }
        let cleanup = previousRender
        renderTask = Task { [weak self] in
            await cleanup?.value
            guard let self, !Task.isCancelled else { return }
            await self.renderObjects()
        }
    }

    private func cancelRender() {
        guard let task = renderTask else { return }
        task.cancel()
        previousRender = task        // its unwind releases the object claim
        renderTask = nil
    }

    #if canImport(CoreMedia)
    /// Route each object to the video or audio renderer by media type, building
    /// each renderer's format description once. Audio is best-effort — a stream
    /// can carry an audio track whose catalog lacks the fields CoreMedia needs,
    /// so we fall back to video-only rather than fail the whole session. The
    /// object stream ends (or throws) when the watch ends; the state observer
    /// owns the resulting `PlayerState`, so errors here are swallowed.
    private func renderObjects() async {
        var videoFormat: CMFormatDescription?
        var audioFormat: CMFormatDescription?
        #if canImport(AVFoundation)
        let pipeline = RenderPipeline(displayLayer: displayLayer)
        self.pipeline = pipeline
        #endif
        do {
            for try await object in session.objects {
                let description = object.track.description
                switch description.mediaType {
                case .video:
                    if videoFormat == nil {
                        videoFormat = try description.makeFormatDescription()
                    }
                    let sample = try object.makeSampleBuffer(
                        formatDescription: videoFormat!)
                    #if canImport(AVFoundation)
                    pipeline.enqueueVideo(sample)
                    #endif
                    sampleCount += 1
                case .audio:
                    if audioFormat == nil,
                       let format = try? description.makeFormatDescription() {
                        audioFormat = format
                        #if canImport(AVFoundation)
                        pipeline.attachAudio()
                        #endif
                    }
                    if let audioFormat {
                        let sample = try object.makeSampleBuffer(
                            formatDescription: audioFormat)
                        #if canImport(AVFoundation)
                        pipeline.enqueueAudio(sample)
                        #endif
                    }
                }
            }
        } catch {
            // Terminal/interrupted object stream; the state observer maps it.
        }
    }
    #else
    /// Non-CoreMedia platforms (Linux): count video access units so the UI has
    /// a liveness signal, without any rendering.
    private func renderObjects() async {
        do {
            for try await object in session.objects
                where object.track.description.mediaType == .video {
                if !object.mediaData.isEmpty { sampleCount += 1 }
            }
        } catch {
            // Terminal/interrupted object stream; the state observer maps it.
        }
    }
    #endif

    /// Tear down the active render pipeline, freeing the shared display layer
    /// to join a fresh pipeline on the next watch.
    private func teardownPipeline() {
        #if canImport(AVFoundation) && canImport(CoreMedia)
        pipeline?.stop()
        pipeline = nil
        #endif
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

    /// Give the most common connection failures a human sentence; fall back
    /// to the error's own description for the rest.
    private func describe(_ error: MoQServiceError) -> String {
        switch error {
        case .interrupted:
            return "Connection interrupted"
        case .closed:
            return "Connection closed"
        case .unsupported:
            return "Unsupported media format (CMAF playback is not " +
                   "wired up yet — use a RAW/LOC stream)"
        // The SDK classifies why a connection failed, so the app can report it
        // truthfully instead of guessing from an opaque code.
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
            return String(describing: error)
        }
    }
}
