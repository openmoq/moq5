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
/// One watch is: connect the endpoint, wait for establishment, attach a
/// ``MediaPlayer`` (automatic selection), and render its outputs. The player
/// owns what used to be hand-rolled here -- track selection, decode order,
/// gaps and recovery, the presentation timeline, how a track ends -- so this
/// model only maps the watch's phases onto `PlayerState` and feeds the
/// render pipeline. Reconnects serialize: a new watch waits for the previous
/// one to close its endpoint.
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
    /// Which QUIC stack serves the connection. This example DEFAULTS to
    /// `.picoquic` on a fresh install: the picoquic family reliably serves
    /// both raw-QUIC `moqt://` relays and WebTransport `https://` relays.
    /// `.automatic` also resolves to the picoquic family. An explicitly saved
    /// user choice always wins over this default.
    ///
    /// `.wtquicNetwork` (WebTransport over Network.framework) is
    /// EXPERIMENTAL. On the tested OS/SDK (macOS 15.7.3 / iOS 26.2),
    /// Network.framework's public QUIC-multiplex client drops a conformant
    /// HTTP/3 server's eager server-initiated control stream during the
    /// connection's ready transition, so establishment against third-party
    /// relays is unreliable. It stays selectable for experimentation, but it
    /// is no longer the default and is not recommended on any platform.
    ///
    /// `.wtquicMsquic` is WebTransport over MsQuic (cross-platform, but the
    /// iOS xcframework does not build MsQuic, so it is unsupported at connect
    /// there). A backend not compiled into the installed service fails with a
    /// clear "unsupported" message.
    enum BackendChoice: String, CaseIterable, Identifiable {
        case wtquicNetwork = "Network.framework (WT)"
        case wtquicMsquic = "MsQuic (WT)"
        case automatic = "Auto (picoquic)"
        case picoquic = "picoquic"
        var id: String { rawValue }
        /// The picker label. Distinct from `rawValue` (which is the persisted
        /// key — never change it, or saved preferences break): the label may
        /// carry an "Experimental" marker the stored value must not.
        var displayName: String {
            switch self {
            case .wtquicNetwork: "Network.framework (WT) · Experimental"
            default: rawValue
            }
        }
    }
    enum VersionChoice: String, CaseIterable, Identifiable {
        case automatic = "Auto"          // legacy offer: draft 18, then draft 16
        case draft22 = "Draft 22"
        case draft18 = "Draft 18"
        case draft16 = "Draft 16"
        var id: String { rawValue }
    }

    @Published var transport: TransportChoice = .automatic
    @Published var backend: BackendChoice {
        didSet { defaults.set(backend.rawValue, forKey: "SimplePlayer.backend") }
    }
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
    /// The backend that the CURRENT/last connection actually used, latched
    /// at connect(). Failure messages describe THIS, not a picker the user
    /// may have changed mid-connection (the picker is also disabled while
    /// running -- see ContentView).
    private var activeBackend: BackendChoice = .picoquic

    #if canImport(AVFoundation)
    /// Called with each video sample once it is enqueued, in enqueue (decode)
    /// order; for tests that check what reached the renderer.
    var onVideoSampleEnqueued: ((CMSampleBuffer) -> Void)?

    /// The render target the view hosts. Sample buffers are enqueued on
    /// iOS 17 / macOS 14 and newer (the sampleBufferRenderer API); older
    /// systems still count samples — this is an example, not a shipping
    /// decoder fallback chain.
    let displayLayer = AVSampleBufferDisplayLayer()
    #endif

    private let defaults = UserDefaults.standard

    #if canImport(AVFoundation) && canImport(CoreMedia)
    /// The render sink for the active watch; nil while idle.
    private var sink: (any RenderSink)?
    /// Makes a watch's render sink: the A/V pipeline on the display layer.
    var makeSink: @MainActor (PlayerModel) -> any RenderSink = { model in
        RenderPipeline(displayLayer: model.displayLayer)
    }
    #endif

    /// How a watch reaches a relay: the real endpoint and player by default.
    var connector = WatchConnector.live

    /// The running watch. A new connect() waits for the previous watch's
    /// task, whose unwind closes its player and endpoint, so two live
    /// endpoints never overlap.
    private var watchTask: Task<Void, Never>?
    /// Identifies the current watch: a superseded watch's late phases and
    /// outputs never reach the UI.
    private var watchID = 0
    /// The watch whose terminal phase was applied: its first one stands.
    private var finishedWatchID = 0

    init() {
        urlText = defaults.string(forKey: "SimplePlayer.url")
            ?? "https://relay.example.com:4433/moq-relay"
        namespaceText = defaults.string(forKey: "SimplePlayer.namespace")
            ?? "live/cam1"
        // Fresh install defaults to the reliable picoquic family; an
        // explicitly saved choice (any prior selection) is preserved.
        backend = defaults.string(forKey: "SimplePlayer.backend")
            .flatMap(BackendChoice.init(rawValue:)) ?? .picoquic
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
        let configuration = makeWatchConfiguration(url: url)

        isRunning = true
        activeBackend = backend        // latch: describe() uses this, not the live picker
        beginWatchUIState()

        watchID += 1
        let id = watchID
        let previous = watchTask
        previous?.cancel()
        watchTask = Task { [weak self] in
            await previous?.value      // the previous endpoint is closed first
            guard !Task.isCancelled else { return }
            await self?.watch(configuration, id: id)
        }
    }

    func disconnect() {
        isRunning = false
        state = .idle
        watchID += 1                   // the cancelled watch's late phases are ignored
        watchTask?.cancel()            // kept: the next connect() waits for its unwind
        teardownPipeline()
    }

    /// Wait for the current watch to finish its unwind.
    func watchCompleted() async {
        await watchTask?.value
    }

    // -- The watch ---------------------------------------------------------

    /// What one watch runs with: the endpoint, the player, and how long
    /// establishment may take.
    struct WatchConfiguration {
        var endpoint: MoQEndpoint.Configuration
        var player: MediaPlayer.Configuration
        var establishmentTimeout: Duration
    }

    /// The lifecycle phases a watch reports to the UI, in order.
    enum WatchPhase {
        case connecting
        case established
        /// Connected and attached; the publisher has produced nothing yet.
        case waitingForMedia
        /// Video units are being presented.
        case receiving
        /// The catalog offers nothing this player can select.
        case noPlayableTracks
        case ended
        /// A track ended in failure (its END said so), or what the watch
        /// enqueued did not present, without a service error to report.
        case trackFailed(TrackFailure)
        case failed(MoQServiceError)
    }

    /// One watch: connect, establish, attach the player, render its outputs
    /// until every selected track has ended and what it rendered has
    /// presented, then close everything it opened, once.
    private func watch(_ configuration: WatchConfiguration, id: Int) async {
        let endpoint: any WatchEndpoint
        do {
            endpoint = try connector.connect(configuration.endpoint)
        } catch {
            report(.failed(serviceError(error)), id: id)
            return
        }
        var player: (any WatchPlayer)?
        do {
            try await endpoint.established(within: configuration.establishmentTimeout)
            report(.established, id: id)
            if id == watchID, let version = endpoint.negotiatedVersion {
                negotiatedVersion = version
            }
            let attached = try await endpoint.attachPlayer(configuration.player)
            player = attached
            report(.waitingForMedia, id: id)
            let render = Task { @MainActor [weak self] in
                guard let self else { return }
                try await self.render(attached, id: id)
            }
            let events = Task { @MainActor [weak self] in
                await self?.observe(attached, id: id, render: render)
            }
            defer { events.cancel() }
            try await withTaskCancellationHandler {
                try await render.value
            } onCancel: {
                render.cancel()
            }
            report(.ended, id: id)
        } catch is CancellationError {
            // Superseded, disconnected, or ended by observe().
        } catch let failure as TrackFailure {
            report(.trackFailed(failure), id: id)
        } catch {
            report(.failed(serviceError(error)), id: id)
        }
        await player?.close()
        await endpoint.close()
    }

    private func serviceError(_ error: any Error) -> MoQServiceError {
        error as? MoQServiceError ?? .internalError(-1)
    }

    /// Apply a phase for the current watch only; after its terminal phase,
    /// nothing more.
    private func report(_ phase: WatchPhase, id: Int) {
        guard id == watchID, finishedWatchID != id else { return }
        if apply(phase) { finishedWatchID = id }
    }

    /// Player events: a catalog with nothing selectable ends the watch;
    /// the rest is diagnostics this example does not show.
    private func observe(_ player: any WatchPlayer, id: Int,
                         render: Task<Void, any Error>) async {
        do {
            while let event = try await player.nextEvent() {
                guard id == watchID else { return }
                if case .ready = event,
                   let selected = try? await player.tracksSelected(), selected == 0 {
                    report(.noPlayableTracks, id: id)
                    render.cancel()
                    return
                }
            }
        } catch {
            // The outputs loop reports how the watch ended.
        }
    }

    /// Per-watch UI reset, applied SYNCHRONOUSLY in connect(), before the
    /// watch task runs, so nothing from the previous watch -- in particular
    /// the establishment latch -- survives into the new one.
    func beginWatchUIState() {
        state = .connecting
        sampleCount = 0
        trackName = nil
        negotiatedVersion = nil
        establishmentLatched = false
    }

    /// Durable establishment latch for failure classification: once a watch
    /// is past establishment, an `.unsupported` is about the media, not
    /// the transport configuration.
    private var establishmentLatched = false

    /// Map one watch phase onto the UI. Returns `true` for a terminal phase.
    @discardableResult
    func apply(_ phase: WatchPhase) -> Bool {
        switch phase {
        case .connecting:
            state = .connecting
            establishmentLatched = false
            sampleCount = 0
            trackName = nil
            negotiatedVersion = nil
            teardownPipeline()
            return false
        case .established:
            state = .established
            establishmentLatched = true
            return false
        case .waitingForMedia:
            state = .waitingForMedia
            establishmentLatched = true
            return false
        case .receiving:
            state = .playing
            establishmentLatched = true
            return false
        case .noPlayableTracks:
            finishWatch(.failed("No playable track in the stream catalog"))
            return true
        case .ended:
            finishWatch(.ended)
            return true
        case .trackFailed(.fatal):
            finishWatch(.failed("The stream failed"))
            return true
        case .trackFailed(.local):
            finishWatch(.failed("A track could not be received on this device"))
            return true
        case .trackFailed(.presentation):
            finishWatch(.failed("The stream's last frames could not be presented"))
            return true
        case .failed(let error):
            finishWatch(.failed(describe(
                error, duringConnect: !establishmentLatched)))
            return true
        }
    }

    private func finishWatch(_ terminal: PlayerState) {
        state = terminal
        isRunning = false
        teardownPipeline()
    }

    // -- Rendering (app-only; the player vends decode-order units) ---------

    #if canImport(AVFoundation) && canImport(CoreMedia)
    /// Render the player's outputs until every selected track has ended: a
    /// CONFIGURE builds the track's format description, the TIMELINE starts
    /// the clock, a DISCONTINUITY flushes the renderer, and each UNIT is
    /// enqueued once its renderer has room (no further output is pulled
    /// meanwhile -- the player's backpressure). Audio is best-effort: a codec
    /// CoreMedia cannot describe here plays video only. A video renderer
    /// that fails is flushed and reported, and the player resumes the track
    /// at its next independent unit. Once every track has ended, the watch
    /// finishes as finishMedia says.
    private func render(_ player: any WatchPlayer, id: Int) async throws {
        let sink = makeSink(self)
        self.sink = sink
        var formats: [UInt64: (video: Bool, format: CMFormatDescription, config: Bool)] = [:]
        var ends = TrackEnds()
        var presentedBy: Duration?
        while let output = try await player.nextOutput() {
            guard id == watchID else { return }
            switch output {
            case .configure(let configuration):
                switch configuration.mediaType {
                case .video:
                    formats[configuration.trackID] =
                        (true, try configuration.makeFormatDescription(),
                         configuration.hasCodecConfig)
                    trackName = trackName ?? configuration.trackName
                case .audio:
                    if let format = try? configuration.makeFormatDescription() {
                        formats[configuration.trackID] = (false, format, true)
                        sink.attachAudio()
                    } else {
                        formats[configuration.trackID] = nil
                    }
                default:
                    formats[configuration.trackID] = nil
                }
            case .timeline(let timeline):
                sink.apply(timeline)
            case .discontinuity(let trackID):
                if let entry = formats[trackID] {
                    sink.renderer(video: entry.video)?.flush()
                }
            case .unit(let unit):
                guard let entry = formats[unit.trackID], sink.drives(unit.timelineID),
                      let renderer = sink.renderer(video: entry.video) else { continue }
                let framing = entry.video
                    ? Self.locFraming(of: unit.data, hasVideoConfig: entry.config)
                    : .lengthPrefixed
                let sample = try unit.makeSampleBuffer(entry.format, framing)
                switch await enqueueWhenReady(sample, into: renderer) {
                case .cancelled:
                    return
                case .failed:
                    renderer.flush()
                    if entry.video {
                        try await player.reportDecodeError(
                            trackID: unit.trackID, generation: unit.configurationGeneration)
                    }
                case .enqueued:
                    presentedBy = max(presentedBy ?? unit.presentedBy, unit.presentedBy)
                    guard entry.video else { break }
                    sampleCount += 1
                    onVideoSampleEnqueued?(sample)
                    if state != .playing { report(.receiving, id: id) }
                }
            case .end(let trackID, let cause):
                ends.end(trackID, cause: cause)
                guard ends.count >= (try await player.tracksSelected()) else { break }
                try await finishMedia(player, ends: ends, sink: sink, presentedBy: presentedBy)
                return
            case .other:
                break
            }
        }
    }

    /// Every selected track ended. A FATAL end fails the watch with the
    /// player's fatal error, which its outputs report next; a local failure
    /// fails it too. Otherwise the media finished once what was enqueued
    /// has presented (the clock reaches the last unit's end); a renderer
    /// that fails first, or a clock that does not get there, fails the
    /// watch. Only then does it report and tear its renderers down.
    private func finishMedia(_ player: any WatchPlayer, ends: TrackEnds, sink: any RenderSink,
                             presentedBy: Duration?) async throws {
        switch ends.failure {
        case .fatal:
            _ = try await player.nextOutput()
            throw TrackFailure.fatal
        case .local:
            throw TrackFailure.local
        case nil:
            guard let presentedBy else { return }
            switch await sink.settle(through: presentedBy) {
            case .presented: return
            case .cancelled: throw CancellationError()
            case .failed, .notPresented: throw TrackFailure.presentation
            }
        }
    }

    /// The NAL framing of an H.264/HEVC unit under LOC (LOC-04 §2.1.3-2.1.4):
    /// a leading 4-byte value of 1 is a start code, never a length; a
    /// 3-byte start code may begin a unit only on a track with no Video
    /// Config; anything else is a 4-byte length prefix.
    static func locFraming(of data: Data, hasVideoConfig: Bool) -> NALFraming {
        if data.count >= 4 && data.starts(with: [0, 0, 0, 1]) { return .annexB }
        if !hasVideoConfig && data.count >= 3 && data.starts(with: [0, 0, 1]) {
            return .annexB
        }
        return .lengthPrefixed
    }
    #else
    /// Non-CoreMedia platforms (Linux): count video units so the UI has a
    /// liveness signal, without any rendering.
    private func render(_ player: any WatchPlayer, id: Int) async throws {
        var video: Set<UInt64> = []
        var ends = TrackEnds()
        while let output = try await player.nextOutput() {
            guard id == watchID else { return }
            switch output {
            case .configure(let configuration) where configuration.mediaType == .video:
                video.insert(configuration.trackID)
                trackName = trackName ?? configuration.trackName
            case .unit(let unit) where video.contains(unit.trackID):
                sampleCount += 1
                if state != .playing { report(.receiving, id: id) }
            case .end(let trackID, let cause):
                ends.end(trackID, cause: cause)
                guard ends.count >= (try await player.tracksSelected()) else { break }
                switch ends.failure {
                case .fatal:
                    _ = try await player.nextOutput()
                    throw TrackFailure.fatal
                case .local:
                    throw TrackFailure.local
                case nil:
                    return
                }
            default:
                break
            }
        }
    }
    #endif

    /// The tracks whose END was rendered: the watch is over once every
    /// selected track has one.
    /// The selected tracks' ENDs: how many tracks ended, and whether one
    /// ended in failure (FATAL above a local failure).
    struct TrackEnds {
        enum Failure { case local, fatal }
        private var ended: Set<UInt64> = []
        private(set) var failure: Failure?

        var count: Int { ended.count }

        /// Record track `id`'s END; a repeated END is the same track.
        mutating func end(_ id: UInt64, cause: PlayerEndCause) {
            ended.insert(id)
            switch cause {
            case .fatal: failure = .fatal
            case .localFailure: if failure == nil { failure = .local }
            default: break
            }
        }
    }

    /// A watch that did not finish: its tracks ended in failure, or what it
    /// enqueued did not present.
    enum TrackFailure: Error { case local, fatal, presentation }

    /// Tear down the active render sink, freeing the shared display layer
    /// to join a fresh pipeline on the next watch.
    private func teardownPipeline() {
        #if canImport(AVFoundation) && canImport(CoreMedia)
        sink?.stop()
        sink = nil
        #endif
    }

    // -- Configuration mapping --------------------------------------------

    func makeWatchConfiguration(url: URL) -> WatchConfiguration {
        var player = MediaPlayer.Configuration(namespace: MediaNamespace(namespaceText))
        player.companionProfile = version == .draft22
            ? .draft22MSF01LOC04Public : .legacyMSF01LOC01
        // A startup buffer: the first unit presents this far after it is
        // released, so arriving media lands ahead of the clock.
        player.cushion = .milliseconds(300)
        return WatchConfiguration(
            endpoint: makeConfiguration(url: url),
            player: player,
            // Bound establishment so an unreachable relay (or an experimental
            // backend that wedges before it establishes) surfaces a clear
            // failure instead of an indefinite "Connecting...".
            establishmentTimeout: .seconds(15))
    }

    /// The Advanced panel maps 1:1 onto `MoQEndpoint.Configuration`; nothing
    /// here is invented. `.automatic` transport derives from the URL scheme
    /// (moqt:// -> raw QUIC, https:// -> WebTransport, taking the WT path
    /// from the URL).
    func makeConfiguration(url: URL) -> MoQEndpoint.Configuration {
        var configuration = MoQEndpoint.Configuration(url: url)
        switch transport {
        case .automatic:    break
        case .webTransport: configuration.transportProtocol = .webTransport
        case .rawQUIC:      configuration.transportProtocol = .rawQUIC
        }
        switch backend {
        case .automatic:     break
        case .picoquic:      configuration.backend = .picoquic
        case .wtquicNetwork: configuration.backend = .wtquicNetwork
        case .wtquicMsquic:  configuration.backend = .wtquicMsquic
        }
        switch version {
        case .automatic: break
        case .draft22:   configuration.wireVersions = .list([.draft22])
        case .draft18:   configuration.versions = .list([.draft18])
        case .draft16:   configuration.versions = .list([.draft16])
        }
        // The wtquic backends verify against system trust (or
        // insecureSkipVerify) and have no custom-CA-file concept: never attach
        // caFileURL for .wtquicNetwork or .wtquicMsquic (the UI also hides the
        // CA control for them, and the C tier rejects a custom CA with
        // verification on). Only the picoquic family consults a CA bundle.
        let trimmedCA = caFilePath.trimmingCharacters(in: .whitespaces)
        if backend == .wtquicNetwork || backend == .wtquicMsquic {
            // no CA file, ever
        } else if !trimmedCA.isEmpty {
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
    /// to the error's own description for the rest. PHASE-AWARE:
    /// `.unsupported` means two different things depending on when it
    /// arrives, and the message must never blame the transport
    /// configuration for a media-format failure (or vice versa).
    func describe(_ error: MoQServiceError,
                  duringConnect: Bool) -> String {
        switch error {
        case .interrupted:
            return "Connection interrupted"
        case .closed:
            return "Connection closed"
        case .unsupported where duringConnect:
            // Pre-establishment this is a transport/backend-config
            // rejection -- e.g. the selected backend is not compiled into
            // the installed service, or its transport/TLS options are
            // unsupported (a .wtquicNetwork raw-QUIC URL, or a CA file
            // with system trust).
            return "This transport configuration isn't supported by the " +
                   "installed service (backend: \(activeBackend.rawValue))."
        case .unsupported:
            // Post-establishment the transport is fine: the stream's
            // media format is what the receiver cannot handle
            // (unsupported packaging/CMAF shape, codec, or track type).
            return "The stream uses a media format this player doesn't " +
                   "support (unsupported packaging or codec)."
        // The SDK classifies why a connection failed, so the app can report it
        // truthfully instead of guessing from an opaque code.
        case .connectionFailed(let failure):
            switch failure.kind {
            case .certificateUnverified:
                switch activeBackend {
                case .wtquicNetwork:
                    return "The server's certificate is not trusted by the " +
                           "system (Network.framework uses system trust; no " +
                           "CA file applies)."
                case .wtquicMsquic:
                    return "The server's certificate is not trusted (the " +
                           "MsQuic WebTransport backend uses system trust or " +
                           "insecureSkipVerify; no CA file applies)."
                case .automatic, .picoquic:
                    return "The server's certificate could not be verified. " +
                           "On iOS, set a CA bundle in Advanced."
                }
            case .tls:
                // the code is a signed transport-native detail (a negative
                // Security OSStatus on the Network.framework backend)
                return "TLS handshake failed (code \(failure.code))"
            case .transport:
                // Covers both an unreachable relay and an establishment
                // timeout (QUIC may have reached the relay but never finished
                // establishing — e.g. the experimental Network.framework
                // backend wedging), so do not claim "could not reach".
                return "Could not establish the connection over " +
                       "\(activeBackend.rawValue). Try a different backend " +
                       "in Advanced."
            }
        case .invalidArgument(let message):
            return message
        default:
            return String(describing: error)
        }
    }
}
