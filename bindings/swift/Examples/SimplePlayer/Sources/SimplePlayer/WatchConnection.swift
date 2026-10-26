import Foundation
import MoQService
#if canImport(CoreMedia)
import CoreMedia
#endif

/// One player output, as the watch renders it. The live player maps each
/// PlayerOutput onto it; tests script these directly, since only the SDK
/// builds its own records.
enum WatchOutput {
    case configure(WatchConfigure)
    case timeline(WatchTimeline)
    case discontinuity(trackID: UInt64)
    case unit(WatchUnit)
    case end(trackID: UInt64, cause: PlayerEndCause)
    case other
}

struct WatchConfigure {
    var trackID: UInt64
    var trackName: String?
    var mediaType: MediaType?
    /// The stream carries a decoder configuration (a Video Config).
    var hasCodecConfig: Bool
    #if canImport(CoreMedia)
    var makeFormatDescription: () throws -> CMFormatDescription
    #endif
}

/// A timeline's mapping: `anchorMedia` presents at host time
/// `anchorLocalMicroseconds`, at `rate`.
struct WatchTimeline {
    var timelineID: UInt32
    var anchorMedia: Duration
    var anchorLocalMicroseconds: UInt64
    var rate: Double
}

struct WatchUnit {
    var trackID: UInt64
    var timelineID: UInt32
    var configurationGeneration: UInt64
    var data: Data
    /// Media time by which the unit has been presented: its presentation
    /// time plus its duration, when known.
    var presentedBy: Duration
    #if canImport(CoreMedia)
    var makeSampleBuffer: (CMFormatDescription, NALFraming) throws -> CMSampleBuffer
    #endif
}

/// A connection a watch runs over: the endpoint, then the player attached
/// to it. The live implementation wraps MoQEndpoint and MediaPlayer; tests
/// script one.
@MainActor
protocol WatchEndpoint: AnyObject {
    /// The negotiated draft, as the UI shows it ("draft16").
    var negotiatedVersion: String? { get }
    func established(within deadline: Duration) async throws
    func attachPlayer(_ configuration: MediaPlayer.Configuration) async throws -> any WatchPlayer
    func close() async
}

/// The player's side of a watch: one consumer each for outputs and events.
@MainActor
protocol WatchPlayer: AnyObject {
    func nextOutput() async throws -> WatchOutput?
    func nextEvent() async throws -> PlayerEvent?
    func tracksSelected() async throws -> Int
    func reportDecodeError(trackID: UInt64, generation: UInt64) async throws
    func close() async
}

/// How a watch opens its connection.
struct WatchConnector {
    var connect: @MainActor (MoQEndpoint.Configuration) throws -> any WatchEndpoint

    static let live = WatchConnector { configuration in
        LiveWatchEndpoint(endpoint: try MoQEndpoint.connect(configuration: configuration))
    }
}

@MainActor
final class LiveWatchEndpoint: WatchEndpoint {
    let endpoint: MoQEndpoint

    init(endpoint: MoQEndpoint) { self.endpoint = endpoint }

    var negotiatedVersion: String? {
        endpoint.negotiatedWireVersion.map { "draft\($0.draftNumber)" }
    }

    func established(within deadline: Duration) async throws {
        try await endpoint.established(within: deadline)
    }

    func attachPlayer(_ configuration: MediaPlayer.Configuration) async throws -> any WatchPlayer {
        LiveWatchPlayer(player: try await MediaPlayer.attach(
            to: endpoint, configuration: configuration))
    }

    func close() async { await endpoint.close() }
}

@MainActor
final class LiveWatchPlayer: WatchPlayer {
    let player: MediaPlayer
    private var outputs: PlayerOutputs.Iterator
    private var events: PlayerEvents.Iterator
    /// Tracks by id, for reporting decode errors on them.
    private var tracks: [UInt64: PlayerTrack] = [:]

    init(player: MediaPlayer) {
        self.player = player
        self.outputs = player.outputs.makeAsyncIterator()
        self.events = player.events.makeAsyncIterator()
    }

    func nextOutput() async throws -> WatchOutput? {
        var iterator = outputs
        defer { outputs = iterator }
        guard let output = try await iterator.next() else { return nil }
        return watchOutput(output)
    }

    private func watchOutput(_ output: PlayerOutput) -> WatchOutput {
        switch output {
        case .configure(let c):
            tracks[c.track.id] = c.track
            #if canImport(CoreMedia)
            return .configure(WatchConfigure(
                trackID: c.track.id, trackName: c.track.name, mediaType: c.mediaType,
                hasCodecConfig: c.codecConfig != nil,
                makeFormatDescription: { try c.makeFormatDescription() }))
            #else
            return .configure(WatchConfigure(
                trackID: c.track.id, trackName: c.track.name, mediaType: c.mediaType,
                hasCodecConfig: c.codecConfig != nil))
            #endif
        case .timeline(let t):
            return .timeline(WatchTimeline(
                timelineID: t.timelineID, anchorMedia: t.anchorMedia,
                anchorLocalMicroseconds: t.anchorLocalMicroseconds, rate: t.rate))
        case .discontinuity(let d):
            return .discontinuity(trackID: d.track.id)
        case .unit(let u):
            let presentedBy = u.presentationTime + (u.sampleDuration ?? .zero)
            #if canImport(CoreMedia)
            return .unit(WatchUnit(
                trackID: u.track.id, timelineID: u.timelineID,
                configurationGeneration: u.configurationGeneration, data: u.data,
                presentedBy: presentedBy,
                makeSampleBuffer: { try u.makeSampleBuffer(formatDescription: $0, framing: $1) }))
            #else
            return .unit(WatchUnit(
                trackID: u.track.id, timelineID: u.timelineID,
                configurationGeneration: u.configurationGeneration, data: u.data,
                presentedBy: presentedBy))
            #endif
        case .end(let e):
            return .end(trackID: e.track.id, cause: e.cause)
        case .unknown:
            return .other
        }
    }

    func nextEvent() async throws -> PlayerEvent? {
        var iterator = events
        defer { events = iterator }
        return try await iterator.next()
    }

    func tracksSelected() async throws -> Int {
        try await player.statistics().tracksSelected
    }

    func reportDecodeError(trackID: UInt64, generation: UInt64) async throws {
        guard let track = tracks[trackID] else { return }
        try await player.reportDecodeError(track, generation: generation)
    }

    func close() async { await player.close() }
}
