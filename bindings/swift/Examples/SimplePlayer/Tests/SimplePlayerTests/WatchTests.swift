// The watch's lifecycle and render decisions, without a network: the LOC
// framing rule, how a watch ends and unwinds, and a renderer that fails
// while it is waited on.

import XCTest
import MoQService
#if canImport(CoreMedia)
import CoreMedia
#endif
#if canImport(AVFoundation)
import AVFoundation
#endif
@testable import SimplePlayer

@MainActor
final class ScriptedWatchPlayer: WatchPlayer {
    var events: [PlayerEvent] = []
    /// Outputs delivered first, in order; then `errorAfterScript`, if set.
    var script: [WatchOutput] = []
    var errorAfterScript: (any Error)?
    var selected = 1
    /// Outputs end (nil) at once; otherwise nextOutput parks until cancelled.
    var outputsEnd = true
    /// Outputs end after this long, whether or not the consumer was
    /// cancelled meanwhile.
    var outputsEndAfter: Duration?
    private(set) var closes = 0
    private(set) var decodeErrors = 0

    func nextOutput() async throws -> WatchOutput? {
        if !script.isEmpty { return script.removeFirst() }
        if let error = errorAfterScript { throw error }
        if let after = outputsEndAfter {
            let end = ContinuousClock.now + after
            while ContinuousClock.now < end { await Task.yield() }
            return nil
        }
        if outputsEnd { return nil }
        while true {
            try Task.checkCancellation()
            try await Task.sleep(for: .milliseconds(5))
        }
    }
    func nextEvent() async throws -> PlayerEvent? {
        if events.isEmpty {
            while true {
                try Task.checkCancellation()
                try await Task.sleep(for: .milliseconds(5))
            }
        }
        return events.removeFirst()
    }
    func tracksSelected() async throws -> Int { selected }
    func reportDecodeError(trackID: UInt64, generation: UInt64) async throws {
        decodeErrors += 1
    }
    func close() async { closes += 1 }
}

@MainActor
final class ScriptedWatchEndpoint: WatchEndpoint {
    let player: ScriptedWatchPlayer
    private(set) var closes = 0
    init(player: ScriptedWatchPlayer) { self.player = player }
    var negotiatedVersion: String? { "draft16" }
    func established(within deadline: Duration) async throws {}
    func attachPlayer(_ configuration: MediaPlayer.Configuration) async throws -> any WatchPlayer {
        player
    }
    func close() async { closes += 1 }
}

@MainActor
final class WatchTests: XCTestCase {

    private func model(with endpoint: ScriptedWatchEndpoint) -> PlayerModel {
        let m = PlayerModel()
        m.urlText = "moqt://relay.example:4443"
        m.namespaceText = "live/cam1"
        m.connector = WatchConnector { _ in endpoint }
        return m
    }

    // MARK: The LOC framing rule

    func testLeadingFourByteStartCodeIsAnnexB() {
        let unit = Data([0, 0, 0, 1, 0x65, 0x88])
        XCTAssertEqual(PlayerModel.locFraming(of: unit, hasVideoConfig: true), .annexB)
        XCTAssertEqual(PlayerModel.locFraming(of: unit, hasVideoConfig: false), .annexB)
    }

    /// A 4-byte NAL length of 256-511 begins 00 00 01: with a Video Config
    /// a 3-byte start code is not allowed (LOC-04 §2.1.4), so it is a length.
    func testLengthsOf256To511AreLengthsWithVideoConfig() {
        for length in [256, 300, 511] {
            var unit = Data([0, 0, UInt8(length >> 8), UInt8(length & 0xFF)])
            unit.append(Data(repeating: 0x41, count: length))
            XCTAssertEqual(PlayerModel.locFraming(of: unit, hasVideoConfig: true),
                           .lengthPrefixed, "length \(length)")
        }
    }

    func testThreeByteStartCodeOnlyWithoutVideoConfig() {
        let unit = Data([0, 0, 1, 0x65, 0x88, 0x84])
        XCTAssertEqual(PlayerModel.locFraming(of: unit, hasVideoConfig: false), .annexB)
        XCTAssertEqual(PlayerModel.locFraming(of: unit, hasVideoConfig: true), .lengthPrefixed)
    }

    func testOrdinaryLengthPrefixStaysLengthPrefixed() {
        let unit = Data([0, 0, 0, 2, 0x09, 0x10])
        XCTAssertEqual(PlayerModel.locFraming(of: unit, hasVideoConfig: true), .lengthPrefixed)
        XCTAssertEqual(PlayerModel.locFraming(of: Data([1, 2]), hasVideoConfig: false),
                       .lengthPrefixed)
    }

    // MARK: How a watch ends

    func testTrackEndsCountTracksAndFailures() {
        var ends = PlayerModel.TrackEnds()
        ends.end(1, cause: .peerEndOfTrack)
        ends.end(1, cause: .peerEndOfTrack)
        XCTAssertEqual(ends.count, 1, "a repeated END is one track")
        XCTAssertNil(ends.failure)
        ends.end(2, cause: .localFailure)
        XCTAssertEqual(ends.failure, .local)
        ends.end(3, cause: .fatal)
        ends.end(4, cause: .localFailure)
        XCTAssertEqual(ends.failure, .fatal, "FATAL stands above a local failure")
        XCTAssertEqual(ends.count, 4)
    }

    /// Outputs that end leave a finished watch with the transport still up:
    /// the watch reports ended and closes its player and endpoint, once each.
    func testFiniteMediaFinishesAndCleansUpOnce() async {
        let player = ScriptedWatchPlayer()
        let endpoint = ScriptedWatchEndpoint(player: player)
        let m = model(with: endpoint)
        m.connect()
        await m.watchCompleted()
        XCTAssertEqual(m.state, .ended)
        XCTAssertFalse(m.isRunning)
        XCTAssertEqual(player.closes, 1)
        XCTAssertEqual(endpoint.closes, 1)
        XCTAssertEqual(m.negotiatedVersion, "draft16")
    }

    /// A catalog with nothing selectable ends the watch with its own
    /// failure, which the outputs' end does not overwrite; cleanup runs once.
    func testNoPlayableTracksSurvivesTheUnwind() async {
        let player = ScriptedWatchPlayer()
        player.events = [.ready]
        player.selected = 0
        player.outputsEnd = false
        let endpoint = ScriptedWatchEndpoint(player: player)
        let m = model(with: endpoint)
        m.connect()
        await m.watchCompleted()
        guard case .failed(let message) = m.state else {
            return XCTFail("expected .failed, got \(m.state)")
        }
        XCTAssertTrue(message.contains("No playable track"))
        XCTAssertEqual(player.closes, 1)
        XCTAssertEqual(endpoint.closes, 1)
    }

    /// The same, when the outputs also end on their own just after: the
    /// watch's first terminal phase stands.
    func testNoPlayableTracksStandsWhenOutputsEndToo() async {
        let player = ScriptedWatchPlayer()
        player.events = [.ready]
        player.selected = 0
        player.outputsEndAfter = .milliseconds(100)
        let endpoint = ScriptedWatchEndpoint(player: player)
        let m = model(with: endpoint)
        m.connect()
        await m.watchCompleted()
        guard case .failed(let message) = m.state else {
            return XCTFail("expected .failed, got \(m.state)")
        }
        XCTAssertTrue(message.contains("No playable track"))
        XCTAssertEqual(player.closes, 1)
        XCTAssertEqual(endpoint.closes, 1)
    }

    /// Disconnecting cancels the watch: it unwinds and closes once each.
    func testDisconnectUnwindsOnce() async {
        let player = ScriptedWatchPlayer()
        player.outputsEnd = false
        let endpoint = ScriptedWatchEndpoint(player: player)
        let m = model(with: endpoint)
        m.connect()
        try? await Task.sleep(for: .milliseconds(50))
        m.disconnect()
        await m.watchCompleted()
        XCTAssertEqual(m.state, .idle)
        XCTAssertEqual(player.closes, 1)
        XCTAssertEqual(endpoint.closes, 1)
    }

    // MARK: A renderer that fails while waited on

    #if canImport(CoreMedia) && canImport(AVFoundation)
    @MainActor
    final class ScriptedRenderer: SampleRenderer {
        var isReadyForMoreMediaData = false
        var hasFailed = false
        private(set) var enqueued = 0
        func enqueue(_ sample: CMSampleBuffer) { enqueued += 1 }
        func flush() {}
    }

    private func emptySample() throws -> CMSampleBuffer {
        var sample: CMSampleBuffer?
        let status = CMSampleBufferCreate(
            allocator: kCFAllocatorDefault, dataBuffer: nil, dataReady: true,
            makeDataReadyCallback: nil, refcon: nil, formatDescription: nil,
            sampleCount: 0, sampleTimingEntryCount: 0, sampleTimingArray: nil,
            sampleSizeEntryCount: 0, sampleSizeArray: nil, sampleBufferOut: &sample)
        XCTAssertEqual(status, noErr)
        return try XCTUnwrap(sample)
    }

    /// The wait ends when the renderer fails, never ready again, and the
    /// sample is not received.
    func testFailureDuringTheCapacityWaitEndsIt() async throws {
        let renderer = ScriptedRenderer()
        let sample = try emptySample()
        Task { @MainActor in
            try? await Task.sleep(for: .milliseconds(30))
            renderer.hasFailed = true
        }
        let wait = Task { @MainActor in await enqueueWhenReady(sample, into: renderer) }
        let watchdog = Task { @MainActor in
            try? await Task.sleep(for: .seconds(2))
            wait.cancel()                     /* a wait that never ends fails here */
        }
        let outcome = await wait.value
        watchdog.cancel()
        XCTAssertEqual(outcome, .failed)
        XCTAssertEqual(renderer.enqueued, 0)
    }

    func testReadyRendererTakesTheSample() async throws {
        let renderer = ScriptedRenderer()
        renderer.isReadyForMoreMediaData = true
        let outcome = await enqueueWhenReady(try emptySample(), into: renderer)
        XCTAssertEqual(outcome, .enqueued)
        XCTAssertEqual(renderer.enqueued, 1)
    }

    func testCancelledWaitTakesNothing() async throws {
        let renderer = ScriptedRenderer()
        let sample = try emptySample()
        let task = Task { @MainActor in await enqueueWhenReady(sample, into: renderer) }
        try await Task.sleep(for: .milliseconds(20))
        task.cancel()
        let outcome = await task.value
        XCTAssertEqual(outcome, .cancelled)
        XCTAssertEqual(renderer.enqueued, 0)
    }
    // MARK: How finite media finishes

    /// A sink whose presentation settles when the test says so.
    @MainActor
    final class ScriptedSink: RenderSink {
        let video = ScriptedRenderer()
        var audio: ScriptedRenderer?
        private(set) var stops = 0
        private(set) var settledThrough: Duration?
        private(set) var settleCalls = 0
        var settled = false
        /// What settle returns once released.
        var outcome = SettleOutcome.presented
        init() { video.isReadyForMoreMediaData = true }
        func renderer(video isVideo: Bool) -> SampleRenderer? { isVideo ? video : audio }
        func attachAudio() {
            let a = ScriptedRenderer()
            a.isReadyForMoreMediaData = true
            audio = a
        }
        func apply(_ timeline: WatchTimeline) {}
        func drives(_ timelineID: UInt32) -> Bool { timelineID == 1 }
        /// Waits until `settled`, cancellation, or two seconds: a watch that
        /// wrongly waits here fails its row instead of hanging it.
        func settle(through end: Duration) async -> SettleOutcome {
            settleCalls += 1
            settledThrough = end
            let ceiling = ContinuousClock.now + .seconds(2)
            while !settled && !Task.isCancelled && ContinuousClock.now < ceiling {
                try? await Task.sleep(for: .milliseconds(5))
            }
            return Task.isCancelled ? .cancelled : outcome
        }
        func stop() { stops += 1 }
    }

    private func format() throws -> CMFormatDescription {
        var description: CMFormatDescription?
        XCTAssertEqual(CMVideoFormatDescriptionCreate(
            allocator: kCFAllocatorDefault, codecType: kCMVideoCodecType_H264,
            width: 640, height: 360, extensions: nil, formatDescriptionOut: &description), noErr)
        return try XCTUnwrap(description)
    }

    /// CONFIGURE, TIMELINE, a video and an audio UNIT presenting ahead of
    /// now, and both tracks' END{cause}.
    private func finiteScript(cause: PlayerEndCause) throws -> [WatchOutput] {
        let f = try format()
        let sample = try emptySample()
        func unit(_ track: UInt64, _ data: Data, by: Duration) -> WatchOutput {
            .unit(WatchUnit(trackID: track, timelineID: 1, configurationGeneration: 1,
                            data: data, presentedBy: by,
                            makeSampleBuffer: { _, _ in sample }))
        }
        return [
            .configure(WatchConfigure(trackID: 1, trackName: "video", mediaType: .video,
                                      hasCodecConfig: true, makeFormatDescription: { f })),
            .configure(WatchConfigure(trackID: 2, trackName: "audio", mediaType: .audio,
                                      hasCodecConfig: true, makeFormatDescription: { f })),
            .timeline(WatchTimeline(timelineID: 1, anchorMedia: .zero,
                                    anchorLocalMicroseconds: 0, rate: 1)),
            unit(1, Data([0, 0, 0, 2, 0x09, 0x10]), by: .milliseconds(333)),
            unit(2, Data([1, 2, 3]), by: .milliseconds(320)),
            .end(trackID: 1, cause: cause),
            .end(trackID: 2, cause: cause),
        ]
    }

    private func finiteModel(_ player: ScriptedWatchPlayer, _ endpoint: ScriptedWatchEndpoint,
                             _ sink: ScriptedSink) -> PlayerModel {
        let m = model(with: endpoint)
        m.makeSink = { _ in sink }
        return m
    }

    /// The ENDs of finite media do not end the watch: until the sink has
    /// presented through the last unit, the renderers keep running, the
    /// transport stays up and the watch is not ended. Then it ends, and
    /// closes its sink, player and endpoint once each.
    func testFiniteMediaPresentsBeforeTeardown() async throws {
        let player = ScriptedWatchPlayer()
        player.selected = 2
        player.outputsEnd = false
        player.script = try finiteScript(cause: .peerEndOfTrack)
        let endpoint = ScriptedWatchEndpoint(player: player)
        let sink = ScriptedSink()
        let m = finiteModel(player, endpoint, sink)
        m.connect()
        for _ in 0..<200 where sink.settleCalls == 0 { try await Task.sleep(for: .milliseconds(5)) }
        XCTAssertEqual(sink.settleCalls, 1)
        XCTAssertEqual(sink.settledThrough, .milliseconds(333))
        try await Task.sleep(for: .milliseconds(50))
        XCTAssertEqual(m.state, .playing)
        XCTAssertEqual(sink.stops, 0)
        XCTAssertEqual(player.closes, 0)
        XCTAssertEqual(endpoint.closes, 0)
        sink.settled = true
        await m.watchCompleted()
        XCTAssertEqual(m.state, .ended)
        XCTAssertEqual(sink.stops, 1)
        XCTAssertEqual(player.closes, 1)
        XCTAssertEqual(endpoint.closes, 1)
    }

    /// With B-frames the last unit in decode order is not the last to
    /// present: finite media settles through the latest presentation end
    /// of any unit, here P's 99 + 33 ms, not the last B's 66 + 33 ms.
    func testReorderedTailSettlesThroughTheLatestPresentation() async throws {
        let f = try format()
        let sample = try emptySample()
        func unit(_ object: UInt8, by: Duration) -> WatchOutput {
            .unit(WatchUnit(trackID: 1, timelineID: 1, configurationGeneration: 1,
                            data: Data([0, 0, 0, 1, 0x65, object]), presentedBy: by,
                            makeSampleBuffer: { _, _ in sample }))
        }
        let player = ScriptedWatchPlayer()
        player.selected = 1
        player.outputsEnd = false
        player.script = [
            .configure(WatchConfigure(trackID: 1, trackName: "video", mediaType: .video,
                                      hasCodecConfig: true, makeFormatDescription: { f })),
            .timeline(WatchTimeline(timelineID: 1, anchorMedia: .zero,
                                    anchorLocalMicroseconds: 0, rate: 1)),
            unit(0, by: .milliseconds(33)),
            unit(1, by: .milliseconds(132)),
            unit(2, by: .milliseconds(66)),
            unit(3, by: .milliseconds(99)),
            .end(trackID: 1, cause: .peerEndOfTrack),
        ]
        let endpoint = ScriptedWatchEndpoint(player: player)
        let sink = ScriptedSink()
        let m = finiteModel(player, endpoint, sink)
        m.connect()
        for _ in 0..<200 where sink.settleCalls == 0 { try await Task.sleep(for: .milliseconds(5)) }
        XCTAssertEqual(sink.settleCalls, 1)
        XCTAssertEqual(sink.settledThrough, .milliseconds(132))
        XCTAssertEqual(sink.video.enqueued, 4)
        sink.settled = true
        await m.watchCompleted()
        XCTAssertEqual(m.state, .ended)
    }

    /// Disconnecting while the tail presents cancels the wait: the watch
    /// unwinds to idle and closes everything once.
    func testDisconnectDuringPresentationUnwindsOnce() async throws {
        let player = ScriptedWatchPlayer()
        player.selected = 2
        player.outputsEnd = false
        player.script = try finiteScript(cause: .peerEndOfTrack)
        let endpoint = ScriptedWatchEndpoint(player: player)
        let sink = ScriptedSink()
        let m = finiteModel(player, endpoint, sink)
        m.connect()
        for _ in 0..<200 where sink.settleCalls == 0 { try await Task.sleep(for: .milliseconds(5)) }
        m.disconnect()
        await m.watchCompleted()
        XCTAssertEqual(m.state, .idle)
        XCTAssertEqual(sink.stops, 1)
        XCTAssertEqual(player.closes, 1)
        XCTAssertEqual(endpoint.closes, 1)
    }

    /// A FATAL END is a failure, with the fatal error the outputs report
    /// next, and nothing waits on presentation.
    func testFatalEndFailsWithTheFatalError() async throws {
        let player = ScriptedWatchPlayer()
        player.selected = 2
        player.outputsEnd = false
        player.script = try finiteScript(cause: .fatal)
        player.errorAfterScript = MoQServiceError.fatal(code: 7)
        let endpoint = ScriptedWatchEndpoint(player: player)
        let sink = ScriptedSink()
        let m = finiteModel(player, endpoint, sink)
        m.connect()
        await m.watchCompleted()
        guard case .failed(let message) = m.state else {
            return XCTFail("expected .failed, got \(m.state)")
        }
        XCTAssertTrue(message.contains("7"), "the staged fatal code: \(message)")
        XCTAssertEqual(sink.settleCalls, 0)
        XCTAssertEqual(sink.stops, 1)
        XCTAssertEqual(player.closes, 1)
        XCTAssertEqual(endpoint.closes, 1)
    }

    /// A local failure's END fails the watch too; it is not completion.
    func testLocalFailureEndFails() async throws {
        let player = ScriptedWatchPlayer()
        player.selected = 2
        player.outputsEnd = false
        player.script = try finiteScript(cause: .localFailure)
        let endpoint = ScriptedWatchEndpoint(player: player)
        let sink = ScriptedSink()
        let m = finiteModel(player, endpoint, sink)
        m.connect()
        await m.watchCompleted()
        guard case .failed(let message) = m.state else {
            return XCTFail("expected .failed, got \(m.state)")
        }
        XCTAssertTrue(message.contains("could not be received"))
        XCTAssertEqual(sink.settleCalls, 0)
        XCTAssertEqual(player.closes, 1)
        XCTAssertEqual(endpoint.closes, 1)
    }
    /// A renderer that fails while the tail presents fails the watch: not
    /// ended, and everything closes once.
    func testRendererFailureDuringPresentationFails() async throws {
        let player = ScriptedWatchPlayer()
        player.selected = 2
        player.outputsEnd = false
        player.script = try finiteScript(cause: .peerEndOfTrack)
        let endpoint = ScriptedWatchEndpoint(player: player)
        let sink = ScriptedSink()
        sink.outcome = .failed
        let m = finiteModel(player, endpoint, sink)
        m.connect()
        for _ in 0..<200 where sink.settleCalls == 0 { try await Task.sleep(for: .milliseconds(5)) }
        XCTAssertEqual(m.state, .playing)
        sink.settled = true
        await m.watchCompleted()
        guard case .failed(let message) = m.state else {
            return XCTFail("expected .failed, got \(m.state)")
        }
        XCTAssertTrue(message.contains("could not be presented"))
        XCTAssertEqual(sink.stops, 1)
        XCTAssertEqual(player.closes, 1)
        XCTAssertEqual(endpoint.closes, 1)
    }

    /// A clock that never reaches the end is not completion either.
    func testUnpresentedTailFails() async throws {
        let player = ScriptedWatchPlayer()
        player.selected = 2
        player.outputsEnd = false
        player.script = try finiteScript(cause: .peerEndOfTrack)
        let endpoint = ScriptedWatchEndpoint(player: player)
        let sink = ScriptedSink()
        sink.outcome = .notPresented
        sink.settled = true
        let m = finiteModel(player, endpoint, sink)
        m.connect()
        await m.watchCompleted()
        guard case .failed = m.state else { return XCTFail("expected .failed, got \(m.state)") }
        XCTAssertEqual(sink.stops, 1)
        XCTAssertEqual(player.closes, 1)
    }

    // MARK: Settlement itself

    @MainActor
    final class ScriptedClock: PresentationClock {
        var mediaTime: Duration?
        var due: UInt64?
        func hostTime(presenting end: Duration) -> UInt64? { due }
    }

    private func hostNow() -> UInt64 { DispatchTime.now().uptimeNanoseconds / 1_000 }

    func testSettlePresentsWhenTheClockGetsThere() async {
        let clock = ScriptedClock()
        clock.due = hostNow() + 20_000
        let renderer = ScriptedRenderer()
        Task { @MainActor in
            try? await Task.sleep(for: .milliseconds(20))
            clock.mediaTime = .milliseconds(100)
        }
        let outcome = await settlePresentation(through: .milliseconds(100), clock: clock,
                                               renderers: [renderer])
        XCTAssertEqual(outcome, .presented)
    }

    /// A mapping that is not yet due is not presentation, however the clock
    /// reads: a zero-duration tail at media time zero, scheduled 300 ms
    /// ahead, against a clock still reading its old zero. Settlement waits
    /// for the mapped host time, then succeeds.
    func testSettleWaitsForAPendingMapping() async {
        let clock = ScriptedClock()
        clock.mediaTime = .zero                      /* stale: the old timebase */
        let due = hostNow() + 300_000
        clock.due = due
        let outcome = await settlePresentation(through: .zero, clock: clock,
                                               renderers: [ScriptedRenderer()])
        XCTAssertEqual(outcome, .presented)
        XCTAssertGreaterThanOrEqual(hostNow(), due)
    }

    func testSettleFailsWhenARendererFailsDuringTheWait() async {
        let clock = ScriptedClock()
        clock.mediaTime = .zero
        clock.due = hostNow() + 2_000_000
        let renderer = ScriptedRenderer()
        Task { @MainActor in
            try? await Task.sleep(for: .milliseconds(30))
            renderer.hasFailed = true
        }
        let start = ContinuousClock.now
        let outcome = await settlePresentation(through: .seconds(2), clock: clock,
                                               renderers: [renderer])
        XCTAssertEqual(outcome, .failed)
        XCTAssertLessThan(ContinuousClock.now - start, .seconds(1))
    }

    func testSettleIsNotPresentedWhenTheClockStalls() async {
        let clock = ScriptedClock()
        clock.mediaTime = .zero
        clock.due = hostNow() + 20_000
        let start = ContinuousClock.now
        let outcome = await settlePresentation(through: .milliseconds(100), clock: clock,
                                               renderers: [ScriptedRenderer()],
                                               grace: .milliseconds(30))
        XCTAssertEqual(outcome, .notPresented)
        XCTAssertLessThan(ContinuousClock.now - start, .seconds(1))
        clock.due = nil                              /* no timeline at all */
        let none = await settlePresentation(through: .milliseconds(100), clock: clock,
                                            renderers: [])
        XCTAssertEqual(none, .notPresented)
    }

    func testSettleEndsOnCancellation() async {
        let clock = ScriptedClock()
        clock.mediaTime = .zero
        clock.due = hostNow() + 5_000_000
        let task = Task { @MainActor in
            await settlePresentation(through: .seconds(5), clock: clock,
                                     renderers: [ScriptedRenderer()])
        }
        try? await Task.sleep(for: .milliseconds(30))
        task.cancel()
        let outcome = await task.value
        XCTAssertEqual(outcome, .cancelled)
    }

    /// 10 ms of silent 48 kHz mono 16-bit PCM at media time zero.
    private func shortPCM() throws -> CMSampleBuffer {
        var asbd = AudioStreamBasicDescription(
            mSampleRate: 48_000, mFormatID: kAudioFormatLinearPCM,
            mFormatFlags: kLinearPCMFormatFlagIsSignedInteger | kLinearPCMFormatFlagIsPacked,
            mBytesPerPacket: 2, mFramesPerPacket: 1, mBytesPerFrame: 2,
            mChannelsPerFrame: 1, mBitsPerChannel: 16, mReserved: 0)
        var format: CMAudioFormatDescription?
        XCTAssertEqual(CMAudioFormatDescriptionCreate(
            allocator: kCFAllocatorDefault, asbd: &asbd, layoutSize: 0, layout: nil,
            magicCookieSize: 0, magicCookie: nil, extensions: nil,
            formatDescriptionOut: &format), noErr)
        let frames = 480
        var block: CMBlockBuffer?
        XCTAssertEqual(CMBlockBufferCreateWithMemoryBlock(
            allocator: kCFAllocatorDefault, memoryBlock: nil, blockLength: frames * 2,
            blockAllocator: kCFAllocatorDefault, customBlockSource: nil, offsetToData: 0,
            dataLength: frames * 2, flags: kCMBlockBufferAssureMemoryNowFlag,
            blockBufferOut: &block), noErr)
        let blockBuffer = try XCTUnwrap(block)
        XCTAssertEqual(CMBlockBufferFillDataBytes(with: 0, blockBuffer: blockBuffer,
                                                  offsetIntoDestination: 0,
                                                  dataLength: frames * 2), noErr)
        var sample: CMSampleBuffer?
        XCTAssertEqual(CMAudioSampleBufferCreateReadyWithPacketDescriptions(
            allocator: kCFAllocatorDefault, dataBuffer: blockBuffer,
            formatDescription: try XCTUnwrap(format), sampleCount: frames,
            presentationTimeStamp: .zero, packetDescriptions: nil,
            sampleBufferOut: &sample), noErr)
        return try XCTUnwrap(sample)
    }

    /// The real pipeline maps media time onto host time from its timeline:
    /// anchorMedia at anchorLocal, at the timeline's rate.
    func testRealPipelineMapping() {
        let pipeline = RenderPipeline(displayLayer: AVSampleBufferDisplayLayer())
        let anchor = hostNow() + 300_000
        XCTAssertNil(pipeline.hostTime(presenting: .zero), "no timeline yet")
        pipeline.apply(WatchTimeline(timelineID: 1, anchorMedia: .milliseconds(40),
                                     anchorLocalMicroseconds: anchor, rate: 1))
        XCTAssertEqual(pipeline.hostTime(presenting: .milliseconds(40)), anchor)
        XCTAssertEqual(pipeline.hostTime(presenting: .milliseconds(140)), anchor + 100_000)
        pipeline.stop()
        let fast = RenderPipeline(displayLayer: AVSampleBufferDisplayLayer())
        fast.apply(WatchTimeline(timelineID: 1, anchorMedia: .zero,
                                 anchorLocalMicroseconds: anchor, rate: 2))
        XCTAssertEqual(fast.hostTime(presenting: .milliseconds(100)), anchor + 50_000)
        fast.stop()
    }

    /// A zero-duration tail at the anchor, scheduled 300 ms ahead on the
    /// real pipeline: settlement does not succeed before it is due.
    func testRealPipelineWaitsForTheMappedTail() async {
        let pipeline = RenderPipeline(displayLayer: AVSampleBufferDisplayLayer())
        let anchor = hostNow() + 300_000
        pipeline.apply(WatchTimeline(timelineID: 1, anchorMedia: .zero,
                                     anchorLocalMicroseconds: anchor, rate: 1))
        let outcome = await pipeline.settle(through: .zero)
        XCTAssertEqual(outcome, .presented)
        XCTAssertGreaterThanOrEqual(hostNow(), anchor)
        pipeline.stop()
    }

    /// The real pipeline presents a finite tail when an attached audio
    /// renderer holds less than its preroll and gets nothing more (a
    /// stream's last 10 ms of audio): the clock reaches the end.
    func testRealPipelineStartsWithoutPreroll() async throws {
        let pipeline = RenderPipeline(displayLayer: AVSampleBufferDisplayLayer())
        pipeline.attachAudio()
        let audio = try XCTUnwrap(pipeline.renderer(video: false))
        audio.enqueue(try shortPCM())
        pipeline.apply(WatchTimeline(timelineID: 1, anchorMedia: .zero,
                                     anchorLocalMicroseconds: hostNow() + 20_000, rate: 1))
        let outcome = await pipeline.settle(through: .milliseconds(80))
        XCTAssertEqual(outcome, .presented)
        pipeline.stop()
    }
    #endif
}
