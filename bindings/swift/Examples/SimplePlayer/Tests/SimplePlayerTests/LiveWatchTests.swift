// One watch of a real finite publication, through the real PlayerModel,
// MediaPlayer and AVFoundation render pipeline. Opt-in: it needs a relay and
// a publisher, so it skips unless SIMPLEPLAYER_LIVE_URL and
// SIMPLEPLAYER_LIVE_NAMESPACE are set. Optional:
//   SIMPLEPLAYER_LIVE_VERSION  16, 18 or 22 (default: the automatic offer)
//   SIMPLEPLAYER_LIVE_TIMEOUT  seconds to wait for the watch to end (90)
//   SIMPLEPLAYER_LIVE_SAMPLES  the exact number of video samples expected
//   SIMPLEPLAYER_LIVE_CA       a CA bundle for picoquic's certificate check,
//                              for a build whose TLS has no default roots
//   SIMPLEPLAYER_LIVE_PTS      a file of the source's video presentation
//                              times (us, one per line) in its decode order:
//                              the enqueued samples, in enqueue order, must
//                              be the reference's from the join to its last
//                              frame, each presenting at its own time
//                              shifted by one offset (within 2 us). With
//                              reordered (B) frames this checks both the
//                              decode order and each frame's presentation
//                              time; it says nothing about when each frame
//                              was displayed.
//   SIMPLEPLAYER_LIVE_PTS_ANCHOR  the publisher's timestamp anchor (us): the
//                              offset, which places a live join; without it
//                              the watch must start at the first frame
//   SIMPLEPLAYER_LIVE_PTS_OUT  write the enqueued presentation times (us, one
//                              per line, enqueue order) to this file
//
// The display layer is hosted in a small window, so its frames are actually
// composited; every 100 ms the test asks the video renderer for the frame it
// is displaying. The watch must end as PlayerModel ends finite media: every
// track ended and what was enqueued has presented on the render clock.

#if canImport(AppKit) && canImport(AVFoundation)
import AppKit
import AVFoundation
import XCTest
@testable import SimplePlayer

@MainActor
final class LiveWatchTests: XCTestCase {
    func testLiveFiniteWatchPresents() async throws {
        let env = ProcessInfo.processInfo.environment
        guard let url = env["SIMPLEPLAYER_LIVE_URL"], let namespace = env["SIMPLEPLAYER_LIVE_NAMESPACE"]
        else { throw XCTSkip("set SIMPLEPLAYER_LIVE_URL and SIMPLEPLAYER_LIVE_NAMESPACE") }
        let timeout = Double(env["SIMPLEPLAYER_LIVE_TIMEOUT"] ?? "") ?? 90

        let model = PlayerModel()
        model.urlText = url
        model.namespaceText = namespace
        model.backend = .picoquic
        model.caFilePath = env["SIMPLEPLAYER_LIVE_CA"] ?? ""
        switch env["SIMPLEPLAYER_LIVE_VERSION"] {
        case "16": model.version = .draft16
        case "18": model.version = .draft18
        case "22": model.version = .draft22
        default: model.version = .automatic
        }

        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 320, height: 180),
                              styleMask: [.borderless], backing: .buffered, defer: false)
        let view = NSView(frame: window.contentLayoutRect)
        view.wantsLayer = true
        model.displayLayer.frame = view.bounds
        view.layer?.addSublayer(model.displayLayer)
        window.contentView = view
        window.orderFrontRegardless()
        defer { window.orderOut(nil) }

        var enqueuedPTS: [Int64] = []
        model.onVideoSampleEnqueued = { sample in
            let pts = CMSampleBufferGetPresentationTimeStamp(sample)
            enqueuedPTS.append(Int64((pts.seconds * 1_000_000).rounded()))
        }

        model.connect()
        let start = ContinuousClock.now
        var probes = 0, displayed = 0
        var states: [PlayerState] = []
        var playingAt: Duration?
        while ContinuousClock.now - start < .seconds(timeout) {
            if states.last != model.state { states.append(model.state) }
            if model.state == .playing, playingAt == nil { playingAt = ContinuousClock.now - start }
            if case .failed = model.state { break }
            if model.state == .ended { break }
            if #available(macOS 14.4, *), model.state == .playing {
                probes += 1
                if model.displayLayer.sampleBufferRenderer.displayedPixelBuffer() != nil {
                    displayed += 1
                }
            }
            try await Task.sleep(for: .milliseconds(100))
        }
        let elapsed = ContinuousClock.now - start
        print("LIVE: states=\(states) samples=\(model.sampleCount) track=\(model.trackName ?? "-") "
              + "negotiated=\(model.negotiatedVersion ?? "-") playingAt=\(playingAt.map { "\($0)" } ?? "-") "
              + "displayedProbes=\(displayed)/\(probes) elapsed=\(elapsed)")
        let finalState = model.state
        model.disconnect()
        await model.watchCompleted()

        XCTAssertEqual(finalState, .ended, "the finite watch ends with its tail presented")
        XCTAssertGreaterThan(model.sampleCount, 0)
        if let expected = env["SIMPLEPLAYER_LIVE_SAMPLES"].flatMap(Int.init) {
            XCTAssertEqual(model.sampleCount, expected, "every video unit was enqueued")
        }
        if #available(macOS 14.4, *) {
            XCTAssertGreaterThan(displayed, 0, "a decoded frame was displayed")
        }
        if let out = env["SIMPLEPLAYER_LIVE_PTS_OUT"] {
            try enqueuedPTS.map(String.init).joined(separator: "\n")
                .write(toFile: out, atomically: true, encoding: .utf8)
        }
        if let path = env["SIMPLEPLAYER_LIVE_PTS"] {
            let reference = try String(contentsOfFile: path, encoding: .utf8)
                .split(whereSeparator: \.isNewline).compactMap { Int64($0) }
            var join: Int?
            var offset: Int64 = 0
            if let first = enqueuedPTS.first {
                if let anchor = env["SIMPLEPLAYER_LIVE_PTS_ANCHOR"].flatMap(Int64.init) {
                    offset = anchor
                    join = reference.firstIndex { abs(first - anchor - $0) <= 2 }
                } else if let ref0 = reference.first {
                    offset = first - ref0
                    join = 0
                }
            }
            XCTAssertNotNil(join, "the first enqueued sample is a reference frame")
            if let join {
                let expected = Array(reference[join...])
                XCTAssertEqual(enqueuedPTS.count, expected.count,
                               "every reference frame from the join was enqueued")
                let off = zip(enqueuedPTS, expected).enumerated()
                    .first { abs($0.element.0 - offset - $0.element.1) > 2 }
                XCTAssertNil(off.map { "sample \($0.offset): \($0.element.0 - offset) != \($0.element.1)" },
                             "each enqueued sample presents at its reference time, in decode order")
            }
            let reordered = zip(enqueuedPTS, enqueuedPTS.dropFirst()).contains { $0.1 < $0.0 }
            print("LIVE: enqueued=\(enqueuedPTS.count) reference=\(reference.count) "
                  + "join=\(join.map(String.init) ?? "-") reordered=\(reordered)")
        }
    }
}
#endif
