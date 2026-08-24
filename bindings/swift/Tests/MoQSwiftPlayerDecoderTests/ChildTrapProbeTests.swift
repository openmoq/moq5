import AudioToolbox
import Foundation
import Testing
@testable import MoQSwiftPlayerDecoders

/*
 * 0259 findings 6 & 7 security regression for the extracted MoQSwiftPlayer
 * decoders. Every malformed-input assertion pins the post-fix behavior; these
 * pass against the fix and failed RED against the pre-fix decoders. Green rows
 * are valid/guarded controls; child-process rows carry attribution of the
 * pre-fix trap / wrong-layer disposition.
 *
 * Finding 6 (MOQ-SWIFTPLAYER-EMPTY-DECODER-POINTER): the avcC parser flattened
 * its parameter sets and force-unwrapped `flatBuf.baseAddress!`; the sample
 * path force-unwrapped `buf.baseAddress!`. Two failure modes:
 *   1. a malformed-input / preflight defect -- empty and zero-count parameter
 *      sets, and an empty access unit, were NOT rejected before CoreMedia; they
 *      reached CoreMedia and surfaced a wrong-layer OSStatus
 *      (`formatDescriptionFailed`/`blockBufferFailed`) instead of a declared
 *      input error. The fix preflights and rejects them up front; and
 *   2. a latent optional force-unwrap hazard whose manifestation can vary
 *      across supported Apple toolchains/provenance (kept child-isolated).
 * The report's "deterministic macOS remote crash" is not reproduced on this
 * Apple toolchain (empty buffers gave non-NULL base addresses) and must be
 * corrected upstream; we do not fabricate a nil buffer or a trap.
 *
 * Finding 7 (MOQ-SWIFTPLAYER-AAC-LENGTH-TRAP): `buildAACMagicCookie` writes
 * each nested descriptor body via a direct `UInt8(...)`. The outer ES
 * descriptor body is `20 + ascLen`, the tightest one-byte field. Under the
 * ruled one-byte descriptor policy, ASC length 107 is the largest accepted
 * (body == 0x7f) and 108 the first rejected -- and the decoder must reject an
 * oversized ASC BEFORE `AudioConverterNew`, which the converter-factory seam
 * makes load-bearing (zero factory calls on rejection).
 */

@Suite("Player decoder trap probes (findings 6 & 7)")
struct ChildTrapProbeTests {

    // MARK: Child-process harness

    private enum Termination: Equatable {
        case exited(Int32)
        case signal(Int32)
    }

    private struct ProbeResult {
        let caseName: String
        let termination: Termination
        let stdout: String

        /// A Swift trap aborts via an uncaught signal (SIGILL/SIGTRAP on Darwin).
        var trapped: Bool {
            if case .signal = termination { return true }
            return false
        }
        /// The first stdout line (nil unless the child exited 0). Used for exact
        /// structured-marker matching so a noisy trailing line cannot sneak past.
        var firstLine: String? {
            guard case .exited(0) = termination else { return nil }
            return stdout.split(separator: "\n").first.map(String.init)
        }
        /// The decoder-error category the child classified (nil if it did not
        /// exit 0 with a `CAT:` line).
        var category: String? {
            guard let line = firstLine, line.hasPrefix("CAT:") else { return nil }
            let body = line.dropFirst("CAT:".count)
            return String(body.split(separator: ":", maxSplits: 1).first ?? "")
        }
    }

    /// The package root (nearest ancestor of this source file holding
    /// Package.swift), for locating SwiftPM build products deterministically.
    private func packageRoot() -> URL? {
        let fm = FileManager.default
        var dir = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<16 {
            if fm.fileExists(atPath: dir.appendingPathComponent("Package.swift").path) {
                return dir
            }
            let parent = dir.deletingLastPathComponent()
            if parent.path == dir.path { break }
            dir = parent
        }
        return nil
    }

    /// Locate the sibling child-probe executable across the ways a runner can
    /// expose its products directory. `MOQ_TRAP_PROBE` overrides for CI.
    private func probeURL() -> URL? {
        let name = "MoQPlayerTrapProbe"
        let fm = FileManager.default
        if let override = ProcessInfo.processInfo.environment["MOQ_TRAP_PROBE"],
           fm.isExecutableFile(atPath: override) {
            return URL(fileURLWithPath: override)
        }

        var dirs: [URL] = []
        if let root = packageRoot() {
            dirs.append(root.appendingPathComponent(".build/debug"))
            #if arch(arm64)
            dirs.append(root.appendingPathComponent(".build/arm64-apple-macosx/debug"))
            #else
            dirs.append(root.appendingPathComponent(".build/x86_64-apple-macosx/debug"))
            #endif
        }
        for bundle in Bundle.allBundles {
            dirs.append(bundle.bundleURL.deletingLastPathComponent())
        }
        dirs.append(Bundle.main.bundleURL.deletingLastPathComponent())
        let arg0 = URL(fileURLWithPath: CommandLine.arguments[0])
        dirs.append(arg0.deletingLastPathComponent())
        for path in dirs.map(\.path) + [arg0.path] {
            if let r = path.range(of: ".xctest") {
                let bundlePath = String(path[..<r.upperBound])
                dirs.append(URL(fileURLWithPath: bundlePath).deletingLastPathComponent())
            }
        }
        for dir in dirs {
            let candidate = dir.appendingPathComponent(name)
            if fm.isExecutableFile(atPath: candidate.path) { return candidate }
        }
        return nil
    }

    /// Run one probe case to completion. The child does one bounded operation,
    /// so it terminates on its own; the watchdog is a safety net, not an
    /// elapsed-time oracle.
    private func runProbe(_ caseName: String,
                          ceilingSeconds: TimeInterval = 60) -> ProbeResult {
        guard let exe = probeURL() else {
            Issue.record("could not locate MoQPlayerTrapProbe; set MOQ_TRAP_PROBE")
            return ProbeResult(caseName: caseName, termination: .exited(-1), stdout: "")
        }
        let process = Process()
        process.executableURL = exe
        process.arguments = [caseName]
        let outPipe = Pipe()
        process.standardOutput = outPipe
        process.standardError = Pipe()
        do { try process.run() } catch {
            Issue.record("failed to launch probe \(exe.path): \(error)")
            return ProbeResult(caseName: caseName, termination: .exited(-1), stdout: "")
        }
        let watchdog = DispatchWorkItem { if process.isRunning { process.terminate() } }
        DispatchQueue.global().asyncAfter(deadline: .now() + ceilingSeconds, execute: watchdog)
        let data = outPipe.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        watchdog.cancel()
        let termination: Termination = process.terminationReason == .uncaughtSignal
            ? .signal(process.terminationStatus) : .exited(process.terminationStatus)
        return ProbeResult(caseName: caseName, termination: termination,
                           stdout: String(decoding: data, as: UTF8.self))
    }

    /// The pre-CoreMedia rejection category for malformed avcC.
    private let malformedInputCategory = "malformedAVCC"
    /// The category the fix emits for an empty access unit -- a distinct
    /// pre-CoreMedia input rejection (`VideoDecoderError.malformedInput`).
    private let emptyAccessUnitCategory = "malformedInput"
    /// The category the fix emits for an oversized AAC codec config, distinct
    /// from a converter-creation failure (`AACConverterError.oversizedCodecConfig`).
    private let oversizedConfigCategory = "oversizedCodecConfig"

    /// Classify a thrown AAC converter-setup error into its category. The
    /// oversized-config rejection is distinct from a converter-creation failure.
    private func aacCategory(_ error: any Error) -> String {
        if let e = error as? AACConverterError {
            switch e {
            case .converterCreationFailed: return "converterCreationFailed"
            case .oversizedCodecConfig: return "oversizedCodecConfig"
            }
        }
        return "\(error)"
    }

    // MARK: Finding 6 -- reject malformed input before CoreMedia

    /// Green control: a genuine avcC decodes cleanly through the extraction.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Valid avcC decodes (behavior-neutral control)")
    func validAVCCDecodes() {
        let r = runProbe("f6_valid")
        #expect(!r.trapped)
        #expect(r.category == "ok", "got \(r.category ?? "nil") / \(r.stdout)")
    }

    /// Green control: a genuine NON-empty access unit builds a sample buffer
    /// cleanly through the same `createSampleBuffer` path -- so the fix cannot
    /// satisfy the empty-access-unit rejection below by rejecting every sample.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Valid non-empty access unit builds a sample buffer (control)")
    func validSampleBuilds() {
        let r = runProbe("f6_valid_sample")
        #expect(!r.trapped)
        #expect(r.category == "ok", "got \(r.category ?? "nil") / \(r.stdout)")
    }

    /// Green control: a truncated avcC is already rejected as `malformedAVCC`
    /// before CoreMedia -- the shape every malformed case below must match.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Truncated avcC is rejected as malformedAVCC (control)")
    func truncatedAVCCRejected() {
        let r = runProbe("f6_truncated")
        #expect(!r.trapped)
        #expect(r.category == malformedInputCategory)
    }

    /// These malformed parameter-set shapes are rejected BEFORE CoreMedia
    /// (`malformedAVCC`). Pre-fix they reached CoreMedia and surfaced
    /// `formatDescriptionFailed` -- a wrong-layer failure -- so each failed RED.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Malformed parameter sets are rejected before CoreMedia",
          arguments: ["f6_zero_sps_count", "f6_zero_pps_count",
                      "f6_zero_len_sps", "f6_zero_len_pps", "f6_all_empty"])
    func malformedParamSetsRejectedPreCoreMedia(_ probeCase: String) {
        let r = runProbe(probeCase)
        #expect(!r.trapped, "\(probeCase): must not trap; got \(r.termination)")
        #expect(r.category == malformedInputCategory,
                "\(probeCase): want pre-CoreMedia \(malformedInputCategory), got \(r.category ?? "nil") (wrong layer)")
    }

    /// An empty access unit is rejected as a distinct pre-CoreMedia input
    /// error, never a CoreMedia `blockBufferFailed` and never a trap. Pre-fix it
    /// surfaced `blockBufferFailed` (wrong layer), so this failed RED.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Empty access unit is a pre-CoreMedia input rejection")
    func emptyAccessUnitRejectedPreCoreMedia() {
        let r = runProbe("f6_empty_access_unit")
        #expect(!r.trapped)
        #expect(r.category != "blockBufferFailed",
                "wrong-layer CoreMedia failure; want a pre-CoreMedia input rejection")
        #expect(r.category == emptyAccessUnitCategory,
                "want declared future \(emptyAccessUnitCategory), got \(r.category ?? "nil")")
    }

    // MARK: Finding 7 -- reject oversized ASC before AudioConverterNew

    /// Green control (wire proof): ASC length 107 is accepted and the real
    /// outer ES descriptor body byte is exactly the one-byte limit 0x7f, with
    /// the inner config body still under it -- derived from the nested lengths
    /// (`20 + ascLen`), not hardcoded. Independent of the converter path.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("ASC 107 accepted; outer descriptor body is exactly 0x7f (control)")
    func asc107AcceptedAtOneByteLimit() throws {
        let ascMax = 107
        let cookie = try AudioDecoder().buildAACMagicCookie(
            asc: Data([UInt8](repeating: 0, count: ascMax)))
        #expect(20 + ascMax == 0x7f)                 // outer body == the limit
        #expect(Int(cookie[1]) == 20 + ascMax)       // real cookie agrees (0x7f)
        #expect(Int(cookie[6]) == 15 + ascMax)       // inner config body still < limit
    }

    /// Green control (converter path): an under-limit 107-byte ASC is NOT
    /// pre-rejected -- it reaches the converter factory. Driven deterministically
    /// with a recording factory that returns a controlled failure and NULL
    /// output (no dependence on the host CoreAudio accepting the ASBD, and no
    /// fabricated converter pointer): the factory must run exactly once and the
    /// error is `converterCreationFailed`. This blocks a "reject everything in
    /// makeAACConverter" repair, which would make the oversized REDs vacuous.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Under-limit 107-byte ASC reaches the converter factory (control)")
    func asc107ReachesConverterFactory() {
        var factoryCalls = 0
        let failingFactory: AudioConverterFactory = { _, _, out in
            factoryCalls += 1
            out.pointee = nil                        // controlled failure, NULL output
            return OSStatus(-1)
        }
        var category = "none"
        do {
            _ = try AudioDecoder().makeAACConverter(
                formatID: kAudioFormatMPEG4AAC, sampleRate: 44100,
                channels: 2, codecConfig: Data([UInt8](repeating: 0, count: 107)),
                factory: failingFactory)
        } catch {
            category = aacCategory(error)
        }
        #expect(factoryCalls == 1, "under-limit ASC must reach the factory")
        #expect(category == "converterCreationFailed",
                "under-limit ASC must not be pre-rejected; got \(category)")
    }

    /// An oversized ASC is rejected BEFORE `AudioConverterNew` (zero factory
    /// calls) and with a DISTINCT oversized-config category, not a
    /// converter-creation failure. Driven in-process with a recording factory
    /// that returns a controlled failure and a NULL converter -- so this path
    /// never traps and no fabricated pointer is used. Pre-fix both expectations
    /// failed RED: the factory ran once and the category was
    /// `converterCreationFailed`. Covers 108 (first over-limit) and 236.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Oversized ASC is rejected before AudioConverterNew, distinct category",
          arguments: [108, 236])
    func oversizedASCRejectedBeforeConverter(_ ascLength: Int) {
        var factoryCalls = 0
        let failingFactory: AudioConverterFactory = { _, _, out in
            factoryCalls += 1
            out.pointee = nil                        // controlled failure, NULL output
            return OSStatus(-1)
        }
        var category = "none"
        do {
            let conv = try AudioDecoder().makeAACConverter(
                formatID: kAudioFormatMPEG4AAC, sampleRate: 44100,
                channels: 2, codecConfig: Data([UInt8](repeating: 0, count: ascLength)),
                factory: failingFactory)
            AudioConverterDispose(conv)              // not expected on either path
        } catch {
            category = aacCategory(error)
        }
        #expect(factoryCalls == 0,
                "ASC(\(ascLength)) must be validated BEFORE AudioConverterNew; factory ran \(factoryCalls)x")
        #expect(category == oversizedConfigCategory,
                "ASC(\(ascLength)) want distinct \(oversizedConfigCategory), got \(category)")
    }

    /// Attribution: the real cookie builder rejects an oversized ASC normally
    /// -- neither trapping nor silently building/truncating a cookie. Run in the
    /// child because pre-fix `UInt8(256)` overflowed and aborted. The parent
    /// requires BOTH survival AND an explicit oversized-config rejection, so a
    /// regression that silently returned a `BUILT:` cookie would still fail.
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cookie builder rejects an oversized ASC (not trap, not silent build)")
    func cookieBuilderRejectsOversizedNotTrap() {
        let r = runProbe("f7_cookie_236")
        #expect(!r.trapped, "oversized ASC must be rejected, not trapped; got \(r.termination)")
        #expect(r.firstLine == "REJECTED:\(oversizedConfigCategory)",
                "cookie builder must report an oversized rejection, not a silent build; got \(r.firstLine ?? "nil") / \(r.stdout)")
    }
}
