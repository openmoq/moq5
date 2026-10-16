import Foundation
import Testing

/*
 * MOQ-SWIFT-VIDEO-DIMENSION-TRAP security regression, service lane.
 *
 * The vulnerability: `makeFormatDescription(codecConfig:)` ->
 * `makeVideoDescription` converting with an unchecked `Int32(width)` /
 * `Int32(height)` TRAPS for a dimension above `Int32.max`. An out-of-range
 * dimension must instead exit NORMALLY with a declared typed rejection, so the
 * conversion is range-checked first; a trapping conversion fails these tests.
 * Representable-but-unrealistic dimensions (e.g. `Int32.max`) are handled by
 * CoreMedia and stay separate from range validation.
 *
 * An unchecked range violation aborts the process, so each case runs in the
 * bounded `MoQServiceTrapProbe` child (service stack only); the parent
 * classifies the termination and the structured outcome line.
 */

@Suite("Service video-dimension conversion")
struct ServiceDimensionTests {

    private enum Termination: Equatable {
        case exited(Int32)
        case signal(Int32)
    }

    private struct ProbeResult {
        let caseName: String
        let termination: Termination
        let stdout: String

        var trapped: Bool {
            if case .signal = termination { return true }
            return false
        }
        /// The structured outcome line: `OK` or `THROWN:<kind>` (nil if the
        /// child did not exit 0 with such a line, e.g. on a trap).
        var outcome: String? {
            guard case .exited(0) = termination,
                  let line = stdout.split(separator: "\n").first else { return nil }
            return String(line)
        }
    }

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

    private func probeURL() -> URL? {
        let name = "MoQServiceTrapProbe"
        let fm = FileManager.default
        if let override = ProcessInfo.processInfo.environment["MOQ_SERVICE_TRAP_PROBE"],
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

    private func runProbe(_ caseName: String,
                          ceilingSeconds: TimeInterval = 60) -> ProbeResult {
        guard let exe = probeURL() else {
            Issue.record("could not locate MoQServiceTrapProbe; set MOQ_SERVICE_TRAP_PROBE")
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

    /// The declared typed rejection for an invalid dimension (a pre-CoreMedia
    /// validation error), distinct from `internalError` (CoreMedia's status).
    private let rangeRejection = "THROWN:invalidArgument"

    // MARK: Controls

    /// Valid dimensions convert cleanly through the real service path.
    @Test("Valid dimensions convert (control)")
    func validDimensions() {
        let r = runProbe("dim_valid")
        #expect(!r.trapped)
        #expect(r.outcome == "OK", "got \(r.outcome ?? "nil") / \(r.stdout)")
    }

    /// Zero / negative dimensions are already rejected with a typed error before
    /// the `Int32` conversion -- the guard the range check must extend.
    @Test("Zero and negative dimensions are typed-rejected (control)",
          arguments: ["dim_zero", "dim_negative"])
    func nonPositiveDimensionsRejected(_ probeCase: String) {
        let r = runProbe(probeCase)
        #expect(!r.trapped)
        #expect(r.outcome == rangeRejection, "\(probeCase): got \(r.outcome ?? "nil")")
    }

    /// A representable-but-unrealistic `Int32.max` -- for EITHER dimension -- is
    /// handled by CoreMedia, NOT range-rejected. Proves range validation stays
    /// separate from CoreMedia's own acceptance of large dimensions, for both
    /// width and height, so a repair cannot reject representable values.
    @Test("Int32.max width/height is CoreMedia-handled, not range-rejected (control)",
          arguments: ["dim_width_int32max", "dim_height_int32max"])
    func representableMaxDimensionNotRangeRejected(_ probeCase: String) {
        let r = runProbe(probeCase)
        #expect(!r.trapped)
        #expect(r.outcome != rangeRejection,
                "\(probeCase): a representable dimension must not be range-rejected; got \(r.outcome ?? "nil")")
    }

    // MARK: out-of-range dimensions must reject, not trap

    /// A dimension above `Int32.max` -- in EITHER width or height -- exits
    /// normally with the declared typed rejection. An unchecked `Int32(width)` /
    /// `Int32(height)` overflows and TRAPS, failing both `!trapped` and the
    /// typed-rejection outcome. Width and height are independently
    /// load-bearing (a width-only repair leaves the height cases failing).
    @Test("Out-of-range width/height is typed-rejected, not trapped",
          arguments: ["dim_width_int32max_plus1", "dim_width_uint32max",
                      "dim_height_int32max_plus1", "dim_height_uint32max"])
    func outOfRangeDimensionRejectedNotTrapped(_ probeCase: String) {
        let r = runProbe(probeCase)
        #expect(!r.trapped, "\(probeCase): out-of-range dimension must be rejected, not trapped; got \(r.termination)")
        #expect(r.outcome == rangeRejection,
                "\(probeCase): want typed \(rangeRejection), got \(r.outcome ?? "nil")")
    }
}
