import CoreMedia
import Foundation
import MoQSwiftPlayerDecoders

// Bounded subprocess probe for the findings 6 & 7 security regression (0259).
// One case per process, named by argv[1], run against the REAL extracted
// decoder. A survivor prints a structured line and exits 0; a case that TRAPS
// aborts (the parent observes the uncaught signal). Kept as a child even though
// the fixed finding 6 does not trap on this toolchain, because a pre-fix build
// or another supported Apple runtime still might. Unbuffered so markers flush
// before any abort.

setbuf(stdout, nil)

/// Classify one video-decoder operation's outcome into a stable category the
/// parent asserts against. `malformedAVCC` is the pre-CoreMedia rejection the
/// fix emits; `formatDescriptionFailed`/`blockBufferFailed` are the wrong-layer
/// CoreMedia failures the pre-fix code produced; `ok` is a clean return.
func classifyVideo(_ op: () throws -> Void) -> String {
    do {
        try op()
        return "CAT:ok"
    } catch let e as VideoDecoderError {
        switch e {
        case .malformedAVCC(let d):          return "CAT:malformedAVCC:\(d)"
        case .malformedInput(let d):         return "CAT:malformedInput:\(d)"
        case .formatDescriptionFailed(let s): return "CAT:formatDescriptionFailed:\(s)"
        case .blockBufferFailed(let s):       return "CAT:blockBufferFailed:\(s)"
        case .sampleBufferFailed(let s):      return "CAT:sampleBufferFailed:\(s)"
        }
    } catch {
        return "CAT:other:\(error)"
    }
}

/// avcC (AVCDecoderConfigurationRecord) with the requested SPS/PPS shapes.
func makeAVCC(spsLens: [Int], ppsLens: [Int]) -> Data {
    var d = Data([0x01, 0x42, 0x00, 0x1e, 0xff])
    d.append(0xE0 | UInt8(spsLens.count))
    for len in spsLens {
        d.append(UInt8((len >> 8) & 0xFF)); d.append(UInt8(len & 0xFF))
        d.append(contentsOf: [UInt8](repeating: 0, count: len))
    }
    d.append(UInt8(ppsLens.count))
    for len in ppsLens {
        d.append(UInt8((len >> 8) & 0xFF)); d.append(UInt8(len & 0xFF))
        d.append(contentsOf: [UInt8](repeating: 0, count: len))
    }
    return d
}

// A real encoder SPS/PPS pair (baseline) for the behavior-neutral controls.
let validSPS: [UInt8] = [
    0x67, 0x42, 0xc0, 0x1e, 0xd9, 0x00, 0xb0, 0x7b,
    0x60, 0x22, 0x00, 0x00, 0x03, 0x00, 0x02, 0x00,
    0x00, 0x03, 0x00, 0x64, 0x1e, 0x28, 0x53, 0x80,
]
let validPPS: [UInt8] = [0x68, 0xce, 0x06, 0xe2]

func validAVCC() -> Data {
    var d = Data([0x01, validSPS[1], validSPS[2], validSPS[3], 0xff, 0xE1])
    d.append(UInt8((validSPS.count >> 8) & 0xFF)); d.append(UInt8(validSPS.count & 0xFF))
    d.append(contentsOf: validSPS)
    d.append(0x01)
    d.append(UInt8((validPPS.count >> 8) & 0xFF)); d.append(UInt8(validPPS.count & 0xFF))
    d.append(contentsOf: validPPS)
    return d
}

func done(_ s: String) -> Never { print(s); exit(0) }

enum ProbeCookieError: Error { case oversizedCodecConfig }

/// Test-only convention modeling the INTENDED product signature transition for
/// finding 7: `buildAACMagicCookie` must reject an oversized ASC (returning a
/// distinct oversized-codec-config error) instead of overflowing `UInt8`. Until
/// the product adopts that shape this wrapper calls the real non-throwing
/// builder directly -- so today an oversized ASC still TRAPS here and the
/// wrapper never returns. When the builder becomes throwing, replace this body
/// with `try AudioDecoder().buildAACMagicCookie(asc: asc)` and map the oversized
/// case to `ProbeCookieError.oversizedCodecConfig`. Declared `throws` so the
/// child's `do/catch` classification compiles cleanly against both shapes.
func buildCookieOrReject(_ asc: Data) throws -> Data {
    do {
        return try AudioDecoder().buildAACMagicCookie(asc: asc)
    } catch AACConverterError.oversizedCodecConfig {
        throw ProbeCookieError.oversizedCodecConfig
    }
}

let name = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : ""

switch name {

// ---- Finding 6: avcC parser / sample-buffer input validation ----

case "f6_valid":
    done(classifyVideo { _ = try createH264FormatDescription(avccData: validAVCC()) })

case "f6_zero_sps_count":
    done(classifyVideo {
        _ = try createH264FormatDescription(
            avccData: makeAVCC(spsLens: [], ppsLens: [validPPS.count]))
    })

case "f6_zero_pps_count":
    done(classifyVideo {
        _ = try createH264FormatDescription(
            avccData: makeAVCC(spsLens: [validSPS.count], ppsLens: []))
    })

case "f6_zero_len_sps":
    done(classifyVideo {
        _ = try createH264FormatDescription(avccData: makeAVCC(spsLens: [0], ppsLens: []))
    })

case "f6_zero_len_pps":
    done(classifyVideo {
        _ = try createH264FormatDescription(avccData: makeAVCC(spsLens: [], ppsLens: [0]))
    })

case "f6_all_empty":
    done(classifyVideo {
        _ = try createH264FormatDescription(avccData: makeAVCC(spsLens: [0], ppsLens: [0]))
    })

case "f6_truncated":
    done(classifyVideo {
        _ = try createH264FormatDescription(
            avccData: Data([0x01, 0x42, 0x00, 0x1e, 0xff, 0xE1, 0x00]))
    })

case "f6_empty_access_unit":
    done(classifyVideo {
        let desc = try createH264FormatDescription(avccData: validAVCC())
        _ = try createSampleBuffer(
            data: Data(), formatDescription: desc,
            decodeTimeUS: 0, presentationTimeUS: 0, durationUS: 0, isKeyframe: true)
    })

case "f6_valid_sample":
    // Behavior-neutral control: a genuine non-empty access unit must build a
    // sample buffer cleanly through the same path -- so a repair cannot satisfy
    // the empty-AU row by rejecting every sample.
    done(classifyVideo {
        let desc = try createH264FormatDescription(avccData: validAVCC())
        _ = try createSampleBuffer(
            data: Data([0x00, 0x00, 0x00, 0x04, 0x65, 0x88, 0x84, 0x21]),
            formatDescription: desc,
            decodeTimeUS: 0, presentationTimeUS: 0, durationUS: 33_000, isKeyframe: true)
    })

// ---- Finding 7: AAC magic-cookie descriptor length ----

case "f7_valid":
    // Real 2-byte AAC-LC ASC -> well-formed cookie, no trap (control).
    if let cookie = try? AudioDecoder().buildAACMagicCookie(asc: Data([0x12, 0x10])) {
        done("OK:cookie-\(cookie.count)")
    } else {
        done("REJECTED:unexpected")
    }

case "f7_cookie_236":
    // Direct call to the REAL cookie builder with an oversized (236B) ASC.
    // Pre-fix `UInt8(esDescLen-2) == UInt8(256)` overflowed and TRAPPED, so the
    // wrapper never returned and the child aborted (the RED evidence). The fix
    // rejects the oversized ASC: the child prints `REJECTED:oversizedCodecConfig`.
    // A silent `BUILT:` (building or truncating an oversized cookie) would fail
    // the parent. No factory, no fabricated pointer -- this isolates the
    // builder's length handling.
    do {
        let cookie = try buildCookieOrReject(Data([UInt8](repeating: 0, count: 236)))
        done("BUILT:\(cookie.count)")
    } catch ProbeCookieError.oversizedCodecConfig {
        done("REJECTED:oversizedCodecConfig")
    } catch {
        done("REJECTED:\(error)")
    }

default:
    FileHandle.standardError.write(Data("unknown probe case: \(name)\n".utf8))
    exit(2)
}
