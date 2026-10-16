import CoreMedia
import Foundation
import MoQSwiftPlayerDecoders

// Bounded subprocess probe for the player decoder security regression
// (MOQ-SWIFTPLAYER-EMPTY-DECODER-POINTER, MOQ-SWIFTPLAYER-AAC-LENGTH-TRAP).
// One case per process, named by argv[1], run against the REAL extracted
// decoder. A survivor prints a structured line and exits 0; a case that TRAPS
// aborts (the parent observes the uncaught signal). The avcC cases run in a
// child even though malformed avcC input does not trap on this toolchain,
// because an unguarded decoder or another supported Apple runtime might.
// Unbuffered so markers flush before any abort.

setbuf(stdout, nil)

/// Classify one video-decoder operation's outcome into a stable category the
/// parent asserts against. `malformedAVCC` is the pre-CoreMedia rejection the
/// decoder emits; `formatDescriptionFailed`/`blockBufferFailed` are wrong-layer
/// CoreMedia failures an unguarded decoder produces; `ok` is a clean return.
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

/// Calls the real cookie builder and maps its oversized-ASC rejection to
/// `ProbeCookieError.oversizedCodecConfig`: `buildAACMagicCookie` must reject an
/// oversized ASC with a distinct oversized-codec-config error instead of
/// overflowing `UInt8`.
func buildCookieOrReject(_ asc: Data) throws -> Data {
    do {
        return try AudioDecoder().buildAACMagicCookie(asc: asc)
    } catch AACConverterError.oversizedCodecConfig {
        throw ProbeCookieError.oversizedCodecConfig
    }
}

let name = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : ""

switch name {

// ---- avcC parser / sample-buffer input validation ----

case "video_valid":
    done(classifyVideo { _ = try createH264FormatDescription(avccData: validAVCC()) })

case "video_zero_sps_count":
    done(classifyVideo {
        _ = try createH264FormatDescription(
            avccData: makeAVCC(spsLens: [], ppsLens: [validPPS.count]))
    })

case "video_zero_pps_count":
    done(classifyVideo {
        _ = try createH264FormatDescription(
            avccData: makeAVCC(spsLens: [validSPS.count], ppsLens: []))
    })

case "video_zero_len_sps":
    done(classifyVideo {
        _ = try createH264FormatDescription(avccData: makeAVCC(spsLens: [0], ppsLens: []))
    })

case "video_zero_len_pps":
    done(classifyVideo {
        _ = try createH264FormatDescription(avccData: makeAVCC(spsLens: [], ppsLens: [0]))
    })

case "video_all_empty":
    done(classifyVideo {
        _ = try createH264FormatDescription(avccData: makeAVCC(spsLens: [0], ppsLens: [0]))
    })

case "video_truncated":
    done(classifyVideo {
        _ = try createH264FormatDescription(
            avccData: Data([0x01, 0x42, 0x00, 0x1e, 0xff, 0xE1, 0x00]))
    })

case "video_empty_access_unit":
    done(classifyVideo {
        let desc = try createH264FormatDescription(avccData: validAVCC())
        _ = try createSampleBuffer(
            data: Data(), formatDescription: desc,
            decodeTimeUS: 0, presentationTimeUS: 0, durationUS: 0, isKeyframe: true)
    })

case "video_valid_sample":
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

// ---- AAC magic-cookie descriptor length ----

case "aac_valid":
    // Real 2-byte AAC-LC ASC -> well-formed cookie, no trap (control).
    if let cookie = try? AudioDecoder().buildAACMagicCookie(asc: Data([0x12, 0x10])) {
        done("OK:cookie-\(cookie.count)")
    } else {
        done("REJECTED:unexpected")
    }

case "aac_cookie_236":
    // Direct call to the REAL cookie builder with an oversized (236B) ASC.
    // An unguarded `UInt8(esDescLen-2) == UInt8(256)` overflows and TRAPS, so
    // the child aborts. The builder rejects the oversized ASC instead: the
    // child prints `REJECTED:oversizedCodecConfig`.
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
