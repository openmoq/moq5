import CoreMedia
import Foundation
import MoQService
import MoQServiceCore

// Bounded subprocess probe for the finding 5 security regression (0259,
// MOQ-SWIFT-VIDEO-DIMENSION-TRAP). One case per process, named by argv[1],
// driving the REAL public service conversion `makeFormatDescription(codecConfig:)`
// -> `makeVideoDescription`. Pre-fix, `Int32(width)` / `Int32(height)` trapped
// for a dimension above `Int32.max`; that trap aborts the process, so a case
// must never run inside `swift test`. A survivor prints a structured line and
// exits 0:
//
//   * a typed rejection  -> "THROWN:<kind>"
//   * a clean conversion -> "OK"
//   * a trap             -> the process aborts (parent observes the signal).

setbuf(stdout, nil)

func done(_ s: String) -> Never { print(s); exit(0) }

/// A video track whose codec/mediaType reach the H.264 dimension path; the
/// avcC content is irrelevant to the `Int32(width)` conversion, which precedes
/// (and never reaches) `CMVideoFormatDescriptionCreate` on the trap path.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func videoTrack(width: Int, height: Int) -> TrackDescription {
    var td = TrackDescription(name: "video", mediaType: .video, packaging: .cmaf)
    td.codec = "avc1.42001e"
    td.width = width
    td.height = height
    return td
}

let avcC = Data([
    0x01, 0x42, 0x00, 0x1e, 0xff, 0xe1, 0x00, 0x04,
    0x67, 0x42, 0x00, 0x1e, 0x01, 0x00, 0x04, 0x68, 0xce, 0x06, 0xe2,
])

/// Drive the real conversion and classify its outcome. `internalError` is a
/// CoreMedia status-bearing failure (the representable-but-unrealistic path);
/// `invalidArgument` is a pre-CoreMedia validation rejection (the fix's
/// disposition for an out-of-range dimension).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func classify(width: Int, height: Int) -> String {
    let td = videoTrack(width: width, height: height)
    do {
        _ = try td.makeFormatDescription(codecConfig: avcC)
        return "OK"
    } catch let e as MoQServiceError {
        switch e {
        case .invalidArgument:      return "THROWN:invalidArgument"
        case .unsupported:          return "THROWN:unsupported"
        case .internalError(let s): return "THROWN:internalError:\(s)"
        case .wrongState:           return "THROWN:wrongState"
        case .interrupted:          return "THROWN:interrupted"
        case .closed:               return "THROWN:closed"
        case .fatal(let c):         return "THROWN:fatal:\(c)"
        default:                    return "THROWN:otherKind"
        }
    } catch {
        return "THROWN:other"
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func run(_ name: String) -> Never {
    let int32Max = Int(Int32.max)          // 2147483647
    let uint32Max = Int(UInt32.max)        // 4294967295
    switch name {
    case "f5_valid":                  done(classify(width: 1280, height: 720))
    case "f5_zero":                   done(classify(width: 0, height: 720))
    case "f5_negative":               done(classify(width: -1, height: 720))
    // width oversized (height fixed valid)
    case "f5_width_int32max":         done(classify(width: int32Max, height: 720))
    case "f5_width_int32max_plus1":   done(classify(width: int32Max + 1, height: 720))
    case "f5_width_uint32max":        done(classify(width: uint32Max, height: 720))
    // height oversized (width fixed valid) -- `Int32(height)` traps independently
    case "f5_height_int32max":        done(classify(width: 1280, height: int32Max))
    case "f5_height_int32max_plus1":  done(classify(width: 1280, height: int32Max + 1))
    case "f5_height_uint32max":       done(classify(width: 1280, height: uint32Max))
    default:
        FileHandle.standardError.write(Data("unknown probe case: \(name)\n".utf8))
        exit(2)
    }
}

let name = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : ""
if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *) {
    run(name)
} else {
    FileHandle.standardError.write(Data("unsupported OS\n".utf8))
    exit(2)
}
