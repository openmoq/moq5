import CMoQCMAF
import CMoQCore
import Foundation
import MoQ

// MARK: - Codec Kind

public enum CMAFCodecKind: Sendable, Hashable {
    case unknown
    case avc
    case hevc
    case av1
    case aac
    case opus
}

// MARK: - Init Info

/// Parsed CMAF init segment (ftyp+moov).
public struct CMAFInitInfo: Sendable, Equatable {
    public let timescale: UInt32
    public let codecKind: CMAFCodecKind
    public let width: UInt32
    public let height: UInt32
    public let samplerate: UInt32
    public let channelCount: UInt32
    /// Codec-specific configuration bytes, copied from the init segment.
    public let codecConfig: Data
    /// The sample entry's format, `(a<<24)|(b<<16)|(c<<8)|d`: a clear
    /// entry's own type (`avc1`, `avc3`, `hvc1`, `hev1`, ...), an encrypted
    /// entry's `frma` original format. Any value, including zero, is reported
    /// as read; nil when the library did not report it.
    public let sampleEntry: UInt32?
}

// MARK: - Parse Error

public enum CMAFParseError: Error, Sendable, Equatable {
    case invalidArgument
    case malformed
}

// MARK: - Parser

public enum CMAFParser {

    /// Parse a CMAF init segment from raw bytes.
    /// The codecConfig is copied into owned Data so the input
    /// can be released immediately after this call returns.
    public static func parseInit(_ data: Data) throws -> CMAFInitInfo {
        try data.withUnsafeBytes { buf in
            let ptr = buf.baseAddress?.assumingMemoryBound(to: UInt8.self)
            let bytes = moq_bytes_t(data: ptr, len: buf.count)

            var info = moq_cmaf_init_info_t()
            let size = MemoryLayout<moq_cmaf_init_info_t>.size
            moq_cmaf_init_info_init_sized(&info, size)

            let rc = moq_cmaf_parse_init_sized(bytes, &info, size)
            if rc == MOQ_ERR_INVAL { throw CMAFParseError.invalidArgument }
            if rc < 0 { throw CMAFParseError.malformed }

            let codecKind: CMAFCodecKind
            switch info.codec_kind {
            case MOQ_CMAF_CODEC_AVC:  codecKind = .avc
            case MOQ_CMAF_CODEC_HEVC: codecKind = .hevc
            case MOQ_CMAF_CODEC_AV1:  codecKind = .av1
            case MOQ_CMAF_CODEC_AAC:  codecKind = .aac
            case MOQ_CMAF_CODEC_OPUS: codecKind = .opus
            default:                  codecKind = .unknown
            }

            let config: Data
            if let p = info.codec_config.data, info.codec_config.len > 0 {
                config = Data(bytes: p, count: info.codec_config.len)
            } else {
                config = Data()
            }

            return CMAFInitInfo(
                timescale: info.timescale,
                codecKind: codecKind,
                width: info.width,
                height: info.height,
                samplerate: info.samplerate,
                channelCount: info.channel_count,
                codecConfig: config,
                sampleEntry: Int(info.struct_size) >= MemoryLayout<moq_cmaf_init_info_t>
                    .offset(of: \.sample_entry)! + MemoryLayout<UInt32>.size
                    ? info.sample_entry : nil
            )
        }
    }

    /// Parse a CMAF init segment from a Buffer.
    public static func parseInit(_ buffer: Buffer) throws -> CMAFInitInfo {
        try buffer.withUnsafeBytes { buf in
            try parseInit(Data(buf))
        }
    }
}

// MARK: - MSFTrack convenience

extension MSFTrack {
    /// Decode initData from base64, then parse the CMAF init segment.
    public func parseCMAFInitInfo() throws -> CMAFInitInfo {
        let data = try decodedInitData()
        return try CMAFParser.parseInit(data)
    }
}
