import CMoQLOC
import CMoQCore
import Foundation
import MoQ

/// Lossless LOC companion metadata of one object's properties
/// (moq_loc_parse_metadata_ex / moq_loc_encode_metadata_ex). Without a
/// ``LOCProfile`` the profile follows the wire version: LOC-01 on drafts 16
/// and 18, LOC-04 on draft 22; name ``LOCProfile/loc05`` to read or write the
/// Codec String. Values are raw: timestamp ticks and timescale units, the
/// marking payload's 1...4 bytes verbatim, the audio level byte with its
/// voice-activity bit. The configs and the Codec String are copied; the Codec
/// String is kept as its exact bytes (``codecString`` is the text).
public struct LOCMetadata: Sendable, Hashable {
    public var timestamp: UInt64?
    public var timescale: UInt64?
    public var marking: Data?
    public var audioLevel: UInt8?
    public var videoConfig: Data?
    public var audioConfig: Data?
    /// LOC-05 only: the Codec String's exact bytes.
    public var codecStringBytes: Data?

    /// The Codec String as text; setting it stores its UTF-8 bytes.
    public var codecString: String? {
        get { codecStringBytes.flatMap { String(data: $0, encoding: .utf8) } }
        set { codecStringBytes = newValue.map { Data($0.utf8) } }
    }

    public init(timestamp: UInt64? = nil, timescale: UInt64? = nil, marking: Data? = nil,
                audioLevel: UInt8? = nil, videoConfig: Data? = nil, audioConfig: Data? = nil,
                codecStringBytes: Data? = nil) {
        self.timestamp = timestamp
        self.timescale = timescale
        self.marking = marking
        self.audioLevel = audioLevel
        self.videoConfig = videoConfig
        self.audioConfig = audioConfig
        self.codecStringBytes = codecStringBytes
    }
}

/// An explicitly named LOC revision.
public enum LOCProfile: Sendable, Hashable {
    /// draft-ietf-moq-loc-01, on drafts 16 and 18.
    case loc01
    /// draft-ietf-moq-loc-04, on draft 22.
    case loc04
    /// draft-ietf-moq-loc-05 (LOC-04 plus the Codec String), on draft 22.
    case loc05

    var cValue: moq_loc_profile_t {
        switch self {
        case .loc01: MOQ_LOC_PROFILE_01
        case .loc04: MOQ_LOC_PROFILE_04
        case .loc05: MOQ_LOC_PROFILE_05
        }
    }
}

/// The presence bits of moq_loc_metadata_t (MOQ_LOC_METADATA_HAS_*, whose
/// UINT32_C spelling Swift does not import).
private enum LOCPresence {
    static let timestamp: UInt32 = 0x01
    static let timescale: UInt32 = 0x02
    static let marking: UInt32 = 0x04
    static let audio_level: UInt32 = 0x08
    static let video_config: UInt32 = 0x10
    static let audio_config: UInt32 = 0x20
    /// MOQ_LOC_EXT_HAS_CODEC_STRING of moq_loc_ext_t.
    static let ext_codec_string: UInt32 = 0x01
}

public enum LOCMetadataCodec {

    private static func profile(_ wireVersion: WireVersion) throws -> moq_loc_profile_t {
        let p = moq_loc_profile_for_version(moq_version_t(rawValue: wireVersion.rawValue))
        guard p.rawValue != 0 else { throw MoQError.invalidArgument }
        return p
    }

    /// Parse the LOC metadata of an object's properties, with the wire
    /// version's compatibility profile.
    public static func parse(wireVersion: WireVersion, properties: Data) throws -> LOCMetadata {
        try parse(wireVersion: wireVersion, profile: profile(wireVersion),
                  properties: properties)
    }

    /// Parse the LOC metadata of an object's properties with a named profile.
    public static func parse(wireVersion: WireVersion, profile: LOCProfile,
                             properties: Data) throws -> LOCMetadata {
        try parse(wireVersion: wireVersion, profile: profile.cValue, properties: properties)
    }

    private static func parse(wireVersion: WireVersion, profile p: moq_loc_profile_t,
                              properties: Data) throws -> LOCMetadata {
        return try properties.withUnsafeBytes { raw -> LOCMetadata in
            let bytes = moq_bytes_t(data: raw.bindMemory(to: UInt8.self).baseAddress,
                                    len: raw.count)
            var m = moq_loc_metadata_t()
            moq_loc_metadata_init_sized(&m, MemoryLayout<moq_loc_metadata_t>.size)
            var e = moq_loc_ext_t()
            moq_loc_ext_init_sized(&e, MemoryLayout<moq_loc_ext_t>.size)
            try MoQError.check(moq_loc_parse_metadata_ex(
                moq_version_t(rawValue: wireVersion.rawValue), p, bytes, &m,
                MemoryLayout<moq_loc_metadata_t>.size, &e,
                MemoryLayout<moq_loc_ext_t>.size))
            func span(_ b: moq_bytes_t) -> Data {
                b.data.map { Data(bytes: $0, count: b.len) } ?? Data()
            }
            var out = LOCMetadata()
            if m.present & LOCPresence.timestamp != 0 { out.timestamp = m.timestamp }
            if m.present & LOCPresence.timescale != 0 { out.timescale = m.timescale }
            if m.present & LOCPresence.marking != 0 {
                let mk = [m.marking.0, m.marking.1, m.marking.2, m.marking.3]
                out.marking = Data(mk.prefix(Int(m.marking_len)))
            }
            if m.present & LOCPresence.audio_level != 0 { out.audioLevel = m.audio_level }
            if m.present & LOCPresence.video_config != 0 { out.videoConfig = span(m.video_config) }
            if m.present & LOCPresence.audio_config != 0 { out.audioConfig = span(m.audio_config) }
            if e.present & LOCPresence.ext_codec_string != 0 {
                out.codecStringBytes = span(e.codec_string)
            }
            return out
        }
    }

    /// Encode LOC metadata as object properties, with the wire version's
    /// compatibility profile (which has no Codec String: one is refused).
    public static func encode(wireVersion: WireVersion, metadata: LOCMetadata) throws -> Data {
        try encode(wireVersion: wireVersion, profile: profile(wireVersion), metadata: metadata)
    }

    /// Encode LOC metadata as object properties with a named profile.
    public static func encode(wireVersion: WireVersion, profile: LOCProfile,
                              metadata: LOCMetadata) throws -> Data {
        try encode(wireVersion: wireVersion, profile: profile.cValue, metadata: metadata)
    }

    private static func encode(wireVersion: WireVersion, profile p: moq_loc_profile_t,
                               metadata: LOCMetadata) throws -> Data {
        let marking = [UInt8](metadata.marking ?? Data())
        guard marking.count <= 4 else { throw MoQError.invalidArgument }
        let video = [UInt8](metadata.videoConfig ?? Data())
        let audio = [UInt8](metadata.audioConfig ?? Data())
        let codec = [UInt8](metadata.codecStringBytes ?? Data())
        var m = moq_loc_metadata_t()
        moq_loc_metadata_init_sized(&m, MemoryLayout<moq_loc_metadata_t>.size)
        if let t = metadata.timestamp { m.present |= LOCPresence.timestamp; m.timestamp = t }
        if let t = metadata.timescale { m.present |= LOCPresence.timescale; m.timescale = t }
        if metadata.marking != nil {
            m.present |= LOCPresence.marking
            m.marking_len = UInt8(marking.count)
            withUnsafeMutableBytes(of: &m.marking) { dst in
                for (i, b) in marking.enumerated() { dst[i] = b }
            }
        }
        if let a = metadata.audioLevel { m.present |= LOCPresence.audio_level; m.audio_level = a }
        var props: OpaquePointer?
        try video.withUnsafeBufferPointer { vp in
            try audio.withUnsafeBufferPointer { ap in
                if metadata.videoConfig != nil {
                    m.present |= LOCPresence.video_config
                    m.video_config = moq_bytes_t(data: vp.baseAddress, len: vp.count)
                }
                if metadata.audioConfig != nil {
                    m.present |= LOCPresence.audio_config
                    m.audio_config = moq_bytes_t(data: ap.baseAddress, len: ap.count)
                }
                try codec.withUnsafeBufferPointer { cp in
                    var e = moq_loc_ext_t()
                    moq_loc_ext_init_sized(&e, MemoryLayout<moq_loc_ext_t>.size)
                    if metadata.codecStringBytes != nil {
                        e.present |= LOCPresence.ext_codec_string
                        e.codec_string = moq_bytes_t(data: cp.baseAddress, len: cp.count)
                    }
                    let version = moq_version_t(rawValue: wireVersion.rawValue)
                    if metadata.codecStringBytes != nil {
                        try MoQError.check(moq_loc_encode_metadata_ex(
                            moq_alloc_default(), version, p, &m, &e, &props))
                    } else {
                        try MoQError.check(moq_loc_encode_metadata_ex(
                            moq_alloc_default(), version, p, &m, nil, &props))
                    }
                }
            }
        }
        guard let props else { return Data() }
        defer { moq_rcbuf_decref(props) }
        guard let d = moq_rcbuf_data(props) else { return Data() }
        return Data(bytes: d, count: moq_rcbuf_len(props))
    }
}
