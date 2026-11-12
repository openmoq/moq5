import Foundation

/// The complete wire identity observed for a received object. A nil identity
/// on MediaObject means unobserved; a nil subgroup here means none on the wire.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct MediaObjectIdentity: Sendable, Hashable {
    public let groupID: UInt64
    public let subgroupID: UInt64?
    public let objectID: UInt64
    /// Effective priority, including an inherited native default.
    public let publisherPriority: UInt8

    public init(groupID: UInt64, subgroupID: UInt64?, objectID: UInt64,
                publisherPriority: UInt8) {
        self.groupID = groupID
        self.subgroupID = subgroupID
        self.objectID = objectID
        self.publisherPriority = publisherPriority
    }
}

/// The native metadata clock domain, without conversion to a Date or Duration.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum MediaTimeDomain: Sendable, Hashable {
    case none
    case unixMicroseconds
    case applicationMedia
    case unknown(UInt32)
}

/// A copied effective LOC snapshot. Optional fields follow native presence,
/// not nonzero values or byte lengths. Present empty configs are empty Data.
/// Construction preserves received unknown bits. OutgoingObject validates
/// known presence/field and marking shape at write time, before allocation;
/// the native service validates outbound clock and profile semantics.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct MediaLOCMetadata: Sendable, Hashable {
    /// The complete native presence mask, including unknown future bits.
    public let rawPresence: UInt32
    /// Raw ticks and units/second; zero and UInt64.max are retained verbatim.
    public let timestamp: UInt64?
    public let timescale: UInt64?
    /// Exact marking payload, without independence or layer normalization.
    public let marking: Data?
    /// Raw voice-activity bit and seven-bit level.
    public let audioLevel: UInt8?
    public let videoConfig: Data?
    public let audioConfig: Data?

    public init(rawPresence: UInt32, timestamp: UInt64? = nil,
                timescale: UInt64? = nil, marking: Data? = nil,
                audioLevel: UInt8? = nil, videoConfig: Data? = nil,
                audioConfig: Data? = nil) {
        self.rawPresence = rawPresence
        self.timestamp = timestamp
        self.timescale = timescale
        self.marking = marking
        self.audioLevel = audioLevel
        self.videoConfig = videoConfig
        self.audioConfig = audioConfig
    }
}

/// Metadata independent of the existing presentation/decode/capture helpers.
/// Nil LOC means its complete native record was not observed.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct MediaObjectMetadata: Sendable, Hashable {
    public let timeDomain: MediaTimeDomain
    /// Available only when the native has_time_us flag is set; the domain
    /// remains explicit. Raw LOC ticks are never relabeled as this value.
    public let timeMicroseconds: UInt64?
    /// Nil means native independence is unknown, not false.
    public let isIndependent: Bool?
    public let loc: MediaLOCMetadata?
    /// The effective LOC-05 Codec String (the object's own, else its
    /// track's): its exact bytes, which the library has checked to be UTF-8
    /// and a usable identifier. Nil when there is none, on any profile but
    /// LOC-05, or when the native block was not observed. Kept as bytes so
    /// equality never merges distinct byte strings; ``codecString`` is the
    /// text. Accepted is not supported: no decoder is consulted.
    public let codecStringBytes: Data?

    /// The Codec String as text; nil when absent or not UTF-8.
    public var codecString: String? {
        codecStringBytes.flatMap { String(data: $0, encoding: .utf8) }
    }

    public init(timeDomain: MediaTimeDomain, timeMicroseconds: UInt64? = nil,
                isIndependent: Bool? = nil, loc: MediaLOCMetadata? = nil,
                codecStringBytes: Data? = nil) {
        self.timeDomain = timeDomain
        self.timeMicroseconds = timeMicroseconds
        self.isIndependent = isIndependent
        self.loc = loc
        self.codecStringBytes = codecStringBytes
    }
}

/// Delivery source, separate from wire location. Unknown origins retain the
/// raw request field without asserting that it is a valid initiating ID.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum MediaObjectDelivery: Sendable, Hashable {
    case live
    case fill(initiatingRequestID: UInt64)
    case unknown(rawOrigin: UInt32, rawInitiatingRequestID: UInt64)

    public var initiatingRequestID: UInt64? {
        if case .fill(let id) = self { return id }
        return nil
    }
}
