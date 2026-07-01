import CMoQMediaObject
import CMoQLOC
import CMoQCore
import Foundation
import MoQ

// MARK: - Media Types

public enum MediaType: Sendable, Hashable {
    case video
    case audio
}

public enum MediaPackaging: Sendable, Hashable {
    case raw
    case cmaf
}

// MARK: - Track Info

public struct MediaTrackInfo: Sendable, Hashable {
    public var mediaType: MediaType
    public var packaging: MediaPackaging
    public var timescale: UInt64

    public init(mediaType: MediaType, packaging: MediaPackaging, timescale: UInt64 = 0) {
        self.mediaType = mediaType
        self.packaging = packaging
        self.timescale = timescale
    }
}

// MARK: - Track Info Error

public enum MediaTrackInfoError: Error, Sendable, Equatable {
    case missingRole
    case unsupportedRole(String?)
    case unsupportedPackaging(String)
}

// MARK: - Drop Reason

public enum MediaDropReason: Error, Sendable, Hashable {
    case malformedLOC
    case malformedCMAF
    case missingTimestamp
    case nonMonotonicDTS
    case unsupportedMultiSample
    case stale
    case keyframeWait
    case unknown(Int32)
}

// MARK: - Parse Error

public enum MediaParseError: Error, Sendable, Equatable {
    case invalidArgument
    case dropped(MediaDropReason)
    case bufferTooSmall(requiredSamples: Int)
}

// MARK: - CMAF Sample

public struct MediaSample: Sendable, Hashable {
    public let duration: UInt32
    public let size: UInt32
    public let flags: UInt32
    public let compositionOffset: Int32
}

// MARK: - Parsed Object

// Not Sendable: holds `Buffer` (non-atomic, shard-confined rcbuf).
public struct MediaParsedObject {
    public let packaging: MediaPackaging
    public let status: ObjectStatus
    public let endOfGroup: Bool
    public let isDatagram: Bool

    public let hasCaptureTime: Bool
    public let captureTimeUS: UInt64
    public let decodeTimeUS: UInt64
    public let compositionOffsetUS: Int64
    public let presentationTimeUS: UInt64
    public let sampleDurationUS: UInt32

    public let keyframe: Bool

    public let payload: Buffer?
    public let properties: Buffer?

    public let mdatOffset: Int
    public let mdatLength: Int

    public let sampleCount: Int
    public let samples: [MediaSample]
}

// MARK: - MSFTrack convenience

extension MSFTrack {
    /// Derive a MediaTrackInfo from MSF catalog fields.
    /// Throws MediaTrackInfoError for missing/unsupported role or packaging.
    public func mediaTrackInfo() throws -> MediaTrackInfo {
        let mt: MediaType
        switch role {
        case "video": mt = .video
        case "audio": mt = .audio
        case nil:     throw MediaTrackInfoError.missingRole
        default:      throw MediaTrackInfoError.unsupportedRole(role)
        }

        let pkg: MediaPackaging
        switch packaging {
        case "loc":  pkg = .raw
        case "cmaf": pkg = .cmaf
        default:     throw MediaTrackInfoError.unsupportedPackaging(packaging)
        }

        return MediaTrackInfo(
            mediaType: mt,
            packaging: pkg,
            timescale: timescale ?? 0
        )
    }
}

// MARK: - Parser

public enum MediaObjectParser {

    /// Parse a received object into packaging-independent timing
    /// and payload metadata. The input object's payload and
    /// properties Buffers are borrowed — the parser does not
    /// incref or copy them. The returned MediaParsedObject holds
    /// references to the same Buffers, keeping them alive.
    public static func parse(
        track: MediaTrackInfo,
        object: FacadeReceivedObject,
        maxSamples: Int = 16
    ) throws -> MediaParsedObject {
        guard maxSamples >= 0 else {
            throw MediaParseError.invalidArgument
        }

        var ti = moq_media_track_info_t()
        moq_media_track_info_init(&ti)
        ti.media_type = track.mediaType == .video
            ? MOQ_MEDIA_TYPE_VIDEO : MOQ_MEDIA_TYPE_AUDIO

        if track.packaging == .cmaf {
            guard track.timescale <= UInt64(UInt32.max) else {
                throw MediaParseError.invalidArgument
            }
            ti.packaging = MOQ_MEDIA_PACKAGING_CMAF
            ti.timescale = UInt32(track.timescale)
        } else {
            ti.packaging = MOQ_MEDIA_PACKAGING_RAW
            ti.timescale = 0
        }

        var input = moq_media_object_input_t()
        moq_media_object_input_init(&input)
        input.group_id = object.groupID
        input.object_id = object.objectID
        input.status = object.status.cValue
        input.end_of_group = object.endOfGroup
        input.datagram = object.isDatagram
        input.payload = object.payload?.raw
        input.properties = object.properties?.raw

        var samples = [moq_cmaf_sample_t](
            repeating: moq_cmaf_sample_t(), count: maxSamples)
        var parsed = moq_media_parsed_object_t()
        moq_media_parsed_object_init(&parsed)
        var dropReason = moq_media_drop_reason_t(rawValue: 0)

        let rc = moq_media_object_parse(
            &ti, &input, &samples, samples.count, &parsed, &dropReason)

        if rc == MOQ_ERR_INVAL {
            throw MediaParseError.invalidArgument
        }
        if rc == MOQ_ERR_BUFFER {
            throw MediaParseError.bufferTooSmall(
                requiredSamples: parsed.sample_count)
        }
        if rc == MOQ_ERR_PROTO {
            let reason: MediaDropReason
            switch dropReason {
            case MOQ_MEDIA_DROP_MALFORMED_LOC:            reason = .malformedLOC
            case MOQ_MEDIA_DROP_MALFORMED_CMAF:           reason = .malformedCMAF
            case MOQ_MEDIA_DROP_MISSING_TIMESTAMP:        reason = .missingTimestamp
            case MOQ_MEDIA_DROP_NON_MONOTONIC_DTS:        reason = .nonMonotonicDTS
            case MOQ_MEDIA_DROP_UNSUPPORTED_MULTI_SAMPLE: reason = .unsupportedMultiSample
            case MOQ_MEDIA_DROP_STALE:                    reason = .stale
            case MOQ_MEDIA_DROP_KEYFRAME_WAIT:            reason = .keyframeWait
            default: reason = .unknown(Int32(dropReason.rawValue))
            }
            throw MediaParseError.dropped(reason)
        }
        if rc < 0 {
            throw MediaParseError.invalidArgument
        }

        let swiftSamples: [MediaSample] = (0..<parsed.sample_count).map { i in
            let s = samples[i]
            return MediaSample(
                duration: s.duration,
                size: s.size,
                flags: s.flags,
                compositionOffset: s.composition_offset)
        }

        return MediaParsedObject(
            packaging: track.packaging,
            status: ObjectStatus(parsed.status),
            endOfGroup: parsed.end_of_group,
            isDatagram: parsed.datagram,
            hasCaptureTime: parsed.has_capture_time,
            captureTimeUS: parsed.capture_time_us,
            decodeTimeUS: parsed.decode_time_us,
            compositionOffsetUS: parsed.composition_offset_us,
            presentationTimeUS: parsed.presentation_time_us,
            sampleDurationUS: parsed.sample_duration_us,
            keyframe: parsed.keyframe,
            payload: object.payload,
            properties: object.properties,
            mdatOffset: parsed.mdat_offset,
            mdatLength: parsed.mdat_len,
            sampleCount: parsed.sample_count,
            samples: swiftSamples
        )
    }
}
