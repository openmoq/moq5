import Foundation

/* The media player's value model: what `MediaPlayer` hands the application.
 * Every record is a Sendable COPY made on the service thread (a backend
 * releases the C records' refs in the same poll), so nothing here borrows
 * from the C tier. Kinds and reasons this binding does not know surface as
 * `.unknown` cases carrying the raw value. */

/// A track the player announced or plays. Identity is the receiver handle:
/// two values for the same handle are equal even when one carries a newer
/// description.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct PlayerTrack: Sendable, Hashable {
    /// The receiver handle, opaque.
    public let id: UInt64
    /// The catalog description, when the track is in the media model
    /// (video or audio).
    public let description: TrackDescription?

    public var name: String? { description?.name }

    package init(id: UInt64, description: TrackDescription?) {
        self.id = id
        self.description = description
    }

    public static func == (lhs: PlayerTrack, rhs: PlayerTrack) -> Bool { lhs.id == rhs.id }
    public func hash(into hasher: inout Hasher) { hasher.combine(id) }
}

/// Where a timeline's timestamps come from.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum PlayerTimeDomain: Sendable, Hashable {
    /// Wall-clock microseconds since the Unix epoch (drafts 16/18, and LOC
    /// without a Timescale).
    case unixTime
    /// Application media time (LOC with a catalog Timescale).
    case applicationMedia
    case unknown(UInt32)
}

/// A decoder (re)configuration, before the units it applies to.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct PlayerDecoderConfiguration: Sendable {
    public let track: PlayerTrack
    public let configurationGeneration: UInt64
    public let timelineID: UInt32
    /// nil for a media type this binding does not model.
    public let mediaType: MediaType?
    public let codec: String
    /// Decoder configuration (avcC / hvcC payload, AudioSpecificConfig, ...),
    /// nil when the stream carries none.
    public let codecConfig: Data?
    public let timescale: UInt32?
    public let width: Int?
    public let height: Int?
    public let sampleRate: Int?
    public let channelCount: Int?

    package init(track: PlayerTrack, configurationGeneration: UInt64, timelineID: UInt32,
                 mediaType: MediaType?, codec: String, codecConfig: Data?,
                 timescale: UInt32?, width: Int?, height: Int?, sampleRate: Int?,
                 channelCount: Int?) {
        self.track = track
        self.configurationGeneration = configurationGeneration
        self.timelineID = timelineID
        self.mediaType = mediaType
        self.codec = codec
        self.codecConfig = codecConfig
        self.timescale = timescale
        self.width = width
        self.height = height
        self.sampleRate = sampleRate
        self.channelCount = channelCount
    }
}

/// One access unit, in decode order.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct PlayerUnit: Sendable {
    public let track: PlayerTrack
    public let configurationGeneration: UInt64
    public let timelineID: UInt32
    public let groupID: UInt64
    public let objectID: UInt64
    /// On CMAF the decode clock. On RAW/LOC a copy of the LOC media time,
    /// equal to `presentationTime`: LOC has no decode timestamp, units come
    /// in decode order, and with frame reordering this can decrease from
    /// one unit to the next, so it is not evidence of a decode clock.
    public let decodeTime: Duration
    public let presentationTime: Duration
    /// nil when the stream does not say (video carries LOC marking).
    public let isIndependent: Bool?
    public let sampleDuration: Duration?
    public let captureTime: Duration?
    public let data: Data

    package init(track: PlayerTrack, configurationGeneration: UInt64, timelineID: UInt32,
                 groupID: UInt64, objectID: UInt64, decodeTime: Duration,
                 presentationTime: Duration, isIndependent: Bool?, sampleDuration: Duration?,
                 captureTime: Duration?, data: Data) {
        self.track = track
        self.configurationGeneration = configurationGeneration
        self.timelineID = timelineID
        self.groupID = groupID
        self.objectID = objectID
        self.decodeTime = decodeTime
        self.presentationTime = presentationTime
        self.isIndependent = isIndependent
        self.sampleDuration = sampleDuration
        self.captureTime = captureTime
        self.data = data
    }
}

/// Why a track's decoder must be reset before its next unit.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum PlayerDiscontinuityReason: Sendable, Hashable {
    /// Units were skipped.
    case gap
    /// The application reported a decode error.
    case decodeError
    case unknown(UInt32)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct PlayerDiscontinuity: Sendable {
    public let track: PlayerTrack
    public let configurationGeneration: UInt64
    public let timelineID: UInt32
    public let reason: PlayerDiscontinuityReason
    public let decoderResetRequired: Bool

    package init(track: PlayerTrack, configurationGeneration: UInt64, timelineID: UInt32,
                 reason: PlayerDiscontinuityReason, decoderResetRequired: Bool) {
        self.track = track
        self.configurationGeneration = configurationGeneration
        self.timelineID = timelineID
        self.reason = reason
        self.decoderResetRequired = decoderResetRequired
    }
}

/// A presentation mapping. A unit of the timeline presents at
/// `anchorLocalMicroseconds + (presentationTime - anchorMedia) / rate` on
/// the player's clock: monotonic microseconds since boot
/// (`DispatchTime.now().uptimeNanoseconds / 1000`), which on Apple
/// platforms is the clock `CMClockGetHostTimeClock()` reads.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct PlayerTimeline: Sendable {
    public let track: PlayerTrack
    public let timelineID: UInt32
    /// Changes when the mapping is re-anchored.
    public let epoch: UInt64
    public let domain: PlayerTimeDomain
    public let anchorMedia: Duration
    public let anchorLocalMicroseconds: UInt64
    public let rate: Double

    package init(track: PlayerTrack, timelineID: UInt32, epoch: UInt64,
                 domain: PlayerTimeDomain, anchorMedia: Duration,
                 anchorLocalMicroseconds: UInt64, rate: Double) {
        self.track = track
        self.timelineID = timelineID
        self.epoch = epoch
        self.domain = domain
        self.anchorMedia = anchorMedia
        self.anchorLocalMicroseconds = anchorLocalMicroseconds
        self.rate = rate
    }

    /// When a presentation time is due on the player's clock.
    public func localMicroseconds(forPresentation time: Duration) -> UInt64 {
        let offset = (Double(time.wholeMicroseconds)
                      - Double(anchorMedia.wholeMicroseconds)) / rate
        let local = Double(anchorLocalMicroseconds) + offset
        return local <= 0 ? 0 : UInt64(local)
    }
}

/// What ended a track; when several apply, the highest wins.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum PlayerEndCause: Sendable, Hashable {
    case peerEndOfTrack
    /// The peer ended the track's delivery without an END_OF_TRACK the
    /// player could use: an error done, or a track ended for good whose
    /// END_OF_TRACK did not arrive within the end tail wait.
    case peerDone
    case removed
    case localFailure
    case closed
    case fatal
    case unknown(UInt32)
}

/// How a track's input was settled when it ended.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum PlayerEndDisposition: Sendable, Hashable {
    /// Every location up to its END_OF_TRACK was played or known not to
    /// exist.
    case complete
    /// A bounded wait gave up on missing input.
    case tailCutoff
    /// No wait was possible.
    case immediate
    case unknown(UInt32)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct PlayerTrackEnd: Sendable {
    public let track: PlayerTrack
    public let configurationGeneration: UInt64
    public let cause: PlayerEndCause
    public let disposition: PlayerEndDisposition

    package init(track: PlayerTrack, configurationGeneration: UInt64,
                 cause: PlayerEndCause, disposition: PlayerEndDisposition) {
        self.track = track
        self.configurationGeneration = configurationGeneration
        self.cause = cause
        self.disposition = disposition
    }
}

/// One player output. Per track: `configure` before the units it applies
/// to, `timeline` before the timeline's first unit, `discontinuity` before
/// the unit that needs a reset decoder, `end` after the last unit.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum PlayerOutput: Sendable {
    case configure(PlayerDecoderConfiguration)
    case unit(PlayerUnit)
    case discontinuity(PlayerDiscontinuity)
    case timeline(PlayerTimeline)
    case end(PlayerTrackEnd)
    case unknown(kind: UInt32, track: PlayerTrack?)
}

/// A player diagnostic or lifecycle event.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum PlayerEvent: Sendable {
    /// Announced and not selected (every track under manual selection).
    case trackAvailable(PlayerTrack)
    case trackRemoved(PlayerTrack)
    /// The catalog is ready; automatic selection has run.
    case ready
    /// A timeline's first unit was released.
    case started(PlayerTrack, timelineID: UInt32)
    /// A missing unit was waited for (`crossed` false) or played past.
    case gap(PlayerTrack, groupID: UInt64, objectID: UInt64, crossed: Bool)
    case skip(PlayerTrack, fromGroupID: UInt64, toGroupID: UInt64)
    case shed(PlayerTrack, droppedGroups: UInt32, remainingGroups: UInt32)
    /// One object dropped; `reason` is the C drop reason
    /// (`moq_playback_drop_reason_t`, or a `MOQ_MEDIA_PLAYER_DROP_*` value).
    case drop(PlayerTrack?, reason: UInt32, groupID: UInt64, objectID: UInt64)
    case loss(PlayerTrack, groupID: UInt64)
    /// Video waits for an independent unit.
    case stall(PlayerTrack)
    /// Every selected track ended.
    case ended
    /// The player closed.
    case closed
    case timingIndependent(renderGroup: Int32?)
    case syncUnavailable(renderGroup: Int32?, reason: UInt32)
    case knowledgeForgotten(PlayerTrack, firstGroupID: UInt64, lastGroupID: UInt64)
    case unknown(kind: UInt32, track: PlayerTrack?)
}
