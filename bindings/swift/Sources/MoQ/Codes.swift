import CMoQCore

// MARK: - Request Error Codes

/// REQUEST_ERROR codes (session.h `MOQ_REQUEST_ERROR_*`), full width on the
/// wire. A rejection passes one of these as its `errorCode`; events carry
/// the peer's code unchanged.
public enum RequestErrorCode {
    public static let internalError: UInt64 = UInt64(MOQ_REQUEST_ERROR_INTERNAL_ERROR)
    public static let unauthorized: UInt64 = UInt64(MOQ_REQUEST_ERROR_UNAUTHORIZED)
    public static let timeout: UInt64 = UInt64(MOQ_REQUEST_ERROR_TIMEOUT)
    public static let notSupported: UInt64 = UInt64(MOQ_REQUEST_ERROR_NOT_SUPPORTED)
    public static let malformedAuthToken: UInt64 = UInt64(MOQ_REQUEST_ERROR_MALFORMED_AUTH_TOKEN)
    public static let expiredAuthToken: UInt64 = UInt64(MOQ_REQUEST_ERROR_EXPIRED_AUTH_TOKEN)
    public static let goingAway: UInt64 = UInt64(MOQ_REQUEST_ERROR_GOING_AWAY)
    public static let excessiveLoad: UInt64 = UInt64(MOQ_REQUEST_ERROR_EXCESSIVE_LOAD)
    public static let doesNotExist: UInt64 = UInt64(MOQ_REQUEST_ERROR_DOES_NOT_EXIST)
    public static let invalidRange: UInt64 = UInt64(MOQ_REQUEST_ERROR_INVALID_RANGE)
    public static let malformedTrack: UInt64 = UInt64(MOQ_REQUEST_ERROR_MALFORMED_TRACK)
    public static let uninterested: UInt64 = UInt64(MOQ_REQUEST_ERROR_UNINTERESTED)
    public static let prefixOverlap: UInt64 = UInt64(MOQ_REQUEST_ERROR_PREFIX_OVERLAP)
    public static let namespaceTooLarge: UInt64 = UInt64(MOQ_REQUEST_ERROR_NAMESPACE_TOO_LARGE)
    public static let unsupportedExtension: UInt64 = UInt64(MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION)
    public static let redirect: UInt64 = UInt64(MOQ_REQUEST_ERROR_REDIRECT)
    /// Drafts 16 and 18 only; draft-22 treats them as unknown.
    public static let duplicateSubscription: UInt64 = UInt64(MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION)
    public static let invalidJoiningRequestID: UInt64 =
        UInt64(MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID)
    /// Draft-22 only.
    public static let conflictingFilters: UInt64 = UInt64(MOQ_REQUEST_ERROR_CONFLICTING_FILTERS)
    public static let invalidFilter: UInt64 = UInt64(MOQ_REQUEST_ERROR_INVALID_FILTER)
}

// MARK: - Stream Error Codes

/// The application error codes a data stream carries in RESET_STREAM or
/// STOP_SENDING (session.h `MOQ_STREAM_ERROR_*`): wire values passed through
/// unchanged, so a caller uses the value its session's draft registers.
public enum StreamErrorCode {
    public static let internalError: UInt64 = UInt64(MOQ_STREAM_ERROR_INTERNAL_ERROR)
    public static let cancelled: UInt64 = UInt64(MOQ_STREAM_ERROR_CANCELLED)
    public static let deliveryTimeout: UInt64 = UInt64(MOQ_STREAM_ERROR_DELIVERY_TIMEOUT)
    public static let sessionClosed: UInt64 = UInt64(MOQ_STREAM_ERROR_SESSION_CLOSED)
    public static let malformedTrack: UInt64 = UInt64(MOQ_STREAM_ERROR_MALFORMED_TRACK)
    /// Drafts 18 and 22 only.
    public static let goingAway: UInt64 = UInt64(MOQ_STREAM_ERROR_GOING_AWAY)
    public static let tooFarBehind: UInt64 = UInt64(MOQ_STREAM_ERROR_TOO_FAR_BEHIND)
    public static let unknownObjectStatus: UInt64 = UInt64(MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS)
    public static let expiredAuthToken: UInt64 = UInt64(MOQ_STREAM_ERROR_EXPIRED_AUTH_TOKEN)
    public static let excessiveLoad: UInt64 = UInt64(MOQ_STREAM_ERROR_EXCESSIVE_LOAD)
    /// Draft 16's value for UNKNOWN_OBJECT_STATUS.
    public static let d16UnknownObjectStatus: UInt64 =
        UInt64(MOQ_STREAM_ERROR_D16_UNKNOWN_OBJECT_STATUS)
}

// MARK: - Request Terminal Outcomes

/// Why a request ended by a stream signal rather than by a message
/// (`MOQ_REQUEST_TERMINAL_*`). `.none` when a message ended it, or when the
/// producer predates the field.
public enum RequestTerminalCause: Sendable, Hashable {
    case none
    case peerResetStream
    case peerStopSending
    case missingResponse
    case truncatedResponse
    case peerFinComplete
    /// This side cancelled a fetch it could not take (FETCH_RESET only).
    case localCancel
    /// This side cancelled the request because its Track is malformed
    /// (draft-22 §12.1): UNSUBSCRIBED, PUBLISH_FINISHED and FETCH_RESET.
    case malformedTrack
    case other(UInt32)

    internal init(_ v: UInt32) {
        switch v {
        case UInt32(MOQ_REQUEST_TERMINAL_NONE):               self = .none
        case UInt32(MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM):  self = .peerResetStream
        case UInt32(MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING):  self = .peerStopSending
        case UInt32(MOQ_REQUEST_TERMINAL_MISSING_RESPONSE):   self = .missingResponse
        case UInt32(MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE): self = .truncatedResponse
        case UInt32(MOQ_REQUEST_TERMINAL_PEER_FIN_COMPLETE):  self = .peerFinComplete
        case UInt32(MOQ_REQUEST_TERMINAL_LOCAL_CANCEL):       self = .localCancel
        case UInt32(MOQ_REQUEST_TERMINAL_MALFORMED_TRACK):    self = .malformedTrack
        default:                                              self = .other(v)
        }
    }
}

/// This endpoint's side of the request an outcome names.
public enum RequestRole: Sendable, Hashable {
    case requester
    case responder
    case other(UInt32)

    internal init(_ v: UInt32) {
        switch v {
        case UInt32(MOQ_REQUEST_ROLE_REQUESTER): self = .requester
        case UInt32(MOQ_REQUEST_ROLE_RESPONDER): self = .responder
        default:                                 self = .other(v)
        }
    }
}

/// The request family an outcome or a migration names.
public enum RequestFamily: Sendable, Hashable {
    case subscribe, fetch, trackStatus, announcement, namespaceSubscription
    case publish, subscribeTracks
    case other(UInt32)

    internal init(_ v: moq_request_family_t) {
        switch v {
        case MOQ_REQUEST_FAMILY_SUBSCRIBE:        self = .subscribe
        case MOQ_REQUEST_FAMILY_FETCH:            self = .fetch
        case MOQ_REQUEST_FAMILY_TRACK_STATUS:     self = .trackStatus
        case MOQ_REQUEST_FAMILY_ANNOUNCEMENT:     self = .announcement
        case MOQ_REQUEST_FAMILY_NS_SUB:           self = .namespaceSubscription
        case MOQ_REQUEST_FAMILY_PUBLISH:          self = .publish
        case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS: self = .subscribeTracks
        default:                                  self = .other(v.rawValue)
        }
    }
}

/// An abnormal TRACK_STATUS outcome, not a peer REQUEST_ERROR.
public enum TrackStatusTerminalCause: Sendable, Hashable {
    case peerResetStream, peerStopSending, missingResponse, truncatedResponse
    case other(UInt32)

    internal init(_ value: UInt32) {
        switch value {
        case UInt32(MOQ_TRACK_STATUS_PEER_RESET_STREAM): self = .peerResetStream
        case UInt32(MOQ_TRACK_STATUS_PEER_STOP_SENDING): self = .peerStopSending
        case UInt32(MOQ_TRACK_STATUS_MISSING_RESPONSE): self = .missingResponse
        case UInt32(MOQ_TRACK_STATUS_TRUNCATED_RESPONSE): self = .truncatedResponse
        default: self = .other(value)
        }
    }
}

/// Namespace announcement outcomes have a distinct native cause domain.
public enum NamespaceTerminalCause: Sendable, Hashable {
    case peerFinComplete, peerResetStream, peerStopSending, missingResponse
    case truncatedMessage, localCapacity
    case other(UInt32)

    internal init(_ value: UInt32) {
        switch value {
        case UInt32(MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE): self = .peerFinComplete
        case UInt32(MOQ_NAMESPACE_TERMINAL_PEER_RESET_STREAM): self = .peerResetStream
        case UInt32(MOQ_NAMESPACE_TERMINAL_PEER_STOP_SENDING): self = .peerStopSending
        case UInt32(MOQ_NAMESPACE_TERMINAL_MISSING_RESPONSE): self = .missingResponse
        case UInt32(MOQ_NAMESPACE_TERMINAL_TRUNCATED_MESSAGE): self = .truncatedMessage
        case UInt32(MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY): self = .localCapacity
        default: self = .other(value)
        }
    }
}

public struct TrackStatusTerminatedInfo: Sendable, Hashable {
    /// Identifier only: the native request handle is stale after commitment.
    public let handle: UInt64
    public let cause: TrackStatusTerminalCause
    public let localRole: RequestRole
    /// The exact peer stream error for RESET/STOP, zero otherwise.
    public let streamErrorCode: UInt64
}

public struct NamespaceTerminatedInfo: Sendable, Hashable {
    public let announcement: Announcement
    public let cause: NamespaceTerminalCause
    public let localRole: RequestRole
    /// The exact peer stream error for RESET/STOP, zero otherwise.
    public let streamErrorCode: UInt64
}

public struct NamespaceSubscriptionTerminatedInfo: Sendable, Hashable {
    /// Identifier only: the native request handle is stale after commitment.
    public let handle: UInt64
    public let cause: RequestTerminalCause
    public let localRole: RequestRole
    /// The exact peer stream error for RESET/STOP, zero otherwise.
    public let streamErrorCode: UInt64
}

/// Why a locally opened subgroup was reset asynchronously. Unknown native
/// causes keep their value; this never means the whole request ended.
public enum OutgoingSubgroupTerminalCause: Sendable, Hashable {
    case none
    case peerStopSending
    case deliveryTimeout
    case unknown(UInt32)

    internal init(_ value: UInt32) {
        switch value {
        case UInt32(MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE): self = .none
        case UInt32(MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING): self = .peerStopSending
        case UInt32(MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT): self = .deliveryTimeout
        default: self = .unknown(value)
        }
    }
}

/// Copied identity of an outgoing subgroup whose reset was committed locally.
/// Exactly one owner is present. Handles identify the terminated generation,
/// not necessarily a live object; no provider completion or peer ACK is implied.
public struct OutgoingSubgroupTerminatedInfo: Sendable, Hashable {
    public let subgroup: SubgroupHandle
    public let subscription: Subscription?
    public let publication: Publication?
    public let groupID: UInt64
    public let subgroupID: UInt64
    public let stream: StreamRef
    /// The exact reset code, including zero when zero was the cause's code.
    public let errorCode: UInt64
    public let cause: OutgoingSubgroupTerminalCause
}

// MARK: - Event field coverage

/// An appended event field is read only when the producer's detail covers all
/// of it; an older producer's event reads it as absent.
internal func detailCovers<T, F>(_ detailSize: UInt32, _ field: KeyPath<T, F>) -> Bool {
    guard let off = MemoryLayout<T>.offset(of: field) else { return false }
    return Int(detailSize) >= off + MemoryLayout<F>.size
}

/// The stream signal that ended a request, read only when the detail covers
/// the field.
internal func terminalCause<T>(
    _ detailSize: UInt32, _ d: T, _ cause: KeyPath<T, moq_request_terminal_cause_t>,
    _ code: KeyPath<T, UInt64>
) -> (RequestTerminalCause, UInt64) {
    let c = detailCovers(detailSize, cause) ? RequestTerminalCause(d[keyPath: cause]) : .none
    let e = detailCovers(detailSize, code) ? d[keyPath: code] : 0
    return (c, e)
}
