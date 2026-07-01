import CMoQCore

/// Every error LibMoQ can produce.
public enum MoQError: Error, Sendable, Hashable {
    case outOfMemory
    case invalidArgument
    case protocolViolation
    case sessionClosed
    case wrongState
    case staleHandle
    case wouldBlock
    case bufferTooSmall
    case requestBlocked
    case abiMismatch
    case goaway
    case `internal`
    case unknown(Int32)
}

extension MoQError {
    /// Map a C result code to a Swift error, or nil for MOQ_OK/MOQ_DONE.
    internal static func from(_ rc: moq_result_t) -> MoQError? {
        switch rc {
        case MOQ_OK, MOQ_DONE: return nil
        case MOQ_ERR_NOMEM:          return .outOfMemory
        case MOQ_ERR_INVAL:          return .invalidArgument
        case MOQ_ERR_PROTO:          return .protocolViolation
        case MOQ_ERR_CLOSED:         return .sessionClosed
        case MOQ_ERR_WRONG_STATE:    return .wrongState
        case MOQ_ERR_STALE_HANDLE:   return .staleHandle
        case MOQ_ERR_WOULD_BLOCK:    return .wouldBlock
        case MOQ_ERR_BUFFER:         return .bufferTooSmall
        case MOQ_ERR_REQUEST_BLOCKED: return .requestBlocked
        case MOQ_ERR_ABI_MISMATCH:   return .abiMismatch
        case MOQ_ERR_GOAWAY:         return .goaway
        case MOQ_ERR_INTERNAL:       return .internal
        default:                     return .unknown(rc)
        }
    }

    /// Throw if the result code indicates an error.
    package static func check(_ rc: moq_result_t) throws {
        if let err = from(rc) { throw err }
    }
}
