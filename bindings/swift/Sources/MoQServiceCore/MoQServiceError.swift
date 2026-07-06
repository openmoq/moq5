/// Errors thrown by the MoQService surface.
///
/// The cases mirror the C service tier's `moq_result_t` vocabulary, but only
/// the states an app can meaningfully react to are distinct cases; internal
/// C codes that "cannot happen" through this API surface land in
/// ``internalError(_:)``.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum MoQServiceError: Error, Sendable, Hashable {
    /// A configuration or argument was rejected before any I/O happened.
    case invalidArgument(String)

    /// The requested capability is not compiled in / not available on this
    /// backend (or, pre-engine, not implemented yet).
    case unsupported

    /// The operation could not make progress right now; retry after the
    /// corresponding wait. Senders see this from `write` under the
    /// fail-fast policy or a lossless deadline.
    case wouldBlock

    /// The object is in the wrong lifecycle state for this call (for
    /// example writing to an ended track).
    case wrongState

    /// The endpoint's sticky interrupt latch is set (``MoQEndpoint/interrupt()``).
    /// Non-terminal: after ``MoQEndpoint/resume()`` the call may be retried.
    case interrupted

    /// Clean terminal state: the endpoint or attachment closed normally.
    case closed

    /// Terminal failure; `code` is the C tier's fatal code (endpoint,
    /// receiver, or sender scoped). Used for MoQ/protocol fatals — a
    /// transport/handshake failure is reported as ``connectionFailed(_:)``,
    /// which classifies why (a `fatal(code:)` with a bare 0 could not).
    case fatal(code: UInt64)

    /// The connection could not be established, classified by its cause.
    /// Distinguishes a certificate rejection from a plain transport failure —
    /// something the endpoint's fatal code alone (0 for all of these) cannot.
    case connectionFailed(ConnectionFailure)

    case outOfMemory

    /// An unexpected C result code leaked through; the raw value is kept
    /// for diagnostics.
    case internalError(Int32)
}

/// Why a connection attempt failed. `code` carries the transport-native detail
/// (for picoquic: a QUIC CRYPTO_ERROR whose low byte is the TLS alert); it is
/// diagnostic, not something to branch on — branch on `kind`.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct ConnectionFailure: Error, Sendable, Hashable {
    public enum Kind: Sendable, Hashable {
        /// The peer's certificate did not verify against the trust anchors.
        case certificateUnverified
        /// Another TLS/crypto handshake failure.
        case tls
        /// A generic transport/handshake failure (timeout, connection
        /// refused, ALPN mismatch, network error).
        case transport
    }
    public let kind: Kind
    public let code: UInt64

    public init(kind: Kind, code: UInt64) {
        self.kind = kind
        self.code = code
    }
}
