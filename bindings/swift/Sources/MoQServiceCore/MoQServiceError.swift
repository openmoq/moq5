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
    /// receiver, or sender scoped).
    case fatal(code: UInt64)

    case outOfMemory

    /// An unexpected C result code leaked through; the raw value is kept
    /// for diagnostics.
    case internalError(Int32)
}
