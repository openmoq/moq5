import CMoQTransport
import CMoQCore
import MoQ

// MARK: - Pump Context

/// Information passed to the on_pump callback.
public struct ThreadedPumpContext: @unchecked Sendable {
    /// Current network time in microseconds.
    public let nowUS: UInt64
    /// The raw C session pointer, owned by the threaded adapter.
    package let sessionRaw: OpaquePointer?

    internal init(nowUS: UInt64, sessionRaw: OpaquePointer?) {
        self.nowUS = nowUS
        self.sessionRaw = sessionRaw
    }

    /// Create a non-owning Session wrapper from the threaded
    /// adapter's session. Returns nil if not yet connected.
    /// The returned Session is valid for the ThreadedConnection's
    /// lifetime but must only be used on the network thread
    /// (inside on_pump). It may be stored in long-lived objects
    /// like Subscriber that are also confined to on_pump.
    package func borrowSession() -> Session? {
        guard let ptr = sessionRaw else { return nil }
        return Session(borrowing: ptr)
    }
}

/// Box holding the Swift pump closure. Prevent a retain cycle:
/// owned by ThreadedConnection, C holds an unretained pointer.
private final class PumpBox {
    let handler: (ThreadedPumpContext) -> Int32
    init(_ handler: @escaping (ThreadedPumpContext) -> Int32) {
        self.handler = handler
    }
}

/// C trampoline for on_pump.
private func pumpTrampoline(
    _ t: OpaquePointer?, _ nowUS: UInt64, _ ctx: UnsafeMutableRawPointer?
) -> Int32 {
    guard let ctx else { return 0 }
    let box = Unmanaged<PumpBox>.fromOpaque(ctx).takeUnretainedValue()
    let session = t.flatMap { moq_pq_threaded_session($0) }
    let pumpCtx = ThreadedPumpContext(nowUS: nowUS, sessionRaw: session)
    return box.handler(pumpCtx)
}

// MARK: - Threaded Connection

/// A MoQ client connection using picoquic's threaded network loop.
///
/// Requires the CMake-built moq-adapter-picoquic-threaded and its
/// dependencies to be installed. Build with:
///
///     export PKG_CONFIG_PATH=$(cmake --install-prefix)/lib/pkgconfig
///     swift build --target MoQTransport
///
/// Probe command (requires installed adapter, no relay needed):
///
///     PKG_CONFIG_PATH=... swift build --target MoQTransport
///     # Then in Swift: try ThreadedConnection(host: "127.0.0.1", port: 0)
///     # → throws MoQError.invalidArgument (port must be 1..65535)
///
/// The threaded adapter creates and owns the moq_session_t internally.
/// A future phase will add a non-owning Session wrapper to expose it
/// to the Publisher/Subscriber facades via ThreadedPumpContext.sessionRaw.
public final class ThreadedConnection {
    private let raw: OpaquePointer  // moq_pq_threaded_t*
    private let pumpBox: PumpBox

    /// Create and start a client connection to a MoQ relay.
    /// The network thread starts immediately upon creation.
    ///
    /// - Parameter onPump: Called on the network thread each pump
    ///   cycle. May call session/adapter/facade APIs via the context.
    ///   Return 0 to continue, nonzero to request clean shutdown.
    ///   The closure must not capture `self` (retain cycle).
    ///   Default is a no-op that returns 0.
    public init(
        host: String,
        port: Int32,
        insecureSkipVerify: Bool = false,
        sendRequestCapacity: Bool = true,
        initialRequestCapacity: UInt64 = 64,
        onPump: @escaping (ThreadedPumpContext) -> Int32 = { _ in 0 }
    ) throws {
        let box = PumpBox(onPump)
        self.pumpBox = box

        var cfg = moq_pq_threaded_cfg_t()
        moq_pq_threaded_cfg_init(&cfg)
        cfg.alloc = moq_alloc_default()
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT
        cfg.port = port
        cfg.insecure_skip_verify = insecureSkipVerify
        cfg.send_request_capacity = sendRequestCapacity
        cfg.initial_request_capacity = initialRequestCapacity
        cfg.on_pump = pumpTrampoline
        cfg.on_pump_ctx = Unmanaged.passUnretained(box).toOpaque()

        // host is consumed during create (getaddrinfo) and not
        // retained, so withCString lifetime is sufficient.
        var ptr: OpaquePointer?
        let rc: moq_result_t = host.withCString { hostPtr in
            cfg.host = hostPtr
            return moq_pq_threaded_create(&cfg, &ptr)
        }
        try MoQError.check(rc)
        guard let p = ptr else { throw MoQError.internal }
        self.raw = p
    }

    deinit {
        moq_pq_threaded_stop(raw)
        moq_pq_threaded_destroy(raw)
    }

    /// Block until the network thread signals activity or timeout.
    public func wait(timeoutUS: UInt64 = 5_000_000) throws {
        try MoQError.check(moq_pq_threaded_wait(raw, timeoutUS))
    }

    /// Signal the network thread to wake up and run on_pump.
    public func wake() throws {
        try MoQError.check(moq_pq_threaded_wake(raw))
    }

    /// Stop the network thread gracefully.
    public func stop() throws {
        try MoQError.check(moq_pq_threaded_stop(raw))
    }

    /// Whether a fatal transport error has occurred.
    public var isFatal: Bool {
        moq_pq_threaded_is_fatal(raw)
    }

    /// The raw C session pointer. Only valid after connection setup.
    package var sessionRaw: OpaquePointer? {
        moq_pq_threaded_session(raw)
    }
}
