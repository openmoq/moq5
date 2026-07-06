import Foundation

/// A client connection to a MoQ relay: one QUIC/WebTransport connection, one
/// network thread (owned by the C service tier), and up to one
/// ``MediaReceiver`` plus one ``MediaSender`` attachment.
///
/// Lifecycle (`established`/`drain`/`close`), the interrupt latch, and state
/// run over the endpoint's service-thread engine. The public `connect`
/// factory (a real transport backend) lands with the C bridge slice.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MoQEndpoint: @unchecked Sendable {

    // MARK: Configuration

    /// How the connection reaches the relay. `.automatic` derives from the
    /// URL scheme (`moqt://`/`moq://` → raw QUIC, `https://` → WebTransport);
    /// an explicit value overrides the scheme.
    public enum TransportProtocol: Sendable, Hashable {
        case automatic, rawQUIC, webTransport
    }

    /// Which QUIC stack serves the connection. `.automatic` picks the first
    /// compiled-in backend supporting the resolved protocol.
    public enum TransportBackend: Sendable, Hashable {
        case automatic, picoquic, mvfst, proxygen
    }

    public struct Configuration: Sendable {
        /// Relay URL: `moqt://host:port[/path]` or `https://host:port[/path]`.
        public var url: URL
        public var transportProtocol: TransportProtocol
        public var backend: TransportBackend
        public var versions: VersionOffer
        /// TLS SNI + certificate-verification name; `nil` uses the URL host.
        public var serverName: String?
        /// CA bundle; `nil` uses the system roots.
        public var caFileURL: URL?
        /// Disables certificate verification. Local/self-signed testing ONLY.
        public var insecureSkipVerify: Bool
        /// WebTransport CONNECT path; `nil` uses the URL path or "/moq".
        public var webTransportPath: String?

        public init(url: URL) {
            self.url = url
            self.transportProtocol = .automatic
            self.backend = .automatic
            self.versions = .automatic
            self.serverName = nil
            self.caFileURL = nil
            self.insecureSkipVerify = false
            self.webTransportPath = nil
        }

        /// Synchronous preflight: the checks that must reject a
        /// configuration BEFORE any I/O. Scheme and host shape only — full
        /// URL semantics remain the C tier's job at connect.
        package func validate() throws {
            guard let scheme = url.scheme?.lowercased(),
                  ["moqt", "moq", "https"].contains(scheme) else {
                throw MoQServiceError.invalidArgument(
                    "URL scheme must be moqt://, moq://, or https://")
            }
            guard let host = url.host, !host.isEmpty else {
                throw MoQServiceError.invalidArgument("URL must include a host")
            }
        }
    }

    // MARK: State

    public enum State: Sendable, Hashable {
        case connecting, established, draining, closed
    }

    public let configuration: Configuration
    package let engine: EndpointEngine
    package var backend: any EndpointBackend { engine.backend }

    package init(configuration: Configuration, engine: EndpointEngine) {
        self.configuration = configuration
        self.engine = engine
    }

    deinit {
        /* Best-effort, non-blocking backstop: an endpoint dropped without
         * close() still tears its service thread and backend down. */
        engine.beginShutdown()
    }

    /// `.closed` once the endpoint has been closed/destroyed (the engine
    /// answers post-teardown reads itself; the backend is never touched
    /// after destroy).
    public var state: State { engine.state }
    /// The negotiated version; retains its last value across close.
    public var negotiatedVersion: MoQVersion? { engine.negotiatedVersion }

    // MARK: Interrupt latch

    /// Set the sticky, endpoint-wide interrupt latch: every suspended call on
    /// this endpoint and its attachments throws
    /// ``MoQServiceError/interrupted`` until ``resume()``. Non-terminal;
    /// callable from any thread (GStreamer `unlock`/`unlock_stop` analog).
    public func interrupt() { engine.setInterrupted(true) }

    /// Clear the interrupt latch; suspended surfaces become usable again.
    public func resume() { engine.setInterrupted(false) }

    // MARK: Lifecycle

    /// Suspends until the connection is ESTABLISHED. Throws
    /// ``MoQServiceError/interrupted`` while the latch is set; once terminal,
    /// ``MoQServiceError/closed`` (clean close), ``MoQServiceError/fatal(code:)``
    /// (protocol fatal), or ``MoQServiceError/connectionFailed(_:)`` (TLS,
    /// certificate, or transport failure, classified by reason). Task
    /// cancellation throws `CancellationError`.
    public func established() async throws {
        let backend = self.backend
        try await engine.waitUntil {
            switch backend.state {
            case .established:
                return .satisfied
            case .closed:
                switch backend.terminalFailure {
                case .none, .clean:
                    return .failure(.closed)
                case .protocolFatal(let code):
                    return .failure(.fatal(code: code))
                case .tlsCertificate(let code):
                    return .failure(.connectionFailed(
                        ConnectionFailure(kind: .certificateUnverified, code: code)))
                case .tls(let code):
                    return .failure(.connectionFailed(
                        ConnectionFailure(kind: .tls, code: code)))
                case .transport(let code):
                    return .failure(.connectionFailed(
                        ConnectionFailure(kind: .transport, code: code)))
                }
            case .connecting, .draining:
                return .pending
            }
        }
    }

    /// Flush locally queued reliable stream data before stopping -- THE
    /// flush guarantee (``close()`` never drains). Returns once the local
    /// stream backlog is provably empty; throws
    /// ``MoQServiceError/wouldBlock`` when `timeout` elapses first,
    /// ``MoQServiceError/unsupported`` when the backend cannot prove a
    /// flush, plus the interrupted/closed/fatal states.
    public func drain(timeout: Duration) async throws {
        guard timeout > .zero else {
            throw MoQServiceError.invalidArgument("drain timeout must be positive")
        }
        try await engine.drain(timeoutMicroseconds: timeout.wholeMicroseconds)
    }

    /// Tear down: stop (joins the network thread) and destroy. Idempotent;
    /// never throws. This is NOT graceful -- queued send data may be
    /// truncated. For a flush guarantee, `try await drain(timeout:)` first
    /// and handle its errors, then close.
    public func close() async {
        await engine.close()
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension Duration {
    /// Whole microseconds, saturating; non-positive durations become 0.
    package var wholeMicroseconds: UInt64 {
        guard self > .zero else { return 0 }
        let parts = components
        let fromSeconds = UInt64(parts.seconds).multipliedReportingOverflow(
            by: 1_000_000)
        if fromSeconds.overflow { return .max }
        /* 1 microsecond == 10^12 attoseconds */
        let fromAtto = UInt64(parts.attoseconds / 1_000_000_000_000)
        let total = fromSeconds.partialValue.addingReportingOverflow(fromAtto)
        return total.overflow ? .max : total.partialValue
    }
}
