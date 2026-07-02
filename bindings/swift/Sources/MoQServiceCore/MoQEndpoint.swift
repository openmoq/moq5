import Foundation

/// A client connection to a MoQ relay: one QUIC/WebTransport connection, one
/// network thread (owned by the C service tier), and up to one
/// ``MediaReceiver`` plus one ``MediaSender`` attachment.
///
/// This is the model-layer shell: configuration, state, and the interrupt
/// latch are live; the connect/wait engine (service thread, streams,
/// teardown) lands with the C bridge slice. Async lifecycle methods throw
/// ``MoQServiceError/unsupported`` until then.
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
    package let backend: any EndpointBackend

    package init(configuration: Configuration, backend: any EndpointBackend) {
        self.configuration = configuration
        self.backend = backend
    }

    public var state: State { backend.state }
    public var negotiatedVersion: MoQVersion? { backend.negotiatedVersion }

    // MARK: Interrupt latch

    /// Set the sticky, endpoint-wide interrupt latch: every suspended call on
    /// this endpoint and its attachments throws
    /// ``MoQServiceError/interrupted`` until ``resume()``. Non-terminal;
    /// callable from any thread (GStreamer `unlock`/`unlock_stop` analog).
    public func interrupt() { backend.setInterrupted(true) }

    /// Clear the interrupt latch; suspended surfaces become usable again.
    public func resume() { backend.setInterrupted(false) }

    // MARK: Lifecycle (engine lands with the C bridge slice)

    /// Suspends until the connection is ESTABLISHED.
    public func established() async throws {
        throw MoQServiceError.unsupported   // TODO(S2): service-thread engine
    }

    /// Pre-stop flush of locally queued reliable stream data.
    public func drain(timeout: Duration) async throws {
        throw MoQServiceError.unsupported   // TODO(S2): service-thread engine
    }

    /// Tear down: children, optional drain, stop (joins the network thread),
    /// destroy. Idempotent.
    public func close(drainTimeout: Duration? = nil) async {
        // TODO(S2): service-thread engine; model shell is a no-op.
    }
}
