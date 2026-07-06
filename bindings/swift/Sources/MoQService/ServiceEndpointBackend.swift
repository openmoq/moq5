import Foundation
import CMoQService
import MoQServiceCore

/* -- C interop helpers ---------------------------------------------------- */

/// Owns stable byte buffers for `moq_bytes_t` fields borrowed by a C call:
/// the arena outlives the call (via withExtendedLifetime at the call site),
/// and the C side copies what it keeps before returning (the cfg contract).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class CBytesArena {
    private var byteAllocations: [UnsafeMutableBufferPointer<UInt8>] = []
    private var versionAllocations: [UnsafeMutableBufferPointer<moq_version_t>] = []

    deinit {
        for allocation in byteAllocations { allocation.deallocate() }
        for allocation in versionAllocations { allocation.deallocate() }
        for allocation in namespaceAllocations { allocation.deallocate() }
    }

    func bytes(_ string: String?) -> moq_bytes_t {
        guard let string, !string.isEmpty else {
            return moq_bytes_t(data: nil, len: 0)
        }
        return bytes(Array(string.utf8))
    }

    func bytes(_ data: Data?) -> moq_bytes_t {
        guard let data, !data.isEmpty else {
            return moq_bytes_t(data: nil, len: 0)
        }
        return bytes(Array(data))
    }

    private func bytes(_ utf8: [UInt8]) -> moq_bytes_t {
        let buffer = UnsafeMutableBufferPointer<UInt8>.allocate(
            capacity: utf8.count)
        _ = buffer.initialize(fromContentsOf: utf8)
        byteAllocations.append(buffer)
        return moq_bytes_t(data: UnsafePointer(buffer.baseAddress),
                           len: utf8.count)
    }

    private var namespaceAllocations: [UnsafeMutableBufferPointer<moq_bytes_t>] = []

    func namespaceParts(_ parts: [String]) -> moq_namespace_t {
        guard !parts.isEmpty else { return moq_namespace_t(parts: nil, count: 0) }
        let buffer = UnsafeMutableBufferPointer<moq_bytes_t>.allocate(
            capacity: parts.count)
        _ = buffer.initialize(fromContentsOf: parts.map { bytes($0) })
        namespaceAllocations.append(buffer)
        return moq_namespace_t(parts: UnsafePointer(buffer.baseAddress),
                               count: parts.count)
    }

    func versions(_ versions: [MoQVersion])
        -> (UnsafePointer<moq_version_t>?, Int) {
        guard !versions.isEmpty else { return (nil, 0) }
        let buffer = UnsafeMutableBufferPointer<moq_version_t>.allocate(
            capacity: versions.count)
        _ = buffer.initialize(fromContentsOf: versions.map(cVersion))
        versionAllocations.append(buffer)
        return (UnsafePointer(buffer.baseAddress), versions.count)
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func cVersion(_ version: MoQVersion) -> moq_version_t {
    switch version {
    case .draft16: MOQ_VERSION_DRAFT_16
    case .draft18: MOQ_VERSION_DRAFT_18
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func swiftVersion(_ version: moq_version_t) -> MoQVersion? {
    switch version {
    case MOQ_VERSION_DRAFT_16: .draft16
    case MOQ_VERSION_DRAFT_18: .draft18
    default: nil
    }
}

/// Map a C create/command failure to the service error (terminal states are
/// discriminated by the caller, which has the backend snapshots).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func serviceError(fromCreate rc: moq_result_t, what: String) -> MoQServiceError {
    switch rc {
    case moq_result_t(MOQ_ERR_INVAL):
        return .invalidArgument("\(what): the configuration was refused")
    case moq_result_t(MOQ_ERR_UNSUPPORTED):
        return .unsupported
    case moq_result_t(MOQ_ERR_NOMEM):
        return .outOfMemory
    case moq_result_t(MOQ_ERR_WRONG_STATE):
        return .wrongState
    case moq_result_t(MOQ_ERR_CLOSED):
        return .closed
    default:
        return .internalError(Int32(rc))
    }
}

/* -- Configuration mapping ------------------------------------------------ */

/// Build the C endpoint cfg from the model configuration and run `body`
/// with it; every borrowed buffer stays alive for the call (the C side
/// copies what it retains before `moq_endpoint_connect` returns).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func withEndpointCFG<T>(
    _ configuration: MoQEndpoint.Configuration,
    _ body: (inout moq_endpoint_cfg_t) throws -> T
) rethrows -> T {
    let arena = CBytesArena()
    var cfg = moq_endpoint_cfg_t()
    moq_endpoint_cfg_init(&cfg)
    cfg.url = arena.bytes(configuration.url.absoluteString)
    cfg.`protocol` = cProtocol(configuration.transportProtocol)
    cfg.backend = cBackend(configuration.backend)
    switch configuration.versions {
    case .automatic:
        break                       /* zero-init == AUTO (offer everything) */
    case .list(let versions):
        let (pointer, count) = arena.versions(versions)
        cfg.versions.policy = MOQ_VERSION_POLICY_LIST
        cfg.versions.versions = pointer
        cfg.versions.version_count = count
    case .exactly(let version):
        let (pointer, count) = arena.versions([version])
        cfg.versions.policy = MOQ_VERSION_POLICY_EXACT
        cfg.versions.versions = pointer
        cfg.versions.version_count = count
    }
    cfg.sni = arena.bytes(configuration.serverName)
    cfg.ca_file = arena.bytes(configuration.caFileURL?.path)
    cfg.insecure_skip_verify = configuration.insecureSkipVerify
    cfg.wt_path = arena.bytes(configuration.webTransportPath)
    return try withExtendedLifetime(arena) {
        try body(&cfg)
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func cProtocol(_ transportProtocol: MoQEndpoint.TransportProtocol)
    -> moq_transport_protocol_t {
    switch transportProtocol {
    case .automatic: MOQ_TRANSPORT_PROTOCOL_AUTO
    case .rawQUIC: MOQ_TRANSPORT_PROTOCOL_RAW_QUIC
    case .webTransport: MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func cBackend(_ backend: MoQEndpoint.TransportBackend)
    -> moq_transport_backend_t {
    switch backend {
    case .automatic: MOQ_TRANSPORT_BACKEND_AUTO
    case .picoquic: MOQ_TRANSPORT_BACKEND_PICOQUIC
    case .mvfst: MOQ_TRANSPORT_BACKEND_MVFST
    case .proxygen: MOQ_TRANSPORT_BACKEND_PROXYGEN
    }
}

/* -- The real endpoint backend -------------------------------------------- */

/// `EndpointBackend` over an owned `moq_endpoint_t`. All C endpoint APIs
/// used here are any-thread safe (the service tier's contract); the engine
/// additionally confines the blocking/teardown calls to its service thread
/// and guarantees nothing runs after `destroy()` (S2 retirement).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ServiceEndpointBackend: EndpointBackend, @unchecked Sendable {

    let endpoint: OpaquePointer

    init(endpoint: OpaquePointer) {
        self.endpoint = endpoint
    }

    var state: MoQEndpoint.State {
        switch moq_endpoint_state(endpoint) {
        case MOQ_ENDPOINT_ESTABLISHED: .established
        case MOQ_ENDPOINT_DRAINING: .draining
        case MOQ_ENDPOINT_CLOSED: .closed
        /* CONNECTING, plus the reserved RECONNECTING v0 never enters. */
        default: .connecting
        }
    }

    var negotiatedVersion: MoQVersion? {
        swiftVersion(moq_endpoint_negotiated_version(endpoint))
    }

    var isFatal: Bool { moq_endpoint_is_fatal(endpoint) }
    var fatalCode: UInt64 { moq_endpoint_fatal_code(endpoint) }

    var terminalFailure: TerminalFailure {
        var out = moq_endpoint_terminal_t()
        let rc = moq_endpoint_get_terminal(
            endpoint, &out, MemoryLayout<moq_endpoint_terminal_t>.size)
        guard rc == moq_result_t(MOQ_OK) else { return .none }
        let code = out.detail_code
        switch out.reason {
        case MOQ_ENDPOINT_TERMINAL_CLEAN:           return .clean
        case MOQ_ENDPOINT_TERMINAL_PROTOCOL:        return .protocolFatal(code: code)
        case MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE: return .tlsCertificate(code: code)
        case MOQ_ENDPOINT_TERMINAL_TLS:             return .tls(code: code)
        case MOQ_ENDPOINT_TERMINAL_TRANSPORT:       return .transport(code: code)
        default:                                    return .none
        }
    }

    func setInterrupted(_ interrupted: Bool) {
        moq_endpoint_set_interrupted(endpoint, interrupted)
    }

    func wake() {
        moq_endpoint_wake(endpoint)
    }

    func waitForActivity(timeoutMicroseconds: UInt64) -> EndpointWaitResult {
        /* Plain moq_endpoint_wait suffices as THE one blocking wait: every
         * receiver/sender state change happens on pump cycles, which mark
         * endpoint activity, and the engine re-evaluates all level
         * predicates on every pass. */
        switch moq_endpoint_wait(endpoint, timeoutMicroseconds) {
        case moq_result_t(MOQ_OK): .activity
        case moq_result_t(MOQ_DONE): .timeout
        case moq_result_t(MOQ_ERR_INTERRUPTED): .interrupted
        default: .closed
        }
    }

    func drain(timeoutMicroseconds: UInt64) -> EndpointDrainResult {
        switch moq_endpoint_drain(endpoint, timeoutMicroseconds) {
        case moq_result_t(MOQ_OK): .complete
        case moq_result_t(MOQ_DONE): .timeout
        case moq_result_t(MOQ_ERR_INTERRUPTED): .interrupted
        case moq_result_t(MOQ_ERR_UNSUPPORTED): .unsupported
        default: .closed
        }
    }

    func stop() {
        _ = moq_endpoint_stop(endpoint)
    }

    func destroy() {
        moq_endpoint_destroy(endpoint)
    }
}

/* -- Public factories ------------------------------------------------------ */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension MoQEndpoint {

    /// Create the managed transport (network thread, QUIC context, session)
    /// and begin connecting. Returns immediately -- the handshake completes
    /// asynchronously; `try await established()` to observe it. Synchronous
    /// validation failures (unrecognized scheme, missing host, a version
    /// this build cannot offer) throw before any I/O.
    public static func connect(configuration: Configuration) throws -> MoQEndpoint {
        _ = moqSwiftStackGuardService()     /* always-linked canary anchor */
        try configuration.validate()
        var endpoint: OpaquePointer?
        let rc = withEndpointCFG(configuration) { cfg in
            withUnsafePointer(to: cfg) { moq_endpoint_connect($0, &endpoint) }
        }
        guard rc == moq_result_t(MOQ_OK), let endpoint else {
            throw serviceError(fromCreate: rc, what: "connect")
        }
        return MoQEndpoint(
            configuration: configuration,
            engine: EndpointEngine(
                backend: ServiceEndpointBackend(endpoint: endpoint)))
    }

    /// ``connect(configuration:)`` with defaults: `moqt://host:port[/path]`
    /// or `https://host:port[/path]`.
    public static func connect(to url: URL) throws -> MoQEndpoint {
        try connect(configuration: Configuration(url: url))
    }
}
