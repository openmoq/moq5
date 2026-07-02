import Testing
import Foundation
import CMoQService
@testable import MoQService

/* Deterministic, NO-SOCKET bridge tests: configuration mapping into the
 * real C structs, enum/version round-trips, result mapping, and synchronous
 * preflight rejection. Nothing here calls moq_endpoint_connect with a
 * dialable configuration; real connect/attach/close smokes are gated behind
 * MOQ_SERVICE_NET_SMOKE=1 (never default CI). */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func string(_ bytes: moq_bytes_t) -> String {
    guard let data = bytes.data, bytes.len > 0 else { return "" }
    return String(decoding: UnsafeBufferPointer(start: data, count: bytes.len),
                  as: UTF8.self)
}

@Suite("Endpoint configuration mapping")
struct EndpointConfigurationMappingTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The model configuration maps field-for-field into moq_endpoint_cfg_t")
    func fullMapping() throws {
        var configuration = MoQEndpoint.Configuration(
            url: URL(string: "https://relay.example.com:4443/session")!)
        configuration.transportProtocol = .webTransport
        configuration.backend = .picoquic
        configuration.serverName = "relay.example.com"
        configuration.insecureSkipVerify = true
        configuration.webTransportPath = "/moq"

        withEndpointCFG(configuration) { cfg in
            #expect(string(cfg.url) == "https://relay.example.com:4443/session")
            #expect(cfg.`protocol` == MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT)
            #expect(cfg.backend == MOQ_TRANSPORT_BACKEND_PICOQUIC)
            #expect(string(cfg.sni) == "relay.example.com")
            #expect(cfg.insecure_skip_verify == true)
            #expect(string(cfg.wt_path) == "/moq")
            #expect(cfg.ca_file.len == 0)
            /* AUTO version offer: zero-init policy. */
            #expect(cfg.versions.policy == MOQ_VERSION_POLICY_AUTO)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Version offers map to LIST/EXACT with the draft values")
    func versionOffers() throws {
        var configuration = MoQEndpoint.Configuration(
            url: URL(string: "moqt://h:1")!)

        configuration.versions = .list([.draft18, .draft16])
        withEndpointCFG(configuration) { cfg in
            #expect(cfg.versions.policy == MOQ_VERSION_POLICY_LIST)
            #expect(cfg.versions.version_count == 2)
            #expect(cfg.versions.versions?[0] == MOQ_VERSION_DRAFT_18)
            #expect(cfg.versions.versions?[1] == MOQ_VERSION_DRAFT_16)
        }
        configuration.versions = .exactly(.draft16)
        withEndpointCFG(configuration) { cfg in
            #expect(cfg.versions.policy == MOQ_VERSION_POLICY_EXACT)
            #expect(cfg.versions.version_count == 1)
            #expect(cfg.versions.versions?[0] == MOQ_VERSION_DRAFT_16)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Version values round-trip both directions")
    func versionRoundTrip() {
        #expect(cVersion(.draft16) == MOQ_VERSION_DRAFT_16)
        #expect(cVersion(.draft18) == MOQ_VERSION_DRAFT_18)
        #expect(swiftVersion(MOQ_VERSION_DRAFT_16) == .draft16)
        #expect(swiftVersion(MOQ_VERSION_DRAFT_18) == .draft18)
        #expect(swiftVersion(moq_version_t(rawValue: 0)) == nil)
    }
}

@Suite("Endpoint bridge preflight")
struct EndpointBridgePreflightTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("connect() rejects malformed configurations before any I/O")
    func synchronousRejection() {
        #expect(throws: MoQServiceError.self) {
            _ = try MoQEndpoint.connect(
                to: URL(string: "ftp://relay.example.com:4443")!)
        }
        #expect(throws: MoQServiceError.self) {
            _ = try MoQEndpoint.connect(to: URL(string: "moqt://")!)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The installed-stack canary is live")
    func canary() {
        #expect(moqSwiftStackGuardService() == 2)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("C result codes map to service errors")
    func resultMapping() {
        #expect(serviceError(fromCreate: moq_result_t(MOQ_ERR_UNSUPPORTED),
                             what: "x") == .unsupported)
        #expect(serviceError(fromCreate: moq_result_t(MOQ_ERR_NOMEM),
                             what: "x") == .outOfMemory)
        #expect(serviceError(fromCreate: moq_result_t(MOQ_ERR_WRONG_STATE),
                             what: "x") == .wrongState)
    }
}

@Suite("Endpoint bridge network smoke",
       .enabled(if: ProcessInfo.processInfo.environment["MOQ_SERVICE_NET_SMOKE"] == "1"))
struct EndpointBridgeNetworkSmokeTests {

    /* Real sockets: opt-in only (MOQ_SERVICE_NET_SMOKE=1), never default
     * CI. Asserts clean teardown, not connection progress. */
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Connect to a dead port, then close, tears down cleanly")
    func lifecycleSmoke() async throws {
        let endpoint = try MoQEndpoint.connect(
            to: URL(string: "moqt://127.0.0.1:1")!)
        #expect(endpoint.state == .connecting || endpoint.state == .closed)
        endpoint.interrupt()
        endpoint.resume()
        await endpoint.close()
        #expect(endpoint.state == .closed)
    }
}
