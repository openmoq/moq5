import Testing
import Foundation
import CMoQSim
@testable import MoQ

// MARK: - Pure Swift Handshake

@Suite("Swift Session Handshake")
struct SwiftHandshakeTests {

    @Test("Two Swift sessions complete setup handshake")
    func pureSwiftHandshake() throws {
        var clientCfg = Session.Configuration(perspective: .client)
        clientCfg.sendRequestCapacity = true
        clientCfg.initialRequestCapacity = 16

        var serverCfg = Session.Configuration(perspective: .server)
        serverCfg.sendRequestCapacity = true
        serverCfg.initialRequestCapacity = 16

        let client = try Session(configuration: clientCfg)
        let server = try Session(configuration: serverCfg)

        try client.start()
        #expect(client.state == .setupSent)

        try pumpControl(from: client, to: server)
        try pumpControl(from: server, to: client)

        #expect(client.state == .established)
        #expect(server.state == .established)

        let clientEvents = client.pollEvents()
        let serverEvents = server.pollEvents()

        expectSetupComplete(clientEvents, sourceLabel: "client")
        expectSetupComplete(serverEvents, sourceLabel: "server")

        #expect(clientEvents == [.setupComplete])
        #expect(serverEvents == [.setupComplete])
    }

    @Test("Draining events twice returns empty")
    func drainEventsTwice() throws {
        var clientCfg = Session.Configuration(perspective: .client)
        clientCfg.sendRequestCapacity = true
        clientCfg.initialRequestCapacity = 16

        var serverCfg = Session.Configuration(perspective: .server)
        serverCfg.sendRequestCapacity = true
        serverCfg.initialRequestCapacity = 16

        let client = try Session(configuration: clientCfg)
        let server = try Session(configuration: serverCfg)

        try client.start()
        try pumpControl(from: client, to: server)
        try pumpControl(from: server, to: client)

        let first = client.pollEvents()
        #expect(!first.isEmpty)

        let second = client.pollEvents()
        #expect(second.isEmpty)
    }
}

// MARK: - CMoQSim Linkage Smoke Test

@Suite("CMoQSim SimPair Smoke")
struct SimPairSmokeTests {

    @Test("SimPair C-level handshake verifies CMoQSim linkage")
    func simpairLinkageSmoke() throws {
        var cfg = moq_simpair_cfg_t()
        cfg.struct_size = UInt32(MemoryLayout<moq_simpair_cfg_t>.size)
        let alloc = moq_alloc_default()!
        cfg.alloc = alloc
        cfg.seed = 42
        cfg.initial_now_us = 1000
        cfg.client_send_request_capacity = true
        cfg.client_initial_request_capacity = 16
        cfg.server_send_request_capacity = true
        cfg.server_initial_request_capacity = 16

        var sp: OpaquePointer?
        let createRC = moq_simpair_create(&cfg, &sp)
        #expect(createRC == MOQ_OK)
        guard let simpair = sp else {
            Issue.record("SimPair create failed")
            return
        }
        defer { moq_simpair_destroy(simpair) }

        moq_simpair_start(simpair)
        moq_simpair_run_until_quiescent(simpair, 8, nil)

        let client = moq_simpair_client(simpair)!
        let server = moq_simpair_server(simpair)!

        var ev = moq_event_t()
        if moq_session_poll_events(client, &ev, 1) == 1 {
            moq_event_cleanup(&ev)
        }
        if moq_session_poll_events(server, &ev, 1) == 1 {
            moq_event_cleanup(&ev)
        }

        #expect(moq_session_state(client) == MOQ_SESS_ESTABLISHED)
        #expect(moq_session_state(server) == MOQ_SESS_ESTABLISHED)
    }
}
