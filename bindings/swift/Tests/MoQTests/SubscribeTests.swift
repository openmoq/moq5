import Testing
import Foundation
@testable import MoQ

@Suite("Subscribe Workflow")
struct SubscribeTests {

    @Test("Full subscribe: request, accept, ok")
    func subscribeAcceptOk() throws {
        let (client, server) = try makeEstablishedPair()

        let track = TrackName(namespace: ["moq-test", "interop"], name: "test-track")
        let handle = try client.subscribe(SubscribeConfiguration(track: track))
        #expect(handle.isValid)

        try pumpControl(from: client, to: server)

        let serverEvents = server.pollEvents()
        #expect(serverEvents.count == 1)
        guard case .subscribeRequest(let request) = serverEvents.first else {
            Issue.record("Expected subscribeRequest, got \(serverEvents)")
            return
        }

        #expect(request.track == track)
        #expect(request.filter == .nextGroup)
        #expect(request.subscription.isValid)

        try server.acceptSubscribe(request.subscription)

        try pumpControl(from: server, to: client)

        let clientEvents = client.pollEvents()
        #expect(clientEvents.count == 1)
        guard case .subscribeOk(let okInfo) = clientEvents.first else {
            Issue.record("Expected subscribeOk, got \(clientEvents)")
            return
        }

        #expect(okInfo.subscription == handle)
    }

    @Test("Subscribe events drain to empty on second poll")
    func drainSubscribeEventsTwice() throws {
        let (client, server) = try makeEstablishedPair()

        let track = TrackName(namespace: ["test"], name: "drain")
        try client.subscribe(SubscribeConfiguration(track: track))
        try pumpControl(from: client, to: server)

        let first = server.pollEvents()
        #expect(!first.isEmpty)

        let second = server.pollEvents()
        #expect(second.isEmpty)
    }

    @Test("Subscribe with latestObject filter")
    func subscribeLatestObject() throws {
        let (client, server) = try makeEstablishedPair()

        let track = TrackName(namespace: ["filter"], name: "test")
        let config = SubscribeConfiguration(track: track, filter: .latestObject)
        try client.subscribe(config)
        try pumpControl(from: client, to: server)

        let events = server.pollEvents()
        guard case .subscribeRequest(let req) = events.first else {
            Issue.record("Expected subscribeRequest")
            return
        }
        #expect(req.filter == .latestObject)
    }

    @Test("Subscribe with absoluteStart filter")
    func subscribeAbsoluteStart() throws {
        let (client, server) = try makeEstablishedPair()

        let track = TrackName(namespace: ["abs"], name: "start")
        let config = SubscribeConfiguration(
            track: track,
            filter: .absoluteStart(group: 5, object: 0))
        try client.subscribe(config)
        try pumpControl(from: client, to: server)

        let events = server.pollEvents()
        guard case .subscribeRequest(let req) = events.first else {
            Issue.record("Expected subscribeRequest")
            return
        }
        #expect(req.filter == .absoluteStart(group: 5, object: 0))
    }

    @Test("Subscription handle equality")
    func subscriptionEquality() throws {
        let (client, server) = try makeEstablishedPair()

        let track = TrackName(namespace: ["eq"], name: "test")
        let h1 = try client.subscribe(SubscribeConfiguration(track: track))

        try pumpControl(from: client, to: server)
        let events = server.pollEvents()

        guard case .subscribeRequest(let req) = events.first else {
            Issue.record("Expected subscribeRequest")
            return
        }

        try server.acceptSubscribe(req.subscription)
        try pumpControl(from: server, to: client)

        let clientEvents = client.pollEvents()
        guard case .subscribeOk(let ok) = clientEvents.first else {
            Issue.record("Expected subscribeOk")
            return
        }

        #expect(ok.subscription == h1)
    }

    @Test("Subscribe on idle session fails")
    func subscribeBeforeStart() throws {
        let client = try Session(
            configuration: Session.Configuration(perspective: .client))
        let track = TrackName(namespace: ["fail"], name: "early")

        #expect(throws: MoQError.self) {
            _ = try client.subscribe(SubscribeConfiguration(track: track))
        }
    }
}
