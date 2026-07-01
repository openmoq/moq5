import Testing
import Foundation
@testable import MoQ

@Suite("Namespace Publishing")
struct PublishNamespaceTests {

    @Test("Full namespace workflow: publish, accept, done")
    func publishAcceptDone() throws {
        let (client, server) = try makeEstablishedPair()

        let ns: Namespace = ["moq-test", "interop"]
        let ann = try client.publishNamespace(ns)
        #expect(ann.isValid)

        try pumpControl(from: client, to: server)

        let serverEvents = server.pollEvents()
        #expect(serverEvents.count == 1)
        guard case .namespacePublished(let info) = serverEvents.first else {
            Issue.record("Expected namespacePublished, got \(serverEvents)")
            return
        }
        #expect(info.namespace == ns)
        #expect(info.announcement.isValid)

        try server.acceptNamespace(info.announcement)

        try pumpControl(from: server, to: client)

        let clientEvents = client.pollEvents()
        #expect(clientEvents.count == 1)
        guard case .namespaceAccepted(let accepted) = clientEvents.first else {
            Issue.record("Expected namespaceAccepted, got \(clientEvents)")
            return
        }
        #expect(accepted == ann)

        try client.publishNamespaceDone(ann)

        try pumpControl(from: client, to: server)

        let doneEvents = server.pollEvents()
        #expect(doneEvents.count == 1)
        guard case .namespaceDone(let doneAnn) = doneEvents.first else {
            Issue.record("Expected namespaceDone, got \(doneEvents)")
            return
        }
        #expect(doneAnn == info.announcement)
    }

    @Test("Namespace events drain to empty on second poll")
    func drainNamespaceEventsTwice() throws {
        let (client, server) = try makeEstablishedPair()

        try client.publishNamespace(["drain", "test"])
        try pumpControl(from: client, to: server)

        let first = server.pollEvents()
        #expect(!first.isEmpty)

        let second = server.pollEvents()
        #expect(second.isEmpty)
    }

    @Test("Publish namespace on idle session fails")
    func publishBeforeEstablished() throws {
        let client = try Session(
            configuration: Session.Configuration(perspective: .client))

        #expect(throws: MoQError.self) {
            _ = try client.publishNamespace(["too", "early"])
        }
    }

    @Test("Announcement handle equality across events")
    func announcementEquality() throws {
        let (client, server) = try makeEstablishedPair()

        let ann = try client.publishNamespace(["eq", "test"])
        try pumpControl(from: client, to: server)

        let events = server.pollEvents()
        guard case .namespacePublished(let info) = events.first else {
            Issue.record("Expected namespacePublished")
            return
        }

        try server.acceptNamespace(info.announcement)
        try pumpControl(from: server, to: client)

        let accepted = client.pollEvents()
        guard case .namespaceAccepted(let handle) = accepted.first else {
            Issue.record("Expected namespaceAccepted")
            return
        }

        #expect(handle == ann)
    }
}
