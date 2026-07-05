import Testing
import Foundation
@testable import MoQ

/// Establish, subscribe, accept, drain events. Returns both sessions
/// and the server-side subscription handle for writing objects.
private func makeSubscribedPair(
    track: TrackName = TrackName(namespace: ["test"], name: "data")
) throws -> (client: Session, server: Session, serverSub: Subscription, clientSub: Subscription) {
    let (client, server) = try makeEstablishedPair()

    let clientSub = try client.subscribe(SubscribeConfiguration(track: track))
    try pumpControl(from: client, to: server)

    guard case .subscribeRequest(let req) = server.pollEvents().first else {
        Issue.record("Expected subscribeRequest")
        throw MoQError.internal
    }
    try server.acceptSubscribe(req.subscription)
    try pumpControl(from: server, to: client)
    _ = client.pollEvents()

    return (client, server, req.subscription, clientSub)
}

@Suite("Object Data")
struct ObjectDataTests {

    @Test("Write object, receive on subscriber")
    func writeAndReceive() throws {
        let (client, server, serverSub, clientSub) = try makeSubscribedPair()

        let sg = try server.openSubgroup(
            for: serverSub, groupID: 0, publisherPriority: 200)
        let payload = try Buffer(Data("hello".utf8))
        try server.writeObject(to: sg, objectID: 0, payload: payload)
        try server.closeSubgroup(sg)

        try pumpAll(from: server, to: client)

        let dataEvents = client.pollEvents()
        // Closing the subgroup FINs its stream, so a subgroupFinished event
        // trails the object.
        #expect(dataEvents.count == 2)
        guard case .objectReceived(let obj) = dataEvents.first else {
            Issue.record("Expected objectReceived, got \(dataEvents)")
            return
        }
        guard case .subgroupFinished(let fin) = dataEvents.last else {
            Issue.record("Expected subgroupFinished, got \(dataEvents)")
            return
        }
        #expect(fin.subscription == clientSub)
        #expect(fin.groupID == 0)
        #expect(fin.subgroupID == 0)
        #expect(fin.endOfGroup == false)

        #expect(obj.subscription == clientSub)
        #expect(obj.groupID == 0)
        #expect(obj.subgroupID == 0)
        #expect(obj.objectID == 0)
        #expect(obj.publisherPriority == 200)
        #expect(obj.status == .normal)
        #expect(obj.isDatagram == false)

        guard let received = obj.payload else {
            Issue.record("Expected non-nil payload")
            return
        }
        #expect(received.copyBytes() == Data("hello".utf8))
    }

    @Test("Payload Buffer remains valid after pollEvents returns")
    func payloadLifetime() throws {
        let (client, server, serverSub, _) = try makeSubscribedPair(
            track: TrackName(namespace: ["test"], name: "lifetime"))

        let sg = try server.openSubgroup(for: serverSub, groupID: 0)
        try server.writeObject(
            to: sg, objectID: 0, payload: try Buffer(Data("owned".utf8)))
        try server.closeSubgroup(sg)
        try pumpAll(from: server, to: client)

        let events = client.pollEvents()
        guard case .objectReceived(let obj) = events.first else {
            Issue.record("Expected objectReceived")
            return
        }

        let buf = obj.payload!
        #expect(buf.count == 5)
        #expect(buf.copyBytes() == Data("owned".utf8))
    }

    @Test("End-of-group flag propagates")
    func endOfGroupFlag() throws {
        let (client, server, serverSub, _) = try makeSubscribedPair(
            track: TrackName(namespace: ["test"], name: "eog"))

        let sg = try server.openSubgroup(
            for: serverSub, groupID: 1, endOfGroup: true)
        try server.writeObject(
            to: sg, objectID: 0, payload: try Buffer(Data("eog".utf8)))
        try server.closeSubgroup(sg)
        try pumpAll(from: server, to: client)

        let events = client.pollEvents()
        guard case .objectReceived(let obj) = events.first else {
            Issue.record("Expected objectReceived")
            return
        }
        #expect(obj.endOfGroup == true)
        #expect(obj.groupID == 1)

        // The END_OF_GROUP bit propagates to the subgroupFinished event too.
        guard case .subgroupFinished(let fin) = events.last else {
            Issue.record("Expected subgroupFinished, got \(events)")
            return
        }
        #expect(fin.endOfGroup == true)
        #expect(fin.groupID == 1)
    }

    @Test("Multiple objects in one subgroup")
    func multipleObjects() throws {
        let (client, server, serverSub, clientSub) = try makeSubscribedPair(
            track: TrackName(namespace: ["test"], name: "multi"))

        let sg = try server.openSubgroup(for: serverSub, groupID: 0)
        try server.writeObject(
            to: sg, objectID: 0, payload: try Buffer(Data("first".utf8)))
        try server.writeObject(
            to: sg, objectID: 1, payload: try Buffer(Data("second".utf8)))
        try server.closeSubgroup(sg)
        try pumpAll(from: server, to: client)

        let events = client.pollEvents()
        // Two objects, then the subgroupFinished from closing the stream.
        #expect(events.count == 3)

        guard case .objectReceived(let obj0) = events[0],
              case .objectReceived(let obj1) = events[1],
              case .subgroupFinished(let fin) = events[2] else {
            Issue.record("Expected two objectReceived then subgroupFinished, got \(events)")
            return
        }
        #expect(fin.subscription == clientSub)
        #expect(fin.groupID == 0)
        #expect(fin.subgroupID == 0)
        #expect(fin.endOfGroup == false)
        #expect(obj0.objectID == 0)
        #expect(obj1.objectID == 1)
        #expect(obj0.payload?.copyBytes() == Data("first".utf8))
        #expect(obj1.payload?.copyBytes() == Data("second".utf8))
    }
}

@Suite("Object Properties")
struct ObjectPropertiesTests {

    @Test("Write object with properties, receive both")
    func writeWithProperties() throws {
        let (client, server, serverSub, _) = try makeSubscribedPair(
            track: TrackName(namespace: ["test"], name: "props"))

        let sg = try server.openSubgroup(
            for: serverSub, groupID: 0, objectProperties: true)

        let propsBytes: [UInt8] = [0x01, 0x03, 0xCA, 0xFE, 0x00]
        let config = ObjectConfiguration(
            objectID: 0,
            payload: try Buffer(Data("frame".utf8)),
            properties: try Buffer(Data(propsBytes)))
        try server.writeObject(to: sg, config)
        try server.closeSubgroup(sg)

        try pumpAll(from: server, to: client)

        let events = client.pollEvents()
        guard case .objectReceived(let obj) = events.first else {
            Issue.record("Expected objectReceived, got \(events)")
            return
        }

        #expect(obj.payload?.copyBytes() == Data("frame".utf8))

        guard let props = obj.properties else {
            Issue.record("Expected non-nil properties")
            return
        }
        #expect(props.copyBytes() == Data(propsBytes))
    }

    @Test("Properties Buffer survives after pollEvents")
    func propertiesLifetime() throws {
        let (client, server, serverSub, _) = try makeSubscribedPair(
            track: TrackName(namespace: ["test"], name: "props-life"))

        // KVP: type=1 (odd → length-prefixed), length=1, value=0xAB
        let propsBytes: [UInt8] = [0x01, 0x01, 0xAB]

        let sg = try server.openSubgroup(
            for: serverSub, groupID: 0, objectProperties: true)
        let config = ObjectConfiguration(
            objectID: 0,
            payload: try Buffer(Data("p".utf8)),
            properties: try Buffer(Data(propsBytes)))
        try server.writeObject(to: sg, config)
        try server.closeSubgroup(sg)
        try pumpAll(from: server, to: client)

        let events = client.pollEvents()
        guard case .objectReceived(let obj) = events.first else {
            Issue.record("Expected objectReceived, got \(events)")
            return
        }

        let receivedProps = obj.properties!
        #expect(receivedProps.count == 3)
        #expect(receivedProps.copyBytes() == Data(propsBytes))
    }

    @Test("Properties on non-properties subgroup returns error")
    func propertiesWithoutFlag() throws {
        let (_, server, serverSub, _) = try makeSubscribedPair(
            track: TrackName(namespace: ["test"], name: "no-props"))

        let sg = try server.openSubgroup(
            for: serverSub, groupID: 0, objectProperties: false)

        let payload = try Buffer(Data("x".utf8))
        let props = try Buffer(Data([0x01, 0x01, 0xFF]))
        let config = ObjectConfiguration(
            objectID: 0, payload: payload, properties: props)

        #expect(throws: MoQError.invalidArgument) {
            try server.writeObject(to: sg, config)
        }

        #expect(payload.copyBytes() == Data("x".utf8))
        #expect(props.copyBytes() == Data([0x01, 0x01, 0xFF]))
    }

    @Test("Nil properties via writeObjectEx behaves like simple write")
    func nilPropertiesViaEx() throws {
        let (client, server, serverSub, _) = try makeSubscribedPair(
            track: TrackName(namespace: ["test"], name: "nil-props"))

        let sg = try server.openSubgroup(for: serverSub, groupID: 0)
        let config = ObjectConfiguration(
            objectID: 0, payload: try Buffer(Data("plain".utf8)))
        try server.writeObject(to: sg, config)
        try server.closeSubgroup(sg)
        try pumpAll(from: server, to: client)

        guard case .objectReceived(let obj) = client.pollEvents().first else {
            Issue.record("Expected objectReceived")
            return
        }
        #expect(obj.payload?.copyBytes() == Data("plain".utf8))
        #expect(obj.properties == nil)
    }
}
