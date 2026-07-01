import Testing
import Foundation
@testable import MoQ

@Suite("Publisher Facade")
struct PublisherFacadeTests {

    @Test("Create and destroy Publisher")
    func createDestroy() throws {
        let (_, server) = try makeEstablishedPair()
        let pub = try Publisher(session: server)
        _ = pub  // alive until scope end
    }

    @Test("Tick on idle publisher succeeds")
    func tickNoOp() throws {
        let (_, server) = try makeEstablishedPair()
        let pub = try Publisher(session: server)
        try pub.tick(now: 0)
    }

    @Test("Add track returns valid handle")
    func addTrack() throws {
        let (_, server) = try makeEstablishedPair()
        let pub = try Publisher(session: server)
        let track = try pub.addTrack(
            namespace: ["test"], name: "video")
        _ = track
    }

    @Test("Add track with custom priority")
    func addTrackPriority() throws {
        let (_, server) = try makeEstablishedPair()
        let pub = try Publisher(session: server)
        let track = try pub.addTrack(
            namespace: ["test"], name: "audio",
            publisherPriority: 200)
        _ = track
    }
}

@Suite("Subscriber Facade")
struct SubscriberFacadeTests {

    @Test("Create and destroy Subscriber")
    func createDestroy() throws {
        let (client, _) = try makeEstablishedPair()
        let sub = try Subscriber(session: client)
        _ = sub
    }

    @Test("Tick on idle subscriber succeeds")
    func tickNoOp() throws {
        let (client, _) = try makeEstablishedPair()
        let sub = try Subscriber(session: client)
        try sub.tick(now: 0)
    }

    @Test("Subscribe returns pending track")
    func subscribePending() throws {
        let (client, _) = try makeEstablishedPair()
        let sub = try Subscriber(session: client)
        let track = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "video"))
        #expect(track.state == .pending)
        #expect(!track.isActive)
    }

    @Test("absoluteStart filter throws invalidArgument")
    func absoluteStartRejected() throws {
        let (client, _) = try makeEstablishedPair()
        let sub = try Subscriber(session: client)

        #expect(throws: MoQError.invalidArgument) {
            _ = try sub.subscribe(
                track: TrackName(namespace: ["t"], name: "x"),
                filter: .absoluteStart(group: 0, object: 0))
        }

        let actions = sub.underlyingSession.pollActions()
        #expect(actions.isEmpty)
    }

    @Test("absoluteRange filter throws invalidArgument")
    func absoluteRangeRejected() throws {
        let (client, _) = try makeEstablishedPair()
        let sub = try Subscriber(session: client)

        #expect(throws: MoQError.invalidArgument) {
            _ = try sub.subscribe(
                track: TrackName(namespace: ["t"], name: "x"),
                filter: .absoluteRange(startGroup: 0, startObject: 0, endGroup: 1))
        }

        let actions = sub.underlyingSession.pollActions()
        #expect(actions.isEmpty)
    }

    @Test("unknown filter throws invalidArgument")
    func unknownFilterRejected() throws {
        let (client, _) = try makeEstablishedPair()
        let sub = try Subscriber(session: client)

        #expect(throws: MoQError.invalidArgument) {
            _ = try sub.subscribe(
                track: TrackName(namespace: ["t"], name: "x"),
                filter: .unknown(99))
        }

        let actions = sub.underlyingSession.pollActions()
        #expect(actions.isEmpty)
    }
}

/// Set up an established pub/sub pair with one active track.
private func makeFacadePair(
    trackNamespace: Namespace = ["test"],
    trackName: String = "data"
) throws -> (pub: Publisher, sub: Subscriber, pubTrack: PublisherTrack, subTrack: SubscribedTrack) {
    let (client, server) = try makeEstablishedPair()
    let pub = try Publisher(
        session: server, configuration: .init(acceptMode: .acceptAll))
    let sub = try Subscriber(session: client)

    let pubTrack = try pub.addTrack(namespace: trackNamespace, name: trackName)
    let subTrack = try sub.subscribe(
        track: TrackName(namespace: trackNamespace, name: trackName))

    try pumpControl(from: client, to: server)
    try pub.tick(now: 0)
    try pumpAll(from: server, to: client)
    try sub.tick(now: 0)
    #expect(subTrack.isActive)

    return (pub, sub, pubTrack, subTrack)
}

@Suite("Pub/Sub Handshake")
struct PubSubHandshakeTests {

    @Test("Full subscribe handshake via facades")
    func facadeHandshake() throws {
        let (client, server) = try makeEstablishedPair()

        let pub = try Publisher(
            session: server,
            configuration: .init(acceptMode: .acceptAll))
        let sub = try Subscriber(session: client)

        try pub.addTrack(namespace: ["test"], name: "video")

        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "video"))
        #expect(subTrack.state == .pending)

        // Pump SUBSCRIBE from client to server.
        try pumpControl(from: client, to: server)

        // Publisher processes SUBSCRIBE event, auto-accepts.
        try pub.tick(now: 0)

        // Pump SUBSCRIBE_OK from server to client.
        try pumpAll(from: server, to: client)

        // Subscriber processes SUBSCRIBE_OK event.
        try sub.tick(now: 0)

        #expect(subTrack.state == .active)
        #expect(subTrack.isActive)
    }

    @Test("Full object delivery via facades")
    func facadeObjectDelivery() throws {
        let (client, server) = try makeEstablishedPair()

        let pub = try Publisher(
            session: server,
            configuration: .init(acceptMode: .acceptAll))
        let sub = try Subscriber(session: client)

        let pubTrack = try pub.addTrack(namespace: ["test"], name: "obj")
        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "obj"))

        try pumpControl(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)
        #expect(subTrack.isActive)

        let payload = try Buffer(Data("hello facade".utf8))
        try pub.writeObject(
            to: pubTrack, groupID: 0, objectID: 0, payload: payload)

        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)

        let obj = try sub.pollObject()
        #expect(obj != nil)
        #expect(obj?.groupID == 0)
        #expect(obj?.objectID == 0)
        #expect(obj?.payload?.copyBytes() == Data("hello facade".utf8))
        #expect(obj?.status == .normal)
    }

    @Test("Payload survives after pollObject returns")
    func payloadLifetime() throws {
        let (client, server) = try makeEstablishedPair()

        let pub = try Publisher(
            session: server,
            configuration: .init(acceptMode: .acceptAll))
        let sub = try Subscriber(session: client)

        let pubTrack = try pub.addTrack(namespace: ["test"], name: "life")
        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "life"))

        try pumpControl(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)
        #expect(subTrack.isActive)

        try pub.writeObject(
            to: pubTrack, groupID: 0, objectID: 0,
            payload: try Buffer(Data("owned".utf8)))

        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)

        let obj = try sub.pollObject()!
        let buf = obj.payload!
        #expect(buf.count == 5)
        #expect(buf.copyBytes() == Data("owned".utf8))
    }

    @Test("Properties round-trip via facades")
    func propertiesRoundTrip() throws {
        let (client, server) = try makeEstablishedPair()

        let pub = try Publisher(
            session: server,
            configuration: .init(acceptMode: .acceptAll))
        let sub = try Subscriber(session: client)

        let pubTrack = try pub.addTrack(namespace: ["test"], name: "props")
        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "props"))

        try pumpControl(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)
        #expect(subTrack.isActive)

        // Valid D16 KVP: type=1 (odd, length-prefixed), length=3, value
        let propsBytes: [UInt8] = [0x01, 0x03, 0xCA, 0xFE, 0x00]
        let config = PublishObjectConfiguration(
            groupID: 0,
            objectID: 0,
            payload: try Buffer(Data("frame".utf8)),
            properties: try Buffer(Data(propsBytes)))
        try pub.writeObject(to: pubTrack, config)

        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)

        let obj = try sub.pollObject()!
        #expect(obj.payload?.copyBytes() == Data("frame".utf8))
        #expect(obj.properties?.copyBytes() == Data(propsBytes))
    }

    @Test("End-of-group propagates via facades")
    func endOfGroupFacade() throws {
        let (client, server) = try makeEstablishedPair()

        let pub = try Publisher(
            session: server,
            configuration: .init(acceptMode: .acceptAll))
        let sub = try Subscriber(session: client)

        let pubTrack = try pub.addTrack(namespace: ["test"], name: "eog")
        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "eog"))

        try pumpControl(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)
        #expect(subTrack.isActive)

        let config = PublishObjectConfiguration(
            groupID: 1, objectID: 0,
            payload: try Buffer(Data("last".utf8)),
            endOfGroup: true)
        try pub.writeObject(to: pubTrack, config)

        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)

        let obj = try sub.pollObject()!
        #expect(obj.groupID == 1)
        #expect(obj.endOfGroup == true)
    }

    @Test("drainObjects drains FIFO, second drain empty")
    func drainObjectsFIFO() throws {
        let (client, server) = try makeEstablishedPair()

        let pub = try Publisher(
            session: server,
            configuration: .init(acceptMode: .acceptAll))
        let sub = try Subscriber(session: client)

        let pubTrack = try pub.addTrack(namespace: ["test"], name: "fifo")
        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "fifo"))

        try pumpControl(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)
        #expect(subTrack.isActive)

        try pub.writeObject(
            to: pubTrack, groupID: 0, objectID: 0,
            payload: try Buffer(Data("first".utf8)))
        try pub.writeObject(
            to: pubTrack, groupID: 0, objectID: 1,
            payload: try Buffer(Data("second".utf8)))

        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)

        let objects = try sub.pollAllObjects()
        #expect(objects.count == 2)
        #expect(objects[0].objectID == 0)
        #expect(objects[1].objectID == 1)
        #expect(objects[0].payload?.copyBytes() == Data("first".utf8))
        #expect(objects[1].payload?.copyBytes() == Data("second".utf8))

        let empty = try sub.pollAllObjects()
        #expect(empty.isEmpty)
    }

    @Test("Reject-all mode rejects subscribe")
    func rejectAll() throws {
        let (client, server) = try makeEstablishedPair()

        let pub = try Publisher(
            session: server,
            configuration: .init(acceptMode: .rejectAll))
        let sub = try Subscriber(session: client)

        try pub.addTrack(namespace: ["test"], name: "rejected")

        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "rejected"))

        try pumpControl(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)

        #expect(subTrack.state == .error)
        #expect(!subTrack.isActive)
    }

    @Test("Received object carries correct track identity")
    func objectTrackIdentity() throws {
        let (pub, sub, pubTrack, subTrack) = try makeFacadePair(
            trackName: "identity")

        try pub.writeObject(
            to: pubTrack, groupID: 0, objectID: 0,
            payload: try Buffer(Data("id".utf8)))
        try pumpAll(from: pub.underlyingSession, to: sub.underlyingSession)
        try sub.tick(now: 0)

        let obj = try sub.pollObject()!
        #expect(obj.track === subTrack)
    }

    @Test("Multi-track: objects carry correct track identity")
    func multiTrackIdentity() throws {
        let (client, server) = try makeEstablishedPair()
        let pub = try Publisher(
            session: server, configuration: .init(acceptMode: .acceptAll))
        let sub = try Subscriber(session: client)

        let audioTrack = try pub.addTrack(namespace: ["test"], name: "audio")
        let videoTrack = try pub.addTrack(namespace: ["test"], name: "video")

        // Subscribe and settle audio first.
        let subAudio = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "audio"))
        try pumpAll(from: client, to: server)
        try pumpAll(from: server, to: client)
        try pub.tick(now: 0)
        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)
        #expect(subAudio.isActive)

        // Subscribe and settle video.
        let subVideo = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "video"))
        try pumpAll(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)
        #expect(subVideo.isActive)

        try pub.writeObject(
            to: audioTrack, groupID: 0, objectID: 0,
            payload: try Buffer(Data("beep".utf8)))
        try pub.writeObject(
            to: videoTrack, groupID: 0, objectID: 0,
            payload: try Buffer(Data("frame".utf8)))

        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)

        let objects = try sub.pollAllObjects()
        #expect(objects.count == 2)

        let audioObj = objects.first { $0.track === subAudio }
        let videoObj = objects.first { $0.track === subVideo }
        #expect(audioObj?.payload?.copyBytes() == Data("beep".utf8))
        #expect(videoObj?.payload?.copyBytes() == Data("frame".utf8))
    }

    @Test("Status object via datagram")
    func statusObject() throws {
        let (pub, sub, pubTrack, _) = try makeFacadePair(
            trackName: "status")

        try pub.writeObject(
            to: pubTrack, groupID: 0, objectID: 0,
            payload: try Buffer(Data("data".utf8)))

        let statusCfg = PublishObjectConfiguration(
            groupID: 0, objectID: 1,
            status: .endOfTrack)
        try pub.writeObject(to: pubTrack, statusCfg)

        try pumpAll(from: pub.underlyingSession, to: sub.underlyingSession)
        try sub.tick(now: 0)

        let objects = try sub.pollAllObjects()
        #expect(objects.count >= 1)
    }

    @Test("Normal config with nil payload throws invalidArgument")
    func nilPayloadThrows() throws {
        let (pub, _, pubTrack, _) = try makeFacadePair(
            trackName: "nilpay")

        var config = PublishObjectConfiguration(
            groupID: 0, objectID: 0,
            payload: try Buffer(Data("x".utf8)))
        config.payload = nil

        #expect(throws: MoQError.self) {
            try pub.writeObject(to: pubTrack, config)
        }
    }

    @Test("Unknown status throws invalidArgument")
    func unknownStatusThrows() throws {
        let (pub, _, pubTrack, _) = try makeFacadePair(
            trackName: "unkstatus")

        let config = PublishObjectConfiguration(
            groupID: 0, objectID: 0, status: .unknown(99))

        #expect(throws: MoQError.invalidArgument) {
            try pub.writeObject(to: pubTrack, config)
        }

        let actions = pub.underlyingSession.pollActions()
        #expect(actions.isEmpty)
    }

    @Test("pollObject returns nil on empty queue")
    func pollObjectEmpty() throws {
        let (_, sub, _, _) = try makeFacadePair(trackName: "empty")
        let obj = try sub.pollObject()
        #expect(obj == nil)
    }

    @Test("drainObjects on empty queue succeeds with no callback")
    func drainObjectsEmpty() throws {
        let (_, sub, _, _) = try makeFacadePair(trackName: "drain-empty")
        var count = 0
        try sub.drainObjects { _ in count += 1 }
        #expect(count == 0)
    }

    @Test("pollAllObjects second call returns empty")
    func pollAllObjectsTwice() throws {
        let (pub, sub, pubTrack, _) = try makeFacadePair(
            trackName: "twice")

        try pub.writeObject(
            to: pubTrack, groupID: 0, objectID: 0,
            payload: try Buffer(Data("once".utf8)))
        try pumpAll(from: pub.underlyingSession, to: sub.underlyingSession)
        try sub.tick(now: 0)

        let first = try sub.pollAllObjects()
        #expect(first.count == 1)

        let second = try sub.pollAllObjects()
        #expect(second.isEmpty)
    }
}

@Suite("Retained Group")
struct RetainedGroupTests {

    @Test("Set and clear retained group does not disturb caller Buffer")
    func setClearRetainedGroup() throws {
        let (_, server) = try makeEstablishedPair()
        let pub = try Publisher(
            session: server, configuration: .init(acceptMode: .acceptAll))
        let track = try pub.addTrack(namespace: ["test"], name: "catalog")

        let payload = try Buffer(Data("catalog".utf8))
        let config = RetainedGroupConfiguration(
            groupID: 0, objects: [RetainedObject(objectID: 0, payload: payload)])
        try pub.setRetainedGroup(on: track, config)

        #expect(payload.copyBytes() == Data("catalog".utf8))

        try pub.clearRetainedGroup(on: track)

        #expect(payload.copyBytes() == Data("catalog".utf8))
    }

    @Test("maxRetainedBytes rejects oversized payload")
    func maxRetainedBytesRejects() throws {
        let (_, server) = try makeEstablishedPair()
        let pub = try Publisher(
            session: server, configuration: .init(acceptMode: .acceptAll))

        let track = try pub.addTrack(
            namespace: ["test"], name: "small-catalog",
            maxRetainedBytes: 4)

        let bigPayload = try Buffer(Data(repeating: 0xAA, count: 16))

        #expect(throws: MoQError.invalidArgument) {
            try pub.setRetainedGroup(on: track, RetainedGroupConfiguration(
                groupID: 0,
                objects: [RetainedObject(objectID: 0, payload: bigPayload)]))
        }

        #expect(bigPayload.count == 16)
    }

    @Test("Clear when no retained group is set succeeds")
    func clearNoRetainedGroup() throws {
        let (_, server) = try makeEstablishedPair()
        let pub = try Publisher(
            session: server, configuration: .init(acceptMode: .acceptAll))
        let track = try pub.addTrack(namespace: ["test"], name: "no-catalog")

        try pub.clearRetainedGroup(on: track)
    }
}
