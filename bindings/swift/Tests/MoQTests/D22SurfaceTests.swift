import Testing
import Foundation
import CMoQCore
@testable import MoQ
import MoQTransportModel

/// The draft-22 session surface through the Swift binding, beyond fills:
/// configuration fields reach the C session; update answers owed under
/// `deferSubscriptionUpdates` on both the SUBSCRIBE and the PUBLISH side,
/// with the owed-update and filter queries and the runtime setter; FETCH
/// with a Location Filter and FILL_TIMEOUT, the options a deferring
/// publisher sees and the Timed-Out writer; capability and version queries;
/// request terminal causes; the transport binding with a URI policy; the
/// reliable-reset size on a reset action; fills through the Subscriber
/// facade; SUBSCRIBE_TRACKS on both sides; and REQUEST_REDIRECT with the
/// handle of the request it names.
@Suite("Draft-22 surface")
struct D22SurfaceTests {

    private struct PumpError: Error {}

    private static let peerBit: [UInt64] = [1 << 40, 1 << 41]

    private func toPeer(_ ref: StreamRef, _ dir: Int) -> StreamRef {
        let back = Self.peerBit[dir ^ 1]
        if ref.rawValue & back != 0 { return StreamRef(rawValue: ref.rawValue & ~back) }
        return StreamRef(rawValue: ref.rawValue | Self.peerBit[dir])
    }

    /// Request streams each direction opened, as the opener names them, and
    /// every data reset seen.
    private final class Observed {
        var opened: [[StreamRef]] = [[], []]
        var resets: [(errorCode: UInt64, reliableSize: UInt64)] = []
    }

    private func pump(_ from: Session, _ to: Session, _ dir: Int, _ seen: Observed?) throws {
        try from.drainActionsExtended { action in
            switch action {
            case .sendControl(let d), .openUniControl(_, let d), .sendUniControl(_, let d, _):
                try d.withUnsafeBytes { try to.receiveControlBytes($0, now: 0) }
            case .sendData(let ref, let header, let payload, let fin):
                var bytes = [UInt8](header)
                payload?.withUnsafeBytes { bytes.append(contentsOf: $0) }
                try bytes.withUnsafeBytes {
                    try to.receiveDataBytes(on: toPeer(ref, dir), $0, fin: fin, now: 0)
                }
            case .resetData(let ref, let code, let reliable):
                seen?.resets.append((code, reliable))
                try to.receiveDataReset(on: toPeer(ref, dir), errorCode: code, now: 0)
            case .stopData(let ref, let code):
                try to.receiveDataStop(on: toPeer(ref, dir), errorCode: code, now: 0)
            case .openBidiStream(let ref, let d, let fin):
                seen?.opened[dir].append(ref)
                try d.withUnsafeBytes {
                    try to.receiveBidiStreamBytes(on: toPeer(ref, dir), $0, fin: fin, now: 0)
                }
            case .sendBidiStream(let ref, let d, let fin):
                try d.withUnsafeBytes {
                    try to.receiveBidiStreamBytes(on: toPeer(ref, dir), $0, fin: fin, now: 0)
                }
            case .resetBidiStream(let ref, let code):
                try to.receiveBidiStreamReset(on: toPeer(ref, dir), errorCode: code, now: 0)
            case .stopBidiStream(let ref, let code):
                try to.receiveBidiStreamStop(on: toPeer(ref, dir), errorCode: code, now: 0)
            case .abortBidiStream(let ref, let code):
                try to.receiveBidiStreamReset(on: toPeer(ref, dir), errorCode: code, now: 0)
                try to.receiveBidiStreamStop(on: toPeer(ref, dir), errorCode: code, now: 0)
            case .sendDatagram(let d):
                try d.withUnsafeBytes { try to.receiveDatagram($0, now: 0) }
            case .closeBidiStream:
                break   // both halves done: the transport releases the stream
            case .closeSession, .unknown:
                throw PumpError()
            }
        }
    }

    private func settle(_ c: Session, _ s: Session, _ seen: Observed? = nil) throws {
        for _ in 0..<6 { try pump(c, s, 0, seen); try pump(s, c, 1, seen) }
    }

    private func pair(
        _ version: WireVersion,
        client: (inout Session.Configuration) -> Void = { _ in },
        server: (inout Session.Configuration) -> Void = { _ in }
    ) throws -> (Session, Session) {
        var cc = Session.Configuration(perspective: .client)
        cc.sendRequestCapacity = true
        cc.initialRequestCapacity = 16
        cc.wireVersion = version
        client(&cc)
        var sc = Session.Configuration(perspective: .server)
        sc.sendRequestCapacity = true
        sc.initialRequestCapacity = 16
        sc.wireVersion = version
        server(&sc)
        let c = try Session(configuration: cc)
        let s = try Session(configuration: sc)
        if c.bindingRequirements != 0 {
            try bindInMemoryTransport(c, s)
        }
        try c.start()
        if version != .draft16 { try s.start() }
        try settle(c, s)
        #expect(c.state == .established)
        #expect(s.state == .established)
        _ = c.pollEventsExtended()
        _ = s.pollEventsExtended()
        return (c, s)
    }

    private func track(_ name: String = "v") -> TrackName {
        TrackName(namespace: Namespace([NamespacePart(Data("d22".utf8))]),
                  name: Data(name.utf8))
    }

    /// Subscribe from the client and accept on the server with an optional Largest.
    private func subscribeAccepted(
        _ c: Session, _ s: Session, location: LocationFilter? = nil,
        _ seen: Observed? = nil, largest: (group: UInt64, object: UInt64)? = (3, 2)
    ) throws -> (Subscription, Subscription) {
        var cfg = SubscribeConfiguration(track: track(), filter: .latestObject)
        cfg.location = location
        let csub = try c.subscribe(cfg)
        try settle(c, s, seen)
        var ssub: Subscription?
        for ev in s.pollEventsExtended() {
            if case .subscribeRequest(let r) = ev { ssub = r.subscription }
        }
        let sub = try #require(ssub)
        var acc = AcceptSubscribeConfiguration()
        acc.largest = largest
        try s.acceptSubscribe(sub, config: acc)
        try settle(c, s, seen)
        _ = c.pollEventsExtended()
        return (csub, sub)
    }

    // MARK: - Versions and capabilities

    @Test("Capability and version queries")
    func capabilities() throws {
        let (c22, _) = try pair(.draft22)
        #expect(c22.supportsFill)
        #expect(!c22.supportsJoiningFetch)
        #expect(c22.supportsConcurrentSubscriptions)
        let (c18, _) = try pair(.draft18)
        #expect(!c18.supportsFill)
        #expect(c18.supportsJoiningFetch)
        #expect(!c18.supportsConcurrentSubscriptions)

        // The modelled versions the library offers, newest first, agreeing
        // with the per-version query.
        let vs = Session.supportedWireVersions
        #expect(!vs.isEmpty)
        for v in vs { #expect(Session.isWireVersionSupported(v)) }
        #expect(vs.map(\.draftNumber) == vs.map(\.draftNumber).sorted(by: >))
        let n = moq_versions_supported(nil, 0)
        #expect(vs.count == n)
        #expect(Session.bindingRequirements(forWireVersion: .draft16) == 0)
        #expect(Session.bindingRequirements(forWireVersion: .draft22) ==
                moq_version_binding_requirements(MOQ_VERSION_DRAFT_22))
    }

    // MARK: - Configuration

    @Test("Configuration fields reach the session")
    func configuration() throws {
        var bad = Session.Configuration(perspective: .client)
        bad.wireVersion = .draft22
        bad.maxFills = 0x10001
        #expect(throws: MoQError.invalidArgument) { try Session(configuration: bad) }
        bad.maxFills = 0x10000
        _ = try Session(configuration: bad)

        // The facades' appended fields are stamped and carried.
        var subCfg = Subscriber.Configuration()
        subCfg.maxFills = 3
        subCfg.maxFillItems = 40
        let sub = subCfg.cValue
        #expect(Int(sub.struct_size) == MemoryLayout<moq_sub_cfg_t>.size)
        #expect(sub.max_fills == 3 && sub.max_fill_items == 40)
        var pubCfg = Publisher.Configuration()
        pubCfg.maxFills = 5
        let pub = pubCfg.cValue
        #expect(Int(pub.struct_size) == MemoryLayout<moq_pub_cfg_t>.size)
        #expect(pub.max_fills == 5)
    }

    // MARK: - Subscription update answers

    @Test("A deferred subscription update is answered by the application")
    func subscriptionUpdateDeferral() throws {
        let (c, s) = try pair(.draft22, server: { $0.deferSubscriptionUpdates = true })
        let (csub, ssub) = try subscribeAccepted(c, s, location: .nextObject)
        #expect(try c.subscriptionFilter(csub) == .nextObject)
        #expect(try s.subscriptionFilter(ssub) == .nextObject)
        #expect(throws: MoQError.wrongState) { try s.owedUpdate(on: ssub) }

        var uc = SubscriptionUpdateConfiguration()
        uc.forward = false
        uc.filter = .absoluteStart(group: 5, object: 1)
        try c.updateSubscription(csub, uc)
        try settle(c, s)
        var upd: SubscriptionUpdatedInfo?
        for ev in s.pollEventsExtended() {
            if case .subscribeUpdated(let u) = ev { upd = u }
        }
        let u = try #require(upd)
        #expect(u.subscription == ssub)
        #expect(u.update.answerOwed)
        #expect(!u.update.fillRequested)
        #expect(u.update.updateCount == 1)
        #expect(u.update.forward == false)
        #expect(u.update.filter == nil)
        #expect(u.update.options?.location == .absoluteStart(group: 5, object: 1))

        let owed = try s.owedUpdate(on: ssub)
        #expect(owed.updateRequestID == u.update.updateRequestID)
        #expect(owed.filter == .absoluteStart(group: 5, object: 1))
        #expect(try s.subscriptionFilter(ssub) == .nextObject)
        #expect(!c.pollEventsExtended().contains { if case .subscriptionUpdateOk = $0 { true } else { false } })

        #expect(throws: MoQError.self) {
            try s.acceptSubscriptionUpdate(ssub, updateRequestID: u.update.updateRequestID + 2)
        }
        try s.acceptSubscriptionUpdate(ssub, updateRequestID: u.update.updateRequestID)
        try settle(c, s)
        let ack = try #require(c.pollEventsExtended().compactMap { event -> SubscriptionUpdateOkInfo? in
            if case .subscriptionUpdateOk(let value) = event { return value }; return nil
        }.first)
        #expect(ack.subscription == csub)
        #expect(ack.largest?.group == 3 && ack.largest?.object == 2)
        #expect(ack.expiresMs == nil)
        #expect(try s.subscriptionFilter(ssub) == .absoluteStart(group: 5, object: 1))
        #expect(throws: MoQError.wrongState) { try s.owedUpdate(on: ssub) }

        // Switched back, the session answers itself.
        try s.setDeferSubscriptionUpdates(false)
        var uc2 = SubscriptionUpdateConfiguration()
        uc2.subscriberPriority = 7
        try c.updateSubscription(csub, uc2)
        try settle(c, s)
        upd = nil
        for ev in s.pollEventsExtended() {
            if case .subscribeUpdated(let u) = ev { upd = u }
        }
        #expect(upd?.update.answerOwed == false)
        #expect(upd?.update.updateCount == 1)
        #expect(upd?.update.subscriberPriority == 7)
        let secondAck = try #require(c.pollEventsExtended().compactMap { event -> SubscriptionUpdateOkInfo? in
            if case .subscriptionUpdateOk(let value) = event { return value }; return nil
        }.first)
        #expect(secondAck.subscription == csub)
        #expect(secondAck.largest?.group == 3 && secondAck.largest?.object == 2)
        #expect(secondAck.expiresMs == nil)

        // Deferred again and rejected: the subscription ends.
        try s.setDeferSubscriptionUpdates(true)
        try c.updateSubscription(csub, uc2)
        try settle(c, s)
        upd = nil
        for ev in s.pollEventsExtended() {
            if case .subscribeUpdated(let u) = ev { upd = u }
        }
        let u3 = try #require(upd)
        #expect(u3.update.answerOwed)
        try s.rejectSubscriptionUpdate(ssub, updateRequestID: u3.update.updateRequestID,
                                       errorCode: RequestErrorCode.invalidFilter,
                                       reason: "no")
        #expect(throws: MoQError.staleHandle) { try s.subscriptionFilter(ssub) }
        try settle(c, s)
        #expect(c.state == .established)
    }

    // MARK: - Publication update answers

    @Test("A deferred publication update is answered by the PUBLISH side")
    func publicationUpdateDeferral() throws {
        let (c, s) = try pair(.draft22, client: { $0.deferSubscriptionUpdates = true })
        let cpub = try c.publish(PublishConfiguration(track: track("p")))
        try settle(c, s)
        var req: PublishRequestInfo?
        for ev in s.pollEventsExtended() {
            if case .publishRequest(let r) = ev { req = r }
        }
        let r = try #require(req)
        #expect(r.track == track("p"))
        let initial = try s.publishInitialParameters(r.publication)
        #expect(initial.hasSubscriberPriority && initial.subscriberPriority == 128)
        #expect(initial.location == nil)
        let spub = r.publication
        #expect(try s.publicationFilter(spub) == LocationFilter.none)
        try s.acceptPublish(spub)
        try settle(c, s)
        let accepted = try #require(c.pollEventsExtended().compactMap { event -> PublishOkInfo? in
            if case .publishOk(let value) = event { return value }; return nil
        }.first)
        #expect(accepted.publication == cpub)
        #expect(accepted.expiresMs == nil)

        // A filter has no place on this request.
        var withFilter = SubscriptionUpdateConfiguration()
        withFilter.filter = .nextGroup
        #expect(throws: MoQError.invalidArgument) {
            try s.updatePublication(spub, withFilter)
        }

        var uc = SubscriptionUpdateConfiguration()
        uc.forward = false
        try s.updatePublication(spub, uc)
        try settle(c, s)
        var upd: PublicationUpdatedInfo?
        for ev in c.pollEventsExtended() {
            if case .publishUpdated(let u) = ev { upd = u }
        }
        let u = try #require(upd)
        #expect(u.publication == cpub)
        #expect(u.update.answerOwed && u.update.updateCount == 1)
        #expect(u.update.forward == false)
        try c.acceptPublicationUpdate(cpub, updateRequestID: u.update.updateRequestID)
        try settle(c, s)
        let ack = try #require(s.pollEventsExtended().compactMap { event -> PublicationUpdateOkInfo? in
            if case .publicationUpdateOk(let value) = event { return value }; return nil
        }.first)
        #expect(ack.publication == spub)
        #expect(ack.largest == nil && ack.expiresMs == nil)

        // A fill asked for on the publication: owed with the update; once
        // accepted, the fill on each side names the publication as its parent
        // (nothing was published, so it ends with no stream).
        var withFill = SubscriptionUpdateConfiguration()
        withFill.fill = FillConfiguration(location: .absoluteStart(group: 0, object: 0))
        try s.updatePublication(spub, withFill)
        try settle(c, s)
        upd = nil
        for ev in c.pollEventsExtended() {
            if case .publishUpdated(let u) = ev { upd = u }
        }
        let uf = try #require(upd)
        #expect(uf.update.answerOwed && uf.update.fillRequested)
        try c.acceptPublicationUpdate(cpub, updateRequestID: uf.update.updateRequestID)
        try settle(c, s)
        var parents: [FillParent] = []
        for ev in c.pollEventsExtended() {
            if case .fillTerminal(let ft) = ev {
                parents.append(ft.parent)
                #expect(ft.reason == .noStream)
            }
        }
        #expect(parents == [.publication(cpub)])
        parents = []
        for ev in s.pollEventsExtended() {
            if case .fillCreated(let fc) = ev { parents.append(fc.parent) }
        }
        #expect(parents == [.publication(spub)])

        // The subscriber authorizes FORWARD=true; deferred acceptance must be ACKed.
        var stateUpdate = SubscriptionUpdateConfiguration()
        stateUpdate.forward = true
        try s.updatePublication(spub, stateUpdate)
        try settle(c, s)
        let stateRequest = try #require(c.pollEventsExtended().compactMap { event -> PublicationUpdatedInfo? in
            if case .publishUpdated(let value) = event { return value }; return nil
        }.first)
        #expect(stateRequest.publication == cpub)
        #expect(stateRequest.update.answerOwed && stateRequest.update.forward == true)
        try c.acceptPublicationUpdate(cpub, updateRequestID: stateRequest.update.updateRequestID)
        try settle(c, s)
        let stateAcks = s.pollEventsExtended().compactMap { event -> PublicationUpdateOkInfo? in
            if case .publicationUpdateOk(let value) = event { return value }; return nil
        }
        try #require(stateAcks.count == 1)
        #expect(stateAcks[0].publication == spub)

        // A state notification from the PUBLISH side names the publication.
        try c.notifyPublicationState(cpub, forward: true, filter: nil)
        try settle(c, s)
        var state: ExtendedPublishStateInfo?
        for ev in s.pollEventsExtended() {
            if case .publishState(let st) = ev { state = st }
        }
        #expect(state?.publication == spub)
        #expect(state?.subscription == nil)
        #expect(state?.forward == true)

        try s.updatePublication(spub, uc)
        try settle(c, s)
        upd = nil
        for ev in c.pollEventsExtended() {
            if case .publishUpdated(let u) = ev { upd = u }
        }
        let u2 = try #require(upd)
        try c.rejectPublicationUpdate(cpub, updateRequestID: u2.update.updateRequestID,
                                      errorCode: RequestErrorCode.internalError)
        #expect(throws: MoQError.staleHandle) { try c.publicationFilter(cpub) }
    }

    @Test("A state notification's filter is the subscriber's filter in effect")
    func stateNotificationFilter() throws {
        let (c, s) = try pair(.draft22)
        let (csub, ssub) = try subscribeAccepted(c, s)
        var stateUpdate = SubscriptionUpdateConfiguration()
        stateUpdate.location = .absoluteStart(group: 7, object: 0)
        try c.updateSubscription(csub, stateUpdate)
        try settle(c, s)
        let stateAcks = c.pollEventsExtended().compactMap { event -> SubscriptionUpdateOkInfo? in
            if case .subscriptionUpdateOk(let value) = event { return value }; return nil
        }
        try #require(stateAcks.count == 1)
        #expect(stateAcks[0].subscription == csub)
        try s.notifySubscriptionState(ssub, forward: nil,
                                      filter: .absoluteStart(group: 7, object: 0))
        try settle(c, s)
        var state: ExtendedPublishStateInfo?
        for ev in c.pollEventsExtended() {
            if case .publishState(let st) = ev { state = st }
        }
        #expect(state?.subscription == csub)
        #expect(state?.hasFilter == true)
        #expect(try c.subscriptionFilter(csub) == .absoluteStart(group: 7, object: 0))
    }

    // MARK: - Initial Admission and State Snapshots

    @Test("Initial admission keeps all locations, FILL and independent nil/zero/MAX timeouts", arguments: 0..<9)
    func initialAdmission(_ row: Int) throws {
        let values: [UInt64?] = [nil, 0, .max]
        let locations: [LocationFilter] = [.none, .relativeGroup(offset: 3),
            .absoluteStart(group: 7, object: 9), .groupEnd(startGroup: 7, startObject: 9, endGroup: 10),
            .objectEnd(startGroup: 7, startObject: 9, endGroup: 10, endObject: .max), .nextObject]
        weak var released: Session?
        func exercise() throws -> ExtendedSubscribeRequest {
            let (c, s) = try pair(.draft22)
            released = s
            var config = SubscribeConfiguration(track: track("admission"))
            config.location = locations[row % 6]
            config.fill = row % 2 == 1 ? FillConfiguration() : nil
            config.objectDeliveryTimeoutMs = values[row / 3]
            config.subgroupDeliveryTimeoutMs = values[row % 3]
            try c.subscribe(config)
            config = SubscribeConfiguration(track: track("overwritten"))
            try settle(c, s)
            let request = try #require(s.pollEventsExtended().compactMap { event -> ExtendedSubscribeRequest? in
                if case .subscribeRequest(let value) = event { return value }; return nil
            }.first)
            try s.tick(now: 1)
            return request
        }
        let request = try exercise()
        #expect(released == nil && request.track == track("admission"))
        let options = try #require(request.options)
        let flags: UInt32 = 1 | (row % 2 == 1 ? 2 : 0) | (row / 3 != 0 ? 4 : 0) | (row % 3 != 0 ? 8 : 0)
        #expect(options.flags == flags && options.fillRequested == (row % 2 == 1))
        #expect(options.location == locations[row % 6])
        #expect(options.objectDeliveryTimeoutMs == values[row / 3])
        #expect(options.subgroupDeliveryTimeoutMs == values[row % 3])
    }

    @Test("Literal empty SUBSCRIBE parameters are not explicit NONE")
    func absentAdmission() throws {
        let (_, s) = try pair(.draft22)
        var empty: [UInt8] = [3, 0, 7, 0, 1, 1, 0x6e, 1, 0x76, 0]
        try empty.withUnsafeBytes { try s.receiveBidiStreamBytes(on: StreamRef(rawValue: 0x7100), $0, fin: false, now: 0) }
        let request = try #require(s.pollEventsExtended().compactMap { event -> ExtendedSubscribeRequest? in
            if case .subscribeRequest(let value) = event { return value }; return nil
        }.first)
        empty = Array(repeating: 0xa5, count: empty.count)
        try s.tick(now: 1)
        let options = try #require(request.options)
        #expect(options.flags == 0 && options.location == nil && !options.fillRequested)
        #expect(options.objectDeliveryTimeoutMs == nil && options.subgroupDeliveryTimeoutMs == nil)
        #expect(request.track.name.bytes == Data("v".utf8))
    }

    @Test("Legacy drafts apply their own rules to selected initial timeouts and expose no admission record", arguments: [WireVersion.draft16, .draft18])
    func legacyAdmission(_ version: MoQTransportModel.WireVersion) throws {
        let (c, s) = try pair(version)
        // Drafts 16 and 18 carry the timeouts in SUBSCRIBE (draft-16 9.2.2.2,
        // draft-18 10.2.3, 10.2.4). Draft-16's single DELIVERY_TIMEOUT is a
        // QUIC varint and never 0; draft-18 takes 0 and UInt64.max alike.
        for selected in 1...3 {
            var config = SubscribeConfiguration(track: track(version == .draft16 ? "legacy" : "legacy-\(selected)"))
            config.objectDeliveryTimeoutMs = selected & 1 != 0 ? 0 : nil
            config.subgroupDeliveryTimeoutMs = selected & 2 != 0 ? .max : nil
            if version == .draft16 {
                #expect(throws: MoQError.invalidArgument) { try c.subscribe(config, now: 9) }
                #expect(c.pollActionsExtended().isEmpty)
            } else {
                _ = try c.subscribe(config, now: 9)
                #expect(!c.pollActionsExtended().isEmpty)
            }
        }
        try c.subscribe(SubscribeConfiguration(track: track("legacy")))
        try settle(c, s)
        let request = try #require(s.pollEventsExtended().compactMap { event -> ExtendedSubscribeRequest? in
            if case .subscribeRequest(let value) = event { return value }; return nil
        }.first)
        #expect(request.options == nil)
    }

    @Test("Two state snapshots surround a real subscriber UPDATE/ACK", arguments: [false, true])
    func historicalStateSnapshots(_ publish: Bool) throws {
        weak var released: Session?
        func exercise() throws -> [ExtendedPublishStateInfo] {
            let (c, s) = try pair(.draft22)
            let seen = Observed()
            let a = LocationFilter.groupEnd(startGroup: 0, startObject: 0, endGroup: 5)
            var sub: Subscription?
            if publish {
                var config = PublishConfiguration(track: track("history"))
                config.location = a
                _ = try c.publish(config)
            } else {
                var config = SubscribeConfiguration(track: track("history"))
                config.location = a
                sub = try c.subscribe(config)
            }
            try settle(c, s, seen)
            var pub: Publication?
            for event in s.pollEventsExtended() {
                switch event {
                case .subscribeRequest(let request): try s.acceptSubscribe(request.subscription)
                case .publishRequest(let request):
                    pub = request.publication
                    try s.acceptPublish(request.publication)
                default: Issue.record("Expected initial request")
                }
            }
            try settle(c, s, seen)
            _ = c.pollEventsExtended()
            _ = s.pollEventsExtended()
            try #require(seen.opened[0].count == 1)
            let receiver = publish ? s : c
            released = receiver
            let ref = publish ? toPeer(seen.opened[0][0], 0) : seen.opened[0][0]
            var notification: [UInt8] = [0x22, 0, 6, 1, 0x21, 3, 0, 0, 5]
            try notification.withUnsafeBytes { try receiver.receiveBidiStreamBytes(on: ref, $0, fin: false, now: 0) }
            #expect(receiver.pollActionsExtended().isEmpty)
            var update = SubscriptionUpdateConfiguration()
            update.location = .groupEnd(startGroup: 0, startObject: 0, endGroup: 10)
            if publish { try receiver.updatePublication(#require(pub), update) }
            else { try receiver.updateSubscription(#require(sub), update) }
            // Both notifications remain unpolled across the real UPDATE/ACK.
            try settle(c, s)
            notification[8] = 10
            try notification.withUnsafeBytes { try receiver.receiveBidiStreamBytes(on: ref, $0, fin: false, now: 0) }
            #expect(receiver.pollActionsExtended().isEmpty)
            notification = Array(repeating: 0xa5, count: notification.count)
            var states: [ExtendedPublishStateInfo] = []
            var ack = 0
            for event in receiver.pollEventsExtended() {
                switch event {
                case .publishState(let state):
                    #expect(state.subscription == (publish ? nil : sub))
                    #expect(state.publication == (publish ? pub : nil))
                    #expect(state.hasFilter && state.forward == nil && state.largest == nil)
                    states.append(state)
                case .subscriptionUpdateOk: #expect(!publish); ack += 1
                case .publicationUpdateOk: #expect(publish); ack += 1
                default: Issue.record("Unexpected event among historical notifications")
                }
            }
            #expect(ack == 1 && states.count == 2)
            try receiver.tick(now: 1)
            return states
        }
        let states = try exercise()
        #expect(released == nil)
        #expect(states.map(\.location) == [.groupEnd(startGroup: 0, startObject: 0, endGroup: 5),
                                         .groupEnd(startGroup: 0, startObject: 0, endGroup: 10)])
    }

    // MARK: - FETCH

    private func nextFetchRequest(_ s: Session) -> FetchRequestInfo? {
        var out: FetchRequestInfo?
        for ev in s.pollEventsExtended() {
            if case .fetchRequest(let r) = ev { out = r }
        }
        return out
    }

    @Test("Every typed FETCH location reaches a copied request observation", arguments: [
        LocationFilter.none, .relativeGroup(offset: 2), .absoluteStart(group: 2, object: 3),
        .groupEnd(startGroup: 2, startObject: 3, endGroup: 7),
        .objectEnd(startGroup: 2, startObject: 3, endGroup: 7, endObject: 9), .nextObject])
    func fetchTypedLocations(_ form: LocationFilter) throws {
        let (c, s) = try pair(.draft22)
        try s.setDeferFetchRanges(true)
        var cfg = FetchConfiguration(track: track("typed-fetch"))
        cfg.location = form
        try c.fetch(cfg)
        cfg.location = nil
        try settle(c, s)
        let req = try #require(nextFetchRequest(s))
        let value = try #require(req.options)
        #expect(value.rangeUnresolved && value.includeProperties && value.fillTimeoutMs == nil)
        #expect(value.location == (form == .none ? nil : form))
        try s.rejectFetch(req.fetch, errorCode: RequestErrorCode.invalidRange)
    }

    @Test("FETCH Location Filter, FILL_TIMEOUT, options and Timed-Out")
    func fetchOptions() throws {
        let (c, s) = try pair(.draft22)
        try s.setDeferFetchRanges(true)
        var fc = FetchConfiguration(track: track("f"))
        fc.location = .relativeGroup(offset: 2)
        fc.fillTimeoutMs = 0
        let cfetch = try c.fetch(fc)
        try settle(c, s)
        let req = try #require(nextFetchRequest(s))
        #expect(req.track == track("f"))
        let opts = try #require(req.options)
        #expect(opts.rangeUnresolved)
        #expect(opts.location == .relativeGroup(offset: 2))
        #expect(opts.fillTimeoutMs == 0)
        #expect(opts.includeProperties)

        // Resolved against the publisher's Largest {5,3}: start {4,0}.
        try s.acceptFetch(req.fetch, endGroup: 5, endObject: 4)
        try s.writeFetchObject(req.fetch, groupID: 4, objectID: 0,
                               payload: try Buffer(Data("g4".utf8)))
        try s.writeFetchRange(req.fetch, kind: .timedOut, groupID: 5, objectID: 0)
        try s.endFetch(req.fetch)
        try settle(c, s)
        var ok = false, objects = 0, timedOut = 0, complete = false
        for ev in c.pollEventsExtended() {
            switch ev {
            case .fetchOk(let o):
                ok = o.fetch == cfetch && o.endGroup == 5
            case .fetchObject(let o):
                objects += 1
                #expect(o.groupID == 4 && o.objectID == 0)
                #expect(o.payload.map { Data($0.copyBytes()) } == Data("g4".utf8))
            case .fetchGap(let g):
                if g.kind == .timedOut { timedOut += 1 }
            case .fetchComplete(let f):
                complete = f == cfetch
            default: break
            }
        }
        #expect(ok && objects == 1 && timedOut >= 1 && complete)

        // An End before an open absolute start contradicts the request.
        var abs = FetchConfiguration(track: track("f"))
        abs.location = .absoluteStart(group: 4, object: 0)
        try c.fetch(abs)
        try settle(c, s)
        let r2 = try #require(nextFetchRequest(s))
        #expect(r2.options?.location == .absoluteStart(group: 4, object: 0))
        #expect(r2.options?.fillTimeoutMs == nil)
        #expect(throws: MoQError.invalidArgument) {
            try s.acceptFetch(r2.fetch, endGroup: 2, endObject: 1)
        }
        try s.rejectFetch(r2.fetch, errorCode: RequestErrorCode.invalidRange)
        try settle(c, s)
        _ = c.pollEventsExtended()

        // No filter: the whole track, and no location in the options.
        var whole = FetchConfiguration(track: track("f"))
        whole.location = LocationFilter.none
        try c.fetch(whole)
        try settle(c, s)
        let r3 = try #require(nextFetchRequest(s))
        #expect(r3.options?.rangeUnresolved == true)
        #expect(r3.options?.location == nil)
        try s.rejectFetch(r3.fetch, errorCode: RequestErrorCode.invalidRange)
        try settle(c, s)
        _ = c.pollEventsExtended()

        // Switched back, the session resolves itself: nothing published, so
        // INVALID_RANGE without surfacing the request.
        try s.setDeferFetchRanges(false)
        try c.fetch(fc)
        try settle(c, s)
        #expect(nextFetchRequest(s) == nil)
        var errors: [FetchErrorInfo] = []
        for ev in c.pollEventsExtended() {
            if case .fetchError(let e) = ev { errors.append(e) }
        }
        #expect(errors.count == 1)
        #expect(errors.first?.errorCode == RequestErrorCode.invalidRange)
    }

    @Test("The configuration field defers FETCH resolution too")
    func fetchDeferFromConfiguration() throws {
        let (c, s) = try pair(.draft22, server: { $0.deferFetchRanges = true })
        var fc = FetchConfiguration(track: track("f"))
        fc.location = .nextObject
        try c.fetch(fc)
        try settle(c, s)
        let r = try #require(nextFetchRequest(s))
        #expect(r.options?.rangeUnresolved == true)
        #expect(r.options?.location == .nextObject)
    }

    @Test("Draft 18 FETCH: no Location Filter, no options, no Timed-Out")
    func fetchDraft18() throws {
        let (c, s) = try pair(.draft18)
        var fc = FetchConfiguration(track: track("f"))
        fc.location = LocationFilter.none
        #expect(throws: MoQError.unsupported) { try c.fetch(fc) }
        fc.location = nil
        fc.fillTimeoutMs = 10
        #expect(throws: MoQError.unsupported) { try c.fetch(fc) }
        fc.fillTimeoutMs = nil
        fc.startGroup = 1
        fc.endGroup = 2
        try c.fetch(fc)
        try settle(c, s)
        let r = try #require(nextFetchRequest(s))
        #expect(r.options == nil)
        try s.acceptFetch(r.fetch, endGroup: 2, endObject: 0)
        #expect(throws: MoQError.invalidArgument) {
            try s.writeFetchRange(r.fetch, kind: .timedOut, groupID: 1, objectID: 0)
        }
        try s.writeFetchRange(r.fetch, kind: .unknown, groupID: 1, objectID: 0)
    }

    // MARK: - Terminal causes

    @Test("Stream signals that end a subscription or a FETCH carry their cause")
    func terminalCauses() throws {
        let (c, s) = try pair(.draft22)
        let seen = Observed()
        let (_, ssub) = try subscribeAccepted(c, s, seen)
        let subRef = try #require(seen.opened[0].first)
        try s.receiveBidiStreamReset(on: toPeer(subRef, 0), errorCode: 9, now: 0)
        var uns: UnsubscribedInfo?
        for ev in s.pollEventsExtended() {
            if case .unsubscribed(let u) = ev { uns = u }
        }
        #expect(uns == UnsubscribedInfo(subscription: ssub, cause: .peerResetStream,
                                        streamErrorCode: 9))

        seen.opened = [[], []]
        try s.setDeferFetchRanges(true)
        var fc = FetchConfiguration(track: track("f"))
        fc.location = LocationFilter.none
        try c.fetch(fc)
        try settle(c, s, seen)
        let req = try #require(nextFetchRequest(s))
        let fetchRef = try #require(seen.opened[0].first)
        try s.receiveBidiStreamReset(on: toPeer(fetchRef, 0), errorCode: 11, now: 0)
        var can: FetchCancelledInfo?
        for ev in s.pollEventsExtended() {
            if case .fetchCancelled(let f) = ev { can = f }
        }
        #expect(can == FetchCancelledInfo(fetch: req.fetch, cause: .peerResetStream,
                                          streamErrorCode: 11))
    }

    @Test("Typed terminal observations preserve family, cause and complete extent")
    func typedTerminalConversion() {
        var ev = moq_event_t()
        ev.kind = MOQ_EVENT_TRACK_STATUS_TERMINATED
        ev.detail_size = 24
        ev.u.track_status_terminated.handle._opaque = 0x77
        ev.u.track_status_terminated.cause = MOQ_TRACK_STATUS_PEER_STOP_SENDING
        ev.u.track_status_terminated.local_role = MOQ_TRACK_STATUS_REQUESTER
        ev.u.track_status_terminated.stream_error_code = UInt64.max
        var saved = ev
        #expect(ExtendedEvent.from(&ev) == .trackStatusTerminated(TrackStatusTerminatedInfo(
            handle: 0x77, cause: .peerStopSending, localRole: .requester,
            streamErrorCode: UInt64.max)))
        for extent in UInt32(0)..<24 {
            ev = saved
            ev.detail_size = extent
            #expect(ExtendedEvent.from(&ev) == .unknown(MOQ_EVENT_TRACK_STATUS_TERMINATED))
        }
        ev = saved
        #expect(Event.from(&ev) == .unknown(MOQ_EVENT_TRACK_STATUS_TERMINATED))
        ev = saved
        ev.detail_size = 32
        ev.u.track_status_terminated.cause = UInt32.max
        ev.u.track_status_terminated.local_role = UInt32.max
        #expect(ExtendedEvent.from(&ev) == .trackStatusTerminated(TrackStatusTerminatedInfo(
            handle: 0x77, cause: .other(UInt32.max), localRole: .other(UInt32.max),
            streamErrorCode: UInt64.max)))

        ev = moq_event_t()
        ev.kind = MOQ_EVENT_NAMESPACE_TERMINATED
        ev.detail_size = 24
        ev.u.namespace_terminated.ann._opaque = 0x88
        ev.u.namespace_terminated.cause = MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY
        ev.u.namespace_terminated.local_role = MOQ_NAMESPACE_ROLE_RESPONDER
        saved = ev
        let announcement = Announcement(ev.u.namespace_terminated.ann)
        #expect(ExtendedEvent.from(&ev) == .namespaceTerminated(NamespaceTerminatedInfo(
            announcement: announcement, cause: .localCapacity, localRole: .responder,
            streamErrorCode: 0)))
        for extent in UInt32(0)..<24 {
            ev = saved
            ev.detail_size = extent
            #expect(ExtendedEvent.from(&ev) == .unknown(MOQ_EVENT_NAMESPACE_TERMINATED))
        }
        ev = saved
        #expect(Event.from(&ev) == .unknown(MOQ_EVENT_NAMESPACE_TERMINATED))

        ev = moq_event_t()
        ev.kind = MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED
        ev.detail_size = 24
        ev.u.ns_subscription_terminated.handle._opaque = 0x99
        ev.u.ns_subscription_terminated.cause = MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM
        ev.u.ns_subscription_terminated.local_role = MOQ_REQUEST_ROLE_RESPONDER
        saved = ev
        #expect(ExtendedEvent.from(&ev) == .namespaceSubscriptionTerminated(
            NamespaceSubscriptionTerminatedInfo(handle: 0x99, cause: .peerResetStream,
                localRole: .responder, streamErrorCode: 0)))
        for extent in UInt32(0)..<24 {
            ev = saved
            ev.detail_size = extent
            #expect(ExtendedEvent.from(&ev) == .unknown(MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED))
        }
        ev = saved
        #expect(Event.from(&ev) == .unknown(MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED))
    }

    @Test("Each typed terminal cause domain preserves all values and roles")
    func typedTerminalDomains() {
        let trackCauses: [(UInt32, TrackStatusTerminalCause)] = [
            (MOQ_TRACK_STATUS_PEER_RESET_STREAM, .peerResetStream),
            (MOQ_TRACK_STATUS_PEER_STOP_SENDING, .peerStopSending),
            (MOQ_TRACK_STATUS_MISSING_RESPONSE, .missingResponse),
            (MOQ_TRACK_STATUS_TRUNCATED_RESPONSE, .truncatedResponse),
            (.max, .other(.max))]
        let announcementCauses: [(UInt32, NamespaceTerminalCause)] = [
            (MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE, .peerFinComplete),
            (MOQ_NAMESPACE_TERMINAL_PEER_RESET_STREAM, .peerResetStream),
            (MOQ_NAMESPACE_TERMINAL_PEER_STOP_SENDING, .peerStopSending),
            (MOQ_NAMESPACE_TERMINAL_MISSING_RESPONSE, .missingResponse),
            (MOQ_NAMESPACE_TERMINAL_TRUNCATED_MESSAGE, .truncatedMessage),
            (MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY, .localCapacity),
            (.max, .other(.max))]
        let subscriptionCauses: [(UInt32, RequestTerminalCause)] = [
            (MOQ_REQUEST_TERMINAL_NONE, .none),
            (MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM, .peerResetStream),
            (MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING, .peerStopSending),
            (MOQ_REQUEST_TERMINAL_MISSING_RESPONSE, .missingResponse),
            (MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE, .truncatedResponse),
            (MOQ_REQUEST_TERMINAL_PEER_FIN_COMPLETE, .peerFinComplete),
            (MOQ_REQUEST_TERMINAL_LOCAL_CANCEL, .localCancel),
            (.max, .other(.max))]
        let roles: [(UInt32, RequestRole)] = [
            (MOQ_REQUEST_ROLE_REQUESTER, .requester),
            (MOQ_REQUEST_ROLE_RESPONDER, .responder),
            (.max, .other(.max))]
        for (nativeRole, role) in roles {
            for code in [UInt64(0), .max] {
                for extent in [UInt32(24), 32] {
                    for (nativeCause, cause) in trackCauses {
                        var ev = moq_event_t()
                        ev.kind = MOQ_EVENT_TRACK_STATUS_TERMINATED
                        ev.detail_size = extent
                        ev.u.track_status_terminated.handle._opaque = 0x77
                        ev.u.track_status_terminated.cause = nativeCause
                        ev.u.track_status_terminated.local_role = nativeRole
                        ev.u.track_status_terminated.stream_error_code = code
                        #expect(ExtendedEvent.from(&ev) == .trackStatusTerminated(
                            TrackStatusTerminatedInfo(handle: 0x77, cause: cause,
                                localRole: role, streamErrorCode: code)))
                    }
                    for (nativeCause, cause) in announcementCauses {
                        var ev = moq_event_t()
                        ev.kind = MOQ_EVENT_NAMESPACE_TERMINATED
                        ev.detail_size = extent
                        ev.u.namespace_terminated.ann._opaque = 0x88
                        ev.u.namespace_terminated.cause = nativeCause
                        ev.u.namespace_terminated.local_role = nativeRole
                        ev.u.namespace_terminated.stream_error_code = code
                        let announcement = Announcement(ev.u.namespace_terminated.ann)
                        #expect(ExtendedEvent.from(&ev) == .namespaceTerminated(
                            NamespaceTerminatedInfo(announcement: announcement, cause: cause,
                                localRole: role, streamErrorCode: code)))
                    }
                    for (nativeCause, cause) in subscriptionCauses {
                        var ev = moq_event_t()
                        ev.kind = MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED
                        ev.detail_size = extent
                        ev.u.ns_subscription_terminated.handle._opaque = 0x99
                        ev.u.ns_subscription_terminated.cause = nativeCause
                        ev.u.ns_subscription_terminated.local_role = nativeRole
                        ev.u.ns_subscription_terminated.stream_error_code = code
                        #expect(ExtendedEvent.from(&ev) == .namespaceSubscriptionTerminated(
                            NamespaceSubscriptionTerminatedInfo(handle: 0x99, cause: cause,
                                localRole: role, streamErrorCode: code)))
                        let saved = ev
                        for shortExtent in [UInt32(0), 8, 12, 16, 23] {
                            ev = saved
                            ev.detail_size = shortExtent
                            #expect(ExtendedEvent.from(&ev) == .unknown(MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED))
                        }
                        ev = saved
                        #expect(Event.from(&ev) == .unknown(MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED))
                    }
                }
            }
        }
    }

    @Test("Appended update fields read as absent from an older producer")
    func updateFieldGates() {
        var ev = moq_event_t()
        ev.kind = MOQ_EVENT_SUBSCRIBE_UPDATED
        ev.u.subscribe_updated.answer_owed = true
        ev.u.subscribe_updated.fill_requested = true
        ev.u.subscribe_updated.update_count = 2
        ev.u.subscribe_updated.update_request_id = 10
        ev.detail_size = UInt32(MemoryLayout<moq_subscribe_updated_event_t>.size)
        var saved = ev
        guard case .subscribeUpdated(let full) = ExtendedEvent.from(&ev) else {
            Issue.record("not converted"); return
        }
        #expect(full.update.answerOwed && full.update.fillRequested)
        #expect(full.update.updateCount == 2 && full.update.updateRequestID == 10)
        #expect(full.update.endObject == 0)
        ev = saved
        ev.detail_size = UInt32(MemoryLayout<moq_subscribe_updated_event_t>.offset(
            of: \moq_subscribe_updated_event_t.options)!)
        guard case .subscribeUpdated(let old) = ExtendedEvent.from(&ev) else {
            Issue.record("not converted"); return
        }
        #expect(!old.update.answerOwed && !old.update.fillRequested)
        #expect(old.update.updateCount == 0 && old.update.updateRequestID == 0)

        // The fetch options pointer is read only when the detail covers it.
        var loc = moq_location_filter_cfg_t()
        moq_location_filter_cfg_init_sized(&loc, MemoryLayout<moq_location_filter_cfg_t>.size)
        loc.kind = MOQ_LOCATION_FILTER_GROUP_END
        loc.start_group = 2
        loc.end_group = 6
        do {
            var o = moq_fetch_request_options_t()
            o.struct_size = UInt32(MemoryLayout<moq_fetch_request_options_t>.size)
            o.range_unresolved = true
            o.flags = UInt32(MOQ_FETCH_REQUEST_INCLUDE_PROPERTIES)
            o.location = loc
            withUnsafePointer(to: &o) { oPtr in
                var fe = moq_event_t()
                fe.kind = MOQ_EVENT_FETCH_REQUEST
                fe.u.fetch_request.options = oPtr
                fe.detail_size = UInt32(MemoryLayout<moq_fetch_request_event_t>.size)
                saved = fe
                guard case .fetchRequest(let r) = ExtendedEvent.from(&fe) else {
                    Issue.record("not converted"); return
                }
                #expect(r.options == FetchRequestOptions(
                    rangeUnresolved: true,
                    location: .groupEnd(startGroup: 2, startObject: 0, endGroup: 6),
                    fillTimeoutMs: nil))
                fe = saved
                fe.detail_size = UInt32(MemoryLayout<moq_fetch_request_event_t>.offset(
                    of: \moq_fetch_request_event_t.options)!)
                guard case .fetchRequest(let r2) = ExtendedEvent.from(&fe) else {
                    Issue.record("not converted"); return
                }
                #expect(r2.options == nil)
            }
        }
    }

    @Test("Terminal causes on the remaining events read as absent from an older producer")
    func causeFieldGates() {
        func convert(_ build: (inout moq_event_t) -> Void, oldSize: Int) -> (ExtendedEvent, ExtendedEvent) {
            var ev = moq_event_t()
            build(&ev)
            let saved = ev
            let full = ExtendedEvent.from(&ev)
            ev = saved
            ev.detail_size = UInt32(oldSize)
            return (full, ExtendedEvent.from(&ev))
        }
        var (full, old) = convert({
            $0.kind = MOQ_EVENT_PUBLISH_FINISHED
            $0.detail_size = UInt32(MemoryLayout<moq_publish_finished_event_t>.size)
            $0.u.publish_finished.status_code = 0
            $0.u.publish_finished.cause = MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM
            $0.u.publish_finished.stream_error_code = 5
        }, oldSize: MemoryLayout<moq_publish_finished_event_t>.offset(
            of: \moq_publish_finished_event_t.cause)!)
        guard case .publishFinished(let pf) = full, case .publishFinished(let pfo) = old else {
            Issue.record("not converted"); return
        }
        #expect(pf.cause == .peerResetStream && pf.streamErrorCode == 5)
        #expect(pfo.cause == .none && pfo.streamErrorCode == 0)

        (full, old) = convert({
            $0.kind = MOQ_EVENT_PUBLISH_UNSUBSCRIBED
            $0.detail_size = UInt32(MemoryLayout<moq_publish_unsubscribed_event_t>.size)
            $0.u.publish_unsubscribed.cause = MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING
            $0.u.publish_unsubscribed.stream_error_code = 6
        }, oldSize: MemoryLayout<moq_publish_unsubscribed_event_t>.offset(
            of: \moq_publish_unsubscribed_event_t.cause)!)
        guard case .publishUnsubscribed(let pu) = full,
              case .publishUnsubscribed(let puo) = old else {
            Issue.record("not converted"); return
        }
        #expect(pu.cause == .peerStopSending && pu.streamErrorCode == 6)
        #expect(puo.cause == .none && puo.streamErrorCode == 0)

        (full, old) = convert({
            $0.kind = MOQ_EVENT_NAMESPACE_CANCELLED
            $0.detail_size = UInt32(MemoryLayout<moq_namespace_cancelled_event_t>.size)
            $0.u.namespace_cancelled.error_code = 7
        }, oldSize: MemoryLayout<moq_namespace_cancelled_event_t>.offset(
            of: \moq_namespace_cancelled_event_t.reason)!)
        guard case .namespaceCancelled(let nc) = full else {
            Issue.record("not converted"); return
        }
        #expect(nc.errorCode == 7 && nc.reason == "")
        #expect(old == .unknown(MOQ_EVENT_NAMESPACE_CANCELLED))

        (full, old) = convert({
            $0.kind = MOQ_EVENT_FETCH_RESET
            $0.detail_size = UInt32(MemoryLayout<moq_fetch_reset_event_t>.size)
            $0.u.fetch_reset.error_code = 0
            $0.u.fetch_reset.data_stream = true
            $0.u.fetch_reset.cause = MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE
        }, oldSize: MemoryLayout<moq_fetch_reset_event_t>.offset(
            of: \moq_fetch_reset_event_t.cause)!)
        guard case .fetchReset(let fr) = full, case .fetchReset(let fro) = old else {
            Issue.record("not converted"); return
        }
        #expect(fr.cause == .truncatedResponse && fr.dataStream)
        #expect(fro.cause == .none)
    }

    // MARK: - Reliable reset and commit report

    @Test("A reset fill stream carries its header as the reliable size")
    func reliableResetSize() throws {
        let (c, s) = try pair(.draft22)
        var cfg = SubscribeConfiguration(track: track(), filter: .latestObject)
        cfg.location = .nextObject
        cfg.fill = .currentGroup
        try c.subscribe(cfg)
        try settle(c, s)
        var ssub: Subscription?
        for ev in s.pollEventsExtended() {
            if case .subscribeRequest(let r) = ev { ssub = r.subscription }
        }
        var acc = AcceptSubscribeConfiguration()
        acc.largest = (3, 2)
        try s.acceptSubscribe(try #require(ssub), config: acc)
        try settle(c, s)
        var fill: Fill?
        for ev in s.pollEventsExtended() {
            if case .fillRequest(let r) = ev { fill = r.fill }
        }
        let seen = Observed()
        try s.abortFill(try #require(fill), errorCode: StreamErrorCode.unknownObjectStatus)
        try settle(c, s, seen)
        let reset = try #require(seen.resets.first)
        #expect(reset.errorCode == StreamErrorCode.unknownObjectStatus)
        #expect(reset.reliableSize > 0)
        // A commit report for a stream the session is not holding is a no-op.
        try c.receiveDataCommitted(on: StreamRef(rawValue: 77), now: 0)
    }

    // MARK: - Transport binding

    @Test("A server's URI policy sees the client's target and decides")
    func transportBindingPolicy() throws {
        func run(_ verdict: URIVerdict) throws -> (Session, Session, [(Data?, Data?)]) {
            var cc = Session.Configuration(perspective: .client)
            cc.wireVersion = .draft22
            var sc = Session.Configuration(perspective: .server)
            sc.wireVersion = .draft22
            let c = try Session(configuration: cc)
            let s = try Session(configuration: sc)
            var asked: [(Data?, Data?)] = []
            var cb = TransportBinding(factsKnown: .all, factsSupported: .all)
            cb.authority = "relay.example:443"
            cb.pathQuery = "/moq?x=1"
            try c.bindTransport(cb)
            var sb = TransportBinding(factsKnown: .all, factsSupported: .all)
            sb.acceptURI = { a, p in asked.append((a, p)); return verdict }
            try s.bindTransport(sb)
            try c.start()
            try s.start()
            for _ in 0..<6 {
                try? pump(c, s, 0, nil)
                try? pump(s, c, 1, nil)
            }
            return (c, s, asked)
        }
        let (c, s, asked) = try run(.accept)
        #expect(asked.count >= 1)
        #expect(asked.first?.0 == Data("relay.example:443".utf8))
        #expect(asked.first?.1 == Data("/moq?x=1".utf8))
        #expect(c.state == .established && s.state == .established)

        let (_, s2, asked2) = try run(.invalidPath)
        #expect(asked2.count >= 1)
        #expect(s2.state == .closed)

        // A binding needs facts known before it binds.
        let lone = try Session(configuration: {
            var x = Session.Configuration(perspective: .client)
            x.wireVersion = .draft22
            return x
        }())
        var partial = TransportBinding(factsKnown: [], factsSupported: [])
        partial.authority = "a"
        #expect(throws: MoQError.wouldBlock) { try lone.bindTransport(partial) }
    }

    // MARK: - Subscriber facade fills

    @Test("Fills through the Subscriber facade")
    func subscriberFacadeFills() throws {
        let (c, s) = try pair(.draft22)
        let pub = try Publisher(session: s)
        let ns = Namespace([NamespacePart(Data("fill".utf8))])
        let ptrack = try pub.addTrack(namespace: ns, name: "v")
        try pub.setRetainedGroup(on: ptrack, RetainedGroupConfiguration(
            groupID: 3,
            objects: [RetainedObject(objectID: 0, payload: try Buffer(Data("g3o0".utf8))),
                      RetainedObject(objectID: 1, payload: try Buffer(Data("g3o1".utf8)))]))

        var subCfg = Subscriber.Configuration()
        subCfg.maxFills = 1
        let sub = try Subscriber(session: c, configuration: subCfg)
        let t = try sub.subscribe(track: TrackName(namespace: ns, name: "v"),
                                  filter: .latestObject, location: .nextObject,
                                  fill: .currentGroup)
        let fill = try #require(sub.fill(of: t))
        // One slot: a second request while the first is held waits.
        #expect(throws: MoQError.wouldBlock) {
            try sub.subscribe(track: TrackName(namespace: ns, name: "w"),
                              filter: .latestObject, location: nil, fill: .currentGroup)
        }

        for i in 0..<6 {
            try settle(c, s)
            try sub.tick(now: UInt64(2 + i))
            try pub.tick(now: UInt64(2 + i))
        }
        var objects: [UInt64] = []
        var kept: Buffer?
        var created = 0
        var terminals = 0
        while let item = try sub.pollFill() {
            #expect(item.fill == fill)
            #expect(item.track === t)
            #expect(item.initiatingRequestID == 0)
            switch item.kind {
            case .created:
                #expect(objects.isEmpty && terminals == 0)
                created += 1
            case .object(let g, _, let o, _, _, let payload, _):
                #expect(g == 3)
                objects.append(o)
                if o == 1 { kept = payload }
            case .gap:
                break
            case .terminal(let reason, let domain, _):
                terminals += 1
                #expect(reason == .complete && domain == FillTerminalInfo.Domain.none)
            }
        }
        #expect(objects == [0, 1])
        #expect(created == 1)
        #expect(terminals == 1)
        #expect(kept.map { Data($0.copyBytes()) } == Data("g3o1".utf8))
        #expect(sub.fill(of: t) == nil)
        #expect(throws: MoQError.staleHandle) { try sub.cancelFill(fill) }
        try sub.requestFill(on: t, FillConfiguration())
        let fill2 = try #require(sub.fill(of: t))
        #expect(fill2 != fill)
    }

    @Test("Overlapping facade fills preserve CREATED, initiators, fields and owned lifetime")
    func overlappingFacadeFillObservations() throws {
        let group: UInt64 = (1 << 40) + 7
        let object: UInt64 = (1 << 36) + 2
        let subgroup: UInt64 = (1 << 42) + 9
        let wideCode: UInt64 = (1 << 54) + 13
        let body = Data([0, 0x42, 0xff])
        let props = Data([2, 0x41])
        weak var releasedSub: Subscriber?
        weak var releasedClient: Session?
        weak var releasedServer: Session?
        weak var releasedPayload: Buffer?
        weak var releasedProperties: Buffer?

        func exercise() throws -> SubscriberFillItem {
            let (c, s) = try pair(.draft22)
            var config = Subscriber.Configuration()
            config.maxFills = 3
            let sub = try Subscriber(session: c, configuration: config)
            releasedSub = sub
            releasedClient = c
            releasedServer = s
            let fill = FillConfiguration(location: .absoluteStart(group: group, object: object))
            let t = try sub.subscribe(track: track("sized-fill"), location: nil, fill: fill)
            var requests: [FillRequestInfo] = []
            func collect() throws {
                for _ in 0..<2 {
                    try settle(c, s)
                    for ev in s.pollEventsExtended() {
                        switch ev {
                        case .subscribeRequest(let r):
                            var accept = AcceptSubscribeConfiguration()
                            accept.largest = (group + 1, 0)
                            try s.acceptSubscribe(r.subscription, config: accept)
                        case .fillRequest(let r): requests.append(r)
                        default: break
                        }
                    }
                }
                try sub.tick(now: 0)
            }
            try collect()
            for _ in 1..<3 {
                try sub.requestFill(on: t, fill)
                try collect()
            }
            try #require(requests.count == 3)
            // Three active children precede polling: the latest query cannot
            // reconstruct their CREATED observations or initiating request IDs.
            var children: [SubscriberFill] = []
            for n in 0..<3 {
                let item = try #require(try sub.pollFill())
                guard case .created = item.kind else {
                    Issue.record("Expected CREATED before fill data")
                    throw PumpError()
                }
                #expect(item.track === t)
                #expect(item.initiatingRequestID == UInt64(2 * n))
                #expect(requests[n].initiatingRequestID == UInt64(2 * n))
                children.append(item.fill)
            }
            #expect(Set(children).count == 3)
            #expect(sub.fill(of: t) == children[2])
            #expect(try sub.pollFill() == nil)
            let payload = try Buffer(body)
            let properties = try Buffer(props)
            try s.openFill(requests[0].fill)
            try s.writeFillObject(requests[0].fill, groupID: group, subgroupID: subgroup,
                                  objectID: object, publisherPriority: 213,
                                  payload: payload, properties: properties)
            // The sender's datagram option is outside this wrapper migration;
            // use the public native writer to exercise the receiving flag.
            var datagram = moq_fetch_object_cfg_t()
            moq_fetch_object_cfg_init(&datagram)
            datagram.group_id = group
            datagram.subgroup_id = subgroup
            datagram.object_id = object + 1
            datagram.publisher_priority = 213
            datagram.datagram = true
            datagram.payload = payload.raw
            datagram.properties = properties.raw
            try MoQError.check(moq_session_write_fill_object(s.raw, requests[0].fill.raw, &datagram, 0))
            try s.writeFillRange(requests[0].fill, kind: .nonExistent, groupID: group, objectID: object + 2)
            try s.endFill(requests[0].fill)
            try s.openFill(requests[1].fill)
            try s.writeFillObject(requests[1].fill, groupID: group, subgroupID: subgroup,
                                  objectID: object, publisherPriority: 213,
                                  payload: payload, properties: properties)
            try s.writeFillRange(requests[1].fill, kind: .nonExistent, groupID: group, objectID: object + 1)
            // Deliver UPDATE data before RESET; transport reset buffering is
            // not part of this observation fixture.
            try settle(c, s)
            try sub.tick(now: 0)
            try s.abortFill(requests[1].fill, errorCode: 0)
            try s.abortFill(requests[2].fill, errorCode: wideCode)
            try settle(c, s)
            try sub.tick(now: 0)
            var objects = [0, 0, 0], gaps = [0, 0, 0]
            var terminals = [0, 0, 0]
            var held: SubscriberFillItem?
            while let item = try sub.pollFill() {
                let n = try #require(children.firstIndex(of: item.fill))
                #expect(item.initiatingRequestID == UInt64(2 * n))
                #expect(item.track === t)
                switch item.kind {
                case .created:
                    Issue.record("Duplicate CREATED")
                case .object(let g, let sg, let o, let priority, let dg, let data, let properties):
                    #expect(n < 2 && objects[n] < (n == 0 ? 2 : 1))
                    #expect(g == group && o == object + UInt64(objects[n]))
                    #expect(sg == (objects[n] == 0 ? subgroup : 0))
                    #expect(dg == (objects[n] == 1) && priority == 213)
                    #expect(data?.copyBytes() == body)
                    #expect(properties?.copyBytes() == props)
                    if n == 0 && objects[n] == 0 { held = item }
                    objects[n] += 1
                case .gap(let kind, let g, let o):
                    #expect(n < 2 && objects[n] == (n == 0 ? 2 : 1))
                    #expect(kind == .nonExistent && g == group && o == object + (n == 0 ? 2 : 1))
                    gaps[n] += 1
                case .terminal(let reason, let domain, let code):
                    terminals[n] += 1
                    #expect(reason == (n == 0 ? .complete : .peerCancelledOrReset))
                    #expect(domain == (n == 0 ? .none : .stream))
                    #expect(code == (n == 2 ? wideCode : 0))
                    #expect(throws: MoQError.staleHandle) { try sub.cancelFill(item.fill) }
                }
            }
            #expect(objects == [2, 1, 0] && gaps == [1, 1, 0] && terminals == [1, 1, 1])
            #expect(sub.fill(of: t) == nil)
            return try #require(held)
        }

        func checkBuffers(_ item: SubscriberFillItem) {
            #expect(item.initiatingRequestID == 0)
            if case .object(_, _, _, _, _, let payload, let properties) = item.kind {
                #expect(payload?.copyBytes() == body && properties?.copyBytes() == props)
                #expect(payload.map { moq_rcbuf_refcount($0.raw) } == 1)
                #expect(properties.map { moq_rcbuf_refcount($0.raw) } == 1)
                releasedPayload = payload
                releasedProperties = properties
            } else { Issue.record("Expected held object") }
        }
        var original: SubscriberFillItem? = try exercise()
        #expect(releasedSub == nil && releasedClient == nil && releasedServer == nil)
        // The value copy shares the Buffer owners, without an extra native
        // reference. Never dereference its now-borrowed-only track pointer.
        var copy = original
        try checkBuffers(#require(original))
        original = nil
        try checkBuffers(#require(copy))
        copy = nil
        #expect(releasedPayload == nil && releasedProperties == nil)
    }

    @Test("FILL fixed-width range and terminal observations preserve unknown values")
    func fillFixedWidthValues() {
        #expect(FetchRangeKind(UInt32.max) == .other(UInt32.max))
        #expect(FillTerminalInfo.Reason(UInt32.max) == .other(UInt32.max))
        #expect(FillTerminalInfo.Domain(UInt32.max) == .other(UInt32.max))
    }

    @Test("The Subscriber facade refuses fills and new forms on draft 18")
    func subscriberFacadeDraft18() throws {
        let (c, _) = try pair(.draft18)
        let sub = try Subscriber(session: c)
        let t = TrackName(namespace: Namespace([NamespacePart(Data("fill".utf8))]),
                          name: Data("v".utf8))
        #expect(throws: MoQError.unsupported) {
            try sub.subscribe(track: t, location: nil, fill: .currentGroup)
        }
        #expect(throws: MoQError.unsupported) {
            try sub.subscribe(track: t, location: .relativeGroup(offset: 3), fill: nil)
        }
        #expect(c.pollActionsExtended().isEmpty)
    }

    // MARK: - SUBSCRIBE_TRACKS

    private func prefix(_ p: String) -> Namespace {
        Namespace([NamespacePart(Data("d22".utf8)), NamespacePart(Data(p.utf8))])
    }

    @Test("SUBSCRIBE_TRACKS on both sides", arguments: [WireVersion.draft22, .draft18])
    func subscribeTracks(_ version: MoQTransportModel.WireVersion) throws {
        let (c, s) = try pair(version)
        let seen = Observed()

        // Accepted, with FORWARD=0 and a PUBLISH_BLOCKED on the open stream.
        var cfg = SubscribeTracksConfiguration(prefix: prefix("a"))
        cfg.forward = false
        let cts = try c.subscribeTracks(cfg)
        #expect(cts.isValid)
        try settle(c, s, seen)
        var req: SubscribeTracksRequestInfo?
        for ev in s.pollEventsExtended() {
            if case .subscribeTracksRequest(let r) = ev { req = r }
        }
        let r = try #require(req)
        #expect(r.prefix == prefix("a"))
        #expect(!r.forward)
        try s.acceptSubscribeTracks(r.trackSubscription)
        try s.sendPublishBlocked(on: r.trackSubscription,
                                 trackNamespaceSuffix: Namespace([NamespacePart(Data("cam".utf8))]),
                                 trackName: Data("video".utf8))
        try settle(c, s, seen)
        let cev = c.pollEventsExtended()
        #expect(cev.contains(.subscribeTracksOk(cts)))
        #expect(cev.contains(.publishBlocked(PublishBlockedInfo(
            trackSubscription: cts,
            trackNamespaceSuffix: Namespace([NamespacePart(Data("cam".utf8))]),
            trackName: Data("video".utf8)))))

        // A default request forwards; a refusal carries its code and reason.
        let cts2 = try c.subscribeTracks(SubscribeTracksConfiguration(prefix: prefix("b")))
        try settle(c, s, seen)
        var req2: SubscribeTracksRequestInfo?
        for ev in s.pollEventsExtended() {
            if case .subscribeTracksRequest(let r) = ev { req2 = r }
        }
        let r2 = try #require(req2)
        #expect(r2.forward)
        #expect(r2.trackSubscription != r.trackSubscription)
        try s.rejectSubscribeTracks(r2.trackSubscription,
                                    errorCode: RequestErrorCode.doesNotExist,
                                    reason: "none here")
        try settle(c, s, seen)
        #expect(c.pollEventsExtended().contains(.subscribeTracksError(SubscribeTracksErrorInfo(
            trackSubscription: cts2, errorCode: RequestErrorCode.doesNotExist,
            reason: "none here", canRetry: false, retryAfterMs: 0))))

        // The subscriber's request stream reset ends the accepted request.
        let ref = try #require(seen.opened[0].first)
        try s.receiveBidiStreamReset(on: toPeer(ref, 0), errorCode: 13, now: 0)
        #expect(s.pollEventsExtended().contains(.subscribeTracksCancelled(SubscribeTracksCancelledInfo(
            trackSubscription: r.trackSubscription, cause: .peerResetStream,
            streamErrorCode: 13))))
    }

    @Test("SUBSCRIBE_TRACKS has no draft-16 form; the pool size reaches the session")
    func subscribeTracksLimits() throws {
        let (c16, _) = try pair(.draft16)
        #expect(throws: MoQError.invalidArgument) {
            try c16.subscribeTracks(SubscribeTracksConfiguration(prefix: prefix("a")))
        }
        #expect(c16.pollActionsExtended().isEmpty)

        var bad = Session.Configuration(perspective: .client)
        bad.wireVersion = .draft22
        bad.maxTrackSubscriptions = 0x10000
        #expect(throws: MoQError.invalidArgument) { try Session(configuration: bad) }

        let (c, _) = try pair(.draft22, client: { $0.maxTrackSubscriptions = 1 })
        try c.subscribeTracks(SubscribeTracksConfiguration(prefix: prefix("a")))
        #expect(throws: MoQError.wouldBlock) {
            try c.subscribeTracks(SubscribeTracksConfiguration(prefix: prefix("b")))
        }
    }

    @Test("A cancelled SUBSCRIBE_TRACKS reads its cause only when the detail covers it")
    func subscribeTracksCancelledGate() {
        var ev = moq_event_t()
        ev.kind = MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED
        ev.detail_size = UInt32(MemoryLayout<moq_subscribe_tracks_cancelled_event_t>.size)
        ev.u.subscribe_tracks_cancelled.handle = moq_track_sub_handle_t(_opaque: 0x55)
        ev.u.subscribe_tracks_cancelled.cause = MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING
        ev.u.subscribe_tracks_cancelled.stream_error_code = 8
        let saved = ev
        let h = TrackSubscription(moq_track_sub_handle_t(_opaque: 0x55))
        #expect(ExtendedEvent.from(&ev) == .subscribeTracksCancelled(SubscribeTracksCancelledInfo(
            trackSubscription: h, cause: .peerStopSending, streamErrorCode: 8)))
        ev = saved
        ev.detail_size = UInt32(MemoryLayout<moq_subscribe_tracks_cancelled_event_t>.offset(
            of: \moq_subscribe_tracks_cancelled_event_t.cause)!)
        #expect(ExtendedEvent.from(&ev) == .subscribeTracksCancelled(SubscribeTracksCancelledInfo(
            trackSubscription: h, cause: .none, streamErrorCode: 0)))
    }

    // MARK: - REQUEST_REDIRECT

    private func nextRedirect(_ s: Session) -> RequestRedirectInfo? {
        for ev in s.pollEventsExtended() {
            if case .requestRedirect(let r) = ev { return r }
        }
        return nil
    }

    @Test("A redirected SUBSCRIBE, FETCH and announcement name their handles")
    func requestRedirect() throws {
        let (c, s) = try pair(.draft22)
        let uri = Array("moqt://relay.invalid/next".utf8)
        let part = Array("moved".utf8)
        let name = Array("t2".utf8)
        let why = Array("go there".utf8)

        func target(_ body: (moq_redirect_target_t) throws -> Void,
                    trackName: Bool = true) rethrows {
            try uri.withUnsafeBufferPointer { u in
                try part.withUnsafeBufferPointer { p in
                    try name.withUnsafeBufferPointer { n in
                        var parts = [moq_bytes_t(data: p.baseAddress, len: p.count)]
                        try parts.withUnsafeMutableBufferPointer { pp in
                            var t = moq_redirect_target_t()
                            t.connect_uri = moq_bytes_t(data: u.baseAddress, len: u.count)
                            t.track_namespace = moq_namespace_t(parts: pp.baseAddress, count: 1)
                            if trackName {
                                t.track_name = moq_bytes_t(data: n.baseAddress, len: n.count)
                            }
                            try body(t)
                        }
                    }
                }
            }
        }
        func reason(_ body: (moq_bytes_t) throws -> Void) rethrows {
            try why.withUnsafeBufferPointer {
                try body(moq_bytes_t(data: $0.baseAddress, len: $0.count))
            }
        }
        func expected(_ req: RedirectedRequest, trackName: Bool = true) -> RequestRedirectInfo {
            RequestRedirectInfo(
                request: req, errorCode: RequestErrorCode.redirect, canRetry: true,
                retryAfterMs: 500, connectURI: Data(uri),
                trackNamespace: Namespace([NamespacePart(Data(part))]),
                trackName: trackName ? Data(name) : Data(), reason: "go there")
        }

        // SUBSCRIBE
        let csub = try c.subscribe(SubscribeConfiguration(track: track(), filter: .latestObject))
        try settle(c, s)
        var ssub: Subscription?
        for ev in s.pollEventsExtended() {
            if case .subscribeRequest(let r) = ev { ssub = r.subscription }
        }
        let sub = try #require(ssub)
        try target { t in
            try reason { rs in
                var cfg = moq_reject_subscribe_cfg_t()
                moq_reject_subscribe_cfg_init(&cfg)
                cfg.error_code = RequestErrorCode.redirect
                cfg.reason = rs
                cfg.can_retry = true
                cfg.retry_after_ms = 500
                cfg.redirect = t
                try MoQError.check(moq_session_reject_subscribe(s.raw, sub.raw, &cfg, 0))
            }
        }
        try settle(c, s)
        #expect(nextRedirect(c) == expected(.subscription(csub)))

        // FETCH
        let cf = try c.fetch(FetchConfiguration(track: track("f")))
        try settle(c, s)
        let freq = try #require(nextFetchRequest(s))
        try target { t in
            try reason { rs in
                var cfg = moq_reject_fetch_cfg_t()
                moq_reject_fetch_cfg_init(&cfg)
                cfg.error_code = RequestErrorCode.redirect
                cfg.reason = rs
                cfg.can_retry = true
                cfg.retry_after_ms = 500
                cfg.redirect = t
                try MoQError.check(moq_session_reject_fetch(s.raw, freq.fetch.raw, &cfg, 0))
            }
        }
        try settle(c, s)
        #expect(nextRedirect(c) == expected(.fetch(cf)))

        // Namespace announcement: a namespace-scoped redirect has no track name.
        let ann = try c.publishNamespace(Namespace([NamespacePart(Data("ns".utf8))]))
        try settle(c, s)
        var sann: Announcement?
        for ev in s.pollEventsExtended() {
            if case .namespacePublished(let p) = ev { sann = p.announcement }
        }
        let peerAnn = try #require(sann)
        try target({ t in
            try reason { rs in
                var cfg = moq_reject_namespace_cfg_t()
                moq_reject_namespace_cfg_init(&cfg)
                cfg.error_code = RequestErrorCode.redirect
                cfg.reason = rs
                cfg.can_retry = true
                cfg.retry_after_ms = 500
                cfg.redirect = t
                try MoQError.check(moq_session_reject_namespace(s.raw, peerAnn.raw, &cfg, 0))
            }
        }, trackName: false)
        try settle(c, s)
        #expect(nextRedirect(c) == expected(.announcement(ann), trackName: false))
    }

    @Test("A redirect's handle is read by its family")
    func requestRedirectFamilies() {
        func convert(_ family: moq_request_family_t, _ raw: UInt64) -> RedirectedRequest? {
            var ev = moq_event_t()
            ev.kind = MOQ_EVENT_REQUEST_REDIRECT
            ev.detail_size = UInt32(MemoryLayout<moq_request_redirect_event_t>.size)
            ev.u.request_redirect.family = family
            ev.u.request_redirect.handle.raw = raw
            ev.u.request_redirect.error_code = RequestErrorCode.redirect
            guard case .requestRedirect(let r) = ExtendedEvent.from(&ev) else { return nil }
            return r.request
        }
        #expect(convert(MOQ_REQUEST_FAMILY_SUBSCRIBE, 0x11) ==
                .subscription(Subscription(moq_subscription_t(_opaque: 0x11))))
        #expect(convert(MOQ_REQUEST_FAMILY_FETCH, 0x12) ==
                .fetch(Fetch(moq_fetch_t(_opaque: 0x12))))
        #expect(convert(MOQ_REQUEST_FAMILY_TRACK_STATUS, 0x13) == .trackStatus(handle: 0x13))
        #expect(convert(MOQ_REQUEST_FAMILY_ANNOUNCEMENT, 0x14) ==
                .announcement(Announcement(moq_announcement_t(_opaque: 0x14))))
        #expect(convert(MOQ_REQUEST_FAMILY_NS_SUB, 0x15) == .namespaceSubscription(handle: 0x15))
        #expect(convert(MOQ_REQUEST_FAMILY_PUBLISH, 0x16) ==
                .publication(Publication(moq_publication_t(_opaque: 0x16))))
        #expect(convert(MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, 0x17) ==
                .trackSubscription(TrackSubscription(moq_track_sub_handle_t(_opaque: 0x17))))
        #expect(convert(moq_request_family_t(rawValue: 99), 0x18) ==
                .other(family: 99, handle: 0x18))
    }

    // MARK: - Value pins

    @Test("PUBLISH request and response retain independent expiry values",
          arguments: [nil, 0, 17, UInt64.max] as [UInt64?],
                     [nil, 0, 29, UInt64.max] as [UInt64?])
    func publishExpiryObservations(_ initialExpiry: UInt64?, _ responseExpiry: UInt64?) throws {
        let snapshots: (PublishRequestInfo, PublishOkInfo) = try {
            let (c, s) = try pair(.draft22)
            var config = PublishConfiguration(track: track("expiry"))
            config.expiresMs = initialExpiry
            let publication = try c.publish(config)
            try settle(c, s)
            let request = try #require(s.pollEventsExtended().compactMap { event -> PublishRequestInfo? in
                if case .publishRequest(let value) = event { return value }; return nil
            }.first)
            #expect(request.expiresMs == initialExpiry)
            try s.acceptPublish(request.publication, expiresMs: responseExpiry)
            try settle(c, s)
            let response = try #require(c.pollEventsExtended().compactMap { event -> PublishOkInfo? in
                if case .publishOk(let value) = event { return value }; return nil
            }.first)
            #expect(response.publication == publication)
            try c.tick(now: 1)
            try s.tick(now: 1)
            return (request, response)
        }()
        // Both sessions are gone; initial and response observations remain distinct.
        #expect(snapshots.0.expiresMs == initialExpiry)
        #expect(snapshots.1.expiresMs == responseExpiry)
    }

    @Test("UPDATE ACKs retain peer Largest presence and full width",
          arguments: [nil, 0, UInt64.max] as [UInt64?])
    func updateAckObservations(_ largest: UInt64?) throws {
        let snapshots: (SubscriptionUpdateOkInfo, PublicationUpdateOkInfo) = try {
            let (c, s) = try pair(.draft22)
            let (subscription, _) = try subscribeAccepted(c, s, largest: largest.map { ($0, $0) })
            var update = SubscriptionUpdateConfiguration()
            update.subscriberPriority = 13
            try c.updateSubscription(subscription, update)
            try settle(c, s)
            _ = s.pollEventsExtended()
            let subscriptionAck = try #require(c.pollEventsExtended().compactMap { event -> SubscriptionUpdateOkInfo? in
                if case .subscriptionUpdateOk(let value) = event { return value }; return nil
            }.first)
            #expect(subscriptionAck.subscription == subscription)

            // The publisher's same-track history supplies the publication ACK too.
            _ = try s.publish(PublishConfiguration(track: track()))
            try settle(c, s)
            let request = try #require(c.pollEventsExtended().compactMap { event -> PublishRequestInfo? in
                if case .publishRequest(let value) = event { return value }; return nil
            }.first)
            try c.acceptPublish(request.publication)
            try settle(c, s)
            _ = s.pollEventsExtended()
            try c.updatePublication(request.publication, update)
            try settle(c, s)
            _ = s.pollEventsExtended()
            let publicationAck = try #require(c.pollEventsExtended().compactMap { event -> PublicationUpdateOkInfo? in
                if case .publicationUpdateOk(let value) = event { return value }; return nil
            }.first)
            #expect(publicationAck.publication == request.publication)
            try c.tick(now: 1)
            try s.tick(now: 1)
            return (subscriptionAck, publicationAck)
        }()
        #expect(snapshots.0.largest?.group == largest && snapshots.0.largest?.object == largest)
        #expect(snapshots.1.largest?.group == largest && snapshots.1.largest?.object == largest)
        // Native outbound ACKs omit expiry; present expiry is conversion-tested.
        #expect(snapshots.0.expiresMs == nil && snapshots.1.expiresMs == nil)
    }

    @Test("Typed initial PUBLISH values stay immutable after UPDATE")
    func immutablePublishParameters() throws {
        let (c, s) = try pair(.draft22)
        var cfg = PublishConfiguration(track: track("initial"))
        cfg.forward = false
        cfg.subscriberPriority = 23
        cfg.groupOrder = .descending
        cfg.location = .nextObject
        cfg.objectDeliveryTimeoutMs = 0
        cfg.subgroupDeliveryTimeoutMs = .max
        cfg.expiresMs = 0
        _ = try c.publish(cfg)
        try settle(c, s)
        let request = try #require(s.pollEventsExtended().compactMap { event -> PublishRequestInfo? in
            if case .publishRequest(let r) = event { return r }; return nil
        }.first)
        #expect(!request.forward)
        #expect(request.expiresMs == 0)
        let initial = try s.publishInitialParameters(request.publication)
        #expect(initial.hasSubscriberPriority && initial.subscriberPriority == 23)
        #expect(initial.hasGroupOrder && initial.groupOrder == .descending)
        #expect(initial.location == .nextObject)
        #expect(initial.objectDeliveryTimeoutMs == 0)
        #expect(initial.subgroupDeliveryTimeoutMs == .max)
        try s.acceptPublish(request.publication, expiresMs: 0)
        try settle(c, s)
        var update = SubscriptionUpdateConfiguration()
        update.subscriberPriority = 71
        try s.updatePublication(request.publication, update)
        try settle(c, s)
        #expect(try s.publishInitialParameters(request.publication) == initial)
    }

    @Test("UPDATE preserves exact independent timeouts and copied location")
    func losslessSubscriptionUpdate() throws {
        let (c, s) = try pair(.draft22)
        let (sub, _) = try subscribeAccepted(c, s)
        var cfg = SubscriptionUpdateConfiguration()
        cfg.location = .none as LocationFilter
        cfg.objectDeliveryTimeoutMs = 0
        cfg.subgroupDeliveryTimeoutMs = .max
        try c.updateSubscription(sub, cfg)
        cfg.location = .nextObject
        cfg.objectDeliveryTimeoutMs = 99
        try settle(c, s)
        let observed = try #require(s.pollEventsExtended().compactMap { event -> SubscriptionUpdateOptions? in
            if case .subscribeUpdated(let r) = event { return r.update.options }; return nil
        }.first)
        try s.tick(now: 1)
        #expect(observed.location == LocationFilter.none)
        #expect(observed.objectDeliveryTimeoutMs == 0)
        #expect(observed.subgroupDeliveryTimeoutMs == .max)
        #expect(observed.initiatingRequestID != 0)
        let (c18, s18) = try pair(.draft18)
        let (sub18, _) = try subscribeAccepted(c18, s18)
        #expect(throws: MoQError.unsupported) { try c18.updateSubscription(sub18, cfg) }
        #expect(c18.pollActionsExtended().isEmpty)
    }

    @Test("Publication UPDATE carries typed selections without borrowing", arguments: 0..<8)
    func losslessPublicationUpdate(_ row: Int) throws {
        let (c, s) = try pair(.draft22)
        let publication = try c.publish(PublishConfiguration(track: track("a08")))
        try settle(c, s)
        let request = try #require(s.pollEventsExtended().compactMap { event -> PublishRequestInfo? in
            if case .publishRequest(let value) = event { return value }; return nil
        }.first)
        let initial = try s.publishInitialParameters(request.publication)
        try s.acceptPublish(request.publication)
        try settle(c, s)
        _ = c.pollEventsExtended()
        var config = SubscriptionUpdateConfiguration()
        switch row {
        case 1: config.location = LocationFilter.none
        case 2:
            config.location = .relativeGroup(offset: .max)
            var fill = FillConfiguration()
            fill.location = LocationFilter.none
            config.fill = fill
        case 3:
            config.location = .objectEnd(startGroup: 1, startObject: 2,
                                         endGroup: 3, endObject: .max)
        case 4: config.objectDeliveryTimeoutMs = 0
        case 5: config.subgroupDeliveryTimeoutMs = .max
        case 6:
            config.objectDeliveryTimeoutMs = 0
            config.subgroupDeliveryTimeoutMs = .max
        case 7:
            config.objectDeliveryTimeoutMs = .max
            config.subgroupDeliveryTimeoutMs = 0
        default: break
        }
        let expected = config
        try s.updatePublication(request.publication, config)
        config = SubscriptionUpdateConfiguration()
        try settle(c, s)
        let updates = c.pollEventsExtended().compactMap { event -> PublicationUpdatedInfo? in
            if case .publishUpdated(let value) = event { return value }; return nil
        }
        #expect(updates.count == 1)
        let update = try #require(updates.first)
        #expect(update.publication == publication)
        #expect(update.update.updateCount == 1 && !update.update.answerOwed)
        #expect(update.update.fillRequested == (expected.fill != nil))
        #expect(update.update.filter == nil && update.update.deliveryTimeoutUs == nil)
        let copied = try #require(update.update.options)
        try c.tick(now: 1)
        #expect(copied.location == expected.location)
        #expect(copied.objectDeliveryTimeoutMs == expected.objectDeliveryTimeoutMs)
        #expect(copied.subgroupDeliveryTimeoutMs == expected.subgroupDeliveryTimeoutMs)
        #expect(try s.publishInitialParameters(request.publication) == initial)
        config.deliveryTimeoutUs = 1000
        #expect(throws: MoQError.unsupported) { try s.updatePublication(request.publication, config) }
        #expect(s.pollActionsExtended().isEmpty)
    }

    @Test("Legacy publication UPDATE keeps generic input and applies the draft's rules to typed options",
          arguments: [MoQTransportModel.WireVersion.draft16, .draft18])
    func legacyPublicationUpdate(_ version: MoQTransportModel.WireVersion) throws {
        let (c, s) = try pair(version)
        _ = try c.publish(PublishConfiguration(track: track("legacy-update")))
        try settle(c, s)
        let request = try #require(s.pollEventsExtended().compactMap { event -> PublishRequestInfo? in
            if case .publishRequest(let value) = event { return value }; return nil
        }.first)
        try s.acceptPublish(request.publication)
        try settle(c, s)
        _ = c.pollEventsExtended()
        // The Location Filter is draft-22 only. Drafts 16 and 18 carry the
        // timeouts (draft-16 9.2.2.2, draft-18 10.2.3, 10.2.4): draft-16's
        // single DELIVERY_TIMEOUT is a QUIC varint and never 0; draft-18 takes
        // 0 and UInt64.max alike.
        for row in 0..<3 {
            var config = SubscriptionUpdateConfiguration()
            if row == 0 { config.location = LocationFilter.none }
            if row == 1 { config.objectDeliveryTimeoutMs = 0 }
            if row == 2 { config.subgroupDeliveryTimeoutMs = .max }
            if row == 0 {
                #expect(throws: MoQError.unsupported) { try s.updatePublication(request.publication, config) }
                #expect(s.pollActionsExtended().isEmpty)
            } else if version == .draft16 {
                #expect(throws: MoQError.invalidArgument) { try s.updatePublication(request.publication, config) }
                #expect(s.pollActionsExtended().isEmpty)
            } else {
                try s.updatePublication(request.publication, config)
                try settle(c, s)
                _ = c.pollEventsExtended()
            }
        }
        var config = SubscriptionUpdateConfiguration()
        config.deliveryTimeoutUs = 1000
        try s.updatePublication(request.publication, config)
        try settle(c, s)
        let update = try #require(c.pollEventsExtended().compactMap { event -> UpdateRequestInfo? in
            if case .publishUpdated(let value) = event { return value.update }; return nil
        }.first)
        #expect(update.options == nil && update.deliveryTimeoutUs == 1000)
    }

    @Test("Codes match the C library's")
    func codes() {
        #expect(RequestErrorCode.conflictingFilters == 0x35)
        #expect(RequestErrorCode.invalidFilter == 0x36)
        #expect(StreamErrorCode.unknownObjectStatus == 0x6)
        #expect(StreamErrorCode.d16UnknownObjectStatus == 0x4)
        #expect(FetchRangeKind(3) == .timedOut)
        #expect(FetchRangeKind.timedOut.cValue == MOQ_FETCH_RANGE_TIMED_OUT)
        #expect(RequestTerminalCause(4) == .truncatedResponse)
        #expect(RequestRole(2) == .responder)
    }

    // MARK: - FIRST_OBJECT

    /// `openSubgroup(firstObject:)` reaches the wire (section 2.2) and the
    /// subscriber reports it on the stream's first object only; a subgroup
    /// opened without it reports none. The event field is read only when the
    /// C detail covers it.
    @Test("FIRST_OBJECT through the binding")
    func firstObject() throws {
        let (c, s) = try pair(.draft22)
        let (_, ssub) = try subscribeAccepted(c, s)
        for (group, first) in [(UInt64(10), true), (UInt64(11), false)] {
            let sg = try s.openSubgroup(for: ssub, groupID: group, firstObject: first)
            for object in UInt64(0)..<2 {
                try s.writeObject(to: sg, objectID: object,
                                  payload: try Buffer(Data([UInt8(object)])))
            }
            try s.closeSubgroup(sg)
            try settle(c, s)
            var flags: [Bool] = []
            for ev in c.pollEvents() {
                if case .objectReceived(let obj) = ev, obj.groupID == group {
                    flags.append(obj.firstObject)
                }
            }
            #expect(flags == [first, false])
        }

        var ev = moq_event_t()
        ev.kind = MOQ_EVENT_OBJECT_RECEIVED
        ev.u.object_received.first_object = true
        let end = MemoryLayout<moq_object_received_event_t>.offset(
            of: \moq_object_received_event_t.first_object)! + 1
        ev.detail_size = UInt32(MemoryLayout<moq_object_received_event_t>.size)
        guard case .objectReceived(let full) = Event.from(&ev) else {
            Issue.record("expected objectReceived"); return
        }
        #expect(full.firstObject)
        ev = moq_event_t()
        ev.kind = MOQ_EVENT_OBJECT_RECEIVED
        ev.u.object_received.first_object = true
        ev.detail_size = UInt32(end - 1)
        if case .objectReceived(let short) = Event.from(&ev) {
            #expect(!short.firstObject)
        }
    }

    /// Gaps written through `ObjectConfiguration` reach the subscriber's
    /// `ReceivedObject` (and the subgroup's PROPERTIES form), read only when
    /// the C detail covers them. A Subgroup reopened at another priority
    /// makes the Track malformed: the subscriber cancels and reports
    /// `.unsubscribed` with `.malformedTrack` (draft-22 §12.1).
    @Test("Prior gaps and malformed Tracks through the binding")
    func priorGapsAndMalformed() throws {
        let (c, s) = try pair(.draft22)
        let (_, ssub) = try subscribeAccepted(c, s)
        let sg = try s.openSubgroup(for: ssub, groupID: 10, objectProperties: true)
        try s.writeObject(to: sg, ObjectConfiguration(
            objectID: 4, payload: try Buffer(Data([7])),
            priorGroupGap: 3, priorObjectGap: 2))
        try s.closeSubgroup(sg)
        try settle(c, s)
        var got: [ReceivedObject] = []
        for ev in c.pollEvents() {
            if case .objectReceived(let obj) = ev { got.append(obj) }
        }
        #expect(got.count == 1)
        #expect(got.first?.priorGroupGap == 3)
        #expect(got.first?.priorObjectGap == 2)
        #expect(got.first?.subgroupProperties == true)

        var ev = moq_event_t()
        ev.kind = MOQ_EVENT_OBJECT_RECEIVED
        ev.u.object_received.has_prior_group_gap = true
        ev.u.object_received.prior_group_gap = 9
        ev.detail_size = UInt32(MemoryLayout<moq_object_received_event_t>.offset(
            of: \moq_object_received_event_t.has_prior_group_gap)!)
        if case .objectReceived(let short) = Event.from(&ev) {
            #expect(short.priorGroupGap == nil)
        }

        for k in UInt64(0)..<2 {
            let h = try s.openSubgroup(for: ssub, groupID: 11,
                                       publisherPriority: UInt8(20 + k))
            try s.writeObject(to: h, objectID: k, payload: try Buffer(Data([1])))
            if k == 0 { try s.closeSubgroup(h) }
            try settle(c, s)
        }
        var malformed = 0
        for ev in c.pollEventsExtended() {
            if case .unsubscribed(let info) = ev, info.cause == .malformedTrack,
               info.streamErrorCode == UInt64(MOQ_STREAM_ERROR_MALFORMED_TRACK) {
                malformed += 1
            }
        }
        #expect(malformed == 1)
    }
}
