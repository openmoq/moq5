import Testing
import Foundation
import CMoQCore
@testable import MoQ
import MoQTransportModel

/// Session and facade control surfaces through the Swift binding: SETUP
/// authorization tokens, GOAWAY with its Timeout (and a receiver that does
/// not close on it), close with a code, the subscription and publication
/// ends each side owns, namespace subscriptions, FETCH updates, resolved
/// filter windows, facade migrations and delivery timeouts, and the
/// publisher's GOAWAY callback.
@Suite("Session control surfaces")
struct SessionControlTests {

    private struct PumpError: Error {}

    private static let peerBit: [UInt64] = [1 << 40, 1 << 41]

    private func toPeer(_ ref: StreamRef, _ dir: Int) -> StreamRef {
        let back = Self.peerBit[dir ^ 1]
        if ref.rawValue & back != 0 { return StreamRef(rawValue: ref.rawValue & ~back) }
        return StreamRef(rawValue: ref.rawValue | Self.peerBit[dir])
    }

    private func pump(_ from: Session, _ to: Session, _ dir: Int) throws {
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
            case .resetData(let ref, let code, _):
                try to.receiveDataReset(on: toPeer(ref, dir), errorCode: code, now: 0)
            case .stopData(let ref, let code):
                try to.receiveDataStop(on: toPeer(ref, dir), errorCode: code, now: 0)
            case .openBidiStream(let ref, let d, let fin), .sendBidiStream(let ref, let d, let fin):
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
                break
            case .closeSession, .unknown:
                throw PumpError()
            }
        }
    }

    private func settle(_ c: Session, _ s: Session) throws {
        for _ in 0..<6 { try pump(c, s, 0); try pump(s, c, 1) }
    }

    /// An established pair; setup events are left queued when `keepSetup`.
    private func pair(
        _ version: WireVersion, keepSetup: Bool = false,
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
        if c.bindingRequirements != 0 { try bindInMemoryTransport(c, s) }
        try c.start()
        if version != .draft16 { try s.start() }
        try settle(c, s)
        #expect(c.state == .established)
        #expect(s.state == .established)
        if !keepSetup {
            _ = c.pollEventsExtended()
            _ = s.pollEventsExtended()
        }
        return (c, s)
    }

    private func details(_ s: Session) -> [(ExtendedEvent, EventDetail?)] {
        var out: [(ExtendedEvent, EventDetail?)] = []
        s.drainEventsWithDetail { out.append(($0, $1)) }
        return out
    }

    private let ns = Namespace([NamespacePart(Data("ctl".utf8))])

    private func track(_ name: String = "v") -> TrackName {
        TrackName(namespace: ns, name: Data(name.utf8))
    }

    private func subscribeAccepted(_ c: Session, _ s: Session,
                                   _ cfg: SubscribeConfiguration? = nil)
        throws -> (Subscription, Subscription) {
        let csub = try c.subscribe(cfg ?? SubscribeConfiguration(track: track(), filter: .latestObject))
        try settle(c, s)
        var ssub: Subscription?
        for ev in s.pollEventsExtended() {
            if case .subscribeRequest(let r) = ev { ssub = r.subscription }
        }
        let sub = try #require(ssub)
        var acc = AcceptSubscribeConfiguration()
        acc.largest = (3, 2)
        try s.acceptSubscribe(sub, config: acc)
        try settle(c, s)
        _ = c.pollEventsExtended()
        return (csub, sub)
    }

    // MARK: - SETUP and GOAWAY

    @Test("SETUP authorization tokens reach the peer's setup event",
          arguments: [WireVersion.draft16, .draft18, .draft22])
    func setupTokens(_ version: MoQTransportModel.WireVersion) throws {
        let tokens = [AuthorizationToken(type: 7, value: Data([1, 2, 3])),
                      AuthorizationToken(type: 0, value: Data([0xff]))]
        let (_, s) = try pair(version, keepSetup: true, client: {
            $0.setupAuthorizationTokens = tokens
        })
        let setup = details(s).first { if case .setupComplete = $0.0 { true } else { false } }
        let got = try #require(setup)
        #expect(got.1?.setupTokens == tokens)

        let (_, plain) = try pair(version, keepSetup: true)
        let none = details(plain).first { if case .setupComplete = $0.0 { true } else { false } }
        let gotNone = try #require(none)
        #expect(gotNone.1?.setupTokens == [])
    }

    @Test("Too many SETUP tokens are refused at create")
    func setupTokenLimit() throws {
        var cfg = Session.Configuration(perspective: .client)
        cfg.wireVersion = .draft22
        cfg.setupAuthorizationTokens = Array(
            repeating: AuthorizationToken(type: 1, value: Data([1])), count: 17)
        #expect(throws: MoQError.invalidArgument) { try Session(configuration: cfg) }
    }

    @Test("GOAWAY carries its URI and Timeout; the receiver does not close on it",
          arguments: [WireVersion.draft16, .draft18, .draft22])
    func goaway(_ version: MoQTransportModel.WireVersion) throws {
        let (c, s) = try pair(version, server: { $0.goawayTimeoutMicroseconds = 5_000_000 })
        let uri = Data("moqt://next.example/".utf8)
        try s.goaway(newSessionURI: uri)
        try settle(c, s)
        let got = details(c).compactMap { $0.1?.goaway }.first
        let g = try #require(got)
        #expect(g.newSessionURI == uri)
        #expect(g.timeoutMs == (version == .draft16 ? nil : 5000))
        // Long past the sender's Timeout the receiver is still open: the
        // GOAWAY_TIMEOUT close is the sender's.
        try c.tick(now: 60_000_000)
        #expect(c.state != .closed)
        var closed: UInt64?
        c.drainActionsExtended { if case .closeSession(let code, _) = $0 { closed = code } }
        #expect(closed == nil)
    }

    @Test("Close with a termination code")
    func closeWithCode() throws {
        let (c, _) = try pair(.draft22)
        try c.close(code: 0x5)
        var closed: UInt64?
        c.drainActionsExtended { if case .closeSession(let code, _) = $0 { closed = code } }
        #expect(closed == 0x5)
        #expect(c.state == .closed)
    }

    // MARK: - Subscription and publication ends

    @Test("Unsubscribe and PUBLISH_DONE end a subscription from each side")
    func subscriptionEnds() throws {
        let (c, s) = try pair(.draft22)
        let (csub, _) = try subscribeAccepted(c, s)
        try c.unsubscribe(csub)
        try settle(c, s)
        #expect(s.pollEventsExtended().contains { if case .unsubscribed = $0 { true } else { false } })

        let (csub2, ssub2) = try subscribeAccepted(c, s,
            SubscribeConfiguration(track: track("w"), filter: .latestObject))
        try s.doneSubscribe(ssub2, statusCode: 0x2, streamCount: 0, reason: Data("over".utf8))
        try settle(c, s)
        let dones = details(c).compactMap { $0.1?.subscribeDone }
        let done = try #require(dones.first)
        #expect(done.subscription == csub2)
        #expect(done.statusCode == 0x2)
        #expect(done.reason == Data("over".utf8))
    }

    @Test("A PUBLISH is refused, or finished by its sender")
    func publicationEnds() throws {
        let (c, s) = try pair(.draft22)
        _ = try c.publish(PublishConfiguration(track: track("p1")))
        try settle(c, s)
        var req: Publication?
        for ev in s.pollEventsExtended() { if case .publishRequest(let r) = ev { req = r.publication } }
        try s.rejectPublish(try #require(req), errorCode: 0x0, reason: Data("no".utf8))
        try settle(c, s)
        #expect(c.pollEventsExtended().contains {
            if case .unknown(let kind) = $0 { kind == MOQ_EVENT_PUBLISH_ERROR } else { false }
        })

        let pub = try c.publish(PublishConfiguration(track: track("p2")))
        try settle(c, s)
        req = nil
        for ev in s.pollEventsExtended() { if case .publishRequest(let r) = ev { req = r.publication } }
        let peer = try #require(req)
        try s.acceptPublish(peer)
        try settle(c, s)
        _ = c.pollEventsExtended()
        try c.finishPublish(pub, statusCode: 0x2, streamCount: 0, reason: Data("end".utf8))
        try settle(c, s)
        let fin = s.pollEventsExtended().compactMap { ev -> PublishFinishedInfo? in
            if case .publishFinished(let f) = ev { return f }; return nil
        }.first
        #expect(try #require(fin).statusCode == 0x2)
        #expect(fin?.publication == peer)
    }

    @Test("The PUBLISH side updates its own delivery timeouts on draft 22 only")
    func publisherUpdate() throws {
        for version in [WireVersion.draft22, .draft18] {
            let (c, s) = try pair(version)
            let pub = try c.publish(PublishConfiguration(track: track()))
            try settle(c, s)
            var req: Publication?
            for ev in s.pollEventsExtended() { if case .publishRequest(let r) = ev { req = r.publication } }
            try s.acceptPublish(try #require(req))
            try settle(c, s)
            _ = c.pollEventsExtended()
            var priority = SubscriptionUpdateConfiguration()
            priority.subscriberPriority = 3
            var timeouts = SubscriptionUpdateConfiguration()
            timeouts.objectDeliveryTimeoutMs = 250
            if version == .draft22 {
                #expect(throws: MoQError.invalidArgument) { try c.updatePublication(pub, priority) }
                try c.updatePublication(pub, timeouts)
                try settle(c, s)
                #expect(c.state == .established && s.state == .established)
            } else {
                #expect(throws: MoQError.unsupported) { try c.updatePublication(pub, timeouts) }
            }
        }
    }

    // MARK: - Namespace subscriptions

    @Test("SUBSCRIBE_NAMESPACE, its answer and the namespaces under it",
          arguments: [WireVersion.draft18, .draft22])
    func namespaceSubscription(_ version: MoQTransportModel.WireVersion) throws {
        let (c, s) = try pair(version)
        let handle = try c.subscribeNamespace(prefix: ns)
        try settle(c, s)
        let reqs = details(s).compactMap { $0.1?.namespaceSubscriptionRequest }
        let req = try #require(reqs.first)
        #expect(req.prefix == ns)
        try s.acceptNamespaceSubscription(req.handle)
        let suffix = Namespace([NamespacePart(Data("room".utf8))])
        try s.sendNamespace(on: req.handle, suffix: suffix)
        try settle(c, s)
        var d = details(c).compactMap { $0.1 }
        #expect(d.contains { $0.namespaceSubscriptionOk?.handle == handle })
        #expect(d.contains { $0.namespaceFound?.suffix == suffix && $0.namespaceFound?.handle == handle })
        try s.sendNamespace(on: req.handle, suffix: suffix, done: true)
        try settle(c, s)
        d = details(c).compactMap { $0.1 }
        #expect(d.contains { $0.namespaceGone?.suffix == suffix })

        let refused = try c.subscribeNamespace(prefix: Namespace([NamespacePart(Data("x".utf8))]))
        try settle(c, s)
        let reqs2 = details(s).compactMap { $0.1?.namespaceSubscriptionRequest }
        let req2 = try #require(reqs2.first)
        try s.rejectNamespaceSubscription(req2.handle, errorCode: 0x0, reason: Data("no".utf8))
        try settle(c, s)
        let errs = details(c).compactMap { $0.1?.namespaceSubscriptionError }
        let err = try #require(errs.first)
        #expect(err.handle == refused && err.errorCode == 0x0 && err.reason == Data("no".utf8))
    }

    // MARK: - FETCH update and windows

    @Test("A FETCH update: one outstanding, acknowledged, draft 22 only")
    func fetchUpdate() throws {
        let (c, s) = try pair(.draft22)
        var fc = FetchConfiguration(track: track("f"))
        fc.endGroup = 4
        let fetch = try c.fetch(fc)
        try settle(c, s)
        var freq: FetchRequestInfo?
        for ev in s.pollEventsExtended() { if case .fetchRequest(let r) = ev { freq = r } }
        try s.acceptFetch(try #require(freq).fetch, endGroup: 4, endObject: 0)
        try settle(c, s)
        _ = c.pollEventsExtended()
        try c.updateFetch(fetch, subscriberPriority: 9)
        #expect(throws: MoQError.wrongState) { try c.updateFetch(fetch, subscriberPriority: 8) }
        try settle(c, s)
        try c.updateFetch(fetch, authorizationTokens: [AuthorizationToken(type: 1, value: Data([1]))])
        #expect(throws: MoQError.invalidArgument) { try c.updateFetch(fetch) }

        let (c18, s18) = try pair(.draft18)
        var fc18 = FetchConfiguration(track: track("f"))
        fc18.endGroup = 4
        let f18 = try c18.fetch(fc18)
        try settle(c18, s18)
        #expect(throws: MoQError.unsupported) { try c18.updateFetch(f18, subscriberPriority: 9) }
    }

    @Test("The resolved filter window of a subscription and a publication")
    func windows() throws {
        let (c, s) = try pair(.draft22)
        var cfg = SubscribeConfiguration(track: track(), filter: .latestObject)
        cfg.location = .groupEnd(startGroup: 5, startObject: 1, endGroup: 7)
        let (csub, ssub) = try subscribeAccepted(c, s, cfg)
        for w in [try c.subscriptionWindow(csub), try s.subscriptionWindow(ssub)] {
            #expect(w.resolved && !w.empty)
            #expect(w.startGroup == 5 && w.startObject == 1)
            #expect(w.endGroup == 7 && w.endObject == nil)
        }

        // A publication without a Location Filter has no window; one with a
        // filter resolves it on both sides.
        for filtered in [false, true] {
            var pc = PublishConfiguration(track: track(filtered ? "pf" : "p"))
            if filtered { pc.location = .groupEnd(startGroup: 2, startObject: 0, endGroup: 9) }
            let pub = try c.publish(pc)
            try settle(c, s)
            var req: Publication?
            for ev in s.pollEventsExtended() { if case .publishRequest(let r) = ev { req = r.publication } }
            let peer = try #require(req)
            try s.acceptPublish(peer)
            try settle(c, s)
            for w in [try c.publicationWindow(pub), try s.publicationWindow(peer)] {
                #expect(w.resolved == filtered)
                if filtered {
                    #expect(w.startGroup == 2 && w.startObject == 0 && w.endGroup == 9)
                }
            }
        }
        #expect(throws: MoQError.self) { try c.subscriptionWindow(ssub) }
    }

    // MARK: - Facades

    @Test("A redirected facade subscription keeps where it went")
    func facadeRedirect() throws {
        let (c, s) = try pair(.draft22)
        let sub = try Subscriber(session: c)
        let t = try sub.subscribe(track: track(), filter: .latestObject)
        try settle(c, s)
        try sub.tick(now: 1)
        var ssub: Subscription?
        for ev in s.pollEventsExtended() {
            if case .subscribeRequest(let r) = ev { ssub = r.subscription }
        }
        let peer = try #require(ssub)
        #expect(try sub.migration(of: t) == nil)
        let uri = Array("moqt://relay.invalid/next".utf8)
        let name = Array("t2".utf8)
        try uri.withUnsafeBufferPointer { u in
            try name.withUnsafeBufferPointer { n in
                var cfg = moq_reject_subscribe_cfg_t()
                moq_reject_subscribe_cfg_init(&cfg)
                cfg.error_code = RequestErrorCode.redirect
                cfg.can_retry = true
                cfg.retry_after_ms = 40
                cfg.redirect.connect_uri = moq_bytes_t(data: u.baseAddress, len: u.count)
                cfg.redirect.track_name = moq_bytes_t(data: n.baseAddress, len: n.count)
                try MoQError.check(moq_session_reject_subscribe(s.raw, peer.raw, &cfg, 0))
            }
        }
        try settle(c, s)
        try sub.tick(now: 2)
        #expect(t.state == .error)
        let m = try #require(try sub.migration(of: t))
        #expect(m.redirect && !m.goaway && m.canRetry && m.retryAfterMs == 40)
        #expect(m.connectURI == Data(uri))
        #expect(m.trackName == Data(name))
    }

    @Test("Facade delivery timeouts reach the publisher on subscribe and update")
    func facadeTimeouts() throws {
        let (c, s) = try pair(.draft22)
        let sub = try Subscriber(session: c)
        let t = try sub.subscribe(track: track(), filter: .latestObject,
                                  objectDeliveryTimeoutMs: 300, subgroupDeliveryTimeoutMs: 0)
        try settle(c, s)
        try sub.tick(now: 1)
        var request: ExtendedSubscribeRequest?
        for ev in s.pollEventsExtended() {
            if case .subscribeRequest(let r) = ev { request = r }
        }
        let r = try #require(request)
        #expect(r.options?.objectDeliveryTimeoutMs == 300)
        #expect(r.options?.subgroupDeliveryTimeoutMs == 0)
        var acc = AcceptSubscribeConfiguration()
        acc.largest = (3, 2)
        try s.acceptSubscribe(r.subscription, config: acc)
        try settle(c, s)
        try sub.tick(now: 2)
        #expect(t.isActive)

        try sub.updateSubscription(t, location: .absoluteStart(group: 6, object: 0),
                                   objectDeliveryTimeoutMs: 900)
        try settle(c, s)
        var updated: SubscriptionUpdatedInfo?
        for ev in s.pollEventsExtended() {
            if case .subscribeUpdated(let u) = ev { updated = u }
        }
        let u = try #require(updated)
        #expect(u.update.options?.location == .absoluteStart(group: 6, object: 0))
        #expect(u.update.options?.objectDeliveryTimeoutMs == 900)
        #expect(u.update.options?.subgroupDeliveryTimeoutMs == nil)

        // Draft-18 carries the timeouts too (10.2.3, 10.2.4).
        let (c18, _) = try pair(.draft18)
        let sub18 = try Subscriber(session: c18)
        _ = try sub18.subscribe(track: track(), filter: .latestObject,
                                objectDeliveryTimeoutMs: 300, subgroupDeliveryTimeoutMs: nil)
    }

    @Test("The publisher facade reports the session GOAWAY with its URI")
    func publisherGoaway() throws {
        let (c, s) = try pair(.draft22)
        let pub = try Publisher(session: c)
        _ = try pub.addTrack(namespace: ns, name: "v")
        var uris: [Data] = []
        pub.onGoaway = { uris.append($0) }
        let uri = Data("moqt://next.example/".utf8)
        try s.goaway(newSessionURI: uri)
        try settle(c, s)
        try pub.tick(now: 3)
        #expect(uris == [uri])
    }
}
