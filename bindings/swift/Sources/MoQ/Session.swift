import CMoQCore

/// Sans-I/O MoQ session. Not thread-safe. Not Sendable.
///
/// All mutating methods are "advancing calls" that invalidate
/// previously borrowed C pointers. The Swift bindings copy all
/// borrowed data into owned values before returning.
public final class Session {

    // MARK: - Configuration

    public struct Configuration: Sendable {
        public var perspective: Perspective
        public var sendRequestCapacity: Bool
        public var initialRequestCapacity: UInt64
        public var maxActions: UInt32
        public var maxEvents: UInt32

        public init(perspective: Perspective) {
            self.perspective = perspective
            self.sendRequestCapacity = false
            self.initialRequestCapacity = 64
            self.maxActions = 64
            self.maxEvents = 16
        }
    }

    public enum Perspective: Sendable {
        case client
        case server
    }

    public enum State: Sendable, Hashable {
        case idle
        case setupSent
        case established
        case draining
        case closed
        case unknown(Int32)
    }

    // MARK: - Storage

    internal let raw: OpaquePointer  // moq_session_t*
    private let owned: Bool

    // MARK: - Lifecycle

    public init(configuration: Configuration) throws {
        /* Always-linked reference to the stack-guard canary: pulls its
         * object file into every consumer, so a dual link with the
         * installed-mode MoQService fails loudly (see StackGuard.swift). */
        _ = moqSwiftStackGuardSource()
        let alloc = moq_alloc_default()!
        var scfg = moq_session_cfg_t()
        // Sized init: this wrapper sets fields past the v0 prefix
        // (send_request_capacity, caps), so it must record the full struct_size
        // -- the pointer-only moq_session_cfg_init() would leave them ignored.
        moq_session_cfg_init_sized(
            &scfg, MemoryLayout<moq_session_cfg_t>.size, alloc,
            configuration.perspective == .client
                ? MOQ_PERSPECTIVE_CLIENT
                : MOQ_PERSPECTIVE_SERVER
        )
        scfg.send_request_capacity = configuration.sendRequestCapacity
        scfg.initial_request_capacity = configuration.initialRequestCapacity
        if configuration.maxActions > 0 {
            scfg.max_actions = configuration.maxActions
        }
        if configuration.maxEvents > 0 {
            scfg.max_events = configuration.maxEvents
        }

        var ptr: OpaquePointer?
        let rc = moq_session_create(&scfg, 0, &ptr)
        try MoQError.check(rc)
        guard let p = ptr else { throw MoQError.internal }
        self.raw = p
        self.owned = true
    }

    /// Wrap an externally-owned session without taking ownership.
    /// The caller (e.g. moq_pq_threaded_t) is responsible for the
    /// session's lifetime. This wrapper must not outlive the owner.
    package init(borrowing raw: OpaquePointer) {
        self.raw = raw
        self.owned = false
    }

    deinit {
        if owned {
            moq_session_destroy(raw)
        }
    }

    // MARK: - State

    public var state: State {
        let s = moq_session_state(raw)
        switch s {
        case MOQ_SESS_IDLE:        return .idle
        case MOQ_SESS_SETUP_SENT:  return .setupSent
        case MOQ_SESS_ESTABLISHED: return .established
        case MOQ_SESS_DRAINING:    return .draining
        case MOQ_SESS_CLOSED:      return .closed
        default:                   return .unknown(Int32(s.rawValue))
        }
    }

    // MARK: - Transport Input

    /// Start the session (client-side: sends CLIENT_SETUP).
    public func start() throws {
        try MoQError.check(moq_session_start(raw, 0))
    }

    /// Advance virtual time, fire deadlines.
    public func tick(now: UInt64) throws {
        try MoQError.check(moq_session_tick(raw, now))
    }

    /// Feed inbound control-stream bytes.
    public func receiveControlBytes(
        _ bytes: UnsafeRawBufferPointer, now: UInt64
    ) throws {
        try MoQError.check(
            moq_session_on_control_bytes(
                raw,
                bytes.baseAddress?.assumingMemoryBound(to: UInt8.self),
                bytes.count, now))
    }

    /// Feed inbound data-stream bytes.
    public func receiveDataBytes(
        on stream: StreamRef,
        _ bytes: UnsafeRawBufferPointer,
        fin: Bool, now: UInt64
    ) throws {
        let ref = moq_stream_ref_t(_v: stream.rawValue)
        try MoQError.check(
            moq_session_on_data_bytes(
                raw, ref,
                bytes.baseAddress?.assumingMemoryBound(to: UInt8.self),
                bytes.count, fin, now))
    }

    // MARK: - Namespace Publishing

    /// Announce a namespace to the peer.
    @discardableResult
    public func publishNamespace(
        _ namespace: Namespace, now: UInt64 = 0
    ) throws -> Announcement {
        try withCNamespace(namespace) { ns in
            var cfg = moq_publish_namespace_cfg_t()
            moq_publish_namespace_cfg_init(&cfg)
            cfg.track_namespace = ns

            var handle = moq_announcement_t(_opaque: 0)
            try MoQError.check(
                moq_session_publish_namespace(raw, &cfg, now, &handle))
            return Announcement(handle)
        }
    }

    /// Withdraw an established namespace announcement.
    public func publishNamespaceDone(
        _ announcement: Announcement, now: UInt64 = 0
    ) throws {
        try MoQError.check(
            moq_session_publish_namespace_done(raw, announcement.raw, now))
    }

    /// Accept an inbound namespace announcement from the peer.
    public func acceptNamespace(
        _ announcement: Announcement, now: UInt64 = 0
    ) throws {
        var cfg = moq_accept_namespace_cfg_t()
        moq_accept_namespace_cfg_init(&cfg)
        try MoQError.check(
            moq_session_accept_namespace(raw, announcement.raw, &cfg, now))
    }

    /// Feed an inbound datagram.
    public func receiveDatagram(
        _ bytes: UnsafeRawBufferPointer, now: UInt64
    ) throws {
        try MoQError.check(
            moq_session_on_datagram(
                raw,
                bytes.baseAddress?.assumingMemoryBound(to: UInt8.self),
                bytes.count, now))
    }

    // MARK: - Data Objects

    /// Open a subgroup data stream for writing objects.
    @discardableResult
    public func openSubgroup(
        for subscription: Subscription,
        groupID: UInt64,
        subgroupID: UInt64 = 0,
        publisherPriority: UInt8 = 128,
        endOfGroup: Bool = false,
        objectProperties: Bool = false,
        now: UInt64 = 0
    ) throws -> SubgroupHandle {
        var cfg = moq_subgroup_cfg_t()
        moq_subgroup_cfg_init(&cfg)
        cfg.group_id = groupID
        cfg.subgroup_id = subgroupID
        cfg.publisher_priority = publisherPriority
        cfg.end_of_group = endOfGroup
        cfg.object_properties = objectProperties

        var handle = moq_subgroup_handle_t(_opaque: 0)
        try MoQError.check(
            moq_session_open_subgroup(
                raw, subscription.raw, &cfg, now, &handle))
        return SubgroupHandle(handle)
    }

    /// Write a complete object to an open subgroup.
    /// The payload Buffer is retained by the session until the action
    /// is polled; the caller keeps its own reference.
    public func writeObject(
        to subgroup: SubgroupHandle,
        objectID: UInt64,
        payload: Buffer,
        now: UInt64 = 0
    ) throws {
        try MoQError.check(
            moq_session_write_object(
                raw, subgroup.raw, objectID, payload.raw, now))
    }

    /// Write an object with optional properties to an open subgroup.
    /// Both payload and properties Buffers are retained by the session
    /// until the actions are polled; the caller keeps its own references.
    public func writeObject(
        to subgroup: SubgroupHandle,
        _ config: ObjectConfiguration,
        now: UInt64 = 0
    ) throws {
        var cfg = moq_object_cfg_t()
        moq_object_cfg_init(&cfg)
        cfg.object_id = config.objectID
        cfg.payload = config.payload.raw
        cfg.properties = config.properties?.raw
        try MoQError.check(
            moq_session_write_object_ex(raw, subgroup.raw, &cfg, now))
    }

    /// Close a subgroup stream (sends FIN).
    public func closeSubgroup(
        _ subgroup: SubgroupHandle, now: UInt64 = 0
    ) throws {
        try MoQError.check(
            moq_session_close_subgroup(raw, subgroup.raw, now))
    }

    // MARK: - Subscribe

    /// Initiate a subscription to a track.
    @discardableResult
    public func subscribe(
        _ config: SubscribeConfiguration, now: UInt64 = 0
    ) throws -> Subscription {
        try withCTrackName(config.track) { ns, name in
            var cfg = moq_subscribe_cfg_t()
            moq_subscribe_cfg_init(&cfg)

            cfg.track_namespace = ns
            cfg.track_name = name

            switch config.filter {
            case .nextGroup:
                cfg.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP
            case .latestObject:
                cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT
            case .absoluteStart(let group, let object):
                cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START
                cfg.start_group = group
                cfg.start_object = object
            case .absoluteRange(let sg, let so, let eg):
                cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE
                cfg.start_group = sg
                cfg.start_object = so
                cfg.end_group = eg
            case .unknown(let v):
                cfg.filter = v
            }

            if let priority = config.subscriberPriority {
                cfg.has_subscriber_priority = true
                cfg.subscriber_priority = priority
            }
            cfg.group_order = config.groupOrder.cValue

            var handle = moq_subscription_t(_opaque: 0)
            try MoQError.check(
                moq_session_subscribe(raw, &cfg, now, &handle))
            return Subscription(handle)
        }
    }

    /// Accept an inbound subscription request.
    public func acceptSubscribe(
        _ subscription: Subscription,
        config: AcceptSubscribeConfiguration = AcceptSubscribeConfiguration(),
        now: UInt64 = 0
    ) throws {
        var cfg = moq_accept_subscribe_cfg_t()
        moq_accept_subscribe_cfg_init(&cfg)

        if let expires = config.expiresMs {
            cfg.has_expires = true
            cfg.expires_ms = expires
        }

        try MoQError.check(
            moq_session_accept_subscribe(raw, subscription.raw, &cfg, now))
    }

    // MARK: - Output Polling

    /// Drain pending actions via callback (low-allocation).
    public func drainActions(_ body: (Action) throws -> Void) rethrows {
        var buf = [moq_action_t](
            repeating: moq_action_t(), count: 16)
        while true {
            let n = moq_session_poll_actions(raw, &buf, 16)
            if n == 0 { break }
            for i in 0..<n {
                if let action = Action.from(&buf[i]) {
                    try body(action)
                } else {
                    // Unknown kind — already cleaned up by Action.from
                }
            }
        }
    }

    /// Convenience: collect all pending actions.
    public func pollActions() -> [Action] {
        var result: [Action] = []
        drainActions { result.append($0) }
        return result
    }

    /// Drain pending events via callback (low-allocation).
    public func drainEvents(_ body: (Event) throws -> Void) rethrows {
        var buf = [moq_event_t](
            repeating: moq_event_t(), count: 16)
        while true {
            let n = moq_session_poll_events(raw, &buf, 16)
            if n == 0 { break }
            for i in 0..<n {
                let event = Event.from(&buf[i])
                try body(event)
            }
        }
    }

    /// Convenience: collect all pending events.
    public func pollEvents() -> [Event] {
        var result: [Event] = []
        drainEvents { result.append($0) }
        return result
    }
}
