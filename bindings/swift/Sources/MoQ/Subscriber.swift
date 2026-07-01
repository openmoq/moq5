import CMoQCore
import Foundation

// MARK: - Subscribed Track Handle

/// Handle to a track subscription managed by a Subscriber.
/// Facade-owned; valid only while the owning Subscriber is alive.
public final class SubscribedTrack {
    internal let raw: OpaquePointer  // moq_sub_track_t*

    internal init(_ raw: OpaquePointer) {
        self.raw = raw
    }

    public enum State: Sendable, Hashable {
        case pending
        case active
        case error
        case done
        case unknown(Int32)
    }

    public var state: State {
        let s = moq_sub_track_get_state(raw)
        switch s {
        case MOQ_SUB_TRACK_PENDING: return .pending
        case MOQ_SUB_TRACK_ACTIVE:  return .active
        case MOQ_SUB_TRACK_ERROR:   return .error
        case MOQ_SUB_TRACK_DONE:    return .done
        default:                    return .unknown(Int32(s.rawValue))
        }
    }

    public var isActive: Bool {
        moq_sub_track_is_active(raw)
    }
}

// MARK: - Subscriber

/// High-level subscriber facade over a sans-I/O Session.
///
/// The Subscriber takes over event polling on the session. After
/// creating a Subscriber, do not call `pollEvents` or `drainEvents`
/// on the underlying Session. Action polling and byte feeding
/// remain the transport's responsibility.
public final class Subscriber {

    // MARK: - Configuration

    public struct Configuration: Sendable {
        public var maxTracks: UInt32
        public var maxObjects: UInt32

        public init(
            maxTracks: UInt32 = 64,
            maxObjects: UInt32 = 256
        ) {
            self.maxTracks = maxTracks
            self.maxObjects = maxObjects
        }
    }

    // MARK: - Storage

    internal let raw: OpaquePointer  // moq_subscriber_t*
    private let session: Session
    private var tracksByRaw: [OpaquePointer: SubscribedTrack] = [:]

    // MARK: - Lifecycle

    public init(
        session: Session,
        configuration: Configuration = Configuration()
    ) throws {
        self.session = session

        var cfg = moq_sub_cfg_t()
        moq_sub_cfg_init(&cfg)
        cfg.max_tracks = configuration.maxTracks
        cfg.max_objects = configuration.maxObjects

        var ptr: OpaquePointer?
        let alloc = moq_alloc_default()!
        try MoQError.check(moq_sub_create(session.raw, alloc, &cfg, &ptr))
        guard let p = ptr else { throw MoQError.internal }
        self.raw = p
    }

    deinit {
        moq_sub_destroy(raw)
    }

    // MARK: - Tick

    /// Process pending session events and dispatch subscription
    /// lifecycle changes.
    public func tick(now: UInt64) throws {
        try MoQError.check(moq_sub_tick(raw, now))
    }

    // MARK: - Subscribe

    /// Subscribe to a track on the peer. The facade supports live
    /// filters only (nextGroup, latestObject). For absolute filters
    /// use the raw `Session.subscribe` API instead.
    @discardableResult
    public func subscribe(
        track: TrackName,
        filter: SubscribeFilter = .nextGroup,
        subscriberPriority: UInt8? = nil,
        now: UInt64 = 0
    ) throws -> SubscribedTrack {
        let cFilter: moq_subscribe_filter_t
        switch filter {
        case .nextGroup:
            cFilter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP
        case .latestObject:
            cFilter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT
        case .absoluteStart, .absoluteRange, .unknown:
            throw MoQError.invalidArgument
        }

        return try withCTrackName(track) { ns, cName in
            var cfg = moq_sub_track_cfg_t()
            moq_sub_track_cfg_init(&cfg)
            cfg.track_namespace = ns
            cfg.track_name = cName
            cfg.filter = cFilter

            if let p = subscriberPriority {
                cfg.has_subscriber_priority = true
                cfg.subscriber_priority = p
            }

            var ptr: OpaquePointer?
            try MoQError.check(
                moq_sub_subscribe(raw, &cfg, now, &ptr))
            guard let p = ptr else { throw MoQError.internal }
            let subscribedTrack = SubscribedTrack(p)
            tracksByRaw[p] = subscribedTrack
            return subscribedTrack
        }
    }

    // MARK: - Object Polling

    /// Poll the next received object. Returns nil only for
    /// MOQ_DONE (empty queue). Throws on C errors or unmapped
    /// track pointers. Payload and properties ownership is
    /// transferred by nulling C pointers before cleanup.
    public func pollObject() throws -> FacadeReceivedObject? {
        var obj = moq_sub_object_t()
        let rc = moq_sub_poll_object(raw, &obj)
        if rc == MOQ_DONE { return nil }
        try MoQError.check(rc)

        guard let trackPtr = obj.track,
              let subscribedTrack = tracksByRaw[trackPtr] else {
            moq_sub_object_cleanup(&obj)
            throw MoQError.internal
        }

        let payload: Buffer?
        if let p = obj.payload {
            obj.payload = nil
            payload = Buffer(adopting: p)
        } else {
            payload = nil
        }

        let properties: Buffer?
        if let p = obj.properties {
            obj.properties = nil
            properties = Buffer(adopting: p)
        } else {
            properties = nil
        }

        moq_sub_object_cleanup(&obj)

        return FacadeReceivedObject(
            track: subscribedTrack,
            groupID: obj.group_id,
            subgroupID: obj.subgroup_id,
            objectID: obj.object_id,
            publisherPriority: obj.publisher_priority,
            status: ObjectStatus(obj.status),
            endOfGroup: obj.end_of_group,
            isDatagram: obj.datagram,
            payload: payload,
            properties: properties
        )
    }

    /// Drain all queued objects via callback.
    public func drainObjects(_ body: (FacadeReceivedObject) throws -> Void) throws {
        while let obj = try pollObject() {
            try body(obj)
        }
    }

    /// Convenience: collect all queued objects.
    public func pollAllObjects() throws -> [FacadeReceivedObject] {
        var result: [FacadeReceivedObject] = []
        try drainObjects { result.append($0) }
        return result
    }

    /// The underlying session, for action pumping and byte feeding.
    public var underlyingSession: Session { session }
}

// MARK: - Facade Received Object

/// An object received through the Subscriber facade.
public struct FacadeReceivedObject {
    public let track: SubscribedTrack
    public let groupID: UInt64
    public let subgroupID: UInt64
    public let objectID: UInt64
    public let publisherPriority: UInt8
    public let status: ObjectStatus
    public let endOfGroup: Bool
    public let isDatagram: Bool
    public let payload: Buffer?
    public let properties: Buffer?
}
