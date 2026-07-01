import CMoQCore
import Foundation

// MARK: - Publisher Track Handle

/// Handle to a track registered with a Publisher. Facade-owned;
/// valid only while the owning Publisher is alive.
public final class PublisherTrack {
    internal let raw: OpaquePointer  // moq_pub_track_t*

    internal init(_ raw: OpaquePointer) {
        self.raw = raw
    }
}

// MARK: - Publisher

/// High-level publisher facade over a sans-I/O Session.
///
/// The Publisher takes over event polling on the session. After
/// creating a Publisher, do not call `pollEvents` or `drainEvents`
/// on the underlying Session. Action polling and byte feeding
/// remain the transport's responsibility.
public final class Publisher {

    // MARK: - Configuration

    public enum AcceptMode: Sendable {
        case rejectAll
        case acceptAll
    }

    public struct Configuration: Sendable {
        public var acceptMode: AcceptMode
        public var defaultPublisherPriority: UInt8

        public init(
            acceptMode: AcceptMode = .acceptAll,
            defaultPublisherPriority: UInt8 = 128
        ) {
            self.acceptMode = acceptMode
            self.defaultPublisherPriority = defaultPublisherPriority
        }
    }

    // MARK: - Storage

    internal let raw: OpaquePointer  // moq_publisher_t*
    private let session: Session

    // MARK: - Lifecycle

    public init(
        session: Session,
        configuration: Configuration = Configuration()
    ) throws {
        self.session = session

        var cfg = moq_pub_cfg_t()
        moq_pub_cfg_init(&cfg)

        switch configuration.acceptMode {
        case .rejectAll: cfg.accept_mode = MOQ_PUB_REJECT_ALL
        case .acceptAll: cfg.accept_mode = MOQ_PUB_ACCEPT_ALL
        }
        cfg.default_publisher_priority = configuration.defaultPublisherPriority

        var ptr: OpaquePointer?
        let alloc = moq_alloc_default()!
        try MoQError.check(moq_pub_create(session.raw, alloc, &cfg, &ptr))
        guard let p = ptr else { throw MoQError.internal }
        self.raw = p
    }

    deinit {
        moq_pub_destroy(raw)
    }

    // MARK: - Tick

    /// Process pending session events, dispatch subscribe requests,
    /// and fire any lifecycle callbacks.
    public func tick(now: UInt64) throws {
        try MoQError.check(moq_pub_tick(raw, now))
    }

    // MARK: - Track Management

    /// Register a track for publishing. Subscribers matching this
    /// namespace and name will be handled per the accept mode.
    @discardableResult
    public func addTrack(
        namespace: Namespace,
        name: String,
        advertiseNamespace: Bool = true,
        publisherPriority: UInt8? = nil,
        maxRetainedBytes: UInt64? = nil,
        now: UInt64 = 0
    ) throws -> PublisherTrack {
        let track = TrackName(namespace: namespace, name: name)
        return try withCTrackName(track) { ns, cName in
            var cfg = moq_pub_track_cfg_t()
            // Sized init: publisherPriority/maxRetainedBytes are appended
            // fields, so the cfg must advertise its full size for add_track
            // to honor them (pointer-only init stamps only the frozen prefix).
            moq_pub_track_cfg_init_sized(&cfg, MemoryLayout<moq_pub_track_cfg_t>.size)
            cfg.track_namespace = ns
            cfg.track_name = cName
            cfg.advertise_namespace = advertiseNamespace
            if let p = publisherPriority {
                cfg.has_publisher_priority = true
                cfg.publisher_priority = p
            }
            if let m = maxRetainedBytes {
                cfg.max_retained_bytes = m
            }

            var ptr: OpaquePointer?
            try MoQError.check(
                moq_pub_add_track(raw, &cfg, now, &ptr))
            guard let p = ptr else { throw MoQError.internal }
            return PublisherTrack(p)
        }
    }

    // MARK: - Retained Group (origin-local Joining-FETCH cache)

    /// Set or replace the retained catalog group for a track. The group
    /// (objects 0..N, ascending object_id) is an origin-local cache that
    /// answers an EXPLICIT Joining FETCH(offset 0); it is NOT pushed to plain
    /// SUBSCRIBE joiners. C retains payload/properties via incref; Swift keeps
    /// its own Buffer refs independently.
    public func setRetainedGroup(
        on track: PublisherTrack,
        _ config: RetainedGroupConfiguration
    ) throws {
        let cObjs = config.objects.map { obj -> moq_pub_retained_object_t in
            var c = moq_pub_retained_object_t()
            c.object_id = obj.objectID
            c.payload = obj.payload.raw
            c.properties = obj.properties?.raw
            c.end_of_group = obj.endOfGroup
            return c
        }
        try cObjs.withUnsafeBufferPointer { buf in
            var cfg = moq_pub_retained_group_cfg_t()
            moq_pub_retained_group_cfg_init(&cfg)
            cfg.group_id = config.groupID
            cfg.objects = buf.baseAddress
            cfg.object_count = buf.count
            try MoQError.check(
                moq_pub_set_retained_group(raw, track.raw, &cfg))
        }
    }

    /// Remove the retained group from a track. Safe to call when none is set.
    public func clearRetainedGroup(
        on track: PublisherTrack
    ) throws {
        try MoQError.check(
            moq_pub_clear_retained_group(raw, track.raw))
    }

    // MARK: - Object Writing

    /// Write a complete object to all subscribers of a track.
    /// The facade manages subgroup lifecycle internally.
    /// The payload Buffer is retained by the session until the
    /// action is polled; the caller keeps its own reference.
    public func writeObject(
        to track: PublisherTrack,
        groupID: UInt64,
        objectID: UInt64,
        payload: Buffer,
        now: UInt64 = 0
    ) throws {
        try MoQError.check(
            moq_pub_write_object(
                raw, track.raw, groupID, objectID, payload.raw, now))
    }

    /// Write an object with full configuration (properties, datagram,
    /// status, end-of-group). Payload and properties Buffers are
    /// retained by the session until actions are polled.
    public func writeObject(
        to track: PublisherTrack,
        _ config: PublishObjectConfiguration,
        now: UInt64 = 0
    ) throws {
        if case .unknown = config.status {
            throw MoQError.invalidArgument
        }

        var cfg = moq_pub_object_cfg_t()
        moq_pub_object_cfg_init(&cfg)
        cfg.group_id = config.groupID
        cfg.object_id = config.objectID
        cfg.payload = config.payload?.raw
        cfg.properties = config.properties?.raw
        cfg.datagram = config.datagram
        cfg.end_of_group = config.endOfGroup
        if let status = config.status {
            cfg.has_status = true
            cfg.status = status.cValue
        }
        try MoQError.check(
            moq_pub_write_object_ex(raw, track.raw, &cfg, now))
    }

    /// The underlying session, for action pumping and byte feeding.
    public var underlyingSession: Session { session }
}

// MARK: - Publish Object Configuration

/// Configuration for writing an object through the Publisher facade.
// Not Sendable: holds `Buffer` (non-atomic, shard-confined rcbuf).
public struct PublishObjectConfiguration {
    public var groupID: UInt64
    public var objectID: UInt64
    public var payload: Buffer?
    public var properties: Buffer?
    public var datagram: Bool
    public var endOfGroup: Bool
    public var status: ObjectStatus?

    /// Normal object with payload.
    public init(
        groupID: UInt64,
        objectID: UInt64,
        payload: Buffer,
        properties: Buffer? = nil,
        datagram: Bool = false,
        endOfGroup: Bool = false
    ) {
        self.groupID = groupID
        self.objectID = objectID
        self.payload = payload
        self.properties = properties
        self.datagram = datagram
        self.endOfGroup = endOfGroup
        self.status = nil
    }

    /// Status object (datagram only, no payload).
    public init(
        groupID: UInt64,
        objectID: UInt64,
        status: ObjectStatus,
        datagram: Bool = true
    ) {
        self.groupID = groupID
        self.objectID = objectID
        self.payload = nil
        self.properties = nil
        self.datagram = datagram
        self.endOfGroup = false
        self.status = status
    }
}

// MARK: - Retained Group Configuration

/// One object of a retained catalog group (object 0 is the independent base;
/// 1..N are deltaUpdate objects).
// Not Sendable: holds `Buffer` (non-atomic, shard-confined rcbuf).
public struct RetainedObject {
    public var objectID: UInt64
    public var payload: Buffer
    public var properties: Buffer?
    public var endOfGroup: Bool

    public init(
        objectID: UInt64,
        payload: Buffer,
        properties: Buffer? = nil,
        endOfGroup: Bool = false
    ) {
        self.objectID = objectID
        self.payload = payload
        self.properties = properties
        self.endOfGroup = endOfGroup
    }
}

/// Configuration for a retained catalog group answered via an explicit
/// Joining FETCH (origin-local cache; not relay-safe, not auto-pushed on
/// plain SUBSCRIBE).
public struct RetainedGroupConfiguration {
    public var groupID: UInt64
    public var objects: [RetainedObject]   // ascending object_id, 0..N

    public init(groupID: UInt64, objects: [RetainedObject]) {
        self.groupID = groupID
        self.objects = objects
    }
}
