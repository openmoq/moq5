import CMoQCore
import Foundation

/// Owned callback snapshot. Announcement termination is not a peer REQUEST_ERROR.
public struct PublisherNamespaceTerminalObservation: Sendable, Hashable {
    public enum Kind: Sendable, Hashable {
        case rejected, cancelled, terminated
        case other(UInt32)
    }
    public let kind: Kind
    public let namespace: Namespace
    public let errorCode: UInt64
    public let canRetry: Bool
    public let retryAfterMs: UInt64
    public let reason: Data
    public let cause: NamespaceTerminalCause?
    public let localRole: RequestRole?
    public let streamErrorCode: UInt64

    internal static func fromNative(_ bytes: UnsafeRawBufferPointer) throws -> Self {
        typealias C = moq_pub_namespace_terminal_info_t
        guard bytes.count >= MemoryLayout<UInt32>.size else { throw MoQError.abiMismatch }
        let n = min(bytes.count, Int(bytes.loadUnaligned(as: UInt32.self)))
        let oldExtent = MemoryLayout<C>.offset(of: \.cause)!
        guard n >= oldExtent else { throw MoQError.abiMismatch }
        var c = C()
        withUnsafeMutableBytes(of: &c) { $0.copyBytes(from: bytes.prefix(min(n, $0.count))) }
        let kind: Kind
        switch c.kind {
        case MOQ_PUB_NAMESPACE_REJECTED: kind = .rejected
        case MOQ_PUB_NAMESPACE_CANCELLED: kind = .cancelled
        case MOQ_PUB_NAMESPACE_TERMINATED:
            guard detailCovers(UInt32(n), \C.stream_error_code) else { throw MoQError.abiMismatch }
            kind = .terminated
        default: kind = .other(UInt32(c.kind.rawValue))
        }
        let terminated = kind == .terminated
        return Self(kind: kind, namespace: Namespace.fromC(c.namespace_),
            errorCode: terminated ? 0 : c.error_code,
            canRetry: !terminated && c.can_retry,
            retryAfterMs: terminated ? 0 : c.retry_after_ms,
            reason: !terminated && c.reason.len > 0 && c.reason.data != nil
                ? Data(bytes: c.reason.data!, count: c.reason.len) : Data(),
            cause: terminated ? NamespaceTerminalCause(c.cause) : nil,
            localRole: terminated ? RequestRole(c.local_role) : nil,
            streamErrorCode: terminated ? c.stream_error_code : 0)
    }
}

/// A subscriber asked this Original Publisher for a new group on a track that
/// advertises DYNAMIC_GROUPS, with a value that warrants one (draft-22
/// section 9.20.19): the value it sent (0: it knows no group) and the track's
/// current group, when it has published one. Start a new group from the
/// application's loop, not from the callback.
public struct PublisherNewGroupRequest: Sendable, Hashable {
    public let requestedGroup: UInt64
    public let currentGroup: UInt64?

    internal static func fromNative(_ bytes: UnsafeRawBufferPointer) throws -> Self {
        typealias C = moq_pub_new_group_request_info_t
        guard bytes.count >= MemoryLayout<UInt32>.size, let base = bytes.baseAddress else {
            throw MoQError.abiMismatch
        }
        let stamped = Int(base.loadUnaligned(as: UInt32.self))
        let n = min(bytes.count, stamped)
        guard n >= MemoryLayout<C>.offset(of: \C.current_group)! + MemoryLayout<UInt64>.size else {
            throw MoQError.abiMismatch
        }
        var c = C()
        withUnsafeMutableBytes(of: &c) { $0.copyMemory(from: UnsafeRawBufferPointer(
            start: base, count: min(n, MemoryLayout<C>.size))) }
        return Self(requestedGroup: c.requested_group,
                    currentGroup: c.has_current_group ? c.current_group : nil)
    }
}

private final class PublisherObservations {
    var goaway: ((Data) -> Void)?
    var namespaceTerminal: ((Result<PublisherNamespaceTerminalObservation, MoQError>) -> Void)?
    var newGroupRequest: ((PublisherTrack, Result<PublisherNewGroupRequest, MoQError>) -> Void)?
}

// MARK: - Publisher Track Handle

/// Handle to a track registered with a Publisher. Facade-owned;
/// valid only while the owning Publisher is alive. Two handles are equal when
/// they name the same track.
public final class PublisherTrack: Hashable {
    internal let raw: OpaquePointer  // moq_pub_track_t*

    internal init(_ raw: OpaquePointer) {
        self.raw = raw
    }

    public static func == (lhs: PublisherTrack, rhs: PublisherTrack) -> Bool {
        lhs.raw == rhs.raw
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(raw)
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
        /// Fill requests the facade can hold in flight at once; 0 selects
        /// 16. Never fewer than the session's fill pool. Reserved only on a
        /// draft with fills.
        public var maxFills: UInt32

        public init(
            acceptMode: AcceptMode = .acceptAll,
            defaultPublisherPriority: UInt8 = 128
        ) {
            self.acceptMode = acceptMode
            self.defaultPublisherPriority = defaultPublisherPriority
            self.maxFills = 0
        }

        /// The C configuration the facade is created with.
        internal var cValue: moq_pub_cfg_t {
            var cfg = moq_pub_cfg_t()
            // Sized init: max_fills is past the original layout.
            moq_pub_cfg_init_sized(&cfg, MemoryLayout<moq_pub_cfg_t>.size)
            switch acceptMode {
            case .rejectAll: cfg.accept_mode = MOQ_PUB_REJECT_ALL
            case .acceptAll: cfg.accept_mode = MOQ_PUB_ACCEPT_ALL
            }
            cfg.default_publisher_priority = defaultPublisherPriority
            cfg.max_fills = maxFills
            return cfg
        }
    }

    // MARK: - Storage

    internal let raw: OpaquePointer  // moq_publisher_t*
    private let session: Session
    private let observations: PublisherObservations

    /// Invoked synchronously on the session executor, once per namespace.
    /// Do not reenter libmoq from the callback. The snapshot owns its bytes;
    /// incomplete producer records report abiMismatch, never invented fields.
    public var onNamespaceTerminal: ((Result<PublisherNamespaceTerminalObservation, MoQError>) -> Void)? {
        get { observations.namespaceTerminal }
        set { observations.namespaceTerminal = newValue }
    }

    /// Invoked synchronously on the session executor when a subscriber asks
    /// for a new group on a track added with `dynamicGroups`. Do not reenter
    /// libmoq from the callback.
    public var onNewGroupRequest: ((PublisherTrack, Result<PublisherNewGroupRequest, MoQError>) -> Void)? {
        get { observations.newGroupRequest }
        set { observations.newGroupRequest = newValue }
    }

    /// Invoked synchronously on the session executor on the peer's
    /// session-wide GOAWAY, with its New Session URI (empty: reconnect to the
    /// current URI). Do not reenter libmoq from the callback.
    public var onGoaway: ((Data) -> Void)? {
        get { observations.goaway }
        set { observations.goaway = newValue }
    }

    // MARK: - Lifecycle

    public init(
        session: Session,
        configuration: Configuration = Configuration()
    ) throws {
        self.session = session
        let observations = PublisherObservations()
        self.observations = observations

        var cfg = configuration.cValue
        cfg.callbacks.ctx = Unmanaged.passUnretained(observations).toOpaque()
        cfg.callbacks.on_namespace_terminal = { ctx, info in
            guard let ctx, let info else { return }
            let observer = Unmanaged<PublisherObservations>.fromOpaque(ctx).takeUnretainedValue()
            guard let callback = observer.namespaceTerminal else { return }
            do {
                let bytes = UnsafeRawBufferPointer(start: info, count: Int(info.pointee.struct_size))
                callback(.success(try PublisherNamespaceTerminalObservation.fromNative(bytes)))
            } catch let error as MoQError {
                callback(.failure(error))
            } catch {
                callback(.failure(.internal))
            }
        }

        cfg.on_new_group_request_ctx = Unmanaged.passUnretained(observations).toOpaque()
        cfg.on_new_group_request = { ctx, track, info in
            guard let ctx, let track, let info else { return }
            let observer = Unmanaged<PublisherObservations>.fromOpaque(ctx).takeUnretainedValue()
            guard let callback = observer.newGroupRequest else { return }
            let bytes = UnsafeRawBufferPointer(start: info, count: Int(info.pointee.struct_size))
            do {
                callback(PublisherTrack(track), .success(try PublisherNewGroupRequest.fromNative(bytes)))
            } catch let error as MoQError {
                callback(PublisherTrack(track), .failure(error))
            } catch {
                callback(PublisherTrack(track), .failure(.internal))
            }
        }

        cfg.on_goaway = { ctx, uri in
            guard let ctx else { return }
            let observer = Unmanaged<PublisherObservations>.fromOpaque(ctx).takeUnretainedValue()
            guard let callback = observer.goaway else { return }
            callback(uri.data.map { Data(bytes: $0, count: uri.len) } ?? Data())
        }

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

    /// Process one bounded FIFO event pass and retry owned work before and
    /// after it, firing lifecycle callbacks synchronously. This is the
    /// session's sole event consumer; do not also poll its events directly.
    /// On every draft, `MoQError.wouldBlock` means staged work remains.
    /// Drain transport actions and retry in driver rounds; yield when nothing
    /// progresses instead of spinning until tick succeeds.
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
        trackProperties: Data = Data(),
        dynamicGroups: Bool = false,
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
            // The track's Track Properties ride every answer for it;
            // dynamicGroups adds DYNAMIC_GROUPS = 1 in the session's encoding.
            cfg.dynamic_groups = dynamicGroups

            var ptr: OpaquePointer?
            try withCBytes(trackProperties) { props in
                cfg.track_properties = props
                try MoQError.check(
                    moq_pub_add_track(raw, &cfg, now, &ptr))
            }
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
    /// Ownership: on success the payload Buffer is retained by the session
    /// until the action is polled. On `wouldBlock` the facade RETAINS a
    /// payload/properties snapshot until the operation completes, and the
    /// retry must be byte-identical -- a fresh Buffer with the same bytes
    /// resumes, changed bytes throw `wrongState`. From Swift the only ways
    /// out of a pending operation are completing that retry or tearing the
    /// publisher/session down (the C tier's per-track abandonment,
    /// moq_pub_reset_group, is not yet surfaced here -- a known binding
    /// gap). The caller keeps its own reference.
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
    /// status, end-of-group). Ownership: on success the payload/properties
    /// Buffers are retained by the session until actions are polled; on
    /// `wouldBlock` the facade retains a snapshot until the operation
    /// completes, and the retry must be byte-identical (fresh Buffer with
    /// same bytes resumes; changed bytes throw `wrongState`). Completing
    /// the retry or tearing the publisher/session down are the only Swift-
    /// visible ways out of a pending operation (per-track abandonment is a
    /// known binding gap).
    public func writeObject(
        to track: PublisherTrack,
        _ config: PublishObjectConfiguration,
        now: UInt64 = 0
    ) throws {
        if case .unknown = config.status {
            throw MoQError.invalidArgument
        }

        var cfg = moq_pub_object_cfg_t()
        /* Sized init: end_of_group below is an appended tail field the
         * frozen pointer-init deliberately leaves disabled. */
        moq_pub_object_cfg_init_sized(&cfg,
            MemoryLayout<moq_pub_object_cfg_t>.size)
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
