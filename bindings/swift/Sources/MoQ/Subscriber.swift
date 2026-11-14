import CMoQCore
import Foundation

/// Identifier-only facade correlation. Polling retires the native slot, so a
/// later request may reuse this value; it is not a persistent unique identity.
public struct SubscriberStatusRequest: Sendable, Hashable {
    public let rawValue: UInt
    internal init(_ raw: OpaquePointer?) { rawValue = raw.map { UInt(bitPattern: $0) } ?? 0 }
}

/// Sized facade TRACK_STATUS observation, separate from existing Event cases.
public struct SubscriberStatusOutcome: Sendable {
    public enum Kind: Sendable, Hashable {
        case ok, error, terminated
        case other(UInt32)
    }
    public let kind: Kind
    public let request: SubscriberStatusRequest
    public let largest: (group: UInt64, object: UInt64)?
    public let expiresMs: UInt64?
    public let errorCode: UInt64
    public let canRetry: Bool
    public let retryAfterMs: UInt64
    public let cause: TrackStatusTerminalCause?
    public let localRole: RequestRole?
    public let streamErrorCode: UInt64

    internal static func fromNative(_ bytes: UnsafeRawBufferPointer) throws -> Self {
        typealias C = moq_sub_status_outcome_t
        guard bytes.count >= MemoryLayout<UInt32>.size else { throw MoQError.abiMismatch }
        let n = min(bytes.count, Int(bytes.loadUnaligned(as: UInt32.self)))
        guard detailCovers(UInt32(n), \C.stream_error_code) else { throw MoQError.abiMismatch }
        var c = C()
        withUnsafeMutableBytes(of: &c) { $0.copyBytes(from: bytes.prefix(min(n, $0.count))) }
        let kind: Kind
        switch c.kind {
        case UInt32(MOQ_SUB_STATUS_OUTCOME_OK): kind = .ok
        case UInt32(MOQ_SUB_STATUS_OUTCOME_ERROR): kind = .error
        case UInt32(MOQ_SUB_STATUS_OUTCOME_TERMINATED): kind = .terminated
        default: kind = .other(c.kind)
        }
        return Self(kind: kind, request: SubscriberStatusRequest(c.request),
            largest: kind == .ok && c.flags & UInt32(MOQ_SUB_STATUS_OUTCOME_HAS_LARGEST) != 0
                ? (c.largest_group, c.largest_object) : nil,
            expiresMs: kind == .ok && c.flags & UInt32(MOQ_SUB_STATUS_OUTCOME_HAS_EXPIRES) != 0
                ? c.expires_ms : nil,
            errorCode: kind == .error ? c.error_code : 0,
            canRetry: kind == .error && c.flags & UInt32(MOQ_SUB_STATUS_OUTCOME_CAN_RETRY) != 0,
            retryAfterMs: kind == .error ? c.retry_after_ms : 0,
            cause: kind == .terminated ? TrackStatusTerminalCause(c.cause) : nil,
            localRole: kind == .terminated ? RequestRole(c.local_role) : nil,
            streamErrorCode: kind == .terminated ? c.stream_error_code : 0)
    }
}

/// A value-only local delivery observation, never a peer error. Correlation
/// does not retain a native owner. A FETCH ID is only a reusable slot identifier,
/// not a pointer to dereference or a persistent unique operation identity.
/// Kept on the session executor, like the existing SubscribedTrack handles.
public struct SubscriberLocalFailure {
    public enum OwnerKind: Sendable, Hashable {
        case track, fetch, fill
        case other(UInt32)
    }
    public let ownerKind: OwnerKind
    public let localResult: Int32
    /// Only the existing wrapper for a known TRACK/FILL owner; never manufactured.
    public let track: SubscribedTrack?
    public let fetchID: UInt?
    public let fill: SubscriberFill?
    /// Present only for FILL; zero is a valid initiating request ID.
    public let initiatingRequestID: UInt64?

    internal static func fromNative(
        _ bytes: UnsafeRawBufferPointer, tracksByRaw: [OpaquePointer: SubscribedTrack]
    ) throws -> Self {
        typealias C = moq_sub_local_failure_t
        guard bytes.count >= MemoryLayout<UInt32>.size else { throw MoQError.abiMismatch }
        let n = min(bytes.count, Int(bytes.loadUnaligned(as: UInt32.self)))
        guard detailCovers(UInt32(n), \C.initiating_request_id) else { throw MoQError.abiMismatch }
        var c = C()
        withUnsafeMutableBytes(of: &c) { $0.copyBytes(from: bytes.prefix(min(n, $0.count))) }
        let kind: OwnerKind
        switch c.owner_kind {
        case UInt32(MOQ_SUB_LOCAL_FAILURE_TRACK): kind = .track
        case UInt32(MOQ_SUB_LOCAL_FAILURE_FETCH): kind = .fetch
        case UInt32(MOQ_SUB_LOCAL_FAILURE_FILL): kind = .fill
        default: kind = .other(c.owner_kind)
        }
        return Self(ownerKind: kind, localResult: c.local_result,
            track: kind == .track || kind == .fill ? c.track.flatMap { tracksByRaw[$0] } : nil,
            fetchID: kind == .fetch ? c.fetch.map { UInt(bitPattern: $0) } : nil,
            fill: kind == .fill && c.fill._opaque != 0 ? SubscriberFill(c.fill) : nil,
            initiatingRequestID: kind == .fill ? c.initiating_request_id : nil)
    }

    internal static func pollNative(
        _ subscriber: OpaquePointer?, tracksByRaw: [OpaquePointer: SubscribedTrack]
    ) throws -> Self? {
        var out = moq_sub_local_failure_t()
        let rc = moq_sub_poll_local_failure(subscriber, &out, MemoryLayout.size(ofValue: out))
        if rc == MOQ_DONE { return nil }
        try MoQError.check(rc)
        return try withUnsafeBytes(of: &out) { try fromNative($0, tracksByRaw: tracksByRaw) }
    }
}

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

/// An owned snapshot of a track's current state and immutable initial ACK.
/// `responseSeen` distinguishes a pending response from an accepted empty
/// parameter/property block. Optional scalars preserve explicit zero values.
public struct SubscribedTrackResult: Sendable, Hashable {
    public let state: SubscribedTrack.State
    public let responseSeen: Bool
    public let largest: (group: UInt64, object: UInt64)?
    public let expiresMs: UInt64?
    public let trackProperties: Data

    public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.state == rhs.state && lhs.responseSeen == rhs.responseSeen &&
        lhs.largest?.group == rhs.largest?.group &&
        lhs.largest?.object == rhs.largest?.object &&
        lhs.expiresMs == rhs.expiresMs && lhs.trackProperties == rhs.trackProperties
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(state)
        hasher.combine(responseSeen)
        hasher.combine(largest?.group)
        hasher.combine(largest?.object)
        hasher.combine(expiresMs)
        hasher.combine(trackProperties)
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
        /// Fill request slots and the fill-item queue depth; 0 selects the
        /// defaults. Reserved only on a draft with fills.
        public var maxFills: UInt32
        public var maxFillItems: UInt32

        public init(
            maxTracks: UInt32 = 64,
            maxObjects: UInt32 = 256
        ) {
            self.maxTracks = maxTracks
            self.maxObjects = maxObjects
            self.maxFills = 0
            self.maxFillItems = 0
        }

        /// The C configuration the facade is created with.
        internal var cValue: moq_sub_cfg_t {
            var cfg = moq_sub_cfg_t()
            // Sized init: max_fills / max_fill_items are past the original
            // layout.
            moq_sub_cfg_init_sized(&cfg, MemoryLayout<moq_sub_cfg_t>.size)
            cfg.max_tracks = maxTracks
            cfg.max_objects = maxObjects
            cfg.max_fills = maxFills
            cfg.max_fill_items = maxFillItems
            return cfg
        }
    }

    // MARK: - Storage

    internal let raw: OpaquePointer  // moq_subscriber_t*
    private let session: Session
    private var tracksByRaw: [OpaquePointer: SubscribedTrack] = [:]

    /// The track object for a facade track this wrapper just created.
    internal func adoptTrack(_ p: OpaquePointer) -> SubscribedTrack {
        let track = SubscribedTrack(p)
        tracksByRaw[p] = track
        return track
    }

    // MARK: - Lifecycle

    public init(
        session: Session,
        configuration: Configuration = Configuration()
    ) throws {
        self.session = session

        var cfg = configuration.cValue

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

    /// Issue a facade TRACK_STATUS request. Results occupy the bounded native
    /// ring until pollStatusOutcome consumes them. No separate Swift queue.
    public func trackStatus(track: TrackName, now: UInt64 = 0) throws -> SubscriberStatusRequest {
        try withCTrackName(track) { namespace, name in
            var cfg = moq_sub_status_cfg_t()
            moq_sub_status_cfg_init(&cfg)
            cfg.track_namespace = namespace
            cfg.track_name = name
            var request: OpaquePointer?
            try MoQError.check(moq_sub_track_status(raw, &cfg, now, &request))
            guard request != nil else { throw MoQError.internal }
            return SubscriberStatusRequest(request)
        }
    }

    /// Consumes OK, real peer ERROR, or typed termination. nil means empty;
    /// native failures and incomplete scalar coverage throw.
    public func pollStatusOutcome() throws -> SubscriberStatusOutcome? {
        var out = moq_sub_status_outcome_t()
        let rc = moq_sub_poll_status_outcome(raw, &out, MemoryLayout.size(ofValue: out))
        if rc == MOQ_DONE { return nil }
        try MoQError.check(rc)
        return try withUnsafeBytes(of: &out) { try SubscriberStatusOutcome.fromNative($0) }
    }

    /// Consume one local notice on the session executor, even after a terminal
    /// item. nil means empty; native errors and incomplete v0 coverage throw.
    /// Non-advancing, with no extra queue or native references. Polling may
    /// finish FETCH/FILL retirement immediately; correlation does not authorize
    /// dereferencing a retired owner, even while this Swift value is alive.
    public func pollLocalFailure() throws -> SubscriberLocalFailure? {
        try SubscriberLocalFailure.pollNative(raw, tracksByRaw: tracksByRaw)
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
        try subscribe(track: track, filter: filter, subscriberPriority: subscriberPriority,
                      location: nil, fill: nil, now: now)
    }

    /// Subscribe with the filter in its semantic form (`location`, which
    /// replaces `filter`) and/or a fill carried on the SUBSCRIBE, whose
    /// items arrive through `pollFill()` and whose handle is
    /// `fill(of:)`. A form or a fill the draft cannot carry throws
    /// `.unsupported` before anything is sent.
    @discardableResult
    public func subscribe(
        track: TrackName,
        filter: SubscribeFilter = .nextGroup,
        subscriberPriority: UInt8? = nil,
        location: LocationFilter?,
        fill: FillConfiguration?,
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
            // Sized init: stamps the full size so appended fields are read.
            moq_sub_track_cfg_init_sized(&cfg, MemoryLayout<moq_sub_track_cfg_t>.size)
            cfg.track_namespace = ns
            cfg.track_name = cName
            cfg.filter = cFilter

            if let p = subscriberPriority {
                cfg.has_subscriber_priority = true
                cfg.subscriber_priority = p
            }

            var loc = moq_location_filter_cfg_t()
            var fillLoc = moq_location_filter_cfg_t()
            var cfill = moq_fill_cfg_t()
            var ptr: OpaquePointer?
            try withUnsafeMutablePointer(to: &loc) { locPtr in
                try withUnsafeMutablePointer(to: &fillLoc) { fillLocPtr in
                    try withUnsafeMutablePointer(to: &cfill) { fillPtr in
                        if let l = location {
                            l.fill(locPtr)
                            cfg.location = UnsafePointer(locPtr)
                        }
                        if let f = fill {
                            f.fill(fillPtr, location: fillLocPtr)
                            cfg.fill = UnsafePointer(fillPtr)
                        }
                        try MoQError.check(
                            moq_sub_subscribe(raw, &cfg, now, &ptr))
                    }
                }
            }
            guard let p = ptr else { throw MoQError.internal }
            let subscribedTrack = SubscribedTrack(p)
            tracksByRaw[p] = subscribedTrack
            return subscribedTrack
        }
    }

    /// Observe a track owned by this Subscriber without pumping events or
    /// changing its lifecycle. Call on the session executor; original ACK
    /// properties are copied synchronously and the result may outlive both
    /// Subscriber and Session. Native query errors, including a retained ACK
    /// copy failure, throw rather than returning an empty successful result.
    public func trackResult(_ track: SubscribedTrack) throws -> SubscribedTrackResult {
        var result = moq_sub_track_result_t()
        try MoQError.check(moq_sub_get_track_result(
            raw, track.raw, &result, MemoryLayout<moq_sub_track_result_t>.size))
        guard detailCovers(result.struct_size, \moq_sub_track_result_t.track_properties) else {
            throw MoQError.abiMismatch
        }
        let state: SubscribedTrack.State
        switch result.state {
        case MOQ_SUB_TRACK_PENDING.rawValue: state = .pending
        case MOQ_SUB_TRACK_ACTIVE.rawValue: state = .active
        case MOQ_SUB_TRACK_ERROR.rawValue: state = .error
        case MOQ_SUB_TRACK_DONE.rawValue: state = .done
        default: state = .unknown(Int32(bitPattern: result.state))
        }
        let properties: Data
        if result.track_properties.len == 0 {
            properties = Data()
        } else {
            guard let bytes = result.track_properties.data else { throw MoQError.internal }
            properties = Data(bytes: bytes, count: result.track_properties.len)
        }
        return SubscribedTrackResult(
            state: state,
            responseSeen: result.flags & MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN != 0,
            largest: result.flags & MOQ_SUB_TRACK_RESULT_HAS_LARGEST != 0
                ? (result.largest_group, result.largest_object) : nil,
            expiresMs: result.flags & MOQ_SUB_TRACK_RESULT_HAS_EXPIRES != 0 ? result.expires_ms : nil,
            trackProperties: properties)
    }

    // MARK: - Fills (draft-22 section 3.4)

    /// The newest fill requested on `track` whose slot has not been released.
    /// A local notice or unsettled native retirement can keep it present after
    /// terminal polling. Read right after the request for its handle. nil when none.
    public func fill(of track: SubscribedTrack) -> SubscriberFill? {
        let h = moq_sub_track_fill(track.raw)
        return h._opaque != 0 ? SubscriberFill(h) : nil
    }

    /// Request a fill on an established track (an update carrying
    /// FILL_PARAMETERS). Its handle is then `fill(of:)`.
    public func requestFill(
        on track: SubscribedTrack, _ fill: FillConfiguration, now: UInt64 = 0
    ) throws {
        var ucfg = moq_sub_update_cfg_t()
        moq_sub_update_cfg_init_sized(&ucfg, MemoryLayout<moq_sub_update_cfg_t>.size)
        var cfill = moq_fill_cfg_t()
        var loc = moq_location_filter_cfg_t()
        try withUnsafeMutablePointer(to: &loc) { locPtr in
            try withUnsafeMutablePointer(to: &cfill) { fillPtr in
                fill.fill(fillPtr, location: locPtr)
                ucfg.fill = UnsafePointer(fillPtr)
                try MoQError.check(moq_sub_update_subscription(raw, track.raw, &ucfg, now))
            }
        }
    }

    /// Stop one fill without touching the subscription; its terminal item
    /// (`.localCancelled`) still arrives through `pollFill()`.
    public func cancelFill(_ fill: SubscriberFill, now: UInt64 = 0) throws {
        try MoQError.check(moq_sub_cancel_fill(raw, fill.raw, now))
    }

    /// Poll the next fill item; nil when none is queued. After a local failure,
    /// retirement also waits for its notice, accepted items and native retirement.
    /// Poll local failures too; the handle is stale only after slot release.
    public func pollFill() throws -> SubscriberFillItem? {
        var item = moq_sub_fill_item_t()
        let rc = moq_sub_poll_fill(raw, &item, MemoryLayout<moq_sub_fill_item_t>.size)
        if rc == MOQ_DONE { return nil }
        try MoQError.check(rc)
        defer { moq_sub_fill_item_cleanup(&item) }
        guard let trackPtr = item.track,
              let subscribedTrack = tracksByRaw[trackPtr] else {
            throw MoQError.internal
        }
        let fill = SubscriberFill(item.request)
        let kind: SubscriberFillItem.Kind
        switch item.kind {
        case MOQ_SUB_FILL_CREATED.rawValue:
            kind = .created
        case MOQ_SUB_FILL_OBJECT.rawValue:
            let payload = item.u.object.payload.map { Buffer(adopting: $0) }
            let properties = item.u.object.properties.map { Buffer(adopting: $0) }
            item.u.object.payload = nil
            item.u.object.properties = nil
            let o = item.u.object
            kind = .object(groupID: o.group_id, subgroupID: o.subgroup_id,
                           objectID: o.object_id, publisherPriority: o.publisher_priority,
                           isDatagram: o.datagram, payload: payload,
                           properties: properties)
        case MOQ_SUB_FILL_GAP.rawValue:
            let g = item.u.gap
            kind = .gap(kind: FetchRangeKind(g.range_kind),
                        groupID: g.group_id, objectID: g.object_id)
        case MOQ_SUB_FILL_TERMINAL.rawValue:
            let t = item.u.terminal
            kind = .terminal(reason: FillTerminalInfo.Reason(t.reason),
                             domain: FillTerminalInfo.Domain(t.error_domain),
                             errorCode: t.error_code)
        default:
            throw MoQError.internal
        }
        return SubscriberFillItem(fill: fill, track: subscribedTrack,
                                  initiatingRequestID: item.initiating_request_id, kind: kind)
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

// MARK: - Facade Fills

/// A fill meaningful only to its issuing Subscriber. Stale once its slot is
/// released; local notices and native retirement can pin it after terminal
/// polling. Unique within one Subscriber and never reused by it; a fill from
/// another Subscriber, or from a destroyed one, is refused by its instance tag
/// as any other handle is (probabilistic, with a core handle's odds). Do not
/// persist or interchange tokens across Subscribers, library copies,
/// unload/reload or processes. Issuance does not make a Subscriber thread-safe.
public struct SubscriberFill: Sendable, Hashable {
    internal let raw: moq_sub_fill_t

    internal init(_ raw: moq_sub_fill_t) { self.raw = raw }

    /// The handle's 64-bit value (zero is never a valid handle).
    public var value: UInt64 { raw._opaque }

    public static func == (lhs: SubscriberFill, rhs: SubscriberFill) -> Bool {
        lhs.raw._opaque == rhs.raw._opaque
    }

    public func hash(into hasher: inout Hasher) { hasher.combine(raw._opaque) }
}

/// One item of a fill polled from the Subscriber. Object buffers are owned by
/// the value and survive track release and Subscriber destruction. The track
/// is borrowed until track release or Subscriber destruction; keeping this
/// value or its SubscribedTrack wrapper does not extend the native lifetime.
// Not Sendable: holds `Buffer` (non-atomic, shard-confined rcbuf).
public struct SubscriberFillItem {
    public enum Kind {
        /// Authoritative child creation, independent of the latest-fill query.
        case created
        case object(groupID: UInt64, subgroupID: UInt64, objectID: UInt64,
                    publisherPriority: UInt8, isDatagram: Bool,
                    payload: Buffer?, properties: Buffer?)
        case gap(kind: FetchRangeKind, groupID: UInt64, objectID: UInt64)
        /// Exactly once per fill; the code is meaningful when the domain is
        /// not `.none` (zero is then a valid code).
        case terminal(reason: FillTerminalInfo.Reason,
                      domain: FillTerminalInfo.Domain, errorCode: UInt64)
    }

    public let fill: SubscriberFill
    public let track: SubscribedTrack
    /// Present on every kind; zero is a valid initiating request ID.
    public let initiatingRequestID: UInt64
    public let kind: Kind
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
