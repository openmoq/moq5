import CMoQCore
import Foundation

// Draft-22 updates of the namespace-scoped requests (section 9.5): a namespace
// announcement (AUTHORIZATION_TOKEN), a namespace subscription (also
// TRACK_NAMESPACE_PREFIX) and a SUBSCRIBE_TRACKS (also FORWARD). The requester
// sends an update; the responder answers each exactly once when it has opted
// in with `setDeferNamespaceUpdates(true)`, and refuses them otherwise. A
// refusal ends the request. Every configuration is copied for the call and
// every observation is copied out of the session's borrowed record.

// MARK: - Values

/// An authorization token as the session resolved it (or as a request
/// carries it): its type and its bytes.
public struct AuthorizationToken: Sendable, Hashable {
    public var type: UInt64
    public var value: Data

    public init(type: UInt64, value: Data) {
        self.type = type
        self.value = value
    }
}

/// The request a namespace update changes.
public enum NamespaceUpdateTarget: Sendable, Hashable {
    case announcement(Announcement)
    /// A namespace subscription, by its opaque handle value.
    case namespaceSubscription(handle: UInt64)
    case trackSubscription(TrackSubscription)
}

/// An update to send. A nil field keeps its value. `prefix` applies to the
/// two subscription kinds and `forward` to SUBSCRIBE_TRACKS only; a field the
/// request does not take throws `.invalidArgument` before anything is sent.
public struct NamespaceUpdateConfiguration: Sendable {
    public var prefix: Namespace?
    public var forward: Bool?
    public var authorizationTokens: [AuthorizationToken]

    public init() {
        self.prefix = nil
        self.forward = nil
        self.authorizationTokens = []
    }
}

/// One namespace update as observed: the selected fields only.
public struct NamespaceUpdateRecord: Sendable, Hashable {
    /// The update's own full-width Request ID, which its answer names.
    public let updateRequestID: UInt64
    public let prefix: Namespace?
    public let forward: Bool?
    /// The update's AUTHORIZATION_TOKEN resolved to at least one token (a
    /// DELETE only retires an alias and resolves to none).
    public let hasAuthorization: Bool
    public let tokens: [AuthorizationToken]

    internal init(_ r: moq_namespace_update_record_t) {
        updateRequestID = r.update_request_id
        prefix = r.flags & UInt32(MOQ_NAMESPACE_UPDATE_PREFIX) != 0
            ? Namespace.fromC(r.track_namespace_prefix) : nil
        forward = r.flags & UInt32(MOQ_NAMESPACE_UPDATE_FORWARD) != 0
            ? r.forward : nil
        hasAuthorization = r.flags & UInt32(MOQ_NAMESPACE_UPDATE_AUTHORIZATION) != 0
        var copied: [AuthorizationToken] = []
        if let t = r.tokens {
            for i in 0..<r.token_count {
                let v = t[i].token_value
                let bytes = v.data.map { Data(bytes: $0, count: v.len) } ?? Data()
                copied.append(AuthorizationToken(type: t[i].token_type, value: bytes))
            }
        }
        tokens = copied
    }
}

/// Responder side: the peer updated a request this side answers; the answer
/// is owed.
public struct NamespaceUpdateRequestInfo: Sendable, Hashable {
    public let target: NamespaceUpdateTarget
    public let update: NamespaceUpdateRecord
}

/// How an update concluded.
public enum NamespaceUpdateOutcome: Sendable, Hashable {
    /// The responder accepted it.
    case ok
    /// The responder refused it and ends the request.
    case error
    /// This side refused a peer update itself (a token failure,
    /// PREFIX_OVERLAP or an unservable filter) and ended the request.
    case refused
    case other(UInt32)

    internal init(_ v: UInt32) {
        switch v {
        case UInt32(MOQ_NAMESPACE_UPDATE_OUTCOME_OK):      self = .ok
        case UInt32(MOQ_NAMESPACE_UPDATE_OUTCOME_ERROR):   self = .error
        case UInt32(MOQ_NAMESPACE_UPDATE_OUTCOME_REFUSED): self = .refused
        default:                                           self = .other(v)
        }
    }
}

/// The conclusion of one update, reported once.
public struct NamespaceUpdateResultInfo: Sendable, Hashable {
    public let target: NamespaceUpdateTarget
    public let updateRequestID: UInt64
    public let outcome: NamespaceUpdateOutcome
    public let localRole: RequestRole
    /// `RequestErrorCode`; zero for `.ok`.
    public let errorCode: UInt64
    public let reason: String
    public let expiresMs: UInt64?
}

extension NamespaceUpdateResultInfo {
    internal init<R>(target: NamespaceUpdateTarget, _ d: R,
                     id: KeyPath<R, UInt64>, outcome: KeyPath<R, UInt32>,
                     role: KeyPath<R, UInt32>, code: KeyPath<R, UInt64>,
                     reason: KeyPath<R, moq_bytes_t>, hasExpires: KeyPath<R, Bool>,
                     expires: KeyPath<R, UInt64>) {
        let r = d[keyPath: reason]
        let text = r.data.flatMap {
            String(bytes: UnsafeBufferPointer(start: $0, count: r.len), encoding: .utf8)
        } ?? ""
        self.init(target: target, updateRequestID: d[keyPath: id],
                  outcome: NamespaceUpdateOutcome(d[keyPath: outcome]),
                  localRole: RequestRole(d[keyPath: role]),
                  errorCode: d[keyPath: code], reason: text,
                  expiresMs: d[keyPath: hasExpires] ? d[keyPath: expires] : nil)
    }
}

// MARK: - Session calls

extension Session {

    private func withNamespaceUpdate<R>(
        _ config: NamespaceUpdateConfiguration,
        _ body: (UnsafePointer<moq_namespace_update_cfg_t>) throws -> R
    ) throws -> R {
        // Every token's bytes in one buffer, so each C token can point into
        // it for the whole call.
        var blob: [UInt8] = []
        var spans: [(offset: Int, count: Int)] = []
        for t in config.authorizationTokens {
            spans.append((blob.count, t.value.count))
            blob.append(contentsOf: t.value)
        }
        return try blob.withUnsafeBufferPointer { bp in
            var cTokens: [moq_auth_token_t] = []
            cTokens.reserveCapacity(spans.count)
            for (i, s) in spans.enumerated() {
                cTokens.append(moq_auth_token_t(
                    token_type: config.authorizationTokens[i].type,
                    token_value: moq_bytes_t(
                        data: s.count > 0 ? bp.baseAddress.map { $0 + s.offset } : nil,
                        len: s.count)))
            }
            return try cTokens.withUnsafeBufferPointer { tp in
                // The exported unsized initializer covers only the frozen
                // layout; the sized one stamps this struct's full size.
                var cfg = moq_namespace_update_cfg_t()
                moq_namespace_update_cfg_init_sized(
                    &cfg, MemoryLayout<moq_namespace_update_cfg_t>.size)
                if let f = config.forward {
                    cfg.flags |= UInt32(MOQ_NAMESPACE_UPDATE_FORWARD)
                    cfg.forward = f
                }
                cfg.auth_tokens = tp.baseAddress
                cfg.auth_token_count = tp.count
                guard let prefix = config.prefix else { return try body(&cfg) }
                return try withCNamespace(prefix) { ns in
                    cfg.flags |= UInt32(MOQ_NAMESPACE_UPDATE_PREFIX)
                    cfg.track_namespace_prefix = ns
                    return try body(&cfg)
                }
            }
        }
    }

    /// Send an update of a request this side made. Returns the update's own
    /// Request ID, which its `.namespaceUpdateResult` names. Throws
    /// `.requestBlocked` while an earlier update of the request is unanswered.
    @discardableResult
    public func updateNamespace(
        _ target: NamespaceUpdateTarget, _ config: NamespaceUpdateConfiguration,
        now: UInt64 = 0
    ) throws -> UInt64 {
        try withNamespaceUpdate(config) { cfg in
            var id: UInt64 = 0
            switch target {
            case .announcement(let a):
                try MoQError.check(moq_session_update_namespace(raw, a.raw, cfg, now, &id))
            case .namespaceSubscription(let h):
                try MoQError.check(moq_session_update_ns_sub(
                    raw, moq_ns_sub_handle_t(_opaque: h), cfg, now, &id))
            case .trackSubscription(let t):
                try MoQError.check(moq_session_update_subscribe_tracks(
                    raw, t.raw, cfg, now, &id))
            }
            return id
        }
    }

    /// Leave the answer to every peer namespace update to the application
    /// (`.namespaceUpdateRequest`). Off, they are refused.
    public func setDeferNamespaceUpdates(_ enabled: Bool) throws {
        try MoQError.check(moq_session_set_defer_namespace_updates(raw, enabled))
    }

    /// Accept the owed update `updateRequestID` names.
    public func acceptNamespaceUpdate(
        _ target: NamespaceUpdateTarget, updateRequestID: UInt64, now: UInt64 = 0
    ) throws {
        var cfg = moq_accept_subscription_update_cfg_t()
        moq_accept_subscription_update_cfg_init(&cfg)
        cfg.update_request_id = updateRequestID
        switch target {
        case .announcement(let a):
            try MoQError.check(moq_session_accept_namespace_update(raw, a.raw, &cfg, now))
        case .namespaceSubscription(let h):
            try MoQError.check(moq_session_accept_ns_sub_update(
                raw, moq_ns_sub_handle_t(_opaque: h), &cfg, now))
        case .trackSubscription(let t):
            try MoQError.check(moq_session_accept_subscribe_tracks_update(
                raw, t.raw, &cfg, now))
        }
    }

    /// Refuse the owed update (`RequestErrorCode`, not REDIRECT); the
    /// request ends.
    public func rejectNamespaceUpdate(
        _ target: NamespaceUpdateTarget, updateRequestID: UInt64,
        errorCode: UInt64, reason: String = "", now: UInt64 = 0
    ) throws {
        let r = Array(reason.utf8)
        try r.withUnsafeBufferPointer { rp in
            var cfg = moq_reject_subscription_update_cfg_t()
            moq_reject_subscription_update_cfg_init(&cfg)
            cfg.update_request_id = updateRequestID
            cfg.error_code = errorCode
            cfg.reason = moq_bytes_t(data: rp.baseAddress, len: rp.count)
            switch target {
            case .announcement(let a):
                try MoQError.check(moq_session_reject_namespace_update(raw, a.raw, &cfg, now))
            case .namespaceSubscription(let h):
                try MoQError.check(moq_session_reject_ns_sub_update(
                    raw, moq_ns_sub_handle_t(_opaque: h), &cfg, now))
            case .trackSubscription(let t):
                try MoQError.check(moq_session_reject_subscribe_tracks_update(
                    raw, t.raw, &cfg, now))
            }
        }
    }

    /// The peer update whose answer is owed, copied out. Throws
    /// `.wrongState` when none is owed. Not an advancing call.
    public func owedNamespaceUpdate(_ target: NamespaceUpdateTarget) throws
        -> NamespaceUpdateRecord {
        var r = moq_namespace_update_record_t()
        let size = MemoryLayout<moq_namespace_update_record_t>.size
        switch target {
        case .announcement(let a):
            try MoQError.check(moq_session_namespace_owed_update(raw, a.raw, &r, size))
        case .namespaceSubscription(let h):
            try MoQError.check(moq_session_ns_sub_owed_update(
                raw, moq_ns_sub_handle_t(_opaque: h), &r, size))
        case .trackSubscription(let t):
            try MoQError.check(moq_session_subscribe_tracks_owed_update(
                raw, t.raw, &r, size))
        }
        return NamespaceUpdateRecord(r)
    }
}

// MARK: - Event decoding

extension ExtendedEvent {
    internal static func namespaceUpdate(_ e: moq_event_t) -> ExtendedEvent? {
        func record(_ p: UnsafePointer<moq_namespace_update_record_t>?) -> NamespaceUpdateRecord? {
            // The frozen v0 record: everything through token_count.
            let v0 = MemoryLayout<moq_namespace_update_record_t>.offset(
                of: \moq_namespace_update_record_t.token_count)! + MemoryLayout<Int>.size
            guard let p else { return nil }
            let raw = UnsafeRawPointer(p)
            guard raw.load(as: UInt32.self) >= UInt32(v0) else { return nil }
            var value = moq_namespace_update_record_t()
            withUnsafeMutableBytes(of: &value) {
                $0.baseAddress!.copyMemory(from: raw, byteCount: v0)
            }
            return NamespaceUpdateRecord(value)
        }
        switch e.kind {
        case MOQ_EVENT_NAMESPACE_UPDATE_REQUEST:
            guard Int(e.detail_size) >= MemoryLayout<moq_namespace_update_request_event_t>.size,
                  let u = record(e.u.namespace_update_request.update) else { return nil }
            return .namespaceUpdateRequest(NamespaceUpdateRequestInfo(
                target: .announcement(Announcement(e.u.namespace_update_request.ann)),
                update: u))
        case MOQ_EVENT_NS_SUB_UPDATE_REQUEST:
            guard Int(e.detail_size) >= MemoryLayout<moq_ns_sub_update_request_event_t>.size,
                  let u = record(e.u.ns_sub_update_request.update) else { return nil }
            return .namespaceUpdateRequest(NamespaceUpdateRequestInfo(
                target: .namespaceSubscription(handle: e.u.ns_sub_update_request.handle._opaque),
                update: u))
        case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST:
            guard Int(e.detail_size) >= MemoryLayout<moq_subscribe_tracks_update_request_event_t>.size,
                  let u = record(e.u.subscribe_tracks_update_request.update) else { return nil }
            return .namespaceUpdateRequest(NamespaceUpdateRequestInfo(
                target: .trackSubscription(TrackSubscription(e.u.subscribe_tracks_update_request.handle)),
                update: u))
        case MOQ_EVENT_NAMESPACE_UPDATE_RESULT:
            guard detailCovers(e.detail_size, \moq_namespace_update_result_event_t.expires_ms)
            else { return nil }
            let d = e.u.namespace_update_result
            return .namespaceUpdateResult(NamespaceUpdateResultInfo(
                target: .announcement(Announcement(d.ann)), d,
                id: \.update_request_id, outcome: \.outcome, role: \.local_role,
                code: \.error_code, reason: \.reason, hasExpires: \.has_expires,
                expires: \.expires_ms))
        case MOQ_EVENT_NS_SUB_UPDATE_RESULT:
            guard Int(e.detail_size) >= MemoryLayout<moq_ns_sub_update_result_event_t>.size
            else { return nil }
            let d = e.u.ns_sub_update_result
            return .namespaceUpdateResult(NamespaceUpdateResultInfo(
                target: .namespaceSubscription(handle: d.handle._opaque), d,
                id: \.update_request_id, outcome: \.outcome, role: \.local_role,
                code: \.error_code, reason: \.reason, hasExpires: \.has_expires,
                expires: \.expires_ms))
        case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT:
            guard Int(e.detail_size) >= MemoryLayout<moq_subscribe_tracks_update_result_event_t>.size
            else { return nil }
            let d = e.u.subscribe_tracks_update_result
            return .namespaceUpdateResult(NamespaceUpdateResultInfo(
                target: .trackSubscription(TrackSubscription(d.handle)), d,
                id: \.update_request_id, outcome: \.outcome, role: \.local_role,
                code: \.error_code, reason: \.reason, hasExpires: \.has_expires,
                expires: \.expires_ms))
        default:
            return nil
        }
    }
}
