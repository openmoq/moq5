import CMoQCore
import Foundation

/// Session events produced by the protocol state machine.
// Not Sendable: cases carry `Buffer` (non-atomic, shard-confined rcbuf).
public enum Event: Equatable {
    case setupComplete
    case sessionClosed(code: UInt64)
    case subscribeRequest(SubscribeRequest)
    case subscribeOk(SubscribeOkInfo)
    case subscribeError(SubscribeErrorInfo)
    case namespacePublished(NamespacePublishedInfo)
    case namespaceAccepted(Announcement)
    case namespaceRejected(NamespaceRejectedInfo)
    case namespaceDone(Announcement)
    case objectReceived(ReceivedObject)
    case subgroupFinished(SubgroupFinishedInfo)
    case unknown(UInt32)
}

/// Detail for ``Event/subgroupFinished(_:)``: a subgroup stream reached a
/// graceful FIN after a usable subgroup header was parsed. Not emitted for
/// FETCH streams, streams reset via RESET_STREAM/STOP_SENDING, or streams that
/// FIN before a header parsed. Carries the subscriber-side identity (mirroring
/// ``ReceivedObject``); the publisher-initiated variant is not surfaced here.
public struct SubgroupFinishedInfo: Sendable, Equatable, Hashable {
    public let subscription: Subscription
    public let groupID: UInt64
    public let subgroupID: UInt64
    /// The subgroup header's END_OF_GROUP bit.
    public let endOfGroup: Bool
}

extension Event {
    /// Convert a C event to Swift. Cleans up the C event exactly once.
    internal static func from(_ cEvent: inout moq_event_t) -> Event {
        let kind = cEvent.kind
        let result: Event

        switch kind {
        case MOQ_EVENT_SETUP_COMPLETE:
            result = .setupComplete

        case MOQ_EVENT_SESSION_CLOSED:
            result = .sessionClosed(code: cEvent.u.closed.code)

        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            let sr = cEvent.u.subscribe_request
            let track = TrackName.fromC(
                namespace: sr.track_namespace, name: sr.track_name)
            let filter = SubscribeFilter.fromC(
                sr.filter,
                startGroup: sr.start_group,
                startObject: sr.start_object,
                endGroup: sr.end_group)
            result = .subscribeRequest(SubscribeRequest(
                subscription: Subscription(sr.sub),
                track: track,
                filter: filter,
                subscriberPriority: sr.subscriber_priority,
                groupOrder: GroupOrder(sr.group_order)
            ))

        case MOQ_EVENT_SUBSCRIBE_OK:
            let so = cEvent.u.subscribe_ok
            result = .subscribeOk(SubscribeOkInfo(
                subscription: Subscription(so.sub),
                expiresMs: so.has_expires ? so.expires_ms : nil
            ))

        case MOQ_EVENT_SUBSCRIBE_ERROR:
            let se = cEvent.u.subscribe_error
            let reason: String
            if let data = se.reason.data, se.reason.len > 0 {
                reason = String(
                    bytes: UnsafeBufferPointer(start: data, count: se.reason.len),
                    encoding: .utf8
                ) ?? ""
            } else {
                reason = ""
            }
            result = .subscribeError(SubscribeErrorInfo(
                subscription: Subscription(se.sub),
                errorCode: se.error_code,
                reason: reason,
                canRetry: se.can_retry
            ))

        case MOQ_EVENT_NAMESPACE_PUBLISHED:
            let np = cEvent.u.namespace_published
            let namespace = Namespace.fromC(np.track_namespace)
            result = .namespacePublished(NamespacePublishedInfo(
                announcement: Announcement(np.ann),
                namespace: namespace
            ))

        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            result = .namespaceAccepted(
                Announcement(cEvent.u.namespace_accepted.ann))

        case MOQ_EVENT_NAMESPACE_REJECTED:
            let nr = cEvent.u.namespace_rejected
            let reason: String
            if let data = nr.reason.data, nr.reason.len > 0 {
                reason = String(
                    bytes: UnsafeBufferPointer(start: data, count: nr.reason.len),
                    encoding: .utf8
                ) ?? ""
            } else {
                reason = ""
            }
            result = .namespaceRejected(NamespaceRejectedInfo(
                announcement: Announcement(nr.ann),
                errorCode: nr.error_code,
                reason: reason,
                canRetry: nr.can_retry
            ))

        case MOQ_EVENT_NAMESPACE_DONE:
            result = .namespaceDone(
                Announcement(cEvent.u.namespace_done.ann))

        case MOQ_EVENT_OBJECT_RECEIVED:
            let payload: Buffer?
            if let p = cEvent.u.object_received.payload {
                cEvent.u.object_received.payload = nil
                payload = Buffer(adopting: p)
            } else {
                payload = nil
            }
            let properties: Buffer?
            if let p = cEvent.u.object_received.properties {
                cEvent.u.object_received.properties = nil
                properties = Buffer(adopting: p)
            } else {
                properties = nil
            }
            let obj = cEvent.u.object_received
            result = .objectReceived(ReceivedObject(
                subscription: Subscription(obj.sub),
                groupID: obj.group_id,
                subgroupID: obj.subgroup_id,
                objectID: obj.object_id,
                publisherPriority: obj.publisher_priority,
                status: ObjectStatus(obj.status),
                endOfGroup: obj.end_of_group,
                isDatagram: obj.datagram,
                payload: payload,
                properties: properties
            ))

        case MOQ_EVENT_SUBGROUP_FINISHED:
            let sf = cEvent.u.subgroup_finished
            result = .subgroupFinished(SubgroupFinishedInfo(
                subscription: Subscription(sf.sub),
                groupID: sf.group_id,
                subgroupID: sf.subgroup_id,
                endOfGroup: sf.end_of_group
            ))

        default:
            result = .unknown(kind)
        }

        moq_event_cleanup(&cEvent)
        return result
    }
}
