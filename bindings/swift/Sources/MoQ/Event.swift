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

/// Complete event view. Borrowed native details are copied before returning;
/// object buffers keep their transferred references until the Swift value dies.
public enum ExtendedEvent: Equatable {
    case setupComplete
    case sessionClosed(code: UInt64)
    case subscribeRequest(ExtendedSubscribeRequest)
    case subscribeOk(SubscribeOkInfo)
    case subscribeError(SubscribeErrorInfo)
    case namespacePublished(NamespacePublishedInfo)
    case namespaceAccepted(Announcement)
    case namespaceRejected(NamespaceRejectedInfo)
    case namespaceDone(NamespaceDoneInfo)
    case objectReceived(ReceivedObject)
    case subgroupFinished(SubgroupFinishedInfo)
    /// Fill children (draft-22 section 3.4).
    case fillCreated(FillCreatedInfo)
    case fillRequest(FillRequestInfo)
    case fillObject(FillObject)
    case fillGap(FillGapInfo)
    case fillTerminal(FillTerminalInfo)
    /// A publisher's state notification (PUBLISH_STATE_NOTIFY).
    case publishState(ExtendedPublishStateInfo)
    /// Publisher side: a subscriber updated a subscription this side serves.
    case subscribeUpdated(SubscriptionUpdatedInfo)
    /// Subscriber side: the publisher acknowledged this side's update.
    case subscriptionUpdateOk(SubscriptionUpdateOkInfo)
    /// Publisher side: the subscriber ended a subscription.
    case unsubscribed(UnsubscribedInfo)
    /// A PUBLISH from the peer, and the answers to this side's PUBLISH.
    case publishRequest(PublishRequestInfo)
    case publishOk(PublishOkInfo)
    /// PUBLISH side: the subscriber updated the subscription this side's
    /// PUBLISH began.
    case publishUpdated(PublicationUpdatedInfo)
    case publicationUpdateOk(PublicationUpdateOkInfo)
    /// Subscriber side of a PUBLISH: the publisher ended the publication.
    case publishFinished(PublishFinishedInfo)
    /// PUBLISH side: the subscriber ended the publication.
    case publishUnsubscribed(PublishUnsubscribedInfo)
    /// The peer cancelled a namespace this side announced.
    case namespaceCancelled(NamespaceCancelledInfo)
    /// FETCH, publisher side.
    case fetchRequest(FetchRequestInfo)
    case fetchCancelled(FetchCancelledInfo)
    /// FETCH, fetcher side.
    case fetchOk(FetchOkInfo)
    case fetchError(FetchErrorInfo)
    case fetchObject(FetchObject)
    case fetchGap(FetchGapInfo)
    case fetchComplete(Fetch)
    /// Fetcher side: a stream of the FETCH ended by a stream signal.
    case fetchReset(FetchResetInfo)
    /// A TRACK_STATUS arrived (answer with acceptTrackStatus /
    /// rejectTrackStatus), or this side's TRACK_STATUS was answered.
    case trackStatusRequest(TrackStatusRequestInfo)
    case trackStatusOk(TrackStatusOkInfo)
    case trackStatusError(TrackStatusErrorInfo)
    case trackStatusTerminated(TrackStatusTerminatedInfo)
    case namespaceTerminated(NamespaceTerminatedInfo)
    case namespaceSubscriptionTerminated(NamespaceSubscriptionTerminatedInfo)
    /// SUBSCRIBE_TRACKS, publisher side: a peer's request.
    case subscribeTracksRequest(SubscribeTracksRequestInfo)
    /// SUBSCRIBE_TRACKS, subscriber side: the publisher's answers.
    case subscribeTracksOk(TrackSubscription)
    case subscribeTracksError(SubscribeTracksErrorInfo)
    case publishBlocked(PublishBlockedInfo)
    /// SUBSCRIBE_TRACKS, either side: the request stream ended.
    case subscribeTracksCancelled(SubscribeTracksCancelledInfo)
    /// A relay redirected one of this side's requests (error code REDIRECT).
    case requestRedirect(RequestRedirectInfo)
    /// Draft-22 namespace updates (section 9.5), responder side: the peer
    /// updated a request this side answers and the answer is owed
    /// (`setDeferNamespaceUpdates`).
    case namespaceUpdateRequest(NamespaceUpdateRequestInfo)
    /// An update concluded: the peer's answer to this side's update, or this
    /// side's own refusal of a peer update.
    case namespaceUpdateResult(NamespaceUpdateResultInfo)
    /// An outgoing subgroup's reset was committed locally. This does not
    /// imply termination or continued liveness of its subscription/publication.
    case outgoingSubgroupTerminated(OutgoingSubgroupTerminatedInfo)
    case unknown(UInt32)
}

/// A borrowed reason string copied out (empty when absent).
private func reasonString(_ r: moq_bytes_t) -> String {
    guard let data = r.data, r.len > 0 else { return "" }
    return String(bytes: UnsafeBufferPointer(start: data, count: r.len),
                  encoding: .utf8) ?? ""
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
    internal static func from(_ cEvent: inout moq_event_t) -> Event {
        let kind = cEvent.kind
        switch ExtendedEvent.from(&cEvent) {
        case .setupComplete: return .setupComplete
        case .sessionClosed(let code): return .sessionClosed(code: code)
        case .subscribeRequest(let value): return .subscribeRequest(value.legacy)
        case .subscribeOk(let value): return .subscribeOk(value)
        case .subscribeError(let value): return .subscribeError(value)
        case .namespacePublished(let value): return .namespacePublished(value)
        case .namespaceAccepted(let value): return .namespaceAccepted(value)
        case .namespaceRejected(let value): return .namespaceRejected(value)
        case .namespaceDone(let value): return .namespaceDone(value.announcement)
        case .objectReceived(let value): return .objectReceived(value)
        case .subgroupFinished(let value): return .subgroupFinished(value)
        default: return .unknown(kind)
        }
    }
}

extension ExtendedEvent {
    /// Convert a C event to Swift. Cleans up the C event exactly once.
    internal static func from(_ cEvent: inout moq_event_t) -> ExtendedEvent {
        let kind = cEvent.kind
        let result: ExtendedEvent

        switch kind {
        case MOQ_EVENT_SETUP_COMPLETE:
            result = .setupComplete

        case MOQ_EVENT_SESSION_CLOSED:
            result = .sessionClosed(code: cEvent.u.closed.code)

        case MOQ_EVENT_SUBSCRIBE_REQUEST:
            guard Int(cEvent.detail_size) >= MemoryLayout<moq_subscribe_request_event_t>.offset(
                of: \moq_subscribe_request_event_t.options)! else {
                moq_event_cleanup(&cEvent)
                return .unknown(kind)
            }
            let sr = cEvent.u.subscribe_request
            let track = TrackName.fromC(
                namespace: sr.track_namespace, name: sr.track_name)
            let filter = SubscribeFilter.fromC(
                sr.filter,
                startGroup: sr.start_group,
                startObject: sr.start_object,
                endGroup: sr.end_group)
            result = .subscribeRequest(ExtendedSubscribeRequest(legacy: SubscribeRequest(
                subscription: Subscription(sr.sub),
                track: track,
                filter: filter,
                subscriberPriority: sr.subscriber_priority,
                groupOrder: GroupOrder(sr.group_order)
            ), options: SubscriptionAdmissionOptions.from(cEvent)))

        case MOQ_EVENT_SUBSCRIBE_OK:
            let so = cEvent.u.subscribe_ok
            result = .subscribeOk(SubscribeOkInfo(
                subscription: Subscription(so.sub),
                expiresMs: so.has_expires ? so.expires_ms : nil,
                trackProperties: copiedData(so.track_properties),
                dynamicGroups: detailCovers(cEvent.detail_size,
                    \moq_subscribe_ok_event_t.dynamic_groups) && so.dynamic_groups
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
            guard Int(cEvent.detail_size) >= MemoryLayout<moq_namespace_done_event_t>.size else {
                result = .unknown(kind)
                break
            }
            let d = cEvent.u.namespace_done
            result = .namespaceDone(NamespaceDoneInfo(announcement: Announcement(d.ann)))

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
            let gapTail = detailCovers(cEvent.detail_size,
                                       \moq_object_received_event_t.prior_object_gap)
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
                properties: properties,
                firstObject: detailCovers(cEvent.detail_size,
                                          \moq_object_received_event_t.first_object)
                    ? obj.first_object : false,
                priorGroupGap: gapTail && obj.has_prior_group_gap ? obj.prior_group_gap : nil,
                priorObjectGap: gapTail && obj.has_prior_object_gap ? obj.prior_object_gap : nil,
                subgroupProperties: gapTail && obj.subgroup_properties
            ))

        case MOQ_EVENT_SUBGROUP_FINISHED:
            let sf = cEvent.u.subgroup_finished
            result = .subgroupFinished(SubgroupFinishedInfo(
                subscription: Subscription(sf.sub),
                groupID: sf.group_id,
                subgroupID: sf.subgroup_id,
                endOfGroup: sf.end_of_group
            ))

        case MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED:
            if cEvent.detail_size < UInt32(MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
                result = .unknown(kind)
            } else {
                let d = cEvent.u.outgoing_subgroup_terminated
                result = .outgoingSubgroupTerminated(OutgoingSubgroupTerminatedInfo(
                    subgroup: SubgroupHandle(d.subgroup),
                    subscription: moq_subscription_is_valid(d.sub) ? Subscription(d.sub) : nil,
                    publication: moq_publication_is_valid(d.pub) ? Publication(d.pub) : nil,
                    groupID: d.group_id, subgroupID: d.subgroup_id,
                    stream: StreamRef(d.stream_ref), errorCode: d.error_code,
                    cause: OutgoingSubgroupTerminalCause(d.cause)
                ))
            }

        case MOQ_EVENT_FILL_CREATED:
            let d = cEvent.u.fill_created
            result = .fillCreated(FillCreatedInfo(
                fill: Fill(d.fill), parent: FillParent(sub: d.sub, pub: d.pub),
                initiatingRequestID: d.initiating_request_id))

        case MOQ_EVENT_FILL_REQUEST:
            let d = cEvent.u.fill_request
            result = .fillRequest(FillRequestInfo(
                fill: Fill(d.fill), parent: FillParent(sub: d.sub, pub: d.pub),
                initiatingRequestID: d.initiating_request_id,
                processedAtUs: d.processed_at_us,
                startGroup: d.start_group, startObject: d.start_object,
                endGroup: d.end_group, endObject: d.end_object,
                subscriberPriority: d.subscriber_priority,
                groupOrder: GroupOrder(d.group_order),
                fillTimeoutMs: d.has_fill_timeout ? d.fill_timeout_ms : nil))

        case MOQ_EVENT_FILL_OBJECT:
            // Take ownership of the transferred refs; null them so the
            // cleanup below does not decref them.
            let payload = cEvent.u.fill_object.payload.map { Buffer(adopting: $0) }
            let properties = cEvent.u.fill_object.properties.map { Buffer(adopting: $0) }
            cEvent.u.fill_object.payload = nil
            cEvent.u.fill_object.properties = nil
            let d = cEvent.u.fill_object
            result = .fillObject(FillObject(
                fill: Fill(d.fill), parent: FillParent(sub: d.sub, pub: d.pub),
                initiatingRequestID: d.initiating_request_id,
                groupID: d.group_id, subgroupID: d.subgroup_id,
                objectID: d.object_id, publisherPriority: d.publisher_priority,
                isDatagram: d.datagram, payload: payload, properties: properties))

        case MOQ_EVENT_FILL_GAP:
            let d = cEvent.u.fill_gap
            result = .fillGap(FillGapInfo(
                fill: Fill(d.fill), parent: FillParent(sub: d.sub, pub: d.pub),
                initiatingRequestID: d.initiating_request_id,
                groupID: d.group_id, objectID: d.object_id,
                kind: FetchRangeKind(d.kind)))

        case MOQ_EVENT_FILL_TERMINAL:
            let d = cEvent.u.fill_terminal
            result = .fillTerminal(FillTerminalInfo(
                fill: Fill(d.fill), parent: FillParent(sub: d.sub, pub: d.pub),
                reason: FillTerminalInfo.Reason(d.reason),
                initiatingRequestID: d.initiating_request_id,
                domain: FillTerminalInfo.Domain(d.error_domain),
                errorCode: d.error_code))

        case MOQ_EVENT_PUBLISH_STATE:
            result = ExtendedPublishStateInfo.from(cEvent).map { .publishState($0) } ?? .unknown(kind)

        case MOQ_EVENT_SUBSCRIBE_UPDATED:
            let d = cEvent.u.subscribe_updated
            typealias T = moq_subscribe_updated_event_t
            result = .subscribeUpdated(SubscriptionUpdatedInfo(
                subscription: Subscription(d.sub),
                update: UpdateRequestInfo(
                    d, detailSize: cEvent.detail_size,
                    hasPriority: \T.has_subscriber_priority, priority: \T.subscriber_priority,
                    hasForward: \T.has_forward, forward: \T.forward,
                    hasTimeout: \T.has_delivery_timeout, timeout: \T.delivery_timeout_us,
                    hasFilter: \T.has_filter, filter: \T.filter,
                    startGroup: \T.start_group, startObject: \T.start_object,
                    endGroup: \T.end_group, endObject: nil,
                    answerOwed: \T.answer_owed, fillRequested: \T.fill_requested,
                    updateCount: \T.update_count, updateRequestID: \T.update_request_id,
                    options: detailCovers(cEvent.detail_size, \T.options)
                        ? d.options : nil)))

        case MOQ_EVENT_SUBSCRIPTION_UPDATE_OK:
            let d = cEvent.u.subscription_update_ok
            typealias T = moq_subscription_update_ok_event_t
            let size = cEvent.detail_size
            let hasLargest = detailCovers(size, \T.has_largest) && d.has_largest &&
                detailCovers(size, \T.largest_group) && detailCovers(size, \T.largest_object)
            let hasExpires = detailCovers(size, \T.has_expires) && d.has_expires &&
                detailCovers(size, \T.expires_ms)
            result = .subscriptionUpdateOk(SubscriptionUpdateOkInfo(
                subscription: Subscription(d.sub),
                largest: hasLargest ? (d.largest_group, d.largest_object) : nil,
                expiresMs: hasExpires ? d.expires_ms : nil))

        case MOQ_EVENT_UNSUBSCRIBED:
            let d = cEvent.u.unsubscribed
            let (cause, code) = terminalCause(
                cEvent.detail_size, d, \moq_unsubscribed_event_t.cause,
                \moq_unsubscribed_event_t.stream_error_code)
            result = .unsubscribed(UnsubscribedInfo(
                subscription: Subscription(d.sub), cause: cause, streamErrorCode: code))

        case MOQ_EVENT_PUBLISH_REQUEST:
            let d = cEvent.u.publish_request
            typealias T = moq_publish_request_event_t
            let size = cEvent.detail_size
            let hasExpires = detailCovers(size, \T.has_expires) && d.has_expires &&
                detailCovers(size, \T.expires_ms)
            result = .publishRequest(PublishRequestInfo(
                publication: Publication(d.pub),
                track: TrackName.fromC(namespace: d.track_namespace, name: d.track_name),
                trackAlias: d.track_alias,
                forward: d.forward,
                expiresMs: hasExpires ? d.expires_ms : nil,
                trackProperties: copiedData(d.track_properties)))

        case MOQ_EVENT_PUBLISH_OK:
            let d = cEvent.u.publish_ok
            typealias T = moq_publish_ok_event_t
            let size = cEvent.detail_size
            let hasExpires = detailCovers(size, \T.has_expires) && d.has_expires &&
                detailCovers(size, \T.expires_ms)
            result = .publishOk(PublishOkInfo(
                publication: Publication(d.pub),
                expiresMs: hasExpires ? d.expires_ms : nil))

        case MOQ_EVENT_PUBLISH_UPDATED:
            let d = cEvent.u.publish_updated
            typealias T = moq_publish_updated_event_t
            result = .publishUpdated(PublicationUpdatedInfo(
                publication: Publication(d.pub),
                update: UpdateRequestInfo(
                    d, detailSize: cEvent.detail_size,
                    hasPriority: \T.has_subscriber_priority, priority: \T.subscriber_priority,
                    hasForward: \T.has_forward, forward: \T.forward,
                    hasTimeout: \T.has_delivery_timeout, timeout: \T.delivery_timeout_us,
                    hasFilter: \T.has_filter, filter: \T.filter,
                    startGroup: \T.start_group, startObject: \T.start_object,
                    endGroup: \T.end_group, endObject: nil,
                    answerOwed: \T.answer_owed, fillRequested: \T.fill_requested,
                    updateCount: \T.update_count, updateRequestID: \T.update_request_id,
                    options: detailCovers(cEvent.detail_size, \T.options)
                        ? d.options : nil)))

        case MOQ_EVENT_PUBLICATION_UPDATE_OK:
            let d = cEvent.u.publication_update_ok
            typealias T = moq_publication_update_ok_event_t
            let size = cEvent.detail_size
            let hasLargest = detailCovers(size, \T.has_largest) && d.has_largest &&
                detailCovers(size, \T.largest_group) && detailCovers(size, \T.largest_object)
            let hasExpires = detailCovers(size, \T.has_expires) && d.has_expires &&
                detailCovers(size, \T.expires_ms)
            result = .publicationUpdateOk(PublicationUpdateOkInfo(
                publication: Publication(d.pub),
                largest: hasLargest ? (d.largest_group, d.largest_object) : nil,
                expiresMs: hasExpires ? d.expires_ms : nil))

        case MOQ_EVENT_FETCH_REQUEST:
            let d = cEvent.u.fetch_request
            result = .fetchRequest(FetchRequestInfo(
                fetch: Fetch(d.fetch),
                track: TrackName.fromC(namespace: d.track_namespace, name: d.track_name),
                startGroup: d.start_group, startObject: d.start_object,
                endGroup: d.end_group, endObject: d.end_object,
                subscriberPriority: d.subscriber_priority,
                groupOrder: GroupOrder(d.group_order),
                options: FetchRequestOptions.from(cEvent)))

        case MOQ_EVENT_FETCH_CANCELLED:
            let d = cEvent.u.fetch_cancelled
            let (cause, code) = terminalCause(
                cEvent.detail_size, d, \moq_fetch_cancelled_event_t.cause,
                \moq_fetch_cancelled_event_t.stream_error_code)
            result = .fetchCancelled(FetchCancelledInfo(
                fetch: Fetch(d.fetch), cause: cause, streamErrorCode: code))

        case MOQ_EVENT_FETCH_OK:
            let d = cEvent.u.fetch_ok
            result = .fetchOk(FetchOkInfo(
                fetch: Fetch(d.fetch), endOfTrack: d.end_of_track,
                endGroup: d.end_group, endObject: d.end_object,
                trackProperties: copiedData(d.track_properties)))

        case MOQ_EVENT_FETCH_ERROR:
            let d = cEvent.u.fetch_error
            result = .fetchError(FetchErrorInfo(
                fetch: Fetch(d.fetch), errorCode: d.error_code,
                reason: reasonString(d.reason), canRetry: d.can_retry))

        case MOQ_EVENT_FETCH_OBJECT:
            // Take ownership of the transferred refs; null them so the
            // cleanup below does not decref them.
            let payload = cEvent.u.fetch_object.payload.map { Buffer(adopting: $0) }
            let properties = cEvent.u.fetch_object.properties.map { Buffer(adopting: $0) }
            cEvent.u.fetch_object.payload = nil
            cEvent.u.fetch_object.properties = nil
            let d = cEvent.u.fetch_object
            let gapTail = detailCovers(cEvent.detail_size,
                                       \moq_fetch_object_event_t.prior_object_gap)
            result = .fetchObject(FetchObject(
                fetch: Fetch(d.fetch), groupID: d.group_id, subgroupID: d.subgroup_id,
                objectID: d.object_id, publisherPriority: d.publisher_priority,
                isDatagram: d.datagram, payload: payload, properties: properties,
                priorGroupGap: gapTail && d.has_prior_group_gap ? d.prior_group_gap : nil,
                priorObjectGap: gapTail && d.has_prior_object_gap ? d.prior_object_gap : nil))

        case MOQ_EVENT_FETCH_GAP:
            let d = cEvent.u.fetch_gap
            result = .fetchGap(FetchGapInfo(
                fetch: Fetch(d.fetch), kind: FetchRangeKind(d.range_kind.rawValue),
                groupID: d.group_id, objectID: d.object_id))

        case MOQ_EVENT_FETCH_COMPLETE:
            result = .fetchComplete(Fetch(cEvent.u.fetch_complete.fetch))

        case MOQ_EVENT_FETCH_RESET:
            let d = cEvent.u.fetch_reset
            result = .fetchReset(FetchResetInfo(
                fetch: Fetch(d.fetch), errorCode: d.error_code,
                dataStream: d.data_stream, stopSending: d.stop_sending,
                cause: detailCovers(cEvent.detail_size, \moq_fetch_reset_event_t.cause)
                    ? RequestTerminalCause(d.cause) : .none))

        case MOQ_EVENT_PUBLISH_FINISHED:
            let d = cEvent.u.publish_finished
            let (cause, code) = terminalCause(
                cEvent.detail_size, d, \moq_publish_finished_event_t.cause,
                \moq_publish_finished_event_t.stream_error_code)
            result = .publishFinished(PublishFinishedInfo(
                publication: Publication(d.pub), statusCode: d.status_code,
                streamCount: d.stream_count, reason: reasonString(d.reason),
                cause: cause, streamErrorCode: code))

        case MOQ_EVENT_PUBLISH_UNSUBSCRIBED:
            let d = cEvent.u.publish_unsubscribed
            let (cause, code) = terminalCause(
                cEvent.detail_size, d, \moq_publish_unsubscribed_event_t.cause,
                \moq_publish_unsubscribed_event_t.stream_error_code)
            result = .publishUnsubscribed(PublishUnsubscribedInfo(
                publication: Publication(d.pub), cause: cause, streamErrorCode: code))

        case MOQ_EVENT_NAMESPACE_CANCELLED:
            guard detailCovers(cEvent.detail_size, \moq_namespace_cancelled_event_t.reason) else {
                result = .unknown(kind)
                break
            }
            let d = cEvent.u.namespace_cancelled
            result = .namespaceCancelled(NamespaceCancelledInfo(
                announcement: Announcement(d.ann), errorCode: d.error_code,
                reason: reasonString(d.reason)))

        case MOQ_EVENT_TRACK_STATUS_REQUEST:
            let d = cEvent.u.track_status_request
            result = .trackStatusRequest(TrackStatusRequestInfo(
                request: TrackStatus(d.handle),
                track: TrackName.fromC(namespace: d.track_namespace, name: d.track_name)))

        case MOQ_EVENT_TRACK_STATUS_OK:
            let d = cEvent.u.track_status_ok
            result = .trackStatusOk(TrackStatusOkInfo(
                request: TrackStatus(d.handle),
                largestGroup: d.has_largest ? d.largest_group : nil,
                largestObject: d.has_largest ? d.largest_object : nil,
                expiresMs: d.has_expires ? d.expires_ms : nil,
                trackProperties: detailCovers(cEvent.detail_size,
                    \moq_track_status_ok_event_t.track_properties)
                    ? copiedData(d.track_properties) : Data()))

        case MOQ_EVENT_TRACK_STATUS_ERROR:
            let d = cEvent.u.track_status_error
            result = .trackStatusError(TrackStatusErrorInfo(
                request: TrackStatus(d.handle), errorCode: d.error_code,
                reason: reasonString(d.reason), canRetry: d.can_retry))

        case MOQ_EVENT_TRACK_STATUS_TERMINATED:
            guard Int(cEvent.detail_size) >= MemoryLayout<moq_track_status_terminated_event_t>.size else {
                result = .unknown(kind)
                break
            }
            let d = cEvent.u.track_status_terminated
            result = .trackStatusTerminated(TrackStatusTerminatedInfo(
                handle: d.handle._opaque,
                cause: TrackStatusTerminalCause(d.cause),
                localRole: RequestRole(d.local_role),
                streamErrorCode: d.stream_error_code))

        case MOQ_EVENT_NAMESPACE_TERMINATED:
            guard Int(cEvent.detail_size) >= MemoryLayout<moq_namespace_terminated_event_t>.size else {
                result = .unknown(kind)
                break
            }
            let d = cEvent.u.namespace_terminated
            result = .namespaceTerminated(NamespaceTerminatedInfo(
                announcement: Announcement(d.ann), cause: NamespaceTerminalCause(d.cause),
                localRole: RequestRole(d.local_role), streamErrorCode: d.stream_error_code))

        case MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED:
            guard Int(cEvent.detail_size) >= MemoryLayout<moq_ns_subscription_terminated_event_t>.size else {
                result = .unknown(kind)
                break
            }
            let d = cEvent.u.ns_subscription_terminated
            result = .namespaceSubscriptionTerminated(NamespaceSubscriptionTerminatedInfo(
                handle: d.handle._opaque,
                cause: RequestTerminalCause(d.cause),
                localRole: RequestRole(d.local_role),
                streamErrorCode: d.stream_error_code))

        case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST:
            let d = cEvent.u.subscribe_tracks_request
            result = .subscribeTracksRequest(SubscribeTracksRequestInfo(
                trackSubscription: TrackSubscription(d.handle),
                prefix: Namespace.fromC(d.track_namespace_prefix),
                forward: d.forward,
                options: SubscribeTracksOptions.from(cEvent)))

        case MOQ_EVENT_SUBSCRIBE_TRACKS_OK:
            result = .subscribeTracksOk(
                TrackSubscription(cEvent.u.subscribe_tracks_ok.handle))

        case MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR:
            let d = cEvent.u.subscribe_tracks_error
            result = .subscribeTracksError(SubscribeTracksErrorInfo(
                trackSubscription: TrackSubscription(d.handle),
                errorCode: d.error_code, reason: reasonString(d.reason),
                canRetry: d.can_retry, retryAfterMs: d.retry_after_ms))

        case MOQ_EVENT_PUBLISH_BLOCKED:
            let d = cEvent.u.publish_blocked
            let suffix = TrackName.fromC(
                namespace: d.track_namespace_suffix, name: d.track_name)
            result = .publishBlocked(PublishBlockedInfo(
                trackSubscription: TrackSubscription(d.handle),
                trackNamespaceSuffix: suffix.namespace, trackName: suffix.name.bytes))

        case MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED:
            let d = cEvent.u.subscribe_tracks_cancelled
            let (cause, code) = terminalCause(
                cEvent.detail_size, d, \moq_subscribe_tracks_cancelled_event_t.cause,
                \moq_subscribe_tracks_cancelled_event_t.stream_error_code)
            result = .subscribeTracksCancelled(SubscribeTracksCancelledInfo(
                trackSubscription: TrackSubscription(d.handle),
                cause: cause, streamErrorCode: code))

        case MOQ_EVENT_REQUEST_REDIRECT:
            let d = cEvent.u.request_redirect
            let target = TrackName.fromC(
                namespace: d.track_namespace, name: d.track_name)
            let uri: Data
            if let p = d.connect_uri.data, d.connect_uri.len > 0 {
                uri = Data(bytes: p, count: d.connect_uri.len)
            } else {
                uri = Data()
            }
            result = .requestRedirect(RequestRedirectInfo(
                request: RedirectedRequest(d),
                errorCode: d.error_code, canRetry: d.can_retry,
                retryAfterMs: d.retry_after_ms, connectURI: uri,
                trackNamespace: target.namespace, trackName: target.name.bytes,
                reason: reasonString(d.reason)))

        default:
            result = ExtendedEvent.namespaceUpdate(cEvent) ?? .unknown(kind)
        }

        moq_event_cleanup(&cEvent)
        return result
    }
}
