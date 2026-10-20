import Foundation
import CMoQService
import MoQServiceCore

/* -- Copied object storage -------------------------------------------------- */

/// Received-object storage, COPIED BY DESIGN in v1: the C tier's rcbuf
/// refcounts are non-atomic and transferred buffers must never reach a
/// cross-thread finalizer (media_receiver.h zero-copy posture), so the
/// bridge copies media/fragment bytes on the service thread and cleans the
/// C object in the same poll job. `cleanup()` therefore has nothing left to
/// release. Zero-copy receive would need a thread-safe zero-copy export from
/// the C API, which it does not provide.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class CopiedObjectStorage: OwnedObjectStorage, Sendable {
    private let media: Data
    private let fragment: Data

    init(media: Data, fragment: Data) {
        self.media = media
        self.fragment = fragment
    }

    var mediaByteCount: Int { media.count }
    var fragmentByteCount: Int { fragment.count }

    func withMediaBytes<R>(
        _ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        try media.withUnsafeBytes(body)
    }

    func withFragmentBytes<R>(
        _ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        try fragment.withUnsafeBytes(body)
    }

    func cleanup() {
        /* The C object was cleaned at poll time, on the service thread. */
    }
}

/* -- Configuration and description mapping ---------------------------------- */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func withReceiverCFG<T>(
    _ configuration: MediaReceiver.Configuration,
    _ body: (inout moq_media_receiver_cfg_t) throws -> T
) rethrows -> T {
    let arena = CBytesArena()
    var cfg = moq_media_receiver_cfg_t()
    moq_media_receiver_cfg_init_sized(&cfg, MemoryLayout<moq_media_receiver_cfg_t>.size)
    cfg.companion_profile = cMediaCompanionProfile(configuration.companionProfile)
    cfg.namespace_ = arena.namespaceParts(configuration.namespace.parts)
    cfg.catalog_track = arena.bytes(configuration.catalogTrack)
    cfg.auto_subscribe = configuration.autoSubscribe
    cfg.time_mode = configuration.timeMode == .sharedEpoch
        ? MOQ_MEDIA_TIME_SHARED_EPOCH : MOQ_MEDIA_TIME_RAW
    let (policy, maxObjects, maxBytes): (moq_media_overflow_policy_t, Int?, Int?)
    switch configuration.overflowPolicy {
    case .dropToKeyframe(let o, let b):
        (policy, maxObjects, maxBytes) = (MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, o, b)
    case .dropGroup(let o, let b):
        (policy, maxObjects, maxBytes) = (MOQ_MEDIA_OVERFLOW_DROP_GROUP, o, b)
    case .flowControl(let o, let b):
        (policy, maxObjects, maxBytes) = (MOQ_MEDIA_OVERFLOW_FLOW_CONTROL, o, b)
    }
    cfg.overflow.policy = policy
    cfg.overflow.max_objects = UInt32(clamping: maxObjects ?? 0)  /* 0 = default */
    cfg.overflow.max_bytes = UInt64(clamping: maxBytes ?? 0)
    return try withExtendedLifetime(arena) {
        try body(&cfg)
    }
}

/// Deep-copy a handle-owned C track description into the Sendable model
/// snapshot. Returns nil for tracks outside the v1 model (eventtimeline /
/// mediatimeline / unknown media types) -- those surface with the SAP and
/// timeline record slices.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func copyTrackDescription(_ desc: moq_media_track_desc_t) -> TrackDescription? {
    let mediaType: MediaType
    switch desc.info.media_type {
    case MOQ_MEDIA_TYPE_VIDEO: mediaType = .video
    case MOQ_MEDIA_TYPE_AUDIO: mediaType = .audio
    default: return nil
    }
    let packaging: MediaPackaging =
        desc.info.packaging == MOQ_MEDIA_PACKAGING_CMAF ? .cmaf : .raw
    var copy = TrackDescription(
        name: swiftString(desc.name), mediaType: mediaType, packaging: packaging)
    copy.codec = optionalString(desc.codec)
    copy.role = optionalString(desc.role)
    copy.language = optionalString(desc.lang)
    copy.label = optionalString(desc.label)
    copy.width = desc.has_width ? Int(desc.width) : nil
    copy.height = desc.has_height ? Int(desc.height) : nil
    copy.samplerate = desc.has_samplerate ? Int(desc.samplerate) : nil
    copy.channelConfig = optionalString(desc.channel_config)
    copy.framerateMillis = desc.has_framerate ? desc.framerate_millis : nil
    copy.bitrate = desc.has_bitrate ? desc.bitrate : nil
    copy.timescale = desc.info.timescale != 0 ? desc.info.timescale : nil
    copy.isLive = desc.is_live
    copy.trackDuration = desc.has_track_duration
        ? .milliseconds(desc.track_duration_ms) : nil
    copy.initData = optionalData(desc.init_data)
    copy.codecConfig = desc.has_init
        ? optionalData(desc.`init`.codec_config) : nil
    return copy
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func swiftString(_ bytes: moq_bytes_t) -> String {
    guard let data = bytes.data, bytes.len > 0 else { return "" }
    return String(
        decoding: UnsafeBufferPointer(start: data, count: bytes.len),
        as: UTF8.self)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func optionalString(_ bytes: moq_bytes_t) -> String? {
    bytes.len > 0 ? swiftString(bytes) : nil
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func optionalData(_ bytes: moq_bytes_t) -> Data? {
    guard let data = bytes.data, bytes.len > 0 else { return nil }
    return Data(bytes: data, count: bytes.len)
}

/// COPY-OUT: media/fragment bytes into Swift-owned Data. For CMAF the media
/// bytes are the mdat slice inside the fragment; for RAW/LOC the payload.
/// The caller cleans the C object immediately after.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func copyObjectPayload(_ object: moq_media_object_t) -> (media: Data, fragment: Data) {
    if object.packaging == MOQ_MEDIA_PACKAGING_CMAF {
        let fragment = optionalData(object.fragment) ?? Data()
        let media: Data
        if let base = object.fragment.data, object.mdat_len > 0,
           object.mdat_offset + object.mdat_len <= object.fragment.len {
            media = Data(bytes: base + object.mdat_offset, count: object.mdat_len)
        } else {
            media = Data()
        }
        return (media, fragment)
    }
    return (optionalData(object.payload) ?? Data(), Data())
}

private func receivedFieldIsPresent<Root, Field>(
    _ stamp: UInt32, _ field: KeyPath<Root, Field>
) -> Bool {
    Int(stamp) >= MemoryLayout<Root>.offset(of: field)! + MemoryLayout<Field>.size
}

/// Successful native outputs own valid spans. A present zero-length span is
/// empty Data, not absence; no pointer is retained by the copy.
private func copyReceivedSpan(_ bytes: moq_bytes_t) -> Data {
    if bytes.len == 0 { return Data() }
    return Data(bytes: bytes.data!, count: bytes.len)
}

private func copyReceivedProperties(_ ref: OpaquePointer?) -> Data {
    copyReceivedSpan(moq_bytes_t(data: moq_rcbuf_data(ref), len: moq_rcbuf_len(ref)))
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func copyReceivedLOC(_ loc: moq_loc_metadata_t) -> MediaLOCMetadata? {
    guard receivedFieldIsPresent(loc.struct_size, \moq_loc_metadata_t.audio_config) else {
        return nil
    }
    let marking: Data?
    if loc.present & moq_swift_loc_metadata_has_marking() != 0 {
        marking = withUnsafeBytes(of: loc.marking) {
            Data($0[..<Int(loc.marking_len)])
        }
    } else {
        marking = nil
    }
    return MediaLOCMetadata(
        rawPresence: loc.present,
        timestamp: loc.present & moq_swift_loc_metadata_has_timestamp() != 0
            ? loc.timestamp : nil,
        timescale: loc.present & moq_swift_loc_metadata_has_timescale() != 0
            ? loc.timescale : nil,
        marking: marking,
        audioLevel: loc.present & moq_swift_loc_metadata_has_audio_level() != 0
            ? loc.audio_level : nil,
        videoConfig: loc.present & moq_swift_loc_metadata_has_video_config() != 0
            ? copyReceivedSpan(loc.video_config) : nil,
        audioConfig: loc.present & moq_swift_loc_metadata_has_audio_config() != 0
            ? copyReceivedSpan(loc.audio_config) : nil)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func copyReceivedMetadata(_ metadata: moq_media_metadata_t) -> MediaObjectMetadata? {
    guard receivedFieldIsPresent(metadata.struct_size, \moq_media_metadata_t.reserved) else {
        return nil
    }
    let domain: MediaTimeDomain
    switch metadata.time_domain {
    case moq_swift_media_time_domain_none(): domain = .none
    case moq_swift_media_time_domain_unix_us(): domain = .unixMicroseconds
    case moq_swift_media_time_domain_application_media(): domain = .applicationMedia
    default: domain = .unknown(metadata.time_domain)
    }
    return MediaObjectMetadata(
        timeDomain: domain,
        timeMicroseconds: metadata.has_time_us != 0 ? metadata.time_us : nil,
        isIndependent: metadata.independence_known != 0 ? metadata.independent != 0 : nil,
        loc: copyReceivedLOC(metadata.loc))
}

/// Consume one successful native poll on the service thread. Every owned span
/// is copied before the single stamped cleanup; no C buffer reaches Sendable.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func takeReceivedObject(_ object: inout moq_media_object_t) -> ReceiverPolledObject {
    defer { moq_media_object_cleanup(&object) }
    let (media, fragment) = copyObjectPayload(object)
    let identity = receivedFieldIsPresent(object.struct_size, \moq_media_object_t.has_subgroup)
        ? MediaObjectIdentity(
            groupID: object.group_id,
            subgroupID: object.has_subgroup ? object.subgroup_id : nil,
            objectID: object.object_id, publisherPriority: object.publisher_priority)
        : nil
    let metadata = receivedFieldIsPresent(object.struct_size, \moq_media_object_t.metadata)
        ? copyReceivedMetadata(object.metadata) : nil
    let trackProperties = receivedFieldIsPresent(
        object.struct_size, \moq_media_object_t.track_properties_ref)
        ? copyReceivedProperties(object.track_properties_ref) : nil
    let delivery: MediaObjectDelivery?
    if receivedFieldIsPresent(object.struct_size, \moq_media_object_t.initiating_request_id) {
        switch object.delivery_origin {
        case MOQ_MEDIA_DELIVERY_LIVE: delivery = .live
        case MOQ_MEDIA_DELIVERY_FILL:
            delivery = .fill(initiatingRequestID: object.initiating_request_id)
        default:
            delivery = .unknown(rawOrigin: object.delivery_origin,
                                rawInitiatingRequestID: object.initiating_request_id)
        }
    } else {
        delivery = nil
    }
    return ReceiverPolledObject(
        handleID: UInt64(UInt(bitPattern: object.track)),
        storage: CopiedObjectStorage(media: media, fragment: fragment),
        isKeyframe: object.keyframe,
        endsGroup: object.end_of_group,
        isDatagram: object.datagram,
        presentationTime: .microseconds(object.presentation_time_us),
        decodeTime: .microseconds(object.decode_time_us),
        compositionOffset: .microseconds(object.composition_offset_us),
        captureTime: object.has_capture_time
            ? Date(timeIntervalSince1970:
                    Double(object.capture_time_us) / 1_000_000)
            : nil,
        wireIdentity: identity,
        metadata: metadata,
        objectProperties: copyReceivedProperties(object.properties_ref),
        trackProperties: trackProperties,
        delivery: delivery)
}

/* -- The real receiver backend ---------------------------------------------- */

/// `ReceiverBackend` over an owned `moq_media_receiver_t`. Polls and detach
/// run on the engine's service thread (its confinement); the snapshot
/// getters are the C any-thread queries. Received object bytes are COPIED
/// at poll time (see ``CopiedObjectStorage``).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ServiceReceiverBackend: ReceiverBackend, @unchecked Sendable {

    private let receiver: OpaquePointer

    init(receiver: OpaquePointer) {
        self.receiver = receiver
    }

    var isFatal: Bool { moq_media_receiver_is_fatal(receiver) }
    var fatalCode: UInt64 { moq_media_receiver_fatal_code(receiver) }
    var isTerminal: Bool {
        moq_media_receiver_is_closed(receiver)
            || moq_media_receiver_is_fatal(receiver)
    }

    var hasQueuedObjects: Bool {
        var stats = moq_media_receiver_stats_t()
        let rc = moq_media_receiver_get_stats(
            receiver, &stats, MemoryLayout<moq_media_receiver_stats_t>.size)
        return rc == moq_result_t(MOQ_OK) && stats.objects_queued > 0
    }

    func pollTrackEvent() -> ReceiverPollResult {
        while true {
            var event = moq_media_track_event_t()
            let rc = moq_media_receiver_poll_track(
                receiver, &event, MemoryLayout<moq_media_track_event_t>.size)
            switch rc {
            case moq_result_t(MOQ_DONE):
                return .none
            case moq_result_t(MOQ_OK):
                break
            default:
                return .closed
            }
            if let copied = copyReceiverTrackEvent(event) { return .event(copied) }
        }
    }

    func pollObject() -> ReceiverObjectPollResult {
        var object = moq_media_object_t()
        let rc = moq_media_receiver_poll_object(
            receiver, &object, MemoryLayout<moq_media_object_t>.size)
        switch rc {
        case moq_result_t(MOQ_DONE):
            return .none
        case moq_result_t(MOQ_ERR_INTERRUPTED):
            return .interrupted
        case moq_result_t(MOQ_OK):
            break
        default:
            return .closed
        }
        return .object(takeReceivedObject(&object))
    }

    func subscribe(handleID: UInt64, start: StartMode,
                   priority: UInt8?, options: MediaTrackSubscriptionOptions) -> ReceiverCommandResult {
        guard let track = OpaquePointer(bitPattern: UInt(handleID)) else {
            return .invalidArgument
        }
        do {
            return try withReceiverTrackSubscribeCFG(start: start, priority: priority, options: options) {
                receiverCommandResult(moq_media_receiver_subscribe_track(receiver, track, &$0))
            }
        } catch {
            // The nonthrowing body leaves only the marshaller's size refusal.
            return .nativeFailure(Int32(MOQ_ERR_ABI_MISMATCH))
        }
    }

    func unsubscribe(handleID: UInt64) -> ReceiverCommandResult {
        guard let track = OpaquePointer(bitPattern: UInt(handleID)) else {
            return .invalidArgument
        }
        return receiverCommandResult(
            moq_media_receiver_unsubscribe_track(receiver, track))
    }

    func cancelFill(handleID: UInt64, initiatingRequestID: UInt64) -> ReceiverCommandResult {
        cancelReceiverFill(receiver: receiver, handleID: handleID,
                           initiatingRequestID: initiatingRequestID)
    }

    func trackState(handleID: UInt64) -> Result<ExtendedTrackSubscriptionState, MoQServiceError> {
        queryReceiverTrackState(receiver: receiver, handleID: handleID)
    }

    func detach() {
        moq_media_receiver_destroy(receiver)
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func copyReceiverTrackEvent(_ event: moq_media_track_event_t) -> ReceiverPolledEvent? {
    let handleID = UInt64(UInt(bitPattern: event.track))
    switch event.kind {
    case MOQ_MEDIA_CATALOG_READY:
        return ReceiverPolledEvent(kind: .catalogReady, handleID: 0, trackDescription: nil)
    case MOQ_MEDIA_TRACK_ADDED:
        // Timeline/unknown media tracks stay outside the legacy model.
        guard let description = event.desc.flatMap({ copyTrackDescription($0.pointee) }) else {
            return nil
        }
        return ReceiverPolledEvent(kind: .added, handleID: handleID, trackDescription: description)
    case MOQ_MEDIA_TRACK_UPDATED:
        return ReceiverPolledEvent(kind: .updated, handleID: handleID,
                                   trackDescription: event.desc.flatMap { copyTrackDescription($0.pointee) })
    case MOQ_MEDIA_TRACK_REMOVED:
        return ReceiverPolledEvent(kind: .removed, handleID: handleID, trackDescription: nil)
    case MOQ_MEDIA_TRACK_ENDED:
        return ReceiverPolledEvent(kind: .ended, handleID: handleID, trackDescription: nil)
    case MOQ_MEDIA_TRACK_PARSE_DROP:
        let cls: ParseDropClass
        switch event.parse_drop_class {
        case MOQ_MEDIA_PARSE_DROP_MEDIA: cls = .media
        case MOQ_MEDIA_PARSE_DROP_SAP: cls = .sap
        case MOQ_MEDIA_PARSE_DROP_MEDIA_TIMELINE: cls = .mediaTimeline
        default: return nil // Do not invent a class or a track end.
        }
        return ReceiverPolledEvent(
            kind: .parseDrop, handleID: handleID, trackDescription: nil,
            parseDropClass: cls, parseDropTotal: event.parse_drops_total,
            parseDropDelta: event.parse_drops_delta)
    case MOQ_MEDIA_TRACK_FILL_TERMINAL:
        let terminal = MediaFillTerminal(
            initiatingRequestID: receivedFieldIsPresent(event.struct_size, \moq_media_track_event_t.initiating_request_id)
                ? event.initiating_request_id : nil,
            reason: receivedFieldIsPresent(event.struct_size, \moq_media_track_event_t.fill_reason)
                ? copyFillReason(event.fill_reason) : nil,
            errorDomain: receivedFieldIsPresent(event.struct_size, \moq_media_track_event_t.fill_error_domain)
                ? copyFillErrorDomain(event.fill_error_domain) : nil,
            errorCode: receivedFieldIsPresent(event.struct_size, \moq_media_track_event_t.fill_error_code)
                ? event.fill_error_code : nil)
        return ReceiverPolledEvent(kind: .fillTerminal(terminal), handleID: handleID, trackDescription: nil)
    case MOQ_MEDIA_TRACK_LOCAL_FAILURE:
        let localResult = receivedFieldIsPresent(event.struct_size, \moq_media_track_event_t.local_result)
            ? event.local_result : nil
        return ReceiverPolledEvent(kind: .localFailure(localResult: localResult),
                                   handleID: handleID, trackDescription: nil)
    case MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE:
        let localResult = receivedFieldIsPresent(event.struct_size, \moq_media_track_event_t.local_result)
            ? event.local_result : nil
        return ReceiverPolledEvent(kind: .updateLocalFailure(localResult: localResult),
                                   handleID: handleID, trackDescription: nil)
    case MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE:
        let localResult = receivedFieldIsPresent(event.struct_size, \moq_media_track_event_t.local_result)
            ? event.local_result : nil
        let origin = receivedFieldIsPresent(event.struct_size, \moq_media_track_event_t.delivery_origin)
            ? event.delivery_origin : nil
        let initiatingID = origin == MOQ_MEDIA_DELIVERY_FILL && receivedFieldIsPresent(
            event.struct_size, \moq_media_track_event_t.delivery_initiating_request_id)
            ? event.delivery_initiating_request_id : nil
        return ReceiverPolledEvent(kind: .deliveryLocalFailure(localResult: localResult,
            origin: origin, initiatingRequestID: initiatingID), handleID: handleID, trackDescription: nil)
    default:
        // Unmodeled kinds are not false track ends.
        return nil
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func copyFillReason(_ raw: UInt32) -> MediaFillReason {
    switch raw {
    case MOQ_FILL_COMPLETE: .complete
    case MOQ_FILL_NO_STREAM: .noStream
    case MOQ_FILL_LOCAL_CANCELLED: .localCancelled
    case MOQ_FILL_PEER_CANCELLED_OR_RESET: .peerCancelledOrReset
    case MOQ_FILL_LOCAL_ABORTED: .localAborted
    case MOQ_FILL_PARENT_REJECTED: .parentRejected
    case MOQ_FILL_PARENT_TERMINATED: .parentTerminated
    default: .unknown(raw)
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func copyFillErrorDomain(_ raw: UInt32) -> MediaFillErrorDomain {
    switch raw {
    case MOQ_FILL_ERROR_NONE: .none
    case MOQ_FILL_ERROR_STREAM: .stream
    case MOQ_FILL_ERROR_PARENT_REQUEST: .parentRequest
    case MOQ_FILL_ERROR_PARENT_DONE: .parentDone
    default: .unknown(raw)
    }
}

/// Every pointer below borrows a sized local only until the synchronous body
/// returns. The service copies selected values before returning from subscribe.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func withReceiverTrackSubscribeCFG<T>(
    start: StartMode, priority: UInt8?, options: MediaTrackSubscriptionOptions,
    _ body: (inout moq_media_receiver_track_subscribe_cfg_t) -> T
) throws -> T {
    var cfg = moq_media_receiver_track_subscribe_cfg_t()
    moq_media_receiver_track_subscribe_cfg_init_sized(&cfg, MemoryLayout.size(ofValue: cfg))
    guard receivedFieldIsPresent(cfg.struct_size, \moq_media_receiver_track_subscribe_cfg_t.priority),
          options.location == nil || receivedFieldIsPresent(cfg.struct_size, \moq_media_receiver_track_subscribe_cfg_t.location),
          options.fill == nil || receivedFieldIsPresent(cfg.struct_size, \moq_media_receiver_track_subscribe_cfg_t.fill) else {
        throw MoQServiceError.internalError(Int32(MOQ_ERR_ABI_MISMATCH))
    }
    cfg.start = start == .nextGroup ? MOQ_MEDIA_START_NEXT_GROUP : MOQ_MEDIA_START_CURRENT
    if let priority {
        cfg.has_priority = true
        cfg.priority = priority
    }
    return try withReceiverLocation(options.location) { location in
        try withReceiverFill(options.fill) { fill in
            cfg.location = location
            cfg.fill = fill
            return body(&cfg)
        }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func withReceiverLocation<T>(
    _ location: MediaLocationFilter?,
    _ body: (UnsafePointer<moq_location_filter_cfg_t>?) throws -> T
) throws -> T {
    guard let location else { return try body(nil) }
    var cfg = moq_location_filter_cfg_t()
    moq_location_filter_cfg_init_sized(&cfg, MemoryLayout.size(ofValue: cfg))
    guard cfg.struct_size >= MOQ_LOCATION_FILTER_CFG_V0_SIZE else {
        throw MoQServiceError.internalError(Int32(MOQ_ERR_ABI_MISMATCH))
    }
    switch location {
    case .none: cfg.kind = MOQ_LOCATION_FILTER_NONE
    case .nextObject: cfg.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT
    case .relativeGroup(let offset):
        cfg.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP
        cfg.relative_group_offset = offset
    case .absoluteStart(let group, let object):
        cfg.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START
        cfg.start_group = group
        cfg.start_object = object
    case .groupEnd(let startGroup, let startObject, let endGroup):
        cfg.kind = MOQ_LOCATION_FILTER_GROUP_END
        cfg.start_group = startGroup
        cfg.start_object = startObject
        cfg.end_group = endGroup
    case .objectEnd(let startGroup, let startObject, let endGroup, let endObject):
        cfg.kind = MOQ_LOCATION_FILTER_OBJECT_END
        cfg.start_group = startGroup
        cfg.start_object = startObject
        cfg.end_group = endGroup
        cfg.end_object = endObject
    }
    return try withUnsafePointer(to: &cfg) { try body($0) }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func withReceiverFill<T>(
    _ fill: MediaFillOptions?, _ body: (UnsafePointer<moq_fill_cfg_t>?) throws -> T
) throws -> T {
    guard let fill else { return try body(nil) }
    var cfg = moq_fill_cfg_t()
    moq_fill_cfg_init_sized(&cfg, MemoryLayout.size(ofValue: cfg))
    guard cfg.struct_size >= MOQ_FILL_CFG_V0_SIZE else {
        throw MoQServiceError.internalError(Int32(MOQ_ERR_ABI_MISMATCH))
    }
    if let priority = fill.subscriberPriority {
        cfg.has_subscriber_priority = true
        cfg.subscriber_priority = priority
    }
    if let order = fill.groupOrder {
        cfg.has_group_order = true
        cfg.group_order = order == .ascending ? MOQ_GROUP_ORDER_ASCENDING : MOQ_GROUP_ORDER_DESCENDING
    }
    if let timeout = fill.timeoutMilliseconds {
        cfg.has_fill_timeout = true
        cfg.fill_timeout_ms = timeout
    }
    return try withReceiverLocation(fill.location) { location in
        cfg.location = location
        return try withUnsafePointer(to: &cfg) { try body($0) }
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func cancelReceiverFill(
    receiver: OpaquePointer, handleID: UInt64, initiatingRequestID: UInt64,
    call: (OpaquePointer?, OpaquePointer?, UInt64) -> moq_result_t = moq_media_receiver_cancel_fill
) -> ReceiverCommandResult {
    guard let track = OpaquePointer(bitPattern: UInt(handleID)) else { return .invalidArgument }
    return receiverCommandResult(call(receiver, track, initiatingRequestID))
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func receiverCommandResult(_ rc: moq_result_t) -> ReceiverCommandResult {
    switch rc {
    case moq_result_t(MOQ_OK): .ok
    case moq_result_t(MOQ_ERR_INVAL): .invalidArgument
    case moq_result_t(MOQ_ERR_WRONG_STATE): .wrongState
    case moq_result_t(MOQ_ERR_CLOSED): .closed
    case moq_result_t(MOQ_ERR_UNSUPPORTED): .unsupported
    default: .nativeFailure(rc)
    }
}

/// Use the real getter for each call; neither delivery events nor descriptions
/// provide an authoritative current state. The output is read on success only.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func queryReceiverTrackState(
    receiver: OpaquePointer?, handleID: UInt64,
    call: (OpaquePointer?, OpaquePointer?, UnsafeMutablePointer<moq_media_track_state_t>?) -> moq_result_t
        = moq_media_receiver_track_state
) -> Result<ExtendedTrackSubscriptionState, MoQServiceError> {
    guard let track = OpaquePointer(bitPattern: UInt(handleID)) else {
        return .failure(.invalidArgument("track state: invalid track"))
    }
    var state = MOQ_MEDIA_TRACK_STATE_DISCOVERED
    switch receiverCommandResult(call(receiver, track, &state)) {
    case .ok:
        let copied: ExtendedTrackSubscriptionState
        switch state {
        case MOQ_MEDIA_TRACK_STATE_DISCOVERED: copied = .legacy(.discovered)
        case MOQ_MEDIA_TRACK_STATE_PENDING: copied = .legacy(.pending)
        case MOQ_MEDIA_TRACK_STATE_ACTIVE: copied = .legacy(.active)
        case MOQ_MEDIA_TRACK_STATE_PAUSED_APP: copied = .legacy(.pausedByApp)
        case MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW: copied = .legacy(.pausedByFlowControl)
        case MOQ_MEDIA_TRACK_STATE_ENDED: copied = .legacy(.ended)
        case MOQ_MEDIA_TRACK_STATE_FAILED: copied = .failed
        default: copied = .unknown(state.rawValue)
        }
        return .success(copied)
    case .invalidArgument: return .failure(.invalidArgument("track state: invalid track"))
    case .wrongState: return .failure(.wrongState)
    case .closed: return .failure(.closed)
    case .unsupported: return .failure(.unsupported)
    case .nativeFailure(let code): return .failure(.internalError(code))
    }
}

/* -- The factory (real public attach) ---------------------------------------- */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension ServiceEndpointBackend: ReceiverBackendFactory {
    func makeReceiverBackend(
        configuration: MediaReceiver.Configuration
    ) throws -> any ReceiverBackend {
        var receiver: OpaquePointer?
        let rc = withReceiverCFG(configuration) { cfg in
            withUnsafePointer(to: cfg) {
                moq_media_receiver_attach(endpoint, $0, &receiver)
            }
        }
        guard rc == moq_result_t(MOQ_OK), let receiver else {
            throw serviceError(fromCreate: rc, what: "receiver attach")
        }
        return ServiceReceiverBackend(receiver: receiver)
    }
}
