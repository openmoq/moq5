import Foundation
import CMoQService
import MoQServiceCore

/* -- Copied object storage -------------------------------------------------- */

/// Received-object storage, COPIED BY DESIGN in v1: the C tier's rcbuf
/// refcounts are non-atomic and transferred buffers must never reach a
/// cross-thread finalizer (media_receiver.h zero-copy posture), so the
/// bridge copies media/fragment bytes on the service thread and cleans the
/// C object in the same poll job. `cleanup()` therefore has nothing left to
/// release. Zero-copy receive is a future C API project (BACKLOG:
/// "Thread-safe zero-copy receive export").
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
    moq_media_receiver_cfg_init(&cfg)
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
            let handleID = UInt64(UInt(bitPattern: event.track))
            switch event.kind {
            case MOQ_MEDIA_CATALOG_READY:
                return .event(ReceiverPolledEvent(
                    kind: .catalogReady, handleID: 0, trackDescription: nil))
            case MOQ_MEDIA_TRACK_ADDED:
                /* Deep copy at poll time; nil = outside the v1 model
                 * (timeline tracks) -- skip it and poll the next event.
                 * Later events for a skipped handle drop harmlessly in the
                 * core's unknown-handle guard. */
                guard let description = event.desc.flatMap({
                    copyTrackDescription($0.pointee)
                }) else { continue }
                return .event(ReceiverPolledEvent(
                    kind: .added, handleID: handleID,
                    trackDescription: description))
            case MOQ_MEDIA_TRACK_UPDATED:
                return .event(ReceiverPolledEvent(
                    kind: .updated, handleID: handleID,
                    trackDescription: event.desc.flatMap {
                        copyTrackDescription($0.pointee)
                    }))
            case MOQ_MEDIA_TRACK_REMOVED:
                return .event(ReceiverPolledEvent(
                    kind: .removed, handleID: handleID, trackDescription: nil))
            default:    /* MOQ_MEDIA_TRACK_ENDED */
                return .event(ReceiverPolledEvent(
                    kind: .ended, handleID: handleID, trackDescription: nil))
            }
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
        /* Ownership was transferred: copy out on THIS service thread, then
         * clean the C object here too -- the rcbuf-backed buffers never
         * cross the seam (non-atomic refcounts; copied-by-design v1). */
        let (media, fragment) = copyObjectPayload(object)
        let polled = ReceiverPolledObject(
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
                : nil)
        moq_media_object_cleanup(&object)
        return .object(polled)
    }

    func subscribe(handleID: UInt64, start: StartMode,
                   priority: UInt8?) -> ReceiverCommandResult {
        guard let track = OpaquePointer(bitPattern: UInt(handleID)) else {
            return .invalidArgument
        }
        var cfg = moq_media_receiver_track_subscribe_cfg_t()
        moq_media_receiver_track_subscribe_cfg_init(&cfg)
        cfg.start = start == .nextGroup
            ? MOQ_MEDIA_START_NEXT_GROUP : MOQ_MEDIA_START_CURRENT
        if let priority {
            cfg.has_priority = true
            cfg.priority = priority
        }
        return commandResult(
            moq_media_receiver_subscribe_track(receiver, track, &cfg))
    }

    func unsubscribe(handleID: UInt64) -> ReceiverCommandResult {
        guard let track = OpaquePointer(bitPattern: UInt(handleID)) else {
            return .invalidArgument
        }
        return commandResult(
            moq_media_receiver_unsubscribe_track(receiver, track))
    }

    private func commandResult(_ rc: moq_result_t) -> ReceiverCommandResult {
        switch rc {
        case moq_result_t(MOQ_OK): .ok
        case moq_result_t(MOQ_ERR_WRONG_STATE): .wrongState
        case moq_result_t(MOQ_ERR_CLOSED): .closed
        case moq_result_t(MOQ_ERR_UNSUPPORTED): .unsupported
        default: .invalidArgument
        }
    }

    func detach() {
        moq_media_receiver_destroy(receiver)
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
