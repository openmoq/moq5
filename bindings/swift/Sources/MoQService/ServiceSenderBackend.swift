import Foundation
import CMoQService
import MoQServiceCore

/* -- Configuration mapping ------------------------------------------------- */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func withSenderCFG<T>(
    _ configuration: MediaSender.Configuration,
    _ body: (inout moq_media_sender_cfg_t) throws -> T
) rethrows -> T {
    let arena = CBytesArena()
    var cfg = moq_media_sender_cfg_t()
    moq_media_sender_cfg_init(&cfg)
    cfg.namespace_ = arena.namespaceParts(configuration.namespace.parts)
    cfg.catalog_track = arena.bytes(configuration.catalogTrack)
    switch configuration.sendPolicy {
    case .dropToKeyframe:
        cfg.backpressure = MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME
    case .dropGroup:
        cfg.backpressure = MOQ_MEDIA_SEND_BP_DROP_GROUP
    case .lossless, .failFast:
        /* Both run over the C fail-fast mode: `.lossless` is the Swift
         * sliced suspend-retry loop; C BLOCK_TIMEOUT is never used (it
         * blocks the service thread uncancellably). */
        cfg.backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK
    }
    if let limits = configuration.queueLimits {
        cfg.queue_max_objects = UInt32(clamping: limits.maxObjects ?? 0)
        cfg.queue_max_bytes = UInt32(clamping: limits.maxBytes ?? 0)
    }
    cfg.validate_cmaf = configuration.validateCMAF
    return try withExtendedLifetime(arena) {
        try body(&cfg)
    }
}

/// Map a track configuration into the C track cfg. `label` has no C
/// counterpart (the v0 catalog builder does not author MSF labels) and is
/// not sent; everything else maps 1:1. Strings/init data are borrowed for
/// the call -- C copies during add_track.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func withTrackCFG<T>(
    _ configuration: TrackConfiguration,
    _ body: (inout moq_media_track_cfg_t) throws -> T
) rethrows -> T {
    let arena = CBytesArena()
    var cfg = moq_media_track_cfg_t()
    moq_media_track_cfg_init(&cfg)
    cfg.name = arena.bytes(configuration.name)
    cfg.media_type = configuration.mediaType == .audio
        ? MOQ_MEDIA_TYPE_AUDIO : MOQ_MEDIA_TYPE_VIDEO
    cfg.packaging = configuration.packaging == .cmaf
        ? MOQ_MEDIA_PACKAGING_CMAF : MOQ_MEDIA_PACKAGING_RAW
    cfg.codec = arena.bytes(configuration.codec)
    cfg.timescale = configuration.timescale ?? 0    /* 0 = microseconds */
    cfg.init_data = arena.bytes(configuration.initData)
    cfg.role = arena.bytes(configuration.role)
    cfg.lang = arena.bytes(configuration.language)
    cfg.is_live = configuration.isLive
    cfg.width = UInt32(clamping: configuration.width ?? 0)
    cfg.height = UInt32(clamping: configuration.height ?? 0)
    cfg.framerate_millis = configuration.framerateMillis ?? 0
    cfg.samplerate = UInt32(clamping: configuration.samplerate ?? 0)
    cfg.channel_config = arena.bytes(configuration.channelConfig)
    cfg.bitrate = configuration.bitrate
    return try withExtendedLifetime(arena) {
        try body(&cfg)
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func cSAPType(_ sapType: SAPType) -> moq_sap_type_t {
    switch sapType {
    case .type1: MOQ_SAP_TYPE_1
    case .type2: MOQ_SAP_TYPE_2
    case .type3: MOQ_SAP_TYPE_3
    }
}

/* -- The real sender backend ------------------------------------------------ */

/// `SenderBackend` over an owned `moq_media_sender_t`. Writes run on the
/// engine's service thread: the payload is copied into a fresh rcbuf whose
/// lifetime begins and ends inside the one write call -- transferred to C
/// on MOQ_OK, decref'd immediately on ANY failure -- so the non-atomic
/// refcounts never cross threads.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ServiceSenderBackend: SenderBackend, @unchecked Sendable {

    private let sender: OpaquePointer

    init(sender: OpaquePointer) {
        self.sender = sender
    }

    var isReady: Bool { moq_media_sender_is_ready(sender) }
    var isFatal: Bool { moq_media_sender_is_fatal(sender) }
    var fatalCode: UInt64 { moq_media_sender_fatal_code(sender) }
    var isTerminal: Bool {
        moq_media_sender_is_closed(sender) || moq_media_sender_is_fatal(sender)
    }

    func addTrack(_ configuration: TrackConfiguration) -> SenderAddTrackResult {
        var track: OpaquePointer?
        let rc = withTrackCFG(configuration) { cfg in
            withUnsafePointer(to: cfg) {
                moq_media_sender_add_track(sender, $0, &track)
            }
        }
        switch rc {
        case moq_result_t(MOQ_OK):
            guard let track else { return .invalidArgument }
            return .track(handleID: UInt64(UInt(bitPattern: track)))
        case moq_result_t(MOQ_ERR_WOULD_BLOCK): return .wouldBlock
        case moq_result_t(MOQ_ERR_WRONG_STATE): return .wrongState
        case moq_result_t(MOQ_ERR_CLOSED): return .closed
        case moq_result_t(MOQ_ERR_NOMEM): return .outOfMemory
        default: return .invalidArgument
        }
    }

    func write(handleID: UInt64,
               object: OutgoingObject) -> SenderOperationResult {
        guard let track = OpaquePointer(bitPattern: UInt(handleID)) else {
            return .invalidArgument
        }
        let alloc = moq_alloc_default()

        /* Payload rcbuf: created here, transferred on MOQ_OK only. */
        var payload: OpaquePointer?
        let payloadRC = object.payload.withUnsafeBytes { raw in
            moq_rcbuf_create(
                alloc, raw.bindMemory(to: UInt8.self).baseAddress,
                raw.count, &payload)
        }
        guard payloadRC == moq_result_t(MOQ_OK), let payload else {
            return .outOfMemory
        }

        /* Optional CMAF property block (must stay nil for RAW: the C
         * service generates the LOC block itself and rejects extras). */
        var properties: OpaquePointer?
        if let propertyBytes = object.properties {
            let propertiesRC = propertyBytes.withUnsafeBytes { raw in
                moq_rcbuf_create(
                    alloc, raw.bindMemory(to: UInt8.self).baseAddress,
                    raw.count, &properties)
            }
            guard propertiesRC == moq_result_t(MOQ_OK) else {
                moq_rcbuf_decref(payload)
                return .outOfMemory
            }
        }

        var send = moq_media_send_object_t()
        send.struct_size = UInt32(MemoryLayout<moq_media_send_object_t>.size)
        send.payload = payload
        send.properties = properties
        send.is_sync = object.isSync
        send.starts_group = object.startsGroup
        send.ends_group = object.endsGroup
        send.presentation_time_us = object.presentationTime.wholeMicroseconds
        send.decode_time_us = object.decodeTime?.wholeMicroseconds ?? 0
        if let captureTime = object.captureTime {
            send.has_capture_time = true
            send.capture_time_us =
                UInt64(max(0, captureTime.timeIntervalSince1970) * 1_000_000)
        }
        if let sapType = object.sapType {
            send.has_sap_type = true
            send.sap_type = cSAPType(sapType)
        }

        let rc = withUnsafePointer(to: send) {
            moq_media_sender_write(sender, track, $0)
        }
        if rc == moq_result_t(MOQ_OK) {
            return .ok      /* refs transferred to the service */
        }
        moq_rcbuf_decref(payload)
        if let properties { moq_rcbuf_decref(properties) }
        return operationResult(rc)
    }

    func endTrack(handleID: UInt64) -> SenderOperationResult {
        guard let track = OpaquePointer(bitPattern: UInt(handleID)) else {
            return .invalidArgument
        }
        return operationResult(moq_media_sender_end_track(sender, track))
    }

    func wait(timeoutMicroseconds: UInt64) -> SenderWaitOutcome {
        switch moq_media_sender_wait(sender, timeoutMicroseconds) {
        case moq_result_t(MOQ_OK): .activity
        case moq_result_t(MOQ_DONE): .timeout
        case moq_result_t(MOQ_ERR_INTERRUPTED): .interrupted
        default: .closed
        }
    }

    func detach() {
        moq_media_sender_destroy(sender)
    }

    private func operationResult(_ rc: moq_result_t) -> SenderOperationResult {
        switch rc {
        case moq_result_t(MOQ_OK): .ok
        case moq_result_t(MOQ_ERR_WOULD_BLOCK): .wouldBlock
        case moq_result_t(MOQ_ERR_WRONG_STATE): .wrongState
        case moq_result_t(MOQ_ERR_INTERRUPTED): .interrupted
        case moq_result_t(MOQ_ERR_CLOSED): .closed
        case moq_result_t(MOQ_ERR_NOMEM): .outOfMemory
        default: .invalidArgument
        }
    }
}

/* -- The factory (real public attach) ---------------------------------------- */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension ServiceEndpointBackend: SenderBackendFactory {
    func makeSenderBackend(
        configuration: MediaSender.Configuration
    ) throws -> any SenderBackend {
        var sender: OpaquePointer?
        let rc = withSenderCFG(configuration) { cfg in
            withUnsafePointer(to: cfg) {
                moq_media_sender_attach(endpoint, $0, &sender)
            }
        }
        guard rc == moq_result_t(MOQ_OK), let sender else {
            throw serviceError(fromCreate: rc, what: "sender attach")
        }
        return ServiceSenderBackend(sender: sender)
    }
}
