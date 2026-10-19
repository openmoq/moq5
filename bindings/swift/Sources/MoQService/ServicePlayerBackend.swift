import Foundation
import CMoQService
import MoQServiceCore

/// The MoQService player backend: a C receiver attached to the endpoint
/// (RAW time, no auto-subscribe, the stream-terminal lane on) and a C
/// `moq_media_player` attached to it. Service thread only.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class ServicePlayerBackend: PlayerBackend, @unchecked Sendable {

    private var receiver: OpaquePointer?
    private var player: OpaquePointer?
    private var tracks: [UInt64: PlayerTrack] = [:]

    private init(receiver: OpaquePointer, player: OpaquePointer) {
        self.receiver = receiver
        self.player = player
    }

    static func make(endpoint cEndpoint: OpaquePointer,
                     configuration: MediaPlayer.Configuration) throws -> ServicePlayerBackend {
        var receiver: OpaquePointer?
        let rrc = withReceiverCFG(configuration.receiverConfiguration) { cfg in
            cfg.surface_stream_terminals = true      /* the player's lane */
            return withUnsafePointer(to: cfg) {
                moq_media_receiver_attach(cEndpoint, $0, &receiver)
            }
        }
        guard rrc == moq_result_t(MOQ_OK), let receiver else {
            throw serviceError(fromCreate: rrc, what: "player receiver attach")
        }
        var player: OpaquePointer?
        let prc = withPlayerCFG(configuration) { cfg in
            withUnsafePointer(to: cfg) { moq_media_player_attach(receiver, $0, &player) }
        }
        guard prc == moq_result_t(MOQ_OK), let player else {
            moq_media_receiver_destroy(receiver)
            throw serviceError(fromCreate: prc, what: "player attach")
        }
        return ServicePlayerBackend(receiver: receiver, player: player)
    }

    func destroy() {
        if let player { moq_media_player_destroy(player) }
        if let receiver { moq_media_receiver_destroy(receiver) }
        player = nil
        receiver = nil
    }

    func tick(nowMicroseconds: UInt64) -> PlayerTickResult {
        guard let player else { return .closed }
        switch moq_media_player_tick(player, nowMicroseconds) {
        case moq_result_t(MOQ_ERR_WOULD_BLOCK): return .wouldBlock
        case moq_result_t(MOQ_ERR_CLOSED): return .closed
        default: return .ok
        }
    }

    func pollOutput() -> PlayerOutput? {
        guard let player else { return nil }
        var out = moq_media_player_output_t()
        guard moq_media_player_poll_output(
            player, &out, MemoryLayout<moq_media_player_output_t>.size) == moq_result_t(MOQ_OK)
        else { return nil }
        let copied = copyPlayerOutput(out, track: track(out.track))
        moq_media_player_output_cleanup(&out)
        return copied
    }

    func pollEvent() -> PlayerEvent? {
        guard let player else { return nil }
        var ev = moq_media_player_event_t()
        guard moq_media_player_poll_event(
            player, &ev, MemoryLayout<moq_media_player_event_t>.size) == moq_result_t(MOQ_OK)
        else { return nil }
        return copyPlayerEvent(ev, track: track(ev.track))
    }

    var nextDeadlineMicroseconds: UInt64? {
        guard let player else { return nil }
        let due = moq_media_player_next_deadline_us(player)
        return due == UInt64.max ? nil : due
    }

    var isClosed: Bool {
        guard let player else { return true }
        return moq_media_player_is_closed(player)
    }

    var fatalCode: UInt64? {
        guard let player, moq_media_player_is_fatal(player) else { return nil }
        return moq_media_player_fatal_code(player)
    }

    func select(trackID: UInt64, start: StartMode, independentTiming: Bool,
                sharedEpoch: Bool) throws {
        guard let player, let track = OpaquePointer(bitPattern: UInt(trackID)) else {
            throw MoQServiceError.closed
        }
        var cfg = moq_media_player_select_cfg_t()
        moq_media_player_select_cfg_init_sized(
            &cfg, MemoryLayout<moq_media_player_select_cfg_t>.size)
        cfg.start = start == .nextGroup
            ? UInt32(MOQ_MEDIA_START_NEXT_GROUP.rawValue)
            : UInt32(MOQ_MEDIA_START_CURRENT.rawValue)
        cfg.independent_timing = independentTiming
        cfg.shared_epoch = sharedEpoch
        try check(moq_media_player_select_track(player, track, &cfg), what: "select")
    }

    func deselect(trackID: UInt64) throws {
        guard let player, let track = OpaquePointer(bitPattern: UInt(trackID)) else {
            throw MoQServiceError.closed
        }
        try check(moq_media_player_deselect_track(player, track), what: "deselect")
    }

    func feedback(trackID: UInt64, generation: UInt64, kind: PlayerFeedback) throws {
        guard let player, let track = OpaquePointer(bitPattern: UInt(trackID)) else {
            throw MoQServiceError.closed
        }
        let arena = CBytesArena()
        var fb = moq_media_player_feedback_t()
        moq_media_player_feedback_init_sized(&fb, MemoryLayout<moq_media_player_feedback_t>.size)
        fb.track = track
        fb.configuration_generation = generation
        switch kind {
        case .decoderQueue(let depth, let maxRecommended):
            fb.kind = UInt32(MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE)
            fb.u.decoder_queue.depth = UInt32(clamping: depth)
            fb.u.decoder_queue.max_recommended = UInt32(clamping: maxRecommended)
        case .decodeError(let message):
            fb.kind = UInt32(MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR)
            fb.u.decode_error.message = arena.bytes(message)
        case .flushComplete:
            fb.kind = UInt32(MOQ_MEDIA_PLAYER_FEEDBACK_FLUSH_COMPLETE)
        }
        let rc = withExtendedLifetime(arena) {
            moq_media_player_handle_feedback(player, &fb, EndpointEngine.monotonicMicroseconds())
        }
        try check(rc, what: "feedback")
    }

    func statistics() throws -> MediaPlayer.Statistics {
        guard let player else { throw MoQServiceError.closed }
        var s = moq_media_player_stats_t()
        try check(moq_media_player_get_stats(player, &s, MemoryLayout<moq_media_player_stats_t>.size),
                  what: "statistics")
        return MediaPlayer.Statistics(
            tracksSelected: Int(s.tracks_selected), objectsTaken: s.objects_taken,
            objectsPushed: s.objects_pushed, objectsDropped: s.objects_dropped,
            objectsDiscarded: s.objects_discarded, unitsOutput: s.units_output,
            configuresOutput: s.configures_output,
            discontinuitiesOutput: s.discontinuities_output, endsOutput: s.ends_output,
            eventsDropped: s.events_dropped, feedbackStale: s.feedback_stale,
            outputsBytes: s.outputs_bytes, selectionsFailed: s.selections_failed)
    }

    private func check(_ rc: moq_result_t, what: String) throws {
        guard rc != moq_result_t(MOQ_OK) else { return }
        if rc == moq_result_t(MOQ_ERR_INVAL) {
            throw MoQServiceError.invalidArgument("\(what): refused (not a track of this player, or not selected)")
        }
        throw serviceError(fromCreate: rc, what: what)
    }

    /// The track model for a receiver handle, described once and cached.
    private func track(_ handle: OpaquePointer?) -> PlayerTrack? {
        guard let handle else { return nil }
        let id = UInt64(UInt(bitPattern: handle))
        if let known = tracks[id] { return known }
        var description: TrackDescription?
        if let receiver {
            var desc = moq_media_track_desc_t()
            if moq_media_receiver_track_desc_copy(
                receiver, handle, &desc, MemoryLayout<moq_media_track_desc_t>.size)
                == moq_result_t(MOQ_OK) {
                description = copyTrackDescription(desc)
            }
        }
        let track = PlayerTrack(id: id, description: description)
        tracks[id] = track
        return track
    }
}

/* -- The factory (real public attach) ---------------------------------------- */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension ServiceEndpointBackend: PlayerBackendFactory {
    func makePlayerBackend(configuration: MediaPlayer.Configuration) throws -> any PlayerBackend {
        try ServicePlayerBackend.make(endpoint: endpoint, configuration: configuration)
    }
}

/* -- Configuration mapping ------------------------------------------------ */

/// Build the C player cfg for an attach (no receiver cfg: the player
/// attaches to the receiver made from the same configuration) and run
/// `body` with it; borrowed bytes stay alive for the call.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func withPlayerCFG<T>(
    _ configuration: MediaPlayer.Configuration,
    _ body: (inout moq_media_player_cfg_t) throws -> T
) rethrows -> T {
    let arena = CBytesArena()
    var cfg = moq_media_player_cfg_t()
    moq_media_player_cfg_init_sized(&cfg, MemoryLayout<moq_media_player_cfg_t>.size)
    cfg.selection = configuration.selection == .automatic
        ? UInt32(MOQ_MEDIA_SELECTION_AUTO) : UInt32(MOQ_MEDIA_SELECTION_MANUAL)
    cfg.timing_policy = configuration.timingPolicy == .requireSync
        ? UInt32(MOQ_MEDIA_TIMING_REQUIRE_SYNC) : UInt32(MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT)
    cfg.preferred_lang = arena.bytes(configuration.preferredLanguage)
    cfg.assume_shared_epoch = configuration.assumeSharedEpoch
    cfg.cushion_us = configuration.cushion?.wholeMicroseconds ?? 0
    cfg.end_tail_wait_us = configuration.endTailWait?.wholeMicroseconds ?? 0
    return try withExtendedLifetime(arena) { try body(&cfg) }
}

/* -- Copying C records ---------------------------------------------------- */

/// Deep-copy one polled output; `track` is its handle's model. The caller
/// still gives the C record to moq_media_player_output_cleanup.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func copyPlayerOutput(_ out: moq_media_player_output_t, track: PlayerTrack?) -> PlayerOutput {
    guard let track else {
        return .unknown(kind: out.kind, track: nil)
    }
    let generation = out.configuration_generation
    switch out.kind {
    case UInt32(MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE):
        let c = out.u.configure
        let mediaType: MediaType?
        switch c.media_type {
        case UInt32(MOQ_MEDIA_TYPE_VIDEO.rawValue): mediaType = .video
        case UInt32(MOQ_MEDIA_TYPE_AUDIO.rawValue): mediaType = .audio
        default: mediaType = nil
        }
        return .configure(PlayerDecoderConfiguration(
            track: track, configurationGeneration: generation,
            timelineID: out.timeline_id, mediaType: mediaType,
            codec: c.codec.map { String(decoding: rcbufBytes($0), as: UTF8.self) } ?? "",
            codecConfig: c.codec_config.map { rcbufBytes($0) },
            timescale: c.timescale != 0 ? c.timescale : nil,
            width: c.width != 0 ? Int(c.width) : nil,
            height: c.height != 0 ? Int(c.height) : nil,
            sampleRate: c.samplerate != 0 ? Int(c.samplerate) : nil,
            channelCount: c.channel_count != 0 ? Int(c.channel_count) : nil))
    case UInt32(MOQ_MEDIA_PLAYER_OUTPUT_UNIT):
        let u = out.u.unit
        var data = Data()
        if let payload = u.payload, u.len > 0, let base = moq_rcbuf_data(payload) {
            data = Data(bytes: base + u.offset, count: u.len)
        }
        return .unit(PlayerUnit(
            track: track, configurationGeneration: generation,
            timelineID: out.timeline_id, groupID: u.group_id, objectID: u.object_id,
            decodeTime: .microseconds(Int64(clamping: u.decode_time_us)),
            presentationTime: .microseconds(Int64(clamping: u.presentation_time_us)),
            isIndependent: u.independence_known ? u.independent : nil,
            sampleDuration: u.sample_duration_us != 0
                ? .microseconds(Int64(u.sample_duration_us)) : nil,
            captureTime: u.has_capture_time
                ? .microseconds(Int64(clamping: u.capture_time_us)) : nil,
            data: data))
    case UInt32(MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY):
        let d = out.u.discontinuity
        let reason: PlayerDiscontinuityReason
        switch d.reason {
        case UInt32(MOQ_MEDIA_PLAYER_DISCONTINUITY_GAP): reason = .gap
        case UInt32(MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR): reason = .decodeError
        default: reason = .unknown(d.reason)
        }
        return .discontinuity(PlayerDiscontinuity(
            track: track, configurationGeneration: generation,
            timelineID: out.timeline_id, reason: reason,
            decoderResetRequired: d.decoder_reset_required))
    case UInt32(MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE):
        let t = out.u.timeline
        let domain: PlayerTimeDomain
        switch t.domain {
        case moq_swift_media_time_domain_unix_us(): domain = .unixTime
        case moq_swift_media_time_domain_application_media(): domain = .applicationMedia
        default: domain = .unknown(t.domain)
        }
        return .timeline(PlayerTimeline(
            track: track, timelineID: t.timeline_id, epoch: t.timeline_epoch,
            domain: domain,
            anchorMedia: .microseconds(Int64(clamping: t.anchor_media_us)),
            anchorLocalMicroseconds: t.anchor_local_us, rate: t.rate))
    case UInt32(MOQ_MEDIA_PLAYER_OUTPUT_END):
        let e = out.u.end
        let cause: PlayerEndCause
        switch e.cause {
        case UInt32(MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK): cause = .peerEndOfTrack
        case UInt32(MOQ_MEDIA_PLAYER_END_PEER_DONE): cause = .peerDone
        case UInt32(MOQ_MEDIA_PLAYER_END_REMOVED): cause = .removed
        case UInt32(MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE): cause = .localFailure
        case UInt32(MOQ_MEDIA_PLAYER_END_CLOSED): cause = .closed
        case UInt32(MOQ_MEDIA_PLAYER_END_FATAL): cause = .fatal
        default: cause = .unknown(e.cause)
        }
        let disposition: PlayerEndDisposition
        switch e.disposition {
        case UInt32(MOQ_MEDIA_PLAYER_END_COMPLETE): disposition = .complete
        case UInt32(MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF): disposition = .tailCutoff
        case UInt32(MOQ_MEDIA_PLAYER_END_IMMEDIATE): disposition = .immediate
        default: disposition = .unknown(e.disposition)
        }
        return .end(PlayerTrackEnd(track: track, configurationGeneration: generation,
                                   cause: cause, disposition: disposition))
    default:
        return .unknown(kind: out.kind, track: track)
    }
    }

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func copyPlayerEvent(_ ev: moq_media_player_event_t, track: PlayerTrack?) -> PlayerEvent {
    switch (ev.kind, track) {
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_TRACK_AVAILABLE), let t?):
        return .trackAvailable(t)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED), let t?):
        return .trackRemoved(t)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_READY), _):
        return .ready
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_STARTED), let t?):
        return .started(t, timelineID: ev.u.started.timeline_id)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_GAP), let t?):
        let g = ev.u.gap
        return .gap(t, groupID: g.group_id, objectID: g.object_id, crossed: g.crossed)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_SKIP), let t?):
        return .skip(t, fromGroupID: ev.u.skip.from_group_id,
                     toGroupID: ev.u.skip.to_group_id)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_SHED), let t?):
        return .shed(t, droppedGroups: ev.u.shed.dropped_groups,
                     remainingGroups: ev.u.shed.remaining_groups)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_DROP), _):
        let d = ev.u.drop
        return .drop(track, reason: d.reason, groupID: d.group_id, objectID: d.object_id)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_LOSS), let t?):
        return .loss(t, groupID: ev.u.loss.group_id)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_STALL), let t?):
        return .stall(t)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_ENDED), _):
        return .ended
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_CLOSED), _):
        return .closed
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_TIMING_INDEPENDENT), _):
        let s = ev.u.sync
        return .timingIndependent(renderGroup: s.has_render_group ? s.render_group : nil)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_SYNC_UNAVAILABLE), _):
        let s = ev.u.sync
        return .syncUnavailable(renderGroup: s.has_render_group ? s.render_group : nil,
                                reason: s.reason)
    case (UInt32(MOQ_MEDIA_PLAYER_EVENT_KNOWLEDGE_FORGOTTEN), let t?):
        let k = ev.u.knowledge_forgotten
        return .knowledgeForgotten(t, firstGroupID: k.first_group_id,
                                   lastGroupID: k.last_group_id)
    default:
        return .unknown(kind: ev.kind, track: track)
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func rcbufBytes(_ buf: OpaquePointer) -> Data {
    guard let base = moq_rcbuf_data(buf) else { return Data() }
    return Data(bytes: base, count: moq_rcbuf_len(buf))
}
