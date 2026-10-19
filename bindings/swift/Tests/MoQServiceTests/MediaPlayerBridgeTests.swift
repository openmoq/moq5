import Testing
import Foundation
import CMoQService
@testable import MoQService
import MoQServiceCore
#if canImport(CoreMedia)
import CoreMedia
#endif

/* Deterministic, NO-SOCKET media player bridge tests: configuration mapping
 * into the real C cfg, the deep copy of every C output and event kind
 * (fabricated records over real rcbufs, released through the C cleanup as a
 * polled record is), the staging queue, the timeline mapping and the
 * CoreMedia helpers. */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func makeRcbuf(_ bytes: [UInt8]) throws -> OpaquePointer {
    var buf: OpaquePointer?
    let rc = bytes.withUnsafeBufferPointer {
        moq_rcbuf_create(moq_alloc_default(), $0.baseAddress, $0.count, &buf)
    }
    try #require(rc == moq_result_t(MOQ_OK))
    return try #require(buf)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private let videoTrack = PlayerTrack(
    id: 0x1000,
    description: TrackDescription(name: "video", mediaType: .video, packaging: .raw))

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func output(kind: UInt32) -> moq_media_player_output_t {
    var out = moq_media_player_output_t()
    out.struct_size = UInt32(MemoryLayout<moq_media_player_output_t>.size)
    out.kind = kind
    out.timeline_id = 3
    out.configuration_generation = 9
    out.track = OpaquePointer(bitPattern: 0x1000)
    return out
}

@Suite("Media player configuration mapping")
struct PlayerConfigurationMappingTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Defaults map to the C defaults: AUTO, REQUIRE_SYNC, zeros")
    func defaults() {
        withPlayerCFG(MediaPlayer.Configuration(namespace: "live/cam1")) { cfg in
            #expect(cfg.struct_size == MemoryLayout<moq_media_player_cfg_t>.size)
            #expect(cfg.selection == UInt32(MOQ_MEDIA_SELECTION_AUTO))
            #expect(cfg.timing_policy == UInt32(MOQ_MEDIA_TIMING_REQUIRE_SYNC))
            #expect(cfg.receiver == nil)          /* attach takes none */
            #expect(cfg.playback == nil)
            #expect(cfg.preferred_lang.len == 0)
            #expect(cfg.cushion_us == 0 && cfg.end_tail_wait_us == 0)
            #expect(cfg.assume_shared_epoch == false)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Every setting maps into moq_media_player_cfg_t")
    func fullMapping() {
        var configuration = MediaPlayer.Configuration(namespace: "live/cam1")
        configuration.selection = .manual
        configuration.timingPolicy = .allowIndependent
        configuration.preferredLanguage = "fr"
        configuration.assumeSharedEpoch = true
        configuration.cushion = .milliseconds(250)
        configuration.endTailWait = .seconds(5)
        withPlayerCFG(configuration) { cfg in
            #expect(cfg.selection == UInt32(MOQ_MEDIA_SELECTION_MANUAL))
            #expect(cfg.timing_policy == UInt32(MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT))
            #expect(swiftString(cfg.preferred_lang) == "fr")
            #expect(cfg.assume_shared_epoch)
            #expect(cfg.cushion_us == 250_000)
            #expect(cfg.end_tail_wait_us == 5_000_000)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The owned receiver is RAW and never auto-subscribes")
    func receiverConfiguration() {
        var configuration = MediaPlayer.Configuration(namespace: "live/cam1")
        configuration.companionProfile = .draft22MSF01LOC04Public
        configuration.catalogTrack = "alt"
        withReceiverCFG(configuration.receiverConfiguration) { cfg in
            #expect(cfg.auto_subscribe == false)
            #expect(cfg.time_mode == MOQ_MEDIA_TIME_RAW)
            #expect(cfg.overflow.policy == MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME)
            #expect(cfg.companion_profile == moq_swift_media_companion_d22_public())
            #expect(swiftString(cfg.catalog_track) == "alt")
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Non-positive durations are refused before any I/O")
    func validation() {
        var configuration = MediaPlayer.Configuration(namespace: "live/cam1")
        configuration.cushion = .zero
        #expect(throws: MoQServiceError.self) { try configuration.validate() }
        configuration.cushion = nil
        configuration.endTailWait = .milliseconds(-1)
        #expect(throws: MoQServiceError.self) { try configuration.validate() }
        configuration.endTailWait = nil
        #expect(throws: Never.self) { try configuration.validate() }
    }
}

@Suite("Media player output copies")
struct PlayerOutputCopyTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A unit copies exactly its span of the payload, then cleanup frees it")
    func unit() throws {
        let payload = try makeRcbuf([0xAA, 0xBB, 1, 2, 3, 4, 0xCC])
        var out = output(kind: UInt32(MOQ_MEDIA_PLAYER_OUTPUT_UNIT))
        out.u.unit.group_id = 5
        out.u.unit.object_id = 6
        out.u.unit.decode_time_us = 1_000
        out.u.unit.presentation_time_us = 34_333
        out.u.unit.independence_known = true
        out.u.unit.independent = false
        out.u.unit.sample_duration_us = 33_333
        out.u.unit.payload = payload
        out.u.unit.offset = 2
        out.u.unit.len = 4
        let copied = copyPlayerOutput(out, track: videoTrack)
        moq_media_player_output_cleanup(&out)        /* releases the only ref */
        #expect(out.u.unit.payload == nil)
        guard case .unit(let unit) = copied else {
            Issue.record("expected a unit, got \(copied)")
            return
        }
        #expect(unit.data == Data([1, 2, 3, 4]))     /* outlives the C record */
        #expect(unit.track == videoTrack)
        #expect(unit.configurationGeneration == 9 && unit.timelineID == 3)
        #expect(unit.groupID == 5 && unit.objectID == 6)
        #expect(unit.decodeTime == .microseconds(1_000))
        #expect(unit.presentationTime == .microseconds(34_333))
        #expect(unit.isIndependent == false)
        #expect(unit.sampleDuration == .microseconds(33_333))
        #expect(unit.captureTime == nil)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Unknown independence and an absent duration stay nil")
    func unitUnknowns() {
        var out = output(kind: UInt32(MOQ_MEDIA_PLAYER_OUTPUT_UNIT))
        out.u.unit.has_capture_time = true
        out.u.unit.capture_time_us = 77
        guard case .unit(let unit) = copyPlayerOutput(out, track: videoTrack) else {
            Issue.record("expected a unit")
            return
        }
        #expect(unit.isIndependent == nil)
        #expect(unit.sampleDuration == nil)
        #expect(unit.captureTime == .microseconds(77))
        #expect(unit.data.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A configure copies codec and decoder configuration; zeros are nil")
    func configure() throws {
        var out = output(kind: UInt32(MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE))
        out.u.configure.media_type = UInt32(MOQ_MEDIA_TYPE_VIDEO.rawValue)
        out.u.configure.codec = try makeRcbuf(Array("avc1.42c01e".utf8))
        out.u.configure.codec_config = try makeRcbuf([1, 0x42, 0xC0])
        out.u.configure.width = 640
        out.u.configure.height = 360
        let copied = copyPlayerOutput(out, track: videoTrack)
        moq_media_player_output_cleanup(&out)
        guard case .configure(let c) = copied else {
            Issue.record("expected a configure")
            return
        }
        #expect(c.mediaType == .video)
        #expect(c.codec == "avc1.42c01e")
        #expect(c.codecConfig == Data([1, 0x42, 0xC0]))
        #expect(c.width == 640 && c.height == 360)
        #expect(c.timescale == nil && c.sampleRate == nil && c.channelCount == nil)
        #expect(c.configurationGeneration == 9)

        var audio = output(kind: UInt32(MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE))
        audio.u.configure.media_type = UInt32(MOQ_MEDIA_TYPE_AUDIO.rawValue)
        audio.u.configure.samplerate = 48_000
        audio.u.configure.channel_count = 2
        audio.u.configure.timescale = 48_000
        guard case .configure(let a) = copyPlayerOutput(audio, track: videoTrack) else {
            Issue.record("expected a configure")
            return
        }
        #expect(a.mediaType == .audio && a.codec.isEmpty && a.codecConfig == nil)
        #expect(a.sampleRate == 48_000 && a.channelCount == 2 && a.timescale == 48_000)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Timeline, discontinuity and end map every known value")
    func lifecycleRecords() {
        var tl = output(kind: UInt32(MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE))
        tl.u.timeline.domain = moq_swift_media_time_domain_application_media()
        tl.u.timeline.timeline_id = 3
        tl.u.timeline.timeline_epoch = 1
        tl.u.timeline.anchor_media_us = 2_000_000
        tl.u.timeline.anchor_local_us = 10_000_000
        tl.u.timeline.rate = 1.0
        guard case .timeline(let t) = copyPlayerOutput(tl, track: videoTrack) else {
            Issue.record("expected a timeline")
            return
        }
        #expect(t.domain == .applicationMedia && t.timelineID == 3 && t.epoch == 1)
        #expect(t.anchorMedia == .seconds(2) && t.anchorLocalMicroseconds == 10_000_000)

        let reasons: [(UInt32, PlayerDiscontinuityReason)] = [
            (UInt32(MOQ_MEDIA_PLAYER_DISCONTINUITY_GAP), .gap),
            (UInt32(MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR), .decodeError),
            (99, .unknown(99)),
        ]
        for (raw, expected) in reasons {
            var d = output(kind: UInt32(MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY))
            d.u.discontinuity.reason = raw
            d.u.discontinuity.decoder_reset_required = true
            guard case .discontinuity(let disc) = copyPlayerOutput(d, track: videoTrack) else {
                Issue.record("expected a discontinuity")
                continue
            }
            #expect(disc.reason == expected && disc.decoderResetRequired)
        }

        let causes: [(UInt32, PlayerEndCause)] = [
            (UInt32(MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK), .peerEndOfTrack),
            (UInt32(MOQ_MEDIA_PLAYER_END_PEER_DONE), .peerDone),
            (UInt32(MOQ_MEDIA_PLAYER_END_REMOVED), .removed),
            (UInt32(MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE), .localFailure),
            (UInt32(MOQ_MEDIA_PLAYER_END_CLOSED), .closed),
            (UInt32(MOQ_MEDIA_PLAYER_END_FATAL), .fatal),
            (40, .unknown(40)),
        ]
        let dispositions: [(UInt32, PlayerEndDisposition)] = [
            (UInt32(MOQ_MEDIA_PLAYER_END_COMPLETE), .complete),
            (UInt32(MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF), .tailCutoff),
            (UInt32(MOQ_MEDIA_PLAYER_END_IMMEDIATE), .immediate),
            (41, .unknown(41)),
        ]
        for ((rawCause, cause), (rawDisposition, disposition)) in zip(causes, dispositions + dispositions) {
            var e = output(kind: UInt32(MOQ_MEDIA_PLAYER_OUTPUT_END))
            e.u.end.cause = rawCause
            e.u.end.disposition = rawDisposition
            guard case .end(let end) = copyPlayerOutput(e, track: videoTrack) else {
                Issue.record("expected an end")
                continue
            }
            #expect(end.cause == cause && end.disposition == disposition)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("An unknown kind, or a record without a track, surfaces as unknown")
    func unknowns() {
        guard case .unknown(let kind, let track) = copyPlayerOutput(
            output(kind: 77), track: videoTrack) else {
            Issue.record("expected unknown")
            return
        }
        #expect(kind == 77 && track == videoTrack)
        guard case .unknown(_, let none) = copyPlayerOutput(
            output(kind: UInt32(MOQ_MEDIA_PLAYER_OUTPUT_UNIT)), track: nil) else {
            Issue.record("expected unknown")
            return
        }
        #expect(none == nil)
    }
}

@Suite("Media player event copies")
struct PlayerEventCopyTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    private func event(_ kind: UInt32, _ fill: (inout moq_media_player_event_t) -> Void = { _ in })
        -> moq_media_player_event_t {
        var ev = moq_media_player_event_t()
        ev.struct_size = UInt32(MemoryLayout<moq_media_player_event_t>.size)
        ev.kind = kind
        fill(&ev)
        return ev
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Track events carry their details")
    func trackEvents() {
        let gap = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_GAP)) {
            $0.u.gap.group_id = 4; $0.u.gap.object_id = 2; $0.u.gap.crossed = true
        }, track: videoTrack)
        guard case .gap(let t, 4, 2, true) = gap else {
            Issue.record("gap: \(gap)")
            return
        }
        #expect(t == videoTrack)
        let skip = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_SKIP)) {
            $0.u.skip.from_group_id = 1; $0.u.skip.to_group_id = 3
        }, track: videoTrack)
        guard case .skip(_, 1, 3) = skip else {
            Issue.record("skip: \(skip)")
            return
        }
        let shed = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_SHED)) {
            $0.u.shed.dropped_groups = 2; $0.u.shed.remaining_groups = 3
        }, track: videoTrack)
        guard case .shed(_, 2, 3) = shed else {
            Issue.record("shed: \(shed)")
            return
        }
        let started = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_STARTED)) {
            $0.u.started.timeline_id = 5
        }, track: videoTrack)
        guard case .started(_, 5) = started else {
            Issue.record("started: \(started)")
            return
        }
        let forgotten = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_KNOWLEDGE_FORGOTTEN)) {
            $0.u.knowledge_forgotten.first_group_id = 7
            $0.u.knowledge_forgotten.last_group_id = 9
        }, track: videoTrack)
        guard case .knowledgeForgotten(_, 7, 9) = forgotten else {
            Issue.record("forgotten: \(forgotten)")
            return
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Player-wide events need no track; a drop may have none")
    func playerWide() {
        guard case .ready = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_READY)), track: nil),
              case .ended = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_ENDED)), track: nil),
              case .closed = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_CLOSED)), track: nil)
        else {
            Issue.record("lifecycle events")
            return
        }
        let drop = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_DROP)) {
            $0.u.drop.reason = UInt32(MOQ_MEDIA_PLAYER_DROP_OVERSIZE)
            $0.u.drop.group_id = 8
        }, track: nil)
        guard case .drop(nil, UInt32(MOQ_MEDIA_PLAYER_DROP_OVERSIZE), 8, 0) = drop else {
            Issue.record("drop: \(drop)")
            return
        }
        let sync = copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_SYNC_UNAVAILABLE)) {
            $0.u.sync.has_render_group = true
            $0.u.sync.render_group = -2
            $0.u.sync.reason = UInt32(MOQ_MEDIA_PLAYER_SYNC_NO_SHARED_EPOCH)
        }, track: nil)
        guard case .syncUnavailable(-2, UInt32(MOQ_MEDIA_PLAYER_SYNC_NO_SHARED_EPOCH)) = sync else {
            Issue.record("sync: \(sync)")
            return
        }
        let independent = copyPlayerEvent(
            event(UInt32(MOQ_MEDIA_PLAYER_EVENT_TIMING_INDEPENDENT)), track: nil)
        guard case .timingIndependent(nil) = independent else {
            Issue.record("independent: \(independent)")
            return
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("An unknown kind, or a track event without its track, is unknown")
    func unknowns() {
        guard case .unknown(90, nil) = copyPlayerEvent(event(90), track: nil),
              case .unknown(UInt32(MOQ_MEDIA_PLAYER_EVENT_STALL), nil) =
                copyPlayerEvent(event(UInt32(MOQ_MEDIA_PLAYER_EVENT_STALL)), track: nil)
        else {
            Issue.record("unknown events")
            return
        }
    }
}

@Suite("Media player staging")
struct PlayerStagingTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Bounded FIFO: room, the freeing pop, then the end after the items")
    func fifo() {
        let queue = StagedQueue<Int>(capacity: 2)
        #expect(queue.hasRoom && !queue.hasItemOrEnd)
        queue.push(1)
        queue.push(2)
        #expect(!queue.hasRoom)
        queue.finish(fatalCode: 5)
        guard case .item(1, freedCapacity: true) = queue.pop(),
              case .item(2, freedCapacity: false) = queue.pop(),
              case .failed(code: 5) = queue.pop() else {
            Issue.record("FIFO order, the freeing edge, then the failure")
            return
        }
        queue.finish(fatalCode: nil)                 /* the first end stands */
        guard case .failed(code: 5) = queue.pop() else {
            Issue.record("a second finish changed the end")
            return
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("One consumer at a time")
    func consumerClaim() {
        let queue = StagedQueue<Int>(capacity: 1)
        #expect(queue.claimConsumer())
        #expect(!queue.claimConsumer())
        queue.releaseConsumer()
        #expect(queue.claimConsumer())
        guard case .empty = queue.pop() else {
            Issue.record("expected empty")
            return
        }
        queue.finish(fatalCode: nil)
        guard case .finished = queue.pop() else {
            Issue.record("expected finished")
            return
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Long runs keep FIFO order across compaction")
    func compaction() {
        let queue = StagedQueue<Int>(capacity: 200)
        for i in 0..<150 { queue.push(i) }
        var seen: [Int] = []
        for _ in 0..<100 {
            if case .item(let v, _) = queue.pop() { seen.append(v) }
        }
        for i in 150..<180 { queue.push(i) }
        while case .item(let v, _) = queue.pop() { seen.append(v) }
        #expect(seen == Array(0..<180))
    }
}

@Suite("Media player timeline")
struct PlayerTimelineTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A presentation time maps through anchor and rate")
    func mapping() {
        let timeline = PlayerTimeline(
            track: videoTrack, timelineID: 1, epoch: 0, domain: .unixTime,
            anchorMedia: .seconds(10), anchorLocalMicroseconds: 5_000_000, rate: 1.0)
        #expect(timeline.localMicroseconds(forPresentation: .seconds(10)) == 5_000_000)
        #expect(timeline.localMicroseconds(forPresentation: .milliseconds(10_500)) == 5_500_000)
        #expect(timeline.localMicroseconds(forPresentation: .seconds(9)) == 4_000_000)
        let fast = PlayerTimeline(
            track: videoTrack, timelineID: 1, epoch: 0, domain: .unixTime,
            anchorMedia: .seconds(10), anchorLocalMicroseconds: 5_000_000, rate: 2.0)
        #expect(fast.localMicroseconds(forPresentation: .seconds(12)) == 6_000_000)
        /* Before the clock's origin clamps to it. */
        #expect(timeline.localMicroseconds(forPresentation: .zero) == 0)
    }
}

#if canImport(CoreMedia)
/* Minimal syntactically-valid avcC (CoreMedia stores atoms verbatim). */
private let playerAVCC = Data([
    0x01, 0x42, 0xC0, 0x1E, 0xFF,
    0xE1, 0x00, 0x04, 0x67, 0x42, 0xC0, 0x1E,
    0x01, 0x00, 0x04, 0x68, 0xCE, 0x3C, 0x80,
])

@Suite("Media player CoreMedia helpers")
struct PlayerCoreMediaTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    private func configuration(codecConfig: Data?, codec: String = "avc1.42c01e",
                               mediaType: MediaType? = .video) -> PlayerDecoderConfiguration {
        PlayerDecoderConfiguration(
            track: videoTrack, configurationGeneration: 1, timelineID: 1,
            mediaType: mediaType, codec: codec, codecConfig: codecConfig,
            timescale: nil, width: 640, height: 360, sampleRate: nil, channelCount: nil)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A configure becomes an H.264 description with its dimensions and avcC")
    func formatDescription() throws {
        let format = try configuration(codecConfig: playerAVCC).makeFormatDescription()
        #expect(CMFormatDescriptionGetMediaSubType(format) == kCMVideoCodecType_H264)
        let dims = CMVideoFormatDescriptionGetDimensions(format)
        #expect(dims.width == 640 && dims.height == 360)
        let atoms = CMFormatDescriptionGetExtension(
            format, extensionKey: kCMFormatDescriptionExtension_SampleDescriptionExtensionAtoms)
            as? [String: Any]
        #expect((atoms?["avcC"] as? Data) == playerAVCC)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("No decoder configuration, an unmodeled type or codec is refused")
    func refusals() {
        #expect(throws: MoQServiceError.self) {
            try configuration(codecConfig: nil).makeFormatDescription()
        }
        #expect(throws: MoQServiceError.unsupported) {
            try configuration(codecConfig: playerAVCC, mediaType: nil).makeFormatDescription()
        }
        #expect(throws: MoQServiceError.unsupported) {
            try configuration(codecConfig: Data([1]), codec: "opus",
                              mediaType: .audio).makeFormatDescription()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A unit becomes one timed sample; a dependent video unit is not sync")
    func sampleBuffer() throws {
        let format = try configuration(codecConfig: playerAVCC).makeFormatDescription()
        func unit(independent: Bool?) -> PlayerUnit {
            PlayerUnit(track: videoTrack, configurationGeneration: 1, timelineID: 1,
                       groupID: 0, objectID: 1, decodeTime: .microseconds(1_000),
                       presentationTime: .microseconds(2_000), isIndependent: independent,
                       sampleDuration: .microseconds(33_333), captureTime: nil,
                       data: Data([0, 0, 0, 1, 0x65]))
        }
        func notSync(_ sample: CMSampleBuffer) -> Bool {
            let attachments = CMSampleBufferGetSampleAttachmentsArray(
                sample, createIfNecessary: false) as? [[CFString: Any]]
            return (attachments?.first?[kCMSampleAttachmentKey_NotSync] as? Bool) == true
        }
        let dependent = try unit(independent: false).makeSampleBuffer(formatDescription: format)
        #expect(notSync(dependent))
        #expect(CMSampleBufferGetPresentationTimeStamp(dependent)
                == CMTime(value: 2_000, timescale: 1_000_000))
        /* A RAW unit's decode time copies its LOC media time: no decode
         * clock is asserted. */
        #expect(!CMSampleBufferGetDecodeTimeStamp(dependent).isValid)
        #expect(CMSampleBufferGetDuration(dependent)
                == CMTime(value: 33_333, timescale: 1_000_000))
        #expect(CMSampleBufferGetTotalSampleSize(dependent) == 5)
        #expect(!notSync(try unit(independent: true).makeSampleBuffer(formatDescription: format)))
        #expect(!notSync(try unit(independent: nil).makeSampleBuffer(formatDescription: format)))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("RAW B-frames keep their decode order and presentation times; no dts")
    func reorderedRawUnits() throws {
        let format = try configuration(codecConfig: playerAVCC).makeFormatDescription()
        /* Decode order I P B B, presenting at 0, 99, 33 and 66 ms, as the
         * player outputs them: each unit's decode time is its media time. */
        let presentation: [Int64] = [0, 99, 33, 66]
        var samples: [CMSampleBuffer] = []
        for (object, ms) in presentation.enumerated() {
            var out = output(kind: UInt32(MOQ_MEDIA_PLAYER_OUTPUT_UNIT))
            out.u.unit.group_id = 7
            out.u.unit.object_id = UInt64(object)
            out.u.unit.decode_time_us = UInt64(ms * 1_000)
            out.u.unit.presentation_time_us = UInt64(ms * 1_000)
            out.u.unit.independence_known = true
            out.u.unit.independent = object == 0
            out.u.unit.sample_duration_us = 33_000
            out.u.unit.payload = try makeRcbuf([0, 0, 0, 1, 0x65, UInt8(object)])
            out.u.unit.len = 6
            let copied = copyPlayerOutput(out, track: videoTrack)
            moq_media_player_output_cleanup(&out)
            guard case .unit(let unit) = copied else {
                Issue.record("expected a unit")
                return
            }
            #expect(unit.objectID == UInt64(object))
            #expect(unit.presentationTime == .milliseconds(ms))
            let sample = try unit.makeSampleBuffer(formatDescription: format)
            #expect(try sampleBytes(sample) == Data([0, 0, 0, 1, 0x65, UInt8(object)]))
            samples.append(sample)
        }
        #expect(samples.map { CMSampleBufferGetPresentationTimeStamp($0) }
                == presentation.map { CMTime(value: $0 * 1_000, timescale: 1_000_000) })
        #expect(samples.allSatisfy { !CMSampleBufferGetDecodeTimeStamp($0).isValid })
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A CMAF unit keeps its decode clock")
    func cmafUnitKeepsDecodeTime() throws {
        let format = try configuration(codecConfig: playerAVCC).makeFormatDescription()
        let cmaf = PlayerTrack(
            id: 0x2000,
            description: TrackDescription(name: "video", mediaType: .video, packaging: .cmaf))
        let unit = PlayerUnit(track: cmaf, configurationGeneration: 1, timelineID: 1,
                              groupID: 0, objectID: 1, decodeTime: .microseconds(1_000),
                              presentationTime: .microseconds(2_000), isIndependent: false,
                              sampleDuration: nil, captureTime: nil,
                              data: Data([0, 0, 0, 1, 0x65]))
        let sample = try unit.makeSampleBuffer(formatDescription: format)
        #expect(CMSampleBufferGetDecodeTimeStamp(sample)
                == CMTime(value: 1_000, timescale: 1_000_000))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    private func sampleBytes(_ sample: CMSampleBuffer) throws -> Data {
        let block = try #require(CMSampleBufferGetDataBuffer(sample))
        var out = Data(count: CMBlockBufferGetDataLength(block))
        let status = out.withUnsafeMutableBytes {
            CMBlockBufferCopyDataBytes(block, atOffset: 0, dataLength: $0.count,
                                       destination: $0.baseAddress!)
        }
        #expect(status == noErr)
        return out
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A length-prefixed unit whose NAL length is 256-511 is used byte for byte")
    func lengthPrefixedByteForByte() throws {
        let format = try configuration(codecConfig: playerAVCC).makeFormatDescription()
        for length in [256, 300, 511] {
            var bytes = Data([0, 0, UInt8(length >> 8), UInt8(length & 0xFF)])
            bytes.append(Data((0..<length).map { UInt8($0 & 0xFF) }))
            bytes.append(contentsOf: [0, 0, 0, 2, 0x09, 0x10])
            let unit = PlayerUnit(track: videoTrack, configurationGeneration: 1, timelineID: 1,
                                  groupID: 0, objectID: 0, decodeTime: .zero,
                                  presentationTime: .zero, isIndependent: true,
                                  sampleDuration: nil, captureTime: nil, data: bytes)
            let sample = try unit.makeSampleBuffer(formatDescription: format,
                                                   framing: .lengthPrefixed)
            #expect(try sampleBytes(sample) == bytes)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Annex-B start codes, 3- or 4-byte, become 4-byte NAL lengths")
    func annexBConversion() throws {
        /* AUD (4-byte code), SPS (3-byte code), IDR slice (4-byte code). */
        let annexB = Data([0, 0, 0, 1, 0x09, 0x10,
                           0, 0, 1, 0x67, 0x42, 0xC0,
                           0, 0, 0, 1, 0x65, 0x88, 0x84, 0x00])
        let converted = try #require(lengthPrefixedNALs(fromAnnexB: annexB)?.width(4))
        #expect(converted == Data([0, 0, 0, 2, 0x09, 0x10,
                                   0, 0, 0, 3, 0x67, 0x42, 0xC0,
                                   0, 0, 0, 4, 0x65, 0x88, 0x84, 0x00]))
        #expect(lengthPrefixedNALs(fromAnnexB: Data([0, 0, 0, 2, 0x09, 0x10])) == nil)
        #expect(nalLengthSize(of: try configuration(codecConfig: playerAVCC)
            .makeFormatDescription()) == 4)
        #expect(lengthPrefixedNALs(fromAnnexB: Data()) == nil)

        let format = try configuration(codecConfig: playerAVCC).makeFormatDescription()
        let unit = PlayerUnit(track: videoTrack, configurationGeneration: 1, timelineID: 1,
                              groupID: 0, objectID: 0, decodeTime: .zero, presentationTime: .zero,
                              isIndependent: true, sampleDuration: nil, captureTime: nil,
                              data: annexB)
        let sample = try unit.makeSampleBuffer(formatDescription: format, framing: .annexB)
        #expect(CMSampleBufferGetTotalSampleSize(sample) == converted.count)
        #expect(try sampleBytes(sample) == converted)
        let asDelivered = try unit.makeSampleBuffer(formatDescription: format)
        #expect(CMSampleBufferGetTotalSampleSize(asDelivered) == annexB.count)

        let prefixed = PlayerUnit(track: videoTrack, configurationGeneration: 1, timelineID: 1,
                                  groupID: 0, objectID: 0, decodeTime: .zero,
                                  presentationTime: .zero, isIndependent: true,
                                  sampleDuration: nil, captureTime: nil, data: converted)
        #expect(throws: MoQServiceError.self) {
            try prefixed.makeSampleBuffer(formatDescription: format, framing: .annexB)
        }
    }
    /// `config` with its lengthSizeMinusOne set to `width` - 1.
    private func avcC(width: Int) -> Data {
        var c = playerAVCC
        c[4] = 0xFC | UInt8(width - 1)
        return c
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    private func annexBUnit(_ data: Data) -> PlayerUnit {
        PlayerUnit(track: videoTrack, configurationGeneration: 1, timelineID: 1,
                   groupID: 0, objectID: 0, decodeTime: .zero, presentationTime: .zero,
                   isIndependent: true, sampleDuration: nil, captureTime: nil, data: data)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Annex-B units take the NAL length width their H.264 description declares")
    func annexBHonorsAvcCWidth() throws {
        /* A valid LOC unit: a 4-byte start code, then a 3-byte one. */
        let annexB = Data([0, 0, 0, 1, 0x09, 0x10, 0, 0, 1, 0x65, 0x88, 0x84])
        let expected: [Int: Data] = [
            1: Data([2, 0x09, 0x10, 3, 0x65, 0x88, 0x84]),
            2: Data([0, 2, 0x09, 0x10, 0, 3, 0x65, 0x88, 0x84]),
            4: Data([0, 0, 0, 2, 0x09, 0x10, 0, 0, 0, 3, 0x65, 0x88, 0x84]),
        ]
        for (width, bytes) in expected {
            let format = try configuration(codecConfig: avcC(width: width)).makeFormatDescription()
            #expect(nalLengthSize(of: format) == width)
            let sample = try annexBUnit(annexB).makeSampleBuffer(formatDescription: format,
                                                                  framing: .annexB)
            #expect(try sampleBytes(sample) == bytes, "width \(width)")
            /* Length-prefixed units are used as they are, whatever the width. */
            let plain = try annexBUnit(bytes).makeSampleBuffer(formatDescription: format)
            #expect(try sampleBytes(plain) == bytes)
        }
        /* A width CoreMedia does not use (3) names no framing. */
        let three = try configuration(codecConfig: avcC(width: 3)).makeFormatDescription()
        #expect(nalLengthSize(of: three) == nil)
        #expect(throws: MoQServiceError.self) {
            try annexBUnit(annexB).makeSampleBuffer(formatDescription: three, framing: .annexB)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A NAL unit too long for the declared width is refused, not truncated")
    func annexBLengthOverflow() throws {
        var annexB = Data([0, 0, 0, 1])
        annexB.append(Data(repeating: 0x41, count: 256))
        let one = try configuration(codecConfig: avcC(width: 1)).makeFormatDescription()
        #expect(throws: MoQServiceError.self) {
            try annexBUnit(annexB).makeSampleBuffer(formatDescription: one, framing: .annexB)
        }
        let two = try configuration(codecConfig: avcC(width: 2)).makeFormatDescription()
        let sample = try annexBUnit(annexB).makeSampleBuffer(formatDescription: two,
                                                              framing: .annexB)
        #expect(try sampleBytes(sample).prefix(2) == Data([1, 0]))
        var long = Data([0, 0, 0, 1])
        long.append(Data(repeating: 0x41, count: 65_536))
        #expect(throws: MoQServiceError.self) {
            try annexBUnit(long).makeSampleBuffer(formatDescription: two, framing: .annexB)
        }
        #expect(LengthPrefixedNALs(units: [Data(count: 255)]).width(1)?.first == 255)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("HEVC: the hvcC width is honored; a description without avcC/hvcC refuses Annex-B")
    func annexBHevcAndOtherCodecs() throws {
        /* A minimal hvcC (no arrays) declaring 2-byte lengths. */
        var hvcC = Data(count: 23)
        hvcC[0] = 1
        hvcC[21] = 0x0C | 0x01
        let format = try configuration(codecConfig: hvcC, codec: "hvc1.1.6.L93.B0")
            .makeFormatDescription()
        #expect(nalLengthSize(of: format) == 2)
        let annexB = Data([0, 0, 0, 1, 0x40, 0x01, 0, 0, 0, 1, 0x26, 0x01, 0xAF])
        let sample = try annexBUnit(annexB).makeSampleBuffer(formatDescription: format,
                                                              framing: .annexB)
        #expect(try sampleBytes(sample) == Data([0, 2, 0x40, 0x01, 0, 3, 0x26, 0x01, 0xAF]))

        var vp9: CMFormatDescription?
        #expect(CMVideoFormatDescriptionCreate(
            allocator: kCFAllocatorDefault, codecType: kCMVideoCodecType_VP9,
            width: 640, height: 360, extensions: nil, formatDescriptionOut: &vp9) == noErr)
        let other = try #require(vp9)
        #expect(nalLengthSize(of: other) == nil)
        #expect(throws: MoQServiceError.self) {
            try annexBUnit(annexB).makeSampleBuffer(formatDescription: other, framing: .annexB)
        }
    }
}
#endif
