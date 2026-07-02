import Testing
import Foundation
import CMoQService
@testable import MoQService

/* Deterministic, NO-SOCKET receiver-bridge tests: configuration mapping
 * into the real C structs, description deep-copying, and the copy-out
 * object payload mapping -- all against fabricated C values (no receiver is
 * created; moq_media_object_cleanup on refless fabricated objects is a
 * documented no-op). */

@Suite("Receiver configuration mapping")
struct ReceiverConfigurationMappingTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The model configuration maps into moq_media_receiver_cfg_t")
    func fullMapping() throws {
        var configuration = MediaReceiver.Configuration(
            namespace: "live/cam1",
            overflowPolicy: .flowControl(maxObjects: 128, maxBytes: 1 << 20),
            autoSubscribe: false)
        configuration.timeMode = .sharedEpoch
        configuration.catalogTrack = "alt-catalog"

        withReceiverCFG(configuration) { cfg in
            #expect(cfg.namespace_.count == 2)
            #expect(swiftString(cfg.namespace_.parts![0]) == "live")
            #expect(swiftString(cfg.namespace_.parts![1]) == "cam1")
            #expect(swiftString(cfg.catalog_track) == "alt-catalog")
            #expect(cfg.auto_subscribe == false)
            #expect(cfg.time_mode == MOQ_MEDIA_TIME_SHARED_EPOCH)
            #expect(cfg.overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL)
            #expect(cfg.overflow.max_objects == 128)
            #expect(cfg.overflow.max_bytes == 1 << 20)
            #expect(cfg.endpoint == nil)        /* attach borrows */
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Presets map to their C policies with library defaults (0)")
    func presetMapping() throws {
        withReceiverCFG(.live(namespace: "live/cam1")) { cfg in
            #expect(cfg.overflow.policy == MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME)
            #expect(cfg.overflow.max_objects == 0)  /* 0 = C default (256) */
            #expect(cfg.overflow.max_bytes == 0)    /* 0 = C default (32 MiB) */
            #expect(cfg.auto_subscribe == true)
            #expect(cfg.time_mode == MOQ_MEDIA_TIME_RAW)
            #expect(cfg.catalog_track.len == 0)     /* MSF default "catalog" */
        }
        withReceiverCFG(.flowControl(namespace: "vod/movie")) { cfg in
            #expect(cfg.overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL)
        }
    }
}

@Suite("Track description copying")
struct TrackDescriptionCopyTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A C description deep-copies field-for-field")
    func deepCopy() throws {
        let arena = CBytesArena()
        var desc = moq_media_track_desc_t()
        desc.name = arena.bytes("video")
        desc.codec = arena.bytes("avc1.64001f")
        desc.lang = arena.bytes("en")
        desc.info.media_type = MOQ_MEDIA_TYPE_VIDEO
        desc.info.packaging = MOQ_MEDIA_PACKAGING_CMAF
        desc.info.timescale = 90_000
        desc.has_width = true
        desc.width = 1280
        desc.has_height = true
        desc.height = 720
        desc.has_bitrate = true
        desc.bitrate = 3_000_000
        desc.is_live = false
        desc.has_track_duration = true
        desc.track_duration_ms = 90_000
        desc.init_data = arena.bytes("INIT")

        let copy = withExtendedLifetime(arena) { copyTrackDescription(desc) }
        #expect(copy?.name == "video")
        #expect(copy?.mediaType == .video)
        #expect(copy?.packaging == .cmaf)
        #expect(copy?.codec == "avc1.64001f")
        #expect(copy?.language == "en")
        #expect(copy?.width == 1280)
        #expect(copy?.height == 720)
        #expect(copy?.bitrate == 3_000_000)
        #expect(copy?.timescale == 90_000)
        #expect(copy?.isLive == false)
        #expect(copy?.trackDuration == .seconds(90))
        #expect(copy?.initData == Data("INIT".utf8))
        #expect(copy?.samplerate == nil)        /* has_samplerate false */
        #expect(copy?.role == nil)              /* empty spans become nil */
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Non-media tracks (timeline et al.) are outside the v1 model")
    func nonMediaSkipped() {
        /* A zeroed desc models a timeline/unknown track: the C enum has no
         * zero member (VIDEO=1, AUDIO=2), and non-media tracks leave
         * info.media_type unset. */
        let desc = moq_media_track_desc_t()
        #expect(copyTrackDescription(desc) == nil)
    }
}

@Suite("Object payload copy-out")
struct ObjectCopyOutTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("RAW objects copy the payload; the fragment stays empty")
    func rawCopyOut() {
        let arena = CBytesArena()
        var object = moq_media_object_t()
        object.packaging = MOQ_MEDIA_PACKAGING_RAW
        object.payload = arena.bytes("frame-bytes")
        let (media, fragment) = withExtendedLifetime(arena) {
            copyObjectPayload(object)
        }
        #expect(media == Data("frame-bytes".utf8))
        #expect(fragment.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("CMAF objects copy the full fragment and slice the mdat as media")
    func cmafCopyOut() {
        let arena = CBytesArena()
        var object = moq_media_object_t()
        object.packaging = MOQ_MEDIA_PACKAGING_CMAF
        object.fragment = arena.bytes("moofHEADmdatDATA")
        object.mdat_offset = 12
        object.mdat_len = 4
        let (media, fragment) = withExtendedLifetime(arena) {
            copyObjectPayload(object)
        }
        #expect(fragment == Data("moofHEADmdatDATA".utf8))
        #expect(media == Data("DATA".utf8))

        /* Out-of-bounds mdat metadata degrades to empty media, never a
         * wild read. */
        object.mdat_offset = 100
        let (badMedia, _) = withExtendedLifetime(arena) {
            copyObjectPayload(object)
        }
        #expect(badMedia.isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Copied storage serves both byte surfaces and needs no cleanup")
    func copiedStorage() {
        let storage = CopiedObjectStorage(
            media: Data([1, 2]), fragment: Data([3, 4, 5]))
        #expect(storage.mediaByteCount == 2)
        #expect(storage.fragmentByteCount == 3)
        #expect(storage.withMediaBytes { Data($0) } == Data([1, 2]))
        #expect(storage.withFragmentBytes { Data($0) } == Data([3, 4, 5]))
        storage.cleanup()
        storage.cleanup()                       /* trivially idempotent */
        #expect(storage.withMediaBytes { Data($0) } == Data([1, 2]))
    }
}
