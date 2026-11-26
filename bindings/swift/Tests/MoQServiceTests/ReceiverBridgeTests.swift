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
    @Test("The receiver bridge exposes the complete native configuration")
    func sizedConfiguration() {
        withReceiverCFG(.live(namespace: "live/cam1")) { cfg in
            #expect(cfg.struct_size == MemoryLayout<moq_media_receiver_cfg_t>.size)
            #expect(cfg.companion_profile == moq_swift_media_companion_auto())
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The receiver profile maps independently of its other settings")
    func companionProfileMapping() {
        var configuration = MediaReceiver.Configuration(
            namespace: "live/cam1", overflowPolicy: .flowControl(maxObjects: 4),
            autoSubscribe: false)
        for profile in MediaCompanionProfile.allCases {
            configuration.companionProfile = profile
            withReceiverCFG(configuration) { cfg in
                let expected: moq_media_companion_profile_t
                switch profile {
                case .automatic: expected = moq_swift_media_companion_auto()
                case .legacyMSF01LOC01: expected = moq_swift_media_companion_legacy()
                case .draft22MSF01LOC04Public: expected = moq_swift_media_companion_d22_public()
                case .draft22MSF01LOC05Public: expected = moq_swift_media_companion_d22_loc05_public()
                }
                #expect(cfg.companion_profile == expected)
                #expect(cfg.struct_size == MemoryLayout<moq_media_receiver_cfg_t>.size)
                #expect(cfg.time_mode == MOQ_MEDIA_TIME_RAW)
                #expect(cfg.auto_subscribe == false)
                #expect(cfg.overflow.max_objects == 4)
                #expect(cfg.overflow.policy == MOQ_MEDIA_OVERFLOW_FLOW_CONTROL)
            }
        }
    }

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
            #expect(cfg.companion_profile == moq_swift_media_companion_auto())
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
        /* Decoder extradata is the common span: no parsed CMAF record is
         * needed (a LOC track never has one). */
        desc.codec_config = arena.bytes("CFG")
        #expect(desc.cmaf == nil)

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
        #expect(copy?.codecConfig == Data("CFG".utf8))
        #expect(copy?.samplerate == nil)        /* has_samplerate false */
        #expect(copy?.role == nil)              /* empty spans become nil */
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A LOCMAF track is a CMAF track: the service delivers its Objects as CMAF")
    func locmafIsCMAF() {
        let arena = CBytesArena()
        var desc = moq_media_track_desc_t()
        desc.name = arena.bytes("video")
        desc.packaging = arena.bytes("locmaf")
        desc.info.media_type = MOQ_MEDIA_TYPE_VIDEO
        desc.info.packaging = MOQ_MEDIA_PACKAGING_LOCMAF
        desc.info.timescale = 90_000
        let copy = withExtendedLifetime(arena) { copyTrackDescription(desc) }
        #expect(copy?.packaging == .cmaf)

        /* Its Objects are canonical CMAF chunks: the fragment and its mdat. */
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

@Suite("MSF-02 description and track-event copies")
struct MSF02ReceiverCopyTests {

    /// A description stamped at the full size, naming a video track whose
    /// extension list holds `supplement` (when given).
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    private func description(
        _ arena: CBytesArena,
        extensions: UnsafePointer<moq_msf_track_ext_t>? = nil, count: Int = 0
    ) -> moq_media_track_desc_t {
        var desc = moq_media_track_desc_t()
        desc.struct_size = UInt32(MemoryLayout<moq_media_track_desc_t>.size)
        desc.name = arena.bytes("video")
        desc.info.media_type = MOQ_MEDIA_TYPE_VIDEO
        desc.info.packaging = MOQ_MEDIA_PACKAGING_RAW
        desc.extensions = extensions
        desc.extension_count = count
        return desc
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A description copies its generation, init source and encryption fields")
    func generationInitSourceAndEncryption() throws {
        let arena = CBytesArena()
        let supplement = UnsafeMutablePointer<moq_msf_track_supplement_t>.allocate(capacity: 1)
        defer { supplement.deallocate() }
        supplement.initialize(to: moq_msf_track_supplement_t())
        moq_msf_track_supplement_init_sized(
            supplement, MemoryLayout<moq_msf_track_supplement_t>.size)
        supplement.pointee.has_encryption_scheme = true
        supplement.pointee.encryption_scheme = arena.bytes("moq-secure-objects")
        supplement.pointee.has_cipher_suite = true
        supplement.pointee.cipher_suite = arena.bytes("aes-128-gcm-sha256")
        supplement.pointee.has_key_id = true
        supplement.pointee.key_id = arena.bytes("key-1")
        supplement.pointee.has_track_base_key = true
        supplement.pointee.track_base_key = arena.bytes("MTIzNDU2Nzg5MDEyMzQ1Ng==")
        let ext = UnsafeMutablePointer<moq_msf_track_ext_t>.allocate(capacity: 1)
        defer { ext.deallocate() }
        ext.initialize(to: moq_msf_track_ext_t())
        ext.pointee.kind = UInt32(MOQ_MSF_EXT_SUPPLEMENT)
        ext.pointee.u.supplement = UnsafePointer(supplement)

        var desc = description(arena, extensions: UnsafePointer(ext), count: 1)
        desc.config_generation = 2
        desc.init_source = moq_swift_media_init_source_track_property()
        desc.init_property_type = 0x0D
        let copy = try #require(withExtendedLifetime(arena) { copyTrackDescription(desc) })
        #expect(copy.configGeneration == 2)
        #expect(copy.initSource == .trackProperty)
        #expect(copy.initPropertyType == 0x0D)
        #expect(copy.encryptionScheme == "moq-secure-objects")
        #expect(copy.cipherSuite == "aes-128-gcm-sha256")
        #expect(copy.keyID == "key-1")
        #expect(copy.trackBaseKey?.value == "MTIzNDU2Nzg5MDEyMzQ1Ng==")
        #expect(!String(describing: copy).contains("MTIzNDU2Nzg5MDEyMzQ1Ng=="))

        /* Each field is gated by its own has_ flag; a present empty value
         * is empty, not absent. */
        supplement.pointee.has_cipher_suite = false
        supplement.pointee.key_id = moq_bytes_t(data: nil, len: 0)
        let partial = try #require(withExtendedLifetime(arena) { copyTrackDescription(desc) })
        #expect(partial.cipherSuite == nil)
        #expect(partial.keyID == "")
        #expect(partial.encryptionScheme == "moq-secure-objects")

        /* A record below its first published size is not read. */
        let v0 = MemoryLayout<moq_msf_track_supplement_t>.offset(
            of: \moq_msf_track_supplement_t.malformed)! + MemoryLayout<UInt32>.size
        supplement.pointee.struct_size = UInt32(v0) - 1
        let short = try #require(withExtendedLifetime(arena) { copyTrackDescription(desc) })
        #expect(short.encryptionScheme == nil && short.keyID == nil && short.trackBaseKey == nil)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Each init source maps; an unknown one is kept")
    func initSources() throws {
        let arena = CBytesArena()
        let cases: [(UInt32, InitSource)] = [
            (moq_swift_media_init_source_none(), .none),
            (moq_swift_media_init_source_inline(), .inline),
            (moq_swift_media_init_source_track_property(), .trackProperty),
            (moq_swift_media_init_source_object_property(), .objectProperty),
            (9, .unknown(9)),
        ]
        for (raw, expected) in cases {
            var desc = description(arena)
            desc.init_source = raw
            let copy = try #require(withExtendedLifetime(arena) { copyTrackDescription(desc) })
            #expect(copy.initSource == expected)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Fields the description's stamp does not cover stay nil")
    func stampGating() throws {
        let arena = CBytesArena()
        var desc = description(arena)
        desc.config_generation = 5
        desc.init_source = moq_swift_media_init_source_inline()
        desc.init_property_type = 7
        desc.struct_size = UInt32(MemoryLayout<moq_media_track_desc_t>.offset(
            of: \moq_media_track_desc_t.init_property_type)!)
        let short = try #require(withExtendedLifetime(arena) { copyTrackDescription(desc) })
        #expect(short.configGeneration == nil)
        #expect(short.initSource == nil)
        #expect(short.initPropertyType == nil)
        desc.struct_size = UInt32(MemoryLayout<moq_media_track_desc_t>.size)
        let full = try #require(withExtendedLifetime(arena) { copyTrackDescription(desc) })
        #expect(full.configGeneration == 5 && full.initSource == .inline)
        #expect(full.initPropertyType == 7)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A replacement's REMOVED names its successor; the ADDED names its predecessor")
    func replacementEvents() throws {
        let arena = CBytesArena()
        var desc = description(arena)
        desc.config_generation = 3
        let old = try #require(OpaquePointer(bitPattern: 0x1000))
        let new = try #require(OpaquePointer(bitPattern: 0x2000))
        let fullSize = UInt32(MemoryLayout<moq_media_track_event_t>.size)
        try withUnsafePointer(to: &desc) { descPointer in
            var removed = moq_media_track_event_t()
            removed.struct_size = fullSize
            removed.kind = MOQ_MEDIA_TRACK_REMOVED
            removed.track = old
            removed.replaced_by = new
            let r = try #require(copyReceiverTrackEvent(removed))
            #expect(r.handleID == 0x1000)
            #expect(r.replacedByHandleID == 0x2000)
            #expect(r.replacesHandleID == nil)

            var added = moq_media_track_event_t()
            added.struct_size = fullSize
            added.kind = MOQ_MEDIA_TRACK_ADDED
            added.track = new
            added.desc = descPointer
            added.replaces = old
            let a = try #require(copyReceiverTrackEvent(added))
            #expect(a.handleID == 0x2000)
            #expect(a.replacesHandleID == 0x1000)
            #expect(a.replacedByHandleID == nil)

            var updated = moq_media_track_event_t()
            updated.struct_size = fullSize
            updated.kind = MOQ_MEDIA_TRACK_UPDATED
            updated.track = new
            updated.desc = descPointer
            updated.config_generation = 3
            let u = try #require(copyReceiverTrackEvent(updated))
            #expect(u.configGeneration == 3)
            #expect(u.trackDescription?.configGeneration == 3)

            /* A stamp short of replaced_by carries neither link. */
            removed.struct_size = UInt32(MemoryLayout<moq_media_track_event_t>.offset(
                of: \moq_media_track_event_t.replaced_by)!)
            let short = try #require(copyReceiverTrackEvent(removed))
            #expect(short.replacedByHandleID == nil && short.replacesHandleID == nil)
        }
    }
}
