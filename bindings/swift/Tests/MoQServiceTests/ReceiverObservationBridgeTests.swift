import Testing
import Foundation
import CMoQService
@testable import MoQService

private func fieldEnd<Root, Field>(_ field: KeyPath<Root, Field>) -> UInt32 {
    UInt32(MemoryLayout<Root>.offset(of: field)! + MemoryLayout<Field>.size)
}

private func fullObject() -> moq_media_object_t {
    var object = moq_media_object_t()
    object.struct_size = UInt32(MemoryLayout<moq_media_object_t>.size)
    object.packaging = MOQ_MEDIA_PACKAGING_RAW
    object.metadata.struct_size = fieldEnd(\moq_media_metadata_t.reserved)
    object.metadata.loc.struct_size = fieldEnd(\moq_loc_metadata_t.audio_config)
    object.delivery_origin = MOQ_MEDIA_DELIVERY_LIVE
    return object
}

private final class ReleaseWitness {
    var count = 0
    var releasedOnCreatingThread = true
    let creatingThread = Thread.current

    func wrap(_ bytes: [UInt8]) throws -> OpaquePointer {
        let data = UnsafeMutablePointer<UInt8>.allocate(capacity: bytes.count)
        data.initialize(from: bytes, count: bytes.count)
        var ref: OpaquePointer?
        let result = moq_rcbuf_wrap(moq_alloc_default(), data, bytes.count, {
            context, bytes, _ in
            let witness = Unmanaged<ReleaseWitness>.fromOpaque(context!).takeUnretainedValue()
            witness.count += 1
            witness.releasedOnCreatingThread = witness.releasedOnCreatingThread
                && Thread.current === witness.creatingThread
            UnsafeMutablePointer(mutating: bytes!).deallocate()
        }, Unmanaged.passUnretained(self).toOpaque(), &ref)
        if result != moq_result_t(MOQ_OK) { data.deallocate() }
        #expect(result == moq_result_t(MOQ_OK))
        return try #require(ref)
    }
}

@Suite("Receiver observation bridge")
struct ReceiverObservationBridgeTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The Codec String is copied as exact bytes only when present and covered")
    func codecString() {
        let text: [UInt8] = Array("avc1.640028".utf8)
        text.withUnsafeBufferPointer { buffer in
            for (present, covered) in [(true, true), (false, true), (true, false)] {
                var object = fullObject()
                object.loc_ext_present = present ? moq_swift_loc_ext_has_codec_string() : 0
                object.codec_string = moq_bytes_t(data: buffer.baseAddress, len: buffer.count)
                if !covered {
                    object.struct_size = UInt32(MemoryLayout<moq_media_object_t>.offset(
                        of: \moq_media_object_t.loc_ext_present)!)
                }
                let copied = takeReceivedObject(&object)
                let expected: Data? = present && covered ? Data(text) : nil
                #expect(copied.metadata?.codecStringBytes == expected)
                #expect(copied.metadata?.codecString == (expected == nil ? nil : "avc1.640028"))
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Full-width wire identity and FILL zero survive the actual copy path")
    func identityAndFill() {
        var object = moq_media_object_t()
        object.struct_size = UInt32(MemoryLayout<moq_media_object_t>.size)
        object.packaging = MOQ_MEDIA_PACKAGING_RAW
        object.group_id = .max
        object.subgroup_id = 0
        object.has_subgroup = true
        object.object_id = .max
        object.publisher_priority = .max
        object.delivery_origin = MOQ_MEDIA_DELIVERY_FILL
        object.initiating_request_id = 0
        let copied = takeReceivedObject(&object)
        #expect(copied.wireIdentity == MediaObjectIdentity(
            groupID: .max, subgroupID: 0, objectID: .max, publisherPriority: .max))
        #expect(copied.delivery == .fill(initiatingRequestID: 0))
        #expect(copied.objectProperties == Data())
        #expect(copied.trackProperties == Data())
        #expect(object.struct_size == 0)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Old and partial identity/origin records remain unobserved")
    func outerExtents() {
        let identityEnd = fieldEnd(\moq_media_object_t.has_subgroup)
        let metadataEnd = fieldEnd(\moq_media_object_t.metadata)
        let trackRefEnd = fieldEnd(\moq_media_object_t.track_properties_ref)
        let originEnd = fieldEnd(\moq_media_object_t.initiating_request_id)
        let v0End = fieldEnd(\moq_media_object_t.samples_owned)
        for size in [v0End, identityEnd - 1, identityEnd,
                     metadataEnd - 1, metadataEnd, trackRefEnd - 1,
                     trackRefEnd, originEnd - 1, originEnd] {
            var object = fullObject()
            object.struct_size = size
            object.group_id = .max
            object.subgroup_id = 42
            object.object_id = 0
            object.has_subgroup = false
            object.metadata.time_domain = .max
            object.delivery_origin = MOQ_MEDIA_DELIVERY_FILL
            object.initiating_request_id = .max
            let copied = takeReceivedObject(&object)
            #expect((copied.wireIdentity != nil) == (size >= identityEnd))
            #expect(copied.wireIdentity?.subgroupID == nil)
            #expect((copied.metadata != nil) == (size >= metadataEnd))
            #expect((copied.trackProperties != nil) == (size >= trackRefEnd))
            #expect((copied.delivery != nil) == (size >= originEnd))
            #expect(copied.objectProperties == Data())
            if size >= identityEnd {
                #expect(copied.wireIdentity?.groupID == .max)
                #expect(copied.wireIdentity?.objectID == 0)
            }
            if size >= originEnd { #expect(copied.delivery == .fill(initiatingRequestID: .max)) }
            #expect(object.struct_size == 0)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Metadata and LOC require their complete independent nested floors")
    func nestedExtents() {
        let metadataEnd = fieldEnd(\moq_media_metadata_t.reserved)
        let locEnd = fieldEnd(\moq_loc_metadata_t.audio_config)
        for metadataSize in [UInt32(0), metadataEnd - 1, metadataEnd, metadataEnd + 8] {
            for locSize in [UInt32(0), locEnd - 1, locEnd, locEnd + 8] {
                var object = fullObject()
                object.metadata.struct_size = metadataSize
                object.metadata.loc.struct_size = locSize
                object.metadata.time_domain = .max
                object.metadata.loc.present = 0x8000_0000
                let copied = takeReceivedObject(&object)
                #expect((copied.metadata != nil) == (metadataSize >= metadataEnd))
                #expect((copied.metadata?.loc != nil)
                    == (metadataSize >= metadataEnd && locSize >= locEnd))
                if metadataSize >= metadataEnd {
                    #expect(copied.metadata?.timeDomain == .unknown(.max))
                    #expect(copied.metadata?.timeMicroseconds == nil)
                    #expect(copied.metadata?.isIndependent == nil)
                }
                if metadataSize >= metadataEnd && locSize >= locEnd {
                    #expect(copied.metadata?.loc?.rawPresence == 0x8000_0000)
                }
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("LIVE and unknown origins never fabricate a valid initiating ID")
    func origins() {
        for requestID in [UInt64(0), UInt64.max] {
            var fill = fullObject()
            fill.delivery_origin = MOQ_MEDIA_DELIVERY_FILL
            fill.initiating_request_id = requestID
            let copiedFill = takeReceivedObject(&fill)
            #expect(copiedFill.delivery == .fill(initiatingRequestID: requestID))
            #expect(copiedFill.delivery?.initiatingRequestID == requestID)
        }
        var live = fullObject()
        let copiedLive = takeReceivedObject(&live)
        #expect(copiedLive.delivery == .live)
        #expect(copiedLive.delivery?.initiatingRequestID == nil)
        for raw in [UInt32(0), UInt32.max] {
            var unknown = fullObject()
            unknown.delivery_origin = raw
            unknown.initiating_request_id = .max
            let copiedUnknown = takeReceivedObject(&unknown)
            #expect(copiedUnknown.delivery == .unknown(rawOrigin: raw, rawInitiatingRequestID: .max))
            #expect(copiedUnknown.delivery?.initiatingRequestID == nil)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Native spans become independent Data before exactly one stamped release")
    func ownedCopies() throws {
        let payloadWitness = ReleaseWitness()
        let objectWitness = ReleaseWitness()
        let trackWitness = ReleaseWitness()
        let payload = [UInt8](repeating: 0x33, count: 73)
        let objectProperties = [UInt8](repeating: 0x44, count: 79)
        let trackProperties = [UInt8](repeating: 0x55, count: 83)
        var object = fullObject()
        object.payload_ref = try payloadWitness.wrap(payload)
        object.properties_ref = try objectWitness.wrap(objectProperties)
        object.track_properties_ref = try trackWitness.wrap(trackProperties)
        object.payload = moq_bytes_t(data: moq_rcbuf_data(object.payload_ref), len: payload.count)
        object.presentation_time_us = .max
        object.decode_time_us = 0
        object.composition_offset_us = .min
        object.has_capture_time = true
        object.capture_time_us = 1_000_000
        let copied = takeReceivedObject(&object)
        #expect(payloadWitness.count == 1)
        #expect(objectWitness.count == 1)
        #expect(trackWitness.count == 1)
        #expect(payloadWitness.releasedOnCreatingThread)
        #expect(objectWitness.releasedOnCreatingThread)
        #expect(trackWitness.releasedOnCreatingThread)
        #expect(copied.storage.withMediaBytes { Data($0) } == Data(payload))
        #expect(copied.objectProperties == Data(objectProperties))
        #expect(copied.trackProperties == Data(trackProperties))
        #expect(copied.presentationTime == .microseconds(UInt64.max))
        #expect(copied.decodeTime == .zero)
        #expect(copied.compositionOffset == .microseconds(Int64.min))
        #expect(copied.captureTime == Date(timeIntervalSince1970: 1))
        #expect(object.payload_ref == nil)
        #expect(object.properties_ref == nil)
        #expect(object.track_properties_ref == nil)
        copied.storage.cleanup()
        #expect(payloadWitness.count == 1)
        #expect(objectWitness.count == 1)
        #expect(trackWitness.count == 1)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Partial ref tail neither reads nor cleans an unstamped track ref")
    func partialRef() throws {
        let witness = ReleaseWitness()
        let ref = try witness.wrap([1, 2, 3])
        var object = fullObject()
        object.struct_size = fieldEnd(\moq_media_object_t.track_properties_ref) - 1
        object.track_properties_ref = ref
        let copied = takeReceivedObject(&object)
        #expect(copied.trackProperties == nil)
        #expect(copied.delivery == nil)
        #expect(witness.count == 0)
        moq_rcbuf_decref(ref)
        #expect(witness.count == 1)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("LOC presence preserves zero/MAX, marking bytes and unknown bits")
    func locValues() throws {
        let flags = moq_swift_loc_metadata_has_timestamp()
            | moq_swift_loc_metadata_has_timescale()
            | moq_swift_loc_metadata_has_marking()
            | moq_swift_loc_metadata_has_audio_level()
            | moq_swift_loc_metadata_has_video_config()
            | moq_swift_loc_metadata_has_audio_config()
        for raw in [UInt64(0), UInt64.max] {
            for markingLength in 1...4 {
                var object = fullObject()
                object.metadata.loc.present = flags | 0x8000_0000
                object.metadata.loc.timestamp = raw
                object.metadata.loc.timescale = raw
                object.metadata.loc.marking_len = UInt8(markingLength)
                object.metadata.loc.marking = (0, 0x80, 0xff, 1)
                object.metadata.loc.audio_level = .max
                let copied = takeReceivedObject(&object)
                let loc = try #require(copied.metadata?.loc)
                #expect(loc.rawPresence == flags | 0x8000_0000)
                #expect(loc.timestamp == raw)
                #expect(loc.timescale == raw)
                #expect(loc.marking == Data([0, 0x80, 0xff, 1].prefix(markingLength)))
                #expect(loc.audioLevel == .max)
                #expect(loc.videoConfig == Data())
                #expect(loc.audioConfig == Data())
                #expect(copied.metadata?.timeMicroseconds == nil)
                #expect(copied.captureTime == nil)
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Unselected LOC fields are absent despite nonzero raw storage")
    func absentLOCValues() throws {
        var object = fullObject()
        object.metadata.loc.present = 0
        object.metadata.loc.timestamp = .max
        object.metadata.loc.timescale = .max
        object.metadata.loc.marking_len = 4
        object.metadata.loc.marking = (0xff, 0xff, 0xff, 0xff)
        object.metadata.loc.audio_level = .max
        object.metadata.time_us = .max
        object.metadata.independent = 1
        let loc = try #require(takeReceivedObject(&object).metadata?.loc)
        #expect(loc.rawPresence == 0)
        #expect(loc.timestamp == nil)
        #expect(loc.timescale == nil)
        #expect(loc.marking == nil)
        #expect(loc.audioLevel == nil)
        #expect(loc.videoConfig == nil)
        #expect(loc.audioConfig == nil)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Every presence bit independently selects its own field")
    func individualPresence() throws {
        let flags = [moq_swift_loc_metadata_has_timestamp(),
                     moq_swift_loc_metadata_has_timescale(),
                     moq_swift_loc_metadata_has_marking(),
                     moq_swift_loc_metadata_has_audio_level(),
                     moq_swift_loc_metadata_has_video_config(),
                     moq_swift_loc_metadata_has_audio_config()]
        for selected in flags.indices {
            var object = fullObject()
            object.metadata.loc.present = flags[selected]
            object.metadata.loc.marking_len = 1
            let loc = try #require(takeReceivedObject(&object).metadata?.loc)
            let observed = [loc.timestamp != nil, loc.timescale != nil,
                            loc.marking != nil, loc.audioLevel != nil,
                            loc.videoConfig != nil, loc.audioConfig != nil]
            #expect(observed == flags.indices.map { $0 == selected })
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Raw domain and normalized-time presence do not infer capture time")
    func timeDomains() throws {
        let domains: [(UInt32, MediaTimeDomain)] = [
            (moq_swift_media_time_domain_none(), .none),
            (moq_swift_media_time_domain_unix_us(), .unixMicroseconds),
            (moq_swift_media_time_domain_application_media(), .applicationMedia),
            (.max, .unknown(.max))]
        for (rawDomain, expectedDomain) in domains {
            for rawTime in [UInt64(0), UInt64.max] {
                for hasTime in [false, true] {
                    var object = fullObject()
                    object.metadata.time_domain = rawDomain
                    object.metadata.has_time_us = hasTime ? 1 : 0
                    object.metadata.time_us = rawTime
                    object.metadata.independence_known = hasTime ? 1 : 0
                    object.metadata.independent = 1
                    let copied = takeReceivedObject(&object)
                    let metadata = try #require(copied.metadata)
                    #expect(metadata.timeDomain == expectedDomain)
                    #expect(metadata.timeMicroseconds == (hasTime ? rawTime : nil))
                    #expect(metadata.isIndependent == (hasTime ? true : nil))
                    #expect(copied.captureTime == nil)
                    #expect(copied.presentationTime == .zero)
                    #expect(copied.decodeTime == .zero)
                }
            }
        }
        var dependent = fullObject()
        dependent.metadata.independence_known = 1
        #expect(takeReceivedObject(&dependent).metadata?.isIndependent == false)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Effective object and inherited configs survive native release and task handoff")
    func ownedConfigCopies() async throws {
        let objectBytes = Array(UInt8(0)...UInt8(95))
        let trackBytes = Array(UInt8(128)...UInt8(223))
        for inheritedVideo in [false, true] {
            let objectWitness = ReleaseWitness()
            let trackWitness = ReleaseWitness()
            var object = fullObject()
            object.properties_ref = try objectWitness.wrap(objectBytes)
            object.track_properties_ref = try trackWitness.wrap(trackBytes)
            let objectBase = moq_rcbuf_data(object.properties_ref)!
            let trackBase = moq_rcbuf_data(object.track_properties_ref)!
            object.metadata.loc.present = moq_swift_loc_metadata_has_video_config()
                | moq_swift_loc_metadata_has_audio_config()
            object.metadata.loc.video_config = moq_bytes_t(
                data: (inheritedVideo ? trackBase : objectBase) + 7, len: 67)
            object.metadata.loc.audio_config = moq_bytes_t(
                data: (inheritedVideo ? objectBase : trackBase) + 11, len: 59)
            let copied = takeReceivedObject(&object)
            #expect(objectWitness.count == 1)
            #expect(trackWitness.count == 1)
            #expect(objectWitness.releasedOnCreatingThread)
            #expect(trackWitness.releasedOnCreatingThread)
            #expect(object.struct_size == 0)
            let transferred = await Task.detached { copied }.value
            let loc = try #require(transferred.metadata?.loc)
            let videoBytes = inheritedVideo ? trackBytes : objectBytes
            let audioBytes = inheritedVideo ? objectBytes : trackBytes
            #expect(loc.videoConfig == Data(videoBytes[7..<74]))
            #expect(loc.audioConfig == Data(audioBytes[11..<70]))
            #expect(transferred.objectProperties == Data(objectBytes))
            #expect(transferred.trackProperties == Data(trackBytes))
            transferred.storage.cleanup()
            #expect(objectWitness.count == 1)
            #expect(trackWitness.count == 1)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Present empty object config is not replaced by nonempty inherited bytes")
    func emptyConfigOverride() throws {
        let witness = ReleaseWitness()
        let trackBytes = [UInt8](repeating: 0x55, count: 64)
        var object = fullObject()
        object.track_properties_ref = try witness.wrap(trackBytes)
        object.metadata.loc.present = moq_swift_loc_metadata_has_video_config()
        object.metadata.loc.video_config = moq_bytes_t(data: nil, len: 0)
        let copied = takeReceivedObject(&object)
        #expect(copied.metadata?.loc?.videoConfig == Data())
        #expect(copied.metadata?.loc?.audioConfig == nil)
        #expect(copied.trackProperties == Data(trackBytes))
        #expect(witness.count == 1)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Complete metadata can copy object-backed config before the ref tail exists")
    func configBeforeTrackRef() throws {
        let witness = ReleaseWitness()
        let bytes = [UInt8](repeating: 0x77, count: 64)
        var object = fullObject()
        object.struct_size = fieldEnd(\moq_media_object_t.metadata)
        object.properties_ref = try witness.wrap(bytes)
        object.metadata.loc.present = moq_swift_loc_metadata_has_audio_config()
        object.metadata.loc.audio_config = moq_bytes_t(
            data: moq_rcbuf_data(object.properties_ref), len: bytes.count)
        let copied = takeReceivedObject(&object)
        #expect(copied.metadata?.loc?.audioConfig == Data(bytes))
        #expect(copied.objectProperties == Data(bytes))
        #expect(copied.trackProperties == nil)
        #expect(copied.delivery == nil)
        #expect(witness.count == 1)
    }
}
