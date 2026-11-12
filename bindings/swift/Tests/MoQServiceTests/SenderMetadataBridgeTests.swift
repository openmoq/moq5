import Testing
import Foundation
import CMoQService
@testable import MoQService
import MoQServiceCore

private final class SenderBufferAllocator {
    var attempts = 0
    var frees = 0
    var failAt: Int?
    var live: Set<UInt> = []
    var thread: Thread?
    var wrongThread = false

    private func noteThread() {
        if let thread { wrongThread = wrongThread || thread !== Thread.current }
        else { thread = Thread.current }
    }

    func withAllocator<T>(_ body: (UnsafePointer<moq_alloc_t>) -> T) -> T {
        var allocator = moq_alloc_t()
        allocator.ctx = Unmanaged.passUnretained(self).toOpaque()
        allocator.alloc = { size, context in
            let owner = Unmanaged<SenderBufferAllocator>.fromOpaque(context!).takeUnretainedValue()
            owner.noteThread()
            owner.attempts += 1
            if owner.attempts == owner.failAt { return nil }
            let pointer = UnsafeMutableRawPointer.allocate(
                byteCount: size, alignment: MemoryLayout<UInt64>.alignment)
            owner.live.insert(UInt(bitPattern: pointer))
            return pointer
        }
        allocator.free = { pointer, _, context in
            let owner = Unmanaged<SenderBufferAllocator>.fromOpaque(context!).takeUnretainedValue()
            owner.noteThread()
            owner.frees += 1
            owner.live.remove(UInt(bitPattern: pointer))
            pointer?.deallocate()
        }
        return withExtendedLifetime(self) {
            withUnsafePointer(to: allocator, body)
        }
    }
}

private func spanData(_ bytes: moq_bytes_t) -> Data {
    guard bytes.len > 0, let pointer = bytes.data else { return Data() }
    return Data(bytes: pointer, count: bytes.len)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func checkMetadata(_ pointer: UnsafePointer<moq_media_metadata_t>?,
                           _ expected: MediaLOCMetadata) {
    #expect(pointer != nil)
    guard let pointer else { return }
    let value = pointer.pointee
    #expect(value.struct_size == MemoryLayout<moq_media_metadata_t>.size)
    #expect(value.loc.struct_size == MemoryLayout<moq_loc_metadata_t>.size)
    #expect(value.time_domain == moq_swift_media_time_domain_none())
    #expect(value.time_us == 0)
    #expect(value.has_time_us == 0)
    #expect(value.independence_known == 0)
    #expect(value.independent == 0)
    #expect(withUnsafeBytes(of: value.reserved) { $0.allSatisfy { $0 == 0 } })
    #expect(withUnsafeBytes(of: value.loc.reserved) { $0.allSatisfy { $0 == 0 } })
    #expect(value.loc.present == expected.rawPresence)
    #expect(value.loc.timestamp == expected.timestamp ?? 0)
    #expect(value.loc.timescale == expected.timescale ?? 0)
    #expect(value.loc.audio_level == expected.audioLevel ?? 0)
    #expect(Int(value.loc.marking_len) == expected.marking?.count ?? 0)
    let marking = withUnsafeBytes(of: value.loc.marking) { Data($0) }
    let selected = expected.marking ?? Data()
    #expect(marking.prefix(selected.count) == selected)
    #expect(marking.dropFirst(selected.count).allSatisfy { $0 == 0 })
    #expect(spanData(value.loc.video_config) == expected.videoConfig ?? Data())
    #expect(spanData(value.loc.audio_config) == expected.audioConfig ?? Data())
    if expected.videoConfig == nil { #expect(value.loc.video_config.data == nil) }
    if expected.audioConfig == nil { #expect(value.loc.audio_config.data == nil) }
}

// Only copied Swift values cross this condition; C records/rcbufs stay on the
// worker for allocation, synchronous call inspection, and final release.
private final class SenderCallBarrier: @unchecked Sendable {
    private let condition = NSCondition()
    private var entered = false
    private var released = false
    private var result: [Data]?

    func enter() -> Bool {
        condition.lock(); defer { condition.unlock() }
        entered = true
        condition.broadcast()
        let deadline = Date().addingTimeInterval(10)
        while !released {
            if !condition.wait(until: deadline) { return false }
        }
        return true
    }

    func awaitEntry() -> Bool {
        condition.lock(); defer { condition.unlock() }
        let deadline = Date().addingTimeInterval(10)
        while !entered {
            if !condition.wait(until: deadline) { return false }
        }
        return true
    }

    func release() {
        condition.lock(); defer { condition.unlock() }
        released = true
        condition.broadcast()
    }

    func finish(_ result: [Data]) {
        condition.lock(); defer { condition.unlock() }
        self.result = result
        condition.broadcast()
    }

    func awaitResult() -> [Data]? {
        condition.lock(); defer { condition.unlock() }
        let deadline = Date().addingTimeInterval(10)
        while result == nil {
            if !condition.wait(until: deadline) { return nil }
        }
        return result
    }
}

@Suite("Sender metadata bridge")
struct SenderMetadataBridgeTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The production write marshaller forwards explicit LOC metadata")
    func explicitMetadata() {
        let object = OutgoingObject(payload: Data([1]), locMetadata: .init(
            rawPresence: moq_swift_loc_metadata_has_timestamp(), timestamp: 0))
        var calls = 0
        let result = writeSenderObject(object) { send in
            calls += 1
            #expect(send.pointee.metadata != nil)
            return moq_result_t(MOQ_ERR_WOULD_BLOCK)
        }
        #expect(calls == 1)
        guard case .wouldBlock = result else {
            Issue.record("Unexpected write result")
            return
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A Codec String reaches the service as its exact bytes, borrowed for the call")
    func codecString() {
        let exact = Data([0x61, 0x76, 0x63, 0x31, 0xc3, 0xa9])   // "avc1" + U+00E9
        var object = OutgoingObject(payload: Data())
        object.codecStringBytes = exact
        var calls = 0
        _ = writeSenderObject(object) { send in
            calls += 1
            let ext = send.pointee.loc_ext
            #expect(ext != nil)
            if let ext {
                #expect(ext.pointee.struct_size == MemoryLayout<moq_loc_ext_t>.size)
                #expect(ext.pointee.present == moq_swift_loc_ext_has_codec_string())
                #expect(spanData(ext.pointee.codec_string) == exact)
            }
            return moq_result_t(MOQ_ERR_INVAL)
        }
        #expect(calls == 1)
        _ = writeSenderObject(OutgoingObject(payload: Data())) { send in
            #expect(send.pointee.loc_ext == nil)
            return moq_result_t(MOQ_ERR_INVAL)
        }
        object.codecString = "opus"
        #expect(object.codecStringBytes == Data("opus".utf8))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("All six independent presence bits and combinations retain zero/MAX")
    func presenceMatrix() {
        let bits = [moq_swift_loc_metadata_has_timestamp(), moq_swift_loc_metadata_has_timescale(),
                    moq_swift_loc_metadata_has_marking(), moq_swift_loc_metadata_has_audio_level(),
                    moq_swift_loc_metadata_has_video_config(), moq_swift_loc_metadata_has_audio_config()]
        for mask in 0..<64 {
            for maximum in [false, true] {
                let present = bits.enumerated().reduce(UInt32(0)) {
                    $0 | (mask & (1 << $1.offset) != 0 ? $1.element : 0)
                }
                let metadata = MediaLOCMetadata(
                    rawPresence: present,
                    timestamp: mask & 1 != 0 ? (maximum ? .max : 0) : nil,
                    timescale: mask & 2 != 0 ? (maximum ? .max : 0) : nil,
                    marking: mask & 4 != 0 ? Data([0, 255, 0, 66]) : nil,
                    audioLevel: mask & 8 != 0 ? (maximum ? .max : 0) : nil,
                    videoConfig: mask & 16 != 0 ? Data([0, 255, 86, 0]) : nil,
                    audioConfig: mask & 32 != 0 ? Data() : nil)
                var calls = 0
                _ = writeSenderObject(.init(payload: Data(), locMetadata: metadata)) { send in
                    calls += 1
                    #expect(send.pointee.struct_size == MemoryLayout<moq_media_send_object_t>.size)
                    checkMetadata(send.pointee.metadata, metadata)
                    return moq_result_t(MOQ_ERR_INVAL)
                }
                #expect(calls == 1)
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Configs distinguish absent, empty, and nonempty; marking keeps 1..4 exact bytes")
    func byteShapes() {
        let configs: [Data?] = [nil, Data(), Data([0, 255, 0, 65])]
        for video in configs {
            for audio in configs {
                for count in 1...4 {
                    let present = moq_swift_loc_metadata_has_marking()
                        | (video != nil ? moq_swift_loc_metadata_has_video_config() : 0)
                        | (audio != nil ? moq_swift_loc_metadata_has_audio_config() : 0)
                    let metadata = MediaLOCMetadata(rawPresence: present,
                        marking: Data([0, 255, 0, 65].prefix(count)),
                        videoConfig: video, audioConfig: audio)
                    var calls = 0
                    _ = writeSenderObject(.init(payload: Data(), locMetadata: metadata)) { send in
                        calls += 1
                        checkMetadata(send.pointee.metadata, metadata)
                        return moq_result_t(MOQ_ERR_INVAL)
                    }
                    #expect(calls == 1)
                }
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Unknown bits, both mismatch directions, and bad marking allocate nothing")
    func invalidShapes() {
        let bits = [moq_swift_loc_metadata_has_timestamp(), moq_swift_loc_metadata_has_timescale(),
                    moq_swift_loc_metadata_has_marking(), moq_swift_loc_metadata_has_audio_level(),
                    moq_swift_loc_metadata_has_video_config(), moq_swift_loc_metadata_has_audio_config()]
        var rows = bits.map { MediaLOCMetadata(rawPresence: $0) }
        rows += [.init(rawPresence: 0, timestamp: 0), .init(rawPresence: 0, timescale: 0),
                 .init(rawPresence: 0, marking: Data([0])), .init(rawPresence: 0, audioLevel: 0),
                 .init(rawPresence: 0, videoConfig: Data()), .init(rawPresence: 0, audioConfig: Data()),
                 .init(rawPresence: 1 << 31),
                 .init(rawPresence: moq_swift_loc_metadata_has_marking(), marking: Data()),
                 .init(rawPresence: moq_swift_loc_metadata_has_marking(), marking: Data(repeating: 0, count: 5))]
        for metadata in rows {
            let allocations = SenderBufferAllocator()
            let result = allocations.withAllocator { alloc in
                writeSenderObject(.init(payload: Data([1]), properties: Data([2]), locMetadata: metadata),
                                  alloc: alloc) { _ in
                    Issue.record("Invalid shape reached native write")
                    return moq_result_t(MOQ_ERR_INVAL)
                }
            }
            guard case .invalidArgument = result else {
                Issue.record("Invalid shape did not fail as invalidArgument")
                continue
            }
            #expect(allocations.attempts == 0)
            #expect(allocations.frees == 0)
        }
        // Received construction is still permissive, including unknown bits.
        #expect(rows[12].rawPresence == 1 << 31)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Unusable clocks still reach native unchanged, with no legacy fallback")
    func nativeClockAuthority() {
        let both = moq_swift_loc_metadata_has_timestamp() | moq_swift_loc_metadata_has_timescale()
        let rows: [MediaLOCMetadata] = [
            .init(rawPresence: 0),
            .init(rawPresence: moq_swift_loc_metadata_has_timescale(), timescale: 0),
            .init(rawPresence: both, timestamp: 0, timescale: 0),
            .init(rawPresence: both, timestamp: .max, timescale: 1),
            .init(rawPresence: both, timestamp: 123, timescale: 1_000_000),
        ]
        for metadata in rows {
            let allocations = SenderBufferAllocator()
            var calls = 0
            let result = allocations.withAllocator { alloc in
                writeSenderObject(.init(payload: Data([1]), presentationTime: .seconds(7),
                    captureTime: Date(timeIntervalSince1970: 42), locMetadata: metadata), alloc: alloc) { send in
                    calls += 1
                    checkMetadata(send.pointee.metadata, metadata)
                    return moq_result_t(MOQ_ERR_INVAL)
                }
            }
            #expect(calls == 1)
            #expect(allocations.attempts == 1)
            #expect(allocations.frees == 1)
            guard case .invalidArgument = result else { Issue.record("Wrong refusal"); continue }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Old/nil and required-final-argument initializers preserve legacy fields")
    func legacyAndInitializerCompatibility() {
        let old: (Data, Bool, Bool, Bool, Duration, Duration?, Date?, SAPType?, Data?) -> OutgoingObject
            = OutgoingObject.init(payload:isSync:startsGroup:endsGroup:presentationTime:
                                  decodeTime:captureTime:sapType:properties:)
        let extended: (Data, Bool, Bool, Bool, Duration, Duration?, Date?, SAPType?, Data?, MediaLOCMetadata?)
            -> OutgoingObject = OutgoingObject.init(payload:isSync:startsGroup:endsGroup:presentationTime:
                decodeTime:captureTime:sapType:properties:locMetadata:)
        for sap in [SAPType.type1, .type2, .type3] {
            let object = old(Data([1, 0, 255]), true, true, true, .microseconds(123),
                             .microseconds(45), Date(timeIntervalSince1970: 2), sap, Data([2, 0]))
            #expect(object.locMetadata == nil)
            let nilObject = extended(object.payload, true, true, true, object.presentationTime,
                                     object.decodeTime, object.captureTime, sap, object.properties, nil)
            for value in [object, nilObject] {
                _ = writeSenderObject(value) { send in
                    let c = send.pointee
                    #expect(c.metadata == nil)
                    #expect(c.struct_size == MemoryLayout<moq_media_send_object_t>.size)
                    #expect(c.is_sync && c.starts_group && c.ends_group)
                    #expect(c.presentation_time_us == 123)
                    #expect(c.decode_time_us == 45)
                    #expect(c.has_capture_time && c.capture_time_us == 2_000_000)
                    #expect(c.has_sap_type && c.sap_type == cSAPType(sap))
                    #expect(Data(bytes: moq_rcbuf_data(c.payload)!, count: moq_rcbuf_len(c.payload)) == value.payload)
                    #expect(Data(bytes: moq_rcbuf_data(c.properties)!, count: moq_rcbuf_len(c.properties)) == value.properties)
                    return moq_result_t(MOQ_ERR_WOULD_BLOCK)
                }
            }
        }
        _ = writeSenderObject(.init(payload: Data())) { send in
            #expect(send.pointee.properties == nil && send.pointee.metadata == nil)
            #expect(!send.pointee.is_sync && !send.pointee.starts_group && !send.pointee.ends_group)
            #expect(send.pointee.presentation_time_us == 0 && send.pointee.decode_time_us == 0)
            #expect(!send.pointee.has_capture_time && !send.pointee.has_sap_type)
            return moq_result_t(MOQ_ERR_INVAL)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Payload/property allocation refusal never calls native; every partial ref is released")
    func allocationRefusal() {
        for failAt in [1, 2] {
            let allocations = SenderBufferAllocator()
            allocations.failAt = failAt
            let result = allocations.withAllocator { alloc in
                writeSenderObject(.init(payload: Data([1]), properties: Data(),
                    locMetadata: .init(rawPresence: 0)), alloc: alloc) { _ in
                    Issue.record("Allocation failure reached native write")
                    return moq_result_t(MOQ_OK)
                }
            }
            guard case .outOfMemory = result else { Issue.record("Wrong allocation error"); continue }
            #expect(allocations.attempts == failAt)
            #expect(allocations.frees == failAt - 1)
            #expect(allocations.live.isEmpty)
            #expect(!allocations.wrongThread)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("OK alone transfers both references; every native refusal frees them exactly once")
    func nativeRefusalsAndTransfer() {
        let rows = [MOQ_OK, MOQ_ERR_WOULD_BLOCK, MOQ_ERR_INVAL, MOQ_ERR_WRONG_STATE,
                    MOQ_ERR_INTERRUPTED, MOQ_ERR_CLOSED, MOQ_ERR_NOMEM, MOQ_ERR_UNSUPPORTED, MOQ_ERR_PROTO]
        for rc in rows {
            for properties in [nil, Data(), Data([9, 0])] as [Data?] {
                let allocations = SenderBufferAllocator()
                var transferred: [OpaquePointer] = []
                var calls = 0
                let result = allocations.withAllocator { alloc in
                    writeSenderObject(.init(payload: Data([1]), properties: properties,
                        locMetadata: .init(rawPresence: 0)), alloc: alloc) { send in
                        calls += 1
                        #expect(moq_rcbuf_refcount(send.pointee.payload) == 1)
                        if let extra = send.pointee.properties { #expect(moq_rcbuf_refcount(extra) == 1) }
                        if rc == MOQ_OK {
                            transferred = [send.pointee.payload!, send.pointee.properties].compactMap { $0 }
                        }
                        return moq_result_t(rc)
                    }
                }
                let count = properties == nil ? 1 : 2
                #expect(calls == 1 && allocations.attempts == count)
                #expect(allocations.frees == (rc == MOQ_OK ? 0 : count))
                #expect(allocations.live.count == (rc == MOQ_OK ? count : 0))
                switch (rc, result) {
                case (MOQ_OK, .ok), (MOQ_ERR_WOULD_BLOCK, .wouldBlock),
                     (MOQ_ERR_INVAL, .invalidArgument), (MOQ_ERR_WRONG_STATE, .wrongState),
                     (MOQ_ERR_INTERRUPTED, .interrupted), (MOQ_ERR_CLOSED, .closed),
                     (MOQ_ERR_NOMEM, .outOfMemory), (MOQ_ERR_UNSUPPORTED, .unsupported),
                     (MOQ_ERR_PROTO, .invalidArgument): break
                default: Issue.record("Native result mapping changed: \(rc)")
                }
                for pointer in transferred {
                    #expect(moq_rcbuf_refcount(pointer) == 1)
                    moq_rcbuf_decref(pointer)
                }
                #expect(allocations.frees == count && allocations.live.isEmpty)
                #expect(!allocations.wrongThread)
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Each retry allocates fresh owned refs inside that synchronous attempt")
    func retryOwnership() {
        let allocations = SenderBufferAllocator()
        allocations.withAllocator { alloc in
            for _ in 0..<3 {
                let before = allocations.attempts
                _ = writeSenderObject(.init(payload: Data([1]), properties: Data([2]),
                    locMetadata: .init(rawPresence: 0)), alloc: alloc) { send in
                    #expect(allocations.attempts == before + 2)
                    #expect(moq_rcbuf_refcount(send.pointee.payload) == 1)
                    #expect(moq_rcbuf_refcount(send.pointee.properties) == 1)
                    return moq_result_t(MOQ_ERR_WOULD_BLOCK)
                }
                #expect(allocations.live.isEmpty)
            }
        }
        #expect(allocations.attempts == 6 && allocations.frees == 6)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Nested metadata bytes remain valid until the synchronous call finishes")
    func synchronousByteLifetime() {
        var video = Data((0..<256).map { UInt8($0) })
        var audio = Data([0, 255, 0, 65])
        let originalVideo = video
        let originalAudio = audio
        let metadata = MediaLOCMetadata(rawPresence: moq_swift_loc_metadata_has_video_config()
            | moq_swift_loc_metadata_has_audio_config() | moq_swift_loc_metadata_has_marking(),
            marking: Data([0, 255, 0, 65]), videoConfig: video, audioConfig: audio)
        let object = OutgoingObject(payload: Data([1, 0]), properties: Data([2, 0]), locMetadata: metadata)
        let barrier = SenderCallBarrier()
        Thread.detachNewThread {
            let allocations = SenderBufferAllocator()
            var observed: [Data] = []
            let result = allocations.withAllocator { alloc in
                writeSenderObject(object, alloc: alloc) { send in
                    guard barrier.enter(), let metadata = send.pointee.metadata else {
                        return moq_result_t(MOQ_ERR_INVAL)
                    }
                    observed = [spanData(metadata.pointee.loc.video_config),
                                spanData(metadata.pointee.loc.audio_config),
                                withUnsafeBytes(of: metadata.pointee.loc.marking) { Data($0) },
                                Data(bytes: moq_rcbuf_data(send.pointee.payload)!, count: moq_rcbuf_len(send.pointee.payload)),
                                Data(bytes: moq_rcbuf_data(send.pointee.properties)!, count: moq_rcbuf_len(send.pointee.properties))]
                    return moq_result_t(MOQ_ERR_UNSUPPORTED)
                }
            }
            if case .unsupported = result, allocations.frees == 2,
               allocations.live.isEmpty, !allocations.wrongThread {
                observed.append(Data([1]))
            }
            barrier.finish(observed)
        }
        #expect(barrier.awaitEntry())
        video.resetBytes(in: 0..<video.count)
        audio.removeAll()
        barrier.release()
        #expect(barrier.awaitResult() == [originalVideo, originalAudio, Data([0, 255, 0, 65]),
                                         Data([1, 0]), Data([2, 0]), Data([1])])
    }
}
