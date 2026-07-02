import Testing
import Foundation
@testable import MoQServiceCore

/// Scripted storage: counts cleanup calls so the tests can prove the
/// exactly-once contract, including the deinit-driven path.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class MockObjectStorage: OwnedObjectStorage, @unchecked Sendable {
    let media: [UInt8]
    let fragment: [UInt8]
    private let lock = NSLock()
    private var _cleanupCount = 0
    var cleanupCount: Int {
        lock.lock(); defer { lock.unlock() }
        return _cleanupCount
    }

    init(media: [UInt8], fragment: [UInt8] = []) {
        self.media = media
        self.fragment = fragment
    }

    var mediaByteCount: Int { media.count }
    var fragmentByteCount: Int { fragment.count }

    func withMediaBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        try media.withUnsafeBytes(body)
    }
    func withFragmentBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        try fragment.withUnsafeBytes(body)
    }
    func cleanup() {
        lock.lock(); _cleanupCount += 1; lock.unlock()
    }
}

@Suite("MediaTrack identity")
struct MediaTrackIdentityTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Same handle is identical; distinct handles are not equal")
    func identity() {
        let desc = TrackDescription(name: "video", mediaType: .video, packaging: .raw)
        let a = MediaTrack(description: desc)
        let b = MediaTrack(description: desc)
        #expect(a === a)
        #expect(a !== b)
        #expect(a == a)
        #expect(a != b)              /* Equatable is identity, not value */
        #expect(Set([a, b]).count == 2)
        #expect(a.description.name == "video")
    }
}

@Suite("MediaObject ownership")
struct MediaObjectOwnershipTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    private func makeObject(_ storage: MockObjectStorage) -> MediaObject {
        MediaObject(
            storage: storage,
            track: MediaTrack(description:
                TrackDescription(name: "v", mediaType: .video, packaging: .raw)),
            isKeyframe: true, endsGroup: false, isDatagram: false)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cleanup runs exactly once, driven by deinit")
    func exactlyOnceCleanup() {
        let storage = MockObjectStorage(media: [1, 2, 3])
        do {
            let object = makeObject(storage)
            #expect(storage.cleanupCount == 0)   /* alive: no cleanup yet */
            #expect(object.isKeyframe)
        }
        #expect(storage.cleanupCount == 1)       /* released: exactly once */
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("mediaData copies; unsafe access sees the same bytes")
    func byteAccess() {
        let storage = MockObjectStorage(media: [0xAA, 0xBB, 0xCC])
        let object = makeObject(storage)
        #expect(object.mediaData == Data([0xAA, 0xBB, 0xCC]))
        let sum = object.withUnsafeMediaBytes { buf in
            buf.reduce(0) { $0 + Int($1) }
        }
        #expect(sum == 0xAA + 0xBB + 0xCC)
        #expect(storage.cleanupCount == 0)       /* access never cleans up */
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("CMAF fragment accessor is nil for RAW objects")
    func fragmentAccess() {
        let raw = makeObject(MockObjectStorage(media: [1]))
        #expect(raw.fragmentData == nil)

        let cmafStorage = MockObjectStorage(media: [9], fragment: [1, 2, 3, 4])
        let cmaf = MediaObject(
            storage: cmafStorage,
            track: MediaTrack(description:
                TrackDescription(name: "v", mediaType: .video, packaging: .cmaf)),
            isKeyframe: false, endsGroup: true, isDatagram: false)
        #expect(cmaf.fragmentData == Data([1, 2, 3, 4]))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Object survives crossing a task boundary before cleanup")
    func sendableShape() async {
        let storage = MockObjectStorage(media: [7])
        let object = makeObject(storage)
        let count = await Task.detached { object.mediaData.count }.value
        #expect(count == 1)
        #expect(storage.cleanupCount == 0)
    }
}

@Suite("Sendable shape")
struct SendableShapeTests {

    private func requiresSendable<T: Sendable>(_: T.Type) {}

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Value types and handles are Sendable")
    func sendableTypes() {
        requiresSendable(TrackDescription.self)
        requiresSendable(TrackEvent.self)
        requiresSendable(OutgoingObject.self)
        requiresSendable(MoQServiceError.self)
        requiresSendable(MoQVersion.self)
        requiresSendable(MediaNamespace.self)
        requiresSendable(MoQEndpoint.Configuration.self)
        requiresSendable(MediaReceiver.Configuration.self)
        requiresSendable(MediaSender.Configuration.self)
        requiresSendable(TrackConfiguration.self)
        requiresSendable(MediaTrack.self)      /* @unchecked: identity handle */
        requiresSendable(MediaObject.self)     /* @unchecked: exclusive owner */
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Object storage seam itself is Sendable")
    func storageSeamSendable() {
        /* MediaObject is Sendable BECAUSE its storage is: the seam must
         * carry the requirement so every implementation states its own
         * concurrency contract (the C storage will be @unchecked with the
         * exclusive-ownership rationale; mocks must lock). This compiles
         * ONLY if OwnedObjectStorage refines Sendable. */
        func storageIsSendable<S: OwnedObjectStorage>(_: S.Type) {
            requiresSendable(S.self)
        }
        storageIsSendable(MockObjectStorage.self)
    }
}
