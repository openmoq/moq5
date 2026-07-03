import Foundation

/// One received media object, exclusively owned by this instance.
///
/// Ownership: the bytes are owned by this object's ``OwnedObjectStorage``,
/// released exactly once via `deinit` → `cleanup()` (legal on any thread).
/// The MoQService C bridge COPIES received bytes into Swift-owned storage
/// at poll time and cleans the C object on the service thread in the same
/// poll (v1 is copied-by-design: the C tier's rcbuf refcounts are
/// non-atomic and must never reach a cross-thread finalizer; zero-copy
/// receive is a future C API project). No refcounted C handle ever crosses
/// into this type — hence `@unchecked Sendable` with all stored properties
/// immutable.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MediaObject: @unchecked Sendable {

    package let storage: any OwnedObjectStorage

    public let track: MediaTrack
    public let isKeyframe: Bool
    public let endsGroup: Bool
    public let isDatagram: Bool
    public let presentationTime: Duration
    public let decodeTime: Duration
    public let compositionOffset: Duration
    public let captureTime: Date?

    package init(storage: any OwnedObjectStorage,
                 track: MediaTrack,
                 isKeyframe: Bool,
                 endsGroup: Bool,
                 isDatagram: Bool,
                 presentationTime: Duration = .zero,
                 decodeTime: Duration = .zero,
                 compositionOffset: Duration = .zero,
                 captureTime: Date? = nil) {
        self.storage = storage
        self.track = track
        self.isKeyframe = isKeyframe
        self.endsGroup = endsGroup
        self.isDatagram = isDatagram
        self.presentationTime = presentationTime
        self.decodeTime = decodeTime
        self.compositionOffset = compositionOffset
        self.captureTime = captureTime
    }

    deinit {
        storage.cleanup()   /* exactly once, any thread (exclusive owner) */
    }

    /// The decodable media bytes, copied: the RAW/LOC payload, or the CMAF
    /// mdat slice. Prefer ``withUnsafeMediaBytes(_:)`` (or the CoreMedia
    /// helpers, in the bridge slice) to avoid the copy.
    public var mediaData: Data {
        withUnsafeMediaBytes { Data($0) }
    }

    /// The full CMAF fragment (container framing included), copied; `nil`
    /// for RAW/LOC objects. Muxers/recorders want this; decoders want
    /// ``mediaData``.
    public var fragmentData: Data? {
        guard storage.fragmentByteCount > 0 else { return nil }
        return storage.withFragmentBytes { Data($0) }
    }

    /// Scoped zero-copy access to the media bytes. The pointer is valid only
    /// inside `body`; do not store it.
    public func withUnsafeMediaBytes<R>(
        _ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        try storage.withMediaBytes(body)
    }

    /// Scoped zero-copy access to the full CMAF fragment bytes (empty buffer
    /// for RAW/LOC objects).
    public func withUnsafeFragmentBytes<R>(
        _ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R {
        try storage.withFragmentBytes(body)
    }
}

/// One object to publish through a ``MediaSender``.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct OutgoingObject: Sendable {
    /// The media payload; copied into a C buffer on the service thread at
    /// write time (zero-copy send is future work).
    public var payload: Data
    /// Extra LOC property extensions (CMAF/LOC passthrough); must be `nil`
    /// for RAW tracks (the service generates the standard LOC properties).
    public var properties: Data?
    public var isSync: Bool
    public var startsGroup: Bool
    public var endsGroup: Bool
    public var presentationTime: Duration
    public var decodeTime: Duration?
    public var captureTime: Date?
    public var sapType: SAPType?

    public init(payload: Data,
                isSync: Bool = false,
                startsGroup: Bool = false,
                endsGroup: Bool = false,
                presentationTime: Duration = .zero,
                decodeTime: Duration? = nil,
                captureTime: Date? = nil,
                sapType: SAPType? = nil,
                properties: Data? = nil) {
        self.payload = payload
        self.properties = properties
        self.isSync = isSync
        self.startsGroup = startsGroup
        self.endsGroup = endsGroup
        self.presentationTime = presentationTime
        self.decodeTime = decodeTime
        self.captureTime = captureTime
        self.sapType = sapType
    }
}
