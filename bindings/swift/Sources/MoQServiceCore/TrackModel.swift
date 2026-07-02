import Foundation

/// What a track carries.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum MediaType: Sendable, Hashable {
    case video, audio
}

/// How a track's media is packaged on the wire.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum MediaPackaging: Sendable, Hashable {
    case raw     /// LOC objects: bare access units + LOC properties
    case cmaf    /// complete CMAF fragments, passed through byte-exact
}

/// Where a fresh subscription starts.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum StartMode: Sendable, Hashable {
    case current       /// live edge (default)
    case nextGroup     /// clean join at the next group boundary
}

/// Receiver-side timestamp handling.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum TimeMode: Sendable, Hashable {
    case raw           /// pass timestamps through untouched (default)
    case sharedEpoch   /// rebase timestamp FIELDS; payload bytes untouched
}

/// CMAF Stream Access Point types (CMSF §3.4).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum SAPType: Sendable, Hashable {
    case type1, type2, type3
}

/// An immutable, Sendable snapshot of a track's catalog description. Copied
/// eagerly from the C tier's handle-owned spans at event time; never borrows.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct TrackDescription: Sendable, Hashable {
    public var name: String
    public var mediaType: MediaType
    public var packaging: MediaPackaging
    public var codec: String?
    public var role: String?
    public var language: String?
    public var label: String?
    public var width: Int?
    public var height: Int?
    public var framerateMillis: UInt64?
    public var samplerate: Int?
    public var channelConfig: String?
    public var bitrate: UInt64?
    public var timescale: UInt32?
    public var isLive: Bool
    /// Present once a track is (or became) VOD (MSF §5.2.35).
    public var trackDuration: Duration?
    /// Full container init segment (CMAF: ftyp+moov) — what a muxer wants.
    public var initData: Data?
    /// Decoder extradata (SPS/PPS, AudioSpecificConfig) — what a decoder wants.
    public var codecConfig: Data?

    public init(name: String, mediaType: MediaType, packaging: MediaPackaging) {
        self.name = name
        self.mediaType = mediaType
        self.packaging = packaging
        self.isLive = true
    }
}

/// A stable handle to one discovered (receiver) or added (sender) track.
///
/// Identity semantics: the same underlying C handle always maps to the same
/// `MediaTrack` instance, so `object.track === video` works; `Equatable` and
/// `Hashable` are identity, not value. All stored state is immutable or
/// lock-guarded snapshots, and every C call taking the handle executes on the
/// endpoint's service thread — hence `@unchecked Sendable`.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MediaTrack: @unchecked Sendable, Hashable {

    public enum SubscriptionState: Sendable, Hashable {
        case discovered, pending, active, pausedByApp, pausedByFlowControl, ended
    }

    private let lock = NSLock()
    private var _description: TrackDescription

    /// The current catalog description (re-snapshotted on `.updated` events,
    /// e.g. a live→VOD conversion).
    public var description: TrackDescription {
        lock.lock(); defer { lock.unlock() }
        return _description
    }

    package init(description: TrackDescription) {
        self._description = description
    }

    package func updateDescription(_ new: TrackDescription) {
        lock.lock(); _description = new; lock.unlock()
    }

    public static func == (lhs: MediaTrack, rhs: MediaTrack) -> Bool {
        lhs === rhs
    }
    public func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(self))
    }
}

/// Receiver-side track lifecycle events (the `trackEvents` stream elements).
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum TrackEvent: Sendable {
    case added(MediaTrack)
    case updated(MediaTrack)      /// live→VOD conversion et al.
    case removed(MediaTrack)
    case ended(MediaTrack)
    case catalogReady             /// first catalog fully enumerated (once)
}
