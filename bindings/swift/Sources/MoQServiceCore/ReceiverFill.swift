/// A semantic location, resolved and validated by the native service.
/// Nil in an option is omission; `.some(.none)` explicitly removes the
/// constraint. End objects are inclusive, including UInt64.max.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum MediaLocationFilter: Sendable, Hashable {
    case none, nextObject
    case relativeGroup(offset: UInt64)
    case absoluteStart(group: UInt64, object: UInt64)
    case groupEnd(startGroup: UInt64, startObject: UInt64, endGroup: UInt64)
    case objectEnd(startGroup: UInt64, startObject: UInt64,
                   endGroup: UInt64, endObject: UInt64)
}

/// A non-nil value requests a fill, even when all overrides are omitted.
/// Omitted values inherit native policy. Timeout is upstream wait policy,
/// not a child lifetime: nil differs from an explicit zero.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct MediaFillOptions: Sendable, Hashable {
    public enum GroupOrder: Sendable, Hashable { case ascending, descending }
    public var subscriberPriority: UInt8?
    public var groupOrder: GroupOrder?
    public var timeoutMilliseconds: UInt64?
    public var location: MediaLocationFilter?

    public init(subscriberPriority: UInt8? = nil, groupOrder: GroupOrder? = nil,
                timeoutMilliseconds: UInt64? = nil, location: MediaLocationFilter? = nil) {
        self.subscriberPriority = subscriberPriority
        self.groupOrder = groupOrder
        self.timeoutMilliseconds = timeoutMilliseconds
        self.location = location
    }
}

/// Explicit options suppress the implicit joining fill. Location supersedes
/// StartMode; only non-nil fill requests a child. Empty options retain native
/// defaults. On resume, options are validated but do not reconfigure a track.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct MediaTrackSubscriptionOptions: Sendable, Hashable {
    public var location: MediaLocationFilter?
    public var fill: MediaFillOptions?

    public init(location: MediaLocationFilter? = nil, fill: MediaFillOptions? = nil) {
        self.location = location
        self.fill = fill
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum MediaFillReason: Sendable, Hashable {
    case complete, noStream, localCancelled, peerCancelledOrReset, localAborted
    case parentRejected, parentTerminated
    case unknown(UInt32)
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum MediaFillErrorDomain: Sendable, Hashable {
    case none, stream, parentRequest, parentDone
    case unknown(UInt32)
}

/// Copied terminal facts. Nil means the sized native output did not contain
/// that complete field; zero is a valid request ID or code, not absence.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct MediaFillTerminal: Sendable, Hashable {
    public let initiatingRequestID: UInt64?
    public let reason: MediaFillReason?
    public let errorDomain: MediaFillErrorDomain?
    public let errorCode: UInt64?

    public init(initiatingRequestID: UInt64?, reason: MediaFillReason?,
                errorDomain: MediaFillErrorDomain?, errorCode: UInt64?) {
        self.initiatingRequestID = initiatingRequestID
        self.reason = reason
        self.errorDomain = errorDomain
        self.errorCode = errorCode
    }
}

/// A consuming projection of the same bounded queue as TrackEvent, not a
/// broadcast or replay. A fill terminal does not end its parent track.
/// A local failure is track-scoped, not a receiver terminal. This view does
/// not project every native event; event order is native poll order.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum ExtendedTrackEvent: Sendable {
    case legacy(TrackEvent)
    case fillTerminal(MediaTrack, MediaFillTerminal)
    /// Nil means the native sized output omitted the complete local result.
    /// Present values retain their exact signed native result, including zero.
    case localFailure(MediaTrack, localResult: Int32?)
    /// An UPDATE failed before admission, not a peer rejection or track end.
    case updateLocalFailure(MediaTrack, localResult: Int32?)
    /// Local capacity refusal. Origin is the native MOQ_MEDIA_DELIVERY_* value;
    /// only FILL carries an initiating ID. Nil denotes an absent complete field.
    /// LIVE fails this generation; FILL leaves its parent active.
    case deliveryLocalFailure(MediaTrack, localResult: Int32?, origin: UInt32?, initiatingRequestID: UInt64?)
}

/// A point-in-time native delivery state, queried separately from events.
/// The legacy enum stays unchanged; unknown native values remain observable.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum ExtendedTrackSubscriptionState: Sendable, Hashable {
    case legacy(MediaTrack.SubscriptionState)
    case failed
    case unknown(UInt32)
}

/// Shares the receiver's single next-call consumer claim with TrackEvents.
/// Staged values survive detach; unpolled native events need not survive it.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct ExtendedTrackEvents: AsyncSequence, Sendable {
    public typealias Element = ExtendedTrackEvent
    let receiver: MediaReceiver

    public func makeAsyncIterator() -> Iterator { Iterator(receiver: receiver) }

    public struct Iterator: AsyncIteratorProtocol {
        let receiver: MediaReceiver
        public mutating func next() async throws -> ExtendedTrackEvent? {
            try await receiver.nextExtendedTrackEvent()
        }
    }
}
