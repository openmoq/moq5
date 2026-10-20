import MoQService

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func fillConsumer(_ receiver: MediaReceiver, _ track: MediaTrack) async throws {
    let options = MediaTrackSubscriptionOptions(
        location: .some(.none),
        fill: MediaFillOptions(subscriberPriority: 0, groupOrder: .ascending,
                               timeoutMilliseconds: 0,
                               location: .objectEnd(startGroup: 0, startObject: 0,
                                                    endGroup: .max, endObject: .max)))
    let subscribe: (MediaTrack, StartMode, UInt8?, MediaTrackSubscriptionOptions) async throws -> Void = receiver.subscribe
    let cancel: (MediaTrack, UInt64) async throws -> Void = receiver.cancelFill
    try await subscribe(track, .current, nil, options)
    try await receiver.subscribe(track, options: options)
    try await cancel(track, .max)
    var iterator = receiver.trackEventsExtended.makeAsyncIterator()
    switch try await iterator.next() {
    case .legacy(let legacy):
        switch legacy {
        case .added, .updated, .removed, .ended, .catalogReady, .parseDrop: break
        }
    case .fillTerminal(let parent, let terminal):
        let copy: MediaFillTerminal = terminal
        _ = parent === track
        _ = copy.initiatingRequestID
        _ = copy.reason
        _ = copy.errorDomain
        _ = copy.errorCode
    case .localFailure(let parent, let localResult):
        let result: Int32? = localResult
        _ = parent === track
        _ = result
    case .updateLocalFailure(let parent, let localResult):
        let result: Int32? = localResult
        _ = parent === track
        _ = result
    case .deliveryLocalFailure(let parent, let localResult, let origin, let initiatingID):
        let result: Int32? = localResult
        let deliveryOrigin: UInt32? = origin
        let requestID: UInt64? = initiatingID
        _ = parent === track
        _ = (result, deliveryOrigin, requestID)
    case nil: break
    }
    let stateQuery: (MediaTrack) async throws -> ExtendedTrackSubscriptionState = receiver.trackStateExtended
    switch try await stateQuery(track) {
    case .legacy(let state):
        switch state {
        case .discovered, .pending, .active, .pausedByApp, .pausedByFlowControl, .ended: break
        }
    case .failed: break
    case .unknown(let raw):
        let value: UInt32 = raw
        _ = value
    }
}
