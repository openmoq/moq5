import Foundation
import MoQ

/// Maps subscribed tracks to playback pipeline tracks and
/// dispatches received objects in one call.
public struct TrackRouter {
    private var routes: [(sub: SubscribedTrack, pb: PlaybackTrack)] = []

    public init() {}

    /// Register or replace a mapping from a subscribed track to a
    /// playback track. If a route for the same subscribed track
    /// identity already exists, its playback track is replaced.
    public mutating func add(
        subscribed: SubscribedTrack, playback: PlaybackTrack
    ) {
        if let idx = routes.firstIndex(where: { $0.sub === subscribed }) {
            routes[idx] = (sub: subscribed, pb: playback)
        } else {
            routes.append((sub: subscribed, pb: playback))
        }
    }

    /// Route an object to its playback track and push it into the
    /// pipeline. Returns true if the object was routed and pushed.
    /// Returns false for unrecognized tracks. Throws if push throws.
    @discardableResult
    public func pushIfRouted(
        object: FacadeReceivedObject,
        pipeline: PlaybackPipeline,
        now: UInt64
    ) throws -> Bool {
        guard let route = routes.first(where: { $0.sub === object.track })
        else { return false }
        try pipeline.push(object: object, track: route.pb, now: now)
        return true
    }
}
