import Foundation

/// Subscribes to a namespace's catalog and media tracks over an endpoint.
///
/// Model-layer shell: configuration and presets are live; discovery/object
/// streams and subscription control land with the service-thread engine.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MediaReceiver: @unchecked Sendable {

    // MARK: Configuration

    public struct Configuration: Sendable {
        /// What happens when the app polls slower than media arrives. The C
        /// tier owns the queue and the eviction; there is no default — the
        /// choice is forced, exactly like the C configuration.
        public enum OverflowPolicy: Sendable {
            /// Live playback: evict whole groups, always keep the newest
            /// keyframe suffix.
            case dropToKeyframe(maxObjects: Int? = nil, maxBytes: Int? = nil)
            /// Evict whole groups from the oldest end.
            case dropGroup(maxObjects: Int? = nil, maxBytes: Int? = nil)
            /// Never drop: pause the subscription upstream instead
            /// (auto-resumes as the app catches up).
            case flowControl(maxObjects: Int? = nil, maxBytes: Int? = nil)
        }

        public var namespace: MediaNamespace
        public var overflowPolicy: OverflowPolicy
        public var autoSubscribe: Bool
        public var timeMode: TimeMode
        /// Catalog track name; `nil` uses the MSF default ("catalog").
        public var catalogTrack: String?

        public init(namespace: MediaNamespace,
                    overflowPolicy: OverflowPolicy,
                    autoSubscribe: Bool = true) {
            self.namespace = namespace
            self.overflowPolicy = overflowPolicy
            self.autoSubscribe = autoSubscribe
            self.timeMode = .raw
            self.catalogTrack = nil
        }

        /// The live-player preset (mirrors the C `cfg_init_live`):
        /// drop-to-keyframe overflow, auto-subscribe.
        public static func live(namespace: MediaNamespace) -> Configuration {
            Configuration(namespace: namespace, overflowPolicy: .dropToKeyframe())
        }

        /// The lossless/VOD preset (mirrors the C `cfg_init_flow_control`):
        /// upstream pause instead of drops.
        public static func flowControl(namespace: MediaNamespace) -> Configuration {
            Configuration(namespace: namespace, overflowPolicy: .flowControl())
        }

        package func validate() throws {
            try namespace.validate()
            let (maxObjects, maxBytes): (Int?, Int?)
            switch overflowPolicy {
            case .dropToKeyframe(let o, let b),
                 .dropGroup(let o, let b),
                 .flowControl(let o, let b):
                (maxObjects, maxBytes) = (o, b)
            }
            if let o = maxObjects, o <= 0 {
                throw MoQServiceError.invalidArgument(
                    "overflow maxObjects must be positive (nil = library default)")
            }
            if let b = maxBytes, b <= 0 {
                throw MoQServiceError.invalidArgument(
                    "overflow maxBytes must be positive (nil = library default)")
            }
        }
    }

    public let endpoint: MoQEndpoint
    public let configuration: Configuration
    package let backend: any ReceiverBackend

    package init(endpoint: MoQEndpoint,
                 configuration: Configuration,
                 backend: any ReceiverBackend) {
        self.endpoint = endpoint
        self.configuration = configuration
        self.backend = backend
    }

    // MARK: Lifecycle (engine lands with the C bridge slice)

    /// Attach to an existing endpoint (one receiver per endpoint; may share
    /// the endpoint with one ``MediaSender``).
    public static func attach(to endpoint: MoQEndpoint,
                              configuration: Configuration) throws -> MediaReceiver {
        try configuration.validate()
        throw MoQServiceError.unsupported   // TODO(S2): service-thread engine
    }

    /// Subscribe a discovered track (manual selection; pause/resume model).
    public func subscribe(_ track: MediaTrack,
                          start: StartMode = .current,
                          priority: UInt8? = nil) async throws {
        throw MoQServiceError.unsupported   // TODO(S2)
    }

    /// Pause delivery and purge this track's queued objects.
    public func unsubscribe(_ track: MediaTrack) async throws {
        throw MoQServiceError.unsupported   // TODO(S2)
    }

    public func close() async {
        // TODO(S2): detach via the engine; model shell is a no-op.
    }
}
