import Foundation

/// Publishes a namespace's catalog and media tracks over an endpoint.
///
/// Model-layer shell: configuration, presets, and track configuration are
/// live; the write path and readiness land with the service-thread engine.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public final class MediaSender: @unchecked Sendable {

    // MARK: Configuration

    public struct Configuration: Sendable {
        /// What `write` does when the send queue is full. Drop policies map
        /// to the matching C eviction; `lossless`/`failFast` run over the C
        /// fail-fast mode with the retry loop in Swift (Task-cancellable).
        /// No default — the choice is forced, exactly like the C
        /// configuration.
        public enum SendPolicy: Sendable {
            /// Live: evict whole groups, always keep the newest keyframe
            /// suffix (C DROP_TO_KEYFRAME).
            case dropToKeyframe
            /// Evict whole groups from the oldest end (C DROP_GROUP).
            case dropGroup
            /// Never drop: `write` suspends until space or the deadline,
            /// then throws ``MoQServiceError/wouldBlock``.
            case lossless(timeout: Duration = .seconds(1))
            /// Throw ``MoQServiceError/wouldBlock`` immediately when full.
            case failFast
        }

        public struct QueueLimits: Sendable, Hashable {
            public var maxObjects: Int?
            public var maxBytes: Int?
            public init(maxObjects: Int? = nil, maxBytes: Int? = nil) {
                self.maxObjects = maxObjects
                self.maxBytes = maxBytes
            }
        }

        public var namespace: MediaNamespace
        public var sendPolicy: SendPolicy
        public var queueLimits: QueueLimits?
        /// Strict CMAF validation of written fragments (default true, like
        /// the C tier); disable only for known-good passthrough.
        public var validateCMAF: Bool
        /// Catalog track name; `nil` uses the MSF default ("catalog").
        public var catalogTrack: String?

        public init(namespace: MediaNamespace, sendPolicy: SendPolicy) {
            self.namespace = namespace
            self.sendPolicy = sendPolicy
            self.queueLimits = nil
            self.validateCMAF = true
            self.catalogTrack = nil
        }

        /// The live preset (mirrors the C `cfg_init_live`): drop-to-keyframe.
        public static func live(namespace: MediaNamespace) -> Configuration {
            Configuration(namespace: namespace, sendPolicy: .dropToKeyframe)
        }

        /// The lossless preset (mirrors the C `cfg_init_lossless`): suspend
        /// on backpressure with a bounded deadline.
        public static func lossless(namespace: MediaNamespace) -> Configuration {
            Configuration(namespace: namespace, sendPolicy: .lossless())
        }

        package func validate() throws {
            try namespace.validate()
            if case .lossless(let timeout) = sendPolicy, timeout <= .zero {
                throw MoQServiceError.invalidArgument(
                    "lossless timeout must be positive")
            }
            if let limits = queueLimits {
                if let o = limits.maxObjects, o <= 0 {
                    throw MoQServiceError.invalidArgument(
                        "queue maxObjects must be positive (nil = library default)")
                }
                if let b = limits.maxBytes, b <= 0 {
                    throw MoQServiceError.invalidArgument(
                        "queue maxBytes must be positive (nil = library default)")
                }
            }
        }
    }

    public let endpoint: MoQEndpoint
    public let configuration: Configuration
    package let backend: any SenderBackend

    package init(endpoint: MoQEndpoint,
                 configuration: Configuration,
                 backend: any SenderBackend) {
        self.endpoint = endpoint
        self.configuration = configuration
        self.backend = backend
    }

    // MARK: Lifecycle (engine lands with the C bridge slice)

    /// Attach to an existing endpoint (one sender per endpoint; may share
    /// the endpoint with one ``MediaReceiver``).
    public static func attach(to endpoint: MoQEndpoint,
                              configuration: Configuration) throws -> MediaSender {
        try configuration.validate()
        throw MoQServiceError.unsupported   // TODO(S2): service-thread engine
    }

    public func addTrack(_ configuration: TrackConfiguration) async throws -> MediaTrack {
        throw MoQServiceError.unsupported   // TODO(S2)
    }

    public func write(_ object: OutgoingObject, to track: MediaTrack) async throws {
        throw MoQServiceError.unsupported   // TODO(S2)
    }

    /// Reliable END_OF_TRACK after queued objects drain; idempotent.
    public func endTrack(_ track: MediaTrack) async throws {
        throw MoQServiceError.unsupported   // TODO(S2)
    }

    public func close() async {
        // TODO(S2): detach via the engine; model shell is a no-op.
    }
}
