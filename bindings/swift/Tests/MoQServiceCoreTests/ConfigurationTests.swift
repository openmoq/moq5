import Testing
import Foundation
@testable import MoQServiceCore

@Suite("Endpoint configuration")
struct EndpointConfigurationTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Recognized URL schemes pass preflight")
    func recognizedSchemes() throws {
        for url in ["moqt://relay.example.com:4443",
                    "moq://relay.example.com:4443",
                    "https://relay.example.com:4443/moq"] {
            let cfg = MoQEndpoint.Configuration(url: URL(string: url)!)
            try cfg.validate()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Unknown scheme is rejected before dialing")
    func unknownScheme() {
        let cfg = MoQEndpoint.Configuration(url: URL(string: "ftp://relay.example.com:4443")!)
        #expect(throws: MoQServiceError.self) { try cfg.validate() }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Missing host is rejected")
    func missingHost() {
        let cfg = MoQEndpoint.Configuration(url: URL(string: "moqt://")!)
        #expect(throws: MoQServiceError.self) { try cfg.validate() }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Defaults are automatic/secure")
    func defaults() {
        let cfg = MoQEndpoint.Configuration(url: URL(string: "moqt://h:1")!)
        #expect(cfg.transportProtocol == .automatic)
        #expect(cfg.backend == .automatic)
        #expect(cfg.versions == .automatic)
        #expect(cfg.serverName == nil)
        #expect(!cfg.insecureSkipVerify)
    }
}

@Suite("Media namespace")
struct MediaNamespaceTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("String literal splits on slash")
    func literal() {
        let ns: MediaNamespace = "live/cam1"
        #expect(ns.parts == ["live", "cam1"])
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Explicit parts round-trip")
    func parts() {
        let ns = MediaNamespace(parts: ["a", "b", "c"])
        #expect(ns.parts.count == 3)
        #expect(!ns.isEmpty)
        #expect(MediaNamespace(parts: []).isEmpty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Malformed literals are preserved and rejected at validation")
    func malformedLiterals() {
        /* The literal initializer must NOT silently normalize a malformed
         * namespace -- empty components are preserved so validation can
         * reject them loudly instead of mutating the app's intent. */
        let trailing: MediaNamespace = "live/"
        #expect(trailing.parts == ["live", ""])
        #expect(throws: MoQServiceError.self) { try trailing.validate() }

        let doubled: MediaNamespace = "live//cam"
        #expect(doubled.parts == ["live", "", "cam"])
        #expect(throws: MoQServiceError.self) { try doubled.validate() }

        #expect(throws: MoQServiceError.self) {
            try MediaNamespace(parts: ["live", ""]).validate()
        }
        #expect(throws: MoQServiceError.self) {
            try MediaNamespace(parts: []).validate()
        }
        /* Well-formed namespaces validate. */
        #expect(throws: Never.self) {
            try MediaNamespace(parts: ["live", "cam1"]).validate()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Runtime string initializer mirrors the string literal")
    func runtimeString() {
        /* The runtime init(_:) is what apps use for user-entered paths; it
         * must be byte-for-byte identical to the literal path (same split,
         * same empty-component preservation, same validation outcome). */
        #expect(MediaNamespace("live/cam1").parts == ["live", "cam1"])

        for input in ["live/cam1", "live/", "live//cam", "solo", "/lead", ""] {
            let runtime = MediaNamespace(input)
            let literal = MediaNamespace(stringLiteral: input)
            #expect(runtime.parts == literal.parts)
        }

        /* Empty components are preserved (not normalized) and still rejected. */
        #expect(MediaNamespace("live/").parts == ["live", ""])
        #expect(throws: MoQServiceError.self) {
            try MediaNamespace("live//cam").validate()
        }
        #expect(throws: Never.self) {
            try MediaNamespace("live/cam1").validate()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Configurations reject namespaces with empty parts")
    func configRejectsEmptyParts() {
        let rcfg = MediaReceiver.Configuration(
            namespace: "live/", overflowPolicy: .dropToKeyframe())
        #expect(throws: MoQServiceError.self) { try rcfg.validate() }

        let scfg = MediaSender.Configuration(
            namespace: MediaNamespace(parts: ["a", ""]), sendPolicy: .failFast)
        #expect(throws: MoQServiceError.self) { try scfg.validate() }
    }
}

@Suite("Receiver configuration")
struct ReceiverConfigurationTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Live preset mirrors the C live preset")
    func livePreset() {
        let cfg = MediaReceiver.Configuration.live(namespace: "live/cam1")
        guard case .dropToKeyframe = cfg.overflowPolicy else {
            Issue.record("live preset must select dropToKeyframe")
            return
        }
        #expect(cfg.autoSubscribe)
        #expect(cfg.timeMode == .raw)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Flow-control preset selects flowControl")
    func flowControlPreset() {
        let cfg = MediaReceiver.Configuration.flowControl(namespace: "vod/movie")
        guard case .flowControl = cfg.overflowPolicy else {
            Issue.record("flowControl preset must select flowControl")
            return
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Empty namespace is rejected")
    func emptyNamespace() {
        let cfg = MediaReceiver.Configuration(
            namespace: MediaNamespace(parts: []),
            overflowPolicy: .dropToKeyframe())
        #expect(throws: MoQServiceError.self) { try cfg.validate() }
    }
}

@Suite("Sender configuration")
struct SenderConfigurationTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Presets mirror the C presets")
    func presets() {
        let live = MediaSender.Configuration.live(namespace: "live/cam1")
        guard case .dropToKeyframe = live.sendPolicy else {
            Issue.record("live preset must select dropToKeyframe")
            return
        }
        let lossless = MediaSender.Configuration.lossless(namespace: "live/cam1")
        guard case .lossless(let timeout) = lossless.sendPolicy else {
            Issue.record("lossless preset must select lossless")
            return
        }
        #expect(timeout == .seconds(1))
        #expect(live.validateCMAF)     /* strict by default, both presets */
        #expect(lossless.validateCMAF)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Video track factory carries the required catalog fields")
    func videoTrackFactory() throws {
        let cfg = TrackConfiguration.video(
            name: "video", codec: "avc1.64001f", bitrate: 3_000_000,
            width: 1280, height: 720)
        #expect(cfg.name == "video")
        #expect(cfg.mediaType == .video)
        #expect(cfg.packaging == .raw)
        try cfg.validate()
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Required track fields are enforced")
    func trackValidation() {
        /* Empty codec: rejected (MSF-01 §5.2.18 requires it for A/V). */
        var cfg = TrackConfiguration.video(
            name: "v", codec: "", bitrate: 1, width: 1, height: 1)
        #expect(throws: MoQServiceError.self) { try cfg.validate() }

        /* Zero bitrate: rejected (§5.2.22). */
        cfg = TrackConfiguration.video(
            name: "v", codec: "av01", bitrate: 0, width: 1, height: 1)
        #expect(throws: MoQServiceError.self) { try cfg.validate() }

        /* Audio without samplerate/channelConfig: rejected. */
        let audio = TrackConfiguration.audio(
            name: "a", codec: "mp4a.40.2", bitrate: 128_000,
            samplerate: 0, channelConfig: "")
        #expect(throws: MoQServiceError.self) { try audio.validate() }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Non-positive numeric fields never reach the bridge")
    func numericValidation() {
        /* Zero/negative video dimensions: rejected (they would become huge
         * unsigned C values or invalid catalog fields). */
        for (w, h) in [(0, 1), (1, 0), (-1, 720), (1280, -1)] {
            let cfg = TrackConfiguration.video(
                name: "v", codec: "av01", bitrate: 1, width: w, height: h)
            #expect(throws: MoQServiceError.self) { try cfg.validate() }
        }
        /* Negative audio samplerate: rejected. */
        let audio = TrackConfiguration.audio(
            name: "a", codec: "mp4a.40.2", bitrate: 1,
            samplerate: -48_000, channelConfig: "2")
        #expect(throws: MoQServiceError.self) { try audio.validate() }

        /* Receiver overflow limits must be positive when given. */
        for policy in [
            MediaReceiver.Configuration.OverflowPolicy.dropToKeyframe(maxObjects: 0),
            .dropGroup(maxBytes: -1),
            .flowControl(maxObjects: -5),
        ] {
            let cfg = MediaReceiver.Configuration(
                namespace: "live/cam1", overflowPolicy: policy)
            #expect(throws: MoQServiceError.self) { try cfg.validate() }
        }

        /* Sender queue limits must be positive when given. */
        var scfg = MediaSender.Configuration.live(namespace: "live/cam1")
        scfg.queueLimits = .init(maxObjects: -1)
        #expect(throws: MoQServiceError.self) { try scfg.validate() }
        scfg.queueLimits = .init(maxBytes: 0)
        #expect(throws: MoQServiceError.self) { try scfg.validate() }
        scfg.queueLimits = .init(maxObjects: 256, maxBytes: 1 << 20)
        #expect(throws: Never.self) { try scfg.validate() }

        /* Lossless timeout must be positive. */
        var lcfg = MediaSender.Configuration.lossless(namespace: "live/cam1")
        lcfg.sendPolicy = .lossless(timeout: .zero)
        #expect(throws: MoQServiceError.self) { try lcfg.validate() }
    }
}

@Suite("Errors and versions")
struct ErrorValueTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Errors are value-comparable")
    func errorEquality() {
        #expect(MoQServiceError.fatal(code: 5) == .fatal(code: 5))
        #expect(MoQServiceError.fatal(code: 5) != .fatal(code: 6))
        #expect(MoQServiceError.interrupted != .closed)
        #expect(MoQServiceError.invalidArgument("x") == .invalidArgument("x"))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Versions expose their draft numbers")
    func versionNumbers() {
        #expect(MoQVersion.draft16.draftNumber == 16)
        #expect(MoQVersion.draft18.draftNumber == 18)
        #expect(MoQVersion.draft16 != MoQVersion.draft18)
    }
}
