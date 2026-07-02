import Testing
import Foundation
import CMoQService
@testable import MoQService

/* Deterministic, NO-SOCKET sender-bridge tests: configuration/track-config
 * mapping into the real C structs and the SAP enum round-trip. Live write/
 * wait paths need a real sender and stay behind MOQ_SERVICE_NET_SMOKE. */

@Suite("Sender configuration mapping")
struct SenderConfigurationMappingTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Send policies map to the C backpressure modes")
    func policyMapping() throws {
        withSenderCFG(.live(namespace: "live/cam1")) { cfg in
            #expect(cfg.backpressure == MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME)
            #expect(cfg.validate_cmaf == true)      /* strict by default */
            #expect(cfg.queue_max_objects == 0)     /* library defaults */
        }
        withSenderCFG(.lossless(namespace: "live/cam1")) { cfg in
            /* Lossless is the SWIFT retry loop over C fail-fast -- never
             * BLOCK_TIMEOUT (uncancellable on the service thread). */
            #expect(cfg.backpressure == MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK)
        }
        withSenderCFG(.init(namespace: "live/cam1", sendPolicy: .failFast)) { cfg in
            #expect(cfg.backpressure == MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK)
        }
        withSenderCFG(.init(namespace: "live/cam1", sendPolicy: .dropGroup)) { cfg in
            #expect(cfg.backpressure == MOQ_MEDIA_SEND_BP_DROP_GROUP)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Queue limits, namespace, and catalog track map through")
    func limitsAndNamespace() throws {
        var configuration = MediaSender.Configuration(
            namespace: "live/cam1", sendPolicy: .dropToKeyframe)
        configuration.queueLimits = .init(maxObjects: 64, maxBytes: 1 << 16)
        configuration.validateCMAF = false
        configuration.catalogTrack = "alt-catalog"

        withSenderCFG(configuration) { cfg in
            #expect(cfg.namespace_.count == 2)
            #expect(swiftString(cfg.namespace_.parts![1]) == "cam1")
            #expect(cfg.queue_max_objects == 64)
            #expect(cfg.queue_max_bytes == 1 << 16)
            #expect(cfg.validate_cmaf == false)
            #expect(swiftString(cfg.catalog_track) == "alt-catalog")
            #expect(cfg.endpoint == nil)            /* attach borrows */
        }
    }
}

@Suite("Track configuration mapping")
struct TrackConfigurationMappingTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A video track maps field-for-field")
    func videoTrack() throws {
        var configuration = TrackConfiguration.video(
            name: "video", codec: "avc1.64001f", bitrate: 3_000_000,
            width: 1280, height: 720, initData: Data("SPS".utf8))
        configuration.framerateMillis = 30_000
        configuration.timescale = 90_000
        configuration.role = "main"

        withTrackCFG(configuration) { cfg in
            #expect(swiftString(cfg.name) == "video")
            #expect(cfg.media_type == MOQ_MEDIA_TYPE_VIDEO)
            #expect(cfg.packaging == MOQ_MEDIA_PACKAGING_RAW)
            #expect(swiftString(cfg.codec) == "avc1.64001f")
            #expect(cfg.bitrate == 3_000_000)
            #expect(cfg.width == 1280)
            #expect(cfg.height == 720)
            #expect(cfg.framerate_millis == 30_000)
            #expect(cfg.timescale == 90_000)
            #expect(swiftString(cfg.role) == "main")
            #expect(cfg.is_live == true)
            #expect(swiftString(cfg.init_data) == "SPS")
            #expect(cfg.samplerate == 0)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("An audio track maps its required audio fields")
    func audioTrack() throws {
        let configuration = TrackConfiguration.audio(
            name: "audio", codec: "mp4a.40.2", bitrate: 128_000,
            samplerate: 48_000, channelConfig: "2")
        withTrackCFG(configuration) { cfg in
            #expect(cfg.media_type == MOQ_MEDIA_TYPE_AUDIO)
            #expect(cfg.samplerate == 48_000)
            #expect(swiftString(cfg.channel_config) == "2")
            #expect(cfg.timescale == 0)             /* 0 = C default (us) */
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("SAP types map to the CMSF enum")
    func sapMapping() {
        #expect(cSAPType(.type1) == MOQ_SAP_TYPE_1)
        #expect(cSAPType(.type2) == MOQ_SAP_TYPE_2)
        #expect(cSAPType(.type3) == MOQ_SAP_TYPE_3)
    }
}
