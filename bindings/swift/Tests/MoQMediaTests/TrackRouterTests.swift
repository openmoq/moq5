import Testing
import Foundation
import CMoQLOC
import CMoQCore
@testable import MoQ
@testable import MoQMedia

private func encodeLOCProps(
    timestamp: UInt64, keyframe: Bool = false
) throws -> Buffer? {
    var h = moq_loc_headers_t()
    moq_loc_headers_init(&h)
    h.has_timestamp = true
    h.timestamp = timestamp
    if keyframe {
        h.has_video_frame_marking = true
        h.video_frame_marking.independent = true
        h.video_frame_marking.start_of_frame = true
        h.video_frame_marking.end_of_frame = true
    }
    var p: OpaquePointer?
    let rc = moq_loc_encode(moq_alloc_default()!, MOQ_LOC_PROFILE_01, &h, &p)
    guard rc == MOQ_OK else { throw MoQError.internal }
    guard let ptr = p else { return nil }
    return Buffer(adopting: ptr)
}

@Suite("TrackRouter")
struct TrackRouterTests {

    @Test("Route one track and verify push succeeds")
    func routeOne() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let pbTrack = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .audio, packaging: .raw, codec: "opus"))

        let subTrack = SubscribedTrack(OpaquePointer(bitPattern: 42)!)

        var router = TrackRouter()
        router.add(subscribed: subTrack, playback: pbTrack)

        let props = try encodeLOCProps(timestamp: 1_000_000)!
        let obj = FacadeReceivedObject(
            track: subTrack,
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: try Buffer(Data([0x01])),
            properties: props)

        let routed = try router.pushIfRouted(
            object: obj, pipeline: pb, now: 1_000_000)
        #expect(routed == true)

        try pb.tick(now: 1_000_000)
        var cmds: [PlaybackCommand] = []
        try pb.drainCommands { cmds.append($0) }
        #expect(!cmds.isEmpty)
    }

    @Test("Unknown track returns false")
    func unknownTrack() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let _ = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .audio, packaging: .raw, codec: "opus"))

        let unknownSub = SubscribedTrack(OpaquePointer(bitPattern: 99)!)
        let router = TrackRouter()

        let obj = FacadeReceivedObject(
            track: unknownSub,
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: try Buffer(Data([0x01])),
            properties: nil)

        let routed = try router.pushIfRouted(
            object: obj, pipeline: pb, now: 0)
        #expect(routed == false)
    }

    @Test("Multiple routes dispatch to correct track")
    func multipleRoutes() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let pbVideo = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .video, packaging: .raw, codec: "avc1"))
        let pbAudio = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .audio, packaging: .raw, codec: "opus"))

        let subVideo = SubscribedTrack(OpaquePointer(bitPattern: 10)!)
        let subAudio = SubscribedTrack(OpaquePointer(bitPattern: 20)!)

        var router = TrackRouter()
        router.add(subscribed: subVideo, playback: pbVideo)
        router.add(subscribed: subAudio, playback: pbAudio)

        let propsV = try encodeLOCProps(timestamp: 1_000_000, keyframe: true)!
        let videoObj = FacadeReceivedObject(
            track: subVideo,
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: try Buffer(Data([0x65])),
            properties: propsV)

        let propsA = try encodeLOCProps(timestamp: 1_000_000)!
        let audioObj = FacadeReceivedObject(
            track: subAudio,
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: try Buffer(Data([0x01])),
            properties: propsA)

        #expect(try router.pushIfRouted(
            object: videoObj, pipeline: pb, now: 1_000_000))
        #expect(try router.pushIfRouted(
            object: audioObj, pipeline: pb, now: 1_000_000))

        try pb.tick(now: 1_000_000)
        var cmds: [PlaybackCommand] = []
        try pb.drainCommands { cmds.append($0) }

        let videoCmd = cmds.contains {
            if case .configureVideo(let t, _, _, _, _) = $0 {
                return t == pbVideo
            }; return false
        }
        let audioCmd = cmds.contains {
            if case .configureAudio(let t, _, _, _, _) = $0 {
                return t == pbAudio
            }; return false
        }
        #expect(videoCmd)
        #expect(audioCmd)
    }

    @Test("Duplicate add replaces playback track")
    func duplicateReplace() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let pbTrack1 = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .audio, packaging: .raw, codec: "opus"))
        let pbTrack2 = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .audio, packaging: .raw, codec: "aac"))

        let subTrack = SubscribedTrack(OpaquePointer(bitPattern: 42)!)

        var router = TrackRouter()
        router.add(subscribed: subTrack, playback: pbTrack1)
        router.add(subscribed: subTrack, playback: pbTrack2)

        let props = try encodeLOCProps(timestamp: 1_000_000)!
        let obj = FacadeReceivedObject(
            track: subTrack,
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: try Buffer(Data([0x01])),
            properties: props)

        #expect(try router.pushIfRouted(
            object: obj, pipeline: pb, now: 1_000_000))

        try pb.tick(now: 1_000_000)
        var cmds: [PlaybackCommand] = []
        try pb.drainCommands { cmds.append($0) }

        // Configure should be for pbTrack2, not pbTrack1.
        let hasTrack2 = cmds.contains {
            if case .configureAudio(let t, _, _, _, _) = $0 {
                return t == pbTrack2
            }; return false
        }
        let hasTrack1 = cmds.contains {
            if case .configureAudio(let t, _, _, _, _) = $0 {
                return t == pbTrack1
            }; return false
        }
        #expect(hasTrack2)
        #expect(!hasTrack1)
    }
}
