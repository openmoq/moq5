import Testing
import Foundation
import CMoQPlayback
@testable import MoQ
@testable import MoQMedia

// MARK: - MediaTimeRebaser

@Suite("MediaTimeRebaser")
struct MediaTimeRebaserTests {

    @Test("First rebase returns 0 and establishes base")
    func firstRebase() throws {
        var rebaser = MediaTimeRebaser()
        #expect(!rebaser.isEstablished)

        let result = rebaser.rebase(1_000_000)
        #expect(result == 0)
        #expect(rebaser.isEstablished)
    }

    @Test("Subsequent rebase returns positive delta")
    func positiveDelta() throws {
        var rebaser = MediaTimeRebaser()
        _ = rebaser.rebase(1_000_000)

        let delta = rebaser.rebase(1_041_666)
        #expect(delta == 41_666)
    }

    @Test("Reset clears the base")
    func reset() throws {
        var rebaser = MediaTimeRebaser()
        _ = rebaser.rebase(1_000_000)
        #expect(rebaser.isEstablished)

        rebaser.reset()
        #expect(!rebaser.isEstablished)

        let result = rebaser.rebase(2_000_000)
        #expect(result == 0)
    }

    @Test("Large forward delta")
    func largeDelta() throws {
        var rebaser = MediaTimeRebaser()
        _ = rebaser.rebase(0)

        let delta = rebaser.rebase(10_000_000_000)
        #expect(delta == 10_000_000_000)
    }

    @Test("First timestamp at UInt64.max returns 0")
    func maxBase() throws {
        var rebaser = MediaTimeRebaser()
        let result = rebaser.rebase(UInt64.max)
        #expect(result == 0)
        #expect(rebaser.isEstablished)
    }

    @Test("Base near UInt64.max with small forward delta")
    func nearMaxForward() throws {
        var rebaser = MediaTimeRebaser()
        _ = rebaser.rebase(UInt64.max - 100)
        let delta = rebaser.rebase(UInt64.max - 50)
        #expect(delta == 50)
    }

    @Test("Timestamp lower than base returns negative delta")
    func negativeDelta() throws {
        var rebaser = MediaTimeRebaser()
        _ = rebaser.rebase(1_000_000)
        let delta = rebaser.rebase(900_000)
        #expect(delta == -100_000)
    }

    @Test("Huge forward delta saturates to Int64.max")
    func hugeDeltaSaturates() throws {
        var rebaser = MediaTimeRebaser()
        _ = rebaser.rebase(0)
        let delta = rebaser.rebase(UInt64.max)
        #expect(delta == Int64.max)
    }
}

// MARK: - Buffer.sliceData

@Suite("Buffer.sliceData")
struct BufferSliceTests {

    @Test("Full range")
    func fullRange() throws {
        let buf = try Buffer(Data([0xCA, 0xFE, 0xBA, 0xBE]))
        let data = buf.sliceData(0..<4)
        #expect(data == Data([0xCA, 0xFE, 0xBA, 0xBE]))
    }

    @Test("Subrange")
    func subrange() throws {
        let buf = try Buffer(Data([0x00, 0x01, 0x02, 0x03, 0x04]))
        let data = buf.sliceData(1..<4)
        #expect(data == Data([0x01, 0x02, 0x03]))
    }

    @Test("Empty range")
    func emptyRange() throws {
        let buf = try Buffer(Data([0xAA, 0xBB]))
        let data = buf.sliceData(1..<1)
        #expect(data.isEmpty)
    }
}

// MARK: - PlaybackCommand Accessors

@Suite("PlaybackCommand Accessors")
struct PlaybackCommandAccessorTests {

    @Test("decodeCMAF mediaData returns mdat slice")
    func cmafMediaData() throws {
        let fragment = try Buffer(Data([
            0x00, 0x00, 0x00, 0x00,  // moof header (fake)
            0xCA, 0xFE, 0xBA, 0xBE,  // mdat payload
        ]))
        let cmd = PlaybackCommand.decodeCMAF(
            track: PlaybackTrack(moq_playback_track_t(_opaque: 1)),
            groupID: 0, objectID: 0,
            decodeTimeUS: 0, compositionOffsetUS: 0,
            presentationTimeUS: 0,
            hasCaptureTime: false, captureTimeUS: 0,
            keyframe: true, sampleDurationUS: 33333,
            fragment: fragment, mdatRange: 4..<8)

        let data = cmd.mediaData
        #expect(data == Data([0xCA, 0xFE, 0xBA, 0xBE]))
    }

    @Test("decodeVideo mediaData returns full payload")
    func videoMediaData() throws {
        let payload = try Buffer(Data([0xDE, 0xAD]))
        let cmd = PlaybackCommand.decodeVideo(
            track: PlaybackTrack(moq_playback_track_t(_opaque: 1)),
            groupID: 0, objectID: 0,
            decodeTimeUS: 0, compositionOffsetUS: 0,
            presentationTimeUS: 0,
            hasCaptureTime: false, captureTimeUS: 0,
            keyframe: true, payload: payload)

        #expect(cmd.mediaData == Data([0xDE, 0xAD]))
    }

    @Test("decodeAudio mediaData with mdat range")
    func audioMediaDataCMAF() throws {
        let payload = try Buffer(Data([0x00, 0x01, 0x02, 0x03]))
        let cmd = PlaybackCommand.decodeAudio(
            track: PlaybackTrack(moq_playback_track_t(_opaque: 1)),
            groupID: 0, objectID: 0,
            decodeTimeUS: 0, compositionOffsetUS: 0,
            presentationTimeUS: 0,
            hasCaptureTime: false, captureTimeUS: 0,
            sampleDurationUS: 21333,
            payload: payload, mdatRange: 1..<3)

        #expect(cmd.mediaData == Data([0x01, 0x02]))
    }

    @Test("decodeAudio mediaData with empty mdat range returns full payload")
    func audioMediaDataRAW() throws {
        let payload = try Buffer(Data([0xAA, 0xBB]))
        let cmd = PlaybackCommand.decodeAudio(
            track: PlaybackTrack(moq_playback_track_t(_opaque: 1)),
            groupID: 0, objectID: 0,
            decodeTimeUS: 0, compositionOffsetUS: 0,
            presentationTimeUS: 0,
            hasCaptureTime: false, captureTimeUS: 0,
            sampleDurationUS: 21333,
            payload: payload, mdatRange: 0..<0)

        #expect(cmd.mediaData == Data([0xAA, 0xBB]))
    }

    @Test("configureVideo codecString and codecConfigData")
    func configureVideoAccessors() throws {
        let codec = try Buffer(Data("avc1".utf8))
        let config = try Buffer(Data([0x01, 0x64, 0x00, 0x1F]))
        let cmd = PlaybackCommand.configureVideo(
            track: PlaybackTrack(moq_playback_track_t(_opaque: 1)),
            codec: codec, codecConfig: config,
            width: 1920, height: 1080)

        #expect(cmd.codecString == "avc1")
        #expect(cmd.codecConfigData == Data([0x01, 0x64, 0x00, 0x1F]))
        #expect(cmd.mediaData == nil)
    }

    @Test("configureAudio codecString and codecConfigData")
    func configureAudioAccessors() throws {
        let codec = try Buffer(Data("mp4a.40.2".utf8))
        let config = try Buffer(Data([0x12, 0x10]))
        let cmd = PlaybackCommand.configureAudio(
            track: PlaybackTrack(moq_playback_track_t(_opaque: 1)),
            codec: codec, codecConfig: config,
            samplerate: 48000, channelCount: 2)

        #expect(cmd.codecString == "mp4a.40.2")
        #expect(cmd.codecConfigData == Data([0x12, 0x10]))
    }

    @Test("reset returns nil for all accessors")
    func resetNil() throws {
        let cmd = PlaybackCommand.reset(
            track: PlaybackTrack(moq_playback_track_t(_opaque: 1)),
            reason: .gap)

        #expect(cmd.mediaData == nil)
        #expect(cmd.codecString == nil)
        #expect(cmd.codecConfigData == nil)
    }
}
