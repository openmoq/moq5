#if canImport(CoreMedia)

import Testing
import Foundation
import CoreMedia
@testable import MoQService
import MoQServiceCore

/* Deterministic, NO-NETWORK CoreMedia helper tests: fixed extradata
 * vectors, package-init MediaObjects over the bridge's own
 * CopiedObjectStorage. CMVideoFormatDescriptionCreate stores extension
 * atoms verbatim (validation happens at decode time), so synthetic
 * avcC/SPS bytes are fine for construction tests. Apple-platform gated by
 * canImport. */

/* Minimal syntactically-valid avcC: version 1, baseline profile, one SPS
 * + one PPS (dummy payloads -- creation does not parse them). */
private let testAVCC = Data([
    0x01, 0x42, 0xC0, 0x1E, 0xFF,
    0xE1, 0x00, 0x04, 0x67, 0x42, 0xC0, 0x1E,
    0x01, 0x00, 0x04, 0x68, 0xCE, 0x3C, 0x80,
])

/* AudioSpecificConfig: AAC-LC, 44.1 kHz, stereo. */
private let testASC = Data([0x12, 0x10])

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func videoDescription(
    codec: String = "avc1.64001f",
    codecConfig: Data? = testAVCC,
    width: Int? = 1280, height: Int? = 720,
    framerateMillis: UInt64? = nil
) -> TrackDescription {
    var description = TrackDescription(
        name: "video", mediaType: .video, packaging: .raw)
    description.codec = codec
    description.codecConfig = codecConfig
    description.width = width
    description.height = height
    description.framerateMillis = framerateMillis
    return description
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
private func makeObject(media: [UInt8], keyframe: Bool,
                        packaging: MediaPackaging = .raw,
                        framerateMillis: UInt64? = nil) -> MediaObject {
    var description = TrackDescription(
        name: "v", mediaType: .video, packaging: packaging)
    description.framerateMillis = framerateMillis
    return MediaObject(
        storage: CopiedObjectStorage(media: Data(media), fragment: Data()),
        track: MediaTrack(description: description),
        isKeyframe: keyframe, endsGroup: false, isDatagram: false,
        presentationTime: .milliseconds(40),
        decodeTime: .milliseconds(33))
}

@Suite("Format descriptions")
struct FormatDescriptionTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("H.264 descriptions carry dimensions and the avcC atom")
    func h264() throws {
        let format = try videoDescription().makeFormatDescription()
        #expect(CMFormatDescriptionGetMediaType(format) == kCMMediaType_Video)
        #expect(CMFormatDescriptionGetMediaSubType(format)
                == kCMVideoCodecType_H264)
        let dimensions = CMVideoFormatDescriptionGetDimensions(format)
        #expect(dimensions.width == 1280)
        #expect(dimensions.height == 720)
        let atoms = CMFormatDescriptionGetExtension(
            format,
            extensionKey:
                kCMFormatDescriptionExtension_SampleDescriptionExtensionAtoms
        ) as? NSDictionary
        #expect((atoms?["avcC"] as? Data) == testAVCC)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("AAC descriptions carry the ASBD and the ASC cookie")
    func aac() throws {
        var description = TrackDescription(
            name: "audio", mediaType: .audio, packaging: .raw)
        description.codec = "mp4a.40.2"
        description.codecConfig = testASC
        description.samplerate = 44_100
        description.channelConfig = "2"

        let format = try description.makeFormatDescription()
        #expect(CMFormatDescriptionGetMediaType(format) == kCMMediaType_Audio)
        let asbd = CMAudioFormatDescriptionGetStreamBasicDescription(format)
        #expect(asbd?.pointee.mSampleRate == 44_100)
        #expect(asbd?.pointee.mChannelsPerFrame == 2)
        #expect(asbd?.pointee.mFormatID == kAudioFormatMPEG4AAC)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Strict policy: no codecConfig means a precise error, never a sniff")
    func strictExtradataPolicy() {
        var description = videoDescription(codecConfig: nil)
        description.initData = testAVCC    /* present but deliberately unused */
        #expect(throws: MoQServiceError.self) {
            _ = try description.makeFormatDescription()
        }
        /* The out-of-band overload is the sanctioned escape hatch. */
        #expect(throws: Never.self) {
            _ = try description.makeFormatDescription(codecConfig: testAVCC)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Unsupported codecs and missing fields fail precisely")
    func errorPaths() {
        #expect(throws: MoQServiceError.unsupported) {
            _ = try videoDescription(codec: "av01.0.04M.08")
                .makeFormatDescription()
        }
        #expect(throws: MoQServiceError.self) {
            _ = try videoDescription(width: nil, height: nil)
                .makeFormatDescription()
        }
        var audio = TrackDescription(
            name: "a", mediaType: .audio, packaging: .raw)
        audio.codec = "mp4a.40.2"
        audio.codecConfig = testASC
        audio.samplerate = 48_000
        audio.channelConfig = "stereo"     /* non-numeric */
        #expect(throws: MoQServiceError.self) {
            _ = try audio.makeFormatDescription()
        }
    }
}

@Suite("Sample buffers")
struct SampleBufferTests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Bytes round-trip through the block buffer (copied, independent)")
    func blockBufferRoundTrip() throws {
        let object = makeObject(media: [1, 2, 3, 4, 5], keyframe: true)
        let block = try object.makeBlockBuffer()
        #expect(CMBlockBufferGetDataLength(block) == 5)
        var copied = [UInt8](repeating: 0, count: 5)
        #expect(CMBlockBufferCopyDataBytes(
            block, atOffset: 0, dataLength: 5, destination: &copied) == noErr)
        #expect(copied == [1, 2, 3, 4, 5])
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Video timing: pts/dts map at µs precision; framerate sets duration")
    func videoTiming() throws {
        let format = try videoDescription().makeFormatDescription()
        let object = makeObject(media: [9], keyframe: true,
                                framerateMillis: 30_000)
        let sample = try object.makeSampleBuffer(formatDescription: format)

        #expect(CMSampleBufferGetNumSamples(sample) == 1)
        let pts = CMSampleBufferGetPresentationTimeStamp(sample)
        #expect(pts.value == 40_000 && pts.timescale == 1_000_000)
        let dts = CMSampleBufferGetDecodeTimeStamp(sample)
        #expect(dts.value == 33_000 && dts.timescale == 1_000_000)
        let duration = CMSampleBufferGetDuration(sample)
        #expect(duration.value == 33_333)   /* 1e9 / 30_000 µs */

        /* No framerate -> invalid duration, never a guess. */
        let untimed = makeObject(media: [9], keyframe: true)
        let sample2 = try untimed.makeSampleBuffer(formatDescription: format)
        #expect(!CMSampleBufferGetDuration(sample2).isValid)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Non-keyframe video carries NotSync/DependsOnOthers; keyframes do not")
    func syncAttachments() throws {
        let format = try videoDescription().makeFormatDescription()

        let delta = try makeObject(media: [1], keyframe: false)
            .makeSampleBuffer(formatDescription: format)
        let attachments = CMSampleBufferGetSampleAttachmentsArray(
            delta, createIfNecessary: false) as? [NSDictionary]
        #expect(attachments?.first?[kCMSampleAttachmentKey_NotSync]
                as? Bool == true)
        #expect(attachments?.first?[kCMSampleAttachmentKey_DependsOnOthers]
                as? Bool == true)

        let key = try makeObject(media: [1], keyframe: true)
            .makeSampleBuffer(formatDescription: format)
        let keyAttachments = CMSampleBufferGetSampleAttachmentsArray(
            key, createIfNecessary: false) as? [NSDictionary]
        #expect(keyAttachments?.first?[kCMSampleAttachmentKey_NotSync]
                as? Bool != true)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Audio duration is invalid: framerate is never applied to audio")
    func audioDuration() throws {
        var description = TrackDescription(
            name: "audio", mediaType: .audio, packaging: .raw)
        description.codec = "mp4a.40.2"
        description.codecConfig = testASC
        description.samplerate = 44_100
        description.channelConfig = "2"
        /* Even a (bogus) framerate on the track must not become an audio
         * sample duration. */
        description.framerateMillis = 30_000
        let format = try description.makeFormatDescription()

        let object = MediaObject(
            storage: CopiedObjectStorage(media: Data([7]), fragment: Data()),
            track: MediaTrack(description: description),
            isKeyframe: true, endsGroup: false, isDatagram: false,
            presentationTime: .milliseconds(20), decodeTime: .milliseconds(20))
        let sample = try object.makeSampleBuffer(formatDescription: format)
        #expect(!CMSampleBufferGetDuration(sample).isValid)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Media-type mismatches are refused, never silently mislabeled")
    func mediaTypeMismatch() throws {
        var audioDescription = TrackDescription(
            name: "audio", mediaType: .audio, packaging: .raw)
        audioDescription.codec = "mp4a.40.2"
        audioDescription.codecConfig = testASC
        audioDescription.samplerate = 44_100
        audioDescription.channelConfig = "2"
        let audioFormat = try audioDescription.makeFormatDescription()
        let videoFormat = try videoDescription().makeFormatDescription()

        /* A VIDEO object with an AUDIO format description (and vice
         * versa) must fail loudly -- a mislabeled sample buffer poisons
         * AV pipelines downstream. */
        let videoObject = makeObject(media: [1], keyframe: true)
        #expect(throws: MoQServiceError.self) {
            _ = try videoObject.makeSampleBuffer(formatDescription: audioFormat)
        }
        let audioObject = MediaObject(
            storage: CopiedObjectStorage(media: Data([7]), fragment: Data()),
            track: MediaTrack(description: audioDescription),
            isKeyframe: true, endsGroup: false, isDatagram: false,
            presentationTime: .zero, decodeTime: .zero)
        #expect(throws: MoQServiceError.self) {
            _ = try audioObject.makeSampleBuffer(formatDescription: videoFormat)
        }

        /* And the same consistency at description level: a codec family
         * that contradicts the declared mediaType is refused. */
        var lying = videoDescription()
        lying.codec = "mp4a.40.2"       /* audio codec on a video track */
        #expect(throws: MoQServiceError.self) {
            _ = try lying.makeFormatDescription(codecConfig: testASC)
        }
        var lyingAudio = audioDescription
        lyingAudio.codec = "avc1.64001f"
        #expect(throws: MoQServiceError.self) {
            _ = try lyingAudio.makeFormatDescription(codecConfig: testAVCC)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("CMAF objects are unsupported until the sample table is surfaced")
    func cmafUnsupported() throws {
        let format = try videoDescription().makeFormatDescription()
        let cmaf = makeObject(media: [1, 2], keyframe: true, packaging: .cmaf)
        #expect(throws: MoQServiceError.unsupported) {
            _ = try cmaf.makeSampleBuffer(formatDescription: format)
        }
    }
}

#endif  /* canImport(CoreMedia) */
