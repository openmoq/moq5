import Testing
import Foundation
@testable import MoQMedia

// MARK: - CMAF Init Builders (minimal, for descriptor tests)

private func buildAVCInit(
    timescale: UInt32, width: UInt16, height: UInt16,
    avccData: [UInt8]
) -> Data {
    var buf = Data()
    func w32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func cc(_ s: String) { buf.append(contentsOf: Array(s.utf8)) }
    func hdr(_ sz: UInt32, _ t: String) { w32(sz); cc(t) }

    hdr(20, "ftyp"); cc("isom"); w32(0); cc("isom")
    let avccBox = 8 + avccData.count
    let avc1 = 8 + 78 + avccBox; let stsd = 8 + 8 + avc1
    let stbl = 8 + stsd; let minf = 8 + stbl
    let mdhd = 24; let mdia = 8 + mdhd + minf
    let trak = 8 + mdia; let moov = 8 + trak

    hdr(UInt32(moov), "moov"); hdr(UInt32(trak), "trak")
    hdr(UInt32(mdia), "mdia"); hdr(UInt32(mdhd), "mdhd")
    w32(0); w32(0); w32(0); w32(timescale)
    hdr(UInt32(minf), "minf"); hdr(UInt32(stbl), "stbl")
    hdr(UInt32(stsd), "stsd"); w32(0); w32(1)
    hdr(UInt32(avc1), "avc1")
    var fixed = Data(repeating: 0, count: 78)
    fixed[24] = UInt8(width >> 8); fixed[25] = UInt8(width & 0xFF)
    fixed[26] = UInt8(height >> 8); fixed[27] = UInt8(height & 0xFF)
    buf.append(fixed)
    hdr(UInt32(avccBox), "avcC"); buf.append(contentsOf: avccData)
    return buf
}

private func buildAACInit(
    timescale: UInt32, samplerate: UInt16, channels: UInt16,
    ascData: [UInt8]
) -> Data {
    var buf = Data()
    func w32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func w16(_ v: UInt16) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func cc(_ s: String) { buf.append(contentsOf: Array(s.utf8)) }
    func hdr(_ sz: UInt32, _ t: String) { w32(sz); cc(t) }

    hdr(20, "ftyp"); cc("isom"); w32(0); cc("isom")
    let decSpecLen = 2 + ascData.count
    let decCfgLen = 2 + 13 + decSpecLen
    let esDescLen = 2 + 3 + decCfgLen
    let esdsBody = 4 + esDescLen; let esdsSize = 8 + esdsBody
    let mp4aSize = 8 + 28 + esdsSize; let stsdSize = 8 + 8 + mp4aSize
    let stblSize = 8 + stsdSize; let minfSize = 8 + stblSize
    let mdhdSize = 24; let mdiaSize = 8 + mdhdSize + minfSize
    let trakSize = 8 + mdiaSize; let moovSize = 8 + trakSize

    hdr(UInt32(moovSize), "moov"); hdr(UInt32(trakSize), "trak")
    hdr(UInt32(mdiaSize), "mdia"); hdr(UInt32(mdhdSize), "mdhd")
    w32(0); w32(0); w32(0); w32(timescale)
    hdr(UInt32(minfSize), "minf"); hdr(UInt32(stblSize), "stbl")
    hdr(UInt32(stsdSize), "stsd"); w32(0); w32(1)
    hdr(UInt32(mp4aSize), "mp4a")
    var mp4aFixed = Data(repeating: 0, count: 28)
    mp4aFixed[8] = UInt8(channels >> 8); mp4aFixed[9] = UInt8(channels & 0xFF)
    mp4aFixed[24] = UInt8(samplerate >> 8); mp4aFixed[25] = UInt8(samplerate & 0xFF)
    buf.append(mp4aFixed)
    hdr(UInt32(esdsSize), "esds"); w32(0)
    buf.append(0x03); buf.append(UInt8(esDescLen - 2))
    w16(0); buf.append(0)
    buf.append(0x04); buf.append(UInt8(decCfgLen - 2))
    buf.append(0x40); buf.append(0x15)
    buf.append(contentsOf: [0, 0, 0]); w32(0); w32(0)
    buf.append(0x05); buf.append(UInt8(ascData.count))
    buf.append(contentsOf: ascData)
    return buf
}

// MARK: - CMAF Tests

@Suite("PlaybackTrackDescriptor — CMAF")
struct DescriptorCMAFTests {

    @Test("CMAF video descriptor")
    func cmafVideo() throws {
        let avcc: [UInt8] = [0x01, 0x64, 0x00, 0x1F, 0xFF]
        let initSeg = buildAVCInit(
            timescale: 90000, width: 1920, height: 1080, avccData: avcc)

        let track = MSFTrack(
            name: "video", packaging: "cmaf", isLive: true,
            role: "video", codec: "avc1.64001f",
            initData: initSeg.base64EncodedString(),
            width: 1920, height: 1080, timescale: 90000)

        let desc = try track.playbackDescriptor()

        #expect(desc.configuration.mediaType == .video)
        #expect(desc.configuration.packaging == .cmaf)
        #expect(desc.configuration.codec == "avc1.64001f")
        #expect(desc.configuration.timescale == 90000)
        #expect(desc.configuration.width == 1920)
        #expect(desc.configuration.height == 1080)
        #expect(desc.configuration.isLive == true)
        #expect(desc.configuration.initData != nil)
        #expect(desc.codecKind == .avc)
        #expect(desc.codecConfig == Data(avcc))
        #expect(desc.catalogTrack.name == "video")
    }

    @Test("CMAF audio descriptor")
    func cmafAudio() throws {
        let asc: [UInt8] = [0x12, 0x10]
        let initSeg = buildAACInit(
            timescale: 48000, samplerate: 48000, channels: 2,
            ascData: asc)

        let track = MSFTrack(
            name: "audio", packaging: "cmaf", isLive: true,
            role: "audio", codec: "mp4a.40.2",
            initData: initSeg.base64EncodedString(),
            samplerate: 48000, timescale: 48000,
            channelConfig: "2")

        let desc = try track.playbackDescriptor()

        #expect(desc.configuration.mediaType == .audio)
        #expect(desc.configuration.packaging == .cmaf)
        #expect(desc.configuration.samplerate == 48000)
        #expect(desc.configuration.channelCount == 2)
        #expect(desc.codecKind == .aac)
        #expect(desc.codecConfig == Data(asc))
    }
}

// MARK: - LOC Tests

@Suite("PlaybackTrackDescriptor — LOC")
struct DescriptorLOCTests {

    @Test("LOC video descriptor")
    func locVideo() throws {
        let avcc: [UInt8] = [0x01, 0x64, 0x00, 0x28, 0xFF]
        let track = MSFTrack(
            name: "video-0", packaging: "loc", isLive: true,
            role: "video", codec: "avc1.640028",
            initData: Data(avcc).base64EncodedString(),
            width: 1920, height: 1080)

        let desc = try track.playbackDescriptor()

        #expect(desc.configuration.mediaType == .video)
        #expect(desc.configuration.packaging == .raw)
        #expect(desc.configuration.width == 1920)
        #expect(desc.configuration.height == 1080)
        #expect(desc.codecKind == nil)
        #expect(desc.codecConfig == Data(avcc))
        #expect(desc.configuration.initData == Data(avcc))
    }

    @Test("LOC audio descriptor")
    func locAudio() throws {
        let asc: [UInt8] = [0x12, 0x10]
        let track = MSFTrack(
            name: "audio-0", packaging: "loc", isLive: true,
            role: "audio", codec: "mp4a.40.2",
            initData: Data(asc).base64EncodedString(),
            samplerate: 48000, channelConfig: "2")

        let desc = try track.playbackDescriptor()

        #expect(desc.configuration.mediaType == .audio)
        #expect(desc.configuration.packaging == .raw)
        #expect(desc.configuration.samplerate == 48000)
        #expect(desc.configuration.channelCount == 2)
        #expect(desc.codecKind == nil)
        #expect(desc.codecConfig == Data(asc))
    }
}

// MARK: - Error Tests

@Suite("PlaybackTrackDescriptor — Errors")
struct DescriptorErrorTests {

    @Test("Missing role throws")
    func missingRole() throws {
        let track = MSFTrack(
            name: "t", packaging: "loc", isLive: true,
            initData: Data([0x01]).base64EncodedString())

        #expect(throws: MediaTrackInfoError.missingRole) {
            _ = try track.playbackDescriptor()
        }
    }

    @Test("Unsupported packaging throws")
    func unsupportedPackaging() throws {
        let track = MSFTrack(
            name: "t", packaging: "mp2t", isLive: true,
            role: "video",
            initData: Data([0x01]).base64EncodedString())

        #expect(throws: MediaTrackInfoError.unsupportedPackaging("mp2t")) {
            _ = try track.playbackDescriptor()
        }
    }

    @Test("Missing initData throws")
    func missingInitData() throws {
        let track = MSFTrack(
            name: "video", packaging: "loc", isLive: true,
            role: "video", codec: "avc1")

        #expect(throws: MSFError.missingInitData) {
            _ = try track.playbackDescriptor()
        }
    }
}
