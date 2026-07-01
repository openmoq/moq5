import Testing
import Foundation
@testable import MoQMedia

// MARK: - Init Segment Builders

private func writeU32(_ buf: inout Data, _ v: UInt32) {
    withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) }
}
private func writeU16(_ buf: inout Data, _ v: UInt16) {
    withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) }
}
private func fourCC(_ buf: inout Data, _ s: String) {
    buf.append(contentsOf: Array(s.utf8))
}
private func boxHdr(_ buf: inout Data, _ size: UInt32, _ type: String) {
    writeU32(&buf, size); fourCC(&buf, type)
}

private func buildAVCInit(
    timescale: UInt32, width: UInt16, height: UInt16,
    avccData: [UInt8]
) -> Data {
    var buf = Data()

    // ftyp (20 bytes)
    boxHdr(&buf, 20, "ftyp")
    fourCC(&buf, "isom"); writeU32(&buf, 0); fourCC(&buf, "isom")

    let avccBoxSize = 8 + avccData.count
    let avc1Size = 8 + 78 + avccBoxSize
    let stsdSize = 8 + 8 + avc1Size
    let stblSize = 8 + stsdSize
    let minfSize = 8 + stblSize
    let mdhdSize = 8 + 4 + 12
    let mdiaSize = 8 + mdhdSize + minfSize
    let trakSize = 8 + mdiaSize
    let moovSize = 8 + trakSize

    boxHdr(&buf, UInt32(moovSize), "moov")
    boxHdr(&buf, UInt32(trakSize), "trak")
    boxHdr(&buf, UInt32(mdiaSize), "mdia")

    // mdhd
    boxHdr(&buf, UInt32(mdhdSize), "mdhd")
    writeU32(&buf, 0) // version+flags
    writeU32(&buf, 0) // creation_time
    writeU32(&buf, 0) // modification_time
    writeU32(&buf, timescale)

    boxHdr(&buf, UInt32(minfSize), "minf")
    boxHdr(&buf, UInt32(stblSize), "stbl")

    // stsd
    boxHdr(&buf, UInt32(stsdSize), "stsd")
    writeU32(&buf, 0) // version+flags
    writeU32(&buf, 1) // entry_count

    // avc1
    boxHdr(&buf, UInt32(avc1Size), "avc1")
    var fixed = Data(repeating: 0, count: 78)
    fixed[24] = UInt8(width >> 8)
    fixed[25] = UInt8(width & 0xFF)
    fixed[26] = UInt8(height >> 8)
    fixed[27] = UInt8(height & 0xFF)
    buf.append(fixed)

    // avcC
    boxHdr(&buf, UInt32(avccBoxSize), "avcC")
    buf.append(contentsOf: avccData)

    return buf
}

private func buildAACInit(
    timescale: UInt32, samplerate: UInt16, channels: UInt16,
    ascData: [UInt8]
) -> Data {
    var buf = Data()

    // ftyp
    boxHdr(&buf, 20, "ftyp")
    fourCC(&buf, "isom"); writeU32(&buf, 0); fourCC(&buf, "isom")

    let decSpecLen = 2 + ascData.count
    let decCfgLen = 2 + 13 + decSpecLen
    let esDescLen = 2 + 3 + decCfgLen
    let esdsBody = 4 + esDescLen
    let esdsSize = 8 + esdsBody

    let mp4aSize = 8 + 28 + esdsSize
    let stsdSize = 8 + 8 + mp4aSize
    let stblSize = 8 + stsdSize
    let minfSize = 8 + stblSize
    let mdhdSize = 8 + 4 + 12
    let mdiaSize = 8 + mdhdSize + minfSize
    let trakSize = 8 + mdiaSize
    let moovSize = 8 + trakSize

    boxHdr(&buf, UInt32(moovSize), "moov")
    boxHdr(&buf, UInt32(trakSize), "trak")
    boxHdr(&buf, UInt32(mdiaSize), "mdia")

    // mdhd
    boxHdr(&buf, UInt32(mdhdSize), "mdhd")
    writeU32(&buf, 0)
    writeU32(&buf, 0)
    writeU32(&buf, 0)
    writeU32(&buf, timescale)

    boxHdr(&buf, UInt32(minfSize), "minf")
    boxHdr(&buf, UInt32(stblSize), "stbl")

    boxHdr(&buf, UInt32(stsdSize), "stsd")
    writeU32(&buf, 0)
    writeU32(&buf, 1)

    // mp4a
    boxHdr(&buf, UInt32(mp4aSize), "mp4a")
    var mp4aFixed = Data(repeating: 0, count: 28)
    mp4aFixed[8] = UInt8(channels >> 8)
    mp4aFixed[9] = UInt8(channels & 0xFF)
    mp4aFixed[24] = UInt8(samplerate >> 8)
    mp4aFixed[25] = UInt8(samplerate & 0xFF)
    buf.append(mp4aFixed)

    // esds
    boxHdr(&buf, UInt32(esdsSize), "esds")
    writeU32(&buf, 0) // version+flags

    buf.append(0x03) // ES_Descriptor tag
    buf.append(UInt8(esDescLen - 2))
    writeU16(&buf, 0) // ES_ID
    buf.append(0) // flags

    buf.append(0x04) // DecoderConfigDescriptor tag
    buf.append(UInt8(decCfgLen - 2))
    buf.append(0x40) // objectTypeIndication: AAC
    buf.append(0x15) // streamType: audio
    buf.append(contentsOf: [0, 0, 0]) // bufferSizeDB
    writeU32(&buf, 0) // maxBitrate
    writeU32(&buf, 0) // avgBitrate

    buf.append(0x05) // DecoderSpecificInfo tag
    buf.append(UInt8(ascData.count))
    buf.append(contentsOf: ascData)

    return buf
}

// MARK: - Tests

@Suite("CMAF Init Parser")
struct CMAFInitParserTests {

    @Test("AVC init parses timescale, codec, dimensions, config")
    func avcInit() throws {
        let avcc: [UInt8] = [0x01, 0x64, 0x00, 0x1F, 0xFF]
        let data = buildAVCInit(
            timescale: 90000, width: 1920, height: 1080, avccData: avcc)

        let info = try CMAFParser.parseInit(data)

        #expect(info.timescale == 90000)
        #expect(info.codecKind == .avc)
        #expect(info.width == 1920)
        #expect(info.height == 1080)
        #expect(info.codecConfig == Data(avcc))
    }

    @Test("AAC init parses timescale, codec, samplerate, channels, config")
    func aacInit() throws {
        let asc: [UInt8] = [0x12, 0x10]
        let data = buildAACInit(
            timescale: 48000, samplerate: 48000, channels: 2, ascData: asc)

        let info = try CMAFParser.parseInit(data)

        #expect(info.timescale == 48000)
        #expect(info.codecKind == .aac)
        #expect(info.samplerate == 48000)
        #expect(info.channelCount == 2)
        #expect(info.codecConfig == Data(asc))
    }

    @Test("Malformed init throws malformed")
    func malformedInit() throws {
        let garbage = Data([0x00, 0x00, 0x00, 0x08, 0x62, 0x61, 0x64, 0x21])

        #expect(throws: CMAFParseError.malformed) {
            _ = try CMAFParser.parseInit(garbage)
        }
    }

    @Test("codecConfig Data survives after input goes out of scope")
    func codecConfigLifetime() throws {
        let avcc: [UInt8] = [0x01, 0x64, 0x00, 0x1F, 0xFF]
        let info: CMAFInitInfo
        do {
            let data = buildAVCInit(
                timescale: 90000, width: 640, height: 480, avccData: avcc)
            info = try CMAFParser.parseInit(data)
        }
        #expect(info.codecConfig == Data(avcc))
    }
}

@Suite("MSFTrack.parseCMAFInitInfo")
struct MSFTrackCMAFInitTests {

    @Test("Decode base64 initData and parse CMAF init")
    func parseCMAFFromCatalog() throws {
        let avcc: [UInt8] = [0x01, 0x64, 0x00, 0x1F]
        let initData = buildAVCInit(
            timescale: 90000, width: 1280, height: 720, avccData: avcc)

        let track = MSFTrack(
            name: "video", packaging: "cmaf", isLive: true,
            initData: initData.base64EncodedString())

        let info = try track.parseCMAFInitInfo()
        #expect(info.codecKind == .avc)
        #expect(info.timescale == 90000)
        #expect(info.width == 1280)
        #expect(info.height == 720)
    }

    @Test("Missing initData throws missingInitData")
    func missingInitData() throws {
        let track = MSFTrack(
            name: "video", packaging: "cmaf", isLive: true)
        #expect(throws: MSFError.missingInitData) {
            _ = try track.parseCMAFInitInfo()
        }
    }

    @Test("Invalid base64 throws invalidBase64")
    func invalidBase64() throws {
        let track = MSFTrack(
            name: "video", packaging: "cmaf", isLive: true,
            initData: "!!!bad!!!")
        #expect(throws: MSFError.invalidBase64) {
            _ = try track.parseCMAFInitInfo()
        }
    }
}
