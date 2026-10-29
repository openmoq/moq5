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
    mp4aFixed[16] = UInt8(channels >> 8)
    mp4aFixed[17] = UInt8(channels & 0xFF)
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

/// One box: its size, its type and `body`.
private func box(_ type: String, _ body: Data) -> Data {
    var out = Data()
    boxHdr(&out, UInt32(8 + body.count), type)
    out.append(body)
    return out
}

/// ftyp + moov(trak(tkhd mdia(mdhd minf(stbl(stsd(<entry>)))))) with one
/// sample entry of type `entry` and a zeroed `fixed`-byte body, an optional
/// avcC box and, when `original` is given, sinf(frma schm schi(tenc)) whose
/// frma names it.
private func buildEntryInit(
    entry: UInt32, fixed: Int, avccData: [UInt8]? = nil, original: String? = nil
) -> Data {
    var body = Data(repeating: 0, count: fixed)
    if let avccData { body.append(box("avcC", Data(avccData))) }
    if let original {
        var frma = Data()
        fourCC(&frma, original)
        var schm = Data()
        writeU32(&schm, 0); fourCC(&schm, "cbcs"); writeU32(&schm, 0x0001_0000)
        var tenc = Data(repeating: 0, count: 24)
        tenc[6] = 1 // default_isProtected
        for i in 0..<16 { tenc[8 + i] = UInt8(0x30 + i) } // default_KID
        body.append(box("sinf",
                        box("frma", frma) + box("schm", schm) + box("schi", box("tenc", tenc))))
    }
    var entryBox = Data()
    writeU32(&entryBox, UInt32(8 + body.count)); writeU32(&entryBox, entry)
    entryBox.append(body)
    var stsd = Data()
    writeU32(&stsd, 0); writeU32(&stsd, 1) // version+flags, entry_count
    stsd.append(entryBox)
    var mdhd = Data()
    writeU32(&mdhd, 0); writeU32(&mdhd, 0); writeU32(&mdhd, 0); writeU32(&mdhd, 90000)
    var tkhd = Data()
    writeU32(&tkhd, 0); writeU32(&tkhd, 0); writeU32(&tkhd, 0); writeU32(&tkhd, 7)
    var ftyp = Data()
    fourCC(&ftyp, "isom"); writeU32(&ftyp, 0); fourCC(&ftyp, "isom")
    let mdia = box("mdia", box("mdhd", mdhd) + box("minf", box("stbl", box("stsd", stsd))))
    return box("ftyp", ftyp) + box("moov", box("trak", box("tkhd", tkhd) + mdia))
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
        #expect(info.sampleEntry == 0x6176_6331) // avc1
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
        #expect(info.sampleEntry == 0x6d70_3461) // mp4a
    }

    @Test("An encrypted entry reports its frma original format, never encv")
    func encryptedSampleEntry() throws {
        let avcc: [UInt8] = [0x01, 0x64, 0x00, 0x1F, 0xFF]
        let data = buildEntryInit(
            entry: 0x656e_6376, fixed: 78, avccData: avcc, original: "avc3") // encv

        let info = try CMAFParser.parseInit(data)

        #expect(info.codecKind == .avc)
        #expect(info.sampleEntry == 0x6176_6333) // avc3
        #expect(info.codecConfig == Data(avcc))
    }

    @Test("Unknown and zero sample entries are reported exactly as read",
          arguments: [UInt32(0x7a7a_7a7a), 0, 0xffff_ffff]) // zzzz, zero, all ones
    func unknownSampleEntry(entry: UInt32) throws {
        let info = try CMAFParser.parseInit(buildEntryInit(entry: entry, fixed: 16))

        #expect(info.codecKind == .unknown)
        #expect(info.sampleEntry == entry, "zero is a value, not an absence")
        #expect(info.codecConfig.isEmpty)
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
