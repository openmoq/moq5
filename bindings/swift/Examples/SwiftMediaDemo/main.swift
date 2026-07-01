import Foundation
import MoQ
import MoQMedia

// MARK: - Local Pump Helpers

func pumpAll(from source: Session, to dest: Session) throws {
    try source.drainActions { action in
        switch action {
        case .sendControl(let data):
            try data.withUnsafeBytes {
                try dest.receiveControlBytes($0, now: 0)
            }
        case .sendData(let stream, let header, let payload, let fin):
            var bytes = [UInt8](header)
            if let buf = payload {
                buf.withUnsafeBytes { bytes.append(contentsOf: $0) }
            }
            try bytes.withUnsafeBufferPointer { ptr in
                let raw = UnsafeRawBufferPointer(
                    start: ptr.baseAddress, count: ptr.count)
                try dest.receiveDataBytes(
                    on: stream, raw, fin: fin, now: 0)
            }
        case .sendDatagram(let data):
            try data.withUnsafeBytes {
                try dest.receiveDatagram($0, now: 0)
            }
        default:
            break
        }
    }
}

func establish() throws -> (client: Session, server: Session) {
    var clientCfg = Session.Configuration(perspective: .client)
    clientCfg.sendRequestCapacity = true
    clientCfg.initialRequestCapacity = 16
    var serverCfg = Session.Configuration(perspective: .server)
    serverCfg.sendRequestCapacity = true
    serverCfg.initialRequestCapacity = 16

    let client = try Session(configuration: clientCfg)
    let server = try Session(configuration: serverCfg)
    try client.start()
    try pumpAll(from: client, to: server)
    try pumpAll(from: server, to: client)
    _ = client.pollEvents()
    _ = server.pollEvents()
    return (client, server)
}

// MARK: - CMAF Init Segment Builder

func buildAVCInit(
    timescale: UInt32, width: UInt16, height: UInt16,
    avccData: [UInt8]
) -> Data {
    var buf = Data()
    func w32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func w16(_ v: UInt16) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func cc(_ s: String)  { buf.append(contentsOf: Array(s.utf8)) }
    func hdr(_ sz: UInt32, _ t: String) { w32(sz); cc(t) }

    hdr(20, "ftyp"); cc("isom"); w32(0); cc("isom")

    let avccBox = 8 + avccData.count
    let avc1 = 8 + 78 + avccBox
    let stsd = 8 + 8 + avc1
    let stbl = 8 + stsd
    let minf = 8 + stbl
    let mdhd = 24
    let mdia = 8 + mdhd + minf
    let trak = 8 + mdia
    let moov = 8 + trak

    hdr(UInt32(moov), "moov")
    hdr(UInt32(trak), "trak")
    hdr(UInt32(mdia), "mdia")
    hdr(UInt32(mdhd), "mdhd")
    w32(0); w32(0); w32(0); w32(timescale)
    hdr(UInt32(minf), "minf")
    hdr(UInt32(stbl), "stbl")
    hdr(UInt32(stsd), "stsd"); w32(0); w32(1)
    hdr(UInt32(avc1), "avc1")
    var fixed = Data(repeating: 0, count: 78)
    fixed[24] = UInt8(width >> 8); fixed[25] = UInt8(width & 0xFF)
    fixed[26] = UInt8(height >> 8); fixed[27] = UInt8(height & 0xFF)
    buf.append(fixed)
    hdr(UInt32(avccBox), "avcC")
    buf.append(contentsOf: avccData)
    return buf
}

// MARK: - CMAF Fragment Builder

func buildCMAFFragment(
    baseDecodeTime: UInt64,
    sampleDuration: UInt32,
    sampleSize: UInt32,
    sampleFlags: UInt32,
    compositionOffset: Int32 = 0,
    mdatPayload: [UInt8]
) -> Data {
    var buf = Data()
    func w32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func w64(_ v: UInt64) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func i32(_ v: Int32)  { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func cc(_ s: String)  { buf.append(contentsOf: Array(s.utf8)) }
    func hdr(_ sz: UInt32, _ t: String) { w32(sz); cc(t) }

    let moofStart = buf.count
    hdr(0, "moof")

    hdr(16, "mfhd"); w32(0); w32(1)

    let trafStart = buf.count
    hdr(0, "traf")
    hdr(16, "tfhd"); w32(0x00020000); w32(1)
    hdr(20, "tfdt"); w32(0x01000000); w64(baseDecodeTime)

    let trunSize: UInt32 = 8 + 4 + 4 + 4 + 16
    hdr(trunSize, "trun")
    w32(0x00000F01) // data-offset|duration|size|flags|comp-offset
    w32(1) // sample_count
    let dataOffsetPos = buf.count
    w32(0) // placeholder
    w32(sampleDuration); w32(sampleSize); w32(sampleFlags); i32(compositionOffset)

    // Patch traf size
    let trafSize = UInt32(buf.count - trafStart)
    buf.replaceSubrange(trafStart..<trafStart+4,
        with: withUnsafeBytes(of: trafSize.bigEndian) { Data($0) })
    let moofSize = UInt32(buf.count - moofStart)
    buf.replaceSubrange(moofStart..<moofStart+4,
        with: withUnsafeBytes(of: moofSize.bigEndian) { Data($0) })

    let mdatSize = UInt32(8 + UInt32(mdatPayload.count))
    hdr(mdatSize, "mdat")
    buf.append(contentsOf: mdatPayload)

    let dataOffset = UInt32(buf.count - mdatPayload.count - moofStart)
    buf.replaceSubrange(dataOffsetPos..<dataOffsetPos+4,
        with: withUnsafeBytes(of: dataOffset.bigEndian) { Data($0) })

    return buf
}

// MARK: - Main

func run() throws {
    print("Swift Media Demo — in-process sans-I/O pub/sub")
    print()

    // 1. Establish sessions.
    let (client, server) = try establish()
    print("  [1] Sessions established")

    // 2. Create Publisher on server.
    let pub = try Publisher(
        session: server,
        configuration: .init(acceptMode: .acceptAll))

    // 3. Add catalog and video tracks.
    let catalogTrack = try pub.addTrack(
        namespace: ["moq-demo"], name: "catalog")
    let videoTrack = try pub.addTrack(
        namespace: ["moq-demo"], name: "video")
    print("  [2] Publisher: catalog + video tracks added")

    // 4. Build MSF catalog with one CMAF video track.
    let avccData: [UInt8] = [0x01, 0x64, 0x00, 0x1F, 0xFF, 0xE1]
    let initSegment = buildAVCInit(
        timescale: 90000, width: 1920, height: 1080, avccData: avccData)

    let catalog = MSFCatalog(version: 1, tracks: [
        MSFTrack(
            name: "video",
            packaging: "cmaf",
            isLive: true,
            namespace: "moq-demo",
            role: "video",
            codec: "avc1.64001f",
            initData: initSegment.base64EncodedString(),
            width: 1920, height: 1080,
            bitrate: 2_000_000,
            framerate: 30,
            timescale: 90000)
    ])
    let catalogJSON = try JSONEncoder().encode(catalog)
    print("  [3] MSF catalog built (\(catalogJSON.count) bytes JSON)")

    // 5. Parse initData through CMAF init parser.
    let initInfo = try CMAFParser.parseInit(initSegment)
    assert(initInfo.codecKind == .avc)
    assert(initInfo.timescale == 90000)
    assert(initInfo.width == 1920)
    assert(initInfo.height == 1080)
    assert(initInfo.codecConfig == Data(avccData))
    print("  [4] CMAF init parsed: AVC \(initInfo.width)x\(initInfo.height) @ \(initInfo.timescale) Hz")

    // 6. Retain catalog JSON for an explicit Joining FETCH (MSF-01 §5).
    try pub.setRetainedGroup(on: catalogTrack, RetainedGroupConfiguration(
        groupID: 0,
        objects: [RetainedObject(objectID: 0, payload: try Buffer(catalogJSON))]))
    print("  [5] Catalog retained for Joining FETCH")

    // 7. Client subscribes to catalog, FETCHes the retained group, decodes MSF.
    let sub = try Subscriber(session: client)

    let subCatalog = try sub.subscribe(
        track: TrackName(namespace: ["moq-demo"], name: "catalog"))

    try pumpAll(from: client, to: server)
    try pub.tick(now: 0)
    try pumpAll(from: server, to: client)
    try sub.tick(now: 0)
    assert(subCatalog.isActive)

    let catalogObj = try sub.pollObject()!
    let receivedCatalog = try JSONDecoder().decode(
        MSFCatalog.self, from: catalogObj.payload!.copyBytes())
    assert(receivedCatalog.version == 1)
    assert(receivedCatalog.tracks.count == 1)
    assert(receivedCatalog.tracks[0].name == "video")
    print("  [6] Subscriber received catalog: \(receivedCatalog.tracks.count) track(s)")

    // 8. Client subscribes to video.
    let subVideo = try sub.subscribe(
        track: TrackName(namespace: ["moq-demo"], name: "video"))

    try pumpAll(from: client, to: server)
    try pub.tick(now: 0)
    try pumpAll(from: server, to: client)
    try sub.tick(now: 0)
    assert(subVideo.isActive)
    print("  [7] Subscriber subscribed to video track")

    // 9. Publisher writes one CMAF fragment (1 keyframe at 2s).
    let frameData: [UInt8] = [0xCA, 0xFE, 0xBA, 0xBE]
    let fragment = buildCMAFFragment(
        baseDecodeTime: 180000,       // 2 seconds at 90kHz
        sampleDuration: 3000,          // 33.3ms
        sampleSize: UInt32(frameData.count),
        sampleFlags: 0x00000000,       // sync sample (keyframe)
        compositionOffset: 1000,       // ~11ms CTO
        mdatPayload: frameData)

    try pub.writeObject(
        to: videoTrack, groupID: 0, objectID: 0,
        payload: try Buffer(fragment))

    try pumpAll(from: server, to: client)
    try sub.tick(now: 0)
    print("  [8] Publisher wrote 1 CMAF fragment (\(fragment.count) bytes)")

    // 10. Client receives and parses the media object.
    let mediaObj = try sub.pollObject()!
    let trackInfo = try receivedCatalog.tracks[0].mediaTrackInfo()
    let parsed = try MediaObjectParser.parse(
        track: trackInfo, object: mediaObj)

    // 11. Validate timing and payload.
    assert(parsed.keyframe == true)
    assert(parsed.decodeTimeUS == 2_000_000)
    assert(parsed.compositionOffsetUS == 11111)
    assert(parsed.presentationTimeUS == 2_011_111)
    assert(parsed.sampleDurationUS == 33333)
    assert(parsed.sampleCount == 1)
    assert(parsed.mdatLength == 4)
    assert(parsed.samples[0].size == 4)
    print("  [9] Media object parsed:")
    print("       keyframe=\(parsed.keyframe)")
    print("       DTS=\(parsed.decodeTimeUS)us")
    print("       PTS=\(parsed.presentationTimeUS)us")
    print("       CTO=\(parsed.compositionOffsetUS)us")
    print("       duration=\(parsed.sampleDurationUS)us")
    print("       mdat=\(parsed.mdatLength) bytes, \(parsed.sampleCount) sample(s)")

    // 12. Done.
    print()
    print("PASS: Swift sans-I/O media publish/subscribe demo complete")
}

try run()
