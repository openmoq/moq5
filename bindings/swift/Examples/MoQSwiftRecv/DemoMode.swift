import Foundation
import MoQ
import MoQMedia
import MoQRecvArgs

enum RecvError: Error, CustomStringConvertible {
    case subscriptionFailed(String)
    var description: String {
        switch self {
        case .subscriptionFailed(let t): return "Subscription to '\(t)' did not activate"
        }
    }
}

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
        default: break
        }
    }
}

func buildAVCInit(
    timescale: UInt32, width: UInt16, height: UInt16,
    avccData: [UInt8]
) -> Data {
    var buf = Data()
    func w32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func cc(_ s: String)  { buf.append(contentsOf: Array(s.utf8)) }
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

func buildCMAFFragment(
    baseDecodeTime: UInt64, duration: UInt32, size: UInt32,
    flags: UInt32, compOffset: Int32 = 0, mdat: [UInt8]
) -> Data {
    var buf = Data()
    func w32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func w64(_ v: UInt64) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func i32(_ v: Int32)  { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func cc(_ s: String)  { buf.append(contentsOf: Array(s.utf8)) }
    func hdr(_ sz: UInt32, _ t: String) { w32(sz); cc(t) }

    let moofStart = buf.count
    hdr(0, "moof"); hdr(16, "mfhd"); w32(0); w32(1)
    let trafStart = buf.count
    hdr(0, "traf")
    hdr(16, "tfhd"); w32(0x00020000); w32(1)
    hdr(20, "tfdt"); w32(0x01000000); w64(baseDecodeTime)
    hdr(36, "trun"); w32(0x00000F01); w32(1)
    let doPos = buf.count
    w32(0); w32(duration); w32(size); w32(flags); i32(compOffset)

    let trafSz = UInt32(buf.count - trafStart)
    buf.replaceSubrange(trafStart..<trafStart+4,
        with: withUnsafeBytes(of: trafSz.bigEndian) { Data($0) })
    let moofSz = UInt32(buf.count - moofStart)
    buf.replaceSubrange(moofStart..<moofStart+4,
        with: withUnsafeBytes(of: moofSz.bigEndian) { Data($0) })
    hdr(UInt32(8 + mdat.count), "mdat"); buf.append(contentsOf: mdat)
    let dataOff = UInt32(buf.count - mdat.count - moofStart)
    buf.replaceSubrange(doPos..<doPos+4,
        with: withUnsafeBytes(of: dataOff.bigEndian) { Data($0) })
    return buf
}

func runDemo(args: RecvArgs) throws {
    let ns: Namespace = Namespace(args.namespaceParts.isEmpty
        ? [NamespacePart("moq-demo")]
        : args.namespaceParts.map { NamespacePart($0) })

    print("moq-swift-recv [demo] namespace=\(ns)")
    print()

    // Establish sessions.
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
    _ = client.pollEvents(); _ = server.pollEvents()

    // Publisher side.
    let pub = try Publisher(
        session: server, configuration: .init(acceptMode: .acceptAll))
    let catTrack = try pub.addTrack(namespace: ns, name: args.catalogTrack)
    let vidTrack = try pub.addTrack(namespace: ns, name: "video")

    let avcc: [UInt8] = [0x01, 0x64, 0x00, 0x1F, 0xFF, 0xE1]
    let initSeg = buildAVCInit(
        timescale: 90000, width: 1920, height: 1080, avccData: avcc)
    let catalog = MSFCatalog(version: 1, tracks: [
        MSFTrack(name: "video", packaging: "cmaf", isLive: true,
                 namespace: ns.parts.map(\.description).joined(separator: "/"),
                 role: "video", codec: "avc1.64001f",
                 initData: initSeg.base64EncodedString(),
                 width: 1920, height: 1080, bitrate: 2_000_000,
                 framerate: 30, timescale: 90000)
    ])
    let catJSON = try JSONEncoder().encode(catalog)
    try pub.setRetainedGroup(on: catTrack, RetainedGroupConfiguration(
        groupID: 0,
        objects: [RetainedObject(objectID: 0, payload: try Buffer(catJSON))]))

    // Subscriber side.
    let sub = try Subscriber(session: client)

    // Subscribe to catalog.
    let subCat = try sub.subscribe(
        track: TrackName(namespace: ns, name: args.catalogTrack))
    try pumpAll(from: client, to: server)
    try pub.tick(now: 0)
    try pumpAll(from: server, to: client)
    try sub.tick(now: 0)
    guard subCat.isActive else {
        throw RecvError.subscriptionFailed(args.catalogTrack)
    }

    let catObj = try sub.pollObject()!
    let recvCatalog = try JSONDecoder().decode(
        MSFCatalog.self, from: catObj.payload!.copyBytes())
    print("  CATALOG: \(recvCatalog.tracks.count) track(s)")
    for t in recvCatalog.tracks {
        print("    - \(t.name) [\(t.packaging)] \(t.role ?? "?") \(t.codec ?? "?")")
    }
    print()

    // Set up playback pipeline.
    let pipeline = try PlaybackPipeline(configuration: .init(
        maxBufferedObjects: 512,
        maxBufferedBytes: 32 * 1024 * 1024,
        maxCommands: 128,
        maxEvents: 32,
        gapTimeoutUS: args.bufferMS * 1000))

    // Subscribe to discovered tracks and add to pipeline.
    var trackMap: [String: (sub: SubscribedTrack, pb: PlaybackTrack)] = [:]
    for msfTrack in recvCatalog.tracks {
        let info = try msfTrack.mediaTrackInfo()
        var trackCfg = PlaybackTrackConfiguration(
            mediaType: info.mediaType, packaging: info.packaging,
            codec: msfTrack.codec, timescale: UInt32(info.timescale))
        if info.packaging == .cmaf, let b64 = msfTrack.initData {
            trackCfg.initData = Data(base64Encoded: b64)
        }
        trackCfg.width = msfTrack.width ?? 0
        trackCfg.height = msfTrack.height ?? 0
        trackCfg.samplerate = msfTrack.samplerate ?? 0
        trackCfg.channelCount = UInt32(msfTrack.channelConfig.flatMap(UInt32.init) ?? 0)
        trackCfg.isLive = msfTrack.isLive

        let pbTrack = try pipeline.addTrack(trackCfg)
        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ns, name: msfTrack.name))

        try pumpAll(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAll(from: server, to: client)
        try sub.tick(now: 0)
        guard subTrack.isActive else {
            throw RecvError.subscriptionFailed(msfTrack.name)
        }

        trackMap[msfTrack.name] = (sub: subTrack, pb: pbTrack)
        print("  SUBSCRIBED: \(msfTrack.name)")
    }
    print()

    // Publisher writes 3 CMAF fragments.
    let now: UInt64 = 1_000_000
    for i: UInt64 in 0..<3 {
        let dts = i * 3000
        let frameBytes: [UInt8] = [0xCA, 0xFE, UInt8(i & 0xFF), UInt8((i >> 8) & 0xFF)]
        let frag = buildCMAFFragment(
            baseDecodeTime: dts, duration: 3000,
            size: UInt32(frameBytes.count), flags: (i == 0) ? 0 : 0x00010000,
            mdat: frameBytes)
        try pub.writeObject(
            to: vidTrack, groupID: 0, objectID: i,
            payload: try Buffer(frag))
    }

    try pumpAll(from: server, to: client)
    try sub.tick(now: now)

    // Feed objects into pipeline.
    let objects = try sub.pollAllObjects()
    for obj in objects {
        if let entry = trackMap.first(where: { $0.value.sub === obj.track }) {
            try pipeline.push(object: obj, track: entry.value.pb, now: now)
        }
    }

    try pipeline.tick(now: now)

    // Print commands.
    try pipeline.drainCommands { cmd in
        switch cmd {
        case .configureVideo(_, let codec, _, let w, let h):
            let codecStr = String(data: codec.copyBytes(), encoding: .utf8) ?? "?"
            print("  CONFIGURE_VIDEO \(codecStr) \(w)x\(h)")
        case .configureAudio(_, let codec, _, let sr, let ch):
            let codecStr = String(data: codec.copyBytes(), encoding: .utf8) ?? "?"
            print("  CONFIGURE_AUDIO \(codecStr) \(sr)Hz \(ch)ch")
        case .decodeVideo(_, let g, let o, let dts, _, let pts, _, _, let kf, let p):
            print("  DECODE_VIDEO g=\(g)/\(o) dts=\(dts) pts=\(pts) keyframe=\(kf) \(p.count)B")
        case .decodeCMAF(_, let g, let o, let dts, _, let pts, _, _, let kf, _, _, let mdat):
            print("  DECODE_CMAF  g=\(g)/\(o) dts=\(dts) pts=\(pts) keyframe=\(kf) mdat=\(mdat.count)B")
        case .decodeAudio(_, let g, let o, let dts, _, _, _, _, _, let p, _):
            print("  DECODE_AUDIO g=\(g)/\(o) dts=\(dts) \(p.count)B")
        case .reset(_, let reason):
            print("  RESET \(reason)")
        case .unknown(let k):
            print("  UNKNOWN_CMD kind=\(k)")
        }
    }

    // Print events.
    try pipeline.drainEvents { evt in
        switch evt {
        case .gapDetected(_, let g):
            print("  EVENT: GAP group=\(g)")
        case .keyframeWaiting:
            print("  EVENT: KEYFRAME_WAIT")
        case .objectDropped(_, let r, let g, let o):
            print("  EVENT: DROPPED g=\(g)/\(o) reason=\(r)")
        case .skipForward(_, let from, let to):
            print("  EVENT: SKIP \(from)->\(to)")
        case .trackEnded:
            print("  EVENT: TRACK_ENDED")
        case .backlogShed(_, let dropped, _):
            print("  EVENT: BACKLOG_SHED dropped=\(dropped)")
        case .partialGroupAbandoned(_, let from, let to):
            print("  EVENT: PARTIAL_ABANDONED \(from)->\(to)")
        case .unknown(let k):
            print("  EVENT: UNKNOWN kind=\(k)")
        }
    }

    print()
    print("PASS: demo receiver complete")
}
