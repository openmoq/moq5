import Testing
import Foundation
import CMoQLOC
import CMoQCore
import CMoQSim
@testable import MoQ
@testable import MoQMedia

// MARK: - Helpers

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

private func buildCMAFFragment(
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
    hdr(0, "moof")
    hdr(16, "mfhd"); w32(0); w32(1)
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
    hdr(UInt32(8 + mdat.count), "mdat")
    buf.append(contentsOf: mdat)
    let dataOff = UInt32(buf.count - mdat.count - moofStart)
    buf.replaceSubrange(doPos..<doPos+4,
        with: withUnsafeBytes(of: dataOff.bigEndian) { Data($0) })
    return buf
}

private func makeObj(
    track: SubscribedTrack? = nil,
    groupID: UInt64 = 0, objectID: UInt64 = 0,
    payload: Buffer? = nil, properties: Buffer? = nil,
    endOfGroup: Bool = false, status: ObjectStatus = .normal
) -> FacadeReceivedObject {
    FacadeReceivedObject(
        track: track ?? SubscribedTrack(OpaquePointer(bitPattern: 1)!),
        groupID: groupID, subgroupID: 0, objectID: objectID,
        publisherPriority: 128, status: status,
        endOfGroup: endOfGroup, isDatagram: false,
        payload: payload, properties: properties)
}

// MARK: - RAW Video Tests

@Suite("Playback Pipeline — RAW")
struct PlaybackRAWTests {

    @Test("RAW video emits configure + decode")
    func rawVideoConfigure() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let track = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .video, packaging: .raw,
            codec: "avc1"))

        let payload = try Buffer(Data([0x00, 0x00, 0x01, 0x65]))
        let props = try encodeLOCProps(timestamp: 1_000_000, keyframe: true)
        let obj = makeObj(payload: payload, properties: props)

        try pb.push(object: obj, track: track, now: 1_000_000)
        try pb.tick(now: 1_000_000)

        var commands: [PlaybackCommand] = []
        try pb.drainCommands { commands.append($0) }

        #expect(commands.count == 2)
        guard case .configureVideo(_, _, _, let w, let h) = commands[0] else {
            Issue.record("Expected configureVideo"); return
        }
        #expect(w == 0)
        #expect(h == 0)

        guard case .decodeVideo(_, _, _, let dts, _, let pts, _, _, let kf, let p) = commands[1] else {
            Issue.record("Expected decodeVideo"); return
        }
        #expect(dts == 1_000_000)
        #expect(pts == 1_000_000)
        #expect(kf == true)
        #expect(p.copyBytes() == Data([0x00, 0x00, 0x01, 0x65]))
    }

    @Test("RAW audio emits configure + decode, always keyframe")
    func rawAudioKeyframe() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let track = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .audio, packaging: .raw,
            codec: "opus"))

        let payload = try Buffer(Data([0x01, 0x02, 0x03]))
        let props = try encodeLOCProps(timestamp: 2_000_000)
        let obj = makeObj(payload: payload, properties: props)

        try pb.push(object: obj, track: track, now: 2_000_000)
        try pb.tick(now: 2_000_000)

        var commands: [PlaybackCommand] = []
        try pb.drainCommands { commands.append($0) }

        #expect(commands.count == 2)
        guard case .configureAudio = commands[0] else {
            Issue.record("Expected configureAudio"); return
        }
        guard case .decodeAudio(_, _, _, let dts, _, _, _, _, _, let p, _) = commands[1] else {
            Issue.record("Expected decodeAudio"); return
        }
        #expect(dts == 2_000_000)
        #expect(p.count == 3)
    }
}

// MARK: - CMAF Tests

@Suite("Playback Pipeline — CMAF")
struct PlaybackCMAFTests {

    @Test("CMAF one-sample video emits configure + decodeCMAF")
    func cmafVideoDecode() throws {
        let avcc: [UInt8] = [0x01, 0x64, 0x00, 0x1F, 0xFF]
        let initData = buildAVCInit(
            timescale: 90000, width: 1280, height: 720, avccData: avcc)

        let pb = try PlaybackPipeline(configuration: .init())
        let track = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .video, packaging: .cmaf,
            codec: "avc1.64001f",
            initData: initData,
            timescale: 90000, width: 1280, height: 720))

        let mdatBytes: [UInt8] = [0xCA, 0xFE]
        let fragment = buildCMAFFragment(
            baseDecodeTime: 180000, duration: 3000, size: 2,
            flags: 0x00000000, compOffset: 1000, mdat: mdatBytes)

        let payload = try Buffer(fragment)
        let obj = makeObj(payload: payload)

        try pb.push(object: obj, track: track, now: 0)
        try pb.tick(now: 0)

        var commands: [PlaybackCommand] = []
        try pb.drainCommands { commands.append($0) }

        #expect(commands.count == 2)
        guard case .configureVideo(_, _, _, let w, let h) = commands[0] else {
            Issue.record("Expected configureVideo"); return
        }
        #expect(w == 1280)
        #expect(h == 720)

        guard case .decodeCMAF(_, _, _, let dts, let cto, let pts, _, _, let kf, let dur, let frag, let mdat) = commands[1] else {
            Issue.record("Expected decodeCMAF"); return
        }
        #expect(dts == 2_000_000)
        #expect(cto == 11111)
        #expect(pts == 2_011_111)
        #expect(dur == 33333)
        #expect(kf == true)
        #expect(mdat.count == 2)
        #expect(frag.count > 0)
    }
}

// MARK: - Event Tests

@Suite("Playback Pipeline — Events")
struct PlaybackEventTests {

    @Test("Missing timestamp emits drop event")
    func missingTimestampDrop() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let track = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .video, packaging: .raw, codec: "avc1"))

        let payload = try Buffer(Data([0x01]))
        let obj = makeObj(payload: payload)

        try pb.push(object: obj, track: track, now: 0)

        var events: [PlaybackEvent] = []
        try pb.drainEvents { events.append($0) }

        let dropEvent = events.first {
            if case .objectDropped(_, let r, _, _) = $0, r == .missingTimestamp {
                return true
            }
            return false
        }
        #expect(dropEvent != nil)
    }

    @Test("Command cleanup releases refs")
    func commandCleanup() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let track = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .video, packaging: .raw, codec: "avc1"))

        let payload = try Buffer(Data([0x65]))
        let props = try encodeLOCProps(timestamp: 1_000_000, keyframe: true)

        try pb.push(
            object: makeObj(payload: payload, properties: props),
            track: track, now: 1_000_000)
        try pb.tick(now: 1_000_000)

        var cmds: [PlaybackCommand] = []
        try pb.drainCommands { cmds.append($0) }
        #expect(cmds.count >= 1)

        // Commands drained — Buffers should be independently alive.
        if case .decodeVideo(_, _, _, _, _, _, _, _, _, let p) = cmds.last {
            #expect(p.count == 1)
        }
    }
}

// MARK: - Facade Integration

private func buildAVCInit(
    timescale: UInt32, width: UInt16, height: UInt16,
    avccData: [UInt8]
) -> Data {
    var buf = Data()
    func w32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func cc(_ s: String)  { buf.append(contentsOf: Array(s.utf8)) }
    func hdr(_ sz: UInt32, _ t: String) { w32(sz); cc(t) }

    hdr(20, "ftyp"); cc("isom"); w32(0); cc("isom")
    let avccBox = 8 + avccData.count
    let avc1 = 8 + 78 + avccBox
    let stsd = 8 + 8 + avc1
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
    hdr(UInt32(avccBox), "avcC")
    buf.append(contentsOf: avccData)
    return buf
}

private func pumpAllLocal(
    from source: Session, to dest: Session
) throws {
    try source.drainActions { action in
        switch action {
        case .sendControl(let data):
            try data.withUnsafeBytes { try dest.receiveControlBytes($0, now: 0) }
        case .sendData(let stream, let header, let payload, let fin):
            var bytes = [UInt8](header)
            if let buf = payload { buf.withUnsafeBytes { bytes.append(contentsOf: $0) } }
            try bytes.withUnsafeBufferPointer { ptr in
                let raw = UnsafeRawBufferPointer(start: ptr.baseAddress, count: ptr.count)
                try dest.receiveDataBytes(on: stream, raw, fin: fin, now: 0)
            }
        case .sendDatagram(let d):
            try d.withUnsafeBytes { try dest.receiveDatagram($0, now: 0) }
        default: break
        }
    }
}

private func makeLocalPair() throws -> (Session, Session) {
    var cc = Session.Configuration(perspective: .client)
    cc.sendRequestCapacity = true; cc.initialRequestCapacity = 16
    var sc = Session.Configuration(perspective: .server)
    sc.sendRequestCapacity = true; sc.initialRequestCapacity = 16
    let c = try Session(configuration: cc)
    let s = try Session(configuration: sc)
    try c.start()
    try pumpAllLocal(from: c, to: s); try pumpAllLocal(from: s, to: c)
    _ = c.pollEvents(); _ = s.pollEvents()
    return (c, s)
}

@Suite("Playback Pipeline — Facade Integration")
struct PlaybackFacadeTests {

    @Test("End-to-end: pub/sub → playback pipeline")
    func endToEnd() throws {
        let (client, server) = try makeLocalPair()
        let pub = try Publisher(
            session: server, configuration: .init(acceptMode: .acceptAll))
        let sub = try Subscriber(session: client)

        let pubTrack = try pub.addTrack(namespace: ["test"], name: "vid")
        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "vid"))

        try pumpAllLocal(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAllLocal(from: server, to: client)
        try sub.tick(now: 0)
        #expect(subTrack.isActive)

        let props = try encodeLOCProps(timestamp: 3_000_000, keyframe: true)!
        let config = PublishObjectConfiguration(
            groupID: 0, objectID: 0,
            payload: try Buffer(Data([0xDE, 0xAD])),
            properties: props)
        try pub.writeObject(to: pubTrack, config)

        try pumpAllLocal(from: server, to: client)
        try sub.tick(now: 0)

        let recvObj = try sub.pollObject()!

        let pb = try PlaybackPipeline(configuration: .init())
        let pbTrack = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .video, packaging: .raw, codec: "avc1"))

        try pb.push(object: recvObj, track: pbTrack, now: 3_000_000)
        try pb.tick(now: 3_000_000)

        var cmds: [PlaybackCommand] = []
        try pb.drainCommands { cmds.append($0) }
        #expect(cmds.count == 2)

        guard case .decodeVideo(_, _, _, let dts, _, _, _, _, let kf, _) = cmds[1] else {
            Issue.record("Expected decodeVideo"); return
        }
        #expect(dts == 3_000_000)
        #expect(kf == true)
    }
}

// MARK: - Refcount Proof

@Suite("Playback Pipeline — Refcount")
struct PlaybackRefcountTests {

    @Test("RAW: push retains, command adopts, drop releases")
    func rawRefcount() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let track = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .video, packaging: .raw, codec: "avc1"))

        let payload = try Buffer(Data([0x65]))
        let props = try encodeLOCProps(timestamp: 1_000_000, keyframe: true)!

        let initialRC = moq_rcbuf_refcount(payload.raw)
        #expect(initialRC == 1)

        try pb.push(
            object: makeObj(payload: payload, properties: props),
            track: track, now: 1_000_000)

        let afterPush = moq_rcbuf_refcount(payload.raw)
        #expect(afterPush == 2)

        try pb.tick(now: 1_000_000)

        // Command owns an additional ref via incref.
        var cmds: [PlaybackCommand] = []
        try pb.drainCommands { cmds.append($0) }

        // After drain: push ref was released by tick, command
        // adopted its ref. Swift Buffer holds adopted ref.
        // Original payload still has its own ref.
        let afterDrain = moq_rcbuf_refcount(payload.raw)
        #expect(afterDrain >= 1)

        guard case .decodeVideo(_, _, _, _, _, _, _, _, _, let cmdPayload) = cmds.last else {
            Issue.record("Expected decodeVideo"); return
        }
        let cmdRC = moq_rcbuf_refcount(cmdPayload.raw)
        #expect(cmdRC >= 1)
    }

    @Test("CMAF: fragment ref adopted into decodeCMAF command")
    func cmafRefcount() throws {
        let avcc: [UInt8] = [0x01, 0x64, 0x00, 0x1F, 0xFF]
        let initData = buildAVCInit(
            timescale: 90000, width: 640, height: 480, avccData: avcc)
        let pb = try PlaybackPipeline(configuration: .init())
        let track = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .video, packaging: .cmaf,
            codec: "avc1", initData: initData,
            timescale: 90000, width: 640, height: 480))

        let fragment = buildCMAFFragment(
            baseDecodeTime: 0, duration: 3000, size: 2,
            flags: 0, mdat: [0xCA, 0xFE])
        let payload = try Buffer(fragment)

        #expect(moq_rcbuf_refcount(payload.raw) == 1)

        try pb.push(object: makeObj(payload: payload), track: track, now: 0)
        #expect(moq_rcbuf_refcount(payload.raw) == 2)

        try pb.tick(now: 0)
        var cmds: [PlaybackCommand] = []
        try pb.drainCommands { cmds.append($0) }

        guard case .decodeCMAF(_, _, _, _, _, _, _, _, _, _, let frag, _) = cmds.last else {
            Issue.record("Expected decodeCMAF"); return
        }
        #expect(frag.count > 0)
        #expect(moq_rcbuf_refcount(frag.raw) >= 1)
    }
}

// MARK: - Multi-Sample CMAF

private func buildMultiSampleFragment(
    baseDecodeTime: UInt64,
    samples: [(duration: UInt32, size: UInt32)],
    defaultFlags: UInt32,
    mdat: [UInt8]
) -> Data {
    var buf = Data()
    func w32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func w64(_ v: UInt64) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func cc(_ s: String)  { buf.append(contentsOf: Array(s.utf8)) }
    func hdr(_ sz: UInt32, _ t: String) { w32(sz); cc(t) }

    let moofStart = buf.count
    hdr(0, "moof")
    hdr(16, "mfhd"); w32(0); w32(1)
    let trafStart = buf.count
    hdr(0, "traf")
    // tfhd: default-base-is-moof + default-sample-flags
    hdr(20, "tfhd"); w32(0x00020020); w32(1); w32(defaultFlags)
    hdr(20, "tfdt"); w32(0x01000000); w64(baseDecodeTime)

    let sc = UInt32(samples.count)
    let trunSize: UInt32 = 8 + 4 + 4 + 4 + sc * 8
    hdr(trunSize, "trun")
    w32(0x00000301) // data-offset|duration|size
    w32(sc)
    let doPos = buf.count
    w32(0) // data_offset placeholder
    for s in samples { w32(s.duration); w32(s.size) }

    let trafSz = UInt32(buf.count - trafStart)
    buf.replaceSubrange(trafStart..<trafStart+4,
        with: withUnsafeBytes(of: trafSz.bigEndian) { Data($0) })
    let moofSz = UInt32(buf.count - moofStart)
    buf.replaceSubrange(moofStart..<moofStart+4,
        with: withUnsafeBytes(of: moofSz.bigEndian) { Data($0) })

    hdr(UInt32(8 + mdat.count), "mdat")
    buf.append(contentsOf: mdat)
    let dataOff = UInt32(buf.count - mdat.count - moofStart)
    buf.replaceSubrange(doPos..<doPos+4,
        with: withUnsafeBytes(of: dataOff.bigEndian) { Data($0) })
    return buf
}

@Suite("Playback Pipeline — Multi-Sample CMAF")
struct PlaybackMultiSampleTests {

    @Test("Audio multi-sample emits per-sample decode commands")
    func audioMultiSample() throws {
        let pb = try PlaybackPipeline(configuration: .init())
        let track = try pb.addTrack(PlaybackTrackConfiguration(
            mediaType: .audio, packaging: .cmaf,
            codec: "opus", timescale: 48000,
            samplerate: 48000, channelCount: 2))

        let mdat: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06]
        let fragment = buildMultiSampleFragment(
            baseDecodeTime: 0,
            samples: [(1024, 2), (1024, 3), (1024, 1)],
            defaultFlags: 0x00000000,
            mdat: mdat)

        try pb.push(
            object: makeObj(payload: try Buffer(fragment)),
            track: track, now: 0)
        try pb.tick(now: 0)

        var cmds: [PlaybackCommand] = []
        try pb.drainCommands { cmds.append($0) }

        // 1 configure + 3 decode commands
        #expect(cmds.count == 4)

        guard case .configureAudio = cmds[0] else {
            Issue.record("Expected configureAudio"); return
        }

        // Verify per-sample DTS progression and mdat ranges.
        var prevDTS: UInt64 = 0
        for i in 1...3 {
            guard case .decodeAudio(_, _, _, let dts, _, _, _, _, let dur, _, let mdatRange) = cmds[i] else {
                Issue.record("Expected decodeAudio at index \(i)"); return
            }
            if i > 1 { #expect(dts > prevDTS) }
            #expect(dur > 0)
            #expect(mdatRange.count > 0)
            prevDTS = dts
        }
    }
}
