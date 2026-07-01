import Testing
import Foundation
import CMoQLOC
import CMoQCore
@testable import MoQ
@testable import MoQMedia

// MARK: - LOC Property Helpers

private func encodeLOCProperties(
    timestamp: UInt64? = nil,
    keyframe: Bool = false
) throws -> Buffer? {
    var headers = moq_loc_headers_t()
    moq_loc_headers_init(&headers)
    if let ts = timestamp {
        headers.has_timestamp = true
        headers.timestamp = ts
    }
    if keyframe {
        headers.has_video_frame_marking = true
        headers.video_frame_marking.independent = true
        headers.video_frame_marking.start_of_frame = true
        headers.video_frame_marking.end_of_frame = true
    }
    var props: OpaquePointer?
    let alloc = moq_alloc_default()!
    let rc = moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &headers, &props)
    guard rc == MOQ_OK else { throw MoQError.internal }
    guard let p = props else { return nil }
    return Buffer(adopting: p)
}

// MARK: - CMAF Fragment Builder

private func buildCMAFFragment(
    baseDecodeTime: UInt64,
    sampleDuration: UInt32,
    sampleSize: UInt32,
    sampleFlags: UInt32,
    compositionOffset: Int32 = 0,
    mdatPayload: [UInt8]
) -> Data {
    var buf = Data()

    func writeU32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func writeU64(_ v: UInt64) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func writeI32(_ v: Int32)  { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func fourCC(_ s: String)   { buf.append(contentsOf: Array(s.utf8)) }

    // moof box
    let moofStart = buf.count

    // moof header placeholder
    writeU32(0); fourCC("moof")

    // mfhd box (16 bytes)
    writeU32(16); fourCC("mfhd")
    writeU32(0) // version+flags
    writeU32(1) // sequence_number

    // traf box
    let trafStart = buf.count
    writeU32(0); fourCC("traf")

    // tfhd box (16 bytes): flags=0x020000 (default-base-is-moof)
    writeU32(16); fourCC("tfhd")
    writeU32(0x00020000) // version=0, flags=default-base-is-moof
    writeU32(1) // track_id

    // tfdt box (20 bytes): version=1 for 64-bit base_decode_time
    writeU32(20); fourCC("tfdt")
    writeU32(0x01000000) // version=1, flags=0
    writeU64(baseDecodeTime)

    // trun box: version=0, flags=0x000F01 (duration, size, flags, composition_offset, data_offset)
    let sampleCount: UInt32 = 1
    let trunSize: UInt32 = 12 + 4 + 4 + (sampleCount * 16) // header + data_offset + sample_count + samples
    writeU32(trunSize); fourCC("trun")
    writeU32(0x00000F01) // version=0, flags=data-offset|duration|size|flags|composition-offset
    writeU32(sampleCount)
    // data_offset: will be patched after moof size is known
    let dataOffsetPos = buf.count
    writeU32(0) // placeholder
    // sample entry
    writeU32(sampleDuration)
    writeU32(sampleSize)
    writeU32(sampleFlags)
    writeI32(compositionOffset)

    // Patch traf size
    let trafSize = UInt32(buf.count - trafStart)
    buf.replaceSubrange(trafStart..<trafStart+4,
                        with: withUnsafeBytes(of: trafSize.bigEndian) { Data($0) })

    // Patch moof size
    let moofSize = UInt32(buf.count - moofStart)
    buf.replaceSubrange(moofStart..<moofStart+4,
                        with: withUnsafeBytes(of: moofSize.bigEndian) { Data($0) })

    // mdat box
    let mdatSize = UInt32(8 + UInt32(mdatPayload.count))
    writeU32(mdatSize); fourCC("mdat")
    buf.append(contentsOf: mdatPayload)

    // Patch data_offset: offset from moof start to mdat data
    let dataOffset = UInt32(buf.count - mdatPayload.count - moofStart)
    buf.replaceSubrange(dataOffsetPos..<dataOffsetPos+4,
                        with: withUnsafeBytes(of: dataOffset.bigEndian) { Data($0) })

    return buf
}

private func buildMultiSampleCMAFFragment(
    baseDecodeTime: UInt64,
    samples: [(duration: UInt32, size: UInt32)],
    defaultFlags: UInt32,
    mdatPayload: [UInt8]
) -> Data {
    var buf = Data()

    func writeU32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func writeU64(_ v: UInt64) { withUnsafeBytes(of: v.bigEndian) { buf.append(contentsOf: $0) } }
    func fourCC(_ s: String)   { buf.append(contentsOf: Array(s.utf8)) }

    let moofStart = buf.count
    writeU32(0); fourCC("moof")

    // mfhd
    writeU32(16); fourCC("mfhd")
    writeU32(0); writeU32(1)

    // traf
    let trafStart = buf.count
    writeU32(0); fourCC("traf")

    // tfhd with default_sample_flags (20 = 8 header + 4 ver/flags + 4 track_id + 4 default_flags)
    writeU32(20); fourCC("tfhd")
    writeU32(0x00020020) // flags=default-base-is-moof|default-sample-flags
    writeU32(1) // track_id
    writeU32(defaultFlags)

    // tfdt
    writeU32(20); fourCC("tfdt")
    writeU32(0x01000000)
    writeU64(baseDecodeTime)

    // trun: flags=0x000301 (duration|size|data-offset)
    let sampleCount = UInt32(samples.count)
    let trunSize: UInt32 = 12 + 4 + 4 + (sampleCount * 8)
    writeU32(trunSize); fourCC("trun")
    writeU32(0x00000301) // duration|size|data-offset
    writeU32(sampleCount)
    let dataOffsetPos = buf.count
    writeU32(0)
    for s in samples {
        writeU32(s.duration)
        writeU32(s.size)
    }

    let trafSize = UInt32(buf.count - trafStart)
    buf.replaceSubrange(trafStart..<trafStart+4,
                        with: withUnsafeBytes(of: trafSize.bigEndian) { Data($0) })
    let moofSize = UInt32(buf.count - moofStart)
    buf.replaceSubrange(moofStart..<moofStart+4,
                        with: withUnsafeBytes(of: moofSize.bigEndian) { Data($0) })

    let mdatSize = UInt32(8 + UInt32(mdatPayload.count))
    writeU32(mdatSize); fourCC("mdat")
    buf.append(contentsOf: mdatPayload)

    let dataOffset = UInt32(buf.count - mdatPayload.count - moofStart)
    buf.replaceSubrange(dataOffsetPos..<dataOffsetPos+4,
                        with: withUnsafeBytes(of: dataOffset.bigEndian) { Data($0) })

    return buf
}

// MARK: - RAW Tests

@Suite("Media Object Parser — RAW")
struct MediaObjectParserRAWTests {

    @Test("RAW video with LOC timestamp and keyframe")
    func rawVideoKeyframe() throws {
        let payload = try Buffer(Data([0x00, 0x00, 0x01, 0x65]))
        let props = try encodeLOCProperties(timestamp: 1_000_000, keyframe: true)

        let obj = FacadeReceivedObject(
            track: SubscribedTrack(OpaquePointer(bitPattern: 1)!),
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: payload, properties: props)

        let track = MediaTrackInfo(mediaType: .video, packaging: .raw)
        let parsed = try MediaObjectParser.parse(track: track, object: obj)

        #expect(parsed.hasCaptureTime)
        #expect(parsed.captureTimeUS == 1_000_000)
        #expect(parsed.decodeTimeUS == 1_000_000)
        #expect(parsed.compositionOffsetUS == 0)
        #expect(parsed.presentationTimeUS == 1_000_000)
        #expect(parsed.keyframe == true)
    }

    @Test("RAW audio always keyframe")
    func rawAudioKeyframe() throws {
        let payload = try Buffer(Data([0x01, 0x02, 0x03]))
        let props = try encodeLOCProperties(timestamp: 2_000_000)

        let obj = FacadeReceivedObject(
            track: SubscribedTrack(OpaquePointer(bitPattern: 1)!),
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: payload, properties: props)

        let track = MediaTrackInfo(mediaType: .audio, packaging: .raw)
        let parsed = try MediaObjectParser.parse(track: track, object: obj)

        #expect(parsed.keyframe == true)
        #expect(parsed.captureTimeUS == 2_000_000)
    }

    @Test("RAW missing LOC timestamp parses without capture timestamp")
    func rawMissingTimestamp() throws {
        let payload = try Buffer(Data([0x01]))
        // Empty properties — no LOC timestamp
        let props = try encodeLOCProperties()

        let obj = FacadeReceivedObject(
            track: SubscribedTrack(OpaquePointer(bitPattern: 1)!),
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: payload, properties: props)

        let track = MediaTrackInfo(mediaType: .video, packaging: .raw)

        // Matches the C object layer: a timestamp-less RAW/LOC object parses
        // successfully and is surfaced with hasCaptureTime == false /
        // captureTimeUS == 0. Strictness lives at the playback layer, which
        // drops such objects (see PlaybackPipelineTests).
        let parsed = try MediaObjectParser.parse(track: track, object: obj)
        #expect(parsed.hasCaptureTime == false)
        #expect(parsed.captureTimeUS == 0)
    }
}

// MARK: - CMAF Tests

@Suite("Media Object Parser — CMAF")
struct MediaObjectParserCMAFTests {

    @Test("CMAF one-sample video parses mdat and timing")
    func cmafVideoOneSample() throws {
        let mdatBytes: [UInt8] = [0xCA, 0xFE]
        let fragment = buildCMAFFragment(
            baseDecodeTime: 180000,
            sampleDuration: 3000,
            sampleSize: 2,
            sampleFlags: 0x00000000,
            compositionOffset: 1000,
            mdatPayload: mdatBytes)

        let payload = try Buffer(fragment)

        let obj = FacadeReceivedObject(
            track: SubscribedTrack(OpaquePointer(bitPattern: 1)!),
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: payload, properties: nil)

        let track = MediaTrackInfo(
            mediaType: .video, packaging: .cmaf, timescale: 90000)
        let parsed = try MediaObjectParser.parse(track: track, object: obj)

        #expect(parsed.decodeTimeUS == 2_000_000)
        #expect(parsed.compositionOffsetUS == 11111)
        #expect(parsed.presentationTimeUS == 2_011_111)
        #expect(parsed.sampleDurationUS == 33333)
        #expect(parsed.keyframe == true)
        #expect(parsed.sampleCount == 1)
        #expect(parsed.mdatLength == 2)
        #expect(parsed.samples.count == 1)
        #expect(parsed.samples[0].size == 2)
    }

    @Test("CMAF audio multi-sample parses sampleCount > 1")
    func cmafAudioMultiSample() throws {
        let mdatBytes: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06]
        let fragment = buildMultiSampleCMAFFragment(
            baseDecodeTime: 48000,
            samples: [(1024, 2), (1024, 3), (1024, 1)],
            defaultFlags: 0x00000000,
            mdatPayload: mdatBytes)

        let payload = try Buffer(fragment)

        let obj = FacadeReceivedObject(
            track: SubscribedTrack(OpaquePointer(bitPattern: 1)!),
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: payload, properties: nil)

        let track = MediaTrackInfo(
            mediaType: .audio, packaging: .cmaf, timescale: 48000)
        let parsed = try MediaObjectParser.parse(track: track, object: obj)

        #expect(parsed.sampleCount == 3)
        #expect(parsed.samples.count == 3)
        #expect(parsed.samples[0].size == 2)
        #expect(parsed.samples[1].size == 3)
        #expect(parsed.samples[2].size == 1)
        #expect(parsed.keyframe == true)
        #expect(parsed.mdatLength == 6)
    }

    @Test("CMAF scratch too small surfaces required sample count")
    func cmafScratchTooSmall() throws {
        let mdatBytes: [UInt8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06]
        let fragment = buildMultiSampleCMAFFragment(
            baseDecodeTime: 48000,
            samples: [(1024, 2), (1024, 3), (1024, 1)],
            defaultFlags: 0x00000000,
            mdatPayload: mdatBytes)

        let payload = try Buffer(fragment)

        let obj = FacadeReceivedObject(
            track: SubscribedTrack(OpaquePointer(bitPattern: 1)!),
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: payload, properties: nil)

        let track = MediaTrackInfo(
            mediaType: .audio, packaging: .cmaf, timescale: 48000)

        do {
            _ = try MediaObjectParser.parse(
                track: track, object: obj, maxSamples: 1)
            Issue.record("Expected bufferTooSmall")
        } catch let err as MediaParseError {
            guard case .bufferTooSmall(let required) = err else {
                Issue.record("Expected bufferTooSmall, got \(err)")
                return
            }
            #expect(required == 3)
        }
    }
}

// MARK: - Timescale & Input Validation

@Suite("Media Object Parser — Validation")
struct MediaObjectParserValidationTests {

    @Test("CMAF parse with timescale > UInt32.max throws")
    func cmafTimescaleOverflow() throws {
        let payload = try Buffer(Data([0x01]))
        let obj = FacadeReceivedObject(
            track: SubscribedTrack(OpaquePointer(bitPattern: 1)!),
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: payload, properties: nil)

        let big: UInt64 = UInt64(UInt32.max) + 1
        let track = MediaTrackInfo(
            mediaType: .audio, packaging: .cmaf, timescale: big)

        #expect(throws: MediaParseError.invalidArgument) {
            _ = try MediaObjectParser.parse(track: track, object: obj)
        }
    }

    @Test("RAW parse with large timescale succeeds")
    func rawLargeTimescaleOK() throws {
        let payload = try Buffer(Data([0x01, 0x02]))
        let props = try encodeLOCProperties(timestamp: 1_000_000)

        let obj = FacadeReceivedObject(
            track: SubscribedTrack(OpaquePointer(bitPattern: 1)!),
            groupID: 0, subgroupID: 0, objectID: 0,
            publisherPriority: 128, status: .normal,
            endOfGroup: false, isDatagram: false,
            payload: payload, properties: props)

        let big: UInt64 = UInt64(UInt32.max) + 1
        let track = MediaTrackInfo(
            mediaType: .audio, packaging: .raw, timescale: big)
        let parsed = try MediaObjectParser.parse(track: track, object: obj)
        #expect(parsed.captureTimeUS == 1_000_000)
    }
}

// MARK: - MSFTrack Convenience

@Suite("MSFTrack.mediaTrackInfo")
struct MSFTrackInfoTests {

    @Test("Video LOC track info")
    func videoLOC() throws {
        let track = MSFTrack(
            name: "video", packaging: "loc", isLive: true, role: "video")
        let info = try track.mediaTrackInfo()
        #expect(info.mediaType == .video)
        #expect(info.packaging == .raw)
    }

    @Test("Audio CMAF track info with timescale")
    func audioCMAF() throws {
        let track = MSFTrack(
            name: "audio", packaging: "cmaf", isLive: true,
            role: "audio", timescale: 48000)
        let info = try track.mediaTrackInfo()
        #expect(info.mediaType == .audio)
        #expect(info.packaging == .cmaf)
        #expect(info.timescale == 48000)
    }

    @Test("Timescale > UInt32.max is preserved")
    func largeTimescale() throws {
        let big: UInt64 = UInt64(UInt32.max) + 1
        let info = MediaTrackInfo(
            mediaType: .audio, packaging: .cmaf, timescale: big)
        #expect(info.timescale == big)
    }

    @Test("Missing role throws missingRole")
    func missingRole() throws {
        let track = MSFTrack(
            name: "t", packaging: "loc", isLive: true)
        #expect(throws: MediaTrackInfoError.missingRole) {
            _ = try track.mediaTrackInfo()
        }
    }

    @Test("Unknown role throws unsupportedRole")
    func unknownRole() throws {
        let track = MSFTrack(
            name: "t", packaging: "loc", isLive: true, role: "subtitle")
        #expect(throws: MediaTrackInfoError.unsupportedRole("subtitle")) {
            _ = try track.mediaTrackInfo()
        }
    }

    @Test("Unknown packaging throws unsupportedPackaging")
    func unknownPackaging() throws {
        let track = MSFTrack(
            name: "t", packaging: "mp2t", isLive: true, role: "video")
        #expect(throws: MediaTrackInfoError.unsupportedPackaging("mp2t")) {
            _ = try track.mediaTrackInfo()
        }
    }
}

// MARK: - End-to-end with real facade objects

import CMoQSim

private func pumpAllMedia(
    from source: Session, to dest: Session, now: UInt64 = 0
) throws {
    try source.drainActions { action in
        switch action {
        case .sendControl(let data):
            try data.withUnsafeBytes { try dest.receiveControlBytes($0, now: now) }
        case .sendData(let stream, let header, let payload, let fin):
            var bytes = [UInt8](header)
            if let buf = payload {
                buf.withUnsafeBytes { bytes.append(contentsOf: $0) }
            }
            try bytes.withUnsafeBufferPointer { ptr in
                let raw = UnsafeRawBufferPointer(start: ptr.baseAddress, count: ptr.count)
                try dest.receiveDataBytes(on: stream, raw, fin: fin, now: now)
            }
        case .sendDatagram(let data):
            try data.withUnsafeBytes { try dest.receiveDatagram($0, now: now) }
        default: break
        }
    }
}

private func makeMediaEstablishedPair() throws -> (client: Session, server: Session) {
    var clientCfg = Session.Configuration(perspective: .client)
    clientCfg.sendRequestCapacity = true
    clientCfg.initialRequestCapacity = 16
    var serverCfg = Session.Configuration(perspective: .server)
    serverCfg.sendRequestCapacity = true
    serverCfg.initialRequestCapacity = 16

    let client = try Session(configuration: clientCfg)
    let server = try Session(configuration: serverCfg)
    try client.start()
    try pumpAllMedia(from: client, to: server)
    try pumpAllMedia(from: server, to: client)
    _ = client.pollEvents()
    _ = server.pollEvents()
    return (client, server)
}

@Suite("Media Object Parser — Facade Integration")
struct MediaObjectParserFacadeTests {

    @Test("Parse real FacadeReceivedObject from pub/sub pipeline")
    func parseRealFacadeObject() throws {
        let (client, server) = try makeMediaEstablishedPair()

        let pub = try Publisher(
            session: server, configuration: .init(acceptMode: .acceptAll))
        let sub = try Subscriber(session: client)

        let pubTrack = try pub.addTrack(namespace: ["test"], name: "parse")
        let subTrack = try sub.subscribe(
            track: TrackName(namespace: ["test"], name: "parse"))

        try pumpAllMedia(from: client, to: server)
        try pub.tick(now: 0)
        try pumpAllMedia(from: server, to: client)
        try sub.tick(now: 0)
        #expect(subTrack.isActive)

        let props = try encodeLOCProperties(timestamp: 5_000_000, keyframe: true)
        let config = PublishObjectConfiguration(
            groupID: 0, objectID: 0,
            payload: try Buffer(Data([0x00, 0x00, 0x01, 0x65])),
            properties: props)
        try pub.writeObject(to: pubTrack, config)

        try pumpAllMedia(from: server, to: client)
        try sub.tick(now: 0)

        let obj = try sub.pollObject()!
        let trackInfo = MediaTrackInfo(mediaType: .video, packaging: .raw)
        let parsed = try MediaObjectParser.parse(
            track: trackInfo, object: obj)

        #expect(parsed.keyframe == true)
        #expect(parsed.captureTimeUS == 5_000_000)
        #expect(parsed.presentationTimeUS == 5_000_000)
    }
}
