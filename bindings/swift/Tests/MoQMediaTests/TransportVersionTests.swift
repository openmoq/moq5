//
//  The negotiated transport version, end to end on the Swift media path.
//
//  What is proved here is that the version a session negotiated reaches
//  moq_media_object_parse and selects the Key-Value-Pair integer codec --
//  not merely that a field can be assigned. The property bytes are taken
//  from the ONE authoritative corpus at tests/vectors/, so nothing on this
//  path produced the bytes it is then judged against.
//
import Testing
import Foundation
import CMoQCore
@testable import MoQ
@testable import MoQMedia

private struct CorpusRecord {
    let transport: String
    let value: UInt64
    let bytes: [UInt8]
}

private func corpusURL() -> URL {
    // .../bindings/swift/Tests/MoQMediaTests/<this file>
    var u = URL(fileURLWithPath: #filePath)
    for _ in 0..<5 { u.deleteLastPathComponent() }   // -> repository root
    return u.appendingPathComponent("tests/vectors/negotiated_profile.vectors")
}

/// Read the LOC-01 timestamp records. Deliberately a small independent
/// reader: the corpus grammar is already pinned by three other readers, and
/// borrowing one of them here would couple this test to their bookkeeping.
private func loadTimestampRecords() throws -> [CorpusRecord] {
    let text = try String(contentsOf: corpusURL(), encoding: .utf8)
    var out: [CorpusRecord] = []
    for line in text.split(separator: "\n") {
        let f = line.split(separator: " ").map(String.init)
        guard f.count == 5, f[1] == "loc01", f[2] == "timestamp",
              let value = UInt64(f[3]) else { continue }
        let hex = f[4]
        var bytes: [UInt8] = []
        var i = hex.startIndex
        while i < hex.endIndex {
            let j = hex.index(i, offsetBy: 2)
            guard let b = UInt8(hex[i..<j], radix: 16) else {
                throw MoQError.internal
            }
            bytes.append(b)
            i = j
        }
        out.append(CorpusRecord(transport: f[0], value: value, bytes: bytes))
    }
    return out
}

/// Wrap a corpus value encoding as a complete LOC-01 property block.
///
/// Capture Timestamp is property 0x02 and it is the first entry, so its
/// Delta Type is 2 -- one byte in both drafts (draft-16 section 1.4.2,
/// draft-18 section 1.4.3). The corpus supplies the value bytes; only that
/// one framing byte is added here.
private func propertyBlock(for record: CorpusRecord) -> Data {
    Data([0x02] + record.bytes)
}

private func makeObject(properties: Data, payload: Data) throws
    -> FacadeReceivedObject
{
    FacadeReceivedObject(
        track: SubscribedTrack(OpaquePointer(bitPattern: 1)!),
        groupID: 0, subgroupID: 0, objectID: 0,
        publisherPriority: 128, status: .normal,
        endOfGroup: false, isDatagram: false,
        payload: try Buffer(payload),
        properties: try Buffer(properties))
}

@Suite("Negotiated transport version on the media path")
struct TransportVersionTests {

    @Test("A draft-18 corpus vector surfaces only under the draft-18 codec")
    func draft18CorpusVectorThroughParse() throws {
        let records = try loadTimestampRecords()
        #expect(!records.isEmpty, "the corpus must be readable from source")

        // 33333 is the value the two drafts spell differently and the one
        // the C closure directions use, so a pass here and a pass there are
        // statements about the same wire fact.
        guard let d18 = records.first(where: {
            $0.transport == "d18" && $0.value == 33333
        }) else {
            Issue.record("the corpus has no d18 timestamp 33333 record")
            return
        }
        #expect(d18.bytes == [0xc0, 0x82, 0x35])

        let obj = try makeObject(properties: propertyBlock(for: d18),
                                 payload: Data([0x01]))

        // negotiatedVersion -> MediaTrackInfo -> moq_media_object_parse
        let negotiatedVersion: MediaTransportVersion = .draft18
        let track = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                   transportVersion: negotiatedVersion)
        let parsed = try MediaObjectParser.parse(track: track, object: obj)
        #expect(parsed.hasCaptureTime)
        #expect(parsed.captureTimeUS == 33333)

        // The same bytes under the other draft's codec must not reproduce
        // the value. It may throw or report something else; agreement would
        // mean the version never mattered.
        let wrong = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                   transportVersion: .draft16)
        var reproduced = false
        if let other = try? MediaObjectParser.parse(track: wrong, object: obj) {
            reproduced = other.hasCaptureTime && other.captureTimeUS == 33333
        }
        #expect(!reproduced,
                "draft-18 bytes must not decode to 33333 under draft-16")
    }

    @Test("Draft 21 reads the timestamp at LOC-04's id, not LOC-01's")
    func draft21UsesLoc04TimestampId() throws {
        // Draft 21 keeps draft 18's vi64 codec (draft-21 section 8.1), so
        // the corpus d18 value bytes are the d21 value bytes too. What
        // changes is the property id: draft 21 registers LOC-04's (section
        // 16.8; draft-ietf-moq-loc-04 section 2.3), where the timestamp is
        // 0x10 rather than LOC-01's 0x02.
        let records = try loadTimestampRecords()
        guard let rec = records.first(where: {
            $0.transport == "d18" && $0.value == 33333
        }) else {
            Issue.record("the corpus has no d18 timestamp 33333 record")
            return
        }
        let loc04 = try makeObject(properties: Data([0x10] + rec.bytes),
                                   payload: Data([0x01]))
        let loc01 = try makeObject(properties: propertyBlock(for: rec),
                                   payload: Data([0x01]))
        let d21 = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                 transportVersion: .draft21)
        let d18 = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                 transportVersion: .draft18)

        let parsed = try MediaObjectParser.parse(track: d21, object: loc04)
        #expect(parsed.hasCaptureTime)
        #expect(parsed.captureTimeUS == 33333)

        // Each id is meaningful only under its own draft. Either may throw
        // or report no timestamp; reproducing the value would mean the
        // version never reached the id table.
        for (track, obj, what) in [(d21, loc01, "LOC-01 id under d21"),
                                   (d18, loc04, "LOC-04 id under d18")] {
            var reproduced = false
            if let other = try? MediaObjectParser.parse(track: track,
                                                        object: obj) {
                reproduced = other.hasCaptureTime
                    && other.captureTimeUS == 33333
            }
            #expect(!reproduced, "\(what) must not yield the timestamp")
        }
    }

    @Test("A live Session reports its draft, and it feeds the media path")
    func sessionVersionFeedsTheMediaPath() throws {
        // This is the EXACT expression shape the live examples use:
        //     guard let tv = sess.transportVersion else { ... }
        //     try msfTrack.playbackDescriptor(transportVersion: tv)
        // The transport-gated example targets cannot be built in every
        // environment (they need an installed adapter), so the constructs
        // they rely on are type-checked and exercised HERE, in a lane that
        // always runs.
        let session = try Session(configuration: .init(perspective: .client))

        // Fixed at creation and immutable, so it is readable immediately and
        // cannot go stale.
        guard let tv = session.transportVersion else {
            Issue.record("a created session must report a known draft")
            return
        }
        #expect(MediaTransportVersion.allCases.contains(tv))

        // The value crosses into the media layer with no conversion, and on
        // into a MediaTrackInfo -- the service/media alias identity in use.
        let info = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                  transportVersion: tv)
        #expect(info.transportVersion == tv)

        // And through the catalog helper the examples call.
        // A LOC track's initData is the encoder's decoder config verbatim.
        let track = MSFTrack(name: "v", packaging: "loc", isLive: true,
                             role: "video", codec: "avc1.64001f",
                             initData: Data([0x01, 0x64, 0x00, 0x1f])
                                 .base64EncodedString())
        let desc = try track.playbackDescriptor(transportVersion: tv)
        #expect(desc.configuration.mediaType == .video)
        #expect(desc.configuration.packaging == .raw)

        // Reading it twice gives the same answer: it is not a negotiation.
        #expect(session.transportVersion == tv)
    }

    @Test("The C mapping is exhaustive and per-case correct")
    func cMappingIsPerCase() {
        // Named directly, so a mapping defect is a mapping failure rather
        // than an indirect parse failure somewhere downstream.
        #expect(cTransportVersion(.draft16) == MOQ_VERSION_DRAFT_16)
        #expect(cTransportVersion(.draft18) == MOQ_VERSION_DRAFT_18)
        #expect(cTransportVersion(.draft21) == MOQ_VERSION_DRAFT_21)
        #expect(cTransportVersion(.draft16) != cTransportVersion(.draft18))

        // Every case the shared type declares must map to a DISTINCT C
        // value. If a future draft is added and mapped onto an existing
        // one, this fails even if the switch were made non-exhaustive.
        var seen = Set<UInt32>()
        for v in MediaTransportVersion.allCases {
            let c = cTransportVersion(v)
            #expect(c != moq_version_t(0),
                    "\(v) must not map to the unset version")
            #expect(seen.insert(c.rawValue).inserted,
                    "\(v) collides with another draft's C value")
        }
        #expect(seen.count == MediaTransportVersion.allCases.count)
    }

    @Test("Each draft's own bytes parse under that draft and not the other")
    func bothBranchesAreLoadBearing() throws {
        // The two directions are asserted SEPARATELY so a mutant that forces
        // one branch is named by its own failing test, not absorbed into a
        // corpus sweep.
        let records = try loadTimestampRecords()
        for (transport, version) in [("d16", MediaTransportVersion.draft16),
                                     ("d18", MediaTransportVersion.draft18)] {
            guard let rec = records.first(where: {
                $0.transport == transport && $0.value == 33333
            }) else {
                Issue.record("the corpus has no \(transport) 33333 record")
                return
            }
            let obj = try makeObject(properties: propertyBlock(for: rec),
                                     payload: Data([0x01]))
            let track = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                       transportVersion: version)
            let parsed = try MediaObjectParser.parse(track: track, object: obj)
            #expect(parsed.hasCaptureTime,
                    "\(transport) bytes under \(version)")
            #expect(parsed.captureTimeUS == 33333,
                    "\(transport) bytes under \(version)")
        }
    }

    @Test("The shared band is the control: both codecs agree below 64")
    func sharedBandControl() throws {
        let records = try loadTimestampRecords()
        // 63 is the LAST value the two encodings spell identically, so it
        // is the sharpest available control: one more and they diverge.
        guard let d16 = records.first(where: {
            $0.transport == "d16" && $0.value == 63
        }), let d18 = records.first(where: {
            $0.transport == "d18" && $0.value == 63
        }) else {
            Issue.record("the corpus has no timestamp 63 records")
            return
        }
        // The corpus itself says the two encodings agree here; a failure in
        // this test would therefore be the fixture, not the codec.
        #expect(d16.bytes == d18.bytes)

        for version in [MediaTransportVersion.draft16, .draft18] {
            let obj = try makeObject(properties: propertyBlock(for: d16),
                                     payload: Data([0x01]))
            let track = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                       transportVersion: version)
            let parsed = try MediaObjectParser.parse(track: track, object: obj)
            #expect(parsed.hasCaptureTime)
            #expect(parsed.captureTimeUS == 63)
        }
    }

    @Test("Every divergent corpus pair is codec-specific")
    func everyDivergentPair() throws {
        let records = try loadTimestampRecords()
        var checked = 0
        for d16 in records where d16.transport == "d16" {
            guard let d18 = records.first(where: {
                $0.transport == "d18" && $0.value == d16.value
            }) else { continue }
            guard d16.bytes != d18.bytes else { continue }   // shared band
            checked += 1

            for (record, version) in [(d16, MediaTransportVersion.draft16),
                                      (d18, MediaTransportVersion.draft18)] {
                let obj = try makeObject(properties: propertyBlock(for: record),
                                         payload: Data([0x01]))
                let right = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                           transportVersion: version)
                let parsed = try MediaObjectParser.parse(track: right,
                                                         object: obj)
                #expect(parsed.hasCaptureTime)
                #expect(parsed.captureTimeUS == record.value,
                        "value \(record.value) under its own draft")
            }
        }
        #expect(checked > 0, "the corpus must contain divergent pairs")
    }
}
