import Testing
import Foundation
@testable import MoQCatalog

/// The MSF-02 example catalogs (draft-ietf-moq-msf-02 5.6), shared with the C
/// library's tests.
private func msf02FixtureDirectory() -> URL {
    // .../bindings/swift/Tests/MoQCatalogTests/<this file>
    var u = URL(fileURLWithPath: #filePath)
    for _ in 0..<5 { u.deleteLastPathComponent() }   // -> repository root
    return u.appendingPathComponent("media/msf/tests/fixtures/msf02")
}

private func msf02Fixture(_ name: String) throws -> MSFCatalog {
    let data = try Data(contentsOf: msf02FixtureDirectory().appendingPathComponent(name))
    return try JSONDecoder().decode(MSFCatalog.self, from: data)
}

private func decode(_ json: String) throws -> MSFCatalog {
    try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
}

private func encodeObject(_ catalog: MSFCatalog) throws -> [String: Any] {
    let data = try JSONEncoder().encode(catalog)
    return try #require(JSONSerialization.jsonObject(with: data) as? [String: Any])
}

@Suite("MSF-02 example catalogs (5.6)")
struct MSF02FixtureTests {

    @Test("Every example decodes, and survives an encode and a second decode")
    func everyExampleRoundTrips() throws {
        let names = try FileManager.default
            .contentsOfDirectory(atPath: msf02FixtureDirectory().path)
            .filter { $0.hasSuffix(".json") }
            .sorted()
        #expect(names.count == 19)
        for name in names {
            let catalog = try msf02Fixture(name)
            let again = try JSONDecoder().decode(
                MSFCatalog.self, from: JSONEncoder().encode(catalog))
            #expect(again == catalog, "\(name)")
        }
    }

    @Test("An independent example is draft-02; a delta follows its vocabulary")
    func revisions() throws {
        #expect(try msf02Fixture("5_6_1_av_single.json").revision == .msf02)
        #expect(try msf02Fixture("5_6_13_terminate.json").revision == .msf02)
        // An "update" op is MSF-02 vocabulary; a remove op is not.
        #expect(try msf02Fixture("5_6_4_delta_update_add.json").revision == .msf02)
        #expect(try msf02Fixture("5_6_5_delta_remove.json").revision == .msf01)
    }

    @Test("The update op names its target by parentName and carries no name")
    func updateOp() throws {
        let catalog = try msf02Fixture("5_6_4_delta_update_add.json")
        let ops = try #require(catalog.deltaUpdate)
        #expect(ops.map(\.op) == [.update, .add, .clone])
        let update = try #require(ops.first?.tracks.first)
        #expect(update.parentName == "video-1080")
        #expect(update.name.isEmpty)
        #expect(update.bitrate == 4_000_000)
        #expect(ops[2].tracks[0].parentNamespace == "example.com/custom")
    }

    @Test("Encryption fields, keyed by their wire names")
    func encryption() throws {
        let track = try msf02Fixture("5_6_8_encrypted.json").tracks[0]
        #expect(track.encryptionScheme == "moq-secure-objects")
        #expect(track.cipherSuite == "aes-128-gcm-sha256")
        #expect(track.keyId == "key-2024-q1-premium")
        #expect(track.trackBaseKey?.value == "MTIzNDU2Nzg5MDEyMzQ1Ng==")
    }

    @Test("A Number depends entry is a renderGroup; accessibleWindowTime is read")
    func timelines() throws {
        let tracks = try msf02Fixture("5_6_9_timelines.json").tracks
        #expect(tracks[0].dependsRenderGroups == [1])
        #expect(tracks[0].depends == nil)
        #expect(tracks[1].depends == ["history"])
        #expect(tracks[1].dependsRenderGroups == nil)
        #expect(tracks[2].accessibleWindowTime == 300_000)
    }

    @Test("Publish tracks are their own list")
    func publishTracks() throws {
        let catalog = try msf02Fixture("5_6_17_publish_tracks.json")
        #expect(catalog.tracks.map(\.name) == ["video", "audio"])
        #expect(catalog.publishTracks?.map(\.name) == ["4", "6"])
        #expect(catalog.publishTracks?.map(\.packaging) == ["moqmetrics", "moqlog"])
        #expect(try msf02Fixture("5_6_1_av_single.json").publishTracks == nil)
    }
}

@Suite("MSF-02 authoring")
struct MSF02AuthoringTests {

    @Test("draft-02 encodes its version, the supplement fields and one depends array")
    func encodeDraft02() throws {
        var video = MSFTrack(name: "video", packaging: "loc", isLive: true, renderGroup: 1)
        video.packagingMetadata = "streamMapping=object"
        video.keyframeSpacing = 30
        video.accessibleWindowTime = 60_000
        video.encryptionScheme = "moq-secure-objects"
        video.cipherSuite = "aes-128-gcm-sha256"
        video.keyId = "kid"
        video.trackBaseKey = MSFTrackBaseKey("MTIz")
        var captions = MSFTrack(name: "captions", packaging: "loc", isLive: true,
                                depends: ["video"])
        captions.dependsRenderGroups = [1]
        let catalog = MSFCatalog(
            tracks: [video, captions], revision: .msf02,
            publishTracks: [MSFTrack(name: "log", packaging: "moqlog", isLive: true)])

        let object = try encodeObject(catalog)
        #expect(object["version"] as? String == "draft-02")
        let tracks = try #require(object["tracks"] as? [[String: Any]])
        #expect(tracks[0]["packagingMetadata"] as? String == "streamMapping=object")
        #expect(tracks[0]["keyframeSpacing"] as? Int == 30)
        #expect(tracks[0]["accessibleWindowTime"] as? Int == 60_000)
        #expect(tracks[0]["trackBaseKey"] as? String == "MTIz")
        let depends = try #require(tracks[1]["depends"] as? [Any])
        #expect(depends.count == 2)
        #expect(depends[0] as? String == "video")
        #expect(depends[1] as? Int == 1)
        #expect((object["publishTracks"] as? [[String: Any]])?.count == 1)

        let back = try JSONDecoder().decode(MSFCatalog.self, from: JSONEncoder().encode(catalog))
        #expect(back == catalog)
    }

    @Test("draft-01 refuses the MSF-02-only fields but carries encryption")
    func encodeDraft01() throws {
        var encrypted = MSFTrack(name: "v", packaging: "loc", isLive: true)
        encrypted.encryptionScheme = "moq-secure-objects"
        encrypted.keyId = "k"
        let ok = try encodeObject(MSFCatalog(tracks: [encrypted]))
        #expect(ok["version"] as? String == "draft-01")
        #expect((ok["tracks"] as? [[String: Any]])?[0]["keyId"] as? String == "k")

        var spaced = MSFTrack(name: "v", packaging: "loc", isLive: true)
        spaced.keyframeSpacing = 15
        var grouped = MSFTrack(name: "v", packaging: "loc", isLive: true, renderGroup: 1)
        grouped.dependsRenderGroups = [1]
        var mapped = MSFTrack(name: "v", packaging: "loc", isLive: true)
        mapped.packagingMetadata = "streamMapping=group"
        var windowed = MSFTrack(name: "v", packaging: "loc", isLive: true)
        windowed.accessibleWindowTime = 1
        for track in [spaced, grouped, mapped, windowed] {
            #expect(throws: EncodingError.self) {
                _ = try JSONEncoder().encode(MSFCatalog(tracks: [track]))
            }
        }
        let update = MSFDeltaUpdate(op: .update, tracks: [
            MSFDeltaTrack(name: "", parentName: "v", bitrate: 5)])
        #expect(throws: EncodingError.self) {
            _ = try JSONEncoder().encode(MSFCatalog(generatedAt: 1, deltaUpdate: [update]))
        }
        let delta = try encodeObject(
            MSFCatalog(generatedAt: 1, deltaUpdate: [update], revision: .msf02))
        let ops = try #require(delta["deltaUpdate"] as? [[String: Any]])
        let first = try #require((ops[0]["tracks"] as? [[String: Any]])?.first)
        #expect(ops[0]["op"] as? String == "update")
        #expect(first["parentName"] as? String == "v")
        #expect(first["name"] == nil)
        #expect(delta["version"] == nil)
    }

    @Test("An unknown revision is refused on encode")
    func unknownRevision() {
        let catalog = MSFCatalog(revision: MSFRevision(rawValue: 9))
        #expect(throws: EncodingError.self) { _ = try JSONEncoder().encode(catalog) }
        #expect(MSFRevision(rawValue: 1) == .msf02)
        #expect(MSFRevision.msf01.rawValue == 0)
    }

    @Test("A track base key never reaches a description")
    func keyIsRedacted() {
        var track = MSFTrack(name: "v", packaging: "loc", isLive: true)
        track.trackBaseKey = MSFTrackBaseKey("c2VjcmV0LWtleQ==")
        let catalog = MSFCatalog(tracks: [track], revision: .msf02)
        var dumped = ""
        dump(catalog, to: &dumped)
        for text in [String(describing: catalog), String(reflecting: catalog), dumped,
                     String(describing: track.trackBaseKey!),
                     String(reflecting: track.trackBaseKey!)] {
            #expect(!text.contains("c2VjcmV0LWtleQ=="))
        }
        #expect(track.trackBaseKey?.value == "c2VjcmV0LWtleQ==")
    }
}

@Suite("MSF-02 decode rules")
struct MSF02DecodeRuleTests {

    @Test("draft-02 is accepted; other version strings are still refused")
    func versions() throws {
        #expect(try decode(#"{"version":"draft-02","tracks":[]}"#).revision == .msf02)
        #expect(try decode(#"{"version":"draft-01","tracks":[]}"#).revision == .msf01)
        #expect(try decode(#"{"version":1,"tracks":[]}"#).revision == .msf01)
        #expect(throws: DecodingError.self) { try decode(#"{"version":"draft-03","tracks":[]}"#) }
    }

    @Test("An MSF-01 catalog leaves the MSF-02-only keys unread")
    func msf01IgnoresMSF02Keys() throws {
        let catalog = try decode("""
        {"version":"draft-01","tracks":[{"name":"v","packaging":"loc","isLive":true,
          "keyframeSpacing":30,"accessibleWindowTime":5,"packagingMetadata":"streamMapping=object",
          "encryptionScheme":"moq-secure-objects","keyId":"k"},
         {"name":"c","packaging":"loc","isLive":true,"depends":["v",1]}]}
        """)
        let v = catalog.tracks[0]
        #expect(v.keyframeSpacing == nil && v.accessibleWindowTime == nil)
        #expect(v.packagingMetadata == nil)
        #expect(v.encryptionScheme == "moq-secure-objects" && v.keyId == "k")
        // A Number entry is malformed in MSF-01, so the array is absent.
        #expect(catalog.tracks[1].depends == nil)
        #expect(catalog.tracks[1].dependsRenderGroups == nil)
    }

    @Test("A depends entry that is neither a String nor an integer is malformed")
    func malformedDepends() throws {
        let catalog = try decode("""
        {"version":"draft-02","tracks":[{"name":"c","packaging":"loc","isLive":true,
          "depends":["v",true]}]}
        """)
        #expect(catalog.tracks[0].depends == nil)
        #expect(catalog.tracks[0].dependsRenderGroups == nil)
    }

    @Test("update needs parentName; a name it carries is non-empty")
    func updateShape() throws {
        let ok = try decode("""
        {"generatedAt":1,"deltaUpdate":[{"op":"update","tracks":[
          {"parentName":"v","name":"v","keyframeSpacing":60,"keyId":"k2"}]}]}
        """)
        let track = try #require(ok.deltaUpdate?.first?.tracks.first)
        #expect(track.name == "v" && track.keyframeSpacing == 60 && track.keyId == "k2")
        #expect(ok.revision == .msf02)

        #expect(throws: DecodingError.self) {
            try decode(#"{"deltaUpdate":[{"op":"update","tracks":[{"name":"v","bitrate":1}]}]}"#)
        }
        #expect(throws: DecodingError.self) {
            try decode(#"{"deltaUpdate":[{"op":"update","tracks":[{"parentName":"v","name":""}]}]}"#)
        }
        // The other ops still require name.
        #expect(throws: DecodingError.self) {
            try decode(#"{"deltaUpdate":[{"op":"add","tracks":[{"packaging":"loc"}]}]}"#)
        }
        #expect(throws: DecodingError.self) {
            try decode(#"{"deltaUpdate":[{"op":"clone","tracks":[{"parentName":"v"}]}]}"#)
        }
    }

    @Test("A delta's MSF-02 track fields make it draft-02 vocabulary")
    func deltaVocabulary() throws {
        let fields = try decode("""
        {"deltaUpdate":[{"op":"add","tracks":[{"name":"a","packaging":"loc","isLive":true,
          "keyframeSpacing":2}]}]}
        """)
        #expect(fields.revision == .msf02)
        let numbers = try decode("""
        {"deltaUpdate":[{"op":"add","tracks":[{"name":"a","packaging":"loc","isLive":true,
          "depends":[3]}]}]}
        """)
        #expect(numbers.revision == .msf02)
        #expect(numbers.deltaUpdate?[0].tracks[0].dependsRenderGroups == [3])
        let plain = try decode("""
        {"deltaUpdate":[{"op":"add","tracks":[{"name":"a","packaging":"loc","isLive":true,
          "keyId":"k"}]}]}
        """)
        #expect(plain.revision == .msf01)
    }
}
