import Testing
import Foundation
@testable import MoQMedia

@Suite("MSF Catalog Decode")
struct MSFCatalogDecodeTests {

    @Test("Decode minimal catalog")
    func decodeMinimal() throws {
        let json = """
        {
          "version": 1,
          "tracks": [
            {"name": "video", "packaging": "loc", "isLive": true}
          ]
        }
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))

        #expect(catalog.version == 1)
        #expect(catalog.generatedAt == nil)
        #expect(catalog.tracks.count == 1)
        #expect(catalog.tracks[0].name == "video")
        #expect(catalog.tracks[0].packaging == "loc")
        #expect(catalog.tracks[0].isLive == true)
    }

    @Test("Decode video track with all fields")
    func decodeVideoTrack() throws {
        let json = """
        {
          "version": 1,
          "generatedAt": 1746104606044,
          "tracks": [{
            "name": "1080p-video",
            "namespace": "conference.example.com/conference123/alice",
            "packaging": "loc",
            "isLive": true,
            "targetLatency": 2000,
            "role": "video",
            "renderGroup": 1,
            "codec": "av01.0.08M.10.0.110.09",
            "width": 1920,
            "height": 1080,
            "framerate": 30,
            "bitrate": 1500000,
            "initData": "AAAB",
            "timescale": 90000,
            "altGroup": 2,
            "label": "HD Video",
            "lang": "en"
          }]
        }
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))

        #expect(catalog.generatedAt == 1746104606044)
        let t = catalog.tracks[0]
        #expect(t.name == "1080p-video")
        #expect(t.namespace == "conference.example.com/conference123/alice")
        #expect(t.role == "video")
        #expect(t.codec == "av01.0.08M.10.0.110.09")
        #expect(t.width == 1920)
        #expect(t.height == 1080)
        #expect(t.framerate == 30)
        #expect(t.bitrate == 1500000)
        #expect(t.initData == "AAAB")
        #expect(t.renderGroup == 1)
        #expect(t.altGroup == 2)
        #expect(t.timescale == 90000)
        #expect(t.targetLatency == 2000)
        #expect(t.label == "HD Video")
        #expect(t.lang == "en")
    }

    @Test("Decode audio track")
    func decodeAudioTrack() throws {
        let json = """
        {
          "version": 1,
          "tracks": [{
            "name": "audio",
            "packaging": "loc",
            "isLive": true,
            "role": "audio",
            "codec": "opus",
            "samplerate": 48000,
            "channelConfig": "2",
            "bitrate": 32000
          }]
        }
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))

        let t = catalog.tracks[0]
        #expect(t.samplerate == 48000)
        #expect(t.channelConfig == "2")
        #expect(t.codec == "opus")
    }

    @Test("Unknown extra fields are ignored")
    func unknownFieldsIgnored() throws {
        let json = """
        {
          "version": 1,
          "futureField": "ignored",
          "tracks": [{
            "name": "t",
            "packaging": "loc",
            "isLive": false,
            "extraTrackField": 42
          }]
        }
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))

        #expect(catalog.tracks[0].name == "t")
    }

    @Test("Negative width decodes as nil")
    func negativeWidth() throws {
        let json = """
        {"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true,"width":-1}]}
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))
        #expect(catalog.tracks[0].width == nil)
    }

    @Test("Fractional width decodes as nil")
    func fractionalWidth() throws {
        let json = """
        {"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true,"width":19.5}]}
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))
        #expect(catalog.tracks[0].width == nil)
    }

    @Test("Negative generatedAt decodes as nil")
    func negativeGeneratedAt() throws {
        let json = """
        {"version":1,"generatedAt":-1,"tracks":[{"name":"v","packaging":"loc","isLive":true}]}
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))
        #expect(catalog.generatedAt == nil)
    }

    @Test("renderGroup overflow decodes as nil")
    func renderGroupOverflow() throws {
        let json = """
        {"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true,"renderGroup":99999999999999999999}]}
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))
        #expect(catalog.tracks[0].renderGroup == nil)
    }

    @Test("renderGroup just above Int32.max decodes as nil")
    func renderGroupAboveInt32Max() throws {
        let json = """
        {"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true,"renderGroup":2147483648}]}
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))
        #expect(catalog.tracks[0].renderGroup == nil)
    }

    @Test("altGroup just below Int32.min decodes as nil")
    func altGroupBelowInt32Min() throws {
        let json = """
        {"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true,"altGroup":-2147483649}]}
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))
        #expect(catalog.tracks[0].altGroup == nil)
    }

    @Test("renderGroup at Int32.max decodes correctly")
    func renderGroupAtInt32Max() throws {
        let json = """
        {"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true,"renderGroup":2147483647}]}
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))
        #expect(catalog.tracks[0].renderGroup == 2147483647)
    }

    @Test("Huge framerate decodes as nil")
    func hugeFramerate() throws {
        let json = """
        {"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":true,"framerate":1e308}]}
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))
        #expect(catalog.tracks[0].framerate == nil)
    }

    @Test("Missing required name throws")
    func missingName() throws {
        let json = """
        {"version":1,"tracks":[{"packaging":"loc","isLive":true}]}
        """
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(
                MSFCatalog.self, from: Data(json.utf8))
        }
    }

    @Test("Wrong-type required isLive throws")
    func wrongTypeIsLive() throws {
        let json = """
        {"version":1,"tracks":[{"name":"v","packaging":"loc","isLive":"yes"}]}
        """
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(
                MSFCatalog.self, from: Data(json.utf8))
        }
    }

    @Test("Fractional framerate decodes correctly")
    func fractionalFramerate() throws {
        let json = """
        {
          "version": 1,
          "tracks": [{
            "name": "ntsc",
            "packaging": "loc",
            "isLive": true,
            "framerate": 29.97
          }]
        }
        """
        let catalog = try JSONDecoder().decode(
            MSFCatalog.self, from: Data(json.utf8))

        #expect(catalog.tracks[0].framerate == 29.97)
    }
}

@Suite("MSF Catalog Version (MSF-01 §5.1.1)")
struct MSFCatalogVersionTests {
    private func decode(_ versionLiteral: String) throws -> MSFCatalog {
        let json = "{\"version\":\(versionLiteral),\"tracks\":[]}"
        return try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
    }

    @Test("Accepts canonical String version \"1\"")
    func acceptsStringOne() throws {
        #expect(try decode("\"1\"").version == 1)
    }

    @Test("Accepts the draft-01 convention")
    func acceptsDraft01() throws {
        #expect(try decode("\"draft-01\"").version == 1)
    }

    @Test("Accepts legacy numeric 1 (MSF-00 compatibility)")
    func acceptsLegacyNumeric() throws {
        #expect(try decode("1").version == 1)
    }

    @Test("Rejects unsupported / malformed versions")
    func rejectsUnsupported() throws {
        for bad in ["\"2\"", "\"draft-00\"", "\"draft-99\"", "\"draft-1\"", "\"\"", "2", "true"] {
            #expect(throws: (any Error).self) { _ = try decode(bad) }
        }
    }

    @Test("Encodes the canonical String version \"1\"")
    func encodesCanonicalString() throws {
        let data = try JSONEncoder().encode(MSFCatalog(version: 1, tracks: []))
        let json = String(data: data, encoding: .utf8)!
        #expect(json.contains("\"version\":\"1\""))
    }

    @Test("Encoding an unsupported version throws (no wire we'd refuse to parse)")
    func encodingUnsupportedVersionThrows() throws {
        #expect(throws: (any Error).self) {
            _ = try JSONEncoder().encode(MSFCatalog(version: 2, tracks: []))
        }
    }

    @Test("Round-trips a libmoq-style MSF-01 catalog (String version)")
    func roundTripsStringVersion() throws {
        let json = """
        {"version":"1","generatedAt":1,"tracks":[\
        {"name":"video","packaging":"loc","isLive":true,"role":"video","codec":"avc1.42e01e"}]}
        """
        let decoded = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        #expect(decoded.version == 1)
        #expect(decoded.tracks.map(\.name) == ["video"])

        // Re-encode → canonical String "1".
        let reJson = String(data: try JSONEncoder().encode(decoded), encoding: .utf8)!
        #expect(reJson.contains("\"version\":\"1\""))
    }
}

@Suite("MSF Catalog Encode")
struct MSFCatalogEncodeTests {

    @Test("Encode omits nil optional fields")
    func encodeOmitsNil() throws {
        let track = MSFTrack(name: "v", packaging: "loc", isLive: true)
        let catalog = MSFCatalog(version: 1, tracks: [track])

        let data = try JSONEncoder().encode(catalog)
        let json = String(data: data, encoding: .utf8)!

        #expect(!json.contains("namespace"))
        #expect(!json.contains("codec"))
        #expect(!json.contains("width"))
        #expect(!json.contains("samplerate"))
        #expect(!json.contains("framerate"))
        #expect(!json.contains("initData"))
        #expect(!json.contains("renderGroup"))
        #expect(!json.contains("null"))
        #expect(json.contains("\"name\""))
        #expect(json.contains("\"packaging\""))
        #expect(json.contains("\"isLive\""))
    }

    @Test("Encode round-trips with decode")
    func encodeDecodeRoundTrip() throws {
        let track = MSFTrack(
            name: "video", packaging: "loc", isLive: true,
            role: "video", codec: "h264", width: 1280, height: 720,
            framerate: 30, renderGroup: 1)
        let original = MSFCatalog(
            version: 1, generatedAt: 12345, tracks: [track])

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(
            MSFCatalog.self, from: data)

        #expect(decoded == original)
    }

    @Test("Out-of-range renderGroup is omitted on encode")
    func encodeOmitsOutOfRangeRenderGroup() throws {
        var track = MSFTrack(name: "v", packaging: "loc", isLive: true)
        track.renderGroup = Int(Int32.max) + 1

        let data = try JSONEncoder().encode(track)
        let json = String(data: data, encoding: .utf8)!
        #expect(!json.contains("renderGroup"))
    }

    @Test("In-range altGroup is included on encode")
    func encodeIncludesInRangeAltGroup() throws {
        let track = MSFTrack(
            name: "v", packaging: "loc", isLive: true, altGroup: -5)

        let data = try JSONEncoder().encode(track)
        let json = String(data: data, encoding: .utf8)!
        #expect(json.contains("\"altGroup\""))
        #expect(json.contains("-5"))
    }
}

@Suite("MSF Wire Keys")
struct MSFWireKeyTests {

    @Test("samplerate wire key is exact")
    func samplerateKey() throws {
        let track = MSFTrack(
            name: "a", packaging: "loc", isLive: true, samplerate: 44100)
        let data = try JSONEncoder().encode(track)
        let json = String(data: data, encoding: .utf8)!

        #expect(json.contains("\"samplerate\""))
        #expect(!json.contains("\"sampleRate\""))
        #expect(!json.contains("\"sample_rate\""))
    }

    @Test("framerate wire key is exact")
    func framerateKey() throws {
        let track = MSFTrack(
            name: "v", packaging: "loc", isLive: true, framerate: 60)
        let data = try JSONEncoder().encode(track)
        let json = String(data: data, encoding: .utf8)!

        #expect(json.contains("\"framerate\""))
        #expect(!json.contains("\"frameRate\""))
        #expect(!json.contains("\"frame_rate\""))
    }

    @Test("initData wire key is exact")
    func initDataKey() throws {
        let track = MSFTrack(
            name: "v", packaging: "loc", isLive: true, initData: "AAAB")
        let data = try JSONEncoder().encode(track)
        let json = String(data: data, encoding: .utf8)!

        #expect(json.contains("\"initData\""))
        #expect(!json.contains("\"init_data\""))
    }

    @Test("channelConfig wire key is exact")
    func channelConfigKey() throws {
        let track = MSFTrack(
            name: "a", packaging: "loc", isLive: true, channelConfig: "2")
        let data = try JSONEncoder().encode(track)
        let json = String(data: data, encoding: .utf8)!

        #expect(json.contains("\"channelConfig\""))
    }
}

@Suite("MSF initData Helpers")
struct MSFInitDataTests {

    @Test("decodedInitData round-trips with setInitData")
    func initDataRoundTrip() throws {
        let original = Data([0x00, 0x00, 0x01, 0xFF, 0xAB])
        var track = MSFTrack(name: "v", packaging: "loc", isLive: true)
        track.setInitData(original)

        #expect(track.initData != nil)

        let decoded = try track.decodedInitData()
        #expect(decoded == original)
    }

    @Test("decodedInitData from JSON base64")
    func initDataFromJSON() throws {
        let json = """
        {"name":"v","packaging":"loc","isLive":true,"initData":"AAAB"}
        """
        let track = try JSONDecoder().decode(
            MSFTrack.self, from: Data(json.utf8))

        let data = try track.decodedInitData()
        #expect(data == Data([0x00, 0x00, 0x01]))
    }

    @Test("decodedInitData throws when missing")
    func initDataMissing() throws {
        let track = MSFTrack(name: "v", packaging: "loc", isLive: true)
        #expect(throws: MSFError.missingInitData) {
            _ = try track.decodedInitData()
        }
    }

    @Test("decodedInitData throws on invalid base64")
    func initDataInvalid() throws {
        let track = MSFTrack(
            name: "v", packaging: "loc", isLive: true,
            initData: "!!!not-base64!!!")
        #expect(throws: MSFError.invalidBase64) {
            _ = try track.decodedInitData()
        }
    }
}

@Suite("MSF Catalog Field Parity (MSF-01/CMSF-01)")
struct MSFCatalogParityTests {

    /// Decode a C-encoder-compatible catalog exercising the newly added fields,
    /// then round-trip it through encode+decode.
    @Test("Decode + round-trip the added MSF-01/CMSF-01 track + catalog fields")
    func parityFields() throws {
        let json = """
        {
          "version": "1",
          "isComplete": true,
          "tracks": [
            {
              "name": "v", "packaging": "cmaf", "isLive": false,
              "codec": "avc1.640028", "bitrate": 1500000,
              "initRef": "init-v", "mimeType": "video/mp4",
              "trackDuration": 8072,
              "contentProtectionRefIDs": ["cp1", "cp2"],
              "maxGrpSapStartingType": 1, "maxObjSapStartingType": 2
            },
            {
              "name": "v.sap", "packaging": "eventtimeline", "isLive": false,
              "eventType": "org.ietf.moq.cmsf.sap", "mimeType": "application/json",
              "depends": ["v"]
            }
          ]
        }
        """
        let cat = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        #expect(cat.isComplete == true)
        let v = cat.tracks[0]
        #expect(v.initRef == "init-v")
        #expect(v.mimeType == "video/mp4")
        #expect(v.trackDuration == 8072)
        #expect(v.contentProtectionRefIDs == ["cp1", "cp2"])
        #expect(v.maxGrpSapStartingType == 1)
        #expect(v.maxObjSapStartingType == 2)
        let sap = cat.tracks[1]
        #expect(sap.eventType == "org.ietf.moq.cmsf.sap")
        #expect(sap.depends == ["v"])

        // Round-trip: encode then decode yields an equal catalog.
        let data = try JSONEncoder().encode(cat)
        let back = try JSONDecoder().decode(MSFCatalog.self, from: data)
        #expect(back == cat)
        #expect(back.isComplete == true)
        #expect(back.tracks[0].contentProtectionRefIDs == ["cp1", "cp2"])
        #expect(back.tracks[1].depends == ["v"])
    }

    /// §5.1.3: isComplete is emitted only when true (false => absent on the wire).
    @Test("isComplete false is omitted on encode")
    func isCompleteOmittedWhenFalse() throws {
        let cat = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v", packaging: "loc", isLive: true)
        ], isComplete: false)
        let data = try JSONEncoder().encode(cat)
        let s = String(decoding: data, as: UTF8.self)
        #expect(!s.contains("isComplete"))
    }
}

@Suite("MSF initDataList Parity (MSF-01 §5.1.7)")
struct MSFInitDataListTests {

    @Test("Decode + round-trip root initDataList with a track initRef")
    func initDataList() throws {
        let json = """
        {
          "version": "1",
          "initDataList": [
            {"id": "init-v", "type": "inline", "data": "AAECAwQF"},
            {"id": "init-a", "type": "inline", "data": "BgcICQ=="}
          ],
          "tracks": [
            {"name": "v", "packaging": "cmaf", "isLive": false,
             "codec": "avc1.640028", "bitrate": 1500000, "initRef": "init-v"}
          ]
        }
        """
        let cat = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        #expect(cat.initDataList?.count == 2)
        #expect(cat.initDataList?[0] == MSFInitDataEntry(
            id: "init-v", type: "inline", data: "AAECAwQF"))
        #expect(cat.initDataList?[1].id == "init-a")
        #expect(cat.tracks[0].initRef == "init-v")   // references the entry id

        let data = try JSONEncoder().encode(cat)
        let back = try JSONDecoder().decode(MSFCatalog.self, from: data)
        #expect(back == cat)
        #expect(back.initDataList?[0].data == "AAECAwQF")
    }

    @Test("nil initDataList is omitted on encode (no key, no null)")
    func initDataListOmittedWhenNil() throws {
        let cat = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v", packaging: "loc", isLive: true)
        ])
        let s = String(decoding: try JSONEncoder().encode(cat), as: UTF8.self)
        #expect(!s.contains("initDataList"))
        #expect(!s.contains("null"))
    }
}

@Suite("CMSF contentProtections Parity (CMSF §4.1.1/§4.1.2)")
struct CMSFContentProtectionTests {

    @Test("Decode + round-trip root contentProtections with track refs")
    func contentProtections() throws {
        let json = """
        {
          "version": "1",
          "contentProtections": [
            {
              "refID": "cp1",
              "defaultKID": ["01234567-89ab-cdef-0123-456789abcdef"],
              "scheme": "cbcs",
              "drmSystem": {
                "systemID": "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed",
                "laURL": {"url": "https://la.example/x", "type": "EME-1.0"},
                "certURL": {"url": "https://cert.example/y"},
                "authURL": {"url": "https://auth.example/z", "type": "auth"},
                "pssh": "AAAB",
                "robustness": "HW_SECURE_ALL"
              }
            }
          ],
          "tracks": [
            {"name": "v", "packaging": "cmaf", "isLive": false,
             "codec": "avc1.640028", "bitrate": 1500000,
             "contentProtectionRefIDs": ["cp1"]}
          ]
        }
        """
        let cat = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        #expect(cat.contentProtections?.count == 1)
        let cp = try #require(cat.contentProtections?.first)
        #expect(cp.refID == "cp1")
        #expect(cp.defaultKID == ["01234567-89ab-cdef-0123-456789abcdef"])
        #expect(cp.scheme == "cbcs")
        #expect(cp.drmSystem.systemID == "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed")
        #expect(cp.drmSystem.laURL == CMSFURL(url: "https://la.example/x", type: "EME-1.0"))
        #expect(cp.drmSystem.certURL?.url == "https://cert.example/y")
        #expect(cp.drmSystem.certURL?.type == nil)   // optional type absent
        #expect(cp.drmSystem.authURL?.type == "auth")
        #expect(cp.drmSystem.pssh == "AAAB")
        #expect(cp.drmSystem.robustness == "HW_SECURE_ALL")
        #expect(cat.tracks[0].contentProtectionRefIDs == ["cp1"])

        // Round-trip equality.
        let back = try JSONDecoder().decode(
            MSFCatalog.self, from: try JSONEncoder().encode(cat))
        #expect(back == cat)
    }

    @Test("nil contentProtections is omitted on encode (no key, no null)")
    func contentProtectionsOmittedWhenNil() throws {
        let cat = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v", packaging: "loc", isLive: true)
        ])
        let s = String(decoding: try JSONEncoder().encode(cat), as: UTF8.self)
        #expect(!s.contains("contentProtections"))
        #expect(!s.contains("null"))
    }

    @Test("Optional drmSystem URL/pssh/robustness omitted when nil")
    func optionalDrmFieldsOmitted() throws {
        let cat = MSFCatalog(version: 1, tracks: [], contentProtections: [
            CMSFContentProtection(
                refID: "cp1", defaultKID: ["k"], scheme: "cenc",
                drmSystem: CMSFDRMSystem(systemID: "sys"))
        ])
        let s = String(decoding: try JSONEncoder().encode(cat), as: UTF8.self)
        #expect(s.contains("\"systemID\""))
        #expect(!s.contains("laURL"))
        #expect(!s.contains("pssh"))
        #expect(!s.contains("robustness"))
        #expect(!s.contains("null"))
    }
}

@Suite("MSF Media Timeline Template Parity (MSF-01 §5.2.15/§7.4)")
struct MSFMediaTemplateTests {

    @Test("Decode + round-trip a mediatimeline track with a template tuple")
    func template() throws {
        let json = """
        {
          "version": "1",
          "tracks": [
            {"name": "v.timeline", "packaging": "mediatimeline", "isLive": true,
             "mimeType": "application/json", "depends": ["v"],
             "template": [0, 33, [0, 0], [1, 0], 1700000000000, 33]}
          ]
        }
        """
        let cat = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        let t = cat.tracks[0]
        #expect(t.packaging == "mediatimeline")
        #expect(t.mimeType == "application/json")
        #expect(t.depends == ["v"])
        let tpl = try #require(t.template)
        #expect(tpl.startMediaTime == 0)
        #expect(tpl.deltaMediaTime == 33)
        #expect(tpl.startGroup == 0)
        #expect(tpl.startObject == 0)
        #expect(tpl.deltaGroup == 1)
        #expect(tpl.deltaObject == 0)
        #expect(tpl.startWallclock == 1_700_000_000_000)
        #expect(tpl.deltaWallclock == 33)

        // Round-trip: encode (as a JSON array) then decode yields equality.
        let data = try JSONEncoder().encode(cat)
        #expect(String(decoding: data, as: UTF8.self).contains("\"template\":[0,33,[0,0],[1,0],"))
        let back = try JSONDecoder().decode(MSFCatalog.self, from: data)
        #expect(back == cat)
    }

    @Test("nil template is omitted on encode (no key, no null)")
    func templateOmittedWhenNil() throws {
        let cat = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v", packaging: "loc", isLive: true)
        ])
        let s = String(decoding: try JSONEncoder().encode(cat), as: UTF8.self)
        #expect(!s.contains("template"))
        #expect(!s.contains("null"))
    }

    @Test("Malformed template decodes as nil (lenient), not a throw")
    func malformedTemplateIsNil() throws {
        // Wrong length (5 elements) and a wrong-shape location are both rejected
        // by the strict template decoder, so the optional field becomes nil.
        for bad in [
            "[0, 33, [0, 0], [1, 0], 1700000000000]",          // 5 elements
            "[0, 33, [0, 0, 9], [1, 0], 0, 33]",                // 3-element loc
            "{\"startMediaTime\": 0}",                          // object, not array
        ] {
            let json = """
            {"version":"1","tracks":[
              {"name":"v.t","packaging":"mediatimeline","isLive":true,
               "template": \(bad)}]}
            """
            let cat = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
            #expect(cat.tracks[0].template == nil)   // malformed -> absent
        }
    }
}

@Suite("MSF deltaUpdate Parity (MSF-01 §5.1.6/§5.3)")
struct MSFDeltaUpdateTests {

    @Test("add delta: round-trips; encodes deltaUpdate, not version/tracks/isComplete")
    func addDelta() throws {
        let json = """
        {"generatedAt":2,"deltaUpdate":[
          {"op":"add","tracks":[
            {"name":"a","packaging":"loc","isLive":true,"codec":"opus"}]}]}
        """
        let cat = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        #expect(cat.generatedAt == 2)
        #expect(cat.tracks.isEmpty)            // delta carries no root tracks
        #expect(cat.deltaUpdate?.count == 1)
        let op = try #require(cat.deltaUpdate?.first)
        #expect(op.op == .add)
        #expect(op.tracks[0].name == "a")
        #expect(op.tracks[0].packaging == "loc")
        #expect(op.tracks[0].isLive == true)
        #expect(op.tracks[0].codec == "opus")

        let data = try JSONEncoder().encode(cat)
        let s = String(decoding: data, as: UTF8.self)
        #expect(s.contains("deltaUpdate"))
        #expect(!s.contains("\"version\""))
        #expect(!s.contains("isComplete"))
        // No ROOT tracks: a delta has op-level "tracks" but empty root tracks,
        // proven by the round-trip below (back.tracks is empty).
        let back = try JSONDecoder().decode(MSFCatalog.self, from: data)
        #expect(back == cat)
        #expect(back.tracks.isEmpty)
    }

    @Test("remove delta: name-only track round-trips")
    func removeDelta() throws {
        let json = """
        {"deltaUpdate":[{"op":"remove","tracks":[{"name":"a"},{"name":"b","namespace":"ns"}]}]}
        """
        let cat = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        let op = try #require(cat.deltaUpdate?.first)
        #expect(op.op == .remove)
        #expect(op.tracks.map(\.name) == ["a", "b"])
        #expect(op.tracks[0].packaging == nil)   // partial: no packaging/isLive
        #expect(op.tracks[1].namespace == "ns")
        #expect(try JSONDecoder().decode(
            MSFCatalog.self, from: try JSONEncoder().encode(cat)) == cat)
    }

    @Test("clone delta: parentName + name + overrides round-trips")
    func cloneDelta() throws {
        let json = """
        {"deltaUpdate":[{"op":"clone","tracks":[
          {"parentName":"video-1080","name":"video-720","width":1280,"height":720,
           "bitrate":600000}]}]}
        """
        let cat = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        let t = try #require(cat.deltaUpdate?.first?.tracks.first)
        #expect(t.parentName == "video-1080")
        #expect(t.name == "video-720")
        #expect(t.width == 1280)
        #expect(t.height == 720)
        #expect(t.bitrate == 600000)
        #expect(try JSONDecoder().decode(
            MSFCatalog.self, from: try JSONEncoder().encode(cat)) == cat)
    }

    @Test("Independent catalog still encodes version + tracks and no deltaUpdate")
    func independentUnchanged() throws {
        let cat = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v", packaging: "loc", isLive: true)
        ])
        let s = String(decoding: try JSONEncoder().encode(cat), as: UTF8.self)
        #expect(s.contains("\"version\":\"1\""))
        #expect(s.contains("\"tracks\""))
        #expect(!s.contains("deltaUpdate"))
    }

    @Test("Unknown delta op throws")
    func unknownOpThrows() {
        let json = """
        {"deltaUpdate":[{"op":"replace","tracks":[{"name":"a"}]}]}
        """
        #expect(throws: (any Error).self) {
            _ = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        }
    }

    // -- Parity hardening: malformed deltas the C parser rejects --------
    // (media/msf/src/msf.c: root delta rules at the deltaUpdate branch, per-op
    //  rules in parse_track). These mirror the C MOQ_ERR_PROTO cases.

    private func expectDecodeThrows(_ json: String) {
        #expect(throws: (any Error).self) {
            _ = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        }
    }

    @Test("Empty deltaUpdate array throws (§5.1.6: at least one op)")
    func emptyDeltaThrows() {
        expectDecodeThrows(#"{"deltaUpdate":[]}"#)
    }

    @Test("Empty deltaUpdate mixed with version/tracks/isComplete throws")
    func emptyDeltaWithRootFieldsThrows() {
        expectDecodeThrows(
            #"{"version":"1","tracks":[],"isComplete":true,"deltaUpdate":[]}"#)
    }

    @Test("Nonempty deltaUpdate mixed with version/tracks/isComplete throws")
    func nonemptyDeltaWithRootFieldsThrows() {
        expectDecodeThrows("""
        {"version":"1","tracks":[],"isComplete":true,"deltaUpdate":[
          {"op":"add","tracks":[{"name":"a","packaging":"loc","isLive":true}]}]}
        """)
    }

    @Test("Delta carrying only version (forbidden root field) throws")
    func deltaWithVersionThrows() {
        expectDecodeThrows("""
        {"version":"1","deltaUpdate":[
          {"op":"add","tracks":[{"name":"a","packaging":"loc","isLive":true}]}]}
        """)
    }

    @Test("remove op carrying forbidden payload fields throws (§5.1.6: name/namespace only)")
    func removeWithForbiddenFieldsThrows() {
        expectDecodeThrows("""
        {"deltaUpdate":[{"op":"remove","tracks":[
          {"name":"a","packaging":"loc","isLive":true,"codec":"opus"}]}]}
        """)
    }

    @Test("clone op missing parentName throws (§5.1.6: clone requires parentName)")
    func cloneMissingParentNameThrows() {
        expectDecodeThrows("""
        {"deltaUpdate":[{"op":"clone","tracks":[{"name":"video-720","width":1280}]}]}
        """)
    }

    @Test("add op carrying parentName throws (§5.2.33/34: clone-only field)")
    func addWithParentNameThrows() {
        expectDecodeThrows("""
        {"deltaUpdate":[{"op":"add","tracks":[
          {"name":"a","packaging":"loc","isLive":true,"parentName":"x"}]}]}
        """)
    }

    @Test("add op carrying parentNamespace throws (§5.2.33/34: clone-only field)")
    func addWithParentNamespaceThrows() {
        expectDecodeThrows("""
        {"deltaUpdate":[{"op":"add","tracks":[
          {"name":"a","packaging":"loc","isLive":true,"parentNamespace":"ns"}]}]}
        """)
    }

    @Test("remove op carrying parentName throws (clone-only field)")
    func removeWithParentNameThrows() {
        expectDecodeThrows("""
        {"deltaUpdate":[{"op":"remove","tracks":[{"name":"a","parentName":"x"}]}]}
        """)
    }

    @Test("op with empty tracks array throws (§5.1.6: op MUST carry >= 1 track)")
    func opWithEmptyTracksThrows() {
        // C parity: media/msf/src/msf.c rejects this for every op kind.
        expectDecodeThrows(#"{"deltaUpdate":[{"op":"remove","tracks":[]}]}"#)
        expectDecodeThrows(#"{"deltaUpdate":[{"op":"add","tracks":[]}]}"#)
        expectDecodeThrows(#"{"deltaUpdate":[{"op":"clone","tracks":[]}]}"#)
    }

    // Structured fields (depends/contentProtectionRefIDs/template) decode
    // strictly: a present-but-malformed value throws, matching the C parser
    // (get_string_array / parse_template abort the track). Otherwise a malformed
    // value would silently become nil and let, e.g., a remove track pass the
    // name/namespace-only shape check.

    @Test("remove op with malformed depends throws (C: get_string_array errors)")
    func removeMalformedDependsThrows() {
        expectDecodeThrows(
            #"{"deltaUpdate":[{"op":"remove","tracks":[{"name":"a","depends":123}]}]}"#)
    }

    @Test("remove op with malformed template throws (C: parse_template errors)")
    func removeMalformedTemplateThrows() {
        expectDecodeThrows(
            #"{"deltaUpdate":[{"op":"remove","tracks":[{"name":"a","template":[1,2]}]}]}"#)
    }

    @Test("remove op with malformed contentProtectionRefIDs throws")
    func removeMalformedCpRefsThrows() {
        expectDecodeThrows("""
        {"deltaUpdate":[{"op":"remove","tracks":[
          {"name":"a","contentProtectionRefIDs":"not-an-array"}]}]}
        """)
    }

    @Test("add op with malformed depends throws (parity holds for all ops)")
    func addMalformedDependsThrows() {
        expectDecodeThrows("""
        {"deltaUpdate":[{"op":"add","tracks":[
          {"name":"a","packaging":"loc","isLive":true,"depends":123}]}]}
        """)
    }

    @Test("add op with VALID depends + template still decodes (no over-strictness)")
    func addValidStructuredFieldsDecodes() throws {
        let json = """
        {"deltaUpdate":[{"op":"add","tracks":[
          {"name":"a","packaging":"loc","isLive":true,"depends":["x","y"],
           "template":[0,1000,[0,0],[1,0],0,1000]}]}]}
        """
        let cat = try JSONDecoder().decode(MSFCatalog.self, from: Data(json.utf8))
        let t = try #require(cat.deltaUpdate?.first?.tracks.first)
        #expect(t.depends == ["x", "y"])
        #expect(t.template != nil)
    }
}
