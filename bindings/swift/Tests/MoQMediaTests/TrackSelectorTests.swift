import Testing
import Foundation
@testable import MoQMedia

@Suite("TrackSelector")
struct TrackSelectorTests {

    @Test("CMAF preferred over LOC for H.264 video")
    func cmafPreferred() throws {
        let catalog = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v-loc", packaging: "loc", isLive: true,
                     role: "video", codec: "avc1.640028"),
            MSFTrack(name: "v-cmaf", packaging: "cmaf", isLive: true,
                     role: "video", codec: "avc1.64001f"),
        ])

        let selected = catalog.selectTracks(matching: .h264Video)
        #expect(selected.count == 2)
        #expect(selected[0].name == "v-cmaf")
        #expect(selected[1].name == "v-loc")
    }

    @Test("Catalog order preserved within same packaging rank")
    func catalogOrder() throws {
        let catalog = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v1", packaging: "cmaf", isLive: true,
                     role: "video", codec: "avc1.1"),
            MSFTrack(name: "v2", packaging: "cmaf", isLive: true,
                     role: "video", codec: "avc1.2"),
            MSFTrack(name: "v3", packaging: "cmaf", isLive: true,
                     role: "video", codec: "avc1.3"),
        ])

        let selected = catalog.selectTracks(matching: .h264Video)
        #expect(selected.map(\.name) == ["v1", "v2", "v3"])
    }

    @Test("Codec prefix matching is case-insensitive")
    func caseInsensitive() throws {
        let catalog = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v", packaging: "loc", isLive: true,
                     role: "video", codec: "AVC1.640028"),
        ])

        let selected = catalog.selectTracks(matching: .h264Video)
        #expect(selected.count == 1)
    }

    @Test("No matches returns empty")
    func noMatches() throws {
        let catalog = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v", packaging: "loc", isLive: true,
                     role: "video", codec: "hev1.1.6.L93"),
        ])

        let selected = catalog.selectTracks(matching: .h264Video)
        #expect(selected.isEmpty)
    }

    @Test("Empty codecPrefixes matches any codec for the role")
    func emptyCodecPrefixes() throws {
        let catalog = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "a1", packaging: "loc", isLive: true,
                     role: "audio", codec: "opus"),
            MSFTrack(name: "a2", packaging: "loc", isLive: true,
                     role: "audio", codec: "mp4a.40.2"),
            MSFTrack(name: "v1", packaging: "loc", isLive: true,
                     role: "video", codec: "avc1"),
        ])

        let criteria = TrackSelector.Criteria(
            role: "audio", codecPrefixes: [])
        let selected = catalog.selectTracks(matching: criteria)
        #expect(selected.count == 2)
        #expect(selected[0].name == "a1")
        #expect(selected[1].name == "a2")
    }

    @Test("Unpreferred packaging appears after preferred")
    func unpreferredLast() throws {
        let catalog = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v-mp2t", packaging: "mp2t", isLive: true,
                     role: "video", codec: "avc1"),
            MSFTrack(name: "v-loc", packaging: "loc", isLive: true,
                     role: "video", codec: "avc1"),
        ])

        let selected = catalog.selectTracks(matching: .h264Video)
        #expect(selected.count == 2)
        #expect(selected[0].name == "v-loc")
        #expect(selected[1].name == "v-mp2t")
    }

    @Test("Multiple unpreferred tracks preserve catalog order")
    func unpreferredOrder() throws {
        let catalog = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "v-dash", packaging: "dash", isLive: true,
                     role: "video", codec: "avc1"),
            MSFTrack(name: "v-hls", packaging: "hls", isLive: true,
                     role: "video", codec: "avc1"),
            MSFTrack(name: "v-mp2t", packaging: "mp2t", isLive: true,
                     role: "video", codec: "avc1"),
        ])

        let selected = catalog.selectTracks(matching: .h264Video)
        #expect(selected.map(\.name) == ["v-dash", "v-hls", "v-mp2t"])
    }

    @Test("aacAudio matches mp4a only, not opus")
    func aacNotOpus() throws {
        let catalog = MSFCatalog(version: 1, tracks: [
            MSFTrack(name: "opus", packaging: "loc", isLive: true,
                     role: "audio", codec: "opus"),
            MSFTrack(name: "aac", packaging: "loc", isLive: true,
                     role: "audio", codec: "mp4a.40.2"),
        ])

        let selected = catalog.selectTracks(matching: .aacAudio)
        #expect(selected.count == 1)
        #expect(selected[0].name == "aac")
    }
}
