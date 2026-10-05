//
//  The service and media spellings must be ONE type, not two that match.
//
//  This is the test the "single nominal type" ruling exists for. Two
//  separate enums with the same cases would satisfy every other test in the
//  tree -- each layer would compile, each would round-trip its own values --
//  and would still require a hand-written mapping at the boundary, which is
//  exactly where a draft-16 / draft-18 confusion hides.
//
import Testing
import MoQMedia
import MoQServiceCore
import MoQTransportModel

@Suite("Transport version alias identity")
struct AliasIdentityTests {

    @Test("MoQVersion and MediaTransportVersion are the same nominal type")
    func sameNominalType() {
        #expect(MoQVersion.self == MediaTransportVersion.self)
        #expect(MoQVersion.self == MoQTransportVersion.self)
        #expect(MediaTransportVersion.self == MoQTransportVersion.self)
    }

    @Test("A negotiated version crosses the boundary with no conversion")
    func noConversionNeeded() {
        // Declared with the service spelling ...
        let negotiated: MoQVersion = .draft18
        // ... consumed with the media spelling, by assignment and by call,
        // with no initializer, cast or mapping anywhere. This does not
        // compile if the two are distinct types.
        let media: MediaTransportVersion = negotiated
        #expect(media == .draft18)

        var info = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                  transportVersion: negotiated)
        #expect(info.transportVersion == negotiated)

        // And back the other way.
        let backToService: MoQVersion = info.transportVersion
        #expect(backToService.draftNumber == 18)

        info.transportVersion = .draft16
        #expect(MoQVersion.draft16 == info.transportVersion)
    }

    @Test("The shared type carries one case set")
    func oneCaseSet() {
        #expect(MoQVersion.allCases == MediaTransportVersion.allCases)
        #expect(MoQVersion.allCases.map(\.draftNumber).sorted() == [16, 18, 21])
    }
}
