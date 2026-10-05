//
//  A consumer that imports ONLY MoQMedia.
//
//  The point is the import list, so this file deliberately imports nothing
//  else -- no MoQ, no MoQServiceCore, no C module. If naming or setting the
//  media-facing transport version ever required another module, this file
//  would stop compiling, which is the whole assertion.
//
import Testing
import MoQMedia

@Suite("MoQMedia-only consumer")
struct MediaOnlyConsumerTests {

    @Test("The media-facing version type can be named and set")
    func nameAndSet() {
        // Named as a type, in a signature, and as a stored value.
        let version: MediaTransportVersion = .draft18
        #expect(version == .draft18)
        #expect(version.draftNumber == 18)

        var info = MediaTrackInfo(mediaType: .video, packaging: .raw,
                                  transportVersion: version)
        #expect(info.transportVersion == .draft18)

        info.transportVersion = .draft16
        #expect(info.transportVersion == .draft16)
        #expect(info.transportVersion.draftNumber == 16)

        // Usable as a generic constraint witness and across a function
        // boundary without any conversion.
        #expect(roundTrip(.draft18) == .draft18)
        #expect(Set(MediaTransportVersion.allCases).count == 3)
    }

    private func roundTrip(_ v: MediaTransportVersion) -> MediaTransportVersion {
        v
    }
}
