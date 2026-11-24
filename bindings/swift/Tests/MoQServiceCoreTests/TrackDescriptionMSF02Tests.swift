import Testing
import Foundation
@testable import MoQServiceCore

/* The pure model of the MSF-02 description fields: absent by default, the
 * track base key kept out of every description of a value, and the
 * replacement vocabulary of the player model. */

@Suite("MSF-02 track description model")
struct TrackDescriptionMSF02Tests {

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A new description carries none of the MSF-02 fields")
    func defaults() {
        let description = TrackDescription(name: "video", mediaType: .video, packaging: .raw)
        #expect(description.configGeneration == nil)
        #expect(description.initSource == nil)
        #expect(description.initPropertyType == nil)
        #expect(description.encryptionScheme == nil)
        #expect(description.cipherSuite == nil)
        #expect(description.keyID == nil)
        #expect(description.trackBaseKey == nil)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The track base key is readable but never described")
    func keyIsRedacted() {
        var description = TrackDescription(name: "video", mediaType: .video, packaging: .raw)
        description.encryptionScheme = "moq-secure-objects"
        description.trackBaseKey = TrackBaseKey("c2VjcmV0LWtleQ==")
        #expect(description.trackBaseKey?.value == "c2VjcmV0LWtleQ==")
        var dumped = ""
        dump(description, to: &dumped)
        let key = description.trackBaseKey!
        for text in [String(describing: description), String(reflecting: description), dumped,
                     String(describing: key), String(reflecting: key), "\(key)"] {
            #expect(!text.contains("c2VjcmV0LWtleQ=="))
        }
        #expect(String(describing: description).contains("moq-secure-objects"))
        #expect(TrackBaseKey("a") == TrackBaseKey("a"))
        #expect(TrackBaseKey("a") != TrackBaseKey("b"))
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Init sources compare by value, an unknown one by its raw value")
    func initSources() {
        #expect(InitSource.unknown(9) == .unknown(9))
        #expect(InitSource.unknown(9) != .unknown(8))
        #expect(InitSource.inline != .trackProperty)
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The player model names a replacement and its end cause")
    func playerReplacement() {
        let old = PlayerTrack(
            id: 1, description: TrackDescription(name: "v", mediaType: .video, packaging: .raw))
        let new = PlayerTrack(
            id: 2, description: TrackDescription(name: "v", mediaType: .video, packaging: .raw))
        let event = PlayerEvent.trackReplaced(old, replacement: new)
        guard case .trackReplaced(let a, let b) = event else {
            Issue.record("trackReplaced")
            return
        }
        #expect(a == old && b == new)
        #expect(PlayerEndCause.replaced != .removed)
    }
}
