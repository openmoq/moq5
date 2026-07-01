import Testing
import Foundation
@testable import MoQ

@Suite("NamespacePart")
struct NamespacePartTests {
    @Test("String init stores UTF-8 bytes")
    func stringInit() {
        let part = NamespacePart("hello")
        #expect(part.bytes == Data("hello".utf8))
        #expect(part.stringValue == "hello")
    }

    @Test("Data init stores raw bytes")
    func dataInit() {
        let raw = Data([0xFF, 0xFE, 0x00, 0x01])
        let part = NamespacePart(raw)
        #expect(part.bytes == raw)
        #expect(part.stringValue == nil)  // not valid UTF-8
    }

    @Test("Codable round-trip for UTF-8 part")
    func codableUTF8() throws {
        let original = NamespacePart("moq-test")
        let json = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(NamespacePart.self, from: json)
        #expect(decoded.bytes == original.bytes)
    }

    @Test("Codable round-trip for non-UTF-8 bytes")
    func codableNonUTF8() throws {
        let raw = Data([0xFF, 0xFE, 0x00, 0x80])
        let original = NamespacePart(raw)
        let json = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(NamespacePart.self, from: json)
        #expect(decoded.bytes == original.bytes)
    }

    @Test("Codable encodes as base64 string")
    func codableFormat() throws {
        let part = NamespacePart("AB")
        let json = try JSONEncoder().encode(part)
        let str = String(data: json, encoding: .utf8)!
        let expected = Data("AB".utf8).base64EncodedString()
        #expect(str.contains(expected))
    }

    @Test("Codable decodes plain non-base64 string as UTF-8 fallback")
    func codablePlainStringFallback() throws {
        // "hello world" is not valid base64 (contains space).
        let json = Data("\"hello world\"".utf8)
        let decoded = try JSONDecoder().decode(NamespacePart.self, from: json)
        #expect(decoded.bytes == Data("hello world".utf8))
        #expect(decoded.stringValue == "hello world")
    }
}

@Suite("Namespace")
struct NamespaceTests {
    @Test("Array literal")
    func arrayLiteral() {
        let ns: Namespace = ["moq-test", "interop"]
        #expect(ns.parts.count == 2)
        #expect(ns.parts[0].stringValue == "moq-test")
        #expect(ns.parts[1].stringValue == "interop")
    }

    @Test("Description")
    func description() {
        let ns: Namespace = ["live", "video"]
        #expect(ns.description == "live/video")
    }
}

@Suite("TrackName")
struct TrackNameTests {
    @Test("String convenience")
    func stringConvenience() {
        let tn = TrackName(namespace: ["live"], name: "video")
        #expect(tn.namespace.parts.count == 1)
        #expect(tn.name.stringValue == "video")
        #expect(tn.description == "live/video")
    }
}
