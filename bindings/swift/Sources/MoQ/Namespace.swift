import Foundation

/// A MoQ namespace part: arbitrary bytes with string convenience.
///
/// Codable: always encodes as base64 for unambiguous round-trip.
/// Decoding tries base64 first, falls back to UTF-8.
public struct NamespacePart: Sendable, Hashable {
    public let bytes: Data

    /// Create from raw bytes.
    public init(_ bytes: Data) {
        self.bytes = bytes
    }

    /// Create from a UTF-8 string.
    public init(_ string: String) {
        self.bytes = Data(string.utf8)
    }

    /// The bytes interpreted as UTF-8, if valid.
    public var stringValue: String? {
        String(data: bytes, encoding: .utf8)
    }
}

extension NamespacePart: Codable {
    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        let str = try container.decode(String.self)
        // Try base64 first.
        if let decoded = Data(base64Encoded: str) {
            self.bytes = decoded
        } else {
            // Fall back to UTF-8.
            self.bytes = Data(str.utf8)
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(bytes.base64EncodedString())
    }
}

extension NamespacePart: CustomStringConvertible {
    public var description: String {
        stringValue ?? bytes.base64EncodedString()
    }
}

// MARK: - Namespace

/// A MoQ namespace: ordered sequence of parts.
public struct Namespace: Sendable, Hashable, Codable {
    public let parts: [NamespacePart]

    public init(_ parts: [NamespacePart]) {
        self.parts = parts
    }

    public init(_ parts: NamespacePart...) {
        self.parts = parts
    }

    /// Convenience: create from string literals.
    public init(strings: String...) {
        self.parts = strings.map { NamespacePart($0) }
    }
}

extension Namespace: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: String...) {
        self.parts = elements.map { NamespacePart($0) }
    }
}

extension Namespace: CustomStringConvertible {
    public var description: String {
        parts.map(\.description).joined(separator: "/")
    }
}

// MARK: - TrackName

/// A fully-qualified track name.
public struct TrackName: Sendable, Hashable {
    public let namespace: Namespace
    public let name: NamespacePart

    public init(namespace: Namespace, name: String) {
        self.namespace = namespace
        self.name = NamespacePart(name)
    }

    public init(namespace: Namespace, name: Data) {
        self.namespace = namespace
        self.name = NamespacePart(name)
    }
}

extension TrackName: CustomStringConvertible {
    public var description: String {
        "\(namespace)/\(name)"
    }
}
