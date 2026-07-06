/// A MoQ track namespace: an ordered tuple of UTF-8 parts.
///
/// String literals split on `/` for the common path-like shape:
/// `"live/cam1"` is the two-part namespace `["live", "cam1"]`. Malformed
/// literals are NOT silently normalized — `"live/"` keeps its trailing empty
/// part and is rejected at configuration validation, so a typo fails loudly
/// instead of quietly changing the namespace.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct MediaNamespace: Sendable, Hashable, ExpressibleByStringLiteral {
    public var parts: [String]

    public init(parts: [String]) {
        self.parts = parts
    }

    /// Build a namespace from a runtime `/`-separated path, with the same
    /// semantics as a string literal: split on `/` keeping empty parts, so a
    /// trailing or doubled `/` is preserved and fails loudly at validation
    /// rather than being silently normalized. Lets apps pass a user-entered
    /// string directly instead of hand-splitting into `parts`.
    public init(_ path: String) {
        self.parts = path
            .split(separator: "/", omittingEmptySubsequences: false)
            .map(String.init)
    }

    public init(stringLiteral value: String) {
        self.init(value)
    }

    public var isEmpty: Bool { parts.isEmpty }

    /// A namespace usable on the wire: at least one part, no empty parts.
    package func validate() throws {
        guard !parts.isEmpty else {
            throw MoQServiceError.invalidArgument("namespace must not be empty")
        }
        guard !parts.contains(where: \.isEmpty) else {
            throw MoQServiceError.invalidArgument(
                "namespace parts must not be empty (check for trailing or doubled '/')")
        }
    }
}
