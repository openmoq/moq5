import CMoQCore
import Foundation

// MARK: - Announcement Handle

/// Opaque namespace announcement identifier.
public struct Announcement: Sendable, Hashable {
    internal let raw: moq_announcement_t

    internal init(_ raw: moq_announcement_t) {
        self.raw = raw
    }

    public var isValid: Bool {
        moq_announcement_is_valid(raw)
    }

    public static func == (lhs: Announcement, rhs: Announcement) -> Bool {
        moq_announcement_eq(lhs.raw, rhs.raw)
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(moq_announcement_hash(raw))
    }
}

// MARK: - Namespace Event Payloads

/// Receiver-side event: peer announced a namespace.
public struct NamespacePublishedInfo: Sendable, Equatable, Hashable {
    public let announcement: Announcement
    public let namespace: Namespace
}

/// Publisher-side event: peer rejected our namespace announcement.
public struct NamespaceRejectedInfo: Sendable, Equatable, Hashable {
    public let announcement: Announcement
    public let errorCode: UInt32
    public let reason: String
    public let canRetry: Bool
}
