import CMoQCore
import Foundation

// MARK: - Object Status

public enum ObjectStatus: Sendable, Hashable {
    case normal
    case endOfGroup
    case endOfTrack
    case unknown(UInt8)

    package init(_ raw: moq_object_status_t) {
        switch raw {
        case MOQ_OBJECT_NORMAL:       self = .normal
        case MOQ_OBJECT_END_OF_GROUP: self = .endOfGroup
        case MOQ_OBJECT_END_OF_TRACK: self = .endOfTrack
        default:                      self = .unknown(raw)
        }
    }

    package var cValue: moq_object_status_t {
        switch self {
        case .normal:       return MOQ_OBJECT_NORMAL
        case .endOfGroup:   return MOQ_OBJECT_END_OF_GROUP
        case .endOfTrack:   return MOQ_OBJECT_END_OF_TRACK
        case .unknown(let v): return v
        }
    }
}

// MARK: - Subgroup Handle

/// Opaque handle to an open subgroup data stream.
public struct SubgroupHandle: Sendable, Hashable {
    internal let raw: moq_subgroup_handle_t

    internal init(_ raw: moq_subgroup_handle_t) {
        self.raw = raw
    }

    public var isValid: Bool {
        moq_subgroup_is_valid(raw)
    }

    public static func == (lhs: SubgroupHandle, rhs: SubgroupHandle) -> Bool {
        moq_subgroup_eq(lhs.raw, rhs.raw)
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(moq_subgroup_hash(raw))
    }
}

// MARK: - Object Configuration

/// Configuration for writing an object with optional properties.
// Not Sendable: holds `Buffer` (non-atomic, shard-confined rcbuf).
public struct ObjectConfiguration {
    public var objectID: UInt64
    public var payload: Buffer
    public var properties: Buffer?

    public init(objectID: UInt64, payload: Buffer, properties: Buffer? = nil) {
        self.objectID = objectID
        self.payload = payload
        self.properties = properties
    }
}

// MARK: - Received Object

/// An object delivered to a subscriber.
// Not Sendable: holds `Buffer` (non-atomic, shard-confined rcbuf).
public struct ReceivedObject {
    public let subscription: Subscription
    public let groupID: UInt64
    public let subgroupID: UInt64
    public let objectID: UInt64
    public let publisherPriority: UInt8
    public let status: ObjectStatus
    public let endOfGroup: Bool
    public let isDatagram: Bool
    public let payload: Buffer?
    public let properties: Buffer?
}

extension ReceivedObject: Equatable {
    public static func == (lhs: ReceivedObject, rhs: ReceivedObject) -> Bool {
        lhs.subscription == rhs.subscription &&
        lhs.groupID == rhs.groupID &&
        lhs.subgroupID == rhs.subgroupID &&
        lhs.objectID == rhs.objectID &&
        lhs.publisherPriority == rhs.publisherPriority &&
        lhs.status == rhs.status &&
        lhs.endOfGroup == rhs.endOfGroup &&
        lhs.isDatagram == rhs.isDatagram &&
        lhs.payload === rhs.payload &&
        lhs.properties === rhs.properties
    }
}
