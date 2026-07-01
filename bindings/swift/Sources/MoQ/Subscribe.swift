import CMoQCore
import Foundation

// MARK: - Subscription Handle

/// Opaque subscription identifier.
public struct Subscription: Sendable, Hashable {
    internal let raw: moq_subscription_t

    internal init(_ raw: moq_subscription_t) {
        self.raw = raw
    }

    public var isValid: Bool {
        moq_subscription_is_valid(raw)
    }

    public static func == (lhs: Subscription, rhs: Subscription) -> Bool {
        moq_subscription_eq(lhs.raw, rhs.raw)
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(moq_subscription_hash(raw))
    }
}

// MARK: - Group Order

public enum GroupOrder: Sendable, Hashable {
    case `default`
    case ascending
    case descending
    case unknown(UInt32)

    internal init(_ raw: moq_group_order_t) {
        switch raw {
        case MOQ_GROUP_ORDER_DEFAULT:    self = .default
        case MOQ_GROUP_ORDER_ASCENDING:  self = .ascending
        case MOQ_GROUP_ORDER_DESCENDING: self = .descending
        default:                         self = .unknown(raw)
        }
    }

    internal var cValue: moq_group_order_t {
        switch self {
        case .default:        return MOQ_GROUP_ORDER_DEFAULT
        case .ascending:      return MOQ_GROUP_ORDER_ASCENDING
        case .descending:     return MOQ_GROUP_ORDER_DESCENDING
        case .unknown(let v): return v
        }
    }
}

// MARK: - Subscribe Filter

public enum SubscribeFilter: Sendable, Hashable {
    case nextGroup
    case latestObject
    case absoluteStart(group: UInt64, object: UInt64)
    case absoluteRange(startGroup: UInt64, startObject: UInt64, endGroup: UInt64)
    case unknown(UInt32)

    internal static func fromC(
        _ raw: moq_subscribe_filter_t,
        startGroup: UInt64, startObject: UInt64, endGroup: UInt64
    ) -> SubscribeFilter {
        switch raw {
        case MOQ_SUBSCRIBE_FILTER_NEXT_GROUP:     return .nextGroup
        case MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT: return .latestObject
        case MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START:
            return .absoluteStart(group: startGroup, object: startObject)
        case MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE:
            return .absoluteRange(
                startGroup: startGroup, startObject: startObject,
                endGroup: endGroup)
        default: return .unknown(raw)
        }
    }
}

// MARK: - Subscribe Configuration

public struct SubscribeConfiguration: Sendable {
    public var track: TrackName
    public var filter: SubscribeFilter
    public var subscriberPriority: UInt8?
    public var groupOrder: GroupOrder

    public init(track: TrackName, filter: SubscribeFilter = .nextGroup) {
        self.track = track
        self.filter = filter
        self.subscriberPriority = nil
        self.groupOrder = .default
    }
}

// MARK: - Accept Subscribe Configuration

public struct AcceptSubscribeConfiguration: Sendable {
    public var expiresMs: UInt64?

    public init() {
        self.expiresMs = nil
    }
}

// MARK: - Subscribe Event Payloads

public struct SubscribeRequest: Sendable, Equatable, Hashable {
    public let subscription: Subscription
    public let track: TrackName
    public let filter: SubscribeFilter
    public let subscriberPriority: UInt8
    public let groupOrder: GroupOrder
}

public struct SubscribeOkInfo: Sendable, Equatable, Hashable {
    public let subscription: Subscription
    public let expiresMs: UInt64?
}

public struct SubscribeErrorInfo: Sendable, Equatable, Hashable {
    public let subscription: Subscription
    public let errorCode: UInt32
    public let reason: String
    public let canRetry: Bool
}

// MARK: - C Interop Helpers

extension Namespace {
    internal static func fromC(_ ns: moq_namespace_t) -> Namespace {
        var parts: [NamespacePart] = []
        if let partsPtr = ns.parts {
            for i in 0..<ns.count {
                let part = partsPtr[i]
                if let data = part.data {
                    parts.append(NamespacePart(Data(bytes: data, count: part.len)))
                } else {
                    parts.append(NamespacePart(Data()))
                }
            }
        }
        return Namespace(parts)
    }
}

extension TrackName {
    internal static func fromC(
        namespace ns: moq_namespace_t, name: moq_bytes_t
    ) -> TrackName {
        let namespace = Namespace.fromC(ns)
        let nameData: Data
        if let namePtr = name.data, name.len > 0 {
            nameData = Data(bytes: namePtr, count: name.len)
        } else {
            nameData = Data()
        }
        return TrackName(namespace: namespace, name: nameData)
    }
}

/// Pin a Namespace's bytes for a C call that borrows moq_namespace_t.
internal func withCNamespace<R>(
    _ namespace: Namespace,
    _ body: (moq_namespace_t) throws -> R
) rethrows -> R {
    let parts = namespace.parts
    var flat: [UInt8] = []
    var ranges: [(offset: Int, length: Int)] = []
    for part in parts {
        let offset = flat.count
        flat.append(contentsOf: part.bytes)
        ranges.append((offset: offset, length: part.bytes.count))
    }

    return try flat.withUnsafeBufferPointer { flatPtr in
        let base = flatPtr.baseAddress
        var cParts = ranges.map { range in
            moq_bytes_t(data: base.map { $0 + range.offset }, len: range.length)
        }

        return try cParts.withUnsafeMutableBufferPointer { cPartsPtr in
            let ns = moq_namespace_t(
                parts: cPartsPtr.baseAddress,
                count: cPartsPtr.count
            )
            return try body(ns)
        }
    }
}

/// Pin a TrackName's bytes for a C call that borrows namespace + name.
internal func withCTrackName<R>(
    _ track: TrackName,
    _ body: (moq_namespace_t, moq_bytes_t) throws -> R
) rethrows -> R {
    let nameBytes = [UInt8](track.name.bytes)
    return try nameBytes.withUnsafeBufferPointer { namePtr in
        let cName = moq_bytes_t(data: namePtr.baseAddress, len: namePtr.count)
        return try withCNamespace(track.namespace) { ns in
            try body(ns, cName)
        }
    }
}
