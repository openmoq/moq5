import CMoQCore
import Foundation

/// Owned, refcounted byte buffer.
///
/// `Buffer` owns exactly one `moq_rcbuf_t` reference for its lifetime;
/// `deinit` decrefs it once.
///
/// Concurrency: **not `Sendable`, by design.** The underlying
/// `moq_rcbuf_t` uses **non-atomic** refcounting and is
/// shard/executor-confined — the C core (`moq/rcbuf.h`) requires that
/// all references to a given rcbuf stay within one executor-affinity
/// domain (thread, executor, or run loop). A `Buffer` may also wrap an
/// rcbuf that the C session still holds a reference to, so sharing a
/// `Buffer` across concurrency domains can race the non-atomic refcount
/// on `deinit`. Keep a `Buffer` on the domain that created (or received)
/// it.
///
/// To move bytes to another Swift concurrency domain (actor/task), copy
/// them out as `Data` via ``copyBytes()`` — `Data` is `Sendable` — and
/// rebuild a `Buffer` on the destination domain. ``clone()`` gives an
/// independent rcbuf (own storage and lifetime) for same-domain
/// duplication or externally-synchronized ownership transfer, but the
/// clone is **also not `Sendable`** and does not by itself make a
/// `Buffer` safe to pass across concurrency domains.
public final class Buffer {
    package let raw: OpaquePointer  // moq_rcbuf_t*
    public let count: Int

    // MARK: - Internal init (adopt existing ref without incref)

    package init(adopting ptr: OpaquePointer) {
        self.raw = ptr
        self.count = moq_rcbuf_len(ptr)
    }

    // MARK: - Public initializers

    /// Create by copying a `Data` value.
    public init(_ data: Data) throws {
        var ptr: OpaquePointer?
        let rc: moq_result_t = data.withUnsafeBytes { bytes in
            let base = bytes.baseAddress?.assumingMemoryBound(to: UInt8.self)
            return moq_rcbuf_create(moq_alloc_default(), base, bytes.count, &ptr)
        }
        try MoQError.check(rc)
        guard let p = ptr else { throw MoQError.internal }
        self.raw = p
        self.count = moq_rcbuf_len(p)
    }

    /// Create by copying a byte sequence.
    public convenience init(_ bytes: some Sequence<UInt8>) throws {
        try self.init(Data(bytes))
    }

    deinit {
        moq_rcbuf_decref(raw)
    }

    // MARK: - Properties

    public var isEmpty: Bool { count == 0 }

    // MARK: - Access

    /// Scoped read-only access to the buffer's bytes.
    public func withUnsafeBytes<R>(
        _ body: (UnsafeRawBufferPointer) throws -> R
    ) rethrows -> R {
        let ptr = moq_rcbuf_data(raw)
        let buf = UnsafeRawBufferPointer(start: ptr, count: count)
        return try body(buf)
    }

    /// Copy the bytes out as `Data`.
    public func copyBytes() -> Data {
        withUnsafeBytes { Data($0) }
    }

    /// Copy a subrange of bytes as `Data`.
    public func sliceData(_ range: Range<Int>) -> Data {
        precondition(range.lowerBound >= 0 && range.upperBound <= count,
                     "Buffer.sliceData: range \(range) out of bounds (0..<\(count))")
        if range.isEmpty { return Data() }
        return withUnsafeBytes { buf in
            Data(bytes: buf.baseAddress! + range.lowerBound,
                 count: range.count)
        }
    }

    /// Make an independent copy backed by its own `moq_rcbuf_t`.
    ///
    /// The returned `Buffer` shares **nothing** with this one: it owns a
    /// freshly allocated rcbuf holding a copy of these bytes (via
    /// `moq_rcbuf_clone`), not an incref of this buffer's rcbuf — so its
    /// storage and lifetime are fully independent.
    ///
    /// Use it for same-domain duplication (an independent buffer whose
    /// lifetime is decoupled from this one), or for an ownership handoff
    /// only when the caller has an external protocol guaranteeing the
    /// clone stays single-domain with no concurrent rcbuf refcount
    /// access.
    ///
    /// `clone()` does **not** make the result safe to pass across Swift
    /// concurrency domains: the returned `Buffer` is still **not
    /// `Sendable`** (its rcbuf refcount is non-atomic). For a
    /// Swift-concurrency-safe handoff across actors/tasks, copy the bytes
    /// out as `Data` via ``copyBytes()`` (`Data` is `Sendable`) and
    /// rebuild a `Buffer` on the destination domain.
    public func clone() throws -> Buffer {
        var out: OpaquePointer?
        let rc = moq_rcbuf_clone(moq_alloc_default(), raw, &out)
        try MoQError.check(rc)
        guard let p = out else { throw MoQError.internal }
        return Buffer(adopting: p)
    }

    // MARK: - Internal

    /// Scoped access to the raw C handle. Not public.
    internal func withUnsafeRawHandle<R>(
        _ body: (OpaquePointer) throws -> R
    ) rethrows -> R {
        try body(raw)
    }
}

extension Buffer: CustomStringConvertible {
    public var description: String { "\(count) bytes" }
}
