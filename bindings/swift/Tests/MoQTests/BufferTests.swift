import Testing
import Foundation
@testable import MoQ

@Suite("Buffer")
struct BufferTests {
    @Test("Data round-trip")
    func dataRoundTrip() throws {
        let original = Data([0xDE, 0xAD, 0xBE, 0xEF])
        let buf = try Buffer(original)

        #expect(buf.count == 4)
        #expect(!buf.isEmpty)

        let copied = buf.copyBytes()
        #expect(copied == original)

        buf.withUnsafeBytes { bytes in
            #expect(bytes.count == 4)
            #expect(bytes[0] == 0xDE)
            #expect(bytes[3] == 0xEF)
        }
    }

    @Test("Empty data")
    func emptyData() throws {
        let buf = try Buffer(Data())
        #expect(buf.count == 0)
        #expect(buf.isEmpty)
        #expect(buf.copyBytes() == Data())
    }

    @Test("Byte sequence init")
    func byteSequence() throws {
        let buf = try Buffer([0x01, 0x02, 0x03] as [UInt8])
        #expect(buf.count == 3)
        let data = buf.copyBytes()
        #expect(data == Data([0x01, 0x02, 0x03]))
    }

    @Test("Description")
    func description() throws {
        let buf = try Buffer(Data([1, 2, 3, 4, 5]))
        #expect(buf.description == "5 bytes")
    }

    @Test("clone preserves bytes")
    func clonePreservesBytes() throws {
        let original = Data([0x01, 0x02, 0x03, 0x04, 0x05])
        let buf = try Buffer(original)
        let cloned = try buf.clone()

        #expect(cloned.count == buf.count)
        #expect(cloned.copyBytes() == original)
    }

    @Test("clone of empty buffer")
    func cloneEmpty() throws {
        let buf = try Buffer(Data())
        let cloned = try buf.clone()
        #expect(cloned.isEmpty)
        #expect(cloned.copyBytes() == Data())
    }

    @Test("clone is an independent buffer with independent storage")
    func cloneIsIndependent() throws {
        let buf = try Buffer(Data([0xAA, 0xBB, 0xCC]))
        let cloned = try buf.clone()

        // Distinct Swift objects.
        #expect(cloned !== buf)

        // Distinct underlying rcbuf storage: clone() copies into a fresh
        // rcbuf rather than increfing/sharing the original's. The visible
        // byte pointers must therefore differ.
        let origPtr = buf.withUnsafeBytes { $0.baseAddress }
        let clonePtr = cloned.withUnsafeBytes { $0.baseAddress }
        #expect(origPtr != clonePtr)

        // Both remain readable and equal in content after the original is
        // released — the clone shares no refcount with it.
        let expected = buf.copyBytes()
        // (buf goes out of scope at end of test; clone stays valid.)
        #expect(cloned.copyBytes() == expected)
    }

    // Concurrency contract (compile-time, not a runtime assertion):
    //
    // `Buffer` is intentionally NOT `Sendable` — it owns a `moq_rcbuf_t`
    // whose refcount is non-atomic and shard/executor-confined (see
    // core/include/moq/rcbuf.h). Uncommenting the following would fail to
    // compile, which is the contract:
    //
    //     func requiresSendable<T: Sendable>(_ value: T) {}
    //     let buf = try Buffer(Data([0x00]))
    //     requiresSendable(buf)        // error: Buffer is not Sendable
    //     requiresSendable(try buf.clone())  // error: clone() is also a Buffer
    //
    // `clone()` produces independent storage but the result is STILL a
    // (non-Sendable) `Buffer` — it does not enable crossing Swift
    // concurrency domains. `Data` (from `copyBytes()`) IS `Sendable` and
    // is the supported cross-domain handoff type.
    @Test("Data is the Sendable handoff type")
    func dataIsSendable() throws {
        func requiresSendable<T: Sendable>(_ value: T) -> T { value }
        let buf = try Buffer(Data([0x10, 0x20]))
        let sendable = requiresSendable(buf.copyBytes())  // Data: Sendable
        #expect(sendable == Data([0x10, 0x20]))
    }
}
