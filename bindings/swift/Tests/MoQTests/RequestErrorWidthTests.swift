import Testing
import Foundation
import CMoQCore
@testable import MoQ

/*
 * Request error codes are FULL WIDTH on the wire.
 *
 * `moq_request_error_t` is uint64_t (core/include/moq/session.h). The Swift
 * carriers used to declare `errorCode: UInt32`, which did not merely lose the
 * top half -- it stopped the package compiling at all, because Swift refuses
 * the narrowing implicitly (bindings/swift/Sources/MoQ/Event.swift:84 and
 * :114). That compile failure was the whole `Swift Binding` CI job.
 *
 * Widening the carriers fixes the build; these tests are what stop a future
 * "fix" from restoring the build with a narrowing conversion instead. They
 * drive the REAL `Event.from` conversion -- not the Swift structs directly --
 * because that function is where the imported C value crosses into Swift, and
 * it is the only place a cast could hide.
 *
 * Each value is chosen so a silent narrowing is visible rather than plausible:
 *
 *   0x1_0000_0001  truncates to 1              and clamps to 4294967295
 *   0x1_0000_0000  truncates to 0              and clamps to 4294967295
 *   UInt64.max     truncates to 4294967295     and clamps to 4294967295
 *
 * so truncation, clamping and a correct pass-through are three distinct
 * observations. Each test also asserts its own input really is above
 * UInt32.max, so none of them can quietly decay into a 32-bit test.
 *
 * The synthetic events own nothing: `moq_event_cleanup` is documented and
 * implemented as a no-op for every kind except OBJECT_RECEIVED, FETCH_OBJECT
 * and OBJECT_CHUNK (core/src/session/session.c), so `Event.from`'s single
 * cleanup call has nothing to release and cannot double-free. The reason bytes
 * stay owned by the caller for the duration of the call, which is all the
 * conversion needs -- it copies them into a Swift String.
 */
@Suite("Request error width")
struct RequestErrorWidthTests {

    private static let aboveUInt32: [UInt64] = [
        0x1_0000_0001,
        0x1_0000_0000,
        UInt64.max,
    ]

    /// Build a SUBSCRIBE_ERROR C event and run the real conversion.
    private func subscribeError(code: UInt64, reason: String?) -> Event {
        var ev = moq_event_t()
        ev.kind = MOQ_EVENT_SUBSCRIBE_ERROR
        ev.u.subscribe_error.sub = moq_subscription_t()
        ev.u.subscribe_error.error_code = code
        ev.u.subscribe_error.can_retry = true
        guard let reason else {
            // Empty reason: NULL data, zero length -- the conversion must take
            // its empty-string branch rather than dereferencing.
            return Event.from(&ev)
        }
        var bytes = Array(reason.utf8)
        return bytes.withUnsafeMutableBufferPointer { buf -> Event in
            ev.u.subscribe_error.reason = moq_bytes_t(
                data: buf.baseAddress, len: buf.count)
            return Event.from(&ev)
        }
    }

    /// Build a NAMESPACE_REJECTED C event and run the real conversion.
    private func namespaceRejected(code: UInt64, reason: String?) -> Event {
        var ev = moq_event_t()
        ev.kind = MOQ_EVENT_NAMESPACE_REJECTED
        ev.u.namespace_rejected.ann = moq_announcement_t()
        ev.u.namespace_rejected.error_code = code
        ev.u.namespace_rejected.can_retry = false
        guard let reason else {
            return Event.from(&ev)
        }
        var bytes = Array(reason.utf8)
        return bytes.withUnsafeMutableBufferPointer { buf -> Event in
            ev.u.namespace_rejected.reason = moq_bytes_t(
                data: buf.baseAddress, len: buf.count)
            return Event.from(&ev)
        }
    }

    @Test("SUBSCRIBE_ERROR preserves a code above UInt32.max exactly")
    func subscribeErrorFullWidth() throws {
        for code in Self.aboveUInt32 {
            #expect(code > UInt64(UInt32.max),
                    "fixture must exceed 32 bits to discriminate")

            guard case .subscribeError(let info) =
                subscribeError(code: code, reason: "denied") else {
                Issue.record("expected .subscribeError for code \(code)")
                return
            }
            #expect(info.errorCode == code)
            // Named so a failure says WHICH narrowing happened.
            #expect(info.errorCode != UInt64(truncatingIfNeeded: code) ||
                    code == UInt64(truncatingIfNeeded: code),
                    "errorCode was truncated to 32 bits")
            #expect(info.errorCode != UInt64(UInt32.max) || code == UInt64(UInt32.max),
                    "errorCode was clamped to UInt32.max")
            #expect(info.reason == "denied")
            #expect(info.canRetry)
        }
    }

    @Test("NAMESPACE_REJECTED preserves a code above UInt32.max exactly")
    func namespaceRejectedFullWidth() throws {
        for code in Self.aboveUInt32 {
            #expect(code > UInt64(UInt32.max),
                    "fixture must exceed 32 bits to discriminate")

            guard case .namespaceRejected(let info) =
                namespaceRejected(code: code, reason: "no such namespace") else {
                Issue.record("expected .namespaceRejected for code \(code)")
                return
            }
            #expect(info.errorCode == code)
            #expect(info.errorCode != UInt64(truncatingIfNeeded: code) ||
                    code == UInt64(truncatingIfNeeded: code),
                    "errorCode was truncated to 32 bits")
            #expect(info.errorCode != UInt64(UInt32.max) || code == UInt64(UInt32.max),
                    "errorCode was clamped to UInt32.max")
            #expect(info.reason == "no such namespace")
            #expect(!info.canRetry)
        }
    }

    @Test("An empty reason converts without dereferencing, code still exact")
    func emptyReasonKeepsFullWidth() throws {
        let code: UInt64 = 0x1_0000_0001

        guard case .subscribeError(let se) =
            subscribeError(code: code, reason: nil) else {
            Issue.record("expected .subscribeError")
            return
        }
        #expect(se.errorCode == code)
        #expect(se.reason.isEmpty)

        guard case .namespaceRejected(let nr) =
            namespaceRejected(code: code, reason: nil) else {
            Issue.record("expected .namespaceRejected")
            return
        }
        #expect(nr.errorCode == code)
        #expect(nr.reason.isEmpty)
    }

    @Test("The carriers are declared full width, not merely assigned to")
    func carriersAreUInt64() throws {
        // A compile-time statement of the contract: if either carrier were
        // narrowed again, these bindings would not type-check.
        guard case .subscribeError(let se) =
            subscribeError(code: UInt64.max, reason: nil),
              case .namespaceRejected(let nr) =
            namespaceRejected(code: UInt64.max, reason: nil) else {
            Issue.record("conversion did not produce the expected cases")
            return
        }
        let seCode: UInt64 = se.errorCode
        let nrCode: UInt64 = nr.errorCode
        #expect(seCode == UInt64.max)
        #expect(nrCode == UInt64.max)
    }
}
