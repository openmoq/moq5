import Testing
import CMoQCore
@testable import MoQ

@Suite("Outgoing subgroup terminal observations")
struct OutgoingSubgroupTerminalTests {
    private func event(publication: Bool, cause: UInt32, code: UInt64) -> moq_event_t {
        var native = moq_event_t()
        native.kind = MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
        native.detail_size = UInt32(MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE)
        var d = moq_outgoing_subgroup_terminated_event_t()
        d.subgroup._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP, 17, 3, 9)
        if publication {
            d.pub._opaque = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION, 17, 5, 11)
        } else {
            d.sub._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, 17, 7, 13)
        }
        d.group_id = .max
        d.subgroup_id = .max - 1
        d.stream_ref = moq_stream_ref_from_u64(.max - 2)
        d.error_code = code
        d.cause = cause
        native.u.outgoing_subgroup_terminated = d
        return native
    }

    @Test("Copies both owner forms, zero/full-width codes and unknown causes")
    func copiedValues() {
        let causes: [(UInt32, OutgoingSubgroupTerminalCause)] = [
            (MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING, .peerStopSending),
            (MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT, .deliveryTimeout),
            (.max, .unknown(.max))
        ]
        for publication in [false, true] {
            for (cause, expectedCause) in causes {
                for code in [UInt64(0), .max] {
                    var native = event(publication: publication, cause: cause, code: code)
                    let expected = native.u.outgoing_subgroup_terminated
                    guard case .outgoingSubgroupTerminated(let kept) = ExtendedEvent.from(&native) else {
                        Issue.record("Missing outgoing subgroup terminal")
                        continue
                    }
                    native = moq_event_t()
                    #expect(kept.subgroup == SubgroupHandle(expected.subgroup))
                    #expect(kept.subscription == (publication ? nil : Subscription(expected.sub)))
                    #expect(kept.publication == (publication ? Publication(expected.pub) : nil))
                    #expect(kept.groupID == UInt64.max && kept.subgroupID == UInt64.max - 1)
                    #expect(kept.stream.rawValue == UInt64.max - 2)
                    #expect(kept.errorCode == code)
                    #expect(kept.cause == expectedCause)
                    var legacy = event(publication: publication, cause: cause, code: code)
                    #expect(Event.from(&legacy) == .unknown(MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED))
                }
            }
        }
    }

    @Test("Every short detail is unknown; a larger producer preserves the prefix")
    func detailCoverage() {
        for prefix in 0..<UInt32(MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE) {
            var native = event(publication: false,
                               cause: MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING, code: 0)
            native.detail_size = prefix
            #expect(ExtendedEvent.from(&native) == .unknown(MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED))
        }
        var native = event(publication: false,
                           cause: MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT, code: 0)
        native.detail_size = .max
        guard case .outgoingSubgroupTerminated(let value) = ExtendedEvent.from(&native) else {
            Issue.record("Larger producer lost its known prefix")
            return
        }
        #expect(value.cause == .deliveryTimeout && value.errorCode == 0)
        native.kind = .max
        #expect(ExtendedEvent.from(&native) == .unknown(.max))
    }
}
