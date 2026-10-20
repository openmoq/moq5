import Testing
import CMoQService
@testable import MoQService

@Suite("Receiver UPDATE failure bridge")
struct ReceiverUpdateFailureBridgeTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("UPDATE failures preserve signed results only through a complete field")
    func sizedResult() throws {
        let end = try #require(MemoryLayout<moq_media_track_event_t>.offset(of: \.local_result)) + 4
        let full = MemoryLayout<moq_media_track_event_t>.size
        let results: [Int32] = [-1, -5, -9, 0, .min, .max]
        for stamp in [0, 4, end - 4, end - 1, end, full, full + 64] {
            for result in results {
                var event = moq_media_track_event_t()
                event.struct_size = UInt32(stamp)
                event.kind = MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE
                event.track = OpaquePointer(bitPattern: 0x2000)
                event.local_result = result
                event.delivery_origin = MOQ_MEDIA_DELIVERY_FILL
                event.fill_error_code = .max
                event.parse_drops_total = .max
                let copy = try #require(copyReceiverTrackEvent(event))
                event.local_result = 42
                #expect(copy.handleID == 0x2000 && copy.trackDescription == nil)
                #expect(copy.parseDropClass == nil && copy.parseDropTotal == 0 && copy.parseDropDelta == 0)
                guard case .updateLocalFailure(let copied) = copy.kind else {
                    Issue.record("UPDATE admission failure was reclassified")
                    continue
                }
                #expect(copied == (stamp >= end ? result : nil))
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("UPDATE failure does not consume another kind's local-result bytes")
    func kindIsolation() {
        var event = moq_media_track_event_t()
        event.struct_size = UInt32(MemoryLayout<moq_media_track_event_t>.size)
        event.track = OpaquePointer(bitPattern: 0x2000)
        event.local_result = MOQ_ERR_WRONG_STATE
        event.kind = MOQ_MEDIA_TRACK_LOCAL_FAILURE
        guard case .localFailure? = copyReceiverTrackEvent(event)?.kind else {
            Issue.record("ACK failure changed")
            return
        }
        event.kind = MOQ_MEDIA_TRACK_ENDED
        guard case .ended? = copyReceiverTrackEvent(event)?.kind else {
            Issue.record("Peer end changed")
            return
        }
        event.kind = MOQ_MEDIA_TRACK_UPDATE_OK
        #expect(copyReceiverTrackEvent(event) == nil)
    }
}
