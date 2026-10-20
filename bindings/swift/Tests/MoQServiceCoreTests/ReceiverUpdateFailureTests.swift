import Testing
import Foundation
@testable import MoQServiceCore

@Suite("Receiver UPDATE failure model")
struct ReceiverUpdateFailureTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Local UPDATE failures preserve identity, result and FIFO without ending the endpoint")
    func updateFailureOrdering() async throws {
        let endpointBackend = ScriptedEndpointBackend()
        let backend = ScriptedReceiverBackend(latch: { endpointBackend.isLatched })
        let endpoint = MoQEndpoint(configuration: .init(url: URL(string: "moqt://relay.test:4443")!),
                                   engine: EndpointEngine(backend: endpointBackend))
        let receiver = try MediaReceiver.attach(to: endpoint,
            configuration: .live(namespace: "live/video"), backend: backend)
        backend.scriptEvents([
            .added(7, name: "video"),
            .init(kind: .updateLocalFailure(localResult: -5), handleID: 7, trackDescription: nil),
            .init(kind: .updateLocalFailure(localResult: nil), handleID: 7, trackDescription: nil),
            .catalogReady,
        ])
        let attachment = receiver.attachment
        _ = try await endpoint.engine.perform { attachment.drainTrackEvents() }
        #expect(attachment.stagedTrackEventCount == 4)
        var events = receiver.trackEventsExtended.makeAsyncIterator()
        guard case .legacy(.added(let track))? = try await events.next(),
              case .updateLocalFailure(let first, let result)? = try await events.next(),
              case .updateLocalFailure(let second, let absent)? = try await events.next(),
              case .legacy(.catalogReady)? = try await events.next() else {
            Issue.record("UPDATE failure order or kind changed")
            await receiver.close()
            await endpoint.close()
            return
        }
        #expect(track === first && first === second)
        #expect(result == -5 && absent == nil)
        #expect(!backend.isFatal && !backend.isTerminal)
        #expect(backend.snapshot().subscribeCalls.isEmpty && backend.snapshot().cancelFillCalls.isEmpty)
        await receiver.close()
        await endpoint.close()
        #expect(try await events.next() == nil)
        #expect(backend.snapshot().violations.isEmpty)
    }
}
