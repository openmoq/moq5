import Testing
import Foundation
@testable import MoQ

@Suite("Action Draining")
struct ActionTests {
    @Test("Client start produces sendControl action")
    func clientStartAction() throws {
        let session = try Session(configuration: .init(perspective: .client))
        try session.start()

        var actions: [Action] = []
        session.drainActions { actions.append($0) }

        #expect(actions.count == 1)
        guard case .sendControl(let data) = actions.first else {
            Issue.record("Expected sendControl, got \(String(describing: actions.first))")
            return
        }
        // CLIENT_SETUP should be non-empty control bytes.
        #expect(!data.isEmpty)
    }

    @Test("pollActions returns same shape")
    func pollActionsConvenience() throws {
        let session = try Session(configuration: .init(perspective: .client))
        try session.start()

        let actions = session.pollActions()
        #expect(actions.count == 1)
        if case .sendControl(let data) = actions.first {
            #expect(!data.isEmpty)
        } else {
            Issue.record("Expected sendControl")
        }
    }

    @Test("Draining twice gives empty second result")
    func drainTwice() throws {
        let session = try Session(configuration: .init(perspective: .client))
        try session.start()

        let first = session.pollActions()
        #expect(first.count == 1)

        let second = session.pollActions()
        #expect(second.isEmpty)
    }

    @Test("Server session has no actions before start")
    func serverNoActions() throws {
        let session = try Session(configuration: .init(perspective: .server))
        let actions = session.pollActions()
        #expect(actions.isEmpty)
    }
}
