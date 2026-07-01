import Testing
@testable import MoQ

@Suite("Session")
struct SessionTests {
    @Test("Create client session")
    func createClient() throws {
        let session = try Session(configuration: .init(perspective: .client))
        #expect(session.state == .idle)
    }

    @Test("Create server session")
    func createServer() throws {
        let session = try Session(configuration: .init(perspective: .server))
        #expect(session.state == .idle)
    }

    @Test("Client start transitions to setupSent")
    func clientStart() throws {
        let session = try Session(configuration: .init(perspective: .client))
        try session.start()
        #expect(session.state == .setupSent)
    }

    @Test("Destroy is clean")
    func destroyClean() throws {
        _ = try Session(configuration: .init(perspective: .client))
    }

    @Test("Borrowed session reads state without destroying")
    func borrowedSession() throws {
        let owned = try Session(configuration: .init(perspective: .client))
        let borrowed = Session(borrowing: owned.raw)
        #expect(borrowed.state == .idle)
        try borrowed.start()
        #expect(borrowed.state == .setupSent)
        #expect(owned.state == .setupSent)
        // borrowed goes out of scope — does NOT call moq_session_destroy.
        // owned still works after borrowed is deallocated.
    }
}
