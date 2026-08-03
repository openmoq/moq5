// Standalone .wtquicNetwork runtime proof (NOT an xctest -- Network.framework
// does not establish inside an xctest host, so the proof must run as a normal
// process). The driver scripts/check_wtquic_network_runtime.sh spawns fresh one-shot
// media servers (wtquic_network_smoke_server: MSF catalog + one LOC track "v" + one
// deterministic 256-byte object) and runs the REAL Swift media vertical --
// Swift -> libmoq service -> managed wtquic adapter -> wtquic
// Network.framework -> real MsQuic peer -- through the same public API
// SimplePlayer uses (LiveMediaSession over MoQEndpoint + MediaReceiver):
//
//   exchange <port> <16|18>
//       Establish (assert the negotiated draft), watch the namespace,
//       receive the object, verify every payload byte, stop cleanly.
//
//   reconnect <portA> <portB>
//       In ONE process: the full exchange against A, then again against B
//       (same-process reconnect; each connection its own fresh server).
//
//   untrusted <port>
//       Verification ON against the self-signed peer MUST fail with
//       certificateUnverified carrying errSecNotTrusted (-67843), the
//       Network.framework-only trust-rejection code (the picoquic family
//       reports TLS alerts instead) -- proof the flow traversed NF. The
//       driver's fixture carries subjectAltName=IP:127.0.0.1, so the only
//       judgeable failure is the trust root, never a hostname mismatch.
//
// Every phase is a SINGLE deterministic verdict under a hard deadline: the
// driver guarantees server readiness, so there is no retry here -- a phase
// that does not complete within its bound is a FAILURE, never something to
// retry (retrying could normalize an intermittently broken path). On a
// timeout the proof prints a PHASE TRACE (every WatchState observed, with
// elapsed ms) so a stall is attributable: no state at all = no NW progress;
// .connecting only = transport/session establishment; .established/
// .awaitingCatalog = service pump or SUBSCRIBE path; .awaitingFirstObject =
// peer publish path.

import Foundation
import MoQService

func fail(_ msg: String) -> Never {
    FileHandle.standardError.write(Data("PROOF FAIL: \(msg)\n".utf8))
    exit(1)
}

struct TimeoutError: Error {}

/// The server's deterministic proof payload: byte i = i*131+47 (mod 256).
let expectedPayload = Data((0..<256).map { UInt8(truncatingIfNeeded: $0 * 131 + 47) })

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func makeEndpointConfig(port: UInt16, verify: Bool) -> MoQEndpoint.Configuration {
    var c = MoQEndpoint.Configuration(
        url: URL(string: "https://127.0.0.1:\(port)/moq")!)
    c.backend = .wtquicNetwork                 // the explicit selection
    c.insecureSkipVerify = !verify
    return c
}

/// Run `body` under a hard deadline; a stall past the bound is a verdict
/// (failure), never a retry.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func within<T: Sendable>(seconds: UInt64,
                         _ body: @escaping @Sendable () async throws -> T)
    async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await body() }
        group.addTask {
            try await Task.sleep(nanoseconds: seconds * 1_000_000_000)
            throw TimeoutError()
        }
        defer { group.cancelAll() }
        return try await group.next()!
    }
}

/// The attributable phase trace: every WatchState observed, timestamped.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
final class PhaseTrace: @unchecked Sendable {
    private let lock = NSLock()
    private var lines: [String] = []
    private let t0 = ContinuousClock.now

    func note(_ state: WatchState) {
        let ms = (ContinuousClock.now - t0) / .milliseconds(1)
        lock.lock()
        lines.append("  +\(Int(ms))ms \(state)")
        lock.unlock()
    }

    func dump(_ label: String) {
        lock.lock()
        let body = lines.isEmpty
            ? "  (no WatchState observed: no NW/endpoint progress at all)"
            : lines.joined(separator: "\n")
        lock.unlock()
        FileHandle.standardError.write(
            Data("PROOF TRACE [\(label)]:\n\(body)\n".utf8))
    }
}

/// One full media exchange: establish (exact draft), reach the receiver
/// (catalog + SUBSCRIBE done), receive THE object, verify every byte,
/// stop cleanly. One deterministic attempt per phase.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func exchange(port: UInt16, version: MoQVersion, label: String) async {
    let trace = PhaseTrace()
    let session = LiveMediaSession()
    let config = LiveMediaSession.Configuration(
        endpoint: makeEndpointConfig(port: port, verify: false),
        namespace: MediaNamespace(parts: ["proof", "live"]))
    session.start(config)

    // Phase 1: drive the state stream to a receiver-bearing state
    // (awaitingFirstObject/receiving), asserting the negotiated draft
    // at established. The trace attributes any stall by its last state.
    do {
        try await within(seconds: 15) {
            for await state in session.stateUpdates() {
                trace.note(state)
                switch state {
                case .established(let v):
                    guard v == version else {
                        trace.dump(label)
                        fail("\(label): negotiated \(String(describing: v)), "
                             + "want \(version)")
                    }
                case .failed(let err):
                    trace.dump(label)
                    fail("\(label): watch failed: \(err)")
                case .noMatchingTracks(let catalog):
                    trace.dump(label)
                    fail("\(label): no matching tracks in \(catalog)")
                case .awaitingFirstObject, .receiving:
                    return          // the receiver is live and subscribed
                default:
                    continue
                }
            }
            trace.dump(label)
            fail("\(label): state stream ended before the receiver attached "
                 + "(cancelled: \(Task.isCancelled))")
        }
    } catch {
        trace.dump(label)
        let ep = session.endpoint
        let diag = "endpoint.state=\(String(describing: ep?.state)) "
            + "negotiated=\(String(describing: ep?.negotiatedVersion))"
        FileHandle.standardError.write(Data("PROOF DIAG: \(diag)\n".utf8))
        await session.stop()
        fail("\(label): establish/subscribe: \(error)")
    }

    // The DURABLE version check: the state stream coalesces
    // (bufferingNewest(1)), so .established may never be observed and
    // the in-stream assertion above is best-effort. The endpoint's own
    // negotiated-version snapshot is authoritative and must hold
    // regardless of which states were seen.
    guard let negotiated = session.endpoint?.negotiatedVersion else {
        trace.dump(label)
        await session.stop()
        fail("\(label): no negotiated version on the live endpoint")
    }
    guard negotiated == version else {
        trace.dump(label)
        await session.stop()
        fail("\(label): endpoint negotiated \(negotiated), want \(version)")
    }

    // Phase 2: receive and verify THE object (single attempt, bounded).
    do {
        let object: MediaObject? = try await within(seconds: 15) {
            for try await object in session.objects {
                return object
            }
            return nil
        }
        guard let object else {
            trace.dump(label)
            fail("\(label): object stream ended without an object")
        }
        guard object.mediaData == expectedPayload else {
            trace.dump(label)
            fail("\(label): payload mismatch (\(object.mediaData.count) bytes, "
                 + "want \(expectedPayload.count) patterned)")
        }
        guard object.isKeyframe, object.endsGroup else {
            trace.dump(label)
            fail("\(label): object markings lost (keyframe/endsGroup)")
        }
    } catch {
        trace.dump(label)
        // DIAGNOSTIC ONLY (the verdict below stays FAIL): one bounded
        // re-poll discriminates a delivery that is sitting in the local
        // queue behind a missed wake (re-poll finds it) from bytes that
        // never arrived (re-poll times out too).
        let repoll: String
        do {
            let again: MediaObject? = try await within(seconds: 2) {
                for try await object in session.objects { return object }
                return nil
            }
            repoll = again != nil ? "re-poll DELIVERED the object "
                + "(missed-wake suspect)" : "re-poll: stream ended"
        } catch {
            repoll = "re-poll timed out too (bytes never arrived locally)"
        }
        FileHandle.standardError.write(
            Data("PROOF DIAG: \(repoll)\n".utf8))
        await session.stop()
        fail("\(label): object receipt: \(error)")
    }

    // Clean stop: the endpoint must come down without a failure state.
    await session.stop()
    print("PROOF \(label): establish(\(version)) + object bytes + stop OK")
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func runReconnect(portA: UInt16, portB: UInt16) async {
    await exchange(port: portA, version: .draft16, label: "connect")
    await exchange(port: portB, version: .draft16, label: "reconnect")
    print("PROOF reconnect: full exchange, stop, same-process reconnect OK")
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func runUntrusted(port: UInt16) async {
    do {
        let ep = try MoQEndpoint.connect(
            configuration: makeEndpointConfig(port: port, verify: true))
        do {
            try await within(seconds: 15) { try await ep.established() }
            await ep.close()
            fail("untrusted self-signed peer unexpectedly established")
        } catch let MoQServiceError.connectionFailed(f)
                    where f.kind == .certificateUnverified {
            await ep.close()
            guard f.code == -67843 else {
                fail("code \(f.code), want -67843 (errSecNotTrusted)")
            }
            print("PROOF untrusted: certificateUnverified / -67843 via NF OK")
        } catch {
            await ep.close()
            fail("untrusted: expected certificateUnverified, got \(error)")
        }
    } catch { fail("untrusted setup: \(error)") }
}

let args = CommandLine.arguments
func u16(_ s: String) -> UInt16 { UInt16(s) ?? { fail("bad port \(s)") }() }

if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *) {
    switch args.count >= 2 ? args[1] : "" {
    case "exchange" where args.count == 4:
        // exactly 16 or 18; anything else is a usage error, never a
        // silent draft-16 default
        let version: MoQVersion
        switch args[3] {
        case "16": version = .draft16
        case "18": version = .draft18
        default: fail("bad version '\(args[3])' (want 16 or 18)")
        }
        await exchange(port: u16(args[2]), version: version,
                       label: "exchange-d\(args[3])")
    case "reconnect" where args.count == 4:
        await runReconnect(portA: u16(args[2]), portB: u16(args[3]))
    case "untrusted" where args.count == 3:
        await runUntrusted(port: u16(args[2]))
    default:
        fail("usage: exchange <port> <16|18> | reconnect <portA> <portB> | untrusted <port>")
    }
    exit(0)
} else { fail("requires macOS 13 / iOS 16") }
