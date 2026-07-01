import Foundation
import MoQ
import MoQMedia
import MoQTransport
import MoQRecvArgs

func writeStderr(_ msg: String) {
    FileHandle.standardError.write(Data((msg + "\n").utf8))
}

final class ReceiverState: @unchecked Sendable {
    private let lock = NSLock()
    private var _objectCount: UInt64 = 0
    private var _commandCount: UInt64 = 0
    private var _error: String?
    private var _done: Bool = false

    func addObjects(_ n: UInt64) { lock.withLock { _objectCount += n } }
    func addCommands(_ n: UInt64) { lock.withLock { _commandCount += n } }
    func setError(_ e: String) { lock.withLock { _error = e; _done = true } }
    func setDone() { lock.withLock { _done = true } }
    var isDone: Bool { lock.withLock { _done } }
    var error: String? { lock.withLock { _error } }
    var objectCount: UInt64 { lock.withLock { _objectCount } }
    var commandCount: UInt64 { lock.withLock { _commandCount } }
}

enum LiveError: Error, CustomStringConvertible {
    case catalogMissing
    case catalogMalformed(String)
    case initDataMalformed(String, String)
    var description: String {
        switch self {
        case .catalogMissing: return "Catalog object had no payload"
        case .catalogMalformed(let e): return "Catalog JSON malformed: \(e)"
        case .initDataMalformed(let t, let e): return "Track '\(t)' initData malformed: \(e)"
        }
    }
}

func runLive(args: RecvArgs) throws {
    setbuf(stdout, nil)
    setbuf(stderr, nil)

    let relay = try args.parseRelay()
    let ns = Namespace(args.namespaceParts.map { NamespacePart($0) })

    print("moq-swift-recv-live connecting to \(relay.host):\(relay.port)")
    print("  namespace: \(ns)")
    print("  catalog:   \(args.catalogTrack)")
    if args.tlsDisableVerify { print("  TLS:       skip verify") }
    if args.maxObjects > 0 { print("  limit:     \(args.maxObjects) objects") }
    if args.maxSeconds > 0 { print("  timeout:   \(args.maxSeconds)s") }
    print()

    let state = ReceiverState()
    let startTime = DispatchTime.now()

    // Network-thread-only state.
    var session: Session?
    var subscriber: Subscriber?
    var pipeline: PlaybackPipeline?
    var catalogTrackHandle: SubscribedTrack?
    var catalogReceived = false
    var router = TrackRouter()

    let conn = try ThreadedConnection(
        host: relay.host,
        port: relay.port,
        insecureSkipVerify: args.tlsDisableVerify,
        onPump: { ctx in
            do {
                guard let sess = ctx.borrowSession() else { return 0 }

                // Store borrowed session for Subscriber's lifetime.
                if session == nil { session = sess }

                // Wait for MoQ setup handshake.
                if sess.state != .established && subscriber == nil {
                    return 0
                }

                // Lazy init after established.
                if subscriber == nil {
                    if args.verbose { print("  [pump] session established") }
                    subscriber = try Subscriber(session: sess)
                    var pbCfg = PlaybackConfiguration.liveCMAF
                    pbCfg.gapTimeoutUS = args.bufferMS * 1000
                    pipeline = try PlaybackPipeline(configuration: pbCfg)

                    let catTrack = try subscriber!.subscribe(
                        track: TrackName(namespace: ns, name: args.catalogTrack))
                    catalogTrackHandle = catTrack
                    if args.verbose { print("  [pump] subscribed to catalog") }
                }

                guard let sub = subscriber, let pb = pipeline else { return 0 }

                try sub.tick(now: ctx.nowUS)

                // Check limits.
                if args.maxObjects > 0 && state.objectCount >= args.maxObjects {
                    state.setDone()
                    return 1
                }

                while let obj = try sub.pollObject() {
                    // Route catalog objects by track identity.
                    if obj.track === catalogTrackHandle && !catalogReceived {
                        guard let payload = obj.payload else {
                            throw LiveError.catalogMissing
                        }
                        let catalog: MSFCatalog
                        do {
                            catalog = try JSONDecoder().decode(
                                MSFCatalog.self, from: payload.copyBytes())
                        } catch {
                            throw LiveError.catalogMalformed("\(error)")
                        }
                        catalogReceived = true
                        print("  CATALOG: \(catalog.tracks.count) track(s)")
                        for t in catalog.tracks {
                            print("    - \(t.name) [\(t.packaging)] \(t.role ?? "?") \(t.codec ?? "?")")
                        }

                        for msfTrack in catalog.tracks {
                            do {
                                let desc = try msfTrack.playbackDescriptor()
                                let pbTrack = try pb.addTrack(desc.configuration)
                                let subTrack = try sub.subscribe(
                                    track: TrackName(namespace: ns, name: msfTrack.name))
                                router.add(subscribed: subTrack, playback: pbTrack)
                                print("  SUBSCRIBED: \(msfTrack.name)")
                            } catch {
                                if args.verbose {
                                    print("  [skip] \(msfTrack.name): \(error)")
                                }
                            }
                        }
                        print()
                        continue
                    }

                    if try router.pushIfRouted(object: obj, pipeline: pb, now: ctx.nowUS) {
                        state.addObjects(1)
                    }
                }

                try pb.tick(now: ctx.nowUS)

                var cmdCount: UInt64 = 0
                try pb.drainCommands { cmd in
                    cmdCount += 1
                    switch cmd {
                    case .configureVideo(_, _, _, let w, let h):
                        print("  CONFIGURE_VIDEO \(cmd.codecString ?? "?") \(w)x\(h)")
                    case .configureAudio(_, _, _, let sr, let ch):
                        print("  CONFIGURE_AUDIO \(cmd.codecString ?? "?") \(sr)Hz \(ch)ch")
                    case .decodeCMAF(_, let g, let o, let dts, _, let pts, _, _, let kf, _, _, _):
                        print("  DECODE_CMAF  g=\(g)/\(o) dts=\(dts) pts=\(pts) kf=\(kf) \(cmd.mediaData?.count ?? 0)B")
                    case .decodeVideo(_, let g, let o, let dts, _, let pts, _, _, let kf, _):
                        print("  DECODE_VIDEO g=\(g)/\(o) dts=\(dts) pts=\(pts) kf=\(kf) \(cmd.mediaData?.count ?? 0)B")
                    case .decodeAudio(_, let g, let o, let dts, _, _, _, _, _, _, _):
                        print("  DECODE_AUDIO g=\(g)/\(o) dts=\(dts) \(cmd.mediaData?.count ?? 0)B")
                    case .reset(_, let reason):
                        print("  RESET \(reason)")
                    case .unknown(let k):
                        print("  UNKNOWN_CMD kind=\(k)")
                    }
                }
                state.addCommands(cmdCount)

                try pb.drainEvents { evt in
                    switch evt {
                    case .gapDetected(_, let g):
                        print("  EVENT: GAP group=\(g)")
                    case .keyframeWaiting:
                        print("  EVENT: KEYFRAME_WAIT")
                    case .objectDropped(_, let r, let g, let o):
                        print("  EVENT: DROPPED g=\(g)/\(o) \(r)")
                    case .skipForward(_, let from, let to):
                        print("  EVENT: SKIP \(from)->\(to)")
                    case .trackEnded:
                        print("  EVENT: TRACK_ENDED")
                    case .backlogShed(_, let d, _):
                        print("  EVENT: BACKLOG_SHED dropped=\(d)")
                    case .partialGroupAbandoned(_, let f, let t):
                        print("  EVENT: PARTIAL_ABANDONED \(f)->\(t)")
                    case .unknown(let k):
                        print("  EVENT: UNKNOWN kind=\(k)")
                    }
                }

                // Check limits after processing.
                if args.maxObjects > 0 && state.objectCount >= args.maxObjects {
                    state.setDone()
                    return 1
                }

            } catch let e as MoQError where e == .wouldBlock || e == .requestBlocked {
                return 0
            } catch {
                state.setError("\(error)")
                return 1
            }
            return 0
        }
    )

    print("  Connected, entering wait loop...")
    print()

    while !state.isDone {
        // Check time limit.
        if args.maxSeconds > 0 {
            let elapsed = DispatchTime.now().uptimeNanoseconds - startTime.uptimeNanoseconds
            if elapsed >= args.maxSeconds * 1_000_000_000 {
                print("\n  Time limit reached (\(args.maxSeconds)s)")
                break
            }
        }

        do {
            try conn.wait(timeoutUS: 500_000)
        } catch let e as MoQError {
            if case .sessionClosed = e {
                print("\n  Session closed")
                break
            }
            writeStderr("Wait error: \(e)")
            break
        }

        if conn.isFatal {
            writeStderr("Fatal transport error")
            break
        }
    }

    if let err = state.error {
        writeStderr("Pump error: \(err)")
    }

    try? conn.stop()
    subscriber = nil
    pipeline = nil
    session = nil

    let limitNote = args.maxObjects > 0 ? " (limit: \(args.maxObjects))" : ""
    print("\nReceived \(state.objectCount) objects, \(state.commandCount) commands\(limitNote)")
}
