import AppKit
import AVFoundation
import CoreMedia
import Foundation
import MoQ
import MoQMedia
import MoQTransport
import MoQRecvArgs

setbuf(stdout, nil); setbuf(stderr, nil)
func log(_ msg: String) { print("  \(msg)") }
func logErr(_ msg: String) {
    FileHandle.standardError.write(Data(("  ERROR: \(msg)\n").utf8))
}

// MARK: - Player Window

class PlayerView: NSView {
    let displayLayer = AVSampleBufferDisplayLayer()
    override init(frame: NSRect) {
        super.init(frame: frame)
        wantsLayer = true
        layer?.backgroundColor = NSColor.black.cgColor
        layer?.addSublayer(displayLayer)
        displayLayer.videoGravity = .resizeAspect
    }
    required init?(coder: NSCoder) { nil }
    override func layout() {
        super.layout()
        CATransaction.begin()
        CATransaction.setDisableActions(true)
        displayLayer.frame = bounds
        CATransaction.commit()
    }
}

// MARK: - Player State

final class PlayerState {
    var subscriber: Subscriber?
    var pipeline: PlaybackPipeline?
    var catalogTrack: SubscribedTrack?
    var router = TrackRouter()
    var catalogReceived = false

    var formatDesc: CMVideoFormatDescription?
    var audioDecoder = AudioDecoder()
    var videoTime = MediaTimeRebaser()
    var audioTime = MediaTimeRebaser()
    var frameCount: UInt64 = 0
    var syncHostTime: UInt64 = 0
    var timebaseStarted = false

    let displayLayer: AVSampleBufferDisplayLayer
    let videoTimebase: CMTimebase?

    init(displayLayer: AVSampleBufferDisplayLayer) {
        self.displayLayer = displayLayer
        self.videoTimebase = displayLayer.controlTimebase
    }

    func startTimelineIfNeeded() {
        if !timebaseStarted, let tb = videoTimebase {
            CMTimebaseSetTime(tb, time: .zero)
            CMTimebaseSetRate(tb, rate: 1.0)
            syncHostTime = mach_absolute_time()
            timebaseStarted = true
        }
    }

    func resetTimeline() {
        videoTime.reset(); audioTime.reset()
        syncHostTime = 0; timebaseStarted = false
        if let tb = videoTimebase { CMTimebaseSetRate(tb, rate: 0) }
        displayLayer.flush(); audioDecoder.flush()
    }
}

// MARK: - Main

func run() throws {
    let args = try RecvArgs.parse(CommandLine.arguments)
    guard !args.demo else {
        print("Use moq-swift-recv --demo instead."); exit(1)
    }
    let relay = try args.parseRelay()
    let ns = Namespace(args.namespaceParts.map { NamespacePart($0) })
    print("moq-swift-player \(relay.host):\(relay.port) namespace=\(ns)\n")

    let app = NSApplication.shared
    app.setActivationPolicy(.regular)
    let window = NSWindow(
        contentRect: NSRect(x: 0, y: 0, width: 640, height: 480),
        styleMask: [.titled, .closable, .resizable, .miniaturizable],
        backing: .buffered, defer: false)
    window.title = "MOQ5 Player — \(ns)"
    let playerView = PlayerView(frame: window.contentView!.bounds)
    playerView.autoresizingMask = [.width, .height]
    window.contentView?.addSubview(playerView)
    window.center()

    var timebase: CMTimebase?
    CMTimebaseCreateWithSourceClock(
        allocator: nil, sourceClock: CMClockGetHostTimeClock(),
        timebaseOut: &timebase)
    if let tb = timebase {
        playerView.displayLayer.controlTimebase = tb
        CMTimebaseSetRate(tb, rate: 0)
    }

    let state = PlayerState(displayLayer: playerView.displayLayer)

    let conn = try ThreadedConnection(
        host: relay.host, port: relay.port,
        insecureSkipVerify: args.tlsDisableVerify,
        onPump: { ctx in
            do {
                guard let sess = ctx.borrowSession() else { return 0 }

                if state.subscriber == nil {
                    if sess.state != .established { return 0 }
                    log("[pump] established")
                    state.subscriber = try Subscriber(session: sess)
                    var cfg = PlaybackConfiguration.liveCMAF
                    cfg.gapTimeoutUS = args.bufferMS * 1000
                    state.pipeline = try PlaybackPipeline(configuration: cfg)
                    state.catalogTrack = try state.subscriber!.subscribe(
                        track: TrackName(namespace: ns, name: args.catalogTrack))
                }
                guard let sub = state.subscriber,
                      let pb = state.pipeline else { return 0 }
                try sub.tick(now: ctx.nowUS)
                if args.maxObjects > 0 && state.frameCount >= args.maxObjects { return 1 }

                while let obj = try sub.pollObject() {
                    if obj.track === state.catalogTrack && !state.catalogReceived {
                        guard let payload = obj.payload else { continue }
                        let catalog = try JSONDecoder().decode(
                            MSFCatalog.self, from: payload.copyBytes())
                        state.catalogReceived = true
                        log("CATALOG: \(catalog.tracks.count) track(s)")

                        for candidate in catalog.selectTracks(matching: .h264Video) {
                            do {
                                let desc = try candidate.playbackDescriptor()
                                state.formatDesc = try createH264FormatDescription(
                                    avccData: desc.codecConfig)
                                let pbTrack = try pb.addTrack(desc.configuration)
                                let subTrack = try sub.subscribe(
                                    track: TrackName(namespace: ns, name: candidate.name))
                                state.router.add(subscribed: subTrack, playback: pbTrack)
                                let c = desc.configuration
                                log("  video: \(candidate.name) \(candidate.codec ?? "") \(c.width)x\(c.height)")
                                DispatchQueue.main.async {
                                    window.title = "MOQ5 — \(candidate.codec ?? "video") \(c.width)x\(c.height)"
                                }
                                break
                            } catch {
                                logErr("Skipping \(candidate.name): \(error)")
                            }
                        }

                        for candidate in catalog.selectTracks(matching: .aacAudio) {
                            do {
                                let desc = try candidate.playbackDescriptor()
                                let pbTrack = try pb.addTrack(desc.configuration)
                                let subTrack = try sub.subscribe(
                                    track: TrackName(namespace: ns, name: candidate.name))
                                state.router.add(subscribed: subTrack, playback: pbTrack)
                                log("  audio: \(candidate.name) \(candidate.codec ?? "")")
                                break
                            } catch {
                                logErr("Skipping \(candidate.name): \(error)")
                            }
                        }

                        for t in catalog.tracks where t.role == "video" {
                            if t.codec?.lowercased().hasPrefix("avc1") != true {
                                log("  Skipping \(t.name): \(t.codec ?? "?") not supported")
                            }
                        }
                        continue
                    }

                    try state.router.pushIfRouted(object: obj, pipeline: pb, now: ctx.nowUS)
                }

                try pb.tick(now: ctx.nowUS)

                try pb.drainCommands { cmd in
                    switch cmd {
                    case .configureVideo(_, _, _, let w, let h):
                        log("CONFIGURE_VIDEO \(cmd.codecString ?? "?") \(w)x\(h)")

                    case .configureAudio(_, _, _, let sr, let ch):
                        log("CONFIGURE_AUDIO \(cmd.codecString ?? "?") \(sr)Hz \(ch)ch")
                        state.audioDecoder.configure(
                            codec: cmd.codecString ?? "",
                            sampleRate: sr, channels: ch,
                            codecConfig: cmd.codecConfigData)

                    case .decodeCMAF(_, let g, let o, let dts, _, let pts,
                                     _, _, let kf, let dur, _, _):
                        guard let fd = state.formatDesc,
                              let data = cmd.mediaData else { break }
                        state.frameCount += 1
                        state.startTimelineIfNeeded()
                        let relDTS = state.videoTime.rebase(dts)
                        let relPTS = state.videoTime.rebase(pts)
                        if state.frameCount <= 5 || state.frameCount % 90 == 0 {
                            log("FRAME #\(state.frameCount) g=\(g)/\(o) dts=\(relDTS) kf=\(kf) \(data.count)B")
                        }
                        do {
                            let sb = try createSampleBuffer(
                                data: data, formatDescription: fd,
                                decodeTimeUS: relDTS, presentationTimeUS: relPTS,
                                durationUS: dur, isKeyframe: kf)
                            state.displayLayer.enqueue(sb)
                        } catch {
                            if state.frameCount <= 10 { logErr("Sample: \(error)") }
                        }

                    case .decodeVideo(_, let g, let o, let dts, _, let pts,
                                      _, _, let kf, _):
                        guard let fd = state.formatDesc,
                              let data = cmd.mediaData else { break }
                        state.frameCount += 1
                        state.startTimelineIfNeeded()
                        let relDTS = state.videoTime.rebase(dts)
                        let relPTS = state.videoTime.rebase(pts)
                        if state.frameCount <= 5 || state.frameCount % 90 == 0 {
                            log("FRAME #\(state.frameCount) g=\(g)/\(o) dts=\(relDTS) kf=\(kf) \(data.count)B [RAW]")
                        }
                        do {
                            let sb = try createSampleBuffer(
                                data: data, formatDescription: fd,
                                decodeTimeUS: relDTS, presentationTimeUS: relPTS,
                                durationUS: 0, isKeyframe: kf)
                            state.displayLayer.enqueue(sb)
                        } catch {
                            if state.frameCount <= 10 { logErr("Sample: \(error)") }
                        }

                    case .decodeAudio(_, _, _, let dts, _, _, _, _, _, _, _):
                        guard let data = cmd.mediaData else { break }
                        state.startTimelineIfNeeded()
                        state.audioDecoder.decode(
                            payload: data,
                            presentationTimeUS: state.audioTime.rebase(dts),
                            syncHostTime: Int64(state.syncHostTime))

                    case .reset(_, let reason):
                        log("RESET \(reason)")
                        state.resetTimeline()
                    default: break
                    }
                }

                try pb.drainEvents { evt in
                    switch evt {
                    case .objectDropped(_, let r, let g, let o):
                        if state.frameCount <= 200 { logErr("DROPPED g=\(g)/\(o) \(r)") }
                    case .gapDetected(_, let g): logErr("GAP group=\(g)")
                    case .keyframeWaiting: logErr("KEYFRAME_WAIT")
                    default: break
                    }
                }
                if args.maxObjects > 0 && state.frameCount >= args.maxObjects { return 1 }
            } catch let e as MoQError where e == .wouldBlock || e == .requestBlocked {
                return 0
            } catch {
                logErr("Pump: \(error)"); return 1
            }
            return 0
        }
    )

    let delegate = PlayerWindowDelegate()
    delegate.onClose = { try? conn.stop(); DispatchQueue.main.async { app.terminate(nil) } }
    window.delegate = delegate
    window.makeKeyAndOrderFront(nil)
    app.activate(ignoringOtherApps: true)
    log("Connected, rendering...\n")
    app.run()
}

class PlayerWindowDelegate: NSObject, NSWindowDelegate {
    var onClose: (() -> Void)?
    func windowWillClose(_ notification: Notification) { onClose?() }
}

do { try run() } catch let e as ArgError {
    if case .help = e { print(e.description); exit(0) }
    FileHandle.standardError.write(Data(("Error: \(e)\n").utf8)); exit(1)
} catch {
    FileHandle.standardError.write(Data(("Error: \(error)\n").utf8)); exit(1)
}
