import Foundation
import MoQService
#if canImport(CoreMedia)
import CoreMedia
#endif
#if canImport(AVFoundation)
import AVFoundation
#endif

/// The app-level playback state and loop. `@MainActor` so `@Published` UI
/// state and the display layer are touched only on the main thread; the
/// heavy work stays inside MoQService's own service thread, which the loop
/// merely awaits.
@MainActor
final class PlayerModel: ObservableObject {
    /// Connection input (a real app would persist these as bookmarks).
    @Published var urlText: String = "moqt://relay.example.com:4443"
    @Published var namespaceText: String = "live/cam1"

    /// Observable status for the UI.
    @Published private(set) var status: String = "Idle"
    @Published private(set) var sampleCount: Int = 0
    @Published private(set) var isRunning: Bool = false

    #if canImport(AVFoundation)
    /// The intended render target; wired into the SwiftUI view. Sample
    /// buffers are enqueued here on iOS 17+ (the pre-17 path counts only —
    /// this is a skeleton, not a shipping decoder).
    let displayLayer = AVSampleBufferDisplayLayer()
    #endif

    private var playbackTask: Task<Void, Never>?

    /// Generation token. Bumped on every connect()/disconnect() so a stale
    /// task that is still unwinding cannot clobber a newer connection's UI
    /// state (terminal clears, status). A task's writes apply only while its
    /// captured `run` is still current.
    private var runID: Int = 0

    func toggle() {
        isRunning ? disconnect() : connect()
    }

    func connect() {
        guard playbackTask == nil else { return }
        guard let url = URL(string: urlText) else {
            status = "Invalid URL"
            return
        }
        let namespace = MediaNamespace(parts: namespaceText
            .split(separator: "/", omittingEmptySubsequences: false)
            .map(String.init))
        runID &+= 1
        let run = runID
        isRunning = true
        sampleCount = 0
        status = "Connecting…"
        playbackTask = Task { [weak self] in
            await self?.play(run: run, url: url, namespace: namespace)
        }
    }

    func disconnect() {
        // Bump first: the in-flight task's captured `run` is now stale, so its
        // terminal writes below no-op even as it unwinds.
        runID &+= 1
        playbackTask?.cancel()
        playbackTask = nil
        isRunning = false
        status = "Stopped"
    }

    /// True only while `run` is the newest connect()/disconnect() generation.
    private func isCurrent(_ run: Int) -> Bool { run == runID }

    private func play(run: Int, url: URL, namespace: MediaNamespace) async {
        let outcome: String
        do {
            let endpoint = try MoQEndpoint.connect(to: url)
            outcome = await receive(
                run: run, endpoint: endpoint, namespace: namespace)
        } catch is CancellationError {
            outcome = "Cancelled"
        } catch {
            outcome = "Error: \(error)"
        }
        // Terminal UI state: apply only if this task is still current. A newer
        // connect()/disconnect() has already installed its own state.
        guard isCurrent(run) else { return }
        playbackTask = nil
        isRunning = false
        status = outcome
    }

    /// established -> receiver -> first video track -> format -> for-await
    /// objects -> sample buffers. The ~12-line MoQService shape. Owns the
    /// endpoint's teardown on every path and returns the terminal status.
    private func receive(
        run: Int, endpoint: MoQEndpoint, namespace: MediaNamespace
    ) async -> String {
        do {
            try await endpoint.established()
            // Bail before attaching if superseded/cancelled during the
            // establish await (the next cancellation-aware await is inside the
            // object loop, so guard the gap explicitly).
            guard isCurrent(run), !Task.isCancelled else {
                await endpoint.close()
                return "Cancelled"
            }
            status = "Established"

            let receiver = try MediaReceiver.attach(
                to: endpoint, configuration: .live(namespace: namespace))
            guard let video =
                try await receiver.trackEvents.firstVideoTrack() else {
                await receiver.close()
                await endpoint.close()
                return "Stream ended before a video track appeared"
            }
            if isCurrent(run) {
                status = "Video track \"\(video.description.name)\""
            }

            #if canImport(CoreMedia)
            let format = try video.description.makeFormatDescription()
            for try await object in receiver.objects
            where object.track === video {
                // Stop as soon as this task is superseded or cancelled; no
                // suspension follows before the state writes, so runID is
                // stable through the rest of the iteration.
                guard isCurrent(run), !Task.isCancelled else { break }
                let sample = try object.makeSampleBuffer(
                    formatDescription: format)
                enqueue(sample)
                sampleCount += 1
                status = "Playing — \(sampleCount) sample buffers"
            }
            #else
            for try await object in receiver.objects
            where object.track === video {
                guard isCurrent(run), !Task.isCancelled else { break }
                if !object.mediaData.isEmpty { sampleCount += 1 }
                status = "Receiving — \(sampleCount) objects"
            }
            #endif

            await receiver.close()
            await endpoint.close()
            return "Finished"
        } catch is CancellationError {
            await endpoint.close()
            return "Cancelled"
        } catch {
            await endpoint.close()
            return "Error: \(error)"
        }
    }

    #if canImport(CoreMedia) && canImport(AVFoundation)
    private func enqueue(_ sample: CMSampleBuffer) {
        // iOS 17 replaced direct-to-layer enqueue with the video renderer.
        // Gating on it keeps the skeleton free of the deprecated call; iOS 16
        // counts sample buffers without enqueuing.
        if #available(iOS 17.0, *) {
            let renderer = displayLayer.sampleBufferRenderer
            if renderer.isReadyForMoreMediaData {
                renderer.enqueue(sample)
            }
        }
    }
    #elseif canImport(CoreMedia)
    private func enqueue(_ sample: CMSampleBuffer) {}
    #endif
}
