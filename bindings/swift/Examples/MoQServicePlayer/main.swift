/*
 * moq-service-player — the MoQService product-surface proof.
 *
 * Demonstrates the intended Apple call-site shape end to end: connect ->
 * established -> receiver -> first video track -> format description ->
 * for-await objects -> sample buffers. A CLI validator, not a GUI player:
 * it constructs CMSampleBuffers and reports progress (feeding a display
 * layer is app code).
 *
 * Usage: moq-service-player [url] [namespace] [object-count]
 *   e.g. moq-service-player moqt://127.0.0.1:4443 live/cam1 60
 *
 * Build-only in CI (needs a real relay to run); links ONLY the MoQService
 * product -- never the source-built MoQ stack (binary exclusivity).
 */

import Foundation
import MoQService
#if canImport(CoreMedia)
import CoreMedia
#endif

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
func run() async {
    let arguments = CommandLine.arguments
    guard let url = URL(string: arguments.count > 1
        ? arguments[1] : "moqt://127.0.0.1:4443") else {
        fputs("invalid URL\n", stderr)
        exit(2)
    }
    let namespaceText = arguments.count > 2 ? arguments[2] : "live/cam1"
    let namespace = MediaNamespace(parts: namespaceText
        .split(separator: "/", omittingEmptySubsequences: false)
        .map(String.init))
    let target = arguments.count > 3 ? (Int(arguments[3]) ?? 60) : 60

    do {
        /* The ~12-line shape. */
        let endpoint = try MoQEndpoint.connect(to: url)
        try await endpoint.established()
        print("established (\(endpoint.negotiatedVersion.map(String.init(describing:)) ?? "?"))")

        let receiver = try MediaReceiver.attach(
            to: endpoint, configuration: .live(namespace: namespace))
        guard let video = try await receiver.trackEvents.firstVideoTrack() else {
            fputs("stream ended before a video track appeared\n", stderr)
            await receiver.close()
            await endpoint.close()
            exit(1)
        }
        print("video track \"\(video.description.name)\" "
            + "codec=\(video.description.codec ?? "?")")

#if canImport(CoreMedia)
        let format = try video.description.makeFormatDescription()
        var delivered = 0
        for try await object in receiver.objects where object.track === video {
            let sample = try object.makeSampleBuffer(formatDescription: format)
            _ = sample                      /* app code would enqueue it */
            delivered += 1
            if delivered % 30 == 0 { print("\(delivered) sample buffers") }
            if delivered >= target { break }
        }
        print("done: \(delivered) sample buffers")
#else
        var delivered = 0
        for try await object in receiver.objects where object.track === video {
            delivered += object.mediaData.isEmpty ? 0 : 1
            if delivered >= target { break }
        }
        print("done: \(delivered) objects (no CoreMedia on this platform)")
#endif

        await receiver.close()
        await endpoint.close()
    } catch {
        fputs("error: \(error)\n", stderr)
        exit(1)
    }
}

if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *) {
    await run()
} else {
    fputs("requires macOS 13 / iOS 16 or newer\n", stderr)
    exit(2)
}
