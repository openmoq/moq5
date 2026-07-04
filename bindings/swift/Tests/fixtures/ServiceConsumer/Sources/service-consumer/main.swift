import Foundation
import MoQService

/* Cold-build packaging smoke: exercise the public surface shape without
 * any I/O (this binary is built, never run in CI). Built for macOS via
 * pkg-config and for arm64-apple-ios*-simulator via the xcframework lane
 * (MOQ_SERVICE_IOS=1); the availability floor covers both. */
if #available(macOS 13.0, iOS 16.0, *) {
    var configuration = MoQEndpoint.Configuration(
        url: URL(string: "moqt://relay.example.com:4443")!)
    configuration.versions = .list([.draft18, .draft16])
    let endpoint = try MoQEndpoint.connect(configuration: configuration)
    let receiver = try MediaReceiver.attach(
        to: endpoint, configuration: .live(namespace: "live/cam1"))
    for try await event in receiver.trackEvents {
        if case .added(let track) = event, track.description.mediaType == .video {
            #if canImport(CoreMedia)
            /* Exercise the CoreMedia bridge surface (compile-only). */
            _ = try? track.description.makeFormatDescription()
            #endif
            try await receiver.subscribe(track, start: .nextGroup)
        }
        if case .catalogReady = event { break }
    }
    for try await object in receiver.objects {
        _ = object.mediaData
        break
    }
    await receiver.close()
    await endpoint.close()
}
