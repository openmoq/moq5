import Foundation
import MoQServiceCore

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension LiveMediaSession {

    /// Installed-mode convenience: wire the real transport factory
    /// (`MoQEndpoint.connect`) as the injected endpoint factory. Apps using
    /// the `MoQService` product get a ready-to-use ``LiveMediaSession`` with no
    /// factory boilerplate; `MoQServiceCore` stays free of this gated factory.
    public convenience init() {
        self.init(endpointFactory: { try MoQEndpoint.connect(configuration: $0) })
    }
}
