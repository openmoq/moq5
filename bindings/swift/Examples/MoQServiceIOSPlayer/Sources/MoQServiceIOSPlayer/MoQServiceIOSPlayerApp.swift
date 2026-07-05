/*
 * MoQServiceIOSPlayer — a minimal SwiftUI iOS player skeleton over the
 * MoQService binary lane (LibMoQ.xcframework + OpenSSL.xcframework).
 *
 * Purpose: prove the app-level architecture end to end —
 *   bookmark/input -> connect -> receiver attach -> firstVideoTrack ->
 *   sample-buffer loop -> AVSampleBufferDisplayLayer
 * — not to be a polished player. The connection runs behind a Connect
 * action; CI builds and links this for the iOS simulator but never runs it
 * (no network). Links ONLY the MoQService product (binary exclusivity).
 *
 * The package's iOS-16 deployment floor (see Package.swift) matches
 * MoQService's availability, so the app types need no per-declaration
 * @available (and @main stays clean).
 */

import SwiftUI

@main
struct MoQServiceIOSPlayerApp: App {
    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}
