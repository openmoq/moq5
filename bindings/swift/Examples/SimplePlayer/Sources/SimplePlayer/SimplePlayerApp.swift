/*
 * SimplePlayer — the official MoQService example app.
 *
 * A universal SwiftUI player (iPhone / iPad / macOS) showing the complete
 * receive path on one screen:
 *
 *   configure -> connect -> established -> MediaReceiver.attach(.live) ->
 *   firstVideoTrack -> makeFormatDescription -> for-await objects ->
 *   makeSampleBuffer -> AVSampleBufferDisplayLayer
 *
 * Reading order: PlayerModel.swift (the state machine and playback loop),
 * ContentView.swift (how the UI renders that state), VideoSurface.swift
 * (the only platform-conditional file).
 *
 * The connection runs strictly behind the Connect button — CI builds and
 * links this app for both platforms but never runs it (no network).
 */

import SwiftUI

@main
struct SimplePlayerApp: App {
    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}
