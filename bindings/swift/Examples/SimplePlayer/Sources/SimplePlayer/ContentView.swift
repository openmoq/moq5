import SwiftUI

/// One screen, cinematic: the video fills the surface on a dark canvas, a
/// small status pill floats top-left, and the connection controls live in a
/// translucent card that recedes while playing (tap the video to bring it
/// back). Every control still maps 1:1 to an SDK concept — the styling is the
/// only thing between this and the plain form it started as.
struct ContentView: View {
    @StateObject private var model = PlayerModel()

    /// Chrome is auto-hidden once playback starts; a tap on the video toggles
    /// it. When not playing, the controls are always shown (you need them to
    /// connect), so this only matters mid-stream.
    @State private var chromeVisible = true
    @State private var advancedExpanded = false

    private var isPlaying: Bool {
        if case .playing = model.state { return true }
        return false
    }
    private var showChrome: Bool { !isPlaying || chromeVisible }

    var body: some View {
        ZStack(alignment: .bottom) {
            videoLayer
            safeAreaChrome
        }
        .background(Color.black.ignoresSafeArea())
        .preferredColorScheme(.dark)
        #if os(macOS)
        .frame(minWidth: 480, minHeight: 660)
        #endif
        .animation(.easeInOut(duration: 0.28), value: showChrome)
        .onReceive(model.$state) { state in
            // Fade the chrome away the moment media starts; restore it on every
            // non-playing state so the connect card is never stranded off-screen.
            if case .playing = state { chromeVisible = false }
            else { chromeVisible = true }
        }
    }

    // -- Full-bleed video + centered state, tap toggles chrome ------------

    private var videoLayer: some View {
        ZStack {
            #if canImport(AVFoundation)
            VideoSurfaceView(layer: model.displayLayer)
            #else
            Color.black
            #endif
            stateOverlay
        }
        .ignoresSafeArea()
        .contentShape(Rectangle())
        .onTapGesture { if isPlaying { chromeVisible.toggle() } }
    }

    /// Calm, honest states drawn over the video. `waitingForMedia` is "between
    /// stations", never an alarm; only `failed` is red.
    @ViewBuilder
    private var stateOverlay: some View {
        switch model.state {
        case .idle:
            centerHint("Not connected",
                       systemImage: "antenna.radiowaves.left.and.right.slash")
        case .connecting:
            centerSpinner("Connecting…")
        case .established, .waitingForMedia:
            centerSpinner("Waiting for signal…")
        case .playing:
            EmptyView()  // the video is the interface
        case .ended:
            centerHint("Stream ended", systemImage: "stop.circle")
        case .failed(let message):
            failureCard(message)
        }
    }

    private func centerHint(_ text: String, systemImage: String) -> some View {
        VStack(spacing: 12) {
            Image(systemName: systemImage)
                .font(.system(size: 34, weight: .light))
            Text(text).font(.callout)
        }
        .foregroundStyle(.secondary)
        .allowsHitTesting(false)
    }

    private func centerSpinner(_ text: String) -> some View {
        VStack(spacing: 12) {
            ProgressView().controlSize(.large)
            Text(text).font(.callout).foregroundStyle(.secondary)
        }
        .allowsHitTesting(false)
    }

    private func failureCard(_ message: String) -> some View {
        VStack(spacing: 14) {
            Label("Connection failed", systemImage: "exclamationmark.triangle.fill")
                .font(.headline)
                .foregroundStyle(.red)
            Text(message)
                .font(.footnote)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
            Button("Retry") { model.connect() }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)
        }
        .padding(22)
        .frame(maxWidth: 320)
        .glassCard(cornerRadius: 20)
        .padding(40)
    }

    // -- Safe-area chrome: the control card (bottom), which recedes on play --

    private var safeAreaChrome: some View {
        VStack(spacing: 0) {
            Spacer(minLength: 0)
            if showChrome {
                controlCard
                    .transition(.move(edge: .bottom).combined(with: .opacity))
            }
        }
        .padding(16)
    }

    private var controlCard: some View {
        VStack(spacing: 12) {
            // Status lives in the card, not over the video: nothing overlays a
            // good movie while you watch — tap to reveal it with the controls.
            statusRow
            field(systemImage: "link", placeholder: "Relay URL (https:// or moqt://)",
                  text: $model.urlText)
            field(systemImage: "number", placeholder: "Namespace (e.g. live/cam1)",
                  text: $model.namespaceText)

            Button { model.toggle() } label: {
                Text(model.isRunning ? "Disconnect" : "Connect")
                    .font(.headline)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderedProminent)
            .controlSize(.large)
            .tint(model.isRunning ? .red : .accentColor)
            .keyboardShortcut(.defaultAction)

            advanced
        }
        .padding(16)
        .glassCard(cornerRadius: 22)
        // Cap the width so the card stays a tidy panel in landscape / on iPad
        // rather than stretching edge to edge.
        .frame(maxWidth: 520)
    }

    // -- Advanced options (each maps 1:1 to MoQEndpoint.Configuration) ----

    private var advanced: some View {
        DisclosureGroup(isExpanded: $advancedExpanded) {
            VStack(alignment: .leading, spacing: 12) {
                Picker("Transport", selection: $model.transport) {
                    ForEach(PlayerModel.TransportChoice.allCases) {
                        Text($0.rawValue).tag($0)
                    }
                }
                .pickerStyle(.segmented)

                Picker("Version", selection: $model.version) {
                    ForEach(PlayerModel.VersionChoice.allCases) {
                        Text($0.rawValue).tag($0)
                    }
                }
                .pickerStyle(.segmented)

                // iOS ships no readable system PEM bundle, so OpenSSL
                // verification there needs an explicit CA file.
                field(systemImage: "lock.shield",
                      placeholder: "CA bundle path (PEM, optional)",
                      text: $model.caFilePath)

                Toggle("Skip TLS verification (insecure)",
                       isOn: $model.insecureSkipVerify)
                    .font(.callout)
            }
            .padding(.top, 12)
        } label: {
            Label("Advanced", systemImage: "slider.horizontal.3")
                .font(.subheadline)
                .foregroundStyle(.secondary)
        }
        .tint(.secondary)
    }

    private func field(systemImage: String, placeholder: String,
                       text: Binding<String>) -> some View {
        HStack(spacing: 10) {
            Image(systemName: systemImage)
                .foregroundStyle(.secondary)
                .frame(width: 16)
            TextField(placeholder, text: text)
                .textFieldStyle(.plain)
                .plainTyping()
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 11)
        .background(Color.white.opacity(0.06),
                    in: RoundedRectangle(cornerRadius: 12))
    }

    // -- Status derivation ------------------------------------------------

    /// A composed status header rather than one truncating string: a state
    /// word beside its dot, the track and negotiated draft as pill chips (which
    /// can't get clipped), and a quiet frame counter while playing.
    private var statusRow: some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack(spacing: 10) {
                HStack(spacing: 7) {
                    Circle().fill(statusColor).frame(width: 8, height: 8)
                    Text(stateWord)
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.primary)
                }
                Spacer(minLength: 8)
                if let track = model.trackName {
                    chip(track, systemImage: "film")
                }
                if let version = model.negotiatedVersion {
                    chip(draftLabel(version))
                }
            }
            if isPlaying {
                Text("\(model.sampleCount) frames decoded")
                    .font(.caption2.monospacedDigit())
                    .foregroundStyle(.tertiary)
            }
        }
    }

    private func chip(_ text: String, systemImage: String? = nil) -> some View {
        HStack(spacing: 4) {
            if let systemImage {
                Image(systemName: systemImage).font(.caption2)
            }
            Text(text)
                .font(.caption.weight(.medium))
                .monospacedDigit()
                .lineLimit(1)
        }
        .foregroundStyle(.secondary)
        .padding(.horizontal, 9)
        .padding(.vertical, 4)
        .background(Color.white.opacity(0.08), in: Capsule())
    }

    /// "draft16" → "draft 16"; anything unexpected passes through unchanged.
    private func draftLabel(_ version: String) -> String {
        guard let range = version.range(
            of: #"\d+"#, options: .regularExpression) else { return version }
        return "draft \(version[range])"
    }

    private var stateWord: String {
        switch model.state {
        case .idle:            return "Idle"
        case .connecting:      return "Connecting"
        case .established:     return "Established"
        case .waitingForMedia: return "Waiting for signal"
        case .playing:         return "Playing"
        case .ended:           return "Ended"
        case .failed:          return "Failed"
        }
    }

    private var statusColor: Color {
        switch model.state {
        case .idle, .ended:                                return .gray
        case .connecting, .established, .waitingForMedia:  return .yellow
        case .playing:                                     return .green
        case .failed:                                      return .red
        }
    }
}

// -- Small cross-platform helpers ----------------------------------------

private extension View {
    /// A translucent, hairline-bordered "glass" surface — the app's one
    /// recurring material (pill, control card, failure card).
    func glassCard(cornerRadius: CGFloat) -> some View {
        let shape = RoundedRectangle(cornerRadius: cornerRadius, style: .continuous)
        return self
            .background(.ultraThinMaterial, in: shape)
            .overlay(shape.strokeBorder(Color.white.opacity(0.10)))
    }

    /// URLs and namespaces are exact strings: no autocorrect, no
    /// capitalization. (The iOS modifiers don't exist on macOS.)
    func plainTyping() -> some View {
        #if os(iOS)
        return self
            .autocorrectionDisabled()
            .textInputAutocapitalization(.never)
        #else
        return self.autocorrectionDisabled()
        #endif
    }
}
