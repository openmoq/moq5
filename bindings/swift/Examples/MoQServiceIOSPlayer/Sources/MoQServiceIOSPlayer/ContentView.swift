import SwiftUI
#if canImport(AVFoundation)
import AVFoundation
#endif
#if canImport(UIKit)
import UIKit
#endif

/// Deliberately unpolished: input fields, a connect/disconnect action, a
/// status line, a sample counter, and the display layer. Enough to prove the
/// app shape, not a finished player.
struct ContentView: View {
    @StateObject private var model = PlayerModel()

    var body: some View {
        VStack(spacing: 16) {
            #if canImport(AVFoundation) && canImport(UIKit)
            DisplayLayerView(layer: model.displayLayer)
                .aspectRatio(16.0 / 9.0, contentMode: .fit)
                .frame(maxWidth: .infinity)
                .background(Color.black)
                .clipShape(RoundedRectangle(cornerRadius: 8))
            #endif

            TextField("moqt:// relay URL", text: $model.urlText)
                .textFieldStyle(.roundedBorder)
                .autocorrectionDisabled()
                .textInputAutocapitalization(.never)

            TextField("namespace (e.g. live/cam1)", text: $model.namespaceText)
                .textFieldStyle(.roundedBorder)
                .autocorrectionDisabled()
                .textInputAutocapitalization(.never)

            Button(model.isRunning ? "Disconnect" : "Connect") {
                model.toggle()
            }
            .buttonStyle(.borderedProminent)

            Text(model.status)
                .font(.footnote)
                .foregroundStyle(.secondary)
            Text("\(model.sampleCount) sample buffers")
                .font(.caption.monospacedDigit())
                .foregroundStyle(.secondary)

            Spacer()
        }
        .padding()
    }
}

#if canImport(AVFoundation) && canImport(UIKit)
/// Hosts an `AVSampleBufferDisplayLayer` in the SwiftUI hierarchy.
struct DisplayLayerView: UIViewRepresentable {
    let layer: AVSampleBufferDisplayLayer

    func makeUIView(context: Context) -> DisplayLayerHostView {
        let view = DisplayLayerHostView()
        view.attach(layer)
        return view
    }

    func updateUIView(_ uiView: DisplayLayerHostView, context: Context) {}
}

/// A plain view that keeps the display layer sized to its bounds.
final class DisplayLayerHostView: UIView {
    private weak var displayLayer: AVSampleBufferDisplayLayer?

    func attach(_ displayLayer: AVSampleBufferDisplayLayer) {
        displayLayer.videoGravity = .resizeAspect
        layer.addSublayer(displayLayer)
        self.displayLayer = displayLayer
    }

    override func layoutSubviews() {
        super.layoutSubviews()
        displayLayer?.frame = bounds
    }
}
#endif
