import SwiftUI
#if canImport(AVFoundation)
import AVFoundation
#endif

/* The ONLY platform-conditional file in the app: hosting an
 * AVSampleBufferDisplayLayer needs UIViewRepresentable on iOS and
 * NSViewRepresentable on macOS, but the layer itself — and everything the
 * PlayerModel does with it — is identical. Keep the split isolated here so
 * the rest of the app stays shared source. */

#if canImport(AVFoundation) && canImport(UIKit)

/// Hosts an `AVSampleBufferDisplayLayer` in the SwiftUI hierarchy (iOS).
struct VideoSurfaceView: UIViewRepresentable {
    let layer: AVSampleBufferDisplayLayer

    func makeUIView(context: Context) -> VideoSurfaceHostView {
        let view = VideoSurfaceHostView()
        view.attach(layer)
        return view
    }

    func updateUIView(_ uiView: VideoSurfaceHostView, context: Context) {}
}

/// A plain view that keeps the display layer sized to its bounds.
final class VideoSurfaceHostView: UIView {
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

#elseif canImport(AVFoundation) && canImport(AppKit)

/// Hosts an `AVSampleBufferDisplayLayer` in the SwiftUI hierarchy (macOS).
struct VideoSurfaceView: NSViewRepresentable {
    let layer: AVSampleBufferDisplayLayer

    func makeNSView(context: Context) -> VideoSurfaceHostView {
        let view = VideoSurfaceHostView()
        view.attach(layer)
        return view
    }

    func updateNSView(_ nsView: VideoSurfaceHostView, context: Context) {}
}

/// A layer-backed view that keeps the display layer sized to its bounds.
final class VideoSurfaceHostView: NSView {
    private weak var displayLayer: AVSampleBufferDisplayLayer?

    func attach(_ displayLayer: AVSampleBufferDisplayLayer) {
        wantsLayer = true
        displayLayer.videoGravity = .resizeAspect
        layer?.addSublayer(displayLayer)
        self.displayLayer = displayLayer
    }

    override func layout() {
        super.layout()
        displayLayer?.frame = bounds
    }
}

#endif
