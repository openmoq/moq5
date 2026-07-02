import Foundation

/// Configuration for one published track (``MediaSender/addTrack(_:)``).
///
/// The required-field rules mirror the C tier / MSF-01: `name`, `mediaType`,
/// `packaging`, `codec`, and `bitrate` always; audio additionally requires
/// `samplerate` and `channelConfig`.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public struct TrackConfiguration: Sendable {
    public var name: String
    public var mediaType: MediaType
    public var packaging: MediaPackaging
    public var codec: String
    public var bitrate: UInt64
    /// Codec/decoder init data (H.264 SPS/PPS, AAC ASC, CMAF ftyp+moov);
    /// copied at addTrack. Empty when the codec carries parameters in-band.
    public var initData: Data?
    public var timescale: UInt32?
    public var isLive: Bool
    // Video
    public var width: Int?
    public var height: Int?
    public var framerateMillis: UInt64?
    // Audio
    public var samplerate: Int?
    public var channelConfig: String?
    // Optional metadata
    public var role: String?
    public var language: String?
    public var label: String?

    public init(name: String, mediaType: MediaType,
                packaging: MediaPackaging, codec: String, bitrate: UInt64) {
        self.name = name
        self.mediaType = mediaType
        self.packaging = packaging
        self.codec = codec
        self.bitrate = bitrate
        self.isLive = true
    }

    /// A RAW/LOC video track with the commonly required fields.
    public static func video(name: String, codec: String, bitrate: UInt64,
                             width: Int, height: Int,
                             initData: Data? = nil) -> TrackConfiguration {
        var cfg = TrackConfiguration(name: name, mediaType: .video,
                                     packaging: .raw, codec: codec,
                                     bitrate: bitrate)
        cfg.width = width
        cfg.height = height
        cfg.initData = initData
        return cfg
    }

    /// A RAW/LOC audio track with the commonly required fields.
    public static func audio(name: String, codec: String, bitrate: UInt64,
                             samplerate: Int, channelConfig: String,
                             initData: Data? = nil) -> TrackConfiguration {
        var cfg = TrackConfiguration(name: name, mediaType: .audio,
                                     packaging: .raw, codec: codec,
                                     bitrate: bitrate)
        cfg.samplerate = samplerate
        cfg.channelConfig = channelConfig
        cfg.initData = initData
        return cfg
    }

    package func validate() throws {
        guard !name.isEmpty else {
            throw MoQServiceError.invalidArgument("track name must not be empty")
        }
        guard !codec.isEmpty else {
            throw MoQServiceError.invalidArgument(
                "codec is required for audio/video tracks (MSF-01 §5.2.18)")
        }
        guard bitrate > 0 else {
            throw MoQServiceError.invalidArgument(
                "bitrate is required for audio/video tracks (MSF-01 §5.2.22)")
        }
        /* Optional numerics must be positive when given: a zero/negative
         * value would become a huge unsigned C size or an invalid catalog
         * field at the bridge. */
        if let w = width, w <= 0 {
            throw MoQServiceError.invalidArgument("width must be positive")
        }
        if let h = height, h <= 0 {
            throw MoQServiceError.invalidArgument("height must be positive")
        }
        if let fr = framerateMillis, fr == 0 {
            throw MoQServiceError.invalidArgument("framerate must be positive")
        }
        if mediaType == .audio {
            guard let sr = samplerate, sr > 0 else {
                throw MoQServiceError.invalidArgument(
                    "samplerate is required for audio tracks and must be positive")
            }
            guard let cc = channelConfig, !cc.isEmpty else {
                throw MoQServiceError.invalidArgument(
                    "channelConfig is required for audio tracks")
            }
        } else if let sr = samplerate, sr <= 0 {
            throw MoQServiceError.invalidArgument("samplerate must be positive")
        }
    }
}
