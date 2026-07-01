import Foundation
import MoQ

/// Everything a renderer needs to configure a playback track from
/// an MSF catalog entry: the pipeline configuration, the codec kind,
/// and the raw codec config bytes for format description / decoder setup.
public struct PlaybackTrackDescriptor: Sendable {
    /// The original catalog track entry.
    public let catalogTrack: MSFTrack
    /// Ready-to-use pipeline track configuration.
    public let configuration: PlaybackTrackConfiguration
    /// CMAF codec kind (avc, hevc, aac, etc.), nil for LOC/RAW tracks.
    public let codecKind: CMAFCodecKind?
    /// Codec config bytes: avcC for AVC video, AudioSpecificConfig
    /// for AAC audio. For CMAF, extracted from the init segment.
    /// For LOC, the raw decoded initData.
    public let codecConfig: Data
}

extension MSFTrack {
    /// Build a playback descriptor from this catalog entry.
    ///
    /// For CMAF tracks with initData, parses the CMAF init segment
    /// to extract timescale, dimensions, sample rate, channel count,
    /// and codec config. For LOC/RAW tracks, populates from catalog
    /// fields and uses initData as raw codec config.
    ///
    /// Throws `MediaTrackInfoError` for missing/unsupported role or
    /// packaging. Throws `MSFError` or `CMAFParseError` for
    /// malformed initData.
    public func playbackDescriptor() throws -> PlaybackTrackDescriptor {
        let info = try mediaTrackInfo()

        var cfg = PlaybackTrackConfiguration(
            mediaType: info.mediaType,
            packaging: info.packaging,
            codec: codec)
        cfg.isLive = isLive

        var kind: CMAFCodecKind? = nil
        var config = Data()

        if info.packaging == .cmaf {
            let initInfo = try parseCMAFInitInfo()
            cfg.timescale = initInfo.timescale
            cfg.initData = try decodedInitData()
            cfg.width = initInfo.width
            cfg.height = initInfo.height
            cfg.samplerate = initInfo.samplerate
            cfg.channelCount = initInfo.channelCount
            kind = initInfo.codecKind
            config = initInfo.codecConfig
        } else {
            let rawInit = try decodedInitData()
            cfg.initData = rawInit
            config = rawInit
            cfg.width = width ?? 0
            cfg.height = height ?? 0
            cfg.samplerate = samplerate ?? 0
            cfg.channelCount = UInt32(
                channelConfig.flatMap(UInt32.init) ?? 0)
            if let ts = timescale, ts <= UInt64(UInt32.max) {
                cfg.timescale = UInt32(ts)
            }
        }

        return PlaybackTrackDescriptor(
            catalogTrack: self,
            configuration: cfg,
            codecKind: kind,
            codecConfig: config)
    }
}
