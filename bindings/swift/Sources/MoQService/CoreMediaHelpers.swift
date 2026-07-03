#if canImport(CoreMedia)

import CoreMedia
import Foundation
import MoQServiceCore

/* CoreMedia bridging for the MoQService receive path.
 *
 * Posture (v1, deliberate):
 *  - STRICT extradata policy: format descriptions require the
 *    service-derived `codecConfig` (the avcC/hvcC box payload or AAC
 *    AudioSpecificConfig the C tier extracted from a CMAF init segment) or
 *    an explicit out-of-band blob via `makeFormatDescription(codecConfig:)`.
 *    `initData` is never sniffed -- a guessed extradata form produces
 *    silently wrong CoreMedia descriptions.
 *  - Sample buffers are RAW/LOC only (one access unit per object, the LOC
 *    contract). CMAF objects throw `.unsupported`: correct construction
 *    needs the per-sample table (sizes/durations) that the bridge does not
 *    surface yet -- faking a whole fragment as one sample would be wrong.
 *  - Bytes are COPIED into CMBlockBuffer-owned memory: no lifetime coupling
 *    (returned buffers do not keep the MediaObject alive). Zero-copy into
 *    CoreMedia is a contained future optimization.
 *  - Audio duration is `.invalid` (one-sample buffers): the model carries
 *    no audio sample-count/duration metadata, and video framerate must
 *    never be applied to audio. Video duration derives from the track's
 *    `framerateMillis` when present, else `.invalid`.
 */

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension Duration {
    /// CMTime at microsecond precision (our timestamps are µs-derived).
    var cmTime: CMTime {
        CMTime(value: Int64(wholeMicroseconds), timescale: 1_000_000)
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension TrackDescription {

    /// Build a `CMFormatDescription` from the service-derived decoder
    /// configuration (``TrackDescription/codecConfig``). Throws
    /// ``MoQServiceError/invalidArgument(_:)`` when the track carries no
    /// codecConfig (the catalog init was not CMAF-parseable) -- supply
    /// out-of-band extradata via ``makeFormatDescription(codecConfig:)``
    /// instead; `initData` is deliberately never guessed at.
    public func makeFormatDescription() throws -> CMFormatDescription {
        guard let codecConfig else {
            throw MoQServiceError.invalidArgument(
                "the track has no codecConfig (catalog init was not " +
                "CMAF-parseable); pass out-of-band extradata via " +
                "makeFormatDescription(codecConfig:)")
        }
        return try makeFormatDescription(codecConfig: codecConfig)
    }

    /// Build a `CMFormatDescription` with explicit decoder configuration:
    /// the avcC box payload (H.264), hvcC box payload (HEVC), or
    /// AudioSpecificConfig (AAC), matching this description's codec
    /// string. Unsupported codec families throw
    /// ``MoQServiceError/unsupported``.
    public func makeFormatDescription(codecConfig: Data) throws
        -> CMFormatDescription {
        let codecString = codec ?? ""
        if codecString.hasPrefix("avc1") || codecString.hasPrefix("avc3") {
            try requireMediaType(.video, for: codecString)
            return try makeVideoDescription(
                codecType: kCMVideoCodecType_H264, atom: "avcC",
                configuration: codecConfig)
        }
        if codecString.hasPrefix("hvc1") || codecString.hasPrefix("hev1") {
            try requireMediaType(.video, for: codecString)
            return try makeVideoDescription(
                codecType: kCMVideoCodecType_HEVC, atom: "hvcC",
                configuration: codecConfig)
        }
        if codecString.hasPrefix("mp4a") {
            try requireMediaType(.audio, for: codecString)
            return try makeAudioDescription(magicCookie: codecConfig)
        }
        throw MoQServiceError.unsupported
    }

    /// A codec family that contradicts the declared mediaType is refused:
    /// a mislabeled format description poisons AV pipelines downstream.
    private func requireMediaType(
        _ required: MediaType, for codecString: String) throws {
        guard mediaType == required else {
            throw MoQServiceError.invalidArgument(
                "codec \"\(codecString)\" contradicts the track's " +
                "declared media type")
        }
    }

    private func makeVideoDescription(
        codecType: CMVideoCodecType, atom: String,
        configuration: Data) throws -> CMFormatDescription {
        guard let width, let height, width > 0, height > 0 else {
            throw MoQServiceError.invalidArgument(
                "video dimensions are required for a format description")
        }
        let atoms: NSDictionary = [atom: configuration as NSData]
        let extensions: NSDictionary = [
            kCMFormatDescriptionExtension_SampleDescriptionExtensionAtoms:
                atoms,
        ]
        var description: CMFormatDescription?
        let status = CMVideoFormatDescriptionCreate(
            allocator: kCFAllocatorDefault,
            codecType: codecType,
            width: Int32(width), height: Int32(height),
            extensions: extensions as CFDictionary,
            formatDescriptionOut: &description)
        guard status == noErr, let description else {
            throw MoQServiceError.internalError(status)
        }
        return description
    }

    private func makeAudioDescription(
        magicCookie: Data) throws -> CMFormatDescription {
        guard let samplerate, samplerate > 0 else {
            throw MoQServiceError.invalidArgument(
                "samplerate is required for an audio format description")
        }
        guard let channels = channelConfig.flatMap(UInt32.init),
              channels > 0 else {
            throw MoQServiceError.invalidArgument(
                "channelConfig must be a numeric channel count " +
                "for an audio format description")
        }
        var asbd = AudioStreamBasicDescription(
            mSampleRate: Float64(samplerate),
            mFormatID: kAudioFormatMPEG4AAC,
            mFormatFlags: 0,
            mBytesPerPacket: 0,
            mFramesPerPacket: 1024,
            mBytesPerFrame: 0,
            mChannelsPerFrame: channels,
            mBitsPerChannel: 0,
            mReserved: 0)
        var description: CMFormatDescription?
        let status = magicCookie.withUnsafeBytes { raw in
            CMAudioFormatDescriptionCreate(
                allocator: kCFAllocatorDefault,
                asbd: &asbd,
                layoutSize: 0, layout: nil,
                magicCookieSize: raw.count, magicCookie: raw.baseAddress,
                extensions: nil,
                formatDescriptionOut: &description)
        }
        guard status == noErr, let description else {
            throw MoQServiceError.internalError(status)
        }
        return description
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension MediaObject {

    /// The media bytes as a `CMBlockBuffer` (COPIED into CoreMedia-owned
    /// memory; the returned buffer does not keep this object alive).
    public func makeBlockBuffer() throws -> CMBlockBuffer {
        try withUnsafeMediaBytes { raw in
            var block: CMBlockBuffer?
            var status = CMBlockBufferCreateWithMemoryBlock(
                allocator: kCFAllocatorDefault,
                memoryBlock: nil,
                blockLength: raw.count,
                blockAllocator: kCFAllocatorDefault,
                customBlockSource: nil,
                offsetToData: 0,
                dataLength: raw.count,
                flags: kCMBlockBufferAssureMemoryNowFlag,
                blockBufferOut: &block)
            guard status == noErr, let block else {
                throw MoQServiceError.internalError(status)
            }
            if let base = raw.baseAddress, raw.count > 0 {
                status = CMBlockBufferReplaceDataBytes(
                    with: base, blockBuffer: block,
                    offsetIntoDestination: 0, dataLength: raw.count)
                guard status == noErr else {
                    throw MoQServiceError.internalError(status)
                }
            }
            return block
        }
    }

    /// One-sample `CMSampleBuffer` for a RAW/LOC object (one access unit
    /// per object, the LOC contract), ready for
    /// `AVSampleBufferDisplayLayer`/`AVSampleBufferAudioRenderer` flows.
    ///
    /// Timing: pts/dts from the object's timestamps (µs timescale). Video
    /// duration derives from the track's `framerateMillis` when present;
    /// audio duration is always `.invalid` (the model carries no audio
    /// sample timing, and video framerate must never be applied to audio).
    /// Non-keyframe VIDEO samples carry NotSync/DependsOnOthers
    /// attachments.
    ///
    /// CMAF objects throw ``MoQServiceError/unsupported``: correct
    /// construction needs per-sample sizes/durations (the C tier's sample
    /// table), which the bridge does not surface yet; a whole fragment
    /// faked as one sample would be silently wrong.
    public func makeSampleBuffer(
        formatDescription: CMFormatDescription) throws -> CMSampleBuffer {
        guard track.description.packaging == .raw else {
            throw MoQServiceError.unsupported
        }
        /* The caller's format description must MATCH this object's track:
         * timing and attachments key off the description's media type, so
         * a video object with an audio description (or vice versa) would
         * silently produce a mislabeled sample buffer. */
        let mediaType = CMFormatDescriptionGetMediaType(formatDescription)
        let expected: CMMediaType = track.description.mediaType == .video
            ? kCMMediaType_Video : kCMMediaType_Audio
        guard mediaType == expected else {
            throw MoQServiceError.invalidArgument(
                "the format description's media type does not match " +
                "the object's track")
        }
        let block = try makeBlockBuffer()

        var duration = CMTime.invalid
        if mediaType == kCMMediaType_Video,
           let framerateMillis = track.description.framerateMillis,
           framerateMillis > 0 {
            /* frames/s * 1000 -> one frame lasts 1e9/framerateMillis µs. */
            duration = CMTime(
                value: Int64(1_000_000_000 / framerateMillis),
                timescale: 1_000_000)
        }
        var timing = CMSampleTimingInfo(
            duration: duration,
            presentationTimeStamp: presentationTime.cmTime,
            decodeTimeStamp: decodeTime.cmTime)
        var sampleSize = CMBlockBufferGetDataLength(block)

        var sample: CMSampleBuffer?
        let status = CMSampleBufferCreateReady(
            allocator: kCFAllocatorDefault,
            dataBuffer: block,
            formatDescription: formatDescription,
            sampleCount: 1,
            sampleTimingEntryCount: 1, sampleTimingArray: &timing,
            sampleSizeEntryCount: 1, sampleSizeArray: &sampleSize,
            sampleBufferOut: &sample)
        guard status == noErr, let sample else {
            throw MoQServiceError.internalError(status)
        }

        if mediaType == kCMMediaType_Video && !isKeyframe,
           let attachments = CMSampleBufferGetSampleAttachmentsArray(
               sample, createIfNecessary: true) as? [NSMutableDictionary],
           let first = attachments.first {
            first[kCMSampleAttachmentKey_NotSync] = kCFBooleanTrue
            first[kCMSampleAttachmentKey_DependsOnOthers] = kCFBooleanTrue
        }
        return sample
    }
}

#endif  /* canImport(CoreMedia) */
