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
            return try makeAudioDescription(audioSpecificConfig: codecConfig)
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
        // CoreMedia dimensions are Int32. Reject out-of-range values with a
        // typed error rather than trapping the `Int32(...)` conversion below.
        // A representable Int32.max is deliberately allowed through to CoreMedia
        // (unrealistic, but not a Swift range violation).
        guard width <= Int(Int32.max), height <= Int(Int32.max) else {
            throw MoQServiceError.invalidArgument(
                "video dimensions exceed the CoreMedia Int32 range")
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
        audioSpecificConfig asc: Data) throws -> CMFormatDescription {
        // MSF-01 §5.2.28/§5.2.29 require samplerate/channelConfig on authored
        // audio tracks, but real catalogs (moqtail) omit them, and both are
        // carried in the AAC AudioSpecificConfig already in codecConfig. Derive
        // ONLY the field(s) the catalog omits, as a tolerant CONSUMER
        // convenience; explicit catalog metadata always wins, and an ASC piece
        // needed only for a field the catalog DID provide must never cause a
        // rejection -- e.g. a program-config-element channel layout is fine when
        // channelConfig is explicit. Skip the parser entirely when nothing is
        // missing. (codecConfig is required upstream, so `asc` is always present
        // here -- sender-side strictness is unchanged.)
        let needSampleRate = (samplerate == nil)
        let needChannels = (channelConfig == nil)
        let derived: (sampleRate: Int?, channels: UInt32?)
        if needSampleRate || needChannels {
            derived = try Self.deriveAudioParameters(
                fromASC: asc,
                needSampleRate: needSampleRate, needChannels: needChannels)
        } else {
            derived = (nil, nil)
        }

        let effectiveSampleRate: Float64
        if let samplerate {
            guard samplerate > 0 else {
                throw MoQServiceError.invalidArgument(
                    "samplerate must be a positive value " +
                    "for an audio format description")
            }
            effectiveSampleRate = Float64(samplerate)
        } else {
            // deriveAudioParameters throws for a needed-but-underivable field,
            // so this is non-nil here; the guard is defensive.
            guard let derivedRate = derived.sampleRate else {
                throw MoQServiceError.invalidArgument(
                    "samplerate is absent and could not be derived from the " +
                    "AudioSpecificConfig")
            }
            effectiveSampleRate = Float64(derivedRate)
        }

        let channels: UInt32
        if let channelConfig {
            guard let explicit = UInt32(channelConfig), explicit > 0 else {
                throw MoQServiceError.invalidArgument(
                    "channelConfig must be a numeric channel count " +
                    "for an audio format description")
            }
            channels = explicit
        } else {
            guard let derivedChannels = derived.channels else {
                throw MoQServiceError.invalidArgument(
                    "channelConfig is absent and could not be derived from " +
                    "the AudioSpecificConfig")
            }
            channels = derivedChannels
        }

        var asbd = AudioStreamBasicDescription(
            mSampleRate: effectiveSampleRate,
            mFormatID: kAudioFormatMPEG4AAC,
            mFormatFlags: 0,
            mBytesPerPacket: 0,
            mFramesPerPacket: 1024,
            mBytesPerFrame: 0,
            mChannelsPerFrame: channels,
            mBitsPerChannel: 0,
            mReserved: 0)
        // CoreAudio's AAC decoder wants the AudioSpecificConfig wrapped in an
        // MPEG-4 ES descriptor (esds) as its magic cookie; handed the bare ASC
        // it fails at converter-init time ("failure when setting cookie"), so
        // the description parses fine but decodes nothing. Wrap it.
        let cookie = Self.aacMagicCookie(audioSpecificConfig: asc)
        var description: CMFormatDescription?
        let status = cookie.withUnsafeBytes { raw in
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

    /// Wrap a raw AAC AudioSpecificConfig in the minimal MPEG-4 ES descriptor
    /// (esds) CoreAudio expects as its decoder magic cookie: ES_Descriptor ->
    /// DecoderConfigDescriptor (objectType 0x40 = audio, streamType 0x15 =
    /// AudioStream) -> DecoderSpecificInfo (= the ASC). Descriptor lengths use
    /// single-byte encoding, valid while the whole cookie stays under 128 bytes
    /// (an ASC is only a few bytes); a pathologically large config falls back
    /// to the bare ASC rather than trap.
    private static func aacMagicCookie(audioSpecificConfig asc: Data) -> Data {
        let decSpecLen = 2 + asc.count
        let decCfgLen = 2 + 13 + decSpecLen
        let esDescLen = 2 + 3 + decCfgLen
        guard esDescLen <= 0x7F else { return asc }
        var cookie = Data(capacity: esDescLen)
        cookie.append(0x03)                                  // ES_Descriptor
        cookie.append(UInt8(esDescLen - 2))
        cookie.append(contentsOf: [0x00, 0x00])              // ES_ID
        cookie.append(0x00)                                  // stream priority
        cookie.append(0x04)                                  // DecoderConfig
        cookie.append(UInt8(decCfgLen - 2))
        cookie.append(0x40)                                  // objectType: audio
        cookie.append(0x15)                                  // streamType: audio
        cookie.append(contentsOf: [0x00, 0x00, 0x00])        // bufferSizeDB
        cookie.append(contentsOf: [0x00, 0x00, 0x00, 0x00])  // maxBitrate
        cookie.append(contentsOf: [0x00, 0x00, 0x00, 0x00])  // avgBitrate
        cookie.append(0x05)                                  // DecoderSpecificInfo
        cookie.append(UInt8(asc.count))
        cookie.append(asc)
        return cookie
    }

    /// Derive the sampling frequency (Hz) and/or channel count from an AAC
    /// AudioSpecificConfig (ISO/IEC 14496-3 §1.6.2.1): a 5-bit audioObjectType
    /// (with the 6-bit escape when 31), a 4-bit samplingFrequencyIndex (with a
    /// 24-bit explicit-frequency escape when 15), then a 4-bit
    /// channelConfiguration. This is a fallback for catalogs that omit the MSF
    /// samplerate/channelConfig fields.
    ///
    /// It always reads the whole prefix so the bit cursor lands correctly, but
    /// only THROWS for a field the caller actually needs (`needSampleRate` /
    /// `needChannels`) and cannot derive unambiguously -- a reserved frequency
    /// index, or a channelConfiguration of 0/reserved (a program config element
    /// whose channel count is not a simple field). A field that is not needed is
    /// returned as nil without rejection, so an unsupported ASC piece required
    /// only for an explicit catalog value never blocks construction. A truncated
    /// config always throws, since the mandatory prefix cannot be parsed.
    /// Returned components are non-nil for every needed field.
    private static func deriveAudioParameters(
        fromASC asc: Data, needSampleRate: Bool, needChannels: Bool
    ) throws -> (sampleRate: Int?, channels: UInt32?) {
        let bytes = [UInt8](asc)
        var bitPos = 0
        func readBits(_ count: Int) throws -> UInt32 {
            var value: UInt32 = 0
            for _ in 0..<count {
                let byteIndex = bitPos >> 3
                guard byteIndex < bytes.count else {
                    throw MoQServiceError.invalidArgument(
                        "AudioSpecificConfig is too short to derive the " +
                        "audio format")
                }
                let bit = (bytes[byteIndex] >> (7 - (bitPos & 7))) & 1
                value = (value << 1) | UInt32(bit)
                bitPos += 1
            }
            return value
        }

        // audioObjectType, with the 31 -> +6-bit escape. Not used for the
        // rate/channel fields, but read so the bit cursor lands on them.
        var audioObjectType = try readBits(5)
        if audioObjectType == 31 {
            audioObjectType = 32 + (try readBits(6))
        }
        _ = audioObjectType

        // Sampling frequency: read the index (and its 24-bit escape) regardless,
        // so the channelConfiguration bits are reached even when the rate itself
        // is not needed or not derivable.
        let frequencyIndex = try readBits(4)
        var sampleRate: Int?
        if frequencyIndex == 15 {
            let explicit = Int(try readBits(24))   // explicit frequency escape
            sampleRate = explicit > 0 ? explicit : nil
        } else {
            let table = [96000, 88200, 64000, 48000, 44100, 32000, 24000,
                         22050, 16000, 12000, 11025, 8000, 7350]
            sampleRate = Int(frequencyIndex) < table.count
                ? table[Int(frequencyIndex)] : nil
        }
        if needSampleRate {
            guard let derivedRate = sampleRate, derivedRate > 0 else {
                throw MoQServiceError.invalidArgument(
                    "AudioSpecificConfig sampling-frequency index " +
                    "\(frequencyIndex) is reserved; supply an explicit " +
                    "samplerate")
            }
        }

        let channelConfiguration = try readBits(4)
        let channels: UInt32?
        switch channelConfiguration {
        case 1...6:
            channels = channelConfiguration          // 1..6 map 1:1
        case 7:
            channels = 8                             // 7.1
        default:
            // 0 = program config element (count is not a simple field);
            // 8..15 reserved.
            channels = nil
        }
        if needChannels {
            guard channels != nil else {
                // Refuse rather than guess -- but only when the catalog did not
                // supply an explicit channelConfig.
                throw MoQServiceError.invalidArgument(
                    "AudioSpecificConfig channel configuration " +
                    "\(channelConfiguration) is not a simple channel count; " +
                    "supply an explicit channelConfig")
            }
        }
        return (sampleRate, channels)
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
