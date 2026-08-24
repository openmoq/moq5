import AudioToolbox
import AVFoundation
import Foundation

package enum AACConverterError: Error, CustomStringConvertible {
    case converterCreationFailed(OSStatus)
    /// The AudioSpecificConfig is too large to encode in the one-byte ESDS
    /// descriptor bodies -- rejected BEFORE creating the converter, distinct
    /// from a converter-creation failure.
    case oversizedCodecConfig
    package var description: String {
        switch self {
        case .converterCreationFailed(let s): return "AudioConverterNew failed: \(s)"
        case .oversizedCodecConfig: return "oversized AAC codec config"
        }
    }
}

/// The largest AudioSpecificConfig that fits the one-byte ESDS descriptor
/// bodies: the outer ES descriptor body is `20 + ascLen`, the tightest field,
/// so `20 + ascLen <= 0x7f` gives `ascLen <= 107`.
let maxAACCodecConfigBytes = 107

/// The converter-creation seam. Defaults to `AudioConverterNew`, so the real
/// player path is behavior-identical; a test can inject a recording factory to
/// observe whether the converter is created BEFORE oversized-ASC validation
/// (the "before AudioConverterNew" rule) without touching audio hardware.
package typealias AudioConverterFactory = (
    UnsafePointer<AudioStreamBasicDescription>,
    UnsafePointer<AudioStreamBasicDescription>,
    UnsafeMutablePointer<AudioConverterRef?>) -> OSStatus

package final class AudioDecoder {
    private var engine: AVAudioEngine?
    private var playerNode: AVAudioPlayerNode?
    private var converter: AudioConverterRef?
    private var pcmFormat: AVAudioFormat?
    private var sampleRate: Double = 0
    private var channels: UInt32 = 0

    package init() {}

    package func configure(codec: String, sampleRate: UInt32, channels: UInt32,
                   codecConfig: Data?) {
        teardown()

        guard let formatID = audioFormatID(from: codec) else {
            print("  [audio] unsupported codec: \(codec)")
            return
        }

        var ch = channels
        if ch == 0, let asc = codecConfig, asc.count >= 2 {
            ch = aacChannelCount(from: asc)
        }
        if ch == 0 {
            print("  [audio] cannot determine channel count")
            return
        }

        self.sampleRate = Double(sampleRate)
        self.channels = ch

        // Non-interleaved Float32 PCM at the SOURCE sample rate.
        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: Double(sampleRate),
            channels: AVAudioChannelCount(ch),
            interleaved: false
        ) else {
            print("  [audio] AVAudioFormat failed")
            return
        }

        let conv: AudioConverterRef
        do {
            conv = try makeAACConverter(
                formatID: formatID, sampleRate: sampleRate,
                channels: ch, codecConfig: codecConfig)
        } catch {
            print("  [audio] \(error)")
            return
        }

        self.converter = conv
        self.pcmFormat = format

        let engine = AVAudioEngine()
        let playerNode = AVAudioPlayerNode()
        engine.attach(playerNode)

        // Connect at the SOURCE sample rate. AVAudioEngine handles
        // resampling to the hardware rate internally.
        engine.connect(playerNode, to: engine.mainMixerNode, format: format)

        do {
            try engine.start()
        } catch {
            print("  [audio] engine start failed: \(error)")
            AudioConverterDispose(conv)
            self.converter = nil
            self.pcmFormat = nil
            return
        }

        playerNode.play()
        self.engine = engine
        self.playerNode = playerNode
        print("  [audio] configured: \(codec) \(sampleRate)Hz \(ch)ch")
    }

    package func decode(payload: Data, presentationTimeUS: Int64,
                syncHostTime: Int64) {
        guard let converter, let playerNode, let pcmFormat else { return }
        guard !payload.isEmpty else { return }

        guard let pcm = decodePacket(
            payload: payload, converter: converter,
            pcmFormat: pcmFormat) else { return }

        if syncHostTime > 0 {
            let offsetTicks = microsToHostTicks(presentationTimeUS)
            let target: UInt64
            if offsetTicks >= 0 {
                target = UInt64(syncHostTime) &+ UInt64(offsetTicks)
            } else {
                let neg = UInt64(-offsetTicks)
                target = neg < UInt64(syncHostTime)
                    ? UInt64(syncHostTime) - neg : 0
            }
            playerNode.scheduleBuffer(
                pcm, at: AVAudioTime(hostTime: target))
        } else {
            playerNode.scheduleBuffer(pcm)
        }
    }

    package func flush() {
        playerNode?.stop()
        playerNode?.play()
    }

    package func teardown() {
        playerNode?.stop()
        engine?.stop()
        if let converter { AudioConverterDispose(converter) }
        engine = nil
        playerNode = nil
        converter = nil
        pcmFormat = nil
    }

    private func decodePacket(
        payload: Data, converter: AudioConverterRef,
        pcmFormat: AVAudioFormat
    ) -> AVAudioPCMBuffer? {
        let framesPerPacket: AVAudioFrameCount = 1024

        // Decode to interleaved PCM first.
        let interleavedBytes = Int(framesPerPacket) * Int(channels) * 4
        var interleavedBuf = Data(count: interleavedBytes)

        var outputBufferList = AudioBufferList(
            mNumberBuffers: 1,
            mBuffers: AudioBuffer(
                mNumberChannels: channels,
                mDataByteSize: UInt32(interleavedBytes),
                mData: nil))

        var packetCount = framesPerPacket
        let nsPayload = payload as NSData
        var context = AudioConverterContext(
            dataPtr: UnsafeMutableRawPointer(mutating: nsPayload.bytes),
            dataSize: UInt32(nsPayload.length),
            channels: channels,
            consumed: false,
            packetDesc: AudioStreamPacketDescription(
                mStartOffset: 0, mVariableFramesInPacket: 0,
                mDataByteSize: UInt32(nsPayload.length)))

        let status: OSStatus = interleavedBuf.withUnsafeMutableBytes { rawBuf in
            outputBufferList.mBuffers.mData = rawBuf.baseAddress
            return withUnsafeMutablePointer(to: &context) { ctxPtr in
                AudioConverterFillComplexBuffer(
                    converter, audioConverterInputCallback,
                    ctxPtr, &packetCount, &outputBufferList, nil)
            }
        }

        if status != noErr && packetCount == 0 { return nil }
        guard packetCount > 0 else { return nil }

        // Convert interleaved → non-interleaved for AVAudioEngine.
        guard let pcmBuffer = AVAudioPCMBuffer(
            pcmFormat: pcmFormat,
            frameCapacity: packetCount) else { return nil }
        pcmBuffer.frameLength = packetCount

        let frameCount = Int(packetCount)
        let chCount = Int(channels)

        interleavedBuf.withUnsafeBytes { src in
            let srcFloats = src.bindMemory(to: Float.self)
            for ch in 0..<chCount {
                guard let dst = pcmBuffer.floatChannelData?[ch] else { continue }
                for f in 0..<frameCount {
                    dst[f] = srcFloats[f * chCount + ch]
                }
            }
        }

        return pcmBuffer
    }

    /// Create the AAC decompression converter and install its magic cookie.
    /// Extracted from `configure` (same order, same effect) so the real player
    /// runs this exact path; `factory` defaults to `AudioConverterNew`. The ASC
    /// length is validated BEFORE the converter is created, so an oversized
    /// config is rejected with a typed error and `factory` is never called (the
    /// security regression asserts exactly that -- zero factory calls). This
    /// closes MOQ-SWIFTPLAYER-AAC-LENGTH-TRAP, where the pre-fix code built the
    /// converter first and only then reached `buildAACMagicCookie`, whose
    /// descriptor-length `UInt8(...)` conversion overflowed on a large ASC.
    package func makeAACConverter(
        formatID: AudioFormatID, sampleRate: UInt32, channels ch: UInt32,
        codecConfig: Data?,
        factory: AudioConverterFactory = AudioConverterNew
    ) throws -> AudioConverterRef {
        // Validate the ASC length BEFORE creating the converter, so an oversized
        // config is rejected with a typed error and the factory is never called.
        if let asc = codecConfig, !asc.isEmpty, asc.count > maxAACCodecConfigBytes {
            throw AACConverterError.oversizedCodecConfig
        }

        var inputDesc = AudioStreamBasicDescription(
            mSampleRate: Float64(sampleRate), mFormatID: formatID,
            mFormatFlags: 0, mBytesPerPacket: 0, mFramesPerPacket: 1024,
            mBytesPerFrame: 0, mChannelsPerFrame: ch, mBitsPerChannel: 0,
            mReserved: 0)

        // Interleaved output for AudioConverter (simpler callback).
        var outputDesc = AudioStreamBasicDescription(
            mSampleRate: Float64(sampleRate), mFormatID: kAudioFormatLinearPCM,
            mFormatFlags: kAudioFormatFlagIsFloat | kAudioFormatFlagIsPacked,
            mBytesPerPacket: ch * 4, mFramesPerPacket: 1,
            mBytesPerFrame: ch * 4, mChannelsPerFrame: ch,
            mBitsPerChannel: 32, mReserved: 0)

        var conv: AudioConverterRef?
        var status = factory(&inputDesc, &outputDesc, &conv)
        guard status == noErr, let conv else {
            throw AACConverterError.converterCreationFailed(status)
        }

        if let asc = codecConfig, !asc.isEmpty {
            let cookie = try buildAACMagicCookie(asc: asc)
            cookie.withUnsafeBytes { buf in
                status = AudioConverterSetProperty(
                    conv, kAudioConverterDecompressionMagicCookie,
                    UInt32(buf.count), buf.baseAddress!)
            }
            if status != noErr {
                asc.withUnsafeBytes { buf in
                    _ = AudioConverterSetProperty(
                        conv, kAudioConverterDecompressionMagicCookie,
                        UInt32(buf.count), buf.baseAddress!)
                }
            }
        }
        return conv
    }

    package func buildAACMagicCookie(asc: Data) throws -> Data {
        let ascLen = asc.count
        // Reject an oversized ASC normally instead of overflowing the one-byte
        // ESDS descriptor bodies via `UInt8(...)` below.
        guard ascLen <= maxAACCodecConfigBytes else {
            throw AACConverterError.oversizedCodecConfig
        }
        let decSpecLen = 2 + ascLen
        let decCfgLen = 2 + 13 + decSpecLen
        let esDescLen = 2 + 3 + decCfgLen

        var buf = Data(capacity: esDescLen)
        buf.append(0x03)
        buf.append(UInt8(esDescLen - 2))
        buf.append(contentsOf: [0x00, 0x00])
        buf.append(0x00)
        buf.append(0x04)
        buf.append(UInt8(decCfgLen - 2))
        buf.append(0x40)
        buf.append(0x15)
        buf.append(contentsOf: [0x00, 0x00, 0x00])
        buf.append(contentsOf: [0x00, 0x00, 0x00, 0x00])
        buf.append(contentsOf: [0x00, 0x00, 0x00, 0x00])
        buf.append(0x05)
        buf.append(UInt8(ascLen))
        buf.append(asc)
        return buf
    }

    private func aacChannelCount(from asc: Data) -> UInt32 {
        guard asc.count >= 2 else { return 0 }
        let b0 = asc[asc.startIndex]
        let b1 = asc[asc.startIndex + 1]
        let freqIdx = ((b0 & 0x07) << 1) | ((b1 & 0x80) >> 7)
        let chanCfg: UInt8
        if freqIdx == 0x0F {
            guard asc.count >= 5 else { return 0 }
            chanCfg = (asc[asc.startIndex + 4] & 0x78) >> 3
        } else {
            chanCfg = (b1 & 0x78) >> 3
        }
        let table: [UInt32] = [0, 1, 2, 3, 4, 5, 6, 8]
        return chanCfg < table.count ? table[Int(chanCfg)] : 0
    }

    private func audioFormatID(from codec: String) -> AudioFormatID? {
        let lower = codec.lowercased()
        if lower.hasPrefix("mp4a.40.29") { return kAudioFormatMPEG4AAC_HE_V2 }
        if lower.hasPrefix("mp4a.40.5") { return kAudioFormatMPEG4AAC_HE }
        if lower.hasPrefix("mp4a.40.2") { return kAudioFormatMPEG4AAC }
        if lower.hasPrefix("mp4a.40") { return kAudioFormatMPEG4AAC }
        if lower.hasPrefix("mp4a") { return kAudioFormatMPEG4AAC }
        // Opus requires different packetization and dOps config — not yet supported.
        return nil
    }

    private static let machNumer: Int64 = {
        var info = mach_timebase_info_data_t()
        mach_timebase_info(&info)
        return Int64(info.numer)
    }()
    private static let machDenom: Int64 = {
        var info = mach_timebase_info_data_t()
        mach_timebase_info(&info)
        return Int64(info.denom)
    }()

    private func microsToHostTicks(_ us: Int64) -> Int64 {
        let ns = us &* 1000
        return ns &* AudioDecoder.machDenom / AudioDecoder.machNumer
    }
}

private struct AudioConverterContext {
    var dataPtr: UnsafeMutableRawPointer?
    var dataSize: UInt32
    var channels: UInt32
    var consumed: Bool
    var packetDesc: AudioStreamPacketDescription
}

private let audioConverterInputCallback: AudioConverterComplexInputDataProc = {
    (_, ioNumberDataPackets, ioData, outDataPacketDescription, inUserData) -> OSStatus in
    guard let ctxPtr = inUserData?.assumingMemoryBound(
        to: AudioConverterContext.self) else {
        ioNumberDataPackets.pointee = 0
        return noErr
    }
    if ctxPtr.pointee.consumed || ctxPtr.pointee.dataPtr == nil {
        ioNumberDataPackets.pointee = 0
        return noErr
    }
    ioNumberDataPackets.pointee = 1
    ioData.pointee.mNumberBuffers = 1
    ioData.pointee.mBuffers.mNumberChannels = ctxPtr.pointee.channels
    ioData.pointee.mBuffers.mDataByteSize = ctxPtr.pointee.dataSize
    ioData.pointee.mBuffers.mData = ctxPtr.pointee.dataPtr
    if let outDesc = outDataPacketDescription {
        let rawCtx = UnsafeMutableRawPointer(ctxPtr)
        let offset = MemoryLayout<AudioConverterContext>.offset(
            of: \AudioConverterContext.packetDesc)!
        let descPtr = rawCtx.advanced(by: offset)
            .assumingMemoryBound(to: AudioStreamPacketDescription.self)
        outDesc.pointee = descPtr
    }
    ctxPtr.pointee.consumed = true
    return noErr
}
