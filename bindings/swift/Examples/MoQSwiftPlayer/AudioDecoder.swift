import AudioToolbox
import AVFoundation
import Foundation

final class AudioDecoder {
    private var engine: AVAudioEngine?
    private var playerNode: AVAudioPlayerNode?
    private var converter: AudioConverterRef?
    private var pcmFormat: AVAudioFormat?
    private var sampleRate: Double = 0
    private var channels: UInt32 = 0

    func configure(codec: String, sampleRate: UInt32, channels: UInt32,
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

        var inputDesc = AudioStreamBasicDescription(
            mSampleRate: Float64(sampleRate),
            mFormatID: formatID,
            mFormatFlags: 0,
            mBytesPerPacket: 0,
            mFramesPerPacket: 1024,
            mBytesPerFrame: 0,
            mChannelsPerFrame: ch,
            mBitsPerChannel: 0,
            mReserved: 0
        )

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

        // Interleaved output for AudioConverter (simpler callback).
        var outputDesc = AudioStreamBasicDescription(
            mSampleRate: Float64(sampleRate),
            mFormatID: kAudioFormatLinearPCM,
            mFormatFlags: kAudioFormatFlagIsFloat | kAudioFormatFlagIsPacked,
            mBytesPerPacket: ch * 4,
            mFramesPerPacket: 1,
            mBytesPerFrame: ch * 4,
            mChannelsPerFrame: ch,
            mBitsPerChannel: 32,
            mReserved: 0
        )

        var conv: AudioConverterRef?
        var status = AudioConverterNew(&inputDesc, &outputDesc, &conv)
        guard status == noErr, let conv else {
            print("  [audio] AudioConverterNew failed: \(status)")
            return
        }

        if let asc = codecConfig, !asc.isEmpty {
            let cookie = buildAACMagicCookie(asc: asc)
            cookie.withUnsafeBytes { buf in
                status = AudioConverterSetProperty(
                    conv,
                    kAudioConverterDecompressionMagicCookie,
                    UInt32(buf.count),
                    buf.baseAddress!)
            }
            if status != noErr {
                asc.withUnsafeBytes { buf in
                    _ = AudioConverterSetProperty(
                        conv,
                        kAudioConverterDecompressionMagicCookie,
                        UInt32(buf.count),
                        buf.baseAddress!)
                }
            }
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

    func decode(payload: Data, presentationTimeUS: Int64,
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

    func flush() {
        playerNode?.stop()
        playerNode?.play()
    }

    func teardown() {
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

    private func buildAACMagicCookie(asc: Data) -> Data {
        let ascLen = asc.count
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
