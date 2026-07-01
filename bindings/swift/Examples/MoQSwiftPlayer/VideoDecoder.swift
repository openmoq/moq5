import AVFoundation
import CoreMedia
import Foundation
import MoQ
import MoQMedia

enum VideoDecoderError: Error, CustomStringConvertible {
    case malformedAVCC(String)
    case formatDescriptionFailed(OSStatus)
    case blockBufferFailed(OSStatus)
    case sampleBufferFailed(OSStatus)

    var description: String {
        switch self {
        case .malformedAVCC(let d): return "Malformed avcC: \(d)"
        case .formatDescriptionFailed(let s): return "CMVideoFormatDescription failed: \(s)"
        case .blockBufferFailed(let s): return "CMBlockBuffer failed: \(s)"
        case .sampleBufferFailed(let s): return "CMSampleBuffer failed: \(s)"
        }
    }
}

func createH264FormatDescription(
    avccData: Data
) throws -> CMVideoFormatDescription {
    guard avccData.count >= 7, avccData[avccData.startIndex] == 1 else {
        throw VideoDecoderError.malformedAVCC("too short or wrong version")
    }

    var paramSets: [[UInt8]] = []
    var offset = 5

    let numSPS = Int(avccData[avccData.startIndex + offset] & 0x1F)
    offset += 1
    for _ in 0..<numSPS {
        guard offset + 2 <= avccData.count else {
            throw VideoDecoderError.malformedAVCC("SPS length truncated")
        }
        let len = Int(avccData[avccData.startIndex + offset]) << 8
            | Int(avccData[avccData.startIndex + offset + 1])
        offset += 2
        guard offset + len <= avccData.count else {
            throw VideoDecoderError.malformedAVCC("SPS data truncated")
        }
        paramSets.append(Array(avccData[
            (avccData.startIndex + offset)..<(avccData.startIndex + offset + len)]))
        offset += len
    }

    guard offset < avccData.count else {
        throw VideoDecoderError.malformedAVCC("no PPS count")
    }
    let numPPS = Int(avccData[avccData.startIndex + offset])
    offset += 1
    for _ in 0..<numPPS {
        guard offset + 2 <= avccData.count else {
            throw VideoDecoderError.malformedAVCC("PPS length truncated")
        }
        let len = Int(avccData[avccData.startIndex + offset]) << 8
            | Int(avccData[avccData.startIndex + offset + 1])
        offset += 2
        guard offset + len <= avccData.count else {
            throw VideoDecoderError.malformedAVCC("PPS data truncated")
        }
        paramSets.append(Array(avccData[
            (avccData.startIndex + offset)..<(avccData.startIndex + offset + len)]))
        offset += len
    }

    guard !paramSets.isEmpty else {
        throw VideoDecoderError.malformedAVCC("no parameter sets")
    }

    let nalLengthSize: Int32 = Int32((avccData[avccData.startIndex + 4] & 0x03) + 1)

    var formatDesc: CMVideoFormatDescription?
    let status: OSStatus = paramSets.withUnsafeBufferPointers { ptrs, sizes in
        CMVideoFormatDescriptionCreateFromH264ParameterSets(
            allocator: nil,
            parameterSetCount: paramSets.count,
            parameterSetPointers: ptrs,
            parameterSetSizes: sizes,
            nalUnitHeaderLength: nalLengthSize,
            formatDescriptionOut: &formatDesc)
    }

    guard status == noErr, let desc = formatDesc else {
        throw VideoDecoderError.formatDescriptionFailed(status)
    }
    return desc
}

/// Create a CMSampleBuffer for one video frame from CMAF mdat bytes.
/// Timestamps are rebased: pass DTS/PTS relative to stream start.
func createSampleBuffer(
    data: Data,
    formatDescription: CMVideoFormatDescription,
    decodeTimeUS: Int64,
    presentationTimeUS: Int64,
    durationUS: UInt32,
    isKeyframe: Bool
) throws -> CMSampleBuffer {
    let timescale: CMTimeScale = 1_000_000

    var blockBuffer: CMBlockBuffer?
    let blockStatus = data.withUnsafeBytes { buf -> OSStatus in
        var bb: CMBlockBuffer?
        let s1 = CMBlockBufferCreateWithMemoryBlock(
            allocator: nil, memoryBlock: nil,
            blockLength: buf.count, blockAllocator: nil,
            customBlockSource: nil, offsetToData: 0,
            dataLength: buf.count, flags: 0,
            blockBufferOut: &bb)
        guard s1 == noErr, let block = bb else { return s1 }
        let s2 = CMBlockBufferReplaceDataBytes(
            with: buf.baseAddress!, blockBuffer: block,
            offsetIntoDestination: 0, dataLength: buf.count)
        if s2 == noErr { blockBuffer = block }
        return s2
    }
    guard blockStatus == noErr, let bb = blockBuffer else {
        throw VideoDecoderError.blockBufferFailed(blockStatus)
    }

    var timing = CMSampleTimingInfo(
        duration: CMTime(value: CMTimeValue(durationUS), timescale: timescale),
        presentationTimeStamp: CMTime(
            value: CMTimeValue(presentationTimeUS), timescale: timescale),
        decodeTimeStamp: CMTime(
            value: CMTimeValue(decodeTimeUS), timescale: timescale))

    var sampleSize = data.count

    var sampleBuffer: CMSampleBuffer?
    let sbStatus = CMSampleBufferCreateReady(
        allocator: nil, dataBuffer: bb,
        formatDescription: formatDescription,
        sampleCount: 1,
        sampleTimingEntryCount: 1, sampleTimingArray: &timing,
        sampleSizeEntryCount: 1, sampleSizeArray: &sampleSize,
        sampleBufferOut: &sampleBuffer)

    guard sbStatus == noErr, let sb = sampleBuffer else {
        throw VideoDecoderError.sampleBufferFailed(sbStatus)
    }

    let attachments = CMSampleBufferGetSampleAttachmentsArray(
        sb, createIfNecessary: true)! as NSArray
    if let dict = attachments.firstObject as? NSMutableDictionary {
        dict[kCMSampleAttachmentKey_NotSync] = !isKeyframe
    }

    return sb
}

extension Array where Element == [UInt8] {
    func withUnsafeBufferPointers<R>(
        _ body: (UnsafePointer<UnsafePointer<UInt8>>,
                 UnsafePointer<Int>) -> R
    ) -> R {
        // Flatten all parameter sets into one contiguous buffer
        // so a single withUnsafeBufferPointer scope covers all data.
        var flat: [UInt8] = []
        var offsets: [(offset: Int, length: Int)] = []
        for arr in self {
            offsets.append((offset: flat.count, length: arr.count))
            flat.append(contentsOf: arr)
        }
        return flat.withUnsafeBufferPointer { flatBuf in
            let base = flatBuf.baseAddress!
            var ptrs = offsets.map { UnsafePointer(base + $0.offset) }
            var sizes = offsets.map { $0.length }
            return ptrs.withUnsafeMutableBufferPointer { pBuf in
                sizes.withUnsafeMutableBufferPointer { sBuf in
                    body(pBuf.baseAddress!, sBuf.baseAddress!)
                }
            }
        }
    }
}
