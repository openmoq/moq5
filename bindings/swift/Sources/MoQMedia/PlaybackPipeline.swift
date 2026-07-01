import CMoQPlayback
import CMoQCore
import Foundation
import MoQ

// MARK: - Playback Track Handle

public struct PlaybackTrack: Sendable, Hashable {
    internal let raw: moq_playback_track_t

    internal init(_ raw: moq_playback_track_t) {
        self.raw = raw
    }

    public static func == (lhs: PlaybackTrack, rhs: PlaybackTrack) -> Bool {
        lhs.raw._opaque == rhs.raw._opaque
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(raw._opaque)
    }
}

// MARK: - Track Configuration

public struct PlaybackTrackConfiguration: Sendable {
    public var mediaType: MediaType
    public var packaging: MediaPackaging
    public var codec: String?
    public var initData: Data?
    public var timescale: UInt32
    public var width: UInt32
    public var height: UInt32
    public var samplerate: UInt32
    public var channelCount: UInt32
    public var targetLatencyUS: UInt64
    public var isLive: Bool

    public init(
        mediaType: MediaType,
        packaging: MediaPackaging,
        codec: String? = nil,
        initData: Data? = nil,
        timescale: UInt32 = 0,
        width: UInt32 = 0,
        height: UInt32 = 0,
        samplerate: UInt32 = 0,
        channelCount: UInt32 = 0,
        targetLatencyUS: UInt64 = 0,
        isLive: Bool = true
    ) {
        self.mediaType = mediaType
        self.packaging = packaging
        self.codec = codec
        self.initData = initData
        self.timescale = timescale
        self.width = width
        self.height = height
        self.samplerate = samplerate
        self.channelCount = channelCount
        self.targetLatencyUS = targetLatencyUS
        self.isLive = isLive
    }
}

// MARK: - Reset Reason

public enum PlaybackResetReason: Sendable, Hashable {
    case gap
    case decodeError
    case trackSwitch
    case configChange
    case unknown(UInt32)
}

// MARK: - Drop Reason

public enum PlaybackDropReason: Sendable, Hashable {
    case malformedLOC
    case malformedCMAF
    case missingTimestamp
    case nonMonotonicDTS
    case unsupportedMultiSample
    case stale
    case keyframeWait
    case unknown(UInt32)
}

// MARK: - Commands

public enum PlaybackCommand {
    case configureVideo(
        track: PlaybackTrack,
        codec: Buffer, codecConfig: Buffer,
        width: UInt32, height: UInt32)
    case configureAudio(
        track: PlaybackTrack,
        codec: Buffer, codecConfig: Buffer,
        samplerate: UInt32, channelCount: UInt32)
    case decodeVideo(
        track: PlaybackTrack,
        groupID: UInt64, objectID: UInt64,
        decodeTimeUS: UInt64, compositionOffsetUS: Int64,
        presentationTimeUS: UInt64,
        hasCaptureTime: Bool, captureTimeUS: UInt64,
        keyframe: Bool, payload: Buffer)
    case decodeCMAF(
        track: PlaybackTrack,
        groupID: UInt64, objectID: UInt64,
        decodeTimeUS: UInt64, compositionOffsetUS: Int64,
        presentationTimeUS: UInt64,
        hasCaptureTime: Bool, captureTimeUS: UInt64,
        keyframe: Bool, sampleDurationUS: UInt32,
        fragment: Buffer, mdatRange: Range<Int>)
    case decodeAudio(
        track: PlaybackTrack,
        groupID: UInt64, objectID: UInt64,
        decodeTimeUS: UInt64, compositionOffsetUS: Int64,
        presentationTimeUS: UInt64,
        hasCaptureTime: Bool, captureTimeUS: UInt64,
        sampleDurationUS: UInt32,
        payload: Buffer, mdatRange: Range<Int>)
    case reset(track: PlaybackTrack, reason: PlaybackResetReason)
    case unknown(UInt32)
}

extension PlaybackCommand {
    /// Decoded media bytes: mdat slice for CMAF, full payload for RAW.
    /// Returns nil for configure, reset, and unknown commands.
    public var mediaData: Data? {
        switch self {
        case .decodeCMAF(_, _, _, _, _, _, _, _, _, _, let frag, let mdat):
            return frag.sliceData(mdat)
        case .decodeVideo(_, _, _, _, _, _, _, _, _, let payload):
            return payload.copyBytes()
        case .decodeAudio(_, _, _, _, _, _, _, _, _, let payload, let mdat):
            return mdat.isEmpty ? payload.copyBytes() : payload.sliceData(mdat)
        default:
            return nil
        }
    }

    /// Codec string from configure commands, nil otherwise.
    public var codecString: String? {
        switch self {
        case .configureVideo(_, let codec, _, _, _),
             .configureAudio(_, let codec, _, _, _):
            return String(data: codec.copyBytes(), encoding: .utf8)
        default:
            return nil
        }
    }

    /// Codec config bytes from configure commands, nil otherwise.
    public var codecConfigData: Data? {
        switch self {
        case .configureVideo(_, _, let cfg, _, _),
             .configureAudio(_, _, let cfg, _, _):
            return cfg.copyBytes()
        default:
            return nil
        }
    }
}

// MARK: - Media Time Rebaser

/// Tracks the first timestamp seen and rebases subsequent values
/// relative to it. Useful for converting absolute pipeline DTS/PTS
/// to zero-based presentation times.
public struct MediaTimeRebaser: Sendable {
    private var base: UInt64?

    public init() {}

    /// Record a timestamp and return the rebased (relative) value.
    /// The first call establishes the base and returns 0.
    /// Deltas that exceed Int64 range are saturated.
    public mutating func rebase(_ timeUS: UInt64) -> Int64 {
        if base == nil { base = timeUS }
        let b = base!
        if timeUS >= b {
            let delta = timeUS - b
            return delta <= UInt64(Int64.max) ? Int64(delta) : Int64.max
        } else {
            let delta = b - timeUS
            return delta <= UInt64(Int64.max) ? -Int64(delta) : Int64.min
        }
    }

    /// Clear the base timestamp.
    public mutating func reset() {
        base = nil
    }

    /// Whether a base timestamp has been established.
    public var isEstablished: Bool { base != nil }
}

// MARK: - Events

public enum PlaybackEvent: Sendable, Hashable {
    case gapDetected(track: PlaybackTrack, groupID: UInt64)
    case keyframeWaiting(track: PlaybackTrack)
    case skipForward(track: PlaybackTrack, fromGroup: UInt64, toGroup: UInt64)
    case objectDropped(track: PlaybackTrack, reason: PlaybackDropReason,
                       groupID: UInt64, objectID: UInt64)
    case trackEnded(track: PlaybackTrack)
    case backlogShed(track: PlaybackTrack, droppedGroups: UInt32,
                     remainingGroups: UInt32)
    case partialGroupAbandoned(track: PlaybackTrack, fromGroup: UInt64,
                               toGroup: UInt64)
    case unknown(UInt32)
}

// MARK: - Pipeline Configuration

public struct PlaybackConfiguration: Sendable {
    public var maxTracks: UInt32
    public var maxBufferedObjects: UInt32
    public var maxBufferedBytes: UInt64
    public var maxBacklogGroups: UInt32
    public var maxCommands: UInt32
    public var maxEvents: UInt32
    public var gapTimeoutUS: UInt64
    public var maxReleasePerTick: UInt32
    public var maxSamplesPerObject: UInt32
    public var ignoreEOGBit: Bool
    public var inferEndOfGroupFromNextGroup: Bool

    public init(
        maxTracks: UInt32 = 4,
        maxBufferedObjects: UInt32 = 256,
        maxBufferedBytes: UInt64 = 16 * 1024 * 1024,
        maxBacklogGroups: UInt32 = 3,
        maxCommands: UInt32 = 64,
        maxEvents: UInt32 = 16,
        gapTimeoutUS: UInt64 = 500_000,
        maxReleasePerTick: UInt32 = 0,
        maxSamplesPerObject: UInt32 = 16,
        ignoreEOGBit: Bool = false,
        inferEndOfGroupFromNextGroup: Bool = false
    ) {
        self.maxTracks = maxTracks
        self.maxBufferedObjects = maxBufferedObjects
        self.maxBufferedBytes = maxBufferedBytes
        self.maxBacklogGroups = maxBacklogGroups
        self.maxCommands = maxCommands
        self.maxEvents = maxEvents
        self.gapTimeoutUS = gapTimeoutUS
        self.maxReleasePerTick = maxReleasePerTick
        self.maxSamplesPerObject = maxSamplesPerObject
        self.ignoreEOGBit = ignoreEOGBit
        self.inferEndOfGroupFromNextGroup = inferEndOfGroupFromNextGroup
    }

    public static var live: PlaybackConfiguration {
        PlaybackConfiguration(gapTimeoutUS: 2_000_000)
    }

    public static var liveCMAF: PlaybackConfiguration {
        PlaybackConfiguration(
            maxBufferedObjects: 4096,
            maxBufferedBytes: 128 * 1024 * 1024,
            maxBacklogGroups: 30,
            maxCommands: 4096,
            maxEvents: 256,
            gapTimeoutUS: 5_000_000,
            maxReleasePerTick: 0,
            maxSamplesPerObject: 512,
            ignoreEOGBit: true,
            inferEndOfGroupFromNextGroup: true)
    }
}

// MARK: - Pipeline

public final class PlaybackPipeline {
    private let raw: OpaquePointer // moq_playback_t*

    public init(configuration: PlaybackConfiguration = .live) throws {
        var cfg = moq_playback_cfg_t()
        moq_playback_cfg_init(&cfg)
        cfg.max_tracks = configuration.maxTracks
        cfg.max_buffered_objects = configuration.maxBufferedObjects
        cfg.max_buffered_bytes = configuration.maxBufferedBytes
        cfg.max_backlog_groups = configuration.maxBacklogGroups
        cfg.max_commands = configuration.maxCommands
        cfg.max_events = configuration.maxEvents
        cfg.gap_timeout_us = configuration.gapTimeoutUS
        cfg.max_release_per_tick = configuration.maxReleasePerTick
        cfg.max_samples_per_object = configuration.maxSamplesPerObject
        cfg.ignore_eog_bit = configuration.ignoreEOGBit
        cfg.infer_end_of_group_from_next_group =
            configuration.inferEndOfGroupFromNextGroup

        var ptr: OpaquePointer?
        let alloc = moq_alloc_default()!
        try MoQError.check(moq_playback_create(alloc, &cfg, &ptr))
        guard let p = ptr else { throw MoQError.internal }
        self.raw = p
    }

    deinit {
        moq_playback_destroy(raw)
    }

    // MARK: - Track Management

    @discardableResult
    public func addTrack(
        _ config: PlaybackTrackConfiguration
    ) throws -> PlaybackTrack {
        var codecBytes = config.codec.map { Array($0.utf8) } ?? []
        var initBytes = config.initData.map { Array($0) } ?? []

        return try codecBytes.withUnsafeMutableBufferPointer { codecPtr in
            try initBytes.withUnsafeMutableBufferPointer { initPtr in
                var cfg = moq_playback_track_cfg_t()
                moq_playback_track_cfg_init(&cfg)
                cfg.media_type = config.mediaType == .video
                    ? MOQ_PLAYBACK_MEDIA_VIDEO : MOQ_PLAYBACK_MEDIA_AUDIO
                cfg.packaging = config.packaging == .raw
                    ? MOQ_PLAYBACK_PACKAGING_RAW : MOQ_PLAYBACK_PACKAGING_CMAF
                cfg.codec = moq_bytes_t(
                    data: codecPtr.baseAddress, len: codecPtr.count)
                cfg.init_data = moq_bytes_t(
                    data: initPtr.baseAddress, len: initPtr.count)
                cfg.timescale = config.timescale
                cfg.width = config.width
                cfg.height = config.height
                cfg.samplerate = config.samplerate
                cfg.channel_count = config.channelCount
                cfg.target_latency_us = config.targetLatencyUS
                cfg.is_live = config.isLive

                var handle = moq_playback_track_t(_opaque: 0)
                try MoQError.check(
                    moq_playback_add_track(raw, &cfg, &handle))
                return PlaybackTrack(handle)
            }
        }
    }

    public func removeTrack(_ track: PlaybackTrack) throws {
        try MoQError.check(moq_playback_remove_track(raw, track.raw))
    }

    // MARK: - Push

    public func push(
        object: FacadeReceivedObject,
        track: PlaybackTrack,
        now: UInt64
    ) throws {
        var obj = moq_playback_object_t()
        moq_playback_object_init(&obj)
        obj.track = track.raw
        obj.group_id = object.groupID
        obj.subgroup_id = object.subgroupID
        obj.object_id = object.objectID
        obj.publisher_priority = object.publisherPriority
        obj.status = object.status.cValue
        obj.end_of_group = object.endOfGroup
        obj.datagram = object.isDatagram
        obj.payload = object.payload?.raw
        obj.properties = object.properties?.raw
        try MoQError.check(moq_playback_push_object(raw, &obj, now))
    }

    // MARK: - Tick

    public func tick(now: UInt64) throws {
        try MoQError.check(moq_playback_tick(raw, now))
    }

    // MARK: - Commands

    public func pollCommand() throws -> PlaybackCommand? {
        var cmd = moq_playback_cmd_t()
        let rc = moq_playback_poll_command(raw, &cmd)
        if rc == MOQ_DONE { return nil }
        try MoQError.check(rc)

        let track = PlaybackTrack(cmd.track)
        let result: PlaybackCommand

        switch cmd.kind {
        case MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO:
            let cv = cmd.u.configure_video
            let codec: Buffer
            let config: Buffer
            if let p = cv.codec {
                cmd.u.configure_video.codec = nil
                codec = Buffer(adopting: p)
            } else { codec = try Buffer(Data()) }
            if let p = cv.codec_config {
                cmd.u.configure_video.codec_config = nil
                config = Buffer(adopting: p)
            } else { config = try Buffer(Data()) }
            result = .configureVideo(
                track: track, codec: codec, codecConfig: config,
                width: cv.width, height: cv.height)

        case MOQ_PLAYBACK_CMD_CONFIGURE_AUDIO:
            let ca = cmd.u.configure_audio
            let codec: Buffer
            let config: Buffer
            if let p = ca.codec {
                cmd.u.configure_audio.codec = nil
                codec = Buffer(adopting: p)
            } else { codec = try Buffer(Data()) }
            if let p = ca.codec_config {
                cmd.u.configure_audio.codec_config = nil
                config = Buffer(adopting: p)
            } else { config = try Buffer(Data()) }
            result = .configureAudio(
                track: track, codec: codec, codecConfig: config,
                samplerate: ca.samplerate,
                channelCount: ca.channel_count)

        case MOQ_PLAYBACK_CMD_DECODE_VIDEO:
            let dv = cmd.u.decode_video
            let payload: Buffer
            if let p = dv.payload {
                cmd.u.decode_video.payload = nil
                payload = Buffer(adopting: p)
            } else { payload = try Buffer(Data()) }
            result = .decodeVideo(
                track: track,
                groupID: dv.group_id, objectID: dv.object_id,
                decodeTimeUS: dv.decode_time_us,
                compositionOffsetUS: dv.composition_offset_us,
                presentationTimeUS: dv.presentation_time_us,
                hasCaptureTime: dv.has_capture_time,
                captureTimeUS: dv.capture_time_us,
                keyframe: dv.keyframe,
                payload: payload)

        case MOQ_PLAYBACK_CMD_DECODE_CMAF:
            let dc = cmd.u.decode_cmaf
            let fragment: Buffer
            if let p = dc.fragment {
                cmd.u.decode_cmaf.fragment = nil
                fragment = Buffer(adopting: p)
            } else { fragment = try Buffer(Data()) }
            result = .decodeCMAF(
                track: track,
                groupID: dc.group_id, objectID: dc.object_id,
                decodeTimeUS: dc.decode_time_us,
                compositionOffsetUS: dc.composition_offset_us,
                presentationTimeUS: dc.presentation_time_us,
                hasCaptureTime: dc.has_capture_time,
                captureTimeUS: dc.capture_time_us,
                keyframe: dc.keyframe,
                sampleDurationUS: dc.sample_duration_us,
                fragment: fragment,
                mdatRange: dc.mdat_offset ..< (dc.mdat_offset + dc.mdat_len))

        case MOQ_PLAYBACK_CMD_DECODE_AUDIO:
            let da = cmd.u.decode_audio
            let payload: Buffer
            if let p = da.payload {
                cmd.u.decode_audio.payload = nil
                payload = Buffer(adopting: p)
            } else { payload = try Buffer(Data()) }
            let mdatEnd = da.mdat_offset + da.mdat_len
            result = .decodeAudio(
                track: track,
                groupID: da.group_id, objectID: da.object_id,
                decodeTimeUS: da.decode_time_us,
                compositionOffsetUS: da.composition_offset_us,
                presentationTimeUS: da.presentation_time_us,
                hasCaptureTime: da.has_capture_time,
                captureTimeUS: da.capture_time_us,
                sampleDurationUS: da.sample_duration_us,
                payload: payload,
                mdatRange: da.mdat_offset ..< mdatEnd)

        case MOQ_PLAYBACK_CMD_RESET:
            let reason: PlaybackResetReason
            switch cmd.u.reset.reason {
            case MOQ_PLAYBACK_RESET_GAP:           reason = .gap
            case MOQ_PLAYBACK_RESET_DECODE_ERROR:  reason = .decodeError
            case MOQ_PLAYBACK_RESET_TRACK_SWITCH:  reason = .trackSwitch
            case MOQ_PLAYBACK_RESET_CONFIG_CHANGE: reason = .configChange
            default: reason = .unknown(cmd.u.reset.reason.rawValue)
            }
            result = .reset(track: track, reason: reason)

        default:
            result = .unknown(cmd.kind.rawValue)
        }

        moq_playback_cmd_cleanup(&cmd)
        return result
    }

    public func drainCommands(
        _ body: (PlaybackCommand) throws -> Void
    ) throws {
        while let cmd = try pollCommand() {
            try body(cmd)
        }
    }

    // MARK: - Events

    public func pollEvent() throws -> PlaybackEvent? {
        var evt = moq_playback_event_t()
        let rc = moq_playback_poll_event(raw, &evt)
        if rc == MOQ_DONE { return nil }
        try MoQError.check(rc)

        let track = PlaybackTrack(evt.track)

        switch evt.kind {
        case MOQ_PLAYBACK_EVENT_GAP_DETECTED:
            return .gapDetected(
                track: track, groupID: evt.u.gap_detected.group_id)
        case MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING:
            return .keyframeWaiting(track: track)
        case MOQ_PLAYBACK_EVENT_SKIP_FORWARD:
            return .skipForward(
                track: track,
                fromGroup: evt.u.skip_forward.from_group_id,
                toGroup: evt.u.skip_forward.to_group_id)
        case MOQ_PLAYBACK_EVENT_OBJECT_DROPPED:
            let dr: PlaybackDropReason
            switch evt.u.object_dropped.reason {
            case MOQ_PLAYBACK_DROP_MALFORMED_LOC:            dr = .malformedLOC
            case MOQ_PLAYBACK_DROP_MALFORMED_CMAF:           dr = .malformedCMAF
            case MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP:        dr = .missingTimestamp
            case MOQ_PLAYBACK_DROP_NON_MONOTONIC_DTS:        dr = .nonMonotonicDTS
            case MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE: dr = .unsupportedMultiSample
            case MOQ_PLAYBACK_DROP_STALE:                    dr = .stale
            case MOQ_PLAYBACK_DROP_KEYFRAME_WAIT:            dr = .keyframeWait
            default: dr = .unknown(evt.u.object_dropped.reason.rawValue)
            }
            return .objectDropped(
                track: track, reason: dr,
                groupID: evt.u.object_dropped.group_id,
                objectID: evt.u.object_dropped.object_id)
        case MOQ_PLAYBACK_EVENT_TRACK_ENDED:
            return .trackEnded(track: track)
        case MOQ_PLAYBACK_EVENT_BACKLOG_SHED:
            return .backlogShed(
                track: track,
                droppedGroups: evt.u.backlog_shed.dropped_groups,
                remainingGroups: evt.u.backlog_shed.remaining_groups)
        case MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED:
            return .partialGroupAbandoned(
                track: track,
                fromGroup: evt.u.partial_group_abandoned.from_group_id,
                toGroup: evt.u.partial_group_abandoned.to_group_id)
        default:
            return .unknown(evt.kind.rawValue)
        }
    }

    public func drainEvents(
        _ body: (PlaybackEvent) throws -> Void
    ) throws {
        while let evt = try pollEvent() {
            try body(evt)
        }
    }
}
