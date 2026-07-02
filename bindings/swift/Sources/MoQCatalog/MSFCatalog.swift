import Foundation

// MARK: - MSF Catalog

/// MSF (Media over QUIC Streaming Format) catalog per draft-ietf-moq-msf-01.
///
/// `version` is kept as an `Int` (the in-memory MSF version, 1). On the wire,
/// MSF-01 §5.1.1 encodes `version` as a JSON **String**: decoding accepts the
/// canonical `"1"`, the draft convention `"draft-01"`, and — for legacy
/// compatibility only — the MSF-00 numeric `1`; encoding always emits the
/// canonical String `"1"`.
public struct MSFCatalog: Sendable, Equatable {
    /// The supported MSF version (the value 1; §5.1.1).
    public static let supportedVersion = 1

    public var version: Int
    public var generatedAt: UInt64?
    public var tracks: [MSFTrack]
    /// Broadcast completion (MSF-01 §5.1.3): only the TRUE state is meaningful
    /// (the spec forbids emitting `isComplete:false`), so this is a plain Bool
    /// where false means "absent". Encoded only when true.
    public var isComplete: Bool
    /// Root Initialization Data List (MSF-01 §5.1.7). Tracks reference entries
    /// by `initRef`. nil = absent (omitted on encode).
    public var initDataList: [MSFInitDataEntry]?
    /// Root content protections (CMSF §4.1.1). Tracks reference entries by
    /// `contentProtectionRefIDs`. nil = absent (omitted on encode).
    public var contentProtections: [CMSFContentProtection]?
    /// Delta update operations (MSF-01 §5.1.6). When non-nil/non-empty this is a
    /// DELTA catalog: it encodes only `generatedAt` + `deltaUpdate` (no version,
    /// tracks, isComplete, initDataList, or contentProtections), matching the C
    /// model. nil = an independent catalog (the default).
    public var deltaUpdate: [MSFDeltaUpdate]?

    public init(
        version: Int = 1,
        generatedAt: UInt64? = nil,
        tracks: [MSFTrack] = [],
        isComplete: Bool = false,
        initDataList: [MSFInitDataEntry]? = nil,
        contentProtections: [CMSFContentProtection]? = nil,
        deltaUpdate: [MSFDeltaUpdate]? = nil
    ) {
        self.version = version
        self.generatedAt = generatedAt
        self.tracks = tracks
        self.isComplete = isComplete
        self.initDataList = initDataList
        self.contentProtections = contentProtections
        self.deltaUpdate = deltaUpdate
    }
}

extension MSFCatalog: Encodable {
    enum CodingKeys: String, CodingKey {
        case version, generatedAt, tracks, isComplete, initDataList
        case contentProtections, deltaUpdate
    }

    public func encode(to encoder: any Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        // MSF-01 §5.1.6/§5.3: a DELTA catalog carries only generatedAt +
        // deltaUpdate -- never version, tracks, isComplete, initDataList, or
        // contentProtections. Emit that shape and return before the independent
        // catalog path below.
        if let delta = deltaUpdate, !delta.isEmpty {
            try c.encodeIfPresent(generatedAt, forKey: .generatedAt)
            try c.encode(delta, forKey: .deltaUpdate)
            return
        }
        // MSF-01 §5.1.1: version is a JSON String; emit the canonical "1".
        // Refuse to emit an unsupported version — a §5.1.1-conformant subscriber
        // (this binding included) would reject the wire we'd produce.
        guard version == Self.supportedVersion else {
            throw EncodingError.invalidValue(version, EncodingError.Context(
                codingPath: c.codingPath + [CodingKeys.version],
                debugDescription: "Unsupported MSF version \(version); only \(Self.supportedVersion) is supported (§5.1.1)"))
        }
        try c.encode(String(version), forKey: .version)
        try c.encodeIfPresent(generatedAt, forKey: .generatedAt)
        try c.encode(tracks, forKey: .tracks)
        // §5.1.3: only the TRUE state is emitted; false means "absent".
        if isComplete { try c.encode(true, forKey: .isComplete) }
        try c.encodeIfPresent(initDataList, forKey: .initDataList)
        try c.encodeIfPresent(contentProtections, forKey: .contentProtections)
    }
}

extension MSFCatalog: Decodable {
    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        // §5.1.6/§5.3: a delta catalog is identified by the deltaUpdate key and
        // carries no version/tracks/isComplete. Decode that shape strictly for
        // the ops (an unknown op or non-array throws) without requiring the
        // independent-catalog fields. version defaults to the in-memory 1.
        if c.contains(.deltaUpdate) {
            // §5.1.6/§5.3: a delta MUST NOT carry version, tracks, or isComplete
            // and MUST hold at least one op. This mirrors the C MSF parser
            // (media/msf/src/msf.c) so the two decoders agree on what is a valid
            // delta. (initDataList/contentProtections are tolerated-but-ignored
            // here, also matching C: "a delta carries no tracks/idl/cp".)
            if c.contains(.version) || c.contains(.tracks) ||
               c.contains(.isComplete) {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: c.codingPath + [CodingKeys.deltaUpdate],
                    debugDescription:
                        "delta catalog MUST NOT carry version, tracks, or "
                        + "isComplete (§5.1.6/§5.3)"))
            }
            let ops = try c.decode([MSFDeltaUpdate].self, forKey: .deltaUpdate)
            if ops.isEmpty {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: c.codingPath + [CodingKeys.deltaUpdate],
                    debugDescription:
                        "delta catalog MUST carry at least one operation (§5.1.6)"))
            }
            deltaUpdate = ops
            version = Self.supportedVersion
            generatedAt = Self.lenientUInt64(c, .generatedAt)
            tracks = []
            isComplete = false
            initDataList = nil
            contentProtections = nil
            return
        }
        deltaUpdate = nil
        version = try Self.decodeVersion(c)
        generatedAt = Self.lenientUInt64(c, .generatedAt)
        tracks = try c.decode([MSFTrack].self, forKey: .tracks)
        // §5.1.3: lenient — present-and-true => true, otherwise absent (false).
        isComplete = ((try? c.decodeIfPresent(Bool.self, forKey: .isComplete)) ?? nil) ?? false
        // §5.1.7: lenient — a malformed list (or entry) decodes as absent (nil).
        initDataList = try? c.decodeIfPresent([MSFInitDataEntry].self, forKey: .initDataList)
        // CMSF §4.1.1: lenient — a malformed array (or entry missing a required
        // nested field) decodes as absent (nil), consistent with initDataList.
        contentProtections =
            try? c.decodeIfPresent([CMSFContentProtection].self, forKey: .contentProtections)
    }

    /// MSF-01 §5.1.1: `version` is a JSON String. Accept the canonical `"1"` and
    /// the draft-XX convention for this draft (`"draft-01"`); also accept the
    /// legacy MSF-00 numeric `1` for back-compat. All map to the in-memory
    /// version 1. Reject anything else — per §5.1.1 a subscriber MUST NOT parse
    /// a catalog version it does not understand.
    private static func decodeVersion(
        _ c: KeyedDecodingContainer<CodingKeys>
    ) throws -> Int {
        if let s = try? c.decode(String.self, forKey: .version) {
            if s == "1" || s == "draft-01" { return supportedVersion }
            throw DecodingError.dataCorruptedError(
                forKey: .version, in: c,
                debugDescription: "Unsupported MSF version \"\(s)\" (§5.1.1)")
        }
        if let n = try? c.decode(Int.self, forKey: .version) {  // legacy MSF-00 numeric
            if n == supportedVersion { return n }
            throw DecodingError.dataCorruptedError(
                forKey: .version, in: c,
                debugDescription: "Unsupported MSF version \(n) (§5.1.1)")
        }
        throw DecodingError.dataCorruptedError(
            forKey: .version, in: c,
            debugDescription: "MSF version is required and must be a String or number (§5.1.1)")
    }

    private static func lenientUInt64(
        _ c: KeyedDecodingContainer<CodingKeys>, _ key: CodingKeys
    ) -> UInt64? {
        guard let v = try? c.decodeIfPresent(UInt64.self, forKey: key) else {
            return nil
        }
        return v
    }
}

// MARK: - MSF Init Data List

/// A root Initialization Data List entry (MSF-01 §5.1.7). A track references an
/// entry by `initRef` -> `id`. `data` is base64 for `type == "inline"` (the only
/// type in this MSF version). Model-only: values are carried verbatim (no
/// `type`/base64 validation), consistent with the lenient Swift parse stance.
public struct MSFInitDataEntry: Sendable, Equatable, Codable {
    public var id: String
    public var type: String
    public var data: String

    public init(id: String, type: String, data: String) {
        self.id = id
        self.type = type
        self.data = data
    }
}

// MARK: - CMSF Content Protection

/// A license / certificate / authorization URL object (CMSF §4.1.1.4.2–4.4):
/// `url` required, `type` optional. Model-only: URL reachability is not checked.
public struct CMSFURL: Sendable, Equatable, Codable {
    public var url: String
    public var type: String?

    public init(url: String, type: String? = nil) {
        self.url = url
        self.type = type
    }
}

/// A DRM system descriptor (CMSF §4.1.1.4). `systemID` is a UUID string on the
/// wire; the optional laURL/certURL/authURL/pssh/robustness fields are omitted
/// when nil. Model-only: UUID syntax, PSSH base64, robustness meaning, and DRM
/// policy are NOT interpreted here -- values are carried verbatim (the C encoder
/// enforces the §4 authoring syntax).
public struct CMSFDRMSystem: Sendable, Equatable, Codable {
    public var systemID: String
    public var laURL: CMSFURL?
    public var certURL: CMSFURL?
    public var authURL: CMSFURL?
    public var pssh: String?
    public var robustness: String?

    public init(
        systemID: String,
        laURL: CMSFURL? = nil,
        certURL: CMSFURL? = nil,
        authURL: CMSFURL? = nil,
        pssh: String? = nil,
        robustness: String? = nil
    ) {
        self.systemID = systemID
        self.laURL = laURL
        self.certURL = certURL
        self.authURL = authURL
        self.pssh = pssh
        self.robustness = robustness
    }
}

/// A root contentProtections entry (CMSF §4.1.1). Tracks reference it via
/// `contentProtectionRefIDs` -> `refID`. Model-only: scheme enum, UUID syntax,
/// and refID uniqueness are NOT validated in Swift (the C catalog encoder does).
public struct CMSFContentProtection: Sendable, Equatable, Codable {
    public var refID: String
    public var defaultKID: [String]
    public var scheme: String
    public var drmSystem: CMSFDRMSystem

    public init(
        refID: String,
        defaultKID: [String],
        scheme: String,
        drmSystem: CMSFDRMSystem
    ) {
        self.refID = refID
        self.defaultKID = defaultKID
        self.scheme = scheme
        self.drmSystem = drmSystem
    }
}

// MARK: - MSF Media Timeline Template

/// An inline media-timeline template on a media track (MSF-01 §5.2.15 / §7.4).
/// The wire form is a 6-element JSON **array** (not an object):
/// `[startMediaTime, deltaMediaTime, [startGroup, startObject],
///   [deltaGroup, deltaObject], startWallclock, deltaWallclock]` (all integers;
/// wallclocks are 0 for VOD). Elements 2 and 3 are 2-element location pairs.
public struct MSFMediaTemplate: Sendable, Equatable {
    public var startMediaTime: UInt64
    public var deltaMediaTime: UInt64
    public var startGroup: UInt64
    public var startObject: UInt64
    public var deltaGroup: UInt64
    public var deltaObject: UInt64
    public var startWallclock: UInt64
    public var deltaWallclock: UInt64

    public init(
        startMediaTime: UInt64, deltaMediaTime: UInt64,
        startGroup: UInt64, startObject: UInt64,
        deltaGroup: UInt64, deltaObject: UInt64,
        startWallclock: UInt64, deltaWallclock: UInt64
    ) {
        self.startMediaTime = startMediaTime
        self.deltaMediaTime = deltaMediaTime
        self.startGroup = startGroup
        self.startObject = startObject
        self.deltaGroup = deltaGroup
        self.deltaObject = deltaObject
        self.startWallclock = startWallclock
        self.deltaWallclock = deltaWallclock
    }
}

extension MSFMediaTemplate: Codable {
    public func encode(to encoder: any Encoder) throws {
        var a = encoder.unkeyedContainer()
        try a.encode(startMediaTime)
        try a.encode(deltaMediaTime)
        var loc1 = a.nestedUnkeyedContainer()
        try loc1.encode(startGroup)
        try loc1.encode(startObject)
        var loc2 = a.nestedUnkeyedContainer()
        try loc2.encode(deltaGroup)
        try loc2.encode(deltaObject)
        try a.encode(startWallclock)
        try a.encode(deltaWallclock)
    }

    /// Strict array decode -- a wrong length, wrong nesting, or non-integer
    /// element THROWS. Callers decode this as an optional via `try?`, so a
    /// present-but-malformed template becomes nil rather than failing the whole
    /// track (mirrors the C parser: a malformed template is rejected, §5.2.15).
    public init(from decoder: any Decoder) throws {
        var a = try decoder.unkeyedContainer()
        // MUST be exactly 6 elements (a.count is the array length when known).
        if let n = a.count, n != 6 {
            throw DecodingError.dataCorrupted(.init(
                codingPath: decoder.codingPath,
                debugDescription: "template MUST have exactly 6 elements (§5.2.15)"))
        }
        startMediaTime = try a.decode(UInt64.self)
        deltaMediaTime = try a.decode(UInt64.self)
        (startGroup, startObject) = try Self.decodeLocPair(&a)
        (deltaGroup, deltaObject) = try Self.decodeLocPair(&a)
        startWallclock = try a.decode(UInt64.self)
        deltaWallclock = try a.decode(UInt64.self)
        if !a.isAtEnd {
            throw DecodingError.dataCorrupted(.init(
                codingPath: decoder.codingPath,
                debugDescription: "template has more than 6 elements (§5.2.15)"))
        }
    }

    private static func decodeLocPair(
        _ a: inout UnkeyedDecodingContainer
    ) throws -> (UInt64, UInt64) {
        var loc = try a.nestedUnkeyedContainer()
        if let n = loc.count, n != 2 {
            throw DecodingError.dataCorrupted(.init(
                codingPath: loc.codingPath,
                debugDescription: "template location MUST be a 2-element [group, object] pair"))
        }
        let g = try loc.decode(UInt64.self)
        let o = try loc.decode(UInt64.self)
        if !loc.isAtEnd {
            throw DecodingError.dataCorrupted(.init(
                codingPath: loc.codingPath,
                debugDescription: "template location has more than 2 elements"))
        }
        return (g, o)
    }
}

// MARK: - MSF Delta Update

/// A deltaUpdate operation kind (MSF-01 §5.1.6). Decoding an unknown value
/// throws (rejecting a malformed delta), via the raw-value enum synthesis.
public enum MSFDeltaOperation: String, Sendable, Codable {
    case add, remove, clone
}

/// One ordered deltaUpdate operation: `{ "op": ..., "tracks": [...] }`.
public struct MSFDeltaUpdate: Sendable, Equatable, Codable {
    public var op: MSFDeltaOperation
    public var tracks: [MSFDeltaTrack]

    public init(op: MSFDeltaOperation, tracks: [MSFDeltaTrack]) {
        self.op = op
        self.tracks = tracks
    }

    enum CodingKeys: String, CodingKey { case op, tracks }

    // Custom decode (encode stays synthesized) so per-operation track shape is
    // validated at decode time, matching the C MSF parser's parse_track()
    // (media/msf/src/msf.c §5.1.6). An unknown op already throws via the
    // raw-value enum; here we additionally reject malformed valid-op payloads.
    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        op = try c.decode(MSFDeltaOperation.self, forKey: .op)  // unknown -> throws
        tracks = try c.decode([MSFDeltaTrack].self, forKey: .tracks)
        // §5.1.6: every op MUST carry at least one track (matches C msf.c:571).
        if tracks.isEmpty {
            throw Self.malformed(c, "deltaUpdate op MUST carry at least one track (§5.1.6)")
        }
        for t in tracks {
            // §5.2.33/34: parentName/parentNamespace MAY appear only in a clone.
            if op != .clone && (t.parentName != nil || t.parentNamespace != nil) {
                throw Self.malformed(
                    c, "parentName/parentNamespace are only valid on a clone op "
                       + "(§5.2.33/34)")
            }
            switch op {
            case .add:
                // §5.1.6/§5.6.4: only name is required (packaging MAY be omitted).
                break
            case .clone:
                // §5.1.6: clone requires parentName (and name, already required).
                if t.parentName == nil {
                    throw Self.malformed(c, "clone op requires parentName (§5.1.6)")
                }
            case .remove:
                // §5.1.6: remove MUST carry only name (+optional namespace).
                if !t.isNameNamespaceOnly {
                    throw Self.malformed(
                        c, "remove op MUST carry only name and optional namespace "
                           + "(§5.1.6)")
                }
            }
        }
    }

    private static func malformed(
        _ c: KeyedDecodingContainer<CodingKeys>, _ msg: String
    ) -> DecodingError {
        DecodingError.dataCorrupted(.init(
            codingPath: c.codingPath + [CodingKeys.tracks],
            debugDescription: msg))
    }
}

/// Shared lenient-decode helpers, generic over the CodingKey type, so the delta
/// partial-track model reuses the same "malformed optional => nil" policy as
/// `MSFTrack` without duplicating the clamp logic.
private enum MSFLenient {
    static func string<K: CodingKey>(_ c: KeyedDecodingContainer<K>, _ k: K) -> String? {
        try? c.decodeIfPresent(String.self, forKey: k)
    }
    static func bool<K: CodingKey>(_ c: KeyedDecodingContainer<K>, _ k: K) -> Bool? {
        try? c.decodeIfPresent(Bool.self, forKey: k)
    }
    static func u32<K: CodingKey>(_ c: KeyedDecodingContainer<K>, _ k: K) -> UInt32? {
        try? c.decodeIfPresent(UInt32.self, forKey: k)
    }
    static func u64<K: CodingKey>(_ c: KeyedDecodingContainer<K>, _ k: K) -> UInt64? {
        try? c.decodeIfPresent(UInt64.self, forKey: k)
    }
    static func cint<K: CodingKey>(_ c: KeyedDecodingContainer<K>, _ k: K) -> Int? {
        guard let v = try? c.decodeIfPresent(Int.self, forKey: k) else { return nil }
        guard v >= Int(Int32.min), v <= Int(Int32.max) else { return nil }
        return v
    }
    static let maxFramerate = Double(UInt64.max) / 1000
    static func framerate<K: CodingKey>(_ c: KeyedDecodingContainer<K>, _ k: K) -> Double? {
        guard let v = try? c.decodeIfPresent(Double.self, forKey: k) else { return nil }
        guard v.isFinite, v >= 0, v <= maxFramerate else { return nil }
        return v
    }
    static func strings<K: CodingKey>(_ c: KeyedDecodingContainer<K>, _ k: K) -> [String]? {
        try? c.decodeIfPresent([String].self, forKey: k)
    }
    static func encodeCInt<K: CodingKey>(
        _ c: inout KeyedEncodingContainer<K>, _ v: Int?, _ k: K
    ) throws {
        guard let v = v, v >= Int(Int32.min), v <= Int(Int32.max) else { return }
        try c.encode(v, forKey: k)
    }
}

/// A partial track inside a deltaUpdate op (MSF-01 §5.1.6). Only `name` is
/// required; `packaging`/`isLive` are optional (an add op may omit them, §5.6.4),
/// `parentName`/`parentNamespace` are clone overrides, and the remaining fields
/// mirror `MSFTrack`'s optionals (an add/clone op may carry any of them). This
/// is deliberately separate from `MSFTrack` so independent-catalog tracks keep
/// strict required `name`/`packaging`/`isLive` decode. The per-operation
/// required/forbidden-field rules (remove = name/namespace-only, clone =
/// parentName-required, parent fields clone-only) are enforced by
/// `MSFDeltaUpdate.init(from:)`, matching the C MSF parser.
public struct MSFDeltaTrack: Sendable, Equatable, Codable {
    public var name: String
    public var namespace: String?
    public var packaging: String?
    public var isLive: Bool?
    public var parentName: String?
    public var parentNamespace: String?
    public var role: String?
    public var codec: String?
    public var initData: String?
    public var initTrack: String?
    public var width: UInt32?
    public var height: UInt32?
    public var samplerate: UInt32?
    public var bitrate: UInt64?
    public var framerate: Double?
    public var renderGroup: Int?
    public var altGroup: Int?
    public var timescale: UInt64?
    public var targetLatency: UInt64?
    public var channelConfig: String?
    public var lang: String?
    public var label: String?
    public var initRef: String?
    public var eventType: String?
    public var mimeType: String?
    public var trackDuration: UInt64?
    public var depends: [String]?
    public var contentProtectionRefIDs: [String]?
    public var maxGrpSapStartingType: UInt32?
    public var maxObjSapStartingType: UInt32?
    public var template: MSFMediaTemplate?

    public init(
        name: String,
        namespace: String? = nil,
        packaging: String? = nil,
        isLive: Bool? = nil,
        parentName: String? = nil,
        parentNamespace: String? = nil,
        role: String? = nil,
        codec: String? = nil,
        initData: String? = nil,
        initTrack: String? = nil,
        width: UInt32? = nil,
        height: UInt32? = nil,
        samplerate: UInt32? = nil,
        bitrate: UInt64? = nil,
        framerate: Double? = nil,
        renderGroup: Int? = nil,
        altGroup: Int? = nil,
        timescale: UInt64? = nil,
        targetLatency: UInt64? = nil,
        channelConfig: String? = nil,
        lang: String? = nil,
        label: String? = nil,
        initRef: String? = nil,
        eventType: String? = nil,
        mimeType: String? = nil,
        trackDuration: UInt64? = nil,
        depends: [String]? = nil,
        contentProtectionRefIDs: [String]? = nil,
        maxGrpSapStartingType: UInt32? = nil,
        maxObjSapStartingType: UInt32? = nil,
        template: MSFMediaTemplate? = nil
    ) {
        self.name = name
        self.namespace = namespace
        self.packaging = packaging
        self.isLive = isLive
        self.parentName = parentName
        self.parentNamespace = parentNamespace
        self.role = role
        self.codec = codec
        self.initData = initData
        self.initTrack = initTrack
        self.width = width
        self.height = height
        self.samplerate = samplerate
        self.bitrate = bitrate
        self.framerate = framerate
        self.renderGroup = renderGroup
        self.altGroup = altGroup
        self.timescale = timescale
        self.targetLatency = targetLatency
        self.channelConfig = channelConfig
        self.lang = lang
        self.label = label
        self.initRef = initRef
        self.eventType = eventType
        self.mimeType = mimeType
        self.trackDuration = trackDuration
        self.depends = depends
        self.contentProtectionRefIDs = contentProtectionRefIDs
        self.maxGrpSapStartingType = maxGrpSapStartingType
        self.maxObjSapStartingType = maxObjSapStartingType
        self.template = template
    }

    enum CodingKeys: String, CodingKey {
        case name, namespace, packaging, isLive, parentName, parentNamespace
        case role, codec, initData, initTrack
        case width, height, samplerate, bitrate, framerate
        case renderGroup, altGroup, timescale, targetLatency
        case channelConfig, lang, label
        case initRef, eventType, mimeType, trackDuration, depends
        case contentProtectionRefIDs, maxGrpSapStartingType, maxObjSapStartingType
        case template
    }

    public func encode(to encoder: any Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(name, forKey: .name)               // required
        try c.encodeIfPresent(namespace, forKey: .namespace)
        try c.encodeIfPresent(packaging, forKey: .packaging)
        try c.encodeIfPresent(isLive, forKey: .isLive)
        try c.encodeIfPresent(parentName, forKey: .parentName)
        try c.encodeIfPresent(parentNamespace, forKey: .parentNamespace)
        try c.encodeIfPresent(role, forKey: .role)
        try c.encodeIfPresent(codec, forKey: .codec)
        try c.encodeIfPresent(initData, forKey: .initData)
        try c.encodeIfPresent(initTrack, forKey: .initTrack)
        try c.encodeIfPresent(width, forKey: .width)
        try c.encodeIfPresent(height, forKey: .height)
        try c.encodeIfPresent(samplerate, forKey: .samplerate)
        try c.encodeIfPresent(bitrate, forKey: .bitrate)
        try c.encodeIfPresent(framerate, forKey: .framerate)
        try MSFLenient.encodeCInt(&c, renderGroup, .renderGroup)
        try MSFLenient.encodeCInt(&c, altGroup, .altGroup)
        try c.encodeIfPresent(timescale, forKey: .timescale)
        try c.encodeIfPresent(targetLatency, forKey: .targetLatency)
        try c.encodeIfPresent(channelConfig, forKey: .channelConfig)
        try c.encodeIfPresent(lang, forKey: .lang)
        try c.encodeIfPresent(label, forKey: .label)
        try c.encodeIfPresent(initRef, forKey: .initRef)
        try c.encodeIfPresent(eventType, forKey: .eventType)
        try c.encodeIfPresent(mimeType, forKey: .mimeType)
        try c.encodeIfPresent(trackDuration, forKey: .trackDuration)
        try c.encodeIfPresent(depends, forKey: .depends)
        try c.encodeIfPresent(contentProtectionRefIDs, forKey: .contentProtectionRefIDs)
        try c.encodeIfPresent(maxGrpSapStartingType, forKey: .maxGrpSapStartingType)
        try c.encodeIfPresent(maxObjSapStartingType, forKey: .maxObjSapStartingType)
        try c.encodeIfPresent(template, forKey: .template)
    }

    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        name = try c.decode(String.self, forKey: .name)   // required
        namespace = MSFLenient.string(c, .namespace)
        packaging = MSFLenient.string(c, .packaging)
        isLive = MSFLenient.bool(c, .isLive)
        parentName = MSFLenient.string(c, .parentName)
        parentNamespace = MSFLenient.string(c, .parentNamespace)
        role = MSFLenient.string(c, .role)
        codec = MSFLenient.string(c, .codec)
        initData = MSFLenient.string(c, .initData)
        initTrack = MSFLenient.string(c, .initTrack)
        width = MSFLenient.u32(c, .width)
        height = MSFLenient.u32(c, .height)
        samplerate = MSFLenient.u32(c, .samplerate)
        bitrate = MSFLenient.u64(c, .bitrate)
        framerate = MSFLenient.framerate(c, .framerate)
        renderGroup = MSFLenient.cint(c, .renderGroup)
        altGroup = MSFLenient.cint(c, .altGroup)
        timescale = MSFLenient.u64(c, .timescale)
        targetLatency = MSFLenient.u64(c, .targetLatency)
        channelConfig = MSFLenient.string(c, .channelConfig)
        lang = MSFLenient.string(c, .lang)
        label = MSFLenient.string(c, .label)
        initRef = MSFLenient.string(c, .initRef)
        eventType = MSFLenient.string(c, .eventType)
        mimeType = MSFLenient.string(c, .mimeType)
        trackDuration = MSFLenient.u64(c, .trackDuration)
        // Structured fields are decoded STRICTLY (not via the lenient try? path):
        // a present-but-malformed depends/contentProtectionRefIDs/template throws,
        // matching the C parser, which aborts the whole track on a bad array
        // (get_string_array) or template (parse_template) before the per-op shape
        // check. Scalar optionals above stay lenient (C likewise treats a
        // malformed scalar as absent), so only these three needed tightening.
        depends = try c.decodeIfPresent([String].self, forKey: .depends)
        contentProtectionRefIDs =
            try c.decodeIfPresent([String].self, forKey: .contentProtectionRefIDs)
        maxGrpSapStartingType = MSFLenient.u32(c, .maxGrpSapStartingType)
        maxObjSapStartingType = MSFLenient.u32(c, .maxObjSapStartingType)
        template = try c.decodeIfPresent(MSFMediaTemplate.self, forKey: .template)
    }
}

extension MSFDeltaTrack {
    /// True when only `name` (and optionally `namespace`) is present -- the shape
    /// a `remove` op MUST have (MSF-01 §5.1.6). Mirrors the C parser's
    /// track_is_name_namespace_only(); array fields count only when non-empty,
    /// matching the C `*_count == 0` checks.
    var isNameNamespaceOnly: Bool {
        packaging == nil && isLive == nil && parentName == nil &&
        parentNamespace == nil && role == nil && codec == nil &&
        initData == nil && initTrack == nil && width == nil && height == nil &&
        samplerate == nil && bitrate == nil && framerate == nil &&
        renderGroup == nil && altGroup == nil && timescale == nil &&
        targetLatency == nil && channelConfig == nil && lang == nil &&
        label == nil && initRef == nil && eventType == nil && mimeType == nil &&
        trackDuration == nil && (depends?.isEmpty ?? true) &&
        (contentProtectionRefIDs?.isEmpty ?? true) &&
        maxGrpSapStartingType == nil && maxObjSapStartingType == nil &&
        template == nil
    }
}

// MARK: - MSF Track

/// A single track entry in an MSF catalog. All JSON field names
/// match the MSF-01 wire format exactly.
///
/// Required fields (name, packaging, isLive) throw on malformed
/// input. Optional fields decode leniently: malformed, negative,
/// fractional, or out-of-range values are treated as absent (nil),
/// matching the C MSF parser's policy.
public struct MSFTrack: Sendable, Equatable {

    // Required fields.
    public var name: String
    public var packaging: String
    public var isLive: Bool

    // Optional fields — nil values are omitted on encode.
    // MSF JSON namespaces are UTF-8 strings (not MoQ namespace part arrays).
    public var namespace: String?
    public var role: String?
    public var codec: String?
    /// Base64-encoded initialization data (RFC 4648). Use
    /// `decodedInitData()` / `setInitData(_:)` for raw bytes.
    public var initData: String?
    public var initTrack: String?
    public var width: UInt32?
    public var height: UInt32?
    public var samplerate: UInt32?
    public var bitrate: UInt64?
    /// Framerate as a decimal number (e.g., 30, 29.97).
    public var framerate: Double?
    public var renderGroup: Int?
    public var altGroup: Int?
    public var timescale: UInt64?
    public var targetLatency: UInt64?
    public var channelConfig: String?
    public var lang: String?
    public var label: String?
    /// initRef -> a root `initDataList[].id` (MSF-01 §5.2.13).
    public var initRef: String?
    /// eventType for eventtimeline tracks (MSF-01 §5.2.5).
    public var eventType: String?
    /// mimeType (MSF-01 §5.2.19).
    public var mimeType: String?
    /// Track length in integer milliseconds (MSF-01 §5.2.35; VOD/ended only).
    public var trackDuration: UInt64?
    /// Track names this track applies to / depends on (MSF-01 §5.2.14).
    public var depends: [String]?
    /// contentProtectionRefIDs into the root contentProtections (CMSF §4.1.2).
    public var contentProtectionRefIDs: [String]?
    /// maxGrpSapStartingType (CMSF §3.5.2.1).
    public var maxGrpSapStartingType: UInt32?
    /// maxObjSapStartingType (CMSF §3.5.2.2).
    public var maxObjSapStartingType: UInt32?
    /// Inline media-timeline template (MSF-01 §5.2.15 / §7.4).
    public var template: MSFMediaTemplate?

    public init(
        name: String,
        packaging: String,
        isLive: Bool,
        namespace: String? = nil,
        role: String? = nil,
        codec: String? = nil,
        initData: String? = nil,
        initTrack: String? = nil,
        width: UInt32? = nil,
        height: UInt32? = nil,
        samplerate: UInt32? = nil,
        bitrate: UInt64? = nil,
        framerate: Double? = nil,
        renderGroup: Int? = nil,
        altGroup: Int? = nil,
        timescale: UInt64? = nil,
        targetLatency: UInt64? = nil,
        channelConfig: String? = nil,
        lang: String? = nil,
        label: String? = nil,
        initRef: String? = nil,
        eventType: String? = nil,
        mimeType: String? = nil,
        trackDuration: UInt64? = nil,
        depends: [String]? = nil,
        contentProtectionRefIDs: [String]? = nil,
        maxGrpSapStartingType: UInt32? = nil,
        maxObjSapStartingType: UInt32? = nil,
        template: MSFMediaTemplate? = nil
    ) {
        self.name = name
        self.packaging = packaging
        self.isLive = isLive
        self.namespace = namespace
        self.role = role
        self.codec = codec
        self.initData = initData
        self.initTrack = initTrack
        self.width = width
        self.height = height
        self.samplerate = samplerate
        self.bitrate = bitrate
        self.framerate = framerate
        self.renderGroup = renderGroup
        self.altGroup = altGroup
        self.timescale = timescale
        self.targetLatency = targetLatency
        self.channelConfig = channelConfig
        self.lang = lang
        self.label = label
        self.initRef = initRef
        self.eventType = eventType
        self.mimeType = mimeType
        self.trackDuration = trackDuration
        self.depends = depends
        self.contentProtectionRefIDs = contentProtectionRefIDs
        self.maxGrpSapStartingType = maxGrpSapStartingType
        self.maxObjSapStartingType = maxObjSapStartingType
        self.template = template
    }
}

// MARK: - MSFTrack Codable

extension MSFTrack: Encodable {
    enum CodingKeys: String, CodingKey {
        case name, packaging, isLive
        case namespace, role, codec, initData, initTrack
        case width, height, samplerate, bitrate, framerate
        case renderGroup, altGroup, timescale, targetLatency
        case channelConfig, lang, label
        case initRef, eventType, mimeType, trackDuration, depends
        case contentProtectionRefIDs, maxGrpSapStartingType, maxObjSapStartingType
        case template
    }

    public func encode(to encoder: any Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(name, forKey: .name)
        try c.encode(packaging, forKey: .packaging)
        try c.encode(isLive, forKey: .isLive)
        try c.encodeIfPresent(namespace, forKey: .namespace)
        try c.encodeIfPresent(role, forKey: .role)
        try c.encodeIfPresent(codec, forKey: .codec)
        try c.encodeIfPresent(initData, forKey: .initData)
        try c.encodeIfPresent(initTrack, forKey: .initTrack)
        try c.encodeIfPresent(width, forKey: .width)
        try c.encodeIfPresent(height, forKey: .height)
        try c.encodeIfPresent(samplerate, forKey: .samplerate)
        try c.encodeIfPresent(bitrate, forKey: .bitrate)
        try c.encodeIfPresent(framerate, forKey: .framerate)
        try Self.encodeCIntIfPresent(&c, renderGroup, .renderGroup)
        try Self.encodeCIntIfPresent(&c, altGroup, .altGroup)
        try c.encodeIfPresent(timescale, forKey: .timescale)
        try c.encodeIfPresent(targetLatency, forKey: .targetLatency)
        try c.encodeIfPresent(channelConfig, forKey: .channelConfig)
        try c.encodeIfPresent(lang, forKey: .lang)
        try c.encodeIfPresent(label, forKey: .label)
        try c.encodeIfPresent(initRef, forKey: .initRef)
        try c.encodeIfPresent(eventType, forKey: .eventType)
        try c.encodeIfPresent(mimeType, forKey: .mimeType)
        try c.encodeIfPresent(trackDuration, forKey: .trackDuration)
        try c.encodeIfPresent(depends, forKey: .depends)
        try c.encodeIfPresent(contentProtectionRefIDs, forKey: .contentProtectionRefIDs)
        try c.encodeIfPresent(maxGrpSapStartingType, forKey: .maxGrpSapStartingType)
        try c.encodeIfPresent(maxObjSapStartingType, forKey: .maxObjSapStartingType)
        try c.encodeIfPresent(template, forKey: .template)
    }

    private static func encodeCIntIfPresent(
        _ c: inout KeyedEncodingContainer<CodingKeys>,
        _ value: Int?, _ key: CodingKeys
    ) throws {
        guard let v = value,
              v >= Int(Int32.min), v <= Int(Int32.max) else { return }
        try c.encode(v, forKey: key)
    }
}

extension MSFTrack: Decodable {
    public init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)

        // Required fields — strict decode.
        name = try c.decode(String.self, forKey: .name)
        packaging = try c.decode(String.self, forKey: .packaging)
        isLive = try c.decode(Bool.self, forKey: .isLive)

        // Optional fields — lenient: malformed → nil.
        namespace = Self.lenientString(c, .namespace)
        role = Self.lenientString(c, .role)
        codec = Self.lenientString(c, .codec)
        initData = Self.lenientString(c, .initData)
        initTrack = Self.lenientString(c, .initTrack)
        channelConfig = Self.lenientString(c, .channelConfig)
        lang = Self.lenientString(c, .lang)
        label = Self.lenientString(c, .label)

        width = Self.lenientUInt32(c, .width)
        height = Self.lenientUInt32(c, .height)
        samplerate = Self.lenientUInt32(c, .samplerate)

        bitrate = Self.lenientUInt64(c, .bitrate)
        timescale = Self.lenientUInt64(c, .timescale)
        targetLatency = Self.lenientUInt64(c, .targetLatency)

        renderGroup = Self.lenientCInt(c, .renderGroup)
        altGroup = Self.lenientCInt(c, .altGroup)

        framerate = Self.lenientFramerate(c, .framerate)

        initRef = Self.lenientString(c, .initRef)
        eventType = Self.lenientString(c, .eventType)
        mimeType = Self.lenientString(c, .mimeType)
        trackDuration = Self.lenientUInt64(c, .trackDuration)
        depends = Self.lenientStringArray(c, .depends)
        contentProtectionRefIDs = Self.lenientStringArray(c, .contentProtectionRefIDs)
        maxGrpSapStartingType = Self.lenientUInt32(c, .maxGrpSapStartingType)
        maxObjSapStartingType = Self.lenientUInt32(c, .maxObjSapStartingType)
        // §5.2.15: lenient — a present-but-malformed template decodes as absent.
        template = try? c.decodeIfPresent(MSFMediaTemplate.self, forKey: .template)
    }

    private static func lenientStringArray(
        _ c: KeyedDecodingContainer<CodingKeys>, _ key: CodingKeys
    ) -> [String]? {
        try? c.decodeIfPresent([String].self, forKey: key)
    }

    private static func lenientString(
        _ c: KeyedDecodingContainer<CodingKeys>, _ key: CodingKeys
    ) -> String? {
        try? c.decodeIfPresent(String.self, forKey: key)
    }

    private static func lenientUInt32(
        _ c: KeyedDecodingContainer<CodingKeys>, _ key: CodingKeys
    ) -> UInt32? {
        try? c.decodeIfPresent(UInt32.self, forKey: key)
    }

    private static func lenientUInt64(
        _ c: KeyedDecodingContainer<CodingKeys>, _ key: CodingKeys
    ) -> UInt64? {
        try? c.decodeIfPresent(UInt64.self, forKey: key)
    }

    private static func lenientCInt(
        _ c: KeyedDecodingContainer<CodingKeys>, _ key: CodingKeys
    ) -> Int? {
        guard let v = try? c.decodeIfPresent(Int.self, forKey: key) else {
            return nil
        }
        guard v >= Int(Int32.min), v <= Int(Int32.max) else { return nil }
        return v
    }

    // C stores framerate as uint64 millis; cap at that range.
    private static let maxFramerate = Double(UInt64.max) / 1000

    private static func lenientFramerate(
        _ c: KeyedDecodingContainer<CodingKeys>, _ key: CodingKeys
    ) -> Double? {
        guard let v = try? c.decodeIfPresent(Double.self, forKey: key) else {
            return nil
        }
        guard v.isFinite, v >= 0, v <= maxFramerate else { return nil }
        return v
    }
}

// MARK: - initData Helpers

public enum MSFError: Error, Sendable {
    case missingInitData
    case invalidBase64
}

extension MSFTrack {
    /// Decode the base64 `initData` field to raw bytes.
    public func decodedInitData() throws -> Data {
        guard let b64 = initData else { throw MSFError.missingInitData }
        guard let data = Data(base64Encoded: b64) else {
            throw MSFError.invalidBase64
        }
        return data
    }

    /// Set `initData` from raw bytes using standard base64.
    public mutating func setInitData(_ data: Data) {
        initData = data.base64EncodedString()
    }
}
