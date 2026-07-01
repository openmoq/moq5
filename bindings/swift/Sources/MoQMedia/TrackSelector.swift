import Foundation

/// Criteria for selecting tracks from an MSF catalog.
public struct TrackSelector: Sendable {

    public struct Criteria: Sendable {
        public var role: String
        public var codecPrefixes: [String]
        public var packagingPreference: [String]

        public init(
            role: String,
            codecPrefixes: [String],
            packagingPreference: [String] = ["cmaf", "loc"]
        ) {
            self.role = role
            self.codecPrefixes = codecPrefixes
            self.packagingPreference = packagingPreference
        }
    }

}

extension TrackSelector.Criteria {
    public static let h264Video = TrackSelector.Criteria(
        role: "video", codecPrefixes: ["avc1"])

    public static let aacAudio = TrackSelector.Criteria(
        role: "audio", codecPrefixes: ["mp4a"])
}

extension MSFCatalog {
    /// Return tracks matching the criteria, sorted by packaging preference.
    ///
    /// Matches role exactly and codec by case-insensitive prefix.
    /// Empty codecPrefixes matches any codec for the role.
    /// Tracks with packaging not in the preference list appear last,
    /// preserving catalog order within the same packaging rank.
    public func selectTracks(
        matching criteria: TrackSelector.Criteria
    ) -> [MSFTrack] {
        let matched = tracks.filter { track in
            guard track.role == criteria.role else { return false }
            if criteria.codecPrefixes.isEmpty { return true }
            guard let codec = track.codec?.lowercased() else { return false }
            return criteria.codecPrefixes.contains { prefix in
                codec.hasPrefix(prefix.lowercased())
            }
        }

        let prefCount = criteria.packagingPreference.count
        return matched.enumerated()
            .sorted { a, b in
                let rankA = criteria.packagingPreference.firstIndex(
                    of: a.element.packaging) ?? prefCount
                let rankB = criteria.packagingPreference.firstIndex(
                    of: b.element.packaging) ?? prefCount
                if rankA != rankB { return rankA < rankB }
                return a.offset < b.offset
            }
            .map(\.element)
    }
}
