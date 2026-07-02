/// A MoQ transport draft version this SDK can negotiate.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum MoQVersion: Sendable, Hashable, CaseIterable {
    case draft16
    case draft18

    /// The IETF draft number (16, 18, …) — display / logging convenience.
    public var draftNumber: Int {
        switch self {
        case .draft16: return 16
        case .draft18: return 18
        }
    }
}

/// What versions an endpoint offers during negotiation.
///
/// Mirrors the C `moq_version_offer_t` policies: `.automatic` offers every
/// version this build supports (never "pick newest and hope"), `.list` offers
/// exactly the given set in preference order, `.exactly` pins one version and
/// makes a mismatch a terminal connect failure.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
public enum VersionOffer: Sendable, Hashable {
    case automatic
    case list([MoQVersion])
    case exactly(MoQVersion)
}
