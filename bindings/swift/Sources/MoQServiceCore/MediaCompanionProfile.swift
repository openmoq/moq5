/// The media specifications used by a sender or receiver. `.automatic` takes
/// the profile matching the endpoint's negotiated draft. An explicit profile is
/// fixed: a negotiated version mismatch fails the attachment locally, and
/// selecting draft 22 here does not enable it in the endpoint's offer.
public enum MediaCompanionProfile: Sendable, Hashable, CaseIterable {
    /// The companion matching the negotiated transport draft: the legacy
    /// profile on draft 16 or 18, the public LOC-05 profile on draft 22. The
    /// default.
    case automatic
    /// MSF-01 and LOC-01 on transport draft 16 or 18 only.
    case legacyMSF01LOC01
    /// MSF-01 and the public LOC-04 profile on transport draft 22.
    case draft22MSF01LOC04Public
    /// MSF-01 and the public LOC-05 profile (LOC-04 plus the Codec String) on
    /// transport draft 22.
    case draft22MSF01LOC05Public
}
