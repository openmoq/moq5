/// The MoQ transport draft a session negotiated.
///
/// This is the SINGLE nominal type for that fact across the whole Swift
/// surface. It lives in its own dependency-free module for one reason:
/// both the service layer and the media layer need to name the negotiated
/// draft, and if each declared its own enum the two would be different
/// types that merely look alike. A value could then be converted between
/// them only by a hand-written mapping -- exactly the place a draft-16 /
/// draft-18 confusion hides, and exactly the confusion the transport
/// version exists to prevent.
///
/// It carries NO availability constraint of its own: it is a plain model
/// enum touching no platform API, and constraining it would push the
/// service layer's deployment floor onto every media consumer.
///
/// `MoQServiceCore` keeps its published `MoQVersion` spelling as a
/// typealias to this type, and `MoQMedia` exposes a media-facing alias to
/// the same type. Both spellings therefore denote ONE type, so a version
/// read from an endpoint can be handed to a media parser with no
/// conversion at all. `MoQMedia` does not depend on `MoQServiceCore`.
///
/// The draft decides the integer encoding of object-property
/// Key-Value-Pairs (draft-ietf-moq-transport-16 section 1.4 versus
/// draft-ietf-moq-transport-18 section 1.4.1) and, from draft 21, the LOC
/// property ids (draft 21 registers LOC-04's in section 16.8; drafts 16/18
/// carry LOC-01's), which is why media code needs it and why there is no
/// default.
public enum MoQTransportVersion: Sendable, Hashable, CaseIterable {
    case draft16
    case draft18
    case draft21

    /// The IETF draft number (16, 18, 21, …) — display / logging convenience.
    public var draftNumber: Int {
        switch self {
        case .draft16: return 16
        case .draft18: return 18
        case .draft21: return 21
        }
    }
}
