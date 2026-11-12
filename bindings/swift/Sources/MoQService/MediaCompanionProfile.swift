import CMoQService
import MoQServiceCore

func cMediaCompanionProfile(_ profile: MediaCompanionProfile) -> moq_media_companion_profile_t {
    switch profile {
    case .automatic: moq_swift_media_companion_auto()
    case .legacyMSF01LOC01: moq_swift_media_companion_legacy()
    case .draft22MSF01LOC04Public: moq_swift_media_companion_d22_public()
    case .draft22MSF01LOC05Public: moq_swift_media_companion_d22_loc05_public()
    }
}
