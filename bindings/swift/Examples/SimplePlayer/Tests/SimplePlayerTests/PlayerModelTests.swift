// Load-bearing model tests: the watch-configuration mapping (backend
// selection, the .wtquicNetwork CA-file suppression, the player's companion
// profile and startup cushion) and the PHASE-AWARE
// unsupported messaging (pre-establishment backend/config rejection vs
// post-establishment unsupported media). Pure model logic: no
// connection is ever attempted.

import XCTest
import MoQService
@testable import SimplePlayer

@MainActor
final class PlayerModelTests: XCTestCase {

    private func model(backend: PlayerModel.BackendChoice)
        -> PlayerModel {
        let m = PlayerModel()
        m.backend = backend
        return m
    }

    // MARK: Configuration mapping

    func testDraft22ExplicitlySelectsWireOfferAndCompanion() {
        let m = model(backend: .picoquic)
        m.version = .draft22
        let cfg = m.makeWatchConfiguration(url: URL(string: "moqt://relay.example:4443")!)
        XCTAssertEqual(cfg.endpoint.versions, .automatic)
        XCTAssertEqual(cfg.endpoint.wireVersions, .list([.draft22]))
        XCTAssertEqual(cfg.player.companionProfile, .draft22MSF01LOC04Public)
        XCTAssertNil(cfg.endpoint.preferredVersion)
        XCTAssertNil(cfg.endpoint.preferredWireVersion)
    }

    func testAutomaticKeepsLegacyOfferAndCompanion() {
        let m = model(backend: .picoquic)
        XCTAssertEqual(m.version, .automatic)
        let cfg = m.makeWatchConfiguration(url: URL(string: "moqt://relay.example:4443")!)
        XCTAssertEqual(cfg.endpoint.versions, .automatic)
        XCTAssertNil(cfg.endpoint.wireVersions)
        XCTAssertEqual(cfg.player.companionProfile, .legacyMSF01LOC01)
        XCTAssertEqual(cfg.establishmentTimeout, .seconds(15))
        XCTAssertEqual(cfg.player.selection, .automatic)
        XCTAssertEqual(cfg.player.cushion, .milliseconds(300))
    }

    func testLegacySelectionsKeepTheirOfferAndCompanion() {
        let m = model(backend: .picoquic)
        for (choice, version) in [(PlayerModel.VersionChoice.draft16, MoQVersion.draft16),
                                  (.draft18, .draft18)] {
            m.version = choice
            let cfg = m.makeWatchConfiguration(url: URL(string: "moqt://relay.example:4443")!)
            XCTAssertEqual(cfg.endpoint.versions, .list([version]))
            XCTAssertNil(cfg.endpoint.wireVersions)
            XCTAssertEqual(cfg.player.companionProfile, .legacyMSF01LOC01)
        }
    }

    func testWtquicNetworkNeverAttachesCAFile() {
        let m = model(backend: .wtquicNetwork)
        m.caFilePath = "/tmp/some-ca.pem"   // must be ignored
        m.insecureSkipVerify = true
        let cfg = m.makeConfiguration(
            url: URL(string: "https://relay.example:4433/moq")!)
        XCTAssertEqual(cfg.backend, .wtquicNetwork)
        XCTAssertNil(cfg.caFileURL,
                     ".wtquicNetwork uses system trust; a CA file must never reach it")
        XCTAssertTrue(cfg.insecureSkipVerify)
    }

    func testPicoquicCarriesTheCAFile() {
        let m = model(backend: .picoquic)
        m.caFilePath = "/tmp/some-ca.pem"
        let cfg = m.makeConfiguration(
            url: URL(string: "https://relay.example:4433/moq")!)
        XCTAssertEqual(cfg.backend, .picoquic)
        XCTAssertEqual(cfg.caFileURL,
                       URL(fileURLWithPath: "/tmp/some-ca.pem"))
    }

    func testWtquicMsquicMapsAndNeverAttachesCAFile() {
        let m = model(backend: .wtquicMsquic)
        m.caFilePath = "/tmp/some-ca.pem"   // must be ignored
        m.insecureSkipVerify = true
        let cfg = m.makeConfiguration(
            url: URL(string: "https://relay.example:4433/moq")!)
        XCTAssertEqual(cfg.backend, .wtquicMsquic,
                       ".wtquicMsquic must map to the WebTransport-over-MsQuic backend")
        XCTAssertNil(cfg.caFileURL,
                     ".wtquicMsquic uses system trust; a CA file must never reach it")
        XCTAssertTrue(cfg.insecureSkipVerify)
    }

    // MARK: Backend default

    func testFreshInstallDefaultsToPicoquicAndPreservesSavedChoice() {
        let key = "SimplePlayer.backend"
        let d = UserDefaults.standard
        let saved = d.string(forKey: key)
        defer {
            if let s = saved { d.set(s, forKey: key) } else { d.removeObject(forKey: key) }
        }
        // Fresh install (no saved value) -> the reliable picoquic family, not
        // the experimental Network.framework backend.
        d.removeObject(forKey: key)
        XCTAssertEqual(PlayerModel().backend, .picoquic,
                       "fresh install must default to .picoquic")
        // An explicitly saved choice always wins over the default.
        d.set(PlayerModel.BackendChoice.wtquicNetwork.rawValue, forKey: key)
        XCTAssertEqual(PlayerModel().backend, .wtquicNetwork,
                       "a saved backend choice must be preserved")
    }

    func testExperimentalLabelDoesNotChangeThePersistedRawValue() {
        // The picker label may mark Network.framework experimental, but the
        // persisted rawValue must stay stable so saved preferences keep loading.
        XCTAssertEqual(PlayerModel.BackendChoice.wtquicNetwork.rawValue,
                       "Network.framework (WT)")
        XCTAssertTrue(PlayerModel.BackendChoice.wtquicNetwork.displayName
                        .contains("Experimental"))
        // Non-experimental backends: the label is just the raw value.
        XCTAssertEqual(PlayerModel.BackendChoice.picoquic.displayName,
                       PlayerModel.BackendChoice.picoquic.rawValue)
    }

    // MARK: Phase-aware unsupported messaging

    func testUnsupportedDuringConnectBlamesTransportConfiguration() {
        let m = model(backend: .wtquicNetwork)
        let message = m.describe(.unsupported, duringConnect: true)
        XCTAssertTrue(message.contains("transport configuration"),
                      "pre-establishment: backend/config rejection, got: \(message)")
        XCTAssertFalse(message.contains("media format"))
    }

    func testUnsupportedAfterEstablishmentBlamesTheMediaFormat() {
        let m = model(backend: .wtquicNetwork)
        let message = m.describe(.unsupported, duringConnect: false)
        XCTAssertTrue(message.contains("media format"),
                      "post-establishment: unsupported media, got: \(message)")
        XCTAssertFalse(message.contains("transport configuration"))
    }

    // MARK: The establishment latch

    /// A media `.unsupported` is classified from any establishment-IMPLYING
    /// phase, never from having literally observed `.established`.
    func testImpliedEstablishmentStillClassifiesMediaFailure() {
        let m = model(backend: .wtquicNetwork)
        _ = m.apply(.connecting)
        // no .established: the later phase implies it
        _ = m.apply(.waitingForMedia)
        _ = m.apply(.failed(.unsupported))
        guard case .failed(let message) = m.state else {
            return XCTFail("expected .failed, got \(m.state)")
        }
        XCTAssertTrue(message.contains("media format"),
                      "implied establishment misclassified: \(message)")
    }

    /// And a genuinely pre-establishment failure still blames the
    /// transport configuration (the latch resets per watch).
    /// A catalog with nothing playable fails the watch with its own message.
    func testNoPlayableTracksFailsTheWatch() {
        let m = model(backend: .picoquic)
        _ = m.apply(.waitingForMedia)
        XCTAssertTrue(m.apply(.noPlayableTracks))
        guard case .failed(let message) = m.state else {
            return XCTFail("expected .failed, got \(m.state)")
        }
        XCTAssertTrue(message.contains("No playable track"))
        XCTAssertFalse(m.isRunning)
    }

    func testConnectPhaseFailureStillBlamesConfiguration() {
        let m = model(backend: .wtquicNetwork)
        _ = m.apply(.waitingForMedia)          // stale prior watch
        _ = m.apply(.connecting)               // new watch resets the latch
        _ = m.apply(.failed(.unsupported))
        guard case .failed(let message) = m.state else {
            return XCTFail("expected .failed, got \(m.state)")
        }
        XCTAssertTrue(message.contains("transport configuration"),
                      "pre-establishment misclassified: \(message)")
    }

    /// A new watch can fail before it reports `.connecting`, so the
    /// connect() call's SYNCHRONOUS reset is what clears the prior watch's
    /// establishment latch: a pre-establishment failure of the new watch
    /// must still blame the transport configuration.
    func testSynchronousResetSurvivesUnreportedConnecting() {
        let m = model(backend: .wtquicNetwork)
        _ = m.apply(.waitingForMedia)          // prior watch established
        m.beginWatchUIState()                  // what connect() runs first
        // no .connecting is ever applied: straight to the failure
        _ = m.apply(.failed(.unsupported))
        guard case .failed(let message) = m.state else {
            return XCTFail("expected .failed, got \(m.state)")
        }
        XCTAssertTrue(message.contains("transport configuration"),
                      "unreported .connecting misclassified: \(message)")
        XCTAssertFalse(message.contains("media format"))
    }
}
