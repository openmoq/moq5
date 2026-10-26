package org.openmoq.moq5.samples.player

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The three inputs as the Activity's fields start: the launch's string extras where present,
 * the sample's defaults otherwise. The launch intent is the Activity's own, so a recreated
 * Activity starts from the same values.
 */
class PlayerLaunchTest {
    private fun extras(vararg pairs: Pair<String, String>): (String) -> String? = pairs.toMap()::get

    @Test fun anOrdinaryLaunchShowsTheDefaults() {
        assertEquals(PlayerInputs("moqt://10.0.2.2:4443", "live/cam1", "120"), playerInputs(extras()))
    }

    @Test fun eachExtraPresetsOnlyItsOwnField() {
        assertEquals(PlayerInputs("moqt://10.0.2.2:46443", "live/cam1", "120"),
            playerInputs(extras(RELAY_URL_EXTRA to "moqt://10.0.2.2:46443")))
        assertEquals(PlayerInputs("moqt://10.0.2.2:4443", "live/cam2", "120"),
            playerInputs(extras(NAMESPACE_EXTRA to "live/cam2")))
        assertEquals(PlayerInputs("moqt://10.0.2.2:4443", "live/cam1", "60"),
            playerInputs(extras(OBJECT_BUDGET_EXTRA to "60")))
        assertEquals(PlayerInputs("moqt://10.0.2.2:46443", "live/cam2", "60"),
            playerInputs(extras(RELAY_URL_EXTRA to "moqt://10.0.2.2:46443", NAMESPACE_EXTRA to "live/cam2",
                OBJECT_BUDGET_EXTRA to "60")))
    }

    @Test fun anExtraIsShownExactlyAsGiven() {
        // The field shows what the launch said; judging it is the session's job, at Start.
        assertEquals(PlayerInputs(" moqt://x ", "", "sixty"),
            playerInputs(extras(RELAY_URL_EXTRA to " moqt://x ", NAMESPACE_EXTRA to "", OBJECT_BUDGET_EXTRA to "sixty")))
    }

    @Test fun theExtrasAreNamedInThePackage() {
        assertEquals("org.openmoq.moq5.samples.player.RELAY_URL", RELAY_URL_EXTRA)
        assertEquals("org.openmoq.moq5.samples.player.NAMESPACE", NAMESPACE_EXTRA)
        assertEquals("org.openmoq.moq5.samples.player.OBJECT_BUDGET", OBJECT_BUDGET_EXTRA)
    }

    @Test fun theSupervisedLauncherPresetsAllThreeInputs() {
        // The path is the test task's declared input; see the player build script.
        val launcher = File(checkNotNull(System.getProperty("moq5.player.mediaLauncher")) { "moq5.player.mediaLauncher is not set" })
        check(launcher.isFile) { "launcher not found at ${launcher.path}" }
        val text = launcher.readText()
        for (extra in listOf(RELAY_URL_EXTRA, NAMESPACE_EXTRA, OBJECT_BUDGET_EXTRA)) {
            assertEquals(true, "\"--es\", \"$extra\"" in text, "the supervised device start must preset $extra")
        }
    }
}
