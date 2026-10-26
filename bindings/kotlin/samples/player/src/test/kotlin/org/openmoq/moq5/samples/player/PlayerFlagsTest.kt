package org.openmoq.moq5.samples.player

import kotlin.test.Test
import kotlin.test.assertEquals

/** The acceptance observer and the diagnostic trace are two independent launch switches, both off by default. */
class PlayerFlagsTest {
    private fun flags(vararg on: String) = playerFlags { key, default -> if (key in on) true else default }

    @Test
    fun anOrdinaryLaunchArmsNeither() {
        assertEquals(PlayerFlags(observe = false, trace = false), flags())
    }

    @Test
    fun eachSwitchIsIndependentOfTheOther() {
        assertEquals(PlayerFlags(observe = true, trace = false), flags(OBSERVE_PRESENTATION_EXTRA))
        assertEquals(PlayerFlags(observe = false, trace = true), flags(DIAGNOSTIC_TRACE_EXTRA))
        assertEquals(PlayerFlags(observe = true, trace = true), flags(OBSERVE_PRESENTATION_EXTRA, DIAGNOSTIC_TRACE_EXTRA))
    }

    @Test
    fun theTraceSwitchHasItsOwnName() {
        assertEquals("org.openmoq.moq5.samples.player.DIAGNOSTIC_TRACE", DIAGNOSTIC_TRACE_EXTRA)
    }
}
