package org.openmoq.moq5.samples.player

import org.openmoq.moq5.WireIdentity
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The diagnostic breadcrumb that records a delivered object's public wire identity, exactly as the
 * binding delivered it: one line per object, the four fields verbatim, `subgroup=none` for a
 * datagram's absent subgroup (distinct from `subgroup=0`), and `absent` when the binding carried no
 * identity at all. The line is what an offline oracle compares against the publisher's own decisions,
 * so its shape is pinned here.
 */
class PlaybackIdentityBreadcrumbTest {
    @Test
    fun aStreamObjectPrintsAllFourFields() {
        assertEquals(
            "object identity lane=video group=1 subgroup=0 object=23 priority=128",
            identityBreadcrumb(PlaybackLane.Video, WireIdentity(group = 1uL, subgroup = 0uL, objectId = 23uL, publisherPriority = 128u)),
        )
    }

    @Test
    fun highBitIdsPrintUnsigned() {
        assertEquals(
            "object identity lane=audio group=18446744073709551615 subgroup=9223372036854775808 object=0 priority=255",
            identityBreadcrumb(PlaybackLane.Audio, WireIdentity(ULong.MAX_VALUE, 0x8000_0000_0000_0000uL, 0uL, 255u)),
        )
    }

    @Test
    fun aDatagramHasNoSubgroupWhichIsNotSubgroupZero() {
        assertEquals(
            "object identity lane=video group=7 subgroup=none object=5 priority=200",
            identityBreadcrumb(PlaybackLane.Video, WireIdentity(7uL, null, 5uL, 200u)),
        )
    }

    @Test
    fun noIdentityIsAbsentAndAnUnselectedTrackIsNamedOther() {
        assertEquals("object identity lane=video absent", identityBreadcrumb(PlaybackLane.Video, null))
        assertEquals("object identity lane=other absent", identityBreadcrumb(null, null))
    }
}
