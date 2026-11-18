package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The session's ONE callback authority, by role: a track's format
 * observations and its codec callbacks are admitted, released and retired by
 * the same lock, one key per (kind, role) and one active reservation per key.
 *
 * Each row plays the callback boundary by calling the real session directly,
 * on the row's own thread, so what is pinned is the session and never a model
 * of it. The format role's own rows (`FormatReservationTest`) are unchanged;
 * these pin what the codec role adds and that the format inventory keeps its
 * meaning beside it.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class CodecCallbackAuthorityTest {

    private fun admitted(admission: FormatAdmission): FormatReservation = assertIs<FormatAdmission.Admitted>(admission).reservation

    @Test
    fun `a1 a track's format and codec callbacks coexist, kinds are independent, and at most four are admitted`() {
        val rig = Rig()
        val videoFormat = rig.gate.bindFormatHolder(MediaKind.Video)
        val audioFormat = rig.gate.bindFormatHolder(MediaKind.Audio)
        val videoCodec = rig.codecGate.bindCodecCallbacks(MediaKind.Video)
        val audioCodec = rig.codecGate.bindCodecCallbacks(MediaKind.Audio)

        val held =
            listOf(
                admitted(rig.gate.reserveFormat(videoFormat)),
                admitted(rig.codecGate.reserveCodec(videoCodec)),
                admitted(rig.codecGate.reserveCodec(audioCodec)),
                admitted(rig.gate.reserveFormat(audioFormat)),
            )
        for (key in listOf(videoFormat, audioFormat)) assertSame(FormatAdmission.Overlapping, rig.gate.reserveFormat(key), "a second ${key.kind} format callback")
        for (key in listOf(videoCodec, audioCodec)) assertSame(FormatAdmission.Overlapping, rig.codecGate.reserveCodec(key), "a second ${key.kind} codec callback")
        val all = setOf(MediaKind.Video, MediaKind.Audio).flatMap { kind -> CallbackRole.entries.map { kind to it } }.toSet()
        assertEquals(CallbackInventory(all, emptyList(), null), rig.codecGate.callbackInventory(), "four admitted, and the overlaps added nothing")
        assertEquals(FormatInventory(setOf(MediaKind.Video, MediaKind.Audio), emptyList(), null), rig.gate.formatInventory(), "the format inventory still counts format callbacks only")

        // Releasing one role of a kind leaves the other role, and the other kind, admitted.
        assertEquals(FormatRelease.Released(null), rig.codecGate.releaseCodec(held[1], null))
        assertEquals(all - (MediaKind.Video to CallbackRole.Codec), rig.codecGate.callbackInventory().active)
        assertEquals(setOf(MediaKind.Video, MediaKind.Audio), rig.gate.formatInventory().active)
        val again = admitted(rig.codecGate.reserveCodec(videoCodec))
        assertEquals(FormatRelease.Released(null), rig.codecGate.releaseCodec(again, null))
        assertEquals(FormatRelease.Released(null), rig.gate.releaseFormat(held[0], null))
        assertEquals(FormatRelease.Released(null), rig.codecGate.releaseCodec(held[2], null))
        assertEquals(FormatRelease.Released(null), rig.gate.releaseFormat(held[3], null))
        assertEquals(CallbackInventory(emptySet(), emptyList(), null), rig.codecGate.callbackInventory())
    }

    @Test
    fun `a2 keys are role-exact, and a foreign, duplicate or stale release authorizes nothing`() {
        val rig = Rig()
        val other = Rig()
        val videoFormat = rig.gate.bindFormatHolder(MediaKind.Video)
        val videoCodec = rig.codecGate.bindCodecCallbacks(MediaKind.Video)
        assertFailsWith<IllegalStateException>("a second codec bind for a kind") { rig.codecGate.bindCodecCallbacks(MediaKind.Video) }
        assertSame(FormatAdmission.NotBound, rig.codecGate.reserveCodec(videoFormat), "a format key reserves no codec callback")
        assertSame(FormatAdmission.NotBound, rig.gate.reserveFormat(videoCodec), "a codec key reserves no format callback")
        assertSame(FormatAdmission.NotBound, rig.codecGate.reserveCodec(FormatHolderKey(MediaKind.Video, CallbackRole.Codec)), "a key this session never minted")
        val foreignKey = other.codecGate.bindCodecCallbacks(MediaKind.Video)
        assertSame(FormatAdmission.NotBound, rig.codecGate.reserveCodec(foreignKey), "another session's key")

        val codec = admitted(rig.codecGate.reserveCodec(videoCodec))
        val format = admitted(rig.gate.reserveFormat(videoFormat))
        val foreign = admitted(other.codecGate.reserveCodec(foreignKey))
        val before = rig.codecGate.callbackInventory()
        val raised = IllegalStateException("reported through the wrong door")
        assertEquals(FormatRelease.Refused(raised), rig.gate.releaseFormat(codec, raised), "a codec reservation is not released as a format one")
        assertEquals(FormatRelease.Refused(raised), rig.codecGate.releaseCodec(format, raised), "nor a format reservation as a codec one")
        assertEquals(FormatRelease.Refused(raised), rig.codecGate.releaseCodec(foreign, raised), "nor another session's")
        assertEquals(before, rig.codecGate.callbackInventory(), "every refusal left the authority exactly as it was")
        assertNull(rig.session.termination, "and claimed nothing")

        assertEquals(FormatRelease.Released(null), rig.codecGate.releaseCodec(codec, null))
        assertEquals(FormatRelease.Refused(raised), rig.codecGate.releaseCodec(codec, raised), "a duplicate release")
        val newer = admitted(rig.codecGate.reserveCodec(videoCodec))
        assertEquals(FormatRelease.Refused(raised), rig.codecGate.releaseCodec(codec, raised), "a stale release cannot touch the newer reservation")
        assertTrue((MediaKind.Video to CallbackRole.Codec) in rig.codecGate.callbackInventory().active)
        assertEquals(emptyList(), rig.codecGate.callbackInventory().causes, "no refused release recorded its cause")

        assertFailsWith<IllegalArgumentException>("a format key reports no codec fault") { rig.codecGate.reportCodec(videoFormat, "misrouted") }
        assertFailsWith<IllegalArgumentException>("another session's key reports nothing here") { rig.codecGate.reportCodec(foreignKey, "misrouted") }
        assertNull(rig.session.termination, "neither report reached the fault route")

        assertEquals(FormatRelease.Released(null), rig.codecGate.releaseCodec(newer, null))
        assertEquals(FormatRelease.Released(null), rig.gate.releaseFormat(format, null))
        assertEquals(FormatRelease.Released(null), other.codecGate.releaseCodec(foreign, null))
    }

    @Test
    fun `a3 a claimed ending freezes only once every admitted role has settled`() = runTest {
        val rig = Rig()
        val videoFormat = admitted(rig.gate.reserveFormat(rig.gate.bindFormatHolder(MediaKind.Video)))
        val videoCodec = admitted(rig.codecGate.reserveCodec(rig.codecGate.bindCodecCallbacks(MediaKind.Video)))
        rig.session.stop(null)
        val receipt = async { rig.session.retirement() }
        runCurrent()
        assertFalse(receipt.isCompleted, "both roles are still admitted")
        assertSame(FormatAdmission.Ended, rig.codecGate.reserveCodec(videoCodec.key), "nothing new is admitted once the ending is claimed")

        assertEquals(FormatRelease.Released(null), rig.gate.releaseFormat(videoFormat, null))
        runCurrent()
        assertFalse(receipt.isCompleted, "the codec callback is still admitted")

        assertEquals(FormatRelease.Released(null), rig.codecGate.releaseCodec(videoCodec, null))
        runCurrent()
        assertTrue(receipt.isCompleted, "the last admitted role froze it")
        assertEquals(CaptureTermination.Completed, receipt.await().termination)
        assertEquals(emptyList(), receipt.await().causes)
    }

    @Test
    fun `a4 a codec callback's cause claims the ending, is recorded once, and the format inventory does not count it`() = runTest {
        val rig = Rig()
        val videoFormatKey = rig.gate.bindFormatHolder(MediaKind.Video)
        val audioCodecKey = rig.codecGate.bindCodecCallbacks(MediaKind.Audio)
        val videoCodecKey = rig.codecGate.bindCodecCallbacks(MediaKind.Video)
        val format = admitted(rig.gate.reserveFormat(videoFormatKey))
        val audio = admitted(rig.codecGate.reserveCodec(audioCodecKey))
        val video = admitted(rig.codecGate.reserveCodec(videoCodecKey))
        val cause = AssertionError("the audio callback's collaborator raised an Error")

        val released = assertIs<FormatRelease.Released>(rig.codecGate.releaseCodec(audio, cause))
        val claimed = CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Audio, cause))
        assertEquals(claimed, released.claimed, "the first unexpected cause claims the ending")
        assertEquals(listOf<Throwable>(cause), rig.codecGate.callbackInventory().causes)
        assertEquals(emptyList(), rig.gate.formatInventory().causes, "a codec cause is not a format cause")
        assertEquals(FormatRelease.Released(null), rig.codecGate.releaseCodec(video, cause), "the same Throwable again, from another kind, is not a second cause")
        assertEquals(FormatRelease.Released(null), rig.gate.releaseFormat(format, cause), "nor from the format role")
        val receipt = rig.session.retirement()
        assertEquals(claimed, receipt.termination)
        assertEquals(1, receipt.causes.count { it === cause }, "recorded once")
        assertEquals(listOf<Throwable>(cause), receipt.causes)
    }

    @Test
    fun `a5 a cleanup failure the ending already holds is not a cause, and the session says which failures it owns`() = runTest {
        val rig = Rig()
        val closeFailure = IllegalStateException("the video track's close failed")
        rig.videoEntry.beforeClose = { throw closeFailure }
        val video = admitted(rig.codecGate.reserveCodec(rig.codecGate.bindCodecCallbacks(MediaKind.Video)))
        val audio = admitted(rig.codecGate.reserveCodec(rig.codecGate.bindCodecCallbacks(MediaKind.Audio)))
        val cause = IllegalStateException("the video output collaborator failed")

        // The claiming release does the claimant's closes; what they raise leaves it, already recorded.
        val raised = assertFailsWith<IllegalStateException> { rig.codecGate.releaseCodec(video, cause) }
        assertSame(closeFailure, raised)
        assertTrue(rig.codecGate.owns(closeFailure), "the cleanup failure is the ending's")
        assertTrue(rig.codecGate.owns(cause), "and so is the cause")
        assertFalse(rig.codecGate.owns(IllegalStateException("unrelated")), "and nothing else is")

        assertEquals(FormatRelease.Released(null), rig.codecGate.releaseCodec(audio, closeFailure), "reporting the owned cleanup failure as a cause records nothing")
        val receipt = rig.session.retirement()
        assertEquals(CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Video, cause)), receipt.termination)
        assertEquals(listOf<Throwable>(cause), receipt.causes)
        assertEquals(1, receipt.cleanup.count { it === closeFailure }, "the cleanup failure is kept once, as cleanup")
    }
}
