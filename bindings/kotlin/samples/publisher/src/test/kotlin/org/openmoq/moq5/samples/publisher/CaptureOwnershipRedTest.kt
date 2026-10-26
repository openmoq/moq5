package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Ownership, close ordering, and asynchronous settlement for a capture
 * session's platform resources.
 *
 * Every row is written against the [CaptureOwnership] INTERFACE and run three
 * ways: against the Phase A declaration (RED at its named `TODO`), against the
 * conforming host fixture (must PASS -- the contract is satisfiable), and,
 * for the discrimination rows, against stand-ins that get the totals right.
 *
 * Identity is the PLATFORM resource, not the wrapper: the platform hands back
 * one `CameraDevice` however many times it calls, so a second wrapper around
 * the same device is a duplicate delivery and must close nothing.
 *
 * Every resource here is a host fake. None of this is evidence that a camera,
 * a microphone or an encoder was ever acquired.
 */
class CaptureOwnershipRedTest {

    /** A platform with resources, wrappers, and a close log by identity. */
    private class Platform {
        val closes = mutableListOf<Any>()
        val worker = HostResource("worker")
        val camera = HostResource("camera")
        val stream = HostResource("stream")
        val surface = HostResource("surface")
        val mic = HostResource("mic")
        val venc = HostResource("venc")
        val aenc = HostResource("aenc")

        fun handle(identity: Any, fail: Exception? = null) = HostHandle(identity, closes, fail)

        fun closed(): List<Any> = synchronized(closes) { closes.toList() }
    }

    private class Rig {
        val platform = Platform()
        val dispatcher = HostDispatcher()
        val late = LateLog()
    }

    private fun interface Make {
        fun make(rig: Rig): CaptureOwnership
    }

    /**
     * Right totals, wrong facts: closes in acquisition order and again on the
     * second close, drops cleanup failures, installs whatever is delivered,
     * closes by wrapper rather than identity, and QUITS the dispatcher at close
     * -- so a late delivery is refused by the dispatcher and the device leaks.
     */
    private class CountingOwnership(override val generation: Long, override val dispatcher: CallbackDispatcher) :
        CaptureOwnership {
        private val handles = mutableListOf<ResourceHandle>()
        private val tickets = mutableListOf<AcquisitionTicket>()
        override var isClosed: Boolean = false
            private set
        override var pending: Int = 0
            private set

        override fun acquire(resource: CaptureResource, open: () -> ResourceHandle): AcquireOutcome {
            handles.add(open())
            return AcquireOutcome.Acquired
        }

        override fun retainWorker(): Boolean = true

        override fun releaseWorker() = Unit

        override fun request(resource: CaptureResource): AcquisitionTicket =
            AcquisitionTicket(generation, resource).also { tickets.add(it); pending++ }

        override fun adopt(ticket: AcquisitionTicket, handle: ResourceHandle): AdoptOutcome {
            handles.add(handle)
            pending--
            return AdoptOutcome.Adopted
        }

        override fun fail(ticket: AcquisitionTicket, reason: String, carried: ResourceHandle?): AdoptOutcome {
            pending--
            return AdoptOutcome.Failed(reason)
        }

        override fun lost(resource: CaptureResource, reason: String): LostOutcome = LostOutcome.Released

        override fun retire(reason: String) = Unit

        override fun close(primary: Exception?): Exception? {
            isClosed = true
            dispatcher.quit()
            handles.forEach {
                try {
                    it.close()
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (dropped: Exception) {
                    check(dropped.message != null)
                }
            }
            return primary
        }
    }

    // ---- synchronous ownership ---------------------------------------------

    private fun o1_closesChildrenBeforeWhatTheyHangFrom(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        val order = listOf(p.worker, p.camera, p.venc, p.surface, p.stream)
        val roles =
            listOf(
                CaptureResource.Worker, CaptureResource.Camera, CaptureResource.VideoEncoder,
                CaptureResource.CameraSurface, CaptureResource.CameraStream,
            )
        roles.forEachIndexed { i, role -> assertEquals(AcquireOutcome.Acquired, owned.acquire(role) { p.handle(order[i]) }) }
        owned.close(null)
        assertEquals(order.reversed(), p.closed(), "a child closes before what it was opened from; the worker closes last")
    }

    private fun o2_aPartialAcquisitionUnwindsOnlyWhatItAcquired(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        owned.acquire(CaptureResource.Worker) { p.handle(p.worker) }
        owned.acquire(CaptureResource.Camera) { p.handle(p.camera) }
        val failed = owned.acquire(CaptureResource.VideoEncoder) { throw IllegalStateException("the encoder refused the format") }
        assertIs<AcquireOutcome.Failed>(failed)
        assertEquals(CaptureResource.VideoEncoder, failed.resource)
        owned.close(null)
        assertEquals(listOf<Any>(p.camera, p.worker), p.closed(), "exactly the acquired resources unwind, once, in reverse")
    }

    private fun o3_closingTwiceClosesNothingTwice(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        owned.acquire(CaptureResource.Microphone) { p.handle(p.mic) }
        owned.close(null)
        assertTrue(owned.isClosed)
        owned.close(null)
        assertEquals(listOf<Any>(p.mic), p.closed(), "one idempotent close")
    }

    private fun o4_cleanupContinuesPastAFailureAndKeepsThePrimary(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        val encoderClose = IllegalStateException("the encoder would not stop")
        val cameraClose = IllegalStateException("the camera was already gone")
        owned.acquire(CaptureResource.Camera) { p.handle(p.camera, cameraClose) }
        owned.acquire(CaptureResource.VideoEncoder) { p.handle(p.venc, encoderClose) }
        val primary = IllegalStateException("the broadcast failed")
        assertSame(primary, owned.close(primary), "the actual failure ends the session")
        assertEquals(listOf<Any>(p.venc, p.camera), p.closed(), "an earlier close failure does not abandon the rest")
        assertEquals(listOf(encoderClose, cameraClose), primary.suppressed.toList(), "every cleanup failure attached, in order")
    }

    private fun o5_withNoPrimaryTheFirstCleanupFailureIsTheOutcome(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        val micClose = IllegalStateException("the microphone would not stop")
        val encoderClose = IllegalStateException("the encoder would not stop")
        owned.acquire(CaptureResource.Microphone) { p.handle(p.mic, micClose) }
        owned.acquire(CaptureResource.AudioEncoder) { p.handle(p.aenc, encoderClose) }
        val ending = owned.close(null)
        assertSame(encoderClose, ending)
        assertEquals(listOf(micClose), encoderClose.suppressed.toList())
    }

    private fun o6_anAcquisitionThatRacesRetirementOwnsNothing(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        owned.retire("the Activity was destroyed while the camera was opening")
        assertIs<AcquireOutcome.Retired>(owned.acquire(CaptureResource.Camera) { p.handle(p.camera) })
        assertEquals(listOf<Any>(p.camera), p.closed(), "what the platform handed back is closed HERE, once")
        assertEquals(listOf("late-close:Camera"), rig.late.all(), "and that disposal is observable")
        owned.close(null)
        assertEquals(listOf<Any>(p.camera), p.closed(), "and not closed a second time")
    }

    // ---- asynchronous settlement (openCamera returns void) ---------------------

    private fun o7_anAdoptedResourceIsOwnedInAcquisitionOrder(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        owned.acquire(CaptureResource.Worker) { p.handle(p.worker) }
        val ticket = owned.request(CaptureResource.Camera)
        assertEquals(1, owned.pending)
        // onOpened arrives through the dispatcher, as it would on a HandlerThread.
        assertTrue(rig.dispatcher.deliver { assertEquals(AdoptOutcome.Adopted, owned.adopt(ticket, p.handle(p.camera))) })
        assertEquals(0, owned.pending)
        owned.acquire(CaptureResource.VideoEncoder) { p.handle(p.venc) }
        owned.close(null)
        assertEquals(listOf<Any>(p.venc, p.camera, p.worker), p.closed(), "adopted resources close in reverse like the rest")
    }

    private fun o8_aDeviceDeliveredAfterRetirementIsClosedNotInstalled(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        val ticket = owned.request(CaptureResource.Camera)
        owned.retire("the Activity was destroyed while the camera was opening")
        assertTrue(rig.dispatcher.deliver { assertIs<AdoptOutcome.Retired>(owned.adopt(ticket, p.handle(p.camera))) })
        assertEquals(listOf<Any>(p.camera), p.closed(), "closed HERE, exactly once, rather than leaked")
        assertEquals(0, owned.pending)
        owned.close(null)
        assertEquals(listOf<Any>(p.camera), p.closed())
    }

    private fun o9_aDuplicateDeliveryOfTheLiveDeviceClosesNothing(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        val ticket = owned.request(CaptureResource.Camera)
        assertEquals(AdoptOutcome.Adopted, owned.adopt(ticket, p.handle(p.camera)))
        // The SAME CameraDevice again, in a DIFFERENT wrapper: identity is the
        // platform object, so this is a duplicate and must not touch the camera.
        assertIs<AdoptOutcome.Unknown>(owned.adopt(ticket, p.handle(p.camera)))
        assertEquals(emptyList(), p.closed(), "a duplicate delivery of the LIVE resource closes nothing")
        assertEquals(emptyList(), rig.late.all())
        owned.close(null)
        assertEquals(listOf<Any>(p.camera), p.closed(), "and the live device is still there to be closed once")
    }

    private fun o10_aForeignDeliveryOfADifferentResourceIsDisposedHere(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        val ticket = owned.request(CaptureResource.Camera)
        assertEquals(AdoptOutcome.Adopted, owned.adopt(ticket, p.handle(p.camera)))
        // A foreign ticket carrying the LIVE device: never close it.
        val foreignLive = AcquisitionTicket(owned.generation - 1L, CaptureResource.Camera)
        assertIs<AdoptOutcome.Unknown>(owned.adopt(foreignLive, p.handle(p.camera)))
        assertEquals(emptyList(), p.closed(), "a foreign ticket cannot close or replace the live camera")
        // A settled ticket carrying a DIFFERENT, unowned resource: this
        // ownership is the only party holding it, so it disposes of it, once,
        // and says so.
        assertIs<AdoptOutcome.Unknown>(owned.adopt(ticket, p.handle(p.stream)))
        assertEquals(listOf<Any>(p.stream), p.closed())
        assertEquals(listOf("foreign:Camera"), rig.late.all(), "disposal authority is declared, not silent")
        owned.close(null)
        assertEquals(listOf<Any>(p.stream, p.camera), p.closed())
    }

    private fun o11_aFailureInsteadOfOpenedClosesWhatItCarries(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        // onError/onDisconnected "may be called instead of onOpened" and carry
        // the CameraDevice, which must be closed.
        val camera = owned.request(CaptureResource.Camera)
        assertIs<AdoptOutcome.Failed>(owned.fail(camera, "onError(ERROR_CAMERA_IN_USE)", p.handle(p.camera)))
        assertEquals(listOf<Any>(p.camera), p.closed(), "the device the failure carried is closed once, never installed")
        assertEquals(0, owned.pending)
        assertIs<AdoptOutcome.Unknown>(owned.fail(camera, "again", null), "and it settles once")
        // onConfigureFailed: the session "is considered to be closed"; nothing
        // is carried and nothing is closed.
        val stream = owned.request(CaptureResource.CameraStream)
        assertIs<AdoptOutcome.Failed>(owned.fail(stream, "onConfigureFailed", null))
        assertEquals(listOf<Any>(p.camera), p.closed(), "a failed capture session is not closed again")
        owned.close(null)
        assertEquals(listOf<Any>(p.camera), p.closed())
    }

    private fun o12_losingAnInstalledDeviceClosesItOnceAndSkipsItLater(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        owned.acquire(CaptureResource.Worker) { p.handle(p.worker) }
        val ticket = owned.request(CaptureResource.Camera)
        owned.adopt(ticket, p.handle(p.camera))
        // onDisconnected AFTER onOpened: the camera is gone; close it now.
        assertEquals(LostOutcome.Released, owned.lost(CaptureResource.Camera, "onDisconnected"))
        assertEquals(listOf<Any>(p.camera), p.closed())
        assertIs<LostOutcome.Ignored>(owned.lost(CaptureResource.Camera, "onDisconnected again"))
        owned.close(null)
        assertEquals(listOf<Any>(p.camera, p.worker), p.closed(), "the lost device is not closed again by close")
    }

    private fun o13_closeRetainsTheWorkerUntilTheLateCallbackSettles(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        owned.acquire(CaptureResource.Worker) { p.handle(p.worker) }
        val ticket = owned.request(CaptureResource.Camera)
        owned.close(null)
        assertTrue(owned.isClosed)
        assertEquals(1, owned.pending)
        assertEquals(emptyList(), p.closed(), "the worker is RETAINED: quitting it would drop the coming onOpened")
        assertTrue(rig.dispatcher.isAlive, "the dispatcher is alive to deliver it")
        val delivered = rig.dispatcher.deliver { assertIs<AdoptOutcome.Retired>(owned.adopt(ticket, p.handle(p.camera))) }
        assertTrue(delivered, "and it was delivered")
        assertEquals(listOf<Any>(p.camera, p.worker), p.closed(), "the late device closes, then the worker it needed")
        assertEquals(0, owned.pending)
        assertEquals(listOf("late-close:Camera", "late-close:Worker"), rig.late.all(), "both observable after close returned")
    }

    private fun o14_aLateCloseFailureIsObservableNotLost(make: Make) {
        val rig = Rig()
        val p = rig.platform
        val owned = make.make(rig)
        val ticket = owned.request(CaptureResource.Camera)
        val ending = owned.close(null)
        assertEquals(null, ending, "nothing had failed when close returned")
        owned.adopt(ticket, p.handle(p.camera, IllegalStateException("late close failed")))
        assertEquals(
            listOf("late-close:Camera:late close failed"),
            rig.late.all(),
            "a failure after close returned rides its own sink, not a return value already consumed",
        )
    }

    private fun runContract(make: Make) {
        o1_closesChildrenBeforeWhatTheyHangFrom(make)
        o2_aPartialAcquisitionUnwindsOnlyWhatItAcquired(make)
        o3_closingTwiceClosesNothingTwice(make)
        o4_cleanupContinuesPastAFailureAndKeepsThePrimary(make)
        o5_withNoPrimaryTheFirstCleanupFailureIsTheOutcome(make)
        o6_anAcquisitionThatRacesRetirementOwnsNothing(make)
        o7_anAdoptedResourceIsOwnedInAcquisitionOrder(make)
        o8_aDeviceDeliveredAfterRetirementIsClosedNotInstalled(make)
        o9_aDuplicateDeliveryOfTheLiveDeviceClosesNothing(make)
        o10_aForeignDeliveryOfADifferentResourceIsDisposedHere(make)
        o11_aFailureInsteadOfOpenedClosesWhatItCarries(make)
        o12_losingAnInstalledDeviceClosesItOnceAndSkipsItLater(make)
        o13_closeRetainsTheWorkerUntilTheLateCallbackSettles(make)
        o14_aLateCloseFailureIsObservableNotLost(make)
    }

    private val real = Make { rig -> OrderedCaptureOwnership(GENERATION, rig.dispatcher, rig.late) }
    private val conforming = Make { rig -> ConformingOwnership(GENERATION, rig.dispatcher, rig.late) }
    private val counting = Make { rig -> CountingOwnership(GENERATION, rig.dispatcher) }

    // ---- RED, and satisfiability -------------------------------------------------

    @Test
    fun o_theCaptureOwnershipMeetsItsContract() = runContract(real)

    @Test
    fun s_theContractIsSatisfiableByAConformingFixture() = runContract(conforming)

    // ---- discrimination: right totals, wrong facts -----------------------------

    @Test
    fun m1_closingInAcquisitionOrderFails() = assertFailsWithAssertion { o1_closesChildrenBeforeWhatTheyHangFrom(counting) }

    @Test
    fun m2_aSecondCloseThatClosesAgainFails() = assertFailsWithAssertion { o3_closingTwiceClosesNothingTwice(counting) }

    @Test
    fun m3_droppingTheCleanupFailuresFails() {
        assertFailsWithAssertion { o4_cleanupContinuesPastAFailureAndKeepsThePrimary(counting) }
        assertFailsWithAssertion { o5_withNoPrimaryTheFirstCleanupFailureIsTheOutcome(counting) }
    }

    @Test
    fun m4_installingWhateverArrivesFails() {
        assertFailsWithAssertion { o8_aDeviceDeliveredAfterRetirementIsClosedNotInstalled(counting) }
        assertFailsWithAssertion { o9_aDuplicateDeliveryOfTheLiveDeviceClosesNothing(counting) }
        assertFailsWithAssertion { o10_aForeignDeliveryOfADifferentResourceIsDisposedHere(counting) }
        assertFailsWithAssertion { o11_aFailureInsteadOfOpenedClosesWhatItCarries(counting) }
    }

    @Test
    fun m5_quittingTheDispatcherAtCloseLeaksTheLateDeviceAndFails() {
        // The stand-in settles the right NUMBER of tickets and closes the right
        // number of handles at close. It quits the dispatcher at close, so the
        // platform's onOpened is refused and the CameraDevice is never closed.
        val rig = Rig()
        val owned = counting.make(rig)
        owned.acquire(CaptureResource.Worker) { rig.platform.handle(rig.platform.worker) }
        val ticket = owned.request(CaptureResource.Camera)
        owned.close(null)
        val delivered = rig.dispatcher.deliver { owned.adopt(ticket, rig.platform.handle(rig.platform.camera)) }
        assertEquals(false, delivered, "the quit dispatcher refused the delivery")
        assertEquals(listOf<Any>(rig.platform.worker), rig.platform.closed(), "and the camera leaked -- never closed")
        assertFailsWithAssertion { o13_closeRetainsTheWorkerUntilTheLateCallbackSettles(counting) }
    }

    private fun assertFailsWithAssertion(body: () -> Unit) {
        val thrown =
            try {
                body()
                null
            } catch (assertion: AssertionError) {
                assertion
            }
        assertTrue(thrown != null, "this row is satisfied by an ownership that only gets the totals right")
    }

    private companion object {
        const val GENERATION = 1L
    }
}
