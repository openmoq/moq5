package org.openmoq.moq5.jni

import org.openmoq.moq5.EndpointState
import org.openmoq.moq5.EndpointTerminal
import org.openmoq.moq5.MoqErrorCode
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.MoqVersion
import org.openmoq.moq5.engine.BackendDrainResult
import org.openmoq.moq5.engine.BackendAttachResult
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.OverflowPolicy
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BackendTrackEventResult
import org.openmoq.moq5.NativeBridgeInitializationException
import org.openmoq.moq5.NativeEndpointOffer
import org.openmoq.moq5.jni.ReceiverEventScalars
import org.openmoq.moq5.ByteRange
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.MediaReceiver
import org.openmoq.moq5.engine.EndpointBackend
import org.openmoq.moq5.engine.BackendObjectResult
import org.openmoq.moq5.engine.MediaObjectCodec
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.engine.BackendOpResult
import org.openmoq.moq5.engine.BackendSnapshotResult
import org.openmoq.moq5.engine.BackendStopResult
import org.openmoq.moq5.engine.BackendWaitResult
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.JniEndpointBackend
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.TransportProtocol
import org.openmoq.moq5.VersionOffer
import kotlin.time.Duration.Companion.milliseconds
import org.openmoq.moq5.EndpointCreateFailedException
import org.openmoq.moq5.MoqEndpoint
import org.openmoq.moq5.TrustPolicyUnsupportedException
import org.openmoq.moq5.trust.PlatformTrustSupport
import org.openmoq.moq5.trust.TestCerts

/**
 * The child process the selector rows drive.
 *
 * It exists because the constructor selector is read once at load time: a
 * process can only ever demonstrate ONE selector state, so each state needs its
 * own JVM. The child prints machine-readable lines and the parent asserts on
 * them, so a child that dies before reaching its oracle can never read as a
 * pass.
 */
internal object EndpointChildMain {

    /** Mirrors `MOQ5_REGISTRY_MAX_ENTRIES`. */
    private const val MAX_ENTRIES: Int = 4096

    private fun slotOf(token: Long): Long = (token ushr 8) and 0xFFFFFFL

    @JvmStatic
    fun main(args: Array<String>) {
        val scenario = args.firstOrNull() ?: "load"
        try {
            when (scenario) {
                "load" -> {
                    System.load(BridgeDsos.path("moq5_jni"))
                    println("RESULT loaded")
                }
                "scripted" -> scripted()
                "discriminator" -> discriminator()
                "capacity" -> capacity()
                "backend" -> backend()
                "stoprefuse" -> stopRefuse()
                "interrupt" -> interrupted()
                "wakerace" -> wakeRace()
                "waitclosed" -> waitExact(EndpointAbi.CLOSED, "MOQ_ERR_CLOSED")
                "waitodd" -> waitExact(EndpointAbi.ODD, "an unexpected negative result")
                "snapfuture" -> snapshotFuture()
                "retired" -> retired()
                "foreign" -> foreign()
                "publishfail" -> publishFail()
                "backendmap" -> backendMap()
                "backendsnap" -> backendSnapshot()
                "backendwaitodd" ->
                    backendWaitExact(
                        BackendWaitResult.Bridge(BridgeFailure("wait", EndpointAbi.ODD)),
                        "a result outside the four-value wait contract",
                    )
                "backendwaitclosed" ->
                    backendWaitExact(BackendWaitResult.CLOSED, "MOQ_ERR_CLOSED")
                "backendinterrupt" -> backendInterrupt()
                "setinterrupted" -> setInterrupted()
                "setinterruptedstale" -> setInterruptedStale()
                "backendsetinterrupted" -> backendSetInterrupted()
                "drain" -> drain()
                "drainclosed" -> drainClosed()
                "draindone" -> drainDone()
                "drainunsupported" -> drainUnsupported()
                "backenddrain" -> backendDrain()
                "trustedroute" -> trustedRoute()
                "trustedcallback" -> trustedCallback()
                "trustedfault" -> trustedFault()
                "trustedshadow" -> trustedShadow()
                "trustedscripted" -> trustedScripted()
                "trustedpublishfail" -> trustedPublishFail()
                "trustedcapacity" -> trustedCapacity()
                "trustedrefusals" -> trustedRefusals()
                "trustedctorerror" -> trustedCtorError()
                "trustedctornull" -> trustedCtorNull()
                "publicopen" -> publicOpen()
                "publicopenfresh" -> publicOpenFresh()
                "createunready" -> createUnready()
                "publicopendefaults" -> publicOpenDefaults()
                "publicopenconfigured" -> publicOpenConfigured()
                "publicopenexact" -> publicOpenExact()
                "publicopenwide" -> publicOpenWide()
                "publicopentransportdefaults" -> publicOpenTransportDefaults()
                "publicopentransport" -> publicOpenTransport()
                "publicopentransportoverride" -> publicOpenTransportOverride()
                "createbadtransport" -> createBadTransport()
                "receiverattach" -> receiverAttach()
                "receiverclosestale" -> receiverCloseStale()
                "receivermalformed" -> receiverMalformed()
                "receiverinsertfault" -> receiverInsertFault()
                "receiversecond" -> receiverSecond()
                "backendreceiver" -> backendReceiver()
                "receiverdiscovery" -> receiverDiscovery()
                "receivertracktokens" -> receiverTrackTokens()
                "backendreceiverevents" -> backendReceiverEvents()
                "receivereventmalformed" -> receiverEventMalformed()
                "receivertrackinsertfault" -> receiverTrackInsertFault()
                "receiverobjectnoversion" -> receiverObjectNoVersion()
                "receiverobjectundesired" -> receiverObjectUndesired()
                "receiverobjectloc" -> receiverObjectLoc()
                "receiverobjectcmaf" -> receiverObjectCmaf()
                "receiverobjectmalformed" -> receiverObjectMalformed()
                "receiverobjectinterrupted" -> receiverObjectInterrupted()
                "receiverobjectidentity" -> receiverObjectIdentity()
                "receiverobjectfirstsight" -> receiverObjectFirstSight()
                "receiverobjectownership" -> receiverObjectOwnership()
                "receivercontrolsubscribe" -> receiverControlSubscribe()
                "receivercontrolpurge" -> receiverControlPurge()
                "receivercontrolended" -> receiverControlEnded()
                "receivercontrolremoved" -> receiverControlRemoved()
                "receivercontrolnamespace" -> receiverControlNamespace()
                "receivercontrolmalformed" -> receiverControlMalformed()
                "receivertimelinesap" -> receiverTimelineSap()
                "receivertimelinemt" -> receiverTimelineMt()
                "receivertimelinesapdefer" -> receiverTimelineSapDefer()
                "receivertimelinemtdefer" -> receiverTimelineMtDefer()
                "receivertimelinemalformed" -> receiverTimelineMalformed()
                "receiverstats" -> receiverStats()
                "receiverstatscomplete" -> receiverStatsComplete()
                "receiverstatsmalformed" -> receiverStatsMalformed()
                "senderattach" -> senderAttach()
                "sendertracks" -> senderTracks()
                "senderwouldblock" -> senderWouldBlock()
                "sendermalformed" -> senderMalformed()
                "registrycrosstype" -> registryCrossType()
                "sendertrackorphan" -> senderTrackOrphan()
                "senderoutputacquire" -> senderOutputAcquire()
                "describeallocpending" -> resultAllocFail(describe = true, pending = true, atIndex = 0)
                "describeallocbare" -> resultAllocFail(describe = true, pending = false, atIndex = 0)
                "objectallocpending" -> resultAllocFail(describe = false, pending = true, atIndex = 0)
                "objectallocbare" -> resultAllocFail(describe = false, pending = false, atIndex = 0)
                "objectallocpartial" -> resultAllocFail(describe = false, pending = false, atIndex = 1)
                "cmafstatusonly" -> cmafStatusOnly()
                "cmafstatuspublic" -> cmafStatusPublic()
                "cmafstatuspublicbody" -> cmafStatusPublic(PublicFault.Body)
                "cmafstatuspublicboth" -> cmafStatusPublic(PublicFault.BodyAndCleanup)
                "cmafstatuspubliccleanup" -> cmafStatusPublic(PublicFault.CleanupOnly)
                "cmafstatuspublicsetup" -> cmafStatusPublic(PublicFault.SetupAfterAttach)
                "cmafstatuspublicaudit" -> cmafStatusPublic(PublicFault.BodyCleanupAudit)
                "publicopentransportrawpath" -> publicOpenTransportRawPath()
                "createnegativehandshake" -> createNegativeHandshake()
                "createmalformed" -> createMalformed()
                "publicct" -> publicCt()
                else -> {
                    println("RESULT unknown-scenario $scenario")
                    return
                }
            }
        } catch (e: UnsatisfiedLinkError) {
            println("LOADFAIL " + e.message.orEmpty().replace('\n', ' '))
            return
        } catch (e: Throwable) {
            println("RESULT threw ${e::class.java.name}: ${e.message}")
            return
        }
    }

    private fun check(cond: Boolean, what: String) {
        if (!cond) {
            println("RESULT failed: $what")
            throw IllegalStateException(what)
        }
    }

    private fun scripted() {
        System.load(BridgeDsos.path("moq5_jni"))
        val a = longArrayOf(0L)
        val b = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, a) == EndpointAbi.OK,
            "the scripted target must construct")
        check(a[0] != 0L, "a successful construction publishes a token")
        check(EndpointAbi.kindOf(a[0]) == EndpointAbi.KIND_ROOT,
            "the token must be a ROOT of the private registry")
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, b) == EndpointAbi.OK,
            "a second root must construct")
        check(a[0] != b[0], "distinct roots carry distinct tokens")
        check(NativeBridge.nativeEndpointRelease(a[0]) == EndpointAbi.OK, "the first root releases")
        check(NativeBridge.nativeEndpointRelease(a[0]) < 0,
            "a retired token must never address a root again, and must not clean twice")
        check(NativeBridge.nativeEndpointRelease(b[0]) == EndpointAbi.OK, "the second root releases")
        println("RESULT ok")
    }

    /**
     * The registry's own capacity refusal, forced for real.
     *
     * The point is not the result code: it is that the refusal happens BEFORE
     * anything is constructed and consumes nothing. Slot reuse is the
     * observable that proves it -- a reservation that had been consumed, or an
     * endpoint left owned by nobody, would move the next token.
     */
    private fun capacity() {
        System.load(BridgeDsos.path("moq5_jni"))
        val tokens = ArrayList<Long>(MAX_ENTRIES)
        val out = longArrayOf(0L)
        while (tokens.size < MAX_ENTRIES) {
            val rc = NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, out)
            if (rc != EndpointAbi.OK) {
                check(false, "setup: filling stopped early at ${tokens.size} with rc=$rc")
                return
            }
            tokens.add(out[0])
        }
        // The registry is full: the next construction must be refused.
        out[0] = 0L
        val refused = NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, out)
        check(refused < 0, "a full registry must refuse (rc=$refused)")
        check(out[0] == 0L, "a refused construction must publish nothing")

        // Free exactly one slot. If the refusal had consumed a slot or a
        // generation, the next token could not land on the slot just released.
        val freed = tokens.removeAt(tokens.size - 1)
        check(NativeBridge.nativeEndpointRelease(freed) == EndpointAbi.OK, "the root releases")
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, out) == EndpointAbi.OK,
            "one released slot must admit exactly one new root")
        check(slotOf(out[0]) == slotOf(freed),
            "the new root must reuse the released slot: nothing was consumed by the refusal")
        check(out[0] != freed, "the reused slot must carry a new generation")
        tokens.add(out[0])

        for (t in tokens) {
            check(NativeBridge.nativeEndpointRelease(t) == EndpointAbi.OK, "every root releases")
        }
        // And the registry is usable again, exactly as before the refusal.
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, out) == EndpointAbi.OK,
            "the registry must be reusable after a capacity refusal")
        check(NativeBridge.nativeEndpointRelease(out[0]) == EndpointAbi.OK, "and release")
        println("RESULT ok")
    }

    /**
     * Publication fails AFTER a real root has been constructed.
     *
     * By then the root is already COMMITTED, so the endpoint is owned by the
     * registry and the failure is an ordered teardown of an owned root: stop,
     * then retire, whose cleanup destroys it exactly once.
     *
     * The oracle is ownership, not the result: the caller learns no token, the
     * pending Java exception is not swallowed, nothing is left live, and the
     * retired slot is reusable with a new generation.
     */
    private fun publishFail() {
        System.load(BridgeDsos.path("moq5_jni"))
        val out = longArrayOf(0L)
        var threw = false
        try {
            NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, out)
        } catch (e: ArrayIndexOutOfBoundsException) {
            threw = true
        }
        check(threw, "the injected publication failure must surface as its real exception")
        check(out[0] == 0L, "no token may reach the caller")

        // The LIFECYCLE oracle. The next construction reports any latched
        // violation by name instead of succeeding: a branch that dropped the
        // endpoint instead of destroying it would leave it and its probe alive,
        // which registry conservation alone cannot see.
        //
        // The injection is one-shot, so the very next construction is ordinary.
        // It must land on the FIRST slot the registry hands out: that proves the
        // root was RETIRED rather than left live, and that its slot went back to
        // the free list.
        val first = longArrayOf(0L)
        val rc = NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, first)
        check(rc != EndpointAbi.LC_LEAKED,
            "the failed publication leaked the endpoint: it was never destroyed")
        check(rc != EndpointAbi.LC_DESTROY_BEFORE_STOP,
            "the failed publication destroyed the endpoint before stopping it")
        check(rc != EndpointAbi.LC_DOUBLE_DESTROY,
            "the failed publication destroyed the endpoint twice")
        check(rc == EndpointAbi.OK,
            "construction must still work after the failed publication (rc=$rc)")
        check(slotOf(first[0]) == 0L,
            "the retired root must leave slot 0 free; got slot ${slotOf(first[0])}")

        // And the ordinary lifecycle still holds on that slot.
        check(NativeBridge.nativeEndpointRelease(first[0]) == EndpointAbi.OK, "the root releases")
        val again = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, again) == EndpointAbi.OK,
            "and the slot is reusable")
        check(slotOf(again[0]) == 0L, "the released slot is reused")
        check(again[0] != first[0], "with a new generation")
        check(NativeBridge.nativeEndpointRelease(again[0]) == EndpointAbi.OK, "and releases")
        println("RESULT ok")
    }

    /**
     * The stateful boundary on a live root.
     *
     * The scripted facade supplies the INPUTS -- a future negotiated version, a
     * terminal with bit 63 and bit 0 set, and a service function so a wake is
     * answered -- and the assertions are about what the JNI does with them: one
     * coherent record, full width, verbatim results.
     */
    private fun backend() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: the scripted target must construct")
        val token = t[0]

        // WAIT, verbatim. With no wake pending the scripted facade times out.
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.DONE,
            "an unwoken wait must report MOQ_DONE verbatim")

        // WAKE is any-thread; a woken wait reports activity, not a timeout.
        check(NativeBridge.nativeEndpointWake(token) == EndpointAbi.OK, "wake must be accepted")
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.OK,
            "a wait after a wake must report MOQ_OK")
        val other = Thread { check(NativeBridge.nativeEndpointWake(token) == EndpointAbi.OK,
            "wake must be accepted from ANY thread") }
        other.start(); other.join()
        // Consume that wake, so the endpoint's activity level is deterministic
        // for the rows below rather than depending on what a prior row left.
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.OK,
            "the any-thread wake must be observable by the owning thread")

        // SNAPSHOT: one record, full width, nothing narrowed.
        val s = LongArray(4)
        check(NativeBridge.nativeEndpointSnapshot(token, s) == EndpointAbi.OK, "snapshot must succeed")
        // The version reachable here is ABSENT: the endpoint latches a negotiated
        // version only when its pump observes an ESTABLISHED session, which a
        // bare scripted facade never reaches. What this row can prove is that
        // the record's documented 0-is-absent value crosses as 0 and is not
        // turned into something else; a FUTURE nonzero version needs a scripted
        // establishment, and is reported as a bootstrap blocker rather than
        // faked here.
        check(s[1] == 0L, "an absent negotiated version must cross as 0; got ${s[1]}")
        check(s[3] == EndpointAbi.SCRIPTED_DETAIL,
            "all 64 terminal-detail bits must cross as raw bits; got ${java.lang.Long.toHexString(s[3])}")
        check(s[0] >= 0 && s[2] >= 0, "state and reason discriminators must not be negative")
        check(NativeBridge.nativeEndpointSnapshot(token, LongArray(3)) == EndpointAbi.INVAL,
            "an undersized output array is refused")

        // DESTROY BEFORE STOP is refused by the REGISTRY's own state category,
        // and the root is left valid and uncleaned -- destroy never stops on
        // the caller's behalf, so the two operations stay distinguishable.
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.REG_STATE,
            "an unready root must be refused by registry STATE")
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.DONE,
            "a refused destroy must leave the root live and usable")

        // STOP is carried verbatim, and is idempotent in C.
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "stop must succeed")
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK,
            "a repeated stop is folded into MOQ_OK by C and is carried verbatim")

        // Only now is the root eligible, and exactly one retirement owner wins.
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "destroy must succeed")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.REG_STALE,
            "a retired token must not be destroyed twice")
        println("RESULT ok")
    }

    /**
     * A REFUSED stop, supplied as an input the product cannot mint here.
     *
     * What is asserted is the JNI's own behaviour: the refusal is carried
     * exactly, nothing is marked ready, the root stays live and usable, and
     * destroy still refuses because the endpoint was never quiesced.
     */
    private fun stopRefuse() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: construct")
        val token = t[0]

        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.WRONG_STATE,
            "a refused stop must be carried EXACTLY, not normalised")
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.DONE,
            "a refused stop must leave the root live and usable")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.REG_STATE,
            "a root whose stop refused was never marked ready and must not be cleaned")
        check(NativeBridge.nativeEndpointRelease(token) == EndpointAbi.WRONG_STATE,
            "release must report the same refusal and retire nothing")
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.DONE,
            "and the root is STILL live after the refused release")
        println("RESULT ok")
    }

    /**
     * The latch row through the real bridge: set from the authority thread,
     * sticky and beating a raised level, cleared, and -- because the C contract
     * is any-thread -- accepted from a thread that is NOT the root's authority,
     * which an acquiring row would refuse with a registry thread error.
     */
    private fun setInterrupted() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: construct")
        val token = t[0]
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.DONE, "setup: an unlatched wait times out")

        check(NativeBridge.nativeEndpointSetInterrupted(token, true) == EndpointAbi.OK, "set must answer MOQ_OK")
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.INTERRUPTED,
            "a latched wait must answer MOQ_ERR_INTERRUPTED exactly")
        check(NativeBridge.nativeEndpointSetInterrupted(token, true) == EndpointAbi.OK, "set is idempotent")
        check(NativeBridge.nativeEndpointWake(token) == EndpointAbi.OK, "wake while latched")
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.INTERRUPTED,
            "the latch beats a raised level")

        check(NativeBridge.nativeEndpointSetInterrupted(token, false) == EndpointAbi.OK, "clear must answer MOQ_OK")
        val cleared = NativeBridge.nativeEndpointWait(token, 0L)
        check(cleared != EndpointAbi.INTERRUPTED, "a cleared latch restores normal blocking; got $cleared")

        var offThread = Int.MIN_VALUE
        val other = Thread { offThread = NativeBridge.nativeEndpointSetInterrupted(token, true) }
        other.start()
        other.join()
        check(offThread == EndpointAbi.OK, "the row is any-thread (leased, like wake); got $offThread")
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.INTERRUPTED,
            "the latch set from another thread is the same latch")
        check(NativeBridge.nativeEndpointSetInterrupted(token, false) == EndpointAbi.OK, "cleanup: clear")

        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "cleanup: destroy")
        println("RESULT ok")
    }

    /** After destroy the token is stale, and a zero token malformed: the row answers each registry refusal as an Int. */
    private fun setInterruptedStale() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: construct")
        val token = t[0]
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "setup: stop")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "setup: destroy")
        val rc = NativeBridge.nativeEndpointSetInterrupted(token, true)
        check(rc == EndpointAbi.REG_STALE, "a stale token must be refused by the registry exactly; got $rc")
        val zero = NativeBridge.nativeEndpointSetInterrupted(0L, false)
        check(zero == EndpointAbi.REG_INVALID, "a never-issued token is malformed to the registry; got $zero")
        println("RESULT ok")
    }

    /** The backend seam: MOQ_OK is Ok; a registry refusal is Bridge with the exact code; nothing else exists. */
    private fun backendSetInterrupted() {
        System.load(BridgeDsos.path("moq5_jni"))
        val b = JniEndpointBackend.create(EndpointAbi.UNSERVABLE)
        check(b.setInterrupted(true) == BackendOpResult.Ok, "set maps to Ok")
        check(b.waitForActivity(0L) == BackendWaitResult.INTERRUPTED, "and the real wait is latched")
        check(b.setInterrupted(false) == BackendOpResult.Ok, "clear maps to Ok")
        check(b.waitForActivity(0L) != BackendWaitResult.INTERRUPTED, "and the real wait is unlatched")
        check(b.stop() == BackendStopResult.Stopped, "cleanup: stop")
        check(b.destroy() == BackendOpResult.Ok, "cleanup: destroy")
        val stale = b.setInterrupted(true)
        check(stale is BackendOpResult.Bridge && stale.failure == BridgeFailure("set_interrupted", EndpointAbi.REG_STALE),
            "a refused row is a Bridge failure carrying the private code verbatim; got $stale")
        println("RESULT ok")
    }

    /**
     * The drain row through the real bridge against the host facade, which
     * reports a PENDING transport: a zero timeout takes one probe pass and
     * answers MOQ_DONE; the latch answers ahead of any probe; and -- because
     * the C contract is any-thread for the application -- the row is accepted
     * from a thread that is NOT the root's authority, which an acquiring row
     * would refuse with the registry thread error.
     */
    private fun drain() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK, "setup: construct")
        val token = t[0]
        val pending = NativeBridge.nativeEndpointDrain(token, 0L)
        check(pending == EndpointAbi.DONE, "a pending transport with no time left is MOQ_DONE; got $pending")
        // The signed/unsigned boundary: a negative timeout is refused before
        // any lease and the live token stays usable.
        val negative = NativeBridge.nativeEndpointDrain(token, -1L)
        check(negative == EndpointAbi.INVAL, "a negative timeout must be MOQ_ERR_INVAL, never a huge unsigned wait; got $negative")
        check(NativeBridge.nativeEndpointDrain(token, 0L) == EndpointAbi.DONE, "the root is still live after the refused call")

        check(NativeBridge.nativeEndpointSetInterrupted(token, true) == EndpointAbi.OK, "setup: latch")
        val latched = NativeBridge.nativeEndpointDrain(token, 1_000_000L)
        check(latched == EndpointAbi.INTERRUPTED, "the latch answers first, whatever the timeout; got $latched")
        check(NativeBridge.nativeEndpointSetInterrupted(token, false) == EndpointAbi.OK, "setup: clear")

        var offThread = Int.MIN_VALUE
        val other = Thread { offThread = NativeBridge.nativeEndpointDrain(token, 0L) }
        other.start()
        other.join()
        check(offThread == EndpointAbi.DONE, "the row is any-thread (leased, like wake); got $offThread")

        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "cleanup: destroy")
        println("RESULT ok")
    }

    /** After stop the endpoint is closed: MOQ_ERR_CLOSED; after destroy the token is stale: the registry's Int. */
    private fun drainClosed() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK, "setup: construct")
        val token = t[0]
        check(NativeBridge.nativeEndpointDrain(token, 0L) == EndpointAbi.DONE, "setup: live and pending before the stop")
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "setup: stop")
        val closed = NativeBridge.nativeEndpointDrain(token, 1_000_000L)
        check(closed == EndpointAbi.CLOSED, "a stopped endpoint answers MOQ_ERR_CLOSED at once; got $closed")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "setup: destroy")
        val stale = NativeBridge.nativeEndpointDrain(token, 0L)
        check(stale == EndpointAbi.REG_STALE, "a stale token is the registry's refusal exactly; got $stale")
        println("RESULT ok")
    }

    /** The facade reports an idle transport: the product's loop answers MOQ_OK. */
    private fun drainDone() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK, "setup: construct")
        val token = t[0]
        val rc = NativeBridge.nativeEndpointDrain(token, 0L)
        check(rc == EndpointAbi.OK, "an idle transport with no attachments is flushed; got $rc")
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "cleanup: destroy")
        println("RESULT ok")
    }

    /** The facade cannot prove a flush: MOQ_ERR_UNSUPPORTED, verbatim. */
    private fun drainUnsupported() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK, "setup: construct")
        val token = t[0]
        val rc = NativeBridge.nativeEndpointDrain(token, 1_000_000L)
        check(rc == EndpointAbi.ERR_UNSUPPORTED, "an unprovable flush is MOQ_ERR_UNSUPPORTED; got $rc")
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "cleanup: destroy")
        println("RESULT ok")
    }

    /** The backend seam maps each documented answer to its own value and a refused row to Bridge. */
    private fun backendDrain() {
        System.load(BridgeDsos.path("moq5_jni"))
        val b = JniEndpointBackend.create(EndpointAbi.UNSERVABLE)
        check(b.drain(0L) == BackendDrainResult.TimedOut, "MOQ_DONE maps to TimedOut")
        check(b.setInterrupted(true) == BackendOpResult.Ok, "setup: latch")
        check(b.drain(1_000_000L) == BackendDrainResult.Interrupted, "MOQ_ERR_INTERRUPTED maps to Interrupted")
        check(b.setInterrupted(false) == BackendOpResult.Ok, "setup: clear")
        check(b.stop() == BackendStopResult.Stopped, "setup: stop")
        check(b.drain(1_000_000L) == BackendDrainResult.Closed, "MOQ_ERR_CLOSED maps to Closed")
        check(b.destroy() == BackendOpResult.Ok, "setup: destroy")
        val stale = b.drain(0L)
        check(stale is BackendDrainResult.Bridge && stale.failure == BridgeFailure("drain", EndpointAbi.REG_STALE),
            "a refused row is a Bridge failure carrying the private code verbatim; got $stale")
        println("RESULT ok")
    }

    /** An armed interrupt latch: MOQ_ERR_INTERRUPTED must cross exactly. */
    private fun interrupted() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: construct")
        val token = t[0]
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.INTERRUPTED,
            "an interrupted wait must report MOQ_ERR_INTERRUPTED exactly, not a timeout")
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "cleanup: destroy")
        println("RESULT ok")
    }

    /**
     * The wake/retirement composition, driven deterministically.
     *
     * The two sides rendezvous inside the bridge, so the sequence is fixed
     * whatever the JVM schedules: the lease is taken and the wake body is held;
     * the authority thread then begins retirement, which invalidates the token
     * and waits for pins; the wake body checks that no cleanup ran while its
     * lease was held, releases, and only then may cleanup run -- exactly once.
     * A violation is named by the lifecycle ledger, not inferred from timing.
     */
    private fun wakeRace() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: construct")
        val token = t[0]
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "setup: stop marks ready")

        // TWO JVM workers, both crossing the registered JNI boundary. Which one
        // becomes the lease holder is the scheduler's choice; the protocol is
        // not -- the second arrival cannot attempt its lease until the registry
        // milestone says the tree is RETIRING.
        val results = IntArray(2)
        val w0 = Thread { results[0] = NativeBridge.nativeEndpointWake(token) }
        val w1 = Thread { results[1] = NativeBridge.nativeEndpointWake(token) }
        w0.start(); w1.start()
        val destroyRc = NativeBridge.nativeEndpointDestroy(token)
        w0.join(); w1.join()

        check(destroyRc == EndpointAbi.OK,
            "retirement must complete once the held lease drops; got $destroyRc")
        // EXACT MULTIPLICITY: one held wake succeeded, one probe was refused
        // while that lease was still held.
        val ok = results.count { it == EndpointAbi.OK }
        val state = results.count { it == EndpointAbi.REG_STATE }
        check(ok == 1,
            "exactly one worker may hold the lease and succeed; got ${results.toList()}")
        check(state == 1,
            "the other worker must be refused with registry STATE while the lease is " +
                "held; got ${results.toList()}")

        // After retirement the token answers exact STALE, not STATE.
        check(NativeBridge.nativeEndpointWake(token) == EndpointAbi.REG_STALE,
            "the retired token must answer STALE after retirement completes")
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.REG_STALE,
            "wait on the retired token must answer STALE")

        // The rendezvous is ONE-SHOT: this second root's teardown is ordinary
        // and must not re-enter it, which the ledger would name.
        val next = longArrayOf(0L)
        val rc = NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, next)
        check(rc != EndpointAbi.LC_CLEANUP_RACED,
            "cleanup ran while the any-thread lease was still held")
        check(rc != EndpointAbi.LC_RETIRING_ACCEPTED,
            "a lease during retirement was not refused with registry STATE")
        check(rc != EndpointAbi.LC_MILESTONE_REPEATED,
            "the one-shot rendezvous fired again on a later retirement")
        check(rc == EndpointAbi.OK, "the registry must still be usable (rc=$rc)")
        check(NativeBridge.nativeEndpointStop(next[0]) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(next[0]) == EndpointAbi.OK, "cleanup: destroy")
        val after = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, after) == EndpointAbi.OK,
            "and the ledger is still clean after that ordinary teardown")
        check(NativeBridge.nativeEndpointStop(after[0]) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(after[0]) == EndpointAbi.OK, "cleanup: destroy")
        println("RESULT ok")
    }

    /**
     * A wait result the current product cannot mint, driven through the REAL
     * registered method with its real token acquire and release.
     */
    private fun waitExact(expected: Int, what: String) {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: construct")
        val token = t[0]
        val rc = NativeBridge.nativeEndpointWait(token, 0L)
        check(rc == expected,
            "$what must cross exactly; expected $expected, got $rc")
        // The token is still live and the ordinary lifecycle still holds.
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "cleanup: destroy")
        println("RESULT ok")
    }

    /**
     * A structurally valid service record carrying a FUTURE version, driven
     * through the real snapshot method with its real acquire, output validation
     * and widening. This proves JNI TRANSPORT of a future record -- it is not a
     * claim that an unregistered profile can be negotiated.
     */
    private fun snapshotFuture() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: construct")
        val token = t[0]
        val s = LongArray(4)
        check(NativeBridge.nativeEndpointSnapshot(token, s) == EndpointAbi.OK, "snapshot")
        // ALL FOUR fields of a coherent record, not two of them.
        check(s[0] == EndpointAbi.STATE_CLOSED,
            "the state must cross exactly; expected ${EndpointAbi.STATE_CLOSED}, got ${s[0]}")
        check(s[1] == EndpointAbi.FUTURE_VERSION,
            "a future nonzero version must cross exactly; expected " +
                "${EndpointAbi.FUTURE_VERSION}, got ${s[1]}")
        check(s[2] == EndpointAbi.TERMINAL_PROTOCOL,
            "the terminal reason must cross exactly; expected " +
                "${EndpointAbi.TERMINAL_PROTOCOL}, got ${s[2]}")
        check(s[3] == EndpointAbi.SCRIPTED_DETAIL,
            "the detail must keep all 64 bits; got ${java.lang.Long.toHexString(s[3])}")
        check(NativeBridge.nativeEndpointSnapshot(token, LongArray(3)) == EndpointAbi.INVAL,
            "output validation still runs in front of the fixture record")
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "cleanup: destroy")
        println("RESULT ok")
    }

    /** After retirement the token addresses nothing, through every operation. */
    private fun retired() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: construct")
        val token = t[0]
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "setup: stop")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "setup: destroy")

        // Every stateful operation, and the any-thread lease, must refuse it by
        // registry category -- none of them may reach an endpoint pointer.
        // EXACT registry categories, not merely "negative": a collapsed error or
        // an early fixture refusal would pass a sign test and prove nothing.
        check(NativeBridge.nativeEndpointWait(token, 0L) == EndpointAbi.REG_STALE,
            "wait must refuse a retired token by STALE")
        check(NativeBridge.nativeEndpointWake(token) == EndpointAbi.REG_STALE,
            "wake must refuse a retired token by STALE")
        check(NativeBridge.nativeEndpointSnapshot(token, LongArray(4)) == EndpointAbi.REG_STALE,
            "snapshot must refuse a retired token by STALE")
        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.REG_STALE,
            "stop must refuse a retired token by STALE")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.REG_STALE,
            "destroy must refuse a retired token by STALE, and must not clean twice")

        // A forged token: same slot, a generation that was never issued.
        val forged = token xor 0x100000000L
        check(NativeBridge.nativeEndpointWait(forged, 0L) == EndpointAbi.REG_STALE,
            "wait must refuse a forged generation by STALE")
        check(NativeBridge.nativeEndpointSnapshot(forged, LongArray(4)) == EndpointAbi.REG_STALE,
            "snapshot must refuse a forged generation by STALE")

        // A WRONG-KIND token: a live root's slot and generation, spelled as a
        // child. It must be refused before any pointer is produced.
        val t2 = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t2) == EndpointAbi.OK,
            "setup: a second root")
        val wrongKind = (t2[0] and 0xFFL.inv()) or 2L   // MOQ5_KIND_CHILD_OWNED
        // EXACT: a re-spelled kind fails the entry-kind equality inside
        // reg_resolve_locked, so the answer is STALE -- not KIND, which belongs
        // to a valid token presented to an API expecting another kind, and no
        // such endpoint method exists in this tranche.
        val kindRc = NativeBridge.nativeEndpointWait(wrongKind, 0L)
        check(kindRc == EndpointAbi.REG_STALE,
            "a wrong-kind token must be refused by STALE; got $kindRc")
        check(NativeBridge.nativeEndpointStop(t2[0]) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(t2[0]) == EndpointAbi.OK, "cleanup: destroy")
        println("RESULT ok")
    }

    /** Only wake may run off the thread that created the root. */
    private fun foreign() {
        System.load(BridgeDsos.path("moq5_jni"))
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK,
            "setup: construct")
        val token = t[0]
        val results = IntArray(4)
        val other = Thread {
            results[0] = NativeBridge.nativeEndpointWait(token, 0L)
            results[1] = NativeBridge.nativeEndpointSnapshot(token, LongArray(4))
            results[2] = NativeBridge.nativeEndpointStop(token)
            results[3] = NativeBridge.nativeEndpointWake(token)
        }
        other.start(); other.join()
        check(results[0] == EndpointAbi.REG_THREAD,
            "wait off the authority thread must be refused by THREAD; got ${results[0]}")
        check(results[1] == EndpointAbi.REG_THREAD,
            "snapshot off the authority thread must be refused by THREAD; got ${results[1]}")
        check(results[2] == EndpointAbi.REG_THREAD,
            "stop off the authority thread must be refused by THREAD; got ${results[2]}")
        check(results[3] == EndpointAbi.OK, "wake is the any-thread exception and must succeed")

        // A foreign destroy is refused by THREAD as well, and leaves the root
        // valid: the refusals cost the endpoint nothing.
        val foreignDestroy = IntArray(1)
        val t3 = Thread { foreignDestroy[0] = NativeBridge.nativeEndpointDestroy(token) }
        t3.start(); t3.join()
        check(foreignDestroy[0] == EndpointAbi.REG_THREAD,
            "destroy off the authority thread must be refused by THREAD; got ${foreignDestroy[0]}")

        check(NativeBridge.nativeEndpointStop(token) == EndpointAbi.OK, "stop on the owning thread")
        check(NativeBridge.nativeEndpointDestroy(token) == EndpointAbi.OK, "destroy on the owning thread")
        println("RESULT ok")
    }

    /**
     * Every native answer, mapped through the real [JniEndpointBackend].
     *
     * The raw JNI facts are proven elsewhere; what this scenario proves is that
     * the backend puts each of them in the RIGHT DOMAIN -- a private registry
     * refusal never becomes a `MoqResultCode`, and an endpoint refusal never
     * becomes a bridge failure.
     */
    private fun backendMap() {
        System.load(BridgeDsos.path("moq5_jni"))
        val b = JniEndpointBackend.create(EndpointAbi.UNSERVABLE)

        // THE CALLER-THREAD CONSTRUCTION DISCRIMINATOR, first and on its own.
        // The root was created on THIS thread, so every stateful call from
        // another one must be refused by the registry's THREAD category with
        // that exact private code -- not by a generic lifecycle assertion, and
        // not after some other failure has already fired.
        val foreign = arrayOfNulls<Any>(4)
        val other = Thread {
            foreign[0] = b.waitForActivity(0L)
            foreign[1] = b.snapshot()
            foreign[2] = b.stop()
            foreign[3] = b.destroy()
        }
        other.start(); other.join()
        check(foreign[0] == BackendWaitResult.Bridge(BridgeFailure("wait", EndpointAbi.REG_THREAD)),
            "a caller-thread-constructed root must refuse a foreign wait with the exact " +
                "private THREAD code; got ${foreign[0]}")
        check(foreign[1] == BackendSnapshotResult.Bridge(
            BridgeFailure("snapshot", EndpointAbi.REG_THREAD)),
            "a foreign snapshot must carry the exact THREAD code; got ${foreign[1]}")
        check(foreign[2] == BackendStopResult.Bridge(BridgeFailure("stop", EndpointAbi.REG_THREAD)),
            "a foreign stop must be a BRIDGE refusal, never Refused(MoqResultCode); " +
                "got ${foreign[2]}")
        check(foreign[3] == BackendOpResult.Bridge(
            BridgeFailure("destroy", EndpointAbi.REG_THREAD)),
            "a foreign destroy must carry the exact THREAD code; got ${foreign[3]}")

        // WAKE is the any-thread exception, and stays ordinary on both threads.
        val anyThread = arrayOfNulls<Any>(1)
        val waker = Thread { anyThread[0] = b.wake() }
        waker.start(); waker.join()
        check(anyThread[0] == BackendOpResult.Ok, "wake is any-thread; got ${anyThread[0]}")
        check(b.wake() == BackendOpResult.Ok, "wake on the owning thread must be Ok")

        // WAIT maps the two reachable contract answers exactly. The scripted
        // facade answers one MOQ_OK per wake -- coalescing is the real C
        // tier's property and is proven against it elsewhere -- so both wakes
        // above are consumed here before an unwoken wait is asked for.
        val w1 = b.waitForActivity(0L)
        check(w1 == BackendWaitResult.ACTIVITY, "a woken wait must map MOQ_OK to ACTIVITY; got $w1")
        val w2 = b.waitForActivity(0L)
        check(w2 == BackendWaitResult.ACTIVITY, "the second wake must also map to ACTIVITY; got $w2")
        val w3 = b.waitForActivity(0L)
        check(w3 == BackendWaitResult.TIMEOUT,
            "an unwoken wait must map MOQ_DONE to TIMEOUT, not to CLOSED; got $w3")

        // DESTROY BEFORE STOP is refused by the registry's STATE category and
        // leaves the root LIVE and uncleaned -- destroy never stops on the
        // caller's behalf, so the two stay distinguishable through the mapping.
        check(b.destroy() == BackendOpResult.Bridge(BridgeFailure("destroy", EndpointAbi.REG_STATE)),
            "an unready root must be refused by the private STATE code")
        check(b.waitForActivity(0L) == BackendWaitResult.TIMEOUT,
            "a refused destroy must leave the root live and usable")

        // The canonical order, exactly once each.
        check(b.stop() == BackendStopResult.Stopped, "stop must map MOQ_OK to Stopped")
        check(b.destroy() == BackendOpResult.Ok, "a stopped root must destroy exactly once")

        // The token now addresses nothing. Every operation says so in its own
        // domain, and none of them touches a freed endpoint.
        // Each answer is taken ONCE into a local and both compared and reported
        // from it. `check` takes an eagerly built String, so a call inside a
        // message would run on every pass -- an extra native operation on a
        // retired token, and a reported value that is not the compared one.
        val staleWake = b.wake()
        check(staleWake == BackendOpResult.Bridge(BridgeFailure("wake", EndpointAbi.REG_STALE)),
            "a retired token must be refused as STALE; got $staleWake")
        val staleDestroy = b.destroy()
        check(staleDestroy == BackendOpResult.Bridge(
            BridgeFailure("destroy", EndpointAbi.REG_STALE)),
            "a second destroy must be refused as STALE, never repeated; got $staleDestroy")
        val staleStop = b.stop()
        check(staleStop == BackendStopResult.Bridge(BridgeFailure("stop", EndpointAbi.REG_STALE)),
            "a stop on a retired token is a BRIDGE refusal, not an endpoint refusal; " +
                "got $staleStop")
        println("RESULT ok")
    }

    /**
     * One wait answer, mapped.
     *
     * The odd case is the discriminating one: `MOQ5_ODD_WAIT_RESULT` is -777,
     * which is NOT in the private registry space, so a mapping that decided
     * "bridge failure" by range alone would pass it through as an endpoint
     * state. The wait contract is the four documented answers and nothing else,
     * so anything outside them is a bridge-contract failure whatever its value.
     */
    private fun backendWaitExact(expected: BackendWaitResult, what: String) {
        System.load(BridgeDsos.path("moq5_jni"))
        val b = JniEndpointBackend.create(EndpointAbi.UNSERVABLE)
        val got = b.waitForActivity(0L)
        check(got == expected, "$what must map exactly; expected $expected, got $got")
        // The token is still live and the ordinary lifecycle still holds.
        check(b.stop() == BackendStopResult.Stopped, "cleanup: stop")
        check(b.destroy() == BackendOpResult.Ok, "cleanup: destroy")
        println("RESULT ok")
    }

    /**
     * `MOQ_ERR_INTERRUPTED` through the real backend.
     *
     * It gets its own scenario rather than riding [backendWaitExact] because
     * the interrupt is a LATCH, not a one-shot answer, and the two facts that
     * distinguish a correct mapping from a lucky one are both about what the
     * latch does NOT do: it does not decay into a timeout, and it does not lose
     * to a raised activity level. `endpoint.h` states the precedence -- the
     * latch beats the level -- so a mapping that read ACTIVITY here would be
     * reporting work that is not there.
     *
     * The scripted mode arms the latch at construction and models NO terminal
     * state, so the root stays live throughout and the canonical teardown must
     * still run normally afterwards.
     */
    private fun backendInterrupt() {
        System.load(BridgeDsos.path("moq5_jni"))
        val b = JniEndpointBackend.create(EndpointAbi.UNSERVABLE)

        val first = b.waitForActivity(0L)
        check(first == BackendWaitResult.INTERRUPTED,
            "an interrupted wait must map MOQ_ERR_INTERRUPTED to INTERRUPTED, never to a " +
                "timeout or a terminal; got $first")

        // The latch is sticky: it is not consumed by being observed.
        val second = b.waitForActivity(0L)
        check(second == BackendWaitResult.INTERRUPTED,
            "the interrupt latch must survive being read; got $second")

        // And it OUTRANKS a raised activity level, which is the discriminator a
        // mapping that folded INTERRUPTED into ACTIVITY would fail.
        check(b.wake() == BackendOpResult.Ok, "wake must still be accepted while interrupted")
        val afterWake = b.waitForActivity(0L)
        check(afterWake == BackendWaitResult.INTERRUPTED,
            "the interrupt latch must beat a raised level; got $afterWake")

        // The root is LIVE, not terminal: the canonical teardown still runs.
        val stopped = b.stop()
        check(stopped == BackendStopResult.Stopped,
            "an interrupted root must still stop normally; got $stopped")
        val destroyed = b.destroy()
        check(destroyed == BackendOpResult.Ok,
            "an interrupted root must still destroy normally; got $destroyed")
        println("RESULT ok")
    }

    /**
     * One native snapshot call, all four fields mapped.
     *
     * Driven under the scripted future record, so the row proves the two facts
     * a narrowing bug would destroy: a nonzero version this build does not know
     * survives as itself, and the terminal detail keeps all 64 bits.
     */
    private fun backendSnapshot() {
        System.load(BridgeDsos.path("moq5_jni"))
        val b = JniEndpointBackend.create(EndpointAbi.UNSERVABLE)
        val r = b.snapshot()
        check(r is BackendSnapshotResult.Ok, "the snapshot must succeed; got $r")
        val snap = (r as BackendSnapshotResult.Ok).snapshot
        check(snap.state == EndpointState.Closed,
            "the state must map exactly; got ${snap.state}")
        check(snap.negotiatedVersion == MoqVersion(EndpointAbi.FUTURE_VERSION.toUInt()),
            "a future nonzero version must survive unclamped; got ${snap.negotiatedVersion}")
        val terminal = snap.terminal
        check(terminal is EndpointTerminal.Protocol,
            "the terminal reason must select the PROTOCOL domain; got $terminal")
        check((terminal as EndpointTerminal.Protocol).detailCode ==
            MoqErrorCode(EndpointAbi.SCRIPTED_DETAIL.toULong()),
            "the detail must keep all 64 bits; got ${terminal.detailCode}")
        check(b.stop() == BackendStopResult.Stopped, "cleanup: stop")
        check(b.destroy() == BackendOpResult.Ok, "cleanup: destroy")
        println("RESULT ok")
    }

    private fun discriminator() {
        System.load(BridgeDsos.path("moq5_jni"))
        val out = longArrayOf(0L)
        val rc = NativeBridge.nativeEndpointCreate("", out)
        check(rc != EndpointAbi.ERR_INVAL && rc != EndpointAbi.ERR_UNSUPPORTED,
            "the scripted target must not be able to produce the real discriminator (rc=$rc)")
        check(rc == EndpointAbi.OK, "the scripted target ignores the configuration (rc=$rc)")
        check(NativeBridge.nativeEndpointRelease(out[0]) == EndpointAbi.OK, "the root releases")
        println("RESULT ok")
    }

    /* -- the trusted-create row ---------------------------------------------
     *
     * These scenarios run with the REAL target and the host recorder armed
     * (TrustRouteProbe.ENV), or with the scripted target for the ownership
     * rows. The row itself is reached by reflection (TrustedRow) so the
     * scenario compiles while the row is absent; an absent row is a named
     * failure here, never a skipped oracle. */

    /** The recorder must have latched no instrumentation fault: it fails closed, so do we. */
    private fun noFault(where: String) {
        val f = TrustRouteProbe.fault()
        check(f == null, "$where: the recorder latched an instrumentation fault: $f")
    }

    /** Reserve / construct multiplicities across one action, from the recorder's causal counters. */
    private fun expectCounts(what: String, before: TrustRouteProbe.Counters, reserves: Long,
                             reserveFailures: Long, constructs: Long): TrustRouteProbe.Counters {
        val after = TrustRouteProbe.counters()
        check(after.reserveAttempts - before.reserveAttempts == reserves,
            "$what: expected $reserves reserve attempt(s), got ${after.reserveAttempts - before.reserveAttempts}")
        check(after.reserveFailures - before.reserveFailures == reserveFailures,
            "$what: expected $reserveFailures failed reserve attempt(s), got ${after.reserveFailures - before.reserveFailures}")
        check(after.constructAttempts - before.constructAttempts == constructs,
            "$what: expected $constructs constructor attempt(s), got ${after.constructAttempts - before.constructAttempts}")
        return after
    }

    /**
     * Policy 0 reaches `moq_endpoint_connect_with_trust()` with the library
     * delegate AND a context; every unknown or negative policy is refused
     * before the route with MOQ_ERR_INVAL (chats/libmoq/1369), with nothing
     * published; the ordinary row never touches the trusted route at all.
     *
     * The host image links no facade, so the real route answers UNSUPPORTED
     * for a well-formed request and INVAL for a malformed one: the recorder
     * FORWARDS, so those are the answers the trusted row must carry.
     */
    private fun trustedRoute() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val out = longArrayOf(EndpointAbi.SENTINEL)

        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, out) == EndpointAbi.ERR_UNSUPPORTED,
            "setup: the ordinary row refuses an unservable request with UNSUPPORTED")
        check(TrustRouteProbe.routes.isEmpty(),
            "the ordinary row must reach the ordinary constructor and never the trusted route; " +
                "the route saw ${TrustRouteProbe.routes}")
        check(out[0] == EndpointAbi.SENTINEL, "a refused ordinary creation must not touch the output")

        var counts = TrustRouteProbe.counters()
        val rc = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
        check(rc == EndpointAbi.ERR_UNSUPPORTED,
            "policy 0 must carry the real route's answer for a request this build cannot serve; got $rc")
        counts = expectCounts("policy 0", counts, reserves = 1, reserveFailures = 0, constructs = 1)
        check(TrustRouteProbe.routes.size == 1,
            "policy 0 must reach the trusted route exactly once; it was reached " +
                "${TrustRouteProbe.routes.size} time(s)")
        val r = TrustRouteProbe.routes[0]
        check(r.fnPresent, "policy 0 must hand the route a NON-NULL library delegate")
        check(r.ctxPresent, "policy 0 must hand the route a NON-NULL delegate context")
        check(!r.insecure && r.caFileLen == 0,
            "the trusted configuration must carry neither insecure_skip_verify nor a ca_file " +
                "(insecure=${r.insecure}, ca_file len=${r.caFileLen})")
        check(r.url == EndpointAbi.UNSERVABLE, "the URL must cross exactly; the route saw '${r.url}'")
        check(out[0] == EndpointAbi.SENTINEL, "a refused trusted creation must not touch the output")

        for (policy in intArrayOf(1, -1, Int.MAX_VALUE, Int.MIN_VALUE)) {
            val bad = TrustedRow.create(EndpointAbi.UNSERVABLE, policy, out)
            check(bad == EndpointAbi.ERR_INVAL,
                "trust policy $policy is unknown and must be refused with INVAL; got $bad")
            check(TrustRouteProbe.routes.size == 1,
                "trust policy $policy must be refused BEFORE the trusted route; the route saw it")
            check(out[0] == EndpointAbi.SENTINEL, "policy $policy must publish nothing")
            counts = expectCounts("policy $policy", counts, reserves = 0, reserveFailures = 0, constructs = 0)
        }

        // The URL is validated by the REAL route, not pre-judged by the bridge.
        val empty = TrustedRow.create("", TrustedRow.PLATFORM_TRUST_MANAGER, out)
        check(empty == EndpointAbi.ERR_INVAL,
            "an empty URL must be refused by the real route with INVAL; got $empty")
        check(TrustRouteProbe.routes.size == 2 && TrustRouteProbe.routes[1].url == "" &&
            TrustRouteProbe.routes[1].fnPresent,
            "the empty URL must have reached the route with the delegate, for the route to refuse")
        check(out[0] == EndpointAbi.SENTINEL, "a refused creation must publish nothing")

        // Output validation precedes construction exactly as on the ordinary row.
        counts = TrustRouteProbe.counters()
        check(TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, LongArray(0)) ==
            EndpointAbi.ERR_INVAL, "an undersized output array is refused")
        check(TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, null) ==
            EndpointAbi.ERR_INVAL, "a null output array is refused")
        check(TrustRouteProbe.routes.size == 2,
            "an unusable output must be refused before the route, not after it")
        expectCounts("invalid output", counts, reserves = 0, reserveFailures = 0, constructs = 0)
        noFault("trustedroute")
        println("RESULT ok")
    }

    /** The certificate material the callback scenarios share. */
    private class TrustMaterial {
        val host = "moq.example"
        val otherHost = "other.example"
        val ip = "192.0.2.10"
        private val cn = "CN=moq5-test-leaf"
        val leafRsa: ByteArray = TestCerts.selfSigned(cn, "RSA", "dns:$host")
        val leafEc: ByteArray = TestCerts.selfSigned(cn, "EC", "dns:$host")
        val leafIp: ByteArray = TestCerts.selfSigned(cn, "RSA", "ip:$ip")

        /** A dNSName whose TEXT is the IP literal: an IP identity must never accept it. */
        val leafDnsSpelledLikeIp: ByteArray = TestCerts.selfSigned(cn, "RSA", "dns:$ip")
        val intermediate: ByteArray =
            TestCerts.selfSigned("CN=moq5-test-intermediate", "RSA", "dns:intermediate.example")
        val malformed = byteArrayOf(0x30, 0x03, 0x02, 0x01, 0x01)
    }

    /**
     * The per-thread ATTACH DISCIPLINE, judged on causal counts the recorder
     * took at the forwarded JavaVM table -- not inferred from Java thread
     * identity or from the JVM's later view of the thread.
     *
     * For a thread the library attaches: attachment is discovered through
     * GetEnv on every call, AttachCurrentThreadAsDaemon is attempted exactly
     * once, no detach is attempted between calls, and exactly one detach is
     * attempted at thread exit. For a thread the library did NOT attach:
     * nothing is attached and nothing is detached, ever.
     *
     * And on every call, the LOCAL-REFERENCE bound: at most
     * [TrustRouteProbe.LOCAL_REF_BOUND] references live at once whatever the
     * chain length, and none left live when the callback returns.
     */
    private fun checkDiscipline(
        i: Int,
        c: TrustRouteProbe.Case,
        rs: List<TrustRouteProbe.Record>,
        ts: TrustRouteProbe.ThreadStats,
    ) {
        if (c.preAttached) {
            rs.forEachIndexed { k, r ->
                check(r.getEnv == k + 1,
                    "case $i call $k: exactly one GetEnv per callback; GetEnv count ${r.getEnv} after call $k")
                check(r.attachDaemon == 0 && r.attach == 0,
                    "case $i call ${r.call}: the library must not attach a thread that is already " +
                        "attached (daemon=${r.attachDaemon}, plain=${r.attach})")
                check(r.detach == 0,
                    "case $i call ${r.call}: the library must not detach a thread it did not attach")
            }
            check(ts.attachDaemon == 0 && ts.attach == 0 && ts.detach == 0,
                "case $i: after exit, no attach or detach may have been attempted on a thread the " +
                    "library did not attach (daemon=${ts.attachDaemon}, plain=${ts.attach}, detach=${ts.detach})")
        } else {
            rs.forEachIndexed { k, r ->
                check(r.getEnv == k + 1,
                    "case $i call $k: attachment must be discovered through exactly one GetEnv per " +
                        "callback; GetEnv count ${r.getEnv} after call $k")
                check(r.attachDaemon == 1,
                    "case $i call $k: AttachCurrentThreadAsDaemon must be attempted exactly once per " +
                        "thread; count ${r.attachDaemon} after call $k")
                check(r.attach == 0,
                    "case $i call $k: AttachCurrentThread (non-daemon) must not be used; count ${r.attach}")
                check(r.detach == 0,
                    "case $i call $k: no detach may be attempted between calls; count ${r.detach}")
            }
            check(ts.attachDaemon == 1,
                "case $i: exactly one attach over the thread's life; final count ${ts.attachDaemon}")
            check(ts.detach == 1,
                "case $i: exactly one detach must be attempted at thread exit, for a thread this " +
                    "library attached; final detach count ${ts.detach}")
        }
        rs.forEach { r ->
            check(r.highWater <= TrustRouteProbe.LOCAL_REF_BOUND,
                "case $i call ${r.call}: at most ${TrustRouteProbe.LOCAL_REF_BOUND} local references " +
                    "may be live at once; high-water ${r.highWater} for a ${c.chain.size}-element chain")
            check(r.live == 0,
                "case $i call ${r.call}: every local reference must be released before the callback " +
                    "returns; ${r.live} still live")
        }
    }

    /**
     * THE PRODUCT CALLBACK BOUNDARY. The recorder invokes the delegate policy 0
     * handed the route, on a thread the JVM has never seen, with each scripted
     * case; the shipped delegate must decide natively first and consult
     * `Moq5PlatformTrust.checkServerTrusted` only for a chain whose leaf
     * carries the reference identity -- with the exact leaf-first DER chain,
     * the exact server name, and the protocol-derived GENERIC auth type --
     * under the attach and local-reference discipline above.
     */
    private fun trustedCallback() {
        System.load(BridgeDsos.path("moq5_jni"))
        val m = TrustMaterial()
        val accept = TrustRouteProbe.Answer.Accept

        val cases = listOf(
            /* 0 */ TrustRouteProbe.Case(arrayOf(m.leafRsa), m.host, List(4) { accept }),
            /* 1 */ TrustRouteProbe.Case(arrayOf(m.leafEc), m.host, listOf(accept)),
            /* 2 */ TrustRouteProbe.Case(arrayOf(m.leafRsa), m.otherHost, listOf(accept)),
            /* 3 */ TrustRouteProbe.Case(arrayOf(m.leafIp), m.ip, listOf(accept)),
            /* 4 */ TrustRouteProbe.Case(arrayOf(m.leafDnsSpelledLikeIp), m.ip, listOf(accept)),
            /* 5 */ TrustRouteProbe.Case(arrayOf(m.leafRsa, m.intermediate), m.host, listOf(accept)),
            /* 6 */ TrustRouteProbe.Case(
                arrayOf(m.leafRsa), m.host,
                listOf(TrustRouteProbe.Answer.ThrowCertificate, TrustRouteProbe.Answer.ThrowOther, accept),
            ),
            /* 7 */ TrustRouteProbe.Case(emptyArray(), m.host, listOf(accept)),
            /* 8 */ TrustRouteProbe.Case(arrayOf(m.malformed), m.host, listOf(accept)),
            /* 9 */ TrustRouteProbe.Case(arrayOf(m.leafRsa), null, listOf(accept)),
            // The provider-reachable maximum: exactly the adapter's cap
            // (MOQ_PICOQUIC_CERT_CHAIN_MAX = 16). Judged BEFORE the 64-element
            // arm so a chain-proportional local-reference defect is named here.
            /* 10 */ TrustRouteProbe.Case(Array(16) { m.leafRsa }, m.host, listOf(accept)),
            // A 64-element chain BYPASSES that cap: it is not a wire-reachable
            // chain through that provider today and stays as defence in depth.
            /* 11 */ TrustRouteProbe.Case(Array(64) { m.leafRsa }, m.host, listOf(accept)),
            // A thread the library did NOT attach.
            /* 12 */ TrustRouteProbe.Case(arrayOf(m.leafRsa), m.host, listOf(accept, accept), preAttached = true),
        )
        val consultedCases = listOf(0, 1, 3, 5, 6, 10, 11, 12)
        val nativelyRefused = listOf(2, 4, 7, 8, 9)
        TrustRouteProbe.arm(cases)

        val out = longArrayOf(EndpointAbi.SENTINEL)
        val rc = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
        check(rc == EndpointAbi.ERR_UNSUPPORTED, "the forwarded route answer must be carried; got $rc")
        check(out[0] == EndpointAbi.SENTINEL, "nothing may be published")
        check(TrustRouteProbe.routes.size == 1 && TrustRouteProbe.routes[0].fnPresent &&
            TrustRouteProbe.routes[0].ctxPresent,
            "the route must have received the library delegate and its context; saw ${TrustRouteProbe.routes}")
        check(TrustRouteProbe.doneCases == cases.indices.toList(),
            "every case must be driven to completion by the recorder; done=${TrustRouteProbe.doneCases}")

        val main = Thread.currentThread()
        fun recordsOf(i: Int) = TrustRouteProbe.records.filter { it.case == i }.sortedBy { it.call }
        fun consultedOf(i: Int) = TrustRouteProbe.consulted.filter { it.case == i }
        fun statsOf(i: Int) = TrustRouteProbe.threads.single { it.case == i }
        // The consultations every case BEFORE [i] must have made, from the
        // script itself -- the causal prefix each call is held to.
        fun consultedBefore(i: Int): Long =
            cases.indices.filter { it < i && it in consultedCases }.sumOf { cases[it].repeats }.toLong()

        for (i in consultedCases) {
            val c = cases[i]
            val rs = recordsOf(i)
            val cs = consultedOf(i)
            check(rs.size == c.repeats, "case $i: expected ${c.repeats} verdict record(s), got ${rs.size}")
            check(cs.size == c.repeats,
                "case $i: the platform bridge must be consulted exactly ${c.repeats} time(s); got ${cs.size}")
            rs.forEachIndexed { k, r ->
                val expect = consultedBefore(i) + k + 1
                check(r.consultedPrefix == expect,
                    "case $i call $k: exactly one consultation must precede this call's verdict -- " +
                        "expected prefix $expect, the recorder read ${r.consultedPrefix} right after the call")
                val prevCreated = if (k == 0) 0 else rs[k - 1].created
                check(r.created - prevCreated >= c.chain.size + 1,
                    "case $i call $k: the local-reference counter must have been active -- at least " +
                        "${c.chain.size + 1} references created for this chain, counted ${r.created - prevCreated}")
            }
            cs.forEachIndexed { k, s ->
                check(s.authType == TrustRouteProbe.AUTH_TYPE,
                    "case $i call $k: authType must be exactly '${TrustRouteProbe.AUTH_TYPE}' " +
                        "(the TLS 1.3 key-exchange constant), got '${s.authType}'")
                check(s.host == c.serverName, "case $i call $k: host must be '${c.serverName}', got '${s.host}'")
                check(s.chain.size == c.chain.size,
                    "case $i call $k: the whole chain must cross; ${s.chain.size} of ${c.chain.size} arrived")
                for (e in c.chain.indices) {
                    check(s.chain[e].contentEquals(c.chain[e]),
                        "case $i call $k: element $e must cross leaf-first and byte-exact")
                }
                check(s.thread !== main,
                    "case $i call $k: the platform was consulted on the calling thread, not the transport's")
            }
            rs.forEachIndexed { k, r ->
                val expect = if (c.answers[k] == accept) TrustRouteProbe.ACCEPT else TrustRouteProbe.REJECT
                check(r.verdict == expect,
                    "case $i call $k: answer ${c.answers[k]} must yield verdict $expect; got ${r.verdict}")
                check(r.attachedAfter,
                    "case $i call $k: the thread must still be attached after the callback")
                check(!r.pendingAfter,
                    "case $i call $k: a Java exception was left pending after the callback")
            }
            checkDiscipline(i, c, rs, statsOf(i))
        }
        for (i in nativelyRefused) {
            val rs = recordsOf(i)
            val ts = statsOf(i)
            check(consultedOf(i).isEmpty(),
                "case $i must be refused NATIVELY, before the platform bridge is consulted; " +
                    "the bridge saw ${consultedOf(i).size} call(s)")
            check(rs.size == 1 && rs[0].verdict == TrustRouteProbe.REJECT,
                "case $i must yield REJECT from the delegate; got ${rs.map { it.verdict }}")
            check(!rs[0].pendingAfter, "case $i: nothing may be left pending")
            check(rs[0].consultedPrefix == consultedBefore(i),
                "case $i: a native refusal must add no consultation; prefix ${rs[0].consultedPrefix}, " +
                    "expected ${consultedBefore(i)}")
            // Refused BEFORE any JVM work: no environment discovery, no attach,
            // no detach, on the call or at thread exit.
            check(rs[0].getEnv == 0 && rs[0].attachDaemon == 0 && rs[0].attach == 0 && rs[0].detach == 0,
                "case $i: a native refusal must make no JNI attempt at all (GetEnv=${rs[0].getEnv}, " +
                    "daemon=${rs[0].attachDaemon}, plain=${rs[0].attach}, detach=${rs[0].detach})")
            check(ts.getEnv == 0 && ts.attachDaemon == 0 && ts.attach == 0 && ts.detach == 0,
                "case $i: a natively refused thread must end with no JNI attempt on record")
            rs.forEach { r ->
                check(r.live == 0, "case $i: a native refusal must leave no local reference live (${r.live})")
            }
        }
        noFault("trustedcallback")
        println("RESULT ok")
    }

    /**
     * CLEANUP REGISTRATION FAILS after the attach. The forwarded
     * `pthread_setspecific` is armed to fail, so the callback cannot register
     * its exit cleanup: it must fail closed -- consult nothing, answer REJECT
     * -- and IMMEDIATELY return the attachment it acquired, so no attached
     * foreign thread is ever left without a cleanup path. Each later call
     * repeats that exactly, and thread exit adds no detach of its own.
     */
    private fun trustedFault() {
        System.load(BridgeDsos.path("moq5_jni"))
        val m = TrustMaterial()
        val accept = TrustRouteProbe.Answer.Accept
        val cases = listOf(TrustRouteProbe.Case(arrayOf(m.leafRsa), m.host, listOf(accept, accept)))
        TrustRouteProbe.arm(cases)

        val out = longArrayOf(EndpointAbi.SENTINEL)
        val rc = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
        check(rc == EndpointAbi.ERR_UNSUPPORTED, "the forwarded route answer must be carried; got $rc")
        check(TrustRouteProbe.doneCases == listOf(0), "the case must complete; done=${TrustRouteProbe.doneCases}")
        val rs = TrustRouteProbe.records.filter { it.case == 0 }.sortedBy { it.call }
        check(rs.size == 2, "two calls must be recorded; got ${rs.size}")
        rs.forEachIndexed { k, r ->
            check(r.verdict == TrustRouteProbe.REJECT,
                "call $k: a callback whose cleanup registration failed must answer REJECT; got ${r.verdict}")
            check(r.getEnv == k + 1, "call $k: exactly one GetEnv per callback; count ${r.getEnv}")
            check(r.attachDaemon == k + 1,
                "call $k: the attach must be attempted again on each call once the previous one was " +
                    "returned; attach count ${r.attachDaemon}")
            check(r.detach == k + 1,
                "call $k: the attachment must be returned IMMEDIATELY when cleanup cannot be " +
                    "registered; detach count ${r.detach}")
            check(!r.attachedAfter, "call $k: the thread must not remain attached")
            check(!r.pendingAfter, "call $k: nothing may be left pending")
        }
        check(TrustRouteProbe.consulted.isEmpty(),
            "a callback that cannot register its cleanup must not consult the platform; " +
                "${TrustRouteProbe.consulted.size} consultation(s)")
        val ts = TrustRouteProbe.threads.single { it.case == 0 }
        check(ts.detach == 2,
            "thread exit must add no detach for a thread whose cleanup was never registered; " +
                "final detach count ${ts.detach}")
        noFault("trustedfault")
        println("RESULT ok")
    }

    /**
     * A JAVA EXCEPTION CROSSES JNI. The process carries a test-only class with
     * the exact `org/openmoq/moq5/trust/Moq5PlatformTrust` name, INSTANCE
     * field and method descriptor, whose method THROWS. `JNI_OnLoad` cached
     * that class and method; the product callback must clear the pending
     * throwable, translate it to REJECT, and leave nothing pending. The shadow
     * counts its own invocations, which is the proof that the cached target
     * is the shadow's method.
     */
    private fun trustedShadow() {
        System.load(BridgeDsos.path("moq5_jni"))
        val m = TrustMaterial()
        val accept = TrustRouteProbe.Answer.Accept
        val cases = listOf(TrustRouteProbe.Case(arrayOf(m.leafRsa), m.host, listOf(accept, accept)))
        TrustRouteProbe.armCases(cases)

        val out = longArrayOf(EndpointAbi.SENTINEL)
        val rc = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
        check(rc == EndpointAbi.ERR_UNSUPPORTED, "the forwarded route answer must be carried; got $rc")
        val shadow = Class.forName("org.openmoq.moq5.trust.Moq5PlatformTrust")
        val calls = shadow.getField("CALLS").getInt(null)
        val lastAuth = shadow.getField("LAST_AUTH").get(null) as String?
        println("SHADOW-CALLS $calls auth=$lastAuth")
        check(calls == 2,
            "the cached checkServerTrusted must be the shadow's: it must have been invoked exactly " +
                "twice, was invoked $calls time(s)")
        check(lastAuth == TrustRouteProbe.AUTH_TYPE, "the shadow must have received GENERIC; got '$lastAuth'")
        val rs = TrustRouteProbe.records.filter { it.case == 0 }.sortedBy { it.call }
        check(rs.size == 2, "two calls must be recorded; got ${rs.size}")
        rs.forEachIndexed { k, r ->
            check(r.verdict == TrustRouteProbe.REJECT,
                "call $k: a throwable crossing JNI must be translated to REJECT; got ${r.verdict}")
            check(!r.pendingAfter,
                "call $k: the throwable must be cleared; a Java exception was left pending")
            check(r.attachedAfter, "call $k: the thread must remain attached")
            check(r.live == 0, "call $k: every local reference must be released (${r.live} live)")
        }
        noFault("trustedshadow")
        println("RESULT ok")
    }

    /**
     * OWNERSHIP through the trusted row, on the scripted target: a refused
     * policy consumes no slot or generation and publishes nothing; policy 0
     * constructs and publishes one ROOT token; that root is an ordinary root to
     * every other operation and retires exactly once.
     */
    private fun trustedScripted() {
        System.load(BridgeDsos.path("moq5_jni"))
        val a = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, a) == EndpointAbi.OK,
            "setup: an ordinary root constructs")
        val slotA = slotOf(a[0])
        check(NativeBridge.nativeEndpointRelease(a[0]) == EndpointAbi.OK, "setup: and releases")

        val out = longArrayOf(EndpointAbi.SENTINEL)
        var counts = TrustRouteProbe.counters()
        for (policy in intArrayOf(1, -1)) {
            check(TrustedRow.create(EndpointAbi.UNSERVABLE, policy, out) == EndpointAbi.ERR_INVAL,
                "policy $policy must be refused with INVAL")
            check(out[0] == EndpointAbi.SENTINEL, "policy $policy must publish nothing")
            counts = expectCounts("policy $policy", counts, reserves = 0, reserveFailures = 0, constructs = 0)
        }

        val t = longArrayOf(0L)
        val rc = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, t)
        check(rc == EndpointAbi.OK, "policy 0 must construct through the scripted target (rc=$rc)")
        counts = expectCounts("trusted success", counts, reserves = 1, reserveFailures = 0, constructs = 1)
        check(t[0] != 0L, "a successful trusted construction publishes a token")
        check(EndpointAbi.kindOf(t[0]) == EndpointAbi.KIND_ROOT, "the token must be a ROOT")
        check(slotOf(t[0]) == slotA,
            "the refused policies consumed nothing: the trusted root must reuse the released slot " +
                "$slotA, got ${slotOf(t[0])}")
        check(t[0] != a[0], "the reused slot must carry a new generation")

        val u = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, u) == EndpointAbi.OK,
            "an ordinary root still constructs beside a trusted one")
        check(u[0] != t[0], "distinct roots carry distinct tokens")

        // One lifecycle: the trusted root is an ordinary root to every other row.
        check(NativeBridge.nativeEndpointWait(t[0], 0L) == EndpointAbi.DONE, "wait on the trusted root")
        check(NativeBridge.nativeEndpointDestroy(t[0]) == EndpointAbi.REG_STATE,
            "an unready trusted root is refused by registry STATE")
        check(NativeBridge.nativeEndpointStop(t[0]) == EndpointAbi.OK, "stop")
        check(NativeBridge.nativeEndpointDestroy(t[0]) == EndpointAbi.OK, "destroy exactly once")
        check(NativeBridge.nativeEndpointDestroy(t[0]) == EndpointAbi.REG_STALE,
            "a retired trusted token must not be destroyed twice")
        check(NativeBridge.nativeEndpointRelease(u[0]) == EndpointAbi.OK, "the ordinary root releases")

        // The ledger is clean: nothing leaked, nothing destroyed twice or early.
        val z = longArrayOf(0L)
        val again = NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, z)
        check(again == EndpointAbi.OK, "the lifecycle ledger must be clean afterwards (rc=$again)")
        check(NativeBridge.nativeEndpointRelease(z[0]) == EndpointAbi.OK, "cleanup")
        noFault("trustedscripted")
        println("RESULT ok")
    }

    /**
     * Publication fails AFTER a trusted root has been constructed and
     * committed: the same ordered teardown as the ordinary row, exactly once,
     * with no token reaching the caller and the slot back on the free list.
     */
    private fun trustedPublishFail() {
        System.load(BridgeDsos.path("moq5_jni"))
        val out = longArrayOf(EndpointAbi.SENTINEL)
        var threw = false
        try {
            TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
        } catch (_: ArrayIndexOutOfBoundsException) {
            threw = true
        }
        check(threw, "the injected publication failure must surface as its real exception")
        check(out[0] == EndpointAbi.SENTINEL, "no token may reach the caller")

        val first = longArrayOf(0L)
        val rc = NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, first)
        check(rc != EndpointAbi.LC_LEAKED, "the failed trusted publication leaked the endpoint")
        check(rc != EndpointAbi.LC_DESTROY_BEFORE_STOP,
            "the failed trusted publication destroyed the endpoint before stopping it")
        check(rc != EndpointAbi.LC_DOUBLE_DESTROY, "the failed trusted publication destroyed the endpoint twice")
        check(rc == EndpointAbi.OK, "construction must still work afterwards (rc=$rc)")
        check(slotOf(first[0]) == 0L,
            "the retired trusted root must leave slot 0 free; got slot ${slotOf(first[0])}")
        check(NativeBridge.nativeEndpointRelease(first[0]) == EndpointAbi.OK, "cleanup")
        println("RESULT ok")
    }

    /**
     * A FULL REGISTRY refuses the trusted row BEFORE its constructor: the
     * refusal is the registry's own, the output is untouched, and nothing is
     * consumed -- the freed slot admits exactly one trusted root with a new
     * generation.
     */
    private fun trustedCapacity() {
        System.load(BridgeDsos.path("moq5_jni"))
        val tokens = ArrayList<Long>(MAX_ENTRIES)
        val fill = longArrayOf(0L)
        while (tokens.size < MAX_ENTRIES) {
            val rc = NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, fill)
            if (rc != EndpointAbi.OK) {
                check(false, "setup: filling stopped early at ${tokens.size} with rc=$rc")
                return
            }
            tokens.add(fill[0])
        }
        val out = longArrayOf(EndpointAbi.SENTINEL)
        var counts = TrustRouteProbe.counters()
        val refused = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
        check(refused == EndpointAbi.REG_CAPACITY,
            "a full registry must refuse the trusted row with the exact CAPACITY code " +
                "${EndpointAbi.REG_CAPACITY}; got $refused")
        check(out[0] == EndpointAbi.SENTINEL, "a refused trusted construction must publish nothing")
        // ONE failed reservation and NO constructor attempt: the refusal is the
        // registry's, before construction -- not a construct-then-teardown.
        counts = expectCounts("capacity refusal", counts, reserves = 1, reserveFailures = 1, constructs = 0)

        val freed = tokens.removeAt(tokens.size - 1)
        check(NativeBridge.nativeEndpointRelease(freed) == EndpointAbi.OK, "the root releases")
        val t = longArrayOf(EndpointAbi.SENTINEL)
        check(TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, t) == EndpointAbi.OK,
            "one released slot must admit exactly one new trusted root")
        counts = expectCounts("trusted success", counts, reserves = 1, reserveFailures = 0, constructs = 1)
        check(slotOf(t[0]) == slotOf(freed),
            "the trusted root must reuse the released slot: nothing was consumed by the refusal")
        check(t[0] != freed, "the reused slot must carry a new generation")
        tokens.add(t[0])
        for (tok in tokens) {
            check(NativeBridge.nativeEndpointRelease(tok) == EndpointAbi.OK, "every root releases")
        }
        val again = longArrayOf(0L)
        check(TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, again) == EndpointAbi.OK,
            "the registry must be reusable after a capacity refusal")
        check(NativeBridge.nativeEndpointRelease(again[0]) == EndpointAbi.OK, "and release")
        noFault("trustedcapacity")
        println("RESULT ok")
    }

    /**
     * A CONSTRUCTOR REFUSAL through the trusted row aborts the unused
     * reservation. With the real target every construction is refused, so
     * the observable is EXHAUSTION: one more refusal than the registry has
     * slots must still be the constructor's own answer -- a leaked
     * reservation per refusal would turn the last one into a registry code.
     */
    private fun trustedRefusals() {
        System.load(BridgeDsos.path("moq5_jni"))
        val out = longArrayOf(EndpointAbi.SENTINEL)
        val n = MAX_ENTRIES + 1
        for (k in 0 until n) {
            val rc = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
            check(rc == EndpointAbi.ERR_UNSUPPORTED,
                "refusal $k of $n must be the constructor's UNSUPPORTED, not a registry answer " +
                    "(a leaked reservation would exhaust the registry); got $rc")
            check(out[0] == EndpointAbi.SENTINEL, "refusal $k must publish nothing")
        }
        for (k in 0 until n) {
            val rc = TrustedRow.create("", TrustedRow.PLATFORM_TRUST_MANAGER, out)
            check(rc == EndpointAbi.ERR_INVAL, "malformed refusal $k must stay INVAL; got $rc")
        }
        for (k in 0 until n) {
            val rc = TrustedRow.create(EndpointAbi.UNSERVABLE, 7, out)
            check(rc == EndpointAbi.ERR_INVAL, "unknown-policy refusal $k must stay INVAL; got $rc")
        }
        check(out[0] == EndpointAbi.SENTINEL, "no refusal may touch the output")
        val c = TrustRouteProbe.counters()
        check(c.reserveFailures == 0L,
            "no reservation may have failed across ${3 * n} refusals; ${c.reserveFailures} did")
        check(c.reserveAttempts == n.toLong() * 2 && c.constructAttempts == n.toLong() * 2,
            "each of the $n well-formed and $n malformed refusals reserves once and constructs once; " +
                "reserves=${c.reserveAttempts}, constructs=${c.constructAttempts}")
        noFault("trustedrefusals")
        println("RESULT ok")
    }

    /**
     * A ONE-SHOT SCRIPTED CONSTRUCTOR ERROR, then success in the same child.
     *
     * The expected next token is established independently first: an
     * ordinary root is created and released twice, which shows the registry's
     * own rule (same slot, generation advanced by one on retirement). The
     * aborted trusted reservation must then leave that exact slot AND
     * generation for the next construction -- an abort that advanced the
     * generation without leaking the slot would pass every reuse-only oracle.
     */
    private fun trustedCtorError() {
        System.load(BridgeDsos.path("moq5_jni"))
        val a = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, a) == EndpointAbi.OK, "setup: root A")
        check(NativeBridge.nativeEndpointRelease(a[0]) == EndpointAbi.OK, "setup: A releases")
        val b = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, b) == EndpointAbi.OK, "setup: root B")
        check(EndpointAbi.slotOf(b[0]) == EndpointAbi.slotOf(a[0]) &&
            EndpointAbi.generationOf(b[0]) == EndpointAbi.generationOf(a[0]) + 1,
            "setup: the registry's own rule must hold -- B reuses A's slot with the generation " +
                "advanced by one (A=${java.lang.Long.toHexString(a[0])}, B=${java.lang.Long.toHexString(b[0])})")
        check(NativeBridge.nativeEndpointRelease(b[0]) == EndpointAbi.OK, "setup: B releases")
        val expected = EndpointAbi.tokenOf(EndpointAbi.KIND_ROOT, EndpointAbi.slotOf(b[0]),
            EndpointAbi.generationOf(b[0]) + 1)

        val out = longArrayOf(EndpointAbi.SENTINEL)
        var counts = TrustRouteProbe.counters()
        TrustRouteProbe.nativeArmCtorFault(TrustRouteProbe.CTOR_FAULT_ERROR_ONCE)
        val rc = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
        check(rc == EndpointAbi.ERR_NOMEM,
            "the scripted constructor's one error is NOMEM and must be carried exactly; got $rc")
        check(out[0] == EndpointAbi.SENTINEL, "a constructor error must publish nothing")
        counts = expectCounts("constructor error", counts, reserves = 1, reserveFailures = 0, constructs = 1)

        val t = longArrayOf(EndpointAbi.SENTINEL)
        val ok = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, t)
        check(ok == EndpointAbi.OK, "the next trusted construction must succeed; got $ok")
        check(t[0] == expected,
            "the aborted reservation must have restored the EXACT slot and generation: expected " +
                "${java.lang.Long.toHexString(expected)}, got ${java.lang.Long.toHexString(t[0])}")
        expectCounts("recovery", counts, reserves = 1, reserveFailures = 0, constructs = 1)
        check(NativeBridge.nativeEndpointRelease(t[0]) == EndpointAbi.OK, "cleanup")
        noFault("trustedctorerror")
        println("RESULT ok")
    }

    /**
     * SUCCESS WITH NO ENDPOINT, one shot, from the real trusted route. The
     * transaction must report it rather than publish, and the unused
     * reservation must be restored exactly: the next reservation is handed the
     * same slot and generation.
     */
    private fun trustedCtorNull() {
        System.load(BridgeDsos.path("moq5_jni"))
        val out = longArrayOf(EndpointAbi.SENTINEL)
        var counts = TrustRouteProbe.counters()
        TrustRouteProbe.nativeArmCtorFault(TrustRouteProbe.CTOR_FAULT_NULL_ONCE)
        val rc = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
        check(rc == EndpointAbi.ERR_INVAL,
            "a constructor reporting success without an endpoint is a contract violation, reported " +
                "as INVAL; got $rc")
        check(out[0] == EndpointAbi.SENTINEL, "nothing may be published")
        counts = expectCounts("success-with-NULL", counts, reserves = 1, reserveFailures = 0, constructs = 1)
        val h1 = counts.lastReserveHandle
        check(h1 != 0L, "setup: the reservation handle must be on record")

        val again = TrustedRow.create(EndpointAbi.UNSERVABLE, TrustedRow.PLATFORM_TRUST_MANAGER, out)
        check(again == EndpointAbi.ERR_UNSUPPORTED, "the next construction is the real route's refusal; got $again")
        counts = expectCounts("next attempt", counts, reserves = 1, reserveFailures = 0, constructs = 1)
        check(counts.lastReserveHandle == h1,
            "the aborted reservation must be restored EXACTLY: the next reservation was handed " +
                "${java.lang.Long.toHexString(counts.lastReserveHandle)}, the aborted one was " +
                "${java.lang.Long.toHexString(h1)}")
        check(out[0] == EndpointAbi.SENTINEL, "nothing may be published")
        noFault("trustedctornull")
        println("RESULT ok")
    }

    /* -- the PUBLIC endpoint surface, at the real JNI boundary ---------------- */

    /** Opens through the public API with [config] and returns the ONE route the recorder saw. */
    private fun openAndRoute(config: EndpointConfig): TrustRouteProbe.Route {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        PlatformTrustSupport.sdkInt = { 35 }
        var failure: EndpointCreateFailedException? = null
        try {
            kotlinx.coroutines.runBlocking { MoqEndpoint.open(config) }
        } catch (e: EndpointCreateFailedException) {
            failure = e
        }
        check(failure != null && failure.nativeCode == EndpointAbi.ERR_UNSUPPORTED,
            "this build cannot serve the request: the typed creation failure with the exact code; got $failure")
        check(TrustRouteProbe.routes.size == 1, "exactly one route; saw ${TrustRouteProbe.routes}")
        return TrustRouteProbe.routes[0]
    }

    private fun publicOpenDefaults() {
        val r = openAndRoute(EndpointConfig(EndpointAbi.UNSERVABLE))
        check(r.versionPolicy == 0 && r.versions.isEmpty(), "AUTO offers no list; saw $r")
        check(r.tailCovered, "the create row must use the sized init so the tail is read; saw $r")
        check(r.handshakeMicros == 0L, "no bound means the backend default (0); saw $r")
        noFault("publicopendefaults")
        println("RESULT ok")
    }

    private fun publicOpenConfigured() {
        val r = openAndRoute(
            EndpointConfig(
                EndpointAbi.UNSERVABLE,
                versions = VersionOffer.Offer(listOf(MoqVersion.Draft18, MoqVersion.Draft16)),
                handshakeTimeout = 2500.milliseconds,
            ),
        )
        check(r.versionPolicy == 1 && r.versions == listOf(18, 16), "POLICY_LIST with the codes in order; saw $r")
        check(r.tailCovered && r.handshakeMicros == 2_500_000L, "the bound crosses as whole microseconds; saw $r")
        noFault("publicopenconfigured")
        println("RESULT ok")
    }

    private fun publicOpenExact() {
        val r = openAndRoute(EndpointConfig(EndpointAbi.UNSERVABLE, versions = VersionOffer.Exactly(MoqVersion.Draft18)))
        check(r.versionPolicy == 2 && r.versions == listOf(18), "POLICY_EXACT with one code; saw $r")
        noFault("publicopenexact")
        println("RESULT ok")
    }

    private fun publicOpenTransportDefaults() {
        val r = openAndRoute(EndpointConfig(EndpointAbi.UNSERVABLE))
        check(r.protocol == 0 && r.webTransportPath == null, "AUTO and no path override; saw $r")
        noFault("publicopentransportdefaults")
        println("RESULT ok")
    }

    private fun publicOpenTransport() {
        val r = openAndRoute(EndpointConfig(EndpointAbi.UNSERVABLE_WT, protocol = TransportProtocol.WebTransport, webTransportPath = "/relay"))
        check(r.url == EndpointAbi.UNSERVABLE_WT, "the https URL crosses exactly; saw $r")
        check(r.protocol == 2 && r.webTransportPath == "/relay", "WEBTRANSPORT with the exact path; saw $r")
        noFault("publicopentransport")
        println("RESULT ok")
    }

    private fun publicOpenTransportOverride() {
        val r = openAndRoute(EndpointConfig(EndpointAbi.UNSERVABLE_WT, protocol = TransportProtocol.RawQuic))
        check(r.protocol == 1 && r.webTransportPath == null, "RAW_QUIC overrides the https scheme at the field; saw $r")
        noFault("publicopentransportoverride")
        println("RESULT ok")
    }

    /** A path is carried whatever the protocol says: the bridge never second-guesses it. */
    private fun publicOpenTransportRawPath() {
        val r = openAndRoute(EndpointConfig(EndpointAbi.UNSERVABLE, protocol = TransportProtocol.RawQuic, webTransportPath = "/relay"))
        check(r.protocol == 1 && r.webTransportPath == "/relay", "RAW_QUIC with the path still present in cfg->wt_path; saw $r")
        noFault("publicopentransportrawpath")
        println("RESULT ok")
    }

    /** Malformed transport selection fails closed before reservation and route, publishing nothing. */
    private fun createBadTransport() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val before = TrustRouteProbe.counters()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        fun call(protocol: Int, path: String?, what: String) {
            out[0] = EndpointAbi.SENTINEL
            val rc = NativeBridge.nativeEndpointCreateTrusted(EndpointAbi.UNSERVABLE, 0, 0, IntArray(0), 0L, protocol, path, out)
            check(rc == EndpointAbi.INVAL, "$what must be MOQ_ERR_INVAL; got $rc")
            check(out[0] == EndpointAbi.SENTINEL, "$what: a failed create publishes nothing; out[0] was rewritten to ${out[0]}")
        }
        call(3, null, "an unknown protocol ordinal")
        call(-1, null, "a negative protocol ordinal")
        call(0, "", "an empty path string (null is the only spelling of no override)")
        call(2, "relay", "a relative path (no leading slash)")
        call(2, "moq/", "a path missing its leading slash")
        call(2, "/re\u0000lay", "a NUL-bearing path")
        call(2, "/a%2", "a truncated percent escape")
        call(2, "/a%zz", "a non-hex percent escape")
        call(2, "/caf\u00e9", "a non-ASCII path")
        call(2, "/a b", "a path with whitespace")
        call(2, "/a\u0001b", "a path with a control character")
        call(2, "/a#b", "a path with a fragment")
        call(2, "/a\\b", "a backslash, printable but outside pchar")
        call(2, "/a<b", "'<' outside pchar")
        call(2, "/a>b", "'>' outside pchar")
        call(2, "/a[b]", "brackets outside pchar")
        call(2, "/a^b", "'^' outside pchar")
        call(2, "/a|b", "'|' outside pchar")
        call(2, "/a`b", "a backtick outside pchar")
        call(2, "/a{b}", "braces outside pchar")
        call(2, "/ok?x=[", "'[' inside the query")
        check(TrustRouteProbe.counters().reserveAttempts == before.reserveAttempts, "refused before any reservation")
        check(TrustRouteProbe.routes.isEmpty(), "and before any route")
        noFault("createbadtransport")
        println("RESULT ok")
    }

    // -- the receiver rows, against a LIVE scripted root ---------------------

    private fun liveRoot(): Long {
        val t = longArrayOf(0L)
        check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK, "setup: construct a live root")
        check(NativeBridge.nativeEndpointWait(t[0], 0L) == EndpointAbi.DONE, "setup: the root is live (the facade is not terminal)")
        return t[0]
    }

    private val ns: Array<ByteArray> = arrayOf("live".toByteArray(), "cam1".toByteArray())

    private fun attach(root: Long, out: LongArray, parts: Array<ByteArray> = ns, overflow: Int = 1, maxObjects: Int = 0,
                       maxBytes: Long = 0L, maxTrackEvents: Int = 0, catalog: ByteArray? = null, timeMode: Int = 0): Int =
        NativeBridge.nativeReceiverAttach(root, parts, overflow, maxObjects, maxBytes, maxTrackEvents, true, catalog, timeMode, out)

    private fun receiverAttach() {
        System.load(BridgeDsos.path("moq5_jni"))
        val root = liveRoot()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        check(attach(root, out) == EndpointAbi.OK, "attach on a live root")
        val child = out[0]
        check(child != EndpointAbi.SENTINEL && child != 0L, "a child token was published")
        check(EndpointAbi.kindOf(child) == 2L, "the token is an OWNED CHILD (kind 2); got kind ${EndpointAbi.kindOf(child)}")
        check(child < 0L, "a first-generation token crosses full width, as a negative jlong")
        val gated = NativeBridge.nativeEndpointStop(root)
        check(gated == EndpointAbi.WRONG_STATE, "the C stop refuses while a receiver is attached; got $gated")
        check(NativeBridge.nativeReceiverClose(root, child) == EndpointAbi.OK, "close retires the child")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK, "stop succeeds once the receiver is gone")
        check(NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "destroy")
        println("RESULT ok")
    }

    private fun receiverCloseStale() {
        System.load(BridgeDsos.path("moq5_jni"))
        val root = liveRoot()
        val other = liveRoot()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        check(attach(root, out) == EndpointAbi.OK, "attach")
        val child = out[0]
        check(NativeBridge.nativeReceiverClose(other, child) == EndpointAbi.REG_OWNER, "a foreign root is not the owner")
        var foreign = Int.MIN_VALUE
        val t = Thread { foreign = NativeBridge.nativeReceiverClose(root, child) }
        t.start(); t.join()
        check(foreign == EndpointAbi.REG_THREAD, "close is authority-thread confined; got $foreign")
        check(NativeBridge.nativeReceiverClose(root, child) == EndpointAbi.OK, "close on the authority thread")
        check(NativeBridge.nativeReceiverClose(root, child) == EndpointAbi.REG_STALE, "a second close is stale")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup root")
        check(NativeBridge.nativeEndpointStop(other) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(other) == EndpointAbi.OK, "cleanup other")
        println("RESULT ok")
    }

    private fun receiverMalformed() {
        System.load(BridgeDsos.path("moq5_jni"))
        val root = liveRoot()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        fun bad(what: String, expect: Int, call: () -> Int) {
            out[0] = EndpointAbi.SENTINEL
            val rc = call()
            check(rc == expect, "$what must be $expect; got $rc")
            check(out[0] == EndpointAbi.SENTINEL, "$what: nothing published; out[0] = ${out[0]}")
        }
        bad("a short out array", EndpointAbi.INVAL) { attach(root, LongArray(0)).also { out[0] = EndpointAbi.SENTINEL } }
        bad("an empty namespace", EndpointAbi.INVAL) { attach(root, out, parts = arrayOf()) }
        bad("an empty part", EndpointAbi.INVAL) { attach(root, out, parts = arrayOf("live".toByteArray(), ByteArray(0))) }
        bad("33 parts", EndpointAbi.INVAL) { attach(root, out, parts = Array(33) { "p".toByteArray() }) }
        bad("more than 4096 bytes of name", EndpointAbi.INVAL) { attach(root, out, parts = arrayOf(ByteArray(4097) { 'a'.code.toByte() })) }
        bad("overflow UNSET", EndpointAbi.INVAL) { attach(root, out, overflow = 0) }
        bad("an unknown overflow ordinal", EndpointAbi.INVAL) { attach(root, out, overflow = 4) }
        bad("an unknown time mode", EndpointAbi.INVAL) { attach(root, out, timeMode = 2) }
        bad("a negative object bound", EndpointAbi.INVAL) { attach(root, out, maxObjects = -1) }
        bad("a negative byte bound", EndpointAbi.INVAL) { attach(root, out, maxBytes = -1L) }
        bad("a negative event bound", EndpointAbi.INVAL) { attach(root, out, maxTrackEvents = -1) }
        bad("an empty catalog track (null is the default)", EndpointAbi.INVAL) { attach(root, out, catalog = ByteArray(0)) }
        val forged = EndpointAbi.tokenOf(EndpointAbi.KIND_ROOT, EndpointAbi.slotOf(root), EndpointAbi.generationOf(root) + 3L)
        bad("a stale root token", EndpointAbi.REG_STALE) { attach(forged, out) }
        // A null out array: refused, nothing published, no throw.
        val nullRc = NativeBridge::class.java.getDeclaredMethod("nativeReceiverAttach", java.lang.Long.TYPE, Array<ByteArray>::class.java, Integer.TYPE, Integer.TYPE, java.lang.Long.TYPE, Integer.TYPE, java.lang.Boolean.TYPE, ByteArray::class.java, Integer.TYPE, LongArray::class.java)
            .let { m -> runCatching { m.invoke(NativeBridge, root, ns, 1, 0, 0L, 0, true, null, 0, null) as Int }.getOrElse { throw (it as? java.lang.reflect.InvocationTargetException)?.targetException ?: it } }
        check(nullRc == EndpointAbi.INVAL, "a null out array is INVAL; got $nullRc")
        // Nothing attached: the root stops cleanly.
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK, "no malformed call attached anything: stop succeeds")
        check(NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "destroy")
        println("RESULT ok")
    }

    private fun receiverInsertFault() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        TrustRouteProbe.nativeArmCtorFault(TrustRouteProbe.CHILD_INSERT_REFUSE_ONCE)
        val rc = attach(root, out)
        check(rc == EndpointAbi.REG_CAPACITY, "a refused child insertion is the registry's exact code; got $rc")
        check(out[0] == EndpointAbi.SENTINEL, "nothing published")
        // The receiver that was attached-but-unowned was destroyed at once:
        // the endpoint's attachment count is restored, so it stops.
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK, "stop succeeds: the unowned receiver did not leak an attachment")
        check(NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "destroy")
        val again = liveRoot()
        check(attach(again, out) == EndpointAbi.OK, "the fault was one-shot: a later attach succeeds")
        check(NativeBridge.nativeReceiverClose(again, out[0]) == EndpointAbi.OK, "close")
        check(NativeBridge.nativeEndpointStop(again) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(again) == EndpointAbi.OK, "cleanup")
        noFault("receiverinsertfault")
        println("RESULT ok")
    }

    private fun receiverSecond() {
        System.load(BridgeDsos.path("moq5_jni"))
        val root = liveRoot()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        check(attach(root, out) == EndpointAbi.OK, "first attach")
        val first = out[0]
        out[0] = EndpointAbi.SENTINEL
        val second = attach(root, out)
        check(second == EndpointAbi.WRONG_STATE, "the endpoint admits one receiver: WRONG_STATE exactly; got $second")
        check(out[0] == EndpointAbi.SENTINEL, "nothing published for the refusal")
        check(NativeBridge.nativeReceiverClose(root, first) == EndpointAbi.OK, "close the first")
        check(attach(root, out) == EndpointAbi.OK, "the slot is free again")
        check(NativeBridge.nativeReceiverClose(root, out[0]) == EndpointAbi.OK, "close the second")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        println("RESULT ok")
    }

    /** The backend seam: the endpoint's refusal is a MoqResultCode; a registry refusal is a Bridge failure, never a code. */
    private fun backendReceiver() {
        System.load(BridgeDsos.path("moq5_jni"))
        val root = liveRoot()
        val b = JniEndpointBackend.adopt(root)
        val cfg = ReceiverConfig(Namespace(listOf(NamespacePart("live".toByteArray()))), OverflowPolicy.DropToKeyframe)
        val first = b.attachReceiver(cfg)
        check(first is BackendAttachResult.Attached, "attach maps to Attached; got $first")
        val second = b.attachReceiver(cfg)
        check(second == BackendAttachResult.Refused(MoqResultCode(EndpointAbi.WRONG_STATE)), "the endpoint's WRONG_STATE is a MoqResultCode; got $second")
        check(b.closeReceiver((first as BackendAttachResult.Attached).token) == BackendOpResult.Ok, "close maps to Ok")
        val stale = b.closeReceiver(first.token)
        check(stale == BackendOpResult.Bridge(BridgeFailure("receiver-close", EndpointAbi.REG_STALE)), "a stale child is a Bridge failure verbatim; got $stale")
        check(b.stop() == BackendStopResult.Stopped && b.destroy() == BackendOpResult.Ok, "cleanup")
        val gone = JniEndpointBackend.adopt(root).attachReceiver(cfg)
        check(gone == BackendAttachResult.Bridge(BridgeFailure("receiver-attach", EndpointAbi.REG_STALE)), "a stale root is a Bridge failure, never a MoqResultCode; got $gone")
        println("RESULT ok")
    }

    // -- discovery through the real receiver -------------------------------

    private val catalogAB = """{"version":"1","tracks":[{"name":"a","packaging":"loc","isLive":true,"role":"video"},{"name":"b","packaging":"loc","isLive":true,"role":"audio"}]}"""

    /** Attaches a discovery-only receiver (no auto-subscribe: no subscription side effects on a bare endpoint). */
    private fun attachDiscovery(root: Long): Long {
        val out = longArrayOf(EndpointAbi.SENTINEL)
        check(NativeBridge.nativeReceiverAttach(root, ns, 1, 0, 0L, 0, false, null, 0, out) == EndpointAbi.OK, "setup: attach")
        return out[0]
    }

    private fun poll(root: Long, rx: Long, scalars: LongArray?): Int = NativeBridge.nativeReceiverPollTrack(root, rx, scalars)

    private fun receiverDiscovery() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val sc = LongArray(ReceiverEventScalars.COUNT)
        check(poll(root, rx, sc) == EndpointAbi.DONE, "no catalog yet: the real poll answers MOQ_DONE")
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalogAB.toByteArray()) == EndpointAbi.OK, "setup: ingest the catalog through the service seam")
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "TRACK_ADDED first; got kind ${sc[ReceiverEventScalars.KIND]}")
        val ta = sc[ReceiverEventScalars.TRACK]
        check(EndpointAbi.kindOf(ta) == 3L && ta < 0L, "a track token is a BORROWED child (kind 3), full width; got $ta")
        val ds = LongArray(TrackDescribeScalars.COUNT)
        val bytes = NativeBridge.nativeTrackDescribe(root, rx, ta, ds)
        check(bytes != null && ds[TrackDescribeScalars.RESULT] == 0L, "describe answers the bytes; result ${ds[TrackDescribeScalars.RESULT]}")
        check(String(bytes!![TrackDescribeBytes.NAME]) == "a" && String(bytes[TrackDescribeBytes.ROLE]) == "video" && String(bytes[TrackDescribeBytes.PACKAGING]) == "loc",
            "the real description crosses byte for byte")
        check(ds[TrackDescribeScalars.IS_LIVE] == 1L, "isLive true from the catalog")
        check(ds[TrackDescribeScalars.HAS_INIT] == 0L && ds[TrackDescribeScalars.HAS_WIDTH] == 0L, "no init and no catalog dimensions in this catalog")
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "TRACK_ADDED(b)")
        val tb = sc[ReceiverEventScalars.TRACK]
        check(tb != ta && EndpointAbi.kindOf(tb) == 3L, "a distinct token per handle")
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 5L && sc[ReceiverEventScalars.TRACK] == 0L, "CATALOG_READY once, no track")
        check(poll(root, rx, sc) == EndpointAbi.DONE, "then empty")
        check(poll(root, rx, sc) == EndpointAbi.DONE, "and still empty: nothing is re-delivered")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverdiscovery")
        println("RESULT ok")
    }

    private fun receiverTrackTokens() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val other = liveRoot()
        val rxOther = attachDiscovery(other)
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalogAB.toByteArray()) == EndpointAbi.OK, "setup: ingest")
        val sc = LongArray(ReceiverEventScalars.COUNT)
        check(poll(root, rx, sc) == EndpointAbi.OK, "ADDED")
        val ta = sc[ReceiverEventScalars.TRACK]
        val ds = LongArray(TrackDescribeScalars.COUNT)
        check(NativeBridge.nativeTrackDescribe(other, rxOther, ta, ds) == null && ds[TrackDescribeScalars.RESULT] == EndpointAbi.REG_OWNER.toLong(), "a track under another receiver is not its child")
        check(poll(other, rx, sc) == EndpointAbi.REG_OWNER, "a receiver polled under the wrong root")
        check(NativeBridge.nativeTrackDescribe(other, rx, ta, ds) == null && ds[TrackDescribeScalars.RESULT] == EndpointAbi.REG_OWNER.toLong(), "a receiver described under the wrong root")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK, "close the receiver")
        check(poll(root, rx, sc) == EndpointAbi.REG_STALE, "the receiver token is stale after close")
        check(NativeBridge.nativeTrackDescribe(root, rx, ta, ds) == null && ds[TrackDescribeScalars.RESULT] == EndpointAbi.REG_STALE.toLong(), "track tokens die with their receiver")
        check(NativeBridge.nativeReceiverClose(other, rxOther) == EndpointAbi.OK, "close the other")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup root")
        check(NativeBridge.nativeEndpointStop(other) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(other) == EndpointAbi.OK, "cleanup other")
        noFault("receivertracktokens")
        println("RESULT ok")
    }

    private fun backendReceiverEvents() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val b = JniEndpointBackend.adopt(root)
        val attached = b.attachReceiver(ReceiverConfig(Namespace(listOf(NamespacePart("live".toByteArray()))), OverflowPolicy.DropToKeyframe, autoSubscribe = false))
        check(attached is BackendAttachResult.Attached, "attach")
        val rx = (attached as BackendAttachResult.Attached).token
        check(b.pollTrack(rx) == BackendTrackEventResult.Empty, "DONE maps to Empty")
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalogAB.toByteArray()) == EndpointAbi.OK, "setup: ingest")
        val ev = b.pollTrack(rx)
        check(ev is BackendTrackEventResult.Event && ev.event.kind == BackendTrackEventKind.Added && ev.event.description?.name == MediaBytes("a".toByteArray()),
            "ADDED maps to an Event carrying the described track; got $ev")
        check(b.closeReceiver(rx) == BackendOpResult.Ok, "close")
        val stale = b.pollTrack(rx)
        check(stale == BackendTrackEventResult.Bridge(BridgeFailure("receiver-poll-track", EndpointAbi.REG_STALE)), "a stale receiver is a Bridge failure, never a code; got $stale")
        check(b.stop() == BackendStopResult.Stopped && b.destroy() == BackendOpResult.Ok, "cleanup")
        println("RESULT ok")
    }

    private fun receiverEventMalformed() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalogAB.toByteArray()) == EndpointAbi.OK, "setup: ingest")
        check(poll(root, rx, null) == EndpointAbi.INVAL, "null scalars")
        val short = LongArray(ReceiverEventScalars.COUNT - 1) { EndpointAbi.SENTINEL }
        check(poll(root, rx, short) == EndpointAbi.INVAL && short.all { it == EndpointAbi.SENTINEL }, "short scalars: refused, untouched")
        val sc = LongArray(ReceiverEventScalars.COUNT)
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "the malformed calls consumed nothing: ADDED(a) is still first")
        val ta = sc[ReceiverEventScalars.TRACK]
        check(NativeBridge.nativeTrackDescribe(root, rx, ta, null) == null, "null describe scalars: null, no throw")
        val dshort = LongArray(TrackDescribeScalars.COUNT - 1) { EndpointAbi.SENTINEL }
        check(NativeBridge.nativeTrackDescribe(root, rx, ta, dshort) == null && dshort[TrackDescribeScalars.RESULT] == EndpointAbi.INVAL.toLong() && dshort.drop(1).all { it == EndpointAbi.SENTINEL }, "short describe scalars: null with INVAL in slot 0, the rest untouched")
        val ds = LongArray(TrackDescribeScalars.COUNT)
        val nextGeneration = ta + (1L shl 32) // the same slot under a generation the registry never minted
        check(NativeBridge.nativeTrackDescribe(root, rx, nextGeneration, ds) == null && ds[TrackDescribeScalars.RESULT] == EndpointAbi.REG_STALE.toLong(), "an unknown track token is the registry's refusal")
        check(NativeBridge.nativeTrackDescribe(root, rx, rx, ds) == null && ds[TrackDescribeScalars.RESULT] == EndpointAbi.REG_KIND.toLong(), "the receiver's own token is not a track")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivereventmalformed")
        println("RESULT ok")
    }

    private fun receiverTrackInsertFault() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalogAB.toByteArray()) == EndpointAbi.OK, "setup: ingest")
        TrustRouteProbe.nativeArmCtorFault(TrustRouteProbe.CHILD_INSERT_REFUSE_ONCE)
        val sc = LongArray(ReceiverEventScalars.COUNT) { EndpointAbi.SENTINEL }
        val rc = poll(root, rx, sc)
        check(rc == EndpointAbi.REG_CAPACITY, "a refused track-token insertion after a consumed event is the registry's exact code, never a track; got $rc")
        check(sc[ReceiverEventScalars.TRACK] == EndpointAbi.SENTINEL, "no token published")
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L && EndpointAbi.kindOf(sc[ReceiverEventScalars.TRACK]) == 3L, "the fault was one-shot: the next event still mints a token")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK, "the receiver still retires cleanly")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivertrackinsertfault")
        println("RESULT ok")
    }

    // -- objects through the real receiver ----------------------------------

    /**
     * The two explicit test-only preconditions a session-less receiver needs
     * before an object can be admitted, then discovery: the transport draft
     * (what the pump reads from a live session; set BEFORE the catalog is
     * ingested, because the track descriptions derive from it), and, after
     * discovery, the track marked DESIRED. Returns the first track's token.
     */
    private fun discover(root: Long, rx: Long, catalog: String = catalogAB): Long {
        check(TrustRouteProbe.nativeSetTransportVersion(root, rx, DRAFT_16) == EndpointAbi.OK, "setup: the transport-draft precondition")
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalog.toByteArray()) == EndpointAbi.OK, "setup: ingest")
        val sc = LongArray(ReceiverEventScalars.COUNT)
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "setup: ADDED")
        val ta = sc[ReceiverEventScalars.TRACK]
        while (poll(root, rx, sc) == EndpointAbi.OK) { /* drain the rest of discovery */ }
        check(TrustRouteProbe.nativeMarkTrackDesired(root, rx, ta) == EndpointAbi.OK, "setup: the explicit test-only precondition for object admission")
        return ta
    }

    private fun receiverObjectNoVersion() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        // No transport draft: the catalog still discovers, but no object can parse.
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalogAB.toByteArray()) == EndpointAbi.OK, "setup: ingest without the draft precondition")
        val sc = LongArray(ReceiverEventScalars.COUNT)
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "discovery still answers ADDED")
        val ta = sc[ReceiverEventScalars.TRACK]
        while (poll(root, rx, sc) == EndpointAbi.OK) { /* drain */ }
        check(TrustRouteProbe.nativeMarkTrackDesired(root, rx, ta) == EndpointAbi.OK, "the desire precondition alone")
        val cleanupsBefore = TrustRouteProbe.objectCleanups()
        check(TrustRouteProbe.nativeIngestObject(root, rx, ta, 0L, 0L, 0, false, 1_700_000_000_000_000L, "unparseable".toByteArray()) == EndpointAbi.OK, "feed")
        val os = LongArray(ReceiverObjectScalars.COUNT)
        check(pollObject(root, rx, os) == null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.DONE.toLong(), "the real intake could not parse it without a transport draft: nothing admitted")
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore, "nothing reached the bridge")
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 7L && sc[ReceiverEventScalars.TRACK] == ta, "and the drop is visible as the track's coalesced PARSE_DROP: the draft precondition is load-bearing")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverobjectnoversion")
        println("RESULT ok")
    }

    private fun receiverObjectUndesired() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        check(TrustRouteProbe.nativeSetTransportVersion(root, rx, DRAFT_16) == EndpointAbi.OK, "setup: the transport-draft precondition")
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalogAB.toByteArray()) == EndpointAbi.OK, "setup: ingest")
        val sc = LongArray(ReceiverEventScalars.COUNT)
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "setup: ADDED")
        val ta = sc[ReceiverEventScalars.TRACK]
        while (poll(root, rx, sc) == EndpointAbi.OK) { /* drain */ }
        val cleanupsBefore = TrustRouteProbe.objectCleanups()
        check(TrustRouteProbe.nativeIngestObject(root, rx, ta, 0L, 0L, 0, false, 0L, "dropped".toByteArray()) == EndpointAbi.OK, "feed without the precondition")
        val os = LongArray(ReceiverObjectScalars.COUNT)
        check(pollObject(root, rx, os) == null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.DONE.toLong(), "the real intake dropped it: an undesired track admits nothing")
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore, "nothing reached the bridge, nothing was cleaned by it")
        check(TrustRouteProbe.nativeMarkTrackDesired(root, rx, ta) == EndpointAbi.OK, "the explicit precondition")
        check(TrustRouteProbe.nativeIngestObject(root, rx, ta, 0L, 1L, 0, false, 0L, "admitted".toByteArray()) == EndpointAbi.OK, "feed again")
        val bytes = pollObject(root, rx, os)
        check(bytes != null && String(bytes!![ReceiverObjectBytes.PAYLOAD]) == "admitted", "now it delivers: the precondition is load-bearing, not vacuous")
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore + 1, "cleaned exactly once")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverobjectundesired")
        println("RESULT ok")
    }

    private fun pollObject(root: Long, rx: Long, scalars: LongArray?): Array<ByteArray>? = NativeBridge.nativeReceiverPollObject(root, rx, scalars)

    private fun receiverObjectLoc() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val ta = discover(root, rx)
        val os = LongArray(ReceiverObjectScalars.COUNT)
        check(pollObject(root, rx, os) == null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.DONE.toLong(), "no object yet: MOQ_DONE")
        check(TrustRouteProbe.nativeIngestObject(root, rx, ta, 0L, 0L, 0, false, 1_700_000_000_000_000L, "LOCPAYLOAD".toByteArray()) == EndpointAbi.OK, "setup: feed one LOC object through the real intake")
        val cleanupsBefore = TrustRouteProbe.objectCleanups()
        val bytes = pollObject(root, rx, os)
        check(bytes != null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.OK.toLong(), "the real poll answers the object; slot 0 = ${os[ReceiverObjectScalars.RESULT]}")
        check(os[ReceiverObjectScalars.TRACK] == ta, "the track crosses as the SAME borrowed token discovery minted")
        check(os[ReceiverObjectScalars.PACKAGING] == 1L, "LOC media is RAW geometry")
        check(String(bytes!![ReceiverObjectBytes.PAYLOAD]) == "LOCPAYLOAD" && bytes[ReceiverObjectBytes.FRAGMENT].isEmpty() && os[ReceiverObjectScalars.SAMPLE_COUNT] == 0L, "payload exact, fragment empty, no samples")
        check(os[ReceiverObjectScalars.HAS_CAPTURE] == 1L && os[ReceiverObjectScalars.CAPTURE_US] == 1_700_000_000_000_000L, "capture time as fed (wall clock)")
        check(os[ReceiverObjectScalars.DECODE_US] == 1_700_000_000_000_000L && os[ReceiverObjectScalars.PRESENTATION_US] == 1_700_000_000_000_000L && os[ReceiverObjectScalars.COMPOSITION_US] == 0L,
            "for RAW/LOC the C derives decode and presentation time FROM the capture time, offset zero")
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore + 1, "the C object was cleaned exactly once, inside the row")
        check(pollObject(root, rx, os) == null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.DONE.toLong(), "then empty")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverobjectloc")
        println("RESULT ok")
    }

    private fun receiverObjectCmaf() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val tc = discover(root, rx, catalogCmaf)
        val fragment = TrustRouteProbe.nativeCmafFragmentFixture()
        check(TrustRouteProbe.nativeIngestObject(root, rx, tc, 0L, 0L, 0, false, 0L, fragment) == EndpointAbi.OK, "setup: feed one CMAF fragment")
        val cleanupsBefore = TrustRouteProbe.objectCleanups()
        val os = LongArray(ReceiverObjectScalars.COUNT)
        val bytes = pollObject(root, rx, os)
        check(bytes != null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.OK.toLong(), "the CMAF object delivers")
        check(os[ReceiverObjectScalars.PACKAGING] == 2L, "CMAF geometry")
        check(bytes!![ReceiverObjectBytes.FRAGMENT].contentEquals(fragment) && bytes[ReceiverObjectBytes.PAYLOAD].isEmpty(), "the full fragment is kept and the payload is empty")
        val off = os[ReceiverObjectScalars.MDAT_OFFSET]; val len = os[ReceiverObjectScalars.MDAT_LEN]
        check(len > 0 && off + len <= fragment.size, "the mdat range lies inside the fragment: $off+$len of ${fragment.size}")
        check(os[ReceiverObjectScalars.SAMPLE_COUNT] >= 1L && bytes[ReceiverObjectBytes.SAMPLES].size == (os[ReceiverObjectScalars.SAMPLE_COUNT] * ReceiverObjectBytes.SAMPLE_STRIDE).toInt(), "samples crossed, packed at the fixed stride")
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore + 1, "cleaned exactly once")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverobjectcmaf")
        println("RESULT ok")
    }

    /**
     * A payload-free CMAF status object, end to end.
     *
     * `moq_media_object_parse` accepts END_OF_GROUP with no payload and returns
     * before it parses any media (`media/object/src/media_object.c:157-174`),
     * and the receiver converts only END_OF_TRACK into a track event
     * (`service/src/media_receiver.c:1843`). So a CMAF track can legally deliver
     * a status-only object, and the carrier that reaches Kotlin is empty in
     * every media field.
     *
     * Both halves are exercised on the real thing: the raw carrier is pinned
     * field by field, and a second identical object is decoded through
     * `JniEndpointBackend.pollObject`, which is the call production makes.
     */
    private fun cmafStatusOnly() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val problems = mutableListOf<String>()
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val tc = discover(root, rx, catalogCmaf)

        // Two identical status-only objects: one for the carrier, one for the
        // production decode. Each poll consumes one.
        check(TrustRouteProbe.nativeIngestObject(root, rx, tc, 0L, 0L, 1, true, 0L, ByteArray(0)) == EndpointAbi.OK,
            "setup: feed a payload-free END_OF_GROUP object")
        check(TrustRouteProbe.nativeIngestObject(root, rx, tc, 0L, 1L, 1, true, 0L, ByteArray(0)) == EndpointAbi.OK,
            "setup: feed a second one")

        // -- the carrier the C actually produces -----------------------------
        val cleanupsBefore = TrustRouteProbe.objectCleanups()
        val os = LongArray(ReceiverObjectScalars.COUNT)
        val bytes = pollObject(root, rx, os)
        check(bytes != null, "the status-only object delivers; result=${os[ReceiverObjectScalars.RESULT]}")
        fun want(what: String, wanted: Long, got: Long) { if (got != wanted) problems += "$what: expected $wanted, got $got" }
        want("the poll answers MOQ_OK", EndpointAbi.OK.toLong(), os[ReceiverObjectScalars.RESULT])
        want("the object carries the discovered track token", tc, os[ReceiverObjectScalars.TRACK])
        want("the end-of-group flag is set", 1L, os[ReceiverObjectScalars.END_OF_GROUP])
        want("packaging is CMAF", 2L, os[ReceiverObjectScalars.PACKAGING])
        want("status is END_OF_GROUP", 1L, os[ReceiverObjectScalars.STATUS])
        want("the mdat offset is zero", 0L, os[ReceiverObjectScalars.MDAT_OFFSET])
        want("the mdat length is zero", 0L, os[ReceiverObjectScalars.MDAT_LEN])
        want("no samples", 0L, os[ReceiverObjectScalars.SAMPLE_COUNT])
        if (bytes!![ReceiverObjectBytes.PAYLOAD].isNotEmpty()) problems += "the payload must be empty"
        if (bytes[ReceiverObjectBytes.FRAGMENT].isNotEmpty()) problems += "the fragment must be empty"
        if (bytes[ReceiverObjectBytes.SAMPLES].isNotEmpty()) problems += "the sample carrier must be empty"
        val afterFirst = TrustRouteProbe.objectCleanups() - cleanupsBefore
        if (afterFirst != 1L) problems += "the first poll must release exactly one native object, saw $afterFirst"

        // -- the production decode path --------------------------------------
        val b = JniEndpointBackend.adopt(root)
        val decoded = runCatching { b.pollObject(rx) }
        val failure = decoded.exceptionOrNull()
        if (failure != null) {
            problems += "the production decode refused a legal status-only CMAF object: $failure"
        } else {
            val r = decoded.getOrNull()
            if (r !is BackendObjectResult.Object) {
                problems += "the production decode did not deliver an object; got $r"
            } else {
                val o = r.value
                if (o.status != ObjectStatus.EndOfGroup) problems += "status decoded as ${o.status}"
                val payload = o.payload
                if (payload !is MediaPayload.Cmaf) {
                    problems += "a CMAF track's status object must stay CMAF; got $payload"
                } else {
                    if (payload.fragment.bytes().isNotEmpty()) problems += "the decoded fragment must be empty"
                    if (payload.mdat.offset != 0 || payload.mdat.length != 0) problems += "the decoded mdat must be 0,0; got ${payload.mdat}"
                    if (payload.samples.isNotEmpty()) problems += "the decoded object must carry no samples"
                    val media = MediaObjectCodec.mediaBytesOf(payload)
                    if (media.bytes().isNotEmpty()) problems += "its decodable media must be empty; got ${media.bytes().size} bytes"
                }
            }
        }

        val afterBoth = TrustRouteProbe.objectCleanups() - cleanupsBefore
        if (afterBoth != 2L) problems += "both polls together must release exactly two native objects, saw $afterBoth"
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK &&
            NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("cmafstatusonly")
        check(problems.isEmpty(), "payload-free CMAF status: " + problems.joinToString(" | "))
        println("RESULT ok")
    }

    /**
     * The PUBLIC object stream, delivering a payload-free CMAF status object.
     *
     * Everything production owns stays production's: `MoqEndpoint` builds the
     * engine, the engine's worker drives the real `JniEndpointBackend`, and the
     * object is polled, decoded and handed to `receiver.objects.next()` by the
     * shipped code. The fixture owns only the ROOT and the injected media, and
     * it does that work on the engine's own worker thread -- which it must,
     * because the registry is authority-thread confined and the worker is the
     * authority here.
     *
     * The delegating backend below is the same `EndpointBackend by inner`
     * pattern the engine fixtures already use. It adds nothing to the calls it
     * forwards: it attaches through the real backend and then, ON THAT THREAD,
     * sets the transport-draft precondition and ingests the CMAF catalog; and
     * when the real track poll returns the ADDED event, it marks that track
     * desired and injects one END_OF_GROUP before returning the event
     * unchanged. Object polling, decoding, scheduling and delivery are never
     * touched.
     *
     * This is a HOST fixture: the root is the scripted target and the media is
     * injected. It is not authenticated-network and not device evidence.
     */
    /** Where the public row may be made to fail, so its reporting can be judged. */
    private enum class PublicFault { None, Body, BodyAndCleanup, CleanupOnly, SetupAfterAttach, BodyCleanupAudit }

    /**
     * The declared, distinguishable body failure: a class and a cause the
     * parent can name exactly, so an unrelated exception cannot satisfy it.
     */
    private fun injectedBodyFailure(): Nothing =
        throw UnsupportedOperationException("r6-injected-body", IllegalArgumentException("r6-injected-cause"))

    /**
     * One reported problem, kept as the THROWABLE until it is emitted, so its
     * class and cause survive to the child's output and the primary can be
     * rethrown unchanged.
     */
    private class Problem(val phase: String, val error: Throwable?, val note: String? = null) {
        /** A field-delimited record, so the parent compares an exact inventory rather than a substring. */
        fun line(): String {
            val e = error
            return if (e == null) {
                "PROBLEM|$phase|note||${note.orEmpty()}|"
            } else {
                val c = e.cause
                "PROBLEM|$phase|${e::class.java.name}|${e.message.orEmpty()}|" +
                    "${c?.let { it::class.java.name }.orEmpty()}|${c?.message.orEmpty()}"
            }
        }
    }

    private class PublicFixtureBackend(
        private val inner: JniEndpointBackend,
        private val root: Long,
        private val fault: PublicFault,
    ) : EndpointBackend by inner {
        var attachSetups = 0
            private set
        var injections = 0
            private set
        var setupThreads = mutableSetOf<Long>()
            private set
        var track = 0L
            private set
        /** What the fixture's own release of a not-yet-returned child answered. */
        var orphanClose: BackendOpResult? = null
            private set
        var orphanToken = 0L
            private set

        override fun attachReceiver(config: ReceiverConfig): BackendAttachResult {
            val r = inner.attachReceiver(config)
            if (r is BackendAttachResult.Attached) {
                setupThreads += Thread.currentThread().threadId()
                try {
                    check(TrustRouteProbe.nativeSetTransportVersion(root, r.token, DRAFT_16) == EndpointAbi.OK) {
                        "fixture: the transport-draft precondition"
                    }
                    check(TrustRouteProbe.nativeIngestCatalog(root, r.token, catalogCmaf.toByteArray()) == EndpointAbi.OK) {
                        "fixture: ingest the CMAF catalog"
                    }
                    if (fault == PublicFault.SetupAfterAttach) {
                        error("fixture: injected setup failure after a successful native attach")
                    }
                } catch (t: Throwable) {
                    // THE ENGINE DOES NOT OWN THIS CHILD YET: it registers only
                    // once this Attached result returns, so a setup that throws
                    // here would strand a live native receiver. The fixture
                    // closes what it caused to exist, then rethrows.
                    orphanToken = r.token
                    orphanClose = runCatching { inner.closeReceiver(r.token) }.getOrNull()
                    throw t
                }
                attachSetups++
            }
            return r
        }

        override fun pollTrack(token: Long): BackendTrackEventResult {
            val ev = inner.pollTrack(token)
            if (ev is BackendTrackEventResult.Event && ev.event.kind == BackendTrackEventKind.Added && injections == 0) {
                setupThreads += Thread.currentThread().threadId()
                track = ev.event.track
                check(TrustRouteProbe.nativeMarkTrackDesired(root, token, track) == EndpointAbi.OK) {
                    "fixture: mark the discovered track desired"
                }
                check(
                    TrustRouteProbe.nativeIngestObject(root, token, track, 0L, 0L, 1, true, 0L, ByteArray(0)) ==
                        EndpointAbi.OK,
                ) { "fixture: inject one payload-free END_OF_GROUP object" }
                injections++
            }
            return ev
        }
    }

    /**
     * The PUBLIC object stream, delivering a payload-free CMAF status object.
     *
     * Everything production owns stays production's: `MoqEndpoint` builds the
     * engine, the engine's worker drives the real `JniEndpointBackend`, and the
     * object is polled, decoded and handed to `receiver.objects.next()` by the
     * shipped code. The fixture owns only the ROOT and the injected media, and
     * does that work on the engine's own worker thread -- which it must, because
     * the registry is authority-thread confined and the worker is the authority
     * for a root it created.
     *
     * REPORTING: the body's failure is CAPTURED, never thrown out of the
     * coroutine. Teardown and the fixture audits then always run, and every
     * problem -- the body's, teardown's and the audits' -- is printed to the
     * child's own output before the row reports. A cleanup-only failure fails on
     * its own, and no path prints `RESULT ok` when anything failed.
     *
     * This is a HOST fixture: the root is the scripted target and the media is
     * injected. It is not authenticated-network and not device evidence.
     */
    private fun cmafStatusPublic(fault: PublicFault = PublicFault.None) {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val problems = mutableListOf<Problem>()
        val evidence = mutableListOf<String>()
        var fixture: PublicFixtureBackend? = null
        var rootToken = 0L
        val cleanupsBefore = TrustRouteProbe.objectCleanups()

        kotlinx.coroutines.runBlocking {
            var endpoint: MoqEndpoint? = null
            var receiver: MediaReceiver? = null
            // The body's own failure is COLLECTED, so teardown and the audits
            // below always run and are always reported.
            val body =
                runCatching {
                    endpoint =
                        MoqEndpoint.open(EndpointConfig(EndpointAbi.UNSERVABLE)) {
                            // ON THE ENGINE WORKER: the root created here has
                            // that thread as its registry authority, which is
                            // what makes the fixture's later injections legal.
                            val t = longArrayOf(0L)
                            check(NativeBridge.nativeEndpointCreate(EndpointAbi.UNSERVABLE, t) == EndpointAbi.OK) {
                                "fixture: construct the scripted root on the worker"
                            }
                            rootToken = t[0]
                            PublicFixtureBackend(JniEndpointBackend.adopt(t[0]), t[0], fault).also { fixture = it }
                        }
                    val rx =
                        endpoint!!.attachReceiver(
                            ReceiverConfig(
                                Namespace(listOf(NamespacePart("live".toByteArray()))),
                                OverflowPolicy.DropToKeyframe,
                                autoSubscribe = false,
                            ),
                        )
                    receiver = rx
                    val obj = kotlinx.coroutines.withTimeout(30_000L) { rx.objects.next() }
                    checkNotNull(obj) { "the public stream ended before delivering the status object" }

                    // -- IDENTITY: the object carries the INVENTORY's instance --
                    val inventory = rx.inventory.value.tracks
                    check(inventory.size == 1) { "the inventory must hold exactly the discovered track; got ${inventory.size}" }
                    val inventoryTrack = inventory[0]
                    val f = checkNotNull(fixture) { "the fixture backend was never constructed" }
                    check(f.track != 0L) { "the fixture never recorded the discovered track token" }
                    check(inventoryTrack.token == f.track) {
                        "the inventory track must be the one the real Added event supplied; got ${inventoryTrack.token} not ${f.track}"
                    }
                    check(obj.track === inventoryTrack) {
                        "the delivered object must carry the INVENTORY's own MediaTrack instance, not an equal copy"
                    }
                    // Separately, as catalog evidence rather than identity.
                    check(obj.track.description.name == MediaBytes("v".toByteArray())) {
                        "the catalog named the track 'v'; got ${obj.track.description.name}"
                    }

                    // -- METADATA, derived from the parser and the injection ---
                    // A non-NORMAL status returns from moq_media_object_parse
                    // before any media or timing is computed, so everything
                    // below the status is the zeroed initial state; the fixture
                    // declares end_of_group=true, datagram=false, capture=0.
                    check(obj.status == ObjectStatus.EndOfGroup) { "status was ${obj.status}" }
                    check(obj.endOfGroup) { "the injection declared end_of_group=true" }
                    check(!obj.viaDatagram) { "the injection declared datagram=false" }
                    check(!obj.keyframe) { "a status object parses no media and is never a keyframe" }
                    check(obj.captureTime == null) { "no LOC timestamp was attached; got ${obj.captureTime}" }
                    check(obj.decodeTime == MediaMicros(0uL)) { "no timing is computed for a status object; got ${obj.decodeTime}" }
                    check(obj.presentationTime == MediaMicros(0uL)) { "no timing is computed for a status object; got ${obj.presentationTime}" }
                    check(obj.compositionOffset == MediaOffsetMicros(0L)) { "no timing is computed for a status object; got ${obj.compositionOffset}" }

                    // -- the media, empty at every level ----------------------
                    val payload = obj.payload
                    check(payload is MediaPayload.Cmaf) { "a CMAF track's status object must stay CMAF; got $payload" }
                    payload as MediaPayload.Cmaf
                    check(payload.fragment.bytes().isEmpty()) { "the fragment must be empty" }
                    check(payload.mdat == ByteRange(0, 0)) { "the mdat must be 0,0; got ${payload.mdat}" }
                    check(payload.samples.isEmpty()) { "there must be no samples" }
                    check(obj.mediaBytes().bytes().isEmpty()) {
                        "the PUBLIC decodable media must be empty; got ${obj.mediaBytes().bytes().size} bytes"
                    }

                    // -- ownership --------------------------------------------
                    val cleaned = TrustRouteProbe.objectCleanups() - cleanupsBefore
                    check(cleaned == 1L) { "expected exactly one native object cleanup, saw $cleaned" }
                    val queued = rx.statsSnapshot().objectsQueued
                    check(queued == 0uL) { "the consumed object is still queued: $queued" }
                    if (fault == PublicFault.Body || fault == PublicFault.BodyAndCleanup ||
                        fault == PublicFault.BodyCleanupAudit
                    ) {
                        injectedBodyFailure()
                    }
                }.exceptionOrNull()
            if (body != null) problems += Problem("body", body)

            // Teardown ALWAYS runs, bounded, receiver before endpoint, and what
            // it actually did is reported -- not merely that nothing failed.
            val closed = runCatching { receiver?.close() }.exceptionOrNull()
            if (closed != null) problems += Problem("cleanup", closed)
            val ended =
                runCatching { kotlinx.coroutines.withTimeout(30_000L) { endpoint?.closeAndJoin() } }.exceptionOrNull()
            if (ended != null) problems += Problem("cleanup", ended)
            evidence += "DONE|teardown|receiver=${if (receiver == null) "absent" else if (closed == null) "closed" else "failed"}" +
                "|endpoint=${if (endpoint == null) "absent" else if (ended == null) "retired" else "failed"}"
            if (fault == PublicFault.CleanupOnly || fault == PublicFault.BodyAndCleanup ||
                fault == PublicFault.BodyCleanupAudit
            ) {
                problems += Problem("cleanup", null, "r6-injected-cleanup")
            }
        }

        // The audits run whatever happened above, and a THROWING audit is
        // collected as one more failure rather than being allowed to skip the
        // emission below.
        val audit =
            runCatching {
                val f = fixture
                if (f == null) {
                    problems += Problem("audit", null, "the internal factory was never invoked")
                } else if (fault == PublicFault.SetupAfterAttach) {
                    // The fixture had to release a child the engine had not
                    // adopted yet. Native evidence, not a count of attempts.
                    if (f.orphanToken == 0L) problems += Problem("audit", null, "no attached child was released")
                    if (f.orphanClose != BackendOpResult.Ok) {
                        problems += Problem("audit", null, "the fixture's own close answered ${f.orphanClose}")
                    }
                    val stale = NativeBridge.nativeReceiverClose(rootToken, f.orphanToken)
                    if (stale != EndpointAbi.REG_STALE) {
                        problems += Problem("audit", null, "the released child is not stale; close answered $stale")
                    }
                    evidence += "DONE|audits|released=${f.orphanToken != 0L}|close=${f.orphanClose}|stale=$stale"
                } else {
                    if (f.attachSetups != 1) problems += Problem("audit", null, "${f.attachSetups} attachments were set up, expected 1")
                    if (f.injections != 1) problems += Problem("audit", null, "${f.injections} objects were injected, expected 1")
                    if (f.setupThreads.size != 1) problems += Problem("audit", null, "the fixture ran on ${f.setupThreads.size} threads")
                    evidence += "DONE|audits|setups=${f.attachSetups}|injections=${f.injections}|threads=${f.setupThreads.size}"
                }
                if (rootToken == 0L) problems += Problem("audit", null, "no root was constructed")
                // A test-local controlled failure of the FINAL audit. It is not
                // a native recorder fault; it exists to prove that a throwing
                // audit is collected rather than allowed to suppress emission.
                if (fault == PublicFault.BodyCleanupAudit) error("r6-injected-audit")
                // The real recorder check, which can itself throw.
                noFault("cmafstatuspublic")
                evidence += "DONE|final-audit|recorder=clean"
            }.exceptionOrNull()
        if (audit != null) problems += Problem("final-audit", audit)

        // EVERYTHING the parent judges is emitted FIRST, before anything that
        // can throw: every problem with its class and cause, and the positive
        // evidence that teardown and the audits actually completed.
        problems.forEach { println(it.line()) }
        evidence.forEach { println(it) }

        // The ORIGINAL primary is rethrown unchanged -- same object, same class,
        // same cause. Only a failure with no throwable of its own is reported
        // through a new one.
        val primary = problems.firstOrNull { it.error != null }?.error
        if (primary != null) throw primary
        check(problems.isEmpty()) {
            "public CMAF status delivery: " + problems.joinToString(" | ") { "${it.phase}: ${it.note}" }
        }
        println("RESULT ok")
    }

    private fun receiverObjectMalformed() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val ta = discover(root, rx)
        check(TrustRouteProbe.nativeIngestObject(root, rx, ta, 0L, 0L, 0, false, 0L, "queued".toByteArray()) == EndpointAbi.OK, "setup: one queued object")
        val cleanupsBefore = TrustRouteProbe.objectCleanups()
        check(pollObject(root, rx, null) == null, "null scalars: null, no throw")
        val short = LongArray(ReceiverObjectScalars.COUNT - 1) { EndpointAbi.SENTINEL }
        check(pollObject(root, rx, short) == null && short[ReceiverObjectScalars.RESULT] == EndpointAbi.INVAL.toLong() && short.drop(1).all { it == EndpointAbi.SENTINEL }, "short scalars: INVAL in slot 0, the rest untouched")
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore, "nothing was polled, so nothing was cleaned")
        val os = LongArray(ReceiverObjectScalars.COUNT)
        val bytes = pollObject(root, rx, os)
        check(bytes != null && String(bytes!![ReceiverObjectBytes.PAYLOAD]) == "queued", "the malformed calls consumed nothing")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverobjectmalformed")
        println("RESULT ok")
    }

    private fun receiverObjectInterrupted() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val ta = discover(root, rx)
        check(TrustRouteProbe.nativeIngestObject(root, rx, ta, 0L, 0L, 0, false, 0L, "gated".toByteArray()) == EndpointAbi.OK, "setup: one queued object")
        check(NativeBridge.nativeEndpointSetInterrupted(root, true) == EndpointAbi.OK, "set the latch")
        val cleanupsBefore = TrustRouteProbe.objectCleanups()
        val os = LongArray(ReceiverObjectScalars.COUNT)
        check(pollObject(root, rx, os) == null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.ERR_INTERRUPTED.toLong(), "the real C gate: INTERRUPTED before the queue is read")
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore, "nothing consumed, nothing cleaned")
        check(NativeBridge.nativeEndpointSetInterrupted(root, false) == EndpointAbi.OK, "clear the latch")
        val bytes = pollObject(root, rx, os)
        check(bytes != null && String(bytes!![ReceiverObjectBytes.PAYLOAD]) == "gated", "the same object delivers after resume")
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore + 1, "cleaned exactly once")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverobjectinterrupted")
        println("RESULT ok")
    }

    private fun receiverObjectIdentity() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val ta = discover(root, rx)
        // (group, subgroup, object, priority, datagram): one id above Long.MAX_VALUE crosses as a negative jlong bit pattern
        val fed = listOf(
            longArrayOf(9L, 4L, 3L, 17L, 0L),
            longArrayOf(-1L, Long.MIN_VALUE, -2L, 255L, 0L),
            longArrayOf(0L, 0L, 0L, 0L, 0L),
            longArrayOf(7L, 0L, 5L, 200L, 1L),
        )
        for ((i, f) in fed.withIndex())
            check(TrustRouteProbe.nativeIngestObjectIdentity(root, rx, ta, f[0], f[1], f[2], f[3].toInt(), f[4] != 0L, "id$i".toByteArray()) == EndpointAbi.OK, "setup: object $i queued")
        val cleanupsBefore = TrustRouteProbe.objectCleanups()
        val problems = mutableListOf<String>()
        fun want(what: String, wanted: Long, got: Long) { if (got != wanted) problems += "$what: expected $wanted, got $got" }
        for ((i, f) in fed.withIndex()) {
            val os = LongArray(ReceiverObjectScalars.COUNT)
            val bytes = pollObject(root, rx, os)
            check(bytes != null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.OK.toLong(), "object $i delivers")
            check(String(bytes!![ReceiverObjectBytes.PAYLOAD]) == "id$i", "object $i bytes")
            want("object $i HAS_IDENTITY", 1L, os[ReceiverObjectScalars.HAS_IDENTITY])
            want("object $i GROUP", f[0], os[ReceiverObjectScalars.GROUP])
            want("object $i OBJECT", f[2], os[ReceiverObjectScalars.OBJECT])
            want("object $i PRIORITY", f[3], os[ReceiverObjectScalars.PRIORITY])
            want("object $i DATAGRAM", f[4], os[ReceiverObjectScalars.DATAGRAM])
            want("object $i HAS_SUBGROUP", if (f[4] != 0L) 0L else 1L, os[ReceiverObjectScalars.HAS_SUBGROUP])
            want("object $i SUBGROUP", if (f[4] != 0L) 0L else f[1], os[ReceiverObjectScalars.SUBGROUP])
        }
        check(problems.isEmpty(), problems.joinToString("; "))
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore + fed.size, "each cleaned exactly once")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverobjectidentity")
        println("RESULT ok")
    }

    private fun receiverObjectFirstSight() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        check(TrustRouteProbe.nativeSetTransportVersion(root, rx, DRAFT_16) == EndpointAbi.OK, "setup: the transport-draft precondition")
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalogAB.toByteArray()) == EndpointAbi.OK, "setup: ingest, but do NOT drain discovery")
        check(TrustRouteProbe.nativeMarkTrackDesired(root, rx, TrustRouteProbe.FIRST_TRACK) == EndpointAbi.OK, "setup: the precondition by handle (no token exists yet)")
        check(TrustRouteProbe.nativeIngestObject(root, rx, TrustRouteProbe.FIRST_TRACK, 0L, 0L, 0, false, 0L, "early".toByteArray()) == EndpointAbi.OK, "setup: an object for the first track, whose ADDED is queued but never polled")
        val cleanupsBefore = TrustRouteProbe.objectCleanups()
        val os = LongArray(ReceiverObjectScalars.COUNT)
        val bytes = pollObject(root, rx, os)
        check(bytes != null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.OK.toLong(), "the object delivers although its ADDED is still queued; slot 0 = ${os[ReceiverObjectScalars.RESULT]}")
        val minted = os[ReceiverObjectScalars.TRACK]
        check(minted < 0L && EndpointAbi.kindOf(minted) == 3L, "its track crosses as a BORROWED token minted on this first sight; got $minted")
        check(String(bytes!![ReceiverObjectBytes.PAYLOAD]) == "early", "the bytes")
        check(TrustRouteProbe.objectCleanups() == cleanupsBefore + 1, "cleaned exactly once")
        val sc = LongArray(ReceiverEventScalars.COUNT)
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 1L, "the later track poll: ADDED")
        check(sc[ReceiverEventScalars.TRACK] == minted, "and it carries the SAME token the object minted: one token per handle, from the bridge's first sight")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverobjectfirstsight")
        println("RESULT ok")
    }

    private fun receiverObjectOwnership() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val other = liveRoot()
        discover(root, rx)
        val os = LongArray(ReceiverObjectScalars.COUNT)
        check(pollObject(other, rx, os) == null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.REG_OWNER.toLong(), "wrong root")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK, "close")
        check(pollObject(root, rx, os) == null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.REG_STALE.toLong(), "stale after close")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup root")
        check(NativeBridge.nativeEndpointStop(other) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(other) == EndpointAbi.OK, "cleanup other")
        noFault("receiverobjectownership")
        println("RESULT ok")
    }

    private val catalogCmaf = """{"version":"1","tracks":[{"name":"v","packaging":"cmaf","isLive":true,"role":"video","codec":"avc1.42e01e","timescale":90000}]}"""

    /** `MOQ_VERSION_DRAFT_16` (`core/include/moq/session.h`). */
    private val DRAFT_16 = 16

    // -- manual controls through the real receiver -----------------------

    private fun trackState(root: Long, rx: Long, track: Long): Long {
        val out = longArrayOf(EndpointAbi.SENTINEL)
        val rc = NativeBridge.nativeTrackState(root, rx, track, out)
        check(rc == EndpointAbi.OK, "track_state answers OK; got $rc")
        return out[0]
    }

    /** Discovery with the draft precondition but WITHOUT the desire helper: the controls set the desire themselves. */
    private fun discoverManual(root: Long, rx: Long, catalog: String = catalogAB): List<Long> {
        check(TrustRouteProbe.nativeSetTransportVersion(root, rx, DRAFT_16) == EndpointAbi.OK, "setup: the transport-draft precondition")
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalog.toByteArray()) == EndpointAbi.OK, "setup: ingest")
        val tokens = ArrayList<Long>()
        val sc = LongArray(ReceiverEventScalars.COUNT)
        while (poll(root, rx, sc) == EndpointAbi.OK) if (sc[ReceiverEventScalars.KIND] == 1L) tokens += sc[ReceiverEventScalars.TRACK]
        return tokens
    }

    private fun receiverControlSubscribe() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (ta, _) = discoverManual(root, rx)
        check(trackState(root, rx, ta) == 0L, "DISCOVERED before any command")
        check(NativeBridge.nativeTrackSubscribe(root, rx, ta, 1, true, 7) == EndpointAbi.OK, "subscribe(NEXT_GROUP, priority 7) is validated and recorded")
        check(trackState(root, rx, ta) == 1L, "PENDING: wanted, not yet accepted (no session ever will)")
        check(NativeBridge.nativeTrackSubscribe(root, rx, ta, 0, false, 0) == EndpointAbi.OK, "a repeat is an idempotent resume")
        check(trackState(root, rx, ta) == 1L, "still PENDING")
        check(NativeBridge.nativeTrackUnsubscribe(root, rx, ta) == EndpointAbi.OK, "unsubscribe")
        check(trackState(root, rx, ta) == 0L, "never issued on the wire: DISCOVERED again, not PAUSED_APP (the C's own rule)")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivercontrolsubscribe")
        println("RESULT ok")
    }

    private fun receiverControlPurge() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (ta, tb) = discoverManual(root, rx)
        check(NativeBridge.nativeTrackSubscribe(root, rx, ta, 0, false, 0) == EndpointAbi.OK && NativeBridge.nativeTrackSubscribe(root, rx, tb, 0, false, 0) == EndpointAbi.OK, "both wanted (the real desire, set by the real command)")
        check(TrustRouteProbe.nativeIngestObject(root, rx, ta, 0L, 0L, 0, false, 0L, "v1".toByteArray()) == EndpointAbi.OK, "feed v1")
        check(TrustRouteProbe.nativeIngestObject(root, rx, tb, 0L, 0L, 0, false, 0L, "a1".toByteArray()) == EndpointAbi.OK, "feed a1")
        check(TrustRouteProbe.nativeIngestObject(root, rx, ta, 0L, 1L, 0, false, 0L, "v2".toByteArray()) == EndpointAbi.OK, "feed v2")
        check(NativeBridge.nativeTrackUnsubscribe(root, rx, ta) == EndpointAbi.OK, "unsubscribe(v) purges v's queued objects")
        val os = LongArray(ReceiverObjectScalars.COUNT)
        val bytes = pollObject(root, rx, os)
        check(bytes != null && String(bytes!![ReceiverObjectBytes.PAYLOAD]) == "a1" && os[ReceiverObjectScalars.TRACK] == tb, "the audio object survives the purge")
        check(pollObject(root, rx, os) == null && os[ReceiverObjectScalars.RESULT] == EndpointAbi.DONE.toLong(), "and nothing of v remains")
        val ds = LongArray(TrackDescribeScalars.COUNT)
        check(NativeBridge.nativeTrackDescribe(root, rx, ta, ds) != null, "the catalog handle stays valid after unsubscribe")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivercontrolpurge")
        println("RESULT ok")
    }

    private fun receiverControlEnded() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (_, tb) = discoverManual(root, rx)
        check(NativeBridge.nativeTrackSubscribe(root, rx, tb, 0, false, 0) == EndpointAbi.OK, "wanted")
        check(TrustRouteProbe.nativeIngestObject(root, rx, tb, 0L, 0L, 2, false, 0L, ByteArray(0)) == EndpointAbi.OK, "feed END_OF_TRACK for a: the real intake ends the track")
        val sc = LongArray(ReceiverEventScalars.COUNT)
        check(poll(root, rx, sc) == EndpointAbi.OK && sc[ReceiverEventScalars.KIND] == 4L && sc[ReceiverEventScalars.TRACK] == tb, "TRACK_ENDED for a")
        check(NativeBridge.nativeTrackSubscribe(root, rx, tb, 0, false, 0) == EndpointAbi.WRONG_STATE, "an ended track cannot be (re)subscribed: WRONG_STATE, exactly")
        check(trackState(root, rx, tb) == 5L, "ENDED")
        check(NativeBridge.nativeTrackUnsubscribe(root, rx, tb) == EndpointAbi.OK, "unsubscribe of an ended (not removed) track is the C's OK")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivercontrolended")
        println("RESULT ok")
    }

    private fun receiverControlRemoved() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (ta, tb) = discoverManual(root, rx)
        check(TrustRouteProbe.nativeIngestCatalogAt(root, rx, 1L, 0L, catalogOnlyA.toByteArray()) == EndpointAbi.OK, "generation 2: a later group, without b")
        val sc = LongArray(ReceiverEventScalars.COUNT)
        var removed = false
        while (poll(root, rx, sc) == EndpointAbi.OK) if (sc[ReceiverEventScalars.KIND] == 3L && sc[ReceiverEventScalars.TRACK] == tb) removed = true
        check(removed, "TRACK_REMOVED for b")
        val out = longArrayOf(EndpointAbi.SENTINEL)
        check(NativeBridge.nativeTrackSubscribe(root, rx, tb, 0, false, 0) == EndpointAbi.WRONG_STATE, "subscribe(removed) -> WRONG_STATE")
        check(NativeBridge.nativeTrackUnsubscribe(root, rx, tb) == EndpointAbi.WRONG_STATE, "unsubscribe(removed) -> WRONG_STATE (unlike an ended track)")
        check(NativeBridge.nativeTrackState(root, rx, tb, out) == EndpointAbi.WRONG_STATE && out[0] == EndpointAbi.SENTINEL, "state(removed) -> WRONG_STATE, the block untouched")
        check(trackState(root, rx, ta) == 0L && NativeBridge.nativeTrackSubscribe(root, rx, ta, 0, false, 0) == EndpointAbi.OK, "a survives the generation and subscribes")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivercontrolremoved")
        println("RESULT ok")
    }

    private val catalogOnlyA = """{"version":"1","tracks":[{"name":"a","packaging":"loc","isLive":true,"role":"video"}]}"""

    private fun receiverControlNamespace() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val tokens = discoverManual(root, rx, catalogOverride)
        check(tokens.size == 1, "one track discovered")
        check(NativeBridge.nativeTrackSubscribe(root, rx, tokens[0], 0, false, 0) == EndpointAbi.ERR_UNSUPPORTED, "a namespace-override track: MOQ_ERR_UNSUPPORTED, exactly")
        check(trackState(root, rx, tokens[0]) == 0L, "and it stays DISCOVERED")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivercontrolnamespace")
        println("RESULT ok")
    }

    private fun receiverControlMalformed() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (ta, _) = discoverManual(root, rx)
        val other = liveRoot()
        val rxOther = attachDiscovery(other)
        check(NativeBridge.nativeTrackSubscribe(root, rx, ta, 2, false, 0) == EndpointAbi.INVAL, "start ordinal 2 is not a C start mode")
        check(NativeBridge.nativeTrackSubscribe(root, rx, ta, -1, false, 0) == EndpointAbi.INVAL, "nor is -1")
        check(NativeBridge.nativeTrackSubscribe(root, rx, ta, 0, true, 256) == EndpointAbi.INVAL && NativeBridge.nativeTrackSubscribe(root, rx, ta, 0, true, -1) == EndpointAbi.INVAL, "a priority outside 0..255 cannot cross as uint8")
        check(trackState(root, rx, ta) == 0L, "nothing above reached the receiver: still DISCOVERED")
        check(NativeBridge.nativeTrackState(root, rx, ta, null) == EndpointAbi.INVAL, "null state block")
        val short = LongArray(0)
        check(NativeBridge.nativeTrackState(root, rx, ta, short) == EndpointAbi.INVAL, "short state block")
        check(NativeBridge.nativeTrackSubscribe(other, rxOther, ta, 0, false, 0) == EndpointAbi.REG_OWNER && NativeBridge.nativeTrackUnsubscribe(other, rxOther, ta) == EndpointAbi.REG_OWNER && NativeBridge.nativeTrackState(other, rxOther, ta, LongArray(1)) == EndpointAbi.REG_OWNER, "a track under another receiver: the registry's OWNER, never the C's INVAL")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK, "close")
        check(NativeBridge.nativeTrackSubscribe(root, rx, ta, 0, false, 0) == EndpointAbi.REG_STALE && NativeBridge.nativeTrackUnsubscribe(root, rx, ta) == EndpointAbi.REG_STALE && NativeBridge.nativeTrackState(root, rx, ta, LongArray(1)) == EndpointAbi.REG_STALE, "after close: STALE for all three")
        check(NativeBridge.nativeTrackState(root, rx, ta, null) == EndpointAbi.INVAL && NativeBridge.nativeTrackSubscribe(root, rx, ta, 7, false, 0) == EndpointAbi.INVAL, "a malformed carrier is refused BEFORE acquisition: INVAL even on a stale token, never STALE")
        check(NativeBridge.nativeReceiverClose(other, rxOther) == EndpointAbi.OK, "close other")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(other) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(other) == EndpointAbi.OK, "cleanup")
        noFault("receivercontrolmalformed")
        println("RESULT ok")
    }

    private val catalogOverride = """{"version":"1","tracks":[{"name":"x","namespace":"elsewhere","packaging":"loc","isLive":true,"role":"video"}]}"""

    // -- SAP and media-timeline records through the real receiver ---------------

    private val catalogTimelines = """{"version":1,"generatedAt":1,"tracks":[{"name":"v","packaging":"loc","isLive":true,"role":"video","codec":"av01","timescale":1000000},{"name":"sap","packaging":"eventtimeline","isLive":true,"eventType":"org.ietf.moq.cmsf.sap","mimeType":"application/json","depends":["v"]},{"name":"history","packaging":"mediatimeline","isLive":true,"mimeType":"application/json","depends":["v"]}]}"""

    private fun sapJson(first: Int, count: Int): ByteArray = (first until first + count).joinToString(",", "[", "]") { """{"l":[7,$it],"data":[2,${it * 10}]}""" }.toByteArray()
    private fun mtJson(first: Int, count: Int): ByteArray = (first until first + count).joinToString(",", "[", "]") { "[${it * 40},[7,$it],${1_700_000_000_000L + it}]" }.toByteArray()

    /** Discovery, both timeline tracks subscribed through the real command; returns (sap token, history token). */
    private fun discoverTimelines(root: Long, rx: Long): Pair<Long, Long> {
        check(TrustRouteProbe.nativeSetTransportVersion(root, rx, DRAFT_16) == EndpointAbi.OK, "setup: draft")
        check(TrustRouteProbe.nativeIngestCatalog(root, rx, catalogTimelines.toByteArray()) == EndpointAbi.OK, "setup: ingest")
        var sap = 0L; var history = 0L
        val sc = LongArray(ReceiverEventScalars.COUNT)
        val ds = LongArray(TrackDescribeScalars.COUNT)
        while (poll(root, rx, sc) == EndpointAbi.OK) {
            if (sc[ReceiverEventScalars.KIND] != 1L) continue
            val bytes = NativeBridge.nativeTrackDescribe(root, rx, sc[ReceiverEventScalars.TRACK], ds)!!
            when (String(bytes[TrackDescribeBytes.NAME])) { "sap" -> sap = sc[ReceiverEventScalars.TRACK]; "history" -> history = sc[ReceiverEventScalars.TRACK] }
        }
        check(sap != 0L && history != 0L, "setup: both timeline tracks discovered")
        check(NativeBridge.nativeTrackSubscribe(root, rx, sap, 0, false, 0) == EndpointAbi.OK && NativeBridge.nativeTrackSubscribe(root, rx, history, 0, false, 0) == EndpointAbi.OK, "setup: wanted")
        return sap to history
    }

    private fun pollSap(root: Long, rx: Long, sc: LongArray?): Int = NativeBridge.nativeReceiverPollSap(root, rx, sc)
    private fun pollMt(root: Long, rx: Long, sc: LongArray?): Int = NativeBridge.nativeReceiverPollMediaTimeline(root, rx, sc)

    private fun receiverTimelineSap() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (sap, _) = discoverTimelines(root, rx)
        val sc = LongArray(ReceiverSapScalars.COUNT)
        check(pollSap(root, rx, sc) == EndpointAbi.DONE && sc[ReceiverSapScalars.DEFERRALS] == 0L, "nothing yet, counter 0")
        check(TrustRouteProbe.nativeIngestObject(root, rx, sap, 0L, 0L, 0, false, 0L, """[{"l":[7,0],"data":[2,0]},{"l":[7,1],"data":[3,2100]}]""".toByteArray()) == EndpointAbi.OK, "feed")
        check(pollSap(root, rx, sc) == EndpointAbi.OK && sc[ReceiverSapScalars.TRACK] == sap && sc[ReceiverSapScalars.GROUP] == 7L && sc[ReceiverSapScalars.OBJECT] == 0L && sc[ReceiverSapScalars.SAP_TYPE] == 2L && sc[ReceiverSapScalars.EPT_MS] == 0L && sc[ReceiverSapScalars.DEFERRALS] == 0L, "record 0: ${sc.toList()}")
        check(pollSap(root, rx, sc) == EndpointAbi.OK && sc[ReceiverSapScalars.OBJECT] == 1L && sc[ReceiverSapScalars.SAP_TYPE] == 3L && sc[ReceiverSapScalars.EPT_MS] == 2100L, "record 1: ${sc.toList()}")
        check(pollSap(root, rx, sc) == EndpointAbi.DONE, "then DONE")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivertimelinesap")
        println("RESULT ok")
    }

    private fun receiverTimelineMt() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (_, history) = discoverTimelines(root, rx)
        check(TrustRouteProbe.nativeIngestObject(root, rx, history, 0L, 0L, 0, false, 0L, "[[0,[7,0],0],[40,[7,1],1700000000001]]".toByteArray()) == EndpointAbi.OK, "feed")
        val sc = LongArray(ReceiverTimelineScalars.COUNT)
        check(pollMt(root, rx, sc) == EndpointAbi.OK && sc[ReceiverTimelineScalars.TRACK] == history && sc[ReceiverTimelineScalars.MEDIA_TIME_MS] == 0L && sc[ReceiverTimelineScalars.GROUP] == 7L && sc[ReceiverTimelineScalars.OBJECT] == 0L && sc[ReceiverTimelineScalars.WALLCLOCK_MS] == 0L && sc[ReceiverTimelineScalars.DEFERRALS] == 0L, "record 0: ${sc.toList()}")
        check(pollMt(root, rx, sc) == EndpointAbi.OK && sc[ReceiverTimelineScalars.MEDIA_TIME_MS] == 40L && sc[ReceiverTimelineScalars.OBJECT] == 1L && sc[ReceiverTimelineScalars.WALLCLOCK_MS] == 1_700_000_000_001L, "record 1: ${sc.toList()}")
        check(pollMt(root, rx, sc) == EndpointAbi.DONE && sc[ReceiverTimelineScalars.DEFERRALS] == 0L, "DONE, counter 0")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivertimelinemt")
        println("RESULT ok")
    }

    private fun receiverTimelineSapDefer() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (sap, _) = discoverTimelines(root, rx)
        check(TrustRouteProbe.nativeIngestObject(root, rx, sap, 0L, 0L, 0, false, 0L, sapJson(0, 300)) == EndpointAbi.OK, "feed 300 in one independent object")
        val sc = LongArray(ReceiverSapScalars.COUNT)
        for (i in 0 until 256) {
            check(pollSap(root, rx, sc) == EndpointAbi.OK && sc[ReceiverSapScalars.OBJECT] == i.toLong() && sc[ReceiverSapScalars.DEFERRALS] == 44L, "prefix record $i with the counter in the same block: ${sc.toList()}")
        }
        check(pollSap(root, rx, sc) == EndpointAbi.DONE && sc[ReceiverSapScalars.DEFERRALS] == 44L, "empty, and the block still says 44 deferred")
        check(TrustRouteProbe.nativeIngestObject(root, rx, sap, 1L, 0L, 0, false, 0L, sapJson(0, 300)) == EndpointAbi.OK, "the next independent object carries the history")
        for (i in 256 until 300) {
            check(pollSap(root, rx, sc) == EndpointAbi.OK && sc[ReceiverSapScalars.OBJECT] == i.toLong() && sc[ReceiverSapScalars.DEFERRALS] == 44L, "tail record $i: ${sc.toList()}")
        }
        check(pollSap(root, rx, sc) == EndpointAbi.DONE && sc[ReceiverSapScalars.DEFERRALS] == 44L, "the tail delivered; nothing more deferred")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivertimelinesapdefer")
        println("RESULT ok")
    }

    private fun receiverTimelineMtDefer() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (_, history) = discoverTimelines(root, rx)
        check(TrustRouteProbe.nativeIngestObject(root, rx, history, 0L, 0L, 0, false, 0L, mtJson(0, 300)) == EndpointAbi.OK, "feed 300")
        val sc = LongArray(ReceiverTimelineScalars.COUNT)
        for (i in 0 until 256) check(pollMt(root, rx, sc) == EndpointAbi.OK && sc[ReceiverTimelineScalars.OBJECT] == i.toLong() && sc[ReceiverTimelineScalars.DEFERRALS] == 44L, "prefix $i: ${sc.toList()}")
        check(pollMt(root, rx, sc) == EndpointAbi.DONE && sc[ReceiverTimelineScalars.DEFERRALS] == 44L, "empty with 44 deferred")
        val ss = LongArray(ReceiverSapScalars.COUNT)
        check(pollSap(root, rx, ss) == EndpointAbi.DONE && ss[ReceiverSapScalars.DEFERRALS] == 0L, "the sap counter is untouched")
        check(TrustRouteProbe.nativeIngestObject(root, rx, history, 1L, 0L, 0, false, 0L, mtJson(0, 300)) == EndpointAbi.OK, "re-send")
        for (i in 256 until 300) check(pollMt(root, rx, sc) == EndpointAbi.OK && sc[ReceiverTimelineScalars.OBJECT] == i.toLong(), "tail $i")
        check(pollMt(root, rx, sc) == EndpointAbi.DONE && sc[ReceiverTimelineScalars.DEFERRALS] == 44L, "done")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receivertimelinemtdefer")
        println("RESULT ok")
    }

    private fun receiverTimelineMalformed() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val other = liveRoot()
        val (sap, _) = discoverTimelines(root, rx)
        check(TrustRouteProbe.nativeIngestObject(root, rx, sap, 0L, 0L, 0, false, 0L, sapJson(0, 1)) == EndpointAbi.OK, "one queued record")
        check(pollSap(root, rx, null) == EndpointAbi.INVAL && pollMt(root, rx, null) == EndpointAbi.INVAL, "null blocks")
        val short = LongArray(ReceiverSapScalars.COUNT - 1) { EndpointAbi.SENTINEL }
        check(pollSap(root, rx, short) == EndpointAbi.INVAL && short.all { it == EndpointAbi.SENTINEL }, "short sap block: refused, untouched")
        val shortMt = LongArray(ReceiverTimelineScalars.COUNT - 1) { EndpointAbi.SENTINEL }
        check(pollMt(root, rx, shortMt) == EndpointAbi.INVAL && shortMt.all { it == EndpointAbi.SENTINEL }, "short timeline block")
        val sc = LongArray(ReceiverSapScalars.COUNT)
        check(pollSap(other, rx, sc) == EndpointAbi.REG_OWNER && pollMt(other, rx, LongArray(ReceiverTimelineScalars.COUNT)) == EndpointAbi.REG_OWNER, "wrong root")
        check(pollSap(root, rx, sc) == EndpointAbi.OK && sc[ReceiverSapScalars.OBJECT] == 0L, "the malformed calls consumed nothing")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK, "close")
        check(pollSap(root, rx, sc) == EndpointAbi.REG_STALE && pollMt(root, rx, LongArray(ReceiverTimelineScalars.COUNT)) == EndpointAbi.REG_STALE, "stale after close")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(other) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(other) == EndpointAbi.OK, "cleanup")
        noFault("receivertimelinemalformed")
        println("RESULT ok")
    }

    // -- the stats snapshot through the real receiver -------------------------

    private fun stats(root: Long, rx: Long): LongArray {
        val sc = LongArray(ReceiverStatsScalars.COUNT) { EndpointAbi.SENTINEL }
        val rc = NativeBridge.nativeReceiverStats(root, rx, sc)
        check(rc == EndpointAbi.OK, "stats answers OK; got $rc")
        check(sc.none { it == EndpointAbi.SENTINEL }, "every slot written exactly once: ${sc.toList()}")
        return sc
    }

    private fun receiverStats() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val (sap, _) = discoverTimelines(root, rx)
        // The catalog's first track is the video track; it is wanted through the explicit precondition by handle.
        check(TrustRouteProbe.nativeMarkTrackDesired(root, rx, TrustRouteProbe.FIRST_TRACK) == EndpointAbi.OK, "setup: the video track (first handle) wanted")
        val rest = stats(root, rx)
        check(rest.all { it == 0L }, "at rest every field is 0: ${rest.toList()}")
        check(TrustRouteProbe.nativeIngestObject(root, rx, TrustRouteProbe.FIRST_TRACK, 0L, 0L, 0, false, 0L, "abcd".toByteArray()) == EndpointAbi.OK, "feed 4 bytes")
        check(TrustRouteProbe.nativeIngestObject(root, rx, TrustRouteProbe.FIRST_TRACK, 0L, 1L, 0, false, 0L, "efghij".toByteArray()) == EndpointAbi.OK, "feed 6 bytes")
        var s = stats(root, rx)
        check(s[ReceiverStatsScalars.OBJECTS_RECEIVED] == 2L && s[ReceiverStatsScalars.OBJECTS_QUEUED] == 2L && s[ReceiverStatsScalars.BYTES_QUEUED] == 10L, "received 2, queued 2, 10 bytes: ${s.toList()}")
        val os = LongArray(ReceiverObjectScalars.COUNT)
        check(NativeBridge.nativeReceiverPollObject(root, rx, os) != null, "poll one")
        s = stats(root, rx)
        check(s[ReceiverStatsScalars.OBJECTS_QUEUED] == 1L && s[ReceiverStatsScalars.BYTES_QUEUED] == 6L && s[ReceiverStatsScalars.OBJECTS_RECEIVED] == 2L, "queued 1 after a pull; received stays 2: ${s.toList()}")
        val video = os[ReceiverObjectScalars.TRACK]
        check(NativeBridge.nativeTrackSubscribe(root, rx, video, 0, false, 0) == EndpointAbi.OK, "wanted through the real command too")
        check(NativeBridge.nativeTrackUnsubscribe(root, rx, video) == EndpointAbi.OK, "unsubscribe purges the queued object")
        s = stats(root, rx)
        // The one remaining queued object is purged: exactly one drop, its 6 bytes released; a RAW/LOC object fed with no
        // frame-marking header is not a keyframe, so the keyframe counter stays 0; received stays 2.
        check(s[ReceiverStatsScalars.OBJECTS_QUEUED] == 0L && s[ReceiverStatsScalars.BYTES_QUEUED] == 0L && s[ReceiverStatsScalars.OBJECTS_DROPPED] == 1L && s[ReceiverStatsScalars.KEYFRAMES_DROPPED] == 0L && s[ReceiverStatsScalars.GROUPS_DROPPED] == 0L && s[ReceiverStatsScalars.OBJECTS_RECEIVED] == 2L, "purged exactly: ${s.toList()}")
        check(TrustRouteProbe.nativeIngestObject(root, rx, sap, 0L, 0L, 0, false, 0L, sapJson(0, 300)) == EndpointAbi.OK, "300 SAP records")
        s = stats(root, rx)
        check(s[ReceiverStatsScalars.SAP_RECORD_DEFERRALS] == 44L && s[ReceiverStatsScalars.MEDIA_TIMELINE_RECORD_DEFERRALS] == 0L, "sap deferrals 44, timeline 0: ${s.toList()}")
        check(s[ReceiverStatsScalars.PAUSED] == 0L && s[ReceiverStatsScalars.CATALOG_COMPLETE] == 0L && s[ReceiverStatsScalars.PARSE_DROPS] == 0L, "not paused, not complete, no parse drop")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverstats")
        println("RESULT ok")
    }

    private fun receiverStatsComplete() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        discoverManual(root, rx)
        check(stats(root, rx)[ReceiverStatsScalars.CATALOG_COMPLETE] == 0L, "not complete yet")
        check(TrustRouteProbe.nativeIngestCatalogAt(root, rx, 1L, 0L, """{"version":"1","isComplete":true,"tracks":[]}""".toByteArray()) == EndpointAbi.OK, "generation 2: isComplete, no tracks")
        var s = stats(root, rx)
        check(s[ReceiverStatsScalars.CATALOG_COMPLETE] == 1L && s[ReceiverStatsScalars.CATALOG_DROPS] == 0L, "complete latched, no drop: ${s.toList()}")
        check(!(NativeBridge.nativeReceiverPollTrack(root, rx, LongArray(ReceiverEventScalars.COUNT)) == EndpointAbi.CLOSED), "the receiver is not terminal")
        check(TrustRouteProbe.nativeIngestCatalogAt(root, rx, 2L, 0L, catalogAB.toByteArray()) == EndpointAbi.OK, "generation 3 after completion")
        s = stats(root, rx)
        check(s[ReceiverStatsScalars.CATALOG_COMPLETE] == 1L && s[ReceiverStatsScalars.CATALOG_DROPS] == 1L, "still complete; the one later catalog object is exactly one counted drop: ${s.toList()}")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        noFault("receiverstatscomplete")
        println("RESULT ok")
    }

    private fun receiverStatsMalformed() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val root = liveRoot()
        val rx = attachDiscovery(root)
        val other = liveRoot()
        check(NativeBridge.nativeReceiverStats(root, rx, null) == EndpointAbi.INVAL, "null block")
        val short = LongArray(ReceiverStatsScalars.COUNT - 1) { EndpointAbi.SENTINEL }
        check(NativeBridge.nativeReceiverStats(root, rx, short) == EndpointAbi.INVAL && short.all { it == EndpointAbi.SENTINEL }, "short block: refused, untouched")
        check(NativeBridge.nativeReceiverStats(other, rx, LongArray(ReceiverStatsScalars.COUNT)) == EndpointAbi.REG_OWNER, "wrong root")
        check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK, "close")
        check(NativeBridge.nativeReceiverStats(root, rx, LongArray(ReceiverStatsScalars.COUNT)) == EndpointAbi.REG_STALE, "stale after close")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(other) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(other) == EndpointAbi.OK, "cleanup")
        noFault("receiverstatsmalformed")
        println("RESULT ok")
    }

    // -- the sender rows -----------------------------------------------------------

    /** The two declared payloads the allocation-failure rows feed and identify. */
    private const val FIRST_PAYLOAD = "first-object"
    private const val SECOND_PAYLOAD = "second-object"

    private fun senderScalars(backpressure: Long = 4L, preReadyMaxObjects: Long = 256L): LongArray =
        longArrayOf(backpressure, 256L, 32L * 1024L * 1024L, preReadyMaxObjects, 32L * 1024L * 1024L, 1L, 0L, 0L, 0L)

    private fun attachSender(root: Long, out: LongArray, parts: Array<ByteArray>? = ns, catalog: ByteArray? = null, scalars: LongArray? = senderScalars()): Int =
        NativeBridge.nativeSenderAttach(root, parts, catalog, scalars, out)

    private fun videoBytes(name: String): Array<ByteArray?> = arrayOf(name.toByteArray(), "avc1.64001f".toByteArray(), null, null, null, null)

    private fun videoScalars(): LongArray {
        val sc = LongArray(SendTrackScalars.COUNT)
        sc[SendTrackScalars.MEDIA_TYPE] = 1L
        sc[SendTrackScalars.PACKAGING] = 1L
        sc[SendTrackScalars.TIMESCALE] = 1_000_000L
        sc[SendTrackScalars.IS_LIVE] = 1L
        sc[SendTrackScalars.BITRATE] = 2_000_000L
        sc[SendTrackScalars.SAP_TIMELINE_HISTORY_GROUPS] = 8L
        sc[SendTrackScalars.MEDIA_TIMELINE_HISTORY_GROUPS] = 8L
        return sc
    }

    private fun addTrack(root: Long, tx: Long, name: String): Long {
        val out = longArrayOf(EndpointAbi.SENTINEL)
        val rc = NativeBridge.nativeSenderAddTrack(root, tx, videoBytes(name), videoScalars(), out)
        check(rc == EndpointAbi.OK, "add track '$name': $rc")
        check(out[0] != EndpointAbi.SENTINEL && EndpointAbi.kindOf(out[0]) == 3L, "a track token is a BORROWED child (kind 3); got kind ${EndpointAbi.kindOf(out[0])}")
        return out[0]
    }

    private fun objectScalars(sync: Boolean, startsGroup: Boolean, pt: Long): LongArray =
        longArrayOf(if (sync) 1L else 0L, if (startsGroup) 1L else 0L, 0L, pt, pt, 0L, 0L, 0L, 0L)

    private fun senderStats(root: Long, tx: Long): LongArray {
        val sc = LongArray(SenderStatsScalars.COUNT) { EndpointAbi.SENTINEL }
        check(NativeBridge.nativeSenderStats(root, tx, sc) == EndpointAbi.OK, "stats")
        check(sc.none { it == EndpointAbi.SENTINEL }, "every stats slot written: ${sc.toList()}")
        return sc
    }

    private fun senderAttach() {
        System.load(BridgeDsos.path("moq5_jni"))
        val root = liveRoot()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        check(attachSender(root, out) == EndpointAbi.OK, "attach a sender on a live root")
        val tx = out[0]
        check(tx != EndpointAbi.SENTINEL && tx != 0L && EndpointAbi.kindOf(tx) == 2L && tx < 0L, "an OWNED child token, full width; got ${EndpointAbi.kindOf(tx)}")
        val gated = NativeBridge.nativeEndpointStop(root)
        check(gated == EndpointAbi.WRONG_STATE, "the C stop refuses while a sender is attached; got $gated")
        out[0] = EndpointAbi.SENTINEL
        val second = attachSender(root, out)
        check(second == EndpointAbi.WRONG_STATE && out[0] == EndpointAbi.SENTINEL, "one sender per endpoint: WRONG_STATE, nothing published; got $second")
        val rxOut = longArrayOf(EndpointAbi.SENTINEL)
        check(attach(root, rxOut) == EndpointAbi.OK, "a receiver attaches beside the sender")
        check(NativeBridge.nativeReceiverClose(root, rxOut[0]) == EndpointAbi.OK, "close the receiver")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.WRONG_STATE, "still gated by the sender")
        check(NativeBridge.nativeSenderClose(root, tx) == EndpointAbi.OK, "close retires the sender child")
        check(NativeBridge.nativeSenderClose(root, tx) == EndpointAbi.REG_STALE, "a second close is stale")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK, "stop succeeds once both are gone")
        check(NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "destroy")
        println("RESULT ok")
    }

    private fun senderTracks() {
        System.load(BridgeDsos.path("moq5_jni"))
        val root = liveRoot()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        check(attachSender(root, out) == EndpointAbi.OK, "attach")
        val tx = out[0]
        val probe = LongArray(SenderProbeScalars.COUNT) { EndpointAbi.SENTINEL }
        check(NativeBridge.nativeSenderProbe(root, tx, probe) == EndpointAbi.OK, "probe")
        check(probe.toList() == listOf(0L, 0L, 0L, 0L, 0L), "pre-ready, not closed, not fatal, no demand: ${probe.toList()}")
        var s = senderStats(root, tx)
        check(s.all { it == 0L }, "at rest every stat is 0: ${s.toList()}")
        val v = addTrack(root, tx, "v")
        check(NativeBridge.nativeSenderWrite(root, tx, v, byteArrayOf(1, 2, 3), null, objectScalars(sync = true, startsGroup = true, pt = 1_000L)) == EndpointAbi.OK, "a keyframe opens the group: accepted pre-ready")
        check(NativeBridge.nativeSenderWrite(root, tx, v, byteArrayOf(4, 5, 6, 7), null, objectScalars(sync = false, startsGroup = false, pt = 2_000L)) == EndpointAbi.OK, "a delta continues it")
        s = senderStats(root, tx)
        check(s[SenderStatsScalars.OBJECTS_WRITTEN] == 2L && s[SenderStatsScalars.OBJECTS_QUEUED] == 2L && s[SenderStatsScalars.BYTES_QUEUED] == 7L && s[SenderStatsScalars.OBJECTS_SENT] == 0L && s[SenderStatsScalars.LAST_ERROR] == 0L, "written 2, queued 2, bytes 7, sent 0: ${s.toList()}")
        check(NativeBridge.nativeSenderEndTrack(root, tx, v) == EndpointAbi.OK, "end_track queues the marker")
        check(NativeBridge.nativeSenderEndTrack(root, tx, v) == EndpointAbi.OK, "end_track is idempotent")
        check(NativeBridge.nativeSenderWrite(root, tx, v, byteArrayOf(9), null, objectScalars(sync = true, startsGroup = true, pt = 3_000L)) == EndpointAbi.WRONG_STATE, "a write after end is WRONG_STATE")
        s = senderStats(root, tx)
        check(s[SenderStatsScalars.OBJECTS_QUEUED] == 2L, "the marker is not media: queued stays 2: ${s.toList()}")
        val w = addTrack(root, tx, "w")
        val demand = longArrayOf(EndpointAbi.SENTINEL)
        check(NativeBridge.nativeSenderTrackDemand(root, tx, w, demand) == EndpointAbi.OK && demand[0] == 0L, "no subscriber: demand 0")
        check(NativeBridge.nativeSenderRemoveTrack(root, tx, w) == EndpointAbi.OK, "remove")
        check(NativeBridge.nativeSenderRemoveTrack(root, tx, w) == EndpointAbi.WRONG_STATE, "a second remove is WRONG_STATE")
        check(NativeBridge.nativeSenderWrite(root, tx, w, byteArrayOf(9), null, objectScalars(sync = true, startsGroup = true, pt = 1L)) == EndpointAbi.WRONG_STATE, "a write to a removed track is WRONG_STATE")
        check(NativeBridge.nativeSenderComplete(root, tx) == EndpointAbi.WRONG_STATE, "complete pre-ready is WRONG_STATE")
        val x = addTrack(root, tx, "x")
        check(NativeBridge.nativeSenderConvertToVod(root, tx, longArrayOf(x), longArrayOf(90_000L)) == EndpointAbi.WRONG_STATE, "convert_to_vod pre-ready is WRONG_STATE")
        check(NativeBridge.nativeSenderClose(root, tx) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        println("RESULT ok")
    }

    private fun senderWouldBlock() {
        System.load(BridgeDsos.path("moq5_jni"))
        val root = liveRoot()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        check(attachSender(root, out, scalars = senderScalars(preReadyMaxObjects = 1L)) == EndpointAbi.OK, "attach with a pre-ready bound of one object")
        val tx = out[0]
        val v = addTrack(root, tx, "v")
        check(NativeBridge.nativeSenderWrite(root, tx, v, byteArrayOf(1, 2, 3), null, objectScalars(sync = true, startsGroup = true, pt = 1L)) == EndpointAbi.OK, "the first keyframe is accepted")
        val second = NativeBridge.nativeSenderWrite(root, tx, v, byteArrayOf(4), null, objectScalars(sync = false, startsGroup = false, pt = 2L))
        check(second == EndpointAbi.WOULD_BLOCK, "the bound is full: WOULD_BLOCK; got $second")
        var s = senderStats(root, tx)
        check(s[SenderStatsScalars.OBJECTS_WRITTEN] == 1L && s[SenderStatsScalars.BYTES_QUEUED] == 3L && s[SenderStatsScalars.LAST_ERROR] == EndpointAbi.WOULD_BLOCK.toLong(), "nothing transferred; last_error latched WOULD_BLOCK: ${s.toList()}")
        check(NativeBridge.nativeSenderClose(root, tx) == EndpointAbi.OK, "close the bounded sender")
        out[0] = EndpointAbi.SENTINEL
        check(attachSender(root, out) == EndpointAbi.OK, "attach an ordinary sender")
        val tx2 = out[0]
        val w = addTrack(root, tx2, "w")
        check(NativeBridge.nativeSenderWrite(root, tx2, w, byteArrayOf(1), null, objectScalars(sync = false, startsGroup = false, pt = 1L)) == EndpointAbi.WOULD_BLOCK, "a delta with no anchor is WOULD_BLOCK")
        check(NativeBridge.nativeSenderWrite(root, tx2, w, byteArrayOf(1), byteArrayOf(7), objectScalars(sync = true, startsGroup = true, pt = 1L)) == EndpointAbi.INVAL, "a RAW object carrying a property block is the C's INVAL")
        s = senderStats(root, tx2)
        check(s[SenderStatsScalars.OBJECTS_WRITTEN] == 0L && s[SenderStatsScalars.LAST_ERROR] == EndpointAbi.INVAL.toLong(), "nothing transferred; last_error INVAL: ${s.toList()}")
        check(NativeBridge.nativeSenderClose(root, tx2) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup")
        println("RESULT ok")
    }

    private fun senderMalformed() {
        System.load(BridgeDsos.path("moq5_jni"))
        val root = liveRoot()
        val other = liveRoot()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        fun untouched(what: String, expect: Int, block: LongArray?, call: () -> Int) {
            val before = block?.toList()
            val rc = call()
            check(rc == expect, "$what must be $expect; got $rc")
            check(block == null || block.toList() == before, "$what: the block is untouched")
        }
        untouched("attach: null out", EndpointAbi.INVAL, null) { attachSender(root, null as LongArray? ?: LongArray(0)) }
        untouched("attach: null scalars", EndpointAbi.INVAL, out) { attachSender(root, out, scalars = null) }
        untouched("attach: short scalars", EndpointAbi.INVAL, out) { attachSender(root, out, scalars = LongArray(SenderAttachScalars.COUNT - 1)) }
        untouched("attach: BLOCK_TIMEOUT has no spelling", EndpointAbi.INVAL, out) { attachSender(root, out, scalars = senderScalars(backpressure = 3L)) }
        untouched("attach: UNSET", EndpointAbi.INVAL, out) { attachSender(root, out, scalars = senderScalars(backpressure = 0L)) }
        untouched("attach: empty namespace", EndpointAbi.INVAL, out) { attachSender(root, out, parts = arrayOf()) }
        untouched("attach: empty catalog track", EndpointAbi.INVAL, out) { attachSender(root, out, catalog = ByteArray(0)) }
        check(out[0] == EndpointAbi.SENTINEL, "nothing published by a malformed attach")
        check(attachSender(root, out) == EndpointAbi.OK, "attach")
        val tx = out[0]
        val tOut = longArrayOf(EndpointAbi.SENTINEL)
        untouched("add: null bytes", EndpointAbi.INVAL, tOut) { NativeBridge.nativeSenderAddTrack(root, tx, null, videoScalars(), tOut) }
        untouched("add: short scalars", EndpointAbi.INVAL, tOut) { NativeBridge.nativeSenderAddTrack(root, tx, videoBytes("v"), LongArray(SendTrackScalars.COUNT - 1), tOut) }
        untouched("add: null out", EndpointAbi.INVAL, null) { NativeBridge.nativeSenderAddTrack(root, tx, videoBytes("v"), videoScalars(), null) }
        untouched("add: wrong root", EndpointAbi.REG_OWNER, tOut) { NativeBridge.nativeSenderAddTrack(other, tx, videoBytes("v"), videoScalars(), tOut) }
        val v = addTrack(root, tx, "v")
        val probe = LongArray(SenderProbeScalars.COUNT - 1) { EndpointAbi.SENTINEL }
        untouched("probe: short block", EndpointAbi.INVAL, probe) { NativeBridge.nativeSenderProbe(root, tx, probe) }
        untouched("probe: null block", EndpointAbi.INVAL, null) { NativeBridge.nativeSenderProbe(root, tx, null) }
        val stats = LongArray(SenderStatsScalars.COUNT - 1) { EndpointAbi.SENTINEL }
        untouched("stats: short block", EndpointAbi.INVAL, stats) { NativeBridge.nativeSenderStats(root, tx, stats) }
        untouched("write: null payload", EndpointAbi.INVAL, null) { NativeBridge.nativeSenderWrite(root, tx, v, null, null, objectScalars(true, true, 1L)) }
        untouched("write: short scalars", EndpointAbi.INVAL, null) { NativeBridge.nativeSenderWrite(root, tx, v, byteArrayOf(1), null, LongArray(SendObjectScalars.COUNT - 1)) }
        untouched("demand: short out", EndpointAbi.INVAL, null) { NativeBridge.nativeSenderTrackDemand(root, tx, v, LongArray(0)) }
        untouched("write: wrong root", EndpointAbi.REG_OWNER, null) { NativeBridge.nativeSenderWrite(other, tx, v, byteArrayOf(1), null, objectScalars(true, true, 1L)) }
        untouched("vod: unequal arrays", EndpointAbi.INVAL, null) { NativeBridge.nativeSenderConvertToVod(root, tx, longArrayOf(v), LongArray(0)) }
        // A track of ANOTHER sender: the registry's OWNER refusal, before any C call.
        val otherOut = longArrayOf(EndpointAbi.SENTINEL)
        check(attachSender(other, otherOut) == EndpointAbi.OK, "attach on the other root")
        val foreign = addTrack(other, otherOut[0], "f")
        untouched("write: a foreign track", EndpointAbi.REG_OWNER, null) { NativeBridge.nativeSenderWrite(root, tx, foreign, byteArrayOf(1), null, objectScalars(true, true, 1L)) }
        untouched("end: a foreign track", EndpointAbi.REG_OWNER, null) { NativeBridge.nativeSenderEndTrack(root, tx, foreign) }
        untouched("remove: a foreign track", EndpointAbi.REG_OWNER, null) { NativeBridge.nativeSenderRemoveTrack(root, tx, foreign) }
        val s = senderStats(root, tx)
        check(s[SenderStatsScalars.OBJECTS_WRITTEN] == 0L, "no malformed call transferred anything")
        check(NativeBridge.nativeSenderClose(root, tx) == EndpointAbi.OK, "close")
        untouched("stats after close", EndpointAbi.REG_STALE, null) { NativeBridge.nativeSenderStats(root, tx, LongArray(SenderStatsScalars.COUNT)) }
        untouched("write after close: the track token is stale too", EndpointAbi.REG_STALE, null) { NativeBridge.nativeSenderWrite(root, tx, v, byteArrayOf(1), null, objectScalars(true, true, 1L)) }
        check(NativeBridge.nativeSenderClose(other, otherOut[0]) == EndpointAbi.OK, "close the other")
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK && NativeBridge.nativeEndpointStop(other) == EndpointAbi.OK && NativeBridge.nativeEndpointDestroy(other) == EndpointAbi.OK, "cleanup")
        println("RESULT ok")
    }

    /**
     * R9: the registry names LIFETIME roles -- root, owned child, borrowed
     * leaf -- and not the CONCRETE type behind an owned child. A receiver
     * child and a sender child of one root therefore carry the same kind and
     * the same owner, so every consumer that validates only kind and owner
     * accepts either one.
     *
     * Every case is observed at a SAFE boundary. The close rows run the
     * entry's OWN registered cleanup, so a cross-type close destroys the
     * wrong object but never reinterprets a pointer. The acquisition rows are
     * driven with an INVALID track token, so the row refuses at its second
     * acquisition and returns before the mis-typed wrapper is dereferenced --
     * the returned code says which argument was refused. Nothing here casts
     * and dereferences an incompatible object.
     *
     * Failures are collected so both directions and both boundaries are
     * attributable in one run instead of stopping at the first.
     */
    private fun registryCrossType() {
        System.load(BridgeDsos.path("moq5_jni"))
        val problems = mutableListOf<String>()
        fun expect(what: String, want: Int, got: Int) {
            if (got != want) problems += "$what: expected $want, got $got"
        }

        // -- retirement, on its own root -------------------------------------
        val a = liveRoot()
        val other = liveRoot()
        val rxOut = longArrayOf(EndpointAbi.SENTINEL)
        check(attach(a, rxOut) == EndpointAbi.OK, "setup: attach a receiver")
        val rx = rxOut[0]
        val txOut = longArrayOf(EndpointAbi.SENTINEL)
        check(attachSender(a, txOut) == EndpointAbi.OK, "setup: attach a sender beside it")
        val tx = txOut[0]
        check(rx != tx, "setup: two distinct live tokens under one root")
        check(EndpointAbi.kindOf(rx) == 2L && EndpointAbi.kindOf(tx) == 2L,
            "setup: both children carry the SAME generic kind (CHILD_OWNED); only the concrete type differs")
        val track = addTrack(a, tx, "v")

        // Controls: the lifetime and owner checks that already work.
        expect("close control: a borrowed leaf is not an owned child", EndpointAbi.REG_KIND, NativeBridge.nativeSenderClose(a, track))
        expect("close control: the root itself is not an owned child", EndpointAbi.REG_KIND, NativeBridge.nativeSenderClose(a, a))
        expect("close control: a child of another root", EndpointAbi.REG_OWNER, NativeBridge.nativeSenderClose(other, tx))
        expect("close control: a never-issued token", EndpointAbi.REG_INVALID, NativeBridge.nativeSenderClose(a, 0L))

        // The reviewed witness, and its reciprocal.
        expect("close: nativeSenderClose must refuse a RECEIVER token by concrete type",
            EndpointAbi.REG_TYPE, NativeBridge.nativeSenderClose(a, rx))
        expect("close: the receiver is still live after a refused cross-type close",
            EndpointAbi.OK, NativeBridge.nativeReceiverStats(a, rx, LongArray(ReceiverStatsScalars.COUNT)))
        expect("close: nativeReceiverClose must refuse a SENDER token by concrete type",
            EndpointAbi.REG_TYPE, NativeBridge.nativeReceiverClose(a, tx))
        expect("close: the sender is still live after a refused cross-type close",
            EndpointAbi.OK, NativeBridge.nativeSenderStats(a, tx, LongArray(SenderStatsScalars.COUNT)))

        // Cleanup conservation: each child retires exactly once, through its
        // own row, and a repeat is the registry's stale answer.
        expect("close: the receiver retires through its own row", EndpointAbi.OK, NativeBridge.nativeReceiverClose(a, rx))
        expect("close: the sender retires through its own row", EndpointAbi.OK, NativeBridge.nativeSenderClose(a, tx))
        expect("close: a second receiver close is stale", EndpointAbi.REG_STALE, NativeBridge.nativeReceiverClose(a, rx))
        expect("close: a second sender close is stale", EndpointAbi.REG_STALE, NativeBridge.nativeSenderClose(a, tx))
        // The SENDER is stale, so this row never reaches its track acquisition:
        // it is evidence about the retired sender argument, not about the
        // descendant track token. Descendant invalidation is proven directly
        // in the registry oracle's child-retirement row.
        expect("close: a row on the retired sender is refused at the sender", EndpointAbi.REG_STALE, NativeBridge.nativeSenderRemoveTrack(a, tx, track))

        // -- acquisition, on its own root ------------------------------------
        val c = liveRoot()
        val rx2Out = longArrayOf(EndpointAbi.SENTINEL)
        check(attach(c, rx2Out) == EndpointAbi.OK, "setup: attach a receiver on the acquisition root")
        val rx2 = rx2Out[0]
        val tx2Out = longArrayOf(EndpointAbi.SENTINEL)
        check(attachSender(c, tx2Out) == EndpointAbi.OK, "setup: attach a sender on the acquisition root")
        val tx2 = tx2Out[0]
        val track2 = addTrack(c, tx2, "w")

        // Controls: the same rows with the RIGHT type reach their track check.
        expect("acquire control: a sender row with its own sender reaches the track check",
            EndpointAbi.REG_INVALID, NativeBridge.nativeSenderRemoveTrack(c, tx2, 0L))
        expect("acquire control: a sender row under the wrong root",
            EndpointAbi.REG_OWNER, NativeBridge.nativeSenderRemoveTrack(other, tx2, 0L))
        expect("acquire control: a borrowed leaf is not a sender",
            EndpointAbi.REG_KIND, NativeBridge.nativeSenderRemoveTrack(c, track2, 0L))

        // A SENDER row given the RECEIVER token. The invalid track makes the
        // row refuse at its second acquisition, so nothing is dereferenced;
        // the code says whether the sender argument itself was refused.
        expect("acquire: a sender row must refuse a RECEIVER token as its sender, before its track",
            EndpointAbi.REG_TYPE, NativeBridge.nativeSenderRemoveTrack(c, rx2, 0L))

        // The reciprocal boundary: a RECEIVER row given the SENDER token.
        // The describe row reports its refusal in slot 0 ONLY and returns no
        // array. The full 64-bit carrier is compared, not a narrowed int, and
        // every trailing slot must still hold the sentinel: the row writes one
        // slot, and a zero-filled output is not its contract.
        fun describeRefusal(what: String, want: Int, receiverArg: Long) {
            val sc = LongArray(TrackDescribeScalars.COUNT) { EndpointAbi.SENTINEL }
            val arr = NativeBridge.nativeTrackDescribe(c, receiverArg, 0L, sc)
            check(arr == null, "$what: a refused describe returns no array")
            if (sc[0] != want.toLong()) problems += "$what: expected ${want.toLong()}, got ${sc[0]}"
            val tailIndex = (1 until sc.size).firstOrNull { sc[it] != EndpointAbi.SENTINEL }
            if (tailIndex != null) {
                problems += "$what: slot $tailIndex was written (${sc[tailIndex]}); the refusal writes slot 0 only"
            }
        }
        describeRefusal("acquire control: a receiver row with its own receiver reaches the track check",
            EndpointAbi.REG_INVALID, rx2)
        describeRefusal("acquire: a receiver row must refuse a SENDER token as its receiver, before its track",
            EndpointAbi.REG_TYPE, tx2)

        // Both children survive every refused cross-type acquisition.
        expect("acquire: the receiver is still usable afterwards",
            EndpointAbi.OK, NativeBridge.nativeReceiverStats(c, rx2, LongArray(ReceiverStatsScalars.COUNT)))
        expect("acquire: the sender is still usable afterwards",
            EndpointAbi.OK, NativeBridge.nativeSenderStats(c, tx2, LongArray(SenderStatsScalars.COUNT)))
        expect("acquire: the receiver retires through its own row", EndpointAbi.OK, NativeBridge.nativeReceiverClose(c, rx2))
        expect("acquire: the sender retires through its own row", EndpointAbi.OK, NativeBridge.nativeSenderClose(c, tx2))

        for (root in listOf(a, other, c)) {
            check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK, "cleanup: stop (no child may still be attached)")
            check(NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup: destroy")
        }
        check(problems.isEmpty(), "R9 concrete native handle type: " + problems.joinToString(" | "))
        println("RESULT ok")
    }

    /**
     * R12: a refused track-token mint can leave a track the C still holds and
     * no token can address.
     *
     * `nativeSenderAddTrack` adds the track at the C FIRST and mints its token
     * second. When the mint is refused it tries to take the C addition back,
     * and discards what that attempt answered. The two operations do not have
     * the same preconditions: `moq_media_sender_add_track` is not gated on the
     * endpoint's interrupt latch, and `moq_media_sender_remove_track` is
     * (service/src/media_sender.c). So with the latch set, the rollback is
     * refused, the row still reports the registry's refusal and publishes no
     * token, and the track stays in the sender's catalog under a name nothing
     * can address.
     *
     * Every part is separate and real: the insertion refusal is the existing
     * one-shot host seam, the C addition and removal are the real operations,
     * and each consequence is observed on its own. Nothing here fabricates a C
     * success or races a thread.
     */
    private fun senderTrackOrphan() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val problems = mutableListOf<String>()
        fun expect(what: String, want: Int, got: Int) {
            if (got != want) problems += "$what: expected $want, got $got"
        }

        val root = liveRoot()
        val txOut = longArrayOf(EndpointAbi.SENTINEL)
        check(attachSender(root, txOut) == EndpointAbi.OK, "setup: attach a sender")
        val tx = txOut[0]

        // A live track of the matching family, to be unaffected by all of it.
        val keep = addTrack(root, tx, "keep")
        // Every mint this sender makes takes the next slot. A refused insertion
        // that had consumed one would push the following mint past it, so this
        // running expectation is about SLOT SELECTION only: a token can reuse a
        // slot having consumed a generation and still satisfy it. Preservation
        // of the physical generation is not observable from here and is proven
        // directly in the registry oracle's reservation and reuse rows.
        var nextSlot = slotOf(keep) + 1L
        fun add(name: String, out: LongArray): Int =
            NativeBridge.nativeSenderAddTrack(root, tx, videoBytes(name), videoScalars(), out)
        fun mintedOnItsOwnSlot(what: String, token: Long) {
            if (slotOf(token) != nextSlot) {
                problems += "$what: expected slot $nextSlot, got ${slotOf(token)}; a refusal selected a slot"
            }
            nextSlot += 1L
        }

        // -- the control: an ordinary registry refusal -----------------------
        val callsBefore = TrustRouteProbe.nativeAddTrackCalls()
        // Only the insertion is refused here. The row's rollback runs against
        // an uninterrupted endpoint, takes the C addition back, and the name is
        // free again. This is what the defect below is measured against.
        TrustRouteProbe.nativeArmCtorFault(TrustRouteProbe.CHILD_INSERT_REFUSE_ONCE)
        val controlOut = longArrayOf(EndpointAbi.SENTINEL)
        expect("control: the row answers the registry's own refusal",
            EndpointAbi.REG_CAPACITY, add("rollback", controlOut))
        if (controlOut[0] != EndpointAbi.SENTINEL) {
            problems += "control: a refused add published ${controlOut[0]}: the output must be untouched"
        }
        // OBSERVED, not inferred: the product never reached the service call.
        val callsAfterControl = TrustRouteProbe.nativeAddTrackCalls()
        if (callsAfterControl != callsBefore) {
            problems += "a refused reservation still called the C add ${callsAfterControl - callsBefore} time(s)"
        }
        val recoveredOut = longArrayOf(EndpointAbi.SENTINEL)
        val recoveredRc = add("rollback", recoveredOut)
        expect("control: a refused reservation leaves the name free", EndpointAbi.OK, recoveredRc)
        if (recoveredRc == EndpointAbi.OK) mintedOnItsOwnSlot("control: the recovered track", recoveredOut[0])

        // -- the same sequence with the endpoint's interrupt latch set -------
        // The latch is what makes `moq_media_sender_remove_track` refuse, so a
        // row that rolled a C addition back by removing it would strand the
        // track here. This one never adds a track to roll back.
        TrustRouteProbe.nativeArmCtorFault(TrustRouteProbe.CHILD_INSERT_REFUSE_ONCE)
        check(NativeBridge.nativeEndpointSetInterrupted(root, true) == EndpointAbi.OK, "setup: latch the interrupt")
        val orphanOut = longArrayOf(EndpointAbi.SENTINEL)
        expect("the row answers the registry's own refusal",
            EndpointAbi.REG_CAPACITY, add("orphan", orphanOut))
        if (orphanOut[0] != EndpointAbi.SENTINEL) {
            problems += "a refused add published ${orphanOut[0]}: the output must be untouched"
        }
        check(NativeBridge.nativeEndpointSetInterrupted(root, false) == EndpointAbi.OK, "setup: clear the interrupt")

        // -- consequence 1: the registry consumed nothing --------------------
        val afterOut = longArrayOf(EndpointAbi.SENTINEL)
        val afterRc = add("after", afterOut)
        expect("an ordinary add after the refusal succeeds", EndpointAbi.OK, afterRc)
        if (afterRc == EndpointAbi.OK) mintedOnItsOwnSlot("the track added after the refusal", afterOut[0])

        // -- consequence 2: the name is free because no track was added ------
        // The sender refuses a duplicate name for catalog coherence, so this
        // add can only succeed if nothing was ever added under that name.
        val againOut = longArrayOf(EndpointAbi.SENTINEL)
        val againRc = add("orphan", againOut)
        expect("a refused reservation leaves its name free whatever the latch says", EndpointAbi.OK, againRc)
        if (againRc == EndpointAbi.OK) {
            if (EndpointAbi.kindOf(againOut[0]) != 3L) {
                problems += "the recovered track must be a BORROWED child; got kind ${EndpointAbi.kindOf(againOut[0])}"
            }
            mintedOnItsOwnSlot("the recovered track", againOut[0])
        } else if (againOut[0] != EndpointAbi.SENTINEL) {
            problems += "a refused recovery published ${againOut[0]}"
        }

        // -- the live matching-family controls -------------------------------
        val otherOut = longArrayOf(EndpointAbi.SENTINEL)
        expect("a different name was never in question", EndpointAbi.OK, add("other", otherOut))
        expect("the track that was already live is still writable",
            EndpointAbi.OK,
            NativeBridge.nativeSenderWrite(root, tx, keep, byteArrayOf(1, 2, 3), null,
                objectScalars(sync = true, startsGroup = true, pt = 1_000L)))
        if (afterRc == EndpointAbi.OK) {
            expect("and so is the one added after the refusal",
                EndpointAbi.OK,
                NativeBridge.nativeSenderWrite(root, tx, afterOut[0], byteArrayOf(4), null,
                    objectScalars(sync = true, startsGroup = true, pt = 2_000L)))
        }

        // -- ownership conservation on the way out ---------------------------
        expect("the sender retires cleanly", EndpointAbi.OK, NativeBridge.nativeSenderClose(root, tx))
        // The SENDER is stale now, so this row is refused at its first
        // acquisition and never reaches the track. It is evidence about the
        // retired sender argument only; that a retirement invalidates its
        // whole descendant subtree is proven directly in the registry oracle.
        expect("a row on the retired sender is refused at the sender", EndpointAbi.REG_STALE, NativeBridge.nativeSenderRemoveTrack(root, tx, keep))
        expect("the endpoint is not left gated by anything the failure leaked", EndpointAbi.OK, NativeBridge.nativeEndpointStop(root))
        check(NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup: destroy")
        // Exactly the four adds asked for after the counter was read reached
        // the service -- the two recovered names, the one after the refusal and
        // the different name -- and the two refused reservations reached it not
        // at all.
        val calls = TrustRouteProbe.nativeAddTrackCalls() - callsBefore
        if (calls != 4L) problems += "expected 4 real add-track calls, saw $calls"
        noFault("sendertrackorphan")
        check(problems.isEmpty(), "R12 refused track mint: " + problems.joinToString(" | "))
        println("RESULT ok")
    }

    /**
     * The output storage is acquired BEFORE anything is owed.
     *
     * `GetLongArrayElements` may answer NULL, with or without a Java exception
     * pending, and both are answers the specification permits. Because the row
     * takes it before the reservation and before the service call, either one
     * costs nothing: no track is added, no registry entry exists, the caller's
     * array is untouched, and the very next add succeeds.
     *
     * The failure is injected at load, once, from the host image only; the
     * pending-exception arm provokes a GENUINE runtime exception rather than
     * throwing a synthetic one.
     */
    private fun senderOutputAcquire() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val problems = mutableListOf<String>()
        fun expect(what: String, want: Int, got: Int) {
            if (got != want) problems += "$what: expected $want, got $got"
        }
        val pending = System.getenv("MOQ5_JNI_HOST_TEST_FAIL_ACQUIRE") == "fail-acquire-pending-v1"

        val root = liveRoot()
        val txOut = longArrayOf(EndpointAbi.SENTINEL)
        check(attachSender(root, txOut) == EndpointAbi.OK, "setup: attach a sender")
        val tx = txOut[0]
        val callsBefore = TrustRouteProbe.nativeAddTrackCalls()

        val out = longArrayOf(EndpointAbi.SENTINEL)
        var threw: Throwable? = null
        var rc = Int.MIN_VALUE
        try {
            rc = NativeBridge.nativeSenderAddTrack(root, tx, videoBytes("first"), videoScalars(), out)
        } catch (t: Throwable) {
            threw = t
        }
        if (pending) {
            check(threw is ArrayIndexOutOfBoundsException,
                "a pending runtime exception must reach the caller unchanged; saw $threw (rc=$rc)")
        } else {
            check(threw == null, "a bare acquisition failure raises no exception; saw $threw")
            expect("a bare acquisition failure is the allocation code", EndpointAbi.ERR_NOMEM, rc)
        }
        if (out[0] != EndpointAbi.SENTINEL) {
            problems += "a refused acquisition wrote ${out[0]} into the caller's array"
        }
        val callsAfter = TrustRouteProbe.nativeAddTrackCalls()
        if (callsAfter != callsBefore) {
            problems += "a refused acquisition still called the C add ${callsAfter - callsBefore} time(s)"
        }

        // One-shot: the next add is ordinary, and its name was never taken.
        val second = longArrayOf(EndpointAbi.SENTINEL)
        expect("the injection was one-shot", EndpointAbi.OK,
            NativeBridge.nativeSenderAddTrack(root, tx, videoBytes("first"), videoScalars(), second))
        if (EndpointAbi.kindOf(second[0]) != 3L) {
            problems += "the recovered track must be a BORROWED child; got kind ${EndpointAbi.kindOf(second[0])}"
        }
        if (TrustRouteProbe.nativeAddTrackCalls() - callsBefore != 1L) {
            problems += "exactly one real add-track call was expected after the refusal"
        }

        expect("the sender retires cleanly", EndpointAbi.OK, NativeBridge.nativeSenderClose(root, tx))
        check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK, "cleanup: stop")
        check(NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup: destroy")
        check(problems.isEmpty(), "output acquisition: " + problems.joinToString(" | "))
        println("RESULT ok")
    }

    /**
     * A result array the JVM cannot allocate, in both answers it may give.
     *
     * A JNI function that can return NULL and throw is permitted to report
     * failure with NULL ALONE, so the row must tell the two apart. With an
     * exception PENDING it may not call a scalar writer at all, so the whole
     * block is left exactly as the caller handed it over and the exception is
     * the answer. With NOTHING pending the block is the caller's only channel,
     * so the refusal is published there.
     *
     * The failure is injected -- one-shot, armed immediately before the row it
     * is meant for, host image only -- because a healthy VM will not
     * allocate-fail on demand. Everything observed afterwards is real: the
     * exception or its absence, the caller's block, the object the receiver
     * released, and the next call.
     *
     * `atIndex` selects which allocation fails: 0 the result array itself, or
     * the ordinal of a byte field, so a PARTIALLY BUILT result reaches the same
     * cleanup path.
     */
    private fun resultAllocFail(describe: Boolean, pending: Boolean, atIndex: Int) {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val problems = mutableListOf<String>()
        val root = liveRoot()
        var rx = 0L
        try {
            rx = attachDiscovery(root)
            val (sap, _) = discoverTimelines(root, rx)
            check(sap != 0L, "setup: a timeline track")
            check(TrustRouteProbe.nativeMarkTrackDesired(root, rx, TrustRouteProbe.FIRST_TRACK) == EndpointAbi.OK,
                "setup: the video track is wanted")
            // TWO objects with DISTINCT payloads: the refused poll consumes and
            // releases the first, and the retry must deliver the second.
            check(TrustRouteProbe.nativeIngestObject(root, rx, TrustRouteProbe.FIRST_TRACK, 0L, 0L, 0, false, 0L,
                FIRST_PAYLOAD.toByteArray()) == EndpointAbi.OK, "setup: feed the first object")
            check(TrustRouteProbe.nativeIngestObject(root, rx, TrustRouteProbe.FIRST_TRACK, 0L, 1L, 0, false, 0L,
                SECOND_PAYLOAD.toByteArray()) == EndpointAbi.OK, "setup: feed the second object")
            val track = sap

            val cleanupsBefore = TrustRouteProbe.objectCleanups()
            // Armed HERE, after every setup row, so nothing else consumes it.
            TrustRouteProbe.nativeArmAllocFault(pending, atIndex)
            val slots = if (describe) TrackDescribeScalars.COUNT else ReceiverObjectScalars.COUNT
            val scalars = LongArray(slots) { EndpointAbi.SENTINEL }
            var threw: Throwable? = null
            var arr: Array<ByteArray>? = null
            try {
                arr = if (describe) NativeBridge.nativeTrackDescribe(root, rx, track, scalars)
                      else NativeBridge.nativeReceiverPollObject(root, rx, scalars)
            } catch (t: Throwable) {
                threw = t
            }
            if (arr != null) problems += "a refused allocation still returned an array"
            if (TrustRouteProbe.nativeAllocFaultPending()) problems += "the armed allocation failure never fired"

            if (pending) {
                if (threw !is ArrayIndexOutOfBoundsException) {
                    problems += "the pending runtime exception must reach the caller; saw $threw"
                }
                // NOT ONE slot may be written: a writer may not be called then.
                val written = scalars.indices.filter { scalars[it] != EndpointAbi.SENTINEL }
                if (written.isNotEmpty()) problems += "the row wrote scalars $written while an exception was pending"
            } else {
                if (threw != null) problems += "a bare NULL answer raises no exception; saw $threw"
                // The refusal is the caller's only answer, so it is published --
                // in slot 0 exactly, and nowhere else.
                if (scalars[0] != EndpointAbi.ERR_NOMEM.toLong()) {
                    problems += "expected ${EndpointAbi.ERR_NOMEM} in slot 0, got ${scalars[0]}"
                }
                val tail = (1 until slots).firstOrNull { scalars[it] != EndpointAbi.SENTINEL }
                if (tail != null) problems += "slot $tail was written (${scalars[tail]}); the refusal writes slot 0 only"
            }

            // OWNERSHIP, from the recorder rather than from a successful retry:
            // a failed OBJECT poll releases exactly the one object it took, and
            // a failed DESCRIBE takes none at all.
            val delta = TrustRouteProbe.objectCleanups() - cleanupsBefore
            val wanted = if (describe) 0L else 1L
            if (delta != wanted) problems += "expected $wanted object cleanup(s) after the refusal, saw $delta"

            // The receiver survives it, and the next ordinary row works.
            val again = LongArray(slots) { EndpointAbi.SENTINEL }
            val second = if (describe) NativeBridge.nativeTrackDescribe(root, rx, track, again)
                         else NativeBridge.nativeReceiverPollObject(root, rx, again)
            if (second == null) {
                problems += "the injection was not one-shot: the next call also refused (${again[0]})"
            } else if (!describe) {
                // The EXACT declared second object, not merely a non-null array.
                val payload = String(second[0])
                if (payload != SECOND_PAYLOAD) problems += "the retry delivered '$payload', not '$SECOND_PAYLOAD'"
                val afterRetry = TrustRouteProbe.objectCleanups() - cleanupsBefore
                if (afterRetry != wanted + 1L) {
                    problems += "the delivered object must be released too: saw $afterRetry cleanup(s)"
                }
            }
        } finally {
            if (rx != 0L) check(NativeBridge.nativeReceiverClose(root, rx) == EndpointAbi.OK, "cleanup: close the receiver")
            check(NativeBridge.nativeEndpointStop(root) == EndpointAbi.OK, "cleanup: stop")
            check(NativeBridge.nativeEndpointDestroy(root) == EndpointAbi.OK, "cleanup: destroy")
        }
        check(problems.isEmpty(), "pending-exception unwind: " + problems.joinToString(" | "))
        println("RESULT ok")
    }

    /** The largest offerable code reaches the route verbatim; the bridge does not call it malformed. */
    private fun publicOpenWide() {
        val r = openAndRoute(EndpointConfig(EndpointAbi.UNSERVABLE, versions = VersionOffer.Exactly(MoqVersion(Int.MAX_VALUE.toUInt()))))
        check(r.versionPolicy == 2 && r.versions == listOf(Int.MAX_VALUE), "Int.MAX_VALUE must cross positive and verbatim; saw $r")
        noFault("publicopenwide")
        println("RESULT ok")
    }

    /**
     * Every malformed direct call fails closed with MOQ_ERR_INVAL before the
     * registry reserves and before the trust route. Unknown POSITIVE codes are
     * not malformed: they reach the route, whose resolution answers.
     */
    private fun createMalformed() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val before = TrustRouteProbe.counters()
        val out = longArrayOf(EndpointAbi.SENTINEL)
        fun call(policy: Int, versions: IntArray?, what: String) {
            out[0] = EndpointAbi.SENTINEL
            val rc = NativeBridge.nativeEndpointCreateTrusted(EndpointAbi.UNSERVABLE, 0, policy, versions!!, 0L, 0, null, out)
            check(rc == EndpointAbi.INVAL, "$what must be MOQ_ERR_INVAL; got $rc")
            check(out[0] == EndpointAbi.SENTINEL, "$what: a failed create publishes nothing; out[0] was rewritten to ${out[0]}")
        }
        call(1, IntArray(9) { it + 16 }, "nine distinct versions (over the cap of 8)")
        call(1, intArrayOf(16, 18, 16), "a duplicate code")
        call(1, IntArray(0), "LIST with an empty array")
        call(2, intArrayOf(16, 18), "EXACT with two codes")
        call(2, IntArray(0), "EXACT with no code")
        call(0, intArrayOf(16), "AUTO with a list it would ignore")
        call(1, intArrayOf(0), "a zero code")
        call(1, intArrayOf(-16), "a negative code")
        call(3, intArrayOf(16), "an unknown policy ordinal")
        val after = TrustRouteProbe.counters()
        check(after.reserveAttempts == before.reserveAttempts, "refused before any reservation")
        check(TrustRouteProbe.routes.isEmpty(), "and before any route")
        // The null-array boundary: refused, no reservation, no throw.
        out[0] = EndpointAbi.SENTINEL
        val nullRc = runCatching {
            @Suppress("UNCHECKED_CAST")
            (NativeBridge::class.java.getDeclaredMethod("nativeEndpointCreateTrusted", String::class.java, Int::class.javaPrimitiveType, Int::class.javaPrimitiveType, IntArray::class.java, Long::class.javaPrimitiveType, Int::class.javaPrimitiveType, String::class.java, LongArray::class.java)
                .invoke(NativeBridge, EndpointAbi.UNSERVABLE, 0, 1, null, 0L, 0, null, out) as Int)
        }.getOrElse { throw (it as? java.lang.reflect.InvocationTargetException)?.targetException ?: it }
        check(nullRc == EndpointAbi.INVAL, "a null version array must be MOQ_ERR_INVAL; got $nullRc")
        check(out[0] == EndpointAbi.SENTINEL, "a null version array: a failed create publishes nothing")
        check(TrustRouteProbe.counters().reserveAttempts == before.reserveAttempts && TrustRouteProbe.routes.isEmpty())
        // An unknown POSITIVE code is not a bridge error: it reaches the route.
        val unknown = NativeBridge.nativeEndpointCreateTrusted(EndpointAbi.UNSERVABLE, 0, 2, intArrayOf(99), 0L, 0, null, out)
        check(unknown != EndpointAbi.INVAL, "an unknown positive code is the service tier's to answer; got INVAL")
        check(TrustRouteProbe.routes.size == 1 && TrustRouteProbe.routes[0].versions == listOf(99),
            "the unknown code must reach the route verbatim; saw ${TrustRouteProbe.routes}")
        noFault("createmalformed")
        println("RESULT ok")
    }

    /** The row's signed/unsigned boundary, like wait and drain: refused before the registry reserves. */
    private fun createNegativeHandshake() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        val before = TrustRouteProbe.counters()
        val out = longArrayOf(0L)
        val rc = NativeBridge.nativeEndpointCreateTrusted(EndpointAbi.UNSERVABLE, 0, 0, IntArray(0), -1L, 0, null, out)
        check(rc == EndpointAbi.INVAL, "a negative handshake timeout is MOQ_ERR_INVAL; got $rc")
        val after = TrustRouteProbe.counters()
        check(after.reserveAttempts == before.reserveAttempts, "refused before any reservation")
        check(TrustRouteProbe.routes.isEmpty(), "and before any route")
        noFault("createnegativehandshake")
        println("RESULT ok")
    }

    /**
     * A public endpoint opened with the DEFAULT configuration selects the
     * trusted native create path: the recorder must see the trusted route
     * entered with the library delegate and its context, exactly one
     * reservation and one construction, and the native refusal the host build
     * gives (no facade) must surface from `open()` itself as the typed public
     * failure carrying the exact code.
     */
    private fun publicOpen() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        PlatformTrustSupport.sdkInt = { 35 }
        val counts = TrustRouteProbe.counters()
        var failure: EndpointCreateFailedException? = null
        try {
            kotlinx.coroutines.runBlocking { MoqEndpoint.open(EndpointConfig(EndpointAbi.UNSERVABLE)) }
        } catch (e: EndpointCreateFailedException) {
            failure = e
        }
        check(failure != null, "open() on a request this build cannot serve must throw the typed creation failure")
        check(failure!!.nativeCode == EndpointAbi.ERR_UNSUPPORTED,
            "the failure must carry the exact native code ${EndpointAbi.ERR_UNSUPPORTED}; got ${failure.nativeCode}")
        check(TrustRouteProbe.routes.size == 1,
            "the default configuration must reach the TRUSTED route exactly once; saw ${TrustRouteProbe.routes}")
        val r = TrustRouteProbe.routes[0]
        check(r.fnPresent && r.ctxPresent, "the route must receive the library delegate and its context")
        check(r.url == EndpointAbi.UNSERVABLE, "the URL must cross exactly; saw '${r.url}'")
        check(!r.insecure && r.caFileLen == 0, "no insecure flag and no CA file may cross")
        val after = TrustRouteProbe.counters()
        check(after.reserveAttempts - counts.reserveAttempts == 1L &&
            after.constructAttempts - counts.constructAttempts == 1L,
            "exactly one reservation and one construction; reserves=${after.reserveAttempts - counts.reserveAttempts} " +
                "constructs=${after.constructAttempts - counts.constructAttempts}")
        noFault("publicopen")
        println("RESULT ok")
    }

    /**
     * The trusted creation row against a REAL unready bridge: the variant
     * named by `MOQ5_BRIDGE_VARIANT` is one of the built host objects that
     * fail the handshake for a known reason. The row must throw the public
     * typed failure carrying that reason's exact values, and the process-wide
     * owner must remain undecided -- the controlled initializer, not the
     * product owner, made this decision, so the product's packaged-name entry
     * was neither weakened nor consumed.
     */
    private fun createUnready() {
        val variant = System.getenv("MOQ5_BRIDGE_VARIANT")
            ?: error("setup: MOQ5_BRIDGE_VARIANT names the host object to load")
        val initializer = BridgeInitializer()
        var failure: NativeBridgeInitializationException? = null
        try {
            JniEndpointBackend.createTrusted(
                EndpointAbi.UNSERVABLE, 0, NativeEndpointOffer(0, emptyList()), 0L, 0, null,
            ) { initializer.initialize(BridgeDsos.image(variant)) }
        } catch (e: NativeBridgeInitializationException) {
            failure = e
        }
        check(failure != null, "an unready bridge must refuse creation with the typed failure")
        check(NativeBridgeOwner.outcome == null, "the product owner must not have been consulted")
        check(!initializer.isReady, "the controlled initializer must not report ready")
        println("RESULT ok")
        println("FAILURE " + failure!!.failure)
    }

    /**
     * THE FRESH PROCESS. Nothing here loads the bridge: no `System.load`, no
     * explicit handshake, no probe armed before the call. The public open must
     * make the process-wide decision itself and then reach the real native
     * refusal for a request this build cannot serve, with the exact code. A
     * library that works only after something else loaded it is not a product;
     * this is the scenario that says so.
     */
    private fun publicOpenFresh() {
        check(NativeBridgeOwner.outcome == null, "setup: the bridge must be undecided in a fresh process")
        PlatformTrustSupport.sdkInt = { 35 }
        var failure: EndpointCreateFailedException? = null
        try {
            kotlinx.coroutines.runBlocking { MoqEndpoint.open(EndpointConfig(EndpointAbi.UNSERVABLE)) }
        } catch (e: EndpointCreateFailedException) {
            failure = e
        }
        check(failure != null, "a fresh-process open() must reach the native refusal as the typed creation failure")
        check(failure!!.nativeCode == EndpointAbi.ERR_UNSUPPORTED,
            "the failure must carry the exact native code ${EndpointAbi.ERR_UNSUPPORTED}; got ${failure.nativeCode}")
        check(NativeBridgeOwner.isReady, "the public open must have decided the process-wide bridge: ${NativeBridgeOwner.outcome}")
        noFault("publicopenfresh")
        println("RESULT ok")
    }

    /**
     * At API 36 with certificate transparency required for the host, the
     * public open must fail closed BEFORE any native work: no route, no
     * reservation, no construction.
     */
    private fun publicCt() {
        System.load(BridgeDsos.path("moq5_jni"))
        TrustRouteProbe.arm(emptyList())
        PlatformTrustSupport.sdkInt = { 36 }
        PlatformTrustSupport.ctRequired = { true }
        val counts = TrustRouteProbe.counters()
        var refusal: TrustPolicyUnsupportedException? = null
        try {
            kotlinx.coroutines.runBlocking { MoqEndpoint.open(EndpointConfig(EndpointAbi.UNSERVABLE)) }
        } catch (e: TrustPolicyUnsupportedException) {
            refusal = e
        }
        check(refusal != null, "CT required at API 36 must refuse the open with the unsupported-policy cause")
        check(refusal!!.host == "example.invalid", "the refusal must name the host; got '${refusal.host}'")
        check(TrustRouteProbe.routes.isEmpty(), "no route may be entered; saw ${TrustRouteProbe.routes}")
        val after = TrustRouteProbe.counters()
        check(after.reserveAttempts == counts.reserveAttempts && after.constructAttempts == counts.constructAttempts,
            "no reservation and no construction may be attempted before the refusal")
        noFault("publicct")
        println("RESULT ok")
    }
}
