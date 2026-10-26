package org.openmoq.moq5.samples.publisher

/**
 * The runtime permission flow a real capture session needs, as a pure value
 * machine.
 *
 * Nothing here touches an Activity, a dialog or the platform: every platform
 * fact is PASSED IN, exactly as the acquisition boundary already does for the
 * capture clock, so the whole flow -- including the parts that only happen when
 * a user taps "Deny" twice, rotates the screen mid-dialog, or leaves the app
 * while the request is outstanding -- can be exercised on the host.
 *
 * Two decisions are load-bearing:
 *
 *  * A PERMANENT denial is DERIVED, never guessed. One denial says only that
 *    this request was denied; the platform's own "may I still explain myself?"
 *    fact -- `Activity.shouldShowRequestPermissionRationale`, read after the
 *    result -- is what separates a denial this sample will ask again from one
 *    it will not. That fact is an input, not a promise: it is not a guarantee
 *    about whether the system would show a dialog, and this sample's
 *    `PermanentlyDenied` means only that THIS generation stops asking.
 *    Guessing from a single denial would either nag a user who has said never,
 *    or silently stop asking a user who has said not now.
 *  * Every result is stamped with the GENERATION that asked for it. A result
 *    for a retired generation -- the session was stopped, the owner was
 *    destroyed, the dialog was cancelled and the flow restarted -- settles
 *    nothing, because starting capture from it would resurrect a session
 *    nobody is waiting for.
 *
 * None of this is evidence about an actual platform dialog; it is the decision
 * an Activity's `onRequestPermissionsResult` hands here.
 */
internal enum class CapturePermission { Camera, Microphone }

/**
 * What the platform knows after ONE permission result: the grant result itself,
 * and whether the platform will still show a rationale for that permission.
 *
 * `mayExplain` is `Activity.shouldShowRequestPermissionRationale`, read AFTER
 * the result. Read before it, it answers a different question. It is reported
 * as a fact about the rationale, not as a statement that a dialog will or will
 * not be shown.
 */
internal data class PermissionFacts(val granted: Boolean, val mayExplain: Boolean)

/** One permission's place in `NotRequested -> Requested -> Granted | Denied | PermanentlyDenied`. */
internal enum class PermissionState { NotRequested, Requested, Granted, Denied, PermanentlyDenied }

/** What a delivered result did, or why it changed nothing. */
internal sealed interface PermissionOutcome {
    data class Settled(val permission: CapturePermission, val state: PermissionState) : PermissionOutcome

    /** A duplicate, an out-of-order arrival, or a result for a retired generation. */
    data class Ignored(val reason: String) : PermissionOutcome
}

/** Whether capture may start on the permissions settled so far. */
internal sealed interface CaptureReadiness {
    data object Ready : CaptureReadiness

    /** At least one permission is still outstanding, and none has been refused. */
    data object Waiting : CaptureReadiness

    data class Refused(val reason: String) : CaptureReadiness
}

/**
 * The permission flow for one capture generation.
 *
 * Every implementation is judged against the same contract, so a stand-in that
 * merely counts results the way the real one does is not mistaken for it.
 */
internal interface PermissionFlow {
    /** The generation this flow speaks for. A result for any other is ignored. */
    val generation: Long

    fun stateOf(permission: CapturePermission): PermissionState

    /**
     * Asks for the permissions that may still be asked for, and returns exactly
     * those. A permission already granted, already outstanding, or permanently
     * denied is not re-asked: this flow has its answer, or is waiting for one,
     * and a retry loop over it is a spin.
     */
    fun request(permissions: Set<CapturePermission>): Set<CapturePermission>

    /** One platform result, stamped with the generation that asked for it. */
    fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts): PermissionOutcome

    /**
     * The dialog was cancelled, or the owner was destroyed. The generation
     * retires: outstanding requests settle nothing and no later result may
     * start capture.
     */
    fun retire(reason: String)

    /** Whether both permissions are granted, still outstanding, or refused. */
    fun readiness(): CaptureReadiness
}

/**
 * The flow this sample runs: one state per permission, and nothing else.
 *
 * Bounded state -- two entries and a retirement flag -- under one short
 * monitor, so an Activity callback and the session's own thread may both call
 * in. It calls nothing back, queries no platform, shows no dialog, sleeps for
 * nothing and retries nothing on its own.
 *
 * **Asking.** [request] asks for exactly the permissions that may still be
 * asked: `NotRequested` and `Denied`. Granted, outstanding and
 * `PermanentlyDenied` are left alone -- the first two because the answer is
 * already here or on its way, the last because this flow will not ask again.
 * The returned set is a value snapshot, never this flow's own state.
 *
 * **Settling.** A result settles only an OUTSTANDING permission of the live
 * matching generation. Anything else -- a duplicate, a result nobody asked
 * for, a result for another generation, a result after retirement -- settles
 * nothing and leaves the whole snapshot exactly as it was. A grant is a grant
 * whatever `mayExplain` says; a denial uses the post-result fact to separate
 * `Denied` from `PermanentlyDenied`.
 *
 * **Retirement is absorbing.** After [retire] nothing revives the generation:
 * no request is issued, no result settles, and [readiness] can never be Ready
 * again -- including when both grants had already arrived.
 *
 * `PermanentlyDenied` is THIS SAMPLE's no-reprompt state for THIS generation.
 * It does not claim the grant can never change: a user can change it in
 * Settings, and this tranche does not model that. Likewise `mayExplain` is the
 * platform fact the caller read after the result, not a guarantee about what a
 * dialog will or will not do.
 *
 * **What the caller must do.** A result carries only a generation and a
 * permission. Once a permission has been asked AGAIN after an ordinary denial,
 * those two cannot distinguish the late result of the previous ask from the
 * result of the current one. This flow settles the outstanding request with
 * whichever arrives first; a caller that can interleave them must route each
 * result to the ask it belongs to. No request identifier is invented here.
 */
internal class CapturePermissionFlow(override val generation: Long) : PermissionFlow {
    private val lock = Any()
    private val states = CapturePermission.entries.associateWithTo(mutableMapOf()) { PermissionState.NotRequested }
    private var retired = false

    override fun stateOf(permission: CapturePermission): PermissionState = synchronized(lock) { states.getValue(permission) }

    override fun request(permissions: Set<CapturePermission>): Set<CapturePermission> =
        synchronized(lock) {
            if (retired) return emptySet()
            val asked = mutableSetOf<CapturePermission>()
            for (permission in permissions) {
                val state = states.getValue(permission)
                if (state == PermissionState.NotRequested || state == PermissionState.Denied) {
                    states[permission] = PermissionState.Requested
                    asked.add(permission)
                }
            }
            // A value snapshot: the caller never holds this flow's own state.
            asked.toSet()
        }

    override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts): PermissionOutcome =
        synchronized(lock) {
            if (retired) return PermissionOutcome.Ignored("this generation has retired")
            if (generation != this.generation) return PermissionOutcome.Ignored("not generation ${this.generation}")
            if (states.getValue(permission) != PermissionState.Requested) {
                return PermissionOutcome.Ignored("$permission has no outstanding request")
            }
            val settled =
                when {
                    facts.granted -> PermissionState.Granted
                    facts.mayExplain -> PermissionState.Denied
                    else -> PermissionState.PermanentlyDenied
                }
            states[permission] = settled
            PermissionOutcome.Settled(permission, settled)
        }

    override fun retire(reason: String) {
        synchronized(lock) { retired = true }
    }

    override fun readiness(): CaptureReadiness =
        synchronized(lock) {
            if (retired) return CaptureReadiness.Refused("this generation has retired")
            val refused =
                states.entries.firstOrNull {
                    it.value == PermissionState.Denied || it.value == PermissionState.PermanentlyDenied
                }
            when {
                refused != null -> CaptureReadiness.Refused("${refused.key} is ${refused.value}")
                states.values.all { it == PermissionState.Granted } -> CaptureReadiness.Ready
                else -> CaptureReadiness.Waiting
            }
        }
}
