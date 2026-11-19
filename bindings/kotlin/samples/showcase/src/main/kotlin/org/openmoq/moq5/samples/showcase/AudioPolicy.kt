package org.openmoq.moq5.samples.showcase

/** The platform's audio focus and noisy-output signals, as [AudioPolicy] uses them. */
interface AudioFocusPlatform {
    /** Ask for focus. True only when it is granted now; [lost] runs if it is lost later. */
    fun request(lost: (transient: Boolean) -> Unit): Boolean

    fun abandon()

    /** Hear of output becoming noisy (headphones unplugged, a Bluetooth sink lost) through [noisy]. */
    fun listenNoisy(noisy: () -> Unit)

    fun stopListeningNoisy()
}

/**
 * Audio focus and noisy-output handling for a session, as the [FocusPort]
 * that [ShowcaseCore] asks before it starts one.
 *
 * [acquire] asks the platform for focus and holds it only when granted, then
 * listens for noisy output. Losing focus -- a call, another player, a
 * transient loss -- and audio becoming noisy (headphones unplugged, a
 * Bluetooth sink lost) both call [onInterrupted] with why, which stops the
 * session: a live broadcast has no pause, so the viewer rejoins with Play. A
 * loss that only asks for ducking is left to the platform, which lowers this
 * app's volume itself. [release] gives back exactly what was acquired.
 */
class AudioPolicy(
    private val platform: AudioFocusPlatform,
    private val onInterrupted: (reason: String) -> Unit,
) : FocusPort {
    private var held = false

    /** Which grant is held. A loss or noisy callback from an earlier grant, still queued, is stale and ignored. */
    private var grant = 0L

    /** True while focus is granted and held. */
    val holding: Boolean get() = held

    override fun acquire(): Boolean {
        if (held) return true
        val mine = ++grant
        val granted = platform.request { transient -> interrupted(mine, if (transient) INTERRUPTED_TRANSIENT else INTERRUPTED) }
        if (!granted) return false
        held = true
        platform.listenNoisy { interrupted(mine, NOISY) }
        return true
    }

    override fun release() {
        if (!held) return
        held = false
        platform.stopListeningNoisy()
        platform.abandon()
    }

    private fun interrupted(from: Long, reason: String) {
        if (held && from == grant) onInterrupted(reason)
    }

    internal companion object {
        const val INTERRUPTED = "Stopped: another app is playing audio"
        const val INTERRUPTED_TRANSIENT = "Stopped for another app's audio"
        const val NOISY = "Stopped: the audio output changed"
    }
}
