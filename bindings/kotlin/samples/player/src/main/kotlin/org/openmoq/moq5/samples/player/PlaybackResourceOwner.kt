package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException

/**
 * One native allocation, owned BEFORE fallible setup. The renderer holds this
 * cell before entering the factory; a failed setup plus failed release cannot
 * lose the native reference or authorize disposal of its dependencies.
 * Single serial owner, no reentrant methods, no allocation-exhaustion promise.
 */
internal class PlaybackResourceOwner<T : Any>(private val release: (T) -> Unit) {
    private var resource: T? = null
    private var attempted = false
    private var closeAttempted = false
    private var closeFailure: Throwable? = null
    var acquired: Boolean = false
        private set
    val retained: Boolean get() = resource != null

    fun prepare(create: () -> T, setup: (T) -> Unit): T {
        check(!attempted && !closeAttempted) { "native resource acquisition is one-shot" }
        attempted = true
        val native = create()
        resource = native
        acquired = true
        try {
            setup(native)
            return native
        } catch (cancelled: CancellationException) {
            failed(cancelled)
        } catch (raised: Exception) {
            failed(raised)
        } catch (raised: Error) {
            failed(raised)
        }
    }

    private fun failed(primary: Throwable): Nothing {
        val secondary = runCatching { close() }.exceptionOrNull()
        if (secondary != null && secondary !== primary && primary.suppressed.none { it === secondary }) primary.addSuppressed(secondary)
        throw primary
    }

    /** Once only; later calls rethrow the cached failure, never retry native release. */
    fun close() {
        if (!closeAttempted) {
            closeAttempted = true
            val native = resource
            if (native != null) {
                closeFailure = runCatching { release(native) }.exceptionOrNull()
                if (closeFailure == null) resource = null
            }
        }
        closeFailure?.let { throw it }
    }
}
