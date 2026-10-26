package org.openmoq.moq5

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.openmoq.moq5.engine.BackendDrainResult
import org.openmoq.moq5.engine.EndpointBackend
import org.openmoq.moq5.engine.EndpointCreateException
import org.openmoq.moq5.engine.EndpointEngine
import org.openmoq.moq5.engine.EngineClosedException
import org.openmoq.moq5.engine.JniEndpointBackend
import org.openmoq.moq5.engine.ReceiverRefusedException
import org.openmoq.moq5.engine.SenderRefusedException
import org.openmoq.moq5.trust.PlatformTrustSupport
import kotlin.time.Duration

/**
 * A public endpoint root: open, observe, tear down.
 *
 * [open] decides trust BEFORE any native work, creates the root on the
 * endpoint's own service thread through the trusted native path, and returns
 * only once the root exists and its first record has been taken -- a creation
 * failure is thrown from [open] itself, typed, with the exact native code.
 *
 * Observation is ONE coherent native record at a time. [snapshots] carries
 * the record the service thread took after each endpoint activity (the
 * service marks activity whenever a pump cycle changes state), seeded with the
 * record taken right after construction; it is never composed from separate
 * reads and never refreshed on a timer. [snapshot] takes one record now.
 * [awaitEstablished] suspends on the same stream.
 *
 * Teardown: [close] is the immediate, non-suspending logical close -- later
 * operations refuse, the native stop and destroy are scheduled on the service
 * thread; [closeAndJoin] is the deterministic boundary that returns once they
 * have run, exactly once. Neither implies a flush.
 */
public class MoqEndpoint private constructor(private val engine: EndpointEngine) {

    /** The activity-driven record stream; its current value is the latest coherent record. */
    public val snapshots: StateFlow<EndpointSnapshot> = engine.snapshots

    /** One coherent observation of the endpoint, taken now. */
    public suspend fun snapshot(): EndpointSnapshot =
        try {
            engine.snapshot()
        } catch (_: EngineClosedException) {
            throw EndpointClosedException(EndpointTerminal.Unclassified)
        }

    /**
     * Suspend until the endpoint is ESTABLISHED. Throws
     * [EndpointClosedException] with the exact terminal when a record reports
     * one (or with [EndpointTerminal.Unclassified] after a local [close]), and
     * [EndpointTimeoutException] when [timeout] elapses first. Cancelling the
     * caller cancels this wait only: it never touches the endpoint's own
     * interrupt latch.
     */
    public suspend fun awaitEstablished(timeout: Duration) {
        val record =
            try {
                withTimeoutOrNull(timeout) { engine.awaitEstablished() }
            } catch (_: EngineClosedException) {
                throw EndpointClosedException(EndpointTerminal.Unclassified)
            }
        if (record == null) throw EndpointTimeoutException(timeout)
    }

    /**
     * Attaches a media receiver to this endpoint (`moq_media_receiver_attach`),
     * on the endpoint's service worker. At most one receiver is attached at a
     * time: a second attach is refused by the endpoint with
     * [ReceiverAttachFailedException] (`WRONG_STATE`) until the first is
     * closed, after which a new one may attach. The receiver is closed by
     * [MediaReceiver.close], or by this endpoint's teardown before the
     * endpoint is stopped. Refused with [EndpointClosedException] after
     * [close].
     *
     * Ownership boundary: the attach is commit-grade. Cancellation before
     * the submission was accepted attaches nothing; once accepted, the native
     * attach runs to completion and the [MediaReceiver] (or the typed
     * refusal) is delivered to this caller even if it was cancelled
     * meanwhile, so an attached child is never left without the handle that
     * closes it. It is retired exactly once, by [MediaReceiver.close] or by
     * this endpoint's teardown.
     */
    public suspend fun attachReceiver(config: ReceiverConfig): MediaReceiver {
        val attached =
            try {
                engine.attachReceiver(config)
            } catch (_: EngineClosedException) {
                throw EndpointClosedException(EndpointTerminal.Unclassified)
            } catch (e: ReceiverRefusedException) {
                throw ReceiverAttachFailedException(e.code)
            }
        return MediaReceiver(engine, attached.token, attached.model)
    }

    /**
     * Attaches a media sender to this endpoint (`moq_media_sender_attach`),
     * on the endpoint's service worker. At most one sender is attached at a
     * time (a second attach is refused by the endpoint with
     * [SenderAttachFailedException], `WRONG_STATE`, until the first is
     * closed); a sender and a receiver share one endpoint with independent
     * lifecycles. The sender is closed by [MediaSender.close], or by this
     * endpoint's teardown before the endpoint is stopped. Refused with
     * [EndpointClosedException] after [close].
     *
     * Ownership boundary: commit-grade like [attachReceiver] -- cancellation
     * before acceptance attaches nothing; an accepted attach delivers its
     * [MediaSender] (or typed refusal) to this caller even if cancelled
     * meanwhile, and the child is retired exactly once.
     */
    public suspend fun attachSender(config: SenderConfig): MediaSender {
        val attached =
            try {
                engine.attachSender(config)
            } catch (_: EngineClosedException) {
                throw EndpointClosedException(EndpointTerminal.Unclassified)
            } catch (e: SenderRefusedException) {
                throw SenderAttachFailedException(e.code)
            }
        return MediaSender(engine, attached.token, attached.model, config)
    }

    /**
     * Sets the sticky interrupt latch: from now until [resume], every blocking
     * call on the endpoint returns at once with an interrupted result. The
     * latch is set on the calling thread immediately; it is not a state change
     * and publishes no record, and a pending [awaitEstablished] stays pending.
     * Idempotent. Refused with [EndpointClosedException] after [close].
     */
    public fun interrupt(): Unit = setInterrupted(true)

    /**
     * Clears the latch set by [interrupt], restoring normal blocking. Idempotent.
     * Refused with [EndpointClosedException] after [close].
     */
    public fun resume(): Unit = setInterrupted(false)

    private fun setInterrupted(interrupted: Boolean) {
        try {
            engine.setInterrupted(interrupted)
        } catch (_: EngineClosedException) {
            throw EndpointClosedException(EndpointTerminal.Unclassified)
        }
    }

    /**
     * Graceful flush: the caller suspends while the blocking native drain runs
     * on a blocking dispatcher, off the endpoint's service worker, until send
     * work already accepted by attached services has reached the core and the
     * local reliable stream backlog is empty, or until [timeout] elapses. Call it after the last writes and before
     * [closeAndJoin], which never flushes on its own. Every answer is a value:
     *
     *  * [DrainOutcome.Flushed]: stopping now will not truncate queued stream
     *    data.
     *  * [DrainOutcome.TimedOut]: the timeout elapsed first.
     *  * [DrainOutcome.Interrupted]: the [interrupt] latch is set; the latch
     *    answers ahead of any probe.
     *  * [DrainOutcome.Closed]: the requested flush did NOT complete. The
     *    endpoint, or an attached producer holding accepted output,
     *    terminalised first and queued send data may have been truncated.
     *    Never a graceful completion.
     *  * [DrainOutcome.Unsupported]: the backend cannot prove a local flush
     *    and does not claim one.
     *
     * [timeout] must be finite and non-negative ([Duration.ZERO] is a single
     * probe), checked before any native call. Native drains are serialised:
     * concurrent callers wait their turn. Cancelling the caller detaches it
     * from the result only; the bounded native call still completes. Refused
     * with [EndpointClosedException] after [close].
     */
    public suspend fun drain(timeout: Duration): DrainOutcome {
        require(timeout.isFinite() && !timeout.isNegative()) {
            "drain timeout must be finite and non-negative: $timeout"
        }
        val micros = timeout.inWholeMicroseconds
        require(micros < Long.MAX_VALUE) { "drain timeout is not representable in microseconds: $timeout" }
        val result =
            try {
                engine.drain(micros)
            } catch (_: EngineClosedException) {
                throw EndpointClosedException(EndpointTerminal.Unclassified)
            }
        return when (result) {
            BackendDrainResult.Flushed -> DrainOutcome.Flushed
            BackendDrainResult.TimedOut -> DrainOutcome.TimedOut
            BackendDrainResult.Interrupted -> DrainOutcome.Interrupted
            BackendDrainResult.Closed -> DrainOutcome.Closed
            BackendDrainResult.Unsupported -> DrainOutcome.Unsupported
            // The engine throws the quarantine failure for a bridge answer.
            is BackendDrainResult.Bridge -> error("a bridge failure must be thrown by the engine, not returned")
        }
    }

    /**
     * The immediate logical close: future operations refuse at once, and the
     * existing teardown is scheduled on the service thread. Returns without
     * waiting; idempotent. No record is published for it -- a local close is
     * not a terminal the endpoint reported.
     */
    public fun close(): Unit = engine.requestClose()

    /**
     * Stop, then destroy, then retire the root, exactly once, and return when
     * done. Idempotent: a later call makes no native call and resolves the
     * same way the first did.
     */
    public suspend fun closeAndJoin(): Unit = engine.close()

    public companion object {
        /** The private ordinal of [TrustPolicy.PlatformTrustManager]: `MOQ5_TRUST_POLICY_PLATFORM`. */
        private const val TRUST_ORDINAL_PLATFORM = 0

        /**
         * Open an endpoint for [config].
         *
         * Three typed failures, each from its own domain and none narrowed
         * into another: [TrustPolicyUnsupportedException] before any native
         * work when the policy cannot be honoured for the host;
         * [NativeBridgeInitializationException] when the packaged bridge
         * cannot be brought up in this process (decided once, sticky, before
         * any reservation); and [EndpointCreateFailedException] when the
         * native root itself could not be created. In every case nothing was
         * published and nothing needs closing.
         */
        public suspend fun open(config: EndpointConfig): MoqEndpoint =
            open(config) { cfg ->
                JniEndpointBackend.createTrusted(
                    cfg.url, TRUST_ORDINAL_PLATFORM, cfg.nativeOffer(), cfg.nativeHandshakeMicros(),
                    cfg.nativeProtocol(), cfg.nativeWebTransportPath(),
                )
            }

        /**
         * [open], then [awaitEstablished] with [timeout]: returns only an
         * ESTABLISHED endpoint. An open failure propagates with nothing to
         * close. If establishment fails after the endpoint exists -- the typed
         * timeout, the typed terminal, a bridge quarantine, or this caller's
         * cancellation -- the endpoint is torn down with [closeAndJoin] before
         * the failure is rethrown, so no endpoint escapes; a teardown failure
         * is retained as suppressed on it. The timeout is [awaitEstablished]'s:
         * the caller's coroutine bound, never a poll.
         */
        public suspend fun connect(config: EndpointConfig, timeout: Duration): MoqEndpoint =
            establish(open(config), timeout)

        internal suspend fun connect(
            config: EndpointConfig,
            timeout: Duration,
            factory: (EndpointConfig) -> EndpointBackend,
        ): MoqEndpoint = establish(open(config, factory), timeout)

        private suspend fun establish(endpoint: MoqEndpoint, timeout: Duration): MoqEndpoint {
            try {
                endpoint.awaitEstablished(timeout)
            } catch (establishmentFailure: Throwable) {
                try {
                    withContext(NonCancellable) { endpoint.closeAndJoin() }
                } catch (teardownFailure: Throwable) {
                    // A quarantine reports the SAME stable failure to the
                    // waiter and to the teardown. Kotlin's addSuppressed already
                    // ignores a throwable's own identity; the guard states that
                    // intent here rather than leaning on the stdlib detail. The
                    // establishment failure is never wrapped or replaced.
                    if (teardownFailure !== establishmentFailure) {
                        establishmentFailure.addSuppressed(teardownFailure)
                    }
                }
                throw establishmentFailure
            }
            return endpoint
        }

        /**
         * The construction path with the backend factory as a parameter. It is
         * INTERNAL: it exists so the wrapper's own contract can be judged over a
         * scripted backend, and it is not offered to applications.
         */
        internal suspend fun open(
            config: EndpointConfig,
            factory: (EndpointConfig) -> EndpointBackend,
        ): MoqEndpoint {
            PlatformTrustSupport.refusalFor(config)?.let { throw it }
            val engine = EndpointEngine({ factory(config) }, publishSnapshots = true)
            try {
                engine.awaitConstructed()
            } catch (e: EndpointCreateException) {
                // The engine has already retired: no root was published, so
                // there is nothing to tear down. ONLY the native creation
                // result is translated here; a NativeBridgeInitializationException
                // from the factory is already the public failure and passes
                // through unchanged, so the two domains cannot collapse.
                throw EndpointCreateFailedException(e.code)
            }
            return MoqEndpoint(engine)
        }
    }
}

/** The answers of [MoqEndpoint.drain]: exactly the C tier's, one value each. */
public sealed interface DrainOutcome {
    /** Every accepted send obligation reached the core and the local reliable backlog is empty. */
    public data object Flushed : DrainOutcome

    /** The caller's timeout elapsed before the flush completed. */
    public data object TimedOut : DrainOutcome

    /** The interrupt latch is set; nothing was probed. */
    public data object Interrupted : DrainOutcome

    /**
     * The requested flush did not complete: the endpoint, or an attached
     * producer holding accepted output, terminalised first, and queued send
     * data may have been truncated. Not a graceful completion.
     */
    public data object Closed : DrainOutcome

    /** The backend cannot prove a local flush and does not claim one. */
    public data object Unsupported : DrainOutcome
}

/** [awaitEstablished]'s bound elapsed before the endpoint was established. */
public class EndpointTimeoutException internal constructor(
    public val timeout: Duration,
) : IllegalStateException("the endpoint was not established within $timeout")

/**
 * The endpoint is closed. [terminal] is the exact terminal the endpoint's own
 * record reported, or [EndpointTerminal.Unclassified] for a local close that
 * no record classified -- never a fabricated cause.
 */
public class EndpointClosedException internal constructor(
    public val terminal: EndpointTerminal,
) : IllegalStateException("the endpoint is closed: $terminal")

/**
 * Open an endpoint, run [block] with it, and ALWAYS tear it down once the
 * block has finished -- normally, by exception, or by cancellation -- in a
 * cancellation-immune [closeAndJoin]. If [MoqEndpoint.open] itself fails, no
 * endpoint exists and nothing is closed; the open's failure propagates.
 */
public suspend fun <T> withMoqEndpoint(config: EndpointConfig, block: suspend (MoqEndpoint) -> T): T =
    withMoqEndpoint(MoqEndpoint.open(config), block)

/** The factory-taking form, for the wrapper's own rows over a scripted backend. */
internal suspend fun <T> withMoqEndpoint(
    config: EndpointConfig,
    factory: (EndpointConfig) -> EndpointBackend,
    block: suspend (MoqEndpoint) -> T,
): T = withMoqEndpoint(MoqEndpoint.open(config, factory), block)

private suspend fun <T> withMoqEndpoint(endpoint: MoqEndpoint, block: suspend (MoqEndpoint) -> T): T {
    val value =
        try {
            block(endpoint)
        } catch (blockFailure: Throwable) {
            // The block's own failure (or cancellation) is the outcome; a
            // teardown that also fails is kept as suppressed evidence on it,
            // never allowed to replace it.
            try {
                withContext(NonCancellable) { endpoint.closeAndJoin() }
            } catch (teardownFailure: Throwable) {
                blockFailure.addSuppressed(teardownFailure)
            }
            throw blockFailure
        }
    // No block failure: a teardown failure is the outcome.
    withContext(NonCancellable) { endpoint.closeAndJoin() }
    return value
}
