package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException

/**
 * One platform encoder, as [TrackEncoder] drives it: `MediaCodec` in
 * asynchronous mode (`MediaCodecEncoder.kt`), or a host double. Every call
 * may throw; [TrackEncoder] makes none of them under its lock.
 */
internal interface EncoderDevice {
    /** The platform codec itself: what the ownership compares a handle by. */
    val identity: Any

    /** `setCallback(callback, handler)` on the owned worker's handler. Called before [configure]. */
    fun setCallbacks(callbacks: EncoderCallbacks)

    fun configure(configuration: EncoderConfiguration)

    /** `createInputSurface()`: after [configure], before [start], for a surface-input role only. */
    fun createInputSurface(): ResourceHandle

    fun start()

    /** `getOutputBuffer(index)`: `null` when the codec does not own [index]. */
    fun outputBuffer(index: Int): ByteBuffer?

    /** `releaseOutputBuffer(index, false)`: an encoder renders nothing. */
    fun releaseOutputBuffer(index: Int)

    /** `getInputBuffer(index)`: the borrowed writable input buffer, or `null` when the codec does not own [index]. */
    fun inputBuffer(index: Int): ByteBuffer?

    /** `queueInputBuffer(index, 0, size, presentationTimeUs, 0)`: the index is the codec's from here, even if this throws. */
    fun queueInputBuffer(index: Int, size: Int, presentationTimeUs: Long)

    fun stop()

    fun release()
}

/**
 * Creates the platform encoder for one configuration. A created encoder that
 * does not support it is released here and refused as [EncoderUnsupported];
 * nothing is configured either way.
 */
internal fun interface EncoderBackend {
    fun create(configuration: EncoderConfiguration): EncoderDevice
}

/** The created encoder does not support the requested configuration; nothing was configured, and nothing substitutes another format. */
internal class EncoderUnsupported(val kind: MediaKind, val reason: String) : IllegalStateException("the $kind encoder cannot be configured: $reason")

/**
 * The asynchronous callbacks of one encoder, with the platform types already
 * taken apart: an output is its index and the four transient `BufferInfo`
 * scalars, copied by the caller before this call; a format is a borrowed
 * [FormatFields] view, read only while this call runs.
 */
internal interface EncoderCallbacks {
    fun onOutputAvailable(index: Int, offset: Int, size: Int, presentationTimeUs: Long, flags: Int)

    fun onFormatChanged(fields: FormatFields)

    fun onInputAvailable(index: Int)

    fun onError(reason: String)
}

/**
 * Where one encoder's callbacks go: the session that admits them, the key it
 * minted for this track's codec callbacks, the generation, the track's
 * handoff, and its format holder -- given the declared maximum bitrate.
 */
internal class EncoderRoute(
    val gate: CodecCallbackGate,
    val key: FormatHolderKey,
    val generation: Long,
    val handoff: CaptureHandoff,
    val format: (FormatFields, ULong?) -> Unit,
)

/** A codec whose callbacks are bound to one [EncoderRoute], once, before it can be opened. */
internal interface EncoderBinding {
    val configuration: EncoderConfiguration

    fun bind(route: EncoderRoute)
}

/**
 * One track's encoder: the ONE lifetime owner of one platform codec, shared by
 * the resource handle the ownership closes and the leases the track's handoff
 * takes, and the platform callback boundary for that codec.
 *
 * **Opening.** [open] runs inside the ownership's acquisition; assembly
 * creates nothing. It refuses, before any platform call, an encoder whose
 * callbacks are not yet bound. It then creates the codec, installs it as this
 * lifetime's codec, and only then registers the callbacks and configures,
 * creates the input surface for the surface role, and starts -- so a callback
 * delivered during any of those reaches this same codec under a lease. If a
 * step fails the codec is disposed through the same lifetime (deferred while a
 * lease holds it). The failure leaves as itself when that disposal
 * succeeded or is still deferred; when a disposal step failed, it leaves
 * inside [UnprovedDisposal], which the ownership's acquisition unwraps.
 *
 * **Lifetime.** A lease takes the owned worker FIRST and then this codec, both
 * or neither. The disposal is requested by closing the handle; it happens
 * once, outside the lock, when the last lease has been returned -- stop if it
 * started, release, then the input surface -- every step attempted, the first
 * failure carrying the rest. A disposal is never taken as an end of track.
 *
 * **Callbacks.** Each callback is admitted by the session as this track's
 * codec callback before it touches anything:
 *
 *  * ENDED: it returns. Nothing is fetched, released or handed off; an index
 *    it named stays the codec's, reclaimed by the codec's own disposal.
 *  * OVERLAPPING another of this track's codec callbacks: the track's
 *    `CaptureFault.Codec` is reported and it returns. Nothing is fetched,
 *    released or handed off, and nothing is reserved again.
 *  * NOT BOUND: a wiring defect, refused by throwing before anything is
 *    touched.
 *
 * An admitted callback runs its one collaborator -- the handoff, the format
 * holder, or this encoder's input slots -- and then releases its reservation
 * with whatever that collaborator raised, so the session records it as the
 * ending's cause, once, before the reservation is removed. The callback then
 * RETURNS: the owned worker keeps running. What the release or a report raises
 * is a cleanup failure the session's ending already holds; it is neither
 * rethrown to the worker nor recorded again. Anything else leaves.
 */
internal class TrackEncoder(
    override val configuration: EncoderConfiguration,
    private val backend: EncoderBackend,
    /** The most PCM input indices this encoder holds for its input driver. */
    private val inputCapacity: Int = DEFAULT_INPUT_CAPACITY,
) : CodecLifetime, OutputBufferAccess, CodecBufferOwner, EncoderBinding {
    val kind: MediaKind get() = configuration.kind

    private val lock = Any()
    private var route: EncoderRoute? = null
    private var opened = false
    private var device: EncoderDevice? = null
    private var worker: WorkerRetention? = null
    private var surface: ResourceHandle? = null
    private var started = false
    private var leases = 0
    private var requested = false
    private var committed = false
    private val inputs = ArrayDeque<Int>()
    private var inputWake: (() -> Unit)? = null

    init {
        require(inputCapacity > 0) { "the input capacity is positive, not $inputCapacity" }
    }

    /** This encoder as its track's codec: the one lifetime, buffer access, index owner and callback binding. */
    val trackCodec: TrackCodec get() = TrackCodec(this, this, this, this)

    /** The surface the camera stream draws into, once opened; `null` for the PCM role. */
    val inputSurface: Any? get() = synchronized(lock) { surface?.identity }

    override fun bind(route: EncoderRoute) {
        require(route.key.kind == kind && route.key.role == CallbackRole.Codec) {
            "a $kind encoder bound to a ${route.key.kind} ${route.key.role.name.lowercase()} key"
        }
        synchronized(lock) {
            check(this.route == null) { "this $kind encoder is already bound to a session" }
            this.route = route
        }
    }

    /**
     * Creates, configures and starts the codec for the owned worker's
     * [retention]; ownership of the returned handle begins when this returns.
     * Closing the handle requests the disposal.
     */
    fun open(retention: WorkerRetention): ResourceHandle {
        synchronized(lock) {
            checkNotNull(route) { "the $kind encoder's callbacks are not bound to a session; bind them before opening" }
            check(!opened) { "the $kind encoder has already been opened" }
            opened = true
        }
        val created = backend.create(configuration)
        synchronized(lock) {
            device = created
            worker = retention
        }
        var primary: Throwable? = null
        try {
            created.setCallbacks(callbacks)
            created.configure(configuration)
            if (configuration.surfaceInput) {
                val input = created.createInputSurface()
                synchronized(lock) { surface = input }
            }
            created.start()
            synchronized(lock) { started = true }
        } catch (cancelled: CancellationException) {
            primary = cancelled
            throw cancelled
        } catch (failure: Exception) {
            primary = failure
            throw failure
        } catch (error: Error) {
            primary = error
            throw error
        } finally {
            // A disposal that failed is stated, not inferred: the opening
            // failure leaves inside the explicit fact, for the ownership to
            // unwrap. A clean disposal leaves it as it is.
            primary?.let { first ->
                val failed = disposalFailures()
                if (failed.isNotEmpty()) throw UnprovedDisposal(first, failed)
            }
        }
        return object : ResourceHandle {
            override val identity: Any = created.identity

            override fun close() = requestDisposal()
        }
    }

    /** The next PCM input index the codec offered, for the input driver to fill and queue; `null` when none is held. */
    fun takeInput(): Int? = synchronized(lock) { inputs.removeFirstOrNull() }

    /**
     * Copies [unit]'s remaining bytes into input [index] and queues it at
     * [presentationTimeUs], under a transfer lease. Returns null once queued,
     * or the named reason nothing was queued. A queue that throws is never
     * retried: the index is the codec's either way.
     */
    fun queueInput(index: Int, unit: ByteBuffer, presentationTimeUs: Long): String? {
        if (!retainForTransfer()) return "the $kind encoder is no longer available"
        var primary: Throwable? = null
        try {
            val codec = leased()
            val buffer = codec.inputBuffer(index) ?: return "the $kind codec does not own input $index"
            val size = unit.remaining()
            if (buffer.capacity() < size) return "the $kind codec's input $index holds ${buffer.capacity()} bytes, not $size"
            buffer.clear()
            buffer.put(unit.duplicate())
            codec.queueInputBuffer(index, size, presentationTimeUs)
            return null
        } catch (cancelled: CancellationException) {
            primary = cancelled
            throw cancelled
        } catch (failure: Exception) {
            primary = failure
            throw failure
        } catch (error: Error) {
            primary = error
            throw error
        } finally {
            val returned = collected { releaseAfterTransfer() }
            if (returned != null) {
                val leaving = primary
                if (leaving == null) throw returned
                if (returned !== leaving) leaving.addSuppressed(returned)
            }
        }
    }

    /** Runs [body] as one of this codec's callbacks: admitted by the session's codec-role authority, or not at all. */
    fun driveInput(body: () -> Unit) = admitted { body() }

    /** The input driver's wake, run inside the admitted callback that offered a PCM input index. Set once, before start. */
    fun onInputOffered(wake: () -> Unit) {
        synchronized(lock) {
            check(inputWake == null) { "this $kind encoder's input wake is already set" }
            inputWake = wake
        }
    }

    override fun retainForTransfer(): Boolean {
        val retention = synchronized(lock) { worker } ?: return false
        if (!retention.retainWorker()) return false
        val granted =
            synchronized(lock) {
                if (committed || device == null) {
                    false
                } else {
                    leases++
                    true
                }
            }
        if (!granted) retention.releaseWorker()
        return granted
    }

    override fun releaseAfterTransfer() {
        val commit: Boolean
        val retention: WorkerRetention?
        synchronized(lock) {
            leases--
            commit = commitLocked()
            retention = worker
        }
        val raised = mutableListOf<Throwable>()
        if (commit) raised.addAll(dispose())
        // The codec goes before the worker it was driven from.
        retention?.let { held -> collected { held.releaseWorker() }?.let { raised.add(it) } }
        carrying(raised)?.let { throw it }
    }

    override fun outputBuffer(index: Int): ByteBuffer? = leased().outputBuffer(index)

    override fun release(index: Int) = leased().releaseOutputBuffer(index)

    /** The owner is finished with the codec. It is disposed now, or by the last lease returned. */
    private fun requestDisposal() {
        carrying(disposalFailures())?.let { throw it }
    }

    /** Requests the disposal; what it raised now, each step's failure separately, in order. */
    private fun disposalFailures(): List<Throwable> {
        val commit = synchronized(lock) {
            requested = true
            commitLocked()
        }
        return if (commit) dispose() else emptyList()
    }

    /** The disposal decision, under the lock, exactly once; the disposal itself is not done here. */
    private fun commitLocked(): Boolean {
        if (!requested || committed || leases > 0) return false
        committed = true
        return true
    }

    /** Stop if it started, release, then the input surface: every step attempted, outside the lock; the failures in order. */
    private fun dispose(): List<Throwable> {
        val codec: EncoderDevice?
        val input: ResourceHandle?
        val running: Boolean
        synchronized(lock) {
            codec = device
            input = surface
            running = started
        }
        val raised = mutableListOf<Throwable>()
        if (codec != null) {
            if (running) collected { codec.stop() }?.let { raised.add(it) }
            collected { codec.release() }?.let { raised.add(it) }
        }
        input?.let { handle -> collected { handle.close() }?.let { raised.add(it) } }
        return raised
    }

    /** Only a transfer holding a lease reaches the codec's buffers. */
    private fun leased(): EncoderDevice =
        synchronized(lock) {
            check(leases > 0) { "the $kind codec's buffers are reached only under a transfer lease" }
            checkNotNull(device)
        }

    private val callbacks =
        object : EncoderCallbacks {
            override fun onOutputAvailable(index: Int, offset: Int, size: Int, presentationTimeUs: Long, flags: Int) =
                admitted { route -> route.handoff.onOutput(route.generation, CodecOutput(index, offset, size, presentationTimeUs, flags)) }

            override fun onFormatChanged(fields: FormatFields) = admitted { route -> route.format(fields, configuration.maximumBitrate) }

            override fun onInputAvailable(index: Int) =
                admitted { route ->
                    when {
                        configuration.surfaceInput -> route.gate.reportCodec(route.key, "the surface-input $kind encoder offered input buffer $index")
                        !offerInput(index) -> route.gate.reportCodec(route.key, "the $kind encoder's input queue already holds $inputCapacity indices")
                        else -> synchronized(lock) { inputWake }?.invoke()
                    }
                }

            override fun onError(reason: String) = admitted { route -> route.handoff.onCodecError(route.generation, reason) }
        }

    private fun offerInput(index: Int): Boolean =
        synchronized(lock) {
            if (inputs.size >= inputCapacity) {
                false
            } else {
                inputs.addLast(index)
                true
            }
        }

    /** One platform callback under this track's codec reservation; see the class contract. */
    private fun admitted(body: (EncoderRoute) -> Unit) {
        val route = checkNotNull(synchronized(lock) { route }) { "a $kind encoder callback arrived before its route was bound" }
        val reservation =
            when (val admission = route.gate.reserveCodec(route.key)) {
                is FormatAdmission.Admitted -> admission.reservation
                FormatAdmission.Ended -> return
                FormatAdmission.Overlapping -> {
                    receiptOwned(route) { route.gate.reportCodec(route.key, OVERLAPPING) }
                    return
                }
                FormatAdmission.NotBound -> throw IllegalStateException("this $kind encoder's codec key is not bound to its session")
            }
        val cause = collected { body(route) }
        receiptOwned(route) {
            val released = route.gate.releaseCodec(reservation, cause)
            if (released is FormatRelease.Refused) throw IllegalStateException("the session refused this $kind encoder's own reservation", cause)
        }
    }

    /**
     * A session call whose raise, if the session's ending already holds it, is
     * that ending's and goes no further. Anything else it raised leaves.
     */
    private fun receiptOwned(route: EncoderRoute, step: () -> Unit) {
        val raised = collected(step) ?: return
        if (!route.gate.owns(raised)) throw raised
    }

    /**
     * One step whose failure is COLLECTED, whatever its class, and returned
     * for the caller to own. The inner rethrow is the cancellation rule for
     * ordinary bodies; this is a cleanup collector, so it is caught again here.
     */
    private fun collected(step: () -> Unit): Throwable? =
        try {
            try {
                step()
                null
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                failure
            } catch (error: Error) {
                error
            }
        } catch (cancelled: CancellationException) {
            cancelled
        }

    /** The first failure, carrying every later distinct one; `null` when there were none. */
    private fun carrying(raised: List<Throwable>): Throwable? {
        val first = raised.firstOrNull() ?: return null
        for (other in raised.drop(1)) if (other !== first && first.suppressed.none { it === other }) first.addSuppressed(other)
        return first
    }

    private companion object {
        const val DEFAULT_INPUT_CAPACITY = 16
        const val OVERLAPPING = "overlapping codec callbacks"
    }
}
