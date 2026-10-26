package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException

/**
 * The four platform steps an [EncoderBackend] takes to hand over one encoder:
 * allocate the native codec, read its capabilities, release it, or wrap it as
 * the [EncoderDevice] that from then on owns it. `MediaCodecEncoder.kt`
 * implements them over `MediaCodec`; nothing else here is platform code.
 */
internal interface EncoderAllocationPort<C : Any> {
    fun allocate(configuration: EncoderConfiguration): C

    fun capabilities(codec: C, configuration: EncoderConfiguration): EncoderCapabilities

    fun release(codec: C)

    fun device(codec: C, configuration: EncoderConfiguration): EncoderDevice
}

/**
 * The backend's one control flow over an [EncoderAllocationPort]: allocate,
 * check the configuration against what the allocated codec reports, and
 * transfer it to its device -- or release it, once, and fail.
 *
 * An allocation that throws allocated nothing and releases nothing. A
 * refusal, or any failure between allocation and transfer, releases the codec
 * once. If that release succeeds the failure leaves as itself -- a refusal as
 * the named [EncoderUnsupported]. If it fails, the codec was not proved
 * released, and the failure leaves inside [UnprovedDisposal], stated rather
 * than inferred, for the acquisition to unwrap -- even when the release raised
 * the failure's own instance.
 */
internal class AllocatingBackend<C : Any>(private val port: EncoderAllocationPort<C>) : EncoderBackend {
    override fun create(configuration: EncoderConfiguration): EncoderDevice {
        val codec = port.allocate(configuration)
        var primary: Throwable? = null
        try {
            val refusal = EncoderCapabilityCheck.refusal(configuration, port.capabilities(codec, configuration))
            if (refusal != null) throw EncoderUnsupported(configuration.kind, refusal)
            return port.device(codec, configuration)
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
            primary?.let { first -> collected { port.release(codec) }?.let { throw UnprovedDisposal(first, listOf(it)) } }
        }
    }

    /** One cleanup step whose failure is collected, whatever its class, for the caller to own. */
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
}
