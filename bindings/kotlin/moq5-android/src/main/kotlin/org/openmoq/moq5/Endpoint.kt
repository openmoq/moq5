package org.openmoq.moq5

/**
 * The endpoint's lifecycle state, mirroring `moq_endpoint_state_t`
 * (`service/include/moq/endpoint.h`).
 *
 * [Reconnecting] is reserved by the C tier so reconnect can land without an ABI
 * break; v0 never enters it. It is represented here for the same reason: a
 * binding that could not name it would have to fail on a value the ABI already
 * promises.
 */
public enum class EndpointState {
    Connecting,
    Established,
    Reconnecting,
    Draining,
    Closed,
}

/**
 * A backend-native diagnostic carried with a transport or TLS terminal, as RAW
 * BITS.
 *
 * This is deliberately NOT [MoqErrorCode]: that domain is the MoQ protocol
 * error carried on the wire, and an `OSStatus` or a QUIC CRYPTO_ERROR is
 * neither. picoquic packs a TLS alert in the low byte of a CRYPTO_ERROR, and
 * wtquic-Network stores a NEGATIVE `OSStatus` two's-complement, so bit 63 is
 * set for ordinary certificate failures -- which is also why this is unsigned
 * and full width.
 *
 * It is a diagnostic. Branch on the [EndpointTerminal] case; log or report the
 * bits, do not interpret them as a MoQ code.
 */
@JvmInline
public value class EndpointErrorDetail(public val rawBits: ULong) {
    override fun toString(): String = "EndpointErrorDetail(0x${rawBits.toString(16)})"
}

/**
 * Why an endpoint reached a terminal state, mirroring
 * `moq_endpoint_terminal_reason_t`.
 *
 * The five classified reasons stay DISTINCT: `TLS_CERTIFICATE`, `TLS` and
 * `TRANSPORT` exist precisely because `fatal_code` reports 0 for all three, so
 * collapsing any pair would throw away the only signal that separates a
 * rejected certificate from a timeout or an ALPN mismatch.
 *
 * The detail carried by each case is in the domain that case actually belongs
 * to. [Protocol] carries a MoQ protocol error code -- a value defined by the
 * MoQ wire. The TLS and transport cases carry an [EndpointErrorDetail]: a
 * BACKEND-NATIVE diagnostic that is not a MoQ code at all. Apps branch on the
 * case, never on the number.
 */
public sealed interface EndpointTerminal {
    /** A clean close. */
    public data object Clean : EndpointTerminal

    /** A MoQ/bridge protocol fatal; [detailCode] is the nonzero fatal code. */
    public data class Protocol(val detailCode: MoqErrorCode) : EndpointTerminal

    /** Peer certificate verification failed. */
    public data class TlsCertificate(val detail: EndpointErrorDetail) : EndpointTerminal

    /** Another TLS/crypto handshake failure. */
    public data class Tls(val detail: EndpointErrorDetail) : EndpointTerminal

    /** A generic transport or handshake failure. */
    public data class Transport(val detail: EndpointErrorDetail) : EndpointTerminal

    /**
     * The endpoint is CLOSED and the service cannot classify why.
     *
     * This is ordinary, not exotic. A local stop before any terminal condition
     * reports it, and raw picoquic has no clean-close concept at all
     * (`pq_vt_terminal` passes `false`), so a cleanly closed picoquic endpoint
     * reports it too. It is deliberately NOT [Clean] -- that would assert a
     * graceful close that may never have happened -- and deliberately not
     * [Transport], which would assert a failure that did not occur.
     */
    public data object Unclassified : EndpointTerminal
}

/**
 * ONE coherent observation of an endpoint: `moq_endpoint_get_snapshot()`.
 *
 * Every field comes from a single native record. The binding never composes
 * independent reads and never infers one field from another -- in particular it
 * does not infer a terminal state from the presence of a negotiated version,
 * which is retained across close and so says nothing about liveness.
 *
 * [negotiatedVersion] is `null` when the endpoint never negotiated one; once
 * present it is retained through draining and close, so a terminal handler can
 * still report which draft was in force. [terminal] is `null` while the
 * endpoint is not terminal.
 */
public data class EndpointSnapshot(
    val state: EndpointState,
    val negotiatedVersion: MoqVersion?,
    val terminal: EndpointTerminal?,
)

/**
 * Raised when a native record carries a discriminator this binding does not
 * know.
 *
 * It fails CLOSED. Falling back to a known case would silently report a state
 * or a terminal cause that the peer did not signal, which is worse than
 * refusing: a future `moq_endpoint_state_t` or terminal reason must be added
 * deliberately, not absorbed.
 */
public class UnknownNativeValueException internal constructor(
    /** Which native field carried the value, for diagnostics. */
    public val field: String,
    value: Long,
) : IllegalStateException("unknown native $field value $value") {
    internal constructor(field: String, value: Int) : this(field, value.toLong())
}

/**
 * The single mapper from a native snapshot record to [EndpointSnapshot].
 *
 * It consumes all four fields of ONE record. It is pure, so the JNI layer's
 * only job is to deliver the record.
 */
internal object EndpointSnapshotMapper {

    fun map(
        state: Int,
        negotiatedVersion: UInt,
        terminalReason: Int,
        terminalDetail: ULong,
    ): EndpointSnapshot {
        val mappedState =
            when (state) {
                0 -> EndpointState.Connecting
                1 -> EndpointState.Established
                2 -> EndpointState.Reconnecting
                3 -> EndpointState.Draining
                4 -> EndpointState.Closed
                else -> throw UnknownNativeValueException("endpoint state", state)
            }
        // Only the domain the native reason selects is constructed: a
        // transport diagnostic is never built as a MoQ protocol code, even
        // transiently.
        val mappedTerminal =
            when (terminalReason) {
                // NONE reads two ways, decided by the state in the SAME record.
                0 ->
                    if (mappedState == EndpointState.Closed) {
                        EndpointTerminal.Unclassified
                    } else {
                        null
                    }
                1 -> EndpointTerminal.Clean
                2 -> EndpointTerminal.Protocol(MoqErrorCode(terminalDetail))
                3 -> EndpointTerminal.TlsCertificate(EndpointErrorDetail(terminalDetail))
                4 -> EndpointTerminal.Tls(EndpointErrorDetail(terminalDetail))
                5 -> EndpointTerminal.Transport(EndpointErrorDetail(terminalDetail))
                else ->
                    throw UnknownNativeValueException("terminal reason", terminalReason)
            }
        return EndpointSnapshot(
            state = mappedState,
            // Zero means absent; every nonzero value, known draft or not,
            // crosses unchanged.
            negotiatedVersion = MoqVersion.fromNativeOrNull(negotiatedVersion),
            terminal = mappedTerminal,
        )
    }
}
