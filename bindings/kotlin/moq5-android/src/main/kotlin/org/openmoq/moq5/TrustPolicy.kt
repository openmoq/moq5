package org.openmoq.moq5

import kotlin.time.Duration
import kotlin.time.Duration.Companion.microseconds

/**
 * Whom an endpoint trusts when the peer presents a certificate chain.
 *
 * ONE case. There is deliberately no insecure policy, no PEM bundle, no custom
 * root set and no application callback: none of those can be expressed from
 * Kotlin, and the internal native route independently refuses a platform
 * delegate combined with an insecure or file-based trust source.
 */
public sealed interface TrustPolicy {
    /**
     * The Android platform trust manager, library-owned end to end: the app's
     * Network Security Config, its per-domain anchors and pin sets, consulted
     * on the transport's own thread after the reference identity has been
     * verified natively. User code is never on the path.
     */
    public data object PlatformTrustManager : TrustPolicy
}

/**
 * Which MoQ drafts to offer the peer (`moq_version_offer_t`).
 *
 * [Automatic] offers the FULL set this build supports -- never "the newest and
 * hope". [Offer] offers exactly its list, in that order, and the peer picks.
 * [Exactly] offers one; a mismatch is a fatal connect. A version whose profile
 * is not compiled in is refused at creation with `MOQ_ERR_UNSUPPORTED`, never
 * downgraded. Codes cross as the draft numbers themselves.
 *
 * An OFFER constraint, not a version-domain one: [MoqVersion] keeps the full
 * unsigned width because the negotiated version is read back from the peer
 * unchanged, but an offered code crosses the bridge as a positive signed
 * 32-bit value, so an offer may only name codes up to [MAX_OFFERABLE_CODE].
 * Unknown codes inside that range still cross verbatim; the service tier
 * answers whether it supports them.
 */
public sealed interface VersionOffer {
    /** The build's full set; the C zero offer. */
    public data object Automatic : VersionOffer

    /**
     * Exactly [versions], in order: non-empty, duplicate-free, and at most
     * [MAX_VERSIONS] long. The list is SNAPSHOTTED here: a caller mutating
     * the list it passed cannot change what crosses.
     */
    public class Offer(versions: List<MoqVersion>) : VersionOffer {
        public val versions: List<MoqVersion> = versions.toList()

        init {
            require(this.versions.isNotEmpty()) { "a version offer must name at least one version; use Automatic for the build's full set" }
            require(this.versions.size <= MAX_VERSIONS) { "a version offer names at most $MAX_VERSIONS versions; got ${this.versions.size}" }
            require(this.versions.toSet().size == this.versions.size) { "a version offer must not repeat a version: ${this.versions}" }
            this.versions.forEach { requireOfferable(it) }
        }

        override fun equals(other: Any?): Boolean = other is Offer && other.versions == versions
        override fun hashCode(): Int = versions.hashCode()
        override fun toString(): String = "VersionOffer.Offer($versions)"
    }

    /** Exactly one version; the peer must speak it. */
    public data class Exactly(val version: MoqVersion) : VersionOffer {
        init {
            requireOfferable(version)
        }
    }

    public companion object {
        /** The service tier's own cap on an offer (`MOQ_ENDPOINT_MAX_VERSIONS`). */
        public const val MAX_VERSIONS: Int = 8

        /** The largest code an offer can carry across the bridge: a positive signed 32-bit value. */
        public val MAX_OFFERABLE_CODE: UInt = Int.MAX_VALUE.toUInt()

        private fun requireOfferable(v: MoqVersion) {
            require(v.code <= MAX_OFFERABLE_CODE) {
                "$v cannot be offered: an offered code must not exceed $MAX_OFFERABLE_CODE (the bridge carries offers as positive 32-bit values)"
            }
        }
    }
}

/**
 * Which transport carries the session (`moq_transport_protocol_t`).
 *
 * [Automatic] derives it from the URL scheme: `moqt://` or `moq://` is native
 * MoQ-over-QUIC, `https://` is WebTransport over HTTP/3. An explicit
 * [RawQuic] or [WebTransport] overrides the scheme -- the service contract
 * makes the field the override knob, and a scheme/field conflict is not an
 * error -- while the URL itself must still parse in a recognised scheme.
 */
public sealed interface TransportProtocol {
    /** Derive from the URL scheme; the C zero value. */
    public data object Automatic : TransportProtocol

    /** Native MoQ-over-QUIC (`moqt-NN` ALPN), whatever the scheme. */
    public data object RawQuic : TransportProtocol

    /** WebTransport over HTTP/3 (an extended CONNECT), whatever the scheme. */
    public data object WebTransport : TransportProtocol
}

/**
 * How to reach an endpoint, whom to trust when it answers, which drafts to
 * offer, how long the transport handshake may take, and which transport to
 * use.
 *
 * [handshakeTimeout] bounds the QUIC/TLS handshake only (a peer that never
 * completes it holds the endpoint in Connecting until this elapses); it does
 * not bound the WebTransport CONNECT or the MoQ SETUP. `null` leaves the
 * backend's own value. A value must be finite, positive and at least one
 * whole microsecond: `Duration.ZERO` is not a second spelling of "default".
 *
 * [webTransportPath] is the WebTransport CONNECT request target override.
 * `null` means no override: the service then uses the URL's path, or `/moq`
 * when the URL has none. A non-null value is the exact request target the
 * CONNECT carries: RFC 3986 `path-abempty [ "?" query ]`, rooted (`/`), made
 * of `pchar` and `/` (plus `?` inside the query) with complete `%HH` escapes;
 * no fragment, and no character outside that set (control, space, DEL,
 * non-ASCII, or printable characters such as `<`, `[` or `|` that RFC 3986
 * requires to be percent-encoded). It is not the MoQ SETUP `PATH` option
 * (which follows the same URI rules on native QUIC), and on native QUIC it has no wire
 * effect; it is still carried to the service unchanged rather than
 * second-guessed against [protocol].
 */
public data class EndpointConfig(
    public val url: String,
    public val trustPolicy: TrustPolicy = TrustPolicy.PlatformTrustManager,
    public val versions: VersionOffer = VersionOffer.Automatic,
    public val handshakeTimeout: Duration? = null,
    public val protocol: TransportProtocol = TransportProtocol.Automatic,
    public val webTransportPath: String? = null,
) {
    init {
        if (handshakeTimeout != null) {
            require(handshakeTimeout.isFinite() && handshakeTimeout.isPositive()) {
                "handshakeTimeout must be finite and positive, or null for the backend default: $handshakeTimeout"
            }
            require(handshakeTimeout >= 1.microseconds) {
                "handshakeTimeout is a microsecond duration; $handshakeTimeout is below one microsecond"
            }
        }
        if (webTransportPath != null) {
            require(WebTransportRequestTarget.isWellFormed(webTransportPath)) {
                "webTransportPath must be an RFC 3986 path-abempty [ \"?\" query ] request target: rooted, pchar and '/' (plus '?' in the query), complete %HH escapes, nothing else; got '$webTransportPath'"
            }
        }
    }
}

/**
 * The WebTransport CONNECT request-target grammar the config and the bridge
 * both apply: `path-abempty [ "?" query ]` in RFC 3986 terms, rooted and
 * non-empty. Before the first `?` every character is `pchar` or `/`; after
 * it, `pchar`, `/` or `?`; `pchar` is `ALPHA / DIGIT / "-" / "." / "_" / "~"
 * / pct-encoded / sub-delims / ":" / "@"` and `sub-delims` is `"!" / "$" /
 * "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="`. Every `%` must be a
 * complete `%HH`. Anything else -- `#`, control, space, DEL, non-ASCII, and
 * printable characters outside that set such as `"`, `<`, `>`, `\`, `[`, `]`,
 * `^`, backtick, `{`, `|`, `}` -- is refused. Pure and allocation-free.
 */
internal object WebTransportRequestTarget {
    fun isWellFormed(path: String): Boolean {
        if (path.isEmpty() || path[0] != '/') return false
        var inQuery = false
        var i = 0
        while (i < path.length) {
            val c = path[i]
            when {
                c == '%' -> {
                    if (i + 2 >= path.length || !isHex(path[i + 1]) || !isHex(path[i + 2])) return false
                    i += 2
                }
                c == '?' -> if (!inQuery) inQuery = true // a later '?' is query data
                c == '/' -> Unit
                isPcharLiteral(c) -> Unit
                else -> return false
            }
            i++
        }
        return true
    }

    /** `pchar` minus `pct-encoded`: unreserved, sub-delims, ':' and '@'. */
    private fun isPcharLiteral(c: Char): Boolean =
        c in 'A'..'Z' || c in 'a'..'z' || c in '0'..'9' ||
            c == '-' || c == '.' || c == '_' || c == '~' ||
            c == '!' || c == '$' || c == '&' || c == '\'' || c == '(' || c == ')' ||
            c == '*' || c == '+' || c == ',' || c == ';' || c == '=' ||
            c == ':' || c == '@'

    private fun isHex(c: Char): Boolean = c in '0'..'9' || c in 'a'..'f' || c in 'A'..'F'
}

/** The offer as it crosses the bridge: the C policy ordinal and the codes. */
internal data class NativeEndpointOffer(val policy: Int, val versions: List<Int>)

internal fun EndpointConfig.nativeOffer(): NativeEndpointOffer =
    when (val v = versions) {
        VersionOffer.Automatic -> NativeEndpointOffer(0, emptyList())
        is VersionOffer.Offer -> NativeEndpointOffer(1, v.versions.map { it.code.toInt() })
        is VersionOffer.Exactly -> NativeEndpointOffer(2, listOf(v.version.code.toInt()))
    }

/** Whole microseconds, `0` for the backend default; validated at construction. */
internal fun EndpointConfig.nativeHandshakeMicros(): Long = handshakeTimeout?.inWholeMicroseconds ?: 0L

/** The C `moq_transport_protocol_t` ordinal. */
internal fun EndpointConfig.nativeProtocol(): Int =
    when (protocol) {
        TransportProtocol.Automatic -> 0
        TransportProtocol.RawQuic -> 1
        TransportProtocol.WebTransport -> 2
    }

/** The request-target override verbatim, or null for none; validated at construction. */
internal fun EndpointConfig.nativeWebTransportPath(): String? = webTransportPath

/**
 * The selected [policy] cannot be honoured for [host] on this device.
 *
 * Decided BEFORE any native work: no reservation, no endpoint. The one cause
 * in this slice is a host for which the platform requires certificate
 * transparency, which this transport cannot present.
 */
public class TrustPolicyUnsupportedException internal constructor(
    public val policy: TrustPolicy,
    public val host: String,
    reason: String,
) : IllegalStateException("$policy cannot be honoured for $host: $reason")

/**
 * The native endpoint could not be created.
 *
 * [nativeCode] is the native result verbatim. When [isBridgeRefusal] it is the
 * bridge's own private refusal -- a registry category -- which is NOT an
 * endpoint result and is never narrowed into one: [endpointCode] is then null.
 * Otherwise [endpointCode] offers the same value as the endpoint's
 * `moq_result_t`.
 */
public class EndpointCreateFailedException internal constructor(
    public val nativeCode: Int,
) : IllegalStateException("the endpoint could not be created: native result $nativeCode") {
    /** True for a private-registry refusal, which no endpoint ever answered. */
    public val isBridgeRefusal: Boolean
        get() = nativeCode < BRIDGE_BASE

    /** The endpoint's own result, or null when the refusal was the bridge's. */
    public val endpointCode: MoqResultCode?
        get() = if (isBridgeRefusal) null else MoqResultCode(nativeCode)

    private companion object {
        /** `MOQ5_ERR_REGISTRY_BASE`: everything at or below it is the bridge's. */
        const val BRIDGE_BASE = -1000
    }
}
