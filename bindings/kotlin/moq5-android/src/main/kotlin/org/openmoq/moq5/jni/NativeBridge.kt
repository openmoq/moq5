package org.openmoq.moq5.jni

/**
 * The one class the native side binds to.
 *
 * It holds no state and performs no logic: `JNI_OnLoad` registers exactly the
 * rows of the single authority in `jni/moq5_abi.h` -- that table, and nothing
 * else, decides what is bound, so this comment names the authority rather than
 * a count that would rot as rows are added. Keeping the bound class free of
 * state is what lets the handshake below be an ordinary instantiable object --
 * no reset seam has to be added to production code so that tests can run more
 * than one handshake.
 *
 * There are no `Java_*` exports: the methods are bound by explicit
 * `RegisterNatives`, so the shared object's only dynamic export stays
 * `JNI_OnLoad`. That is NOT a licence to minify these names: `RegisterNatives`
 * resolves each row by its compile-time name and descriptor, so the class and
 * every bound method must survive R8 -- which is exactly what the shipped
 * `consumer-rules.pro` keeps, and what `verifyBridgeAbiAuthority` and the R8
 * identity gate enforce.
 */
internal object NativeBridge {

    /**
     * The private bridge ABI epoch this Kotlin side was compiled against.
     *
     * Mirrors `MOQ5_ABI_EPOCH`; `verifyBridgeAbiAuthority` fails the build if
     * the two drift. It is deliberately NOT `MOQ_VERSION_NUMBER`: the semantic
     * LibMoQ version describes the library release, not the layout of this
     * method table, and the two move independently.
     */
    const val ABI_EPOCH: Int = 17

    /** Mirrors `MOQ5_FEATURE_BRIDGE_HANDSHAKE`. */
    const val FEATURE_BRIDGE_HANDSHAKE: Long = 0x1L

    /** Mirrors `MOQ5_FEATURES_REQUIRED`. Absence fails closed. */
    const val FEATURES_REQUIRED: Long = FEATURE_BRIDGE_HANDSHAKE

    /**
     * Mirrors `MOQ5_FEATURES_KNOWN`. Bits outside this mask are ADDITIVE: a
     * newer native side may advertise capabilities this build has never heard
     * of, and that must not make an otherwise compatible bridge fail.
     */
    const val FEATURES_KNOWN: Long = FEATURE_BRIDGE_HANDSHAKE

    external fun nativeAbiEpoch(): Int

    external fun nativeFeatureBits(): Long

    /** Diagnostic input only -- never a compatibility decision. */
    external fun nativeLibmoqVersion(): Int

    /**
     * Construct an endpoint root and publish its registry token into [out].
     *
     * Returns a `moq_result_t` verbatim. On any failure NOTHING is published:
     * [out] is untouched and no registry slot or generation is consumed. The
     * token is opaque -- it is never a pointer, and the whole 64 bits are
     * carried.
     */
    external fun nativeEndpointCreate(url: String, out: LongArray): Int

    /**
     * Construct an endpoint root whose certificate trust is decided by the
     * LIBRARY-OWNED platform trust manager, and publish its registry token
     * into [out].
     *
     * [trustPolicy] is a private ordinal, never a callback, a PEM bundle, an
     * insecure option or a pointer: `0` selects the Android platform trust
     * manager and is the only value accepted; any other value is refused
     * with `MOQ_ERR_INVAL` before a reservation or an endpoint exists. The
     * reference identity is verified natively before the platform is
     * consulted, and the platform is consulted on the transport's own thread,
     * never the caller's. Everything else -- the returned `moq_result_t`, the
     * untouched [out] on failure, the opaque token -- is exactly
     * [nativeEndpointCreate]'s contract: the two rows share one transaction.
     *
     * The endpoint configuration rides this row: [versionPolicy] is the C
     * `moq_version_policy_t` ordinal (0 AUTO, 1 LIST, 2 EXACT) with
     * [versions] the `moq_version_t` codes (empty for AUTO, exactly one for
     * EXACT, 1..8 distinct positive codes for LIST), and [handshakeMicros] the
     * transport handshake bound (0 = the backend default). A malformed
     * combination -- a null or oversized array, a bad ordinal, a negative
     * bound, a non-positive or duplicate code -- is refused with
     * `MOQ_ERR_INVAL` before any reservation; an unknown positive code is not
     * malformed and is answered by the service tier itself.
     *
     * Transport selection rides it too: [protocol] is the C
     * `moq_transport_protocol_t` ordinal (0 AUTO, 1 RAW_QUIC, 2 WEBTRANSPORT;
     * an explicit value overrides the URL scheme, as the service contract
     * says), and [webTransportPath] the WebTransport CONNECT request target
     * override, or null for none (the service then resolves the URL path or
     * `/moq`). A non-null path must be a well-formed RFC 3986 request target
     * -- `path-abempty [ "?" query ]`, rooted, `pchar` and `/` (plus `?` in
     * the query), complete `%HH` escapes, nothing outside that set -- else
     * `MOQ_ERR_INVAL` before any reservation. It is carried whatever the
     * protocol says; the service tier decides what it means.
     */
    external fun nativeEndpointCreateTrusted(
        url: String,
        trustPolicy: Int,
        versionPolicy: Int,
        versions: IntArray,
        handshakeMicros: Long,
        protocol: Int,
        webTransportPath: String?,
        out: LongArray,
    ): Int

    /**
     * Stop, then destroy, then retire the root named by [token].
     *
     * Destruction happens only after a successful stop; a stop that refuses
     * leaves the root live and reports the refusal rather than hiding it.
     */
    external fun nativeEndpointRelease(token: Long): Int

    /**
     * `moq_endpoint_wake()`. Any-thread, like [nativeEndpointSetInterrupted]
     * and [nativeEndpointDrain].
     *
     * It leases the root rather than acquiring it, so it is not
     * authority-thread confined, and the lease pins the entry for the duration
     * of the call -- retirement cannot cross it. Returns a registry refusal for
     * a token that is not a live root; the wake itself cannot fail.
     */
    external fun nativeEndpointWake(token: Long): Int

    /**
     * `moq_endpoint_set_interrupted()`: the sticky, non-terminal interrupt
     * latch. Any-thread and void by contract, so the row leases like
     * [nativeEndpointWake] and answers only MOQ_OK or a registry refusal for a
     * token that is not a live root. Setting breaks a blocked wait inside the
     * C tier; clearing restores normal blocking and wakes nothing.
     */
    external fun nativeEndpointSetInterrupted(token: Long, interrupted: Boolean): Int

    /**
     * `moq_endpoint_drain()`: the application-thread graceful flush, blocking
     * for up to [timeoutMicros]. Any-thread for the application, so the row
     * leases like [nativeEndpointWake] and holds that lease for the whole
     * call. Returns the C result verbatim (MOQ_OK, MOQ_DONE,
     * MOQ_ERR_INTERRUPTED, MOQ_ERR_CLOSED, MOQ_ERR_UNSUPPORTED) or a registry
     * refusal for a token that is not a live root. The caller validates the
     * timeout before calling; the row carries the value it is given.
     */
    external fun nativeEndpointDrain(token: Long, timeoutMicros: Long): Int

    /**
     * `moq_media_receiver_attach()` under the endpoint root [root]: creates
     * the receiver, inserts it as an OWNED child of the root in the registry,
     * and publishes the full-width child token to `out[0]`. Authority-thread
     * confined. The namespace crosses as byte arrays (one per part, each
     * non-empty, 1..32 parts, at most 4096 bytes in all) and the catalog
     * track as bytes or null for the default -- never as text. [overflow] is
     * the C `moq_media_overflow_policy_t` ordinal (1 DROP_TO_KEYFRAME,
     * 2 DROP_GROUP, 3 FLOW_CONTROL; UNSET is refused), [timeMode] the
     * `moq_media_time_mode_t` ordinal (0 RAW, 1 SHARED_EPOCH); the bounds are
     * non-negative (0 = the C default). A malformed direct call is
     * `MOQ_ERR_INVAL` before the root is pinned and before anything is
     * published; the endpoint's own refusal (WRONG_STATE for a second
     * receiver, CLOSED once terminal) crosses exactly; a registry refusal is
     * the registry's, and a receiver attached but unowned is destroyed at once.
     */
    external fun nativeReceiverAttach(
        root: Long,
        namespace: Array<ByteArray>,
        overflow: Int,
        maxObjects: Int,
        maxBytes: Long,
        maxTrackEvents: Int,
        autoSubscribe: Boolean,
        catalogTrack: ByteArray?,
        timeMode: Int,
        out: LongArray,
    ): Int

    /**
     * Close a receiver: retires the child [child] under its owner [root] --
     * the registry destroys the receiver on this (authority) thread and the
     * root stays live. Returns MOQ_OK or the registry's exact refusal (a stale
     * child, a foreign owner, the wrong thread).
     */
    external fun nativeReceiverClose(root: Long, child: Long): Int

    /**
     * Drain ONE track event from the receiver [child] under [root]
     * (`moq_media_receiver_poll_track`), on the root's authority thread.
     * [scalars] is the [ReceiverEventScalars] block; null or short is
     * `MOQ_ERR_INVAL` before anything is polled. Returns `MOQ_OK` with the
     * block filled (the track slot carries a BORROWED child token under the
     * receiver, minted on the handle's first sight and stable until the
     * receiver closes; zero for CATALOG_READY), `MOQ_DONE` when the queue is
     * empty, `MOQ_ERR_CLOSED` when empty and terminal (the terminal slots
     * then carry `moq_media_receiver_is_fatal` and `_fatal_code`), or the
     * registry's exact refusal. A refused token mint after the event was
     * consumed is the registry's code and nothing is published.
     */
    external fun nativeReceiverPollTrack(root: Long, child: Long, scalars: LongArray?): Int

    /**
     * Snapshot the description of [track], a borrowed child of the receiver
     * [child] under [root] (`moq_media_track_desc_get`), on the authority
     * thread. Returns the byte fields in [TrackDescribeBytes] order (the
     * fixed fields, then `depends`, then the content-protection ref ids) and
     * fills the [TrackDescribeScalars] block; or null with the refusal in
     * the block's first slot: `MOQ_ERR_INVAL` for a short block (null block:
     * null alone), otherwise the registry's exact code.
     */
    external fun nativeTrackDescribe(root: Long, child: Long, track: Long, scalars: LongArray?): Array<ByteArray>?

    /**
     * ONE `moq_media_receiver_poll_object` attempt on the receiver [child]
     * under [root], on the authority thread. Returns the byte arrays in
     * [ReceiverObjectBytes] order (payload, fragment, packed samples) with
     * the [ReceiverObjectScalars] block filled, the C object's buffers having
     * been copied and released exactly once inside the row; or null with the
     * answer in the block's first slot: `MOQ_DONE`, `MOQ_ERR_INTERRUPTED`
     * (the latch, before the queue was read), `MOQ_ERR_CLOSED` (with the
     * terminal slots), `MOQ_ERR_INVAL` for a short block (a null block: null
     * alone), or the registry's exact refusal -- including a refused token
     * mint, after which the object has been released. The track slot carries
     * the handle's BORROWED child token from the receiver's first-sight map:
     * minted here when an object is the bridge's first sight of the handle
     * (the C queues the track's ADDED first, but it may not be drained yet),
     * and reused by the later track poll.
     */
    external fun nativeReceiverPollObject(root: Long, child: Long, scalars: LongArray?): Array<ByteArray>?

    /**
     * `moq_media_receiver_subscribe_track` for the track [track] (a borrowed
     * child of the receiver [child]) under [root], on the authority thread.
     * [start] is the `moq_media_start_mode_t` ordinal (0 CURRENT, 1
     * NEXT_GROUP); [priority] is used only when [hasPriority] and must be
     * 0..255. A malformed direct call is `MOQ_ERR_INVAL` before anything is
     * acquired; the endpoint's own `moq_result_t` crosses exactly (OK,
     * WRONG_STATE, UNSUPPORTED, CLOSED); a registry refusal is the registry's.
     */
    external fun nativeTrackSubscribe(root: Long, child: Long, track: Long, start: Int, hasPriority: Boolean, priority: Int): Int

    /** `moq_media_receiver_unsubscribe_track`, the same acquisition and answers as [nativeTrackSubscribe]. */
    external fun nativeTrackUnsubscribe(root: Long, child: Long, track: Long): Int

    /**
     * `moq_media_receiver_track_state` into [out] `[0]` (the C value 0..5).
     * A null or empty block is `MOQ_ERR_INVAL` before anything is acquired.
     */
    external fun nativeTrackState(root: Long, child: Long, track: Long, out: LongArray?): Int

    /**
     * ONE `moq_media_receiver_poll_sap` attempt on the receiver [child] under
     * [root], on the authority thread, into the [ReceiverSapScalars] block,
     * then -- still under the same registry pin -- `moq_media_receiver_get_stats`
     * for `sap_record_deferrals` into the block's DEFERRALS slot. Returns
     * `MOQ_OK` (record + counter), `MOQ_DONE` (counter), `MOQ_ERR_CLOSED`
     * (counter + terminal slots), `MOQ_ERR_INVAL` for a null/short block before
     * anything is acquired, or the registry's exact refusal (a refused token
     * mint included). The track slot carries the handle's borrowed token from
     * the receiver's first-sight map.
     */
    external fun nativeReceiverPollSap(root: Long, child: Long, scalars: LongArray?): Int

    /** `moq_media_receiver_poll_media_timeline` with `media_timeline_record_deferrals`, into [ReceiverTimelineScalars]; the same contract as [nativeReceiverPollSap]. */
    external fun nativeReceiverPollMediaTimeline(root: Long, child: Long, scalars: LongArray?): Int

    /**
     * ONE `moq_media_receiver_get_stats` read on the receiver [child] under
     * [root], on the authority thread, every field published exactly once
     * into the [ReceiverStatsScalars] block. A null or short block is
     * `MOQ_ERR_INVAL` before anything is acquired; a registry refusal is the
     * registry's exact code.
     */
    external fun nativeReceiverStats(root: Long, child: Long, scalars: LongArray?): Int

    /**
     * The sender rows. Each is rooted: the sender is an OWNED child of
     * [root], its tracks BORROWED children of the sender. A null or short
     * block is `MOQ_ERR_INVAL` before anything is acquired; a registry
     * refusal is the registry's exact code; the endpoint's own result is
     * carried verbatim.
     *
     * [nativeSenderAttach]: `moq_media_sender_attach` with the
     * `SenderAttachScalars` block; installs the C demand/lifecycle callbacks
     * as wake-only (each calls `moq_endpoint_wake`, nothing else); the
     * owned child token into [out].
     */
    external fun nativeSenderAttach(root: Long, parts: Array<ByteArray>?, catalogTrack: ByteArray?, scalars: LongArray?, out: LongArray?): Int

    /** `moq_media_sender_destroy` through the child's retirement; the root stays live. */
    external fun nativeSenderClose(root: Long, child: Long): Int

    /** `moq_media_sender_add_track` from the `SendTrackBytes` fields and `SendTrackScalars` block; the borrowed track token into [out]. */
    external fun nativeSenderAddTrack(root: Long, child: Long, bytes: Array<ByteArray?>?, scalars: LongArray?, out: LongArray?): Int

    /** `moq_media_sender_remove_track`. */
    external fun nativeSenderRemoveTrack(root: Long, child: Long, track: Long): Int

    /**
     * ONE `moq_media_sender_write`: the payload (and the CMAF property block,
     * or null) are copied into fresh rcbufs inside the call, transferred to
     * the service on `MOQ_OK` only, and released on every other path.
     */
    external fun nativeSenderWrite(root: Long, child: Long, track: Long, payload: ByteArray?, properties: ByteArray?, scalars: LongArray?): Int

    /** ONE `moq_media_sender_end_track`. */
    external fun nativeSenderEndTrack(root: Long, child: Long, track: Long): Int

    /** ONE `moq_media_sender_convert_to_vod` over the whole batch: [tracks] and [durations] of equal length. */
    external fun nativeSenderConvertToVod(root: Long, child: Long, tracks: LongArray?, durations: LongArray?): Int

    /** `moq_media_sender_complete`. */
    external fun nativeSenderComplete(root: Long, child: Long): Int

    /** The five state queries under one pin into the `SenderProbeScalars` block. */
    external fun nativeSenderProbe(root: Long, child: Long, scalars: LongArray?): Int

    /** `moq_media_sender_track_subscriptions` into `out[0]`. */
    external fun nativeSenderTrackDemand(root: Long, child: Long, track: Long, out: LongArray?): Int

    /** `moq_media_sender_get_stats` into the `SenderStatsScalars` block. */
    external fun nativeSenderStats(root: Long, child: Long, scalars: LongArray?): Int

    /**
     * `moq_codec_init_data_build`: a size query, then the build, into
     * `out[0]`. Returns a `moq_result_t`; only `MOQ_OK` publishes an array.
     * [sourceFormat] is the native `moq_codec_source_format_t` value and
     * [nalLengthSize] must be representable in the native unsigned byte, or
     * the row refuses with `MOQ_ERR_INVAL` without calling the utility. No
     * endpoint, token or worker is involved: the utility is pure.
     */
    external fun nativeCodecInitDataBuild(sourceFormat: Int, source: ByteArray?, nalLengthSize: Int, out: Array<ByteArray?>?): Int

    /**
     * `moq_codec_string_format`: a size query, then the formatting, of the
     * ASCII codec string into `out[0]`. [configFormat] is the native
     * `moq_codec_config_format_t` value; [oti] must be representable in the
     * native unsigned byte when [hasOti], and zero otherwise.
     */
    external fun nativeCodecStringFormat(configFormat: Int, sampleEntry: ByteArray?, hasOti: Boolean, oti: Int, decoderConfig: ByteArray?, out: Array<ByteArray?>?): Int

    /**
     * `moq_endpoint_wait()`. Blocking, authority-thread confined.
     *
     * Returns the native `moq_result_t` VERBATIM: `MOQ_OK`, `MOQ_DONE`,
     * `MOQ_ERR_INTERRUPTED`, `MOQ_ERR_CLOSED`, or any other negative value.
     * Nothing is collapsed and no sign is narrowed.
     */
    external fun nativeEndpointWait(token: Long, timeoutMicros: Long): Int

    /**
     * `moq_endpoint_get_snapshot()` as ONE coherent native record.
     *
     * [out] receives exactly four values -- state, negotiated version, terminal
     * reason, terminal detail -- from a single native call. The snapshot is
     * never rebuilt from independent accessors, a future nonzero version
     * survives, and all 64 detail bits cross as raw bits.
     */
    external fun nativeEndpointSnapshot(token: Long, out: LongArray): Int

    /**
     * `moq_endpoint_stop()`, carried exactly. `MOQ_OK` means stopped; every
     * other value is the refusal, unnormalised, and the endpoint is still live.
     */
    external fun nativeEndpointStop(token: Long): Int

    /**
     * Retire the root: the token is invalidated BEFORE registry cleanup
     * destroys the endpoint.
     *
     * It does NOT stop the endpoint. Eligibility is owned by the registry: a
     * successful [nativeEndpointStop] marks the root ready, and this call
     * retires only a ready root. An unready live root is refused by the
     * registry's state category, stays valid, and is not cleaned -- so a
     * caller that skipped the stop learns it, and the second stop the engine
     * never asked for is never made on its behalf.
     */
    external fun nativeEndpointDestroy(token: Long): Int
}
