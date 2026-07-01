/*
 * Draft-18 profile (partial).
 *
 * Implements the pieces needed to bring up a draft-18 session's control
 * channel: each peer opens its own unidirectional control channel and sends a
 * unified SETUP, and the session reaches ESTABLISHED once it has both sent and
 * received SETUP. Request and data-plane messages are added as the
 * corresponding implementation is added; their vtable ops are intentionally
 * left NULL until then.
 */

#include "session_internal.h"
#include "moq/control_d18.h"
#include "moq/vi64.h"

/* -- D18 profile state --------------------------------------------- */

typedef struct moq_d18_profile_state {
    moq_version_t version;
    bool          setup_sent;
    bool          setup_received;
    /* Request IDs are parity-allocated (client even, server odd). Draft-18 has
     * no MAX_REQUEST_ID; QUIC stream limits provide flow control. */
    uint64_t      next_local_request_id;
    uint64_t      peer_next_request_id;
    uint64_t      next_track_alias;
} moq_d18_profile_state_t;

static void d18_init_in_place(void *profile_state, const moq_session_cfg_t *cfg)
{
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)profile_state;
    memset(d18, 0, sizeof(*d18));
    d18->version = MOQ_VERSION_DRAFT_18;
    d18->next_local_request_id =
        (cfg->perspective == MOQ_PERSPECTIVE_CLIENT) ? 0 : 1;
    d18->peer_next_request_id =
        (cfg->perspective == MOQ_PERSPECTIVE_CLIENT) ? 1 : 0;
    d18->next_track_alias = 1;
}

static void d18_destroy(void *profile_state)
{
    (void)profile_state;
}

/* -- Setup handshake ----------------------------------------------- */

static void d18_fill_setup_complete_event(moq_session_t *s, moq_event_t *e)
{
    memset(e, 0, sizeof(*e));
    e->kind = MOQ_EVENT_SETUP_COMPLETE;
    e->detail_size = (uint32_t)sizeof(moq_setup_complete_event_t);
    e->borrow_epoch = s->borrow_epoch;
    e->u.setup_complete.local_perspective = s->perspective;
    e->u.setup_complete.peer_perspective =
        (s->perspective == MOQ_PERSPECTIVE_CLIENT)
            ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
}

/* Establish the session once SETUP has been both sent and received. */
static moq_result_t d18_maybe_complete(moq_session_t *s)
{
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)s->profile_state;
    if (d18->setup_sent && d18->setup_received &&
        s->state != MOQ_SESS_ESTABLISHED) {
        s->state = MOQ_SESS_ESTABLISHED;
        moq_event_t e;
        d18_fill_setup_complete_event(s, &e);
        return push_event(s, &e);
    }
    return MOQ_OK;
}

/*
 * Draft-18 start is valid for both client and server: each opens its own
 * unidirectional control channel and sends SETUP without waiting for the peer.
 */
static moq_result_t d18_start(moq_session_t *s)
{
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)s->profile_state;
    if (s->state != MOQ_SESS_IDLE)
        return MOQ_ERR_WRONG_STATE;

    uint8_t buf[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    /* Advertise the auth-token cache size when configured (§10.3.1.3) so the
     * peer may register aliases; no other Setup Option is sourced yet. */
    moq_d18_setup_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    if (s->send_auth_token_cache_size) {
        opts.has_max_auth_token_cache_size = true;
        opts.max_auth_token_cache_size = s->auth_token_cache_size;
    }
    moq_result_t rc = moq_d18_encode_setup_opts(&w, &opts);
    if (rc < 0) return rc;

    moq_stream_ref_t ref = moq_stream_ref_from_u64(s->next_stream_ref);
    rc = queue_open_uni_control(s, ref, buf, moq_buf_writer_offset(&w));
    if (rc < 0) return rc;
    s->next_stream_ref++;

    d18->setup_sent = true;
    s->state = MOQ_SESS_SETUP_SENT;
    return d18_maybe_complete(s);
}

static moq_result_t d18_resolve_auth_token_list(
    moq_session_t *s, const moq_d18_auth_token_t *tokens, size_t count,
    moq_resolved_token_t *out_tokens, size_t *out_token_count,
    bool *out_staged, moq_auth_txn_t *txn, uint64_t *out_reject_code);

static moq_result_t d18_handle_setup(moq_session_t *s,
                                     const moq_control_envelope_t *env)
{
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)s->profile_state;
    if (d18->setup_received)
        return close_with_error(s, 0x3, "duplicate SETUP");

    /* Reserve the completion event up front: this handler is retryable until the
     * auth transaction commits, and nothing below may mutate before it can run to
     * the push. (setup_sent is true post-start, so receipt always completes.) */
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    /* Decode the Setup Options (§10.3.1): unknown options are skipped, a
     * duplicate known non-repeatable option / over-cap length closes 0x3, a
     * malformed token structure closes 0x6. */
    moq_d18_setup_opts_t opts;
    moq_result_t orc = moq_d18_decode_setup_opts(env->payload, env->payload_len,
                                                 &opts);
    if (orc == MOQ_D18_ERR_KVP_FORMAT)
        return close_with_error(s, 0x6, "malformed auth token structure");
    if (orc < 0)
        return close_with_error(s, 0x3, "malformed SETUP options");

    /* PATH / AUTHORITY (§10.3.1.1-2): client-to-server only. Mirror the draft-16
     * setup-parameter handling: a server-sent PATH/AUTHORITY closes with
     * INVALID_PATH (0x8) / INVALID_AUTHORITY (0x19); a server receiving them
     * records presence (the application owns URI policy). */
    bool peer_is_client = (s->perspective == MOQ_PERSPECTIVE_SERVER);
    if (opts.has_path && !peer_is_client)
        return close_with_error(s, 0x8, "PATH from server");
    if (opts.has_authority && !peer_is_client)
        return close_with_error(s, 0x19, "AUTHORITY from server");

    /* SETUP auth tokens (§10.3.1.4): functionally equivalent to the message
     * parameter. DELETE/USE_ALIAS reference an alias, and nothing can be
     * registered before the peer's first message, so they cannot appear here
     * (mirrors the draft-16 CLIENT_SETUP rule). */
    for (size_t i = 0; i < opts.auth_token_count; i++) {
        if (opts.auth_tokens[i].alias_type == MOQ_AUTH_TOKEN_DELETE ||
            opts.auth_tokens[i].alias_type == MOQ_AUTH_TOKEN_USE_ALIAS)
            return close_with_error(s, 0x3, "DELETE/USE_ALIAS in SETUP");
    }

    /* §10.3.1.4: a SETUP REGISTER exceeding MAX_AUTH_TOKEN_CACHE_SIZE MUST NOT
     * fail the session with AUTH_TOKEN_CACHE_OVERFLOW; it is treated as
     * USE_VALUE (the token still authorizes, the alias is just not registered;
     * the sender purges aliases that failed per our advertised size). Project
     * capacity cumulatively across this SETUP's own REGISTERs on top of the
     * current cache usage, downgrading each one that would not fit BEFORE the
     * shared resolution path (whose overflow close is the request-path rule). */
    {
        size_t proj_bytes = s->peer_token_cache.used_bytes;
        size_t proj_active = 0;
        for (size_t i = 0; i < s->peer_token_cache.cap; i++)
            if (s->peer_token_cache.entries[i].active) proj_active++;
        for (size_t i = 0; i < opts.auth_token_count; i++) {
            if (opts.auth_tokens[i].alias_type != MOQ_AUTH_TOKEN_REGISTER)
                continue;
            size_t esize = MOQ_TOKEN_ENTRY_OVERHEAD +
                           opts.auth_tokens[i].token_value.len;
            bool fits = esize >= MOQ_TOKEN_ENTRY_OVERHEAD &&
                        proj_bytes <= s->peer_token_cache.max_bytes &&
                        esize <= s->peer_token_cache.max_bytes - proj_bytes &&
                        proj_active < s->peer_token_cache.cap;
            if (fits) {
                proj_bytes += esize;
                proj_active++;
            } else {
                opts.auth_tokens[i].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
            }
        }
    }

    moq_resolved_token_t resolved[MOQ_DECODED_MAX_TOKENS];
    bool staged[MOQ_DECODED_MAX_TOKENS];
    size_t token_count = 0;
    uint64_t reject_code = 0;
    moq_auth_txn_t txn;
    memset(&txn, 0, sizeof(txn));
    moq_result_t arc = d18_resolve_auth_token_list(s,
        opts.auth_tokens, opts.auth_token_count,
        resolved, &token_count, staged, &txn, &reject_code);
    if (arc < 0) return arc;
    /* close_with_error inside the resolution (duplicate alias) returns MOQ_OK
     * with the session CLOSED; the close must stick -- never fall through to
     * establish below. */
    if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
    if (reject_code != 0) {
        /* There is no request to reject at SETUP time: an unusable token here
         * is a session error. A request-level MALFORMED_AUTH_TOKEN (0x4 --
         * semantically invalid or duplicate resolved value) maps to the
         * MALFORMED_AUTH_TOKEN session error (0x16); anything else keeps the
         * generic formatting close. */
        process_auth_tokens_free_staging(s, resolved, staged, token_count);
        process_auth_tokens_abort_txn(s, &txn);
        if (reject_code == 0x4)
            return close_with_error(s, 0x16, "malformed auth token in SETUP");
        return close_with_error(s, 0x6, "invalid auth token in SETUP");
    }

    /* Scratch-copy the resolved token values for the SETUP_COMPLETE event,
     * freeing any staged heap values (mirrors the request paths). */
    size_t scratch_saved = s->event_scratch_len;
    moq_resolved_token_t *ev_tokens = NULL;
    for (size_t i = 0; i < token_count; i++) {
        if (resolved[i].token_value.len > 0) {
            const uint8_t *src = resolved[i].token_value.data;
            size_t slen = resolved[i].token_value.len;
            uint8_t *copy = event_scratch_copy(s, src, slen);
            if (staged[i])
                s->alloc.free((void *)(uintptr_t)src, slen, s->alloc.ctx);
            staged[i] = false;
            if (!copy) {
                s->event_scratch_len = scratch_saved;
                process_auth_tokens_free_staging(s, resolved, staged, token_count);
                process_auth_tokens_abort_txn(s, &txn);
                if (scratch_saved == 0)
                    return close_with_error(s, 0x1,
                        "event scratch permanently too small");
                return MOQ_ERR_BUFFER;
            }
            resolved[i].token_value.data = copy;
        } else {
            resolved[i].token_value.data = NULL;
        }
    }
    if (token_count > 0) {
        ev_tokens = (moq_resolved_token_t *)event_scratch_alloc_aligned(s,
            token_count * sizeof(moq_resolved_token_t),
            _Alignof(moq_resolved_token_t));
        if (!ev_tokens) {
            s->event_scratch_len = scratch_saved;
            process_auth_tokens_abort_txn(s, &txn);
            if (scratch_saved == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small");
            return MOQ_ERR_BUFFER;
        }
        memcpy(ev_tokens, resolved, token_count * sizeof(moq_resolved_token_t));
    }

    /* Commit: record peer options, complete the handshake, surface the event
     * with the tokens, then commit the auth transaction (commit-last). */
    s->peer_setup.has_path = opts.has_path;
    s->peer_setup.has_authority = opts.has_authority;
    s->peer_setup.has_max_auth_token_cache_size =
        opts.has_max_auth_token_cache_size;
    s->peer_setup.max_auth_token_cache_size = opts.max_auth_token_cache_size;
    d18->setup_received = true;
    if (d18->setup_sent && s->state != MOQ_SESS_ESTABLISHED) {
        s->state = MOQ_SESS_ESTABLISHED;
        moq_event_t e;
        d18_fill_setup_complete_event(s, &e);
        e.u.setup_complete.tokens = ev_tokens;
        e.u.setup_complete.token_count = ev_tokens ? token_count : 0;
        moq_result_t prc = push_event(s, &e);   /* slot reserved above */
        if (prc < 0) {
            process_auth_tokens_abort_txn(s, &txn);
            return prc;
        }
    }
    process_auth_tokens_commit_txn(s, &txn);
    return MOQ_OK;
}

/* GOAWAY on the control stream (§10.4). The profile owns the wire decode and the
 * pre-decode active/duplicate checks + Request-ID parity; the draft-neutral core
 * owns the DRAINING transition, drain deadline, and the surfaced event. */
static moq_result_t d18_handle_goaway(moq_session_t *s,
                                      const moq_control_envelope_t *env)
{
    if (!session_is_active(s))
        return close_with_error(s, 0x3, "GOAWAY before ESTABLISHED");
    if (s->goaway_received)
        return close_with_error(s, 0x3, "duplicate GOAWAY");

    moq_d18_goaway_t ga;
    moq_result_t rc = moq_d18_decode_goaway(env->payload, env->payload_len, &ga);
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed GOAWAY");

    /* The Request ID names the smallest of OUR outbound requests the peer will
     * not process, so it must carry our outbound parity (client even, server
     * odd); a mismatch is INVALID_REQUEST_ID (§10.4). Acting on it (rejecting
     * in-flight requests >= this id) is deferred; the blanket goaway_received
     * guard already refuses all new outbound requests. */
    uint64_t our_parity = (s->perspective == MOQ_PERSPECTIVE_CLIENT) ? 0u : 1u;
    if ((ga.request_id & 1u) != our_parity)
        return close_with_error(s, 0x4, "GOAWAY Request ID parity");

    return session_core_on_goaway(s, ga.uri.data, ga.uri.len);
}

static moq_result_t d18_process_control_data(moq_session_t *s,
                                             const uint8_t *data, size_t len,
                                             size_t *out_consumed)
{
    size_t total = 0;

    while (total < len && s->state != MOQ_SESS_CLOSED) {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, data + total, len - total);

        moq_control_envelope_t env;
        moq_result_t rc = moq_d18_decode_envelope(&r, &env);
        if (rc == MOQ_ERR_BUFFER) break;
        if (rc < 0) {
            *out_consumed = total;
            return close_with_error(s, 0x3, "malformed control envelope");
        }

        switch (env.msg_type) {
        case MOQ_D18_STREAM_SETUP:
            rc = d18_handle_setup(s, &env);
            break;
        case MOQ_D18_GOAWAY:
            rc = d18_handle_goaway(s, &env);
            break;
        default:
            /* SETUP and GOAWAY are the control-stream messages handled so far.
             * Draft-18 requires closing the session on an unknown message type,
             * and no control message is meant to be silently ignored, so any
             * other (or not-yet-supported) type is a protocol violation. */
            *out_consumed = total;
            return close_with_error(s, 0x3,
                                    "unsupported draft-18 control message");
        }
        if (rc < 0) {
            *out_consumed = total;
            return rc;
        }
        total += moq_buf_reader_offset(&r);
    }

    *out_consumed = total;
    return MOQ_OK;
}

/* Classify an inbound unidirectional stream by its leading stream type. */
static moq_uni_class_t d18_classify_uni_stream(const uint8_t *data, size_t len)
{
    uint64_t type = 0;
    size_t n = moq_vi64_decode(data, len, &type);
    if (n == 0)
        return MOQ_UNI_CLASS_NEED_MORE;

    switch (type) {
    case MOQ_D18_STREAM_SETUP:
        return MOQ_UNI_CLASS_CONTROL;
    case MOQ_D18_STREAM_PADDING:
        return MOQ_UNI_CLASS_PADDING;
    case MOQ_D18_STREAM_FETCH_HEADER:
        return MOQ_UNI_CLASS_DATA;
    default:
        /* Subgroup header types are 0b0XX1XXXX (bit7 clear, bit4 set). */
        if (type <= 0x7Fu && ((uint8_t)type & 0x90u) == 0x10u)
            return MOQ_UNI_CLASS_DATA;
        return MOQ_UNI_CLASS_UNKNOWN;
    }
}

/* -- Request admission --------------------------------------------- *
 * Request IDs are parity-allocated; there is no MAX_REQUEST_ID gate (QUIC
 * stream limits provide flow control). Each outbound request travels on its own
 * bidi stream; the session core mints the stream_ref and registers it. */

static moq_result_t d18_prepare_request(moq_session_t *s,
                                        struct moq_request_endpoint *out)
{
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)s->profile_state;
    memset(out, 0, sizeof(*out));
    out->has_request_id = true;
    out->request_id = d18->next_local_request_id;
    return MOQ_OK;
}

static void d18_commit_request(moq_session_t *s,
                               const struct moq_request_endpoint *ep)
{
    (void)ep;
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)s->profile_state;
    d18->next_local_request_id += 2;   /* keep parity */
}

static void d18_abort_request(moq_session_t *s,
                              const struct moq_request_endpoint *ep)
{
    (void)s; (void)ep;   /* request ID not consumed until commit */
}

static void d18_release_request(moq_session_t *s,
                                const struct moq_request_endpoint *ep)
{
    (void)s; (void)ep;   /* request IDs are not recycled */
}

static uint64_t d18_next_track_alias(const moq_session_t *s)
{
    return ((const moq_d18_profile_state_t *)s->profile_state)->next_track_alias;
}

static void d18_advance_track_alias(moq_session_t *s, uint64_t next_after)
{
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)s->profile_state;
    if (next_after >= d18->next_track_alias)
        d18->next_track_alias = next_after + 1;
}

/* Validate an inbound request that arrived on its own bidi stream: the wire
 * request id must follow the peer's parity and be the next in sequence. The
 * endpoint binds both the request id and the bidi stream_ref. */
static moq_result_t d18_validate_inbound_request_stream(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t msg_type,
    uint64_t wire_request_id, struct moq_request_endpoint *out)
{
    (void)msg_type;
    const moq_d18_profile_state_t *d18 =
        (const moq_d18_profile_state_t *)s->profile_state;
    bool peer_is_client = (s->perspective == MOQ_PERSPECTIVE_SERVER);
    uint64_t expected_parity = peer_is_client ? 0 : 1;
    if ((wire_request_id & 1) != expected_parity)
        return close_with_error(s, 0x4, "wrong request ID parity");
    if (wire_request_id != d18->peer_next_request_id)
        return close_with_error(s, 0x4, "request ID not next in sequence");
    memset(out, 0, sizeof(*out));
    out->has_request_id = true;
    out->request_id = wire_request_id;
    out->has_stream_ref = true;
    out->stream_ref = ref;
    return MOQ_OK;
}

static void d18_commit_inbound_request(moq_session_t *s,
                                       const struct moq_request_endpoint *ep)
{
    (void)ep;
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)s->profile_state;
    d18->peer_next_request_id += 2;
}

/* Map the representable SUBSCRIBE/FETCH settings onto draft-18 Message
 * Parameters, emitting a parameter only when it differs from the protocol
 * default (so the omitted form yields the default on the peer). */
static void d18_fill_request_params(moq_d18_msg_params_t *p,
                                    uint8_t subscriber_priority,
                                    bool has_forward, bool forward,
                                    uint8_t group_order)
{
    memset(p, 0, sizeof(*p));
    if (subscriber_priority != 128) {
        p->has_subscriber_priority = true;
        p->subscriber_priority = subscriber_priority;
    }
    if (has_forward && !forward) {       /* default is forward = 1 */
        p->has_forward = true;
        p->forward = 0;
    }
    if (group_order != MOQ_GROUP_ORDER_DEFAULT) {
        p->has_group_order = true;
        p->group_order = group_order;
    }
}

/* Map the draft-18 OBJECT_/SUBGROUP_DELIVERY_TIMEOUT parameters (milliseconds)
 * onto the single semantic delivery timeout (microseconds). The two carriers
 * must agree when both are present (else PROTOCOL_VIOLATION); a single carrier
 * maps directly. */
static moq_result_t d18_map_delivery_timeout(const moq_d18_msg_params_t *p,
                                             bool *has_out, uint64_t *us_out)
{
    bool ho = p->has_object_delivery_timeout;
    bool hs = p->has_subgroup_delivery_timeout;
    if (ho && hs &&
        p->object_delivery_timeout_ms != p->subgroup_delivery_timeout_ms)
        return MOQ_ERR_PROTO;
    if (!ho && !hs) {
        *has_out = false;
        *us_out = 0;
        return MOQ_OK;
    }
    uint64_t ms = ho ? p->object_delivery_timeout_ms
                     : p->subgroup_delivery_timeout_ms;
    if (ms > UINT64_MAX / 1000) return MOQ_ERR_PROTO;   /* overflow guard */
    *has_out = true;
    *us_out = ms * 1000;
    return MOQ_OK;
}

/* The codec token array and the session-core decoded-token array share a cap, so
 * a message that decodes within one fits the other. */
_Static_assert(MOQ_D18_MAX_AUTH_TOKENS == MOQ_DECODED_MAX_TOKENS,
               "auth-token caps must match");

/* Map the public authorization tokens onto the wire parameter block as USE_VALUE
 * Token structures (the alias mechanism is not exposed by the public API, the
 * same as draft-16). Returns MOQ_ERR_INVAL if more than the codec cap. */
static moq_result_t d18_fill_auth_tokens(moq_d18_msg_params_t *p,
                                         const moq_auth_token_t *tokens,
                                         size_t count)
{
    if (count > MOQ_D18_MAX_AUTH_TOKENS) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < count; i++) {
        p->auth_tokens[i].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p->auth_tokens[i].alias = 0;
        p->auth_tokens[i].token_type = tokens[i].token_type;
        p->auth_tokens[i].token_value = tokens[i].token_value;
    }
    p->auth_token_count = count;
    return MOQ_OK;
}

/* Resolve decoded wire authorization tokens against the shared session token
 * cache (REGISTER/DELETE/USE_ALIAS/USE_VALUE), staging into the caller's
 * resolved-token array. Mirrors the draft-16 inbound auth path; the cache and
 * resolution live in session_auth.c (draft-neutral). Shared by the request
 * message-parameter path and the SETUP option path (§10.3.1.4). */
static moq_result_t d18_resolve_auth_token_list(
    moq_session_t *s, const moq_d18_auth_token_t *tokens, size_t count,
    moq_resolved_token_t *out_tokens, size_t *out_token_count,
    bool *out_staged, moq_auth_txn_t *txn, uint64_t *out_reject_code)
{
    moq_decoded_auth_token_t in[MOQ_DECODED_MAX_TOKENS];
    for (size_t i = 0; i < count; i++) {
        const moq_d18_auth_token_t *t = &tokens[i];
        switch (t->alias_type) {
        case MOQ_AUTH_TOKEN_DELETE:    in[i].op = MOQ_AUTH_OP_DELETE; break;
        case MOQ_AUTH_TOKEN_REGISTER:  in[i].op = MOQ_AUTH_OP_REGISTER; break;
        case MOQ_AUTH_TOKEN_USE_ALIAS: in[i].op = MOQ_AUTH_OP_USE_ALIAS; break;
        default:                       in[i].op = MOQ_AUTH_OP_USE_VALUE; break;
        }
        in[i].alias = t->alias;
        in[i].token_type = t->token_type;
        in[i].token_value = t->token_value.data;
        in[i].token_value_len = t->token_value.len;
    }
    return process_auth_tokens(s, in, count, out_tokens, out_token_count,
                               MOQ_DECODED_MAX_TOKENS, out_reject_code,
                               out_staged, txn);
}

static moq_result_t d18_resolve_auth_tokens(moq_session_t *s,
                                            const moq_d18_msg_params_t *params,
                                            moq_resolved_token_t *out_tokens,
                                            size_t *out_token_count,
                                            bool *out_staged,
                                            moq_auth_txn_t *txn,
                                            uint64_t *out_reject_code)
{
    return d18_resolve_auth_token_list(s, params->auth_tokens,
                                       params->auth_token_count,
                                       out_tokens, out_token_count,
                                       out_staged, txn, out_reject_code);
}

/* SUBSCRIBE encode. Priority/forward/group-order/filter and authorization tokens
 * travel as Message Parameters. */
static moq_result_t d18_encode_subscribe(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_subscribe_encode_args *args)
{
    (void)s;
    moq_d18_msg_params_t p;
    d18_fill_request_params(&p, args->subscriber_priority,
                            args->has_forward, args->forward,
                            args->group_order);
    moq_result_t arc = d18_fill_auth_tokens(&p, args->auth_tokens,
                                            args->auth_token_count);
    if (arc < 0) return arc;
    if (args->filter != MOQ_SUBSCRIBE_FILTER_NONE) {
        p.has_filter = true;
        p.filter_type = args->filter;
        p.filter_start_group = args->start_group;
        p.filter_start_object = args->start_object;
        p.filter_end_group = args->end_group;
    }
    p.has_new_group_request = args->has_new_group_request;
    p.new_group_request = args->new_group_request;
    return moq_d18_encode_subscribe(w, args->request_id,
                                    &args->track_namespace, args->track_name, &p);
}

/* PUBLISH encode (§10.10): a publisher-initiated subscription. Track Alias is
 * publisher-chosen; only FORWARD (emitted when withholding, i.e. forward=0) and
 * AUTHORIZATION_TOKEN travel as Message Parameters; an opaque Track Properties
 * tail follows. */
static moq_result_t d18_encode_publish(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_publish_encode_args *args)
{
    (void)s;
    moq_d18_publish_t p;
    memset(&p, 0, sizeof(p));
    p.request_id = args->request_id;
    p.track_namespace = args->track_namespace;
    p.track_name = args->track_name;
    p.track_alias = args->track_alias;
    if (args->has_forward && !args->forward) {   /* default is forward = 1 */
        p.params.has_forward = true;
        p.params.forward = 0;
    }
    moq_result_t arc = d18_fill_auth_tokens(&p.params, args->auth_tokens,
                                            args->auth_token_count);
    if (arc < 0) return arc;
    p.track_properties.data = args->track_properties;
    p.track_properties.len = args->track_properties_len;
    return moq_d18_encode_publish(w, &p);
}

/* Lenient outbound-side extraction of dynamic-group support: reuses the
 * shared property walker without the mandatory/violation outcomes (a blob
 * the encoder would refuse simply reads as unsupported). */
static bool d18_track_properties_dynamic_groups(const uint8_t *data,
                                                size_t len)
{
    if (!data || len == 0) return false;
    bool dyn = false;
    if (moq_d18_scan_dynamic_groups(data, len, &dyn) < 0)
        return false;
    return dyn;
}

/* PUBLISH_OK encode (§10.10 / §10.5): the subscriber's delivery parameters
 * (SUBSCRIBER_PRIORITY / GROUP_ORDER) on the request bidi; empty Track
 * Properties. No request id (stream-correlated). */
static moq_result_t d18_encode_publish_ok(
    moq_session_t *s, struct moq_buf_writer *w, uint64_t request_id,
    uint8_t subscriber_priority, uint8_t group_order,
    bool has_new_group_request, uint64_t new_group_request)
{
    (void)s; (void)request_id;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    if (subscriber_priority != 128) {
        p.has_subscriber_priority = true;
        p.subscriber_priority = subscriber_priority;
    }
    if (group_order != MOQ_GROUP_ORDER_DEFAULT) {
        p.has_group_order = true;
        p.group_order = group_order;
    }
    p.has_new_group_request = has_new_group_request;
    p.new_group_request = new_group_request;
    return moq_d18_encode_publish_ok(w, &p);
}

/* PUBLISH_NAMESPACE encode. Only AUTHORIZATION_TOKEN travels as a Message
 * Parameter; the response is REQUEST_OK / REQUEST_ERROR on the request bidi. */
static moq_result_t d18_encode_publish_namespace(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_publish_namespace_encode_args *args)
{
    (void)s;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    moq_result_t arc = d18_fill_auth_tokens(&p, args->auth_tokens,
                                            args->auth_token_count);
    if (arc < 0) return arc;
    return moq_d18_encode_publish_namespace(w, args->request_id,
                                            &args->track_namespace, &p);
}

/* SUBSCRIBE_NAMESPACE encode (§10.18). Namespace-only in draft-18: only
 * AUTHORIZATION_TOKEN travels as a Message Parameter; the public interest field
 * has no draft-18 wire representation (it split into SUBSCRIBE_TRACKS). */
static moq_result_t d18_encode_subscribe_namespace(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_subscribe_namespace_encode_args *args)
{
    (void)s;
    /* draft-18 SUBSCRIBE_NAMESPACE is namespace-only: the public interest field
     * has no wire representation here (PUBLISHER_STATE / BOTH are served by the
     * separate SUBSCRIBE_TRACKS message and its own public API). Reject an
     * unrepresentable interest rather than silently downgrading the request to
     * namespace-only. */
    if (args->namespace_interest != MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE)
        return MOQ_ERR_INVAL;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    moq_result_t arc = d18_fill_auth_tokens(&p, args->auth_tokens,
                                            args->auth_token_count);
    if (arc < 0) return arc;
    return moq_d18_encode_subscribe_namespace(w, args->request_id,
                                              &args->prefix, &p);
}

/* NAMESPACE / NAMESPACE_DONE encode (§10.16 / §10.17): a single namespace
 * suffix on a SUBSCRIBE_NAMESPACE response stream. */
static moq_result_t d18_encode_namespace_msg(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_namespace_msg_encode_args *args)
{
    (void)s;
    return moq_d18_encode_namespace_msg(w, &args->suffix, args->is_done);
}

/* SUBSCRIBE_TRACKS request encode (§10.19): Track Namespace Prefix + Message
 * Parameters (FORWARD and AUTHORIZATION_TOKEN). FORWARD defaults to 1, so it is
 * emitted only when explicitly set to 0. */
static moq_result_t d18_encode_subscribe_tracks(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_subscribe_tracks_encode_args *args)
{
    (void)s;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    if (args->has_forward && !args->forward) {   /* default is forward = 1 */
        p.has_forward = true;
        p.forward = 0;
    }
    moq_result_t arc = d18_fill_auth_tokens(&p, args->auth_tokens,
                                            args->auth_token_count);
    if (arc < 0) return arc;
    return moq_d18_encode_subscribe_tracks(w, args->request_id,
                                           &args->prefix, &p);
}

/* PUBLISH_BLOCKED encode (§10.20): a Track Namespace Suffix + Track Name on the
 * SUBSCRIBE_TRACKS response stream. */
static moq_result_t d18_encode_publish_blocked(
    moq_session_t *s, struct moq_buf_writer *w,
    const moq_namespace_t *suffix, moq_bytes_t track_name)
{
    (void)s;
    return moq_d18_encode_publish_blocked(w, suffix, track_name);
}

/* TRACK_STATUS request encode (§10.14): the SUBSCRIBE layout minus delivery
 * params; only AUTHORIZATION_TOKEN travels as a Message Parameter. */
static moq_result_t d18_encode_track_status(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_track_status_encode_args *args)
{
    (void)s;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    moq_result_t arc = d18_fill_auth_tokens(&p, args->auth_tokens,
                                            args->auth_token_count);
    if (arc < 0) return arc;
    return moq_d18_encode_track_status(w, args->request_id,
                                       &args->track_namespace, args->track_name, &p);
}

/* TRACK_STATUS_OK encode (§10.14): a REQUEST_OK with LARGEST_OBJECT / EXPIRES
 * params and an opaque Track Properties tail (no Track Alias). */
static moq_result_t d18_encode_track_status_ok(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_track_status_ok_encode_args *args)
{
    (void)s;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    if (args->has_largest) {
        p.has_largest = true;
        p.largest_group = args->largest_group;
        p.largest_object = args->largest_object;
    }
    if (args->has_expires) {
        p.has_expires = true;
        p.expires_ms = args->expires_ms;
    }
    moq_bytes_t props = { args->track_properties, args->track_properties_len };
    return moq_d18_encode_track_status_ok(w, &p, props);
}

/* Decode an inbound SUBSCRIBE_NAMESPACE (0x50, full envelope) into the
 * draft-neutral request the ns_sub core processes. Namespace-only: interest is
 * reported as NAMESPACE and forward defaults to true (no draft-18 wire fields).
 * A malformed AUTHORIZATION_TOKEN closes the session with
 * KEY_VALUE_FORMATTING_ERROR (0x6, §10.2.2); the shared ns_sub handler respects
 * the already-closed session rather than re-closing with PROTOCOL_VIOLATION. */
static moq_result_t d18_decode_ns_sub_request(
    moq_session_t *s, const uint8_t *data, size_t len,
    moq_decoded_ns_sub_request_t *out)
{
    memset(out, 0, sizeof(*out));
    out->forward = true;
    out->namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_control_envelope_t env;
    moq_result_t rc = moq_d18_decode_envelope(&r, &env);
    if (rc == MOQ_ERR_BUFFER) return MOQ_ERR_BUFFER;
    if (rc < 0) return MOQ_ERR_PROTO;
    if (env.msg_type != MOQ_D18_SUBSCRIBE_NAMESPACE) return MOQ_ERR_PROTO;

    out->has_trailing_bytes = (moq_buf_reader_offset(&r) != len);

    moq_d18_subscribe_namespace_t sn;
    rc = moq_d18_decode_subscribe_namespace(env.payload, env.payload_len,
                                            out->parts_buf,
                                            MOQ_DECODED_NS_SUB_MAX_PARTS, &sn);
    if (rc == MOQ_D18_ERR_KVP_FORMAT)
        return close_with_error(s, 0x6, "malformed auth token structure");
    if (rc < 0) return MOQ_ERR_PROTO;

    out->request_id = sn.request_id;
    out->prefix = sn.track_namespace_prefix;   /* parts in out->parts_buf */

    /* Convert decoded wire tokens to the core's decoded-token form; the shared
     * ns_sub handler resolves them against the session token cache. */
    size_t n = sn.params.auth_token_count;
    for (size_t i = 0; i < n; i++) {
        const moq_d18_auth_token_t *t = &sn.params.auth_tokens[i];
        switch (t->alias_type) {
        case MOQ_AUTH_TOKEN_DELETE:    out->auth_tokens[i].op = MOQ_AUTH_OP_DELETE; break;
        case MOQ_AUTH_TOKEN_REGISTER:  out->auth_tokens[i].op = MOQ_AUTH_OP_REGISTER; break;
        case MOQ_AUTH_TOKEN_USE_ALIAS: out->auth_tokens[i].op = MOQ_AUTH_OP_USE_ALIAS; break;
        default:                       out->auth_tokens[i].op = MOQ_AUTH_OP_USE_VALUE; break;
        }
        out->auth_tokens[i].alias = t->alias;
        out->auth_tokens[i].token_type = t->token_type;
        out->auth_tokens[i].token_value = t->token_value.data;
        out->auth_tokens[i].token_value_len = t->token_value.len;
    }
    out->auth_token_count = n;
    return MOQ_OK;
}

/* Decode a message on a SUBSCRIBE_NAMESPACE response stream. The first message
 * must be REQUEST_OK or REQUEST_ERROR (§10.18: any other first message is a
 * PROTOCOL_VIOLATION); afterwards, NAMESPACE / NAMESPACE_DONE carry namespace
 * suffixes. The request id correlates by the bidi, so REQUEST_OK / REQUEST_ERROR
 * carry none (expected_request_id is unused). */
static moq_result_t d18_decode_ns_sub_response(
    moq_session_t *s, const uint8_t *data, size_t len,
    bool got_response, uint64_t expected_request_id,
    moq_decoded_ns_sub_response_t *out)
{
    (void)s;
    (void)expected_request_id;
    memset(out, 0, sizeof(*out));

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_control_envelope_t env;
    moq_result_t rc = moq_d18_decode_envelope(&r, &env);
    if (rc == MOQ_ERR_BUFFER) return MOQ_ERR_BUFFER;
    if (rc < 0) {
        out->close_reason = "malformed bidi response stream envelope";
        return MOQ_ERR_PROTO;
    }
    size_t consumed = moq_buf_reader_offset(&r);

    /* A request-stream GOAWAY (§10.4) may arrive in any state of the ns_sub bidi;
     * it migrates the subscription. Decode it state-agnostically. */
    if (env.msg_type == MOQ_D18_GOAWAY) {
        moq_d18_goaway_t ga;
        rc = moq_d18_decode_goaway_request(env.payload, env.payload_len, &ga);
        if (rc < 0) {
            out->close_reason = "malformed request-stream GOAWAY";
            return MOQ_ERR_PROTO;
        }
        out->kind = MOQ_NS_RESP_GOAWAY;
        out->consumed = consumed;
        out->has_trailing_bytes = (consumed < len);
        out->goaway_uri = ga.uri.data;
        out->goaway_uri_len = ga.uri.len;
        out->goaway_timeout_ms = ga.timeout_ms;
        return MOQ_OK;
    }

    if (!got_response) {
        /* First response: must be REQUEST_OK or REQUEST_ERROR. */
        switch (env.msg_type) {
        case MOQ_D18_REQUEST_OK:
            rc = moq_d18_decode_request_ok(env.payload, env.payload_len);
            if (rc < 0) {
                out->close_reason = "malformed REQUEST_OK on bidi stream";
                return MOQ_ERR_PROTO;
            }
            out->kind = MOQ_NS_RESP_OK;
            out->consumed = consumed;
            return MOQ_OK;
        case MOQ_D18_REQUEST_ERROR: {
            out->has_trailing_bytes = (consumed < len);
            moq_d18_request_error_t err;
            moq_d18_redirect_t rd;
            rc = moq_d18_decode_request_error_redirect(env.payload,
                     env.payload_len, out->parts_buf, MOQ_DECODED_NS_SUB_MAX_PARTS,
                     &err, &rd);
            if (rc < 0) {
                out->close_reason = "malformed REQUEST_ERROR on bidi stream";
                return MOQ_ERR_PROTO;
            }
            out->kind = MOQ_NS_RESP_ERROR;
            out->consumed = consumed;
            out->error_code = err.error_code;
            out->retry_interval = err.retry_interval;
            out->reason = err.reason.data;
            out->reason_len = err.reason.len;
            if (err.error_code == MOQ_D18_ERROR_REDIRECT) {
                out->has_redirect = true;
                out->redirect.connect_uri = rd.connect_uri.data;
                out->redirect.connect_uri_len = rd.connect_uri.len;
                out->redirect.track_namespace = rd.track_namespace; /* parts_buf */
                out->redirect.track_name = rd.track_name.data;
                out->redirect.track_name_len = rd.track_name.len;
            }
            return MOQ_OK;
        }
        case MOQ_D18_NAMESPACE:
        case MOQ_D18_NAMESPACE_DONE:
            out->close_reason = "NAMESPACE/NAMESPACE_DONE before REQUEST_OK";
            return MOQ_ERR_PROTO;
        default:
            out->close_reason = "unexpected message type on bidi response stream";
            return MOQ_ERR_PROTO;
        }
    } else {
        /* Post-OK: NAMESPACE or NAMESPACE_DONE. */
        switch (env.msg_type) {
        case MOQ_D18_NAMESPACE:
        case MOQ_D18_NAMESPACE_DONE: {
            moq_namespace_t suffix;
            rc = moq_d18_decode_namespace_msg(env.payload, env.payload_len,
                                              out->parts_buf,
                                              MOQ_DECODED_NS_SUB_MAX_PARTS,
                                              &suffix);
            if (rc < 0) {
                out->close_reason = "malformed namespace suffix on bidi stream";
                return MOQ_ERR_PROTO;
            }
            out->suffix = suffix;   /* parts in out->parts_buf */
            out->kind = (env.msg_type == MOQ_D18_NAMESPACE)
                ? MOQ_NS_RESP_NAMESPACE : MOQ_NS_RESP_NAMESPACE_DONE;
            out->consumed = consumed;
            return MOQ_OK;
        }
        case MOQ_D18_REQUEST_OK:
        case MOQ_D18_REQUEST_ERROR:
            out->close_reason = "duplicate response on bidi stream";
            return MOQ_ERR_PROTO;
        default:
            out->close_reason = "unexpected message type on bidi response stream";
            return MOQ_ERR_PROTO;
        }
    }
}

/* Decode and dispatch an inbound message buffered on a request bidi stream: the
 * first request (SUBSCRIBE / FETCH / PUBLISH_NAMESPACE / TRACK_STATUS /
 * SUBSCRIBE_NAMESPACE / SUBSCRIBE_TRACKS) or a post-establishment lifecycle
 * message (REQUEST_UPDATE) on an already-committed bidi. The core owns the
 * reserved slot and buffering. */
/* Map an internal request kind to the public migration family (0 if the kind has
 * no request bidi / is not eligible for a request-stream GOAWAY here). */
static moq_request_family_t d18_family_from_req_kind(uint32_t kind)
{
    switch (kind) {
    case MOQ_REQ_SUBSCRIPTION:     return MOQ_REQUEST_FAMILY_SUBSCRIBE;
    case MOQ_REQ_FETCH:            return MOQ_REQUEST_FAMILY_FETCH;
    case MOQ_REQ_TRACK_STATUS:     return MOQ_REQUEST_FAMILY_TRACK_STATUS;
    case MOQ_REQ_ANNOUNCEMENT:     return MOQ_REQUEST_FAMILY_ANNOUNCEMENT;
    case MOQ_REQ_NAMESPACE_SUB:    return MOQ_REQUEST_FAMILY_NS_SUB;
    case MOQ_REQ_PUBLISH:          return MOQ_REQUEST_FAMILY_PUBLISH;
    case MOQ_REQ_SUBSCRIBE_TRACKS: return MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS;
    default:                       return (moq_request_family_t)0;
    }
}

/* Handle a GOAWAY (§10.4) decoded on an established request bidi: validate
 * (no trailing bytes; a server must not receive a non-zero New Session URI),
 * then migrate + free + strict-drain via the draft-neutral core. `r` is the
 * dispatcher's reader positioned just past the GOAWAY envelope. */
static moq_result_t d18_request_goaway(moq_session_t *s, moq_stream_ref_t ref,
    moq_request_family_t family, int slot,
    const moq_control_envelope_t *env, const moq_buf_reader_t *r,
    size_t len, size_t *out_consumed)
{
    moq_d18_goaway_t ga;
    moq_result_t rc = moq_d18_decode_goaway_request(env->payload,
                                                    env->payload_len, &ga);
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed request-stream GOAWAY");
    size_t off = moq_buf_reader_offset(r);
    if (off < len)   /* GOAWAY is terminal on this bidi; nothing may follow it */
        return close_with_error(s, 0x3, "extra bytes after GOAWAY");
    if (s->perspective == MOQ_PERSPECTIVE_SERVER && ga.uri.len > 0)
        return close_with_error(s, 0x3,
            "server received GOAWAY with non-zero New Session URI");
    rc = session_core_on_request_goaway(s, family, slot, ref, ga.uri.data,
                                        ga.uri.len, ga.timeout_ms);
    if (rc < 0)
        return rc;
    if (s->state == MOQ_SESS_CLOSED)
        return MOQ_OK;
    *out_consumed = off;
    return MOQ_OK;
}

static moq_result_t d18_process_request_stream(
    moq_session_t *s, moq_stream_ref_t ref, int slot,
    const uint8_t *buf, size_t len, bool fin, size_t *out_consumed)
{
    *out_consumed = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, buf, len);
    moq_control_envelope_t env;
    moq_result_t rc = moq_d18_decode_envelope(&r, &env);
    if (rc == MOQ_ERR_BUFFER)
        return MOQ_OK;            /* incomplete; the core waits / handles FIN */
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed request envelope");

    /* A GOAWAY on a *committed* request bidi migrates that single request (§10.4).
     * It is valid only once the request owns the bidi; as a first message on a
     * fresh staging stream (no committed request) it is a protocol violation. */
    if (env.msg_type == MOQ_D18_GOAWAY) {
        moq_request_endpoint_t gep = request_registry_find_by_streamref(s, ref);
        moq_request_family_t fam = d18_family_from_req_kind((uint32_t)gep.kind);
        bool committed = (fam != (moq_request_family_t)0) &&
            !(gep.kind == MOQ_REQ_SUBSCRIPTION &&
              s->subs[gep.slot].state == MOQ_SUB_RECVING_REQUEST);
        if (!committed)
            return close_with_error(s, 0x3,
                "GOAWAY as first message on a request stream");
        return d18_request_goaway(s, ref, fam, gep.slot, &env, &r, len,
                                  out_consumed);
    }

    /* An established announce bidi (keyed ANNOUNCEMENT after the PUBLISH_NAMESPACE
     * handoff) carries only a REQUEST_UPDATE. Handle that kind definitively here,
     * before the first-message branches, so a peer cannot send SUBSCRIBE / FETCH /
     * PUBLISH_NAMESPACE on it and have the announce slot used as a subscription or
     * fetch slot (cross-pool corruption / out-of-bounds when ann_cap > sub_cap).
     * §10.9.1: a failed announce REQUEST_UPDATE closes the bidi; real
     * announce-update is unsupported, so reject + terminate. */
    {
        moq_request_endpoint_t aep = request_registry_find_by_streamref(s, ref);
        if (aep.kind == MOQ_REQ_ANNOUNCEMENT) {
            if (env.msg_type != MOQ_D18_REQUEST_UPDATE)
                return close_with_error(s, 0x3,
                    "unexpected message on announce request bidi");
            if (s->announcements[aep.slot].state != MOQ_ANN_ESTABLISHED ||
                s->announcements[aep.slot].role != MOQ_ANN_ROLE_RECEIVER)
                return close_with_error(s, 0x3,
                    "REQUEST_UPDATE on non-established announcement");
            moq_d18_request_update_t u;
            rc = moq_d18_decode_request_update(env.payload, env.payload_len, &u);
            if (rc == MOQ_D18_ERR_KVP_FORMAT)
                return close_with_error(s, 0x6, "malformed auth token structure");
            if (rc < 0)
                return close_with_error(s, 0x3, "malformed REQUEST_UPDATE");
            moq_request_endpoint_t uep;
            rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                     u.request_id, &uep);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            rc = session_core_on_announce_update_rejected(s, aep.slot, &uep);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            *out_consumed = moq_buf_reader_offset(&r);
            return MOQ_OK;
        }
    }

    /* An established/pending SUBSCRIBE_TRACKS publisher-side bidi (keyed
     * MOQ_REQ_SUBSCRIBE_TRACKS after the handoff) carries only a REQUEST_UPDATE
     * (re-prefix), which is deferred (§10.9.1): reject and close the bidi, not the
     * session. Handle it definitively here, before the first-message branches, so
     * a peer cannot reuse the established bidi as a fresh request. */
    {
        moq_request_endpoint_t tep = request_registry_find_by_streamref(s, ref);
        if (tep.kind == MOQ_REQ_SUBSCRIBE_TRACKS) {
            if (env.msg_type != MOQ_D18_REQUEST_UPDATE)
                return close_with_error(s, 0x3,
                    "unexpected message on subscribe-tracks request bidi");
            /* A REQUEST_UPDATE is only valid once the subscription is established
             * on the publisher side; one before the app accepts/rejects (or on
             * the subscriber side) is a protocol violation -- close, no mutation
             * (mirrors the announcement-update guard). */
            if (s->track_subs[tep.slot].state != MOQ_TRACK_SUB_ESTABLISHED ||
                s->track_subs[tep.slot].role != MOQ_TRACK_SUB_ROLE_PUBLISHER)
                return close_with_error(s, 0x3,
                    "REQUEST_UPDATE on non-established subscribe-tracks");
            moq_d18_request_update_t u;
            rc = moq_d18_decode_request_update(env.payload, env.payload_len, &u);
            if (rc == MOQ_D18_ERR_KVP_FORMAT)
                return close_with_error(s, 0x6, "malformed auth token structure");
            if (rc < 0)
                return close_with_error(s, 0x3, "malformed REQUEST_UPDATE");
            moq_request_endpoint_t uep;
            rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                     u.request_id, &uep);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            rc = session_core_on_track_sub_update_rejected(s, tep.slot, &uep);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            *out_consumed = moq_buf_reader_offset(&r);
            return MOQ_OK;
        }
    }

    /* An established PUBLISH bidi (keyed MOQ_REQ_PUBLISH after the handoff) is
     * read here only on the subscriber (responder) side: the publisher (opener)
     * sends the terminal PUBLISH_DONE, or a REQUEST_OK acknowledging our pending
     * REQUEST_UPDATE. Handle these definitively before the first-message branches
     * so a peer cannot reuse the established bidi as a fresh request. */
    {
        moq_request_endpoint_t pep = request_registry_find_by_streamref(s, ref);
        if (pep.kind == MOQ_REQ_PUBLISH) {
            moq_pub_entry_t *pe = &s->publishes[pep.slot];
            if (env.msg_type == MOQ_D18_PUBLISH_DONE) {
                if (pe->state != MOQ_PUB_ESTABLISHED)
                    return close_with_error(s, 0x3,
                        "PUBLISH_DONE on non-established publish");
                moq_d18_publish_done_t pd;
                rc = moq_d18_decode_publish_done(env.payload, env.payload_len,
                                                 &pd);
                if (rc < 0)
                    return close_with_error(s, 0x3, "malformed PUBLISH_DONE");
                /* PUBLISH_DONE is terminal: nothing but the FIN may follow. */
                if (moq_buf_reader_offset(&r) < len)
                    return close_with_error(s, 0x3,
                        "extra bytes after PUBLISH_DONE");
                /* A failed update mandates termination via PUBLISH_DONE(UPDATE_FAILED);
                 * any other status after an update failure is a violation. */
                if (pe->update_failed && pd.status_code != 0x8)
                    return close_with_error(s, 0x3,
                        "PUBLISH_DONE after update failure must be UPDATE_FAILED");
                moq_decoded_publish_done_t d;
                memset(&d, 0, sizeof(d));
                d.target_slot = pep.slot;
                d.status_code = pd.status_code;
                d.stream_count = pd.stream_count;
                d.reason = pd.reason.data;
                d.reason_len = pd.reason.len;
                rc = session_core_on_publish_done(s, &d, false /* drain FIN */);
                if (rc < 0)
                    return rc;
                if (s->state == MOQ_SESS_CLOSED)
                    return MOQ_OK;
                *out_consumed = moq_buf_reader_offset(&r);
                return MOQ_OK;
            }
            if (env.msg_type == MOQ_D18_REQUEST_OK &&
                pe->state == MOQ_PUB_ESTABLISHED && pe->update_pending) {
                /* The publisher acknowledged our REQUEST_UPDATE (stream-correlated,
                 * so no request id); clear the pending update. */
                rc = moq_d18_decode_request_ok(env.payload, env.payload_len);
                if (rc < 0)
                    return close_with_error(s, 0x3, "malformed REQUEST_OK");
                pe->update_pending = false;
                *out_consumed = moq_buf_reader_offset(&r);
                return MOQ_OK;
            }
            if (env.msg_type == MOQ_D18_REQUEST_ERROR &&
                pe->state == MOQ_PUB_ESTABLISHED && pe->update_pending) {
                /* The publisher rejected our REQUEST_UPDATE (§10.9): clear the
                 * pending update and require the terminating PUBLISH_DONE to carry
                 * UPDATE_FAILED. Further updates are blocked until then. */
                moq_d18_request_error_t er;
                rc = moq_d18_decode_request_error(env.payload, env.payload_len,
                                                  &er);
                if (rc < 0)
                    return close_with_error(s, 0x3, "malformed REQUEST_ERROR");
                pe->update_pending = false;
                pe->update_request_id = 0;
                pe->update_failed = true;
                *out_consumed = moq_buf_reader_offset(&r);
                return MOQ_OK;
            }
            return close_with_error(s, 0x3,
                "unexpected message on publish request bidi");
        }
    }

    /* The first message on a request stream is the request itself -- i.e. the
     * peer opening a NEW request. After our GOAWAY (§10.4) that is forbidden.
     * Messages on an already-committed bidi (GOAWAY / REQUEST_UPDATE /
     * PUBLISH_DONE / REQUEST_OK ...) were handled and returned above, so
     * reaching here with one of these first-message types means a fresh
     * request; refuse it as a protocol violation. */
    if (session_refuses_new_requests(s) &&
        (env.msg_type == MOQ_D18_SUBSCRIBE ||
         env.msg_type == MOQ_D18_PUBLISH ||
         env.msg_type == MOQ_D18_FETCH ||
         env.msg_type == MOQ_D18_PUBLISH_NAMESPACE ||
         env.msg_type == MOQ_D18_TRACK_STATUS ||
         env.msg_type == MOQ_D18_SUBSCRIBE_TRACKS ||
         env.msg_type == MOQ_D18_SUBSCRIBE_NAMESPACE))
        return close_with_error(s, 0x3, "new request after local GOAWAY");

    /* The first message on a request stream is the request itself. */
    if (env.msg_type == MOQ_D18_SUBSCRIBE) {
        moq_decoded_subscribe_t d;
        memset(&d, 0, sizeof(d));
        moq_d18_subscribe_t sub;
        rc = moq_d18_decode_subscribe(env.payload, env.payload_len,
                                      d.track_namespace_parts,
                                      MOQ_DECODED_MAX_NAMESPACE_PARTS, &sub);
        if (rc == MOQ_D18_ERR_KVP_FORMAT)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed SUBSCRIBE");

        d.track_namespace = sub.track_namespace; /* parts = d.track_namespace_parts */
        d.track_name = sub.track_name;
        d.request_id = sub.request_id;
        /* Surface the decoded Message Parameters (defaults apply when omitted)
         * so the publisher/relay sees the real request. */
        d.subscriber_priority = sub.params.has_subscriber_priority
            ? sub.params.subscriber_priority : 128;
        d.group_order = sub.params.has_group_order
            ? sub.params.group_order : MOQ_GROUP_ORDER_DEFAULT;
        d.has_forward = true;
        d.forward = sub.params.has_forward ? (sub.params.forward != 0) : true;
        d.has_filter = sub.params.has_filter;
        d.filter_type = sub.params.has_filter
            ? sub.params.filter_type : MOQ_SUBSCRIBE_FILTER_NONE;
        d.start_group = sub.params.filter_start_group;
        d.start_object = sub.params.filter_start_object;
        d.end_group = sub.params.filter_end_group;
        if (d18_map_delivery_timeout(&sub.params, &d.has_delivery_timeout,
                                     &d.delivery_timeout_us) < 0)
            return close_with_error(s, 0x3,
                "inconsistent delivery timeout parameters");
        d.has_new_group_request = sub.params.has_new_group_request;
        d.new_group_request = sub.params.new_group_request;

        rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                 sub.request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* validate failed closed (bad request id) */

        /* Resolve AUTHORIZATION_TOKEN parameters against the session token cache
         * (fatal errors close; a message-level reject is carried in
         * d.auth_reject_code for the core to turn into REQUEST_ERROR). */
        rc = d18_resolve_auth_tokens(s, &sub.params, d.tokens, &d.token_count,
                                     d.token_staged, &d.auth_txn,
                                     &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */

        rc = session_core_on_subscribe(s, &d, slot);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* core failed/closed (e.g. duplicate) */
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    /* PUBLISH (§10.10): a publisher-initiated subscription. The first message on
     * the bidi; the subscriber (us) replies PUBLISH_OK / REQUEST_ERROR. The core
     * hands the request off from the staging slot to the publishes pool, re-keying
     * the bidi to MOQ_REQ_PUBLISH. */
    if (env.msg_type == MOQ_D18_PUBLISH) {
        moq_decoded_publish_t d;
        memset(&d, 0, sizeof(d));
        moq_d18_publish_t pub;
        rc = moq_d18_decode_publish(env.payload, env.payload_len,
                                    d.track_namespace_parts,
                                    MOQ_DECODED_MAX_NAMESPACE_PARTS, &pub);
        if (rc == MOQ_D18_ERR_KVP_FORMAT)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed PUBLISH");

        d.track_namespace = pub.track_namespace; /* parts = d.track_namespace_parts */
        d.track_name = pub.track_name;
        d.request_id = pub.request_id;
        d.track_alias = pub.track_alias;
        /* Omitted FORWARD means forward = 1 (§5.4.1). */
        d.has_forward = true;
        d.forward = pub.params.has_forward ? (pub.params.forward != 0) : true;
        d.track_properties = pub.track_properties.data;
        d.track_properties_len = pub.track_properties.len;
        d.track_properties_unsupported = pub.track_properties_unsupported;
        d.dynamic_groups = pub.dynamic_groups;

        rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                 pub.request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;

        rc = d18_resolve_auth_tokens(s, &pub.params, d.tokens, &d.token_count,
                                     d.token_staged, &d.auth_txn,
                                     &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */

        rc = session_core_on_publish(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* core failed closed (e.g. duplicate alias) */
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    if (env.msg_type == MOQ_D18_FETCH) {
        moq_decoded_fetch_t fd;
        memset(&fd, 0, sizeof(fd));
        fd.joining_sub_slot = -1;
        moq_d18_fetch_t f;
        rc = moq_d18_decode_fetch(env.payload, env.payload_len,
                                  fd.track_namespace_parts,
                                  MOQ_DECODED_MAX_NAMESPACE_PARTS, &f);
        if (rc == MOQ_D18_ERR_KVP_FORMAT)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed FETCH");

        fd.request_id = f.request_id;
        fd.fetch_type = (uint32_t)f.fetch_type;
        fd.track_namespace = f.track_namespace; /* parts = fd.track_namespace_parts */
        fd.track_name = f.track_name;
        fd.start_group = f.start.group;
        fd.start_object = f.start.object;
        fd.end_group = f.end.group;
        fd.end_object = f.end.object;
        fd.joining_request_id = f.joining_request_id;
        fd.joining_start = f.joining_start;
        fd.subscriber_priority = f.params.has_subscriber_priority
            ? f.params.subscriber_priority : 128;
        /* Omitted GROUP_ORDER on a FETCH means Ascending (§10.2.8). */
        fd.group_order = f.params.has_group_order
            ? f.params.group_order : MOQ_GROUP_ORDER_ASCENDING;

        rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                 f.request_id, &fd.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;

        rc = d18_resolve_auth_tokens(s, &f.params, fd.tokens, &fd.token_count,
                                     fd.token_staged, &fd.auth_txn,
                                     &fd.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */

        fd.request_fin = fin;     /* request bidi FIN'd in this chunk? */
        rc = session_core_on_fetch(s, &fd);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* core failed closed (e.g. joining/pool-full) */
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    if (env.msg_type == MOQ_D18_PUBLISH_NAMESPACE) {
        moq_decoded_publish_namespace_t d;
        memset(&d, 0, sizeof(d));
        moq_d18_publish_namespace_t pn;
        rc = moq_d18_decode_publish_namespace(env.payload, env.payload_len,
                                              d.track_namespace_parts,
                                              MOQ_DECODED_MAX_NAMESPACE_PARTS, &pn);
        if (rc == MOQ_D18_ERR_KVP_FORMAT)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed PUBLISH_NAMESPACE");

        d.track_namespace = pn.track_namespace; /* parts = d.track_namespace_parts */
        d.request_id = pn.request_id;

        rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                 pn.request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;

        rc = d18_resolve_auth_tokens(s, &pn.params, d.tokens, &d.token_count,
                                     d.token_staged, &d.auth_txn,
                                     &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */

        rc = session_core_on_publish_namespace(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    if (env.msg_type == MOQ_D18_TRACK_STATUS) {
        moq_decoded_track_status_request_t d;
        memset(&d, 0, sizeof(d));
        moq_d18_track_status_t ts;
        rc = moq_d18_decode_track_status(env.payload, env.payload_len,
                                         d.track_namespace_parts,
                                         MOQ_DECODED_MAX_NAMESPACE_PARTS, &ts);
        if (rc == MOQ_D18_ERR_KVP_FORMAT)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed TRACK_STATUS");

        d.track_namespace = ts.track_namespace; /* parts = d.track_namespace_parts */
        d.track_name = ts.track_name;

        rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                 ts.request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;

        rc = d18_resolve_auth_tokens(s, &ts.params, d.tokens, &d.token_count,
                                     d.token_staged, &d.auth_txn,
                                     &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */

        rc = session_core_on_track_status_request(s, &d, fin);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        /* TRACK_STATUS is the first and only message; the requester's FIN rides
         * this chunk. Record it on the committed entry so accept/reject need not
         * leave a drain ref for a FIN that already arrived. */
        if (fin) {
            moq_request_endpoint_t te =
                request_registry_find_by_streamref(s, ref);
            if (te.kind == MOQ_REQ_TRACK_STATUS)
                s->track_statuses[te.slot].req_recv_fin = true;
        }
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    if (env.msg_type == MOQ_D18_SUBSCRIBE_TRACKS) {
        moq_decoded_subscribe_tracks_request_t d;
        memset(&d, 0, sizeof(d));
        moq_d18_subscribe_tracks_t st;
        rc = moq_d18_decode_subscribe_tracks(env.payload, env.payload_len,
                                             d.prefix_parts,
                                             MOQ_DECODED_MAX_NAMESPACE_PARTS, &st);
        if (rc == MOQ_D18_ERR_KVP_FORMAT)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed SUBSCRIBE_TRACKS");

        d.track_namespace_prefix = st.track_namespace_prefix; /* parts = d.prefix_parts */
        d.forward = st.params.has_forward ? (st.params.forward != 0) : true;

        rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                 st.request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;

        rc = d18_resolve_auth_tokens(s, &st.params, d.tokens, &d.token_count,
                                     d.token_staged, &d.auth_txn,
                                     &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */

        rc = session_core_on_subscribe_tracks(s, &d, fin);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    if (env.msg_type == MOQ_D18_SUBSCRIBE_NAMESPACE) {
        /* Hand the request off to the namespace-sub pool, then drive the shared
         * ns_sub commit. Buffer the full message into a fresh ns_sub entry (keyed
         * in idx_ns_by_ref, which owns the bidi from here) and process it; the
         * core releases this generic staging slot once the bytes are reported
         * consumed. A WOULD_BLOCK keeps the staging buffer and the (already
         * created) ns_sub entry for a re-feed -- the idx_ns_by_ref lookup makes
         * the create idempotent. The decode (and any malformed-token close) lives
         * in the ns_sub profile op driven by the shared handler. */
        size_t msg_len = moq_buf_reader_offset(&r);
        int32_t ns_slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        if (ns_slot < 0) {
            rc = ns_sub_on_new_bidi(s, ref, buf, msg_len);
            if (rc == MOQ_ERR_BUFFER)
                return close_with_error(s, 0x3,
                    "SUBSCRIBE_NAMESPACE too large for buffer");
            if (rc < 0)
                return rc;            /* WOULD_BLOCK: ns_sub pool full; nothing
                                       * consumed, staging retained for re-feed */
            ns_slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        }
        /* The message bytes are now owned by the ns_sub entry, so report them
         * consumed even if processing blocks. This lets the core's handoff cleanup
         * detect (and reject) any trailing bytes the same chunk carried after the
         * SUBSCRIBE_NAMESPACE envelope, on the WOULD_BLOCK path as on the OK path. */
        *out_consumed = msg_len;
        rc = ns_sub_process_recving_publisher(s, ns_slot, fin);
        if (rc < 0)
            return rc;                /* WOULD_BLOCK keeps the entry for a re-feed */
        return MOQ_OK;
    }

    /* REQUEST_UPDATE arrives on an established subscription's request bidi (the
     * core passes the committed slot). It consumes a fresh peer request id; the
     * response correlates by the stream, so the target is the slot, not a
     * by-id lookup. It is not a valid first message on a fresh request bidi (a
     * staging slot is still RECVING) -- accept it only once the subscription is
     * established on the publisher side; otherwise it falls through to the
     * unexpected-first-message close below. */
    if (env.msg_type == MOQ_D18_REQUEST_UPDATE &&
        s->subs[slot].state == MOQ_SUB_ESTABLISHED &&
        s->subs[slot].role == MOQ_SUB_ROLE_PUBLISHER) {
        moq_d18_request_update_t u;
        rc = moq_d18_decode_request_update(env.payload, env.payload_len, &u);
        if (rc == MOQ_D18_ERR_KVP_FORMAT)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_UPDATE");

        moq_request_endpoint_t ep;
        rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                 u.request_id, &ep);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;

        moq_decoded_request_update_t d;
        memset(&d, 0, sizeof(d));
        d.endpoint = ep;
        d.request_id = u.request_id;
        d.target_kind = MOQ_REQ_SUBSCRIPTION;
        d.target_slot = slot;
        d.has_forward = u.params.has_forward;
        d.forward = u.params.forward != 0;
        d.has_subscriber_priority = u.params.has_subscriber_priority;
        d.subscriber_priority = u.params.subscriber_priority;
        if (d18_map_delivery_timeout(&u.params, &d.has_delivery_timeout,
                                     &d.delivery_timeout_us) < 0)
            return close_with_error(s, 0x3,
                "inconsistent delivery timeout parameters");
        d.has_new_group_request = u.params.has_new_group_request;
        d.new_group_request = u.params.new_group_request;

        rc = d18_resolve_auth_tokens(s, &u.params, d.tokens, &d.token_count,
                                     d.token_staged, &d.auth_txn,
                                     &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */

        rc = session_core_on_request_update(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    return close_with_error(s, 0x3, "unexpected first request message");
}

/* SUBSCRIBE_OK encode. The bidi stream correlates the response, so no request
 * id is carried. LARGEST_OBJECT / EXPIRES travel as Message Parameters; the
 * Track Properties tail is carried opaquely (validated by the codec). */
static moq_result_t d18_encode_subscribe_ok(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_subscribe_ok_encode_args *args)
{
    (void)s;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    if (args->has_largest) {
        p.has_largest = true;
        p.largest_group = args->largest_group;
        p.largest_object = args->largest_object;
    }
    if (args->has_expires) {
        p.has_expires = true;
        p.expires_ms = args->expires_ms;
    }
    moq_bytes_t props = { args->track_properties, args->track_properties_len };
    return moq_d18_encode_subscribe_ok(w, args->track_alias, &p, props);
}

/* REQUEST_ERROR encode. No request id (stream-correlated); retry_interval is 0
 * when retry is not offered. The optional Redirect structure (§10.6.1) is encoded
 * when args->has_redirect (an all-empty Redirect is valid). */
static moq_result_t d18_encode_request_error(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_request_error_encode_args *args)
{
    (void)s;
    moq_bytes_t reason = { args->reason, args->reason_len };
    uint64_t retry = args->can_retry ? args->retry_after_ms : 0;
    if (args->has_redirect) {
        moq_d18_redirect_t rd = {
            .connect_uri     = { args->connect_uri, args->connect_uri_len },
            .track_namespace = args->redirect_namespace,
            .track_name      = { args->redirect_track_name,
                                 args->redirect_track_name_len },
        };
        return moq_d18_encode_request_error_redirect(w, args->error_code, retry,
                                                     reason, &rd);
    }
    return moq_d18_encode_request_error(w, args->error_code, retry, reason);
}

static moq_result_t d18_encode_request_goaway(
    moq_session_t *s, struct moq_buf_writer *w,
    const uint8_t *uri, size_t uri_len, uint64_t timeout_ms)
{
    (void)s;
    return moq_d18_encode_goaway_request(w, uri, uri_len, timeout_ms);
}

/* REQUEST_UPDATE encode (subscription update on the request bidi). Carries a
 * fresh request id and the representable parameters (FORWARD, SUBSCRIBER_-
 * PRIORITY, delivery timeout). The single semantic delivery timeout maps onto
 * both draft-18 carriers (OBJECT_ and SUBGROUP_DELIVERY_TIMEOUT) with the same
 * millisecond value. */
static moq_result_t d18_encode_request_update(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_request_update_encode_args *args)
{
    (void)s;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_forward = args->has_forward;
    p.forward = args->forward ? 1 : 0;
    p.has_subscriber_priority = args->has_subscriber_priority;
    p.subscriber_priority = args->subscriber_priority;
    if (args->has_delivery_timeout) {
        uint64_t ms = args->delivery_timeout_us / 1000;
        p.has_object_delivery_timeout = true;
        p.object_delivery_timeout_ms = ms;
        p.has_subgroup_delivery_timeout = true;
        p.subgroup_delivery_timeout_ms = ms;
    }
    moq_result_t arc = d18_fill_auth_tokens(&p, args->auth_tokens,
                                            args->auth_token_count);
    if (arc < 0) return arc;
    p.has_new_group_request = args->has_new_group_request;
    p.new_group_request = args->new_group_request;
    return moq_d18_encode_request_update(w, args->request_id, &p);
}

/* REQUEST_OK encode. The bidi stream correlates the response, so no request id
 * is carried; REQUEST_UPDATE_OK has no parameters and empty Track Properties. */
static moq_result_t d18_encode_request_ok(
    moq_session_t *s, struct moq_buf_writer *w, uint64_t request_id)
{
    (void)s; (void)request_id;
    return moq_d18_encode_request_ok(w);
}

/* PUBLISH_DONE encode. The bidi stream correlates the subscription, so no
 * request id is carried; the caller FINs the stream after it. */
static moq_result_t d18_encode_publish_done(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_finish_publish_encode_args *args)
{
    (void)s;
    moq_bytes_t reason = { args->reason, args->reason_len };
    return moq_d18_encode_publish_done(w, args->status_code, args->stream_count,
                                       reason);
}

/* FETCH encode (standalone + joining, §10.12). SUBSCRIBER_PRIORITY / GROUP_ORDER
 * and authorization tokens travel as Message Parameters. */
static moq_result_t d18_encode_fetch_op(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_fetch_encode_args *args)
{
    (void)s;
    if (args->fetch_type < 1 || args->fetch_type > 3)
        return MOQ_ERR_INVAL;
    moq_d18_fetch_t f;
    memset(&f, 0, sizeof(f));
    f.request_id = args->request_id;
    f.fetch_type = args->fetch_type;
    if (args->fetch_type == MOQ_D18_FETCH_TYPE_STANDALONE) {
        f.track_namespace = args->track_namespace;
        f.track_name = args->track_name;
        f.start = (moq_d18_location_t){ args->start_group, args->start_object };
        f.end = (moq_d18_location_t){ args->end_group, args->end_object };
    } else {
        /* Joining Fetch (relative=2 / absolute=3, §10.12.2): Joining Request ID +
         * Joining Start; the publisher derives namespace/name/end from the sub. */
        f.joining_request_id = args->joining_request_id;
        f.joining_start = args->joining_start;
    }
    /* FETCH carries no FORWARD parameter. */
    d18_fill_request_params(&f.params, args->subscriber_priority,
                            false, false, args->group_order);
    moq_result_t arc = d18_fill_auth_tokens(&f.params, args->auth_tokens,
                                            args->auth_token_count);
    if (arc < 0) return arc;
    return moq_d18_encode_fetch(w, &f);
}

/* FETCH_OK encode. The bidi stream correlates the response, so no request id is
 * carried; the Track Properties tail is carried opaquely (validated by the
 * codec). */
static moq_result_t d18_encode_fetch_ok_op(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_accept_fetch_encode_args *args)
{
    (void)s;
    moq_d18_location_t end = { args->end_group, args->end_object };
    moq_bytes_t props = { args->track_properties, args->track_properties_len };
    return moq_d18_encode_fetch_ok(w, args->end_of_track, end, props);
}

/* -- Fetch data plane (draft-18 §11.4.4) --------------------------- *
 * FETCH_HEADER (Request ID) leads a fetch data uni; objects follow with a vi64
 * Serialization Flags field. Object Properties (0x20) and the Datagram forwarding-
 * preference bit (0x40, §11.4.4.1 -- the subgroup LSBs are ignored) are supported
 * in both directions. Group reconstruction is ascending-only; the session refuses
 * a DESCENDING fetch request via the fetch_descending_supported capability. */
#define D18_FETCH_OBJ_SUBGROUP_MASK  0x03u
#define D18_FETCH_OBJ_OBJECT_DELTA   0x04u
#define D18_FETCH_OBJ_GROUP_DELTA    0x08u
#define D18_FETCH_OBJ_PRIORITY       0x10u
#define D18_FETCH_OBJ_PROPERTIES     0x20u
#define D18_FETCH_OBJ_DATAGRAM       0x40u
#define D18_FETCH_END_NON_EXISTENT   0x8Cu
#define D18_FETCH_END_UNKNOWN        0x10Cu

static moq_result_t d18_decode_fetch_header_op(
    moq_session_t *s, moq_buf_reader_t *r,
    moq_decoded_fetch_stream_header_t *out)
{
    (void)s;
    return moq_d18_decode_fetch_header(r, &out->request_id);
}

static moq_result_t d18_decode_fetch_object_op(
    moq_session_t *s, moq_buf_reader_t *r,
    const moq_fetch_prior_object_t *prev, moq_decoded_fetch_object_t *out)
{
    (void)s;
    memset(out, 0, sizeof(*out));

    uint64_t flags;
    if (moq_buf_read_vi64(r, &flags) < 0) return MOQ_ERR_BUFFER;

    if (flags == D18_FETCH_END_NON_EXISTENT || flags == D18_FETCH_END_UNKNOWN) {
        out->is_range_marker = true;
        out->range_kind = (flags == D18_FETCH_END_NON_EXISTENT)
            ? MOQ_FETCH_RANGE_NON_EXISTENT : MOQ_FETCH_RANGE_UNKNOWN;
        if (moq_buf_read_vi64(r, &out->group_id) < 0) return MOQ_ERR_BUFFER;
        if (moq_buf_read_vi64(r, &out->object_id) < 0) return MOQ_ERR_BUFFER;
        return MOQ_OK;
    }
    if (flags > 0x7F) return MOQ_ERR_PROTO;            /* other big values invalid */

    bool has_group = (flags & D18_FETCH_OBJ_GROUP_DELTA) != 0;
    bool has_object = (flags & D18_FETCH_OBJ_OBJECT_DELTA) != 0;
    bool has_priority = (flags & D18_FETCH_OBJ_PRIORITY) != 0;
    /* §11.4.4.1: the Datagram bit (0x40) marks an object whose original forwarding
     * preference was Datagram -- it has no subgroup, so the two LSBs MUST be ignored
     * (treated as Subgroup ID zero). Surface the preference; the subgroup stays 0. */
    out->datagram = (flags & D18_FETCH_OBJ_DATAGRAM) != 0;
    uint8_t sg_mode = out->datagram
        ? 0u : (uint8_t)(flags & D18_FETCH_OBJ_SUBGROUP_MASK);

    /* "Prior location" (group/object) may come from a prior object OR an
     * End-of-Range marker; "prior actual metadata" (subgroup/priority) only
     * from an actual object. References to each must have the matching prior. */
    bool has_loc = prev && prev->has_prev;
    bool has_act = prev && prev->has_actual;
    bool refs_prior_subgroup = (sg_mode == 0x01 || sg_mode == 0x02);

    /* The first object (no prior location) must carry both deltas and must not
     * reference any prior-object field (priority or subgroup). */
    if (!has_loc) {
        if (!has_group || !has_object || !has_priority || refs_prior_subgroup)
            return MOQ_ERR_PROTO;
    }
    /* A present group delta requires the (absolute) object id alongside it. */
    if (has_group && !has_object) return MOQ_ERR_PROTO;
    /* References to prior actual-object metadata require such an object. */
    if (refs_prior_subgroup && !has_act) return MOQ_ERR_PROTO;
    if (!has_priority && !has_act) return MOQ_ERR_PROTO;

    uint64_t pg = has_loc ? prev->group_id : 0;
    uint64_t po = has_loc ? prev->object_id : 0;
    uint64_t psg = has_act ? prev->subgroup_id : 0;
    uint8_t  ppr = has_act ? prev->publisher_priority : 0;

    uint64_t group_delta = 0, obj_delta = 0, subgroup = 0;
    if (has_group && moq_buf_read_vi64(r, &group_delta) < 0) return MOQ_ERR_BUFFER;
    if (sg_mode == 0x03 && moq_buf_read_vi64(r, &subgroup) < 0) return MOQ_ERR_BUFFER;
    if (has_object && moq_buf_read_vi64(r, &obj_delta) < 0) return MOQ_ERR_BUFFER;
    uint8_t priority = ppr;
    if (has_priority) {
        if (moq_buf_reader_remaining(r) < 1) return MOQ_ERR_BUFFER;
        priority = *moq_buf_reader_ptr(r);
        r->pos += 1;
    }
    /* Object Properties (§11.4.4): a length-prefixed KVP block between the priority
     * byte and the payload length. Surface the borrowed bytes; reject a malformed
     * block or a Mandatory Track Property carried as an object property. */
    if (flags & D18_FETCH_OBJ_PROPERTIES) {
        uint64_t props_len;
        if (moq_buf_read_vi64(r, &props_len) < 0) return MOQ_ERR_BUFFER;
        if ((size_t)props_len > moq_buf_reader_remaining(r)) return MOQ_ERR_BUFFER;
        if (props_len > 0) {
            const uint8_t *pp = moq_buf_reader_ptr(r);
            moq_result_t prc = moq_d18_validate_properties(pp, (size_t)props_len);
            if (prc < 0) return prc;
            out->properties = pp;
            out->properties_len = (size_t)props_len;
        }
        r->pos += (size_t)props_len;
    }
    uint64_t payload_len;
    if (moq_buf_read_vi64(r, &payload_len) < 0) return MOQ_ERR_BUFFER;
    /* The object payload follows inline; consume it so the caller can read it
     * from (reader end - payload_len) and the reader is positioned at the next
     * object. Wait for more bytes if the payload is not fully buffered. */
    if (payload_len > moq_buf_reader_remaining(r)) return MOQ_ERR_BUFFER;
    r->pos += (size_t)payload_len;

    uint64_t group_id, object_id;
    if (has_group) {
        if (!has_loc) {
            group_id = group_delta;
        } else {
            /* Ascending order (the only order this profile emits/accepts). */
            if (group_delta > UINT64_MAX - 1 - pg) return MOQ_ERR_PROTO;
            group_id = pg + group_delta + 1;
        }
        object_id = obj_delta;                 /* absolute within the new group */
    } else {
        group_id = pg;
        if (has_object) {
            if (obj_delta > UINT64_MAX - po) return MOQ_ERR_PROTO;
            object_id = po + obj_delta;
        } else {
            if (po == UINT64_MAX) return MOQ_ERR_PROTO;
            object_id = po + 1;
        }
    }
    switch (sg_mode) {
    case 0x00: subgroup = 0; break;
    case 0x01: subgroup = psg; break;
    case 0x02:
        if (psg == UINT64_MAX) return MOQ_ERR_PROTO;   /* would wrap */
        subgroup = psg + 1;
        break;
    default:   break;                          /* 0x03: read above */
    }

    out->group_id = group_id;
    out->object_id = object_id;
    out->subgroup_id = subgroup;
    out->publisher_priority = priority;
    out->payload_len = payload_len;
    return MOQ_OK;
}

static moq_result_t d18_encode_fetch_object_op(
    moq_session_t *s, moq_buf_writer_t *w,
    const moq_fetch_object_encode_args_t *args,
    const moq_fetch_prior_object_t *prev)
{
    (void)s;
    bool has_loc = prev && prev->has_prev;
    bool has_act = prev && prev->has_actual;
    /* Always carry priority so an object never references prior-object priority
     * (which the first object/post-marker object must not do). */
    uint64_t flags = D18_FETCH_OBJ_PRIORITY;
    if (args->properties_len > 0)
        flags |= D18_FETCH_OBJ_PROPERTIES;
    uint64_t group_delta = 0, obj_delta = 0;
    uint8_t sg_mode;

    if (args->datagram) {
        /* §11.4.4.1: a Datagram forwarding-preference object has no subgroup; set
         * the bit and the two LSBs to zero (no subgroup field is written, and
         * cfg->subgroup_id is ignored). */
        flags |= D18_FETCH_OBJ_DATAGRAM;
        sg_mode = 0x00u;
    } else if (has_act && args->subgroup_id == prev->subgroup_id) {
        /* Prior-subgroup modes (0x01/0x02) require prior actual-object metadata and
         * must not wrap; otherwise emit the explicit subgroup (or zero). */
        sg_mode = 0x01u;
    } else if (has_act && prev->subgroup_id != UINT64_MAX &&
               args->subgroup_id == prev->subgroup_id + 1) {
        sg_mode = 0x02u;
    } else if (args->subgroup_id == 0) {
        sg_mode = 0x00u;
    } else {
        sg_mode = 0x03u;
    }
    flags |= sg_mode;

    bool new_group = !has_loc || args->group_id != prev->group_id;
    if (new_group) {
        if (has_loc && args->group_id <= prev->group_id)
            return MOQ_ERR_INVAL;              /* ascending only (MVP) */
        flags |= D18_FETCH_OBJ_GROUP_DELTA | D18_FETCH_OBJ_OBJECT_DELTA;
        group_delta = has_loc ? args->group_id - prev->group_id - 1
                              : args->group_id;
        obj_delta = args->object_id;           /* absolute within the group */
    } else if (args->object_id != prev->object_id + 1) {
        if (args->object_id <= prev->object_id)
            return MOQ_ERR_INVAL;              /* ascending only (MVP) */
        flags |= D18_FETCH_OBJ_OBJECT_DELTA;
        obj_delta = args->object_id - prev->object_id;
    }

    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_vi64(w, flags);
    if (rc < 0) return rc;
    if (flags & D18_FETCH_OBJ_GROUP_DELTA) {
        if ((rc = moq_buf_write_vi64(w, group_delta)) < 0) goto fail;
    }
    if (sg_mode == 0x03) {
        if ((rc = moq_buf_write_vi64(w, args->subgroup_id)) < 0) goto fail;
    }
    if (flags & D18_FETCH_OBJ_OBJECT_DELTA) {
        if ((rc = moq_buf_write_vi64(w, obj_delta)) < 0) goto fail;
    }
    if ((rc = moq_buf_write_raw(w, &args->publisher_priority, 1)) < 0) goto fail;
    /* Object Properties length precedes the payload length; the property bytes and
     * payload are appended by the caller (two-action write), so a header-only encode
     * stops here. */
    if (flags & D18_FETCH_OBJ_PROPERTIES) {
        if ((rc = moq_buf_write_vi64(w, (uint64_t)args->properties_len)) < 0)
            goto fail;
    }
    if (args->header_only)
        return MOQ_OK;
    if ((rc = moq_buf_write_vi64(w, args->payload_len)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

static moq_result_t d18_encode_fetch_range_op(
    moq_session_t *s, moq_buf_writer_t *w, uint32_t range_kind,
    uint64_t group_id, uint64_t object_id)
{
    (void)s;
    uint64_t flags;
    if (range_kind == MOQ_FETCH_RANGE_NON_EXISTENT) flags = D18_FETCH_END_NON_EXISTENT;
    else if (range_kind == MOQ_FETCH_RANGE_UNKNOWN) flags = D18_FETCH_END_UNKNOWN;
    else return MOQ_ERR_INVAL;
    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_vi64(w, flags);
    if (rc == MOQ_OK) rc = moq_buf_write_vi64(w, group_id);
    if (rc == MOQ_OK) rc = moq_buf_write_vi64(w, object_id);
    if (rc < 0) w->pos = saved;
    return rc;
}

static moq_result_t d18_encode_fetch_header_op(
    moq_session_t *s, struct moq_buf_writer *w, uint64_t request_id)
{
    (void)s;
    return moq_d18_encode_fetch_header(w, request_id);
}

/* Decode and dispatch a response message buffered on an outbound request bidi
 * stream. The core passes the request `kind`; the first response must be valid
 * for it (SUBSCRIBE_OK / FETCH_OK, or the shared REQUEST_ERROR). The core owns
 * the slot, buffering, and lifecycle. */
static moq_result_t d18_process_response_stream(
    moq_session_t *s, moq_stream_ref_t ref, int slot, uint32_t kind,
    const uint8_t *buf, size_t len, bool fin, size_t *out_consumed)
{
    (void)fin;            /* ref is used by the inbound REQUEST_UPDATE arm */
    *out_consumed = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, buf, len);
    moq_control_envelope_t env;
    moq_result_t rc = moq_d18_decode_envelope(&r, &env);
    if (rc == MOQ_ERR_BUFFER)
        return MOQ_OK;            /* incomplete; the core waits / handles FIN */
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed response envelope");

    /* A GOAWAY on this (committed) request bidi migrates the single request
     * (§10.4). The opener side already has the committed (kind, slot). */
    if (env.msg_type == MOQ_D18_GOAWAY) {
        moq_request_family_t fam = d18_family_from_req_kind(kind);
        if (fam == (moq_request_family_t)0)
            return close_with_error(s, 0x3, "GOAWAY on unsupported request kind");
        return d18_request_goaway(s, ref, fam, slot, &env, &r, len, out_consumed);
    }

    /* REQUEST_ERROR is valid for either request kind; route by kind and state. */
    if (env.msg_type == MOQ_D18_REQUEST_ERROR) {
        moq_d18_request_error_t er;
        moq_d18_redirect_t wire_rd;
        moq_bytes_t rd_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
        rc = moq_d18_decode_request_error_redirect(env.payload, env.payload_len,
                 rd_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS, &er, &wire_rd);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_ERROR");
        size_t off = moq_buf_reader_offset(&r);
        bool can_retry = er.retry_interval != 0;

        /* REDIRECT (§10.6) carries a Redirect tail and is valid only for SUBSCRIBE,
         * FETCH, TRACK_STATUS and PUBLISH_NAMESPACE here (SUBSCRIBE_NAMESPACE is on
         * its own response path); it is a protocol violation on any other family,
         * and on an established subscription's update. */
        bool is_redirect = (er.error_code == MOQ_D18_ERROR_REDIRECT);
        moq_decoded_redirect_t rd_core;
        const moq_decoded_redirect_t *redirect = NULL;
        if (is_redirect) {
            if (kind == (uint32_t)MOQ_REQ_PUBLISH ||
                kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS)
                return close_with_error(s, 0x3,
                    "REDIRECT on a family that does not permit it");
            if (s->perspective == MOQ_PERSPECTIVE_SERVER &&
                wire_rd.connect_uri.len > 0)
                return close_with_error(s, 0x3,
                    "server received REDIRECT with non-zero Connect URI");
            if (kind == (uint32_t)MOQ_REQ_ANNOUNCEMENT &&
                wire_rd.track_name.len > 0)
                return close_with_error(s, 0x3,
                    "REDIRECT Track Name on namespace-scoped request");
            rd_core.connect_uri = wire_rd.connect_uri.data;
            rd_core.connect_uri_len = wire_rd.connect_uri.len;
            rd_core.track_namespace = wire_rd.track_namespace;   /* parts in rd_parts */
            rd_core.track_name = wire_rd.track_name.data;
            rd_core.track_name_len = wire_rd.track_name.len;
            redirect = &rd_core;
        }

        if (kind == (uint32_t)MOQ_REQ_FETCH) {
            /* Terminal for a fetch: nothing follows on the bidi. */
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            /* Defer the free: the core drains the request stream's FIN. */
            rc = session_core_on_fetch_error(s, slot, er.error_code, can_retry,
                er.retry_interval, er.reason.data, er.reason.len, false, redirect);
        } else if (kind == (uint32_t)MOQ_REQ_ANNOUNCEMENT) {
            /* Terminal rejection of a pending PUBLISH_NAMESPACE. A REQUEST_ERROR
             * after the announcement is established is not a valid response. */
            if (s->announcements[slot].state != MOQ_ANN_PENDING_ANNOUNCER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR on non-pending announcement");
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            moq_decoded_announcement_error_t d;
            memset(&d, 0, sizeof(d));
            d.target_slot = slot;
            d.error_code = er.error_code;
            d.can_retry = can_retry;
            d.retry_after_ms = er.retry_interval;
            d.reason = er.reason.data;
            d.reason_len = er.reason.len;
            rc = session_core_on_announcement_error(s, &d, redirect);
        } else if (kind == (uint32_t)MOQ_REQ_TRACK_STATUS) {
            /* Terminal: a failed TRACK_STATUS; nothing follows but the FIN. */
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            rc = session_core_on_track_status_error(s, slot, er.error_code,
                can_retry, er.retry_interval, er.reason.data, er.reason.len,
                redirect);
        } else if (kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS) {
            /* Terminal first-response rejection of a pending SUBSCRIBE_TRACKS.
             * REDIRECT is not permitted here (excluded above). */
            if (s->track_subs[slot].state != MOQ_TRACK_SUB_PENDING_SUBSCRIBER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR on non-pending subscribe-tracks");
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            rc = session_core_on_subscribe_tracks_error(s, slot, er.error_code,
                can_retry, er.retry_interval, er.reason.data, er.reason.len);
        } else if (kind == (uint32_t)MOQ_REQ_PUBLISH) {
            /* Terminal rejection of a pending outbound PUBLISH. REDIRECT is not
             * permitted here (excluded above). */
            if (s->publishes[slot].state != MOQ_PUB_PENDING_PUBLISHER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR on non-pending publish");
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            /* Defer the free: the core drains the request stream's FIN and closes
             * our send half reciprocally. */
            rc = session_core_on_publish_error(s, slot, er.error_code, can_retry,
                er.retry_interval, er.reason.data, er.reason.len, false);
        } else if (s->subs[slot].state == MOQ_SUB_ESTABLISHED) {
            /* A REQUEST_ERROR on an established subscription is an update
             * failure: clear the pending update. The publisher must follow with
             * PUBLISH_DONE(UPDATE_FAILED) to terminate the subscription, so more
             * messages may follow -- do not reject trailing bytes here. REDIRECT
             * is not a valid update response. */
            if (is_redirect)
                return close_with_error(s, 0x3,
                    "REDIRECT on a subscription update");
            rc = session_core_on_subscribe_update_error(s, slot);
        } else {
            /* First-response rejection of a pending subscription: terminal. */
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            moq_decoded_subscribe_error_t d;
            memset(&d, 0, sizeof(d));
            d.target_slot = slot;
            d.error_code = er.error_code;
            d.can_retry = can_retry;
            d.retry_after_ms = er.retry_interval;
            d.reason = er.reason.data;
            d.reason_len = er.reason.len;
            rc = session_core_on_subscribe_error(s, &d, false, redirect);
        }
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* PUBLISH_DONE terminates an established subscription (publisher -> the
     * subscriber); it is the final message before FIN. It is not a valid first
     * response to a pending SUBSCRIBE (which must be SUBSCRIBE_OK or
     * REQUEST_ERROR), so it is accepted only once the subscription is
     * established. Surface SUBSCRIBE_DONE and keep the entry to drain the FIN. */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIPTION &&
        env.msg_type == MOQ_D18_PUBLISH_DONE &&
        s->subs[slot].state == MOQ_SUB_ESTABLISHED) {
        moq_d18_publish_done_t pd;
        rc = moq_d18_decode_publish_done(env.payload, env.payload_len, &pd);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed PUBLISH_DONE");
        /* A failed update mandates termination via PUBLISH_DONE(UPDATE_FAILED);
         * any other status after an update failure is a protocol violation. */
        if (s->subs[slot].update_failed && pd.status_code != 0x8)
            return close_with_error(s, 0x3,
                "PUBLISH_DONE after update failure must be UPDATE_FAILED");
        moq_decoded_publish_done_t d;
        memset(&d, 0, sizeof(d));
        d.target_slot = slot;
        d.status_code = pd.status_code;
        d.stream_count = pd.stream_count;
        d.reason = pd.reason.data;
        d.reason_len = pd.reason.len;
        rc = session_core_on_subscribe_done(s, slot, &d, false /* drain FIN */);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    if (kind == (uint32_t)MOQ_REQ_SUBSCRIPTION &&
        env.msg_type == MOQ_D18_SUBSCRIBE_OK) {
        moq_d18_subscribe_ok_t ok;
        rc = moq_d18_decode_subscribe_ok(env.payload, env.payload_len, &ok);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed SUBSCRIBE_OK");
        moq_decoded_subscribe_ok_t d;
        memset(&d, 0, sizeof(d));
        d.track_alias = ok.track_alias;
        d.has_largest = ok.params.has_largest;
        d.largest_group = ok.params.largest_group;
        d.largest_object = ok.params.largest_object;
        d.has_expires = ok.params.has_expires;
        d.expires_ms = ok.params.expires_ms;
        d.track_properties = ok.track_properties.data;
        d.track_properties_len = ok.track_properties.len;
        d.track_properties_unsupported = ok.track_properties_unsupported;
        d.dynamic_groups = ok.dynamic_groups;
        rc = session_core_on_subscribe_ok(s, &d, slot);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* core failed closed (e.g. duplicate alias) */
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    if (kind == (uint32_t)MOQ_REQ_FETCH &&
        env.msg_type == MOQ_D18_FETCH_OK) {
        moq_d18_fetch_ok_t ok;
        rc = moq_d18_decode_fetch_ok(env.payload, env.payload_len, &ok);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed FETCH_OK");
        moq_decoded_fetch_ok_t d;
        memset(&d, 0, sizeof(d));
        d.target_slot = slot;
        d.end_of_track = ok.end_of_track;
        d.end_group = ok.end.group;
        d.end_object = ok.end.object;
        d.track_properties = ok.track_properties.data;
        d.track_properties_len = ok.track_properties.len;
        d.track_properties_unsupported = ok.track_properties_unsupported;
        rc = session_core_on_fetch_ok(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    /* REQUEST_OK accepting a pending PUBLISH_NAMESPACE (PUBLISH_NAMESPACE_OK):
     * carries no parameters and empty Track Properties. The announce bidi stays
     * open (ESTABLISHED) until teardown. */
    if (kind == (uint32_t)MOQ_REQ_ANNOUNCEMENT &&
        env.msg_type == MOQ_D18_REQUEST_OK) {
        /* PUBLISH_NAMESPACE_OK is the single first response; a second REQUEST_OK
         * after establishment is a protocol violation. */
        if (s->announcements[slot].state != MOQ_ANN_PENDING_ANNOUNCER)
            return close_with_error(s, 0x3,
                "REQUEST_OK on non-pending announcement");
        rc = moq_d18_decode_request_ok(env.payload, env.payload_len);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        if (moq_buf_reader_offset(&r) < len)
            return close_with_error(s, 0x3, "extra bytes after REQUEST_OK");
        moq_decoded_announcement_ok_t d;
        memset(&d, 0, sizeof(d));
        d.target_slot = slot;
        rc = session_core_on_announcement_ok(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    /* TRACK_STATUS_OK: a REQUEST_OK carrying LARGEST_OBJECT / EXPIRES params and
     * a Track Properties tail. Terminal (FIN follows); the core surfaces the
     * status and drains the request bidi. */
    if (kind == (uint32_t)MOQ_REQ_TRACK_STATUS &&
        env.msg_type == MOQ_D18_REQUEST_OK) {
        if (s->track_statuses[slot].state != MOQ_TS_PENDING_REQUESTER)
            return close_with_error(s, 0x3,
                "REQUEST_OK on non-pending track-status");
        moq_d18_track_status_ok_t ok;
        rc = moq_d18_decode_track_status_ok(env.payload, env.payload_len, &ok);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed TRACK_STATUS_OK");
        if (moq_buf_reader_offset(&r) < len)
            return close_with_error(s, 0x3, "extra bytes after TRACK_STATUS_OK");
        moq_decoded_track_status_ok_t d;
        memset(&d, 0, sizeof(d));
        d.target_slot = slot;
        d.has_largest = ok.params.has_largest;
        d.largest_group = ok.params.largest_group;
        d.largest_object = ok.params.largest_object;
        d.has_expires = ok.params.has_expires;
        d.expires_ms = ok.params.expires_ms;
        d.track_properties = ok.track_properties.data;
        d.track_properties_len = ok.track_properties.len;
        rc = session_core_on_track_status_ok(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    /* REQUEST_OK on an established subscription's request bidi acknowledges a
     * pending REQUEST_UPDATE (REQUEST_UPDATE_OK). It correlates by the stream;
     * the core clears the pending-update state. */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIPTION &&
        env.msg_type == MOQ_D18_REQUEST_OK) {
        rc = moq_d18_decode_request_ok(env.payload, env.payload_len);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        rc = session_core_on_subscribe_update_ok(s, slot);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    /* REQUEST_OK accepting a pending SUBSCRIBE_TRACKS: the bidi stays established
     * for PUBLISH_BLOCKED (and future PUBLISH on separate bidis). */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS &&
        env.msg_type == MOQ_D18_REQUEST_OK) {
        if (s->track_subs[slot].state != MOQ_TRACK_SUB_PENDING_SUBSCRIBER)
            return close_with_error(s, 0x3,
                "REQUEST_OK on non-pending subscribe-tracks");
        rc = moq_d18_decode_request_ok(env.payload, env.payload_len);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        rc = session_core_on_subscribe_tracks_ok(s, slot);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    /* PUBLISH_BLOCKED on an established SUBSCRIBE_TRACKS response stream. */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS &&
        env.msg_type == MOQ_D18_PUBLISH_BLOCKED) {
        if (s->track_subs[slot].state != MOQ_TRACK_SUB_ESTABLISHED)
            return close_with_error(s, 0x3, "PUBLISH_BLOCKED before REQUEST_OK");
        moq_decoded_publish_blocked_t d;
        memset(&d, 0, sizeof(d));
        moq_d18_publish_blocked_t pb;
        rc = moq_d18_decode_publish_blocked(env.payload, env.payload_len,
                                            d.suffix_parts,
                                            MOQ_DECODED_MAX_NAMESPACE_PARTS, &pb);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed PUBLISH_BLOCKED");
        d.target_slot = slot;
        d.track_namespace_suffix = pb.track_namespace_suffix; /* parts = d.suffix_parts */
        d.track_name = pb.track_name;
        rc = session_core_on_publish_blocked(s, slot, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    /* PUBLISH_OK: a REQUEST_OK carrying the subscriber's delivery parameters
     * (and empty Track Properties). The single first response to a pending
     * outbound PUBLISH; it establishes the publication. */
    if (kind == (uint32_t)MOQ_REQ_PUBLISH &&
        env.msg_type == MOQ_D18_REQUEST_OK) {
        if (s->publishes[slot].state != MOQ_PUB_PENDING_PUBLISHER)
            return close_with_error(s, 0x3, "PUBLISH_OK on non-pending publish");
        moq_d18_publish_ok_t ok;
        rc = moq_d18_decode_publish_ok(env.payload, env.payload_len, &ok);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed PUBLISH_OK");
        moq_decoded_publish_ok_t d;
        memset(&d, 0, sizeof(d));
        d.target_slot = slot;
        /* Omitted FORWARD means forward = 1 (§5.4.1). */
        d.forward = ok.params.has_forward ? (ok.params.forward != 0) : true;
        d.subscriber_priority = ok.params.has_subscriber_priority
            ? ok.params.subscriber_priority : 128;
        d.group_order = ok.params.has_group_order
            ? ok.params.group_order : MOQ_GROUP_ORDER_DEFAULT;
        bool has_to;
        uint64_t to_us;
        if (d18_map_delivery_timeout(&ok.params, &has_to, &to_us) < 0)
            return close_with_error(s, 0x3,
                "inconsistent delivery timeout parameters");
        d.has_delivery_timeout = has_to;
        d.delivery_timeout_ms = to_us / 1000;
        d.has_new_group_request = ok.params.has_new_group_request;
        d.new_group_request = ok.params.new_group_request;
        rc = session_core_on_publish_ok(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    /* REQUEST_UPDATE inbound on an established PUBLISH (the subscriber updates
     * the forward state); the publisher (us, the opener) reads it on the bidi's
     * response stream and acknowledges with REQUEST_OK. It consumes a fresh peer
     * request id and correlates by the stream. */
    if (kind == (uint32_t)MOQ_REQ_PUBLISH &&
        env.msg_type == MOQ_D18_REQUEST_UPDATE &&
        s->publishes[slot].state == MOQ_PUB_ESTABLISHED &&
        s->publishes[slot].role == MOQ_PUB_ROLE_PUBLISHER) {
        moq_d18_request_update_t u;
        rc = moq_d18_decode_request_update(env.payload, env.payload_len, &u);
        if (rc == MOQ_D18_ERR_KVP_FORMAT)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_UPDATE");

        moq_request_endpoint_t ep;
        rc = d18_validate_inbound_request_stream(s, ref, env.msg_type,
                                                 u.request_id, &ep);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;

        moq_decoded_request_update_t d;
        memset(&d, 0, sizeof(d));
        d.endpoint = ep;
        d.request_id = u.request_id;
        d.target_kind = MOQ_REQ_PUBLISH;
        d.target_slot = slot;
        d.has_forward = u.params.has_forward;
        d.forward = u.params.forward != 0;
        d.has_subscriber_priority = u.params.has_subscriber_priority;
        d.subscriber_priority = u.params.subscriber_priority;
        if (d18_map_delivery_timeout(&u.params, &d.has_delivery_timeout,
                                     &d.delivery_timeout_us) < 0)
            return close_with_error(s, 0x3,
                "inconsistent delivery timeout parameters");
        d.has_new_group_request = u.params.has_new_group_request;
        d.new_group_request = u.params.new_group_request;

        rc = d18_resolve_auth_tokens(s, &u.params, d.tokens, &d.token_count,
                                     d.token_staged, &d.auth_txn,
                                     &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */

        rc = session_core_on_publish_request_update(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = moq_buf_reader_offset(&r);
        return MOQ_OK;
    }

    /* First-response enforcement: only the response valid for this request
     * kind (or REQUEST_ERROR) may appear. */
    return close_with_error(s, 0x3, "unexpected first response message");
}

/* -- Data plane: subgroup streams ---------------------------------- *
 * Object Properties (vi64 KVP, §11.2.1.2) are decoded and encoded on subgroup
 * objects; datagrams are handled by the OBJECT_DATAGRAM ops. (FETCH data
 * streams are handled below.) */

static uint32_t d18_classify_data_stream(const uint8_t *data, size_t len)
{
    if (len == 0) return (uint32_t)MOQ_STREAM_KIND_NEED_MORE;

    /* The stream type is a vi64, which may be encoded non-minimally (multi-byte).
     * If the leading byte announces more bytes than are buffered, classification
     * is not yet possible: ask the receiver to buffer more and re-classify. */
    size_t need = moq_vi64_encoded_len(data[0]);
    if (need > len)
        return (uint32_t)MOQ_STREAM_KIND_NEED_MORE;

    uint64_t type = 0;
    moq_vi64_decode(data, len, &type);
    if (type <= 0xFF && moq_d18_subgroup_type_valid((uint8_t)type))
        return (uint32_t)MOQ_STREAM_KIND_SUBGROUP;
    if (type == MOQ_D18_STREAM_FETCH_HEADER)
        return (uint32_t)MOQ_STREAM_KIND_FETCH;
    return (uint32_t)MOQ_STREAM_KIND_UNKNOWN;
}

static moq_result_t d18_decode_subgroup_header_op(
    moq_session_t *s, moq_buf_reader_t *r, moq_decoded_subgroup_header_t *out)
{
    (void)s;
    moq_d18_subgroup_header_t hdr;
    moq_result_t rc = moq_d18_decode_subgroup_header(r, &hdr);
    if (rc < 0) return rc;

    out->track_alias = hdr.track_alias;
    out->group_id = hdr.group_id;
    out->publisher_priority = hdr.default_priority ? 128 : hdr.publisher_priority;
    out->has_extensions = hdr.has_properties;
    out->end_of_group = hdr.end_of_group;
    switch (hdr.subgroup_id_mode) {
    case MOQ_SUBGROUP_ID_MODE_ZERO:
        out->subgroup_id = 0;
        out->subgroup_id_resolved = true;
        out->subgroup_id_from_first_object = false;
        break;
    case MOQ_SUBGROUP_ID_MODE_FIRST_OBJ:
        out->subgroup_id = 0;
        out->subgroup_id_resolved = false;
        out->subgroup_id_from_first_object = true;
        break;
    case MOQ_SUBGROUP_ID_MODE_PRESENT:
        out->subgroup_id = hdr.subgroup_id;
        out->subgroup_id_resolved = true;
        out->subgroup_id_from_first_object = false;
        break;
    }
    return MOQ_OK;
}

static moq_result_t d18_encode_subgroup_header_op(
    moq_session_t *s, moq_buf_writer_t *w,
    const moq_subgroup_header_encode_args_t *args)
{
    (void)s;
    moq_d18_subgroup_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.has_properties = args->has_extensions;
    hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
    hdr.end_of_group = args->end_of_group;
    hdr.default_priority = false;
    hdr.first_object = false;
    hdr.track_alias = args->track_alias;
    hdr.group_id = args->group_id;
    hdr.subgroup_id = args->subgroup_id;
    hdr.publisher_priority = args->publisher_priority;
    return moq_d18_encode_subgroup_header(w, &hdr);
}

static moq_object_status_t d18_wire_status_to_semantic(uint64_t wire_status)
{
    switch (wire_status) {
    case 0x0: return MOQ_OBJECT_NORMAL;
    case 0x3: return MOQ_OBJECT_END_OF_GROUP;
    case 0x4: return MOQ_OBJECT_END_OF_TRACK;
    default:  return (moq_object_status_t)0xFF;
    }
}

static moq_result_t d18_encode_object_header_op(
    moq_session_t *s, moq_buf_writer_t *w,
    const moq_object_header_encode_args_t *args)
{
    (void)s;
    uint64_t delta = args->has_prev_object
        ? args->object_id - args->prev_object_id - 1
        : args->object_id;

    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_vi64(w, delta);
    if (rc < 0) return rc;
    /* Object Properties (§11.4.2): [Object ID Delta][Properties][Payload Length]
     * [Status]. The property length precedes the payload length; the property
     * bytes and payload are appended by the caller (two-action write), so a
     * header-only encode of a properties-bearing object stops after the length. */
    if (args->has_extensions) {
        rc = moq_buf_write_vi64(w, (uint64_t)args->properties_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }
    if (args->header_only)
        return MOQ_OK;
    rc = moq_buf_write_vi64(w, args->payload_len);
    if (rc < 0) { w->pos = saved; return rc; }
    if (args->payload_len == 0 && args->properties_len == 0) {
        rc = moq_buf_write_vi64(w, args->object_status);
        if (rc < 0) { w->pos = saved; return rc; }
    }
    return MOQ_OK;
}

static moq_result_t d18_encode_object_payload_prefix_op(moq_session_t *s,
                                                        moq_buf_writer_t *w,
                                                        uint64_t payload_len,
                                                        bool with_status)
{
    (void)s;
    moq_result_t rc = moq_buf_write_vi64(w, payload_len);
    if (rc < 0) return rc;
    if (with_status && payload_len == 0) {
        rc = moq_buf_write_vi64(w, MOQ_OBJECT_STATUS_NORMAL);
        if (rc < 0) return rc;
    }
    return MOQ_OK;
}

/* Refuse to emit a Mandatory Track Property (0x4000-0x7FFF) as an object
 * property — symmetric with the inbound malformed check (§11.2.1.2). Also rejects
 * a structurally malformed block. */
static moq_result_t d18_validate_object_properties_op(moq_session_t *s,
                                                      const uint8_t *props,
                                                      size_t len)
{
    (void)s;
    return moq_d18_validate_properties(props, len);
}

static moq_result_t d18_decode_object_header_op(
    moq_session_t *s, moq_buf_reader_t *r, bool has_extensions,
    uint64_t prev_object_id, bool has_prev_object,
    moq_decoded_object_header_t *out)
{
    (void)s;
    memset(out, 0, sizeof(*out));

    uint64_t delta;
    if (moq_buf_read_vi64(r, &delta) < 0) return MOQ_ERR_BUFFER;

    /* Object Properties (§11.4.2): a length-prefixed KVP block between the Object
     * ID Delta and the payload length. Surface the borrowed bytes; reject a
     * malformed block or a Mandatory Track Property carried as an object property. */
    if (has_extensions) {
        uint64_t props_len;
        if (moq_buf_read_vi64(r, &props_len) < 0) return MOQ_ERR_BUFFER;
        if ((size_t)props_len > moq_buf_reader_remaining(r)) return MOQ_ERR_BUFFER;
        if (props_len > 0) {
            const uint8_t *pp = moq_buf_reader_ptr(r);
            moq_result_t prc = moq_d18_validate_properties(pp, (size_t)props_len);
            if (prc < 0) return prc;
            out->has_properties = true;
            out->properties = pp;
            out->properties_len = (size_t)props_len;
        }
        r->pos += (size_t)props_len;
    }

    uint64_t payload_len;
    if (moq_buf_read_vi64(r, &payload_len) < 0) return MOQ_ERR_BUFFER;
    out->payload_len = payload_len;

    if (payload_len == 0) {
        uint64_t st;
        if (moq_buf_read_vi64(r, &st) < 0) return MOQ_ERR_BUFFER;
        moq_object_status_t sem = d18_wire_status_to_semantic(st);
        if (sem == (moq_object_status_t)0xFF) return MOQ_ERR_PROTO;
        out->status = sem;
    } else {
        out->status = MOQ_OBJECT_NORMAL;
    }

    /* Properties are valid only on a Normal object (§11.2.1.2). */
    if (out->has_properties && out->status != MOQ_OBJECT_NORMAL)
        return MOQ_ERR_PROTO;

    if (!has_prev_object) {
        out->object_id = delta;
    } else {
        out->object_id = prev_object_id + 1 + delta;
        if (out->object_id <= prev_object_id)   /* wrapped past 2^64-1 */
            return MOQ_ERR_PROTO;
    }
    return MOQ_OK;
}

/* -- Defensive op stubs --------------------------------------------- *
 * Ops that are unreachable or intentionally absent on draft-18, kept non-NULL
 * so an unexpected call fails cleanly instead of dereferencing NULL:
 * UNSUBSCRIBE has no draft-18 wire message (cancel tears down the request
 * bidi); the bidi classify hook is pre-empted by the request-stream router;
 * request-capacity has no draft-18 wire form (no MAX_REQUEST_ID). */

static moq_result_t d18_unimpl_encode_unsubscribe(
    moq_session_t *s, struct moq_buf_writer *w, uint64_t request_id)
{ (void)s; (void)w; (void)request_id; return MOQ_ERR_INVAL; }

static moq_result_t d18_unimpl_classify_bidi_stream(moq_session_t *s,
                                                    moq_stream_ref_t ref,
                                                    const uint8_t *data,
                                                    size_t len)
{
    (void)ref; (void)data; (void)len;
    /* Unreachable for draft-18: unknown inbound bidis route through
     * handle_request_stream_bytes before this classify hook (the ns_sub bidi
     * handler's uses_request_streams branch). Defensive close. */
    return close_with_error(s, 0x3, "unclassifiable bidi stream");
}

/* OBJECT_DATAGRAM (§11.3.1) — decode the wire form, resolve the track alias to a
 * sub/pub slot, and map the wire status to the semantic enum. Returns MOQ_DONE
 * (caller discards) for a padding datagram or an unknown track alias, MOQ_ERR_PROTO
 * for a malformed/invalid datagram (caller closes 0x3). */
static moq_result_t d18_decode_object_datagram(
    moq_session_t *s, const uint8_t *data, size_t len,
    struct moq_decoded_object_datagram *out)
{
    memset(out, 0, sizeof(*out));

    moq_d18_object_datagram_t dg;
    moq_result_t rc = moq_d18_decode_object_datagram(data, len, &dg);
    if (rc != MOQ_OK) return rc;   /* MOQ_DONE (padding) / MOQ_ERR_PROTO propagate */

    int sub_slot = sub_find_by_alias_subscriber(s, dg.track_alias);
    int pub_slot = -1;
    if (sub_slot < 0)
        pub_slot = pub_find_by_alias_subscriber(s, dg.track_alias);
    if (sub_slot < 0 && pub_slot < 0) {
        /* Parsed cleanly but no established alias: surface the alias so the
         * caller can hold the datagram for control/data reordering (§11.3). */
        out->track_alias   = dg.track_alias;
        out->unknown_alias = true;
        return MOQ_DONE;
    }

    out->sub_slot           = sub_slot;
    out->pub_slot           = pub_slot;
    out->group_id           = dg.group_id;
    out->object_id          = dg.object_id;
    out->publisher_priority = dg.publisher_priority;
    out->default_priority   = dg.default_priority;
    out->end_of_group       = dg.end_of_group;
    out->is_status          = dg.is_status;
    out->status             = dg.is_status
        ? d18_wire_status_to_semantic(dg.object_status)
        : MOQ_OBJECT_NORMAL;
    if (dg.has_properties) {
        out->properties     = dg.properties;
        out->properties_len = dg.properties_len;
    }
    out->payload     = dg.payload;
    out->payload_len = dg.payload_len;
    return MOQ_OK;
}

static moq_result_t d18_encode_object_datagram(
    moq_session_t *s, moq_buf_writer_t *w,
    const struct moq_datagram_encode_args *args)
{
    (void)s;
    moq_d18_object_datagram_t dg;
    memset(&dg, 0, sizeof(dg));
    dg.track_alias        = args->track_alias;
    dg.group_id           = args->group_id;
    dg.object_id          = args->object_id;
    dg.publisher_priority = args->publisher_priority;
    dg.default_priority   = args->default_priority;
    dg.end_of_group       = args->end_of_group;
    dg.is_status          = args->is_status;
    dg.object_status      = args->object_status;
    if (args->properties_len > 0) {
        dg.has_properties = true;
        dg.properties     = args->properties;
        dg.properties_len = args->properties_len;
    }
    dg.payload     = args->payload;
    dg.payload_len = args->payload_len;
    return moq_d18_encode_object_datagram(w, &dg);
}

static moq_result_t d18_unimpl_grant_capacity(moq_session_t *s,
                                              uint64_t new_capacity,
                                              uint64_t now_us)
{
    (void)s; (void)new_capacity; (void)now_us;
    return MOQ_ERR_INVAL;
}

static uint64_t d18_unimpl_request_capacity(const moq_session_t *s)
{
    (void)s;
    return 0;
}

/* GOAWAY encode (§10.4). The public API carries only the URI; the Timeout (the
 * local drain timeout, ms) and the Request ID (the smallest unprocessed peer
 * Request ID) are derived from session/profile state, so the draft-neutral
 * encode args stay {uri, uri_len} and draft-16 is unaffected. */
static moq_result_t d18_encode_goaway(moq_session_t *s,
                                      struct moq_buf_writer *w,
                                      const struct moq_goaway_encode_args *args)
{
    const moq_d18_profile_state_t *d18 =
        (const moq_d18_profile_state_t *)s->profile_state;
    uint64_t timeout_ms = s->goaway_timeout_us / 1000;
    return moq_d18_encode_goaway(w, args->uri, args->uri_len, timeout_ms,
                                 d18->peer_next_request_id);
}

/* -- Vtable -------------------------------------------------------- *
 * Fully populated. The d18_unimpl_* entries are defensive stubs for ops that
 * are unreachable on draft-18 (UNSUBSCRIBE has no wire message; bidi classify
 * is pre-empted by the request-stream router) or intentionally absent
 * (request-capacity: draft-18 has no MAX_REQUEST_ID). */
static const moq_profile_ops_t d18_ops = {
    .state_size              = sizeof(moq_d18_profile_state_t),
    .state_align             = _Alignof(moq_d18_profile_state_t),
    .init_in_place           = d18_init_in_place,
    .destroy                 = d18_destroy,
    .start                   = d18_start,
    .process_control_data    = d18_process_control_data,
    .uses_request_streams    = true,
    .fetch_descending_supported = false,   /* ascending-only delta reconstruction */
    .uses_uni_control_channel = true,
    .classify_uni_stream     = d18_classify_uni_stream,
    /* Request admission + SUBSCRIBE outbound. */
    .prepare_request         = d18_prepare_request,
    .commit_request          = d18_commit_request,
    .abort_request           = d18_abort_request,
    .release_request         = d18_release_request,
    .next_track_alias        = d18_next_track_alias,
    .advance_track_alias     = d18_advance_track_alias,
    .validate_inbound_request_stream = d18_validate_inbound_request_stream,
    .commit_inbound_request  = d18_commit_inbound_request,
    .process_request_stream  = d18_process_request_stream,
    .process_response_stream = d18_process_response_stream,
    .encode_subscribe        = d18_encode_subscribe,
    .encode_subscribe_ok     = d18_encode_subscribe_ok,
    .encode_request_error    = d18_encode_request_error,
    .encode_request_update   = d18_encode_request_update,
    .encode_request_ok       = d18_encode_request_ok,
    .encode_publish_done     = d18_encode_publish_done,
    /* Data plane: subgroup object streams. */
    .classify_data_stream    = d18_classify_data_stream,
    .decode_subgroup_header  = d18_decode_subgroup_header_op,
    .encode_subgroup_header  = d18_encode_subgroup_header_op,
    .encode_object_header    = d18_encode_object_header_op,
    .encode_request_goaway   = d18_encode_request_goaway,
    .encode_object_payload_prefix = d18_encode_object_payload_prefix_op,
    .validate_object_properties = d18_validate_object_properties_op,
    .decode_object_header    = d18_decode_object_header_op,
    /* Not-yet-implemented guards (first op on each reachable path). */
    .encode_fetch            = d18_encode_fetch_op,
    .encode_fetch_ok         = d18_encode_fetch_ok_op,
    .encode_fetch_header     = d18_encode_fetch_header_op,
    .decode_fetch_header     = d18_decode_fetch_header_op,
    .encode_fetch_object     = d18_encode_fetch_object_op,
    .decode_fetch_object     = d18_decode_fetch_object_op,
    .encode_fetch_range      = d18_encode_fetch_range_op,
    .encode_publish          = d18_encode_publish,
    .track_properties_dynamic_groups = d18_track_properties_dynamic_groups,
    .encode_publish_ok       = d18_encode_publish_ok,
    .encode_track_status     = d18_encode_track_status,
    .encode_track_status_ok  = d18_encode_track_status_ok,
    .encode_subscribe_namespace = d18_encode_subscribe_namespace,
    .encode_namespace_msg    = d18_encode_namespace_msg,
    .encode_subscribe_tracks = d18_encode_subscribe_tracks,
    .encode_publish_blocked  = d18_encode_publish_blocked,
    .decode_ns_sub_request   = d18_decode_ns_sub_request,
    .decode_ns_sub_response  = d18_decode_ns_sub_response,
    .encode_publish_namespace   = d18_encode_publish_namespace,
    .encode_unsubscribe      = d18_unimpl_encode_unsubscribe,
    .classify_bidi_stream    = d18_unimpl_classify_bidi_stream,
    .encode_object_datagram  = d18_encode_object_datagram,
    .decode_object_datagram  = d18_decode_object_datagram,
    .grant_capacity          = d18_unimpl_grant_capacity,
    .peer_request_capacity   = d18_unimpl_request_capacity,
    .local_request_capacity  = d18_unimpl_request_capacity,
    .encode_goaway           = d18_encode_goaway,
};

const moq_profile_ops_t *moq_d18_profile_ops(void)
{
    return &d18_ops;
}
