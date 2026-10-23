/*
 * Draft-18 wire profile.
 *
 * Setup handshake over a unidirectional control channel per side, the
 * draft-18 codec adapters that feed the shared request-stream engine
 * (request_stream.c), the outbound control encoders, and the data-plane
 * framing for subgroup, datagram and fetch streams. Request admission and
 * request/response dispatch live in the engine; this file owns only what
 * differs on the wire.
 */

#include "session_internal.h"
#include "request_stream_internal.h"
#include "profile_d18_ops_internal.h"
#include "moq/control_d18.h"
#include "../wire/control_d18_internal.h"
#include "../wire/kvp_internal.h"
#include "moq/vi64.h"

/* -- D18 profile state --------------------------------------------- */
typedef struct moq_d18_profile_state {
    /* Request-stream engine state; must stay first (see request_stream_internal.h). */
    moq_request_stream_state_t rs;
    moq_version_t version;
    bool          setup_sent;
    bool          setup_received;
} moq_d18_profile_state_t;

_Static_assert(offsetof(moq_d18_profile_state_t, rs) == 0,
               "request-stream state leads the draft-18 profile state");

static void d18_init_in_place(void *profile_state, const moq_session_cfg_t *cfg)
{
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)profile_state;
    memset(d18, 0, sizeof(*d18));
    moq_request_stream_state_init(&d18->rs, cfg->perspective, cfg->alloc);
    d18->version = MOQ_VERSION_DRAFT_18;
}

static void d18_destroy(void *profile_state)
{
    moq_d18_profile_state_t *d18 = (moq_d18_profile_state_t *)profile_state;
    moq_request_stream_state_destroy(&d18->rs);
}


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
        moq_result_t rc = push_event(s, &e);
        if (rc < 0) return rc;
        /* Dispatch request bidis that arrived before establishment (§3.3). */
        return request_streams_refeed_deferred(s);
    }
    return MOQ_OK;
}

/* Encode this side's SETUP into `w`: the auth-token cache size when
 * configured (§10.3.1.3) so the peer may register aliases, and the configured
 * authorization tokens by value (§10.3.1.4). A client carried natively over
 * QUIC names its connection target: PATH is the URI's path-abempty with "?"
 * and the query appended when present, AUTHORITY the URI's authority
 * (§10.3.1.1, §10.3.1.2). Neither is sent by a server, over WebTransport, or
 * by a session never told its target. */
static moq_result_t d18_encode_own_setup(moq_session_t *s, moq_buf_writer_t *w)
{
    moq_d18_setup_opts_t opts;
    moq_d18_setup_opts_init(&opts);
    if (moq_binding_sends_target(s)) {
        opts.has_path = true;
        opts.path = s->binding.value.path_query;
        opts.has_authority = true;
        opts.authority = s->binding.value.authority;
    }
    if (s->send_auth_token_cache_size) {
        opts.has_max_auth_token_cache_size = true;
        opts.max_auth_token_cache_size = s->auth_token_cache_size;
    }
    for (size_t i = 0; i < s->setup_token_count; i++) {
        opts.auth_tokens[i].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        opts.auth_tokens[i].token_type = s->setup_tokens[i].token_type;
        opts.auth_tokens[i].token_value = s->setup_tokens[i].token_value;
    }
    opts.auth_token_count = s->setup_token_count;
    return moq_d18_encode_setup_opts(w, &opts, sizeof(opts));
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

    size_t cap = 32;
    for (size_t i = 0; i < s->setup_token_count; i++)
        cap += s->setup_tokens[i].token_value.len + 32u;
    if (moq_binding_sends_target(s))
        cap += s->binding.value.path_query.len +
               s->binding.value.authority.len + 32u;
    uint8_t stack_buf[32];
    uint8_t *buf = stack_buf;
    if (cap > sizeof(stack_buf)) {
        buf = (uint8_t *)s->alloc.alloc(cap, s->alloc.ctx);
        if (!buf) return MOQ_ERR_NOMEM;
    }
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_result_t rc = d18_encode_own_setup(s, &w);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(s->next_stream_ref);
    if (rc == MOQ_OK)
        rc = queue_open_uni_control(s, ref, buf, moq_buf_writer_offset(&w));
    if (buf != stack_buf) {
        session_wipe(buf, cap);   /* the encoded SETUP carries the tokens */
        s->alloc.free(buf, cap, s->alloc.ctx);
    }
    if (rc < 0) return rc;
    s->next_stream_ref++;

    d18->setup_sent = true;
    s->state = MOQ_SESS_SETUP_SENT;
    return d18_maybe_complete(s);
}


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
     * malformed token structure closes 0x6, and more tokens than this side
     * holds closes with the local-capacity INTERNAL_ERROR (0x1), as on
     * draft-16: the peer broke no rule. */
    moq_d18_setup_opts_t opts;
    moq_result_t orc = moq_d18_decode_setup_opts(env->payload, env->payload_len,
                                                 &opts, sizeof(opts));
    if (orc == MOQ_D18_ERR_KVP_FORMAT)
        return close_with_error(s, 0x6, "malformed auth token structure");
    if (orc == MOQ_ERR_BUFFER)
        return close_with_error(s, 0x1,
            "SETUP has more authorization tokens than this side holds");
    if (orc < 0)
        return close_with_error(s, 0x3, "malformed SETUP options");

    /* PATH and AUTHORITY travel client to server, natively over QUIC only
     * (§10.3.1.1, §10.3.1.2): from a server or over WebTransport each is
     * INVALID_*, a value outside RFC 3986 syntax is MALFORMED_*, and a raw
     * server bound with a URI policy lets it decide whether it serves the
     * named target. The policy runs before any token takes effect and may run
     * again if this SETUP has to be retried before it commits. */
    const char *reason = NULL;
    const moq_binding_peer_target_t target = {
        .has_path = opts.has_path, .has_authority = opts.has_authority,
        .path = opts.path, .authority = opts.authority,
    };
    uint64_t target_err = moq_binding_judge_peer_target(s, &target, &reason);
    if (target_err) return close_with_error(s, target_err, reason);

    /* SETUP auth tokens (§10.3.1.4): functionally equivalent to the message
     * parameter. DELETE/USE_ALIAS reference an alias, and nothing can be
     * registered before the peer's first message, so they cannot appear here
     * (mirrors the draft-16 CLIENT_SETUP rule). */
    for (size_t i = 0; i < opts.auth_token_count; i++) {
        if (opts.auth_tokens[i].alias_type == MOQ_AUTH_TOKEN_DELETE ||
            opts.auth_tokens[i].alias_type == MOQ_AUTH_TOKEN_USE_ALIAS)
            return close_with_error(s, 0x3, "DELETE/USE_ALIAS in SETUP");
    }

    /* The remainder is draft-neutral: resolve the tokens against the cache,
     * surface SETUP_COMPLETE, commit. */
    moq_setup_workspace_t workspace;
    moq_result_t rc = session_setup_workspace_acquire(s, &workspace);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    moq_decoded_auth_token_t *in = workspace.decoded->setup_tokens;
    for (size_t i = 0; i < opts.auth_token_count; i++) {
        const moq_d18_auth_token_t *t = &opts.auth_tokens[i];
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
    workspace.count = opts.auth_token_count;
    moq_setup_params_t peer;
    memset(&peer, 0, sizeof(peer));
    peer.has_path = opts.has_path;
    peer.has_authority = opts.has_authority;
    peer.has_max_auth_token_cache_size = opts.has_max_auth_token_cache_size;
    peer.max_auth_token_cache_size = opts.max_auth_token_cache_size;
    return session_setup_complete_peer(s, &workspace, &peer,
                                       &d18->setup_received, d18->setup_sent);
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

    return session_core_on_goaway(s, ga.uri.data, ga.uri.len, true, ga.timeout_ms);
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
moq_uni_class_t moq_d18_op_classify_uni_stream(const uint8_t *data, size_t len)
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

/* §9.8 pure scanner (vtable op): draft-18 per-type extraction; structural
 * failure, duplicate timeout types (mutable UNION immutable), and nested
 * immutable blocks error in every mode. */
moq_result_t moq_d18_op_scan_delivery_timeouts(const uint8_t *data,
                                               size_t len,
                                               bool strict_local,
                                               moq_dt_scan_t *out)
{
    (void)strict_local;
    return moq_d18_scan_delivery_timeouts(data, len,
        &out->has_object, &out->object_ms,
        &out->has_subgroup, &out->subgroup_ms);
}

/* Legacy projection of this MESSAGE's timeout carriers into the single
 * generic field (§9.8 A5): draft-18 defines the two timeouts independently,
 * so differing values are LEGAL and projected as min_nonzero (zero means
 * "no timeout", §8); the exact per-type values ride the decoded dt_*
 * fields. Conversion saturates -- a legal vi64 value never violates or
 * wraps. */
static moq_result_t d18_map_delivery_timeout(const moq_d18_msg_params_t *p,
                                             bool *has_out, uint64_t *us_out)
{
    bool ho = p->has_object_delivery_timeout;
    bool hs = p->has_subgroup_delivery_timeout;
    if (!ho && !hs) {
        *has_out = false;
        *us_out = 0;
        return MOQ_OK;
    }
    *has_out = true;
    *us_out = ms_to_us_sat(dt_negotiate_ms(
        ho, p->object_delivery_timeout_ms,
        hs, p->subgroup_delivery_timeout_ms));
    return MOQ_OK;
}


/* Copy this message's exact per-type carriers into a decoded struct's dt_*
 * fields (all decoded shapes share the field names via macro). */
#define D18_FILL_DT(dst, p) do {                                           \
        (dst)->dt_has_object   = (p)->has_object_delivery_timeout;         \
        (dst)->dt_object_ms    = (p)->object_delivery_timeout_ms;          \
        (dst)->dt_has_subgroup = (p)->has_subgroup_delivery_timeout;       \
        (dst)->dt_subgroup_ms  = (p)->subgroup_delivery_timeout_ms;        \
    } while (0)

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
        /* A relative start more than one group back and an absolute end
         * object have no form on this draft. */
        if (args->filter == MOQ_SUBSCRIBE_FILTER_RELATIVE_GROUP_INTERNAL ||
            args->filter == MOQ_SUBSCRIBE_FILTER_OBJECT_END_INTERNAL)
            return MOQ_ERR_UNSUPPORTED;
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
 * publisher-chosen; FORWARD (emitted when withholding, i.e. forward=0),
 * AUTHORIZATION_TOKEN, and LARGEST_OBJECT (from the track-history registry --
 * MUST be advertised once Objects have been published) travel as Message
 * Parameters; an opaque Track Properties tail follows. */
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
    if (args->has_largest) {
        p.params.has_largest = true;
        p.params.largest_group = args->largest_group;
        p.params.largest_object = args->largest_object;
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
bool moq_d18_op_track_properties_dynamic_groups(const uint8_t *data,
                                                size_t len)
{
    if (!data || len == 0) return false;
    bool dyn = false;
    if (moq_d18_scan_dynamic_groups(data, len, &dyn) < 0)
        return false;
    return dyn;
}

/* §12.6 DYNAMIC_GROUPS (0x30) = 1 added to a Track Property list, which
 * draft-22 shares (§10.6). The property is looked up at the top level and in
 * IMMUTABLE_PROPERTIES together (§12.7, draft-22 §10.7): a set that already
 * advertises 1 is copied unchanged, one that states 0 (or states it twice)
 * is refused. */
moq_result_t moq_d18_op_track_properties_add_dynamic_groups(const uint8_t *in,
                                                            size_t len,
                                                            uint8_t *out,
                                                            size_t cap,
                                                            size_t *out_len)
{
    if (!out_len) return MOQ_ERR_INVAL;
    return moq_kvp_list_merge_int(in, len, true, 0x30, 1, 0x0B, out, cap, out_len);
}

/* §12.5 DEFAULT_PUBLISHER_GROUP_ORDER, searched in the mutable list and in
 * IMMUTABLE_PROPERTIES (§12.7). Shared with draft-22, whose Track Properties
 * use the same encoding and code points. */
#define D18_PROP_GROUP_ORDER 0x22u
#define D18_PROP_IMMUTABLE_PROPERTIES 0x0Bu
static uint8_t d18_props_group_order(const uint8_t *data, size_t len, bool nested)
{
    uint8_t order = MOQ_GROUP_ORDER_DEFAULT;
    if (!data || len == 0) return order;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    uint64_t prev = 0;
    while (moq_buf_reader_remaining(&r) > 0) {
        uint64_t delta;
        if (moq_buf_read_vi64(&r, &delta) < 0 || delta > UINT64_MAX - prev) break;
        uint64_t type = prev + delta;
        prev = type;
        if (type & 1) {
            uint64_t vlen;
            if (moq_buf_read_vi64(&r, &vlen) < 0 ||
                vlen > moq_buf_reader_remaining(&r)) break;
            if (type == D18_PROP_IMMUTABLE_PROPERTIES && !nested) {
                uint8_t inner = d18_props_group_order(moq_buf_reader_ptr(&r),
                                                      (size_t)vlen, true);
                if (inner != MOQ_GROUP_ORDER_DEFAULT) order = inner;
            }
            r.pos += (size_t)vlen;
        } else {
            uint64_t v;
            if (moq_buf_read_vi64(&r, &v) < 0) break;
            if (type == D18_PROP_GROUP_ORDER &&
                (v == MOQ_GROUP_ORDER_ASCENDING || v == MOQ_GROUP_ORDER_DESCENDING))
                order = (uint8_t)v;
        }
    }
    return order;
}

uint8_t moq_d18_op_track_properties_group_order(const uint8_t *data, size_t len)
{
    return d18_props_group_order(data, len, false);
}

/* DEFAULT_PUBLISHER_GROUP_ORDER = `order` added to a Track Property list,
 * which draft-22 shares, looked up together with IMMUTABLE_PROPERTIES as
 * DYNAMIC_GROUPS is. */
moq_result_t moq_d18_op_track_properties_add_group_order(const uint8_t *in,
                                                         size_t len,
                                                         uint8_t order,
                                                         uint8_t *out,
                                                         size_t cap,
                                                         size_t *out_len)
{
    if (!out_len) return MOQ_ERR_INVAL;
    return moq_kvp_list_merge_int(in, len, true, D18_PROP_GROUP_ORDER, order,
                                  D18_PROP_IMMUTABLE_PROPERTIES, out, cap,
                                  out_len);
}

/* §12.3 MAX_CACHE_DURATION (0x04): an integer of milliseconds. */
#define D18_PROP_MAX_CACHE_DURATION 0x04u
static bool d18_props_max_cache(const uint8_t *data, size_t len, bool nested,
                                uint64_t *out_ms)
{
    bool found = false;
    if (!data || len == 0) return false;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    uint64_t prev = 0;
    while (moq_buf_reader_remaining(&r) > 0) {
        uint64_t delta;
        if (moq_buf_read_vi64(&r, &delta) < 0 || delta > UINT64_MAX - prev) break;
        uint64_t type = prev + delta;
        prev = type;
        if (type & 1) {
            uint64_t vlen;
            if (moq_buf_read_vi64(&r, &vlen) < 0 ||
                vlen > moq_buf_reader_remaining(&r)) break;
            if (type == D18_PROP_IMMUTABLE_PROPERTIES && !nested &&
                d18_props_max_cache(moq_buf_reader_ptr(&r), (size_t)vlen, true,
                                    out_ms))
                found = true;
            r.pos += (size_t)vlen;
        } else {
            uint64_t v;
            if (moq_buf_read_vi64(&r, &v) < 0) break;
            if (type == D18_PROP_MAX_CACHE_DURATION) {
                *out_ms = v;
                found = true;
            }
        }
    }
    return found;
}

bool moq_d18_op_track_properties_max_cache(const uint8_t *data, size_t len,
                                           uint64_t *out_ms)
{
    return d18_props_max_cache(data, len, false, out_ms);
}

/* PUBLISH_OK encode (§10.10 / §10.5): the subscriber's delivery parameters
 * (SUBSCRIBER_PRIORITY / GROUP_ORDER), plus its SUBSCRIPTION_FILTER and FORWARD
 * choices, on the request bidi; empty Track Properties. No request id
 * (stream-correlated). */
static moq_result_t d18_encode_publish_ok(
    moq_session_t *s, struct moq_buf_writer *w,
    const moq_publish_ok_encode_args_t *args)
{
    (void)s;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    if (args->subscriber_priority != 128) {
        p.has_subscriber_priority = true;
        p.subscriber_priority = args->subscriber_priority;
    }
    if (args->group_order != MOQ_GROUP_ORDER_DEFAULT) {
        p.has_group_order = true;
        p.group_order = args->group_order;
    }
    if (args->has_filter) {
        p.has_filter = true;
        p.filter_type = args->filter;
        p.filter_start_group = args->start_group;
        p.filter_start_object = args->start_object;
        p.filter_end_group = args->end_group;
    }
    /* Omission defaults to 1 (§10.2.12 -- only REQUEST_UPDATE means
     * "unchanged"), so only 0 is worth sending, same as SUBSCRIBE.
     * args->forward carries the effective value. */
    if (args->has_forward && !args->forward) {
        p.has_forward = true;
        p.forward = 0;
    }
    p.has_new_group_request = args->has_new_group_request;
    p.new_group_request = args->new_group_request;
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
    moq_bytes_t props = {
        args->omit_track_properties ? NULL : args->track_properties,
        args->omit_track_properties ? 0 : args->track_properties_len
    };
    return moq_d18_encode_subscribe_ok(w, args->track_alias, &p, props);
}

/* REQUEST_ERROR encode. No request id (stream-correlated). Retry Interval is
 * "the minimum time (in milliseconds) before the request SHOULD be sent again,
 * plus one", 0 when retry is not offered (§10.6.2). The optional Redirect
 * structure (§10.6.1) is encoded when args->has_redirect (an all-empty
 * Redirect is valid). */
static moq_result_t d18_encode_request_error(
    moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_request_error_encode_args *args)
{
    (void)s;
    moq_bytes_t reason = { args->reason, args->reason_len };
    if (args->can_retry && args->retry_after_ms >= MOQ_VI64_MAX)
        return MOQ_ERR_INVAL;   /* the plus-one form has no wire value */
    uint64_t retry = args->can_retry ? args->retry_after_ms + 1 : 0;
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
    if (args->sender != MOQ_UPDATE_SENDER_SUBSCRIBER) return MOQ_ERR_UNSUPPORTED;
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
    /* subscription-filter update. Locations ride only the ABSOLUTE_*
     * forms (the param encoder omits them otherwise). */
    if (args->has_filter) {
        p.has_filter = true;
        p.filter_type = args->filter;
        p.filter_start_group = args->filter_start_group;
        p.filter_start_object = args->filter_start_object;
        p.filter_end_group = args->filter_end_group;
    }
    return moq_d18_encode_request_update(w, args->request_id, &p);
}

/* REQUEST_OK encode (zero-parameter form). The bidi stream correlates the
 * response, so no request id is carried. Used for PUBLISH_NAMESPACE_OK /
 * SUBSCRIBE_NAMESPACE_OK / SUBSCRIBE_TRACKS_OK: no parameters, empty Track
 * Properties. REQUEST_UPDATE_OK is a SEPARATE encoder (d18_encode_request_
 * update_ok below) -- it MAY carry LARGEST_OBJECT / EXPIRES. */
static moq_result_t d18_encode_request_ok(
    moq_session_t *s, struct moq_buf_writer *w, uint64_t request_id)
{
    (void)s; (void)request_id;
    return moq_d18_encode_request_ok(w);
}

/* REQUEST_UPDATE_OK (draft-18): a REQUEST_OK carrying the resolved
 * LARGEST_OBJECT and, draft-18 only, EXPIRES as response parameters. */
static moq_result_t d18_encode_request_update_ok(
    moq_session_t *s, struct moq_buf_writer *w,
    const moq_request_update_ok_encode_args_t *args)
{
    (void)s;
    moq_d18_msg_params_t p;
    memset(&p, 0, sizeof(p));
    p.has_largest = args->has_largest;
    p.largest_group = args->largest_group;
    p.largest_object = args->largest_object;
    p.has_expires = args->has_expires;
    p.expires_ms = args->expires_ms;
    return moq_d18_encode_request_update_ok(w, &p);
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

/* An Object Property block as the profile receives it. Draft-18 refuses every
 * structural or semantic failure (MOQ_ERR_PROTO, a session close). With
 * `track_scoped` the failures draft-22 makes Track-level are reported in
 * *malformed instead (§3.7, §10.7, §10.8, §10.9, §12.1). The Prior Group and
 * Object ID Gaps are reported for a block without such a failure. */
#define D18_OBJECT_PROP_PRIOR_GROUP_GAP  0x3Cu   /* §12.8 */
#define D18_OBJECT_PROP_PRIOR_OBJECT_GAP 0x3Eu   /* §12.9 */

_Static_assert(MOQ_OBJ_PROPS_UNPARSEABLE == MOQ_MALFORMED_PROPS_UNPARSEABLE &&
               MOQ_OBJ_PROPS_NESTED_IMMUTABLE == MOQ_MALFORMED_NESTED_IMMUTABLE &&
               MOQ_OBJ_PROPS_DUPLICATE_IMMUTABLE ==
                   MOQ_MALFORMED_DUPLICATE_IMMUTABLE &&
               MOQ_OBJ_PROPS_MANDATORY == MOQ_MALFORMED_MANDATORY_PROPERTY &&
               MOQ_OBJ_PROPS_DUPLICATE_GAP == MOQ_MALFORMED_DUPLICATE_GAP,
               "property violations share the malformed reason values");

moq_result_t moq_d18_op_scan_object_gaps(const uint8_t *props, size_t len,
                                         moq_obj_gaps_t *out)
{
    memset(out, 0, sizeof(*out));
    moq_obj_props_scan_t scan;
    moq_result_t rc = moq_d18_scan_object_properties(props, len, &scan);
    if (rc < 0) return rc;
    out->has_group_gap = scan.has_prior_group_gap;
    out->group_gap = scan.prior_group_gap;
    out->has_object_gap = scan.has_prior_object_gap;
    out->object_gap = scan.prior_object_gap;
    return MOQ_OK;
}

/* A Prior Group/Object ID Gap is one property whether it sits in the list or
 * inside Immutable Properties (§12.7, draft-22 §10.7), and an Object carries
 * at most one instance (§12.8/§12.9, draft-22 §10.8/§10.9): an equal value
 * already stated in either place is kept as it is, a different one or a
 * second instance is refused, and the Immutable Properties are never
 * rewritten. */
moq_result_t moq_d18_op_add_object_gap(const uint8_t *in, size_t len,
                                       bool object_gap, uint64_t value,
                                       uint8_t *out, size_t cap, size_t *out_len)
{
    return moq_kvp_list_merge_int(in, len, true,
                                  object_gap ? D18_OBJECT_PROP_PRIOR_OBJECT_GAP
                                             : D18_OBJECT_PROP_PRIOR_GROUP_GAP,
                                  value, D18_PROP_IMMUTABLE_PROPERTIES, out, cap, out_len);
}

static moq_result_t d18_object_props(const uint8_t *pp, size_t len,
                                     bool track_scoped, uint8_t *malformed,
                                     moq_obj_gaps_t *gaps)
{
    if (!track_scoped) {
        moq_result_t prc = moq_d18_validate_properties(pp, len);
        if (prc < 0) return prc;
    }
    moq_obj_props_scan_t scan;
    moq_result_t src = moq_d18_scan_object_properties(pp, len, &scan);
    if (src < 0) return src;
    if (track_scoped) *malformed = scan.malformed;
    if (scan.malformed == MOQ_OBJ_PROPS_OK) {
        gaps->has_group_gap = scan.has_prior_group_gap;
        gaps->group_gap = scan.prior_group_gap;
        gaps->has_object_gap = scan.has_prior_object_gap;
        gaps->object_gap = scan.prior_object_gap;
    }
    return MOQ_OK;
}

/* -- Fetch data plane (draft-18 §11.4.4) --------------------------- *
 * FETCH_HEADER (Request ID) leads a fetch data uni; objects follow with a vi64
 * Serialization Flags field. Object Properties (0x20) and the Datagram forwarding-
 * preference bit (0x40, §11.4.4.1 -- the subgroup LSBs are ignored) are supported
 * in both directions. Group reconstruction uses the request's retained order:
 * a descending response's Group ID Delta counts down (11.4.4.1). */
#define D18_FETCH_OBJ_SUBGROUP_MASK  0x03u
#define D18_FETCH_OBJ_OBJECT_DELTA   0x04u
#define D18_FETCH_OBJ_GROUP_DELTA    0x08u
#define D18_FETCH_OBJ_PRIORITY       0x10u
#define D18_FETCH_OBJ_PROPERTIES     0x20u
#define D18_FETCH_OBJ_DATAGRAM       0x40u
#define D18_FETCH_END_NON_EXISTENT   0x8Cu
#define D18_FETCH_END_UNKNOWN        0x10Cu

moq_result_t moq_d18_op_decode_fetch_header(
    moq_session_t *s, moq_buf_reader_t *r,
    moq_decoded_fetch_stream_header_t *out)
{
    (void)s;
    return moq_d18_decode_fetch_header(r, &out->request_id);
}

static moq_result_t d18_decode_fetch_object_op(
    moq_session_t *s, moq_buf_reader_t *r,
    moq_group_order_t effective_order,
    const moq_fetch_prior_object_t *prev, moq_decoded_fetch_object_t *out)
{
    bool track_scoped = s && s->profile && s->profile->malformed_track_local;
    if (effective_order != MOQ_GROUP_ORDER_ASCENDING &&
        effective_order != MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;
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
        /* Compare at full width: a length beyond the buffered input (which
         * includes any length above SIZE_MAX) waits for more bytes rather than
         * narrowing to a smaller size_t. */
        if (props_len > moq_buf_reader_remaining(r)) return MOQ_ERR_BUFFER;
        if (props_len > 0) {
            const uint8_t *pp = moq_buf_reader_ptr(r);
            moq_result_t prc = d18_object_props(pp, (size_t)props_len,
                                                track_scoped, &out->malformed,
                                                &out->gaps);
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
        } else if (effective_order == MOQ_GROUP_ORDER_DESCENDING) {
            if (group_delta >= pg) return MOQ_ERR_PROTO;
            group_id = pg - group_delta - 1;
        } else {
            if (pg == UINT64_MAX || group_delta > UINT64_MAX - pg - 1)
                return MOQ_ERR_PROTO;
            group_id = pg + group_delta + 1;
        }
    } else {
        group_id = pg;
    }
    if (!has_object) {
        /* An omitted Object Delta increments the prior ID across groups too. */
        if (po == UINT64_MAX) return MOQ_ERR_PROTO;
        object_id = po + 1;
    } else if (has_group) {
        object_id = obj_delta;
    } else {
        if (obj_delta > UINT64_MAX - po) return MOQ_ERR_PROTO;
        object_id = po + obj_delta;
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

moq_result_t moq_d18_op_encode_fetch_object(
    moq_session_t *s, moq_buf_writer_t *w,
    moq_group_order_t effective_order,
    const moq_fetch_object_encode_args_t *args,
    const moq_fetch_prior_object_t *prev)
{
    (void)s;
    if (effective_order != MOQ_GROUP_ORDER_ASCENDING &&
        effective_order != MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;
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
        if (!has_loc) {
            group_delta = args->group_id;
        } else if (effective_order == MOQ_GROUP_ORDER_DESCENDING) {
            if (args->group_id >= prev->group_id) return MOQ_ERR_INVAL;
            group_delta = prev->group_id - args->group_id - 1;
        } else {
            if (args->group_id <= prev->group_id) return MOQ_ERR_INVAL;
            group_delta = args->group_id - prev->group_id - 1;
        }
        flags |= D18_FETCH_OBJ_GROUP_DELTA | D18_FETCH_OBJ_OBJECT_DELTA;
        obj_delta = args->object_id;           /* absolute within the group */
    } else {
        /* Check progress before the omission predicate can wrap MAX + 1. */
        if (args->object_id <= prev->object_id) return MOQ_ERR_INVAL;
        if (args->object_id != prev->object_id + 1) {
            flags |= D18_FETCH_OBJ_OBJECT_DELTA;
            obj_delta = args->object_id - prev->object_id;
        }
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

moq_result_t moq_d18_op_encode_fetch_header(
    moq_session_t *s, struct moq_buf_writer *w, uint64_t request_id)
{
    (void)s;
    return moq_d18_encode_fetch_header(w, request_id);
}

/* Decode and dispatch a response message buffered on an outbound request bidi
 * stream. The core passes the request `kind`; the first response must be valid
 * for it (SUBSCRIBE_OK / FETCH_OK, or the shared REQUEST_ERROR). The core owns
 * the slot, buffering, and lifecycle. */
/* -- Data plane: subgroup streams ---------------------------------- *
 * Object Properties (vi64 KVP, §11.2.1.2) are decoded and encoded on subgroup
 * objects; datagrams are handled by the OBJECT_DATAGRAM ops. (FETCH data
 * streams are handled below.) */

uint32_t moq_d18_op_classify_data_stream(const uint8_t *data, size_t len)
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

moq_result_t moq_d18_op_decode_subgroup_header(
    moq_session_t *s, moq_buf_reader_t *r, moq_decoded_subgroup_header_t *out)
{
    (void)s;
    moq_d18_subgroup_header_t hdr;
    moq_result_t rc = moq_d18_decode_subgroup_header(r, &hdr);
    if (rc < 0) return rc;

    out->track_alias = hdr.track_alias;
    out->group_id = hdr.group_id;
    out->publisher_priority = hdr.publisher_priority;
    out->default_priority = hdr.default_priority;
    out->has_extensions = hdr.has_properties;
    out->end_of_group = hdr.end_of_group;
    out->first_object = hdr.first_object;
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

moq_result_t moq_d18_op_encode_subgroup_header(
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
    hdr.first_object = args->first_object;
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

moq_result_t moq_d18_op_encode_object_header(
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

moq_result_t moq_d18_op_encode_object_payload_prefix(moq_session_t *s,
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
moq_result_t moq_d18_op_validate_object_properties(moq_session_t *s,
                                                   const uint8_t *props,
                                                   size_t len)
{
    (void)s;
    return moq_d18_validate_properties(props, len);
}

moq_result_t moq_d18_op_decode_object_header(
    moq_session_t *s, moq_buf_reader_t *r, bool has_extensions,
    uint64_t prev_object_id, bool has_prev_object,
    moq_decoded_object_header_t *out)
{
    memset(out, 0, sizeof(*out));
    bool track_scoped = s && s->profile && s->profile->malformed_track_local;

    uint64_t delta;
    if (moq_buf_read_vi64(r, &delta) < 0) return MOQ_ERR_BUFFER;

    /* Object Properties (§11.4.2): a length-prefixed KVP block between the Object
     * ID Delta and the payload length. Surface the borrowed bytes; reject a
     * malformed block or a Mandatory Track Property carried as an object property. */
    if (has_extensions) {
        uint64_t props_len;
        if (moq_buf_read_vi64(r, &props_len) < 0) return MOQ_ERR_BUFFER;
        /* Compare at full width: a length beyond the buffered input (which
         * includes any length above SIZE_MAX) waits for more bytes rather than
         * narrowing to a smaller size_t. */
        if (props_len > moq_buf_reader_remaining(r)) return MOQ_ERR_BUFFER;
        if (props_len > 0) {
            const uint8_t *pp = moq_buf_reader_ptr(r);
            moq_result_t prc = d18_object_props(pp, (size_t)props_len,
                                                track_scoped, &out->malformed,
                                                &out->gaps);
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

moq_result_t moq_d18_op_encode_unsubscribe(
    moq_session_t *s, struct moq_buf_writer *w, uint64_t request_id)
{ (void)s; (void)w; (void)request_id; return MOQ_ERR_INVAL; }

moq_result_t moq_d18_op_classify_bidi_stream(moq_session_t *s,
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
moq_result_t moq_d18_op_decode_object_datagram(
    moq_session_t *s, const uint8_t *data, size_t len,
    struct moq_decoded_object_datagram *out)
{
    memset(out, 0, sizeof(*out));

    moq_d18_object_datagram_t dg;
    moq_obj_props_scan_t scan;
    bool track_scoped = s->profile->malformed_track_local;
    moq_result_t rc = moq_d18_decode_object_datagram_scan(data, len, track_scoped,
                                                          &dg, &scan);
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
    out->track_alias        = dg.track_alias;
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
    if (track_scoped) out->malformed = scan.malformed;
    if (out->malformed == MOQ_MALFORMED_NONE) {
        out->gaps.has_group_gap = scan.has_prior_group_gap;
        out->gaps.group_gap = scan.prior_group_gap;
        out->gaps.has_object_gap = scan.has_prior_object_gap;
        out->gaps.object_gap = scan.prior_object_gap;
    }
    return MOQ_OK;
}

moq_result_t moq_d18_op_encode_object_datagram(
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

moq_result_t moq_d18_op_grant_capacity(moq_session_t *s,
                                       uint64_t new_capacity,
                                       uint64_t now_us)
{
    (void)s; (void)new_capacity; (void)now_us;
    return MOQ_ERR_INVAL;
}

uint64_t moq_d18_op_request_capacity(const moq_session_t *s)
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
    uint64_t timeout_ms = s->goaway_timeout_us / 1000;
    return moq_d18_encode_goaway(w, args->uri, args->uri_len, timeout_ms,
                                 moq_request_stream_peer_next_id(s));
}


/* -- Request-stream codec table ------------------------------------- *
 * Draft-18 bodies decoded into the core's records for the shared engine
 * (request_stream.c). Each adapter zeroes its record, maps omitted Message
 * Parameters onto their protocol defaults, and reports the token list; the
 * engine admits the request ID and resolves tokens afterwards. */

/* Decoded tokens in wire order, alias ops mapped onto the cache operations. */
static void d18_tokens_out(const moq_d18_msg_params_t *p, moq_request_tokens_t *tk)
{
    tk->count = p->auth_token_count;
    for (size_t i = 0; i < p->auth_token_count; i++) {
        const moq_d18_auth_token_t *w = &p->auth_tokens[i];
        moq_decoded_auth_token_t *d = &tk->in[i];
        switch (w->alias_type) {
        case MOQ_AUTH_TOKEN_DELETE:    d->op = MOQ_AUTH_OP_DELETE; break;
        case MOQ_AUTH_TOKEN_REGISTER:  d->op = MOQ_AUTH_OP_REGISTER; break;
        case MOQ_AUTH_TOKEN_USE_ALIAS: d->op = MOQ_AUTH_OP_USE_ALIAS; break;
        default:                       d->op = MOQ_AUTH_OP_USE_VALUE; break;
        }
        d->alias = w->alias;
        d->token_type = w->token_type;
        d->token_value = w->token_value.data;
        d->token_value_len = w->token_value.len;
    }
}

/* A malformed parameter block has its own close code at the engine. */
static moq_result_t d18_rq_rc(moq_result_t rc)
{
    return rc == MOQ_D18_ERR_KVP_FORMAT ? MOQ_REQUEST_DECODE_BAD_PARAMS : rc;
}

static moq_result_t d18_rq_envelope(const uint8_t *buf, size_t len,
                                    moq_request_envelope_t *env, size_t *consumed)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, buf, len);
    moq_control_envelope_t e;
    moq_result_t rc = moq_d18_decode_envelope(&r, &e);
    if (rc < 0) return rc;
    env->msg_type = e.msg_type;
    env->payload = e.payload;
    env->payload_len = e.payload_len;
    *consumed = moq_buf_reader_offset(&r);
    return MOQ_OK;
}

static moq_result_t d18_rq_subscribe(const uint8_t *p, size_t n,
                                     moq_decoded_subscribe_t *d,
                                     uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d18_subscribe_t sub;
    moq_result_t rc = moq_d18_decode_subscribe(p, n, d->track_namespace_parts,
                                               MOQ_DECODED_MAX_NAMESPACE_PARTS, &sub);
    if (rc < 0) return d18_rq_rc(rc);
    d->track_namespace = sub.track_namespace; /* parts = d->track_namespace_parts */
    d->track_name = sub.track_name;
    d->request_id = sub.request_id;
    /* Surface the decoded Message Parameters (defaults apply when omitted)
     * so the publisher/relay sees the real request. */
    d->subscriber_priority = sub.params.has_subscriber_priority
        ? sub.params.subscriber_priority : 128;
    d->group_order = sub.params.has_group_order
        ? sub.params.group_order : MOQ_GROUP_ORDER_DEFAULT;
    d->has_forward = true;
    d->forward = sub.params.has_forward ? (sub.params.forward != 0) : true;
    d->has_filter = sub.params.has_filter;
    d->filter_type = sub.params.has_filter
        ? sub.params.filter_type : MOQ_SUBSCRIBE_FILTER_NONE;
    d->start_group = sub.params.filter_start_group;
    d->start_object = sub.params.filter_start_object;
    d->end_group = sub.params.filter_end_group;
    D18_FILL_DT(d, &sub.params);
    (void)d18_map_delivery_timeout(&sub.params, &d->has_delivery_timeout,
                                   &d->delivery_timeout_us);
    d->has_new_group_request = sub.params.has_new_group_request;
    d->new_group_request = sub.params.new_group_request;
    *request_id = sub.request_id;
    d18_tokens_out(&sub.params, tk);
    return MOQ_OK;
}

static moq_result_t d18_rq_publish(const uint8_t *p, size_t n,
                                   moq_decoded_publish_t *d,
                                   uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d18_publish_t pub;
    moq_result_t rc = moq_d18_decode_publish(p, n, d->track_namespace_parts,
                                             MOQ_DECODED_MAX_NAMESPACE_PARTS, &pub);
    if (rc < 0) return d18_rq_rc(rc);
    d->track_namespace = pub.track_namespace; /* parts = d->track_namespace_parts */
    d->track_name = pub.track_name;
    d->request_id = pub.request_id;
    d->track_alias = pub.track_alias;
    /* Omitted FORWARD means forward = 1 (§5.4.1). */
    d->has_forward = true;
    d->forward = pub.params.has_forward ? (pub.params.forward != 0) : true;
    d->track_properties = pub.track_properties.data;
    d->track_properties_len = pub.track_properties.len;
    d->track_properties_unsupported = pub.track_properties_unsupported;
    d->dynamic_groups = pub.dynamic_groups;
    d->has_largest = pub.params.has_largest;
    d->largest_group = pub.params.largest_group;
    d->largest_object = pub.params.largest_object;
    d->has_expires = pub.params.has_expires;
    d->expires_ms = pub.params.expires_ms;
    *request_id = pub.request_id;
    d18_tokens_out(&pub.params, tk);
    return MOQ_OK;
}

static moq_result_t d18_rq_fetch(const uint8_t *p, size_t n,
                                 moq_decoded_fetch_t *fd,
                                 uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(fd, 0, sizeof(*fd));
    fd->joining_sub_slot = -1;
    fd->joining_pub_slot = -1;
    moq_d18_fetch_t f;
    moq_result_t rc = moq_d18_decode_fetch(p, n, fd->track_namespace_parts,
                                           MOQ_DECODED_MAX_NAMESPACE_PARTS, &f);
    if (rc < 0) return d18_rq_rc(rc);
    fd->request_id = f.request_id;
    fd->fetch_type = (uint32_t)f.fetch_type;
    fd->track_namespace = f.track_namespace; /* parts = fd->track_namespace_parts */
    fd->track_name = f.track_name;
    fd->start_group = f.start.group;
    fd->start_object = f.start.object;
    fd->end_group = f.end.group;
    fd->end_object = f.end.object;
    fd->joining_request_id = f.joining_request_id;
    fd->joining_start = f.joining_start;
    fd->subscriber_priority = f.params.has_subscriber_priority
        ? f.params.subscriber_priority : 128;
    /* Omitted GROUP_ORDER on a FETCH means Ascending (§10.2.8). */
    fd->group_order = f.params.has_group_order
        ? f.params.group_order : MOQ_GROUP_ORDER_ASCENDING;
    *request_id = f.request_id;
    d18_tokens_out(&f.params, tk);
    return MOQ_OK;
}

static moq_result_t d18_rq_publish_namespace(const uint8_t *p, size_t n,
                                             moq_decoded_publish_namespace_t *d,
                                             uint64_t *request_id,
                                             moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d18_publish_namespace_t pn;
    moq_result_t rc = moq_d18_decode_publish_namespace(p, n, d->track_namespace_parts,
                                                       MOQ_DECODED_MAX_NAMESPACE_PARTS, &pn);
    if (rc < 0) return d18_rq_rc(rc);
    d->track_namespace = pn.track_namespace; /* parts = d->track_namespace_parts */
    d->request_id = pn.request_id;
    *request_id = pn.request_id;
    d18_tokens_out(&pn.params, tk);
    return MOQ_OK;
}

static moq_result_t d18_rq_track_status(const uint8_t *p, size_t n,
                                        moq_decoded_track_status_request_t *d,
                                        uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d18_track_status_t ts;
    moq_result_t rc = moq_d18_decode_track_status(p, n, d->track_namespace_parts,
                                                  MOQ_DECODED_MAX_NAMESPACE_PARTS, &ts);
    if (rc < 0) return d18_rq_rc(rc);
    d->track_namespace = ts.track_namespace; /* parts = d->track_namespace_parts */
    d->track_name = ts.track_name;
    *request_id = ts.request_id;
    d18_tokens_out(&ts.params, tk);
    return MOQ_OK;
}

static moq_result_t d18_rq_subscribe_tracks(const uint8_t *p, size_t n,
                                            moq_decoded_subscribe_tracks_request_t *d,
                                            uint64_t *request_id,
                                            moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d18_subscribe_tracks_t st;
    moq_result_t rc = moq_d18_decode_subscribe_tracks(p, n, d->prefix_parts,
                                                      MOQ_DECODED_MAX_NAMESPACE_PARTS, &st);
    if (rc < 0) return d18_rq_rc(rc);
    d->track_namespace_prefix = st.track_namespace_prefix; /* parts = d->prefix_parts */
    d->forward = st.params.has_forward ? (st.params.forward != 0) : true;
    *request_id = st.request_id;
    d18_tokens_out(&st.params, tk);
    return MOQ_OK;
}

static moq_result_t d18_rq_request_update(const uint8_t *p, size_t n,
                                          moq_request_family_t target_family,
                                          bool from_subscriber,
                                          moq_decoded_request_update_t *d,
                                          moq_request_tokens_t *tk)
{
    (void)from_subscriber;
    moq_d18_request_update_t u;
    moq_result_t rc = moq_d18_decode_request_update(p, n, &u);
    if (rc < 0) return d18_rq_rc(rc);
    /* §10.2.1: a parameter in a message its definition does not name closes
     * the session with PROTOCOL_VIOLATION. FORWARD, SUBSCRIPTION_FILTER and
     * NEW_GROUP_REQUEST appear in a REQUEST_UPDATE "for a subscription"
     * (one begun by SUBSCRIBE or by PUBLISH), SUBSCRIBER_PRIORITY "for a
     * subscription or FETCH". AUTHORIZATION_TOKEN and the two delivery
     * timeouts name REQUEST_UPDATE without a family. */
    bool subscription = target_family == MOQ_REQUEST_FAMILY_SUBSCRIBE ||
                        target_family == MOQ_REQUEST_FAMILY_PUBLISH;
    if (!subscription &&
        (u.params.has_forward || u.params.has_filter ||
         u.params.has_new_group_request ||
         (u.params.has_subscriber_priority &&
          target_family != MOQ_REQUEST_FAMILY_FETCH)))
        return MOQ_ERR_PROTO;
    memset(d, 0, sizeof(*d));
    d->request_id = u.request_id;
    d->has_forward = u.params.has_forward;
    d->forward = u.params.forward != 0;
    d->has_subscriber_priority = u.params.has_subscriber_priority;
    d->subscriber_priority = u.params.subscriber_priority;
    d->has_filter = u.params.has_filter;
    d->filter_type = u.params.filter_type;
    d->start_group = u.params.filter_start_group;
    d->start_object = u.params.filter_start_object;
    d->end_group = u.params.filter_end_group;
    D18_FILL_DT(d, &u.params);
    (void)d18_map_delivery_timeout(&u.params, &d->has_delivery_timeout,
                                   &d->delivery_timeout_us);
    d->has_new_group_request = u.params.has_new_group_request;
    d->new_group_request = u.params.new_group_request;
    d18_tokens_out(&u.params, tk);
    return MOQ_OK;
}

static moq_result_t d18_rq_goaway(const uint8_t *p, size_t n,
                                  moq_request_goaway_msg_t *out)
{
    moq_d18_goaway_t ga;
    moq_result_t rc = moq_d18_decode_goaway_request(p, n, &ga);
    if (rc < 0) return rc;
    out->uri = ga.uri;
    out->timeout_ms = ga.timeout_ms;
    return MOQ_OK;
}

static moq_result_t d18_rq_publish_done(const uint8_t *p, size_t n,
                                        moq_request_publish_done_msg_t *out)
{
    moq_d18_publish_done_t pd;
    moq_result_t rc = moq_d18_decode_publish_done(p, n, &pd);
    if (rc < 0) return rc;
    out->status_code = pd.status_code;
    out->stream_count = pd.stream_count;
    out->reason = pd.reason;
    return MOQ_OK;
}

static moq_result_t d18_rq_request_error(const uint8_t *p, size_t n,
                                         moq_bytes_t *rd_parts, size_t rd_parts_max,
                                         moq_request_error_msg_t *out,
                                         moq_decoded_redirect_t *rd)
{
    moq_d18_request_error_t er;
    moq_d18_redirect_t wire_rd;
    moq_result_t rc = moq_d18_decode_request_error_redirect(p, n, rd_parts,
                                                            rd_parts_max, &er, &wire_rd);
    if (rc < 0) return rc;
    out->error_code = er.error_code;
    out->retry_interval = er.retry_interval;
    out->reason = er.reason;
    out->is_redirect = (er.error_code == MOQ_D18_ERROR_REDIRECT);
    if (out->is_redirect) {
        rd->connect_uri = wire_rd.connect_uri.data;
        rd->connect_uri_len = wire_rd.connect_uri.len;
        rd->track_namespace = wire_rd.track_namespace;   /* parts in rd_parts */
        rd->track_name = wire_rd.track_name.data;
        rd->track_name_len = wire_rd.track_name.len;
    }
    return MOQ_OK;
}

static moq_result_t d18_rq_subscribe_ok(const uint8_t *p, size_t n,
                                        moq_decoded_subscribe_ok_t *d)
{
    moq_d18_subscribe_ok_t ok;
    moq_result_t rc = moq_d18_decode_subscribe_ok(p, n, &ok);
    if (rc < 0) return rc;
    memset(d, 0, sizeof(*d));
    d->track_alias = ok.track_alias;
    d->has_largest = ok.params.has_largest;
    d->largest_group = ok.params.largest_group;
    d->largest_object = ok.params.largest_object;
    d->has_expires = ok.params.has_expires;
    d->expires_ms = ok.params.expires_ms;
    d->track_properties = ok.track_properties.data;
    d->track_properties_len = ok.track_properties.len;
    d->track_properties_unsupported = ok.track_properties_unsupported;
    d->dynamic_groups = ok.dynamic_groups;
    return MOQ_OK;
}

static moq_result_t d18_rq_fetch_ok(const uint8_t *p, size_t n,
                                    moq_decoded_fetch_ok_t *d)
{
    moq_d18_fetch_ok_t ok;
    moq_result_t rc = moq_d18_decode_fetch_ok(p, n, &ok);
    if (rc < 0) return rc;
    memset(d, 0, sizeof(*d));
    d->end_of_track = ok.end_of_track;
    d->end_group = ok.end.group;
    d->end_object = ok.end.object;
    d->track_properties = ok.track_properties.data;
    d->track_properties_len = ok.track_properties.len;
    d->track_properties_unsupported = ok.track_properties_unsupported;
    return MOQ_OK;
}

static moq_result_t d18_rq_request_ok(const uint8_t *p, size_t n)
{
    return moq_d18_decode_request_ok(p, n);
}

static moq_result_t d18_rq_track_status_ok(const uint8_t *p, size_t n,
                                           moq_decoded_track_status_ok_t *d)
{
    moq_d18_track_status_ok_t ok;
    moq_result_t rc = moq_d18_decode_track_status_ok(p, n, &ok);
    if (rc < 0) return rc;
    memset(d, 0, sizeof(*d));
    d->has_largest = ok.params.has_largest;
    d->largest_group = ok.params.largest_group;
    d->largest_object = ok.params.largest_object;
    d->has_expires = ok.params.has_expires;
    d->expires_ms = ok.params.expires_ms;
    d->track_properties = ok.track_properties.data;
    d->track_properties_len = ok.track_properties.len;
    return MOQ_OK;
}

static moq_result_t d18_rq_update_ok(const uint8_t *p, size_t n,
                                     moq_request_update_ok_msg_t *out)
{
    moq_d18_msg_params_t up;
    moq_result_t rc = moq_d18_decode_request_update_ok(p, n, &up);
    if (rc < 0) return rc;
    out->has_largest = up.has_largest;
    out->largest_group = up.largest_group;
    out->largest_object = up.largest_object;
    out->has_expires = up.has_expires;
    out->expires_ms = up.expires_ms;
    return MOQ_OK;
}

static moq_result_t d18_rq_publish_blocked(const uint8_t *p, size_t n,
                                           moq_decoded_publish_blocked_t *d)
{
    memset(d, 0, sizeof(*d));
    moq_d18_publish_blocked_t pb;
    moq_result_t rc = moq_d18_decode_publish_blocked(p, n, d->suffix_parts,
                                                     MOQ_DECODED_MAX_NAMESPACE_PARTS, &pb);
    if (rc < 0) return rc;
    d->track_namespace_suffix = pb.track_namespace_suffix; /* parts = d->suffix_parts */
    d->track_name = pb.track_name;
    return MOQ_OK;
}

static moq_result_t d18_rq_publish_ok(const uint8_t *p, size_t n,
                                      moq_decoded_publish_ok_t *d)
{
    moq_d18_publish_ok_t ok;
    moq_result_t rc = moq_d18_decode_publish_ok(p, n, &ok);
    if (rc < 0) return rc;
    memset(d, 0, sizeof(*d));
    /* Omitted FORWARD means forward = 1 (§5.4.1). */
    d->forward = ok.params.has_forward ? (ok.params.forward != 0) : true;
    d->subscriber_priority = ok.params.has_subscriber_priority
        ? ok.params.subscriber_priority : 128;
    d->group_order = ok.params.has_group_order
        ? ok.params.group_order : MOQ_GROUP_ORDER_DEFAULT;
    /* Legacy _ms projection stays EXACT milliseconds (§9.8 A5): the
     * min_nonzero of this message's carriers, no unit conversion. */
    D18_FILL_DT(d, &ok.params);
    d->has_delivery_timeout = ok.params.has_object_delivery_timeout ||
                              ok.params.has_subgroup_delivery_timeout;
    d->delivery_timeout_ms = dt_negotiate_ms(
        ok.params.has_object_delivery_timeout,
        ok.params.object_delivery_timeout_ms,
        ok.params.has_subgroup_delivery_timeout,
        ok.params.subgroup_delivery_timeout_ms);
    d->has_new_group_request = ok.params.has_new_group_request;
    d->new_group_request = ok.params.new_group_request;
    d->has_expires = ok.params.has_expires;
    d->expires_ms = ok.params.expires_ms;
    d->has_filter = ok.params.has_filter;
    d->filter_type = ok.params.filter_type;
    d->filter_start_group = ok.params.filter_start_group;
    d->filter_start_object = ok.params.filter_start_object;
    d->filter_end_group = ok.params.filter_end_group;
    return MOQ_OK;
}

static const moq_request_codec_t d18_request_codec = {
    .decode_envelope          = d18_rq_envelope,
    .type = {
        .subscribe           = MOQ_D18_SUBSCRIBE,
        .publish             = MOQ_D18_PUBLISH,
        .fetch               = MOQ_D18_FETCH,
        .publish_namespace   = MOQ_D18_PUBLISH_NAMESPACE,
        .track_status        = MOQ_D18_TRACK_STATUS,
        .subscribe_tracks    = MOQ_D18_SUBSCRIBE_TRACKS,
        .subscribe_namespace = MOQ_D18_SUBSCRIBE_NAMESPACE,
        .request_update      = MOQ_D18_REQUEST_UPDATE,
        .request_ok          = MOQ_D18_REQUEST_OK,
        .request_error       = MOQ_D18_REQUEST_ERROR,
        .publish_done        = MOQ_D18_PUBLISH_DONE,
        .publish_blocked     = MOQ_D18_PUBLISH_BLOCKED,
        .goaway              = MOQ_D18_GOAWAY,
        .subscribe_ok        = MOQ_D18_SUBSCRIBE_OK,
        .fetch_ok            = MOQ_D18_FETCH_OK,
    },
    .decode_subscribe         = d18_rq_subscribe,
    .decode_publish           = d18_rq_publish,
    .decode_fetch             = d18_rq_fetch,
    .decode_publish_namespace = d18_rq_publish_namespace,
    .decode_track_status      = d18_rq_track_status,
    .decode_subscribe_tracks  = d18_rq_subscribe_tracks,
    .decode_request_update    = d18_rq_request_update,
    .decode_goaway_request    = d18_rq_goaway,
    .decode_publish_done      = d18_rq_publish_done,
    .decode_request_error     = d18_rq_request_error,
    .decode_subscribe_ok      = d18_rq_subscribe_ok,
    .decode_fetch_ok          = d18_rq_fetch_ok,
    .decode_request_ok        = d18_rq_request_ok,
    .decode_track_status_ok   = d18_rq_track_status_ok,
    .decode_update_ok         = d18_rq_update_ok,
    .decode_publish_blocked   = d18_rq_publish_blocked,
    .decode_publish_ok        = d18_rq_publish_ok,
};

/* -- Vtable -------------------------------------------------------- *
 * Fully populated. The d18_unimpl_* entries are defensive stubs for ops that
 * are unreachable on draft-18 (UNSUBSCRIBE has no wire message; bidi classify
 * is pre-empted by the request-stream router) or intentionally absent
 * (request-capacity: draft-18 has no MAX_REQUEST_ID). */
/*
 * Draft-18 §15 (:6411-6415): "Receipt of an unknown error code in any error
 * context ... MUST be treated as equivalent to INTERNAL_ERROR for that
 * context." The registry is §15.10.2 (:6839) -- 18 codes. The GREASE row
 * (§14, 0x7f*N + 0x9D) is reserved precisely so implementations exercise
 * values they do not understand, so it is NOT a semantic code and falls
 * under the same rule.
 */
static bool d18_request_error_registered(uint64_t raw)
{
    switch (raw) {
    case MOQ_REQUEST_ERROR_INTERNAL_ERROR:
    case MOQ_REQUEST_ERROR_UNAUTHORIZED:
    case MOQ_REQUEST_ERROR_TIMEOUT:
    case MOQ_REQUEST_ERROR_NOT_SUPPORTED:
    case MOQ_REQUEST_ERROR_MALFORMED_AUTH_TOKEN:
    case MOQ_REQUEST_ERROR_EXPIRED_AUTH_TOKEN:
    case MOQ_REQUEST_ERROR_GOING_AWAY:
    case MOQ_REQUEST_ERROR_EXCESSIVE_LOAD:
    case MOQ_REQUEST_ERROR_DOES_NOT_EXIST:
    case MOQ_REQUEST_ERROR_INVALID_RANGE:
    case MOQ_REQUEST_ERROR_MALFORMED_TRACK:
    case MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION:
    case MOQ_REQUEST_ERROR_UNINTERESTED:
    case MOQ_REQUEST_ERROR_PREFIX_OVERLAP:
    case MOQ_REQUEST_ERROR_NAMESPACE_TOO_LARGE:
    case MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID:
    case MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION:
    case MOQ_REQUEST_ERROR_REDIRECT:
        return true;
    default:
        return false;
    }
}

static moq_request_error_t d18_semantic_request_error(uint64_t raw)
{
    if (d18_request_error_registered(raw))
        return (moq_request_error_t)raw;
    return MOQ_REQUEST_ERROR_INTERNAL_ERROR;
}

static const moq_profile_ops_t d18_ops = {
    .version                 = MOQ_VERSION_DRAFT_18,
    .state_size              = sizeof(moq_d18_profile_state_t),
    .state_align             = _Alignof(moq_d18_profile_state_t),
    .init_in_place           = d18_init_in_place,
    .destroy                 = d18_destroy,
    .start                   = d18_start,
    .process_control_data    = d18_process_control_data,
    .min_track_namespace_fields = 0,
    .uses_request_streams    = true,
    .object_payload_len_max  = MOQ_VI64_MAX,
    .location_varint_max     = MOQ_VI64_MAX,
    .fetch_datagram_supported = true,   /* fetch object header carries the datagram bit */
    .request_error_wire_max  = MOQ_VI64_MAX,
    .semantic_request_error  = d18_semantic_request_error,
    .fetch_descending_supported = true,   /* response deltas in request order */
    .uses_uni_control_channel = true,
    .wire_ready               = true,
    .request_retains_pending_original_input = false,
    .request_fin_is_not_cancel = false,
    .premature_fin_fails_request = false,
    .request_drain_requires_receive_terminal = false,
    .allows_concurrent_subscriptions = false,
    .duplicate_subscription_error = MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION,
    .publish_initial_parameters = false,
    .publish_ok_carries_params = true,
    .supports_joining_fetch   = true,
    .supports_fill            = false,
    .stream_count_unknown     = MOQ_QUIC_VARINT_MAX,
    .subgroup_timer_at_fin    = true,
    .object_timer_at_header   = false,
    .first_object_timeout_override = false,
    .publish_done_subscription_ended = true,
    .redirect_on_any_request = false,
    .request_update_errors_coalesce = true,
    .request_updates_max     = 0,   /* no bound is advertised */
    .request_updates_exceeded_error = 0,
    .going_away_request_error       = MOQ_REQUEST_ERROR_GOING_AWAY,
    .capacity_request_error         = MOQ_REQUEST_ERROR_EXCESSIVE_LOAD,
    .capacity_request_error_retryable = true,
    .reserved_session_namespaces    = true,
    .binding_requirements     = 0,
    .auth_token_type_max      = UINT64_MAX,   /* §10.2.2: vi64 */
    .classify_uni_stream     = moq_d18_op_classify_uni_stream,
    /* Request admission + SUBSCRIBE outbound. */
    .prepare_request         = moq_request_stream_prepare_request,
    .commit_request          = moq_request_stream_commit_request,
    .abort_request           = moq_request_stream_abort_request,
    .release_request         = moq_request_stream_release_request,
    .next_track_alias        = moq_request_stream_next_track_alias,
    .advance_track_alias     = moq_request_stream_advance_track_alias,
    .validate_inbound_request_stream = moq_request_stream_validate_inbound,
    .commit_inbound_request  = moq_request_stream_commit_inbound,
    .process_request_stream  = moq_request_stream_process_request,
    .process_response_stream = moq_request_stream_process_response,
    .request_codec           = &d18_request_codec,
    .encode_own_setup        = d18_encode_own_setup,
    .scan_delivery_timeouts  = moq_d18_op_scan_delivery_timeouts,
    .scan_default_publisher_priority = moq_d18_scan_default_publisher_priority,
    .validate_track_properties = moq_d18_validate_track_properties,
    .encode_subscribe        = d18_encode_subscribe,
    .encode_subscribe_ok     = d18_encode_subscribe_ok,
    .encode_request_error    = d18_encode_request_error,
    .encode_request_update   = d18_encode_request_update,
    .encode_request_ok       = d18_encode_request_ok,
    .encode_request_update_ok = d18_encode_request_update_ok,
    .encode_publish_done     = d18_encode_publish_done,
    /* Data plane: subgroup object streams. */
    .classify_data_stream    = moq_d18_op_classify_data_stream,
    .decode_subgroup_header  = moq_d18_op_decode_subgroup_header,
    .encode_subgroup_header  = moq_d18_op_encode_subgroup_header,
    .encode_object_header    = moq_d18_op_encode_object_header,
    .encode_request_goaway   = d18_encode_request_goaway,
    .encode_object_payload_prefix = moq_d18_op_encode_object_payload_prefix,
    .validate_object_properties = moq_d18_op_validate_object_properties,
    .decode_object_header    = moq_d18_op_decode_object_header,
    /* Not-yet-implemented guards (first op on each reachable path). */
    .encode_fetch            = d18_encode_fetch_op,
    .encode_fetch_ok         = d18_encode_fetch_ok_op,
    .encode_fetch_header     = moq_d18_op_encode_fetch_header,
    .decode_fetch_header     = moq_d18_op_decode_fetch_header,
    .encode_fetch_object     = moq_d18_op_encode_fetch_object,
    .decode_fetch_object     = d18_decode_fetch_object_op,
    .encode_fetch_range      = d18_encode_fetch_range_op,
    .encode_publish          = d18_encode_publish,
    .track_properties_dynamic_groups = moq_d18_op_track_properties_dynamic_groups,
    .track_properties_add_dynamic_groups = moq_d18_op_track_properties_add_dynamic_groups,
    .track_properties_group_order = moq_d18_op_track_properties_group_order,
    .track_properties_add_group_order = moq_d18_op_track_properties_add_group_order,
    .track_properties_max_cache = moq_d18_op_track_properties_max_cache,
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
    .encode_unsubscribe      = moq_d18_op_encode_unsubscribe,
    .classify_bidi_stream    = moq_d18_op_classify_bidi_stream,
    .encode_object_datagram  = moq_d18_op_encode_object_datagram,
    .decode_object_datagram  = moq_d18_op_decode_object_datagram,
    .grant_capacity          = moq_d18_op_grant_capacity,
    .peer_request_capacity   = moq_d18_op_request_capacity,
    .local_request_capacity  = moq_d18_op_request_capacity,
    .encode_goaway           = d18_encode_goaway,
    .relative_group_clamps   = false,
    /* §12.8, §12.9: Prior Group ID Gap and Prior Object ID Gap are draft-18
     * Object Properties. */
    .scan_object_gaps        = moq_d18_op_scan_object_gaps,
    .add_object_gap          = moq_d18_op_add_object_gap,
    /* §2.4.2: "When a subscriber detects a Malformed Track, it MUST cancel
     * any corresponding subscription or fetches for that Track from that
     * publisher". */
    .malformed_track_local   = true,
};

const moq_profile_ops_t *moq_d18_profile_ops(void)
{
    return &d18_ops;
}
