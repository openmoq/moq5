/*
 * Draft-22 wire profile.
 *
 * Setup handshake over a unidirectional control channel per side, GOAWAY,
 * the codec adapters that feed the shared request-stream engine, and the
 * outbound encoders for every request family. The data-plane framing is
 * draft-18's, which draft-22 did not change; those vtable slots are taken
 * from the draft-18 table at first use. The profile is wire-ready and included
 * in default offers when the selected transport backend can carry it.
 */

#include "session_internal.h"
#include "request_stream_internal.h"
#include "profile_d18_ops_internal.h"
#include "../wire/control_d22_internal.h"
#include "../wire/control_d18_internal.h"
#include "../wire/messages_d22_internal.h"
#include "location_d22_internal.h"
#include "moq/vi64.h"

typedef struct moq_d22_profile_state {
    /* Request-stream engine state; must stay first (see request_stream_internal.h). */
    moq_request_stream_state_t rs;
    moq_version_t version;
    bool          setup_sent;
    bool          setup_received;
    /* Peer limits from its SETUP; zero means unlimited. */
    uint64_t      peer_max_filter_ranges;
    uint64_t      peer_max_request_updates;
} moq_d22_profile_state_t;

_Static_assert(offsetof(moq_d22_profile_state_t, rs) == 0,
               "request-stream state leads the draft-22 profile state");

/* Setup Option types (§9.1). */
#define D22_OPT_PATH                      UINT64_C(0x01)
#define D22_OPT_AUTHORIZATION_TOKEN       UINT64_C(0x03)
#define D22_OPT_MAX_AUTH_TOKEN_CACHE_SIZE UINT64_C(0x04)
#define D22_OPT_AUTHORITY                 UINT64_C(0x05)
#define D22_OPT_MAX_FILTER_RANGES         UINT64_C(0x06)
#define D22_OPT_MOQT_IMPLEMENTATION       UINT64_C(0x07)
#define D22_OPT_MAX_REQUEST_UPDATES       UINT64_C(0x08)

/* Advertised limits: no Range Filters, one outstanding REQUEST_UPDATE per
 * request stream (§9.1.6, §9.1.7). */
#define D22_LOCAL_MAX_FILTER_RANGES   UINT64_C(0)
#define D22_LOCAL_MAX_REQUEST_UPDATES UINT64_C(1)
#define D22_ERR_TOO_MANY_REQUEST_UPDATES UINT64_C(0x1b)

static void d22_init_in_place(void *profile_state, const moq_session_cfg_t *cfg)
{
    moq_d22_profile_state_t *p = (moq_d22_profile_state_t *)profile_state;
    memset(p, 0, sizeof(*p));
    moq_request_stream_state_init(&p->rs, cfg->perspective, cfg->alloc);
    p->version = MOQ_VERSION_DRAFT_22;
}

static void d22_destroy(void *profile_state)
{
    moq_d22_profile_state_t *p = (moq_d22_profile_state_t *)profile_state;
    moq_request_stream_state_destroy(&p->rs);
}

/* Both sides open their own control channel and send SETUP without waiting.
 * A client carried natively over QUIC names its connection target: PATH is
 * the URI's path-abempty with "?" and the query appended when present, and
 * AUTHORITY is the URI's authority component (§9.1.1, §9.1.2). Neither is
 * sent by a server or over WebTransport. Local privacy policy omits the
 * optional implementation identifier (§9.1.5); peer identifiers remain legal.
 * SETUP is encoded straight into
 * the send arena, so a target up to the 16-bit body limit needs no scratch
 * allocation; a body that can never fit the arena is MOQ_ERR_BUFFER, one that
 * does not fit right now is MOQ_ERR_WOULD_BLOCK, and both leave the session
 * IDLE with the arena cursor untouched. */
static moq_result_t d22_start(moq_session_t *s)
{
    moq_d22_profile_state_t *p = (moq_d22_profile_state_t *)s->profile_state;
    if (moq_binding_required_unbound(s)) return MOQ_ERR_WRONG_STATE;
    if (s->state != MOQ_SESS_IDLE)
        return MOQ_ERR_WRONG_STATE;
    const moq_transport_binding_t *binding = &s->binding.value;
    bool target = binding->kind == MOQ_BINDING_RAW_QUIC &&
                  s->perspective == MOQ_PERSPECTIVE_CLIENT;
    moq_d22_kvp_t options[5 + MOQ_DECODED_MAX_TOKENS];
    size_t n = 0;
    if (target) {
        options[n].type = D22_OPT_PATH;
        options[n].bytes = binding->path_query;
        n++;
    }
    /* §9.1.4: one AUTHORIZATION TOKEN option per configured token, each a
     * Token structure carrying its value (encoded at create). */
    for (size_t i = 0; i < s->setup_token_count; i++) {
        options[n].type = D22_OPT_AUTHORIZATION_TOKEN;
        options[n].bytes = s->setup_token_wire[i];
        n++;
    }
    if (s->send_auth_token_cache_size) {
        options[n].type = D22_OPT_MAX_AUTH_TOKEN_CACHE_SIZE;
        options[n].integer = s->auth_token_cache_size;
        n++;
    }
    if (target) {
        options[n].type = D22_OPT_AUTHORITY;
        options[n].bytes = binding->authority;
        n++;
    }
    options[n].type = D22_OPT_MAX_FILTER_RANGES;
    options[n].integer = D22_LOCAL_MAX_FILTER_RANGES;
    n++;
    options[n].type = D22_OPT_MAX_REQUEST_UPDATES;
    options[n].integer = D22_LOCAL_MAX_REQUEST_UPDATES;
    n++;
    /* Setup Options are delta-typed key-value pairs behind a 16-bit Length;
     * the body is sized before anything is written. */
    size_t body = 0;
    uint64_t previous = 0;
    for (size_t i = 0; i < n; i++) {
        size_t length = moq_vi64_len(options[i].type - previous);
        if (options[i].type & 1u)
            length += moq_vi64_len(options[i].bytes.len) + options[i].bytes.len;
        else
            length += moq_vi64_len(options[i].integer);
        if (length > UINT16_MAX - body) return MOQ_ERR_BUFFER;
        body += length;
        previous = options[i].type;
    }
    size_t length = moq_vi64_len(MOQ_D22_SETUP) + 2 + body;
    if (length > s->send_cap) return MOQ_ERR_BUFFER;
    if (length > s->send_cap - s->send_len) return MOQ_ERR_WOULD_BLOCK;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len, length);
    moq_result_t rc = moq_d22_encode_setup(&w, options, n);
    if (rc < 0) return rc;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(s->next_stream_ref);
    rc = queue_open_uni_control_in_place(s, ref, moq_buf_writer_offset(&w));
    if (rc < 0) return rc;
    s->next_stream_ref++;
    p->setup_sent = true;
    s->state = MOQ_SESS_SETUP_SENT;
    return MOQ_OK;
}

/* Walk the validated Setup Options for AUTHORIZATION_TOKEN values (§9.1.3):
 * each is Alias Type, [Alias], [Token Type, Token Value]. The codec already
 * proved the structure; this projects it into the shared token form. */
static moq_result_t d22_setup_tokens(moq_session_t *s,
                                     const moq_d22_setup_options_t *opts,
                                     moq_decoded_auth_token_t *tokens,
                                     size_t *out_count)
{
    if (opts->auth_token_count > MOQ_DECODED_MAX_TOKENS)
        return close_with_error(s, 0x1, "too many SETUP authorization tokens");
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, opts->encoded.data, opts->encoded.len);
    uint64_t previous = 0;
    size_t n = 0;
    while (moq_buf_reader_remaining(&r) > 0) {
        moq_d22_kvp_t kv;
        if (moq_d22_decode_kvp(&r, &previous, &kv) != MOQ_OK)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (kv.type != D22_OPT_AUTHORIZATION_TOKEN) continue;
        moq_buf_reader_t tr;
        moq_buf_reader_init(&tr, kv.bytes.data, kv.bytes.len);
        uint64_t op = 0;
        moq_decoded_auth_token_t t;
        memset(&t, 0, sizeof(t));
        if (moq_buf_read_vi64(&tr, &op) != MOQ_OK || op > 3)
            return close_with_error(s, 0x6, "malformed auth token structure");
        t.op = (moq_auth_op_t)op;
        if (op != MOQ_AUTH_OP_USE_VALUE &&
            moq_buf_read_vi64(&tr, &t.alias) != MOQ_OK)
            return close_with_error(s, 0x6, "malformed auth token structure");
        if (op == MOQ_AUTH_OP_REGISTER || op == MOQ_AUTH_OP_USE_VALUE) {
            if (moq_buf_read_vi64(&tr, &t.token_type) != MOQ_OK)
                return close_with_error(s, 0x6, "malformed auth token structure");
            t.token_value = moq_buf_reader_ptr(&tr);
            t.token_value_len = moq_buf_reader_remaining(&tr);
        }
        /* Nothing can be registered before the peer's first message, so an
         * alias reference cannot appear in SETUP. */
        if (op == MOQ_AUTH_OP_DELETE || op == MOQ_AUTH_OP_USE_ALIAS)
            return close_with_error(s, 0x3, "DELETE/USE_ALIAS in SETUP");
        tokens[n++] = t;
    }
    *out_count = n;
    return MOQ_OK;
}

static moq_result_t d22_handle_setup(moq_session_t *s,
                                     const moq_control_envelope_t *env)
{
    moq_d22_profile_state_t *p = (moq_d22_profile_state_t *)s->profile_state;
    if (p->setup_received)
        return close_with_error(s, 0x3, "duplicate SETUP");
    /* Reserve the completion event up front; the handler is retryable until
     * the shared completion commits. */
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_d22_setup_options_t opts;
    moq_result_t rc = moq_d22_decode_setup_options(env->payload, env->payload_len,
                                                   &opts);
    if (rc == MOQ_D22_ERR_KVP_FORMAT)
        return close_with_error(s, 0x6, "malformed auth token structure");
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed SETUP options");
    /* PATH and AUTHORITY travel client to server, natively over QUIC only
     * (§9.1.1, §9.1.2): from a server or over WebTransport each is INVALID_*,
     * a value outside RFC 3986 syntax is MALFORMED_*, and a raw server's
     * policy decides whether it serves the named target. The policy runs
     * before any token takes effect and may run again if this SETUP has to
     * be retried before it commits. */
    const char *reason = NULL;
    const moq_binding_peer_target_t target = {
        .has_path = opts.has_path, .has_authority = opts.has_authority,
        .path = opts.path, .authority = opts.authority,
    };
    uint64_t target_err = moq_binding_judge_peer_target(s, &target, &reason);
    if (target_err) return close_with_error(s, target_err, reason);
    moq_setup_workspace_t workspace;
    rc = session_setup_workspace_acquire(s, &workspace);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    rc = d22_setup_tokens(s, &opts, workspace.decoded->setup_tokens,
                          &workspace.count);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) {
        session_setup_workspace_release(s, &workspace);
        return rc;
    }
    moq_setup_params_t peer;
    memset(&peer, 0, sizeof(peer));
    peer.has_path = opts.has_path;
    peer.has_authority = opts.has_authority;
    peer.has_max_auth_token_cache_size = opts.has_max_auth_token_cache_size;
    peer.max_auth_token_cache_size = opts.max_auth_token_cache_size;
    peer.has_implementation = opts.has_implementation;
    peer.implementation = opts.implementation;
    p->peer_max_filter_ranges = opts.has_max_filter_ranges ? opts.max_filter_ranges : 0;
    p->peer_max_request_updates = opts.has_max_request_updates ? opts.max_request_updates : 0;
    return session_setup_complete_peer(s, &workspace, &peer,
                                       &p->setup_received, p->setup_sent);
}

/* GOAWAY on the control stream (§9.2): New Session URI and Timeout, no
 * Request ID. The draft-neutral core owns the DRAINING transition. */
static moq_result_t d22_handle_goaway(moq_session_t *s,
                                      const moq_control_envelope_t *env)
{
    if (!session_is_active(s))
        return close_with_error(s, 0x3, "GOAWAY before ESTABLISHED");
    if (s->goaway_received)
        return close_with_error(s, 0x3, "duplicate GOAWAY");
    moq_d22_goaway_t ga;
    moq_result_t rc = moq_d22_decode_goaway(env->payload, env->payload_len, &ga);
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed GOAWAY");
    if (s->perspective == MOQ_PERSPECTIVE_SERVER && ga.uri.len > 0)
        return close_with_error(s, 0x3,
            "server received GOAWAY with non-zero New Session URI");
    return session_core_on_goaway(s, ga.uri.data, ga.uri.len, true, ga.timeout_ms);
}

static moq_result_t d22_process_control_data(moq_session_t *s,
                                             const uint8_t *data, size_t len,
                                             size_t *out_consumed)
{
    size_t total = 0;
    *out_consumed = 0;
    if (moq_binding_required_unbound(s)) return MOQ_ERR_WRONG_STATE;
    while (total < len && s->state != MOQ_SESS_CLOSED) {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, data + total, len - total);
        moq_control_envelope_t env;
        moq_result_t rc = moq_d22_decode_envelope(&r, &env);
        if (rc == MOQ_ERR_BUFFER) break;
        if (rc < 0) {
            *out_consumed = total;
            return close_with_error(s, 0x3, "malformed control envelope");
        }
        switch (env.msg_type) {
        case MOQ_D22_SETUP:
            rc = d22_handle_setup(s, &env);
            break;
        case MOQ_D22_GOAWAY:
            rc = d22_handle_goaway(s, &env);
            break;
        default:
            /* Every other control message rides a request stream; one on the
             * control channel is a protocol violation. */
            *out_consumed = total;
            return close_with_error(s, 0x3, "unexpected control message");
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

static moq_result_t d22_encode_goaway(moq_session_t *s,
                                      struct moq_buf_writer *w,
                                      const struct moq_goaway_encode_args *args)
{
    moq_d22_goaway_t ga;
    ga.uri.data = args->uri;
    ga.uri.len = args->uri_len;
    ga.timeout_ms = s->goaway_timeout_us / 1000;
    return moq_d22_encode_goaway(w, &ga);
}

/* -- Draft-22 bodies <-> the core's records -------------------------- *
 * Every message is decoded by the shared codec into a tagged message; the
 * adapters below project it onto the records the core handlers consume and
 * build messages from the core's encode arguments. Parameter defaults follow
 * the draft: FORWARD 1, SUBSCRIBER_PRIORITY 128, GROUP_ORDER publisher
 * default, FETCH GROUP_ORDER Ascending. */

#define D22_PARAM_OBJECT_DELIVERY_TIMEOUT   UINT64_C(0x02)
#define D22_PARAM_AUTHORIZATION_TOKEN       UINT64_C(0x03)
#define D22_PARAM_RENDEZVOUS_TIMEOUT        UINT64_C(0x04)
#define D22_PARAM_SUBGROUP_DELIVERY_TIMEOUT UINT64_C(0x06)
#define D22_PARAM_EXPIRES                   UINT64_C(0x08)
#define D22_PARAM_LARGEST_OBJECT            UINT64_C(0x09)
#define D22_PARAM_FILL_TIMEOUT              UINT64_C(0x0a)
#define D22_PARAM_FORWARD                   UINT64_C(0x10)
#define D22_PARAM_SUBSCRIBER_PRIORITY       UINT64_C(0x20)
#define D22_PARAM_LOCATION_FILTER           UINT64_C(0x21)
#define D22_PARAM_GROUP_ORDER               UINT64_C(0x22)
#define D22_PARAM_FILL_PARAMETERS           UINT64_C(0x23)
#define D22_PARAM_NEW_GROUP_REQUEST         UINT64_C(0x32)
#define D22_PARAM_TRACK_NAMESPACE_PREFIX    UINT64_C(0x34)
#define D22_PARAM_INCLUDE_PROPERTIES        UINT64_C(0x35)
#define D22_TOKEN_USE_VALUE                 UINT64_C(3)

/* Every scalar a message's parameters can carry, with presence. */
typedef struct d22_scalars {
    bool has_forward; bool forward;
    bool has_priority; uint8_t priority;
    bool has_group_order; uint8_t group_order;
    bool has_filter; moq_d22_location_filter_t filter;
    bool has_largest; uint64_t largest_group, largest_object;
    bool has_expires; uint64_t expires_ms;
    bool has_object_dt; uint64_t object_dt_ms;
    bool has_subgroup_dt; uint64_t subgroup_dt_ms;
    bool has_new_group; uint64_t new_group;
    bool has_include_properties; bool include_properties;
    bool has_fill; moq_bytes_t fill;
    bool has_fill_timeout; uint64_t fill_timeout_ms;
    bool has_rendezvous_timeout; uint64_t rendezvous_timeout_ms;
    bool has_prefix; moq_d22_namespace_t prefix;
} d22_scalars_t;

/* Walk a validated parameter sequence. Tokens go to tk when given. */
static moq_result_t d22_scan(const moq_d22_params_t *ps, d22_scalars_t *out,
                             moq_request_tokens_t *tk)
{
    memset(out, 0, sizeof(*out));
    if (tk) tk->count = 0;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, ps->encoded.data, ps->encoded.len);
    uint64_t prev = 0;
    while (moq_buf_reader_remaining(&r) > 0) {
        moq_d22_param_t pv;
        memset(&pv, 0, sizeof(pv));
        moq_result_t rc = moq_d22_param_next_scanned(&r, &prev, &pv);
        if (rc != MOQ_OK) return MOQ_ERR_PROTO;
        switch (pv.type) {
        case D22_PARAM_FORWARD:
            out->has_forward = true; out->forward = pv.value.integer != 0; break;
        case D22_PARAM_SUBSCRIBER_PRIORITY:
            out->has_priority = true; out->priority = (uint8_t)pv.value.integer; break;
        case D22_PARAM_GROUP_ORDER:
            out->has_group_order = true; out->group_order = (uint8_t)pv.value.integer; break;
        case D22_PARAM_LOCATION_FILTER:
            out->has_filter = true; out->filter = pv.value.filter; break;
        case D22_PARAM_LARGEST_OBJECT:
            out->has_largest = true;
            out->largest_group = pv.value.location.group;
            out->largest_object = pv.value.location.object; break;
        case D22_PARAM_EXPIRES:
            out->has_expires = true; out->expires_ms = pv.value.integer; break;
        case D22_PARAM_OBJECT_DELIVERY_TIMEOUT:
            out->has_object_dt = true; out->object_dt_ms = pv.value.integer; break;
        case D22_PARAM_SUBGROUP_DELIVERY_TIMEOUT:
            out->has_subgroup_dt = true; out->subgroup_dt_ms = pv.value.integer; break;
        case D22_PARAM_NEW_GROUP_REQUEST:
            out->has_new_group = true; out->new_group = pv.value.integer; break;
        case D22_PARAM_INCLUDE_PROPERTIES:
            out->has_include_properties = true;
            out->include_properties = pv.value.integer != 0; break;
        case D22_PARAM_FILL_PARAMETERS:
            out->has_fill = true; out->fill = pv.value.fill; break;
        case D22_PARAM_FILL_TIMEOUT:
            out->has_fill_timeout = true; out->fill_timeout_ms = pv.value.integer; break;
        case D22_PARAM_RENDEZVOUS_TIMEOUT:
            out->has_rendezvous_timeout = true;
            out->rendezvous_timeout_ms = pv.value.integer; break;
        case D22_PARAM_TRACK_NAMESPACE_PREFIX:
            out->has_prefix = true; out->prefix = pv.value.ns; break;
        case D22_PARAM_AUTHORIZATION_TOKEN:
            if (tk) {
                if (tk->count >= MOQ_DECODED_MAX_TOKENS) return MOQ_ERR_PROTO;
                moq_decoded_auth_token_t *t = &tk->in[tk->count++];
                switch (pv.value.token.alias_type) {
                case 0: t->op = MOQ_AUTH_OP_DELETE; break;
                case 1: t->op = MOQ_AUTH_OP_REGISTER; break;
                case 2: t->op = MOQ_AUTH_OP_USE_ALIAS; break;
                default: t->op = MOQ_AUTH_OP_USE_VALUE; break;
                }
                t->alias = pv.value.token.alias;
                t->token_type = pv.value.token.type;
                t->token_value = pv.value.token.value.data;
                t->token_value_len = pv.value.token.value.len;
            }
            break;
        default:
            break;   /* range filters are counted by the codec against our limit */
        }
    }
    return MOQ_OK;
}

/* Project a decoded namespace onto borrowed parts. */
static moq_result_t d22_ns_parts(const moq_d22_namespace_t *ns, moq_bytes_t *parts,
                                 size_t cap, moq_namespace_t *out)
{
    if (ns->count > cap) return MOQ_ERR_PROTO;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, ns->fields.data, ns->fields.len);
    for (size_t i = 0; i < ns->count; i++) {
        uint64_t len;
        if (moq_buf_read_vi64(&r, &len) != MOQ_OK) return MOQ_ERR_PROTO;
        if (moq_buf_read_raw(&r, (size_t)len, &parts[i]) != MOQ_OK) return MOQ_ERR_PROTO;
    }
    out->parts = parts;
    out->count = ns->count;
    return MOQ_OK;
}

/* A location filter as the core's filter record: the public forms where
 * they exist, the internal forms for a relative start and an object end.
 * Fields a form does not carry are 0. */
typedef struct d22_filter_record {
    bool     present;
    uint32_t type;
    uint64_t start_group, start_object, end_group, end_object;
} d22_filter_record_t;

static d22_filter_record_t d22_filter_record(const moq_d22_location_filter_t *f)
{
    d22_filter_record_t r = { .present = true };
    switch (f->type) {
    case MOQ_D22_FILTER_NONE:
        r.present = false; r.type = MOQ_SUBSCRIBE_FILTER_NONE; break;
    case MOQ_D22_FILTER_NEXT_OBJECT:
        r.type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT; break;
    case MOQ_D22_FILTER_RELATIVE:
        r.type = MOQ_SUBSCRIBE_FILTER_RELATIVE_GROUP_INTERNAL;
        r.start_group = f->start_group;
        break;
    case MOQ_D22_FILTER_ABSOLUTE:
        r.type = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        r.start_group = f->start_group; r.start_object = f->start_object; break;
    case MOQ_D22_FILTER_GROUP_END:
        r.type = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        r.start_group = f->start_group; r.start_object = f->start_object;
        r.end_group = f->start_group + f->end_group_delta; break;
    case MOQ_D22_FILTER_RANGE:
    default:
        r.type = MOQ_SUBSCRIBE_FILTER_OBJECT_END_INTERNAL;
        r.start_group = f->start_group; r.start_object = f->start_object;
        r.end_group = f->start_group + f->end_group_delta;
        r.end_object = f->end_object; break;
    }
    return r;
}

/* The FILL_PARAMETERS value (Section 9.20.15): a Number of Parameters and
 * that many Parameters of their own scope, with the delta chain restarting.
 * Only the settings that differ from the subscription travel; the core fills
 * in what is omitted. Both callers supply a span fully validated by
 * d22_decode's enclosing message scan, including the count, scope and range
 * limits; the walk still requires the count to end the value exactly.
 * Project refused ranges without re-entering the strict decoder; they carry
 * nothing the core consumes, and the caller retains the enclosing refusal
 * classification. */
static moq_result_t d22_scan_fill(moq_bytes_t body, moq_fill_input_t *out)
{
    memset(out, 0, sizeof(*out));
    out->requested = true;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, body.data, body.len);
    uint64_t count;
    if (moq_buf_read_vi64(&r, &count) != MOQ_OK) return MOQ_ERR_PROTO;
    uint64_t prev = 0;
    for (uint64_t i = 0; i < count; i++) {
        moq_d22_param_t pv;
        memset(&pv, 0, sizeof(pv));
        if (moq_d22_param_next_scanned(&r, &prev, &pv) != MOQ_OK) return MOQ_ERR_PROTO;
        switch (pv.type) {
        case D22_PARAM_FILL_TIMEOUT:
            out->has_fill_timeout = true; out->fill_timeout_ms = pv.value.integer; break;
        case D22_PARAM_SUBSCRIBER_PRIORITY:
            out->has_subscriber_priority = true;
            out->subscriber_priority = (uint8_t)pv.value.integer; break;
        case D22_PARAM_GROUP_ORDER:
            if (pv.value.integer != MOQ_GROUP_ORDER_ASCENDING &&
                pv.value.integer != MOQ_GROUP_ORDER_DESCENDING)
                return MOQ_ERR_PROTO;
            out->has_group_order = true;
            out->group_order = (uint32_t)pv.value.integer; break;
        case D22_PARAM_LOCATION_FILTER: {
            const d22_filter_record_t fr = d22_filter_record(&pv.value.filter);
            out->filter_type = fr.type;
            out->start_group = fr.start_group; out->start_object = fr.start_object;
            out->end_group = fr.end_group; out->end_object = fr.end_object;
            /* An explicit empty filter selects the whole track. */
            out->has_location = true;
            break;
        }
        default:
            break;
        }
    }
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

static moq_result_t d22_rq_rc(moq_result_t rc)
{
    if (rc == MOQ_D22_ERR_KVP_FORMAT) return MOQ_REQUEST_DECODE_BAD_PARAMS;
    if (rc == MOQ_D22_ERR_INVALID_FILTER) return MOQ_REQUEST_DECODE_INVALID_FILTER;
    return rc;
}

static moq_result_t d22_rq_envelope(const uint8_t *buf, size_t len,
                                    moq_request_envelope_t *env, size_t *consumed)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, buf, len);
    moq_control_envelope_t e;
    moq_result_t rc = moq_d22_decode_envelope(&r, &e);
    if (rc < 0) return rc;
    env->msg_type = e.msg_type;
    env->payload = e.payload;
    env->payload_len = e.payload_len;
    *consumed = moq_buf_reader_offset(&r);
    return MOQ_OK;
}

static moq_result_t d22_decode(uint64_t type, const uint8_t *p, size_t n,
                               uint64_t request_type, bool from_subscriber,
                               moq_d22_message_t *m)
{
    moq_d22_message_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.request_type = request_type;
    ctx.updated_request_type = request_type == MOQ_D22_REQUEST_UPDATE ? MOQ_D22_SUBSCRIBE : 0;
    ctx.update_from_subscriber = from_subscriber;
    ctx.max_filter_ranges = D22_LOCAL_MAX_FILTER_RANGES;
    memset(m, 0, sizeof(*m));
    return d22_rq_rc(moq_d22_scan_message(type, p, n, &ctx, m));
}

static moq_result_t d22_rq_subscribe(const uint8_t *p, size_t n,
                                     moq_decoded_subscribe_t *d,
                                     uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_SUBSCRIBE, p, n, 0, false, &m);
    const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
    if (rc < 0 && !invalid_filter) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, tk);
    if (rc < 0) return rc;
    rc = d22_ns_parts(&m.ns, d->track_namespace_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS,
                      &d->track_namespace);
    if (rc < 0) return rc;
    d->track_name = m.track_name;
    d->request_id = m.request_id;
    d->subscriber_priority = v.has_priority ? v.priority : 128;
    d->group_order = v.has_group_order ? v.group_order : MOQ_GROUP_ORDER_DEFAULT;
    d->has_forward = true;
    d->forward = v.has_forward ? v.forward : true;
    /* §9.20.21: 0 asks for empty Track Properties in the answer; 1 is the
     * default. */
    d->omit_track_properties = v.has_include_properties && !v.include_properties;
    if (v.has_filter) {
        const d22_filter_record_t fr = d22_filter_record(&v.filter);
        d->has_filter = fr.present; d->filter_type = fr.type;
        d->start_group = fr.start_group; d->start_object = fr.start_object;
        d->end_group = fr.end_group; d->end_object = fr.end_object;
    } else {
        d->filter_type = MOQ_SUBSCRIBE_FILTER_NONE;
    }
    /* Initial admission distinguishes omitted and explicitly selected NONE. */
    d->has_filter = v.has_filter;
    d->has_rendezvous_timeout = v.has_rendezvous_timeout;
    d->rendezvous_timeout_ms = v.rendezvous_timeout_ms;
    d->dt_has_object = v.has_object_dt;   d->dt_object_ms = v.object_dt_ms;
    d->dt_has_subgroup = v.has_subgroup_dt; d->dt_subgroup_ms = v.subgroup_dt_ms;
    if (v.has_object_dt || v.has_subgroup_dt) {
        d->has_delivery_timeout = true;
        d->delivery_timeout_us = ms_to_us_sat(dt_negotiate_ms(
            v.has_object_dt, v.object_dt_ms, v.has_subgroup_dt, v.subgroup_dt_ms));
    }
    d->has_new_group_request = v.has_new_group;
    d->new_group_request = v.new_group;
    if (v.has_fill) {
        rc = d22_scan_fill(v.fill, &d->fill);
        if (rc < 0) return d22_rq_rc(rc);
    }
    *request_id = m.request_id;
    return invalid_filter ? MOQ_REQUEST_DECODE_INVALID_FILTER : MOQ_OK;
}

static moq_result_t d22_rq_publish(const uint8_t *p, size_t n,
                                   moq_decoded_publish_t *d,
                                   uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_PUBLISH, p, n, 0, false, &m);
    const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
    if (rc < 0 && !invalid_filter) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, tk);
    if (rc < 0) return rc;
    rc = d22_ns_parts(&m.ns, d->track_namespace_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS,
                      &d->track_namespace);
    if (rc < 0) return rc;
    d->track_name = m.track_name;
    d->request_id = m.request_id;
    d->track_alias = m.track_alias;
    d->has_forward = true;
    d->forward = v.has_forward ? v.forward : true;
    d->track_properties = m.properties.data;
    d->track_properties_len = m.properties.len;
    d->track_properties_unsupported = m.mandatory_track_property;
    d->dynamic_groups = moq_d18_op_track_properties_dynamic_groups(
        m.properties.data, m.properties.len);
    d->has_largest = v.has_largest;
    d->largest_group = v.largest_group;
    d->largest_object = v.largest_object;
    d->has_expires = v.has_expires;
    d->expires_ms = v.expires_ms;
    d->has_subscriber_priority = v.has_priority;
    d->subscriber_priority = v.priority;
    d->has_group_order = v.has_group_order;
    d->group_order = v.group_order;
    if (v.has_filter) {
        const d22_filter_record_t fr = d22_filter_record(&v.filter);
        d->has_filter = fr.present; d->filter_type = fr.type;
        d->filter_start_group = fr.start_group; d->filter_start_object = fr.start_object;
        d->filter_end_group = fr.end_group; d->filter_end_object = fr.end_object;
    }
    d->initial.struct_size = sizeof(d->initial);
    d->initial.subscriber_priority = v.has_priority ? v.priority : 128;
    d->initial.group_order = v.has_group_order ? v.group_order : MOQ_GROUP_ORDER_DEFAULT;
    if (v.has_priority) d->initial.flags |= MOQ_PUBLISH_INITIAL_HAS_SUBSCRIBER_PRIORITY;
    if (v.has_group_order) d->initial.flags |= MOQ_PUBLISH_INITIAL_HAS_GROUP_ORDER;
    if (v.has_filter) {
        /* Presence survives explicit NONE, unlike the legacy raw-filter flag. */
        d->initial.flags |= MOQ_PUBLISH_INITIAL_HAS_LOCATION;
        moq_location_filter_from_core(&(moq_core_filter_t){
            .type = d->filter_type,
            .start_group = d->filter_start_group, .start_object = d->filter_start_object,
            .end_group = d->filter_end_group, .end_object = d->filter_end_object },
            &d->initial.location, sizeof(d->initial.location));
    }
    if (v.has_object_dt) {
        d->initial.flags |= MOQ_PUBLISH_INITIAL_HAS_OBJECT_TIMEOUT;
        d->initial.object_delivery_timeout_ms = v.object_dt_ms;
    }
    if (v.has_subgroup_dt) {
        d->initial.flags |= MOQ_PUBLISH_INITIAL_HAS_SUBGROUP_TIMEOUT;
        d->initial.subgroup_delivery_timeout_ms = v.subgroup_dt_ms;
    }
    *request_id = m.request_id;
    return invalid_filter ? MOQ_REQUEST_DECODE_INVALID_FILTER : MOQ_OK;
}

/* FETCH carries its range in LOCATION_FILTER; the core's fetch record keeps
 * the exclusive end object with zero meaning the whole end group. */
static moq_result_t d22_rq_fetch(const uint8_t *p, size_t n,
                                 moq_decoded_fetch_t *fd,
                                 uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(fd, 0, sizeof(*fd));
    fd->joining_sub_slot = -1;
    fd->joining_pub_slot = -1;
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_FETCH, p, n, 0, false, &m);
    const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
    if (rc < 0 && !invalid_filter) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, tk);
    if (rc < 0) return rc;
    rc = d22_ns_parts(&m.ns, fd->track_namespace_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS,
                      &fd->track_namespace);
    if (rc < 0) return rc;
    fd->request_id = m.request_id;
    fd->fetch_type = 1;   /* the only form: a standalone range */
    fd->track_name = m.track_name;
    /* Without a filter, or without an end, the range ends at the Largest
     * Object; a relative or Next Object start is also resolved against it.
     * The core does both from its track history. */
    fd->start_group = 0; fd->start_object = 0;
    fd->end_group = 0; fd->end_object = 0;
    fd->end_at_largest = true;
    if (v.has_filter) {
        const moq_d22_location_filter_t *f = &v.filter;
        switch (f->type) {
        case MOQ_D22_FILTER_NONE:
            break;
        case MOQ_D22_FILTER_RELATIVE:
            fd->start_relative = true; fd->relative_offset = f->start_group; break;
        case MOQ_D22_FILTER_NEXT_OBJECT:
            fd->start_next_object = true; break;
        case MOQ_D22_FILTER_ABSOLUTE:
            fd->start_group = f->start_group; fd->start_object = f->start_object; break;
        case MOQ_D22_FILTER_GROUP_END:
            fd->start_group = f->start_group; fd->start_object = f->start_object;
            fd->end_group = f->start_group + f->end_group_delta; fd->end_object = 0;
            fd->end_at_largest = false; break;
        case MOQ_D22_FILTER_RANGE:
        default:
            fd->start_group = f->start_group; fd->start_object = f->start_object;
            fd->end_group = f->start_group + f->end_group_delta;
            /* inclusive on the wire; an end at the ceiling is the whole group */
            fd->end_object = f->end_object == UINT64_MAX ? 0 : f->end_object + 1;
            fd->end_at_largest = false; break;
        }
    }
    /* The filter as sent, and the request's FILL_TIMEOUT, for the event's
     * options (the relay-facing details of the request). */
    fd->has_options = true;
    if (v.has_filter && v.filter.type != MOQ_D22_FILTER_NONE) {
        const d22_filter_record_t fr = d22_filter_record(&v.filter);
        fd->has_location = fr.present; fd->location_type = fr.type;
        fd->location_start_group = fr.start_group; fd->location_start_object = fr.start_object;
        fd->location_end_group = fr.end_group; fd->location_end_object = fr.end_object;
    }
    fd->has_fill_timeout = v.has_fill_timeout;
    fd->fill_timeout_ms = v.fill_timeout_ms;
    fd->subscriber_priority = v.has_priority ? v.priority : 128;
    fd->group_order = v.has_group_order ? v.group_order : MOQ_GROUP_ORDER_ASCENDING;
    fd->omit_track_properties = v.has_include_properties && !v.include_properties;
    *request_id = m.request_id;
    return invalid_filter ? MOQ_REQUEST_DECODE_INVALID_FILTER : MOQ_OK;
}

static moq_result_t d22_rq_publish_namespace(const uint8_t *p, size_t n,
                                             moq_decoded_publish_namespace_t *d,
                                             uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_PUBLISH_NAMESPACE, p, n, 0, false, &m);
    if (rc < 0) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, tk);
    if (rc < 0) return rc;
    rc = d22_ns_parts(&m.ns, d->track_namespace_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS,
                      &d->track_namespace);
    if (rc < 0) return rc;
    d->request_id = m.request_id;
    *request_id = m.request_id;
    return MOQ_OK;
}

static moq_result_t d22_rq_track_status(const uint8_t *p, size_t n,
                                        moq_decoded_track_status_request_t *d,
                                        uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_TRACK_STATUS, p, n, 0, false, &m);
    if (rc < 0) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, tk);
    if (rc < 0) return rc;
    rc = d22_ns_parts(&m.ns, d->track_namespace_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS,
                      &d->track_namespace);
    if (rc < 0) return rc;
    d->track_name = m.track_name;
    /* §9.20.21: 0 asks for empty Track Properties in the answer. */
    d->omit_track_properties = v.has_include_properties && !v.include_properties;
    *request_id = m.request_id;
    return MOQ_OK;
}

static moq_result_t d22_rq_subscribe_tracks(const uint8_t *p, size_t n,
                                            moq_decoded_subscribe_tracks_request_t *d,
                                            uint64_t *request_id, moq_request_tokens_t *tk)
{
    memset(d, 0, sizeof(*d));
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_SUBSCRIBE_TRACKS, p, n, 0, false, &m);
    const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
    if (rc < 0 && !invalid_filter) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, tk);
    if (rc < 0) return rc;
    rc = d22_ns_parts(&m.ns, d->prefix_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS,
                      &d->track_namespace_prefix);
    if (rc < 0) return rc;
    d->forward = v.has_forward ? v.forward : true;
    d->options.struct_size = sizeof(d->options);
    d->options.flags = (v.has_group_order ? MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER : 0) |
        (v.has_include_properties ? MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES : 0);
    d->options.group_order = v.has_group_order ? v.group_order : MOQ_GROUP_ORDER_DEFAULT;
    d->options.include_properties = v.has_include_properties ? v.include_properties : true;
    *request_id = m.request_id;
    return invalid_filter ? MOQ_REQUEST_DECODE_INVALID_FILTER : MOQ_OK;
}

static uint64_t d22_family_type(moq_request_family_t family)
{
    switch (family) {
    case MOQ_REQUEST_FAMILY_SUBSCRIBE:        return MOQ_D22_SUBSCRIBE;
    case MOQ_REQUEST_FAMILY_FETCH:            return MOQ_D22_FETCH;
    case MOQ_REQUEST_FAMILY_TRACK_STATUS:     return MOQ_D22_TRACK_STATUS;
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT:     return MOQ_D22_PUBLISH_NAMESPACE;
    case MOQ_REQUEST_FAMILY_NS_SUB:           return MOQ_D22_SUBSCRIBE_NAMESPACE;
    case MOQ_REQUEST_FAMILY_PUBLISH:          return MOQ_D22_PUBLISH;
    case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS: return MOQ_D22_SUBSCRIBE_TRACKS;
    default:                                  return 0;
    }
}

/* The codec scopes an update by the request it modifies and, for a
 * subscription, by the sender's role; other families have their own scopes. */
static moq_result_t d22_rq_request_update(const uint8_t *p, size_t n,
                                          moq_request_family_t target_family,
                                          bool from_subscriber,
                                          moq_decoded_request_update_t *d,
                                          moq_request_tokens_t *tk)
{
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_REQUEST_UPDATE, p, n,
                                 d22_family_type(target_family), from_subscriber, &m);
    const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
    if (rc < 0 && !invalid_filter) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, tk);
    if (rc < 0) return rc;
    memset(d, 0, sizeof(*d));
    d->request_id = m.request_id;
    d->has_forward = v.has_forward;
    d->forward = v.forward;
    d->has_subscriber_priority = v.has_priority;
    d->subscriber_priority = v.priority;
    if (v.has_filter) {
        const d22_filter_record_t fr = d22_filter_record(&v.filter);
        d->has_filter = fr.present; d->filter_type = fr.type;
        d->start_group = fr.start_group; d->start_object = fr.start_object;
        d->end_group = fr.end_group; d->end_object = fr.end_object;
        /* A selected NONE removes the restriction of either subscription
         * establishment path; omission alone means unchanged. */
        if ((target_family == MOQ_REQUEST_FAMILY_SUBSCRIBE ||
             target_family == MOQ_REQUEST_FAMILY_PUBLISH) && from_subscriber)
            d->has_filter = true;
    }
    d->dt_has_object = v.has_object_dt;   d->dt_object_ms = v.object_dt_ms;
    d->dt_has_subgroup = v.has_subgroup_dt; d->dt_subgroup_ms = v.subgroup_dt_ms;
    if (v.has_object_dt || v.has_subgroup_dt) {
        d->has_delivery_timeout = true;
        d->delivery_timeout_us = ms_to_us_sat(dt_negotiate_ms(
            v.has_object_dt, v.object_dt_ms, v.has_subgroup_dt, v.subgroup_dt_ms));
    }
    d->has_new_group_request = v.has_new_group;
    d->new_group_request = v.new_group;
    if (v.has_fill) {
        rc = d22_scan_fill(v.fill, &d->fill);
        if (rc < 0) return d22_rq_rc(rc);
    }
    return invalid_filter ? MOQ_REQUEST_DECODE_INVALID_FILTER : MOQ_OK;
}

/* The family selects the legal parameter set (§9.5): a parameter another
 * family permits is a protocol violation here, a Range Filter is a request
 * refusal at the advertised zero limit. */
static moq_result_t d22_rq_namespace_update(const uint8_t *p, size_t n,
                                            moq_request_family_t family,
                                            moq_decoded_namespace_update_t *out,
                                            moq_request_tokens_t *tk)
{
    memset(out, 0, sizeof(*out));
    if (family != MOQ_REQUEST_FAMILY_ANNOUNCEMENT &&
        family != MOQ_REQUEST_FAMILY_NS_SUB &&
        family != MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS)
        return MOQ_ERR_INVAL;
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_REQUEST_UPDATE, p, n,
                                 d22_family_type(family), true, &m);
    const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
    if (rc < 0 && !invalid_filter) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, tk);
    if (rc < 0) return rc;
    out->request_id = m.request_id;
    if (v.has_prefix) {
        rc = d22_ns_parts(&v.prefix, out->prefix_parts,
                          MOQ_DECODED_MAX_NAMESPACE_PARTS, &out->prefix);
        if (rc < 0) return rc;
        out->has_prefix = true;
    }
    out->has_forward = v.has_forward;
    out->forward = v.forward;
    return invalid_filter ? MOQ_REQUEST_DECODE_INVALID_FILTER : MOQ_OK;
}

static moq_result_t d22_rq_goaway(const uint8_t *p, size_t n,
                                  moq_request_goaway_msg_t *out)
{
    moq_d22_goaway_t ga;
    moq_result_t rc = moq_d22_decode_goaway(p, n, &ga);
    if (rc < 0) return rc;
    out->uri = ga.uri;
    out->timeout_ms = ga.timeout_ms;
    return MOQ_OK;
}

static moq_result_t d22_rq_publish_done(const uint8_t *p, size_t n,
                                        moq_request_publish_done_msg_t *out)
{
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_PUBLISH_DONE, p, n, 0, false, &m);
    if (rc < 0) return rc;
    out->status_code = m.code;
    out->stream_count = m.stream_count;
    out->reason = m.reason;
    return MOQ_OK;
}

static moq_result_t d22_rq_state_notify(const uint8_t *p, size_t n,
                                        moq_decoded_state_notify_t *out)
{
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_PUBLISH_STATE_NOTIFY, p, n, 0, false, &m);
    if (rc < 0) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, NULL);
    if (rc < 0) return rc;
    memset(out, 0, sizeof(*out));
    out->has_forward = v.has_forward;
    out->forward = v.forward;
    out->has_largest = v.has_largest;
    out->largest_group = v.largest_group;
    out->largest_object = v.largest_object;
    if (v.has_filter) {
        const d22_filter_record_t fr = d22_filter_record(&v.filter);
        out->has_filter = fr.present; out->filter_type = fr.type;
        out->start_group = fr.start_group; out->start_object = fr.start_object;
        out->end_group = fr.end_group; out->end_object = fr.end_object;
    }
    out->has_filter = v.has_filter;
    return MOQ_OK;
}

static moq_result_t d22_rq_request_error(const uint8_t *p, size_t n,
                                         moq_bytes_t *rd_parts, size_t rd_parts_max,
                                         moq_request_error_msg_t *out,
                                         moq_decoded_redirect_t *rd)
{
    moq_d22_message_t m;
    /* The redirect's namespace rule depends on the family; the engine applies
     * it, so decode under a family that permits a track name. */
    moq_result_t rc = d22_decode(MOQ_D22_REQUEST_ERROR, p, n, MOQ_D22_SUBSCRIBE, false, &m);
    if (rc < 0) return rc;
    out->error_code = m.code;
    out->retry_interval = m.retry_interval;
    out->reason = m.reason;
    out->is_redirect = (m.code == MOQ_D22_ERROR_REDIRECT);
    if (out->is_redirect) {
        memset(rd, 0, sizeof(*rd));
        rd->connect_uri = m.redirect_uri.data;
        rd->connect_uri_len = m.redirect_uri.len;
        rc = d22_ns_parts(&m.ns, rd_parts, rd_parts_max, &rd->track_namespace);
        if (rc < 0) return rc;
        rd->track_name = m.track_name.data;
        rd->track_name_len = m.track_name.len;
    }
    return MOQ_OK;
}

static moq_result_t d22_rq_subscribe_ok(const uint8_t *p, size_t n,
                                        moq_decoded_subscribe_ok_t *d)
{
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_SUBSCRIBE_OK, p, n, 0, false, &m);
    if (rc < 0) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, NULL);
    if (rc < 0) return rc;
    memset(d, 0, sizeof(*d));
    d->track_alias = m.track_alias;
    d->has_largest = v.has_largest;
    d->largest_group = v.largest_group;
    d->largest_object = v.largest_object;
    d->has_expires = v.has_expires;
    d->expires_ms = v.expires_ms;
    d->track_properties = m.properties.data;
    d->track_properties_len = m.properties.len;
    d->track_properties_unsupported = m.mandatory_track_property;
    d->dynamic_groups = moq_d18_op_track_properties_dynamic_groups(
        m.properties.data, m.properties.len);
    return MOQ_OK;
}

static moq_result_t d22_rq_fetch_ok(const uint8_t *p, size_t n,
                                    moq_decoded_fetch_ok_t *d)
{
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_FETCH_OK, p, n, 0, false, &m);
    if (rc < 0) return rc;
    memset(d, 0, sizeof(*d));
    d->end_of_track = m.end_of_track != 0;
    /* §9.12: the End Location is inclusive. The core's end object is
     * exclusive with zero meaning the whole end group, which the group's
     * last Location {G, 2^64-1} is. */
    d->end_group = m.end_location.group;
    d->end_object = m.end_location.object == UINT64_MAX ? 0 : m.end_location.object + 1;
    d->track_properties = m.properties.data;
    d->track_properties_len = m.properties.len;
    d->track_properties_unsupported = m.mandatory_track_property;
    return MOQ_OK;
}

static moq_result_t d22_rq_request_ok(const uint8_t *p, size_t n)
{
    moq_d22_message_t m;
    return d22_decode(MOQ_D22_REQUEST_OK, p, n, MOQ_D22_SUBSCRIBE_NAMESPACE, false, &m);
}

/* SUBSCRIBE_TRACKS_OK: Track Properties may be present (§9.3 names no close
 * for this answer); nothing here consumes them. */
static moq_result_t d22_rq_subscribe_tracks_ok(const uint8_t *p, size_t n)
{
    moq_d22_message_t m;
    return d22_decode(MOQ_D22_REQUEST_OK, p, n, MOQ_D22_SUBSCRIBE_TRACKS, false, &m);
}

static moq_result_t d22_rq_track_status_ok(const uint8_t *p, size_t n,
                                           moq_decoded_track_status_ok_t *d)
{
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_REQUEST_OK, p, n, MOQ_D22_TRACK_STATUS, false, &m);
    if (rc < 0) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, NULL);
    if (rc < 0) return rc;
    memset(d, 0, sizeof(*d));
    d->has_largest = v.has_largest;
    d->largest_group = v.largest_group;
    d->largest_object = v.largest_object;
    d->track_properties = m.properties.data;
    d->track_properties_len = m.properties.len;
    return MOQ_OK;
}

static moq_result_t d22_rq_update_ok(const uint8_t *p, size_t n,
                                     moq_request_update_ok_msg_t *out)
{
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_REQUEST_OK, p, n, MOQ_D22_REQUEST_UPDATE, false, &m);
    if (rc < 0) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, NULL);
    if (rc < 0) return rc;
    out->has_largest = v.has_largest;
    out->largest_group = v.largest_group;
    out->largest_object = v.largest_object;
    out->has_expires = v.has_expires;
    out->expires_ms = v.expires_ms;
    return MOQ_OK;
}

static moq_result_t d22_rq_publish_blocked(const uint8_t *p, size_t n,
                                           moq_decoded_publish_blocked_t *d)
{
    memset(d, 0, sizeof(*d));
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_PUBLISH_SKIPPED, p, n, 0, false, &m);
    if (rc < 0) return rc;
    rc = d22_ns_parts(&m.ns, d->suffix_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS,
                      &d->track_namespace_suffix);
    if (rc < 0) return rc;
    d->track_name = m.track_name;
    return MOQ_OK;
}

/* The PUBLISH response is a bare REQUEST_OK: the subscriber's choices follow
 * as a REQUEST_UPDATE, so the record carries the defaults. */
static moq_result_t d22_rq_publish_ok(const uint8_t *p, size_t n,
                                      moq_decoded_publish_ok_t *d)
{
    moq_d22_message_t m;
    moq_result_t rc = d22_decode(MOQ_D22_REQUEST_OK, p, n, MOQ_D22_PUBLISH, false, &m);
    if (rc < 0) return rc;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, NULL);
    if (rc < 0) return rc;
    memset(d, 0, sizeof(*d));
    d->params_absent = true;
    d->forward = true;
    d->subscriber_priority = 128;
    d->group_order = MOQ_GROUP_ORDER_DEFAULT;
    d->has_expires = v.has_expires;
    d->expires_ms = v.expires_ms;
    return MOQ_OK;
}

static const moq_request_codec_t d22_request_codec = {
    .decode_envelope          = d22_rq_envelope,
    .type = {
        .subscribe           = MOQ_D22_SUBSCRIBE,
        .publish             = MOQ_D22_PUBLISH,
        .fetch               = MOQ_D22_FETCH,
        .publish_namespace   = MOQ_D22_PUBLISH_NAMESPACE,
        .track_status        = MOQ_D22_TRACK_STATUS,
        .subscribe_tracks    = MOQ_D22_SUBSCRIBE_TRACKS,
        .subscribe_namespace = MOQ_D22_SUBSCRIBE_NAMESPACE,
        .request_update      = MOQ_D22_REQUEST_UPDATE,
        .request_ok          = MOQ_D22_REQUEST_OK,
        .request_error       = MOQ_D22_REQUEST_ERROR,
        .publish_done        = MOQ_D22_PUBLISH_DONE,
        .publish_blocked     = MOQ_D22_PUBLISH_SKIPPED,
        .goaway              = MOQ_D22_GOAWAY,
        .subscribe_ok        = MOQ_D22_SUBSCRIBE_OK,
        .fetch_ok            = MOQ_D22_FETCH_OK,
        .publish_state_notify = MOQ_D22_PUBLISH_STATE_NOTIFY,
    },
    .decode_subscribe         = d22_rq_subscribe,
    .decode_publish           = d22_rq_publish,
    .decode_fetch             = d22_rq_fetch,
    .decode_publish_namespace = d22_rq_publish_namespace,
    .decode_track_status      = d22_rq_track_status,
    .decode_subscribe_tracks  = d22_rq_subscribe_tracks,
    .decode_request_update    = d22_rq_request_update,
    .decode_namespace_update  = d22_rq_namespace_update,
    .decode_goaway_request    = d22_rq_goaway,
    .decode_publish_done      = d22_rq_publish_done,
    .decode_publish_state_notify = d22_rq_state_notify,
    .decode_request_error     = d22_rq_request_error,
    .decode_subscribe_ok      = d22_rq_subscribe_ok,
    .decode_fetch_ok          = d22_rq_fetch_ok,
    .decode_request_ok        = d22_rq_request_ok,
    .decode_subscribe_tracks_ok = d22_rq_subscribe_tracks_ok,
    .decode_track_status_ok   = d22_rq_track_status_ok,
    .decode_update_ok         = d22_rq_update_ok,
    .decode_publish_blocked   = d22_rq_publish_blocked,
    .decode_publish_ok        = d22_rq_publish_ok,
};

/* -- Outbound encoders ----------------------------------------------- *
 * Each builds a tagged message plus a type-sorted parameter array and hands
 * both to the codec, which validates and self-checks the bytes. */

typedef struct d22_params_builder {
    moq_d22_param_t p[MOQ_DECODED_MAX_TOKENS + 12];
    size_t n;
} d22_pb_t;

static void d22_pb_add(d22_pb_t *b, const moq_d22_param_t *v)
{
    /* Keep the array sorted by type; repeats (tokens) keep arrival order. */
    size_t i = b->n;
    while (i > 0 && b->p[i - 1].type > v->type) { b->p[i] = b->p[i - 1]; i--; }
    b->p[i] = *v;
    b->n++;
}

static void d22_pb_int(d22_pb_t *b, uint64_t type, uint64_t value)
{
    moq_d22_param_t v; memset(&v, 0, sizeof(v));
    v.type = type; v.value.integer = value; d22_pb_add(b, &v);
}

static void d22_pb_location(d22_pb_t *b, uint64_t group, uint64_t object)
{
    moq_d22_param_t v; memset(&v, 0, sizeof(v));
    v.type = D22_PARAM_LARGEST_OBJECT;
    v.value.location.group = group; v.value.location.object = object;
    d22_pb_add(b, &v);
}

static void d22_pb_filter(d22_pb_t *b, const moq_d22_location_filter_t *f)
{
    moq_d22_param_t v; memset(&v, 0, sizeof(v));
    v.type = D22_PARAM_LOCATION_FILTER; v.value.filter = *f; d22_pb_add(b, &v);
}

static moq_result_t d22_pb_tokens(d22_pb_t *b, const moq_auth_token_t *tokens, size_t count)
{
    if (count > MOQ_DECODED_MAX_TOKENS) return MOQ_ERR_INVAL;
    for (size_t i = 0; i < count; i++) {
        moq_d22_param_t v; memset(&v, 0, sizeof(v));
        v.type = D22_PARAM_AUTHORIZATION_TOKEN;
        v.value.token.alias_type = D22_TOKEN_USE_VALUE;
        v.value.token.alias = 0;
        v.value.token.type = tokens[i].token_type;
        v.value.token.value = tokens[i].token_value;
        d22_pb_add(b, &v);
    }
    return MOQ_OK;
}

/* FILL_PARAMETERS: the request's own settings as a nested Number of
 * Parameters and Parameters (Section 9.20.15), encoded into caller scratch
 * the codec borrows while the message is emitted. A fill with no settings
 * of its own is the count 0, never an empty value. */
typedef struct d22_fill_scratch { uint8_t buf[96]; } d22_fill_scratch_t;

static moq_result_t d22_pb_fill(d22_pb_t *b, const moq_fill_input_t *f,
                                d22_fill_scratch_t *sc)
{
    d22_pb_t inner = { .n = 0 };
    if (f->has_fill_timeout) d22_pb_int(&inner, D22_PARAM_FILL_TIMEOUT, f->fill_timeout_ms);
    if (f->has_subscriber_priority)
        d22_pb_int(&inner, D22_PARAM_SUBSCRIBER_PRIORITY, f->subscriber_priority);
    if (f->has_location) {
        moq_d22_location_filter_t lf;
        moq_result_t rc = moq_d22_from_subscription_filter(f->filter_type, f->start_group,
            f->start_object, f->end_group, f->end_object, &lf);
        if (rc < 0) return rc;
        d22_pb_filter(&inner, &lf);
    }
    if (f->has_group_order) d22_pb_int(&inner, D22_PARAM_GROUP_ORDER, f->group_order);
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, sc->buf, sizeof(sc->buf));
    moq_result_t rc = moq_buf_write_vi64(&w, inner.n);
    if (rc == MOQ_OK)
        rc = moq_d22_encode_params(&w, inner.p, inner.n, MOQ_D22_SCOPE_FILL,
                                   D22_LOCAL_MAX_FILTER_RANGES);
    if (rc < 0) return rc == MOQ_ERR_BUFFER ? MOQ_ERR_BUFFER : MOQ_ERR_INVAL;
    moq_d22_param_t v; memset(&v, 0, sizeof(v));
    v.type = D22_PARAM_FILL_PARAMETERS;
    v.value.fill.data = sc->buf;
    v.value.fill.len = moq_buf_writer_offset(&w);
    d22_pb_add(b, &v);
    return MOQ_OK;
}

/* Namespace scratch: the codec takes an already encoded namespace. */
typedef struct d22_ns_scratch { uint8_t buf[4096 + 64]; } d22_ns_scratch_t;

static moq_result_t d22_set_ns(moq_d22_message_t *m, const moq_namespace_t *ns,
                               d22_ns_scratch_t *sc)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, sc->buf, sizeof(sc->buf));
    moq_result_t rc = moq_d22_encode_namespace(&w, ns);
    if (rc < 0) return rc;
    m->ns.encoded.data = sc->buf;
    m->ns.encoded.len = moq_buf_writer_offset(&w);
    m->ns.count = ns->count;
    return MOQ_OK;
}

static moq_result_t d22_emit_params(struct moq_buf_writer *w, const moq_d22_message_t *m,
                                    const moq_d22_param_t *params, size_t count,
                                    uint64_t request_type, bool from_subscriber)
{
    moq_d22_message_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.request_type = request_type;
    ctx.updated_request_type = request_type == MOQ_D22_REQUEST_UPDATE ? MOQ_D22_SUBSCRIBE : 0;
    ctx.update_from_subscriber = from_subscriber;
    ctx.max_filter_ranges = D22_LOCAL_MAX_FILTER_RANGES;
    moq_result_t rc = moq_d22_encode_message(w, m, params, count, &ctx);
    return rc == MOQ_ERR_BUFFER ? MOQ_ERR_BUFFER : (rc < 0 ? MOQ_ERR_INVAL : MOQ_OK);
}

static moq_result_t d22_emit(struct moq_buf_writer *w, const moq_d22_message_t *m,
                             const d22_pb_t *b, uint64_t request_type, bool from_subscriber)
{
    return d22_emit_params(w, m, b ? b->p : NULL, b ? b->n : 0,
                           request_type, from_subscriber);
}

static moq_result_t d22_encode_subscribe(moq_session_t *s, struct moq_buf_writer *w,
                                         const struct moq_subscribe_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = MOQ_D22_SUBSCRIBE;
    m.request_id = args->request_id;
    moq_result_t rc = d22_set_ns(&m, &args->track_namespace, &sc);
    if (rc < 0) return rc;
    m.track_name = args->track_name;
    d22_pb_t b = { .n = 0 };
    if (args->has_rendezvous_timeout)
        d22_pb_int(&b, D22_PARAM_RENDEZVOUS_TIMEOUT, args->rendezvous_timeout_ms);
    if (args->dt_has_object)
        d22_pb_int(&b, D22_PARAM_OBJECT_DELIVERY_TIMEOUT, args->dt_object_ms);
    if (args->dt_has_subgroup)
        d22_pb_int(&b, D22_PARAM_SUBGROUP_DELIVERY_TIMEOUT, args->dt_subgroup_ms);
    if (args->has_forward && !args->forward) d22_pb_int(&b, D22_PARAM_FORWARD, 0);
    if (args->subscriber_priority != 128)
        d22_pb_int(&b, D22_PARAM_SUBSCRIBER_PRIORITY, args->subscriber_priority);
    if (args->group_order != MOQ_GROUP_ORDER_DEFAULT)
        d22_pb_int(&b, D22_PARAM_GROUP_ORDER, args->group_order);
    if (args->has_location || args->filter != MOQ_SUBSCRIBE_FILTER_NONE) {
        moq_d22_location_filter_t f;
        rc = moq_d22_from_subscription_filter(args->filter, args->start_group,
                                              args->start_object, args->end_group,
                                              args->end_object, &f);
        if (rc < 0) return rc;
        d22_pb_filter(&b, &f);
    }
    if (args->has_new_group_request)
        d22_pb_int(&b, D22_PARAM_NEW_GROUP_REQUEST, args->new_group_request);
    if (args->has_include_properties)
        d22_pb_int(&b, D22_PARAM_INCLUDE_PROPERTIES, args->include_properties ? 1 : 0);
    d22_fill_scratch_t fsc;
    if (args->fill) {
        rc = d22_pb_fill(&b, args->fill, &fsc);
        if (rc < 0) return rc;
    }
    rc = d22_pb_tokens(&b, args->auth_tokens, args->auth_token_count);
    if (rc < 0) return rc;
    return d22_emit(w, &m, &b, 0, false);
}

/* INCLUDE_PROPERTIES=0 cannot change either inherited default. Rebase only
 * outer type deltas; immutable lengths and contents remain byte-for-byte. */
static moq_result_t d22_subscribe_ok_retain_defaults(moq_buf_writer_t *w,
    size_t message_start, moq_bytes_t properties, bool retain_order,
    bool retain_priority)
{
    moq_buf_writer_t out = *w;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, properties.data, properties.len);
    uint64_t previous = 0, retained = 0;
    while (moq_buf_reader_remaining(&r)) {
        size_t start = r.pos;
        moq_d22_kvp_t kvp;
        moq_result_t rc = moq_d22_decode_kvp(&r, &previous, &kvp);
        if (rc < 0) return MOQ_ERR_INVAL;
        bool keep = (retain_order && kvp.type == 0x22) ||
            (retain_priority && kvp.type == 0x0e && kvp.integer != 128) ||
            (retain_order && kvp.type == 0x0b &&
                moq_d18_op_track_properties_group_order(kvp.bytes.data,
                    kvp.bytes.len) != MOQ_GROUP_ORDER_DEFAULT);
        if (retain_priority && kvp.type == 0x0b) {
            uint8_t priority;
            rc = moq_d18_scan_default_publisher_priority(kvp.bytes.data,
                                                       kvp.bytes.len, &priority);
            if (rc < 0) return MOQ_ERR_INVAL;
            keep = keep || priority != 128;
        }
        if (!keep) continue;
        moq_buf_reader_t raw;
        moq_buf_reader_init(&raw, properties.data + start, r.pos - start);
        uint64_t delta;
        rc = moq_buf_read_vi64(&raw, &delta);
        if (rc < 0) return MOQ_ERR_INVAL;
        if (delta == kvp.type - retained) {
            rc = moq_buf_write_raw(&out, properties.data + start, r.pos - start);
        } else {
            rc = moq_buf_write_vi64(&out, kvp.type - retained);
            if (rc == MOQ_OK)
                rc = moq_buf_write_raw(&out, moq_buf_reader_ptr(&raw),
                                       moq_buf_reader_remaining(&raw));
        }
        if (rc < 0) return rc;
        retained = kvp.type;
    }
    moq_buf_reader_t envelope;
    moq_buf_reader_init(&envelope, out.data + message_start, w->pos - message_start);
    uint64_t type;
    moq_result_t rc = moq_buf_read_vi64(&envelope, &type);
    if (rc < 0) return MOQ_ERR_INTERNAL;
    size_t length_at = message_start + envelope.pos;
    uint16_t old_length;
    rc = moq_buf_read_uint16(&envelope, &old_length);
    if (rc < 0 || type != MOQ_D22_SUBSCRIBE_OK) return MOQ_ERR_INTERNAL;
    size_t body_len = out.pos - message_start - envelope.pos;
    if (body_len > UINT16_MAX) return MOQ_ERR_INVAL;
    rc = moq_buf_patch_uint16(&out, length_at, (uint16_t)body_len);
    if (rc < 0) return rc;
    *w = out;
    return MOQ_OK;
}

static moq_result_t d22_encode_subscribe_ok(moq_session_t *s, struct moq_buf_writer *w,
                                            const struct moq_subscribe_ok_encode_args *args)
{
    (void)s;
    if (moq_d22_validate_track_properties(args->track_properties,
                                         args->track_properties_len) < 0)
        return MOQ_ERR_INVAL;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_SUBSCRIBE_OK;
    m.track_alias = args->track_alias;
    m.properties.data = args->track_properties;
    m.properties.len = args->track_properties_len;
    /* EXPIRES (0x08) and LARGEST_OBJECT (0x09), in wire type order. */
    moq_d22_param_t params[2] = {0};
    size_t count = 0;
    if (args->has_expires) {
        params[count].type = D22_PARAM_EXPIRES;
        params[count].value.integer = args->expires_ms;
        ++count;
    }
    if (args->has_largest) {
        params[count].type = D22_PARAM_LARGEST_OBJECT;
        params[count].value.location.group = args->largest_group;
        params[count].value.location.object = args->largest_object;
        ++count;
    }
    if (!args->omit_track_properties)
        return d22_emit_params(w, &m, params, count, 0, false);
    uint8_t priority;
    moq_result_t rc = moq_d18_scan_default_publisher_priority(
        args->track_properties, args->track_properties_len, &priority);
    if (rc < 0) return MOQ_ERR_INVAL;
    bool retain_order = args->inherit_track_group_order &&
        moq_d18_op_track_properties_group_order(args->track_properties,
            args->track_properties_len) == MOQ_GROUP_ORDER_DESCENDING;
    bool retain_priority = priority != 128;
    moq_bytes_t original = m.properties;
    m.properties = (moq_bytes_t){NULL, 0};
    moq_buf_writer_t out = *w;
    size_t message_start = out.pos;
    rc = d22_emit_params(&out, &m, params, count, 0, false);
    if (rc < 0) return rc;
    if (retain_order || retain_priority) {
        rc = d22_subscribe_ok_retain_defaults(&out, message_start, original,
                                             retain_order, retain_priority);
        if (rc < 0) return rc;
    }
    *w = out;
    return MOQ_OK;
}

static moq_result_t d22_encode_request_ok(moq_session_t *s, struct moq_buf_writer *w,
                                          uint64_t request_id)
{
    (void)s; (void)request_id;   /* the stream correlates the response */
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_REQUEST_OK;
    return d22_emit(w, &m, NULL, MOQ_D22_SUBSCRIBE_NAMESPACE, false);
}

static moq_result_t d22_encode_request_update_ok(moq_session_t *s, struct moq_buf_writer *w,
                                                 const struct moq_request_update_ok_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_REQUEST_OK;
    d22_pb_t b = { .n = 0 };
    if (args->has_expires) d22_pb_int(&b, D22_PARAM_EXPIRES, args->expires_ms);
    if (args->has_largest) d22_pb_location(&b, args->largest_group, args->largest_object);
    return d22_emit(w, &m, &b, MOQ_D22_REQUEST_UPDATE, false);
}

static moq_result_t d22_encode_request_error(moq_session_t *s, struct moq_buf_writer *w,
                                             const struct moq_request_error_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = MOQ_D22_REQUEST_ERROR;
    m.code = args->error_code;
    /* §9.4.2: the retry time plus one; zero means "do not retry". */
    if (args->can_retry && args->retry_after_ms >= MOQ_VI64_MAX)
        return MOQ_ERR_INVAL;   /* the plus-one form has no wire value */
    m.retry_interval = args->can_retry ? args->retry_after_ms + 1 : 0;
    m.reason.data = args->reason; m.reason.len = args->reason_len;
    if (args->has_redirect) {
        if (m.code != MOQ_D22_ERROR_REDIRECT) return MOQ_ERR_INVAL;
        m.redirect_uri.data = args->connect_uri; m.redirect_uri.len = args->connect_uri_len;
        moq_result_t rc = d22_set_ns(&m, &args->redirect_namespace, &sc);
        if (rc < 0) return rc;
        m.track_name.data = args->redirect_track_name;
        m.track_name.len = args->redirect_track_name_len;
    } else if (m.code == MOQ_D22_ERROR_REDIRECT) {
        return MOQ_ERR_INVAL;   /* REDIRECT without its tail */
    }
    return d22_emit(w, &m, NULL, MOQ_D22_SUBSCRIBE, false);
}

static moq_result_t d22_encode_request_goaway(moq_session_t *s, struct moq_buf_writer *w,
                                              const uint8_t *uri, size_t uri_len,
                                              uint64_t timeout_ms)
{
    (void)s;
    moq_d22_goaway_t ga;
    ga.uri.data = uri; ga.uri.len = uri_len; ga.timeout_ms = timeout_ms;
    return moq_d22_encode_goaway(w, &ga);
}

static moq_result_t d22_encode_request_update(moq_session_t *s, struct moq_buf_writer *w,
                                              const struct moq_request_update_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_REQUEST_UPDATE;
    m.request_id = args->request_id;
    d22_pb_t b = { .n = 0 };
    /* The subscription API refuses generic input before effects; preserve
     * the publication UPDATE caller's existing projection. */
    if (args->has_delivery_timeout) {
        if (args->dt_has_object || args->dt_has_subgroup) return MOQ_ERR_INVAL;
        uint64_t ms = args->delivery_timeout_us / 1000;
        d22_pb_int(&b, D22_PARAM_OBJECT_DELIVERY_TIMEOUT, ms);
        d22_pb_int(&b, D22_PARAM_SUBGROUP_DELIVERY_TIMEOUT, ms);
    }
    if (args->dt_has_object)
        d22_pb_int(&b, D22_PARAM_OBJECT_DELIVERY_TIMEOUT, args->dt_object_ms);
    if (args->dt_has_subgroup)
        d22_pb_int(&b, D22_PARAM_SUBGROUP_DELIVERY_TIMEOUT, args->dt_subgroup_ms);
    if (args->has_forward) d22_pb_int(&b, D22_PARAM_FORWARD, args->forward ? 1 : 0);
    if (args->has_subscriber_priority)
        d22_pb_int(&b, D22_PARAM_SUBSCRIBER_PRIORITY, args->subscriber_priority);
    if (args->has_filter) {
        moq_d22_location_filter_t f;
        moq_result_t rc = moq_d22_from_subscription_filter(args->filter,
            args->filter_start_group, args->filter_start_object, args->filter_end_group,
            args->filter_end_object, &f);
        if (rc < 0) return rc;
        d22_pb_filter(&b, &f);
    }
    if (args->has_new_group_request)
        d22_pb_int(&b, D22_PARAM_NEW_GROUP_REQUEST, args->new_group_request);
    d22_fill_scratch_t fsc;
    if (args->fill) {
        moq_result_t frc = d22_pb_fill(&b, args->fill, &fsc);
        if (frc < 0) return frc;
    }
    moq_result_t rc = d22_pb_tokens(&b, args->auth_tokens, args->auth_token_count);
    if (rc < 0) return rc;
    /* §9.5: the subscriber of a subscription (begun with SUBSCRIBE or
     * PUBLISH; the codec scopes both the same), the sender of a PUBLISH on
     * its own request, or the sender of a FETCH. The codec refuses any
     * parameter outside the scope of the chosen sender. */
    switch (args->sender) {
    case MOQ_UPDATE_SENDER_SUBSCRIBER:
        return d22_emit(w, &m, &b, MOQ_D22_SUBSCRIBE, true);
    case MOQ_UPDATE_SENDER_PUBLISHER:
        return d22_emit(w, &m, &b, MOQ_D22_PUBLISH, false);
    case MOQ_UPDATE_SENDER_FETCHER:
        return d22_emit(w, &m, &b, MOQ_D22_FETCH, true);
    default:
        return MOQ_ERR_INVAL;
    }
}

static moq_result_t d22_encode_publish_namespace(moq_session_t *s, struct moq_buf_writer *w,
                                                 const struct moq_publish_namespace_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = MOQ_D22_PUBLISH_NAMESPACE;
    m.request_id = args->request_id;
    moq_result_t rc = d22_set_ns(&m, &args->track_namespace, &sc);
    if (rc < 0) return rc;
    d22_pb_t b = { .n = 0 };
    rc = d22_pb_tokens(&b, args->auth_tokens, args->auth_token_count);
    if (rc < 0) return rc;
    return d22_emit(w, &m, &b, 0, false);
}

static moq_result_t d22_encode_publish(moq_session_t *s, struct moq_buf_writer *w,
                                       const struct moq_publish_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = MOQ_D22_PUBLISH;
    m.request_id = args->request_id;
    moq_result_t rc = d22_set_ns(&m, &args->track_namespace, &sc);
    if (rc < 0) return rc;
    m.track_name = args->track_name;
    m.track_alias = args->track_alias;
    m.properties.data = args->track_properties;
    m.properties.len = args->track_properties_len;
    /* Preserve location refusal before consulting authorization tokens. */
    moq_d22_location_filter_t filter;
    if (args->initial.flags & MOQ_PUBLISH_INITIAL_HAS_LOCATION) {
        rc = moq_d22_from_location_cfg(&args->initial.location, &filter);
        if (rc < 0) return rc;
    }
    if (args->auth_token_count > MOQ_DECODED_MAX_TOKENS) return MOQ_ERR_INVAL;

    /* D22 9.8 permits eight non-token options, in the wire type order below. */
    moq_d22_param_t params[MOQ_DECODED_MAX_TOKENS + 8] = {0};
    size_t count = 0;
    if (args->initial.flags & MOQ_PUBLISH_INITIAL_HAS_OBJECT_TIMEOUT) {
        params[count].type = D22_PARAM_OBJECT_DELIVERY_TIMEOUT;
        params[count].value.integer = args->initial.object_delivery_timeout_ms;
        ++count;
    }
    for (size_t i = 0; i < args->auth_token_count; ++i) {
        params[count].type = D22_PARAM_AUTHORIZATION_TOKEN;
        params[count].value.token.alias_type = D22_TOKEN_USE_VALUE;
        params[count].value.token.alias = 0;
        params[count].value.token.type = args->auth_tokens[i].token_type;
        params[count].value.token.value = args->auth_tokens[i].token_value;
        ++count;
    }
    if (args->initial.flags & MOQ_PUBLISH_INITIAL_HAS_SUBGROUP_TIMEOUT) {
        params[count].type = D22_PARAM_SUBGROUP_DELIVERY_TIMEOUT;
        params[count].value.integer = args->initial.subgroup_delivery_timeout_ms;
        ++count;
    }
    if (args->has_expires) {
        params[count].type = D22_PARAM_EXPIRES;
        params[count].value.integer = args->expires_ms;
        ++count;
    }
    if (args->has_largest) {
        params[count].type = D22_PARAM_LARGEST_OBJECT;
        params[count].value.location.group = args->largest_group;
        params[count].value.location.object = args->largest_object;
        ++count;
    }
    if (args->has_forward && !args->forward) {
        params[count].type = D22_PARAM_FORWARD;
        params[count].value.integer = 0;
        ++count;
    }
    if (args->initial.flags & MOQ_PUBLISH_INITIAL_HAS_SUBSCRIBER_PRIORITY) {
        params[count].type = D22_PARAM_SUBSCRIBER_PRIORITY;
        params[count].value.integer = args->initial.subscriber_priority;
        ++count;
    }
    if (args->initial.flags & MOQ_PUBLISH_INITIAL_HAS_LOCATION) {
        params[count].type = D22_PARAM_LOCATION_FILTER;
        params[count].value.filter = filter;
        ++count;
    }
    if (args->initial.flags & MOQ_PUBLISH_INITIAL_HAS_GROUP_ORDER) {
        params[count].type = D22_PARAM_GROUP_ORDER;
        params[count].value.integer = args->initial.group_order;
        ++count;
    }
    return d22_emit_params(w, &m, params, count, 0, false);
}

/* D22 9.3: PUBLISH_OK permits only EXPIRES and empty Track Properties. */
static moq_result_t d22_encode_publish_ok(moq_session_t *s, struct moq_buf_writer *w,
                                          const struct moq_publish_ok_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_REQUEST_OK;
    moq_d22_param_t expires = { .type = D22_PARAM_EXPIRES,
                                .value.integer = args->expires_ms };
    return d22_emit_params(w, &m, args->has_expires ? &expires : NULL,
                           args->has_expires ? 1 : 0, MOQ_D22_PUBLISH, false);
}

static moq_result_t d22_encode_publish_done(moq_session_t *s, struct moq_buf_writer *w,
                                            const struct moq_finish_publish_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_PUBLISH_DONE;
    m.code = args->status_code;
    m.stream_count = args->stream_count;
    m.reason.data = args->reason; m.reason.len = args->reason_len;
    return d22_emit(w, &m, NULL, 0, false);
}

static moq_result_t d22_encode_publish_state_notify(moq_session_t *s, struct moq_buf_writer *w,
                                                    const struct moq_publish_state_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_PUBLISH_STATE_NOTIFY;
    d22_pb_t b = { .n = 0 };
    if (args->has_largest) d22_pb_location(&b, args->largest_group, args->largest_object);
    if (args->has_forward) d22_pb_int(&b, D22_PARAM_FORWARD, args->forward ? 1 : 0);
    if (args->has_filter) {
        moq_d22_location_filter_t f;
        moq_result_t rc = moq_d22_from_subscription_filter(args->filter, args->start_group,
            args->start_object, args->end_group, args->end_object, &f);
        if (rc < 0) return rc;
        d22_pb_filter(&b, &f);
    }
    return d22_emit(w, &m, &b, 0, false);
}

static moq_result_t d22_encode_fetch(moq_session_t *s, struct moq_buf_writer *w,
                                     const struct moq_fetch_encode_args *args)
{
    (void)s;
    if (args->fetch_type != 1) return MOQ_ERR_UNSUPPORTED;   /* no joining forms */
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = MOQ_D22_FETCH;
    m.request_id = args->request_id;
    moq_result_t rc = d22_set_ns(&m, &args->track_namespace, &sc);
    if (rc < 0) return rc;
    m.track_name = args->track_name;
    d22_pb_t b = { .n = 0 };
    moq_d22_location_filter_t f;
    if (args->location) {
        /* The semantic form; an explicit NONE is the absent filter, which
         * selects the whole track through the Largest Object. */
        rc = moq_d22_from_location_cfg(args->location, &f);
        if (rc < 0) return rc;
        if (f.type != MOQ_D22_FILTER_NONE) d22_pb_filter(&b, &f);
    } else {
        rc = moq_d22_from_fetch_range(args->start_group, args->start_object,
                                      args->end_group, args->end_object, &f);
        if (rc < 0) return rc;
        d22_pb_filter(&b, &f);
    }
    if (args->has_fill_timeout)
        d22_pb_int(&b, D22_PARAM_FILL_TIMEOUT, args->fill_timeout_ms);
    if (args->subscriber_priority != 128)
        d22_pb_int(&b, D22_PARAM_SUBSCRIBER_PRIORITY, args->subscriber_priority);
    if (args->group_order != MOQ_GROUP_ORDER_DEFAULT)
        d22_pb_int(&b, D22_PARAM_GROUP_ORDER, args->group_order);
    if (args->has_include_properties)
        d22_pb_int(&b, D22_PARAM_INCLUDE_PROPERTIES, args->include_properties ? 1 : 0);
    rc = d22_pb_tokens(&b, args->auth_tokens, args->auth_token_count);
    if (rc < 0) return rc;
    return d22_emit(w, &m, &b, 0, false);
}

static moq_result_t d22_encode_fetch_ok(moq_session_t *s, struct moq_buf_writer *w,
                                        const struct moq_accept_fetch_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_FETCH_OK;
    m.end_of_track = args->end_of_track ? 1 : 0;
    /* The core's exclusive end object to §9.12's inclusive End Location; a
     * whole end group (zero) ends at the group's last Location, {G, 2^64-1}
     * (§3.2.2). */
    m.end_location.group = args->end_group;
    m.end_location.object = args->end_object == 0 ? UINT64_MAX : args->end_object - 1;
    m.properties.data = args->track_properties;
    m.properties.len = args->track_properties_len;
    return d22_emit(w, &m, NULL, 0, false);
}

static moq_result_t d22_encode_subscribe_namespace(moq_session_t *s, struct moq_buf_writer *w,
                                                   const struct moq_subscribe_namespace_encode_args *args)
{
    (void)s;
    /* Namespace-only: the public interest field has no wire form here. */
    if (args->namespace_interest != MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE)
        return MOQ_ERR_INVAL;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = MOQ_D22_SUBSCRIBE_NAMESPACE;
    m.request_id = args->request_id;
    moq_result_t rc = d22_set_ns(&m, &args->prefix, &sc);
    if (rc < 0) return rc;
    d22_pb_t b = { .n = 0 };
    rc = d22_pb_tokens(&b, args->auth_tokens, args->auth_token_count);
    if (rc < 0) return rc;
    return d22_emit(w, &m, &b, 0, false);
}

static moq_result_t d22_encode_namespace_msg(moq_session_t *s, struct moq_buf_writer *w,
                                             const struct moq_namespace_msg_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = args->is_done ? MOQ_D22_NAMESPACE_DONE : MOQ_D22_NAMESPACE;
    moq_result_t rc = d22_set_ns(&m, &args->suffix, &sc);
    if (rc < 0) return rc;
    return d22_emit(w, &m, NULL, 0, false);
}

static moq_result_t d22_encode_subscribe_tracks(moq_session_t *s, struct moq_buf_writer *w,
                                                const struct moq_subscribe_tracks_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = MOQ_D22_SUBSCRIBE_TRACKS;
    m.request_id = args->request_id;
    moq_result_t rc = d22_set_ns(&m, &args->prefix, &sc);
    if (rc < 0) return rc;
    d22_pb_t b = { .n = 0 };
    if (args->has_forward && !args->forward) d22_pb_int(&b, D22_PARAM_FORWARD, 0);
    if (args->options_flags & MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER)
        d22_pb_int(&b, D22_PARAM_GROUP_ORDER, args->group_order);
    if (args->options_flags & MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES)
        d22_pb_int(&b, D22_PARAM_INCLUDE_PROPERTIES, args->include_properties ? 1 : 0);
    rc = d22_pb_tokens(&b, args->auth_tokens, args->auth_token_count);
    if (rc < 0) return rc;
    return d22_emit(w, &m, &b, 0, false);
}

static moq_result_t d22_encode_publish_blocked(moq_session_t *s, struct moq_buf_writer *w,
                                               const moq_namespace_t *suffix,
                                               moq_bytes_t track_name)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = MOQ_D22_PUBLISH_SKIPPED;
    moq_result_t rc = d22_set_ns(&m, suffix, &sc);
    if (rc < 0) return rc;
    m.track_name = track_name;
    return d22_emit(w, &m, NULL, 0, false);
}

static moq_result_t d22_encode_namespace_update(moq_session_t *s, struct moq_buf_writer *w,
    const struct moq_namespace_update_encode_args *args)
{
    (void)s;
    uint64_t family_type = d22_family_type(args->family);
    if (family_type != MOQ_D22_PUBLISH_NAMESPACE &&
        family_type != MOQ_D22_SUBSCRIBE_NAMESPACE &&
        family_type != MOQ_D22_SUBSCRIBE_TRACKS)
        return MOQ_ERR_INVAL;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_REQUEST_UPDATE;
    m.request_id = args->request_id;
    d22_pb_t b = { .n = 0 };
    if (args->has_forward) d22_pb_int(&b, D22_PARAM_FORWARD, args->forward ? 1 : 0);
    d22_ns_scratch_t sc;
    if (args->has_prefix) {
        moq_buf_writer_t nw;
        moq_buf_writer_init(&nw, sc.buf, sizeof(sc.buf));
        moq_result_t rc = moq_d22_encode_namespace(&nw, &args->prefix);
        if (rc < 0) return rc;
        moq_d22_param_t v; memset(&v, 0, sizeof(v));
        v.type = D22_PARAM_TRACK_NAMESPACE_PREFIX;
        v.value.ns.encoded.data = sc.buf;
        v.value.ns.encoded.len = moq_buf_writer_offset(&nw);
        v.value.ns.count = args->prefix.count;
        d22_pb_add(&b, &v);
    }
    moq_result_t rc = d22_pb_tokens(&b, args->auth_tokens, args->auth_token_count);
    if (rc < 0) return rc;
    return d22_emit(w, &m, &b, family_type, true);
}

static moq_result_t d22_encode_track_status(moq_session_t *s, struct moq_buf_writer *w,
                                            const struct moq_track_status_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    d22_ns_scratch_t sc;
    m.type = MOQ_D22_TRACK_STATUS;
    m.request_id = args->request_id;
    moq_result_t rc = d22_set_ns(&m, &args->track_namespace, &sc);
    if (rc < 0) return rc;
    m.track_name = args->track_name;
    d22_pb_t b = { .n = 0 };
    if (args->has_include_properties)
        d22_pb_int(&b, D22_PARAM_INCLUDE_PROPERTIES, args->include_properties ? 1 : 0);
    rc = d22_pb_tokens(&b, args->auth_tokens, args->auth_token_count);
    if (rc < 0) return rc;
    return d22_emit(w, &m, &b, 0, false);
}

/* TRACK_STATUS's REQUEST_OK carries LARGEST_OBJECT and the track properties;
 * this draft defines no EXPIRES for it, so that argument has no wire form. */
static moq_result_t d22_encode_track_status_ok(moq_session_t *s, struct moq_buf_writer *w,
                                               const struct moq_track_status_ok_encode_args *args)
{
    (void)s;
    moq_d22_message_t m; memset(&m, 0, sizeof(m));
    m.type = MOQ_D22_REQUEST_OK;
    m.properties.data = args->track_properties;
    m.properties.len = args->track_properties_len;
    d22_pb_t b = { .n = 0 };
    if (args->has_largest) d22_pb_location(&b, args->largest_group, args->largest_object);
    return d22_emit(w, &m, &b, MOQ_D22_TRACK_STATUS, false);
}

/* -- Namespace-subscription stream decoders --------------------------- */

static moq_result_t d22_decode_ns_sub_request(moq_session_t *s, const uint8_t *data,
                                              size_t len, moq_decoded_ns_sub_request_t *out)
{
    memset(out, 0, sizeof(*out));
    out->forward = true;
    out->namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_control_envelope_t env;
    moq_result_t rc = moq_d22_decode_envelope(&r, &env);
    if (rc == MOQ_ERR_BUFFER) return MOQ_ERR_BUFFER;
    if (rc < 0) return MOQ_ERR_PROTO;
    if (env.msg_type != MOQ_D22_SUBSCRIBE_NAMESPACE) return MOQ_ERR_PROTO;
    out->has_trailing_bytes = (moq_buf_reader_offset(&r) != len);
    moq_d22_message_t m;
    rc = d22_decode(MOQ_D22_SUBSCRIBE_NAMESPACE, env.payload, env.payload_len, 0, false, &m);
    if (rc == MOQ_REQUEST_DECODE_BAD_PARAMS)
        return close_with_error(s, 0x6, "malformed auth token structure");
    if (rc < 0) return MOQ_ERR_PROTO;
    moq_request_tokens_t tk;
    d22_scalars_t v;
    rc = d22_scan(&m.params, &v, &tk);
    if (rc < 0) return MOQ_ERR_PROTO;
    rc = d22_ns_parts(&m.ns, out->parts_buf, MOQ_DECODED_NS_SUB_MAX_PARTS, &out->prefix);
    if (rc < 0) return MOQ_ERR_PROTO;
    out->request_id = m.request_id;
    for (size_t i = 0; i < tk.count; i++) out->auth_tokens[i] = tk.in[i];
    out->auth_token_count = tk.count;
    return MOQ_OK;
}

static moq_result_t d22_decode_ns_sub_response(moq_session_t *s, const uint8_t *data,
                                               size_t len, bool got_response,
                                               uint64_t expected_request_id,
                                               moq_decoded_ns_sub_response_t *out)
{
    (void)s; (void)expected_request_id;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_control_envelope_t env;
    moq_result_t rc = moq_d22_decode_envelope(&r, &env);
    if (rc == MOQ_ERR_BUFFER) return MOQ_ERR_BUFFER;
    if (rc < 0) {
        out->close_reason = "malformed bidi response stream envelope";
        return MOQ_ERR_PROTO;
    }
    size_t consumed = moq_buf_reader_offset(&r);
    if (env.msg_type == MOQ_D22_GOAWAY) {
        moq_d22_goaway_t ga;
        rc = moq_d22_decode_goaway(env.payload, env.payload_len, &ga);
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
    moq_d22_message_t m;
    if (!got_response) {
        switch (env.msg_type) {
        case MOQ_D22_REQUEST_OK:
            rc = d22_decode(MOQ_D22_REQUEST_OK, env.payload, env.payload_len,
                            MOQ_D22_SUBSCRIBE_NAMESPACE, false, &m);
            if (rc < 0) {
                out->close_reason = "malformed REQUEST_OK on bidi stream";
                return MOQ_ERR_PROTO;
            }
            out->kind = MOQ_NS_RESP_OK;
            out->consumed = consumed;
            return MOQ_OK;
        case MOQ_D22_REQUEST_ERROR:
            out->has_trailing_bytes = (consumed < len);
            rc = d22_decode(MOQ_D22_REQUEST_ERROR, env.payload, env.payload_len,
                            MOQ_D22_SUBSCRIBE_NAMESPACE, false, &m);
            if (rc < 0) {
                out->close_reason = "malformed REQUEST_ERROR on bidi stream";
                return MOQ_ERR_PROTO;
            }
            out->kind = MOQ_NS_RESP_ERROR;
            out->consumed = consumed;
            out->error_code = m.code;
            out->retry_interval = m.retry_interval;
            out->reason = m.reason.data;
            out->reason_len = m.reason.len;
            if (m.code == MOQ_D22_ERROR_REDIRECT) {
                out->has_redirect = true;
                out->redirect.connect_uri = m.redirect_uri.data;
                out->redirect.connect_uri_len = m.redirect_uri.len;
                if (d22_ns_parts(&m.ns, out->parts_buf, MOQ_DECODED_NS_SUB_MAX_PARTS,
                                 &out->redirect.track_namespace) < 0) {
                    out->close_reason = "malformed REQUEST_ERROR on bidi stream";
                    return MOQ_ERR_PROTO;
                }
                out->redirect.track_name = m.track_name.data;
                out->redirect.track_name_len = m.track_name.len;
            }
            return MOQ_OK;
        case MOQ_D22_NAMESPACE:
        case MOQ_D22_NAMESPACE_DONE:
            out->close_reason = "NAMESPACE/NAMESPACE_DONE before REQUEST_OK";
            return MOQ_ERR_PROTO;
        default:
            out->close_reason = "unexpected message type on bidi response stream";
            return MOQ_ERR_PROTO;
        }
    }
    switch (env.msg_type) {
    case MOQ_D22_NAMESPACE:
    case MOQ_D22_NAMESPACE_DONE:
        rc = d22_decode(env.msg_type, env.payload, env.payload_len, 0, false, &m);
        if (rc < 0 || d22_ns_parts(&m.ns, out->parts_buf, MOQ_DECODED_NS_SUB_MAX_PARTS,
                                   &out->suffix) < 0) {
            out->close_reason = "malformed namespace suffix on bidi stream";
            return MOQ_ERR_PROTO;
        }
        out->kind = (env.msg_type == MOQ_D22_NAMESPACE)
            ? MOQ_NS_RESP_NAMESPACE : MOQ_NS_RESP_NAMESPACE_DONE;
        out->consumed = consumed;
        return MOQ_OK;
    case MOQ_D22_REQUEST_OK: {
        /* Only an answer to this side's update; the session checks that one
         * is outstanding. */
        rc = d22_decode(MOQ_D22_REQUEST_OK, env.payload, env.payload_len,
                        MOQ_D22_REQUEST_UPDATE, false, &m);
        d22_scalars_t v;
        if (rc < 0 || d22_scan(&m.params, &v, NULL) < 0) {
            out->close_reason = "malformed REQUEST_OK on bidi stream";
            return MOQ_ERR_PROTO;
        }
        out->kind = MOQ_NS_RESP_UPDATE_OK;
        out->consumed = consumed;
        out->has_expires = v.has_expires;
        out->expires_ms = v.expires_ms;
        return MOQ_OK;
    }
    case MOQ_D22_REQUEST_ERROR:
        rc = d22_decode(MOQ_D22_REQUEST_ERROR, env.payload, env.payload_len,
                        MOQ_D22_SUBSCRIBE_NAMESPACE, false, &m);
        if (rc < 0) {
            out->close_reason = "malformed REQUEST_ERROR on bidi stream";
            return MOQ_ERR_PROTO;
        }
        if (m.code == MOQ_D22_ERROR_REDIRECT) {
            out->close_reason = "REDIRECT on a namespace update";
            return MOQ_ERR_PROTO;
        }
        out->kind = MOQ_NS_RESP_UPDATE_ERROR;
        out->consumed = consumed;
        out->has_trailing_bytes = (consumed < len);
        out->error_code = m.code;
        out->retry_interval = m.retry_interval;
        out->reason = m.reason.data;
        out->reason_len = m.reason.len;
        return MOQ_OK;
    default:
        out->close_reason = "unexpected message type on bidi response stream";
        return MOQ_ERR_PROTO;
    }
}

/* -- Fetch range markers: the shared framing plus End of Timed-Out Range -- */

#define D22_FETCH_END_TIMED_OUT UINT64_C(0x20C)

static moq_result_t d22_decode_fetch_object(moq_session_t *s, struct moq_buf_reader *r,
                                            moq_group_order_t effective_order,
                                            const struct moq_fetch_prior_object *prev,
                                            struct moq_decoded_fetch_object *out)
{
    if (effective_order != MOQ_GROUP_ORDER_ASCENDING &&
        effective_order != MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;
    size_t saved = r->pos;
    uint64_t flags;
    if (moq_buf_read_vi64(r, &flags) < 0) return MOQ_ERR_BUFFER;
    if (flags == D22_FETCH_END_TIMED_OUT) {
        memset(out, 0, sizeof(*out));
        out->is_range_marker = true;
        out->range_kind = MOQ_FETCH_RANGE_TIMED_OUT;
        if (moq_buf_read_vi64(r, &out->group_id) < 0) return MOQ_ERR_BUFFER;
        if (moq_buf_read_vi64(r, &out->object_id) < 0) return MOQ_ERR_BUFFER;
        return MOQ_OK;
    }
    r->pos = saved;
    return moq_d18_profile_ops()->decode_fetch_object(s, r, effective_order, prev, out);
}

static moq_result_t d22_encode_fetch_range(moq_session_t *s, struct moq_buf_writer *w,
                                           uint32_t range_kind, uint64_t group_id,
                                           uint64_t object_id)
{
    if (range_kind != MOQ_FETCH_RANGE_TIMED_OUT)
        return moq_d18_profile_ops()->encode_fetch_range(s, w, range_kind, group_id,
                                                         object_id);
    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_vi64(w, D22_FETCH_END_TIMED_OUT);
    if (rc == MOQ_OK) rc = moq_buf_write_vi64(w, group_id);
    if (rc == MOQ_OK) rc = moq_buf_write_vi64(w, object_id);
    if (rc < 0) w->pos = saved;
    return rc;
}

/* §13: an unknown error code "MUST be treated as equivalent to
 * INTERNAL_ERROR". The registry is §16.11.2: draft-18's codes without
 * DUPLICATE_SUBSCRIPTION (0x19) and INVALID_JOINING_REQUEST_ID (0x32), plus
 * CONFLICTING_FILTERS (0x35) and INVALID_FILTER (0x36). The greasing row is
 * reserved so that implementations meet values they do not understand; it
 * is not a semantic code. */
static moq_request_error_t d22_semantic_request_error(uint64_t raw)
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
    case MOQ_REQUEST_ERROR_UNINTERESTED:
    case MOQ_REQUEST_ERROR_PREFIX_OVERLAP:
    case MOQ_REQUEST_ERROR_NAMESPACE_TOO_LARGE:
    case MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION:
    case MOQ_REQUEST_ERROR_REDIRECT:
    case MOQ_REQUEST_ERROR_CONFLICTING_FILTERS:
    case MOQ_REQUEST_ERROR_INVALID_FILTER:
        return (moq_request_error_t)raw;
    default:
        return MOQ_REQUEST_ERROR_INTERNAL_ERROR;
    }
}

/* -- Vtable --------------------------------------------------------- */

/* Constant, like the draft-16 and draft-18 tables: the lookup is reachable
 * from any thread (moq_version_supported, profile lookup at session
 * create), so the table is never written after load. The data plane and the
 * request-stream stubs name the draft-18 operations directly. */
static moq_result_t d22_encode_setup_token(struct moq_buf_writer *w,
                                           uint64_t token_type, moq_bytes_t value);

static const moq_profile_ops_t d22_ops = {
    .version      = MOQ_VERSION_DRAFT_22,
    .state_size   = sizeof(moq_d22_profile_state_t),
    .state_align  = _Alignof(moq_d22_profile_state_t),
    .init_in_place        = d22_init_in_place,
    .destroy              = d22_destroy,
    .start                = d22_start,
    .process_control_data = d22_process_control_data,
    .encode_goaway        = d22_encode_goaway,

    /* Request-family encoders and the namespace-subscription decoders. */
    .encode_subscribe           = d22_encode_subscribe,
    .encode_subscribe_ok        = d22_encode_subscribe_ok,
    .encode_request_ok          = d22_encode_request_ok,
    .encode_request_update_ok   = d22_encode_request_update_ok,
    .encode_request_error       = d22_encode_request_error,
    .encode_request_goaway      = d22_encode_request_goaway,
    .encode_request_update      = d22_encode_request_update,
    .encode_publish_namespace   = d22_encode_publish_namespace,
    .encode_publish             = d22_encode_publish,
    .encode_publish_ok          = d22_encode_publish_ok,
    .encode_publish_done        = d22_encode_publish_done,
    .encode_publish_state_notify = d22_encode_publish_state_notify,
    .encode_fetch               = d22_encode_fetch,
    .encode_fetch_ok            = d22_encode_fetch_ok,
    .encode_subscribe_namespace = d22_encode_subscribe_namespace,
    .encode_namespace_msg       = d22_encode_namespace_msg,
    .encode_subscribe_tracks    = d22_encode_subscribe_tracks,
    .encode_namespace_update    = d22_encode_namespace_update,
    .encode_publish_blocked     = d22_encode_publish_blocked,
    .encode_track_status        = d22_encode_track_status,
    .encode_track_status_ok     = d22_encode_track_status_ok,
    .decode_ns_sub_request      = d22_decode_ns_sub_request,
    .decode_ns_sub_response     = d22_decode_ns_sub_response,
    .encode_unsubscribe         = moq_d18_op_encode_unsubscribe,   /* no wire message on either draft */
    .track_properties_dynamic_groups = moq_d18_op_track_properties_dynamic_groups,
    .track_properties_add_dynamic_groups = moq_d18_op_track_properties_add_dynamic_groups,
    .track_properties_group_order = moq_d18_op_track_properties_group_order,
    .track_properties_add_group_order = moq_d18_op_track_properties_add_group_order,
    .track_properties_max_cache = moq_d18_op_track_properties_max_cache,

    /* Request admission and dispatch: the shared engine. */
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
    .request_codec           = &d22_request_codec,
    .grant_capacity          = moq_d18_op_grant_capacity,
    .peer_request_capacity   = moq_d18_op_request_capacity,
    .local_request_capacity  = moq_d18_op_request_capacity,
    .semantic_request_error  = d22_semantic_request_error,
    .duplicate_subscription_error = MOQ_REQUEST_ERROR_NOT_SUPPORTED,

    /* Data plane and stream classification: identical framing to draft-18. */
    .classify_uni_stream          = moq_d18_op_classify_uni_stream,
    .classify_data_stream         = moq_d18_op_classify_data_stream,
    .classify_bidi_stream         = moq_d18_op_classify_bidi_stream,
    .decode_subgroup_header       = moq_d18_op_decode_subgroup_header,
    .encode_subgroup_header       = moq_d18_op_encode_subgroup_header,
    .encode_object_header         = moq_d18_op_encode_object_header,
    .encode_object_payload_prefix = moq_d18_op_encode_object_payload_prefix,
    .validate_object_properties   = moq_d18_op_validate_object_properties,
    .decode_object_header         = moq_d18_op_decode_object_header,
    .encode_object_datagram       = moq_d18_op_encode_object_datagram,
    .decode_object_datagram       = moq_d18_op_decode_object_datagram,
    .decode_fetch_header          = moq_d18_op_decode_fetch_header,
    .encode_fetch_header          = moq_d18_op_encode_fetch_header,
    .decode_fetch_object          = d22_decode_fetch_object,
    .encode_fetch_object          = moq_d18_op_encode_fetch_object,
    .encode_fetch_range           = d22_encode_fetch_range,
    .scan_delivery_timeouts       = moq_d18_op_scan_delivery_timeouts,
    .scan_default_publisher_priority = moq_d18_scan_default_publisher_priority,
    .validate_track_properties = moq_d22_validate_track_properties,
    .min_track_namespace_fields   = 0,
    .object_payload_len_max       = MOQ_VI64_MAX,
    .location_varint_max          = MOQ_VI64_MAX,
    .request_error_wire_max       = MOQ_VI64_MAX,
    .uses_request_streams         = true,
    .uses_uni_control_channel     = true,
    .fetch_datagram_supported     = true,   /* fetch object header carries the datagram bit */
    .fetch_descending_supported   = true,   /* retained request/RX order for deltas */

    /* Behavioural capabilities. */
    .wire_ready                       = true,
    .request_retains_pending_original_input = true,
    .request_fin_is_not_cancel        = true,
    .premature_fin_fails_request      = true,
    .request_drain_requires_receive_terminal = true,
    /* §3.1: "An endpoint MAY have multiple concurrent subscriptions to the
     * same Track ... A publisher MAY assign the same or different Track
     * Aliases to these subscriptions." duplicate_subscription_error above is
     * therefore never sent on this wire. */
    .allows_concurrent_subscriptions  = true,
    .publish_initial_parameters       = true,
    .subscription_update_options      = true,
    .subscription_admission_options   = true,
    .publish_ok_carries_params        = false,
    .supports_joining_fetch           = false,
    .supports_fill                    = true,
    .fetch_request_options            = true,
    .fetch_range_timed_out            = true,
    .stream_count_unknown             = UINT64_MAX,
    .subgroup_timer_at_fin            = true,
    .object_timer_at_header           = true,
    .first_object_timeout_override    = true,
    .publish_done_subscription_ended  = false,
    .redirect_on_any_request          = true,
    .request_update_errors_coalesce   = true,
    /* SETUP on this wire names the connection target when carried natively
     * over QUIC and refuses it over WebTransport, and the connection must
     * have negotiated QUIC DATAGRAM support; the host states both before
     * start. */
    .binding_requirements             = MOQ_BINDING_FACTS_ALL,
    .auth_token_type_max              = UINT64_MAX,   /* §8.9: vi64 */
    /* MAX_REQUEST_UPDATES as sent in SETUP; exceeding it is
     * TOO_MANY_REQUEST_UPDATES. */
    .subscribe_tracks_options         = true,
    .include_properties_request       = true,
    .request_updates_max              = D22_LOCAL_MAX_REQUEST_UPDATES,
    .request_updates_exceeded_error   = D22_ERR_TOO_MANY_REQUEST_UPDATES,
    .going_away_request_error         = MOQ_REQUEST_ERROR_GOING_AWAY,
    .capacity_request_error           = MOQ_REQUEST_ERROR_EXCESSIVE_LOAD,
    .capacity_request_error_retryable = true,
    .reserved_session_namespaces      = true,
    .relative_group_clamps            = true,
    .malformed_track_local            = true,
    .scan_object_gaps                 = moq_d18_op_scan_object_gaps,
    .add_object_gap                   = moq_d18_op_add_object_gap,
    .requester_updates                = true,
    .encode_setup_token               = d22_encode_setup_token,
};

/* A SETUP token's option value (§9.1.4, §8.9): Alias Type USE_VALUE, Token
 * Type, Token Value; the option's own length delimits the value. */
static moq_result_t d22_encode_setup_token(struct moq_buf_writer *w,
                                           uint64_t token_type, moq_bytes_t value)
{
    moq_result_t rc = moq_buf_write_vi64(w, D22_TOKEN_USE_VALUE);
    if (rc == MOQ_OK) rc = moq_buf_write_vi64(w, token_type);
    if (rc == MOQ_OK && value.len) rc = moq_buf_write_raw(w, value.data, value.len);
    return rc;
}

const moq_profile_ops_t *moq_d22_profile_ops(void)
{
    return &d22_ops;
}
