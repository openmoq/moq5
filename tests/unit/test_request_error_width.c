/*
 * REQUEST_ERROR code width and unknown-code handling: the regression
 * contract for the semantic error domain.
 *
 * `moq_request_error_t` is 64 bits wide, because the protocol's error-code
 * domain is. The two profiles differ in what they can carry and in what a
 * received value MEANS, and both differences are protocol facts, not
 * implementation choices:
 *
 *   WIRE DOMAIN
 *     draft-16 §9.8 encodes Error Code as a QUIC variable-length integer,
 *     so its ceiling is MOQ_QUIC_VARINT_MAX (2^62-1). draft-18 §10.6.2
 *     encodes it as a vi64, which §1.4.1 (:490-535) states can encode every
 *     64-bit unsigned integer, so its ceiling is MOQ_VI64_MAX (UINT64_MAX).
 *     Each profile publishes its own limit as `request_error_wire_max`, and
 *     the public reject/cancel APIs refuse an unrepresentable code BEFORE
 *     the advancing preamble runs rather than truncating it.
 *
 *   SEMANTICS
 *     draft-18 §15 (:6411-6415) requires every UNKNOWN code -- including the
 *     §14 GREASE pattern 0x7f*N + 0x9D, which is reserved precisely so
 *     implementations exercise values they do not understand -- to be
 *     treated as INTERNAL_ERROR, and forbids closing the session over one.
 *     Its registry is §15.10.2 (:6839), 18 codes. draft-16 states no such
 *     rule at all: its §13.4.2 registry (:5453) is a bare table of 13, so a
 *     draft-16 session surfaces the peer's value unchanged. Each profile
 *     owns that decision through `semantic_request_error`.
 *
 * What this file prevents, and why each cell exists:
 *
 *   - TRUNCATION. UINT64_C(0x100000001) is legal and unregistered on both
 *     drafts, and its low 32 bits are exactly UNAUTHORIZED. A narrowed
 *     carrier does not merely lose information -- it manufactures a
 *     DIFFERENT, REGISTERED, security-relevant verdict the peer never sent.
 *     Every public carrier, config field and C++ record is pinned at 64
 *     bits, and the width oracles observe truncation at RUNTIME so a
 *     narrowing mutant still builds.
 *   - MISSING NORMALIZATION, which is a separate defect: 0x77 and GREASE
 *     0x9D involve no truncation at all, and draft-18 must still map them.
 *   - OVER-BROAD NORMALIZATION: every registered code of each draft's own
 *     table must pass through unchanged on that draft.
 *   - REDIRECT recognition happening after truncation or normalization: a
 *     value whose low bits are 0x34 must never manufacture a redirect
 *     event, while a genuine 0x34 with a Redirect tail still must.
 *   - REFUSAL AFTER MUTATION: an unrepresentable outbound code must be
 *     refused before logical time advances, borrows are invalidated,
 *     scratch is reclaimed or a deferred sweep runs.
 *
 * Every inbound cell additionally pins the arm identity, correlation
 * against the handle the public API returned, the reason bytes, the owner
 * and registry topology across draining and retirement, the drain
 * lifecycle, and monotonically increasing epochs -- so a close, drop or
 * misroute cannot satisfy a code assertion.
 */
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include "../support/ownership_graph.h"
#include <moq/control.h>
#include <moq/control_d18.h>

/* -- fixture constants: everything compared against is declared here ---- */

#define WIDE_UNKNOWN_CODE   UINT64_C(0x100000001) /* low 32 bits == 0x1 */
#define NARROW_UNKNOWN_CODE UINT64_C(0x77)        /* in range, unregistered */
#define GREASE_CODE         UINT64_C(0x9D)        /* d18 §14 first GREASE */
/*
 * A wide value whose LOW 32 BITS are REDIRECT (0x34). Redirect recognition
 * must be a FULL-WIDTH operation: truncating first would manufacture a
 * redirect the peer never sent, on a message carrying no Redirect tail.
 */
#define WIDE_REDIRECT_CODE  UINT64_C(0x100000034)
#define COLLIDING_LOW32     MOQ_REQUEST_ERROR_UNAUTHORIZED
#define RETRY_WIRE          UINT64_C(4000)
#define OUT_RETRY_MS        UINT64_C(1234)

/*
 * Deliberately NON-DEFAULT request values, so the emitted SUBSCRIBE is
 * compared against something an encoder cannot satisfy by omission.
 */
#define SUB_PRIORITY      200
#define SUB_GROUP_ORDER   MOQ_GROUP_ORDER_DESCENDING
#define SUB_FILTER        MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT
#define SUB_FORWARD       false
#define SUB_NEW_GROUP     UINT64_C(7)
#define SUB_TOKEN_TYPE    UINT64_C(5)

static const uint8_t k_token_bytes[] = { 't', 'o', 'k', '1' };

static const uint8_t k_ns_bytes[] = { 'l', 'i', 'v', 'e' };
static const uint8_t k_name_bytes[] = { 'v' };
static const uint8_t k_reason_bytes[] = { 'n', 'o', 'p', 'e' };
static const moq_bytes_t k_ns[1] = { { k_ns_bytes, sizeof(k_ns_bytes) } };

/* ------------------------------------------------------------------ */
/* Codec fidelity: exact envelope type, exact body, reader exhausted.    */
/* ------------------------------------------------------------------ */

static int codec_d16_preserves_wide_code(void)
{
    int failures = 0;
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_reader_t r;
    moq_control_envelope_t env;
    moq_d16_request_error_t err;

    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT(moq_d16_encode_request_error(
        &w, 7, WIDE_UNKNOWN_CODE, RETRY_WIRE,
        k_reason_bytes, sizeof(k_reason_bytes)), MOQ_OK);

    moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK_EQ_INT(moq_control_decode_envelope(&r, &env), MOQ_OK);
    MOQ_TEST_CHECK(env.msg_type == MOQ_D16_REQUEST_ERROR);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);

    memset(&err, 0, sizeof(err));
    MOQ_TEST_CHECK_EQ_INT(
        moq_d16_decode_request_error(env.payload, env.payload_len, &err),
        MOQ_OK);

    /* the discriminator: full width, not the low 32 bits */
    MOQ_TEST_CHECK(err.error_code == WIDE_UNKNOWN_CODE);
    MOQ_TEST_CHECK(err.error_code != (uint64_t)COLLIDING_LOW32);
    /* and every accompanying field, so this is a whole-message claim */
    MOQ_TEST_CHECK(err.request_id == 7);
    MOQ_TEST_CHECK(err.retry_interval == RETRY_WIRE);
    MOQ_TEST_CHECK_EQ_SIZE(err.reason_len, sizeof(k_reason_bytes));
    MOQ_TEST_CHECK(err.reason != NULL);
    if (err.reason != NULL)
        MOQ_TEST_CHECK(memcmp(err.reason, k_reason_bytes,
                              sizeof(k_reason_bytes)) == 0);
    return failures;
}

static int codec_d18_preserves_wide_code(void)
{
    int failures = 0;
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_reader_t r;
    moq_control_envelope_t env;
    moq_d18_request_error_t err;

    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT(moq_d18_encode_request_error(
        &w, WIDE_UNKNOWN_CODE, RETRY_WIRE,
        (moq_bytes_t){ k_reason_bytes, sizeof(k_reason_bytes) }), MOQ_OK);

    moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
    MOQ_TEST_CHECK_EQ_INT(moq_d18_decode_envelope(&r, &env), MOQ_OK);
    MOQ_TEST_CHECK(env.msg_type == MOQ_D18_REQUEST_ERROR);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);

    memset(&err, 0, sizeof(err));
    MOQ_TEST_CHECK_EQ_INT(
        moq_d18_decode_request_error(env.payload, env.payload_len, &err),
        MOQ_OK);

    MOQ_TEST_CHECK(err.error_code == WIDE_UNKNOWN_CODE);
    MOQ_TEST_CHECK(err.error_code != (uint64_t)COLLIDING_LOW32);
    MOQ_TEST_CHECK(err.retry_interval == RETRY_WIRE);
    MOQ_TEST_CHECK_EQ_SIZE(err.reason.len, sizeof(k_reason_bytes));
    MOQ_TEST_CHECK(err.reason.data != NULL);
    if (err.reason.data != NULL)
        MOQ_TEST_CHECK(memcmp(err.reason.data, k_reason_bytes,
                              sizeof(k_reason_bytes)) == 0);
    return failures;
}

/* ------------------------------------------------------------------ */
/* Queue classification                                                 */
/* ------------------------------------------------------------------ */

struct act_tally {
    int total;
    int send_control;
    int open_uni_control;
    int send_uni_control;
    int open_bidi;
    int send_bidi;
    int close_bidi;
    int other;
    moq_stream_ref_t close_ref;
    moq_stream_ref_t open_ref;
    /* the last SEND_CONTROL / SEND_BIDI payload, copied out of the borrow */
    uint8_t          bytes[512];
    size_t           bytes_len;
    moq_stream_ref_t bytes_ref;
    bool             bytes_fin;
    bool             bytes_truncated;
};

static void tally_actions(moq_session_t *s, struct act_tally *t)
{
    moq_action_t a;

    memset(t, 0, sizeof(*t));
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        t->total++;
        switch (a.kind) {
        case MOQ_ACTION_SEND_CONTROL:
            t->send_control++;
            if (a.u.send_control.len <= sizeof(t->bytes)) {
                memcpy(t->bytes, a.u.send_control.data, a.u.send_control.len);
                t->bytes_len = a.u.send_control.len;
            } else {
                t->bytes_truncated = true;
            }
            break;
        case MOQ_ACTION_OPEN_UNI_CONTROL:
            /* draft-18 carries the first control bytes inline on the open */
            t->open_uni_control++;
            if (a.u.open_uni_control.len <= sizeof(t->bytes)) {
                memcpy(t->bytes, a.u.open_uni_control.data,
                       a.u.open_uni_control.len);
                t->bytes_len = a.u.open_uni_control.len;
            } else {
                t->bytes_truncated = true;
            }
            break;
        case MOQ_ACTION_SEND_UNI_CONTROL:
            t->send_uni_control++;
            if (a.u.send_uni_control.len <= sizeof(t->bytes)) {
                memcpy(t->bytes, a.u.send_uni_control.data,
                       a.u.send_uni_control.len);
                t->bytes_len = a.u.send_uni_control.len;
            } else {
                t->bytes_truncated = true;
            }
            break;
        case MOQ_ACTION_OPEN_BIDI_STREAM:
            /* the first request bytes ride inline on the open, as with the
             * uni control channel */
            t->open_bidi++;
            t->open_ref = a.u.open_bidi_stream.stream_ref;
            if (a.u.open_bidi_stream.len <= sizeof(t->bytes)) {
                memcpy(t->bytes, a.u.open_bidi_stream.data,
                       a.u.open_bidi_stream.len);
                t->bytes_len = a.u.open_bidi_stream.len;
            } else {
                t->bytes_truncated = true;
            }
            t->bytes_ref = a.u.open_bidi_stream.stream_ref;
            t->bytes_fin = a.u.open_bidi_stream.fin;
            break;
        case MOQ_ACTION_SEND_BIDI_STREAM:
            t->send_bidi++;
            if (a.u.send_bidi_stream.len <= sizeof(t->bytes)) {
                memcpy(t->bytes, a.u.send_bidi_stream.data,
                       a.u.send_bidi_stream.len);
                t->bytes_len = a.u.send_bidi_stream.len;
            } else {
                t->bytes_truncated = true;
            }
            t->bytes_ref = a.u.send_bidi_stream.stream_ref;
            t->bytes_fin = a.u.send_bidi_stream.fin;
            break;
        case MOQ_ACTION_CLOSE_BIDI_STREAM:
            t->close_bidi++;
            t->close_ref = a.u.close_bidi_stream.stream_ref;
            break;
        default:
            t->other++;
            break;
        }
        moq_action_cleanup(&a);
    }
}

/* Drain every event, returning how many matched `kind` and setting *total. */
static int count_events_of_kind(moq_session_t *s, uint32_t kind, int *total)
{
    moq_event_t e;
    int n = 0;

    *total = 0;
    while (moq_session_poll_events(s, &e, 1) > 0) {
        (*total)++;
        if (e.kind == kind)
            n++;
        moq_event_cleanup(&e);
    }
    return n;
}

/* ------------------------------------------------------------------ */
/* Establishment, classified exactly                                    */
/* ------------------------------------------------------------------ */

static int make_pair(moq_version_t version, moq_session_t **c_out,
                     moq_session_t **sv_out)
{
    int failures = 0;
    moq_session_cfg_t ccfg, scfg;
    moq_session_t *c = NULL, *sv = NULL;
    struct act_tally t;
    int total = 0;

    *c_out = NULL;
    *sv_out = NULL;

    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    ccfg.version = version;
    scfg.version = version;
    ccfg.send_request_capacity = true;
    scfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 16;
    scfg.initial_request_capacity = 16;

    MOQ_TEST_CHECK_EQ_INT(moq_session_create(&ccfg, 0, &c), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_session_create(&scfg, 0, &sv), MOQ_OK);
    if (c == NULL || sv == NULL)
        return failures;

    /*
     * The SETUP handshake is one control message each way, but its shape is
     * per draft and both are classified exactly rather than drained blindly:
     *
     *   draft-16 -- the client writes CLIENT_SETUP on the control stream
     *     (SEND_CONTROL); the server answers with SERVER_SETUP the same way,
     *     and only the client is started.
     *   draft-18 -- each side opens a dedicated unidirectional control
     *     channel and carries its own setup inline on the open
     *     (OPEN_UNI_CONTROL), so BOTH sides are started and the server's
     *     setup exists before it has seen the client's.
     *
     * Either way the bytes are handed to the peer through on_control_bytes,
     * which is the session's own contract; the uni/bidi framing above it
     * belongs to the transport bridge and is not what this file is about.
     */
    if (version == MOQ_VERSION_DRAFT_18) {
        uint8_t server_setup[512];
        size_t  server_setup_len;

        MOQ_TEST_CHECK_EQ_INT(moq_session_start(sv, 0), MOQ_OK);
        tally_actions(sv, &t);
        MOQ_TEST_CHECK_EQ_INT(t.total, 1);
        MOQ_TEST_CHECK_EQ_INT(t.open_uni_control, 1);
        MOQ_TEST_CHECK(!t.bytes_truncated);
        MOQ_TEST_CHECK(t.bytes_len > 0 && t.bytes_len <= sizeof(server_setup));
        memcpy(server_setup, t.bytes, t.bytes_len);
        server_setup_len = t.bytes_len;

        MOQ_TEST_CHECK_EQ_INT(moq_session_start(c, 0), MOQ_OK);
        tally_actions(c, &t);
        MOQ_TEST_CHECK_EQ_INT(t.total, 1);
        MOQ_TEST_CHECK_EQ_INT(t.open_uni_control, 1);
        MOQ_TEST_CHECK(!t.bytes_truncated);
        MOQ_TEST_CHECK_EQ_INT(
            moq_session_on_control_bytes(sv, t.bytes, t.bytes_len, 0), MOQ_OK);

        /* server: exactly one SETUP_COMPLETE and no further output */
        MOQ_TEST_CHECK_EQ_INT(
            count_events_of_kind(sv, MOQ_EVENT_SETUP_COMPLETE, &total), 1);
        MOQ_TEST_CHECK_EQ_INT(total, 1);
        tally_actions(sv, &t);
        MOQ_TEST_CHECK_EQ_INT(t.total, 0);

        MOQ_TEST_CHECK_EQ_INT(
            moq_session_on_control_bytes(c, server_setup, server_setup_len, 0),
            MOQ_OK);
    } else {
        MOQ_TEST_CHECK_EQ_INT(moq_session_start(c, 0), MOQ_OK);

        /* client SETUP: exactly one control message and nothing else */
        tally_actions(c, &t);
        MOQ_TEST_CHECK_EQ_INT(t.total, 1);
        MOQ_TEST_CHECK_EQ_INT(t.send_control, 1);
        MOQ_TEST_CHECK(!t.bytes_truncated);
        MOQ_TEST_CHECK_EQ_INT(
            moq_session_on_control_bytes(sv, t.bytes, t.bytes_len, 0), MOQ_OK);

        /* server: exactly one SETUP_COMPLETE and exactly one reply */
        MOQ_TEST_CHECK_EQ_INT(
            count_events_of_kind(sv, MOQ_EVENT_SETUP_COMPLETE, &total), 1);
        MOQ_TEST_CHECK_EQ_INT(total, 1);
        tally_actions(sv, &t);
        MOQ_TEST_CHECK_EQ_INT(t.total, 1);
        MOQ_TEST_CHECK_EQ_INT(t.send_control, 1);
        MOQ_TEST_CHECK(!t.bytes_truncated);
        MOQ_TEST_CHECK_EQ_INT(
            moq_session_on_control_bytes(c, t.bytes, t.bytes_len, 0), MOQ_OK);
    }

    /* client: exactly one SETUP_COMPLETE, then both queues empty */
    MOQ_TEST_CHECK_EQ_INT(
        count_events_of_kind(c, MOQ_EVENT_SETUP_COMPLETE, &total), 1);
    MOQ_TEST_CHECK_EQ_INT(total, 1);
    tally_actions(c, &t);
    MOQ_TEST_CHECK_EQ_INT(t.total, 0);
    tally_actions(sv, &t);
    MOQ_TEST_CHECK_EQ_INT(t.total, 0);
    (void)count_events_of_kind(sv, 0, &total);
    MOQ_TEST_CHECK_EQ_INT(total, 0);

    MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    *c_out = c;
    *sv_out = sv;
    return failures;
}

/* ------------------------------------------------------------------ */
/* The outstanding request: identity DECLARED from the public API and    */
/* from the fully decoded SUBSCRIBE the session emitted.                 */
/* ------------------------------------------------------------------ */

struct request_ident {
    /* DECLARED BEFORE the call: derived from pool/session state, never from
     * anything the call itself produced. */
    int                slot;
    uint32_t           generation;
    moq_subscription_t handle;
    uint64_t           request_id;
    moq_stream_ref_t   ref;      /* draft-18 request bidi; zero on draft-16 */
    /* the exact bytes the session emitted for this request */
    uint8_t            bytes[512];
    size_t             bytes_len;
};

/*
 * Derive what a correct commit MUST install, before it happens: the first
 * free subscription slot, the generation that slot's commit will move to
 * (`old | 1` -- even is free), the packed handle those imply, this fresh
 * session's first client request id, and (draft-18) the stream ref the next
 * open will take. Nothing here reads a result of the call under test, so a
 * coherent but WRONG slot/handle/ref cannot bless itself.
 */
static int derive_expected_ident(const moq_session_t *s, moq_version_t version,
                                 struct request_ident *id)
{
    int failures = 0;
    size_t i;

    memset(id, 0, sizeof(*id));
    id->slot = -1;
    for (i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) {
            id->slot = (int)i;
            break;
        }
    MOQ_TEST_CHECK(id->slot >= 0);
    if (id->slot < 0)
        return failures;

    id->generation = s->subs[id->slot].generation | 1u;
    id->handle._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                         s->session_tag, id->generation,
                                         (uint32_t)id->slot);
    MOQ_TEST_CHECK(id->handle._opaque != 0);
    /* a fresh client's first request id is 0 in both drafts */
    id->request_id = 0;
    if (version == MOQ_VERSION_DRAFT_18)
        id->ref._v = s->next_stream_ref;
    return failures;
}

/* The exact owner a committed SUBSCRIBE must have armed. */
static int check_sub_armed(const moq_session_t *s,
                           const struct request_ident *id,
                           moq_version_t version, const char *what)
{
    int failures = 0;
    og_graph_t g;
    og_edge_spec_t want[1];

    MOQ_TEST_CHECK(id->slot >= 0 && (size_t)id->slot < s->sub_cap);
    if (id->slot < 0 || (size_t)id->slot >= s->sub_cap)
        return failures;
    MOQ_TEST_CHECK(s->subs[id->slot].state == MOQ_SUB_PENDING_SUBSCRIBER);
    MOQ_TEST_CHECK(s->subs[id->slot].role == MOQ_SUB_ROLE_SUBSCRIBER);
    MOQ_TEST_CHECK(s->subs[id->slot].generation == id->generation);
    MOQ_TEST_CHECK(s->subs[id->slot].handle._opaque == id->handle._opaque);
    MOQ_TEST_CHECK(s->subs[id->slot].request_id == id->request_id);
    MOQ_TEST_CHECK(s->subs[id->slot].request_stream_ref._v == id->ref._v);

    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    if (version == MOQ_VERSION_DRAFT_18) {
        want[0].domain = OG_DOM_REQ_STREAMREF;
        want[0].key = id->ref._v;
    } else {
        want[0].domain = OG_DOM_REQ_RID;
        want[0].key = id->request_id;
    }
    failures += og_check_owner_edges(&g, MOQ_REQ_SUBSCRIPTION, id->slot,
                                     want, 1, what);
    return failures;
}

/* A varint KVP's decoded value, or UINT64_MAX if it is not decodable. */
static uint64_t kvp_varint(const moq_kvp_entry_t *p)
{
    uint64_t v = 0;
    size_t n;

    if (p == NULL || p->value == NULL)
        return UINT64_MAX;
    n = moq_vi64_decode(p->value, p->value_len, &v);
    /* a decoded PREFIX is not a decoded value: trailing bytes must not pass */
    if (n == 0 || n != p->value_len)
        return UINT64_MAX;
    return v;
}

/* Find a decoded KVP by type; returns NULL if absent. */
static const moq_kvp_entry_t *kvp_find(const moq_kvp_entry_t *params,
                                       size_t count, uint64_t type)
{
    size_t i;

    for (i = 0; i < count; i++)
        if (params[i].type == type)
            return &params[i];
    return NULL;
}

static int check_subscribe_bytes_d16(const uint8_t *bytes, size_t len,
                                     uint64_t *out_request_id)
{
    int failures = 0;
    moq_buf_reader_t r;
    moq_control_envelope_t env;
    moq_d16_subscribe_t sub;
    moq_bytes_t parts[4];
    moq_kvp_entry_t params[16];
    const moq_kvp_entry_t *p;

    moq_buf_reader_init(&r, bytes, len);
    MOQ_TEST_CHECK_EQ_INT(moq_control_decode_envelope(&r, &env), MOQ_OK);
    MOQ_TEST_CHECK(env.msg_type == MOQ_D16_SUBSCRIBE);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);

    memset(&sub, 0, sizeof(sub));
    sub.params = params;
    sub.params_cap = 16;   /* bounded before any decode writes into it */
    MOQ_TEST_CHECK_EQ_INT(
        moq_d16_decode_subscribe(env.payload, env.payload_len, parts, 4, &sub),
        MOQ_OK);
    MOQ_TEST_CHECK(sub.params_count <= sub.params_cap);
    if (sub.params_count > sub.params_cap)
        return failures;

    MOQ_TEST_CHECK_EQ_SIZE(sub.track_namespace.count, 1);
    /* diagnose a null parts array rather than dereferencing it */
    MOQ_TEST_CHECK(sub.track_namespace.parts != NULL);
    if (sub.track_namespace.count == 1 &&
        sub.track_namespace.parts != NULL) {
        MOQ_TEST_CHECK_EQ_SIZE(sub.track_namespace.parts[0].len,
                               sizeof(k_ns_bytes));
        MOQ_TEST_CHECK(sub.track_namespace.parts[0].data != NULL);
        if (sub.track_namespace.parts[0].data != NULL)
            MOQ_TEST_CHECK(memcmp(sub.track_namespace.parts[0].data,
                                  k_ns_bytes, sizeof(k_ns_bytes)) == 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(sub.track_name.len, sizeof(k_name_bytes));
    MOQ_TEST_CHECK(sub.track_name.data != NULL);
    if (sub.track_name.data != NULL)
        MOQ_TEST_CHECK(memcmp(sub.track_name.data, k_name_bytes,
                              sizeof(k_name_bytes)) == 0);

    /* every declared parameter, by type, and no unexpected one */
    MOQ_TEST_CHECK_EQ_SIZE(sub.params_count, 6);
    /* priority and group order are fixed single-byte parameters, so they
     * are read with the profile's OWN decoders rather than as varints */
    p = kvp_find(params, sub.params_count, MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY);
    MOQ_TEST_CHECK(p != NULL);
    if (p != NULL) {
        uint8_t v = 0;

        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_param_subscriber_priority(
            p->value, p->value_len, &v), MOQ_OK);
        MOQ_TEST_CHECK(v == SUB_PRIORITY);
    }
    p = kvp_find(params, sub.params_count, MOQ_MSG_PARAM_GROUP_ORDER);
    MOQ_TEST_CHECK(p != NULL);
    if (p != NULL) {
        uint8_t v = 0;

        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_param_group_order(
            p->value, p->value_len, &v), MOQ_OK);
        MOQ_TEST_CHECK(v == SUB_GROUP_ORDER);
    }
    /* the Subscription Filter is a STRUCTURE (§5.1.2), not a bare varint */
    p = kvp_find(params, sub.params_count, MOQ_MSG_PARAM_SUBSCRIPTION_FILTER);
    MOQ_TEST_CHECK(p != NULL);
    if (p != NULL) {
        moq_d16_subscription_filter_t f;

        memset(&f, 0, sizeof(f));
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_subscription_filter(
            p->value, p->value_len, &f), MOQ_OK);
        MOQ_TEST_CHECK(f.filter_type == SUB_FILTER);
        /* LARGEST_OBJECT carries no range: every other field is zero */
        MOQ_TEST_CHECK(f.start_group == 0);
        MOQ_TEST_CHECK(f.start_object == 0);
        MOQ_TEST_CHECK(f.end_group == 0);
    }
    p = kvp_find(params, sub.params_count, MOQ_MSG_PARAM_FORWARD);
    MOQ_TEST_CHECK(p != NULL);
    if (p != NULL) {
        bool v = true;

        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_param_forward(
            p->value, p->value_len, &v), MOQ_OK);
        MOQ_TEST_CHECK(v == SUB_FORWARD);
    }
    p = kvp_find(params, sub.params_count, MOQ_MSG_PARAM_NEW_GROUP_REQUEST);
    MOQ_TEST_CHECK(p != NULL);
    if (p != NULL)
        MOQ_TEST_CHECK(kvp_varint(p) == (uint64_t)(SUB_NEW_GROUP));
    /* the auth token is a Token STRUCTURE: decode it and compare every
     * field, including the alias that USE_VALUE does not use */
    p = kvp_find(params, sub.params_count, MOQ_MSG_PARAM_AUTHORIZATION_TOKEN);
    MOQ_TEST_CHECK(p != NULL);
    if (p != NULL) {
        moq_d16_auth_token_t at;

        memset(&at, 0, sizeof(at));
        MOQ_TEST_CHECK_EQ_INT(moq_d16_auth_token_decode(p->value, p->value_len,
                                                        &at), MOQ_OK);
        MOQ_TEST_CHECK(at.alias_type == MOQ_AUTH_TOKEN_USE_VALUE);
        MOQ_TEST_CHECK(at.token_type == SUB_TOKEN_TYPE);
        MOQ_TEST_CHECK(at.alias == 0);   /* inapplicable to USE_VALUE */
        MOQ_TEST_CHECK_EQ_SIZE(at.token_value_len, sizeof(k_token_bytes));
        MOQ_TEST_CHECK(at.token_value != NULL);
        if (at.token_value != NULL &&
            at.token_value_len == sizeof(k_token_bytes))
            MOQ_TEST_CHECK(memcmp(at.token_value, k_token_bytes,
                                  sizeof(k_token_bytes)) == 0);
    }
    /* ... and a parameter this request deliberately does not set is ABSENT */
    MOQ_TEST_CHECK(kvp_find(params, sub.params_count,
                            MOQ_MSG_PARAM_DELIVERY_TIMEOUT) == NULL);
    MOQ_TEST_CHECK(kvp_find(params, sub.params_count,
                            MOQ_MSG_PARAM_EXPIRES) == NULL);

    *out_request_id = sub.request_id;
    return failures;
}

static int check_subscribe_bytes_d18(const uint8_t *bytes, size_t len,
                                     uint64_t *out_request_id)
{
    int failures = 0;
    moq_buf_reader_t r;
    moq_control_envelope_t env;
    moq_d18_subscribe_t sub;
    moq_bytes_t parts[4];

    moq_buf_reader_init(&r, bytes, len);
    MOQ_TEST_CHECK_EQ_INT(moq_d18_decode_envelope(&r, &env), MOQ_OK);
    MOQ_TEST_CHECK(env.msg_type == MOQ_D18_SUBSCRIBE);
    MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);

    memset(&sub, 0, sizeof(sub));
    MOQ_TEST_CHECK_EQ_INT(
        moq_d18_decode_subscribe(env.payload, env.payload_len, parts, 4, &sub),
        MOQ_OK);
    MOQ_TEST_CHECK(sub.track_namespace.count <= 4);
    if (sub.track_namespace.count > 4)
        return failures;

    MOQ_TEST_CHECK_EQ_SIZE(sub.track_namespace.count, 1);
    /* diagnose a null parts array rather than dereferencing it */
    MOQ_TEST_CHECK(sub.track_namespace.parts != NULL);
    if (sub.track_namespace.count == 1 &&
        sub.track_namespace.parts != NULL) {
        MOQ_TEST_CHECK_EQ_SIZE(sub.track_namespace.parts[0].len,
                               sizeof(k_ns_bytes));
        MOQ_TEST_CHECK(sub.track_namespace.parts[0].data != NULL);
        if (sub.track_namespace.parts[0].data != NULL)
            MOQ_TEST_CHECK(memcmp(sub.track_namespace.parts[0].data,
                                  k_ns_bytes, sizeof(k_ns_bytes)) == 0);
    }
    MOQ_TEST_CHECK_EQ_SIZE(sub.track_name.len, sizeof(k_name_bytes));
    MOQ_TEST_CHECK(sub.track_name.data != NULL);
    if (sub.track_name.data != NULL)
        MOQ_TEST_CHECK(memcmp(sub.track_name.data, k_name_bytes,
                              sizeof(k_name_bytes)) == 0);

    /* every declared parameter present with its declared value ... */
    MOQ_TEST_CHECK(sub.params.has_subscriber_priority);
    MOQ_TEST_CHECK(sub.params.subscriber_priority == SUB_PRIORITY);
    MOQ_TEST_CHECK(sub.params.has_group_order);
    MOQ_TEST_CHECK(sub.params.group_order == SUB_GROUP_ORDER);
    MOQ_TEST_CHECK(sub.params.has_filter);
    MOQ_TEST_CHECK(sub.params.filter_type == SUB_FILTER);
    MOQ_TEST_CHECK(sub.params.has_forward);
    MOQ_TEST_CHECK(sub.params.forward == (SUB_FORWARD ? 1 : 0));
    MOQ_TEST_CHECK(sub.params.has_new_group_request);
    MOQ_TEST_CHECK(sub.params.new_group_request == SUB_NEW_GROUP);
    MOQ_TEST_CHECK_EQ_SIZE(sub.params.auth_token_count, 1);
    if (sub.params.auth_token_count == 1) {
        const moq_d18_auth_token_t *at = &sub.params.auth_tokens[0];

        MOQ_TEST_CHECK(at->alias_type == MOQ_AUTH_TOKEN_USE_VALUE);
        MOQ_TEST_CHECK(at->token_type == SUB_TOKEN_TYPE);
        MOQ_TEST_CHECK(at->alias == 0);   /* inapplicable to USE_VALUE */
        MOQ_TEST_CHECK_EQ_SIZE(at->token_value.len, sizeof(k_token_bytes));
        MOQ_TEST_CHECK(at->token_value.data != NULL);
        if (at->token_value.data != NULL)
            MOQ_TEST_CHECK(memcmp(at->token_value.data, k_token_bytes,
                                  sizeof(k_token_bytes)) == 0);
    }
    /* ... and every field this request deliberately does not set is ABSENT */
    MOQ_TEST_CHECK(!sub.params.has_expires);
    MOQ_TEST_CHECK(!sub.params.has_largest);
    MOQ_TEST_CHECK(!sub.params.has_object_delivery_timeout);
    MOQ_TEST_CHECK(!sub.params.has_subgroup_delivery_timeout);

    *out_request_id = sub.request_id;
    return failures;
}

static int issue_subscribe(moq_session_t *c, moq_version_t version,
                           struct request_ident *id)
{
    int failures = 0;
    moq_subscribe_cfg_t cfg;
    moq_auth_token_t tok;
    struct act_tally t;
    struct request_ident got;
    int total = 0;

    /* declared BEFORE the call */
    failures += derive_expected_ident(c, version, id);
    if (id->slot < 0)
        return failures;

    tok.token_type = SUB_TOKEN_TYPE;
    tok.token_value = (moq_bytes_t){ k_token_bytes, sizeof(k_token_bytes) };

    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ (moq_bytes_t *)k_ns, 1 };
    cfg.track_name = (moq_bytes_t){ k_name_bytes, sizeof(k_name_bytes) };
    cfg.filter = SUB_FILTER;
    cfg.has_subscriber_priority = true;
    cfg.subscriber_priority = SUB_PRIORITY;
    cfg.group_order = SUB_GROUP_ORDER;
    cfg.has_forward = true;
    cfg.forward = SUB_FORWARD;
    cfg.has_new_group_request = true;
    cfg.new_group_request = SUB_NEW_GROUP;
    cfg.auth_tokens = &tok;
    cfg.auth_token_count = 1;

    memset(&got, 0, sizeof(got));
    MOQ_TEST_CHECK_EQ_INT(moq_session_subscribe(c, &cfg, 1, &got.handle),
                          MOQ_OK);
    MOQ_TEST_CHECK(moq_subscription_is_valid(got.handle));
    /* the returned handle must equal the one derived before the call */
    MOQ_TEST_CHECK(got.handle._opaque == id->handle._opaque);

    tally_actions(c, &t);
    MOQ_TEST_CHECK(!t.bytes_truncated);
    if (version == MOQ_VERSION_DRAFT_18) {
        /* exactly one request bidi carrying exactly this request, no FIN
         * (the subscription's send half stays open for a later update) */
        MOQ_TEST_CHECK_EQ_INT(t.total, 1);
        MOQ_TEST_CHECK_EQ_INT(t.open_bidi, 1);
        MOQ_TEST_CHECK_EQ_INT(t.send_bidi, 0);
        MOQ_TEST_CHECK_EQ_INT(t.send_control, 0);
        MOQ_TEST_CHECK_EQ_INT(t.other, 0);
        MOQ_TEST_CHECK(!t.bytes_fin);
        /* the opened ref must be the one derived before the call */
        MOQ_TEST_CHECK(t.open_ref._v == id->ref._v);
        MOQ_TEST_CHECK(t.bytes_ref._v == id->ref._v);
        failures += check_subscribe_bytes_d18(t.bytes, t.bytes_len,
                                              &got.request_id);
    } else {
        /* exactly one control message, no stream activity */
        MOQ_TEST_CHECK_EQ_INT(t.total, 1);
        MOQ_TEST_CHECK_EQ_INT(t.send_control, 1);
        MOQ_TEST_CHECK_EQ_INT(t.open_bidi, 0);
        MOQ_TEST_CHECK_EQ_INT(t.other, 0);
        failures += check_subscribe_bytes_d16(t.bytes, t.bytes_len,
                                              &got.request_id);
    }
    /* the decoded request id must equal the one declared before the call */
    MOQ_TEST_CHECK(got.request_id == id->request_id);
    memcpy(id->bytes, t.bytes, t.bytes_len);
    id->bytes_len = t.bytes_len;

    /* issuing a request owes no event */
    (void)count_events_of_kind(c, 0, &total);
    MOQ_TEST_CHECK_EQ_INT(total, 0);

    /* and the physical owner must be exactly what was declared */
    failures += check_sub_armed(c, id, version, "armed subscription");
    return failures;
}

/* ------------------------------------------------------------------ */
/* Inbound observation                                                  */
/* ------------------------------------------------------------------ */

struct inbound_obs {
    int                 events;
    int                 sub_errors;
    int                 redirects;    /* MOQ_EVENT_REQUEST_REDIRECT */
    moq_request_error_t redirect_code;
    moq_request_error_t code;
    moq_subscription_t  sub;
    bool                can_retry;
    uint64_t            retry_after_ms;
    uint8_t             reason[64];   /* DEEP COPY, taken before cleanup */
    size_t              reason_len;
    moq_result_t        feed_rc;
    moq_session_state_t state;
    struct act_tally    acts;
};

static void collect_error_event(moq_session_t *s, struct inbound_obs *o)
{
    moq_event_t ev;

    while (moq_session_poll_events(s, &ev, 1) > 0) {
        o->events++;
        if (ev.kind == MOQ_EVENT_REQUEST_REDIRECT) {
            o->redirects++;
            o->redirect_code = ev.u.request_redirect.error_code;
        }
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
            const moq_subscribe_error_event_t *e = &ev.u.subscribe_error;

            o->sub_errors++;
            o->code = e->error_code;
            o->sub = e->sub;
            o->can_retry = e->can_retry;
            o->retry_after_ms = e->retry_after_ms;
            if (e->reason.len <= sizeof(o->reason) && e->reason.data != NULL) {
                memcpy(o->reason, e->reason.data, e->reason.len);
                o->reason_len = e->reason.len;
            } else {
                o->reason_len = SIZE_MAX;   /* unrepresentable: fails below */
            }
        }
        moq_event_cleanup(&ev);
    }
}

/* The shape every inbound run must satisfy, whatever the code turns out to
 * be. Correlation is against the handle the PUBLIC API returned, and the
 * reason is compared byte for byte with the independently declared value. */
static int check_error_shape(const struct inbound_obs *o,
                             const struct request_ident *id,
                             uint64_t exp_retry_ms)
{
    int failures = 0;

    MOQ_TEST_CHECK_EQ_INT(o->feed_rc, MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(o->sub_errors, 1);
    MOQ_TEST_CHECK_EQ_INT(o->events, 1);
    /* explicit, not merely implied by the two counts above */
    MOQ_TEST_CHECK_EQ_INT(o->redirects, 0);
    MOQ_TEST_CHECK(o->sub._opaque == id->handle._opaque);
    MOQ_TEST_CHECK(o->can_retry);
    MOQ_TEST_CHECK(o->retry_after_ms == exp_retry_ms);
    MOQ_TEST_CHECK_EQ_SIZE(o->reason_len, sizeof(k_reason_bytes));
    if (o->reason_len == sizeof(k_reason_bytes))
        MOQ_TEST_CHECK(memcmp(o->reason, k_reason_bytes,
                              sizeof(k_reason_bytes)) == 0);
    MOQ_TEST_CHECK(o->state == MOQ_SESS_ESTABLISHED);
    return failures;
}

/* Exact retirement: free slot, advanced generation, stale handle, no edge. */
/*
 * Structural retirement: pool slot, generation, and index topology. Takes no
 * advancing call, so it never moves the session clock.
 */
static int check_sub_retired(moq_session_t *s, const struct request_ident *id,
                             const char *what)
{
    int failures = 0;
    og_graph_t g;

    MOQ_TEST_CHECK(id->slot >= 0 && (size_t)id->slot < s->sub_cap);
    if (id->slot < 0 || (size_t)id->slot >= s->sub_cap)
        return failures;
    MOQ_TEST_CHECK(s->subs[id->slot].state == MOQ_SUB_FREE);
    /* the free generation is EXACTLY the live one plus one in this fixture:
     * odd is live, even is free, and the slot is used exactly once here */
    MOQ_TEST_CHECK(s->subs[id->slot].generation == id->generation + 1u);

    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    failures += og_check_no_edge(&g, OG_DOM_REQ_RID, id->request_id, what);
    if (id->ref._v != 0)
        failures += og_check_no_edge(&g, OG_DOM_REQ_STREAMREF, id->ref._v,
                                     what);
    /* and nothing under ANY other key still points at the freed slot */
    failures += og_check_owner_unreferenced(&g, MOQ_REQ_SUBSCRIPTION,
                                            id->slot, what);
    return failures;
}

/*
 * The public stale-handle probe is an ADVANCING call, so it is separated from
 * the structural check above and takes an explicit epoch the caller keeps
 * monotonically increasing. `session_sweep_arm()` stamps `last_now_us`, so a
 * probe at a stale epoch would step the session clock backward.
 */
static int check_handle_stale(moq_session_t *s,
                              const struct request_ident *id, uint64_t now_us)
{
    int failures = 0;

    MOQ_TEST_CHECK_EQ_INT(moq_session_unsubscribe(s, id->handle, now_us),
                          MOQ_ERR_STALE_HANDLE);
    return failures;
}

/* ------------------------------------------------------------------ */
/* draft-16: control-stream response, immediate free                    */
/* ------------------------------------------------------------------ */

static int run_inbound_d16(uint64_t wire_code, struct inbound_obs *o)
{
    int failures = 0;
    moq_session_t *c = NULL, *sv = NULL;
    struct request_ident id;
    uint8_t msg[128];
    moq_buf_writer_t w;

    memset(o, 0, sizeof(*o));
    failures += make_pair(MOQ_VERSION_DRAFT_16, &c, &sv);
    if (c == NULL || sv == NULL)
        return failures;
    failures += issue_subscribe(c, MOQ_VERSION_DRAFT_16, &id);

    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT(moq_d16_encode_request_error(
        &w, id.request_id, wire_code, RETRY_WIRE,
        k_reason_bytes, sizeof(k_reason_bytes)), MOQ_OK);

    o->feed_rc = moq_session_on_control_bytes(c, msg,
                                              moq_buf_writer_offset(&w), 1000);
    collect_error_event(c, o);
    tally_actions(c, &o->acts);
    o->state = moq_session_state(c);

    failures += check_error_shape(o, &id, RETRY_WIRE - 1);
    MOQ_TEST_CHECK_EQ_INT(o->acts.total, 0);   /* no output on this route */
    /* draft-16 answers on the control stream: there is no request stream to
     * drain, so the ring must be untouched */
    MOQ_TEST_CHECK_EQ_SIZE(c->drain_ref_count, 0);
    /* draft-16 frees immediately (free_now) */
    failures += check_sub_retired(c, &id, "d16 after REQUEST_ERROR");
    failures += check_handle_stale(c, &id, 2000);   /* > the 1000 feed */

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

/* ------------------------------------------------------------------ */
/* draft-18: request bidi, draining owner, then FIN retirement          */
/* ------------------------------------------------------------------ */

static int run_inbound_d18(uint64_t wire_code, struct inbound_obs *o)
{
    int failures = 0;
    moq_session_t *c = NULL, *sv = NULL;
    struct request_ident id;
    uint8_t msg[128];
    moq_buf_writer_t w;
    og_graph_t g;
    int total = 0;

    memset(o, 0, sizeof(*o));
    failures += make_pair(MOQ_VERSION_DRAFT_18, &c, &sv);
    if (c == NULL || sv == NULL)
        return failures;
    failures += issue_subscribe(c, MOQ_VERSION_DRAFT_18, &id);

    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT(moq_d18_encode_request_error(
        &w, wire_code, RETRY_WIRE,
        (moq_bytes_t){ k_reason_bytes, sizeof(k_reason_bytes) }), MOQ_OK);

    /* Phase 1: the terminal response WITHOUT the trailing FIN. */
    o->feed_rc = moq_session_on_bidi_stream_bytes(c, id.ref, msg,
                                                  moq_buf_writer_offset(&w),
                                                  false, 1000);
    collect_error_event(c, o);
    tally_actions(c, &o->acts);
    o->state = moq_session_state(c);

    failures += check_error_shape(o, &id, RETRY_WIRE);
    /* our own send half closes, on the EXACT predeclared request ref, and
     * nothing else is emitted */
    MOQ_TEST_CHECK_EQ_INT(o->acts.total, 1);
    MOQ_TEST_CHECK_EQ_INT(o->acts.close_bidi, 1);
    MOQ_TEST_CHECK(o->acts.close_ref._v == id.ref._v);
    /* the inbound terminal retains the owner itself; it owes NO drain-ring
     * entry, because the entry is what absorbs the trailing FIN */
    MOQ_TEST_CHECK_EQ_SIZE(c->drain_ref_count, 0);

    /* the owner is DRAINING, not retired: exact live state and edge set */
    MOQ_TEST_CHECK(id.slot >= 0 && (size_t)id.slot < c->sub_cap);
    if (id.slot >= 0 && (size_t)id.slot < c->sub_cap) {
        MOQ_TEST_CHECK(c->subs[id.slot].state == MOQ_SUB_TERMINATED);
        MOQ_TEST_CHECK(c->subs[id.slot].generation == id.generation);
        MOQ_TEST_CHECK(c->subs[id.slot].handle._opaque == id.handle._opaque);
        MOQ_TEST_CHECK(c->subs[id.slot].request_stream_ref._v == id.ref._v);
    }
    {
        const og_edge_spec_t want[] = {
            { OG_DOM_REQ_STREAMREF, id.ref._v },
        };

        og_capture(c, &g);
        failures += og_check_integrity(&g, "d18 draining");
        failures += og_check_owner_edges(&g, MOQ_REQ_SUBSCRIPTION, id.slot,
                                         want, 1, "d18 draining");
    }

    /* Phase 2: the trailing empty FIN retires it. */
    MOQ_TEST_CHECK_EQ_INT(
        moq_session_on_bidi_stream_bytes(c, id.ref, NULL, 0, true, 2000),
        MOQ_OK);
    (void)count_events_of_kind(c, 0, &total);
    MOQ_TEST_CHECK_EQ_INT(total, 0);           /* no event on the FIN */
    {
        struct act_tally t2;

        tally_actions(c, &t2);
        MOQ_TEST_CHECK_EQ_INT(t2.total, 0);    /* and no output */
    }
    MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
    failures += check_sub_retired(c, &id, "d18 after FIN");
    failures += check_handle_stale(c, &id, 3000);   /* > the 2000 FIN */

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

/*
 * Positive control for the redirect counter: a GENUINE draft-18
 * REQUEST_ERROR(REDIRECT) with a Redirect tail must surface exactly one
 * MOQ_EVENT_REQUEST_REDIRECT and no SUBSCRIBE_ERROR. Without this, the
 * wide-REDIRECT cells' `redirects == 0` could be a counter that never
 * observes anything rather than a fact about the product.
 */
static int redirect_control_d18(void)
{
    int failures = 0;
    moq_session_t *c = NULL, *sv = NULL;
    struct request_ident id;
    struct inbound_obs o;
    uint8_t msg[256];
    moq_buf_writer_t w;
    moq_d18_redirect_t rd;
    moq_bytes_t parts[1];

    memset(&o, 0, sizeof(o));
    failures += make_pair(MOQ_VERSION_DRAFT_18, &c, &sv);
    if (c == NULL || sv == NULL)
        return failures;
    failures += issue_subscribe(c, MOQ_VERSION_DRAFT_18, &id);

    memset(&rd, 0, sizeof(rd));
    parts[0] = (moq_bytes_t){ k_ns_bytes, sizeof(k_ns_bytes) };
    rd.track_namespace = (moq_namespace_t){ parts, 1 };
    rd.track_name = (moq_bytes_t){ k_name_bytes, sizeof(k_name_bytes) };

    moq_buf_writer_init(&w, msg, sizeof(msg));
    MOQ_TEST_CHECK_EQ_INT(moq_d18_encode_request_error_redirect(
        &w, MOQ_REQUEST_ERROR_REDIRECT, RETRY_WIRE,
        (moq_bytes_t){ k_reason_bytes, sizeof(k_reason_bytes) }, &rd), MOQ_OK);

    MOQ_TEST_CHECK_EQ_INT(
        moq_session_on_bidi_stream_bytes(c, id.ref, msg,
                                         moq_buf_writer_offset(&w), false,
                                         1000), MOQ_OK);
    collect_error_event(c, &o);
    /* the counter really does observe redirects ... */
    MOQ_TEST_CHECK_EQ_INT(o.redirects, 1);
    /* ... and REDIRECT is the 18th registered draft-18 code, so the value it
     * surfaces is checked here rather than in the sweep (0x34 with no
     * Redirect tail is malformed and cannot be swept) */
    MOQ_TEST_CHECK(o.redirect_code == MOQ_REQUEST_ERROR_REDIRECT);
    MOQ_TEST_CHECK_EQ_INT(o.sub_errors, 0);
    MOQ_TEST_CHECK_EQ_INT(o.events, 1);
    MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

/* ------------------------------------------------------------------ */
/* Pre-advance conservation                                             */
/* ------------------------------------------------------------------ */

/*
 * The DECLARED pre-advance conservation inventory -- not a claim to cover
 * every field `session_begin_advance()` can reach. A public call that
 * refuses an unrepresentable argument must refuse BEFORE the advance
 * preamble runs, so none of the fields below may change; that is a stronger
 * statement than "the encoder eventually returned an error", because by then
 * logical time has moved, borrows are invalid, send/scratch state has been
 * reclaimed and a deferred sweep may have run.
 *
 * The inventory is: logical time, the borrow epoch, the selected sweep cursor
 * fields (active / stage / slot / phase / rx position / the epoch the sweep
 * began at), the three scratch-and-send cursors, and the two queue depths.
 *
 * Deliberately outside it: deadlines a COMPLETE sweep can recompute --
 * `expiry_retry_deadline_us`, `subgroup_deadline_us`, `idle_deadline_us`.
 * They are consequences of sweep work rather than of the preamble itself,
 * and the sweep-cursor fields above already fail if a sweep ran here.
 *
 * Deliberately NOT `txs_snapshot_t`: that models owner/queue topology and
 * omits every advance field named above.
 */
struct advance_snap {
    uint64_t          last_now_us;
    uint64_t          borrow_epoch;
    bool              sweep_active;
    int               sweep_stage;
    size_t            sweep_slot;
    int               sweep_phase;
    size_t            sweep_rx_pos;
    uint64_t          sweep_now_us;
    size_t            send_len;
    size_t            output_scratch_len;
    size_t            event_scratch_len;
    size_t            action_depth;
    size_t            event_depth;
};

static void advance_snap_take(const moq_session_t *s, struct advance_snap *a)
{
    memset(a, 0, sizeof(*a));
    a->last_now_us        = s->last_now_us;
    a->borrow_epoch       = s->borrow_epoch;
    a->sweep_active       = s->sweep_active;
    a->sweep_stage        = (int)s->sweep_stage;
    a->sweep_slot         = s->sweep_slot;
    a->sweep_phase        = (int)s->sweep_phase;
    a->sweep_rx_pos       = s->sweep_rx_pos;
    a->sweep_now_us       = s->sweep_now_us;
    a->send_len           = s->send_len;
    a->output_scratch_len = s->output_scratch_len;
    a->event_scratch_len  = s->event_scratch_len;
    a->action_depth       = s->action_tail - s->action_head;
    a->event_depth        = s->event_tail - s->event_head;
}

static int advance_snap_unchanged(const struct advance_snap *before,
                                  const moq_session_t *s, const char *what)
{
    int failures = 0;
    struct advance_snap now;

    (void)what;
    advance_snap_take(s, &now);
    MOQ_TEST_CHECK(now.last_now_us == before->last_now_us);
    MOQ_TEST_CHECK(now.borrow_epoch == before->borrow_epoch);
    MOQ_TEST_CHECK(now.sweep_active == before->sweep_active);
    MOQ_TEST_CHECK(now.sweep_stage == before->sweep_stage);
    MOQ_TEST_CHECK(now.sweep_slot == before->sweep_slot);
    MOQ_TEST_CHECK(now.sweep_phase == before->sweep_phase);
    MOQ_TEST_CHECK_EQ_SIZE(now.sweep_rx_pos, before->sweep_rx_pos);
    MOQ_TEST_CHECK(now.sweep_now_us == before->sweep_now_us);
    MOQ_TEST_CHECK_EQ_SIZE(now.send_len, before->send_len);
    MOQ_TEST_CHECK_EQ_SIZE(now.output_scratch_len, before->output_scratch_len);
    MOQ_TEST_CHECK_EQ_SIZE(now.event_scratch_len, before->event_scratch_len);
    MOQ_TEST_CHECK_EQ_SIZE(now.action_depth, before->action_depth);
    MOQ_TEST_CHECK_EQ_SIZE(now.event_depth, before->event_depth);
    return failures;
}

/* ------------------------------------------------------------------ */
/* Outbound                                                             */
/* ------------------------------------------------------------------ */

/*
 * All EIGHT reject/cancel configs carry `moq_request_error_t`, so each must
 * be able to REPRESENT the full domain: eight bytes, and a widened value
 * assigned into the field must round-trip. That is a width statement only --
 * whether a given code is ACCEPTED also depends on the profile's wire
 * ceiling (see the refusal cells) and on pre-existing family rules such as
 * PUBLISH and SUBSCRIBE_TRACKS refusing REDIRECT, which this slice does not
 * change.
 */
static int outbound_public_width_is_64_bit(void)
{
    int failures = 0;
    moq_reject_subscribe_cfg_t        c1;
    moq_reject_fetch_cfg_t            c2;
    moq_reject_publish_cfg_t          c3;
    moq_reject_namespace_cfg_t        c4;
    moq_cancel_namespace_cfg_t        c5;
    moq_reject_track_status_cfg_t     c6;
    moq_reject_ns_sub_cfg_t           c7;
    moq_reject_subscribe_tracks_cfg_t c8;

    MOQ_TEST_CHECK_EQ_SIZE(sizeof(moq_request_error_t), 8);
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(c1.error_code), 8);
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(c2.error_code), 8);
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(c3.error_code), 8);
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(c4.error_code), 8);
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(c5.error_code), 8);
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(c6.error_code), 8);
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(c7.error_code), 8);
    MOQ_TEST_CHECK_EQ_SIZE(sizeof(c8.error_code), 8);

    /*
     * and each field really CARRIES a widened value, not merely occupies
     * eight bytes: assignment must round-trip without truncation. The source
     * is volatile so a narrowed field truncates at RUNTIME rather than
     * tripping a compile-time constant-conversion diagnostic -- a build
     * failure is void evidence under the mutant rule, so the oracle has to be
     * able to observe the truncation instead.
     */
    volatile uint64_t wide = WIDE_UNKNOWN_CODE;

    c1.error_code = (moq_request_error_t)wide;
    c2.error_code = (moq_request_error_t)wide;
    c3.error_code = (moq_request_error_t)wide;
    c4.error_code = (moq_request_error_t)wide;
    c5.error_code = (moq_request_error_t)wide;
    c6.error_code = (moq_request_error_t)wide;
    c7.error_code = (moq_request_error_t)wide;
    c8.error_code = (moq_request_error_t)wide;
    MOQ_TEST_CHECK((uint64_t)c1.error_code == (uint64_t)wide);
    MOQ_TEST_CHECK((uint64_t)c2.error_code == (uint64_t)wide);
    MOQ_TEST_CHECK((uint64_t)c3.error_code == (uint64_t)wide);
    MOQ_TEST_CHECK((uint64_t)c4.error_code == (uint64_t)wide);
    MOQ_TEST_CHECK((uint64_t)c5.error_code == (uint64_t)wide);
    MOQ_TEST_CHECK((uint64_t)c6.error_code == (uint64_t)wide);
    MOQ_TEST_CHECK((uint64_t)c7.error_code == (uint64_t)wide);
    MOQ_TEST_CHECK((uint64_t)c8.error_code == (uint64_t)wide);
    MOQ_TEST_CHECK(WIDE_UNKNOWN_CODE > (uint64_t)UINT32_MAX);
    return failures;
}

/* The exact owner an inbound SUBSCRIBE must have armed on the responder. */
static int check_inbound_sub_armed(const moq_session_t *s,
                                   const struct request_ident *id,
                                   moq_version_t version, const char *what)
{
    int failures = 0;
    og_graph_t g;
    og_edge_spec_t want[1];

    MOQ_TEST_CHECK(id->slot >= 0 && (size_t)id->slot < s->sub_cap);
    if (id->slot < 0 || (size_t)id->slot >= s->sub_cap)
        return failures;
    MOQ_TEST_CHECK(s->subs[id->slot].state == MOQ_SUB_PENDING_PUBLISHER);
    MOQ_TEST_CHECK(s->subs[id->slot].role == MOQ_SUB_ROLE_PUBLISHER);
    MOQ_TEST_CHECK(s->subs[id->slot].generation == id->generation);
    MOQ_TEST_CHECK(s->subs[id->slot].handle._opaque == id->handle._opaque);
    MOQ_TEST_CHECK(s->subs[id->slot].request_id == id->request_id);
    MOQ_TEST_CHECK(s->subs[id->slot].request_stream_ref._v == id->ref._v);

    og_capture(s, &g);
    failures += og_check_integrity(&g, what);
    if (version == MOQ_VERSION_DRAFT_18) {
        want[0].domain = OG_DOM_REQ_STREAMREF;
        want[0].key = id->ref._v;
    } else {
        want[0].domain = OG_DOM_REQ_RID;
        want[0].key = id->request_id;
    }
    failures += og_check_owner_edges(&g, MOQ_REQ_SUBSCRIPTION, id->slot,
                                     want, 1, what);
    return failures;
}

/*
 * The real outbound path per draft: a real inbound request armed on the
 * responder, a real public reject, and the exact wire output, owner
 * retirement, registry topology and drain lifecycle that follow.
 */
static int outbound_reject_code(moq_version_t version, uint64_t send_code)
{
    int failures = 0;
    moq_session_t *c = NULL, *sv = NULL;
    struct request_ident id;       /* the requester's own identity */
    struct request_ident srv;      /* the RESPONDER's destination owner */
    moq_subscription_t inbound = MOQ_SUBSCRIPTION_INVALID;
    moq_event_t ev;
    struct act_tally t;
    int total = 0, got = 0;
    moq_reject_subscribe_cfg_t rcfg;

    failures += make_pair(version, &c, &sv);
    if (c == NULL || sv == NULL)
        return failures;
    failures += issue_subscribe(c, version, &id);

    /* declare the responder's destination owner BEFORE the request lands */
    failures += derive_expected_ident(sv, version, &srv);
    if (srv.slot < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return failures;
    }
    /* the peer-correlated identity is the requester's, replayed verbatim */
    srv.request_id = id.request_id;
    srv.ref = id.ref;

    /* replay the exact bytes the client emitted */
    if (version == MOQ_VERSION_DRAFT_18)
        MOQ_TEST_CHECK_EQ_INT(
            moq_session_on_bidi_stream_bytes(sv, id.ref, id.bytes,
                                             id.bytes_len, false, 0), MOQ_OK);
    else
        MOQ_TEST_CHECK_EQ_INT(
            moq_session_on_control_bytes(sv, id.bytes, id.bytes_len, 0),
            MOQ_OK);

    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        total++;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            inbound = ev.u.subscribe_request.sub;
            got++;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(got, 1);
    MOQ_TEST_CHECK_EQ_INT(total, 1);
    if (got != 1) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return failures;
    }
    /* the surfaced handle must be the one derived before ingress */
    MOQ_TEST_CHECK(inbound._opaque == srv.handle._opaque);
    failures += check_inbound_sub_armed(sv, &srv, version, "inbound armed");
    MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 0);

    moq_reject_subscribe_cfg_init(&rcfg);
    rcfg.error_code = (moq_request_error_t)send_code;
    rcfg.reason = (moq_bytes_t){ k_reason_bytes, sizeof(k_reason_bytes) };
    rcfg.can_retry = true;
    rcfg.retry_after_ms = OUT_RETRY_MS;
    MOQ_TEST_CHECK_EQ_INT(moq_session_reject_subscribe(sv, inbound, &rcfg, 0),
                          MOQ_OK);

    tally_actions(sv, &t);
    MOQ_TEST_CHECK(!t.bytes_truncated);
    MOQ_TEST_CHECK_EQ_INT(t.total, 1);        /* exactly one output record */
    if (t.bytes_len == 0) {
        /* no output to decode: report it rather than reading an empty buffer */
        MOQ_TEST_CHECK(0 && "reject produced no wire bytes");
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return failures;
    }
    if (version == MOQ_VERSION_DRAFT_18) {
        moq_buf_reader_t r;
        moq_control_envelope_t env;
        moq_d18_request_error_t err;

        MOQ_TEST_CHECK_EQ_INT(t.send_bidi, 1);
        MOQ_TEST_CHECK(t.bytes_ref._v == srv.ref._v);  /* the exact stream */
        MOQ_TEST_CHECK(t.bytes_fin);                   /* terminal: FIN set */

        moq_buf_reader_init(&r, t.bytes, t.bytes_len);
        MOQ_TEST_CHECK_EQ_INT(moq_d18_decode_envelope(&r, &env), MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D18_REQUEST_ERROR);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);
        memset(&err, 0, sizeof(err));
        MOQ_TEST_CHECK_EQ_INT(
            moq_d18_decode_request_error(env.payload, env.payload_len, &err),
            MOQ_OK);
        MOQ_TEST_CHECK(err.error_code == send_code);   /* exact, full width */
        /* d18 emits the ms value unchanged where d16 applies the spec's
         * "plus one"; pinned per draft, see the retry note in main() */
        MOQ_TEST_CHECK(err.retry_interval == OUT_RETRY_MS);
        MOQ_TEST_CHECK_EQ_SIZE(err.reason.len, sizeof(k_reason_bytes));
        MOQ_TEST_CHECK(err.reason.data != NULL);
        if (err.reason.data != NULL)
            MOQ_TEST_CHECK(memcmp(err.reason.data, k_reason_bytes,
                                  sizeof(k_reason_bytes)) == 0);
    } else {
        moq_buf_reader_t r;
        moq_control_envelope_t env;
        moq_d16_request_error_t err;

        MOQ_TEST_CHECK_EQ_INT(t.send_control, 1);

        moq_buf_reader_init(&r, t.bytes, t.bytes_len);
        MOQ_TEST_CHECK_EQ_INT(moq_control_decode_envelope(&r, &env), MOQ_OK);
        MOQ_TEST_CHECK(env.msg_type == MOQ_D16_REQUEST_ERROR);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);
        memset(&err, 0, sizeof(err));
        MOQ_TEST_CHECK_EQ_INT(
            moq_d16_decode_request_error(env.payload, env.payload_len, &err),
            MOQ_OK);
        MOQ_TEST_CHECK(err.request_id == srv.request_id);
        MOQ_TEST_CHECK(err.error_code == send_code);   /* exact, full width */
        MOQ_TEST_CHECK(err.retry_interval == OUT_RETRY_MS + 1);
        MOQ_TEST_CHECK_EQ_SIZE(err.reason_len, sizeof(k_reason_bytes));
        MOQ_TEST_CHECK(err.reason != NULL);
        if (err.reason != NULL)
            MOQ_TEST_CHECK(memcmp(err.reason, k_reason_bytes,
                                  sizeof(k_reason_bytes)) == 0);
    }
    /* rejecting owes the rejecting side no event */
    (void)count_events_of_kind(sv, 0, &total);
    MOQ_TEST_CHECK_EQ_INT(total, 0);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

    /* the destination owner is retired, whichever draft */
    failures += check_sub_retired(sv, &srv, "outbound after reject");
    failures += check_handle_stale(sv, &srv, 1000); /* > the 0 reject */

    /*
     * The drain lifecycle differs, and it is the responder's own: draft-16
     * answered on the control stream and has no request stream to drain,
     * while draft-18 sent a terminal FIN on the request bidi whose PEER send
     * half is still open, so exactly one drain reference is owed for it.
     */
    if (version == MOQ_VERSION_DRAFT_18) {
        MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 1);
        MOQ_TEST_CHECK(drain_ref_contains(sv, srv.ref));
        /* membership and count say nothing about WHY it drains */
        MOQ_TEST_CHECK(drain_ref_reason(sv, srv.ref) == MOQ_DRAIN_NORMAL);

        /* the peer's late empty FIN releases it and nothing else happens */
        MOQ_TEST_CHECK_EQ_INT(
            moq_session_on_bidi_stream_bytes(sv, srv.ref, NULL, 0, true, 5000),
            MOQ_OK);
        (void)count_events_of_kind(sv, 0, &total);
        MOQ_TEST_CHECK_EQ_INT(total, 0);
        tally_actions(sv, &t);
        MOQ_TEST_CHECK_EQ_INT(t.total, 0);
        MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 0);
        MOQ_TEST_CHECK(!drain_ref_contains(sv, srv.ref));
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        /* still retired, and still nothing pointing at the freed slot */
        failures += check_sub_retired(sv, &srv, "outbound after peer FIN");
        failures += check_handle_stale(sv, &srv, 6000); /* > the 5000 FIN */
    } else {
        MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 0);
    }

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

static int outbound_reject(moq_version_t version)
{
    return outbound_reject_code(version, MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
}

/*
 * A code this profile CANNOT put on the wire must be refused before any
 * mutation: no output, no owner retirement, no registry change, no drain,
 * no session state change. Draft-16's ceiling is the QUIC varint's 2^62-1;
 * draft-18 has no such ceiling below UINT64_MAX, so this cell is draft-16's.
 */
static int outbound_refuses_unrepresentable_d16(void)
{
    int failures = 0;
    moq_session_t *c = NULL, *sv = NULL;
    struct request_ident id, srv;
    moq_subscription_t inbound = MOQ_SUBSCRIPTION_INVALID;
    moq_event_t ev;
    struct act_tally t;
    int total = 0, got = 0;
    moq_reject_subscribe_cfg_t rcfg;
    og_graph_t before, after;
    struct advance_snap pre;
    enum { REFUSE_EPOCH = 4000, RETRY_EPOCH = 5000 };

    failures += make_pair(MOQ_VERSION_DRAFT_16, &c, &sv);
    if (c == NULL || sv == NULL)
        return failures;
    failures += issue_subscribe(c, MOQ_VERSION_DRAFT_16, &id);
    failures += derive_expected_ident(sv, MOQ_VERSION_DRAFT_16, &srv);
    if (srv.slot < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return failures;
    }
    srv.request_id = id.request_id;
    srv.ref = id.ref;

    MOQ_TEST_CHECK_EQ_INT(
        moq_session_on_control_bytes(sv, id.bytes, id.bytes_len, 0), MOQ_OK);
    while (moq_session_poll_events(sv, &ev, 1) > 0) {
        total++;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            inbound = ev.u.subscribe_request.sub;
            got++;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(got, 1);
    if (got != 1) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return failures;
    }
    failures += check_inbound_sub_armed(sv, &srv, MOQ_VERSION_DRAFT_16,
                                        "inbound armed (refusal)");
    og_capture(sv, &before);

    /* Everything above happened at epoch 0. The invalid call is made at a
     * STRICTLY LATER epoch, so `session_begin_advance()` running would move
     * `last_now_us` (and bump the borrow epoch, and reclaim scratch, and
     * possibly run a deferred sweep) -- which is what makes the refusal
     * PRE-ADVANCE observable at all. */
    advance_snap_take(sv, &pre);

    moq_reject_subscribe_cfg_init(&rcfg);
    /* volatile so a narrowed public type truncates at RUNTIME instead of
     * tripping a compile-time constant-conversion diagnostic (see the width
     * cell): a build failure would be void evidence, not a kill */
    {
        volatile uint64_t over = MOQ_QUIC_VARINT_MAX + 1;

        rcfg.error_code = (moq_request_error_t)over;
    }
    rcfg.reason = (moq_bytes_t){ k_reason_bytes, sizeof(k_reason_bytes) };
    rcfg.can_retry = true;
    rcfg.retry_after_ms = OUT_RETRY_MS;
    MOQ_TEST_CHECK_EQ_INT(
        moq_session_reject_subscribe(sv, inbound, &rcfg, REFUSE_EPOCH),
        MOQ_ERR_INVAL);

    /* the advance preamble never ran */
    failures += advance_snap_unchanged(&pre, sv, "d16 refusal");

    /* nothing moved */
    tally_actions(sv, &t);
    MOQ_TEST_CHECK_EQ_INT(t.total, 0);
    (void)count_events_of_kind(sv, 0, &total);
    MOQ_TEST_CHECK_EQ_INT(total, 0);
    MOQ_TEST_CHECK_EQ_SIZE(sv->drain_ref_count, 0);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    /* the owner is untouched, and the topology is identical */
    failures += check_inbound_sub_armed(sv, &srv, MOQ_VERSION_DRAFT_16,
                                        "owner after refusal");
    og_capture(sv, &after);
    failures += og_check_same_topology(&before, &after, "refusal topology");

    /* and the SAME call with a representable code still works */
    {
        volatile uint64_t at_max = MOQ_QUIC_VARINT_MAX;

        rcfg.error_code = (moq_request_error_t)at_max;
    }
    MOQ_TEST_CHECK_EQ_INT(
        moq_session_reject_subscribe(sv, inbound, &rcfg, RETRY_EPOCH),
        MOQ_OK);
    tally_actions(sv, &t);
    MOQ_TEST_CHECK_EQ_INT(t.total, 1);
    MOQ_TEST_CHECK_EQ_INT(t.send_control, 1);
    {
        moq_buf_reader_t r;
        moq_control_envelope_t env;
        moq_d16_request_error_t err;

        moq_buf_reader_init(&r, t.bytes, t.bytes_len);
        MOQ_TEST_CHECK_EQ_INT(moq_control_decode_envelope(&r, &env), MOQ_OK);
        memset(&err, 0, sizeof(err));
        MOQ_TEST_CHECK_EQ_INT(moq_d16_decode_request_error(
            env.payload, env.payload_len, &err), MOQ_OK);
        MOQ_TEST_CHECK(err.error_code == MOQ_QUIC_VARINT_MAX);
    }

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

/*
 * Every draft-16-reachable reject/cancel API refuses an above-ceiling code
 * BEFORE its advancing preamble.
 *
 * The guard is duplicated per API, not a shared call path, so each one needs
 * its own evidence. The discriminator needs no per-family fixture: with an
 * INVALID handle,
 *
 *   - an above-ceiling code must return MOQ_ERR_INVAL with the advance
 *     snapshot untouched -- the width check ran first;
 *   - a representable code returns MOQ_ERR_STALE_HANDLE, and that answer
 *     comes from handle resolution AFTER session_begin_advance(), so the
 *     snapshot MUST have moved.
 *
 * The second half is what makes the first non-vacuous: it proves the API
 * really does advance when it gets that far, so "nothing moved" is a fact
 * about the width check and not about an API that never advances.
 *
 * SUBSCRIBE_TRACKS is excluded: it has no draft-16 presence at all (no
 * MOQ_D16_* message type, and profile_d16.c never mentions the family), so
 * it is draft-18-only -- and on draft-18 the ceiling is UINT64_MAX, which no
 * uint64_t argument can exceed, so the check there is unreachable by
 * construction rather than untested.
 */
static int d16_family_refuses_before_advance(void)
{
    int failures = 0;
    moq_session_t *c = NULL, *sv = NULL;
    const moq_request_error_t over =
        (moq_request_error_t)(MOQ_QUIC_VARINT_MAX + 1);
    const moq_bytes_t reason = { k_reason_bytes, sizeof(k_reason_bytes) };
    struct advance_snap pre;

    failures += make_pair(MOQ_VERSION_DRAFT_16, &c, &sv);
    if (c == NULL || sv == NULL)
        return failures;

#define REFUSE_CELL(NAME, CFGT, INIT, STALE_US, CALL_OVER, CALL_STALE)        \
    do {                                                                      \
        CFGT cfg;                                                             \
        INIT(&cfg);                                                           \
        cfg.error_code = over;                                                \
        cfg.reason = reason;                                                  \
        advance_snap_take(sv, &pre);                                          \
        MOQ_TEST_CHECK_EQ_INT(CALL_OVER, MOQ_ERR_INVAL);                      \
        failures += advance_snap_unchanged(&pre, sv, NAME);                   \
        /*                                                                    \
         * ... and the SAME API does run its advancing preamble when the      \
         * width check passes. Proven against a snapshot taken immediately    \
         * before THIS call, never against a timestamp an earlier cell may    \
         * already have installed: borrow_epoch is bumped unconditionally     \
         * once per physical advancing call, so +1 is this call's own         \
         * evidence. Each cell also uses its own strictly increasing epoch,   \
         * asserted to be past the one in force before it.                    \
         */                                                                   \
        advance_snap_take(sv, &pre);                                          \
        cfg.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;                    \
        MOQ_TEST_CHECK((uint64_t)(STALE_US) > pre.last_now_us);               \
        MOQ_TEST_CHECK_EQ_INT(CALL_STALE, MOQ_ERR_STALE_HANDLE);              \
        MOQ_TEST_CHECK(sv->borrow_epoch == pre.borrow_epoch + 1);             \
        MOQ_TEST_CHECK(sv->last_now_us == (uint64_t)(STALE_US));              \
    } while (0)

    REFUSE_CELL("reject_subscribe", moq_reject_subscribe_cfg_t,
        moq_reject_subscribe_cfg_init, 7100,
        moq_session_reject_subscribe(sv, MOQ_SUBSCRIPTION_INVALID, &cfg, 7000),
        moq_session_reject_subscribe(sv, MOQ_SUBSCRIPTION_INVALID, &cfg, 7100));
    REFUSE_CELL("reject_fetch", moq_reject_fetch_cfg_t,
        moq_reject_fetch_cfg_init, 7300,
        moq_session_reject_fetch(sv, MOQ_FETCH_INVALID, &cfg, 7200),
        moq_session_reject_fetch(sv, MOQ_FETCH_INVALID, &cfg, 7300));
    REFUSE_CELL("reject_publish", moq_reject_publish_cfg_t,
        moq_reject_publish_cfg_init, 7500,
        moq_session_reject_publish(sv, MOQ_PUBLICATION_INVALID, &cfg, 7400),
        moq_session_reject_publish(sv, MOQ_PUBLICATION_INVALID, &cfg, 7500));
    REFUSE_CELL("reject_namespace", moq_reject_namespace_cfg_t,
        moq_reject_namespace_cfg_init, 7700,
        moq_session_reject_namespace(sv, MOQ_ANNOUNCEMENT_INVALID, &cfg, 7600),
        moq_session_reject_namespace(sv, MOQ_ANNOUNCEMENT_INVALID, &cfg, 7700));
    REFUSE_CELL("cancel_namespace", moq_cancel_namespace_cfg_t,
        moq_cancel_namespace_cfg_init, 7900,
        moq_session_cancel_namespace(sv, MOQ_ANNOUNCEMENT_INVALID, &cfg, 7800),
        moq_session_cancel_namespace(sv, MOQ_ANNOUNCEMENT_INVALID, &cfg, 7900));
    REFUSE_CELL("reject_track_status", moq_reject_track_status_cfg_t,
        moq_reject_track_status_cfg_init, 8100,
        moq_session_reject_track_status(sv, MOQ_TRACK_STATUS_HANDLE_INVALID,
                                        &cfg, 8000),
        moq_session_reject_track_status(sv, MOQ_TRACK_STATUS_HANDLE_INVALID,
                                        &cfg, 8100));
    REFUSE_CELL("reject_ns_sub", moq_reject_ns_sub_cfg_t,
        moq_reject_ns_sub_cfg_init, 8300,
        moq_session_reject_ns_sub(sv, MOQ_NS_SUB_HANDLE_INVALID, &cfg, 8200),
        moq_session_reject_ns_sub(sv, MOQ_NS_SUB_HANDLE_INVALID, &cfg, 8300));
#undef REFUSE_CELL

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return failures;
}

/* ------------------------------------------------------------------ */

int main(void)
{
    int failures = 0;

    /* -- codec fidelity: the defect is not here ---------------------- */
    failures += codec_d16_preserves_wide_code();
    failures += codec_d18_preserves_wide_code();

    /* -- non-vacuity controls: a REGISTERED code arrives unchanged ---- */
    {
        struct inbound_obs o;

        failures += run_inbound_d16((uint64_t)COLLIDING_LOW32, &o);
        MOQ_TEST_CHECK(o.code == COLLIDING_LOW32);
    }
    {
        struct inbound_obs o;

        failures += run_inbound_d18((uint64_t)COLLIDING_LOW32, &o);
        MOQ_TEST_CHECK(o.code == COLLIDING_LOW32);
    }

    /* -- RED: draft-18 normalization, three independent shapes -------- */
    {   /* wide unknown: truncation AND missing normalization */
        struct inbound_obs o;

        failures += run_inbound_d18(WIDE_UNKNOWN_CODE, &o);
        MOQ_TEST_CHECK(o.code == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
        MOQ_TEST_CHECK(o.code != COLLIDING_LOW32);
    }
    {   /* in-range unknown: NO truncation, missing normalization alone */
        struct inbound_obs o;

        failures += run_inbound_d18(NARROW_UNKNOWN_CODE, &o);
        MOQ_TEST_CHECK(o.code == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    }
    {   /* GREASE is unknown, not a registered semantic (d18 §14) */
        struct inbound_obs o;

        failures += run_inbound_d18(GREASE_CODE, &o);
        MOQ_TEST_CHECK(o.code == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    }

    /*
     * -- RED: a wide value whose low bits are REDIRECT ----------------
     * Redirect recognition must look at the FULL decoded value. On both
     * drafts this must stay an ORDINARY REQUEST_ERROR: exactly one
     * SUBSCRIBE_ERROR, zero MOQ_EVENT_REQUEST_REDIRECT. What the code
     * should then BE differs by draft, exactly as above.
     */
    {   /* draft-16: no normalization rule, so the raw value survives */
        struct inbound_obs o;

        failures += run_inbound_d16(WIDE_REDIRECT_CODE, &o);
        MOQ_TEST_CHECK_EQ_INT(o.redirects, 0);
        MOQ_TEST_CHECK(o.code != MOQ_REQUEST_ERROR_REDIRECT);
        MOQ_TEST_CHECK((uint64_t)o.code == WIDE_REDIRECT_CODE);
    }
    {   /* draft-18: unknown, so INTERNAL_ERROR -- and still no redirect */
        struct inbound_obs o;

        failures += run_inbound_d18(WIDE_REDIRECT_CODE, &o);
        MOQ_TEST_CHECK_EQ_INT(o.redirects, 0);
        MOQ_TEST_CHECK(o.code != MOQ_REQUEST_ERROR_REDIRECT);
        MOQ_TEST_CHECK(o.code == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    }

    /* -- RED: draft-16, the narrowest defensible expectation ---------- */
    {
        struct inbound_obs o;

        failures += run_inbound_d16(WIDE_UNKNOWN_CODE, &o);
        MOQ_TEST_CHECK(o.code != COLLIDING_LOW32);
        MOQ_TEST_CHECK((uint64_t)o.code == WIDE_UNKNOWN_CODE);
    }

    /*
     * -- every REGISTERED code of each draft's own table passes unchanged --
     * This is what stops an over-broad predicate from flattening real codes
     * to INTERNAL_ERROR. The tables are the drafts', not the header's.
     */
    {
        static const uint64_t d16_registered[] = {
            0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x10, 0x11, 0x12, 0x19, 0x20,
            0x30, 0x32,
        };
        size_t i;

        MOQ_TEST_CHECK_EQ_SIZE(sizeof(d16_registered) / sizeof(*d16_registered),
                               13);
        for (i = 0; i < sizeof(d16_registered) / sizeof(*d16_registered); i++) {
            struct inbound_obs o;

            failures += run_inbound_d16(d16_registered[i], &o);
            MOQ_TEST_CHECK((uint64_t)o.code == d16_registered[i]);
        }
    }
    {
        static const uint64_t d18_registered[] = {
            0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x9, 0x10, 0x11, 0x12, 0x19,
            0x20, 0x30, 0x31, 0x32, 0x33, 0x34,
        };
        size_t i;

        MOQ_TEST_CHECK_EQ_SIZE(sizeof(d18_registered) / sizeof(*d18_registered),
                               18);
        for (i = 0; i < sizeof(d18_registered) / sizeof(*d18_registered); i++) {
            struct inbound_obs o;

            /* 0x34 with no Redirect tail is a malformed REDIRECT, so its
             * pass-through is checked by redirect_control_d18() -- which
             * asserts the surfaced code exactly -- rather than here */
            if (d18_registered[i] == MOQ_REQUEST_ERROR_REDIRECT)
                continue;
            failures += run_inbound_d18(d18_registered[i], &o);
            MOQ_TEST_CHECK((uint64_t)o.code == d18_registered[i]);
        }
    }

    /*
     * -- draft-16 preserves EVERY unknown, including draft-18-only codes --
     * A value registered only by draft-18 is simply unknown to draft-16, and
     * draft-16 states no normalization rule, so it must survive verbatim.
     */
    {
        static const uint64_t d16_unknown[] = {
            NARROW_UNKNOWN_CODE, GREASE_CODE,
            0x6, 0x9, 0x31, 0x33, 0x34,          /* draft-18-only */
            WIDE_UNKNOWN_CODE, WIDE_REDIRECT_CODE,
        };
        size_t i;

        for (i = 0; i < sizeof(d16_unknown) / sizeof(*d16_unknown); i++) {
            struct inbound_obs o;

            failures += run_inbound_d16(d16_unknown[i], &o);
            MOQ_TEST_CHECK((uint64_t)o.code == d16_unknown[i]);
            MOQ_TEST_CHECK_EQ_INT(o.redirects, 0);
        }
    }

    /* -- draft-18 normalizes the whole unknown domain, up to UINT64_MAX -- */
    {
        struct inbound_obs o;

        failures += run_inbound_d18(UINT64_MAX, &o);
        MOQ_TEST_CHECK(o.code == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
    }

    /* -- positive control: the redirect counter is not stuck at zero -- */
    failures += redirect_control_d18();

    /* -- outbound ---------------------------------------------------- */
    failures += outbound_public_width_is_64_bit();
    failures += outbound_reject(MOQ_VERSION_DRAFT_16);
    failures += outbound_reject(MOQ_VERSION_DRAFT_18);
    /* a real code above UINT32_MAX, within draft-16's own ceiling */
    failures += outbound_reject_code(MOQ_VERSION_DRAFT_16,
                                     UINT64_C(0x1FFFFFFFF));
    /* draft-18 can express the whole domain */
    failures += outbound_reject_code(MOQ_VERSION_DRAFT_18, UINT64_MAX);
    failures += outbound_refuses_unrepresentable_d16();
    failures += d16_family_refuses_before_advance();

    if (failures == 0)
        MOQ_TEST_PASS("request_error_width");
    return failures != 0;
}
