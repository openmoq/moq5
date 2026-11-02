#include "test_cases.h"
#include "../test_binding_support.h"

/* -- The peer control stream's last free stream-map entry ---------------- *
 *
 * A draft-18/22 server session whose peer control stream is not mapped yet,
 * with a two-entry stream map, `occupied` entries taken first. With one
 * occupied, a single entry is free when the peer's control stream arrives.
 * The control stream must be admitted there whether its stream type arrives
 * whole or split across callbacks: the receiver cannot know a split type is
 * the control stream until its last byte arrives (draft-22 section 6.3,
 * -22.txt:2556-2558, draft-18 section 3.3, -18.txt:1495-1497: a control
 * stream must not be closed during the session). */
static int cr_open(hb_rig_t *r, moq_version_t ver, uint32_t occupied)
{
    memset(r, 0, sizeof(*r));
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    cfg.version = ver;
    if (moq_session_create(&cfg, 0, &r->s) < 0) return -1;
    if (test_bind_if_required(r->s) < 0) { moq_session_destroy(r->s); return -1; }
    held_endpoint_init(&r->ep, 100, 200);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    bcfg.max_streams = 2;
    bcfg.max_pending = 2;
    if (held_bridge_create(&bcfg, r->s, &r->ep.vtable, &r->ep, &r->b) < 0) {
        moq_session_destroy(r->s);
        return -1;
    }
    for (uint32_t i = 0; i < occupied && i < r->b->max_streams; i++) {
        bridge_stream_entry_t *e = bridge_alloc_stream(r->b);
        if (e != &r->b->streams[i]) goto fail;
        e->transport_id = 900 + i;
        e->kind = BRIDGE_STREAM_UNI;
        e->origin = BRIDGE_ORIGIN_PEER;
        e->uni_disp = BRIDGE_UNI_DISP_DATA;
        e->ref = moq_stream_ref_from_u64(BRIDGE_INBOUND_REF_BASE + 0x900 + i);
    }
    const uint32_t expected = occupied < r->b->max_streams ? occupied : r->b->max_streams;
    if (r->b->streams_active != expected ||
        moq_transport_bridge_stream_count(r->b) != expected) goto fail;
    return 0;
fail:
    held_bridge_destroy(r->b);
    moq_session_destroy(r->s);
    return -1;
}


int test_control_last_entry(moq_version_t ver, bool split)
{
    int failures = 0;
    static const uint8_t control[2] = { 0xaf, 0x00 };   /* type 0x2f00 */
    hb_rig_t r;
    if (cr_open(&r, ver, 1) < 0) { MOQ_TEST_CHECK(false); return failures; }
    moq_result_t rc;
    if (split) {
        rc = moq_transport_bridge_on_peer_uni_bytes(r.b, 302, control, 1,
                                                    false, 1000);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
        MOQ_TEST_CHECK(!r.b->peer_ctrl_uni_open);
        rc = moq_transport_bridge_on_peer_uni_bytes(r.b, 302, control + 1, 1,
                                                    false, 1001);
    } else {
        rc = moq_transport_bridge_on_peer_uni_bytes(r.b, 302, control,
                                                    sizeof(control), false, 1000);
    }
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK)
        fprintf(stderr, "control last entry d%d %s: rc=%d fatal=%d\n",
                ver == MOQ_VERSION_DRAFT_22 ? 22 : 18, split ? "split" : "whole",
                (int)rc, (int)moq_transport_bridge_is_fatal(r.b));
    MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));
    MOQ_TEST_CHECK(r.b->peer_ctrl_uni_open && r.b->peer_ctrl_uni_stream_id == 302);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_is_control(r.b, 302));
    hb_close(&r);
    return failures;
}


/* While the session drains buffered control input on a uni-pair profile,
 * only the identified control stream's own input is the session's retained
 * input (WOULD_BLOCK); any other peer uni stream's chunk is refused whole
 * (MOQ_ERR_INPUT_NOT_CONSUMED: nothing mapped, nothing fed) and reports
 * pending until the control input drains. Streams the bridge knows were
 * never refused there -- a mapped request stream, a local data uni and the
 * local control uni -- do not report pending; an unmapped id does, because
 * the bridge cannot tell it from a refused peer uni stream. Draft-16
 * (control on a bidi stream) defers no uni input. */
int test_uni_deferred_behind_control(moq_version_t ver)
{
    int failures = 0;
    static const uint8_t data[3] = { 0x05, 0x01, 0x00 };
    hb_rig_t r;
    if (cr_open(&r, ver, 1) < 0) { MOQ_TEST_CHECK(false); return failures; }
    const bool uni_pair = ver != MOQ_VERSION_DRAFT_16;
    r.b->streams[0].kind = BRIDGE_STREAM_BIDI;        /* a mapped request stream */
    bridge_stream_entry_t *lu = bridge_alloc_stream(r.b); /* a local data uni */
    MOQ_TEST_CHECK(lu == &r.b->streams[1]);
    if (lu != &r.b->streams[1]) { hb_close(&r); return failures; }
    lu->transport_id = 901;
    lu->kind = BRIDGE_STREAM_UNI;
    lu->origin = BRIDGE_ORIGIN_LOCAL;
    lu->ref = moq_stream_ref_from_u64(0x901);
    MOQ_TEST_CHECK(r.b->streams_active == r.b->max_streams);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(r.b) == r.b->max_streams);
    r.b->pending_control = true;
    r.b->pending_control_stream_id = 3;

    if (uni_pair) {
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(r.b, 3, NULL, 0, false, 1000) ==
                       MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(r.b, 7, data, sizeof(data), false, 1000) ==
                       MOQ_ERR_INPUT_NOT_CONSUMED);
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(r.b, 7, NULL, 0, true, 1000) ==
                       MOQ_ERR_INPUT_NOT_CONSUMED);
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(r.b, 11, NULL, 0, false, 1000) == MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_find_ref(r.b, 7)._v == 0);
        MOQ_TEST_CHECK(!r.b->pending_control_fin);
        MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 7));
    } else {
        MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 7));
    }
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 3));
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 900));   /* request stream */
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 901));   /* local data uni */
    /* The same entry as the local control uni. */
    lu->transport_id = 902;
    r.b->local_ctrl_uni_open = true;
    r.b->local_ctrl_uni_stream_id = 902;
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 902));
    /* An unmapped id is answered conservatively. */
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(r.b, 4000) == uni_pair);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(r.b));

    r.b->pending_control = false;                      /* the control input drained */
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 7));
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(r.b, 3));
    hb_close(&r);
    return failures;
}


/* -- Minimum stream-map capacity at create ------------------------------ *
 *
 * A unidirectional control pair (drafts 18 and 22) keeps one map entry per
 * control stream for the whole session, so create refuses an effective
 * max_streams below 2 there: MOQ_ERR_INVAL, before any allocation attempt or
 * endpoint call, with *out NULL and the session left free for a later valid
 * create. Zero selects the default and is accepted. Draft-16 (one bidi
 * control stream) accepts 1. The single-owner refusal is unchanged. */

static unsigned g_mc_alloc_attempts;   /* alloc and realloc calls */
static long     g_mc_live;             /* outstanding blocks */
static unsigned g_mc_ep_calls;         /* endpoint operations invoked */

static void *mc_alloc(size_t n, void *ctx)
{
    (void)ctx;
    g_mc_alloc_attempts++;
    void *p = malloc(n);
    if (p) g_mc_live++;
    return p;
}

static void *mc_realloc(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    g_mc_alloc_attempts++;
    const bool was_null = p == NULL;   /* p may be invalid after realloc */
    void *q = realloc(p, n);
    if (was_null && q) g_mc_live++;
    return q;
}

static void mc_free(void *p, size_t n, void *ctx)
{
    (void)n; (void)ctx;
    if (p) g_mc_live--;
    free(p);
}


static moq_transport_result_t mc_open(void *ctx, uint64_t *out_id)
{ (void)ctx; g_mc_ep_calls++; *out_id = 0; return MOQ_TRANSPORT_OK; }

static moq_transport_result_t mc_write(void *ctx, uint64_t sid, const uint8_t *d,
                                       size_t len, bool fin)
{ (void)ctx; (void)sid; (void)d; (void)len; (void)fin; g_mc_ep_calls++; return MOQ_TRANSPORT_OK; }

static moq_transport_result_t mc_code(void *ctx, uint64_t sid, uint64_t code)
{ (void)ctx; (void)sid; (void)code; g_mc_ep_calls++; return MOQ_TRANSPORT_OK; }

static moq_transport_result_t mc_close(void *ctx, uint64_t code,
                                       const uint8_t *reason, size_t reason_len)
{ (void)ctx; (void)code; (void)reason; (void)reason_len; g_mc_ep_calls++; return MOQ_TRANSPORT_OK; }


static moq_transport_endpoint_ops_t mc_ops(void)
{
    moq_transport_endpoint_ops_t o = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    o.capabilities = MOQ_TRANSPORT_CAP_HOLD_INPUT;
    o.open_uni = mc_open;
    o.open_bidi = mc_open;
    o.write = mc_write;
    o.reset_stream = mc_code;
    o.stop_sending = mc_code;
    o.close_transport = mc_close;
    return o;
}


static moq_result_t mc_create(moq_session_t *s, uint32_t max_streams,
                              const moq_transport_endpoint_ops_t *ops,
                              moq_transport_bridge_t **out)
{
    static const moq_alloc_t al = { .ctx = NULL, .alloc = mc_alloc,
                                    .realloc = mc_realloc, .free = mc_free };
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, &al);
    bcfg.max_streams = max_streams;
    *out = (moq_transport_bridge_t *)(uintptr_t)1;      /* must become NULL */
    return moq_transport_bridge_create(&bcfg, s, ops, NULL, out);
}


int test_min_streams_at_create(moq_version_t ver, moq_perspective_t persp)
{
    int failures = 0;
    const bool uni_pair = ver != MOQ_VERSION_DRAFT_16;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = ver;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) { MOQ_TEST_CHECK(false); return failures; }
    if (test_bind_if_required(s) < 0) { MOQ_TEST_CHECK(false); moq_session_destroy(s); return failures; }
    MOQ_TEST_CHECK(moq_session_uses_uni_control(s) == uni_pair);
    moq_transport_endpoint_ops_t ops = mc_ops();
    moq_transport_bridge_t *b = NULL;

    /* One entry: refused on a uni pair with no allocation attempt and no
     * endpoint call; accepted on draft-16. */
    g_mc_alloc_attempts = 0;
    g_mc_ep_calls = 0;
    moq_result_t rc = mc_create(s, 1, &ops, &b);
    if (uni_pair) {
        MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(b == NULL);
        MOQ_TEST_CHECK(g_mc_alloc_attempts == 0);
        MOQ_TEST_CHECK(g_mc_ep_calls == 0);
        MOQ_TEST_CHECK(g_mc_live == 0);
        /* A create that wrongly succeeded is released, so the accepted
         * cases below still judge their own outcome. */
        if (rc == MOQ_OK && b != NULL &&
            b != (moq_transport_bridge_t *)(uintptr_t)1)
            moq_transport_bridge_destroy(b);
    } else {
        MOQ_TEST_CHECK(rc == MOQ_OK && b != NULL);
        if (rc == MOQ_OK) moq_transport_bridge_destroy(b);
    }
    MOQ_TEST_CHECK(g_mc_live == 0);

    /* Two entries: accepted, and a refusal above did not claim the session. */
    rc = mc_create(s, 2, &ops, &b);
    MOQ_TEST_CHECK(rc == MOQ_OK && b != NULL);
    if (rc == MOQ_OK) {
        /* The single-owner refusal is unchanged; the capacity check comes
         * first, before anything is allocated or the owner is registered. */
        moq_transport_bridge_t *b2 = NULL;
        MOQ_TEST_CHECK(mc_create(s, 2, &ops, &b2) == MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(b2 == NULL);
        if (uni_pair) {
            g_mc_alloc_attempts = 0;
            MOQ_TEST_CHECK(mc_create(s, 1, &ops, &b2) == MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(b2 == NULL);
            MOQ_TEST_CHECK(g_mc_alloc_attempts == 0);
        }
        moq_transport_bridge_destroy(b);
    }
    MOQ_TEST_CHECK(g_mc_live == 0);

    /* Zero selects the default capacity and is accepted. */
    rc = mc_create(s, 0, &ops, &b);
    MOQ_TEST_CHECK(rc == MOQ_OK && b != NULL);
    if (rc == MOQ_OK) {
        MOQ_TEST_CHECK(b->max_streams == BRIDGE_DEFAULT_MAX_STREAMS);
        moq_transport_bridge_destroy(b);
    }
    MOQ_TEST_CHECK(g_mc_live == 0);
    moq_session_destroy(s);
    return failures;
}
