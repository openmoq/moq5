/* Private binding accounting plus real-session wire oracles. Include the
 * existing rig and the delivery and draft-22 suites unchanged; their renamed
 * entry points keep the retained controls available without copying a second
 * simulator harness. No core TU is included.
 * Private binding accounting is linked locally so saturation is checked
 * as arithmetic, without manufacturing a live subscription with billions of
 * streams. */
#include "../../relay/src/bind/bind_internal.h"
#include "../../relay/src/bind/intents_internal.h"
#include "../../relay/tests/relay_session_binding_harness.c"
#define main relay_session_binding_delivery_main
#include "../../relay/tests/test_relay_session_binding_delivery.c"
#undef main
#define main relay_session_binding_d22_main
#include "../../relay/tests/test_relay_session_binding_d22.c"
#undef main
#include <moq/control.h>
#include <moq/control_d18.h>

typedef struct completion_wire {
    moq_version_t version;
    uint64_t streams[32], counts[8];
    unsigned opened, accepts, terminals, resets, datagrams, errors;
} completion_wire_t;

static unsigned completion_checks, completion_cases;
#define C_CHECK(r, expr) do { completion_checks++; R_CHECK((r), (expr)); } while (0)

static void
completion_trace(void *ctx, const moq_sim_trace_record_t *r)
{
    completion_wire_t *w = ctx;
    if (r->kind != MOQ_SIM_TRACE_ACTION ||
        r->from != MOQ_PERSPECTIVE_SERVER) return;
    if (r->action_kind == MOQ_ACTION_RESET_DATA) w->resets++;
    if (r->action_kind == MOQ_ACTION_SEND_DATAGRAM) w->datagrams++;
    if (r->action_kind == MOQ_ACTION_SEND_DATA) {
        unsigned i = 0;
        while (i < w->opened && w->streams[i] != r->stream_ref._v) i++;
        if (i == w->opened) {
            if (i == 32) { w->errors++; return; }
            w->streams[w->opened++] = r->stream_ref._v;
        }
    }
    if (r->action_kind != MOQ_ACTION_SEND_CONTROL &&
        r->action_kind != MOQ_ACTION_SEND_BIDI_STREAM) return;
    moq_buf_reader_t reader;
    moq_bytes_t bytes = r->action_kind == MOQ_ACTION_SEND_CONTROL
                           ? r->bytes : r->detail_bytes;
    moq_buf_reader_init(&reader, bytes.data, bytes.len);
    while (moq_buf_reader_remaining(&reader) != 0) {
        moq_control_envelope_t env;
        moq_result_t rc = w->version == MOQ_VERSION_DRAFT_16
            ? moq_control_decode_envelope(&reader, &env)
            : moq_d18_decode_envelope(&reader, &env);
        if (rc != MOQ_OK) { w->errors++; return; }
        if (env.msg_type == UINT64_C(0x04)) w->accepts++;
        if (env.msg_type != UINT64_C(0x0b)) continue;
        if (w->accepts == 0) w->errors++;
        uint64_t count = 0;
        if (w->version == MOQ_VERSION_DRAFT_16) {
            moq_d16_publish_done_t done;
            rc = moq_d16_decode_publish_done(env.payload, env.payload_len, &done);
            if (rc == MOQ_OK) count = done.stream_count;
        } else {
            /* D18 and D22 DONE share the vi64 code/count/reason grammar. */
            moq_d18_publish_done_t done;
            rc = moq_d18_decode_publish_done(env.payload, env.payload_len, &done);
            if (rc == MOQ_OK) count = done.stream_count;
        }
        if (rc != MOQ_OK || w->terminals == 8) { w->errors++; return; }
        w->counts[w->terminals++] = count;
    }
}

static conn_t *
completion_connect(rig_t *r, completion_wire_t *w, moq_version_t version)
{
    memset(w, 0, sizeof(*w));
    w->version = version;
    relay_bind_test_trace_fn = completion_trace;
    relay_bind_test_trace_ctx = w;
    conn_t *cn = relay_bind_test_rig_connect(r, version);
    relay_bind_test_trace_fn = NULL;
    relay_bind_test_trace_ctx = NULL;
    return cn;
}

static moq_publication_t
completion_publish(rig_t *r, conn_t *pub, const char *name)
{
    moq_bytes_t ns = B("count");
    moq_publish_cfg_t cfg;
    moq_publish_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ .parts = &ns, .count = 1 };
    cfg.track_name = B(name);
    moq_publication_t handle = {0};
    C_CHECK(r, moq_session_publish(pub->peer, &cfg, r->now, &handle) == MOQ_OK);
    relay_bind_test_rig_pump(r, 6);
    peer_state_t ps = {0};
    relay_bind_test_peer_drain(r, pub, &ps, false, false, 0, 0);
    C_CHECK(r, ps.publish_ok);
    return handle;
}

static moq_subscription_t
completion_subscribe(rig_t *r, conn_t *sub, const char *name)
{
    moq_bytes_t ns = B("count");
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ .parts = &ns, .count = 1 };
    cfg.track_name = B(name);
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t handle = {0};
    C_CHECK(r, moq_session_subscribe(sub->peer, &cfg, r->now, &handle) == MOQ_OK);
    relay_bind_test_rig_pump(r, 6);
    peer_state_t ps = {0};
    relay_bind_test_peer_drain(r, sub, &ps, false, false, 0, 0);
    C_CHECK(r, ps.subscribe_ok && !ps.session_closed);
    return handle;
}

static void
completion_finish(rig_t *r, conn_t *pub, moq_publication_t pubh, uint64_t count)
{
    moq_finish_publish_cfg_t cfg;
    moq_finish_publish_cfg_init(&cfg);
    cfg.stream_count = count;
    C_CHECK(r, moq_session_finish_publish(pub->peer, pubh, &cfg, r->now) == MOQ_OK);
    relay_bind_test_rig_pump(r, 16);
}

static int
completion_end(rig_t *r, ca_t *a, const char *name, completion_wire_t *w)
{
    moqr_bind_stats_t stats;
    moqr_bind_get_stats(r->bind, &stats);
    C_CHECK(r, stats.session_errors == 0);
    C_CHECK(r, w->errors == 0);
    printf("WIRE %s opened=%u terminals=%u first=%llu second=%llu resets=%u datagrams=%u\n",
           name, w->opened, w->terminals, (unsigned long long)w->counts[0],
           (unsigned long long)w->counts[1], w->resets, w->datagrams);
    relay_bind_test_rig_destroy(r);
    C_CHECK(r, a->live == 0);
    completion_cases++;
    printf("%s completion/%s failures=%d\n", r->failures ? "FAIL" : "PASS", name, r->failures);
    return r->failures;
}

static int
completion_simple(moq_version_t version, unsigned streams, bool retry)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_open_subgroups = 1;
    rig_t r;
    if (relay_bind_test_rig_create(&r, &a) != MOQR_OK) return 1;
    relay_bind_test_max_open_subgroups = 0;
    completion_wire_t w;
    conn_t *pub = relay_bind_test_rig_connect(&r, version);
    conn_t *sub = completion_connect(&r, &w, version);
    if (!pub || !sub) { relay_bind_test_rig_destroy(&r); return 1; }
    relay_bind_test_rig_pump(&r, 4);
    moq_publication_t ph = completion_publish(&r, pub, "v");
    (void)completion_subscribe(&r, sub, "v");
    peer_state_t ps = {0};
    for (unsigned i = 0; i < streams; i++) {
        relay_bind_test_se_send_group(&r, pub, true, (moq_subscription_t){0}, ph, i, 2);
        relay_bind_test_rig_pump(&r, 6);
        relay_bind_test_peer_drain(&r, sub, &ps, false, false, 0, 0);
    }
    C_CHECK(&r, ps.objects == (int)(streams * 2));
    C_CHECK(&r, w.opened == streams);
    if (retry) moqr_bind_debug_block_sub_done(4);
    completion_finish(&r, pub, ph, streams);
    C_CHECK(&r, moqr_bind_debug_sub_done_blocks_left() == 0);
    relay_bind_test_peer_drain(&r, sub, &ps, false, false, 0, 0);
    C_CHECK(&r, ps.done_count == 1 && !ps.session_closed);
    C_CHECK(&r, w.terminals == 1 && w.counts[0] == streams);
    char name[96];
    (void)snprintf(name, sizeof(name), "profile-%llu/streams-%u/retry-%d",
                   (unsigned long long)version, streams, (int)retry);
    return completion_end(&r, &a, name, &w);
}

static int
completion_independent(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t r;
    if (relay_bind_test_rig_create(&r, &a) != MOQR_OK) return 1;
    completion_wire_t w;
    conn_t *pub = relay_bind_test_rig_connect(&r, version);
    conn_t *sub = completion_connect(&r, &w, version);
    if (!pub || !sub) { relay_bind_test_rig_destroy(&r); return 1; }
    relay_bind_test_rig_pump(&r, 4);
    moq_publication_t p0 = completion_publish(&r, pub, "v");
    moq_publication_t p1 = completion_publish(&r, pub, "a");
    (void)completion_subscribe(&r, sub, "v");
    (void)completion_subscribe(&r, sub, "a");
    relay_bind_test_se_send_group(&r, pub, true, (moq_subscription_t){0}, p0, 0, 1);
    relay_bind_test_rig_pump(&r, 6);
    relay_bind_test_se_send_group(&r, pub, true, (moq_subscription_t){0}, p1, 0, 1);
    relay_bind_test_rig_pump(&r, 6);
    relay_bind_test_se_send_group(&r, pub, true, (moq_subscription_t){0}, p0, 1, 1);
    relay_bind_test_rig_pump(&r, 6);
    completion_finish(&r, pub, p0, 2);
    C_CHECK(&r, w.terminals == 1 && w.counts[0] == 2);
    completion_finish(&r, pub, p1, 1);
    C_CHECK(&r, w.terminals == 2 && w.counts[1] == 1);
    moq_publication_t p2 = completion_publish(&r, pub, "new");
    (void)completion_subscribe(&r, sub, "new");
    completion_finish(&r, pub, p2, 0);
    C_CHECK(&r, w.terminals == 3 && w.counts[2] == 0);
    C_CHECK(&r, w.opened == 3);
    return completion_end(&r, &a, "independent-same-connection", &w);
}

static moq_subgroup_handle_t
completion_open(rig_t *r, conn_t *pub, moq_publication_t ph, uint64_t group)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    moq_subgroup_handle_t h = {0};
    DP_RETRY(r, moq_session_open_pub_subgroup(pub->peer, ph, &cfg, r->now, &h));
    moq_rcbuf_t *payload = NULL;
    const uint8_t bytes[3] = {0x91, 0x72, 0x53};
    C_CHECK(r, moq_rcbuf_create(&r->alloc->vt, bytes, sizeof(bytes), &payload) == MOQ_OK);
    if (payload) {
        DP_RETRY(r, moq_session_write_object(pub->peer, h, 0, payload, r->now));
        moq_rcbuf_decref(payload);
    }
    return h;
}

static int
completion_blocked_open(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t r;
    if (relay_bind_test_rig_create(&r, &a) != MOQR_OK) return 1;
    completion_wire_t w;
    conn_t *pub = relay_bind_test_rig_connect(&r, version);
    relay_bind_test_server_max_open_subgroups = 1;
    conn_t *sub = completion_connect(&r, &w, version);
    relay_bind_test_server_max_open_subgroups = 0;
    if (!pub || !sub) { relay_bind_test_rig_destroy(&r); return 1; }
    relay_bind_test_rig_pump(&r, 4);
    moq_publication_t ph = completion_publish(&r, pub, "v");
    (void)completion_subscribe(&r, sub, "v");
    moq_subgroup_handle_t h0 = completion_open(&r, pub, ph, 0);
    relay_bind_test_rig_pump(&r, 6);
    moq_subgroup_handle_t h1 = completion_open(&r, pub, ph, 1);
    relay_bind_test_rig_pump(&r, 6);
    moqr_bind_blocked_agg_t blocked;
    C_CHECK(&r, moqr_bind_debug_blocked_aggregate(r.bind, &blocked) == MOQR_OK);
    C_CHECK(&r, blocked.session_sg_total > 0);
    C_CHECK(&r, w.opened == 1);
    DP_RETRY(&r, moq_session_close_subgroup(pub->peer, h0, r.now));
    DP_RETRY(&r, moq_session_close_subgroup(pub->peer, h1, r.now));
    relay_bind_test_rig_pump(&r, 16);
    completion_finish(&r, pub, ph, 2);
    peer_state_t ps = {0};
    relay_bind_test_peer_drain(&r, sub, &ps, false, false, 0, 0);
    C_CHECK(&r, ps.objects == 2 && ps.done_count == 1 && !ps.session_closed);
    C_CHECK(&r, w.opened == 2 && w.terminals == 1 && w.counts[0] == 2);
    printf("BLOCKED failed-open-observations=%llu\n", (unsigned long long)blocked.session_sg_total);
    return completion_end(&r, &a, "failed-open-retry", &w);
}

static int
completion_empty(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 1;
    rig_t r;
    if (relay_bind_test_rig_create(&r, &a) != MOQR_OK) { relay_bind_test_max_actions = 0; return 1; }
    completion_wire_t w;
    conn_t *pub = relay_bind_test_rig_connect(&r, version);
    conn_t *sub = completion_connect(&r, &w, version);
    relay_bind_test_max_actions = 0;
    if (!pub || !sub) { relay_bind_test_rig_destroy(&r); return 1; }
    relay_bind_test_rig_pump(&r, 4);
    moq_publication_t ph = completion_publish(&r, pub, "v");
    (void)completion_subscribe(&r, sub, "v");
    sub->frozen = true;
    relay_bind_test_se_send_group(&r, pub, true, (moq_subscription_t){0}, ph, 0, 1);
    relay_bind_test_rig_pump_except(&r, sub, 8);
    b_conn_t *cn = conn_by_session(r.bind, sub->rsess);
    C_CHECK(&r, cn != NULL);
    b_dsub_t *ds = NULL;
    if (cn) for (uint32_t i = 0; i < r.bind->n_dsubs; i++) {
        if (cn->dsubs[i].used) { ds = &cn->dsubs[i]; break; }
    }
    C_CHECK(&r, ds != NULL && ds->streams_opened == 1);
    C_CHECK(&r, moq_session_action_capacity(sub->rsess) == 0 && w.opened == 0);
    if (ds) {
        /* Legitimate relay revoke while only the header fits in the action
         * queue. Read its real core handle; do not mutate either owner. */
        moqr_pd_desc_t terminal;
        C_CHECK(&r, moqr_pd_desc_extension(0x7, &terminal) == MOQR_OK);
        C_CHECK(&r, moqr_core_revoke_sub(r.core, ds->rsub, terminal, r.now) == MOQR_OK);
    }
    relay_bind_test_rig_bind_pump_once(&r);
    C_CHECK(&r, w.terminals == 0);
    sub->frozen = false;
    relay_bind_test_rig_pump(&r, 16);
    peer_state_t ps = {0};
    relay_bind_test_peer_drain(&r, sub, &ps, false, false, 0, 0);
    C_CHECK(&r, ps.objects == 0 && ps.done_count == 1 && !ps.session_closed);
    C_CHECK(&r, w.opened == 1 && w.terminals == 1 && w.counts[0] == 1);
    /* The revoke ended the subscription before the subgroup's object went
     * out, so the opened stream is reset (CANCELLED), never FINed (draft-18
     * section 11.4.3, draft-22 section 11.3.2). */
    C_CHECK(&r, w.resets == 1);
    return completion_end(&r, &a, "empty-stream-terminal-close-retry", &w);
}

static int
completion_reset(void)
{
    completion_wire_t w = { .version = MOQ_VERSION_DRAFT_18 };
    relay_bind_test_trace_fn = completion_trace;
    relay_bind_test_trace_ctx = &w;
    int failures = sub_done_resets_begun_subgroup();
    relay_bind_test_trace_fn = NULL;
    relay_bind_test_trace_ctx = NULL;
    completion_checks += 5;
    failures += w.errors != 0;
    failures += w.opened != 1;
    failures += w.terminals != 1;
    failures += w.counts[0] != 1;
    failures += w.resets != 1;
    completion_cases++;
    printf("%s completion/reset-stream count=%llu opened=%u resets=%u failures=%d\n",
           failures ? "FAIL" : "PASS", (unsigned long long)w.counts[0], w.opened, w.resets, failures);
    return failures;
}

static int
completion_datagrams(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t r;
    if (relay_bind_test_rig_create(&r, &a) != MOQR_OK) return 1;
    completion_wire_t w;
    conn_t *pub = relay_bind_test_rig_connect(&r, version);
    conn_t *sub = completion_connect(&r, &w, version);
    if (!pub || !sub) { relay_bind_test_rig_destroy(&r); return 1; }
    relay_bind_test_rig_pump(&r, 4);
    moq_publication_t ph = completion_publish(&r, pub, "v");
    (void)completion_subscribe(&r, sub, "v");
    moq_rcbuf_t *payload = NULL;
    const uint8_t bytes[3] = {0x91, 0x72, 0x53};
    C_CHECK(&r, moq_rcbuf_create(&a.vt, bytes, sizeof(bytes), &payload) == MOQ_OK);
    if (payload) {
        C_CHECK(&r, moq_session_send_pub_object_datagram(pub->peer, ph,
            0, 0, 128, false, payload, NULL, 0, r.now) == MOQ_OK);
        moq_rcbuf_decref(payload);
    }
    relay_bind_test_rig_pump(&r, 8);
    completion_finish(&r, pub, ph, 0);
    peer_state_t ps = {0};
    relay_bind_test_peer_drain(&r, sub, &ps, false, false, 0, 0);
    C_CHECK(&r, ps.objects == 1 && ps.done_count == 1 && !ps.session_closed);
    C_CHECK(&r, w.datagrams == 1 && w.opened == 0);
    C_CHECK(&r, w.terminals == 1 && w.counts[0] == 0);
    return completion_end(&r, &a, "datagrams-not-streams", &w);
}

static int
completion_cancel_race(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t r;
    if (relay_bind_test_rig_create(&r, &a) != MOQR_OK) return 1;
    completion_wire_t w;
    conn_t *pub = relay_bind_test_rig_connect(&r, version);
    conn_t *sub = completion_connect(&r, &w, version);
    if (!pub || !sub) { relay_bind_test_rig_destroy(&r); return 1; }
    relay_bind_test_rig_pump(&r, 4);
    moq_publication_t ph = completion_publish(&r, pub, "v");
    moq_subscription_t sh = completion_subscribe(&r, sub, "v");
    moq_finish_publish_cfg_t cfg;
    moq_finish_publish_cfg_init(&cfg);
    moqr_bind_debug_block_sub_done(8);
    C_CHECK(&r, moq_session_finish_publish(pub->peer, ph, &cfg, r.now) == MOQ_OK);
    relay_bind_test_rig_step_pair(&r, pub);
    relay_bind_test_rig_bind_pump_once(&r);
    C_CHECK(&r, r.bind->pending_count == 1);
    C_CHECK(&r, moqr_bind_debug_sub_done_blocks_left() < 8);
    C_CHECK(&r, moq_session_unsubscribe(sub->peer, sh, r.now) == MOQ_OK);
    /* A real DONE is already retained. Cancellation retires its owner before
     * that retry, so the ownerless terminal must be consumed without output. */
    relay_bind_test_rig_step_pair(&r, sub);
    moqr_bind_debug_block_sub_done(0);
    relay_bind_test_rig_bind_pump_once(&r);
    relay_bind_test_rig_pump(&r, 8);
    C_CHECK(&r, w.terminals == 0 && w.opened == 0);
    C_CHECK(&r, r.bind->pending_count == 0);
    moq_publication_t next = completion_publish(&r, pub, "next");
    (void)completion_subscribe(&r, sub, "next");
    completion_finish(&r, pub, next, 0);
    C_CHECK(&r, w.terminals == 1 && w.counts[0] == 0);
    return completion_end(&r, &a, "cancelled-owner-terminal-race", &w);
}

static int
completion_accept_blocked(moq_version_t version)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 1;
    rig_t r;
    if (relay_bind_test_rig_create(&r, &a) != MOQR_OK) { relay_bind_test_max_actions = 0; return 1; }
    completion_wire_t w;
    conn_t *pub = relay_bind_test_rig_connect(&r, version);
    conn_t *sub = completion_connect(&r, &w, version);
    relay_bind_test_max_actions = 0;
    if (!pub || !sub) { relay_bind_test_rig_destroy(&r); return 1; }
    relay_bind_test_rig_pump(&r, 4);
    moq_publication_t ph = completion_publish(&r, pub, "v");
    moq_bytes_t ns = B("count");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = &ns, .count = 1 };
    sc.track_name = B("v");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    C_CHECK(&r, moq_session_subscribe(sub->peer, &sc, r.now, &sh) == MOQ_OK);
    relay_bind_test_rig_step_pair(&r, sub);
    /* A lawful independent namespace request fills the one-slot action queue
     * before the relay processes SUBSCRIBE. Do not transport it until release. */
    moq_publish_namespace_cfg_t pc;
    moq_publish_namespace_cfg_init(&pc);
    moq_bytes_t filler = B("accounting-capacity-control");
    pc.track_namespace = (moq_namespace_t){ .parts = &filler, .count = 1 };
    moq_announcement_t ann;
    C_CHECK(&r, moq_session_publish_namespace(sub->rsess, &pc, r.now, &ann) == MOQ_OK);
    C_CHECK(&r, moq_session_action_capacity(sub->rsess) == 0);
    relay_bind_test_rig_bind_pump_once(&r);
    C_CHECK(&r, r.bind->pending_count == 1);
    uint64_t cookie = 0;
    if (r.bind->pending_count) {
        const moqr_intent_t *first = &r.bind->pending[r.bind->pending_head];
        C_CHECK(&r, first->kind == MOQR_INTENT_ACCEPT_SUB);
        cookie = first->cookie;
    }
    b_conn_t *cn = conn_by_session(r.bind, sub->rsess);
    C_CHECK(&r, cn && !dsub_find(r.bind, cn, cookie));
    moq_finish_publish_cfg_t done;
    moq_finish_publish_cfg_init(&done);
    C_CHECK(&r, moq_session_finish_publish(pub->peer, ph, &done, r.now) == MOQ_OK);
    relay_bind_test_rig_step_pair(&r, pub);
    relay_bind_test_rig_bind_pump_once(&r);
    /* The parked ACCEPT stays first and alone: the source end lets the
     * subscription finish its delivery, which waits behind the ACCEPT, so
     * its DONE can only follow it. */
    C_CHECK(&r, r.bind->pending_count == 1);
    if (r.bind->pending_count == 1) {
        const moqr_intent_t *first = &r.bind->pending[r.bind->pending_head];
        C_CHECK(&r, first->kind == MOQR_INTENT_ACCEPT_SUB && first->cookie == cookie);
    }
    C_CHECK(&r, w.accepts == 0 && w.terminals == 0);
    relay_bind_test_rig_pump(&r, 16);
    peer_state_t ps = {0};
    relay_bind_test_peer_drain(&r, sub, &ps, false, false, 0, 0);
    C_CHECK(&r, ps.subscribe_ok && ps.done_count == 1 && !ps.session_closed);
    C_CHECK(&r, w.accepts == 1 && w.terminals == 1 && w.counts[0] == 0);
    C_CHECK(&r, w.opened == 0 && r.bind->pending_count == 0);
    C_CHECK(&r, cn && !dsub_find(r.bind, cn, cookie));
    moq_location_filter_cfg_t filter;
    C_CHECK(&r, moq_session_subscription_filter(sub->peer, sh, &filter) == MOQ_ERR_STALE_HANDLE);
    return completion_end(&r, &a, "blocked-ACCEPT-then-source-DONE", &w);
}

static int
completion_fill(bool live)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t r;
    if (relay_bind_test_rig_create(&r, &a) != MOQR_OK) return 1;
    d22_track_t t;
    relay_bind_test_d22_hole_setup(&r, &t);
    completion_wire_t w;
    conn_t *sub = completion_connect(&r, &w, MOQ_VERSION_DRAFT_22);
    if (!sub) { relay_bind_test_rig_destroy(&r); return 1; }
    relay_bind_test_rig_pump(&r, 4);
    moq_subscription_t h;
    C_CHECK(&r, relay_bind_test_d22_subscribe_fill(&r, sub, &t, true, 0, &h) == MOQ_OK);
    relay_bind_test_rig_pump(&r, 8);
    peer_state_t ps = {0};
    relay_bind_test_peer_drain(&r, sub, &ps, false, false, 0, 0);
    C_CHECK(&r, ps.fill_objects == 5 && ps.fill_terminals == 1);
    C_CHECK(&r, w.opened == 1);
    if (live) {
        relay_bind_test_se_send_group(&r, t.pub, false, t.pub_ps.up_sub, (moq_publication_t){0}, 2, 1);
        relay_bind_test_rig_pump(&r, 6);
    }
    moq_done_subscribe_cfg_t cfg;
    moq_done_subscribe_cfg_init(&cfg);
    cfg.stream_count = live ? 3 : 2;
    C_CHECK(&r, moq_session_done_subscribe(t.pub->peer, t.pub_ps.up_sub, &cfg, r.now) == MOQ_OK);
    relay_bind_test_rig_pump(&r, 16);
    relay_bind_test_peer_drain(&r, sub, &ps, false, false, 0, 0);
    C_CHECK(&r, ps.objects == (live ? 1 : 0));
    C_CHECK(&r, ps.done_count == 1 && !ps.session_closed);
    C_CHECK(&r, w.opened == (live ? 2u : 1u));
    C_CHECK(&r, w.terminals == 1 && w.counts[0] == w.opened);
    return completion_end(&r, &a, live ? "fill-plus-live" : "fill-only", &w);
}

static int
completion_boundary(moq_version_t version, uint8_t draft, uint64_t unknown)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t r;
    if (relay_bind_test_rig_create(&r, &a) != MOQR_OK) return 1;
    completion_wire_t w;
    conn_t *pub = relay_bind_test_rig_connect(&r, version);
    conn_t *sub = completion_connect(&r, &w, version);
    if (!pub || !sub) { relay_bind_test_rig_destroy(&r); return 1; }
    relay_bind_test_rig_pump(&r, 4);
    (void)completion_publish(&r, pub, "v");
    (void)completion_subscribe(&r, sub, "v");
    C_CHECK(&r, bind_stream_count_next(0, draft) == 1);
    C_CHECK(&r, bind_stream_count_next(unknown - 2, draft) == unknown - 1);
    C_CHECK(&r, bind_stream_count_next(unknown - 1, draft) == unknown);
    C_CHECK(&r, bind_stream_count_next(unknown, draft) == unknown);
    C_CHECK(&r, bind_stream_count_next(UINT64_MAX, draft) == unknown);
    b_conn_t *cn = conn_by_session(r.bind, sub->rsess);
    C_CHECK(&r, cn != NULL);
    b_dsub_t *ds = NULL;
    if (cn) for (uint32_t i = 0; i < r.bind->n_dsubs; i++) {
        if (cn->dsubs[i].used) { ds = &cn->dsubs[i]; break; }
    }
    C_CHECK(&r, ds != NULL);
    if (ds) {
        /* Exercise the public encoder using the pure counter's sentinel.
         * This is not a fabricated live binding count or a traffic claim. */
        moq_done_subscribe_cfg_t cfg;
        moq_done_subscribe_cfg_init(&cfg);
        cfg.stream_count = bind_stream_count_next(unknown - 1, draft);
        C_CHECK(&r, moq_session_done_subscribe(sub->rsess,
            (moq_subscription_t){ds->sub_raw}, &cfg, r.now) == MOQ_OK);
        relay_bind_test_rig_pump(&r, 6);
        C_CHECK(&r, w.terminals == 1 && w.counts[0] == unknown);
        C_CHECK(&r, w.opened == 0);
    }
    return completion_end(&r, &a, "saturation-and-wire-sentinel", &w);
}

int
main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "retained") == 0) {
        return dataplane_source_end_oracle(false, 0) +
               dataplane_source_end_oracle(true, 0) +
               dataplane_source_end_oracle(false, 1) +
               dataplane_source_end_oracle(true, 1) +
               sub_done_resets_begun_subgroup() +
               draft22_update_answer_before_sub_done(false) +
               draft22_update_answer_before_sub_done(true);
    }
    int failures = 0;
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (unsigned i = 0; i < 3; i++) {
        failures += completion_simple(versions[i], 0, false);
        failures += completion_simple(versions[i], 1, false);
        failures += completion_simple(versions[i], 5, false);
        failures += completion_simple(versions[i], 2, true);
        failures += completion_independent(versions[i]);
        failures += completion_blocked_open(versions[i]);
        failures += completion_empty(versions[i]);
        failures += completion_datagrams(versions[i]);
        failures += completion_cancel_race(versions[i]);
        failures += completion_accept_blocked(versions[i]);
        failures += completion_boundary(versions[i], (uint8_t)i,
            i == 2 ? UINT64_MAX : (UINT64_C(1) << 62) - 1);
    }
    failures += completion_fill(false);
    failures += completion_fill(true);
    failures += completion_reset();
    printf("completion-stream-count cases=%u checks=%u failures=%d\n",
           completion_cases, completion_checks, failures);
    return failures ? 1 : 0;
}
