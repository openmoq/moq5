/*
 * Pico-WT receive-backpressure lifecycle.
 *
 * Covers what happens to a PAUSED stream -- one holding retained inbound bytes
 * with its receive window frozen -- when the stream terminates, when the
 * connection is torn down, and before the connection is ready to carry a grant.
 * The capacity-invariance workload lives in test_pico_wt_backpressure.c; this
 * file is about the state transitions around it.
 *
 * Retention buffers are allocated through the connection's own allocator, so
 * lifetime properties are asserted by allocator accounting rather than by
 * inspection: every retained byte must be released exactly once.
 */
#include "pico_wt_harness.h"
#include "pico_wt_test_seam.h"
#include "../pico_wt_adapter.h"

#include "picoquic_internal.h"

#include <moq/pico_wt.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>
#include <moq/control.h>
#include <moq/buf.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_failures;
#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
            g_failures++;                                                    \
        }                                                                    \
    } while (0)

/* Enough to overrun the advertised uni window (65535) and force a pause. */
#define OBJ_COUNT 128
#define OBJ_BYTES 1024

/* -- Counting allocator ---------------------------------------------- */

typedef struct {
    size_t live;        /* outstanding allocations */
    size_t bytes_live;  /* outstanding bytes */
    size_t allocs;
    size_t frees;
} acct_t;

static acct_t g_acct;

static void *acct_alloc(size_t n, void *ctx)
{
    (void)ctx;
    void *p = moq_alloc_default()->alloc(n, moq_alloc_default()->ctx);
    if (p) { g_acct.live++; g_acct.bytes_live += n; g_acct.allocs++; }
    return p;
}

static void *acct_realloc(void *p, size_t old, size_t nw, void *ctx)
{
    (void)ctx;
    void *r = moq_alloc_default()->realloc(p, old, nw,
                                           moq_alloc_default()->ctx);
    if (r) {
        if (p == NULL) { g_acct.live++; g_acct.allocs++; }
        else           { g_acct.bytes_live -= old; }
        g_acct.bytes_live += nw;
    }
    return r;
}

static void acct_free(void *p, size_t n, void *ctx)
{
    (void)ctx;
    if (p) { g_acct.live--; g_acct.bytes_live -= n; g_acct.frees++; }
    moq_alloc_default()->free(p, n, moq_alloc_default()->ctx);
}

/* Route the receiver's retention allocations through the accounting hooks.
 * Installed after setup and before any data arrives, so every retained buffer
 * is covered. */
static void install_acct(moq_pico_wt_conn_t *c)
{
    memset(&g_acct, 0, sizeof(g_acct));
    c->alloc.alloc = acct_alloc;
    c->alloc.realloc = acct_realloc;
    c->alloc.free = acct_free;
    c->alloc.ctx = NULL;
}

/* -- Shared setup: subscribe, then fill until the receiver pauses ----- */

typedef struct {
    pico_wt_harness_t h;
    moq_subscription_t client_sub;
    moq_subscription_t server_sub;
    moq_subgroup_handle_t sg;
    uint64_t sid;          /* the subgroup's uni stream */
    uint32_t published;
    bool     closed_ok;    /* close_subgroup was ACCEPTED, not merely attempted */
    bool     ok;
} fixture_t;

static uint64_t find_subgroup_stream(picoquic_cnx_t *cnx)
{
    picoquic_stream_head_t *best = NULL;
    for (picoquic_stream_head_t *s = picoquic_first_stream(cnx); s != NULL;
         s = picoquic_next_stream(s)) {
        if ((s->stream_id & 2) == 0) continue;
        if (best == NULL || s->consumed_offset > best->consumed_offset)
            best = s;
    }
    return best ? best->stream_id : UINT64_MAX;
}

/* The receiver is paused on `sid`: it holds retained work and its window is
 * frozen, so the peer cannot make progress. */
static bool receiver_paused(fixture_t *f)
{
    if (f->sid == UINT64_MAX) return false;
    for (size_t i = 0; i < f->h.client_conn->rx_count; i++) {
        pw_rx_stream_t *st = &f->h.client_conn->rx[i];
        if (st->active && st->stream_id == f->sid && st->paused)
            return true;
    }
    return false;
}

static pw_rx_stream_t *rx_entry(fixture_t *f, uint64_t sid)
{
    for (size_t i = 0; i < f->h.client_conn->rx_count; i++)
        if (f->h.client_conn->rx[i].active &&
            f->h.client_conn->rx[i].stream_id == sid)
            return &f->h.client_conn->rx[i];
    return NULL;
}

/* Bring up a pair, subscribe, and publish until the receiving side is paused
 * with retained bytes. The application never drains, which is what keeps the
 * stream paused. */
static bool fixture_up_to_pause_sz(fixture_t *f, uint8_t cid,
                                   uint32_t max_events,
                                   bool close_subgroup_at_end,
                                   size_t obj_bytes)
{
    memset(f, 0, sizeof(*f));
    f->sid = UINT64_MAX;
    pico_wt_harness_cfg_t cfg = { .cid_byte = cid, .request_capacity = 10,
                                  .max_events = max_events };
    if (pico_wt_harness_setup(&f->h, &cfg) != 0)
        return false;
    /* Before the handshake: the adapter allocates its per-stream table on the
     * first tracked stream, and that happens during CONNECT. */
    install_acct(f->h.client_conn);
    if (pico_wt_harness_handshake(&f->h) != 0)
        return false;

    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = { {(const uint8_t *)"pico", 4},
                         {(const uint8_t *)"wt", 2} };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){(const uint8_t *)"video", 5};
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    if (moq_session_subscribe(f->h.client_session, &sc, 0, &f->client_sub) < 0)
        return false;
    moq_pico_wt_service(f->h.client_conn, f->h.now);
    if (pico_wt_harness_pump(&f->h, 4000) != 0) return false;

    f->server_sub = MOQ_SUBSCRIPTION_INVALID;
    {
        moq_event_t ev;
        while (moq_session_poll_events(f->h.server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                f->server_sub = ev.u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                if (moq_session_accept_subscribe(f->h.server_session,
                                                 f->server_sub, &acfg, 0) < 0)
                    f->server_sub = MOQ_SUBSCRIPTION_INVALID;
            }
            moq_event_cleanup(&ev);
        }
    }
    if (!moq_subscription_is_valid(f->server_sub)) return false;
    moq_pico_wt_service(f->h.server_conn, f->h.now);
    if (pico_wt_harness_pump(&f->h, 4000) != 0) return false;

    /* Drain the client once so the run starts from an empty queue; after this
     * the application deliberately stops polling. */
    {
        moq_event_t ev;
        while (moq_session_poll_events(f->h.client_session, &ev, 1) > 0)
            moq_event_cleanup(&ev);
    }

    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.publisher_priority = 200;
    if (moq_session_open_subgroup(f->h.server_session, f->server_sub, &sgcfg, 0,
                                  &f->sg) < 0)
        return false;

    /*
     * Build the send backlog WITHOUT pumping the simulation, so the whole
     * workload is queued before any of it moves. When the pump starts, the
     * receiver pauses after its first object while the rest of the window is
     * still in flight -- those in-flight bytes are what the adapter must
     * retain, and retention is the subject of every assertion below.
     */
    uint8_t *payload = (uint8_t *)malloc(obj_bytes);
    if (payload == NULL) return false;
    {
        uint32_t i = 0;
        for (int spin = 0; spin < 4000 && i < OBJ_COUNT; spin++) {
            memset(payload, (int)(i & 0xff), obj_bytes);
            moq_rcbuf_t *buf = NULL;
            if (moq_rcbuf_create(moq_alloc_default(), payload, obj_bytes,
                                 &buf) < 0 || buf == NULL)
                { free(payload); return false; }
            moq_result_t wr = moq_session_write_object(f->h.server_session,
                                                       f->sg, i, buf, 0);
            moq_rcbuf_decref(buf);
            if (wr >= 0) { i++; f->published = i; }
            else if (wr != MOQ_ERR_WOULD_BLOCK) break;
            moq_pico_wt_service(f->h.server_conn, f->h.now);
        }
    }
    free(payload);

    for (int round = 0; round < 4000; round++) {
        moq_pico_wt_service(f->h.server_conn, f->h.now);
        if (pico_wt_harness_pump(&f->h, 100) != 0) break;
        if (f->sid == UINT64_MAX)
            f->sid = find_subgroup_stream(f->h.test_ctx->cnx_client);
        if (receiver_paused(f)) {
            /* The pause begins while the peer still holds unspent credit, so
             * bytes remain in flight. Let them land: those are precisely the
             * in-window bytes the adapter must retain, and the retention is
             * what the lifecycle assertions below are about. */
            for (int k = 0; k < 30; k++) {
                moq_pico_wt_service(f->h.server_conn, f->h.now);
                if (pico_wt_harness_pump(&f->h, 50) != 0) break;
            }
            break;
        }
    }
    if (f->sid == UINT64_MAX)
        f->sid = find_subgroup_stream(f->h.test_ctx->cnx_client);

    if (close_subgroup_at_end) {
        /* close_subgroup may WOULD_BLOCK behind a full action queue; the FIN is
         * the point of this fixture, so retry until it is ACCEPTED rather than
         * proceeding on an unsent close. */
        for (int k = 0; k < 400 && !f->closed_ok; k++) {
            moq_result_t cr = moq_session_close_subgroup(f->h.server_session,
                                                         f->sg, 0);
            if (cr >= 0) { f->closed_ok = true; break; }
            if (cr != MOQ_ERR_WOULD_BLOCK) break;
            moq_pico_wt_service(f->h.server_conn, f->h.now);
            if (pico_wt_harness_pump(&f->h, 50) != 0) break;
        }
        moq_pico_wt_service(f->h.server_conn, f->h.now);
        (void)pico_wt_harness_pump(&f->h, 100);
    }

    f->ok = receiver_paused(f);
    return f->ok;
}

static bool fixture_up_to_pause(fixture_t *f, uint8_t cid, uint32_t max_events,
                                bool close_subgroup_at_end)
{
    return fixture_up_to_pause_sz(f, cid, max_events, close_subgroup_at_end,
                                  OBJ_BYTES);
}

/* Drain the receiver to a local fixed point WITHOUT advancing transport time,
 * then pump. Returns objects delivered. */
static uint32_t drain_and_pump(fixture_t *f, int rounds, uint32_t *out_order_ok,
                               uint64_t *next_expected)
{
    uint32_t got = 0;
    for (int r = 0; r < rounds; r++) {
        for (;;) {
            uint32_t before = got;
            moq_event_t ev;
            while (moq_session_poll_events(f->h.client_session, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                    if (next_expected) {
                        if (ev.u.object_received.object_id != *next_expected &&
                            out_order_ok)
                            *out_order_ok = 0;
                        *next_expected = ev.u.object_received.object_id + 1;
                    }
                    got++;
                }
                moq_event_cleanup(&ev);
            }
            moq_pico_wt_service(f->h.client_conn, f->h.now);
            if (got == before) break;
        }
        if (pico_wt_harness_pump(&f->h, 100) != 0) break;
    }
    return got;
}

/* -- Adapter observers (test build only) ------------------------------- *
 * The adapter reports every stream-event callback boundary, every local
 * STOP_SENDING it issues and every receive-credit grant it hands picoquic.
 * Records carry the adapter connection, so client and server sides are told
 * apart by identity, never by output interleaving. */

#define OBS_LOCAL_STOP (-1)
typedef struct {
    moq_pico_wt_conn_t *conn;
    int      event;          /* picohttp_call_back_event_t, or OBS_LOCAL_STOP */
    uint64_t sid;
    int      phase;          /* 0 entry, 1 exit (callbacks only) */
    bool     entry_present;  /* receive entry for sid present at this point */
    size_t   entry_buf_len;
    uint64_t entry_delivered; /* callback bytes delivered to that entry so far */
    int      grants;         /* grants recorded for (conn, sid) so far */
    size_t   acct_live;      /* allocator accounting at this point */
} obs_rec_t;
#define OBS_MAX 512
#define GRANT_KEYS_MAX 64
static obs_rec_t g_obs[OBS_MAX];
static int g_obs_n;
static struct { moq_pico_wt_conn_t *conn; uint64_t sid; int n; } g_grant_tab[GRANT_KEYS_MAX];
static int g_grant_n;
/* Sticky: once either recorder could not keep a record, the inventory is
 * incomplete and no row may treat it as proof. */
static bool g_obs_overflow, g_grant_overflow;
static bool observers_complete(void) { return !g_obs_overflow && !g_grant_overflow; }

static int grants_for(moq_pico_wt_conn_t *c, uint64_t sid)
{
    for (int i = 0; i < g_grant_n; i++)
        if (g_grant_tab[i].conn == c && g_grant_tab[i].sid == sid) return g_grant_tab[i].n;
    return 0;
}

static pw_rx_stream_t *entry_of(moq_pico_wt_conn_t *c, uint64_t sid)
{
    for (size_t i = 0; i < c->rx_count; i++)
        if (c->rx[i].active && c->rx[i].stream_id == sid) return &c->rx[i];
    return NULL;
}

static void obs_record(moq_pico_wt_conn_t *c, int event, uint64_t sid, int phase)
{
    if (g_obs_n >= OBS_MAX) { g_obs_overflow = true; return; }
    obs_rec_t *r = &g_obs[g_obs_n++];
    pw_rx_stream_t *e = entry_of(c, sid);
    r->conn = c; r->event = event; r->sid = sid; r->phase = phase;
    r->entry_present = e != NULL;
    r->entry_buf_len = e ? e->buf_len : 0;
    r->entry_delivered = e ? e->delivered : 0;
    r->grants = grants_for(c, sid);
    r->acct_live = g_acct.live;
}
static void obs_callback(moq_pico_wt_conn_t *c, int event, uint64_t sid, int phase)
    { obs_record(c, event, sid, phase); }
static void obs_local_stop(moq_pico_wt_conn_t *c, uint64_t sid)
    { obs_record(c, OBS_LOCAL_STOP, sid, 0); }
static void obs_grant(moq_pico_wt_conn_t *c, uint64_t sid, uint64_t headroom)
{
    (void)headroom;
    for (int i = 0; i < g_grant_n; i++)
        if (g_grant_tab[i].conn == c && g_grant_tab[i].sid == sid) { g_grant_tab[i].n++; return; }
    if (g_grant_n < GRANT_KEYS_MAX) { g_grant_tab[g_grant_n].conn = c; g_grant_tab[g_grant_n].sid = sid; g_grant_tab[g_grant_n].n = 1; g_grant_n++; }
    else g_grant_overflow = true;
}

static void observers_install(void)
{
    g_obs_n = 0; g_grant_n = 0;
    g_obs_overflow = false; g_grant_overflow = false;
    memset(g_obs, 0, sizeof(g_obs)); memset(g_grant_tab, 0, sizeof(g_grant_tab));
    pw_test_callback_observer = obs_callback;
    pw_test_local_stop_observer = obs_local_stop;
    pw_test_grant_observer = obs_grant;
}
static void observers_remove(void)
{
    pw_test_callback_observer = NULL;
    pw_test_local_stop_observer = NULL;
    pw_test_grant_observer = NULL;
}

/* The recorders fail closed: past their capacity they set the sticky flag
 * instead of silently truncating. Driven directly, without a connection. */
static void test_observer_recorders_fail_closed(void)
{
    moq_pico_wt_conn_t probe_conn;
    memset(&probe_conn, 0, sizeof(probe_conn));   /* rx_count 0: no entries to look up */
    observers_install();
    CHECK(observers_complete());
    for (int i = 0; i < OBS_MAX; i++) obs_record(&probe_conn, (int)picohttp_callback_post_data, 4, 0);
    CHECK(g_obs_n == OBS_MAX && observers_complete());      /* full, still complete */
    obs_record(&probe_conn, (int)picohttp_callback_post_data, 4, 1);
    CHECK(g_obs_n == OBS_MAX && g_obs_overflow && !observers_complete());   /* one more: sticky */
    observers_install();
    for (uint64_t sid = 0; sid < GRANT_KEYS_MAX; sid++) obs_grant(&probe_conn, sid * 4, 1);
    CHECK(g_grant_n == GRANT_KEYS_MAX && observers_complete());
    obs_grant(&probe_conn, 4, 1);                           /* an existing key still counts */
    CHECK(grants_for(&probe_conn, 4) == 2 && observers_complete());
    obs_grant(&probe_conn, (uint64_t)GRANT_KEYS_MAX * 4, 1); /* a new key past capacity: sticky */
    CHECK(g_grant_n == GRANT_KEYS_MAX && g_grant_overflow && !observers_complete());
    observers_remove();
}

/* first record index matching (conn, event, sid, phase), or -1 */
static int obs_find(moq_pico_wt_conn_t *c, int event, uint64_t sid, int phase)
{
    for (int i = 0; i < g_obs_n; i++)
        if (g_obs[i].conn == c && g_obs[i].event == event && g_obs[i].sid == sid && g_obs[i].phase == phase)
            return i;
    return -1;
}

/* ==================================================================== */

/*
 * Pre-READY owed grant.
 *
 * picoquic_open_flow_control() silently no-ops unless the connection is exactly
 * READY (picoquic/picoquic/sender.c). A grant attempted earlier must therefore
 * be treated as OWED -- `granted` must not advance, or the window would be
 * recorded as raised while no frame was ever emitted, and the peer would stall
 * forever. The post-service sweep must then issue it once READY.
 */
static void test_pre_ready_grant_is_owed(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x31, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    picoquic_cnx_t *cnx = h.test_ctx->cnx_client;
    CHECK(picoquic_get_cnx_state(cnx) == picoquic_state_ready);

    /* A real stream the adapter can own the window for. */
    uint64_t sid = 3;
    (void)picoquic_add_to_stream(cnx, sid, (const uint8_t *)"x", 1, 0);
    CHECK(picoquic_set_app_flow_control(cnx, sid, 1) == 0);

    pw_rx_stream_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.active = true;
    probe.app_fc = true;
    probe.stream_id = sid;
    probe.budget = 65535;
    probe.delivered = 40000;      /* enough owed to clear the budget/2 batch */

    moq_pico_wt_conn_t *c = h.client_conn;
    pw_rx_stream_t *saved_rx = c->rx;
    size_t saved_count = c->rx_count, saved_cap = c->rx_cap;
    c->rx = &probe; c->rx_count = 1; c->rx_cap = 1;

    /* NOT READY: picoquic_open_flow_control() silently emits nothing unless the
     * connection is exactly READY, so the grant must be left OWED. Recording it
     * as issued would strand the peer behind a window that was never raised. */
    picoquic_state_enum saved_state = cnx->cnx_state;
    cnx->cnx_state = picoquic_state_client_ready_start;
    moq_pico_wt_service(c, h.now);
    CHECK(probe.granted == 0);

    /* READY: the same post-service sweep issues the owed grant. */
    cnx->cnx_state = saved_state;
    moq_pico_wt_service(c, h.now);
    CHECK(probe.granted > 0);

    c->rx = saved_rx; c->rx_count = saved_count; c->rx_cap = saved_cap;
    pico_wt_harness_cleanup(&h);
}

/*
 * FIN while paused: the retained bytes replay in order, the FIN follows them,
 * the entry retires, and no credit is granted to a finished stream (picoquic
 * skips fin_received streams in its own window loop, so a grant here would be
 * both useless and a bound violation).
 */
static void test_fin_while_paused(void)
{
    fixture_t f;
    if (!fixture_up_to_pause(&f, 0x32, 1, true)) {
        CHECK(0);
        pico_wt_harness_cleanup(&f.h);
        return;
    }
    CHECK(receiver_paused(&f));

    uint32_t order_ok = 1;
    uint64_t next = 0;
    uint32_t got = drain_and_pump(&f, 400, &order_ok, &next);

    CHECK(f.closed_ok);              /* the close was accepted, not just tried */
    CHECK(got == f.published);       /* every retained byte replayed */
    CHECK(order_ok);                 /* in publication order */
    CHECK(!moq_pico_wt_conn_is_fatal(f.h.client_conn));

    /* The peer's FIN really arrived: picoquic recorded it on the receive
     * stream, or the stream is already gone because it completed. Retirement is
     * only meaningful once the FIN is on the wire. */
    {
        picoquic_stream_head_t *rs =
            picoquic_find_stream(f.h.test_ctx->cnx_client, f.sid);
        CHECK(rs == NULL || rs->fin_received);
    }

    /* The stream retired: no live rx entry remains for it. */
    CHECK(rx_entry(&f, f.sid) == NULL);

    /*
     * No credit may be granted to a finished stream. Keep servicing and
     * pumping, then require the stream to be fully retired on BOTH sides: the
     * adapter has no entry, and picoquic has deleted the stream, so there is
     * nothing left to grant against.
     *
     * The transport-side half of this guarantee is upstream, not adapter logic:
     * picoquic skips fin_received streams in its own window loop and deletes
     * the stream once it is fully consumed. Asserting a frozen maxdata_local
     * here would be vacuous, because the stream object is already gone.
     */
    for (int i = 0; i < 40; i++) {
        moq_pico_wt_service(f.h.client_conn, f.h.now);
        if (pico_wt_harness_pump(&f.h, 50) != 0) break;
    }
    CHECK(rx_entry(&f, f.sid) == NULL);
    CHECK(picoquic_find_stream(f.h.test_ctx->cnx_client, f.sid) == NULL);
    CHECK(!moq_pico_wt_conn_is_fatal(f.h.client_conn));

    pico_wt_harness_cleanup(&f.h);
}

/*
 * Expected bridge-owned object inventory, derived independently of the
 * receiver under test. The fixture's subgroup wire layout is deterministic:
 * the publisher encodes a draft-16 subgroup header (type 0x14: subgroup id
 * present, explicit priority) followed, per object, by the object-id delta,
 * the payload length and the payload. The test re-encodes that layout with
 * the public codec for the fixture's values (group 0, subgroup 0, priority
 * 200, consecutive ids from 0, OBJ_BYTES bytes each) and asks how many whole
 * objects fit in the first `fed` bytes -- the bytes the adapter handed the
 * bridge (callback-delivered bytes minus the tail it still held) at the
 * ownership boundary. The track alias is the one value the fixture does not
 * pin; any alias below 64 encodes in one byte, which the single subscription
 * of this fixture guarantees. Returns -1 when the layout cannot be encoded. */
static int expected_objects_in(uint64_t fed, uint32_t published)
{
    uint8_t tmp[64];
    moq_buf_writer_t w;
    moq_d16_subgroup_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.type = 0x14;
    hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
    hdr.track_alias = 0;
    hdr.group_id = 0;
    hdr.subgroup_id = 0;
    hdr.publisher_priority = 200;
    moq_buf_writer_init(&w, tmp, sizeof(tmp));
    if (moq_d16_encode_subgroup_header(&w, &hdr) < 0) return -1;
    uint64_t off = moq_buf_writer_offset(&w);
    int n = 0;
    for (uint32_t i = 0; i < published; i++) {
        moq_buf_writer_init(&w, tmp, sizeof(tmp));
        /* object-id delta: the first object carries its id (0), each later one
         * id - previous - 1, which is 0 for the fixture's consecutive ids */
        if (moq_d16_encode_object_header(&w, 0, OBJ_BYTES) < 0) return -1;
        off += moq_buf_writer_offset(&w) + OBJ_BYTES;
        if (off > fed) break;
        n++;
    }
    return n;
}

/*
 * RESET while paused, over the real transport. The publisher resets the
 * subgroup stream while the receiver holds retained bytes (adapter-owned,
 * never fed) behind input the session already retains (bridge-owned), with an
 * application that has not polled.
 *
 * Two ownership layers, two outcomes. At the reset callback's return the
 * adapter's own layer is settled: its retained bytes and the receive entry
 * are released, nothing is fatal, and no receive credit follows. The bridge-
 * owned layer settles in order once the application drains: exactly the
 * whole objects that fit in the bytes the bridge was given (an expectation
 * computed from the wire layout, not from what the receiver emits) arrive
 * first, each with the fixture's payload pattern, then exactly one
 * SUBGROUP_RESET for this subscription/subgroup carrying the publisher's
 * error code, after which the bridge reports no pending state and further
 * service adds nothing. The adapter-dropped tail never arrives.
 */
static void test_reset_while_paused(void)
{
    fixture_t f;
    if (!fixture_up_to_pause(&f, 0x33, 1, false)) {
        CHECK(0);
        pico_wt_harness_cleanup(&f.h);
        return;
    }
    CHECK(receiver_paused(&f));
    moq_pico_wt_conn_t *c = f.h.client_conn;
    pw_rx_stream_t *st = rx_entry(&f, f.sid);
    CHECK(st != NULL && st->buf_len > 0);   /* bytes really were retained */
    CHECK(g_acct.live == 2);                /* table + retention buffer */
    CHECK(moq_transport_bridge_stream_has_pending(c->bridge, f.sid));   /* bridge-owned work queued */

    observers_install();
    const uint64_t RESET_CODE = 0x7;
    int grants_before = grants_for(c, f.sid);

    /* The publisher resets the subgroup; the RESET_STREAM crosses the
     * simulated transport into the receiver's production callback. */
    CHECK(moq_session_reset_subgroup(f.h.server_session, f.sg, RESET_CODE, f.h.now) == MOQ_OK);
    int exit_idx = -1;
    for (int i = 0; i < 400 && exit_idx < 0; i++) {
        moq_pico_wt_service(f.h.server_conn, f.h.now);
        if (pico_wt_harness_pump(&f.h, 20) != 0) break;
        exit_idx = obs_find(c, (int)picohttp_callback_reset, f.sid, 1);
    }
    int entry_idx = obs_find(c, (int)picohttp_callback_reset, f.sid, 0);
    CHECK(exit_idx >= 0);                   /* the reset reached the receiver's callback */
    CHECK(entry_idx >= 0 && entry_idx < exit_idx);
    CHECK(observers_complete());
    if (exit_idx < 0 || entry_idx < 0 || entry_idx >= exit_idx || !observers_complete()) {
        /* prerequisite failed: unwind without touching the records */
        observers_remove();
        pico_wt_harness_cleanup(&f.h);
        return;
    }
    const obs_rec_t *at_entry = &g_obs[entry_idx], *at_exit = &g_obs[exit_idx];

    /* Adapter layer, at the callback's return: entry and retained bytes gone. */
    CHECK(at_entry->entry_present && at_entry->entry_buf_len > 0);
    CHECK(!at_exit->entry_present);
    CHECK(at_exit->acct_live == at_entry->acct_live - 1);   /* exactly the retention buffer */
    CHECK(rx_entry(&f, f.sid) == NULL);
    CHECK(!moq_pico_wt_conn_is_fatal(c));
    CHECK(at_exit->grants == grants_before);

    /* Ownership boundary: the bytes the bridge was given are what picoquic
     * delivered to the callback minus what the adapter still held. The
     * expected inventory is the whole objects that fit in them. */
    CHECK(at_entry->entry_delivered >= at_entry->entry_buf_len);
    uint64_t fed = at_entry->entry_delivered - at_entry->entry_buf_len;
    int expected = expected_objects_in(fed, f.published);
    printf("reset-while-paused: delivered=%llu held=%zu fed=%llu published=%u expected_objects=%d\n",
           (unsigned long long)at_entry->entry_delivered, at_entry->entry_buf_len, (unsigned long long)fed,
           f.published, expected);
    CHECK(expected >= 1 && (uint32_t)expected < f.published);   /* some accepted, the tail excluded */

    /* Bridge layer: bounded poll/service to a fixed point. */
    enum { INV_MAX = OBJ_COUNT + 8 };
    uint64_t ids[INV_MAX];
    int n_ids = 0, resets = 0, other = 0, reset_after_last_object = -1, bad_payload = 0, wrong_sub = 0;
    bool inventory_overflow = false;
    uint64_t reset_code_seen = UINT64_MAX;
    bool reset_matches_sub = false;
    bool pending_cleared = false;
    for (int round = 0; round < 400 && !(pending_cleared && resets > 0); round++) {
        moq_event_t ev;
        while (moq_session_poll_events(f.h.client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                const moq_object_received_event_t *o = &ev.u.object_received;
                if (n_ids < INV_MAX) ids[n_ids] = o->object_id; else inventory_overflow = true;
                if (!moq_subscription_eq(o->sub, f.client_sub) || o->group_id != 0 || o->subgroup_id != 0) wrong_sub++;
                bool good = o->payload != NULL && moq_rcbuf_len(o->payload) == OBJ_BYTES;
                if (good) {
                    const uint8_t *d = moq_rcbuf_data(o->payload);
                    for (size_t k = 0; k < OBJ_BYTES; k++)
                        if (d[k] != (uint8_t)(o->object_id & 0xff)) { good = false; break; }
                }
                if (!good) bad_payload++;
                n_ids++;
            } else if (ev.kind == MOQ_EVENT_SUBGROUP_RESET) {
                resets++;
                reset_code_seen = ev.u.subgroup_reset.error_code;
                reset_matches_sub = moq_subscription_eq(ev.u.subgroup_reset.sub, f.client_sub) &&
                                    ev.u.subgroup_reset.group_id == 0 && ev.u.subgroup_reset.subgroup_id == 0;
                reset_after_last_object = n_ids;
            } else {
                other++;
            }
            moq_event_cleanup(&ev);
        }
        moq_pico_wt_service(c, f.h.now);
        pending_cleared = !moq_transport_bridge_stream_has_pending(c->bridge, f.sid);
    }
    CHECK(pending_cleared);                 /* a timeout here is a failure, not settlement */
    CHECK(!inventory_overflow);             /* the finite inventory represented every observation */
    CHECK(resets == 1);
    CHECK(reset_matches_sub && reset_code_seen == RESET_CODE);
    CHECK(reset_after_last_object == n_ids);   /* the terminal came after every retained object */
    /* exact set: the expected whole objects, ids 0..expected-1 in order, each
     * with the fixture's payload, on this subscription/group/subgroup */
    CHECK(n_ids == expected);
    bool exact = n_ids == expected && !inventory_overflow;
    for (int i = 0; exact && i < n_ids; i++) if (ids[i] != (uint64_t)i) exact = false;
    CHECK(exact);
    CHECK(bad_payload == 0 && wrong_sub == 0);
    CHECK(other == 0);

    /* More service adds nothing: no duplicate terminal, no credit. */
    int extra_resets = 0, extra_objects = 0;
    for (int round = 0; round < 20; round++) {
        moq_event_t ev;
        while (moq_session_poll_events(f.h.client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBGROUP_RESET) extra_resets++;
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) extra_objects++;
            moq_event_cleanup(&ev);
        }
        moq_pico_wt_service(c, f.h.now);
        CHECK(pico_wt_harness_pump(&f.h, 20) == 0);   /* the quiet tail must actually be pumped */
    }
    CHECK(extra_resets == 0 && extra_objects == 0);
    CHECK(observers_complete());
    CHECK(grants_for(c, f.sid) == grants_before);
    CHECK(rx_entry(&f, f.sid) == NULL);
    CHECK(!moq_pico_wt_conn_is_fatal(c));
    CHECK(g_acct.live == 1);                /* only the receive table remains */

    observers_remove();
    pico_wt_harness_cleanup(&f.h);
}

/*
 * STOP_SENDING, part 1 of 2: the real transport path on a real BIDIRECTIONAL
 * stream, and the boundary between STOP alone and a later protocol terminal.
 *
 * A draft-18 FETCH request rides a client-initiated bidi, so the client tracks
 * it as a PW_RX_BIDI receive entry. The server aborts the client's SENDING
 * half with a genuine STOP_SENDING through its endpoint vtable. What follows
 * on the wire is the protocol's own cancellation: the client resets its send
 * half, the publisher resets the FETCH data stream, the fetcher's FETCH state
 * ends on that data-stream reset and it terminates the request stream's
 * remaining direction with its own STOP_SENDING.
 *
 * Pinned, by callback-boundary observation on the CLIENT adapter (the two
 * sides are told apart by connection identity): at the exit of the peer-STOP
 * callback the receive entry is still present and no receive credit was
 * granted for it -- STOP alone retires nothing; the entry is retired only by
 * the client's own local STOP, which comes after the data-stream RESET
 * callback; afterwards no credit is granted to the retired stream; and the
 * application sees exactly one FETCH_RESET for its fetch handle with
 * data_stream set. The retained-state invariant is proved separately in part 2.
 */
static void test_stop_sending_on_real_request_bidi(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x37, .request_capacity = 10,
                                  .max_events = 4,
                                  .version = MOQ_VERSION_DRAFT_18 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    moq_bytes_t ns[] = { {(const uint8_t *)"pico", 4},
                         {(const uint8_t *)"wt", 2} };
    fc.track_namespace.parts = ns;
    fc.track_namespace.count = 2;
    fc.track_name = (moq_bytes_t){(const uint8_t *)"video", 5};
    fc.end_group = 4;
    moq_fetch_t cf;
    CHECK(moq_session_fetch(h.client_session, &fc, h.now, &cf) == MOQ_OK);
    moq_pico_wt_service(h.client_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);

    /* Server accepts, so FETCH_OK travels back on the same request bidi and the
     * client's inbound side of that bidi is exercised. */
    {
        moq_event_t ev;
        while (moq_session_poll_events(h.server_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                moq_accept_fetch_cfg_t ac;
                moq_accept_fetch_cfg_init(&ac);
                ac.end_group = 4;
                (void)moq_session_accept_fetch(h.server_session,
                                               ev.u.fetch_request.fetch, &ac,
                                               h.now);
            }
            moq_event_cleanup(&ev);
        }
    }
    moq_pico_wt_service(h.server_conn, h.now);
    CHECK(pico_wt_harness_pump(&h, 2000) == 0);
    {   /* the application has its FETCH_OK; nothing else is pending */
        moq_event_t ev;
        while (moq_session_poll_events(h.client_session, &ev, 1) > 0) moq_event_cleanup(&ev);
    }

    /* Locate the tracked BIDIRECTIONAL receive entry. */
    moq_pico_wt_conn_t *c = h.client_conn;
    uint64_t bidi_sid = UINT64_MAX;
    for (size_t i = 0; i < c->rx_count; i++) {
        pw_rx_stream_t *st = &c->rx[i];
        if (st->active && st->kind == PW_RX_BIDI) bidi_sid = st->stream_id;
    }
    CHECK(bidi_sid != UINT64_MAX);
    if (bidi_sid == UINT64_MAX) { pico_wt_harness_cleanup(&h); return; }
    CHECK((bidi_sid & 2) == 0);          /* genuinely bidirectional */

    observers_install();
    uint32_t stops_before = (uint32_t)c->stop_sending_count;
    int grants_before = grants_for(c, bidi_sid);

    /* Real STOP_SENDING from the peer, emitted through the server's endpoint
     * vtable and carried by the simulation -- not an injected callback. Pump
     * until the client has issued its own STOP of the request stream (the
     * expected end of the exchange) or the bound runs out. */
    moq_transport_endpoint_ops_t *ops = &h.server_conn->endpoint_ops;
    CHECK(ops->stop_sending != NULL);
    moq_transport_result_t sr =
        ops->stop_sending(&h.server_conn->endpoint_ctx, bidi_sid, 0x2);
    CHECK(sr == MOQ_TRANSPORT_OK);
    for (int i = 0; i < 80; i++) {
        moq_pico_wt_service(h.server_conn, h.now);
        moq_pico_wt_service(c, h.now);
        CHECK(pico_wt_harness_pump(&h, 20) == 0);
        if (obs_find(c, OBS_LOCAL_STOP, bidi_sid, 0) >= 0) break;
    }
    CHECK(observers_complete());            /* the record is a complete inventory */

    /* The signal crossed the transport and reached the production callback. */
    CHECK(c->stop_sending_count > stops_before);
    CHECK(c->last_stop_sending_stream_id == bidi_sid);

    /* STOP alone: at the exit of the peer-STOP callback the entry is present
     * and no credit was granted for it. */
    int stop_exit = obs_find(c, (int)picohttp_callback_stop_sending, bidi_sid, 1);
    CHECK(stop_exit >= 0);
    if (stop_exit >= 0) {
        CHECK(g_obs[stop_exit].entry_present);
        CHECK(g_obs[stop_exit].grants == grants_before);
    }

    /* The later terminal: the FETCH data stream's RESET (a unidirectional
     * peer stream) reaches the client, and only once that RESET is being
     * handled does the client's own local STOP of the request stream retire
     * the entry -- it is generated while the RESET callback runs (entry
     * record before it, exit record after it), never before. */
    int data_reset_entry = -1, data_reset_exit = -1, local_stop = -1, reset_count = 0;
    for (int i = 0; i < g_obs_n; i++) {
        const obs_rec_t *r = &g_obs[i];
        if (r->conn != c) continue;
        if (r->event == (int)picohttp_callback_reset && (r->sid & 2u)) {
            if (r->phase == 0 && data_reset_entry < 0) data_reset_entry = i;
            if (r->phase == 1) { reset_count++; if (data_reset_exit < 0) data_reset_exit = i; }
        }
        if (r->event == OBS_LOCAL_STOP && r->sid == bidi_sid && local_stop < 0) local_stop = i;
    }
    CHECK(reset_count == 1);
    CHECK(data_reset_entry >= 0 && data_reset_exit >= 0 && local_stop >= 0);
    CHECK(stop_exit >= 0 && data_reset_entry > stop_exit);   /* RESET came after the STOP boundary */
    CHECK(local_stop > data_reset_entry && local_stop < data_reset_exit);   /* retired while handling the RESET */
    CHECK(local_stop >= 0 && g_obs[local_stop].entry_present);   /* alive until that very STOP */
    /* no other retirement: the entry survived every record before the local STOP */
    bool survived_until_local_stop = true;
    for (int i = 0; i < local_stop; i++)
        if (g_obs[i].conn == c && g_obs[i].sid == bidi_sid && !g_obs[i].entry_present)
            survived_until_local_stop = false;
    CHECK(survived_until_local_stop);
    CHECK(entry_of(c, bidi_sid) == NULL);                 /* retired now */
    CHECK(grants_for(c, bidi_sid) == grants_before);      /* no grant through STOP, none after the terminal */
    CHECK(!moq_pico_wt_conn_is_fatal(c));

    /* The application's view: exactly one FETCH_RESET for this fetch, from
     * the data stream. */
    int fetch_resets = 0; bool reset_for_cf = false, reset_from_data = false;
    {
        moq_event_t ev;
        while (moq_session_poll_events(h.client_session, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_RESET) {
                fetch_resets++;
                reset_for_cf = moq_fetch_eq(ev.u.fetch_reset.fetch, cf);
                reset_from_data = ev.u.fetch_reset.data_stream;
            }
            moq_event_cleanup(&ev);
        }
    }
    CHECK(fetch_resets == 1 && reset_for_cf && reset_from_data);
    CHECK(observers_complete());

    observers_remove();
    pico_wt_harness_cleanup(&h);
}

/*
 * STOP_SENDING, part 2 of 2: retained receive state survives.
 *
 * STOP_SENDING aborts only OUR sending direction, so a peer that is still
 * sending on the other half of a bidi must not lose inbound bytes. Driving that
 * end-to-end would need a request bidi carrying bulk peer payload, which
 * draft-18 does not produce (fetch data rides a separate unidirectional
 * stream). So the invariant is pinned structurally: a paused bidi entry holding
 * retained bytes is placed in the adapter's table, the SAME production callback
 * is invoked, and the entry must survive as a live receive entry whose bytes
 * leave only by replay -- see the invariant note at the assertions below, which
 * explains why "unchanged" is NOT the requirement.
 *
 * Reusing the RESET path here would discard inbound data on a stream the peer
 * may still be sending on, and fails this test.
 */
static void test_stop_sending_preserves_retained_bidi_state(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x38, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }
    install_acct(h.client_conn);

    moq_pico_wt_conn_t *c = h.client_conn;
    pw_rx_stream_t *saved_rx = c->rx;
    size_t saved_count = c->rx_count, saved_cap = c->rx_cap;

    /* A paused bidi entry with retained bytes, allocated through the
     * connection's own allocator so release is accounted. */
    static const uint8_t RETAINED[] = "retained-inbound-bytes-on-a-bidi";
    const size_t rlen = sizeof(RETAINED) - 1;
    pw_rx_stream_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.active = true;
    probe.app_fc = true;
    probe.kind = PW_RX_BIDI;
    probe.stream_id = 4;                 /* client-initiated bidi */
    CHECK((probe.stream_id & 2) == 0);
    probe.budget = 65535;
    probe.delivered = 1024;
    probe.granted = 65535;
    probe.blocked = 512;
    probe.paused = true;
    probe.buf_cap = rlen;
    probe.buf_len = rlen;
    probe.buf = (uint8_t *)c->alloc.alloc(rlen, c->alloc.ctx);
    CHECK(probe.buf != NULL);
    if (probe.buf == NULL) { pico_wt_harness_cleanup(&h); return; }
    memcpy(probe.buf, RETAINED, rlen);

    c->rx = &probe; c->rx_count = 1; c->rx_cap = 1;
    size_t live_before = g_acct.live;

    h3zero_stream_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    sctx.stream_id = probe.stream_id;
    int rc = moq_pico_wt_callback(h.test_ctx->cnx_client, NULL, 0,
                                  picohttp_callback_stop_sending, &sctx, c);
    CHECK(rc == 0);

    /*
     * The invariant is NOT "nothing moved": this same callback services the
     * bridge, and an entry with no outstanding bridge work legitimately replays
     * its retained bytes and unpauses. That is correct handling.
     *
     * What must never happen is the RESET behaviour -- retiring the entry and
     * freeing the retained bytes WITHOUT delivering them. So the discriminator
     * is that the entry survives as a live receive entry and its bytes left via
     * replay rather than discard.
     */
    CHECK(probe.active);                 /* not retired: the RESET path would */
    CHECK(probe.kind == PW_RX_BIDI);
    CHECK(probe.delivered >= 1024);      /* receive accounting not rewound */
    CHECK(!moq_pico_wt_conn_is_fatal(c));

    if (probe.buf != NULL) {
        /* Still retained: byte-for-byte intact. */
        CHECK(probe.buf_len == rlen);
        CHECK(memcmp(probe.buf, RETAINED, rlen) == 0);
        CHECK(g_acct.live == live_before);
    } else {
        /* Replayed and released -- delivered, not discarded. */
        CHECK(probe.buf_len == 0);
        CHECK(g_acct.live == live_before - 1);
    }

    if (probe.buf != NULL)
        c->alloc.free(probe.buf, probe.buf_cap, c->alloc.ctx);
    c->rx = saved_rx; c->rx_count = saved_count; c->rx_cap = saved_cap;
    pico_wt_harness_cleanup(&h);
}

/*
 * Teardown while paused: retained allocations are released exactly once, and no
 * grant is attempted through a connection pointer the deregister already
 * cleared.
 */
static void test_teardown_while_paused(void)
{
    fixture_t f;
    if (!fixture_up_to_pause(&f, 0x35, 1, false)) {
        CHECK(0);
        pico_wt_harness_cleanup(&f.h);
        return;
    }
    CHECK(receiver_paused(&f));
    {
        pw_rx_stream_t *st = rx_entry(&f, f.sid);
        CHECK(st != NULL && st->buf_len > 0);
    }
    CHECK(g_acct.live == 2);         /* table + retention buffer */

    /* Deregister clears the connection pointer while bytes are still retained;
     * a later service must not reach picoquic through it. */
    h3zero_stream_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    sctx.stream_id = f.sid;
    (void)moq_pico_wt_callback(f.h.test_ctx->cnx_client, NULL, 0,
                               picohttp_callback_deregister, &sctx,
                               f.h.client_conn);

    CHECK(f.h.client_conn->cnx == NULL);
    CHECK(g_acct.live == 0);         /* released exactly once, at deregister */
    CHECK(g_acct.bytes_live == 0);
    CHECK(g_acct.allocs == g_acct.frees);

    /* Servicing a deregistered connection must be safe and must not grant. */
    moq_pico_wt_service(f.h.client_conn, f.h.now);
    CHECK(g_acct.live == 0);

    pico_wt_harness_cleanup(&f.h);
}

/*
 * Upstream interaction: picoquic_set_max_data_control() is a QUIC-CONTEXT-wide
 * switch, and while it is set picoquic_open_flow_control() returns SUCCESS
 * while emitting nothing (the guard on quic->max_data_limit == 0 in
 * picoquic/picoquic/sender.c). picoquic exposes no getter for max_data_limit,
 * so the adapter cannot detect this and promises no runtime detection -- a
 * nonzero max_data_control is documented invalid caller configuration.
 *
 * This proves the exclusion at its source rather than asserting an adapter
 * behaviour that cannot exist.
 */
static void test_max_data_control_excludes_open_flow_control(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x36, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }

    picoquic_cnx_t *cnx = h.test_ctx->cnx_client;
    uint64_t sid = 3;
    (void)picoquic_add_to_stream(cnx, sid, (const uint8_t *)"x", 1, 0);
    CHECK(picoquic_set_app_flow_control(cnx, sid, 1) == 0);

    picoquic_stream_head_t *st = picoquic_find_stream(cnx, sid);
    CHECK(st != NULL);
    if (st == NULL) { pico_wt_harness_cleanup(&h); return; }

    /* Baseline: with the switch OFF a grant really does raise the window. */
    uint64_t before = st->maxdata_local;
    CHECK(picoquic_open_flow_control(cnx, sid, 100000) == 0);
    CHECK(st->maxdata_local > before);

    /* With the QUIC-wide switch ON the same call succeeds and does nothing. */
    picoquic_set_max_data_control(h.test_ctx->qclient, 1000000);
    uint64_t held = st->maxdata_local;
    int rc = picoquic_open_flow_control(cnx, sid, held + 500000);
    CHECK(rc == 0);                       /* SUCCESS ... */
    CHECK(st->maxdata_local == held);     /* ... and no grant was emitted */

    picoquic_set_max_data_control(h.test_ctx->qclient, 0);
    pico_wt_harness_cleanup(&h);
}

/*
 * A local STOP_SENDING that the transport ACCEPTS cancels our receive direction,
 * so the adapter must drop that stream's receive tracking: retained bytes must
 * never be replayed, and no further credit may be granted, for a stream the
 * application just stopped. libmoq emits exactly this when cancelling a
 * peer-origin fetch data stream, so the drop is load-bearing for that path.
 *
 * A FAILED stop sends nothing, so receive state must survive untouched.
 */
static void test_local_stop_sending_drops_receive_state(void)
{
    fixture_t f;
    if (!fixture_up_to_pause(&f, 0x39, 1, false)) {
        CHECK(0);
        pico_wt_harness_cleanup(&f.h);
        return;
    }
    CHECK(receiver_paused(&f));
    pw_rx_stream_t *st = rx_entry(&f, f.sid);
    CHECK(st != NULL && st->buf_len > 0);
    size_t live_before = g_acct.live;

    moq_transport_endpoint_ops_t *ops = &f.h.client_conn->endpoint_ops;

    /* Failure path first, and it must fail INSIDE picoquic rather than at the
     * error-code mapping, or it would not exercise the success-only rule.
     * A tracked entry whose stream picoquic does not know makes
     * picoquic_stop_sending() fail while a receive entry genuinely exists;
     * nothing is sent, so that entry must survive untouched. */
    {
        moq_pico_wt_conn_t *c = f.h.client_conn;
        pw_rx_stream_t *saved_rx = c->rx;
        size_t saved_count = c->rx_count, saved_cap = c->rx_cap;
        pw_rx_stream_t ghost;
        memset(&ghost, 0, sizeof(ghost));
        ghost.active = true;
        ghost.app_fc = true;
        ghost.paused = true;
        ghost.kind = PW_RX_UNI;
        ghost.stream_id = 0xFFFF0;      /* never opened on this connection */
        ghost.budget = 65535;
        ghost.blocked = 128;
        c->rx = &ghost; c->rx_count = 1; c->rx_cap = 1;

        moq_transport_result_t bad =
            ops->stop_sending(&c->endpoint_ctx, ghost.stream_id, 0x1);
        CHECK(bad == MOQ_TRANSPORT_ERROR);
        CHECK(ghost.active);                       /* untouched */
        CHECK(ghost.paused);
        CHECK(ghost.blocked == 128);

        c->rx = saved_rx; c->rx_count = saved_count; c->rx_cap = saved_cap;
    }
    CHECK(rx_entry(&f, f.sid) != NULL);
    CHECK(g_acct.live == live_before);

    /* Success path: the receive entry and its retained bytes are dropped. */
    moq_transport_result_t ok =
        ops->stop_sending(&f.h.client_conn->endpoint_ctx, f.sid, 0x1);
    CHECK(ok == MOQ_TRANSPORT_OK);
    CHECK(rx_entry(&f, f.sid) == NULL);            /* dropped exactly once */
    CHECK(g_acct.live == live_before - 1);         /* retention released once */

    /* No replay and no credit afterwards: the window must not advance. */
    picoquic_stream_head_t *rs =
        picoquic_find_stream(f.h.test_ctx->cnx_client, f.sid);
    uint64_t win = rs ? rs->maxdata_local : 0;
    for (int i = 0; i < 30; i++) {
        moq_pico_wt_service(f.h.client_conn, f.h.now);
        if (pico_wt_harness_pump(&f.h, 50) != 0) break;
    }
    rs = picoquic_find_stream(f.h.test_ctx->cnx_client, f.sid);
    CHECK(rs == NULL || rs->maxdata_local == win);
    CHECK(rx_entry(&f, f.sid) == NULL);
    CHECK(!moq_pico_wt_conn_is_fatal(f.h.client_conn));

    pico_wt_harness_cleanup(&f.h);
}

/* Test-build-only dispatch hook (see pw_after_fin_service). Not exported from
 * the shipped library. */
extern moq_result_t (*pw_test_after_fin_service_hook)(moq_transport_bridge_t *,
                                                      uint64_t);

static moq_transport_bridge_t *g_hook_bridge;
static uint64_t g_hook_now;
static int g_hook_calls;

/* Emulates a control FIN whose replay initiated a connection close: the
 * dispatch makes the bridge terminal and reports success. */
static moq_result_t hook_close_dispatched(moq_transport_bridge_t *b, uint64_t now)
{
    g_hook_calls++;
    moq_transport_bridge_on_transport_error(b, 0, now);
    return MOQ_OK;
}

/* Emulates the branch the real bridge cannot produce: a negative result that
 * did NOT go terminal. */
static moq_result_t hook_negative_nonterminal(moq_transport_bridge_t *b,
                                              uint64_t now)
{
    (void)b; (void)now;
    g_hook_calls++;
    return MOQ_ERR_INTERNAL;
}

/*
 * A control-stream FIN retained while paused is replayed inside the receive
 * sweep. On the control stream that only marks the bridge's deferred close, so
 * the sweep must dispatch it and stop -- otherwise a LATER stream in the same
 * sweep gets its retained bytes replayed, or fresh credit granted, after the
 * connection was already meant to be closing.
 *
 * Also covers the unexpected case: a dispatch that fails without going terminal
 * must become an adapter-reported transport error rather than being swallowed.
 */
static void test_retained_control_fin_stops_sweep(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x3A, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }
    install_acct(h.client_conn);
    moq_pico_wt_conn_t *c = h.client_conn;
    pw_rx_stream_t *saved_rx = c->rx;
    size_t saved_count = c->rx_count, saved_cap = c->rx_cap;

    /* [0] control stream holding a retained FIN; [1] a later data stream that
     * still has retained bytes and owed credit. Sweep order is array order. */
    pw_rx_stream_t ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].active = true; ent[0].app_fc = true; ent[0].paused = true;
    ent[0].kind = PW_RX_CONTROL;
    ent[0].stream_id = c->moq_control_stream_id;
    ent[0].budget = 65535; ent[0].buf_fin = true;

    static const uint8_t LATER[] = "later-stream-retained-bytes";
    const size_t llen = sizeof(LATER) - 1;
    ent[1].active = true; ent[1].app_fc = true; ent[1].paused = true;
    ent[1].kind = PW_RX_UNI; ent[1].stream_id = 7;
    ent[1].budget = 65535; ent[1].delivered = 40000; ent[1].granted = 0;
    ent[1].buf_cap = llen; ent[1].buf_len = llen;
    ent[1].buf = (uint8_t *)c->alloc.alloc(llen, c->alloc.ctx);
    CHECK(ent[1].buf != NULL);
    if (ent[1].buf == NULL) { pico_wt_harness_cleanup(&h); return; }
    memcpy(ent[1].buf, LATER, llen);

    c->rx = ent; c->rx_count = 2; c->rx_cap = 2;
    g_hook_bridge = c->bridge; g_hook_now = h.now; g_hook_calls = 0;
    (void)g_hook_bridge; (void)g_hook_now;

    /* The control FIN replay dispatches a close: the sweep must stop there. */
    pw_test_after_fin_service_hook = hook_close_dispatched;
    moq_pico_wt_service(c, h.now);
    pw_test_after_fin_service_hook = NULL;

    CHECK(g_hook_calls == 1);                    /* the close WAS dispatched */
    CHECK(moq_transport_bridge_is_terminal(c->bridge));
    /* The later stream was never swept: bytes not replayed, no credit granted. */
    CHECK(ent[1].buf != NULL);
    CHECK(ent[1].buf_len == llen);
    CHECK(ent[1].buf != NULL && memcmp(ent[1].buf, LATER, llen) == 0);
    CHECK(ent[1].granted == 0);
    CHECK(ent[1].paused);

    if (ent[1].buf != NULL)
        c->alloc.free(ent[1].buf, ent[1].buf_cap, c->alloc.ctx);
    c->rx = saved_rx; c->rx_count = saved_count; c->rx_cap = saved_cap;
    pico_wt_harness_cleanup(&h);
}

/* A dispatch that returns negative WITHOUT going terminal must be surfaced as
 * an adapter transport error, not swallowed. */
static void test_control_fin_dispatch_error_is_fatal(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x3B, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }
    moq_pico_wt_conn_t *c = h.client_conn;
    pw_rx_stream_t *saved_rx = c->rx;
    size_t saved_count = c->rx_count, saved_cap = c->rx_cap;

    pw_rx_stream_t ent;
    memset(&ent, 0, sizeof(ent));
    ent.active = true; ent.app_fc = true; ent.paused = true;
    ent.kind = PW_RX_CONTROL;
    ent.stream_id = c->moq_control_stream_id;
    ent.budget = 65535; ent.buf_fin = true;
    c->rx = &ent; c->rx_count = 1; c->rx_cap = 1;

    CHECK(!moq_pico_wt_conn_is_fatal(c));
    g_hook_calls = 0;
    pw_test_after_fin_service_hook = hook_negative_nonterminal;
    moq_pico_wt_service(c, h.now);
    pw_test_after_fin_service_hook = NULL;

    CHECK(g_hook_calls == 1);
    CHECK(moq_pico_wt_conn_is_fatal(c));         /* reported, not swallowed */

    c->rx = saved_rx; c->rx_count = saved_count; c->rx_cap = saved_cap;
    pico_wt_harness_cleanup(&h);
}

/*
 * A control FIN delivered DIRECTLY (not replayed from retention) takes a
 * different entry path into the sweep: the feed records a deferred close
 * without going terminal, so the caller's pre-service terminal check passes,
 * and the service that follows dispatches the close and DOES go terminal. The
 * sweep must not run at all in that state, or a later stream's retained bytes
 * get replayed, or credit granted, after the connection is already closing.
 *
 * The sibling test covers a FIN replayed inside the sweep; this one covers the
 * way in.
 */
static void test_direct_control_fin_does_not_sweep(void)
{
    pico_wt_harness_t h;
    pico_wt_harness_cfg_t cfg = { .cid_byte = 0x3C, .request_capacity = 10 };
    if (pico_wt_harness_setup(&h, &cfg) != 0 ||
        pico_wt_harness_handshake(&h) != 0) {
        CHECK(0);
        pico_wt_harness_cleanup(&h);
        return;
    }
    install_acct(h.client_conn);
    moq_pico_wt_conn_t *c = h.client_conn;
    pw_rx_stream_t *saved_rx = c->rx;
    size_t saved_count = c->rx_count, saved_cap = c->rx_cap;

    /* [0] the real control stream, NOT paused: its FIN arrives directly.
     * [1] a later stream holding retained bytes and owed credit. */
    static const uint8_t LATER[] = "later-stream-must-not-be-swept";
    const size_t llen = sizeof(LATER) - 1;
    pw_rx_stream_t ent[2];
    memset(ent, 0, sizeof(ent));
    ent[0].active = true; ent[0].app_fc = true;
    ent[0].kind = PW_RX_CONTROL;
    ent[0].stream_id = c->moq_control_stream_id;
    ent[0].budget = 65535;

    ent[1].active = true; ent[1].app_fc = true; ent[1].paused = true;
    ent[1].kind = PW_RX_UNI; ent[1].stream_id = 7;
    ent[1].budget = 65535; ent[1].delivered = 40000; ent[1].granted = 0;
    ent[1].buf_cap = llen; ent[1].buf_len = llen;
    ent[1].buf = (uint8_t *)c->alloc.alloc(llen, c->alloc.ctx);
    CHECK(ent[1].buf != NULL);
    if (ent[1].buf == NULL) { pico_wt_harness_cleanup(&h); return; }
    memcpy(ent[1].buf, LATER, llen);

    c->rx = ent; c->rx_count = 2; c->rx_cap = 2;
    CHECK(!moq_transport_bridge_is_terminal(c->bridge));

    /* Deliver the control FIN through the production callback. */
    h3zero_stream_ctx_t sctx;
    memset(&sctx, 0, sizeof(sctx));
    sctx.stream_id = ent[0].stream_id;
    (void)moq_pico_wt_callback(h.test_ctx->cnx_client, NULL, 0,
                               picohttp_callback_post_fin, &sctx, c);

    /* The close was dispatched; the later stream was never touched. */
    CHECK(moq_transport_bridge_is_terminal(c->bridge));
    CHECK(ent[1].buf != NULL);
    CHECK(ent[1].buf_len == llen);
    CHECK(ent[1].buf != NULL && memcmp(ent[1].buf, LATER, llen) == 0);
    CHECK(ent[1].granted == 0);
    CHECK(ent[1].paused);

    if (ent[1].buf != NULL)
        c->alloc.free(ent[1].buf, ent[1].buf_cap, c->alloc.ctx);
    c->rx = saved_rx; c->rx_count = saved_count; c->rx_cap = saved_cap;
    pico_wt_harness_cleanup(&h);
}

int main(void)
{
    test_pre_ready_grant_is_owed();
    test_fin_while_paused();
    test_observer_recorders_fail_closed();
    test_reset_while_paused();
    test_stop_sending_on_real_request_bidi();
    test_stop_sending_preserves_retained_bidi_state();
    test_teardown_while_paused();
    test_retained_control_fin_stops_sweep();
    test_direct_control_fin_does_not_sweep();
    test_control_fin_dispatch_error_is_fatal();
    test_local_stop_sending_drops_receive_state();
    test_max_data_control_excludes_open_flow_control();

    if (g_failures) {
        fprintf(stderr, "FAILED: test_pico_wt_rx_lifecycle (%d)\n", g_failures);
        return 1;
    }
    fprintf(stderr, "PASS: test_pico_wt_rx_lifecycle\n");
    return 0;
}
