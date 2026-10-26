/*
 * Shared send admission (MOQ_TRANSPORT_CAP_SHARED_ADMISSION) over the REAL
 * picoquic WebTransport adapter in the deterministic simulator: the
 * adapter's own send queue (its default budget for the connection) kept full
 * behind a rate-limited link. Each case runs from the same start with the
 * endpoint advertising the capability, as shipped, and with it withdrawn,
 * and the subscriber must receive the same: every object's bytes in the same
 * order, and every subgroup's end. Each run must receive exactly the plan,
 * checked by coordinate: every object the publisher wrote and every
 * subgroup's end, once, so a repeat cannot stand in for a loss. The
 * advertised run offers fewer writes the queue refuses. The advertised case
 * runs twice: the simulator repeats itself.
 *
 * As for PicoQUIC (test_pq_sim_shared_admission.c), the FIN rows follow the
 * queue's charging: a FIN onto bytes the queue still holds is coalesced and
 * taken while the budget is spent; a FIN after the stream's bytes have left
 * needs a standalone marker, refused while the budget is spent and taken
 * once it frees. A write interposer records both, and a ledger of owed FINs
 * (tests/support/fin_obligations.h) must find every refused FIN taken later.
 * Controls first show the delivery check and the ledger refusing what they
 * must.
 */
#include "pico_wt_harness.h"
#include "../pico_wt_adapter.h"
#include "../../../tests/support/fin_obligations.h"

#include <moq/pico_wt.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>

#include <stdio.h>
#include <string.h>

static int g_failures;
#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
            g_failures++;                                                    \
        }                                                                    \
    } while (0)

#define OBJ_SIZE         8000u
#define OBJS_PER_GRP     4
#define GROUPS           8         /* each of the two tracks closing at once */
#define LATE_GROUPS      2         /* the track closing after its bytes left */
#define LINK_PS_PER_BYTE 800000u   /* 10 Mbps */

/* -- the write interposer ------------------------------------------------- */

typedef struct {
    moq_transport_endpoint_ops_t orig;
    moq_pq_send_queue_t *queue;
    uint64_t cap;
    uint32_t refused;            /* writes the queue refused */
    uint32_t coalesced_spent;    /* FIN-only, onto held bytes, budget spent: taken */
    uint32_t standalone_spent;   /* FIN-only, no held bytes, budget spent: refused */
    fin_obligations_t fins;      /* streams whose refused FIN is still owed */
} tap_t;

static tap_t g_tap;

static bool tap_spent(void)
{
    return moq_pq_send_queue_queued_bytes(g_tap.queue) >= g_tap.cap;
}

static void tap_note(uint64_t sid, size_t len, bool fin, bool held, bool spent,
                     moq_transport_result_t rc)
{
    const bool fin_only = len == 0 && fin;
    if (rc == MOQ_TRANSPORT_WOULD_BLOCK) {
        g_tap.refused++;
        if (fin_only && !held && spent) g_tap.standalone_spent++;
    } else if (rc == MOQ_TRANSPORT_OK && fin_only && held && spent) {
        g_tap.coalesced_spent++;
    }
    if (fin) fin_obligations_note(&g_tap.fins, sid, rc == MOQ_TRANSPORT_OK);
}

/* A FIN-carrying write's result, brought to tap_note as a write brings it:
 * the path the ledger's controls drive. */
static void tap_note_fin(uint64_t sid, size_t len, bool taken)
{
    tap_note(sid, len, true, false, false,
             taken ? MOQ_TRANSPORT_OK : MOQ_TRANSPORT_WOULD_BLOCK);
}

static moq_transport_result_t tap_write(void *ctx, uint64_t sid,
                                        const uint8_t *data, size_t len,
                                        bool fin)
{
    const bool held = moq_pq_send_queue_has_data(g_tap.queue, sid);
    const bool spent = tap_spent();
    moq_transport_result_t rc = g_tap.orig.write(ctx, sid, data, len, fin);
    tap_note(sid, len, fin, held, spent, rc);
    return rc;
}

static moq_transport_result_t tap_write_payload(void *ctx, uint64_t sid,
                                                moq_rcbuf_t *buf, bool fin)
{
    const bool held = moq_pq_send_queue_has_data(g_tap.queue, sid);
    const bool spent = tap_spent();
    moq_transport_result_t rc = g_tap.orig.write_payload(ctx, sid, buf, fin);
    tap_note(sid, buf ? moq_rcbuf_len(buf) : 0, fin, held, spent, rc);
    return rc;
}

/* Wrap the publisher's writes; withdraw the capability unless `advertise`. */
static void tap_install(moq_pico_wt_conn_t *conn, bool advertise)
{
    memset(&g_tap, 0, sizeof(g_tap));
    g_tap.queue = conn->endpoint_ctx.queue;
    g_tap.cap = MOQ_PQ_SEND_QUEUE_CAP_DEFAULT;
    moq_transport_endpoint_ops_t *ops = &conn->endpoint_ops;
    CHECK((ops->capabilities & MOQ_TRANSPORT_CAP_SHARED_ADMISSION) != 0);
    g_tap.orig = *ops;
    if (!advertise)
        ops->capabilities &= ~(uint32_t)MOQ_TRANSPORT_CAP_SHARED_ADMISSION;
    ops->write = tap_write;
    if (ops->write_payload) ops->write_payload = tap_write_payload;
}

static void tap_remove(moq_pico_wt_conn_t *conn)
{
    conn->endpoint_ops.write = g_tap.orig.write;
    conn->endpoint_ops.write_payload = g_tap.orig.write_payload;
}

/* -- subscriptions, data and what the subscriber received ---------------- */

enum { HI, MID, LATE, NTRACKS };
static const char *const k_names[NTRACKS] = { "hi", "mid", "late" };
static const uint8_t k_prio[NTRACKS] = { 5, 10, 30 };

typedef struct {
    moq_subscription_t pub;   /* the publisher's handle */
    moq_subscription_t sub;   /* the subscriber's handle */
} track_t;

typedef struct {
    uint8_t  kind;            /* 1 object, 2 subgroup finished, 3 reset */
    uint8_t  track;
    uint16_t group;
    uint16_t object;          /* the subgroup, for kinds 2 and 3 */
    uint32_t sum;             /* FNV-1a of the payload */
} rx_t;

#define RX_MAX 512

/* What the subscriber received: in arrival order, and against the plan. */
typedef struct {
    rx_t     ev[RX_MAX];
    size_t   n;
    bool     obj[NTRACKS][GROUPS + 1][OBJS_PER_GRP];  /* by group id, from 1 */
    bool     end[NTRACKS][GROUPS + 1];
    uint32_t resets[NTRACKS];
    uint32_t repeats;         /* an object or a subgroup end received again */
    uint32_t bad;             /* off the plan, a wrong payload, or a full log */
} rxlog_t;

/* The publisher's plan: track t's groups 1..groups_of(t), each one subgroup
 * (0) of objects 0..OBJS_PER_GRP-1. */
static uint64_t groups_of(int t)
{
    return t == LATE ? LATE_GROUPS : GROUPS;
}

/* (t, g, sg) is a planned subgroup, judged at full width: before the log
 * narrows any coordinate. */
static bool planned(int t, uint64_t g, uint64_t sg)
{
    return t >= 0 && t < NTRACKS && g >= 1 && g <= groups_of(t) && sg == 0;
}

static int subscribe(pico_wt_harness_t *h, int t, track_t *out)
{
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns[] = { {(const uint8_t *)"pico", 4},
                         {(const uint8_t *)"wt", 2} };
    sc.track_namespace.parts = ns;
    sc.track_namespace.count = 2;
    sc.track_name = (moq_bytes_t){ (const uint8_t *)k_names[t],
                                   strlen(k_names[t]) };
    sc.has_subscriber_priority = true;
    sc.subscriber_priority = k_prio[t];
    if (moq_session_subscribe(h->client_session, &sc, h->now, &out->sub) < 0)
        return -1;
    moq_pico_wt_service(h->client_conn, h->now);
    if (pico_wt_harness_pump(h, 500) != 0) return -1;
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(h->server_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            out->pub = ev.u.subscribe_request.sub;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return -1;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(h->server_session, out->pub, &ac,
                                     h->now) < 0)
        return -1;
    moq_pico_wt_service(h->server_conn, h->now);
    if (pico_wt_harness_pump(h, 500) != 0) return -1;
    bool ok = false;
    while (moq_session_poll_events(h->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) ok = true;
        moq_event_cleanup(&ev);
    }
    return ok ? 0 : -1;
}

static void fill_payload(uint8_t *p, int t, uint64_t g, uint64_t o)
{
    p[0] = (uint8_t)t;
    p[1] = (uint8_t)g;
    p[2] = (uint8_t)o;
    for (size_t i = 3; i < OBJ_SIZE; i++)
        p[i] = (uint8_t)(t * 31 + g * 7 + o * 3 + i);
}

static uint32_t fnv(const uint8_t *p, size_t n)
{
    uint32_t hsh = 2166136261u;
    for (size_t i = 0; i < n; i++) { hsh ^= p[i]; hsh *= 16777619u; }
    return hsh;
}

/* Open (group, subgroup 0) and write its objects; FIN after the last unless
 * `keep_open`. */
static int publish_group(pico_wt_harness_t *h, const track_t *tr, int t,
                         uint64_t g, bool keep_open, moq_subgroup_handle_t *out)
{
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = g;
    cfg.publisher_priority = 128;
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(h->server_session, tr->pub, &cfg, h->now,
                                  &sg) < 0)
        return -1;
    static uint8_t payload[OBJ_SIZE];
    for (uint64_t o = 0; o < OBJS_PER_GRP; o++) {
        fill_payload(payload, t, g, o);
        moq_rcbuf_t *buf = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), payload, sizeof(payload),
                             &buf) < 0)
            return -1;
        moq_result_t rc = moq_session_write_object(h->server_session, sg, o,
                                                   buf, h->now);
        moq_rcbuf_decref(buf);
        if (rc < 0) return -1;
    }
    if (out) *out = sg;
    if (keep_open) return 0;
    return moq_session_close_subgroup(h->server_session, sg, h->now) < 0
        ? -1 : 0;
}

static int track_of(const track_t *tr, moq_subscription_t sub)
{
    for (int t = 0; t < NTRACKS; t++)
        if (moq_subscription_eq(tr[t].sub, sub)) return t;
    return -1;
}

static void rx_add(rxlog_t *log, uint8_t kind, int t, uint64_t g, uint64_t o,
                   uint32_t sum)
{
    if (log->n == RX_MAX) { log->bad++; return; }
    log->ev[log->n++] = (rx_t){ kind, (uint8_t)t, (uint16_t)g, (uint16_t)o,
                                sum };
}

/* An object as the subscriber received it: planned, carrying the bytes the
 * publisher wrote at its coordinates. A second copy is a repeat; it fills no
 * other object's place. */
static void rx_object(rxlog_t *log, int t, uint64_t g, uint64_t sg,
                      uint64_t o, const uint8_t *p, size_t len)
{
    static uint8_t want[OBJ_SIZE];
    if (!planned(t, g, sg) || o >= OBJS_PER_GRP || !p || len != OBJ_SIZE) {
        log->bad++;
        return;
    }
    fill_payload(want, t, g, o);
    if (memcmp(p, want, OBJ_SIZE) != 0) {
        log->bad++;
        return;
    }
    if (log->obj[t][g][o]) log->repeats++;
    log->obj[t][g][o] = true;
    rx_add(log, 1, t, g, o, fnv(p, OBJ_SIZE));
}

/* A subgroup's end: planned. A second is a repeat; it ends no other
 * subgroup. */
static void rx_end(rxlog_t *log, int t, uint64_t g, uint64_t sg)
{
    if (!planned(t, g, sg)) {
        log->bad++;
        return;
    }
    if (log->end[t][g]) log->repeats++;
    log->end[t][g] = true;
    rx_add(log, 2, t, g, sg, 0);
}

static void rx_reset(rxlog_t *log, int t, uint64_t g, uint64_t sg)
{
    if (!planned(t, g, sg)) {
        log->bad++;
        return;
    }
    log->resets[t]++;
    rx_add(log, 3, t, g, sg, 0);
}

/* Poll the subscriber into `log`. */
static void rx_poll(pico_wt_harness_t *h, const track_t *tr, rxlog_t *log)
{
    moq_event_t ev;
    while (moq_session_poll_events(h->client_session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            rx_object(log, track_of(tr, o->sub), o->group_id, o->subgroup_id,
                      o->object_id,
                      o->payload ? moq_rcbuf_data(o->payload) : NULL,
                      o->payload ? moq_rcbuf_len(o->payload) : 0);
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            const moq_subgroup_finished_event_t *f = &ev.u.subgroup_finished;
            rx_end(log, track_of(tr, f->sub), f->group_id, f->subgroup_id);
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_RESET) {
            const moq_subgroup_reset_event_t *r = &ev.u.subgroup_reset;
            rx_reset(log, track_of(tr, r->sub), r->group_id, r->subgroup_id);
        }
        moq_event_cleanup(&ev);
    }
}

/* Every planned object of track t has arrived. */
static bool rx_objects_done(const rxlog_t *log, int t)
{
    for (uint64_t g = 1; g <= groups_of(t); g++)
        for (uint64_t o = 0; o < OBJS_PER_GRP; o++)
            if (!log->obj[t][g][o]) return false;
    return true;
}

/* Every planned object and every planned subgroup's end has arrived. */
static bool rx_complete(const rxlog_t *log)
{
    for (int t = 0; t < NTRACKS; t++) {
        if (!rx_objects_done(log, t)) return false;
        for (uint64_t g = 1; g <= groups_of(t); g++)
            if (!log->end[t][g]) return false;
    }
    return true;
}

/* -- the delivery check's controls ---------------------------------------- */

#define RX_END UINT64_MAX         /* rx_feed: leave out the subgroup's end */

/* Feed `log`, from empty, the plan as the subscriber receives it, less one
 * record: object (t, g, o), or the end of (t, g) when o is RX_END; t < 0
 * leaves out nothing. */
static void rx_feed(rxlog_t *log, int t_out, uint64_t g_out, uint64_t o_out)
{
    static uint8_t p[OBJ_SIZE];
    memset(log, 0, sizeof(*log));
    for (int t = 0; t < NTRACKS; t++)
        for (uint64_t g = 1; g <= groups_of(t); g++) {
            const bool here = t == t_out && g == g_out;
            for (uint64_t o = 0; o < OBJS_PER_GRP; o++) {
                if (here && o == o_out) continue;
                fill_payload(p, t, g, o);
                rx_object(log, t, g, 0, o, p, OBJ_SIZE);
            }
            if (!(here && o_out == RX_END)) rx_end(log, t, g, 0);
        }
}

/* Records of `kind` the log holds for track t: an aggregate count. */
static uint32_t rx_count(const rxlog_t *log, uint8_t kind, int t)
{
    uint32_t n = 0;
    for (size_t i = 0; i < log->n; i++)
        if (log->ev[i].kind == kind && log->ev[i].track == t) n++;
    return n;
}

/* The whole plan, then deliveries the check must refuse although their
 * aggregate counts match the plan's. Returns the controls it got wrong, bit
 * k for the k-th in order (0 when none). */
static unsigned rx_controls(void)
{
    static rxlog_t c;
    static uint8_t p[OBJ_SIZE], q[OBJ_SIZE];
    unsigned failed = 0;

    /* The whole plan, once: complete. */
    rx_feed(&c, -1, 0, 0);
    if (!rx_complete(&c) || c.bad || c.repeats) failed |= 1u << 0;

    /* A repeated object in a lost one's place. */
    rx_feed(&c, HI, 2, 3);
    fill_payload(p, HI, 2, 2);
    rx_object(&c, HI, 2, 0, 2, p, OBJ_SIZE);
    if (rx_count(&c, 1, HI) != GROUPS * OBJS_PER_GRP || rx_complete(&c) ||
        c.repeats != 1)
        failed |= 1u << 1;

    /* A repeated subgroup end in another subgroup's place. */
    rx_feed(&c, MID, 2, RX_END);
    rx_end(&c, MID, 1, 0);
    if (rx_count(&c, 2, MID) != GROUPS || rx_complete(&c) || c.repeats != 1)
        failed |= 1u << 2;

    /* A group id that narrows to a planned one, carrying the very bytes the
     * publisher wrote for that one, in its place. */
    rx_feed(&c, HI, 1, 0);
    fill_payload(p, HI, 1, 0);
    fill_payload(q, HI, 65537, 0);
    rx_object(&c, HI, 65537, 0, 0, q, OBJ_SIZE);
    if (memcmp(p, q, OBJ_SIZE) != 0 || rx_complete(&c) || c.bad != 1)
        failed |= 1u << 3;

    /* A subgroup id that narrows to 0, on an end in the planned one's
     * place. */
    rx_feed(&c, LATE, 1, RX_END);
    rx_end(&c, LATE, 1, 65536);
    if (rx_complete(&c) || c.bad != 1) failed |= 1u << 4;

    return failed;
}

/* -- the case -------------------------------------------------------------- */

typedef struct {
    rxlog_t rx;
    tap_t   tap;
    bool    late_drained;      /* LATE's objects all arrived before its FINs */
    bool    spent_at_close;    /* the budget was spent when LATE closed */
} run_t;

static void pump_poll(pico_wt_harness_t *h, const track_t *tr, rxlog_t *log)
{
    moq_pico_wt_service(h->server_conn, h->now);
    (void)pico_wt_harness_pump(h, 20);
    rx_poll(h, tr, log);
}

static bool run_case(moq_version_t ver, uint8_t cid, bool advertise, run_t *r)
{
    memset(r, 0, sizeof(*r));
    pico_wt_harness_t h;
    /* The publisher holds the whole workload at once, as the PicoQUIC
     * case's does. */
    pico_wt_harness_cfg_t cfg = { .cid_byte = cid, .request_capacity = 16,
                                  .max_events = 256, .version = ver,
                                  .server_max_actions = 1024,
                                  .server_send_buffer = 1u << 20 };
    bool ok = pico_wt_harness_setup(&h, &cfg) == 0 &&
              pico_wt_harness_handshake(&h) == 0;
    CHECK(ok);
    /* The publisher's direction is the bottleneck. */
    if (ok) h.test_ctx->s_to_c_link->picosec_per_byte = LINK_PS_PER_BYTE;
    track_t tr[NTRACKS];
    for (int t = 0; ok && t < NTRACKS; t++) {
        ok = subscribe(&h, t, &tr[t]) == 0;
        CHECK(ok);
    }
    if (ok) tap_install(h.server_conn, advertise);

    /* LATE's subgroups stay open until all their objects have arrived. */
    moq_subgroup_handle_t late[LATE_GROUPS];
    for (int g = 0; ok && g < LATE_GROUPS; g++) {
        ok = publish_group(&h, &tr[LATE], LATE, (uint64_t)g + 1, true,
                           &late[g]) == 0;
        CHECK(ok);
    }
    const uint64_t deadline = h.now + 60000000;
    while (ok && !rx_objects_done(&r->rx, LATE) && h.now < deadline)
        pump_poll(&h, tr, &r->rx);
    r->late_drained = rx_objects_done(&r->rx, LATE);
    CHECK(r->late_drained);

    /* HI and MID queue far more than the budget, each subgroup closed at
     * once; LATE then closes while the budget is spent. */
    for (int g = 0; ok && g < GROUPS; g++) {
        ok = publish_group(&h, &tr[HI], HI, (uint64_t)g + 1, false, NULL) == 0 &&
             publish_group(&h, &tr[MID], MID, (uint64_t)g + 1, false, NULL) == 0;
        CHECK(ok);
    }
    for (int i = 0; ok && i < 1000 && !tap_spent(); i++) {
        int wa = 0;
        if (pico_wt_harness_sim_round(&h, h.now + 1000, &wa) != 0) break;
    }
    r->spent_at_close = ok && tap_spent();
    CHECK(r->spent_at_close);
    for (int g = 0; ok && g < LATE_GROUPS; g++) {
        ok = moq_session_close_subgroup(h.server_session, late[g], h.now) >= 0;
        CHECK(ok);
    }

    while (ok && !rx_complete(&r->rx) && h.now < deadline)
        pump_poll(&h, tr, &r->rx);
    CHECK(rx_complete(&r->rx));
    CHECK(r->rx.bad == 0);
    CHECK(r->rx.repeats == 0);
    for (int t = 0; t < NTRACKS; t++) CHECK(r->rx.resets[t] == 0);
    r->tap = g_tap;
    if (h.server_conn) tap_remove(h.server_conn);
    pico_wt_harness_cleanup(&h);
    return ok;
}

static bool same_rx(const rxlog_t *a, const rxlog_t *b)
{
    if (a->n != b->n) return false;
    for (size_t i = 0; i < a->n; i++)
        if (memcmp(&a->ev[i], &b->ev[i], sizeof(rx_t)) != 0) return false;
    return true;
}

static int draft_of(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ? 16 : v == MOQ_VERSION_DRAFT_18 ? 18 : 22;
}

int main(void)
{
    static const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    static run_t adv, again, wd;
    const unsigned rx_failed = rx_controls();
    const unsigned fin_failed =
        fin_obligations_controls(&g_tap.fins, tap_note_fin);
    memset(&g_tap, 0, sizeof(g_tap));
    CHECK(rx_failed == 0);
    CHECK(fin_failed == 0);
    if (rx_failed || fin_failed)
        fprintf(stderr, "controls failed: delivery 0x%x, FIN ledger 0x%x\n",
                rx_failed, fin_failed);
    else
        printf("controls: the delivery check refuses a repeat in a lost "
               "object's place, a repeated end in another subgroup's, and "
               "ids that narrow onto the plan; the FIN ledger keeps a FIN "
               "refused after 300 retries of another, owes a FIN taken "
               "before its refusal, and fails full\n");
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++) {
        const uint8_t cid = (uint8_t)(0x50 + v);
        const bool ok = run_case(versions[v], cid, true, &adv) &&
                        run_case(versions[v], cid, true, &again) &&
                        run_case(versions[v], cid, false, &wd);
        CHECK(ok);
        CHECK(same_rx(&adv.rx, &again.rx));
        CHECK(same_rx(&adv.rx, &wd.rx));
        CHECK(adv.tap.refused < wd.tap.refused);
        const run_t *both[2] = { &adv, &wd };
        for (int i = 0; i < 2; i++) {
            CHECK(both[i]->tap.coalesced_spent > 0);
            CHECK(both[i]->tap.standalone_spent > 0);
            CHECK(both[i]->tap.fins.opened > 0);
            CHECK(fin_obligations_settled(&both[i]->tap.fins));
        }
        const fin_obligations_t *fw = &wd.tap.fins, *fa = &adv.tap.fins;
        printf("d%d: %zu events alike, each planned object and subgroup end "
               "once; refused writes %u -> %u\n",
               draft_of(versions[v]), adv.rx.n, wd.tap.refused,
               adv.tap.refused);
        printf("d%d: FINs while spent (withdrawn / advertised): coalesced "
               "taken %u / %u, standalone refused %u / %u; refused FIN "
               "writes %u / %u on %u / %u streams, at most %zu / %zu owed at "
               "once, all later taken\n",
               draft_of(versions[v]),
               wd.tap.coalesced_spent, adv.tap.coalesced_spent,
               wd.tap.standalone_spent, adv.tap.standalone_spent,
               fw->refusals, fa->refusals, fw->opened, fa->opened,
               fw->peak, fa->peak);
    }
    if (g_failures) {
        fprintf(stderr, "test_pico_wt_shared_admission: %d failure(s)\n",
                g_failures);
        return 1;
    }
    printf("PASS: test_pico_wt_shared_admission\n");
    return 0;
}
