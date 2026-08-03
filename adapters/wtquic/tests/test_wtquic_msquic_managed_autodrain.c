/*
 * Autonomous bounded-poll delivery over the managed wtquic-MsQuic facade.
 *
 * A MANAGED-FACADE subscriber must deliver a whole track to a bounded-poll app
 * with NO explicit managed receiver wake after setup — its only pumps are the
 * autonomous, transport-driven ones the doorbell schedules. Without that
 * autonomous re-drive the transfer stalls on a poll-batch boundary.
 *
 *   SUBSCRIBER: the managed wtquic-MsQuic CLIENT facade (under test). Its
 *               on_lane_pump polls a bounded batch (<= POLL) per pump, subscribes
 *               once at setup, and is NEVER explicitly woken again.
 *   PUBLISHER : a RAW wtquic SERVER (listener + attach adapter, driven on the
 *               transport worker by its hook) that accepts the subscribe and
 *               publishes N x SZ bytes on one subgroup. Reuses the contract-pair
 *               payload helper for byte-exact verification.
 *
 * Real MsQuic loopback, real threads, real TLS. Every timeout is a HANG GUARD
 * only; stall/no-spin are decided by PUMP-GENERATION counters, never elapsed
 * time.
 *
 * Usage: test_wtquic_msquic_managed_autodrain <cert.pem> <key.pem>
 */
#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <moq/wtquic_msquic_managed.h>
#include <moq/wtquic.h>
#include <moq/rcbuf.h>

#include <wtquic/wtquic.h>
#include <wtquic/wtquic_msquic.h>

#include "wtquic_contract_pair.h"   /* wtqc_payload_fill */

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int g_fail;
#define CHECK(c)                                                            \
    do {                                                                    \
        if (!(c)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);    \
            g_fail++;                                                       \
        }                                                                   \
    } while (0)

enum { OBJ_SZ = 256 };
enum { OBJ_N = 1024 };
enum { POLL = 16 };            /* bounded per-pump event batch */
#define HANG_GUARD_US (30u * 1000000u)

/* ------------------------------------------------------------------ *
 * RAW wtquic SERVER publisher: attach adapter driven by its hook on the
 * transport worker. On SUBSCRIBE_REQUEST it accepts and publishes the whole
 * track once, on one subgroup.
 * ------------------------------------------------------------------ */
struct pub_side {
    pthread_mutex_t mu;
    moq_session_t *ms;
    moq_wtquic_conn_t *conn;
    bool have_sub;
    moq_subscription_t sub;
    bool accepted;
    bool sg_open;
    moq_subgroup_handle_t sg;
    uint64_t cursor;             /* next object to write */
    bool published;              /* all written + subgroup closed */
    int publish_errors;
};

/* Incremental publish: write one subgroup of OBJ_N x OBJ_SZ, retrying across
 * hook fires on transport backpressure (WOULD_BLOCK is NOT an error — the hook
 * re-runs as send capacity frees). Close the subgroup only once complete. */
static void pub_publish_step(struct pub_side *sv, moq_session_t *s)
{
    if (!sv->sg_open) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.publisher_priority = 200;
        moq_result_t rc = moq_session_open_subgroup(s, sv->sub, &sgc, 0, &sv->sg);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;
        if (rc < 0) { sv->publish_errors++; return; }
        sv->sg_open = true;
    }
    while (sv->cursor < (uint64_t)OBJ_N) {
        uint8_t bytes[OBJ_SZ];
        moq_rcbuf_t *buf = NULL;
        wtqc_payload_fill(bytes, OBJ_SZ, sv->cursor);
        if (moq_rcbuf_create(moq_alloc_default(), bytes, OBJ_SZ, &buf) < 0) {
            sv->publish_errors++;
            return;
        }
        moq_result_t rc = moq_session_write_object(s, sv->sg, sv->cursor, buf, 0);
        moq_rcbuf_decref(buf);
        if (rc == MOQ_ERR_WOULD_BLOCK) return;   /* retry next hook fire */
        if (rc < 0) { sv->publish_errors++; return; }
        sv->cursor++;
    }
    /* Deliberately DO NOT close the subgroup: closing starts a delivery/done
     * timeout that would cleanly complete (and close) the subscription while the
     * receiver is stalled, confounding the un-pumped-stall signal. Leaving it
     * open keeps every object deliverable with no delivery deadline, so a stall
     * is purely the scheduler's missing re-drive. */
    sv->published = true;
}

static void pub_hook(moq_wtquic_conn_t *conn, void *user)
{
    struct pub_side *sv = user;
    moq_session_t *s = moq_wtquic_conn_session(conn);
    moq_event_t ev;

    pthread_mutex_lock(&sv->mu);
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            sv->sub = ev.u.subscribe_request.sub;
            sv->have_sub = true;
        }
        moq_event_cleanup(&ev);
    }
    if (sv->have_sub && !sv->accepted) {
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        if (moq_session_accept_subscribe(s, sv->sub, &ac, 0) < 0)
            sv->publish_errors++;
        else
            sv->accepted = true;
    }
    if (sv->accepted && !sv->published)
        pub_publish_step(sv, s);
    pthread_mutex_unlock(&sv->mu);
}

static int pub_publish_errors(struct pub_side *sv)
{
    pthread_mutex_lock(&sv->mu);
    int e = sv->publish_errors;
    pthread_mutex_unlock(&sv->mu);
    return e;
}

/* ------------------------------------------------------------------ *
 * MANAGED CLIENT subscriber (under test): bounded-poll app.
 * ------------------------------------------------------------------ */
struct sub_side {
    pthread_mutex_t mu;
    moq_wtquic_msquic_managed_t *m;
    int setup;
    bool subscribed;
    bool freeze;                 /* no-spin variant: stop draining once true */
    int drained;
    int object_errors, dup_objects;
    uint64_t pumps;              /* completed pump generations */
    bool test_stopping;          /* set before rig_down: a later close is ours */
    bool closed_seen;            /* an UNSOLICITED SESSION_CLOSED was observed */
    int closed_at_drained;       /* delivered count when it fired */
    uint64_t closed_at_pump;     /* pump generation when it fired */
    uint64_t closed_code;
    uint8_t *seen;               /* OBJ_N flags */
};

static void sub_verify(struct sub_side *sv, const moq_event_t *ev)
{
    const moq_rcbuf_t *p = ev->u.object_received.payload;
    uint64_t oid = ev->u.object_received.object_id;
    if (p == NULL || moq_rcbuf_len(p) != OBJ_SZ) {
        sv->object_errors++;
        return;
    }
    uint8_t want[OBJ_SZ];
    wtqc_payload_fill(want, OBJ_SZ, oid);
    if (memcmp(moq_rcbuf_data(p), want, OBJ_SZ) != 0)
        sv->object_errors++;
    if (oid < (uint64_t)OBJ_N && sv->seen[oid]++ != 0)
        sv->dup_objects++;
}

static int sub_pump(moq_wtquic_msquic_managed_t *m,
                    moq_wtquic_msquic_managed_lane_t *lane, uint64_t now,
                    void *ctx)
{
    struct sub_side *sv = ctx;
    moq_session_t *s = moq_wtquic_msquic_managed_session(m);

    (void)lane;
    (void)now;
    pthread_mutex_lock(&sv->mu);
    sv->pumps++;
    if (s != NULL && !sv->freeze) {
        /* BOUNDED poll: one batch of up to POLL events, never a drain loop. */
        moq_event_t ev[POLL];
        size_t ne = 0;
        (void)moq_session_poll_events_ex(s, ev, POLL, sizeof(ev[0]), &ne);
        for (size_t i = 0; i < ne; i++) {
            switch (ev[i].kind) {
            case MOQ_EVENT_SETUP_COMPLETE: sv->setup++; break;
            case MOQ_EVENT_OBJECT_RECEIVED:
                sub_verify(sv, &ev[i]);
                sv->drained++;
                break;
            case MOQ_EVENT_SESSION_CLOSED:
                /* Record the FIRST close and whether it preceded our teardown.
                 * A close before test_stopping is UNSOLICITED (not ours). */
                if (!sv->closed_seen) {
                    sv->closed_seen = true;
                    sv->closed_at_drained = sv->drained;
                    sv->closed_at_pump = sv->pumps;
                    sv->closed_code = ev[i].u.closed.code;
                    fprintf(stderr,
                            "  [cli SESSION_CLOSED code=%llu at drained=%d pump=%llu "
                            "test_stopping=%d]\n",
                            (unsigned long long)ev[i].u.closed.code, sv->drained,
                            (unsigned long long)sv->pumps, sv->test_stopping);
                }
                break;
            default: break;
            }
            moq_event_cleanup(&ev[i]);
        }
        if (sv->setup > 0 && !sv->subscribed) {
            static const moq_bytes_t ns[] = { { (const uint8_t *)"wtqc", 4 } };
            moq_subscribe_cfg_t sc;
            moq_subscription_t sub;
            moq_subscribe_cfg_init(&sc);
            sc.track_namespace.parts = (moq_bytes_t *)ns;
            sc.track_namespace.count = 1;
            sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            sv->subscribed = true;
            (void)moq_session_subscribe(s, &sc, now, &sub);
        }
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

static int sub_drained(struct sub_side *sv)
{
    pthread_mutex_lock(&sv->mu);
    int d = sv->drained;
    pthread_mutex_unlock(&sv->mu);
    return d;
}
static uint64_t sub_pumps(struct sub_side *sv)
{
    pthread_mutex_lock(&sv->mu);
    uint64_t p = sv->pumps;
    pthread_mutex_unlock(&sv->mu);
    return p;
}
static bool sub_ready(struct sub_side *sv)
{
    pthread_mutex_lock(&sv->mu);
    bool r = sv->setup > 0 && sv->subscribed;
    pthread_mutex_unlock(&sv->mu);
    return r;
}
static uint64_t lane_pump_sweeps(moq_wtquic_msquic_managed_t *m)
{
    moq_wtquic_msquic_managed_lane_t *lane =
        moq_wtquic_msquic_managed_lane(m, 0);
    moq_wtquic_msquic_lane_stats_t st;
    if (lane == NULL ||
        moq_wtquic_msquic_lane_get_stats(lane, &st, sizeof(st)) != MOQ_OK)
        return UINT64_MAX;
    return st.pump_sweeps;
}

/* ------------------------------------------------------------------ *
 * transport bring-up shared by both cases: raw server listener + managed
 * client facade dialing it. Returns 0 on success (out handles owned by caller).
 * ------------------------------------------------------------------ */
struct rig {
    struct pub_side pub;
    moq_session_t *pub_ms;
    wtq_msquic_env_t *senv;
    wtq_msquic_listener_t *listener;
    moq_wtquic_msquic_managed_t *cli;
};

static int rig_up(struct rig *r, struct sub_side *sub, const char *cert,
                  const char *key, uint32_t cap,
                  moq_wtquic_msquic_lane_pump_fn pump)
{
    memset(r, 0, sizeof(*r));
    pthread_mutex_init(&r->pub.mu, NULL);

    /* raw server session + attach adapter (publisher, driven by pub_hook) */
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = 1;
    scfg.initial_request_capacity = 16;
    scfg.max_events = 256;
    if (moq_session_create(&scfg, 0, &r->pub_ms) < 0) return -1;
    r->pub.ms = r->pub_ms;
    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.session = r->pub_ms;
    ccfg.hook = pub_hook;
    ccfg.hook_user = &r->pub;
    if (moq_wtquic_conn_create(&ccfg, &r->pub.conn) < 0) return -1;

    wtq_msquic_env_cfg_t secfg = WTQ_MSQUIC_ENV_CFG_INIT;
    secfg.tuning.idle_timeout_ms = 300000; /* HANG GUARD ONLY: keep the QUIC
        conn alive across a receiver stall so the stall is nonfatal/observable */
    if (wtq_msquic_env_open(&secfg, &r->senv) != WTQ_OK) return -1;
    static const char *const server_protos[] = { "moqt-16" };
    wtq_serve_config_t serve = WTQ_SERVE_CONFIG_INIT;
    serve.path = "/moq";
    serve.subprotocols = server_protos;
    serve.subprotocol_count = 1;
    serve.require_subprotocol = true;
    wtq_msquic_listener_cfg_t lcfg = WTQ_MSQUIC_LISTENER_CFG_INIT;
    lcfg.bind_address = "127.0.0.1";
    lcfg.port = 0;
    lcfg.cert_file = cert;
    lcfg.key_file = key;
    lcfg.paths = &serve;
    lcfg.path_count = 1;
    lcfg.events = moq_wtquic_conn_events();
    lcfg.user = r->pub.conn;
    if (wtq_msquic_listener_start(r->senv, &lcfg, &r->listener) != WTQ_OK) return -1;
    uint16_t port = wtq_msquic_listener_port(r->listener);

    /* managed client facade (subscriber under test) */
    static const char *const offers[] = { "moqt-16" };
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.wt_path = "/moq";
    cfg.wt_protocols = offers;
    cfg.wt_protocol_count = 1;
    cfg.idle_timeout_ms = 300000; /* HANG GUARD ONLY (matches server) */
    cfg.max_events = cap;
    cfg.on_lane_pump = pump;
    cfg.on_lane_pump_user = sub;
    if (moq_wtquic_msquic_managed_create(&cfg, &r->cli) != MOQ_OK) return -1;
    sub->m = r->cli;
    return 0;
}

static void rig_down(struct rig *r)
{
    if (r->cli) {
        (void)moq_wtquic_msquic_managed_stop(r->cli);
        moq_wtquic_msquic_managed_destroy(r->cli);
    }
    if (r->listener) wtq_msquic_listener_stop(r->listener);
    if (r->senv) wtq_msquic_env_close(r->senv);
    if (r->pub.conn) moq_wtquic_conn_destroy(r->pub.conn);
    if (r->pub_ms) moq_session_destroy(r->pub_ms);
    pthread_mutex_destroy(&r->pub.mu);
}

/* ------------------------------------------------------------------ *
 * autonomous delivery: default + large capacity.
 * ------------------------------------------------------------------ */
static void t_autonomous(const char *cert, const char *key, uint32_t cap)
{
    int before = g_fail;
    struct sub_side sv;
    struct rig r;
    memset(&sv, 0, sizeof(sv));
    pthread_mutex_init(&sv.mu, NULL);
    sv.seen = calloc(OBJ_N, 1);
    CHECK(sv.seen != NULL);
    if (!sv.seen) return;

    if (rig_up(&r, &sv, cert, key, cap, sub_pump) != 0) {
        CHECK(0 && "rig_up");
        rig_down(&r);
        free(sv.seen);
        return;
    }

    /* Phase A — SETUP: the managed client may be woken until it has subscribed
     * (this is setup). The raw server runs autonomously on its own worker. */
    bool ready = false;
    for (int i = 0; i < 40000 && !ready; i++) {
        (void)moq_wtquic_msquic_managed_wake(r.cli);
        (void)moq_wtquic_msquic_managed_wait(r.cli, 2000);
        ready = sub_ready(&sv);
    }
    CHECK(ready);

    /* Phase B — AUTONOMOUS delivery: NO managed wake to the client. Its pumps
     * are the transport-driven ones the doorbell schedules. Each loop iteration
     * is one bounded WAIT — a driving generation (deterministic iteration count,
     * NOT an elapsed-time cadence). A fully un-pumped stall idles the doorbell,
     * so delivery detection must be keyed on driving generations here, not the
     * client's own pump count (which freezes when the doorbell goes idle).
     * STALL = STALL_QUIET_GENS consecutive driving generations with no new
     * delivery. The long QUIC idle keeps the connection alive across the stall
     * so it stays nonfatal and observable. */
    enum { STALL_QUIET_GENS = 3000 };
    bool full = false;
    int last_drained = -1;
    int quiet_gens = 0;
    for (int i = 0; i < 400000 && !full; i++) {
        (void)moq_wtquic_msquic_managed_wait(r.cli, 2000);  /* hang guard only */
        int d = sub_drained(&sv);
        full = d >= OBJ_N;
        if (d != last_drained) {
            last_drained = d;
            quiet_gens = 0;
        } else if (++quiet_gens > STALL_QUIET_GENS) {
            break;                          /* stalled: no delivery for many gens */
        }
    }

    int drained = sub_drained(&sv);
    bool cli_fatal = moq_wtquic_msquic_managed_is_fatal(r.cli);
    int pub_err = pub_publish_errors(&r.pub);

    if (!full) {
        pthread_mutex_lock(&r.pub.mu);
        uint64_t pcur = r.pub.cursor; bool ppub = r.pub.published;
        pthread_mutex_unlock(&r.pub.mu);
        fprintf(stderr,
                "WTQ-MSQ AUTONOMOUS STALL[cap=%u]: drained=%d/%d (=%d*%d + %d) "
                "quiet_gens>600 cli_fatal=%d pub_err=%d pub_cursor=%llu "
                "pub_done=%d srv_closed=%d srv_fatal=%d\n",
                cap, drained, OBJ_N, drained / POLL, POLL, drained % POLL,
                cli_fatal, pub_err, (unsigned long long)pcur, ppub,
                moq_wtquic_conn_is_closed(r.pub.conn),
                moq_wtquic_conn_is_fatal(r.pub.conn));
        pthread_mutex_lock(&sv.mu);
        fprintf(stderr,
                "  close-record: closed_seen=%d code=%llu at drained=%d "
                "pump=%llu (test_stopping was %d when it fired)\n",
                sv.closed_seen, (unsigned long long)sv.closed_code,
                sv.closed_at_drained, (unsigned long long)sv.closed_at_pump,
                0);
        pthread_mutex_unlock(&sv.mu);
    }

    /* From here a SESSION_CLOSED is OUR teardown, not unsolicited. */
    pthread_mutex_lock(&sv.mu);
    sv.test_stopping = true;
    pthread_mutex_unlock(&sv.mu);
    rig_down(&r);

    /* Acceptance: NO unsolicited session close before full delivery. */
    CHECK(!(sv.closed_seen && sv.closed_at_drained < OBJ_N));
    CHECK(full);                    /* autonomous delivery reached the whole track */
    CHECK(drained == OBJ_N);
    CHECK(sv.dup_objects == 0);     /* exactly once */
    CHECK(sv.object_errors == 0);   /* byte-exact */
    int missing = 0;
    for (int i = 0; i < OBJ_N; i++)
        if (sv.seen[i] != 1) missing++;
    CHECK(missing == 0);
    CHECK(pub_err == 0);
    CHECK(!cli_fatal);

    if (g_fail == before)
        printf("PASS: wtq_msquic_autonomous[cap=%u] delivered=%d/%d\n",
               cap, drained, OBJ_N);

    free(sv.seen);
    pthread_mutex_destroy(&sv.mu);
}

/* ------------------------------------------------------------------ *
 * anti-spin: a non-draining subscriber must not busy-loop the doorbell.
 * Measured with pump-generation counters, not elapsed time.
 * ------------------------------------------------------------------ */
static int nospin_pump(moq_wtquic_msquic_managed_t *m,
                       moq_wtquic_msquic_managed_lane_t *lane, uint64_t now,
                       void *ctx)
{
    struct sub_side *sv = ctx;
    moq_session_t *s = moq_wtquic_msquic_managed_session(m);
    (void)lane;
    (void)now;
    pthread_mutex_lock(&sv->mu);
    sv->pumps++;
    if (s != NULL && !sv->freeze) {
        moq_event_t ev[POLL];
        size_t ne = 0;
        (void)moq_session_poll_events_ex(s, ev, POLL, sizeof(ev[0]), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SETUP_COMPLETE) sv->setup++;
            moq_event_cleanup(&ev[i]);
        }
        if (sv->setup > 0 && !sv->subscribed) {
            static const moq_bytes_t ns[] = { { (const uint8_t *)"wtqc", 4 } };
            moq_subscribe_cfg_t sc;
            moq_subscription_t sub;
            moq_subscribe_cfg_init(&sc);
            sc.track_namespace.parts = (moq_bytes_t *)ns;
            sc.track_namespace.count = 1;
            sc.track_name = (moq_bytes_t){ (const uint8_t *)"track", 5 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            sv->subscribed = true;
            (void)moq_session_subscribe(s, &sc, now, &sub);
            sv->freeze = true;   /* drain nothing after subscribe */
        }
    }
    pthread_mutex_unlock(&sv->mu);
    return 0;
}

static void t_nondraining_no_spin(const char *cert, const char *key)
{
    int before = g_fail;
    struct sub_side sv;
    struct rig r;
    memset(&sv, 0, sizeof(sv));
    pthread_mutex_init(&sv.mu, NULL);

    if (rig_up(&r, &sv, cert, key, 0, nospin_pump) != 0) {
        CHECK(0 && "rig_up");
        rig_down(&r);
        return;
    }

    bool ready = false;
    for (int i = 0; i < 40000 && !ready; i++) {
        (void)moq_wtquic_msquic_managed_wake(r.cli);
        (void)moq_wtquic_msquic_managed_wait(r.cli, 2000);
        ready = sub_ready(&sv);
    }
    CHECK(ready);

    /* Prime: drive a fixed number of bounded-wait generations so the server
     * publishes and the frozen client absorbs the burst (then its receive
     * pauses — it drains nothing). Each iteration is one driving generation
     * (deterministic count, not elapsed time). */
    enum { NOSPIN_PRIME_GENS = 2000, NOSPIN_QUIET_GENS = 4000 };
    for (int i = 0; i < NOSPIN_PRIME_GENS; i++)
        (void)moq_wtquic_msquic_managed_wait(r.cli, 2000);

    /* QUIET window: NOSPIN_QUIET_GENS driving generations, NO wake. Measure the
     * client doorbell's OWN pump-sweep count across it: a correctly gated
     * re-drive idles for a non-draining app (few sweeps); an unconditional
     * re-arm would spin (a sweep per idle tick, far more than the driving
     * generations). Pump-sweep count is the pump-generation counter here. */
    uint64_t sweeps0 = lane_pump_sweeps(r.cli);
    for (int i = 0; i < NOSPIN_QUIET_GENS; i++)
        (void)moq_wtquic_msquic_managed_wait(r.cli, 2000);  /* quiet: no wake */
    uint64_t sweeps1 = lane_pump_sweeps(r.cli);

    bool cli_fatal = moq_wtquic_msquic_managed_is_fatal(r.cli);
    rig_down(&r);

    CHECK(sweeps0 != UINT64_MAX && sweeps1 != UINT64_MAX);
    /* Bounded well below a spin: an idle doorbell runs at most a handful of
     * sweeps across the quiet window (transport-driven), while a per-tick spin
     * would run thousands. */
    uint64_t delta = sweeps1 - sweeps0;
    CHECK(delta < (uint64_t)NOSPIN_QUIET_GENS);
    CHECK(!cli_fatal);

    printf("WTQ-MSQ NO-SPIN: non-draining quiet-window pump-sweeps delta=%llu "
           "(bound<4000)\n", (unsigned long long)delta);

    if (g_fail == before)
        printf("PASS: wtq_msquic_nondraining_no_spin\n");
    pthread_mutex_destroy(&sv.mu);
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    t_autonomous(argv[1], argv[2], 0);      /* default event capacity */
    t_autonomous(argv[1], argv[2], 8192);   /* large event capacity */
    t_nondraining_no_spin(argv[1], argv[2]);

    if (g_fail != 0) {
        fprintf(stderr, "FAILED: test_wtquic_msquic_managed_autodrain (%d)\n",
                g_fail);
        return 1;
    }
    printf("PASS: test_wtquic_msquic_managed_autodrain\n");
    return 0;
}
