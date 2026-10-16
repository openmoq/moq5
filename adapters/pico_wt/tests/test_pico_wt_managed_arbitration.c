/*
 * Session-terminal arbitration through the managed facade, both drafts, over
 * real loopback UDP.
 *
 * One side (the actor) ends the WebTransport session in a scripted way from its
 * own network thread; the other side (the receiver) must reach the terminal the
 * adapter's rules give:
 *   - a WT_SESSION_GONE reset of the actor's MoQ control stream decides
 *     nothing, and a following close capsule closes the receiver with its code;
 *   - a GONE reset followed by an abrupt connection close is a transport
 *     failure (0x1), never a clean close;
 *   - a GONE reset followed by an orderly connection close is a clean close 0;
 *   - a close capsule followed by an abrupt connection close keeps the
 *     capsule's code -- asserted only once the actor's close succeeded and
 *     the receiver's loss path ran after the capsule;
 *   - the receiver itself closing the connection with an application error,
 *     with no WebTransport terminal, is a transport failure (0x1): a local
 *     application error is not an orderly end.
 * A server receiver learns of the connection's end through h3zero's session
 * deregistration; a client receiver through the facade's own disconnect
 * check. Both paths are the product's, not the test's.
 *
 * White-box: the facade and adapter sources are compiled in with
 * MOQ_PICO_WT_TESTING to reach the actor's adapter and connection from its
 * on_pump callback.
 */
#include <moq/pico_wt_managed.h>
#include <moq/pico_wt.h>
#include <moq/moq.h>
#include "pico_wt_adapter.h"
#include "pico_wt_test_seam.h"

#include <pico_webtransport.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

static int failures;
#define CHECK(e) do { if (!(e)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #e); failures++; } } while (0)

#define WT_SESSION_GONE      0x170d7b68ull
#define H3_REQUEST_CANCELLED 0x10cull

typedef enum {
    SC_RESET_CAPSULE,     /* GONE reset of control, then close capsule 7 */
    SC_RESET_ABRUPT,      /* GONE reset of control, then abrupt close */
    SC_RESET_ORDERLY,     /* GONE reset of control, then orderly close */
    SC_CAPSULE_ABRUPT,    /* close capsule 7, then abrupt close */
    SC_SELF_ABRUPT        /* the receiver closes abruptly, no WT terminal */
} scenario_t;

static const char *sc_name(scenario_t s)
{
    switch (s) {
    case SC_RESET_CAPSULE:  return "reset+capsule";
    case SC_RESET_ABRUPT:   return "reset+abrupt";
    case SC_RESET_ORDERLY:  return "reset+orderly";
    case SC_SELF_ABRUPT:    return "self-abrupt";
    default:                return "capsule+abrupt";
    }
}

typedef struct {
    scenario_t    sc;
    moq_version_t v;
    int           step;
    int           idle;          /* pumps since the last step */
    atomic_int    established;
    atomic_int    close_rc;      /* picoquic_close result; -1 until called */
    _Atomic(moq_pico_wt_managed_t *) receiver;   /* the other facade */
} actor_t;

/* The receiver's adapter, published from its network thread, and the loss
 * reports the adapter made for it (after the session had ended, or not). */
static _Atomic(moq_pico_wt_conn_t *) g_rcv_conn;
static atomic_int g_lost_after_terminal, g_lost_before_terminal;

static void on_quic_lost(moq_pico_wt_conn_t *c, bool was_terminal)
{
    if (c != atomic_load(&g_rcv_conn)) return;
    if (was_terminal) atomic_fetch_add(&g_lost_after_terminal, 1);
    else              atomic_fetch_add(&g_lost_before_terminal, 1);
}

static int receiver_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)now; (void)ctx;
    moq_pico_wt_conn_t *c = moq_pico_wt_managed_test_conn(m);
    if (c) atomic_store(&g_rcv_conn, c);
    return 0;
}

/* -- bounded observation ------------------------------------------- *
 * A terminal facade's wait() returns at once, and the receiver is terminal
 * while the test still waits for its loss report. Observation is therefore
 * bounded by a monotonic deadline, and a wait that returns at once is
 * followed by a short sleep, so the budget is wall time, never a count. */
static moq_result_t (*g_wait_fn)(moq_pico_wt_managed_t *, uint64_t) =
    moq_pico_wt_managed_wait;

static int64_t us_until(const struct timespec *end)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    int64_t us = (int64_t)(end->tv_sec - now.tv_sec) * 1000000 +
                 (end->tv_nsec - now.tv_nsec) / 1000;
    return us > 0 ? us : 0;
}

static struct timespec after_ms(int64_t ms)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    t.tv_sec += (time_t)(ms / 1000);
    t.tv_nsec += (long)((ms % 1000) * 1000000);
    if (t.tv_nsec >= 1000000000L) { t.tv_sec++; t.tv_nsec -= 1000000000L; }
    return t;
}

/* One observation step on `m`: wait at most `step_us` (cut to the time
 * left); if that returned at once, yield for up to the same slice. */
static void observe_step(moq_pico_wt_managed_t *m, const struct timespec *end,
                         int64_t step_us)
{
    int64_t left = us_until(end);
    if (left == 0) return;
    int64_t slice = left < step_us ? left : step_us;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    (void)g_wait_fn(m, (uint64_t)slice);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    int64_t spent = (int64_t)(t1.tv_sec - t0.tv_sec) * 1000000 +
                    (t1.tv_nsec - t0.tv_nsec) / 1000;
    if (spent < slice / 2) {
        int64_t nap = slice - spent;
        struct timespec ts = { .tv_sec = (time_t)(nap / 1000000),
                               .tv_nsec = (long)((nap % 1000000) * 1000) };
        nanosleep(&ts, NULL);
    }
}

/* Control: against a wait that always returns at once (a terminal facade),
 * observation lasts its whole budget and yields between waits instead of
 * spinning: about one wait per slice, not thousands. */
static int g_fast_calls;
static moq_result_t wait_returns_at_once(moq_pico_wt_managed_t *m, uint64_t us)
{ (void)m; (void)us; g_fast_calls++; return MOQ_ERR_CLOSED; }

static void observe_fast_wake_control(void)
{
    g_wait_fn = wait_returns_at_once;
    g_fast_calls = 0;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec end = after_ms(150);
    while (us_until(&end) > 0) observe_step(NULL, &end, 50000);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    g_wait_fn = moq_pico_wt_managed_wait;
    int64_t el = (int64_t)(t1.tv_sec - t0.tv_sec) * 1000000 +
                 (t1.tv_nsec - t0.tv_nsec) / 1000;
    CHECK(el >= 150000);
    CHECK(g_fast_calls >= 1 && g_fast_calls <= 8);   /* 150 ms / 50 ms slices */
}

static int idle_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{ (void)m; (void)now; (void)ctx; return 0; }

/* Network thread of the actor. */
static int actor_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    actor_t *a = (actor_t *)ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    if (a->step == 0 &&
        (s == NULL || moq_session_state(s) != MOQ_SESS_ESTABLISHED))
        return 0;
    atomic_store(&a->established, 1);
    moq_pico_wt_conn_t *conn = moq_pico_wt_managed_test_conn(m);
    picoquic_cnx_t *cnx = moq_pico_wt_managed_test_cnx(m);
    if (conn == NULL || cnx == NULL) return 0;
    if (a->step > 0 && ++a->idle < 3) return 0;   /* let the last step fly */
    /* A capsule is only known to have arrived once the receiver closed on
     * it; closing the connection before that could drop the capsule. */
    if (a->sc == SC_CAPSULE_ABRUPT && a->step == 1) {
        moq_pico_wt_managed_t *r = atomic_load(&a->receiver);
        if (r == NULL || !moq_pico_wt_managed_is_closed(r)) return 0;
    }
    a->idle = 0;

    uint64_t ctrl = a->v == MOQ_VERSION_DRAFT_18 ? conn->last_opened_uni_id
                                                 : conn->moq_control_stream_id;
    if (ctrl == UINT64_MAX) {
        /* draft-16 server: the control stream is the client's bidi */
        for (uint64_t sid = 4; sid < 256 && ctrl == UINT64_MAX; sid += 4)
            if (moq_transport_bridge_stream_is_control(conn->bridge, sid))
                ctrl = sid;
    }
    if (a->sc == SC_SELF_ABRUPT) {
        moq_pico_wt_conn_t *c = moq_pico_wt_managed_test_conn(m);
        if (c) atomic_store(&g_rcv_conn, c);
        if (a->step++ == 0)
            atomic_store(&a->close_rc, picoquic_close(cnx, H3_REQUEST_CANCELLED));
        return 0;
    }
    bool reset_first = a->sc != SC_CAPSULE_ABRUPT;
    switch (a->step++) {
    case 0:
        if (reset_first)
            (void)picoquic_reset_stream(cnx, ctrl, WT_SESSION_GONE);
        else
            (void)picowt_send_close_session_message(
                cnx, conn->control_stream_ctx, 7, NULL);
        break;
    case 1:
        switch (a->sc) {
        case SC_RESET_CAPSULE:
            (void)picowt_send_close_session_message(
                cnx, conn->control_stream_ctx, 7, NULL);
            break;
        case SC_RESET_ORDERLY:
            atomic_store(&a->close_rc, picoquic_close(cnx, 0));
            break;
        default:
            atomic_store(&a->close_rc, picoquic_close(cnx, H3_REQUEST_CANCELLED));
            break;
        }
        break;
    default:
        break;
    }
    return 0;
}

static moq_pico_wt_managed_t *make(moq_perspective_t persp, const char *cert,
                                   const char *key, int port, moq_version_t v,
                                   int (*pump)(moq_pico_wt_managed_t *, uint64_t, void *),
                                   void *pump_ctx)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = persp;
    cfg.port = port;
    cfg.path = "/moq";
    cfg.wt_protocols = v == MOQ_VERSION_DRAFT_18 ? "moqt-18" : "moqt-16";
    cfg.on_pump = pump;
    cfg.on_pump_ctx = pump_ctx;
    if (persp == MOQ_PERSPECTIVE_SERVER) {
        cfg.cert_path = cert;
        cfg.key_path = key;
    } else {
        cfg.host = "127.0.0.1";
        cfg.insecure_skip_verify = true;
    }
    moq_pico_wt_managed_t *m = NULL;
    if (moq_pico_wt_managed_create(&cfg, &m) != MOQ_OK) return NULL;
    return m;
}

static void teardown(moq_pico_wt_managed_t *m)
{
    if (!m) return;
    (void)moq_pico_wt_managed_stop(m);
    moq_pico_wt_managed_destroy(m);
}

/* Returns 1 when the case ran (pass or fail recorded), 0 to retry. */
static int run_case(const char *cert, const char *key, int port,
                    moq_version_t v, bool recv_server, scenario_t sc)
{
    actor_t a;
    memset(&a, 0, sizeof(a));
    a.sc = sc;
    a.v = v;
    atomic_init(&a.established, 0);
    atomic_init(&a.close_rc, -1);
    atomic_init(&a.receiver, NULL);
    atomic_store(&g_rcv_conn, NULL);
    atomic_store(&g_lost_after_terminal, 0);
    atomic_store(&g_lost_before_terminal, 0);
    bool self = sc == SC_SELF_ABRUPT;

    bool srv_acts = self ? recv_server : !recv_server;
    moq_pico_wt_managed_t *srv = make(MOQ_PERSPECTIVE_SERVER, cert, key, port, v,
        srv_acts ? actor_pump : (recv_server ? receiver_pump : idle_pump),
        srv_acts ? &a : NULL);
    if (!srv) return 0;
    for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);
    moq_pico_wt_managed_t *cli = make(MOQ_PERSPECTIVE_CLIENT, NULL, NULL,
        moq_pico_wt_managed_local_port(srv), v,
        !srv_acts ? actor_pump : (!recv_server ? receiver_pump : idle_pump),
        !srv_acts ? &a : NULL);
    if (!cli) { teardown(srv); return 0; }

    moq_pico_wt_managed_t *rcv = recv_server ? srv : cli;
    atomic_store(&a.receiver, rcv);
    moq_pico_wt_managed_t *act = srv_acts ? srv : cli;
    bool needs_loss = sc == SC_CAPSULE_ABRUPT;
    struct timespec end = after_ms(5000);
    while (us_until(&end) > 0) {
        bool done = moq_pico_wt_managed_is_closed(rcv) ||
                    moq_pico_wt_managed_is_fatal(rcv);
        if (done && (!needs_loss || atomic_load(&g_lost_after_terminal) > 0))
            break;
        observe_step(rcv, &end, 50000);
        observe_step(act, &end, 50000);
    }
    int established = atomic_load(&a.established);
    int close_rc = atomic_load(&a.close_rc);
    int lost_after = atomic_load(&g_lost_after_terminal);
    bool closed = moq_pico_wt_managed_is_closed(rcv);
    bool fatal = moq_pico_wt_managed_is_fatal(rcv);
    uint64_t close_code = moq_pico_wt_managed_close_code(rcv);
    uint64_t fatal_code = moq_pico_wt_managed_fatal_code(rcv);
    teardown(cli);
    teardown(srv);
    if (!established) return 0;

    fprintf(stderr, "[arbitration] d%d %s receiver %s: closed=%d(0x%llx) fatal=%d(0x%llx) close_rc=%d lost_after=%d\n",
            v == MOQ_VERSION_DRAFT_18 ? 18 : 16, recv_server ? "server" : "client",
            sc_name(sc), closed, (unsigned long long)close_code, fatal,
            (unsigned long long)fatal_code, close_rc, lost_after);
    switch (sc) {
    case SC_RESET_CAPSULE:
        CHECK(closed && !fatal && close_code == 7);
        break;
    case SC_CAPSULE_ABRUPT:
        /* the loss really happened, and was reported after the capsule */
        CHECK(close_rc == 0);
        CHECK(lost_after >= 1);
        CHECK(closed && !fatal && close_code == 7);
        break;
    case SC_SELF_ABRUPT:
        CHECK(close_rc == 0);
        CHECK(fatal && !closed && fatal_code == 0x1);
        break;
    case SC_RESET_ORDERLY:
        CHECK(closed && !fatal && close_code == 0);
        break;
    case SC_RESET_ABRUPT:
        CHECK(fatal && !closed && fatal_code == 0x1);
        break;
    }
    return 1;
}

int main(int argc, char **argv)
{
    if (argc < 3) { fprintf(stderr, "usage: %s <cert> <key>\n", argv[0]); return 2; }
    observe_fast_wake_control();
    pw_test_quic_lost_observer = on_quic_lost;
    unsigned base = 23000u + (unsigned)((uintptr_t)getpid() * 2654435761u) % 20000u;
    static const scenario_t scs[] = {
        SC_RESET_CAPSULE, SC_RESET_ABRUPT, SC_RESET_ORDERLY, SC_CAPSULE_ABRUPT,
        SC_SELF_ABRUPT
    };
    int n = 0;
    for (int vi = 0; vi < 2; vi++) {
        moq_version_t v = vi ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        for (int r = 0; r < 2; r++) {
            for (size_t i = 0; i < sizeof(scs) / sizeof(scs[0]); i++) {
                int ran = 0;
                for (int attempt = 0; attempt < 4 && !ran; attempt++)
                    ran = run_case(argv[1], argv[2],
                                   (int)(base + (unsigned)(n * 4 + attempt) * 7u),
                                   v, r != 0, scs[i]);
                CHECK(ran);
                n++;
            }
        }
    }
    if (failures == 0)
        printf("test_pico_wt_managed_arbitration: PASS (%d cases)\n", n);
    return failures ? 1 : 0;
}
