/*
 * Sans-I/O Architecture Demo
 *
 * This single file demonstrates everything that makes libmoq's
 * architecture distinctive:
 *
 *   1. Two sessions wired back-to-back with zero sockets
 *   2. Deterministic setup handshake via action pumping
 *   3. Protocol-violation error injection and clean recovery
 *   4. Borrow-epoch validation (use-after-invalidation detection)
 *   5. Counting allocator proving zero memory leaks
 *   6. Virtual time threading (every advancing call takes now_us)
 *   7. First-class SimPair automation of the raw pump pattern
 *
 * Compare this with Vista (Rust/SimPair), moqt-go (zero-goroutine),
 * or Playa (TypeScript/sans-I/O). Same architecture, pure C, no
 * async runtime, no hidden allocator, no global state.
 */

#include <moq/moq.h>
#ifdef MOQ_DEMO_HAS_SIM
#include <moq/sim.h>
#endif
#include <stdio.h>
#include <stdlib.h>

/* ================================================================== */
/*  1. Counting Allocator - proves zero leaks                        */
/* ================================================================== */

typedef struct {
    int64_t allocs;
    int64_t frees;
} alloc_stats_t;

static void *counted_alloc(size_t size, void *ctx)
{
    alloc_stats_t *s = (alloc_stats_t *)ctx;
    if (size == 0)
        return NULL;
    void *p = malloc(size);
    if (p) s->allocs++;
    return p;
}

static void *counted_realloc(void *ptr, size_t old_size, size_t new_size,
                             void *ctx)
{
    alloc_stats_t *s = (alloc_stats_t *)ctx;
    (void)old_size;
    if (new_size == 0) {
        if (ptr) {
            free(ptr);
            s->frees++;
        }
        return NULL;
    }
    if (!ptr) {
        void *p = malloc(new_size);
        if (p) s->allocs++;
        return p;
    }
    return realloc(ptr, new_size);
}

static void counted_free(void *ptr, size_t size, void *ctx)
{
    alloc_stats_t *s = (alloc_stats_t *)ctx;
    (void)size;
    if (ptr) s->frees++;
    free(ptr);
}

/* ================================================================== */
/*  2. Action Pump - the core of sans-I/O wiring                     */
/* ================================================================== */
/*
 * This is exactly what SimPair automates and what a QUIC adapter does
 * in production. One session's outbound actions become the other's
 * inbound bytes. No sockets, no threads, fully deterministic.
 *
 * Production adapters should close the underlying transport when
 * close_actions is nonzero.
 */

static moq_result_t pump(moq_session_t *from, moq_session_t *to,
                         uint64_t now, size_t *out_close_actions)
{
    moq_action_t acts[16];
    size_t n;
    size_t close_actions = 0;
    while ((n = moq_session_poll_actions(from, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_result_t rc = moq_session_on_control_bytes(to,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len,
                    now);
                if (rc < 0) return rc;
            } else if (acts[i].kind == MOQ_ACTION_CLOSE_SESSION) {
                close_actions++;
            } else {
                return MOQ_ERR_INVAL;
            }
        }
    }
    if (out_close_actions)
        *out_close_actions = close_actions;
    return MOQ_OK;
}

/* ================================================================== */
/*  Helpers                                                           */
/* ================================================================== */

static int g_pass = 0;
static int g_fail = 0;

#define ASSERT(expr) do { \
    if (expr) { g_pass++; } \
    else { g_fail++; fprintf(stderr, "  FAIL: %s:%d: %s\n", \
           __FILE__, __LINE__, #expr); } \
} while (0)

#define SECTION(name) printf("\n--- %s ---\n", name)

/* ================================================================== */
/*  main                                                              */
/* ================================================================== */

int main(void)
{
    alloc_stats_t stats = {0};
    moq_alloc_t alloc = { &stats, counted_alloc, counted_realloc, counted_free };

    uint64_t now = 1000; /* virtual time in microseconds */

    /* ============================================================== */
    SECTION("1. Deterministic Setup Handshake");
    /* ============================================================== */
    /*
     * Two sessions, back-to-back, byte-pumped. This is the SimPair
     * pattern: no QUIC stack, no sockets, no threads.
     */
    {
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_request_capacity = true;       
        ccfg.initial_request_capacity = 100;    

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_request_capacity = true;       
        scfg.initial_request_capacity = 50;      

        moq_session_t *client = NULL, *server = NULL;
        ASSERT(moq_session_create(&ccfg, now, &client) == MOQ_OK);
        ASSERT(moq_session_create(&scfg, now, &server) == MOQ_OK);

        /* Client initiates setup. Server is ready after create. */
        ASSERT(moq_session_start(client, now) == MOQ_OK);
        ASSERT(moq_session_state(client) == MOQ_SESS_SETUP_SENT);

        /* Pump client setup → server processes → server responds. */
        ASSERT(pump(client, server, now, NULL) == MOQ_OK);
        ASSERT(moq_session_state(server) == MOQ_SESS_ESTABLISHED);

        /* Pump server response → client becomes ESTABLISHED. */
        ASSERT(pump(server, client, now, NULL) == MOQ_OK);
        ASSERT(moq_session_state(client) == MOQ_SESS_ESTABLISHED);

        /* Both sides see SETUP_COMPLETE with correct perspectives. */
        moq_event_t ev;
        ASSERT(moq_session_poll_events(server, &ev, 1) == 1);
        ASSERT(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
        ASSERT(ev.u.setup_complete.local_perspective == MOQ_PERSPECTIVE_SERVER);
        ASSERT(ev.u.setup_complete.peer_perspective == MOQ_PERSPECTIVE_CLIENT);

        ASSERT(moq_session_poll_events(client, &ev, 1) == 1);
        ASSERT(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
        ASSERT(ev.u.setup_complete.local_perspective == MOQ_PERSPECTIVE_CLIENT);

        printf("  client + server: ESTABLISHED\n");

        moq_session_destroy(client);
        moq_session_destroy(server);
    }

    /* ============================================================== */
    SECTION("2. Protocol Violation - Error Injection");
    /* ============================================================== */
    /*
     * Feed an unknown control message type to a session. The spec
     * says the session MUST close. We verify the close is clean:
     * deterministic error code, reason bytes, and a single
     * CLOSE_SESSION action for the adapter to execute.
     */
    {
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, now, &s);
        moq_session_start(s, now);

        /* Drain the setup action so the queue is clean. */
        moq_action_t drain[4];
        moq_session_poll_actions(s, drain, 4);

        /* Inject a prebuilt unknown control message with zero payload. */
        const uint8_t bad_msg[] = { 0x40, 0xFF, 0x00, 0x00 };

        now += 1000;
        ASSERT(moq_session_on_control_bytes(s, bad_msg,
            sizeof(bad_msg), now) == MOQ_OK);
        ASSERT(moq_session_state(s) == MOQ_SESS_CLOSED);

        /* Session closed event has structured error info. */
        moq_event_t ev;
        ASSERT(moq_session_poll_events(s, &ev, 1) == 1);
        ASSERT(ev.kind == MOQ_EVENT_SESSION_CLOSED);
        ASSERT(ev.u.closed.code == 0x3); /* PROTOCOL_VIOLATION */
        ASSERT(ev.u.closed.reason.len > 0);
        ASSERT(ev.u.closed.reason.data != NULL);

        printf("  close code=0x%llx reason=\"%.*s\"\n",
               (unsigned long long)ev.u.closed.code,
               (int)ev.u.closed.reason.len, (const char *)ev.u.closed.reason.data);

        /* Exactly one CLOSE_SESSION action, no stale sends. */
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(s, acts, 4);
        ASSERT(na == 1);
        ASSERT(acts[0].kind == MOQ_ACTION_CLOSE_SESSION);
        printf("  adapter action: close session code=0x%llx\n",
               (unsigned long long)acts[0].u.close_session.code);

        /* Further bytes are rejected deterministically. */
        ASSERT(moq_session_on_control_bytes(s, bad_msg, 1, now) == MOQ_ERR_CLOSED);

        moq_session_destroy(s);
    }

    /* ============================================================== */
    SECTION("3. Borrow Epoch - Use-After-Invalidation Detection");
    /* ============================================================== */
    /*
     * Every action/event carries a borrow_epoch. Pointers in that
     * action/event are valid as long as the epoch matches the
     * session's current epoch. Any advancing call bumps the epoch,
     * invalidating old borrows.
     *
     * Rust has compile-time borrow checking. In C, libmoq exposes an
     * explicit runtime validation hook for debug assertions and tests.
     */
    {
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, now, &s);
        moq_session_start(s, now);

        /* Poll an action - its epoch is valid right now. */
        moq_action_t act;
        ASSERT(moq_session_poll_actions(s, &act, 1) == 1);
        ASSERT(act.kind == MOQ_ACTION_SEND_CONTROL);
        ASSERT(act.u.send_control.data != NULL);
        ASSERT(act.u.send_control.len > 0);

        uint64_t epoch = act.borrow_epoch;
        ASSERT(moq_session_borrow_valid(s, epoch) == true);

        printf("  epoch %llu: valid (pointers safe to read)\n",
               (unsigned long long)epoch);

        /* Make an advancing call - epoch increments, old borrows die. */
        now += 1000;
        moq_session_tick(s, now);
        ASSERT(moq_session_borrow_valid(s, epoch) == false);

        printf("  epoch %llu: INVALID after advancing call\n",
               (unsigned long long)epoch);

        /* A debug wrapper can assert moq_session_borrow_valid(s, epoch)
         * before using borrowed pointers. In release, the caller is
         * responsible for not using stale pointers. */

        moq_session_destroy(s);
    }

    /* ============================================================== */
    SECTION("4. Zero-Leak Proof - Counting Allocator");
    /* ============================================================== */
    /*
     * The counting allocator tracks every alloc/free through the
     * session's lifetime.
     * After destroy, the balance must be zero.
     */
    {
        printf("  total allocs: %lld\n", (long long)stats.allocs);
        printf("  total frees:  %lld\n", (long long)stats.frees);
        ASSERT(stats.allocs == stats.frees);
        printf("  balance: %lld (zero = no leaks)\n",
               (long long)(stats.allocs - stats.frees));
    }

    /* ============================================================== */
    SECTION("5. Virtual Time - No System Clock");
    /* ============================================================== */
    /*
     * Every function that can advance state takes uint64_t now_us.
     * The library never calls clock_gettime or gettimeofday.
     * Tests and SimPair control time directly.
     */
    {
        printf("  final virtual time: %llu us\n", (unsigned long long)now);
        printf("  no system clock was read during this entire demo\n");

        /* Default config has no idle timeout, so deadline is UINT64_MAX. */
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *s = NULL;
        moq_session_create(&cfg, now, &s);
        ASSERT(moq_session_next_deadline_us(s) == UINT64_MAX);
        moq_session_destroy(s);
    }

    /* ============================================================== */
    SECTION("6. SimPair - Automated Deterministic Pump");
    /* ============================================================== */
#ifdef MOQ_DEMO_HAS_SIM
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.seed = 0xC0FFEE;
        cfg.initial_now_us = now;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 100;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 50;

        moq_simpair_t *sp = NULL;
        ASSERT(moq_simpair_create(&cfg, &sp) == MOQ_OK);
        ASSERT(moq_simpair_start(sp) == MOQ_OK);

        size_t steps = 0;
        ASSERT(moq_simpair_run_until_quiescent(sp, 8, &steps) == MOQ_OK);
        ASSERT(steps == 2);
        ASSERT(moq_session_state(moq_simpair_client(sp)) == MOQ_SESS_ESTABLISHED);
        ASSERT(moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        ASSERT(moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1);
        ASSERT(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
        ASSERT(ev.u.setup_complete.local_perspective == MOQ_PERSPECTIVE_CLIENT);
        ASSERT(moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1);
        ASSERT(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
        ASSERT(ev.u.setup_complete.local_perspective == MOQ_PERSPECTIVE_SERVER);

        ASSERT(moq_simpair_advance_to(sp, now + 1000) == MOQ_OK);
        ASSERT(moq_simpair_now_us(sp) == now + 1000);

        printf("  seed=0x%llx steps=%llu state=ESTABLISHED\n",
               (unsigned long long)moq_simpair_seed(sp),
               (unsigned long long)steps);

        moq_simpair_destroy(sp);
        now += 1000;
    }
#else
    {
        printf("  moq-sim disabled in this build\n");
    }
#endif

    /* ============================================================== */
    SECTION("7. API Safety");
    /* ============================================================== */
    {
        /* destroy(NULL) is a no-op. */
        moq_session_destroy(NULL);
        printf("  destroy(NULL): no crash\n");

        /* Server start is rejected (client-only). */
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        moq_session_t *s = NULL;
        moq_session_create(&cfg, now, &s);
        ASSERT(moq_session_start(s, now) == MOQ_ERR_WRONG_STATE);
        printf("  server start rejected: MOQ_ERR_WRONG_STATE\n");
        moq_session_destroy(s);

        /* NULL guards on all public functions. */
        ASSERT(moq_session_create(NULL, 0, NULL) == MOQ_ERR_INVAL);
        ASSERT(moq_session_start(NULL, 0) == MOQ_ERR_INVAL);
        ASSERT(moq_session_on_control_bytes(NULL, NULL, 0, 0) == MOQ_ERR_INVAL);
        ASSERT(moq_session_state(NULL) == MOQ_SESS_CLOSED);
        ASSERT(moq_session_borrow_valid(NULL, 0) == false);
        printf("  NULL guards: all safe\n");
    }

    /* ============================================================== */
    /* Final leak check (covers sections 5 and 6 too). */
    ASSERT(stats.allocs == stats.frees);

    /* ============================================================== */
    printf("\n=== Results: %d passed, %d failed ===\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
