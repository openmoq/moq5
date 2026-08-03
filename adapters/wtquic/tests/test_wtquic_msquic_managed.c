/*
 * Black-box behavioral coverage for moq_wtquic_msquic_managed over the shipped
 * library: config sizing, owned copies, create/unwind (clean rollback from every
 * partial-create state), and the teardown coordinator lifecycle (stop/join,
 * stop_begin, on_stopped as the final facade access). Transport/negotiation,
 * server admission, the pump window, live reap, and the wake-driven doorbell are
 * covered white-box in the internal test and over real MsQuic in the loopback
 * smoke.
 *
 * Deterministic: gates and counters only, no sleeps or latency assertions.
 */
#include <moq/wtquic_msquic_managed.h>

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_fail;
#define CHECK(c)                                                            \
    do {                                                                    \
        if (!(c)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);    \
            g_fail++;                                                       \
        }                                                                   \
    } while (0)

/* Counting allocator with optional Nth-allocation failure (create/unwind).
 * THREAD-SAFE (the facade contract requires it): teardown frees the facade on
 * the coordinator thread while create allocated on the caller thread. */
typedef struct {
    pthread_mutex_t mu;
    long live;      /* outstanding allocations (0 == balanced) */
    long budget;    /* allocations remaining before failure */
    bool limited;   /* honor budget */
} acnt_t;

static void acnt_init(acnt_t *a, long budget, bool limited)
{
    pthread_mutex_init(&a->mu, NULL);
    a->live = 0;
    a->budget = budget;
    a->limited = limited;
}
static long acnt_live(acnt_t *a)
{
    pthread_mutex_lock(&a->mu);
    long v = a->live;
    pthread_mutex_unlock(&a->mu);
    return v;
}
static void acnt_destroy(acnt_t *a) { pthread_mutex_destroy(&a->mu); }

static void *acnt_alloc(size_t n, void *ctx)
{
    acnt_t *a = ctx;
    pthread_mutex_lock(&a->mu);
    bool deny = a->limited && a->budget-- <= 0;
    if (!deny)
        a->live++;
    pthread_mutex_unlock(&a->mu);
    if (deny)
        return NULL;
    void *p = malloc(n ? n : 1);
    if (p == NULL) {
        pthread_mutex_lock(&a->mu);
        a->live--;
        pthread_mutex_unlock(&a->mu);
    }
    return p;
}
static void *acnt_realloc(void *p, size_t os, size_t ns, void *ctx)
{
    acnt_t *a = ctx;
    (void)os;
    pthread_mutex_lock(&a->mu);
    bool deny = a->limited && a->budget-- <= 0;
    if (!deny && p == NULL)
        a->live++;
    pthread_mutex_unlock(&a->mu);
    if (deny)
        return NULL;
    void *q = realloc(p, ns ? ns : 1);
    if (q == NULL && p == NULL) {
        pthread_mutex_lock(&a->mu);
        a->live--;
        pthread_mutex_unlock(&a->mu);
    }
    return q;
}
static void acnt_free(void *p, size_t n, void *ctx)
{
    acnt_t *a = ctx;
    (void)n;
    if (p == NULL)
        return;
    free(p);
    pthread_mutex_lock(&a->mu);
    a->live--;
    pthread_mutex_unlock(&a->mu);
}

static int dummy_pump(moq_wtquic_msquic_managed_t *m,
                      moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                      void *user)
{
    (void)m;
    (void)lane;
    (void)now_us;
    (void)user;
    return 0;
}

/* A successful client create() opens the MsQuic env and dials, so these
 * black-box lifecycle tests target loopback with nothing listening: no DNS, the
 * async handshake never completes, and env_close aborts it immediately at
 * teardown. Deterministic and network-free; real establishment is the separate
 * white-box/loopback coverage. */
static void base_client_cfg(moq_wtquic_msquic_managed_cfg_t *cfg,
                            const moq_alloc_t *alloc)
{
    moq_wtquic_msquic_managed_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = alloc;
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->host = "127.0.0.1";
    cfg->port = 47811;
    cfg->on_lane_pump = dummy_pump;
}

/* --- config sizing + validation ------------------------------------------ */
static void test_cfg_sizing(const moq_alloc_t *alloc)
{
    moq_wtquic_msquic_managed_cfg_t cfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    CHECK(cfg.struct_size == (uint32_t)sizeof(cfg));

    /* a prefix-sized init stamps the smaller size and zeroes it (aligned for
     * the cast to the config type) */
    _Alignas(moq_wtquic_msquic_managed_cfg_t) unsigned char buf[sizeof(cfg)];
    memset(buf, 0xEE, sizeof(buf));
    size_t half = offsetof(moq_wtquic_msquic_managed_cfg_t, on_activity_ctx) +
                  sizeof(cfg.on_activity_ctx);
    moq_wtquic_msquic_managed_cfg_init_sized(
        (moq_wtquic_msquic_managed_cfg_t *)buf, half);
    CHECK(((moq_wtquic_msquic_managed_cfg_t *)buf)->struct_size ==
          (uint32_t)half);

    moq_wtquic_msquic_managed_t *m = NULL;
    /* NULL out / NULL cfg */
    CHECK(moq_wtquic_msquic_managed_create(&cfg, NULL) == MOQ_ERR_INVAL);
    CHECK(moq_wtquic_msquic_managed_create(NULL, &m) == MOQ_ERR_INVAL);
    /* too-small struct_size */
    base_client_cfg(&cfg, alloc);
    cfg.struct_size = 4;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    /* missing required fields */
    base_client_cfg(&cfg, alloc);
    cfg.on_lane_pump = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    base_client_cfg(&cfg, alloc);
    cfg.host = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    base_client_cfg(&cfg, alloc);
    cfg.perspective = (moq_perspective_t)0;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    /* server needs cert+key */
    base_client_cfg(&cfg, alloc);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);
}

/* --- ABI: heap-backed struct_size boundaries (ASan bounds the reads) ----- */
static void abi_set_v1(void *buf, uint32_t struct_size, const moq_alloc_t *alloc)
{
    /* Populate a full, correctly-typed config, then copy only the requested
     * prefix into the (possibly undersized) caller buffer. Writing through a
     * cfg-typed pointer aimed at an undersized allocation is UB that -O1 UBSan
     * (object-size) rejects; the typed-value memcpy avoids it. */
    moq_wtquic_msquic_managed_cfg_t full;
    memset(&full, 0, sizeof(full));
    full.struct_size = struct_size;
    full.alloc = alloc;
    full.perspective = MOQ_PERSPECTIVE_CLIENT;
    full.host = "127.0.0.1";
    full.port = 47811;
    full.on_lane_pump = dummy_pump;
    size_t n = struct_size < sizeof(full) ? struct_size : sizeof(full);
    memcpy(buf, &full, n);
}
static void test_cfg_abi(const moq_alloc_t *alloc)
{
    const size_t v0 = MOQ_WTQUIC_MSQUIC_MANAGED_CFG_V0_SIZE;

    /* exact v0 prefix: a legacy-sized object connects; the read never passes
     * the caller's object (heap-bounded). */
    unsigned char *buf = malloc(v0);
    if (buf != NULL) {
        memset(buf, 0, v0);
        abi_set_v1(buf, (uint32_t)v0, alloc);
        moq_wtquic_msquic_managed_t *m = NULL;
        CHECK(moq_wtquic_msquic_managed_create(
                  (moq_wtquic_msquic_managed_cfg_t *)buf, &m) == MOQ_OK);
        if (m != NULL) {
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        free(buf);
    }

    /* prefix-minus-one: rejected on struct_size alone (no over-read) */
    buf = malloc(v0 - 1);
    if (buf != NULL) {
        memset(buf, 0xEE, v0 - 1);
        /* struct_size is the first member (offset 0); a typed write through the
         * undersized buffer would be object-size UB, so set it by value copy */
        uint32_t ss = (uint32_t)(v0 - 1);
        memcpy(buf, &ss, sizeof(ss));
        moq_wtquic_msquic_managed_t *m = NULL;
        CHECK(moq_wtquic_msquic_managed_create(
                  (moq_wtquic_msquic_managed_cfg_t *)buf, &m) == MOQ_ERR_INVAL);
        CHECK(m == NULL);
        free(buf);
    }

    /* partial optional pointer: struct_size lands mid on_stopped -> the field
     * is treated as absent (not read), so on_stopped is NOT installed and
     * join() is NOT refused. */
    size_t on_stopped_off =
        offsetof(moq_wtquic_msquic_managed_cfg_t, on_stopped);
    /* sizeof the actual member (a function pointer) — object and function
     * pointer widths need not match, so sizeof(void *) would be wrong. */
    size_t partial = on_stopped_off +
        sizeof(((moq_wtquic_msquic_managed_cfg_t *)0)->on_stopped) / 2;
    buf = malloc(partial);
    if (buf != NULL) {
        /* zero the read fields (valid bools), then poison ONLY the straddled
         * optional pointer so create() must not read it */
        memset(buf, 0, partial);
        abi_set_v1(buf, (uint32_t)partial, alloc);
        memset(buf + on_stopped_off, 0xEE, partial - on_stopped_off);
        moq_wtquic_msquic_managed_t *m = NULL;
        CHECK(moq_wtquic_msquic_managed_create(
                  (moq_wtquic_msquic_managed_cfg_t *)buf, &m) == MOQ_OK);
        if (m != NULL) {
            /* on_stopped was NOT picked up from the partial field: join() is
             * permitted (would be refused with WRONG_STATE if installed). */
            CHECK(moq_wtquic_msquic_managed_stop_begin(m) == true);
            CHECK(moq_wtquic_msquic_managed_join(m) == MOQ_OK);
            moq_wtquic_msquic_managed_destroy(m);
        }
        free(buf);
    }

    /* oversized future config: accepted, unknown tail ignored */
    size_t big = sizeof(moq_wtquic_msquic_managed_cfg_t) + 64;
    buf = malloc(big);
    if (buf != NULL) {
        memset(buf, 0, big);
        abi_set_v1(buf, (uint32_t)big, alloc);
        moq_wtquic_msquic_managed_t *m = NULL;
        CHECK(moq_wtquic_msquic_managed_create(
                  (moq_wtquic_msquic_managed_cfg_t *)buf, &m) == MOQ_OK);
        if (m != NULL) {
            moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
        free(buf);
    }
}

/* --- owned copies: mutate/free caller inputs after create ---------------- */
static void test_owned_copies(const moq_alloc_t *alloc)
{
    char host[] = "127.0.0.1";
    char p0[] = "moqt-18";
    char p1[] = "moqt-16";
    const char *protos[] = { p0, p1 };
    moq_wtquic_msquic_managed_cfg_t cfg;
    base_client_cfg(&cfg, alloc);
    cfg.host = host;
    cfg.wt_protocols = protos;
    cfg.wt_protocol_count = 2;
    cfg.lane_count = 3; /* client -> forced to 1 */

    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    CHECK(m != NULL);
    if (m == NULL)
        return;
    /* Smoke: scribble every borrowed input and keep operating. The
     * DISCRIMINATING proof that the copies are deep (fails if create() only
     * shallow-copies) reads the retained values back in the white-box
     * test_wtquic_msquic_managed_internal; this path just proves nothing
     * crashes when the caller's storage is poisoned post-create. */
    memset(host, 'X', sizeof(host) - 1);
    memset(p0, 'X', sizeof(p0) - 1);
    memset(p1, 'X', sizeof(p1) - 1);
    protos[0] = NULL;
    protos[1] = NULL;
    /* client forces one lane regardless of the requested 3 */
    CHECK(moq_wtquic_msquic_managed_lane_count(m) == 1);
    CHECK(moq_wtquic_msquic_managed_lane(m, 0) != NULL);
    CHECK(moq_wtquic_msquic_managed_lane(m, 1) == NULL);
    CHECK(moq_wtquic_msquic_managed_port(m) == 0);       /* no listener yet */
    CHECK(moq_wtquic_msquic_managed_session(m) == NULL); /* no transport yet */
    CHECK(moq_wtquic_msquic_managed_negotiated_version(m) == 0);
    moq_wtquic_msquic_managed_stop(m);
    moq_wtquic_msquic_managed_destroy(m);
}

/* --- create/unwind: clean rollback from every partial state --------------- */
/* create-unwind OOM coverage lives in the white-box internal test: it uses the
 * fake-connect seam so injected failures exercise the facade's own allocations
 * and the real env_open, without driving OOM into wtquic's connect (which is
 * not OOM-hardened at the pinned revision). */

/* --- coordinator: stop()/join() blocking path ---------------------------- */
static void test_coordinator_stop_join(const moq_alloc_t *alloc)
{
    moq_wtquic_msquic_managed_cfg_t cfg;
    base_client_cfg(&cfg, alloc);
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        return;
    /* first stop_begin initiates; a second is idempotent (false) */
    CHECK(moq_wtquic_msquic_managed_stop_begin(m) == true);
    CHECK(moq_wtquic_msquic_managed_stop_begin(m) == false);
    CHECK(moq_wtquic_msquic_managed_join(m) == MOQ_OK);
    /* wake after teardown observed -> CLOSED */
    CHECK(moq_wtquic_msquic_managed_wake(m) == MOQ_ERR_CLOSED);
    CHECK(moq_wtquic_msquic_lane_wake(moq_wtquic_msquic_managed_lane(m, 0)) ==
          MOQ_ERR_CLOSED);
    /* stop() after completion is idempotent */
    CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_OK);
    moq_wtquic_msquic_managed_destroy(m);
}

/* --- coordinator: on_stopped is the final facade access ------------------ */
struct stopped_probe {
    moq_wtquic_msquic_managed_t *m;
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int calls;
    bool done;
};
static void on_stopped_cb(void *ctx)
{
    struct stopped_probe *sp = ctx;
    /* legal and expected: destroy() directly inside on_stopped, no UAF. */
    moq_wtquic_msquic_managed_destroy(sp->m);
    /* Signal completion under our OWN lock (m is gone; we cannot use it to
     * synchronize). This is the coordinator's last touch of sp. */
    pthread_mutex_lock(&sp->mu);
    sp->calls++;
    sp->done = true;
    pthread_cond_signal(&sp->cv);
    pthread_mutex_unlock(&sp->mu);
}
static void test_coordinator_on_stopped(const moq_alloc_t *alloc)
{
    struct stopped_probe sp;
    memset(&sp, 0, sizeof(sp));
    pthread_mutex_init(&sp.mu, NULL);
    pthread_cond_init(&sp.cv, NULL);
    moq_wtquic_msquic_managed_cfg_t cfg;
    base_client_cfg(&cfg, alloc);
    cfg.on_stopped = on_stopped_cb;
    cfg.on_stopped_ctx = &sp;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        return;
    sp.m = m;
    /* on_stopped owns completion: join()/stop() are refused */
    CHECK(moq_wtquic_msquic_managed_join(m) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_wtquic_msquic_managed_stop(m) == MOQ_ERR_WRONG_STATE);
    /* initiate; on_stopped fires exactly once on the coordinator and destroys */
    CHECK(moq_wtquic_msquic_managed_stop_begin(m) == true);
    /* observe completion via our own condvar (a synchronized happens-before
     * with the coordinator's last sp access), never touching m again */
    pthread_mutex_lock(&sp.mu);
    while (!sp.done)
        pthread_cond_wait(&sp.cv, &sp.mu);
    pthread_mutex_unlock(&sp.mu);
    CHECK(sp.calls == 1);
    pthread_mutex_destroy(&sp.mu);
    pthread_cond_destroy(&sp.cv);
}

/* --- lane stats: zeroed, size-validated ---------------------------------- */
static void test_lane_stats(const moq_alloc_t *alloc)
{
    moq_wtquic_msquic_managed_cfg_t cfg;
    base_client_cfg(&cfg, alloc);
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_OK);
    if (m == NULL)
        return;
    moq_wtquic_msquic_managed_lane_t *lane =
        moq_wtquic_msquic_managed_lane(m, 0);
    CHECK(moq_wtquic_msquic_lane_index(lane) == 0);
    moq_wtquic_msquic_lane_stats_t st;
    CHECK(moq_wtquic_msquic_lane_get_stats(lane, &st, sizeof(st)) == MOQ_OK);
    CHECK(st.struct_size == (uint32_t)sizeof(st));
    CHECK(st.pump_sweeps == 0 && st.flush_bytes == 0 && st.wakes_external == 0);
    /* undersized out is rejected */
    CHECK(moq_wtquic_msquic_lane_get_stats(
              lane, &st, MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE - 1) ==
          MOQ_ERR_INVAL);
    CHECK(moq_wtquic_msquic_lane_get_stats(NULL, &st, sizeof(st)) ==
          MOQ_ERR_INVAL);
    moq_wtquic_msquic_managed_stop(m);
    moq_wtquic_msquic_managed_destroy(m);
}

/* --- NULL tolerance ------------------------------------------------------ */
static void test_null_tolerance(void)
{
    moq_wtquic_msquic_managed_cfg_init_sized(NULL, 0);
    moq_wtquic_msquic_managed_destroy(NULL);
    CHECK(moq_wtquic_msquic_managed_stop_begin(NULL) == false);
    CHECK(moq_wtquic_msquic_managed_join(NULL) == MOQ_ERR_INVAL);
    CHECK(moq_wtquic_msquic_managed_stop(NULL) == MOQ_ERR_INVAL);
    CHECK(moq_wtquic_msquic_managed_lane_count(NULL) == 0);
    CHECK(moq_wtquic_msquic_managed_lane(NULL, 0) == NULL);
    CHECK(moq_wtquic_msquic_managed_conn_count(NULL) == 0);
    CHECK(moq_wtquic_msquic_managed_is_fatal(NULL) == false);
    CHECK(moq_wtquic_msquic_managed_is_closed(NULL) == false);
    CHECK(moq_wtquic_msquic_managed_negotiated_version(NULL) == 0);
    CHECK(moq_wtquic_msquic_managed_wait(NULL, 0) == MOQ_ERR_INVAL);
}

/* End-to-end over the real transport: an oversized WT-Protocol offer is
 * rejected synchronously by wtquic (WTQ_ERR_INVALID_ARG/TOO_LARGE) and the
 * facade surfaces MOQ_ERR_INVAL, not INTERNAL — and the synchronous close-time
 * cleanup neither deadlocks (guard installed) nor unbalances the allocator
 * (the wtquic abandon path). The per-code mapping itself is unit-tested via the
 * seam in the internal test. */
static void test_connect_error_mapping(const moq_alloc_t *alloc)
{
    char big[600];
    memset(big, 'a', sizeof(big) - 1);
    big[sizeof(big) - 1] = '\0';
    const char *oversized[] = { big };
    moq_wtquic_msquic_managed_cfg_t cfg;
    base_client_cfg(&cfg, alloc);
    cfg.wt_protocols = oversized;
    cfg.wt_protocol_count = 1;
    moq_wtquic_msquic_managed_t *m = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);
}

int main(void)
{
    acnt_t a;
    acnt_init(&a, 0, false);
    moq_alloc_t alloc = { &a, acnt_alloc, acnt_realloc, acnt_free };

    test_cfg_sizing(&alloc);
    test_cfg_abi(&alloc);
    test_owned_copies(&alloc);
    test_coordinator_stop_join(&alloc);
    test_coordinator_on_stopped(&alloc);
    test_lane_stats(&alloc);
    test_null_tolerance();
    test_connect_error_mapping(&alloc);

    CHECK(acnt_live(&a) == 0); /* shared allocator balanced at exit */
    acnt_destroy(&a);

    if (g_fail != 0) {
        fprintf(stderr, "FAILED: test_wtquic_msquic_managed (%d)\n", g_fail);
        return 1;
    }
    printf("PASS: test_wtquic_msquic_managed\n");
    return 0;
}
