/*
 * Managed mvfst adapter lifecycle tests.
 * No real network connection — tests create/stop/destroy discipline.
 */

#include <moq/mvfst.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

static int failures = 0;

#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Pump callbacks -------------------------------------------------- */

static int pump_exit(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 1;
}

typedef struct {
    int session_ok;
} pump_session_ctx_t;

static int pump_check_session(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    pump_session_ctx_t *c = (pump_session_ctx_t *)ctx;
    moq_session_t *s = moq_mvfst_managed_session(m);
    c->session_ok = (s != NULL) ? 1 : 0;
    return 1;
}

static int pump_count_val;
static int pump_counting(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    pump_count_val++;
    return (pump_count_val >= 3) ? 1 : 0;
}

static int pump_forever(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 0;
}

static moq_mvfst_managed_cfg_t make_cfg(moq_mvfst_pump_fn fn, void *ctx)
{
    moq_mvfst_managed_cfg_t cfg;
    /* Full current struct: these tests set appended fields (alpn/sni/...). */
    moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    /* host = NULL: lifecycle-only mode, no transport created. */
    cfg.host = NULL;
    cfg.port = 4433;
    cfg.on_pump = fn;
    cfg.user_ctx = ctx;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    return cfg;
}

/* -- Tests ----------------------------------------------------------- */

static void test_create_stop_destroy(void)
{
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    MVFST_CHECK(m != NULL);
    MVFST_CHECK(!moq_mvfst_managed_is_fatal(m));
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    MVFST_CHECK(moq_mvfst_managed_session(m) == NULL);
    moq_mvfst_managed_destroy(m);
}

static void test_destroy_without_stop(void)
{
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    moq_mvfst_managed_destroy(m);
}

static void test_stop_idempotent(void)
{
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    moq_mvfst_managed_destroy(m);
}

static void test_repeated_lifecycle(void)
{
    for (int i = 0; i < 10; i++) {
        moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
        moq_mvfst_managed_t *m = NULL;
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
        moq_mvfst_managed_destroy(m);
    }
}

static void test_session_in_pump(void)
{
    pump_session_ctx_t ctx = {0};
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_check_session, &ctx);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    for (int i = 0; i < 100; i++) {
        moq_result_t rc = moq_mvfst_managed_wait(m, 100000);
        if (rc == MOQ_ERR_CLOSED) break;
    }
    MVFST_CHECK(ctx.session_ok == 1);
    moq_mvfst_managed_destroy(m);
}

static void test_session_null_from_app_thread(void)
{
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_forever, NULL);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    /* session() from the app thread returns NULL (wrong thread). */
    MVFST_CHECK(moq_mvfst_managed_session(m) == NULL);
    moq_mvfst_managed_destroy(m);
}

static void test_pump_runs_multiple(void)
{
    pump_count_val = 0;
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_counting, NULL);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    for (int i = 0; i < 100; i++) {
        moq_result_t rc = moq_mvfst_managed_wait(m, 100000);
        if (rc == MOQ_ERR_CLOSED) break;
    }
    MVFST_CHECK(pump_count_val >= 3);
    moq_mvfst_managed_destroy(m);
}

static void test_wake_after_stop(void)
{
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    moq_mvfst_managed_stop(m);
    MVFST_CHECK(moq_mvfst_managed_wake(m) == MOQ_ERR_CLOSED);
    MVFST_CHECK(moq_mvfst_managed_wait(m, 0) == MOQ_ERR_CLOSED);
    moq_mvfst_managed_destroy(m);
}

/* Waiter thread for blocking wait test. */
typedef struct {
    moq_mvfst_managed_t *m;
    moq_result_t result;
    int done;
} waiter_ctx_t;

static void *waiter_thread_fn(void *arg)
{
    waiter_ctx_t *w = (waiter_ctx_t *)arg;
    /* Loop until we get MOQ_ERR_CLOSED. Pump activity may
     * return MOQ_OK before stop is called. */
    for (;;) {
        w->result = moq_mvfst_managed_wait(w->m, UINT64_MAX);
        if (w->result == MOQ_ERR_CLOSED) break;
    }
    w->done = 1;
    return NULL;
}

static void test_blocking_wait_wakes_on_stop(void)
{
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_forever, NULL);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);

    waiter_ctx_t wctx = {m, MOQ_OK, 0};
    pthread_t tid;
    MVFST_CHECK(pthread_create(&tid, NULL, waiter_thread_fn, &wctx) == 0);

    /* Give the waiter a chance to enter wait(). */
    struct timespec ts = {0, 50000000}; /* 50ms */
    nanosleep(&ts, NULL);

    moq_mvfst_managed_stop(m);
    pthread_join(tid, NULL);

    MVFST_CHECK(wctx.done == 1);
    MVFST_CHECK(wctx.result == MOQ_ERR_CLOSED);
    moq_mvfst_managed_destroy(m);
}

static void test_natural_exit_closes(void)
{
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    for (int i = 0; i < 100; i++) {
        moq_result_t rc = moq_mvfst_managed_wait(m, 100000);
        if (rc == MOQ_ERR_CLOSED) break;
    }
    MVFST_CHECK(moq_mvfst_managed_wait(m, 0) == MOQ_ERR_CLOSED);
    MVFST_CHECK(moq_mvfst_managed_wake(m) == MOQ_ERR_CLOSED);
    MVFST_CHECK(moq_mvfst_managed_session(m) == NULL);
    moq_mvfst_managed_destroy(m);
}

typedef struct {
    moq_result_t stop_result;
} pump_stop_ctx_t;

static int pump_try_stop(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    pump_stop_ctx_t *c = (pump_stop_ctx_t *)ctx;
    c->stop_result = moq_mvfst_managed_stop(m);
    return 1;
}

static void test_stop_from_pump_rejected(void)
{
    pump_stop_ctx_t ctx = { MOQ_OK };
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_try_stop, &ctx);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    for (int i = 0; i < 100; i++) {
        moq_result_t rc = moq_mvfst_managed_wait(m, 100000);
        if (rc == MOQ_ERR_CLOSED) break;
    }
    MVFST_CHECK(ctx.stop_result == MOQ_ERR_INVAL);
    moq_mvfst_managed_destroy(m);
}

typedef struct {
    int callback_ran;
} pump_destroy_ctx_t;

static int pump_try_destroy(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    pump_destroy_ctx_t *c = (pump_destroy_ctx_t *)ctx;
    moq_mvfst_managed_destroy(m);
    c->callback_ran = 1;
    return 1;
}

static void test_destroy_from_pump_safe(void)
{
    pump_destroy_ctx_t ctx = { 0 };
    moq_mvfst_managed_cfg_t cfg = make_cfg(pump_try_destroy, &ctx);
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    for (int i = 0; i < 100; i++) {
        moq_result_t rc = moq_mvfst_managed_wait(m, 100000);
        if (rc == MOQ_ERR_CLOSED) break;
    }
    MVFST_CHECK(ctx.callback_ran == 1);
    moq_mvfst_managed_destroy(m);
}

static void test_invalid_config(void)
{
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(NULL, &m) == MOQ_ERR_INVAL);

    moq_mvfst_managed_cfg_t cfg = make_cfg(NULL, NULL);
    cfg.on_pump = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    cfg = make_cfg(pump_exit, NULL);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* Invalid port with host set. */
    cfg = make_cfg(pump_exit, NULL);
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* Lifecycle-only (host=NULL) accepts any port. */
    cfg = make_cfg(pump_exit, NULL);
    cfg.host = NULL;
    cfg.port = 0;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
    moq_mvfst_managed_destroy(m);
    m = NULL;

    /* Too-small struct_size. */
    cfg = make_cfg(pump_exit, NULL);
    cfg.struct_size = 4;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* Invalid perspective. */
    cfg = make_cfg(pump_exit, NULL);
    cfg.perspective = 99;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* Old struct_size excluding on_activity/user_ctx with dirty
     * trailing memory. Must succeed without reading past struct_size. */
    {
        /* Fill a buffer with 0xFF (dirty), then overlay a valid cfg
         * up to just before on_activity. */
        char dirty[sizeof(moq_mvfst_managed_cfg_t)];
        memset(dirty, 0xFF, sizeof(dirty));
        moq_mvfst_managed_cfg_t *old =
            (moq_mvfst_managed_cfg_t *)dirty;
        moq_mvfst_managed_cfg_init(old);
        old->struct_size =
            (uint32_t)((char *)&old->on_activity - (char *)old);
        old->perspective = MOQ_PERSPECTIVE_CLIENT;
        old->host = NULL;
        old->port = 4433;
        old->on_pump = pump_exit;
        old->cert_path = NULL;
        old->key_path = NULL;
        old->insecure_skip_verify = false;
        old->alloc = NULL;
        MVFST_CHECK(moq_mvfst_managed_create(old, &m) == MOQ_OK);
        moq_mvfst_managed_destroy(m);
        m = NULL;
    }

    /* Allocator with alloc=NULL rejected. */
    {
        moq_alloc_t bad_alloc = {NULL, NULL, NULL, NULL};
        bad_alloc.free = moq_alloc_default()->free;
        bad_alloc.realloc = moq_alloc_default()->realloc;
        cfg = make_cfg(pump_exit, NULL);
        cfg.alloc = &bad_alloc;
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    }

    /* Allocator with free=NULL rejected. */
    {
        moq_alloc_t bad_alloc = {NULL, NULL, NULL, NULL};
        bad_alloc.alloc = moq_alloc_default()->alloc;
        bad_alloc.realloc = moq_alloc_default()->realloc;
        cfg = make_cfg(pump_exit, NULL);
        cfg.alloc = &bad_alloc;
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    }

    /* Allocator with realloc=NULL rejected. */
    {
        moq_alloc_t bad_alloc = {NULL, NULL, NULL, NULL};
        bad_alloc.alloc = moq_alloc_default()->alloc;
        bad_alloc.free = moq_alloc_default()->free;
        cfg = make_cfg(pump_exit, NULL);
        cfg.alloc = &bad_alloc;
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    }
}

static void test_server_config_validation(void)
{
    moq_mvfst_managed_t *m = NULL;
    moq_mvfst_managed_cfg_t cfg;

    /* Server without cert_path. */
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.port = 0;
    cfg.on_pump = pump_exit;
    cfg.key_path = "/tmp/nonexistent_key.pem";
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* Server without key_path. */
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.port = 0;
    cfg.on_pump = pump_exit;
    cfg.cert_path = "/tmp/nonexistent_cert.pem";
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* Server with insecure_skip_verify. */
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.port = 0;
    cfg.on_pump = pump_exit;
    cfg.cert_path = "/tmp/nonexistent_cert.pem";
    cfg.key_path = "/tmp/nonexistent_key.pem";
    cfg.insecure_skip_verify = true;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);

    /* Server with unreadable cert_path (valid key_path check skipped
     * because cert_path fails first). */
    moq_mvfst_managed_cfg_init(&cfg);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.port = 0;
    cfg.on_pump = pump_exit;
    cfg.cert_path = "/nonexistent/path/cert.pem";
    cfg.key_path = "/nonexistent/path/key.pem";
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
}

/* ALPN offer / exact MoQ version + SNI config (lifecycle-only: no transport,
 * so negotiated_version stays 0 — the session never reaches ESTABLISHED). */
static void test_alpn_version_config(void)
{
    moq_mvfst_managed_t *m = NULL;

    /* Default (no alpn_list): accepted, negotiated 0 until established. */
    {
        moq_mvfst_managed_cfg_t cfg = make_cfg(pump_forever, NULL);
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
        MVFST_CHECK(moq_mvfst_managed_negotiated_version(m) == 0);
        moq_mvfst_managed_destroy(m);
        m = NULL;
    }

    /* Single "moqt-16" ALPN accepted. */
    {
        static const char *const alpn[] = { "moqt-16" };
        moq_mvfst_managed_cfg_t cfg = make_cfg(pump_forever, NULL);
        cfg.alpn_list = alpn;
        cfg.alpn_count = 1;
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
        MVFST_CHECK(moq_mvfst_managed_negotiated_version(m) == 0);
        moq_mvfst_managed_destroy(m);
        m = NULL;
    }

    /* Multi-version offer (AUTO) rejected: this is an exact-version adapter. */
    {
        static const char *const alpn[] = { "moqt-18", "moqt-16" };
        moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
        cfg.alpn_list = alpn;
        cfg.alpn_count = 2;
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_UNSUPPORTED);
    }

    /* Unknown / non-MoQ ALPN rejected. */
    {
        static const char *const alpn[] = { "h3" };
        moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
        cfg.alpn_list = alpn;
        cfg.alpn_count = 1;
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_UNSUPPORTED);
    }

    /* alpn_count > 0 but NULL list rejected. */
    {
        moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
        cfg.alpn_list = NULL;
        cfg.alpn_count = 1;
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_ERR_INVAL);
    }

    /* SNI accepted (stored; lifecycle-only so no transport uses it). */
    {
        moq_mvfst_managed_cfg_t cfg = make_cfg(pump_forever, NULL);
        cfg.sni = "relay.example";
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
        moq_mvfst_managed_destroy(m);
        m = NULL;
    }

    /* sized init leaves max_connections at 0 so create() applies its default. */
    {
        moq_mvfst_managed_cfg_t cfg;
        moq_mvfst_managed_cfg_init_sized(&cfg, sizeof(cfg));
        MVFST_CHECK(cfg.max_connections == 0);
    }

    /* Old caller whose struct ends before the appended max_connections field:
     * dirty trailing memory must NOT be read as a custom connection cap (the
     * struct_size gate keeps it at the default). Same 0xFF-tail technique as the
     * sni/alpn old-prefix test below. */
    {
        moq_mvfst_managed_cfg_t prefix;
        moq_mvfst_managed_cfg_init_sized(&prefix, sizeof(prefix));
        uint32_t old_size =
            (uint32_t)((char *)&prefix.max_connections - (char *)&prefix);
        prefix.struct_size = old_size;
        prefix.perspective = MOQ_PERSPECTIVE_CLIENT;
        prefix.host = NULL;
        prefix.port = 4433;
        prefix.on_pump = pump_exit;

        char dirty[sizeof(moq_mvfst_managed_cfg_t)];
        memset(dirty, 0xFF, sizeof(dirty));
        memcpy(dirty, &prefix, old_size);   /* tail past old_size stays 0xFF */
        moq_mvfst_managed_cfg_t *old = (moq_mvfst_managed_cfg_t *)dirty;

        MVFST_CHECK(moq_mvfst_managed_create(old, &m) == MOQ_OK);
        moq_mvfst_managed_destroy(m);
        m = NULL;
    }

    /* Old caller whose struct ends before the appended sni/alpn fields:
     * dirty trailing memory must NOT be read as sni/alpn_list/alpn_count.
     * Build a valid old-prefix in a clean struct, then copy ONLY that prefix
     * over a 0xFF-filled buffer so the bytes past struct_size stay dirty
     * (cfg_init would otherwise zero the whole struct). */
    {
        moq_mvfst_managed_cfg_t prefix;
        moq_mvfst_managed_cfg_init_sized(&prefix, sizeof(prefix));
        uint32_t old_size =
            (uint32_t)((char *)&prefix.sni - (char *)&prefix);
        prefix.struct_size = old_size;
        prefix.perspective = MOQ_PERSPECTIVE_CLIENT;
        prefix.host = NULL;
        prefix.port = 4433;
        prefix.on_pump = pump_exit;

        char dirty[sizeof(moq_mvfst_managed_cfg_t)];
        memset(dirty, 0xFF, sizeof(dirty));
        memcpy(dirty, &prefix, old_size);   /* tail past old_size stays 0xFF */
        moq_mvfst_managed_cfg_t *old = (moq_mvfst_managed_cfg_t *)dirty;

        MVFST_CHECK(moq_mvfst_managed_create(old, &m) == MOQ_OK);
        moq_mvfst_managed_destroy(m);
        m = NULL;
    }

    /* Old caller whose struct ends before the appended transport-tuning fields
     * (max_num_ptos / initial_rtt_us, moved out of the middle of the struct):
     * dirty trailing memory must NOT be read as tuning values; the struct_size
     * gate keeps them at the library defaults. */
    {
        moq_mvfst_managed_cfg_t prefix;
        moq_mvfst_managed_cfg_init_sized(&prefix, sizeof(prefix));
        uint32_t old_size =
            (uint32_t)((char *)&prefix.max_num_ptos - (char *)&prefix);
        prefix.struct_size = old_size;
        prefix.perspective = MOQ_PERSPECTIVE_CLIENT;
        prefix.host = NULL;
        prefix.port = 4433;
        prefix.on_pump = pump_exit;

        char dirty[sizeof(moq_mvfst_managed_cfg_t)];
        memset(dirty, 0xFF, sizeof(dirty));
        memcpy(dirty, &prefix, old_size);   /* tail past old_size stays 0xFF */
        moq_mvfst_managed_cfg_t *old = (moq_mvfst_managed_cfg_t *)dirty;

        MVFST_CHECK(moq_mvfst_managed_create(old, &m) == MOQ_OK);
        moq_mvfst_managed_destroy(m);
        m = NULL;
    }

    /* Old-storage overflow canary: a caller compiled against the ORIGINAL
     * struct allocated only the frozen v0 prefix (it ended at the callback
     * pointers, before any appended field), then placed its own bytes
     * immediately after. Pointer-only init must clear/stamp ONLY that prefix --
     * the old memset(sizeof current) would write the appended region and clobber
     * the byte right after the old struct. The union's first member forces
     * alignment for the cfg* cast; the cfg is never accessed through the full
     * struct type (the storage is old-sized). */
    {
        union {
            moq_mvfst_managed_cfg_t aligner;   /* alignment only */
            struct {
                unsigned char prefix[
                    offsetof(moq_mvfst_managed_cfg_t, goaway_timeout_us)];
                uint64_t canary;               /* the old caller's next bytes */
            } box;
        } u;
        memset(&u, 0xAB, sizeof(u));
        moq_mvfst_managed_cfg_init((moq_mvfst_managed_cfg_t *)&u.box);

        uint32_t ss;
        memcpy(&ss, u.box.prefix +
               offsetof(moq_mvfst_managed_cfg_t, struct_size), sizeof(ss));
        MVFST_CHECK(ss ==
            (uint32_t)offsetof(moq_mvfst_managed_cfg_t, goaway_timeout_us));
        MVFST_CHECK(u.box.canary == 0xABABABABABABABABULL);  /* not overflowed */
    }
}

static void test_stack_local_allocator(void)
{
    moq_mvfst_managed_t *m = NULL;
    {
        /* Copy the default allocator into a stack-local struct.
         * After create() returns, this struct goes out of scope.
         * Destroy must use its own copy, not the dangling pointer. */
        moq_alloc_t stack_alloc = *moq_alloc_default();
        moq_mvfst_managed_cfg_t cfg = make_cfg(pump_exit, NULL);
        cfg.alloc = &stack_alloc;
        MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);
        MVFST_CHECK(m != NULL);
    }
    /* stack_alloc is now out of scope. */
    MVFST_CHECK(moq_mvfst_managed_stop(m) == MOQ_OK);
    moq_mvfst_managed_destroy(m);
}

typedef struct {
    atomic_int stop_result;
    atomic_int callback_ran;
} activity_stop_ctx_t;

static int activity_pump_forever(moq_mvfst_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 0;
}

static void activity_try_stop(moq_mvfst_managed_t *m, void *ctx)
{
    activity_stop_ctx_t *c = (activity_stop_ctx_t *)ctx;
    atomic_store(&c->stop_result, (int)moq_mvfst_managed_stop(m));
    moq_mvfst_managed_destroy(m);
    atomic_store(&c->callback_ran, 1);
}

static void test_stop_from_activity_rejected(void)
{
    activity_stop_ctx_t ctx = { MOQ_OK, 0 };
    moq_mvfst_managed_cfg_t cfg = make_cfg(activity_pump_forever, &ctx);
    cfg.on_activity = activity_try_stop;
    moq_mvfst_managed_t *m = NULL;
    MVFST_CHECK(moq_mvfst_managed_create(&cfg, &m) == MOQ_OK);

    /* Wake to trigger on_activity. */
    moq_mvfst_managed_wake(m);
    struct timespec ts = {0, 100000000}; /* 100ms */
    nanosleep(&ts, NULL);
    moq_mvfst_managed_wake(m);
    nanosleep(&ts, NULL);

    /* Force a few more wakes to ensure on_activity fires. */
    for (int i = 0; i < 10; i++) {
        moq_mvfst_managed_wake(m);
        moq_mvfst_managed_wait(m, 50000);
        if (atomic_load(&ctx.callback_ran)) break;
    }
    MVFST_CHECK(atomic_load(&ctx.callback_ran) == 1);
    MVFST_CHECK(atomic_load(&ctx.stop_result) == MOQ_ERR_INVAL);
    moq_mvfst_managed_destroy(m);
}

int main(void)
{
    test_create_stop_destroy();
    test_destroy_without_stop();
    test_stop_idempotent();
    test_repeated_lifecycle();
    test_session_in_pump();
    test_session_null_from_app_thread();
    test_pump_runs_multiple();
    test_wake_after_stop();
    test_blocking_wait_wakes_on_stop();
    test_natural_exit_closes();
    test_stop_from_pump_rejected();
    test_destroy_from_pump_safe();
    test_stack_local_allocator();
    test_stop_from_activity_rejected();
    test_invalid_config();
    test_server_config_validation();
    test_alpn_version_config();

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
