#include "support/fake_pq_pair.h"
#include "../../tests/conformance/conformance_scenarios.h"
#include <stdio.h>
#include <string.h>
#include <stddef.h>

#define PQ_CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static int test_config_validation(void)
{
    int failures = 0;
    moq_pq_conn_t *out = NULL;

    /* NULL cfg */
    PQ_CHECK(moq_pq_conn_create(NULL, &out) != 0);

    /* Too-small struct_size */
    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.struct_size = 4;
    cfg.session = (moq_session_t *)(uintptr_t)0x1;
    cfg.cnx = (picoquic_cnx_t *)(uintptr_t)0x1;
    cfg.alloc = moq_alloc_default();
    PQ_CHECK(moq_pq_conn_create(&cfg, &out) != 0);

    /* Missing alloc->realloc */
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = (moq_session_t *)(uintptr_t)0x1;
    cfg.cnx = (picoquic_cnx_t *)(uintptr_t)0x1;
    moq_alloc_t bad_alloc = *moq_alloc_default();
    bad_alloc.realloc = NULL;
    cfg.alloc = &bad_alloc;
    PQ_CHECK(moq_pq_conn_create(&cfg, &out) != 0);

    /* Missing alloc->alloc */
    bad_alloc = *moq_alloc_default();
    bad_alloc.alloc = NULL;
    cfg.alloc = &bad_alloc;
    PQ_CHECK(moq_pq_conn_create(&cfg, &out) != 0);

    /* Missing alloc->free */
    bad_alloc = *moq_alloc_default();
    bad_alloc.free = NULL;
    cfg.alloc = &bad_alloc;
    PQ_CHECK(moq_pq_conn_create(&cfg, &out) != 0);

    return failures;
}

/* ABI safety of the config initializers (no shared-library mismatch needed:
 * model the old/new layouts inline). */
static int test_cfg_init_abi(void)
{
    int failures = 0;

    /* Old-ABI canary: a binary compiled against the v0 header allocated a
     * moq_pq_conn_cfg_t that ended at `alloc`. Place a canary immediately after
     * that v0 prefix and call the legacy pointer-only initializer. It must
     * touch ONLY the v0 prefix; the canary must survive. (A memset(sizeof(*cfg))
     * would zero a pointer-width into the canary.) */
    {
        struct old_cfg_v0 {
            uint32_t           struct_size;
            moq_session_t     *session;
            picoquic_cnx_t    *cnx;
            const moq_alloc_t *alloc;
        };
        struct {
            struct old_cfg_v0 cfg;
            uint64_t          canary;
        } holder;
        memset(&holder, 0, sizeof(holder));
        holder.canary = 0xC0FFEE5AA5C0FFEEull;

        moq_pq_conn_cfg_init((moq_pq_conn_cfg_t *)&holder.cfg);

        PQ_CHECK(holder.canary == 0xC0FFEE5AA5C0FFEEull);   /* not overwritten */
        PQ_CHECK(holder.cfg.struct_size ==
                 (uint32_t)sizeof(struct old_cfg_v0));
        /* The v0 prefix is exactly offsetof(user_ctx). */
        PQ_CHECK(sizeof(struct old_cfg_v0) ==
                 offsetof(moq_pq_conn_cfg_t, user_ctx));
    }

    /* Sized initializer fully clears the current struct and stamps its size,
     * so a current caller gets zeroed appended fields ready to set. */
    {
        moq_pq_conn_cfg_t cfg;
        memset(&cfg, 0xAB, sizeof(cfg));   /* dirty every byte */
        moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));

        PQ_CHECK(cfg.struct_size == (uint32_t)sizeof(cfg));
        PQ_CHECK(cfg.session == NULL);
        PQ_CHECK(cfg.cnx == NULL);
        PQ_CHECK(cfg.alloc == NULL);
        PQ_CHECK(cfg.user_ctx == NULL);
        PQ_CHECK(cfg.after_callback == NULL);
    }

    /* Sized initializer with a v0 caller size clears/stamps only that prefix
     * and never writes past it (canary intact). */
    {
        struct {
            moq_pq_conn_cfg_t cfg;
            uint64_t          canary;
        } holder;
        memset(&holder, 0xCD, sizeof(holder));
        holder.canary = 0x1234567890ABCDEFull;

        moq_pq_conn_cfg_init_sized(&holder.cfg,
                                   offsetof(moq_pq_conn_cfg_t, user_ctx));

        PQ_CHECK(holder.cfg.struct_size ==
                 (uint32_t)offsetof(moq_pq_conn_cfg_t, user_ctx));
        PQ_CHECK(holder.canary == 0x1234567890ABCDEFull);
        /* Appended fields beyond the v0 prefix were NOT cleared (still 0xCD). */
        PQ_CHECK(holder.cfg.user_ctx != NULL);
    }

    return failures;
}

extern uint64_t g_pending_app_error;

static int test_closed_rejects_late_callbacks(void)
{
    int failures = 0;
    fake_pq_pair_t pair;
    PQ_CHECK(fake_pq_pair_create(&pair) == 0);

    moq_session_start(pair.client_session, pair.now);
    for (int i = 0; i < 100; i++)
        if (!fake_pq_pair_pump_once(&pair)) break;

    /* Server sends GOAWAY → tick fires CLOSE_SESSION. */
    moq_session_goaway(pair.server_session, NULL, 0, pair.now);
    fake_pq_pair_pump_once(&pair);
    pair.now += 2000;
    for (int i = 0; i < 10; i++)
        fake_pq_pair_pump_once(&pair);

    PQ_CHECK(moq_pq_conn_is_closed(pair.server_conn));

    /* Late stream data after close — should be ignored. */
    uint8_t data[] = {0xFF};
    moq_pq_callback((picoquic_cnx_t *)&pair.server_side, 99,
        data, 1, picoquic_callback_stream_data,
        pair.server_conn, NULL);
    PQ_CHECK(!moq_pq_conn_is_fatal(pair.server_conn));

    /* Late application close after close — should not turn fatal. */
    g_pending_app_error = 0x42;
    moq_pq_callback((picoquic_cnx_t *)&pair.server_side, 0,
        NULL, 0, picoquic_callback_application_close,
        pair.server_conn, NULL);
    g_pending_app_error = 0;
    PQ_CHECK(!moq_pq_conn_is_fatal(pair.server_conn));
    PQ_CHECK(moq_pq_conn_is_closed(pair.server_conn));

    fake_pq_pair_destroy(&pair);
    return failures;
}

extern picoquic_stream_data_cb_fn g_last_set_callback_fn;
extern void *g_last_set_callback_ctx;

static int test_destroy_clears_callback(void)
{
    int failures = 0;

    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    moq_session_t *s = NULL;
    PQ_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);

    moq_pq_conn_cfg_t cc;
    moq_pq_conn_cfg_init_sized(&cc, sizeof(cc));
    cc.session = s;
    cc.cnx = (picoquic_cnx_t *)(uintptr_t)0xBEEF;
    cc.alloc = moq_alloc_default();

    moq_pq_conn_t *conn = NULL;
    PQ_CHECK(moq_pq_conn_create(&cc, &conn) == 0);
    PQ_CHECK(conn != NULL);

    moq_pq_conn_destroy(conn);

    PQ_CHECK(g_last_set_callback_fn == NULL);
    PQ_CHECK(g_last_set_callback_ctx == NULL);

    moq_session_destroy(s);
    return failures;
}

int main(void)
{
    int total = 0;

    total += test_config_validation();
    total += test_cfg_init_abi();
    total += test_closed_rejects_late_callbacks();
    total += test_destroy_clears_callback();

    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_setup_handshake(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_subscribe_and_object(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_datagram_object(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_single_control_stream(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_close_not_fatal(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_session_timer_goaway(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_bidi_halfclose_accept(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_bidi_halfclose_reject_late_fin(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_crossed_cancel(&p);
        moq_pair_destroy(&p);
    }

    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_reset_propagation(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_would_block_ordering(&p);
        moq_pair_destroy(&p);
    }
    {
        moq_adapter_pair_t p = fake_pq_conformance_create();
        if (!p.ops) { fprintf(stderr, "FAIL: create\n"); return 1; }
        total += conformance_dropped_datagram_unblocks(&p);
        moq_pair_destroy(&p);
    }

    printf("%s: %d failures\n", total ? "FAIL" : "PASS", total);
    return total ? 1 : 0;
}
