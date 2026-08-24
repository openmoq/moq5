/* Callback-ownership lifecycle oracle for the managed pico_wt SERVER.
 *
 * The adapter's teardown contract is that clear_all_stream_callbacks() clears
 * every callback whose path_callback_ctx == conn, so detach_from_picoquic()
 * can promise that no h3zero callback reaches a freed adapter. That promise
 * holds only while the WT CONTROL stream's context IS the adapter. A facade
 * pointer parked there escapes adapter cleanup entirely.
 *
 * This test pins both halves against the real server path:
 *   1. before teardown, the control binding's context is the ADAPTER;
 *   2. after the adapter is detached, that binding is CLEARED, so nothing can
 *      reach the freed adapter.
 *
 * Uses the private MOQ_PICO_WT_TESTING seam; nothing is exported.
 */
#include <moq/pico_wt_managed.h>
#include <moq/pico_wt.h>
#include <moq/moq.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

static int failures;
#define CHECK(e) do { if (!(e)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #e); failures++; } } while (0)

#include "pico_wt_test_seam.h"

#define WT_ALPN_ERROR_CODE 0x0817b3ddULL

static const char *cb_name(moq_pwt_cb_kind_t k)
{
    switch (k) {
    case MOQ_PWT_CB_NONE:    return "none";
    case MOQ_PWT_CB_ADAPTER: return "adapter_cb";
    case MOQ_PWT_CB_FACADE:  return "facade_cb";
    default:                 return "other";
    }
}
static const char *ctx_name(moq_pwt_ctx_kind_t k)
{
    switch (k) {
    case MOQ_PWT_CTX_NONE:    return "none";
    case MOQ_PWT_CTX_ADAPTER: return "adapter";
    case MOQ_PWT_CTX_FACADE:  return "facade";
    default:                  return "other";
    }
}

static int srv_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{ (void)m; (void)now; (void)ctx; return 0; }

int main(int argc, char **argv)
{
    if (argc < 3) { fprintf(stderr, "usage: %s <cert> <key>\n", argv[0]); return 2; }
    const char *cert = argv[1], *key = argv[2];
    unsigned base = 21000u + (unsigned)((uintptr_t)getpid() * 2654435761u) % 25000u;

    for (int attempt = 0; attempt < 4; attempt++) {
        int port = (int)(base + (unsigned)attempt * 137u);

        moq_pico_wt_managed_cfg_t scfg;
        moq_pico_wt_managed_cfg_init(&scfg);
        scfg.alloc = moq_alloc_default();
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.cert_path = cert; scfg.key_path = key;
        scfg.port = port; scfg.path = "/moq";
        scfg.on_pump = srv_pump;
        scfg.wt_protocols = NULL;          /* echo no token: the ALPN geometry */
        moq_pico_wt_managed_t *srv = NULL;
        if (moq_pico_wt_managed_create(&scfg, &srv) != MOQ_OK || srv == NULL) continue;
        for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);

        moq_pico_wt_managed_cfg_t ccfg;
        moq_pico_wt_managed_cfg_init(&ccfg);
        ccfg.alloc = moq_alloc_default();
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.host = "127.0.0.1";
        ccfg.port = moq_pico_wt_managed_local_port(srv);
        ccfg.path = "/moq";
        ccfg.insecure_skip_verify = true;
        ccfg.on_pump = srv_pump;
        ccfg.wt_protocols = "moqt-18";
        moq_pico_wt_managed_t *cli = NULL;
        if (moq_pico_wt_managed_create(&ccfg, &cli) != MOQ_OK || cli == NULL) {
            moq_pico_wt_managed_stop(srv); moq_pico_wt_managed_destroy(srv);
            continue;
        }

        /* -- observe the ALPN close through THREAD-SAFE accessors only ----
         * No seam, no h3zero traversal, and no adapter teardown may run while
         * either network thread is live. This loop only waits and reads the
         * facade's own published state. */
        int srv_session_seen = 0, closed_ok = 0;
        for (int i = 0; i < 80; i++) {
            if (moq_pico_wt_managed_session(srv) != NULL) srv_session_seen = 1;
            if (moq_pico_wt_managed_is_closed(srv) &&
                moq_pico_wt_managed_close_code(srv) == WT_ALPN_ERROR_CODE) {
                closed_ok = 1;
                break;
            }
            moq_pico_wt_managed_wait(cli, 100000);
            moq_pico_wt_managed_wait(srv, 100000);
        }
        /* ARM GATE: the arm is established once the server published its
         * session. The adapter fact rides the SAME critical section --
         * pico_wt_managed.c:537-542 assigns m->session and m->conn under one
         * m->mutex hold -- so the public accessor covers both and no seam is
         * needed here. Only a pre-accept failure (bind contention, cert,
         * connect) may retry: a missing close on an ESTABLISHED arm is a real
         * failure and must not be laundered into a retry, or a product defect
         * that suppresses the capsule would silently exhaust the attempts. */
        if (!srv_session_seen) {
            moq_pico_wt_managed_stop(cli); moq_pico_wt_managed_destroy(cli);
            moq_pico_wt_managed_stop(srv); moq_pico_wt_managed_destroy(srv);
            continue;
        }

        /* -- QUIESCE both autonomous network threads, client first ---------
         * stop() joins the picoquic network thread. Only after BOTH joins may
         * the seam walk h3zero state or invoke the adapter destructor. The
         * join is also the happens-before edge that makes the callback-thread
         * event inventory safe to read. The QUIC contexts stay alive: they are
         * deleted by destroy(), not by stop(). */
        const moq_result_t cli_stop = moq_pico_wt_managed_stop(cli);
        const moq_result_t srv_stop = moq_pico_wt_managed_stop(srv);
        fprintf(stderr, "[ownership] quiesce cli_stop=%d srv_stop=%d\n",
                (int)cli_stop, (int)srv_stop);
        /* the client latched a local fatal, so its stop reports CLOSED; the
         * server observed a clean peer close, so its stop reports OK */
        CHECK(cli_stop == MOQ_ERR_CLOSED);
        CHECK(srv_stop == MOQ_OK);

        /* -- the close obligation, asserted (facade state survives stop) --- */
        const int      srv_closed = moq_pico_wt_managed_is_closed(srv);
        const uint64_t srv_code   = moq_pico_wt_managed_close_code(srv);
        fprintf(stderr, "[ownership] srv_closed=%d srv_close_code=0x%llx\n",
                srv_closed, (unsigned long long)srv_code);
        CHECK(closed_ok);
        CHECK(srv_closed);
        CHECK(srv_code == WT_ALPN_ERROR_CODE);

        /* -- callback event inventory (recorded on the network thread) -----
         * Declared LITERALLY: this fixture produces exactly three callbacks,
         * in this order, with these contexts. Presence-only checking would
         * accept a reordering or a foreign extra event. */
        static const struct {
            picohttp_call_back_event_t ev;
            moq_pwt_ctx_kind_t         ctx;
        } expect[] = {
            { picohttp_callback_connect,  MOQ_PWT_CTX_FACADE  },
            { picohttp_callback_post,     MOQ_PWT_CTX_FACADE  },
            { picohttp_callback_post_fin, MOQ_PWT_CTX_ADAPTER },
        };
        const size_t nexp = sizeof(expect) / sizeof(expect[0]);

        const size_t nev = moq_pico_wt_managed_test_event_count(srv);
        fprintf(stderr, "[ownership] events n=%zu:", nev);
        for (size_t i = 0; i < nev; i++) {
            int ev = -1; moq_pwt_ctx_kind_t ck = MOQ_PWT_CTX_NONE;
            if (moq_pico_wt_managed_test_event_at(srv, i, &ev, &ck) == 0)
                fprintf(stderr, " %d/%s", ev, ctx_name(ck));
            else
                fprintf(stderr, " <read-failed>");
        }
        fprintf(stderr, "\n");

        CHECK(moq_pico_wt_managed_test_event_overflow(srv) == 0);
        CHECK(nev == nexp);                    /* exact count, no extras */
        if (nev == nexp) {
            for (size_t i = 0; i < nexp; i++) {
                int ev = -1; moq_pwt_ctx_kind_t ck = MOQ_PWT_CTX_NONE;
                const int rc = moq_pico_wt_managed_test_event_at(srv, i, &ev, &ck);
                CHECK(rc == 0);                /* never silently skipped */
                if (rc != 0) continue;
                CHECK(ev == (int)expect[i].ev);   /* exact event, in position */
                CHECK(ck == expect[i].ctx);       /* and its exact context */
            }
        }

        /* -- the POST-BODY control binding, the state under review --------- */
        moq_pwt_cb_kind_t cb = MOQ_PWT_CB_NONE;
        moq_pwt_ctx_kind_t ctx = MOQ_PWT_CTX_NONE;
        const int present = moq_pico_wt_managed_test_ctrl_binding(srv, &cb, &ctx);
        fprintf(stderr, "[ownership] post-body binding present=%d cb=%s ctx=%s\n",
                present, cb_name(cb), ctx_name(ctx));
        CHECK(present == 1);
        CHECK(cb == MOQ_PWT_CB_FACADE);      /* h3zero restored the path-table fn */
        CHECK(ctx == MOQ_PWT_CTX_ADAPTER);   /* but kept the ADAPTER context */
        CHECK(ctx != MOQ_PWT_CTX_FACADE);

        /* -- detach the adapter, then re-read the SAME live stream --------- */
        moq_pico_wt_managed_test_detach_conn(srv);
        moq_pwt_cb_kind_t cb2 = MOQ_PWT_CB_NONE;
        moq_pwt_ctx_kind_t ctx2 = MOQ_PWT_CTX_NONE;
        const int present2 = moq_pico_wt_managed_test_ctrl_binding(srv, &cb2, &ctx2);
        fprintf(stderr, "[ownership] post-detach present=%d cb=%s ctx=%s\n",
                present2, cb_name(cb2), ctx_name(ctx2));
        CHECK(present2 == 1);                /* stream still live, so this is real */
        CHECK(cb2 == MOQ_PWT_CB_NONE);
        CHECK(ctx2 == MOQ_PWT_CTX_NONE);     /* cleared */
        CHECK(ctx2 != MOQ_PWT_CTX_ADAPTER);  /* not the freed adapter */

        moq_pico_wt_managed_destroy(cli);
        moq_pico_wt_managed_destroy(srv);

        if (failures == 0) { printf("test_pico_wt_ownership: PASS\n"); return 0; }
        fprintf(stderr, "test_pico_wt_ownership: %d failure(s)\n", failures);
        return 1;
    }
    fprintf(stderr, "test_pico_wt_ownership: no arm could be established\n");
    return 1;
}
