/*
 * Deterministic policy oracle for the raw threaded picoquic CLIENT's TLS
 * certificate policy (finding 2). It proves the fail-closed default WITHOUT a
 * live handshake, by inspecting the actual picotls master context in a
 * configure_quic hook and using the narrowly scoped injected installer seam --
 * exactly the seam the ruling calls for, not a mocked constructor.
 *
 * Uses only the public void picoquic_set_verify_certificate_callback() setter,
 * which is what the pinned CI picoquic exposes (the _ex reporting variant is not
 * present at that pin). At this pin the setter directly replaces the callback
 * and disposes the previously-owned one; it has no fallible ownership-record
 * allocation, and the adapter's real verifier helper does its own fallible
 * store/verifier creation BEFORE invoking the setter -- so the injected
 * installer-failure case still proves a helper failure aborts creation before
 * the configure hook.
 *
 * Observability (white-box, test-internals build only):
 *   - the adapter's default cert-verifier installer is a swappable seam
 *     (moq_pq_threaded_test_cert_installer) with call counters, compiled only
 *     under MOQ_PQ_THREADED_TESTING and absent from production;
 *   - the installed verifier is read straight off the picotls master context
 *     (((ptls_context_t*)quic->tls_master_ctx)->verify_certificate), whose
 *     pointer identity distinguishes "no verifier" (NULL), the adapter default
 *     (non-NULL, foreign), and a caller-installed custom verifier;
 *   - disposal is observed at EACH ownership boundary IN PHASE: a test-owned
 *     counted default is installed via the seam, and per-step snapshots are
 *     captured inside the hook so the disposal is asserted at the exact
 *     replacement it occurs at, not only by the final totals.
 *
 * Links the test-internals build of the adapter (never shipped).
 */

#include <moq/picoquic_threaded.h>
#include <moq/picoquic_verify.h>

#include <picoquic.h>
#include <picoquic_internal.h>   /* picoquic_quic_t: tls_master_ctx */
#include <picotls.h>             /* ptls_context_t: verify_certificate */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* The MOQ_PQ_THREADED_TESTING seam (deliberately NOT in the public header). */
extern int (*moq_pq_threaded_test_cert_installer)(picoquic_quic_t *,
                                                  const char *);
extern unsigned moq_pq_threaded_test_cert_install_calls;
extern unsigned moq_pq_threaded_test_null_verifier_calls;

static int failures = 0;

#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n",                        \
                    __FILE__, __LINE__, #expr);                         \
            failures++;                                                 \
        }                                                               \
    } while (0)

#ifdef MOQ_TEST_CERT_PATH

/* The verifier pointer currently installed on the QUIC context's picotls
 * master context, or NULL. This IS the picotls master context the ruling
 * names -- not a picoquic mirror field. */
static void *installed_verifier(picoquic_quic_t *quic)
{
    ptls_context_t *m = (ptls_context_t *)quic->tls_master_ctx;
    return m ? (void *)m->verify_certificate : NULL;
}

/* Minimally valid verifier objects: the cb is only invoked on a received server
 * certificate (which never arrives here), but the client thread reads algos
 * while building its ClientHello, so each must be a non-NULL, UINT16_MAX-
 * terminated list. picoquic stores the pointer as-is; the dispose fns only
 * COUNT (the objects are static, never freed). */
static const uint16_t k_test_algos[] = { 0x0403 /* ecdsa_secp256r1_sha256 */,
                                         UINT16_MAX };

/* Test-owned DEFAULTS installed through the seam, one per case that needs to
 * observe the adapter default's own disposal. Distinct objects keep the two
 * cases' counters independent. */
static ptls_verify_certificate_t default0 = { NULL, k_test_algos };  /* case 3 */
static ptls_verify_certificate_t default5 = { NULL, k_test_algos };  /* case 5 */
static int default0_disposed, default5_disposed;
static void dispose_default0(ptls_verify_certificate_t *v) { (void)v; default0_disposed++; }
static void dispose_default5(ptls_verify_certificate_t *v) { (void)v; default5_disposed++; }

/* Two distinct test-owned custom verifiers with counting dispose fns. */
static ptls_verify_certificate_t custom1 = { NULL, k_test_algos };
static ptls_verify_certificate_t custom2 = { NULL, k_test_algos };
static int custom1_disposed, custom2_disposed;
static void dispose1(ptls_verify_certificate_t *v) { (void)v; custom1_disposed++; }
static void dispose2(ptls_verify_certificate_t *v) { (void)v; custom2_disposed++; }

/* Seam installers that stand in for the adapter's default cert-verifier
 * install: each installs a COUNTED test-owned default via the public void
 * setter and reports success the way the adapter's real installer does when its
 * own store/verifier creation succeeded (0 ok). */
static int counting_installer0(picoquic_quic_t *quic, const char *ca)
{
    (void)ca;
    picoquic_set_verify_certificate_callback(quic, &default0, dispose_default0);
    return 0;
}
static int counting_installer5(picoquic_quic_t *quic, const char *ca)
{
    (void)ca;
    picoquic_set_verify_certificate_callback(quic, &default5, dispose_default5);
    return 0;
}

/* A default-installer stub that always fails, modelling the real helper's
 * store/verifier creation failing before the setter is ever called. Narrowly
 * scoped: it replaces only the default install, nothing else. */
static int failing_installer(picoquic_quic_t *quic, const char *ca)
{
    (void)quic; (void)ca;
    return -1;
}

typedef struct {
    void *verify_at_entry;   /* installed verifier when the hook was entered */
    int   hook_calls;
    int   install_customs;   /* case 3: replace default with custom1 -> custom2 */
    int   return_fail;       /* case 5: hook fails after the default installed */
    int   replacement_ok;    /* case 3: master ctx ended on custom2 */

    /* Per-phase snapshots (case 3), captured inside the hook so disposal is
     * asserted at the exact replacement boundary, not only by final totals. */
    int   d0_before,  c1_before,  c2_before;   void *active_before;
    int   d0_after1,  c1_after1,  c2_after1;   void *active_after1;
    int   d0_after2,  c1_after2,  c2_after2;   void *active_after2;

    int   d5_at_entry;       /* case 5: default5 disposal count at hook entry */
} hook_ctx_t;

/* configure_quic hook: runs on the create thread, after the adapter installed
 * (or skipped) its default verifier and BEFORE any network thread. */
static int policy_hook(picoquic_quic_t *quic, void *vctx)
{
    hook_ctx_t *h = (hook_ctx_t *)vctx;
    h->verify_at_entry = installed_verifier(quic);
    h->d5_at_entry = default5_disposed;
    h->hook_calls++;
    if (h->install_customs) {
        /* Phase 0: before any replacement. */
        h->d0_before = default0_disposed;
        h->c1_before = custom1_disposed;
        h->c2_before = custom2_disposed;
        h->active_before = installed_verifier(quic);

        /* Replace #1: custom1 replaces the default -> disposes the default. */
        picoquic_set_verify_certificate_callback(quic, &custom1, dispose1);
        h->d0_after1 = default0_disposed;
        h->c1_after1 = custom1_disposed;
        h->c2_after1 = custom2_disposed;
        h->active_after1 = installed_verifier(quic);

        /* Replace #2: custom2 replaces custom1 -> disposes custom1. */
        picoquic_set_verify_certificate_callback(quic, &custom2, dispose2);
        h->d0_after2 = default0_disposed;
        h->c1_after2 = custom1_disposed;
        h->c2_after2 = custom2_disposed;
        h->active_after2 = installed_verifier(quic);

        h->replacement_ok = (installed_verifier(quic) == (void *)&custom2);
    }
    return h->return_fail ? -1 : 0;
}

static int dummy_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                      uint64_t now_us, void *user)
{
    (void)t; (void)lane; (void)now_us; (void)user;
    return 0;
}

static void reset_seam(void)
{
    moq_pq_threaded_test_cert_installer = NULL;
    moq_pq_threaded_test_cert_install_calls = 0;
    moq_pq_threaded_test_null_verifier_calls = 0;
}

static void client_cfg(moq_pq_threaded_cfg_t *c, int port,
                       hook_ctx_t *h, int insecure)
{
    moq_pq_threaded_cfg_init_sized(c, sizeof(*c));
    c->alloc = moq_alloc_default();
    c->perspective = MOQ_PERSPECTIVE_CLIENT;
    c->host = "127.0.0.1";
    c->port = port;
    c->insecure_skip_verify = insecure ? true : false;
    c->on_lane_pump = dummy_pump;
    if (h) {
        c->configure_quic = policy_hook;
        c->configure_quic_ctx = h;
    }
}

int main(void)
{
    srand((unsigned)getpid());
    int base = 15600 + (rand() % 300);

    /* -- Case 1: default client -- the GENUINE system-trust verifier is
     *    installed once (real installer, seam left NULL), present before the
     *    configure hook, no null verifier. Kept as the real-verifier control,
     *    separate from the counted-disposal cases below. */
    {
        reset_seam();
        hook_ctx_t h; memset(&h, 0, sizeof(h));
        moq_pq_threaded_cfg_t c; client_cfg(&c, base + 0, &h, 0);
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&c, &t) == MOQ_OK);
        CHECK(moq_pq_threaded_test_cert_install_calls == 1);
        CHECK(moq_pq_threaded_test_null_verifier_calls == 0);
        CHECK(h.hook_calls == 1);
        CHECK(h.verify_at_entry != NULL);   /* default present before hook */
        if (t) { moq_pq_threaded_stop(t); moq_pq_threaded_destroy(t); }
    }

    /* -- Case 2: insecure client -- null verifier selected, system install
     *    NOT called; master ctx carries no verifier. */
    {
        reset_seam();
        hook_ctx_t h; memset(&h, 0, sizeof(h));
        moq_pq_threaded_cfg_t c; client_cfg(&c, base + 1, &h, 1);
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&c, &t) == MOQ_OK);
        CHECK(moq_pq_threaded_test_cert_install_calls == 0);
        CHECK(moq_pq_threaded_test_null_verifier_calls == 1);
        CHECK(h.hook_calls == 1);
        CHECK(h.verify_at_entry == NULL);   /* null verifier => no policy */
        if (t) { moq_pq_threaded_stop(t); moq_pq_threaded_destroy(t); }
    }

    /* -- Case 3: custom hook -- disposal is exact at EACH ownership boundary,
     *    asserted IN PHASE from snapshots captured inside the hook. */
    {
        reset_seam();
        moq_pq_threaded_test_cert_installer = counting_installer0;
        default0_disposed = custom1_disposed = custom2_disposed = 0;
        hook_ctx_t h; memset(&h, 0, sizeof(h));
        h.install_customs = 1;
        moq_pq_threaded_cfg_t c; client_cfg(&c, base + 2, &h, 0);
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&c, &t) == MOQ_OK);
        CHECK(moq_pq_threaded_test_cert_install_calls == 1);
        CHECK(h.verify_at_entry == (void *)&default0);  /* hook sees default0 */

        /* Phase 0 -- before any replacement: nothing disposed, default active. */
        CHECK(h.d0_before == 0 && h.c1_before == 0 && h.c2_before == 0);
        CHECK(h.active_before == (void *)&default0);
        /* Phase 1 -- after installing custom1: only the default is disposed. */
        CHECK(h.d0_after1 == 1);
        CHECK(h.c1_after1 == 0);
        CHECK(h.c2_after1 == 0);
        CHECK(h.active_after1 == (void *)&custom1);
        /* Phase 2 -- after installing custom2: only custom1 additionally. */
        CHECK(h.d0_after2 == 1);
        CHECK(h.c1_after2 == 1);
        CHECK(h.c2_after2 == 0);
        CHECK(h.active_after2 == (void *)&custom2);
        CHECK(h.replacement_ok == 1);

        /* Post-create (== end of hook, before destroy). */
        CHECK(default0_disposed == 1);
        CHECK(custom1_disposed == 1);
        CHECK(custom2_disposed == 0);
        if (t) { moq_pq_threaded_stop(t); moq_pq_threaded_destroy(t); }
        /* Phase 3 -- free boundary: only custom2 advances, exactly to one. */
        CHECK(default0_disposed == 1);
        CHECK(custom1_disposed == 1);
        CHECK(custom2_disposed == 1);
    }

    /* -- Case 4: default install failure -- create fails, handle stays NULL,
     *    the configure hook is never reached, no connection/thread escapes. */
    {
        reset_seam();
        moq_pq_threaded_test_cert_installer = failing_installer;
        hook_ctx_t h; memset(&h, 0, sizeof(h));
        moq_pq_threaded_cfg_t c; client_cfg(&c, base + 3, &h, 0);
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&c, &t) != MOQ_OK);
        CHECK(t == NULL);
        CHECK(moq_pq_threaded_test_cert_install_calls == 1);
        CHECK(h.hook_calls == 0);   /* fail-closed BEFORE the hook */
    }

    /* -- Case 5: configure-hook failure AFTER the default install -- the
     *    installed default is disposed EXACTLY ONCE on the failure teardown,
     *    proven by phase: zero at hook entry, one only after the failed create. */
    {
        reset_seam();
        moq_pq_threaded_test_cert_installer = counting_installer5;
        default5_disposed = 0;
        hook_ctx_t h; memset(&h, 0, sizeof(h));
        h.return_fail = 1;
        moq_pq_threaded_cfg_t c; client_cfg(&c, base + 4, &h, 0);
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&c, &t) != MOQ_OK);
        CHECK(t == NULL);
        CHECK(moq_pq_threaded_test_cert_install_calls == 1);  /* default ran */
        CHECK(h.hook_calls == 1);                             /* then failed */
        CHECK(h.d5_at_entry == 0);        /* not yet disposed at hook entry */
        CHECK(default5_disposed == 1);    /* disposed once on the failure path */
    }

    /* -- Case 6: server -- no automatic client verifier installation. */
    {
        reset_seam();
        moq_pq_threaded_cfg_t s;
        moq_pq_threaded_cfg_init_sized(&s, sizeof(s));
        s.alloc = moq_alloc_default();
        s.perspective = MOQ_PERSPECTIVE_SERVER;
        s.cert_path = MOQ_TEST_CERT_PATH;
        s.key_path = MOQ_TEST_KEY_PATH;
        s.port = base + 5;
        s.on_lane_pump = dummy_pump;
        moq_pq_threaded_t *t = NULL;
        CHECK(moq_pq_threaded_create(&s, &t) == MOQ_OK);
        CHECK(moq_pq_threaded_test_cert_install_calls == 0);
        CHECK(moq_pq_threaded_test_null_verifier_calls == 0);
        if (t) { moq_pq_threaded_stop(t); moq_pq_threaded_destroy(t); }
    }

    if (failures) {
        fprintf(stderr, "threaded_tls_policy: %d failure(s)\n", failures);
        return 1;
    }
    printf("threaded_tls_policy: all cases passed\n");
    return 0;
}

#else  /* !MOQ_TEST_CERT_PATH: fixtures are guaranteed by CMake (fail-fast when
        * MOQ_BUILD_TESTS && MOQ_BUILD_PQ_THREADED), so this arm is a
        * compile-time defense-in-depth guard only -- never a runtime skip. It
        * fails loudly instead of returning success. */

int main(void)
{
    fprintf(stderr,
            "FAIL: threaded_tls_policy built without MOQ_TEST_CERT_PATH "
            "(CMake fixture contract broken)\n");
    return 1;
}

#endif
