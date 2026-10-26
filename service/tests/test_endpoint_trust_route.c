/*
 * The delegated-trust connect route, driven through the REAL endpoint create
 * path with NO transport.
 *
 * endpoint.c is compiled into this binary with two boundary groups renamed to
 * recorders defined below: the two final verifier setters, and the whole
 * managed-facade runtime. Everything between them - resolution, endpoint
 * allocation, `ep_create_pq`/`ep_create_wt`, verifier route selection,
 * callback translation, and connect-failure cleanup - is production code.
 *
 * The fake facade opens no socket, starts no thread, consults no clock and
 * depends on no peer. It receives the real production facade config and
 * invokes its real `configure_quic` callback exactly once on a deterministic
 * QUIC context, and mirrors production semantics: a non-zero configure result
 * fails the create (adapters/picoquic/moq_picoquic_threaded.c:1358).
 *
 * The route is implemented; these rows are its permanent contract. Each row
 * asserts on its own named oracle, and the rows that characterise the existing
 * NULL-delegate behaviour are the controls.
 */
#include "endpoint_internal.h"
#include "test_support.h"

#include <moq/picoquic_verify.h>

#include "endpoint_trust.h"

#include <moq/picoquic_threaded.h>
#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
#include <moq/mvfst.h>
#endif
/* The renamed NON-PICOQUIC facade runtime. It exists so a row can OBSERVE that
 * a delegate requested on another backend never reaches that backend's create
 * path -- an UNSUPPORTED answer alone could come from the backend simply not
 * being built, which is not an oracle. No mvfst library is linked and no socket
 * is opened; only the entry points endpoint.c calls are provided. */
static unsigned g_mvfst_creates;
moq_result_t moq_test_mvfst_create(const moq_mvfst_managed_cfg_t *cfg,
                                   moq_mvfst_managed_t **out)
{
    (void)cfg;
    g_mvfst_creates++;
    if (out) *out = (moq_mvfst_managed_t *)&g_mvfst_creates;
    return MOQ_OK;
}
static unsigned g_mvfst_stops;
static unsigned g_mvfst_destroys;
moq_result_t moq_test_mvfst_stop(moq_mvfst_managed_t *t)
{ (void)t; g_mvfst_stops++; return MOQ_OK; }
void moq_test_mvfst_destroy(moq_mvfst_managed_t *t) { (void)t; g_mvfst_destroys++; }
moq_result_t moq_test_mvfst_wait(moq_mvfst_managed_t *t, uint64_t us)
{ (void)t; (void)us; return MOQ_OK; }
moq_result_t moq_test_mvfst_wake(moq_mvfst_managed_t *t) { (void)t; return MOQ_OK; }
moq_session_t *moq_test_mvfst_session(moq_mvfst_managed_t *t) { (void)t; return NULL; }
bool moq_test_mvfst_is_fatal(const moq_mvfst_managed_t *t) { (void)t; return false; }
uint64_t moq_test_mvfst_fatal_code(const moq_mvfst_managed_t *t) { (void)t; return 0; }
moq_version_t moq_test_mvfst_negotiated_version(const moq_mvfst_managed_t *t)
{ (void)t; return 0; }
void moq_test_mvfst_cfg_init_sized(moq_mvfst_managed_cfg_t *cfg, size_t n)
{
    if (cfg == NULL || n == 0) return;
    unsigned char *p = (unsigned char *)cfg;
    for (size_t i = 0; i < n; i++) p[i] = 0;
    cfg->struct_size = n;
}

#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
#include <moq/pico_wt_managed.h>
#endif

#include <picoquic.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

/* No peer is contacted, so the authority is arbitrary; it is a syntactically
 * valid URL and nothing more. */
#define FAKE_URL_QUIC "moqt://endpoint.invalid:14461"
#define FAKE_URL_WT   "https://endpoint.invalid:14462/moq"

/* -- counting allocator -------------------------------------------------- *
 * Every endpoint-owned allocation goes through this, so a leak on any exit
 * path is an assertion rather than a later crash. */

typedef struct count_alloc {
    size_t live_bytes;
    size_t live_blocks;
    size_t total_allocs;
} count_alloc_t;

static count_alloc_t g_alloc;

static void *ca_alloc(size_t n, void *ctx)
{
    count_alloc_t *a = (count_alloc_t *)ctx;
    void *p = malloc(n);
    if (p) { a->live_bytes += n; a->live_blocks++; a->total_allocs++; }
    return p;
}

static void ca_free(void *p, size_t n, void *ctx)
{
    count_alloc_t *a = (count_alloc_t *)ctx;
    if (!p) return;
    a->live_bytes -= n;
    a->live_blocks--;
    free(p);
}

static void *ca_realloc(void *p, size_t oldn, size_t newn, void *ctx)
{
    count_alloc_t *a = (count_alloc_t *)ctx;
    void *q = realloc(p, newn);
    if (!q) return NULL;
    if (!p) { a->live_blocks++; a->total_allocs++; a->live_bytes += newn; }
    else    { a->live_bytes = a->live_bytes - oldn + newn; }
    return q;
}

static moq_alloc_t counting_alloc(void)
{
    moq_alloc_t a;
    memset(&a, 0, sizeof(a));
    a.alloc = ca_alloc;
    a.free = ca_free;
    a.realloc = ca_realloc;
    a.ctx = &g_alloc;
    return a;
}

/* -- the deterministic fake managed facade ------------------------------- *
 * One shared record for both protocols: the endpoint holds an opaque pointer
 * and only ever passes it back through the vtable, so a single identity is
 * enough and keeps the allocator oracle about ENDPOINT-owned memory only. */

typedef struct fake_facade {
    /* ATTEMPTS, not just successes: a row that must prove the facade was never
     * reached needs to see an attempt that failed during configure too. */
    unsigned create_attempts;
    unsigned created;
    unsigned stopped;
    unsigned destroyed;
    unsigned configure_calls;
    int      configure_rc;
    bool     live;            /* create succeeded and destroy has not run */
} fake_facade_t;

static fake_facade_t g_fac;
static bool g_fake_create_fail;          /* force the create itself to fail */
static picoquic_quic_t *g_fake_quic;     /* the deterministic context */

/* Which facade the route actually selected. Counted separately so a row can
 * say the REQUESTED protocol reached exactly its matching create and never the
 * other one -- a shared total cannot distinguish that. */
typedef enum fake_kind { FAKE_RAW = 0, FAKE_WT = 1, FAKE_KIND_COUNT } fake_kind_t;
static unsigned g_create_calls[FAKE_KIND_COUNT];
static const char *const k_fake_kind_name[FAKE_KIND_COUNT] = { "RAW_QUIC",
                                                               "WEBTRANSPORT" };

static void fake_reset(void)
{
    memset(&g_fac, 0, sizeof(g_fac));
    memset(g_create_calls, 0, sizeof(g_create_calls));
    g_fake_create_fail = false;
}

/* A QUIC context with no listener, no thread and no clock dependency:
 * picoquic_create() only builds the context object. */
static picoquic_quic_t *fake_quic_acquire(void)
{
    if (g_fake_quic == NULL)
        g_fake_quic = picoquic_create(4, NULL, NULL, NULL, "moq-00", NULL,
                                      NULL, NULL, NULL, NULL, 0, NULL, NULL,
                                      NULL, 0);
    return g_fake_quic;
}

static moq_result_t fake_create(fake_kind_t kind,
                                int (*configure)(picoquic_quic_t *, void *),
                                void *configure_ctx, void **out)
{
    *out = NULL;
    g_fac.create_attempts++;
    g_create_calls[kind]++;
    if (g_fake_create_fail) return MOQ_ERR_INTERNAL;
    picoquic_quic_t *q = fake_quic_acquire();
    MOQ_TEST_CHECK(q != NULL);
    if (q == NULL) return MOQ_ERR_INTERNAL;
    if (configure != NULL) {
        g_fac.configure_calls++;
        g_fac.configure_rc = configure(q, configure_ctx);
        /* production semantics: a non-zero configure fails the create */
        if (g_fac.configure_rc != 0) return MOQ_ERR_INTERNAL;
    }
    g_fac.created++;
    g_fac.live = true;
    *out = &g_fac;
    return MOQ_OK;
}

static moq_result_t fake_stop(void *f)
{
    MOQ_TEST_CHECK(f == &g_fac);
    g_fac.stopped++;
    return MOQ_OK;
}

static void fake_destroy(void *f)
{
    MOQ_TEST_CHECK(f == &g_fac);
    g_fac.destroyed++;
    g_fac.live = false;
}

/* The renamed picoquic-threaded facade runtime. */
moq_result_t moq_test_pq_create(const moq_pq_threaded_cfg_t *cfg,
                                moq_pq_threaded_t **out)
{
    /* the fake must see the REAL production config */
    MOQ_TEST_CHECK(cfg != NULL && cfg->alloc != NULL);
    MOQ_TEST_CHECK(cfg->configure_quic != NULL);
    return fake_create(FAKE_RAW, cfg->configure_quic, cfg->configure_quic_ctx,
                       (void **)out);
}
moq_result_t moq_test_pq_stop(moq_pq_threaded_t *t) { return fake_stop(t); }
void moq_test_pq_destroy(moq_pq_threaded_t *t) { fake_destroy(t); }
moq_result_t moq_test_pq_wait(moq_pq_threaded_t *t, uint64_t us)
{ (void)t; (void)us; return MOQ_OK; }
moq_result_t moq_test_pq_wake(moq_pq_threaded_t *t) { (void)t; return MOQ_OK; }
moq_session_t *moq_test_pq_session(moq_pq_threaded_t *t) { (void)t; return NULL; }
bool moq_test_pq_is_fatal(const moq_pq_threaded_t *t) { (void)t; return false; }
uint64_t moq_test_pq_fatal_code(const moq_pq_threaded_t *t) { (void)t; return 0; }
int moq_test_pq_drain_state(moq_pq_threaded_t *t) { (void)t; return 1; }
uint64_t moq_test_pq_terminal_error(const moq_pq_threaded_t *t) { (void)t; return 0; }
moq_version_t moq_test_pq_negotiated_version(const moq_pq_threaded_t *t)
{ (void)t; return 0; }

#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
moq_result_t moq_test_wt_create(const moq_pico_wt_managed_cfg_t *cfg,
                                moq_pico_wt_managed_t **out)
{
    MOQ_TEST_CHECK(cfg != NULL && cfg->alloc != NULL);
    MOQ_TEST_CHECK(cfg->configure_quic != NULL);
    return fake_create(FAKE_WT, cfg->configure_quic, cfg->configure_quic_ctx,
                       (void **)out);
}
moq_result_t moq_test_wt_stop(moq_pico_wt_managed_t *m) { return fake_stop(m); }
void moq_test_wt_destroy(moq_pico_wt_managed_t *m) { fake_destroy(m); }
moq_result_t moq_test_wt_wait(moq_pico_wt_managed_t *m, uint64_t us)
{ (void)m; (void)us; return MOQ_OK; }
moq_result_t moq_test_wt_wake(moq_pico_wt_managed_t *m) { (void)m; return MOQ_OK; }
moq_session_t *moq_test_wt_session(moq_pico_wt_managed_t *m) { (void)m; return NULL; }
bool moq_test_wt_is_fatal(const moq_pico_wt_managed_t *m) { (void)m; return false; }
bool moq_test_wt_is_closed(const moq_pico_wt_managed_t *m) { (void)m; return false; }
uint64_t moq_test_wt_fatal_code(const moq_pico_wt_managed_t *m) { (void)m; return 0; }
int moq_test_wt_drain_state(moq_pico_wt_managed_t *m) { (void)m; return 1; }
uint64_t moq_test_wt_terminal_error(const moq_pico_wt_managed_t *m) { (void)m; return 0; }
moq_version_t moq_test_wt_negotiated_version(const moq_pico_wt_managed_t *m)
{ (void)m; return 0; }
#endif

/* -- recorders ---------------------------------------------------------- *
 * The two final adapter boundaries. Counting BOTH is what lets a row say
 * which verifier was selected, rather than only that something was. */

static unsigned deleg_calls;
static int deleg_result;
static moq_picoquic_cert_verify_fn deleg_installed_fn;
static void *deleg_installed_ctx;

static unsigned path_calls;
static int path_result;
static char path_last_ca[256];
static bool path_last_ca_null;

int moq_test_set_cert_verify_callback(picoquic_quic_t *quic,
                                      moq_picoquic_cert_verify_fn verify,
                                      void *ctx)
{
    deleg_calls++;
    MOQ_TEST_CHECK(quic != NULL);
    deleg_installed_fn = verify;
    deleg_installed_ctx = ctx;
    return deleg_result;
}

int moq_test_set_cert_verifier(picoquic_quic_t *quic, const char *ca_file)
{
    path_calls++;
    MOQ_TEST_CHECK(quic != NULL);
    path_last_ca_null = (ca_file == NULL);
    path_last_ca[0] = '\0';
    if (ca_file != NULL) {
        size_t n = strlen(ca_file);
        if (n >= sizeof(path_last_ca)) n = sizeof(path_last_ca) - 1;
        memcpy(path_last_ca, ca_file, n);
        path_last_ca[n] = '\0';
    }
    return path_result;
}

static void reset_recorders(void)
{
    deleg_calls = 0;
    deleg_result = 0;
    deleg_installed_fn = NULL;
    deleg_installed_ctx = NULL;
    path_calls = 0;
    path_result = 0;
    path_last_ca[0] = '\0';
    path_last_ca_null = false;
}

/* -- the service-shaped delegate ----------------------------------------- */

typedef struct trust_probe {
    unsigned calls;
    moq_endpoint_trust_verdict_t verdict;
    void *identity;
    bool saw_expected_input;
} trust_probe_t;

static const uint8_t k_name[] = "relay.example";
static const uint8_t k_leaf[] = {0x30, 0x03, 0x01};
static const uint8_t k_issuer[] = {0x30, 0x03, 0x02};

static moq_endpoint_trust_verdict_t probe_verify(void *ctx,
                                                 moq_bytes_t server_name,
                                                 const moq_bytes_t *certs,
                                                 size_t n)
{
    trust_probe_t *p = (trust_probe_t *)ctx;
    if (p == NULL) return MOQ_ENDPOINT_TRUST_REJECT;
    p->calls++;
    p->saw_expected_input =
        p->identity == p &&
        server_name.len == sizeof(k_name) - 1 &&
        server_name.data != NULL &&
        memcmp(server_name.data, k_name, sizeof(k_name) - 1) == 0 &&
        n == 2 && certs != NULL &&
        certs[0].len == sizeof(k_leaf) &&
        certs[0].data != NULL &&
        memcmp(certs[0].data, k_leaf, sizeof(k_leaf)) == 0 &&
        certs[1].len == sizeof(k_issuer) &&
        certs[1].data != NULL &&
        memcmp(certs[1].data, k_issuer, sizeof(k_issuer)) == 0;
    return p->verdict;
}

/* Never expected to run: the validation rows refuse before any install. */
static moq_endpoint_trust_verdict_t never_called(void *ctx, moq_bytes_t name,
                                                 const moq_bytes_t *certs,
                                                 size_t n)
{
    (void)ctx; (void)name; (void)certs; (void)n;
    MOQ_TEST_CHECK(false);
    return MOQ_ENDPOINT_TRUST_REJECT;
}

static moq_bytes_t B(const char *s)
{
    return (moq_bytes_t){(const uint8_t *)s, strlen(s)};
}

static const char *g_url;
static moq_transport_protocol_t g_protocol;

static moq_alloc_t g_cfg_alloc;

static void base_cfg(moq_endpoint_cfg_t *cfg)
{
    moq_endpoint_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->url = B(g_url);
    cfg->protocol = g_protocol;
    cfg->alloc = &g_cfg_alloc;
}

/* -- the row and scenario ledgers ---------------------------------------- *
 * Row ownership is explicit and mechanically total. A row is opened with its
 * identity AND its facade-selection obligation, and the single common close
 * path discharges both. Facade selection is therefore not a call a row can
 * forget: it is acquired before any product code runs and compared exactly
 * once at close.
 *
 * The active token proves begin/end are balanced. It does NOT prove a whole
 * scenario was invoked, so a separate scenario ledger counts each row identity
 * and is compared at exit against a set derived independently from the
 * compiled facade set, the configured provider, and the accepted struct_size
 * count. */

typedef enum row_id {
    ROW_CONFLICT_INSECURE = 0,
    ROW_CONFLICT_CA_FILE,
    ROW_UNSUPPORTED_PROVIDER,
    ROW_PARITY_WITH_TRUST_NULL,
    ROW_PARITY_PLAIN_CONNECT,
    ROW_NULL_SELECTS_PATH_VERIFIER,
    ROW_DELEGATED_SELECTION,
    ROW_INSTALL_FAILURE,
    ROW_INSTALL_FAILURE_REPEAT,
    ROW_CREATE_FAILURE,
    ROW_DELEGATE_WRONG_BACKEND,
    ROW_UNDERSIZED_CFG,
    ROW_SERVICE_STATELESS_CTX,
    ROW_ID_COUNT
} row_id_t;

static const char *const k_row_name[ROW_ID_COUNT] = {
    "conflict.insecure",
    "conflict.ca_file",
    "unsupported_provider",
    "parity.with_trust_null",
    "parity.plain_connect",
    "null_selects_path_verifier",
    "delegated_selection",
    "install_failure",
    "install_failure.repeat",
    "create_failure",
    "delegate.wrong_backend",
    "undersized_cfg",
    "service.stateless_ctx",
};

/* One counter per (protocol, row): a missing, duplicated or foreign scenario
 * is a named failure at exit. */
static unsigned g_scenario_runs[FAKE_KIND_COUNT][ROW_ID_COUNT];

static bool g_row_active;
static row_id_t g_row_id;
static bool g_row_create_expected;

static fake_kind_t requested_kind(void)
{
    return g_protocol == MOQ_TRANSPORT_PROTOCOL_RAW_QUIC ? FAKE_RAW : FAKE_WT;
}

/* The single protocol pass that carries the rows which need one representative
 * run. It is DETERMINISTIC and derived from the compiled facade set, never from
 * a hard-coded protocol: this target is built when EITHER picoquic facade is
 * enabled, so a pico-WT-only build must still run them. The same derivation
 * produces the ledger's expected identity. */
static fake_kind_t first_picoquic_kind(void)
{
#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
    return FAKE_RAW;
#else
    return FAKE_WT;
#endif
}

/* Open a row. `create_expected` is the row's DECLARED facade obligation and is
 * checked at close, so it cannot be omitted per row. */
static void row_begin(row_id_t id, bool create_expected)
{
    MOQ_TEST_CHECK(!g_row_active);
    if (g_row_active)
        fprintf(stderr, "ROW LEAK: '%s' still active when '%s' began\n",
                k_row_name[g_row_id], k_row_name[id]);
    /* the prior row's facade obligation is judged BEFORE any counter reset */
    MOQ_TEST_CHECK(!g_fac.live);

    g_row_active = true;
    g_row_id = id;
    g_row_create_expected = create_expected;
    g_scenario_runs[requested_kind()][id]++;
    fake_reset();
    reset_recorders();
    MOQ_TEST_CHECK_EQ_U64(g_alloc.live_blocks, 0);
    MOQ_TEST_CHECK_EQ_U64(g_alloc.live_bytes, 0);
}

/* The single close path: allocator, facade lifetime, and the row's declared
 * facade selection. */
static void row_close_common(void)
{
    MOQ_TEST_CHECK(g_row_active);
    if (g_row_active) {
        fake_kind_t want = requested_kind();
        for (int k = 0; k < FAKE_KIND_COUNT; k++) {
            unsigned expect =
                (k == (int)want && g_row_create_expected) ? 1u : 0u;
            if (g_create_calls[k] != expect)
                fprintf(stderr,
                        "FACADE SELECTION [%s]: requested %s, %s create x%u, "
                        "expected x%u\n",
                        k_row_name[g_row_id], k_fake_kind_name[want],
                        k_fake_kind_name[k], g_create_calls[k], expect);
            MOQ_TEST_CHECK_EQ_U64(g_create_calls[k], expect);
        }
    }
    g_row_active = false;
    MOQ_TEST_CHECK_EQ_U64(g_alloc.live_blocks, 0);
    MOQ_TEST_CHECK_EQ_U64(g_alloc.live_bytes, 0);
    MOQ_TEST_CHECK_EQ_U64(g_fac.stopped, g_fac.created);
    MOQ_TEST_CHECK_EQ_U64(g_fac.destroyed, g_fac.created);
    MOQ_TEST_CHECK(!g_fac.live);
}

/* End a row that produced no endpoint. */
static void row_end_no_endpoint(void)
{
    row_close_common();
}

/* The explicit CA a NULL-delegate fixture needs on a provider with NO implicit
 * store. On such a provider a config with no CA is refused up front, which
 * would make the parity rows compare two identical refusals and prove nothing;
 * the CA is what keeps them non-vacuous. `ca_file` is inside the frozen v0
 * prefix, so it is settable at every accepted struct_size. */
static void add_path_ca(moq_endpoint_cfg_t *cfg)
{
#if defined(MOQ_TEST_VERIFY_BACKEND_MBEDTLS)
    cfg->ca_file = B(MOQ_TEST_CA_PEM);
#else
    (void)cfg;
#endif
}

/* Teardown is asserted, never conditional: a stop that does not return MOQ_OK
 * would otherwise silently skip destroy and hide a leak. */
static void destroy_ep(moq_endpoint_t *ep)
{
    MOQ_TEST_CHECK(ep != NULL);
    if (ep == NULL) return;
    unsigned stopped_before = g_fac.stopped;
    unsigned destroyed_before = g_fac.destroyed;
    MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
    /* the endpoint stops the facade it acquired, exactly once */
    MOQ_TEST_CHECK_EQ_U64(g_fac.stopped, stopped_before + 1);
    moq_endpoint_destroy(ep);
    /* and destroys it exactly once, after the stop */
    MOQ_TEST_CHECK_EQ_U64(g_fac.destroyed, destroyed_before + 1);
    MOQ_TEST_CHECK(!g_fac.live);
    row_close_common();
}

/* -- ORACLE 1: both trust-source conflicts are exactly MOQ_ERR_INVAL ----- */
static void oracle_conflicts(void)
{
    {
        moq_endpoint_cfg_t cfg; moq_endpoint_t *ep = (moq_endpoint_t *)1;
        base_cfg(&cfg);
        cfg.insecure_skip_verify = true;
        row_begin(ROW_CONFLICT_INSECURE, /*create_expected=*/false);
        moq_result_t rc =
            moq_endpoint_connect_with_trust(&cfg, never_called, NULL, &ep);
        /* a delegate together with insecure_skip_verify is a configuration
         * error, not a precedence question */
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(ep == NULL);
        /* refused before any install boundary is reached */
        MOQ_TEST_CHECK_EQ_U64(deleg_calls, 0);
        MOQ_TEST_CHECK_EQ_U64(path_calls, 0);
        /* the refusal precedes facade selection entirely */
        row_end_no_endpoint();
    }
    {
        moq_endpoint_cfg_t cfg; moq_endpoint_t *ep = (moq_endpoint_t *)1;
        base_cfg(&cfg);
        /* A REAL, loadable bundle. An unloadable path would make the CA
         * preflight produce the same MOQ_ERR_INVAL, so the row could not tell
         * the conflict rule from the preflight and the one-change mutant would
         * survive. */
        cfg.ca_file = B(MOQ_TEST_CA_PEM);
        row_begin(ROW_CONFLICT_CA_FILE, /*create_expected=*/false);
        moq_result_t rc =
            moq_endpoint_connect_with_trust(&cfg, never_called, NULL, &ep);
        /* two trust sources is a configuration error */
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(ep == NULL);
        MOQ_TEST_CHECK_EQ_U64(deleg_calls, 0);
        MOQ_TEST_CHECK_EQ_U64(path_calls, 0);
        row_end_no_endpoint();
    }
}

/* -- ORACLE 2: an unsupported provider is refused by name ----------------- */
/* -- a delegate on a NON-PICOQUIC backend is refused, not ignored --------- *
 * Only picoquic's create path consumes the delegate. Every other backend would
 * verify with its own TLS policy while the caller believed the delegate was in
 * force, so the route must refuse before that backend is created.
 *
 * The fake non-picoquic facade compiled into this object set is what makes the
 * proof real: an UNSUPPORTED answer alone could equally mean "that backend is
 * not built", which is no oracle at all. Here the backend IS built, so a
 * create count of zero is evidence. */
#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
static void oracle_delegate_wrong_backend(void)
{
    /* Runs ONCE per suite, on whichever picoquic facade the build compiled --
     * the contract it proves is about the SELECTED backend, not about which
     * picoquic protocol supplied the test entry. Its own cfg therefore names
     * raw QUIC and a raw-QUIC URL outright, independent of the enclosing pass:
     * mvfst is native QUIC only, and a URL the protocol does not name would
     * make the resolver refuse for a reason unrelated to trust. */
    if (requested_kind() != first_picoquic_kind()) return;
    row_begin(ROW_DELEGATE_WRONG_BACKEND, /*create_expected=*/false);
    moq_endpoint_cfg_t cfg; moq_endpoint_t *ep = (moq_endpoint_t *)1;
    /* mvfst is EXACT-version: its session is created before TLS, so a
     * multi-version offer is refused for a reason unrelated to trust, and the
     * row would then prove nothing. The NESTED struct_size is what makes the
     * offer read at all -- a zero one means AUTO and the fields below are
     * deliberately ignored (endpoint.c: "Zero-init (struct_size 0) means
     * AUTO"). */
    static const moq_version_t k_one = MOQ_VERSION_DRAFT_18;
    base_cfg(&cfg);
    cfg.url      = B(FAKE_URL_QUIC);
    cfg.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
    cfg.backend  = MOQ_TRANSPORT_BACKEND_MVFST;
    cfg.versions.struct_size = sizeof(cfg.versions);
    cfg.versions.policy = MOQ_VERSION_POLICY_EXACT;
    cfg.versions.versions = &k_one;
    cfg.versions.version_count = 1;

    trust_probe_t probe = {0, MOQ_ENDPOINT_TRUST_ACCEPT, NULL, false};
    probe.identity = &probe;
    unsigned creates_before = g_mvfst_creates;
    unsigned deleg_before = deleg_calls;
    unsigned path_before = path_calls;

    moq_result_t rc =
        moq_endpoint_connect_with_trust(&cfg, probe_verify, &probe, &ep);

    /* refused BY NAME, with no endpoint ... */
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK(ep == NULL);
    /* ... and if a mutant DID produce one, release it AFTER the diagnostics
     * above are recorded, so the mutant's own failures survive and its leak
     * does not contaminate every later row's allocator balance. The release
     * uses the CANONICAL stop-then-destroy lifetime: destroying a running
     * endpoint is a documented programming error the product asserts on, and
     * an abort there would stop the row before its create-count oracle ran.
     * destroy_ep() cannot serve here -- it asserts the PICOQUIC facade's
     * counters, and a mutant that reached this point created the fake mvfst
     * facade instead -- so this path checks the mvfst counters exactly. */
    if (rc == MOQ_OK && ep != NULL) {
        unsigned mv_stops = g_mvfst_stops;
        unsigned mv_destroys = g_mvfst_destroys;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(g_mvfst_stops, mv_stops + 1);
        moq_endpoint_destroy(ep);
        MOQ_TEST_CHECK_EQ_U64(g_mvfst_destroys, mv_destroys + 1);
        ep = NULL;
    }
    /* ... the other backend was never created ... */
    MOQ_TEST_CHECK_EQ_U64(g_mvfst_creates - creates_before, 0);
    /* ... neither verifier setter ran, and the delegate was never consulted. */
    MOQ_TEST_CHECK_EQ_U64(path_calls - path_before, 0);
    MOQ_TEST_CHECK_EQ_U64(deleg_calls - deleg_before, 0);

    /* This IS the discriminator: the backend is compiled in and genuinely
     * selectable (a single EXACT version satisfies mvfst's own precondition),
     * so removing only the selected-backend guard reaches the fake create --
     * which is what makes the zeros above evidence rather than a coincidence. */
    row_close_common();
}
#endif

/* A STATELESS SERVICE delegate: `ctx` is opaque at this tier too, and a
 * delegate that keeps no state legitimately passes NULL. File-scope counters
 * stand in for the state a context would carry. */
static unsigned g_svc_stateless_calls;
/* Counted per invocation, not latched: a sticky "some call saw NULL" boolean
 * cannot fail when a LATER call is handed a context. */
static unsigned g_svc_null_ctx;
static unsigned g_svc_nonnull_ctx;
static moq_endpoint_trust_verdict_t g_svc_stateless_verdict;
static moq_endpoint_trust_verdict_t svc_stateless_verify(
    void *ctx, moq_bytes_t server_name,
    const moq_bytes_t *certificates, size_t certificate_count)
{
    (void)server_name; (void)certificates; (void)certificate_count;
    g_svc_stateless_calls++;
    if (ctx == NULL) g_svc_null_ctx++; else g_svc_nonnull_ctx++;
    return g_svc_stateless_verdict;
}

/* -- the hidden route stores and forwards a NULL context ------------------ *
 * The adapter rows prove the adapter API accepts NULL. This proves the SERVICE
 * layer: that connect_with_trust() stores NULL, that the bridge installed on
 * the adapter carries the ENDPOINT as its own context, and that the service
 * callback is handed NULL exactly once per invocation with each verdict
 * translated exactly. */
static void oracle_service_stateless_ctx(void)
{
#if !defined(MOQ_TEST_VERIFY_BACKEND_OPENSSL)
    return;   /* the configured provider cannot carry a delegate */
#else
    if (requested_kind() != first_picoquic_kind()) return;
    row_begin(ROW_SERVICE_STATELESS_CTX, /*create_expected=*/true);
    moq_endpoint_cfg_t cfg; moq_endpoint_t *ep = NULL;
    base_cfg(&cfg);

    unsigned deleg_before = deleg_calls;
    unsigned path_before = path_calls;
    deleg_installed_fn = NULL;
    deleg_installed_ctx = NULL;
    g_svc_stateless_calls = 0;
    g_svc_null_ctx = 0;
    g_svc_nonnull_ctx = 0;

    moq_result_t rc =
        moq_endpoint_connect_with_trust(&cfg, svc_stateless_verify, NULL, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    /* exactly the delegated setter, and never the path setter */
    MOQ_TEST_CHECK_EQ_U64(deleg_calls - deleg_before, 1);
    MOQ_TEST_CHECK_EQ_U64(path_calls - path_before, 0);
    MOQ_TEST_CHECK(deleg_installed_fn != NULL);
    /* the bridge's OWN installed context is the endpoint, not the caller's */
    MOQ_TEST_CHECK(deleg_installed_ctx == (void *)ep);

    if (deleg_installed_fn != NULL && ep != NULL) {
        moq_bytes_t name = B("relay.example");
        moq_bytes_t chain[1]; chain[0] = B("der");

        g_svc_stateless_verdict = MOQ_ENDPOINT_TRUST_ACCEPT;
        moq_picoquic_cert_verdict_t v =
            deleg_installed_fn(deleg_installed_ctx, name, chain, 1);
        MOQ_TEST_CHECK_EQ_U64(g_svc_stateless_calls, 1);
        MOQ_TEST_CHECK_EQ_U64(g_svc_null_ctx, 1);
        MOQ_TEST_CHECK_EQ_U64(g_svc_nonnull_ctx, 0);
        MOQ_TEST_CHECK_EQ_INT((int)v, (int)MOQ_PICOQUIC_CERT_TRUST);

        g_svc_stateless_verdict = MOQ_ENDPOINT_TRUST_REJECT;
        v = deleg_installed_fn(deleg_installed_ctx, name, chain, 1);
        MOQ_TEST_CHECK_EQ_U64(g_svc_stateless_calls, 2);
        MOQ_TEST_CHECK_EQ_U64(g_svc_null_ctx, 2);
        MOQ_TEST_CHECK_EQ_U64(g_svc_nonnull_ctx, 0);
        MOQ_TEST_CHECK_EQ_INT((int)v, (int)MOQ_PICOQUIC_CERT_REJECT);

        /* an UNKNOWN service verdict fails closed */
        g_svc_stateless_verdict = (moq_endpoint_trust_verdict_t)7;
        v = deleg_installed_fn(deleg_installed_ctx, name, chain, 1);
        MOQ_TEST_CHECK_EQ_U64(g_svc_stateless_calls, 3);
        MOQ_TEST_CHECK_EQ_U64(g_svc_null_ctx, 3);
        MOQ_TEST_CHECK_EQ_U64(g_svc_nonnull_ctx, 0);
        MOQ_TEST_CHECK_EQ_INT((int)v, (int)MOQ_PICOQUIC_CERT_REJECT);
    }

    if (ep != NULL) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
        moq_endpoint_destroy(ep);
    }
    MOQ_TEST_CHECK_EQ_U64(g_alloc.live_blocks, 0);
    row_close_common();
#endif
}

/* -- an UNDERSIZED caller is refused before any field is read ------------- *
 * The storage here contains ONLY `struct_size`. Every other field -- including
 * the trust-conflict fields, which live INSIDE the v0 prefix, not past it --
 * is therefore outside the allocation, so reading any of them before the floor
 * check is a real out-of-bounds read that ASan/UBSan names. */
static void oracle_undersized_cfg(void)
{
    row_begin(ROW_UNDERSIZED_CFG, /*create_expected=*/false);
    /* Suitably aligned storage that ends immediately after struct_size. */
    uint32_t *raw = (uint32_t *)malloc(sizeof(uint32_t));
    MOQ_TEST_CHECK(raw != NULL);
    if (raw == NULL) return;
    *raw = (uint32_t)sizeof(uint32_t);

    const moq_endpoint_cfg_t *cfg = (const moq_endpoint_cfg_t *)(void *)raw;
    moq_endpoint_t *ep = (moq_endpoint_t *)1;

    unsigned fac_before = g_fac.created, fac_att = g_fac.create_attempts;
    unsigned mv_before = g_mvfst_creates;
    unsigned cbs = deleg_calls, paths = path_calls;
    size_t allocs = g_alloc.total_allocs;

    /* the HIDDEN route, with a non-NULL callback: the trust fields it would
     * otherwise read are past the end of this object */
    moq_result_t rc = moq_endpoint_connect_with_trust(cfg, probe_verify, NULL, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(ep == NULL);
    MOQ_TEST_CHECK_EQ_U64(g_fac.created - fac_before, 0);
    MOQ_TEST_CHECK_EQ_U64(g_fac.create_attempts - fac_att, 0);
    MOQ_TEST_CHECK_EQ_U64(g_mvfst_creates - mv_before, 0);
    MOQ_TEST_CHECK_EQ_U64(deleg_calls - cbs, 0);
    MOQ_TEST_CHECK_EQ_U64(path_calls - paths, 0);
    MOQ_TEST_CHECK_EQ_U64(g_alloc.total_allocs - allocs, 0);

    /* the PUBLIC entry is a parity control: it refuses identically. It does not
     * read a trust field, so the same mutation is not claimed to make it do so. */
    ep = (moq_endpoint_t *)1;
    rc = moq_endpoint_connect(cfg, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(ep == NULL);
    MOQ_TEST_CHECK_EQ_U64(g_fac.created - fac_before, 0);
    MOQ_TEST_CHECK_EQ_U64(g_fac.create_attempts - fac_att, 0);
    MOQ_TEST_CHECK_EQ_U64(g_mvfst_creates - mv_before, 0);
    MOQ_TEST_CHECK_EQ_U64(deleg_calls - cbs, 0);
    MOQ_TEST_CHECK_EQ_U64(path_calls - paths, 0);
    MOQ_TEST_CHECK_EQ_U64(g_alloc.total_allocs - allocs, 0);
    free(raw);
    row_close_common();
}

static void oracle_unsupported_provider(void)
{
#if !defined(MOQ_TEST_VERIFY_BACKEND_MBEDTLS)
    return;   /* the configured provider is expected to support delegation */
#else
    /* an observation, not a selector: the branch was chosen by the configured
     * backend, so a wrong answer here fails this row rather than redirecting
     * execution into the other one */
    MOQ_TEST_CHECK_EQ_INT(moq_picoquic_cert_verify_callback_supported(), 0);

    moq_endpoint_cfg_t cfg; moq_endpoint_t *ep = (moq_endpoint_t *)1;
    trust_probe_t probe = {0, MOQ_ENDPOINT_TRUST_ACCEPT, NULL, false};
    probe.identity = &probe;
    base_cfg(&cfg);
    row_begin(ROW_UNSUPPORTED_PROVIDER, /*create_expected=*/false);
    moq_result_t rc =
        moq_endpoint_connect_with_trust(&cfg, probe_verify, &probe, &ep);
    /* an unusable delegate is refused by name, never ignored */
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK(ep == NULL);
    /* and nothing was installed on the way to saying so */
    MOQ_TEST_CHECK_EQ_U64(deleg_calls, 0);
    MOQ_TEST_CHECK_EQ_U64(path_calls, 0);
    MOQ_TEST_CHECK_EQ_U64(probe.calls, 0);
    /* refused before any facade was selected */
    row_end_no_endpoint();
#endif
}

/* -- ORACLE 3: NULL-delegate parity at every accepted struct_size --------- */
static void oracle_null_parity(void)
{
    bool any_size_ran = false;
    bool non_vacuous = false;

    for (size_t sz = MOQ_ENDPOINT_CFG_V0_SIZE;
         sz <= sizeof(moq_endpoint_cfg_t); sz++) {
        moq_endpoint_cfg_t a, b;
        moq_endpoint_cfg_init_sized(&a, sz);
        moq_endpoint_cfg_init_sized(&b, sz);
        if (a.struct_size != (uint32_t)sz) continue;   /* not an accepted size */
        a.url = B(g_url); a.protocol = g_protocol; add_path_ca(&a);
        b.url = B(g_url); b.protocol = g_protocol; add_path_ca(&b);
        any_size_ran = true;

        moq_endpoint_t *ep_a = (moq_endpoint_t *)1;
        row_begin(ROW_PARITY_WITH_TRUST_NULL, /*create_expected=*/true);
        moq_result_t with_null =
            moq_endpoint_connect_with_trust(&a, NULL, NULL, &ep_a);
        unsigned deleg_a = deleg_calls, path_a = path_calls;
        if (ep_a != NULL) destroy_ep(ep_a); else row_end_no_endpoint();

        moq_endpoint_t *ep_b = (moq_endpoint_t *)1;
        row_begin(ROW_PARITY_PLAIN_CONNECT, /*create_expected=*/true);
        moq_result_t plain = moq_endpoint_connect(&b, &ep_b);
        unsigned deleg_b = deleg_calls, path_b = path_calls;
        /* the baseline really did route through the requested facade */
        if (ep_b != NULL) destroy_ep(ep_b); else row_end_no_endpoint();

        /* NULL trust must reproduce moq_endpoint_connect() exactly, at this
         * size, in result, endpoint existence, and which verifier was chosen */
        MOQ_TEST_CHECK_EQ_INT((int)with_null, (int)plain);
        MOQ_TEST_CHECK((ep_a != NULL) == (ep_b != NULL));
        MOQ_TEST_CHECK_EQ_U64(deleg_a, deleg_b);
        MOQ_TEST_CHECK_EQ_U64(path_a, path_b);

        /* non-vacuity: the BASELINE connect must actually succeed and must not
         * itself be an unrelated refusal, so the parity above
         * cannot be satisfied by two identical refusals */
        if (plain == MOQ_OK && plain != MOQ_ERR_UNSUPPORTED && ep_b != NULL &&
            path_b == 1)
            non_vacuous = true;
    }

    /* at least one accepted size, and at least one of them proved non-vacuous */
    MOQ_TEST_CHECK(any_size_ran);
    MOQ_TEST_CHECK(non_vacuous);
}

/* -- ORACLE 6: a NULL delegate selects the path verifier, once ------------ */
static void oracle_null_selects_path_verifier(void)
{
    moq_endpoint_cfg_t cfg; moq_endpoint_t *ep = NULL;
    base_cfg(&cfg);
    add_path_ca(&cfg);
    row_begin(ROW_NULL_SELECTS_PATH_VERIFIER, /*create_expected=*/true);
    moq_result_t rc = moq_endpoint_connect_with_trust(&cfg, NULL, NULL, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    /* exactly one path-verifier install, and never the delegated setter */
    MOQ_TEST_CHECK_EQ_U64(path_calls, 1);
    MOQ_TEST_CHECK_EQ_U64(deleg_calls, 0);
    /* and the REQUESTED protocol selected its own facade, once, exclusively */
    /* the configured CA reaches the path verifier verbatim: the default-store
     * form where the provider has an implicit store, the explicit file where
     * it does not */
#if defined(MOQ_TEST_VERIFY_BACKEND_MBEDTLS)
    MOQ_TEST_CHECK(!path_last_ca_null &&
                   strcmp(path_last_ca, MOQ_TEST_CA_PEM) == 0);
#else
    MOQ_TEST_CHECK(path_last_ca_null || path_last_ca[0] == '\0');
#endif
    if (ep != NULL) destroy_ep(ep); else row_end_no_endpoint();
}

/* -- ORACLE 5: a supported provider selects the DELEGATED verifier -------- */
static void oracle_delegated_selection(void)
{
#if !defined(MOQ_TEST_VERIFY_BACKEND_OPENSSL)
    return;   /* the configured provider cannot carry a delegate */
#else
    /* an observation, not a selector */
    MOQ_TEST_CHECK_EQ_INT(moq_picoquic_cert_verify_callback_supported(), 1);

    moq_endpoint_cfg_t cfg; moq_endpoint_t *ep = NULL;
    trust_probe_t probe = {0, MOQ_ENDPOINT_TRUST_ACCEPT, NULL, false};
    probe.identity = &probe;
    base_cfg(&cfg);
    row_begin(ROW_DELEGATED_SELECTION, /*create_expected=*/true);

    moq_result_t rc =
        moq_endpoint_connect_with_trust(&cfg, probe_verify, &probe, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(ep != NULL);
    /* exactly one delegated install and zero path-verifier installs */
    MOQ_TEST_CHECK_EQ_U64(deleg_calls, 1);
    MOQ_TEST_CHECK_EQ_U64(path_calls, 0);
    MOQ_TEST_CHECK(deleg_installed_fn != NULL);
    /* the installed context is the endpoint, so the bridge can find the
     * caller's delegate and ctx without a second lifetime to keep alive */
    MOQ_TEST_CHECK(ep != NULL && deleg_installed_ctx == ep);

    if (deleg_installed_fn != NULL) {
        const moq_bytes_t chain[] = {
            {k_leaf, sizeof(k_leaf)},
            {k_issuer, sizeof(k_issuer)}
        };
        moq_bytes_t nm = {k_name, sizeof(k_name) - 1};

        probe.verdict = MOQ_ENDPOINT_TRUST_ACCEPT;
        moq_picoquic_cert_verdict_t v =
            deleg_installed_fn(deleg_installed_ctx, nm, chain, 2);
        /* only the declared ACCEPT translates to TRUST */
        MOQ_TEST_CHECK_EQ_INT((int)v, (int)MOQ_PICOQUIC_CERT_TRUST);
        MOQ_TEST_CHECK_EQ_U64(probe.calls, 1);
        /* the delegate saw the borrowed name and leaf-first chain unchanged */
        MOQ_TEST_CHECK(probe.saw_expected_input);

        probe.verdict = MOQ_ENDPOINT_TRUST_REJECT;
        v = deleg_installed_fn(deleg_installed_ctx, nm, chain, 2);
        MOQ_TEST_CHECK_EQ_INT((int)v, (int)MOQ_PICOQUIC_CERT_REJECT);
        MOQ_TEST_CHECK_EQ_U64(probe.calls, 2);

        /* an unrecognised verdict fails closed rather than being treated as
         * "not REJECT" */
        probe.verdict = (moq_endpoint_trust_verdict_t)99;
        v = deleg_installed_fn(deleg_installed_ctx, nm, chain, 2);
        MOQ_TEST_CHECK_EQ_INT((int)v, (int)MOQ_PICOQUIC_CERT_REJECT);
        MOQ_TEST_CHECK_EQ_U64(probe.calls, 3);
    }

    if (ep != NULL) destroy_ep(ep); else row_end_no_endpoint();
#endif
}

/* -- ORACLE 4: a delegated install that FAILS leaves nothing behind ------- *
 * Cleanup is OBSERVED, not inferred: the fake facade counts create/stop/
 * destroy and the counting allocator accounts for every endpoint-owned byte,
 * so this row states where the failure happened and what was released. */
static void oracle_install_failure(void)
{
#if !defined(MOQ_TEST_VERIFY_BACKEND_OPENSSL)
    return;   /* the configured provider never reaches a delegated install */
#else
    moq_endpoint_cfg_t cfg; moq_endpoint_t *ep = (moq_endpoint_t *)1;
    trust_probe_t probe = {0, MOQ_ENDPOINT_TRUST_ACCEPT, NULL, false};
    probe.identity = &probe;
    base_cfg(&cfg);
    row_begin(ROW_INSTALL_FAILURE, /*create_expected=*/true);
    deleg_result = -1;

    moq_result_t rc =
        moq_endpoint_connect_with_trust(&cfg, probe_verify, &probe, &ep);
    /* the failure is reported, never absorbed into a working endpoint whose
     * requested policy was not installed */
    MOQ_TEST_CHECK(rc < 0);
    MOQ_TEST_CHECK(ep == NULL);
    /* the failure happened INSIDE create/configure, which ran exactly once */
    MOQ_TEST_CHECK_EQ_U64(g_fac.configure_calls, 1);
    /* the requested facade's create was the one entered */
    /* exactly one install attempt, and no fallback to the path verifier */
    MOQ_TEST_CHECK_EQ_U64(deleg_calls, 1);
    MOQ_TEST_CHECK_EQ_U64(path_calls, 0);
    /* the configure result is what failed the create, so no facade was ever
     * transferred to the endpoint -- and therefore none may be destroyed */
    MOQ_TEST_CHECK(g_fac.configure_rc != 0);
    MOQ_TEST_CHECK_EQ_U64(g_fac.created, 0);
    MOQ_TEST_CHECK_EQ_U64(g_fac.stopped, 0);
    MOQ_TEST_CHECK_EQ_U64(g_fac.destroyed, 0);
    /* and every endpoint-owned allocation was returned */
    row_end_no_endpoint();
    deleg_installed_fn = NULL;
    deleg_installed_ctx = NULL;

    /* Repeatability control only -- NOT the cleanup oracle above. */
    ep = (moq_endpoint_t *)1;
    row_begin(ROW_INSTALL_FAILURE_REPEAT, /*create_expected=*/true);
    deleg_result = -1;
    moq_result_t rc2 =
        moq_endpoint_connect_with_trust(&cfg, probe_verify, &probe, &ep);
    MOQ_TEST_CHECK_EQ_INT((int)rc2, (int)rc);
    MOQ_TEST_CHECK(ep == NULL);
    MOQ_TEST_CHECK_EQ_U64(deleg_calls, 1);
    MOQ_TEST_CHECK_EQ_U64(path_calls, 0);
    row_end_no_endpoint();
    deleg_installed_fn = NULL;
    deleg_installed_ctx = NULL;
#endif
}

/* -- ORACLE 4b: a facade create that fails releases the endpoint --------- *
 * The complementary ownership case: the create fails BEFORE any facade is
 * transferred, so the endpoint must be released and nothing destroyed. This
 * row is provider-independent and needs no delegate. */
static void oracle_create_failure(void)
{
    moq_endpoint_cfg_t cfg; moq_endpoint_t *ep = (moq_endpoint_t *)1;
    base_cfg(&cfg);
    add_path_ca(&cfg);
    row_begin(ROW_CREATE_FAILURE, /*create_expected=*/true);
    g_fake_create_fail = true;

    moq_result_t rc = moq_endpoint_connect(&cfg, &ep);
    MOQ_TEST_CHECK(rc < 0);
    MOQ_TEST_CHECK(ep == NULL);
    /* the create was attempted on the requested facade and refused before
     * configure ran */
    MOQ_TEST_CHECK_EQ_U64(g_fac.configure_calls, 0);
    /* a facade that was never transferred is not destroyed by the endpoint */
    MOQ_TEST_CHECK_EQ_U64(g_fac.created, 0);
    MOQ_TEST_CHECK_EQ_U64(g_fac.destroyed, 0);
    row_end_no_endpoint();
}

/* -- real-provider SMOKE, on a REAL QUIC context -------------------------- *
 * These call the shipped helpers on a genuine picoquic context, so they judge
 * the product rather than a recorder. They are a SUCCESS smoke only:
 * transactional identity and lifetime -- which verifier survives a rejected
 * replacement, and what happens to the candidate -- are proven directly by the
 * focused wrapper test (adapters/picoquic/tests/test_verify_callback.c), which
 * can see the installed verifier and disposer. Nothing here infers prior state
 * from a later install succeeding.
 *
 * The provider is the CONFIGURED one; a runtime probe cannot select which
 * branch runs, because that probe is itself under test. */

static picoquic_quic_t *make_quic(void)
{
    return picoquic_create(4, NULL, NULL, NULL, "moq-00", NULL, NULL, NULL,
                           NULL, NULL, 0, NULL, NULL, NULL, 0);
}

/* A callback that must never run: it exists so the unsupported-provider call
 * below is made with valid arguments, and so an invalid-argument path can
 * never explain the refusal. */
static moq_picoquic_cert_verdict_t smoke_never_called(
    void *ctx, moq_bytes_t name, const moq_bytes_t *certs, size_t n)
{
    (void)ctx; (void)name; (void)certs; (void)n;
    MOQ_TEST_CHECK(false);
    return MOQ_PICOQUIC_CERT_REJECT;
}

static void provider_contract(void)
{
#if defined(MOQ_TEST_VERIFY_BACKEND_OPENSSL)
    /* this provider has an implicit system store, so a NULL CA is admissible */
    MOQ_TEST_CHECK_EQ_INT(moq_picoquic_ca_file_loadable(NULL), 1);
    MOQ_TEST_CHECK_EQ_INT(moq_picoquic_ca_file_loadable(MOQ_TEST_CA_PEM), 1);
    /* an unloadable explicit CA is rejected BEFORE any installation */
    MOQ_TEST_CHECK_EQ_INT(moq_picoquic_ca_file_loadable("/nonexistent/ca.pem"), 0);

    {
        picoquic_quic_t *q = make_quic();
        MOQ_TEST_CHECK(q != NULL);
        if (q != NULL) {
            /* default roots install on a real context */
            MOQ_TEST_CHECK_EQ_INT(moq_picoquic_set_cert_verifier(q, NULL), 0);
            picoquic_free(q);
        }
        q = make_quic();
        MOQ_TEST_CHECK(q != NULL);
        if (q != NULL) {
            /* a valid explicit CA installs */
            MOQ_TEST_CHECK_EQ_INT(
                moq_picoquic_set_cert_verifier(q, MOQ_TEST_CA_PEM), 0);
            /* an invalid explicit CA is refused. No claim is made here about
             * what survived: see the focused wrapper test. */
            MOQ_TEST_CHECK(
                moq_picoquic_set_cert_verifier(q, "/nonexistent/ca.pem") != 0);
            picoquic_free(q);
        }
    }
#elif defined(MOQ_TEST_VERIFY_BACKEND_MBEDTLS)
    /* no implicit system store: a NULL or empty CA fails closed */
    MOQ_TEST_CHECK_EQ_INT(moq_picoquic_ca_file_loadable(NULL), 0);
    MOQ_TEST_CHECK_EQ_INT(moq_picoquic_ca_file_loadable(""), 0);
    MOQ_TEST_CHECK_EQ_INT(moq_picoquic_ca_file_loadable(MOQ_TEST_CA_PEM), 1);

    {
        picoquic_quic_t *q = make_quic();
        MOQ_TEST_CHECK(q != NULL);
        if (q != NULL) {
            /* a valid explicit CA installs */
            MOQ_TEST_CHECK_EQ_INT(
                moq_picoquic_set_cert_verifier(q, MOQ_TEST_CA_PEM), 0);
            picoquic_free(q);
        }
        q = make_quic();
        MOQ_TEST_CHECK(q != NULL);
        if (q != NULL) {
            /* a NULL CA fails closed rather than silently trusting */
            MOQ_TEST_CHECK(moq_picoquic_set_cert_verifier(q, NULL) != 0);
            picoquic_free(q);
        }
        q = make_quic();
        MOQ_TEST_CHECK(q != NULL);
        if (q != NULL) {
            int marker = 0;
            /* callback trust is unsupported. Called with a VALID callback and
             * a non-NULL context, so invalid arguments cannot explain the
             * refusal. Whether anything was replaced is proven directly by the
             * focused wrapper test, not inferred here. */
            MOQ_TEST_CHECK_EQ_INT(
                moq_picoquic_cert_verify_callback_supported(), 0);
            MOQ_TEST_CHECK_EQ_INT(
                moq_picoquic_set_cert_verify_callback(q, smoke_never_called,
                                                      &marker), -1);
            picoquic_free(q);
        }
    }
#else
#error "test_endpoint_trust_route needs exactly one configured verify backend"
#endif

    /* both delegated symbols are DEFINED in every provider build: calling them
     * at all is the evidence. A provider-conditional declaration would not
     * have linked. */
    MOQ_TEST_CHECK(moq_picoquic_cert_verify_callback_supported() == 0 ||
                   moq_picoquic_cert_verify_callback_supported() == 1);
    MOQ_TEST_CHECK_EQ_INT(
        moq_picoquic_set_cert_verify_callback(NULL, NULL, NULL), -1);
}

/* How many row sets each compiled protocol actually ran. The expected set is
 * derived from the compile-time facade defines, so a build that contains a
 * facade must run its rows exactly once -- not "at least one protocol ran". */
static unsigned g_protocol_runs[FAKE_KIND_COUNT];

static void run_protocol(const char *name, const char *url,
                         moq_transport_protocol_t protocol)
{
    printf("PROTOCOL: %s (%s)\n", name, url);
    g_protocol_runs[protocol == MOQ_TRANSPORT_PROTOCOL_RAW_QUIC
                        ? FAKE_RAW : FAKE_WT]++;
    g_url = url;
    g_protocol = protocol;
    oracle_conflicts();
    oracle_undersized_cfg();
    oracle_service_stateless_ctx();
#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
    oracle_delegate_wrong_backend();
#endif
    oracle_unsupported_provider();
    oracle_null_parity();
    oracle_null_selects_path_verifier();
    oracle_delegated_selection();
    oracle_install_failure();
    oracle_create_failure();
}

int main(void)
{
    g_cfg_alloc = counting_alloc();
    provider_contract();
#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
    run_protocol("RAW_QUIC", FAKE_URL_QUIC, MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
#endif
#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
    run_protocol("WEBTRANSPORT", FAKE_URL_WT, MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);
#endif
#if !defined(MOQ_SERVICE_HAVE_PQ_THREADED) && \
    !defined(MOQ_SERVICE_HAVE_PICO_WT_MANAGED)
#error "test_endpoint_trust_route requires a picoquic service facade"
#endif
    /* Exactly the compiled protocols ran, each exactly once. */
    unsigned expect_runs[FAKE_KIND_COUNT] = {0, 0};
#ifdef MOQ_SERVICE_HAVE_PQ_THREADED
    expect_runs[FAKE_RAW] = 1;
#endif
#ifdef MOQ_SERVICE_HAVE_PICO_WT_MANAGED
    expect_runs[FAKE_WT] = 1;
#endif
    for (int k = 0; k < FAKE_KIND_COUNT; k++) {
        if (g_protocol_runs[k] != expect_runs[k])
            fprintf(stderr, "PROTOCOL COVERAGE: %s ran x%u, expected x%u\n",
                    k_fake_kind_name[k], g_protocol_runs[k], expect_runs[k]);
        MOQ_TEST_CHECK_EQ_U64(g_protocol_runs[k], expect_runs[k]);
    }

    /* The exact scenario ledger. The expected multiplicity is derived
     * INDEPENDENTLY of the loops that produce it: from the compiled facade
     * set, the configured provider, and the accepted struct_size count (every
     * size from the v0 floor through the current size is accepted, because
     * moq_endpoint_cfg_init_sized() stamps whatever it is given). A missing,
     * duplicated, or foreign scenario is named. */
    const unsigned accepted_sizes =
        (unsigned)(sizeof(moq_endpoint_cfg_t) - MOQ_ENDPOINT_CFG_V0_SIZE + 1);
    unsigned expect_scn[ROW_ID_COUNT] = {0};
    expect_scn[ROW_CONFLICT_INSECURE] = 1;
    expect_scn[ROW_CONFLICT_CA_FILE] = 1;
    expect_scn[ROW_PARITY_WITH_TRUST_NULL] = accepted_sizes;
    expect_scn[ROW_PARITY_PLAIN_CONNECT] = accepted_sizes;
    expect_scn[ROW_NULL_SELECTS_PATH_VERIFIER] = 1;
    expect_scn[ROW_CREATE_FAILURE] = 1;
    expect_scn[ROW_UNDERSIZED_CFG] = 1;
#if defined(MOQ_TEST_VERIFY_BACKEND_MBEDTLS)
    expect_scn[ROW_UNSUPPORTED_PROVIDER] = 1;
#else
    expect_scn[ROW_DELEGATED_SELECTION] = 1;
    expect_scn[ROW_INSTALL_FAILURE] = 1;
    expect_scn[ROW_INSTALL_FAILURE_REPEAT] = 1;
#endif
    /* Rows that need exactly one representative run land on the first compiled
     * picoquic facade. The expected identity is derived the SAME way the rows
     * select it -- from the compiled facade set -- so a pico-WT-only build
     * expects them on WT rather than expecting zero. */
    const fake_kind_t k_first = first_picoquic_kind();
    unsigned expect_first_only[ROW_ID_COUNT] = {0};
#if !defined(MOQ_TEST_VERIFY_BACKEND_MBEDTLS)
    expect_first_only[ROW_SERVICE_STATELESS_CTX] = 1;
#endif
#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
    expect_first_only[ROW_DELEGATE_WRONG_BACKEND] = 1;
#endif
    for (int k = 0; k < FAKE_KIND_COUNT; k++) {
        for (int r = 0; r < ROW_ID_COUNT; r++) {
            unsigned want = expect_runs[k]
                                ? expect_scn[r] +
                                      (k == (int)k_first ? expect_first_only[r]
                                                         : 0u)
                                : 0u;
            if (g_scenario_runs[k][r] != want)
                fprintf(stderr,
                        "SCENARIO LEDGER: %s/%s ran x%u, expected x%u\n",
                        k_fake_kind_name[k], k_row_name[r],
                        g_scenario_runs[k][r], want);
            MOQ_TEST_CHECK_EQ_U64(g_scenario_runs[k][r], want);
        }
    }

    /* no row may still be open at exit */
    MOQ_TEST_CHECK(!g_row_active);
    if (g_row_active)
        fprintf(stderr, "ROW LEAK AT EXIT: '%s'\n", k_row_name[g_row_id]);
    if (g_fake_quic != NULL) picoquic_free(g_fake_quic);
    /* nothing endpoint-owned survives the whole run */
    MOQ_TEST_CHECK_EQ_U64(g_alloc.live_blocks, 0);
    MOQ_TEST_CHECK_EQ_U64(g_alloc.live_bytes, 0);
    MOQ_TEST_PASS("endpoint_trust_route");
    return failures != 0;
}
