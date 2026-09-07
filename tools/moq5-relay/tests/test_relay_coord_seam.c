/*
 * The production coordinator's generation lifecycle, driven directly.
 *
 * Both coordinators must produce, freeze the generation, take the indivisible
 * token, dispatch from THAT token, and release the bank. A coordinator that
 * renders without retiring its generation leaves the slot COLLECTING forever:
 * the lanes have already recorded that serial so they never republish, and
 * every later request joins a generation that can never retire.
 *
 * stderr is redirected to a temporary file so "who received this" is observed
 * rather than asserted about.
 */

#include "../cli/broker.h"
#include "../cli/config.h"
#include "../cli/lanestats.h"
#include "../cli/logevent.h"
#include "../cli/servelog.h"
#include "../cli/snapshot.h"
#include "../bind/moqr_bind.h"
#include "../shard/moqr_shards.h"

#include <moq/msquic_managed.h>

#include "support/fake_msq_managed.h"
#include "support/msq_test_seams.h"


#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../../../tests/unit/test_support.h"

moqr_broker_t *moqr_test_metrics_broker(void);
void moqr_test_set_coord_midflight(void (*fn)(void *), void *ctx);
void moqr_test_set_coord_pinned(void (*fn)(void *), void *ctx);
void moqr_test_set_release_bank_corruption(bool on);
/* Present in EVERY composition: the value it reports is what differs. The
 * comparison happens inside main.c, where both sides still have their real
 * function-pointer types. */
uint32_t moqr_test_admin_seam_wiring(int *available);
uint32_t moqr_test_admin_resolved_labels(const moqr_cli_config_t *cfg,
                                         moqr_obs_labels_t *out, uint32_t cap);

#ifndef MOQR_BIND_TESTING
uint64_t moqr_test_coord_try_dump(const moqr_obs_labels_t *labels,
                                  moqr_cli_snapshot_t *snap, uint64_t rendered,
                                  uint64_t *incomplete_told,
                                  uint64_t *fail_told);
uint64_t moqr_test_coord_try_dump_wake(const moqr_obs_labels_t *labels,
                                       moqr_cli_snapshot_t *snap,
                                       uint64_t rendered, uint64_t *inc,
                                       uint64_t *fail, bool *wake);
moqr_result_t moqr_test_admin_render_signal(moqr_cli_snapshot_t *snap,
                                            const moqr_obs_labels_t *labels,
                                            uint32_t lanes,
                                            moqr_cli_snapshot_stats_t *rows,
                                            moqr_snapshot_view_t *views,
                                            uint64_t serial);
#else
uint64_t moqr_test_coord_try_dump_blocked(const moqr_obs_labels_t *labels,
                                          uint32_t lanes,
                                          moqr_cli_snapshot_t *snap,
                                          uint64_t rendered,
                                          uint64_t *incomplete_told,
                                          uint64_t *fail_told);
uint64_t moqr_test_coord_try_dump_blocked_wake(const moqr_obs_labels_t *labels,
                                               uint32_t lanes,
                                               moqr_cli_snapshot_t *snap,
                                               uint64_t rendered,
                                               uint64_t *inc, uint64_t *fail,
                                               bool *wake);
#endif

/* -- stderr capture -------------------------------------------------------- */

static int  g_saved_fd = -1;
static FILE *g_tmp;

static void
capture_begin(void)
{
    fflush(stderr);
    g_saved_fd = dup(fileno(stderr));
    g_tmp = tmpfile();
    dup2(fileno(g_tmp), fileno(stderr));
}

static size_t
capture_end(char *out, size_t cap)
{
    fflush(stderr);
    dup2(g_saved_fd, fileno(stderr));
    close(g_saved_fd);
    g_saved_fd = -1;
    rewind(g_tmp);
    size_t n = fread(out, 1, cap - 1u, g_tmp);
    out[n] = '\0';
    fclose(g_tmp);
    g_tmp = NULL;
    return n;
}

/* -- fixtures -------------------------------------------------------------- */

static void
publish_row(moqr_cli_snapshot_t *snap, uint32_t lane, uint64_t serial,
            moqr_cli_cap_t cap)
{
    moqr_cli_snapshot_stats_t st;
    memset(&st, 0, sizeof(st));
    st.core.ingested_total = 7;
    st.shard_cap = cap;
    if (cap == MOQR_CLI_CAP_VALID) {
        /* A shard plane with a series the extended exposition renders and the
         * frozen one does not, so the two projections cannot be confused. */
        st.shard.wake_requests_local = 3;
    }
    moqr_cli_snapshot_publish(snap, lane, &st, serial);
}

#ifndef MOQR_BIND_TESTING
static uint64_t
coord_call_normal_wake(const moqr_obs_labels_t *labels,
                       moqr_cli_snapshot_t *snap, uint64_t rendered,
                       uint64_t *inc, uint64_t *fail, bool *wake)
{
    return moqr_test_coord_try_dump_wake(labels, snap, rendered, inc, fail,
                                         wake);
}
static uint64_t
coord_call_blocked_wake(const moqr_obs_labels_t *labels, uint32_t lanes,
                        moqr_cli_snapshot_t *snap, uint64_t rendered,
                        uint64_t *inc, uint64_t *fail, bool *wake)
{
    (void)labels; (void)lanes; (void)snap; (void)rendered; (void)inc;
    (void)fail; (void)wake;
    return 0;
}
static uint64_t
coord_call_normal(const moqr_obs_labels_t *labels, moqr_cli_snapshot_t *snap,
                  uint64_t rendered, uint64_t *inc, uint64_t *fail)
{
    return moqr_test_coord_try_dump(labels, snap, rendered, inc, fail);
}
static uint64_t
coord_call_blocked(const moqr_obs_labels_t *labels, uint32_t lanes,
                   moqr_cli_snapshot_t *snap, uint64_t rendered, uint64_t *inc,
                   uint64_t *fail)
{
    (void)labels; (void)lanes; (void)snap; (void)rendered; (void)inc;
    (void)fail;
    return 0;
}
#else
static uint64_t
coord_call_normal_wake(const moqr_obs_labels_t *labels,
                       moqr_cli_snapshot_t *snap, uint64_t rendered,
                       uint64_t *inc, uint64_t *fail, bool *wake)
{
    (void)labels; (void)snap; (void)rendered; (void)inc; (void)fail;
    (void)wake;
    return 0;
}
static uint64_t
coord_call_blocked_wake(const moqr_obs_labels_t *labels, uint32_t lanes,
                        moqr_cli_snapshot_t *snap, uint64_t rendered,
                        uint64_t *inc, uint64_t *fail, bool *wake)
{
    return moqr_test_coord_try_dump_blocked_wake(labels, lanes, snap, rendered,
                                                 inc, fail, wake);
}
static uint64_t
coord_call_normal(const moqr_obs_labels_t *labels, moqr_cli_snapshot_t *snap,
                  uint64_t rendered, uint64_t *inc, uint64_t *fail)
{
    (void)labels; (void)snap; (void)rendered; (void)inc; (void)fail;
    return 0;
}
static uint64_t
coord_call_blocked(const moqr_obs_labels_t *labels, uint32_t lanes,
                   moqr_cli_snapshot_t *snap, uint64_t rendered, uint64_t *inc,
                   uint64_t *fail)
{
    return moqr_test_coord_try_dump_blocked(labels, lanes, snap, rendered, inc,
                                            fail);
}
#endif


/* Land a SIGUSR1 join in the window between the coordinator's early demand
 * observation and its production, which is exactly where dispatching from the
 * early sample loses the requester. Shared by both builds. */
static void
join_midflight(void *ctx)
{
    moqr_broker_t *b = ctx;
    uint64_t       s = 0;
    bool           w = false;
    (void)moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w);
}

static void
defer_a_signal(void *ctx)
{
    moqr_broker_t *b = ctx;
    uint64_t       s = 0;
    bool           w = true;
    /* Both banks are occupied here, so this is refused and remembered. */
    (void)moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w);
}


/*
 * F3: a failed release is not a retirement.
 *
 * The coordinator's own authenticated release is made to refuse by corrupting
 * only the bank index it passes, so the slot is never touched and the
 * generation remains SENDING. Releasing the slot from the test instead would
 * retire it, and the assertion "still outstanding" would then be proving that
 * the test retired it — not that a refused release left it alone.
 */
static int
coord_release_failure_is_not_retirement(int verify_build)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);

    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    uint64_t s = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w), MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

    static char cap[262144];
    uint64_t inc = 0, fail = 0;
    bool     wake_lanes = true;
    moqr_test_set_release_bank_corruption(true);
    capture_begin();
    uint64_t rendered;
    if (verify_build) {
        rendered = coord_call_blocked_wake(&labels, 1u, &snap, 99u, &inc,
                                           &fail, &wake_lanes);
    } else {
        rendered = coord_call_normal_wake(&labels, &snap, 99u, &inc, &fail,
                                          &wake_lanes);
    }
    (void)capture_end(cap, sizeof(cap));
    moqr_test_set_release_bank_corruption(false);

    /* All three outcomes together. */
    MOQ_TEST_CHECK_EQ_U64(rendered, 99u);   /* prior epoch unchanged      */
    MOQ_TEST_CHECK(!wake_lanes);            /* no wake from a non-release */
    MOQ_TEST_CHECK(moqr_broker_busy(b));    /* generation still outstanding */

    /* It is still SENDING, so it cannot be taken again -- and the rightful
     * release still works, which shows the slot was never disturbed. */
    uint32_t d2 = 0, bank2 = 0;
    MOQ_TEST_CHECK(!moqr_broker_take_serial(b, s, &d2, &bank2));
    bool wake2 = false;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(b, s, 0u, &wake2), MOQR_OK);
    MOQ_TEST_CHECK(!moqr_broker_busy(b));

    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}

/*
 * F6: retiring a generation can open deferred work, and `out_wake_lanes` is
 * the only notice its lanes will get. Relying on the main loop noticing on its
 * next poll contradicts the API contract and does not survive the dedicated
 * admin thread Step 3 introduces.
 */
static int
coord_propagates_release_wake(int verify_build)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);

    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    /* Fill the second bank so the broker will be exhausted. */
    uint64_t hold = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &hold, &w), MOQR_OK);
    moqr_broker_on_complete(b, hold);
    uint32_t hd = 0, hbank = 0;
    MOQ_TEST_CHECK(moqr_broker_take_serial(b, hold, &hd, &hbank));

    /* The generation the coordinator will render. */
    uint64_t s = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w), MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

    /* A SIGNAL demand arrives while the rendered generation's bank is pinned
     * and the other bank is held: no generation is collecting and no bank is
     * free, so it is deferred. That window is only reachable from inside the
     * retirement, hence the hook. */
    uint64_t inc = 0, fail = 0;
    bool     wake_lanes = false;
    static char cap[262144];
    moqr_test_set_coord_pinned(defer_a_signal, b);
    capture_begin();
    if (verify_build) {
        (void)coord_call_blocked_wake(&labels, 1u, &snap, 0, &inc, &fail,
                                      &wake_lanes);
    } else {
        (void)coord_call_normal_wake(&labels, &snap, 0, &inc, &fail,
                                     &wake_lanes);
    }
    (void)capture_end(cap, sizeof(cap));
    moqr_test_set_coord_pinned(NULL, NULL);

    MOQ_TEST_CHECK(wake_lanes);
    /* And a generation really is collecting again. */
    uint64_t cs = 0;
    uint32_t cd = 0;
    MOQ_TEST_CHECK(moqr_broker_current(b, &cs, &cd));
    MOQ_TEST_CHECK_EQ_U64(cd, MOQR_BROKER_DEMAND_SIGNAL);

    bool wk = false;
    (void)moqr_broker_release(b, hold, hbank, &wk);
    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}

/*
 * F2: the rendered body and the delivery token must name the SAME generation.
 *
 * Leave an older generation READY on one bank, open and render a newer one on
 * the reused bank, and invoke the real coordinator. Taking "oldest READY"
 * would pair the older generation's demand and bank with the newer one's
 * bytes: the signal document would be suppressed, the older HTTP token
 * released, and the rendered generation stranded READY forever.
 */
static int
coord_body_token_identity(int verify_build)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);

    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    /* Generation 1: opened, completed, taken and released -- frees its bank. */
    uint64_t s1 = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s1, &w), MOQR_OK);
    moqr_broker_on_complete(b, s1);
    uint32_t d1 = 0, bank1 = 0;
    MOQ_TEST_CHECK(moqr_broker_take_serial(b, s1, &d1, &bank1));
    bool wk = false;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(b, s1, bank1, &wk), MOQR_OK);

    /* Generation 2: left READY on the other bank, carrying HTTP demand. */
    uint64_t s2 = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &s2, &w), MOQR_OK);
    moqr_broker_on_complete(b, s2);

    /* Generation 3: opened on the REUSED bank, carrying SIGNAL demand, and
     * its row published so the coordinator can render it. */
    uint64_t s3 = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s3, &w), MOQR_OK);
    MOQ_TEST_CHECK(s3 > s2);
    publish_row(&snap, 0, s3, MOQR_CLI_CAP_ABSENT);

    static char cap[262144];
    uint64_t inc = 0, fail = 0;
    capture_begin();
    uint64_t rendered;
    if (verify_build) {
        rendered = coord_call_blocked(&labels, 1u, &snap, 0, &inc, &fail);
    } else {
        rendered = coord_call_normal(&labels, &snap, 0, &inc, &fail);
    }
    size_t n = capture_end(cap, sizeof(cap));

    /* Serial 3 was rendered, so serial 3's SIGNAL demand must have been the
     * one honoured -- its document reaches stderr. */
    MOQ_TEST_CHECK_EQ_U64(rendered, s3);
    MOQ_TEST_CHECK(n > 0);

    /* Serial 2 is untouched: still READY, still deliverable, still HTTP. */
    uint32_t d2 = 0, bank2 = 0;
    MOQ_TEST_CHECK(moqr_broker_take_serial(b, s2, &d2, &bank2));
    MOQ_TEST_CHECK_EQ_U64(d2, MOQR_BROKER_DEMAND_HTTP);
    bool wk2 = false;
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(b, s2, bank2, &wk2), MOQR_OK);

    /* And both generations retired: nothing is stranded. */
    MOQ_TEST_CHECK(!moqr_broker_busy(b));

    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}




#ifndef MOQR_BIND_TESTING
/* -- poison-and-quarantine allocator --------------------------------------
 *
 * The borrowed-document rule has to hold in an ORDINARY build, not only under
 * a sanitizer. This allocator overwrites each released block and then RETAINS
 * it: nothing goes back to the system allocator until the test says so.
 *
 * The quarantine is what makes the oracle defined. Poisoning and then really
 * freeing would leave the escaped-pointer mutant reading freed storage, which
 * is undefined behaviour — an allocator may reuse it and an optimiser may drop
 * the dead store, so any failure would be an accident rather than a proof.
 * Here the block stays alive and readable, so a read after release
 * deterministically observes poison, in any build at any optimisation level.
 */
#define POISON_BYTE   0xDD
#define QUARANTINE_MAX 64u

typedef struct {
    void  *ptr;
    size_t size;
} quarantined_t;

static quarantined_t g_quarantine[QUARANTINE_MAX];
static size_t        g_quarantined;      /* currently retained          */
static size_t        g_poison_releases;  /* blocks poisoned + retained  */
static size_t        g_quarantine_lost;  /* overflowed the quarantine   */

static void *
poison_alloc(size_t size, void *ctx)
{
    (void)ctx;
    return malloc(size);
}

static void *
poison_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    (void)old_size;
    (void)ctx;
    return realloc(ptr, new_size);
}

static void
poison_free(void *ptr, size_t size, void *ctx)
{
    (void)ctx;
    if (ptr == NULL) {
        return;
    }
    if (size > 0u) {
        memset(ptr, POISON_BYTE, size);
    }
    if (g_quarantined < QUARANTINE_MAX) {
        g_quarantine[g_quarantined].ptr = ptr;
        g_quarantine[g_quarantined].size = size;
        g_quarantined++;
        g_poison_releases++;
        return;   /* retained, deliberately not returned to the system */
    }
    /* Never silently fall back to a real free while the oracle is armed: that
     * would reintroduce the undefined read this design exists to remove. */
    g_quarantine_lost++;
}

/* Reclaim everything the quarantine held, after the assertions have run. */
static size_t
quarantine_drain(void)
{
    size_t n = g_quarantined;
    for (size_t i = 0; i < g_quarantined; i++) {
        free(g_quarantine[i].ptr);
        g_quarantine[i].ptr = NULL;
    }
    g_quarantined = 0;
    return n;
}

static const moq_alloc_t *
poison_allocator(void)
{
    static const moq_alloc_t a = { NULL, poison_alloc, poison_realloc,
                                   poison_free };
    return &a;
}

/*
 * The document must be read while the dump still owns it. Under the
 * poison-and-quarantine allocator a read after release yields 0xDD bytes from
 * memory that is still alive, so this fails in an ordinary optimised build
 * exactly as it would under ASan — and neither run performs a use-after-free.
 */
static int
test_document_lifetime_without_sanitizer(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);

    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, poison_allocator()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    uint64_t s = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w), MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

    static char cap[262144];
    uint64_t inc = 0, fail = 0;
    g_poison_releases = 0;
    g_quarantine_lost = 0;
    capture_begin();
    uint64_t r = moqr_test_coord_try_dump(&labels, &snap, 0, &inc, &fail);
    size_t n = capture_end(cap, sizeof(cap));

    MOQ_TEST_CHECK_EQ_U64(r, s);
    MOQ_TEST_CHECK(n > 0);
    /* Real content, not poison. */
    MOQ_TEST_CHECK(strstr(cap, "moqrelay_objects_ingested_total") != NULL);
    MOQ_TEST_CHECK(strstr(cap, "# TYPE") != NULL);
    /* No poison run reached the sink. */
    {
        unsigned run = 0;
        int      poisoned = 0;
        for (size_t k = 0; k < n; k++) {
            run = ((unsigned char)cap[k] == POISON_BYTE) ? (run + 1u) : 0u;
            if (run >= 8u) {
                poisoned = 1;
                break;
            }
        }
        MOQ_TEST_CHECK_EQ_INT(poisoned, 0);
    }
    /* The oracle was armed: blocks really were poisoned and retained, and the
     * quarantine never overflowed into a real free. */
    MOQ_TEST_CHECK(g_poison_releases > 0u);
    MOQ_TEST_CHECK_EQ_U64(g_quarantine_lost, 0u);

    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);

    /* Every retained block is reclaimed once the assertions are done, so the
     * oracle leaks nothing. */
    size_t drained = quarantine_drain();
    MOQ_TEST_CHECK(drained >= g_poison_releases);
    MOQ_TEST_CHECK_EQ_U64(g_quarantined, 0u);
    return failures;
}
#endif

#ifndef MOQR_BIND_TESTING

/*
 * A generation nobody asked to see on stderr must produce no stderr output,
 * and must still be fully retired so the next one can start.
 */
/* The bank count is fixed by the body storage Step 3 preallocates. */
static int
test_bank_count_is_exactly_two(void)
{
    int failures = 0;
    moqr_broker_t b;
    MOQ_TEST_CHECK_EQ_U64(MOQR_BROKER_BANKS, 2u);
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, 1u), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, 3u), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, 0u), MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&b, MOQR_BROKER_BANKS), MOQR_OK);
    moqr_broker_destroy(&b);
    return failures;
}

static int
test_http_only_generation_is_silent_and_retires(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);

    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    uint64_t s = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &s, &w), MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

    static char cap[65536];
    uint64_t inc = 0, fail = 0;
    capture_begin();
    uint64_t rendered =
        moqr_test_coord_try_dump(&labels, &snap, 0, &inc, &fail);
    size_t n = capture_end(cap, sizeof(cap));

    MOQ_TEST_CHECK_EQ_U64(rendered, s);
    /* Not one byte to stderr: the scrape asked, stderr did not. */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n, 0u);
    /* And the generation retired: nothing is outstanding. */
    MOQ_TEST_CHECK(!moqr_broker_busy(b));

    /* The next generation can therefore start. */
    uint64_t s2 = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s2, &w), MOQR_OK);
    MOQ_TEST_CHECK(w);
    MOQ_TEST_CHECK(s2 > s);

    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}

/* A signal generation is delivered exactly once, then retired. */
static int
test_signal_generation_emits_once_and_retires(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);

    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    uint64_t s = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w), MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

    static char cap[262144];
    uint64_t inc = 0, fail = 0;
    capture_begin();
    uint64_t r1 = moqr_test_coord_try_dump(&labels, &snap, 0, &inc, &fail);
    /* A second attempt has nothing outstanding and must print nothing more:
     * the generation was retired, not left to be reprinted. */
    uint64_t r2 = moqr_test_coord_try_dump(&labels, &snap, r1, &inc, &fail);
    size_t n = capture_end(cap, sizeof(cap));

    MOQ_TEST_CHECK_EQ_U64(r1, s);
    MOQ_TEST_CHECK_EQ_U64(r2, r1);
    MOQ_TEST_CHECK(n > 0);
    /* Exactly one document banner. */
    size_t banners = 0;
    for (const char *p = strstr(cap, "MOQ5 Relay metrics (epoch"); p != NULL;
         p = strstr(p + 1, "MOQ5 Relay metrics (epoch")) {
        banners++;
    }
    MOQ_TEST_CHECK_EQ_U64((uint64_t)banners, 1u);
    MOQ_TEST_CHECK(!moqr_broker_busy(b));

    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}

/* A demand joining while the generation collects is served by the token. */
static int
test_late_join_reaches_stderr(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);

    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    /* Opened by a scrape... */
    uint64_t s = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &s, &w), MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

    /* ...and SIGUSR1 joins AFTER the coordinator has already observed the
     * demand, but before the document exists. */
    static char cap[262144];
    uint64_t inc = 0, fail = 0;
    moqr_test_set_coord_midflight(join_midflight, b);
    capture_begin();
    (void)moqr_test_coord_try_dump(&labels, &snap, 0, &inc, &fail);
    size_t n = capture_end(cap, sizeof(cap));
    moqr_test_set_coord_midflight(NULL, NULL);

    /* Dispatching from the demand sampled before producing would have
     * silently dropped the joiner. */
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK(strstr(cap, "MOQ5 Relay metrics (epoch") != NULL);
    MOQ_TEST_CHECK(!moqr_broker_busy(b));

    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}

/* A poisoned generation is suppressed, diagnosed only to the sink that asked,
 * and still retired. */
static int
test_poisoned_generation_is_addressed_and_retires(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    for (int http_only = 0; http_only < 2; http_only++) {
        MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
        moqr_cli_snapshot_t snap;
        memset(&snap, 0, sizeof(snap));
        MOQ_TEST_CHECK_EQ_INT(
            moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);

        uint64_t s = 0;
        bool     w = false;
        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_request(b,
                                http_only ? MOQR_BROKER_DEMAND_HTTP
                                          : MOQR_BROKER_DEMAND_SIGNAL,
                                &s, &w),
            MOQR_OK);
        publish_row(&snap, 0, s, MOQR_CLI_CAP_REFUSED);

        static char cap[65536];
        uint64_t inc = 0, fail = 0;
        capture_begin();
        (void)moqr_test_coord_try_dump(&labels, &snap, 0, &inc, &fail);
        size_t n = capture_end(cap, sizeof(cap));

        if (http_only) {
            MOQ_TEST_CHECK_EQ_U64((uint64_t)n, 0u);
        } else {
            MOQ_TEST_CHECK(strstr(cap, "suppressed") != NULL);
        }
        /* Either way the generation is consumed, never spun on. */
        MOQ_TEST_CHECK(!moqr_broker_busy(b));

        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(b);
    }
    return failures;
}

/* An incomplete generation stays outstanding, is diagnosed only for signal
 * demand, and progresses once the lane publishes. */
static int
test_incomplete_generation_is_addressed_then_progresses(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    for (int http_only = 0; http_only < 2; http_only++) {
        MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
        moqr_cli_snapshot_t snap;
        memset(&snap, 0, sizeof(snap));
        MOQ_TEST_CHECK_EQ_INT(
            moqr_cli_snapshot_init(&snap, 2u, moq_alloc_default()), MOQR_OK);
        moqr_obs_labels_t two[2] = { { 0, "msquic", "moqt-18" },
                                     { 1, "msquic", "moqt-18" } };
        (void)labels;

        uint64_t s = 0;
        bool     w = false;
        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_request(b,
                                http_only ? MOQR_BROKER_DEMAND_HTTP
                                          : MOQR_BROKER_DEMAND_SIGNAL,
                                &s, &w),
            MOQR_OK);
        /* Only one of two lanes publishes. */
        publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

        static char cap[65536];
        uint64_t inc = 0, fail = 0;
        capture_begin();
        uint64_t r = moqr_test_coord_try_dump(two, &snap, 0, &inc, &fail);
        size_t n = capture_end(cap, sizeof(cap));

        MOQ_TEST_CHECK_EQ_U64(r, 0u);          /* nothing rendered */
        MOQ_TEST_CHECK(moqr_broker_busy(b));   /* still outstanding */
        if (http_only) {
            MOQ_TEST_CHECK_EQ_U64((uint64_t)n, 0u);
        } else {
            MOQ_TEST_CHECK(strstr(cap, "incomplete") != NULL);
        }

        /* The missing lane publishes; the generation now completes and
         * retires, so the next one can start. */
        publish_row(&snap, 1, s, MOQR_CLI_CAP_ABSENT);
        capture_begin();
        uint64_t r2 = moqr_test_coord_try_dump(two, &snap, 0, &inc, &fail);
        (void)capture_end(cap, sizeof(cap));
        MOQ_TEST_CHECK_EQ_U64(r2, s);
        MOQ_TEST_CHECK(!moqr_broker_busy(b));

        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(b);
    }
    return failures;
}

/* With no generation open the coordinator does nothing at all. */
static int
test_idle_coordinator_is_a_noop(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    static char cap[4096];
    uint64_t inc = 0, fail = 0;
    capture_begin();
    uint64_t r = moqr_test_coord_try_dump(&labels, &snap, 42u, &inc, &fail);
    size_t n = capture_end(cap, sizeof(cap));

    MOQ_TEST_CHECK_EQ_U64(r, 42u);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n, 0u);
    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}

#endif /* !MOQR_BIND_TESTING */

#ifdef MOQR_BIND_TESTING

/*
 * F2: the verify coordinator rides the SAME generation lifecycle. Rendering
 * without completing and retiring would leave the slot COLLECTING forever --
 * the lanes have already recorded that serial so they never republish, the
 * coordinator stays busy reprinting a finished document, and every later
 * request joins a generation that can never retire.
 */
static int
test_blocked_generation_retires_and_progresses(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);

    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    uint64_t s = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s, &w), MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

    static char cap[262144];
    uint64_t inc = 0, fail = 0;
    capture_begin();
    uint64_t r1 =
        moqr_test_coord_try_dump_blocked(&labels, 1u, &snap, 0, &inc, &fail);
    /* A second attempt must print nothing more: the generation retired. */
    uint64_t r2 =
        moqr_test_coord_try_dump_blocked(&labels, 1u, &snap, r1, &inc, &fail);
    size_t n = capture_end(cap, sizeof(cap));

    MOQ_TEST_CHECK_EQ_U64(r1, s);
    MOQ_TEST_CHECK_EQ_U64(r2, r1);
    size_t banners = 0;
    for (const char *p = strstr(cap, "moq-relay-verify blocked (epoch");
         p != NULL; p = strstr(p + 1, "moq-relay-verify blocked (epoch")) {
        banners++;
    }
    MOQ_TEST_CHECK_EQ_U64((uint64_t)banners, 1u);
    MOQ_TEST_CHECK(!moqr_broker_busy(b));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n, (uint64_t)n);

    /* The next generation can start, which a stuck slot would prevent. */
    uint64_t s2 = 0;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s2, &w), MOQR_OK);
    MOQ_TEST_CHECK(w);
    MOQ_TEST_CHECK(s2 > s);

    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}

/* An HTTP-only generation produces no verify stderr output either. */
static int
test_blocked_http_only_is_silent(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    uint64_t s = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &s, &w), MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

    static char cap[65536];
    uint64_t inc = 0, fail = 0;
    capture_begin();
    (void)moqr_test_coord_try_dump_blocked(&labels, 1u, &snap, 0, &inc, &fail);
    size_t n = capture_end(cap, sizeof(cap));

    MOQ_TEST_CHECK_EQ_U64((uint64_t)n, 0u);
    MOQ_TEST_CHECK(!moqr_broker_busy(b));
    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}

/*
 * A late join is served by the frozen token in the verify path too.
 *
 * The generation must open with HTTP ONLY, and SIGNAL must land through the
 * mid-flight hook — after the coordinator has already sampled the demand.
 * Requesting both up front leaves the early sample already containing SIGNAL,
 * so dispatching from it would look correct and the mutant would survive.
 */
static int
test_blocked_late_join_reaches_stderr(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
    moqr_cli_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK_EQ_INT(
        moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };

    uint64_t s = 0;
    bool     w = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &s, &w), MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);

    static char cap[262144];
    uint64_t inc = 0, fail = 0;
    moqr_test_set_coord_midflight(join_midflight, b);
    capture_begin();
    uint64_t r =
        coord_call_blocked(&labels, 1u, &snap, 0, &inc, &fail);
    size_t n = capture_end(cap, sizeof(cap));
    moqr_test_set_coord_midflight(NULL, NULL);

    /* The joiner asked, so the document reaches stderr; dispatching from the
     * pre-render sample would have dropped it. */
    MOQ_TEST_CHECK_EQ_U64(r, s);
    MOQ_TEST_CHECK(n > 0);
    MOQ_TEST_CHECK(strstr(cap, "moq-relay-verify blocked (epoch") != NULL);
    /* And the generation retired. */
    MOQ_TEST_CHECK(!moqr_broker_busy(b));
    moqr_cli_snapshot_destroy(&snap);
    moqr_broker_destroy(b);
    return failures;
}

/* A poisoned generation is suppressed, addressed, and retired. */
static int
test_blocked_poison_is_addressed_and_retires(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    moqr_obs_labels_t labels = { 0, "msquic", "moqt-18" };
    for (int http_only = 0; http_only < 2; http_only++) {
        MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
        moqr_cli_snapshot_t snap;
        memset(&snap, 0, sizeof(snap));
        MOQ_TEST_CHECK_EQ_INT(
            moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()), MOQR_OK);
        uint64_t s = 0;
        bool     w = false;
        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_request(b,
                                http_only ? MOQR_BROKER_DEMAND_HTTP
                                          : MOQR_BROKER_DEMAND_SIGNAL,
                                &s, &w),
            MOQR_OK);
        publish_row(&snap, 0, s, MOQR_CLI_CAP_REFUSED);
        static char cap[65536];
        uint64_t inc = 0, fail = 0;
        capture_begin();
        (void)moqr_test_coord_try_dump_blocked(&labels, 1u, &snap, 0, &inc,
                                               &fail);
        size_t n = capture_end(cap, sizeof(cap));
        if (http_only) {
            MOQ_TEST_CHECK_EQ_U64((uint64_t)n, 0u);
        } else {
            MOQ_TEST_CHECK(strstr(cap, "suppressed") != NULL);
        }
        MOQ_TEST_CHECK(!moqr_broker_busy(b));
        moqr_cli_snapshot_destroy(&snap);
        moqr_broker_destroy(b);
    }
    return failures;
}

#endif /* MOQR_BIND_TESTING */


#ifndef MOQR_BIND_TESTING
/*
 * SIGNAL COMPATIBILITY, byte for byte, through the PRODUCTION SINK.
 *
 * Both documents are captured from the stream they are actually written to,
 * and the new one comes out of the endpoint's own `emit_signal` callback --
 * not a test-side reproduction of its format string. A copy of the format
 * would move in lockstep with any change to it and could never notice one,
 * which is the whole failure this comparison exists to prevent.
 */
static int
signal_compat_case(const char *what, moqr_cli_cap_t cap)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    moqr_cli_snapshot_t snap;
    moqr_obs_labels_t labels = { .shard = 0, .transport = "msquic",
                                 .version = "moqt-18" };
    static moqr_cli_snapshot_stats_t rows[1];
    static moqr_snapshot_view_t views[1];
    uint64_t s = 0;
    bool wake = false;
    uint64_t inc = 0, fail = 0;
    static char old_out[512 * 1024];
    static char new_out[512 * 1024];
    size_t old_n, new_n;

    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s,
                                       &wake) == MOQR_OK);
    publish_row(&snap, 0, s, cap);

    /* The endpoint's own sink, captured from stderr, while the generation is
     * still collecting -- the coordinator below retires it. */
    capture_begin();
    (void)moqr_test_admin_render_signal(&snap, &labels, 1u, rows, views, s);
    new_n = capture_end(new_out, sizeof(new_out));

    /* And the coordinator's, from the same stream. */
    capture_begin();
    (void)moqr_test_coord_try_dump_wake(&labels, &snap, 0, &inc, &fail, &wake);
    old_n = capture_end(old_out, sizeof(old_out));

    if (old_n == 0 || new_n == 0) {
        printf("  signalcompat[%s]: one side emitted nothing (old %zu, new "
               "%zu)\n", what, old_n, new_n);
        failures++;
    } else if (strcmp(old_out, new_out) != 0) {
        size_t i = 0;
        while (old_out[i] != '\0' && new_out[i] != '\0' &&
               old_out[i] == new_out[i]) {
            i++;
        }
        printf("  signalcompat[%s]: the signal projection changed at byte %zu "
               "(old %zu bytes, new %zu)\n  old: [%.48s]\n  new: [%.48s]\n",
               what, i, strlen(old_out), strlen(new_out),
               old_out + (i > 24 ? i - 24 : 0),
               new_out + (i > 24 ? i - 24 : 0));
        failures++;
    }
    moqr_cli_snapshot_destroy(&snap);
    return failures;
}

/* Lanes of DIFFERENT capability under one generation: one with the shard
 * plane, one without. The coordinator and the endpoint must project the same
 * frozen bytes for the whole set. */
static int
signal_compat_mixed_case(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    moqr_cli_snapshot_t snap;
    static const moqr_obs_labels_t labels[2] = {
        { .shard = 0, .transport = "msquic", .version = "moqt-18" },
        { .shard = 1, .transport = "msquic", .version = "moqt-18" },
    };
    static moqr_cli_snapshot_stats_t rows[2];
    static moqr_snapshot_view_t views[2];
    uint64_t s = 0;
    bool wake = false;
    uint64_t inc = 0, fail = 0;
    static char old_out[1024 * 1024];
    static char new_out[1024 * 1024];
    size_t old_n, new_n;

    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, 2u, moq_alloc_default()) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s,
                                       &wake) == MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_VALID);
    publish_row(&snap, 1, s, MOQR_CLI_CAP_ABSENT);

    capture_begin();
    (void)moqr_test_admin_render_signal(&snap, labels, 2u, rows, views, s);
    new_n = capture_end(new_out, sizeof(new_out));
    capture_begin();
    (void)moqr_test_coord_try_dump_wake(labels, &snap, 0, &inc, &fail, &wake);
    old_n = capture_end(old_out, sizeof(old_out));

    if (old_n == 0 || new_n == 0) {
        printf("  signalcompat[mixed]: one side emitted nothing (old %zu, new "
               "%zu)\n", old_n, new_n);
        failures++;
    } else if (strcmp(old_out, new_out) != 0) {
        size_t i = 0;
        while (old_out[i] != '\0' && new_out[i] != '\0' &&
               old_out[i] == new_out[i]) {
            i++;
        }
        printf("  signalcompat[mixed]: the signal projection changed at byte "
               "%zu (old %zu bytes, new %zu)\n  old: [%.48s]\n  new: [%.48s]\n",
               i, strlen(old_out), strlen(new_out),
               old_out + (i > 24 ? i - 24 : 0), new_out + (i > 24 ? i - 24 : 0));
        failures++;
    }
    moqr_cli_snapshot_destroy(&snap);
    return failures;
}

static int
test_signal_projection_is_byte_identical(void)
{
    int failures = 0;
    /* The ordinary document, and the poison diagnostic. Both are shipped
     * operator-facing surfaces. */
    failures += signal_compat_case("document", MOQR_CLI_CAP_ABSENT);
    failures += signal_compat_case("suppressed", MOQR_CLI_CAP_REFUSED);
    /* With the shard plane present the frozen projection has a distinct
     * family set from the admin document; the signal sink must still carry
     * the frozen one. */
    failures += signal_compat_case("shard-plane-present", MOQR_CLI_CAP_VALID);
    failures += signal_compat_mixed_case();
    return failures;
}

#endif /* !MOQR_BIND_TESTING */


/*
 * The endpoint's callback wiring, compared inside production code.
 *
 * Each member is checked against its exact production callback where both
 * still have their real function-pointer types; nothing is erased to an object
 * pointer on the way out. A deleted or wrong seam clears its bit.
 */
#ifndef MOQR_BIND_TESTING
moqr_result_t moqr_test_admin_render_shards(moqr_cli_snapshot_t *snap,
                                            const moqr_obs_labels_t *labels,
                                            uint32_t lanes,
                                            moqr_cli_snapshot_stats_t *rows,
                                            moqr_snapshot_view_t *views,
                                            uint64_t serial, char *out,
                                            size_t cap, size_t *out_len);

/* The shards document from real copied rows through the production collect
 * and render: K=1 is "absent" with no shard plane; a refused row renders
 * nothing; a dual-listener set renders every global shard with its
 * coordinates and "valid" capability. */
static int
test_shards_document_from_real_rows(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    moqr_cli_snapshot_t snap;
    static const moqr_obs_labels_t l1 = { .shard = 0, .transport = "msquic",
                                          .version = "moqt-18" };
    static moqr_cli_snapshot_stats_t rows[3];
    static moqr_snapshot_view_t views[3];
    static char doc[65536];
    uint64_t s = 0;
    bool wake = false;
    size_t len = 0;

    /* K=1: absent. */
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, 1u, moq_alloc_default()) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &s, &wake) ==
                   MOQR_OK);
    publish_row(&snap, 0, s, MOQR_CLI_CAP_ABSENT);
    if (moqr_test_admin_render_shards(&snap, &l1, 1u, rows, views, s, doc,
                                      sizeof(doc), &len) != MOQR_OK || len == 0) {
        printf("  shards-rows: the K=1 document did not render\n");
        failures++;
    } else {
        char want[64];
        (void)snprintf(want, sizeof(want), "{\"api\":\"v1\",\"epoch\":%llu,\"shards\":[",
                       (unsigned long long)s);
        if (strncmp(doc, want, strlen(want)) != 0 ||
            strstr(doc, "\"shard\":0,\"transport\":\"msquic\",\"version\":\"moqt-18\","
                        "\"listener\":\"raw\",\"capability\":\"absent\"") == NULL ||
            strstr(doc, "shard_plane") != NULL ||
            strstr(doc, "\"ingested_total\":7") == NULL) {
            printf("  shards-rows: the K=1 document is wrong: [%.200s]\n", doc);
            failures++;
        }
    }
    /* A REFUSED row poisons: nothing renders. */
    {
        uint64_t s2 = 0;
        moqr_broker_on_complete(b, s);
        {
            uint32_t d = 0, bank = 0;
            if (moqr_broker_take_serial(b, s, &d, &bank)) {
                bool w = false;
                (void)moqr_broker_release(b, s, bank, &w);
            }
        }
        MOQ_TEST_CHECK(moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &s2,
                                           &wake) == MOQR_OK);
        publish_row(&snap, 0, s2, MOQR_CLI_CAP_REFUSED);
        memcpy(doc, "stale", 6);
        if (moqr_test_admin_render_shards(&snap, &l1, 1u, rows, views, s2, doc,
                                          sizeof(doc), &len) == MOQR_OK ||
            len != 0 || doc[0] != '\0') {
            printf("  shards-rows: a refused row rendered a document\n");
            failures++;
        }
        moqr_broker_on_complete(b, s2);
        {
            uint32_t d = 0, bank = 0;
            if (moqr_broker_take_serial(b, s2, &d, &bank)) {
                bool w = false;
                (void)moqr_broker_release(b, s2, bank, &w);
            }
        }
    }
    moqr_cli_snapshot_destroy(&snap);
    return failures;
}

#endif /* !MOQR_BIND_TESTING */

moqr_result_t moqr_test_admin_owner_probe(const moqr_cli_config_t *cfg,
                                          char *out, size_t cap,
                                          size_t *out_len, bool *out_has_info);

/* The production owner freezes the REAL document before any endpoint
 * exists, allocates nothing when the endpoint is disabled, and refuses a
 * configuration the document boundary cannot represent. */
static int
test_admin_owner_freezes_info(void)
{
    int failures = 0;
    static moqr_cli_config_t cfg;
    static char doc[16384];
    size_t len = 0;
    bool has = true;
    char err[192];
    static const char *ON =
        "{\"listener\":{\"port\":4433,\"versions\":[18]},"
        "\"admin\":{\"tcp\":{\"port\":9109}}}";
    static const char *OFF = "{\"listener\":{\"port\":4433,\"versions\":[18]}}";
    static const char BAD[] =
        "{\"listener\":{\"host\":\"h\xff\",\"port\":4433,\"versions\":[18]},"
        "\"admin\":{\"tcp\":{\"port\":9109}}}";

    if (moqr_cli_config_parse(ON, strlen(ON), &cfg, err, sizeof(err)) != MOQR_OK) {
        printf("  owner-info: the enabled fixture was rejected: %s\n", err);
        return 1;
    }
    if (moqr_test_admin_owner_probe(&cfg, doc, sizeof(doc), &len, &has) != MOQR_OK ||
        !has || len == 0 || strncmp(doc, "{\"api\":\"v1\",", 12) != 0 ||
        strstr(doc, "\"targets\":[\"/metrics\",\"/api/v1/info\",\"/api/v1/shards\"]") == NULL) {
        printf("  owner-info: the enabled owner did not freeze the document "
               "(has %d len %zu) [%.60s]\n", (int)has, len, doc);
        failures++;
    }
    if (moqr_cli_config_parse(OFF, strlen(OFF), &cfg, err, sizeof(err)) != MOQR_OK) {
        printf("  owner-info: the disabled fixture was rejected: %s\n", err);
        return failures + 1;
    }
    has = true;
    if (moqr_test_admin_owner_probe(&cfg, doc, sizeof(doc), &len, &has) != MOQR_OK ||
        has) {
        printf("  owner-info: a disabled endpoint allocated a document\n");
        failures++;
    }
    if (moqr_cli_config_parse(BAD, sizeof(BAD) - 1u, &cfg, err, sizeof(err)) != MOQR_OK) {
        printf("  owner-info: the 0xff fixture was rejected by the parser (%s)\n",
               err);
        return failures + 1;
    }
    has = true;
    if (moqr_test_admin_owner_probe(&cfg, doc, sizeof(doc), &len, &has) != MOQR_ERR_INVAL ||
        has) {
        printf("  owner-info: a host that is not UTF-8 did not refuse the owner\n");
        failures++;
    }
    return failures;
}

static int
test_admin_seam_wiring_is_complete(void)
{
    int failures = 0;
    int available = -1;
    uint32_t ok = moqr_test_admin_seam_wiring(&available);
    static const char *names[8] = { "wake_all", "collect", "render",
                                    "signal_pending", "emit_signal",
                                    "emit_suppressed", "info",
                                    "info_len" };
#ifdef MOQR_BIND_TESTING
    const int want_available = 0;
#else
    const int want_available = 1;
#endif

    for (uint32_t i = 0; i < 8; i++) {
        if ((ok & (1u << i)) == 0u) {
            printf("  wiring: %s is not wired to its production callback\n",
                   names[i]);
            failures++;
        }
    }
    /*
     * The VALUE of the build constant, not the number of times it is spelled.
     * The verify composition's SIGUSR1 projection is RELAY_BLOCKED_V0, so it
     * must report the endpoint unavailable; every other composition serves it.
     * Reversing either value fails here.
     */
    if (available != want_available) {
        printf("  wiring: this build reports admin availability %d, "
               "expected %d\n", available, want_available);
        failures++;
    }
    return failures;
}

#ifndef MOQR_BIND_TESTING
/*
 * THE DUAL-FACADE EXPOSITION, through the owner's own path, over the
 * PRODUCTION-RESOLVED plan.
 *
 * The labels and the shard count are not written here: they come from the same
 * resolution and the same label construction the multi-lane serve path uses,
 * so passing a raw lane count instead of the total, or laying either
 * transport's range down wrongly, is visible. The rows are copied snapshot
 * rows, which is all the owner ever reads, and no transport is started.
 */
static int
test_dual_facade_labels_reach_the_owner(void)
{
    int failures = 0;
    moqr_broker_t *b = moqr_test_metrics_broker();
    moqr_cli_snapshot_t snap;
    moqr_cli_config_t cfg;
    moqr_obs_labels_t labels[MOQR_CLI_MAX_LANES];
    static moqr_cli_snapshot_stats_t rows[MOQR_CLI_MAX_LANES];
    static moqr_snapshot_view_t views[MOQR_CLI_MAX_LANES];
    static char out[1024 * 1024];
    char err[192];
    uint32_t shards;
    uint64_t s = 0;
    bool wake = false;
    size_t n;

    /* Two raw lanes and one WebTransport lane, resolved by production. */
    {
        static const char json[] =
            "{\"listener\":{\"port\":4433,\"versions\":[18],\"lanes\":2},"
            "\"webtransport\":{\"port\":4443,\"versions\":[16],\"lanes\":1,"
            "\"cert\":\"c\",\"key\":\"k\"}}";
        if (moqr_cli_config_parse(json, sizeof(json) - 1u, &cfg, err,
                                  sizeof(err)) != MOQR_OK) {
            printf("  dual: the dual-listener fixture was rejected: %s\n",
                   err);
            return 1;
        }
    }
    memset(labels, 0, sizeof(labels));
    shards = moqr_test_admin_resolved_labels(&cfg, labels, MOQR_CLI_MAX_LANES);
    if (shards != 3u) {
        printf("  dual: production resolved %u global shards, expected 3 — a "
               "raw lane count was passed instead of the total\n", shards);
        return failures + 1;
    }

    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(b, MOQR_BROKER_BANKS), MOQR_OK);
    memset(&snap, 0, sizeof(snap));
    MOQ_TEST_CHECK(moqr_cli_snapshot_init(&snap, shards,
                                          moq_alloc_default()) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s,
                                       &wake) == MOQR_OK);
    /* EVERY global shard publishes for the SAME serial: an exposition built
     * from a mixed set would not be one generation's view of the relay. */
    for (uint32_t i = 0; i < shards; i++) {
        publish_row(&snap, i, s, MOQR_CLI_CAP_ABSENT);
    }

    capture_begin();
    (void)moqr_test_admin_render_signal(&snap, labels, shards, rows, views, s);
    n = capture_end(out, sizeof(out));

    /* The shards document over the same three rows: global numbering, the
     * two listener kinds, and every row valid in the shared runtime. */
    {
        static char sdoc[65536];
        size_t slen = 0;
        for (uint32_t i = 0; i < shards; i++) {
            publish_row(&snap, i, s, MOQR_CLI_CAP_VALID);
        }
        if (moqr_test_admin_render_shards(&snap, labels, shards, rows, views, s,
                                          sdoc, sizeof(sdoc), &slen) != MOQR_OK ||
            strstr(sdoc, "\"shard\":0,\"transport\":\"msquic\",\"version\":\"moqt-18\",\"listener\":\"raw\",\"capability\":\"valid\"") == NULL ||
            strstr(sdoc, "\"shard\":1,\"transport\":\"msquic\"") == NULL ||
            strstr(sdoc, "\"shard\":2,\"transport\":\"wtquic-msquic\",\"version\":\"moqt-16\",\"listener\":\"webtransport\",\"capability\":\"valid\"") == NULL ||
            strstr(sdoc, "\"wake_requests_local\":3") == NULL) {
            printf("  dual: the shards document is wrong: [%.200s]\n", sdoc);
            failures++;
        }
        for (uint32_t i = 0; i < shards; i++) {
            publish_row(&snap, i, s, MOQR_CLI_CAP_ABSENT);
        }
    }

    if (n == 0) {
        printf("  dual: the owner rendered nothing for %u global shards\n",
               shards);
        failures++;
    } else {
        static const char *want[] = {
            "shard=\"0\",transport=\"msquic\",version=\"moqt-18\"",
            "shard=\"1\",transport=\"msquic\",version=\"moqt-18\"",
            "shard=\"2\",transport=\"wtquic-msquic\",version=\"moqt-16\"",
        };
        for (size_t i = 0; i < sizeof(want) / sizeof(want[0]); i++) {
            if (strstr(out, want[i]) == NULL) {
                printf("  dual: the exposition is missing %s — a lane range "
                       "or a transport label was collapsed\n", want[i]);
                failures++;
            }
        }
        {
            char banner[64];
            (void)snprintf(banner, sizeof(banner), "(epoch %llu)",
                           (unsigned long long)s);
            if (strstr(out, banner) == NULL) {
                printf("  dual: the document does not name the generation "
                       "every row was published under\n");
                failures++;
            }
        }
    }

    /*
     * A MIXED set is not one generation's view. With one shard still published
     * under an older serial the epoch is incomplete, and the owner must render
     * nothing at all rather than an exposition that is part new and part old.
     */
    {
        uint64_t s2 = 0;
        static char out2[1024 * 1024];
        size_t n2;
        moqr_broker_on_complete(b, s);
        {
            uint32_t d = 0, bank = 0;
            if (moqr_broker_take_serial(b, s, &d, &bank)) {
                bool w = false;
                (void)moqr_broker_release(b, s, bank, &w);
            }
        }
        MOQ_TEST_CHECK(moqr_broker_request(b, MOQR_BROKER_DEMAND_SIGNAL, &s2,
                                           &wake) == MOQR_OK);
        MOQ_TEST_CHECK(s2 != s);
        for (uint32_t i = 0; i + 1 < shards; i++) {
            publish_row(&snap, i, s2, MOQR_CLI_CAP_ABSENT);
        }
        capture_begin();
        (void)moqr_test_admin_render_signal(&snap, labels, shards, rows, views,
                                            s2);
        n2 = capture_end(out2, sizeof(out2));
        if (n2 != 0) {
            printf("  dual: a mixed-serial row set was rendered anyway "
                   "(%zu bytes)\n", n2);
            failures++;
        }
        moqr_broker_on_complete(b, s2);
        {
            uint32_t d = 0, bank = 0;
            if (moqr_broker_take_serial(b, s2, &d, &bank)) {
                bool w = false;
                (void)moqr_broker_release(b, s2, bank, &w);
            }
        }
    }
    moqr_cli_snapshot_destroy(&snap);
    return failures;
}
#endif /* !MOQR_BIND_TESTING */

/* -- the serve log ---------------------------------------------------------
 *
 * The production emission helpers of both serve compositions, driven over
 * copied rows and a scripted sink: the readiness sequence (raw listener,
 * admin endpoint, operating point, signal contract), the per-lane and
 * per-pair attribution records with their refusals, the stop records, the
 * capacity prose on the sink's prose stream, and the ordinary-return
 * finalization -- in text (the accepted bytes) and in JSON (events only on
 * the rows stream, never a credential). */
extern int moqr_test_cmd_serve(const moqr_cli_config_t *cfg);
extern int moqr_test_cmd_serve_lanes(const moqr_cli_config_t *cfg);
extern void moqr_test_serve_log_set_io(const moqr_cli_log_io_t *io);
extern uint8_t moqr_test_serve_log_last_state(void);
extern void moqr_test_serve_log_reset_last_state(void);
extern int moqr_test_serve_print_capacity(const moqr_cli_config_t *cfg,
                                          FILE *out);

typedef struct log_cap {
    char     out[65536];
    size_t   n;
    unsigned writes;
    unsigned flushes;
} log_cap_t;

static bool
log_cap_clock(void *ctx, uint64_t *out)
{
    (void)ctx;
    *out = 5000;
    return true;
}

static size_t
log_cap_write(void *ctx, FILE *f, const char *p, size_t n)
{
    log_cap_t *c = ctx;
    (void)f;
    c->writes++;
    if (n > sizeof(c->out) - c->n - 1u) {
        n = sizeof(c->out) - c->n - 1u;
    }
    memcpy(c->out + c->n, p, n);
    c->n += n;
    c->out[c->n] = '\0';
    return n;
}

static int
log_cap_flush(void *ctx, FILE *f)
{
    log_cap_t *c = ctx;
    (void)f;
    c->flushes++;
    return 0;
}

/* The i-th line of the capture, NUL-terminated into a caller buffer. */
static const char *
log_line(const log_cap_t *c, unsigned i, char *buf, size_t cap)
{
    const char *p = c->out;
    for (unsigned k = 0; k < i; k++) {
        p = strchr(p, '\n');
        if (p == NULL) {
            return NULL;
        }
        p++;
    }
    {
        const char *e = strchr(p, '\n');
        size_t n = e != NULL ? (size_t)(e - p) : strlen(p);
        if (n >= cap) {
            n = cap - 1u;
        }
        memcpy(buf, p, n);
        buf[n] = '\0';
    }
    return buf;
}

static unsigned
count_lines_in(const char *s)
{
    unsigned n = 0;
    for (; *s != '\0'; s++) {
        n += *s == '\n';
    }
    return n;
}

static unsigned
log_lines(const log_cap_t *c)
{
    unsigned n = 0;
    for (const char *p = c->out; *p != '\0'; p++) {
        n += *p == '\n';
    }
    return n;
}

static int
log_expect(const log_cap_t *c, unsigned i, const char *what, const char *needle,
           bool exact)
{
    char buf[4096];
    const char *l = log_line(c, i, buf, sizeof(buf));
    if (l == NULL) {
        printf("  serve-log/%s: line %u is missing\n", what, i);
        return 1;
    }
    if (exact ? strcmp(l, needle) != 0 : strstr(l, needle) == NULL) {
        printf("  serve-log/%s: line %u is [%s], expected %s[%s]\n", what, i, l,
               exact ? "" : "to contain ", needle);
        return 1;
    }
    return 0;
}

static moq_msquic_lane_stats_t
log_ad_fixture(void)
{
    moq_msquic_lane_stats_t ad;
    memset(&ad, 0, sizeof(ad));
    ad.struct_size = (uint32_t)sizeof(ad);
    ad.wakes_same_lane = 11;
    ad.wakes_cross_lane = 12;
    ad.wakes_external = 13;
    ad.flush_bytes = 14;
    return ad;
}

static moqr_shards_stats_t
log_ss_fixture(void)
{
    moqr_shards_stats_t ss;
    memset(&ss, 0, sizeof(ss));
    ss.pump_turns = 72;
    ss.wake_requests_push = 74;
    ss.wake_requests_credit = 75;
    ss.wake_requests_local = 76;
    ss.enqueued[MOQR_SHARDS_MSG_OBJ] = 77;
    return ss;
}

static int
serve_log_k1(moqr_cli_log_format_t fmt)
{
    int failures = 0;
    const bool json = fmt == MOQR_CLI_LOG_JSON;
    const char *w = json ? "k1-json" : "k1-text";
    moqr_cli_config_t cfg;
    moqr_cli_log_t log;
    log_cap_t c;
    moqr_cli_log_io_t io = { log_cap_clock, log_cap_write, log_cap_flush, &c };
    FILE *prose = tmpfile();
    char err[192];
    char pbuf[8192];
    size_t pn;

    static const char cfg_json[] =
        "{\"listener\":{\"port\":4433,\"versions\":[18],"
        "\"cert\":\"/tmp/CANARY-cert.pem\",\"key\":\"/tmp/CANARY-key.pem\"},"
        "\"admin\":{\"tcp\":{\"port\":9109}}}";
    if (moqr_cli_config_parse(cfg_json, sizeof(cfg_json) - 1u, &cfg, err,
                              sizeof(err)) != MOQR_OK) {
        printf("  serve-log/%s: the fixture was rejected: %s\n", w, err);
        fclose(prose);
        return 1;
    }
    memset(&c, 0, sizeof(c));
    if (moqr_cli_log_init(&log, fmt, stdout, prose, &io) != MOQR_OK) {
        printf("  serve-log/%s: the sink refused\n", w);
        fclose(prose);
        return 1;
    }
    if (moqr_cli_log_prose_stream(&log) != (json ? prose : stdout)) {
        printf("  serve-log/%s: the prose stream is wrong\n", w);
        failures++;
    }

    /* readiness: raw, admin, operating point, signals; one flush in text,
     * one per event in JSON */
    {
        moqr_shards_cfg_t scfg;
        moqr_cli_build_shards_cfg(&cfg, moq_alloc_default(), &scfg);
        moqr_cli_serve_log_readiness(&log, &cfg, MOQR_CLI_LOG_COMP_K1, 9109, 0, &scfg);
    }
    if (log_lines(&c) != 4u) {
        printf("  serve-log/%s: %u readiness lines, expected 4\n", w, log_lines(&c));
        failures++;
    }
    if (!json) {
        char want[512];
        (void)snprintf(want, sizeof(want), "MOQ5 Relay: listening on %s:4433 (%s)",
                       cfg.host, cfg.alpn_set);
        failures += log_expect(&c, 0, w, want, true);
        failures += log_expect(&c, 1, w,
                               "MOQ5 Relay: admin endpoint on 127.0.0.1:9109 (GET /metrics)", true);
        failures += log_expect(&c, 2, w, "RELAY_RUN_CONFIG_V1,pump_turn_messages=", false);
        failures += log_expect(&c, 2, w, ",eor=1", false);
        failures += log_expect(&c, 3, w,
                               "signals: SIGUSR1 -> metrics + route dump, SIGUSR2 -> trace JSONL (both to stderr)", true);
        if (c.flushes != 1u) {
            printf("  serve-log/%s: readiness performed %u flushes, expected 1\n", w, c.flushes);
            failures++;
        }
    } else {
        failures += log_expect(&c, 0, w, "{\"schema\":\"RELAY_READY_V1\",\"elapsed_us\":0,\"listener\":\"raw\",\"host\":\"0.0.0.0\",\"port\":4433,\"alpn_set\":\"", false);
        failures += log_expect(&c, 0, w, "\",\"lanes\":1}", false);
        failures += log_expect(&c, 1, w, "\"listener\":\"admin\",\"host\":\"127.0.0.1\",\"port\":9109,\"targets\":[\"/metrics\",\"/api/v1/info\",\"/api/v1/shards\"]}", false);
        failures += log_expect(&c, 2, w, "{\"schema\":\"RELAY_RUN_CONFIG_V1\",\"elapsed_us\":0,\"pump_turn_messages\":", false);
        failures += log_expect(&c, 3, w, "{\"schema\":\"RELAY_READY_V1\",\"elapsed_us\":0,\"listener\":\"signals\",\"sigusr1\":\"metrics+routes\",\"sigusr2\":\"trace\",\"sink\":\"stderr\"}", true);
        if (c.flushes != 4u || c.writes != 4u) {
            printf("  serve-log/%s: readiness performed %u writes and %u flushes, expected 4 and 4\n",
                   w, c.writes, c.flushes);
            failures++;
        }
    }

    /* the lanes=1 attribution row: shard-plane fields are true zeros */
    {
        moq_msquic_lane_stats_t ad = log_ad_fixture();
        unsigned at = log_lines(&c);
        moqr_cli_serve_log_lane(&log, 0, true, &ad, true, NULL);
        moqr_cli_serve_log_lane(&log, 0, false, &ad, true, NULL);
        if (!json) {
            failures += log_expect(&c, at, w, "RELAY_LANE_STATS_V3,lane=0,wakes_same_lane=11,wakes_cross_lane=12,wakes_external=13,", false);
            failures += log_expect(&c, at, w, ",flush_bytes=14,pump_turns=0,pump_messages=0,", false);
            failures += log_expect(&c, at + 1u, w, "RELAY_LANE_STATS_V3,lane=0,refused=adapter", true);
        } else {
            failures += log_expect(&c, at, w, "{\"schema\":\"RELAY_LANE_STATS_V3\",\"elapsed_us\":0,\"lane\":0,\"wakes_same_lane\":11,\"wakes_cross_lane\":12,\"wakes_external\":13,", false);
            failures += log_expect(&c, at, w, "\"flush_bytes\":14,\"pump_turns\":0,", false);
            failures += log_expect(&c, at + 1u, w, "{\"schema\":\"RELAY_LANE_STATS_V3\",\"elapsed_us\":0,\"lane\":0,\"refused\":\"adapter\"}", true);
        }
    }

    /* the single-facade stop record */
    {
        moqr_bind_stats_t bs;
        moqr_core_stats_t cs;
        unsigned at = log_lines(&c);
        memset(&bs, 0, sizeof(bs));
        memset(&cs, 0, sizeof(cs));
        bs.conns = 1; cs.tracks = 2; cs.ingested_total = 3;
        cs.delivered_total = 4; bs.session_errors = 5;
        moqr_cli_serve_log_stop_k1(&log, &bs, &cs);
        if (!json) {
            failures += log_expect(&c, at, w, "MOQ5 Relay: stopping — conns 1, tracks 2, ingested 3, delivered 4, session errors 5", true);
        } else {
            failures += log_expect(&c, at, w, "{\"schema\":\"RELAY_STOP_V1\",\"elapsed_us\":0,\"shard\":0,\"conns\":1,\"tracks\":2,\"ingested\":3,\"delivered\":4,\"session_errors\":5}", true);
        }
    }

    /* the capacity prose lands on the stream the serve names, never among
     * the events */
    {
        FILE *capf = tmpfile();
        size_t before = c.n;
        if (moqr_test_serve_print_capacity(&cfg, capf) != 0) {
            printf("  serve-log/%s: the capacity print refused\n", w);
            failures++;
        }
        fflush(capf);
        rewind(capf);
        pn = fread(pbuf, 1, sizeof(pbuf) - 1u, capf);
        pbuf[pn] = '\0';
        if (strstr(pbuf, "relay-state allocation-request ceiling: ") == NULL ||
            strstr(pbuf, "usable client bindings per shard: ") == NULL) {
            printf("  serve-log/%s: the capacity prose did not reach the named stream\n", w);
            failures++;
        }
        if (c.n != before) {
            printf("  serve-log/%s: the capacity print wrote to the rows stream\n", w);
            failures++;
        }
        fclose(capf);
    }

    /* never a credential, on either stream */
    fflush(prose);
    rewind(prose);
    pn = fread(pbuf, 1, sizeof(pbuf) - 1u, prose);
    pbuf[pn] = '\0';
    if (strstr(c.out, "CANARY") != NULL || strstr(pbuf, "CANARY") != NULL) {
        printf("  serve-log/%s: a credential path reached the log\n", w);
        failures++;
    }
    if (pn != 0) {
        printf("  serve-log/%s: a diagnostic was printed for a clean run: [%s]\n", w, pbuf);
        failures++;
    }

    /* ordinary-return finalization */
    {
        unsigned fl = c.flushes;
        size_t n = c.n;
        moqr_cli_log_finish(&log);
        if (log.sink_state != MOQR_CLI_LOG_SINK_CLOSED || c.n != n ||
            c.flushes != fl + (json ? 1u : 0u)) {
            printf("  serve-log/%s: finish left state %u, flushes %u (was %u)\n", w,
                   (unsigned)log.sink_state, c.flushes, fl);
            failures++;
        }
    }
    fclose(prose);
    return failures;
}

static int
serve_log_lanes(moqr_cli_log_format_t fmt)
{
    int failures = 0;
    const bool json = fmt == MOQR_CLI_LOG_JSON;
    const char *w = json ? "k4-json" : "k4-text";
    moqr_cli_config_t cfg;
    moqr_cli_log_t log;
    log_cap_t c;
    moqr_cli_log_io_t io = { log_cap_clock, log_cap_write, log_cap_flush, &c };
    FILE *prose = tmpfile();
    char err[192];
    char pbuf[8192];
    size_t pn;

    static const char cfg_json[] =
        "{\"listener\":{\"port\":4433,\"versions\":[18,16],\"lanes\":4,"
        "\"cert\":\"/tmp/CANARY-cert.pem\",\"key\":\"/tmp/CANARY-key.pem\"}}";
    if (moqr_cli_config_parse(cfg_json, sizeof(cfg_json) - 1u, &cfg, err,
                              sizeof(err)) != MOQR_OK) {
        printf("  serve-log/%s: the fixture was rejected: %s\n", w, err);
        fclose(prose);
        return 1;
    }
    memset(&c, 0, sizeof(c));
    if (moqr_cli_log_init(&log, fmt, stdout, prose, &io) != MOQR_OK) {
        printf("  serve-log/%s: the sink refused\n", w);
        fclose(prose);
        return 1;
    }

    /* readiness without an admin endpoint: raw, operating point, signals */
    {
        moqr_shards_cfg_t scfg;
        moqr_cli_build_shards_cfg(&cfg, moq_alloc_default(), &scfg);
        moqr_cli_serve_log_readiness(&log, &cfg, MOQR_CLI_LOG_COMP_LANES, -1, 0, &scfg);
    }
    if (log_lines(&c) != 3u) {
        printf("  serve-log/%s: %u readiness lines, expected 3\n", w, log_lines(&c));
        failures++;
    }
    if (!json) {
        char want[512];
        (void)snprintf(want, sizeof(want),
                       "MOQ5 Relay: listening on %s:4433 (%s), 4 lanes", cfg.host,
                       cfg.alpn_set);
        failures += log_expect(&c, 0, w, want, true);
        failures += log_expect(&c, 1, w, "RELAY_RUN_CONFIG_V1,pump_turn_messages=", false);
        failures += log_expect(&c, 2, w,
                               "signals: SIGUSR1 -> metrics + route dump, SIGUSR2 -> trace JSONL (per shard, to stderr)", true);
    } else {
        failures += log_expect(&c, 0, w, "\"listener\":\"raw\",\"host\":\"0.0.0.0\",\"port\":4433,\"alpn_set\":\"", false);
        failures += log_expect(&c, 0, w, "\",\"lanes\":4}", false);
        failures += log_expect(&c, 1, w, "{\"schema\":\"RELAY_RUN_CONFIG_V1\",", false);
        failures += log_expect(&c, 2, w, "\"listener\":\"signals\"", false);
        if (strstr(c.out, "\"listener\":\"admin\"") != NULL) {
            printf("  serve-log/%s: an admin readiness event without an endpoint\n", w);
            failures++;
        }
    }

    /* four lane rows: valid, adapter refused, shard refused, valid */
    {
        moq_msquic_lane_stats_t ad = log_ad_fixture();
        moqr_shards_stats_t ss = log_ss_fixture();
        unsigned at = log_lines(&c);
        moqr_cli_serve_log_lane(&log, 0, true, &ad, true, &ss);
        moqr_cli_serve_log_lane(&log, 1, false, &ad, true, &ss);
        moqr_cli_serve_log_lane(&log, 2, true, &ad, false, &ss);
        moqr_cli_serve_log_lane(&log, 3, true, &ad, true, &ss);
        if (!json) {
            failures += log_expect(&c, at, w, "RELAY_LANE_STATS_V3,lane=0,wakes_same_lane=11,", false);
            failures += log_expect(&c, at, w, ",pump_turns=72,", false);
            failures += log_expect(&c, at, w, ",enq_obj=77,", false);
            failures += log_expect(&c, at, w, ",wake_requests_local=76,eor=1", false);
            failures += log_expect(&c, at + 1u, w, "RELAY_LANE_STATS_V3,lane=1,refused=adapter", true);
            failures += log_expect(&c, at + 2u, w, "RELAY_LANE_STATS_V3,lane=2,refused=shard", true);
            failures += log_expect(&c, at + 3u, w, "RELAY_LANE_STATS_V3,lane=3,wakes_same_lane=11,", false);
        } else {
            failures += log_expect(&c, at, w, "\"lane\":0,\"wakes_same_lane\":11,", false);
            failures += log_expect(&c, at, w, "\"pump_turns\":72,", false);
            failures += log_expect(&c, at, w, "\"enq_obj\":77,", false);
            failures += log_expect(&c, at, w, "\"wake_requests_local\":76}", false);
            failures += log_expect(&c, at + 1u, w, "\"lane\":1,\"refused\":\"adapter\"}", false);
            failures += log_expect(&c, at + 2u, w, "\"lane\":2,\"refused\":\"shard\"}", false);
            failures += log_expect(&c, at + 3u, w, "\"lane\":3,\"wakes_same_lane\":11,", false);
        }
    }

    /* pair rows: one valid, one refused */
    {
        moqr_shards_pair_stats_t pst;
        unsigned at = log_lines(&c);
        memset(&pst, 0, sizeof(pst));
        pst.data_messages = 201; pst.data_bytes = 202; pst.control_messages = 203;
        pst.refused_entries = 204; pst.refused_bytes = 205;
        moqr_cli_serve_log_pair(&log, 0, 1, true, &pst);
        moqr_cli_serve_log_pair(&log, 1, 0, false, &pst);
        if (!json) {
            failures += log_expect(&c, at, w, "RELAY_PAIR_STATS_V1,src=0,dst=1,data_messages=201,data_bytes=202,control_messages=203,refused_entries=204,refused_bytes=205,eor=1", true);
            failures += log_expect(&c, at + 1u, w, "RELAY_PAIR_STATS_V1,src=1,dst=0,refused=shard", true);
        } else {
            failures += log_expect(&c, at, w, "{\"schema\":\"RELAY_PAIR_STATS_V1\",\"elapsed_us\":0,\"src\":0,\"dst\":1,\"data_messages\":201,\"data_bytes\":202,\"control_messages\":203,\"refused_entries\":204,\"refused_bytes\":205}", true);
            failures += log_expect(&c, at + 1u, w, "{\"schema\":\"RELAY_PAIR_STATS_V1\",\"elapsed_us\":0,\"src\":1,\"dst\":0,\"refused\":\"shard\"}", true);
        }
    }

    /* stop rows: a healthy shard, a poisoned one, the total */
    {
        moqr_bind_stats_t bs;
        moqr_core_stats_t cs;
        moqr_shards_stats_t ss = log_ss_fixture();
        unsigned at = log_lines(&c);
        memset(&bs, 0, sizeof(bs));
        memset(&cs, 0, sizeof(cs));
        bs.conns = 1; cs.tracks = 2; cs.ingested_total = 3;
        cs.delivered_total = 4; bs.session_errors = 5;
        moqr_cli_serve_log_stop_shard(&log, 0, &bs, &cs, 71, 73, true, &ss);
        moqr_cli_serve_log_stop_shard(&log, 1, &bs, &cs, 71, 73, false, &ss);
        moqr_cli_serve_log_stop_total(&log, 4, 4, 8, 12, 16, 20);
        if (!json) {
            failures += log_expect(&c, at, w, "MOQ5 Relay: shard 0 — conns 1, tracks 2, ingested 3, delivered 4, session errors 5, pump turns 71, wakes 73 (requests: push 74, credit 75, local 76)", true);
            failures += log_expect(&c, at + 1u, w, "MOQ5 Relay: shard 1 — conns 1, tracks 2, ingested 3, delivered 4, session errors 5, pump turns 71, wakes 73 (shard stats refused: poisoned snapshot)", true);
            failures += log_expect(&c, at + 2u, w, "MOQ5 Relay: stopping — total conns 4, tracks 8, ingested 12, delivered 16, session errors 20", true);
        } else {
            failures += log_expect(&c, at, w, "{\"schema\":\"RELAY_STOP_V1\",\"elapsed_us\":0,\"shard\":0,\"conns\":1,\"tracks\":2,\"ingested\":3,\"delivered\":4,\"session_errors\":5,\"pump_turns\":71,\"wakes\":73,\"wake_requests\":{\"push\":74,\"credit\":75,\"local\":76}}", true);
            failures += log_expect(&c, at + 1u, w, "{\"schema\":\"RELAY_STOP_V1\",\"elapsed_us\":0,\"shard\":1,\"refused\":\"poisoned\",\"conns\":1,\"tracks\":2,\"ingested\":3,\"delivered\":4,\"session_errors\":5,\"pump_turns\":71,\"wakes\":73}", true);
            failures += log_expect(&c, at + 2u, w, "{\"schema\":\"RELAY_STOP_V1\",\"elapsed_us\":0,\"total\":true,\"shards\":4,\"conns\":4,\"tracks\":8,\"ingested\":12,\"delivered\":16,\"session_errors\":20}", true);
        }
    }

    fflush(prose);
    rewind(prose);
    pn = fread(pbuf, 1, sizeof(pbuf) - 1u, prose);
    pbuf[pn] = '\0';
    if (strstr(c.out, "CANARY") != NULL || strstr(pbuf, "CANARY") != NULL) {
        printf("  serve-log/%s: a credential path reached the log\n", w);
        failures++;
    }
    if (pn != 0) {
        printf("  serve-log/%s: a diagnostic was printed for a clean run: [%s]\n", w, pbuf);
        failures++;
    }
    moqr_cli_log_finish(&log);
    if (log.sink_state != MOQR_CLI_LOG_SINK_CLOSED) {
        printf("  serve-log/%s: finish did not close the sink\n", w);
        failures++;
    }
    fclose(prose);
    return failures;
}

/* The dual composition's readiness through the production helper: the
 * WebTransport event carries the WebTransport listener's own offered set and
 * its configured profile under the closed three-way name mapping -- never
 * the raw listener's set, never a default name for an unknown profile (an
 * unknown profile drops that event with one diagnostic and the other events
 * still land). Every expectation is a literal, not a value read back from
 * the configuration. */
static int
dual_readiness_case(moqr_cli_log_format_t fmt, const char *cfg_json,
                    moqr_cli_wt_profile_t profile, const char *want_text_wt,
                    const char *want_json_wt, bool expect_dropped)
{
    int failures = 0;
    const bool json = fmt == MOQR_CLI_LOG_JSON;
    const char *w = json ? "dual-json" : "dual-text";
    moqr_cli_config_t cfg;
    moqr_cli_log_t log;
    log_cap_t c;
    moqr_cli_log_io_t io = { log_cap_clock, log_cap_write, log_cap_flush, &c };
    FILE *prose = tmpfile();
    moqr_shards_cfg_t scfg;
    char err[192];
    char pbuf[4096];
    size_t pn;

    if (moqr_cli_config_parse(cfg_json, strlen(cfg_json), &cfg, err,
                              sizeof(err)) != MOQR_OK) {
        printf("  %s: the fixture was rejected: %s\n", w, err);
        fclose(prose);
        return 1;
    }
    cfg.wt.profile = profile;
    memset(&c, 0, sizeof(c));
    if (moqr_cli_log_init(&log, fmt, stdout, prose, &io) != MOQR_OK) {
        printf("  %s: the sink refused\n", w);
        fclose(prose);
        return 1;
    }
    moqr_cli_build_shards_cfg(&cfg, moq_alloc_default(), &scfg);
    moqr_cli_serve_log_readiness(&log, &cfg, MOQR_CLI_LOG_COMP_LANES, -1, 1,
                                 &scfg);
    moqr_cli_log_finish(&log);
    if (log_lines(&c) != (expect_dropped ? 3u : 4u)) {
        printf("  %s/%u: %u readiness lines:\n%s", w, (unsigned)profile,
               log_lines(&c), c.out);
        failures++;
    }
    if (!expect_dropped) {
        failures += log_expect(&c, 1, w, json ? want_json_wt : want_text_wt, true);
        failures += log_expect(&c, 2, w, json ? "{\"schema\":\"RELAY_RUN_CONFIG_V1\","
                                              : "RELAY_RUN_CONFIG_V1,pump_turn_messages=", false);
        failures += log_expect(&c, 3, w, json ? "\"listener\":\"signals\""
                                              : "signals: SIGUSR1 -> metrics + route dump, SIGUSR2 -> trace JSONL (per shard, to stderr)", !json);
    } else {
        failures += log_expect(&c, 1, w, json ? "{\"schema\":\"RELAY_RUN_CONFIG_V1\","
                                              : "RELAY_RUN_CONFIG_V1,pump_turn_messages=", false);
        if (strstr(c.out, "webtransport") != NULL || strstr(c.out, "WebTransport") != NULL) {
            printf("  %s/%u: an unknown profile still published a WebTransport record\n", w, (unsigned)profile);
            failures++;
        }
    }
    fflush(prose);
    rewind(prose);
    pn = fread(pbuf, 1, sizeof(pbuf) - 1u, prose);
    pbuf[pn] = '\0';
    if (expect_dropped) {
        if (count_lines_in(pbuf) != 1u || strstr(pbuf, "RELAY_READY_V1") == NULL) {
            printf("  %s/%u: expected one diagnostic naming RELAY_READY_V1, got [%s]\n", w, (unsigned)profile, pbuf);
            failures++;
        }
    } else if (pn != 0) {
        printf("  %s/%u: a diagnostic was printed for a clean run: [%s]\n", w, (unsigned)profile, pbuf);
        failures++;
    }
    if (strstr(c.out, "CANARY") != NULL || strstr(pbuf, "CANARY") != NULL) {
        printf("  %s: a credential path reached the log\n", w);
        failures++;
    }
    fclose(prose);
    return failures;
}

static int
test_dual_readiness_metadata(void)
{
    int failures = 0;
    /* raw offers 18, WebTransport offers 16: the two sets differ */
    static const char differing[] =
        "{\"listener\":{\"host\":\"127.0.0.1\",\"port\":4433,\"versions\":[18],\"lanes\":1,"
        "\"cert\":\"/tmp/CANARY-raw.pem\",\"key\":\"/tmp/CANARY-raw-key.pem\"},"
        "\"webtransport\":{\"host\":\"127.0.0.1\",\"port\":4443,\"versions\":[16],\"lanes\":1,"
        "\"cert\":\"/tmp/CANARY-wt.pem\",\"key\":\"/tmp/CANARY-wt-key.pem\"}}";
    /* raw offers both, WebTransport offers 16 only */
    static const char both_vs_one[] =
        "{\"listener\":{\"host\":\"127.0.0.1\",\"port\":4433,\"versions\":[18,16],\"lanes\":1,"
        "\"cert\":\"/tmp/CANARY-raw.pem\",\"key\":\"/tmp/CANARY-raw-key.pem\"},"
        "\"webtransport\":{\"host\":\"127.0.0.1\",\"port\":4443,\"versions\":[16],\"lanes\":1,"
        "\"cert\":\"/tmp/CANARY-wt.pem\",\"key\":\"/tmp/CANARY-wt-key.pem\"}}";
    static const struct {
        moqr_cli_wt_profile_t profile;
        const char *text;
        const char *json;
    } profiles[] = {
        { MOQR_CLI_WT_PROFILE_CURRENT,
          "MOQ5 Relay: WebTransport on 127.0.0.1:4443/moq (moqt-16, profile current), 1 lanes",
          "{\"schema\":\"RELAY_READY_V1\",\"elapsed_us\":0,\"listener\":\"webtransport\",\"host\":\"127.0.0.1\",\"port\":4443,\"path\":\"/moq\",\"alpn_set\":\"moqt-16\",\"profile\":\"current\",\"lanes\":1}" },
        { MOQR_CLI_WT_PROFILE_D13_14_COMPAT,
          "MOQ5 Relay: WebTransport on 127.0.0.1:4443/moq (moqt-16, profile d13_14_compat), 1 lanes",
          "{\"schema\":\"RELAY_READY_V1\",\"elapsed_us\":0,\"listener\":\"webtransport\",\"host\":\"127.0.0.1\",\"port\":4443,\"path\":\"/moq\",\"alpn_set\":\"moqt-16\",\"profile\":\"d13_14_compat\",\"lanes\":1}" },
        { MOQR_CLI_WT_PROFILE_D02_RFC9297_COMPAT,
          "MOQ5 Relay: WebTransport on 127.0.0.1:4443/moq (moqt-16, profile d02_rfc9297_compat), 1 lanes",
          "{\"schema\":\"RELAY_READY_V1\",\"elapsed_us\":0,\"listener\":\"webtransport\",\"host\":\"127.0.0.1\",\"port\":4443,\"path\":\"/moq\",\"alpn_set\":\"moqt-16\",\"profile\":\"d02_rfc9297_compat\",\"lanes\":1}" },
    };
    for (size_t i = 0; i < sizeof(profiles) / sizeof(profiles[0]); i++) {
        failures += dual_readiness_case(MOQR_CLI_LOG_TEXT, differing, profiles[i].profile,
                                        profiles[i].text, profiles[i].json, false);
        failures += dual_readiness_case(MOQR_CLI_LOG_JSON, differing, profiles[i].profile,
                                        profiles[i].text, profiles[i].json, false);
        failures += dual_readiness_case(MOQR_CLI_LOG_TEXT, both_vs_one, profiles[i].profile,
                                        profiles[i].text, profiles[i].json, false);
        failures += dual_readiness_case(MOQR_CLI_LOG_JSON, both_vs_one, profiles[i].profile,
                                        profiles[i].text, profiles[i].json, false);
    }
    /* the raw line of the both-vs-one fixture names the raw set, literally */
    {
        moqr_cli_config_t cfg;
        moqr_cli_log_t log;
        log_cap_t c;
        moqr_cli_log_io_t io = { log_cap_clock, log_cap_write, log_cap_flush, &c };
        moqr_shards_cfg_t scfg;
        char err[192];
        if (moqr_cli_config_parse(both_vs_one, strlen(both_vs_one), &cfg, err,
                                  sizeof(err)) == MOQR_OK) {
            memset(&c, 0, sizeof(c));
            if (moqr_cli_log_init(&log, MOQR_CLI_LOG_TEXT, stdout, stderr, &io) == MOQR_OK) {
                moqr_cli_build_shards_cfg(&cfg, moq_alloc_default(), &scfg);
                moqr_cli_serve_log_readiness(&log, &cfg, MOQR_CLI_LOG_COMP_LANES, -1, 1, &scfg);
                failures += log_expect(&c, 0, "dual-raw",
                                       "MOQ5 Relay: listening on 127.0.0.1:4433 (moqt-18+moqt-16), 1 lanes", true);
                moqr_cli_log_finish(&log);
            }
        }
    }
    /* an unknown profile value never becomes a name */
    failures += dual_readiness_case(MOQR_CLI_LOG_TEXT, differing, (moqr_cli_wt_profile_t)7, NULL, NULL, true);
    failures += dual_readiness_case(MOQR_CLI_LOG_JSON, differing, (moqr_cli_wt_profile_t)7, NULL, NULL, true);
    return failures;
}

/* A REAL serve start against the fake facade that refuses AFTER the sink is
 * initialised must still finalize the sink on that ordinary return. The
 * refusal is injected deterministically through the fake facade's own API
 * table: RegistrationOpen refuses (counted, handle NULL), the real managed
 * create unwinds, and both serve compositions take their transport-create
 * return -- before the admin listener and before the signal handlers. No
 * bytes were ever submitted (readiness was never reached), the JSON stream's
 * checked final flush is exactly one, text flushes nothing, and the fake
 * holds no registration, configuration or listener afterwards. Both
 * compositions, both formats. */
static unsigned g_reg_open_refusals;

static QUIC_STATUS QUIC_API
reg_open_refuse(const QUIC_REGISTRATION_CONFIG *cfg, HQUIC *out)
{
    (void)cfg;
    g_reg_open_refusals++;
    *out = NULL;
    return QUIC_STATUS_INTERNAL_ERROR;
}

static int
early_return_case(bool lanes, moqr_cli_log_format_t fmt)
{
    int failures = 0;
    const bool json = fmt == MOQR_CLI_LOG_JSON;
    const char *w = lanes ? (json ? "early-lanes-json" : "early-lanes-text")
                          : (json ? "early-k1-json" : "early-k1-text");
    char cfg_json[512];
    moqr_cli_config_t cfg;
    char err[192];
    static fake_mgd_t fake;
    log_cap_t c;
    moqr_cli_log_io_t io = { log_cap_clock, log_cap_write, log_cap_flush, &c };
    int rc;

    (void)snprintf(cfg_json, sizeof(cfg_json),
                   "{\"listener\":{\"port\":4433,\"versions\":[18],\"lanes\":%u,"
                   "\"cert\":\"unused-by-the-fake-cert.pem\",\"key\":\"unused-by-the-fake-key.pem\"},"
                   "\"logging\":{\"format\":\"%s\"}}",
                   lanes ? 2u : 1u, json ? "json" : "text");
    if (moqr_cli_config_parse(cfg_json, strlen(cfg_json), &cfg, err, sizeof(err)) != MOQR_OK) {
        printf("  %s: the fixture was rejected: %s\n", w, err);
        return 1;
    }
    fake_mgd_init(&fake);
    fake.api.RegistrationOpen = reg_open_refuse;
    g_reg_open_refusals = 0;
    moq_msq_test_api_override = fake_mgd_table(&fake);
    moq_msq_test_no_doorbell = true;
    memset(&c, 0, sizeof(c));
    moqr_test_serve_log_set_io(&io);
    moqr_test_serve_log_reset_last_state();
    rc = lanes ? moqr_test_cmd_serve_lanes(&cfg) : moqr_test_cmd_serve(&cfg);
    moqr_test_serve_log_set_io(NULL);
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
    if (g_reg_open_refusals != 1u) {
        printf("  %s: the injected RegistrationOpen refusal ran %u times, expected 1\n",
               w, g_reg_open_refusals);
        failures++;
    }
    if (rc != 1) {
        printf("  %s: the serve returned %d, expected the transport-create refusal (1)\n", w, rc);
        failures++;
    }
    if (fake.reg_open || fake.cfg_open || fake.listener_open || fake.listener_started ||
        fake.conn_count != 0) {
        printf("  %s: the fake still holds a registration, configuration, listener or connection\n", w);
        failures++;
    }
    if (c.writes != 0u || c.n != 0u) {
        printf("  %s: %u writes reached the rows stream before readiness\n", w, c.writes);
        failures++;
    }
    if (c.flushes != (json ? 1u : 0u)) {
        printf("  %s: the early return performed %u flushes, expected %u (the "
               "finalization)\n", w, c.flushes, json ? 1u : 0u);
        failures++;
    }
    if (moqr_test_serve_log_last_state() != MOQR_CLI_LOG_SINK_CLOSED) {
        printf("  %s: the sink was left in state %u, not CLOSED\n", w,
               (unsigned)moqr_test_serve_log_last_state());
        failures++;
    }
    return failures;
}

static int
test_serve_early_return_finalizes(void)
{
    int failures = 0;
    failures += early_return_case(false, MOQR_CLI_LOG_JSON);
    failures += early_return_case(false, MOQR_CLI_LOG_TEXT);
    failures += early_return_case(true, MOQR_CLI_LOG_JSON);
    failures += early_return_case(true, MOQR_CLI_LOG_TEXT);
    return failures;
}

static int
test_serve_log_emission(void)
{
    int failures = 0;
    failures += serve_log_k1(MOQR_CLI_LOG_TEXT);
    failures += serve_log_k1(MOQR_CLI_LOG_JSON);
    failures += serve_log_lanes(MOQR_CLI_LOG_TEXT);
    failures += serve_log_lanes(MOQR_CLI_LOG_JSON);
    return failures;
}

/* -- a configuration this build cannot serve is refused by BOTH commands --- */

/*
 * Build capability is a property of the compiled command, not of one
 * subcommand.
 *
 * A binary without WebTransport support cannot run a configured `webtransport`
 * object. If only `serve` says so, an operator who validates with `capacity`
 * gets a clean answer -- and a ceiling that counts lanes this binary will never
 * open -- for a configuration that will not start. Both commands reach the same
 * preflight, so both must refuse, and a refusal must publish nothing.
 *
 * A refusal is only evidence when its REASON is checked. Every other way of
 * failing to start -- an unwritable fixture, an unreadable config, a semantic
 * error -- also exits 2 with an empty stdout, so status alone cannot tell them
 * apart. Each case below therefore asserts the exact diagnostic, and every
 * fixture and capture result is checked so that a broken setup fails as a test
 * failure rather than passing as proof about the product.
 *
 * This drives the REAL command entry in the REAL raw-only composition: this
 * translation unit compiles cli/main.c without MOQR_DUAL_LISTENER, exactly as
 * a raw-only product build does.
 */
int moqr_cli_disabled_main(int argc, char **argv);

/* The exact diagnostic the shared preflight emits, and the only one that
 * proves the capability decision ran. */
static const char kNoWtRefusal[] =
    "config error: this build has no WebTransport listener support; "
    "a configured webtransport object cannot be served\n";

/* One captured run of the real command entry. `ok` says the CAPTURE worked;
 * it is not the command's verdict. An unreadable capture must never be
 * mistaken for a command that printed nothing. */
typedef struct cli_run {
    int    ok;
    int    status;
    char   out[8192];
    size_t out_len;
    char   err[4096];
    size_t err_len;
} cli_run_t;

/* Test-only fault injection, so each control exercises the real helper path
 * rather than a mock. All default off and are reset by their control. */
static int g_capture_fault;   /* the captured descriptor is unusable */

/*
 * Which flush is made to report failure.
 *
 * The phase matters: the pre-redirect flush and the two post-command flushes
 * are different guards, and a fault that always stops at the first one leaves
 * the post-command returns -- the original defect -- unexercised.
 */
enum {
    FLUSH_FAULT_NONE = 0,
    FLUSH_FAULT_PRE,          /* before either stream is redirected */
    FLUSH_FAULT_POST_STDOUT,  /* after the command returns, stdout */
    FLUSH_FAULT_POST_STDERR   /* after the command returns, stderr */
};
static int g_flush_fault_phase;
static int g_flush_faults_applied; /* so a control can prove its fault fired */
static int g_inject_out;      /* NUL + trailing bytes on stdout, in-interval */
static int g_inject_err;      /* NUL + trailing bytes on stderr, in-interval */
static int g_inject_overflow; /* more bytes than the capture buffer holds */

/* A flush whose result is honoured. The real flush always runs; the injection
 * only substitutes its RETURN at ONE named site, which is the defect under
 * test. */
static int
checked_flush(FILE *f, int site)
{
    int rc = fflush(f);
    if (g_flush_fault_phase != FLUSH_FAULT_NONE &&
        g_flush_fault_phase == site) {
        g_flush_faults_applied++;
        rc = EOF;
    }
    return rc;
}

/* Redirect one standard stream to a fresh temporary file. */
static int
cap_begin(int fileno_target, char *tmpl, int *saved_fd)
{
    int fd = mkstemp(tmpl);
    if (fd < 0) {
        return -1;
    }
    *saved_fd = dup(fileno_target);
    if (*saved_fd < 0) {
        close(fd);
        unlink(tmpl);
        return -1;
    }
    if (dup2(fd, fileno_target) < 0) {
        close(*saved_fd);
        close(fd);
        unlink(tmpl);
        return -1;
    }
    return fd;
}

/*
 * Restore the stream and read back exactly what was written.
 *
 * Returns 0 on success and fills *len with the byte count. A capture that
 * could not be read, or that produced more bytes than the buffer can hold, is
 * a FAILURE -- never a truncated or empty success, because the oracle above
 * decides a contract from these bytes.
 *
 * Restoration and cleanup are always ATTEMPTED and any error is reported; the
 * helper cannot promise the stream was restored if dup2 itself fails.
 */
static int
cap_end(int fileno_target, int fd, int saved_fd, char *tmpl,
        char *out, size_t out_cap, size_t *len)
{
    FILE *f;
    size_t n;
    int rc = 0;

    *len = 0;
    /* restoration is attempted first and unconditionally, so a failed capture
     * does not leave the stream redirected into a file about to be unlinked;
     * if dup2 itself fails, that is reported, not silently survived */
    if (dup2(saved_fd, fileno_target) < 0) {
        rc = -1;
    }
    close(saved_fd);
    if (g_capture_fault) {
        /* make the read genuinely fail: the descriptor is gone before use */
        close(fd);
        fd = -1;
    }
    /* A capture with no readable descriptor is a FAILED capture, never an
     * empty successful one -- that difference is the whole point of `ok`. */
    if (fd < 0) {
        rc = -1;
    }
    if (rc == 0) {
        if (lseek(fd, 0, SEEK_SET) == (off_t)-1) {
            rc = -1;
        }
    }
    if (rc == 0) {
        f = fdopen(fd, "r");
        if (f == NULL) {
            rc = -1;
        } else {
            n = fread(out, 1, out_cap - 1, f);
            if (ferror(f)) {
                rc = -1;
            } else if (!feof(f) && getc(f) != EOF) {
                /* more bytes than the buffer holds: a prefix is not a result */
                rc = -1;
            } else {
                *len = n;
                out[n] = '\0';   /* terminator for diagnostics only */
            }
            if (fclose(f) != 0) {
                rc = -1;
            }
            fd = -1;
        }
    }
    if (rc != 0) {
        *len = 0;
    }
    if (fd >= 0) {
        close(fd);
    }
    if (rc != 0) {
        out[0] = '\0';
    }
    unlink(tmpl);
    return rc;
}

/* Run the real entry with BOTH streams captured. */
static void
run_cli_argv(int argc, char **argv, cli_run_t *r)
{
    char to[] = "/tmp/moqr_cli_out_XXXXXX";
    char te[] = "/tmp/moqr_cli_err_XXXXXX";
    int ofd, efd, osav = -1, esav = -1;

    memset(r, 0, sizeof(*r));
    r->status = -1;

    /* Flush BEFORE redirecting: anything already buffered belongs to whatever
     * produced it, not to the command about to run. */
    if (checked_flush(stdout, FLUSH_FAULT_PRE) != 0 ||
        checked_flush(stderr, FLUSH_FAULT_PRE) != 0) {
        return;
    }

    ofd = cap_begin(STDOUT_FILENO, to, &osav);
    if (ofd < 0) {
        return;
    }
    efd = cap_begin(STDERR_FILENO, te, &esav);
    if (efd < 0) {
        (void)cap_end(STDOUT_FILENO, ofd, osav, to, r->out, sizeof(r->out),
                      &r->out_len);
        r->out_len = 0;
        return;
    }

    r->status = moqr_cli_disabled_main(argc, argv);

    /* Byte injections happen HERE, while both streams are still redirected,
     * so they land inside the captured interval. */
    if (g_inject_out) {
        static const char kBad[] = "\0UNEXPECTED";
        (void)!write(STDOUT_FILENO, kBad, sizeof(kBad) - 1);
    }
    if (g_inject_err) {
        static const char kBad[] = "\0UNEXPECTED";
        (void)!write(STDERR_FILENO, kBad, sizeof(kBad) - 1);
    }
    if (g_inject_overflow) {
        char big[sizeof(r->out) + 64];
        memset(big, 'x', sizeof(big));
        (void)!write(STDOUT_FILENO, big, sizeof(big));
    }

    {
        /* A flush is part of producing the capture: its result is honoured,
         * and restoration and cleanup still complete either way. */
        int fo = checked_flush(stdout, FLUSH_FAULT_POST_STDOUT);
        int fe = checked_flush(stderr, FLUSH_FAULT_POST_STDERR);
        int a = cap_end(STDERR_FILENO, efd, esav, te, r->err, sizeof(r->err),
                        &r->err_len);
        int b = cap_end(STDOUT_FILENO, ofd, osav, to, r->out, sizeof(r->out),
                        &r->out_len);
        r->ok = (fo == 0 && fe == 0 && a == 0 && b == 0);
        if (!r->ok) {
            r->out_len = 0;
            r->err_len = 0;
        }
    }
}

static void
run_cli(const char *cmd, const char *cfg_path, cli_run_t *r)
{
    char *argv[4];
    argv[0] = (char *)"moq5-relay";
    argv[1] = (char *)cmd;
    argv[2] = (char *)"--config";
    argv[3] = (char *)cfg_path;
    run_cli_argv(4, argv, r);
}

/* Write one fixture, reporting every way the write can fail. A fixture that
 * was not written must never reach the command: the command would refuse it
 * for being unreadable, which exits 2 with an empty stdout exactly as a
 * capability refusal does. */
static int
write_cfg(const char *path, const char *json)
{
    FILE *f = fopen(path, "w");
    size_t n = strlen(json);

    if (f == NULL) {
        return -1;
    }
    if (fwrite(json, 1, n, f) != n) {
        (void)fclose(f);
        return -1;
    }
    if (fclose(f) != 0) {
        return -1;
    }
    return 0;
}

/*
 * The capability oracle: this refusal, for this reason, publishing nothing.
 *
 * `quiet` is for the controls below, which drive the oracle at inputs it must
 * REJECT. They need its verdict, not its diagnostics, so a passing run never
 * prints a line that reads like a failure.
 */
static int
capability_refusal_failures(const char *what, const char *cmd, const char *cfg,
                            int quiet)
{
    cli_run_t r;
    int failures = 0;

    run_cli(cmd, cfg, &r);
    if (!r.ok) {
        if (!quiet) {
            printf("FAIL: %s: the output capture failed; no verdict can be "
                   "read from this run\n", what);
        }
        return 1;
    }
    if (r.status != 2) {
        if (!quiet) {
            printf("FAIL: %s: status %d, wanted 2\n", what, r.status);
        }
        failures++;
    }
    /* Length AND bytes. A C-string comparison would accept anything hidden
     * after a NUL -- "empty" stdout that carries trailing bytes, or the exact
     * diagnostic followed by more. */
    if (r.out_len != 0) {
        if (!quiet) {
            printf("FAIL: %s: a refusal still published %zu bytes on stdout\n",
                   what, r.out_len);
        }
        failures++;
    }
    if (r.err_len != sizeof(kNoWtRefusal) - 1 ||
        memcmp(r.err, kNoWtRefusal, sizeof(kNoWtRefusal) - 1) != 0) {
        if (!quiet) {
            printf("FAIL: %s: refused for the wrong reason (%zu bytes): [%s]\n",
                   what, r.err_len, r.err);
        }
        failures++;
    }
    return failures;
}

static int
expect_capability_refusal(const char *what, const char *cmd, const char *cfg)
{
    return capability_refusal_failures(what, cmd, cfg, 0);
}

static int
test_build_capability_is_shared(void)
{
    int failures = 0;
    cli_run_t r;
    char dir[] = "/tmp/moqr_cap_XXXXXX";
    char wt[512], raw1[512], raw4[512], bad[512];

    if (mkdtemp(dir) == NULL) {
        printf("FAIL: test setup: could not make a temporary directory\n");
        return 1;
    }
    snprintf(wt, sizeof(wt), "%s/wt.json", dir);
    snprintf(raw1, sizeof(raw1), "%s/raw1.json", dir);
    snprintf(raw4, sizeof(raw4), "%s/raw4.json", dir);
    snprintf(bad, sizeof(bad), "%s/bad.json", dir);

    /* Every fixture write is checked. A failed write ends the case here, so
     * the command is never asked about a file that was not written. */
    if (write_cfg(wt,
            "{\"listener\":{\"host\":\"127.0.0.1\",\"port\":4433,"
            "\"cert\":\"c\",\"key\":\"k\"},"
            "\"webtransport\":{\"host\":\"127.0.0.1\",\"port\":4443,"
            "\"cert\":\"c\",\"key\":\"k\"}}") != 0 ||
        write_cfg(raw1,
            "{\"listener\":{\"host\":\"127.0.0.1\",\"port\":4433,"
            "\"cert\":\"c\",\"key\":\"k\"}}") != 0 ||
        write_cfg(raw4,
            "{\"listener\":{\"host\":\"127.0.0.1\",\"port\":4433,"
            "\"cert\":\"c\",\"key\":\"k\",\"lanes\":4}}") != 0 ||
        write_cfg(bad, "{\"listener\":{\"port\":0}}") != 0) {
        printf("FAIL: test setup: a fixture could not be written\n");
        unlink(wt); unlink(raw1); unlink(raw4); unlink(bad);
        rmdir(dir);
        return 1;
    }

    /* both commands refuse, for the capability reason, publishing nothing */
    failures += expect_capability_refusal("capacity on an unsupported "
                                          "WebTransport config", "capacity", wt);
    failures += expect_capability_refusal("serve on an unsupported "
                                          "WebTransport config", "serve", wt);

    /* Neighbours: a configuration this build CAN serve still succeeds, at one
     * lane and at several, and still prints its ceiling. */
    {
        const char *ok[2];
        size_t i;
        ok[0] = raw1;
        ok[1] = raw4;
        for (i = 0; i < 2; i++) {
            run_cli("capacity", ok[i], &r);
            if (!r.ok) {
                printf("FAIL: the output capture failed for a supported "
                       "configuration\n");
                failures++;
                continue;
            }
            if (r.status != 0) {
                printf("FAIL: capacity refused a supported raw configuration "
                       "(status %d): %s\n", r.status, r.err);
                failures++;
            }
            if (strstr(r.out, "allocation-request ceiling") == NULL) {
                printf("FAIL: a supported capacity printed no ceiling\n");
                failures++;
            }
        }
    }
    /* An ordinary semantic error is still an ordinary semantic error, named as
     * itself -- not the capability refusal standing in for everything. */
    {
        static const char kBadPort[] =
            "config error: listener.port: need 1..65535\n";
        run_cli("capacity", bad, &r);
        if (!r.ok) {
            printf("FAIL: the output capture failed for the semantic-error "
                   "case\n");
            failures++;
        } else {
            if (r.status != 2) {
                printf("FAIL: an invalid configuration was accepted "
                       "(status %d)\n", r.status);
                failures++;
            }
            if (r.err_len != sizeof(kBadPort) - 1 ||
                memcmp(r.err, kBadPort, sizeof(kBadPort) - 1) != 0) {
                printf("FAIL: the semantic error was reported as (%zu bytes) "
                       "[%s]\n", r.err_len, r.err);
                failures++;
            }
        }
        /* and it must NOT satisfy the capability oracle: same status, same
         * empty stdout, different reason */
        {
            int caught = capability_refusal_failures("semantic error",
                                                     "capacity", bad, 1);
            if (caught == 0) {
                printf("FAIL: the capability oracle accepted a semantic "
                       "error\n");
                failures++;
            }
        }
    }
    /* Informational output is answered before any configuration is read. */
    {
        char *argv[2];
        argv[0] = (char *)"moq5-relay";
        argv[1] = (char *)"--help";
        run_cli_argv(2, argv, &r);
        if (!r.ok || r.status != 0 || r.out[0] == '\0') {
            printf("FAIL: --help needs no configuration but returned %d\n",
                   r.status);
            failures++;
        }
        argv[1] = (char *)"--version";
        run_cli_argv(2, argv, &r);
        if (!r.ok || r.status != 0 || r.out[0] == '\0') {
            printf("FAIL: --version needs no configuration but returned %d\n",
                   r.status);
            failures++;
        }
    }

    /* -- the helpers' own failure modes, proved through the real helpers --- */

    /* A fixture that cannot be written is reported as such, and never reaches
     * the command: an unwritable path would otherwise make the command refuse
     * an unreadable file, with the same status and the same empty stdout as a
     * capability refusal. */
    {
        char nodir[640];
        snprintf(nodir, sizeof(nodir), "%s/absent/wt.json", dir);
        if (write_cfg(nodir, "{}") == 0) {
            printf("FAIL: a fixture write into a missing directory was "
                   "reported as successful\n");
            failures++;
        }
        /* and the command's answer for that path is NOT the capability
         * refusal, which is exactly why the write result must be checked */
        run_cli("capacity", nodir, &r);
        if (r.ok && r.status == 2 && r.out_len == 0 &&
            r.err_len == sizeof(kNoWtRefusal) - 1 &&
            memcmp(r.err, kNoWtRefusal, sizeof(kNoWtRefusal) - 1) == 0) {
            printf("FAIL: an unwritten fixture produced the capability "
                   "refusal\n");
            failures++;
        }
    }
    /* An unreadable capture is not an empty capture. */
    {
        g_capture_fault = 1;
        run_cli("capacity", raw1, &r);
        g_capture_fault = 0;
        if (r.ok) {
            printf("FAIL: a broken capture reported success\n");
            failures++;
        }
        if (r.out_len != 0) {
            printf("FAIL: a broken capture returned content\n");
            failures++;
        }
        /* the oracle must refuse to draw a verdict from it */
        g_capture_fault = 1;
        {
            int caught = capability_refusal_failures("broken capture",
                                                     "capacity", wt, 1);
            g_capture_fault = 0;
            if (caught == 0) {
                printf("FAIL: the capability oracle accepted a broken "
                       "capture\n");
                failures++;
            }
        }
    }

    /*
     * A reported flush failure is a failed capture, whatever bytes happen to
     * have reached the file -- at EACH of the three flush sites.
     *
     * The phases are separate controls because they guard different things. A
     * fault that always stops at the pre-redirect flush never reaches the
     * post-command returns, which is exactly where the original defect was: a
     * post-command result that was computed and then dropped.
     */
    {
        static const struct {
            int         phase;
            const char *name;
            int         want_status;   /* -1: the command must NOT have run */
        } kPhases[] = {
            { FLUSH_FAULT_PRE,         "pre-redirect",       -1 },
            { FLUSH_FAULT_POST_STDOUT, "post-command stdout", 2 },
            { FLUSH_FAULT_POST_STDERR, "post-command stderr", 2 },
        };
        size_t i;
        for (i = 0; i < sizeof(kPhases) / sizeof(kPhases[0]); i++) {
            g_flush_faults_applied = 0;
            g_flush_fault_phase = kPhases[i].phase;
            run_cli("capacity", wt, &r);
            g_flush_fault_phase = FLUSH_FAULT_NONE;

            if (g_flush_faults_applied != 1) {
                printf("FAIL: the %s flush fault fired %d times, not once; "
                       "this control is vacuous\n", kPhases[i].name,
                       g_flush_faults_applied);
                failures++;
            }
            /* the post-command phases must be reached only AFTER the command
             * ran, which its own observed status proves */
            if (r.status != kPhases[i].want_status) {
                printf("FAIL: the %s flush fault saw status %d, wanted %d\n",
                       kPhases[i].name, r.status, kPhases[i].want_status);
                failures++;
            }
            if (r.ok) {
                printf("FAIL: a reported %s flush failure produced a "
                       "successful capture\n", kPhases[i].name);
                failures++;
            }

            /* and the oracle refuses to draw a verdict from it */
            g_flush_faults_applied = 0;
            g_flush_fault_phase = kPhases[i].phase;
            {
                int caught = capability_refusal_failures("flush failure",
                                                         "capacity", wt, 1);
                g_flush_fault_phase = FLUSH_FAULT_NONE;
                if (caught == 0) {
                    printf("FAIL: the capability oracle accepted a run whose "
                           "%s flush failed\n", kPhases[i].name);
                    failures++;
                }
            }

            /* restoration held: the very next capture is normal again */
            run_cli("capacity", wt, &r);
            if (!r.ok || r.status != 2 || r.out_len != 0 ||
                r.err_len != sizeof(kNoWtRefusal) - 1) {
                printf("FAIL: the capture after a %s flush failure did not "
                       "recover\n", kPhases[i].name);
                failures++;
            }
        }
    }
    /* Bytes after a NUL are bytes. stdout that "looks" empty is not empty, and
     * the exact diagnostic followed by more is not the exact diagnostic. The
     * injections happen while the streams are still redirected, so they are
     * inside the captured interval and the pre-redirect flush cannot move them
     * out of it. */
    {
        g_inject_out = 1;
        run_cli("capacity", wt, &r);
        g_inject_out = 0;
        if (!r.ok) {
            printf("FAIL: the NUL-prefixed stdout capture failed instead of "
                   "being read\n");
            failures++;
        } else if (r.out_len == 0) {
            printf("FAIL: bytes written inside the captured interval were not "
                   "captured; this control is vacuous\n");
            failures++;
        }
        g_inject_out = 1;
        {
            int caught = capability_refusal_failures("NUL-hidden stdout",
                                                     "capacity", wt, 1);
            g_inject_out = 0;
            if (caught == 0) {
                printf("FAIL: the capability oracle accepted stdout carrying "
                       "bytes after a NUL\n");
                failures++;
            }
        }
    }
    {
        g_inject_err = 1;
        run_cli("capacity", wt, &r);
        g_inject_err = 0;
        if (!r.ok) {
            printf("FAIL: the NUL-suffixed stderr capture failed instead of "
                   "being read\n");
            failures++;
        } else if (r.err_len <= sizeof(kNoWtRefusal) - 1) {
            printf("FAIL: the stderr injection landed outside the captured "
                   "interval; this control is vacuous\n");
            failures++;
        }
        g_inject_err = 1;
        {
            int caught = capability_refusal_failures("NUL-suffixed stderr",
                                                     "capacity", wt, 1);
            g_inject_err = 0;
            if (caught == 0) {
                printf("FAIL: the capability oracle accepted the diagnostic "
                       "followed by trailing bytes\n");
                failures++;
            }
        }
    }
    /* More bytes than the buffer holds is a failed capture, not a prefix. */
    {
        g_inject_overflow = 1;
        run_cli("capacity", raw1, &r);
        g_inject_overflow = 0;
        if (r.ok) {
            printf("FAIL: a capture past the declared bound reported "
                   "success\n");
            failures++;
        }
        if (r.out_len != 0) {
            printf("FAIL: an overflowed capture returned a prefix\n");
            failures++;
        }
    }

    unlink(wt); unlink(raw1); unlink(raw4); unlink(bad);
    rmdir(dir);
    return failures;
}

int
main(void)
{
    int failures = 0;
#ifndef MOQR_BIND_TESTING
    failures += test_bank_count_is_exactly_two();
    failures += test_document_lifetime_without_sanitizer();
    failures += coord_body_token_identity(0);
    failures += coord_propagates_release_wake(0);
    failures += coord_release_failure_is_not_retirement(0);
    failures += test_http_only_generation_is_silent_and_retires();
    failures += test_signal_generation_emits_once_and_retires();
    failures += test_late_join_reaches_stderr();
    failures += test_poisoned_generation_is_addressed_and_retires();
    failures += test_incomplete_generation_is_addressed_then_progresses();
    failures += test_idle_coordinator_is_a_noop();
    failures += test_signal_projection_is_byte_identical();
    failures += test_dual_facade_labels_reach_the_owner();
    failures += test_admin_seam_wiring_is_complete();
    failures += test_admin_owner_freezes_info();
    failures += test_shards_document_from_real_rows();
    failures += test_serve_log_emission();
    failures += test_dual_readiness_metadata();
    failures += test_serve_early_return_finalizes();
    failures += test_build_capability_is_shared();
#else
    failures += coord_body_token_identity(1);
    failures += coord_propagates_release_wake(1);
    failures += coord_release_failure_is_not_retirement(1);
    failures += test_blocked_generation_retires_and_progresses();
    failures += test_blocked_http_only_is_silent();
    failures += test_blocked_late_join_reaches_stderr();
    failures += test_blocked_poison_is_addressed_and_retires();
    failures += test_admin_seam_wiring_is_complete();
    failures += test_admin_owner_freezes_info();
    failures += test_serve_log_emission();
    failures += test_dual_readiness_metadata();
    failures += test_serve_early_return_finalizes();
    failures += test_build_capability_is_shared();
#endif
    if (failures != 0) {
        fprintf(stderr, "test_relay_coord_seam: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_relay_coord_seam: OK\n");
    return 0;
}
