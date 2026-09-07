/* The deterministic admin state machine: generation identity, exact-once
 * release, reserve/commit, partial reads and writes, deadlines, overload and
 * cancellation.
 *
 * Every verdict is a function of an input transcript and an explicit clock.
 * Nothing sleeps, and no test asserts on elapsed wall time. */

#include <moqr_admin.h>

#include <stdio.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

/* The demand bits are opaque to the admin tier -- it carries them, it does
 * not interpret them -- so these pure-admin tests use a placeholder rather
 * than depending on the broker header. */
#define ADMIN_TEST_DEMAND 0x2u

#define OM MOQR_OBS_FMT_OPENMETRICS_100
#define PR MOQR_OBS_FMT_PROMETHEUS_004

#define BODY_CAP 256u
/* The immutable document the machine is constructed with. */
#define FX_INFO "{\"api\":\"v1\",\"fixture\":true}"

typedef struct {
    moqr_admin_t a;
    char         storage[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT][BODY_CAP];
} fixture_t;

static int
fx_init(fixture_t *f)
{
    int failures = 0;
    char *bodies[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT];
    size_t caps[MOQR_ADMIN_BODY__COUNT];
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            bodies[b][k] = f->storage[b][k];
        }
    }
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        caps[k] = BODY_CAP;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_init(&f->a, bodies, caps, FX_INFO,
                                          strlen(FX_INFO)), MOQR_OK);
    return failures;
}

/* reserve -> render -> commit, the whole producer side.
 *
 * A generation must already be JOINED: in production a request binds its serial
 * before the renderer is asked for anything, and commit now enforces that, so
 * the fixture follows the same order rather than inventing generations no
 * request asked for. */
static int
fx_generate(fixture_t *f, uint32_t bank, uint64_t serial, const char *doc)
{
    int failures = 0;
    size_t len[MOQR_ADMIN_BODY__COUNT];
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f->a, bank, serial), MOQR_OK);
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        size_t cap = 0;
        char *dst = moqr_admin_bank_storage(&f->a, bank, serial, k, &cap);
        MOQ_TEST_CHECK(dst != NULL);
        if (dst == NULL) {
            return failures;
        }
        memcpy(dst, doc, strlen(doc));
        len[k] = strlen(doc);
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_commit(&f->a, bank, serial, len, 0), MOQR_OK);
    return failures;
}

/* Drive one client from bytes to a bound serial. */
static int
fx_request(fixture_t *f, uint32_t *c, const char *req, uint64_t serial,
           uint64_t now)
{
    int failures = 0;
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f->a, now, c), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_bytes(&f->a, *c, req, strlen(req), now),
                          MOQR_OK);
    if (serial != 0) {
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_bind_serial(&f->a, *c, serial, now),
                              MOQR_OK);
    }
    return failures;
}

/* A loop that stops spinning but returns normally still lets a nonterminating
 * path exit zero. Every bounded loop records an overrun here, and main() fails
 * the run on it. */
static int loop_overruns = 0;

static size_t
fx_drain(fixture_t *f, uint32_t c, char *out, size_t cap, uint64_t now)
{
    size_t total = 0;
    const char *span;
    size_t n;
    /* Bounded: a response is a fixed head plus one body, so a drain that does
     * not terminate is a defect to report, never something to spin on. */
    uint32_t guard = 0;
    while (moqr_admin_pending(&f->a, c, &span, &n)) {
        if (++guard > 4096u) {
            printf("  fx_drain did not terminate\n");
            loop_overruns++;
            break;
        }
        if (n > cap - total - 1u) {
            n = cap - total - 1u;
        }
        if (n == 0) {
            break;
        }
        memcpy(out + total, span, n);
        total += n;
        if (moqr_admin_on_written(&f->a, c, n, now) != MOQR_OK) {
            printf("  on_written refused a span it had just offered\n");
            break;
        }
    }
    out[total] = '\0';
    return total;
}

/* Settle every owed release the way Step 4 does: peek, "broker accepts", ack. */
static uint32_t
fx_tokens(fixture_t *f, moqr_admin_release_t *out, uint32_t cap)
{
    uint32_t n = 0;
    moqr_admin_release_t r;
    while (moqr_admin_peek_release(&f->a, &r)) {
        if (n < cap) {
            out[n] = r;
        }
        n++;
        if (n > MOQR_ADMIN_BANKS) {
            printf("  more release tokens than banks\n");
            loop_overruns++;
            break;
        }
        if (moqr_admin_ack_release(&f->a, r.serial, r.bank) != MOQR_OK) {
            printf("  the admin refused to acknowledge a token it offered\n");
            break;
        }
        if (n > MOQR_ADMIN_BANKS * 8u) {
            printf("  peek_release did not terminate\n");
            break;
        }
    }
    return n;
}

/* Bind a client to `serial` so a generation legitimately exists, for cases
 * whose subject is the bank rather than the request. */
static int
fx_hold(fixture_t *f, uint64_t serial, uint32_t *out_c)
{
    int failures = 0;
    static const char *req =
        "GET /metrics HTTP/1.1\r\nHost: x\r\n"
        "Accept: application/openmetrics-text\r\n\r\n";
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f->a, 0, out_c), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&f->a, *out_c, req, strlen(req), 0), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_bind_serial(&f->a, *out_c, serial, 0),
                          MOQR_OK);
    return failures;
}

/* Stands in for the owner's broker release. The point of the coupled call is
 * that the settlement cannot be separated from it, so a test callback that
 * simply succeeds still exercises the transaction shape. */
static moqr_result_t
ok_release(void *ctx, uint64_t serial, uint32_t demand, uint32_t bank)
{
    (void)ctx; (void)serial; (void)demand; (void)bank;
    return MOQR_OK;
}

/* Counts invocations, so the test can prove the release is attempted exactly
 * once on success and not at all when the settle is refused. */
static int  release_calls = 0;
static bool release_should_fail = true;

static moqr_result_t
counting_release(void *ctx, uint64_t serial, uint32_t demand, uint32_t bank)
{
    (void)ctx; (void)serial; (void)demand; (void)bank;
    release_calls++;
    return release_should_fail ? MOQR_ERR_WOULD_BLOCK : MOQR_OK;
}

/* Retire a claim the way an owner must: claim, record the take the broker
 * performed, then acknowledge. Acknowledging without a recorded take is
 * refused, so a helper that skipped the take would not settle anything. */
static bool
claim_and_ack(moqr_admin_t *a, uint64_t *out)
{
    moqr_admin_retire_t r;
    if (!moqr_admin_claim_abandon(a, &r)) {
        return false;
    }
    *out = r.serial;
    if (!r.taken) {
        (void)moqr_admin_record_take(a, r.serial, ADMIN_TEST_DEMAND, 0);
    }
    (void)moqr_admin_settle_abandon(a, r.serial, ok_release, NULL);
    return true;
}

static const char *GET_OM =
    "GET /metrics HTTP/1.1\r\nHost: x\r\n"
    "Accept: application/openmetrics-text\r\n\r\n";

/* ---- F1: generation identity ------------------------------------------- */

/* The defect: with generation 1 pinned by a slow client and generation 2 also
 * ready, a request that joined 2 was handed 1 because bank 0 is scanned first.
 * A waiter must attach only to the bank carrying ITS serial. */
static int
test_exact_serial_routing(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t slow, late;
    char out[1024];

    failures += fx_init(&f);
    /* Both requests join their own generation first -- the production order --
     * and only then is anything rendered for either. */
    failures += fx_request(&f, &slow, GET_OM, 11, 0);
    failures += fx_request(&f, &late, GET_OM, 22, 0);
    failures += fx_generate(&f, 0, 11, "moqrelay_gen 11\n");
    failures += fx_generate(&f, 1, 22, "moqrelay_gen 22\n");

    /* A slow reader takes generation 11 on bank 0. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);

    if (f.a.client[late].serial != 22u) {
        printf("  a request bound to 22 was rerouted to %llu\n",
               (unsigned long long)f.a.client[late].serial);
        failures++;
    }
    (void)fx_drain(&f, late, out, sizeof(out), 0);
    if (strstr(out, "moqrelay_gen 22") == NULL) {
        printf("  the later request did not receive its own generation\n");
        failures++;
    }
    if (strstr(out, "moqrelay_gen 11") != NULL) {
        printf("  the later request received a stale generation\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* A serial is only ever assigned by the caller, from the broker. */
static int
test_no_serial_no_delivery(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    const char *sp;
    size_t n;

    failures += fx_init(&f);
    /* Another request owns generation 7, so it legitimately exists -- a bank
     * is only ever rendered for a generation some request joined. */
    {
        uint32_t owner;
        failures += fx_hold(&f, 7, &owner);
    }
    failures += fx_generate(&f, 0, 7, "moqrelay_x 1\n");
    failures += fx_request(&f, &c, GET_OM, 0, 0);   /* deliberately unbound */
    if (f.a.client[c].state != MOQR_ADMIN_CS_PARSED) {
        printf("  a parsed request did not park awaiting a generation\n");
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
    if (moqr_admin_pending(&f.a, c, &sp, &n)) {
        printf("  an unassigned request was served a generation\n");
        failures++;
    }
    /* Zero is never an identity. */
    if (moqr_admin_bind_serial(&f.a, c, 0, 0) != MOQR_ERR_INVAL) {
        printf("  serial zero was accepted as a generation identity\n");
        failures++;
    }
    /* next_demand surfaces exactly this client. */
    {
        uint32_t d = 0xffffffffu;
        if (!moqr_admin_next_demand(&f.a, &d) || d != c) {
            printf("  next_demand did not surface the parked request\n");
            failures++;
        }
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* A broker refusal answers THAT request 503 at once -- nothing is coming. */
static int
test_refuse_is_immediate(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    char out[1024];

    failures += fx_init(&f);
    failures += fx_request(&f, &c, GET_OM, 0, 0);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_refuse(&f.a, c, MOQR_HTTP_503, 0),
                          MOQR_OK);
    (void)fx_drain(&f, c, out, sizeof(out), 0);
    if (strstr(out, "503") == NULL || strstr(out, "Retry-After: 1") == NULL) {
        printf("  a broker refusal was not answered 503 immediately\n");
        failures++;
    }
    if (f.a.client[c].pinned) {
        printf("  a refusal pinned a bank\n");
        failures++;
    }
    /* A refusal is never a success. */
    moqr_admin_destroy(&f.a);
    return failures;
}

/* Every generation that reaches a bank yields exactly one release token. */
static int
test_exact_once_release(void)
{
    int failures = 0;
    moqr_admin_release_t tok[8];
    uint32_t n;

    /* delivered */
    {
        fixture_t f;
        uint32_t c;
        char out[1024];
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 5, 0);
        failures += fx_generate(&f, 0, 5, "moqrelay_d 1\n");
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
        (void)fx_drain(&f, c, out, sizeof(out), 0);
        n = fx_tokens(&f, tok, 8);
        if (n != 1u || tok[0].serial != 5u || tok[0].bank != 0u) {
            printf("  delivery produced %u tokens (want 1 for serial 5)\n", n);
            failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, c), MOQR_OK);
        if (fx_tokens(&f, tok, 8) != 0u) {
            printf("  closing after delivery produced a second token\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
        if (fx_tokens(&f, tok, 8) != 0u) {
            printf("  teardown produced a token already emitted\n");
            failures++;
        }
    }
    /* aborted */
    {
        fixture_t f;
        failures += fx_init(&f);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 1, 9), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_abort(&f.a, 1, 9), MOQR_OK);
        n = fx_tokens(&f, tok, 8);
        if (n != 1u || tok[0].serial != 9u || tok[0].bank != 1u) {
            printf("  abort produced %u tokens (want 1 for serial 9)\n", n);
            failures++;
        }
        if (moqr_admin_abort(&f.a, 1, 9) == MOQR_OK) {
            printf("  a second abort of the same reservation succeeded\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    /* Bound, then the waiter leaves before the generation is delivered: the
     * bank owes its release at that exact event, not on a later scan. */
    {
        fixture_t f;
        uint32_t c;
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 3, 0);
        failures += fx_generate(&f, 0, 3, "moqrelay_n 1\n");
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, c), MOQR_OK);
        n = fx_tokens(&f, tok, 8);
        if (n != 1u || tok[0].serial != 3u) {
            printf("  a generation whose last waiter left produced %u "
                   "tokens\n", n);
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    /* Reserved but never committed -- the signal-only shape -- is not
     * discarded by a tick, and teardown settles it exactly once. */
    {
        fixture_t f;
        failures += fx_init(&f);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 0, 31), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 1), MOQR_OK);
        if (fx_tokens(&f, tok, 8) != 0u) {
            printf("  a tick discarded a bank a renderer still held\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
        n = fx_tokens(&f, tok, 8);
        if (n != 1u || tok[0].serial != 31u) {
            printf("  teardown did not settle the reserved bank\n");
            failures++;
        }
    }
    /* torn down while still held */
    {
        fixture_t f;
        uint32_t c;
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 4, 0);
        failures += fx_generate(&f, 0, 4, "moqrelay_t 1\n");
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
        moqr_admin_destroy(&f.a);
        n = fx_tokens(&f, tok, 8);
        if (n != 1u || tok[0].serial != 4u) {
            printf("  teardown of a held generation produced %u tokens\n", n);
            failures++;
        }
    }
    return failures;
}

/* ---- F2: reserve / render / commit -------------------------------------- */

static int
test_commit_protocol(void)
{
    int failures = 0;
    fixture_t f;
    size_t len[MOQR_ADMIN_BODY__COUNT];
    char before[BODY_CAP];
    uint32_t holder;

    failures += fx_init(&f);
    /* A request joined serial 1 first: a commit for a serial no request asked
     * for has no reader and is refused. */
    failures += fx_hold(&f, 1, &holder);
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        len[k] = 4;
    }

    /* Storage is unreachable until reserved, and only under its own serial. */
    if (moqr_admin_bank_storage(&f.a, 0, 1, OM, NULL) != NULL) {
        printf("  storage was reachable before reservation\n");
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 0, 1), MOQR_OK);
    if (moqr_admin_bank_storage(&f.a, 0, 2, OM, NULL) != NULL) {
        printf("  storage was reachable under the wrong serial\n");
        failures++;
    }
    if (moqr_admin_bank_storage(&f.a, 0, 1, MOQR_ADMIN_BODY__COUNT, NULL) != NULL) {
        printf("  storage was reachable for an out-of-range format\n");
        failures++;
    }

    /* Serial zero is never a generation. */
    if (moqr_admin_reserve(&f.a, 1, 0) != MOQR_ERR_INVAL) {
        printf("  a bank was reserved for serial zero\n");
        failures++;
    }
    /* A reserved bank cannot be reserved again. */
    if (moqr_admin_reserve(&f.a, 0, 2) != MOQR_ERR_WOULD_BLOCK) {
        printf("  a reserved bank was reserved a second time\n");
        failures++;
    }
    /* Committing under a different serial than was reserved is refused. */
    if (moqr_admin_commit(&f.a, 0, 2, len, 0) != MOQR_ERR_INVAL) {
        printf("  a commit succeeded under the wrong serial\n");
        failures++;
    }
    /* A refused commit mutates nothing, including earlier formats. */
    {
        size_t bad[MOQR_ADMIN_BODY__COUNT];
        /* The first format validated must PASS and a later one FAIL, or a
         * validate-then-store-per-format loop is indistinguishable from
         * validate-all-then-store. PROMETHEUS_004 is enum 0, so it goes first. */
        bad[PR] = 4;
        bad[OM] = BODY_CAP + 1u;
        memcpy(before, (const char *)&f.a.bank[0].body_len[0], sizeof(size_t));
        if (moqr_admin_commit(&f.a, 0, 1, bad, 0) != MOQR_ERR_CAPACITY) {
            printf("  an over-capacity commit was accepted\n");
            failures++;
        }
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            if (f.a.bank[0].body_len[k] != 0u) {
                printf("  a refused commit stored format %u's length\n", k);
                failures++;
            }
        }
        if (f.a.bank[0].state != MOQR_ADMIN_BS_RESERVED) {
            printf("  a refused commit changed the bank state\n");
            failures++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_commit(&f.a, 0, 1, len, 0), MOQR_OK);
    /* A committed, undelivered generation is not overwritable. */
    if (moqr_admin_reserve(&f.a, 0, 3) != MOQR_ERR_WOULD_BLOCK) {
        printf("  a ready but unpinned bank was reserved out from under its "
               "generation\n");
        failures++;
    }
    if (moqr_admin_commit(&f.a, 0, 1, len, 0) != MOQR_ERR_WRONG_STATE) {
        printf("  a committed bank accepted a second commit\n");
        failures++;
    }
    if (moqr_admin_bank_storage(&f.a, 0, 1, OM, NULL) != NULL) {
        printf("  storage stayed reachable after commit\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* A bank rendered for demand that is not an HTTP request has no reader here.
 * Committing it would leave it READY forever, so the commit is refused and the
 * closed transition is: render under RESERVED, read it synchronously, abort. */
static int
test_signal_only_path(void)
{
    int failures = 0;
    fixture_t f;
    size_t len[MOQR_ADMIN_BODY__COUNT];
    moqr_admin_release_t tok[4];

    failures += fx_init(&f);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 0, 900), MOQR_OK);
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        size_t cap = 0;
        char *dst = moqr_admin_bank_storage(&f.a, 0, 900, k, &cap);
        MOQ_TEST_CHECK(dst != NULL);
        if (dst != NULL) {
            memcpy(dst, "SIG\n", 4);
        }
        len[k] = 4;
    }
    if (moqr_admin_commit(&f.a, 0, 900, len, 0) != MOQR_ERR_WRONG_STATE) {
        printf("  a bank with no HTTP waiter was committed\n");
        failures++;
    }
    if (f.a.bank[0].state != MOQR_ADMIN_BS_RESERVED) {
        printf("  the refused commit changed the bank state\n");
        failures++;
    }
    /* The signal sink reads it here, while the bank is still RESERVED. */
    MOQ_TEST_CHECK(moqr_admin_bank_storage(&f.a, 0, 900, OM, NULL) != NULL);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_abort(&f.a, 0, 900), MOQR_OK);
    if (fx_tokens(&f, tok, 4) != 1u || tok[0].serial != 900u) {
        printf("  the signal-only generation did not settle exactly once\n");
        failures++;
    }
    if (!moqr_admin_bank_available(&f.a)) {
        printf("  the bank was not reusable after the signal-only path\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* ---- F3: one request per connection ------------------------------------- */

static int
test_trailing_bytes(void)
{
    int failures = 0;
    const char *two =
        "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\nGET /metrics HTTP/1.1\r\n\r\n";
    const char *body_after =
        "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\nxxxx";

    /* In one read. */
    {
        fixture_t f;
        uint32_t c;
        char out[1024];
        failures += fx_init(&f);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&f.a, c, two, strlen(two), 0), MOQR_OK);
        if (f.a.client[c].state == MOQR_ADMIN_CS_PARSED ||
            f.a.client[c].state == MOQR_ADMIN_CS_WAITING) {
            printf("  a pipelined second request was admitted\n");
            failures++;
        }
        (void)fx_drain(&f, c, out, sizeof(out), 0);
        if (strstr(out, "400") == NULL) {
            printf("  trailing bytes in one read were not 400\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    /* Split across reads, one trailing byte at a time. */
    {
        fixture_t f;
        uint32_t c;
        char out[1024];
        const char *head = "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n";
        failures += fx_init(&f);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
        for (size_t i = 0; i < strlen(head); i++) {
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_on_bytes(&f.a, c, head + i, 1, 0), MOQR_OK);
        }
        /* The head is complete and valid; a later byte must not be absorbed. */
        /* A byte after a complete head ends the request. The call reports
         * MOQR_OK because it CONSTRUCTED a response the caller must drain. */
        if (moqr_admin_on_bytes(&f.a, c, "x", 1, 0) != MOQR_OK) {
            printf("  a byte after a complete head was absorbed\n");
            failures++;
        }
        if (f.a.client[c].status != MOQR_HTTP_400) {
            printf("  a byte after a complete head did not produce 400\n");
            failures++;
        }
        (void)fx_drain(&f, c, out, sizeof(out), 0);
        moqr_admin_destroy(&f.a);
    }
    /* An unframed body in the same read. */
    {
        fixture_t f;
        uint32_t c;
        char out[1024];
        failures += fx_init(&f);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&f.a, c, body_after, strlen(body_after), 0),
            MOQR_OK);
        (void)fx_drain(&f, c, out, sizeof(out), 0);
        if (strstr(out, "400") == NULL) {
            printf("  an unframed body was not 400\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    /* No generation is ever requested on this path. */
    {
        fixture_t f;
        uint32_t c, d;
        failures += fx_init(&f);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&f.a, c, two, strlen(two), 0), MOQR_OK);
        if (moqr_admin_next_demand(&f.a, &d)) {
            printf("  a rejected pipelined request still demanded a "
                   "generation\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    return failures;
}

/* ---- F4: deadlines and shutdown ----------------------------------------- */

static int
test_zero_progress_does_not_refresh(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    uint64_t d0;

    failures += fx_init(&f);
    failures += fx_request(&f, &c, GET_OM, 1, 0);
    failures += fx_generate(&f, 0, 1, "moqrelay_zp 1\n");
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
    d0 = f.a.client[c].dl_start_us;

    for (int i = 0; i < 5; i++) {
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_written(&f.a, c, 0, 1000u * i),
                              MOQR_OK);
    }
    if (f.a.client[c].dl_start_us != d0) {
        printf("  a zero-byte write refreshed the deadline\n");
        failures++;
    }
    /* The bank is therefore reclaimable at the deadline. */
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_tick(&f.a, MOQR_ADMIN_WRITE_DEADLINE_US + 1), MOQR_OK);
    if (f.a.client[c].pinned || f.a.bank[0].pins != 0) {
        printf("  a zero-progress writer retained its bank\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* A 503 produced BY the epoch deadline must not inherit that expired deadline
 * and be dropped by the very next tick. */
static int
test_timeout_error_survives_next_tick(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    char out[1024];
    uint64_t t = MOQR_ADMIN_EPOCH_DEADLINE_US + 1u;

    failures += fx_init(&f);
    failures += fx_request(&f, &c, GET_OM, 42, 0);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, t), MOQR_OK);
    if (f.a.client[c].state != MOQR_ADMIN_CS_WRITING) {
        printf("  the epoch timeout did not produce a response\n");
        failures++;
    }
    /* A second tick at the same instant must not discard it. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, t), MOQR_OK);
    (void)fx_drain(&f, c, out, sizeof(out), t);
    if (strstr(out, "503") == NULL) {
        printf("  the timeout 503 was dropped before it could be written\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

static int
test_cancel_states(void)
{
    int failures = 0;

    /* Before any head bytes: 503. */
    {
        fixture_t f;
        uint32_t c;
        char out[1024];
        failures += fx_init(&f);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
        moqr_admin_cancel(&f.a, 0);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 1), MOQR_OK);
        (void)fx_drain(&f, c, out, sizeof(out), 1);
        if (strstr(out, "503") == NULL) {
            printf("  cancelling a reading client did not answer 503\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    /* Waiting on a generation: 503. */
    {
        fixture_t f;
        uint32_t c;
        char out[1024];
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 8, 0);
        moqr_admin_cancel(&f.a, 0);
        (void)fx_drain(&f, c, out, sizeof(out), 0);
        if (strstr(out, "503") == NULL) {
            printf("  cancelling a waiter did not answer 503\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    /* Mid-response: dropped, not rewritten, and the pin released at once. */
    {
        fixture_t f;
        uint32_t c;
        const char *sp;
        size_t n;
        moqr_admin_release_t tok[4];
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 6, 0);
        failures += fx_generate(&f, 0, 6, "moqrelay_c 1\n");
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
        MOQ_TEST_CHECK(moqr_admin_pending(&f.a, c, &sp, &n));
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_written(&f.a, c, 8, 0), MOQR_OK);

        moqr_admin_cancel(&f.a, 0);
        if (f.a.client[c].pinned || f.a.bank[0].pins != 0) {
            printf("  cancellation left a writer holding its bank\n");
            failures++;
        }
        if (moqr_admin_pending(&f.a, c, &sp, &n)) {
            printf("  a cancelled writer still had bytes to send\n");
            failures++;
        }
        if (f.a.client[c].status == MOQR_HTTP_503) {
            printf("  a partly sent 200 was rewritten as 503\n");
            failures++;
        }
        if (fx_tokens(&f, tok, 4) != 1u) {
            printf("  cancelling a writer did not release its token once\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    return failures;
}

/* Deadlines are a start and a budget, compared as elapsed unsigned time. That
 * is exact across the whole range including wrap: a clock near the top of its
 * range neither expires everything at once nor makes a client immortal. */
static int
test_deadline_boundaries(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    const char *sp;
    size_t n;
    uint64_t huge = UINT64_MAX - 3u;

    failures += fx_init(&f);
    failures += fx_request(&f, &c, GET_OM, 2, huge);
    failures += fx_generate(&f, 0, 2, "moqrelay_b 1\n");
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, huge), MOQR_OK);
    if (!moqr_admin_pending(&f.a, c, &sp, &n)) {
        printf("  a near-UINT64_MAX clock expired a fresh request\n");
        failures++;
    }
    /* The budget is held as start + elapsed, so a clock at the top of its
     * range is neither immortal nor instantly expired. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, huge + 3u), MOQR_OK);
    if (!moqr_admin_pending(&f.a, c, &sp, &n)) {
        printf("  a request expired three microseconds into a ten-second "
               "budget\n");
        failures++;
    }
    /* And it DOES expire once the budget is spent, even across the wrap. */
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_tick(&f.a, huge + MOQR_ADMIN_WRITE_DEADLINE_US + 4u),
        MOQR_OK);
    if (f.a.client[c].state == MOQR_ADMIN_CS_WRITING) {
        printf("  a request at the clock edge became immortal\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);

    /* The boundary is strict: exactly at the deadline is not yet expired. */
    {
        fixture_t g;
        uint32_t d;
        failures += fx_init(&g);
        failures += fx_request(&g, &d, GET_OM, 3, 0);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_tick(&g.a, MOQR_ADMIN_EPOCH_DEADLINE_US), MOQR_OK);
        if (g.a.client[d].state != MOQR_ADMIN_CS_WAITING) {
            printf("  a request expired exactly at its deadline\n");
            failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_tick(&g.a, MOQR_ADMIN_EPOCH_DEADLINE_US + 1u), MOQR_OK);
        if (g.a.client[d].state != MOQR_ADMIN_CS_WRITING) {
            printf("  a request did not expire one tick past its deadline\n");
            failures++;
        }
        moqr_admin_destroy(&g.a);
    }
    return failures;
}

/* ---- the behaviours checkpoint 3 already had ---------------------------- */

static int
test_partial_read_and_write(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    char out[1024];
    size_t total = 0;
    const char *span;
    size_t n;

    failures += fx_init(&f);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
    for (size_t i = 0; i < strlen(GET_OM); i++) {
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_bytes(&f.a, c, GET_OM + i, 1, 0),
                              MOQR_OK);
        if (i + 1 < strlen(GET_OM) && moqr_admin_pending(&f.a, c, &span, &n)) {
            printf("  answered after %zu of %zu bytes\n", i + 1,
                   strlen(GET_OM));
            failures++;
            break;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_bind_serial(&f.a, c, 1, 0), MOQR_OK);
    failures += fx_generate(&f, 0, 1, "moqrelay_alpha 7\n");
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);

    while (moqr_admin_pending(&f.a, c, &span, &n)) {
        if (total >= sizeof(out) - 1u) {
            printf("  byte-at-a-time drain did not terminate\n");
            loop_overruns++;
            failures++;
            break;
        }
        out[total++] = span[0];
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_written(&f.a, c, 1, 0), MOQR_OK);
        if (total >= sizeof(out) - 1) {
            break;
        }
    }
    out[total] = '\0';
    if (strstr(out, "200 OK") == NULL ||
        strstr(out, "moqrelay_alpha 7") == NULL) {
        printf("  byte-at-a-time request/response did not reassemble\n");
        failures++;
    }
    if (strstr(out, "moqrelay_alpha 7\nmoqrelay") != NULL) {
        printf("  body was duplicated across resumes\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

static int
test_pins_and_banks(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    const char *span;
    size_t n;

    failures += fx_init(&f);
    if (!moqr_admin_bank_available(&f.a)) {
        printf("  a fresh admin reports no free bank\n");
        failures++;
    }
    failures += fx_request(&f, &c, GET_OM, 1, 0);
    failures += fx_generate(&f, 0, 1, "moqrelay_x 1\n");
    if (!moqr_admin_bank_available(&f.a)) {
        printf("  one occupied bank exhausted both\n");
        failures++;
    }
    {
        uint32_t other;
        failures += fx_hold(&f, 2, &other);
        failures += fx_generate(&f, 1, 2, "moqrelay_y 2\n");
    }
    if (moqr_admin_bank_available(&f.a)) {
        printf("  a third generation was admitted with both banks occupied\n");
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
    MOQ_TEST_CHECK(moqr_admin_pending(&f.a, c, &span, &n));
    if (!f.a.client[c].pinned || f.a.bank[0].pins != 1u) {
        printf("  the bank being written from was not pinned exactly once\n");
        failures++;
    }
    if (moqr_admin_on_written(&f.a, c, n + 4096u, 0) != MOQR_ERR_INVAL) {
        printf("  an impossible written count was accepted\n");
        failures++;
    }
    {
        char out[1024];
        (void)fx_drain(&f, c, out, sizeof(out), 0);
        if (f.a.client[c].pinned || f.a.bank[0].pins != 0u) {
            printf("  the pin outlived a fully written response\n");
            failures++;
        }
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

static int
test_slow_loris_and_capacity(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;

    failures += fx_init(&f);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_bytes(&f.a, c, "GET /me", 7, 0),
                          MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_tick(&f.a, MOQR_ADMIN_READ_DEADLINE_US + 1), MOQR_OK);
    if (f.a.client[c].state == MOQR_ADMIN_CS_READING) {
        printf("  a slow-loris connection outlived its read deadline\n");
        failures++;
    }
    {
        const char *sp;
        size_t n;
        if (moqr_admin_pending(&f.a, c, &sp, &n)) {
            printf("  a slow-loris connection was answered at all\n");
            failures++;
        }
    }
    moqr_admin_destroy(&f.a);

    {
        fixture_t g;
        uint32_t d;
        failures += fx_init(&g);
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            if (moqr_admin_accept(&g.a, 0, &d) != MOQR_OK) {
                printf("  slot %u of the fixed capacity was refused\n", i);
                failures++;
            }
        }
        if (moqr_admin_accept(&g.a, 0, &d) != MOQR_ERR_CAPACITY) {
            printf("  capacity was exceeded rather than refused\n");
            failures++;
        }
        moqr_admin_cancel(&g.a, 0);
        if (moqr_admin_accept(&g.a, 1, &d) == MOQR_OK) {
            printf("  a connection was admitted after cancellation\n");
            failures++;
        }
        moqr_admin_destroy(&g.a);
    }
    return failures;
}

static int
test_format_selects_body(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    char out[1024];
    size_t len[MOQR_ADMIN_BODY__COUNT];
    const char *req_pr =
        "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: text/plain\r\n\r\n";

    failures += fx_init(&f);
    failures += fx_request(&f, &c, req_pr, 1, 0);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 0, 1), MOQR_OK);
    memcpy(moqr_admin_bank_storage(&f.a, 0, 1, OM, NULL), "OMBODY\n", 7);
    memcpy(moqr_admin_bank_storage(&f.a, 0, 1, PR, NULL), "PRBODY\n", 7);
    len[OM] = 7;
    len[PR] = 7;
    len[MOQR_ADMIN_BODY_SHARDS] = 0;
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_commit(&f.a, 0, 1, len, 0), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
    (void)fx_drain(&f, c, out, sizeof(out), 0);
    if (strstr(out, "PRBODY") == NULL || strstr(out, "OMBODY") != NULL) {
        printf("  the Prometheus request did not receive the Prometheus body\n");
        failures++;
    }
    if (strstr(out, MOQR_ADMIN_CT_PROMETHEUS) == NULL) {
        printf("  the Prometheus response lacked its content type\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

static int
test_error_needs_no_generation(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    char out[1024];
    const char *bad = "POST /metrics HTTP/1.1\r\nHost: x\r\n\r\n";

    failures += fx_init(&f);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_bytes(&f.a, c, bad, strlen(bad), 0),
                          MOQR_OK);
    if (f.a.client[c].pinned) {
        printf("  an error response pinned a bank before sending\n");
        failures++;
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (f.a.bank[b].pins != 0) {
            printf("  an error response left bank %u pinned\n", b);
            failures++;
        }
    }
    (void)fx_drain(&f, c, out, sizeof(out), 0);
    if (strstr(out, "405") == NULL || strstr(out, "Allow: GET") == NULL) {
        printf("  a bad method was not answered 405 immediately\n");
        failures++;
    }
    if (strstr(out, "moqrelay_") != NULL) {
        printf("  an error response carried metrics bytes\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

static int
test_oversized_head(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    char big[MOQR_ADMIN_MAX_REQUEST + 64];
    char out[1024];

    failures += fx_init(&f);
    memset(big, 'x', sizeof(big));
    memcpy(big, "GET /metrics HTTP/1.1\r\nHost: x\r\nX: ", 26);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
    (void)moqr_admin_on_bytes(&f.a, c, big, sizeof(big), 0);
    if (f.a.client[c].req_len > MOQR_ADMIN_MAX_REQUEST) {
        printf("  an oversized head was buffered beyond the bound\n");
        failures++;
    }
    (void)fx_drain(&f, c, out, sizeof(out), 0);
    if (strstr(out, "431") == NULL) {
        printf("  an oversized head was not refused 431\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* The sample/recheck predicate. What is actually proven is the level-triggered
 * property: a commit landing after the sample is still observable afterwards,
 * which is exactly what a flag consumed between check and wait would lose. */
static int
test_publish_predicate(void)
{
    int failures = 0;
    fixture_t f;
    uint64_t e0;

    failures += fx_init(&f);
    {
        uint32_t h1, h2;
        failures += fx_hold(&f, 1, &h1);
        failures += fx_hold(&f, 2, &h2);
    }
    e0 = moqr_admin_publish_epoch(&f.a);
    if (moqr_admin_publish_since(&f.a, e0)) {
        printf("  the predicate fired with no publication -- a spinning "
               "wait loop\n");
        failures++;
    }
    /* A commit between the sample and the wait. */
    failures += fx_generate(&f, 0, 1, "moqrelay_p 1\n");
    if (!moqr_admin_publish_since(&f.a, e0)) {
        printf("  a commit after the sample was not observable\n");
        failures++;
    }
    /* Still observable on a later re-read: the epoch is level-triggered, so
     * nothing consumes it. */
    if (!moqr_admin_publish_since(&f.a, e0)) {
        printf("  the epoch was consumed by reading it\n");
        failures++;
    }
    /* A fresh sample rearms. */
    {
        uint64_t e1 = moqr_admin_publish_epoch(&f.a);
        if (moqr_admin_publish_since(&f.a, e1)) {
            printf("  a fresh sample already reported a publication\n");
            failures++;
        }
        failures += fx_generate(&f, 1, 2, "moqrelay_p 2\n");
        if (!moqr_admin_publish_since(&f.a, e1)) {
            printf("  a second commit did not advance the epoch\n");
            failures++;
        }
    }
    /* An aborted reservation publishes nothing. */
    {
        uint64_t e2;
        moqr_admin_release_t tk[8];
        (void)fx_tokens(&f, tk, 8);
        moqr_admin_destroy(&f.a);
        failures += fx_init(&f);
        e2 = moqr_admin_publish_epoch(&f.a);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 0, 5), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_abort(&f.a, 0, 5), MOQR_OK);
        if (moqr_admin_publish_since(&f.a, e2)) {
            printf("  an aborted reservation advanced the publication epoch\n");
            failures++;
        }
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* An out-of-range format on a client must be refused at the seam that would
 * otherwise index the body array. The parser cannot produce one, so it is
 * placed directly -- the guard exists for the invariant, not for the parser. */
static int
test_bad_format_is_refused(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    char out[1024];

    failures += fx_init(&f);
    failures += fx_request(&f, &c, GET_OM, 1, 0);
    failures += fx_generate(&f, 0, 1, "moqrelay_bf 1\n");
    f.a.client[c].parsed.fmt = 99u;
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
    if (f.a.client[c].pinned) {
        printf("  an out-of-range format pinned a bank\n");
        failures++;
    }
    (void)fx_drain(&f, c, out, sizeof(out), 0);
    if (strstr(out, "500") == NULL) {
        printf("  an out-of-range format was not refused 500\n");
        failures++;
    }
    if (strstr(out, "moqrelay_") != NULL) {
        printf("  an out-of-range format still emitted a body\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* F1: the release carrier is bounded by the banks themselves, so a caller that
 * does not drain cannot make a token disappear. */
static int
test_release_carrier_is_bounded(void)
{
    int failures = 0;
    fixture_t f;
    moqr_admin_release_t tok[16];
    uint32_t rounds = 0;

    failures += fx_init(&f);
    /* Reserve and abort repeatedly WITHOUT draining. Each cycle owes a token;
     * a queue would silently discard them. */
    for (uint64_t s = 1; s <= 6u; s++) {
        if (moqr_admin_reserve(&f.a, 0, s) != MOQR_OK) {
            /* Correct: bank 0 still owes an unacknowledged release, so it is
             * NOT reusable. That refusal is the bound. */
            rounds++;
            continue;
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_abort(&f.a, 0, s), MOQR_OK);
    }
    if (rounds == 0u) {
        printf("  an undrained bank was reserved again -- the token was "
               "dropped\n");
        failures++;
    }
    /* Exactly one token is owed, and it is the first serial. */
    if (fx_tokens(&f, tok, 16) != 1u || tok[0].serial != 1u) {
        printf("  the undrained burst did not preserve exactly one token\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* F1: a refused broker release keeps the token and the bank, and only a
 * matching acknowledgement frees it. */
static int
test_failed_ack_retains(void)
{
    int failures = 0;
    fixture_t f;
    moqr_admin_release_t a1, a2;

    failures += fx_init(&f);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 1, 42), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_abort(&f.a, 1, 42), MOQR_OK);

    MOQ_TEST_CHECK(moqr_admin_peek_release(&f.a, &a1));
    /* The broker refused: nothing is acknowledged, so the same token is still
     * there and the bank is still unusable. */
    MOQ_TEST_CHECK(moqr_admin_peek_release(&f.a, &a2));
    if (a1.serial != a2.serial || a1.bank != a2.bank) {
        printf("  peeking twice returned different tokens\n");
        failures++;
    }
    if (moqr_admin_reserve(&f.a, 1, 43) != MOQR_ERR_WOULD_BLOCK) {
        printf("  a bank owing an unacknowledged release was reused\n");
        failures++;
    }
    /* A bank that is merely RESERVED owes nothing: acknowledging it, even
     * with the right serial, would free storage a renderer still holds. */
    {
        fixture_t g;
        failures += fx_init(&g);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&g.a, 0, 55), MOQR_OK);
        if (moqr_admin_ack_release(&g.a, 55, 0) != MOQR_ERR_INVAL) {
            printf("  a reserved bank accepted a release acknowledgement\n");
            failures++;
        }
        if (g.a.bank[0].state != MOQR_ADMIN_BS_RESERVED) {
            printf("  a rejected acknowledgement still changed the bank\n");
            failures++;
        }
        moqr_admin_destroy(&g.a);
    }
    /* Stale and mismatched acknowledgements fail closed. */
    if (moqr_admin_ack_release(&f.a, 41, 1) != MOQR_ERR_INVAL ||
        moqr_admin_ack_release(&f.a, 42, 0) != MOQR_ERR_INVAL ||
        moqr_admin_ack_release(&f.a, 0, 1) != MOQR_ERR_INVAL) {
        printf("  a stale or mismatched acknowledgement was accepted\n");
        failures++;
    }
    if (!moqr_admin_peek_release(&f.a, &a2)) {
        printf("  a failed acknowledgement consumed the token\n");
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_ack_release(&f.a, 42, 1), MOQR_OK);
    /* A duplicate acknowledgement fails closed. */
    if (moqr_admin_ack_release(&f.a, 42, 1) != MOQR_ERR_INVAL) {
        printf("  a duplicate acknowledgement was accepted\n");
        failures++;
    }
    if (moqr_admin_peek_release(&f.a, &a2)) {
        printf("  a token survived its acknowledgement\n");
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 1, 43), MOQR_OK);
    moqr_admin_destroy(&f.a);
    return failures;
}

/* F2: one broker generation never occupies two banks. */
static int
test_duplicate_serial_refused(void)
{
    int failures = 0;
    fixture_t f;

    failures += fx_init(&f);
    {
        uint32_t h;
        failures += fx_hold(&f, 77, &h);
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 0, 77), MOQR_OK);
    if (moqr_admin_reserve(&f.a, 1, 77) != MOQR_ERR_INVAL) {
        printf("  the same serial was reserved in two banks\n");
        failures++;
    }
    {
        size_t len[MOQR_ADMIN_BODY__COUNT];
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            len[k] = 2;
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_commit(&f.a, 0, 77, len, 0), MOQR_OK);
        if (moqr_admin_reserve(&f.a, 1, 77) != MOQR_ERR_INVAL) {
            printf("  a committed serial was reserved again elsewhere\n");
            failures++;
        }
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* F4: cancellation is classified by BYTES EMITTED, not by the state label. */
static int
test_cancel_by_bytes(void)
{
    int failures = 0;
    struct { const char *name; size_t prefix; bool want_503; } cases[] = {
        { "zero bytes",    0u, true  },
        { "one head byte", 1u, false },
    };

    for (uint32_t i = 0; i < 2u; i++) {
        fixture_t f;
        uint32_t c;
        const char *sp;
        size_t n;
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 1, 0);
        failures += fx_generate(&f, 0, 1, "moqrelay_cb 1\n");
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
        MOQ_TEST_CHECK(moqr_admin_pending(&f.a, c, &sp, &n));
        if (cases[i].prefix > 0u) {
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_on_written(&f.a, c, cases[i].prefix, 0), MOQR_OK);
        }
        moqr_admin_cancel(&f.a, 0);
        if (cases[i].want_503) {
            char out[512];
            (void)fx_drain(&f, c, out, sizeof(out), 0);
            if (strstr(out, "503") == NULL) {
                printf("  cancel at %s did not replace the unsent 200 with "
                       "503\n", cases[i].name);
                failures++;
            }
        } else {
            if (moqr_admin_pending(&f.a, c, &sp, &n)) {
                printf("  cancel at %s left bytes pending\n", cases[i].name);
                failures++;
            }
            if (f.a.client[c].status == MOQR_HTTP_503) {
                printf("  cancel at %s rewrote a started response as 503\n",
                       cases[i].name);
                failures++;
            }
        }
        if (f.a.client[c].pinned || f.a.bank[0].pins != 0) {
            printf("  cancel at %s left the bank pinned\n", cases[i].name);
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }

    /* Complete head, and one body byte: both are "started". */
    {
        fixture_t f;
        uint32_t c;
        const char *sp;
        size_t n;
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 1, 0);
        failures += fx_generate(&f, 0, 1, "moqrelay_cb 2\n");
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
        MOQ_TEST_CHECK(moqr_admin_pending(&f.a, c, &sp, &n));
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_written(&f.a, c, n, 0), MOQR_OK);
        MOQ_TEST_CHECK(moqr_admin_pending(&f.a, c, &sp, &n));
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_written(&f.a, c, 1, 0), MOQR_OK);
        moqr_admin_cancel(&f.a, 0);
        if (f.a.client[c].status == MOQR_HTTP_503) {
            printf("  cancel after a body byte rewrote the response\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    return failures;
}

/* F1: settling a retirement that was never claimed is refused, even for a
 * generation that exists. */
static int
test_settle_requires_claim(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;

    failures += fx_init(&f);
    failures += fx_request(&f, &c, GET_OM, 61, 0);
    /* JOINED, not RETIRING: acknowledging it would discard a live waiter's
     * generation. */
    if (moqr_admin_settle_abandon(&f.a, 61, ok_release, NULL) !=
        MOQR_ERR_INVAL) {
        printf("  a JOINED generation accepted a retirement settlement\n");
        failures++;
    }
    if (f.a.client[c].serial != 61u) {
        printf("  the rejected settlement still changed the client\n");
        failures++;
    }
    if (moqr_admin_settle_abandon(&f.a, 999, ok_release, NULL) !=
        MOQR_ERR_INVAL) {
        printf("  an unknown serial was settled\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* F1: the claim is a transition, not a peek. */
static int
test_claim_is_a_transition(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    moqr_admin_retire_t r1, r2;

    failures += fx_init(&f);
    failures += fx_request(&f, &c, GET_OM, 77, 0);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, c), MOQR_OK);

    /* ABANDONED before the claim. */
    {
        bool abandoned = false;
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            if (f.a.gen[i].serial == 77u &&
                f.a.gen[i].state == MOQR_ADMIN_GS_ABANDONED) {
                abandoned = true;
            }
        }
        if (!abandoned) {
            printf("  a closed sole waiter did not abandon its generation\n");
            failures++;
        }
    }
    MOQ_TEST_CHECK(moqr_admin_claim_abandon(&f.a, &r1));
    if (r1.serial != 77u || r1.taken) {
        printf("  the first claim reported the wrong state\n");
        failures++;
    }
    /* RETIRING after it: that transition is the whole point of the call. */
    {
        bool retiring = false;
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            if (f.a.gen[i].serial == 77u &&
                f.a.gen[i].state == MOQR_ADMIN_GS_RETIRING) {
                retiring = true;
            }
        }
        if (!retiring) {
            printf("  the claim did not move the generation to RETIRING\n");
            failures++;
        }
    }
    /* Re-claiming returns the same serial and makes no further transition. */
    MOQ_TEST_CHECK(moqr_admin_claim_abandon(&f.a, &r2));
    if (r2.serial != r1.serial || r2.taken != r1.taken) {
        printf("  re-claiming reported a different retirement\n");
        failures++;
    }
    /* Recording the take makes the retained token visible to the next claim. */
    /* Phase 3 cannot be reached from phase 1: settling before the take would
     * destroy a retirement the broker never performed. */
    if (moqr_admin_settle_abandon(&f.a, 77, ok_release, NULL) !=
        MOQR_ERR_INVAL) {
        printf("  a retirement was settled before its take\n");
        failures++;
    }
    {
        moqr_admin_retire_t still;
        if (!moqr_admin_claim_abandon(&f.a, &still) || still.serial != 77u) {
            printf("  a refused pre-take settlement lost the retirement\n");
            failures++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_record_take(&f.a, 77, 0x3u, 1u), MOQR_OK);
    if (moqr_admin_record_take(&f.a, 77, 0x3u, 1u) != MOQR_ERR_INVAL) {
        printf("  a second take was recorded for one serial\n");
        failures++;
    }
    MOQ_TEST_CHECK(moqr_admin_claim_abandon(&f.a, &r2));
    if (!r2.taken || r2.demand != 0x3u || r2.bank != 1u) {
        printf("  the retained token was not handed back by the claim\n");
        failures++;
    }
    /* A successful settle invokes the release exactly once. */
    release_calls = 0;
    release_should_fail = false;
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_settle_abandon(&f.a, 77, counting_release, NULL), MOQR_OK);
    if (release_calls != 1) {
        printf("  a successful settle invoked the release %d times, want 1\n",
               release_calls);
        failures++;
    }
    if (moqr_admin_claim_abandon(&f.a, &r2)) {
        printf("  a settled retirement was claimable again\n");
        failures++;
    }
    /* Settling twice is refused, and must not reach the broker at all. */
    release_calls = 0;
    if (moqr_admin_settle_abandon(&f.a, 77, counting_release, NULL) !=
        MOQR_ERR_INVAL) {
        printf("  a settled retirement was settled again\n");
        failures++;
    }
    if (release_calls != 0) {
        printf("  a refused settle still invoked the broker release\n");
        failures++;
    }
    /* record_take is refused for a serial that is not RETIRING. */
    if (moqr_admin_record_take(&f.a, 77, 0, 0) != MOQR_ERR_INVAL) {
        printf("  a take was recorded for a settled serial\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* A failing settle callback is an INVARIANT FAILURE, not a retry. What the
 * module owes afterwards is a readable token for diagnosis and an orderly
 * teardown -- not a path back to success. This test therefore never settles
 * again after the failure. */
static int
test_settle_failure_is_terminal(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    moqr_admin_retire_t r;

    failures += fx_init(&f);
    failures += fx_request(&f, &c, GET_OM, 88, 0);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, c), MOQR_OK);
    MOQ_TEST_CHECK(moqr_admin_claim_abandon(&f.a, &r));
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_record_take(&f.a, 88, 0x3u, 1u), MOQR_OK);

    release_calls = 0;
    release_should_fail = true;
    if (moqr_admin_settle_abandon(&f.a, 88, counting_release, NULL) !=
        MOQR_ERR_WOULD_BLOCK) {
        printf("  a failing settle did not surface its error\n");
        failures++;
    }
    if (release_calls != 1) {
        printf("  a failing settle invoked the release %d times, want 1\n",
               release_calls);
        failures++;
    }
    /* The retained identity is still READABLE, for the report and the teardown
     * that follow. Reading it is not a retry, and this test does not attempt
     * one: the contract says the operation must not be repeated. */
    {
        moqr_admin_retire_t diag;
        if (!moqr_admin_claim_abandon(&f.a, &diag) || diag.serial != 88u ||
            !diag.taken || diag.demand != 0x3u || diag.bank != 1u) {
            printf("  the retained token was not readable after a failure\n");
            failures++;
        }
    }
    /* Teardown proceeds without claiming the generation was ever settled. */
    moqr_admin_destroy(&f.a);
    return failures;
}

/* F3: a SIGNAL-only reserved bank has no client and no generation record, so
 * only the failure transition itself can make it owe its release. */
static int
test_fail_signal_only_reserved_bank(void)
{
    int failures = 0;
    fixture_t f;
    moqr_admin_release_t tok;

    failures += fx_init(&f);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 1, 555), MOQR_OK);
    /* Nothing else knows this serial. */
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (f.a.client[i].state != MOQR_ADMIN_CS_FREE ||
            f.a.gen[i].state != MOQR_ADMIN_GS_FREE) {
            printf("  the signal-only fixture is not clientless\n");
            failures++;
            break;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_fail_serial(&f.a, 555, MOQR_HTTP_500, 0), MOQR_OK);
    if (f.a.bank[1].state != MOQR_ADMIN_BS_RELEASING) {
        printf("  the failed signal-only bank is in state %u, not "
               "RELEASING\n", f.a.bank[1].state);
        failures++;
    }
    if (!moqr_admin_peek_release(&f.a, &tok) || tok.serial != 555u ||
        tok.bank != 1u) {
        printf("  the failed signal-only bank owes no token\n");
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_ack_release(&f.a, 555, 1), MOQR_OK);
    moqr_admin_destroy(&f.a);
    return failures;
}

/* F4: an identity no authoritative carrier holds fails closed. */
static int
test_fail_serial_unknown(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;

    failures += fx_init(&f);
    /* fresh unknown */
    if (moqr_admin_fail_serial(&f.a, 4242, MOQR_HTTP_500, 0) !=
        MOQR_ERR_INVAL) {
        printf("  failing an unknown serial reported success\n");
        failures++;
    }
    /* waiter-only is authoritative */
    failures += fx_request(&f, &c, GET_OM, 12, 0);
    if (moqr_admin_fail_serial(&f.a, 12, MOQR_HTTP_500, 0) != MOQR_OK) {
        printf("  failing a waiter-only generation was refused\n");
        failures++;
    }
    /* stale after settlement */
    {
        moqr_admin_release_t tok;
        uint64_t ab = 0;
        {
            uint32_t g = 0;
            while (moqr_admin_peek_release(&f.a, &tok)) {
                if (++g > MOQR_ADMIN_BANKS) {
                    printf("  release drain exceeded the bank count\n");
                    loop_overruns++;
                    failures++;
                    break;
                }
                MOQ_TEST_CHECK_EQ_INT(
                    moqr_admin_ack_release(&f.a, tok.serial, tok.bank),
                    MOQR_OK);
            }
            g = 0;
            /* Bounded by the number of generation records: a claim that never
             * transitions would otherwise spin here forever, turning a failing
             * mutant into a hung test. */
            while (claim_and_ack(&f.a, &ab)) {
                if (++g > MOQR_ADMIN_MAX_CLIENTS) {
                    printf("  retirement drain did not terminate\n");
                    loop_overruns++;
                    failures++;
                    break;
                }
            }
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, c), MOQR_OK);
        if (moqr_admin_fail_serial(&f.a, 12, MOQR_HTTP_500, 0) !=
            MOQR_ERR_INVAL) {
            printf("  failing a settled serial reported success\n");
            failures++;
        }
    }
    moqr_admin_destroy(&f.a);

    /* signal-only reserved bank is authoritative with no client at all */
    {
        fixture_t g;
        failures += fx_init(&g);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&g.a, 0, 900), MOQR_OK);
        if (moqr_admin_fail_serial(&g.a, 900, MOQR_HTTP_500, 0) != MOQR_OK) {
            printf("  failing a signal-only reserved bank was refused\n");
            failures++;
        }
        moqr_admin_destroy(&g.a);
    }
    /* ordinary banked */
    {
        fixture_t g;
        uint32_t d;
        failures += fx_init(&g);
        failures += fx_request(&g, &d, GET_OM, 21, 0);
        failures += fx_generate(&g, 0, 21, "moqrelay_fk 1\n");
        if (moqr_admin_fail_serial(&g.a, 21, MOQR_HTTP_500, 0) != MOQR_OK) {
            printf("  failing a banked generation was refused\n");
            failures++;
        }
        moqr_admin_destroy(&g.a);
    }
    return failures;
}

/* F2: the one-request boundary does not end when a response is SELECTED; it
 * ends when a byte of that response is on the wire. */
static int
test_late_bytes_while_writing(void)
{
    int failures = 0;
    struct { const char *name; size_t prefix; bool want_400; } cases[] = {
        { "zero bytes emitted", 0u, true  },
        { "one head byte",      1u, false },
    };

    for (uint32_t i = 0; i < 2u; i++) {
        fixture_t f;
        uint32_t c;
        const char *sp;
        size_t n;
        char out[512];

        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 1, 0);
        failures += fx_generate(&f, 0, 1, "moqrelay_lw 1\n");
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
        MOQ_TEST_CHECK(moqr_admin_pending(&f.a, c, &sp, &n));
        if (cases[i].prefix > 0u) {
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_on_written(&f.a, c, cases[i].prefix, 0), MOQR_OK);
        }
        /* A pipelined byte arrives. */
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_bytes(&f.a, c, "x", 1, 0), MOQR_OK);
        if (f.a.client[c].pinned || f.a.bank[0].pins != 0) {
            printf("  late bytes at %s left the bank pinned\n",
                   cases[i].name);
            failures++;
        }
        (void)fx_drain(&f, c, out, sizeof(out), 0);
        if (cases[i].want_400) {
            if (strstr(out, "400") == NULL) {
                printf("  late bytes at %s did not replace the unsent 200\n",
                       cases[i].name);
                failures++;
            }
            if (strstr(out, "moqrelay_lw") != NULL) {
                printf("  late bytes at %s still delivered the body\n",
                       cases[i].name);
                failures++;
            }
        } else {
            if (f.a.client[c].status == MOQR_HTTP_400) {
                printf("  late bytes at %s rewrote a started response\n",
                       cases[i].name);
                failures++;
            }
            if (moqr_admin_pending(&f.a, c, &sp, &n)) {
                printf("  late bytes at %s left the response continuing\n",
                       cases[i].name);
                failures++;
            }
        }
        moqr_admin_destroy(&f.a);
    }
    /* DONE: nothing left to act on. */
    {
        fixture_t f;
        uint32_t c;
        char out[512];
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 1, 0);
        failures += fx_generate(&f, 0, 1, "moqrelay_lw 2\n");
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 0), MOQR_OK);
        (void)fx_drain(&f, c, out, sizeof(out), 0);
        if (moqr_admin_on_bytes(&f.a, c, "x", 1, 0) != MOQR_ERR_WRONG_STATE) {
            printf("  late bytes on a finished response were not refused\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    return failures;
}

/* F4: bytes arriving after a complete head withdraw the request AND its
 * generation demand -- it must not remain demandable. */
static int
test_later_feed_withdraws_demand(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c, d;
    char out[512];
    const char *head = "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n";

    failures += fx_init(&f);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_bytes(&f.a, c, head, strlen(head), 0),
                          MOQR_OK);
    if (!moqr_admin_next_demand(&f.a, &d)) {
        printf("  a complete head did not become demandable\n");
        failures++;
    }
    /* Now a trailing byte in a LATER feed. */
    (void)moqr_admin_on_bytes(&f.a, c, "x", 1, 0);
    if (moqr_admin_next_demand(&f.a, &d)) {
        printf("  a withdrawn request was still demandable\n");
        failures++;
    }
    (void)fx_drain(&f, c, out, sizeof(out), 0);
    if (strstr(out, "400") == NULL) {
        printf("  a later trailing byte was not answered 400\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

/* F4: refuse validates its status against the finite table before mutating. */
static int
test_refuse_validates_status(void)
{
    int failures = 0;
    static const moqr_http_status_t bad[] = { 0u, 200u, 201u, 418u, 999u };
    static const moqr_http_status_t good[] = {
        MOQR_HTTP_400, MOQR_HTTP_404, MOQR_HTTP_405, MOQR_HTTP_406,
        MOQR_HTTP_431, MOQR_HTTP_500, MOQR_HTTP_503,
    };

    for (uint32_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        fixture_t f;
        uint32_t c;
        const char *sp;
        size_t n;
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 0, 0);
        if (moqr_admin_refuse(&f.a, c, bad[i], 0) != MOQR_ERR_INVAL) {
            printf("  refusal with status %u was accepted\n",
                   (unsigned)bad[i]);
            failures++;
        }
        if (f.a.client[c].state != MOQR_ADMIN_CS_PARSED) {
            printf("  an invalid refusal mutated the client\n");
            failures++;
        }
        if (moqr_admin_pending(&f.a, c, &sp, &n)) {
            printf("  an invalid refusal produced pending bytes\n");
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    for (uint32_t i = 0; i < sizeof(good) / sizeof(good[0]); i++) {
        fixture_t f;
        uint32_t c;
        const char *sp;
        size_t n;
        char out[512];
        failures += fx_init(&f);
        failures += fx_request(&f, &c, GET_OM, 0, 0);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_refuse(&f.a, c, good[i], 0), MOQR_OK);
        if (!moqr_admin_pending(&f.a, c, &sp, &n)) {
            printf("  refusal with status %u produced nothing to send\n",
                   (unsigned)good[i]);
            failures++;
        }
        (void)fx_drain(&f, c, out, sizeof(out), 0);
        if (strstr(out, "moqrelay_") != NULL) {
            printf("  refusal with status %u carried metrics bytes\n",
                   (unsigned)good[i]);
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    return failures;
}

/* F6: the publication predicate is correct across wrap. */
static int
test_publish_wrap(void)
{
    int failures = 0;
    fixture_t f;
    uint64_t sampled;

    failures += fx_init(&f);
    {
        uint32_t h;
        failures += fx_hold(&f, 1, &h);
    }
    f.a.publish_gen = UINT64_MAX;
    sampled = moqr_admin_publish_epoch(&f.a);
    if (moqr_admin_publish_since(&f.a, sampled)) {
        printf("  the predicate fired with no commit at the boundary\n");
        failures++;
    }
    failures += fx_generate(&f, 0, 1, "moqrelay_wr 1\n");
    if (!moqr_admin_publish_since(&f.a, sampled)) {
        printf("  one commit after a sample at UINT64_MAX was invisible\n");
        failures++;
    }
    moqr_admin_destroy(&f.a);
    return failures;
}

static const char *GET_INFO =
    "GET /api/v1/info HTTP/1.1\r\nHost: x\r\n\r\n";

/* Nothing an /info client does may touch generation or bank state. */
static int
generation_state_untouched(const fixture_t *f, const char *what)
{
    int failures = 0;
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (f->a.gen[i].state != MOQR_ADMIN_GS_FREE) {
            printf("  %s: a generation record exists for an /info client\n",
                   what);
            failures++;
        }
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (f->a.bank[b].pins != 0 || f->a.bank[b].state != MOQR_ADMIN_BS_FREE) {
            printf("  %s: a bank moved for an /info client\n", what);
            failures++;
        }
    }
    return failures;
}

/* /api/v1/info is served STATICALLY: straight from the parsed request to a
 * 200 over the immutable document the machine was constructed with. It is
 * never demand, never joins a generation, never pins a bank, and owes no
 * release. */
static int
test_info_is_served_statically(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    uint32_t d = 99;
    char out[1024];
    size_t n;
    moqr_admin_release_t tok[MOQR_ADMIN_BANKS];

    failures += fx_init(&f);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&f.a, c, GET_INFO, strlen(GET_INFO), 0), MOQR_OK);
    if (f.a.client[c].state != MOQR_ADMIN_CS_WRITING) {
        printf("  info: the request is not WRITING immediately (state %u)\n",
               (unsigned)f.a.client[c].state);
        failures++;
    }
    if (moqr_admin_next_demand(&f.a, &d)) {
        printf("  info: an /info client was reported as generation demand\n");
        failures++;
    }
    if (f.a.client[c].carrier != MOQR_ADMIN_CARRIER_STATIC ||
        f.a.client[c].pinned || f.a.client[c].serial != 0u ||
        f.a.client[c].bank < MOQR_ADMIN_BANKS) {
        printf("  info: the client is not carried statically (carrier %u "
               "pinned %d serial %llu bank %u)\n",
               (unsigned)f.a.client[c].carrier, (int)f.a.client[c].pinned,
               (unsigned long long)f.a.client[c].serial,
               (unsigned)f.a.client[c].bank);
        failures++;
    }
    n = fx_drain(&f, c, out, sizeof(out), 0);
    if (n == 0 || strstr(out, "HTTP/1.1 200 OK\r\n") == NULL ||
        strstr(out, "Content-Type: " MOQR_ADMIN_CT_JSON "\r\n") == NULL ||
        strstr(out, "\r\n\r\n" FX_INFO) == NULL) {
        printf("  info: the response is not the document [%.160s]\n", out);
        failures++;
    }
    /* The body is the EXACT bytes the machine was constructed with. */
    if (f.a.client[c].body != f.a.info) {
        printf("  info: the body was not borrowed from the constructed document\n");
        failures++;
    }
    if (f.a.client[c].state != MOQR_ADMIN_CS_DONE) {
        printf("  info: a fully written response is not DONE\n");
        failures++;
    }
    failures += generation_state_untouched(&f, "info");
    if (fx_tokens(&f, tok, MOQR_ADMIN_BANKS) != 0) {
        printf("  info: a release token was owed for an /info response\n");
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, c), MOQR_OK);
    failures += generation_state_untouched(&f, "info-closed");
    return failures;
}

/* /info beside metrics: both banks pinned by slow scrapers still leaves
 * /info served; timing out, cancelling, closing and dropping an /info client
 * changes no pin, no generation and no release. */
static int
test_info_beside_metrics(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t m1, m2, i1, i2, i3, i4;
    char out[1024];
    size_t n;
    moqr_admin_release_t tok[MOQR_ADMIN_BANKS];
    uint32_t pins_before[MOQR_ADMIN_BANKS];

    failures += fx_init(&f);
    /* Two generations, both banked and pinned by scrapers mid-write. */
    failures += fx_request(&f, &m1, GET_OM, 11, 0);
    failures += fx_generate(&f, 0, 11, "OM11 ");
    failures += fx_request(&f, &m2, GET_OM, 12, 0);
    failures += fx_generate(&f, 1, 12, "OM12 ");
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 1), MOQR_OK);
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        pins_before[b] = f.a.bank[b].pins;
    }
    if (pins_before[0] != 1u || pins_before[1] != 1u) {
        printf("  beside: the fixture did not pin both banks (%u, %u)\n",
               pins_before[0], pins_before[1]);
        failures++;
    }
    /* /info is served while both banks are pinned. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 1, &i1), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&f.a, i1, GET_INFO, strlen(GET_INFO), 1), MOQR_OK);
    n = fx_drain(&f, i1, out, sizeof(out), 1);
    if (n == 0 || strstr(out, "200 OK") == NULL ||
        strstr(out, FX_INFO) == NULL) {
        printf("  beside: /info was not served with both banks pinned\n");
        failures++;
    }
    /* A partial /info write that times out is dropped: no pin, no gen. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 2, &i2), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&f.a, i2, GET_INFO, strlen(GET_INFO), 2), MOQR_OK);
    {
        const char *span; size_t len;
        MOQ_TEST_CHECK(moqr_admin_pending(&f.a, i2, &span, &len));
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_written(&f.a, i2, 3, 2), MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_tick(&f.a, 2 + MOQR_ADMIN_WRITE_DEADLINE_US + 1u), MOQR_OK);
    if (f.a.client[i2].state != MOQR_ADMIN_CS_DONE) {
        printf("  beside: a timed-out partial /info write was not dropped\n");
        failures++;
    }
    /* Cancellation: an unsent /info becomes 503; a partly sent one is
     * dropped; neither touches a bank. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 3, &i3), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&f.a, i3, GET_INFO, strlen(GET_INFO), 3), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 3, &i4), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&f.a, i4, GET_INFO, strlen(GET_INFO), 3), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_written(&f.a, i4, 1, 3), MOQR_OK);
    moqr_admin_cancel(&f.a, 4);
    if (f.a.client[i3].status != MOQR_HTTP_503 ||
        f.a.client[i3].carrier != MOQR_ADMIN_CARRIER_NONE) {
        printf("  beside: an unsent /info was not turned into a 503\n");
        failures++;
    }
    if (f.a.client[i4].state != MOQR_ADMIN_CS_DONE) {
        printf("  beside: a partly sent /info was not dropped on cancel\n");
        failures++;
    }
    /* The metrics side is exactly as the fixture left it, apart from what
     * cancellation does to metrics clients on its own terms. */
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (f.a.bank[b].pins > pins_before[b]) {
            printf("  beside: an /info client added a pin to bank %u\n", b);
            failures++;
        }
    }
    (void)fx_tokens(&f, tok, MOQR_ADMIN_BANKS);
    /* Repeated cleanup is harmless. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, i1), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, i2), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, i3), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, i4), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, i1), MOQR_ERR_WRONG_STATE);
    moqr_admin_cancel(&f.a, 5);
    moqr_admin_destroy(&f.a);
    return failures;
}

/* The construction contract: no document, no machine. */
static int
test_info_document_is_required(void)
{
    int failures = 0;
    fixture_t f;
    char *bodies[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT];
    size_t caps[MOQR_ADMIN_BODY__COUNT];
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            bodies[b][k] = f.storage[b][k];
        }
    }
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        caps[k] = BODY_CAP;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_init(&f.a, bodies, caps, NULL, 0),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_init(&f.a, bodies, caps, FX_INFO, 0),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_init(&f.a, bodies, caps, NULL, 3),
                          MOQR_ERR_INVAL);
    /* An alleged length above the finite limit is refused without reading a
     * byte of it: the pointer here is two bytes long. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_init(&f.a, bodies, caps, "{}", SIZE_MAX),
                          MOQR_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_init(&f.a, bodies, caps, "{}",
                                          (size_t)MOQR_ADMIN_MAX_STATIC_DOC + 1u),
                          MOQR_ERR_INVAL);
    /* Exactly the limit is a valid document, served whole, with the head
     * acknowledged on its own and the body in parts. */
    {
        static char big[MOQR_ADMIN_MAX_STATIC_DOC];
        static char out[MOQR_ADMIN_MAX_STATIC_DOC + 1024u];
        uint32_t c;
        const char *span;
        size_t n;
        memset(big, 'x', sizeof(big));
        big[0] = '{';
        big[sizeof(big) - 1u] = '}';
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_init(&f.a, bodies, caps, big,
                                              sizeof(big)), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&f.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&f.a, c, GET_INFO, strlen(GET_INFO), 0), MOQR_OK);
        MOQ_TEST_CHECK(moqr_admin_pending(&f.a, c, &span, &n));
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_written(&f.a, c, f.a.client[c].head_len, 0), MOQR_OK);
        if (f.a.client[c].head_sent != f.a.client[c].head_len) {
            printf("  limit: acknowledging exactly the head was not counted\n");
            failures++;
        }
        /* One byte beyond what remains is refused, never wrapped. */
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_written(&f.a, c, sizeof(big) + 1u, 0), MOQR_ERR_INVAL);
        n = fx_drain(&f, c, out, sizeof(out), 0);
        if (n != sizeof(big) || memcmp(out, big, sizeof(big)) != 0 ||
            f.a.client[c].state != MOQR_ADMIN_CS_DONE) {
            printf("  limit: an exact-limit document was not served whole "
                   "(%zu bytes)\n", n);
            failures++;
        }
        moqr_admin_destroy(&f.a);
    }
    return failures;
}

static const char *GET_SHARDS =
    "GET /api/v1/shards HTTP/1.1\r\nHost: x\r\n\r\n";

/* Write DISTINCT documents into every slot of a bank so a response can be
 * matched to the slot it was served from. */
static int
fx_generate_slots(fixture_t *f, uint32_t bank, uint64_t serial)
{
    int failures = 0;
    static const char *const docs[MOQR_ADMIN_BODY__COUNT] = {
        [MOQR_OBS_FMT_PROMETHEUS_004] = "PROM-BODY",
        [MOQR_OBS_FMT_OPENMETRICS_100] = "OM-BODY",
        [MOQR_ADMIN_BODY_SHARDS] = "{\"api\":\"v1\",\"shards\":[]}",
    };
    size_t len[MOQR_ADMIN_BODY__COUNT];
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f->a, bank, serial), MOQR_OK);
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        size_t cap = 0;
        char *dst = moqr_admin_bank_storage(&f->a, bank, serial, k, &cap);
        MOQ_TEST_CHECK(dst != NULL);
        if (dst == NULL) {
            return failures;
        }
        memcpy(dst, docs[k], strlen(docs[k]));
        len[k] = strlen(docs[k]);
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_commit(&f->a, bank, serial, len, 0), MOQR_OK);
    return failures;
}

/* /api/v1/shards is a GENERATION request: it joins a serial exactly as a
 * metrics scrape does, waits for that serial's bank, and is served the bank's
 * shards JSON slot -- never a metrics slot, never another serial's bytes. A
 * metrics scrape on the same generation is served its own slot. */
static int
test_shards_is_served_from_the_generation_slot(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t cs, cm;
    char out[1024];
    size_t n;

    failures += fx_init(&f);
    failures += fx_request(&f, &cs, GET_SHARDS, 41, 0);
    failures += fx_request(&f, &cm, GET_OM, 41, 0);
    if (f.a.client[cs].state != MOQR_ADMIN_CS_WAITING ||
        f.a.client[cs].serial != 41u) {
        printf("  shards: the request did not join its generation (state %u "
               "serial %llu)\n", (unsigned)f.a.client[cs].state,
               (unsigned long long)f.a.client[cs].serial);
        failures++;
    }
    failures += fx_generate_slots(&f, 0, 41);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 1), MOQR_OK);
    n = fx_drain(&f, cs, out, sizeof(out), 1);
    if (n == 0 || strstr(out, "HTTP/1.1 200 OK\r\n") == NULL ||
        strstr(out, "Content-Type: " MOQR_ADMIN_CT_JSON "\r\n") == NULL ||
        strstr(out, "\r\n\r\n{\"api\":\"v1\",\"shards\":[]}") == NULL ||
        strstr(out, "PROM-BODY") != NULL || strstr(out, "OM-BODY") != NULL) {
        printf("  shards: not served the shards slot: [%.160s]\n", out);
        failures++;
    }
    if (f.a.client[cs].carrier != MOQR_ADMIN_CARRIER_BANK) {
        printf("  shards: the body was not carried by the bank\n");
        failures++;
    }
    n = fx_drain(&f, cm, out, sizeof(out), 1);
    if (n == 0 || strstr(out, "OM-BODY") == NULL ||
        strstr(out, "\"shards\"") != NULL) {
        printf("  shards: the metrics scrape on the same generation was not "
               "served its own slot: [%.120s]\n", out);
        failures++;
    }
    return failures;
}

/* The generation semantics shards inherit: incomplete → 503 with Retry-After,
 * poisoned → 500, both banks pinned → 503, and a stale serial can never be
 * the source of a body. */
static int
test_shards_generation_semantics(void)
{
    int failures = 0;
    fixture_t f;
    uint32_t c1, c2, p1, p2, c3;
    char out[1024];
    size_t n;

    failures += fx_init(&f);
    /* Incomplete: the generation never reaches a bank before the deadline. */
    failures += fx_request(&f, &c1, GET_SHARDS, 51, 0);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_tick(&f.a, MOQR_ADMIN_EPOCH_DEADLINE_US + 1u), MOQR_OK);
    n = fx_drain(&f, c1, out, sizeof(out), MOQR_ADMIN_EPOCH_DEADLINE_US + 1u);
    if (n == 0 || strstr(out, "HTTP/1.1 503 ") == NULL ||
        strstr(out, "Retry-After: 1\r\n") == NULL || strstr(out, "{") != NULL) {
        printf("  shards-sem: an incomplete generation was not answered 503 "
               "without a body: [%.100s]\n", out);
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, c1), MOQR_OK);
    /* Poisoned: the owner fails the serial with 500. */
    failures += fx_request(&f, &c2, GET_SHARDS, 52, 0);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_fail_serial(&f.a, 52, MOQR_HTTP_500, 0),
                          MOQR_OK);
    n = fx_drain(&f, c2, out, sizeof(out), 0);
    if (n == 0 || strstr(out, "HTTP/1.1 500 ") == NULL ||
        strstr(out, "{") != NULL) {
        printf("  shards-sem: a poisoned generation was not answered 500 "
               "without a body: [%.100s]\n", out);
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, c2), MOQR_OK);
    /* Both banks pinned by scrapers mid-write: a third generation's shards
     * request cannot be banked. */
    failures += fx_request(&f, &p1, GET_OM, 61, 0);
    failures += fx_generate_slots(&f, 0, 61);
    failures += fx_request(&f, &p2, GET_OM, 62, 0);
    failures += fx_generate_slots(&f, 1, 62);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&f.a, 1), MOQR_OK);
    failures += fx_request(&f, &c3, GET_SHARDS, 63, 1);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 0, 63), MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&f.a, 1, 63), MOQR_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_refuse(&f.a, c3, MOQR_HTTP_503, 1), MOQR_OK);
    n = fx_drain(&f, c3, out, sizeof(out), 1);
    if (n == 0 || strstr(out, "HTTP/1.1 503 ") == NULL) {
        printf("  shards-sem: both banks pinned did not answer 503\n");
        failures++;
    }
    /* A stale serial is not storage: nothing can be rendered under it. */
    if (moqr_admin_bank_storage(&f.a, 0, 60, MOQR_ADMIN_BODY_SHARDS, NULL) != NULL ||
        moqr_admin_bank_storage(&f.a, 0, 61, MOQR_ADMIN_BODY_SHARDS, NULL) != NULL) {
        printf("  shards-sem: a stale or committed serial yielded storage\n");
        failures++;
    }
    return failures;
}

/* GENERATION MEMBERSHIP IS NOT BODY-BANK OWNERSHIP.
 *
 * A metrics waiter joins its generation at bind time and enters WAITING with
 * no bank pinned. Whatever ends that request before a bank is reached --
 * close, the epoch deadline, cancellation -- must still leave the generation,
 * so the sole waiter's departure reaches the abandonment path the broker can
 * retire. A cleanup gated on "does this client own a bank" would strand every
 * one of these as JOINED forever. */
static int
prebank_waiter_reaches_abandonment(int how)
{
    int failures = 0;
    fixture_t f;
    uint32_t c;
    moqr_admin_retire_t r;
    const char *what = how == 0 ? "close" : how == 1 ? "epoch-timeout"
                                                     : "cancel";

    failures += fx_init(&f);
    failures += fx_request(&f, &c, GET_OM, 91, 0);
    if (how == 0) {
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&f.a, c), MOQR_OK);
    } else if (how == 1) {
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_tick(&f.a, MOQR_ADMIN_EPOCH_DEADLINE_US + 1u), MOQR_OK);
        if (f.a.client[c].status != MOQR_HTTP_503) {
            printf("  prebank[%s]: the expired waiter was not answered 503\n",
                   what);
            failures++;
        }
    } else {
        moqr_admin_cancel(&f.a, 0);
    }
    if (!moqr_admin_claim_abandon(&f.a, &r) || r.serial != 91u || r.taken) {
        printf("  prebank[%s]: the sole pre-bank waiter's generation was not "
               "abandoned -- it is stranded\n", what);
        failures++;
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (f.a.bank[b].pins != 0 || f.a.bank[b].state != MOQR_ADMIN_BS_FREE) {
            printf("  prebank[%s]: a bank moved for a client that never "
                   "reached one\n", what);
            failures++;
        }
    }
    return failures;
}

static int
test_prebank_waiter_cleanup_is_serial_based(void)
{
    int failures = 0;
    for (int how = 0; how < 3; how++) {
        failures += prebank_waiter_reaches_abandonment(how);
    }
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_exact_serial_routing();
    failures += test_no_serial_no_delivery();
    failures += test_refuse_is_immediate();
    failures += test_exact_once_release();
    failures += test_commit_protocol();
    failures += test_signal_only_path();
    failures += test_trailing_bytes();
    failures += test_zero_progress_does_not_refresh();
    failures += test_timeout_error_survives_next_tick();
    failures += test_cancel_states();
    failures += test_deadline_boundaries();
    failures += test_partial_read_and_write();
    failures += test_pins_and_banks();
    failures += test_slow_loris_and_capacity();
    failures += test_format_selects_body();
    failures += test_error_needs_no_generation();
    failures += test_oversized_head();
    failures += test_publish_predicate();
    failures += test_bad_format_is_refused();
    failures += test_release_carrier_is_bounded();
    failures += test_failed_ack_retains();
    failures += test_duplicate_serial_refused();
    failures += test_cancel_by_bytes();
    failures += test_later_feed_withdraws_demand();
    failures += test_settle_requires_claim();
    failures += test_late_bytes_while_writing();
    failures += test_claim_is_a_transition();
    failures += test_settle_failure_is_terminal();
    failures += test_fail_signal_only_reserved_bank();
    failures += test_fail_serial_unknown();
    failures += test_refuse_validates_status();
    failures += test_publish_wrap();
    failures += test_prebank_waiter_cleanup_is_serial_based();
    failures += test_info_is_served_statically();
    failures += test_info_beside_metrics();
    failures += test_info_document_is_required();
    failures += test_shards_is_served_from_the_generation_slot();
    failures += test_shards_generation_semantics();
    if (loop_overruns != 0) {
        printf("  %d bounded loop(s) overran their cardinality\n",
               loop_overruns);
        failures += loop_overruns;
    }
    if (failures != 0) {
        printf("FAIL: %d admin state-machine violation(s)\n", failures);
        return 1;
    }
    printf("OK\n");
    return 0;
}
