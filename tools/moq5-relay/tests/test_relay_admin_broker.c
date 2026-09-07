/* The admin state machine driven by the REAL broker.
 *
 * The two pieces have to agree about generation identity and about who owes a
 * release, and neither can be checked from one side alone: the broker decides
 * which serial a request joins, and the admin decides which bytes that serial
 * receives. This drives them together, with a ledger that must balance. */

#include <moqr_admin.h>

#include "../cli/broker.h"

#include <stdio.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

#define BODY_CAP 256u

typedef struct {
    moqr_admin_t  a;
    moqr_broker_t b;
    char          storage[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT][BODY_CAP];
    /* Ledger: how many generations the BROKER opened, and how many were
     * released back to it. A successful admin reservation is not a broker
     * generation, and counting reservations would hide exactly the leaks this
     * ledger exists to find. */
    uint32_t      issued;
    uint32_t      released;
    uint64_t      last_issued;
    /* What the OWNER actually observed on the frozen token, as distinct from
     * whatever the retirement bookkeeping happened to accumulate. Conflating
     * them let a lost SIGNAL bit read as "nothing to check". */
    uint32_t      observed_demand;
    uint32_t      retired_demand;
} rig_t;

/* Claim the next retirement, reporting just its serial. */
static bool
claim_serial(moqr_admin_t *a, uint64_t *out)
{
    moqr_admin_retire_t r;
    if (!moqr_admin_claim_abandon(a, &r)) {
        return false;
    }
    *out = r.serial;
    return true;
}

/* A loop that stops spinning but returns normally still lets a nonterminating
 * path exit zero. Every bounded loop records an overrun here, and main() fails
 * the run on it. */
static int loop_overruns = 0;

static const char *GET_OM =
    "GET /metrics HTTP/1.1\r\nHost: x\r\n"
    "Accept: application/openmetrics-text\r\n\r\n";

static int
rig_init(rig_t *r)
{
    int failures = 0;
    char *bodies[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT];
    size_t caps[MOQR_ADMIN_BODY__COUNT];

    memset(r, 0, sizeof(*r));
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            bodies[b][k] = r->storage[b][k];
        }
    }
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        caps[k] = BODY_CAP;
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_init(&r->a, bodies, caps, "{\"api\":\"v1\"}",
                                          12), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_broker_init(&r->b, MOQR_BROKER_BANKS), MOQR_OK);
    return failures;
}

/* The owner's broker release, invoked from inside moqr_admin_settle_abandon so
 * the release and the admin settlement share one stack frame. */
static moqr_result_t
rig_release_cb(void *ctx, uint64_t serial, uint32_t demand, uint32_t bank)
{
    rig_t *r = (rig_t *)ctx;
    bool wake = false;
    r->retired_demand |= demand;
    return moqr_broker_release(&r->b, serial, bank, &wake);
}

/* The Step 4 loop, written exactly as the header documents it. */
static int
rig_pump_demands(rig_t *r, uint64_t now)
{
    int failures = 0;
    uint32_t c;

    uint32_t pumped = 0;
    while (moqr_admin_next_demand(&r->a, &c)) {
        /* A bind that fails would leave the same PARSED client selected
         * forever; the bound turns that into a verdict. */
        if (++pumped > (MOQR_ADMIN_MAX_CLIENTS * 2u) + 4u) {
            printf("  rig_pump_demands did not make progress\n");
            loop_overruns++;
            failures++;
            break;
        }
        uint64_t serial = 0;
        bool wake = false;
        moqr_result_t rc = moqr_broker_request(&r->b, MOQR_BROKER_DEMAND_HTTP,
                                               &serial, &wake);
        if (rc == MOQR_OK) {
            if (serial == 0) {
                printf("  the broker admitted a request with serial 0\n");
                failures++;
            }
            /* A join returns the serial already open; only a new one is a new
             * broker generation. */
            if (serial != r->last_issued) {
                r->last_issued = serial;
                r->issued++;
            }
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_bind_serial(&r->a, c, serial, now), MOQR_OK);
        } else {
            /* Definitive refusal: this request is answered 503 at once. */
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_refuse(&r->a, c, MOQR_HTTP_503, now), MOQR_OK);
        }
    }
    return failures;
}

/* The intended Step-4 producer order:
 *   1. complete the exact broker serial;
 *   2. take THAT serial's token and keep all three fields;
 *   3. reserve/render/commit against the bank the broker named;
 *   4. (delivery happens in tick)
 *   5. release and acknowledge the same token exactly once.
 *
 * The broker's bank is authoritative. Choosing an admin bank independently and
 * discarding `bk` happens to work only while both allocators pick the lowest
 * free index; the broker then rejects the release of any other pairing. */
static int
rig_produce(rig_t *r, uint64_t now, uint32_t *out_bank)
{
    int failures = 0;
    uint64_t serial = 0;
    uint32_t demand = 0;
    uint32_t bank = 0;
    size_t len[MOQR_ADMIN_BODY__COUNT];
    char doc[64];

    if (!moqr_broker_current(&r->b, &serial, &demand)) {
        return failures;
    }
    moqr_broker_on_complete(&r->b, serial);
    if (!moqr_broker_take_serial(&r->b, serial, &demand, &bank)) {
        printf("  the broker would not hand over the serial it just froze\n");
        failures++;
        return failures;
    }
    /* The owner observes the frozen token's demand here, before rendering. */
    r->observed_demand |= demand;
    if (bank >= MOQR_ADMIN_BANKS) {
        printf("  the broker named bank %u, outside the admin's banks\n", bank);
        failures++;
        return failures;
    }
    {
        moqr_result_t rc = moqr_admin_reserve(&r->a, bank, serial);
        if (rc != MOQR_OK) {
            printf("  the admin refused the bank the broker named: %d\n",
                   (int)rc);
            failures++;
            return failures;
        }
    }
    (void)snprintf(doc, sizeof(doc), "moqrelay_serial %llu\n",
                   (unsigned long long)serial);
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        size_t cap = 0;
        char *dst = moqr_admin_bank_storage(&r->a, bank, serial, k, &cap);
        MOQ_TEST_CHECK(dst != NULL);
        if (dst == NULL) {
            return failures;
        }
        memcpy(dst, doc, strlen(doc));
        len[k] = strlen(doc);
    }
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_commit(&r->a, bank, serial, len, now),
                          MOQR_OK);
    if (out_bank != NULL) {
        *out_bank = bank;
    }
    return failures;
}

/* Retire a generation that never reached a bank, using only the broker
 * vocabulary: freeze it, take its token (which carries any SIGNAL demand that
 * joined the same serial), then release it. */
static int
rig_retire_abandoned(rig_t *r)
{
    int failures = 0;
    uint64_t serial = 0;

    uint32_t guard = 0;
    moqr_admin_retire_t ret;

    (void)serial;
    while (moqr_admin_claim_abandon(&r->a, &ret)) {
        uint32_t demand = ret.demand;
        uint32_t bank = ret.bank;
        bool wake = false;
        if (++guard > 32u) {
            printf("  claim_abandon did not terminate\n");
            failures++;
            break;
        }
        if (!ret.taken) {
            /* Phase 1: nothing consumed yet, so this is safe to re-enter. */
            moqr_broker_on_complete(&r->b, ret.serial);
            if (!moqr_broker_take_serial(&r->b, ret.serial, &demand, &bank)) {
                printf("  a claimed serial %llu was not takeable\n",
                       (unsigned long long)ret.serial);
                failures++;
                break;
            }
            /* Phase 2: the take is destructive, so the token is retained
             * before anything else can interrupt the sequence. */
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_record_take(&r->a, ret.serial, demand, bank),
                MOQR_OK);
        }
        (void)wake;
        (void)bank;
        (void)demand;
        /* Phase 3: the broker release and the admin settlement are ONE call,
         * so no interruption can land between them. */
        if (moqr_admin_settle_abandon(&r->a, ret.serial, rig_release_cb, r) !=
            MOQR_OK) {
            printf("  the coupled settle transaction failed\n");
            failures++;
            break;
        }
        r->released++;
    }
    return failures;
}

/* Drain admin release tokens back into the broker. */
static int
rig_settle(rig_t *r)
{
    int failures = 0;
    moqr_admin_release_t rel;
    uint32_t guard = 0;

    while (moqr_admin_peek_release(&r->a, &rel)) {
        bool wake = false;
        moqr_result_t rc;
        if (++guard > 64u) {
            printf("  peek_release did not terminate\n");
            failures++;
            break;
        }
        if (rel.serial == 0) {
            printf("  a release token carried serial 0\n");
            failures++;
            break;
        }
        /* The broker authenticates {serial,bank}. A token it rejects means the
         * admin released something it did not hold, or a bank pairing the
         * broker never issued. */
        rc = moqr_broker_release(&r->b, rel.serial, rel.bank, &wake);
        if (rc != MOQR_OK) {
            printf("  the broker rejected release {%llu,%u}: %d\n",
                   (unsigned long long)rel.serial, rel.bank, (int)rc);
            failures++;
            break;
        }
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_ack_release(&r->a, rel.serial, rel.bank), MOQR_OK);
        r->released++;
    }
    failures += rig_retire_abandoned(r);
    return failures;
}

static size_t
drain_client(rig_t *r, uint32_t c, char *out, size_t cap, uint64_t now)
{
    size_t total = 0;
    const char *span;
    size_t n;
    uint32_t guard = 0;
    while (moqr_admin_pending(&r->a, c, &span, &n)) {
        if (++guard > 4096u) {
            printf("  drain_client did not terminate\n");
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
        if (moqr_admin_on_written(&r->a, c, n, now) != MOQR_OK) {
            break;
        }
    }
    out[total] = '\0';
    return total;
}

/* Requests arriving while a generation is COLLECTING all join that serial,
 * and all receive its bytes. */
static int
test_joiners_share_the_collecting_serial(void)
{
    int failures = 0;
    rig_t r;
    uint32_t c[3];
    char out[512];

    failures += rig_init(&r);
    for (uint32_t i = 0; i < 3; i++) {
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c[i]), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c[i], GET_OM, strlen(GET_OM), 0),
            MOQR_OK);
    }
    failures += rig_pump_demands(&r, 0);

    if (r.a.client[c[0]].serial != r.a.client[c[1]].serial ||
        r.a.client[c[1]].serial != r.a.client[c[2]].serial) {
        printf("  concurrent joiners did not share one collecting serial\n");
        failures++;
    }
    if (r.a.client[c[0]].serial == 0) {
        printf("  a joiner was bound to serial 0\n");
        failures++;
    }

    failures += rig_produce(&r, 0, NULL);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
    for (uint32_t i = 0; i < 3; i++) {
        (void)drain_client(&r, c[i], out, sizeof(out), 0);
        if (strstr(out, "moqrelay_serial") == NULL) {
            printf("  joiner %u did not receive the shared generation\n", i);
            failures++;
        }
    }
    failures += rig_settle(&r);
    if (r.issued != r.released) {
        printf("  ledger: %u issued, %u released\n", r.issued, r.released);
        failures++;
    }
    moqr_admin_destroy(&r.a);
    moqr_broker_destroy(&r.b);
    return failures;
}

/* A request arriving after freeze gets the NEXT serial, and the two
 * generations never cross-deliver even while both banks are ready. */
static int
test_post_freeze_gets_next_serial(void)
{
    int failures = 0;
    rig_t r;
    uint32_t early, late;
    uint64_t s_early, s_late;
    char out[512];

    failures += rig_init(&r);

    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &early), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, early, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    failures += rig_pump_demands(&r, 0);
    s_early = r.a.client[early].serial;
    failures += rig_produce(&r, 0, NULL);       /* freezes s_early */

    /* Now a second request: the first generation is frozen, so this must open
     * a new one rather than joining the closed epoch. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &late), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, late, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    failures += rig_pump_demands(&r, 0);
    s_late = r.a.client[late].serial;

    if (s_late == s_early) {
        printf("  a post-freeze request joined the frozen generation\n");
        failures++;
    }
    failures += rig_produce(&r, 0, NULL);       /* renders s_late */

    /* Both generations are ready at once. Neither may cross-deliver. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
    {
        char want[64];
        (void)drain_client(&r, early, out, sizeof(out), 0);
        (void)snprintf(want, sizeof(want), "moqrelay_serial %llu",
                       (unsigned long long)s_early);
        if (strstr(out, want) == NULL) {
            printf("  the early request did not receive serial %llu\n",
                   (unsigned long long)s_early);
            failures++;
        }
        (void)drain_client(&r, late, out, sizeof(out), 0);
        (void)snprintf(want, sizeof(want), "moqrelay_serial %llu",
                       (unsigned long long)s_late);
        if (strstr(out, want) == NULL) {
            printf("  the late request did not receive serial %llu\n",
                   (unsigned long long)s_late);
            failures++;
        }
    }
    failures += rig_settle(&r);
    if (r.issued != r.released) {
        printf("  ledger: %u issued, %u released\n", r.issued, r.released);
        failures++;
    }
    moqr_admin_destroy(&r.a);
    moqr_broker_destroy(&r.b);
    return failures;
}

/* With both banks occupied the broker refuses, and that request is answered
 * 503 at once rather than waiting for a generation that cannot start. */
static int
test_bank_exhaustion_refuses_immediately(void)
{
    int failures = 0;
    rig_t r;
    uint32_t c;
    char out[512];
    uint32_t answered_503 = 0;

    failures += rig_init(&r);
    /* Fill both banks with undelivered generations. */
    for (uint32_t i = 0; i < MOQR_ADMIN_BANKS; i++) {
        uint32_t x;
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &x), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, x, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        failures += rig_produce(&r, 0, NULL);
    }
    if (moqr_admin_bank_available(&r.a)) {
        printf("  a bank was still free with both generations undelivered\n");
        failures++;
    }
    /* A further request the broker cannot seat. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    failures += rig_pump_demands(&r, 0);
    /* The broker has no bank to open, so this request must be refused NOW --
     * an exact 503, not silence and not another generation's bytes. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
    (void)drain_client(&r, c, out, sizeof(out), 0);
    if (strstr(out, "HTTP/1.1 503") != NULL) {
        answered_503 = 1;
    }
    if (!answered_503) {
        printf("  an over-capacity request was not answered 503 (got %.32s)\n",
               out[0] ? out : "(nothing)");
        failures++;
    }
    if (strstr(out, "moqrelay_serial") != NULL) {
        printf("  an over-capacity request received a generation\n");
        failures++;
    }
    moqr_admin_destroy(&r.a);
    failures += rig_settle(&r);
    moqr_broker_destroy(&r.b);
    return failures;
}

/* Every generation the broker issues is released exactly once, across
 * delivery, timeout, cancellation and teardown. */
static int
test_token_conservation(void)
{
    int failures = 0;

    /* delivered */
    {
        rig_t r;
        uint32_t c;
        char out[512];
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        failures += rig_produce(&r, 0, NULL);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
        (void)drain_client(&r, c, out, sizeof(out), 0);
        failures += rig_settle(&r);
        if (r.issued != 1u || r.released != 1u) {
            printf("  delivery ledger: issued=%u released=%u\n", r.issued,
                   r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        failures += rig_settle(&r);
        if (r.released != 1u) {
            printf("  teardown double-released a delivered generation\n");
            failures++;
        }
        moqr_broker_destroy(&r.b);
    }
    /* cancelled mid-write */
    {
        rig_t r;
        uint32_t c;
        const char *sp;
        size_t n;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        failures += rig_produce(&r, 0, NULL);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
        MOQ_TEST_CHECK(moqr_admin_pending(&r.a, c, &sp, &n));
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_written(&r.a, c, 4, 0), MOQR_OK);
        moqr_admin_cancel(&r.a, 0);
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  cancellation ledger: issued=%u released=%u\n", r.issued,
                   r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  teardown after cancellation unbalanced the ledger\n");
            failures++;
        }
        moqr_broker_destroy(&r.b);
    }
    /* rendered, then its last waiter closes before delivery */
    {
        rig_t r;
        uint32_t c;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        failures += rig_produce(&r, 0, NULL);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&r.a, c), MOQR_OK);
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  unclaimed ledger: issued=%u released=%u\n", r.issued,
                   r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
    }
    return failures;
}

/* F3: a generation joined by a request that then times out BEFORE anything was
 * rendered must be retired in the broker. Otherwise it collects forever and
 * every later request joins the dead epoch and times out too. */
static int
test_prebank_timeout_retires_the_generation(void)
{
    int failures = 0;
    rig_t r;
    uint32_t c, c2;
    uint64_t dead;
    char out[512];

    failures += rig_init(&r);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    failures += rig_pump_demands(&r, 0);
    dead = r.a.client[c].serial;

    /* Nothing is ever rendered; the bounded wait elapses. */
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_tick(&r.a, MOQR_ADMIN_EPOCH_DEADLINE_US + 1u), MOQR_OK);
    (void)drain_client(&r, c, out, sizeof(out),
                       MOQR_ADMIN_EPOCH_DEADLINE_US + 1u);
    if (strstr(out, "503") == NULL) {
        printf("  the timed-out request was not answered 503\n");
        failures++;
    }
    /* The generation must now be offered for retirement. */
    {
        uint64_t ab = 0;
        if (!claim_serial(&r.a, &ab) || ab != dead) {
            printf("  a pre-bank generation was not offered for retirement\n");
            failures++;
        } else {
            uint32_t demand = 0;
            uint32_t bank = 0;
            bool wake = false;
            (void)wake;
            moqr_broker_on_complete(&r.b, ab);
            MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, ab, &demand, &bank));
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_record_take(&r.a, ab, demand, bank), MOQR_OK);
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_settle_abandon(&r.a, ab, rig_release_cb, &r),
                MOQR_OK);
            r.released++;
        }
    }
    if (moqr_broker_busy(&r.b)) {
        printf("  the broker is still busy after the generation was retired\n");
        failures++;
    }
    /* A later request must get a NEW serial and be servable. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c2), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, c2, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    failures += rig_pump_demands(&r, 0);
    if (r.a.client[c2].serial == dead) {
        printf("  a later request joined the dead generation\n");
        failures++;
    }
    if (r.a.client[c2].serial == 0) {
        printf("  a later request could not open a generation at all\n");
        failures++;
    }
    failures += rig_produce(&r, 0, NULL);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
    (void)drain_client(&r, c2, out, sizeof(out), 0);
    if (strstr(out, "moqrelay_serial") == NULL) {
        printf("  the later request was starved by the dead generation\n");
        failures++;
    }
    failures += rig_settle(&r);
    if (r.issued != r.released) {
        printf("  ledger: issued=%u released=%u\n", r.issued, r.released);
        failures++;
    }
    moqr_admin_destroy(&r.a);
    moqr_broker_destroy(&r.b);
    return failures;
}

/* One waiter timing out while another remains must NOT retire the shared
 * generation -- the survivor is still entitled to it. */
static int
test_one_waiter_leaves_shared_generation(void)
{
    int failures = 0;
    rig_t r;
    uint32_t a1, a2;
    uint64_t shared;
    uint64_t ab = 0;
    char out[512];

    failures += rig_init(&r);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &a1), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, a1, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &a2), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, a2, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    failures += rig_pump_demands(&r, 0);
    shared = r.a.client[a1].serial;
    if (shared == 0 || r.a.client[a2].serial != shared) {
        printf("  the two joiners did not share one generation\n");
        failures++;
    }

    /* One of them goes away. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&r.a, a1), MOQR_OK);
    if (claim_serial(&r.a, &ab)) {
        printf("  a shared generation was retired while a waiter remained\n");
        failures++;
    }
    /* The survivor is still served. */
    failures += rig_produce(&r, 0, NULL);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
    (void)drain_client(&r, a2, out, sizeof(out), 0);
    if (strstr(out, "moqrelay_serial") == NULL) {
        printf("  the surviving waiter was not served\n");
        failures++;
    }
    failures += rig_settle(&r);
    if (r.issued != r.released) {
        printf("  ledger: issued=%u released=%u\n", r.issued, r.released);
        failures++;
    }
    moqr_admin_destroy(&r.a);
    moqr_broker_destroy(&r.b);
    return failures;
}

/* ALL waiters leaving retires the generation; so does global cancellation and
 * teardown, each exactly once. */
static int
test_all_waiters_leave_and_shutdown(void)
{
    int failures = 0;

    /* all waiters close */
    {
        rig_t r;
        uint32_t a1, a2;
        uint64_t shared, ab = 0;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &a1), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, a1, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &a2), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, a2, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        shared = r.a.client[a1].serial;
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&r.a, a1), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&r.a, a2), MOQR_OK);
        if (!claim_serial(&r.a, &ab) || ab != shared) {
            printf("  losing every waiter did not retire the generation\n");
            failures++;
        } else {
            uint32_t d = 0, bk = 0;
            bool wake = false;
            (void)wake;
            moqr_broker_on_complete(&r.b, ab);
            MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, ab, &d, &bk));
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_record_take(&r.a, ab, d, bk), MOQR_OK);
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_settle_abandon(&r.a, ab, rig_release_cb, &r),
                MOQR_OK);
            r.released++;
        }
        if (moqr_broker_busy(&r.b)) {
            printf("  the broker stayed busy after retirement\n");
            failures++;
        }
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
    }
    /* cancellation while a generation IS banked: the bank owes the release,
     * so the broker must NOT also be told to abandon it -- that would retire
     * one generation twice. */
    {
        rig_t r;
        uint32_t c;
        uint64_t ab = 0;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        failures += rig_produce(&r, 0, NULL);
        moqr_admin_cancel(&r.a, 0);
        if (claim_serial(&r.a, &ab)) {
            printf("  a banked generation was ALSO offered for abandonment\n");
            failures++;
        }
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  banked-cancel ledger: issued=%u released=%u\n",
                   r.issued, r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  teardown after banked cancel unbalanced the ledger\n");
            failures++;
        }
        moqr_broker_destroy(&r.b);
    }
    /* global cancellation */
    {
        rig_t r;
        uint32_t c;
        uint64_t s0;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        s0 = r.a.client[c].serial;
        moqr_admin_cancel(&r.a, 0);
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  cancellation left generation %llu unretired\n",
                   (unsigned long long)s0);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  teardown after cancellation unbalanced the ledger\n");
            failures++;
        }
        moqr_broker_destroy(&r.b);
    }
    /* teardown with a generation still joined */
    {
        rig_t r;
        uint32_t c;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        moqr_admin_destroy(&r.a);
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  teardown left a joined generation unretired\n");
            failures++;
        }
        moqr_broker_destroy(&r.b);
    }
    return failures;
}

/* F2: {serial, bank} is ONE token. The broker authenticates both halves, so an
 * admin that picks its own bank and discards the broker's is only correct while
 * the two allocators happen to agree. */
static int
test_broker_bank_is_authoritative(void)
{
    int failures = 0;
    rig_t r;
    uint32_t c;
    uint64_t serial = 0;
    uint32_t demand = 0;
    uint32_t bank = 99;
    uint32_t other;
    bool wake = false;

    failures += rig_init(&r);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    failures += rig_pump_demands(&r, 0);

    MOQ_TEST_CHECK(moqr_broker_current(&r.b, &serial, &demand));
    moqr_broker_on_complete(&r.b, serial);
    MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, serial, &demand, &bank));
    if (bank >= MOQR_ADMIN_BANKS) {
        printf("  the broker named bank %u\n", bank);
        failures++;
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
        return failures;
    }
    other = (bank + 1u) % MOQR_ADMIN_BANKS;

    /* Releasing the same serial against a DIFFERENT bank is refused: the bank
     * is part of the identity, not an incidental index. */
    if (moqr_broker_release(&r.b, serial, other, &wake) == MOQR_OK) {
        printf("  the broker accepted a release naming the wrong bank\n");
        failures++;
    }
    /* The admin binds its storage to the bank the broker named. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&r.a, bank, serial), MOQR_OK);
    if (r.a.bank[bank].serial != serial) {
        printf("  the admin bank does not hold the broker's serial\n");
        failures++;
    }
    if (r.a.bank[other].serial == serial) {
        printf("  the serial also occupies the other bank\n");
        failures++;
    }
    /* And the token the admin offers names that same bank. */
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_abort(&r.a, bank, serial), MOQR_OK);
    {
        moqr_admin_release_t tok;
        MOQ_TEST_CHECK(moqr_admin_peek_release(&r.a, &tok));
        if (tok.bank != bank || tok.serial != serial) {
            printf("  the admin token {%llu,%u} is not the broker's {%llu,%u}\n",
                   (unsigned long long)tok.serial, tok.bank,
                   (unsigned long long)serial, bank);
            failures++;
        }
    }
    failures += rig_settle(&r);
    if (r.issued != r.released) {
        printf("  ledger: issued=%u released=%u\n", r.issued, r.released);
        failures++;
    }
    moqr_admin_destroy(&r.a);
    moqr_broker_destroy(&r.b);
    return failures;
}

/* F5: the serial space is finite. When it is spent the broker refuses rather
 * than wrapping to zero -- the value both modules read as "no generation". */
static int
test_serial_exhaustion(void)
{
    int failures = 0;
    rig_t r;
    uint64_t s1 = 0;
    uint64_t s2 = 0;
    bool wake = false;

    failures += rig_init(&r);
    moqr_broker_test_seed_serial(&r.b, UINT64_MAX - 1u);

    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&r.b, MOQR_BROKER_DEMAND_HTTP, &s1, &wake),
        MOQR_OK);
    if (s1 != UINT64_MAX - 1u) {
        printf("  MAX-1 was not issued (%llu)\n", (unsigned long long)s1);
        failures++;
    }
    moqr_broker_on_complete(&r.b, s1);
    {
        uint32_t d = 0, bk = 0;
        MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, s1, &d, &bk));
        MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&r.b, s1, bk, &wake),
                              MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(
        moqr_broker_request(&r.b, MOQR_BROKER_DEMAND_HTTP, &s2, &wake),
        MOQR_OK);
    if (s2 != UINT64_MAX) {
        printf("  MAX was not issued (%llu)\n", (unsigned long long)s2);
        failures++;
    }
    moqr_broker_on_complete(&r.b, s2);
    {
        uint32_t d = 0, bk = 0;
        MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, s2, &d, &bk));
        MOQ_TEST_CHECK_EQ_INT(moqr_broker_release(&r.b, s2, bk, &wake),
                              MOQR_OK);
    }
    /* The first post-MAX request is a TERMINAL refusal: no serial, no wake,
     * and no deferred work. */
    {
        uint64_t s3 = 12345u;
        bool w3 = true;
        moqr_result_t rc =
            moqr_broker_request(&r.b, MOQR_BROKER_DEMAND_HTTP, &s3, &w3);
        if (rc != MOQR_ERR_CAPACITY) {
            printf("  a post-exhaustion request returned %d\n", (int)rc);
            failures++;
        }
        if (s3 != 0u) {
            printf("  a refused request reported serial %llu\n",
                   (unsigned long long)s3);
            failures++;
        }
        if (w3) {
            printf("  a refused request asked the lanes to wake\n");
            failures++;
        }
        if (moqr_broker_busy(&r.b)) {
            printf("  a refused request left the broker busy\n");
            failures++;
        }
        /* And the admin would reject serial zero anyway. */
        {
            uint32_t c;
            MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0),
                MOQR_OK);
            if (moqr_admin_bind_serial(&r.a, c, s3, 0) != MOQR_ERR_INVAL) {
                printf("  the admin accepted serial zero\n");
                failures++;
            }
        }
        /* Repeated requests stay terminal rather than eventually wrapping. */
        for (int i = 0; i < 3; i++) {
            uint64_t sx = 1u;
            bool wx = false;
            if (moqr_broker_request(&r.b, MOQR_BROKER_DEMAND_HTTP, &sx, &wx) !=
                MOQR_ERR_CAPACITY || sx != 0u) {
                printf("  exhaustion was not terminal\n");
                failures++;
            }
        }
    }
    moqr_admin_destroy(&r.a);
    moqr_broker_destroy(&r.b);
    return failures;
}

/* F7: a poisoned generation answers its waiters 500, not 503. */
static int
test_fail_serial(void)
{
    int failures = 0;
    rig_t r;
    uint32_t c1, c2, c3;
    uint64_t s;
    char out[512];
    const char *sp;
    size_t n;

    failures += rig_init(&r);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c1), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, c1, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c2), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, c2, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c3), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, c3, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    failures += rig_pump_demands(&r, 0);
    s = r.a.client[c1].serial;
    /* SIGNAL demand joins the same generation. */
    {
        uint64_t sig = 0;
        bool wake = false;
        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_request(&r.b, MOQR_BROKER_DEMAND_SIGNAL, &sig, &wake),
            MOQR_OK);
        if (sig != s) {
            printf("  SIGNAL demand did not join the collecting serial\n");
            failures++;
        }
    }
    /* One waiter has already started its response. */
    failures += rig_produce(&r, 0, NULL);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
    MOQ_TEST_CHECK(moqr_admin_pending(&r.a, c3, &sp, &n));
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_on_written(&r.a, c3, 5, 0), MOQR_OK);

    /* The renderer discovers the generation is poisoned. */
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_fail_serial(&r.a, s, MOQR_HTTP_500, 0), MOQR_OK);

    for (uint32_t x = 0; x < 2u; x++) {
        uint32_t who = (x == 0) ? c1 : c2;
        (void)drain_client(&r, who, out, sizeof(out), 0);
        if (strstr(out, "500") == NULL) {
            printf("  an unstarted waiter was not answered 500\n");
            failures++;
        }
        if (strstr(out, "moqrelay_serial") != NULL) {
            printf("  a 500 carried a metrics body\n");
            failures++;
        }
        if (strstr(out, "503") != NULL) {
            printf("  a poisoned generation reported temporary "
                   "unavailability\n");
            failures++;
        }
    }
    if (moqr_admin_pending(&r.a, c3, &sp, &n)) {
        printf("  a started response was not dropped\n");
        failures++;
    }
    if (r.a.client[c3].status == MOQR_HTTP_500) {
        printf("  a started response was rewritten as 500\n");
        failures++;
    }
    failures += rig_settle(&r);
    if (r.issued != r.released) {
        printf("  fail ledger: issued=%u released=%u\n", r.issued, r.released);
        failures++;
    }
    /* The SIGNAL demand that joined must be on the token the owner observed --
     * unconditionally. The previous form excused itself when nothing had been
     * observed at all, which is exactly the lost-signal case. */
    if ((r.observed_demand & MOQR_BROKER_DEMAND_SIGNAL) == 0u) {
        printf("  the owner never observed the SIGNAL demand (observed=0x%x)\n",
               r.observed_demand);
        failures++;
    }
    /* An unknown status is refused. */
    if (moqr_admin_fail_serial(&r.a, s, MOQR_HTTP_404, 0) != MOQR_ERR_INVAL) {
        printf("  fail_serial accepted a request-level status\n");
        failures++;
    }
    moqr_admin_destroy(&r.a);
    moqr_broker_destroy(&r.b);
    return failures;
}

/* F7: failing a generation BEFORE any waiter has been attached. The waiters are
 * still WAITING and the bank has no pin, so both the waiter answer and the bank
 * release have to come from fail_serial itself. */
static int
test_fail_serial_before_delivery(void)
{
    int failures = 0;
    rig_t r;
    uint32_t c1, c2;
    uint64_t s;
    char out[512];

    failures += rig_init(&r);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c1), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, c1, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c2), MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_on_bytes(&r.a, c2, GET_OM, strlen(GET_OM), 0), MOQR_OK);
    failures += rig_pump_demands(&r, 0);
    s = r.a.client[c1].serial;

    /* The bank is rendered and committed, but nothing has been ticked, so no
     * client has attached and the bank holds no pin. */
    failures += rig_produce(&r, 0, NULL);
    if (r.a.client[c1].state != MOQR_ADMIN_CS_WAITING ||
        r.a.client[c2].state != MOQR_ADMIN_CS_WAITING) {
        printf("  the waiters were attached before the failure\n");
        failures++;
    }

    MOQ_TEST_CHECK_EQ_INT(
        moqr_admin_fail_serial(&r.a, s, MOQR_HTTP_500, 0), MOQR_OK);

    for (uint32_t x = 0; x < 2u; x++) {
        uint32_t who = (x == 0) ? c1 : c2;
        (void)drain_client(&r, who, out, sizeof(out), 0);
        if (strstr(out, "500") == NULL) {
            printf("  a WAITING waiter was not answered 500\n");
            failures++;
        }
        if (strstr(out, "moqrelay_serial") != NULL) {
            printf("  a 500 carried a metrics body\n");
            failures++;
        }
    }
    /* The bank owes its release from the failure itself, not from a pin drop
     * that never happened. */
    {
        moqr_admin_release_t tok;
        if (!moqr_admin_peek_release(&r.a, &tok) || tok.serial != s) {
            printf("  the failed generation did not owe its bank release\n");
            failures++;
        }
    }
    failures += rig_settle(&r);
    if (r.issued != r.released) {
        printf("  fail-before-delivery ledger: issued=%u released=%u\n",
               r.issued, r.released);
        failures++;
    }
    moqr_admin_destroy(&r.a);
    moqr_broker_destroy(&r.b);

    /* And with NO client left at all: the waiters closed first, so nothing
     * else can owe the bank's release -- only the failure itself. */
    {
        rig_t q;
        uint32_t c;
        uint64_t s2;
        moqr_admin_release_t tok;
        failures += rig_init(&q);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&q.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&q.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&q, 0);
        s2 = q.a.client[c].serial;
        {
            /* Render and commit while the request is still present, then take
             * the request away without touching the bank. */
            uint32_t demand = 0, bank = 0;
            size_t len[MOQR_ADMIN_BODY__COUNT];
            moqr_broker_on_complete(&q.b, s2);
            MOQ_TEST_CHECK(moqr_broker_take_serial(&q.b, s2, &demand, &bank));
            MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&q.a, bank, s2), MOQR_OK);
            for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
                char *dst = moqr_admin_bank_storage(&q.a, bank, s2, k, NULL);
                MOQ_TEST_CHECK(dst != NULL);
                if (dst != NULL) {
                    memcpy(dst, "Y\n", 2);
                }
                len[k] = 2;
            }
            MOQ_TEST_CHECK_EQ_INT(moqr_admin_commit(&q.a, bank, s2, len, 0),
                                  MOQR_OK);
        }
        /* The commit already settled it because the waiter was gone? No -- the
         * waiter is still here; remove it now WITHOUT closing, by failing. */
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&q.a, c), MOQR_OK);
        uint32_t g = 0;
        while (moqr_admin_peek_release(&q.a, &tok)) {
            if (++g > MOQR_ADMIN_BANKS) {
                printf("  release drain exceeded the bank count\n");
                failures++;
                break;
            }
            bool wake = false;
            MOQ_TEST_CHECK_EQ_INT(
                moqr_broker_release(&q.b, tok.serial, tok.bank, &wake),
                MOQR_OK);
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_ack_release(&q.a, tok.serial, tok.bank), MOQR_OK);
            q.released++;
        }
        if (q.issued != q.released) {
            printf("  clientless ledger: issued=%u released=%u\n", q.issued,
                   q.released);
            failures++;
        }
        moqr_admin_destroy(&q.a);
        moqr_broker_destroy(&q.b);
    }
    return failures;
}

/* F1: the exact interleavings, through the real broker. */
static int
test_leave_during_render(void)
{
    int failures = 0;

    /* close while RESERVED, then COMMIT */
    {
        rig_t r;
        uint32_t c;
        uint64_t s = 0, ab = 0;
        uint32_t demand = 0, bank = 0;
        size_t len[MOQR_ADMIN_BODY__COUNT];
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        s = r.a.client[c].serial;
        moqr_broker_on_complete(&r.b, s);
        MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, s, &demand, &bank));
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&r.a, bank, s), MOQR_OK);
        /* The request disconnects while the renderer holds the bank. */
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&r.a, c), MOQR_OK);
        if (claim_serial(&r.a, &ab)) {
            printf("  a banked generation was offered for abandonment\n");
            failures++;
        }
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            char *dst = moqr_admin_bank_storage(&r.a, bank, s, k, NULL);
            MOQ_TEST_CHECK(dst != NULL);
            if (dst != NULL) {
                memcpy(dst, "X\n", 2);
            }
            len[k] = 2;
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_commit(&r.a, bank, s, len, 0),
                              MOQR_OK);
        if (r.a.bank[bank].state == MOQR_ADMIN_BS_READY) {
            printf("  the commit left a READY, unpinned, unreachable bank\n");
            failures++;
        }
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  render-leave ledger: issued=%u released=%u\n",
                   r.issued, r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
    }
    /* close while RESERVED, then ABORT */
    {
        rig_t r;
        uint32_t c;
        uint64_t s = 0;
        uint32_t demand = 0, bank = 0;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        s = r.a.client[c].serial;
        moqr_broker_on_complete(&r.b, s);
        MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, s, &demand, &bank));
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&r.a, bank, s), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&r.a, c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_abort(&r.a, bank, s), MOQR_OK);
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  render-abort ledger: issued=%u released=%u\n",
                   r.issued, r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
    }
    return failures;
}

/* F1: a rejoin before retirement is offered cancels it; after it is offered,
 * the serial is refused so a new generation is opened instead. */
static int
test_rejoin_windows(void)
{
    int failures = 0;

    /* rejoin BEFORE peek */
    {
        rig_t r;
        uint32_t c1, c2;
        uint64_t s = 0, ab = 0;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c1), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c1, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        s = r.a.client[c1].serial;
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&r.a, c1), MOQR_OK);

        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c2), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c2, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        if (moqr_admin_bind_serial(&r.a, c2, s, 0) != MOQR_OK) {
            printf("  a rejoin before retirement was refused\n");
            failures++;
        }
        if (claim_serial(&r.a, &ab)) {
            printf("  a rejoined generation was still retired underneath its "
                   "new waiter\n");
            failures++;
        }
        failures += rig_produce(&r, 0, NULL);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
        {
            char out[512];
            (void)drain_client(&r, c2, out, sizeof(out), 0);
            if (strstr(out, "moqrelay_serial") == NULL) {
                printf("  the rejoined waiter was not served\n");
                failures++;
            }
        }
        failures += rig_settle(&r);
        if (r.issued != r.released) {
            printf("  rejoin ledger: issued=%u released=%u\n", r.issued,
                   r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
    }
    /* rejoin AFTER peek: refused, and the retirement still completes once */
    {
        rig_t r;
        uint32_t c1, c2;
        uint64_t s = 0, ab = 0;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c1), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c1, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        s = r.a.client[c1].serial;
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&r.a, c1), MOQR_OK);
        MOQ_TEST_CHECK(claim_serial(&r.a, &ab));

        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c2), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c2, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        if (moqr_admin_bind_serial(&r.a, c2, s, 0) == MOQR_OK) {
            printf("  a serial already being retired accepted a new waiter\n");
            failures++;
        }
        /* A broker failure before the acknowledgement keeps the disposition. */
        {
            uint64_t again = 0;
            if (!claim_serial(&r.a, &again) || again != ab) {
                printf("  an unacknowledged retirement was lost\n");
                failures++;
            }
        }
        if (moqr_admin_settle_abandon(&r.a, ab + 1u, rig_release_cb, &r) !=
            MOQR_ERR_INVAL) {
            printf("  a stale retirement settlement was accepted\n");
            failures++;
        }
        {
            uint32_t d = 0, bk = 0;
            bool wake = false;
            (void)wake;
            moqr_broker_on_complete(&r.b, ab);
            MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, ab, &d, &bk));
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_record_take(&r.a, ab, d, bk), MOQR_OK);
            MOQ_TEST_CHECK_EQ_INT(
                moqr_admin_settle_abandon(&r.a, ab, rig_release_cb, &r),
                MOQR_OK);
            r.released++;
            if (moqr_admin_settle_abandon(&r.a, ab, rig_release_cb, &r) !=
                MOQR_ERR_INVAL) {
                printf("  a duplicate retirement settlement was accepted\n");
                failures++;
            }
        }
        moqr_admin_destroy(&r.a);
        failures += rig_settle(&r);
        moqr_broker_destroy(&r.b);
    }
    return failures;
}

/* F2: the retirement sequence is restartable only BEFORE the take. */
static int
test_retirement_phases(void)
{
    int failures = 0;

    /* Interrupted before complete, and again before take: both re-enter. */
    {
        rig_t r;
        uint32_t c;
        uint64_t s;
        moqr_admin_retire_t a1, a2, a3;
        failures += rig_init(&r);
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        s = r.a.client[c].serial;
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_close(&r.a, c), MOQR_OK);

        MOQ_TEST_CHECK(moqr_admin_claim_abandon(&r.a, &a1));
        if (a1.serial != s || a1.taken) {
            printf("  the first claim was not a fresh retirement\n");
            failures++;
        }
        /* interrupted before complete */
        MOQ_TEST_CHECK(moqr_admin_claim_abandon(&r.a, &a2));
        if (a2.serial != s || a2.taken) {
            printf("  re-entering before complete lost the retirement\n");
            failures++;
        }
        moqr_broker_on_complete(&r.b, s);
        /* interrupted after complete, before take */
        MOQ_TEST_CHECK(moqr_admin_claim_abandon(&r.a, &a3));
        if (a3.serial != s || a3.taken) {
            printf("  re-entering after complete lost the retirement\n");
            failures++;
        }
        {
            uint32_t d = 0, bk = 0;
            bool wake = false;
            MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, s, &d, &bk));
            /* A SECOND take is false: the slot left READY. */
            {
                uint32_t d2 = 0, bk2 = 0;
                if (moqr_broker_take_serial(&r.b, s, &d2, &bk2)) {
                    printf("  a second take of one serial succeeded\n");
                    failures++;
                }
            }
            MOQ_TEST_CHECK_EQ_INT(moqr_admin_record_take(&r.a, s, d, bk),
                                  MOQR_OK);
            /* interrupted after take: the claim now hands the token back, and
             * the caller must NOT take again. */
            {
                moqr_admin_retire_t a4;
                MOQ_TEST_CHECK(moqr_admin_claim_abandon(&r.a, &a4));
                if (!a4.taken || a4.bank != bk || a4.demand != d) {
                    printf("  the retained token was not carried across the "
                           "interruption\n");
                    failures++;
                }
                /* Phase 3 is ONE call: the broker release happens inside the
                 * settle, so there is no interruptible gap to test between
                 * them -- and no way for the owner to create one. */
                (void)wake;
                MOQ_TEST_CHECK_EQ_INT(
                    moqr_admin_settle_abandon(&r.a, s, rig_release_cb, &r),
                    MOQR_OK);
                if (moqr_admin_settle_abandon(&r.a, s, rig_release_cb, &r) !=
                    MOQR_ERR_INVAL) {
                    printf("  a second settlement was accepted\n");
                    failures++;
                }
                r.released++;
            }
        }
        if (r.issued != r.released) {
            printf("  phase ledger: issued=%u released=%u\n", r.issued,
                   r.released);
            failures++;
        }
        {
            moqr_admin_retire_t a5;
            if (moqr_admin_claim_abandon(&r.a, &a5)) {
                printf("  a completed retirement was claimable again\n");
                failures++;
            }
        }
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
    }
    return failures;
}

/* -- the SIGNAL emitter fixture -----------------------------------------
 *
 * Test-owned, not Step 4 wiring. It receives the exact projection the owner
 * would emit, records how many times and how many bytes, and records whether
 * the bank had already been committed when it ran -- which is how the ordering
 * requirement becomes observable rather than asserted. */
enum { EMIT_OK = 0, EMIT_SHORT, EMIT_FAIL };

#define EMIT_CAP 256u

static int    emit_calls;
static size_t emit_bytes;
static char   emit_capture[EMIT_CAP];
static bool   emit_saw_committed;
static int    emit_mode;

/* `len` is the owner's AUTHORITATIVE rendered length. Measuring the projection
 * with strlen would trust a NUL the renderer never promised, and comparing only
 * a length would pass any same-length corruption -- so the exact bytes are
 * captured and compared. */
static moqr_result_t
emit_signal(rig_t *r, uint32_t bank, uint64_t serial, bool committed,
            size_t len)
{
    size_t cap = 0;
    const char *proj =
        moqr_admin_bank_storage(&r->a, bank, serial,
                                MOQR_OBS_FMT_OPENMETRICS_100, &cap);
    emit_calls++;
    if (committed) {
        emit_saw_committed = true;
    }
    if (proj == NULL) {
        /* Storage is reachable only while RESERVED; if the owner emitted after
         * commit there is nothing to emit, and that is the defect. */
        return MOQR_ERR_WRONG_STATE;
    }
    if (len > cap || len > EMIT_CAP) {
        return MOQR_ERR_CAPACITY;
    }
    switch (emit_mode) {
    case EMIT_SHORT:
        memcpy(emit_capture, proj, 1u);
        emit_bytes = 1u;
        return MOQR_ERR_WOULD_BLOCK;
    case EMIT_FAIL:
        return MOQR_ERR_INTERNAL;
    default:
        memcpy(emit_capture, proj, len);
        emit_bytes = len;
        return MOQR_OK;
    }
}

/* Exact bytes and length, not a substring. */
static int
check_emitted(const char *doc, const char *what)
{
    size_t n = strlen(doc);
    if (emit_bytes != n) {
        printf("  %s emitted %zu bytes, want %zu\n", what, emit_bytes, n);
        return 1;
    }
    if (memcmp(emit_capture, doc, n) != 0) {
        printf("  %s emitted different bytes than were rendered\n", what);
        return 1;
    }
    return 0;
}

/* The response body, compared exactly against the rendered document. */
static int
check_body(const char *resp, size_t resp_len, const char *doc,
           const char *what)
{
    const char *sep = NULL;
    size_t i;
    size_t body_len;
    size_t n = strlen(doc);

    for (i = 0; i + 3u < resp_len; i++) {
        if (resp[i] == '\r' && resp[i + 1] == '\n' && resp[i + 2] == '\r' &&
            resp[i + 3] == '\n') {
            sep = resp + i + 4u;
            break;
        }
    }
    if (sep == NULL) {
        printf("  %s response had no head terminator\n", what);
        return 1;
    }
    body_len = resp_len - (size_t)(sep - resp);
    if (body_len != n || memcmp(sep, doc, n) != 0) {
        printf("  %s body is %zu bytes and does not match the rendered %zu\n",
               what, body_len, n);
        return 1;
    }
    return 0;
}

/* The owner sequence under test. Kept in one place so a mutation of it -- drop
 * the SIGNAL branch, move it after commit, abort mixed HTTP on a SIGNAL error,
 * or retry a failed emission -- is a single decisive edit.
 *
 * THE SIGNAL-SINK FAILURE POLICY, stated once:
 *
 *   A local signal sink that short-writes or fails does NOT make the rendered
 *   snapshot invalid. It is an output problem, not a document problem, so it
 *   must never cost an HTTP waiter its scrape.
 *
 *     SIGNAL-only   attempt once from RESERVED bytes; on success, short write
 *                   or failure alike, do not retry a possibly partial stream --
 *                   abort the bank and release exactly once.
 *     SIGNAL+HTTP   attempt once from RESERVED bytes FIRST; then commit the
 *                   unchanged complete bytes for HTTP regardless of how the
 *                   sink fared, deliver them exactly, and release once after
 *                   the HTTP pin drains.
 *     HTTP-only     never invoke the emitter at all.
 *
 * A renderer or snapshot failure is a different condition and may answer HTTP
 * 500; that is not this path. */
static int
owner_sequence(rig_t *r, uint32_t demand, uint32_t bank, uint64_t serial,
               const char *doc, bool *out_committed)
{
    int failures = 0;
    size_t len[MOQR_ADMIN_BODY__COUNT];
    bool committed = false;

    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        char *dst = moqr_admin_bank_storage(&r->a, bank, serial, k, NULL);
        MOQ_TEST_CHECK(dst != NULL);
        if (dst == NULL) {
            return failures;
        }
        memcpy(dst, doc, strlen(doc));
        len[k] = strlen(doc);
    }
    /* SIGNAL is emitted from RESERVED storage, BEFORE any commit, exactly
     * once. The result is deliberately NOT branched on: a partial stream is
     * not re-attempted, and a failed one does not withdraw the document. */
    if ((demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u) {
        (void)emit_signal(r, bank, serial, committed,
                          len[MOQR_OBS_FMT_OPENMETRICS_100]);
    }
    if ((demand & MOQR_BROKER_DEMAND_HTTP) != 0u) {
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_commit(&r->a, bank, serial, len, 0),
                              MOQR_OK);
        committed = true;
    } else {
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_abort(&r->a, bank, serial), MOQR_OK);
    }
    *out_committed = committed;
    return failures;
}

/* Drain every owed release exactly once. */
static int
settle_all(rig_t *r, uint32_t *out_n)
{
    int failures = 0;
    moqr_admin_release_t tok;
    uint32_t n = 0;
    while (moqr_admin_peek_release(&r->a, &tok)) {
        bool wake = false;
        if (++n > MOQR_ADMIN_BANKS + 1u) {
            printf("  release drain exceeded the bank count\n");
            loop_overruns++;
            failures++;
            break;
        }
        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_release(&r->b, tok.serial, tok.bank, &wake), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_ack_release(&r->a, tok.serial, tok.bank), MOQR_OK);
        r->released++;
    }
    *out_n = n;
    return failures;
}

/* F7: the owner sequence for each demand mask, with a real emitter. */
static int
test_demand_mask_sequences(void)
{
    int failures = 0;
    static const struct { const char *name; int mode; } modes[] = {
        { "ok", EMIT_OK }, { "short", EMIT_SHORT }, { "fail", EMIT_FAIL },
    };

    /* SIGNAL-only, across all three emission outcomes. */
    for (uint32_t mi = 0; mi < 3u; mi++) {
        rig_t r;
        uint64_t s = 0;
        uint32_t demand = 0, bank = 0, rel = 0;
        bool wake = false, committed = true;
        failures += rig_init(&r);
        emit_calls = 0; emit_bytes = 0; emit_saw_committed = false;
        emit_mode = modes[mi].mode;

        MOQ_TEST_CHECK_EQ_INT(
            moqr_broker_request(&r.b, MOQR_BROKER_DEMAND_SIGNAL, &s, &wake),
            MOQR_OK);
        r.issued++;
        moqr_broker_on_complete(&r.b, s);
        MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, s, &demand, &bank));
        r.observed_demand |= demand;
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&r.a, bank, s), MOQR_OK);
        failures += owner_sequence(&r, demand, bank, s, "SIG\n", &committed);

        if (emit_calls != 1) {
            printf("  signal-only/%s emitted %d times, want 1\n",
                   modes[mi].name, emit_calls);
            failures++;
        }
        if (emit_saw_committed) {
            printf("  signal-only/%s emitted after commit\n", modes[mi].name);
            failures++;
        }
        if (committed) {
            printf("  signal-only/%s committed a bank with no HTTP reader\n",
                   modes[mi].name);
            failures++;
        }
        if (modes[mi].mode == EMIT_OK) {
            failures += check_emitted("SIG\n", "signal-only/ok");
        }
        failures += settle_all(&r, &rel);
        if (rel != 1u || r.issued != r.released) {
            printf("  signal-only/%s settled %u times (issued=%u released=%u)\n",
                   modes[mi].name, rel, r.issued, r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
    }

    /* HTTP-only: no SIGNAL bit, so the emitter must never run. The bank still
     * commits, the waiter receives the exact bytes, and one release settles. */
    {
        rig_t r;
        uint32_t c;
        uint64_t s = 0;
        uint32_t demand = 0, bank = 0, rel = 0;
        bool committed = false;
        char out[512];
        failures += rig_init(&r);
        emit_calls = 0; emit_bytes = 0; emit_saw_committed = false;
        emit_mode = EMIT_OK;

        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        s = r.a.client[c].serial;
        moqr_broker_on_complete(&r.b, s);
        MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, s, &demand, &bank));
        r.observed_demand |= demand;
        if ((demand & MOQR_BROKER_DEMAND_SIGNAL) != 0u) {
            printf("  an HTTP-only generation carried a SIGNAL bit\n");
            failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&r.a, bank, s), MOQR_OK);
        failures += owner_sequence(&r, demand, bank, s, "moqrelay_http 1\n",
                                   &committed);
        if (emit_calls != 0 || emit_bytes != 0u) {
            printf("  HTTP-only emitted %d time(s)/%zu bytes, want 0/0\n",
                   emit_calls, emit_bytes);
            failures++;
        }
        if (!committed) {
            printf("  HTTP-only did not commit for its waiter\n");
            failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
        {
            size_t got = drain_client(&r, c, out, sizeof(out), 0);
            failures += check_body(out, got, "moqrelay_http 1\n", "http-only");
        }
        failures += settle_all(&r, &rel);
        if (rel != 1u || r.issued != r.released) {
            printf("  HTTP-only settled %u times (issued=%u released=%u)\n",
                   rel, r.issued, r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
    }

    /* SIGNAL + HTTP, across all three emission outcomes.
     *
     * A local signal-sink short write or failure does not make the rendered
     * snapshot invalid, so it must NOT cost the HTTP waiter its scrape: the
     * unchanged complete bytes are committed and delivered in every case. */
    for (uint32_t mi = 0; mi < 3u; mi++) {
        rig_t r;
        uint32_t c;
        uint64_t s = 0;
        uint32_t demand = 0, bank = 0, rel = 0;
        bool wake = false, committed = false;
        char out[512];
        static const char *const DOC = "moqrelay_mixed 1\n";
        failures += rig_init(&r);
        emit_calls = 0; emit_bytes = 0; emit_saw_committed = false;
        emit_mode = modes[mi].mode;

        MOQ_TEST_CHECK_EQ_INT(moqr_admin_accept(&r.a, 0, &c), MOQR_OK);
        MOQ_TEST_CHECK_EQ_INT(
            moqr_admin_on_bytes(&r.a, c, GET_OM, strlen(GET_OM), 0), MOQR_OK);
        failures += rig_pump_demands(&r, 0);
        s = r.a.client[c].serial;
        {
            uint64_t sig = 0;
            MOQ_TEST_CHECK_EQ_INT(
                moqr_broker_request(&r.b, MOQR_BROKER_DEMAND_SIGNAL, &sig,
                                    &wake), MOQR_OK);
            if (sig != s) {
                printf("  mixed/%s: SIGNAL did not join the HTTP generation\n",
                       modes[mi].name);
                failures++;
            }
        }
        moqr_broker_on_complete(&r.b, s);
        MOQ_TEST_CHECK(moqr_broker_take_serial(&r.b, s, &demand, &bank));
        r.observed_demand |= demand;
        if ((demand & MOQR_BROKER_DEMAND_SIGNAL) == 0u ||
            (demand & MOQR_BROKER_DEMAND_HTTP) == 0u) {
            printf("  mixed/%s: the token lost a demand bit (0x%x)\n",
                   modes[mi].name, demand);
            failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_reserve(&r.a, bank, s), MOQR_OK);
        failures += owner_sequence(&r, demand, bank, s, DOC, &committed);

        /* Attempted exactly once, and never retried. */
        if (emit_calls != 1) {
            printf("  mixed/%s emitted %d times, want 1\n", modes[mi].name,
                   emit_calls);
            failures++;
        }
        if (emit_saw_committed) {
            printf("  mixed/%s emitted the projection after commit\n",
                   modes[mi].name);
            failures++;
        }
        /* What the sink recorded differs by outcome; what HTTP receives does
         * not. */
        switch (modes[mi].mode) {
        case EMIT_OK:
            failures += check_emitted(DOC, "mixed/ok");
            break;
        case EMIT_SHORT:
            if (emit_bytes != 1u ||
                memcmp(emit_capture, DOC, 1u) != 0) {
                printf("  mixed/short recorded %zu bytes, want the 1-byte "
                       "prefix\n", emit_bytes);
                failures++;
            }
            break;
        default:
            if (emit_bytes != 0u) {
                printf("  mixed/fail recorded %zu bytes, want 0\n",
                       emit_bytes);
                failures++;
            }
            break;
        }
        /* The HTTP waiter is served identically in all three cases. */
        if (!committed) {
            printf("  mixed/%s did not commit for its HTTP waiter\n",
                   modes[mi].name);
            failures++;
        }
        if (moqr_admin_bank_storage(&r.a, bank, s,
                                    MOQR_OBS_FMT_OPENMETRICS_100,
                                    NULL) != NULL) {
            printf("  mixed/%s left bank storage readable after commit\n",
                   modes[mi].name);
            failures++;
        }
        MOQ_TEST_CHECK_EQ_INT(moqr_admin_tick(&r.a, 0), MOQR_OK);
        {
            size_t got = drain_client(&r, c, out, sizeof(out), 0);
            char what[32];
            (void)snprintf(what, sizeof(what), "mixed/%s", modes[mi].name);
            failures += check_body(out, got, DOC, what);
        }
        failures += settle_all(&r, &rel);
        if (rel != 1u || r.issued != r.released) {
            printf("  mixed/%s settled %u times (issued=%u released=%u)\n",
                   modes[mi].name, rel, r.issued, r.released);
            failures++;
        }
        moqr_admin_destroy(&r.a);
        moqr_broker_destroy(&r.b);
    }
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_joiners_share_the_collecting_serial();
    failures += test_post_freeze_gets_next_serial();
    failures += test_bank_exhaustion_refuses_immediately();
    failures += test_token_conservation();
    failures += test_prebank_timeout_retires_the_generation();
    failures += test_one_waiter_leaves_shared_generation();
    failures += test_all_waiters_leave_and_shutdown();
    failures += test_broker_bank_is_authoritative();
    failures += test_serial_exhaustion();
    failures += test_fail_serial();
    failures += test_fail_serial_before_delivery();
    failures += test_leave_during_render();
    failures += test_rejoin_windows();
    failures += test_retirement_phases();
    failures += test_demand_mask_sequences();
    if (loop_overruns != 0) {
        printf("  %d bounded loop(s) overran their cardinality\n",
               loop_overruns);
        failures += loop_overruns;
    }
    if (failures != 0) {
        printf("FAIL: %d admin/broker integration violation(s)\n", failures);
        return 1;
    }
    printf("OK\n");
    return 0;
}
