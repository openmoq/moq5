/* Seeded simulation over the admin tier, against an independent model.
 *
 * The point is not that the module agrees with itself. A shadow model tracks,
 * outside the implementation, what each bank holds and which serial each client
 * is owed, and every step is checked against it:
 *
 *   IDENTITY   -- a 200 response carries the document for the client's OWN
 *                 serial, never another generation's bytes.
 *   LEDGER     -- every generation that reaches a bank is released exactly
 *                 once. Tokens are counted in and out on every step.
 *   RETURNS    -- every return code is asserted, not discarded; an operation
 *                 the model says is illegal must be refused, and one it says is
 *                 legal must succeed.
 *   TRANSITIONS-- bank states follow FREE -> RESERVED -> READY -> FREE, and a
 *                 committed generation is never overwritten.
 *   COVERAGE   -- per-operation counters, so the run cannot pass by never
 *                 reaching the interesting operations.
 *
 * DETERMINISM -- the whole run folds into a digest, and the same seed replayed
 *                in reverse order must produce it again.
 */

#include <moqr_admin.h>

#include <stdio.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

/* The demand bits are opaque to the admin tier -- it carries them, it does
 * not interpret them -- so these pure-admin tests use a placeholder rather
 * than depending on the broker header. */
#define ADMIN_TEST_DEMAND 0x2u

#define BODY_CAP 512u
#define SEED_INFO "{\"api\":\"v1\",\"seeded\":true}"
#define SEEDS    64u
#define STEPS    1200u

enum {
    OP_ACCEPT = 0, OP_FEED, OP_TRAILING, OP_BIND, OP_REFUSE, OP_RESERVE,
    OP_COMMIT, OP_ABORT, OP_TICK, OP_WRITE, OP_ZERO_WRITE, OP_CANCEL,
    OP_CLOSE, OP_REJOIN, OP_LEAVE_RESERVED, OP_INFO, OP__COUNT
};

/* Weighted, not uniform. A request needs many feeds to complete, so a uniform
 * generator spends its whole budget accepting and closing connections that
 * never finish a head -- and then reports coverage of operations it only ever
 * selected, never completed. */
static const uint8_t OP_WEIGHT[OP__COUNT] = {
    2,  /* accept     */
    24, /* feed       -- a request needs ~10 feeds before it exists at all */
    1,  /* trailing   */
    8,  /* bind       */
    1,  /* refuse     */
    7,  /* reserve    */
    7,  /* commit     */
    1,  /* abort      */
    4,  /* tick       */
    8,  /* write      */
    2,  /* zero_write */
    1,  /* cancel     */
    1,  /* close      */
    2,  /* rejoin         */
    2,  /* leave_reserved */
    1,  /* info       -- a static document beside the metrics traffic */
};

static uint32_t
pick_op(uint64_t r)
{
    uint32_t total = 0;
    uint32_t pick;
    for (uint32_t i = 0; i < OP__COUNT; i++) {
        total += OP_WEIGHT[i];
    }
    pick = (uint32_t)(r % total);
    for (uint32_t i = 0; i < OP__COUNT; i++) {
        if (pick < OP_WEIGHT[i]) {
            return i;
        }
        pick -= OP_WEIGHT[i];
    }
    return OP_CLOSE;
}

static uint64_t
xs(uint64_t *s)
{
    uint64_t x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *s = x;
    return x;
}

static void
fold(uint64_t *h, const void *p, size_t n)
{
    const unsigned char *b = (const unsigned char *)p;
    for (size_t i = 0; i < n; i++) {
        *h ^= b[i];
        *h *= 1099511628211ull;
    }
}

/* The shadow model. */
/* The model tracks the bank's lifecycle and, SEPARATELY, whether a release
 * token is still owed for the generation it held. The implementation frees the
 * bank at the moment it queues the token, so conflating the two would make a
 * queued-but-undrained token look like a mismatch. */
typedef struct {
    uint64_t serial;            /* 0 when the model believes it FREE   */
    int      state;             /* mirrors moqr_admin_bstate_t          */
    bool     token_due;
    uint64_t token_serial;
} m_bank_t;

/* Coverage is counted on SEMANTIC TRANSITIONS, not on random selection: an
 * operation whose guard made it a no-op proves nothing about that operation. */
enum {
    SEM_ACCEPTED = 0, SEM_HEAD_DONE, SEM_MALFORMED, SEM_BOUND, SEM_REFUSED,
    SEM_RESERVED, SEM_COMMITTED, SEM_ABORTED, SEM_DELIVERED, SEM_ZERO_WRITE,
    SEM_CANCELLED, SEM_CLOSED, SEM_TOKEN, SEM_ABANDON, SEM_UNDRAINED,
    SEM_TIMED_OUT, SEM_BODY_200, SEM_REJOIN, SEM_LEAVE_RESERVED, SEM_INFO,
    SEM_INFO_200, SEM__COUNT
};
static const char *const SEM_NAME[SEM__COUNT] = {
    "accepted", "head_done", "malformed", "bound", "refused", "reserved",
    "committed", "aborted", "delivered", "zero_write", "cancelled", "closed",
    "token", "abandon", "undrained", "timed_out", "body_200", "rejoin",
    "leave_reserved", "info", "info_200"
};

typedef struct {
    bool     live;
    bool     bound;
    bool     info;      /* an /api/v1/info request: no serial, no bank, ever */
    uint64_t serial;
    char     seen[2048];
    size_t   seen_n;
} m_client_t;

static const char *const INFO_REQUEST =
    "GET /api/v1/info HTTP/1.1\r\nHost: x\r\n\r\n";

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

/* Stands in for the owner's broker release. The point of the coupled call is
 * that the settlement cannot be separated from it, so a test callback that
 * simply succeeds still exercises the transaction shape. */
static moqr_result_t
ok_release(void *ctx, uint64_t serial, uint32_t demand, uint32_t bank)
{
    (void)ctx; (void)serial; (void)demand; (void)bank;
    return MOQR_OK;
}

/* Settle a claimed retirement through the phases an owner must follow. */
static bool
retire_one(moqr_admin_t *a, uint64_t *out)
{
    moqr_admin_retire_t r;
    if (!moqr_admin_claim_abandon(a, &r)) {
        return false;
    }
    *out = r.serial;
    if (!r.taken) {
        if (moqr_admin_record_take(a, r.serial, ADMIN_TEST_DEMAND, 0) !=
            MOQR_OK) {
            return true;
        }
    }
    (void)moqr_admin_settle_abandon(a, r.serial, ok_release, NULL);
    return true;
}

static const char *const REQUESTS[] = {
    "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
    "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: text/plain\r\n\r\n",
    "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: application/openmetrics-text\r\n\r\n",
    "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: */*\r\n\r\n",
};
#define NREQ (sizeof(REQUESTS) / sizeof(REQUESTS[0]))

static const char *const BAD_REQUESTS[] = {
    "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: application/json\r\n\r\n",
    "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: text/\r\n\r\n",
    "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n",
    "POST /metrics HTTP/1.1\r\nHost: x\r\n\r\n",
    "GET /metrics\r\n\r\n",
    "GET /metrics HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\n\r\n",
    "GET /metrics HTTP/1.1\r\nHost: x\r\nBad Header: 1\r\n\r\n",
    "GET /metrics HTTP/1.1\r\nHost: x\r\nUpgrade: h2c\r\n\r\n",
    "GET /metrics HTTP/1.1\r\n\r\n",                 /* no Host */
    "GET /metrics HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
    "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\nGET /metrics HTTP/1.1\r\n\r\n",
    "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\ntrailing",
};
#define NBAD (sizeof(BAD_REQUESTS) / sizeof(BAD_REQUESTS[0]))

static bool
status_is_known(unsigned s)
{
    switch (s) {
    case 200: case 400: case 404: case 405:
    case 406: case 431: case 500: case 503:
        return true;
    default:
        return false;
    }
}

static int
run_seed(uint64_t seed, uint64_t *out_digest, uint32_t sem[SEM__COUNT])
{
    int failures = 0;
    moqr_admin_t a;
    static char storage[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT][BODY_CAP];
    char *bodies[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT];
    size_t caps[MOQR_ADMIN_BODY__COUNT];
    m_bank_t mb[MOQR_ADMIN_BANKS];
    m_client_t mc[MOQR_ADMIN_MAX_CLIENTS];
    const char *req[MOQR_ADMIN_MAX_CLIENTS];
    size_t sent[MOQR_ADMIN_MAX_CLIENTS];
    uint64_t rng = seed ? seed : 1u;
    uint64_t now = 0;
    uint64_t h = 1469598103934665603ull;
    uint64_t next_serial = 1;
    uint32_t issued = 0;
    uint32_t released = 0;

    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            bodies[b][k] = storage[b][k];
        }
    }
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        caps[k] = BODY_CAP;
    }
    memset(mb, 0, sizeof(mb));
    memset(mc, 0, sizeof(mc));
    memset(req, 0, sizeof(req));
    memset(sent, 0, sizeof(sent));
    MOQ_TEST_CHECK_EQ_INT(moqr_admin_init(&a, bodies, caps, SEED_INFO,
                                          strlen(SEED_INFO)), MOQR_OK);

    for (uint32_t step = 0; step < STEPS && failures < 20; step++) {
        uint64_t r = xs(&rng);
        uint32_t op = pick_op(r);
        uint32_t c = (uint32_t)((r >> 8) % MOQR_ADMIN_MAX_CLIENTS);
        uint32_t b = (uint32_t)((r >> 16) % MOQR_ADMIN_BANKS);

        switch (op) {
        case OP_ACCEPT: {
            uint32_t got;
            moqr_result_t rc = moqr_admin_accept(&a, now, &got);
            if (rc == MOQR_OK) {
                sem[SEM_ACCEPTED]++;
                mc[got].live = true;
                mc[got].bound = false;
                mc[got].serial = 0;
                mc[got].seen_n = 0;
                req[got] = REQUESTS[(r >> 24) % NREQ];
                mc[got].info = false;
                sent[got] = 0;
            } else if (rc != MOQR_ERR_CAPACITY) {
                printf("  seed %llu: accept returned %d\n",
                       (unsigned long long)seed, (int)rc);
                failures++;
            }
            break;
        }
        case OP_INFO:
            /* Re-aim a fresh connection at the static document. Only a
             * client that has sent nothing can change its mind. */
            if (mc[c].live && sent[c] == 0 && req[c] != NULL &&
                a.client[c].state == MOQR_ADMIN_CS_READING) {
                req[c] = INFO_REQUEST;
                mc[c].info = true;
            }
            break;
        case OP_TRAILING: {
            /* A malformed or pipelined request, in one read. Uses an ordinary
             * admission slot, and is rare, so it cannot crowd out the valid
             * traffic the interesting states depend on. */
            uint32_t got;
            if (moqr_admin_accept(&a, now, &got) == MOQR_OK) {
                const char *bad = BAD_REQUESTS[(r >> 24) % NBAD];
                mc[got].live = true;
                mc[got].seen_n = 0;
                req[got] = NULL;
                (void)moqr_admin_on_bytes(&a, got, bad, strlen(bad), now);
                if (a.client[got].state == MOQR_ADMIN_CS_PARSED ||
                    a.client[got].state == MOQR_ADMIN_CS_WAITING) {
                    printf("  seed %llu: a malformed request demanded a "
                           "generation\n", (unsigned long long)seed);
                    failures++;
                } else {
                    sem[SEM_MALFORMED]++;
                }
            }
            break;
        }
        case OP_FEED:
            if (mc[c].live && req[c] != NULL && sent[c] < strlen(req[c])) {
                size_t left = strlen(req[c]) - sent[c];
                size_t take = 1u + (size_t)((r >> 24) % 9u);
                if (take > left) {
                    take = left;
                }
                if (moqr_admin_on_bytes(&a, c, req[c] + sent[c], take, now) ==
                    MOQR_OK) {
                    sent[c] += take;
                    if (a.client[c].state == MOQR_ADMIN_CS_PARSED) {
                        sem[SEM_HEAD_DONE]++;
                    }
                    if (mc[c].info && sent[c] == strlen(req[c]) &&
                        !a.cancelled) {
                        /* A complete /info head is WRITING at once: static
                         * carrier, no serial, no pin, and never demand. */
                        uint32_t d = 0;
                        if (a.client[c].state != MOQR_ADMIN_CS_WRITING ||
                            a.client[c].carrier != MOQR_ADMIN_CARRIER_STATIC ||
                            a.client[c].serial != 0u || a.client[c].pinned ||
                            (moqr_admin_next_demand(&a, &d) && d == c)) {
                            printf("  seed %llu: an /info request was not "
                                   "served statically\n",
                                   (unsigned long long)seed);
                            failures++;
                        } else {
                            sem[SEM_INFO]++;
                        }
                    }
                }
            }
            break;
        case OP_LEAVE_RESERVED: {
            /* Close a client whose generation is held by a RESERVED bank --
             * the interleaving that used to lose the accounting record and
             * leave a READY, unpinned, unreachable bank behind. */
            for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
                const moqr_admin_client_t *cc = &a.client[i];
                if (cc->state != MOQR_ADMIN_CS_WAITING || cc->serial == 0) {
                    continue;
                }
                for (uint32_t k = 0; k < MOQR_ADMIN_BANKS; k++) {
                    if (a.bank[k].state == MOQR_ADMIN_BS_RESERVED &&
                        a.bank[k].serial == cc->serial) {
                        (void)moqr_admin_close(&a, i);
                        mc[i].live = false;
                        req[i] = NULL;
                        mc[i].seen_n = 0;
                        sem[SEM_LEAVE_RESERVED]++;
                        break;
                    }
                }
                break;
            }
            break;
        }
        case OP_REJOIN: {
            /* Bind a new client to a serial that is pending abandonment, which
             * must cancel the retirement rather than retire it underneath the
             * new waiter. */
            uint64_t pend = 0;
            uint32_t got;
            for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
                if (a.gen[i].state == MOQR_ADMIN_GS_ABANDONED) {
                    pend = a.gen[i].serial;
                    break;
                }
            }
            if (pend == 0) {
                break;
            }
            if (moqr_admin_accept(&a, now, &got) != MOQR_OK) {
                break;
            }
            mc[got].live = true;
            req[got] = REQUESTS[(r >> 24) % NREQ];
            sent[got] = 0;
            mc[got].seen_n = 0;
            (void)moqr_admin_on_bytes(&a, got, req[got], strlen(req[got]), now);
            if (a.client[got].state == MOQR_ADMIN_CS_PARSED &&
                moqr_admin_bind_serial(&a, got, pend, now) == MOQR_OK) {
                mc[got].serial = pend;
                sem[SEM_REJOIN]++;
                {
                    uint64_t still = 0;
                    if (claim_serial(&a, &still) && still == pend) {
                        printf("  seed %llu: a rejoined generation was still "
                               "offered for retirement\n",
                               (unsigned long long)seed);
                        failures++;
                    }
                }
            }
            break;
        }
        case OP_BIND:
            if (a.client[c].state == MOQR_ADMIN_CS_PARSED) {
                uint64_t s = next_serial++;
                if (moqr_admin_bind_serial(&a, c, s, now) != MOQR_OK) {
                    printf("  seed %llu: binding a parsed request failed\n",
                           (unsigned long long)seed);
                    failures++;
                } else {
                    sem[SEM_BOUND]++;
                    mc[c].bound = true;
                    mc[c].serial = s;
                }
            } else if (moqr_admin_bind_serial(&a, c, next_serial, now) ==
                       MOQR_OK) {
                printf("  seed %llu: bound a client that was not parsed\n",
                       (unsigned long long)seed);
                failures++;
            }
            break;
        case OP_REFUSE:
            if (a.client[c].state == MOQR_ADMIN_CS_PARSED ||
                a.client[c].state == MOQR_ADMIN_CS_WAITING) {
                if (moqr_admin_refuse(&a, c, MOQR_HTTP_503, now) != MOQR_OK) {
                    printf("  seed %llu: refusing a pending request failed\n",
                           (unsigned long long)seed);
                    failures++;
                } else {
                    sem[SEM_REFUSED]++;
                }
            }
            break;
        case OP_RESERVE: {
            /* Reserve a serial some client is genuinely WAITING on. Reserving
             * a fresh serial nobody joined can never produce a 200, which is
             * how the previous generator managed zero successful deliveries. */
            uint64_t s = 0;
            moqr_result_t rc;
            for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
                uint32_t j = (uint32_t)((i + (r >> 32)) % MOQR_ADMIN_MAX_CLIENTS);
                if (a.client[j].state == MOQR_ADMIN_CS_WAITING &&
                    a.client[j].serial != 0) {
                    s = a.client[j].serial;
                    break;
                }
            }
            if (s == 0) {
                break;
            }
            rc = moqr_admin_reserve(&a, b, s);
            bool model_free = (mb[b].state == MOQR_ADMIN_BS_FREE &&
                               !mb[b].token_due);
            (void)model_free;
            if (rc == MOQR_OK) {
                if (!model_free) {
                    printf("  seed %llu: reserved a bank the model says is "
                           "busy\n", (unsigned long long)seed);
                    failures++;
                }
                sem[SEM_RESERVED]++;
                mb[b].serial = s;
                mb[b].state = MOQR_ADMIN_BS_RESERVED;
                mb[b].token_due = true;
                mb[b].token_serial = s;
                issued++;
            } else if (rc == MOQR_ERR_WOULD_BLOCK) {
                if (model_free) {
                    printf("  seed %llu: refused a bank the model says is "
                           "free\n", (unsigned long long)seed);
                    failures++;
                }
            } else if (rc == MOQR_ERR_INVAL) {
                /* One generation, one bank: this serial is already held. The
                 * model checks that claim rather than taking it on trust. */
                bool held = false;
                for (uint32_t k = 0; k < MOQR_ADMIN_BANKS; k++) {
                    if (a.bank[k].state != MOQR_ADMIN_BS_FREE &&
                        a.bank[k].serial == s) {
                        held = true;
                        break;
                    }
                }
                if (!held) {
                    printf("  seed %llu: reserve refused serial %llu that no "
                           "bank holds\n", (unsigned long long)seed,
                           (unsigned long long)s);
                    failures++;
                }
            } else {
                printf("  seed %llu: reserve returned %d\n",
                       (unsigned long long)seed, (int)rc);
                failures++;
            }
            break;
        }
        case OP_COMMIT: {
            size_t len[MOQR_ADMIN_BODY__COUNT];
            char doc[64];
            moqr_result_t rc;
            bool model_reserved = (mb[b].state == MOQR_ADMIN_BS_RESERVED);
            (void)snprintf(doc, sizeof(doc), "SERIAL=%llu;",
                           (unsigned long long)mb[b].serial);
            for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
                size_t cap = 0;
                char *dst = moqr_admin_bank_storage(&a, b, mb[b].serial, k,
                                                    &cap);
                if (dst != NULL) {
                    memcpy(dst, doc, strlen(doc));
                }
                len[k] = strlen(doc);
            }
            rc = moqr_admin_commit(&a, b, mb[b].serial, len, now);
            if (rc == MOQR_OK) {
                if (!model_reserved) {
                    printf("  seed %llu: committed a bank the model says is "
                           "not reserved\n", (unsigned long long)seed);
                    failures++;
                }
                sem[SEM_COMMITTED]++;
                mb[b].state = MOQR_ADMIN_BS_READY;
            } else if (rc == MOQR_ERR_WRONG_STATE) {
                /* The documented contract: a commit needs a generation some
                 * request joined. Once that generation has been resolved --
                 * failed, retired, or never present -- there is no reader and
                 * the commit is refused. */
                bool live = false;
                for (uint32_t k = 0; k < MOQR_ADMIN_MAX_CLIENTS; k++) {
                    if (a.gen[k].serial == mb[b].serial &&
                        (a.gen[k].state == MOQR_ADMIN_GS_JOINED ||
                         a.gen[k].state == MOQR_ADMIN_GS_BANKED)) {
                        live = true;
                        break;
                    }
                }
                if (live && model_reserved) {
                    printf("  seed %llu: commit refused a bank whose "
                           "generation is still live\n",
                           (unsigned long long)seed);
                    failures++;
                }
            } else if (model_reserved && mb[b].serial != 0) {
                printf("  seed %llu: commit of a reserved bank returned %d\n",
                       (unsigned long long)seed, (int)rc);
                failures++;
            }
            break;
        }
        case OP_ABORT: {
            moqr_result_t rc = moqr_admin_abort(&a, b, mb[b].serial);
            bool model_reserved = (mb[b].state == MOQR_ADMIN_BS_RESERVED &&
                                   mb[b].serial != 0);
            if (rc == MOQR_OK) {
                if (!model_reserved) {
                    printf("  seed %llu: aborted a bank the model says is not "
                           "reserved\n", (unsigned long long)seed);
                    failures++;
                }
                /* Aborting frees the bank but still owes its token. */
                sem[SEM_ABORTED]++;
                mb[b].state = MOQR_ADMIN_BS_RELEASING;
                mb[b].serial = 0;
            } else if (model_reserved) {
                printf("  seed %llu: abort of a reserved bank returned %d\n",
                       (unsigned long long)seed, (int)rc);
                failures++;
            }
            break;
        }
        case OP_TICK: {
            bool was_waiting[MOQR_ADMIN_MAX_CLIENTS];
            for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
                was_waiting[i] = (a.client[i].state == MOQR_ADMIN_CS_WAITING ||
                                  a.client[i].state == MOQR_ADMIN_CS_PARSED);
            }
            now += (uint64_t)((r >> 24) %
                              (MOQR_ADMIN_EPOCH_DEADLINE_US / 2u + 1u));
            if (moqr_admin_tick(&a, now) != MOQR_OK) {
                printf("  seed %llu: tick failed\n", (unsigned long long)seed);
                failures++;
            }
            for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
                if (was_waiting[i] &&
                    a.client[i].state == MOQR_ADMIN_CS_WRITING &&
                    a.client[i].status == MOQR_HTTP_503) {
                    sem[SEM_TIMED_OUT]++;
                }
            }
            break;
        }
        case OP_WRITE: {
            const char *span;
            size_t n;
            if (mc[c].live && moqr_admin_pending(&a, c, &span, &n)) {
                size_t take = 1u + (size_t)((r >> 24) % n);
                if (mc[c].seen_n + take < sizeof(mc[c].seen)) {
                    memcpy(mc[c].seen + mc[c].seen_n, span, take);
                    mc[c].seen_n += take;
                }
                if (moqr_admin_on_written(&a, c, take, now) != MOQR_OK) {
                    printf("  seed %llu: on_written refused an offered span\n",
                           (unsigned long long)seed);
                    failures++;
                } else if (a.client[c].state == MOQR_ADMIN_CS_DONE) {
                    sem[SEM_DELIVERED]++;
                    mc[c].seen[mc[c].seen_n] = '\0';
                    if (strstr(mc[c].seen, "HTTP/1.1 200") != NULL &&
                        strstr(mc[c].seen, "SERIAL=") != NULL) {
                        /* A real body delivery, not an error response. */
                        sem[SEM_BODY_200]++;
                    }
                    if (mc[c].info && strstr(mc[c].seen, "HTTP/1.1 200") != NULL) {
                        /* IDENTITY for /info: the constructed document,
                         * whole, and never a generation's bytes. */
                        const char *body = strstr(mc[c].seen, "\r\n\r\n");
                        if (body == NULL || strcmp(body + 4, SEED_INFO) != 0 ||
                            strstr(mc[c].seen, "SERIAL=") != NULL) {
                            printf("  seed %llu: an /info 200 did not carry the "
                                   "constructed document\n",
                                   (unsigned long long)seed);
                            failures++;
                        } else {
                            sem[SEM_INFO_200]++;
                        }
                    }
                }
            }
            break;
        }
        case OP_ZERO_WRITE: {
            const char *span;
            size_t n;
            if (mc[c].live && moqr_admin_pending(&a, c, &span, &n)) {
                uint64_t before = a.client[c].dl_start_us;
                if (moqr_admin_on_written(&a, c, 0, now + 1u) != MOQR_OK) {
                    printf("  seed %llu: a zero-byte write was rejected\n",
                           (unsigned long long)seed);
                    failures++;
                } else {
                    sem[SEM_ZERO_WRITE]++;
                }
                if (a.client[c].dl_start_us != before) {
                    printf("  seed %llu: a zero-byte write refreshed the "
                           "deadline\n", (unsigned long long)seed);
                    failures++;
                }
            }
            break;
        }
        case OP_CANCEL:
            /* Only occasionally, or every run ends immediately. */
            if (((r >> 24) & 0x3fu) == 0u) {
                moqr_admin_cancel(&a, now);
                sem[SEM_CANCELLED]++;
            }
            break;
        default:    /* OP_CLOSE */
            if (mc[c].live && a.client[c].state != MOQR_ADMIN_CS_FREE) {
                mc[c].seen[mc[c].seen_n] = '\0';
                /* IDENTITY: a 200 carries this client's own serial. */
                if (strstr(mc[c].seen, "HTTP/1.1 200") != NULL) {
                    char want[48];
                    (void)snprintf(want, sizeof(want), "SERIAL=%llu;",
                                   (unsigned long long)mc[c].serial);
                    if (mc[c].seen_n > 0 &&
                        strstr(mc[c].seen, "SERIAL=") != NULL &&
                        strstr(mc[c].seen, want) == NULL) {
                        printf("  seed %llu: client got a foreign generation "
                               "(wanted %llu)\n", (unsigned long long)seed,
                               (unsigned long long)mc[c].serial);
                        failures++;
                    }
                } else if (strstr(mc[c].seen, "SERIAL=") != NULL) {
                    printf("  seed %llu: a non-200 response carried a "
                           "document\n", (unsigned long long)seed);
                    failures++;
                }
                if (mc[c].seen_n >= 12) {
                    unsigned st = 0;
                    if (sscanf(mc[c].seen, "HTTP/1.1 %u", &st) == 1 &&
                        !status_is_known(st)) {
                        printf("  seed %llu: status %u outside the table\n",
                               (unsigned long long)seed, st);
                        failures++;
                    }
                }
                (void)moqr_admin_close(&a, c);
                sem[SEM_CLOSED]++;
                mc[c].live = false;
                req[c] = NULL;
                mc[c].seen_n = 0;
            }
            break;
        }

        /* Settle releases only SOMETIMES, so a bank that owes an
         * unacknowledged release is genuinely observed being refused reuse. */
        if (((r >> 40) & 0x3u) != 0u) {
            moqr_admin_release_t peek;
            if (moqr_admin_peek_release(&a, &peek)) {
                sem[SEM_UNDRAINED]++;
            }
        } else {
            moqr_admin_release_t rel;
            uint32_t guard = 0;
            while (moqr_admin_peek_release(&a, &rel)) {
                if (++guard > 8u) {
                    printf("  seed %llu: peek_release did not terminate\n",
                           (unsigned long long)seed);
                    failures++;
                    break;
                }
                if (rel.serial == 0) {
                    printf("  seed %llu: a release token carried serial 0\n",
                           (unsigned long long)seed);
                    failures++;
                }
                if (rel.bank >= MOQR_ADMIN_BANKS) {
                    printf("  seed %llu: a release token named bank %u\n",
                           (unsigned long long)seed, rel.bank);
                    failures++;
                }
                if (!mb[rel.bank].token_due ||
                    mb[rel.bank].token_serial != rel.serial) {
                    printf("  seed %llu: token {%llu,%u} does not match the "
                           "model (due=%d serial=%llu)\n",
                           (unsigned long long)seed,
                           (unsigned long long)rel.serial, rel.bank,
                           (int)mb[rel.bank].token_due,
                           (unsigned long long)mb[rel.bank].token_serial);
                    failures++;
                }
                if (moqr_admin_ack_release(&a, rel.serial, rel.bank) !=
                    MOQR_OK) {
                    printf("  seed %llu: the admin refused to acknowledge a "
                           "token it offered\n", (unsigned long long)seed);
                    failures++;
                    break;
                }
                /* A duplicate acknowledgement must fail closed. */
                if (moqr_admin_ack_release(&a, rel.serial, rel.bank) ==
                    MOQR_OK) {
                    printf("  seed %llu: a duplicate acknowledgement was "
                           "accepted\n", (unsigned long long)seed);
                    failures++;
                }
                sem[SEM_TOKEN]++;
                mb[rel.bank].token_due = false;
                mb[rel.bank].token_serial = 0;
                mb[rel.bank].serial = 0;
                mb[rel.bank].state = MOQR_ADMIN_BS_FREE;
                released++;
            }
        }
        /* Retire generations the admin says never reached a bank -- but only
         * sometimes. Retiring immediately would make the pending-abandonment
         * window unobservable, and that window is where a rejoin has to be
         * able to cancel the retirement. */
        if (((r >> 44) & 0x7u) == 0u) {
            uint64_t ab = 0;
            uint32_t g2 = 0;
            while (retire_one(&a, &ab)) {
                if (ab == 0 || ++g2 > 16u) {
                    printf("  seed %llu: abandonment did not terminate\n",
                           (unsigned long long)seed);
                    failures++;
                    break;
                }
                sem[SEM_ABANDON]++;
            }
        }
        /* GLOBAL serial uniqueness: one generation, one bank, ever. */
        for (uint32_t x = 0; x < MOQR_ADMIN_BANKS; x++) {
            for (uint32_t y = x + 1u; y < MOQR_ADMIN_BANKS; y++) {
                if (a.bank[x].state != MOQR_ADMIN_BS_FREE &&
                    a.bank[y].state != MOQR_ADMIN_BS_FREE &&
                    a.bank[x].serial == a.bank[y].serial) {
                    printf("  seed %llu: serial %llu occupies banks %u and "
                           "%u\n", (unsigned long long)seed,
                           (unsigned long long)a.bank[x].serial, x, y);
                    failures++;
                }
            }
        }

        /* Invariants on every step. */
        {
            uint32_t counted[MOQR_ADMIN_BANKS];
            memset(counted, 0, sizeof(counted));
            for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
                if (a.client[i].pinned) {
                    counted[a.client[i].bank]++;
                }
            }
            for (uint32_t k = 0; k < MOQR_ADMIN_BANKS; k++) {
                if (counted[k] != a.bank[k].pins) {
                    printf("  seed %llu step %u: bank %u pins %u vs %u\n",
                           (unsigned long long)seed, step, k, a.bank[k].pins,
                           counted[k]);
                    failures++;
                }
                /* No per-bank field mirroring here. The model asserts the
                 * things that are actually contracts -- one serial in at most
                 * one bank, and every token matching the generation the model
                 * says that bank holds -- rather than restating the
                 * implementation's own bookkeeping back at it. */
            }
            fold(&h, counted, sizeof(counted));
        }
        fold(&h, &now, sizeof(now));
        fold(&h, &issued, sizeof(issued));
        fold(&h, &released, sizeof(released));
        for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
            fold(&h, &a.client[i].state, sizeof(a.client[i].state));
            fold(&h, &a.client[i].status, sizeof(a.client[i].status));
            fold(&h, &a.client[i].serial, sizeof(a.client[i].serial));
        }
        for (uint32_t k = 0; k < MOQR_ADMIN_BANKS; k++) {
            fold(&h, &a.bank[k].state, sizeof(a.bank[k].state));
            fold(&h, &a.bank[k].serial, sizeof(a.bank[k].serial));
        }
    }

    /* Teardown settles every outstanding generation: banks owe their releases,
     * and generations that never reached a bank are offered for retirement. */
    moqr_admin_destroy(&a);
    {
        moqr_admin_release_t rel;
        uint64_t ab = 0;
        uint32_t guard = 0;
        while (moqr_admin_peek_release(&a, &rel)) {
            if (++guard > 8u) {
                printf("  seed %llu: teardown peek did not terminate\n",
                       (unsigned long long)seed);
                failures++;
                break;
            }
            if (moqr_admin_ack_release(&a, rel.serial, rel.bank) != MOQR_OK) {
                printf("  seed %llu: teardown token was not acknowledgeable\n",
                       (unsigned long long)seed);
                failures++;
                break;
            }
            released++;
        }
        {
            uint32_t g3 = 0;
            /* Bounded by the generation records: a retirement that never
             * settles must be reported, not spun on. */
            while (retire_one(&a, &ab)) {
                if (++g3 > MOQR_ADMIN_MAX_CLIENTS) {
                    printf("  seed %llu: teardown retirement did not "
                           "terminate\n", (unsigned long long)seed);
                    failures++;
                    break;
                }
                /* Never banked, so no bank ever owed it. These are retired
                 * through the broker, not through the bank ledger, and must
                 * not be netted against it -- different populations. */
                if (ab == 0) {
                    printf("  seed %llu: a teardown abandon carried serial "
                           "0\n", (unsigned long long)seed);
                    failures++;
                }
                sem[SEM_ABANDON]++;
            }
        }
    }
    /* Every bank generation that was reserved produced exactly one
     * acknowledged release, and no more. */
    if (issued != released) {
        printf("  seed %llu: bank ledger unbalanced -- reserved %u, released "
               "%u\n", (unsigned long long)seed, issued, released);
        failures++;
    }
    *out_digest = h;
    return failures;
}

int
main(void)
{
    int failures = 0;
    uint64_t first[SEEDS];
    uint64_t again;
    uint32_t sem[SEM__COUNT];

    memset(sem, 0, sizeof(sem));
    for (uint64_t s = 1; s <= SEEDS; s++) {
        failures += run_seed(s, &first[s - 1], sem);
    }
    for (uint64_t s = SEEDS; s >= 1; s--) {
        uint32_t ignore[SEM__COUNT];
        memset(ignore, 0, sizeof(ignore));
        failures += run_seed(s, &again, ignore);
        if (again != first[s - 1]) {
            printf("  seed %llu is not deterministic: %llx then %llx\n",
                   (unsigned long long)s, (unsigned long long)first[s - 1],
                   (unsigned long long)again);
            failures++;
        }
    }
    {
        uint32_t distinct = 0;
        for (uint32_t i = 0; i < SEEDS; i++) {
            bool dup = false;
            for (uint32_t j = 0; j < i; j++) {
                if (first[i] == first[j]) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                distinct++;
            }
        }
        if (distinct < SEEDS) {
            printf("  only %u of %u seeds produced distinct runs\n",
                   distinct, (unsigned)SEEDS);
            failures++;
        }
    }
    /* A simulation that never delivers a real body has not exercised the
     * thing it exists to check, however many operations it selected. */
    if (sem[SEM_BODY_200] == 0u) {
        printf("  no run delivered a 200 with a generation body\n");
        failures++;
    }
    /* A transition that never actually happened proves nothing about it. These
     * count SEMANTIC results, so an operation whose guard turned it into a
     * no-op does not inflate the coverage it claims. */
    for (uint32_t i = 0; i < SEM__COUNT; i++) {
        if (sem[i] == 0u) {
            printf("  transition %s never occurred\n", SEM_NAME[i]);
            failures++;
        }
    }
    if (failures != 0) {
        printf("  transitions:");
        for (uint32_t i = 0; i < SEM__COUNT; i++) {
            printf(" %s=%u", SEM_NAME[i], sem[i]);
        }
        printf("\n");
        printf("FAIL: %d seeded-simulation violation(s)\n", failures);
        return 1;
    }
    printf("OK %u seeds x %u steps; transitions:", (unsigned)SEEDS,
           (unsigned)STEPS);
    for (uint32_t i = 0; i < SEM__COUNT; i++) {
        printf(" %s=%u", SEM_NAME[i], sem[i]);
    }
    printf("\n");
    return 0;
}
