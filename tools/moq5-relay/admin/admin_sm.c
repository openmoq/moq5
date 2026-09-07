/*
 * The deterministic admin state machine.
 *
 * Fixed-capacity clients and two fixed body banks. Time enters only through the
 * `now_us` parameter, so slow-client and overload behaviour is reproducible
 * from a transcript rather than dependent on scheduling.
 *
 * Three invariants everything here exists to hold:
 *
 *   GENERATION IDENTITY. A request is delivered the generation it joined, and
 *   no other. Attaching a waiter to "whichever bank is ready" silently serves
 *   stale bytes as soon as two generations are in flight, which is the normal
 *   case with two banks and one slow reader.
 *
 *   EXACT-ONCE RELEASE. Every generation that reaches a bank yields exactly one
 *   release token -- delivered, aborted, timed out, cancelled or torn down --
 *   so the broker bank it holds is neither leaked nor double-released.
 *
 *   A PINNED BANK IS NEVER WRITTEN. Storage is reachable only while RESERVED,
 *   and deliverable only while READY, so a renderer and a reader can never look
 *   at the same bytes.
 */
#include "moqr_admin.h"

#include <string.h>

/* Deadlines are a START and a BUDGET, never an absolute instant.
 *
 * An absolute deadline has to do something at the top of the range: saturating
 * makes the client immortal (nothing can ever exceed UINT64_MAX), and wrapping
 * expires it instantly. Elapsed time computed as `now - start` in unsigned
 * arithmetic is exact across the whole range, including a clock that wraps,
 * for any elapsed interval shorter than the full range -- which every budget
 * here is by many orders of magnitude. */
static void
arm(moqr_admin_client_t *c, uint64_t now_us, uint64_t budget)
{
    c->dl_start_us = now_us;
    c->dl_budget_us = budget;
}

static bool
expired(const moqr_admin_client_t *c, uint64_t now_us)
{
    return (uint64_t)(now_us - c->dl_start_us) > c->dl_budget_us;
}

/* The bank stops holding its generation and starts OWING a release. It is not
 * free, and its serial is not forgotten, until the broker acknowledges: the
 * token lives in the bank, so it cannot be dropped for want of queue space. */
static void
owe_release(moqr_admin_bank_t *bk)
{
    if (bk->state == MOQR_ADMIN_BS_FREE ||
        bk->state == MOQR_ADMIN_BS_RELEASING) {
        return;             /* nothing held, or already owed */
    }
    bk->state = MOQR_ADMIN_BS_RELEASING;
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        bk->body_len[k] = 0;
    }
}

/* -- in-flight generations (joined, not yet banked) ----------------------- */

static moqr_admin_gen_t *
gen_find(moqr_admin_t *a, uint64_t serial)
{
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (a->gen[i].state != MOQR_ADMIN_GS_FREE &&
            a->gen[i].serial == serial && serial != 0) {
            return &a->gen[i];
        }
    }
    return NULL;
}

/* The bank holding `serial`, or NULL. */
static moqr_admin_bank_t *
bank_of(moqr_admin_t *a, uint64_t serial, uint32_t *out_bank)
{
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (a->bank[b].state != MOQR_ADMIN_BS_FREE &&
            a->bank[b].serial == serial) {
            if (out_bank != NULL) {
                *out_bank = b;
            }
            return &a->bank[b];
        }
    }
    return NULL;
}

static moqr_admin_gen_t *
gen_join(moqr_admin_t *a, uint64_t serial)
{
    moqr_admin_gen_t *g = gen_find(a, serial);
    if (g != NULL) {
        if (g->state == MOQR_ADMIN_GS_RETIRING) {
            /* The caller has already told the broker to retire this serial.
             * Joining it now would resurrect a generation that is on its way
             * out; a new request opens a new one instead. */
            return NULL;
        }
        if (g->state == MOQR_ADMIN_GS_ABANDONED) {
            /* Retirement had not been offered yet, so it is simply cancelled:
             * this generation has a waiter again. */
            g->state = MOQR_ADMIN_GS_JOINED;
        }
        g->waiters++;
        return g;
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (a->gen[i].state == MOQR_ADMIN_GS_FREE) {
            a->gen[i].serial = serial;
            a->gen[i].waiters = 1;
            a->gen[i].state = MOQR_ADMIN_GS_JOINED;
            a->gen[i].token_taken = false;
            a->gen[i].token_demand = 0;
            a->gen[i].token_bank = 0;
            return &a->gen[i];
        }
    }
    return NULL;
}

static void
gen_release(moqr_admin_gen_t *g)
{
    g->serial = 0;
    g->waiters = 0;
    g->state = MOQR_ADMIN_GS_FREE;
    g->token_taken = false;
    g->token_demand = 0;
    g->token_bank = 0;
}

static bool
banked(const moqr_admin_t *a, uint64_t serial)
{
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (a->bank[b].state != MOQR_ADMIN_BS_FREE &&
            a->bank[b].serial == serial) {
            return true;
        }
    }
    return false;
}

/* A waiter has stopped waiting. If it was the last one and the generation never
 * reached a bank, it must be retired in the broker -- otherwise it collects
 * forever and every later request joins a dead epoch. */
static void
gen_leave(moqr_admin_t *a, uint64_t serial)
{
    moqr_admin_gen_t *g;

    if (serial == 0) {
        return;
    }
    g = gen_find(a, serial);
    if (g == NULL) {
        return;
    }
    if (g->waiters > 0) {
        g->waiters--;
    }
    if (g->waiters != 0) {
        return;
    }
    if (g->state == MOQR_ADMIN_GS_BANKED) {
        moqr_admin_bank_t *bk = bank_of(a, serial, NULL);
        if (bk == NULL) {
            gen_release(g);
            return;
        }
        if (bk->state == MOQR_ADMIN_BS_RESERVED) {
            /* The renderer still owns the storage. The record is KEPT so the
             * commit or abort that ends the reservation can resolve it; losing
             * it here is how a bank became READY, unpinned and unreachable,
             * with no release event left to fire. */
            return;
        }
        if (bk->pins == 0) {
            owe_release(bk);
        }
        gen_release(g);
        return;
    }
    /* Never banked: only the broker can retire it. */
    g->state = MOQR_ADMIN_GS_ABANDONED;
}

static bool
head_span(const char *b, size_t n, size_t *out_len)
{
    if (n < 4u) {
        return false;
    }
    for (size_t i = 0; i + 3 < n; i++) {
        if (b[i] == '\r' && b[i + 1] == '\n' &&
            b[i + 2] == '\r' && b[i + 3] == '\n') {
            *out_len = i + 4u;
            return true;
        }
    }
    return false;
}

static void
answer_error(moqr_admin_t *a, moqr_admin_client_t *c, moqr_http_status_t st,
             uint64_t now_us)
{
    /* Leaving a joined generation is part of failing this request: without it
     * the generation keeps a waiter that will never arrive. */
    gen_leave(a, c->serial);
    c->status = st;
    c->body = NULL;
    c->body_len = 0;
    c->body_sent = 0;
    c->serial = 0;
    c->carrier = MOQR_ADMIN_CARRIER_NONE;
    c->head_len = moqr_http_write_head(st, MOQR_HTTP_REP_METRICS,
                                       MOQR_OBS_FMT_OPENMETRICS_100, 0,
                                       c->head, sizeof(c->head));
    c->head_sent = 0;
    c->bytes_out = 0;
    c->state = MOQR_ADMIN_CS_WRITING;
    /* A generated error gets a FRESH write deadline. Inheriting the deadline
     * that produced it -- the epoch timeout, already in the past -- would have
     * the next tick drop the response before a byte of it could be written. */
    arm(c, now_us, MOQR_ADMIN_WRITE_DEADLINE_US);
}

/*
 * The STATIC path: /api/v1/info goes from its parsed request straight to a
 * 200 over the immutable document the machine was constructed with. There is
 * no generation to join, no broker demand, no bank to pin and no release to
 * owe, so nothing that later ends this client -- completion, timeout, partial
 * write, cancellation, close, teardown -- can touch a bank or a generation:
 * its serial is zero (which every cleanup already treats as "none") and its
 * carrier says the body is not a bank.
 */
static void
start_static_write(moqr_admin_t *a, moqr_admin_client_t *c, uint64_t now_us)
{
    c->serial = 0;
    c->pinned = false;
    c->bank = MOQR_ADMIN_BANKS;   /* deliberately not a bank index */
    c->carrier = MOQR_ADMIN_CARRIER_STATIC;
    c->body = a->info;
    c->body_len = a->info_len;
    c->body_sent = 0;
    c->head_len = moqr_http_write_head(MOQR_HTTP_200, MOQR_HTTP_REP_JSON,
                                       MOQR_OBS_FMT__COUNT, c->body_len,
                                       c->head, sizeof(c->head));
    if (c->head_len == 0) {
        answer_error(a, c, MOQR_HTTP_500, now_us);
        return;
    }
    c->head_sent = 0;
    c->bytes_out = 0;
    c->state = MOQR_ADMIN_CS_WRITING;
    arm(c, now_us, MOQR_ADMIN_WRITE_DEADLINE_US);
}

static void
release_pin(moqr_admin_t *a, moqr_admin_client_t *c)
{
    if (!c->pinned) {
        return;
    }
    c->pinned = false;
    c->body = NULL;
    c->body_len = 0;
    if (c->bank >= MOQR_ADMIN_BANKS) {
        return;
    }
    {
        moqr_admin_bank_t *bk = &a->bank[c->bank];
        if (bk->pins > 0) {
            bk->pins--;
        }
        if (bk->pins == 0) {
            owe_release(bk);
        }
    }
}

/* Drop a client without answering: there is either no request to answer, or a
 * response already partway onto the wire that cannot be rewound. */
static void
drop_client(moqr_admin_t *a, moqr_admin_client_t *c)
{
    gen_leave(a, c->serial);
    release_pin(a, c);
    c->carrier = MOQR_ADMIN_CARRIER_NONE;
    c->head_len = 0;
    c->head_sent = 0;
    c->body = NULL;
    c->body_len = 0;
    c->body_sent = 0;
    c->bytes_out = 0;
    c->serial = 0;
    c->state = MOQR_ADMIN_CS_DONE;
}

moqr_result_t
moqr_admin_init(moqr_admin_t *a,
                char *bodies[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT],
                const size_t caps[MOQR_ADMIN_BODY__COUNT],
                const char *info, size_t info_len)
{
    if (a == NULL || bodies == NULL || caps == NULL) {
        return MOQR_ERR_INVAL;
    }
    /* The document contract is explicit: a machine that serves the target
     * must have been handed a complete document to serve. */
    if (info == NULL || info_len == 0u ||
        info_len > MOQR_ADMIN_MAX_STATIC_DOC) {
        return MOQR_ERR_INVAL;
    }
    memset(a, 0, sizeof(*a));
    a->info = info;
    a->info_len = info_len;
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
            if (bodies[b][k] == NULL || caps[k] == 0) {
                return MOQR_ERR_INVAL;
            }
            a->bank[b].body[k] = bodies[b][k];
            a->bank[b].body_cap[k] = caps[k];
        }
        a->bank[b].state = MOQR_ADMIN_BS_FREE;
    }
    return MOQR_OK;
}

void
moqr_admin_destroy(moqr_admin_t *a)
{
    if (a == NULL) {
        return;
    }
    /* Teardown still owes a token for every generation a bank holds; dropping
     * them here would leak broker banks across a restart. */
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        release_pin(a, &a->client[i]);
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        owe_release(&a->bank[b]);
    }
    /* A generation joined but never banked owes nothing to a bank, so only the
     * broker can retire it. Dropping it here would leak a broker generation
     * across a restart -- the same starvation as an expired pre-bank waiter,
     * just triggered by shutdown instead of a deadline. */
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (a->gen[i].state == MOQR_ADMIN_GS_JOINED) {
            a->gen[i].waiters = 0;
            a->gen[i].state = MOQR_ADMIN_GS_ABANDONED;
        }
    }
}

moqr_result_t
moqr_admin_accept(moqr_admin_t *a, uint64_t now_us, uint32_t *out_client)
{
    if (a == NULL || out_client == NULL) {
        return MOQR_ERR_INVAL;
    }
    if (a->cancelled) {
        return MOQR_ERR_CAPACITY;
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (a->client[i].state != MOQR_ADMIN_CS_FREE) {
            continue;
        }
        memset(&a->client[i], 0, sizeof(a->client[i]));
        a->client[i].state = MOQR_ADMIN_CS_READING;
        arm(&a->client[i], now_us, MOQR_ADMIN_READ_DEADLINE_US);
        a->clients_used++;
        *out_client = i;
        return MOQR_OK;
    }
    return MOQR_ERR_CAPACITY;
}

moqr_result_t
moqr_admin_on_bytes(moqr_admin_t *a, uint32_t client, const char *data,
                    size_t len, uint64_t now_us)
{
    moqr_admin_client_t *c;
    size_t head_len = 0;

    if (a == NULL || client >= MOQR_ADMIN_MAX_CLIENTS || data == NULL) {
        return MOQR_ERR_INVAL;
    }
    c = &a->client[client];
    if (c->state != MOQR_ADMIN_CS_READING) {
        /* The head was already complete. Anything further on this connection is
         * a pipelined request or an unframed body, and v1 serves one request
         * per connection. The boundary does NOT end when a response is
         * selected: it ends when a byte of that response is on the wire.
         *
         * Return contract: MOQR_OK whenever a response was constructed or an
         * existing one was dropped -- the caller keeps draining its pending
         * output either way. MOQR_ERR_WRONG_STATE only when there was nothing
         * left to act on. */
        switch (c->state) {
        case MOQR_ADMIN_CS_PARSED:
        case MOQR_ADMIN_CS_WAITING:
            answer_error(a, c, MOQR_HTTP_400, now_us);
            return MOQR_OK;
        case MOQR_ADMIN_CS_WRITING:
            if (c->bytes_out == 0u) {
                /* Nothing is committed to the wire yet, so the selected 200 can
                 * still be replaced -- after giving back the bank it pinned. */
                release_pin(a, c);
                answer_error(a, c, MOQR_HTTP_400, now_us);
            } else {
                /* The status line has gone out; it cannot be rewritten, so the
                 * connection is dropped and the pin released. */
                drop_client(a, c);
            }
            return MOQR_OK;
        default:
            return MOQR_ERR_WRONG_STATE;
        }
    }

    if (len > MOQR_ADMIN_MAX_REQUEST - c->req_len) {
        answer_error(a, c, MOQR_HTTP_431, now_us);
        return MOQR_OK;
    }
    memcpy(c->req + c->req_len, data, len);
    c->req_len += len;

    if (!head_span(c->req, c->req_len, &head_len)) {
        return MOQR_OK;     /* still reading: not yet a decision */
    }
    /* v1 is ONE request per connection. Anything after the terminated head --
     * a second request, an unframed body, or noise -- is a protocol error, in
     * this read or a later one. Keeping it would let a pipelined second
     * request ride in on the first one's admission. */
    if (head_len != c->req_len) {
        answer_error(a, c, MOQR_HTTP_400, now_us);
        return MOQR_OK;
    }

    {
        moqr_http_status_t st = moqr_http_parse(c->req, head_len, &c->parsed);
        if (st != MOQR_HTTP_200) {
            answer_error(a, c, st, now_us);
            return MOQR_OK;
        }
    }
    if (a->cancelled) {
        answer_error(a, c, MOQR_HTTP_503, now_us);
        return MOQR_OK;
    }
    c->status = MOQR_HTTP_200;
    c->serial = 0;
    /* Dispatch on the VALIDATED (target, representation) pair. The static
     * document needs no generation; the metrics document needs exactly the
     * one the caller will assign. Any other pair is a parser contradiction. */
    if (c->parsed.target == MOQR_HTTP_TARGET_INFO &&
        c->parsed.rep == MOQR_HTTP_REP_JSON) {
        start_static_write(a, c, now_us);
        return MOQR_OK;
    }
    if (!((c->parsed.target == MOQR_HTTP_TARGET_METRICS &&
           c->parsed.rep == MOQR_HTTP_REP_METRICS) ||
          (c->parsed.target == MOQR_HTTP_TARGET_SHARDS &&
           c->parsed.rep == MOQR_HTTP_REP_JSON))) {
        answer_error(a, c, MOQR_HTTP_500, now_us);
        return MOQR_OK;
    }
    /* PARSED, not WAITING: this request has no generation until the caller
     * assigns it one from the broker. */
    c->state = MOQR_ADMIN_CS_PARSED;
    arm(c, now_us, MOQR_ADMIN_EPOCH_DEADLINE_US);
    return MOQR_OK;
}

bool
moqr_admin_next_demand(const moqr_admin_t *a, uint32_t *out_client)
{
    if (a == NULL || out_client == NULL) {
        return false;
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (a->client[i].state == MOQR_ADMIN_CS_PARSED) {
            *out_client = i;
            return true;
        }
    }
    return false;
}

moqr_result_t
moqr_admin_bind_serial(moqr_admin_t *a, uint32_t client, uint64_t serial,
                       uint64_t now_us)
{
    moqr_admin_client_t *c;

    if (a == NULL || client >= MOQR_ADMIN_MAX_CLIENTS) {
        return MOQR_ERR_INVAL;
    }
    /* Zero is the broker's "no generation" value. Admitting it as an identity
     * would make every unbound bank match every waiter. */
    if (serial == 0) {
        return MOQR_ERR_INVAL;
    }
    c = &a->client[client];
    if (c->state != MOQR_ADMIN_CS_PARSED) {
        return MOQR_ERR_WRONG_STATE;
    }
    if (gen_join(a, serial) == NULL) {
        return MOQR_ERR_CAPACITY;
    }
    c->serial = serial;
    c->state = MOQR_ADMIN_CS_WAITING;
    arm(c, now_us, MOQR_ADMIN_EPOCH_DEADLINE_US);
    return MOQR_OK;
}

moqr_result_t
moqr_admin_refuse(moqr_admin_t *a, uint32_t client, moqr_http_status_t status,
                  uint64_t now_us)
{
    moqr_admin_client_t *c;

    if (a == NULL || client >= MOQR_ADMIN_MAX_CLIENTS) {
        return MOQR_ERR_INVAL;
    }
    /* A refusal is never a success, and never a code the finite table does not
     * contain. Validated BEFORE anything is mutated, so a bad status cannot
     * leave a client WRITING a zero-length response with nothing pending. */
    switch (status) {
    case MOQR_HTTP_400: case MOQR_HTTP_404: case MOQR_HTTP_405:
    case MOQR_HTTP_406: case MOQR_HTTP_431: case MOQR_HTTP_500:
    case MOQR_HTTP_503:
        break;
    default:
        return MOQR_ERR_INVAL;
    }
    c = &a->client[client];
    if (c->state != MOQR_ADMIN_CS_PARSED && c->state != MOQR_ADMIN_CS_WAITING) {
        return MOQR_ERR_WRONG_STATE;
    }
    answer_error(a, c, status, now_us);
    if (c->head_len == 0) {
        /* The head could not be built. Refusing to leave a client in WRITING
         * with nothing to send matters more than the status we wanted. */
        drop_client(a, c);
        return MOQR_ERR_INTERNAL;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_admin_reserve(moqr_admin_t *a, uint32_t bank, uint64_t serial)
{
    moqr_admin_bank_t *bk;

    if (a == NULL || bank >= MOQR_ADMIN_BANKS || serial == 0) {
        return MOQR_ERR_INVAL;
    }
    /* One broker generation, one bank. Two banks holding one serial would owe
     * two release tokens for a generation the broker issued once. */
    if (banked(a, serial)) {
        return MOQR_ERR_INVAL;
    }
    bk = &a->bank[bank];
    if (bk->state != MOQR_ADMIN_BS_FREE || bk->pins != 0) {
        /* Reserved, holding an undelivered generation, being written from, or
         * still owing an unacknowledged release: the storage is not ours. */
        return MOQR_ERR_WOULD_BLOCK;
    }
    /* Banking CANCELS a pending abandonment: the bank now owes this
     * generation's release, so it must never also be offered for broker
     * retirement. A serial already RETIRING cannot be banked -- the caller has
     * told the broker it is going away. */
    {
        moqr_admin_gen_t *g = gen_find(a, serial);
        if (g != NULL) {
            if (g->state == MOQR_ADMIN_GS_RETIRING) {
                return MOQR_ERR_WRONG_STATE;
            }
            g->state = MOQR_ADMIN_GS_BANKED;
        }
    }
    bk->serial = serial;
    bk->state = MOQR_ADMIN_BS_RESERVED;
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        bk->body_len[k] = 0;
    }
    return MOQR_OK;
}

char *
moqr_admin_bank_storage(moqr_admin_t *a, uint32_t bank, uint64_t serial,
                        moqr_admin_body_t slot, size_t *out_cap)
{
    moqr_admin_bank_t *bk;

    if (a == NULL || bank >= MOQR_ADMIN_BANKS || slot >= MOQR_ADMIN_BODY__COUNT) {
        return NULL;
    }
    bk = &a->bank[bank];
    if (bk->state != MOQR_ADMIN_BS_RESERVED || bk->serial != serial ||
        serial == 0) {
        return NULL;
    }
    if (out_cap != NULL) {
        *out_cap = bk->body_cap[slot];
    }
    return bk->body[slot];
}

moqr_result_t
moqr_admin_commit(moqr_admin_t *a, uint32_t bank, uint64_t serial,
                  const size_t len[MOQR_ADMIN_BODY__COUNT], uint64_t now_us)
{
    moqr_admin_bank_t *bk;

    if (a == NULL || bank >= MOQR_ADMIN_BANKS || len == NULL || serial == 0) {
        return MOQR_ERR_INVAL;
    }
    bk = &a->bank[bank];
    if (bk->state != MOQR_ADMIN_BS_RESERVED) {
        return MOQR_ERR_WRONG_STATE;
    }
    if (bk->serial != serial) {
        /* Committing under a different serial than was reserved would publish
         * one generation's bytes under another's identity. */
        return MOQR_ERR_INVAL;
    }
    /* Validate EVERY length before storing any. A commit that stored the first
     * format and then refused the second would leave a half-described body
     * behind. */
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        if (len[k] > bk->body_cap[k]) {
            return MOQR_ERR_CAPACITY;
        }
    }
    /* A bank rendered for demand that is not an HTTP request has no reader
     * here, and would sit READY forever. Signal-only output is read
     * synchronously from the RESERVED bank and then aborted. */
    {
        moqr_admin_gen_t *g = gen_find(a, serial);
        if (g == NULL || g->state != MOQR_ADMIN_GS_BANKED) {
            return MOQR_ERR_WRONG_STATE;
        }
    }
    for (uint32_t k = 0; k < MOQR_ADMIN_BODY__COUNT; k++) {
        bk->body_len[k] = len[k];
    }
    (void)now_us;
    bk->state = MOQR_ADMIN_BS_READY;
    a->publish_gen++;
    {
        /* The last waiter left while the renderer held the bank. The commit
         * still succeeds -- the renderer is entitled to finish -- but the bank
         * owes its release at once rather than becoming a READY, unpinned
         * generation nobody will ever ask for. */
        moqr_admin_gen_t *g = gen_find(a, serial);
        if (g != NULL && g->waiters == 0u) {
            owe_release(bk);
            gen_release(g);
        }
    }
    return MOQR_OK;
}

moqr_result_t
moqr_admin_abort(moqr_admin_t *a, uint32_t bank, uint64_t serial)
{
    moqr_admin_bank_t *bk;

    if (a == NULL || bank >= MOQR_ADMIN_BANKS || serial == 0) {
        return MOQR_ERR_INVAL;
    }
    bk = &a->bank[bank];
    if (bk->state != MOQR_ADMIN_BS_RESERVED || bk->serial != serial) {
        return MOQR_ERR_WRONG_STATE;
    }
    owe_release(bk);
    {
        /* The reservation is over, so whatever the record was tracking is
         * settled by the bank's release. */
        moqr_admin_gen_t *g = gen_find(a, serial);
        if (g != NULL) {
            gen_release(g);
        }
    }
    return MOQR_OK;
}

bool
moqr_admin_peek_release(const moqr_admin_t *a, moqr_admin_release_t *out)
{
    if (a == NULL || out == NULL) {
        return false;
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (a->bank[b].state == MOQR_ADMIN_BS_RELEASING) {
            out->serial = a->bank[b].serial;
            out->bank = b;
            return true;
        }
    }
    return false;
}

moqr_result_t
moqr_admin_ack_release(moqr_admin_t *a, uint64_t serial, uint32_t bank)
{
    moqr_admin_bank_t *bk;

    if (a == NULL || bank >= MOQR_ADMIN_BANKS || serial == 0) {
        return MOQR_ERR_INVAL;
    }
    bk = &a->bank[bank];
    /* Both halves must match. A stale or duplicate acknowledgement would free
     * storage the broker still considers in flight. */
    if (bk->state != MOQR_ADMIN_BS_RELEASING || bk->serial != serial) {
        return MOQR_ERR_INVAL;
    }
    bk->serial = 0;
    bk->state = MOQR_ADMIN_BS_FREE;
    return MOQR_OK;
}

bool
moqr_admin_claim_abandon(moqr_admin_t *a, moqr_admin_retire_t *out)
{
    if (a == NULL || out == NULL) {
        return false;
    }
    /* An already-claimed retirement comes first, so a re-entering caller sees
     * the SAME serial -- with its retained token -- rather than starting a
     * second retirement alongside it. */
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (a->gen[i].state == MOQR_ADMIN_GS_RETIRING) {
            out->serial = a->gen[i].serial;
            out->taken = a->gen[i].token_taken;
            out->demand = a->gen[i].token_demand;
            out->bank = a->gen[i].token_bank;
            return true;
        }
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (a->gen[i].state == MOQR_ADMIN_GS_ABANDONED) {
            /* This transition is the point of the call: it is what excludes a
             * rejoin. Naming it a peek, and casting away const to perform it,
             * described the opposite of what happens. */
            a->gen[i].state = MOQR_ADMIN_GS_RETIRING;
            a->gen[i].token_taken = false;
            out->serial = a->gen[i].serial;
            out->taken = false;
            out->demand = 0;
            out->bank = 0;
            return true;
        }
    }
    return false;
}

moqr_result_t
moqr_admin_record_take(moqr_admin_t *a, uint64_t serial, uint32_t demand,
                       uint32_t bank)
{
    moqr_admin_gen_t *g;

    if (a == NULL || serial == 0 || bank >= MOQR_ADMIN_BANKS) {
        return MOQR_ERR_INVAL;
    }
    g = gen_find(a, serial);
    if (g == NULL || g->state != MOQR_ADMIN_GS_RETIRING) {
        return MOQR_ERR_INVAL;
    }
    if (g->token_taken) {
        /* The broker token was already consumed once. Recording a second take
         * would claim an identity the broker never issued twice. */
        return MOQR_ERR_INVAL;
    }
    g->token_taken = true;
    g->token_demand = demand;
    g->token_bank = bank;
    return MOQR_OK;
}

moqr_result_t
moqr_admin_settle_abandon(moqr_admin_t *a, uint64_t serial,
                          moqr_admin_release_fn release, void *ctx)
{
    moqr_admin_gen_t *g;
    moqr_result_t rc;

    if (a == NULL || serial == 0 || release == NULL) {
        return MOQR_ERR_INVAL;
    }
    g = gen_find(a, serial);
    if (g == NULL || g->state != MOQR_ADMIN_GS_RETIRING) {
        return MOQR_ERR_INVAL;
    }
    if (!g->token_taken) {
        /* Phase 3 cannot be reached from phase 1: settling before the broker
         * token was taken would destroy a retirement the broker never
         * performed, and the generation would collect forever with no record
         * left to retire it. */
        return MOQR_ERR_INVAL;
    }
    /* The broker release and the admin settlement are ONE transaction. A
     * separate acknowledgement would leave a gap between the release
     * succeeding and the record of it, and a re-entry landing in that gap
     * would release the same token twice. */
    rc = release(ctx, serial, g->token_demand, g->token_bank);
    if (rc != MOQR_OK) {
        /* Nothing mutated. The retained token stays readable so the failure can
         * be reported and the tier torn down in order -- NOT so the owner can
         * settle again. A refusal here means the broker and this module have
         * already disagreed about a token the broker itself issued. */
        return rc;
    }
    gen_release(g);
    return MOQR_OK;
}

moqr_result_t
moqr_admin_fail_serial(moqr_admin_t *a, uint64_t serial,
                       moqr_http_status_t status, uint64_t now_us)
{
    moqr_admin_gen_t *g;
    moqr_admin_bank_t *bk;
    uint32_t bank = 0;

    if (a == NULL || serial == 0) {
        return MOQR_ERR_INVAL;
    }
    switch (status) {
    case MOQR_HTTP_500: case MOQR_HTTP_503:
        break;
    default:
        /* A generation-wide failure is an internal condition, not a request
         * error; the other codes describe the request, not the generation. */
        return MOQR_ERR_INVAL;
    }
    /* Fail closed on an identity no authoritative carrier holds. Returning OK
     * for a misspelled or already-settled serial makes "nothing to do" and
     * "failure transition completed" the same answer. A SIGNAL-only reserved
     * bank is authoritative even with no client and no generation record. */
    {
        bool known = (gen_find(a, serial) != NULL) ||
                     (bank_of(a, serial, NULL) != NULL);
        if (!known) {
            for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
                if (a->client[i].state != MOQR_ADMIN_CS_FREE &&
                    a->client[i].serial == serial) {
                    known = true;
                    break;
                }
            }
        }
        if (!known) {
            return MOQR_ERR_INVAL;
        }
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        moqr_admin_client_t *c = &a->client[i];
        if (c->serial != serial) {
            continue;
        }
        switch (c->state) {
        case MOQR_ADMIN_CS_PARSED:
        case MOQR_ADMIN_CS_WAITING:
            answer_error(a, c, status, now_us);
            break;
        case MOQR_ADMIN_CS_WRITING:
            /* The same byte rule cancellation uses. */
            if (c->bytes_out == 0u) {
                release_pin(a, c);
                answer_error(a, c, status, now_us);
            } else {
                drop_client(a, c);
            }
            break;
        default:
            break;
        }
    }
    bk = bank_of(a, serial, &bank);
    if (bk != NULL && bk->pins == 0) {
        owe_release(bk);
    }
    g = gen_find(a, serial);
    if (g != NULL) {
        if (bk != NULL) {
            gen_release(g);         /* the bank owes the release */
        } else if (g->state != MOQR_ADMIN_GS_RETIRING) {
            g->waiters = 0;
            g->state = MOQR_ADMIN_GS_ABANDONED;
        }
    }
    return MOQR_OK;
}

static void
start_write(moqr_admin_t *a, moqr_admin_client_t *c, uint32_t bank,
            uint64_t now_us)
{
    moqr_admin_bank_t *bk = &a->bank[bank];
    moqr_obs_format_t fmt = c->parsed.fmt;
    moqr_admin_body_t slot;

    /* The body SLOT follows the validated (target, representation) pair: the
     * shards document is the bank's JSON slot; a metrics representation is
     * its format's slot. An out-of-range format would index the body array;
     * that is an invariant contradiction, not a client error. */
    if (c->parsed.target == MOQR_HTTP_TARGET_SHARDS &&
        c->parsed.rep == MOQR_HTTP_REP_JSON && fmt == MOQR_OBS_FMT__COUNT) {
        slot = MOQR_ADMIN_BODY_SHARDS;
    } else if (c->parsed.rep == MOQR_HTTP_REP_METRICS &&
               fmt < MOQR_OBS_FMT__COUNT) {
        slot = (moqr_admin_body_t)fmt;
    } else {
        answer_error(a, c, MOQR_HTTP_500, now_us);
        return;
    }
    c->bank = bank;
    c->pinned = true;
    bk->pins++;
    c->body = bk->body[slot];
    c->body_len = bk->body_len[slot];
    c->body_sent = 0;
    c->carrier = MOQR_ADMIN_CARRIER_BANK;
    c->head_len = moqr_http_write_head(MOQR_HTTP_200, c->parsed.rep, fmt,
                                       c->body_len, c->head, sizeof(c->head));
    if (c->head_len == 0) {
        release_pin(a, c);
        answer_error(a, c, MOQR_HTTP_500, now_us);
        return;
    }
    c->head_sent = 0;
    c->bytes_out = 0;
    c->state = MOQR_ADMIN_CS_WRITING;
    arm(c, now_us, MOQR_ADMIN_WRITE_DEADLINE_US);
}

moqr_result_t
moqr_admin_tick(moqr_admin_t *a, uint64_t now_us)
{
    if (a == NULL) {
        return MOQR_ERR_INVAL;
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        moqr_admin_client_t *c = &a->client[i];

        switch (c->state) {
        case MOQR_ADMIN_CS_READING:
            if (expired(c, now_us)) {
                drop_client(a, c);
            }
            break;

        case MOQR_ADMIN_CS_PARSED:
            /* Still unassigned. The caller has not yet asked the broker; the
             * bounded wait applies from here so a caller that never asks does
             * not strand the client. */
            if (a->cancelled) {
                answer_error(a, c, MOQR_HTTP_503, now_us);
            } else if (expired(c, now_us)) {
                answer_error(a, c, MOQR_HTTP_503, now_us);
            }
            break;

        case MOQR_ADMIN_CS_WAITING:
            if (a->cancelled) {
                answer_error(a, c, MOQR_HTTP_503, now_us);
                break;
            }
            {
                bool started = false;
                for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
                    /* THIS client's serial, never merely a ready bank. */
                    if (a->bank[b].state == MOQR_ADMIN_BS_READY &&
                        a->bank[b].serial == c->serial) {
                        start_write(a, c, b, now_us);
                        started = true;
                        break;
                    }
                }
                if (!started && expired(c, now_us)) {
                    answer_error(a, c, MOQR_HTTP_503, now_us);
                }
            }
            break;

        case MOQR_ADMIN_CS_WRITING:
            if (expired(c, now_us)) {
                drop_client(a, c);
            }
            break;

        default:
            break;
        }
    }
    /* No sweep here. A bank's release is owed at the exact event that ends its
     * usefulness -- the last pin dropping, an abort, the last waiter leaving,
     * cancellation or teardown -- never by a periodic scan. A scan cannot tell
     * a generation whose waiters have all gone from one whose waiters have not
     * arrived yet, and guessing costs either a leaked bank or a discarded
     * generation.
     */
    return MOQR_OK;
}

bool
moqr_admin_pending(const moqr_admin_t *a, uint32_t client, const char **out,
                   size_t *out_len)
{
    const moqr_admin_client_t *c;

    if (a == NULL || client >= MOQR_ADMIN_MAX_CLIENTS || out == NULL ||
        out_len == NULL) {
        return false;
    }
    c = &a->client[client];
    if (c->state != MOQR_ADMIN_CS_WRITING) {
        return false;
    }
    if (c->head_sent < c->head_len) {
        *out = c->head + c->head_sent;
        *out_len = c->head_len - c->head_sent;
        return true;
    }
    if (c->body != NULL && c->body_sent < c->body_len) {
        *out = c->body + c->body_sent;
        *out_len = c->body_len - c->body_sent;
        return true;
    }
    return false;
}

moqr_result_t
moqr_admin_on_written(moqr_admin_t *a, uint32_t client, size_t n,
                      uint64_t now_us)
{
    moqr_admin_client_t *c;

    if (a == NULL || client >= MOQR_ADMIN_MAX_CLIENTS) {
        return MOQR_ERR_INVAL;
    }
    c = &a->client[client];
    if (c->state != MOQR_ADMIN_CS_WRITING) {
        return MOQR_ERR_WRONG_STATE;
    }
    {
        /* Compared part by part, never as a sum that could wrap. */
        size_t head_left = c->head_len - c->head_sent;
        size_t body_left = (c->body != NULL) ? c->body_len - c->body_sent : 0u;
        if (n > head_left && n - head_left > body_left) {
            return MOQR_ERR_INVAL;
        }
    }
    if (n == 0) {
        /* No progress, no reprieve: refreshing here would let a client hold a
         * bank open indefinitely by reporting nothing, forever. */
        return MOQR_OK;
    }
    c->bytes_out += n;
    while (n > 0) {
        if (c->head_sent < c->head_len) {
            size_t left = c->head_len - c->head_sent;
            size_t take = (n < left) ? n : left;
            c->head_sent += take;
            n -= take;
            continue;
        }
        if (c->body != NULL && c->body_sent < c->body_len) {
            size_t left = c->body_len - c->body_sent;
            size_t take = (n < left) ? n : left;
            c->body_sent += take;
            n -= take;
            continue;
        }
        break;
    }
    arm(c, now_us, MOQR_ADMIN_WRITE_DEADLINE_US);
    if (c->head_sent >= c->head_len &&
        (c->body == NULL || c->body_sent >= c->body_len)) {
        release_pin(a, c);
        c->state = MOQR_ADMIN_CS_DONE;
    }
    return MOQR_OK;
}

moqr_result_t
moqr_admin_close(moqr_admin_t *a, uint32_t client)
{
    moqr_admin_client_t *c;

    if (a == NULL || client >= MOQR_ADMIN_MAX_CLIENTS) {
        return MOQR_ERR_INVAL;
    }
    c = &a->client[client];
    if (c->state == MOQR_ADMIN_CS_FREE) {
        return MOQR_ERR_WRONG_STATE;
    }
    gen_leave(a, c->serial);
    release_pin(a, c);
    memset(c, 0, sizeof(*c));
    c->state = MOQR_ADMIN_CS_FREE;
    if (a->clients_used > 0) {
        a->clients_used--;
    }
    return MOQR_OK;
}

void
moqr_admin_cancel(moqr_admin_t *a, uint64_t now_us)
{
    if (a == NULL) {
        return;
    }
    a->cancelled = true;
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        moqr_admin_client_t *c = &a->client[i];
        switch (c->state) {
        case MOQR_ADMIN_CS_READING:
        case MOQR_ADMIN_CS_PARSED:
        case MOQR_ADMIN_CS_WAITING:
            /* Nothing has gone out yet, so a status can still be chosen. */
            answer_error(a, c, MOQR_HTTP_503, now_us);
            break;
        case MOQR_ADMIN_CS_WRITING:
            /* Classified by bytes actually emitted, not by the state label. A
             * 200 that tick() selected but which has not put a single byte on
             * the wire can still legitimately become a 503; once any byte has
             * gone out the status line is committed and the response can only
             * be dropped. */
            if (c->bytes_out == 0u) {
                release_pin(a, c);
                answer_error(a, c, MOQR_HTTP_503, now_us);
            } else {
                drop_client(a, c);
            }
            break;
        default:
            break;
        }
    }
    /* Generations nobody will now receive still owe their releases, and every
     * joined-but-unbanked generation must be retired in the broker. */
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (a->bank[b].pins == 0) {
            owe_release(&a->bank[b]);
        }
    }
    for (uint32_t i = 0; i < MOQR_ADMIN_MAX_CLIENTS; i++) {
        if (a->gen[i].state == MOQR_ADMIN_GS_JOINED) {
            a->gen[i].waiters = 0;
            a->gen[i].state = MOQR_ADMIN_GS_ABANDONED;
        }
    }
}

uint64_t
moqr_admin_publish_epoch(const moqr_admin_t *a)
{
    return (a == NULL) ? 0u : a->publish_gen;
}

bool
moqr_admin_publish_since(const moqr_admin_t *a, uint64_t epoch)
{
    if (a == NULL) {
        return false;
    }
    /* A caller that samples and immediately rechecks with no commit in between
     * must NOT see a spurious wake, or its loop degenerates into a spin. */
    /* Inequality, never `>`. A greater-than comparison makes the claim false at
     * the boundary: one commit after a sample at the maximum is invisible.
     * Inequality is exactly the sampled-generation contract, and it is correct
     * across wrap. */
    return a->publish_gen != epoch;
}

bool
moqr_admin_bank_available(const moqr_admin_t *a)
{
    if (a == NULL) {
        return false;
    }
    for (uint32_t b = 0; b < MOQR_ADMIN_BANKS; b++) {
        if (a->bank[b].state == MOQR_ADMIN_BS_FREE && a->bank[b].pins == 0) {
            return true;
        }
    }
    return false;
}
