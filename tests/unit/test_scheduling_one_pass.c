/*
 * One-pass scheduling selection.
 *
 * The session picks the action a poll takes in one pass over its queue,
 * with a scratch set of the streams the pass has visited, and queues a
 * subgroup's data with the record it already holds instead of searching for
 * it. The test-internals core this links holds every pick to the defining
 * rescan, and every subgroup record to the search it replaces, aborting on
 * any difference. These rows drive the shapes where the two could part --
 * ring wrap, repeated and colliding streams, mixed kinds, barriers, a
 * blocked earlier head, each mapped gate outcome, a full queue, the epoch
 * restart and a producer's own priority snapshot -- and check each result
 * against the rule as well. The scratch set is optional: a session whose
 * set could not be allocated, or whose set is missing or full, selects by
 * the rescan, and those rows check the same orders without it.
 */
#include <moq/moq.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum { MAXQ = 64, MAX_TAGS = 8192 };
enum { POLL_PLAIN, POLL_READY, POLL_MAPPED };

/* One queued action as the test sees it. The tag is its push ordinal and is
 * carried in the action itself, so a polled action names its record. */
typedef struct {
    moq_action_kind_t    kind;
    uint64_t             ref;
    uint8_t              sp, pp;
    bridge_action_gate_t gate;        /* the mapped gate's answer for it */
    uint64_t             done_owner;  /* terminal_owner of a DONE */
    uint64_t             tag;
} rec_t;

typedef struct {
    moq_session_t       *s;
    rec_t                q[MAXQ];     /* queue order, head first */
    size_t               n;
    uint64_t             next_tag;
    bridge_action_gate_t gate_of[MAX_TAGS];
    bool                 ready;       /* the DONE-ready callback's answer */
} fix_t;

/* A counting allocator after the fail-at-N and byte-counting allocators
 * of the session tests: it fails exactly the fail_at-th attempt, keeps the
 * block balance and the live bytes by stated size, and records the size of
 * each attempt, so a row can name an allocation by what it is. */
enum { LEDGER_REC = 64 };
typedef struct {
    int64_t  balance;
    int64_t  live_bytes;
    uint64_t calls;
    uint64_t fail_at;     /* 1-indexed; 0 = never */
    size_t   last_size;
    size_t   sizes[LEDGER_REC];
} ledger_t;

static void ledger_note(ledger_t *l, size_t size)
{
    l->calls++;
    l->last_size = size;
    if (l->calls <= LEDGER_REC) l->sizes[l->calls - 1] = size;
}

static void *ledger_alloc(size_t size, void *ctx)
{
    ledger_t *l = (ledger_t *)ctx;
    ledger_note(l, size);
    if (l->fail_at && l->calls == l->fail_at) return NULL;
    void *p = malloc(size);
    if (p) { l->balance++; l->live_bytes += (int64_t)size; }
    return p;
}

static void *ledger_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    ledger_t *l = (ledger_t *)ctx;
    /* Read before the call: a successful realloc may end the old block's
     * lifetime, so its pointer is not looked at afterwards. */
    const bool was_null = ptr == NULL;
    const int64_t old_bytes = was_null ? 0 : (int64_t)old_size;
    ledger_note(l, new_size);
    if (l->fail_at && l->calls == l->fail_at) return NULL;
    void *p = realloc(ptr, new_size);
    if (p) {
        if (was_null) l->balance++;
        l->live_bytes += (int64_t)new_size - old_bytes;
    }
    return p;
}

static void ledger_free(void *ptr, size_t size, void *ctx)
{
    ledger_t *l = (ledger_t *)ctx;
    if (ptr) { l->balance--; l->live_bytes -= (int64_t)size; }
    free(ptr);
}

static moq_alloc_t ledger_allocator(ledger_t *l)
{
    return (moq_alloc_t){ .ctx = l, .alloc = ledger_alloc,
                          .realloc = ledger_realloc, .free = ledger_free };
}

/* The 1-indexed attempt of `l` that asked for `size`: 0 when none did, and
 * UINT64_MAX when more than one did, so a same-sized allocation is never
 * taken for the one meant. */
static uint64_t ledger_find(const ledger_t *l, size_t size)
{
    uint64_t found = 0;
    for (uint64_t i = 0; i < l->calls && i < LEDGER_REC; i++) {
        if (l->sizes[i] != size) continue;
        if (found) return UINT64_MAX;
        found = i + 1;
    }
    return found;
}

/* A SETUP token, as the SETUP-token tests configure one: with it, create
 * encodes this side's SETUP once into a scratch buffer of this size. */
static uint8_t k_token_value[64];
static const size_t k_setup_scratch_bytes = (size_t)UINT16_MAX + 16u;

/* One create; *out is left to create, so a caller can poison it first. */
static moq_result_t create_session_cfg(uint32_t max_actions, const moq_alloc_t *alloc,
                                       bool setup_tokens, moq_session_t **out)
{
    static moq_auth_token_t token;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg),
                               alloc ? alloc : moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    cfg.max_actions = max_actions;
    if (setup_tokens) {
        for (size_t i = 0; i < sizeof(k_token_value); i++)
            k_token_value[i] = (uint8_t)(0xA5u ^ i);
        token = (moq_auth_token_t){
            .token_type = 4,
            .token_value = { .data = k_token_value, .len = sizeof(k_token_value) },
        };
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.send_auth_token_cache_size = true;
        cfg.auth_token_cache_size = 256;
        cfg.setup_auth_tokens = &token;
        cfg.setup_auth_token_count = 1;
    }
    return moq_session_create(&cfg, 0, out);
}

static moq_result_t create_session(uint32_t max_actions, const moq_alloc_t *alloc,
                                   moq_session_t **out)
{
    *out = NULL;
    return create_session_cfg(max_actions, alloc, false, out);
}

static moq_session_t *make_session(uint32_t max_actions)
{
    moq_session_t *s = NULL;
    if (create_session(max_actions, NULL, &s) < 0) return NULL;
    return s;
}

static void fix_adopt(fix_t *f, moq_session_t *s)
{
    memset(f, 0, sizeof(*f));
    f->s = s;
    f->next_tag = 1;
    f->ready = true;
}

static bool fix_init(fix_t *f, uint32_t cap)
{
    fix_adopt(f, make_session(cap));
    return f->s != NULL;
}

static void fix_done(fix_t *f)
{
    moq_session_destroy(f->s);
    f->s = NULL;
}

/* -- The rule, restated over the test's records -------------------- */

static bool rec_is_stream(const rec_t *r)
{
    return r->kind == MOQ_ACTION_SEND_DATA ||
           (r->kind == MOQ_ACTION_SET_DATA_PRIORITY && r->ref != 0);
}

static bool rec_is_data(const rec_t *r)
{
    return r->kind == MOQ_ACTION_SEND_DATAGRAM || rec_is_stream(r);
}

/* The records carry no owner, so only the priorities order them. */
static bool rec_before(const rec_t *x, const rec_t *y)
{
    if (x->sp != y->sp) return x->sp < y->sp;
    return x->pp < y->pp;
}

static bool rec_behind_its_stream(const rec_t *q, size_t i)
{
    for (size_t j = 0; j < i; j++)
        if (rec_is_stream(&q[j]) && q[j].ref == q[i].ref) return true;
    return false;
}

static size_t model_select(const rec_t *q, size_t n)
{
    if (!rec_is_data(&q[0])) return 0;
    size_t best = 0;
    for (size_t i = 1; i < n; i++) {
        if (!rec_is_data(&q[i])) break;
        if (rec_is_stream(&q[i]) && rec_behind_its_stream(q, i)) continue;
        if (rec_before(&q[i], &q[best])) best = i;
    }
    return best;
}

/* n when nothing qualifies. */
static size_t model_mapped(const rec_t *q, size_t n)
{
    size_t best = n;
    for (size_t i = 0; i < n; i++) {
        const rec_t *r = &q[i];
        if (r->done_owner) break;
        if (r->kind == MOQ_ACTION_CLOSE_SESSION) {
            if (i == 0) best = 0;
            break;
        }
        if (!rec_is_data(r)) {
            if (r->gate == BRIDGE_ACTION_SKIP) continue;
            uint64_t ref = r->kind == MOQ_ACTION_SEND_BIDI_STREAM ? r->ref : 0;
            bool blocked_open = false;
            for (size_t j = 0; ref && j < i; j++)
                if (q[j].kind == MOQ_ACTION_OPEN_BIDI_STREAM && q[j].ref == ref &&
                    q[j].gate == BRIDGE_ACTION_SKIP) {
                    blocked_open = true;
                    break;
                }
            if (blocked_open) continue;
            break;
        }
        if (r->kind != MOQ_ACTION_SEND_DATA) continue;
        if (rec_behind_its_stream(q, i) || r->gate != BRIDGE_ACTION_READY) continue;
        if (best == n || rec_before(r, &q[best])) best = i;
    }
    return best;
}

/* -- Driving the session ------------------------------------------- */

static bool fix_push(fix_t *f, rec_t r)
{
    if (f->n >= MAXQ || f->next_tag >= MAX_TAGS) return false;
    r.tag = f->next_tag;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = r.kind;
    a.borrow_epoch = f->s->borrow_epoch;
    moq_action_meta_t m;
    memset(&m, 0, sizeof(m));
    m.deadline_us = UINT64_MAX;
    m.subscriber_priority = r.sp;
    m.publisher_priority = r.pp;
    m.terminal_owner = r.done_owner;
    const moq_stream_ref_t ref = moq_stream_ref_from_u64(r.ref);
    switch (r.kind) {
    case MOQ_ACTION_SEND_DATA:
        a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a.u.send_data.stream_ref = ref;
        memcpy(a.u.send_data.header, &r.tag, sizeof(r.tag));
        a.u.send_data.header_len = (uint8_t)sizeof(r.tag);
        m.mode = MOQ_SCHED_MODE_SUBGROUP;
        break;
    case MOQ_ACTION_SEND_DATAGRAM:
        a.detail_size = (uint32_t)sizeof(moq_send_datagram_action_t);
        a.u.send_datagram.len = (size_t)r.tag;
        m.mode = MOQ_SCHED_MODE_DATAGRAM;
        break;
    case MOQ_ACTION_SET_DATA_PRIORITY:
        a.detail_size = (uint32_t)sizeof(moq_set_data_priority_action_t);
        a.u.set_data_priority.stream_ref = ref;
        a.u.set_data_priority.priority.group_id = r.tag;
        break;
    case MOQ_ACTION_OPEN_BIDI_STREAM:
        a.detail_size = (uint32_t)sizeof(moq_open_bidi_stream_action_t);
        a.u.open_bidi_stream.stream_ref = ref;
        a.u.open_bidi_stream.len = (size_t)r.tag;
        break;
    case MOQ_ACTION_SEND_BIDI_STREAM:
        a.detail_size = (uint32_t)sizeof(moq_send_bidi_stream_action_t);
        a.u.send_bidi_stream.stream_ref = ref;
        a.u.send_bidi_stream.len = (size_t)r.tag;
        break;
    case MOQ_ACTION_CLOSE_SESSION:
        a.detail_size = (uint32_t)sizeof(moq_close_session_action_t);
        a.u.close_session.code = r.tag;
        break;
    default:
        a.kind = MOQ_ACTION_SEND_CONTROL;
        r.kind = MOQ_ACTION_SEND_CONTROL;
        a.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
        a.u.send_control.len = (size_t)r.tag;
        break;
    }
    if (push_action_ex(f->s, &a, &m, UINT64_MAX) != MOQ_OK) return false;
    f->next_tag++;
    f->gate_of[r.tag] = r.gate;
    f->q[f->n++] = r;
    return true;
}

static uint64_t tag_of(const moq_action_t *a)
{
    uint64_t t = 0;
    switch (a->kind) {
    case MOQ_ACTION_SEND_DATA:
        memcpy(&t, a->u.send_data.header, sizeof(t));
        break;
    case MOQ_ACTION_SEND_DATAGRAM:      t = a->u.send_datagram.len; break;
    case MOQ_ACTION_SET_DATA_PRIORITY:  t = a->u.set_data_priority.priority.group_id; break;
    case MOQ_ACTION_OPEN_BIDI_STREAM:   t = a->u.open_bidi_stream.len; break;
    case MOQ_ACTION_SEND_BIDI_STREAM:   t = a->u.send_bidi_stream.len; break;
    case MOQ_ACTION_CLOSE_SESSION:      t = a->u.close_session.code; break;
    default:                            t = a->u.send_control.len; break;
    }
    return t;
}

static size_t fix_find(const fix_t *f, uint64_t tag)
{
    for (size_t i = 0; i < f->n; i++)
        if (f->q[i].tag == tag) return i;
    return f->n;
}

static void fix_remove(fix_t *f, size_t at)
{
    memmove(&f->q[at], &f->q[at + 1], (f->n - at - 1) * sizeof(f->q[0]));
    f->n--;
}

/* Inspecting and reserving calls answer alike; nothing is reserved. */
static bridge_action_gate_t test_gate(void *ctx, const moq_action_t *a,
                                      const moq_action_meta_t *m, bool reserve)
{
    const fix_t *f = (const fix_t *)ctx;
    (void)m;
    (void)reserve;
    const uint64_t t = tag_of(a);
    return t < MAX_TAGS ? f->gate_of[t] : BRIDGE_ACTION_BARRIER;
}

static bool test_ready(void *ctx, uint64_t owner)
{
    const fix_t *f = (const fix_t *)ctx;
    (void)owner;
    return f->ready;
}

/* One poll of the given kind. Returns the tag taken, 0 for none. */
static uint64_t fix_poll(fix_t *f, int how)
{
    moq_action_t out;
    moq_action_meta_t meta;
    memset(&out, 0, sizeof(out));
    bool got;
    if (how == POLL_MAPPED)
        got = bridge_session_poll_mapped_action(f->s, &out, &meta, true,
                                                test_gate, f);
    else if (how == POLL_READY)
        got = bridge_session_poll_ready_action(f->s, &out, &meta, test_ready, f);
    else
        got = bridge_session_poll_action(f->s, &out, &meta);
    if (!got) return 0;
    const uint64_t t = tag_of(&out);
    moq_action_cleanup(&out);
    const size_t at = fix_find(f, t);
    if (at < f->n) fix_remove(f, at);
    return t;
}

/* One poll checked against the rule. The ready callback says yes and no
 * record is a DONE, so a plain and a ready poll follow the same rule. */
static int poll_against_rule(fix_t *f, int how)
{
    int failures = 0;
    uint64_t want = 0;
    if (how == POLL_MAPPED) {
        const size_t at = model_mapped(f->q, f->n);
        if (at < f->n) want = f->q[at].tag;
    } else if (f->n) {
        want = f->q[model_select(f->q, f->n)].tag;
    }
    const size_t before = f->n;
    const uint64_t got = fix_poll(f, how);
    MOQ_TEST_CHECK_EQ_U64(got, want);
    if (got) MOQ_TEST_CHECK_EQ_SIZE(f->n, before - 1);
    return failures;
}

/* Takes k actions and checks both the rule and the session name the
 * expected push ordinals, in order. */
static int expect_order(fix_t *f, int how, const uint64_t *tags, size_t k)
{
    int failures = 0;
    for (size_t i = 0; i < k; i++) {
        if (how == POLL_MAPPED) {
            const size_t at = model_mapped(f->q, f->n);
            MOQ_TEST_CHECK(at < f->n && f->q[at].tag == tags[i]);
        } else if (f->n) {
            MOQ_TEST_CHECK_EQ_U64(f->q[model_select(f->q, f->n)].tag, tags[i]);
        }
        MOQ_TEST_CHECK_EQ_U64(fix_poll(f, how), tags[i]);
    }
    return failures;
}

#define REC_DATA(r, s_, p_)  ((rec_t){ .kind = MOQ_ACTION_SEND_DATA, .ref = (r), \
                                       .sp = (s_), .pp = (p_), \
                                       .gate = BRIDGE_ACTION_READY })
#define REC_DGRAM(s_)        ((rec_t){ .kind = MOQ_ACTION_SEND_DATAGRAM, .sp = (s_) })
#define REC_KEY(r, s_)       ((rec_t){ .kind = MOQ_ACTION_SET_DATA_PRIORITY, \
                                       .ref = (r), .sp = (s_) })
#define REC_CTRL()           ((rec_t){ .kind = MOQ_ACTION_SEND_CONTROL })
#define COUNT(a)             (sizeof(a) / sizeof((a)[0]))

/* The session's index hash, restated so rows can pick refs that share a
 * home slot in the seen table. A different core hash only weakens the
 * collision rows to ordinary rows; every check still holds. */
static uint64_t mix(uint64_t key)
{
    key ^= key >> 33;
    key *= 0xFF51AFD7ED558CCDULL;
    key ^= key >> 33;
    key *= 0xC4CEB9FE1A85EC53ULL;
    key ^= key >> 33;
    return key;
}

static uint32_t lcg(uint32_t *st)
{
    *st = *st * 1664525u + 1013904223u;
    return *st >> 8;
}

/* -- Rows ---------------------------------------------------------- */

/* A later action on a stream never overtakes an earlier one on it, however
 * urgent; once the earlier one leaves, it competes again. */
static int test_repeated_streams_keep_their_order(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(1, 5, 0)));   /* 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(2, 5, 0)));   /* 2 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(1, 0, 0)));   /* 3: behind 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(3, 0, 0)));   /* 4 */
    static const uint64_t order[] = { 4, 1, 3, 2 };
    failures += expect_order(&f, POLL_PLAIN, order, COUNT(order));
    MOQ_TEST_CHECK_EQ_SIZE(f.n, 0);
    fix_done(&f);
    return failures;
}

/* A datagram has no stream: it is always a candidate. */
static int test_datagrams_are_always_candidates(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(1, 5, 0)));   /* 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DGRAM(3)));        /* 2 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DGRAM(3)));        /* 3 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(1, 0, 0)));   /* 4: behind 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DGRAM(0)));        /* 5 */
    static const uint64_t order[] = { 5, 2, 3, 1, 4 };
    failures += expect_order(&f, POLL_PLAIN, order, COUNT(order));
    fix_done(&f);
    return failures;
}

/* A non-data action, an owner-wide key included, holds its place; a
 * stream's key is data of its stream and orders the stream's SEND_DATA. */
static int test_barriers_and_stream_keys(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(1, 5, 0)));   /* 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_CTRL()));          /* 2 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(2, 0, 0)));   /* 3 */
    MOQ_TEST_CHECK(fix_push(&f, REC_KEY(0, 0)));       /* 4: owner-wide */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(3, 0, 0)));   /* 5 */
    static const uint64_t order1[] = { 1, 2, 3, 4, 5 };
    failures += expect_order(&f, POLL_PLAIN, order1, COUNT(order1));

    MOQ_TEST_CHECK(fix_push(&f, REC_KEY(7, 7)));       /* 6: stream 7's key */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(7, 0, 0)));   /* 7: behind its key */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(8, 3, 0)));   /* 8 */
    static const uint64_t order2[] = { 8, 6, 7 };
    failures += expect_order(&f, POLL_PLAIN, order2, COUNT(order2));
    fix_done(&f);
    return failures;
}

/* Equal candidates keep queue order: replacement is strictly-less only. */
static int test_ties_keep_queue_order(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(1, 2, 4)));
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(2, 2, 4)));
    rec_t dgram = REC_DGRAM(2);
    dgram.pp = 4;
    MOQ_TEST_CHECK(fix_push(&f, dgram));
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(3, 2, 4)));
    static const uint64_t order[] = { 1, 2, 3, 4 };
    failures += expect_order(&f, POLL_PLAIN, order, COUNT(order));
    fix_done(&f);
    return failures;
}

/* Every bit of the ref is its identity: refs that differ only in high bits,
 * the all-ones ref and ref 0 are distinct streams. */
static int test_whole_ref_is_the_identity(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(5, 5, 0)));                           /* 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(5 | (UINT64_C(1) << 32), 4, 0)));     /* 2 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(5 | (UINT64_C(1) << 63), 3, 0)));     /* 3 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(UINT64_MAX, 2, 0)));                  /* 4 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(0, 1, 0)));                           /* 5 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(5, 0, 0)));                           /* 6: behind 1 */
    static const uint64_t order[] = { 5, 4, 3, 2, 1, 6 };
    failures += expect_order(&f, POLL_PLAIN, order, COUNT(order));
    fix_done(&f);
    return failures;
}

/* Refs sharing the table's last home slot: their probes wrap to slot 0, a
 * different ref in a probed slot is not a match, and a repeated ref is
 * found past the others. */
static int test_colliding_refs(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    const size_t mask = f.s->action_seen.mask;
    MOQ_TEST_CHECK(mask + 1 >= 2u * f.s->action_cap);
    uint64_t r[5] = { 0 };
    size_t found = 0;
    for (uint64_t k = 1; found < COUNT(r) && k < 1000000; k++)
        if ((mix(k) & mask) == mask) r[found++] = k;
    MOQ_TEST_CHECK_EQ_SIZE(found, COUNT(r));
    if (found < COUNT(r)) { fix_done(&f); return failures; }
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(r[0], 5, 0)));   /* 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(r[1], 5, 0)));   /* 2 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(r[2], 5, 0)));   /* 3 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(r[0], 0, 0)));   /* 4: behind 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(r[3], 1, 0)));   /* 5 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(r[1], 0, 0)));   /* 6: behind 2 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(r[4], 2, 0)));   /* 7 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(r[2], 0, 0)));   /* 8: behind 3 */
    MOQ_TEST_CHECK(action_queue_full(f.s));
    static const uint64_t first[] = { 5 };
    failures += expect_order(&f, POLL_PLAIN, first, COUNT(first));
    /* That pass added the five distinct refs in queue order, all homed at
     * the last slot, so they sit in it and the four slots after it. */
    const moq_seen_set_t *set = &f.s->action_seen;
    for (size_t k = 0; k < COUNT(r); k++) {
        const moq_seen_slot_t *slot = &set->slots[(mask + k) & mask];
        MOQ_TEST_CHECK(slot->stamp == set->epoch && slot->ref == r[k]);
    }
    static const uint64_t rest[] = { 7, 1, 4, 2, 6, 3, 8 };
    failures += expect_order(&f, POLL_PLAIN, rest, COUNT(rest));
    fix_done(&f);
    return failures;
}

/* A queue at capacity: all distinct streams (the table's highest load),
 * all one stream, and a mix. */
static int test_queue_at_capacity(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    static const uint8_t sp1[8] = { 5, 3, 7, 3, 0, 9, 0, 2 };
    for (size_t i = 0; i < 8; i++)
        MOQ_TEST_CHECK(fix_push(&f, REC_DATA(100 + i, sp1[i], 0)));
    MOQ_TEST_CHECK(action_queue_full(f.s));
    MOQ_TEST_CHECK(!fix_push(&f, REC_DATA(99, 0, 0)));
    static const uint64_t order1[] = { 5, 7, 8, 2, 4, 1, 3, 6 };
    failures += expect_order(&f, POLL_PLAIN, order1, COUNT(order1));

    for (size_t i = 0; i < 8; i++)
        MOQ_TEST_CHECK(fix_push(&f, REC_DATA(200, (uint8_t)(8 - i), 0)));
    static const uint64_t order2[] = { 9, 10, 11, 12, 13, 14, 15, 16 };
    failures += expect_order(&f, POLL_READY, order2, COUNT(order2));

    static const uint64_t ref3[8] = { 1, 2, 1, 2, 3, 3, 4, 1 };
    static const uint8_t sp3[8] = { 9, 8, 0, 0, 5, 0, 7, 0 };
    for (size_t i = 0; i < 8; i++)
        MOQ_TEST_CHECK(fix_push(&f, REC_DATA(ref3[i], sp3[i], 0)));
    static const uint64_t order3[] = { 21, 22, 23, 18, 20, 17, 19, 24 };
    failures += expect_order(&f, POLL_PLAIN, order3, COUNT(order3));
    fix_done(&f);
    return failures;
}

/* A DONE that is not ready yet is passed over; selection starts after it
 * and still keeps each stream's order from there. */
static int test_blocked_earlier_head(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    rec_t done = REC_CTRL();
    done.done_owner = 77;
    MOQ_TEST_CHECK(fix_push(&f, done));                /* 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(1, 5, 0)));   /* 2 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(1, 0, 0)));   /* 3: behind 2 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(2, 1, 0)));   /* 4 */
    f.ready = false;
    MOQ_TEST_CHECK_EQ_U64(fix_poll(&f, POLL_READY), 4);
    MOQ_TEST_CHECK_EQ_U64(fix_poll(&f, POLL_READY), 2);
    MOQ_TEST_CHECK_EQ_U64(fix_poll(&f, POLL_READY), 3);
    MOQ_TEST_CHECK_EQ_U64(fix_poll(&f, POLL_READY), 0);   /* only the DONE is left */
    MOQ_TEST_CHECK_EQ_U64(fix_poll(&f, POLL_MAPPED), 0);  /* a DONE ends a mapped scan */
    f.ready = true;
    MOQ_TEST_CHECK_EQ_U64(fix_poll(&f, POLL_READY), 1);
    MOQ_TEST_CHECK_EQ_SIZE(f.n, 0);
    fix_done(&f);
    return failures;
}

/* Each gate outcome, and every earlier action on a stream counting against
 * a later SEND_DATA on it whether or not that earlier action could go. */
static int test_mapped_gate_outcomes(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;

    /* A refused SEND_DATA still holds back the stream behind it. */
    rec_t refused = REC_DATA(1, 9, 0);
    refused.gate = BRIDGE_ACTION_SKIP;
    MOQ_TEST_CHECK(fix_push(&f, refused));             /* 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(1, 0, 0)));   /* 2: behind 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(2, 5, 0)));   /* 3 */
    rec_t barred = REC_DATA(4, 0, 0);
    barred.gate = BRIDGE_ACTION_BARRIER;
    MOQ_TEST_CHECK(fix_push(&f, barred));              /* 4: not admitted */
    static const uint64_t m1[] = { 3 };
    failures += expect_order(&f, POLL_MAPPED, m1, COUNT(m1));
    MOQ_TEST_CHECK_EQ_U64(fix_poll(&f, POLL_MAPPED), 0);
    static const uint64_t p1[] = { 4, 1, 2 };
    failures += expect_order(&f, POLL_PLAIN, p1, COUNT(p1));

    /* A stream's key holds back that stream's SEND_DATA; a datagram is not
     * a mapped candidate and ties keep queue order. */
    MOQ_TEST_CHECK(fix_push(&f, REC_KEY(5, 0)));       /* 5 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(5, 0, 0)));   /* 6: behind its key */
    MOQ_TEST_CHECK(fix_push(&f, REC_DGRAM(0)));        /* 7 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(6, 4, 0)));   /* 8 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(7, 4, 0)));   /* 9 */
    static const uint64_t m2[] = { 8, 9 };
    failures += expect_order(&f, POLL_MAPPED, m2, COUNT(m2));
    static const uint64_t p2[] = { 5, 6, 7 };
    failures += expect_order(&f, POLL_PLAIN, p2, COUNT(p2));

    /* A refused open lets its own request's bytes pass; another request's
     * bytes, or a barrier, end the scan. */
    rec_t opener = { .kind = MOQ_ACTION_OPEN_BIDI_STREAM, .ref = 77,
                     .gate = BRIDGE_ACTION_SKIP };
    rec_t bytes = { .kind = MOQ_ACTION_SEND_BIDI_STREAM, .ref = 77,
                    .gate = BRIDGE_ACTION_BARRIER };
    MOQ_TEST_CHECK(fix_push(&f, opener));              /* 10 */
    MOQ_TEST_CHECK(fix_push(&f, bytes));               /* 11 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(8, 3, 0)));   /* 12 */
    static const uint64_t m3[] = { 12 };
    failures += expect_order(&f, POLL_MAPPED, m3, COUNT(m3));
    bytes.ref = 78;
    MOQ_TEST_CHECK(fix_push(&f, bytes));               /* 13: another request */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(9, 0, 0)));   /* 14 */
    MOQ_TEST_CHECK_EQ_U64(fix_poll(&f, POLL_MAPPED), 0);
    static const uint64_t p3[] = { 10, 11, 13, 14 };
    failures += expect_order(&f, POLL_PLAIN, p3, COUNT(p3));

    /* A leading close supersedes; one behind data ends the scan there. */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(10, 1, 0)));  /* 15 */
    rec_t closing = { .kind = MOQ_ACTION_CLOSE_SESSION, .gate = BRIDGE_ACTION_READY };
    MOQ_TEST_CHECK(fix_push(&f, closing));             /* 16 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(11, 0, 0)));  /* 17 */
    static const uint64_t m4[] = { 15, 16 };
    failures += expect_order(&f, POLL_MAPPED, m4, COUNT(m4));
    static const uint64_t m5[] = { 17 };
    failures += expect_order(&f, POLL_MAPPED, m5, COUNT(m5));
    MOQ_TEST_CHECK_EQ_SIZE(f.n, 0);
    fix_done(&f);
    return failures;
}

/* The epoch restarts at 1 after 2^32 passes; stamps left from before must
 * not read as members of the restarted pass. */
static int test_epoch_restart_clears_stale_stamps(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    moq_seen_set_t *set = &f.s->action_seen;
    const size_t mask = set->mask;
    set->epoch = UINT32_MAX;
    /* A stale member at the home slot of a ref the pass will meet, and two
     * at slots of a ref it never meets. */
    set->slots[0] = (moq_seen_slot_t){ .ref = 0xDEAD, .stamp = 1 };
    set->slots[1] = (moq_seen_slot_t){ .ref = 0xDEAD, .stamp = 1 };
    set->slots[mix(11) & mask] = (moq_seen_slot_t){ .ref = 11, .stamp = 1 };
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(10, 5, 0)));   /* 1 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(11, 0, 0)));   /* 2 */
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(12, 1, 0)));   /* 3 */
    static const uint64_t first[] = { 2 };
    failures += expect_order(&f, POLL_PLAIN, first, COUNT(first));
    MOQ_TEST_CHECK_EQ_U64(set->epoch, 1);
    size_t members = 0;
    for (size_t i = 0; i <= mask; i++)
        if (set->slots[i].stamp == set->epoch) members++;
    MOQ_TEST_CHECK_EQ_SIZE(members, 3);   /* exactly the pass's three refs */
    static const uint64_t rest[] = { 3, 1 };
    failures += expect_order(&f, POLL_PLAIN, rest, COUNT(rest));
    fix_done(&f);
    return failures;
}

/* Seeded mixed traffic through a small ring: the head wraps the ring many
 * times, every poll kind runs, and each pick is checked against the rule. */
static int run_mixed(uint32_t cap, uint32_t seed, int steps)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, cap));
    if (!f.s) return failures;
    static const uint64_t refs[] = {
        0, 1, 2, 3, 1 + (UINT64_C(1) << 40), UINT64_MAX,
    };
    uint32_t st = seed;
    size_t start_head = f.s->action_head;
    for (int step = 0; step < steps && f.next_tag + 1 < MAX_TAGS; step++) {
        const uint32_t op = lcg(&st) % 8;
        if (f.n < cap && (op < 4 || f.n == 0)) {
            rec_t r;
            memset(&r, 0, sizeof(r));
            const uint32_t k = lcg(&st) % 20;
            r.ref = refs[lcg(&st) % COUNT(refs)];
            r.sp = (uint8_t)(lcg(&st) % 4);
            r.pp = (uint8_t)(lcg(&st) % 2);
            const uint32_t g = lcg(&st) % 20;
            if (k < 10) {
                r.kind = MOQ_ACTION_SEND_DATA;
                r.gate = g < 12 ? BRIDGE_ACTION_READY
                       : g < 17 ? BRIDGE_ACTION_SKIP : BRIDGE_ACTION_BARRIER;
            } else if (k < 13) {
                r.kind = MOQ_ACTION_SEND_DATAGRAM;
            } else if (k < 15) {
                /* A key: ref 0 is owner-wide, any other ref a stream's. */
                r.kind = MOQ_ACTION_SET_DATA_PRIORITY;
                r.gate = BRIDGE_ACTION_BARRIER;
            } else if (k < 17) {
                r.kind = k == 15 ? MOQ_ACTION_OPEN_BIDI_STREAM
                                 : MOQ_ACTION_SEND_BIDI_STREAM;
                r.ref = 77 + (lcg(&st) % 2);
                r.gate = g < 8 ? BRIDGE_ACTION_SKIP : BRIDGE_ACTION_BARRIER;
            } else if (k < 19) {
                r.kind = MOQ_ACTION_SEND_CONTROL;
                r.gate = g < 6 ? BRIDGE_ACTION_SKIP : BRIDGE_ACTION_BARRIER;
            } else {
                r.kind = MOQ_ACTION_CLOSE_SESSION;
                r.gate = BRIDGE_ACTION_READY;
            }
            MOQ_TEST_CHECK(fix_push(&f, r));
        } else {
            failures += poll_against_rule(&f, (int)(op % 3));
        }
        if (failures) break;
    }
    while (f.n && !failures) failures += poll_against_rule(&f, POLL_PLAIN);
    MOQ_TEST_CHECK_EQ_SIZE(f.n, 0);
    /* The head went round the ring many times. */
    MOQ_TEST_CHECK(f.s->action_head - start_head > 8u * cap);
    fix_done(&f);
    return failures;
}

static int test_mixed_traffic_through_a_wrapping_ring(void)
{
    int failures = 0;
    failures += run_mixed(1, 0x5eed0001u, 400);
    failures += run_mixed(2, 0x5eed0002u, 1500);
    failures += run_mixed(4, 0x5eed0004u, 3000);
    failures += run_mixed(8, 0x5eed0008u, 3000);
    failures += run_mixed(16, 0x5eed0010u, 3000);
    return failures;
}

/* -- Without the scratch set --------------------------------------- */

/* Orders that depend on every rule the scratch set speeds up: a stream held
 * behind its earlier action, strict ties, a barrier and -- for a mapped
 * poll -- a refused SEND_DATA holding back its stream. Ordinals are relative
 * to the fixture's next push; the queue is empty after each row. */
static int run_order_rows(fix_t *f)
{
    int failures = 0;
    static const int kinds[] = { POLL_PLAIN, POLL_MAPPED };
    for (size_t k = 0; k < COUNT(kinds); k++) {
        const int how = kinds[k];
        uint64_t t = f->next_tag;
        MOQ_TEST_CHECK(fix_push(f, REC_DATA(31, 5, 0)));
        MOQ_TEST_CHECK(fix_push(f, REC_DATA(32, 5, 0)));
        MOQ_TEST_CHECK(fix_push(f, REC_DATA(31, 0, 0)));   /* behind t */
        MOQ_TEST_CHECK(fix_push(f, REC_DATA(33, 0, 0)));
        const uint64_t held[] = { t + 3, t, t + 2, t + 1 };
        failures += expect_order(f, how, held, COUNT(held));

        t = f->next_tag;
        MOQ_TEST_CHECK(fix_push(f, REC_DATA(34, 2, 4)));
        MOQ_TEST_CHECK(fix_push(f, REC_DATA(35, 2, 4)));
        MOQ_TEST_CHECK(fix_push(f, REC_DATA(36, 2, 4)));
        const uint64_t ties[] = { t, t + 1, t + 2 };
        failures += expect_order(f, how, ties, COUNT(ties));

        t = f->next_tag;
        MOQ_TEST_CHECK(fix_push(f, REC_DATA(37, 5, 0)));
        MOQ_TEST_CHECK(fix_push(f, REC_CTRL()));
        MOQ_TEST_CHECK(fix_push(f, REC_DATA(38, 0, 0)));
        if (how == POLL_PLAIN) {
            const uint64_t barrier[] = { t, t + 1, t + 2 };
            failures += expect_order(f, how, barrier, COUNT(barrier));
        } else {
            const uint64_t before[] = { t };
            failures += expect_order(f, how, before, COUNT(before));
            MOQ_TEST_CHECK_EQ_U64(fix_poll(f, POLL_MAPPED), 0);  /* the barrier ends it */
            const uint64_t ctrl[] = { t + 1 };
            failures += expect_order(f, POLL_PLAIN, ctrl, COUNT(ctrl));
            const uint64_t after[] = { t + 2 };
            failures += expect_order(f, how, after, COUNT(after));
        }
        MOQ_TEST_CHECK_EQ_SIZE(f->n, 0);
    }
    const uint64_t t = f->next_tag;
    rec_t refused = REC_DATA(39, 9, 0);
    refused.gate = BRIDGE_ACTION_SKIP;
    MOQ_TEST_CHECK(fix_push(f, refused));
    MOQ_TEST_CHECK(fix_push(f, REC_DATA(39, 0, 0)));      /* behind t */
    MOQ_TEST_CHECK(fix_push(f, REC_DATA(40, 5, 0)));
    const uint64_t mapped[] = { t + 2 };
    failures += expect_order(f, POLL_MAPPED, mapped, COUNT(mapped));
    MOQ_TEST_CHECK_EQ_U64(fix_poll(f, POLL_MAPPED), 0);
    const uint64_t rest[] = { t, t + 1 };
    failures += expect_order(f, POLL_PLAIN, rest, COUNT(rest));
    MOQ_TEST_CHECK_EQ_SIZE(f->n, 0);
    return failures;
}

/* A clean create through `l`, then destroyed. Returns its attempt count
 * (0 when create failed) and, in *cache, the attempt that asked for the
 * scratch set's size (see ledger_find). */
static uint64_t baseline_create(uint32_t cap, bool setup_tokens, ledger_t *l,
                                uint64_t *cache)
{
    memset(l, 0, sizeof(*l));
    moq_alloc_t a = ledger_allocator(l);
    moq_session_t *s = NULL;
    *cache = 0;
    if (create_session_cfg(cap, &a, setup_tokens, &s) != MOQ_OK || !s) return 0;
    const uint64_t n = l->calls;
    *cache = ledger_find(l, moq_index_cap_for(cap) * sizeof(moq_seen_slot_t));
    moq_session_destroy(s);
    return n;
}

/* Only the scratch set's allocation is refused -- found by its size, in a
 * plain create and in one that encodes a SETUP: the session is still
 * created, without the set, selects every order by the rescan in a plain
 * and a mapped poll, allocates nothing more, and gives back every block
 * and byte on destroy. */
static int test_cache_allocation_failure_is_optional(void)
{
    int failures = 0;
    const uint32_t cap = 8;
    const size_t cache_bytes = moq_index_cap_for(cap) * sizeof(moq_seen_slot_t);
    for (int setup_tokens = 0; setup_tokens < 2; setup_tokens++) {
        ledger_t clean;
        uint64_t c = 0;
        const uint64_t n = baseline_create(cap, setup_tokens != 0, &clean, &c);
        MOQ_TEST_CHECK(n >= 2 && n <= LEDGER_REC);
        MOQ_TEST_CHECK(c != 0 && c != UINT64_MAX);   /* exactly one attempt */
        MOQ_TEST_CHECK(clean.balance == 0 && clean.live_bytes == 0);
        if (c == 0 || c == UINT64_MAX) continue;

        ledger_t led;
        memset(&led, 0, sizeof(led));
        led.fail_at = c;
        moq_alloc_t la = ledger_allocator(&led);
        moq_session_t *s = NULL;
        const moq_result_t rc = create_session_cfg(cap, &la, setup_tokens != 0, &s);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_U64(led.calls, n);                 /* nothing retried */
        MOQ_TEST_CHECK_EQ_SIZE(led.sizes[c - 1], cache_bytes); /* the set was refused */
        if (rc != MOQ_OK || !s) {
            MOQ_TEST_CHECK(led.balance == 0 && led.live_bytes == 0);
            continue;
        }
        MOQ_TEST_CHECK(s->action_seen.slots == NULL);
        MOQ_TEST_CHECK_EQ_SIZE(s->action_seen.mask, 0);
        fix_t f;
        fix_adopt(&f, s);
        failures += run_order_rows(&f);
        MOQ_TEST_CHECK_EQ_U64(s->action_seen.epoch, 0);      /* no pass used a set */
        MOQ_TEST_CHECK_EQ_U64(led.calls, n);                 /* selection allocated nothing */
        fix_done(&f);
        MOQ_TEST_CHECK(led.balance == 0 && led.live_bytes == 0);
    }
    return failures;
}

/* Every allocation create makes except the scratch set is required: each
 * one refused fails create with MOQ_ERR_NOMEM, resets *out, stops create at
 * that attempt and leaves nothing behind. The set is create's last attempt,
 * so no required allocation can fail after it has been taken; with SETUP
 * tokens the SETUP scratch is still required and comes before it. */
static int test_required_allocations_still_fail(void)
{
    int failures = 0;
    const uint32_t cap = 8;
    for (int setup_tokens = 0; setup_tokens < 2; setup_tokens++) {
        ledger_t clean;
        uint64_t c = 0;
        const uint64_t n = baseline_create(cap, setup_tokens != 0, &clean, &c);
        MOQ_TEST_CHECK(n >= 2 && n <= LEDGER_REC);
        MOQ_TEST_CHECK(c != 0 && c != UINT64_MAX);
        MOQ_TEST_CHECK_EQ_U64(c, n);                         /* the set is last */
        if (setup_tokens) {
            const uint64_t scratch = ledger_find(&clean, k_setup_scratch_bytes);
            MOQ_TEST_CHECK(scratch != 0 && scratch != UINT64_MAX);
            MOQ_TEST_CHECK(scratch < c);
        }
        for (uint64_t k = 1; k <= n && k <= LEDGER_REC; k++) {
            if (k == c) continue;                            /* the only optional one */
            ledger_t led;
            memset(&led, 0, sizeof(led));
            led.fail_at = k;
            moq_alloc_t la = ledger_allocator(&led);
            moq_session_t *s = (moq_session_t *)(uintptr_t)1;   /* poison */
            MOQ_TEST_CHECK_EQ_INT((int)create_session_cfg(cap, &la, setup_tokens != 0, &s),
                                  (int)MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK(s == NULL);                       /* *out reset */
            MOQ_TEST_CHECK_EQ_U64(led.calls, k);             /* the refusal was reached */
            MOQ_TEST_CHECK_EQ_SIZE(led.sizes[k - 1], clean.sizes[k - 1]);
            if (s && s != (moq_session_t *)(uintptr_t)1) moq_session_destroy(s);
            MOQ_TEST_CHECK(led.balance == 0 && led.live_bytes == 0);
        }
    }
    return failures;
}

/* The normal set, as a control for the rows above: the same orders, with
 * the set in use. */
static int test_rows_with_the_set(void)
{
    int failures = 0;
    ledger_t led;
    memset(&led, 0, sizeof(led));
    moq_alloc_t la = ledger_allocator(&led);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)create_session(8, &la, &s), (int)MOQ_OK);
    if (!s) return failures;
    MOQ_TEST_CHECK(s->action_seen.slots != NULL);
    fix_t f;
    fix_adopt(&f, s);
    failures += run_order_rows(&f);
    MOQ_TEST_CHECK(s->action_seen.epoch > 0);            /* passes used the set */
    fix_done(&f);
    MOQ_TEST_CHECK(led.balance == 0 && led.live_bytes == 0);
    return failures;
}

/* A session whose set is taken away selects the same orders; the set is
 * put back before destroy releases it. */
static int test_missing_set_fallback(void)
{
    int failures = 0;
    ledger_t led;
    memset(&led, 0, sizeof(led));
    moq_alloc_t la = ledger_allocator(&led);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)create_session(8, &la, &s), (int)MOQ_OK);
    if (!s) return failures;
    fix_t f;
    fix_adopt(&f, s);
    const moq_seen_set_t saved = s->action_seen;
    MOQ_TEST_CHECK(saved.slots != NULL);
    s->action_seen.slots = NULL;
    s->action_seen.mask = 0;
    failures += run_order_rows(&f);
    MOQ_TEST_CHECK_EQ_U64(s->action_seen.epoch, saved.epoch);  /* no pass used a set */
    s->action_seen = saved;
    fix_done(&f);
    MOQ_TEST_CHECK(led.balance == 0 && led.live_bytes == 0);
    return failures;
}

/* A one-slot set fills as soon as a pass meets a second stream, and that
 * pick takes the rescan; the session's own set is put back for destroy. */
static int test_full_set_fallback(void)
{
    int failures = 0;
    ledger_t led;
    memset(&led, 0, sizeof(led));
    moq_alloc_t la = ledger_allocator(&led);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)create_session(8, &la, &s), (int)MOQ_OK);
    if (!s) return failures;
    fix_t f;
    fix_adopt(&f, s);
    const moq_seen_set_t saved = s->action_seen;
    moq_seen_slot_t tiny[1];
    memset(tiny, 0, sizeof(tiny));
    s->action_seen.slots = tiny;
    s->action_seen.mask = 0;
    s->action_seen.epoch = 0;

    const uint64_t t = f.next_tag;
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(41, 5, 0)));
    MOQ_TEST_CHECK(fix_push(&f, REC_DATA(42, 0, 0)));
    const uint64_t first[] = { t + 1 };
    failures += expect_order(&f, POLL_PLAIN, first, COUNT(first));
    /* That pass held the head's stream in the only slot and found no room
     * for the second. */
    MOQ_TEST_CHECK_EQ_U64(s->action_seen.epoch, 1);
    MOQ_TEST_CHECK(tiny[0].stamp == 1 && tiny[0].ref == 41);
    const uint64_t second[] = { t };
    failures += expect_order(&f, POLL_PLAIN, second, COUNT(second));
    failures += run_order_rows(&f);

    s->action_seen = saved;
    fix_done(&f);
    MOQ_TEST_CHECK(led.balance == 0 && led.live_bytes == 0);
    return failures;
}

/* -- The subgroup record ------------------------------------------- */

static moq_sg_entry_t *arm_subgroup(moq_session_t *s, size_t slot, uint64_t ref,
                                    uint8_t pp, uint64_t group, uint64_t subgroup,
                                    moq_sg_state_t state)
{
    moq_sg_entry_t *e = &s->subgroups[slot];
    e->state = state;
    e->sub = MOQ_SUBSCRIPTION_INVALID;
    e->pub = MOQ_PUBLICATION_INVALID;
    e->stream_ref = moq_stream_ref_from_u64(ref);
    e->publisher_priority = pp;
    e->group_id = group;
    e->subgroup_id = subgroup;
    sg_occ_link(s, slot);
    return e;
}

/* The record a held subgroup's data is queued with is the one the search
 * would have found, and the snapshot is stamped only when the producer left
 * it zero. Live subgroups at scattered slots make the search pass others. */
static int test_subgroup_record_matches_search(void)
{
    int failures = 0;
    moq_session_t *s = make_session(8);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;
    static const struct {
        size_t slot; uint64_t ref; uint8_t pp; uint64_t group, subgroup;
        moq_sg_state_t state;
    } k[] = {
        { 0, 40, 7, 1, 0, MOQ_SG_OPEN },
        { 3, 41, 200, 2, 5, MOQ_SG_STREAMING },
        { 9, 42, 0, 3, 1, MOQ_SG_OPEN },
    };
    moq_sg_entry_t *e[COUNT(k)];
    for (size_t i = 0; i < COUNT(k); i++)
        e[i] = arm_subgroup(s, k[i].slot, k[i].ref, k[i].pp, k[i].group,
                            k[i].subgroup, k[i].state);
    const uint64_t snap = UINT64_C(0x10000) | (UINT64_C(3) << 8) | UINT64_C(9);
    for (size_t i = 0; i < COUNT(k); i++) {
        for (int producer = 0; producer < 2; producer++) {
            moq_action_t a;
            memset(&a, 0, sizeof(a));
            a.kind = MOQ_ACTION_SEND_DATA;
            a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
            a.borrow_epoch = s->borrow_epoch;
            a.u.send_data.stream_ref = e[i]->stream_ref;
            a.u.send_data.fin = producer != 0;
            if (producer) a.u.send_data.scheduling_priority = snap;
            MOQ_TEST_CHECK_EQ_INT((int)session_push_subgroup_data(s, e[i], &a, 1234),
                                  (int)MOQ_OK);
            const size_t at = (s->action_tail - 1) % s->action_cap;
            const moq_action_t *q = &s->actions[at];
            const moq_action_meta_t *m = &s->action_meta[at];
            /* No owner resolves, so the default subscriber priority applies. */
            const uint64_t want_snap = producer ? snap
                : (UINT64_C(0x10000) | (UINT64_C(128) << 8) | k[i].pp);
            MOQ_TEST_CHECK_EQ_HEX(q->u.send_data.scheduling_priority, want_snap);
            MOQ_TEST_CHECK_EQ_INT(m->subscriber_priority, producer ? 3 : 128);
            MOQ_TEST_CHECK_EQ_INT(m->publisher_priority, producer ? 9 : k[i].pp);
            MOQ_TEST_CHECK_EQ_INT(m->mode, (int)MOQ_SCHED_MODE_SUBGROUP);
            MOQ_TEST_CHECK(m->has_location);
            MOQ_TEST_CHECK_EQ_U64(m->group_id, k[i].group);
            MOQ_TEST_CHECK_EQ_U64(m->subgroup_id, k[i].subgroup);
            MOQ_TEST_CHECK_EQ_U64(m->owner, 0);
            MOQ_TEST_CHECK_EQ_U64(m->deadline_us, 1234);
            MOQ_TEST_CHECK(!m->opens_data && !m->subgroup_header);
            MOQ_TEST_CHECK(!m->terminal_barrier && !m->outgoing_terminal);
            MOQ_TEST_CHECK_EQ_U64(m->terminal_owner, 0);
            moq_action_t out;
            moq_action_meta_t meta;
            MOQ_TEST_CHECK(bridge_session_poll_action(s, &out, &meta));
            MOQ_TEST_CHECK_EQ_U64(out.u.send_data.stream_ref._v, k[i].ref);
            moq_action_cleanup(&out);
        }
    }
    for (size_t i = 0; i < COUNT(k); i++) sg_free_entry(s, k[i].slot);
    moq_session_destroy(s);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_repeated_streams_keep_their_order();
    failures += test_datagrams_are_always_candidates();
    failures += test_barriers_and_stream_keys();
    failures += test_ties_keep_queue_order();
    failures += test_whole_ref_is_the_identity();
    failures += test_colliding_refs();
    failures += test_queue_at_capacity();
    failures += test_blocked_earlier_head();
    failures += test_mapped_gate_outcomes();
    failures += test_epoch_restart_clears_stale_stamps();
    failures += test_mixed_traffic_through_a_wrapping_ring();
    failures += test_subgroup_record_matches_search();
    failures += test_cache_allocation_failure_is_optional();
    failures += test_required_allocations_still_fail();
    failures += test_rows_with_the_set();
    failures += test_missing_set_fallback();
    failures += test_full_set_fallback();

    if (failures == 0)
        printf("test_scheduling_one_pass: all tests passed\n");
    else
        fprintf(stderr, "test_scheduling_one_pass: %d failure(s)\n", failures);
    return failures;
}
