/*
 * Certified selection in the batched poll.
 *
 * session_poll_actions takes many actions in one call. When one walk of the
 * leading data window shows that no action in it is placed before an action
 * queued earlier in it, each pick is the head and needs no further pass;
 * otherwise the pick takes the one-pass selector. The test-internals core
 * this links holds every certified pick to the defining rescan, every new
 * certificate to its property over every pair of its window, and every reuse
 * to the ring it was built on, aborting on any difference.
 *
 * These rows give hand-worked orders for the shapes where a certificate
 * could be wrong: owners that tie with each other but order within
 * themselves, a later action exposed behind its own stream, each comparator
 * branch, a window boundary, barriers and a too-small element. They also
 * count, through the session's selection counters, when a certificate is
 * built, reused, refused and dropped: a ring that wraps, persistent event
 * pressure, a retained terminal reset and a reset queued during the poll.
 */
#include <moq/moq.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum { MAXQ = 64 };

/* One queued action as the test sees it. The tag is its push ordinal and is
 * carried in the action itself, so a polled action names its record. */
typedef struct {
    moq_action_kind_t kind;
    uint64_t          ref;
    uint8_t           sp, pp;
    uint64_t          owner;
    bool              loc;        /* has_location */
    uint8_t           mode;       /* MOQ_SCHED_MODE_* */
    bool              desc;       /* descending group order */
    uint64_t          group, subgroup, object;
    bool              barrier;    /* terminal_barrier */
    uint64_t          tag;
} rec_t;

typedef struct {
    moq_session_t *s;
    rec_t          q[MAXQ];       /* queue order, head first */
    size_t         n;
    uint64_t       next_tag;
} fix_t;

static bool fix_init(fix_t *f, uint32_t cap)
{
    memset(f, 0, sizeof(*f));
    f->next_tag = 1;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    cfg.max_actions = cap;
    if (moq_session_create(&cfg, 0, &f->s) != MOQ_OK) f->s = NULL;
    return f->s != NULL;
}

static void fix_done(fix_t *f)
{
    moq_session_destroy(f->s);
    f->s = NULL;
}

/* -- Records ------------------------------------------------------- */

static rec_t data_rec(uint64_t ref, uint8_t sp, uint8_t pp)
{
    return (rec_t){ .kind = MOQ_ACTION_SEND_DATA, .ref = ref, .sp = sp, .pp = pp,
                    .mode = MOQ_SCHED_MODE_SUBGROUP };
}

/* A subgroup action of `owner` with a location in `group`, priority 0. */
static rec_t loc_rec(uint64_t ref, uint64_t owner, uint64_t group)
{
    rec_t r = data_rec(ref, 0, 0);
    r.owner = owner;
    r.loc = true;
    r.group = group;
    return r;
}

static rec_t dgram_rec(uint8_t sp)
{
    return (rec_t){ .kind = MOQ_ACTION_SEND_DATAGRAM, .sp = sp,
                    .mode = MOQ_SCHED_MODE_DATAGRAM };
}

static rec_t loc_dgram(uint64_t owner, uint64_t group, uint64_t object)
{
    rec_t r = dgram_rec(0);
    r.owner = owner;
    r.loc = true;
    r.group = group;
    r.object = object;
    return r;
}

/* A priority key: ref 0 is owner-wide (not data), any other a stream's. */
static rec_t key_rec(uint64_t ref, uint8_t sp)
{
    return (rec_t){ .kind = MOQ_ACTION_SET_DATA_PRIORITY, .ref = ref, .sp = sp };
}

static rec_t ctrl_rec(void)
{
    return (rec_t){ .kind = MOQ_ACTION_SEND_CONTROL };
}

static rec_t with_mode(rec_t r, uint8_t mode)
{
    r.mode = mode;
    return r;
}

static rec_t with_subgroup(rec_t r, uint64_t subgroup)
{
    r.subgroup = subgroup;
    return r;
}

static rec_t descending(rec_t r)
{
    r.desc = true;
    return r;
}

static rec_t as_barrier(rec_t r)
{
    r.barrier = true;
    return r;
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

/* The scheduling comparison. A fill's own group order equals its parent's in
 * these records (fix_push stamps both alike), so the fill-yields rule does not
 * arise. */
static int model_cmp(const rec_t *x, const rec_t *y)
{
    if (x->sp != y->sp) return x->sp < y->sp ? -1 : 1;
    if (x->pp != y->pp) return x->pp < y->pp ? -1 : 1;
    if (x->owner == 0 || x->owner != y->owner || !x->loc || !y->loc) return 0;
    const bool xf = x->mode == MOQ_SCHED_MODE_FILL, yf = y->mode == MOQ_SCHED_MODE_FILL;
    if (xf && yf) return 0;
    if (x->group != y->group) {
        const bool x_first = x->desc ? x->group > y->group : x->group < y->group;
        return x_first ? -1 : 1;
    }
    if (xf != yf) return xf ? -1 : 1;
    if (x->mode != y->mode) {
        if (x->mode == MOQ_SCHED_MODE_DATAGRAM) return -1;
        if (y->mode == MOQ_SCHED_MODE_DATAGRAM) return 1;
        return 0;
    }
    if (x->mode == MOQ_SCHED_MODE_SUBGROUP && x->subgroup != y->subgroup)
        return x->subgroup < y->subgroup ? -1 : 1;
    if (x->mode == MOQ_SCHED_MODE_DATAGRAM && x->object != y->object)
        return x->object < y->object ? -1 : 1;
    return 0;
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
        if (model_cmp(&q[i], &q[best]) < 0) best = i;
    }
    return best;
}

/* What one batched poll of up to k takes from the model: picks by the rule
 * until k, the queue's end, or a barrier picked (which stops the poll). */
static size_t model_batch(const fix_t *f, size_t k, uint64_t *tags, bool *blocked)
{
    rec_t q[MAXQ];
    size_t n = f->n, m = 0;
    memcpy(q, f->q, n * sizeof(q[0]));
    *blocked = false;
    while (m < k && n) {
        const size_t at = model_select(q, n);
        if (q[at].barrier) {
            *blocked = true;
            break;
        }
        tags[m++] = q[at].tag;
        memmove(&q[at], &q[at + 1], (n - at - 1) * sizeof(q[0]));
        n--;
    }
    return m;
}

/* -- Driving the session ------------------------------------------- */

static bool fix_push(fix_t *f, rec_t r)
{
    if (f->n >= MAXQ) return false;
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
    m.owner = r.owner;
    m.has_location = r.loc;
    m.mode = r.mode;
    m.group_order = (uint8_t)(r.desc ? MOQ_GROUP_ORDER_DESCENDING : MOQ_GROUP_ORDER_ASCENDING);
    m.fill_group_order = m.group_order;
    m.group_id = r.group;
    m.subgroup_id = r.subgroup;
    m.object_id = r.object;
    m.terminal_barrier = r.barrier;
    const moq_stream_ref_t ref = moq_stream_ref_from_u64(r.ref);
    switch (r.kind) {
    case MOQ_ACTION_SEND_DATA:
        a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a.u.send_data.stream_ref = ref;
        memcpy(a.u.send_data.header, &r.tag, sizeof(r.tag));
        a.u.send_data.header_len = (uint8_t)sizeof(r.tag);
        break;
    case MOQ_ACTION_SEND_DATAGRAM:
        a.detail_size = (uint32_t)sizeof(moq_send_datagram_action_t);
        a.u.send_datagram.len = (size_t)r.tag;
        break;
    case MOQ_ACTION_SET_DATA_PRIORITY:
        a.detail_size = (uint32_t)sizeof(moq_set_data_priority_action_t);
        a.u.set_data_priority.stream_ref = ref;
        a.u.set_data_priority.priority.group_id = r.tag;
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

/* One batched poll of up to k actions. Fills tags, drops the taken records
 * from the model and returns how many the poll took. */
static size_t fix_poll_batch(fix_t *f, size_t k, uint64_t *tags, moq_result_t *rc)
{
    moq_action_t out[MAXQ];
    size_t got = 0;
    *rc = moq_session_poll_actions_ex(f->s, out, k, sizeof(out[0]), &got);
    for (size_t i = 0; i < got; i++) {
        tags[i] = tag_of(&out[i]);
        moq_action_cleanup(&out[i]);
        const size_t at = fix_find(f, tags[i]);
        if (at < f->n) fix_remove(f, at);
    }
    return got;
}

/* One batched poll of up to k, checked against the hand-worked tags and
 * against the rule. */
static int expect_batch(fix_t *f, size_t k, const uint64_t *want, size_t m,
                        moq_result_t want_rc)
{
    int failures = 0;
    uint64_t model[MAXQ];
    bool blocked = false;
    const size_t mm = model_batch(f, k, model, &blocked);
    MOQ_TEST_CHECK_EQ_SIZE(mm, m);
    for (size_t i = 0; i < m && i < mm; i++) MOQ_TEST_CHECK_EQ_U64(model[i], want[i]);
    uint64_t got[MAXQ];
    moq_result_t rc = MOQ_OK;
    const size_t n = fix_poll_batch(f, k, got, &rc);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)want_rc);
    MOQ_TEST_CHECK_EQ_SIZE(n, m);
    for (size_t i = 0; i < n && i < m; i++) MOQ_TEST_CHECK_EQ_U64(got[i], want[i]);
    return failures;
}

/* -- Selection counters -------------------------------------------- */

typedef struct {
    uint64_t nondata, fallback, builds, certified;
    uint64_t priority, key, mode, direction, owners;
    uint64_t visits, served, reused, invalidated;
} counts_t;

static counts_t counts_now(void)
{
    return (counts_t){
        .nondata = session_sched_select_nondata,
        .fallback = session_sched_select_fallback,
        .builds = session_sched_cert_builds,
        .certified = session_sched_cert_certified,
        .priority = session_sched_cert_refused_priority,
        .key = session_sched_cert_refused_key,
        .mode = session_sched_cert_refused_mode,
        .direction = session_sched_cert_refused_direction,
        .owners = session_sched_cert_refused_owners,
        .visits = session_sched_cert_visits,
        .served = session_sched_cert_served,
        .reused = session_sched_cert_reused,
        .invalidated = session_sched_cert_invalidated,
    };
}

static counts_t counts_since(const counts_t *b)
{
    const counts_t n = counts_now();
    return (counts_t){
        .nondata = n.nondata - b->nondata,
        .fallback = n.fallback - b->fallback,
        .builds = n.builds - b->builds,
        .certified = n.certified - b->certified,
        .priority = n.priority - b->priority,
        .key = n.key - b->key,
        .mode = n.mode - b->mode,
        .direction = n.direction - b->direction,
        .owners = n.owners - b->owners,
        .visits = n.visits - b->visits,
        .served = n.served - b->served,
        .reused = n.reused - b->reused,
        .invalidated = n.invalidated - b->invalidated,
    };
}

static uint64_t refused(const counts_t *d)
{
    return d->priority + d->key + d->mode + d->direction + d->owners;
}

/* The identities the counters keep by definition. */
static int check_identities(const counts_t *d)
{
    int failures = 0;
    MOQ_TEST_CHECK_EQ_U64(d->builds, d->certified + refused(d));
    MOQ_TEST_CHECK_EQ_U64(d->served, d->certified + d->reused);
    return failures;
}

#define COUNT(a) (sizeof(a) / sizeof((a)[0]))

/* -- Single windows ------------------------------------------------ */

enum { CERT, REF_PRIORITY, REF_KEY, REF_MODE, REF_DIRECTION, REF_OWNERS };

/* Push one window, take it whole in one batched poll, and check the order
 * (indices into recs) and how the window's single walk ended. */
static int run_row(uint32_t cap, const rec_t *recs, size_t n, const size_t *order,
                   int outcome)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, cap));
    if (!f.s) return failures;
    const uint64_t t0 = f.next_tag;
    for (size_t i = 0; i < n; i++) MOQ_TEST_CHECK(fix_push(&f, recs[i]));
    uint64_t want[MAXQ];
    for (size_t i = 0; i < n; i++) want[i] = t0 + order[i];
    const counts_t before = counts_now();
    failures += expect_batch(&f, n, want, n, MOQ_OK);
    const counts_t d = counts_since(&before);
    failures += check_identities(&d);
    MOQ_TEST_CHECK_EQ_U64(d.builds, 1);
    MOQ_TEST_CHECK_EQ_U64(d.nondata, 0);
    MOQ_TEST_CHECK_EQ_U64(d.invalidated, 0);
    if (outcome == CERT) {
        MOQ_TEST_CHECK_EQ_U64(d.certified, 1);
        MOQ_TEST_CHECK_EQ_U64(d.served, n);
        MOQ_TEST_CHECK_EQ_U64(d.reused, n - 1);
        MOQ_TEST_CHECK_EQ_U64(d.fallback, 0);
        MOQ_TEST_CHECK_EQ_U64(d.visits, n);
    } else {
        const uint64_t reason = outcome == REF_PRIORITY ? d.priority
                              : outcome == REF_KEY ? d.key
                              : outcome == REF_MODE ? d.mode
                              : outcome == REF_DIRECTION ? d.direction : d.owners;
        MOQ_TEST_CHECK_EQ_U64(reason, 1);
        MOQ_TEST_CHECK_EQ_U64(d.certified, 0);
        MOQ_TEST_CHECK_EQ_U64(d.served, 0);
        MOQ_TEST_CHECK_EQ_U64(d.fallback, n);
    }
    MOQ_TEST_CHECK_EQ_SIZE(f.n, 0);
    fix_done(&f);
    return failures;
}

/* Owners tie with each other at one priority but order within themselves,
 * so the tie is not transitive: A ties B, B ties C, C precedes A. Each of
 * the six queue orders, with its order worked by hand. A window certifies
 * only when each owner's own actions are in order; otherwise the one-pass
 * selector decides every pick. */
static int test_cross_owner_permutations(void)
{
    int failures = 0;
    const rec_t A = loc_rec(1, 1, 2), B = loc_rec(2, 2, 1), C = loc_rec(3, 1, 1);
    static const struct {
        char queue[4];
        char taken[4];
        int  outcome;
    } rows[] = {
        { "ABC", "CAB", REF_KEY },
        { "ACB", "CAB", REF_KEY },
        { "BAC", "BCA", REF_KEY },
        { "BCA", "BCA", CERT },
        { "CAB", "CAB", CERT },
        { "CBA", "CBA", CERT },
    };
    for (size_t r = 0; r < COUNT(rows); r++) {
        rec_t recs[3];
        size_t order[3] = { 0, 0, 0 };
        for (size_t i = 0; i < 3; i++) {
            const char c = rows[r].queue[i];
            recs[i] = c == 'A' ? A : c == 'B' ? B : C;
        }
        for (size_t i = 0; i < 3; i++)
            for (size_t j = 0; j < 3; j++)
                if (rows[r].queue[j] == rows[r].taken[i]) order[i] = j;
        failures += run_row(8, recs, 3, order, rows[r].outcome);
    }
    return failures;
}

/* Taking an action makes the next one on its stream a candidate, so every
 * data action counts, not only the current candidates. A(S, 1), B(T, 2),
 * C(S, 0): C waits behind A, A goes first, then C beats B. The same with
 * one owner's locations. And when the window does certify, the exposed
 * action ties and keeps queue order. */
static int test_successor_exposure(void)
{
    int failures = 0;
    {
        const rec_t recs[] = { data_rec(1, 1, 0), data_rec(2, 2, 0), data_rec(1, 0, 0) };
        static const size_t order[] = { 0, 2, 1 };
        failures += run_row(8, recs, COUNT(recs), order, REF_PRIORITY);
    }
    {
        const rec_t recs[] = { loc_rec(1, 1, 2), loc_rec(2, 1, 3), loc_rec(1, 1, 1) };
        static const size_t order[] = { 0, 2, 1 };
        failures += run_row(8, recs, COUNT(recs), order, REF_KEY);
    }
    {
        const rec_t recs[] = { data_rec(1, 0, 0), data_rec(2, 1, 0), data_rec(1, 1, 0) };
        static const size_t order[] = { 0, 1, 2 };
        failures += run_row(8, recs, COUNT(recs), order, CERT);
    }
    return failures;
}

/* Each branch of the comparison the walk relies on, and each shape it does
 * not certify. */
static int test_comparator_boundaries(void)
{
    int failures = 0;
    /* A better subscriber, then publisher, priority later in the window. */
    {
        const rec_t recs[] = { data_rec(1, 2, 0), data_rec(2, 1, 0) };
        static const size_t order[] = { 1, 0 };
        failures += run_row(8, recs, COUNT(recs), order, REF_PRIORITY);
    }
    {
        const rec_t recs[] = { data_rec(1, 1, 3), data_rec(2, 1, 2) };
        static const size_t order[] = { 1, 0 };
        failures += run_row(8, recs, COUNT(recs), order, REF_PRIORITY);
    }
    /* Descending group order: the later group first. */
    {
        const rec_t recs[] = { descending(loc_rec(1, 1, 1)), descending(loc_rec(2, 1, 2)) };
        static const size_t order[] = { 1, 0 };
        failures += run_row(8, recs, COUNT(recs), order, REF_KEY);
    }
    {
        const rec_t recs[] = { descending(loc_rec(1, 1, 2)), descending(loc_rec(2, 1, 1)) };
        static const size_t order[] = { 0, 1 };
        failures += run_row(8, recs, COUNT(recs), order, CERT);
    }
    /* Within a group a datagram precedes a subgroup. */
    {
        const rec_t recs[] = { loc_rec(1, 1, 1), loc_dgram(1, 1, 0) };
        static const size_t order[] = { 1, 0 };
        failures += run_row(8, recs, COUNT(recs), order, REF_KEY);
    }
    {
        const rec_t recs[] = { loc_dgram(1, 1, 0), loc_rec(1, 1, 1) };
        static const size_t order[] = { 0, 1 };
        failures += run_row(8, recs, COUNT(recs), order, CERT);
    }
    /* Lower Subgroup ID, lower datagram Object ID. */
    {
        const rec_t recs[] = { with_subgroup(loc_rec(1, 1, 1), 5),
                               with_subgroup(loc_rec(2, 1, 1), 2) };
        static const size_t order[] = { 1, 0 };
        failures += run_row(8, recs, COUNT(recs), order, REF_KEY);
    }
    {
        const rec_t recs[] = { loc_dgram(1, 1, 7), loc_dgram(1, 1, 3) };
        static const size_t order[] = { 1, 0 };
        failures += run_row(8, recs, COUNT(recs), order, REF_KEY);
    }
    /* Equal keys keep queue order. */
    {
        const rec_t recs[] = { loc_rec(1, 1, 1), loc_rec(2, 1, 1), loc_rec(3, 1, 1) };
        static const size_t order[] = { 0, 1, 2 };
        failures += run_row(8, recs, COUNT(recs), order, CERT);
    }
    /* An action without an owner or a location ties at equal priority; it
     * does not hide an owner's order across it. */
    {
        const rec_t recs[] = { data_rec(1, 0, 0), loc_rec(2, 1, 1) };
        static const size_t order[] = { 0, 1 };
        failures += run_row(8, recs, COUNT(recs), order, CERT);
    }
    {
        const rec_t recs[] = { loc_rec(1, 1, 2), data_rec(2, 0, 0), loc_rec(3, 1, 1) };
        static const size_t order[] = { 2, 0, 1 };
        failures += run_row(8, recs, COUNT(recs), order, REF_KEY);
    }
    {
        rec_t fetch = with_mode(data_rec(1, 0, 0), MOQ_SCHED_MODE_FETCH);
        fetch.owner = 1;
        const rec_t recs[] = { fetch, loc_rec(2, 1, 1) };
        static const size_t order[] = { 0, 1 };
        failures += run_row(8, recs, COUNT(recs), order, CERT);
    }
    /* A located fill precedes the subgroup object of its group; a located
     * mode other than subgroup or datagram is not certified. */
    {
        const rec_t recs[] = { loc_rec(1, 1, 1), with_mode(loc_rec(2, 1, 1), MOQ_SCHED_MODE_FILL) };
        static const size_t order[] = { 1, 0 };
        failures += run_row(8, recs, COUNT(recs), order, REF_MODE);
    }
    {
        const rec_t recs[] = { loc_rec(1, 1, 1), with_mode(loc_rec(2, 1, 1), MOQ_SCHED_MODE_NONE) };
        static const size_t order[] = { 0, 1 };
        failures += run_row(8, recs, COUNT(recs), order, REF_MODE);
    }
    /* One owner's group order changing: the comparison follows the later
     * action's order. */
    {
        const rec_t recs[] = { loc_rec(1, 1, 1), descending(loc_rec(2, 1, 2)) };
        static const size_t order[] = { 1, 0 };
        failures += run_row(8, recs, COUNT(recs), order, REF_DIRECTION);
    }
    /* Eight located owners are tracked; a ninth is not certified. */
    {
        rec_t recs[9];
        size_t order[9];
        for (size_t i = 0; i < 9; i++) {
            recs[i] = loc_rec(10 + i, 1 + i, 1);
            order[i] = i;
        }
        failures += run_row(16, recs, 8, order, CERT);
        failures += run_row(16, recs, 9, order, REF_OWNERS);
    }
    /* A stream's priority key is data: one that improves on the window is
     * not certified, one that does not is. */
    {
        const rec_t recs[] = { data_rec(1, 3, 0), key_rec(2, 1) };
        static const size_t order[] = { 1, 0 };
        failures += run_row(8, recs, COUNT(recs), order, REF_PRIORITY);
    }
    {
        const rec_t recs[] = { data_rec(1, 1, 0), key_rec(2, 3) };
        static const size_t order[] = { 0, 1 };
        failures += run_row(8, recs, COUNT(recs), order, CERT);
    }
    /* Two owners interleaved at one priority, each in its own order. */
    {
        const rec_t recs[] = { loc_rec(1, 1, 1), loc_rec(2, 2, 5),
                               loc_rec(3, 1, 2), loc_rec(4, 2, 6) };
        static const size_t order[] = { 0, 1, 2, 3 };
        failures += run_row(8, recs, COUNT(recs), order, CERT);
    }
    return failures;
}

/* -- Several windows and several polls ----------------------------- */

/* One poll takes a whole certified window from rings of capacity 1 to 16,
 * the head wrapping each ring many times: one walk per poll, every later
 * pick reused. Above capacity 1 the empty ring is first advanced by one, so
 * every window starts one slot in and its last action sits in slot 0: the
 * certificate's walk and its reuse both cross the end of the storage. */
static int test_ring_wrap_reuse(void)
{
    int failures = 0;
    static const uint32_t caps[] = { 1, 2, 4, 8, 16 };
    for (size_t c = 0; c < COUNT(caps); c++) {
        const uint32_t cap = caps[c];
        fix_t f;
        MOQ_TEST_CHECK(fix_init(&f, cap));
        if (!f.s) continue;
        if (cap > 1) {
            const uint64_t lead[] = { f.next_tag };
            MOQ_TEST_CHECK(fix_push(&f, data_rec(999, 0, 0)));
            failures += expect_batch(&f, 1, lead, COUNT(lead), MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(f.s->action_head % cap, 1);
        }
        const size_t start = f.s->action_head;
        for (uint64_t round = 0; round < 12 && !failures; round++) {
            if (cap > 1) {
                const size_t first = f.s->action_head % cap;
                const size_t last = (f.s->action_head + cap - 1) % cap;
                MOQ_TEST_CHECK_EQ_SIZE(first, 1);
                MOQ_TEST_CHECK_EQ_SIZE(last, 0);
            }
            uint64_t want[MAXQ];
            for (uint32_t i = 0; i < cap; i++) {
                /* Pairs share a stream; Subgroup IDs never go back. */
                want[i] = f.next_tag;
                MOQ_TEST_CHECK(fix_push(&f, with_subgroup(
                    loc_rec(1000 + round * 16 + i / 2, 1, round), i / 2)));
            }
            const counts_t before = counts_now();
            failures += expect_batch(&f, cap, want, cap, MOQ_OK);
            const counts_t d = counts_since(&before);
            failures += check_identities(&d);
            MOQ_TEST_CHECK_EQ_U64(d.builds, 1);
            MOQ_TEST_CHECK_EQ_U64(d.certified, 1);
            MOQ_TEST_CHECK_EQ_U64(d.served, cap);
            MOQ_TEST_CHECK_EQ_U64(d.reused, cap - 1);
            MOQ_TEST_CHECK_EQ_U64(d.fallback, 0);
        }
        MOQ_TEST_CHECK(f.s->action_head - start > 8u * cap);
        fix_done(&f);
    }
    return failures;
}

/* A non-data action ends a window; behind it a new window is walked. */
static int test_window_boundary(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    const uint64_t t = f.next_tag;
    MOQ_TEST_CHECK(fix_push(&f, data_rec(1, 0, 0)));   /* t */
    MOQ_TEST_CHECK(fix_push(&f, data_rec(2, 1, 0)));   /* t+1 */
    MOQ_TEST_CHECK(fix_push(&f, ctrl_rec()));          /* t+2 */
    MOQ_TEST_CHECK(fix_push(&f, data_rec(3, 5, 0)));   /* t+3 */
    MOQ_TEST_CHECK(fix_push(&f, data_rec(4, 2, 0)));   /* t+4: better than t+3 */
    const uint64_t want[] = { t, t + 1, t + 2, t + 4, t + 3 };
    const counts_t before = counts_now();
    failures += expect_batch(&f, COUNT(want), want, COUNT(want), MOQ_OK);
    const counts_t d = counts_since(&before);
    failures += check_identities(&d);
    MOQ_TEST_CHECK_EQ_U64(d.builds, 2);
    MOQ_TEST_CHECK_EQ_U64(d.certified, 1);
    MOQ_TEST_CHECK_EQ_U64(d.priority, 1);
    MOQ_TEST_CHECK_EQ_U64(d.served, 2);
    MOQ_TEST_CHECK_EQ_U64(d.reused, 1);
    MOQ_TEST_CHECK_EQ_U64(d.nondata, 1);
    MOQ_TEST_CHECK_EQ_U64(d.fallback, 2);
    fix_done(&f);
    return failures;
}

/* A terminal barrier stays a candidate; when it is picked the poll stops,
 * whether the certificate or the one-pass selector picked it. */
static int test_barriers(void)
{
    int failures = 0;
    {
        fix_t f;
        MOQ_TEST_CHECK(fix_init(&f, 8));
        if (!f.s) return failures;
        MOQ_TEST_CHECK(fix_push(&f, as_barrier(data_rec(1, 0, 0))));
        MOQ_TEST_CHECK(fix_push(&f, data_rec(2, 5, 0)));
        const counts_t before = counts_now();
        failures += expect_batch(&f, 8, NULL, 0, MOQ_ERR_WOULD_BLOCK);
        const counts_t d = counts_since(&before);
        MOQ_TEST_CHECK_EQ_U64(d.certified, 1);
        MOQ_TEST_CHECK_EQ_U64(d.served, 1);
        MOQ_TEST_CHECK_EQ_SIZE(f.n, 2);
        fix_done(&f);
    }
    {
        fix_t f;
        MOQ_TEST_CHECK(fix_init(&f, 8));
        if (!f.s) return failures;
        const uint64_t t = f.next_tag;
        MOQ_TEST_CHECK(fix_push(&f, data_rec(1, 0, 0)));
        MOQ_TEST_CHECK(fix_push(&f, as_barrier(data_rec(2, 1, 0))));
        const uint64_t want[] = { t };
        const counts_t before = counts_now();
        failures += expect_batch(&f, 8, want, COUNT(want), MOQ_OK);
        const counts_t d = counts_since(&before);
        failures += check_identities(&d);
        MOQ_TEST_CHECK_EQ_U64(d.certified, 1);
        MOQ_TEST_CHECK_EQ_U64(d.served, 2);
        MOQ_TEST_CHECK_EQ_U64(d.reused, 1);
        fix_done(&f);
    }
    {
        fix_t f;
        MOQ_TEST_CHECK(fix_init(&f, 8));
        if (!f.s) return failures;
        MOQ_TEST_CHECK(fix_push(&f, data_rec(2, 5, 0)));
        MOQ_TEST_CHECK(fix_push(&f, as_barrier(data_rec(1, 0, 0))));
        const counts_t before = counts_now();
        failures += expect_batch(&f, 8, NULL, 0, MOQ_ERR_WOULD_BLOCK);
        const counts_t d = counts_since(&before);
        MOQ_TEST_CHECK_EQ_U64(d.priority, 1);
        MOQ_TEST_CHECK_EQ_U64(d.fallback, 1);
        fix_done(&f);
    }
    return failures;
}

/* An element too small for SEND_DATA stops the poll at the first one; the
 * actions stay queued for a full-size poll, in the same order. */
static int test_small_element(void)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, 8));
    if (!f.s) return failures;
    const uint64_t t = f.next_tag;
    MOQ_TEST_CHECK(fix_push(&f, ctrl_rec()));          /* t */
    MOQ_TEST_CHECK(fix_push(&f, data_rec(1, 0, 0)));   /* t+1 */
    MOQ_TEST_CHECK(fix_push(&f, data_rec(2, 0, 0)));   /* t+2 */
    const size_t prefix = offsetof(moq_action_t, u);
    uint8_t buf[4 * sizeof(moq_action_t)];
    size_t got = 99;
    counts_t before = counts_now();
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_actions_ex(f.s, buf, 4, prefix, &got), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(got, 1);
    moq_action_t first;
    memset(&first, 0, sizeof(first));
    memcpy(&first, buf, prefix);
    MOQ_TEST_CHECK(first.kind == MOQ_ACTION_SEND_CONTROL);
    if (got == 1) fix_remove(&f, 0);
    counts_t d = counts_since(&before);
    MOQ_TEST_CHECK_EQ_U64(d.nondata, 1);
    MOQ_TEST_CHECK_EQ_U64(d.certified, 1);
    MOQ_TEST_CHECK_EQ_U64(d.served, 1);

    got = 99;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_actions_ex(f.s, buf, 4, prefix, &got),
                          (int)MOQ_ERR_ABI_MISMATCH);
    MOQ_TEST_CHECK_EQ_SIZE(got, 0);

    const uint64_t want[] = { t + 1, t + 2 };
    before = counts_now();
    failures += expect_batch(&f, 4, want, COUNT(want), MOQ_OK);
    d = counts_since(&before);
    failures += check_identities(&d);
    MOQ_TEST_CHECK_EQ_U64(d.certified, 1);
    MOQ_TEST_CHECK_EQ_U64(d.served, 2);
    MOQ_TEST_CHECK_EQ_U64(d.reused, 1);
    fix_done(&f);
    return failures;
}

/* -- Seeded traffic ------------------------------------------------ */

static uint32_t lcg(uint32_t *st)
{
    *st = *st * 1664525u + 1013904223u;
    return *st >> 8;
}

typedef struct {
    uint64_t certified, reused, fallback, refused;
} traffic_t;

/* Seeded traffic through a small ring, every batched poll checked against
 * the rule. Stretches of one or two owners in order, which certify, alternate
 * with stretches of every kind, owner, mode, group order and priority. No
 * record is a terminal barrier; test_barriers covers those. */
static int run_random(uint32_t cap, uint32_t seed, int steps, traffic_t *total)
{
    int failures = 0;
    fix_t f;
    MOQ_TEST_CHECK(fix_init(&f, cap));
    if (!f.s) return failures;
    static const uint64_t refs[] = { 1, 2, 3, 4, 1 + (UINT64_C(1) << 40), UINT64_MAX };
    uint32_t st = seed;
    bool owner_desc[11];
    for (size_t o = 0; o < COUNT(owner_desc); o++) owner_desc[o] = lcg(&st) % 4 == 0;
    uint64_t run_group = 0;
    const size_t start = f.s->action_head;
    const counts_t before = counts_now();
    for (int step = 0; step < steps && !failures; step++) {
        const bool orderly = (step / 50) % 2 == 0;
        const uint32_t op = lcg(&st) % 8;
        if (f.n < cap && (op < 4 || f.n == 0)) {
            rec_t r;
            if (orderly) {
                if (lcg(&st) % 8 == 0) run_group++;
                const uint64_t ref = refs[lcg(&st) % 4];
                const uint64_t owner = 1 + lcg(&st) % 2;
                r = loc_rec(ref, owner, run_group);
                r.sp = r.pp = 1;
            } else {
                const uint32_t k = lcg(&st) % 20;
                if (k < 12) {
                    r = data_rec(refs[lcg(&st) % COUNT(refs)], 0, 0);
                    const uint32_t m = lcg(&st) % 16;
                    r.mode = m < 11 ? MOQ_SCHED_MODE_SUBGROUP
                           : m < 13 ? MOQ_SCHED_MODE_FILL
                           : m < 15 ? MOQ_SCHED_MODE_FETCH : MOQ_SCHED_MODE_NONE;
                } else if (k < 15) {
                    r = dgram_rec(0);
                } else if (k < 17) {
                    r = key_rec(lcg(&st) % 3 == 0 ? 0 : refs[lcg(&st) % COUNT(refs)], 0);
                } else {
                    r = ctrl_rec();
                }
                r.sp = (uint8_t)(lcg(&st) % 3);
                r.pp = (uint8_t)(lcg(&st) % 2);
                if (r.kind == MOQ_ACTION_SEND_DATA || r.kind == MOQ_ACTION_SEND_DATAGRAM) {
                    r.owner = lcg(&st) % 11;
                    r.loc = lcg(&st) % 8 != 0;
                    r.desc = owner_desc[r.owner] != (lcg(&st) % 32 == 0);
                    r.group = lcg(&st) % 4;
                    r.subgroup = lcg(&st) % 3;
                    r.object = lcg(&st) % 4;
                }
            }
            MOQ_TEST_CHECK(fix_push(&f, r));
        } else {
            const size_t k = 1 + lcg(&st) % 8;
            uint64_t want[MAXQ];
            bool blocked = false;
            const size_t m = model_batch(&f, k, want, &blocked);
            failures += expect_batch(&f, k, want, m, MOQ_OK);
        }
    }
    while (f.n && !failures) {
        uint64_t want[MAXQ];
        bool blocked = false;
        const size_t m = model_batch(&f, cap, want, &blocked);
        failures += expect_batch(&f, cap, want, m, MOQ_OK);
    }
    MOQ_TEST_CHECK_EQ_SIZE(f.n, 0);
    MOQ_TEST_CHECK(f.s->action_head - start > 8u * cap);
    const counts_t d = counts_since(&before);
    failures += check_identities(&d);
    total->certified += d.certified;
    total->reused += d.reused;
    total->fallback += d.fallback;
    total->refused += refused(&d);
    fix_done(&f);
    return failures;
}

static int test_seeded_traffic(void)
{
    int failures = 0;
    traffic_t total = { .certified = 0 };
    failures += run_random(1, 0xce710001u, 400, &total);
    failures += run_random(2, 0xce710002u, 1500, &total);
    failures += run_random(4, 0xce710004u, 3000, &total);
    failures += run_random(8, 0xce710008u, 3000, &total);
    failures += run_random(16, 0xce710010u, 3000, &total);
    /* Both paths ran: certified windows with reused picks, and refused
     * windows decided by the one-pass selector. */
    MOQ_TEST_CHECK(total.certified > 0);
    MOQ_TEST_CHECK(total.reused > 0);
    MOQ_TEST_CHECK(total.fallback > 0);
    MOQ_TEST_CHECK(total.refused > 0);
    return failures;
}

/* -- An established session ---------------------------------------- */

#define REQUIRE(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        return false; \
    } \
} while (0)

enum { SESS_OBJECTS = 8 };

typedef struct {
    moq_simpair_t      *pair;
    moq_session_t      *sender;
    moq_subscription_t  sub;
    moq_rcbuf_t        *objects[SESS_OBJECTS];   /* a payload of its own per object */
    uint64_t            now;
} sess_t;

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) moq_event_cleanup(&ev);
}

/* A server that has accepted one subscription and sends its objects. */
static bool sess_setup(sess_t *f, uint32_t max_actions, uint32_t max_events)
{
    memset(f, 0, sizeof(*f));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = MOQ_VERSION_DRAFT_18;
    cfg.max_actions = max_actions;
    cfg.max_events = max_events;
    cfg.initial_now_us = 1000000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 32;
    REQUIRE(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    REQUIRE(moq_simpair_start(f->pair) == MOQ_OK);
    REQUIRE(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    moq_session_t *client = moq_simpair_client(f->pair);
    moq_session_t *server = moq_simpair_server(f->pair);
    f->sender = server;
    drain_events(client);
    drain_events(server);
    f->now = moq_simpair_now_us(f->pair);
    moq_bytes_t ns = MOQ_BYTES_LITERAL("certified");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.track_namespace = (moq_namespace_t){ &ns, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("v");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t local;
    REQUIRE(moq_session_subscribe(client, &sc, f->now, &local) == MOQ_OK);
    REQUIRE(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    moq_event_t ev;
    REQUIRE(moq_session_poll_events(server, &ev, 1) == 1);
    REQUIRE(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    f->sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    REQUIRE(moq_session_accept_subscribe(server, f->sub, &ac, f->now) == MOQ_OK);
    REQUIRE(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    drain_events(client);
    drain_events(server);
    REQUIRE(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    for (size_t i = 0; i < SESS_OBJECTS; i++) {
        char text[24];
        const int len = snprintf(text, sizeof(text), "object-%u", (unsigned)i);
        REQUIRE(len > 0 && (size_t)len < sizeof(text));
        REQUIRE(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)text, (size_t)len,
                                 &f->objects[i]) == MOQ_OK);
    }
    return true;
}

static void sess_cleanup(sess_t *f)
{
    for (size_t i = 0; i < SESS_OBJECTS; i++)
        if (f->objects[i]) moq_rcbuf_decref(f->objects[i]);
    if (f->pair) moq_simpair_destroy(f->pair);
    memset(f, 0, sizeof(*f));
}

/* Open a subgroup of `group` and take its header, leaving the ring empty. */
static bool sess_open_group(sess_t *f, uint64_t group, moq_subgroup_handle_t *h,
                            moq_stream_ref_t *ref)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = group;
    sc.subgroup_id = 7;
    REQUIRE(moq_session_open_subgroup(f->sender, f->sub, &sc, f->now, h) == MOQ_OK);
    moq_action_t a;
    REQUIRE(moq_session_poll_actions(f->sender, &a, 1) == 1);
    REQUIRE(a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.header_len != 0);
    *ref = a.u.send_data.stream_ref;
    moq_action_cleanup(&a);
    REQUIRE(f->sender->action_head == f->sender->action_tail);
    return true;
}

/* Fill the ring with X's objects 0 .. cap-2, each with its own payload, and
 * then X's FIN. */
static bool sess_fill_x(sess_t *f, moq_subgroup_handle_t x)
{
    const size_t objects = f->sender->action_cap - 1;
    REQUIRE(objects <= SESS_OBJECTS);
    for (size_t i = 0; i < objects; i++)
        REQUIRE(moq_session_write_object(f->sender, x, i, f->objects[i], f->now) == MOQ_OK);
    REQUIRE(moq_session_close_subgroup(f->sender, x, f->now) == MOQ_OK);
    REQUIRE(action_queue_full(f->sender));
    return true;
}

/* Fill notification storage with scalar events that own nothing. */
static size_t fill_events(moq_session_t *s)
{
    size_t n = 0;
    while (s->event_tail - s->event_head < s->event_cap) {
        moq_event_t *e = &s->events[s->event_tail++ % s->event_cap];
        memset(e, 0, sizeof(*e));
        e->kind = MOQ_EVENT_GOAWAY;
        ++n;
    }
    return n;
}

static bool release_events(moq_session_t *s, size_t n)
{
    while (n--) {
        moq_event_t e;
        REQUIRE(moq_session_poll_events(s, &e, 1) == 1);
        REQUIRE(e.kind == MOQ_EVENT_GOAWAY);
        moq_event_cleanup(&e);
    }
    return true;
}

/* Two subgroups opened, H's terminal latched by a STOP_SENDING, and the
 * ring filled with X's objects and FIN. */
static bool sess_stopped_and_full(sess_t *f, moq_subgroup_handle_t *h, moq_stream_ref_t *href,
                                  moq_stream_ref_t *xref, bool events_full, size_t *events)
{
    moq_subgroup_handle_t x;
    REQUIRE(sess_open_group(f, 4, h, href));
    REQUIRE(sess_open_group(f, 5, &x, xref));
    *events = events_full ? fill_events(f->sender) : 0;
    if (events_full)
        REQUIRE(moq_session_on_data_stop(f->sender, *href, 0, f->now) == MOQ_ERR_WOULD_BLOCK);
    REQUIRE(sess_fill_x(f, x));
    if (!events_full)
        REQUIRE(moq_session_on_data_stop(f->sender, *href, 0, f->now) == MOQ_ERR_WOULD_BLOCK);
    REQUIRE(f->sender->subgroups[moq_handle_slot(h->_opaque)].state == MOQ_SG_TERMINAL_OWED);
    return true;
}

/* What one SEND_DATA hands over: its stream, payload, header bytes and FIN. */
typedef struct {
    moq_stream_ref_t   ref;
    const moq_rcbuf_t *payload;
    uint8_t            header[32];
    uint8_t            header_len;
    bool               fin;
} sent_t;

/* The queued actions, read in queue order before the poll. They must be X's
 * objects with their own payloads in write order, then X's FIN; the result
 * is their count, or 0 when the ring holds anything else. */
static size_t snapshot_x(const sess_t *f, moq_stream_ref_t xref, sent_t *snap)
{
    const moq_session_t *s = f->sender;
    const size_t n = s->action_tail - s->action_head;
    if (n == 0 || n > MAXQ || n > SESS_OBJECTS + 1) return 0;
    for (size_t i = 0; i < n; i++) {
        const moq_action_t *a = &s->actions[(s->action_head + i) % s->action_cap];
        const moq_send_data_action_t *d = &a->u.send_data;
        const bool fin = i + 1 == n;
        if (a->kind != MOQ_ACTION_SEND_DATA || d->stream_ref._v != xref._v ||
            d->fin != fin || d->header_len > sizeof(d->header) ||
            d->payload != (fin ? NULL : f->objects[i]) ||
            (fin ? d->header_len != 0 : d->header_len == 0))
            return 0;
        snap[i] = (sent_t){ .ref = d->stream_ref, .payload = d->payload,
                            .header_len = d->header_len, .fin = d->fin };
        memcpy(snap[i].header, d->header, d->header_len);
    }
    return n;
}

/* Whether out[0, n) hands over exactly want[0, n), in order. */
static bool sent_match(const moq_action_t *out, const sent_t *want, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        const moq_send_data_action_t *d = &out[i].u.send_data;
        if (out[i].kind != MOQ_ACTION_SEND_DATA || d->stream_ref._v != want[i].ref._v ||
            d->payload != want[i].payload || d->fin != want[i].fin ||
            d->header_len != want[i].header_len ||
            memcmp(d->header, want[i].header, want[i].header_len) != 0)
            return false;
    }
    return true;
}

/* The identity check rejects an output with two objects swapped or one
 * object's payload substituted, and one with a header byte changed or the
 * FIN dropped. The probes are shallow copies of the real output; they share
 * its references and are never cleaned up. */
static int check_identity_controls(const moq_action_t *out, const sent_t *want, size_t n)
{
    int failures = 0;
    MOQ_TEST_CHECK(n >= 3 && n <= MAXQ);
    if (n < 3 || n > MAXQ) return failures;
    moq_action_t probe[MAXQ];
    MOQ_TEST_CHECK(sent_match(out, want, n));

    memcpy(probe, out, n * sizeof(probe[0]));
    const moq_action_t first = probe[0];
    probe[0] = probe[1];
    probe[1] = first;
    MOQ_TEST_CHECK(!sent_match(probe, want, n));
    memcpy(probe, out, n * sizeof(probe[0]));
    probe[1].u.send_data.payload = probe[2].u.send_data.payload;
    MOQ_TEST_CHECK(!sent_match(probe, want, n));

    memcpy(probe, out, n * sizeof(probe[0]));
    probe[0].u.send_data.header[0] ^= 0x01;
    MOQ_TEST_CHECK(!sent_match(probe, want, n));
    memcpy(probe, out, n * sizeof(probe[0]));
    probe[n - 1].u.send_data.fin = false;
    MOQ_TEST_CHECK(!sent_match(probe, want, n));
    return failures;
}

/* A terminal owed while the event queue is full stays owed through the
 * whole poll: every reset retry finds the debt and drops the certificate,
 * so each pick walks the rest of the window again. The objects still go
 * out exactly as queued. */
static int test_event_pressure_rebuilds_every_pick(void)
{
    int failures = 0;
    sess_t f;
    moq_subgroup_handle_t h;
    moq_stream_ref_t href, xref;
    size_t events = 0;
    if (!sess_setup(&f, 8, 4) || !sess_stopped_and_full(&f, &h, &href, &xref, true, &events)) {
        failures++;
        sess_cleanup(&f);
        return failures;
    }
    const size_t n = f.sender->action_cap;
    sent_t want[MAXQ];
    const size_t queued = snapshot_x(&f, xref, want);
    MOQ_TEST_CHECK_EQ_SIZE(queued, n);
    if (queued != n) {
        /* Without the expected queue there is nothing to compare against. */
        sess_cleanup(&f);
        return failures;
    }
    moq_action_t out[MAXQ];
    size_t got = 0;
    const counts_t before = counts_now();
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_actions_ex(f.sender, out, n, sizeof(out[0]), &got),
                          (int)MOQ_OK);
    const counts_t d = counts_since(&before);
    MOQ_TEST_CHECK_EQ_SIZE(got, n);
    if (got == n) {
        MOQ_TEST_CHECK(sent_match(out, want, n));
        failures += check_identity_controls(out, want, n);
    }
    for (size_t i = 0; i < got; i++) moq_action_cleanup(&out[i]);
    failures += check_identities(&d);
    MOQ_TEST_CHECK_EQ_U64(d.builds, n);
    MOQ_TEST_CHECK_EQ_U64(d.certified, n);
    MOQ_TEST_CHECK_EQ_U64(d.served, n);
    MOQ_TEST_CHECK_EQ_U64(d.reused, 0);
    MOQ_TEST_CHECK_EQ_U64(d.invalidated, n);
    MOQ_TEST_CHECK_EQ_U64(d.fallback, 0);
    MOQ_TEST_CHECK_EQ_U64(d.visits, n * (n + 1) / 2);
    const moq_sg_entry_t *debt = &f.sender->subgroups[moq_handle_slot(h._opaque)];
    MOQ_TEST_CHECK(debt->state == MOQ_SG_TERMINAL_OWED && f.sender->sg_reset_owed);

    /* Once the events are taken the reset goes out, then its terminal
     * event. */
    MOQ_TEST_CHECK(release_events(f.sender, events));
    got = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_actions_ex(f.sender, out, 4, sizeof(out[0]), &got),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(got, 1);
    if (got == 1)
        MOQ_TEST_CHECK(out[0].kind == MOQ_ACTION_RESET_DATA &&
                       out[0].u.reset_data.stream_ref._v == href._v &&
                       out[0].u.reset_data.error_code == 0);
    for (size_t i = 0; i < got; i++) moq_action_cleanup(&out[i]);
    moq_event_t e;
    if (moq_session_poll_events(f.sender, &e, 1) == 1) {
        MOQ_TEST_CHECK(e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
        moq_event_cleanup(&e);
    } else {
        fprintf(stderr, "FAIL: %s:%d: no terminal event\n", __FILE__, __LINE__);
        failures++;
    }
    sess_cleanup(&f);
    return failures;
}

/* A retained terminal reset goes out before any ring action and takes no
 * ring slot. It is not a selection: the objects behind it are walked once,
 * the rest reuse that walk, and they go out exactly as queued. */
static int test_retained_terminal(void)
{
    int failures = 0;
    sess_t f;
    moq_subgroup_handle_t h;
    moq_stream_ref_t href, xref;
    size_t events = 0;
    if (!sess_setup(&f, 8, 4) || !sess_stopped_and_full(&f, &h, &href, &xref, false, &events)) {
        failures++;
        sess_cleanup(&f);
        return failures;
    }
    const size_t n = f.sender->action_cap;
    sent_t want[MAXQ];
    const size_t queued = snapshot_x(&f, xref, want);
    MOQ_TEST_CHECK_EQ_SIZE(queued, n);
    if (queued != n) {
        /* Without the expected queue there is nothing to compare against. */
        sess_cleanup(&f);
        return failures;
    }
    moq_action_t out[MAXQ];
    size_t got = 0;
    const counts_t before = counts_now();
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_actions_ex(f.sender, out, n + 1, sizeof(out[0]), &got),
                          (int)MOQ_OK);
    const counts_t d = counts_since(&before);
    MOQ_TEST_CHECK_EQ_SIZE(got, n + 1);
    if (got == n + 1) {
        MOQ_TEST_CHECK(out[0].kind == MOQ_ACTION_RESET_DATA &&
                       out[0].u.reset_data.stream_ref._v == href._v &&
                       out[0].u.reset_data.error_code == 0);
        MOQ_TEST_CHECK(sent_match(out + 1, want, n));
    }
    for (size_t i = 0; i < got; i++) moq_action_cleanup(&out[i]);
    failures += check_identities(&d);
    MOQ_TEST_CHECK_EQ_U64(d.builds, 1);
    MOQ_TEST_CHECK_EQ_U64(d.certified, 1);
    MOQ_TEST_CHECK_EQ_U64(d.served, n);
    MOQ_TEST_CHECK_EQ_U64(d.reused, n - 1);
    MOQ_TEST_CHECK_EQ_U64(d.invalidated, 0);
    MOQ_TEST_CHECK_EQ_U64(d.nondata, 0);
    MOQ_TEST_CHECK_EQ_U64(d.fallback, 0);
    moq_event_t e;
    if (moq_session_poll_events(f.sender, &e, 1) == 1) {
        MOQ_TEST_CHECK(e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED &&
                       e.u.outgoing_subgroup_terminated.cause ==
                           MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING);
        moq_event_cleanup(&e);
    } else {
        fprintf(stderr, "FAIL: %s:%d: no terminal event\n", __FILE__, __LINE__);
        failures++;
    }
    sess_cleanup(&f);
    return failures;
}

/* A reset owed while the ring was full is queued by the retry after the
 * poll's first pick, which drops the certificate; the next pick walks the
 * window again, now ended by that reset, and the rest reuse the walk. The
 * objects go out exactly as queued, then the reset. */
static int test_reset_queued_during_poll(void)
{
    int failures = 0;
    sess_t f;
    if (!sess_setup(&f, 8, 4)) {
        failures++;
        sess_cleanup(&f);
        return failures;
    }
    moq_subgroup_handle_t h, x;
    moq_stream_ref_t href, xref;
    const bool ready = sess_open_group(&f, 4, &h, &href) && sess_open_group(&f, 5, &x, &xref) &&
                       sess_fill_x(&f, x);
    MOQ_TEST_CHECK(ready);
    if (!ready) {
        sess_cleanup(&f);
        return failures;
    }
    const size_t n = f.sender->action_cap;
    /* The state a cancellation leaves when the ring has no room for its
     * reset. */
    moq_sg_entry_t *owed = &f.sender->subgroups[moq_handle_slot(h._opaque)];
    owed->state = MOQ_SG_RESET_OWED;
    owed->owed_reset_code = 9;
    f.sender->sg_reset_owed = true;
    sent_t want[MAXQ];
    const size_t queued = snapshot_x(&f, xref, want);
    MOQ_TEST_CHECK_EQ_SIZE(queued, n);
    if (queued != n) {
        /* Without the expected queue there is nothing to compare against. */
        sess_cleanup(&f);
        return failures;
    }

    moq_action_t out[MAXQ];
    size_t got = 0;
    const counts_t before = counts_now();
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_poll_actions_ex(f.sender, out, n + 1, sizeof(out[0]), &got),
                          (int)MOQ_OK);
    const counts_t d = counts_since(&before);
    MOQ_TEST_CHECK_EQ_SIZE(got, n + 1);
    if (got == n + 1) {
        MOQ_TEST_CHECK(sent_match(out, want, n));
        MOQ_TEST_CHECK(out[n].kind == MOQ_ACTION_RESET_DATA &&
                       out[n].u.reset_data.stream_ref._v == href._v &&
                       out[n].u.reset_data.error_code == 9);
    }
    for (size_t i = 0; i < got; i++) moq_action_cleanup(&out[i]);
    failures += check_identities(&d);
    MOQ_TEST_CHECK_EQ_U64(d.builds, 2);
    MOQ_TEST_CHECK_EQ_U64(d.certified, 2);
    MOQ_TEST_CHECK_EQ_U64(d.served, n);
    MOQ_TEST_CHECK_EQ_U64(d.reused, n - 2);
    MOQ_TEST_CHECK_EQ_U64(d.invalidated, 1);
    MOQ_TEST_CHECK_EQ_U64(d.nondata, 1);
    MOQ_TEST_CHECK_EQ_U64(d.fallback, 0);
    MOQ_TEST_CHECK_EQ_U64(d.visits, n + (n - 1));
    MOQ_TEST_CHECK(owed->state == MOQ_SG_RESETTING && !f.sender->sg_reset_owed);
    sess_cleanup(&f);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_cross_owner_permutations();
    failures += test_successor_exposure();
    failures += test_comparator_boundaries();
    failures += test_ring_wrap_reuse();
    failures += test_window_boundary();
    failures += test_barriers();
    failures += test_small_element();
    failures += test_seeded_traffic();
    failures += test_event_pressure_rebuilds_every_pick();
    failures += test_retained_terminal();
    failures += test_reset_queued_during_poll();

    if (failures == 0)
        printf("test_scheduling_certified: all tests passed\n");
    else
        fprintf(stderr, "test_scheduling_certified: %d failure(s)\n", failures);
    return failures;
}
