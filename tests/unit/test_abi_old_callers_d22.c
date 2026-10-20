/*
 * Callers built against the layout a draft-22 field was appended to.
 *
 * Such a caller's struct ends where that layout ended: struct_size is the
 * layout's sizeof and its tail padding is whatever the caller left there.
 * Each config is passed both ways an old caller can present it -- as a heap
 * block of exactly the old size (a read past it is out of bounds under
 * ASan) and as a current-size struct whose every byte past the old layout's
 * last field is poisoned -- and the result must equal the current struct
 * with every appended field at its default: the same allocations, the same
 * actions on the wire, the same session state.
 *
 * The exported initializers are called as such a caller calls them, through
 * the symbol: they clear and stamp only the old layout and never write past
 * it.
 */
#include <moq/moq.h>
#include <moq/subscriber.h>
#include <moq/publisher.h>
#include "publisher_old_layout.h"
#include "cfg_old_layout.h"
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <stdlib.h>
#include <string.h>

#define FIELD_END(T, f) (offsetof(T, f) + sizeof(((T *)0)->f))
/* The sizeof of the layout that ends at `last` (the alignment is pinned
 * unchanged by test_abi_layout_d22). */
#define SIZEOF_THROUGH(T, last) \
    ((FIELD_END(T, last) + _Alignof(T) - 1) / _Alignof(T) * _Alignof(T))

#define POISON 0xA5u
#define GUARD  16u

/* -- Counting allocator ---------------------------------------------------- */

typedef struct tally {
    moq_alloc_t vt;
    size_t      allocs;
    size_t      bytes;
    long        live;
} tally_t;

static void *tally_alloc(size_t n, void *ctx)
{
    tally_t *t = ctx;
    void *p = malloc(n);
    if (p) { t->allocs++; t->bytes += n; t->live++; }
    return p;
}
static void *tally_realloc(void *p, size_t o, size_t n, void *ctx)
{
    tally_t *t = ctx;
    void *q = realloc(p, n);
    if (q) t->bytes += n > o ? n - o : 0;
    return q;
}
static void tally_free(void *p, size_t n, void *ctx)
{
    tally_t *t = ctx;
    (void)n;
    if (p) t->live--;
    free(p);
}
static void tally_init(tally_t *t)
{
    memset(t, 0, sizeof(*t));
    t->vt.ctx = t;
    t->vt.alloc = tally_alloc;
    t->vt.realloc = tally_realloc;
    t->vt.free = tally_free;
}

/* -- Old-caller views -------------------------------------------------------- */

/* The old caller's struct as an exact heap block: the old layout's bytes
 * from `cur`, its tail padding poisoned, struct_size the old sizeof. */
static void *old_block(const void *cur, size_t end, size_t old)
{
    unsigned char *p = malloc(old);
    if (!p) return NULL;
    memcpy(p, cur, end);
    memset(p + end, POISON, old - end);
    uint32_t ss = (uint32_t)old;
    memcpy(p, &ss, sizeof(ss));
    return p;
}

/* The same caller in current-size storage: every byte past the old layout's
 * last field poisoned, appended fields included. */
static void old_inplace(void *cur, size_t cur_size, size_t end, size_t old)
{
    memset((unsigned char *)cur + end, POISON, cur_size - end);
    uint32_t ss = (uint32_t)old;
    memcpy(cur, &ss, sizeof(ss));
}

/* -- Action digests ------------------------------------------------------------ */

typedef struct digest {
    unsigned char b[4096];
    size_t        n;
    bool          overflow;
} digest_t;

static void dg_put(digest_t *d, const void *p, size_t n)
{
    if (d->n + n > sizeof(d->b)) { d->overflow = true; return; }
    memcpy(d->b + d->n, p, n);
    d->n += n;
}

/* Every queued action, in order: its kind, stream, bytes and FIN. */
static void digest_actions(moq_session_t *s, digest_t *d)
{
    memset(d, 0, sizeof(*d));
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        uint32_t kind = a.kind;
        dg_put(d, &kind, sizeof(kind));
        switch (a.kind) {
        case MOQ_ACTION_SEND_CONTROL:
            dg_put(d, a.u.send_control.data, a.u.send_control.len);
            break;
        case MOQ_ACTION_OPEN_BIDI_STREAM:
            dg_put(d, &a.u.open_bidi_stream.stream_ref,
                   sizeof(a.u.open_bidi_stream.stream_ref));
            dg_put(d, a.u.open_bidi_stream.data, a.u.open_bidi_stream.len);
            dg_put(d, &a.u.open_bidi_stream.fin, sizeof(bool));
            break;
        case MOQ_ACTION_SEND_BIDI_STREAM:
            dg_put(d, &a.u.send_bidi_stream.stream_ref,
                   sizeof(a.u.send_bidi_stream.stream_ref));
            dg_put(d, a.u.send_bidi_stream.data, a.u.send_bidi_stream.len);
            dg_put(d, &a.u.send_bidi_stream.fin, sizeof(bool));
            break;
        default:
            break;
        }
        moq_action_cleanup(&a);
    }
}

static bool digest_same(const digest_t *a, const digest_t *b)
{
    return !a->overflow && !b->overflow && a->n > 0 && a->n == b->n &&
           memcmp(a->b, b->b, a->n) == 0;
}

/* -- Pairs ------------------------------------------------------------------------ */

static moq_bytes_t ns_parts[] = { { (const uint8_t *)"live", 4 } };

static moq_simpair_t *mk_pair(void)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = MOQ_VERSION_DRAFT_22;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return NULL;
    if (moq_simpair_start(sp) < 0) { moq_simpair_destroy(sp); return NULL; }
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    return sp;
}

static void drain(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
}

static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == kind) { *out = ev; return true; }
        moq_event_cleanup(&ev);
    }
    return false;
}

static void subscribe_cfg(moq_subscribe_cfg_t *c)
{
    memset(c, 0, sizeof(*c));
    c->struct_size = sizeof(*c);
    c->track_namespace = (moq_namespace_t){ ns_parts, 1 };
    c->track_name = MOQ_BYTES_LITERAL("v");
    c->filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    c->has_subscriber_priority = true;
    c->subscriber_priority = 9;
}

/* A subscription the client requested and the server accepted. */
static bool established(moq_simpair_t *sp, moq_subscription_t *out)
{
    moq_session_t *c = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    drain(c); drain(sv);
    moq_subscribe_cfg_t sc;
    subscribe_cfg(&sc);
    if (moq_session_subscribe(c, &sc, 1, out) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 64, NULL);
    moq_event_t ev;
    if (!take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    moq_subscription_t sh = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(sv, sh, &ac, 2) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 64, NULL);
    drain(c); drain(sv);
    return true;
}

/* A publication the server published and the client accepted. */
static bool accepted_publication(moq_simpair_t *sp, moq_publication_t *out)
{
    moq_session_t *c = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    drain(c); drain(sv);
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
    pc.track_name = MOQ_BYTES_LITERAL("v");
    moq_publication_t spub;
    if (moq_session_publish(sv, &pc, 1, &spub) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 64, NULL);
    moq_event_t ev;
    if (!take(c, MOQ_EVENT_PUBLISH_REQUEST, &ev)) return false;
    *out = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    if (moq_session_accept_publish(c, *out, &ac, 2) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 64, NULL);
    drain(c); drain(sv);
    return true;
}

/* -- Session configuration ------------------------------------------------------ */

static int test_session_cfg(void)
{
    int failures = 0;
    const size_t end = FIELD_END(moq_session_cfg_t,
                                 max_namespace_suffixes_per_subscription);
    const size_t old = SIZEOF_THROUGH(moq_session_cfg_t,
                                      max_namespace_suffixes_per_subscription);
    for (int view = 0; view < 2; view++) {
        tally_t ref, got;
        tally_init(&ref);
        tally_init(&got);
        moq_session_cfg_t cur;
        moq_session_cfg_init_sized(&cur, sizeof(cur), &ref.vt, MOQ_PERSPECTIVE_CLIENT);
        cur.version = MOQ_VERSION_DRAFT_22;
        moq_session_t *a = NULL, *b = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cur, 0, &a) == MOQ_OK);

        moq_session_cfg_t oc = cur;
        oc.alloc = &got.vt;
        moq_result_t rc;
        if (view == 0) {
            void *blk = old_block(&oc, end, old);
            MOQ_TEST_CHECK(blk != NULL);
            rc = moq_session_create(blk, 0, &b);
            free(blk);
        } else {
            old_inplace(&oc, sizeof(oc), end, old);
            rc = moq_session_create(&oc, 0, &b);
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (a && b) {
            MOQ_TEST_CHECK_EQ_SIZE(got.allocs, ref.allocs);
            MOQ_TEST_CHECK_EQ_SIZE(got.bytes, ref.bytes);
            MOQ_TEST_CHECK_EQ_SIZE(b->fill_cap, a->fill_cap);
            MOQ_TEST_CHECK_EQ_SIZE(a->fill_cap, (size_t)MOQ_DEFAULT_MAX_FILLS);
            MOQ_TEST_CHECK(!b->defer_subscription_updates);
            MOQ_TEST_CHECK(!b->defer_fetch_ranges);
        }
        if (a) moq_session_destroy(a);
        if (b) moq_session_destroy(b);
        MOQ_TEST_CHECK(ref.live == 0 && got.live == 0);
    }
    return failures;
}

/* A pair config from a caller built against the layout that ended at
 * client_defer_subscription_updates: its poisoned tail padding (where the
 * reserved slot now sits) is never read, and the client keeps the default
 * receive-stream pool. */
static int test_simpair_cfg(void)
{
    int failures = 0;
    const size_t end = FIELD_END(moq_simpair_cfg_t,
                                 client_defer_subscription_updates);
    const size_t old = SIZEOF_THROUGH(moq_simpair_cfg_t,
                                      client_defer_subscription_updates);
    MOQ_TEST_CHECK(offsetof(moq_simpair_cfg_t, client_max_data_streams) >= old);
    for (int view = 0; view < 2; view++) {
        moq_simpair_cfg_t cur = MOQ_SIMPAIR_CFG_INIT;
        cur.alloc = moq_alloc_default();
        cur.version = MOQ_VERSION_DRAFT_22;
        moq_simpair_t *sp = NULL;
        moq_result_t rc;
        if (view == 0) {
            void *blk = old_block(&cur, end, old);
            MOQ_TEST_CHECK(blk != NULL);
            rc = moq_simpair_create(blk, &sp);
            free(blk);
        } else {
            old_inplace(&cur, sizeof(cur), end, old);
            rc = moq_simpair_create(&cur, &sp);
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (sp) {
            MOQ_TEST_CHECK_EQ_SIZE(moq_simpair_client(sp)->rx_cap,
                                   (size_t)MOQ_DEFAULT_MAX_DATA_STREAMS);
            moq_simpair_destroy(sp);
        }
    }
    /* A current caller's field is honoured. */
    moq_simpair_cfg_t cur = MOQ_SIMPAIR_CFG_INIT;
    cur.alloc = moq_alloc_default();
    cur.client_max_data_streams = 2;
    moq_simpair_t *sp = NULL;
    MOQ_TEST_CHECK(moq_simpair_create(&cur, &sp) == MOQ_OK);
    if (sp) {
        MOQ_TEST_CHECK_EQ_SIZE(moq_simpair_client(sp)->rx_cap, (size_t)2);
        MOQ_TEST_CHECK_EQ_SIZE(moq_simpair_server(sp)->rx_cap,
                               (size_t)MOQ_DEFAULT_MAX_DATA_STREAMS);
        moq_simpair_destroy(sp);
    }
    return failures;
}

/* -- Exported initializers --------------------------------------------------------- */

/* `init` called through its symbol on an old caller's storage: it stamps the
 * old sizeof and leaves every byte past it untouched. */
static int check_init_symbol(void (*init)(void *), size_t old, const char *name)
{
    int failures = 0;
    unsigned char *p = malloc(old + GUARD);
    if (!p) { MOQ_TEST_CHECK(p != NULL); return failures; }
    memset(p, POISON, old + GUARD);
    init(p);
    uint32_t ss;
    memcpy(&ss, p, sizeof(ss));
    if (ss != old) {
        fprintf(stderr, "FAIL: %s stamped %u, the old sizeof is %zu\n", name,
                (unsigned)ss, old);
        failures++;
    }
    for (size_t i = old; i < old + GUARD; i++) {
        if (p[i] != POISON) {
            fprintf(stderr, "FAIL: %s wrote byte %zu past the old sizeof %zu\n",
                    name, i, old);
            failures++;
            break;
        }
    }
    free(p);
    /* Exactly the old size: a write past it is out of bounds under ASan. */
    p = malloc(old);
    if (p) { init(p); free(p); }
    return failures;
}

/* Parenthesized: the symbol itself, as a binary built against the old
 * header calls it. */
static void init_subscribe(void *p) { (moq_subscribe_cfg_init)(p); }
static void init_pub_update(void *p) { (moq_publication_update_cfg_init)(p); }
static void init_sub_track(void *p) { (moq_sub_track_cfg_init)(p); }

static int test_init_symbols(void)
{
    int failures = 0;
    failures += check_init_symbol(init_subscribe,
        SIZEOF_THROUGH(moq_subscribe_cfg_t, new_group_request),
        "moq_subscribe_cfg_init");
    failures += check_init_symbol(init_pub_update,
        SIZEOF_THROUGH(moq_publication_update_cfg_t, new_group_request),
        "moq_publication_update_cfg_init");
    failures += check_init_symbol(init_sub_track,
        SIZEOF_THROUGH(moq_sub_track_cfg_t, auth_token_count),
        "moq_sub_track_cfg_init");

    /* The sized initializers write min(size, current) and stamp it. */
    {
        const size_t old = SIZEOF_THROUGH(moq_subscribe_cfg_t, new_group_request);
        unsigned char buf[sizeof(moq_subscribe_cfg_t) + GUARD];
        memset(buf, POISON, sizeof(buf));
        moq_subscribe_cfg_init_sized((moq_subscribe_cfg_t *)(void *)buf, old);
        MOQ_TEST_CHECK(((moq_subscribe_cfg_t *)(void *)buf)->struct_size == old);
        MOQ_TEST_CHECK(buf[old] == POISON);
        memset(buf, POISON, sizeof(buf));
        moq_subscribe_cfg_init_sized((moq_subscribe_cfg_t *)(void *)buf, sizeof(buf));
        MOQ_TEST_CHECK(((moq_subscribe_cfg_t *)(void *)buf)->struct_size ==
                       sizeof(moq_subscribe_cfg_t));
        MOQ_TEST_CHECK(buf[sizeof(moq_subscribe_cfg_t)] == POISON);
        MOQ_TEST_CHECK(((moq_subscribe_cfg_t *)(void *)buf)->fill == NULL);
    }
    {
        const size_t old = SIZEOF_THROUGH(moq_publication_update_cfg_t, new_group_request);
        unsigned char buf[sizeof(moq_publication_update_cfg_t) + GUARD];
        memset(buf, POISON, sizeof(buf));
        moq_publication_update_cfg_init_sized((moq_publication_update_cfg_t *)(void *)buf, old);
        MOQ_TEST_CHECK(((moq_publication_update_cfg_t *)(void *)buf)->struct_size == old);
        MOQ_TEST_CHECK(buf[old] == POISON);
    }
    {
        const size_t old = SIZEOF_THROUGH(moq_sub_track_cfg_t, auth_token_count);
        unsigned char buf[sizeof(moq_sub_track_cfg_t) + GUARD];
        memset(buf, POISON, sizeof(buf));
        moq_sub_track_cfg_init_sized((moq_sub_track_cfg_t *)(void *)buf, old);
        MOQ_TEST_CHECK(((moq_sub_track_cfg_t *)(void *)buf)->struct_size == old);
        MOQ_TEST_CHECK(((moq_sub_track_cfg_t *)(void *)buf)->filter ==
                       MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
        MOQ_TEST_CHECK(buf[old] == POISON);
    }

    /* A caller built against this header gets the whole struct. */
    {
        moq_subscribe_cfg_t a;
        moq_publication_update_cfg_t b;
        moq_sub_track_cfg_t c;
        memset(&a, POISON, sizeof(a));
        memset(&b, POISON, sizeof(b));
        memset(&c, POISON, sizeof(c));
        moq_subscribe_cfg_init(&a);
        moq_publication_update_cfg_init(&b);
        moq_sub_track_cfg_init(&c);
        MOQ_TEST_CHECK(a.struct_size == sizeof(a) && a.location == NULL && a.fill == NULL);
        MOQ_TEST_CHECK(b.struct_size == sizeof(b) && b.fill == NULL);
        MOQ_TEST_CHECK(c.struct_size == sizeof(c) && c.location == NULL && c.fill == NULL);
        MOQ_TEST_CHECK(c.filter == MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
    }
    return failures;
}

/* -- Request configs ------------------------------------------------------------ */

/* SUBSCRIBE from the current struct (location and fill NULL) and from the
 * old caller's views: the same request on the wire. */
static int test_subscribe_cfg(void)
{
    int failures = 0;
    const size_t end = FIELD_END(moq_subscribe_cfg_t, new_group_request);
    const size_t old = SIZEOF_THROUGH(moq_subscribe_cfg_t, new_group_request);
    digest_t want, got;
    moq_simpair_t *sp = mk_pair();
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return failures;
    moq_subscribe_cfg_t cur;
    subscribe_cfg(&cur);
    moq_subscription_t h;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &cur, 1, &h) == MOQ_OK);
    digest_actions(moq_simpair_client(sp), &want);
    moq_simpair_destroy(sp);

    for (int view = 0; view < 2; view++) {
        sp = mk_pair();
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) continue;
        moq_subscribe_cfg_t oc;
        subscribe_cfg(&oc);
        moq_result_t rc;
        if (view == 0) {
            void *blk = old_block(&oc, end, old);
            rc = moq_session_subscribe(moq_simpair_client(sp), blk, 1, &h);
            free(blk);
        } else {
            old_inplace(&oc, sizeof(oc), end, old);
            rc = moq_session_subscribe(moq_simpair_client(sp), &oc, 1, &h);
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        digest_actions(moq_simpair_client(sp), &got);
        MOQ_TEST_CHECK(digest_same(&want, &got));
        moq_simpair_destroy(sp);
    }
    return failures;
}

/* A subscription update from the current struct (fill NULL) and from the old
 * caller's views. */
static int test_subscription_update_cfg(void)
{
    int failures = 0;
    const size_t end = FIELD_END(moq_subscription_update_cfg_t, end_group);
    const size_t old = SIZEOF_THROUGH(moq_subscription_update_cfg_t, end_group);
    digest_t want, got;
    for (int view = -1; view < 2; view++) {
        moq_simpair_t *sp = mk_pair();
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) continue;
        moq_subscription_t h;
        MOQ_TEST_CHECK(established(sp, &h));
        moq_subscription_update_cfg_t uc;
        moq_subscription_update_cfg_init_sized(&uc, sizeof(uc));
        uc.has_subscriber_priority = true;
        uc.subscriber_priority = 3;
        uc.has_forward = true;
        uc.forward = false;
        moq_session_t *c = moq_simpair_client(sp);
        moq_result_t rc;
        if (view < 0) {
            rc = moq_session_update_subscription(c, h, &uc, 3);
        } else if (view == 0) {
            void *blk = old_block(&uc, end, old);
            rc = moq_session_update_subscription(c, h, blk, 3);
            free(blk);
        } else {
            old_inplace(&uc, sizeof(uc), end, old);
            rc = moq_session_update_subscription(c, h, &uc, 3);
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        digest_actions(c, view < 0 ? &want : &got);
        if (view >= 0) MOQ_TEST_CHECK(digest_same(&want, &got));
        moq_simpair_destroy(sp);
    }
    return failures;
}

/* A publication update (the subscriber's REQUEST_UPDATE on a PUBLISH it
 * accepted) from the current struct (fill NULL) and from the old caller's
 * views. */
static int test_publication_update_cfg(void)
{
    int failures = 0;
    const size_t end = FIELD_END(moq_publication_update_cfg_t, new_group_request);
    const size_t old = SIZEOF_THROUGH(moq_publication_update_cfg_t, new_group_request);
    digest_t want, got;
    for (int view = -1; view < 2; view++) {
        moq_simpair_t *sp = mk_pair();
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) continue;
        moq_publication_t pub;
        MOQ_TEST_CHECK(accepted_publication(sp, &pub));
        moq_publication_update_cfg_t uc;
        memset(&uc, 0, sizeof(uc));
        uc.struct_size = sizeof(uc);
        uc.has_subscriber_priority = true;
        uc.subscriber_priority = 4;
        moq_session_t *c = moq_simpair_client(sp);
        moq_result_t rc;
        if (view < 0) {
            rc = moq_session_update_publication(c, pub, &uc, 3);
        } else if (view == 0) {
            void *blk = old_block(&uc, end, old);
            rc = moq_session_update_publication(c, pub, blk, 3);
            free(blk);
        } else {
            old_inplace(&uc, sizeof(uc), end, old);
            rc = moq_session_update_publication(c, pub, &uc, 3);
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        digest_actions(c, view < 0 ? &want : &got);
        if (view >= 0) MOQ_TEST_CHECK(digest_same(&want, &got));
        moq_simpair_destroy(sp);
    }
    return failures;
}

/* -- Facades ----------------------------------------------------------------------- */

/* moq_sub_create from the current struct (fill caps zero: the defaults) and
 * from the old caller's views: the same reservations. Then a track subscribe
 * and an update from each config's current and old views put the same
 * requests on the wire. */
static int test_subscriber_cfgs(void)
{
    int failures = 0;
    const size_t cend = FIELD_END(moq_sub_cfg_t, on_update_ok);
    const size_t cold = SIZEOF_THROUGH(moq_sub_cfg_t, on_update_ok);
    const size_t tend = FIELD_END(moq_sub_track_cfg_t, auth_token_count);
    const size_t told = SIZEOF_THROUGH(moq_sub_track_cfg_t, auth_token_count);
    const size_t uend = FIELD_END(moq_sub_update_cfg_t, end_group);
    const size_t uold = SIZEOF_THROUGH(moq_sub_update_cfg_t, end_group);
    tally_t ref = {0};
    digest_t want_sub, want_upd, got;
    for (int view = -1; view < 2; view++) {
        moq_simpair_t *sp = mk_pair();
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) continue;
        moq_session_t *c = moq_simpair_client(sp);
        tally_t t;
        tally_init(&t);
        moq_sub_cfg_t sc;
        moq_sub_cfg_init_sized(&sc, sizeof(sc));
        sc.max_tracks = 4;
        moq_subscriber_t *sub = NULL;
        moq_result_t rc;
        void *blk = NULL;
        if (view < 0) {
            rc = moq_sub_create(c, &t.vt, &sc, &sub);
        } else if (view == 0) {
            blk = old_block(&sc, cend, cold);
            rc = moq_sub_create(c, &t.vt, blk, &sub);
        } else {
            old_inplace(&sc, sizeof(sc), cend, cold);
            rc = moq_sub_create(c, &t.vt, &sc, &sub);
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (view < 0) {
            ref = t;
        } else {
            MOQ_TEST_CHECK_EQ_SIZE(t.allocs, ref.allocs);
            MOQ_TEST_CHECK_EQ_SIZE(t.bytes, ref.bytes);
        }
        if (!sub) { free(blk); moq_simpair_destroy(sp); continue; }

        moq_sub_track_cfg_t tc;
        memset(&tc, 0, sizeof(tc));
        tc.struct_size = sizeof(tc);
        tc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        tc.track_name = MOQ_BYTES_LITERAL("v");
        tc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_sub_track_t *track = NULL;
        void *tblk = NULL;
        if (view < 0) {
            rc = moq_sub_subscribe(sub, &tc, 1, &track);
        } else if (view == 0) {
            tblk = old_block(&tc, tend, told);
            rc = moq_sub_subscribe(sub, tblk, 1, &track);
        } else {
            old_inplace(&tc, sizeof(tc), tend, told);
            rc = moq_sub_subscribe(sub, &tc, 1, &track);
        }
        free(tblk);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        digest_actions(c, view < 0 ? &want_sub : &got);
        if (view >= 0) MOQ_TEST_CHECK(digest_same(&want_sub, &got));

        /* Re-run the request through the pair so the track establishes. */
        moq_simpair_destroy(sp);
        moq_sub_destroy(sub);
        free(blk);
        MOQ_TEST_CHECK(t.live == 0);

        sp = mk_pair();
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) continue;
        c = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        moq_sub_cfg_init_sized(&sc, sizeof(sc));
        sc.max_tracks = 4;
        sub = NULL;
        MOQ_TEST_CHECK(moq_sub_create(c, &t.vt, &sc, &sub) == MOQ_OK);
        if (!sub) { moq_simpair_destroy(sp); continue; }
        memset(&tc, 0, sizeof(tc));
        tc.struct_size = sizeof(tc);
        tc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        tc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        tc.track_name = MOQ_BYTES_LITERAL("v");
        track = NULL;
        MOQ_TEST_CHECK(moq_sub_subscribe(sub, &tc, 1, &track) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 64, NULL);
        moq_event_t ev;
        MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
        moq_subscription_t sh = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, sh, &ac, 2) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 64, NULL);
        MOQ_TEST_CHECK(moq_sub_tick(sub, 2) >= 0);

        moq_sub_update_cfg_t uc;
        moq_sub_update_cfg_init_sized(&uc, sizeof(uc));
        uc.has_subscriber_priority = true;
        uc.subscriber_priority = 5;
        void *ublk = NULL;
        if (view < 0) {
            rc = moq_sub_update_subscription(sub, track, &uc, 3);
        } else if (view == 0) {
            ublk = old_block(&uc, uend, uold);
            rc = moq_sub_update_subscription(sub, track, ublk, 3);
        } else {
            old_inplace(&uc, sizeof(uc), uend, uold);
            rc = moq_sub_update_subscription(sub, track, &uc, 3);
        }
        free(ublk);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        digest_actions(c, view < 0 ? &want_upd : &got);
        if (view >= 0) MOQ_TEST_CHECK(digest_same(&want_upd, &got));
        moq_sub_destroy(sub);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(t.live == 0);
    }
    return failures;
}

/* moq_pub_create from the current struct (max_fills zero: the default) and
 * from the old caller's views: the same reservations. */
static int test_publisher_cfg(void)
{
    int failures = 0;
    const size_t end = FIELD_END(test_pub_cfg_callbacks_t, callbacks);
    const size_t old = sizeof(test_pub_cfg_callbacks_t);
    tally_t ref = {0};
    for (int view = -1; view < 2; view++) {
        moq_simpair_t *sp = mk_pair();
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) continue;
        moq_session_t *sv = moq_simpair_server(sp);
        MOQ_TEST_CHECK(moq_session_supports_fill(sv));
        tally_t t;
        tally_init(&t);
        moq_pub_cfg_t pc;
        moq_pub_cfg_init_sized(&pc, sizeof(pc));
        moq_publisher_t *pub = NULL;
        moq_result_t rc;
        if (view < 0) {
            rc = moq_pub_create(sv, &t.vt, &pc, &pub);
        } else if (view == 0) {
            void *blk = old_block(&pc, end, old);
            rc = moq_pub_create(sv, &t.vt, blk, &pub);
            free(blk);
        } else {
            old_inplace(&pc, sizeof(pc), end, old);
            rc = moq_pub_create(sv, &t.vt, &pc, &pub);
        }
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        if (view < 0) {
            ref = t;
        } else {
            MOQ_TEST_CHECK_EQ_SIZE(t.allocs, ref.allocs);
            MOQ_TEST_CHECK_EQ_SIZE(t.bytes, ref.bytes);
        }
        if (pub) moq_pub_destroy(pub);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(t.live == 0);
    }
    return failures;
}

/* The subscriber snapshot for a caller whose struct ends at `closed`: the
 * library writes exactly that many bytes and stamps that size. */
static int test_sub_stats(void)
{
    int failures = 0;
    const size_t old = SIZEOF_THROUGH(moq_sub_stats_t, closed);
    moq_simpair_t *sp = mk_pair();
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return failures;
    moq_subscriber_t *sub = NULL;
    moq_sub_cfg_t sc;
    moq_sub_cfg_init_sized(&sc, sizeof(sc));
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(sp), moq_alloc_default(), &sc,
                                  &sub) == MOQ_OK);
    if (sub) {
        unsigned char buf[sizeof(moq_sub_stats_t) + GUARD];
        memset(buf, POISON, sizeof(buf));
        uint32_t ss = (uint32_t)old;
        memcpy(buf, &ss, sizeof(ss));
        MOQ_TEST_CHECK(moq_sub_get_stats(sub, (moq_sub_stats_t *)(void *)buf, old) == MOQ_OK);
        memcpy(&ss, buf, sizeof(ss));
        MOQ_TEST_CHECK_EQ_SIZE((size_t)ss, old);
        for (size_t i = old; i < sizeof(buf); i++) {
            if (buf[i] != POISON) {
                fprintf(stderr, "FAIL: stats wrote byte %zu past %zu\n", i, old);
                failures++;
                break;
            }
        }
        unsigned char *blk = malloc(old);
        if (blk) {
            MOQ_TEST_CHECK(moq_sub_get_stats(sub, (moq_sub_stats_t *)(void *)blk, old) == MOQ_OK);
            free(blk);
        }
        moq_sub_destroy(sub);
    }
    moq_simpair_destroy(sp);
    return failures;
}

/* -- Draft-22 configs and outputs with a frozen v0 layout -------------------------- */

/* The server's handle on a subscription the client requested and the
 * server accepted. */
static bool established_server(moq_simpair_t *sp, moq_subscription_t *out,
                               moq_subscription_t *client_out)
{
    moq_session_t *c = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    drain(c); drain(sv);
    moq_subscribe_cfg_t sc;
    subscribe_cfg(&sc);
    moq_subscription_t ch;
    if (moq_session_subscribe(c, &sc, 1, &ch) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 64, NULL);
    moq_event_t ev;
    if (!take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    *out = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(sv, *out, &ac, 2) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 64, NULL);
    drain(c); drain(sv);
    *client_out = ch;
    return true;
}

static void init_publish_state(void *p) { (moq_publish_state_cfg_init)(p); }
static void init_accept_update(void *p) { (moq_accept_subscription_update_cfg_init)(p); }
static void init_reject_update(void *p) { (moq_reject_subscription_update_cfg_init)(p); }

/* Each symbol stamps its v0 layout and writes nothing past it; each sized
 * initializer writes min(size, current) and stamps it; the macro reaches the
 * whole struct. The v0 layouts are the structs' current sizeof. */
static int test_v0_cfg_inits(void)
{
    int failures = 0;
    MOQ_TEST_CHECK_EQ_SIZE(MOQ_PUBLISH_STATE_CFG_V0_SIZE, sizeof(moq_publish_state_cfg_t));
    MOQ_TEST_CHECK_EQ_SIZE(MOQ_ACCEPT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE,
                           sizeof(moq_accept_subscription_update_cfg_t));
    MOQ_TEST_CHECK_EQ_SIZE(MOQ_REJECT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE,
                           sizeof(moq_reject_subscription_update_cfg_t));
    failures += check_init_symbol(init_publish_state, MOQ_PUBLISH_STATE_CFG_V0_SIZE,
                                  "moq_publish_state_cfg_init");
    failures += check_init_symbol(init_accept_update,
                                  MOQ_ACCEPT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE,
                                  "moq_accept_subscription_update_cfg_init");
    failures += check_init_symbol(init_reject_update,
                                  MOQ_REJECT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE,
                                  "moq_reject_subscription_update_cfg_init");

    unsigned char buf[sizeof(moq_reject_subscription_update_cfg_t) + GUARD];
    /* A size that holds only struct_size: stamped, nothing else written. */
    memset(buf, POISON, sizeof(buf));
    moq_publish_state_cfg_init_sized((moq_publish_state_cfg_t *)(void *)buf, 4);
    MOQ_TEST_CHECK(((moq_publish_state_cfg_t *)(void *)buf)->struct_size == 4);
    MOQ_TEST_CHECK(buf[4] == POISON);
    /* Smaller than struct_size: untouched. */
    memset(buf, POISON, sizeof(buf));
    moq_accept_subscription_update_cfg_init_sized(
        (moq_accept_subscription_update_cfg_t *)(void *)buf, 3);
    MOQ_TEST_CHECK(buf[0] == POISON);
    /* A larger size: the current struct, nothing past it. */
    memset(buf, POISON, sizeof(buf));
    moq_reject_subscription_update_cfg_init_sized(
        (moq_reject_subscription_update_cfg_t *)(void *)buf, sizeof(buf));
    MOQ_TEST_CHECK(((moq_reject_subscription_update_cfg_t *)(void *)buf)->struct_size ==
                   sizeof(moq_reject_subscription_update_cfg_t));
    MOQ_TEST_CHECK(buf[sizeof(moq_reject_subscription_update_cfg_t)] == POISON);

    moq_publish_state_cfg_t a;
    moq_accept_subscription_update_cfg_t b;
    moq_reject_subscription_update_cfg_t c;
    memset(&a, POISON, sizeof(a));
    memset(&b, POISON, sizeof(b));
    memset(&c, POISON, sizeof(c));
    moq_publish_state_cfg_init(&a);
    moq_accept_subscription_update_cfg_init(&b);
    moq_reject_subscription_update_cfg_init(&c);
    MOQ_TEST_CHECK(a.struct_size == sizeof(a) && !a.has_forward && a.filter == NULL);
    MOQ_TEST_CHECK(b.struct_size == sizeof(b) && b.update_request_id == 0);
    MOQ_TEST_CHECK(c.struct_size == sizeof(c) && c.reason.len == 0);
    return failures;
}

/* Each consuming call accepts a struct of exactly its v0 size (an exact
 * heap block: a read past it is out of bounds under ASan) and refuses one
 * byte less before looking at anything else. */
static int test_v0_cfg_floors(void)
{
    int failures = 0;
    moq_simpair_t *sp = mk_pair();
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return failures;
    moq_session_t *c = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    moq_subscription_t ssub, csub;
    bool ready = established_server(sp, &ssub, &csub);
    MOQ_TEST_CHECK(ready);
    if (!ready) { moq_simpair_destroy(sp); return failures; }

    /* A notification observes a filter authorized by the subscriber's ACKed
     * UPDATE; the ABI floor must not depend on an unauthorized state change. */
    moq_location_filter_cfg_t nf;
    moq_location_filter_cfg_init(&nf);
    nf.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    nf.start_group = 7;
    moq_subscription_update_cfg_t update;
    moq_subscription_update_cfg_init_sized(&update, sizeof(update));
    update.location = &nf;
    moq_result_t rc = moq_session_update_subscription(c, csub, &update, 3);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) { moq_simpair_destroy(sp); return failures; }
    rc = moq_simpair_run_until_quiescent(sp, 64, NULL);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_event_t ev;
    ready = take(c, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev);
    MOQ_TEST_CHECK(ready);
    if (!ready) { moq_simpair_destroy(sp); return failures; }
    MOQ_TEST_CHECK(moq_subscription_eq(ev.u.subscription_update_ok.sub, csub));
    moq_event_cleanup(&ev);
    drain(c); drain(sv);
    moq_publish_state_cfg_t ps;
    moq_publish_state_cfg_init(&ps);
    ps.filter = &nf;
    const size_t ps_v0 = MOQ_PUBLISH_STATE_CFG_V0_SIZE;
    void *blk = old_block(&ps, ps_v0, ps_v0);
    MOQ_TEST_CHECK(blk != NULL);
    if (blk) {
        uint32_t under = (uint32_t)ps_v0 - 1;
        memcpy(blk, &under, sizeof(under));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_notify_subscription_state(sv, ssub, blk, 3),
                              (int)MOQ_ERR_INVAL);
        uint32_t exact = (uint32_t)ps_v0;
        memcpy(blk, &exact, sizeof(exact));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_notify_subscription_state(sv, ssub, blk, 3),
                              (int)MOQ_OK);
        free(blk);
    }
    moq_simpair_run_until_quiescent(sp, 64, NULL);
    ready = take(c, MOQ_EVENT_PUBLISH_STATE, &ev);
    MOQ_TEST_CHECK(ready);
    if (ready) {
        MOQ_TEST_CHECK(ev.u.publish_state.has_filter && !ev.u.publish_state.has_forward);
        MOQ_TEST_CHECK(ev.u.publish_state.location != NULL);
        if (ev.u.publish_state.location) {
            MOQ_TEST_CHECK(ev.u.publish_state.location->kind ==
                           MOQ_LOCATION_FILTER_ABSOLUTE_START);
            MOQ_TEST_CHECK(ev.u.publish_state.location->start_group == 7);
        }
        moq_event_cleanup(&ev);
    }
    /* Neither a Forward change nor a filter: nothing to notify. */
    moq_publish_state_cfg_init(&ps);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_notify_subscription_state(sv, ssub, &ps, 3),
                          (int)MOQ_ERR_INVAL);

    /* The update answers: the size gate comes before the owed-answer check,
     * so the exact v0 block reaches that check (no update is owed here). */
    moq_accept_subscription_update_cfg_t au;
    moq_accept_subscription_update_cfg_init(&au);
    const size_t au_v0 = MOQ_ACCEPT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE;
    blk = old_block(&au, au_v0, au_v0);
    if (blk) {
        uint32_t under = (uint32_t)au_v0 - 1;
        memcpy(blk, &under, sizeof(under));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscription_update(sv, ssub, blk, 3),
                              (int)MOQ_ERR_INVAL);
        uint32_t exact = (uint32_t)au_v0;
        memcpy(blk, &exact, sizeof(exact));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscription_update(sv, ssub, blk, 3),
                              (int)MOQ_ERR_WRONG_STATE);
        free(blk);
    }
    moq_reject_subscription_update_cfg_t ru;
    moq_reject_subscription_update_cfg_init(&ru);
    ru.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
    const size_t ru_v0 = MOQ_REJECT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE;
    blk = old_block(&ru, ru_v0, ru_v0);
    if (blk) {
        uint32_t under = (uint32_t)ru_v0 - 1;
        memcpy(blk, &under, sizeof(under));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscription_update(sv, ssub, blk, 3),
                              (int)MOQ_ERR_INVAL);
        uint32_t exact = (uint32_t)ru_v0;
        memcpy(blk, &exact, sizeof(exact));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscription_update(sv, ssub, blk, 3),
                              (int)MOQ_ERR_WRONG_STATE);
        free(blk);
    }
    moq_simpair_destroy(sp);
    return failures;
}

/* Count the bytes of `buf` past `from` that are no longer POISON. */
static size_t written_past(const unsigned char *buf, size_t from, size_t len)
{
    size_t n = 0;
    for (size_t i = from; i < len; i++)
        if (buf[i] != POISON) n++;
    return n;
}

typedef moq_result_t (*filter_query_fn)(moq_simpair_t *sp, void *out, size_t out_size,
                                        bool through_symbol);

static moq_subscription_t g_query_sub;
static moq_publication_t  g_query_pub;

static moq_result_t query_sub_filter(moq_simpair_t *sp, void *out, size_t out_size,
                                     bool through_symbol)
{
    moq_session_t *c = moq_simpair_client(sp);
    if (through_symbol)
        return (moq_session_subscription_filter)(c, g_query_sub, out);
    return moq_session_subscription_filter_sized(c, g_query_sub, out, out_size);
}

static moq_result_t query_pub_filter(moq_simpair_t *sp, void *out, size_t out_size,
                                     bool through_symbol)
{
    moq_session_t *c = moq_simpair_client(sp);
    if (through_symbol)
        return (moq_session_publication_filter)(c, g_query_pub, out);
    return moq_session_publication_filter_sized(c, g_query_pub, out, out_size);
}

/* One filter query, every way a caller can present its output. */
static int check_filter_query(moq_simpair_t *sp, filter_query_fn q, uint32_t want_kind,
                              const char *name)
{
    int failures = 0;
    const size_t v0 = MOQ_LOCATION_FILTER_CFG_V0_SIZE;
    unsigned char buf[sizeof(moq_location_filter_cfg_t) + GUARD];
    moq_location_filter_cfg_t lf;

    /* The symbol: the v0 layout, stamped, nothing past it. */
    memset(buf, POISON, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)q(sp, buf, 0, true), (int)MOQ_OK);
    memcpy(&lf, buf, v0);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)lf.struct_size, v0);
    MOQ_TEST_CHECK_EQ_U64(lf.kind, want_kind);
    if (written_past(buf, v0, sizeof(buf)) != 0) {
        fprintf(stderr, "FAIL: %s symbol wrote past the v0 layout\n", name);
        failures++;
    }
    unsigned char *blk = malloc(v0);
    if (blk) {
        MOQ_TEST_CHECK_EQ_INT((int)q(sp, blk, 0, true), (int)MOQ_OK);
        free(blk);
    }

    /* Sized below v0: refused, nothing written. */
    memset(buf, POISON, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)q(sp, buf, v0 - 1, false), (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(written_past(buf, 0, sizeof(buf)), 0);

    /* Sized exactly v0, in an exact block and in larger storage. */
    blk = malloc(v0);
    if (blk) {
        MOQ_TEST_CHECK_EQ_INT((int)q(sp, blk, v0, false), (int)MOQ_OK);
        free(blk);
    }
    memset(buf, POISON, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)q(sp, buf, v0, false), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(written_past(buf, v0, sizeof(buf)), 0);

    /* Sized past the current struct: the current struct, nothing past it. */
    memset(buf, POISON, sizeof(buf));
    MOQ_TEST_CHECK_EQ_INT((int)q(sp, buf, sizeof(buf), false), (int)MOQ_OK);
    memcpy(&lf, buf, sizeof(lf));
    MOQ_TEST_CHECK_EQ_SIZE((size_t)lf.struct_size, sizeof(moq_location_filter_cfg_t));
    MOQ_TEST_CHECK_EQ_U64(lf.kind, want_kind);
    MOQ_TEST_CHECK_EQ_SIZE(written_past(buf, sizeof(lf), sizeof(buf)), 0);
    return failures;
}

static int test_filter_outputs(void)
{
    int failures = 0;
    moq_simpair_t *sp = mk_pair();
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return failures;
    MOQ_TEST_CHECK(established(sp, &g_query_sub));
    MOQ_TEST_CHECK(accepted_publication(sp, &g_query_pub));
    failures += check_filter_query(sp, query_sub_filter, MOQ_LOCATION_FILTER_NEXT_OBJECT,
                                   "moq_session_subscription_filter");
    failures += check_filter_query(sp, query_pub_filter, MOQ_LOCATION_FILTER_NONE,
                                   "moq_session_publication_filter");

    /* The macro passes the caller's sizeof. */
    moq_location_filter_cfg_t lf;
    memset(&lf, POISON, sizeof(lf));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_filter(moq_simpair_client(sp),
                              g_query_sub, &lf), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE((size_t)lf.struct_size, sizeof(lf));

    /* The owed-update query checks a non-NULL output's size before anything
     * else; with no output it reaches the owed-answer check. */
    unsigned char buf[sizeof(moq_location_filter_cfg_t) + GUARD];
    memset(buf, POISON, sizeof(buf));
    moq_session_t *c = moq_simpair_client(sp);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_owed_update_sized(c, g_query_sub,
                              NULL, NULL, (moq_location_filter_cfg_t *)(void *)buf,
                              MOQ_LOCATION_FILTER_CFG_V0_SIZE - 1),
                          (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_SIZE(written_past(buf, 0, sizeof(buf)), 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_owed_update_sized(c, g_query_sub,
                              NULL, NULL, NULL, 0),
                          (int)MOQ_ERR_WRONG_STATE);
    MOQ_TEST_CHECK_EQ_INT((int)(moq_session_subscription_owed_update)(c, g_query_sub,
                              NULL, NULL, NULL),
                          (int)MOQ_ERR_WRONG_STATE);
    moq_simpair_destroy(sp);
    return failures;
}

/* -- Initializers on historical layouts ---------------------------------------------- */

/* The historical records are the old layouts themselves: their sizeof is the
 * extent through the last old field, as the current struct reports it. */
_Static_assert(sizeof(test_subgroup_cfg_old_t) ==
               SIZEOF_THROUGH(moq_subgroup_cfg_t, end_of_group), "subgroup cfg");
_Static_assert(sizeof(test_object_cfg_old_t) ==
               SIZEOF_THROUGH(moq_object_cfg_t, properties), "object cfg");
_Static_assert(sizeof(test_track_status_cfg_old_t) ==
               SIZEOF_THROUGH(moq_track_status_cfg_t, auth_token_count), "track status cfg");
_Static_assert(sizeof(test_accept_track_status_cfg_old_t) ==
               SIZEOF_THROUGH(moq_accept_track_status_cfg_t, expires_ms),
               "accept track status cfg");

/* A caller built against the historical layout, with a guard right after
 * its struct: the exported symbol stamps the historical sizeof and leaves
 * the guard as the caller left it. */
#define OLD_CALLER(T) struct { T cfg; unsigned char guard[GUARD]; }
#define CHECK_OLD_CALLER(T, CUR_T, SYM, NAME)                                    \
    do {                                                                       \
        OLD_CALLER(T) w;                                                       \
        memset(&w, POISON, sizeof(w));                                         \
        (SYM)((CUR_T *)(void *)&w.cfg);                                        \
        if (w.cfg.struct_size != sizeof(T)) {                                  \
            fprintf(stderr, "FAIL: %s stamped %u, the caller's sizeof is %zu\n", \
                    NAME, (unsigned)w.cfg.struct_size, sizeof(T));             \
            failures++;                                                        \
        }                                                                      \
        size_t past_ = 0;                                                      \
        for (size_t i_ = 0; i_ < GUARD; i_++)                                  \
            if (w.guard[i_] != POISON) past_ = i_ + 1;                         \
        if (past_) {                                                           \
            fprintf(stderr, "FAIL: %s wrote %zu byte(s) past the caller's "    \
                    "struct\n", NAME, past_);                                  \
            failures++;                                                        \
        }                                                                      \
        failures += check_init_symbol(init_old_##SYM, sizeof(T), NAME);       \
    } while (0)

static void init_old_moq_subgroup_cfg_init(void *p) { (moq_subgroup_cfg_init)(p); }
static void init_old_moq_object_cfg_init(void *p) { (moq_object_cfg_init)(p); }
static void init_old_moq_track_status_cfg_init(void *p) { (moq_track_status_cfg_init)(p); }
static void init_old_moq_accept_track_status_cfg_init(void *p)
{
    (moq_accept_track_status_cfg_init)(p);
}

static int test_historical_init_extents(void)
{
    int failures = 0;
    CHECK_OLD_CALLER(test_subgroup_cfg_old_t, moq_subgroup_cfg_t,
                     moq_subgroup_cfg_init, "moq_subgroup_cfg_init");
    CHECK_OLD_CALLER(test_object_cfg_old_t, moq_object_cfg_t,
                     moq_object_cfg_init, "moq_object_cfg_init");
    CHECK_OLD_CALLER(test_track_status_cfg_old_t, moq_track_status_cfg_t,
                     moq_track_status_cfg_init, "moq_track_status_cfg_init");
    CHECK_OLD_CALLER(test_accept_track_status_cfg_old_t, moq_accept_track_status_cfg_t,
                     moq_accept_track_status_cfg_init, "moq_accept_track_status_cfg_init");

    /* The old caller's subgroup opens and carries its objects; with no
     * first_object in its layout the header's FIRST_OBJECT stays clear. */
    moq_simpair_t *sp = mk_pair();
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return failures;
    moq_session_t *c = moq_simpair_client(sp), *sv = moq_simpair_server(sp);
    moq_subscription_t ssub, csub;
    MOQ_TEST_CHECK(established_server(sp, &ssub, &csub));
    test_subgroup_cfg_old_t *old = malloc(sizeof(*old));
    MOQ_TEST_CHECK(old != NULL);
    if (old) {
        (moq_subgroup_cfg_init)((moq_subgroup_cfg_t *)(void *)old);
        old->group_id = 4;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(
                                  sv, ssub, (const moq_subgroup_cfg_t *)(const void *)old,
                                  3, &sg),
                              (int)MOQ_OK);
        free(old);
        uint8_t d = 7;
        moq_rcbuf_t *pl = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), &d, 1, &pl) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object(sv, sg, 0, pl, 3), (int)MOQ_OK);
        moq_rcbuf_decref(pl);
        moq_simpair_run_until_quiescent(sp, 64, NULL);
        moq_event_t ev;
        bool got = take(c, MOQ_EVENT_OBJECT_RECEIVED, &ev);
        MOQ_TEST_CHECK(got);
        if (got) {
            MOQ_TEST_CHECK_EQ_U64(ev.u.object_received.group_id, 4);
            MOQ_TEST_CHECK(!ev.u.object_received.first_object);
            moq_event_cleanup(&ev);
        }
    }
    moq_simpair_destroy(sp);

    /* A caller built against the current header reaches the whole struct
     * through the same-name macro. */
    moq_subgroup_cfg_t cur;
    memset(&cur, POISON, sizeof(cur));
    moq_subgroup_cfg_init(&cur);
    MOQ_TEST_CHECK_EQ_SIZE(cur.struct_size, sizeof(cur));
    MOQ_TEST_CHECK(!cur.first_object && !cur.end_of_group && cur.group_id == 0);
    unsigned char buf[sizeof(moq_subgroup_cfg_t) + GUARD];
    memset(buf, POISON, sizeof(buf));
    moq_subgroup_cfg_init_sized((moq_subgroup_cfg_t *)(void *)buf, sizeof(buf));
    MOQ_TEST_CHECK_EQ_SIZE(((moq_subgroup_cfg_t *)(void *)buf)->struct_size,
                           sizeof(moq_subgroup_cfg_t));
    MOQ_TEST_CHECK(buf[sizeof(moq_subgroup_cfg_t)] == POISON);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_session_cfg();
    failures += test_simpair_cfg();
    failures += test_init_symbols();
    failures += test_historical_init_extents();
    failures += test_subscribe_cfg();
    failures += test_subscription_update_cfg();
    failures += test_publication_update_cfg();
    failures += test_subscriber_cfgs();
    failures += test_publisher_cfg();
    failures += test_sub_stats();
    failures += test_v0_cfg_inits();
    failures += test_v0_cfg_floors();
    failures += test_filter_outputs();
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("PASS: abi_old_callers_d22\n");
    return 0;
}
