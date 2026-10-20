/*
 * Draft-16 subscription terminals for a request this side no longer holds.
 *
 * §5.1.1 lets a subscriber dispose of its subscription state once it sends
 * UNSUBSCRIBE and a publisher once it sends PUBLISH_DONE (also §9.15), and
 * §9.12 lets the subscriber UNSUBSCRIBE without having seen that DONE, so a
 * PUBLISH_DONE and an UNSUBSCRIBE can each name a subscription its receiver
 * already ended. This side's interoperability policy absorbs either, with no
 * effect, when its Request ID was committed earlier in its origin's sequence
 * (§9.1) and is not an outstanding obligation. Pinned here, for PUBLISH_DONE
 * and, where named, for UNSUBSCRIBE:
 *   - each origin's committed next ID bounds the policy: the exact next ID
 *     and anything past it close, and an uncommitted local request does not
 *     advance the bound;
 *   - a DONE whose bytes do not parse closes even for an absorbable ID;
 *   - a live registry owner keeps its family, role, state and duplicate
 *     checks, and a failed check never falls through to the policy;
 *   - a reply tombstone or an owed inbound update makes its exact ID strict,
 *     and DONE consumes neither;
 *   - an absorbed DONE queues no event or action and changes no owner, for a
 *     cancelled subscription of either origin, a rejected request, a retired
 *     request of another family, and a duplicate;
 *   - repeated cancels need no retained state for a DONE that never comes;
 *   - UNSUBSCRIBE after DONE, for our PUBLISH (this side's origin) and for a
 *     SUBSCRIBE we served (the peer's), with the same bound and obligations.
 */
#include <moq/moq.h>
#include "test_raw_pair_support.h"
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

#include <stdio.h>
#include <string.h>

static int failures = 0;

static moq_bytes_t g_ns_part[] = { { (const uint8_t *)"live", 4 } };
static moq_bytes_t g_track = { (const uint8_t *)"video", 5 };

/* -- Wire helpers ---------------------------------------------------------- */

static size_t done_bytes(uint8_t *buf, size_t cap, uint64_t id, uint64_t stream_count)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    static const uint8_t reason[] = { 'e', 'n', 'd' };
    if (moq_d16_encode_publish_done(&w, id, 0x2, stream_count, reason, sizeof(reason)) != MOQ_OK)
        return 0;
    return moq_buf_writer_offset(&w);
}

static void inject_done(moq_session_t *s, uint64_t id, uint64_t stream_count, uint64_t now)
{
    uint8_t buf[64];
    size_t n = done_bytes(buf, sizeof(buf), id, stream_count);
    MOQ_TEST_CHECK(n > 0);
    (void)moq_session_on_control_bytes(s, buf, n, now);
}

static size_t unsub_bytes(uint8_t *buf, size_t cap, uint64_t id)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    if (moq_d16_encode_varint_msg(&w, MOQ_D16_UNSUBSCRIBE, id) != MOQ_OK) return 0;
    return moq_buf_writer_offset(&w);
}

static void inject_unsub(moq_session_t *s, uint64_t id, uint64_t now)
{
    uint8_t buf[32];
    size_t n = unsub_bytes(buf, sizeof(buf), id);
    MOQ_TEST_CHECK(n > 0);
    (void)moq_session_on_control_bytes(s, buf, n, now);
}

/* A terminal of either kind. */
static void inject_terminal(moq_session_t *s, bool unsub, uint64_t id, uint64_t now)
{
    if (unsub) inject_unsub(s, id, now);
    else inject_done(s, id, 0, now);
}

static void inject(moq_session_t *s, const uint8_t *buf, size_t n, uint64_t now)
{
    MOQ_TEST_CHECK(n > 0);
    (void)moq_session_on_control_bytes(s, buf, n, now);
}

/* Queued events and actions, all dropped. */
static size_t queued(moq_session_t *s)
{
    size_t n = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) { n++; moq_event_cleanup(&ev); }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) { n++; moq_action_cleanup(&a); }
    return n;
}

/* The close code of a closed session (UINT64_MAX when it did not close). */
static uint64_t closed_code(moq_session_t *s)
{
    uint64_t code = UINT64_MAX;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) code = ev.u.closed.code;
        moq_event_cleanup(&ev);
    }
    return code;
}

#define CHECK_CLOSED_0X3(s) do { \
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_CLOSED); \
    MOQ_TEST_CHECK_EQ_U64(closed_code(s), 0x3); } while (0)

/* PUBLISH_DONE refusals close with PROTOCOL_VIOLATION, UNSUBSCRIBE refusals
 * with INVALID_REQUEST_ID. */
#define CHECK_CLOSED_0X3_OR_0X4(s, unsub) do { \
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_CLOSED); \
    MOQ_TEST_CHECK_EQ_U64(closed_code(s), (unsub) ? 0x4 : 0x3); } while (0)

/* Absorbed: still established, nothing queued, the owner counts unchanged. */
#define CHECK_ABSORBED(s, subs_before, pubs_before) do { \
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(s), (int)MOQ_SESS_ESTABLISHED); \
    MOQ_TEST_CHECK_EQ_SIZE(queued(s), 0); \
    MOQ_TEST_CHECK_EQ_SIZE(live_subs(s), (subs_before)); \
    MOQ_TEST_CHECK_EQ_SIZE(live_pubs(s), (pubs_before)); } while (0)

static size_t live_subs(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->sub_cap; i++) if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}

static size_t live_pubs(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->pub_cap; i++) if (s->publishes[i].state != MOQ_PUB_FREE) n++;
    return n;
}

/* -- Session helpers ------------------------------------------------------- */

static moq_subscribe_cfg_t sub_cfg_track(moq_bytes_t track)
{
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ g_ns_part, 1 };
    cfg.track_name = track;
    return cfg;
}

static moq_subscribe_cfg_t sub_cfg(void) { return sub_cfg_track(g_track); }

static uint64_t sub_request_id(moq_session_t *s, moq_subscription_t h)
{
    int slot = sub_resolve_handle(s, h);
    return slot < 0 ? UINT64_MAX : s->subs[slot].request_id;
}

/* The client subscribes to `track`; the server accepts. */
static bool established_track(moq_session_t *c, moq_session_t *sv, moq_bytes_t track,
                              moq_subscription_t *csub, moq_subscription_t *ssub)
{
    moq_subscribe_cfg_t cfg = sub_cfg_track(track);
    if (moq_session_subscribe(c, &cfg, 10, csub) != MOQ_OK) return false;
    pump(c, sv, false, 10, 0);
    moq_event_t ev;
    if (!take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    *ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(sv, *ssub, &ac, 10) != MOQ_OK) return false;
    run(c, sv, 10);
    drain(c); drain(sv);
    return true;
}

static bool established(moq_session_t *c, moq_session_t *sv, moq_subscription_t *csub,
                        moq_subscription_t *ssub)
{
    return established_track(c, sv, g_track, csub, ssub);
}

/* The server publishes; the client accepts. */
static bool published(moq_session_t *c, moq_session_t *sv, moq_publication_t *cpub,
                      moq_publication_t *spub)
{
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ g_ns_part, 1 };
    pc.track_name = g_track;
    if (moq_session_publish(sv, &pc, 10, spub) != MOQ_OK) return false;
    pump(sv, c, true, 10, 0);
    moq_event_t ev;
    if (!take(c, MOQ_EVENT_PUBLISH_REQUEST, &ev)) return false;
    *cpub = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    if (moq_session_accept_publish(c, *cpub, &ac, 10) != MOQ_OK) return false;
    run(c, sv, 10);
    drain(c); drain(sv);
    return true;
}

static void drop_actions(moq_session_t *s)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
}

/* -- Committed-ID bound ---------------------------------------------------- */

/* The client's own origin (even IDs) and the server's (odd), before anything
 * was committed: the exact next ID and the one after it both close. */
static void test_counters_uncommitted(void)
{
    const uint64_t ids[] = { 0, 2, 1, 3 };
    for (size_t i = 0; i < sizeof(ids) / sizeof(ids[0]); i++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
        inject_done(c, ids[i], 0, 20);
        CHECK_CLOSED_0X3(c);
        destroy(c, sv);
    }
}

/* Once ID 0 (client origin) is committed and its subscription cancelled, a
 * DONE for 0 is absorbed and one for 2, the next ID, still closes. */
static void test_counter_local_committed(void)
{
    for (int next = 0; next < 2; next++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
        moq_subscription_t csub, ssub;
        MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
        MOQ_TEST_CHECK_EQ_U64(sub_request_id(c, csub), 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, csub, 20), (int)MOQ_OK);
        drop_actions(c);
        if (!next) {
            inject_done(c, 0, 0, 30);
            CHECK_ABSORBED(c, 0, 0);
            inject_done(c, 0, 0, 31);              /* a duplicate, too */
            CHECK_ABSORBED(c, 0, 0);
        } else {
            inject_done(c, 2, 0, 30);
            CHECK_CLOSED_0X3(c);
        }
        destroy(c, sv);
    }
}

/* The server's origin: its PUBLISH took ID 1. Cancelled by the client, a
 * DONE for 1 is absorbed and one for 3 closes. */
static void test_counter_peer_committed(void)
{
    for (int next = 0; next < 2; next++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
        moq_publication_t cpub, spub;
        MOQ_TEST_CHECK(published(c, sv, &cpub, &spub));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_cancel_publication(c, cpub, 0, 20), (int)MOQ_OK);
        drop_actions(c);
        drain(c);
        inject_done(c, next ? 3 : 1, 0, 30);
        if (!next) CHECK_ABSORBED(c, 0, 0);
        else CHECK_CLOSED_0X3(c);
        destroy(c, sv);
    }
}

/* A subscribe refused before it committed (the action queue was full) does
 * not advance the bound: its would-be ID 0 still closes. */
static void test_uncommitted_local_request(void)
{
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_CONTROL;
    while (push_action(c, &a) == MOQ_OK) { }
    moq_subscribe_cfg_t cfg = sub_cfg();
    moq_subscription_t csub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(c, &cfg, 10, &csub),
                          (int)MOQ_ERR_WOULD_BLOCK);
    drop_actions(c);
    inject_done(c, 0, 0, 20);
    CHECK_CLOSED_0X3(c);
    destroy(c, sv);
}

/* A DONE that does not parse closes even for an ID the policy would absorb. */
static void test_malformed_obsolete(void)
{
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_subscription_t csub, ssub;
    MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, csub, 20), (int)MOQ_OK);
    drop_actions(c);
    uint8_t buf[64];
    size_t n = done_bytes(buf, sizeof(buf), 0, 0);
    MOQ_TEST_CHECK(n > 4 && buf[0] == 0x0b);
    /* Drop the reason's last byte and declare the shorter Length: the
     * reason length no longer fits the body. */
    uint16_t len = (uint16_t)(((unsigned)buf[1] << 8) | buf[2]);
    len--;
    buf[1] = (uint8_t)(len >> 8);
    buf[2] = (uint8_t)len;
    inject(c, buf, n - 1, 30);
    CHECK_CLOSED_0X3(c);
    destroy(c, sv);
}

/* -- Live owners keep their checks ----------------------------------------- */

static void test_live_refusals(void)
{
    moq_session_t *c, *sv;
    moq_subscription_t csub, ssub;

    /* A subscription this side publishes (publisher role). */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
    inject_done(sv, 0, 0, 20);
    CHECK_CLOSED_0X3(sv);
    destroy(c, sv);

    /* A PUBLISH this side sent (publisher role). */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_publication_t cpub, spub;
    MOQ_TEST_CHECK(published(c, sv, &cpub, &spub));
    inject_done(sv, 1, 0, 20);
    CHECK_CLOSED_0X3(sv);
    destroy(c, sv);

    /* A SUBSCRIBE still pending its first response. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_subscribe_cfg_t cfg = sub_cfg();
    MOQ_TEST_CHECK(moq_session_subscribe(c, &cfg, 10, &csub) == MOQ_OK);
    drop_actions(c);
    inject_done(c, 0, 0, 20);
    CHECK_CLOSED_0X3(c);
    destroy(c, sv);

    /* The ID of this side's live REQUEST_UPDATE. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
    moq_subscription_update_cfg_t u;
    moq_subscription_update_cfg_init_sized(&u, sizeof(u));
    u.has_forward = true; u.forward = false;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, csub, &u, 20), (int)MOQ_OK);
    drop_actions(c);
    inject_done(c, 2, 0, 30);
    CHECK_CLOSED_0X3(c);
    destroy(c, sv);

    /* A second DONE while the first is deferred on its Stream Count. */
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
    inject_done(c, 0, 3, 20);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(sub_request_id(c, csub) == 0);   /* still held, deferred */
    inject_done(c, 0, 3, 21);
    CHECK_CLOSED_0X3(c);
    destroy(c, sv);
}

/* -- Reply tombstones ------------------------------------------------------ */

/* A SUBSCRIBE cancelled before its answer: its ID waits for the OK/ERROR.
 * A DONE for it closes and leaves the tombstone in place. */
static void test_cancelled_initial_tomb(void)
{
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_subscribe_cfg_t cfg = sub_cfg();
    moq_subscription_t csub;
    MOQ_TEST_CHECK(moq_session_subscribe(c, &cfg, 10, &csub) == MOQ_OK);
    drop_actions(c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, csub, 20), (int)MOQ_OK);
    drop_actions(c);
    MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 1);
    inject_done(c, 0, 0, 30);
    MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 1);
    CHECK_CLOSED_0X3(c);
    destroy(c, sv);
}

/* The publisher answered the SUBSCRIBE and then ended it, both before the
 * UNSUBSCRIBE reached it: the answer settles the tombstone, and the DONE
 * that follows is absorbed. */
static void test_initial_ok_then_done(void)
{
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_subscribe_cfg_t cfg = sub_cfg();
    moq_subscription_t csub;
    MOQ_TEST_CHECK(moq_session_subscribe(c, &cfg, 10, &csub) == MOQ_OK);
    pump(c, sv, false, 10, 0);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, csub, 11), (int)MOQ_OK);
    drop_actions(c);   /* the UNSUBSCRIBE never reaches the server */
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, ssub, &ac, 12), (int)MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, ssub, &dc, 13), (int)MOQ_OK);
    pump(sv, c, true, 14, 0);
    MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 0);
    CHECK_ABSORBED(c, 0, 0);
    destroy(c, sv);
}

/* An established subscription cancelled with its REQUEST_UPDATE unanswered:
 * the update's ID is strict while the tombstone holds it, the original's
 * DONE is absorbed without touching it, and the update's answer then
 * settles it exactly once. */
static void test_cancelled_update_tomb(void)
{
    for (int done_on_update = 0; done_on_update < 2; done_on_update++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
        moq_subscription_t csub, ssub;
        MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
        moq_subscription_update_cfg_t u;
        moq_subscription_update_cfg_init_sized(&u, sizeof(u));
        u.has_forward = true; u.forward = false;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, csub, &u, 20), (int)MOQ_OK);
        drop_actions(c);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, csub, 21), (int)MOQ_OK);
        drop_actions(c);
        drain(c);
        MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 1);
        if (done_on_update) {
            inject_done(c, 2, 0, 30);
            MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 1);
            CHECK_CLOSED_0X3(c);
            destroy(c, sv);
            continue;
        }
        inject_done(c, 0, 0, 30);
        MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 1);
        CHECK_ABSORBED(c, 0, 0);
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d16_encode_request_ok(&w, 2, NULL, 0) == MOQ_OK);
        inject(c, buf, moq_buf_writer_offset(&w), 31);
        MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 0);
        CHECK_ABSORBED(c, 0, 0);
        /* Settled once: a second answer is now a reply to nothing. */
        inject(c, buf, moq_buf_writer_offset(&w), 32);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(c), (int)MOQ_SESS_CLOSED);
        destroy(c, sv);
    }
}

/* -- Owed inbound updates ------------------------------------------------- */

static size_t raw_update(uint8_t *buf, size_t cap, uint64_t id, uint64_t existing)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    if (moq_d16_encode_request_update(&w, id, existing, NULL, 0) != MOQ_OK) return 0;
    return moq_buf_writer_offset(&w);
}

/* `pub` receives, from its peer, REQUEST_UPDATE `first` for request
 * `existing`, a SUBSCRIBE `first + 2` it refuses, then REQUEST_UPDATE
 * `first + 4`; with answers deferred, the owed run is {first, first + 4}. */
static void owe_two_updates(moq_session_t *pub, uint64_t existing, uint64_t first)
{
    uint8_t buf[128];
    inject(pub, buf, raw_update(buf, sizeof(buf), first, existing), 20);
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t other = { (const uint8_t *)"audio", 5 };
    moq_namespace_t ns = { g_ns_part, 1 };
    MOQ_TEST_CHECK(moq_d16_encode_subscribe(&w, first + 2, &ns, other, NULL, 0) == MOQ_OK);
    inject(pub, buf, moq_buf_writer_offset(&w), 21);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(pub, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t refused = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_reject_subscribe_cfg_t rj;
    moq_reject_subscribe_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(pub, refused, &rj, 22),
                          (int)MOQ_OK);
    inject(pub, buf, raw_update(buf, sizeof(buf), first + 4, existing), 23);
    drop_actions(pub);
    drain(pub);
}

static void check_owed_run(const moq_owed_update_t *o, uint64_t first)
{
    MOQ_TEST_CHECK(o->owed && o->count == 2);
    MOQ_TEST_CHECK(session_owed_update_id(o, 0) == first &&
                   session_owed_update_id(o, 1) == first + 4);
}

/* Subscription pool: the server publishes the client's subscription 0 and
 * owes {2, 6}. While owed, a terminal for the oldest (2) or the latest (6)
 * closes; one for 4, a committed and retired ID between them, is absorbed
 * and the run stays owed. */
static void test_owed_updates_subscription(void)
{
    const uint64_t probes[] = { 2, 6, 4 };
    for (int unsub = 0; unsub < 2; unsub++)
    for (size_t k = 0; k < sizeof(probes) / sizeof(probes[0]); k++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_set_defer_subscription_updates(sv, true),
                              (int)MOQ_OK);
        moq_subscription_t csub, ssub;
        MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
        owe_two_updates(sv, 0, 2);
        int slot = sub_resolve_handle(sv, ssub);
        MOQ_TEST_CHECK(slot >= 0);
        if (slot >= 0) check_owed_run(&sv->subs[slot].owed_update, 2);
        inject_terminal(sv, unsub != 0, probes[k], 30);
        if (probes[k] == 4) {
            CHECK_ABSORBED(sv, 1, 0);
            if (slot >= 0) check_owed_run(&sv->subs[slot].owed_update, 2);
        } else {
            CHECK_CLOSED_0X3_OR_0X4(sv, unsub);
        }
        destroy(c, sv);
    }
}

/* Publication pool: the client's PUBLISH 0, accepted by the server, owes
 * the server's updates {1, 5}; 3 between them is a refused, retired ID. */
static void test_owed_updates_publication(void)
{
    const uint64_t probes[] = { 1, 5, 3 };
    for (int unsub = 0; unsub < 2; unsub++)
    for (size_t k = 0; k < sizeof(probes) / sizeof(probes[0]); k++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_set_defer_subscription_updates(c, true),
                              (int)MOQ_OK);
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ g_ns_part, 1 };
        pc.track_name = g_track;
        moq_publication_t cpub;
        MOQ_TEST_CHECK(moq_session_publish(c, &pc, 10, &cpub) == MOQ_OK);
        pump(c, sv, false, 10, 0);
        moq_event_t ev;
        MOQ_TEST_CHECK(take(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev));
        moq_publication_t spub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        MOQ_TEST_CHECK(moq_session_accept_publish(sv, spub, &ac, 10) == MOQ_OK);
        run(c, sv, 10);
        drain(c); drain(sv);
        owe_two_updates(c, 0, 1);
        int slot = pub_resolve_handle(c, cpub);
        MOQ_TEST_CHECK(slot >= 0);
        if (slot >= 0) check_owed_run(&c->publishes[slot].owed_update, 1);
        inject_terminal(c, unsub != 0, probes[k], 30);
        if (probes[k] == 3) {
            CHECK_ABSORBED(c, 0, 1);
            if (slot >= 0) check_owed_run(&c->publishes[slot].owed_update, 1);
        } else {
            CHECK_CLOSED_0X3_OR_0X4(c, unsub);
        }
        destroy(c, sv);
    }
}

/* -- Tolerated policy rows ------------------------------------------------- */

/* A SUBSCRIBE the server refused, and a retired request of another family
 * (the client's own refused PUBLISH): DONEs for either, repeated, are
 * absorbed with no effect on the live subscription beside them. */
static void test_tolerated_retired(void)
{
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    /* SUBSCRIBE 0, refused. */
    moq_subscribe_cfg_t cfg = sub_cfg();
    moq_subscription_t csub;
    MOQ_TEST_CHECK(moq_session_subscribe(c, &cfg, 10, &csub) == MOQ_OK);
    pump(c, sv, false, 10, 0);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t ssub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_reject_subscribe_cfg_t rj;
    moq_reject_subscribe_cfg_init(&rj);
    rj.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(sv, ssub, &rj, 11), (int)MOQ_OK);
    run(c, sv, 12);
    drain(c); drain(sv);
    /* PUBLISH 2 from the client, refused by the server. */
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ g_ns_part, 1 };
    pc.track_name = g_track;
    moq_publication_t cpub;
    MOQ_TEST_CHECK(moq_session_publish(c, &pc, 13, &cpub) == MOQ_OK);
    pump(c, sv, false, 13, 0);
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t spub = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_reject_publish_cfg_t rp;
    moq_reject_publish_cfg_init(&rp);
    rp.error_code = MOQ_REQUEST_ERROR_UNINTERESTED;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_publish(sv, spub, &rp, 14), (int)MOQ_OK);
    run(c, sv, 15);
    drain(c); drain(sv);
    /* A live subscription beside them, ID 4. */
    moq_subscription_t live_c, live_s;
    MOQ_TEST_CHECK(established(c, sv, &live_c, &live_s));
    MOQ_TEST_CHECK_EQ_U64(sub_request_id(c, live_c), 4);
    for (int rep = 0; rep < 2; rep++) {
        inject_done(c, 0, 0, 20 + (uint64_t)rep);
        CHECK_ABSORBED(c, 1, 0);
        inject_done(c, 2, 0, 22 + (uint64_t)rep);
        CHECK_ABSORBED(c, 1, 0);
    }
    MOQ_TEST_CHECK_EQ_U64(sub_request_id(c, live_c), 4);
    destroy(c, sv);
}

/* -- No retention ---------------------------------------------------------- */

/* A client with four subscription slots and one publication slot: a
 * reply-tombstone table of five. More cancels than that, none answered by a
 * DONE, all succeed, and the DONEs that arrive afterwards are absorbed. */
static moq_session_t *small_client(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_16;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    cfg.max_subscriptions = 4;
    cfg.max_publishes = 1;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (test_bind_if_required(s) < 0 || moq_session_start(s, 0) < 0) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

static void test_no_retention(void)
{
    side_cfg_t scfg = { .version = MOQ_VERSION_DRAFT_16 };
    moq_session_t *c = small_client();
    moq_session_t *sv = session(MOQ_PERSPECTIVE_SERVER, &scfg);
    MOQ_TEST_CHECK(c != NULL && sv != NULL);
    if (!c || !sv) {
        if (c) moq_session_destroy(c);
        if (sv) moq_session_destroy(sv);
        return;
    }
    run(c, sv, 1);
    drain(c); drain(sv);
    MOQ_TEST_CHECK_EQ_SIZE(c->sub_cap, 4);
    MOQ_TEST_CHECK_EQ_SIZE(c->pub_cap, 1);
    MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_cap, c->sub_cap + c->pub_cap);
    const size_t rounds = c->unsub_tomb_cap + 4;
    MOQ_TEST_CHECK(rounds > c->unsub_tomb_cap);
    uint64_t last = 0;
    for (size_t i = 0; i < rounds; i++) {
        moq_subscription_t csub, ssub;
        MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
        last = sub_request_id(c, csub);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, csub, 20), (int)MOQ_OK);
        run(c, sv, 20);
        drain(c); drain(sv);
    }
    MOQ_TEST_CHECK_EQ_U64(last, 2 * (rounds - 1));
    MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 0);
    for (uint64_t id = 0; id <= last; id += 2) {
        inject_done(c, id, 0, 30);
        CHECK_ABSORBED(c, 0, 0);
    }
    destroy(c, sv);
}

/* -- UNSUBSCRIBE after PUBLISH_DONE ---------------------------------------- */

/* Our PUBLISH (client origin, ID 0) ended with PUBLISH_DONE; the server's
 * UNSUBSCRIBE crossed it. The client absorbs it, twice, and the live
 * subscription beside it (client ID 2) is untouched. */
static void test_mirror_our_publish(void)
{
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ g_ns_part, 1 };
    pc.track_name = g_track;
    moq_publication_t cpub;
    MOQ_TEST_CHECK(moq_session_publish(c, &pc, 10, &cpub) == MOQ_OK);
    pump(c, sv, false, 10, 0);
    moq_event_t ev;
    MOQ_TEST_CHECK(take(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t spub = ev.u.publish_request.pub;
    moq_event_cleanup(&ev);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    MOQ_TEST_CHECK(moq_session_accept_publish(sv, spub, &ac, 10) == MOQ_OK);
    run(c, sv, 10);
    drain(c); drain(sv);
    moq_subscription_t live_c, live_s;
    MOQ_TEST_CHECK(established(c, sv, &live_c, &live_s));
    MOQ_TEST_CHECK_EQ_U64(sub_request_id(c, live_c), 2);
    moq_finish_publish_cfg_t fc;
    moq_finish_publish_cfg_init(&fc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_finish_publish(c, cpub, &fc, 20), (int)MOQ_OK);
    drop_actions(c);   /* the DONE never reaches the server */
    drain(c);
    MOQ_TEST_CHECK_EQ_SIZE(live_pubs(c), 0);
    for (int rep = 0; rep < 2; rep++) {
        inject_unsub(c, 0, 30 + (uint64_t)rep);
        CHECK_ABSORBED(c, 1, 0);
    }
    MOQ_TEST_CHECK_EQ_U64(sub_request_id(c, live_c), 2);
    destroy(c, sv);
}

/* A SUBSCRIBE the server served (client origin, ID 0) ended with
 * PUBLISH_DONE; the client's UNSUBSCRIBE crossed it. The server absorbs
 * it, twice, beside a live served subscription (ID 2). */
static void test_mirror_served_subscribe(void)
{
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_subscription_t csub, ssub, live_c, live_s;
    MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
    moq_bytes_t other = { (const uint8_t *)"audio", 5 };
    MOQ_TEST_CHECK(established_track(c, sv, other, &live_c, &live_s));
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, ssub, &dc, 20), (int)MOQ_OK);
    drop_actions(sv);
    drain(sv);
    MOQ_TEST_CHECK_EQ_SIZE(live_subs(sv), 1);
    for (int rep = 0; rep < 2; rep++) {
        inject_unsub(sv, 0, 30 + (uint64_t)rep);
        CHECK_ABSORBED(sv, 1, 0);
    }
    MOQ_TEST_CHECK_EQ_U64(sub_request_id(sv, live_s), 2);
    destroy(c, sv);
}

/* An UNSUBSCRIBE that does not parse closes even for an absorbable ID. */
static void test_mirror_malformed(void)
{
    moq_session_t *c, *sv;
    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_subscription_t csub, ssub;
    MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(sv, ssub, &dc, 20), (int)MOQ_OK);
    drop_actions(sv);
    drain(sv);
    /* Type 0xA, Length 2, then a 2-byte varint prefix with one byte left:
     * the Request ID runs past the body. */
    static const uint8_t bad[] = { 0x0a, 0x00, 0x01, 0x40 };
    inject(sv, bad, sizeof(bad), 30);
    CHECK_CLOSED_0X3_OR_0X4(sv, false);
    destroy(c, sv);
}

/* The bound for UNSUBSCRIBE on the server: before anything is committed,
 * the peer origin's next (0) and later (2) IDs and this side's own next (1)
 * and later (3) IDs all close. */
static void test_mirror_counters(void)
{
    const uint64_t ids[] = { 0, 2, 1, 3 };
    for (size_t i = 0; i < sizeof(ids) / sizeof(ids[0]); i++) {
        moq_session_t *c, *sv;
        MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
        inject_unsub(sv, ids[i], 20);
        CHECK_CLOSED_0X3_OR_0X4(sv, true);
        destroy(c, sv);
    }
}

/* Live owners keep their checks: a subscription this side subscribes to
 * (wrong role), a PUBLISH this side receives (wrong role), this side's live
 * REQUEST_UPDATE (another request family), and this side's cancelled,
 * unanswered SUBSCRIBE (its reply tombstone stays). */
static void test_mirror_live_and_tomb(void)
{
    moq_session_t *c, *sv;
    moq_subscription_t csub, ssub;

    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
    moq_subscription_update_cfg_t u;
    moq_subscription_update_cfg_init_sized(&u, sizeof(u));
    u.has_forward = true; u.forward = false;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_update_subscription(c, csub, &u, 20), (int)MOQ_OK);
    drop_actions(c);
    inject_unsub(c, 2, 30);
    CHECK_CLOSED_0X3_OR_0X4(c, true);
    destroy(c, sv);

    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    MOQ_TEST_CHECK(established(c, sv, &csub, &ssub));
    inject_unsub(c, 0, 20);
    CHECK_CLOSED_0X3_OR_0X4(c, true);
    destroy(c, sv);

    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_publication_t cpub, spub;
    MOQ_TEST_CHECK(published(c, sv, &cpub, &spub));
    inject_unsub(c, 1, 20);
    CHECK_CLOSED_0X3_OR_0X4(c, true);
    destroy(c, sv);

    MOQ_TEST_CHECK(pair(&c, &sv, MOQ_VERSION_DRAFT_16));
    moq_subscribe_cfg_t cfg = sub_cfg();
    MOQ_TEST_CHECK(moq_session_subscribe(c, &cfg, 10, &csub) == MOQ_OK);
    drop_actions(c);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(c, csub, 11), (int)MOQ_OK);
    drop_actions(c);
    MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 1);
    inject_unsub(c, 0, 20);
    MOQ_TEST_CHECK_EQ_SIZE(c->unsub_tomb_count, 1);
    CHECK_CLOSED_0X3_OR_0X4(c, true);
    destroy(c, sv);
}

int main(void)
{
    test_counters_uncommitted();
    test_counter_local_committed();
    test_counter_peer_committed();
    test_uncommitted_local_request();
    test_malformed_obsolete();
    test_live_refusals();
    test_cancelled_initial_tomb();
    test_initial_ok_then_done();
    test_cancelled_update_tomb();
    test_owed_updates_subscription();
    test_owed_updates_publication();
    test_tolerated_retired();
    test_no_retention();
    test_mirror_our_publish();
    test_mirror_served_subscribe();
    test_mirror_malformed();
    test_mirror_counters();
    test_mirror_live_and_tomb();
    if (failures) {
        fprintf(stderr, "FAIL: test_d16_obsolete_done (%d)\n", failures);
        return 1;
    }
    printf("PASS: test_d16_obsolete_done\n");
    return 0;
}
