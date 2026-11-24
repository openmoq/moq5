/*
 * Subscriber facade FETCH_OK result (moq_sub_get_fetch_result), on drafts
 * 16, 18 and 22: a Joining FETCH on drafts 16 and 18, a standalone FETCH on
 * every draft.
 *
 * Invariants pinned here:
 *   - before FETCH_OK the result has zero flags and no properties, and the
 *     query allocates nothing;
 *   - after FETCH_OK it carries RESPONSE_SEEN and FETCH_OK's own Track
 *     Properties, which a Joining FETCH keeps apart from its subscription's
 *     SUBSCRIBE_OK properties (moq_sub_get_track_result returns those);
 *   - the span stays valid and unchanged while the request's OBJECT items
 *     are polled and the session advances, and its storage is freed when
 *     moq_sub_poll_fetch returns the request's terminal item (ERROR, COMPLETE
 *     or RESET), on a successful cancel, and on destroy;
 *   - every refusal (NULL arguments, a foreign or released request, a size
 *     below the v0 floor) is MOQ_ERR_INVAL and leaves the output untouched;
 *     a larger caller struct keeps its tail;
 *   - FETCH_OK properties that cannot be retained report NOMEM (allocation)
 *     or BUFFER (byte budget) from the tick and the query, queue no FETCH_OK
 *     item, still deliver the request's objects and terminal, and stay
 *     latched until the request is released.
 */
#include <moq/subscriber.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include "test_support.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

/* Application-range properties: SUBSCRIBE_OK's and a different FETCH_OK
 * set, in each draft's encoding (draft 16 writes type 0x78 as a two-byte
 * QUIC varint). */
static const uint8_t ack_props[] = { 0x78, 0x01, 0x01, 0x03, 0xaa, 0x00, 0xbb };
static const uint8_t ack_props_d16[] = { 0x40, 0x78, 0x01, 0x01, 0x03, 0xaa, 0x00, 0xbb };
static const uint8_t fetch_props[] = { 0x7a, 0x05, 0x01, 0x02, 0xc1, 0xc2 };
static const uint8_t fetch_props_d16[] = { 0x40, 0x7a, 0x05, 0x01, 0x02, 0xc1, 0xc2 };

typedef enum { ORIGIN_JOINING, ORIGIN_STANDALONE } origin_t;

typedef struct ledger {
    size_t calls;
    size_t blocks;
    size_t bytes;
    bool fail;
} ledger_t;

static void *ledger_alloc(size_t n, void *ctx)
{
    ledger_t *l = ctx;
    l->calls++;
    if (l->fail) return NULL;
    void *p = malloc(n);
    if (p) { l->blocks++; l->bytes += n; }
    return p;
}

static void ledger_free(void *p, size_t n, void *ctx)
{
    ledger_t *l = ctx;
    if (p) {
        MOQ_TEST_CHECK(l->blocks > 0 && l->bytes >= n);
        l->blocks--;
        l->bytes -= n;
    }
    free(p);
}

typedef struct fixture {
    moq_version_t version;
    moq_simpair_t *pair;
    moq_subscriber_t *sub;
    ledger_t ledger;
} fixture_t;

static moq_session_t *server(fixture_t *f) { return moq_simpair_server(f->pair); }

static moq_bytes_t ack_properties(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ?
        (moq_bytes_t){ ack_props_d16, sizeof(ack_props_d16) } :
        (moq_bytes_t){ ack_props, sizeof(ack_props) };
}

static moq_bytes_t fetch_properties(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_16 ?
        (moq_bytes_t){ fetch_props_d16, sizeof(fetch_props_d16) } :
        (moq_bytes_t){ fetch_props, sizeof(fetch_props) };
}

static const char *vname(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_22 ? "d22"
         : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d16";
}

static const char *oname(origin_t o)
{
    return o == ORIGIN_JOINING ? "joining" : "standalone";
}

static void pump(fixture_t *f)
{
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f->pair, 16, NULL) == MOQ_OK);
}

/* Drain the publisher side's events, keeping the request handles. No
 * fixture step produces 64 of them. */
static void collect(fixture_t *f, moq_subscription_t *sub, moq_fetch_t *fetch)
{
    unsigned n = 0;
    for (; n < 64; n++) {
        moq_event_t ev;
        if (moq_session_poll_events(server(f), &ev, 1) != 1) break;
        if (sub && ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            *sub = ev.u.subscribe_request.sub;
        if (fetch && ev.kind == MOQ_EVENT_FETCH_REQUEST)
            *fetch = ev.u.fetch_request.fetch;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(n < 64);
}

static void fixture_init(fixture_t *f, moq_version_t version, size_t cap)
{
    memset(f, 0, sizeof(*f));
    f->version = version;
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default();
    pc.version = version;
    pc.client_send_request_capacity = true;
    pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = 32;
    pc.server_initial_request_capacity = 32;
    MOQ_TEST_CHECK(moq_simpair_create(&pc, &f->pair) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_start(f->pair) == MOQ_OK);
    pump(f);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(f->pair), &ev, 1))
        moq_event_cleanup(&ev);
    collect(f, NULL, NULL);
    moq_alloc_t alloc = { .ctx = &f->ledger, .alloc = ledger_alloc,
                         .free = ledger_free };
    moq_sub_cfg_t sc;
    moq_sub_cfg_init_sized(&sc, sizeof(sc));
    sc.max_tracks = 2;
    sc.max_fetches = 2;
    sc.max_queued_object_bytes = cap;
    MOQ_TEST_CHECK(moq_sub_create(moq_simpair_client(f->pair), &alloc,
                                  &sc, &f->sub) == MOQ_OK);
}

static void fixture_destroy(fixture_t *f)
{
    moq_sub_destroy(f->sub);
    moq_simpair_destroy(f->pair);
    MOQ_TEST_CHECK(f->ledger.blocks == 0 && f->ledger.bytes == 0);
}

/* An active subscription a Joining FETCH can join: Largest {10, 5}, with
 * SUBSCRIBE_OK carrying `props`. */
static moq_sub_track_t *subscribe(fixture_t *f, moq_bytes_t props)
{
    moq_sub_track_cfg_t cfg;
    moq_sub_track_cfg_init(&cfg);
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("fetch-result") };
    cfg.track_namespace = (moq_namespace_t){ ns, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("joined");
    moq_sub_track_t *track = NULL;
    MOQ_TEST_CHECK(moq_sub_subscribe(f->sub, &cfg, 0, &track) == MOQ_OK);
    pump(f);
    moq_subscription_t peer = MOQ_SUBSCRIPTION_INVALID;
    collect(f, &peer, NULL);
    MOQ_TEST_CHECK(moq_subscription_is_valid(peer));
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true;
    ac.largest_group = 10;
    ac.largest_object = 5;
    ac.track_properties = props;
    MOQ_TEST_CHECK(moq_session_accept_subscribe(server(f), peer, &ac, 0) == MOQ_OK);
    pump(f);
    MOQ_TEST_CHECK(moq_sub_tick(f->sub, 0) == MOQ_OK);
    MOQ_TEST_CHECK(moq_sub_track_is_active(track));
    return track;
}

/* Issue a fetch and return the publisher's handle for it. A Joining FETCH
 * starts three groups back (group 7); a standalone one asks for group 0. */
static moq_fetch_t request(fixture_t *f, origin_t origin, moq_sub_track_t *track,
                           const char *name, moq_sub_fetch_req_t **req)
{
    *req = NULL;
    if (origin == ORIGIN_JOINING) {
        moq_sub_joining_fetch_cfg_t jc;
        moq_sub_joining_fetch_cfg_init(&jc);
        jc.track = track;
        jc.relative = true;
        jc.joining_start = 3;
        MOQ_TEST_CHECK(moq_sub_joining_fetch(f->sub, &jc, 0, req) == MOQ_OK);
    } else {
        moq_sub_fetch_cfg_t fc;
        moq_sub_fetch_cfg_init(&fc);
        moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("fetch-result") };
        fc.track_namespace = (moq_namespace_t){ ns, 1 };
        fc.track_name = moq_bytes_cstr(name);
        fc.end_group = 1;
        MOQ_TEST_CHECK(moq_sub_fetch(f->sub, &fc, 0, req) == MOQ_OK);
    }
    MOQ_TEST_CHECK(*req != NULL);
    pump(f);
    moq_fetch_t peer = MOQ_FETCH_INVALID;
    collect(f, NULL, &peer);
    MOQ_TEST_CHECK(moq_fetch_is_valid(peer));
    return peer;
}

static uint64_t first_group(origin_t origin)
{
    return origin == ORIGIN_JOINING ? 7 : 0;
}

/* Accept with FETCH_OK carrying `props`, then send `objects` objects of the
 * first requested group. The fetch stays open. */
static void serve(fixture_t *f, moq_fetch_t peer, origin_t origin,
                  moq_bytes_t props, unsigned objects)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = origin == ORIGIN_JOINING ? 10 : 1;
    ac.end_object = origin == ORIGIN_JOINING ? 6 : 0;
    ac.track_properties = props;
    MOQ_TEST_CHECK(moq_session_accept_fetch(server(f), peer, &ac, 0) == MOQ_OK);
    for (unsigned i = 0; i < objects; i++) {
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = first_group(origin);
        oc.object_id = i;
        oc.publisher_priority = 128;
        MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"body",
                                        4, &oc.payload) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_fetch_object(server(f), peer, &oc, 0) == MOQ_OK);
        moq_rcbuf_decref(oc.payload);
    }
    pump(f);
}

static void end_fetch(fixture_t *f, moq_fetch_t peer)
{
    MOQ_TEST_CHECK(moq_session_end_fetch(server(f), peer, 0) == MOQ_OK);
    pump(f);
}

static void expect_item(fixture_t *f, moq_sub_fetch_item_kind_t kind,
                        const moq_sub_fetch_req_t *req)
{
    moq_sub_fetch_item_t item;
    MOQ_TEST_CHECK(moq_sub_poll_fetch(f->sub, &item) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)item.kind, (int)kind);
    MOQ_TEST_CHECK(item.request == req);
    moq_sub_fetch_item_cleanup(&item);
}

static void expect_refusal(const moq_subscriber_t *sub, const moq_sub_fetch_req_t *req,
                           size_t size, moq_result_t expected)
{
    moq_sub_fetch_result_t out;
    unsigned char saved[sizeof(out)];
    memset(&out, 0xa5, sizeof(out));
    memcpy(saved, &out, sizeof(out));
    MOQ_TEST_CHECK_EQ_INT(moq_sub_get_fetch_result(sub, req, &out, size), expected);
    MOQ_TEST_CHECK(memcmp(saved, &out, sizeof(out)) == 0);
}

static void expect_result(const moq_subscriber_t *sub, const moq_sub_fetch_req_t *req,
                          uint32_t flags, moq_bytes_t props)
{
    moq_sub_fetch_result_t out;
    memset(&out, 0xa5, sizeof(out));
    MOQ_TEST_CHECK(moq_sub_get_fetch_result(sub, req, &out, sizeof(out)) == MOQ_OK);
    MOQ_TEST_CHECK(out.struct_size == sizeof(out));
    MOQ_TEST_CHECK_EQ_INT((int)out.flags, (int)flags);
    MOQ_TEST_CHECK_EQ_SIZE(out.track_properties.len, props.len);
    MOQ_TEST_CHECK((out.track_properties.data == NULL) == (props.len == 0));
    if (props.len && out.track_properties.data && out.track_properties.len == props.len)
        MOQ_TEST_CHECK(memcmp(out.track_properties.data, props.data, props.len) == 0);
}

/* Before FETCH_OK, the argument and size guards, and the extents. */
static void test_initial_and_guards(moq_version_t version, origin_t origin)
{
    int before = failures;
    fixture_t f, other;
    fixture_init(&f, version, 0);
    fixture_init(&other, version, 0);
    moq_sub_track_t *track = origin == ORIGIN_JOINING ?
        subscribe(&f, ack_properties(version)) : NULL;
    moq_sub_fetch_req_t *req, *foreign;
    moq_fetch_t peer = request(&f, origin, track, "guards", &req);
    (void)request(&other, ORIGIN_STANDALONE, NULL, "foreign", &foreign);

    size_t calls = f.ledger.calls;
    moq_bytes_t none = { NULL, 0 };
    expect_result(f.sub, req, 0, none);
    MOQ_TEST_CHECK(f.ledger.calls == calls);
    for (size_t n = 0; n < MOQ_SUB_FETCH_RESULT_V0_SIZE; n++)
        expect_refusal(f.sub, req, n, MOQ_ERR_INVAL);
    expect_refusal(NULL, req, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    expect_refusal(f.sub, NULL, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    expect_refusal(f.sub, foreign, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    expect_refusal(other.sub, req, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_sub_get_fetch_result(f.sub, req, NULL,
                                            sizeof(moq_sub_fetch_result_t)) == MOQ_ERR_INVAL);

    serve(&f, peer, origin, fetch_properties(version), 0);
    end_fetch(&f, peer);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    expect_result(f.sub, req, MOQ_SUB_FETCH_RESULT_RESPONSE_SEEN, fetch_properties(version));
    struct { moq_sub_fetch_result_t value; uint8_t tail[16]; } future;
    memset(&future, 0xa5, sizeof(future));
    MOQ_TEST_CHECK(moq_sub_get_fetch_result(f.sub, req, &future.value,
                                            sizeof(future)) == MOQ_OK);
    MOQ_TEST_CHECK(future.value.struct_size == sizeof(moq_sub_fetch_result_t));
    for (size_t i = 0; i < sizeof(future.tail); i++) MOQ_TEST_CHECK(future.tail[i] == 0xa5);
    moq_sub_fetch_result_t out;
    MOQ_TEST_CHECK(moq_sub_get_fetch_result(f.sub, req, &out, SIZE_MAX) == MOQ_OK);
    MOQ_TEST_CHECK(f.ledger.calls == calls + 1u);

    expect_item(&f, MOQ_SUB_FETCH_OK, req);
    expect_item(&f, MOQ_SUB_FETCH_COMPLETE, req);
    expect_refusal(f.sub, req, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    fixture_destroy(&other);
    fixture_destroy(&f);
    if (failures == before)
        printf("PASS: fetch_result_guards_%s_%s\n", vname(version), oname(origin));
}

/* FETCH_OK's own properties, kept while the request's objects are polled
 * and released with its terminal item. */
static void test_retention(moq_version_t version, origin_t origin, bool with_props)
{
    int before = failures;
    fixture_t f;
    fixture_init(&f, version, 0);
    moq_bytes_t ack = ack_properties(version);
    moq_bytes_t props = with_props ? fetch_properties(version) : (moq_bytes_t){ NULL, 0 };
    moq_sub_track_t *track = origin == ORIGIN_JOINING ? subscribe(&f, ack) : NULL;
    moq_sub_fetch_req_t *req;
    moq_fetch_t peer = request(&f, origin, track, "retained", &req);
    size_t blocks = f.ledger.blocks, bytes = f.ledger.bytes, allocation = 0;
    if (with_props)
        MOQ_TEST_CHECK(moq_rcbuf_allocation_size(props.len, &allocation) == MOQ_OK);

    serve(&f, peer, origin, props, 2);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    MOQ_TEST_CHECK(f.ledger.blocks == blocks + (with_props ? 1u : 0u));
    MOQ_TEST_CHECK(f.ledger.bytes == bytes + allocation);
    moq_sub_stats_t stats;
    MOQ_TEST_CHECK(moq_sub_get_stats(f.sub, &stats, sizeof(stats)) == MOQ_OK);
    MOQ_TEST_CHECK(stats.bytes_queued == 8 && stats.fetch_ok == 1);
    expect_item(&f, MOQ_SUB_FETCH_OK, req);
    moq_sub_fetch_result_t first, again;
    MOQ_TEST_CHECK(moq_sub_get_fetch_result(f.sub, req, &first, sizeof(first)) == MOQ_OK);
    expect_result(f.sub, req, MOQ_SUB_FETCH_RESULT_RESPONSE_SEEN, props);
    if (track) {
        /* The subscription keeps SUBSCRIBE_OK's own properties. */
        moq_sub_track_result_t ack_result;
        MOQ_TEST_CHECK(moq_sub_get_track_result(f.sub, track, &ack_result,
                                                sizeof(ack_result)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(ack_result.track_properties.len, ack.len);
        if (ack_result.track_properties.data && ack_result.track_properties.len == ack.len)
            MOQ_TEST_CHECK(memcmp(ack_result.track_properties.data, ack.data, ack.len) == 0);
        MOQ_TEST_CHECK(ack_result.track_properties.data != first.track_properties.data);
    }

    for (uint64_t i = 0; i < 2; i++) {
        moq_sub_fetch_item_t item;
        MOQ_TEST_CHECK(moq_sub_poll_fetch(f.sub, &item) == MOQ_OK);
        MOQ_TEST_CHECK(item.kind == MOQ_SUB_FETCH_OBJECT && item.request == req);
        MOQ_TEST_CHECK(item.u.object.group_id == first_group(origin));
        MOQ_TEST_CHECK(item.u.object.object_id == i);
        /* Ordinary later work advances the session between the items. */
        if (i == 0) {
            end_fetch(&f, peer);
            MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
        }
        size_t calls = f.ledger.calls;
        MOQ_TEST_CHECK(moq_sub_get_fetch_result(f.sub, req, &again, sizeof(again)) == MOQ_OK);
        MOQ_TEST_CHECK(f.ledger.calls == calls);
        MOQ_TEST_CHECK(memcmp(&first, &again, sizeof(first)) == 0);
        if (with_props && first.track_properties.data)
            MOQ_TEST_CHECK(memcmp(first.track_properties.data, props.data, props.len) == 0);
        moq_sub_fetch_item_cleanup(&item);
    }
    MOQ_TEST_CHECK(f.ledger.blocks == blocks + (with_props ? 1u : 0u));
    expect_item(&f, MOQ_SUB_FETCH_COMPLETE, req);
    expect_refusal(f.sub, req, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(f.ledger.blocks == blocks && f.ledger.bytes == bytes);

    /* The slot's next request starts with no result. */
    moq_sub_fetch_req_t *next;
    moq_fetch_t next_peer = request(&f, ORIGIN_STANDALONE, NULL, "next", &next);
    expect_result(f.sub, next, 0, (moq_bytes_t){ NULL, 0 });
    serve(&f, next_peer, ORIGIN_STANDALONE, ack, 0);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    expect_result(f.sub, next, MOQ_SUB_FETCH_RESULT_RESPONSE_SEEN, ack);
    fixture_destroy(&f);
    if (failures == before)
        printf("PASS: fetch_result_retention_%s_%s_%s\n", vname(version), oname(origin),
               with_props ? "properties" : "empty");
}

/* Two requests in flight keep their own results; releasing one leaves the
 * other's span intact. */
static void test_two_requests(moq_version_t version)
{
    int before = failures;
    fixture_t f;
    fixture_init(&f, version, 0);
    moq_sub_fetch_req_t *a, *b;
    moq_fetch_t peer_a = request(&f, ORIGIN_STANDALONE, NULL, "a", &a);
    moq_fetch_t peer_b = request(&f, ORIGIN_STANDALONE, NULL, "b", &b);
    serve(&f, peer_a, ORIGIN_STANDALONE, fetch_properties(version), 0);
    serve(&f, peer_b, ORIGIN_STANDALONE, ack_properties(version), 0);
    end_fetch(&f, peer_a);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    expect_result(f.sub, a, MOQ_SUB_FETCH_RESULT_RESPONSE_SEEN, fetch_properties(version));
    expect_result(f.sub, b, MOQ_SUB_FETCH_RESULT_RESPONSE_SEEN, ack_properties(version));
    moq_sub_fetch_result_t kept;
    MOQ_TEST_CHECK(moq_sub_get_fetch_result(f.sub, b, &kept, sizeof(kept)) == MOQ_OK);
    expect_item(&f, MOQ_SUB_FETCH_OK, a);
    expect_item(&f, MOQ_SUB_FETCH_OK, b);
    expect_item(&f, MOQ_SUB_FETCH_COMPLETE, a);
    expect_refusal(f.sub, a, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    expect_result(f.sub, b, MOQ_SUB_FETCH_RESULT_RESPONSE_SEEN, ack_properties(version));
    if (kept.track_properties.data)
        MOQ_TEST_CHECK(memcmp(kept.track_properties.data, ack_properties(version).data,
                              kept.track_properties.len) == 0);
    end_fetch(&f, peer_b);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    expect_item(&f, MOQ_SUB_FETCH_COMPLETE, b);
    expect_refusal(f.sub, b, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    fixture_destroy(&f);
    if (failures == before)
        printf("PASS: fetch_result_two_requests_%s\n", vname(version));
}

/* A rejected request and a reset one release the result with their
 * terminal items; a cancel releases it at once; destroy frees what is
 * still held. */
static void test_other_releases(moq_version_t version, origin_t origin)
{
    int before = failures;
    fixture_t f;
    fixture_init(&f, version, 0);
    moq_sub_track_t *track = origin == ORIGIN_JOINING ?
        subscribe(&f, ack_properties(version)) : NULL;
    size_t blocks = f.ledger.blocks;
    moq_sub_fetch_req_t *req;

    /* ERROR: no FETCH_OK, so nothing is retained. */
    moq_fetch_t peer = request(&f, origin, track, "rejected", &req);
    moq_reject_fetch_cfg_t rc;
    moq_reject_fetch_cfg_init(&rc);
    rc.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    MOQ_TEST_CHECK(moq_session_reject_fetch(server(&f), peer, &rc, 0) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    expect_result(f.sub, req, 0, (moq_bytes_t){ NULL, 0 });
    expect_item(&f, MOQ_SUB_FETCH_ERROR, req);
    expect_refusal(f.sub, req, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);

    /* RESET: retained until the RESET item is polled. */
    peer = request(&f, origin, track, "reset", &req);
    serve(&f, peer, origin, fetch_properties(version), 1);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    MOQ_TEST_CHECK(f.ledger.blocks == blocks + 1);
    MOQ_TEST_CHECK(moq_session_abort_fetch(server(&f), peer, 0x7, 0) == MOQ_OK);
    pump(&f);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    expect_item(&f, MOQ_SUB_FETCH_OK, req);
    expect_item(&f, MOQ_SUB_FETCH_OBJECT, req);
    expect_result(f.sub, req, MOQ_SUB_FETCH_RESULT_RESPONSE_SEEN, fetch_properties(version));
    expect_item(&f, MOQ_SUB_FETCH_RESET, req);
    expect_refusal(f.sub, req, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(f.ledger.blocks == blocks);

    /* Cancel: released by the call. */
    peer = request(&f, origin, track, "cancelled", &req);
    serve(&f, peer, origin, fetch_properties(version), 1);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    MOQ_TEST_CHECK(f.ledger.blocks == blocks + 1);
    MOQ_TEST_CHECK(moq_sub_cancel_fetch(f.sub, req, 0) == MOQ_OK);
    expect_refusal(f.sub, req, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(f.ledger.blocks == blocks);
    pump(&f);
    collect(&f, NULL, NULL);

    /* Destroy: a result still held is freed with the subscriber. */
    peer = request(&f, origin, track, "destroyed", &req);
    serve(&f, peer, origin, fetch_properties(version), 1);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    MOQ_TEST_CHECK(f.ledger.blocks == blocks + 1);
    fixture_destroy(&f);
    if (failures == before)
        printf("PASS: fetch_result_releases_%s_%s\n", vname(version), oname(origin));
}

/* FETCH_OK properties that cannot be retained: an allocation failure
 * (NOMEM) or a byte budget too small for them (BUFFER). */
static void test_retention_failure(moq_version_t version, origin_t origin,
                                   bool allocation_failure)
{
    int before = failures;
    moq_bytes_t ack = ack_properties(version);
    moq_bytes_t props = fetch_properties(version);
    size_t cap = 0;
    if (!allocation_failure)
        cap = props.len - 1 + (origin == ORIGIN_JOINING ? ack.len : 0);
    fixture_t f;
    fixture_init(&f, version, cap);
    moq_sub_track_t *track = origin == ORIGIN_JOINING ? subscribe(&f, ack) : NULL;
    size_t blocks = f.ledger.blocks, bytes = f.ledger.bytes;
    moq_sub_fetch_req_t *req;
    moq_fetch_t peer = request(&f, origin, track, "failure", &req);
    serve(&f, peer, origin, props, 1);

    moq_result_t expected = allocation_failure ? MOQ_ERR_NOMEM : MOQ_ERR_BUFFER;
    size_t calls = f.ledger.calls;
    f.ledger.fail = allocation_failure;
    MOQ_TEST_CHECK_EQ_INT(moq_sub_tick(f.sub, 0), expected);
    f.ledger.fail = false;
    MOQ_TEST_CHECK(f.ledger.calls == calls + (allocation_failure ? 1u : 0u));
    MOQ_TEST_CHECK(f.ledger.blocks == blocks && f.ledger.bytes == bytes);
    expect_refusal(f.sub, req, sizeof(moq_sub_fetch_result_t), expected);
    /* The failing tick stopped after FETCH_OK and queued neither its item
     * nor a fabricated error. */
    moq_sub_fetch_item_t item;
    MOQ_TEST_CHECK(moq_sub_poll_fetch(f.sub, &item) == MOQ_DONE);
    moq_sub_stats_t stats;
    MOQ_TEST_CHECK(moq_sub_get_stats(f.sub, &stats, sizeof(stats)) == MOQ_OK);
    MOQ_TEST_CHECK(stats.fetch_ok == 1 && stats.fetch_errors == 0);

    /* Later ticks deliver the request's objects and report the retained
     * error while the request is active. */
    MOQ_TEST_CHECK_EQ_INT(moq_sub_tick(f.sub, 0), expected);
    expect_item(&f, MOQ_SUB_FETCH_OBJECT, req);
    expect_refusal(f.sub, req, sizeof(moq_sub_fetch_result_t), expected);
    if (track) {
        moq_sub_track_result_t ack_result;
        MOQ_TEST_CHECK(moq_sub_get_track_result(f.sub, track, &ack_result,
                                                sizeof(ack_result)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(ack_result.track_properties.len, ack.len);
    }
    end_fetch(&f, peer);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    expect_refusal(f.sub, req, sizeof(moq_sub_fetch_result_t), expected);
    expect_item(&f, MOQ_SUB_FETCH_COMPLETE, req);
    expect_refusal(f.sub, req, sizeof(moq_sub_fetch_result_t), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    MOQ_TEST_CHECK(f.ledger.blocks == blocks && f.ledger.bytes == bytes);

    /* A later request retains what fits. */
    moq_bytes_t next_props = allocation_failure ? props : (moq_bytes_t){ NULL, 0 };
    peer = request(&f, origin, track, "after", &req);
    serve(&f, peer, origin, next_props, 0);
    MOQ_TEST_CHECK(moq_sub_tick(f.sub, 0) == MOQ_OK);
    expect_result(f.sub, req, MOQ_SUB_FETCH_RESULT_RESPONSE_SEEN, next_props);
    expect_item(&f, MOQ_SUB_FETCH_OK, req);
    fixture_destroy(&f);
    if (failures == before)
        printf("PASS: fetch_result_failure_%s_%s_%s\n", vname(version), oname(origin),
               allocation_failure ? "nomem" : "buffer");
}

int main(void)
{
    const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22
    };
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        moq_version_t v = versions[i];
        /* Draft 22 has no Joining FETCH. */
        for (int o = v == MOQ_VERSION_DRAFT_22 ? 1 : 0; o < 2; o++) {
            origin_t origin = (origin_t)o;
            test_initial_and_guards(v, origin);
            test_retention(v, origin, true);
            test_retention(v, origin, false);
            test_other_releases(v, origin);
            test_retention_failure(v, origin, true);
            test_retention_failure(v, origin, false);
        }
        test_two_requests(v);
    }
    if (failures == 0) {
        printf("PASS: all subscriber fetch-result tests\n");
        return 0;
    }
    printf("FAIL: %d subscriber fetch-result test failures\n", failures);
    return 1;
}
