/*
 * Group order negotiation and validation (MOQT 7.1), draft-16 and draft-18.
 * The cases come from a test plan written from the drafts alone, before
 * reading the implementation (IDs in brackets, e.g. [GO-01]):
 *
 *   A. resolution: the subscriber's GROUP_ORDER, else the publisher's
 *      DEFAULT_PUBLISHER_GROUP_ORDER, else ascending; per subscription; the
 *      same for publisher-initiated subscriptions.
 *   B. the order cannot change after establishment.
 *   C. invalid values and duplicates on the wire.
 *   D. what the library emits: the preference on accepts, PUBLISH, FETCH_OK
 *      and TRACK_STATUS_OK; never twice.
 *
 * Ordering of data on the wire is covered by the scheduling tests.
 */
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/publisher.h>
#include <moq/sim.h>
#include <moq/wire.h>
#include "test_support.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define PROTOCOL_VIOLATION 0x3u
#define ASC  MOQ_GROUP_ORDER_ASCENDING
#define DESC MOQ_GROUP_ORDER_DESCENDING
#define NONE MOQ_GROUP_ORDER_DEFAULT

static const moq_version_t k_versions[] = {
    MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
};

static bool is_d18(moq_version_t ver) { return ver == MOQ_VERSION_DRAFT_18; }

/* -- harness --------------------------------------------------------- */

static moq_simpair_t *make_pair(moq_version_t ver)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 1;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    cfg.version = ver;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK) return NULL;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    return sp;
}

static void drain_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) moq_event_cleanup(&e);
}

static bool poll_for(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    bool got = false;
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) {
        if (!got && e.kind == kind) { *out = e; got = true; continue; }
        moq_event_cleanup(&e);
    }
    return got;
}

/* Drain `s`'s actions: the CLOSE_SESSION code (0 if none), and the stream of
 * the last request bidi it opened in *out_ref (when non-NULL). */
static uint64_t drain_actions(moq_session_t *s, moq_stream_ref_t *out_ref)
{
    uint64_t code = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_CLOSE_SESSION) code = a.u.close_session.code;
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM && out_ref)
            *out_ref = a.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&a);
    }
    return code;
}

static moq_namespace_t ns_live(moq_bytes_t *part)
{
    *part = MOQ_BYTES_LITERAL("live");
    return (moq_namespace_t){ part, 1 };
}

/* A one-byte varint KVP value (types are even, values < 64). */
static moq_kvp_entry_t kvp_u8(uint64_t type, uint8_t *storage, uint8_t v)
{
    *storage = v;
    moq_kvp_entry_t e;
    memset(&e, 0, sizeof(e));
    e.type = type;
    e.value = storage;
    e.value_len = 1;
    e.is_varint = true;
    return e;
}

/* The single byte where two encodings of one message differ, or SIZE_MAX. */
static size_t diff_offset(const uint8_t *a, const uint8_t *b, size_t len)
{
    size_t off = SIZE_MAX;
    for (size_t i = 0; i < len; i++) {
        if (a[i] == b[i]) continue;
        if (off != SIZE_MAX) return SIZE_MAX;
        off = i;
    }
    return off;
}

/* Feed a peer message to `s`: on the control stream (draft-16) or on request
 * stream `ref` (draft-18). */
static moq_result_t feed(moq_session_t *s, moq_version_t ver,
                         moq_stream_ref_t ref, const uint8_t *msg, size_t len)
{
    return is_d18(ver)
        ? moq_session_on_bidi_stream_bytes(s, ref, msg, len, false, 2000)
        : moq_session_on_control_bytes(s, msg, len, 2000);
}

/* -- A. resolution --------------------------------------------------- */

/* The client subscribes asking `req`; the server accepts advertising `pref`.
 * Both endpoints must resolve `want`. */
static void check_subscribe(moq_version_t ver, moq_group_order_t req,
                            moq_group_order_t pref, moq_group_order_t want)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_bytes_t part;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = ns_live(&part);
    sc.track_name = MOQ_BYTES_LITERAL("v");
    sc.group_order = req;
    moq_subscription_t cl_sub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(cl, &sc, now, &cl_sub),
                          MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
    moq_subscription_t sv_sub = ev.u.subscribe_request.sub;
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.subscribe_request.group_order, (int)req);
    moq_event_cleanup(&ev);

    uint8_t props[16];
    size_t props_len = 0;
    if (pref != NONE)
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_properties_add_group_order(
            sv, NULL, 0, pref, props, sizeof(props), &props_len), MOQ_OK);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.track_properties = (moq_bytes_t){ props, props_len };
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, sv_sub, &ac,
                                                            now), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_group_order(sv, sv_sub),
                          (int)want);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_SUBSCRIBE_OK, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.subscribe_ok.publisher_group_order,
                          (int)pref);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_group_order(cl, cl_sub),
                          (int)want);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(cl), MOQ_SESS_ESTABLISHED);
    moq_simpair_destroy(sp);
}

/* The client publishes advertising `pref`; the server accepts choosing
 * `choice`. Both endpoints must resolve `want`. */
static void check_publish(moq_version_t ver, moq_group_order_t pref,
                          moq_group_order_t choice, moq_group_order_t want)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    uint8_t props[16];
    size_t props_len = 0;
    if (pref != NONE)
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_properties_add_group_order(
            cl, NULL, 0, pref, props, sizeof(props), &props_len), MOQ_OK);
    moq_bytes_t part;
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = ns_live(&part);
    pc.track_name = MOQ_BYTES_LITERAL("v");
    pc.has_forward = true;
    pc.forward = false;   /* nothing flows before the accept [GO-05] */
    pc.track_properties = (moq_bytes_t){ props, props_len };
    moq_publication_t cl_pub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(cl, &pc, now, &cl_pub),
                          MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    moq_event_t ev;
    MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev));
    moq_publication_t sv_pub = ev.u.publish_request.pub;
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.publish_request.publisher_group_order,
                          (int)pref);
    moq_event_cleanup(&ev);

    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    ac.group_order = choice;
    ac.has_forward = true;
    ac.forward = true;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_publish(sv, sv_pub, &ac, now),
                          MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_group_order(sv, sv_pub),
                          (int)want);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_PUBLISH_OK, &ev));
    MOQ_TEST_CHECK_EQ_INT((int)ev.u.publish_ok.group_order, (int)choice);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_publication_group_order(cl, cl_pub),
                          (int)want);
    moq_simpair_destroy(sp);
}

/* [GO-12] Two subscriptions in one session keep their own order. */
static void check_per_subscription(moq_version_t ver)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    static const char *names[2] = { "t1", "t2" };
    static const moq_group_order_t orders[2] = { ASC, DESC };
    moq_subscription_t cl_sub[2], sv_sub[2];

    for (int i = 0; i < 2; i++) {
        moq_bytes_t part;
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = ns_live(&part);
        sc.track_name = (moq_bytes_t){ (const uint8_t *)names[i], 2 };
        sc.group_order = orders[i];
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(cl, &sc, now,
                                                         &cl_sub[i]), MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        moq_event_t ev;
        MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
        sv_sub[i] = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, sv_sub[i],
                                                                &ac, now),
                              MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        drain_events(cl);
    }
    for (int i = 0; i < 2; i++) {
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscription_group_order(sv, sv_sub[i]),
            (int)orders[i]);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscription_group_order(cl, cl_sub[i]),
            (int)orders[i]);
    }
    moq_simpair_destroy(sp);
}

static void test_resolution(void)
{
    for (size_t v = 0; v < 2; v++) {
        moq_version_t ver = k_versions[v];
        check_subscribe(ver, DESC, ASC, DESC);    /* [GO-01] */
        check_subscribe(ver, ASC, DESC, ASC);     /* [GO-02] */
        check_subscribe(ver, NONE, DESC, DESC);   /* [GO-03] */
        check_subscribe(ver, NONE, NONE, ASC);    /* [GO-04] */
        check_subscribe(ver, NONE, ASC, ASC);
        check_publish(ver, DESC, ASC, ASC);       /* [GO-05] */
        check_publish(ver, DESC, NONE, DESC);     /* [GO-06a] */
        check_publish(ver, NONE, NONE, ASC);      /* [GO-06b] */
        check_publish(ver, NONE, DESC, DESC);
        check_per_subscription(ver);              /* [GO-12] */
    }
    MOQ_TEST_PASS("group order resolution, both drafts");
}

/* -- B/C. what the library accepts on the wire ------------------------- */

#define PEER_REF 0x7f000000u   /* a request stream the simpair never uses */

/* The peer's SUBSCRIBE (request id 0, "live"/"v"), carrying GROUP_ORDER
 * `go` when `has_go`; draft-16 may repeat it (`dup` > 0: a second instance
 * with value `dup`). draft-18 encodes it as uint8, so `go` must fit. */
static size_t enc_subscribe(moq_version_t ver, bool has_go, uint64_t go,
                            uint8_t dup, uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t part;
    moq_namespace_t ns = ns_live(&part);
    if (!is_d18(ver)) {
        uint8_t v[2][8];
        moq_kvp_entry_t kv[2];
        size_t n = 0;
        if (has_go) {
            memset(&kv[n], 0, sizeof(kv[n]));
            kv[n].type = MOQ_MSG_PARAM_GROUP_ORDER;
            kv[n].value = v[n];
            kv[n].value_len = moq_quic_varint_encode(go, v[n], sizeof(v[n]));
            kv[n].is_varint = true;
            n++;
        }
        if (dup) { kv[n] = kvp_u8(MOQ_MSG_PARAM_GROUP_ORDER, v[n], dup); n++; }
        if (moq_d16_encode_subscribe(&w, 0, &ns, MOQ_BYTES_LITERAL("v"), kv,
                                     n) < 0)
            return 0;
        return moq_buf_writer_offset(&w);
    }
    /* draft-18: encode with 1 and with 2, then write `go` into the one byte
     * where they differ (the encoder refuses out-of-range values). */
    uint8_t other[256];
    moq_buf_writer_t w2;
    moq_buf_writer_init(&w2, other, sizeof(other));
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.has_group_order = has_go;
    mp.group_order = 1;
    if (moq_d18_encode_subscribe(&w, 0, &ns, MOQ_BYTES_LITERAL("v"), &mp) < 0)
        return 0;
    if (!has_go) return moq_buf_writer_offset(&w);
    mp.group_order = 2;
    if (moq_d18_encode_subscribe(&w2, 0, &ns, MOQ_BYTES_LITERAL("v"), &mp) < 0)
        return 0;
    size_t len = moq_buf_writer_offset(&w);
    size_t off = diff_offset(buf, other, len);
    if (off == SIZE_MAX) return 0;
    buf[off] = (uint8_t)go;
    return len;
}

/* Feed the peer's SUBSCRIBE to a fresh server; returns the close code (0 =
 * still open) and whether the application saw the request. */
static uint64_t server_takes_subscribe(moq_version_t ver, bool has_go,
                                       uint64_t go, uint8_t dup,
                                       bool *out_requested)
{
    *out_requested = false;
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return 0;
    moq_session_t *sv = moq_simpair_server(sp);
    drain_events(sv);
    uint8_t msg[256];
    size_t n = enc_subscribe(ver, has_go, go, dup, msg, sizeof(msg));
    MOQ_TEST_CHECK(n > 0);
    (void)feed(sv, ver, moq_stream_ref_from_u64(PEER_REF), msg, n);
    moq_event_t ev;
    *out_requested = poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
    if (*out_requested) moq_event_cleanup(&ev);
    uint64_t code = drain_actions(sv, NULL);
    moq_simpair_destroy(sp);
    return code;
}

/* [GV-01] [GV-02] GROUP_ORDER outside 1..2 closes with PROTOCOL_VIOLATION;
 * 1 and 2 are accepted. [GV-04] a repeated GROUP_ORDER SHOULD close too. */
static void test_subscribe_group_order_values(void)
{
    for (size_t v = 0; v < 2; v++) {
        moq_version_t ver = k_versions[v];
        bool req;
        for (uint64_t go = 1; go <= 2; go++) {
            MOQ_TEST_CHECK_EQ_U64(server_takes_subscribe(ver, true, go, 0, &req),
                                  0);
            MOQ_TEST_CHECK(req);
        }
        static const uint64_t bad[] = { 0, 3, 255 };
        for (size_t i = 0; i < 3; i++) {
            MOQ_TEST_CHECK_EQ_U64(
                server_takes_subscribe(ver, true, bad[i], 0, &req),
                PROTOCOL_VIOLATION);
            MOQ_TEST_CHECK(!req);
        }
        if (!is_d18(ver)) {
            /* draft-16 carries it as a varint: a multi-byte value too. */
            MOQ_TEST_CHECK_EQ_U64(server_takes_subscribe(ver, true, 257, 0, &req),
                                  PROTOCOL_VIOLATION);
            MOQ_TEST_CHECK_EQ_U64(server_takes_subscribe(ver, true, 1, 1, &req),
                                  PROTOCOL_VIOLATION);
            MOQ_TEST_CHECK_EQ_U64(server_takes_subscribe(ver, true, 1, 2, &req),
                                  PROTOCOL_VIOLATION);
        }
    }
    MOQ_TEST_PASS("SUBSCRIBE GROUP_ORDER values and duplicates");
}

/* The peer's REQUEST_UPDATE (request id 2) on the subscription with request
 * id 0, carrying SUBSCRIBER_PRIORITY 5, or GROUP_ORDER 2 instead when
 * `group_order` (not a REQUEST_UPDATE parameter; for draft-18 the type is
 * rewritten past the encoder's scope check). */
static size_t enc_update(moq_version_t ver, bool group_order, uint8_t *buf,
                         size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    if (!is_d18(ver)) {
        uint8_t v;
        moq_kvp_entry_t kv = group_order
            ? kvp_u8(MOQ_MSG_PARAM_GROUP_ORDER, &v, 2)
            : kvp_u8(MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY, &v, 5);
        if (moq_d16_encode_request_update(&w, 2, 0, &kv, 1) < 0) return 0;
        return moq_buf_writer_offset(&w);
    }
    moq_d18_msg_params_t mp;
    memset(&mp, 0, sizeof(mp));
    mp.has_subscriber_priority = true;
    mp.subscriber_priority = 5;
    if (moq_d18_encode_request_update(&w, 2, &mp) < 0) return 0;
    size_t len = moq_buf_writer_offset(&w);
    if (!group_order) return len;
    uint8_t other[128];
    moq_buf_writer_t w2;
    moq_buf_writer_init(&w2, other, sizeof(other));
    mp.subscriber_priority = 6;
    if (moq_d18_encode_request_update(&w2, 2, &mp) < 0) return 0;
    size_t off = diff_offset(buf, other, len);
    /* The only parameter: its Type-Delta is the type itself (0x20). */
    if (off == SIZE_MAX || off == 0 || buf[off - 1] != 0x20) return 0;
    buf[off - 1] = 0x22;
    buf[off] = 2;
    return len;
}

/* [GO-07] An established subscription's group order cannot change: a
 * REQUEST_UPDATE carrying GROUP_ORDER closes the session in draft-18 (MUST);
 * draft-16 is ambiguous (ignore or close), so only "the order is unchanged"
 * is asserted there. A plain priority update is the positive control. */
static void check_update_cannot_change_order(moq_version_t ver,
                                             bool group_order)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    drain_events(sv);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(PEER_REF);
    uint8_t msg[256];
    size_t n = enc_subscribe(ver, true, ASC, 0, msg, sizeof(msg));
    MOQ_TEST_CHECK(n > 0);
    (void)feed(sv, ver, ref, msg, n);
    moq_event_t ev;
    bool got = poll_for(sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev);
    MOQ_TEST_CHECK(got);
    if (!got) { moq_simpair_destroy(sp); return; }
    moq_subscription_t sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(sv, sub, &ac, now),
                          MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(drain_actions(sv, NULL), 0);

    n = enc_update(ver, group_order, msg, sizeof(msg));
    MOQ_TEST_CHECK(n > 0);
    (void)feed(sv, ver, ref, msg, n);
    bool updated = poll_for(sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev);
    if (updated) moq_event_cleanup(&ev);
    uint64_t code = drain_actions(sv, NULL);
    if (!group_order) {
        MOQ_TEST_CHECK(updated);
        MOQ_TEST_CHECK_EQ_U64(code, 0);
    } else if (is_d18(ver)) {
        MOQ_TEST_CHECK_EQ_U64(code, PROTOCOL_VIOLATION);
    }
    if (code == 0)
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscription_group_order(sv, sub), (int)ASC);
    moq_simpair_destroy(sp);
}

static void test_order_cannot_change(void)
{
    for (size_t v = 0; v < 2; v++) {
        check_update_cannot_change_order(k_versions[v], false);
        check_update_cannot_change_order(k_versions[v], true);
    }
    MOQ_TEST_PASS("group order cannot change after establishment");
}

/* Peer requests to a server: FETCH carries GROUP_ORDER, PUBLISH carries the
 * DEFAULT_PUBLISHER_GROUP_ORDER Track Property. */
typedef enum { REQ_FETCH, REQ_PUBLISH } req_kind_t;

static size_t enc_request_raw(moq_version_t ver, req_kind_t kind, uint8_t go,
                              uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t part;
    moq_namespace_t ns = ns_live(&part);
    const uint8_t props[2] = { 0x22, go };
    moq_result_t rc;
    if (kind == REQ_FETCH) {
        if (is_d18(ver)) {
            moq_d18_fetch_t f;
            memset(&f, 0, sizeof(f));
            f.fetch_type = 1;
            f.track_namespace = ns;
            f.track_name = MOQ_BYTES_LITERAL("v");
            f.end.object = 1;
            f.params.has_group_order = true;
            f.params.group_order = go;
            rc = moq_d18_encode_fetch(&w, &f);
        } else {
            moq_d16_fetch_t f;
            memset(&f, 0, sizeof(f));
            f.fetch_type = 1;
            f.track_namespace = ns;
            f.track_name = MOQ_BYTES_LITERAL("v");
            f.end_object = 1;
            uint8_t v;
            moq_kvp_entry_t kv = kvp_u8(MOQ_MSG_PARAM_GROUP_ORDER, &v, go);
            rc = moq_d16_encode_fetch(&w, &f, &kv, 1);
        }
    } else if (is_d18(ver)) {
        moq_d18_publish_t p;
        memset(&p, 0, sizeof(p));
        p.track_namespace = ns;
        p.track_name = MOQ_BYTES_LITERAL("v");
        p.track_alias = 1;
        p.track_properties = (moq_bytes_t){ props, sizeof(props) };
        rc = moq_d18_encode_publish(&w, &p);
    } else {
        moq_d16_publish_t p;
        memset(&p, 0, sizeof(p));
        p.track_namespace = ns;
        p.track_name = MOQ_BYTES_LITERAL("v");
        p.track_alias = 1;
        p.track_extensions = props;
        p.track_extensions_len = sizeof(props);
        rc = moq_d16_encode_publish(&w, &p);
    }
    return rc < 0 ? 0 : moq_buf_writer_offset(&w);
}

/* As enc_response: any value, written past the encoders' range checks. */
static size_t enc_request(moq_version_t ver, req_kind_t kind, uint8_t go,
                          uint8_t *buf, size_t cap)
{
    uint8_t other[256];
    size_t len = enc_request_raw(ver, kind, 1, buf, cap);
    if (len == 0 || enc_request_raw(ver, kind, 2, other, sizeof(other)) != len)
        return 0;
    size_t off = diff_offset(buf, other, len);
    if (off == SIZE_MAX) return 0;
    buf[off] = go;
    return len;
}

static uint64_t server_takes_request(moq_version_t ver, req_kind_t kind,
                                     uint8_t go, bool *out_requested)
{
    static const moq_event_kind_t req_event[] = {
        MOQ_EVENT_FETCH_REQUEST, MOQ_EVENT_PUBLISH_REQUEST,
    };
    *out_requested = false;
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return 0;
    moq_session_t *sv = moq_simpair_server(sp);
    drain_events(sv);
    uint8_t msg[256];
    size_t n = enc_request(ver, kind, go, msg, sizeof(msg));
    MOQ_TEST_CHECK(n > 0);
    (void)feed(sv, ver, moq_stream_ref_from_u64(PEER_REF), msg, n);
    moq_event_t ev;
    *out_requested = poll_for(sv, req_event[kind], &ev);
    if (*out_requested) moq_event_cleanup(&ev);
    uint64_t code = drain_actions(sv, NULL);
    moq_simpair_destroy(sp);
    return code;
}

/* [GV-01] [GV-02] FETCH's GROUP_ORDER and [GV-03] PUBLISH's
 * DEFAULT_PUBLISHER_GROUP_ORDER: outside 1..2 closes with PROTOCOL_VIOLATION,
 * 1 and 2 are accepted. */
static void test_request_values(void)
{
    for (size_t v = 0; v < 2; v++) {
        for (int k = REQ_FETCH; k <= REQ_PUBLISH; k++) {
            for (uint8_t go = 0; go <= 3; go++) {
                bool valid = go == 1 || go == 2, requested;
                MOQ_TEST_CHECK_EQ_U64(
                    server_takes_request(k_versions[v], (req_kind_t)k, go,
                                         &requested),
                    valid ? 0 : PROTOCOL_VIOLATION);
                MOQ_TEST_CHECK(requested == valid);
            }
        }
    }
    MOQ_TEST_PASS("request group order values (FETCH, PUBLISH)");
}

/* The client's request with the peer's response fed back: `kind` selects
 * SUBSCRIBE -> SUBSCRIBE_OK, FETCH -> FETCH_OK or PUBLISH -> PUBLISH_OK. */
typedef enum { RESP_SUBSCRIBE_OK, RESP_FETCH_OK, RESP_PUBLISH_OK } resp_kind_t;

/* Encode the response once with group order value `go`: the
 * DEFAULT_PUBLISHER_GROUP_ORDER Track Property (SUBSCRIBE_OK, FETCH_OK) or the
 * GROUP_ORDER parameter (PUBLISH_OK). */
static size_t enc_response_raw(moq_version_t ver, resp_kind_t kind,
                               uint8_t go, uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    const uint8_t props[2] = { 0x22, go };
    moq_bytes_t pb = { props, sizeof(props) };
    moq_result_t rc;
    if (kind == RESP_SUBSCRIBE_OK) {
        if (is_d18(ver)) {
            moq_d18_msg_params_t mp;
            memset(&mp, 0, sizeof(mp));
            rc = moq_d18_encode_subscribe_ok(&w, 1, &mp, pb);
        } else {
            rc = moq_d16_encode_subscribe_ok(&w, 0, 1, NULL, 0, props,
                                             sizeof(props));
        }
    } else if (kind == RESP_FETCH_OK) {
        if (is_d18(ver)) {
            moq_d18_location_t end = { 0, 1 };
            rc = moq_d18_encode_fetch_ok(&w, false, end, pb);
        } else {
            moq_d16_fetch_ok_t ok;
            memset(&ok, 0, sizeof(ok));
            ok.end_object = 1;
            ok.track_extensions = props;
            ok.track_extensions_len = sizeof(props);
            rc = moq_d16_encode_fetch_ok(&w, &ok);
        }
    } else if (is_d18(ver)) {
        moq_d18_msg_params_t mp;
        memset(&mp, 0, sizeof(mp));
        mp.has_group_order = true;
        mp.group_order = go;
        rc = moq_d18_encode_publish_ok(&w, &mp);
    } else {
        uint8_t v;
        moq_kvp_entry_t kv = kvp_u8(MOQ_MSG_PARAM_GROUP_ORDER, &v, go);
        rc = moq_d16_encode_publish_ok(&w, 0, &kv, 1);
    }
    return rc < 0 ? 0 : moq_buf_writer_offset(&w);
}

/* The response with any value `go`: encoders refuse values outside 1..2, so
 * encode with 1 and with 2 and write `go` into the one byte that differs. */
static size_t enc_response(moq_version_t ver, resp_kind_t kind, uint8_t go,
                           uint8_t *buf, size_t cap)
{
    uint8_t other[256];
    size_t len = enc_response_raw(ver, kind, 1, buf, cap);
    if (len == 0 || enc_response_raw(ver, kind, 2, other, sizeof(other)) != len)
        return 0;
    size_t off = diff_offset(buf, other, len);
    if (off == SIZE_MAX) return 0;
    buf[off] = go;
    return len;
}

/* The client sends the request, then takes the crafted response. Returns the
 * close code (0 = still open) and whether the application saw the response
 * (*out_ok). */
static uint64_t client_takes_response(moq_version_t ver, resp_kind_t kind,
                                      uint8_t go, bool *out_ok)
{
    static const moq_event_kind_t ok_event[] = {
        MOQ_EVENT_SUBSCRIBE_OK, MOQ_EVENT_FETCH_OK, MOQ_EVENT_PUBLISH_OK,
    };
    *out_ok = false;
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return 0;
    moq_session_t *cl = moq_simpair_client(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t part;
    moq_result_t rc;
    if (kind == RESP_SUBSCRIBE_OK) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = ns_live(&part);
        sc.track_name = MOQ_BYTES_LITERAL("v");
        moq_subscription_t h;
        rc = moq_session_subscribe(cl, &sc, now, &h);
    } else if (kind == RESP_FETCH_OK) {
        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        fc.track_namespace = ns_live(&part);
        fc.track_name = MOQ_BYTES_LITERAL("v");
        fc.end_group = 0;
        fc.end_object = 1;
        moq_fetch_t h;
        rc = moq_session_fetch(cl, &fc, now, &h);
    } else {
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = ns_live(&part);
        pc.track_name = MOQ_BYTES_LITERAL("v");
        moq_publication_t h;
        rc = moq_session_publish(cl, &pc, now, &h);
    }
    MOQ_TEST_CHECK_EQ_INT((int)rc, MOQ_OK);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    (void)drain_actions(cl, &ref);
    uint8_t msg[256];
    size_t n = enc_response(ver, kind, go, msg, sizeof(msg));
    MOQ_TEST_CHECK(n > 0);
    (void)feed(cl, ver, ref, msg, n);
    moq_event_t ev;
    *out_ok = poll_for(cl, ok_event[kind], &ev);
    if (*out_ok) moq_event_cleanup(&ev);
    uint64_t code = drain_actions(cl, NULL);
    moq_simpair_destroy(sp);
    return code;
}

/* [GV-03] DEFAULT_PUBLISHER_GROUP_ORDER outside 1..2 in SUBSCRIBE_OK or
 * FETCH_OK closes with PROTOCOL_VIOLATION; 1 and 2 are accepted.
 * [GV-01] (PUBLISH_OK) GROUP_ORDER outside 1..2 closes as well. */
static void test_response_values(void)
{
    for (size_t v = 0; v < 2; v++) {
        moq_version_t ver = k_versions[v];
        for (int k = RESP_SUBSCRIBE_OK; k <= RESP_PUBLISH_OK; k++) {
            for (uint8_t go = 0; go <= 3; go++) {
                bool valid = go == 1 || go == 2, ok;
                MOQ_TEST_CHECK_EQ_U64(
                    client_takes_response(ver, (resp_kind_t)k, go, &ok),
                    valid ? 0 : PROTOCOL_VIOLATION);
                MOQ_TEST_CHECK(ok == valid);
            }
        }
    }
    MOQ_TEST_PASS("response group order values (SUBSCRIBE_OK, FETCH_OK, "
                  "PUBLISH_OK)");
}

/* [GV-06] The library's SUBSCRIBE carries the subscriber's order as a
 * GROUP_ORDER parameter that a strict decoder accepts (draft-18 parameters
 * must be in ascending type order), and omits it when none was asked. */
static void check_subscribe_encoding(moq_version_t ver, moq_group_order_t order)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_bytes_t part;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = ns_live(&part);
    sc.track_name = MOQ_BYTES_LITERAL("v");
    sc.has_subscriber_priority = true;
    sc.subscriber_priority = 7;
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.group_order = order;
    moq_subscription_t h;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(cl, &sc,
        moq_simpair_now_us(sp), &h), MOQ_OK);

    bool found = false, has_go = false;
    uint64_t go = 0;
    moq_action_t a;
    while (moq_session_poll_actions(cl, &a, 1) > 0) {
        const uint8_t *data = NULL;
        size_t len = 0;
        if (a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            data = a.u.open_bidi_stream.data;
            len = a.u.open_bidi_stream.len;
        } else if (a.kind == MOQ_ACTION_SEND_CONTROL) {
            data = a.u.send_control.data;
            len = a.u.send_control.len;
        }
        if (data && !found) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, data, len);
            moq_control_envelope_t env;
            moq_bytes_t parts[4];
            if (is_d18(ver)) {
                moq_d18_subscribe_t d;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    moq_d18_decode_subscribe(env.payload, env.payload_len,
                                             parts, 4, &d) == MOQ_OK) {
                    found = true;
                    has_go = d.params.has_group_order;
                    go = d.params.group_order;
                }
            } else {
                moq_kvp_entry_t kv[8];
                moq_d16_subscribe_t d;
                memset(&d, 0, sizeof(d));
                d.params = kv;
                d.params_cap = 8;
                if (moq_control_decode_envelope(&r, &env) == MOQ_OK &&
                    moq_d16_decode_subscribe(env.payload, env.payload_len,
                                             parts, 4, &d) == MOQ_OK) {
                    found = true;
                    for (size_t k = 0; k < d.params_count; k++) {
                        if (kv[k].type != MOQ_MSG_PARAM_GROUP_ORDER) continue;
                        uint8_t v8 = 0;
                        has_go = moq_d16_decode_param_group_order(
                            kv[k].value, kv[k].value_len, &v8) == MOQ_OK;
                        go = v8;
                    }
                }
            }
        }
        moq_action_cleanup(&a);
    }
    MOQ_TEST_CHECK(found);
    MOQ_TEST_CHECK(has_go == (order != NONE));
    if (order != NONE) MOQ_TEST_CHECK_EQ_U64(go, (uint64_t)order);
    moq_simpair_destroy(sp);
}

static void test_subscribe_encoding(void)
{
    for (size_t v = 0; v < 2; v++) {
        check_subscribe_encoding(k_versions[v], NONE);
        check_subscribe_encoding(k_versions[v], ASC);
        check_subscribe_encoding(k_versions[v], DESC);
    }
    MOQ_TEST_PASS("SUBSCRIBE group order encoding");
}

/* -- D. what the library emits -------------------------------------------- */

/* The preference as Track Properties encoded by `s` (expected bytes). */
static size_t pref_props(moq_session_t *s, moq_group_order_t order,
                         uint8_t *out, size_t cap)
{
    size_t len = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_properties_add_group_order(
        s, NULL, 0, order, out, cap, &len), MOQ_OK);
    return len;
}

static moq_publisher_t *make_publisher(moq_session_t *s)
{
    moq_pub_cfg_t pc;
    moq_pub_cfg_init_sized(&pc, sizeof(pc));
    pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_create(s, moq_alloc_default(), &pc,
                                              &pub), MOQ_OK);
    return pub;
}

static moq_pub_track_t *add_track(moq_publisher_t *pub, moq_group_order_t order,
                                  uint64_t now)
{
    moq_bytes_t part;
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = ns_live(&part);
    tc.track_name = MOQ_BYTES_LITERAL("v");
    tc.default_group_order = order;
    moq_pub_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_add_track(pub, &tc, now, &t), MOQ_OK);
    return t;
}

/* Hand every pending event of `s` to the publisher facade. */
static void feed_publisher(moq_publisher_t *pub, moq_session_t *s, uint64_t now)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        moq_pub_event_result_t res;
        (void)moq_pub_handle_event(pub, &ev, now, &res);
        moq_event_cleanup(&ev);
    }
}

/* [GO-03 / GO-04, publisher side] A track's default group order rides every
 * accept of a subscription to it; none is advertised by default. */
static void check_facade_subscribe(moq_version_t ver, moq_group_order_t order)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publisher_t *pub = make_publisher(sv);
    (void)add_track(pub, order, now);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    drain_events(cl);
    feed_publisher(pub, sv, now);

    moq_bytes_t part;
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = ns_live(&part);
    sc.track_name = MOQ_BYTES_LITERAL("v");
    moq_subscription_t cl_sub;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(cl, &sc, now, &cl_sub),
                          MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    feed_publisher(pub, sv, now);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_event_t ev;
    bool ok = poll_for(cl, MOQ_EVENT_SUBSCRIBE_OK, &ev);
    MOQ_TEST_CHECK(ok);
    if (ok) {
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.subscribe_ok.publisher_group_order,
                              (int)order);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscription_group_order(cl, cl_sub),
                          (int)(order == NONE ? ASC : order));
    moq_pub_destroy(pub);
    moq_simpair_destroy(sp);
}

/* [GO-05 / GO-06, publisher side] The default group order rides the track's
 * PUBLISH, merged with the application's own properties. An application blob
 * that already carries the property must agree with the track: the library
 * never emits it twice [GV-04 sender side]. */
static void check_facade_publish(moq_version_t ver, bool app_prop,
                                 moq_group_order_t app_order)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publisher_t *pub = make_publisher(cl);
    moq_pub_track_t *t = add_track(pub, DESC, now);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    drain_events(sv);
    feed_publisher(pub, cl, now);

    uint8_t app[16];
    size_t app_len = app_prop ? pref_props(cl, app_order, app, sizeof(app)) : 0;
    moq_pub_publish_cfg_t pc;
    moq_pub_publish_cfg_init(&pc);
    pc.track_properties = (moq_bytes_t){ app, app_len };
    moq_result_t rc = moq_pub_publish_track(pub, t, &pc, now);
    bool conflict = app_prop && app_order != DESC;
    MOQ_TEST_CHECK_EQ_INT((int)rc, conflict ? MOQ_ERR_INVAL : MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_event_t ev;
    bool got = poll_for(sv, MOQ_EVENT_PUBLISH_REQUEST, &ev);
    MOQ_TEST_CHECK(got == !conflict);
    if (got) {
        MOQ_TEST_CHECK_EQ_INT((int)ev.u.publish_request.publisher_group_order,
                              (int)DESC);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_state(sv), MOQ_SESS_ESTABLISHED);
    moq_pub_destroy(pub);
    moq_simpair_destroy(sp);
}

/* [FET-02, publisher side] FETCH_OK carries the track's Track Properties,
 * the default group order among them. */
static void check_facade_fetch_ok(moq_version_t ver)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_publisher_t *pub = make_publisher(sv);
    moq_pub_track_t *t = add_track(pub, DESC, now);
    moq_rcbuf_t *payload = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(moq_alloc_default(),
        (const uint8_t *)"x", 1, &payload), MOQ_OK);
    moq_pub_retained_object_t obj = { 0, payload, NULL, true };
    moq_pub_retained_group_cfg_t rg;
    moq_pub_retained_group_cfg_init(&rg);
    rg.group_id = 0;
    rg.objects = &obj;
    rg.object_count = 1;
    MOQ_TEST_CHECK_EQ_INT((int)moq_pub_set_retained_group(pub, t, &rg), MOQ_OK);
    moq_rcbuf_decref(payload);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    drain_events(cl);
    feed_publisher(pub, sv, now);

    moq_bytes_t part;
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = ns_live(&part);
    fc.track_name = MOQ_BYTES_LITERAL("v");
    fc.end_group = 0;
    fc.end_object = 1;
    moq_fetch_t h;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(cl, &fc, now, &h), MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    feed_publisher(pub, sv, now);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    uint8_t want[16];
    size_t want_len = pref_props(cl, DESC, want, sizeof(want));
    moq_event_t ev;
    bool ok = poll_for(cl, MOQ_EVENT_FETCH_OK, &ev);
    MOQ_TEST_CHECK(ok);
    if (ok) {
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_ok.track_properties.len, want_len);
        MOQ_TEST_CHECK(ev.u.fetch_ok.track_properties.len == want_len &&
                       memcmp(ev.u.fetch_ok.track_properties.data, want,
                              want_len) == 0);
        moq_event_cleanup(&ev);
    }
    moq_pub_destroy(pub);
    moq_simpair_destroy(sp);
}

/* [GO-10] TRACK_STATUS_OK carries the Track Properties a SUBSCRIBE_OK would
 * (draft-18). Draft-16's response has no field for them: refused locally. */
static void check_track_status(moq_version_t ver)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t part;
    moq_track_status_cfg_t tc;
    moq_track_status_cfg_init(&tc);
    tc.track_namespace = ns_live(&part);
    tc.track_name = MOQ_BYTES_LITERAL("v");
    moq_track_status_handle_t ch;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_status(cl, &tc, now, &ch),
                          MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    moq_event_t ev;
    bool got = poll_for(sv, MOQ_EVENT_TRACK_STATUS_REQUEST, &ev);
    MOQ_TEST_CHECK(got);
    if (!got) { moq_simpair_destroy(sp); return; }
    moq_track_status_handle_t sh = ev.u.track_status_request.handle;
    moq_event_cleanup(&ev);

    uint8_t props[16];
    size_t props_len = pref_props(sv, DESC, props, sizeof(props));
    moq_accept_track_status_cfg_t ac;
    moq_accept_track_status_cfg_init(&ac);
    ac.track_properties = (moq_bytes_t){ props, props_len };
    moq_result_t rc = moq_session_accept_track_status(sv, sh, &ac, now);
    MOQ_TEST_CHECK_EQ_INT((int)rc, is_d18(ver) ? MOQ_OK : MOQ_ERR_INVAL);
    if (is_d18(ver)) {
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        bool ok = poll_for(cl, MOQ_EVENT_TRACK_STATUS_OK, &ev);
        MOQ_TEST_CHECK(ok);
        if (ok) {
            MOQ_TEST_CHECK(ev.u.track_status_ok.track_properties.len ==
                               props_len &&
                           memcmp(ev.u.track_status_ok.track_properties.data,
                                  props, props_len) == 0);
            moq_event_cleanup(&ev);
        }
    }
    moq_simpair_destroy(sp);
}

/* [GV-04 / GV-05, sender side] The helper refuses a blob that already
 * carries the preference, and values other than the two orders. */
static void check_helper_refusals(moq_version_t ver)
{
    moq_simpair_t *sp = make_pair(ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *s = moq_simpair_server(sp);
    uint8_t one[16], two[32];
    size_t one_len = pref_props(s, ASC, one, sizeof(one)), two_len = 0;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_properties_add_group_order(
        s, one, one_len, DESC, two, sizeof(two), &two_len), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_properties_add_group_order(
        s, NULL, 0, NONE, two, sizeof(two), &two_len), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_track_properties_add_group_order(
        s, NULL, 0, 3, two, sizeof(two), &two_len), MOQ_ERR_INVAL);
    moq_simpair_destroy(sp);
}

static void test_emission(void)
{
    for (size_t v = 0; v < 2; v++) {
        moq_version_t ver = k_versions[v];
        check_facade_subscribe(ver, NONE);
        check_facade_subscribe(ver, ASC);
        check_facade_subscribe(ver, DESC);
        check_facade_publish(ver, false, NONE);
        check_facade_publish(ver, true, DESC);
        check_facade_publish(ver, true, ASC);
        check_facade_fetch_ok(ver);
        check_track_status(ver);
        check_helper_refusals(ver);
    }
    MOQ_TEST_PASS("group order preference emitted by the library");
}

int main(void)
{
    test_resolution();
    test_order_cannot_change();
    test_subscribe_group_order_values();
    test_request_values();
    test_response_values();
    test_subscribe_encoding();
    test_emission();
    if (failures) {
        fprintf(stderr, "test_group_order: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_group_order: all passed\n");
    return 0;
}
