/*
 * Draft-18 object/subgroup delivery through the deterministic SimPair harness:
 * SETUP over the unidirectional control-channel pair, SUBSCRIBE/accept on a
 * request bidi, then subgroup object delivery on a data stream. Also covers the
 * SimPair version seam (default = draft-16) and fail-closed parsing of an
 * unknown data stream.
 */
#include <moq/sim.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static moq_simpair_t *make_pair(moq_version_t version, bool truncate_version,
                                uint32_t fault_flags, uint32_t fault_per_mille)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = version;
    cfg.fault_flags = fault_flags;
    cfg.fault_per_mille = fault_per_mille;
    /* Simulate an older caller whose struct predates the version field. */
    if (truncate_version)
        cfg.struct_size = (uint32_t)offsetof(moq_simpair_cfg_t, version);
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) < 0) return NULL;
    return sp;
}

/* Build one even-type Key-Value-Pair (§1.4.3) in draft-18 vi64 form: a Delta
 * Type (here the absolute type, since it is the first/only entry) followed by a
 * vi64 value. `type` must be even. Returns the encoded length. */
static size_t build_kvp_varint(uint8_t *buf, size_t cap, uint64_t type,
                               uint64_t value)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_buf_write_vi64(&w, type);
    moq_buf_write_vi64(&w, value);
    return moq_buf_writer_offset(&w);
}

/* Establish, returning the server-side subscription handle for "live"/track. */
static bool setup_subscription(moq_simpair_t *sp, const char *track,
                               moq_subscription_t *out_server_sub,
                               uint64_t *out_client_alias)
{
    moq_session_t *client = moq_simpair_client(sp);
    moq_session_t *server = moq_simpair_server(sp);

    if (moq_simpair_start(sp) < 0) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    if (client->state != MOQ_SESS_ESTABLISHED ||
        server->state != MOQ_SESS_ESTABLISHED)
        return false;

    moq_subscribe_cfg_t sub;
    moq_subscribe_cfg_init(&sub);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    sub.track_namespace = ns;
    sub.track_name = (moq_bytes_t){ (const uint8_t *)track, strlen(track) };
    moq_subscription_t ch;
    if (moq_session_subscribe(client, &sub, 1, &ch) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    moq_subscription_t sh = {0};
    bool got = false;
    moq_event_t ev;
    while (moq_session_poll_events(server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            sh = ev.u.subscribe_request.sub;
            got = true;
        }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(server, sh, &acc,
                                     moq_simpair_now_us(sp)) != MOQ_OK)
        return false;
    moq_simpair_run_until_quiescent(sp, 16, NULL);

    bool ok = false;
    while (moq_session_poll_events(client, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            ok = true;
            if (out_client_alias) *out_client_alias = ev.u.subscribe_ok.track_alias;
        }
        moq_event_cleanup(&ev);
    }
    if (!ok) return false;

    *out_server_sub = sh;
    return true;
}

int main(void)
{
    int failures = 0;
    const moq_alloc_t *alloc = moq_alloc_default();

    /* == A. SimPair version seam: default / explicit D16 / explicit D18 == */
    {
        struct { moq_version_t v; bool trunc; } cases[] = {
            { (moq_version_t)0, true },              /* old struct -> D16 */
            { MOQ_VERSION_DRAFT_16, false },         /* explicit D16 */
            { MOQ_VERSION_DRAFT_18, false },         /* explicit D18 */
        };
        for (size_t i = 0; i < 3; i++) {
            moq_simpair_t *sp = make_pair(cases[i].v, cases[i].trunc, 0, 0);
            MOQ_TEST_CHECK(sp != NULL);
            MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
            MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_client(sp)->state,
                                  (int)MOQ_SESS_ESTABLISHED);
            MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_server(sp)->state,
                                  (int)MOQ_SESS_ESTABLISHED);
            moq_simpair_destroy(sp);
        }
    }

    /* == B. D18 object delivery: two objects on one subgroup =========== */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, NULL));

        moq_subgroup_cfg_t scfg;
        moq_subgroup_cfg_init(&scfg);
        scfg.group_id = 7;
        scfg.subgroup_id = 0;
        scfg.publisher_priority = 5;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(server, ssub, &scfg,
                moq_simpair_now_us(sp), &sg), (int)MOQ_OK);

        moq_rcbuf_t *p0 = NULL, *p1 = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"hello", 5, &p0);
        moq_rcbuf_create(alloc, (const uint8_t *)"world!", 6, &p1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_object(server, sg, 0, p0,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_object(server, sg, 1, p1,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(p1);
        moq_session_close_subgroup(server, sg, moq_simpair_now_us(sp));

        moq_simpair_run_until_quiescent(sp, 32, NULL);

        int count = 0;
        uint64_t seen_group = 0, seen_obj_max = 0;
        bool payload_ok = true;
        /* draft-18 also emits SUBGROUP_FINISHED on the subgroup's graceful FIN,
         * after both objects and with end_of_group=false (the header had no EOG
         * bit). Verify it arrives exactly once, after the last object. */
        int finished = 0, objs_before_finished = 0;
        moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                seen_group = o->group_id;
                if (o->object_id > seen_obj_max) seen_obj_max = o->object_id;
                size_t want = (o->object_id == 0) ? 5 : 6;
                const char *exp = (o->object_id == 0) ? "hello" : "world!";
                if (!o->payload || moq_rcbuf_len(o->payload) != want ||
                    memcmp(moq_rcbuf_data(o->payload), exp, want) != 0)
                    payload_ok = false;
                count++;
            } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
                if (finished == 0) objs_before_finished = count;
                finished++;
                MOQ_TEST_CHECK_EQ_U64(ev.u.subgroup_finished.group_id, 7);
                MOQ_TEST_CHECK(ev.u.subgroup_finished.end_of_group == false);
                MOQ_TEST_CHECK(moq_subscription_is_valid(ev.u.subgroup_finished.sub));
                MOQ_TEST_CHECK(!moq_publication_is_valid(ev.u.subgroup_finished.pub));
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(count, 2);
        MOQ_TEST_CHECK_EQ_INT(finished, 1);
        MOQ_TEST_CHECK_EQ_INT(objs_before_finished, 2);
        MOQ_TEST_CHECK_EQ_U64(seen_group, 7);
        MOQ_TEST_CHECK_EQ_U64(seen_obj_max, 1);
        MOQ_TEST_CHECK(payload_ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == C. Fragmented delivery: SPLIT_DATA on every data action ======= */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false,
                                      MOQ_SIM_FAULT_SPLIT_DATA, 1000);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, NULL));
        moq_simpair_enable_faults(sp);   /* split only after handshake */

        moq_subgroup_cfg_t scfg;
        moq_subgroup_cfg_init(&scfg);
        scfg.group_id = 3;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(server, ssub, &scfg,
                moq_simpair_now_us(sp), &sg), (int)MOQ_OK);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"fragmented-payload", 18, &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_object(server, sg, 0, p,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(p);
        moq_session_close_subgroup(server, sg, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        int count = 0;
        bool payload_ok = false;
        moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                if (o->payload && moq_rcbuf_len(o->payload) == 18 &&
                    memcmp(moq_rcbuf_data(o->payload),
                           "fragmented-payload", 18) == 0)
                    payload_ok = true;
                count++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(count, 1);
        MOQ_TEST_CHECK(payload_ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == D. Unknown data stream fails closed ========================== */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);

        /* 0x00 is not a valid subgroup type (bit 4 clear) -> UNKNOWN -> close. */
        uint8_t bad[] = { 0x00, 0x01, 0x02 };
        (void)moq_session_on_data_bytes(client, moq_stream_ref_from_u64(0x7777),
                                        bad, sizeof(bad), false,
                                        moq_simpair_now_us(sp));
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_CLOSED);
        moq_simpair_destroy(sp);
    }

    /* == E. Non-minimal vi64 subgroup type is classified as subgroup == *
     * Type 0x14 encoded as the 2-byte vi64 {0x80,0x14}; an unknown track alias
     * stops the stream but must NOT close the session (it is a valid subgroup,
     * not an unknown stream type). Tested whole and split byte-by-byte. */
    {
        /* track_alias 112 (unknown), group 0, subgroup 0, priority 0. */
        uint8_t hdr[] = { 0x80, 0x14, 0x70, 0x00, 0x00, 0x00 };

        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        (void)moq_session_on_data_bytes(client, moq_stream_ref_from_u64(0x8001),
                                        hdr, sizeof(hdr), false,
                                        moq_simpair_now_us(sp));
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);

        moq_simpair_t *sp2 = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp2 != NULL);
        moq_session_t *client2 = moq_simpair_client(sp2);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp2), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp2, 16, NULL);
        for (size_t i = 0; i < sizeof(hdr); i++) {
            (void)moq_session_on_data_bytes(client2,
                moq_stream_ref_from_u64(0x8002), &hdr[i], 1, false,
                moq_simpair_now_us(sp2));
            MOQ_TEST_CHECK_EQ_INT((int)client2->state,
                                  (int)MOQ_SESS_ESTABLISHED);
        }
        moq_simpair_destroy(sp2);
    }

    /* == F. open_subgroup with object_properties=true now succeeds ===== *
     * Object Properties are supported (the end-to-end round-trip is test H). */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, NULL));

        moq_subgroup_cfg_t scfg;
        moq_subgroup_cfg_init(&scfg);
        scfg.group_id = 1;
        scfg.object_properties = true;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_open_subgroup(server, ssub, &scfg,
            moq_simpair_now_us(sp), &sg), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == G. FETCH object delivery end-to-end ========================== */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 10;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_fetch(client, &fc, moq_simpair_now_us(sp), &fh),
            (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_t sfh = {0};
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                sfh = ev.u.fetch_request.fetch; got = true;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);

        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 10;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_fetch(server, sfh, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);

        moq_rcbuf_t *p0 = NULL, *p1 = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"aa", 2, &p0);
        moq_rcbuf_create(alloc, (const uint8_t *)"bbb", 3, &p1);
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.group_id = 0; oc.subgroup_id = 0; oc.object_id = 0; oc.payload = p0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        oc.object_id = 1; oc.payload = p1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(p0);
        moq_rcbuf_decref(p1);
        moq_session_end_fetch(server, sfh, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        int objs = 0; bool complete = false; bool payload_ok = true;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                moq_fetch_object_event_t *o = &ev.u.fetch_object;
                size_t want = (o->object_id == 0) ? 2 : 3;
                const char *exp = (o->object_id == 0) ? "aa" : "bbb";
                if (!o->payload || moq_rcbuf_len(o->payload) != want ||
                    memcmp(moq_rcbuf_data(o->payload), exp, want) != 0)
                    payload_ok = false;
                objs++;
            } else if (ev.kind == MOQ_EVENT_FETCH_COMPLETE) {
                complete = true;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(objs, 2);
        MOQ_TEST_CHECK(complete);
        MOQ_TEST_CHECK(payload_ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == H. Empty FETCH: FETCH_OK + immediate complete, no objects ==== */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 10;
        moq_fetch_t fh;
        moq_session_fetch(client, &fc, moq_simpair_now_us(sp), &fh);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_t sfh = {0};
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) sfh = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 10; ac.empty = true;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_fetch(server, sfh, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        int objs = 0; bool ok = false; bool complete = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) objs++;
            else if (ev.kind == MOQ_EVENT_FETCH_OK) ok = true;
            else if (ev.kind == MOQ_EVENT_FETCH_COMPLETE) complete = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK(complete);
        MOQ_TEST_CHECK_EQ_INT(objs, 0);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == I. Fragmented FETCH delivery (SPLIT_DATA) ==================== */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false,
                                      MOQ_SIM_FAULT_SPLIT_DATA, 1000);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_simpair_enable_faults(sp);

        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 10;
        moq_fetch_t fh;
        moq_session_fetch(client, &fc, moq_simpair_now_us(sp), &fh);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        moq_fetch_t sfh = {0};
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) sfh = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 10;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_fetch(server, sfh, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"fragmented-fetch", 16, &p);
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.payload = p;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(p);
        moq_session_end_fetch(server, sfh, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 128, NULL);

        int objs = 0; bool payload_ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                moq_fetch_object_event_t *o = &ev.u.fetch_object;
                if (o->payload && moq_rcbuf_len(o->payload) == 16 &&
                    memcmp(moq_rcbuf_data(o->payload), "fragmented-fetch", 16) == 0)
                    payload_ok = true;
                objs++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(objs, 1);
        MOQ_TEST_CHECK(payload_ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == H. Subgroup object Properties round-trip (D18) =============== *
     * A properties-bearing subgroup carries an opaque KVP block per object; the
     * subscriber surfaces the borrowed bytes verbatim alongside the payload. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, NULL));

        moq_subgroup_cfg_t scfg;
        moq_subgroup_cfg_init(&scfg);
        scfg.group_id = 9;
        scfg.object_properties = true;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(server, ssub, &scfg,
                moq_simpair_now_us(sp), &sg), (int)MOQ_OK);

        /* One valid even-type KVP entry (type 2, value 0x41). */
        uint8_t pb[16];
        size_t pblen = build_kvp_varint(pb, sizeof(pb), 2, 0x41);
        MOQ_TEST_CHECK(pblen > 0);
        moq_rcbuf_t *props = NULL, *pay = NULL;
        moq_rcbuf_create(alloc, pb, pblen, &props);
        moq_rcbuf_create(alloc, (const uint8_t *)"hi", 2, &pay);

        moq_object_cfg_t oc;
        moq_object_cfg_init(&oc);
        oc.object_id = 0;
        oc.payload = pay;
        oc.properties = props;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_object_ex(server, sg, &oc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(props);
        moq_rcbuf_decref(pay);
        moq_session_close_subgroup(server, sg, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        int count = 0; bool props_ok = false, payload_ok = false;
        moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                if (o->properties && moq_rcbuf_len(o->properties) == pblen &&
                    memcmp(moq_rcbuf_data(o->properties), pb, pblen) == 0)
                    props_ok = true;
                if (o->payload && moq_rcbuf_len(o->payload) == 2 &&
                    memcmp(moq_rcbuf_data(o->payload), "hi", 2) == 0)
                    payload_ok = true;
                count++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(count, 1);
        MOQ_TEST_CHECK(props_ok);
        MOQ_TEST_CHECK(payload_ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == I. Mandatory Track Property as an object property -> close ==== *
     * Type 0x4000-0x7FFF MUST be Track-scoped (§2.5.1); carried as an object
     * property the object is malformed -> PROTOCOL_VIOLATION on the subscriber.
     * (The local encoder refuses to emit this, so craft the wire on the client.) */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_subscription_t ssub;
        uint64_t alias = 0;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, &alias));

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d18_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.has_properties = true;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_subgroup_header(&w, &shdr),
                              (int)MOQ_OK);
        uint8_t pb[16];
        size_t pblen = build_kvp_varint(pb, sizeof(pb), 0x4000, 0);  /* mandatory */
        /* object: delta=0, props(len+bytes), payload_len=1, "x". */
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, pblen);
        moq_buf_write_raw(&w, pb, pblen);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_raw(&w, (const uint8_t *)"x", 1);
        (void)moq_session_on_data_bytes(client, moq_stream_ref_from_u64(0xD18E),
            wire, moq_buf_writer_offset(&w), false, moq_simpair_now_us(sp));
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_CLOSED);
        moq_simpair_destroy(sp);
    }

    /* == J. Properties on a non-Normal object -> close ================ *
     * Properties are valid only on a Normal object (§11.2.1.2). Craft a status
     * object (payload_len 0, status END_OF_GROUP 0x3) carrying a property block
     * on the subscriber's data stream; the alias is the one bound at SUBSCRIBE_OK. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_subscription_t ssub;
        uint64_t alias = 0;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, &alias));

        uint8_t wire[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d18_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.has_properties = true;
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
        shdr.track_alias = alias;
        shdr.group_id = 0;
        shdr.subgroup_id = 0;
        shdr.publisher_priority = 128;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_subgroup_header(&w, &shdr),
                              (int)MOQ_OK);
        uint8_t pb[16];
        size_t pblen = build_kvp_varint(pb, sizeof(pb), 2, 0x41);
        /* object: delta=0, props(len+bytes), payload_len=0, status=0x3 (EOG). */
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, pblen);
        moq_buf_write_raw(&w, pb, pblen);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 0x3);
        (void)moq_session_on_data_bytes(client, moq_stream_ref_from_u64(0xD18D),
            wire, moq_buf_writer_offset(&w), false, moq_simpair_now_us(sp));
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_CLOSED);
        moq_simpair_destroy(sp);
    }

    /* == K. Fetch object Properties round-trip (D18) ================== */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);

        if (moq_simpair_start(sp) < 0) { MOQ_TEST_CHECK(0); }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(client, &fc, 1, &fh),
                              (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_t sfh = {0};
        moq_event_t ev;
        bool got = false;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sfh = ev.u.fetch_request.fetch; got = true; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_accept_fetch_cfg_t afc;
        moq_accept_fetch_cfg_init(&afc);
        afc.end_group = 1; afc.end_object = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_fetch(server, sfh, &afc, moq_simpair_now_us(sp)),
            (int)MOQ_OK);

        uint8_t pb[16];
        size_t pblen = build_kvp_varint(pb, sizeof(pb), 2, 0x41);
        moq_rcbuf_t *props = NULL, *pay = NULL;
        moq_rcbuf_create(alloc, pb, pblen, &props);
        moq_rcbuf_create(alloc, (const uint8_t *)"fp", 2, &pay);
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.payload = pay;
        oc.properties = props;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(props);
        moq_rcbuf_decref(pay);
        moq_session_end_fetch(server, sfh, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        int objs = 0; bool props_ok = false, payload_ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                moq_fetch_object_event_t *o = &ev.u.fetch_object;
                if (o->properties && moq_rcbuf_len(o->properties) == pblen &&
                    memcmp(moq_rcbuf_data(o->properties), pb, pblen) == 0)
                    props_ok = true;
                if (o->payload && moq_rcbuf_len(o->payload) == 2 &&
                    memcmp(moq_rcbuf_data(o->payload), "fp", 2) == 0)
                    payload_ok = true;
                objs++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(objs, 1);
        MOQ_TEST_CHECK(props_ok);
        MOQ_TEST_CHECK(payload_ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == L. D18 object/fetch property encode rolls back on a short buffer = *
     * The codec convention is transactional: a partial encode that runs out of
     * room must leave the writer offset unchanged. Drive the profile encode ops
     * directly (via the vtable) with a 1-byte buffer that fits the delta but not
     * the properties length. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *srv = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        uint8_t tiny[1];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, tiny, sizeof(tiny));
        moq_object_header_encode_args_t oa;
        memset(&oa, 0, sizeof(oa));
        oa.object_id = 5;          /* 1-byte delta fills the buffer */
        oa.has_extensions = true;
        oa.properties_len = 10;    /* the properties length no longer fits */
        oa.header_only = true;
        MOQ_TEST_CHECK(srv->profile->encode_object_header(srv, &w, &oa) < 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);

        moq_buf_writer_init(&w, tiny, sizeof(tiny));
        moq_fetch_object_encode_args_t fa;
        memset(&fa, 0, sizeof(fa));
        fa.group_id = 0; fa.object_id = 0; fa.publisher_priority = 0;
        fa.properties_len = 10;
        fa.header_only = true;
        MOQ_TEST_CHECK(
            srv->profile->encode_fetch_object(srv, &w, &fa, NULL) < 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);

        moq_simpair_destroy(sp);
    }

    /* == M. Subgroup object property round-trip, payload length 64..127 == *
     * Regression: the two-action property path's second fragment (payload length)
     * must use draft-18 vi64, not the QUIC varint. The two encodings diverge at
     * 64..127, so a 100-byte payload would corrupt/close if the wrong width is
     * used. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, NULL));

        moq_subgroup_cfg_t scfg;
        moq_subgroup_cfg_init(&scfg);
        scfg.group_id = 4;
        scfg.object_properties = true;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(server, ssub, &scfg,
                moq_simpair_now_us(sp), &sg), (int)MOQ_OK);

        uint8_t pb[16];
        size_t pblen = build_kvp_varint(pb, sizeof(pb), 2, 0x41);
        uint8_t big[100];
        for (int i = 0; i < 100; i++) big[i] = (uint8_t)(i + 1);
        moq_rcbuf_t *props = NULL, *pay = NULL;
        moq_rcbuf_create(alloc, pb, pblen, &props);
        moq_rcbuf_create(alloc, big, sizeof(big), &pay);
        moq_object_cfg_t oc;
        moq_object_cfg_init(&oc);
        oc.object_id = 0;
        oc.payload = pay;
        oc.properties = props;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_object_ex(server, sg, &oc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(props);
        moq_rcbuf_decref(pay);
        moq_session_close_subgroup(server, sg, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        int count = 0; bool ok = false;
        moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                ok = o->payload && moq_rcbuf_len(o->payload) == 100 &&
                     memcmp(moq_rcbuf_data(o->payload), big, 100) == 0 &&
                     o->properties && moq_rcbuf_len(o->properties) == pblen;
                count++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(count, 1);
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == N. Fetch object property round-trip, payload length 64..127 ==== */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        if (moq_simpair_start(sp) < 0) { MOQ_TEST_CHECK(0); }
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_cfg_t fc;
        moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(client, &fc, 1, &fh),
                              (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_fetch_t sfh = {0};
        moq_event_t ev; bool got = false;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sfh = ev.u.fetch_request.fetch; got = true; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_accept_fetch_cfg_t afc;
        moq_accept_fetch_cfg_init(&afc);
        afc.end_group = 1; afc.end_object = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_fetch(server, sfh, &afc, moq_simpair_now_us(sp)),
            (int)MOQ_OK);

        uint8_t pb[16];
        size_t pblen = build_kvp_varint(pb, sizeof(pb), 2, 0x41);
        uint8_t big[100];
        for (int i = 0; i < 100; i++) big[i] = (uint8_t)(0xA0 ^ i);
        moq_rcbuf_t *props = NULL, *pay = NULL;
        moq_rcbuf_create(alloc, pb, pblen, &props);
        moq_rcbuf_create(alloc, big, sizeof(big), &pay);
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.payload = pay;
        oc.properties = props;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(props);
        moq_rcbuf_decref(pay);
        moq_session_end_fetch(server, sfh, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        int objs = 0; bool ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                moq_fetch_object_event_t *o = &ev.u.fetch_object;
                ok = o->payload && moq_rcbuf_len(o->payload) == 100 &&
                     memcmp(moq_rcbuf_data(o->payload), big, 100) == 0 &&
                     o->properties && moq_rcbuf_len(o->properties) == pblen;
                objs++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(objs, 1);
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == O. Outbound mandatory object property is refused (D18) ========= *
     * A local publisher must not emit a Mandatory Track Property (0x4000-0x7FFF)
     * as an object property -- inbound treats that as malformed, so the write is
     * rejected rather than putting unreadable wire on the network. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, NULL));

        moq_subgroup_cfg_t scfg;
        moq_subgroup_cfg_init(&scfg);
        scfg.group_id = 2;
        scfg.object_properties = true;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(server, ssub, &scfg,
                moq_simpair_now_us(sp), &sg), (int)MOQ_OK);

        uint8_t pb[16];
        size_t pblen = build_kvp_varint(pb, sizeof(pb), 0x4000, 0);
        moq_rcbuf_t *props = NULL, *pay = NULL;
        moq_rcbuf_create(alloc, pb, pblen, &props);
        moq_rcbuf_create(alloc, (const uint8_t *)"x", 1, &pay);
        moq_object_cfg_t oc;
        moq_object_cfg_init(&oc);
        oc.object_id = 0;
        oc.payload = pay;
        oc.properties = props;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_object_ex(server, sg, &oc,
                moq_simpair_now_us(sp)), (int)MOQ_ERR_INVAL);
        moq_rcbuf_decref(props);
        moq_rcbuf_decref(pay);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Outbound D18 fetch object: Datagram forwarding preference (0x40) === *
     * A default cfg surfaces datagram=false; a cfg with datagram=true emits the
     * 0x40 bit, ignores subgroup_id, and surfaces datagram=true with subgroup 0. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(client, &fc, 1, &fh), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_t sfh = {0}; moq_event_t ev; bool got = false;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sfh = ev.u.fetch_request.fetch; got = true; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_accept_fetch_cfg_t afc; moq_accept_fetch_cfg_init(&afc);
        afc.end_group = 1; afc.end_object = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_fetch(server, sfh, &afc, moq_simpair_now_us(sp)),
            (int)MOQ_OK);

        /* Object 0: default cfg (datagram unset) -> normal object. */
        moq_rcbuf_t *pa = NULL, *pb_ = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"a", 1, &pa);
        moq_fetch_object_cfg_t oc0; moq_fetch_object_cfg_init(&oc0);
        oc0.group_id = 0; oc0.object_id = 0; oc0.payload = pa;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc0,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        /* Object 1: datagram=true with a nonzero subgroup_id (must be ignored). */
        moq_rcbuf_create(alloc, (const uint8_t *)"b", 1, &pb_);
        moq_fetch_object_cfg_t oc1; moq_fetch_object_cfg_init(&oc1);
        oc1.group_id = 0; oc1.object_id = 1; oc1.subgroup_id = 99;
        oc1.datagram = true; oc1.payload = pb_;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc1,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(pa); moq_rcbuf_decref(pb_);
        moq_session_end_fetch(server, sfh, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        int objs = 0; bool normal_ok = false, dgram_ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                moq_fetch_object_event_t *o = &ev.u.fetch_object;
                if (o->object_id == 0 && !o->datagram) normal_ok = true;
                if (o->object_id == 1 && o->datagram && o->subgroup_id == 0)
                    dgram_ok = true;
                objs++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(objs, 2);
        MOQ_TEST_CHECK(normal_ok);
        MOQ_TEST_CHECK(dgram_ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Outbound D18 datagram object carrying Object Properties round-trips = */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(client, &fc, 1, &fh), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_fetch_t sfh = {0}; moq_event_t ev; bool got = false;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sfh = ev.u.fetch_request.fetch; got = true; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_accept_fetch_cfg_t afc; moq_accept_fetch_cfg_init(&afc);
        afc.end_group = 1; afc.end_object = 0;
        moq_session_accept_fetch(server, sfh, &afc, moq_simpair_now_us(sp));

        uint8_t pbuf[16];
        size_t pblen = build_kvp_varint(pbuf, sizeof(pbuf), 2, 0x41);
        moq_rcbuf_t *props = NULL, *pay = NULL;
        moq_rcbuf_create(alloc, pbuf, pblen, &props);
        moq_rcbuf_create(alloc, (const uint8_t *)"dp", 2, &pay);
        moq_fetch_object_cfg_t oc; moq_fetch_object_cfg_init(&oc);
        oc.datagram = true; oc.payload = pay; oc.properties = props;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(props); moq_rcbuf_decref(pay);
        moq_session_end_fetch(server, sfh, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        int objs = 0; bool ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                moq_fetch_object_event_t *o = &ev.u.fetch_object;
                ok = o->datagram && o->subgroup_id == 0 &&
                     o->properties && moq_rcbuf_len(o->properties) == pblen &&
                     memcmp(moq_rcbuf_data(o->properties), pbuf, pblen) == 0 &&
                     o->payload && moq_rcbuf_len(o->payload) == 2;
                objs++;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(objs, 1);
        MOQ_TEST_CHECK(ok);
        moq_simpair_destroy(sp);
    }

    /* == Prior-subgroup state after a datagram object ================== *
     * A datagram object has no subgroup (the peer records prior subgroup 0). A
     * following non-datagram object reusing the datagram object's (ignored)
     * subgroup_id must still arrive with that subgroup -- i.e. the sender must not
     * delta-encode it as "prior subgroup" against a stale nonzero value. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_fetch_cfg_t fc; moq_fetch_cfg_init(&fc);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        fc.track_namespace = (moq_namespace_t){ parts, 1 };
        fc.track_name = MOQ_BYTES_LITERAL("video");
        fc.end_group = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_fetch(client, &fc, 1, &fh), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        moq_fetch_t sfh = {0}; moq_event_t ev; bool got = false;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sfh = ev.u.fetch_request.fetch; got = true; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_accept_fetch_cfg_t afc; moq_accept_fetch_cfg_init(&afc);
        afc.end_group = 1; afc.end_object = 0;
        moq_session_accept_fetch(server, sfh, &afc, moq_simpair_now_us(sp));

        /* Object 0: datagram, with a nonzero (ignored) subgroup_id. */
        moq_rcbuf_t *pa = NULL, *pb_ = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"a", 1, &pa);
        moq_fetch_object_cfg_t oc0; moq_fetch_object_cfg_init(&oc0);
        oc0.group_id = 0; oc0.object_id = 0; oc0.subgroup_id = 99; oc0.datagram = true;
        oc0.payload = pa;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc0,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        /* Object 1: non-datagram, same subgroup_id 99 -> must arrive as subgroup 99. */
        moq_rcbuf_create(alloc, (const uint8_t *)"b", 1, &pb_);
        moq_fetch_object_cfg_t oc1; moq_fetch_object_cfg_init(&oc1);
        oc1.group_id = 0; oc1.object_id = 1; oc1.subgroup_id = 99; oc1.payload = pb_;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_fetch_object(server, sfh, &oc1,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(pa); moq_rcbuf_decref(pb_);
        moq_session_end_fetch(server, sfh, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 64, NULL);

        bool dgram_ok = false, sub_ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
                moq_fetch_object_event_t *o = &ev.u.fetch_object;
                if (o->object_id == 0 && o->datagram && o->subgroup_id == 0)
                    dgram_ok = true;
                if (o->object_id == 1 && !o->datagram && o->subgroup_id == 99)
                    sub_ok = true;
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(dgram_ok);
        MOQ_TEST_CHECK(sub_ok);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == draft-16 has no fetch-object Datagram bit: encode rejects datagram=true,
     *    while draft-18 accepts it (drive the profile encode op directly). ==== */
    {
        moq_simpair_t *sp16 = make_pair(MOQ_VERSION_DRAFT_16, false, 0, 0);
        moq_simpair_t *sp18 = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp16 != NULL && sp18 != NULL);
        moq_session_t *s16 = moq_simpair_server(sp16);
        moq_session_t *s18 = moq_simpair_server(sp18);

        moq_fetch_object_encode_args_t fa;
        memset(&fa, 0, sizeof(fa));
        fa.group_id = 0; fa.object_id = 0; fa.publisher_priority = 0;
        fa.datagram = true; fa.payload_len = 1;

        uint8_t buf[32]; moq_buf_writer_t w;
        /* draft-16: no such bit -> MOQ_ERR_INVAL, nothing written. */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)s16->profile->encode_fetch_object(s16, &w, &fa, NULL),
            (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_writer_offset(&w), 0);

        /* draft-18: accepted, and the encoded flags carry the 0x40 Datagram bit. */
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)s18->profile->encode_fetch_object(s18, &w, &fa, NULL), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        uint64_t flags = 0;
        MOQ_TEST_CHECK(moq_buf_read_vi64(&r, &flags) >= 0);
        MOQ_TEST_CHECK((flags & 0x40u) != 0);
        MOQ_TEST_CHECK((flags & 0x03u) == 0);   /* subgroup LSBs zero */

        moq_simpair_destroy(sp16);
        moq_simpair_destroy(sp18);
    }

    /* == Reliable END_OF_TRACK over D18: write_status_object round-trip ===
     * Pins the D18 object-header encoder for a zero-length terminal status
     * object (the new moq_session_write_status_object path routes through the
     * D18 profile hook, not just D16): a NORMAL object then a terminal
     * END_OF_TRACK must arrive over the real SimPair wire with the status
     * surfaced and a NULL payload, and the session must stay ESTABLISHED. */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        moq_subscription_t ssub;
        MOQ_TEST_CHECK(setup_subscription(sp, "eot", &ssub, NULL));

        moq_subgroup_cfg_t scfg;
        moq_subgroup_cfg_init(&scfg);
        scfg.group_id = 3;
        scfg.subgroup_id = 0;
        scfg.publisher_priority = 5;
        moq_subgroup_handle_t sg;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(server, ssub, &scfg,
                moq_simpair_now_us(sp), &sg), (int)MOQ_OK);

        moq_rcbuf_t *p0 = NULL;
        moq_rcbuf_create(alloc, (const uint8_t *)"frame", 5, &p0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_object(server, sg, 0, p0,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_rcbuf_decref(p0);
        /* Terminal END_OF_TRACK (object 1, zero-length). NORMAL is rejected. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_status_object(server, sg, 2,
                MOQ_OBJECT_NORMAL, moq_simpair_now_us(sp)), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_write_status_object(server, sg, 1,
                MOQ_OBJECT_END_OF_TRACK, moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_session_close_subgroup(server, sg, moq_simpair_now_us(sp));

        moq_simpair_run_until_quiescent(sp, 32, NULL);

        int normal_count = 0, eot_count = 0;
        bool eot_null_payload = true;
        moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                moq_object_received_event_t *o = &ev.u.object_received;
                if (o->status == MOQ_OBJECT_END_OF_TRACK) {
                    eot_count++;
                    if (o->payload != NULL) eot_null_payload = false;
                } else if (o->status == MOQ_OBJECT_NORMAL) {
                    normal_count++;
                }
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(normal_count, 1);
        MOQ_TEST_CHECK_EQ_INT(eot_count, 1);
        MOQ_TEST_CHECK(eot_null_payload);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == K. FIRST_OBJECT-mode subgroup, empty + FIN -> no SUBGROUP_FINISHED == *
     * A first-object-mode header whose stream FINs with no object never resolves
     * its subgroup ID, so no SUBGROUP_FINISHED is fabricated (draft-18). */
    {
        moq_simpair_t *sp = make_pair(MOQ_VERSION_DRAFT_18, false, 0, 0);
        MOQ_TEST_CHECK(sp != NULL);
        moq_session_t *client = moq_simpair_client(sp);
        moq_subscription_t ssub;
        uint64_t alias = 0;
        MOQ_TEST_CHECK(setup_subscription(sp, "video", &ssub, &alias));

        uint8_t wire[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, wire, sizeof(wire));
        moq_d18_subgroup_header_t shdr;
        memset(&shdr, 0, sizeof(shdr));
        shdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_FIRST_OBJ;
        shdr.track_alias = alias;
        shdr.group_id = 5;
        shdr.publisher_priority = 128;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_subgroup_header(&w, &shdr),
                              (int)MOQ_OK);

        /* Header then a bare FIN (no object). */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_data_bytes(client,
            moq_stream_ref_from_u64(0xD18F), wire, moq_buf_writer_offset(&w),
            true, moq_simpair_now_us(sp)), (int)MOQ_OK);

        int sgf = 0;
        moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) sgf++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(sgf, 0);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_object");
    return failures != 0;
}
