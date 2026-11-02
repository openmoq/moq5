#include "test_cases.h"

/* Declared, non-default acceptance values. */
#define FU_TRACK_ALIAS    0x51

#define FU_LARGEST_GROUP  0x22

#define FU_LARGEST_OBJECT 0x07

#define FU_EXPIRES_MS     9000


/* Captured at arm time, in the POOL order sub_cancel_subgroups scans, since
 * that is the order the cancellations are queued in. */
moq_stream_ref_t g_fu_sg_refs[FU_SUBGROUPS];

size_t g_fu_sg_count;


size_t enc_subscribe_pub_role(uint8_t *buf, size_t cap, uint64_t rid)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_msg_params_t p = { 0 };
    /* A subscriber-declared DELIVERY_TIMEOUT. The completed-stream wrapper
     * checks exact FIN-started deadlines; unfinished streams have none. */
    p.has_subgroup_delivery_timeout = true;
    p.subgroup_delivery_timeout_ms = 5000;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_subscribe(&w, rid, &ns, MOQ_BYTES_LITERAL("v0"), &p),
        (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


/* A REQUEST_UPDATE that is well-formed on the wire and fails on its CONTENT:
 * one USE_ALIAS naming an alias the peer never registered. A malformed message
 * would close the session at the decoder and never reach this terminal. */
size_t enc_update_unknown_alias(uint8_t *buf, size_t cap)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_d18_msg_params_t p = { 0 };
    p.auth_token_count = 1;
    p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
    p.auth_tokens[0].alias = 99;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_request_update(&w, 2, &p), (int)MOQ_OK);
    return moq_buf_writer_offset(&w);
}


/* An ESTABLISHED publisher-role subscription with FU_SUBGROUPS open data
 * streams, all through the public API: the peer's SUBSCRIBE arrives as request
 * bytes, the application accepts it, and the application opens the subgroups. */
/* Per-subgroup identity the setup declares and the terminal must preserve. */
uint64_t g_fu_sg_group[FU_SUBGROUPS];

static uint64_t g_fu_sg_payload[FU_SUBGROUPS];

static uint64_t g_fu_sg_written[FU_SUBGROUPS];

static uint8_t  g_fu_sg_priority[FU_SUBGROUPS];

static bool g_fu_completed;


/* Two lawful shapes cover distinct cleanup duties: unfinished objects have
 * nonzero streaming counters but no subgroup timer; completed subgroups have
 * finite, distinct FIN-started timers. Both use the public send APIs. */
static moq_stream_ref_t arm_failed_update_subscription_shape(moq_session_t *s,
                                                            bool completed)
{
    g_fu_completed = completed;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
    uint8_t msg[192];
    size_t n = enc_subscribe_pub_role(msg, sizeof(msg), 0);
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
        (int)MOQ_OK);

    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    {
        int req = 0, other = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) {
            if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                req++; sub = e.u.subscribe_request.sub;
            } else {
                other++;
                TXS_DIAG("TXN failed-update arm: unexpected event kind %u\n",
                         (unsigned)e.kind);
            }
            moq_event_cleanup(&e);
        }
        MOQ_TEST_CHECK_EQ_INT(req, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Explicit non-default values, so the response is compared against what
     * the fixture ASKED for rather than against whatever the session chose. */
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    acfg.has_track_alias = true;
    acfg.track_alias = FU_TRACK_ALIAS;
    acfg.has_largest = true;
    acfg.largest_group = FU_LARGEST_GROUP;
    acfg.largest_object = FU_LARGEST_OBJECT;
    acfg.has_expires = true;
    acfg.expires_ms = FU_EXPIRES_MS;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, sub, &acfg, 1),
                          (int)MOQ_OK);

    /* The acceptance owes exactly one SUBSCRIBE_OK on the request bidi -- the
     * subscribe family answers with its own message, not the generic
     * REQUEST_OK -- with our send half left open, a decodable payload, and
     * nothing after the envelope. */
    {
        int oks = 0, other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v) {
                oks++;
                MOQ_TEST_CHECK_EQ_INT(a.u.send_bidi_stream.fin ? 1 : 0, 0);
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                                      (uint64_t)MOQ_D18_SUBSCRIBE_OK);
                MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&rr),
                                       (size_t)0);
                moq_d18_subscribe_ok_t ok;
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_d18_decode_subscribe_ok(env.payload,
                                                     env.payload_len, &ok),
                    (int)MOQ_OK);
                /* Its values against the DECLARED accept configuration, and
                 * the same values against the live entry. */
                MOQ_TEST_CHECK_EQ_U64(ok.track_alias, FU_TRACK_ALIAS);
                MOQ_TEST_CHECK_EQ_INT(ok.params.has_largest ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(ok.params.largest_group,
                                      FU_LARGEST_GROUP);
                MOQ_TEST_CHECK_EQ_U64(ok.params.largest_object,
                                      FU_LARGEST_OBJECT);
                MOQ_TEST_CHECK_EQ_INT(ok.params.has_expires ? 1 : 0, 1);
                MOQ_TEST_CHECK_EQ_U64(ok.params.expires_ms, FU_EXPIRES_MS);
                MOQ_TEST_CHECK_EQ_INT(ok.track_properties_unsupported ? 1 : 0,
                                      0);
                MOQ_TEST_CHECK_EQ_INT(ok.dynamic_groups ? 1 : 0, 0);
                MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, (size_t)0);
                MOQ_TEST_CHECK_EQ_SIZE(ok.params.auth_token_count, (size_t)0);
                {
                    const moq_sub_entry_t *le = sub_entry_by_ref(s, ref);
                    MOQ_TEST_CHECK(le != NULL);
                    if (le) MOQ_TEST_CHECK_EQ_U64(le->track_alias,
                                                  FU_TRACK_ALIAS);
                }
            } else {
                other++;
                TXS_DIAG("TXN failed-update arm: unexpected accept action %u\n",
                         (unsigned)a.kind);
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(oks, 1);
        MOQ_TEST_CHECK_EQ_INT(other, 0);
    }

    /* Opening a subgroup does not start its negotiated delivery timer. */
    moq_subgroup_handle_t sgh[FU_SUBGROUPS];
    for (int i = 0; i < FU_SUBGROUPS; i++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = (uint64_t)(10 + i);
        sgc.subgroup_id = 0;
        /* Distinct and NON-default, so a header that fell back to the
         * default-priority encoding cannot pass. */
        sgc.publisher_priority = (uint8_t)(17 + i);
        g_fu_sg_group[i] = sgc.group_id;
        g_fu_sg_priority[i] = sgc.publisher_priority;
        sgh[i] = MOQ_SUBGROUP_INVALID;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_open_subgroup(s, sub, &sgc, (uint64_t)(2 + i),
                                           &sgh[i]), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_subgroup_is_valid(sgh[i]));
    }

    /* The transport refs, in the pool order the reset loop scans -- which is
     * what fixes the order of the cancellations it queues. */
    g_fu_sg_count = 0;
    for (size_t i = 0; i < s->sg_cap; i++) {
        if (s->subgroups[i].state == MOQ_SG_FREE) continue;
        if (!moq_subscription_eq(s->subgroups[i].sub, sub)) continue;
        MOQ_TEST_CHECK(g_fu_sg_count < FU_SUBGROUPS);
        if (g_fu_sg_count < FU_SUBGROUPS)
            g_fu_sg_refs[g_fu_sg_count] = s->subgroups[i].stream_ref;
        g_fu_sg_count++;
    }
    MOQ_TEST_CHECK_EQ_SIZE(g_fu_sg_count, (size_t)FU_SUBGROUPS);

    /* The opens are a BIJECTION onto those refs: exactly one write per ref,
     * none missing, none repeated, each carrying that subgroup's own header
     * with our send half open. */
    {
        int seen[FU_SUBGROUPS] = { 0 };
        int other = 0;
        /* Every header must carry the DECLARED alias -- not whatever the live
         * entry ended up holding, which an open-path mutation could move in
         * step with the wire. The entry is reasserted against the same
         * constant separately. */
        const uint64_t alias = FU_TRACK_ALIAS;
        {
            const moq_sub_entry_t *ae = sub_entry_by_ref(s, ref);
            MOQ_TEST_CHECK(ae != NULL);
            if (ae) MOQ_TEST_CHECK_EQ_U64(ae->track_alias, FU_TRACK_ALIAS);
        }
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            if (a.kind != MOQ_ACTION_SEND_DATA) {
                other++;
                moq_action_cleanup(&a);
                continue;
            }
            int idx = -1;
            for (size_t k = 0; k < g_fu_sg_count; k++)
                if (g_fu_sg_refs[k]._v == a.u.send_data.stream_ref._v)
                    idx = (int)k;
            MOQ_TEST_CHECK(idx >= 0);
            if (idx < 0) { other++; moq_action_cleanup(&a); continue; }
            seen[idx]++;
            MOQ_TEST_CHECK_EQ_INT(a.u.send_data.fin ? 1 : 0, 0);
            moq_buf_reader_t hr;
            moq_buf_reader_init(&hr, a.u.send_data.header,
                                a.u.send_data.header_len);
            moq_d18_subgroup_header_t hdr;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_subgroup_header(&hr, &hdr), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&hr), (size_t)0);
            MOQ_TEST_CHECK(moq_d18_subgroup_type_valid(hdr.type));
            MOQ_TEST_CHECK_EQ_U64(hdr.group_id, g_fu_sg_group[idx]);
            MOQ_TEST_CHECK_EQ_U64(hdr.subgroup_id, 0);
            /* The publisher spells the subgroup id out on the wire (mode 2)
             * rather than relying on the zero-mode shorthand. */
            MOQ_TEST_CHECK_EQ_INT(hdr.subgroup_id_mode, 2);
            MOQ_TEST_CHECK_EQ_INT(hdr.has_properties ? 1 : 0, 0);
            MOQ_TEST_CHECK_EQ_INT(hdr.end_of_group ? 1 : 0, 0);
            MOQ_TEST_CHECK_EQ_INT(hdr.first_object ? 1 : 0, 0);
            /* Spelled out, and exactly the value asked for. */
            MOQ_TEST_CHECK_EQ_INT(hdr.default_priority ? 1 : 0, 0);
            MOQ_TEST_CHECK_EQ_INT((int)hdr.publisher_priority,
                                  (int)g_fu_sg_priority[idx]);
            /* The alias is the declared one, on every stream. */
            MOQ_TEST_CHECK_EQ_U64(hdr.track_alias, alias);
            /* A header-only write: no object bytes ride the open. */
            MOQ_TEST_CHECK(a.u.send_data.payload == NULL);
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        for (size_t k = 0; k < g_fu_sg_count; k++)
            MOQ_TEST_CHECK_EQ_INT(seen[k], 1);
    }

    /* Leave each stream mid-object: a declared payload length with only part
     * of it written, so streaming_payload_len and streaming_bytes_written are
     * both non-zero and differ per stream. */
    for (int i = 0; i < FU_SUBGROUPS; i++) {
        static const uint8_t body[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
        g_fu_sg_payload[i] = (uint64_t)(16 + i);
        g_fu_sg_written[i] = (uint64_t)(4 + i);
        moq_begin_object_cfg_t bc;
        moq_begin_object_cfg_init(&bc);
        bc.object_id = 0;
        bc.payload_length = g_fu_sg_payload[i];
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_begin_object_ex(s, sgh[i], &bc, 3), (int)MOQ_OK);
        moq_rcbuf_t *chunk = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_rcbuf_create(moq_alloc_default(), body,
                                  (size_t)g_fu_sg_written[i], &chunk),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(chunk != NULL);
        if (chunk) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_write_object_data(s, sgh[i], chunk, 3),
                (int)MOQ_OK);
            moq_rcbuf_decref(chunk);
        }
    }
    /* Exactly the object header and the one payload write per stream. */
    {
        static const uint8_t body[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
        int hdrs[FU_SUBGROUPS] = { 0 }, pays[FU_SUBGROUPS] = { 0 };
        int other = 0;
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) {
            int idx = -1;
            if (a.kind == MOQ_ACTION_SEND_DATA)
                for (size_t k = 0; k < g_fu_sg_count; k++)
                    if (g_fu_sg_refs[k]._v == a.u.send_data.stream_ref._v)
                        idx = (int)k;
            if (idx < 0) { other++; moq_action_cleanup(&a); continue; }
            MOQ_TEST_CHECK_EQ_INT(a.u.send_data.fin ? 1 : 0, 0);
            if (a.u.send_data.header_len > 0) {
                /* The object header: no payload rides with it, and its bytes
                 * are exactly the declared object -- id 0 with this stream's
                 * declared payload length. There is no public object-header
                 * decoder, so the comparison is against a re-encode through
                 * the same profile op the session used. */
                hdrs[idx]++;
                MOQ_TEST_CHECK(a.u.send_data.payload == NULL);
                uint8_t want[32];
                moq_buf_writer_t hw;
                moq_buf_writer_init(&hw, want, sizeof(want));
                moq_object_header_encode_args_t oa;
                memset(&oa, 0, sizeof(oa));
                oa.object_id = 0;
                oa.payload_len = g_fu_sg_payload[idx];
                MOQ_TEST_CHECK_EQ_INT(
                    (int)s->profile->encode_object_header(
                        (moq_session_t *)s, &hw, &oa), (int)MOQ_OK);
                failures += txs_check_span_bytes(
                    a.u.send_data.header, (size_t)a.u.send_data.header_len,
                    want, moq_buf_writer_offset(&hw),
                    "failed-update object header");
            } else {
                /* The payload write: exactly the bytes handed in. */
                pays[idx]++;
                MOQ_TEST_CHECK(a.u.send_data.payload != NULL);
                if (a.u.send_data.payload) {
                    failures += txs_check_span_bytes(
                        moq_rcbuf_data(a.u.send_data.payload),
                        moq_rcbuf_len(a.u.send_data.payload), body,
                        (size_t)g_fu_sg_written[idx],
                        "failed-update payload rcbuf");
                }
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK_EQ_INT(other, 0);
        for (size_t k = 0; k < g_fu_sg_count; k++) {
            MOQ_TEST_CHECK_EQ_INT(hdrs[k], 1);
            MOQ_TEST_CHECK_EQ_INT(pays[k], 1);
        }
    }
    {
        int n_ev = 0;
        moq_event_t e;
        while (moq_session_poll_events(s, &e, 1) > 0) { n_ev++; moq_event_cleanup(&e); }
        MOQ_TEST_CHECK_EQ_INT(n_ev, 0);
    }

    if (completed) {
        static const uint8_t tail[16] = {
            9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
        };
        for (size_t i = 0; i < g_fu_sg_count; i++) {
            const size_t remaining = (size_t)(g_fu_sg_payload[i] - g_fu_sg_written[i]);
            const uint64_t finish_time = 4 + i;
            moq_rcbuf_t *chunk = NULL;
            MOQ_TEST_CHECK(remaining <= sizeof(tail));
            if (remaining > sizeof(tail)) return ref;
            MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_create(
                moq_alloc_default(), tail, remaining, &chunk), (int)MOQ_OK);
            if (!chunk) return ref;
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_write_object_data(
                s, sgh[i], chunk, finish_time), (int)MOQ_OK);
            moq_rcbuf_decref(chunk);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_end_object(s, sgh[i], finish_time),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_close_subgroup(s, sgh[i], finish_time),
                                  (int)MOQ_OK);
            moq_action_t a;
            size_t sends = 0;
            while (moq_session_poll_actions(s, &a, 1) > 0) {
                MOQ_TEST_CHECK_EQ_INT((int)a.kind, (int)MOQ_ACTION_SEND_DATA);
                if (a.kind == MOQ_ACTION_SEND_DATA) {
                    MOQ_TEST_CHECK_EQ_U64(a.u.send_data.stream_ref._v, g_fu_sg_refs[i]._v);
                    MOQ_TEST_CHECK_EQ_SIZE(a.u.send_data.header_len, 0);
                    MOQ_TEST_CHECK_EQ_INT(a.u.send_data.fin ? 1 : 0, sends == 1 ? 1 : 0);
                    if (sends == 0) {
                        MOQ_TEST_CHECK(a.u.send_data.payload != NULL);
                        if (a.u.send_data.payload)
                            failures += txs_check_span_bytes(
                                moq_rcbuf_data(a.u.send_data.payload),
                                moq_rcbuf_len(a.u.send_data.payload), tail, remaining,
                                "failed-update completed payload tail");
                    } else {
                        MOQ_TEST_CHECK(a.u.send_data.payload == NULL);
                    }
                }
                sends++;
                moq_action_cleanup(&a);
            }
            MOQ_TEST_CHECK_EQ_SIZE(sends, 2);
        }
    }

    /* The streams really are in the non-default shape the oracle needs. */
    {
        uint64_t seen_deadline[FU_SUBGROUPS];
        size_t k = 0;
        for (size_t i = 0; i < s->sg_cap && k < g_fu_sg_count; i++) {
            if (s->subgroups[i].state == MOQ_SG_FREE) continue;
            if (!moq_subscription_eq(s->subgroups[i].sub, sub)) continue;
            MOQ_TEST_CHECK_EQ_INT((int)s->subgroups[i].state,
                                  completed ? (int)MOQ_SG_CLOSING : (int)MOQ_SG_STREAMING);
            MOQ_TEST_CHECK_EQ_U64(s->subgroups[i].streaming_payload_len,
                                  completed ? 0 : g_fu_sg_payload[k]);
            MOQ_TEST_CHECK_EQ_U64(s->subgroups[i].streaming_bytes_written,
                                  completed ? 0 : g_fu_sg_written[k]);
            MOQ_TEST_CHECK_EQ_U64(s->subgroups[i].delivery_deadline_us,
                                  completed ? 5000000u + 4u + k : UINT64_MAX);
            seen_deadline[k] = s->subgroups[i].delivery_deadline_us;
            k++;
        }
        MOQ_TEST_CHECK_EQ_SIZE(k, g_fu_sg_count);
        /* Distinct, so a per-stream disarm cannot be confused with a shared
         * one. */
        if (completed)
            for (size_t i = 0; i + 1 < k; i++)
                MOQ_TEST_CHECK(seen_deadline[i] != seen_deadline[i + 1]);
        MOQ_TEST_CHECK_EQ_U64(s->subgroup_deadline_us,
                              completed ? 5000004u : UINT64_MAX);
    }

    /* The ABSOLUTE owner shape the terminal will act on. */
    MOQ_TEST_CHECK_EQ_INT(sub_pool_busy(s), 1);
    const moq_sub_entry_t *se = sub_entry_by_ref(s, ref);
    MOQ_TEST_CHECK(se != NULL);
    if (se) {
        MOQ_TEST_CHECK_EQ_INT((int)se->state, (int)MOQ_SUB_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)se->role, (int)MOQ_SUB_ROLE_PUBLISHER);
        MOQ_TEST_CHECK_EQ_U64(se->handle._opaque, sub._opaque);
        MOQ_TEST_CHECK_EQ_U64(se->request_id, 0);
        MOQ_TEST_CHECK_EQ_U64(se->rs.ref._v, ref._v);
        MOQ_TEST_CHECK_EQ_INT(se->rs.recv_fin ? 1 : 0, 0);
        MOQ_TEST_CHECK_EQ_SIZE(se->rs.recv_len, (size_t)0);
        check_registered(s, ref, MOQ_REQ_SUBSCRIPTION, (int)(se - s->subs));
    }
    return ref;
}


static moq_stream_ref_t arm_failed_update_subscription(moq_session_t *s)
{
    return arm_failed_update_subscription_shape(s, false);
}


moq_stream_ref_t arm_failed_update_completed_subscription(moq_session_t *s)
{
    return arm_failed_update_subscription_shape(s, true);
}


/* The failed-update terminal touches state the generic snapshot does not
 * model: the subscription's data streams (their state, generation, streaming
 * counters and delivery deadline), the aggregate subgroup deadline, and the
 * send buffer the two atomic messages would fill. A refusal must leave every
 * one of them exactly as it was, so they are captured and compared rather than
 * spot-checked. */
typedef struct fu_extra {
    size_t   count;
    uint64_t ref[FU_SUBGROUPS];
    int      state[FU_SUBGROUPS];
    uint32_t generation[FU_SUBGROUPS];
    uint64_t payload_len[FU_SUBGROUPS];
    uint64_t bytes_written[FU_SUBGROUPS];
    uint64_t deadline[FU_SUBGROUPS];
    int      overflow;
    uint64_t agg_deadline;
    size_t   send_len;
} fu_extra_t;


static fu_extra_t g_fu_extra;


/* This family's first headers escape through public raw action polling in
 * arm_failed_update_subscription_shape(), not an observing transport. D18
 * section10.11 requires unknown, 2^62-1, for its automatic DONE. Keep the
 * expected count and wire image independent of the live owner inventory and
 * the profile's count selection. vi64 needs nine bytes here, not one. */
static const uint64_t k_fu_raw_unknown_count = UINT64_C(0x3fffffffffffffff);

static const uint8_t k_fu_raw_done[] = {
    0x0b, 0x00, 0x0b, 0x08, 0xff, 0x3f, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00
};


/* The exact send-buffer cost of the terminal's two atomic messages, declared
 * against that independent raw DONE image; its eight-byte growth is real
 * send-arena usage, not extra snapshot capacity. */
static size_t g_fu_committed_bytes;


static void fu_declare_committed_bytes(void)
{
    uint8_t err[128], done[128];
    size_t n = enc_request_error(err, sizeof(err), 0x17, 0, NULL);
    moq_buf_writer_t dw;
    moq_buf_writer_init(&dw, done, sizeof(done));
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_encode_publish_done(&dw, 0x8, k_fu_raw_unknown_count,
                                       (moq_bytes_t){0}),
        (int)MOQ_OK);
    failures += txs_check_span_bytes(done, moq_buf_writer_offset(&dw),
                                    k_fu_raw_done, sizeof(k_fu_raw_done),
                                    "raw failed-update DONE");
    g_fu_committed_bytes = n + sizeof(k_fu_raw_done);
}


static void fu_extra_read(const moq_session_t *s, fu_extra_t *v)
{
    memset(v, 0, sizeof(*v));
    for (size_t i = 0; i < s->sg_cap; i++) {
        if (s->subgroups[i].state == MOQ_SG_FREE) continue;
        if (v->count >= FU_SUBGROUPS) { v->overflow = 1; break; }
        v->ref[v->count]           = s->subgroups[i].stream_ref._v;
        v->state[v->count]         = (int)s->subgroups[i].state;
        v->generation[v->count]    = s->subgroups[i].generation;
        v->payload_len[v->count]   = s->subgroups[i].streaming_payload_len;
        v->bytes_written[v->count] = s->subgroups[i].streaming_bytes_written;
        v->deadline[v->count]      = s->subgroups[i].delivery_deadline_us;
        v->count++;
    }
    v->agg_deadline = s->subgroup_deadline_us;
    v->send_len = s->send_len;
}


void fu_extra_capture(const moq_session_t *s)
{
    fu_declare_committed_bytes();
    fu_extra_read(s, &g_fu_extra);
    MOQ_TEST_CHECK_EQ_INT(g_fu_extra.overflow, 0);
    MOQ_TEST_CHECK_EQ_SIZE(g_fu_extra.count, (size_t)FU_SUBGROUPS);
}


#define FU_EQ(now_v, want_v, label) do { \
        if ((now_v) != (want_v)) { \
            TXS_DIAG("TXN %s: " label " %llu, expected %llu\n", what, \
                     (unsigned long long)(now_v), \
                     (unsigned long long)(want_v)); \
            bad++; \
        } \
    } while (0)


static int fu_extra_common(const moq_session_t *s, fu_extra_t *now,
                           const char *what)
{
    fu_extra_read(s, now);
    if (now->overflow || g_fu_extra.overflow) {
        TXS_DIAG("TXN %s: incomparable subgroup capture\n", what);
        return 1;
    }
    if (now->count != g_fu_extra.count) {
        TXS_DIAG("TXN %s: %zu live subgroups, expected %zu\n", what,
                 now->count, g_fu_extra.count);
        return 1;
    }
    return 0;
}


/* A refusal leaves every one of them exactly as it found them. */
static int fu_extra_check_blocked(const moq_session_t *s, const char *what)
{
    fu_extra_t now;
    int bad = fu_extra_common(s, &now, what);
    if (bad) return bad;
    for (size_t i = 0; i < now.count; i++) {
        FU_EQ(now.ref[i],           g_fu_extra.ref[i],           "subgroup ref");
        FU_EQ(now.state[i],         g_fu_extra.state[i],         "subgroup state");
        FU_EQ(now.generation[i],    g_fu_extra.generation[i],    "subgroup generation");
        FU_EQ(now.payload_len[i],   g_fu_extra.payload_len[i],   "streaming payload length");
        FU_EQ(now.bytes_written[i], g_fu_extra.bytes_written[i], "streaming bytes written");
        FU_EQ(now.deadline[i],      g_fu_extra.deadline[i],      "subgroup deadline");
    }
    FU_EQ(now.agg_deadline, g_fu_extra.agg_deadline, "aggregate subgroup deadline");
    FU_EQ(now.send_len,     g_fu_extra.send_len,     "send buffer bytes");
    return bad;
}


/* Open streams reset and clear their streaming counters. FIN-closed streams
 * retain their distinct delivery timers until commitment or expiry; D18
 * section10.11 explicitly permits stream state to outlive PUBLISH_DONE. */
int fu_extra_check_committed(const moq_session_t *s, const char *what)
{
    fu_extra_t now;
    int bad = fu_extra_common(s, &now, what);
    if (bad) return bad;
    for (size_t i = 0; i < now.count; i++) {
        FU_EQ(now.ref[i],        g_fu_extra.ref[i],        "subgroup ref");
        FU_EQ(now.generation[i], g_fu_extra.generation[i], "subgroup generation");
        FU_EQ(now.state[i], g_fu_completed ? (int)MOQ_SG_CLOSING
                                          : (int)MOQ_SG_RESETTING,
              "subgroup state");
        FU_EQ(now.payload_len[i],   0, "streaming payload length");
        FU_EQ(now.bytes_written[i], 0, "streaming bytes written");
        FU_EQ(now.deadline[i], g_fu_completed ? g_fu_extra.deadline[i] : UINT64_MAX,
              "subgroup deadline");
    }
    FU_EQ(now.agg_deadline, g_fu_completed ? g_fu_extra.agg_deadline : UINT64_MAX,
          "aggregate subgroup deadline");
    /* The two atomic messages went out, so the send buffer holds exactly what
     * they cost -- not whatever was left over from an earlier attempt. */
    FU_EQ(now.send_len, g_fu_extra.send_len + g_fu_committed_bytes,
          "send buffer bytes");
    return bad;
}

#undef FU_EQ


const extra_ops_t fu_extra_ops = {
    fu_extra_capture, fu_extra_check_blocked, fu_extra_check_committed
};


/* One image for a queued data-stream cancellation. */
static void want_reset_data(txs_norm_vec_t *v, moq_stream_ref_t ref,
                            uint64_t code)
{
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    txs_img_u64(&im, code);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_RESET_DATA, &im));
}


/* One SEND_BIDI image over an already-encoded control message. */
void want_bidi_msg(txs_norm_vec_t *v, moq_stream_ref_t ref, bool fin,
                   const uint8_t *msg, size_t len)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, msg, len);
    moq_control_envelope_t env;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
    txs_img_t im;
    txs_img_init(&im);
    txs_img_u64(&im, ref._v);
    txs_img_u64(&im, fin ? 1u : 0u);
    txs_img_u64(&im, env.msg_type);
    txs_img_bytes(&im, env.payload, env.payload_len);
    MOQ_TEST_CHECK(txs_norm_append_img(v, MOQ_ACTION_SEND_BIDI_STREAM, &im));
}


/* Section10.9.1: cancel still-open subgroups before REQUEST_ERROR and terminal
 * PUBLISH_DONE. Already FIN-closed streams need no second close. No event. */
void want_failed_update_output(txs_norm_vec_t *v, moq_stream_ref_t ref)
{
    if (!g_fu_completed)
        for (size_t i = 0; i < g_fu_sg_count; i++)
            want_reset_data(v, g_fu_sg_refs[i], 0x1);   /* CANCELLED (§3.3.3) */

    uint8_t err[128];
    size_t err_len = enc_request_error(err, sizeof(err), 0x17, 0, NULL);
    want_bidi_msg(v, ref, false, err, err_len);

    /* Logical subgroup inventory cannot attest raw transport opening. Keep
     * the exact unknown-count envelope, terminal FIN and action ordering. */
    want_bidi_msg(v, ref, true, k_fu_raw_done, sizeof(k_fu_raw_done));
}


void test_axis3_fin_terminals(void)
{
    /* session_namespace.c:158 -- announce REQUEST_UPDATE rejection. */
    static const terminal_case_t ann_update = {
        .name = "announce update rejected", .make = make_session_default,
        .arm = arm_established_announcement, .encode = enc_update_next,
        .drain_reason = MOQ_DRAIN_NORMAL,
        .want_output = want_update_rejected, .hooks = &pub_hooks_for_actions,
        .owner = &ann_owner_ops
    };
    /* session_subscribe_tracks.c:479 -- its publisher-side mirror. */
    static const terminal_case_t st_update = {
        .name = "track-sub update rejected", .make = make_session_default,
        .arm = arm_established_track_sub, .encode = enc_update_next,
        .drain_reason = MOQ_DRAIN_NORMAL,
        .want_output = want_update_rejected, .hooks = &pub_hooks_for_actions,
        .owner = &st_owner_ops
    };

    /* profile_d18.c:1092 -- GOAWAY on a committed, peer-opened request bidi. */
    static const terminal_case_t goaway_request_route = {
        .name = "goaway request route", .make = make_session_default,
        .arm = arm_goaway_announcement, .encode = enc_goaway_request,
        .drain_reason = MOQ_DRAIN_GOAWAY_RECEIVED,
        .want_output = want_goaway_output, .hooks = &goaway_hooks,
        .owner = &ann_owner_ops
    };

    run_terminal_matrix(&ann_update, 2);
    run_terminal_matrix(&st_update, 2);
    run_terminal_matrix(&goaway_request_route, 2);

    /* profile_d18.c:2143 -- GOAWAY on our own, locally-opened request bidi. */
    {
        static const terminal_case_t goaway_response_route = {
            .name = "goaway response route", .make = make_session_client,
            .arm = arm_goaway_local_subscribe, .encode = enc_goaway_request_uri,
            .drain_reason = MOQ_DRAIN_GOAWAY_RECEIVED,
            .want_output = want_goaway_output_uri, .hooks = &goaway_hooks,
            .owner = &sub_owner_ops
        };
        run_terminal_matrix(&goaway_response_route, 2);
    }

    /* session_namespace_sub.c:1441 -- the durable-latch control. */
    {
        static const terminal_case_t goaway_ns_sub = {
            .name = "goaway ns_sub", .make = make_session_client,
            .arm = arm_goaway_local_ns_sub, .encode = enc_goaway_request_uri,
            .drain_reason = MOQ_DRAIN_GOAWAY_RECEIVED,
            .want_output = want_goaway_output_uri, .hooks = &goaway_hooks,
            .owner = &nsg_owner_ops
        };
        run_terminal_matrix(&goaway_ns_sub, 2);
    }
    run_ns_goaway_latched();

    /* session_subscribe.c:2226 -- the failed REQUEST_UPDATE termination. It
     * follows the SAME unified rule as every other row: a genuinely open peer
     * half owes exactly one releasable normal reference, a same-call FIN owes
     * none, and an exhausted ring refuses rather than terminating without one. */
    {
        static const terminal_case_t failed_update = {
            .name = "failed subscription update",
            .make = make_session_default,
            .arm = arm_failed_update_subscription,
            .encode = enc_update_unknown_alias,
            .drain_reason = MOQ_DRAIN_NORMAL,
            .want_output = want_failed_update_output,
            .hooks = &pub_hooks_for_actions,
            .owner = &sub_owner_ops,
            .extra = &fu_extra_ops
        };
        run_terminal_matrix(&failed_update, 2);
        terminal_case_t completed_update = failed_update;
        completed_update.name = "failed subscription update after subgroup FIN";
        completed_update.arm = arm_failed_update_completed_subscription;
        run_terminal_matrix(&completed_update, 2);
    }
}
