/* Reuse the frozen fixture's checked wire/ownership helpers without changing it. */
#define main issue44_frozen_fixture_main
#include "test_issue44_rejected_update.c"
#undef main
#include "../../core/src/session/session_internal.h"

#define NEED(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE %s:%d: %s\n", __FILE__, __LINE__, #x); \
    failures++; return false; } } while (0)

typedef struct fixture {
    test_alloc_state_t as;
    moq_alloc_t alloc;
    moq_session_t *pub, *sub;
    moq_publication_t ph, sh;
    int slot;
    uint64_t uid;
    packet update_wire;
} fixture;

/* Inspect private pool state without calling production file-local helpers. */
static int test_subgroup_slot(const moq_session_t *s, moq_subgroup_handle_t h,
                              moq_sg_state_t state)
{
    uint32_t slot = moq_handle_slot(h._opaque);
    if (moq_handle_pool_tag(h._opaque) != MOQ_HANDLE_POOL_SUBGROUP ||
        moq_handle_session_tag(h._opaque) != s->session_tag ||
        slot >= s->sg_cap)
        return -1;
    const moq_sg_entry_t *entry = &s->subgroups[slot];
    if (entry->generation != moq_handle_generation(h._opaque) ||
        entry->state != state)
        return -1;
    return (int)slot;
}

static bool publish_track(fixture *f, const char *name,
                          moq_publication_t *ph, moq_publication_t *sh)
{
    moq_bytes_t part = MOQ_BYTES_LITERAL("pressure");
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ &part, 1 };
    pc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    NEED(moq_session_publish(f->pub, &pc, 0, ph) == MOQ_OK);
    NEED(deliver(f->pub, f->sub));
    moq_event_t e;
    NEED(event(f->sub, MOQ_EVENT_PUBLISH_REQUEST, &e));
    *sh = e.u.publish_request.pub;
    moq_event_cleanup(&e);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init(&ac);
    NEED(moq_session_accept_publish(f->sub, *sh, &ac, 0) == MOQ_OK);
    NEED(deliver(f->sub, f->pub));
    NEED(event(f->pub, MOQ_EVENT_PUBLISH_OK, &e));
    moq_event_cleanup(&e);
    return true;
}

static bool setup(fixture *f, unsigned actions)
{
    f->alloc = test_allocator(&f->as);
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = MOQ_VERSION_DRAFT_16;
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.send_request_capacity = true; cfg.initial_request_capacity = 32;
    cfg.send_auth_token_cache_size = true; cfg.auth_token_cache_size = 1024;
    cfg.max_actions = actions;
    NEED(moq_session_create(&cfg, 0, &f->pub) == MOQ_OK);
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.max_actions = 8;
    NEED(moq_session_create(&cfg, 0, &f->sub) == MOQ_OK);
    NEED(moq_session_start(f->pub, 0) == MOQ_OK);
    NEED(deliver(f->pub, f->sub));
    NEED(deliver(f->sub, f->pub));
    NEED(moq_session_state(f->pub) == MOQ_SESS_ESTABLISHED);
    NEED(moq_session_state(f->sub) == MOQ_SESS_ESTABLISHED);
    moq_event_t e;
    while (moq_session_poll_events(f->pub, &e, 1)) moq_event_cleanup(&e);
    while (moq_session_poll_events(f->sub, &e, 1)) moq_event_cleanup(&e);
    NEED(publish_track(f, "target", &f->ph, &f->sh));
    f->slot = pub_resolve_handle(f->pub, f->ph);
    NEED(f->slot >= 0);
    NEED(f->pub->publishes[f->slot].subgroup_streams_opened == 0);
    return true;
}

static void cleanup_fixture(fixture *f)
{
    moq_event_t e;
    moq_action_t a;
    moq_session_t *ss[] = { f->pub, f->sub };
    for (size_t i = 0; i < 2; i++) if (ss[i]) {
        while (moq_session_poll_events(ss[i], &e, 1)) moq_event_cleanup(&e);
        while (moq_session_poll_actions(ss[i], &a, 1)) moq_action_cleanup(&a);
        moq_session_destroy(ss[i]);
    }
    CHECK(f->as.balance == 0);
}

static bool make_reject(fixture *f)
{
    packet p[8];
    NEED(update(f->sub, true, f->sh, (moq_subscription_t){0}) == MOQ_OK);
    NEED(collect(f->sub, p) == 1);
    moq_control_envelope_t env;
    NEED(envelope(&p[0], &env) && env.msg_type == MOQ_D16_REQUEST_UPDATE);
    moq_kvp_entry_t scratch[8];
    moq_d16_request_update_t u = { .params = scratch, .params_cap = 8 };
    NEED(moq_d16_decode_request_update(env.payload, env.payload_len, &u) == MOQ_OK);
    NEED((u.request_id & 1) == 1 && u.existing_request_id == 0);
    f->uid = u.request_id;
    uint8_t tb[2][64];
    moq_kvp_entry_t tokens[2] = {0};
    moq_d16_auth_token_t ts[2] = {
        { .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 7, .token_type = 1,
          .token_value = (const uint8_t *)"token", .token_value_len = 5 },
        { .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 999 }
    };
    for (size_t i = 0; i < 2; i++) {
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb[i], sizeof(tb[i]));
        NEED(moq_d16_auth_token_encode(&tw, &ts[i]) == MOQ_OK);
        tokens[i].type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN;
        tokens[i].value = tb[i];
        tokens[i].value_len = moq_buf_writer_offset(&tw);
    }
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, f->update_wire.bytes, sizeof(f->update_wire.bytes));
    NEED(moq_d16_encode_request_update(&w, f->uid, 0, tokens, 2) == MOQ_OK);
    f->update_wire.len = moq_buf_writer_offset(&w);
    return true;
}

static bool alias_state(fixture *f, bool committed)
{
    uint64_t type = 0;
    const uint8_t *v = NULL; size_t n = 0;
    int rc = moq_token_cache_lookup(&f->pub->peer_token_cache, 7, &type, &v, &n);
    if (!committed) return rc == MOQ_TOKEN_ERR_UNKNOWN;
    return rc == MOQ_TOKEN_OK && type == 1 && n == 5 && memcmp(v, "token", 5) == 0;
}

static bool next_id(fixture *f, uint64_t id)
{
    moq_request_endpoint_t ep;
    return f->pub->profile->validate_inbound_request(f->pub, id, &ep) == MOQ_OK &&
           moq_session_state(f->pub) == MOQ_SESS_ESTABLISHED;
}

/* Parse all envelopes, including the one-action/two-envelope case. */
static bool terminal_pair(fixture *f, uint64_t count, packet out[8])
{
    int n = collect(f->pub, out);
    NEED(n == (f->pub->action_cap == 1 ? 1 : 2));
    unsigned messages = 0;
    for (int i = 0; i < n; i++) {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, out[i].bytes, out[i].len);
        while (moq_buf_reader_remaining(&r)) {
            moq_control_envelope_t e;
            NEED(moq_control_decode_envelope(&r, &e) == MOQ_OK);
            if (messages == 0) {
                moq_d16_request_error_t error;
                NEED(e.msg_type == MOQ_D16_REQUEST_ERROR);
                NEED(moq_d16_decode_request_error(e.payload, e.payload_len, &error) == MOQ_OK);
                NEED(error.request_id == f->uid && error.error_code == 0x17);
            } else if (messages == 1) {
                moq_d16_publish_done_t done;
                NEED(e.msg_type == MOQ_D16_PUBLISH_DONE);
                NEED(moq_d16_decode_publish_done(e.payload, e.payload_len, &done) == MOQ_OK);
                NEED(done.request_id == 0 && done.status_code == 0x8);
                /* This fixture polls raw actions, not an observing bridge.
                 * Nonzero logical opens therefore have no actual-open fact. */
                NEED(done.stream_count == (count ? MOQ_QUIC_VARINT_MAX : 0));
            } else NEED(false);
            messages++;
        }
    }
    NEED(messages == 2);
    NEED(pub_resolve_handle(f->pub, f->ph) < 0);
    NEED(request_registry_find_by_id(f->pub, 0).kind == MOQ_REQ_NONE);
    NEED(f->pub->recv_len == 0 && alias_state(f, true) && next_id(f, f->uid + 2));
    NEED(no_events(f->pub));
    NEED(moq_session_process_pending(f->pub, 0) == MOQ_OK);
    packet empty[8];
    NEED(collect(f->pub, empty) == 0 && alias_state(f, true));
    return true;
}

static bool open_stream(fixture *f, moq_publication_t ph, uint64_t group,
                        moq_subgroup_handle_t *sg, moq_stream_ref_t *ref)
{
    moq_subgroup_cfg_t cfg; moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    NEED(moq_session_open_pub_subgroup(f->pub, ph, &cfg, 0, sg) == MOQ_OK);
    moq_action_t a;
    NEED(moq_session_poll_actions(f->pub, &a, 1) == 1);
    bool good = a.kind == MOQ_ACTION_SEND_DATA && !a.u.send_data.fin;
    if (good) *ref = a.u.send_data.stream_ref;
    moq_action_cleanup(&a);
    NEED(good);
    return true;
}

static void preflight_case(unsigned mode)
{
    fixture f = {0}; packet p[8];
    size_t original_cap = 0;
    REQUIRE(setup(&f, mode == 1 ? 8 : 2));
    original_cap = f.pub->send_cap;
    REQUIRE(make_reject(&f));
    moq_subgroup_handle_t untouched = {0}; moq_stream_ref_t untouched_ref;
    if (mode == 2) REQUIRE(open_stream(&f, f.ph, 1, &untouched, &untouched_ref));
    uint8_t buf[256]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    REQUIRE(moq_d16_encode_request_error(&w, 1, 0x17, 0, NULL, 0) == MOQ_OK);
    size_t err_len = moq_buf_writer_offset(&w);
    REQUIRE(moq_d16_encode_publish_done(&w, 0, 0x8, 0, NULL, 0) == MOQ_OK);
    size_t total = moq_buf_writer_offset(&w);
    REQUIRE(total > err_len);
    if (mode == 0) {
        REQUIRE(queue_send_control(f.pub, (const uint8_t *)"x", 1) == MOQ_OK);
    } else if (mode == 1) {
        /* Allocate real queued bytes, leaving only ERROR-sized tail capacity. */
        size_t used = original_cap - err_len;
        uint8_t *padding = calloc(used, 1);
        REQUIRE(padding != NULL);
        moq_result_t rc = queue_send_control(f.pub, padding, used);
        free(padding);
        REQUIRE(rc == MOQ_OK);
    } else {
        /* White-box total-capacity bound after handshake; allocation unchanged. */
        f.pub->send_cap = total - 1;
    }
    moq_result_t expected = mode == 2 ? MOQ_ERR_BUFFER : MOQ_ERR_WOULD_BLOCK;
    REQUIRE(moq_session_on_control_bytes(f.pub, f.update_wire.bytes,
                                        f.update_wire.len, 0) == expected);
    CHECK(!f.pub->publishes[f.slot].update_failed);
    CHECK(pub_resolve_handle(f.pub, f.ph) == f.slot);
    CHECK(request_registry_find_by_id(f.pub, 0).kind == MOQ_REQ_PUBLISH);
    CHECK(alias_state(&f, false) && next_id(&f, 1));
    CHECK(f.pub->recv_len == f.update_wire.len);
    if (mode == 2) CHECK(collect(f.pub, p) == 0);
    else {
        moq_action_t filler;
        REQUIRE(moq_session_poll_actions(f.pub, &filler, 1) == 1);
        CHECK(filler.kind == MOQ_ACTION_SEND_CONTROL);
        CHECK(filler.u.send_control.len == (mode == 0 ? 1 : original_cap - err_len));
        moq_action_cleanup(&filler);
        CHECK(collect(f.pub, p) == 0);
    }
    if (mode == 2) {
        REQUIRE(moq_session_process_pending(f.pub, 0) == MOQ_ERR_BUFFER);
        CHECK(!f.pub->publishes[f.slot].update_failed && alias_state(&f, false));
        CHECK(next_id(&f, 1) && f.pub->recv_len == f.update_wire.len);
        CHECK(collect(f.pub, p) == 0);
        CHECK(test_subgroup_slot(f.pub, untouched, MOQ_SG_OPEN) >= 0);
    } else {
        REQUIRE(moq_session_process_pending(f.pub, 0) == MOQ_OK);
        REQUIRE(terminal_pair(&f, 0, p));
    }
cleanup:
    if (original_cap) f.pub->send_cap = original_cap;
    cleanup_fixture(&f);
}

static void one_slot_case(void)
{
    fixture f = {0}; packet p[8];
    REQUIRE(setup(&f, 1));
    REQUIRE(make_reject(&f));
    REQUIRE(moq_session_on_control_bytes(f.pub, f.update_wire.bytes,
                                        f.update_wire.len, 0) == MOQ_OK);
    REQUIRE(terminal_pair(&f, 0, p));
cleanup:
    cleanup_fixture(&f);
}

static void partial_resets_case(void)
{
    fixture f = {0}; packet p[8]; moq_rcbuf_t *payload = NULL;
    REQUIRE(setup(&f, 1));
    moq_publication_t other, other_sub;
    REQUIRE(publish_track(&f, "other", &other, &other_sub));
    moq_bytes_t ns_part = MOQ_BYTES_LITERAL("pressure");
    moq_subscribe_cfg_t sc; moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ &ns_part, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("unrelated-subscription");
    sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
    moq_subscription_t sub_owner, sub_requester;
    REQUIRE(moq_session_subscribe(f.sub, &sc, 0, &sub_requester) == MOQ_OK);
    REQUIRE(deliver(f.sub, f.pub));
    moq_event_t e;
    REQUIRE(event(f.pub, MOQ_EVENT_SUBSCRIBE_REQUEST, &e));
    sub_owner = e.u.subscribe_request.sub;
    moq_event_cleanup(&e);
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    REQUIRE(moq_session_accept_subscribe(f.pub, sub_owner, &ac, 0) == MOQ_OK);
    REQUIRE(deliver(f.pub, f.sub));
    REQUIRE(event(f.sub, MOQ_EVENT_SUBSCRIBE_OK, &e));
    moq_event_cleanup(&e);
    moq_subgroup_cfg_t sub_cfg; moq_subgroup_cfg_init(&sub_cfg);
    moq_subgroup_handle_t sub_sg;
    REQUIRE(moq_session_open_subgroup(f.pub, sub_owner, &sub_cfg, 0, &sub_sg) == MOQ_OK);
    moq_action_t sub_action;
    REQUIRE(moq_session_poll_actions(f.pub, &sub_action, 1) == 1);
    bool sub_action_ok = sub_action.kind == MOQ_ACTION_SEND_DATA;
    moq_stream_ref_t sub_ref = sub_action.u.send_data.stream_ref;
    moq_action_cleanup(&sub_action);
    REQUIRE(sub_action_ok);
    moq_subgroup_handle_t a, b, c, unrelated;
    moq_stream_ref_t ar, br, cr, ur;
    REQUIRE(open_stream(&f, f.ph, 1, &a, &ar));
    REQUIRE(open_stream(&f, f.ph, 2, &b, &br));
    REQUIRE(open_stream(&f, f.ph, 3, &c, &cr));
    REQUIRE(open_stream(&f, other, 3, &unrelated, &ur));
    REQUIRE(moq_session_begin_object(f.pub, c, 0, 1, 0) == MOQ_OK);
    moq_action_t action;
    REQUIRE(moq_session_poll_actions(f.pub, &action, 1) == 1);
    moq_action_cleanup(&action);
    REQUIRE(moq_rcbuf_create(&f.alloc, (const uint8_t *)"x", 1, &payload) == MOQ_OK);
    REQUIRE(moq_session_write_object_data(f.pub, c, payload, 0) == MOQ_OK);
    REQUIRE(moq_session_poll_actions(f.pub, &action, 1) == 1);
    moq_action_cleanup(&action);
    int ai = test_subgroup_slot(f.pub, a, MOQ_SG_OPEN);
    int bi = test_subgroup_slot(f.pub, b, MOQ_SG_OPEN);
    int ci = test_subgroup_slot(f.pub, c, MOQ_SG_STREAMING);
    REQUIRE(ai >= 0 && bi >= 0 && ci >= 0);
    f.pub->subgroups[ai].delivery_deadline_us = 100;
    f.pub->subgroups[bi].delivery_deadline_us = 200;
    f.pub->subgroups[ci].delivery_deadline_us = 300;
    sg_recompute_deadline(f.pub);
    REQUIRE(make_reject(&f));
    REQUIRE(moq_session_on_control_bytes(f.pub, f.update_wire.bytes,
                                        f.update_wire.len, 0) == MOQ_ERR_WOULD_BLOCK);
    REQUIRE(f.pub->publishes[f.slot].update_failed);
    CHECK(f.pub->subgroup_deadline_us == 200);
    CHECK(alias_state(&f, false) && next_id(&f, f.uid));
    REQUIRE(moq_session_poll_actions(f.pub, &action, 1) == 1);
    CHECK(action.kind == MOQ_ACTION_RESET_DATA);
    CHECK(action.u.reset_data.stream_ref._v == ar._v);
    CHECK(action.u.reset_data.error_code == 1);
    moq_action_cleanup(&action);
    uint32_t refs = moq_rcbuf_refcount(payload);
    moq_subgroup_cfg_t cfg; moq_subgroup_cfg_init(&cfg);
    cfg.group_id = 4;
    moq_subgroup_handle_t refused;
    CHECK(moq_session_open_pub_subgroup(f.pub, f.ph, &cfg, 0, &refused) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_write_object(f.pub, b, 0, payload, 0) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_write_status_object(f.pub, b, 0, MOQ_OBJECT_END_OF_GROUP, 0) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_begin_object(f.pub, b, 0, 1, 0) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_close_subgroup(f.pub, b, 0) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_write_object_data(f.pub, c, payload, 0) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_end_object(f.pub, c, 0) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_send_pub_object_datagram(f.pub, f.ph, 0, 0, 0, false,
                                              payload, NULL, 0, 0) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_session_send_pub_status_datagram(f.pub, f.ph, 0, 0, 0,
                                              MOQ_OBJECT_END_OF_GROUP, 0) == MOQ_ERR_WRONG_STATE);
    moq_finish_publish_cfg_t finish; moq_finish_publish_cfg_init(&finish);
    CHECK(moq_session_finish_publish(f.pub, f.ph, &finish, 0) == MOQ_ERR_WRONG_STATE);
    CHECK(moq_rcbuf_refcount(payload) == refs);
    CHECK(collect(f.pub, p) == 0);
    CHECK(test_subgroup_slot(f.pub, unrelated, MOQ_SG_OPEN) >= 0);
    REQUIRE(moq_session_process_pending(f.pub, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK(f.pub->subgroup_deadline_us == 300);
    REQUIRE(moq_session_poll_actions(f.pub, &action, 1) == 1);
    CHECK(action.kind == MOQ_ACTION_RESET_DATA);
    CHECK(action.u.reset_data.stream_ref._v == br._v);
    CHECK(action.u.reset_data.error_code == 1);
    moq_action_cleanup(&action);
    CHECK(alias_state(&f, false) && next_id(&f, f.uid));
    REQUIRE(moq_session_process_pending(f.pub, 0) == MOQ_ERR_WOULD_BLOCK);
    CHECK(f.pub->subgroup_deadline_us == UINT64_MAX);
    CHECK(f.pub->subgroups[ci].streaming_payload_len == 0);
    CHECK(f.pub->subgroups[ci].streaming_bytes_written == 0);
    REQUIRE(moq_session_poll_actions(f.pub, &action, 1) == 1);
    CHECK(action.kind == MOQ_ACTION_RESET_DATA);
    CHECK(action.u.reset_data.stream_ref._v == cr._v);
    CHECK(action.u.reset_data.error_code == 1);
    moq_action_cleanup(&action);
    CHECK(alias_state(&f, false) && next_id(&f, f.uid));
    REQUIRE(moq_session_process_pending(f.pub, 0) == MOQ_OK);
    REQUIRE(terminal_pair(&f, 3, p));
    CHECK(pub_resolve_handle(f.pub, other) >= 0);
    CHECK(test_subgroup_slot(f.pub, unrelated, MOQ_SG_OPEN) >= 0);
    CHECK(test_subgroup_slot(f.pub, sub_sg, MOQ_SG_OPEN) >= 0);
    REQUIRE(moq_session_write_object(f.pub, unrelated, 0, payload, 0) == MOQ_OK);
    REQUIRE(moq_session_poll_actions(f.pub, &action, 1) == 1);
    CHECK(action.kind == MOQ_ACTION_SEND_DATA && action.u.send_data.stream_ref._v == ur._v);
    moq_action_cleanup(&action);
    REQUIRE(moq_session_close_subgroup(f.pub, sub_sg, 0) == MOQ_OK);
    REQUIRE(moq_session_poll_actions(f.pub, &action, 1) == 1);
    CHECK(action.kind == MOQ_ACTION_SEND_DATA && action.u.send_data.stream_ref._v == sub_ref._v);
    moq_action_cleanup(&action);
cleanup:
    if (payload) moq_rcbuf_decref(payload);
    cleanup_fixture(&f);
}

static void lifetime_case(bool saturation)
{
    fixture f = {0}; packet p[8];
    REQUIRE(setup(&f, 2));
    moq_subgroup_handle_t a, b; moq_stream_ref_t ar, br;
    REQUIRE(queue_send_control(f.pub, (const uint8_t *)"x", 1) == MOQ_OK);
    REQUIRE(queue_send_control(f.pub, (const uint8_t *)"x", 1) == MOQ_OK);
    moq_subgroup_cfg_t cfg; moq_subgroup_cfg_init(&cfg);
    REQUIRE(moq_session_open_pub_subgroup(f.pub, f.ph, &cfg, 0, &a) == MOQ_ERR_WOULD_BLOCK);
    CHECK(f.pub->publishes[f.slot].subgroup_streams_opened == 0);
    REQUIRE(collect(f.pub, p) == 2);
    if (saturation) f.pub->publishes[f.slot].subgroup_streams_opened = MOQ_QUIC_VARINT_MAX - 1;
    REQUIRE(open_stream(&f, f.ph, 1, &a, &ar));
    REQUIRE(moq_session_close_subgroup(f.pub, a, 0) == MOQ_OK);
    moq_action_t action;
    REQUIRE(moq_session_poll_actions(f.pub, &action, 1) == 1);
    moq_action_cleanup(&action);
    REQUIRE(open_stream(&f, f.ph, 2, &b, &br));
    REQUIRE(test_subgroup_slot(f.pub, a, MOQ_SG_OPEN) < 0);
    uint64_t count = saturation ? MOQ_QUIC_VARINT_MAX : 2;
    CHECK(f.pub->publishes[f.slot].subgroup_streams_opened == count);
    REQUIRE(make_reject(&f));
    REQUIRE(moq_session_on_control_bytes(f.pub, f.update_wire.bytes,
                                        f.update_wire.len, 0) == MOQ_ERR_WOULD_BLOCK);
    REQUIRE(moq_session_poll_actions(f.pub, &action, 1) == 1);
    CHECK(action.kind == MOQ_ACTION_RESET_DATA && action.u.reset_data.stream_ref._v == br._v);
    moq_action_cleanup(&action);
    REQUIRE(moq_session_process_pending(f.pub, 0) == MOQ_OK);
    REQUIRE(terminal_pair(&f, count, p));
    moq_publication_t fresh, fresh_sub;
    REQUIRE(publish_track(&f, "reuse", &fresh, &fresh_sub));
    int slot = pub_resolve_handle(f.pub, fresh);
    REQUIRE(slot == f.slot);
    CHECK(f.pub->publishes[slot].subgroup_streams_opened == 0);
cleanup:
    cleanup_fixture(&f);
}

static void subscriber_full_case(void)
{
    fixture f = {0}; packet p[8], filler[8];
    REQUIRE(setup(&f, 2));
    REQUIRE(make_reject(&f));
    REQUIRE(moq_session_on_control_bytes(f.pub, f.update_wire.bytes,
                                        f.update_wire.len, 0) == MOQ_OK);
    REQUIRE(terminal_pair(&f, 0, p));
    int slot = pub_resolve_handle(f.sub, f.sh);
    REQUIRE(slot >= 0);
    while (!action_queue_full(f.sub))
        REQUIRE(queue_send_control(f.sub, (const uint8_t *)"x", 1) == MOQ_OK);
    moq_event_t e = {0};
    while (!event_queue_full(f.sub)) REQUIRE(push_event(f.sub, &e) == MOQ_OK);
    size_t ah = f.sub->action_head, at = f.sub->action_tail;
    size_t eh = f.sub->event_head, et = f.sub->event_tail;
    REQUIRE(moq_session_on_control_bytes(f.sub, p[0].bytes, p[0].len, 0) == MOQ_OK);
    CHECK(f.sub->publishes[slot].update_failed);
    CHECK(!f.sub->publishes[slot].update_pending);
    CHECK(f.sub->publishes[slot].update_request_id == 0);
    CHECK(request_registry_find_by_id(f.sub, f.uid).kind == MOQ_REQ_NONE);
    CHECK(request_registry_find_by_id(f.sub, 0).kind == MOQ_REQ_PUBLISH);
    CHECK(update(f.sub, true, f.sh, (moq_subscription_t){0}) == MOQ_ERR_WRONG_STATE);
    CHECK(f.sub->action_head == ah && f.sub->action_tail == at);
    CHECK(f.sub->event_head == eh && f.sub->event_tail == et);
    CHECK(collect(f.sub, filler) == 8);
    while (moq_session_poll_events(f.sub, &e, 1)) moq_event_cleanup(&e);
    REQUIRE(moq_session_on_control_bytes(f.sub, p[1].bytes, p[1].len, 0) == MOQ_OK);
    REQUIRE(event(f.sub, MOQ_EVENT_PUBLISH_FINISHED, &e));
    CHECK(e.u.publish_finished.status_code == 0x8);
    moq_event_cleanup(&e);
    CHECK(update(f.sub, true, f.sh, (moq_subscription_t){0}) == MOQ_ERR_STALE_HANDLE);
cleanup:
    cleanup_fixture(&f);
}

int main(void)
{
    preflight_case(0);
    preflight_case(1);
    preflight_case(2);
    one_slot_case();
    partial_resets_case();
    lifetime_case(false);
    lifetime_case(true);
    subscriber_full_case();
    if (!failures) puts("issue44 pressure: PASS");
    return failures ? 1 : 0;
}
