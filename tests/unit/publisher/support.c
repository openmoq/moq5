#include "publisher_test_internal.h"

int failures = 0;

/* -- Helpers ------------------------------------------------------ */

void simpair_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                   moq_simpair_t **sp)
{
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    moq_simpair_create(&cfg, sp);
    moq_simpair_start(*sp);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    /* Drain setup events. */
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    if (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
}

void drain_all(moq_simpair_t *sp) {
    moq_event_t evts[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
    while ((ne = moq_session_poll_events(moq_simpair_server(sp), evts, 16)) > 0)
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);
    moq_action_t acts[16]; size_t na;
    while ((na = moq_session_poll_actions(moq_simpair_client(sp), acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
    while ((na = moq_session_poll_actions(moq_simpair_server(sp), acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
}

/* END_OF_TRACK can create a new status stream, so an observing transport
 * must admit it before the finite DONE is encoded. Only lossless SimPair
 * cases use this bounded retry; raw scanners below keep their own driving. */
moq_result_t end_track_virtual(moq_simpair_t *sp,
    moq_publisher_t *pub, moq_pub_track_t *track)
{
    for (unsigned i = 0; i < 16; ++i) {
        moq_result_t rc = moq_pub_end_track(pub, track, moq_simpair_now_us(sp));
        if (rc != MOQ_ERR_WOULD_BLOCK) return rc;
        rc = moq_simpair_run_until_quiescent(sp, 16, NULL);
        if (rc != MOQ_OK) return rc;
    }
    return MOQ_ERR_WOULD_BLOCK;
}

moq_result_t flush_virtual(moq_simpair_t *sp, moq_publisher_t *pub)
{
    for (unsigned i = 0; i < 16; ++i) {
        moq_result_t rc = moq_pub_flush(pub, moq_simpair_now_us(sp));
        if (rc != MOQ_ERR_WOULD_BLOCK) return rc;
        rc = moq_simpair_run_until_quiescent(sp, 16, NULL);
        if (rc != MOQ_OK) return rc;
    }
    return MOQ_ERR_WOULD_BLOCK;
}

/* -- Deferred authorization state --------------------------------- */

moq_pub_deferred_t *last_deferred = NULL;
uint64_t last_deferred_id = 0;

/* Retained-op release at session close: a wrapped buffer held by a pending
 * (WOULD_BLOCKed) operation is released when the close is observed via
 * tick -- exactly once, not deferred to destroy. */
int g_wrap_releases = 0;
/* -- Callback fixtures -------------------------------------------- */

moq_pub_accept_decision_t cb_accept_all(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error) {
    (void)ctx; (void)info; (void)out_error;
    return MOQ_PUB_DECISION_ACCEPT;
}

moq_pub_accept_decision_t cb_capture(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error) {
    cb_capture_t *cap = (cb_capture_t *)ctx;
    (void)out_error;
    cap->called = true;
    cap->filter = info->filter;
    cap->subscriber_priority = info->subscriber_priority;
    cap->forward = info->forward;
    cap->ns_count = info->track_namespace.count;
    cap->name_len = info->track_name.len;
    cap->token_count = info->token_count;
    if (info->token_count > 0) {
        cap->first_token_type = info->tokens[0].token_type;
        cap->first_token_value_len = info->tokens[0].token_value.len;
        size_t copy = info->tokens[0].token_value.len;
        if (copy > sizeof(cap->first_token_value))
            copy = sizeof(cap->first_token_value);
        if (copy > 0 && info->tokens[0].token_value.data)
            memcpy(cap->first_token_value,
                   info->tokens[0].token_value.data, copy);
    }
    return MOQ_PUB_DECISION_ACCEPT;
}

void tick_on_publish_ok(void *ctx, moq_pub_track_t *track, bool forward) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    (void)track; (void)forward;
    s->publish_ok++;
}
void tick_on_publish_error(void *ctx, moq_pub_track_t *track,
                           moq_request_error_t code) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    (void)track; (void)code;
    s->publish_error++;
}

void tick_on_joined(void *ctx, moq_pub_track_t *track) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->joined++;
    s->joined_track = track;
}
void tick_on_left(void *ctx, moq_pub_track_t *track) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->left++;
    s->left_track = track;
}
void tick_on_draining(void *ctx) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->draining++;
}
void tick_on_updated(void *ctx, moq_pub_track_t *track,
                     const moq_pub_subscribe_update_info_t *info) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->updated++;
    s->updated_track = track;
    s->last_update = *info;
}
void tick_on_closed(void *ctx, uint64_t code) {
    tick_cb_state_t *s = (tick_cb_state_t *)ctx;
    s->closed++;
    s->close_code = code;
}

moq_pub_accept_decision_t cb_defer(
    void *ctx, const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error)
{
    (void)ctx; (void)out_error;
    last_deferred = info->deferred;
    last_deferred_id = info->deferred_id;
    return MOQ_PUB_DECISION_DEFER;
}
/* -- Publisher-initiated PUBLISH ---------------------------------- */

/* On the client (the subscriber side of the simpair), drain one
 * PUBLISH_REQUEST and accept it, optionally forcing the forward flag. Returns
 * true if a request was accepted. */
bool client_accept_publish(moq_simpair_t *sp) {
    moq_event_t ev;
    if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) != 1)
        return false;
    bool accepted = false;
    if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
        moq_accept_publish_cfg_t acc;
        moq_accept_publish_cfg_init(&acc);
        moq_result_t rc = moq_session_accept_publish(moq_simpair_client(sp),
            ev.u.publish_request.pub, &acc, moq_simpair_now_us(sp));
        MOQ_TEST_CHECK(rc == MOQ_OK);
        accepted = true;
    }
    moq_event_cleanup(&ev);
    return accepted;
}

/* Write objects on a published track: they must reach the publication's data
 * stream (the write fan-out routes to the publication slot). */
/* The status of the one PUBLISH_FINISHED the client has surfaced for its
 * accepted publication -- a completion message, not a stream signal, whose
 * Stream Count is `streams`, the data streams the test opened plus, when
 * `eot`, the terminal END_OF_TRACK stream that precedes it; ~0 when there is
 * none, and a second one fails. */
uint64_t client_publication_done_status(moq_simpair_t *sp, uint64_t streams,
                                        bool eot) {
    int done_n = 0, eot_n = 0; uint64_t status = ~0ull;
    moq_event_t d[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (d[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                d[i].u.object_received.status == MOQ_OBJECT_END_OF_TRACK) {
                eot_n++;
                MOQ_TEST_CHECK_EQ_INT(done_n, 0);
            }
            if (d[i].kind == MOQ_EVENT_PUBLISH_FINISHED) {
                done_n++; status = d[i].u.publish_finished.status_code;
                MOQ_TEST_CHECK_EQ_INT(d[i].u.publish_finished.cause, 0);
                MOQ_TEST_CHECK_EQ_U64(d[i].u.publish_finished.stream_count, streams);
            }
            moq_event_cleanup(&d[i]);
        }
    MOQ_TEST_CHECK_EQ_INT(done_n, 1);
    MOQ_TEST_CHECK_EQ_INT(eot_n, eot ? 1 : 0);
    return status;
}
/* -- Fan-out battery ------------------------------------------------- *
 * Delivery to ALL destinations (both install orders), retry-cursor
 * correctness under a forced second-destination WOULD_BLOCK, byte-exact
 * retry identity, Forward-0 exclusion, and mid-operation joiner exclusion. */

/* Coexist harness: one track, a subscription AND an accepted publication
 * (install order selectable). Returns via out params; caller destroys. */
void fanout_setup_ver(test_alloc_state_t *as, moq_alloc_t *alloc,
                      moq_simpair_t **sp, moq_publisher_t **pub_out,
                      moq_pub_track_t **track_out, bool sub_first,
                      uint32_t max_actions, uint64_t version)
{
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    {
        moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
        cfg.alloc = alloc;
        cfg.seed = 42;
        cfg.initial_now_us = 1000;
        cfg.client_send_request_capacity = true;
        cfg.client_initial_request_capacity = 16;
        cfg.server_send_request_capacity = true;
        cfg.server_initial_request_capacity = 16;
        if (version) cfg.version = version;   /* 0: the default profile */
        cfg.max_actions = max_actions;   /* 0 = default; small forces
                                            mid-fan-out WOULD_BLOCK */
        moq_simpair_create(&cfg, sp);
        moq_simpair_start(*sp);
        moq_simpair_run_until_quiescent(*sp, 8, NULL);
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    moq_publisher_t *pub = NULL;
    moq_pub_create(moq_simpair_server(*sp), alloc, &cfg, &pub);

    moq_pub_track_cfg_t tcfg;
    moq_pub_track_cfg_init(&tcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    tcfg.advertise_namespace = true;
    moq_pub_track_t *track = NULL;
    moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(*sp), &track);

    for (int step = 0; step < 2; step++) {
        bool do_sub = sub_first ? (step == 0) : (step == 1);
        if (do_sub) {
            moq_subscribe_cfg_t scfg;
            moq_subscribe_cfg_init(&scfg);
            scfg.track_namespace.parts = ns_parts;
            scfg.track_namespace.count = 1;
            scfg.track_name = MOQ_BYTES_LITERAL("video");
            moq_subscription_t sub_h;
            moq_session_subscribe(moq_simpair_client(*sp), &scfg,
                moq_simpair_now_us(*sp), &sub_h);
            moq_simpair_run_until_quiescent(*sp, 8, NULL);
            moq_event_t ev;
            while (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1) {
                moq_pub_event_result_t res;
                moq_pub_handle_event(pub, &ev, moq_simpair_now_us(*sp), &res);
                moq_event_cleanup(&ev);
            }
        } else {
            moq_pub_publish_cfg_t pcfg; moq_pub_publish_cfg_init(&pcfg);
            MOQ_TEST_CHECK(moq_pub_publish_track(pub, track, &pcfg,
                moq_simpair_now_us(*sp)) == MOQ_OK);
            moq_simpair_run_until_quiescent(*sp, 8, NULL);
            for (int i = 0; i < 6; i++) {
                moq_event_t ev;
                if (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) != 1)
                    break;
                if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
                    moq_accept_namespace_cfg_t nacc;
                    moq_accept_namespace_cfg_init(&nacc);
                    moq_session_accept_namespace(moq_simpair_client(*sp),
                        ev.u.namespace_published.ann, &nacc,
                        moq_simpair_now_us(*sp));
                } else if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
                    moq_accept_publish_cfg_t acc;
                    moq_accept_publish_cfg_init(&acc);
                    moq_session_accept_publish(moq_simpair_client(*sp),
                        ev.u.publish_request.pub, &acc,
                        moq_simpair_now_us(*sp));
                }
                moq_event_cleanup(&ev);
            }
        }
        moq_simpair_run_until_quiescent(*sp, 8, NULL);
        moq_pub_tick(pub, moq_simpair_now_us(*sp));
        moq_simpair_run_until_quiescent(*sp, 8, NULL);
    }
    /* Drain remaining setup noise on both sides. */
    { moq_event_t ev;
      while (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      while (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1) {
          moq_pub_event_result_t res;
          moq_pub_handle_event(pub, &ev, moq_simpair_now_us(*sp), &res);
          moq_event_cleanup(&ev);
      } }

    MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));
    MOQ_TEST_CHECK(moq_pub_track_is_published(pub, track));
    *pub_out = pub;
    *track_out = track;
}

void fanout_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                  moq_simpair_t **sp, moq_publisher_t **pub_out,
                  moq_pub_track_t **track_out, bool sub_first,
                  uint32_t max_actions)
{
    fanout_setup_ver(as, alloc, sp, pub_out, track_out, sub_first,
                     max_actions, 0);
}

/* Count client OBJECT_RECEIVED events matching (group, object, bytes). */
int fanout_count_objects(moq_simpair_t *sp, uint64_t g, uint64_t o,
                         const uint8_t *bytes, size_t len)
{
    int n = 0;
    moq_event_t evts[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (evts[i].kind == MOQ_EVENT_OBJECT_RECEIVED &&
                evts[i].u.object_received.group_id == g &&
                evts[i].u.object_received.object_id == o &&
                moq_rcbuf_len(evts[i].u.object_received.payload) == len &&
                memcmp(moq_rcbuf_data(evts[i].u.object_received.payload),
                       bytes, len) == 0)
                n++;
            moq_event_cleanup(&evts[i]);
        }
    return n;
}
void count_release(void *ctx, const uint8_t *data, size_t len)
{ (void)ctx; (void)data; (void)len; g_wrap_releases++; }
/* -- Event-parity: manual moq_pub_handle_event forwarding == tick ------- */

/* Drain a session's queued actions (the manual-mode consumer's job before a
 * flush retry). */
void manual_drain_actions(moq_session_t *s) {
    moq_action_t acts[16]; size_t na;
    while ((na = moq_session_poll_actions(s, acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
}

/* Forward every pending server event through the PUBLIC manual API
 * (moq_pub_handle_event) rather than moq_pub_tick, SERIALIZED per the header
 * contract: one event fully processed (drain + moq_pub_flush to MOQ_OK on
 * WOULD_BLOCK) before the next is polled; a borrowed event is never held
 * across a flush or resubmitted. Records, for the target kind `want`, how many
 * were seen and the last result. */
void manual_forward(moq_publisher_t *pub, moq_session_t *sv,
                    uint64_t now, moq_event_kind_t want,
                    int *seen, moq_pub_event_result_t *last_res) {
    if (seen) *seen = 0;
    moq_event_t ev;
    /* Serialized: process ONE event to completion before polling the next.
     * On WOULD_BLOCK, drain actions + flush until OK -- never hold or resubmit
     * the borrowed event (its spans die at the next advancing call). Because
     * the previous event is fully flushed before the next poll, the single
     * pending slot is always free when a new event is handed in, so
     * WOULD_BLOCK + IGNORED (prior-slot-occupied) never arises here. */
    while (moq_session_poll_events(sv, &ev, 1) == 1) {
        moq_pub_event_result_t res = MOQ_PUB_EVENT_IGNORED;
        moq_result_t rc = moq_pub_handle_event(pub, &ev, now, &res);
        if (want != 0 && ev.kind == want) {
            if (seen) (*seen)++;
            if (last_res) *last_res = res;
        }
        moq_event_cleanup(&ev);
        /* Serialized contract: this event MUST be fully flushed to MOQ_OK
         * before the next poll. Any hard error, or failure to reach OK within
         * a bounded number of drains, is a contract violation -- fail, never
         * poll the next event over a still-blocked slot. */
        for (int g = 0; rc == MOQ_ERR_WOULD_BLOCK && g < 32; g++) {
            manual_drain_actions(sv);
            rc = moq_pub_flush(pub, now);
        }
        MOQ_TEST_CHECK(rc == MOQ_OK);
    }
    /* Final flush drives any armed retirement / deferred fetch to completion. */
    moq_result_t rc = MOQ_ERR_WOULD_BLOCK;
    for (int g = 0; g < 32; g++) {
        rc = moq_pub_flush(pub, now);
        if (rc != MOQ_ERR_WOULD_BLOCK) break;
        manual_drain_actions(sv);
    }
    MOQ_TEST_CHECK(rc == MOQ_OK);
}

/* Count (and drain) the server's queued RESET_DATA actions. */
int drain_count_resets(moq_session_t *s) {
    int n = 0; moq_action_t acts[16]; size_t na;
    while ((na = moq_session_poll_actions(s, acts, 16)) > 0)
        for (size_t i = 0; i < na; i++) {
            if (acts[i].kind == MOQ_ACTION_RESET_DATA) n++;
            moq_action_cleanup(&acts[i]);
        }
    return n;
}
/* ========== Subscription-filter window enforcement  ========== *
 * The facade copies each resolved window from the session's internal
 * accessors at slot install / filter update and gates fan-out membership at
 * op admission: eligibility = active && Forward=1 && window admits the
 * location. History stays independent of delivery. */

void windows_setup(test_alloc_state_t *as, moq_alloc_t *alloc,
                   moq_simpair_t **sp, moq_version_t ver,
                   moq_publisher_t **pub, moq_pub_track_t **track)
{
    *as = (test_alloc_state_t){0};
    *alloc = test_allocator(as);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.version = ver;
    moq_simpair_create(&cfg, sp);
    moq_simpair_start(*sp);
    moq_simpair_run_until_quiescent(*sp, 8, NULL);
    { moq_event_t ev;
      if (moq_session_poll_events(moq_simpair_client(*sp), &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(moq_simpair_server(*sp), &ev, 1) == 1)
          moq_event_cleanup(&ev); }

    moq_pub_cfg_t cfg2; moq_pub_cfg_init_sized(&cfg2, sizeof(cfg2));
    cfg2.accept_mode = MOQ_PUB_ACCEPT_ALL;
    *pub = NULL;
    moq_pub_create(moq_simpair_server(*sp), alloc, &cfg2, pub);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
    tcfg.track_namespace.parts = ns_parts;
    tcfg.track_namespace.count = 1;
    tcfg.track_name = MOQ_BYTES_LITERAL("video");
    *track = NULL;
    moq_pub_add_track(*pub, &tcfg, moq_simpair_now_us(*sp), track);
}

/* Client subscribes with a filter; the facade accepts (manual_forward). */
moq_subscription_t windows_subscribe(moq_simpair_t *sp,
    moq_publisher_t *pub, moq_subscribe_filter_t filter,
    uint64_t sg, uint64_t so, uint64_t eg)
{
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace.parts = ns_parts;
    scfg.track_namespace.count = 1;
    scfg.track_name = MOQ_BYTES_LITERAL("video");
    scfg.filter = filter;
    scfg.start_group = sg; scfg.start_object = so; scfg.end_group = eg;
    moq_subscription_t sub_h;
    moq_session_subscribe(cl, &scfg, moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    manual_forward(pub, sv, moq_simpair_now_us(sp), 0, NULL, NULL);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    { moq_event_t ev;
      while (moq_session_poll_events(cl, &ev, 1) == 1) moq_event_cleanup(&ev); }
    return sub_h;
}

/* Datagram write (location-order-free) with a per-probe payload tag. */
moq_result_t windows_write_dg(moq_publisher_t *pub,
    moq_pub_track_t *t, moq_alloc_t *alloc, moq_simpair_t *sp,
    uint64_t g, uint64_t o, uint8_t tag)
{
    uint8_t d[2] = { tag, (uint8_t)(tag ^ 0xFFu) };
    moq_rcbuf_t *p = NULL; moq_rcbuf_create(alloc, d, sizeof(d), &p);
    moq_pub_object_cfg_t oc; moq_pub_object_cfg_init(&oc);
    oc.group_id = g; oc.object_id = o; oc.payload = p; oc.datagram = true;
    moq_result_t rc = moq_pub_write_object_ex(pub, t, &oc,
                                              moq_simpair_now_us(sp));
    moq_rcbuf_decref(p);
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    return rc;
}

/* Count client deliveries of the tagged probe at {g,o}. */
int windows_count(moq_simpair_t *sp, uint64_t g, uint64_t o, uint8_t tag)
{
    uint8_t d[2] = { tag, (uint8_t)(tag ^ 0xFFu) };
    return fanout_count_objects(sp, g, o, d, sizeof(d));
}

/* Inject a subscriber-side REQUEST_UPDATE carrying ONLY a SUBSCRIPTION_FILTER
 * into the facade's session (the public update cfg cannot send filters until
 * update), then let the facade consume the SUBSCRIBE_UPDATED. req_seq: fresh
 * client-side request id ordinal (first update = 1 -> id 2). */
void windows_inject_sub_filter(moq_simpair_t *sp, moq_version_t ver,
    int req_seq, moq_subscribe_filter_t filter,
    uint64_t sg, uint64_t so, uint64_t eg)
{
    moq_session_t *sv = moq_simpair_server(sp);
    uint8_t buf[160]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    if (ver == MOQ_VERSION_DRAFT_18) {
        moq_d18_msg_params_t mp = {0};
        mp.has_filter = true;
        mp.filter_type = filter;
        mp.filter_start_group = sg;
        mp.filter_start_object = so;
        mp.filter_end_group = eg;
        MOQ_TEST_CHECK(moq_d18_encode_request_update(
            &w, (uint64_t)(2 * req_seq), &mp) == MOQ_OK);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        for (size_t i = 0; i < sv->sub_cap; i++)
            if (sv->subs[i].state != MOQ_SUB_FREE &&
                sv->subs[i].role == MOQ_SUB_ROLE_PUBLISHER &&
                sv->subs[i].rs.ref._v != 0)
                ref = sv->subs[i].rs.ref;
        MOQ_TEST_CHECK(ref._v != 0);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            sv, ref, buf, moq_buf_writer_offset(&w), false,
            moq_simpair_now_us(sp)) == MOQ_OK);
    } else {
        uint8_t fbuf[32]; size_t flen = 0;
        moq_d16_subscription_filter_t f = {
            .filter_type = filter, .start_group = sg,
            .start_object = so, .end_group = eg };
        MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(
            fbuf, sizeof(fbuf), &flen, &f) == MOQ_OK);
        moq_kvp_entry_t params[] = {
            { .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
              .value = fbuf, .value_len = flen, .is_varint = false },
        };
        MOQ_TEST_CHECK(moq_d16_encode_request_update(
            &w, (uint64_t)(2 * req_seq), 0 /* existing = first request */,
            params, 1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(
            sv, buf, moq_buf_writer_offset(&w),
            moq_simpair_now_us(sp)) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    /* Swallow the queued acknowledgment action: the injection is one-sided (the
     * client never sent this update), so delivering the ack would close the
     * client on an unsolicited response. The pair was quiescent before the
     * injection, so the only queued actions are the ack bytes. */
    { moq_action_t a;
      while (moq_session_poll_actions(sv, &a, 1) == 1) moq_action_cleanup(&a); }
}

/* Count client-side SUBSCRIBE_DONEs, capturing the last status/count. */
int declare_client_dones(moq_simpair_t *sp, uint64_t *st, uint64_t *sc)
{
    int n = 0;
    moq_event_t d[16]; size_t ne;
    while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
        for (size_t i = 0; i < ne; i++) {
            if (d[i].kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                n++;
                if (st) *st = d[i].u.subscribe_done.status_code;
                if (sc) *sc = d[i].u.subscribe_done.stream_count;
            }
            moq_event_cleanup(&d[i]);
        }
    return n;
}

/* One-shot STREAM write helper (opens/uses the per-slot subgroup). */
moq_result_t declare_write_stream(moq_publisher_t *pub,
    moq_pub_track_t *t, moq_alloc_t *alloc, moq_simpair_t *sp,
    uint64_t g, uint64_t o, uint8_t tag)
{
    uint8_t d[2] = { tag, (uint8_t)(tag ^ 0xFFu) };
    moq_rcbuf_t *pl = NULL; moq_rcbuf_create(alloc, d, sizeof(d), &pl);
    moq_result_t rc = moq_pub_write_object(pub, t, g, o, pl,
                                           moq_simpair_now_us(sp));
    moq_rcbuf_decref(pl);
    return rc;
}

/* One-shot STREAM write with an explicit end_of_group flag. */
moq_result_t mono_write_stream_eog(moq_publisher_t *pub,
    moq_pub_track_t *t, moq_alloc_t *alloc, moq_simpair_t *sp,
    uint64_t g, uint64_t o, bool eog, uint8_t tag)
{
    uint8_t d[2] = { tag, (uint8_t)(tag ^ 0xFFu) };
    moq_rcbuf_t *pl = NULL; moq_rcbuf_create(alloc, d, sizeof(d), &pl);
    moq_pub_object_cfg_t oc;
    moq_pub_object_cfg_init_sized(&oc, sizeof(oc));
    oc.group_id = g; oc.object_id = o; oc.payload = pl;
    oc.end_of_group = eog;
    moq_result_t rc = moq_pub_write_object_ex(pub, t, &oc,
                                              moq_simpair_now_us(sp));
    moq_rcbuf_decref(pl);
    return rc;
}

/* Drive the pair and drain the client: how many times {g,o} arrived, with
 * the FIRST_OBJECT flag and Subgroup ID of its last arrival, and every end
 * of the subscription (a malformed track ends it at the subscriber). */
int fo_seen(moq_simpair_t *sp, uint64_t g, uint64_t o, bool *first,
            uint64_t *sgid, int *ended)
{
    int n = 0;
    moq_event_t ev;
    for (int round = 0; round < 4; round++) {
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
                ev.u.object_received.group_id == g &&
                ev.u.object_received.object_id == o) {
                n++;
                if (first) *first = ev.u.object_received.first_object;
                if (sgid) *sgid = ev.u.object_received.subgroup_id;
            }
            if (ev.kind == MOQ_EVENT_UNSUBSCRIBED ||
                ev.kind == MOQ_EVENT_SUBSCRIBE_DONE)
                (*ended)++;
            moq_event_cleanup(&ev);
        }
    }
    return n;
}
