#include <moq/moq.h>
#include "test_support.h"
#include "test_oom_support.h"
#include <string.h>

/*
 * OOM sweep tests: for each scenario, learn the allocation count with
 * no failures, then run fail_at = 1..count. Every failure path must
 * return an error or close deterministically, and destroy must free
 * all memory (balance == 0).
 */

/* -- Scenario: session create/destroy ------------------------------ */

static uint64_t scenario_create_destroy(moq_alloc_t *alloc)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    moq_result_t rc = moq_session_create(&cfg, 0, &s);
    if (rc < 0) return 0;
    moq_session_destroy(s);
    return 1;
}

/* -- Scenario: setup handshake ------------------------------------- */

static uint64_t scenario_setup_handshake(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    moq_session_start(c, 0);

    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    }
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    }

    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    uint64_t ok = (moq_session_state(c) == MOQ_SESS_ESTABLISHED &&
                    moq_session_state(sv) == MOQ_SESS_ESTABLISHED) ? 1 : 0;
    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: subscribe happy path -------------------------------- */

static uint64_t scenario_subscribe_happy(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    /* Setup. */
    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Subscribe. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("track");

    moq_subscription_t h;
    moq_result_t rc = moq_session_subscribe(c, &sub_cfg, 1000, &h);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump SUBSCRIBE to server. */
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    /* Server accepts. */
    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    rc = moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub,
                                       &acc, 1000);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump SUBSCRIBE_OK to client. */
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    uint64_t ok = 0;
    if (moq_session_poll_events(c, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_SUBSCRIBE_OK)
        ok = 1;

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: reject subscribe ------------------------------------ */

static uint64_t scenario_subscribe_reject(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("track");

    moq_subscription_t h;
    if (moq_session_subscribe(c, &sub_cfg, 1000, &h) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    if (moq_session_poll_events(sv, &ev, 1) != 1) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    moq_reject_subscribe_cfg_t rej;
    moq_reject_subscribe_cfg_init(&rej);
    rej.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    rej.reason = MOQ_BYTES_LITERAL("gone");
    moq_result_t rc = moq_session_reject_subscribe(sv,
        ev.u.subscribe_request.sub, &rej, 1000);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    uint64_t ok = 0;
    if (moq_session_poll_events(c, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR)
        ok = 1;

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: object receive happy path ---------------------------- */

static uint64_t scenario_object_receive(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

    moq_subscription_t h;
    if (moq_session_subscribe(c, &sub_cfg, 1000, &h) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_subscription_t ssub = ev.u.subscribe_request.sub;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(sv, ssub, &acc, 1000) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    if (moq_session_poll_events(c, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_OK) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server writes one object. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(sv, ssub, &sg_cfg, 1000, &sg) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)"X", 1, &p) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_result_t wrc = moq_session_write_object(sv, sg, 0, p, 1000);
    moq_rcbuf_decref(p);
    if (wrc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_session_close_subgroup(sv, sg, 1000);

    /* Feed SEND_DATA to client: header and payload separately. */
    moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(99);
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                bool hp = (acts[i].u.send_data.payload != NULL);
                bool dfin = acts[i].u.send_data.fin;
                if (acts[i].u.send_data.header_len > 0)
                    moq_session_on_data_bytes(c, rx_ref,
                        acts[i].u.send_data.header,
                        acts[i].u.send_data.header_len,
                        dfin && !hp, 1000);
                if (hp)
                    moq_session_on_data_bytes(c, rx_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        dfin, 1000);
                if (!hp && acts[i].u.send_data.header_len == 0 && dfin)
                    moq_session_on_data_bytes(c, rx_ref,
                        NULL, 0, true, 1000);
            }
            moq_action_cleanup(&acts[i]);
        }
    }

    uint64_t ok = 0;
    if (moq_session_poll_events(c, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
        ok = 1;
        moq_event_cleanup(&ev);
    }

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: publish_namespace happy path ------------------------- */

static uint64_t scenario_publish_namespace(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    /* Setup. */
    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server publishes namespace. */
    moq_bytes_t ns_parts[] = { moq_bytes_cstr("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_publish_namespace_cfg_t pn_cfg;
    moq_publish_namespace_cfg_init(&pn_cfg);
    pn_cfg.track_namespace = ns;

    moq_announcement_t ann;
    moq_result_t rc = moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump PUBLISH_NAMESPACE to client. */
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    if (moq_session_poll_events(c, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_NAMESPACE_PUBLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Client accepts. */
    moq_accept_namespace_cfg_t acc;
    moq_accept_namespace_cfg_init(&acc);
    rc = moq_session_accept_namespace(c, ev.u.namespace_published.ann,
                                       &acc, 1000);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump REQUEST_OK to server. */
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    /* Drain NAMESPACE_ACCEPTED from server. */
    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_NAMESPACE_ACCEPTED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server does done. */
    rc = moq_session_publish_namespace_done(sv, ann, 2000);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump PUBLISH_NAMESPACE_DONE to client. */
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 2000);

    uint64_t ok = 0;
    if (moq_session_poll_events(c, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_NAMESPACE_DONE)
        ok = 1;

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: publish_namespace reject ----------------------------- */

static uint64_t scenario_publish_namespace_reject(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    /* Setup. */
    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server publishes namespace. */
    moq_bytes_t ns_parts[] = { moq_bytes_cstr("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_publish_namespace_cfg_t pn_cfg;
    moq_publish_namespace_cfg_init(&pn_cfg);
    pn_cfg.track_namespace = ns;

    moq_announcement_t ann;
    moq_result_t rc = moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump PUBLISH_NAMESPACE to client. */
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    if (moq_session_poll_events(c, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_NAMESPACE_PUBLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Client rejects. */
    moq_reject_namespace_cfg_t rej;
    moq_reject_namespace_cfg_init(&rej);
    rej.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    rej.reason = moq_bytes_cstr("no");
    rc = moq_session_reject_namespace(c, ev.u.namespace_published.ann,
                                       &rej, 1000);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump REQUEST_ERROR to server. */
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    uint64_t ok = 0;
    if (moq_session_poll_events(sv, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_NAMESPACE_REJECTED)
        ok = 1;

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: publish_namespace cancel ------------------------------ */

static uint64_t scenario_publish_namespace_cancel(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    /* Setup. */
    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server publishes namespace. */
    moq_bytes_t ns_parts[] = { moq_bytes_cstr("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_publish_namespace_cfg_t pn_cfg;
    moq_publish_namespace_cfg_init(&pn_cfg);
    pn_cfg.track_namespace = ns;

    moq_announcement_t ann;
    moq_result_t rc = moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump PUBLISH_NAMESPACE to client. */
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    if (moq_session_poll_events(c, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_NAMESPACE_PUBLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Client accepts. */
    moq_accept_namespace_cfg_t acc;
    moq_accept_namespace_cfg_init(&acc);
    moq_announcement_t client_ann = ev.u.namespace_published.ann;
    rc = moq_session_accept_namespace(c, client_ann, &acc, 1000);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump REQUEST_OK to server. */
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);

    /* Drain NAMESPACE_ACCEPTED from server. */
    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_NAMESPACE_ACCEPTED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Client cancels. */
    moq_cancel_namespace_cfg_t can;
    moq_cancel_namespace_cfg_init(&can);
    can.error_code = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
    rc = moq_session_cancel_namespace(c, client_ann, &can, 2000);
    if (rc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump PUBLISH_NAMESPACE_CANCEL to server. */
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++)
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 2000);

    uint64_t ok = 0;
    if (moq_session_poll_events(sv, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_NAMESPACE_CANCELLED)
        ok = 1;

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: fetch happy path ------------------------------------ */

static uint64_t scenario_fetch(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Client sends FETCH. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_fetch_cfg_t fcfg;
    moq_fetch_cfg_init(&fcfg);
    fcfg.track_namespace = ns;
    fcfg.track_name = MOQ_BYTES_LITERAL("t");
    fcfg.end_group = 1;

    moq_fetch_t fh;
    if (moq_session_fetch(c, &fcfg, 1000, &fh) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump FETCH to server. */
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    /* Server receives FETCH_REQUEST. */
    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_FETCH_REQUEST) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_fetch_t sfh = ev.u.fetch_request.fetch;

    /* Server accepts. */
    moq_accept_fetch_cfg_t afcfg;
    moq_accept_fetch_cfg_init(&afcfg);
    afcfg.end_group = 1;
    if (moq_session_accept_fetch(sv, sfh, &afcfg, 1000) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server writes one fetch object. */
    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)"F", 1, &p) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_fetch_object_cfg_t focfg;
    moq_fetch_object_cfg_init(&focfg);
    focfg.group_id = 0;
    focfg.object_id = 0;
    focfg.publisher_priority = 128;
    focfg.payload = p;
    moq_result_t wrc = moq_session_write_fetch_object(sv, sfh, &focfg, 1000);
    moq_rcbuf_decref(p);
    if (wrc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server ends fetch. */
    if (moq_session_end_fetch(sv, sfh, 1000) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump all server actions to client: SEND_CONTROL + SEND_DATA. */
    moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(99);
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            else if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                bool hp = (acts[i].u.send_data.payload != NULL);
                bool dfin = acts[i].u.send_data.fin;
                if (acts[i].u.send_data.header_len > 0)
                    moq_session_on_data_bytes(c, rx_ref,
                        acts[i].u.send_data.header,
                        acts[i].u.send_data.header_len,
                        dfin && !hp, 1000);
                if (hp)
                    moq_session_on_data_bytes(c, rx_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        dfin, 1000);
                if (!hp && acts[i].u.send_data.header_len == 0 && dfin)
                    moq_session_on_data_bytes(c, rx_ref,
                        NULL, 0, true, 1000);
            }
            moq_action_cleanup(&acts[i]);
        }
    }

    /* Client should receive FETCH_OK, FETCH_OBJECT, FETCH_COMPLETE. */
    uint64_t ok = 0;
    bool got_ok = false, got_obj = false, got_done = false;
    while (moq_session_poll_events(c, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_FETCH_OK) got_ok = true;
        else if (ev.kind == MOQ_EVENT_FETCH_OBJECT) got_obj = true;
        else if (ev.kind == MOQ_EVENT_FETCH_COMPLETE) got_done = true;
        moq_event_cleanup(&ev);
    }
    if (got_ok && got_obj && got_done)
        ok = 1;

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: publish happy path --------------------------------- */

static uint64_t scenario_publish_happy(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Client sends PUBLISH. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_publish_cfg_t pcfg;
    moq_publish_cfg_init(&pcfg);
    pcfg.track_namespace = ns;
    pcfg.track_name = MOQ_BYTES_LITERAL("t");

    moq_publication_t pub;
    if (moq_session_publish(c, &pcfg, 1000, &pub) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump PUBLISH to server. */
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    /* Server receives PUBLISH_REQUEST. */
    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_PUBLISH_REQUEST) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_publication_t sv_pub = ev.u.publish_request.pub;

    /* Server accepts. */
    moq_accept_publish_cfg_t apcfg;
    moq_accept_publish_cfg_init(&apcfg);
    if (moq_session_accept_publish(sv, sv_pub, &apcfg, 1000) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump PUBLISH_OK to client. */
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    if (moq_session_poll_events(c, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_PUBLISH_OK) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Client opens subgroup, writes object, closes. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    moq_subgroup_handle_t sg;
    if (moq_session_open_pub_subgroup(c, pub, &sg_cfg, 2000, &sg) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)"P", 1, &p) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_result_t wrc = moq_session_write_object(c, sg, 0, p, 2000);
    moq_rcbuf_decref(p);
    if (wrc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_session_close_subgroup(c, sg, 2000);

    /* Pump SEND_DATA to server. */
    moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(99);
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 2000);
            else if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                bool hp = (acts[i].u.send_data.payload != NULL);
                bool dfin = acts[i].u.send_data.fin;
                if (acts[i].u.send_data.header_len > 0)
                    moq_session_on_data_bytes(sv, rx_ref,
                        acts[i].u.send_data.header,
                        acts[i].u.send_data.header_len,
                        dfin && !hp, 2000);
                if (hp)
                    moq_session_on_data_bytes(sv, rx_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        dfin, 2000);
                if (!hp && acts[i].u.send_data.header_len == 0 && dfin)
                    moq_session_on_data_bytes(sv, rx_ref,
                        NULL, 0, true, 2000);
            }
            moq_action_cleanup(&acts[i]);
        }
    }

    /* Server should receive OBJECT_RECEIVED. */
    uint64_t ok = 0;
    if (moq_session_poll_events(sv, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
        ok = 1;
        moq_event_cleanup(&ev);
    }

    /* Client finishes publish. */
    if (ok) {
        moq_finish_publish_cfg_t fcfg;
        moq_finish_publish_cfg_init(&fcfg);
        moq_session_finish_publish(c, pub, &fcfg, 3000);
    }

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: track_status happy path ----------------------------- */

static uint64_t scenario_track_status(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Client sends TRACK_STATUS request. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_track_status_cfg_t tcfg;
    moq_track_status_cfg_init(&tcfg);
    tcfg.track_namespace = ns;
    tcfg.track_name = MOQ_BYTES_LITERAL("t");

    moq_track_status_handle_t th;
    if (moq_session_track_status(c, &tcfg, 1000, &th) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump to server. */
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    /* Server receives TRACK_STATUS_REQUEST. */
    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_TRACK_STATUS_REQUEST) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server accepts. */
    moq_accept_track_status_cfg_t atcfg;
    moq_accept_track_status_cfg_init(&atcfg);
    if (moq_session_accept_track_status(sv,
        ev.u.track_status_request.handle, &atcfg, 2000) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump back to client. */
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 2000);
            moq_action_cleanup(&acts[i]);
        }

    uint64_t ok = 0;
    if (moq_session_poll_events(c, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_TRACK_STATUS_OK)
        ok = 1;

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: object datagram ------------------------------------ */

static uint64_t scenario_object_datagram(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Subscribe: client subscribes, server accepts. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

    moq_subscription_t h;
    if (moq_session_subscribe(c, &sub_cfg, 1000, &h) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_subscription_t ssub = ev.u.subscribe_request.sub;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(sv, ssub, &acc, 1000) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    if (moq_session_poll_events(c, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_OK) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server sends object datagram. */
    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)"D", 1, &p) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_result_t drc = moq_session_send_object_datagram(sv, ssub,
        0, 0, 128, false, p, NULL, 0, 2000);
    moq_rcbuf_decref(p);
    if (drc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump SEND_DATAGRAM to client. */
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATAGRAM)
                moq_session_on_datagram(c,
                    acts[i].u.send_datagram.data,
                    acts[i].u.send_datagram.len, 2000);
            moq_action_cleanup(&acts[i]);
        }

    uint64_t ok = 0;
    if (moq_session_poll_events(c, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_OBJECT_RECEIVED &&
        ev.u.object_received.datagram == true) {
        ok = 1;
        moq_event_cleanup(&ev);
    }

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: streaming object ----------------------------------- */

static uint64_t scenario_streaming_object(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;
    ccfg.streaming_objects = true;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Subscribe + accept. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("t");

    moq_subscription_t h;
    if (moq_session_subscribe(c, &sub_cfg, 1000, &h) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_subscription_t ssub = ev.u.subscribe_request.sub;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(sv, ssub, &acc, 1000) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    if (moq_session_poll_events(c, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_OK) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server writes one object. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    moq_subgroup_handle_t sg;
    if (moq_session_open_subgroup(sv, ssub, &sg_cfg, 1000, &sg) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    moq_rcbuf_t *p = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)"S", 1, &p) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_result_t wrc = moq_session_write_object(sv, sg, 0, p, 1000);
    moq_rcbuf_decref(p);
    if (wrc < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_session_close_subgroup(sv, sg, 1000);

    /* Pump SEND_DATA to client. */
    moq_stream_ref_t rx_ref = moq_stream_ref_from_u64(99);
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_DATA) {
                bool hp = (acts[i].u.send_data.payload != NULL);
                bool dfin = acts[i].u.send_data.fin;
                if (acts[i].u.send_data.header_len > 0)
                    moq_session_on_data_bytes(c, rx_ref,
                        acts[i].u.send_data.header,
                        acts[i].u.send_data.header_len,
                        dfin && !hp, 1000);
                if (hp)
                    moq_session_on_data_bytes(c, rx_ref,
                        moq_rcbuf_data(acts[i].u.send_data.payload),
                        moq_rcbuf_len(acts[i].u.send_data.payload),
                        dfin, 1000);
                if (!hp && acts[i].u.send_data.header_len == 0 && dfin)
                    moq_session_on_data_bytes(c, rx_ref,
                        NULL, 0, true, 1000);
            }
            moq_action_cleanup(&acts[i]);
        }
    }

    /* Client should receive OBJECT_CHUNK events (streaming mode). */
    uint64_t ok = 0;
    while (moq_session_poll_events(c, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_CHUNK)
            ok = 1;
        moq_event_cleanup(&ev);
    }

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- Scenario: subscribe_namespace -------------------------------- */

static uint64_t scenario_subscribe_namespace(moq_alloc_t *alloc)
{
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 10;

    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), alloc, MOQ_PERSPECTIVE_SERVER);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 10;

    moq_session_t *c = NULL, *sv = NULL;
    if (moq_session_create(&ccfg, 0, &c) < 0) return 0;
    if (moq_session_create(&scfg, 0, &sv) < 0) {
        moq_session_destroy(c);
        return 0;
    }

    moq_session_start(c, 0);
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 0);
            moq_action_cleanup(&acts[i]);
        }
    moq_event_t ev;
    moq_session_poll_events(c, &ev, 1);
    moq_session_poll_events(sv, &ev, 1);

    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Server publishes namespace first. */
    moq_bytes_t ns_parts[] = { moq_bytes_cstr("ns") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_publish_namespace_cfg_t pn_cfg;
    moq_publish_namespace_cfg_init(&pn_cfg);
    pn_cfg.track_namespace = ns;

    moq_announcement_t ann;
    if (moq_session_publish_namespace(sv, &pn_cfg, 1000, &ann) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(c,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }

    if (moq_session_poll_events(c, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_NAMESPACE_PUBLISHED) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    moq_accept_namespace_cfg_t anc;
    moq_accept_namespace_cfg_init(&anc);
    if (moq_session_accept_namespace(c, ev.u.namespace_published.ann,
                                      &anc, 1000) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    while ((n = moq_session_poll_actions(c, acts, 16)) > 0)
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(sv,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, 1000);
            moq_action_cleanup(&acts[i]);
        }
    moq_session_poll_events(sv, &ev, 1); /* drain NAMESPACE_ACCEPTED */

    /* Client subscribes to namespace. */
    moq_subscribe_namespace_cfg_t sn_cfg;
    moq_subscribe_namespace_cfg_init(&sn_cfg);
    sn_cfg.track_namespace_prefix.parts = ns_parts;
    sn_cfg.track_namespace_prefix.count = 1;

    moq_ns_sub_handle_t nsh;
    if (moq_session_subscribe_namespace(c, &sn_cfg, 2000, &nsh) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump OPEN_BIDI_STREAM to server. */
    moq_stream_ref_t bidi_ref = moq_stream_ref_from_u64(99);
    while ((n = moq_session_poll_actions(c, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                bidi_ref = acts[i].u.open_bidi_stream.stream_ref;
                moq_session_on_bidi_stream_bytes(sv, bidi_ref,
                    acts[i].u.open_bidi_stream.data,
                    acts[i].u.open_bidi_stream.len, false, 2000);
            } else if (acts[i].kind == MOQ_ACTION_SEND_BIDI_STREAM) {
                moq_session_on_bidi_stream_bytes(sv,
                    acts[i].u.send_bidi_stream.stream_ref,
                    acts[i].u.send_bidi_stream.data,
                    acts[i].u.send_bidi_stream.len,
                    acts[i].u.send_bidi_stream.fin, 2000);
            }
            moq_action_cleanup(&acts[i]);
        }
    }

    /* Server receives NS_SUB_REQUEST. */
    if (moq_session_poll_events(sv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_NS_SUB_REQUEST) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }
    moq_ns_sub_handle_t srv_nsh = ev.u.ns_sub_request.handle;

    /* Server accepts. */
    moq_accept_ns_sub_cfg_t ans_cfg;
    moq_accept_ns_sub_cfg_init(&ans_cfg);
    if (moq_session_accept_ns_sub(sv, srv_nsh, &ans_cfg, 2000) < 0) {
        moq_session_destroy(c);
        moq_session_destroy(sv);
        return 0;
    }

    /* Pump SEND_BIDI_STREAM response back to client. */
    while ((n = moq_session_poll_actions(sv, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_BIDI_STREAM) {
                moq_session_on_bidi_stream_bytes(c,
                    acts[i].u.send_bidi_stream.stream_ref,
                    acts[i].u.send_bidi_stream.data,
                    acts[i].u.send_bidi_stream.len,
                    acts[i].u.send_bidi_stream.fin, 2000);
            }
            moq_action_cleanup(&acts[i]);
        }
    }

    uint64_t ok = 0;
    if (moq_session_poll_events(c, &ev, 1) == 1 &&
        ev.kind == MOQ_EVENT_NS_SUB_OK)
        ok = 1;

    moq_session_destroy(c);
    moq_session_destroy(sv);
    return ok;
}

/* -- OOM sweep runner ---------------------------------------------- */

typedef uint64_t (*oom_scenario_fn)(moq_alloc_t *alloc);

static int oom_sweep(int *failures, const char *name,
                      oom_scenario_fn scenario)
{
    /* First: learn allocation count with no failures. */
    oom_alloc_state_t baseline = {0};
    moq_alloc_t alloc = oom_allocator(&baseline);
    uint64_t ok = scenario(&alloc);
    if (ok != 1) {
        fprintf(stderr, "FAIL: %s: baseline scenario did not succeed\n",
                name);
        (*failures)++;
        return 1;
    }
    if (baseline.balance != 0) {
        fprintf(stderr, "FAIL: %s: baseline leaked (balance=%lld)\n",
                name, (long long)baseline.balance);
        (*failures)++;
        return 1;
    }
    uint64_t total_allocs = baseline.alloc_count;

    if (total_allocs == 0) {
        fprintf(stderr, "FAIL: %s: baseline allocated 0 times "
                "(scenario is a no-op)\n", name);
        (*failures)++;
        return 0;
    }

    int oom_failures = 0;

    /* Sweep: fail each allocation in turn. */
    for (uint64_t fail = 1; fail <= total_allocs; fail++) {
        oom_alloc_state_t state = { .fail_at = fail };
        moq_alloc_t fa = oom_allocator(&state);
        scenario(&fa);

        if (state.alloc_count < fail) {
            fprintf(stderr, "  OOM SKIP: %s fail_at=%llu but only "
                    "%llu allocs reached\n", name,
                    (unsigned long long)fail,
                    (unsigned long long)state.alloc_count);
            oom_failures++;
        }
        if (state.balance != 0) {
            fprintf(stderr, "  OOM LEAK: %s fail_at=%llu balance=%lld\n",
                    name, (unsigned long long)fail,
                    (long long)state.balance);
            oom_failures++;
        }
    }

    if (oom_failures > 0) {
        fprintf(stderr, "FAIL: %s: %d failure(s) in %llu OOM iterations\n",
                name, oom_failures, (unsigned long long)total_allocs);
        (*failures)++;
    }

    return oom_failures;
}

/* -- main ---------------------------------------------------------- */

int main(void)
{
    int failures = 0;

    oom_sweep(&failures, "create_destroy",     scenario_create_destroy);
    oom_sweep(&failures, "setup_handshake",    scenario_setup_handshake);
    oom_sweep(&failures, "subscribe_happy",    scenario_subscribe_happy);
    oom_sweep(&failures, "subscribe_reject",   scenario_subscribe_reject);
    oom_sweep(&failures, "object_receive",     scenario_object_receive);
    oom_sweep(&failures, "publish_namespace",  scenario_publish_namespace);
    oom_sweep(&failures, "publish_namespace_reject", scenario_publish_namespace_reject);
    oom_sweep(&failures, "publish_namespace_cancel", scenario_publish_namespace_cancel);
    oom_sweep(&failures, "fetch",                scenario_fetch);
    oom_sweep(&failures, "publish_happy",        scenario_publish_happy);
    oom_sweep(&failures, "track_status",         scenario_track_status);
    oom_sweep(&failures, "object_datagram",      scenario_object_datagram);
    oom_sweep(&failures, "streaming_object",     scenario_streaming_object);
    oom_sweep(&failures, "subscribe_namespace",  scenario_subscribe_namespace);

    MOQ_TEST_PASS("test_oom");
    return failures;
}
