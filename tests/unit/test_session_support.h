#ifndef TEST_SESSION_SUPPORT_H
#define TEST_SESSION_SUPPORT_H

#include <moq/codec.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include <stdlib.h>
#include <string.h>

#ifdef __GNUC__
#define TSS_UNUSED __attribute__((unused))
#else
#define TSS_UNUSED
#endif

/* -- White-box helpers (inlined to avoid internal symbol exports) --- */

TSS_UNUSED static int
test_sub_resolve_handle(moq_session_t *s, moq_subscription_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_SUBSCRIPTION) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->sub_cap) return -1;
    if (s->subs[slot].generation != gen) return -1;
    if (s->subs[slot].state == MOQ_SUB_FREE) return -1;
    return (int)slot;
}

TSS_UNUSED static bool
test_unsub_tomb_add(moq_session_t *s, uint64_t request_id)
{
    if (s->unsub_tomb_count >= s->unsub_tomb_cap) return false;
    s->unsub_tombstones[s->unsub_tomb_count++] = request_id;
    return true;
}

TSS_UNUSED static bool
test_unsub_tomb_consume(moq_session_t *s, uint64_t request_id)
{
    for (size_t i = 0; i < s->unsub_tomb_count; i++) {
        if (s->unsub_tombstones[i] == request_id) {
            s->unsub_tombstones[i] =
                s->unsub_tombstones[--s->unsub_tomb_count];
            return true;
        }
    }
    return false;
}

/* -- Counting allocator -------------------------------------------- */

typedef struct test_alloc_state {
    int64_t balance;
} test_alloc_state_t;

TSS_UNUSED static void *test_alloc(size_t size, void *ctx)
{
    test_alloc_state_t *state = (test_alloc_state_t *)ctx;
    void *p = malloc(size);
    if (p) state->balance++;
    return p;
}

TSS_UNUSED static void *test_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    (void)old_size; (void)ctx;
    return realloc(ptr, new_size);
}

TSS_UNUSED static void test_free(void *ptr, size_t size, void *ctx)
{
    test_alloc_state_t *state = (test_alloc_state_t *)ctx;
    (void)size;
    if (ptr) state->balance--;
    free(ptr);
}

TSS_UNUSED static moq_alloc_t test_allocator(test_alloc_state_t *state)
{
    moq_alloc_t alloc = { state, test_alloc, test_realloc, test_free };
    return alloc;
}

/* -- Pump actions -------------------------------------------------- */

TSS_UNUSED static void pump_actions_to_peer(moq_session_t *from, moq_session_t *to,
                                             uint64_t now)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(from, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL)
                moq_session_on_control_bytes(to,
                    acts[i].u.send_control.data,
                    acts[i].u.send_control.len, now);
            else if (acts[i].kind == MOQ_ACTION_SEND_DATAGRAM)
                moq_session_on_datagram(to,
                    acts[i].u.send_datagram.data,
                    acts[i].u.send_datagram.len, now);
        }
    }
}

/* -- Test helper: fill action queue -------------------------------- */

TSS_UNUSED static void test_session_fill_action_queue(moq_session_t *s) {
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_CONTROL;
    a.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
    while (push_action(s, &a) == MOQ_OK) { /* fill all slots */ }
}

/* -- Action decode helpers ----------------------------------------- */

TSS_UNUSED static uint64_t decode_action_msg_type(const moq_action_t *a)
{
    if (a->kind != MOQ_ACTION_SEND_CONTROL || a->u.send_control.len < 1)
        return UINT64_MAX;
    moq_control_envelope_t env;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, a->u.send_control.data, a->u.send_control.len);
    if (moq_control_decode_envelope(&r, &env) < 0) return UINT64_MAX;
    return env.msg_type;
}

TSS_UNUSED static moq_result_t decode_action_subscribe_ok(const moq_action_t *a,
                                                           moq_d16_subscribe_ok_t *out)
{
    if (a->kind != MOQ_ACTION_SEND_CONTROL) return MOQ_ERR_INVAL;
    moq_control_envelope_t env;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, a->u.send_control.data, a->u.send_control.len);
    moq_result_t rc = moq_control_decode_envelope(&r, &env);
    if (rc < 0) return rc;
    if (env.msg_type != MOQ_D16_SUBSCRIBE_OK) return MOQ_ERR_INVAL;
    return moq_d16_decode_subscribe_ok(env.payload, env.payload_len, out);
}

TSS_UNUSED static moq_result_t decode_action_request_error(const moq_action_t *a,
                                                            moq_d16_request_error_t *out)
{
    if (a->kind != MOQ_ACTION_SEND_CONTROL) return MOQ_ERR_INVAL;
    moq_control_envelope_t env;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, a->u.send_control.data, a->u.send_control.len);
    moq_result_t rc = moq_control_decode_envelope(&r, &env);
    if (rc < 0) return rc;
    if (env.msg_type != MOQ_D16_REQUEST_ERROR) return MOQ_ERR_INVAL;
    return moq_d16_decode_request_error(env.payload, env.payload_len, out);
}

/* -- Feed helpers -------------------------------------------------- */

TSS_UNUSED static moq_result_t feed_subscribe_ok(moq_session_t *s, uint64_t request_id,
                                                  uint64_t track_alias,
                                                  const moq_kvp_entry_t *params,
                                                  size_t params_count)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = moq_d16_encode_subscribe_ok(&w, request_id, track_alias,
                                                    params, params_count, NULL, 0);
    if (rc < 0) return rc;
    size_t len = moq_buf_writer_offset(&w);
    if (len == 0) return MOQ_ERR_INVAL;
    return moq_session_on_control_bytes(s, buf, len, 0);
}

TSS_UNUSED static moq_result_t feed_raw_subscribe_ok(moq_session_t *s,
                                                      const uint8_t *payload,
                                                      size_t payload_len)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = moq_control_encode_envelope(&w, MOQ_D16_SUBSCRIBE_OK,
                                                    payload,
                                                    (uint16_t)payload_len);
    if (rc < 0) return rc;
    return moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 0);
}

TSS_UNUSED static void feed_subscribe(moq_session_t *sv, uint64_t request_id,
                                       const char *ns_field, const char *track_name,
                                       const moq_kvp_entry_t *params, size_t params_count)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_bytes_t parts[] = { moq_bytes_cstr(ns_field) };
    moq_namespace_t ns = { parts, 1 };
    moq_d16_encode_subscribe(&w, request_id, &ns,
        moq_bytes_cstr(track_name), params, params_count);
    moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
}

/* -- Establish pair ------------------------------------------------ */

TSS_UNUSED static void establish_pair(moq_alloc_t *alloc,
                                       uint64_t client_max, uint64_t server_max,
                                       moq_session_t **c_out, moq_session_t **s_out,
                                       const moq_session_cfg_t *c_extra,
                                       const moq_session_cfg_t *s_extra)
{
    moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
    ccfg.alloc = alloc;
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    if (client_max > 0) {
        ccfg.send_request_capacity = true;
        ccfg.initial_request_capacity = client_max;
    }
    if (c_extra) {
        ccfg.max_actions = c_extra->max_actions;
        ccfg.max_events = c_extra->max_events;
        ccfg.send_buffer_size = c_extra->send_buffer_size;
        ccfg.recv_buffer_size = c_extra->recv_buffer_size;
        ccfg.max_subscriptions = c_extra->max_subscriptions;
        ccfg.output_scratch_size = c_extra->output_scratch_size;
        ccfg.max_open_subgroups = c_extra->max_open_subgroups;
        ccfg.max_data_streams = c_extra->max_data_streams;
        ccfg.max_object_payload_size = c_extra->max_object_payload_size;
        ccfg.max_receive_buffer_bytes = c_extra->max_receive_buffer_bytes;
        ccfg.max_announcements = c_extra->max_announcements;
        ccfg.send_auth_token_cache_size = c_extra->send_auth_token_cache_size;
        ccfg.auth_token_cache_size = c_extra->auth_token_cache_size;
        ccfg.streaming_objects = c_extra->streaming_objects;
        ccfg.goaway_timeout_us = c_extra->goaway_timeout_us;
        ccfg.max_namespace_subscriptions = c_extra->max_namespace_subscriptions;
        ccfg.max_fetches = c_extra->max_fetches;
        ccfg.max_publishes = c_extra->max_publishes;
        ccfg.max_track_statuses = c_extra->max_track_statuses;
        ccfg.idle_timeout_us = c_extra->idle_timeout_us;
    }
    moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
    scfg.alloc = alloc;
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    if (server_max > 0) {
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = server_max;
    }
    if (s_extra) {
        scfg.max_actions = s_extra->max_actions;
        scfg.max_events = s_extra->max_events;
        scfg.send_buffer_size = s_extra->send_buffer_size;
        scfg.recv_buffer_size = s_extra->recv_buffer_size;
        scfg.max_open_subgroups = s_extra->max_open_subgroups;
        scfg.max_subscriptions = s_extra->max_subscriptions;
        scfg.output_scratch_size = s_extra->output_scratch_size;
        scfg.max_data_streams = s_extra->max_data_streams;
        scfg.max_object_payload_size = s_extra->max_object_payload_size;
        scfg.max_receive_buffer_bytes = s_extra->max_receive_buffer_bytes;
        scfg.max_announcements = s_extra->max_announcements;
        scfg.send_auth_token_cache_size = s_extra->send_auth_token_cache_size;
        scfg.auth_token_cache_size = s_extra->auth_token_cache_size;
        scfg.streaming_objects = s_extra->streaming_objects;
        scfg.goaway_timeout_us = s_extra->goaway_timeout_us;
        scfg.max_namespace_subscriptions = s_extra->max_namespace_subscriptions;
        scfg.max_fetches = s_extra->max_fetches;
        scfg.max_publishes = s_extra->max_publishes;
        scfg.max_track_statuses = s_extra->max_track_statuses;
        scfg.idle_timeout_us = s_extra->idle_timeout_us;
    }
    moq_session_create(&ccfg, 0, c_out);
    moq_session_create(&scfg, 0, s_out);
    moq_session_start(*c_out, 0);
    pump_actions_to_peer(*c_out, *s_out, 0);
    pump_actions_to_peer(*s_out, *c_out, 0);
    moq_event_t drain;
    moq_session_poll_events(*c_out, &drain, 1);
    moq_session_poll_events(*s_out, &drain, 1);
}

/* -- Reject subscribe helper --------------------------------------- */

TSS_UNUSED static moq_result_t reject_sub(moq_session_t *s, moq_subscription_t sub,
                                           moq_request_error_t code, moq_bytes_t reason,
                                           bool can_retry, uint64_t retry_ms, uint64_t now)
{
    moq_reject_subscribe_cfg_t cfg;
    moq_reject_subscribe_cfg_init(&cfg);
    cfg.error_code = code;
    cfg.reason = reason;
    cfg.can_retry = can_retry;
    cfg.retry_after_ms = retry_ms;
    return moq_session_reject_subscribe(s, sub, &cfg, now);
}

/* -- FEED_SEND_DATA macro ------------------------------------------ */

#define FEED_SEND_DATA(to, ref, act, now) do { \
    bool _hp = ((act).u.send_data.payload != NULL); \
    bool _fin = (act).u.send_data.fin; \
    if ((act).u.send_data.header_len > 0) \
        moq_session_on_data_bytes((to), (ref), \
            (act).u.send_data.header, (act).u.send_data.header_len, \
            _fin && !_hp, (now)); \
    if (_hp) \
        moq_session_on_data_bytes((to), (ref), \
            moq_rcbuf_data((act).u.send_data.payload), \
            moq_rcbuf_len((act).u.send_data.payload), \
            _fin, (now)); \
    if (!_hp && (act).u.send_data.header_len == 0 && _fin) \
        moq_session_on_data_bytes((to), (ref), NULL, 0, true, (now)); \
} while (0)

/* -- Byte-counting allocator --------------------------------------- */

typedef struct byte_alloc_state {
    int64_t  balance;
    int64_t  live_bytes;
    int64_t  peak_bytes;
    int64_t  realloc_calls;  /* successful reallocs of an existing pointer */
} byte_alloc_state_t;

TSS_UNUSED static void *byte_alloc(size_t size, void *ctx) {
    byte_alloc_state_t *s = (byte_alloc_state_t *)ctx;
    void *p = malloc(size);
    if (p) { s->balance++; s->live_bytes += (int64_t)size;
             if (s->live_bytes > s->peak_bytes) s->peak_bytes = s->live_bytes; }
    return p;
}

TSS_UNUSED static void *byte_realloc(void *ptr, size_t old_sz, size_t new_sz, void *ctx) {
    byte_alloc_state_t *s = (byte_alloc_state_t *)ctx;
    void *p = realloc(ptr, new_sz);
    if (p) { s->live_bytes += (int64_t)new_sz - (int64_t)old_sz;
             if (s->live_bytes > s->peak_bytes) s->peak_bytes = s->live_bytes;
             if (ptr) s->realloc_calls++; }
    return p;
}

TSS_UNUSED static void byte_free(void *ptr, size_t size, void *ctx) {
    byte_alloc_state_t *s = (byte_alloc_state_t *)ctx;
    if (ptr) { s->balance--; s->live_bytes -= (int64_t)size; }
    free(ptr);
}

/* -- Fail-at-N allocator ------------------------------------------- */

typedef struct fail_alloc_state {
    int64_t balance;
    int     call_count;
    int     fail_at;
    size_t  fail_size;      /* if >0, fail any allocation of exactly this size */
    size_t  fail_min_size;  /* if >0, fail any allocation >= this size */
} fail_alloc_state_t;

TSS_UNUSED static void *fail_alloc_fn(size_t size, void *ctx) {
    fail_alloc_state_t *s = (fail_alloc_state_t *)ctx;
    s->call_count++;
    if (s->fail_at > 0 && s->call_count == s->fail_at) return NULL;
    if (s->fail_size > 0 && size == s->fail_size) return NULL;
    if (s->fail_min_size > 0 && size >= s->fail_min_size) return NULL;
    void *p = malloc(size);
    if (p) s->balance++;
    return p;
}

TSS_UNUSED static void *fail_realloc_fn(void *ptr, size_t old_sz, size_t new_sz, void *ctx) {
    fail_alloc_state_t *s = (fail_alloc_state_t *)ctx;
    (void)old_sz;
    s->call_count++;
    if (s->fail_at > 0 && s->call_count == s->fail_at) return NULL;
    if (s->fail_size > 0 && new_sz == s->fail_size) return NULL;
    if (s->fail_min_size > 0 && new_sz >= s->fail_min_size) return NULL;
    if (!ptr) { void *p = malloc(new_sz); if (p) s->balance++; return p; }
    return realloc(ptr, new_sz);
}

TSS_UNUSED static void fail_free_fn(void *ptr, size_t size, void *ctx) {
    fail_alloc_state_t *s = (fail_alloc_state_t *)ctx;
    (void)size;
    if (ptr) s->balance--;
    free(ptr);
}

TSS_UNUSED static moq_alloc_t fail_allocator(fail_alloc_state_t *state)
{
    moq_alloc_t alloc = { state, fail_alloc_fn, fail_realloc_fn, fail_free_fn };
    return alloc;
}

#endif /* TEST_SESSION_SUPPORT_H */
