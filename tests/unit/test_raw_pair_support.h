#ifndef MOQ_TEST_RAW_PAIR_SUPPORT_H
#define MOQ_TEST_RAW_PAIR_SUPPORT_H

/*
 * Two sessions joined by hand: every action one side queues is delivered to
 * the other in order, data streams offset so both directions stay distinct.
 * Each side takes the profile, request capacity, GOAWAY drain timeout, SETUP
 * tokens and token cache from a side_cfg_t.
 *
 * It needs only the public session API, so a test that links the shipping
 * (possibly shared) libraries can use it; test_raw_pair_d22.h adds raw
 * draft-22 message injection for white-box tests.
 */
#include <moq/moq.h>
#include "test_binding_support.h"
#include <string.h>

#if defined(__GNUC__) || defined(__clang__)
#define RP_UNUSED __attribute__((unused))
#else
#define RP_UNUSED
#endif

#define DATA_REF_OFFSET UINT64_C(0x100000)


/* -- Raw pair ------------------------------------------------------------- */

RP_UNUSED static void deliver(moq_session_t *to, const moq_action_t *a, bool from_server, uint64_t now)
{
    switch (a->kind) {
    case MOQ_ACTION_OPEN_UNI_CONTROL:
        moq_session_on_control_bytes(to, a->u.open_uni_control.data, a->u.open_uni_control.len, now);
        break;
    case MOQ_ACTION_SEND_UNI_CONTROL:
        moq_session_on_control_bytes(to, a->u.send_uni_control.data, a->u.send_uni_control.len, now);
        break;
    case MOQ_ACTION_SEND_CONTROL:
        moq_session_on_control_bytes(to, a->u.send_control.data, a->u.send_control.len, now);
        break;
    case MOQ_ACTION_OPEN_BIDI_STREAM:
        moq_session_on_bidi_stream_bytes(to, a->u.open_bidi_stream.stream_ref,
            a->u.open_bidi_stream.data, a->u.open_bidi_stream.len, a->u.open_bidi_stream.fin, now);
        break;
    case MOQ_ACTION_SEND_BIDI_STREAM:
        moq_session_on_bidi_stream_bytes(to, a->u.send_bidi_stream.stream_ref,
            a->u.send_bidi_stream.data, a->u.send_bidi_stream.len, a->u.send_bidi_stream.fin, now);
        break;
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        moq_session_on_bidi_stream_bytes(to, a->u.close_bidi_stream.stream_ref, NULL, 0, true, now);
        break;
    case MOQ_ACTION_ABORT_BIDI_STREAM:
        moq_session_on_bidi_stream_reset(to, a->u.abort_bidi_stream.stream_ref,
            a->u.abort_bidi_stream.error_code, now);
        moq_session_on_bidi_stream_stop(to, a->u.abort_bidi_stream.stream_ref,
            a->u.abort_bidi_stream.error_code, now);
        break;
    case MOQ_ACTION_SEND_DATA: {
        moq_stream_ref_t r = moq_stream_ref_from_u64(a->u.send_data.stream_ref._v +
            (from_server ? DATA_REF_OFFSET : 0));
        bool hp = a->u.send_data.payload != NULL;
        if (a->u.send_data.header_len > 0)
            moq_session_on_data_bytes(to, r, a->u.send_data.header, a->u.send_data.header_len,
                                      a->u.send_data.fin && !hp, now);
        if (hp)
            moq_session_on_data_bytes(to, r, moq_rcbuf_data(a->u.send_data.payload),
                                      moq_rcbuf_len(a->u.send_data.payload), a->u.send_data.fin, now);
        if (!hp && a->u.send_data.header_len == 0 && a->u.send_data.fin)
            moq_session_on_data_bytes(to, r, NULL, 0, true, now);
        break;
    }
    case MOQ_ACTION_RESET_DATA:
        moq_session_on_data_reset(to, moq_stream_ref_from_u64(a->u.reset_data.stream_ref._v +
            (from_server ? DATA_REF_OFFSET : 0)), a->u.reset_data.error_code, now);
        break;
    case MOQ_ACTION_STOP_DATA:
        moq_session_on_data_stop(to, moq_stream_ref_from_u64(a->u.stop_data.stream_ref._v -
            (from_server ? 0 : DATA_REF_OFFSET)), a->u.stop_data.error_code, now);
        break;
    default: break;
    }
}

/* Deliver one side's actions to the other; returns how many of `kind` went. */
RP_UNUSED static int pump(moq_session_t *from, moq_session_t *to, bool from_server, uint64_t now,
                uint32_t kind)
{
    int n = 0;
    moq_action_t a;
    while (moq_session_poll_actions(from, &a, 1) > 0) {
        if (a.kind == kind) n++;
        deliver(to, &a, from_server, now);
        moq_action_cleanup(&a);
    }
    return n;
}

RP_UNUSED static void run(moq_session_t *c, moq_session_t *sv, uint64_t now)
{
    for (int i = 0; i < 8; i++) {
        pump(c, sv, false, now, 0);
        pump(sv, c, true, now, 0);
    }
}

typedef struct {
    moq_version_t version;
    uint64_t      goaway_timeout_us;
    const moq_auth_token_t *setup_tokens;
    size_t        setup_token_count;
    uint64_t      token_cache;
} side_cfg_t;

RP_UNUSED static moq_session_t *session(moq_perspective_t persp, const side_cfg_t *sc)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = sc->version;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    cfg.goaway_timeout_us = sc->goaway_timeout_us;
    cfg.setup_auth_tokens = sc->setup_tokens;
    cfg.setup_auth_token_count = sc->setup_token_count;
    if (sc->token_cache) {
        cfg.send_auth_token_cache_size = true;
        cfg.auth_token_cache_size = sc->token_cache;
    }
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (test_bind_if_required(s) < 0) { moq_session_destroy(s); return NULL; }
    if (moq_session_start(s, 0) < 0 && persp == MOQ_PERSPECTIVE_CLIENT) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

RP_UNUSED static void drain(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
}

RP_UNUSED static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    moq_event_t ev;
    bool found = false;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (!found && ev.kind == kind) { *out = ev; found = true; continue; }
        moq_event_cleanup(&ev);
    }
    return found;
}

RP_UNUSED static bool pair_cfg(moq_session_t **c, moq_session_t **sv, const side_cfg_t *cc,
                     const side_cfg_t *sc, bool drain_setup)
{
    *c = session(MOQ_PERSPECTIVE_CLIENT, cc);
    *sv = session(MOQ_PERSPECTIVE_SERVER, sc);
    if (!*c || !*sv) {
        if (*c) moq_session_destroy(*c);
        if (*sv) moq_session_destroy(*sv);
        return false;
    }
    run(*c, *sv, 1);
    if (drain_setup) { drain(*c); drain(*sv); }
    return true;
}

RP_UNUSED static bool pair(moq_session_t **c, moq_session_t **sv, moq_version_t v)
{
    side_cfg_t sc = { .version = v };
    return pair_cfg(c, sv, &sc, &sc, true);
}

RP_UNUSED static void destroy(moq_session_t *c, moq_session_t *sv)
{
    moq_session_destroy(c);
    moq_session_destroy(sv);
}

#endif /* MOQ_TEST_RAW_PAIR_SUPPORT_H */
