/*
 * PUBLISH-family parameter matrix (design §8) -- LARGEST_OBJECT on
 * PUBLISH (auto-advertised from the registry, max-merged on receipt),
 * SUBSCRIPTION_FILTER + FORWARD on PUBLISH_OK (accept-cfg tail, raw event
 * surfacing, internal window resolution), mandatory inbound EXPIRES, the
 * profile-distinguished wrong-placement semantics (d18 close vs d16 ignore),
 * and the frozen-v0 + _init_sized config ABI. Both drafts.
 */
#include <moq/codec.h>
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include <moq/wire.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static int failures = 0;

/* -- Allocator (balance-checked) ----------------------------------- */
typedef struct { int64_t balance; } pp_alloc_state_t;
static void *pp_alloc(size_t n, void *ctx)
{ pp_alloc_state_t *s = ctx; void *p = malloc(n); if (p) s->balance++; return p; }
static void *pp_realloc(void *p, size_t o, size_t n, void *ctx)
{ (void)o; (void)ctx; return realloc(p, n); }
static void pp_free(void *p, size_t n, void *ctx)
{ pp_alloc_state_t *s = ctx; (void)n; if (p) s->balance--; free(p); }
static moq_alloc_t pp_allocator(pp_alloc_state_t *s)
{ moq_alloc_t a = { s, pp_alloc, pp_realloc, pp_free }; return a; }

static moq_namespace_t ns1(moq_bytes_t *part, const char *s)
{ part->data = (const uint8_t *)s; part->len = strlen(s); moq_namespace_t ns = { part, 1 }; return ns; }

static moq_track_hist_t *rec_of(moq_session_t *s, const char *nsstr,
                                const char *name)
{
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, nsstr);
    moq_bytes_t nm = { (const uint8_t *)name, strlen(name) };
    size_t kl = 0; uint8_t *k = moq_build_track_key(s, &ns, nm, &kl);
    if (!k && kl > 0) return NULL;
    moq_track_hist_t *r = track_hist_find(s, k, kl);
    if (k) s->alloc.free(k, kl, s->alloc.ctx);
    return r;
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

static void drain_events(moq_session_t *s)
{ moq_event_t e; while (moq_session_poll_events(s, &e, 1) == 1) moq_event_cleanup(&e); }

static moq_simpair_t *make_pair(moq_alloc_t *alloc, moq_version_t ver)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.seed = 1; cfg.initial_now_us = 1000;
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

/* Client publishes ns "live" / `track`; returns both handles via out-params
 * after the server has surfaced PUBLISH_REQUEST (its event copied to *req). */
static bool do_publish(moq_simpair_t *sp, const char *track,
                       moq_publication_t *cl_pub, moq_event_t *req)
{
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = ns;
    pc.track_name = (moq_bytes_t){ (const uint8_t *)track, strlen(track) };
    pc.has_forward = true; pc.forward = true;
    if (moq_session_publish(cl, &pc, now, cl_pub) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(sp, 32, NULL);
    return poll_for(sv, MOQ_EVENT_PUBLISH_REQUEST, req);
}

/* ================================================================== */
/* A. draft-18 codec: round-trips + wrong-placement rejection         */
/* ================================================================== */

static void test_d18_codec(void)
{
    uint8_t buf[256];
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };

    /* PUBLISH round-trips LARGEST_OBJECT + EXPIRES (mask additions). */
    {
        moq_d18_publish_t p = {0};
        p.request_id = 4;
        p.track_namespace = ns;
        p.track_name = MOQ_BYTES_LITERAL("v");
        p.track_alias = 9;
        p.params.has_largest = true;
        p.params.largest_group = 7; p.params.largest_object = 3;
        p.params.has_expires = true; p.params.expires_ms = 30000;
        moq_buf_writer_t w; moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_publish(&w, &p) == MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_d18_decode_envelope(&r, &env) == MOQ_OK);
        moq_bytes_t dp[4]; moq_d18_publish_t out;
        MOQ_TEST_CHECK(moq_d18_decode_publish(env.payload, env.payload_len,
                                              dp, 4, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.params.has_largest &&
                       out.params.largest_group == 7 &&
                       out.params.largest_object == 3);
        MOQ_TEST_CHECK(out.params.has_expires && out.params.expires_ms == 30000);
    }

    /* PUBLISH_OK round-trips FILTER + FORWARD + EXPIRES. */
    {
        moq_d18_msg_params_t p = {0};
        p.has_filter = true;
        p.filter_type = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        p.filter_start_group = 5; p.filter_start_object = 1;
        p.filter_end_group = 9;
        p.has_forward = true; p.forward = 0;
        p.has_expires = true; p.expires_ms = 12345;
        moq_buf_writer_t w; moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_publish_ok(&w, &p) == MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_d18_decode_envelope(&r, &env) == MOQ_OK);
        moq_d18_publish_ok_t ok;
        MOQ_TEST_CHECK(moq_d18_decode_publish_ok(env.payload, env.payload_len,
                                                 &ok) == MOQ_OK);
        MOQ_TEST_CHECK(ok.params.has_filter &&
                       ok.params.filter_type == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE &&
                       ok.params.filter_start_group == 5 &&
                       ok.params.filter_start_object == 1 &&
                       ok.params.filter_end_group == 9);
        MOQ_TEST_CHECK(ok.params.has_forward && ok.params.forward == 0);
        MOQ_TEST_CHECK(ok.params.has_expires && ok.params.expires_ms == 12345);
    }

    /* Wrong placement stays a codec reject: FILTER on PUBLISH. */
    {
        moq_d18_publish_t p = {0};
        p.request_id = 4;
        p.track_namespace = ns;
        p.track_name = MOQ_BYTES_LITERAL("v");
        p.track_alias = 9;
        p.params.has_filter = true;
        p.params.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_buf_writer_t w; moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_publish(&w, &p) == MOQ_ERR_INVAL);
    }
    /* Wrong placement: LARGEST on PUBLISH_OK. */
    {
        moq_d18_msg_params_t p = {0};
        p.has_largest = true; p.largest_group = 1;
        moq_buf_writer_t w; moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_publish_ok(&w, &p) == MOQ_ERR_INVAL);
    }
}

/* ================================================================== */
/* B. End-to-end (simpair, both drafts)                               */
/* ================================================================== */

/* LARGEST rides PUBLISH from the registry; the receiver max-merges it into
 * its OWN registry at commit AND surfaces it on the event. */
static void test_publish_largest_advertise(moq_version_t ver)
{
    pp_alloc_state_t as = {0};
    moq_alloc_t alloc = pp_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    /* Publisher-side registry holds (7,3) before the PUBLISH. */
    MOQ_TEST_CHECK(moq_session_note_object_published(
        cl, &ns, MOQ_BYTES_LITERAL("pub"), 7, 3) == MOQ_OK);

    moq_publication_t cl_pub; moq_event_t req;
    MOQ_TEST_CHECK(do_publish(sp, "pub", &cl_pub, &req));
    /* (i) surfaced on the event... */
    MOQ_TEST_CHECK(req.u.publish_request.has_largest);
    MOQ_TEST_CHECK_EQ_U64(req.u.publish_request.largest_group, 7);
    MOQ_TEST_CHECK_EQ_U64(req.u.publish_request.largest_object, 3);
    MOQ_TEST_CHECK(!req.u.publish_request.has_expires);
    moq_event_cleanup(&req);
    /* (ii) ...AND max-merged into the RECEIVER's registry at commit. */
    moq_track_hist_t *r = rec_of(sv, "live", "pub");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 7 && r->largest_object == 3);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* Nothing published => no LARGEST on the wire (absence means absence). */
static void test_publish_no_largest_when_empty(moq_version_t ver)
{
    pp_alloc_state_t as = {0};
    moq_alloc_t alloc = pp_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *sv = moq_simpair_server(sp);

    moq_publication_t cl_pub; moq_event_t req;
    MOQ_TEST_CHECK(do_publish(sp, "fresh", &cl_pub, &req));
    MOQ_TEST_CHECK(!req.u.publish_request.has_largest);
    moq_event_cleanup(&req);
    moq_track_hist_t *r = rec_of(sv, "live", "fresh");
    MOQ_TEST_CHECK(r && !r->has_largest);   /* reserved, nothing observed */

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* The subscriber's accept carries a FILTER on PUBLISH_OK; the publisher
 * surfaces it RAW on publish_ok and resolves the window INTERNALLY against
 * its registry largest. */
static void test_accept_filter_negotiation(moq_version_t ver)
{
    pp_alloc_state_t as = {0};
    moq_alloc_t alloc = pp_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);
    moq_bytes_t p; moq_namespace_t ns = ns1(&p, "live");

    MOQ_TEST_CHECK(moq_session_note_object_published(
        cl, &ns, MOQ_BYTES_LITERAL("pub"), 7, 3) == MOQ_OK);
    moq_publication_t cl_pub; moq_event_t req;
    MOQ_TEST_CHECK(do_publish(sp, "pub", &cl_pub, &req));
    moq_publication_t sv_pub = req.u.publish_request.pub;
    moq_event_cleanup(&req);

    /* Accept with a LargestObject filter (tail => sized init). */
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    ac.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    MOQ_TEST_CHECK(moq_session_accept_publish(sv, sv_pub, &ac, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    /* Publisher: RAW filter on the event... */
    moq_event_t ok; MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_PUBLISH_OK, &ok));
    MOQ_TEST_CHECK(ok.u.publish_ok.has_filter);
    MOQ_TEST_CHECK_EQ_U64(ok.u.publish_ok.filter,
                          MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
    moq_event_cleanup(&ok);
    /* ...and the INTERNAL window resolved against its registry (7,3):
     * LargestObject => start (7,4). */
    const moq_resolved_window_t *w = moq_session_pub_resolved_window(cl, cl_pub);
    MOQ_TEST_CHECK(w != NULL);
    MOQ_TEST_CHECK(w && w->has_window &&
                   w->filter == MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT &&
                   w->start_group == 7 && w->start_object == 4);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* FORWARD=0 on the accept reaches the publisher's event AND its stored send
 * state. Both values are expressible. */
static void test_accept_forward_zero(moq_version_t ver)
{
    pp_alloc_state_t as = {0};
    moq_alloc_t alloc = pp_allocator(&as);
    moq_simpair_t *sp = make_pair(&alloc, ver);
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) return;
    moq_session_t *cl = moq_simpair_client(sp);
    moq_session_t *sv = moq_simpair_server(sp);
    uint64_t now = moq_simpair_now_us(sp);

    moq_publication_t cl_pub; moq_event_t req;
    MOQ_TEST_CHECK(do_publish(sp, "pub", &cl_pub, &req));
    moq_publication_t sv_pub = req.u.publish_request.pub;
    moq_event_cleanup(&req);

    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    ac.has_forward = true; ac.forward = false;
    MOQ_TEST_CHECK(moq_session_accept_publish(sv, sv_pub, &ac, now) == MOQ_OK);
    moq_simpair_run_until_quiescent(sp, 32, NULL);

    moq_event_t ok; MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_PUBLISH_OK, &ok));
    MOQ_TEST_CHECK(!ok.u.publish_ok.send_allowed);
    moq_event_cleanup(&ok);
    /* Stored send state on the publisher's entry is paused. */
    int slot = pub_resolve_handle(cl, cl_pub);
    MOQ_TEST_CHECK(slot >= 0 && !cl->publishes[slot].send_allowed);

    moq_simpair_destroy(sp);
    MOQ_TEST_CHECK(as.balance == 0);
}

static moq_session_t *d18_server(moq_alloc_t *alloc);

/* FORWARD omitted from PUBLISH_OK defaults to 1 (d16 §9.2.2.8 / d18 §10.2.12)
 * -- NOT "unchanged". PUBLISH(FORWARD=0) accepted with a v0 / no-forward cfg
 * must (a) OMIT the parameter on the wire, and (b) resolve BOTH endpoints'
 * entries and the publisher's event to Forward=1. */
static void test_publish_ok_forward_omission_defaults_on(void)
{
    /* draft-16, direct pair: wire omission + both entries + event. */
    {
        test_alloc_state_t alloc_state = {0};
        moq_alloc_t alloc = test_allocator(&alloc_state);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ ns_parts, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("video");
        pc.has_forward = true; pc.forward = false;       /* publisher pauses */
        moq_publication_t pub_h;
        MOQ_TEST_CHECK(moq_session_publish(c, &pc, 1000, &pub_h) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);
        moq_event_t req;
        MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_PUBLISH_REQUEST, &req));
        MOQ_TEST_CHECK(!req.u.publish_request.forward);
        moq_publication_t sv_pub = req.u.publish_request.pub;
        moq_event_cleanup(&req);

        /* Accept with a v0 cfg (no forward block at all). */
        uint8_t *raw = malloc(MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE);
        MOQ_TEST_CHECK(raw != NULL);
        moq_accept_publish_cfg_init((moq_accept_publish_cfg_t *)raw);
        MOQ_TEST_CHECK(moq_session_accept_publish(
            sv, sv_pub, (const moq_accept_publish_cfg_t *)raw, 2000) == MOQ_OK);
        free(raw);
        /* Accepting entry resolves to the EFFECTIVE default (forwarding). */
        int svslot = pub_resolve_handle(sv, sv_pub);
        MOQ_TEST_CHECK(svslot >= 0 && sv->publishes[svslot].send_allowed);

        /* Wire: the PUBLISH_OK carries NO FORWARD parameter. */
        moq_action_t a; bool sent = false;
        uint8_t wire[256]; size_t wire_len = 0;
        while (moq_session_poll_actions(sv, &a, 1) == 1) {
            if (a.kind == MOQ_ACTION_SEND_CONTROL && !sent) {
                memcpy(wire, a.u.send_control.data, a.u.send_control.len);
                wire_len = a.u.send_control.len;
                sent = true;
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(sent);
        moq_buf_reader_t rr; moq_buf_reader_init(&rr, wire, wire_len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK(moq_control_decode_envelope(&rr, &env) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D16_PUBLISH_OK);
        moq_kvp_entry_t okp[8];
        moq_d16_publish_ok_t okm = { .params = okp, .params_cap = 8 };
        MOQ_TEST_CHECK(moq_d16_decode_publish_ok(env.payload, env.payload_len,
                                                 &okm) == MOQ_OK);
        for (size_t i = 0; i < okm.params_count; i++)
            MOQ_TEST_CHECK(okm.params[i].type != MOQ_MSG_PARAM_FORWARD);

        /* Publisher: event + entry resolve to Forward=1. */
        MOQ_TEST_CHECK(moq_session_on_control_bytes(c, wire, wire_len, 3000)
                       == MOQ_OK);
        moq_event_t ok; MOQ_TEST_CHECK(poll_for(c, MOQ_EVENT_PUBLISH_OK, &ok));
        MOQ_TEST_CHECK(ok.u.publish_ok.send_allowed);
        moq_event_cleanup(&ok);
        int cslot = pub_resolve_handle(c, pub_h);
        MOQ_TEST_CHECK(cslot >= 0 && c->publishes[cslot].send_allowed);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(alloc_state.balance == 0);
    }

    /* draft-18, direct server: wire omission + accepting entry. */
    {
        pp_alloc_state_t as = {0};
        moq_alloc_t alloc = pp_allocator(&as);
        moq_session_t *s = d18_server(&alloc);
        MOQ_TEST_CHECK(s != NULL);
        if (!s) return;
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_d18_publish_t p = {0};
        p.request_id = 0;
        p.track_namespace = (moq_namespace_t){ parts, 1 };
        p.track_name = MOQ_BYTES_LITERAL("v");
        p.track_alias = 3;
        p.params.has_forward = true; p.params.forward = 0;
        uint8_t buf[256]; moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK(moq_d18_encode_publish(&w, &p) == MOQ_OK);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(4);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            s, ref, buf, moq_buf_writer_offset(&w), false, 0) == MOQ_OK);
        moq_event_t req;
        MOQ_TEST_CHECK(poll_for(s, MOQ_EVENT_PUBLISH_REQUEST, &req));
        MOQ_TEST_CHECK(!req.u.publish_request.forward);
        moq_publication_t sv_pub = req.u.publish_request.pub;
        moq_event_cleanup(&req);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) == 1) moq_action_cleanup(&a); }

        moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
        MOQ_TEST_CHECK(moq_session_accept_publish(s, sv_pub, &ac, 1) == MOQ_OK);
        int slot = pub_resolve_handle(s, sv_pub);
        MOQ_TEST_CHECK(slot >= 0 && s->publishes[slot].send_allowed);

        moq_action_t a; bool seen = false;
        while (moq_session_poll_actions(s, &a, 1) == 1) {
            if (a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                a.u.send_bidi_stream.stream_ref._v == ref._v && !seen) {
                moq_buf_reader_t rr;
                moq_buf_reader_init(&rr, a.u.send_bidi_stream.data,
                                    a.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                MOQ_TEST_CHECK(moq_d18_decode_envelope(&rr, &env) == MOQ_OK);
                MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_OK);
                moq_d18_publish_ok_t ok;
                MOQ_TEST_CHECK(moq_d18_decode_publish_ok(
                    env.payload, env.payload_len, &ok) == MOQ_OK);
                MOQ_TEST_CHECK(!ok.params.has_forward);   /* omitted */
                seen = true;
            }
            moq_action_cleanup(&a);
        }
        MOQ_TEST_CHECK(seen);
        moq_session_destroy(s);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* draft-18, simpair end-to-end: publisher event + both entries. */
    {
        pp_alloc_state_t as = {0};
        moq_alloc_t alloc = pp_allocator(&as);
        moq_simpair_t *sp = make_pair(&alloc, MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *cl = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_bytes_t p2; moq_namespace_t ns = ns1(&p2, "live");
        moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
        pc.track_namespace = ns; pc.track_name = MOQ_BYTES_LITERAL("pub");
        pc.has_forward = true; pc.forward = false;
        moq_publication_t cl_pub;
        MOQ_TEST_CHECK(moq_session_publish(cl, &pc, now, &cl_pub) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        moq_event_t req;
        MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_PUBLISH_REQUEST, &req));
        moq_publication_t sv_pub = req.u.publish_request.pub;
        moq_event_cleanup(&req);

        moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
        MOQ_TEST_CHECK(moq_session_accept_publish(sv, sv_pub, &ac, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        moq_event_t ok; MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_PUBLISH_OK, &ok));
        MOQ_TEST_CHECK(ok.u.publish_ok.send_allowed);     /* resolved to 1 */
        moq_event_cleanup(&ok);
        int cslot = pub_resolve_handle(cl, cl_pub);
        int sslot = pub_resolve_handle(sv, sv_pub);
        MOQ_TEST_CHECK(cslot >= 0 && cl->publishes[cslot].send_allowed);
        MOQ_TEST_CHECK(sslot >= 0 && sv->publishes[sslot].send_allowed);

        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* ================================================================== */
/* C. Inbound EXPIRES + wrong placement, profile-distinguished        */
/* ================================================================== */

/* draft-16: craft an inbound PUBLISH carrying EXPIRES (legal => surfaced)
 * plus SUBSCRIPTION_FILTER (misplaced-but-defined => IGNORED, session
 * SURVIVES -- §9.2.2 line 2359, NOT a close). */
static void test_d16_publish_expires_and_misplaced_filter(void)
{
    test_alloc_state_t alloc_state = {0};
    moq_alloc_t alloc = test_allocator(&alloc_state);
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    uint8_t exp_buf[8], filt_buf[32], larg_buf[16];
    size_t exp_n = moq_quic_varint_encode(45000, exp_buf, sizeof(exp_buf));
    size_t filt_n = 0;
    moq_d16_subscription_filter_t f = {
        .filter_type = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP };
    MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(
        filt_buf, sizeof(filt_buf), &filt_n, &f) == MOQ_OK);
    moq_buf_writer_t lw; moq_buf_writer_init(&lw, larg_buf, sizeof(larg_buf));
    moq_buf_write_varint(&lw, 4); moq_buf_write_varint(&lw, 2);
    moq_kvp_entry_t params[3] = {
        { .type = MOQ_MSG_PARAM_EXPIRES, .value = exp_buf, .value_len = exp_n,
          .is_varint = true },
        { .type = MOQ_MSG_PARAM_LARGEST_OBJECT, .value = larg_buf,
          .value_len = moq_buf_writer_offset(&lw), .is_varint = false },
        { .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER, .value = filt_buf,
          .value_len = filt_n, .is_varint = false },   /* misplaced */
    };
    moq_d16_publish_t pub = {
        .request_id = 0,
        .track_namespace = { ns_parts, 1 },
        .track_name = MOQ_BYTES_LITERAL("video"),
        .track_alias = 5,
        .params = params, .params_count = 3, .params_cap = 3,
    };
    uint8_t buf[256]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK(moq_d16_encode_publish(&w, &pub) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_control_bytes(
        sv, buf, moq_buf_writer_offset(&w), 1000) == MOQ_OK);

    /* Session SURVIVES; EXPIRES + LARGEST surfaced; FILTER silently dropped;
     * LARGEST also merged into the receiver's registry. */
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    moq_event_t req;
    MOQ_TEST_CHECK(poll_for(sv, MOQ_EVENT_PUBLISH_REQUEST, &req));
    MOQ_TEST_CHECK(req.u.publish_request.has_expires);
    MOQ_TEST_CHECK_EQ_U64(req.u.publish_request.expires_ms, 45000);
    MOQ_TEST_CHECK(req.u.publish_request.has_largest);
    MOQ_TEST_CHECK_EQ_U64(req.u.publish_request.largest_group, 4);
    MOQ_TEST_CHECK_EQ_U64(req.u.publish_request.largest_object, 2);
    moq_event_cleanup(&req);
    moq_track_hist_t *r = rec_of(sv, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 4 && r->largest_object == 2);

    moq_session_destroy(c);
    moq_session_destroy(sv);
    MOQ_TEST_CHECK(alloc_state.balance == 0);
}

/* draft-16: craft an inbound PUBLISH_OK carrying FILTER + EXPIRES (legal =>
 * surfaced) plus LARGEST (misplaced-but-defined => IGNORED, survives). */
static void test_d16_publish_ok_filter_expires_and_misplaced_largest(void)
{
    test_alloc_state_t alloc_state = {0};
    moq_alloc_t alloc = test_allocator(&alloc_state);
    moq_session_t *c = NULL, *sv = NULL;
    establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    /* Publisher registry (6,1) so the filter window resolves against it. */
    MOQ_TEST_CHECK(moq_session_note_object_published(
        c, &ns, MOQ_BYTES_LITERAL("video"), 6, 1) == MOQ_OK);
    moq_publish_cfg_t pc; moq_publish_cfg_init(&pc);
    pc.track_namespace = ns;
    pc.track_name = MOQ_BYTES_LITERAL("video");
    moq_publication_t pub_h;
    MOQ_TEST_CHECK(moq_session_publish(c, &pc, 1000, &pub_h) == MOQ_OK);
    pump_actions_to_peer(c, sv, 1000);
    drain_events(sv);
    int cslot = pub_resolve_handle(c, pub_h);
    MOQ_TEST_CHECK(cslot >= 0);
    uint64_t rid = c->publishes[cslot].request_id;

    uint8_t filt_buf[32], exp_buf[8], larg_buf[16];
    size_t filt_n = 0;
    moq_d16_subscription_filter_t f = {
        .filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT };
    MOQ_TEST_CHECK(moq_d16_encode_subscription_filter(
        filt_buf, sizeof(filt_buf), &filt_n, &f) == MOQ_OK);
    size_t exp_n = moq_quic_varint_encode(9000, exp_buf, sizeof(exp_buf));
    moq_buf_writer_t lw; moq_buf_writer_init(&lw, larg_buf, sizeof(larg_buf));
    moq_buf_write_varint(&lw, 9); moq_buf_write_varint(&lw, 9);
    moq_kvp_entry_t params[3] = {
        { .type = MOQ_MSG_PARAM_EXPIRES, .value = exp_buf, .value_len = exp_n,
          .is_varint = true },
        { .type = MOQ_MSG_PARAM_LARGEST_OBJECT, .value = larg_buf,
          .value_len = moq_buf_writer_offset(&lw), .is_varint = false },   /* misplaced */
        { .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER, .value = filt_buf,
          .value_len = filt_n, .is_varint = false },
    };
    uint8_t buf[256]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK(moq_d16_encode_publish_ok(&w, rid, params, 3) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_control_bytes(
        c, buf, moq_buf_writer_offset(&w), 2000) == MOQ_OK);

    /* Survives; FILTER + EXPIRES surfaced raw; LARGEST ignored (its registry
     * largest is untouched); window resolved internally to (6,2). */
    MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
    moq_event_t ok; MOQ_TEST_CHECK(poll_for(c, MOQ_EVENT_PUBLISH_OK, &ok));
    MOQ_TEST_CHECK(ok.u.publish_ok.has_filter);
    MOQ_TEST_CHECK_EQ_U64(ok.u.publish_ok.filter,
                          MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
    MOQ_TEST_CHECK(ok.u.publish_ok.has_expires);
    MOQ_TEST_CHECK_EQ_U64(ok.u.publish_ok.expires_ms, 9000);
    moq_event_cleanup(&ok);
    moq_track_hist_t *r = rec_of(c, "live", "video");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 6 && r->largest_object == 1);
    const moq_resolved_window_t *win = moq_session_pub_resolved_window(c, pub_h);
    MOQ_TEST_CHECK(win && win->has_window &&
                   win->start_group == 6 && win->start_object == 2);

    moq_session_destroy(c);
    moq_session_destroy(sv);
    MOQ_TEST_CHECK(alloc_state.balance == 0);
}

/* draft-18: EXPIRES inbound on PUBLISH is legal and MUST NOT close.
 * Hand-rolled PUBLISH with EXPIRES fed to a standalone server. */
static moq_session_t *d18_server(moq_alloc_t *alloc)
{
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = alloc;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    moq_session_start(s, 0);
    moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[32]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    drain_events(s);
    return s;
}

static void test_d18_publish_expires_not_fatal(void)
{
    pp_alloc_state_t as = {0};
    moq_alloc_t alloc = pp_allocator(&as);
    moq_session_t *s = d18_server(&alloc);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_d18_publish_t p = {0};
    p.request_id = 0;   /* first client request id */
    p.track_namespace = (moq_namespace_t){ parts, 1 };
    p.track_name = MOQ_BYTES_LITERAL("v");
    p.track_alias = 3;
    p.params.has_expires = true; p.params.expires_ms = 20000;
    p.params.has_largest = true;
    p.params.largest_group = 2; p.params.largest_object = 8;
    uint8_t buf[256]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    MOQ_TEST_CHECK(moq_d18_encode_publish(&w, &p) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
        s, moq_stream_ref_from_u64(4), buf, moq_buf_writer_offset(&w),
        false, 0) == MOQ_OK);

    MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
    moq_event_t req;
    MOQ_TEST_CHECK(poll_for(s, MOQ_EVENT_PUBLISH_REQUEST, &req));
    MOQ_TEST_CHECK(req.u.publish_request.has_expires);
    MOQ_TEST_CHECK_EQ_U64(req.u.publish_request.expires_ms, 20000);
    MOQ_TEST_CHECK(req.u.publish_request.has_largest);
    MOQ_TEST_CHECK_EQ_U64(req.u.publish_request.largest_group, 2);
    MOQ_TEST_CHECK_EQ_U64(req.u.publish_request.largest_object, 8);
    moq_event_cleanup(&req);
    moq_track_hist_t *r = rec_of(s, "live", "v");
    MOQ_TEST_CHECK(r && r->has_largest &&
                   r->largest_group == 2 && r->largest_object == 8);

    moq_session_destroy(s);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* draft-18: EXPIRES on PUBLISH_OK is legal (surfaced); a LARGEST on
 * PUBLISH_OK is a wrong-placement close. Crafted REQUEST_OK injected on the
 * publisher's request bidi inside a simpair. */
static void craft_d18_request_ok(uint8_t *buf, size_t cap, size_t *out_len,
                                 const moq_d18_msg_params_t *params)
{
    moq_buf_writer_t w; moq_buf_writer_init(&w, buf, cap);
    size_t len_off;
    moq_buf_write_vi64(&w, MOQ_D18_REQUEST_OK);
    moq_buf_reserve_uint16(&w, &len_off);
    size_t start = moq_buf_writer_offset(&w);
    moq_d18_encode_msg_params(&w, params);
    /* Track Properties are empty: the payload ends after the params. */
    moq_buf_patch_uint16(&w, len_off,
                         (uint16_t)(moq_buf_writer_offset(&w) - start));
    *out_len = moq_buf_writer_offset(&w);
}

static void test_d18_publish_ok_expires_legal_largest_fatal(void)
{
    /* EXPIRES legal: surfaced, session survives. */
    {
        pp_alloc_state_t as = {0};
        moq_alloc_t alloc = pp_allocator(&as);
        moq_simpair_t *sp = make_pair(&alloc, MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *cl = moq_simpair_client(sp);
        moq_publication_t cl_pub; moq_event_t req;
        MOQ_TEST_CHECK(do_publish(sp, "pub", &cl_pub, &req));
        moq_event_cleanup(&req);
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0);
        moq_stream_ref_t ref = cl->publishes[slot].request_stream_ref;

        moq_d18_msg_params_t mp = {0};
        mp.has_expires = true; mp.expires_ms = 7500;
        uint8_t buf[128]; size_t n = 0;
        craft_d18_request_ok(buf, sizeof(buf), &n, &mp);
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(
            cl, ref, buf, n, false, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_ESTABLISHED);
        moq_event_t ok; MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_PUBLISH_OK, &ok));
        MOQ_TEST_CHECK(ok.u.publish_ok.has_expires);
        MOQ_TEST_CHECK_EQ_U64(ok.u.publish_ok.expires_ms, 7500);
        MOQ_TEST_CHECK(!ok.u.publish_ok.has_filter);
        moq_event_cleanup(&ok);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* LARGEST misplaced: d18 CLOSES (params are not skippable). */
    {
        pp_alloc_state_t as = {0};
        moq_alloc_t alloc = pp_allocator(&as);
        moq_simpair_t *sp = make_pair(&alloc, MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *cl = moq_simpair_client(sp);
        moq_publication_t cl_pub; moq_event_t req;
        MOQ_TEST_CHECK(do_publish(sp, "pub", &cl_pub, &req));
        moq_event_cleanup(&req);
        int slot = pub_resolve_handle(cl, cl_pub);
        MOQ_TEST_CHECK(slot >= 0);
        moq_stream_ref_t ref = cl->publishes[slot].request_stream_ref;

        moq_d18_msg_params_t mp = {0};
        mp.has_largest = true; mp.largest_group = 1; mp.largest_object = 1;
        uint8_t buf[128]; size_t n = 0;
        craft_d18_request_ok(buf, sizeof(buf), &n, &mp);
        (void)moq_session_on_bidi_stream_bytes(cl, ref, buf, n, false, 0);
        MOQ_TEST_CHECK(moq_session_state(cl) == MOQ_SESS_CLOSED);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

/* draft-18: a FILTER on PUBLISH closes (wrong placement, not skippable). */
static void test_d18_publish_misplaced_filter_fatal(void)
{
    pp_alloc_state_t as = {0};
    moq_alloc_t alloc = pp_allocator(&as);
    moq_session_t *s = d18_server(&alloc);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return;

    /* Hand-roll: PUBLISH with a SUBSCRIPTION_FILTER param (the public encoder
     * refuses it, so build the frame manually). */
    uint8_t buf[256]; moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    size_t len_off;
    moq_buf_write_vi64(&w, MOQ_D18_PUBLISH);
    moq_buf_reserve_uint16(&w, &len_off);
    size_t start = moq_buf_writer_offset(&w);
    moq_buf_write_vi64(&w, 0);                       /* request id */
    moq_buf_write_vi64(&w, 1);                       /* ns count */
    moq_buf_write_vi64(&w, 4);
    moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
    moq_buf_write_vi64(&w, 1);                       /* name len */
    moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
    moq_buf_write_vi64(&w, 3);                       /* track alias */
    moq_d18_msg_params_t mp = {0};
    mp.has_filter = true;
    mp.filter_type = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_d18_encode_msg_params(&w, &mp);
    /* Track Properties are the raw REMAINING bytes: empty means NO bytes (a
     * lone 0x00 would begin an incomplete even property and close for the
     * wrong reason). The payload ends after the params. */
    moq_buf_patch_uint16(&w, len_off,
                         (uint16_t)(moq_buf_writer_offset(&w) - start));

    (void)moq_session_on_bidi_stream_bytes(
        s, moq_stream_ref_from_u64(4), buf, moq_buf_writer_offset(&w),
        false, 0);
    MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

    moq_session_destroy(s);
    MOQ_TEST_CHECK(as.balance == 0);
}

/* ================================================================== */
/* D. ABI: frozen-v0 pointer init, sized init, whole-block poison      */
/* ================================================================== */

static void test_abi_frozen_v0_canaries(void)
{
    /* Old-prefix canary: the pointer init on an EXACT-v0 allocation writes no
     * byte past v0 (guards intact) and stamps struct_size == V0. Heap-allocated
     * so the allocation really is v0-sized, exactly like an old caller's (and
     * opaque to the compiler's array-bounds view of a stack array). */
    {
        uint8_t *raw = malloc(MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE + 8);
        MOQ_TEST_CHECK(raw != NULL);
        if (raw) {
            memset(raw, 0xAA, MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE + 8);
            moq_accept_publish_cfg_init((moq_accept_publish_cfg_t *)raw);
            uint32_t ssz;
            memcpy(&ssz, raw + offsetof(moq_accept_publish_cfg_t, struct_size),
                   sizeof(ssz));
            MOQ_TEST_CHECK_EQ_U64(ssz, (uint64_t)MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE);
            for (size_t i = 0; i < 8; i++)
                MOQ_TEST_CHECK(raw[MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE + i] == 0xAA);
            free(raw);
        }
    }
    {
        uint8_t *raw = malloc(MOQ_PUBLISH_CFG_V0_SIZE + 8);
        MOQ_TEST_CHECK(raw != NULL);
        if (raw) {
            memset(raw, 0xAA, MOQ_PUBLISH_CFG_V0_SIZE + 8);
            moq_publish_cfg_init((moq_publish_cfg_t *)raw);
            uint32_t ssz;
            memcpy(&ssz, raw + offsetof(moq_publish_cfg_t, struct_size),
                   sizeof(ssz));
            MOQ_TEST_CHECK_EQ_U64(ssz, (uint64_t)MOQ_PUBLISH_CFG_V0_SIZE);
            for (size_t i = 0; i < 8; i++)
                MOQ_TEST_CHECK(raw[MOQ_PUBLISH_CFG_V0_SIZE + i] == 0xAA);
            free(raw);
        }
    }
    /* Sized-init canary: _init_sized records the real size. */
    {
        moq_accept_publish_cfg_t c;
        moq_accept_publish_cfg_init_sized(&c, sizeof(c));
        MOQ_TEST_CHECK_EQ_U64(c.struct_size, (uint64_t)sizeof(c));
        moq_publish_cfg_t pc;
        moq_publish_cfg_init_sized(&pc, sizeof(pc));
        MOQ_TEST_CHECK_EQ_U64(pc.struct_size, (uint64_t)sizeof(pc));
    }
}

/* A v0-sized accept cfg (old caller) accepts UNFILTERED; and the two appended
 * blocks are gated as WHOLE BLOCKS -- a struct_size landing mid-block reads
 * as absent, never torn. Driven end-to-end per draft. */
static void test_abi_v0_and_partial_prefix(moq_version_t ver)
{
    /* v0 accept: no filter, no forward override. */
    {
        pp_alloc_state_t as = {0};
        moq_alloc_t alloc = pp_allocator(&as);
        moq_simpair_t *sp = make_pair(&alloc, ver);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *cl = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_publication_t cl_pub; moq_event_t req;
        MOQ_TEST_CHECK(do_publish(sp, "pub", &cl_pub, &req));
        moq_publication_t sv_pub = req.u.publish_request.pub;
        moq_event_cleanup(&req);

        uint8_t *raw = malloc(MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE);
        MOQ_TEST_CHECK(raw != NULL);
        moq_accept_publish_cfg_init((moq_accept_publish_cfg_t *)raw);
        MOQ_TEST_CHECK(moq_session_accept_publish(
            sv, sv_pub, (const moq_accept_publish_cfg_t *)raw, now) == MOQ_OK);
        free(raw);
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        moq_event_t ok; MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_PUBLISH_OK, &ok));
        MOQ_TEST_CHECK(!ok.u.publish_ok.has_filter);
        MOQ_TEST_CHECK(ok.u.publish_ok.send_allowed);   /* no override */
        moq_event_cleanup(&ok);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Partial FILTER block: struct_size stops before end_group => the whole
     * filter block is ignored (unfiltered), no torn read. */
    {
        pp_alloc_state_t as = {0};
        moq_alloc_t alloc = pp_allocator(&as);
        moq_simpair_t *sp = make_pair(&alloc, ver);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *cl = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_publication_t cl_pub; moq_event_t req;
        MOQ_TEST_CHECK(do_publish(sp, "pub", &cl_pub, &req));
        moq_publication_t sv_pub = req.u.publish_request.pub;
        moq_event_cleanup(&req);

        moq_accept_publish_cfg_t c;
        moq_accept_publish_cfg_init_sized(
            &c, offsetof(moq_accept_publish_cfg_t, end_group));
        c.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;   /* half-present */
        c.start_group = 99;
        MOQ_TEST_CHECK(moq_session_accept_publish(sv, sv_pub, &c, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        moq_event_t ok; MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_PUBLISH_OK, &ok));
        MOQ_TEST_CHECK(!ok.u.publish_ok.has_filter);      /* block gated out */
        moq_event_cleanup(&ok);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* Partial FORWARD block: struct_size covers has_forward but not forward
     * => no override (send_allowed stays true). */
    {
        pp_alloc_state_t as = {0};
        moq_alloc_t alloc = pp_allocator(&as);
        moq_simpair_t *sp = make_pair(&alloc, ver);
        MOQ_TEST_CHECK(sp != NULL);
        if (!sp) return;
        moq_session_t *cl = moq_simpair_client(sp);
        moq_session_t *sv = moq_simpair_server(sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_publication_t cl_pub; moq_event_t req;
        MOQ_TEST_CHECK(do_publish(sp, "pub", &cl_pub, &req));
        moq_publication_t sv_pub = req.u.publish_request.pub;
        moq_event_cleanup(&req);

        moq_accept_publish_cfg_t c;
        moq_accept_publish_cfg_init_sized(
            &c, offsetof(moq_accept_publish_cfg_t, forward));
        c.has_forward = true;    /* half-present: forward itself not covered */
        c.forward = false;
        MOQ_TEST_CHECK(moq_session_accept_publish(sv, sv_pub, &c, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);
        moq_event_t ok; MOQ_TEST_CHECK(poll_for(cl, MOQ_EVENT_PUBLISH_OK, &ok));
        MOQ_TEST_CHECK(ok.u.publish_ok.send_allowed);     /* no override */
        moq_event_cleanup(&ok);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
    }
}

int main(void)
{
    test_d18_codec();
    test_publish_largest_advertise(MOQ_VERSION_DRAFT_16);
    test_publish_largest_advertise(MOQ_VERSION_DRAFT_18);
    test_publish_no_largest_when_empty(MOQ_VERSION_DRAFT_16);
    test_publish_no_largest_when_empty(MOQ_VERSION_DRAFT_18);
    test_accept_filter_negotiation(MOQ_VERSION_DRAFT_16);
    test_accept_filter_negotiation(MOQ_VERSION_DRAFT_18);
    test_accept_forward_zero(MOQ_VERSION_DRAFT_16);
    test_accept_forward_zero(MOQ_VERSION_DRAFT_18);
    test_publish_ok_forward_omission_defaults_on();
    test_d16_publish_expires_and_misplaced_filter();
    test_d16_publish_ok_filter_expires_and_misplaced_largest();
    test_d18_publish_expires_not_fatal();
    test_d18_publish_ok_expires_legal_largest_fatal();
    test_d18_publish_misplaced_filter_fatal();
    test_abi_frozen_v0_canaries();
    test_abi_v0_and_partial_prefix(MOQ_VERSION_DRAFT_16);
    test_abi_v0_and_partial_prefix(MOQ_VERSION_DRAFT_18);

    if (failures == 0)
        printf("test_session_publish_params: all passed\n");
    return failures ? 1 : 0;
}
