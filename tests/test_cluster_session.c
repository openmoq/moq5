#include "unit/test_raw_pair_support.h"
#include "../core/src/wire/messages_d22_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static int failures;
#define CHECK(x)                                                                                   \
    do {                                                                                           \
        if (!(x)) {                                                                                \
            fprintf(stderr, "%d: %s\n", __LINE__, #x);                                             \
            ++failures;                                                                            \
        }                                                                                          \
    } while (0)
typedef struct fail_alloc_state {
    size_t live;
    int remaining;
} fail_alloc_state_t;
static const moq_alloc_t *custom_alloc;
static uint32_t custom_scratch;
static void *f_alloc(size_t n, void *ctx)
{
    fail_alloc_state_t *f = ctx;
    if (f->remaining == 0)
        return NULL;
    if (f->remaining > 0)
        --f->remaining;
    void *p = malloc(n);
    if (p)
        f->live++;
    return p;
}
static void *f_realloc(void *p, size_t old, size_t n, void *ctx)
{
    (void)old;
    fail_alloc_state_t *f = ctx;
    if (f->remaining == 0)
        return NULL;
    if (f->remaining > 0)
        --f->remaining;
    void *q = realloc(p, n);
    if (q && !p)
        f->live++;
    return q;
}
static void f_free(void *p, size_t n, void *ctx)
{
    (void)n;
    if (p) {
        fail_alloc_state_t *f = ctx;
        CHECK(f->live > 0);
        --f->live;
        free(p);
    }
}
static moq_session_t *make(moq_perspective_t perspective, bool enabled, uint64_t hop, bool priced,
                           uint64_t cost)
{
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = custom_alloc ? custom_alloc : moq_alloc_default();
    cfg.perspective = perspective;
    cfg.output_scratch_size = custom_scratch;
    cfg.max_actions = 2;
    cfg.max_events = 2;
    cfg.version = MOQ_VERSION_DRAFT_22;
    cfg.cluster_enabled = enabled;
    cfg.cluster_hop_id = hop;
    cfg.cluster_has_relay_cost = priced;
    cfg.cluster_relay_cost = cost;
    moq_session_t *s = NULL;
    CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);
    if (s) {
        CHECK(test_bind_if_required(s) == MOQ_OK);
        CHECK(moq_session_start(s, 0) == MOQ_OK);
    }
    return s;
}
static void negotiated(bool both)
{
    moq_session_t *c = make(MOQ_PERSPECTIVE_CLIENT, true, 0, true, 0);
    moq_session_t *s = make(MOQ_PERSPECTIVE_SERVER, both, UINT64_MAX, false, 999);
    if (!c || !s) {
        destroy(c, s);
        return;
    }
    run(c, s, 1);
    drain(c);
    drain(s);
    moq_cluster_info_t ci = {0}, si = {0};
    CHECK(moq_session_cluster_info(c, &ci, sizeof ci) == MOQ_OK);
    CHECK(moq_session_cluster_info(s, &si, sizeof si) == MOQ_OK);
    CHECK(ci.negotiated == both && si.negotiated == both);
    CHECK(si.peer_hop_id == 0 && si.peer_has_relay_cost && si.peer_relay_cost == 0);
    CHECK(ci.peer_relay_cost == 1 && !ci.peer_has_relay_cost);
    if (both) {
        moq_bytes_t part = {(const uint8_t *)"cluster", 7};
        uint64_t hops[] = {0, 0, UINT64_MAX};
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){&part, 1};
        cfg.cluster = (moq_cluster_metadata_t){hops, 3, true, UINT64_MAX};
        moq_announcement_t ann = MOQ_ANNOUNCEMENT_INVALID;
        CHECK(moq_session_publish_namespace(c, &cfg, 2, &ann) == MOQ_OK);
        hops[2] = 7;
        run(c, s, 3);
        moq_event_t ev = {0};
        CHECK(take(s, MOQ_EVENT_NAMESPACE_PUBLISHED, &ev));
        CHECK(ev.u.namespace_published.cluster.hop_count == 3);
        if (ev.u.namespace_published.cluster.hop_count == 3)
            CHECK(ev.u.namespace_published.cluster.hop_ids[2] == UINT64_MAX);
        CHECK(ev.u.namespace_published.cluster.has_route_cost &&
              ev.u.namespace_published.cluster.route_cost == UINT64_MAX);
        moq_announcement_t incoming = ev.u.namespace_published.ann;
        moq_accept_namespace_cfg_t ac;
        moq_accept_namespace_cfg_init(&ac);
        CHECK(moq_session_accept_namespace(s, incoming, &ac, 4) == MOQ_OK);
        run(c, s, 5);
        drain(c);
        drain(s);
        CHECK(moq_session_set_defer_namespace_updates(s, true) == MOQ_OK);
        moq_namespace_update_cfg_t up;
        moq_namespace_update_cfg_init(&up);
        up.cluster.has_route_cost = true;
        up.cluster.route_cost = 0;
        uint64_t id = 0;
        CHECK(moq_session_update_namespace(c, ann, &up, 6, &id) == MOQ_OK);
        run(c, s, 7);
        CHECK(take(s, MOQ_EVENT_NAMESPACE_UPDATE_REQUEST, &ev));
        moq_namespace_update_record_t record;
        CHECK(moq_session_namespace_owed_update(s, incoming, &record, sizeof record) == MOQ_OK);
        CHECK(record.cluster.hop_count == 0 && record.cluster.has_route_cost &&
              record.cluster.route_cost == 0);
        moq_accept_subscription_update_cfg_t au;
        moq_accept_subscription_update_cfg_init(&au);
        au.update_request_id = id;
        CHECK(moq_session_accept_namespace_update(s, incoming, &au, 8) == MOQ_OK);
        run(c, s, 9);
        drain(c);
        drain(s);
        moq_cluster_metadata_t current = {0};
        CHECK(moq_session_namespace_cluster(s, incoming, &current, sizeof current) == MOQ_OK);
        CHECK(current.hop_count == 3 && current.has_route_cost && current.route_cost == 0);
        uint64_t changed[] = {7, 8};
        up.cluster = (moq_cluster_metadata_t){changed, 2, false, 0};
        CHECK(moq_session_update_namespace(c, ann, &up, 10, &id) == MOQ_ERR_INVAL);
    }
    destroy(c, s);
}
static void discovery(void)
{
    moq_session_t *c = make(MOQ_PERSPECTIVE_CLIENT, true, 1, false, 0);
    moq_session_t *s = make(MOQ_PERSPECTIVE_SERVER, true, 2, false, 0);
    run(c, s, 1);
    drain(c);
    drain(s);
    moq_subscribe_namespace_cfg_t cfg;
    moq_subscribe_namespace_cfg_init(&cfg);
    cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
    moq_ns_sub_handle_t h = MOQ_NS_SUB_HANDLE_INVALID;
    CHECK(moq_session_subscribe_namespace(c, &cfg, 2, &h) == MOQ_OK);
    run(c, s, 3);
    moq_event_t ev = {0};
    CHECK(take(s, MOQ_EVENT_NS_SUB_REQUEST, &ev));
    moq_ns_sub_handle_t incoming = ev.u.ns_sub_request.handle;
    moq_accept_ns_sub_cfg_t accept;
    moq_accept_ns_sub_cfg_init(&accept);
    CHECK(moq_session_accept_ns_sub(s, incoming, &accept, 4) == MOQ_OK);
    run(c, s, 5);
    drain(c);
    drain(s);
    moq_bytes_t p = {(const uint8_t *)"mesh", 4};
    moq_namespace_t ns = {&p, 1};
    uint64_t path[] = {0, 2};
    moq_cluster_metadata_t meta = {path, 2, true, 9};
    CHECK(moq_session_send_namespace_cluster(s, incoming, &ns, &meta, sizeof meta, 6) == MOQ_OK);
    run(c, s, 7);
    CHECK(take(c, MOQ_EVENT_NAMESPACE_FOUND, &ev));
    CHECK(ev.u.namespace_found.cluster.hop_count == 2 &&
          ev.u.namespace_found.cluster.route_cost == 9);
    moq_publish_namespace_cfg_t duplicate;
    moq_publish_namespace_cfg_init(&duplicate);
    duplicate.track_namespace = ns;
    duplicate.cluster = meta;
    moq_announcement_t duplicate_handle;
    CHECK(moq_session_publish_namespace(s, &duplicate, 7, &duplicate_handle) == MOQ_ERR_INVAL);
    meta.route_cost = 0;
    CHECK(moq_session_send_namespace_cluster(s, incoming, &ns, &meta, sizeof meta, 8) == MOQ_OK);
    run(c, s, 9);
    CHECK(take(c, MOQ_EVENT_NAMESPACE_FOUND, &ev));
    CHECK(ev.u.namespace_found.cluster.has_route_cost &&
          ev.u.namespace_found.cluster.route_cost == 0);
    meta.route_cost = 11;
    CHECK(moq_session_send_namespace_cluster(s, incoming, &ns, &meta, sizeof meta, 9) == MOQ_OK);
    meta.route_cost = 12;
    CHECK(moq_session_send_namespace_cluster(s, incoming, &ns, &meta, sizeof meta, 9) == MOQ_OK);
    meta.route_cost = 13;
    CHECK(moq_session_send_namespace_cluster(s, incoming, &ns, &meta, sizeof meta, 9) ==
          MOQ_ERR_WOULD_BLOCK);
    pump(s, c, true, 9, 0);
    CHECK(moq_session_send_namespace_cluster(s, incoming, &ns, &meta, sizeof meta, 9) == MOQ_OK);
    moq_action_t pending = {0};
    CHECK(moq_session_poll_actions(s, &pending, 1) == 1);
    moq_stream_ref_t response_ref = pending.u.send_bidi_stream.stream_ref;
    CHECK(pending.kind == MOQ_ACTION_SEND_BIDI_STREAM);
    CHECK(moq_session_on_bidi_stream_bytes(c, response_ref, pending.u.send_bidi_stream.data,
                                           pending.u.send_bidi_stream.len, false,
                                           9) == MOQ_ERR_WOULD_BLOCK);
    moq_action_cleanup(&pending);
    CHECK(moq_session_poll_events(c, &ev, 1) == 1 && ev.u.namespace_found.cluster.route_cost == 11);
    CHECK(moq_session_poll_events(c, &ev, 1) == 1 && ev.u.namespace_found.cluster.route_cost == 12);
    CHECK(moq_session_on_bidi_stream_bytes(c, response_ref, NULL, 0, false, 9) == MOQ_OK);
    CHECK(take(c, MOQ_EVENT_NAMESPACE_FOUND, &ev) && ev.u.namespace_found.cluster.route_cost == 13);
    path[0] = 3;
    CHECK(moq_session_send_namespace_cluster(s, incoming, &ns, &meta, sizeof meta, 10) ==
          MOQ_ERR_INVAL);
    CHECK(moq_session_send_namespace_done(s, incoming, &ns, 11) == MOQ_OK);
    run(c, s, 12);
    CHECK(take(c, MOQ_EVENT_NAMESPACE_GONE, &ev));
    destroy(c, s);
}
static void failure_cleanup(void)
{
    fail_alloc_state_t state = {.remaining = -1};
    moq_alloc_t allocator = {&state, f_alloc, f_realloc, f_free};
    custom_alloc = &allocator;
    moq_session_t *c = make(MOQ_PERSPECTIVE_CLIENT, true, 1, false, 0);
    moq_session_t *s = make(MOQ_PERSPECTIVE_SERVER, true, 2, false, 0);
    run(c, s, 1);
    drain(c);
    drain(s);
    moq_bytes_t part = {(const uint8_t *)"failed", 6};
    uint64_t hops[] = {1};
    moq_publish_namespace_cfg_t cfg;
    moq_publish_namespace_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){&part, 1};
    cfg.cluster = (moq_cluster_metadata_t){hops, 1, true, 4};
    size_t baseline = state.live;
    for (int fail_at = 0; fail_at < 2; fail_at++) {
        state.remaining = fail_at;
        moq_announcement_t ann;
        CHECK(moq_session_publish_namespace(c, &cfg, 2, &ann) == MOQ_ERR_NOMEM);
        CHECK(state.live == baseline);
    }
    state.remaining = -1;
    moq_announcement_t ann;
    CHECK(moq_session_publish_namespace(c, &cfg, 3, &ann) == MOQ_OK);
    run(c, s, 4);
    moq_event_t ev = {0};
    CHECK(take(s, MOQ_EVENT_NAMESPACE_PUBLISHED, &ev));
    moq_announcement_t incoming = ev.u.namespace_published.ann;
    moq_accept_namespace_cfg_t accept;
    moq_accept_namespace_cfg_init(&accept);
    CHECK(moq_session_accept_namespace(s, incoming, &accept, 5) == MOQ_OK);
    run(c, s, 6);
    drain(c);
    drain(s);
    /* The default automatic cluster update applies a partial zero cost. */
    moq_namespace_update_cfg_t up;
    moq_namespace_update_cfg_init(&up);
    up.cluster.has_route_cost = true;
    up.cluster.route_cost = 0;
    uint64_t id;
    CHECK(moq_session_update_namespace(c, ann, &up, 7, &id) == MOQ_OK);
    run(c, s, 8);
    CHECK(take(s, MOQ_EVENT_NAMESPACE_UPDATE_RESULT, &ev));
    moq_cluster_metadata_t metadata = {0};
    CHECK(moq_session_namespace_cluster(s, incoming, &metadata, sizeof metadata) == MOQ_OK);
    CHECK(metadata.hop_count == 1 && metadata.hop_ids[0] == 1 && metadata.has_route_cost &&
          metadata.route_cost == 0);
    destroy(c, s);
    CHECK(state.live == 0);
    custom_alloc = NULL;
}
static void frozen_initializer(void)
{
    moq_publish_namespace_cfg_t cfg;
    memset(&cfg, 0xa5, sizeof cfg);
    (moq_publish_namespace_cfg_init)(&cfg);
    CHECK(cfg.struct_size == offsetof(moq_publish_namespace_cfg_t, cluster));
    const uint8_t *bytes = (const uint8_t *)&cfg.cluster;
    for (size_t i = 0; i < sizeof cfg.cluster; i++)
        CHECK(bytes[i] == 0xa5);
    moq_session_cfg_t sc = MOQ_SESSION_CFG_INIT;
    sc.alloc = moq_alloc_default();
    sc.perspective = MOQ_PERSPECTIVE_CLIENT;
    sc.version = MOQ_VERSION_DRAFT_18;
    sc.cluster_enabled = true;
    moq_session_t *s = NULL;
    CHECK(moq_session_create(&sc, 0, &s) == MOQ_ERR_UNSUPPORTED && !s);
}
static void malformed_advertisements(void)
{
    const uint8_t bodies[][13] = {{0, 1, 1, 'x', 0},
                                  {0, 1, 1, 'x', 1, 0xc4, 0x0b, 0x57, 0},
                                  {0, 1, 1, 'x', 1, 0xc4, 0x0b, 0x57, 2, 7, 7},
                                  {0, 1, 1, 'x', 1, 0xc4, 0x0b, 0x57, 1, 0x80}};
    const size_t sizes[] = {5, 9, 11, 10};
    for (size_t i = 0; i < 4; i++) {
        moq_session_t *c = make(MOQ_PERSPECTIVE_CLIENT, true, 1, false, 0);
        moq_session_t *s = make(MOQ_PERSPECTIVE_SERVER, true, 2, false, 0);
        run(c, s, 1);
        drain(c);
        drain(s);
        uint8_t wire[16] = {6, 0, (uint8_t)sizes[i]};
        memcpy(wire + 3, bodies[i], sizes[i]);
        moq_stream_ref_t ref = {77};
        size_t n = 3 + sizes[i];
        CHECK(moq_session_on_bidi_stream_bytes(s, ref, wire, n - 1, false, 2) == MOQ_OK);
        CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        CHECK(moq_session_poll_events(s, &ev, 1) == 0);
        CHECK(moq_session_on_bidi_stream_bytes(s, ref, wire + n - 1, 1, false, 3) == MOQ_OK);
        CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
        CHECK(take(s, MOQ_EVENT_SESSION_CLOSED, &ev) && ev.u.closed.code == 3);
        destroy(c, s);
    }
}
static void permanent_scratch_shortage(void)
{
    moq_session_t *c = make(MOQ_PERSPECTIVE_CLIENT, true, 1, false, 0);
    custom_scratch = 128;
    moq_session_t *s = make(MOQ_PERSPECTIVE_SERVER, true, 2, false, 0);
    custom_scratch = 0;
    run(c, s, 1);
    drain(c);
    drain(s);
    moq_bytes_t part = {(const uint8_t *)"scratch", 7};
    uint64_t path[40] = {0};
    moq_publish_namespace_cfg_t cfg;
    moq_publish_namespace_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){&part, 1};
    cfg.cluster = (moq_cluster_metadata_t){path, 40, false, 0};
    moq_announcement_t ann;
    CHECK(moq_session_publish_namespace(c, &cfg, 2, &ann) == MOQ_OK);
    run(c, s, 3);
    moq_event_t ev;
    CHECK(!take(s, MOQ_EVENT_NAMESPACE_PUBLISHED, &ev));
    CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
    drain(c);
    cfg.cluster.hop_count = 1;
    CHECK(moq_session_publish_namespace(c, &cfg, 4, &ann) == MOQ_OK);
    run(c, s, 5);
    CHECK(take(s, MOQ_EVENT_NAMESPACE_PUBLISHED, &ev));
    moq_announcement_t incoming = ev.u.namespace_published.ann;
    moq_accept_namespace_cfg_t accept;
    moq_accept_namespace_cfg_init(&accept);
    CHECK(moq_session_accept_namespace(s, incoming, &accept, 6) == MOQ_OK);
    run(c, s, 7);
    drain(c);
    drain(s);
    moq_namespace_update_cfg_t update;
    moq_namespace_update_cfg_init(&update);
    update.cluster = (moq_cluster_metadata_t){path, 40, false, 0};
    uint64_t id;
    CHECK(moq_session_update_namespace(c, ann, &update, 8, &id) == MOQ_OK);
    run(c, s, 9);
    CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
    destroy(c, s);
}
int main(void)
{
    negotiated(true);
    negotiated(false);
    discovery();
    failure_cleanup();
    frozen_initializer();
    malformed_advertisements();
    permanent_scratch_shortage();
    return failures ? 1 : 0;
}
