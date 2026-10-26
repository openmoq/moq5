#include <moq/publisher.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Session internals are inspected, never mutated. All requests, cancellation,
 * queue pressure and dispatch go through public APIs and the shipping core. */
static unsigned checks, failures, prerequisites, cases;
#define CHECK(e) do { ++checks; if (!(e)) { \
    fprintf(stderr, "FAIL %d: %s\n", __LINE__, #e); ++failures; \
} } while (0)
#define NEED(e) do { ++checks; if (!(e)) { \
    fprintf(stderr, "SETUP FAIL %d: %s\n", __LINE__, #e); \
    ++failures; ++prerequisites; goto cleanup; \
} } while (0)

enum { FETCHES = 16, OBJECTS = 64 };
/* With three slots: OK/header (2), properties/object (2), data/request FIN (2).
 * No two phases fit before transport polling returns action credit. */
enum { FETCH_PHASES = 3 };
typedef struct {
    size_t calls, fail_at, sizes[8], live;
} memory_t;

static void *allocate(size_t size, void *ctx)
{
    memory_t *m = ctx;
    m->sizes[m->calls++ % 8] = size;
    if (m->calls == m->fail_at) return NULL;
    void *ptr = malloc(size);
    if (ptr) ++m->live;
    return ptr;
}

static void *resize(void *ptr, size_t old_size, size_t size, void *ctx)
{
    (void)old_size;
    if (!ptr) return allocate(size, ctx);
    return realloc(ptr, size);
}

static void release(void *ptr, size_t size, void *ctx)
{
    (void)size;
    memory_t *m = ctx;
    if (ptr) { CHECK(m->live > 0); --m->live; }
    free(ptr);
}

typedef struct {
    memory_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *client, *server;
    moq_publisher_t *pub;
    moq_pub_track_t *track;
    moq_bytes_t ns;
    moq_rcbuf_t *payload[2], *properties[2];
    unsigned blocks, closes;
} rig_t;

static void discard_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1)) moq_event_cleanup(&e);
}

static void on_closed(void *ctx, uint64_t code)
{
    rig_t *r = ctx;
    if (code != 0x99) fprintf(stderr, "unexpected server close: %llu\n", (unsigned long long)code);
    CHECK(code == 0x99);
    ++r->closes;
}

static bool settle(rig_t *r)
{ return moq_simpair_run_until_quiescent(r->pair, 8, NULL) == MOQ_OK; }

static bool open_rig(rig_t *r)
{
    r->alloc = (moq_alloc_t){.ctx = &r->memory, .alloc = allocate,
                            .realloc = resize, .free = release};
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc;
    cfg.version = MOQ_VERSION_DRAFT_22;
    cfg.max_actions = 3;
    cfg.initial_now_us = 1000;
    cfg.seed = 7;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 128;
    if (moq_simpair_create(&cfg, &r->pair) != MOQ_OK ||
        moq_simpair_start(r->pair) != MOQ_OK || !settle(r)) return false;
    r->client = moq_simpair_client(r->pair);
    r->server = moq_simpair_server(r->pair);
    discard_events(r->client);
    discard_events(r->server);
    if (r->server->fetch_cap != FETCHES || r->server->event_cap != FETCHES ||
        moq_session_action_capacity(r->server) != 3) return false;
    moq_pub_cfg_t pub_cfg;
    moq_pub_cfg_init_sized(&pub_cfg, sizeof(pub_cfg));
    pub_cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    pub_cfg.max_fills = 1;
    pub_cfg.max_fill_snapshot_bytes = 1;
    pub_cfg.callbacks.ctx = r;
    pub_cfg.callbacks.on_closed = on_closed;
    if (moq_pub_create(r->server, &r->alloc, &pub_cfg, &r->pub) != MOQ_OK) return false;
    r->ns = MOQ_BYTES_LITERAL("fetch-owners");
    moq_pub_track_cfg_t track_cfg;
    moq_pub_track_cfg_init_sized(&track_cfg, sizeof(track_cfg));
    track_cfg.track_namespace = (moq_namespace_t){&r->ns, 1};
    track_cfg.track_name = MOQ_BYTES_LITERAL("video");
    if (moq_pub_add_track(r->pub, &track_cfg, 1000, &r->track) != MOQ_OK) return false;
    for (unsigned i = 0; i < 2; ++i) {
        uint8_t properties[] = {0x10, (uint8_t)i};
        if (moq_rcbuf_create(&r->alloc, (const uint8_t *)(i ? "new" : "old"),
                              3, &r->payload[i]) != MOQ_OK ||
            moq_rcbuf_create(&r->alloc, properties, sizeof(properties),
                              &r->properties[i]) != MOQ_OK) return false;
    }
    return true;
}

static void close_rig(rig_t *r)
{
    moq_pub_destroy(r->pub);
    if (r->pair) moq_simpair_destroy(r->pair);
    for (unsigned i = 0; i < 2; ++i) {
        if (r->payload[i]) {
            CHECK(moq_rcbuf_refcount(r->payload[i]) == 1);
            moq_rcbuf_decref(r->payload[i]);
        }
        if (r->properties[i]) {
            CHECK(moq_rcbuf_refcount(r->properties[i]) == 1);
            moq_rcbuf_decref(r->properties[i]);
        }
    }
    CHECK(r->memory.live == 0);
}

static bool retain(rig_t *r, unsigned version, unsigned count)
{
    moq_pub_retained_object_t objects[OBJECTS];
    memset(objects, 0, sizeof(objects));
    for (unsigned i = 0; i < count; ++i) {
        objects[i].object_id = i;
        objects[i].payload = r->payload[version];
        objects[i].properties = r->properties[version];
        objects[i].end_of_group = i + 1 == count;
    }
    moq_pub_retained_group_cfg_t cfg;
    moq_pub_retained_group_cfg_init(&cfg);
    cfg.group_id = 3;
    cfg.objects = objects;
    cfg.object_count = count;
    return moq_pub_set_retained_group(r->pub, r->track, &cfg) == MOQ_OK;
}

static bool request(rig_t *r, unsigned count, moq_fetch_t *fetch)
{
    moq_fetch_cfg_t cfg;
    moq_fetch_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){&r->ns, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("video");
    cfg.start_group = cfg.end_group = 3;
    cfg.end_object = count;
    return moq_session_fetch(r->client, &cfg, 1000, fetch) == MOQ_OK && settle(r);
}

static bool block_actions(rig_t *r)
{
    for (unsigned i = 0; i < 3; ++i) {
        char name[32];
        int len = snprintf(name, sizeof(name), "held-%u", r->blocks++);
        if (len <= 0 || (size_t)len >= sizeof(name)) return false;
        moq_bytes_t ns = {(const uint8_t *)name, (size_t)len};
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){&ns, 1};
        moq_announcement_t ann;
        if (moq_session_publish_namespace(r->server, &cfg, 1000, &ann) != MOQ_OK)
            return false;
    }
    return moq_session_action_capacity(r->server) == 0;
}

static void unblock_actions(rig_t *r)
{
    /* These are local pressure actions, not part of the simulated exchange. */
    moq_action_t action;
    unsigned count = 0;
    while (moq_session_poll_actions(r->server, &action, 1)) {
        CHECK(action.kind == MOQ_ACTION_OPEN_BIDI_STREAM);
        ++count;
        moq_action_cleanup(&action);
    }
    CHECK(count == 3);
}

static moq_result_t pump(rig_t *r, bool manual)
{
    if (!manual) return moq_pub_tick(r->pub, 1000);
    moq_result_t rc = moq_pub_flush(r->pub, 1000);
    if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_CLOSED) return rc;
    moq_event_t event;
    for (unsigned i = 0; i < FETCHES && moq_session_poll_events(r->server, &event, 1); ++i) {
        moq_pub_event_result_t result;
        rc = moq_pub_handle_event(r->pub, &event, 1000, &result);
        if (event.kind == MOQ_EVENT_FETCH_REQUEST)
            CHECK(result == MOQ_PUB_EVENT_CONSUMED);
        moq_event_cleanup(&event);
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK) return rc;
    }
    return moq_pub_flush(r->pub, 1000);
}

typedef struct {
    moq_fetch_t fetch;
    unsigned oks, objects, completes, errors, version;
} observation_t;

/* Inspect the actual queued pair without consuming it: settle() must still
 * deliver these exact actions through SimPair's ordinary stream mapping. */
static void check_fetch_phase(rig_t *r, moq_stream_ref_t request,
                              moq_stream_ref_t *data, unsigned phase,
                              unsigned version)
{
    const moq_session_t *s = r->server;
    CHECK(s->action_tail - s->action_head == 2);
    CHECK(moq_session_action_capacity(s) == 1);
    if (s->action_tail - s->action_head != 2) return;
    const moq_action_t *a = &s->actions[s->action_head % s->action_cap];
    const moq_action_t *b = &s->actions[(s->action_head + 1) % s->action_cap];
    if (phase == 0) {
        CHECK(a->kind == MOQ_ACTION_SEND_BIDI_STREAM);
        CHECK(b->kind == MOQ_ACTION_SEND_DATA);
        if (a->kind != MOQ_ACTION_SEND_BIDI_STREAM || b->kind != MOQ_ACTION_SEND_DATA) return;
        CHECK(a->u.send_bidi_stream.stream_ref._v == request._v);
        CHECK(!a->u.send_bidi_stream.fin && a->u.send_bidi_stream.len > 0);
        CHECK(!b->u.send_data.fin && !b->u.send_data.payload && b->u.send_data.header_len > 0);
        CHECK(data->_v == 0 && b->u.send_data.stream_ref._v != 0);
        *data = b->u.send_data.stream_ref;
    } else {
        CHECK(a->kind == MOQ_ACTION_SEND_DATA);
        if (a->kind != MOQ_ACTION_SEND_DATA) return;
        CHECK(a->u.send_data.stream_ref._v == data->_v && data->_v != 0);
        if (phase == 1) {
            CHECK(!a->u.send_data.fin && a->u.send_data.payload == r->properties[version]);
            CHECK(b->kind == MOQ_ACTION_SEND_DATA);
            if (b->kind != MOQ_ACTION_SEND_DATA) return;
            CHECK(b->u.send_data.stream_ref._v == data->_v);
            CHECK(!b->u.send_data.fin && b->u.send_data.payload == r->payload[version]);
        } else {
            CHECK(phase == 2);
            CHECK(a->u.send_data.fin && !a->u.send_data.payload && !a->u.send_data.header_len);
            CHECK(b->kind == MOQ_ACTION_CLOSE_BIDI_STREAM);
            if (b->kind != MOQ_ACTION_CLOSE_BIDI_STREAM) return;
            CHECK(b->u.close_bidi_stream.stream_ref._v == request._v);
        }
    }
}

static void observe(rig_t *r, observation_t *o, unsigned count)
{
    moq_event_t e;
    while (moq_session_poll_events(r->client, &e, 1)) {
        moq_fetch_t h = MOQ_FETCH_INVALID;
        if (e.kind == MOQ_EVENT_FETCH_OK) h = e.u.fetch_ok.fetch;
        if (e.kind == MOQ_EVENT_FETCH_OBJECT) h = e.u.fetch_object.fetch;
        if (e.kind == MOQ_EVENT_FETCH_COMPLETE) h = e.u.fetch_complete.fetch;
        if (e.kind == MOQ_EVENT_FETCH_ERROR) h = e.u.fetch_error.fetch;
        if (e.kind == MOQ_EVENT_SESSION_CLOSED) {
            fprintf(stderr, "unexpected client close: %llu %.*s\n",
                    (unsigned long long)e.u.closed.code,
                    (int)e.u.closed.reason.len, (const char *)e.u.closed.reason.data);
            for (unsigned i = 0; i < count; ++i)
                fprintf(stderr, "fetch %u: %u/%u/%u/%u\n", i,
                        o[i].oks, o[i].objects, o[i].completes, o[i].errors);
        }
        CHECK(e.kind != MOQ_EVENT_SESSION_CLOSED);
        if (moq_fetch_is_valid(h)) {
            unsigned i;
            for (i = 0; i < count && !moq_fetch_eq(h, o[i].fetch); ++i) {}
            CHECK(i < count);
            if (i < count) {
                if (e.kind == MOQ_EVENT_FETCH_OK) ++o[i].oks;
                if (e.kind == MOQ_EVENT_FETCH_COMPLETE) ++o[i].completes;
                if (e.kind == MOQ_EVENT_FETCH_ERROR) {
                    ++o[i].errors;
                    CHECK(e.u.fetch_error.error_code == MOQ_REQUEST_ERROR_INTERNAL_ERROR);
                }
                if (e.kind == MOQ_EVENT_FETCH_OBJECT) {
                    const moq_fetch_object_event_t *obj = &e.u.fetch_object;
                    CHECK(obj->group_id == 3 && obj->object_id == o[i].objects);
                    /* The snapshot carries the Subgroup ID each Object was
                     * assigned at install, so neither replacement nor removal
                     * of the track changes it. */
                    CHECK(!obj->datagram && obj->subgroup_id == 0);
                    CHECK(obj->payload && moq_rcbuf_len(obj->payload) == 3);
                    if (obj->payload && moq_rcbuf_len(obj->payload) == 3)
                        CHECK(memcmp(moq_rcbuf_data(obj->payload), o[i].version ? "new" : "old", 3) == 0);
                    CHECK(obj->properties && moq_rcbuf_len(obj->properties) == 2);
                    if (obj->properties && moq_rcbuf_len(obj->properties) == 2) {
                        CHECK(moq_rcbuf_data(obj->properties)[0] == 0x10);
                        CHECK(moq_rcbuf_data(obj->properties)[1] == o[i].version);
                    }
                    ++o[i].objects;
                }
            }
        }
        moq_event_cleanup(&e);
    }
}

typedef enum { COMPLETE, ALLOC_FAIL, CANCEL, LOCAL_CLOSE, TRANSPORT_CLOSE, DESTROY } ending_t;

static void full_pool(bool manual, ending_t ending)
{
    unsigned before = failures;
    ++cases;
    rig_t r = {0};
    observation_t o[FETCHES] = {0};
    NEED(open_rig(&r));
    NEED(retain(&r, 0, OBJECTS));
    for (unsigned i = 0; i < FETCHES; ++i) NEED(request(&r, OBJECTS, &o[i].fetch));
    NEED(r.server->event_tail - r.server->event_head == FETCHES);
    NEED(block_actions(&r));
    if (ending == ALLOC_FAIL) r.memory.fail_at = r.memory.calls + 4;
    size_t allocations = r.memory.calls;
    moq_result_t rc = pump(&r, manual);
    CHECK(rc == MOQ_ERR_WOULD_BLOCK);
    CHECK(r.server->event_tail == r.server->event_head);
    CHECK(r.memory.calls - allocations == FETCHES);
    r.memory.fail_at = 0;
    unsigned owned = FETCHES - (ending == ALLOC_FAIL);
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 1 + (owned + 1) * OBJECTS);
    CHECK(moq_rcbuf_refcount(r.properties[0]) == 1 + (owned + 1) * OBJECTS);
    for (unsigned i = 0; i < 3; ++i) {
        CHECK(pump(&r, manual) == MOQ_ERR_WOULD_BLOCK);
        CHECK(r.memory.calls - allocations == FETCHES);
    }
    /* Replacement and track removal cannot invalidate the sixteen snapshots. */
    NEED(retain(&r, 1, OBJECTS));
    NEED(moq_pub_remove_track(r.pub, r.track, 1000) == MOQ_OK);
    r.track = NULL;
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 1 + owned * OBJECTS);
    CHECK(moq_rcbuf_refcount(r.properties[0]) == 1 + owned * OBJECTS);
    CHECK(moq_rcbuf_refcount(r.payload[1]) == 1);
    CHECK(moq_rcbuf_refcount(r.properties[1]) == 1);
    if (ending == DESTROY) {
        moq_pub_destroy(r.pub);
        r.pub = NULL;
    } else if (ending == LOCAL_CLOSE || ending == TRANSPORT_CLOSE) {
        if (ending == LOCAL_CLOSE)
            NEED(moq_session_close(r.server, 0x99, "owners", 1000) == MOQ_OK);
        else
            NEED(moq_session_on_transport_close(r.server, 0x99, 1000) == MOQ_OK);
        rc = pump(&r, manual);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_CLOSED);
        CHECK(r.closes == 1);
        rc = pump(&r, manual);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_CLOSED);
        CHECK(r.closes == 1);
    } else if (ending == CANCEL) {
        unblock_actions(&r);
        for (unsigned i = 0; i < FETCHES; ++i) {
            NEED(moq_session_fetch_cancel(r.client, o[i].fetch, 1000) == MOQ_OK);
            NEED(settle(&r));
            discard_events(r.client);
        }
        NEED(block_actions(&r));
        /* Reference release must not itself need action or event credit. */
        CHECK(moq_pub_flush(r.pub, 1000) == MOQ_OK);
        CHECK(r.server->event_tail - r.server->event_head == FETCHES);
        CHECK(pump(&r, manual) == MOQ_OK);
    } else {
        unblock_actions(&r);
        NEED(settle(&r));
        observe(&r, o, FETCHES);
        unsigned round;
        for (round = 0; round < 1200; ++round) {
            rc = pump(&r, manual);
            NEED(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
            NEED(settle(&r));
            observe(&r, o, FETCHES);
            unsigned finished = 0;
            for (unsigned i = 0; i < FETCHES; ++i)
                finished += o[i].completes + o[i].errors;
            if (finished == FETCHES) break;
        }
        CHECK(round < 1200);
        for (unsigned i = 0; i < FETCHES; ++i) {
            bool rejected = ending == ALLOC_FAIL && i == 3;
            CHECK(o[i].oks == (rejected ? 0u : 1u));
            CHECK(o[i].objects == (rejected ? 0u : OBJECTS));
            CHECK(o[i].completes == (rejected ? 0u : 1u));
            CHECK(o[i].errors == (rejected ? 1u : 0u));
        }
        CHECK(moq_pub_flush(r.pub, 1000) == MOQ_OK);
        CHECK(moq_session_state(r.server) == MOQ_SESS_ESTABLISHED);
    }
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 1);
    CHECK(moq_rcbuf_refcount(r.properties[0]) == 1);
cleanup:
    close_rig(&r);
    printf("full_pool manual=%u ending=%u: %s\n", manual, (unsigned)ending,
           failures == before ? "PASS" : "FAIL");
}

static void generation_reuse(void)
{
    unsigned before = failures;
    ++cases;
    rig_t r = {0};
    moq_event_t cancelled = {0}, event = {0};
    observation_t old = {0}, replacement = {.version = 1};
    NEED(open_rig(&r));
    NEED(retain(&r, 0, 1));
    NEED(request(&r, 1, &old.fetch));
    NEED(block_actions(&r));
    NEED(moq_session_poll_events(r.server, &event, 1) == 1);
    NEED(event.kind == MOQ_EVENT_FETCH_REQUEST);
    moq_fetch_t original = event.u.fetch_request.fetch;
    moq_pub_event_result_t result;
    NEED(moq_pub_handle_event(r.pub, &event, 1000, &result) == MOQ_ERR_WOULD_BLOCK);
    CHECK(result == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&event);
    unblock_actions(&r);
    NEED(moq_session_fetch_cancel(r.client, old.fetch, 1000) == MOQ_OK);
    NEED(settle(&r));
    discard_events(r.client);
    NEED(moq_session_poll_events(r.server, &cancelled, 1) == 1);
    NEED(cancelled.kind == MOQ_EVENT_FETCH_CANCELLED);
    /* This real terminal is scalar-only, so holding it across the replacement
     * needs no borrowed span. Dispatch it after the newer request. */
    NEED(retain(&r, 1, 1));
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 2);
    NEED(request(&r, 1, &replacement.fetch));
    NEED(block_actions(&r));
    NEED(moq_session_poll_events(r.server, &event, 1) == 1);
    NEED(event.kind == MOQ_EVENT_FETCH_REQUEST);
    moq_fetch_t newer = event.u.fetch_request.fetch;
    CHECK(moq_handle_slot(original._opaque) == moq_handle_slot(newer._opaque));
    CHECK(!moq_fetch_eq(original, newer));
    NEED(moq_pub_handle_event(r.pub, &event, 1000, &result) == MOQ_ERR_WOULD_BLOCK);
    CHECK(result == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&event);
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 1);
    CHECK(moq_rcbuf_refcount(r.properties[0]) == 1);
    CHECK(moq_rcbuf_refcount(r.payload[1]) == 3);
    CHECK(moq_rcbuf_refcount(r.properties[1]) == 3);
    CHECK(moq_pub_handle_event(r.pub, &cancelled, 1000, &result) == MOQ_OK);
    CHECK(result == MOQ_PUB_EVENT_IGNORED);
    moq_event_cleanup(&cancelled);
    CHECK(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_rcbuf_refcount(r.payload[1]) == 3);
    NEED(moq_pub_clear_retained_group(r.pub, r.track) == MOQ_OK);
    unblock_actions(&r);
    NEED(settle(&r));
    discard_events(r.client);
    for (unsigned i = 0; i < 8; ++i) {
        moq_result_t rc = pump(&r, true);
        NEED(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        NEED(settle(&r));
        observe(&r, &replacement, 1);
    }
    CHECK(replacement.oks == 1 && replacement.objects == 1 &&
          replacement.completes == 1 && replacement.errors == 0);
    CHECK(moq_rcbuf_refcount(r.payload[1]) == 1);
    CHECK(moq_rcbuf_refcount(r.properties[1]) == 1);
cleanup:
    moq_event_cleanup(&event);
    moq_event_cleanup(&cancelled);
    close_rig(&r);
    printf("generation_reuse: %s\n", failures == before ? "PASS" : "FAIL");
}

static void cancel_phase(bool manual, bool written)
{
    unsigned before = failures;
    ++cases;
    rig_t r = {0};
    observation_t o = {0};
    NEED(open_rig(&r));
    NEED(retain(&r, 0, 1));
    NEED(request(&r, 1, &o.fetch));
    NEED(block_actions(&r));
    NEED(pump(&r, manual) == MOQ_ERR_WOULD_BLOCK);
    unblock_actions(&r);
    /* FETCH_OK/header use two slots; the two-action property/object write
     * cannot fit in the remaining one. No duplicate OK on later retries. */
    NEED(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_session_action_capacity(r.server) == 1);
    NEED(settle(&r));
    observe(&r, &o, 1);
    CHECK(o.oks == 1 && o.objects == 0 && o.completes == 0);
    if (written) {
        moq_bytes_t ns = MOQ_BYTES_LITERAL("fin-pressure");
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){&ns, 1};
        moq_announcement_t ann;
        NEED(moq_session_publish_namespace(r.server, &cfg, 1000, &ann) == MOQ_OK);
        NEED(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_session_action_capacity(r.server) == 0);
        moq_action_t pressure;
        NEED(moq_session_poll_actions(r.server, &pressure, 1) == 1);
        CHECK(pressure.kind == MOQ_ACTION_OPEN_BIDI_STREAM);
        moq_action_cleanup(&pressure);
        NEED(settle(&r));
        observe(&r, &o, 1);
        CHECK(o.oks == 1 && o.objects == 1 && o.completes == 0);
    }
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 3);
    CHECK(moq_rcbuf_refcount(r.properties[0]) == 3);
    NEED(moq_session_fetch_cancel(r.client, o.fetch, 1000) == MOQ_OK);
    NEED(settle(&r));
    NEED(block_actions(&r));
    CHECK(pump(&r, manual) == MOQ_OK);
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 2);
    CHECK(moq_rcbuf_refcount(r.properties[0]) == 2);
    unblock_actions(&r);
    for (unsigned i = 0; i < 3; ++i) {
        CHECK(pump(&r, manual) == MOQ_OK);
        NEED(settle(&r));
        observe(&r, &o, 1);
    }
    CHECK(o.oks == 1 && o.objects == (written ? 1u : 0u) &&
          o.completes == 0 && o.errors == 0);
cleanup:
    close_rig(&r);
    printf("cancel_phase manual=%u written=%u: %s\n", manual, written,
           failures == before ? "PASS" : "FAIL");
}

static void later_owner_while_head_event_blocked(bool manual)
{
    unsigned before = failures;
    ++cases;
    rig_t r = {0};
    observation_t o[2] = {0};
    moq_fetch_t warmup = MOQ_FETCH_INVALID;
    NEED(open_rig(&r));
    NEED(retain(&r, 0, 1));
    /* Retire request zero so this case isolates event-credit progress from
     * the staging-registry identity covered by request_staging_ownership. */
    NEED(request(&r, 1, &warmup));
    NEED(moq_session_fetch_cancel(r.client, warmup, 1000) == MOQ_OK);
    NEED(settle(&r));
    discard_events(r.client);
    discard_events(r.server);
    for (unsigned i = 0; i < 2; ++i) NEED(request(&r, 1, &o[i].fetch));
    const moq_event_t *event =
        &r.server->events[r.server->event_head % r.server->event_cap];
    NEED(event->kind == MOQ_EVENT_FETCH_REQUEST);
    moq_fetch_t first = event->u.fetch_request.fetch;
    moq_stream_ref_t ref = r.server->fetches[moq_handle_slot(first._opaque)].rs.ref;
    event = &r.server->events[(r.server->event_head + 1) % r.server->event_cap];
    NEED(event->kind == MOQ_EVENT_FETCH_REQUEST);
    moq_stream_ref_t later_ref =
        r.server->fetches[moq_handle_slot(event->u.fetch_request.fetch._opaque)].rs.ref;
    moq_stream_ref_t later_data = {0};
    NEED(block_actions(&r));
    NEED(pump(&r, manual) == MOQ_ERR_WOULD_BLOCK);
    NEED(r.server->event_tail == r.server->event_head);
    unblock_actions(&r);
    for (unsigned i = 0; i < FETCHES; ++i) {
        char name[32];
        int n = snprintf(name, sizeof(name), "fairness-%u", i);
        NEED(n > 0 && (size_t)n < sizeof(name));
        moq_bytes_t ns = {(const uint8_t *)name, (size_t)n};
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){&ns, 1};
        moq_announcement_t ann;
        NEED(moq_session_publish_namespace(r.client, &cfg, 1000, &ann) == MOQ_OK);
        NEED(settle(&r));
    }
    NEED(r.server->event_tail - r.server->event_head == FETCHES);
    /* D22 sections 8.9/9.5: fresh UPDATE100 uses unregistered auth alias99.
     * Its refusal needs event credit; the second FETCH has no such dependency. */
    static const uint8_t refused_update[] = {0x02, 0, 6, 100, 1, 0x03, 2, 2, 99};
    NEED(moq_session_on_bidi_stream_bytes(r.server, ref, refused_update,
                                          sizeof(refused_update), false, 1000) == MOQ_OK);
    NEED(r.server->fetches[moq_handle_slot(first._opaque)].upd_failed);
    for (unsigned phase = 0; phase < FETCH_PHASES; ++phase) {
        CHECK(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
        CHECK(r.server->event_tail - r.server->event_head == FETCHES);
        check_fetch_phase(&r, later_ref, &later_data, phase, 0);
        NEED(settle(&r));
        observe(&r, o, 2);
        CHECK(o[0].oks == 0 && o[0].objects == 0 && o[0].completes == 0 && o[0].errors == 0);
        CHECK(o[1].oks == 1 && o[1].objects == (phase >= 1 ? 1u : 0u) &&
              o[1].completes == (phase == 2 ? 1u : 0u) && o[1].errors == 0);
        CHECK(moq_rcbuf_refcount(r.payload[0]) == (phase == 2 ? 3u : 4u));
        CHECK(moq_rcbuf_refcount(r.properties[0]) == (phase == 2 ? 3u : 4u));
    }
    CHECK(o[0].oks == 0 && o[0].objects == 0 &&
          o[0].completes == 0 && o[0].errors == 0);
    CHECK(o[1].oks == 1 && o[1].objects == 1 &&
          o[1].completes == 1 && o[1].errors == 0);
    CHECK(r.server->event_tail - r.server->event_head == FETCHES);
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 3);
    CHECK(moq_rcbuf_refcount(r.properties[0]) == 3);
cleanup:
    close_rig(&r);
    printf("later_owner_while_head_event_blocked manual=%u: %s\n", manual,
           failures == before ? "PASS" : "FAIL");
}

static void fifo_after_middle_reuse(bool manual)
{
    unsigned before = failures;
    ++cases;
    rig_t r = {0};
    moq_fetch_t original[3] = {0};
    moq_fetch_t server_handles[3] = {0};
    moq_event_t cancelled = {0}, event = {0};
    observation_t o[3] = {0};
    moq_stream_ref_t request_refs[3] = {{0}}, data_refs[3] = {{0}};
    moq_pub_event_result_t result;
    unsigned next_ok = 0;
    NEED(open_rig(&r));
    NEED(retain(&r, 0, 1));
    for (unsigned i = 0; i < 3; ++i) NEED(request(&r, 1, &original[i]));
    NEED(block_actions(&r));
    for (unsigned i = 0; i < 3; ++i) {
        NEED(moq_session_poll_events(r.server, &event, 1) == 1);
        NEED(event.kind == MOQ_EVENT_FETCH_REQUEST);
        server_handles[i] = event.u.fetch_request.fetch;
        NEED(moq_pub_handle_event(r.pub, &event, 1000, &result) == MOQ_ERR_WOULD_BLOCK);
        CHECK(result == MOQ_PUB_EVENT_CONSUMED);
        moq_event_cleanup(&event);
    }
    request_refs[0] = r.server->fetches[moq_handle_slot(server_handles[0]._opaque)].rs.ref;
    request_refs[1] = r.server->fetches[moq_handle_slot(server_handles[2]._opaque)].rs.ref;
    unblock_actions(&r);
    NEED(moq_session_fetch_cancel(r.client, original[1], 1000) == MOQ_OK);
    NEED(settle(&r));
    discard_events(r.client);
    NEED(moq_session_poll_events(r.server, &cancelled, 1) == 1);
    NEED(cancelled.kind == MOQ_EVENT_FETCH_CANCELLED);
    NEED(retain(&r, 1, 1));
    o[0].fetch = original[0];
    o[1].fetch = original[2];
    o[2].version = 1;
    NEED(request(&r, 1, &o[2].fetch));
    NEED(block_actions(&r));
    NEED(moq_session_poll_events(r.server, &event, 1) == 1);
    NEED(event.kind == MOQ_EVENT_FETCH_REQUEST);
    CHECK(moq_handle_slot(event.u.fetch_request.fetch._opaque) ==
          moq_handle_slot(server_handles[1]._opaque));
    CHECK(!moq_fetch_eq(event.u.fetch_request.fetch, server_handles[1]));
    request_refs[2] = r.server->fetches[moq_handle_slot(event.u.fetch_request.fetch._opaque)].rs.ref;
    NEED(moq_pub_handle_event(r.pub, &event, 1000, &result) == MOQ_ERR_WOULD_BLOCK);
    CHECK(result == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&event);
    /* A delayed old cancellation must not remove the replacement or its new
     * FIFO position behind the surviving original owners. */
    CHECK(moq_pub_handle_event(r.pub, &cancelled, 1000, &result) == MOQ_OK);
    CHECK(result == MOQ_PUB_EVENT_IGNORED);
    moq_event_cleanup(&cancelled);
    NEED(moq_pub_remove_track(r.pub, r.track, 1000) == MOQ_OK);
    r.track = NULL;
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 3);
    CHECK(moq_rcbuf_refcount(r.payload[1]) == 2);
    unblock_actions(&r);
    for (unsigned round = 0; round < 3u * FETCH_PHASES; ++round) {
        unsigned owner = round / FETCH_PHASES, phase = round % FETCH_PHASES;
        moq_result_t rc = pump(&r, manual);
        CHECK(rc == (round + 1 == 3u * FETCH_PHASES ? MOQ_OK : MOQ_ERR_WOULD_BLOCK));
        check_fetch_phase(&r, request_refs[owner], &data_refs[owner], phase, o[owner].version);
        NEED(settle(&r));
        while (moq_session_poll_events(r.client, &event, 1)) {
            CHECK(event.kind != MOQ_EVENT_SESSION_CLOSED &&
                  event.kind != MOQ_EVENT_FETCH_ERROR);
            if (event.kind == MOQ_EVENT_FETCH_OK) {
                CHECK(next_ok < 3);
                if (next_ok < 3)
                    CHECK(moq_fetch_eq(event.u.fetch_ok.fetch, o[next_ok].fetch));
                ++next_ok;
            }
            for (unsigned i = 0; i < 3; ++i) {
                if (event.kind == MOQ_EVENT_FETCH_OBJECT &&
                    moq_fetch_eq(event.u.fetch_object.fetch, o[i].fetch)) {
                    ++o[i].objects;
                    CHECK(event.u.fetch_object.payload &&
                          moq_rcbuf_len(event.u.fetch_object.payload) == 3);
                    if (event.u.fetch_object.payload &&
                        moq_rcbuf_len(event.u.fetch_object.payload) == 3)
                        CHECK(memcmp(moq_rcbuf_data(event.u.fetch_object.payload),
                                     i == 2 ? "new" : "old", 3) == 0);
                }
                if (event.kind == MOQ_EVENT_FETCH_COMPLETE &&
                    moq_fetch_eq(event.u.fetch_complete.fetch, o[i].fetch))
                    ++o[i].completes;
            }
            moq_event_cleanup(&event);
        }
        CHECK(next_ok == owner + 1u);
        for (unsigned i = 0; i < 3; ++i) {
            CHECK(o[i].objects == (i < owner || (i == owner && phase >= 1) ? 1u : 0u));
            CHECK(o[i].completes == (i < owner || (i == owner && phase == 2) ? 1u : 0u));
        }
    }
    CHECK(next_ok == 3);
    for (unsigned i = 0; i < 3; ++i)
        CHECK(o[i].objects == 1 && o[i].completes == 1);
    CHECK(moq_pub_flush(r.pub, 1000) == MOQ_OK);
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 1 &&
          moq_rcbuf_refcount(r.properties[0]) == 1);
    CHECK(moq_rcbuf_refcount(r.payload[1]) == 1 &&
          moq_rcbuf_refcount(r.properties[1]) == 1);
cleanup:
    moq_event_cleanup(&event);
    moq_event_cleanup(&cancelled);
    close_rig(&r);
    printf("fifo_after_middle_reuse manual=%u: %s\n", manual,
           failures == before ? "PASS" : "FAIL");
}

static size_t measured_owner_size, measured_publisher_size;

static void constructor(moq_version_t version, uint32_t capacity)
{
    unsigned before = failures;
    ++cases;
    memory_t m = {0};
    moq_alloc_t a = {.ctx = &m, .alloc = allocate, .realloc = resize, .free = release};
    moq_session_t *s = NULL;
    moq_publisher_t *pub = NULL;
    moq_session_cfg_t session_cfg = MOQ_SESSION_CFG_INIT;
    session_cfg.alloc = &a;
    session_cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    session_cfg.version = version;
    session_cfg.max_fetches = capacity;
    NEED(moq_session_create(&session_cfg, 0, &s) == MOQ_OK);
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    size_t start = m.calls;
    NEED(moq_pub_create(s, &a, &cfg, &pub) == MOQ_OK);
    /* Publisher, optional fills, admissions, FETCH owners, generic answers. */
    unsigned count = version == MOQ_VERSION_DRAFT_22 ? 5 : 4;
    CHECK(m.calls - start == count);
    size_t base = m.sizes[start % 8];
    if (!measured_publisher_size) measured_publisher_size = base;
    CHECK(base == measured_publisher_size);
    size_t pool = m.sizes[(m.calls - 2) % 8];
    CHECK(pool % capacity == 0);
    if (!measured_owner_size) measured_owner_size = pool / capacity;
    CHECK(pool / capacity == measured_owner_size);
    CHECK(measured_owner_size >= sizeof(moq_fetch_t));
    printf("storage draft=%u capacity=%u publisher=%zu owner_pool=%zu cell=%zu\n",
           (unsigned)version, capacity, base, pool, pool / capacity);
    moq_pub_destroy(pub);
    pub = NULL;
    for (unsigned fail = 1; fail <= count; ++fail) {
        size_t live = m.live;
        m.fail_at = m.calls + fail;
        CHECK(moq_pub_create(s, &a, &cfg, &pub) == MOQ_ERR_NOMEM);
        CHECK(pub == NULL && m.live == live);
        m.fail_at = 0;
    }
cleanup:
    moq_pub_destroy(pub);
    moq_session_destroy(s);
    CHECK(m.live == 0);
    printf("constructor draft=%u capacity=%u: %s\n", (unsigned)version, capacity,
           failures == before ? "PASS" : "FAIL");
}

/* An accepted response with an unsent tail outlives its track: after
 * FETCH_OK the track is removed, and the response still sends every Object
 * of its snapshot, in order, with the snapshot's payloads, properties and
 * Subgroup IDs, then completes; every reference is released exactly once. */
static void removal_after_accept(bool manual)
{
    unsigned before = failures;
    ++cases;
    rig_t r = {0};
    observation_t o = {0};
    NEED(open_rig(&r));
    NEED(retain(&r, 0, 2));
    NEED(request(&r, 2, &o.fetch));
    NEED(block_actions(&r));
    NEED(pump(&r, manual) == MOQ_ERR_WOULD_BLOCK);
    unblock_actions(&r);
    /* FETCH_OK and its header take two slots; the first object does not
     * fit in the third. */
    NEED(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
    NEED(settle(&r));
    observe(&r, &o, 1);
    CHECK(o.oks == 1 && o.objects == 0 && o.completes == 0);
    NEED(moq_pub_remove_track(r.pub, r.track, 1000) == MOQ_OK);
    r.track = NULL;
    /* Only the response's snapshot still holds the two Objects. */
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 3);
    CHECK(moq_rcbuf_refcount(r.properties[0]) == 3);
    unsigned round;
    for (round = 0; round < 16 && !o.completes; ++round) {
        moq_result_t rc = pump(&r, manual);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        NEED(settle(&r));
        observe(&r, &o, 1);
    }
    CHECK(o.oks == 1 && o.objects == 2 && o.completes == 1 && o.errors == 0);
    CHECK(moq_pub_flush(r.pub, 1000) == MOQ_OK);
    CHECK(moq_rcbuf_refcount(r.payload[0]) == 1);
    CHECK(moq_rcbuf_refcount(r.properties[0]) == 1);
cleanup:
    close_rig(&r);
    printf("removal_after_accept manual=%u: %s\n", manual,
           failures == before ? "PASS" : "FAIL");
}

int main(void)
{
    constructor(MOQ_VERSION_DRAFT_16, 16);
    constructor(MOQ_VERSION_DRAFT_18, 16);
    constructor(MOQ_VERSION_DRAFT_22, 1);
    constructor(MOQ_VERSION_DRAFT_22, 3);
    constructor(MOQ_VERSION_DRAFT_22, 16);
    constructor(MOQ_VERSION_DRAFT_22, 32);
    for (unsigned manual = 0; manual < 2; ++manual)
        for (ending_t ending = COMPLETE; ending <= DESTROY; ++ending)
            full_pool(manual != 0, ending);
    generation_reuse();
    for (unsigned manual = 0; manual < 2; ++manual) {
        cancel_phase(manual != 0, false);
        cancel_phase(manual != 0, true);
        later_owner_while_head_event_blocked(manual != 0);
        fifo_after_middle_reuse(manual != 0);
        removal_after_accept(manual != 0);
    }
    printf("retained FETCH owners: %u cases, %u checks, %u failures, %u prerequisite failures\n",
           cases, checks, failures, prerequisites);
    return failures ? 1 : 0;
}
