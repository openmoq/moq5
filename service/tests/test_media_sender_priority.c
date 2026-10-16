/* Per-track priority through the production service hook, publisher and
 * SimPair wire decoder. No transport provider or private field inspection. */
#include <moq/media_sender.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/sim.h>
#include "test_support.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

static int failures;

/* Existing MOQ_MEDIA_SENDER_TESTING seams, absent from shipping exports. */
moq_media_sender_t *moq_media_sender_test_new_cfg(
    const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_pump(moq_media_sender_t *s,
                               moq_session_t *session, uint64_t now_us);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_clock_ms(moq_media_sender_t *s, bool set,
                                      uint64_t ms);

enum { CATALOG, MEDIA, SAP, TIMELINE, NSUB };
typedef struct {
    moq_simpair_t *pair;
    moq_media_sender_t *sender;
    moq_namespace_t ns;
    moq_subscription_t sub[NSUB];
    unsigned oks[NSUB], objects[NSUB], fins[NSUB];
    unsigned announcements;
    uint64_t now_us;
    uint8_t priority;
    bool written;
} fixture_t;

static int subscription_index(const fixture_t *f, moq_subscription_t sub)
{
    for (int i = 0; i < NSUB; i++)
        if (f->sub[i]._opaque && moq_subscription_eq(f->sub[i], sub)) return i;
    return -1;
}

static void collect(fixture_t *f)
{
    moq_session_t *peer = moq_simpair_server(f->pair);
    moq_event_t ev;
    while (moq_session_poll_events(peer, &ev, 1) == 1) {
        switch (ev.kind) {
        case MOQ_EVENT_NAMESPACE_PUBLISHED: {
            const moq_namespace_published_event_t *n = &ev.u.namespace_published;
            f->announcements++;
            MOQ_TEST_CHECK_EQ_SIZE(n->track_namespace.count, f->ns.count);
            if (n->track_namespace.count == f->ns.count &&
                n->track_namespace.parts) {
                for (size_t i = 0; i < f->ns.count; i++) {
                    moq_bytes_t b = n->track_namespace.parts[i];
                    MOQ_TEST_CHECK_EQ_SIZE(b.len, f->ns.parts[i].len);
                    MOQ_TEST_CHECK(b.data != NULL);
                    if (b.data && b.len == f->ns.parts[i].len)
                        MOQ_TEST_CHECK(memcmp(b.data, f->ns.parts[i].data,
                                              b.len) == 0);
                }
            } else MOQ_TEST_CHECK(false);
            moq_accept_namespace_cfg_t ac;
            moq_accept_namespace_cfg_init(&ac);
            MOQ_TEST_CHECK_EQ_INT(moq_session_accept_namespace(peer, n->ann,
                &ac, f->now_us), MOQ_OK);
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_OK: {
            int i = subscription_index(f, ev.u.subscribe_ok.sub);
            MOQ_TEST_CHECK(i >= 0);
            if (i >= 0) MOQ_TEST_CHECK_EQ_INT(++f->oks[i], 1);
            break;
        }
        case MOQ_EVENT_OBJECT_RECEIVED: {
            const moq_object_received_event_t *o = &ev.u.object_received;
            int i = subscription_index(f, o->sub);
            MOQ_TEST_CHECK(i >= 0);
            MOQ_TEST_CHECK_EQ_U64(o->pub._opaque, 0);
            MOQ_TEST_CHECK_EQ_INT(o->status, MOQ_OBJECT_NORMAL);
            MOQ_TEST_CHECK(!o->datagram);
            MOQ_TEST_CHECK(o->payload != NULL);
            if (i >= 0) {
                MOQ_TEST_CHECK_EQ_INT(++f->objects[i], 1);
                MOQ_TEST_CHECK_EQ_INT(o->publisher_priority,
                    i == CATALOG ? 0 : i == MEDIA ? f->priority : 128);
                MOQ_TEST_CHECK_EQ_U64(o->group_id, i == CATALOG ? 1 : 0);
                MOQ_TEST_CHECK_EQ_U64(o->object_id, 0);
                MOQ_TEST_CHECK_EQ_U64(o->subgroup_id, 0);
                MOQ_TEST_CHECK(o->end_of_group);
                if (i != CATALOG) MOQ_TEST_CHECK(f->written);
                if (o->payload) {
                    MOQ_TEST_CHECK(moq_rcbuf_len(o->payload) > 0);
                    if (i == MEDIA) {
                        static const uint8_t payload[] = { 0x31, 0x72, 0xa5 };
                        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(o->payload),
                                               sizeof(payload));
                        if (moq_rcbuf_len(o->payload) == sizeof(payload))
                            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(o->payload),
                                                  payload, sizeof(payload)) == 0);
                    }
                }
            }
            break;
        }
        case MOQ_EVENT_SUBGROUP_FINISHED: {
            const moq_subgroup_finished_event_t *t = &ev.u.subgroup_finished;
            int i = subscription_index(f, t->sub);
            MOQ_TEST_CHECK(i >= 0);
            MOQ_TEST_CHECK_EQ_U64(t->pub._opaque, 0);
            MOQ_TEST_CHECK_EQ_U64(t->group_id, i == CATALOG ? 1 : 0);
            MOQ_TEST_CHECK_EQ_U64(t->subgroup_id, 0);
            MOQ_TEST_CHECK(t->end_of_group);
            if (i >= 0) {
                MOQ_TEST_CHECK_EQ_INT(++f->fins[i], 1);
                MOQ_TEST_CHECK_EQ_INT(f->objects[i], 1);
            }
            break;
        }
        default:
            fprintf(stderr, "unexpected priority peer event: %d\n", (int)ev.kind);
            MOQ_TEST_CHECK(false);
            break;
        }
        moq_event_cleanup(&ev);
    }
}

static void drive(fixture_t *f)
{
    moq_media_sender_test_pump(f->sender, moq_simpair_client(f->pair),
                             f->now_us);
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_run_until_quiescent(f->pair, 64, NULL),
                          MOQ_OK);
    collect(f);
}

static void handshake(moq_session_t *session)
{
    unsigned count = 0;
    moq_event_t ev;
    while (moq_session_poll_events(session, &ev, 1) == 1) {
        MOQ_TEST_CHECK_EQ_INT(ev.kind, MOQ_EVENT_SETUP_COMPLETE);
        count++;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(count, 1);
}

static bool subscribe(fixture_t *f, int index, const char *name)
{
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = f->ns;
    cfg.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    cfg.has_forward = true;
    cfg.forward = true;
    moq_result_t rc = moq_session_subscribe(moq_simpair_server(f->pair), &cfg,
        f->now_us, &f->sub[index]);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) return false;
    for (int i = 0; i < 16 && !f->oks[index]; i++) drive(f);
    MOQ_TEST_CHECK_EQ_INT(f->oks[index], 1);
    return f->oks[index] == 1;
}

typedef enum { EXPLICIT_0, EXPLICIT_64, EXPLICIT_255, ABSENT, OLD_EXTENT,
               TRUNCATED_PAIR, ABSENT_POISON, POST_READY } priority_case_t;

static moq_media_track_t *add_media(fixture_t *f, priority_case_t which)
{
    moq_media_track_cfg_t cfg;
    moq_media_track_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.name = MOQ_BYTES_LITERAL("v");
    cfg.media_type = MOQ_MEDIA_TYPE_VIDEO;
    cfg.packaging = MOQ_MEDIA_PACKAGING_RAW;
    cfg.codec = MOQ_BYTES_LITERAL("av01");
    cfg.bitrate = 1500000;
    cfg.has_publisher_priority = which != ABSENT && which != ABSENT_POISON;
    cfg.publisher_priority = which == EXPLICIT_255 ? 255 : 64;
    if (which == EXPLICIT_0) cfg.publisher_priority = 0;
    if (which == OLD_EXTENT) {
        /* The old symbol stamps its frozen extent, including old padding.
         * Poison the new fields after restoring the valid old-prefix values. */
        moq_media_track_cfg_t old;
        (moq_media_track_cfg_init)(&old);
        size_t end = offsetof(moq_media_track_cfg_t, alt_group) +
                     sizeof(cfg.alt_group);
        size_t align = _Alignof(moq_media_track_cfg_t);
        size_t legacy_size = (end + align - 1) / align * align;
        MOQ_TEST_CHECK_EQ_SIZE(old.struct_size, legacy_size);
        cfg.struct_size = (uint32_t)legacy_size;
        /* Poison every byte beyond the historical last member, including
         * any old trailing padding, without an invalid bool representation. */
        memset((uint8_t *)&cfg + end, 0xa5, sizeof(cfg) - end);
        cfg.has_publisher_priority = true;
        cfg.publisher_priority = 0;
    } else if (which == TRUNCATED_PAIR) {
        /* With the whole-pair gate removed, the prefix copy preserves true
         * but zero-fills the missing value: the peer sees 0 instead of 128. */
        cfg.struct_size = (uint32_t)offsetof(moq_media_track_cfg_t,
                                            publisher_priority);
        cfg.publisher_priority = 255;
    } else if (which == ABSENT_POISON) cfg.publisher_priority = 255;
    cfg.emit_sap_timeline = which == EXPLICIT_64;
    cfg.emit_media_timeline = which == EXPLICIT_64;
    moq_media_track_t *track = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_media_sender_add_track(f->sender, &cfg, &track),
                          MOQ_OK);
    MOQ_TEST_CHECK(track != NULL);
    return track;
}

static void run_case(moq_version_t version, priority_case_t which)
{
    int before = failures;
    fixture_t f;
    memset(&f, 0, sizeof(f));
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("priority") };
    f.ns = (moq_namespace_t){ parts, 1 };
    f.priority = which == EXPLICIT_0 ? 0 : which == EXPLICIT_255 ? 255 :
                 which == EXPLICIT_64 || which == POST_READY ? 64 : 128;
    moq_simpair_cfg_t sc = MOQ_SIMPAIR_CFG_INIT;
    sc.alloc = moq_alloc_default();
    sc.seed = 42;
    sc.initial_now_us = 1000;
    sc.version = version;
    sc.client_send_request_capacity = sc.server_send_request_capacity = true;
    sc.client_initial_request_capacity = sc.server_initial_request_capacity = 32;
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_create(&sc, &f.pair), MOQ_OK);
    if (!f.pair) goto done;
    moq_simpair_start(f.pair);
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_run_until_quiescent(f.pair, 64, NULL), MOQ_OK);
    handshake(moq_simpair_client(f.pair));
    handshake(moq_simpair_server(f.pair));
    f.now_us = moq_simpair_now_us(f.pair);
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_ = f.ns;
    cfg.publish_tracks = false;
    cfg.catalog_refresh_interval_us = 1000000;
    f.sender = moq_media_sender_test_new_cfg(&cfg);
    MOQ_TEST_CHECK(f.sender != NULL);
    if (!f.sender) goto done;
    moq_media_sender_test_set_clock_ms(f.sender, true, 1700000000000ull);
    moq_media_track_t *track = NULL;
    if (which != POST_READY) track = add_media(&f, which);
    for (int i = 0; i < 16 && !moq_media_sender_is_ready(f.sender); i++) drive(&f);
    MOQ_TEST_CHECK(moq_media_sender_is_ready(f.sender));
    MOQ_TEST_CHECK_EQ_INT(f.announcements, 1);
    if (which == POST_READY) {
        track = add_media(&f, which);
        /* Complete the independent catalog republish before subscribing. */
        for (int i = 0; i < 4; i++) drive(&f);
    } else if (!subscribe(&f, CATALOG, MOQ_MSF_CATALOG_TRACK_NAME)) goto done;
    if (!track || !subscribe(&f, MEDIA, "v")) goto done;
    if (which == EXPLICIT_64 &&
        (!subscribe(&f, SAP, "v.sap") ||
         !subscribe(&f, TIMELINE, "v.timeline"))) goto done;
    static const uint8_t payload[] = { 0x31, 0x72, 0xa5 };
    moq_rcbuf_t *buf = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(moq_alloc_default(), payload,
                                          sizeof(payload), &buf), MOQ_OK);
    if (!buf) goto done;
    moq_media_send_object_t obj;
    memset(&obj, 0, sizeof(obj));
    obj.struct_size = sizeof(obj);
    obj.payload = buf;
    obj.is_sync = obj.starts_group = obj.ends_group = true;
    obj.presentation_time_us = 1000;
    obj.has_sap_type = true;
    obj.sap_type = MOQ_SAP_TYPE_1;
    moq_result_t rc = moq_media_sender_write(f.sender, track, &obj);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc != MOQ_OK) { moq_rcbuf_decref(buf); goto done; }
    f.written = true;
    /* LARGEST_OBJECT does not replay the initial catalog. Advance the logical
     * hook time once to emit one refresh on its real subscribed wire path. */
    if (which != POST_READY) f.now_us += 1000000;
    /* Fixed extra drives also detect duplicate or unexpectedly delayed events. */
    for (int i = 0; i < 16; i++) drive(&f);
    /* END_OF_GROUP is a subgroup HEADER promise, not a FIN notification
     * (D16 section 10.4.2 / D18 section 11.4.2). sender_drain sets that bit
     * for every object, but keeps the publisher subgroup open until a later
     * group. write_stream_object closes it on group change; close_subgroup
     * emits the FIN action that SimPair delivers and rx_finish_stream turns
     * into SUBGROUP_FINISHED. Here every subscribed track emits only one
     * group, with no end_track/remove/second group/second catalog refresh.
     * Thus the complete OBJECT_RECEIVED payload and header bit above are
     * observable now; stream completion is deliberately not yet observable. */
    for (int i = 0; i < NSUB; i++) {
        int expected = i == MEDIA || (i == CATALOG && which != POST_READY) ||
                       (i >= SAP && which == EXPLICIT_64);
        MOQ_TEST_CHECK_EQ_INT(f.oks[i], expected);
        MOQ_TEST_CHECK_EQ_INT(f.objects[i], expected);
        MOQ_TEST_CHECK_EQ_INT(f.fins[i], 0);
    }
    MOQ_TEST_CHECK_EQ_INT(f.announcements, 1);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(f.sender));
done:
    if (f.sender) moq_media_sender_test_free(f.sender);
    if (f.pair) moq_simpair_destroy(f.pair);
    printf("%s: priority draft=%llu case=%d\n",
           failures == before ? "PASS" : "FAIL",
           (unsigned long long)version, (int)which);
}

int main(void)
{
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++)
        for (int c = EXPLICIT_0; c <= POST_READY; c++)
            run_case(versions[i], (priority_case_t)c);
    return failures ? 1 : 0;
}
