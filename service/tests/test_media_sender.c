/*
 * moq_media_sender_t slice 1: cfg validation, attach/create ownership and
 * endpoint attachment gating, add_track gating, the sync-anchored
 * pre-ready queue, terminal behavior, and a live loopback against a
 * raw-session picoquic server that accepts the namespace so the sender
 * reaches ready. Reaching ready already proves the catalog encoded and
 * published; verifying its CONTENT end to end needs a subscriber pulling
 * the track (a relay topology) and is deferred to the interop runner.
 */
#include <moq/media_sender.h>
#include <moq/picoquic_threaded.h>
#include <moq/msf.h>
#include <moq/loc.h>
#include <moq/wire.h>   /* MOQ_QUIC_VARINT_MAX */
#include "test_support.h"
#include "media_builders.h"  /* shared CENC-protected init builder */

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;

/* -- raw-session server: accept the namespace and subscribe to "v" ----- *
 * The sender is a client publisher; readiness requires the peer to accept
 * its PUBLISH_NAMESPACE, and the endpoint now grants the peer request
 * capacity so this server can SUBSCRIBE and pull the media track. The
 * server records each delivered object (group/object IDs, end-of-group,
 * and the round-tripped LOC timestamp + video sync marking) so the test
 * can verify the sender's emission and LOC-01 generation. */

#define SRV_MAX_OBJ 64
typedef struct {
    uint64_t group, object;
    bool     end_of_group;
    bool     has_ts;
    uint64_t ts;
    bool     keyframe;        /* video_frame_marking.independent */
    uint8_t  first_byte;
    moq_object_status_t status;   /* NORMAL / END_OF_TRACK (reliable terminal) */
} srv_obj_t;

/* One captured SAP timeline object: its Location + verbatim JSON payload. */
typedef struct {
    uint64_t group, object;
    uint8_t  buf[1024];
    size_t   len;
} srv_sap_obj_t;

/* One captured catalog object (generation): Location + verbatim JSON. */
typedef struct {
    uint64_t group, object;
    uint8_t  buf[4096];
    size_t   len;
    uint64_t seq;        /* monotonic observation order (see srv_state.ev_seq) */
} srv_cat_obj_t;

typedef struct {
    atomic_bool  ns_accepted;
    bool         no_subscribe;    /* gate the MEDIA ("v") subscription only --
                                     the catalog is always subscribed, so the
                                     network pump stays active and the drain
                                     runs even while "v" has no subscriber */
    bool         want_a;          /* also subscribe the "a" track (multi-track
                                     end_track test) */
    bool         want_sap;        /* also subscribe the generated "v.sap" track */
    int          sap_subscribe_after; /* delay v.sap SUBSCRIBE until this many
                                         "v" objects have arrived (late-join);
                                         0 = subscribe immediately */
    bool         cat_hold;        /* delay the catalog SUBSCRIBE (late joiner) */
    bool         cat_subscribed;
    bool         cat_fetch_issued; /* Joining FETCH(offset 0) for the catalog --
                                      how a joiner obtains the retained generation
                                      (gen 0); subsequent generations arrive live
                                      on the SUBSCRIBE. */
    moq_fetch_t  cat_fetch;
    bool         v_subscribed;
    atomic_bool  unsub_v_request;  /* test asks the peer to drop "v" (demand) */
    bool         v_unsubscribed;   /* pump unsubscribed "v"; do not resubscribe */
    uint64_t     goaway_timeout_us; /* server GOAWAY drain timeout (0 = off) */
    atomic_bool  goaway_request;   /* test asks the peer to GOAWAY + close */
    atomic_bool  goaway_sent;      /* pump sent GOAWAY once (read by the test) */
    bool         a_subscribed;
    bool         sap_subscribed;
    moq_subscription_t sub_cat, sub_v, sub_a, sub_sap;
    pthread_mutex_t mu;
    srv_obj_t    objs[SRV_MAX_OBJ];   /* "v" objects only */
    int          n;
    int          v_eot;           /* count of END_OF_TRACK status objects on "v" */
    int          a_count;         /* count of "a" objects (any status) */
    int          v_done;          /* count of SUBSCRIBE_DONE on "v" */
    uint64_t     v_done_status;   /* status code of the last "v" SUBSCRIBE_DONE */
    int          a_done;          /* count of SUBSCRIBE_DONE on "a" */
    uint64_t     a_done_status;   /* status code of the last "a" SUBSCRIBE_DONE */
    uint64_t     ev_seq;          /* monotonic observation counter (catalog
                                     objects + media SUBSCRIBE_DONEs) -- pins the
                                     order events were seen on the wire */
    uint64_t     v_done_seq;      /* ev_seq when "v" SUBSCRIBE_DONE was observed */
    uint64_t     a_done_seq;      /* ev_seq when "a" SUBSCRIBE_DONE was observed */
    srv_sap_obj_t saps[SRV_MAX_OBJ];  /* captured "v.sap" objects */
    int          sap_n;
    bool         want_timeline;   /* also subscribe the generated "v.timeline" */
    int          timeline_subscribe_after; /* delay "v.timeline" SUBSCRIBE until
                                              this many "v" objects arrive */
    bool         tl_subscribed;
    moq_subscription_t sub_tl;
    srv_sap_obj_t tls[SRV_MAX_OBJ];   /* captured "v.timeline" objects */
    int          tl_n;
    uint8_t      cat_buf[4096];   /* captured catalog payload (JSON), latest */
    size_t       cat_len;         /* 0 until the catalog object is received */
    srv_cat_obj_t cats[SRV_MAX_OBJ];  /* catalog generation sequence */
    int          cat_n;
} srv_state_t;

static srv_state_t g_srv;

static int srv_count(srv_state_t *st);   /* defined below; used in the pump */

static bool srv_do_subscribe(moq_session_t *session, const char *name,
                             uint64_t now_us, moq_subscription_t *out)
{
    moq_bytes_t parts[] = {
        MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_subscribe_cfg_t scfg;
    memset(&scfg, 0, sizeof(scfg));
    scfg.struct_size = sizeof(scfg);
    scfg.track_namespace = (moq_namespace_t){ parts, 2 };
    scfg.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    return moq_session_subscribe(session, &scfg, now_us, out) == MOQ_OK;
}

/* Record one catalog generation (group/object + verbatim JSON). Shared by the
 * SUBSCRIBE delivery path (live generations) and the Joining-FETCH path (the
 * retained gen 0). Caller holds no lock. */
static void srv_capture_catalog(srv_state_t *st, uint64_t group, uint64_t object,
                                moq_rcbuf_t *payload)
{
    if (!payload) return;
    size_t plen = moq_rcbuf_len(payload);
    if (plen == 0) return;
    pthread_mutex_lock(&st->mu);
    if (plen <= sizeof(st->cat_buf)) {
        memcpy(st->cat_buf, moq_rcbuf_data(payload), plen);
        st->cat_len = plen;
    }
    if (st->cat_n < SRV_MAX_OBJ) {
        srv_cat_obj_t *c = &st->cats[st->cat_n++];
        memset(c, 0, sizeof(*c));
        c->group = group;
        c->object = object;
        size_t pl = plen > sizeof(c->buf) ? sizeof(c->buf) : plen;
        memcpy(c->buf, moq_rcbuf_data(payload), pl);
        c->len = pl;
        c->seq = ++st->ev_seq;
    }
    pthread_mutex_unlock(&st->mu);
}

static int server_pump(moq_pq_threaded_t *t, uint64_t now_us, void *ctx)
{
    srv_state_t *st = (srv_state_t *)ctx;
    moq_session_t *session = moq_pq_threaded_session(t);
    if (!session) return 0;
    if (moq_session_state(session) != MOQ_SESS_ESTABLISHED) return 0;

    moq_event_t ev;
    while (moq_session_poll_events(session, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t acc;
            moq_accept_namespace_cfg_init(&acc);
            moq_session_accept_namespace(session,
                ev.u.namespace_published.ann, &acc, now_us);
            atomic_store(&st->ns_accepted, true);
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            /* Capture the catalog payload (so a test can verify what the sender
             * published, e.g. that init_data survives into the catalog); record
             * "v" objects (incl. the reliable END_OF_TRACK terminal); count "a"
             * objects. */
            if (st->cat_subscribed && moq_subscription_eq(o->sub, st->sub_cat)) {
                /* Live catalog generations (republishes) arrive here. */
                srv_capture_catalog(st, o->group_id, o->object_id, o->payload);
            } else if (st->a_subscribed && moq_subscription_eq(o->sub, st->sub_a)) {
                pthread_mutex_lock(&st->mu);
                st->a_count++;
                pthread_mutex_unlock(&st->mu);
            } else if (st->sap_subscribed &&
                moq_subscription_eq(o->sub, st->sub_sap)) {
                pthread_mutex_lock(&st->mu);
                if (st->sap_n < SRV_MAX_OBJ) {
                    srv_sap_obj_t *r = &st->saps[st->sap_n++];
                    memset(r, 0, sizeof(*r));
                    r->group = o->group_id;
                    r->object = o->object_id;
                    if (o->payload) {
                        size_t pl = moq_rcbuf_len(o->payload);
                        if (pl > sizeof(r->buf)) pl = sizeof(r->buf);
                        memcpy(r->buf, moq_rcbuf_data(o->payload), pl);
                        r->len = pl;
                    }
                }
                pthread_mutex_unlock(&st->mu);
            } else if (st->tl_subscribed &&
                moq_subscription_eq(o->sub, st->sub_tl)) {
                pthread_mutex_lock(&st->mu);
                if (st->tl_n < SRV_MAX_OBJ) {
                    srv_sap_obj_t *r = &st->tls[st->tl_n++];
                    memset(r, 0, sizeof(*r));
                    r->group = o->group_id;
                    r->object = o->object_id;
                    if (o->payload) {
                        size_t pl = moq_rcbuf_len(o->payload);
                        if (pl > sizeof(r->buf)) pl = sizeof(r->buf);
                        memcpy(r->buf, moq_rcbuf_data(o->payload), pl);
                        r->len = pl;
                    }
                }
                pthread_mutex_unlock(&st->mu);
            } else if (st->v_subscribed &&
                moq_subscription_eq(o->sub, st->sub_v)) {
                pthread_mutex_lock(&st->mu);
                if (o->status == MOQ_OBJECT_END_OF_TRACK) st->v_eot++;
                if (st->n < SRV_MAX_OBJ) {
                    srv_obj_t *r = &st->objs[st->n++];
                    memset(r, 0, sizeof(*r));
                    r->group = o->group_id;
                    r->object = o->object_id;
                    r->end_of_group = o->end_of_group;
                    r->status = o->status;
                    if (o->payload && moq_rcbuf_len(o->payload) > 0)
                        r->first_byte = moq_rcbuf_data(o->payload)[0];
                    if (o->properties) {
                        moq_loc_headers_t h;
                        moq_loc_headers_init(&h);
                        moq_bytes_t pb = { moq_rcbuf_data(o->properties),
                                           moq_rcbuf_len(o->properties) };
                        if (moq_loc_parse(MOQ_LOC_PROFILE_01, pb, &h)
                            == MOQ_OK) {
                            r->has_ts = h.has_timestamp;
                            r->ts = h.timestamp;
                            r->keyframe = h.has_video_frame_marking &&
                                          h.video_frame_marking.independent;
                        }
                    }
                }
                pthread_mutex_unlock(&st->mu);
            }
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            /* MSF §11.3 step 1: a converting track's live subscription is
             * completed with a status code (Track Ended = 0x2). Capture it per
             * media track so a test can assert the done was sent. */
            const moq_subscribe_done_event_t *dn = &ev.u.subscribe_done;
            pthread_mutex_lock(&st->mu);
            if (st->v_subscribed && moq_subscription_eq(dn->sub, st->sub_v)) {
                st->v_done++;
                st->v_done_status = dn->status_code;
                st->v_done_seq = ++st->ev_seq;
            } else if (st->a_subscribed &&
                       moq_subscription_eq(dn->sub, st->sub_a)) {
                st->a_done++;
                st->a_done_status = dn->status_code;
                st->a_done_seq = ++st->ev_seq;
            }
            pthread_mutex_unlock(&st->mu);
        } else if (ev.kind == MOQ_EVENT_FETCH_OBJECT) {
            /* The catalog's Joining FETCH(offset 0) delivers the retained
             * generation (gen 0) -- capture it as a catalog generation. */
            const moq_fetch_object_event_t *fo = &ev.u.fetch_object;
            if (st->cat_fetch_issued &&
                moq_fetch_eq(fo->fetch, st->cat_fetch))
                srv_capture_catalog(st, fo->group_id, fo->object_id, fo->payload);
        }
        moq_event_cleanup(&ev);
    }

    if (atomic_load(&st->ns_accepted)) {
        /* Catalog first (always, unless held for a late-joiner test): drives
         * pump activity + discovery. */
        if (!st->cat_hold && !st->cat_subscribed &&
            srv_do_subscribe(session, MOQ_MSF_CATALOG_TRACK_NAME, now_us,
                             &st->sub_cat))
            st->cat_subscribed = true;
        /* Obtain the retained catalog generation via a Joining FETCH(offset 0)
         * (MSF-01 §5) -- a plain SUBSCRIBE delivers only subsequent live
         * generations, not the retained base. */
        if (st->cat_subscribed && !st->cat_fetch_issued) {
            moq_fetch_cfg_t fcfg;
            moq_fetch_cfg_init(&fcfg);
            fcfg.is_joining = true;
            fcfg.joining_relative = true;
            fcfg.joining_start = 0;
            fcfg.joining_sub = st->sub_cat;
            if (moq_session_fetch(session, &fcfg, now_us, &st->cat_fetch) == MOQ_OK)
                st->cat_fetch_issued = true;
        }
        /* Media: gated, so a test can keep "v" unsubscribed while the pump
         * stays live via the catalog. */
        if (!st->no_subscribe && !st->v_subscribed && !st->v_unsubscribed &&
            srv_do_subscribe(session, "v", now_us, &st->sub_v))
            st->v_subscribed = true;
        /* Demand test: drop the "v" subscription so the sender sees a left. */
        if (atomic_load(&st->unsub_v_request) && st->v_subscribed &&
            !st->v_unsubscribed) {
            moq_session_unsubscribe(session, st->sub_v, now_us);
            st->v_unsubscribed = true;
            st->v_subscribed = false;
        }
        /* Demand test: gracefully close the whole connection (GOAWAY -> bounded
         * drain -> CONNECTION_CLOSE) so the publisher observes SESSION_CLOSED
         * promptly, without an explicit UNSUBSCRIBE. Needs goaway_timeout_us. */
        if (atomic_load(&st->goaway_request) &&
            !atomic_load(&st->goaway_sent)) {
            moq_session_goaway(session, NULL, 0, now_us);
            atomic_store(&st->goaway_sent, true);
        }
        if (st->want_a && !st->a_subscribed &&
            srv_do_subscribe(session, "a", now_us, &st->sub_a))
            st->a_subscribed = true;
        /* Generated SAP timeline track. Optionally delayed until some "v"
         * objects have arrived, to exercise the late-subscriber path (§8.3:
         * the first object must then be an immediate independent timeline). */
        if (st->want_sap && !st->sap_subscribed &&
            srv_count(st) >= st->sap_subscribe_after &&
            srv_do_subscribe(session, "v.sap", now_us, &st->sub_sap))
            st->sap_subscribed = true;
        /* Generated media-timeline track (MSF §7.3), same late-join option. */
        if (st->want_timeline && !st->tl_subscribed &&
            srv_count(st) >= st->timeline_subscribe_after &&
            srv_do_subscribe(session, "v.timeline", now_us, &st->sub_tl))
            st->tl_subscribed = true;
    }
    return 0;
}

static int srv_count(srv_state_t *st)
{
    pthread_mutex_lock(&st->mu);
    int n = st->n;
    pthread_mutex_unlock(&st->mu);
    return n;
}

static int srv_v_eot(srv_state_t *st)
{
    pthread_mutex_lock(&st->mu);
    int n = st->v_eot;
    pthread_mutex_unlock(&st->mu);
    return n;
}

static int srv_a_count(srv_state_t *st)
{
    pthread_mutex_lock(&st->mu);
    int n = st->a_count;
    pthread_mutex_unlock(&st->mu);
    return n;
}

static void srv_dones(srv_state_t *st, int *vd, uint64_t *vs,
                      int *ad, uint64_t *as)
{
    pthread_mutex_lock(&st->mu);
    if (vd) *vd = st->v_done;
    if (vs) *vs = st->v_done_status;
    if (ad) *ad = st->a_done;
    if (as) *as = st->a_done_status;
    pthread_mutex_unlock(&st->mu);
}

static void srv_done_seqs(srv_state_t *st, uint64_t *vseq, uint64_t *aseq)
{
    pthread_mutex_lock(&st->mu);
    if (vseq) *vseq = st->v_done_seq;
    if (aseq) *aseq = st->a_done_seq;
    pthread_mutex_unlock(&st->mu);
}

static moq_pq_threaded_t *start_server(const char *cert, const char *key,
                                       srv_state_t *st, int *out_port)
{
    pthread_mutex_init(&st->mu, NULL);   /* st was memset by the caller */
    /* Port range disjoint from the other service tests. */
    static int calls = 0;
    int base = 18900 + (int)(getpid() % 997) + (calls++ * 131);
    for (int attempt = 0; attempt < 8; attempt++) {
        int port = base + attempt * 13;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.goaway_timeout_us = st->goaway_timeout_us;  /* 0 = off for most tests */
        cfg.on_pump = server_pump;
        cfg.on_pump_ctx = st;
        moq_pq_threaded_t *srv = NULL;
        if (moq_pq_threaded_create(&cfg, &srv) == MOQ_OK) {
            *out_port = port;
            return srv;
        }
    }
    return NULL;
}

/* -- helpers ---------------------------------------------------------- */

static void fill_cfg(moq_media_sender_cfg_t *cfg, moq_bytes_t *parts)
{
    moq_media_sender_cfg_init_live(cfg);
    parts[0] = MOQ_BYTES_LITERAL("svc");
    parts[1] = MOQ_BYTES_LITERAL("demo");
    cfg->namespace_ = (moq_namespace_t){ parts, 2 };
}

static moq_endpoint_cfg_t ep_cfg(char *urlbuf, size_t cap, int port)
{
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init(&c);
    snprintf(urlbuf, cap, "moqt://127.0.0.1:%d", port);
    c.url = (moq_bytes_t){ (const uint8_t *)urlbuf, strlen(urlbuf) };
    c.insecure_skip_verify = true;
    return c;
}

static void add_video_track(moq_media_sender_t *s, moq_media_track_t **out)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1500000;              /* MSF-01 5.2.22: required for video */
    tc.width = 1280;
    tc.height = 720;
    tc.timescale = 1000000;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, out),
                          (int)MOQ_OK);
}

static void add_audio_track(moq_media_sender_t *s, moq_media_track_t **out)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("a");
    tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("opus");
    tc.samplerate = 48000;
    tc.channel_config = MOQ_BYTES_LITERAL("2");
    tc.bitrate = 32000;               /* MSF-01 5.2.22: required for audio */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, out),
                          (int)MOQ_OK);
}

static moq_rcbuf_t *mkbuf(size_t n, uint8_t fill)
{
    uint8_t *tmp = (uint8_t *)malloc(n);
    memset(tmp, fill, n);
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), tmp, n, &b);
    free(tmp);
    return b;
}

static moq_media_send_object_t mkobj(moq_rcbuf_t *p, bool sync,
                                     bool starts, bool ends)
{
    moq_media_send_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = p;
    o.is_sync = sync;
    o.starts_group = starts;
    o.ends_group = ends;
    return o;
}

/* mkobj + an app-declared SAP type and presentation time (for SAP timeline). */
static moq_media_send_object_t mkobj_sap(moq_rcbuf_t *p, bool sync, bool starts,
                                         bool ends, moq_sap_type_t sap,
                                         uint64_t pts_us)
{
    moq_media_send_object_t o = mkobj(p, sync, starts, ends);
    o.has_sap_type = true;
    o.sap_type = sap;
    o.presentation_time_us = pts_us;
    return o;
}

/* Add a RAW video track "v" with SAP timeline generation enabled. */
static void add_video_track_sap(moq_media_sender_t *s, uint32_t history_groups,
                                moq_media_track_t **out)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1500000;             /* MSF-01 5.2.22: required for video */
    tc.timescale = 1000000;
    tc.emit_sap_timeline = true;
    tc.sap_timeline_history_groups = history_groups;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, out),
                          (int)MOQ_OK);
}

static int srv_sap_count(srv_state_t *st)
{
    pthread_mutex_lock(&st->mu);
    int n = st->sap_n;
    pthread_mutex_unlock(&st->mu);
    return n;
}

/* write() transfers payload ownership only on MOQ_OK; on any error the caller
 * still owns it. Decref on non-OK so the test never leaks. */
static moq_result_t sap_write(moq_media_sender_t *s, moq_media_track_t *v,
                              moq_media_send_object_t *o)
{
    moq_result_t rc = moq_media_sender_write(s, v, o);
    if (rc != MOQ_OK && o->payload) moq_rcbuf_decref(o->payload);
    return rc;
}

static bool buf_contains(const uint8_t *buf, size_t len, const char *needle)
{
    size_t nl = strlen(needle);
    if (len < nl) return false;
    for (size_t j = 0; j + nl <= len; j++)
        if (memcmp(buf + j, needle, nl) == 0) return true;
    return false;
}

/* True if any captured SAP object's payload contains `needle`. */
static bool srv_sap_any_contains(srv_state_t *st, const char *needle)
{
    bool found = false;
    pthread_mutex_lock(&st->mu);
    for (int i = 0; i < st->sap_n && !found; i++)
        found = buf_contains(st->saps[i].buf, st->saps[i].len, needle);
    pthread_mutex_unlock(&st->mu);
    return found;
}

/* True if a captured object 0 (independent) for timeline group `group` exists.
 * The timeline shares media group ids, so each media-group boundary emits its
 * own Object 0. */
static bool srv_sap_has_obj0(srv_state_t *st, uint64_t group)
{
    bool found = false;
    pthread_mutex_lock(&st->mu);
    for (int i = 0; i < st->sap_n && !found; i++)
        found = st->saps[i].object == 0 && st->saps[i].group == group;
    pthread_mutex_unlock(&st->mu);
    return found;
}

/* True if the independent Object 0 for timeline group `group` contains `a`
 * (and, when b != NULL, both). */
static bool srv_sap_obj0_has(srv_state_t *st, uint64_t group,
                             const char *a, const char *b)
{
    bool found = false;
    pthread_mutex_lock(&st->mu);
    for (int i = 0; i < st->sap_n && !found; i++) {
        if (st->saps[i].object != 0 || st->saps[i].group != group) continue;
        found = buf_contains(st->saps[i].buf, st->saps[i].len, a) &&
                (!b || buf_contains(st->saps[i].buf, st->saps[i].len, b));
    }
    pthread_mutex_unlock(&st->mu);
    return found;
}

/* Add a RAW video track "v" with MSF §7 media-timeline generation enabled. */
static void add_video_track_timeline(moq_media_sender_t *s,
                                     uint32_t history_groups, bool is_live,
                                     uint64_t dur_ms, moq_media_track_t **out)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc.codec = MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1500000;             /* MSF-01 5.2.22: required for video */
    tc.timescale = 1000000;
    tc.is_live = is_live;
    tc.has_track_duration = !is_live && dur_ms > 0;
    tc.track_duration_ms = dur_ms;
    tc.emit_media_timeline = true;
    tc.media_timeline_history_groups = history_groups;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, out),
                          (int)MOQ_OK);
}

/* True if a captured media-timeline object 0 (independent) for `group` exists. */
static bool srv_tl_has_obj0(srv_state_t *st, uint64_t group)
{
    bool found = false;
    pthread_mutex_lock(&st->mu);
    for (int i = 0; i < st->tl_n && !found; i++)
        found = st->tls[i].object == 0 && st->tls[i].group == group;
    pthread_mutex_unlock(&st->mu);
    return found;
}

/* Decode the captured media-timeline Object 0 for `group` into `out` (cap
 * records). Returns the decoded record count, or -1 if not found / malformed. */
static int srv_tl_decode_obj0(srv_state_t *st, uint64_t group,
                              moq_msf_media_timeline_record_t *out, size_t cap)
{
    int rc = -1;
    pthread_mutex_lock(&st->mu);
    for (int i = 0; i < st->tl_n; i++) {
        if (st->tls[i].object != 0 || st->tls[i].group != group) continue;
        size_t n = 0;
        if (moq_msf_media_timeline_decode(
                (moq_bytes_t){ st->tls[i].buf, st->tls[i].len },
                out, cap, &n) == MOQ_OK)
            rc = (int)n;
        break;
    }
    pthread_mutex_unlock(&st->mu);
    return rc;
}

/* Capture the published catalog JSON into out; returns its length (0 on
 * timeout). */
static size_t capture_catalog(uint8_t *out, size_t cap)
{
    size_t clen = 0;
    for (int i = 0; i < 200 && clen == 0; i++) {
        pthread_mutex_lock(&g_srv.mu);
        clen = g_srv.cat_len;
        if (clen > 0 && clen <= cap) memcpy(out, g_srv.cat_buf, clen);
        else clen = 0;
        pthread_mutex_unlock(&g_srv.mu);
        if (clen == 0) usleep(50000);
    }
    return clen;
}

static bool bytes_eq(moq_bytes_t b, const char *s);   /* defined below */

static const moq_msf_track_t *find_track(const moq_msf_catalog_t *c,
                                         const char *name)
{
    for (size_t i = 0; i < c->track_count; i++)
        if (bytes_eq(c->tracks[i].name, name)) return &c->tracks[i];
    return NULL;
}

/* Distinct objects captured for a catalog group (S2b: object 0 + deltas 1..N). */
static int group_obj_count(uint64_t group)
{
    int n = 0;
    pthread_mutex_lock(&g_srv.mu);
    for (int i = 0; i < g_srv.cat_n; i++) {
        if (g_srv.cats[i].group != group) continue;
        bool seen = false;
        for (int j = 0; j < i; j++)
            if (g_srv.cats[j].group == group &&
                g_srv.cats[j].object == g_srv.cats[i].object) { seen = true; break; }
        if (!seen) n++;
    }
    pthread_mutex_unlock(&g_srv.mu);
    return n;
}

/* Copy a captured object's bytes by (group, object); returns its length (0 if
 * not captured). */
static size_t cap_obj(uint64_t group, uint64_t object, uint8_t *out, size_t cap)
{
    size_t len = 0;
    pthread_mutex_lock(&g_srv.mu);
    for (int i = 0; i < g_srv.cat_n; i++)
        if (g_srv.cats[i].group == group && g_srv.cats[i].object == object) {
            len = g_srv.cats[i].len < cap ? g_srv.cats[i].len : cap;
            memcpy(out, g_srv.cats[i].buf, len);
            break;
        }
    pthread_mutex_unlock(&g_srv.mu);
    return len;
}

/* Observation seq of catalog generation `group` (object 0); 0 if not captured. */
static uint64_t cat_gen_seq(uint64_t group)
{
    uint64_t seq = 0;
    pthread_mutex_lock(&g_srv.mu);
    for (int i = 0; i < g_srv.cat_n; i++)
        if (g_srv.cats[i].group == group && g_srv.cats[i].object == 0) {
            seq = g_srv.cats[i].seq;
            break;
        }
    pthread_mutex_unlock(&g_srv.mu);
    return seq;
}

/* Count distinct catalog GROUPS captured (S2b: a generation is a group whose
 * object 0 is the independent base and objects 1..N are deltaUpdates). */
static int count_cat_groups(void)
{
    int n = 0;
    pthread_mutex_lock(&g_srv.mu);
    for (int i = 0; i < g_srv.cat_n; i++) {
        bool seen = false;
        for (int j = 0; j < i; j++)
            if (g_srv.cats[j].group == g_srv.cats[i].group) { seen = true; break; }
        if (!seen) n++;
    }
    pthread_mutex_unlock(&g_srv.mu);
    return n;
}

/* Poll until at least `want` distinct catalog groups (generations) captured. */
static int wait_cat_gens(int want, int ticks)
{
    for (int i = 0; i < ticks; i++) {
        if (count_cat_groups() >= want) return count_cat_groups();
        usleep(20000);
    }
    return count_cat_groups();
}

/* Reconstruct catalog GROUP `group` the way the receiver does: parse object 0
 * (the independent base) then apply objects 1..N (deltaUpdates) in order. On
 * success *out is owned (cleanup by caller). Returns false if the group's
 * object 0 was not captured. */
static bool reconstruct_group(uint64_t group, moq_msf_catalog_t *out)
{
    const moq_alloc_t *al = moq_alloc_default();
    memset(out, 0, sizeof(*out));
    /* Find the highest object id captured for the group. */
    long maxobj = -1;
    pthread_mutex_lock(&g_srv.mu);
    for (int i = 0; i < g_srv.cat_n; i++)
        if (g_srv.cats[i].group == group && (long)g_srv.cats[i].object > maxobj)
            maxobj = (long)g_srv.cats[i].object;
    pthread_mutex_unlock(&g_srv.mu);
    if (maxobj < 0) return false;

    bool have = false;
    for (long oid = 0; oid <= maxobj; oid++) {
        uint8_t buf[4096]; size_t len = 0;
        pthread_mutex_lock(&g_srv.mu);
        for (int i = 0; i < g_srv.cat_n; i++)
            if (g_srv.cats[i].group == group &&
                (long)g_srv.cats[i].object == oid) {
                len = g_srv.cats[i].len;
                if (len > sizeof(buf)) len = sizeof(buf);
                memcpy(buf, g_srv.cats[i].buf, len);
                break;
            }
        pthread_mutex_unlock(&g_srv.mu);
        if (!len) continue;
        if (oid == 0) {
            if (moq_msf_catalog_parse(al, (moq_bytes_t){ buf, len }, out) != MOQ_OK)
                return false;
            have = true;
        } else {
            moq_msf_catalog_t delta, next;
            if (moq_msf_catalog_parse(al, (moq_bytes_t){ buf, len }, &delta) != MOQ_OK)
                continue;
            if (moq_msf_catalog_apply_delta(al, out, &delta, &next) == MOQ_OK) {
                moq_msf_catalog_cleanup(al, out);
                *out = next;
            }
            moq_msf_catalog_cleanup(al, &delta);
        }
    }
    return have;
}

/* Reconstruct group `group`; report whether the resulting catalog declares
 * `name`. *object is reported as 0 (the generation's anchor object). */
static bool gen_has(uint64_t group, const char *name,
                    uint64_t *grp, uint64_t *object)
{
    if (grp) *grp = group;
    if (object) *object = 0;
    moq_msf_catalog_t c;
    if (!reconstruct_group(group, &c)) return false;
    bool found = find_track(&c, name) != NULL;
    moq_msf_catalog_cleanup(moq_alloc_default(), &c);
    return found;
}

/* Reconstruct group `group`; whether it is terminal (isComplete + empty). */
static bool gen_is_terminal(uint64_t group, uint64_t *grp, uint64_t *object)
{
    if (grp) *grp = group;
    if (object) *object = 0;
    moq_msf_catalog_t c;
    if (!reconstruct_group(group, &c)) return false;
    bool terminal = c.is_complete && c.track_count == 0;
    moq_msf_catalog_cleanup(moq_alloc_default(), &c);
    return terminal;
}

/* Reconstruct group `group`; whether `name` carries trackDuration == dur. */
static bool gen_track_duration_is(uint64_t group, const char *name, uint64_t dur)
{
    moq_msf_catalog_t c;
    if (!reconstruct_group(group, &c)) return false;
    const moq_msf_track_t *t = find_track(&c, name);
    bool ok = t && !t->is_live && t->has_track_duration &&
              t->track_duration_ms == dur;
    moq_msf_catalog_cleanup(moq_alloc_default(), &c);
    return ok;
}

static bool wait_ready(moq_media_sender_t *s, int ticks)
{
    for (int i = 0; i < ticks; i++) {
        if (moq_media_sender_is_ready(s)) return true;
        if (moq_media_sender_is_fatal(s)) return false;
        usleep(50000);
    }
    return moq_media_sender_is_ready(s);
}

static bool bytes_eq(moq_bytes_t b, const char *s)
{
    size_t n = strlen(s);
    return b.len == n && b.data && memcmp(b.data, s, n) == 0;
}

/* -- Demand-visibility (§7.2) callback capture ----------------------- *
 * The join/left callbacks fire on the sender's network thread; the test thread
 * observes via the seq-cst `joins`/`leaves` counters (a track pointer written
 * before the counter increment is visible once the increment is observed). */
typedef struct {
    atomic_int joins;
    atomic_int leaves;
    moq_media_track_t *last_join_track;
    atomic_int last_join_count;
    moq_media_track_t *last_left_track;
    atomic_int last_left_count;
} demand_state_t;
static demand_state_t g_demand;

static void demand_on_join(void *ctx, moq_media_sender_t *sender,
                           moq_media_track_t *track, size_t active)
{
    demand_state_t *d = (demand_state_t *)ctx;
    (void)sender;
    d->last_join_track = track;
    atomic_store(&d->last_join_count, (int)active);
    atomic_fetch_add(&d->joins, 1);
}

static void demand_on_left(void *ctx, moq_media_sender_t *sender,
                           moq_media_track_t *track, size_t active)
{
    demand_state_t *d = (demand_state_t *)ctx;
    (void)sender;
    d->last_left_track = track;
    atomic_store(&d->last_left_count, (int)active);
    atomic_fetch_add(&d->leaves, 1);
}

/* -- CMAF object/init builders for the validate_cmaf tests ----------- */

static void wr32be(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}
static size_t box_hdr_(uint8_t *p, uint32_t size, const char *type)
{
    wr32be(p, size); memcpy(p + 4, type, 4); return 8;
}
static size_t put_fb_u32(uint8_t *b, const char *type, uint32_t v)
{
    size_t p = box_hdr_(b, 16, type);
    wr32be(b + p, 0); p += 4;   /* version+flags */
    wr32be(b + p, v); p += 4;   /* payload word */
    return p;
}

/* One CMAF chunk: moof(mfhd traf(tfhd[track_id] tfdt trun[1 sample,flags])) + mdat. */
static size_t build_cmaf_obj(uint8_t *out, uint32_t track_id, uint32_t sflags)
{
    uint8_t traf_in[64]; size_t ti = 0;
    ti += put_fb_u32(traf_in + ti, "tfhd", track_id);
    ti += put_fb_u32(traf_in + ti, "tfdt", 0);
    ti += box_hdr_(traf_in + ti, 20, "trun");
    wr32be(traf_in + ti, 0x00000400); ti += 4;   /* sample-flags present */
    wr32be(traf_in + ti, 1); ti += 4;            /* 1 sample */
    wr32be(traf_in + ti, sflags); ti += 4;
    uint8_t traf[80]; size_t tp = 0;
    tp += box_hdr_(traf + tp, (uint32_t)(8 + ti), "traf");
    memcpy(traf + tp, traf_in, ti); tp += ti;
    uint8_t moofc[128]; size_t mc = 0;
    mc += put_fb_u32(moofc + mc, "mfhd", 1);
    memcpy(moofc + mc, traf, tp); mc += tp;
    size_t p = 0;
    p += box_hdr_(out + p, (uint32_t)(8 + mc), "moof");
    memcpy(out + p, moofc, mc); p += mc;
    p += box_hdr_(out + p, 9, "mdat"); out[p++] = 0xAA;
    return p;
}

/* Minimal AVC init segment whose tkhd carries track_id. */
static size_t build_cmaf_init(uint8_t *buf, uint32_t track_id)
{
    static const uint8_t avcc[] = { 0x01 };
    size_t p = 0;
    p += box_hdr_(buf + p, 20, "ftyp");
    memcpy(buf + p, "isom", 4); p += 4; wr32be(buf + p, 0); p += 4;
    memcpy(buf + p, "isom", 4); p += 4;

    size_t avcc_box = 8 + sizeof(avcc);
    size_t avc1 = 8 + 78 + avcc_box;
    size_t stsd = 8 + 8 + avc1;
    size_t stbl = 8 + stsd;
    size_t minf = 8 + stbl;
    size_t mdhd = 8 + 4 + 12;
    size_t mdia = 8 + mdhd + minf;
    size_t tkhd = 24;
    size_t trak = 8 + tkhd + mdia;
    size_t moov = 8 + trak;

    p += box_hdr_(buf + p, (uint32_t)moov, "moov");
    p += box_hdr_(buf + p, (uint32_t)trak, "trak");
    p += box_hdr_(buf + p, (uint32_t)tkhd, "tkhd");
    wr32be(buf + p, 0); p += 4;            /* version+flags */
    wr32be(buf + p, 0); p += 4;            /* creation */
    wr32be(buf + p, 0); p += 4;            /* modification */
    wr32be(buf + p, track_id); p += 4;     /* track_ID */
    p += box_hdr_(buf + p, (uint32_t)mdia, "mdia");
    p += box_hdr_(buf + p, (uint32_t)mdhd, "mdhd");
    wr32be(buf + p, 0); p += 4; wr32be(buf + p, 0); p += 4;
    wr32be(buf + p, 0); p += 4; wr32be(buf + p, 90000); p += 4;
    p += box_hdr_(buf + p, (uint32_t)minf, "minf");
    p += box_hdr_(buf + p, (uint32_t)stbl, "stbl");
    p += box_hdr_(buf + p, (uint32_t)stsd, "stsd");
    wr32be(buf + p, 0); p += 4; wr32be(buf + p, 1); p += 4;
    p += box_hdr_(buf + p, (uint32_t)avc1, "avc1");
    memset(buf + p, 0, 78); p += 78;
    p += box_hdr_(buf + p, (uint32_t)avcc_box, "avcC");
    memcpy(buf + p, avcc, sizeof(avcc)); p += sizeof(avcc);
    return p;
}

static moq_rcbuf_t *mkbuf_bytes(const uint8_t *d, size_t n)
{
    moq_rcbuf_t *b = NULL;
    moq_rcbuf_create(moq_alloc_default(), d, n, &b);
    return b;
}

/* Create a pre-ready CMAF sender, add a CMAF track (optionally with an init
 * segment carrying init_tid), write one object, return the write result. */
static moq_result_t try_cmaf_write(bool validate, bool with_init,
                                   uint32_t init_tid, uint32_t obj_tid,
                                   uint32_t obj_sflags, bool starts_group,
                                   bool malformed)
{
    char url[64];
    moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);  /* dead port; pre-ready */
    moq_bytes_t parts[2];
    moq_media_sender_cfg_t cfg;
    fill_cfg(&cfg, parts);
    cfg.endpoint = &ec;
    cfg.validate_cmaf = validate;
    moq_media_sender_t *s = NULL;
    if (moq_media_sender_create(&cfg, &s) != MOQ_OK) return MOQ_ERR_INTERNAL;

    uint8_t initbuf[256];
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
    tc.codec = MOQ_BYTES_LITERAL("avc1.640028");
    tc.bitrate = 1500000;             /* MSF-01 5.2.22: required for video */
    tc.timescale = 90000;
    if (with_init)
        tc.init_data = (moq_bytes_t){ initbuf, build_cmaf_init(initbuf, init_tid) };
    moq_media_track_t *v = NULL;
    moq_media_sender_add_track(s, &tc, &v);

    moq_rcbuf_t *b;
    if (malformed) {
        b = mkbuf(16, 0xEE);   /* not a moof+mdat object */
    } else {
        uint8_t objbuf[256];
        size_t objlen = build_cmaf_obj(objbuf, obj_tid, obj_sflags);
        b = mkbuf_bytes(objbuf, objlen);
    }
    moq_media_send_object_t o = mkobj(b, true, starts_group, false);
    moq_result_t rc = moq_media_sender_write(s, v, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(b);   /* not queued: caller still owns */

    moq_media_sender_destroy(s);
    return rc;
}

/* Like try_cmaf_write but leaves cfg.validate_cmaf at its cfg_init DEFAULT
 * (no explicit set) -- exercises the default policy. set_passthrough opts out
 * explicitly (validate_cmaf=false). */
static moq_result_t cmaf_write_default_cfg(bool set_passthrough, bool malformed)
{
    char url[64];
    moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);  /* dead port; pre-ready */
    moq_bytes_t parts[2];
    moq_media_sender_cfg_t cfg;
    fill_cfg(&cfg, parts);             /* cfg_init default: validate_cmaf = true */
    if (set_passthrough) cfg.validate_cmaf = false;       /* explicit opt-out */
    cfg.endpoint = &ec;
    moq_media_sender_t *s = NULL;
    if (moq_media_sender_create(&cfg, &s) != MOQ_OK) return MOQ_ERR_INTERNAL;

    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
    tc.codec = MOQ_BYTES_LITERAL("avc1.640028");
    tc.bitrate = 1500000;
    tc.timescale = 90000;
    moq_media_track_t *v = NULL;
    moq_media_sender_add_track(s, &tc, &v);

    moq_rcbuf_t *b;
    if (malformed) {
        b = mkbuf(16, 0xEE);   /* not a moof+mdat object */
    } else {
        uint8_t objbuf[256];
        size_t objlen = build_cmaf_obj(objbuf, 1, 0x02000000);  /* valid sync */
        b = mkbuf_bytes(objbuf, objlen);
    }
    moq_media_send_object_t o = mkobj(b, true, true, false);
    moq_result_t rc = moq_media_sender_write(s, v, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(b);   /* not queued: caller still owns */
    moq_media_sender_destroy(s);
    return rc;
}

/* Write one structurally-valid CMAF object with an app-declared SAP type
 * (CMSF §3.6.1) and return the write result. */
static moq_result_t try_cmaf_write_sap(bool validate, bool starts_group,
                                       bool has_sap, moq_sap_type_t sap)
{
    char url[64];
    moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);  /* dead port; pre-ready */
    moq_bytes_t parts[2];
    moq_media_sender_cfg_t cfg;
    fill_cfg(&cfg, parts);
    cfg.endpoint = &ec;
    cfg.validate_cmaf = validate;
    moq_media_sender_t *s = NULL;
    if (moq_media_sender_create(&cfg, &s) != MOQ_OK) return MOQ_ERR_INTERNAL;

    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = MOQ_BYTES_LITERAL("v");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
    tc.codec = MOQ_BYTES_LITERAL("avc1.640028");
    tc.bitrate = 1500000;             /* MSF-01 5.2.22: required for video */
    tc.timescale = 90000;
    moq_media_track_t *v = NULL;
    moq_media_sender_add_track(s, &tc, &v);

    uint8_t objbuf[256];
    size_t objlen = build_cmaf_obj(objbuf, 0, 0x02000000);  /* valid sync object */
    moq_rcbuf_t *b = mkbuf_bytes(objbuf, objlen);
    moq_media_send_object_t o = mkobj(b, true, starts_group, false);
    o.has_sap_type = has_sap;
    o.sap_type = sap;
    moq_result_t rc = moq_media_sender_write(s, v, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(b);

    moq_media_sender_destroy(s);
    return rc;
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: --cert <pem> --key <pem>\n");
        return 1;
    }

    /* == RAW LOC timestamp range validation (no network) ===============
     * A capture/presentation timestamp beyond the QUIC varint range cannot be
     * LOC-encoded; moq_media_sender_write() must reject it SYNCHRONOUSLY with
     * MOQ_ERR_INVAL -- before enqueue / payload-ownership transfer -- rather
     * than queue it and fatal the sender at drain time. */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 9);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);   /* RAW packaging -> LOC timestamp is encoded */

        /* (1) Explicit capture_time_us out of range -> INVAL, no fatal, and the
         *     payload is NOT taken: the caller still owns it and decrefs below
         *     (a wrongful ownership transfer would surface as a double-free
         *     under a sanitizer build). */
        moq_rcbuf_t *b1 = mkbuf(8, 1);
        moq_media_send_object_t o1 = mkobj(b1, true, true, false);
        o1.has_capture_time = true;
        o1.capture_time_us = MOQ_QUIC_VARINT_MAX + 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o1),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
        moq_rcbuf_decref(b1);     /* ownership not transferred -> caller frees */

        /* (2) Fallback presentation_time_us out of range (has_capture_time
         *     false) is the same LOC sink -> also INVAL, no fatal. */
        moq_rcbuf_t *b2 = mkbuf(8, 2);
        moq_media_send_object_t o2 = mkobj(b2, true, true, false);
        o2.presentation_time_us = MOQ_QUIC_VARINT_MAX + 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o2),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
        moq_rcbuf_decref(b2);

        /* (3) Positive boundary: exactly MOQ_QUIC_VARINT_MAX is encodable and
         *     must be accepted (queued -> MOQ_OK with ownership taken, or
         *     WOULD_BLOCK if the pre-ready buffer is full -- never INVAL). */
        moq_rcbuf_t *b3 = mkbuf(8, 3);
        moq_media_send_object_t o3 = mkobj(b3, true, true, false);
        o3.has_capture_time = true;
        o3.capture_time_us = MOQ_QUIC_VARINT_MAX;
        moq_result_t rc3 = moq_media_sender_write(s, v, &o3);
        MOQ_TEST_CHECK(rc3 == MOQ_OK || rc3 == MOQ_ERR_WOULD_BLOCK);
        if (rc3 != MOQ_OK) moq_rcbuf_decref(b3);   /* own it back unless taken */
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        moq_media_sender_destroy(s);
    }

    /* == cfg validation (no network) =================================== */
    {
        moq_media_sender_t *s = (moq_media_sender_t *)(uintptr_t)0x1;
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;

        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(NULL, &s),
                              (int)MOQ_ERR_INVAL);
        fill_cfg(&cfg, parts);
        cfg.struct_size = 4;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_ERR_INVAL);

        /* Missing namespace. */
        fill_cfg(&cfg, parts);
        cfg.namespace_.count = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_ERR_INVAL);

        /* Incoherent span. */
        fill_cfg(&cfg, parts);
        parts[1] = (moq_bytes_t){ NULL, 4 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_ERR_INVAL);

        /* Backpressure is a forced choice: plain init leaves it UNSET. */
        moq_media_sender_cfg_init(&cfg);
        MOQ_TEST_CHECK_EQ_INT((int)cfg.backpressure,
                              (int)MOQ_MEDIA_SEND_BP_UNSET);
        parts[0] = MOQ_BYTES_LITERAL("svc");
        parts[1] = MOQ_BYTES_LITERAL("demo");
        cfg.namespace_ = (moq_namespace_t){ parts, 2 };
        cfg.endpoint = (const moq_endpoint_cfg_t *)(uintptr_t)0x1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_ERR_INVAL);

        /* Presets set policies. */
        moq_media_sender_cfg_init_live(&cfg);
        MOQ_TEST_CHECK_EQ_INT((int)cfg.backpressure,
                              (int)MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME);
        moq_media_sender_cfg_init_lossless(&cfg);
        MOQ_TEST_CHECK_EQ_INT((int)cfg.backpressure,
                              (int)MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT);
        MOQ_TEST_CHECK(cfg.block_timeout_us > 0);

        /* create requires an endpoint cfg; attach forbids one. */
        fill_cfg(&cfg, parts);
        cfg.endpoint = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(s == NULL);
    }

    /* == END_OF_TRACK marker is non-evictable under drop backpressure ==== *
     * Pre-ready, DROP_TO_KEYFRAME, cap 1: end_track(v) queues the only entry
     * (the terminal marker). A sync write on another track must NOT evict it:
     * the write is refused (WOULD_BLOCK), nothing is dropped, and the ended
     * track stays ended. (Server-free deterministic test -- runs even when the
     * later network phases cannot bind UDP locally.) */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 9);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);                 /* DROP_TO_KEYFRAME */
        cfg.endpoint = &ec;
        cfg.pre_ready_max_objects = 1;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL, *a = NULL;
        add_video_track(s, &v);
        add_audio_track(s, &a);

        /* Queue the terminal marker for v (the only queue entry). */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v),
                              (int)MOQ_OK);

        /* A sync object on a must not make room by evicting v's terminal. */
        moq_rcbuf_t *b = mkbuf(8, 1);
        moq_media_send_object_t o = mkobj(b, true, true, false); /* keyframe */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, a, &o),
                              (int)MOQ_ERR_WOULD_BLOCK);
        moq_rcbuf_decref(b);                    /* retained on WOULD_BLOCK */

        moq_media_sender_stats_t st;
        (void)moq_media_sender_get_stats(s, &st, sizeof(st));
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0); /* terminal NOT dropped */
        MOQ_TEST_CHECK_EQ_U64(st.groups_dropped, 0);

        /* The ended track stays ended: idempotent end, writes refused. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v),
                              (int)MOQ_OK);
        moq_rcbuf_t *b2 = mkbuf(8, 2);
        moq_media_send_object_t o2 = mkobj(b2, true, true, false);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o2),
                              (int)MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(b2);

        moq_media_sender_destroy(s);
    }

    /* == drops still evict real media around a queued terminal (cap 2) ==== *
     * Anti-over-broad guard: a terminal marker plus an evictable media group
     * are queued; a later anchored write evicts the MEDIA group (a real drop)
     * and succeeds, while the terminal marker is preserved. This proves the fix
     * did not "disable all drops whenever a terminal exists". */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 9);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);                 /* DROP_TO_KEYFRAME */
        cfg.endpoint = &ec;
        cfg.pre_ready_max_objects = 2;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL, *a = NULL;
        add_video_track(s, &v);
        add_audio_track(s, &a);

        /* Terminal on v, then a sync group on a -> queue full (2/2). */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v),
                              (int)MOQ_OK);
        moq_rcbuf_t *b = mkbuf(8, 1);
        moq_media_send_object_t o = mkobj(b, true, true, true);  /* group 0 */
        moq_result_t rc = moq_media_sender_write(s, a, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);

        /* A second anchored object on a (new group) re-anchors a: it evicts a's
         * older group (a real media drop), succeeds, and preserves the terminal. */
        moq_rcbuf_t *b2 = mkbuf(8, 2);
        moq_media_send_object_t o2 = mkobj(b2, true, true, true); /* group 1 */
        rc = moq_media_sender_write(s, a, &o2);
        if (rc != MOQ_OK) moq_rcbuf_decref(b2);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);  /* media was evictable */

        moq_media_sender_stats_t st;
        (void)moq_media_sender_get_stats(s, &st, sizeof(st));
        MOQ_TEST_CHECK(st.objects_dropped >= 1);      /* a's old group dropped */

        /* v stays ended (terminal preserved): idempotent end. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v),
                              (int)MOQ_OK);
        moq_media_sender_destroy(s);
    }

    /* == attach/create ownership + stop gating ========================= */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;

        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_endpoint_t *ep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&ec, &ep),
                              (int)MOQ_OK);

        /* attach forbids an endpoint cfg (structural "ignored"). */
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(ep, &cfg, &s),
                              (int)MOQ_ERR_INVAL);

        fill_cfg(&cfg, parts);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(ep, &cfg, &s),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(s != NULL);

        /* One sender per endpoint. */
        moq_media_sender_t *s2 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_attach(ep, &cfg, &s2),
                              (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(s2 == NULL);

        /* Endpoint stop is blocked while a sender is attached. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep),
                              (int)MOQ_ERR_WRONG_STATE);

        moq_media_sender_destroy(s);

        /* After destroy the attachment is gone; stop succeeds. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(ep), (int)MOQ_OK);
        moq_endpoint_destroy(ep);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == pre-ready queue: keyframe + deltas, no silent drop ============ */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);

        /* A keyframe then two deltas, before ready: all accepted. */
        moq_rcbuf_t *b;
        moq_media_send_object_t o;
        b = mkbuf(16, 0); o = mkobj(b, true, true, false);
        moq_result_t rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        b = mkbuf(16, 1); o = mkobj(b, false, false, false);
        rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        b = mkbuf(16, 2); o = mkobj(b, false, false, true);
        rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);

        moq_media_sender_destroy(s);   /* queued refs released cleanly */
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == pre-ready overflow: a single GOP over the bound -> WOULD_BLOCK = *
     * No network needed: connect to a silent UDP port so the sender never
     * reaches ready, keep writing one big GOP (keyframe + deltas) into a
     * tiny pre-ready bound. The keyframe must stay (sync anchor), so once
     * the bound is hit a non-anchor delta cannot evict it -> WOULD_BLOCK
     * with the caller still owning the buffer. */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 9);  /* discard */
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);          /* DROP_TO_KEYFRAME */
        cfg.endpoint = &ec;
        cfg.pre_ready_max_objects = 3;  /* keyframe + 2 deltas fit; 4th not */
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);

        moq_rcbuf_t *b; moq_media_send_object_t o; moq_result_t rc;
        b = mkbuf(8, 0); o = mkobj(b, true, true, false);   /* keyframe */
        rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        for (int i = 1; i <= 2; i++) {                       /* 2 deltas */
            b = mkbuf(8, (uint8_t)i); o = mkobj(b, false, false, false);
            rc = moq_media_sender_write(s, v, &o);
            if (rc != MOQ_OK) moq_rcbuf_decref(b);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }
        /* The 4th object (a delta of the same GOP) cannot fit without
         * evicting the keyframe -> WOULD_BLOCK, ownership retained. */
        b = mkbuf(8, 3); o = mkobj(b, false, false, false);
        rc = moq_media_sender_write(s, v, &o);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
        moq_rcbuf_decref(b);            /* caller still owns it */

        moq_media_sender_destroy(s);
    }

    /* == pre-ready drop: a new keyframe re-anchors, old GOP evicted ===== */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 9);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);          /* DROP_TO_KEYFRAME */
        cfg.endpoint = &ec;
        cfg.pre_ready_max_objects = 2;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);

        moq_rcbuf_t *b; moq_media_send_object_t o; moq_result_t rc;
        b = mkbuf(8, 0); o = mkobj(b, true, true, true);     /* GOP 0 key */
        rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        b = mkbuf(8, 1); o = mkobj(b, true, true, true);     /* GOP 1 key */
        rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        /* A third keyframe re-anchors; the oldest GOP is evicted, fits. */
        b = mkbuf(8, 2); o = mkobj(b, true, true, true);     /* GOP 2 key */
        rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);

        moq_media_sender_destroy(s);
    }

    /* == bad inputs: malformed span, leading non-sync, foreign track ==== */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 9);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);

        /* add_track with an incoherent optional span -> INVAL, no crash. */
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("v");
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ NULL, 4 };   /* len>0, data==NULL */
        moq_media_track_t *bad = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &bad),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(bad == NULL);

        moq_media_track_t *v = NULL;
        add_video_track(s, &v);

        /* A leading delta (no sync point yet) cannot anchor the stream:
         * WOULD_BLOCK, ownership retained. */
        moq_rcbuf_t *b = mkbuf(8, 0);
        moq_media_send_object_t o = mkobj(b, false, false, false);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o),
                              (int)MOQ_ERR_WOULD_BLOCK);
        moq_rcbuf_decref(b);

        /* Queue a real keyframe on v: now the sender has an anchor. */
        b = mkbuf(8, 1); o = mkobj(b, true, true, false);
        moq_result_t rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);

        /* Per-track anchor: a SECOND track's first object being a delta is
         * refused even though v has a keyframe queued -- the anchor is per
         * track, not sender-global. */
        moq_media_track_t *a = NULL;
        add_audio_track(s, &a);
        b = mkbuf(8, 2); o = mkobj(b, false, false, false);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, a, &o),
                              (int)MOQ_ERR_WOULD_BLOCK);
        moq_rcbuf_decref(b);
        /* a's own keyframe anchors it -> accepted. */
        b = mkbuf(8, 3); o = mkobj(b, true, true, false);
        rc = moq_media_sender_write(s, a, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);

        /* §7.4: app properties on a RAW track -> INVAL (the service owns
         * the RAW/LOC block in v0), ownership retained. */
        moq_rcbuf_t *xp = mkbuf(4, 0xAA);
        moq_rcbuf_t *pl = mkbuf(8, 4);
        o = mkobj(pl, true, true, false);
        o.properties = xp;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o),
                              (int)MOQ_ERR_INVAL);
        moq_rcbuf_decref(pl);          /* both refs retained on INVAL */
        moq_rcbuf_decref(xp);

        /* A track owned by a different sender -> INVAL (no deref of a
         * foreign/opaque handle). */
        char url2[64];
        moq_endpoint_cfg_t ec2 = ep_cfg(url2, sizeof(url2), 9);
        moq_bytes_t parts2[2];
        moq_media_sender_cfg_t cfg2;
        fill_cfg(&cfg2, parts2);
        cfg2.endpoint = &ec2;
        moq_media_sender_t *s2 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg2, &s2),
                              (int)MOQ_OK);
        moq_media_track_t *v2 = NULL;
        add_video_track(s2, &v2);
        b = mkbuf(8, 0); o = mkobj(b, true, true, true);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v2, &o),
                              (int)MOQ_ERR_INVAL);   /* v2 belongs to s2 */
        moq_rcbuf_decref(b);

        moq_media_sender_destroy(s2);
        moq_media_sender_destroy(s);
    }

    /* == CMSF §4.2: protected CMAF track requires protected init ========= *
     * A CMAF track that declares contentProtectionRefIDs is CENC-encrypted, so
     * add_track requires a non-empty init carrying the CENC boxes
     * (sinf/schm/schi/tenc). Clear or missing init is rejected -- always-on
     * catalog coherence. The reject paths must free the partial track. */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 9);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_bytes_t kids[] = { MOQ_BYTES_LITERAL("01234567-89ab-cdef-0123-456789abcdef") };
        moq_cmsf_content_protection_t cp;
        memset(&cp, 0, sizeof(cp));
        cp.ref_id = MOQ_BYTES_LITERAL("cp1");
        cp.default_kids = kids;
        cp.default_kid_count = 1;
        cp.scheme = MOQ_BYTES_LITERAL("cbcs");
        cp.drm_system.system_id = MOQ_BYTES_LITERAL("9a04f079-9840-4286-ab92-e65be0885f95");
        cfg.content_protections = &cp;
        cfg.content_protection_count = 1;

        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);

        moq_bytes_t refs[] = { MOQ_BYTES_LITERAL("cp1") };

        /* Protected CMAF track + protected (CENC) init -> OK. */
        {
            const uint8_t avcc[] = { 0x01 };
            const uint8_t kid[16] = { 0 };
            uint8_t initbuf[1024];
            size_t initlen = moq_test_build_cmaf_init_cenc(
                initbuf, 1, 90000, 320, 240, avcc, sizeof(avcc),
                "cbcs", 1, 8, kid, 0, MOQ_TEST_CENC_OK);
            moq_media_track_cfg_t tc;
            moq_media_track_cfg_init(&tc);
            tc.name = MOQ_BYTES_LITERAL("enc");
            tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
            tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
            tc.codec = MOQ_BYTES_LITERAL("avc1.640028");
            tc.bitrate = 1500000;             /* MSF-01 5.2.22 */
            tc.init_data = (moq_bytes_t){ initbuf, initlen };
            tc.content_protection_ref_ids = refs;
            tc.content_protection_ref_id_count = 1;
            moq_media_track_t *t = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK(t != NULL);
        }

        /* Protected CMAF track + clear (unencrypted) init -> INVAL. */
        {
            uint8_t initbuf[256];
            size_t initlen = build_cmaf_init(initbuf, 1);
            moq_media_track_cfg_t tc;
            moq_media_track_cfg_init(&tc);
            tc.name = MOQ_BYTES_LITERAL("enc-clear");
            tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
            tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
            tc.codec = MOQ_BYTES_LITERAL("avc1.640028");
            tc.bitrate = 1500000;             /* MSF-01 5.2.22 */
            tc.init_data = (moq_bytes_t){ initbuf, initlen };
            tc.content_protection_ref_ids = refs;
            tc.content_protection_ref_id_count = 1;
            moq_media_track_t *t = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(t == NULL);
        }

        /* Protected CMAF track + no init -> INVAL. */
        {
            moq_media_track_cfg_t tc;
            moq_media_track_cfg_init(&tc);
            tc.name = MOQ_BYTES_LITERAL("enc-noinit");
            tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
            tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
            tc.codec = MOQ_BYTES_LITERAL("avc1.640028");
            tc.bitrate = 1500000;             /* MSF-01 5.2.22 */
            tc.content_protection_ref_ids = refs;
            tc.content_protection_ref_id_count = 1;
            moq_media_track_t *t = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(t == NULL);
        }

        /* Regression: unprotected clear CMAF track (no refs) -> OK. */
        {
            uint8_t initbuf[256];
            size_t initlen = build_cmaf_init(initbuf, 2);
            moq_media_track_cfg_t tc;
            moq_media_track_cfg_init(&tc);
            tc.name = MOQ_BYTES_LITERAL("clear");
            tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
            tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
            tc.codec = MOQ_BYTES_LITERAL("avc1.640028");
            tc.bitrate = 1500000;             /* MSF-01 5.2.22 */
            tc.init_data = (moq_bytes_t){ initbuf, initlen };
            moq_media_track_t *t = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK(t != NULL);
        }

        moq_media_sender_destroy(s);
    }

    /* == end_track: refuses later writes, idempotent, per-track ========= *
     * Pre-ready (no drain needed): the contract is local state. */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 20);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL, *a = NULL;
        add_video_track(s, &v);
        add_audio_track(s, &a);

        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v),
                              (int)MOQ_OK);
        /* Write after end -> WRONG_STATE (no ownership taken). */
        moq_rcbuf_t *b = mkbuf(8, 0);
        moq_media_send_object_t o = mkobj(b, true, true, false);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o),
                              (int)MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(b);
        /* Repeated end_track is idempotent. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v),
                              (int)MOQ_OK);
        /* Other track unaffected: still writable, then independently endable. */
        b = mkbuf(8, 1); o = mkobj(b, true, true, false);
        moq_result_t rc = moq_media_sender_write(s, a, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, a),
                              (int)MOQ_OK);

        moq_media_sender_destroy(s);
    }

    /* == backpressure policies (deterministic, pre-ready: no drain) ===== *
     * Before ready nothing drains, so the bounded queue fills exactly and
     * the policy decision is deterministic. */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 9);
        moq_bytes_t parts[2];

        /* RETURN_WOULD_BLOCK: a full queue refuses immediately, no drop. */
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK;
        cfg.endpoint = &ec;
        cfg.pre_ready_max_objects = 2;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);
        moq_rcbuf_t *b; moq_media_send_object_t o; moq_result_t rc;
        b = mkbuf(8, 0); o = mkobj(b, true, true, false);   /* keyframe */
        rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        b = mkbuf(8, 1); o = mkobj(b, false, false, false); /* fills to 2 */
        rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        b = mkbuf(8, 2); o = mkobj(b, false, false, false); /* full */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o),
                              (int)MOQ_ERR_WOULD_BLOCK);
        moq_rcbuf_decref(b);
        moq_media_sender_stats_t st;
        (void)moq_media_sender_get_stats(s, &st, sizeof(st));
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);   /* refused, not dropped */
        MOQ_TEST_CHECK_EQ_INT((int)st.last_error, (int)MOQ_ERR_WOULD_BLOCK);

        /* get_stats output-size ABI contract (same as the receiver outputs). */
        moq_media_sender_stats_t st2;
        /* too-small -> INVAL, no write. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_get_stats(s, &st2, 8), (int)MOQ_ERR_INVAL);
        /* at/above the v0 floor but below sizeof (trailing padding after
         * last_error): served + clamped, proving the floor is the frozen v0
         * base, not sizeof(current). */
        memset(&st2, 0, sizeof(st2));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_get_stats(s, &st2, sizeof(st2) - 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)st2.struct_size, (int)(sizeof(st2) - 1));
        /* larger buffer: clamp to the library struct, leave the canary tail. */
        struct { moq_media_sender_stats_t s; unsigned char tail[16]; } big;
        memset(&big, 0xAB, sizeof(big));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_get_stats(s, &big.s, sizeof(big)),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)big.s.struct_size,
                              (int)sizeof(moq_media_sender_stats_t));
        bool tail_ok = true;
        for (int k = 0; k < 16; k++) if (big.tail[k] != 0xAB) tail_ok = false;
        MOQ_TEST_CHECK(tail_ok);

        moq_media_sender_destroy(s);

        /* BLOCK_TIMEOUT: with no drain, a full queue blocks ~timeout then
         * WOULD_BLOCK (never a silent drop). Short timeout for the test. */
        fill_cfg(&cfg, parts);
        cfg.backpressure = MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT;
        cfg.block_timeout_us = 150000;   /* 150 ms */
        cfg.endpoint = &ec;
        cfg.pre_ready_max_objects = 1;
        s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        v = NULL; add_video_track(s, &v);
        b = mkbuf(8, 0); o = mkobj(b, true, true, false);
        rc = moq_media_sender_write(s, v, &o);
        if (rc != MOQ_OK) moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        b = mkbuf(8, 1); o = mkobj(b, false, false, false);
        rc = moq_media_sender_write(s, v, &o);   /* blocks then times out */
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
        moq_rcbuf_decref(b);
        (void)moq_media_sender_get_stats(s, &st, sizeof(st));
        MOQ_TEST_CHECK(st.backpressure_stalls >= 1);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);
        moq_media_sender_destroy(s);

        /* DROP_GROUP: a full queue evicts the oldest whole group, counted. */
        fill_cfg(&cfg, parts);
        cfg.backpressure = MOQ_MEDIA_SEND_BP_DROP_GROUP;
        cfg.endpoint = &ec;
        cfg.pre_ready_max_objects = 2;
        s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        v = NULL; add_video_track(s, &v);
        for (int i = 0; i < 3; i++) {   /* 3 single-object GOPs, bound 2 */
            b = mkbuf(8, (uint8_t)i); o = mkobj(b, true, true, true);
            rc = moq_media_sender_write(s, v, &o);
            if (rc != MOQ_OK) moq_rcbuf_decref(b);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }
        (void)moq_media_sender_get_stats(s, &st, sizeof(st));
        MOQ_TEST_CHECK(st.objects_dropped >= 1);
        MOQ_TEST_CHECK(st.groups_dropped >= 1);
        MOQ_TEST_CHECK(st.keyframes_dropped >= 1);
        moq_media_sender_destroy(s);
    }

    /* == live loopback: write path delivers objects + LOC round-trip === *
     * Pre-ready writes (one GOP, three objects) drain to the subscribing
     * server once ready; verify group/object IDs, end-of-group, keyframe
     * marking, and the LOC timestamp the sender generated. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);

        /* One GOP written before ready: keyframe + two deltas, with
         * ascending presentation timestamps. */
        for (int i = 0; i < 3; i++) {
            moq_rcbuf_t *b = mkbuf(16, (uint8_t)i);
            moq_media_send_object_t o = mkobj(b, i == 0, i == 0, i == 2);
            o.presentation_time_us = (uint64_t)(i + 1) * 1000u;
            moq_result_t rc = moq_media_sender_write(s, v, &o);
            if (rc != MOQ_OK) moq_rcbuf_decref(b);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }

        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
        MOQ_TEST_CHECK(atomic_load(&g_srv.ns_accepted));

        /* add_track after ready is now legal (S1): it registers the track and
         * triggers a catalog republish. (Dedicated catalog-generation tests
         * below assert the republish placement/content.) */
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("late");
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("opus");
        tc.bitrate = 32000;               /* MSF-01 5.2.22 */
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *late = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &late),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(late != NULL);

        /* A post-ready object also flows. */
        {
            moq_rcbuf_t *b = mkbuf(16, 9);
            moq_media_send_object_t o = mkobj(b, true, true, true);
            o.presentation_time_us = 9000u;
            moq_result_t rc = moq_media_sender_write(s, v, &o);
            if (rc != MOQ_OK) moq_rcbuf_decref(b);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }

        /* The server should receive all four objects. */
        for (int i = 0; i < 200 && srv_count(&g_srv) < 4; i++) usleep(50000);
        MOQ_TEST_CHECK(srv_count(&g_srv) >= 4);

        pthread_mutex_lock(&g_srv.mu);
        /* GOP 0: objects 0,1,2 in order; obj0 keyframe, end_of_group on 2. */
        MOQ_TEST_CHECK_EQ_U64(g_srv.objs[0].group, 0);
        MOQ_TEST_CHECK_EQ_U64(g_srv.objs[0].object, 0);
        MOQ_TEST_CHECK(g_srv.objs[0].keyframe);
        MOQ_TEST_CHECK(g_srv.objs[0].has_ts);
        MOQ_TEST_CHECK_EQ_U64(g_srv.objs[0].ts, 1000);
        MOQ_TEST_CHECK_EQ_U64(g_srv.objs[0].first_byte, 0);
        MOQ_TEST_CHECK_EQ_U64(g_srv.objs[1].object, 1);
        MOQ_TEST_CHECK(!g_srv.objs[1].keyframe);
        MOQ_TEST_CHECK_EQ_U64(g_srv.objs[1].ts, 2000);
        MOQ_TEST_CHECK_EQ_U64(g_srv.objs[2].object, 2);
        MOQ_TEST_CHECK(g_srv.objs[2].end_of_group);
        /* Second GOP starts a new group at object 0. */
        MOQ_TEST_CHECK_EQ_U64(g_srv.objs[3].group, 1);
        MOQ_TEST_CHECK_EQ_U64(g_srv.objs[3].object, 0);
        MOQ_TEST_CHECK(g_srv.objs[3].keyframe);
        pthread_mutex_unlock(&g_srv.mu);

        /* Stats reflect the four written + sent objects. */
        moq_media_sender_stats_t st;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_get_stats(s, &st, sizeof(st)),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(st.objects_written, 4);
        MOQ_TEST_CHECK(st.objects_sent >= 4);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);

        /* A delta whose keyframe has already DRAINED off the ring is still
         * accepted (per-track anchored state, not just queued anchor):
         * write a fresh keyframe, wait for it to drain, then a same-group
         * delta -- it must enqueue, not WOULD_BLOCK. */
        {
            moq_rcbuf_t *kb = mkbuf(16, 20);
            moq_media_send_object_t ko = mkobj(kb, true, true, false);
            ko.presentation_time_us = 20000u;
            moq_result_t krc = moq_media_sender_write(s, v, &ko);
            if (krc != MOQ_OK) moq_rcbuf_decref(kb);
            MOQ_TEST_CHECK_EQ_INT((int)krc, (int)MOQ_OK);
            /* Wait until the keyframe has drained off the queue. */
            for (int i = 0; i < 200; i++) {
                (void)moq_media_sender_get_stats(s, &st, sizeof(st));
                if (st.objects_queued == 0) break;
                usleep(50000);
            }
            MOQ_TEST_CHECK_EQ_U64(st.objects_queued, 0);
            moq_rcbuf_t *db = mkbuf(16, 21);
            moq_media_send_object_t dobj = mkobj(db, false, false, true);
            dobj.presentation_time_us = 21000u;
            moq_result_t drc = moq_media_sender_write(s, v, &dobj);
            if (drc != MOQ_OK) moq_rcbuf_decref(db);
            MOQ_TEST_CHECK_EQ_INT((int)drc, (int)MOQ_OK);  /* not WOULD_BLOCK */
        }

        /* LOC-01 2.3.1.1: an explicit Capture Timestamp is wall-clock us since
         * the Unix epoch. When the app supplies has_capture_time, the sender
         * emits capture_time_us VERBATIM as the LOC timestamp -- not
         * presentation_time_us, and with no truncation of a large epoch value.
         * (The legacy fallback -- ts == pts when has_capture_time is false -- is
         * already proven by objs[0..2] above: ts 1000/2000 == their pts.) */
        {
            const uint64_t kCaptureEpochUs = 1746104600000000ULL;  /* large, valid */
            moq_rcbuf_t *cb = mkbuf(16, 22);
            moq_media_send_object_t co = mkobj(cb, true, true, true);
            co.presentation_time_us = 30000u;        /* differs from capture time */
            co.has_capture_time = true;
            co.capture_time_us = kCaptureEpochUs;
            moq_result_t crc = moq_media_sender_write(s, v, &co);
            if (crc != MOQ_OK) moq_rcbuf_decref(cb);
            MOQ_TEST_CHECK_EQ_INT((int)crc, (int)MOQ_OK);

            /* Find the delivered object by its payload marker (robust against the
             * exact arrival index of the preceding writes). */
            int idx = -1;
            for (int i = 0; i < 200 && idx < 0; i++) {
                int cnt = srv_count(&g_srv);
                pthread_mutex_lock(&g_srv.mu);
                for (int j = 0; j < cnt; j++)
                    if (g_srv.objs[j].first_byte == 22) { idx = j; break; }
                pthread_mutex_unlock(&g_srv.mu);
                if (idx < 0) usleep(50000);
            }
            MOQ_TEST_CHECK(idx >= 0);
            if (idx >= 0) {
                pthread_mutex_lock(&g_srv.mu);
                MOQ_TEST_CHECK(g_srv.objs[idx].has_ts);
                MOQ_TEST_CHECK_EQ_U64(g_srv.objs[idx].ts, kCaptureEpochUs);
                pthread_mutex_unlock(&g_srv.mu);
            }
        }

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == live loopback: public end_track() drains to a real peer ========= *
     * Drive the PUBLIC service API moq_media_sender_end_track() (not the
     * lower publisher facade) and prove it drains through media_sender.c to a
     * real subscribing peer as a reliable END_OF_TRACK on "v", while "a"
     * keeps flowing and the session stays alive. The END_OF_TRACK object ->
     * receiver-facade MOQ_MEDIA_TRACK_ENDED mapping is covered by
     * test_media_receiver_subscribe.c; together they close the loop through
     * both public facades. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.want_a = true;
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL, *a = NULL;
        add_video_track(s, &v);
        add_audio_track(s, &a);

        /* One keyframe object per track before ready (sync-anchored group). */
        { moq_rcbuf_t *b = mkbuf(16, 1);
          moq_media_send_object_t o = mkobj(b, true, true, true);
          o.presentation_time_us = 1000u;
          if (moq_media_sender_write(s, v, &o) != MOQ_OK) moq_rcbuf_decref(b); }
        { moq_rcbuf_t *b = mkbuf(16, 2);
          moq_media_send_object_t o = mkobj(b, true, true, true);
          o.presentation_time_us = 1000u;
          if (moq_media_sender_write(s, a, &o) != MOQ_OK) moq_rcbuf_decref(b); }

        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        /* Wait until the peer has pulled at least one object from each track. */
        for (int i = 0; i < 200 &&
             (srv_count(&g_srv) < 1 || srv_a_count(&g_srv) < 1); i++)
            usleep(50000);
        MOQ_TEST_CHECK(srv_count(&g_srv) >= 1);
        MOQ_TEST_CHECK(srv_a_count(&g_srv) >= 1);

        /* End ONLY "v" via the public service API. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v), (int)MOQ_OK);

        /* Writes to the ended track are now refused (no ownership taken). */
        { moq_rcbuf_t *b = mkbuf(16, 3);
          moq_media_send_object_t o = mkobj(b, true, true, true);
          o.presentation_time_us = 2000u;
          MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o),
                                (int)MOQ_ERR_WRONG_STATE);
          moq_rcbuf_decref(b); }

        /* Wait until the peer receives the reliable END_OF_TRACK on "v". */
        for (int i = 0; i < 200 && srv_v_eot(&g_srv) < 1; i++) usleep(50000);
        MOQ_TEST_CHECK(srv_v_eot(&g_srv) >= 1);

        /* "a" continues after "v" ended: keep writing audio and prove the peer
         * keeps receiving it. */
        int a_before = srv_a_count(&g_srv);
        for (int g = 0; g < 4; g++) {
            moq_rcbuf_t *b = mkbuf(16, (uint8_t)(0x40 + g));
            moq_media_send_object_t o = mkobj(b, true, true, true);
            o.presentation_time_us = (uint64_t)(3 + g) * 1000u;
            if (moq_media_sender_write(s, a, &o) != MOQ_OK) moq_rcbuf_decref(b);
        }
        for (int i = 0; i < 200 && srv_a_count(&g_srv) <= a_before; i++)
            usleep(50000);
        MOQ_TEST_CHECK(srv_a_count(&g_srv) > a_before);

        /* Ending one track did not end the other or close the session: "a"
         * still flows (asserted above) and the sender is not fatal. */
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, a), (int)MOQ_OK);

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == end_track on a subscriber-less track must not wedge the queue ==== *
     * Regression: the terminal marker was checked AFTER the no-subscriber hold,
     * so ending a track with no subscriber parked the marker at the head
     * forever and blocked every entry behind it -- including other tracks.
     * Here "v" is never subscribed by the peer (no_subscribe gates it) but "a"
     * is: end "v" (no subscriber) with its terminal at the head, then prove "a"
     * still flows and the queue drains. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.no_subscribe = true;    /* peer never subscribes "v" */
        g_srv.want_a = true;          /* but does subscribe "a" */
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL, *a = NULL;
        add_video_track(s, &v);
        add_audio_track(s, &a);

        /* End "v" first -> terminal marker sits at the queue head, and "v" has
         * no subscriber. Then queue an "a" keyframe behind it. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v), (int)MOQ_OK);
        { moq_rcbuf_t *b = mkbuf(16, 7);
          moq_media_send_object_t o = mkobj(b, true, true, true);
          o.presentation_time_us = 1000u;
          if (moq_media_sender_write(s, a, &o) != MOQ_OK) moq_rcbuf_decref(b); }

        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        /* The "v" terminal drains locally (no subscriber == terminal success),
         * unblocking "a": the peer receives audio and the queue clears. */
        moq_media_sender_stats_t st;
        for (int i = 0; i < 200; i++) {
            (void)moq_media_sender_get_stats(s, &st, sizeof(st));
            if (srv_a_count(&g_srv) >= 1 && st.objects_queued == 0) break;
            usleep(50000);
        }
        MOQ_TEST_CHECK(srv_a_count(&g_srv) >= 1);          /* "a" not blocked */
        MOQ_TEST_CHECK_EQ_U64(st.objects_queued, 0);       /* nothing wedged */
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == no subscriber: ready does not mean a consumer -- hold, don't drop *
     * The server accepts the namespace (sender becomes ready) but never
     * subscribes. Written objects must be HELD (queued), not silently
     * discarded by emitting into a subscriber-less publisher. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.no_subscribe = true;
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);           /* DROP_TO_KEYFRAME, big default bound */
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);
        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(atomic_load(&g_srv.ns_accepted));

        for (int i = 0; i < 3; i++) {
            moq_rcbuf_t *b = mkbuf(16, (uint8_t)i);
            moq_media_send_object_t o = mkobj(b, i == 0, i == 0, i == 2);
            moq_result_t rc = moq_media_sender_write(s, v, &o);
            if (rc != MOQ_OK) moq_rcbuf_decref(b);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }

        /* Grace for any (incorrect) drain to run, then assert the objects
         * are HELD: none sent or dropped, all three queued. */
        usleep(300000);
        moq_media_sender_stats_t st;
        (void)moq_media_sender_get_stats(s, &st, sizeof(st));
        MOQ_TEST_CHECK_EQ_U64(st.objects_sent, 0);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);
        MOQ_TEST_CHECK_EQ_U64(st.objects_queued, 3);

        /* Now let the server subscribe: the HELD objects (not discarded)
         * are delivered. A broken hold would have emitted them into the
         * subscriber-less publisher and the server would receive none. */
        g_srv.no_subscribe = false;
        for (int i = 0; i < 200 && srv_count(&g_srv) < 3; i++) usleep(50000);
        MOQ_TEST_CHECK(srv_count(&g_srv) >= 3);
        (void)moq_media_sender_get_stats(s, &st, sizeof(st));
        MOQ_TEST_CHECK(st.objects_sent >= 3);
        MOQ_TEST_CHECK_EQ_U64(st.objects_dropped, 0);

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == terminal: a fatal handshake makes write return CLOSED ========= *
     * EXACT draft-18 against a draft-16-only server fails fatally. Two
     * race-equivalent terminal outcomes, both correct: create() may refuse
     * at attach if the endpoint is already terminal (the create/connect-
     * failure variant), or create() succeeds and the sender turns terminal
     * shortly after -- then write() returns CLOSED with the caller still
     * owning its buffer. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        static const moq_version_t v18 = MOQ_VERSION_DRAFT_18;
        ec.versions.struct_size = sizeof(moq_version_offer_t);
        ec.versions.policy = MOQ_VERSION_POLICY_EXACT;
        ec.versions.versions = &v18;
        ec.versions.version_count = 1;
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        moq_result_t crc = moq_media_sender_create(&cfg, &s);
        if (crc == MOQ_ERR_CLOSED) {
            MOQ_TEST_CHECK(s == NULL);   /* terminal before attach */
        } else {
            MOQ_TEST_CHECK_EQ_INT((int)crc, (int)MOQ_OK);
            moq_media_track_t *v = NULL;
            add_video_track(s, &v);
            bool terminal = false;
            for (int i = 0; i < 300 && !terminal; i++) {
                usleep(50000);
                terminal = moq_media_sender_is_fatal(s) ||
                           moq_media_sender_is_closed(s);
            }
            MOQ_TEST_CHECK(terminal);
            moq_rcbuf_t *b = mkbuf(16, 0);
            moq_media_send_object_t o = mkobj(b, true, true, true);
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o),
                                  (int)MOQ_ERR_CLOSED);
            moq_rcbuf_decref(b);   /* ownership stayed with the caller */
            moq_media_sender_destroy(s);
        }
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == init_data survives add_track -> published catalog ============== *
     * A RAW/LOC track's init_data must be encoded into the catalog the sender
     * publishes (any packaging). The server captures the catalog payload; we
     * parse it and confirm the "video" track's initData decodes to the bytes
     * passed to add_track. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);

        static const uint8_t extradata[] = {
            0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x09, 0x12, 0x34 };
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("v");
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("av01");
        tc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        tc.timescale = 1000000;
        tc.init_data = (moq_bytes_t){ extradata, sizeof(extradata) };
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v),
                              (int)MOQ_OK);

        MOQ_TEST_CHECK(wait_ready(s, 300));

        /* Wait for the server to capture the published catalog. */
        size_t clen = 0;
        for (int i = 0; i < 200 && clen == 0; i++) {
            pthread_mutex_lock(&g_srv.mu); clen = g_srv.cat_len;
            pthread_mutex_unlock(&g_srv.mu);
            if (clen == 0) usleep(50000);
        }
        MOQ_TEST_CHECK(clen > 0);

        if (clen > 0) {
            uint8_t json[4096];
            pthread_mutex_lock(&g_srv.mu);
            clen = g_srv.cat_len; memcpy(json, g_srv.cat_buf, clen);
            pthread_mutex_unlock(&g_srv.mu);

            const moq_alloc_t *al = moq_alloc_default();
            moq_msf_catalog_t catalog;
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(
                al, (moq_bytes_t){ json, clen }, &catalog), (int)MOQ_OK);
            const moq_msf_track_t *vt =
                moq_msf_catalog_find_role(&catalog, "video");
            MOQ_TEST_CHECK(vt != NULL);
            if (vt) {
                MOQ_TEST_CHECK(vt->has_init_data && vt->init_data.len > 0);
                moq_rcbuf_t *decoded = NULL;
                MOQ_TEST_CHECK_EQ_INT((int)moq_msf_decode_init_data(
                    al, vt->init_data, &decoded), (int)MOQ_OK);
                if (decoded) {
                    MOQ_TEST_CHECK_EQ_U64(moq_rcbuf_len(decoded),
                                          sizeof(extradata));
                    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(decoded), extradata,
                                          sizeof(extradata)) == 0);
                    moq_rcbuf_decref(decoded);
                }
                /* Default config authors NO CMSF metadata: a track/catalog with
                 * none configured emits none (byte-compatible with pre-CMSF). */
                MOQ_TEST_CHECK_EQ_U64(catalog.content_protection_count, 0);
                MOQ_TEST_CHECK(!vt->has_max_grp_sap && !vt->has_max_obj_sap);
                MOQ_TEST_CHECK_EQ_U64(vt->cp_ref_id_count, 0);
            }
            moq_msf_catalog_cleanup(al, &catalog);
        }

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == CMAF init_data -> catalog initDataList[] + track initRef ======= *
     * A CMAF track's init segment must be published via the MSF-01/CMSF
     * initDataList + initRef indirection (not inline initData). The captured
     * catalog's CMAF track carries initRef (no inline initData), and that ref
     * resolves through initDataList to the decoded init bytes. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);

        static const uint8_t cmaf_init[] = {
            0x00, 0x00, 0x00, 0x18, 'f','t','y','p', 'c','m','f','2' };
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("v");
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        tc.codec = MOQ_BYTES_LITERAL("avc1.640028");
        tc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        tc.timescale = 90000;
        tc.init_data = (moq_bytes_t){ cmaf_init, sizeof(cmaf_init) };
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v),
                              (int)MOQ_OK);

        MOQ_TEST_CHECK(wait_ready(s, 300));

        size_t clen = 0;
        for (int i = 0; i < 200 && clen == 0; i++) {
            pthread_mutex_lock(&g_srv.mu); clen = g_srv.cat_len;
            pthread_mutex_unlock(&g_srv.mu);
            if (clen == 0) usleep(50000);
        }
        MOQ_TEST_CHECK(clen > 0);

        if (clen > 0) {
            uint8_t json[4096];
            pthread_mutex_lock(&g_srv.mu);
            clen = g_srv.cat_len; memcpy(json, g_srv.cat_buf, clen);
            pthread_mutex_unlock(&g_srv.mu);

            const moq_alloc_t *al = moq_alloc_default();
            moq_msf_catalog_t catalog;
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(
                al, (moq_bytes_t){ json, clen }, &catalog), (int)MOQ_OK);
            const moq_msf_track_t *vt =
                moq_msf_catalog_find_role(&catalog, "video");
            MOQ_TEST_CHECK(vt != NULL);
            if (vt) {
                MOQ_TEST_CHECK(vt->packaging.len == 4 &&
                    memcmp(vt->packaging.data, "cmaf", 4) == 0);
                MOQ_TEST_CHECK(vt->has_init_ref && !vt->has_init_data);
                const moq_msf_init_data_entry_t *e =
                    moq_msf_catalog_find_init_data(&catalog, vt->init_ref);
                MOQ_TEST_CHECK(e != NULL);
                if (e) {
                    moq_rcbuf_t *decoded = NULL;
                    MOQ_TEST_CHECK_EQ_INT((int)moq_msf_decode_init_data(
                        al, e->data, &decoded), (int)MOQ_OK);
                    if (decoded) {
                        MOQ_TEST_CHECK_EQ_U64(moq_rcbuf_len(decoded),
                                              sizeof(cmaf_init));
                        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(decoded), cmaf_init,
                                              sizeof(cmaf_init)) == 0);
                        moq_rcbuf_decref(decoded);
                    }
                }
            }
            moq_msf_catalog_cleanup(al, &catalog);
        }

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == CMAF validation (CMSF §3.3/§3.4), explicit on/off, deterministic == *
     * sflags 0x02000000 = sync sample (SAP, allowed at group start);
     * 0x01010000 = non-sync P/B frame (sap NONE). */
    {
        /* validate off: a malformed CMAF object passes through (queued). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write(false, false, 0, 0, 0, true, true), (int)MOQ_OK);
        /* validate on: malformed CMAF object rejected, no ownership taken. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write(true, false, 0, 0, 0, true, true), (int)MOQ_ERR_PROTO);
        /* validate on, no init: a well-formed object is accepted. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write(true, false, 0, 1, 0x02000000, true, false), (int)MOQ_OK);
        /* validate on: a group-start object that is known not to be a SAP is
         * rejected (§3.4)... */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write(true, false, 0, 1, 0x01010000, true, false), (int)MOQ_ERR_PROTO);
        /* ...but the same non-SAP object mid-group (not group start) is fine. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write(true, false, 0, 1, 0x01010000, false, false), (int)MOQ_OK);
        /* validate on, with init (tkhd track_ID=7): matching object accepted. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write(true, true, 7, 7, 0x02000000, true, false), (int)MOQ_OK);
        /* validate on, with init (track_ID=7): mismatched object track_ID
         * rejected (§3.3 init match). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write(true, true, 7, 1, 0x02000000, true, false), (int)MOQ_ERR_PROTO);
        /* §3.4: an open-GOP, non-sync INDEPENDENT first sample (non_sync set,
         * depends_on==2 -> sflags 0x02010000) is a possible SAP type 3, which is
         * NOT allowed to start a group without an explicit type-1/2 declaration.
         * It reports sap_type UNKNOWN but starts_with_sync=false, so it is now
         * rejected at group start... */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write(true, false, 0, 1, 0x02010000, true, false), (int)MOQ_ERR_PROTO);
        /* ...while the same open-GOP sample mid-group (not a group start) is
         * accepted (SAP type 3 is fine mid-group). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write(true, false, 0, 1, 0x02010000, false, false), (int)MOQ_OK);
    }

    /* == CMSF validation is STRICT BY DEFAULT (cfg_init), opt out explicitly == *
     * Regression for the default policy: the cfg_init* initializers enable
     * validate_cmaf, so a malformed CMAF object is rejected without the caller
     * opting in; passthrough still works when explicitly requested. */
    {
        /* Default config (no explicit validate_cmaf): malformed CMAF rejected. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)cmaf_write_default_cfg(false, true), (int)MOQ_ERR_PROTO);
        /* Explicit passthrough (validate_cmaf=false): same malformed accepted. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)cmaf_write_default_cfg(true, true), (int)MOQ_OK);
        /* Default config: a well-formed CMAF object is accepted. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)cmaf_write_default_cfg(false, false), (int)MOQ_OK);
    }

    /* == app-declared SAP type drives §3.4 group-start enforcement ======== *
     * With a declaration the exact type governs (we never infer 1/2/3 from
     * flags); without one, the conservative flag-based rule stands. */
    {
        /* validate on, declared TYPE_1 / TYPE_2 at group start -> accepted. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write_sap(true, true, true, MOQ_SAP_TYPE_1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write_sap(true, true, true, MOQ_SAP_TYPE_2), (int)MOQ_OK);
        /* declared TYPE_3 / NONE at group start -> rejected (§3.4: must be 1/2). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write_sap(true, true, true, MOQ_SAP_TYPE_3), (int)MOQ_ERR_PROTO);
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write_sap(true, true, true, MOQ_SAP_NONE), (int)MOQ_ERR_PROTO);
        /* declared TYPE_3 mid-group (not group start) -> accepted. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write_sap(true, false, true, MOQ_SAP_TYPE_3), (int)MOQ_OK);
        /* declared UNKNOWN is not a valid declaration -> INVAL (even validate off). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write_sap(false, true, true, MOQ_SAP_UNKNOWN), (int)MOQ_ERR_INVAL);
        /* validate OFF: a declaration is not enforced (no default change). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)try_cmaf_write_sap(false, true, true, MOQ_SAP_TYPE_3), (int)MOQ_OK);
    }

    /* == CMSF authoring: root contentProtections + track refs + max SAP === *
     * The sender authors a root contentProtections entry and a track that
     * references it (contentProtectionRefIDs) and carries maxGrp/maxObj SAP.
     * The captured catalog parses with the protection entry, the track ref, and
     * the SAP fields, and the ref resolves via the existing parser accessor. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;

        moq_bytes_t kids[] = {
            MOQ_BYTES_LITERAL("01234567-89ab-cdef-0123-456789abcdef") };
        moq_cmsf_content_protection_t cp;
        memset(&cp, 0, sizeof(cp));
        cp.ref_id = MOQ_BYTES_LITERAL("cp-cenc");
        cp.default_kids = kids;
        cp.default_kid_count = 1;
        cp.scheme = MOQ_BYTES_LITERAL("cenc");
        cp.drm_system.system_id =
            MOQ_BYTES_LITERAL("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");
        cp.drm_system.la_url.present = true;
        cp.drm_system.la_url.url = MOQ_BYTES_LITERAL("https://drm.example/la");
        cfg.content_protections = &cp;
        cfg.content_protection_count = 1;

        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);

        moq_bytes_t refs[] = { MOQ_BYTES_LITERAL("cp-cenc") };
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("v");
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("av01");
        tc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        tc.timescale = 1000000;
        tc.content_protection_ref_ids = refs;
        tc.content_protection_ref_id_count = 1;
        tc.has_max_grp_sap = true; tc.max_grp_sap = 1;
        tc.has_max_obj_sap = true; tc.max_obj_sap = 2;
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v),
                              (int)MOQ_OK);

        MOQ_TEST_CHECK(wait_ready(s, 300));

        size_t clen = 0;
        for (int i = 0; i < 200 && clen == 0; i++) {
            pthread_mutex_lock(&g_srv.mu); clen = g_srv.cat_len;
            pthread_mutex_unlock(&g_srv.mu);
            if (clen == 0) usleep(50000);
        }
        MOQ_TEST_CHECK(clen > 0);
        if (clen > 0) {
            uint8_t json[4096];
            pthread_mutex_lock(&g_srv.mu);
            clen = g_srv.cat_len; memcpy(json, g_srv.cat_buf, clen);
            pthread_mutex_unlock(&g_srv.mu);

            const moq_alloc_t *al = moq_alloc_default();
            moq_msf_catalog_t catalog;
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(
                al, (moq_bytes_t){ json, clen }, &catalog), (int)MOQ_OK);

            /* Root contentProtections round-tripped. */
            MOQ_TEST_CHECK_EQ_U64(catalog.content_protection_count, 1);
            if (catalog.content_protection_count >= 1) {
                const moq_cmsf_content_protection_t *gcp =
                    &catalog.content_protections[0];
                MOQ_TEST_CHECK(bytes_eq(gcp->ref_id, "cp-cenc"));
                MOQ_TEST_CHECK(bytes_eq(gcp->scheme, "cenc"));
                MOQ_TEST_CHECK_EQ_U64(gcp->default_kid_count, 1);
                if (gcp->default_kid_count >= 1)
                    MOQ_TEST_CHECK(bytes_eq(gcp->default_kids[0],
                        "01234567-89ab-cdef-0123-456789abcdef"));
                MOQ_TEST_CHECK(bytes_eq(gcp->drm_system.system_id,
                    "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"));
            }

            /* Track refs + SAP round-tripped. */
            const moq_msf_track_t *vt =
                moq_msf_catalog_find_role(&catalog, "video");
            MOQ_TEST_CHECK(vt != NULL);
            if (vt) {
                MOQ_TEST_CHECK_EQ_U64(vt->cp_ref_id_count, 1);
                if (vt->cp_ref_id_count >= 1)
                    MOQ_TEST_CHECK(bytes_eq(vt->cp_ref_ids[0], "cp-cenc"));
                MOQ_TEST_CHECK(vt->has_max_grp_sap && vt->max_grp_sap == 1);
                MOQ_TEST_CHECK(vt->has_max_obj_sap && vt->max_obj_sap == 2);

                /* The track ref resolves to the root entry via the accessor. */
                if (vt->cp_ref_id_count >= 1) {
                    const moq_cmsf_content_protection_t *r =
                        moq_msf_catalog_find_content_protection(&catalog,
                            vt->cp_ref_ids[0]);
                    MOQ_TEST_CHECK(r != NULL);
                    if (r) MOQ_TEST_CHECK(bytes_eq(r->scheme, "cenc"));
                }
            }
            moq_msf_catalog_cleanup(al, &catalog);
        }

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == CMSF authoring validation negatives (no network needed) ========= *
     * Malformed root entries fail create() before any endpoint stands up;
     * a dangling track ref fails add_track. */
    {
        moq_bytes_t kids[] = { MOQ_BYTES_LITERAL("01234567-89ab-cdef-0123-456789abcdef") };
        moq_bytes_t parts[2];

        /* (a) empty refID -> INVAL at create, no endpoint, no sender. */
        {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
            moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            moq_cmsf_content_protection_t cp; memset(&cp, 0, sizeof(cp));
            cp.scheme = MOQ_BYTES_LITERAL("cenc");
            cp.default_kids = kids; cp.default_kid_count = 1;
            cp.drm_system.system_id = MOQ_BYTES_LITERAL("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");
            cfg.content_protections = &cp; cfg.content_protection_count = 1;
            moq_media_sender_t *s = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(s == NULL);
        }
        /* (b) zero defaultKIDs -> INVAL (authoring stricter than the parser). */
        {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
            moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            moq_cmsf_content_protection_t cp; memset(&cp, 0, sizeof(cp));
            cp.ref_id = MOQ_BYTES_LITERAL("cp");
            cp.scheme = MOQ_BYTES_LITERAL("cenc");
            cp.drm_system.system_id = MOQ_BYTES_LITERAL("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");
            cfg.content_protections = &cp; cfg.content_protection_count = 1;
            moq_media_sender_t *s = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(s == NULL);
        }
        /* (b2) scheme outside the CENC enum (cenc|cbcs) -> INVAL (§4.1.1.3). */
        {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
            moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            moq_cmsf_content_protection_t cp; memset(&cp, 0, sizeof(cp));
            cp.ref_id = MOQ_BYTES_LITERAL("cp");
            cp.scheme = MOQ_BYTES_LITERAL("aes-128");   /* not cenc/cbcs */
            cp.default_kids = kids; cp.default_kid_count = 1;
            cp.drm_system.system_id = MOQ_BYTES_LITERAL("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");
            cfg.content_protections = &cp; cfg.content_protection_count = 1;
            moq_media_sender_t *s = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(s == NULL);
        }
        /* (b3) duplicate refID across the array -> INVAL (§4.1.1.1 unique). */
        {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
            moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            moq_cmsf_content_protection_t cps[2]; memset(cps, 0, sizeof(cps));
            cps[0].ref_id = MOQ_BYTES_LITERAL("dup");
            cps[0].scheme = MOQ_BYTES_LITERAL("cenc");
            cps[0].default_kids = kids; cps[0].default_kid_count = 1;
            cps[0].drm_system.system_id = MOQ_BYTES_LITERAL("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");
            cps[1] = cps[0];                            /* same refID -> dup */
            cfg.content_protections = cps; cfg.content_protection_count = 2;
            moq_media_sender_t *s = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(s == NULL);
        }
        /* (b4) cbcs scheme + two DISTINCT refIDs -> accepted (the dup check
         * must not false-positive on different ids). */
        {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
            moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            moq_cmsf_content_protection_t cps[2]; memset(cps, 0, sizeof(cps));
            cps[0].ref_id = MOQ_BYTES_LITERAL("cpA");
            cps[0].scheme = MOQ_BYTES_LITERAL("cbcs");
            cps[0].default_kids = kids; cps[0].default_kid_count = 1;
            cps[0].drm_system.system_id = MOQ_BYTES_LITERAL("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");
            cps[1] = cps[0];
            cps[1].ref_id = MOQ_BYTES_LITERAL("cpB");
            cfg.content_protections = cps; cfg.content_protection_count = 2;
            moq_media_sender_t *s = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                                  (int)MOQ_OK);
            if (s) moq_media_sender_destroy(s);
        }
        /* (b5) systemID not a UUID -> INVAL (§4.1.1.4.1). */
        {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
            moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            moq_cmsf_content_protection_t cp; memset(&cp, 0, sizeof(cp));
            cp.ref_id = MOQ_BYTES_LITERAL("cp");
            cp.scheme = MOQ_BYTES_LITERAL("cenc");
            cp.default_kids = kids; cp.default_kid_count = 1;   /* valid UUID */
            cp.drm_system.system_id = MOQ_BYTES_LITERAL("not-a-uuid");
            cfg.content_protections = &cp; cfg.content_protection_count = 1;
            moq_media_sender_t *s = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(s == NULL);
        }
        /* (b6) a defaultKID not a UUID (16 hex, no hyphens) -> INVAL (§4.1.1.2). */
        {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
            moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            moq_bytes_t bad_kids[] = { MOQ_BYTES_LITERAL("0123456789abcdef") };
            moq_cmsf_content_protection_t cp; memset(&cp, 0, sizeof(cp));
            cp.ref_id = MOQ_BYTES_LITERAL("cp");
            cp.scheme = MOQ_BYTES_LITERAL("cenc");
            cp.default_kids = bad_kids; cp.default_kid_count = 1;
            cp.drm_system.system_id =
                MOQ_BYTES_LITERAL("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");
            cfg.content_protections = &cp; cfg.content_protection_count = 1;
            moq_media_sender_t *s = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                                  (int)MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(s == NULL);
        }
        /* (b7) UPPERCASE-hex UUIDs accepted (UUID syntax is case-insensitive). */
        {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
            moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            moq_bytes_t uc_kids[] = {
                MOQ_BYTES_LITERAL("01234567-89AB-CDEF-0123-456789ABCDEF") };
            moq_cmsf_content_protection_t cp; memset(&cp, 0, sizeof(cp));
            cp.ref_id = MOQ_BYTES_LITERAL("cp");
            cp.scheme = MOQ_BYTES_LITERAL("cenc");
            cp.default_kids = uc_kids; cp.default_kid_count = 1;
            cp.drm_system.system_id =
                MOQ_BYTES_LITERAL("EDEF8BA9-79D6-4ACE-A3C8-27DCD51D21ED");
            cfg.content_protections = &cp; cfg.content_protection_count = 1;
            moq_media_sender_t *s = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                                  (int)MOQ_OK);
            if (s) moq_media_sender_destroy(s);
        }
        /* (c) dangling track ref -> add_track INVAL; matching ref accepted. */
        {
            char url[64];
            moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
            moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
            cfg.endpoint = &ec;
            moq_cmsf_content_protection_t cp; memset(&cp, 0, sizeof(cp));
            cp.ref_id = MOQ_BYTES_LITERAL("cp-known");
            cp.scheme = MOQ_BYTES_LITERAL("cenc");
            cp.default_kids = kids; cp.default_kid_count = 1;
            cp.drm_system.system_id = MOQ_BYTES_LITERAL("edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");
            cfg.content_protections = &cp; cfg.content_protection_count = 1;
            moq_media_sender_t *s = NULL;
            MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                                  (int)MOQ_OK);
            if (s) {
                moq_media_track_cfg_t tc; moq_media_track_cfg_init(&tc);
                tc.name = MOQ_BYTES_LITERAL("v");
                tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
                tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
                tc.codec = MOQ_BYTES_LITERAL("av01");
                tc.bitrate = 1500000;          /* MSF-01 5.2.22 */
                moq_bytes_t bad[] = { MOQ_BYTES_LITERAL("cp-unknown") };
                tc.content_protection_ref_ids = bad;
                tc.content_protection_ref_id_count = 1;
                moq_media_track_t *v = NULL;
                MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v),
                                      (int)MOQ_ERR_INVAL);
                MOQ_TEST_CHECK(v == NULL);

                moq_bytes_t good[] = { MOQ_BYTES_LITERAL("cp-known") };
                tc.content_protection_ref_ids = good;
                MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v),
                                      (int)MOQ_OK);
                moq_media_sender_destroy(s);
            }
        }

        /* Optional-field negatives: a present-but-empty optional span must fail
         * create() (never crash the deep-copy, never fail late in encode). Each
         * builds an otherwise-valid entry and corrupts one optional field. */
        #define CP_BASE(cpv) do {                                       \
            memset(&(cpv), 0, sizeof(cpv));                             \
            (cpv).ref_id = MOQ_BYTES_LITERAL("cp");                     \
            (cpv).scheme = MOQ_BYTES_LITERAL("cenc");                   \
            (cpv).default_kids = kids; (cpv).default_kid_count = 1;     \
            (cpv).drm_system.system_id = MOQ_BYTES_LITERAL(             \
                "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed");                \
        } while (0)
        #define CP_EXPECT_INVAL(cpv) do {                              \
            char url2[64];                                              \
            moq_endpoint_cfg_t ec2 = ep_cfg(url2, sizeof(url2), 1);     \
            moq_media_sender_cfg_t cfg2; fill_cfg(&cfg2, parts);        \
            cfg2.endpoint = &ec2;                                       \
            cfg2.content_protections = &(cpv);                         \
            cfg2.content_protection_count = 1;                         \
            moq_media_sender_t *s2 = NULL;                              \
            MOQ_TEST_CHECK_EQ_INT(                                      \
                (int)moq_media_sender_create(&cfg2, &s2),               \
                (int)MOQ_ERR_INVAL);                                    \
            MOQ_TEST_CHECK(s2 == NULL);                                 \
        } while (0)

        /* (d) laURL marked present without a url. */
        {
            moq_cmsf_content_protection_t cp; CP_BASE(cp);
            cp.drm_system.la_url.present = true;   /* url left empty */
            CP_EXPECT_INVAL(cp);
        }
        /* (e) has_pssh with nonzero len but NULL data (would crash cp_put). */
        {
            moq_cmsf_content_protection_t cp; CP_BASE(cp);
            cp.drm_system.has_pssh = true;
            cp.drm_system.pssh = (moq_bytes_t){ NULL, 8 };
            CP_EXPECT_INVAL(cp);
        }
        /* (f) certURL present + has_type but an empty type span. */
        {
            moq_cmsf_content_protection_t cp; CP_BASE(cp);
            cp.drm_system.cert_url.present = true;
            cp.drm_system.cert_url.url = MOQ_BYTES_LITERAL("https://c.example");
            cp.drm_system.cert_url.has_type = true;   /* type left empty */
            CP_EXPECT_INVAL(cp);
        }
        /* (g) has_robustness with NULL data. */
        {
            moq_cmsf_content_protection_t cp; CP_BASE(cp);
            cp.drm_system.has_robustness = true;
            cp.drm_system.robustness = (moq_bytes_t){ NULL, 3 };
            CP_EXPECT_INVAL(cp);
        }
        #undef CP_BASE
        #undef CP_EXPECT_INVAL
    }

    /* == CMAF initDataList dedup: identical init shared by two tracks ===== *
     * Two CMAF tracks with byte-identical init segments share ONE
     * initDataList entry; both tracks' initRef point at the same id. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);

        static const uint8_t cmaf_init[] = {
            0x00, 0x00, 0x00, 0x18, 'f','t','y','p', 'c','m','f','2' };
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        tc.codec = MOQ_BYTES_LITERAL("avc1.640028");
        tc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        tc.timescale = 90000;
        tc.init_data = (moq_bytes_t){ cmaf_init, sizeof(cmaf_init) };
        tc.name = MOQ_BYTES_LITERAL("v");
        tc.role = MOQ_BYTES_LITERAL("video");
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v),
                              (int)MOQ_OK);
        /* Second CMAF track, identical init bytes (distinct role/name). */
        tc.name = MOQ_BYTES_LITERAL("v2");
        tc.role = MOQ_BYTES_LITERAL("audio");
        moq_media_track_t *v2 = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v2),
                              (int)MOQ_OK);

        MOQ_TEST_CHECK(wait_ready(s, 300));

        size_t clen = 0;
        for (int i = 0; i < 200 && clen == 0; i++) {
            pthread_mutex_lock(&g_srv.mu); clen = g_srv.cat_len;
            pthread_mutex_unlock(&g_srv.mu);
            if (clen == 0) usleep(50000);
        }
        MOQ_TEST_CHECK(clen > 0);
        if (clen > 0) {
            uint8_t json[4096];
            pthread_mutex_lock(&g_srv.mu);
            clen = g_srv.cat_len; memcpy(json, g_srv.cat_buf, clen);
            pthread_mutex_unlock(&g_srv.mu);

            const moq_alloc_t *al = moq_alloc_default();
            moq_msf_catalog_t catalog;
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(
                al, (moq_bytes_t){ json, clen }, &catalog), (int)MOQ_OK);

            /* One shared entry, not two. */
            MOQ_TEST_CHECK_EQ_U64(catalog.init_data_count, 1);

            const moq_msf_track_t *t0 = NULL, *t1 = NULL;
            for (size_t i = 0; i < catalog.track_count; i++) {
                moq_bytes_t nm = catalog.tracks[i].name;
                if (nm.len == 1 && nm.data[0] == 'v') t0 = &catalog.tracks[i];
                else if (nm.len == 2 && memcmp(nm.data, "v2", 2) == 0)
                    t1 = &catalog.tracks[i];
            }
            MOQ_TEST_CHECK(t0 != NULL && t1 != NULL);
            if (t0 && t1) {
                MOQ_TEST_CHECK(t0->has_init_ref && t1->has_init_ref);
                /* Both initRefs are the same shared id and resolve. */
                MOQ_TEST_CHECK(t0->init_ref.len == t1->init_ref.len &&
                    t0->init_ref.len > 0 &&
                    memcmp(t0->init_ref.data, t1->init_ref.data,
                           t0->init_ref.len) == 0);
                MOQ_TEST_CHECK(moq_msf_catalog_find_init_data(
                    &catalog, t0->init_ref) != NULL);
            }
            moq_msf_catalog_cleanup(al, &catalog);
        }

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == SAP timeline: group-start declaration rules (pre-ready) ======== *
     * With emit_sap_timeline, a group-start object MUST declare SAP type 1 or 2.
     * No declaration / NONE / TYPE_3 is rejected; TYPE_1 is accepted. Dead port:
     * write() validates before enqueue. */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_sap(s, 0, &v);

        moq_rcbuf_t *b = mkbuf(16, 0);
        moq_media_send_object_t o = mkobj(b, true, true, false);  /* no sap_type */
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_ERR_INVAL);
        b = mkbuf(16, 0);
        o = mkobj_sap(b, true, true, false, MOQ_SAP_NONE, 0);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_ERR_INVAL);
        b = mkbuf(16, 0);
        o = mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_3, 0);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_ERR_INVAL);
        b = mkbuf(16, 0);
        o = mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_1, 0);   /* OK (queued) */
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        moq_media_sender_destroy(s);
    }

    /* == SAP timeline: generated-name collision rejects (pre-ready) ===== *
     * A pre-existing track named "v.sap" makes the generated timeline for a
     * later "v"+emit_sap_timeline collide -> add_track returns INVAL. */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);

        moq_media_track_cfg_t pc;
        moq_media_track_cfg_init(&pc);
        pc.name = MOQ_BYTES_LITERAL("v.sap");
        pc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        pc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        pc.codec = MOQ_BYTES_LITERAL("av01");
        pc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        moq_media_track_t *pre = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &pc, &pre),
                              (int)MOQ_OK);

        moq_media_track_cfg_t vc;
        moq_media_track_cfg_init(&vc);
        vc.name = MOQ_BYTES_LITERAL("v");
        vc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        vc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        vc.codec = MOQ_BYTES_LITERAL("av01");
        vc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        vc.emit_sap_timeline = true;
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &vc, &v),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(v == NULL);
        moq_media_sender_destroy(s);
    }

    /* reverse: add "v"+emit_sap_timeline (registers generated "v.sap"), then an
     * explicit track named "v.sap" -> rejected by the general dup-name guard. */
    {
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), 1);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_sap(s, 0, &v);   /* registers "v" + generated "v.sap" */

        moq_media_track_cfg_t dc;
        moq_media_track_cfg_init(&dc);
        dc.name = MOQ_BYTES_LITERAL("v.sap");
        dc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        dc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        dc.codec = MOQ_BYTES_LITERAL("av01");
        dc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        moq_media_track_t *dup = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &dc, &dup),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(dup == NULL);
        moq_media_sender_destroy(s);
    }

    /* == SAP timeline: catalog opt-in adds one eventtimeline track ====== *
     * emit_sap_timeline publishes a generated "v.sap" eventtimeline track with
     * the §8.2 fields (eventType, mimeType, depends=["v"]); without it the
     * catalog carries only the media track. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_sap(s, 0, &v);
        MOQ_TEST_CHECK(wait_ready(s, 300));

        uint8_t json[4096];
        size_t clen = capture_catalog(json, sizeof(json));
        MOQ_TEST_CHECK(clen > 0);
        if (clen > 0) {
            const moq_alloc_t *al = moq_alloc_default();
            moq_msf_catalog_t cat;
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(
                al, (moq_bytes_t){ json, clen }, &cat), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(cat.track_count, 2);
            const moq_msf_track_t *tl = find_track(&cat, "v.sap");
            MOQ_TEST_CHECK(tl != NULL);
            if (tl) {
                MOQ_TEST_CHECK(bytes_eq(tl->packaging, "eventtimeline"));
                MOQ_TEST_CHECK(tl->has_event_type &&
                    bytes_eq(tl->event_type, "org.ietf.moq.cmsf.sap"));
                MOQ_TEST_CHECK(tl->has_mime_type &&
                    bytes_eq(tl->mime_type, "application/json"));
                MOQ_TEST_CHECK(tl->depends_count == 1 &&
                    bytes_eq(tl->depends[0], "v"));
            }
            moq_msf_catalog_cleanup(al, &cat);
        }
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* off => no eventtimeline track in the catalog (byte-compatible). */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);   /* no emit_sap_timeline */
        MOQ_TEST_CHECK(wait_ready(s, 300));
        uint8_t json[4096];
        size_t clen = capture_catalog(json, sizeof(json));
        MOQ_TEST_CHECK(clen > 0);
        if (clen > 0) {
            const moq_alloc_t *al = moq_alloc_default();
            moq_msf_catalog_t cat;
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(
                al, (moq_bytes_t){ json, clen }, &cat), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(cat.track_count, 1);
            MOQ_TEST_CHECK(find_track(&cat, "v.sap") == NULL);
            moq_msf_catalog_cleanup(al, &cat);
        }
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == SAP timeline: an unsubscribed timeline never HOL-blocks media === *
     * emit_sap_timeline is on but the server subscribes the catalog + "v"
     * only (never "v.sap"). Media must still flow -- the generated timeline is
     * not in the media FIFO -- and no timeline object is emitted. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        /* want_sap stays false: "v.sap" is never subscribed. */
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_sap(s, 0, &v);
        MOQ_TEST_CHECK(wait_ready(s, 300));

        moq_rcbuf_t *b = mkbuf(16, 0);
        moq_media_send_object_t o =
            mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_2, 0);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        b = mkbuf(16, 1);
        o = mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_2, 4000000);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);

        for (int i = 0; i < 200 && srv_count(&g_srv) < 2; i++) usleep(50000);
        MOQ_TEST_CHECK(srv_count(&g_srv) >= 2);     /* media flowed */
        MOQ_TEST_CHECK_EQ_INT(srv_sap_count(&g_srv), 0);  /* nothing emitted */
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == SAP timeline: per-boundary cadence (§8.3, subscriber present) === *
     * Each media-group boundary MUST emit its OWN independent Object 0 (the
     * timeline shares media group ids); groups are not coalesced even when they
     * drain together. Object 0 of each group accumulates all prior records. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.want_sap = true;          /* subscribe "v.sap" immediately */
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_sap(s, 0, &v);
        MOQ_TEST_CHECK(wait_ready(s, 300));

        /* group 0: keyframe TYPE_2 (ept 0) + a mid-group TYPE_3 (ept 2100). */
        moq_rcbuf_t *b = mkbuf(16, 0);
        moq_media_send_object_t o =
            mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_2, 0);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        b = mkbuf(16, 1);
        o = mkobj_sap(b, false, false, false, MOQ_SAP_TYPE_3, 2100000);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        /* group 1: keyframe TYPE_2 (ept 4000). group 2: keyframe TYPE_2 with a
         * WIDE ept (16 digits) so group 2's 4-record cumulative Object 0
         * exercises near-worst-case encode width + inter-record commas. The pts
         * stays under the QUIC varint ceiling (2^62) so the MEDIA LOC encode is
         * valid; the SAP ept (pts/1000) is the wide JSON value. */
        b = mkbuf(16, 2);
        o = mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_2, 4000000);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        b = mkbuf(16, 3);
        o = mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_2, 4000000000000000000ull);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);

        for (int i = 0; i < 200 && !srv_sap_has_obj0(&g_srv, 2); i++)
            usleep(50000);

        /* Separate Object 0 for EACH media-group boundary (not coalesced). */
        MOQ_TEST_CHECK(srv_sap_has_obj0(&g_srv, 0));
        MOQ_TEST_CHECK(srv_sap_has_obj0(&g_srv, 1));
        MOQ_TEST_CHECK(srv_sap_has_obj0(&g_srv, 2));
        /* §8.3 accumulation: group 1's Object 0 carries group 0 + group 1. */
        MOQ_TEST_CHECK(srv_sap_obj0_has(&g_srv, 1, "\"l\":[0,0]", "\"l\":[1,0]"));
        /* Multi-record encode: group 2's Object 0 has the wide [2,0] record
         * intact AND a correct inter-record comma separator (regression for
         * sap_encode_records sizing). */
        MOQ_TEST_CHECK(srv_sap_obj0_has(&g_srv, 2,
            "\"l\":[2,0],\"data\":[2,4000000000000000]", NULL));
        MOQ_TEST_CHECK(srv_sap_obj0_has(&g_srv, 2, "]},{\"l\":[", NULL));
        MOQ_TEST_CHECK(srv_sap_any_contains(&g_srv,
            "\"l\":[0,1],\"data\":[3,2100]"));
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == SAP timeline: EPT rounds to NEAREST ms (CMSF-01 3.6.1) ========= *
     * The SAP-type timeline EPT is the earliest presentation timestamp rounded
     * to the nearest millisecond -- not floored (unlike the MSF media-timeline
     * mediaTime). Drive group-start Objects whose pts_us carries a sub-ms
     * remainder and assert the encoded EPT rounds: up at >=500us, down below
     * 500us, including a large value (rounding stays overflow-safe). */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.want_sap = true;
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_sap(s, 0, &v);
        MOQ_TEST_CHECK(wait_ready(s, 300));

        /* group 0: 1500us -> 2ms (rounds UP; floor would be 1). */
        moq_rcbuf_t *b = mkbuf(16, 0);
        moq_media_send_object_t o =
            mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_2, 1500);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        /* group 1: 3400us -> 3ms (remainder 400 < 500 stays DOWN, not 4). */
        b = mkbuf(16, 1);
        o = mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_2, 3400);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        /* group 2: large pts with a >=500us remainder rounds up without wrap
         * (stays under the 2^62 LOC varint ceiling): 4000000000000000500 ->
         * 4000000000000001. */
        b = mkbuf(16, 2);
        o = mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_2,
                      4000000000000000500ull);
        MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);

        for (int i = 0; i < 200 && !srv_sap_has_obj0(&g_srv, 2); i++)
            usleep(50000);

        MOQ_TEST_CHECK(srv_sap_has_obj0(&g_srv, 2));
        /* round UP: 1500us -> 2 (floor would be 1). */
        MOQ_TEST_CHECK(srv_sap_any_contains(&g_srv,
            "\"l\":[0,0],\"data\":[2,2]"));
        MOQ_TEST_CHECK(!srv_sap_any_contains(&g_srv,
            "\"l\":[0,0],\"data\":[2,1]"));        /* the floored (incorrect) value */
        /* round DOWN below 500us: 3400us -> 3 (not 4). */
        MOQ_TEST_CHECK(srv_sap_any_contains(&g_srv,
            "\"l\":[1,0],\"data\":[2,3]"));
        MOQ_TEST_CHECK(!srv_sap_any_contains(&g_srv,
            "\"l\":[1,0],\"data\":[2,4]"));
        /* large value rounds up, overflow-safe. */
        MOQ_TEST_CHECK(srv_sap_any_contains(&g_srv,
            "\"l\":[2,0],\"data\":[2,4000000000000001]"));
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == SAP timeline: late subscriber + bounded-history eviction ======= *
     * History bound = 2 groups; "v.sap" subscribes only after 3 media objects
     * (groups) have arrived. The retained groups (1 and 2) each emit their own
     * independent Object 0; the evicted group 0 never appears anywhere, and
     * group 2's Object 0 carries the retained history. Eviction is counted. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.want_sap = true;
        g_srv.sap_subscribe_after = 3;
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_sap(s, 2, &v);   /* history = 2 groups */
        MOQ_TEST_CHECK(wait_ready(s, 300));

        const uint64_t pts[3] = { 0, 4000000, 8000000 };
        for (int gi = 0; gi < 3; gi++) {
            moq_rcbuf_t *b = mkbuf(16, (uint8_t)gi);
            moq_media_send_object_t o =
                mkobj_sap(b, true, true, false, MOQ_SAP_TYPE_2, pts[gi]);
            MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        }

        for (int i = 0; i < 200 && !srv_sap_has_obj0(&g_srv, 2); i++)
            usleep(50000);
        MOQ_TEST_CHECK(srv_sap_has_obj0(&g_srv, 2));

        /* Retained groups each got an Object 0; the evicted group 0 did not. */
        MOQ_TEST_CHECK(srv_sap_has_obj0(&g_srv, 1));
        MOQ_TEST_CHECK(!srv_sap_has_obj0(&g_srv, 0));
        MOQ_TEST_CHECK(srv_sap_obj0_has(&g_srv, 2, "\"l\":[1,0]", "\"l\":[2,0]"));
        MOQ_TEST_CHECK(!srv_sap_any_contains(&g_srv, "\"l\":[0,"));  /* evicted */

        moq_media_sender_stats_t stx;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_get_stats(s, &stx, sizeof(stx)), (int)MOQ_OK);
        MOQ_TEST_CHECK(stx.sap_records_evicted >= 1);
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == live loopback: a pre-ready removed track is not in the initial
     *    catalog (nor registered as a publisher track) ===================== */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);                      /* "v" */
        moq_media_track_cfg_t kc;
        moq_media_track_cfg_init(&kc);
        kc.name = MOQ_BYTES_LITERAL("keep");
        kc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        kc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        kc.codec = MOQ_BYTES_LITERAL("opus");
        kc.bitrate = 32000;               /* MSF-01 5.2.22 */
        kc.samplerate = 48000;
        kc.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *keep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &kc, &keep),
                              (int)MOQ_OK);
        /* Remove "v" before ready. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);
        /* The initial catalog has "keep" but not "v". */
        uint64_t g = 0, o = 0;
        MOQ_TEST_CHECK(gen_has(0, "keep", &g, &o));
        MOQ_TEST_CHECK_EQ_U64(g, 0);
        MOQ_TEST_CHECK_EQ_U64(o, 0);
        MOQ_TEST_CHECK(!gen_has(0, "v", NULL, NULL));
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == live loopback: post-ready name reuse (remove "v", add "v") ======== *
     * The old publisher track must be ended + removed so the new same-name
     * track registers without the sender going fatal. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);
        moq_media_track_cfg_t kc;
        moq_media_track_cfg_init(&kc);
        kc.name = MOQ_BYTES_LITERAL("keep");
        kc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        kc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        kc.codec = MOQ_BYTES_LITERAL("opus");
        kc.bitrate = 32000;               /* MSF-01 5.2.22 */
        kc.samplerate = 48000;
        kc.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *keep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &kc, &keep),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);   /* gen0 {v,keep} */
        MOQ_TEST_CHECK(gen_has(0, "v", NULL, NULL));

        /* Remove "v": a generation without it. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v), (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(2, 200) >= 2);
        MOQ_TEST_CHECK(!gen_has(1, "v", NULL, NULL));
        MOQ_TEST_CHECK(gen_has(1, "keep", NULL, NULL));
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        /* Re-add "v" (with different attributes): the old pub track is torn
         * down, the new one registers, the sender does NOT go fatal, and "v"
         * reappears in a LATER gen than the removal one (gen 1 above had no "v",
         * so the receiver sees remove-then-add, not a changed tuple). The add is
         * retryable (MOQ_ERR_WOULD_BLOCK) until the old teardown completes. */
        moq_media_track_cfg_t vc;
        moq_media_track_cfg_init(&vc);
        vc.name = MOQ_BYTES_LITERAL("v");
        vc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        vc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        vc.is_live = true;
        vc.codec = MOQ_BYTES_LITERAL("avc1.42e01e");   /* differs from old "v" */
        vc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        moq_media_track_t *v2 = NULL;
        moq_result_t addrc = MOQ_ERR_WOULD_BLOCK;
        for (int i = 0; i < 200 && addrc == MOQ_ERR_WOULD_BLOCK; i++) {
            addrc = moq_media_sender_add_track(s, &vc, &v2);
            if (addrc == MOQ_ERR_WOULD_BLOCK) usleep(20000);
        }
        MOQ_TEST_CHECK_EQ_INT((int)addrc, (int)MOQ_OK);
        MOQ_TEST_CHECK(v2 != NULL && v2 != v);
        MOQ_TEST_CHECK(wait_cat_gens(3, 200) >= 3);
        MOQ_TEST_CHECK(gen_has(2, "v", NULL, NULL));
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        /* Writes on the old handle are refused; the new handle accepts. */
        moq_rcbuf_t *b1 = mkbuf(8, 1);
        moq_media_send_object_t o1 = mkobj(b1, true, true, true);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o1),
                              (int)MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(b1);
        moq_rcbuf_t *b2 = mkbuf(8, 2);
        moq_media_send_object_t o2 = mkobj(b2, true, true, true);
        moq_result_t wr = moq_media_sender_write(s, v2, &o2);
        if (wr != MOQ_OK) moq_rcbuf_decref(b2);
        MOQ_TEST_CHECK_EQ_INT((int)wr, (int)MOQ_OK);

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == live loopback: same-name reuse WITHOUT waiting for the removal gen === *
     * Retry add("v") immediately after remove("v") (the app does not wait for
     * the removal generation). The replacement must still not be lost: the
     * generations are {v,keep} -> {keep} -> {keep,v'}, even though the re-add can
     * land between the hook's teardown and its republish. Registration of the
     * replacement must (re)dirty the catalog so its generation is published. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);
        moq_media_track_cfg_t kc;
        moq_media_track_cfg_init(&kc);
        kc.name = MOQ_BYTES_LITERAL("keep");
        kc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        kc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        kc.codec = MOQ_BYTES_LITERAL("opus");
        kc.bitrate = 32000;               /* MSF-01 5.2.22 */
        kc.samplerate = 48000;
        kc.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *keep = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &kc, &keep),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);   /* gen0 {v,keep} */
        MOQ_TEST_CHECK(gen_has(0, "v", NULL, NULL));

        /* Remove, then immediately retry add (no wait for the removal gen). */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v), (int)MOQ_OK);
        moq_media_track_cfg_t vc;
        moq_media_track_cfg_init(&vc);
        vc.name = MOQ_BYTES_LITERAL("v");
        vc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        vc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        vc.is_live = true;
        vc.codec = MOQ_BYTES_LITERAL("avc1.42e01e");
        vc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        moq_media_track_t *v2 = NULL;
        moq_result_t addrc = MOQ_ERR_WOULD_BLOCK;
        for (int i = 0; i < 300 && addrc == MOQ_ERR_WOULD_BLOCK; i++) {
            addrc = moq_media_sender_add_track(s, &vc, &v2);
            if (addrc == MOQ_ERR_WOULD_BLOCK) usleep(10000);
        }
        MOQ_TEST_CHECK_EQ_INT((int)addrc, (int)MOQ_OK);
        MOQ_TEST_CHECK(v2 != NULL && v2 != v);

        /* The replacement must appear (not lost) in a generation after a
         * removal generation that lacked "v". */
        MOQ_TEST_CHECK(wait_cat_gens(3, 300) >= 3);
        MOQ_TEST_CHECK(gen_has(0, "v", NULL, NULL));
        MOQ_TEST_CHECK(!gen_has(1, "v", NULL, NULL));
        MOQ_TEST_CHECK(gen_has(1, "keep", NULL, NULL));
        MOQ_TEST_CHECK(gen_has(2, "v", NULL, NULL));
        MOQ_TEST_CHECK(gen_has(2, "keep", NULL, NULL));
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        /* Old handle stays inert. */
        moq_rcbuf_t *b = mkbuf(8, 1);
        moq_media_send_object_t o = mkobj(b, true, true, true);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &o),
                              (int)MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(b);

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == live loopback: post-ready add/remove republish catalog generations = *
     * Initial catalog is group 0. A post-ready add cuts a new group whose
     * object 0 is the prior independent base and object 1 is a deltaUpdate add;
     * a post-ready remove cuts the next group with a deltaUpdate remove. Each
     * generation is verified by RECONSTRUCTING the group (object 0 + deltas) the
     * way the receiver does; group ids increase by one. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);
        MOQ_TEST_CHECK(wait_ready(s, 300));

        /* gen 0: [0,0] with "v". */
        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);
        uint64_t g = 0, o = 0;
        MOQ_TEST_CHECK(gen_has(0, "v", &g, &o));
        MOQ_TEST_CHECK_EQ_U64(g, 0);
        MOQ_TEST_CHECK_EQ_U64(o, 0);

        /* post-ready add "late": gen 1 at [1,0] with "v" + "late". */
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("late");
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("opus");
        tc.bitrate = 32000;               /* MSF-01 5.2.22 */
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *late = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &late),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(2, 200) >= 2);
        MOQ_TEST_CHECK(gen_has(1, "late", &g, &o));
        MOQ_TEST_CHECK_EQ_U64(g, 1);
        MOQ_TEST_CHECK_EQ_U64(o, 0);
        MOQ_TEST_CHECK(gen_has(1, "v", NULL, NULL));

        /* post-ready remove "late": gen 2 at [2,0] without "late". */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, late),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(3, 200) >= 3);
        MOQ_TEST_CHECK(gen_has(2, "v", &g, &o));
        MOQ_TEST_CHECK_EQ_U64(g, 2);
        MOQ_TEST_CHECK_EQ_U64(o, 0);
        MOQ_TEST_CHECK(!gen_has(2, "late", NULL, NULL));

        /* A write to the removed track is refused. */
        moq_rcbuf_t *b = mkbuf(8, 1);
        moq_media_send_object_t wo = mkobj(b, true, true, true);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, late, &wo),
                              (int)MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(b);

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* Note: a late joiner retrieving the latest retained catalog uses a Joining
     * FETCH(offset=0) (MSF-01 §5), which is the receiver's path and is covered
     * by the receiver tests against a retained-group-publishing server. The
     * sender's republish updates that retained group to the latest generation on every
     * mutation (sender_republish_catalog); the live-write placement above proves
     * the per-generation [group,0] mapping. */

    /* == complete(): terminal catalog generation + post-complete WRONG_STATE = *
     * gen0 [0,0] has "v" + its generated "v.sap"; complete() cuts a terminal
     * generation at [1,0] with isComplete:true + empty tracks (both the media
     * track and the SAP-timeline sibling gone), and every post-complete op is
     * WRONG_STATE while a repeat complete() is an idempotent OK that cuts no new
     * generation. The receiver-side catalog_complete latch + TRACK_ENDED mapping
     * are covered by the V2 receiver tests. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_sap(s, 0, &v);   /* media "v" + generated "v.sap" */
        MOQ_TEST_CHECK(wait_ready(s, 300));

        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);
        MOQ_TEST_CHECK(gen_has(0, "v", NULL, NULL));
        MOQ_TEST_CHECK(gen_has(0, "v.sap", NULL, NULL));

        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(s), (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(2, 200) >= 2);
        uint64_t g = 0, o = 0;
        MOQ_TEST_CHECK(gen_is_terminal(1, &g, &o));   /* isComplete + 0 tracks */
        MOQ_TEST_CHECK_EQ_U64(g, 1);
        MOQ_TEST_CHECK_EQ_U64(o, 0);
        MOQ_TEST_CHECK(!gen_has(1, "v", NULL, NULL));
        MOQ_TEST_CHECK(!gen_has(1, "v.sap", NULL, NULL));

        /* post-complete: write/end/remove/add all WRONG_STATE. */
        moq_rcbuf_t *b = mkbuf(8, 1);
        moq_media_send_object_t wo = mkobj(b, true, true, true);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &wo),
                              (int)MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(b);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_end_track(s, v),
                              (int)MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, v),
                              (int)MOQ_ERR_WRONG_STATE);
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("late");
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("opus");
        tc.bitrate = 32000;               /* MSF-01 5.2.22 */
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *late = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &late),
                              (int)MOQ_ERR_WRONG_STATE);
        /* repeat complete() is idempotent OK and cuts no new generation. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(s), (int)MOQ_OK);
        usleep(150000);
        MOQ_TEST_CHECK_EQ_INT(wait_cat_gens(2, 1), 2);

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == VOD-from-start: a non-live track's trackDuration reaches the wire
     * catalog (the receiver surfaces it on desc -- covered by the V2 receiver
     * tests). ============================================================== */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("vod");
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("av01");
        tc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        tc.is_live = false;
        tc.has_track_duration = true;
        tc.track_duration_ms = 8072340;
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &v),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);
        MOQ_TEST_CHECK(gen_track_duration_is(0, "vod", 8072340));

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == S2b: a post-ready generation is object 0 (independent base) + delta
     * objects (removes then adds); reconstruct == current. ================= */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);
        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);

        const moq_alloc_t *al = moq_alloc_default();
        uint8_t buf[4096];

        /* post-ready add "late" -> group 1 = [obj0 independent(v), obj1 ADD]. */
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        tc.name = MOQ_BYTES_LITERAL("late");
        tc.media_type = MOQ_MEDIA_TYPE_AUDIO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = MOQ_BYTES_LITERAL("opus");
        tc.bitrate = 32000;               /* MSF-01 5.2.22 */
        tc.samplerate = 48000;
        tc.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *late = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &late),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(2, 200) >= 2);
        MOQ_TEST_CHECK_EQ_INT(group_obj_count(1), 2);   /* dense 0..1 */
        /* object 0: independent base, has prior "v", not yet "late". */
        size_t l = cap_obj(1, 0, buf, sizeof(buf));
        MOQ_TEST_CHECK(l > 0);
        moq_msf_catalog_t c0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
            (moq_bytes_t){ buf, l }, &c0), (int)MOQ_OK);
        MOQ_TEST_CHECK(c0.delta_update == NULL);          /* independent */
        MOQ_TEST_CHECK(find_track(&c0, "v") != NULL);
        MOQ_TEST_CHECK(find_track(&c0, "late") == NULL);
        moq_msf_catalog_cleanup(al, &c0);
        /* object 1: deltaUpdate ADD carrying "late". */
        l = cap_obj(1, 1, buf, sizeof(buf));
        MOQ_TEST_CHECK(l > 0);
        moq_msf_catalog_t c1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
            (moq_bytes_t){ buf, l }, &c1), (int)MOQ_OK);
        MOQ_TEST_CHECK(c1.delta_update != NULL && c1.delta_update_count == 1 &&
                       c1.delta_update[0].op == MOQ_MSF_DELTA_OP_ADD);
        bool add_has_late = false;
        if (c1.delta_update)
            for (size_t i = 0; i < c1.delta_update[0].track_count; i++)
                if (bytes_eq(c1.delta_update[0].tracks[i].name, "late"))
                    add_has_late = true;
        MOQ_TEST_CHECK(add_has_late);
        moq_msf_catalog_cleanup(al, &c1);
        /* reconstruct group 1 == current (v + late). */
        MOQ_TEST_CHECK(gen_has(1, "v", NULL, NULL));
        MOQ_TEST_CHECK(gen_has(1, "late", NULL, NULL));

        /* post-ready remove "late" -> group 2 = [obj0 base, obj1 REMOVE]. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, late),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(3, 200) >= 3);
        MOQ_TEST_CHECK_EQ_INT(group_obj_count(2), 2);
        l = cap_obj(2, 1, buf, sizeof(buf));
        MOQ_TEST_CHECK(l > 0);
        moq_msf_catalog_t c2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
            (moq_bytes_t){ buf, l }, &c2), (int)MOQ_OK);
        MOQ_TEST_CHECK(c2.delta_update != NULL && c2.delta_update_count == 1 &&
                       c2.delta_update[0].op == MOQ_MSF_DELTA_OP_REMOVE);
        moq_msf_catalog_cleanup(al, &c2);
        MOQ_TEST_CHECK(gen_has(2, "v", NULL, NULL));
        MOQ_TEST_CHECK(!gen_has(2, "late", NULL, NULL));

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == S2b: a post-ready CMAF init-bearing add falls back to a FULL
     * independent generation (CMSF initDataList/initRef shape), NOT a delta with
     * inline initData -- otherwise the surviving track's init shape would differ
     * from the next independent catalog and the receiver would drop it as an
     * immutable-tuple change. ============================================== */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);                    /* LOC "v" */
        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);

        const moq_alloc_t *al = moq_alloc_default();
        uint8_t buf[4096];

        /* post-ready add CMAF "c" WITH a CMAF init segment. */
        uint8_t initb[256];
        size_t initn = build_cmaf_init(initb, 7);
        moq_media_track_cfg_t cc;
        moq_media_track_cfg_init(&cc);
        cc.name = MOQ_BYTES_LITERAL("c");
        cc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        cc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        cc.codec = MOQ_BYTES_LITERAL("avc1.42e01e");
        cc.bitrate = 1500000;             /* MSF-01 5.2.22 */
        cc.init_data = (moq_bytes_t){ initb, initn };
        moq_media_track_t *c = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &cc, &c),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(2, 200) >= 2);
        /* The generation is a SINGLE independent object (fallback), not a delta. */
        MOQ_TEST_CHECK_EQ_INT(group_obj_count(1), 1);
        size_t l = cap_obj(1, 0, buf, sizeof(buf));
        MOQ_TEST_CHECK(l > 0);
        moq_msf_catalog_t c0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
            (moq_bytes_t){ buf, l }, &c0), (int)MOQ_OK);
        MOQ_TEST_CHECK(c0.delta_update == NULL);             /* independent */
        const moq_msf_track_t *ct = find_track(&c0, "c");
        /* "c" carries the canonical CMSF init shape: initRef into a root
         * initDataList (NOT inline initData). */
        MOQ_TEST_CHECK(ct && ct->has_init_ref && !ct->has_init_data);
        MOQ_TEST_CHECK(c0.init_data_count > 0);
        MOQ_TEST_CHECK(find_track(&c0, "v") != NULL);        /* survivor */
        moq_msf_catalog_cleanup(al, &c0);

        /* A subsequent LOC add still uses the delta path; "c" survives with its
         * initRef shape unchanged (so a receiver would not drop it). */
        moq_media_track_cfg_t ac;
        moq_media_track_cfg_init(&ac);
        ac.name = MOQ_BYTES_LITERAL("a");
        ac.media_type = MOQ_MEDIA_TYPE_AUDIO;
        ac.packaging = MOQ_MEDIA_PACKAGING_RAW;
        ac.codec = MOQ_BYTES_LITERAL("opus");
        ac.bitrate = 32000;               /* MSF-01 5.2.22 */
        ac.samplerate = 48000;
        ac.channel_config = MOQ_BYTES_LITERAL("2");
        moq_media_track_t *a = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &ac, &a),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(3, 200) >= 3);
        MOQ_TEST_CHECK_EQ_INT(group_obj_count(2), 2);        /* base + delta add */
        /* group 2's base (object 0) carries "c" with the SAME initRef shape, so
         * a receiver adopting it after group 1 sees no immutable-tuple change. */
        l = cap_obj(2, 0, buf, sizeof(buf));
        MOQ_TEST_CHECK(l > 0);
        moq_msf_catalog_t b2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse(al,
            (moq_bytes_t){ buf, l }, &b2), (int)MOQ_OK);
        const moq_msf_track_t *ct2 = find_track(&b2, "c");
        MOQ_TEST_CHECK(ct2 && ct2->has_init_ref && !ct2->has_init_data);
        moq_msf_catalog_cleanup(al, &b2);
        MOQ_TEST_CHECK(gen_has(2, "v", NULL, NULL));
        MOQ_TEST_CHECK(gen_has(2, "c", NULL, NULL));
        MOQ_TEST_CHECK(gen_has(2, "a", NULL, NULL));

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == V4: live->VOD conversion publishes an independent generation with the
     * same tuple (isLive:false + trackDuration); post-convert writes refused;
     * repeat/foreign conversion rejected; complete() still works. ========== */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL, *a = NULL;
        add_video_track(s, &v);
        add_audio_track(s, &a);
        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);
        MOQ_TEST_CHECK(gen_has(0, "v", NULL, NULL));

        /* convert_to_vod a batch {v, a} -> one independent generation [1]. */
        moq_media_vod_track_t items[2] = { { v, 8072340 }, { a, 8072340 } };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_convert_to_vod(s, items, 2),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(2, 200) >= 2);
        MOQ_TEST_CHECK_EQ_INT(group_obj_count(1), 1);   /* independent generation */
        /* both tracks now VOD (isLive:false + trackDuration), same tuples. */
        MOQ_TEST_CHECK(gen_track_duration_is(1, "v", 8072340));
        MOQ_TEST_CHECK(gen_track_duration_is(1, "a", 8072340));
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        /* a write to a converted track is refused (no new content). */
        moq_rcbuf_t *b = mkbuf(8, 1);
        moq_media_send_object_t wo = mkobj(b, true, true, true);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_write(s, v, &wo),
                              (int)MOQ_ERR_WRONG_STATE);
        moq_rcbuf_decref(b);

        /* repeat/duration-mutation conversion rejected (already converted). */
        moq_media_vod_track_t again = { v, 9999 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_convert_to_vod(s, &again, 1),
                              (int)MOQ_ERR_INVAL);
        /* empty batch and foreign handle rejected. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_convert_to_vod(s, items, 0),
                              (int)MOQ_ERR_INVAL);
        moq_media_vod_track_t foreign = { (moq_media_track_t *)(uintptr_t)0x1, 1 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_convert_to_vod(s, &foreign, 1),
                              (int)MOQ_ERR_INVAL);

        /* complete() still works after conversion (permanent terminate). */
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_complete(s), (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(3, 200) >= 3);
        MOQ_TEST_CHECK(gen_is_terminal(2, NULL, NULL));   /* isComplete + empty */
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == full §11.3 live->VOD: convert_to_vod sends Track Ended (status 0x2) to
     * active media subscribers, THEN publishes the independent VOD catalog; the
     * tracks stay registered (not END_OF_TRACK), so they remain joinable. ==== */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.want_a = true;          /* subscribe both "v" and "a" */
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL, *a = NULL;
        add_video_track(s, &v);
        add_audio_track(s, &a);
        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(wait_cat_gens(1, 200) >= 1);

        /* Keep writing live objects to both tracks until the server (a
         * LARGEST_OBJECT subscriber) has actually received some -- that confirms
         * each subscription is ACTIVE on the publisher before we convert, so the
         * finish step has a real subscriber to send Track Ended to. */
        for (int i = 0; i < 200 &&
             (srv_count(&g_srv) < 1 || srv_a_count(&g_srv) < 1); i++) {
            /* write() takes ownership of the payload on success; only the
             * caller's ref is dropped on failure (matches the live-loopback
             * write pattern above). */
            moq_rcbuf_t *bv = mkbuf(8, (uint8_t)(i + 1));
            moq_media_send_object_t ov = mkobj(bv, true, true, true);
            if (moq_media_sender_write(s, v, &ov) != MOQ_OK) moq_rcbuf_decref(bv);
            moq_rcbuf_t *ba = mkbuf(8, (uint8_t)(i + 1));
            moq_media_send_object_t oa = mkobj(ba, true, true, true);
            if (moq_media_sender_write(s, a, &oa) != MOQ_OK) moq_rcbuf_decref(ba);
            usleep(50000);
        }
        MOQ_TEST_CHECK(srv_count(&g_srv) >= 1);
        MOQ_TEST_CHECK(srv_a_count(&g_srv) >= 1);

        /* Convert both -> step 1 Track Ended to active subs, then step 2 the
         * independent VOD catalog generation [1] (same tuples). */
        moq_media_vod_track_t items[2] = { { v, 5000 }, { a, 7000 } };
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_convert_to_vod(s, items, 2),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(wait_cat_gens(2, 300) >= 2);
        MOQ_TEST_CHECK_EQ_INT(group_obj_count(1), 1);    /* independent */
        MOQ_TEST_CHECK(gen_track_duration_is(1, "v", 5000));
        MOQ_TEST_CHECK(gen_track_duration_is(1, "a", 7000));

        /* Both media subscribers received exactly one Track Ended (status 0x2). */
        int vd = 0, ad = 0; uint64_t vs = 0, as = 0;
        for (int i = 0; i < 200; i++) {
            srv_dones(&g_srv, &vd, &vs, &ad, &as);
            if (vd >= 1 && ad >= 1) break;
            usleep(50000);
        }
        MOQ_TEST_CHECK_EQ_INT(vd, 1);
        MOQ_TEST_CHECK_EQ_U64(vs, 0x2u);                 /* Track Ended */
        MOQ_TEST_CHECK_EQ_INT(ad, 1);
        MOQ_TEST_CHECK_EQ_U64(as, 0x2u);

        /* Strict ordering (§11.3): both Track Ended signals were observed BEFORE
         * the conversion catalog generation [1]. The pump gates the catalog
         * republish on the finish, so the dones are emitted first; on the
         * in-order loopback that ordering is preserved at the subscriber. */
        uint64_t v_seq = 0, a_seq = 0;
        srv_done_seqs(&g_srv, &v_seq, &a_seq);
        uint64_t conv_seq = cat_gen_seq(1);
        MOQ_TEST_CHECK(v_seq != 0 && a_seq != 0 && conv_seq != 0);
        MOQ_TEST_CHECK(v_seq < conv_seq);
        MOQ_TEST_CHECK(a_seq < conv_seq);

        /* Not terminal: no END_OF_TRACK was emitted, and the VOD catalog still
         * advertises both tracks -- they stay joinable (retained replay is the
         * publisher's job, exercised in the publisher tests, not re-tested here). */
        MOQ_TEST_CHECK_EQ_INT(srv_v_eot(&g_srv), 0);
        MOQ_TEST_CHECK(gen_has(1, "v", NULL, NULL));
        MOQ_TEST_CHECK(gen_has(1, "a", NULL, NULL));
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == media timeline (MSF §7.1.1/§7.3): one record per group-start, each
     *    group boundary emits its own independent Object 0 with all retained
     *    records, decoded back via the §7.1.1 codec. ====================== */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.want_timeline = true;     /* subscribe "v.timeline" immediately */
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_timeline(s, 0, true, 0, &v);   /* live */
        MOQ_TEST_CHECK(wait_ready(s, 300));

        /* Three single-object groups with distinct presentation times. The
         * middle pts carries a 700us remainder: the MSF media-timeline mediaTime
         * is floor-integral ms (7), so it MUST floor to 2002 -- never round to
         * 2003 the way the CMSF SAP EPT does. */
        const uint64_t pts[3] = { 0, 2002700, 4004000 };
        for (int gi = 0; gi < 3; gi++) {
            moq_rcbuf_t *b = mkbuf(16, (uint8_t)gi);
            moq_media_send_object_t o = mkobj(b, true, true, true);
            o.presentation_time_us = pts[gi];
            MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        }

        for (int i = 0; i < 200 && !srv_tl_has_obj0(&g_srv, 2); i++)
            usleep(50000);

        /* Each media-group boundary emitted its own independent Object 0. */
        MOQ_TEST_CHECK(srv_tl_has_obj0(&g_srv, 0));
        MOQ_TEST_CHECK(srv_tl_has_obj0(&g_srv, 1));
        MOQ_TEST_CHECK(srv_tl_has_obj0(&g_srv, 2));

        /* group 0's Object 0 carries exactly its one record. */
        moq_msf_media_timeline_record_t recs[8];
        int n = srv_tl_decode_obj0(&g_srv, 0, recs, 8);
        MOQ_TEST_CHECK_EQ_INT(n, 1);
        if (n == 1) {
            MOQ_TEST_CHECK_EQ_U64(recs[0].media_time_ms, 0);
            MOQ_TEST_CHECK_EQ_U64(recs[0].group, 0);
            MOQ_TEST_CHECK_EQ_U64(recs[0].object, 0);
            MOQ_TEST_CHECK(recs[0].wallclock_ms != 0);   /* live => emit time */
        }

        /* group 2's Object 0 accumulates all three records, in order. */
        n = srv_tl_decode_obj0(&g_srv, 2, recs, 8);
        MOQ_TEST_CHECK_EQ_INT(n, 3);
        if (n == 3) {
            MOQ_TEST_CHECK_EQ_U64(recs[0].media_time_ms, 0);
            MOQ_TEST_CHECK_EQ_U64(recs[0].group, 0);
            MOQ_TEST_CHECK_EQ_U64(recs[1].media_time_ms, 2002);
            MOQ_TEST_CHECK_EQ_U64(recs[1].group, 1);
            MOQ_TEST_CHECK_EQ_U64(recs[1].object, 0);
            MOQ_TEST_CHECK_EQ_U64(recs[2].media_time_ms, 4004);
            MOQ_TEST_CHECK_EQ_U64(recs[2].group, 2);
            MOQ_TEST_CHECK_EQ_U64(recs[2].object, 0);
            MOQ_TEST_CHECK(recs[2].wallclock_ms != 0);
        }
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == media timeline: late subscriber + bounded-history eviction ====== *
     * History bound = 2 groups; "v.timeline" subscribes only after 3 media
     * objects (groups). Retained groups 1 and 2 each emit Object 0; evicted
     * group 0 never appears, and group 2's Object 0 carries [1,0] and [2,0]. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.want_timeline = true;
        g_srv.timeline_subscribe_after = 3;
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_timeline(s, 2, true, 0, &v);   /* history = 2 groups */
        MOQ_TEST_CHECK(wait_ready(s, 300));

        const uint64_t pts[3] = { 0, 2002000, 4004000 };
        for (int gi = 0; gi < 3; gi++) {
            moq_rcbuf_t *b = mkbuf(16, (uint8_t)gi);
            moq_media_send_object_t o = mkobj(b, true, true, true);
            o.presentation_time_us = pts[gi];
            MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        }

        for (int i = 0; i < 200 && !srv_tl_has_obj0(&g_srv, 2); i++)
            usleep(50000);
        MOQ_TEST_CHECK(srv_tl_has_obj0(&g_srv, 2));
        MOQ_TEST_CHECK(srv_tl_has_obj0(&g_srv, 1));
        MOQ_TEST_CHECK(!srv_tl_has_obj0(&g_srv, 0));   /* evicted */

        /* group 2's Object 0 = the two retained groups (1,2), not group 0. */
        moq_msf_media_timeline_record_t recs[8];
        int n = srv_tl_decode_obj0(&g_srv, 2, recs, 8);
        MOQ_TEST_CHECK_EQ_INT(n, 2);
        if (n == 2) {
            MOQ_TEST_CHECK_EQ_U64(recs[0].group, 1);
            MOQ_TEST_CHECK_EQ_U64(recs[1].group, 2);
        }
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == media timeline on a non-live (VOD) track: wallclock is 0 ======== */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.want_timeline = true;
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s), (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track_timeline(s, 0, false, 8072340, &v);   /* VOD */
        MOQ_TEST_CHECK(wait_ready(s, 300));

        for (int gi = 0; gi < 2; gi++) {
            moq_rcbuf_t *b = mkbuf(16, (uint8_t)gi);
            moq_media_send_object_t o = mkobj(b, true, true, true);
            o.presentation_time_us = (uint64_t)gi * 2002000ull;
            MOQ_TEST_CHECK_EQ_INT((int)sap_write(s, v, &o), (int)MOQ_OK);
        }

        for (int i = 0; i < 200 && !srv_tl_has_obj0(&g_srv, 1); i++)
            usleep(50000);
        MOQ_TEST_CHECK(srv_tl_has_obj0(&g_srv, 1));

        moq_msf_media_timeline_record_t recs[8];
        int n = srv_tl_decode_obj0(&g_srv, 1, recs, 8);
        MOQ_TEST_CHECK_EQ_INT(n, 2);
        if (n == 2) {
            MOQ_TEST_CHECK_EQ_U64(recs[0].wallclock_ms, 0);   /* VOD => 0 */
            MOQ_TEST_CHECK_EQ_U64(recs[1].wallclock_ms, 0);
            MOQ_TEST_CHECK_EQ_U64(recs[1].media_time_ms, 2002);
        }
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == demand visibility (§7.2): join/left callbacks + queries ========= *
     * 1. ready + catalog-only subscriber => has_media_subscriber is false (4).
     * 2. media "v" subscribe => join callback (v, count 1), queries flip true.
     * 3. queued-before-demand media drains once "v" joins (5).
     * 4. "v" unsubscribe => left callback (v, count 0), queries return false. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        memset(&g_demand, 0, sizeof(g_demand));
        g_srv.no_subscribe = true;        /* catalog-only to start; gate "v" */
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_callbacks_init(&cfg.callbacks);
        cfg.callbacks.ctx = &g_demand;
        cfg.callbacks.on_subscriber_joined = demand_on_join;
        cfg.callbacks.on_subscriber_left = demand_on_left;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);

        MOQ_TEST_CHECK(wait_ready(s, 300));
        MOQ_TEST_CHECK(moq_media_sender_is_ready(s));
        /* Let the peer subscribe + fetch the catalog (no media sub yet). */
        for (int i = 0; i < 60 && !g_srv.cat_subscribed; i++) usleep(50000);
        /* (1)+(4): ready, catalog subscribed, but no media demand. */
        MOQ_TEST_CHECK(!moq_media_sender_has_media_subscriber(s));
        MOQ_TEST_CHECK(!moq_media_sender_track_has_subscriber(s, v));
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_track_subscriptions(s, v), 0);
        MOQ_TEST_CHECK(atomic_load(&g_demand.joins) == 0);

        /* (3) queue a GOP BEFORE any media subscriber exists. */
        for (int i = 0; i < 3; i++) {
            moq_rcbuf_t *b = mkbuf(16, (uint8_t)i);
            moq_media_send_object_t o = mkobj(b, i == 0, i == 0, i == 2);
            o.presentation_time_us = (uint64_t)(i + 1) * 1000u;
            moq_result_t rc = moq_media_sender_write(s, v, &o);
            if (rc != MOQ_OK) moq_rcbuf_decref(b);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }

        /* (2) allow the peer to subscribe "v": demand appears. */
        g_srv.no_subscribe = false;
        for (int i = 0; i < 200 && atomic_load(&g_demand.joins) == 0; i++)
            usleep(50000);
        MOQ_TEST_CHECK(atomic_load(&g_demand.joins) == 1);
        MOQ_TEST_CHECK(g_demand.last_join_track == v);   /* exact media handle */
        MOQ_TEST_CHECK_EQ_INT(atomic_load(&g_demand.last_join_count), 1);
        MOQ_TEST_CHECK(moq_media_sender_has_media_subscriber(s));
        MOQ_TEST_CHECK(moq_media_sender_track_has_subscriber(s, v));
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_track_subscriptions(s, v), 1);

        /* (3) the pre-demand GOP now drains to the subscriber. */
        for (int i = 0; i < 200 && srv_count(&g_srv) < 3; i++) usleep(50000);
        MOQ_TEST_CHECK(srv_count(&g_srv) >= 3);

        /* (4) the peer unsubscribes "v": demand goes away. */
        atomic_store(&g_srv.unsub_v_request, true);
        for (int i = 0; i < 200 && atomic_load(&g_demand.leaves) == 0; i++)
            usleep(50000);
        MOQ_TEST_CHECK(atomic_load(&g_demand.leaves) == 1);
        MOQ_TEST_CHECK(g_demand.last_left_track == v);
        MOQ_TEST_CHECK_EQ_INT(atomic_load(&g_demand.last_left_count), 0);
        MOQ_TEST_CHECK(!moq_media_sender_has_media_subscriber(s));
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_track_subscriptions(s, v), 0);
        MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == demand: subscription cleared WITHOUT an UNSUBSCRIBE (local finish) = *
     * convert_to_vod finishes active subscribers (Track Ended via
     * moq_pub_finish_subscribers) -- the publisher clears the slot WITHOUT a
     * left callback (the gap the old increment/decrement mirror missed). Demand
     * is reconciled by polling the authoritative count, so has_media_subscriber
     * must still drop and the service left callback still fire. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        memset(&g_demand, 0, sizeof(g_demand));
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_callbacks_init(&cfg.callbacks);
        cfg.callbacks.ctx = &g_demand;
        cfg.callbacks.on_subscriber_joined = demand_on_join;
        cfg.callbacks.on_subscriber_left = demand_on_left;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);
        MOQ_TEST_CHECK(wait_ready(s, 300));

        /* The peer subscribes "v" (default). */
        for (int i = 0; i < 200 && atomic_load(&g_demand.joins) == 0; i++)
            usleep(50000);
        MOQ_TEST_CHECK(atomic_load(&g_demand.joins) == 1);
        MOQ_TEST_CHECK(moq_media_sender_has_media_subscriber(s));

        /* Finish the subscriber with no UNSUBSCRIBE: resync must see the drop. */
        moq_media_vod_track_t item = { .track = v, .duration_ms = 5000 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_sender_convert_to_vod(s, &item, 1), (int)MOQ_OK);
        for (int i = 0; i < 200 && atomic_load(&g_demand.leaves) == 0; i++)
            usleep(50000);
        MOQ_TEST_CHECK(atomic_load(&g_demand.leaves) == 1);
        MOQ_TEST_CHECK(g_demand.last_left_track == v);
        MOQ_TEST_CHECK_EQ_INT(atomic_load(&g_demand.last_left_count), 0);
        MOQ_TEST_CHECK(!moq_media_sender_has_media_subscriber(s));
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_track_subscriptions(s, v), 0);

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == demand visibility: ABI -- old/zero callbacks still work ========== *
     * (a) a caller built before `callbacks` sets struct_size to the
     *     pre-callbacks prefix; create accepts it (callbacks absent).
     * (b) a caller with the full outer cfg but callbacks.struct_size == 0 is
     *     treated as no callbacks (prefix-safe nested copy).
     * Each variant uses its own server (fresh server state). */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.no_subscribe = true;
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        /* (a) Simulate an older caller whose struct predates `callbacks`. */
        cfg.struct_size = (uint32_t)offsetof(moq_media_sender_cfg_t, callbacks);
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        if (s) {
            moq_media_track_t *v = NULL;
            add_video_track(s, &v);
            MOQ_TEST_CHECK(wait_ready(s, 300));
            MOQ_TEST_CHECK(!moq_media_sender_has_media_subscriber(s));
            MOQ_TEST_CHECK_EQ_U64(
                moq_media_sender_track_subscriptions(s, v), 0);
            moq_media_sender_destroy(s);
        }
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        g_srv.no_subscribe = true;
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        /* (b) full outer cfg, callbacks.struct_size == 0 -> no callbacks. */
        memset(&cfg.callbacks, 0, sizeof(cfg.callbacks));
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        if (s) {
            MOQ_TEST_CHECK(wait_ready(s, 300));
            MOQ_TEST_CHECK(!moq_media_sender_has_media_subscriber(s));
            moq_media_sender_destroy(s);
        }
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    /* == demand: graceful subscriber CONNECTION_CLOSE (no UNSUBSCRIBE) ==== *
     * The subscriber sends GOAWAY and, after a short bounded drain
     * (goaway_timeout_us), force-closes the transport -- the publisher observes
     * SESSION_CLOSED PROMPTLY (no QUIC idle-timeout wait). Demand resync must
     * drop to 0 and fire on_subscriber_left once. Deterministic: a few pump
     * cycles + ~50ms drain, no idle-timeout sleep. */
    {
        int port = 0;
        memset(&g_srv, 0, sizeof(g_srv));
        memset(&g_demand, 0, sizeof(g_demand));
        g_srv.goaway_timeout_us = 50000;   /* 50ms bounded GOAWAY drain */
        moq_pq_threaded_t *srv = start_server(cert, key, &g_srv, &port);
        MOQ_TEST_CHECK(srv != NULL);
        if (!srv) return 1;
        char url[64];
        moq_endpoint_cfg_t ec = ep_cfg(url, sizeof(url), port);
        moq_bytes_t parts[2];
        moq_media_sender_cfg_t cfg;
        fill_cfg(&cfg, parts);
        cfg.endpoint = &ec;
        moq_media_sender_callbacks_init(&cfg.callbacks);
        cfg.callbacks.ctx = &g_demand;
        cfg.callbacks.on_subscriber_joined = demand_on_join;
        cfg.callbacks.on_subscriber_left = demand_on_left;
        moq_media_sender_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_create(&cfg, &s),
                              (int)MOQ_OK);
        moq_media_track_t *v = NULL;
        add_video_track(s, &v);
        MOQ_TEST_CHECK(wait_ready(s, 300));

        /* Peer subscribes "v". */
        for (int i = 0; i < 200 && atomic_load(&g_demand.joins) == 0; i++)
            usleep(50000);
        MOQ_TEST_CHECK(atomic_load(&g_demand.joins) == 1);
        MOQ_TEST_CHECK(moq_media_sender_has_media_subscriber(s));

        /* Peer gracefully closes (GOAWAY + 50ms bounded drain). The poll exits
         * the instant `left` fires (~tens of ms after the drain); its 6s ceiling
         * (120 * 50ms) is only a backstop -- if this needed the QUIC idle
         * timeout instead, it would take ~29s. */
        atomic_store(&g_srv.goaway_request, true);
        for (int i = 0; i < 120 && atomic_load(&g_demand.leaves) == 0; i++) {
            moq_pq_threaded_wake(srv);   /* nudge the idle server pump to GOAWAY */
            usleep(50000);
        }
        MOQ_TEST_CHECK(atomic_load(&g_srv.goaway_sent));  /* GOAWAY path exercised */
        MOQ_TEST_CHECK(atomic_load(&g_demand.leaves) == 1);
        MOQ_TEST_CHECK(g_demand.last_left_track == v);
        MOQ_TEST_CHECK_EQ_INT(atomic_load(&g_demand.last_left_count), 0);
        MOQ_TEST_CHECK(!moq_media_sender_has_media_subscriber(s));
        MOQ_TEST_CHECK_EQ_U64(moq_media_sender_track_subscriptions(s, v), 0);

        moq_media_sender_destroy(s);
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }

    MOQ_TEST_PASS("media_sender");
    return failures != 0;
}
