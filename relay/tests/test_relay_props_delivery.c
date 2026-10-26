/* Object Properties delivered across drafts, by delivery family, and what a
 * destination that cannot carry them gets. A Key-Value-Pair's integers are
 * draft-16 (i) and draft-18 (vi64); a value of 1000 is 43 E8 in one and
 * 83 E8 in the other, and 2^62 is valid vi64 (FF 40 00 00 00 00 00 00 00)
 * that no draft-16 integer can hold. The origin pushes the track with
 * PUBLISH; subscribers on other connections read it on either draft.
 *
 *   live_datagram         d18 datagram -> d16 subscriber, converted
 *   live_unsupported      2^62 to a d16 subscriber: that subscription alone
 *                         ends (INTERNAL_ERROR); a d18 sibling and the d16
 *                         session go on
 *   live_datagram_unsupported  the same refusal on a datagram, also with
 *                         terminal-control backpressure
 *   fetch_convert         a retained FETCH from a d16 connection, converted
 *   fetch_unsupported     the same FETCH of a 2^62 Object is reset alone; a
 *                         live subscription on the same connection goes on
 *   fetch_oom             the conversion's allocation fails: that FETCH is
 *                         reset, nothing leaks, the session goes on
 *   ingest_malformed      a d16 Object whose Immutable Extensions nest an
 *                         Immutable entry (draft-16 §11.2) is refused where
 *                         the relay receives it: the relay cancels its
 *                         upstream request, d16 and d18 subscribers alike
 *                         end MALFORMED_TRACK with nothing delivered and no
 *                         conversion attempted, and another Track from the
 *                         same origin goes on
 *   fetch_after_malformed a FETCH of that Track afterwards gets no Object
 *   live_unsupported_blocked  the same, with the subscription's done held
 *                         back (WOULD_BLOCK) three times: it is retried and
 *                         sent exactly once, nothing of the Object first
 *   fetch_unsupported_cancel  the refused FETCH's reset and the fetcher's
 *                         FETCH_CANCEL cross: both sessions go on, the
 *                         live subscription beside it too, nothing leaks
 *   discovery_*           a draft-16 origin's Track, offered through a
 *                         draft-22 track subscription (the relay serves
 *                         track discovery on draft-22): the PUBLISH states
 *                         its Track Properties converted, or is not made at
 *                         all (PUBLISH_BLOCKED) when they cannot be stated,
 *                         while a sibling Track is still offered
 *   same_d18_d22          d18 -> d22 and d22 -> d18 share the vi64 codec:
 *                         delivered byte-exact, nothing converted
 *   embedder_stated       records an embedder appends through the core with
 *                         their codec stated (vi64): a d16 FETCH gets them
 *                         converted, a d18 FETCH byte-exact
 *   embedder_unknown      records appended with a descriptor that predates
 *                         the codec field (UNKNOWN): a nonempty block is
 *                         never passed through as if it matched, so each
 *                         FETCH is reset alone; an empty block is delivered
 *   join_convert          a Joining FETCH (the standalone retrieval path)
 *   converted_outlives    a converted block still owned by an unsent
 *                         session action when the binding is destroyed */
#include "relay_session_binding_harness.h"
#include <stddef.h>

static int failures;

static const uint8_t P16_1000[] = { 0x10, 0x43, 0xE8 };
static const uint8_t P18_1000[] = { 0x10, 0x83, 0xE8 };
/* Type 0x10 = 2^62 (vi64 nine-byte form), then Type 0x11 with a 37-byte
 * value: 49 bytes, a size no other allocation of these rows has. */
static uint8_t P18_HUGE[3 + 8 + 2 + 37];
static size_t P18_HUGE_LEN;
/* Type 0x10 = 1000, then Type 0x11 with 37 bytes: 42 bytes in either draft. */
static uint8_t P18_OOM[3 + 2 + 37];
/* d16: Immutable (0x0B), Length 3, holding Immutable (0x0B) Length 1 00. */
static const uint8_t P16_NESTED[] = { 0x0B, 0x03, 0x0B, 0x01, 0x00 };

static void
fixtures_init(void)
{
    size_t n = 0;
    P18_HUGE[n++] = 0x10; P18_HUGE[n++] = 0xFF; P18_HUGE[n++] = 0x40;
    for (int i = 0; i < 7; i++) P18_HUGE[n++] = 0x00;
    P18_HUGE[n++] = 0x01; P18_HUGE[n++] = 37;
    for (int i = 0; i < 37; i++) P18_HUGE[n++] = (uint8_t)(0xA0 + i);
    P18_HUGE_LEN = n;
    n = 0;
    P18_OOM[n++] = 0x10; P18_OOM[n++] = 0x83; P18_OOM[n++] = 0xE8;
    P18_OOM[n++] = 0x01; P18_OOM[n++] = 37;
    for (int i = 0; i < 37; i++) P18_OOM[n++] = (uint8_t)(0xA0 + i);
}

typedef struct obs {
    conn_t *cn;
    moq_subscription_t sub;
    moq_fetch_t fetch;
    bool sub_ok;
    int objects, datagrams, dones, fetch_ok, fetch_objects, fetch_complete,
        fetch_reset, fetch_error, closed;
    uint64_t done_status, fetch_code;
    uint8_t props[64], fprops[64];
    size_t props_len, fprops_len;
    uint64_t last_group;
    int tracks_ok, tracks_err, pub_reqs, blocked;
    uint64_t tracks_err_code;
    char pub_name[4][8], blocked_name[4][8];
    uint8_t pub_props[4][16];
    size_t pub_props_len[4];
} obs_t;

static void
obs_name(char *out, moq_bytes_t name)
{
    size_t n = name.len < 7 ? name.len : 7;
    memcpy(out, name.data, n);
    out[n] = '\0';
}

static void
obs_drain(obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(o->cn->peer, &ev, 1) == 1) {
        switch (ev.kind) {
        case MOQ_EVENT_SUBSCRIBE_OK:
            o->sub_ok = true;
            break;
        case MOQ_EVENT_OBJECT_RECEIVED: {
            const moq_object_received_event_t *e = &ev.u.object_received;
            o->objects++;
            o->datagrams += e->datagram;
            o->last_group = e->group_id;
            o->props_len = e->properties ? moq_rcbuf_len(e->properties) : 0;
            if (o->props_len && o->props_len <= sizeof(o->props))
                memcpy(o->props, moq_rcbuf_data(e->properties), o->props_len);
            break;
        }
        case MOQ_EVENT_SUBSCRIBE_DONE:
            o->dones++;
            o->done_status = ev.u.subscribe_done.status_code;
            break;
        case MOQ_EVENT_FETCH_OK:
            o->fetch_ok++;
            break;
        case MOQ_EVENT_FETCH_OBJECT: {
            const moq_fetch_object_event_t *e = &ev.u.fetch_object;
            o->fetch_objects++;
            o->fprops_len = e->properties ? moq_rcbuf_len(e->properties) : 0;
            if (o->fprops_len && o->fprops_len <= sizeof(o->fprops))
                memcpy(o->fprops, moq_rcbuf_data(e->properties), o->fprops_len);
            break;
        }
        case MOQ_EVENT_FETCH_COMPLETE:
            o->fetch_complete++;
            break;
        case MOQ_EVENT_FETCH_RESET:
            o->fetch_reset++;
            o->fetch_code = ev.u.fetch_reset.error_code;
            break;
        case MOQ_EVENT_FETCH_ERROR:
            o->fetch_error++;
            o->fetch_code = ev.u.fetch_error.error_code;
            break;
        case MOQ_EVENT_SESSION_CLOSED:
            o->closed++;
            break;
        case MOQ_EVENT_SUBSCRIBE_TRACKS_OK:
            o->tracks_ok++;
            break;
        case MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR:
            o->tracks_err++;
            o->tracks_err_code = ev.u.subscribe_tracks_error.error_code;
            break;
        case MOQ_EVENT_PUBLISH_REQUEST: {
            const moq_publish_request_event_t *pr = &ev.u.publish_request;
            int k = o->pub_reqs++;
            if (k < 4) {
                obs_name(o->pub_name[k], pr->track_name);
                o->pub_props_len[k] = pr->track_properties.len;
                if (pr->track_properties.len != 0 && pr->track_properties.len <= 16)
                    memcpy(o->pub_props[k], pr->track_properties.data,
                           pr->track_properties.len);
            }
            moq_accept_publish_cfg_t a;
            moq_accept_publish_cfg_init(&a);
            (void)moq_session_accept_publish(o->cn->peer, pr->pub, &a, 0);
            break;
        }
        case MOQ_EVENT_PUBLISH_BLOCKED: {
            int k = o->blocked++;
            if (k < 4) obs_name(o->blocked_name[k], ev.u.publish_blocked.track_name);
            break;
        }
        default:
            break;
        }
        moq_event_cleanup(&ev);
    }
}

typedef struct row {
    ca_t a;
    rig_t rig;
    conn_t *origin;
    moq_publication_t ph, ph2;
    int origin_unsubscribed;
    obs_t down[3];
    int n_down;
} row_t;

static void
row_cycle(row_t *w, int n)
{
    for (int i = 0; i < n; i++) {
        relay_bind_test_rig_pump(&w->rig, 1);
        moq_event_t ev;
        while (moq_session_poll_events(w->origin->peer, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_PUBLISH_UNSUBSCRIBED) w->origin_unsubscribed++;
            moq_event_cleanup(&ev);
        }
        for (int k = 0; k < w->n_down; k++) obs_drain(&w->down[k]);
    }
}

/* Track Properties the origin's PUBLISH of "t" states (empty: none). */
static moq_bytes_t g_row_track_props;

static bool
row_open(row_t *w, moq_version_t pub_ver, const moq_version_t *down_ver, int n_down)
{
    memset(w, 0, sizeof(*w));
    relay_bind_test_ca_init(&w->a);
    if (relay_bind_test_rig_create(&w->rig, &w->a) != MOQR_OK) return false;
    w->origin = relay_bind_test_rig_connect(&w->rig, pub_ver);
    w->n_down = n_down;
    for (int k = 0; k < n_down; k++) {
        w->down[k].cn = relay_bind_test_rig_connect(&w->rig, down_ver[k]);
        if (w->down[k].cn == NULL) return false;
    }
    if (w->origin == NULL) return false;
    relay_bind_test_rig_pump(&w->rig, 4);
    moq_bytes_t ns[1] = { B("pd") };
    moq_publish_namespace_cfg_t nc;
    moq_publish_namespace_cfg_init(&nc);
    nc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    moq_announcement_t ann;
    R_CHECK(&w->rig, moq_session_publish_namespace(w->origin->peer, &nc, w->rig.now,
                                                   &ann) == MOQ_OK);
    row_cycle(w, 6);
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    pc.track_name = B("t");
    pc.track_properties = g_row_track_props;
    R_CHECK(&w->rig, moq_session_publish(w->origin->peer, &pc, w->rig.now, &w->ph) == MOQ_OK);
    row_cycle(w, 6);
    return w->rig.failures == 0;
}

static void
row_subscribe_to(row_t *w, int k, moq_bytes_t track)
{
    moq_bytes_t ns[1] = { B("pd") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    sc.track_name = track;
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    R_CHECK(&w->rig, moq_session_subscribe(w->down[k].cn->peer, &sc, w->rig.now,
                                           &w->down[k].sub) == MOQ_OK);
    for (int i = 0; i < 16 && !w->down[k].sub_ok; i++) row_cycle(w, 1);
    R_CHECK(&w->rig, w->down[k].sub_ok);
}

static void
row_subscribe(row_t *w, int k)
{
    row_subscribe_to(w, k, B("t"));
}

/* A second Track "t2" pushed by the same origin. */
static void
row_publish_second(row_t *w)
{
    moq_bytes_t ns[1] = { B("pd") };
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    pc.track_name = B("t2");
    R_CHECK(&w->rig, moq_session_publish(w->origin->peer, &pc, w->rig.now, &w->ph2) == MOQ_OK);
    row_cycle(w, 6);
}

/* One Object (g, 0) on its own subgroup of `ph`, or as a datagram. */
static void
row_publish_on(row_t *w, moq_publication_t ph, uint64_t g, const uint8_t *props, size_t len,
               bool datagram)
{
    uint8_t body[4] = { 0xB0, (uint8_t)g, 0x5A, 0xA5 };
    moq_rcbuf_t *pl = NULL, *pr = NULL;
    R_CHECK(&w->rig, moq_rcbuf_create(&w->a.vt, body, sizeof(body), &pl) == MOQ_OK);
    if (datagram) {
        R_CHECK(&w->rig, moq_session_send_pub_object_datagram(
                             w->origin->peer, ph, g, 0, 0, false, pl, props, len,
                             w->rig.now) == MOQ_OK);
    } else {
        R_CHECK(&w->rig, moq_rcbuf_create(&w->a.vt, props, len, &pr) == MOQ_OK);
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = g;
        sgc.object_properties = true;
        moq_subgroup_handle_t sgh;
        R_CHECK(&w->rig, moq_session_open_pub_subgroup(w->origin->peer, ph, &sgc,
                                                       w->rig.now, &sgh) == MOQ_OK);
        moq_object_cfg_t oc;
        moq_object_cfg_init(&oc);
        oc.object_id = 0;
        oc.payload = pl;
        oc.properties = pr;
        R_CHECK(&w->rig, moq_session_write_object_ex(w->origin->peer, sgh, &oc,
                                                     w->rig.now) == MOQ_OK);
        R_CHECK(&w->rig, moq_session_close_subgroup(w->origin->peer, sgh, w->rig.now) ==
                             MOQ_OK);
        moq_rcbuf_decref(pr);
    }
    moq_rcbuf_decref(pl);
    row_cycle(w, 8);
}

static void
row_publish(row_t *w, uint64_t g, const uint8_t *props, size_t len, bool datagram)
{
    row_publish_on(w, w->ph, g, props, len, datagram);
}

static void
row_fetch(row_t *w, int k, uint64_t g, bool joining)
{
    moq_bytes_t ns[1] = { B("pd") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    if (joining) {
        fc.is_joining = true;
        fc.joining_relative = true;
        fc.joining_sub = w->down[k].sub;
        fc.joining_start = 0;
    } else {
        fc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
        fc.track_name = B("t");
        fc.start_group = g; fc.start_object = 0; fc.end_group = g; fc.end_object = 1;
    }
    fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
    R_CHECK(&w->rig, moq_session_fetch(w->down[k].cn->peer, &fc, w->rig.now,
                                       &w->down[k].fetch) == MOQ_OK);
    row_cycle(w, 16);
}

static int
row_close(row_t *w, const char *name)
{
    moqr_bind_fwd_debug_t st;
    moqr_bind_debug_fwd_stats(w->rig.bind, &st);
    printf("%s: props_converted=%llu props_refused=%llu props_malformed=%llu\n", name,
           (unsigned long long)st.props_converted, (unsigned long long)st.props_refused,
           (unsigned long long)st.props_malformed);
    relay_bind_test_rig_destroy(&w->rig);
    R_CHECK(&w->rig, w->a.live == 0);
    int f = w->rig.failures;
    printf("%s: %s\n", f ? "FAIL" : "PASS", name);
    return f;
}

static bool
props_are(const uint8_t *got, size_t got_len, const uint8_t *want, size_t want_len)
{
    return got_len == want_len && memcmp(got, want, want_len) == 0;
}

static int
live_datagram(void)
{
    static const moq_version_t dv[1] = { MOQ_VERSION_DRAFT_16 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_18, dv, 1)) return 1 + row_close(&w, "live_datagram");
    row_subscribe(&w, 0);
    row_publish(&w, 0, P18_1000, sizeof(P18_1000), true);
    obs_t *d = &w.down[0];
    printf("live_datagram: objects=%d datagrams=%d props_len=%zu\n", d->objects, d->datagrams,
           d->props_len);
    R_CHECK(&w.rig, d->objects == 1 && d->datagrams == 1 &&
                    props_are(d->props, d->props_len, P16_1000, sizeof(P16_1000)));
    return row_close(&w, "live_datagram");
}

static int
live_unsupported(bool datagram)
{
    const char *row = datagram ? "live_datagram_unsupported" : "live_unsupported";
    static const moq_version_t dv[2] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_18, dv, 2)) return 1 + row_close(&w, row);
    row_subscribe(&w, 0);
    row_subscribe(&w, 1);
    row_publish(&w, 0, P18_HUGE, P18_HUGE_LEN, datagram);
    row_cycle(&w, 8);
    row_publish(&w, 1, P18_1000, sizeof(P18_1000), false);
    obs_t *d16 = &w.down[0], *d18 = &w.down[1];
    printf("%s: d16 objects=%d dones=%d status=%llu closed=%d | d18 objects=%d\n",
           row, d16->objects, d16->dones, (unsigned long long)d16->done_status, d16->closed,
           d18->objects);
    R_CHECK(&w.rig, d16->objects == 0 && d16->dones == 1 && d16->done_status == 0x0 &&
                    d16->closed == 0);
    R_CHECK(&w.rig, moq_session_state(d16->cn->peer) == MOQ_SESS_ESTABLISHED);
    R_CHECK(&w.rig, d18->objects == 2 && d18->dones == 0 &&
                    props_are(d18->props, d18->props_len, P18_1000, sizeof(P18_1000)));
    R_CHECK(&w.rig, d18->datagrams == (datagram ? 1 : 0));
    return row_close(&w, row);
}

static int
live_unsupported_blocked(bool datagram)
{
    const char *row = datagram ? "live_datagram_unsupported_blocked" : "live_unsupported_blocked";
    static const moq_version_t dv[2] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_18, dv, 2))
        return 1 + row_close(&w, row);
    row_subscribe(&w, 0);
    row_subscribe(&w, 1);
    moqr_bind_debug_block_sub_done(3);
    row_publish(&w, 0, P18_HUGE, P18_HUGE_LEN, datagram);
    row_cycle(&w, 16);
    int left = moqr_bind_debug_sub_done_blocks_left();
    moqr_bind_debug_block_sub_done(0);
    row_publish(&w, 1, P18_1000, sizeof(P18_1000), false);
    obs_t *d16 = &w.down[0], *d18 = &w.down[1];
    printf("%s: blocks_left=%d d16 objects=%d dones=%d closed=%d | "
           "d18 objects=%d\n", row, left, d16->objects, d16->dones, d16->closed, d18->objects);
    R_CHECK(&w.rig, left == 0);
    R_CHECK(&w.rig, d16->objects == 0 && d16->dones == 1 && d16->done_status == 0x0 &&
                    d16->closed == 0 &&
                    moq_session_state(d16->cn->peer) == MOQ_SESS_ESTABLISHED);
    R_CHECK(&w.rig, d18->objects == 2 && d18->dones == 0);
    R_CHECK(&w.rig, d18->datagrams == (datagram ? 1 : 0));
    return row_close(&w, row);
}

static int
fetch_unsupported_cancel(void)
{
    static const moq_version_t dv[1] = { MOQ_VERSION_DRAFT_16 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_18, dv, 1))
        return 1 + row_close(&w, "fetch_unsupported_cancel");
    row_publish(&w, 0, P18_HUGE, P18_HUGE_LEN, false);
    row_subscribe(&w, 0);
    obs_t *d = &w.down[0];
    moq_bytes_t ns[1] = { B("pd") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    fc.track_name = B("t");
    fc.start_group = 0; fc.start_object = 0; fc.end_group = 0; fc.end_object = 1;
    fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
    R_CHECK(&w.rig, moq_session_fetch(d->cn->peer, &fc, w.rig.now, &d->fetch) == MOQ_OK);
    /* One cycle carries the FETCH to the relay, which answers and resets it;
     * the fetcher, held, has seen neither when it cancels. */
    row_cycle(&w, 1);
    d->cn->frozen = true;
    row_cycle(&w, 2);
    moq_result_t crc = moq_session_fetch_cancel(d->cn->peer, d->fetch, w.rig.now);
    d->cn->frozen = false;
    row_cycle(&w, 12);
    row_publish(&w, 1, P18_1000, sizeof(P18_1000), false);
    printf("fetch_unsupported_cancel: cancel=%d fetch ok=%d objects=%d complete=%d reset=%d "
           "error=%d closed=%d | sibling objects=%d dones=%d\n", (int)crc, d->fetch_ok,
           d->fetch_objects, d->fetch_complete, d->fetch_reset, d->fetch_error, d->closed,
           d->objects, d->dones);
    R_CHECK(&w.rig, crc == MOQ_OK);
    R_CHECK(&w.rig, d->fetch_objects == 0 && d->fetch_complete == 0 && d->closed == 0);
    R_CHECK(&w.rig, moq_session_state(d->cn->peer) == MOQ_SESS_ESTABLISHED &&
                    moq_session_state(d->cn->rsess) == MOQ_SESS_ESTABLISHED);
    R_CHECK(&w.rig, d->objects == 1 && d->dones == 0 &&
                    props_are(d->props, d->props_len, P16_1000, sizeof(P16_1000)));
    return row_close(&w, "fetch_unsupported_cancel");
}

static int
fetch_row(const char *name, const uint8_t *props, size_t len, bool oom, bool sibling)
{
    static const moq_version_t dv[1] = { MOQ_VERSION_DRAFT_16 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_18, dv, 1)) return 1 + row_close(&w, name);
    row_publish(&w, 0, props, len, false);
    if (sibling) row_subscribe(&w, 0);   /* live from here: group 1 on */
    if (oom) w.a.fail_size = len;        /* the converted copy's buffer */
    row_fetch(&w, 0, 0, false);
    w.a.fail_size = 0;
    obs_t *d = &w.down[0];
    printf("%s: fetch ok=%d objects=%d complete=%d reset=%d error=%d code=%llu closed=%d\n",
           name, d->fetch_ok, d->fetch_objects, d->fetch_complete, d->fetch_reset,
           d->fetch_error, (unsigned long long)d->fetch_code, d->closed);
    R_CHECK(&w.rig, d->closed == 0 &&
                    moq_session_state(d->cn->peer) == MOQ_SESS_ESTABLISHED);
    if (props == P18_1000) {
        R_CHECK(&w.rig, d->fetch_objects == 1 && d->fetch_complete == 1 &&
                        props_are(d->fprops, d->fprops_len, P16_1000, sizeof(P16_1000)));
    } else {
        /* Refused after FETCH_OK: this response alone is reset, no object,
         * no clean completion -- a local failure, INTERNAL_ERROR (0x0, draft-16
         * 13.4.4). */
        R_CHECK(&w.rig, d->fetch_objects == 0 && d->fetch_complete == 0 &&
                        d->fetch_reset == 1 && d->fetch_error == 0 && d->fetch_code == 0x0);
    }
    if (sibling) {
        row_publish(&w, 1, P18_1000, sizeof(P18_1000), false);
        printf("%s: sibling objects=%d dones=%d\n", name, d->objects, d->dones);
        R_CHECK(&w.rig, d->objects == 1 && d->dones == 0 &&
                        props_are(d->props, d->props_len, P16_1000, sizeof(P16_1000)));
    }
    return row_close(&w, name);
}

static int
ingest_malformed(void)
{
    static const moq_version_t dv[3] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                         MOQ_VERSION_DRAFT_18 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_16, dv, 3)) return 1 + row_close(&w, "ingest_malformed");
    row_publish_second(&w);
    row_subscribe(&w, 0);
    row_subscribe(&w, 1);
    row_subscribe_to(&w, 2, B("t2"));
    row_publish(&w, 0, P16_NESTED, sizeof(P16_NESTED), false);
    row_cycle(&w, 8);
    row_publish_on(&w, w.ph2, 0, P16_1000, sizeof(P16_1000), false);
    obs_t *d16 = &w.down[0], *d18 = &w.down[1], *other = &w.down[2];
    moqr_bind_fwd_debug_t st;
    moqr_bind_debug_fwd_stats(w.rig.bind, &st);
    printf("ingest_malformed: origin unsubscribed=%d | d16 objects=%d dones=%d status=%llu | "
           "d18 objects=%d dones=%d status=%llu | t2 objects=%d dones=%d\n",
           w.origin_unsubscribed, d16->objects, d16->dones,
           (unsigned long long)d16->done_status, d18->objects, d18->dones,
           (unsigned long long)d18->done_status, other->objects, other->dones);
    R_CHECK(&w.rig, w.origin_unsubscribed == 1);
    for (int k = 0; k < 2; k++)
        R_CHECK(&w.rig, w.down[k].objects == 0 && w.down[k].dones == 1 &&
                        w.down[k].done_status == 0x12 && w.down[k].closed == 0);
    R_CHECK(&w.rig, st.props_converted == 1 && st.props_refused == 0 &&
                    st.props_malformed == 0);   /* the converted one is t2's */
    R_CHECK(&w.rig, other->objects == 1 && other->dones == 0 &&
                    props_are(other->props, other->props_len, P18_1000, sizeof(P18_1000)));
    for (int k = 0; k < 3; k++)
        R_CHECK(&w.rig, moq_session_state(w.down[k].cn->peer) == MOQ_SESS_ESTABLISHED);
    R_CHECK(&w.rig, moq_session_state(w.origin->peer) == MOQ_SESS_ESTABLISHED);
    return row_close(&w, "ingest_malformed");
}

static int
fetch_after_malformed(void)
{
    static const moq_version_t dv[1] = { MOQ_VERSION_DRAFT_16 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_16, dv, 1))
        return 1 + row_close(&w, "fetch_after_malformed");
    row_publish(&w, 0, P16_NESTED, sizeof(P16_NESTED), false);
    row_fetch(&w, 0, 0, false);
    obs_t *d = &w.down[0];
    printf("fetch_after_malformed: origin unsubscribed=%d fetch ok=%d objects=%d complete=%d "
           "reset=%d error=%d closed=%d\n",
           w.origin_unsubscribed, d->fetch_ok, d->fetch_objects, d->fetch_complete,
           d->fetch_reset, d->fetch_error, d->closed);
    R_CHECK(&w.rig, w.origin_unsubscribed == 1 && d->fetch_objects == 0 &&
                    d->fetch_complete == 0 && d->fetch_reset + d->fetch_error == 1 &&
                    d->closed == 0);
    R_CHECK(&w.rig, moq_session_state(d->cn->peer) == MOQ_SESS_ESTABLISHED);
    return row_close(&w, "fetch_after_malformed");
}

typedef enum {
    DISC_CONVERT, DISC_IMMUTABLE, DISC_OOM, DISC_ONE_ACTION, DISC_UNAVAILABLE,
    DISC_NOT_REQUESTED
} disc_kind_t;

/* The relay's handle and generation for "t", through a core-only observer. */
static bool
row_track(row_t *w, moqr_track_t *track, uint64_t *gen)
{
    moqr_binding_t observer;
    if (moqr_core_binding_open(w->rig.core, 902, &observer) != MOQR_OK) return false;
    moq_bytes_t ns[1] = { B("pd") };
    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = (moqr_ns_t){ .parts = ns, .count = 1 };
    req.name = B("t");
    req.forward = false;
    moqr_sub_t sub = MOQR_SUB_INVALID;
    if (moqr_core_subscribe(w->rig.core, observer, &req, &sub) != MOQR_OK) return false;
    moqr_result_t rc = moqr_core_sub_track(w->rig.core, sub, track, gen);
    (void)moqr_core_unsubscribe(w->rig.core, sub, w->rig.now);
    relay_bind_test_rig_bind_pump_once(&w->rig);
    return rc == MOQR_OK;
}

static int
discovery_row(disc_kind_t kind, moq_version_t dest)
{
    /* Type 120 = 1: 40 78 01 in draft-16, 78 01 in vi64. */
    static const uint8_t q120[] = { 0x40, 0x78, 0x01 }, v120[] = { 0x78, 0x01 };
    /* Immutable holding Type 122 = 2, whose bytes would read differently. */
    static const uint8_t qimm[] = { 0x0B, 0x03, 0x40, 0x7A, 0x02 };
    /* Type 121 holding 37 bytes: 40 79 25 ... in draft-16, 79 25 ... (39
     * bytes, a size nothing else here allocates) in vi64. */
    uint8_t qbig[3 + 37], vbig[2 + 37];
    qbig[0] = 0x40; qbig[1] = 0x79; qbig[2] = 37;
    vbig[0] = 0x79; vbig[1] = 37;
    for (int i = 0; i < 37; i++) qbig[3 + i] = vbig[2 + i] = (uint8_t)(0x30 + i);
    const char *kn = kind == DISC_CONVERT ? "convert" : kind == DISC_IMMUTABLE ? "immutable"
                   : kind == DISC_OOM ? "oom" : kind == DISC_ONE_ACTION ? "one_action"
                   : kind == DISC_UNAVAILABLE ? "unavailable" : "not_requested";
    char name[48];
    snprintf(name, sizeof(name), "discovery_%s(d16->d%u)", kn, (unsigned)dest);
    g_row_track_props = kind == DISC_IMMUTABLE || kind == DISC_ONE_ACTION ||
                                kind == DISC_NOT_REQUESTED
                                               ? (moq_bytes_t){ qimm, sizeof(qimm) }
                      : kind == DISC_OOM       ? (moq_bytes_t){ qbig, sizeof(qbig) }
                                               : (moq_bytes_t){ q120, sizeof(q120) };
    const moq_version_t dv[1] = { dest };
    if (kind == DISC_ONE_ACTION) relay_bind_test_max_actions = 1;
    row_t w;
    bool opened = row_open(&w, MOQ_VERSION_DRAFT_16, dv, 1);
    relay_bind_test_max_actions = 0;
    g_row_track_props = (moq_bytes_t){ NULL, 0 };
    if (!opened) return 1 + row_close(&w, name);
    row_publish_second(&w);
    if (kind == DISC_UNAVAILABLE) {
        /* The track's record says its set is not available: a managed
         * track whose properties cannot be stated, not an empty set. */
        moqr_track_t track;
        uint64_t gen = 0;
        R_CHECK(&w.rig, row_track(&w, &track, &gen));
        moqr_bind_meta_ref_t latest;
        memset(&latest, 0, sizeof(latest));
        R_CHECK(&w.rig, moqr_bind_meta_latest(w.rig.bind, track, &latest) == MOQR_OK &&
                        latest.state == MOQR_BIND_META_PRESENT);
        moqr_bind_meta_ref_t input = latest;
        input.state = MOQR_BIND_META_UNAVAILABLE;
        input.bytes = (moq_bytes_t){ NULL, 0 };
        uint64_t revision = 0;
        R_CHECK(&w.rig, moqr_bind_meta_replace(w.rig.bind, track, gen, &input, w.rig.now,
                                               &revision) == MOQR_OK);
        moqr_bind_meta_release(w.rig.bind, &latest);
    }
    obs_t *d = &w.down[0];
    moq_bytes_t pfx[1] = { B("pd") };
    moq_subscribe_tracks_cfg_t tc;
    moq_subscribe_tracks_cfg_init(&tc);
    tc.track_namespace_prefix = (moq_namespace_t){ .parts = pfx, .count = 1 };
    if (kind == DISC_NOT_REQUESTED) {
        /* INCLUDE_PROPERTIES = 0: the offer states none, whatever the set. */
        tc.options_flags = MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES;
        tc.include_properties = false;
    }
    moq_track_sub_handle_t th;
    R_CHECK(&w.rig, moq_session_subscribe_tracks(d->cn->peer, &tc, w.rig.now, &th) == MOQ_OK);
    if (kind == DISC_OOM) w.a.fail_size = sizeof(vbig);   /* the converted copy */
    row_cycle(&w, 24);
    w.a.fail_size = 0;
    row_cycle(&w, 8);
    moqr_bind_fwd_debug_t st;
    moqr_bind_debug_fwd_stats(w.rig.bind, &st);
    moqr_core_stats_t cs;
    moqr_core_get_stats(w.rig.core, &cs);
    printf("%s: tracks_ok=%d pub_reqs=%d [%s len=%zu] [%s len=%zu] blocked=%d [%s] "
           "withheld=%llu subs=%u closed=%d\n", name, d->tracks_ok, d->pub_reqs,
           d->pub_reqs > 0 ? d->pub_name[0] : "-", d->pub_props_len[0],
           d->pub_reqs > 1 ? d->pub_name[1] : "-", d->pub_props_len[1], d->blocked,
           d->blocked > 0 ? d->blocked_name[0] : "-",
           (unsigned long long)st.publish_withheld, cs.subs, d->closed);
    R_CHECK(&w.rig, d->tracks_ok == 1 && d->closed == 0 &&
                    moq_session_state(d->cn->peer) == MOQ_SESS_ESTABLISHED);
    int t_at = -1, t2_at = -1;
    for (int k = 0; k < d->pub_reqs && k < 4; k++) {
        if (strcmp(d->pub_name[k], "t") == 0) t_at = k;
        if (strcmp(d->pub_name[k], "t2") == 0) t2_at = k;
    }
    /* The sibling, without properties, is offered either way. */
    R_CHECK(&w.rig, t2_at >= 0 && d->pub_props_len[t2_at] == 0);
    if (kind == DISC_ONE_ACTION) {
        /* A one-action queue: the report meets WOULD_BLOCK; the match is
         * judged again on a later pump, withheld again, and reported once. */
        printf("%s: discovery_retried=%llu\n", name,
               (unsigned long long)st.discovery_retried);
        R_CHECK(&w.rig, st.discovery_retried >= 1);
    }
    if (kind == DISC_NOT_REQUESTED) {
        R_CHECK(&w.rig, d->pub_reqs == 2 && t_at >= 0 && d->pub_props_len[t_at] == 0 &&
                        d->blocked == 0 && st.publish_withheld == 0 && cs.subs == 2);
    } else if (kind == DISC_CONVERT) {
        R_CHECK(&w.rig, d->pub_reqs == 2 && t_at >= 0 && d->blocked == 0 &&
                        props_are(d->pub_props[t_at], d->pub_props_len[t_at], v120,
                                  sizeof(v120)));
        R_CHECK(&w.rig, st.publish_withheld == 0 && cs.subs == 2);
    } else {
        /* Never a PUBLISH of "t" without its properties. */
        R_CHECK(&w.rig, d->pub_reqs == 1 && t_at < 0 && d->blocked == 1 &&
                        strcmp(d->blocked_name[0], "t") == 0);
        R_CHECK(&w.rig, (kind == DISC_ONE_ACTION ? st.publish_withheld >= 1
                                                 : st.publish_withheld == 1) &&
                        cs.subs == 1);
    }
    return row_close(&w, name);
}

static int
same_pair(moq_version_t pub, moq_version_t sub)
{
    char name[32];
    snprintf(name, sizeof(name), "same_d%u_d%u", (unsigned)pub, (unsigned)sub);
    const moq_version_t dv[1] = { sub };
    row_t w;
    if (!row_open(&w, pub, dv, 1)) return 1 + row_close(&w, name);
    row_subscribe(&w, 0);
    row_publish(&w, 0, P18_1000, sizeof(P18_1000), false);
    row_publish(&w, 1, P18_1000, sizeof(P18_1000), true);
    obs_t *d = &w.down[0];
    moqr_bind_fwd_debug_t st;
    moqr_bind_debug_fwd_stats(w.rig.bind, &st);
    printf("%s: objects=%d datagrams=%d props_len=%zu\n", name, d->objects, d->datagrams,
           d->props_len);
    R_CHECK(&w.rig, d->objects == 2 && d->datagrams == 1 && d->dones == 0 &&
                    props_are(d->props, d->props_len, P18_1000, sizeof(P18_1000)) &&
                    st.props_converted == 0 && st.props_refused == 0);
    return row_close(&w, name);
}

static int
same_d18_d22(void)
{
    return same_pair(MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22) +
           same_pair(MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_18);
}

/* One Object (g, 0, 0) appended through the core by a publisher with no
 * session. `stated` sets the descriptor's codec; otherwise the descriptor is
 * the size before properties_codec, as an embedder built then passes it. */
static void
embedder_ingest(row_t *w, moqr_track_t tk, uint64_t g, const uint8_t *props, size_t len,
                bool stated)
{
    uint8_t body[4] = { 0xE0, (uint8_t)g, 0x5A, 0xA5 };
    moq_rcbuf_t *pl = NULL, *pr = NULL;
    R_CHECK(&w->rig, moq_rcbuf_create(&w->a.vt, body, sizeof(body), &pl) == MOQ_OK);
    if (len)
        R_CHECK(&w->rig, moq_rcbuf_create(&w->a.vt, props, len, &pr) == MOQ_OK);
    moqr_log_append_desc_t d;
    if (stated) {
        moqr_log_append_desc_init(&d);
        d.properties_codec = MOQR_PROPS_CODEC_VI64;
    } else {
        moqr_log_append_desc_init_sized(&d, offsetof(moqr_log_append_desc_t, properties_codec));
    }
    d.group_id = g;
    d.publisher_priority = 1;
    d.payload = pl;
    d.properties = pr;
    d.now_us = w->rig.now;
    if (moqr_core_ingest(w->rig.core, tk, &d) != MOQR_OK) {
        moq_rcbuf_decref(pl);
        if (pr) moq_rcbuf_decref(pr);
        R_CHECK(&w->rig, 0);
    }
}

static void
embedder_fetch(row_t *w, int k, uint64_t g)
{
    moq_bytes_t ns[1] = { B("demo") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = ns, .count = 1 };
    fc.track_name = B("e");
    fc.start_group = g; fc.start_object = 0; fc.end_group = g; fc.end_object = 1;
    fc.group_order = MOQ_GROUP_ORDER_ASCENDING;
    R_CHECK(&w->rig, moq_session_fetch(w->down[k].cn->peer, &fc, w->rig.now,
                                       &w->down[k].fetch) == MOQ_OK);
    row_cycle(w, 16);
}

static int
embedder_row(const char *name, bool stated)
{
    static const moq_version_t dv[2] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_18, dv, 2)) return 1 + row_close(&w, name);
    moqr_binding_t pb = relay_bind_test_fetch_pub_open(&w.rig);
    moqr_track_t tk = relay_bind_test_fetch_pub_track(&w.rig, pb, "e");
    embedder_ingest(&w, tk, 0, P18_1000, sizeof(P18_1000), stated);
    embedder_ingest(&w, tk, 1, NULL, 0, stated);
    for (int k = 0; k < 2; k++) {
        embedder_fetch(&w, k, 0);
        obs_t *d = &w.down[k];
        printf("%s: d%s fetch ok=%d objects=%d complete=%d reset=%d error=%d props_len=%zu\n",
               name, k ? "18" : "16", d->fetch_ok, d->fetch_objects, d->fetch_complete,
               d->fetch_reset, d->fetch_error, d->fprops_len);
        if (stated) {
            R_CHECK(&w.rig, d->fetch_objects == 1 && d->fetch_complete == 1);
            R_CHECK(&w.rig, k == 0 ? props_are(d->fprops, d->fprops_len, P16_1000,
                                               sizeof(P16_1000))
                                   : props_are(d->fprops, d->fprops_len, P18_1000,
                                               sizeof(P18_1000)));
        } else {
            R_CHECK(&w.rig, d->fetch_objects == 0 && d->fetch_complete == 0 &&
                            d->fetch_reset + d->fetch_error == 1);
        }
        /* An Object without properties reads the same in every codec. */
        int before = d->fetch_objects, done_before = d->fetch_complete;
        embedder_fetch(&w, k, 1);
        R_CHECK(&w.rig, d->fetch_objects == before + 1 && d->fetch_complete == done_before + 1);
        R_CHECK(&w.rig, d->closed == 0 && moq_session_state(d->cn->peer) == MOQ_SESS_ESTABLISHED);
    }
    moqr_bind_fwd_debug_t st;
    moqr_bind_debug_fwd_stats(w.rig.bind, &st);
    R_CHECK(&w.rig, stated ? (st.props_converted == 1 && st.props_refused == 0)
                           : (st.props_converted == 0 && st.props_refused == 2));
    return row_close(&w, name);
}

static int
join_convert(void)
{
    static const moq_version_t dv[1] = { MOQ_VERSION_DRAFT_16 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_18, dv, 1)) return 1 + row_close(&w, "join_convert");
    row_publish(&w, 0, P18_1000, sizeof(P18_1000), false);
    row_subscribe(&w, 0);
    row_fetch(&w, 0, 0, true);
    obs_t *d = &w.down[0];
    printf("join_convert: fetch ok=%d objects=%d complete=%d reset=%d error=%d\n", d->fetch_ok,
           d->fetch_objects, d->fetch_complete, d->fetch_reset, d->fetch_error);
    R_CHECK(&w.rig, d->fetch_objects == 1 && d->fetch_complete == 1 &&
                    props_are(d->fprops, d->fprops_len, P16_1000, sizeof(P16_1000)));
    return row_close(&w, "join_convert");
}

/* The d16 subscriber's transport is frozen, so the relay's converted copy
 * stays in an unsent SEND_DATA action of its session when the binding (and
 * then the core) is destroyed; the session releases it afterwards. */
static int
converted_outlives(void)
{
    static const moq_version_t dv[1] = { MOQ_VERSION_DRAFT_16 };
    row_t w;
    if (!row_open(&w, MOQ_VERSION_DRAFT_18, dv, 1)) return 1 + row_close(&w, "converted_outlives");
    row_subscribe(&w, 0);
    w.down[0].cn->frozen = true;
    row_publish(&w, 0, P18_1000, sizeof(P18_1000), false);
    moqr_bind_fwd_debug_t st;
    moqr_bind_debug_fwd_stats(w.rig.bind, &st);
    R_CHECK(&w.rig, st.props_converted >= 1);
    return row_close(&w, "converted_outlives");
}

int
main(void)
{
    fixtures_init();
    failures += live_datagram();
    failures += live_unsupported(false);
    failures += live_unsupported(true);
    failures += fetch_row("fetch_convert", P18_1000, sizeof(P18_1000), false, false);
    failures += fetch_row("fetch_unsupported", P18_HUGE, P18_HUGE_LEN, false, true);
    failures += fetch_row("fetch_oom", P18_OOM, sizeof(P18_OOM), true, true);
    failures += ingest_malformed();
    failures += fetch_after_malformed();
    failures += live_unsupported_blocked(false);
    failures += live_unsupported_blocked(true);
    failures += fetch_unsupported_cancel();
    failures += discovery_row(DISC_CONVERT, MOQ_VERSION_DRAFT_22);
    failures += discovery_row(DISC_IMMUTABLE, MOQ_VERSION_DRAFT_22);
    failures += discovery_row(DISC_OOM, MOQ_VERSION_DRAFT_22);
    failures += discovery_row(DISC_ONE_ACTION, MOQ_VERSION_DRAFT_22);
    failures += discovery_row(DISC_UNAVAILABLE, MOQ_VERSION_DRAFT_22);
    failures += discovery_row(DISC_NOT_REQUESTED, MOQ_VERSION_DRAFT_22);
    failures += same_d18_d22();
    failures += embedder_row("embedder_stated", true);
    failures += embedder_row("embedder_unknown", false);
    failures += join_convert();
    failures += converted_outlives();
    printf("%s: relay_props_delivery failures=%d\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
