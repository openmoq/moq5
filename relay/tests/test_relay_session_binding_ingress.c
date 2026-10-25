/*
 * Track integrity at the relay's ingress over real sessions, on draft-16,
 * draft-18 and draft-22 (§7.1, §12.1): a duplicate Object arriving on
 * another stream is dropped when identical and otherwise makes the Track
 * malformed — the downstream subscription ends with PUBLISH_DONE
 * MALFORMED_TRACK, the upstream request is cancelled and the copy is never
 * delivered; a reset of a copy's own stream ends the copy alone, never the
 * subgroup it copies. Every cancellation and reset the malformation owes is kept
 * until the session takes it, exactly once, including against a full send
 * buffer; a FETCH or fill still being served, and a forwarded Joining
 * FETCH held mid-replay, is reset with MALFORMED_TRACK. Alongside: ingest
 * holes, reopened subgroups and expired tails end their downstream streams
 * in a reset, never a FIN (§11.3.2); the downstream subgroup keeps the
 * upstream stream's properties form; and MAX_CACHE_DURATION keeps an
 * expired object from starting downstream.
 */
#include "relay_session_binding_harness.h"

/* ---- Malformed Tracks, holes and cache duration at the relay ------------ *
 * draft-22 §12.1, §7.1, §7.4, §11.3.2, §10.3 (draft-18 §2.4.2, §9.1, §9.4,
 * §11.4.3, §12.3). */

typedef struct mrig {
    rig_t                rig;
    conn_t              *pub;
    conn_t              *sub;
    peer_state_t         pub_ps;
    peer_state_t         s_ps;
    moq_subscription_t   sh;
} mrig_t;

static bool
mrig_open(mrig_t *w, ca_t *a, moq_version_t v)
{
    memset(w, 0, sizeof(*w));
    if (relay_bind_test_rig_create(&w->rig, a) != MOQR_OK) return false;
    rig_t *rig = &w->rig;
    w->pub = relay_bind_test_rig_connect(rig, v);
    w->sub = relay_bind_test_rig_connect(rig, v);
    if (!w->pub || !w->sub) return false;
    relay_bind_test_rig_pump(rig, 4);
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_publish_namespace_cfg_t pcfg;
    moq_publish_namespace_cfg_init(&pcfg);
    pcfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    moq_announcement_t ann;
    if (moq_session_publish_namespace(w->pub->peer, &pcfg, rig->now, &ann) != MOQ_OK)
        return false;
    relay_bind_test_rig_pump(rig, 4);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    if (moq_session_subscribe(w->sub->peer, &scfg, rig->now, &w->sh) != MOQ_OK)
        return false;
    relay_bind_test_rig_pump(rig, 4);
    relay_bind_test_peer_drain(rig, w->pub, &w->pub_ps, true, false, 0, 0);
    relay_bind_test_rig_pump(rig, 4);
    relay_bind_test_peer_drain(rig, w->sub, &w->s_ps, false, false, 0, 0);
    return w->pub_ps.up_seen;
}

static void
mrig_write(mrig_t *w, moq_subgroup_handle_t h, uint64_t object_id,
         const uint8_t *body, size_t len)
{
    moq_rcbuf_t *pl = NULL;
    R_CHECK(&w->rig, moq_rcbuf_create(&w->rig.alloc->vt, body, len, &pl) == MOQ_OK);
    R_CHECK(&w->rig, moq_session_write_object(w->pub->peer, h, object_id, pl,
                                              w->rig.now) == MOQ_OK);
    moq_rcbuf_decref(pl);
}

static moq_subgroup_handle_t
mrig_open_sg(mrig_t *w, uint64_t g, uint64_t sg, bool props)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = g;
    sc.subgroup_id = sg;
    sc.object_properties = props;
    moq_subgroup_handle_t h;
    memset(&h, 0, sizeof(h));
    R_CHECK(&w->rig, moq_session_open_subgroup(w->pub->peer, w->pub_ps.up_sub, &sc,
                                               w->rig.now, &h) == MOQ_OK);
    return h;
}

static void
mrig_settle(mrig_t *w)
{
    for (int i = 0; i < 4; i++) {
        relay_bind_test_rig_pump(&w->rig, 8);
        relay_bind_test_peer_drain(&w->rig, w->pub, &w->pub_ps, true, false, 0, 0);
        relay_bind_test_peer_drain(&w->rig, w->sub, &w->s_ps, false, false, 0, 0);
    }
}

static int
mrig_close(mrig_t *w, ca_t *a, const char *name, moq_version_t v)
{
    relay_bind_test_rig_destroy(&w->rig);
    R_CHECK(&w->rig, a->live == 0);
    int f = w->rig.failures;
    if (f == 0) printf("PASS: %s d%u\n", name, (unsigned)v);
    return f;
}

/* A duplicate Object — the same Group and Object ID, whatever its subgroup
 * (§7.1: an Object is "identified by the Object's Full Track Name, Group ID
 * and Object ID") — arriving on another stream: identical (same subgroup and
 * payload), it is dropped and the subscription goes on; in another
 * subgroup, with the same payload (mode 2) or another one (mode 1), the
 * Track is malformed — downstream ends with PUBLISH_DONE MALFORMED_TRACK,
 * upstream is cancelled, and the copy is never delivered. */
static int
relay_malformed_duplicate(moq_version_t v, int mode)
{
    bool differs = mode != 0;
    ca_t a;
    relay_bind_test_ca_init(&a);
    mrig_t w;
    if (!mrig_open(&w, &a, v)) {
        printf("FAIL: relay_malformed_duplicate fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t first[3] = { 'o', 'n', 'e' };
    static const uint8_t other[3] = { 't', 'w', 'o' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, first, sizeof(first));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    /* The copy: Subgroup 0 again on a second stream (identical), or the
     * same Object in Subgroup 1 with another payload. */
    R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h0, w.rig.now) == MOQ_OK);
    mrig_settle(&w);
    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, differs ? 1 : 0, false);
    mrig_write(&w, h1, 0, mode == 1 ? other : first, 3);
    mrig_settle(&w);
    if (differs) {
        R_CHECK(&w.rig, w.s_ps.objects == 1);           /* never delivered  */
        R_CHECK(&w.rig, w.s_ps.done_count == 1);
        R_CHECK(&w.rig, w.s_ps.done_code == 0x12);      /* MALFORMED_TRACK  */
        R_CHECK(&w.rig, w.pub_ps.unsubscribed == 1);    /* upstream cancelled */
    } else {
        R_CHECK(&w.rig, w.s_ps.objects == 1);           /* deduplicated     */
        R_CHECK(&w.rig, w.s_ps.done_count == 0);
        R_CHECK(&w.rig, w.pub_ps.unsubscribed == 0);
        moq_subgroup_handle_t h2 = mrig_open_sg(&w, 1, 0, false);
        mrig_write(&w, h2, 0, first, sizeof(first));
        mrig_settle(&w);
        R_CHECK(&w.rig, w.s_ps.objects == 2);           /* the track goes on */
    }
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, mode == 0 ? "relay_malformed_duplicate_identical"
                              : mode == 1 ? "relay_malformed_duplicate_differs"
                                          : "relay_malformed_duplicate_other_subgroup",
                      v);
}


/* Write `len` bytes as object `object_id` on `h` in `pieces` chunks, pumping
 * between them so each reaches the relay as its own read. */
static void
mrig_write_chunked(mrig_t *w, moq_subgroup_handle_t h, uint64_t object_id,
                   const uint8_t *body, size_t len, int pieces)
{
    R_CHECK(&w->rig, moq_session_begin_object(w->pub->peer, h, object_id, len,
                                              w->rig.now) == MOQ_OK);
    size_t off = 0;
    for (int p = 0; p < pieces; p++) {
        size_t n = p + 1 == pieces ? len - off : len / (size_t)pieces;
        moq_rcbuf_t *part = NULL;
        R_CHECK(&w->rig, moq_rcbuf_create(&w->rig.alloc->vt, body + off, n, &part) ==
                             MOQ_OK);
        R_CHECK(&w->rig, moq_session_write_object_data(w->pub->peer, h, part,
                                                       w->rig.now) == MOQ_OK);
        moq_rcbuf_decref(part);
        off += n;
        mrig_settle(w);
    }
}

/* A copy of a retained Object that differs from it only in its payload bytes
 * (same Subgroup, Priority, mode and length) makes the Track malformed
 * (draft-22 §7.1 "different ... Payload"); an identical copy is dropped and
 * the Track goes on. The relay receives the copy whole, or streamed in one
 * or several chunks. */
static int
relay_dup_payload(moq_version_t v, bool streaming, int pieces, bool differs)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = streaming;
    mrig_t w;
    bool ok = mrig_open(&w, &a, v);
    relay_bind_test_server_streaming_objects = false;
    char name[96];
    snprintf(name, sizeof(name), "relay_dup_payload_%s_%s_%d",
             differs ? "differs" : "identical", streaming ? "streamed" : "whole", pieces);
    if (!ok) {
        printf("FAIL: %s fixture\n", name);
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t first[6] = { 'o', 'n', 'e', 'o', 'n', 'e' };
    static const uint8_t other[6] = { 'o', 'n', 'e', 'o', 'n', 'X' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, first, sizeof(first));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h0, w.rig.now) == MOQ_OK);
    mrig_settle(&w);
    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, 0, false);
    const uint8_t *copy = differs ? other : first;
    if (pieces > 0) {
        mrig_write_chunked(&w, h1, 0, copy, sizeof(first), pieces);
    } else {
        mrig_write(&w, h1, 0, copy, sizeof(first));
    }
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);              /* the copy never forwarded */
    if (differs) {
        R_CHECK(&w.rig, w.s_ps.done_count == 1 && w.s_ps.done_code == 0x12);
        R_CHECK(&w.rig, w.pub_ps.unsubscribed == 1);
    } else {
        R_CHECK(&w.rig, w.s_ps.done_count == 0 && w.pub_ps.unsubscribed == 0);
        moq_subgroup_handle_t h2 = mrig_open_sg(&w, 1, 0, false);
        mrig_write(&w, h2, 0, first, sizeof(first));
        mrig_settle(&w);
        R_CHECK(&w.rig, w.s_ps.objects == 2);          /* the Track goes on        */
    }
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, name, v);
}

/* A streamed copy of a retained Object whose first payload byte already
 * differs makes the Track malformed at that byte (§7.1, §12.1): with the
 * copy left unfinished and no more input, the downstream subscription ends
 * with MALFORMED_TRACK and the upstream one is cancelled; the copy's
 * remaining bytes and end add no second terminal. The control copy's first
 * byte matches: nothing happens while it is unfinished, it ends identical
 * and is dropped, and the Track goes on. */
static int
relay_dup_first_byte(moq_version_t v, bool differs)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    mrig_t w;
    bool ok = mrig_open(&w, &a, v);
    relay_bind_test_server_streaming_objects = false;
    const char *name = differs ? "relay_dup_first_byte_differs"
                               : "relay_dup_first_byte_identical_prefix";
    if (!ok) {
        printf("FAIL: %s fixture\n", name);
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t first[6] = { 'o', 'n', 'e', 'o', 'n', 'e' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, first, sizeof(first));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h0, w.rig.now) == MOQ_OK);
    mrig_settle(&w);
    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, 0, false);
    R_CHECK(&w.rig, moq_session_begin_object(w.pub->peer, h1, 0, sizeof(first),
                                             w.rig.now) == MOQ_OK);
    const uint8_t lead = differs ? 'X' : first[0];
    moq_rcbuf_t *part = NULL;
    R_CHECK(&w.rig, moq_rcbuf_create(&w.rig.alloc->vt, &lead, 1, &part) == MOQ_OK);
    R_CHECK(&w.rig, moq_session_write_object_data(w.pub->peer, h1, part, w.rig.now) ==
                        MOQ_OK);
    moq_rcbuf_decref(part);
    mrig_settle(&w);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.done_count == (differs ? 1 : 0));
    if (differs) R_CHECK(&w.rig, w.s_ps.done_code == 0x12);
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == (differs ? 1 : 0));
    /* The rest of the copy, and its end. After a cancellation the origin may
     * refuse the writes; that is not what is measured. */
    part = NULL;
    if (moq_rcbuf_create(&w.rig.alloc->vt, first + 1, sizeof(first) - 1, &part) ==
        MOQ_OK) {
        (void)moq_session_write_object_data(w.pub->peer, h1, part, w.rig.now);
        moq_rcbuf_decref(part);
    }
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.done_count == (differs ? 1 : 0));
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == (differs ? 1 : 0));
    R_CHECK(&w.rig, w.s_ps.objects == 1);              /* the copy never forwarded */
    if (!differs) {
        moq_subgroup_handle_t h2 = mrig_open_sg(&w, 1, 0, false);
        mrig_write(&w, h2, 0, first, sizeof(first));
        mrig_settle(&w);
        R_CHECK(&w.rig, w.s_ps.objects == 2);          /* the Track goes on */
    }
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, name, v);
}

static uint32_t fnv1a(const uint8_t *p, size_t n);

/* Begin a streamed copy of Object `sg` of {g, sg} with `lead` as its first
 * payload byte, leaving the rest unsent. */
static moq_subgroup_handle_t
mrig_begin_copy(mrig_t *w, uint64_t g, uint64_t sg, uint8_t lead)
{
    moq_subgroup_handle_t h = mrig_open_sg(w, g, sg, false);
    R_CHECK(&w->rig, moq_session_begin_object(w->pub->peer, h, sg, 6, w->rig.now) ==
                         MOQ_OK);
    moq_rcbuf_t *part = NULL;
    R_CHECK(&w->rig, moq_rcbuf_create(&w->rig.alloc->vt, &lead, 1, &part) == MOQ_OK);
    R_CHECK(&w->rig, moq_session_write_object_data(w->pub->peer, h, part, w->rig.now) ==
                         MOQ_OK);
    moq_rcbuf_decref(part);
    return h;
}

/* Send the rest of a copy begun by mrig_begin_copy (`tail`, five bytes). */
static void
mrig_end_copy(mrig_t *w, moq_subgroup_handle_t h, const uint8_t *tail)
{
    moq_rcbuf_t *part = NULL;
    R_CHECK(&w->rig, moq_rcbuf_create(&w->rig.alloc->vt, tail, 5, &part) == MOQ_OK);
    R_CHECK(&w->rig, moq_session_write_object_data(w->pub->peer, h, part, w->rig.now) ==
                         MOQ_OK);
    moq_rcbuf_decref(part);
}

/* The relay compares at most eight streamed copies at once (a bounded
 * resource policy; §7.1 lets a relay ignore a later copy). Ten retained
 * Objects, Object sg of Subgroup {g, sg} for g in 0..1 and sg in 0..4.
 *   mode 0, exhaustion and reuse: eight unfinished identical copies hold
 *     every comparison owner; a ninth copy, whose first byte differs, finds
 *     none and is ignored uncompared -- not forwarded, not malformed, the
 *     retained Objects intact. Once the eight end identical their owners are
 *     free, and a tenth copy whose first byte differs is compared and makes
 *     the Track malformed at that byte, once.
 *   mode 1, isolation: seven identical copies are being compared when an
 *     eighth begins with a differing first byte; it alone makes the Track
 *     malformed, exactly once, and the others' identical ends add nothing. */
static int
relay_dup_owner_policy(moq_version_t v, int mode)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    mrig_t w;
    bool ok = mrig_open(&w, &a, v);
    relay_bind_test_server_streaming_objects = false;
    const char *name = mode == 0 ? "relay_dup_owner_exhaustion_reuse"
                                 : "relay_dup_owner_isolation";
    if (!ok) {
        printf("FAIL: %s fixture\n", name);
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t body[6] = { 'o', 'n', 'e', 'o', 'n', 'e' };
    static const uint8_t other[5] = { 'X', 'X', 'X', 'X', 'X' };
    for (uint64_t g = 0; g < 2; g++) {
        for (uint64_t sg = 0; sg < 5; sg++) {
            moq_subgroup_handle_t h = mrig_open_sg(&w, g, sg, false);
            mrig_write(&w, h, sg, body, sizeof(body));
            R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h, w.rig.now) ==
                                MOQ_OK);
            mrig_settle(&w);
        }
    }
    R_CHECK(&w.rig, w.s_ps.objects == 10);
    moq_subgroup_handle_t hs[8];
    for (int k = 0; k < 8; k++) {
        uint8_t lead = (mode == 1 && k == 7) ? 'X' : body[0];
        hs[k] = mrig_begin_copy(&w, (uint64_t)(k / 5), (uint64_t)(k % 5), lead);
        mrig_settle(&w);
    }
    if (mode == 1) {
        R_CHECK(&w.rig, w.s_ps.done_count == 1 && w.s_ps.done_code == 0x12);
        R_CHECK(&w.rig, w.pub_ps.unsubscribed == 1);
        /* The other copies end identical; after the cancellation the origin
         * may refuse the writes, which is not what is measured. */
        for (int k = 0; k < 8; k++) {
            if (k == 7) continue;
            moq_rcbuf_t *part = NULL;
            if (moq_rcbuf_create(&w.rig.alloc->vt, body + 1, 5, &part) == MOQ_OK) {
                (void)moq_session_write_object_data(w.pub->peer, hs[k], part, w.rig.now);
                moq_rcbuf_decref(part);
            }
        }
        mrig_settle(&w);
        R_CHECK(&w.rig, w.s_ps.done_count == 1);
        R_CHECK(&w.rig, w.s_ps.objects == 10);
        R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
        return mrig_close(&w, &a, name, v);
    }
    R_CHECK(&w.rig, w.s_ps.done_count == 0);
    /* The ninth copy: no owner is free. */
    moq_subgroup_handle_t h9 = mrig_begin_copy(&w, 1, 3, 'X');
    mrig_settle(&w);
    mrig_end_copy(&w, h9, other);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.done_count == 0 && w.pub_ps.unsubscribed == 0);
    R_CHECK(&w.rig, w.s_ps.objects == 10);
    /* The eight end identical; their owners are free again. */
    for (int k = 0; k < 8; k++) {
        mrig_end_copy(&w, hs[k], body + 1);
        mrig_settle(&w);
    }
    R_CHECK(&w.rig, w.s_ps.done_count == 0 && w.pub_ps.unsubscribed == 0);
    R_CHECK(&w.rig, w.s_ps.objects == 10);
    /* The retained Object the ignored copy targeted is intact: a new
     * subscriber's FETCH reads the original bytes. */
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    fc.track_name = B("v");
    fc.start_group = 1;
    fc.start_object = 0;
    fc.end_group = 1;
    fc.end_object = 1;
    fc.start_object = 3;
    fc.end_object = 4;
    moq_fetch_t fh;
    R_CHECK(&w.rig, moq_session_fetch(w.sub->peer, &fc, w.rig.now, &fh) == MOQ_OK);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.fdlv_n >= 1);
    for (int i = 0; i < w.s_ps.fdlv_n && i < 8; i++)
        R_CHECK(&w.rig, w.s_ps.fdlv[i].len == 6 &&
                            w.s_ps.fdlv[i].fp_all == fnv1a(body, sizeof(body)));
    /* Reuse: a tenth copy, compared, differs at its first byte. */
    (void)mrig_begin_copy(&w, 1, 4, 'X');
    mrig_settle(&w);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.done_count == 1 && w.s_ps.done_code == 0x12);
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == 1);
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, name, v);
}

/* §7.1 item 1: a retained Normal object followed, at the same Location, by a
 * status that says it does not exist (End of Group there) is the permitted
 * existing-to-missing transition, not a malformed Track: no PUBLISH_DONE
 * MALFORMED_TRACK, no upstream cancel, and the relay keeps what it has. */
static int
relay_existing_to_missing(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    mrig_t w;
    if (!mrig_open(&w, &a, v)) {
        printf("FAIL: relay_existing_to_missing fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t body[3] = { 'e', 'x', 's' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, body, sizeof(body));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h0, w.rig.now) == MOQ_OK);
    mrig_settle(&w);
    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, 0, false);
    R_CHECK(&w.rig, moq_session_write_status_object(w.pub->peer, h1, 0,
                                                    MOQ_OBJECT_END_OF_GROUP,
                                                    w.rig.now) == MOQ_OK);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.done_count == 0);
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == 0);
    moq_subgroup_handle_t h2 = mrig_open_sg(&w, 1, 0, false);
    mrig_write(&w, h2, 0, body, sizeof(body));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 2);
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, "relay_existing_to_missing", v);
}


/* Raw subgroup stream bytes into the relay's upstream session, as if the
 * origin sent them on a stream of its own. */
static void
mrig_inject(mrig_t *w, uint64_t stream, const uint8_t *b, size_t n, bool fin)
{
    (void)moq_session_on_data_bytes(w->pub->rsess, moq_stream_ref_from_u64(stream),
                                    b, n, fin, w->rig.now);
}

static uint32_t
fnv1a(const uint8_t *p, size_t n)
{
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < n; i++) h = (h ^ p[i]) * 16777619u;
    return h;
}

/* An object still arriving on one upstream stream and a copy of it on a
 * second stream of the same subgroup (a reset-and-reopen race): the copy's
 * chunks never reach the record being filled, its end cannot settle it, and
 * the cached and forwarded bytes are the first stream's. */
static int
relay_dup_copy_interleaved(void)
{
    const moq_version_t v = MOQ_VERSION_DRAFT_22;
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    relay_bind_test_accept_track_alias = 9;
    mrig_t w;
    bool ok = mrig_open(&w, &a, v);
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_accept_track_alias = 0;
    if (!ok) {
        printf("FAIL: relay_dup_copy_interleaved fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    /* PROPERTIES subgroup header: alias 9, Group 0, Subgroup 0, priority 0;
     * then Object 0: delta 0, no properties, length 6. */
    static const uint8_t hdr_obj[] = { 0x15, 9, 0, 0, 0, 0x00, 0x00, 0x06 };
    const uint64_t s1 = 0x7F00000u, s2 = 0x7F00004u;
    uint8_t first[16];
    memcpy(first, hdr_obj, sizeof(hdr_obj));
    memcpy(first + sizeof(hdr_obj), "abc", 3);
    mrig_inject(&w, s1, first, sizeof(hdr_obj) + 3, false);
    mrig_settle(&w);
    uint8_t copy[16];
    memcpy(copy, hdr_obj, sizeof(hdr_obj));
    memcpy(copy + sizeof(hdr_obj), "XXXXXX", 6);
    mrig_inject(&w, s2, copy, sizeof(hdr_obj) + 6, true);
    mrig_settle(&w);
    mrig_inject(&w, s1, (const uint8_t *)"def", 3, true);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    R_CHECK(&w.rig, w.s_ps.last_len == 6 && w.s_ps.last_byte == 'a');
    R_CHECK(&w.rig, w.s_ps.done_count == 0);

    /* The cache holds the first stream's bytes. */
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    fc.track_name = B("v");
    fc.start_group = 0;
    fc.start_object = 0;
    fc.end_group = 0;
    fc.end_object = 1;
    moq_fetch_t fh;
    R_CHECK(&w.rig, moq_session_fetch(w.sub->peer, &fc, w.rig.now, &fh) == MOQ_OK);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.fdlv_n == 1);
    if (w.s_ps.fdlv_n >= 1) {
        R_CHECK(&w.rig, w.s_ps.fdlv[0].len == 6);
        R_CHECK(&w.rig, w.s_ps.fdlv[0].fp_all == fnv1a((const uint8_t *)"abcdef", 6));
    }
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, "relay_dup_copy_interleaved", v);
}

/* A copy ignored uncompared -- the ninth while eight are being compared -- of
 * a complete object whose subgroup is still open, arriving on a second stream
 * of that subgroup, which is then reset mid-object: the reset ends the copy
 * alone. Only the source whose OPEN record a reset abandons ends the
 * subgroup's list, so the subgroup goes on: its next object is delivered and
 * its stream ends with the publisher's FIN, never with the copy's reset. */
static int
relay_dup_copy_reset_live(void)
{
    const moq_version_t v = MOQ_VERSION_DRAFT_22;
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    relay_bind_test_accept_track_alias = 9;
    mrig_t w;
    bool ok = mrig_open(&w, &a, v);
    relay_bind_test_server_streaming_objects = false;
    relay_bind_test_accept_track_alias = 0;
    const char *name = "relay_dup_copy_reset_live";
    if (!ok) {
        printf("FAIL: %s fixture\n", name);
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t body[6] = { 'o', 'n', 'e', 'o', 'n', 'e' };
    /* Ten retained Objects, Object sg of Subgroup {g, sg}; {1, 4} stays
     * open. */
    moq_subgroup_handle_t live;
    memset(&live, 0, sizeof(live));
    for (uint64_t g = 0; g < 2; g++) {
        for (uint64_t sg = 0; sg < 5; sg++) {
            moq_subgroup_handle_t h = mrig_open_sg(&w, g, sg, false);
            mrig_write(&w, h, sg, body, sizeof(body));
            if (g == 1 && sg == 4) {
                live = h;
            } else {
                R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h,
                                                           w.rig.now) == MOQ_OK);
            }
            mrig_settle(&w);
        }
    }
    R_CHECK(&w.rig, w.s_ps.objects == 10);
    /* Eight unfinished identical copies hold every comparison owner. */
    moq_subgroup_handle_t hs[8];
    for (int k = 0; k < 8; k++) {
        hs[k] = mrig_begin_copy(&w, (uint64_t)(k / 5), (uint64_t)(k % 5), body[0]);
        mrig_settle(&w);
    }
    /* The ninth, of Object 4 of the open Subgroup {1, 4}, on a stream of its
     * own: SUBGROUP_HEADER (alias 9, Group 1, Subgroup 4, priority 0), then
     * Object 4 (delta 4, length 6) and its first byte. It finds no owner;
     * then its stream is reset. */
    static const uint8_t copy[] = { 0x14, 9, 1, 4, 0, 0x04, 0x06, 'X' };
    const uint64_t s9 = 0x7F00008u;
    mrig_inject(&w, s9, copy, sizeof(copy), false);
    mrig_settle(&w);
    (void)moq_session_on_data_reset(w.pub->rsess, moq_stream_ref_from_u64(s9),
                                    0x7, w.rig.now);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.subgroup_resets == 0);
    /* The subgroup goes on: Object 5, then the FIN. */
    mrig_write(&w, live, 5, body, sizeof(body));
    R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, live, w.rig.now) ==
                        MOQ_OK);
    mrig_settle(&w);
    for (int k = 0; k < 8; k++) {
        mrig_end_copy(&w, hs[k], body + 1);
        mrig_settle(&w);
    }
    R_CHECK(&w.rig, w.s_ps.objects == 11);
    R_CHECK(&w.rig, w.s_ps.last_group == 1 && w.s_ps.last_object == 5);
    R_CHECK(&w.rig, w.s_ps.done_count == 0 && w.pub_ps.unsubscribed == 0);
    R_CHECK(&w.rig, w.s_ps.subgroup_resets == 0);
    int fins14 = 0;
    for (int i = 0; i < w.s_ps.finished_count && i < 16; i++) {
        if (w.s_ps.finished_gsg[i][0] == 1 && w.s_ps.finished_gsg[i][1] == 4) {
            fins14++;
        }
    }
    R_CHECK(&w.rig, fins14 == 1);
    R_CHECK(&w.rig, w.s_ps.subgroup_finished == 10);
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, name, v);
}


/* The relay's upstream cancellation of a malformed Track (draft-22 §12.1:
 * the relay as subscriber "MUST cancel any corresponding subscription") is
 * an obligation kept until it commits: the first attempts are refused as a
 * full session would refuse them, and the origin still sees exactly one
 * cancellation once they go through, without the object being sent again.
 * The relay detects the conflict itself (a copy under another Subgroup
 * ID), which the relay's session does not detect. */
static int
relay_upstream_cancel_owed(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    mrig_t w;
    if (!mrig_open(&w, &a, v)) {
        printf("FAIL: relay_upstream_cancel_owed fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t first[3] = { 'o', 'n', 'e' };
    static const uint8_t other[3] = { 't', 'w', 'o' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, first, sizeof(first));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    moqr_bind_debug_block_upstream_cancels(3);
    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, 1, false);
    mrig_write(&w, h1, 0, other, sizeof(other));
    relay_bind_test_rig_pump(&w.rig, 2);
    relay_bind_test_peer_drain(&w.rig, w.pub, &w.pub_ps, true, false, 0, 0);
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == 0);   /* refused so far */
    mrig_settle(&w);
    moqr_bind_debug_block_upstream_cancels(0);
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == 1);
    R_CHECK(&w.rig, w.s_ps.done_count == 1 && w.s_ps.done_code == 0x12);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, "relay_upstream_cancel_owed", v);
}

/* An upstream cancellation the session refuses with MOQ_ERR_BUFFER can never
 * be made: the session returns BUFFER for a cancellation only when the
 * message is larger than its whole send buffer (a buffer still holding
 * earlier output is WOULD_BLOCK). The refusal is injected, since no session
 * that completed its setup has a buffer too small for these few bytes. Such
 * a cancellation is neither owed forever nor dropped while the upstream
 * request runs on: it is attempted exactly once, and the relay closes that
 * upstream session with INTERNAL_ERROR. The downstream subscription still
 * ends with MALFORMED_TRACK, and the downstream session stays open. */
static int
relay_cancel_hard_failure(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    mrig_t w;
    if (!mrig_open(&w, &a, v)) {
        printf("FAIL: relay_cancel_hard_failure fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t first[3] = { 'o', 'n', 'e' };
    static const uint8_t other[3] = { 't', 'w', 'o' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, first, sizeof(first));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    (void)moqr_bind_debug_fail_upstream_cancels(5);
    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, 1, false);
    mrig_write(&w, h1, 0, other, sizeof(other));
    mrig_settle(&w);
    mrig_settle(&w);
    R_CHECK(&w.rig, moqr_bind_debug_fail_upstream_cancels(0) == 4);   /* one attempt */
    R_CHECK(&w.rig, w.pub_ps.session_closed);              /* upstream closed   */
    R_CHECK(&w.rig, w.pub_ps.close_code == 0x1);            /* INTERNAL_ERROR    */
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == 0);            /* no cancel was sent */
    R_CHECK(&w.rig, w.s_ps.done_count == 1 && w.s_ps.done_code == 0x12);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    R_CHECK(&w.rig, !w.s_ps.session_closed);
    return mrig_close(&w, &a, "relay_cancel_hard_failure", v);
}

/* Queue TRACK_STATUS requests on `s` for another Track of live/cam until
 * its send buffer has at most a byte or two left: each is the largest that
 * still fits, so what remains is smaller than any cancellation message. The
 * caller does not step the connection's transport, so the bytes stay
 * queued. */
static void
fill_send_buffer(rig_t *r, moq_session_t *s)
{
    static uint8_t name[2048];
    memset(name, 'f', sizeof(name));
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    size_t len = sizeof(name);
    int queued = 0;
    for (int guard = 0; guard < 8192; guard++) {
        moq_track_status_cfg_t c;
        moq_track_status_cfg_init(&c);
        c.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        c.track_name = (moq_bytes_t){ name, len };
        moq_track_status_handle_t h;
        moq_result_t rc = moq_session_track_status(s, &c, r->now, &h);
        if (rc == MOQ_OK) { queued++; continue; }
        R_CHECK(r, rc == MOQ_ERR_BUFFER);
        if (rc != MOQ_ERR_BUFFER || len == 0) break;
        len--;
    }
    R_CHECK(r, queued >= 2);
}

/* The relay's upstream cancellation meets a send buffer that still holds
 * earlier control output (draft-16 and draft-18 cancel with a control
 * message): the session refuses it for now, the relay keeps the obligation,
 * and once the transport has written the earlier bytes the origin sees
 * exactly one cancellation, with the session open. No test seam: the
 * buffer is really full. */
static int
relay_cancel_send_buffer(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    mrig_t w;
    if (!mrig_open(&w, &a, v)) {
        printf("FAIL: relay_cancel_send_buffer fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t first[3] = { 'o', 'n', 'e' };
    static const uint8_t other[3] = { 't', 'w', 'o' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, first, sizeof(first));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    /* The conflicting copy reaches the relay's session; before the relay
     * sees it, earlier control output fills that session's send buffer. */
    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, 1, false);
    mrig_write(&w, h1, 0, other, sizeof(other));
    relay_bind_test_rig_step_pair(&w.rig, w.pub);
    fill_send_buffer(&w.rig, w.pub->rsess);
    relay_bind_test_rig_bind_pump_once(&w.rig);
    relay_bind_test_peer_drain(&w.rig, w.pub, &w.pub_ps, true, false, 0, 0);
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == 0);   /* not sent yet */
    mrig_settle(&w);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == 1);
    R_CHECK(&w.rig, w.s_ps.done_count == 1 && w.s_ps.done_code == 0x12);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, "relay_cancel_send_buffer", v);
}

/* The same for a PUBLISH the relay accepted: the publication's cancellation
 * is retried until the origin sees it, once. */
static int
relay_publish_cancel_owed(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: relay_publish_cancel_owed rig\n");
        return 1;
    }
    conn_t *pub = relay_bind_test_rig_connect(&rig, v);
    conn_t *sub = relay_bind_test_rig_connect(&rig, v);
    R_CHECK(&rig, pub && sub);
    if (!pub || !sub) { relay_bind_test_rig_destroy(&rig); return rig.failures; }
    relay_bind_test_rig_pump(&rig, 4);
    moq_bytes_t nsp[2] = { B("live"), B("push") };
    moq_publish_cfg_t pc;
    moq_publish_cfg_init(&pc);
    pc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    pc.track_name = B("v");
    moq_publication_t ph;
    R_CHECK(&rig, moq_session_publish(pub->peer, &pc, rig.now, &ph) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    peer_state_t pub_ps, s_ps;
    memset(&pub_ps, 0, sizeof(pub_ps));
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, pub, &pub_ps, false, false, 0, 0);
    R_CHECK(&rig, pub_ps.publish_ok);
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sh;
    R_CHECK(&rig, moq_session_subscribe(sub->peer, &scfg, rig.now, &sh) == MOQ_OK);
    relay_bind_test_rig_pump(&rig, 6);
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    static const uint8_t first[3] = { 'o', 'n', 'e' };
    static const uint8_t other[3] = { 't', 'w', 'o' };
    int unsubscribed = 0;
    for (uint64_t sg = 0; sg < 2; sg++) {
        if (sg == 1) moqr_bind_debug_block_upstream_cancels(3);
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 0;
        sc.subgroup_id = sg;
        moq_subgroup_handle_t h;
        R_CHECK(&rig, moq_session_open_pub_subgroup(pub->peer, ph, &sc, rig.now, &h) ==
                          MOQ_OK);
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, sg == 0 ? first : other, 3, &pl) == MOQ_OK);
        R_CHECK(&rig, moq_session_write_object(pub->peer, h, 0, pl, rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
        for (int i = 0; i < (sg == 0 ? 8 : 2); i++) {
            relay_bind_test_rig_pump(&rig, 1);
            relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
            moq_event_t ev;
            while (moq_session_poll_events(pub->peer, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_PUBLISH_UNSUBSCRIBED) unsubscribed++;
                moq_event_cleanup(&ev);
            }
        }
    }
    R_CHECK(&rig, s_ps.objects == 1);
    R_CHECK(&rig, unsubscribed == 0);   /* refused so far */
    for (int i = 0; i < 12; i++) {
        relay_bind_test_rig_pump(&rig, 1);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        moq_event_t ev;
        while (moq_session_poll_events(pub->peer, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_UNSUBSCRIBED) unsubscribed++;
            moq_event_cleanup(&ev);
        }
    }
    moqr_bind_debug_block_upstream_cancels(0);
    R_CHECK(&rig, unsubscribed == 1);
    R_CHECK(&rig, s_ps.done_count == 1 && s_ps.done_code == 0x12);
    R_CHECK(&rig, s_ps.objects == 1);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) printf("PASS: relay_publish_cancel_owed d%u\n", (unsigned)v);
    return rig.failures;
}

/* The downstream reset a malformed Track owes on a stream that carries a
 * complete object but not its whole subgroup is kept when the session cannot
 * take it: the stream ends in that reset once the queue drains, never in a
 * FIN (draft-22 §11.3.2) or another code. The downstream session's queue is
 * held full by stepping every pair but the subscriber's. */
static int
relay_malformed_reset_owed(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_max_actions = 6;
    mrig_t w;
    bool ok = mrig_open(&w, &a, v);
    if (!ok) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: relay_malformed_reset_owed fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t first[3] = { 'o', 'n', 'e' };
    static const uint8_t other[3] = { 't', 'w', 'o' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, first, sizeof(first));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);              /* stream (0,0) open */
    /* Fill the relay's queue to the subscriber with objects of other groups
     * while the subscriber's pair is not stepped. */
    for (uint64_t g = 1; g <= 4; g++) {
        moq_subgroup_handle_t hg = mrig_open_sg(&w, g, 0, false);
        mrig_write(&w, hg, 0, first, sizeof(first));
        relay_bind_test_rig_pump_except(&w.rig, w.sub, 1);
        relay_bind_test_peer_drain(&w.rig, w.pub, &w.pub_ps, true, false, 0, 0);
    }
    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, 1, false);
    mrig_write(&w, h1, 0, other, sizeof(other));
    for (int i = 0; i < 4; i++) {
        relay_bind_test_rig_pump_except(&w.rig, w.sub, 1);
        relay_bind_test_peer_drain(&w.rig, w.pub, &w.pub_ps, true, false, 0, 0);
    }
    for (int i = 0; i < 6; i++) mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.done_count == 1 && w.s_ps.done_code == 0x12);
    bool reset00 = false;
    for (int i = 0; i < w.s_ps.subgroup_resets && i < 16; i++)
        if (w.s_ps.reset_gsg[i][0] == 0 && w.s_ps.reset_gsg[i][1] == 0) reset00 = true;
    bool fin00 = false;
    for (int i = 0; i < w.s_ps.finished_count; i++)
        if (w.s_ps.finished_gsg[i][0] == 0 && w.s_ps.finished_gsg[i][1] == 0) fin00 = true;
    R_CHECK(&w.rig, reset00 && !fin00);
    R_CHECK(&w.rig, !w.s_ps.session_closed);
    relay_bind_test_max_actions = 0;
    return mrig_close(&w, &a, "relay_malformed_reset_owed", v);
}


/* One subgroup, two upstream streams: the origin resets the first after
 * object 0 and reopens the subgroup on a new stream for object 1, then FINs
 * it (draft-22 §11.3.2: a stream whose continuity is unknown is reset and a
 * new one opened). The subscriber's first stream ends in the reset; the
 * second carries object 1 and ends in the FIN, exactly once. A subscriber
 * that joins while the new stream is open gets object 0 on a stream that is
 * reset, object 1 on a new stream, and no terminal for it until the FIN. */
static int
relay_subgroup_reopen(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    mrig_t w;
    if (!mrig_open(&w, &a, v)) {
        printf("FAIL: relay_subgroup_reopen fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t body[2] = { 'r', 'o' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, body, sizeof(body));
    mrig_settle(&w);
    R_CHECK(&w.rig, moq_session_reset_subgroup(w.pub->peer, h0, 0x0, w.rig.now) == MOQ_OK);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1 && w.s_ps.subgroup_resets == 1);
    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h1, 1, body, sizeof(body));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 2);

    /* A late subscriber while the new upstream stream is open. */
    conn_t *late = relay_bind_test_rig_connect(&w.rig, v);
    R_CHECK(&w.rig, late != NULL);
    peer_state_t l_ps;
    memset(&l_ps, 0, sizeof(l_ps));
    if (late != NULL) {
        relay_bind_test_rig_pump(&w.rig, 4);
        moq_bytes_t nsp[2] = { B("live"), B("cam") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
        scfg.track_name = B("v");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t lh;
        R_CHECK(&w.rig, moq_session_subscribe(late->peer, &scfg, w.rig.now, &lh) == MOQ_OK);
        for (int i = 0; i < 6; i++) {
            relay_bind_test_rig_pump(&w.rig, 4);
            relay_bind_test_peer_drain(&w.rig, late, &l_ps, false, false, 0, 0);
        }
        R_CHECK(&w.rig, l_ps.objects == 2);
        R_CHECK(&w.rig, l_ps.subgroup_resets == 1);      /* object 0's stream  */
        R_CHECK(&w.rig, l_ps.subgroup_finished == 0);    /* the new one: open  */
    }

    R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h1, w.rig.now) == MOQ_OK);
    mrig_settle(&w);
    if (late != NULL) {
        for (int i = 0; i < 4; i++) {
            relay_bind_test_rig_pump(&w.rig, 4);
            relay_bind_test_peer_drain(&w.rig, late, &l_ps, false, false, 0, 0);
        }
        R_CHECK(&w.rig, l_ps.subgroup_finished == 1 && l_ps.subgroup_resets == 1);
    }
    R_CHECK(&w.rig, w.s_ps.subgroup_finished == 1);      /* the new stream, once */
    R_CHECK(&w.rig, w.s_ps.subgroup_resets == 1);
    R_CHECK(&w.rig, w.s_ps.done_count == 0 && !w.s_ps.session_closed);
    return mrig_close(&w, &a, "relay_subgroup_reopen", v);
}

/* MAX_CACHE_DURATION (draft-22 §10.3) on a subgroup's tail: object 0 is
 * delivered on an open downstream stream, object 1 arrives and the upstream
 * stream FINs while the subscriber's queue is held full, and object 1
 * expires before it can start. It is omitted, so the downstream stream ends
 * in a reset, not a FIN (§11.3.2). */
static int
relay_expired_tail_reset(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    static const uint8_t props_vi64[3] = { 0x04, 0x81, 0xF4 };   /* 500 ms */
    static const uint8_t props_varint[3] = { 0x04, 0x41, 0xF4 };
    relay_bind_test_accept_track_properties = (moq_bytes_t){
        v == MOQ_VERSION_DRAFT_16 ? props_varint : props_vi64, 3 };
    relay_bind_test_max_actions = 6;
    mrig_t w;
    bool ok = mrig_open(&w, &a, v);
    relay_bind_test_accept_track_properties = (moq_bytes_t){ NULL, 0 };
    if (!ok) {
        relay_bind_test_max_actions = 0;
        printf("FAIL: relay_expired_tail_reset fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t body[2] = { 'e', 't' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, body, sizeof(body));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    for (uint64_t g = 1; g <= 4; g++) {
        moq_subgroup_handle_t hg = mrig_open_sg(&w, g, 0, false);
        mrig_write(&w, hg, 0, body, sizeof(body));
        relay_bind_test_rig_pump_except(&w.rig, w.sub, 1);
        relay_bind_test_peer_drain(&w.rig, w.pub, &w.pub_ps, true, false, 0, 0);
    }
    mrig_write(&w, h0, 1, body, sizeof(body));
    R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h0, w.rig.now) == MOQ_OK);
    for (int i = 0; i < 3; i++) {
        relay_bind_test_rig_pump_except(&w.rig, w.sub, 1);
        relay_bind_test_peer_drain(&w.rig, w.pub, &w.pub_ps, true, false, 0, 0);
    }
    w.rig.now += 600000;                                  /* object 1 expires */
    for (int i = 0; i < 6; i++) mrig_settle(&w);
    bool got01 = false;
    for (int i = 0; i < w.s_ps.dlv_n; i++)
        if (w.s_ps.dlv[i].group == 0 && w.s_ps.dlv[i].object_id == 1) got01 = true;
    bool reset00 = false, fin00 = false;
    for (int i = 0; i < w.s_ps.subgroup_resets && i < 16; i++)
        if (w.s_ps.reset_gsg[i][0] == 0 && w.s_ps.reset_gsg[i][1] == 0) reset00 = true;
    for (int i = 0; i < w.s_ps.finished_count; i++)
        if (w.s_ps.finished_gsg[i][0] == 0 && w.s_ps.finished_gsg[i][1] == 0) fin00 = true;
    R_CHECK(&w.rig, !got01);
    R_CHECK(&w.rig, reset00 && !fin00);
    R_CHECK(&w.rig, !w.s_ps.session_closed);
    relay_bind_test_max_actions = 0;
    return mrig_close(&w, &a, "relay_expired_tail_reset", v);
}

/* An object the log refuses (larger than its whole budget) leaves a hole in
 * its subgroup: the downstream stream carrying the objects before it is
 * reset, the objects after it go on a new stream, and the subgroup's FIN is
 * not passed on (a FIN says every object was delivered). */
static int
relay_ingest_hole(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_log_max_bytes = 4096;
    mrig_t w;
    if (!mrig_open(&w, &a, v)) {
        relay_bind_test_log_max_bytes = 0;
        printf("FAIL: relay_ingest_hole fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static uint8_t big[6000];
    static const uint8_t small[2] = { 's', 'm' };
    moq_subgroup_handle_t h = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h, 0, small, sizeof(small));
    mrig_settle(&w);
    mrig_write(&w, h, 1, big, sizeof(big));          /* refused: CAPACITY */
    mrig_write(&w, h, 2, small, sizeof(small));
    R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h, w.rig.now) == MOQ_OK);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 2);            /* objects 0 and 2      */
    R_CHECK(&w.rig, w.s_ps.subgroup_finished == 0);  /* never FINed          */
    R_CHECK(&w.rig, w.s_ps.subgroup_resets == 2);    /* each stream reset    */
    R_CHECK(&w.rig, !w.s_ps.session_closed);
    relay_bind_test_log_max_bytes = 0;
    return mrig_close(&w, &a, "relay_ingest_hole", v);
}

/* The downstream subgroup is opened in the upstream stream's form: an
 * upstream PROPERTIES subgroup whose first object has none still carries a
 * later object's properties (the form comes from the stream, not from the
 * first record). */
static int
relay_properties_form(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    mrig_t w;
    if (!mrig_open(&w, &a, v)) {
        printf("FAIL: relay_properties_form fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t body[2] = { 'p', 'q' };
    moq_subgroup_handle_t h = mrig_open_sg(&w, 0, 0, true);
    mrig_write(&w, h, 0, body, sizeof(body));          /* no properties     */
    mrig_settle(&w);
    static const uint8_t prop[2] = { 0x20, 0x07 };   /* type 0x20, value 7 */
    moq_rcbuf_t *pl = NULL, *pr = NULL;
    R_CHECK(&w.rig, moq_rcbuf_create(&w.rig.alloc->vt, body, 2, &pl) == MOQ_OK);
    R_CHECK(&w.rig, moq_rcbuf_create(&w.rig.alloc->vt, prop, 2, &pr) == MOQ_OK);
    moq_object_cfg_t oc;
    moq_object_cfg_init(&oc);
    oc.object_id = 1;
    oc.payload = pl;
    oc.properties = pr;
    R_CHECK(&w.rig, moq_session_write_object_ex(w.pub->peer, h, &oc, w.rig.now) ==
                        MOQ_OK);
    moq_rcbuf_decref(pl);
    moq_rcbuf_decref(pr);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 2);
    R_CHECK(&w.rig, w.s_ps.last_props_len == 2 && w.s_ps.last_props[0] == 0x20);
    R_CHECK(&w.rig, w.s_ps.done_count == 0 && w.s_ps.subgroup_resets == 0);
    return mrig_close(&w, &a, "relay_properties_form", v);
}

/* MAX_CACHE_DURATION (§10.3): an Object retained longer than the source's
 * duration is not started downstream — a late subscriber's replay skips it
 * and starts the subgroup's next object on its own stream. */
static int
relay_max_cache_duration(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    /* MAX_CACHE_DURATION = 500 ms: a two-byte value, a vi64 (81 f4) on
     * draft-18/22 and a QUIC varint (41 f4) on draft-16. Every pump advances
     * the rig clock by 1 ms, so the delivery steps below stay well inside
     * it. */
    static const uint8_t props_vi64[3] = { 0x04, 0x81, 0xF4 };
    static const uint8_t props_varint[3] = { 0x04, 0x41, 0xF4 };
    relay_bind_test_accept_track_properties = (moq_bytes_t){
        v == MOQ_VERSION_DRAFT_16 ? props_varint : props_vi64, 3 };
    mrig_t w;
    bool ok = mrig_open(&w, &a, v);
    relay_bind_test_accept_track_properties = (moq_bytes_t){ NULL, 0 };
    if (!ok) {
        printf("FAIL: relay_max_cache_duration fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t body[2] = { 'c', 'd' };
    moq_subgroup_handle_t h = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h, 0, body, sizeof(body));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 1);            /* in time: delivered */
    w.rig.now += 600000;                             /* object 0 expires   */
    mrig_write(&w, h, 1, body, sizeof(body));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.objects == 2);            /* live: delivered    */

    /* A late subscriber from Group 0: object 0 has expired, object 1 not. */
    conn_t *late = relay_bind_test_rig_connect(&w.rig, v);
    R_CHECK(&w.rig, late != NULL);
    if (late == NULL) return mrig_close(&w, &a, "relay_max_cache_duration", v);
    relay_bind_test_rig_pump(&w.rig, 4);
    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_subscribe_cfg_t scfg;
    moq_subscribe_cfg_init(&scfg);
    scfg.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    scfg.track_name = B("v");
    scfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    scfg.start_group = 0;
    scfg.start_object = 0;
    moq_subscription_t lh;
    R_CHECK(&w.rig, moq_session_subscribe(late->peer, &scfg, w.rig.now, &lh) == MOQ_OK);
    peer_state_t l_ps;
    memset(&l_ps, 0, sizeof(l_ps));
    for (int i = 0; i < 4; i++) {
        relay_bind_test_rig_pump(&w.rig, 8);
        relay_bind_test_peer_drain(&w.rig, late, &l_ps, false, false, 0, 0);
    }
    R_CHECK(&w.rig, l_ps.subscribe_ok);
    R_CHECK(&w.rig, l_ps.objects == 1 && l_ps.last_object == 1);

    /* A FETCH does not serve the expired object from the cache either. */
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    fc.track_name = B("v");
    fc.start_group = 0;
    fc.start_object = 0;
    fc.end_group = 0;
    fc.end_object = 2;   /* through object 1 on every draft */
    moq_fetch_t fh;
    R_CHECK(&w.rig, moq_session_fetch(late->peer, &fc, w.rig.now, &fh) == MOQ_OK);
    for (int i = 0; i < 6; i++) {
        relay_bind_test_rig_pump(&w.rig, 8);
        relay_bind_test_peer_drain(&w.rig, late, &l_ps, false, false, 0, 0);
    }
    /* The expired Object is unknown, so the relay asks the origin for it
     * within the FETCH's wait budget; nothing past it is served and the FETCH
     * does not end meanwhile. This origin never answers: once the budget is
     * spent the run from {0,0} is Timed-Out on draft-22, and on the drafts
     * without that marker it is Unknown -- never passed over as if it did not
     * exist (object 1 has expired by then as well). */
    R_CHECK(&w.rig, l_ps.fdlv_n == 0 && l_ps.fgap_n == 0 && l_ps.fetch_complete == 0);
    w.rig.now += 60000000;
    for (int i = 0; i < 6; i++) {
        relay_bind_test_rig_pump(&w.rig, 8);
        relay_bind_test_peer_drain(&w.rig, late, &l_ps, false, false, 0, 0);
    }
    printf("relay_max_cache_duration(d%u): objects=%d gaps=%d kind=%u at %llu:%llu complete=%d\n",
           (unsigned)v, l_ps.fdlv_n, l_ps.fgap_n, l_ps.fgap_n ? (unsigned)l_ps.fgap[0].kind : 0u,
           l_ps.fgap_n ? (unsigned long long)l_ps.fgap[0].group : 0ull,
           l_ps.fgap_n ? (unsigned long long)l_ps.fgap[0].object : 0ull, l_ps.fetch_complete);
    R_CHECK(&w.rig, l_ps.fdlv_n == 0);
    R_CHECK(&w.rig, l_ps.fgap_n >= 1 &&
                        l_ps.fgap[0].kind == (v == MOQ_VERSION_DRAFT_22 ? MOQ_FETCH_RANGE_TIMED_OUT
                                                                       : MOQ_FETCH_RANGE_UNKNOWN) &&
                        l_ps.fgap[0].group == 0 && l_ps.fgap[0].object == 0);
    R_CHECK(&w.rig, l_ps.fetch_complete == 1);
    return mrig_close(&w, &a, "relay_max_cache_duration", v);
}


/* The relay's own draft-22 upstream session finds the Track malformed (a
 * Subgroup reopened at another Publisher Priority, §12.1 condition 1): it
 * cancels the upstream subscription, and the relay ends the downstream
 * subscription with PUBLISH_DONE MALFORMED_TRACK. */
static int
relay_upstream_malformed(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    mrig_t w;
    if (!mrig_open(&w, &a, v)) {
        printf("FAIL: relay_upstream_malformed fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t body[2] = { 'u', 'm' };
    for (uint64_t k = 0; k < 2; k++) {
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 0;
        sc.subgroup_id = 0;
        sc.publisher_priority = (uint8_t)(10 + k);
        moq_subgroup_handle_t h;
        R_CHECK(&w.rig, moq_session_open_subgroup(w.pub->peer, w.pub_ps.up_sub, &sc,
                                                  w.rig.now, &h) == MOQ_OK);
        mrig_write(&w, h, k, body, sizeof(body));
        if (k == 0)
            R_CHECK(&w.rig, moq_session_close_subgroup(w.pub->peer, h, w.rig.now) ==
                                MOQ_OK);
        mrig_settle(&w);
    }
    R_CHECK(&w.rig, w.s_ps.objects == 1);
    R_CHECK(&w.rig, w.s_ps.done_count == 1);
    R_CHECK(&w.rig, w.s_ps.done_code == 0x12);
    R_CHECK(&w.rig, w.pub_ps.unsubscribed == 1);
    R_CHECK(&w.rig, !w.s_ps.session_closed && !w.pub_ps.session_closed);
    return mrig_close(&w, &a, "relay_upstream_malformed", v);
}

/* A FETCH the relay is serving from its cache when the Track turns out
 * malformed is reset with MALFORMED_TRACK (§12.1). The FETCH is held at an
 * object still arriving (relay ingest streams), so it is mid-response when a
 * differing duplicate arrives. */
static int
relay_malformed_fetch_reset(moq_version_t v)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    relay_bind_test_server_streaming_objects = true;
    mrig_t w;
    bool ok = mrig_open(&w, &a, v);
    relay_bind_test_server_streaming_objects = false;
    if (!ok) {
        printf("FAIL: relay_malformed_fetch_reset fixture\n");
        relay_bind_test_rig_destroy(&w.rig);
        return 1;
    }
    static const uint8_t first[3] = { 'o', 'n', 'e' };
    static const uint8_t other[3] = { 't', 'w', 'o' };
    moq_subgroup_handle_t h0 = mrig_open_sg(&w, 0, 0, false);
    mrig_write(&w, h0, 0, first, sizeof(first));
    /* Object 1 begins and stays partial. */
    R_CHECK(&w.rig, moq_session_begin_object(w.pub->peer, h0, 1, 64, w.rig.now) ==
                        MOQ_OK);
    moq_rcbuf_t *part = NULL;
    R_CHECK(&w.rig, moq_rcbuf_create(&w.rig.alloc->vt, first, 3, &part) == MOQ_OK);
    R_CHECK(&w.rig, moq_session_write_object_data(w.pub->peer, h0, part, w.rig.now) ==
                        MOQ_OK);
    moq_rcbuf_decref(part);
    mrig_settle(&w);

    moq_bytes_t nsp[2] = { B("live"), B("cam") };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 2 };
    fc.track_name = B("v");
    fc.start_group = 0;
    fc.start_object = 0;
    fc.end_group = 0;
    fc.end_object = 3;
    moq_fetch_t fh;
    R_CHECK(&w.rig, moq_session_fetch(w.sub->peer, &fc, w.rig.now, &fh) == MOQ_OK);
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.fetch_ok);
    R_CHECK(&w.rig, w.s_ps.fetch_resets == 0);

    moq_subgroup_handle_t h1 = mrig_open_sg(&w, 0, 1, false);
    mrig_write(&w, h1, 0, other, sizeof(other));
    mrig_settle(&w);
    R_CHECK(&w.rig, w.s_ps.fetch_resets == 1);
    R_CHECK(&w.rig, w.s_ps.fetch_reset_code == 0x12);
    R_CHECK(&w.rig, w.s_ps.done_count == 1 && w.s_ps.done_code == 0x12);
    R_CHECK(&w.rig, !w.s_ps.session_closed);
    return mrig_close(&w, &a, "relay_malformed_fetch_reset", v);
}

/* The origin makes the catalog Track malformed on the relay's live
 * subscription: Object 100 of Group 0 in Subgroup 0, then the same Object in
 * Subgroup 1 with other bytes (draft-22 §7.1). */
static void
jf_cat_duplicate(jf_t *j)
{
    for (uint64_t sg = 0; sg < 2; sg++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = sg;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t h;
        moq_result_t rc;
        JF_ORIGIN_RETRY(j, moq_session_open_subgroup(j->o.cn->peer, j->o.cat_sub, &sgc,
                                                     j->rig.now, &h), rc);
        R_CHECK(&j->rig, rc == MOQ_OK);
        uint8_t body[8];
        relay_bind_test_boot_fill(body, sizeof(body), (uint8_t)(0x40 + sg));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&j->rig, moq_rcbuf_create(&j->a.vt, body, sizeof(body), &pl) == MOQ_OK);
        JF_ORIGIN_RETRY(j, moq_session_write_object(j->o.cn->peer, h, 100, pl, j->rig.now),
                        rc);
        R_CHECK(&j->rig, rc == MOQ_OK);
        moq_rcbuf_decref(pl);
        JF_ORIGIN_RETRY(j, moq_session_close_subgroup(j->o.cn->peer, h, j->rig.now), rc);
        relay_bind_test_jf_cycle(j);
    }
}

/* A forwarded Joining FETCH whose response was collected completely, accepted
 * downstream and held mid-replay by downstream backpressure when the Track
 * turns out malformed (draft-22 §12.1: a relay "MUST ... reset any fetch
 * streams with Status Code MALFORMED_TRACK"): the held replay is not resumed;
 * the downstream FETCH is reset once with MALFORMED_TRACK and never FINed,
 * and an unrelated Track keeps flowing. */
static int
jf_malformed_collected_held(moq_version_t version)
{
    jf_t j;
    relay_bind_test_max_actions = 32;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 60)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    relay_bind_test_jf_origin_accept(&j, 0, 0, 61, false, false);
    for (uint64_t o = 0; o < 60; o++) {
        R_CHECK(&j.rig, relay_bind_test_jf_origin_write_raw(&j, 0, 0, o, (uint8_t)o, 8, false) == MOQ_OK);
        if ((o % 8) == 7) relay_bind_test_jf_cycle(&j);
    }
    relay_bind_test_jf_origin_end(&j, 0);
    moqr_bind_fwd_debug_t st;
    for (int i = 0; i < 30; i++) {
        relay_bind_test_jf_cycle(&j);
        relay_bind_test_jf_stats(&j, &st);
        if (st.completed == 1) break;
    }
    R_CHECK(&j.rig, st.completed == 1 && st.live == 1 && st.delivered == 0);
    j.d[0].cn->frozen = true;               /* the replay stays held */
    jf_cat_duplicate(&j);
    for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
    j.d[0].cn->frozen = false;
    for (int i = 0; i < 30 && j.d[0].reset + j.d[0].complete == 0; i++) relay_bind_test_jf_cycle(&j);
    for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.d[0].reset == 1 && j.d[0].reset_code == 0x12);
    R_CHECK(&j.rig, j.d[0].complete == 0 && j.d[0].error == 0);
    R_CHECK(&j.rig, j.d[0].objects < 60);
    R_CHECK(&j.rig, j.d[0].done == 1);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_malformed_collected_held");
}

/* The same before the upstream answered: the forwarded upstream FETCH is
 * cancelled and the downstream FETCH refused with MALFORMED_TRACK. */
static int
jf_malformed_pending(moq_version_t version)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    jf_cat_duplicate(&j);
    for (int i = 0; i < 20 && j.d[0].error + j.d[0].reset + j.d[0].complete == 0; i++)
        relay_bind_test_jf_cycle(&j);
    for (int i = 0; i < 4; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.d[0].error == 1 && j.d[0].error_code == 0x12);
    R_CHECK(&j.rig, j.d[0].ok == 0 && j.d[0].complete == 0 && j.d[0].reset == 0);
    R_CHECK(&j.rig, j.o.cancelled == 1);
    R_CHECK(&j.rig, j.d[0].done == 1);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_malformed_pending");
}

/* The forwarded upstream FETCH of a pending Joining FETCH, cancelled for a
 * malformed Track while the upstream session's send buffer still holds
 * earlier control output: the cancellation is kept and the origin sees
 * exactly one FETCH_CANCEL once the buffer drains, the downstream FETCH is
 * refused with MALFORMED_TRACK, and an unrelated Track on the same upstream
 * session keeps flowing. */
static int
jf_malformed_pending_send_buffer(moq_version_t version)
{
    jf_t j;
    if (!relay_bind_test_jf_setup(&j, version, 1, true, 1)) return 1 + j.rig.failures;
    relay_bind_test_jf_join(&j, &j.d[0]);
    relay_bind_test_jf_wait_requests(&j, 1);
    for (uint64_t sg = 0; sg < 2; sg++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = 0;
        sgc.subgroup_id = sg;
        sgc.publisher_priority = 100;
        moq_subgroup_handle_t h;
        moq_result_t rc;
        JF_ORIGIN_RETRY(&j, moq_session_open_subgroup(j.o.cn->peer, j.o.cat_sub, &sgc,
                                                      j.rig.now, &h), rc);
        R_CHECK(&j.rig, rc == MOQ_OK);
        uint8_t body[8];
        relay_bind_test_boot_fill(body, sizeof(body), (uint8_t)(0x40 + sg));
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&j.rig, moq_rcbuf_create(&j.a.vt, body, sizeof(body), &pl) == MOQ_OK);
        JF_ORIGIN_RETRY(&j, moq_session_write_object(j.o.cn->peer, h, 100, pl, j.rig.now),
                        rc);
        R_CHECK(&j.rig, rc == MOQ_OK);
        moq_rcbuf_decref(pl);
        JF_ORIGIN_RETRY(&j, moq_session_close_subgroup(j.o.cn->peer, h, j.rig.now), rc);
        if (sg == 0) relay_bind_test_jf_cycle(&j);
    }
    relay_bind_test_rig_step_pair(&j.rig, j.o.cn);
    fill_send_buffer(&j.rig, j.o.cn->rsess);
    relay_bind_test_rig_bind_pump_once(&j.rig);
    relay_bind_test_jf_origin_drain(&j.rig, &j.o);
    R_CHECK(&j.rig, j.o.cancelled == 0);   /* not sent yet */
    for (int i = 0; i < 20 && j.d[0].error + j.d[0].reset + j.d[0].complete == 0; i++)
        relay_bind_test_jf_cycle(&j);
    for (int i = 0; i < 6; i++) relay_bind_test_jf_cycle(&j);
    R_CHECK(&j.rig, j.d[0].error == 1 && j.d[0].error_code == 0x12);
    R_CHECK(&j.rig, j.o.cancelled == 1);
    R_CHECK(&j.rig, j.d[0].done == 1);
    relay_bind_test_jf_live_control(&j);
    return relay_bind_test_jf_teardown(&j, "fwd_malformed_pending_send_buffer");
}

/* A fill this relay is still serving for a downstream subscription when the
 * Track turns out malformed (draft-22 §12.1): the fill stream is reset with
 * MALFORMED_TRACK itself, not merely ended as a child of its parent's
 * terminal, and the subscription ends with PUBLISH_DONE MALFORMED_TRACK. The
 * origin has not answered the fill's upstream FETCH, so the fill is open. */
static int
relay_malformed_fill(void)
{
    ca_t a;
    relay_bind_test_ca_init(&a);
    rig_t rig;
    if (relay_bind_test_rig_create(&rig, &a) != MOQR_OK) {
        printf("FAIL: relay_malformed_fill rig create\n");
        return 1;
    }
    d22_track_t t;
    relay_bind_test_d22_track_setup(&rig, &t, MOQ_VERSION_DRAFT_22, 2);
    conn_t *sub = relay_bind_test_rig_connect(&rig, MOQ_VERSION_DRAFT_22);
    R_CHECK(&rig, sub != NULL);
    if (sub == NULL) { relay_bind_test_rig_destroy(&rig); return rig.failures; }
    relay_bind_test_rig_pump(&rig, 4);
    moq_location_filter_cfg_t loc, floc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_location_filter_cfg_init(&floc);
    floc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &floc;
    moq_subscription_t h;
    R_CHECK(&rig, relay_bind_test_d22_subscribe(&rig, sub, &t, &loc, &fill, &h) == MOQ_OK);
    for (int i = 0; i < 12; i++) {
        relay_bind_test_rig_cycle(&rig);
        (void)relay_bind_test_d22_fill_origin_tail(&rig, &t, false);
    }
    peer_state_t s_ps;
    memset(&s_ps, 0, sizeof(s_ps));
    relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    R_CHECK(&rig, s_ps.subscribe_ok && s_ps.fill_objects == 3 && s_ps.fill_terminals == 0);

    /* Object 0 of Group 2 in Subgroup 0, then in Subgroup 1 with other bytes. */
    for (uint64_t sg = 0; sg < 2; sg++) {
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = 2;
        sc.subgroup_id = sg;
        moq_subgroup_handle_t sh;
        R_CHECK(&rig, moq_session_open_subgroup(t.pub->peer, t.pub_ps.up_sub, &sc,
                                                rig.now, &sh) == MOQ_OK);
        uint8_t body[2] = { 'f', (uint8_t)('0' + sg) };
        moq_rcbuf_t *pl = NULL;
        R_CHECK(&rig, moq_rcbuf_create(&a.vt, body, sizeof(body), &pl) == MOQ_OK);
        R_CHECK(&rig, moq_session_write_object(t.pub->peer, sh, 0, pl, rig.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
        for (int i = 0; i < 4; i++) {
            relay_bind_test_rig_cycle(&rig);
            (void)relay_bind_test_d22_fill_origin_tail(&rig, &t, false);
            relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
        }
    }
    for (int i = 0; i < 8; i++) {
        relay_bind_test_rig_cycle(&rig);
        (void)relay_bind_test_d22_fill_origin_tail(&rig, &t, false);
        relay_bind_test_peer_drain(&rig, sub, &s_ps, false, false, 0, 0);
    }
    R_CHECK(&rig, s_ps.done_count == 1 && s_ps.done_code == 0x12);
    R_CHECK(&rig, s_ps.fill_terminals == 1);
    R_CHECK(&rig, s_ps.fill_term_reason == MOQ_FILL_PEER_CANCELLED_OR_RESET &&
                      s_ps.fill_term_code == 0x12);
    R_CHECK(&rig, !s_ps.session_closed);
    relay_bind_test_rig_destroy(&rig);
    R_CHECK(&rig, a.live == 0);
    if (rig.failures == 0) printf("PASS: relay_malformed_fill\n");
    return rig.failures;
}

static int
relay_track_integrity(moq_version_t v)
{
    int failures = 0;
    for (int differs = 0; differs < 2; differs++) {
        failures += relay_dup_payload(v, false, 0, differs != 0);
        failures += relay_dup_payload(v, true, 1, differs != 0);
        failures += relay_dup_payload(v, true, 3, differs != 0);
    }
    failures += relay_existing_to_missing(v);
    failures += relay_dup_first_byte(v, true);
    failures += relay_dup_first_byte(v, false);
    failures += relay_dup_owner_policy(v, 0);
    failures += relay_dup_owner_policy(v, 1);
    if (v == MOQ_VERSION_DRAFT_22) {
        failures += relay_dup_copy_interleaved();
        failures += relay_dup_copy_reset_live();
        failures += relay_malformed_fill();
    }
    if (v != MOQ_VERSION_DRAFT_22) {
        failures += jf_malformed_collected_held(v);
        failures += jf_malformed_pending(v);
        failures += jf_malformed_pending_send_buffer(v);
        failures += relay_cancel_send_buffer(v);
    }
    failures += relay_upstream_cancel_owed(v);
    failures += relay_cancel_hard_failure(v);
    failures += relay_publish_cancel_owed(v);
    failures += relay_malformed_reset_owed(v);
    failures += relay_subgroup_reopen(v);
    failures += relay_expired_tail_reset(v);
    return failures;
}

int
main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "--track-integrity") == 0) {
        int failures = 0;
        const moq_version_t drafts[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                         MOQ_VERSION_DRAFT_22 };
        for (size_t i = 0; i < sizeof(drafts) / sizeof(drafts[0]); i++)
            failures += relay_track_integrity(drafts[i]);
        return failures == 0 ? 0 : 1;
    }
    int failures = 0;
    {
        const moq_version_t drafts[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                      MOQ_VERSION_DRAFT_22 };
        for (size_t i = 0; i < sizeof(drafts) / sizeof(drafts[0]); i++) {
            failures += relay_malformed_duplicate(drafts[i], 0);
            failures += relay_malformed_duplicate(drafts[i], 1);
            failures += relay_malformed_duplicate(drafts[i], 2);
            failures += relay_ingest_hole(drafts[i]);
            failures += relay_properties_form(drafts[i]);
            failures += relay_max_cache_duration(drafts[i]);
            if (drafts[i] != MOQ_VERSION_DRAFT_16)
                failures += relay_malformed_fetch_reset(drafts[i]);
            failures += relay_track_integrity(drafts[i]);
        }
        failures += relay_upstream_malformed(MOQ_VERSION_DRAFT_22);
    }
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
