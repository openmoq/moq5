#include "media_sender_internal.h"
#include "media_sender_catalog_internal.h"
#include "media_sender_complete_internal.h"
#include "media_sender_mpeg2ts_internal.h"
#include "media_sender_timeline_internal.h"
#include "media_sender_tracks_internal.h"

/* -- Catalog build (network thread) ----------------------------------- */

/* MSF §5.1.2 generatedAt: the wallclock time the catalog instance was generated,
 * in milliseconds since the Unix epoch. The service tier may read the wall clock
 * (the sans-I/O core/media layers never do). clock_gettime(CLOCK_REALTIME) does
 * not fail for a valid, always-present clock id; on the practically-unreachable
 * error we fall back to 0 rather than invent a new failure path for an optional
 * catalog field. */
static uint64_t sender_now_epoch_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
        return 0;
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}


/* The wallclock a catalog instance is stamped with. Identical to
 * sender_now_epoch_ms() in a shipping build; under the testing define an
 * instance may pin it, which is what lets a test state the two timestamps it
 * expects instead of racing a millisecond boundary. */
static uint64_t sender_catalog_now_ms(const moq_media_sender_t *s)
{
#ifdef MOQ_MEDIA_SENDER_TESTING
    if (s->test_clock_set)
        return s->test_clock_ms;
#else
    (void)s;
#endif
    return sender_now_epoch_ms();
}


void track_fill_msf(const moq_media_track_t *t, moq_msf_track_t *m)
{
    memset(m, 0, sizeof(*m));
    m->struct_size = sizeof(*m);
    m->name = t->name;

    /* Generated MSF §7 media-timeline track (§7.2): packaging "mediatimeline",
     * application/json mimeType, the 'depends' list, and NO eventType. */
    if (t->is_media_timeline) {
        m->packaging = (moq_bytes_t){ (const uint8_t *)"mediatimeline", 13 };
        m->is_live = t->is_live;
        m->has_mime_type = true;
        m->mime_type = (moq_bytes_t){ (const uint8_t *)"application/json", 16 };
        m->depends = t->depends;
        m->depends_count = t->depends_count;
        return;
    }

    /* Generated CMSF SAP timeline track (CMSF §3.6.1, MSF §8.2): a fixed
     * eventtimeline shape carrying eventType, the application/json mimeType and
     * the 'depends' list (the single media track it applies to). */
    if (t->is_event_timeline) {
        m->packaging = (moq_bytes_t){ (const uint8_t *)"eventtimeline", 13 };
        m->is_live = t->is_live;
        m->has_event_type = true;
        m->event_type =
            (moq_bytes_t){ (const uint8_t *)"org.ietf.moq.cmsf.sap", 21 };
        m->has_mime_type = true;
        m->mime_type = (moq_bytes_t){ (const uint8_t *)"application/json", 16 };
        m->depends = t->depends;
        m->depends_count = t->depends_count;
        return;
    }

    if (t->packaging == MOQ_MEDIA_PACKAGING_CMAF)
        m->packaging = (moq_bytes_t){ (const uint8_t *)"cmaf", 4 };
    else if (t->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS)
        m->packaging = (moq_bytes_t){ (const uint8_t *)"mpeg2ts", 7 };
    else
        m->packaging = (moq_bytes_t){ (const uint8_t *)"loc", 3 };
    /* draft-gregoire-moq-msfts: the media type of a TS track, and its
     * mpeg2ts* fields from the sender-owned record copy. */
    if (t->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS) {
        m->has_mime_type = true;
        m->mime_type = (moq_bytes_t){ (const uint8_t *)"video/mp2t", 10 };
    }
    m->extensions = t->ext;
    m->extension_count = t->ext_count;
    m->is_live = t->is_live;
    m->has_role = true;
    m->role = t->role.len ? t->role : default_role(t->media_type);
    if (t->codec.len) { m->has_codec = true; m->codec = t->codec; }
    /* Init data is wired by the caller: root initDataList[] + initRef. */
    if (t->width)  { m->has_width = true;  m->width = t->width; }
    if (t->height) { m->has_height = true; m->height = t->height; }
    if (t->samplerate) { m->has_samplerate = true; m->samplerate = t->samplerate; }
    if (t->bitrate)    { m->has_bitrate = true;    m->bitrate = t->bitrate; }
    if (t->framerate_millis) {
        m->has_framerate = true;
        m->framerate_millis = t->framerate_millis;
    }
    if (t->timescale) { m->has_timescale = true; m->timescale = t->timescale; }
    if (t->channel_config.len) {
        m->has_channel_config = true;
        m->channel_config = t->channel_config;
    }
    if (t->lang.len) { m->has_lang = true; m->lang = t->lang; }
    if (t->has_label) { m->has_label = true; m->label = t->label; }
    /* MSF §5.2.35 track duration (VOD/non-live track; add_track enforces that it
     * is never set together with is_live). */
    if (t->has_track_duration) {
        m->has_track_duration = true;
        m->track_duration_ms = t->track_duration_ms;
    }
    /* CMSF §3.2 alternate group: carried verbatim onto the catalog track entry
     * (the lower MSF encoder emits "altGroup"). */
    if (t->has_alt_group) { m->has_alt_group = true; m->alt_group = t->alt_group; }
    if (t->has_render_group) {
        m->has_render_group = true;
        m->render_group = t->render_group;
    }
    /* CMSF authoring metadata (borrows the track's owned copies). */
    if (t->has_max_grp_sap) { m->has_max_grp_sap = true; m->max_grp_sap = t->max_grp_sap; }
    if (t->has_max_obj_sap) { m->has_max_obj_sap = true; m->max_obj_sap = t->max_obj_sap; }
    if (t->cp_ref_id_count) {
        m->cp_ref_ids = t->cp_ref_ids;
        m->cp_ref_id_count = t->cp_ref_id_count;
    }
}


/* Encode the configured tracks into an MSF catalog JSON rcbuf. Returns
 * MOQ_OK with *out set, or an error (catalog tracks excluded). */
/* A track belongs in the published catalog when it is a non-catalog, non-removed
 * track. With only_registered (the post-ready publish path), it must ALSO have a
 * registered publisher track and no pending publication selection. The catalog
 * must not advertise a registration/PUBLISH still awaiting admission (e.g. a
 * same-name replacement). The white-box build path passes false. */
bool track_in_catalog(const moq_media_track_t *t, bool only_registered)
{
    if (t->is_catalog || t->removed || t->auth_failed) return false;
    if (only_registered && (!t->pub_track || t->publish_auth.prepared)) return false;
    return true;
}


/* Snapshot the track POINTER vector under s->mu. The network thread walks tracks
 * outside s->mu (it makes publisher calls that must not hold the lock), but the
 * app thread can realloc s->tracks (sender_tracks_push) concurrently -- which
 * would free the vector storage a bare unlocked walk is mid-iteration over.
 * Track OBJECTS live until sender destroy, so copying just the pointers under the
 * lock is enough: the snapshot's entries stay valid for the whole walk. Free with
 * sender_tracks_release().
 *
 * Returns false ONLY on allocation failure -- distinct from a genuine zero-track
 * snapshot (true, *out_snap == NULL, *out_count == 0). Callers MUST NOT treat a
 * NOMEM failure as "no tracks": doing so would silently publish an empty catalog,
 * skip teardown/registration/demand resync, or let a conversion catalog publish
 * before Track Ended. On false a caller fatals or preserves its gate. */
bool sender_tracks_snapshot(moq_media_sender_t *s,
                            moq_media_track_t ***out_snap,
                            size_t *out_count)
{
    pthread_mutex_lock(&s->mu);
    size_t n = s->track_count;
    moq_media_track_t **snap = NULL;
    bool ok = true;
    if (n > 0) {
        snap = (moq_media_track_t **)s->alloc.alloc(
            n * sizeof(*snap), s->alloc.ctx);
        if (snap) memcpy(snap, s->tracks, n * sizeof(*snap));
        else ok = false;   /* alloc failure -- NOT "zero tracks" */
    }
    pthread_mutex_unlock(&s->mu);
    *out_snap  = ok ? snap : NULL;
    *out_count = ok ? n : 0;
    return ok;
}


void sender_tracks_release(moq_media_sender_t *s,
                           moq_media_track_t **snap, size_t n)
{
    if (snap) s->alloc.free(snap, n * sizeof(*snap), s->alloc.ctx);
}


/* Builds the catalog JSON from s->tracks at an EXPLICIT generatedAt. CALLER
 * MUST HOLD s->mu: it walks s->tracks and reads per-track fields (name/desc/
 * removed/pub_track), so the lock keeps it from observing a half-applied
 * app-thread add/remove.
 *
 * The stamp is a parameter rather than a clock read so the two kinds of build
 * stay distinguishable: a REAL build (initial/mutation/refresh) chooses one
 * fresh stamp and carries it with the buffer it encoded, while a COMPARISON
 * build is handed the published stamp and never consults the clock. */
/* Every service companion carries nonempty initialization data in the root
 * initDataList with track initRef (MSF-01 5.1.7, 5.2.13). This one rule governs
 * independent catalogs, stable ID assignment and the deltaUpdate fallback. */
bool sender_init_by_reference(const moq_media_track_t *t)
{
    return t->init_data.len > 0;
}


/* The first Group ID of every track of this sender instance, media,
 * generated timeline and catalog alike (sender_resolve_group_base). */
uint64_t sender_group_base(const moq_media_sender_t *s)
{
    return s->group_base;
}


/* Whether a post-ready catalog change can still be given a Group ID. A change
 * made while a generation is already owed (catalog_dirty) coalesces into it;
 * otherwise it needs the group after the last committed or staged one, which
 * must not pass MOQ_QUIC_VARINT_MAX (the publisher's object write-path bound
 * on every profile; the ID space never wraps). Checked by every catalog-mutating call before it
 * changes anything, so an exhausted catalog refuses the change with
 * MOQ_ERR_LIMIT instead of failing later on the network thread.
 * CALLER MUST HOLD s->mu. */
bool sender_catalog_group_available(const moq_media_sender_t *s)
{
    if (!s->ready || s->catalog_dirty) return true;
    uint64_t last = s->pending_obj_count > 0 ? s->pending_group
                                             : s->catalog_group;
    return last < MOQ_QUIC_VARINT_MAX;
}


static moq_result_t sender_build_catalog_at(moq_media_sender_t *s,
                                            moq_rcbuf_t **out,
                                            bool only_registered,
                                            uint64_t generated_at)
{
    *out = NULL;
    /* Count the tracks that belong in the catalog. */
    size_t n = 0;
    for (size_t i = 0; i < s->track_count; i++)
        if (track_in_catalog(s->tracks[i], only_registered)) n++;

    size_t cap = n ? n : 1;
    moq_msf_track_t *mt = (moq_msf_track_t *)s->alloc.alloc(
        cap * sizeof(moq_msf_track_t), s->alloc.ctx);
#ifdef MOQ_MEDIA_SENDER_TESTING
    if (s->test_refuse_catalog_build && mt) {
        s->test_refuse_catalog_build = false;
        s->alloc.free(mt, cap * sizeof(moq_msf_track_t), s->alloc.ctx);
        mt = NULL;
    }
#endif
    moq_rcbuf_t **b64 = (moq_rcbuf_t **)s->alloc.alloc(
        cap * sizeof(moq_rcbuf_t *), s->alloc.ctx);
    moq_msf_init_data_entry_t *idl = (moq_msf_init_data_entry_t *)s->alloc.alloc(
        cap * sizeof(moq_msf_init_data_entry_t), s->alloc.ctx);
    if (!mt || !b64 || !idl) {
        if (mt)  s->alloc.free(mt, cap * sizeof(moq_msf_track_t), s->alloc.ctx);
        if (b64) s->alloc.free(b64, cap * sizeof(moq_rcbuf_t *), s->alloc.ctx);
        if (idl) s->alloc.free(idl,
            cap * sizeof(moq_msf_init_data_entry_t), s->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(b64, 0, cap * sizeof(moq_rcbuf_t *));

    moq_result_t rc = MOQ_OK;
    size_t k = 0;
    size_t idl_count = 0;   /* initDataList entries (cat.init_data_count) */
    size_t nb = 0;          /* base64 buffers used in b64[] (indexing/cleanup) */
    for (size_t i = 0; i < s->track_count && rc == MOQ_OK; i++) {
        moq_media_track_t *t = s->tracks[i];
        if (!track_in_catalog(t, only_registered)) continue;
        track_fill_msf(t, &mt[k]);
        if (sender_init_by_reference(t)) {
            /* Identical init bytes share the stable ID assigned at add_track,
             * so emit one root entry per ID, whichever track still uses it. */
            size_t e = idl_count;
            for (size_t j = 0; j < idl_count; j++) {
                if (idl[j].id.len == t->init_ref.len &&
                    memcmp(idl[j].id.data, t->init_ref.data,
                           t->init_ref.len) == 0) {
                    e = j;
                    break;
                }
            }
            if (e == idl_count) {
                rc = moq_msf_encode_init_data(&s->alloc, t->init_data,
                                              &b64[nb]);
                if (rc < 0) break;
                idl[idl_count].id   = t->init_ref;
                idl[idl_count].type =
                    (moq_bytes_t){ (const uint8_t *)"inline", 6 };
                idl[idl_count].data = (moq_bytes_t){
                    moq_rcbuf_data(b64[nb]), moq_rcbuf_len(b64[nb]) };
                nb++;
                idl_count++;
            }
            mt[k].has_init_ref = true;
            mt[k].init_ref = idl[e].id;
        }
        k++;
    }

    if (rc == MOQ_OK) {
        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.version = MOQ_MSF_VERSION;
        cat.tracks = mt;
        cat.track_count = k;
        cat.init_data_list = idl;
        cat.init_data_count = idl_count;
        /* Root contentProtections: the encoder borrows the sender's deep copy
         * (stable for the sender's life). */
        cat.content_protections = s->content_protections;
        cat.content_protection_count = s->content_protection_count;
        cat.has_generated_at = true;
        cat.generated_at = generated_at;
        /* MSF §5.1.3 / §11.3: a terminal catalog commits the broadcast
         * complete. It is built only once every track has ended and been
         * marked removed (complete_finalized), so the loop above already
         * yields an empty tracks array. */
        cat.is_complete = s->complete_finalized;
        cat.version_form = MOQ_MSF_VERSION_FORM_DRAFT_NAME;   /* MSF-01 5.1.1 */
        rc = moq_msf_catalog_encode(&s->alloc, &cat, out);
    }

    for (size_t i = 0; i < cap; i++)
        if (b64[i]) moq_rcbuf_decref(b64[i]);
    s->alloc.free(mt, cap * sizeof(moq_msf_track_t), s->alloc.ctx);
    s->alloc.free(b64, cap * sizeof(moq_rcbuf_t *), s->alloc.ctx);
    s->alloc.free(idl, cap * sizeof(moq_msf_init_data_entry_t), s->alloc.ctx);
    return rc;
}


/* A REAL catalog build: reads the clock exactly ONCE and returns the stamp it
 * encoded alongside the buffer, so the caller can install the two together.
 * out_stamp is optional (the test build helper has nowhere to keep it).
 * CALLER MUST HOLD s->mu. */
moq_result_t sender_build_catalog_fresh(moq_media_sender_t *s,
                                        moq_rcbuf_t **out,
                                        bool only_registered,
                                        uint64_t *out_stamp)
{
    uint64_t stamp = sender_catalog_now_ms(s);
    moq_result_t rc = sender_build_catalog_at(s, out, only_registered, stamp);
    if (out_stamp) *out_stamp = stamp;
    return rc;
}
#define SENDER_TRACK_SCALE_PRESENT 1u  /* LOC-04 Timescale, any value */

#define SENDER_TRACK_SCALE_UNKNOWN 2u  /* not parseable as LOC-04 metadata */


/* Classify a track's own copy of its Track Properties as draft-22 LOC-04
 * metadata. The parser searches the mutable list and any immutable inner
 * list. A failed parse proves nothing about a Timescale, so it is UNKNOWN,
 * never NONE; the parser publishes no output on failure, and none is read. */
uint8_t sender_track_scale_of(moq_bytes_t track_properties)
{
    if (track_properties.len == 0) return SENDER_TRACK_SCALE_NONE;
    moq_loc_metadata_t m;
    if (moq_loc_parse_metadata(MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_04,
                               track_properties, &m, sizeof(m)) != MOQ_OK)
        return SENDER_TRACK_SCALE_UNKNOWN;
    return (m.present & MOQ_LOC_METADATA_HAS_TIMESCALE)
               ? SENDER_TRACK_SCALE_PRESENT : SENDER_TRACK_SCALE_NONE;
}


/* Whether this AUTO sender is, or may still become, the draft-22 companion:
 * resolved to it, or unresolved with draft 22 in the endpoint's offer. The
 * offer is fixed when the endpoint is created. CALLER MUST HOLD s->mu. */
bool sender_auto_d22_possible_locked(const moq_media_sender_t *s)
{
    if (!s->companion_auto) return false;
    if (s->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC)
        return true;
    if (s->companion_profile != MOQ_MEDIA_COMPANION_AUTO) return false;
    moq_version_t versions[MOQ_ENDPOINT_MAX_VERSIONS];
    size_t n = moq_endpoint_offered_versions_internal(s->ep, versions,
                                                      MOQ_ENDPOINT_MAX_VERSIONS);
    for (size_t i = 0; i < n && i < MOQ_ENDPOINT_MAX_VERSIONS; i++)
        if (versions[i] == MOQ_VERSION_DRAFT_22) return true;
    return false;
}


static bool rcbuf_bytes_eq(const moq_rcbuf_t *a, const moq_rcbuf_t *b)
{
    if (!a || !b) return false;
    size_t la = moq_rcbuf_len(a), lb = moq_rcbuf_len(b);
    return la == lb && memcmp(moq_rcbuf_data(a), moq_rcbuf_data(b), la) == 0;
}


/* Build one deltaUpdate object (ADD or REMOVE) from `tracks`, encoded to an
 * rcbuf. An ADD op carries full, self-contained tracks (packaging/isLive forced
 * on so the encoder emits them). Init-bearing additions use a full independent
 * generation because deltas cannot carry the root initDataList;
 * a REMOVE op carries only the track name (MSF 5.1.6). Returns MOQ_OK with *out
 * set. */
static moq_result_t sender_build_delta_op(moq_media_sender_t *s,
                                          moq_msf_delta_op_kind_t kind,
                                          moq_media_track_t *const *tracks,
                                          size_t n, moq_rcbuf_t **out)
{
    *out = NULL;
    if (n == 0) return MOQ_ERR_INVAL;
    moq_msf_track_t *mt = (moq_msf_track_t *)s->alloc.alloc(
        n * sizeof(moq_msf_track_t), s->alloc.ctx);
    if (!mt) return MOQ_ERR_NOMEM;

    moq_result_t rc = MOQ_OK;
    for (size_t i = 0; i < n; i++) {
        if (kind == MOQ_MSF_DELTA_OP_ADD) {
            track_fill_msf(tracks[i], &mt[i]);
            mt[i].has_packaging = true;   /* encoder emits packaging + isLive */
            mt[i].has_is_live = true;
        } else { /* REMOVE: name only (MSF 5.1.6). */
            memset(&mt[i], 0, sizeof(mt[i]));
            mt[i].struct_size = sizeof(mt[i]);
            mt[i].name = tracks[i]->name;
        }
    }

    if (rc == MOQ_OK) {
        moq_msf_delta_op_t op = { kind, mt, n };
        moq_msf_catalog_t cat;
        memset(&cat, 0, sizeof(cat));
        cat.struct_size = sizeof(cat);
        cat.delta_update = &op;
        cat.delta_update_count = 1;
        rc = moq_msf_catalog_encode(&s->alloc, &cat, out);
    }

    s->alloc.free(mt, n * sizeof(moq_msf_track_t), s->alloc.ctx);
    return rc;
}


/* Free a staged but uncommitted generation's object refs. */
static void sender_pending_clear(moq_media_sender_t *s)
{
    for (size_t i = 0; i < s->pending_obj_count; i++)
        if (s->pending_objs[i]) { moq_rcbuf_decref(s->pending_objs[i]); s->pending_objs[i] = NULL; }
    s->pending_obj_count = 0;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    if (s->pending_current) { moq_rcbuf_decref(s->pending_current); s->pending_current = NULL; }
    s->pending_generated_at = 0;              /* cleared with its buffer */
    s->pending_terminal = false;
    s->pending_subs_finished = false;
    s->pending_pub_finished = false;
    s->pending_rejoined = false;
    s->pending_write_open = false;
}


/* Adopt the current resolved tuple set + metadata as the published baseline:
 * the next deltaUpdate diffs against in_published, and an in-place metadata
 * change is considered published once it lands in a staged generation. Called
 * from every sender_stage_generation success path so the two stay in lockstep. */
void sender_commit_baseline(moq_media_sender_t *s)
{
    for (size_t i = 0; i < s->track_count; i++) {
        s->tracks[i]->in_published = track_in_catalog(s->tracks[i], true);
        s->tracks[i]->catalog_meta_dirty = false;
    }
}


/* Is the catalog the sender would build right now the same DOCUMENT as the last
 * committed one? MSF §5.1.2 generatedAt is a fresh wallclock millisecond on
 * every build, so comparing complete encoded bytes against a freshly stamped
 * build could only ever match inside a single millisecond. The comparison build
 * therefore carries the PUBLISHED stamp and never reads the clock, which makes
 * the byte comparison ask exactly the intended question: identical modulo
 * generatedAt. Everything else -- isComplete included -- still differs on the
 * wire and still cuts a generation.
 *
 * The answer is separate from the result so a failure to allocate or encode
 * cannot be read as "changed" and let the caller carry on. With no published
 * baseline there is nothing to match: unchanged is false, and no comparison
 * build is made. CALLER MUST HOLD s->mu; the comparison buffer never leaves
 * this function and is never written to the wire. */
moq_result_t sender_catalog_semantically_unchanged(
    moq_media_sender_t *s, bool only_registered, bool *out_unchanged)
{
    *out_unchanged = false;
    if (!s->published_catalog) return MOQ_OK;
    moq_rcbuf_t *cmp = NULL;
    moq_result_t rc = sender_build_catalog_at(s, &cmp, only_registered,
                                              s->published_generated_at);
    if (rc != MOQ_OK || !cmp) {
        if (cmp) moq_rcbuf_decref(cmp);
        return rc != MOQ_OK ? rc : MOQ_ERR_INTERNAL;
    }
    *out_unchanged = rcbuf_bytes_eq(cmp, s->published_catalog);
    moq_rcbuf_decref(cmp);
    return MOQ_OK;
}


/* Stage a post-ready generation (caller holds s->mu, pending is empty). Returns
 * true if a generation was staged into s->pending_*, false if nothing to do
 * (no-op dedup) -- on a build/encode failure it sets fatal and returns false. */
bool sender_stage_generation(moq_media_sender_t *s)
{
    /* Decide first, and stage nothing until the answer is known: a comparison
     * failure leaves no half-staged generation behind. */
    bool unchanged = false;
    moq_result_t crc = sender_catalog_semantically_unchanged(s, true, &unchanged);
    if (crc != MOQ_OK) {
        sender_note_completion_failure(s, crc);
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }
    if (unchanged) return false;              /* no-op dedup */

    /* Changed: build the catalog that actually goes on the wire, with its own
     * fresh stamp. The comparison buffer above is never reused here -- it
     * carries the PRIOR generation's timestamp. */
    moq_rcbuf_t *current = NULL;
    uint64_t stamp = 0;
    crc = sender_build_catalog_fresh(s, &current, true, &stamp);
    if (crc != MOQ_OK || !current) {
        sender_note_completion_failure(s, crc != MOQ_OK ? crc : MOQ_ERR_INTERNAL);
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }

    /* Every app-driven change was refused at its call once no group was left
     * (sender_catalog_group_available); a change raised on the network thread
     * alone (a deferred registration) still never wraps or reuses an ID. */
    if (s->catalog_group >= MOQ_QUIC_VARINT_MAX) {
        moq_rcbuf_decref(current);
        sender_note_completion_failure(s, MOQ_ERR_LIMIT);
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }
    s->pending_group = s->catalog_group + 1;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    s->pending_current = current;             /* becomes published on commit */
    s->pending_generated_at = stamp;          /* ... with the stamp it encoded */
    s->pending_terminal = s->complete_finalized;

    /* A terminal (completing) generation is a single independent object; no
     * prior base and no deltas (MSF §11.3). The initial generation has no prior
     * base to diff against either, so fall back to independent. */
    if (s->completing || !s->published_catalog) {
        moq_rcbuf_incref(current);
        s->pending_objs[0] = current;
        s->pending_obj_count = 1;
        sender_commit_baseline(s);
        return true;
    }

    /* Diff the prior committed catalog (in_published flags) against the current
     * build: added tuples vs removed tuples. */
    moq_media_track_t **adds = (moq_media_track_t **)s->alloc.alloc(
        (s->track_count ? s->track_count : 1) * sizeof(*adds), s->alloc.ctx);
    moq_media_track_t **rems = (moq_media_track_t **)s->alloc.alloc(
        (s->track_count ? s->track_count : 1) * sizeof(*rems), s->alloc.ctx);
    if (!adds || !rems) {
        if (adds) s->alloc.free(adds, s->track_count * sizeof(*adds), s->alloc.ctx);
        if (rems) s->alloc.free(rems, s->track_count * sizeof(*rems), s->alloc.ctx);
        sender_pending_clear(s);
        sender_note_completion_failure(s, MOQ_ERR_NOMEM);
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }
    size_t na = 0, nr = 0;
    bool init_ref_add = false;
    bool meta_changed = false;
    for (size_t i = 0; i < s->track_count; i++) {
        moq_media_track_t *t = s->tracks[i];
        bool in_cat = track_in_catalog(t, true);
        if (in_cat && t->in_published && t->catalog_meta_dirty) {
            /* A surviving tuple whose metadata changed in place (e.g. a
             * live->VOD conversion): the deltaUpdate vocabulary is only
             * add/remove, so base + add/remove deltas cannot carry this change.
             * Force a full independent generation so the converted metadata
             * actually reaches receivers (and joiners via the retained group). */
            meta_changed = true;
        }
        if (in_cat && !t->in_published) {
            adds[na++] = t;
            /* Initialization in the independent catalog's root initDataList
             * (referenced by initRef) is a shape a deltaUpdate add cannot
             * carry. Emitting it inline here would diverge from the track's
             * canonical (initRef) shape in every later independent catalog,
             * which the receiver's immutable-tuple check rejects. So a
             * generation that adds any init-bearing track falls back to a
             * full independent generation. Init-less tracks stay on the delta
             * path. */
            if (sender_init_by_reference(t))
                init_ref_add = true;
        } else if (!in_cat && t->in_published) {
            rems[nr++] = t;
        }
    }

    if (init_ref_add || meta_changed) {
        s->alloc.free(adds, s->track_count * sizeof(*adds), s->alloc.ctx);
        s->alloc.free(rems, s->track_count * sizeof(*rems), s->alloc.ctx);
        moq_rcbuf_incref(current);
        s->pending_objs[0] = current;     /* single independent generation */
        s->pending_obj_count = 1;
        sender_commit_baseline(s);
        return true;
    }

    moq_result_t rc = MOQ_OK;
    moq_rcbuf_t *base = NULL, *rem_obj = NULL, *add_obj = NULL;
    /* object 0 = the prior committed catalog (the independent base the deltas
     * apply onto; base + deltas == current). */
    base = s->published_catalog;
    moq_rcbuf_incref(base);
    /* REMOVE object before ADD object so the receiver observes remove-then-add
     * per tuple (immutable-tuple safety for same-name reuse). */
    if (nr > 0) rc = sender_build_delta_op(s, MOQ_MSF_DELTA_OP_REMOVE, rems, nr, &rem_obj);
    if (rc == MOQ_OK && na > 0)
        rc = sender_build_delta_op(s, MOQ_MSF_DELTA_OP_ADD, adds, na, &add_obj);

    s->alloc.free(adds, s->track_count * sizeof(*adds), s->alloc.ctx);
    s->alloc.free(rems, s->track_count * sizeof(*rems), s->alloc.ctx);

    if (rc != MOQ_OK || (nr == 0 && na == 0)) {
        /* Encode failure, or bytes differ but no tuple diff (should not happen
         * for the deterministic builder) -> fall back to a single independent
         * generation (object 0 = current). */
        if (base) moq_rcbuf_decref(base);
        if (rem_obj) moq_rcbuf_decref(rem_obj);
        if (add_obj) moq_rcbuf_decref(add_obj);
        if (rc != MOQ_OK) {   /* genuine encode failure is fatal */
            sender_pending_clear(s);
            sender_note_completion_failure(s, rc);
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
            return false;
        }
        moq_rcbuf_incref(current);
        s->pending_objs[0] = current;
        s->pending_obj_count = 1;
    } else {
        size_t k = 0;
        s->pending_objs[k++] = base;             /* object 0 (prior, base) */
        if (rem_obj) s->pending_objs[k++] = rem_obj;
        if (add_obj) s->pending_objs[k++] = add_obj;
        s->pending_obj_count = k;
    }
    /* Commit the new published tuple set (under mu; staging is atomic vs the
     * app thread). A track added after this point lands in the next diff. */
    sender_commit_baseline(s);
    return true;
}


/* Arm the next automatic-refresh deadline from now (caller holds s->mu, or is
 * on the network thread before the sender is live). UINT64_MAX is reserved
 * EXCLUSIVELY for the disabled/unarmed state (what sender_refresh_due reads as
 * "never"), so a finite interval must never resolve to it: a finite deadline
 * that would overflow saturates to UINT64_MAX-1 (still armed, fires eventually).
 * Refresh is also disarmed once the generation counter reaches the varint
 * ceiling (MOQ_QUIC_VARINT_MAX): the next group would be unencodable, so rather
 * than re-arm and churn (staging a group the facade would reject as fatal) the
 * deadline is parked at UINT64_MAX. */
void sender_arm_refresh(moq_media_sender_t *s, uint64_t now_us)
{
    if (s->catalog_refresh_interval_us == UINT64_MAX ||
        s->catalog_group >= MOQ_QUIC_VARINT_MAX) {
        s->catalog_refresh_deadline_us = UINT64_MAX;   /* disabled / ceiling */
        return;
    }
    uint64_t d = now_us + s->catalog_refresh_interval_us;
    /* Saturate to UINT64_MAX-1 on overflow AND on the exact-max collision:
     * now+interval == UINT64_MAX does not overflow but would alias the
     * disabled/unarmed sentinel, so a finite interval could silently disable. */
    if (d < now_us || d == UINT64_MAX) d = UINT64_MAX - 1;
    s->catalog_refresh_deadline_us = d;
}


/* Recompute the cached managed-adapter wake deadline (caller holds s->mu; run at
 * the end of sender_hook, where facade access is legal). It is the armed refresh
 * deadline iff refresh is genuinely TIMER-WAITING; otherwise UINT64_MAX, so an
 * expired deadline can never keep an idle managed loop immediately-due while the
 * hook does no work. Terminality is read from the cached closed_fired/fatal
 * latches, never sender_ep_closed() (which would reach the adapter). */
void sender_recompute_wake_deadline(moq_media_sender_t *s)
{
    uint64_t d = UINT64_MAX;
    if (s->ready && !s->fatal && !s->closed_fired &&
        s->catalog_refresh_interval_us != UINT64_MAX &&
        s->catalog_refresh_deadline_us != UINT64_MAX &&
        s->catalog_group < MOQ_QUIC_VARINT_MAX &&
        s->pending_obj_count == 0 && !s->catalog_dirty &&
        s->catalog_track && sender_track_demand(s, s->catalog_track))
        d = s->catalog_refresh_deadline_us;
    s->refresh_wake_deadline_us = d;
}


/* Managed-adapter wake query (moq_endpoint_hook_ops.next_deadline_us): a PURE
 * cached-scalar read under s->mu -- no publisher/session/endpoint/adapter
 * re-entry, safe to call after the pump's exclusive window has closed. */
/* Output this sender owes and has not yet handed to the session:
 *   - a queued END_OF_TRACK marker -- unless it waits behind media of its
 *     own track that is retained for a track nobody has subscribed to and
 *     that has no established publication: the default no-demand policy
 *     holds that media, and the marker after it, for a joiner. An accepted
 *     PUBLISH is established even at Forward 0 (no demand): its terminal
 *     control message is owed regardless of Forward, so its held END stays
 *     owed and the drain stays pending rather than reporting completion;
 *   - a queued object of a track that has demand;
 *   - a catalog generation still being written, retained or, when terminal,
 *     finished, or a republish a post-ready catalog change has made owed
 *     (catalog_dirty);
 *   - a reset owed for an abandoned group (pending_reset), which also keeps
 *     a partially emitted object's obligation when its subscriber leaves;
 *   - a removed track not yet torn down: catalog_dirty covers the pump
 *     that tears it down, and this covers a teardown refused and retried
 *     after the republish committed;
 *   - the PUBLISH_DONE a live->VOD conversion armed, through catalog_dirty:
 *     conversion is post-ready only, dirties the catalog, and the republish
 *     stays gated (catalog_dirty kept) until every PUBLISH_DONE is sent.
 * An object queued for a track nobody subscribes to is retained for a
 * future joiner, not owed: it stays owned and counted in objects_queued, it
 * is neither claimed delivered nor discarded, and it does not hold a drain.
 * Called inside the pump cycle right after sender_hook (endpoint hook
 * contract), so the publisher demand read is legal; s->mu nests inside
 * ep->mu. */
bool sender_has_pending_output(void *ctx)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    pthread_mutex_lock(&s->mu);
    bool pending = s->pending_obj_count > 0 || (s->ready && s->catalog_dirty);
    for (uint32_t i = s->preq_head; !pending && i != s->preq_tail; i++) {
        const sender_preq_entry_t *e = &s->preq[i % s->ring_cap];
        if (sender_track_demand(s, e->track)) {
            pending = true;
        } else if (e->is_end) {
            /* An established publication owes its terminal regardless of
             * Forward; otherwise an END behind its own track's retained
             * media is held with that media. */
            bool published = s->publish_tracks && e->track &&
                e->track->pub_track &&
                moq_pub_track_is_published(s->pub, e->track->pub_track);
            bool held_behind_retained = false;
            for (uint32_t j = s->preq_head;
                 !published && j != i && !held_behind_retained; j++) {
                const sender_preq_entry_t *p = &s->preq[j % s->ring_cap];
                held_behind_retained = p->track == e->track && !p->is_end;
            }
            if (!held_behind_retained) pending = true;
        }
    }
    for (size_t i = 0; !pending && i < s->track_count; i++) {
        const moq_media_track_t *t = s->tracks[i];
        if (t->pending_reset || ((t->removed || t->auth_failed) && t->pub_track))
            pending = true;
        if (t->auth_failed) continue;
        /* A completion end owed with nothing of its track left queued is
         * sendable now; one behind retained media is held with that media,
         * as an end_track marker is. A TS track's unfinished source is work
         * once no writer holds it. A failed completion sends no further end
         * (sender_drain_completion), so what it owes is no work. */
        if (t->complete_end_owed && t->tstats.objects_queued == 0 &&
            !s->complete_failed)
            pending = true;
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
        if (t->complete_end_owed && sender_ts_complete_ready(t) && !s->complete_failed)
            pending = true;
#endif
        /* A generated timeline's records not yet out -- a write refused for
         * room included -- are output for its own subscriber, whatever the
         * completion's state; without one they are kept for a joiner, not
         * owed. Caught up, a completing timeline's end is work once its
         * media's end is sent, unless the completion failed. */
        if (track_is_generated_timeline(t) && !t->removed && t->pub_track) {
            if (!sender_timeline_caught_up(t)) {
                if (sender_track_demand(s, (moq_media_track_t *)(uintptr_t)t))
                    pending = true;
            } else if (s->completing && !s->complete_finalized &&
                       !s->complete_failed && !t->end_sent &&
                       sender_timeline_media_ended(s, t)) {
                pending = true;
            }
        }
    }
    /* The terminal catalog is owed once the completion finalizes, which it
     * cannot while an end is held behind retained media or a timeline keeps
     * records for a joiner: those wait for demand, not for a pump. */
    if (s->completing && !s->complete_finalized && !s->complete_failed) {
        bool held = false;
        for (size_t i = 0; i < s->track_count && !held; i++) {
            moq_media_track_t *t = s->tracks[i];
            if (t->is_catalog || t->removed || t->auth_failed) continue;
            if (track_is_generated_timeline(t))
                held = !t->end_sent && t->pub_track &&
                       !sender_timeline_caught_up(t) &&
                       !sender_track_demand(s, t);
            else
                held = t->end_requested && !t->end_sent &&
                       t->tstats.objects_queued != 0;
        }
        if (!held) pending = true;
    }
    pthread_mutex_unlock(&s->mu);
    return pending;
}


uint64_t sender_next_deadline_us(void *ctx)
{
    moq_media_sender_t *s = (moq_media_sender_t *)ctx;
    pthread_mutex_lock(&s->mu);
    uint64_t d = s->refresh_wake_deadline_us;
    pthread_mutex_unlock(&s->mu);
    return d;
}


/* True when an automatic independent catalog refresh is due (caller holds
 * s->mu). Disabled interval or an unarmed deadline is never due. */
static bool sender_refresh_due(const moq_media_sender_t *s, uint64_t now_us)
{
    if (s->catalog_refresh_interval_us == UINT64_MAX) return false;
    if (s->catalog_refresh_deadline_us == UINT64_MAX) return false;
    return now_us >= s->catalog_refresh_deadline_us;
}


/* Stage a refresh generation (caller holds s->mu, pending is empty): a NEW
 * group whose sole object 0 is the COMPLETE current catalog (independent,
 * never a delta), even when its bytes match the last committed catalog -- the
 * point is to place a fresh independent object on the subscribe path for late
 * joiners, not to signal a content change. Reuses the same pending_* cursor /
 * retained-install machinery as a mutation generation, so a WOULD_BLOCK is
 * exactly-once and retryable. Returns true if staged; false at the varint group
 * ceiling (the next group would be unencodable) or on a build failure (fatal). */
static bool sender_stage_refresh(moq_media_sender_t *s)
{
    /* The next group (catalog_group + 1) must be an encodable QUIC varint; at
     * the ceiling refuse cleanly rather than stage a group the facade rejects. */
    if (s->catalog_group >= MOQ_QUIC_VARINT_MAX) return false;
    /* Deliberately NOT routed through the semantic comparison: the refresh
     * exists to put a fresh independent catalog on the subscribe path for late
     * joiners, so it stages even when its document is unchanged. Its stamp is
     * still freshly chosen here and still travels with these exact bytes. */
    moq_rcbuf_t *current = NULL;
    uint64_t stamp = 0;
    moq_result_t brc = sender_build_catalog_fresh(s, &current, true, &stamp);
    if (brc != MOQ_OK || !current) {
        sender_note_completion_failure(s, brc != MOQ_OK ? brc : MOQ_ERR_INTERNAL);
        sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_CATALOG_ENCODE);
        return false;
    }
    s->pending_group = s->catalog_group + 1;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    s->pending_current = current;             /* becomes published on commit */
    s->pending_generated_at = stamp;
    s->pending_terminal = s->complete_finalized;
    moq_rcbuf_incref(current);
    s->pending_objs[0] = current;             /* single independent object */
    s->pending_obj_count = 1;
    sender_commit_baseline(s);                /* no tuple change; keeps in sync */
    return true;
}


/* Finish the catalog's live deliveries after a terminal generation is written
 * and retained (s->mu held). Each active subscription's last subgroup is
 * closed and the request ended with Track Ended (moq_pub_finish_subscribers);
 * then a catalog publication is ended the same way (moq_pub_unpublish_track),
 * which fires no on_publish_finished. Neither ends the track: its namespace,
 * registration and retained group stay, so a later subscribe and Joining FETCH
 * still retrieve this generation. Nothing is sent where there is nothing to
 * finish. A refusal returns false with the completed steps kept, and a retry
 * resumes at the step left; the facade itself resumes inside a step. A
 * request that joins after the subscription step is not finished by it. */
static bool sender_finish_catalog(moq_media_sender_t *s, uint64_t now_us)
{
    moq_pub_track_t *pt = s->catalog_track->pub_track;
    if (!s->pending_subs_finished) {
        moq_result_t rc = moq_pub_finish_subscribers(s->pub, pt,
                                                     MOQ_PUB_DONE_TRACK_ENDED, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_WRONG_STATE) return false;
        if (rc != MOQ_OK) {
            sender_note_completion_failure(s, rc);
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return false;
        }
        s->pending_subs_finished = true;
    }
    if (!s->pending_pub_finished) {
        moq_result_t rc = moq_pub_unpublish_track(s->pub, pt, now_us);
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_WRONG_STATE) return false;
        if (rc != MOQ_OK) {
            sender_note_completion_failure(s, rc);
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            return false;
        }
        s->pending_pub_finished = true;
    }
    return true;
}


/* Post-ready catalog republish (network thread). Coalesces pending add/
 * remove mutations into ONE new generation in a NEW group: object 0 is the
 * prior committed catalog (independent base) and objects 1..N are deltaUpdate
 * objects (removes then adds) transforming it to the current catalog
 * (base + deltas == current). The dense object list is live-written in order to
 * active subscribers, then installed as the publisher's retained group so a
 * Joining FETCH replays objects 0..N. The staged generation is retained across
 * ticks until every live object AND the retained-group install finish, so a
 * transport WOULD_BLOCK retries the unfinished step without re-writing an
 * already-written object or skipping a generation. complete() and the initial
 * generation stage a single independent object instead of deltas, and a
 * terminal generation finishes the catalog's live deliveries before it
 * commits (sender_finish_catalog). */
void sender_republish_catalog(moq_media_sender_t *s, uint64_t now_us)
{
    pthread_mutex_lock(&s->mu);

    /* Completion's END_OF_TRACK objects do not retire served pull slots.
     * Their Track Ended controls are queued by retirement on a later hook.
     * Keep the terminal catalog owed until every such publisher track is
     * retired; a capacity refusal preserves its handle and pending output.
     * A catalog PUBLISH still awaiting its answer holds it too, as a media
     * track's end waits for its own: the terminal generation is then written
     * to the publication the answer establishes, and ended there. */
    if (s->complete_finalized) {
        bool held = !sender_terminal_admitted(s, s->catalog_track);
        for (size_t i = 0; i < s->track_count && !held; i++) {
            const moq_media_track_t *t = s->tracks[i];
            held = !t->is_catalog && t->pub_track;
        }
        if (held) {
            pthread_mutex_unlock(&s->mu);
            return;
        }
    }

    if (s->pending_obj_count == 0) {
        /* A real add/remove/conversion generation takes precedence. */
        if (s->catalog_dirty) {
            s->catalog_dirty = false;   /* a later mutation re-sets it */
            (void)sender_stage_generation(s);   /* stages, no-op dedups, or fatal */
            if (s->fatal) { pthread_mutex_unlock(&s->mu); return; }
        }
        bool cat_demand = sender_track_demand(s, s->catalog_track);
        if (cat_demand != s->catalog_had_demand) {
            s->catalog_had_demand = cat_demand;
            if (cat_demand) sender_arm_refresh(s, now_us);
        }
        /* No mutation generation this cycle (dedup, or nothing was dirty): if a
         * periodic refresh is due AND the catalog has demand, stage exactly one
         * independent refresh. No demand -> no refresh / no group advance. Never
         * a redundant second generation in one cycle (a mutation above leaves
         * pending_obj_count > 0, skipping this). */
        if (s->pending_obj_count == 0 &&
            sender_refresh_due(s, now_us) &&
            cat_demand) {
            (void)sender_stage_refresh(s);
            if (s->fatal) { pthread_mutex_unlock(&s->mu); return; }
        }
        if (s->pending_obj_count == 0) { pthread_mutex_unlock(&s->mu); return; }
    }

    /* Live-write objects 0..N-1 in order wherever the catalog has demand --
     * a subscriber (pull) OR an established forward-on publication (push),
     * the same sender_track_demand the initial catalog and the media drain
     * use. With no demand the live writes are skipped (the retained group
     * below serves any future joiner). A WOULD_BLOCK retries from the cursor
     * (no duplicate / skipped object), and a write the facade holds is
     * completed by the identical call even when its demand has gone.
     *
     * A terminal generation is then written again as the next group while a
     * subscription has joined since its write began (pending_rejoined):
     * finishing now would end that subscription with no catalog object and
     * no request left for its Joining FETCH. The held write is completed
     * first, with its original group and bytes; joins during the rewrite
     * latch again. Each such join costs at most one rewrite. */
    for (;;) {
        if (s->pending_write_open ||
            sender_track_demand(s, s->catalog_track)) {
            while (s->pending_obj_cursor < s->pending_obj_count) {
                size_t k = s->pending_obj_cursor;
#ifdef MOQ_MEDIA_SENDER_TESTING
                if (s->test_block_republish_obj >= 0 &&
                    (int64_t)k == s->test_block_republish_obj) {
                    /* Snapshot the exact mid-generation state, then return
                     * as a WOULD_BLOCK would: cursor untouched, nothing
                     * committed. */
                    s->test_block_republish_obj = -1;   /* one-shot */
                    s->test_block_fired = true;
                    s->test_block_cursor = s->pending_obj_cursor;
                    s->test_block_count = s->pending_obj_count;
                    s->test_block_retained_set = s->pending_retained_set;
                    pthread_mutex_unlock(&s->mu);
                    return;
                }
#endif
                moq_pub_object_cfg_t ocfg;
                moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
                ocfg.group_id = s->pending_group;
                ocfg.object_id = k;
                ocfg.payload = s->pending_objs[k];
                /* One subgroup per generation group: end_of_group is a
                 * per-subgroup header flag, so every object in the group
                 * carries it (mirroring the media drain) -- the group ends
                 * after this single subgroup. */
                ocfg.end_of_group = true;
                moq_result_t wrc = moq_pub_write_object_ex(
                    s->pub, s->catalog_track->pub_track, &ocfg, now_us);
                if (wrc == MOQ_ERR_WOULD_BLOCK) {
                    s->pending_write_open = true;
                    pthread_mutex_unlock(&s->mu);
                    return;
                }
                if (wrc < 0) {
                    sender_note_completion_failure(s, wrc);
                    sender_set_fatal_locked(s,
                                            MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
                    pthread_mutex_unlock(&s->mu);
                    return;
                }
                s->pending_write_open = false;
                s->pending_obj_cursor++;
            }
        } else {
            s->pending_obj_cursor = s->pending_obj_count;
        }
        if (!s->pending_rejoined || s->pending_subs_finished) break;
        s->pending_rejoined = false;
        if (s->pending_group >= MOQ_QUIC_VARINT_MAX) {
            sender_note_completion_failure(s, MOQ_ERR_LIMIT);
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            pthread_mutex_unlock(&s->mu);
            return;
        }
        s->pending_group++;
        s->pending_obj_cursor = 0;
        s->pending_retained_set = false;
    }

    /* Install the dense retained group (objects 0..N) after the live publish, so
     * a joiner reconstructs the same generation a FETCH would. */
    if (!s->pending_retained_set) {
#ifdef MOQ_MEDIA_SENDER_TESTING
        if (s->test_block_retained_once) {
            /* One-shot: behave exactly as a set_retained_group WOULD_BLOCK --
             * the live objects are already written, but the install (and thus
             * the commit + deadline re-arm) is deferred to the next pump with
             * pending state intact. */
            s->test_block_retained_once = false;
            pthread_mutex_unlock(&s->mu);
            return;
        }
#endif
        moq_pub_retained_object_t robjs[3];
        for (size_t i = 0; i < s->pending_obj_count; i++) {
            robjs[i].object_id = i;
            robjs[i].payload = s->pending_objs[i];
            robjs[i].properties = NULL;
            robjs[i].end_of_group = (i + 1 == s->pending_obj_count);
        }
        moq_pub_retained_group_cfg_t gcfg;
        moq_pub_retained_group_cfg_init(&gcfg);
        gcfg.group_id = s->pending_group;
        gcfg.objects = robjs;
        gcfg.object_count = s->pending_obj_count;
        moq_result_t src = moq_pub_set_retained_group(
            s->pub, s->catalog_track->pub_track, &gcfg);
        if (src == MOQ_ERR_WOULD_BLOCK) { pthread_mutex_unlock(&s->mu); return; }
        if (src < 0) {
            sender_note_completion_failure(s, src);
            sender_set_fatal_locked(s, MOQ_MEDIA_SENDER_FATAL_SETUP_FAILED);
            pthread_mutex_unlock(&s->mu);
            return;
        }
        s->pending_retained_set = true;
#ifdef MOQ_MEDIA_SENDER_TESTING
        s->test_retained_installs++;
#endif
    }

    if (s->pending_terminal && !sender_finish_catalog(s, now_us)) {
        pthread_mutex_unlock(&s->mu);
        return;
    }

    /* Commit the generation: advance the group and adopt the new baseline. Any
     * committed generation (mutation OR refresh) resets the refresh cadence, so
     * the next automatic refresh is one full interval away and a WOULD_BLOCK
     * retry across ticks never drifts the cadence (the deadline advances only
     * here, at commit, not while a generation is mid-flight). */
    s->catalog_group = s->pending_group;
    sender_arm_refresh(s, now_us);
    if (s->pending_terminal) s->complete_terminal_committed = true;
    s->pending_terminal = false;
    if (s->published_catalog) moq_rcbuf_decref(s->published_catalog);
    s->published_catalog = s->pending_current;   /* move ref */
    /* The stamp moves WITH the bytes it labels. Reading the clock here instead
     * would mislabel a generation that waited across clock movement, and the
     * next semantic comparison would then see a difference that is not in the
     * document. */
    s->published_generated_at = s->pending_generated_at;
    s->pending_current = NULL;
    s->pending_generated_at = 0;
    for (size_t i = 0; i < s->pending_obj_count; i++)
        if (s->pending_objs[i]) { moq_rcbuf_decref(s->pending_objs[i]); s->pending_objs[i] = NULL; }
    s->pending_obj_count = 0;
    s->pending_obj_cursor = 0;
    s->pending_retained_set = false;
    s->pending_subs_finished = false;
    s->pending_pub_finished = false;
    s->pending_rejoined = false;
    s->pending_write_open = false;
    pthread_mutex_unlock(&s->mu);
}


/* -- CMSF content-protection authoring (deep copy) -------------------- *
 * Root contentProtections are deep-copied from the cfg at create/attach so no
 * caller buffer outlives the call. The copy lives in three places freed by
 * free_root_cps(): the entry array, one shared `cp_strings` byte buffer for
 * every nested string, and a default_kids[] array per entry. The helpers below
 * keep the byte arithmetic out of sender_new. */

/* A present URL must carry a non-empty url; a typed URL must carry a non-empty
 * type. (An absent url is not copied, so its spans are not inspected.) Rejecting
 * an empty-but-present span here keeps a bad span from reaching cp_put's memcpy
 * during the deep-copy and from failing later in catalog encode. */
static moq_result_t cp_url_valid(const moq_cmsf_url_t *u)
{
    if (!u->present) return MOQ_OK;
    if (u->url.len == 0 || !u->url.data) return MOQ_ERR_INVAL;
    if (u->has_type && (u->type.len == 0 || !u->type.data))
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}


/* Shape-only validation (no DRM/UUID/URL semantics). Authoring is stricter than
 * the receive parser: at least one non-empty defaultKID is required here. Every
 * span that the deep-copy will copy -- required AND any optional field marked
 * present -- is checked so a malformed entry fails create/attach with
 * MOQ_ERR_INVAL rather than crashing the copy or failing late in encode. */
/* CMSF §4.1.1.2 / §4.1.1.4.1: a contentProtection UUID string is exactly
 * "8-4-4-4-12" hex digits with hyphens (defaultKID entries and drmSystem
 * systemID). Case-insensitive hex -- the draft's format placeholder is
 * lowercase but does not restrict case. Syntax only: the UUID's value and
 * registry meaning are not interpreted. */
static bool cp_is_uuid(moq_bytes_t v)
{
    if (!v.data || v.len != 36) return false;
    for (size_t i = 0; i < 36; i++) {
        uint8_t c = v.data[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') return false;
        } else if (!((c >= '0' && c <= '9') ||
                     (c >= 'a' && c <= 'f') ||
                     (c >= 'A' && c <= 'F'))) {
            return false;
        }
    }
    return true;
}


/* CMSF §4.1.1.4.5: pssh is a Base64-encoded PSSH box. Local, non-allocating
 * syntax check (not a public API) that mirrors the MSF encoder's b64_syntax_ok,
 * so a content protection accepted here also passes the strict catalog encode
 * (otherwise a bad config would be accepted at create/attach and then fail late
 * during catalog publication). */
static int cp_b64_val(uint8_t c)
{
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}


static bool cp_b64_syntax_ok(moq_bytes_t b)
{
    if (b.len == 0) return true;
    if (!b.data || b.len % 4 != 0) return false;
    size_t pad = 0;
    if (b.data[b.len - 1] == '=') pad++;
    if (b.len >= 2 && b.data[b.len - 2] == '=') pad++;
    for (size_t i = 0; i < b.len; i++) {
        uint8_t c = b.data[i];
        if (c == '=') { if (i < b.len - pad) return false; }
        else if (cp_b64_val(c) < 0) return false;
    }
    if (pad == 2) { if (cp_b64_val(b.data[b.len - 3]) & 0x0F) return false; }
    else if (pad == 1) { if (cp_b64_val(b.data[b.len - 2]) & 0x03) return false; }
    return true;
}


moq_result_t cp_validate_one(const moq_cmsf_content_protection_t *cp)
{
    if (cp->ref_id.len == 0 || !cp->ref_id.data) return MOQ_ERR_INVAL;
    /* CMSF §4.1.1.3: scheme is the CENC protection scheme -- a closed enum of
     * "cenc" or "cbcs". (Syntax only; no DRM-system semantics.) */
    if (!(cp->scheme.data && cp->scheme.len == 4 &&
          (memcmp(cp->scheme.data, "cenc", 4) == 0 ||
           memcmp(cp->scheme.data, "cbcs", 4) == 0)))
        return MOQ_ERR_INVAL;
    /* CMSF §4.1.1.4.1: drmSystem.systemID is a UUID string (syntax only). */
    if (!cp_is_uuid(cp->drm_system.system_id)) return MOQ_ERR_INVAL;
    if (cp->default_kid_count == 0 || !cp->default_kids) return MOQ_ERR_INVAL;
    /* CMSF §4.1.1.2: each defaultKID is a UUID string (syntax only). */
    for (size_t i = 0; i < cp->default_kid_count; i++)
        if (!cp_is_uuid(cp->default_kids[i]))
            return MOQ_ERR_INVAL;
    /* Optional drmSystem fields: validated only when marked present. */
    moq_result_t ur;
    if ((ur = cp_url_valid(&cp->drm_system.la_url))   != MOQ_OK) return ur;
    if ((ur = cp_url_valid(&cp->drm_system.cert_url)) != MOQ_OK) return ur;
    if ((ur = cp_url_valid(&cp->drm_system.auth_url)) != MOQ_OK) return ur;
    /* CMSF §4.1.1.4.5: a present pssh must be a non-empty, syntactically valid
     * Base64 PSSH box (matches the strict MSF catalog encoder). */
    if (cp->drm_system.has_pssh &&
        (cp->drm_system.pssh.len == 0 || !cp->drm_system.pssh.data ||
         !cp_b64_syntax_ok(cp->drm_system.pssh)))
        return MOQ_ERR_INVAL;
    if (cp->drm_system.has_robustness &&
        (cp->drm_system.robustness.len == 0 ||
         !cp->drm_system.robustness.data))
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}


static size_t cp_url_bytes(const moq_cmsf_url_t *u)
{
    if (!u->present) return 0;
    return u->url.len + (u->has_type ? u->type.len : 0);
}


/* Total nested-string bytes one entry contributes to the shared buffer. */
static size_t cp_entry_bytes(const moq_cmsf_content_protection_t *cp)
{
    size_t n = cp->ref_id.len + cp->scheme.len + cp->drm_system.system_id.len;
    for (size_t i = 0; i < cp->default_kid_count; i++)
        n += cp->default_kids[i].len;
    n += cp_url_bytes(&cp->drm_system.la_url);
    n += cp_url_bytes(&cp->drm_system.cert_url);
    n += cp_url_bytes(&cp->drm_system.auth_url);
    if (cp->drm_system.has_pssh) n += cp->drm_system.pssh.len;
    if (cp->drm_system.has_robustness) n += cp->drm_system.robustness.len;
    return n;
}


/* Copy src into buf+*off, return a span into buf, advance *off (empty -> {0}). */
static moq_bytes_t cp_put(uint8_t *buf, size_t *off, moq_bytes_t src)
{
    if (src.len == 0) return (moq_bytes_t){ NULL, 0 };
    memcpy(buf + *off, src.data, src.len);
    moq_bytes_t r = { buf + *off, src.len };
    *off += src.len;
    return r;
}


static moq_cmsf_url_t cp_copy_url(uint8_t *buf, size_t *off,
                                  const moq_cmsf_url_t *u)
{
    moq_cmsf_url_t r;
    memset(&r, 0, sizeof(r));
    if (!u->present) return r;
    r.present = true;
    r.url = cp_put(buf, off, u->url);
    r.has_type = u->has_type;
    if (u->has_type) r.type = cp_put(buf, off, u->type);
    return r;
}


/* Robust against partial copies: default_kids[i] is NULL (zeroed) for entries
 * not yet populated, so it is skipped. content_protection_count must already
 * reflect the array length. */
void free_root_cps(moq_media_sender_t *s)
{
    if (s->content_protections) {
        for (size_t i = 0; i < s->content_protection_count; i++) {
            moq_cmsf_content_protection_t *cp = &s->content_protections[i];
            if (cp->default_kids)
                s->alloc.free(cp->default_kids,
                    cp->default_kid_count * sizeof(moq_bytes_t), s->alloc.ctx);
        }
        s->alloc.free(s->content_protections,
            s->content_protection_count * sizeof(moq_cmsf_content_protection_t),
            s->alloc.ctx);
        s->content_protections = NULL;
    }
    if (s->cp_strings) {
        s->alloc.free(s->cp_strings, s->cp_strings_size, s->alloc.ctx);
        s->cp_strings = NULL;
    }
    s->content_protection_count = 0;
    s->cp_strings_size = 0;
}


/* Deep-copy `count` validated entries into sender-owned storage. On any failure
 * leaves no partial state (frees whatever was allocated). */
moq_result_t sender_copy_root_cps(moq_media_sender_t *s,
    const moq_cmsf_content_protection_t *src, size_t count)
{
    if (count == 0) return MOQ_OK;
    if (!src) return MOQ_ERR_INVAL;

    size_t strbytes = 0;
    for (size_t i = 0; i < count; i++)
        strbytes += cp_entry_bytes(&src[i]);

    moq_cmsf_content_protection_t *arr = (moq_cmsf_content_protection_t *)
        s->alloc.alloc(count * sizeof(*arr), s->alloc.ctx);
    uint8_t *strs = strbytes
        ? (uint8_t *)s->alloc.alloc(strbytes, s->alloc.ctx) : NULL;
    if (!arr || (strbytes && !strs)) {
        if (arr) s->alloc.free(arr, count * sizeof(*arr), s->alloc.ctx);
        if (strs) s->alloc.free(strs, strbytes, s->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(arr, 0, count * sizeof(*arr));
    s->content_protections = arr;
    s->content_protection_count = count;   /* set up front for free_root_cps */
    s->cp_strings = strs;
    s->cp_strings_size = strbytes;

    size_t off = 0;
    for (size_t i = 0; i < count; i++) {
        const moq_cmsf_content_protection_t *in = &src[i];
        moq_cmsf_content_protection_t *out = &arr[i];

        moq_bytes_t *kids = (moq_bytes_t *)s->alloc.alloc(
            in->default_kid_count * sizeof(moq_bytes_t), s->alloc.ctx);
        if (!kids) { free_root_cps(s); return MOQ_ERR_NOMEM; }
        out->default_kids = kids;
        out->default_kid_count = in->default_kid_count;

        out->ref_id = cp_put(strs, &off, in->ref_id);
        out->scheme = cp_put(strs, &off, in->scheme);
        for (size_t j = 0; j < in->default_kid_count; j++)
            kids[j] = cp_put(strs, &off, in->default_kids[j]);
        out->drm_system.system_id =
            cp_put(strs, &off, in->drm_system.system_id);
        out->drm_system.la_url   = cp_copy_url(strs, &off, &in->drm_system.la_url);
        out->drm_system.cert_url = cp_copy_url(strs, &off, &in->drm_system.cert_url);
        out->drm_system.auth_url = cp_copy_url(strs, &off, &in->drm_system.auth_url);
        out->drm_system.has_pssh = in->drm_system.has_pssh;
        if (in->drm_system.has_pssh)
            out->drm_system.pssh = cp_put(strs, &off, in->drm_system.pssh);
        out->drm_system.has_robustness = in->drm_system.has_robustness;
        if (in->drm_system.has_robustness)
            out->drm_system.robustness =
                cp_put(strs, &off, in->drm_system.robustness);
    }
    return MOQ_OK;
}


/* Resolve a track contentProtectionRefID against the configured root entries. */
bool sender_has_cp_ref(const moq_media_sender_t *s, moq_bytes_t ref)
{
    for (size_t i = 0; i < s->content_protection_count; i++) {
        moq_bytes_t r = s->content_protections[i].ref_id;
        if (r.len == ref.len && ref.len &&
            memcmp(r.data, ref.data, ref.len) == 0)
            return true;
    }
    return false;
}
