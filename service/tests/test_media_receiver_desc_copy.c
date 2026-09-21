/*
 * Contract test for moq_media_receiver_track_desc_copy() and the description
 * writer/reader locking it depends on.
 *
 * A1 -- WRITER LOCK. The live->VOD conversion (MSF-01 sec. 11.3) writes three
 *   mutable scalars of a handle-owned description in place on the network
 *   thread. A phase observer brackets those writes (VOD_WRITER_PRE/POST) and
 *   probes r->mu with an exact tri-state try-lock: the mutex must be HELD at
 *   both boundaries. Single-threaded; no timing race is sampled.
 *
 * A2 -- READER LOCK. The copy operation brackets its ownership walk and copy
 *   with DESC_READER_PRE/POST; r->mu must be HELD at both, once per call,
 *   for successful and refused calls alike.
 *
 * A3 -- FUNCTIONAL ORACLE over the real public function: the complete
 *   per-phase event inventory (kind AND handle; anything else fails), every
 *   description field the fixture represents (immutable spans byte-compared to
 *   the declared literals and identity-compared to the handle), lifetime rows
 *   (removed / terminal receiver / foreign / NULL receiver), refusal rows with
 *   a fresh whole-output sentinel, and caller-prefix rows including an
 *   oversized caller. The same oracle is run, quietly, against a TEST-ONLY
 *   legacy stand-in (an unlocked memcpy of the borrowed descriptor) as a named
 *   negative control: it must fail exactly the rows that need ownership
 *   validation, refusal-writes-nothing and stamping. Further perturbation
 *   controls (a same-count wrong-field copy, a foreign event in the inventory,
 *   a candidate that refuses the full-size path) show the oracle's sensitivity
 *   and that a refusal never makes it read an unwritten output.
 *
 * A4 -- PRODUCTION DESCRIPTION through the real endpoint pump hook with a
 *   scripted moqx peer (simpair, declared draft-16): the nested info prefix on
 *   a description produced by a session carries the sized stamp and the
 *   declared negotiated version (the no-session seam used by A1-A3 has
 *   transport_version == 0 and reaches the unsized fallback; that artifact is
 *   pinned separately). The same flow drives the real TRACK_ENDED path (a
 *   refused media-track SUBSCRIBE) for the ended-track row, copied through the
 *   real function.
 *
 * The API/export contract itself is the separate media_receiver_desc_copy_api
 * gate. White-box: links moq-service-receiver-test-internals
 * (MOQ_MEDIA_RECEIVER_TESTING) and moq::sim.
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/control.h>
#include <moq/codec.h>
#include "test_support.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Test seams (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING). */
moq_media_receiver_t *moq_media_receiver_test_new(bool auto_subscribe);
moq_media_receiver_t *moq_media_receiver_test_new_cfg(const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group,
                                    uint64_t object, const char *json,
                                    size_t len);
bool moq_media_receiver_test_poll(moq_media_receiver_t *r,
                                  moq_media_track_event_kind_t *kind,
                                  moq_media_track_t **track);
void moq_media_receiver_test_set_pub_phase_hook(
    void (*fn)(const moq_media_receiver_t *, int, void *), void *ctx);
int moq_media_receiver_test_mu_held(const moq_media_receiver_t *r);
void moq_media_receiver_test_set_fatal(moq_media_receiver_t *r, uint64_t code);
bool moq_media_receiver_test_is_fatal(const moq_media_receiver_t *r);
void moq_media_receiver_test_pump(moq_media_receiver_t *r,
                                  moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
                                                  moq_session_t *session,
                                                  uint64_t now_us);

/* Phase constants mirror media_receiver.c's enum. */
enum { PH_VOD_WRITER_PRE = 4, PH_VOD_WRITER_POST = 5, PH_DESC_READER_PRE = 6, PH_DESC_READER_POST = 7 };

/* The copy operation's frozen v0 prefix, restated independently of the header
 * so a changed public macro fails here. */
#define DESC_COPY_V0_FLOOR (offsetof(moq_media_track_desc_t, is_live) + sizeof(bool))
_Static_assert(DESC_COPY_V0_FLOOR == MOQ_MEDIA_TRACK_DESC_V0_SIZE, "public floor drifted");

static void ingest(moq_media_receiver_t *r, uint64_t g, uint64_t o, const char *json)
{
    moq_media_receiver_test_ingest(r, g, o, json, strlen(json));
}

/* -- fixtures ------------------------------------------------------------- */
/* MSF-01 sec. 11.3: a live LOC track, then the instant-VOD conversion (isLive
 * false + trackDuration) as an INDEPENDENT catalog update, then permanent
 * termination (isComplete + empty tracks). Every declared literal below is
 * checked byte-for-byte on the copy. */
#define FX_NAME     "video"
#define FX_ROLE     "video"
#define FX_CODEC    "avc1.64001f"
#define FX_PKG      "loc"
#define FX_LANG     "en"
#define FX_LABEL    "Main camera"
#define FX_MIME     "video/mp4"
#define FX_DEP      "audio"
#define FX_WIDTH    1280u
#define FX_HEIGHT   720u
#define FX_FPS_MS   30000u          /* framerate 30 -> millis */
#define FX_BITRATE  1500000u
#define FX_DURATION 60000u
#define FX_TRACK_HEAD \
    "{\"name\":\"" FX_NAME "\",\"packaging\":\"" FX_PKG "\",\"codec\":\"" FX_CODEC "\"," \
    "\"role\":\"" FX_ROLE "\",\"lang\":\"" FX_LANG "\",\"label\":\"" FX_LABEL "\"," \
    "\"mimeType\":\"" FX_MIME "\",\"depends\":[\"" FX_DEP "\"]," \
    "\"width\":1280,\"height\":720,\"framerate\":30,\"bitrate\":1500000"
#define LIVE_CAT \
    "{\"version\":\"1\",\"tracks\":[" FX_TRACK_HEAD ",\"isLive\":true}]}"
#define VOD_CAT \
    "{\"version\":\"1\",\"tracks\":[" FX_TRACK_HEAD ",\"isLive\":false,\"trackDuration\":60000}]}"
#define EMPTY_COMPLETE_CAT \
    "{\"version\":\"1\",\"streamingFormatComplete\":true,\"tracks\":[]}"

/* -- event inventory ------------------------------------------------------ */
#define MAX_EV 16
typedef struct { moq_media_track_event_kind_t kind; moq_media_track_t *track; } ev_t;
typedef struct { ev_t ev[MAX_EV]; int n; int overflow; } inventory_t;

static void drain_all(moq_media_receiver_t *r, inventory_t *inv)
{
    memset(inv, 0, sizeof(*inv));
    moq_media_track_event_kind_t k; moq_media_track_t *t;
    while (moq_media_receiver_test_poll(r, &k, &t)) {
        if (inv->n < MAX_EV) inv->ev[inv->n++] = (ev_t){ k, t };
        else inv->overflow++;
    }
}

/* Exact sequence equality: kind AND handle identity, no extras, no unknowns.
 * A NULL expected track means "must be NULL" (CATALOG_READY carries none). */
static bool inventory_matches(const inventory_t *inv, const ev_t *want, int n)
{
    if (inv->overflow || inv->n != n) return false;
    for (int i = 0; i < n; i++)
        if (inv->ev[i].kind != want[i].kind || inv->ev[i].track != want[i].track)
            return false;
    return true;
}

static void dump_inventory(const char *phase, const inventory_t *inv)
{
    fprintf(stderr, "  inventory[%s]: n=%d overflow=%d:", phase, inv->n, inv->overflow);
    for (int i = 0; i < inv->n; i++)
        fprintf(stderr, " kind=%d track=%p", (int)inv->ev[i].kind, (void *)inv->ev[i].track);
    fprintf(stderr, "\n");
}

/* -- oracle rows ---------------------------------------------------------- */
#define MAX_ROWS 128
typedef struct {
    const char *label;
    const char *tag;            /* diagnostic prefix: NATIVE-RED / STANDIN-RED */
    bool        quiet;          /* controls: record only */
    int         fails;
    char        failed[MAX_ROWS][64];   /* copied: row names may be built in a scratch buffer */
} oracle_t;

static void orow(oracle_t *o, const char *name, bool ok, int line)
{
    if (ok) return;
    if (o->fails < MAX_ROWS) snprintf(o->failed[o->fails], sizeof(o->failed[0]), "%s", name);
    o->fails++;
    if (!o->quiet)
        fprintf(stderr, "%s[%s.%s]: %s:%d\n", o->tag, o->label, name, __FILE__, line);
}
#define OROW(o, name, cond) orow((o), (name), (cond), __LINE__)

static bool has_failed(const oracle_t *o, const char *name)
{
    int n = o->fails < MAX_ROWS ? o->fails : MAX_ROWS;
    for (int i = 0; i < n; i++) if (strcmp(o->failed[i], name) == 0) return true;
    return false;
}

/* -- A1: the live->VOD writer holds r->mu across the three writes ---------- */
typedef struct { int seen[8]; int held[8]; int invalid[8]; } probe_t;
static void phase_hook(const moq_media_receiver_t *r, int phase, void *ctx)
{
    probe_t *p = (probe_t *)ctx;
    if (phase < 0 || phase > 7) return;
    p->seen[phase]++;
    int h = moq_media_receiver_test_mu_held(r);
    if (h == 1) p->held[phase]++; else if (h < 0) p->invalid[phase]++;
}

static void test_a1_vod_writer_lock_gap(void)
{
    oracle_t o = { .label = "a1.vod_writer", .tag = "FAIL", .quiet = false };
    probe_t p; memset(&p, 0, sizeof(p));
    moq_media_receiver_test_set_pub_phase_hook(phase_hook, &p);
    moq_media_receiver_t *r = moq_media_receiver_test_new(false);
    OROW(&o, "fixture.receiver", r != NULL);
    if (!r) goto out;
    inventory_t inv;
    ingest(r, 0, 0, LIVE_CAT);
    drain_all(r, &inv);
    OROW(&o, "fixture.live_inventory", inv.n == 2 && inv.ev[0].kind == MOQ_MEDIA_TRACK_ADDED &&
                                       inv.ev[1].kind == MOQ_MEDIA_CATALOG_READY);
    if (inv.n < 1) goto out;
    moq_media_track_t *video = inv.ev[0].track;
    const moq_media_track_desc_t *desc = moq_media_track_desc_get(video);
    OROW(&o, "fixture.live_desc", desc != NULL && desc->is_live && !desc->has_track_duration);
    /* The REAL catalog path: an independent VOD catalog drives receiver_reconcile,
     * which writes the three scalars in place and then queues TRACK_UPDATED. */
    ingest(r, 1, 0, VOD_CAT);
    drain_all(r, &inv);
    { ev_t want[1] = { { MOQ_MEDIA_TRACK_UPDATED, video } };
      OROW(&o, "fixture.vod_inventory", inventory_matches(&inv, want, 1)); }
    OROW(&o, "fixture.same_handle_mutated", desc == moq_media_track_desc_get(video) &&
         desc && !desc->is_live && desc->has_track_duration && desc->track_duration_ms == FX_DURATION);
    /* The observer fired exactly once at each boundary of the writer. */
    OROW(&o, "observer.pre_once", p.seen[PH_VOD_WRITER_PRE] == 1);
    OROW(&o, "observer.post_once", p.seen[PH_VOD_WRITER_POST] == 1);
    OROW(&o, "observer.probe_valid", p.invalid[PH_VOD_WRITER_PRE] + p.invalid[PH_VOD_WRITER_POST] == 0);
    /* The contract holds r->mu across the three writes, so both boundaries
     * observe HELD. Each is independent, so a lock moved inward fails exactly
     * its phase. */
    OROW(&o, "mu_held_at_writer_pre", p.held[PH_VOD_WRITER_PRE] == 1);
    OROW(&o, "mu_held_at_writer_post", p.held[PH_VOD_WRITER_POST] == 1);
out:
    moq_media_receiver_test_set_pub_phase_hook(NULL, NULL);
    if (r) moq_media_receiver_test_free(r);
    failures += o.fails;
}

/* -- A2: the copy reader holds r->mu across ownership walk and copy -------- */
static void test_a2_desc_copy_reader_lock(void)
{
    oracle_t o = { .label = "a2.desc_reader", .tag = "FAIL", .quiet = false };
    probe_t p; memset(&p, 0, sizeof(p));
    moq_media_receiver_t *r = moq_media_receiver_test_new(false);
    moq_media_receiver_t *other = moq_media_receiver_test_new(false);
    OROW(&o, "fixture.receivers", r != NULL && other != NULL);
    if (!r || !other) goto out;
    inventory_t inv;
    ingest(r, 0, 0, LIVE_CAT);
    drain_all(r, &inv);
    ingest(other, 0, 0, LIVE_CAT);
    inventory_t oinv; drain_all(other, &oinv);
    OROW(&o, "fixture.handles", inv.n == 2 && inv.ev[0].track && oinv.n == 2 && oinv.ev[0].track);
    if (!(inv.n == 2 && inv.ev[0].track && oinv.n == 2 && oinv.ev[0].track)) goto out;
    moq_media_track_t *video = inv.ev[0].track, *foreign = oinv.ev[0].track;
    moq_media_track_desc_t out;
    moq_media_receiver_test_set_pub_phase_hook(phase_hook, &p);
    /* One successful copy: PRE and POST fire once each, both HELD. */
    OROW(&o, "copy.result_ok", moq_media_receiver_track_desc_copy(r, video, &out, sizeof(out)) == MOQ_OK);
    OROW(&o, "observer.pre_once", p.seen[PH_DESC_READER_PRE] == 1);
    OROW(&o, "observer.post_once", p.seen[PH_DESC_READER_POST] == 1);
    OROW(&o, "observer.probe_valid", p.invalid[PH_DESC_READER_PRE] + p.invalid[PH_DESC_READER_POST] == 0);
    OROW(&o, "mu_held_at_reader_pre", p.held[PH_DESC_READER_PRE] == 1);
    OROW(&o, "mu_held_at_reader_post", p.held[PH_DESC_READER_POST] == 1);
    /* A refused foreign handle still walks under the lock: fires once more, HELD. */
    OROW(&o, "foreign.refused", moq_media_receiver_track_desc_copy(r, foreign, &out, sizeof(out)) == MOQ_ERR_INVAL);
    OROW(&o, "foreign.observer_twice", p.seen[PH_DESC_READER_PRE] == 2 && p.seen[PH_DESC_READER_POST] == 2);
    OROW(&o, "foreign.mu_held", p.held[PH_DESC_READER_PRE] == 2 && p.held[PH_DESC_READER_POST] == 2);
    /* Argument refusals never reach the locked section. */
    OROW(&o, "null_track.no_phase", moq_media_receiver_track_desc_copy(r, NULL, &out, sizeof(out)) == MOQ_ERR_INVAL &&
                                     p.seen[PH_DESC_READER_PRE] == 2);
    OROW(&o, "below_floor.no_phase", moq_media_receiver_track_desc_copy(r, video, &out, DESC_COPY_V0_FLOOR - 1) == MOQ_ERR_INVAL &&
                                      p.seen[PH_DESC_READER_PRE] == 2);
    /* The writer and reader use the SAME mutex: while the test holds r->mu
     * itself, the reader would block, so the observer is exercised only
     * through the phases above; the identity of the mutex is what
     * moq_media_receiver_test_mu_held probes. */
out:
    moq_media_receiver_test_set_pub_phase_hook(NULL, NULL);
    if (other) moq_media_receiver_test_free(other);
    if (r) moq_media_receiver_test_free(r);
    failures += o.fails;
}

/* -- A3: functional oracle over a copy function ---------------------------- */
typedef moq_result_t (*desc_copy_fn)(const moq_media_receiver_t *r,
                                     const moq_media_track_t *track,
                                     moq_media_track_desc_t *out, size_t out_size);

/* TEST-ONLY LEGACY STAND-IN: what a caller could do before the copy operation
 * existed -- an unlocked memcpy of the borrowed descriptor. It validates
 * nothing, writes on failure, cannot refuse a foreign handle or a NULL
 * receiver, and stamps nothing. It is a named NEGATIVE control for the oracle,
 * never a product result. */
static moq_result_t legacy_standin_copy(const moq_media_receiver_t *r,
                                        const moq_media_track_t *track,
                                        moq_media_track_desc_t *out, size_t out_size)
{
    (void)r;
    const moq_media_track_desc_t *d = moq_media_track_desc_get(track);
    if (!d || !out) return MOQ_ERR_INVAL;
    size_t n = out_size < sizeof(*d) ? out_size : sizeof(*d);
    memcpy(out, d, n);
    return MOQ_OK;
}

/* PERTURBATION CONTROL: same events, same counts, one field wrong (role and
 * label swapped). The oracle must fail exactly the role/label byte rows. */
static moq_result_t wrongfield_control_copy(const moq_media_receiver_t *r,
                                            const moq_media_track_t *track,
                                            moq_media_track_desc_t *out, size_t out_size)
{
    moq_result_t rc = legacy_standin_copy(r, track, out, out_size);
    if (rc == MOQ_OK && out_size >= sizeof(*out)) {
        moq_bytes_t tmp = out->role; out->role = out->label; out->label = tmp;
    }
    return rc;
}

/* REFUSAL CONTROL: a candidate that refuses (INVAL, nothing written) on the
 * ordinary full-size path. The oracle must report the named result rows and
 * terminate without reading the poisoned output. */
static moq_result_t refusing_control_copy(const moq_media_receiver_t *r,
                                          const moq_media_track_t *track,
                                          moq_media_track_desc_t *out, size_t out_size)
{
    if (r && track && out && out_size >= sizeof(*out)) return MOQ_ERR_INVAL;
    return legacy_standin_copy(r, track, out, out_size);
}



static void poison(void *p, size_t n) { memset(p, 0xEE, n); }
static bool all_poisoned(const void *p, size_t n)
{
    const uint8_t *b = (const uint8_t *)p;
    for (size_t i = 0; i < n; i++) if (b[i] != 0xEE) return false;
    return true;
}
static bool span_is(moq_bytes_t s, const char *lit)
{
    size_t n = strlen(lit);
    return s.len == n && (n == 0 || (s.data && memcmp(s.data, lit, n) == 0));
}

/* A candidate that CLAIMS success must have written a well-formed prefix
 * before any field that depends on it is read: the stamp is not the poison
 * word and every bool inside the claimed size holds 0 or 1 (read as bytes,
 * so a poisoned bool is detected rather than loaded). */
static bool written_prefix_valid(const void *p, size_t n)
{
    static const size_t bool_offsets[] = {
        offsetof(moq_media_track_desc_t, has_init),
        offsetof(moq_media_track_desc_t, init) + offsetof(moq_cmaf_init_info_t, has_cenc),
        offsetof(moq_media_track_desc_t, has_width),
        offsetof(moq_media_track_desc_t, has_height),
        offsetof(moq_media_track_desc_t, has_samplerate),
        offsetof(moq_media_track_desc_t, has_framerate),
        offsetof(moq_media_track_desc_t, has_bitrate),
        offsetof(moq_media_track_desc_t, has_max_grp_sap),
        offsetof(moq_media_track_desc_t, has_max_obj_sap),
        offsetof(moq_media_track_desc_t, has_template),
        offsetof(moq_media_track_desc_t, has_track_duration),
        offsetof(moq_media_track_desc_t, is_live),
    };
    const uint8_t *b = (const uint8_t *)p;
    if (n < sizeof(uint32_t)) return false;
    uint32_t stamp; memcpy(&stamp, b, sizeof(stamp));
    if (stamp == 0xEEEEEEEEu) return false;
    for (size_t i = 0; i < sizeof(bool_offsets) / sizeof(bool_offsets[0]); i++)
        if (bool_offsets[i] < n && b[bool_offsets[i]] > 1) return false;
    return true;
}

/* Record the copy result ONCE under <phase>.result_ok and, on a claimed
 * success, <phase>.written_prefix_valid. Dependent reads happen only when
 * this returns true; a refusal still reports and the caller still cleans up. */
static bool claimed(oracle_t *o, const char *phase, moq_result_t rc,
                    const void *out, size_t size, int line)
{
    char name[64];
    snprintf(name, sizeof(name), "%s.result_ok", phase);
    orow(o, name, rc == MOQ_OK, line);
    if (rc != MOQ_OK) return false;
    snprintf(name, sizeof(name), "%s.written_prefix_valid", phase);
    bool ok = written_prefix_valid(out, size);
    orow(o, name, ok, line);
    return ok;
}
#define CLAIMED(o, phase, rc, out, size) claimed((o), (phase), (rc), (out), (size), __LINE__)

/* Every field the LOC fixture represents, checked on a FULL-size copy:
 * immutable spans byte-exact against the declared literals AND borrowed from
 * the handle (identity), scalars against the literals, everything the fixture
 * leaves absent reported absent. `live` selects the mutable triple.
 * `production` selects the derived info values: a description built by a
 * session carries VIDEO/RAW (A4); on the no-session seam the sized helper
 * refuses version 0 and the fallback leaves info zeroed, so there the copy is
 * only required to carry the handle's info verbatim (the seam artifact itself
 * is pinned separately). */
static void check_fixture_fields(oracle_t *o, const char *phase,
                                 const moq_media_track_desc_t *out,
                                 const moq_media_track_desc_t *borrowed, bool live,
                                 bool production)
{
    char name[64];
#define PROW(suffix, cond) do { snprintf(name, sizeof(name), "%s.%s", phase, suffix); OROW(o, name, (cond)); } while (0)
    PROW("struct_size_stamped", out->struct_size == (uint32_t)sizeof(*out));
    PROW("name_bytes", span_is(out->name, FX_NAME));
    PROW("name_borrowed_identity", out->name.data == borrowed->name.data);
    PROW("role_bytes", span_is(out->role, FX_ROLE));
    PROW("role_borrowed_identity", out->role.data == borrowed->role.data);
    PROW("codec_bytes", span_is(out->codec, FX_CODEC));
    PROW("codec_borrowed_identity", out->codec.data == borrowed->codec.data);
    PROW("lang_bytes", span_is(out->lang, FX_LANG));
    PROW("label_bytes", span_is(out->label, FX_LABEL));
    PROW("label_borrowed_identity", out->label.data == borrowed->label.data);
    PROW("packaging_bytes", span_is(out->packaging, FX_PKG));
    PROW("mime_type_bytes", span_is(out->mime_type, FX_MIME));
    PROW("event_type_absent", out->event_type.len == 0);
    PROW("depends_count", out->depends_count == 1);
    PROW("depends_array_identity", out->depends == borrowed->depends);
    PROW("depends_0_bytes", out->depends_count == 1 && out->depends && span_is(out->depends[0], FX_DEP));
    if (production) {
        PROW("info_media_type_video", out->info.media_type == MOQ_MEDIA_TYPE_VIDEO);
        PROW("info_packaging_raw", out->info.packaging == MOQ_MEDIA_PACKAGING_RAW);
    } else {
        PROW("info_media_type_carried", out->info.media_type == borrowed->info.media_type);
        PROW("info_packaging_carried", out->info.packaging == borrowed->info.packaging);
    }
    PROW("info_stamp_carried_verbatim", out->info.struct_size == borrowed->info.struct_size);
    PROW("has_init_false", !out->has_init);
    PROW("init_data_absent", out->init_data.len == 0);
    PROW("width", out->has_width && out->width == FX_WIDTH);
    PROW("height", out->has_height && out->height == FX_HEIGHT);
    PROW("samplerate_absent", !out->has_samplerate);
    PROW("channel_config_absent", out->channel_config.len == 0);
    PROW("framerate_millis", out->has_framerate && out->framerate_millis == FX_FPS_MS);
    PROW("bitrate", out->has_bitrate && out->bitrate == FX_BITRATE);
    PROW("max_sap_absent", !out->has_max_grp_sap && !out->has_max_obj_sap);
    PROW("content_protection_absent", out->content_protection_ref_id_count == 0);
    PROW("template_absent", !out->has_template);
    if (live) {
        PROW("is_live", out->is_live);
        PROW("track_duration_absent", !out->has_track_duration);
    } else {
        PROW("is_live_false", !out->is_live);
        PROW("track_duration", out->has_track_duration && out->track_duration_ms == FX_DURATION);
    }
#undef PROW
}

static void run_desc_copy_oracle(oracle_t *o, desc_copy_fn copy)
{
    moq_media_receiver_t *r = moq_media_receiver_test_new(false);
    moq_media_receiver_t *other = moq_media_receiver_test_new(false);
    inventory_t inv;
    moq_media_track_t *video = NULL, *foreign = NULL;
    OROW(o, "fixture.receivers", r != NULL && other != NULL);
    if (!r || !other) goto cleanup;

    /* Phase LIVE: exact inventory [ADDED(video), CATALOG_READY]. */
    ingest(r, 0, 0, LIVE_CAT);
    drain_all(r, &inv);
    OROW(o, "live.inventory_shape", inv.n == 2 && inv.ev[0].kind == MOQ_MEDIA_TRACK_ADDED &&
                                    inv.ev[0].track != NULL);
    if (inv.n < 1 || !inv.ev[0].track) { dump_inventory("live", &inv); goto cleanup; }
    video = inv.ev[0].track;
    { ev_t want[2] = { { MOQ_MEDIA_TRACK_ADDED, video }, { MOQ_MEDIA_CATALOG_READY, NULL } };
      OROW(o, "live.inventory_exact", inventory_matches(&inv, want, 2)); }
    ingest(other, 0, 0, LIVE_CAT);
    drain_all(other, &inv);
    OROW(o, "foreign.inventory_shape", inv.n == 2 && inv.ev[0].kind == MOQ_MEDIA_TRACK_ADDED &&
                                       inv.ev[0].track != NULL);
    if (inv.n < 1 || !inv.ev[0].track) goto cleanup;
    foreign = inv.ev[0].track;
    OROW(o, "foreign.distinct_handle", video != foreign);
    const moq_media_track_desc_t *borrowed = moq_media_track_desc_get(video);
    OROW(o, "live.borrowed_desc", borrowed != NULL);
    if (!borrowed) goto cleanup;

    moq_media_track_desc_t out;
    poison(&out, sizeof(out));
    if (CLAIMED(o, "live", copy(r, video, &out, sizeof(out)), &out, sizeof(out)))
        check_fixture_fields(o, "live", &out, borrowed, true, false);

    /* Phase VOD: exact inventory [UPDATED(video)]; the copy reports the CURRENT
     * triple and every immutable field unchanged. */
    ingest(r, 1, 0, VOD_CAT);
    drain_all(r, &inv);
    { ev_t want[1] = { { MOQ_MEDIA_TRACK_UPDATED, video } };
      OROW(o, "vod.inventory_exact", inventory_matches(&inv, want, 1));
      if (!inventory_matches(&inv, want, 1)) dump_inventory("vod", &inv); }
    OROW(o, "vod.same_borrowed_desc", moq_media_track_desc_get(video) == borrowed);
    poison(&out, sizeof(out));
    if (CLAIMED(o, "vod", copy(r, video, &out, sizeof(out)), &out, sizeof(out)))
        check_fixture_fields(o, "vod", &out, borrowed, false, false);

    /* Phase REMOVED (isComplete + empty tracks): exact inventory [REMOVED(video)];
     * the handle stays readable with its current values. */
    ingest(r, 2, 0, EMPTY_COMPLETE_CAT);
    drain_all(r, &inv);
    { ev_t want[1] = { { MOQ_MEDIA_TRACK_REMOVED, video } };
      OROW(o, "removed.inventory_exact", inventory_matches(&inv, want, 1));
      if (!inventory_matches(&inv, want, 1)) dump_inventory("removed", &inv); }
    poison(&out, sizeof(out));
    if (CLAIMED(o, "removed", copy(r, video, &out, sizeof(out)), &out, sizeof(out)))
        check_fixture_fields(o, "removed", &out, borrowed, false, false);

    /* Phase TERMINAL: the receiver is fatal (seam-driven through the single
     * transition every fatal path takes); a terminal receiver stays readable
     * until destruction, and its queue is untouched by the transition. */
    moq_media_receiver_test_set_fatal(r, MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
    OROW(o, "terminal.is_fatal", moq_media_receiver_test_is_fatal(r));
    drain_all(r, &inv);
    OROW(o, "terminal.inventory_empty", inventory_matches(&inv, NULL, 0));
    poison(&out, sizeof(out));
    if (CLAIMED(o, "terminal", copy(r, video, &out, sizeof(out)), &out, sizeof(out)))
        check_fixture_fields(o, "terminal", &out, borrowed, false, false);

    /* Refusals: INVAL and NOTHING written -- a FRESH whole-output sentinel per
     * row. Membership is validated without dereferencing the unowned pointer,
     * so the foreign handle here is a live handle of ANOTHER receiver. */
    poison(&out, sizeof(out));
    OROW(o, "foreign.refused_inval", copy(r, foreign, &out, sizeof(out)) == MOQ_ERR_INVAL);
    OROW(o, "foreign.nothing_written", all_poisoned(&out, sizeof(out)));
    poison(&out, sizeof(out));
    OROW(o, "null_receiver.refused_inval", copy(NULL, video, &out, sizeof(out)) == MOQ_ERR_INVAL);
    OROW(o, "null_receiver.nothing_written", all_poisoned(&out, sizeof(out)));
    poison(&out, sizeof(out));
    OROW(o, "null_track.refused_inval", copy(r, NULL, &out, sizeof(out)) == MOQ_ERR_INVAL);
    OROW(o, "null_track.nothing_written", all_poisoned(&out, sizeof(out)));
    OROW(o, "null_out.refused_inval", copy(r, video, NULL, sizeof(out)) == MOQ_ERR_INVAL);
    poison(&out, sizeof(out));
    OROW(o, "zero_size.refused_inval", copy(r, video, &out, 0) == MOQ_ERR_INVAL);
    OROW(o, "zero_size.nothing_written", all_poisoned(&out, sizeof(out)));
    poison(&out, sizeof(out));
    OROW(o, "below_floor.refused_inval", copy(r, video, &out, DESC_COPY_V0_FLOOR - 1) == MOQ_ERR_INVAL);
    OROW(o, "below_floor.nothing_written", all_poisoned(&out, sizeof(out)));

    /* Caller prefixes: write min(caller, library), stamp min, canaries beyond
     * the caller's size intact. The floor covers is_live. */
    {
        size_t part = DESC_COPY_V0_FLOOR;
        poison(&out, sizeof(out));
        if (CLAIMED(o, "prefix_floor", copy(r, video, &out, part), &out, part)) {
            OROW(o, "prefix_floor.stamp_min", out.struct_size == (uint32_t)part);
            OROW(o, "prefix_floor.canaries_intact", all_poisoned((const uint8_t *)&out + part, sizeof(out) - part));
            OROW(o, "prefix_floor.is_live_covered", !out.is_live);
            OROW(o, "prefix_floor.name_bytes", span_is(out.name, FX_NAME));
        }
    }
    if (DESC_COPY_V0_FLOOR + 1 < sizeof(out)) {
        size_t part = DESC_COPY_V0_FLOOR + 1;
        poison(&out, sizeof(out));
        if (CLAIMED(o, "prefix_mid", copy(r, video, &out, part), &out, part)) {
            OROW(o, "prefix_mid.stamp_min", out.struct_size == (uint32_t)part);
            OROW(o, "prefix_mid.canaries_intact", all_poisoned((const uint8_t *)&out + part, sizeof(out) - part));
        }
    }
    {
        /* Oversized caller: a future-larger struct. The library writes only
         * what it knows, stamps ITS size, and leaves the caller's tail alone. */
        union { moq_media_track_desc_t head; uint8_t bytes[sizeof(moq_media_track_desc_t) + 64]; } big;
        poison(&big, sizeof(big));
        moq_media_track_desc_t *bo = &big.head;
        if (CLAIMED(o, "oversized", copy(r, video, bo, sizeof(big)), bo, sizeof(*bo))) {
            OROW(o, "oversized.stamp_library_size", bo->struct_size == (uint32_t)sizeof(*bo));
            OROW(o, "oversized.canaries_intact", all_poisoned(big.bytes + sizeof(*bo), 64));
            OROW(o, "oversized.is_live_false", !bo->is_live);
        }
    }

    /* Nested stamp on THIS seam: no session was ever established, so
     * receiver_track_build's sized helper refuses version 0 and the unsized
     * fallback stamps the v0 floor. That is an artifact of the seam, pinned
     * here as such; the production stamp is measured in A4. */
    OROW(o, "seam.zero_version_fallback_stamp",
         borrowed->info.struct_size == (uint32_t)MOQ_MEDIA_TRACK_INFO_V0_SIZE);
    OROW(o, "seam.zero_version_fallback_info_underived",
         borrowed->info.media_type == 0 && borrowed->info.packaging == 0);

cleanup:
    if (other) moq_media_receiver_test_free(other);
    if (r) moq_media_receiver_test_free(r);
}

/* The oracle's own sensitivity, without a native change: */
static void test_a3_oracle_perturbation_controls(void)
{
    /* Same events, same counts, one field wrong: exactly the role/label rows
     * fail in every phase, and nothing else changes relative to the stand-in. */
    oracle_t base = { .label = "control.standin", .tag = "CONTROL", .quiet = true };
    oracle_t wf = { .label = "control.wrongfield", .tag = "CONTROL", .quiet = true };
    run_desc_copy_oracle(&base, legacy_standin_copy);
    run_desc_copy_oracle(&wf, wrongfield_control_copy);
    /* The legacy stand-in fails EXACTLY the rows that need ownership
     * validation, refusal-writes-nothing and stamping -- and nothing else. */
    static const char *const standin_expected[] = {
        "foreign.refused_inval", "foreign.nothing_written",
        "null_receiver.refused_inval", "null_receiver.nothing_written",
        "zero_size.refused_inval",
        "below_floor.refused_inval", "below_floor.nothing_written",
        "prefix_floor.stamp_min", "prefix_mid.stamp_min",
    };
    for (size_t i = 0; i < sizeof(standin_expected) / sizeof(standin_expected[0]); i++)
        MOQ_TEST_CHECK(has_failed(&base, standin_expected[i]));
    MOQ_TEST_CHECK_EQ_INT(base.fails, (int)(sizeof(standin_expected) / sizeof(standin_expected[0])));
    MOQ_TEST_CHECK(!has_failed(&base, "live.role_bytes"));
    MOQ_TEST_CHECK(!has_failed(&base, "live.label_bytes"));
    MOQ_TEST_CHECK(has_failed(&wf, "live.role_bytes"));
    MOQ_TEST_CHECK(has_failed(&wf, "live.role_borrowed_identity"));
    MOQ_TEST_CHECK(has_failed(&wf, "live.label_bytes"));
    MOQ_TEST_CHECK(has_failed(&wf, "vod.role_bytes"));
    MOQ_TEST_CHECK(has_failed(&wf, "removed.role_bytes"));
    MOQ_TEST_CHECK(has_failed(&wf, "terminal.role_bytes"));
    MOQ_TEST_CHECK(!has_failed(&wf, "live.inventory_exact"));
    MOQ_TEST_CHECK(!has_failed(&wf, "live.name_bytes"));
    MOQ_TEST_CHECK_EQ_INT(wf.fails - base.fails, 4 * 4);   /* role bytes+identity, label bytes+identity, x4 phases */

    /* Refusal on the full-size path: named result failures, the dependent
     * field rows are never evaluated (not failed, not passed), balanced
     * cleanup, and the run terminates normally. */
    oracle_t rf = { .label = "control.refusing", .tag = "CONTROL", .quiet = true };
    run_desc_copy_oracle(&rf, refusing_control_copy);
    MOQ_TEST_CHECK(has_failed(&rf, "live.result_ok"));
    MOQ_TEST_CHECK(has_failed(&rf, "vod.result_ok"));
    MOQ_TEST_CHECK(has_failed(&rf, "removed.result_ok"));
    MOQ_TEST_CHECK(has_failed(&rf, "terminal.result_ok"));
    MOQ_TEST_CHECK(has_failed(&rf, "oversized.result_ok"));
    MOQ_TEST_CHECK(!has_failed(&rf, "live.written_prefix_valid"));
    MOQ_TEST_CHECK(!has_failed(&rf, "live.name_bytes"));
    MOQ_TEST_CHECK(!has_failed(&rf, "live.is_live"));
    MOQ_TEST_CHECK(!has_failed(&rf, "prefix_floor.result_ok"));   /* the prefix path is not refused */
    /* Exactly the five full-size result rows fail; the foreign-handle rows,
     * which the stand-in fails, PASS for a candidate that refuses without
     * writing (it refuses the foreign handle too). Net: +5 - 2. */
    MOQ_TEST_CHECK(!has_failed(&rf, "foreign.refused_inval"));
    MOQ_TEST_CHECK(!has_failed(&rf, "foreign.nothing_written"));
    MOQ_TEST_CHECK_EQ_INT(rf.fails - base.fails, 5 - 2);

    /* Foreign event in the inventory: the same counts with one wrong kind, one
     * wrong handle, or one extra event must not match. */
    moq_media_track_t *a = (moq_media_track_t *)&base, *b = (moq_media_track_t *)&wf;
    inventory_t inv; memset(&inv, 0, sizeof(inv));
    inv.n = 2; inv.ev[0] = (ev_t){ MOQ_MEDIA_TRACK_ADDED, a }; inv.ev[1] = (ev_t){ MOQ_MEDIA_CATALOG_READY, NULL };
    ev_t want[2] = { { MOQ_MEDIA_TRACK_ADDED, a }, { MOQ_MEDIA_CATALOG_READY, NULL } };
    MOQ_TEST_CHECK(inventory_matches(&inv, want, 2));
    inv.ev[0].track = b;                                    /* same kind, foreign handle */
    MOQ_TEST_CHECK(!inventory_matches(&inv, want, 2));
    inv.ev[0].track = a; inv.ev[1].kind = MOQ_MEDIA_TRACK_UPDATE_OK;   /* same count, foreign kind */
    MOQ_TEST_CHECK(!inventory_matches(&inv, want, 2));
    inv.ev[1].kind = MOQ_MEDIA_CATALOG_READY; inv.n = 3; inv.ev[2] = (ev_t){ MOQ_MEDIA_TRACK_PARSE_DROP, a };
    MOQ_TEST_CHECK(!inventory_matches(&inv, want, 2));     /* extra event */
    inv.n = 2; inv.overflow = 1;
    MOQ_TEST_CHECK(!inventory_matches(&inv, want, 2));     /* dropped (unrecorded) events */
}

/* -- A4: production description control + real ENDED path ----------------- */
static size_t put_varint(uint8_t *buf, uint64_t v)
{
    if (v < 0x40) { buf[0] = (uint8_t)v; return 1; }
    if (v < 0x4000) {
        buf[0] = (uint8_t)(0x40 | (v >> 8));
        buf[1] = (uint8_t)(v & 0xff);
        return 2;
    }
    if (v < 0x40000000ull) {
        buf[0] = (uint8_t)(0x80 | (v >> 24));
        buf[1] = (uint8_t)((v >> 16) & 0xff);
        buf[2] = (uint8_t)((v >> 8) & 0xff);
        buf[3] = (uint8_t)(v & 0xff);
        return 4;
    }
    buf[0] = (uint8_t)(0xc0 | (v >> 56));
    for (int i = 1; i < 8; i++)
        buf[i] = (uint8_t)((v >> (8 * (7 - i))) & 0xff);
    return 8;
}

typedef struct {
    bool have_catalog_rid; uint64_t catalog_rid;
    bool have_video_rid;   uint64_t video_rid;
    int  subscribe_seen;
} learned_t;

static void drain_and_learn(moq_session_t *client, learned_t *l)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(client, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind != MOQ_ACTION_SEND_CONTROL) continue;
            moq_control_envelope_t env;
            moq_buf_reader_t rd;
            moq_buf_reader_init(&rd, acts[i].u.send_control.data, acts[i].u.send_control.len);
            if (moq_control_decode_envelope(&rd, &env) < 0) continue;
            if (env.msg_type != MOQ_D16_SUBSCRIBE) continue;
            moq_bytes_t ns_parts[8];
            moq_kvp_entry_t params[16];
            moq_d16_subscribe_t s;
            memset(&s, 0, sizeof(s));
            s.params = params; s.params_cap = 16;
            if (moq_d16_decode_subscribe(env.payload, env.payload_len, ns_parts, 8, &s) < 0) continue;
            l->subscribe_seen++;
            if (!l->have_catalog_rid) { l->have_catalog_rid = true; l->catalog_rid = s.request_id; }
            else if (!l->have_video_rid) { l->have_video_rid = true; l->video_rid = s.request_id; }
        }
    }
}

#define DECLARED_VERSION MOQ_VERSION_DRAFT_16

static void test_a4_production_description_and_ended(desc_copy_fn copy, const char *label)
{
    oracle_t o = { .label = label, .tag = "FAIL", .quiet = false };
    moq_simpair_t *sp = NULL;
    moq_media_receiver_t *r = NULL;
    moq_session_t *client = NULL, *server = NULL;
    uint64_t now = 0;

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.version = DECLARED_VERSION;               /* declared independently of the receiver */
    OROW(&o, "fixture.simpair", moq_simpair_create(&cfg, &sp) == MOQ_OK && sp != NULL);
    if (!sp) goto cleanup;
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 16, NULL);
    client = moq_simpair_client(sp);
    server = moq_simpair_server(sp);
    OROW(&o, "fixture.established", moq_session_state(client) == MOQ_SESS_ESTABLISHED &&
                                    moq_session_state(server) == MOQ_SESS_ESTABLISHED);
    OROW(&o, "fixture.session_version", moq_session_version(client) == DECLARED_VERSION);
    { moq_event_t ev;
      while (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
      while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }

    moq_bytes_t ns_parts[2] = { MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_receiver_cfg_t rcfg;
    moq_media_receiver_cfg_init_live(&rcfg);
    rcfg.namespace_.parts = ns_parts;
    rcfg.namespace_.count = 2;
    rcfg.auto_subscribe = true;
    rcfg.time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg.overflow.max_objects = 64;
    rcfg.overflow.max_bytes = 1u << 20;
    r = moq_media_receiver_test_new_cfg(&rcfg);
    OROW(&o, "fixture.receiver", r != NULL);
    if (!r) goto cleanup;

    now = moq_simpair_now_us(sp);
    learned_t learned; memset(&learned, 0, sizeof(learned));
    uint8_t ctrl[256];
    moq_stream_ref_t rx_cat = moq_stream_ref_from_u64(101);
    bool cat_ok_sent = false, cat_data_sent = false, video_err_sent = false;
    inventory_t all; memset(&all, 0, sizeof(all));

    /* Drive the REAL hook: catalog SUBSCRIBE -> scripted SUBSCRIBE_OK -> catalog
     * subgroup (the LIVE fixture) -> TRACK_ADDED + CATALOG_READY -> the hook's
     * auto video SUBSCRIBE -> scripted REQUEST_ERROR -> TRACK_ENDED (rejection). */
    for (int cycle = 0; cycle < 40 && !moq_media_receiver_test_is_fatal(r); cycle++) {
        now += 1000;
        moq_media_receiver_test_pump(r, client, now);
        drain_and_learn(client, &learned);
        if (learned.have_catalog_rid && !cat_ok_sent) {
            moq_buf_writer_t w; moq_buf_writer_init(&w, ctrl, sizeof(ctrl));
            OROW(&o, "fixture.encode_catalog_ok",
                 moq_d16_encode_subscribe_ok(&w, learned.catalog_rid, 0, NULL, 0, NULL, 0) >= 0);
            OROW(&o, "fixture.catalog_ok_accepted",
                 moq_session_on_control_bytes(client, ctrl, moq_buf_writer_offset(&w), now) >= 0);
            cat_ok_sent = true;
        }
        if (cat_ok_sent && !cat_data_sent) {
            uint8_t buf[1024]; size_t off = 0;
            buf[off++] = 0x30;
            off += put_varint(buf + off, 0);
            off += put_varint(buf + off, 0);
            off += put_varint(buf + off, 0);
            size_t jlen = sizeof(LIVE_CAT) - 1;
            off += put_varint(buf + off, jlen);
            memcpy(buf + off, LIVE_CAT, jlen); off += jlen;
            OROW(&o, "fixture.catalog_data_accepted",
                 moq_session_on_data_bytes(client, rx_cat, buf, off, true, now) >= 0);
            cat_data_sent = true;
        }
        if (learned.have_video_rid && !video_err_sent) {
            moq_buf_writer_t w; moq_buf_writer_init(&w, ctrl, sizeof(ctrl));
            OROW(&o, "fixture.encode_video_error",
                 moq_d16_encode_request_error(&w, learned.video_rid, 0x2, 0,
                                              (const uint8_t *)"refused", 7) >= 0);
            OROW(&o, "fixture.video_error_accepted",
                 moq_session_on_control_bytes(client, ctrl, moq_buf_writer_offset(&w), now) >= 0);
            video_err_sent = true;
        }
        inventory_t inv; drain_all(r, &inv);
        for (int i = 0; i < inv.n && all.n < MAX_EV; i++) all.ev[all.n++] = inv.ev[i];
        all.overflow += inv.overflow;
        (void)moq_session_process_pending(client, now);
    }
    OROW(&o, "fixture.not_fatal", !moq_media_receiver_test_is_fatal(r));
    OROW(&o, "fixture.two_subscribes", learned.subscribe_seen == 2);
    OROW(&o, "fixture.video_error_sent", video_err_sent);

    /* Exact inventory: [ADDED(video), CATALOG_READY, ENDED(video)]. */
    OROW(&o, "inventory_shape", all.n == 3 && all.ev[0].kind == MOQ_MEDIA_TRACK_ADDED && all.ev[0].track);
    if (!(all.n >= 1 && all.ev[0].track)) { dump_inventory("scripted", &all); goto cleanup; }
    moq_media_track_t *video = all.ev[0].track;
    { ev_t want[3] = { { MOQ_MEDIA_TRACK_ADDED, video }, { MOQ_MEDIA_CATALOG_READY, NULL },
                       { MOQ_MEDIA_TRACK_ENDED, video } };
      OROW(&o, "inventory_exact", inventory_matches(&all, want, 3));
      if (!inventory_matches(&all, want, 3)) dump_inventory("scripted", &all); }

    /* PRODUCTION nested prefix: the sized helper ran with the latched session
     * version, so the stamp is the full info size and the version is the
     * declared one -- measured here, not inferred from the no-session seam. */
    const moq_media_track_desc_t *borrowed = moq_media_track_desc_get(video);
    OROW(&o, "production.borrowed_desc", borrowed != NULL);
    if (!borrowed) goto cleanup;
    OROW(&o, "production.info_stamp_sized", borrowed->info.struct_size == (uint32_t)sizeof(moq_media_track_info_t));
    OROW(&o, "production.info_transport_version_declared", borrowed->info.transport_version == DECLARED_VERSION);
    OROW(&o, "production.still_live", borrowed->is_live && !borrowed->has_track_duration);

    /* Ended-track row: the rejected track stays readable with current values,
     * and the copy carries the production nested stamp verbatim. */
    moq_media_track_desc_t out; poison(&out, sizeof(out));
    if (CLAIMED(&o, "ended", copy(r, video, &out, sizeof(out)), &out, sizeof(out))) {
        check_fixture_fields(&o, "ended", &out, borrowed, true, true);
        OROW(&o, "ended.info_stamp_sized", out.info.struct_size == (uint32_t)sizeof(moq_media_track_info_t));
        OROW(&o, "ended.info_transport_version_declared", out.info.transport_version == DECLARED_VERSION);
    }

cleanup:
    if (sp) {
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        if (r) { now += 1000; moq_media_receiver_test_destroy_with_session(r, client, now); r = NULL; }
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        { moq_event_t ev;
          while (moq_session_poll_events(client, &ev, 1) == 1) moq_event_cleanup(&ev);
          while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev); }
        moq_simpair_destroy(sp);
    }
    if (r) moq_media_receiver_test_free(r);
    failures += o.fails;
}

int main(void)
{
    test_a1_vod_writer_lock_gap();
    test_a2_desc_copy_reader_lock();
    { oracle_t o = { .label = "a3.desc_copy", .tag = "FAIL", .quiet = false };
      run_desc_copy_oracle(&o, moq_media_receiver_track_desc_copy);
      failures += o.fails; }
    test_a3_oracle_perturbation_controls();
    test_a4_production_description_and_ended(moq_media_receiver_track_desc_copy, "a4.desc_copy");
    if (failures) {
        fprintf(stderr, "media_receiver_desc_copy: %d failure(s)\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("media_receiver_desc_copy");
    return 0;
}
