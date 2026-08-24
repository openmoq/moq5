/*
 * RED evidence for MOQ-MEDIA-CP-LOOKUP-RACE (security report finding 2): the
 * effective-catalog publication and the public content-protection lookup are
 * not serialized by a common mutex.
 *
 * PRIMARY ORACLE (deterministic, single-threaded, no timing): one phase
 * observer fires at FOUR points that bracket the two critical sections a
 * correct publication contract must serialize -- writer PRE (before any access
 * to has_effective/effective) and POST (after retention/detach, publication,
 * and the completion stat, before the intended unlock); reader PRE (after the
 * null check, before reading has_effective/selecting effective) and POST (after
 * the traversal result is selected, before the intended unlock). Each boundary
 * probes r->mu with an EXACT tri-state try-lock (moq_media_receiver_test_mu_held:
 * FREE / HELD / INVALID). The intended contract is that all four boundaries run
 * with r->mu HELD; current code holds it at NONE, so all four are RED. A
 * decorative lock around a single point cannot pass, because both ends of each
 * section are checked. A complete candidate locking repair satisfies all four
 * (see the report's anti-vacuity / boundary-kill runs).
 *
 * SECONDARY (corroborating, TSan): a threaded stress with an atomic stop flag
 * (so the harness itself is race-free) alternates catalogs with materially
 * different content-protection shapes while a reader loops lookups.
 *
 * White-box: links moq-service-receiver-test-internals
 * (MOQ_MEDIA_RECEIVER_TESTING); the seam is absent from the shipping library.
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include "test_support.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Test seam (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING). */
moq_media_receiver_t *moq_media_receiver_test_new(bool auto_subscribe);
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
size_t moq_media_receiver_test_cp_snap_count(const moq_media_receiver_t *r);

/* Phase constants mirror media_receiver.c's enum. */
enum { PH_WRITER_PRE = 0, PH_WRITER_POST = 1, PH_READER_PRE = 2,
       PH_READER_POST = 3 };
static const char *ph_name[4] = { "writer_pre", "writer_post",
                                  "reader_pre", "reader_post" };

static void ingest(moq_media_receiver_t *r, uint64_t g, uint64_t o,
                   const char *json)
{
    moq_media_receiver_test_ingest(r, g, o, json, strlen(json));
}
static void drain(moq_media_receiver_t *r)
{
    moq_media_track_event_kind_t k;
    moq_media_track_t *t;
    while (moq_media_receiver_test_poll(r, &k, &t)) { /* discard */ }
}

#define TRK(name, role) \
    "{\"name\":\"" name "\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"" role "\"}"
#define CP_CAT \
    "{\"version\":\"1\",\"contentProtections\":[{\"refID\":\"cp1\"," \
    "\"defaultKID\":[\"kid\"],\"scheme\":\"cenc\",\"drmSystem\":" \
    "{\"systemID\":\"sys\"}}],\"tracks\":[" TRK("a", "video") "]}"
#define PLAIN_CAT \
    "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}"
#define COMPLETE_CAT \
    "{\"version\":\"1\",\"streamingFormatComplete\":true,\"tracks\":[" \
    TRK("a", "video") "]}"

typedef struct {
    int seen[4];
    int held[4];
    int invalid[4];
} phase_probe_t;

static void phase_hook(const moq_media_receiver_t *r, int phase, void *ctx)
{
    phase_probe_t *p = (phase_probe_t *)ctx;
    if (phase < 0 || phase > 3) return;
    p->seen[phase]++;
    int h = moq_media_receiver_test_mu_held(r);
    if (h == 1) p->held[phase]++;
    else if (h < 0) p->invalid[phase]++;
}

/* -- The deterministic publication-contract oracle. Drives the six required
 *    commit states so the writer contract is proven across every accepted
 *    catalog path, and two lookups (hit + miss) for the reader contract. Each
 *    of the four phases is an INDEPENDENT held-assertion, so a boundary moved
 *    inward fails exactly its phase (see the report's boundary kills). */
static void test_publication_contract(void)
{
    phase_probe_t p;
    memset(&p, 0, sizeof(p));
    moq_media_receiver_test_set_pub_phase_hook(phase_hook, &p);

    moq_media_receiver_t *r = moq_media_receiver_test_new(false);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) { moq_media_receiver_test_set_pub_phase_hook(NULL, NULL); return; }

    /* Empty-path lookup BEFORE any ingest: no effective catalog exists, so the
     * accessor selects NULL -- but the selection and its POST still run inside
     * the intended lock. This is the reader boundary the early-return shape
     * skipped; it makes reader PRE/POST fire exactly three times (empty, hit,
     * miss) rather than two. */
    MOQ_TEST_CHECK(moq_media_receiver_find_content_protection(
        r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 }) == NULL);

    /* Six commit states -- exactly six writer commits. */
    ingest(r, 0, 0, CP_CAT); drain(r);            /* first independent, w/ CP  */
    ingest(r, 1, 0, CP_CAT); drain(r);            /* replace WITH CP           */
    ingest(r, 2, 0, PLAIN_CAT); drain(r);         /* replace WITHOUT CP        */
    ingest(r, 2, 1, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":["
                    TRK("b", "audio") "]}]}"); drain(r);   /* delta-produced   */
    ingest(r, 3, 0, COMPLETE_CAT); drain(r);      /* terminal/complete         */
    ingest(r, 4, 0, CP_CAT); drain(r);            /* CP-bearing current for the
                                                     reader lookups below       */

    /* Two more lookups: hit then miss (both traverse under the intended lock). */
    const moq_cmsf_content_protection_t *cp =
        moq_media_receiver_find_content_protection(
            r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 });
    MOQ_TEST_CHECK(cp != NULL);
    (void)moq_media_receiver_find_content_protection(
        r, (moq_bytes_t){ (const uint8_t *)"nope", 4 });

    /* No INVALID probe result on any phase (unexpected mutex error). */
    for (int ph = 0; ph < 4; ph++)
        MOQ_TEST_CHECK_EQ_INT(p.invalid[ph], 0);

    /* Each phase fired the EXACT expected number of times, and every firing
     * observed r->mu HELD. RED now (held==0 on every phase). Six writer commits;
     * three reader lookups (empty, hit, miss). An omitted commit path drops the
     * writer count to five; an empty path that skips POST drops the reader-post
     * count to two -- both caught by the exact seen assertions, independently of
     * held. Four independent contracts. */
    MOQ_TEST_CHECK_EQ_INT(p.seen[PH_WRITER_PRE], 6);
    MOQ_TEST_CHECK_EQ_INT(p.seen[PH_WRITER_POST], 6);
    MOQ_TEST_CHECK_EQ_INT(p.seen[PH_READER_PRE], 3);
    MOQ_TEST_CHECK_EQ_INT(p.seen[PH_READER_POST], 3);
    for (int ph = 0; ph < 4; ph++) {
        if (p.held[ph] != p.seen[ph])
            fprintf(stderr, "FAIL: publication phase %s: held %d of %d\n",
                    ph_name[ph], p.held[ph], p.seen[ph]);
        MOQ_TEST_CHECK_EQ_INT(p.held[ph], p.seen[ph]);
    }

    /* Retention contract (A5): the CP-bearing generations really were retained
     * before we free the receiver -- proven through the test accessor, not by
     * merely calling free. Exactly two prior CP generations were retained in
     * cp_snaps by the CP-less/complete replacements. (The retention lifetime
     * itself is covered by test_media_catalog_update.c's cp_pointer_valid and
     * the snapshot cap/byte tests.) */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_test_cp_snap_count(r), 2);

    moq_media_receiver_test_free(r);              /* exclusive teardown        */
    moq_media_receiver_test_set_pub_phase_hook(NULL, NULL);
    MOQ_TEST_PASS("pub_race.publication_contract");
}

/* GREEN control (current semantics): a returned CP pointer stays valid across a
 * later replacement (cp_snaps retention). Holds today and after the fix; does
 * NOT satisfy the publication oracle. */
static void test_retained_pointer_control(void)
{
    moq_media_receiver_test_set_pub_phase_hook(NULL, NULL);
    moq_media_receiver_t *r = moq_media_receiver_test_new(false);
    ingest(r, 0, 0, CP_CAT); drain(r);
    const moq_cmsf_content_protection_t *cp =
        moq_media_receiver_find_content_protection(
            r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 });
    MOQ_TEST_CHECK(cp != NULL);
    const uint8_t *scheme = cp ? cp->scheme.data : NULL;
    size_t scheme_len = cp ? cp->scheme.len : 0;
    ingest(r, 1, 0, PLAIN_CAT); drain(r);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_test_cp_snap_count(r), 1);
    MOQ_TEST_CHECK(scheme && scheme_len == 4 && memcmp(scheme, "cenc", 4) == 0);
    MOQ_TEST_CHECK(moq_media_receiver_find_content_protection(
        r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 }) == NULL);
    moq_media_receiver_test_free(r);
    MOQ_TEST_PASS("pub_race.retained_pointer_control");
}

/* SECONDARY: threaded stress for ThreadSanitizer. The stop flag is atomic so
 * the harness contributes no race of its own; the only race is the product
 * defect. Not the primary oracle. */
typedef struct {
    moq_media_receiver_t *r;
    atomic_bool stop;
} stress_t;

static void *reader_thread(void *arg)
{
    stress_t *s = (stress_t *)arg;
    while (!atomic_load_explicit(&s->stop, memory_order_relaxed)) {
        (void)moq_media_receiver_find_content_protection(
            s->r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 });
    }
    return NULL;
}

static void test_tsan_stress(void)
{
    moq_media_receiver_test_set_pub_phase_hook(NULL, NULL);
    moq_media_receiver_t *r = moq_media_receiver_test_new(false);
    ingest(r, 0, 0, CP_CAT); drain(r);

    stress_t s;
    s.r = r;
    atomic_init(&s.stop, false);
    pthread_t th;
    if (pthread_create(&th, NULL, reader_thread, &s) != 0) {
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("pub_race.tsan_stress(skipped: no thread)");
        return;
    }
    for (uint64_t g = 1; g <= 400; g++) {
        ingest(r, g, 0, (g & 1) ? PLAIN_CAT : CP_CAT);
        drain(r);
    }
    atomic_store_explicit(&s.stop, true, memory_order_relaxed);
    pthread_join(th, NULL);
    moq_media_receiver_test_free(r);
    MOQ_TEST_PASS("pub_race.tsan_stress(ran)");
}

int main(void)
{
    test_publication_contract();
    test_retained_pointer_control();
    /* The stress runs only when requested: in a plain build the real race can
     * crash and mask the deterministic oracle's exit code; the TSan lane sets
     * this and (with a complete candidate patch) must run clean. */
    if (getenv("MOQ_PUB_RACE_STRESS"))
        test_tsan_stress();
    if (failures) {
        fprintf(stderr, "media_catalog_publication_race: %d failure(s)\n",
                failures);
        return 1;
    }
    return 0;
}
