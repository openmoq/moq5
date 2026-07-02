/*
 * White-box tests for MSF-01 catalog updates on the receiver: independent
 * catalog replacement and delta-catalog apply. Drives the catalog
 * ingest/reconcile/apply path directly with crafted (group, object, json)
 * objects via the test seam in media_receiver.c (MOQ_MEDIA_RECEIVER_TESTING) --
 * no network.
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include "test_support.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
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
uint64_t moq_media_receiver_test_catalog_drops(const moq_media_receiver_t *r);
bool moq_media_receiver_test_is_fatal(const moq_media_receiver_t *r);
uint64_t moq_media_receiver_test_fatal_code(const moq_media_receiver_t *r);
void moq_media_receiver_test_set_limits(moq_media_receiver_t *r,
                                        size_t max_stable_handles,
                                        size_t max_cp_snaps,
                                        uint64_t max_retained_bytes);
size_t moq_media_receiver_test_track_count(const moq_media_receiver_t *r);
void moq_media_receiver_test_push_media_timeline(moq_media_receiver_t *r,
                                                 uint64_t group,
                                                 uint64_t object,
                                                 uint64_t media_time_ms,
                                                 uint64_t wallclock_ms);

static void ingest(moq_media_receiver_t *r, uint64_t g, uint64_t o,
                   const char *json)
{
    moq_media_receiver_test_ingest(r, g, o, json, strlen(json));
}

static bool name_is(const moq_media_track_t *t, const char *s)
{
    const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
    size_t n = strlen(s);
    return d && d->name.len == n && memcmp(d->name.data, s, n) == 0;
}

static bool role_is(const moq_media_track_t *t, const char *s)
{
    const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
    size_t n = strlen(s);
    return d && d->role.len == n && memcmp(d->role.data, s, n) == 0;
}

/* Snapshot the receiver's catalog_complete stat (via the public stats path). */
static bool stats_complete(moq_media_receiver_t *r)
{
    moq_media_receiver_stats_t s;
    if (moq_media_receiver_get_stats(r, &s, sizeof(s)) != MOQ_OK) return false;
    return s.catalog_complete;
}

/* True when t's desc carries a template equal to the eight expected values. */
static bool template_is(const moq_media_track_t *t,
                        uint64_t sm, uint64_t dm, uint64_t sg, uint64_t so,
                        uint64_t dg, uint64_t do_, uint64_t sw, uint64_t dw)
{
    const moq_media_track_desc_t *d = moq_media_track_desc_get(t);
    if (!d || !d->has_template) return false;
    const moq_msf_media_template_t *p = &d->template_;
    return p->start_media_ms == sm && p->delta_media_ms == dm &&
           p->start_group == sg && p->start_object == so &&
           p->delta_group == dg && p->delta_object == do_ &&
           p->start_wallclock_ms == sw && p->delta_wallclock_ms == dw;
}

/* Drain events into out arrays; returns the count. */
static int drain(moq_media_receiver_t *r, moq_media_track_event_kind_t *kinds,
                 moq_media_track_t **tracks, int max)
{
    int n = 0;
    moq_media_track_event_kind_t k;
    moq_media_track_t *t;
    while (n < max && moq_media_receiver_test_poll(r, &k, &t)) {
        kinds[n] = k; tracks[n] = t; n++;
    }
    return n;
}

/* Count events of a given kind. When want_name != NULL, count only those whose
 * track name matches and capture the matching track into *out. When want_name
 * == NULL, count every event of that kind. */
static int count_kind(const moq_media_track_event_kind_t *kinds,
                      moq_media_track_t **tracks, int n,
                      moq_media_track_event_kind_t kind,
                      const char *want_name, moq_media_track_t **out)
{
    int c = 0;
    for (int i = 0; i < n; i++) {
        if (kinds[i] != kind) continue;
        if (want_name && !(tracks[i] && name_is(tracks[i], want_name))) continue;
        c++;
        if (want_name && out) *out = tracks[i];
    }
    return c;
}

/* The idx-th TRACK_ADDED handle in event order (NULL if fewer). */
static moq_media_track_t *nth_added(const moq_media_track_event_kind_t *kinds,
                                    moq_media_track_t **tracks, int n, int idx)
{
    int c = 0;
    for (int i = 0; i < n; i++) {
        if (kinds[i] != MOQ_MEDIA_TRACK_ADDED) continue;
        if (c == idx) return tracks[i];
        c++;
    }
    return NULL;
}

#define CAT2(t1, t2) \
    "{\"version\":\"1\",\"tracks\":[" t1 "," t2 "]}"
#define TRK(name, role) \
    "{\"name\":\"" name "\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"" role "\"}"

int main(void)
{
    moq_media_track_event_kind_t k[16];
    moq_media_track_t *tr[16];
    int n;

    /* -- 1. independent -> independent: add + remove, survivor stable ---- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        MOQ_TEST_CHECK(r != NULL);

        ingest(r, 0, 0, CAT2(TRK("a", "video"), TRK("b", "audio")));
        n = drain(r, k, tr, 16);
        moq_media_track_t *a0 = NULL, *b0 = NULL;
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 2);
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "a", &a0);
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "b", &b0);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_CATALOG_READY, NULL, NULL), 1);
        MOQ_TEST_CHECK(a0 && b0);

        /* gen1: a survives, b removed, c added. */
        ingest(r, 1, 0, CAT2(TRK("a", "video"), TRK("c", "video")));
        n = drain(r, k, tr, 16);
        moq_media_track_t *c1 = NULL, *brem = NULL;
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "c", &c1), 1);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_REMOVED, "b", &brem), 1);
        MOQ_TEST_CHECK(brem == b0);                 /* same handle */
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "a", NULL), 0);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_CATALOG_READY, NULL, NULL), 0);
        /* 'a' handle is stable + desc still valid across the update. */
        MOQ_TEST_CHECK(name_is(a0, "a"));
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));

        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.independent_add_remove");
    }

    /* -- 2. dedupe the current generation's object 0 (SUBSCRIBE+FETCH) --- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, CAT2(TRK("a", "video"), TRK("b", "audio")));
        (void)drain(r, k, tr, 16);
        ingest(r, 1, 0, CAT2(TRK("a", "video"), TRK("d", "video")));   /* add d */
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "d", NULL), 1);
        /* Re-deliver the SAME generation's object 0 -> deduped, no events. */
        ingest(r, 1, 0, CAT2(TRK("a", "video"), TRK("d", "video")));
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.dedupe");
    }

    /* -- 3. immutable-tuple violation drops the whole update ------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, CAT2(TRK("a", "video"), TRK("b", "audio")));
        (void)drain(r, k, tr, 16);
        uint64_t drops0 = moq_media_receiver_test_catalog_drops(r);
        /* gen1 changes 'a' role (immutable) AND adds 'e' -> whole drop. */
        ingest(r, 1, 0, CAT2(TRK("a", "audio"), TRK("e", "video")));
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);                /* no ADDED(e), no events */
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), drops0 + 1);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        /* A later clean independent still applies (not stuck/desynced). */
        ingest(r, 2, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_REMOVED, "b", NULL), 1);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.immutable_drop");
    }

    /* -- 4. content-protection: old returned pointer stays valid -------- */
    {
        static const char *g0 =
            "{\"version\":\"1\",\"contentProtections\":[{\"refID\":\"cp1\","
            "\"defaultKID\":[\"kid\"],\"scheme\":\"cenc\",\"drmSystem\":"
            "{\"systemID\":\"sys\"}}],\"tracks\":[" TRK("a", "video") "]}";
        static const char *g1 =   /* no contentProtections */
            "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}";
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, g0);
        (void)drain(r, k, tr, 16);
        const moq_cmsf_content_protection_t *cp =
            moq_media_receiver_find_content_protection(
                r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 });
        MOQ_TEST_CHECK(cp != NULL);
        const uint8_t *scheme_ptr = cp ? cp->scheme.data : NULL;
        size_t scheme_len = cp ? cp->scheme.len : 0;
        /* Update to a generation without contentProtections. */
        ingest(r, 1, 0, g1);
        (void)drain(r, k, tr, 16);
        /* The previously returned pointer is still readable (snapshot retained). */
        MOQ_TEST_CHECK(scheme_ptr && scheme_len == 4 &&
                       memcmp(scheme_ptr, "cenc", 4) == 0);
        /* The current resolver no longer finds cp1. */
        MOQ_TEST_CHECK(moq_media_receiver_find_content_protection(
            r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 }) == NULL);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.cp_pointer_valid");
    }

    /* -- 5. first catalog malformed -> fatal --------------------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, "{ not valid json");
        MOQ_TEST_CHECK(moq_media_receiver_test_is_fatal(r));
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.first_malformed_fatal");
    }

    /* -- 6. later malformed independent -> non-fatal drop -------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        (void)drain(r, k, tr, 16);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        ingest(r, 1, 0, "{ garbage");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.later_malformed_drop");
    }

    /* -- 7. stale group (< latest) ignored ----------------------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 5, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        (void)drain(r, k, tr, 16);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        ingest(r, 3, 0, CAT2(TRK("a", "video"), TRK("b", "audio")));  /* older */
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);                 /* ignored, no events */
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.stale_group_ignored");
    }

    /* -- 8. auto-subscribe seeds new tracks in any generation ---------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(true);
        ingest(r, 0, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *a = NULL;
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "a", &a);
        moq_media_track_state_t st;
        MOQ_TEST_CHECK(a && moq_media_receiver_track_state(r, a, &st) == MOQ_OK);
        MOQ_TEST_CHECK(st == MOQ_MEDIA_TRACK_STATE_PENDING);   /* auto-seeded */
        /* A track added in a LATER generation is auto-seeded too. */
        ingest(r, 1, 0, CAT2(TRK("a", "video"), TRK("b", "audio")));
        n = drain(r, k, tr, 16);
        moq_media_track_t *b = NULL;
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "b", &b);
        MOQ_TEST_CHECK(b && moq_media_receiver_track_state(r, b, &st) == MOQ_OK);
        MOQ_TEST_CHECK(st == MOQ_MEDIA_TRACK_STATE_PENDING);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.auto_subscribe_later_gen");
    }

    /* -- 9. removed handle: subscribe/unsubscribe/state -> WRONG_STATE -- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, CAT2(TRK("a", "video"), TRK("b", "audio")));
        (void)drain(r, k, tr, 16);
        ingest(r, 1, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *brem = NULL;
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_REMOVED, "b", &brem);
        MOQ_TEST_CHECK(brem != NULL);
        if (brem) {
            moq_media_track_state_t st;
            MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, brem, NULL)
                           == MOQ_ERR_WRONG_STATE);
            MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(r, brem)
                           == MOQ_ERR_WRONG_STATE);
            MOQ_TEST_CHECK(moq_media_receiver_track_state(r, brem, &st)
                           == MOQ_ERR_WRONG_STATE);
        }
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.removed_handle_terminal");
    }

    /* -- 10. full tuple key: same name in different namespaces --------- */
    {
        static const char *g0 =
            "{\"version\":\"1\",\"tracks\":["
            "{\"name\":\"v\",\"namespace\":\"n1\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\"},"
            "{\"name\":\"v\",\"namespace\":\"n2\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"audio\"}]}";
        static const char *g1 =   /* n1/v removed, n2/v survives */
            "{\"version\":\"1\",\"tracks\":["
            "{\"name\":\"v\",\"namespace\":\"n2\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"audio\"}]}";
        static const char *g2 =   /* n2/v survives, n3/v added (same local name) */
            "{\"version\":\"1\",\"tracks\":["
            "{\"name\":\"v\",\"namespace\":\"n2\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"audio\"},"
            "{\"name\":\"v\",\"namespace\":\"n3\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\"}]}";

        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, g0);
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 2);
        moq_media_track_t *v_n1 = nth_added(k, tr, n, 0);
        moq_media_track_t *v_n2 = nth_added(k, tr, n, 1);
        MOQ_TEST_CHECK(v_n1 && v_n2 && v_n1 != v_n2);

        /* Removing only n1/v must remove exactly that handle; n2/v survives. */
        ingest(r, 1, 0, g1);
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_REMOVED, NULL, NULL), 1);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 0);
        moq_media_track_t *removed = NULL;
        for (int i = 0; i < n; i++)
            if (k[i] == MOQ_MEDIA_TRACK_REMOVED) removed = tr[i];
        MOQ_TEST_CHECK(removed == v_n1);          /* the n1 handle, not n2 */
        MOQ_TEST_CHECK(name_is(v_n2, "v"));       /* n2 handle still valid */

        /* Adding n3/v (same local name) must not collide with or mutate n2/v. */
        ingest(r, 2, 0, g2);
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 1);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_REMOVED, NULL, NULL), 0);
        moq_media_track_t *v_n3 = nth_added(k, tr, n, 0);
        MOQ_TEST_CHECK(v_n3 && v_n3 != v_n2 && v_n3 != v_n1);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.namespace_tuple_key");
    }

    /* -- 11. namespace-override track is not subscribed under recv ns --- */
    {
        static const char *g0 =
            "{\"version\":\"1\",\"tracks\":["
            "{\"name\":\"m\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\"},"
            "{\"name\":\"x\",\"namespace\":\"other\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\"}]}";
        moq_media_receiver_t *r = moq_media_receiver_test_new(true);  /* auto */
        ingest(r, 0, 0, g0);
        n = drain(r, k, tr, 16);
        moq_media_track_t *m = NULL, *x = NULL;
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "m", &m);
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "x", &x);
        MOQ_TEST_CHECK(m && x);
        moq_media_track_state_t st;
        /* The inherited-namespace track is auto-seeded; the override track is
         * NOT (it would otherwise subscribe under the wrong namespace). */
        MOQ_TEST_CHECK(moq_media_receiver_track_state(r, m, &st) == MOQ_OK);
        MOQ_TEST_CHECK(st == MOQ_MEDIA_TRACK_STATE_PENDING);
        MOQ_TEST_CHECK(moq_media_receiver_track_state(r, x, &st) == MOQ_OK);
        MOQ_TEST_CHECK(st == MOQ_MEDIA_TRACK_STATE_DISCOVERED);
        /* Manual subscribe of the override track is refused, not silently
         * pointed at the receiver namespace. */
        MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, x, NULL)
                       == MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(r, m, NULL) == MOQ_OK);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.namespace_override_not_subscribed");
    }

    /* -- 12. delta add / remove / clone within a group ----------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, CAT2(TRK("a", "video"), TRK("b", "audio")));
        n = drain(r, k, tr, 16);
        moq_media_track_t *b0 = NULL;
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "b", &b0);

        /* delta add c (object 1). */
        ingest(r, 0, 1, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":["
                        TRK("c", "video") "]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "c", NULL), 1);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 1);

        /* delta remove b (object 2) -> REMOVED, handle terminal. */
        ingest(r, 0, 2, "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":["
                        "{\"name\":\"b\"}]}]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *brem = NULL;
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_REMOVED, "b", &brem), 1);
        MOQ_TEST_CHECK(brem == b0);
        moq_media_track_state_t st;
        MOQ_TEST_CHECK(moq_media_receiver_track_state(r, b0, &st) == MOQ_ERR_WRONG_STATE);

        /* delta clone a -> a2 (object 3), inheriting metadata + overriding. */
        ingest(r, 0, 3, "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
                        "{\"parentName\":\"a\",\"name\":\"a2\",\"bitrate\":600000}]}]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *a2 = NULL;
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "a2", &a2), 1);
        MOQ_TEST_CHECK(a2 && role_is(a2, "video"));    /* inherited from a */
        const moq_media_track_desc_t *a2d = moq_media_track_desc_get(a2);
        MOQ_TEST_CHECK(a2d && a2d->has_bitrate && a2d->bitrate == 600000);  /* override */

        /* duplicate already-applied delta object id -> ignored, no events. */
        ingest(r, 0, 3, "{\"deltaUpdate\":[{\"op\":\"clone\",\"tracks\":["
                        "{\"parentName\":\"a\",\"name\":\"a2\",\"bitrate\":600000}]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.delta_add_remove_clone");
    }

    /* -- 13. delta gap/out-of-order desyncs; next independent re-syncs -- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        (void)drain(r, k, tr, 16);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        /* object 2 with object 1 missing -> gap -> desync + one drop. */
        ingest(r, 0, 2, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[" TRK("x", "video") "]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        /* later delta in the same desynced group is ignored (no extra drop). */
        ingest(r, 0, 1, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[" TRK("y", "video") "]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        /* a newer independent object 0 re-syncs. */
        ingest(r, 1, 0, CAT2(TRK("a", "video"), TRK("z", "video")));
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "z", NULL), 1);
        /* a delta after the re-sync applies again. */
        ingest(r, 1, 1, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[" TRK("w", "video") "]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "w", NULL), 1);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.delta_gap_resync");
    }

    /* -- 13b. future-group delta before its object 0 desyncs; object 0
     *         of that group then re-syncs ---------------------------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        (void)drain(r, k, tr, 16);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        /* A delta for group 2 (its independent object 0 not yet seen). */
        ingest(r, 2, 1, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[" TRK("x", "video") "]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        /* Group 2's independent object 0 arrives -> re-syncs, emits diffs. */
        ingest(r, 2, 0, CAT2(TRK("a", "video"), TRK("z", "video")));
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "z", NULL), 1);
        /* A delta in group 2 after object 0 now applies normally. */
        ingest(r, 2, 1, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[" TRK("w", "video") "]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "w", NULL), 1);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.delta_future_group_then_obj0");
    }

    /* -- 14. stale-group delta ignored --------------------------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 5, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        (void)drain(r, k, tr, 16);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        ingest(r, 3, 1, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[" TRK("x", "video") "]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0);  /* not counted */
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.delta_stale_ignored");
    }

    /* -- 15. malformed delta + apply failure: non-fatal, effective kept - */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        (void)drain(r, k, tr, 16);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        ingest(r, 0, 1, "{ not json");                 /* malformed delta */
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        /* effective preserved: a newer independent that drops 'a' still works. */
        ingest(r, 1, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);                    /* a survives, no churn */
        moq_media_receiver_test_free(r);

        /* apply failure (remove an absent track) -> non-fatal drop. */
        r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        (void)drain(r, k, tr, 16);
        d0 = moq_media_receiver_test_catalog_drops(r);
        ingest(r, 0, 1, "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":["
                        "{\"name\":\"ghost\"}]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.delta_failures_nonfatal");
    }

    /* -- 16. first catalog object being a delta is fatal --------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 1, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[" TRK("a", "video") "]}]}");
        MOQ_TEST_CHECK(moq_media_receiver_test_is_fatal(r));
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.first_delta_fatal");
    }

    /* -- 17. CP snapshot preserved across a delta commit --------------- */
    {
        static const char *g0 =
            "{\"version\":\"1\",\"contentProtections\":[{\"refID\":\"cp1\","
            "\"defaultKID\":[\"kid\"],\"scheme\":\"cenc\",\"drmSystem\":"
            "{\"systemID\":\"sys\"}}],\"tracks\":[" TRK("a", "video") "]}";
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, g0);
        (void)drain(r, k, tr, 16);
        const moq_cmsf_content_protection_t *cp =
            moq_media_receiver_find_content_protection(
                r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 });
        MOQ_TEST_CHECK(cp != NULL);
        const uint8_t *scheme_ptr = cp ? cp->scheme.data : NULL;
        /* A delta add commits a new effective (apply carries contentProtections
         * forward) and retains the old CP-bearing effective as a snapshot. */
        ingest(r, 0, 1, "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[" TRK("b", "audio") "]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "b", NULL), 1);
        /* old pointer still valid; cp1 still resolvable (carried forward). */
        MOQ_TEST_CHECK(scheme_ptr && memcmp(scheme_ptr, "cenc", 4) == 0);
        MOQ_TEST_CHECK(moq_media_receiver_find_content_protection(
            r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 }) != NULL);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.delta_cp_snapshot");
    }

    /* -- 18. mediatimeline track: discovered with raw packaging/mimeType/
     *    depends surfaced (the auto-subscribe skip + object-drop is covered by
     *    the loopback media_receiver test) -------------------------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(true);
        ingest(r, 0, 0,
            "{\"version\":\"1\",\"tracks\":[" TRK("v", "video") ","
            "{\"name\":\"history\",\"packaging\":\"mediatimeline\","
            "\"isLive\":true,\"mimeType\":\"application/json\","
            "\"depends\":[\"v\"]}]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *hist = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "history", &hist), 1);
        const moq_media_track_desc_t *d =
            hist ? moq_media_track_desc_get(hist) : NULL;
        MOQ_TEST_CHECK(d && d->packaging.len == 13 &&
            memcmp(d->packaging.data, "mediatimeline", 13) == 0);
        MOQ_TEST_CHECK(d && d->mime_type.len == 16 &&
            memcmp(d->mime_type.data, "application/json", 16) == 0);
        MOQ_TEST_CHECK(d && d->depends_count == 1 && d->depends[0].len == 1 &&
            d->depends[0].data && d->depends[0].data[0] == 'v');
        MOQ_TEST_CHECK(d && d->event_type.len == 0);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.mediatimeline_discovered");
    }

    /* -- 19. changing a surviving track's template is an immutable-tuple
     *    violation (§7.4.2): whole update dropped, drops++, effective kept - */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":true,\"template\":[0,2002,[0,0],[1,0],1000,2002]}]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *v0 = NULL;
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "v", &v0);
        MOQ_TEST_CHECK(v0 && template_is(v0, 0, 2002, 0, 0, 1, 0, 1000, 2002));
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        /* gen1: same tuple, CHANGED template -> violation, whole update dropped */
        ingest(r, 1, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":true,\"template\":[0,3003,[0,0],[1,0],1000,3003]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);                         /* no churn */
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        /* the surviving handle's desc still carries the ORIGINAL template */
        MOQ_TEST_CHECK(v0 && template_is(v0, 0, 2002, 0, 0, 1, 0, 1000, 2002));
        /* effective still holds the ORIGINAL template: re-declaring it is clean. */
        ingest(r, 2, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":true,\"template\":[0,2002,[0,0],[1,0],1000,2002]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);                         /* survivor unchanged */
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.template_immutable_violation");
    }

    /* -- 20. a media track's inline template is surfaced on its desc with all
     *    eight decoded values (MSF §7.4) ---------------------------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":true,\"role\":\"video\","
            "\"template\":[5,2002,[1,2],[3,4],1759924158381,2002]}]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "v", &v), 1);
        MOQ_TEST_CHECK(v && template_is(v, 5, 2002, 1, 2, 3, 4,
                                        1759924158381ULL, 2002));
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.template_surfaced_on_desc");
    }

    /* -- 21. VOD track: isLive:false + trackDuration surfaced on desc ---- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":false,\"trackDuration\":8072340}]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "v", &v), 1);
        const moq_media_track_desc_t *d = v ? moq_media_track_desc_get(v) : NULL;
        MOQ_TEST_CHECK(d && d->has_track_duration &&
                       d->track_duration_ms == 8072340);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.vod_track_duration");
    }

    /* -- 22. changing a surviving track's trackDuration is an immutable-tuple
     *    violation: dropped, drops++, desc keeps the original duration ---- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":false,\"trackDuration\":5000}]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *v = NULL;
        (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "v", &v);
        MOQ_TEST_CHECK(v != NULL);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        ingest(r, 1, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":false,\"trackDuration\":9999}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);                         /* whole update dropped */
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        const moq_media_track_desc_t *d = v ? moq_media_track_desc_get(v) : NULL;
        MOQ_TEST_CHECK(d && d->has_track_duration &&
                       d->track_duration_ms == 5000);       /* original kept */
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.track_duration_immutable");
    }

    /* -- 23. terminating catalog (isComplete + empty tracks): removes tracks,
     *    latches catalog_complete, and is terminal for later updates ------ */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, CAT2(TRK("a", "video"), TRK("b", "audio")));
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(
            count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 2);
        MOQ_TEST_CHECK(!stats_complete(r));
        /* terminating independent update: both tracks REMOVED, completion set. */
        ingest(r, 1, 0, "{\"version\":\"1\",\"isComplete\":true,\"tracks\":[]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(
            count_kind(k, tr, n, MOQ_MEDIA_TRACK_REMOVED, NULL, NULL), 2);
        MOQ_TEST_CHECK(stats_complete(r));
        /* after completion, a later independent generation is dropped and does
         * not resurrect tracks; completion stays latched. */
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        ingest(r, 2, 0, "{\"version\":\"1\",\"tracks\":[" TRK("c", "video") "]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);                         /* no resurrection */
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        MOQ_TEST_CHECK(stats_complete(r));
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.terminating_complete");
    }

    /* -- 24. stats ABI: appended fields honor the prefix size contract --- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        (void)drain(r, k, tr, 16);
        ingest(r, 1, 0, "{\"version\":\"1\",\"isComplete\":true,\"tracks\":[]}");
        (void)drain(r, k, tr, 16);
        /* a post-complete update -> at least one catalog drop. */
        ingest(r, 2, 0, "{\"version\":\"1\",\"tracks\":[" TRK("b", "video") "]}");
        (void)drain(r, k, tr, 16);

        const size_t v0sz =
            offsetof(moq_media_receiver_stats_t, paused) + sizeof(bool);
        const size_t dropsz =
            offsetof(moq_media_receiver_stats_t, catalog_drops) + sizeof(uint64_t);

        /* v0 size: succeeds, needs no appended field, stamps the v0 prefix. */
        moq_media_receiver_stats_t s0;
        memset(&s0, 0, sizeof(s0));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_get_stats(r, &s0, v0sz), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(s0.struct_size, (uint64_t)v0sz);

        /* through catalog_drops: drops surfaced, catalog_complete NOT written. */
        moq_media_receiver_stats_t sd;
        memset(&sd, 0, sizeof(sd));
        sd.catalog_complete = false;                 /* sentinel: must stay false */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_get_stats(r, &sd, dropsz), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(sd.struct_size, (uint64_t)dropsz);
        MOQ_TEST_CHECK(sd.catalog_drops >= 1);
        MOQ_TEST_CHECK(sd.catalog_complete == false);   /* clamped out, untouched */

        /* full size: catalog_complete surfaced (true after termination). */
        moq_media_receiver_stats_t sf;
        memset(&sf, 0, sizeof(sf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_get_stats(r, &sf, sizeof(sf)), (int)MOQ_OK);
        MOQ_TEST_CHECK(sf.catalog_complete == true);
        MOQ_TEST_CHECK(sf.catalog_drops >= 1);

        /* below the v0 base -> INVAL. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_get_stats(r, &sf, v0sz - 1),
            (int)MOQ_ERR_INVAL);

        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.stats_abi_generations");
    }

    /* -- 25. live->VOD conversion (§11.3): a surviving tuple flips isLive
     *    true->false and gains trackDuration -> accepted, same handle, desc
     *    updated, TRACK_UPDATED emitted (the one sanctioned tuple mutation) -- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *v = NULL;
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "v", &v), 1);
        MOQ_TEST_CHECK(v != NULL);
        const moq_media_track_desc_t *d = moq_media_track_desc_get(v);
        MOQ_TEST_CHECK(d && d->is_live && !d->has_track_duration);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);

        /* gen1: same tuple, isLive:false + trackDuration -> conversion. */
        ingest(r, 1, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":false,\"trackDuration\":8072340,\"role\":\"video\","
            "\"codec\":\"av01\"}]}");
        n = drain(r, k, tr, 16);
        moq_media_track_t *upd = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            count_kind(k, tr, n, MOQ_MEDIA_TRACK_UPDATED, "v", &upd), 1);
        MOQ_TEST_CHECK(upd == v);     /* SAME handle */
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 0);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_REMOVED, NULL, NULL), 0);
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0);  /* accepted */
        d = moq_media_track_desc_get(v);
        MOQ_TEST_CHECK(d && !d->is_live && d->has_track_duration &&
                       d->track_duration_ms == 8072340);
        /* not isComplete: catalog_complete must stay false. */
        MOQ_TEST_CHECK(!stats_complete(r));
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.live_to_vod_conversion");
    }

    /* -- 26. forbidden surviving-tuple changes are dropped (immutable §5.3):
     *    isLive false->true; duration change; duration-while-live; unrelated
     *    metadata change. Each keeps the prior effective + counts a drop. ---- */
    {
        struct { const char *g0, *g1; const char *what; } cases[] = {
            /* isLive false->true. */
            { "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":false,\"trackDuration\":5000}]}",
              "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":true}]}", "false->true" },
            /* changing an existing trackDuration. */
            { "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":false,\"trackDuration\":5000}]}",
              "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":false,\"trackDuration\":9999}]}", "duration change" },
            /* adding trackDuration while staying live. */
            { "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":true}]}",
              "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":true,\"trackDuration\":5000}]}", "duration while live" },
            /* half conversion: isLive true->false with NO trackDuration (§11.3
             * requires a duration). */
            { "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":true}]}",
              "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":false}]}", "true->false no duration" },
            /* unrelated metadata change (codec) alongside a legal VOD flip. */
            { "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":true,\"codec\":\"av01\"}]}",
              "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
              "\"isLive\":false,\"trackDuration\":5000,\"codec\":\"hev1\"}]}",
              "unrelated change" },
        };
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            moq_media_receiver_t *r = moq_media_receiver_test_new(false);
            ingest(r, 0, 0, cases[i].g0);
            n = drain(r, k, tr, 16);
            moq_media_track_t *v = NULL;
            (void)count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "v", &v);
            MOQ_TEST_CHECK(v != NULL);
            bool live0 = moq_media_track_desc_get(v)->is_live;
            uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
            ingest(r, 1, 0, cases[i].g1);
            n = drain(r, k, tr, 16);
            MOQ_TEST_CHECK_EQ_INT(n, 0);   /* whole update dropped, no events */
            MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
            /* prior desc unchanged. */
            MOQ_TEST_CHECK(moq_media_track_desc_get(v)->is_live == live0);
            moq_media_receiver_test_free(r);
        }
        MOQ_TEST_PASS("catalog_update.vod_forbidden_changes_dropped");
    }

    /* -- bound: CP-snapshot retention is capped (availability) ----------- *
     * A peer that streams many CP-bearing independent generations would grow
     * cp_snaps without bound. With a small CP-snapshot cap, updates are accepted
     * until the cap, then dropped nonfatally: the current effective and a
     * previously returned CP pointer stay valid. */
    {
        /* One track + one contentProtection (refID cp1). Identical content each
         * generation, so every new group is adopted (TM_SAME, not a violation)
         * and the prior CP-bearing effective is retained as a snapshot. */
        static const char *cpcat =
            "{\"version\":\"1\",\"contentProtections\":[{\"refID\":\"cp1\","
            "\"defaultKID\":[\"kid\"],\"scheme\":\"cenc\",\"drmSystem\":"
            "{\"systemID\":\"sys\"}}],\"tracks\":[" TRK("a", "video") "]}";
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        /* Only the CP-snapshot cap (2) bites; handles/bytes stay generous. */
        moq_media_receiver_test_set_limits(r, 1000, 2, 64u * 1024u * 1024u);

        ingest(r, 0, 0, cpcat);             /* gen0: ready, effective has CP */
        (void)drain(r, k, tr, 16);
        const moq_cmsf_content_protection_t *cp0 =
            moq_media_receiver_find_content_protection(
                r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 });
        MOQ_TEST_CHECK(cp0 != NULL);
        const uint8_t *p0 = cp0 ? cp0->scheme.data : NULL;
        size_t         p0len = cp0 ? cp0->scheme.len : 0;

        ingest(r, 1, 0, cpcat);             /* snapshot gen0 -> cp_snaps[0] */
        ingest(r, 2, 0, cpcat);             /* snapshot gen1 -> cp_snaps[1] */
        (void)drain(r, k, tr, 16);
        uint64_t drops0 = moq_media_receiver_test_catalog_drops(r);

        ingest(r, 3, 0, cpcat);             /* would need cp_snaps[2] -> dropped */
        (void)drain(r, k, tr, 16);

        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));        /* nonfatal */
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), drops0 + 1);
        /* Current CP still resolvable (effective preserved at gen2). */
        MOQ_TEST_CHECK(moq_media_receiver_find_content_protection(
            r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 }) != NULL);
        /* A pointer returned before the cap (into an accepted snapshot) is still
         * valid memory. */
        MOQ_TEST_CHECK(p0 && p0len == 4 && memcmp(p0, "cenc", 4) == 0);

        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.cp_snapshot_cap");
    }

    /* -- bound: unique-track churn is capped (availability) -------------- *
     * Each generation removes the old tracks (handles retained per the lifetime
     * contract) and adds fresh unique ones. With a small handle cap, updates are
     * accepted until the cap, then dropped nonfatally with no handle growth. */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        moq_media_receiver_test_set_limits(r, 4, 1000, 64u * 1024u * 1024u);

        ingest(r, 0, 0, CAT2(TRK("a0", "video"), TRK("b0", "audio")));
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 2);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_media_receiver_test_track_count(r), 2u);

        /* gen1: fresh unique names -> 2 added, 2 removed (handles retained=4). */
        ingest(r, 1, 0, CAT2(TRK("a1", "video"), TRK("b1", "audio")));
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 2);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_media_receiver_test_track_count(r), 4u);
        uint64_t drops0 = moq_media_receiver_test_catalog_drops(r);

        /* gen2: would push handle count to 6 (> cap 4) -> dropped, no growth. */
        ingest(r, 2, 0, CAT2(TRK("a2", "video"), TRK("b2", "audio")));
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 0);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_media_receiver_test_track_count(r), 4u);
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), drops0 + 1);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));

        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.handle_churn_cap");
    }

    /* -- bound: first catalog over the cap terminalizes ------------------ *
     * If the very first catalog exceeds a retained-state cap there is no usable
     * effective, so the receiver goes fatal (CATALOG_UNUSABLE) rather than
     * hanging unready. */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        moq_media_receiver_test_set_limits(r, 1, 1000, 64u * 1024u * 1024u);

        ingest(r, 0, 0, CAT2(TRK("a", "video"), TRK("b", "audio")));  /* 2 > cap 1 */
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_CATALOG_READY, NULL, NULL), 0);
        MOQ_TEST_CHECK(moq_media_receiver_test_is_fatal(r));
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_fatal_code(r),
                              MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);

        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.first_catalog_over_cap_fatal");
    }

    /* -- bound: initRef-resolved init bytes count toward the byte cap ---- *
     * A track using initRef retains a decoded init buffer; the byte estimate
     * must charge the resolved initDataList entry, not just inline initData.
     * With a generous handle cap but a small byte cap, an initRef-bearing churn
     * drops on the byte budget before the handle count is reached. */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        moq_media_receiver_test_set_limits(r, 1000, 1000, 5000); /* bytes bite */

        char cat[6000];
        /* Build a catalog: one ~3 KB inline init-data entry "i0" + one track
         * that pulls it in via initRef (so the handle retains a big init_buf). */
        #define INITREF_CAT(buf, track_name) do {                              \
            int pre_ = snprintf((buf), sizeof(buf),                            \
                "{\"version\":\"1\",\"initDataList\":[{\"id\":\"i0\","         \
                "\"type\":\"inline\",\"data\":\"");                            \
            size_t dlen_ = 3000;   /* base64 chars (multiple of 4) */          \
            memset((buf) + pre_, 'A', dlen_);                                  \
            snprintf((buf) + pre_ + dlen_, sizeof(buf) - pre_ - dlen_,         \
                "\"}],\"tracks\":[{\"name\":\"" track_name "\","              \
                "\"packaging\":\"loc\",\"isLive\":true,\"initRef\":\"i0\"}]}"); \
        } while (0)

        INITREF_CAT(cat, "t0");
        ingest(r, 0, 0, cat);                    /* gen0: ~3 KB init handle */
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 1);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_media_receiver_test_track_count(r), 1u);
        uint64_t drops0 = moq_media_receiver_test_catalog_drops(r);

        INITREF_CAT(cat, "t1");                  /* unique track, same big init */
        ingest(r, 1, 0, cat);                    /* +~3 KB -> over the 5 KB cap */
        n = drain(r, k, tr, 16);
        #undef INITREF_CAT
        MOQ_TEST_CHECK_EQ_INT(count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 0);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_media_receiver_test_track_count(r), 1u);
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), drops0 + 1);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));

        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.initref_byte_cap");
    }

    /* -- bound: CP-snapshot DOM bytes count toward the byte cap ---------- *
     * A retained CP snapshot holds the whole parsed catalog (DOM + parser
     * arrays). The estimate must charge _dom_size, not just summed string
     * lengths, so a CP-bearing catalog with a large DOM trips the byte cap even
     * with a high snapshot-count cap. The padding lives in `systemID` -- a DOM
     * string the count-based estimate never summed -- so this fails without the
     * _dom_size fix. Drop is nonfatal; effective + prior CP pointer survive. */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        moq_media_receiver_test_set_limits(r, 1000, 1000, 4000); /* bytes bite */

        char cat[8000];
        int pre = snprintf(cat, sizeof cat,
            "{\"version\":\"1\",\"contentProtections\":[{\"refID\":\"cp1\","
            "\"defaultKID\":[\"kid\"],\"scheme\":\"cenc\",\"drmSystem\":"
            "{\"systemID\":\"");
        size_t pad = 5000;                       /* DOM padding (not counted by
                                                    the old string-sum estimate) */
        memset(cat + pre, 'x', pad);
        snprintf(cat + pre + pad, sizeof cat - pre - pad,
            "\"}}],\"tracks\":[" TRK("a", "video") "]}");

        ingest(r, 0, 0, cat);                    /* gen0: big-DOM CP effective */
        (void)drain(r, k, tr, 16);
        const moq_cmsf_content_protection_t *cp0 =
            moq_media_receiver_find_content_protection(
                r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 });
        MOQ_TEST_CHECK(cp0 != NULL);
        const uint8_t *p0 = cp0 ? cp0->scheme.data : NULL;
        size_t         p0len = cp0 ? cp0->scheme.len : 0;
        uint64_t drops0 = moq_media_receiver_test_catalog_drops(r);

        ingest(r, 1, 0, cat);   /* committing would snapshot gen0's big DOM -> over cap */
        (void)drain(r, k, tr, 16);

        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), drops0 + 1);
        /* Effective preserved (gen0): current CP resolvable, prior pointer valid. */
        MOQ_TEST_CHECK(moq_media_receiver_find_content_protection(
            r, (moq_bytes_t){ (const uint8_t *)"cp1", 3 }) != NULL);
        MOQ_TEST_CHECK(p0 && p0len == 4 && memcmp(p0, "cenc", 4) == 0);

        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.cp_snapshot_byte_cap");
    }

    /* -- 28. first independent catalog with a LIVE track carrying
     *    trackDuration (§5.2.35 forbidden) is rejected as unusable/fatal,
     *    surfacing no usable track ----------------------------------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0,
            "{\"version\":\"1\",\"tracks\":[{\"name\":\"v\",\"packaging\":\"loc\","
            "\"isLive\":true,\"trackDuration\":424242}]}");
        MOQ_TEST_CHECK(moq_media_receiver_test_is_fatal(r));
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(
            count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, NULL, NULL), 0);
        MOQ_TEST_CHECK_EQ_U64(
            (uint64_t)moq_media_receiver_test_track_count(r), 0);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.first_live_with_duration_fatal");
    }

    /* -- 29. a LATER independent catalog adding a new live track with
     *    trackDuration is dropped; the existing effective is preserved and the
     *    receiver stays non-fatal ----------------------------------------- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(
            count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "a", NULL), 1);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        ingest(r, 1, 0,
            "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") ","
            "{\"name\":\"b\",\"packaging\":\"loc\",\"isLive\":true,"
            "\"trackDuration\":5000}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);                          /* whole update dropped */
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        MOQ_TEST_CHECK_EQ_U64(
            (uint64_t)moq_media_receiver_test_track_count(r), 1);  /* only "a" */
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.later_live_with_duration_dropped");
    }

    /* -- 30. a delta `add` of a live track with trackDuration is dropped; the
     *    existing effective is preserved and the receiver stays non-fatal --- */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        ingest(r, 0, 0, "{\"version\":\"1\",\"tracks\":[" TRK("a", "video") "]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(
            count_kind(k, tr, n, MOQ_MEDIA_TRACK_ADDED, "a", NULL), 1);
        uint64_t d0 = moq_media_receiver_test_catalog_drops(r);
        ingest(r, 0, 1,
            "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[{\"name\":\"b\","
            "\"packaging\":\"loc\",\"isLive\":true,\"trackDuration\":5000}]}]}");
        n = drain(r, k, tr, 16);
        MOQ_TEST_CHECK_EQ_INT(n, 0);                          /* delta dropped */
        MOQ_TEST_CHECK_EQ_U64(moq_media_receiver_test_catalog_drops(r), d0 + 1);
        MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
        MOQ_TEST_CHECK_EQ_U64(
            (uint64_t)moq_media_receiver_test_track_count(r), 1);  /* only "a" */
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.delta_live_with_duration_dropped");
    }

    /* -- 31. wait() is level-triggered on the media-timeline queue too ----- */
    /* A queued media-timeline record with every OTHER queue empty must satisfy
     * moq_media_receiver_wait()'s ready pre-check (MOQ_OK without blocking),
     * exactly like a queued track event / object / SAP record. Regression for
     * the pre-check omitting mt_head/mt_tail: with the omission, wait() falls
     * through to the endpoint path and (no endpoint here) reports CLOSED --
     * i.e. a timeline-only wakeup had no wait-contract guarantee. */
    {
        moq_media_receiver_t *r = moq_media_receiver_test_new(false);
        moq_media_receiver_test_push_media_timeline(r, 3, 0, 1200, 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_wait(r, 0), (int)MOQ_OK);

        /* The record is really there and pollable. */
        moq_media_timeline_record_t rec;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_media_receiver_poll_media_timeline(r, &rec, sizeof(rec)),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(rec.group, 3);
        MOQ_TEST_CHECK_EQ_U64(rec.media_time_ms, 1200);
        moq_media_receiver_test_free(r);
        MOQ_TEST_PASS("catalog_update.wait_level_triggered_on_media_timeline");
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
