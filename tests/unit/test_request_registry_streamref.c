/*
 * Focused white-box tests for the request stream-ref registry
 * (idx_req_by_streamref + request_registry_*_by_streamref).
 *
 * This is a version-neutral core seam: the index lets stream-correlated
 * request profiles (those that open a dedicated bidi stream per request and
 * correlate responses by stream identity) map a request stream_ref back to its
 * pool slot. Draft-16 correlates by request_id and never populates it. The
 * seam is groundwork for draft-18 (the first such profile), but nothing tested
 * here is draft-specific.
 *
 * Coverage: index primitives (insert/find/remove, collisions, probe-chain
 * integrity); independence of the stream-ref and request-id indexes (removing
 * one leaves the other intact); the FETCH request_stream_ref vs data_stream_ref
 * distinction; and proof that a real draft-16 subscribe flow leaves the
 * stream-ref index empty. No wire behavior is exercised.
 */
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"

/* Mirror of session.c's internal idx_hash (fmix64), used only to construct a
 * deterministic collision pair for the probe-chain test. */
static uint64_t test_idx_hash(uint64_t key)
{
    key ^= key >> 33;
    key *= 0xFF51AFD7ED558CCDULL;
    key ^= key >> 33;
    key *= 0xC4CEB9FE1A85EC53ULL;
    key ^= key >> 33;
    return key;
}

/* Count populated slots in the stream-ref index. */
static size_t streamref_index_count(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i <= s->idx_req_streamref_mask; i++)
        if (s->idx_req_by_streamref[i].slot >= 0) n++;
    return n;
}

static moq_request_endpoint_t make_ep(moq_request_kind_t kind, int slot)
{
    moq_request_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.kind = kind;
    ep.slot = slot;
    ep.has_stream_ref = true;
    return ep;
}

static moq_session_t *make_session(moq_alloc_t *alloc, moq_perspective_t persp)
{
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = alloc;
    cfg.perspective = persp;
    moq_session_t *s = NULL;
    moq_session_create(&cfg, 0, &s);
    return s;
}

int main(void)
{
    int failures = 0;

    /* == A. Fresh index is empty; every lookup misses ================= */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_t *s = make_session(&alloc, MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);

        MOQ_TEST_CHECK_EQ_SIZE(streamref_index_count(s), 0);
        for (uint64_t k = 1; k <= 64; k++) {
            moq_request_endpoint_t got =
                request_registry_find_by_streamref(s,
                    moq_stream_ref_from_u64(k));
            MOQ_TEST_CHECK_EQ_INT((int)got.kind, (int)MOQ_REQ_NONE);
            MOQ_TEST_CHECK_EQ_INT(got.slot, -1);
        }

        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    /* == B. Insert / find / remove across request kinds =============== */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_t *s = make_session(&alloc, MOQ_PERSPECTIVE_CLIENT);

        struct { uint64_t ref; moq_request_kind_t kind; int slot; } cases[] = {
            { 1000, MOQ_REQ_SUBSCRIPTION, 0 },
            { 2000, MOQ_REQ_ANNOUNCEMENT, 3 },
            { 3000, MOQ_REQ_FETCH,        1 },
            { 4000, MOQ_REQ_PUBLISH,      2 },
            { 5000, MOQ_REQ_TRACK_STATUS, 4 },
        };
        size_t ncases = sizeof(cases) / sizeof(cases[0]);

        for (size_t i = 0; i < ncases; i++)
            request_registry_insert_by_streamref(s,
                moq_stream_ref_from_u64(cases[i].ref),
                make_ep(cases[i].kind, cases[i].slot));

        MOQ_TEST_CHECK_EQ_SIZE(streamref_index_count(s), ncases);

        for (size_t i = 0; i < ncases; i++) {
            moq_request_endpoint_t got =
                request_registry_find_by_streamref(s,
                    moq_stream_ref_from_u64(cases[i].ref));
            MOQ_TEST_CHECK_EQ_INT((int)got.kind, (int)cases[i].kind);
            MOQ_TEST_CHECK_EQ_INT(got.slot, cases[i].slot);
            MOQ_TEST_CHECK(got.has_stream_ref);
            MOQ_TEST_CHECK_EQ_U64(got.stream_ref._v, cases[i].ref);
            /* find_by_streamref carries no request_id. */
            MOQ_TEST_CHECK(!got.has_request_id);
        }

        /* Remove one; it misses, the rest remain. */
        request_registry_remove_by_streamref(s,
            moq_stream_ref_from_u64(cases[2].ref));
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s,
                moq_stream_ref_from_u64(cases[2].ref)).kind,
            (int)MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_SIZE(streamref_index_count(s), ncases - 1);
        for (size_t i = 0; i < ncases; i++) {
            if (i == 2) continue;
            MOQ_TEST_CHECK_EQ_INT(
                (int)request_registry_find_by_streamref(s,
                    moq_stream_ref_from_u64(cases[i].ref)).kind,
                (int)cases[i].kind);
        }

        /* Remove the rest; index empties. */
        for (size_t i = 0; i < ncases; i++)
            request_registry_remove_by_streamref(s,
                moq_stream_ref_from_u64(cases[i].ref));
        MOQ_TEST_CHECK_EQ_SIZE(streamref_index_count(s), 0);

        /* Removing a missing key is a no-op. */
        request_registry_remove_by_streamref(s,
            moq_stream_ref_from_u64(999999));
        MOQ_TEST_CHECK_EQ_SIZE(streamref_index_count(s), 0);

        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    /* == C. Deterministic collision pair: probe chain + backshift ===== */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_t *s = make_session(&alloc, MOQ_PERSPECTIVE_CLIENT);
        size_t mask = s->idx_req_streamref_mask;

        /* Find two distinct keys that share a home bucket. */
        uint64_t k1 = 7;
        uint64_t home = test_idx_hash(k1) & mask;
        uint64_t k2 = 0;
        for (uint64_t cand = k1 + 1; cand < k1 + 100000; cand++) {
            if ((test_idx_hash(cand) & mask) == home) { k2 = cand; break; }
        }
        MOQ_TEST_CHECK(k2 != 0);

        request_registry_insert_by_streamref(s,
            moq_stream_ref_from_u64(k1), make_ep(MOQ_REQ_SUBSCRIPTION, 5));
        request_registry_insert_by_streamref(s,
            moq_stream_ref_from_u64(k2), make_ep(MOQ_REQ_FETCH, 6));

        /* Both findable despite the collision. */
        MOQ_TEST_CHECK_EQ_INT(
            request_registry_find_by_streamref(s,
                moq_stream_ref_from_u64(k1)).slot, 5);
        MOQ_TEST_CHECK_EQ_INT(
            request_registry_find_by_streamref(s,
                moq_stream_ref_from_u64(k2)).slot, 6);

        /* Remove the head of the chain; the colliding entry survives
         * (validates remove backshift over a real probe chain). */
        request_registry_remove_by_streamref(s, moq_stream_ref_from_u64(k1));
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s,
                moq_stream_ref_from_u64(k1)).kind, (int)MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT(
            request_registry_find_by_streamref(s,
                moq_stream_ref_from_u64(k2)).slot, 6);

        request_registry_remove_by_streamref(s, moq_stream_ref_from_u64(k2));
        MOQ_TEST_CHECK_EQ_SIZE(streamref_index_count(s), 0);

        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    /* == D. Dense run + interleaved removal (chain integrity) ========= */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_t *s = make_session(&alloc, MOQ_PERSPECTIVE_CLIENT);

        /* Stay within table capacity (cap = mask + 1, load factor < 1). */
        size_t cap = s->idx_req_streamref_mask + 1;
        size_t n = cap / 2;
        if (n > 200) n = 200;

        for (size_t i = 0; i < n; i++)
            request_registry_insert_by_streamref(s,
                moq_stream_ref_from_u64(10000 + i),
                make_ep(MOQ_REQ_SUBSCRIPTION, (int)(i % 0xFFFF)));
        MOQ_TEST_CHECK_EQ_SIZE(streamref_index_count(s), n);

        /* Remove the even-indexed keys. */
        for (size_t i = 0; i < n; i += 2)
            request_registry_remove_by_streamref(s,
                moq_stream_ref_from_u64(10000 + i));

        /* Odd ones still resolve to their original slots; even ones gone. */
        for (size_t i = 0; i < n; i++) {
            moq_request_endpoint_t got =
                request_registry_find_by_streamref(s,
                    moq_stream_ref_from_u64(10000 + i));
            if (i % 2 == 0) {
                MOQ_TEST_CHECK_EQ_INT((int)got.kind, (int)MOQ_REQ_NONE);
            } else {
                MOQ_TEST_CHECK_EQ_INT((int)got.kind, (int)MOQ_REQ_SUBSCRIPTION);
                MOQ_TEST_CHECK_EQ_INT(got.slot, (int)(i % 0xFFFF));
            }
        }

        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    /* == E. Dual-index independence: removing the stream-ref key is a ==
     *       pure secondary-index removal and does NOT disturb the
     *       request-id index. The two indexes are independent keys onto
     *       the same request pool; a terminal path removes both. */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_t *s = make_session(&alloc, MOQ_PERSPECTIVE_CLIENT);

        uint64_t rid = 42;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(7777);
        moq_request_endpoint_t ep = make_ep(MOQ_REQ_SUBSCRIPTION, 9);
        ep.has_request_id = true;
        ep.request_id = rid;

        request_registry_insert_by_id(s, rid, ep);
        request_registry_insert_by_streamref(s, ref, ep);

        /* Remove via stream-ref. */
        request_registry_remove_by_streamref(s, ref);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_NONE);

        /* by-id entry is untouched. */
        moq_request_endpoint_t by_id = request_registry_find_by_id(s, rid);
        MOQ_TEST_CHECK_EQ_INT((int)by_id.kind, (int)MOQ_REQ_SUBSCRIPTION);
        MOQ_TEST_CHECK_EQ_INT(by_id.slot, 9);
        MOQ_TEST_CHECK(by_id.has_request_id);

        /* Clean up the by-id entry at the index level (avoid invoking the
         * profile release path on a synthetic slot). */
        moq_index_remove(s->idx_req_by_rid, s->idx_req_mask, rid);

        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    /* == F. FETCH request_stream_ref vs data_stream_ref are distinct == */
    {
        moq_fetch_entry_t fe;
        memset(&fe, 0, sizeof(fe));
        fe.request_stream_ref = moq_stream_ref_from_u64(0xAAAA);
        fe.data_stream_ref    = moq_stream_ref_from_u64(0xBBBB);
        MOQ_TEST_CHECK_EQ_U64(fe.request_stream_ref._v, 0xAAAA);
        MOQ_TEST_CHECK_EQ_U64(fe.data_stream_ref._v, 0xBBBB);
        MOQ_TEST_CHECK(fe.request_stream_ref._v != fe.data_stream_ref._v);
    }

    /* == G. D16 neutrality: a real subscribe flow never touches the ===
     *       stream-ref index on either side. */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        /* Handshake alone must not populate the stream-ref index. */
        MOQ_TEST_CHECK_EQ_SIZE(streamref_index_count(c), 0);
        MOQ_TEST_CHECK_EQ_SIZE(streamref_index_count(sv), 0);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        sub_cfg.track_namespace = ns;
        sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t sub_handle;
        MOQ_TEST_CHECK(moq_session_subscribe(c, &sub_cfg, 1000,
            &sub_handle) == MOQ_OK);
        pump_actions_to_peer(c, sv, 1000);

        /* D16 registered the request the by-id way... */
        MOQ_TEST_CHECK(streamref_index_count(c) == 0);
        MOQ_TEST_CHECK(streamref_index_count(sv) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    MOQ_TEST_PASS("request_registry_streamref");
    return failures != 0;
}
