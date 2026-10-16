/*
 * Negotiated-profile CLOSURE DIRECTION B: independently built LOC-01 property
 * bytes into a real media_receiver.
 *
 * The mirror of direction A, and independent of it: here the BYTES are the
 * known-good input and the product's DECODER is what is measured. A real
 * draft-16 or draft-18 session is established through SimPair; the peer session
 * publishes one real MSF catalog declaring a single live LOC video track, then
 * two objects on that track whose property blocks are built entirely by the
 * scripted builder from the pure oracle -- never by a product LOC encoder, a
 * product key-value-pair codec or a product integer codec. A real
 * media_receiver, driven through its production pump hook, must surface both.
 *
 *   object 0  capture_time_us = 33      -- the shared-band control: the two
 *                                          drafts' bytes are identical, so a
 *                                          failure here would be the fixture,
 *                                          not the defect;
 *   object 1  capture_time_us = 33333   -- the drafts' bytes differ.
 *
 * WHAT THIS TEST ASSERTS is the CORRECT contract: a receiver on a draft-18
 * session must read draft-18 §1.4.1 (vi64) Key-Value-Pairs (§1.4.3), so
 * correctly encoded draft-18 property bytes must surface as
 * has_capture_time = true with the exact value, and must NOT be counted as a
 * malformed-LOC parse drop. The envelope carrying them is produced by the real
 * peer session at the same draft, so a draft-18 failure cannot be a malformed
 * envelope or a broken fixture: object 0 travels the same stream and surfaces.
 *
 * Public API only.
 */
#include <moq/media_receiver.h>
#include <moq/session.h>

#include "np_closure_runner.h"
#include "np_wire_builder.h"
#include "test_support.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

static int failures = 0;

static const uint64_t k_capture_us[2] = { 33, 33333 };
static const bool     k_independent[2] = { true, false };
static const char    *k_payload[2] = { "P0", "P1" };

static size_t build_block(np_enc_t enc, size_t i, uint8_t *out, size_t cap)
{
    np_loc01_block_t b;
    memset(&b, 0, sizeof(b));
    b.has_capture_timestamp = true;
    b.capture_timestamp = k_capture_us[i];
    b.has_video_frame_marking = true;
    b.video_frame_marking = np_pack_video_frame_marking(
        false, false, k_independent[i], false, false, 0, false, 0);
    return np_build_loc01_block(enc, &b, out, cap);
}

static void run(moq_version_t version, const char *label)
{
    np_enc_t enc = np_enc_for_version(version);
    MOQ_TEST_CHECK(enc != (np_enc_t)0);
    if (enc == (np_enc_t)0) return;

    uint8_t blocks[2][NP_LOC01_BLOCK_MAX];
    np_recv_input_t inputs[2];
    memset(inputs, 0, sizeof(inputs));
    for (size_t i = 0; i < 2; i++) {
        size_t n = build_block(enc, i, blocks[i], sizeof(blocks[i]));
        MOQ_TEST_CHECK(n > 0);
        if (n == 0) return;
        inputs[i].properties = blocks[i];
        inputs[i].properties_len = n;
        inputs[i].payload = k_payload[i];
    }

    np_receiver_result_t res;
    MOQ_TEST_CHECK(np_closure_run_receiver(version, inputs, 2, &res) == MOQ_OK);

    fprintf(stderr,
        "%s: version=%u added=%zu ready=%zu other_track_ev=%zu "
        "sub_cat=%zu sub_v=%zu sub_unexpected=%zu(%s) other_sess_ev=%zu(%u) "
        "accept=%d/%d published=%zu "
        "objects=%zu received=%llu dropped=%llu parse_drops=%llu "
        "drop_events=%zu drop_class=%d catalog_drops=%llu fatal=%d "
        "code=0x%llx setup=%s\n",
        label, (unsigned)res.version_before_first_write,
        res.track_added_count, res.catalog_ready_count,
        res.other_track_events, res.sub_catalog_count, res.sub_video_count,
        res.sub_unexpected_count, res.sub_unexpected_name,
        res.other_session_events, res.other_session_event_kind,
        (int)res.accept_catalog_rc, (int)res.accept_video_rc,
        res.published_count,
        res.object_count, (unsigned long long)res.objects_received,
        (unsigned long long)res.objects_dropped,
        (unsigned long long)res.parse_drops,
        res.parse_drop_events, (int)res.parse_drop_last_class,
        (unsigned long long)res.catalog_drops, (int)res.receiver_fatal,
        (unsigned long long)res.receiver_fatal_code,
        res.setup_ok ? "ok" : (res.setup_failure ? res.setup_failure
                                                 : "(unnamed)"));

    if (!res.setup_ok) {
        fprintf(stderr, "FAIL: %s: setup did not complete: %s\n", label,
                res.setup_failure ? res.setup_failure : "(unnamed)");
        failures++;
        return;
    }

    /* The scripted bytes really were admitted by the peer session at the
     * negotiated draft: a refusal here would mean the FIXTURE was malformed
     * for that draft, which is a different failure from a decoder defect. */
    MOQ_TEST_CHECK_EQ_INT((int)res.version_before_first_write, (int)version);
    MOQ_TEST_CHECK_EQ_INT((int)res.state_before_first_write,
                          (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK_EQ_SIZE(res.published_count, 2u);
    for (size_t i = 0; i < 2; i++)
        MOQ_TEST_CHECK_EQ_INT((int)res.publish_rc[i], (int)MOQ_OK);

    /* The peer saw EXACTLY the two subscriptions the fixture declares -- one
     * catalog request and one media request, both on namespace svc/demo -- and
     * accepted each once. A duplicate, a foreign track name, a foreign
     * namespace or any other session event is a named failure, so a
     * correct-looking run cannot hide extra traffic. */
    MOQ_TEST_CHECK_EQ_SIZE(res.sub_catalog_count, 1u);
    MOQ_TEST_CHECK_EQ_SIZE(res.sub_video_count, 1u);
    if (res.sub_unexpected_count != 0) {
        fprintf(stderr,
            "FAIL: %s: %zu unexpected subscription request(s), first named "
            "'%s'\n", label, res.sub_unexpected_count,
            res.sub_unexpected_name);
        failures++;
    }
    if (res.other_session_events != 0) {
        fprintf(stderr,
            "FAIL: %s: %zu unexpected peer session event(s), last kind %u\n",
            label, res.other_session_events, res.other_session_event_kind);
        failures++;
    }
    MOQ_TEST_CHECK_EQ_INT((int)res.accept_catalog_rc, (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)res.accept_video_rc, (int)MOQ_OK);

    /* Catalog discovery actually happened -- exactly once each -- so a missing
     * object below is a routing/parse outcome and not an undiscovered track. */
    MOQ_TEST_CHECK_EQ_SIZE(res.track_added_count, 1u);
    MOQ_TEST_CHECK_EQ_SIZE(res.catalog_ready_count, 1u);
    MOQ_TEST_CHECK_EQ_SIZE(res.other_track_events, 0u);
    MOQ_TEST_CHECK(res.track_added);
    MOQ_TEST_CHECK(res.catalog_ready);

    /* Both objects surface, in order, each with its exact payload and its
     * exact capture timestamp. An object that never surfaced is named here
     * with the bytes the draft required, so the diagnostic says WHICH object
     * and WHY it was expected -- not merely that a count was short. */
    MOQ_TEST_CHECK_EQ_SIZE(res.object_count, 2u);
    for (size_t i = res.object_count; i < 2; i++) {
        uint8_t want[NP_LOC01_BLOCK_MAX];
        size_t want_len = build_block(enc, i, want, sizeof(want));
        fprintf(stderr,
            "FAIL: %s object %zu did not surface; the peer accepted its "
            "properties and the draft's own bytes were", label, i);
        for (size_t k = 0; k < want_len; k++)
            fprintf(stderr, " %02x", want[k]);
        fprintf(stderr, " (Capture Timestamp %llu)\n",
                (unsigned long long)k_capture_us[i]);
        failures++;
    }
    for (size_t i = 0; i < 2 && i < res.object_count; i++) {
        const np_recv_object_t *o = &res.objects[i];
        size_t n = strlen(k_payload[i]);
        /* Identity is the payload: each input carries its own bytes. */
        MOQ_TEST_CHECK(o->len == n && memcmp(o->bytes, k_payload[i], n) == 0);
        /* On a RAW/LOC track the keyframe flag comes from the LOC Video Frame
         * Marking property, so this is a second reading of the same block. */
        MOQ_TEST_CHECK_EQ_INT((int)o->keyframe, (int)k_independent[i]);
        if (!o->has_capture_time) {
            fprintf(stderr,
                "FAIL: %s object %zu: has_capture_time is false; the property "
                "block carried a Capture Timestamp of %llu\n",
                label, i, (unsigned long long)k_capture_us[i]);
            failures++;
        }
        MOQ_TEST_CHECK_EQ_U64(o->capture_time_us, k_capture_us[i]);
    }

    /* Accounting: two objects in, none dropped, none parse-dropped, no
     * catalog drop, and the receiver did not go fatal. */
    MOQ_TEST_CHECK_EQ_U64(res.objects_received, 2u);
    MOQ_TEST_CHECK_EQ_U64(res.objects_dropped, 0u);
    MOQ_TEST_CHECK_EQ_U64(res.parse_drops, 0u);
    MOQ_TEST_CHECK_EQ_U64(res.catalog_drops, 0u);
    /* The per-track coalesced diagnostic must agree: no parse drop happened,
     * so no PARSE_DROP event may have been raised either. A raised event is
     * reported with its class and total. */
    if (res.parse_drop_events != 0) {
        fprintf(stderr,
            "FAIL: %s: %zu PARSE_DROP event(s), last class %d, reported "
            "total %llu; a correctly encoded property block must not be a "
            "malformed-LOC parse drop\n",
            label, res.parse_drop_events, (int)res.parse_drop_last_class,
            (unsigned long long)res.parse_drops_reported_total);
        failures++;
    }
    MOQ_TEST_CHECK(!res.receiver_fatal);
    MOQ_TEST_CHECK_EQ_U64(res.receiver_fatal_code, 0u);
    MOQ_TEST_CHECK_EQ_INT((int)res.session_state_after,
                          (int)MOQ_SESS_ESTABLISHED);
}

int main(void)
{
    int oracle = np_oracle_self_check();
    if (oracle != 0) {
        fprintf(stderr, "FAIL: oracle self-check: %d failure(s)\n", oracle);
        failures += oracle;
    }
    int builder = np_wire_builder_self_check();
    if (builder != 0) {
        fprintf(stderr, "FAIL: builder self-check: %d failure(s)\n", builder);
        failures += builder;
    }
    if (failures) return 1;

    run(MOQ_VERSION_DRAFT_16, "D16");
    run(MOQ_VERSION_DRAFT_18, "D18");

    MOQ_TEST_PASS("np_closure_receiver");
    return failures ? 1 : 0;
}
