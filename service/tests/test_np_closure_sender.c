/*
 * Negotiated-profile CLOSURE DIRECTION A: a real media_sender's LOC-01 object
 * properties, checked byte for byte against an independent oracle.
 *
 * A real draft-16 or draft-18 session is established through SimPair and the
 * production sender_hook is driven against it through the endpoint-free drive
 * seam. The negotiated draft is NEVER injected into the sender: it learns the
 * draft from the attached session, and moq_session_version() is read -- and
 * asserted -- before object 1 is ever written.
 *
 * One RAW LOC-01 video track carries two objects:
 *
 *   object 0  capture_time_us = 33      -- inside the band where the two
 *                                          drafts' integer encodings agree
 *                                          byte for byte, so it is the
 *                                          shared-band control;
 *   object 1  capture_time_us = 33333   -- outside it, so the correct bytes
 *                                          differ per draft. That is the
 *                                          whole point.
 *
 * WHAT THIS TEST ASSERTS is the CORRECT contract: the property block a session
 * puts on the wire must be encoded with the integer encoding the NEGOTIATED
 * draft specifies -- draft-16 §1.4 (i) (RFC 9000 QUIC varint) or draft-18
 * §1.4.1 (vi64) -- because draft-18 §11.2.1.2 object properties are
 * Key-Value-Pairs and §1.4.3 encodes a KVP's Delta Type and its even-Type
 * value as that draft's own integer. Draft-16 satisfies this and draft-18 does
 * not yet; the draft-18 failure is the contract gap this file reports, and it
 * is NOT normalized into an expected-error test.
 *
 * The expected bytes are built by the independent scripted builder from the
 * pure oracle. No product LOC helper, key-value-pair codec or integer codec
 * produces anything this file compares against.
 *
 * Public API only: nothing here calls a test-only function or relies on a
 * changed signature.
 */
#include <moq/media_sender.h>
#include <moq/session.h>

#include "np_closure_runner.h"
#include "np_wire_builder.h"
#include "test_support.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

static int failures = 0;

/* The two objects, declared once and shared by both drafts. */
static const np_sender_object_t k_objects[2] = {
    { 33,    true,  true,  "A0" },
    { 33333, false, false, "B1" },
};

static void dump(const char *label, const uint8_t *b, size_t n)
{
    fprintf(stderr, "  %s (%zu):", label, n);
    for (size_t i = 0; i < n; i++) fprintf(stderr, " %02x", b[i]);
    fprintf(stderr, "\n");
}

static void check_bytes(const char *what, const uint8_t *got, size_t got_len,
                        const uint8_t *want, size_t want_len)
{
    if (got_len == want_len && memcmp(got, want, want_len) == 0) return;
    fprintf(stderr, "FAIL: %s: property bytes differ\n", what);
    dump("on the wire", got, got_len);
    dump("per the draft", want, want_len);
    failures++;
}

/* Build what the negotiated draft REQUIRES for object i, independently. */
static size_t expected_block(np_enc_t enc, size_t i, uint8_t *out, size_t cap)
{
    np_loc01_block_t b;
    memset(&b, 0, sizeof(b));
    b.has_capture_timestamp = true;
    b.capture_timestamp = k_objects[i].capture_time_us;
    /* A RAW video object also carries Video Frame Marking, whose only set
     * field here is Independent, taken from the object's sync flag. */
    b.has_video_frame_marking = true;
    b.video_frame_marking = np_pack_video_frame_marking(
        false, false, k_objects[i].is_sync, false, false, 0, false, 0);
    return np_build_loc01_block(enc, &b, out, cap);
}

static void run(moq_version_t version, const char *label)
{
    np_enc_t enc = np_enc_for_version(version);
    MOQ_TEST_CHECK(enc != (np_enc_t)0);
    if (enc == (np_enc_t)0) return;

    np_sender_result_t res;
    MOQ_TEST_CHECK(np_closure_run_sender(version, k_objects, 2, &res) == MOQ_OK);

    if (!res.setup_ok) {
        fprintf(stderr, "FAIL: %s: setup did not complete: %s\n", label,
                res.setup_failure ? res.setup_failure : "(unnamed)");
        failures++;
        return;
    }

    fprintf(stderr,
        "%s: version=%u alias=%llu sends=%zu subgroup_streams=%zu props=%zu "
        "payloads=%zu unclassified=%zu other_stream=%zu count_mismatch=%zu "
        "objects_sent=%llu dropped=%llu fatal=%d code=0x%llx closed=%d "
        "last_error=%d\n",
        label, (unsigned)res.version_before_first_write,
        (unsigned long long)res.track_alias, res.send_data_records,
        res.subgroup_stream_count, res.prop_count, res.payload_count,
        res.unclassified_count, res.other_stream_records,
        res.count_mismatch_records, (unsigned long long)res.objects_sent,
        (unsigned long long)res.objects_dropped, (int)res.sender_fatal,
        (unsigned long long)res.sender_fatal_code, (int)res.sender_closed,
        (int)res.sender_last_error);

    /* The session is established and its draft fixed before object 1. */
    MOQ_TEST_CHECK_EQ_INT((int)res.version_before_first_write, (int)version);
    MOQ_TEST_CHECK_EQ_INT((int)res.state_before_first_write,
                          (int)MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(res.have_track_alias);

    /* Both writes were accepted, and nothing was lost in capture. */
    MOQ_TEST_CHECK_EQ_SIZE(res.write_count, 2u);
    for (size_t i = 0; i < res.write_count; i++)
        MOQ_TEST_CHECK_EQ_INT((int)res.write_rc[i], (int)MOQ_OK);
    MOQ_TEST_CHECK(!res.trace_overflow);

    /* EXACT capture accounting. A correct run puts exactly five client
     * SEND_DATA records on the wire: the subgroup header, then a properties
     * record and a payload record for each of the two objects. Every record's
     * trace legacy payload count must equal the bytes it carried, and no record
     * may sit on any other stream -- so a record on an undeclared or
     * undecodable stream is a named failure rather than a silent omission. */
    MOQ_TEST_CHECK_EQ_SIZE(res.send_data_records, 5u);
    MOQ_TEST_CHECK_EQ_SIZE(res.count_mismatch_records, 0u);
    MOQ_TEST_CHECK_EQ_SIZE(res.other_stream_records, 0u);

    /* Exactly one data stream, identified by its DECODED subgroup header
     * carrying the alias the peer's SUBSCRIBE_OK named -- not by position. */
    MOQ_TEST_CHECK_EQ_SIZE(res.subgroup_stream_count, 1u);
    MOQ_TEST_CHECK(res.have_subgroup);
    if (res.have_subgroup) {
        MOQ_TEST_CHECK_EQ_U64(res.subgroup.track_alias, res.track_alias);
        MOQ_TEST_CHECK(res.subgroup.has_properties);
        MOQ_TEST_CHECK(res.subgroup.has_subgroup_id);
        MOQ_TEST_CHECK(!res.subgroup.default_priority);
    }

    /* Two objects reached the wire, each as a properties send and a payload
     * send, and nothing on that stream was unclassifiable. */
    MOQ_TEST_CHECK_EQ_SIZE(res.unclassified_count, 0u);
    MOQ_TEST_CHECK_EQ_SIZE(res.prop_count, 2u);
    MOQ_TEST_CHECK_EQ_SIZE(res.payload_count, 2u);

    for (size_t i = 0; i < 2; i++) {
        uint8_t want[NP_LOC01_BLOCK_MAX];
        size_t want_len = expected_block(enc, i, want, sizeof(want));
        MOQ_TEST_CHECK(want_len > 0);
        char what[96];
        snprintf(what, sizeof(what),
                 "%s object %zu (capture_time_us=%llu)", label, i,
                 (unsigned long long)k_objects[i].capture_time_us);
        if (i >= res.prop_count) {
            /* Absence is named with the bytes the draft required, so the
             * diagnostic identifies the object and its expectation rather
             * than reporting only a short count. */
            fprintf(stderr, "FAIL: %s: no property block reached the wire\n",
                    what);
            dump("per the draft", want, want_len);
            failures++;
            continue;
        }
        MOQ_TEST_CHECK_EQ_U64(res.props[i].object_id, (uint64_t)i);
        MOQ_TEST_CHECK_EQ_U64(res.props[i].declared_len,
                              (uint64_t)res.props[i].len);
        fprintf(stderr, "  %s:", what);
        for (size_t k = 0; k < res.props[i].len; k++)
            fprintf(stderr, " %02x", res.props[i].bytes[k]);
        fprintf(stderr, "\n");
        check_bytes(what, res.props[i].bytes, res.props[i].len,
                    want, want_len);
    }

    for (size_t i = 0; i < 2 && i < res.payload_count; i++) {
        size_t n = strlen(k_objects[i].payload);
        MOQ_TEST_CHECK_EQ_U64(res.payloads[i].declared_len, (uint64_t)n);
        MOQ_TEST_CHECK(res.payloads[i].len == n &&
                       memcmp(res.payloads[i].bytes, k_objects[i].payload,
                              n) == 0);
    }

    /* Lifecycle: accepted writes are neither dropped nor abandoned, and
     * nothing unrelated tore the session or the sender down. */
    MOQ_TEST_CHECK_EQ_U64(res.objects_written, 2u);
    MOQ_TEST_CHECK_EQ_U64(res.objects_sent, 2u);
    MOQ_TEST_CHECK_EQ_U64(res.objects_dropped, 0u);
    MOQ_TEST_CHECK_EQ_U64(res.groups_dropped, 0u);
    MOQ_TEST_CHECK_EQ_U64(res.groups_abandoned, 0u);
    MOQ_TEST_CHECK_EQ_INT((int)res.sender_last_error, (int)MOQ_OK);
    MOQ_TEST_CHECK(!res.sender_fatal);
    MOQ_TEST_CHECK_EQ_U64(res.sender_fatal_code, 0u);
    /* moq_media_sender_is_closed() is defined as moq_endpoint_is_closed(s->ep),
     * and this drive seam has NO endpoint, so it is structurally true in both
     * drafts and reports nothing about teardown here. It is pinned at that
     * constant deliberately -- asserting it false would pin a value the seam
     * cannot produce -- while the real liveness facts are the nonfatal check
     * above and the established session state below. */
    MOQ_TEST_CHECK(res.sender_closed);
    MOQ_TEST_CHECK_EQ_INT((int)res.session_state_after,
                          (int)MOQ_SESS_ESTABLISHED);
}

int main(void)
{
    /* The oracle and the builder answer for themselves before anything is
     * judged against them. */
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

    MOQ_TEST_PASS("np_closure_sender");
    return failures ? 1 : 0;
}
