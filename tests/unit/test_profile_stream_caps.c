/*
 * Focused white-box tests for the profile capability seam used by
 * stream-correlated request profiles and unidirectional control-channel
 * profiles:
 *   - moq_session_uses_request_streams()
 *   - moq_session_classify_peer_uni()
 *   - moq_session_validate_inbound_request_stream()
 *
 * These are version-neutral: the session core asks the bound profile for a
 * capability or dispatches to a hook, rather than testing a draft version.
 * The accessors are NULL-safe, and their defaults preserve draft-16 behavior
 * (no per-request bidi streams; every peer unidirectional stream is data).
 *
 * This slice adds only the capability flag, the hooks, and the NULL-safe
 * accessors. No profile populates them yet (draft-16 leaves them at their
 * defaults) and no wire behavior is introduced. Both the default branch (real
 * draft-16 profile) and the capability-gated branch (a fake profile) are
 * exercised.
 */
#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"

/* -- Fake stream-correlated profile hooks -------------------------- */

static moq_uni_class_t fake_classify_uni(const uint8_t *data, size_t len)
{
    if (len == 0) return MOQ_UNI_CLASS_NEED_MORE;
    switch (data[0]) {
    case 0x01: return MOQ_UNI_CLASS_CONTROL;
    case 0x02: return MOQ_UNI_CLASS_PADDING;
    case 0x03: return MOQ_UNI_CLASS_UNKNOWN;
    default:   return MOQ_UNI_CLASS_DATA;
    }
}

static int g_validate_calls;
static uint64_t g_validate_msg_type;

static moq_result_t fake_validate(moq_session_t *s, moq_stream_ref_t ref,
                                  uint64_t msg_type, uint64_t wire_request_id,
                                  moq_request_endpoint_t *out)
{
    (void)s;
    g_validate_calls++;
    g_validate_msg_type = msg_type;  /* record full width for the test */
    memset(out, 0, sizeof(*out));
    out->kind = MOQ_REQ_SUBSCRIPTION;
    out->slot = 7;
    out->has_stream_ref = true;
    out->stream_ref = ref;
    out->has_request_id = true;
    out->request_id = wire_request_id;
    return MOQ_OK;
}

int main(void)
{
    int failures = 0;

    /* == A. Real draft-16 profile: defaults preserve D16 behavior ===== */
    {
        test_alloc_state_t st = {0};
        moq_alloc_t alloc = test_allocator(&st);
        moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
        cfg.alloc = &alloc;
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        moq_session_t *s = NULL;
        moq_session_create(&cfg, 0, &s);
        MOQ_TEST_CHECK(s != NULL);

        /* Capability is false. */
        MOQ_TEST_CHECK(!moq_session_uses_request_streams(s));

        /* classify hook is NULL -> every peer uni stream is DATA, regardless
         * of leading bytes (and for empty input). */
        uint8_t b[1] = { 0x01 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_classify_peer_uni(s, b, 1),
            (int)MOQ_UNI_CLASS_DATA);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_classify_peer_uni(s, NULL, 0),
            (int)MOQ_UNI_CLASS_DATA);

        /* draft-16 uses a bidirectional control channel. */
        MOQ_TEST_CHECK(!moq_session_uses_uni_control(s));

        /* validate hook is NULL -> error + cleared endpoint. */
        moq_request_endpoint_t ep;
        memset(&ep, 0xAB, sizeof(ep));
        moq_result_t rc = moq_session_validate_inbound_request_stream(
            s, moq_stream_ref_from_u64(5), 3, 9, &ep);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT(ep.slot, -1);

        moq_session_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)st.balance, 0);
    }

    /* == B. Capability-gated branch: a fake stream-correlated profile == */
    {
        moq_profile_ops_t fake;
        memset(&fake, 0, sizeof(fake));
        fake.uses_request_streams            = true;
        fake.classify_uni_stream             = fake_classify_uni;
        fake.validate_inbound_request_stream = fake_validate;

        /* The accessors only read s->profile; a zeroed session header with the
         * profile pointer set is sufficient to exercise dispatch. */
        moq_session_t fs;
        memset(&fs, 0, sizeof(fs));
        fs.profile = &fake;

        MOQ_TEST_CHECK(moq_session_uses_request_streams(&fs));

        uint8_t cb[1] = { 0x01 }, pb[1] = { 0x02 },
                ub[1] = { 0x03 }, db[1] = { 0x09 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_classify_peer_uni(&fs, cb, 1),
            (int)MOQ_UNI_CLASS_CONTROL);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_classify_peer_uni(&fs, pb, 1),
            (int)MOQ_UNI_CLASS_PADDING);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_classify_peer_uni(&fs, ub, 1),
            (int)MOQ_UNI_CLASS_UNKNOWN);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_classify_peer_uni(&fs, db, 1),
            (int)MOQ_UNI_CLASS_DATA);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_classify_peer_uni(&fs, NULL, 0),
            (int)MOQ_UNI_CLASS_NEED_MORE);

        g_validate_calls = 0;
        g_validate_msg_type = 0;
        moq_request_endpoint_t ep;
        /* Pass a >32-bit msg_type to prove no truncation through the seam. */
        uint64_t wide_msg_type = 0x100000003ull;
        moq_result_t rc = moq_session_validate_inbound_request_stream(
            &fs, moq_stream_ref_from_u64(0x1234), wide_msg_type, 0x55, &ep);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(g_validate_calls, 1);
        MOQ_TEST_CHECK_EQ_U64(g_validate_msg_type, wide_msg_type);
        MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_SUBSCRIPTION);
        MOQ_TEST_CHECK_EQ_INT(ep.slot, 7);
        MOQ_TEST_CHECK(ep.has_stream_ref);
        MOQ_TEST_CHECK_EQ_U64(ep.stream_ref._v, 0x1234);
        MOQ_TEST_CHECK(ep.has_request_id);
        MOQ_TEST_CHECK_EQ_U64(ep.request_id, 0x55);

        /* uses_uni_control is the EXPLICIT capability, not inferred from the
         * classifier. Classifier present but capability false -> false (this
         * would break if someone reverted to classify_uni_stream != NULL). */
        MOQ_TEST_CHECK(fake.classify_uni_stream != NULL);
        MOQ_TEST_CHECK(!fake.uses_uni_control_channel);
        MOQ_TEST_CHECK(!moq_session_uses_uni_control(&fs));

        /* Capability true, even with NO classifier installed -> true. */
        fake.uses_uni_control_channel = true;
        fake.classify_uni_stream = NULL;
        MOQ_TEST_CHECK(moq_session_uses_uni_control(&fs));
    }

    MOQ_TEST_PASS("profile_stream_caps");
    return failures != 0;
}
