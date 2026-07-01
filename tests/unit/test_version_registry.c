/*
 * Tests for the protocol-version registry: which versions are recognized,
 * which have an available profile, and how session creation responds.
 *
 * A version constant and its ALPN are registered independently of whether a
 * profile for that version is available in the build. moq_profile_lookup()
 * returns a profile only for available versions; moq_session_create() refuses
 * a version with no available profile. draft-16 and draft-18 both have
 * profiles; an unregistered version has none and is refused. (The
 * registered-but-unavailable path is exercised with an unregistered version.)
 */
#include <moq/moq.h>
#include "test_support.h"
#include "../../core/src/session/profile.h"

static moq_session_t *try_create(moq_version_t version, moq_result_t *rc_out)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = version;
    moq_session_t *s = NULL;
    *rc_out = moq_session_create(&cfg, 0, &s);
    return s;
}

int main(void)
{
    int failures = 0;

    /* == Profile availability via moq_profile_lookup ================== */
    MOQ_TEST_CHECK(moq_profile_lookup(MOQ_VERSION_DRAFT_16) != NULL);
    MOQ_TEST_CHECK(moq_profile_lookup(MOQ_VERSION_DRAFT_18) != NULL);
    /* Unregistered versions have no profile. */
    MOQ_TEST_CHECK(moq_profile_lookup((moq_version_t)17) == NULL);
    MOQ_TEST_CHECK(moq_profile_lookup((moq_version_t)99) == NULL);

    /* == Session creation honours profile availability ================ */
    {
        /* Default (version 0) -> draft-16, succeeds. */
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                             MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(s != NULL);
        moq_session_destroy(s);
    }
    {
        moq_result_t rc;
        moq_session_t *s = try_create(MOQ_VERSION_DRAFT_16, &rc);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK(s != NULL);
        moq_session_destroy(s);
    }
    {
        /* draft-18: profile available -> creation succeeds. */
        moq_result_t rc;
        moq_session_t *s = try_create(MOQ_VERSION_DRAFT_18, &rc);
        MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        MOQ_TEST_CHECK(s != NULL);
        moq_session_destroy(s);
    }
    {
        /* Unregistered version: no profile -> refused. */
        moq_result_t rc;
        moq_session_t *s = try_create((moq_version_t)99, &rc);
        MOQ_TEST_CHECK(rc != MOQ_OK);
        MOQ_TEST_CHECK(s == NULL);
    }

    MOQ_TEST_PASS("version_registry");
    return failures != 0;
}
