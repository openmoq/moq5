/*
 * Unit test for the internal adapter ALPN <-> version mapping helper
 * (adapters/common/moq_alpn.h). Header-only; no transport linkage.
 */
#include "moq_alpn.h"
#include "test_support.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    /* moqt-16 maps to DRAFT_16. */
    {
        moq_version_t v = (moq_version_t)0;
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-16", 7, &v) == true);
        MOQ_TEST_CHECK(v == MOQ_VERSION_DRAFT_16);
    }

    /* Unknown / unsupported / wrong-surface ALPNs return false and leave
     * *out untouched (never 0-as-D16). */
    /* moqt-18 maps to DRAFT_18 (a registered version; whether a session can be
     * created for it is gated separately by profile availability). */
    {
        moq_version_t v = (moq_version_t)0;
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-18", 7, &v) == true);
        MOQ_TEST_CHECK(v == MOQ_VERSION_DRAFT_18);
    }

    /* Unknown / unsupported / wrong-surface ALPNs return false and leave
     * *out untouched (never 0-as-D16). */
    {
        moq_version_t v = (moq_version_t)0xDEAD;

        MOQ_TEST_CHECK(moq_alpn_to_version("moqt", 4, &v) == false);
        MOQ_TEST_CHECK(v == (moq_version_t)0xDEAD);

        MOQ_TEST_CHECK(moq_alpn_to_version("moq-00", 6, &v) == false);
        MOQ_TEST_CHECK(v == (moq_version_t)0xDEAD);

        /* "h3" is the H3-WebTransport ALPN, not a MoQ ALPN. */
        MOQ_TEST_CHECK(moq_alpn_to_version("h3", 2, &v) == false);
        MOQ_TEST_CHECK(v == (moq_version_t)0xDEAD);

        MOQ_TEST_CHECK(moq_alpn_to_version(NULL, 0, &v) == false);
        MOQ_TEST_CHECK(moq_alpn_to_version("", 0, &v) == false);
        MOQ_TEST_CHECK(v == (moq_version_t)0xDEAD);

        /* Length guards: correct prefix but wrong length must not match. */
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-16x", 8, &v) == false);
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-1", 6, &v) == false);
    }

    /* NULL out pointer is rejected. */
    MOQ_TEST_CHECK(moq_alpn_to_version("moqt-16", 7, NULL) == false);

    /* Reverse mapping. */
    {
        const char *s = moq_alpn_for_version(MOQ_VERSION_DRAFT_16);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK(strcmp(s, "moqt-16") == 0);

        /* DRAFT_18 is a registered version: it maps to "moqt-18" regardless
         * of profile availability. */
        const char *s18 = moq_alpn_for_version(MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK(s18 != NULL);
        MOQ_TEST_CHECK(s18 && strcmp(s18, "moqt-18") == 0);

        /* Unregistered versions return NULL. */
        MOQ_TEST_CHECK(moq_alpn_for_version((moq_version_t)17) == NULL);
        MOQ_TEST_CHECK(moq_alpn_for_version((moq_version_t)0) == NULL);
    }

    MOQ_TEST_PASS("test_alpn");
    return failures;
}
