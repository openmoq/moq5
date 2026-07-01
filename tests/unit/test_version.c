#include <moq/moq.h>
#include "test_support.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    MOQ_TEST_CHECK(moq_version_number() == MOQ_VERSION_NUMBER);
    MOQ_TEST_CHECK(strcmp(moq_version_string(), MOQ_VERSION_STRING) == 0);

    MOQ_TEST_CHECK(strcmp(moq_strerror(MOQ_OK), "ok") == 0);
    MOQ_TEST_CHECK(strcmp(moq_strerror(MOQ_DONE), "done") == 0);
    MOQ_TEST_CHECK(strcmp(moq_strerror(MOQ_ERR_NOMEM), "out of memory") == 0);
    MOQ_TEST_CHECK(strcmp(moq_strerror(-12345), "unknown error") == 0);

    /* MOQ_DONE is positive (not an error). */
    MOQ_TEST_CHECK(MOQ_OK == 0);
    MOQ_TEST_CHECK(MOQ_DONE > 0);
    MOQ_TEST_CHECK(MOQ_ERR_NOMEM < 0);

    MOQ_TEST_PASS("test_version");
    return failures;
}
