#include <moq/moq.h>
#include "test_support.h"

int main(void)
{
    int failures = 0;

    /* -- Basic pack/unpack ----------------------------------------- */

    /* Use odd generation (live). */
    uint64_t h = moq_handle_pack(
        MOQ_HANDLE_POOL_SUBSCRIPTION,
        0xABCD,
        0x12345,  /* odd = live */
        0x678
    );

    MOQ_TEST_CHECK(h != 0);
    MOQ_TEST_CHECK(moq_handle_pool_tag(h) == MOQ_HANDLE_POOL_SUBSCRIPTION);
    MOQ_TEST_CHECK(moq_handle_session_tag(h) == 0xABCD);
    MOQ_TEST_CHECK(moq_handle_generation(h) == 0x12345);
    MOQ_TEST_CHECK(moq_handle_slot(h) == 0x678);

    /* -- Invalid sentinels ----------------------------------------- */

    MOQ_TEST_CHECK(!moq_subscription_is_valid(MOQ_SUBSCRIPTION_INVALID));
    MOQ_TEST_CHECK(!moq_publication_is_valid(MOQ_PUBLICATION_INVALID));
    MOQ_TEST_CHECK(!moq_fetch_is_valid(MOQ_FETCH_INVALID));
    MOQ_TEST_CHECK(!moq_announcement_is_valid(MOQ_ANNOUNCEMENT_INVALID));

    /* -- Structurally valid handle --------------------------------- */

    moq_subscription_t sub_valid;
    sub_valid._opaque = h;
    MOQ_TEST_CHECK(moq_subscription_is_valid(sub_valid));

    /* -- Wrong pool tag: publication handle is NOT a valid subscription */

    uint64_t h_pub = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION, 0xABCD, 0x12345, 0x678);
    MOQ_TEST_CHECK(h_pub != 0);
    moq_subscription_t wrong_kind;
    wrong_kind._opaque = h_pub;
    MOQ_TEST_CHECK(!moq_subscription_is_valid(wrong_kind));

    moq_publication_t pub_valid;
    pub_valid._opaque = h_pub;
    MOQ_TEST_CHECK(moq_publication_is_valid(pub_valid));

    /* -- Even generation (free slot) is invalid -------------------- */
    /* moq_handle_pack rejects even generation, so construct manually. */

    {
        /* Manually build: pool=1, session=0xABCD, gen=0x12346(even), slot=0x678 */
        uint64_t h_even = ((uint64_t)1u << 60)
                        | ((uint64_t)0xABCDu << 44)
                        | ((uint64_t)0x12346u << 24)
                        | (uint64_t)0x678u;
        moq_subscription_t even_gen;
        even_gen._opaque = h_even;
        MOQ_TEST_CHECK(!moq_subscription_is_valid(even_gen));
    }

    /* -- Zero session tag is invalid ------------------------------- */
    /* moq_handle_pack rejects zero session_tag, so construct manually. */

    {
        uint64_t h_zero_sess = ((uint64_t)1u << 60)
                             | ((uint64_t)0u << 44)
                             | ((uint64_t)0x12345u << 24)
                             | (uint64_t)0x678u;
        moq_subscription_t zero_sess;
        zero_sess._opaque = h_zero_sess;
        MOQ_TEST_CHECK(!moq_subscription_is_valid(zero_sess));
    }

    /* -- moq_handle_pack rejects semantic invalids ----------------- */

    MOQ_TEST_CHECK(moq_handle_pack(0, 0xABCD, 1, 0) == 0);        /* pool_tag 0 */
    MOQ_TEST_CHECK(moq_handle_pack(1, 0, 1, 0) == 0);              /* session_tag 0 */
    MOQ_TEST_CHECK(moq_handle_pack(1, 0xABCD, 2, 0) == 0);         /* even generation */
    MOQ_TEST_CHECK(moq_handle_pack(1, 0xABCD, 0, 0) == 0);         /* generation 0 (even) */

    /* -- Equality -------------------------------------------------- */

    moq_subscription_t sub_same;
    sub_same._opaque = h;
    MOQ_TEST_CHECK(moq_subscription_eq(sub_valid, sub_same));
    MOQ_TEST_CHECK(!moq_subscription_eq(sub_valid, MOQ_SUBSCRIPTION_INVALID));

    /* -- Hash ------------------------------------------------------ */

    MOQ_TEST_CHECK(moq_subscription_hash(sub_valid) == moq_subscription_hash(sub_same));
    MOQ_TEST_CHECK(moq_subscription_hash(sub_valid) != 0);

    /* -- Trace ID -------------------------------------------------- */

    MOQ_TEST_CHECK(moq_subscription_id_for_trace(sub_valid) == h);

    /* -- Max field values pack correctly --------------------------- */

    uint64_t h_max = moq_handle_pack(0xF, 0xFFFF, 0xFFFFFFF, 0xFFFF);
    MOQ_TEST_CHECK(h_max != 0);
    MOQ_TEST_CHECK(moq_handle_pool_tag(h_max) == 0xF);
    MOQ_TEST_CHECK(moq_handle_session_tag(h_max) == 0xFFFF);
    MOQ_TEST_CHECK(moq_handle_generation(h_max) == 0xFFFFFFF);
    MOQ_TEST_CHECK(moq_handle_slot(h_max) == 0xFFFF);

    /* -- Overflow: pool_tag > 15 returns 0 ------------------------- */

    MOQ_TEST_CHECK(moq_handle_pack(0x10, 0xABCD, 1, 0) == 0);
    MOQ_TEST_CHECK(moq_handle_pack(0xFF, 0xABCD, 1, 0) == 0);

    /* -- Overflow: generation > 28 bits returns 0 ------------------ */

    MOQ_TEST_CHECK(moq_handle_pack(1, 0xABCD, 0x10000000, 0) == 0);

    /* -- Overflow: slot > 16 bits returns 0 ------------------------ */

    MOQ_TEST_CHECK(moq_handle_pack(1, 0xABCD, 1, 0x10000) == 0);

    /* -- Consistent helpers for all handle types ------------------- */

    MOQ_TEST_CHECK(moq_publication_hash(pub_valid) != 0);
    MOQ_TEST_CHECK(moq_publication_id_for_trace(pub_valid) == h_pub);

    uint64_t h_fetch = moq_handle_pack(MOQ_HANDLE_POOL_FETCH, 0x1234, 1, 5);
    moq_fetch_t f;
    f._opaque = h_fetch;
    MOQ_TEST_CHECK(moq_fetch_is_valid(f));
    MOQ_TEST_CHECK(moq_fetch_hash(f) != 0);
    MOQ_TEST_CHECK(moq_fetch_id_for_trace(f) == h_fetch);
    MOQ_TEST_CHECK(moq_fetch_eq(f, f));

    uint64_t h_ann = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT, 0x5678, 3, 10);
    moq_announcement_t ann;
    ann._opaque = h_ann;
    MOQ_TEST_CHECK(moq_announcement_is_valid(ann));
    MOQ_TEST_CHECK(moq_announcement_hash(ann) != 0);
    MOQ_TEST_CHECK(moq_announcement_id_for_trace(ann) == h_ann);
    MOQ_TEST_CHECK(moq_announcement_eq(ann, ann));

    MOQ_TEST_PASS("test_handles");
    return failures;
}
