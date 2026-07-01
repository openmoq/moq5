/*
 * Media-format conformance driver. See media_conformance.h.
 *
 * Owns a local `failures` (so MOQ_TEST_CHECK compiles in this separately
 * compiled TU) and returns the count; the caller folds it into its own
 * counter. The round-trip sequencing lives here; format-specific knowledge is
 * confined to the pure-predicate ops the caller supplies.
 */
#include "media_conformance.h"

#include <moq/types.h>

#include "../unit/test_support.h"

static int run_catalog_case(const moq_test_media_format_ops_t *ops,
                            const moq_test_media_case_t *c)
{
    int failures = 0;
    const moq_alloc_t *al = moq_alloc_default();

    void *m1 = NULL;
    moq_result_t pr = ops->catalog_parse(al, c->input, &m1);

    if (!c->expect_valid) {
        MOQ_TEST_CHECK(pr != MOQ_OK);   /* malformed input must be rejected */
        if (m1) ops->catalog_free(al, m1);
        return failures;
    }

    MOQ_TEST_CHECK(pr == MOQ_OK && m1 != NULL);
    if (pr != MOQ_OK || !m1) { if (m1) ops->catalog_free(al, m1); return failures; }

    moq_rcbuf_t *enc = NULL;
    moq_result_t er = ops->catalog_encode(al, m1, &enc);
    MOQ_TEST_CHECK(er == MOQ_OK && enc != NULL);
    if (er == MOQ_OK && enc) {
        moq_bytes_t eb = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
        void *m2 = NULL;
        moq_result_t pr2 = ops->catalog_parse(al, eb, &m2);
        MOQ_TEST_CHECK(pr2 == MOQ_OK && m2 != NULL);
        if (pr2 == MOQ_OK && m2) {
            MOQ_TEST_CHECK(ops->catalog_equal(m1, m2));
            ops->catalog_free(al, m2);
        }
        moq_rcbuf_decref(enc);
    }
    ops->catalog_free(al, m1);
    return failures;
}

static int run_typed_case(const moq_test_media_format_ops_t *ops,
                          const moq_test_media_case_t *c)
{
    int failures = 0;
    const moq_alloc_t *al = moq_alloc_default();

    if (!c->expect_valid) {
        MOQ_TEST_CHECK(ops->typed_parse_only(c->input) != MOQ_OK);
        return failures;
    }

    moq_rcbuf_t *enc = NULL;
    moq_result_t er = ops->typed_encode(al, c->typed_in, &enc);
    MOQ_TEST_CHECK(er == MOQ_OK && enc != NULL);
    if (er == MOQ_OK && enc) {
        moq_bytes_t eb = { moq_rcbuf_data(enc), moq_rcbuf_len(enc) };
        MOQ_TEST_CHECK(ops->typed_reparse_equal(eb, c->typed_in));
        moq_rcbuf_decref(enc);
    }
    return failures;
}

static int run_object_case(const moq_test_media_format_ops_t *ops,
                           const moq_test_media_case_t *c)
{
    int failures = 0;
    bool reported_valid = false;
    bool fields_ok = ops->object_validate(c->input, c, &reported_valid);
    MOQ_TEST_CHECK(reported_valid == c->expect_valid);
    MOQ_TEST_CHECK(fields_ok);
    return failures;
}

int moq_test_run_media_cases(const moq_test_media_format_ops_t *ops,
                             const moq_test_media_case_t *cases, size_t n)
{
    int failures = 0;
    if (!ops || !cases) { MOQ_TEST_CHECK(ops && cases); return failures; }

    for (size_t i = 0; i < n; i++) {
        const moq_test_media_case_t *c = &cases[i];
        switch (c->kind) {
        case MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP:
            if (ops->catalog_parse && ops->catalog_encode &&
                ops->catalog_equal && ops->catalog_free)
                failures += run_catalog_case(ops, c);
            else
                MOQ_TEST_CHECK(!"catalog ops missing for catalog case");
            break;
        case MOQ_TEST_MEDIA_TYPED_ROUNDTRIP:
            if (ops->typed_encode && ops->typed_reparse_equal &&
                ops->typed_parse_only)
                failures += run_typed_case(ops, c);
            else
                MOQ_TEST_CHECK(!"typed ops missing for typed case");
            break;
        case MOQ_TEST_MEDIA_OBJECT_VALIDATE:
            if (ops->object_validate)
                failures += run_object_case(ops, c);
            else
                MOQ_TEST_CHECK(!"object_validate op missing for object case");
            break;
        default:
            MOQ_TEST_CHECK(!"unknown media case kind");
            break;
        }
    }
    return failures;
}
