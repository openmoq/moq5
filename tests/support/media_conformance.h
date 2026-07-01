#ifndef MOQ_TEST_MEDIA_CONFORMANCE_H
#define MOQ_TEST_MEDIA_CONFORMANCE_H

/*
 * Reusable media-format conformance harness (test-only).
 *
 * The "pit both sides against each other" pattern for media formats: producer
 * model <-> wire bytes <-> consumer model. A format registers a small ops table
 * and a vector of cases; the driver runs the round-trip discipline per case
 * kind and tallies failures. New formats (e.g. the MPEG-TS draft) plug in by
 * implementing the applicable ops -- no transport/session state involved (that
 * is simpair's job).
 *
 * Three case kinds cover the formats we have. Not every format has an encoder,
 * hence "conformance", not "roundtrip":
 *   CATALOG_ROUNDTRIP  JSON -> parse -> encode -> reparse -> semantic-equal
 *                      (MSF/CMSF; later m2ts catalog). Negatives: parse fails.
 *   TYPED_ROUNDTRIP    typed model -> encode -> reparse -> field-equal
 *                      (LOC). Negatives: parse of malformed bytes fails.
 *   OBJECT_VALIDATE    producer bytes -> validate -> report; check valid +
 *                      report fields (CMAF; later m2ts object).
 *
 * Counter contract: tests/unit/test_support.h's MOQ_TEST_CHECK increments a
 * bare `failures` in scope. The driver owns a local `failures` and RETURNS the
 * count; the caller does `failures += moq_test_run_media_cases(...)` so no
 * failure is lost to a separate counter. Format ops are pure predicates
 * (return bool / a result code); they do not assert.
 */

#include <moq/types.h>   /* moq_bytes_t, moq_alloc_t, moq_result_t */
#include <moq/rcbuf.h>   /* moq_rcbuf_t */

#include <stdbool.h>
#include <stddef.h>

typedef enum moq_test_media_kind {
    MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP = 1,
    MOQ_TEST_MEDIA_TYPED_ROUNDTRIP,
    MOQ_TEST_MEDIA_OBJECT_VALIDATE,
} moq_test_media_kind_t;

typedef struct moq_test_media_case {
    const char           *name;
    moq_test_media_kind_t  kind;

    /* CATALOG_ROUNDTRIP: catalog JSON. OBJECT_VALIDATE: object bytes.
     * TYPED_ROUNDTRIP negative: the malformed property bytes to reject. */
    moq_bytes_t           input;

    /* TYPED_ROUNDTRIP positive: the typed model to encode then reparse-compare
     * (e.g. a const moq_loc_headers_t *). */
    const void           *typed_in;

    /* CATALOG/TYPED: whether the (re)parse is expected to succeed.
     * OBJECT_VALIDATE: whether the validator should report the object valid. */
    bool                  expect_valid;

    /* OBJECT_VALIDATE: format-specific expected report fields, interpreted by
     * ops->object_validate (e.g. a cmaf_expect_t *). Unused by other kinds. */
    const void           *expect;
} moq_test_media_case_t;

/*
 * Per-format operations. A format only fills the ops for the kinds it supports;
 * a case whose kind needs an op this table leaves NULL is a test failure (the
 * driver flags it rather than skipping silently -- a registered case must be
 * runnable). All callbacks are pure -- they return a result/bool, never assert.
 */
typedef struct moq_test_media_format_ops {
    const char *name;

    /* CATALOG_ROUNDTRIP. catalog_parse heap-allocates an opaque model on
     * success (*model set); catalog_free releases it. catalog_equal is a
     * semantic comparison of two parsed models. */
    moq_result_t (*catalog_parse)(const moq_alloc_t *, moq_bytes_t json,
                                  void **model);
    moq_result_t (*catalog_encode)(const moq_alloc_t *, const void *model,
                                   moq_rcbuf_t **json);
    bool         (*catalog_equal)(const void *a, const void *b);
    void         (*catalog_free)(const moq_alloc_t *, void *model);

    /* TYPED_ROUNDTRIP. typed_encode serializes the typed model to bytes;
     * typed_reparse_equal parses those bytes and compares to the original
     * model (true on match). typed_parse_only parses bytes and returns the
     * result code (for malformed negatives). */
    moq_result_t (*typed_encode)(const moq_alloc_t *, const void *model,
                                 moq_rcbuf_t **bytes);
    bool         (*typed_reparse_equal)(moq_bytes_t bytes, const void *model);
    moq_result_t (*typed_parse_only)(moq_bytes_t bytes);

    /* OBJECT_VALIDATE. Runs the validator, sets *reported_valid to what the
     * validator decided, and returns whether the report's fields match the
     * case's expectations (c->expect). */
    bool         (*object_validate)(moq_bytes_t bytes,
                                    const moq_test_media_case_t *c,
                                    bool *reported_valid);
} moq_test_media_format_ops_t;

/*
 * Run every case against the format's ops, asserting via MOQ_TEST_CHECK.
 * Returns the number of failures so the caller can fold it into its own
 * `failures` counter.
 */
int moq_test_run_media_cases(const moq_test_media_format_ops_t *ops,
                             const moq_test_media_case_t *cases, size_t n);

#endif /* MOQ_TEST_MEDIA_CONFORMANCE_H */
