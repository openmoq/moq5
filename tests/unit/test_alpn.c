/*
 * Unit test for the internal adapter ALPN <-> version mapping helper
 * (adapters/common/moq_alpn.h). Header-only; no transport linkage.
 */
#include "moq_alpn.h"
#include "test_support.h"
#include <stdbool.h>
#include <string.h>

/*
 * The independently declared image of the table. These are FIXTURE literals:
 * nothing here is copied or generated from moq_alpn_table, so the gate below
 * compares the header against a separately written expectation rather than
 * against itself.
 */
typedef struct {
    moq_version_t version;
    const char   *alpn;      /* the reviewed NUL-terminated literal */
    size_t        len;       /* the reviewed wire length */
} alpn_expect_t;

static const alpn_expect_t EXPECT[] = {
    { MOQ_VERSION_DRAFT_16, "moqt-16", 7 },
    { MOQ_VERSION_DRAFT_18, "moqt-18", 7 },
    { MOQ_VERSION_DRAFT_21, "moqt-21", 7 },
};

#define EXPECT_N (sizeof(EXPECT) / sizeof(EXPECT[0]))
#define TABLE_N  (sizeof(moq_alpn_table) / sizeof(moq_alpn_table[0]))

/*
 * SHAPE check for a TABLE row, run before any comparison uses r->len as a
 * byte count.
 *
 * What this can and cannot prove, precisely: the table rows are source-owned,
 * NUL-terminated internal literals, so their NUL-string length is knowable and
 * is required to equal the stored wire length. That is what makes a later
 * memcmp of r->len bytes in-bounds. It does NOT prove that an arbitrary
 * non-NULL pointer addresses readable storage -- no test can -- and nothing
 * here claims it does.
 */
static bool row_shape_ok(const moq_alpn_entry_t *r)
{
    if (!r || !r->alpn) return false;
    if (r->len == 0) return false;
    return strlen(r->alpn) == (size_t)r->len;
}

/* The fixture's own literal must be equally well formed before it is used as
 * a length. */
static bool expect_shape_ok(const alpn_expect_t *e)
{
    if (!e || !e->alpn) return false;
    if (e->len == 0) return false;
    return strlen(e->alpn) == e->len;
}

/*
 * Failure-safe row comparison. Both operands are shape-validated first, so
 * the memcmp below reads r->len bytes from a literal proven to hold at least
 * that many, and e->len bytes from a fixture literal proven the same way.
 */
static bool row_matches(const moq_alpn_entry_t *r, const alpn_expect_t *e)
{
    if (!row_shape_ok(r) || !expect_shape_ok(e)) return false;
    if ((size_t)r->len != e->len) return false;
    return memcmp(r->alpn, e->alpn, e->len) == 0 &&
           r->version == e->version;
}

/*
 * STRUCTURAL VALIDATOR -- runs before any production lookup.
 *
 * The gate fails CLOSED: if the table is not structurally sound, no mapping
 * test may run against it. A malformed row (one whose declared length outruns
 * its literal) cannot be defended against inside the lookup, which trusts its
 * own source-owned table by design -- so the harness must refuse to invoke it,
 * exactly as our other harnesses reject an invalid descriptor before the
 * callback runs.
 *
 * Returns the number of structural problems; zero means the mapping tests may
 * proceed.
 *
 * The eliminated bug class is a SOURCE-SHAPE property: there is one exact
 * table and no version-specific branch outside it, which is what makes a
 * one-direction-only registration impossible. That is a claim about the
 * reviewed source, not about exhausting arbitrary ALPN strings at run time,
 * and it is not claimed to be.
 */
static int table_problems(void)
{
    int failures = 0;

    /* 1. exact cardinality */
    MOQ_TEST_CHECK(TABLE_N == 3);
    MOQ_TEST_CHECK(TABLE_N == EXPECT_N);

    /* 5. every table row is well formed -- non-NULL, nonzero length, and a
     *    NUL-string length equal to the stored wire length. This is the check
     *    every byte comparison depends on, so it runs FIRST. */
    for (size_t i = 0; i < TABLE_N; i++) {
        MOQ_TEST_CHECK(moq_alpn_table[i].alpn != NULL);
        MOQ_TEST_CHECK(moq_alpn_table[i].len > 0);
        MOQ_TEST_CHECK(row_shape_ok(&moq_alpn_table[i]));
    }

    /* 2. each declared row occurs EXACTLY once */
    for (size_t e = 0; e < EXPECT_N; e++) {
        size_t hits = 0;
        for (size_t i = 0; i < TABLE_N; i++)
            if (row_matches(&moq_alpn_table[i], &EXPECT[e])) hits++;
        MOQ_TEST_CHECK(hits == 1);
    }

    /* 3. every table row is DECLARED -- an extra row fails even if every
     *    mapping still works */
    for (size_t i = 0; i < TABLE_N; i++) {
        size_t hits = 0;
        for (size_t e = 0; e < EXPECT_N; e++)
            if (row_matches(&moq_alpn_table[i], &EXPECT[e])) hits++;
        MOQ_TEST_CHECK(hits == 1);
    }

    /* 4. no duplicate version and no duplicate (length, bytes) ALPN */
    for (size_t i = 0; i < TABLE_N; i++) {
        for (size_t j = i + 1; j < TABLE_N; j++) {
            MOQ_TEST_CHECK(moq_alpn_table[i].version !=
                           moq_alpn_table[j].version);
            /* gated on BOTH rows' validated shape, independently of whether
             * the loop above already ran: a malformed row is reported as
             * malformed, never compared byte-wise */
            bool both_ok = row_shape_ok(&moq_alpn_table[i]) &&
                           row_shape_ok(&moq_alpn_table[j]);
            MOQ_TEST_CHECK(both_ok);
            bool same_alpn =
                both_ok &&
                moq_alpn_table[i].len == moq_alpn_table[j].len &&
                memcmp(moq_alpn_table[i].alpn, moq_alpn_table[j].alpn,
                       moq_alpn_table[i].len) == 0;
            MOQ_TEST_CHECK(!same_alpn);
        }
    }

    return failures;
}

int main(void)
{
    int failures = 0;
    int problems = table_problems();

    /* FAIL CLOSED: an unsound table never reaches a production lookup. */
    if (problems != 0) {
        fprintf(stderr,
                "FAIL: alpn table is structurally invalid (%d problems); "
                "no mapping test was run\n", problems);
        return problems;
    }

    /* 6. both directions answer with the INDEPENDENTLY DECLARED value, and
     *    the forward result is the reviewed NUL-terminated literal */
    for (size_t e = 0; e < EXPECT_N; e++) {
        moq_version_t v = (moq_version_t)0xBEEF;
        const char *s;

        MOQ_TEST_CHECK(moq_alpn_to_version(EXPECT[e].alpn, EXPECT[e].len, &v)
                       == true);
        MOQ_TEST_CHECK(v == EXPECT[e].version);

        s = moq_alpn_for_version(EXPECT[e].version);
        MOQ_TEST_CHECK(s != NULL);
        /* NUL-terminated string equality, not just the first len bytes */
        MOQ_TEST_CHECK(s && strlen(s) == EXPECT[e].len);
        MOQ_TEST_CHECK(s && strcmp(s, EXPECT[e].alpn) == 0);
    }

    /* The inverse direction matches on the EXPLICIT LENGTH, not on the
     * terminator: a longer buffer whose first len bytes are the ALPN still
     * matches, and the same bytes with a longer declared length do not. */
    {
        moq_version_t v = (moq_version_t)0xBEEF;
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-16-and-more", 7, &v) == true);
        MOQ_TEST_CHECK(v == MOQ_VERSION_DRAFT_16);

        v = (moq_version_t)0xBEEF;
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-16", 8, &v) == false);
        MOQ_TEST_CHECK(v == (moq_version_t)0xBEEF);
    }

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

    /* moqt-21 maps to DRAFT_21, and neighbouring drafts that this build does
     * not register stay unknown (a wrong guess here would negotiate a version
     * with no profile). */
    {
        moq_version_t v = (moq_version_t)0;
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-21", 7, &v) == true);
        MOQ_TEST_CHECK(v == MOQ_VERSION_DRAFT_21);

        v = (moq_version_t)0xDEAD;
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-19", 7, &v) == false);
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-20", 7, &v) == false);
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-22", 7, &v) == false);
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-210", 8, &v) == false);
        MOQ_TEST_CHECK(moq_alpn_to_version("moqt-2", 6, &v) == false);
        MOQ_TEST_CHECK(v == (moq_version_t)0xDEAD);
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

        const char *s21 = moq_alpn_for_version(MOQ_VERSION_DRAFT_21);
        MOQ_TEST_CHECK(s21 != NULL);
        MOQ_TEST_CHECK(s21 && strcmp(s21, "moqt-21") == 0);

        /* Unregistered versions return NULL. */
        MOQ_TEST_CHECK(moq_alpn_for_version((moq_version_t)17) == NULL);
        MOQ_TEST_CHECK(moq_alpn_for_version((moq_version_t)19) == NULL);
        MOQ_TEST_CHECK(moq_alpn_for_version((moq_version_t)20) == NULL);
        MOQ_TEST_CHECK(moq_alpn_for_version((moq_version_t)0) == NULL);
    }

    MOQ_TEST_PASS("test_alpn");
    return failures;
}
