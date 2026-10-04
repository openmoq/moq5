/*
 * Negotiated-profile substrate (Step 5): the ENDPOINT-OFFER half of the
 * future-draft drift gate.
 *
 * The declared transport rows carry three INDEPENDENT facts -- profile
 * availability, whether the endpoint offers the version, and its AUTO rank.
 * The core half of the gate (tests/unit/test_negotiated_profile.c) compares
 * availability and the ALPN table; only the service tier knows the endpoint's
 * supported set and the exact AUTO offer, so that comparison lives here.
 *
 * Pure resolution: no network, no endpoint is connected.
 */
#include "endpoint_internal.h"
#include "np_tables.h"
#include "test_support.h"

#include <stdio.h>
#include <string.h>

static int failures = 0;

int main(void)
{
    /* the declared tables must be structurally sound before they are used
     * as an expectation */
    MOQ_TEST_CHECK_EQ_INT(np_tables_validate(), 0);

    /*
     * -- endpoint SUPPORT is compared to transport AVAILABILITY -----------
     *
     * `moq_endpoint_version_supported()` answers "can this build serve that
     * version at all", which is the availability question. It is deliberately
     * NOT compared to `endpoint_offered`: the signed model keeps availability,
     * offering and AUTO rank independent, and an available version that the
     * endpoint chooses not to offer must not read as unsupported.
     */
    for (unsigned v = 0; v <= 255; v++) {
        moq_version_t ver = (moq_version_t)v;
        const np_transport_row_t *row = np_transport_by_draft(v);
        bool product  = moq_endpoint_version_supported(ver);
        bool declared = row && row->state == NP_T_AVAILABLE;
        if (product != declared) {
            fprintf(stderr, "FAIL: availability drift: draft %u product=%d "
                            "declared_available=%d\n",
                    v, (int)product, (int)declared);
            failures++;
        }
    }

    /* -- the exact AUTO offer: contents, ORDER and length ---------------- *
     * AUTO is compared to `endpoint_offered` PLUS `auto_rank` -- the two facts
     * that describe the offer -- not to availability. A zero-initialized
     * version offer means AUTO (the cfg contract), so the resolved list is the
     * product's AUTO list verbatim. */
    {
        moq_endpoint_cfg_t cfg;
        moq_endpoint_cfg_init(&cfg);
        cfg.url = (moq_bytes_t){ (const uint8_t *)"moqt://127.0.0.1:4443",
                                 strlen("moqt://127.0.0.1:4443") };
        moq_endpoint_resolved_t r;
        memset(&r, 0, sizeof(r));
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&cfg, &r),
                              (int)MOQ_OK);

        /* the declared AUTO list, built from the rank column */
        size_t nt = 0;
        const np_transport_row_t *rows = np_transports(&nt);
        size_t ranked = 0;
        for (size_t i = 0; i < nt; i++)
            if (rows[i].auto_rank != NP_AUTO_RANK_NONE) {
                ranked++;
                /* only an OFFERED row may carry a rank */
                MOQ_TEST_CHECK(rows[i].endpoint_offered);
            }

        MOQ_TEST_CHECK_EQ_SIZE(r.version_count, ranked);
        for (uint8_t rank = 1; rank <= (uint8_t)ranked; rank++) {
            const np_transport_row_t *want = NULL;
            for (size_t i = 0; i < nt; i++)
                if (rows[i].auto_rank == rank) want = &rows[i];
            MOQ_TEST_CHECK(want != NULL);
            if (!want || rank > r.version_count) continue;
            /* rank 1 is the FIRST offered version */
            MOQ_TEST_CHECK_EQ_U64((uint64_t)r.versions[rank - 1],
                                  (uint64_t)want->draft);
        }
        /* nothing beyond the declared ranks is offered */
        for (size_t i = ranked; i < MOQ_ENDPOINT_MAX_VERSIONS; i++)
            if (i < r.version_count) {
                fprintf(stderr, "FAIL: AUTO offers an undeclared version %u at "
                                "index %zu\n", (unsigned)r.versions[i], i);
                failures++;
            }
    }

    /*
     * -- the independence itself, as a synthetic declaration --------------
     *
     * An AVAILABLE transport may be omitted from AUTO without becoming
     * unsupported: availability and offering are separate columns. The probe
     * builds that declaration and asserts the availability comparison above
     * still holds for it, while the AUTO list legitimately shrinks. Today's
     * D16/D18 rows are untouched.
     */
    {
        np_topology_t d = np_declared_topology();
        np_transport_row_t t[4];
        np_media_row_t     m[4];
        np_cell_row_t      c[16];
        np_pair_row_t      p[8];
        memcpy(t, d.transports, d.n_transports * sizeof(t[0]));
        memcpy(m, d.medias,     d.n_medias     * sizeof(m[0]));
        memcpy(c, d.cells,      d.n_cells      * sizeof(c[0]));
        memcpy(p, d.pairs,      d.n_pairs      * sizeof(p[0]));

        /* find D16 and take it out of the offer entirely: still AVAILABLE,
         * still a supported cell, simply not advertised. */
        for (size_t i = 0; i < d.n_transports; i++)
            if (t[i].draft == 16) {
                t[i].endpoint_offered = false;
                t[i].auto_rank = NP_AUTO_RANK_NONE;
            }
        /* the remaining ranks (D21 = 1, D18 = 2) stay contiguous */

        np_topology_t v;
        v.transports = t; v.n_transports = d.n_transports;
        v.medias     = m; v.n_medias     = d.n_medias;
        v.cells      = c; v.n_cells      = d.n_cells;
        v.pairs      = p; v.n_pairs      = d.n_pairs;

        /* the declaration is VALID: not offering an available version is not
         * an inconsistency */
        MOQ_TEST_CHECK_EQ_INT(np_topology_validate(&v, true), 0);
        /* and D16 is still AVAILABLE, so the product's support answer for it
         * is unchanged -- availability was never a function of the offer */
        MOQ_TEST_CHECK(moq_endpoint_version_supported(MOQ_VERSION_DRAFT_16));
        for (size_t i = 0; i < v.n_transports; i++)
            if (t[i].draft == 16) {
                MOQ_TEST_CHECK(t[i].state == NP_T_AVAILABLE);
                MOQ_TEST_CHECK(!t[i].endpoint_offered);
            }
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
