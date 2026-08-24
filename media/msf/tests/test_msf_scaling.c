/* Structural proof that moq_msf_catalog_apply_delta_ex resolves track
 * identities in O(n), not O(n^2), for a large valid delta (0317 item 1 /
 * test 5). Not a wall-clock benchmark: it counts identity compares through a
 * test-only counter that exists only when msf.c is compiled with
 * MOQ_MSF_TESTING (this target recompiles msf.c with that macro; the shipped
 * library never defines it, so the counter is absent there).
 *
 * The indexed apply does a small constant number of compares per op; the old
 * per-op linear scan did O(n) per op, i.e. O(n^2) overall. With N unique adds
 * the indexed path stays well under a linear bound that a quadratic
 * implementation blows straight through -- the RED mutant (revert an apply
 * lookup to a linear scan over eff[0..n)) drives the counter past it. */
#include <moq/msf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern size_t moq_msf_test_identity_compares;   /* msf.c, under MOQ_MSF_TESTING */

static int failures = 0;
#define CHECK(e) do { if (!(e)) { \
    printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #e); failures++; } } while (0)

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();

    /* N unique ADD ops onto an empty base. Names are "t0".."t{N-1}". */
    enum { N = 3000 };
    moq_msf_track_t *at = calloc(N, sizeof(*at));
    char (*names)[16] = calloc(N, sizeof(*names));
    moq_msf_delta_op_t *ops = calloc(N, sizeof(*ops));
    CHECK(at && names && ops);
    if (!at || !names || !ops) return 1;

    for (int i = 0; i < N; i++) {
        snprintf(names[i], sizeof(names[i]), "t%d", i);
        at[i].struct_size = sizeof(at[i]);
        at[i].name = (moq_bytes_t){ (const uint8_t *)names[i], strlen(names[i]) };
        at[i].packaging = (moq_bytes_t){ (const uint8_t *)"cmaf", 4 };
        at[i].is_live = true;
        ops[i].op = MOQ_MSF_DELTA_OP_ADD;
        ops[i].tracks = &at[i];
        ops[i].track_count = 1;
    }

    moq_msf_catalog_t base;
    memset(&base, 0, sizeof(base));
    base.struct_size = sizeof(base);
    base.version = MOQ_MSF_VERSION;         /* empty independent base */

    moq_msf_catalog_t delta;
    memset(&delta, 0, sizeof(delta));
    delta.struct_size = sizeof(delta);
    delta.delta_update = ops;
    delta.delta_update_count = N;

    moq_msf_catalog_t out;
    memset(&out, 0, sizeof(out));

    moq_msf_test_identity_compares = 0;
    moq_result_t rc = moq_msf_catalog_apply_delta(alloc, &base, &delta, &out);
    size_t compares = moq_msf_test_identity_compares;

    CHECK(rc == MOQ_OK);
    CHECK(out.track_count == (size_t)N);

    /* Indexed lookup: O(n) compares. Bound generously at 16*N -- an O(n^2)
     * scan would be ~N*N/2 = 4.5M for N=3000, an order of magnitude past this.
     * The RED mutant (linear apply lookup) exceeds it. */
    printf("apply_delta compares for N=%d adds: %zu (bound %d)\n",
           N, compares, 16 * N);
    CHECK(compares < (size_t)(16 * N));

    moq_msf_catalog_cleanup(alloc, &out);

    /* validate_identities: N unique initDataList ids must also be O(n) via the
     * byte-span index, not an O(n^2) nested scan (the array is peer-controlled
     * with no numeric cap). Same compare counter (bs_index_add_dup increments
     * it); the RED mutant is the nested-loop reintroduction. */
    {
        moq_msf_track_t one;
        memset(&one, 0, sizeof(one));
        one.struct_size = sizeof(one);
        one.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        one.packaging = (moq_bytes_t){ (const uint8_t *)"cmaf", 4 };
        one.is_live = true;

        char (*inames)[16] = calloc(N, sizeof(*inames));
        moq_msf_init_data_entry_t *ids = calloc(N, sizeof(*ids));
        CHECK(inames && ids);
        if (inames && ids) {
            for (int i = 0; i < N; i++) {
                snprintf(inames[i], sizeof(inames[i]), "i%d", i);
                ids[i].id = (moq_bytes_t){ (const uint8_t *)inames[i], strlen(inames[i]) };
                ids[i].type = (moq_bytes_t){ (const uint8_t *)"inline", 6 };
                ids[i].data = (moq_bytes_t){ (const uint8_t *)"AA", 2 };
            }
            moq_msf_catalog_t vc;
            memset(&vc, 0, sizeof(vc));
            vc.struct_size = sizeof(vc); vc.version = MOQ_MSF_VERSION;
            vc.tracks = &one; vc.track_count = 1;
            vc.init_data_list = ids; vc.init_data_count = N;

            moq_msf_test_identity_compares = 0;
            moq_result_t vrc = moq_msf_catalog_validate_identities(alloc, &vc);
            size_t vcmp = moq_msf_test_identity_compares;
            CHECK(vrc == MOQ_OK);
            printf("validate compares for N=%d init ids: %zu (bound %d)\n",
                   N, vcmp, 16 * N);
            CHECK(vcmp < (size_t)(16 * N));
        }
        free(inames); free(ids);
    }

    free(at); free(names); free(ops);
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
