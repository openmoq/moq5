/* Test-only constructor control for an attach the provider cannot support.
 * Header-only; included by the raw adapter tests that pin the refusal. */
#ifndef MOQ_PQ_ATTACH_REFUSAL_H
#define MOQ_PQ_ATTACH_REFUSAL_H

#include <moq/moq.h>
#include <moq/picoquic.h>
#include <picoquic.h>

#include <stddef.h>

typedef struct {
    unsigned calls;   /* alloc + realloc requests made through this allocator */
} pq_refusal_alloc_t;

static inline void *pq_refusal_alloc(size_t n, void *ctx)
{
    ((pq_refusal_alloc_t *)ctx)->calls++;
    return moq_alloc_default()->alloc(n, moq_alloc_default()->ctx);
}
static inline void *pq_refusal_realloc(void *p, size_t old, size_t n, void *ctx)
{
    ((pq_refusal_alloc_t *)ctx)->calls++;
    return moq_alloc_default()->realloc(p, old, n, moq_alloc_default()->ctx);
}
static inline void pq_refusal_free(void *p, size_t n, void *ctx)
{
    (void)ctx;
    moq_alloc_default()->free(p, n, moq_alloc_default()->ctx);
}

/* Calls the public constructor directly on a connection prepared but not
 * started. `refused` is a session the provider cannot carry; `accepted` is a
 * session of a supported version created alongside it. The documented
 * refusal is -1 with a stale *out cleared, nothing requested from the
 * config's allocator and the connection's callback left with its owner; the
 * same connection and allocator then accept `accepted`, so the refusal is the
 * session's version and not any other input. Returns NULL when all of that
 * holds, otherwise the first property that did not. */
static inline const char *pq_check_constructor_refusal(picoquic_cnx_t *cnx,
                                                       moq_session_t *refused,
                                                       moq_session_t *accepted)
{
    pq_refusal_alloc_t counts = {0};
    const moq_alloc_t a = { .alloc = pq_refusal_alloc,
                            .realloc = pq_refusal_realloc,
                            .free = pq_refusal_free, .ctx = &counts };
    picoquic_stream_data_cb_fn owner = picoquic_get_callback_function(cnx);
    void *owner_ctx = picoquic_get_callback_context(cnx);

    moq_pq_conn_cfg_t cfg;
    moq_pq_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.session = refused;
    cfg.cnx = cnx;
    cfg.alloc = &a;
    moq_pq_conn_t *out = (moq_pq_conn_t *)&counts;   /* stale caller value */
    if (moq_pq_conn_create(&cfg, &out) != -1) {
        if (out != NULL && out != (moq_pq_conn_t *)&counts)
            moq_pq_conn_destroy(out);
        return "constructor did not return -1";
    }
    if (out != NULL) return "refusal left *out set";
    if (counts.calls != 0) return "refusal allocated";
    if (picoquic_get_callback_function(cnx) != owner ||
        picoquic_get_callback_context(cnx) != owner_ctx)
        return "refusal took over the connection callback";

    cfg.session = accepted;
    if (moq_pq_conn_create(&cfg, &out) != 0 || out == NULL)
        return "the same inputs refuse a supported version";
    int took_over = picoquic_get_callback_context(cnx) == (void *)out;
    moq_pq_conn_destroy(out);
    return took_over ? NULL : "supported attach did not take the callback";
}

#endif /* MOQ_PQ_ATTACH_REFUSAL_H */
