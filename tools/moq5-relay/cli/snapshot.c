#include "snapshot.h"

#include <string.h>

size_t
moqr_cli_snapshot_bytes(uint32_t lanes)
{
    return (size_t)lanes * sizeof(moqr_cli_snapshot_row_t);
}

moqr_result_t
moqr_cli_snapshot_init(moqr_cli_snapshot_t *s, uint32_t lanes,
                       const moq_alloc_t *alloc)
{
    if (s == NULL || alloc == NULL || alloc->alloc == NULL ||
        alloc->free == NULL || lanes == 0 || lanes > MOQR_SHARDS_MAX) {
        /* The lane count is bounded by the shard runtime's own cap — the
         * coordinator's fixed view assembly depends on it. */
        return MOQR_ERR_INVAL;
    }
    memset(s, 0, sizeof(*s));
    s->rows = alloc->alloc(moqr_cli_snapshot_bytes(lanes), alloc->ctx);
    if (s->rows == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(s->rows, 0, moqr_cli_snapshot_bytes(lanes));
    for (uint32_t i = 0; i < lanes; i++) {
        if (pthread_mutex_init(&s->rows[i].mu, NULL) != 0) {
            for (uint32_t j = 0; j < i; j++) {
                (void)pthread_mutex_destroy(&s->rows[j].mu);
            }
            alloc->free(s->rows, moqr_cli_snapshot_bytes(lanes), alloc->ctx);
            memset(s, 0, sizeof(*s));
            return MOQR_ERR_NOMEM;
        }
    }
    s->lanes = lanes;
    s->alloc = alloc;
    return MOQR_OK;
}

void
moqr_cli_snapshot_destroy(moqr_cli_snapshot_t *s)
{
    if (s == NULL || s->rows == NULL) {
        return;
    }
    for (uint32_t i = 0; i < s->lanes; i++) {
        (void)pthread_mutex_destroy(&s->rows[i].mu);
    }
    s->alloc->free(s->rows, moqr_cli_snapshot_bytes(s->lanes),
                   s->alloc->ctx);
    memset(s, 0, sizeof(*s));
}

void
moqr_cli_snapshot_publish(moqr_cli_snapshot_t *s, uint32_t lane,
                          const moqr_cli_snapshot_stats_t *st,
                          uint64_t epoch)
{
    if (s == NULL || s->rows == NULL || lane >= s->lanes || st == NULL) {
        return;
    }
    moqr_cli_snapshot_row_t *row = &s->rows[lane];
    pthread_mutex_lock(&row->mu);
    row->st = *st;
    row->epoch = epoch;
    pthread_mutex_unlock(&row->mu);
}

/* Bound on copy-discard retries when requests keep landing between the
 * copy and the re-read. Each retry re-targets the newest epoch, so giving
 * up simply reports that epoch incomplete — the caller retries; a stale
 * set is never handed out. */
#define SNAP_COLLECT_RETRIES 16u

moqr_result_t
moqr_cli_snapshot_collect(moqr_cli_snapshot_t *s, moqr_cli_epoch_fn requested,
                          void *rctx, moqr_cli_snapshot_stats_t *rows,
                          uint64_t *out_epoch)
{
    if (s == NULL || s->rows == NULL || requested == NULL || rows == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint64_t target = requested(rctx);
    for (uint32_t attempt = 0; attempt < SNAP_COLLECT_RETRIES; attempt++) {
        bool complete = true;
        for (uint32_t i = 0; i < s->lanes; i++) {
            moqr_cli_snapshot_row_t *row = &s->rows[i];
            pthread_mutex_lock(&row->mu);
            rows[i] = row->st;
            complete = complete && row->epoch == target;
            pthread_mutex_unlock(&row->mu);
        }
        /* Re-read the requested epoch AFTER the copy and BEFORE anything
         * could render it: a request that landed meanwhile obsoletes this
         * set — discard it and re-target, never render all-E once E+1
         * exists. */
        uint64_t now = requested(rctx);
        if (now != target) {
            target = now;
            continue;
        }
        if (out_epoch != NULL) {
            *out_epoch = target;
        }
        return complete ? MOQR_OK : MOQR_ERR_WOULD_BLOCK;
    }
    if (out_epoch != NULL) {
        *out_epoch = target;
    }
    return MOQR_ERR_WOULD_BLOCK;   /* requests kept arriving: report, retry */
}

moqr_result_t
moqr_cli_snapshot_render(moqr_cli_snapshot_t *s, moqr_cli_epoch_fn requested,
                         void *rctx, moqr_cli_snapshot_stats_t *rows,
                         moqr_cli_produce_fn produce, void *pctx,
                         uint64_t *out_epoch)
{
    if (produce == NULL) {
        return MOQR_ERR_INVAL;
    }
    for (uint32_t attempt = 0; attempt < SNAP_COLLECT_RETRIES; attempt++) {
        uint64_t epoch = 0;
        moqr_result_t rc =
            moqr_cli_snapshot_collect(s, requested, rctx, rows, &epoch);
        if (out_epoch != NULL) {
            *out_epoch = epoch;
        }
        if (rc != MOQR_OK) {
            return rc;   /* incomplete (or bad args): nothing produced */
        }
        /* A complete set carrying a refused lane snapshot never reaches the
         * producer: suppress the whole epoch rather than render zeroed
         * stand-ins as user-facing numbers. An unknown capability is treated
         * as a refusal, never as a default — a value this code does not
         * understand is not evidence that anything is fine. */
        for (uint32_t i = 0; i < s->lanes; i++) {
            if (rows[i].shard_cap == MOQR_CLI_CAP_REFUSED ||
                rows[i].shard_cap >= MOQR_CLI_CAP__COUNT) {
                return MOQR_ERR_INVAL;
            }
        }
        rc = produce(pctx, rows, epoch);
        if (rc != MOQR_OK) {
            return rc;   /* producer failed/suppressed: report verbatim */
        }
        /* The FINAL epoch read, after production and immediately before
         * the caller may emit: a request that landed while the document
         * was being built obsoletes it — discard the produced output and
         * retry against the newer epoch. Epoch E is never emitted once
         * E+1 was requested here. */
        uint64_t now = requested(rctx);
        if (now == epoch) {
            return MOQR_OK;
        }
        if (out_epoch != NULL) {
            *out_epoch = now;   /* obsoleted: report the NEWEST observed
                                 * request, even on retry exhaustion */
        }
    }
    return MOQR_ERR_WOULD_BLOCK;   /* requests kept arriving: report, retry */
}


/* -- the production coordinator ---------------------------------------------- */

/* Serialize-only producer over the copied rows: builds the views and
 * renders the multi-shard document into an allocator-backed buffer that
 * survives across the render cycle's retries. Never emits. */
typedef struct coord_doc {
    const moqr_obs_labels_t *labels;   /* [lanes] */
    uint32_t                 lanes;
    const moq_alloc_t       *alloc;
    char                    *buf;
    size_t                   buf_cap;
    size_t                   len;
} coord_doc_t;

static moqr_result_t
coord_doc_produce(void *vctx, const moqr_cli_snapshot_stats_t *rows,
                  uint64_t epoch)
{
    (void)epoch;
    coord_doc_t *d = vctx;
    moqr_snapshot_view_t views[MOQR_SHARDS_MAX];
    for (uint32_t i = 0; i < d->lanes; i++) {
        /* ABSENT means there is no cross-shard plane to report, so the
         * view carries no shard pointer and the renderer omits those
         * families entirely. Passing the zeroed struct instead would render
         * them as measured zeros. */
        const moqr_shards_stats_t *sh =
            (rows[i].shard_cap == MOQR_CLI_CAP_VALID) ? &rows[i].shard : NULL;
        views[i] = (moqr_snapshot_view_t){ .core = &rows[i].core,
                                           .bind = &rows[i].bind,
                                           .shard = sh,
                                           .labels = d->labels[i],
                                           .lane_wakes =
                                               rows[i].lane_wakes };
    }
    if (d->buf == NULL) {
        d->buf_cap = 32u * 1024u;
        d->buf = d->alloc->alloc(d->buf_cap, d->alloc->ctx);
        if (d->buf == NULL) {
            d->buf_cap = 0;
            return MOQR_ERR_NOMEM;
        }
    }
    size_t w = 0;
    moqr_result_t rc = moqr_metrics_write_prometheus_multi(
        views, d->lanes, d->buf, d->buf_cap, &w);
    if (rc == MOQR_ERR_CAPACITY) {
        /* Regrow and re-render (the writer rebuilds the whole document,
         * so nothing needs copying across). */
        d->alloc->free(d->buf, d->buf_cap, d->alloc->ctx);
        d->buf_cap = w + 1u;
        d->buf = d->alloc->alloc(d->buf_cap, d->alloc->ctx);
        if (d->buf == NULL) {
            d->buf_cap = 0;
            return MOQR_ERR_NOMEM;
        }
        rc = moqr_metrics_write_prometheus_multi(views, d->lanes, d->buf,
                                                 d->buf_cap, &w);
    }
    d->len = w;
    return rc;
}

moqr_result_t
moqr_cli_coord_dump(moqr_cli_snapshot_t *snap,
                    const moqr_obs_labels_t *labels,
                    moqr_cli_epoch_fn requested, void *rctx,
                    moqr_cli_emit_fn emit, void *ectx, uint64_t *out_epoch)
{
    if (snap == NULL || snap->rows == NULL || labels == NULL ||
        requested == NULL || emit == NULL) {
        return MOQR_ERR_INVAL;
    }
    const moq_alloc_t *a = snap->alloc;
    moqr_cli_snapshot_stats_t *rows =
        a->alloc((size_t)snap->lanes * sizeof(*rows), a->ctx);
    if (rows == NULL) {
        /* Even the earliest failure names the epoch it was serving, so the
         * caller's per-epoch de-spammed diagnostic actually fires. */
        if (out_epoch != NULL) {
            *out_epoch = requested(rctx);
        }
        return MOQR_ERR_NOMEM;
    }
    coord_doc_t d = { labels, snap->lanes, a, NULL, 0, 0 };
    uint64_t epoch = 0;
    moqr_result_t rc = moqr_cli_snapshot_render(
        snap, requested, rctx, rows, coord_doc_produce, &d, &epoch);
    if (rc == MOQR_OK) {
        emit(ectx, d.buf, d.len, epoch);
    }
    if (out_epoch != NULL) {
        *out_epoch = epoch;
    }
    if (d.buf != NULL) {
        a->free(d.buf, d.buf_cap, a->ctx);
    }
    a->free(rows, (size_t)snap->lanes * sizeof(*rows), a->ctx);
    return rc;
}
