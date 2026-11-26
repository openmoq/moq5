#include "media_sender_internal.h"
#include "media_sender_locmaf_internal.h"

#ifdef MOQ_SERVICE_HAVE_LOCMAF
#include <moq/locmaf.h>
#endif

#include <stdalign.h>

/* At the start of every wire buffer; the payload follows at
 * LOCMAF_WIRE_PREFIX, so it is aligned for any object. */
typedef struct locmaf_wire_owner {
    moq_alloc_t alloc;        /* a copy of the sender's allocator */
    size_t      block_size;   /* the size the block was allocated with */
} locmaf_wire_owner_t;

#define LOCMAF_WIRE_PREFIX \
    ((sizeof(locmaf_wire_owner_t) + alignof(max_align_t) - 1u) & \
     ~(size_t)(alignof(max_align_t) - 1u))

/* The release of a wrapped wire buffer: the owner is read from the block
 * before it is freed. The sender is never reached, so the last reference
 * may go long after it was destroyed; the allocator's context outlives
 * every buffer, as the rcbuf contract requires. */
static void locmaf_wire_release(void *ctx, const uint8_t *data, size_t len)
{
    (void)data;
    (void)len;
    locmaf_wire_owner_t own;
    memcpy(&own, ctx, sizeof(own));
    own.alloc.free(ctx, own.block_size, own.alloc.ctx);
}

void sender_lm_wire_free(void *block)
{
    if (block) locmaf_wire_release(block, NULL, 0);
}

#ifdef MOQ_SERVICE_HAVE_LOCMAF

/* One track's encoder over its own storage. */
typedef struct sender_lm {
    moq_locmaf_encoder_t enc;
    void                *storage;
    size_t               storage_size;
} sender_lm_t;

moq_result_t sender_lm_prepare(moq_media_sender_t *s, moq_media_track_t *t)
{
    moq_cmaf_init_info_t ii;
    moq_cmaf_init_info_init_sized(&ii, sizeof(ii));
    moq_locmaf_track_t facts;
    moq_locmaf_track_init(&facts);
    if (t->init_data.len == 0 ||
        moq_cmaf_parse_init_sized(t->init_data, &ii, sizeof(ii)) != MOQ_OK ||
        moq_locmaf_track_from_cmaf(&ii, &facts, sizeof(facts)) != MOQ_OK)
        return MOQ_ERR_INVAL;
    moq_locmaf_encoder_cfg_t cfg;
    moq_locmaf_encoder_cfg_init(&cfg);
    cfg.track = &facts;
    size_t bytes = 0;
    moq_result_t rc = moq_locmaf_encoder_storage_size(&cfg, &bytes);
    if (rc != MOQ_OK) return rc;
    if (bytes == 0) bytes = 1;
    sender_lm_t *lm = (sender_lm_t *)s->alloc.alloc(sizeof(*lm), s->alloc.ctx);
    void *mem = lm ? s->alloc.alloc(bytes, s->alloc.ctx) : NULL;
    if (!lm || !mem) {
        if (lm) s->alloc.free(lm, sizeof(*lm), s->alloc.ctx);
        return MOQ_ERR_NOMEM;
    }
    memset(lm, 0, sizeof(*lm));
    cfg.storage = mem;
    cfg.storage_size = bytes;
    rc = moq_locmaf_encoder_init(&lm->enc, sizeof(lm->enc), &cfg);
    if (rc != MOQ_OK) {
        s->alloc.free(mem, bytes, s->alloc.ctx);
        s->alloc.free(lm, sizeof(*lm), s->alloc.ctx);
        return rc;
    }
    lm->storage = mem;
    lm->storage_size = bytes;
    t->lm = lm;
    return MOQ_OK;
}

void sender_lm_free(moq_media_sender_t *s, moq_media_track_t *t)
{
    sender_lm_t *lm = (sender_lm_t *)t->lm;
    if (!lm) return;
    s->alloc.free(lm->storage, lm->storage_size, s->alloc.ctx);
    s->alloc.free(lm, sizeof(*lm), s->alloc.ctx);
    t->lm = NULL;
}

moq_result_t sender_lm_reserve(moq_media_sender_t *s, moq_media_track_t *t,
                               moq_bytes_t chunk, void **block,
                               size_t *block_size, size_t *bound)
{
    *block = NULL;
    *block_size = 0;
    *bound = 0;
    sender_lm_t *lm = (sender_lm_t *)t->lm;
    if (!lm) return MOQ_ERR_INVAL;
    size_t need = 0;
    moq_result_t rc = moq_locmaf_encoder_check(&lm->enc, chunk, &need, NULL);
    if (rc != MOQ_OK) return rc;
    if (need > SIZE_MAX - LOCMAF_WIRE_PREFIX) return MOQ_ERR_INVAL;
    size_t size = LOCMAF_WIRE_PREFIX + need;
    uint8_t *b = (uint8_t *)s->alloc.alloc(size, s->alloc.ctx);
    if (!b) return MOQ_ERR_NOMEM;
    locmaf_wire_owner_t own = { s->alloc, size };
    memcpy(b, &own, sizeof(own));
    *block = b;
    *block_size = size;
    *bound = need;
    return MOQ_OK;
}

moq_result_t sender_lm_emit(moq_media_sender_t *s, moq_media_track_t *t,
                            sender_preq_entry_t *e, uint64_t group,
                            uint64_t object, moq_rcbuf_t **wire)
{
    *wire = NULL;
    if (e->wire_rcbuf) {
        /* A retry: the same chunk at the same Location, unchanged. */
        *wire = e->wire_rcbuf;
        return MOQ_OK;
    }
    sender_lm_t *lm = (sender_lm_t *)t->lm;
    if (!lm || !e->wire_block) return MOQ_ERR_INVAL;
    uint8_t *payload = (uint8_t *)e->wire_block + LOCMAF_WIRE_PREFIX;
    moq_bytes_t chunk = { moq_rcbuf_data(e->payload), moq_rcbuf_len(e->payload) };
    size_t len = 0;
    /* Encoding the same chunk at the same Location again before a commit
     * writes the same bytes, so a failed wrap is retried as it is. */
    moq_result_t rc = moq_locmaf_encode_chunk(&lm->enc, group, object, chunk,
                                              payload, e->wire_bound, &len, NULL);
    if (rc != MOQ_OK) return rc == MOQ_ERR_NOMEM ? MOQ_ERR_INTERNAL : rc;
    moq_rcbuf_t *r = NULL;
    if (moq_rcbuf_wrap(&s->alloc, payload, len, locmaf_wire_release,
                       e->wire_block, &r) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    e->wire_block = NULL;    /* the buffer is the rcbuf's now */
    e->wire_rcbuf = r;
    *wire = r;
    return MOQ_OK;
}

void sender_lm_commit(moq_media_track_t *t)
{
    sender_lm_t *lm = (sender_lm_t *)t->lm;
    if (lm) moq_locmaf_encoder_commit(&lm->enc);
}

void sender_lm_reset(moq_media_track_t *t)
{
    sender_lm_t *lm = (sender_lm_t *)t->lm;
    if (lm) moq_locmaf_encoder_reset(&lm->enc);
}

#else

moq_result_t sender_lm_prepare(moq_media_sender_t *s, moq_media_track_t *t)
{
    (void)s; (void)t;
    return MOQ_ERR_UNSUPPORTED;
}

void sender_lm_free(moq_media_sender_t *s, moq_media_track_t *t)
{
    (void)s; (void)t;
}

moq_result_t sender_lm_reserve(moq_media_sender_t *s, moq_media_track_t *t,
                               moq_bytes_t chunk, void **block,
                               size_t *block_size, size_t *bound)
{
    (void)s; (void)t; (void)chunk;
    *block = NULL;
    *block_size = 0;
    *bound = 0;
    return MOQ_ERR_UNSUPPORTED;
}

moq_result_t sender_lm_emit(moq_media_sender_t *s, moq_media_track_t *t,
                            sender_preq_entry_t *e, uint64_t group,
                            uint64_t object, moq_rcbuf_t **wire)
{
    (void)s; (void)t; (void)e; (void)group; (void)object;
    *wire = NULL;
    return MOQ_ERR_UNSUPPORTED;
}

void sender_lm_commit(moq_media_track_t *t) { (void)t; }

void sender_lm_reset(moq_media_track_t *t) { (void)t; }

#endif
