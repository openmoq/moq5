#include "moq/rcbuf.h"
#include "rcbuf_internal.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/*
 * Internal layout: header + data in one allocation.
 * The allocator vtable is copied so decref can free correctly
 * even after the originating session is destroyed.
 */
struct moq_rcbuf {
    moq_alloc_t          alloc;        /* copied at creation */
    uint32_t             refcount;
    bool                 wrapped;      /* true for wrap(), false for create() */
    bool                 sliced;       /* true for slice() */
    size_t               len;
    const uint8_t       *external;     /* wrapped/sliced: data pointer */
    moq_rcbuf_release_fn release;      /* wrapped-only; may be NULL */
    void                *release_ctx;  /* wrapped-only */
    moq_rcbuf_t         *parent;       /* sliced-only: root parent (increffed) */
};

static uint8_t *rcbuf_data_ptr(moq_rcbuf_t *buf)
{
    return (uint8_t *)buf + sizeof(moq_rcbuf_t);
}

moq_result_t moq_rcbuf_alloc_uninit(const moq_alloc_t *alloc, size_t len,
                                    moq_rcbuf_t **out, uint8_t **data_out)
{
    if (data_out) *data_out = NULL;
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (!data_out) return MOQ_ERR_INVAL;
    if (!alloc || !alloc->alloc || !alloc->free)
        return MOQ_ERR_INVAL;

    /* Guard against size_t overflow. */
    size_t total = sizeof(moq_rcbuf_t) + len;
    if (total < len)
        return MOQ_ERR_INVAL;

    moq_rcbuf_t *buf = (moq_rcbuf_t *)alloc->alloc(total, alloc->ctx);
    if (!buf)
        return MOQ_ERR_NOMEM;

    buf->alloc       = *alloc;
    buf->refcount    = 1;
    buf->wrapped     = false;
    buf->sliced      = false;
    buf->len         = len;
    buf->external    = NULL;
    buf->release     = NULL;
    buf->release_ctx = NULL;
    buf->parent      = NULL;

    /* Payload region is left uninitialised; the caller fills it. */
    *data_out = rcbuf_data_ptr(buf);
    *out = buf;
    return MOQ_OK;
}

moq_result_t moq_rcbuf_create(const moq_alloc_t *alloc,
                               const uint8_t *data, size_t len,
                               moq_rcbuf_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;

    /* Reject non-zero length with NULL data (would leave payload uninitialized). */
    if (len > 0 && !data)
        return MOQ_ERR_INVAL;

    uint8_t *dst = NULL;
    moq_rcbuf_t *buf = NULL;
    moq_result_t rc = moq_rcbuf_alloc_uninit(alloc, len, &buf, &dst);
    if (rc < 0)
        return rc;

    if (len > 0)
        memcpy(dst, data, len);

    *out = buf;
    return MOQ_OK;
}

moq_result_t moq_rcbuf_wrap(const moq_alloc_t *alloc,
                             const uint8_t *data, size_t len,
                             moq_rcbuf_release_fn release,
                             void *release_ctx,
                             moq_rcbuf_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (!alloc || !alloc->alloc || !alloc->free)
        return MOQ_ERR_INVAL;
    if (len > 0 && !data)
        return MOQ_ERR_INVAL;

    moq_rcbuf_t *buf = (moq_rcbuf_t *)alloc->alloc(sizeof(moq_rcbuf_t),
                                                     alloc->ctx);
    if (!buf)
        return MOQ_ERR_NOMEM;

    buf->alloc       = *alloc;
    buf->refcount    = 1;
    buf->wrapped     = true;
    buf->sliced      = false;
    buf->len         = len;
    buf->external    = data;
    buf->release     = release;
    buf->release_ctx = release_ctx;
    buf->parent      = NULL;

    *out = buf;
    return MOQ_OK;
}

moq_result_t moq_rcbuf_slice(const moq_alloc_t *alloc,
                              moq_rcbuf_t *parent,
                              size_t offset, size_t len,
                              moq_rcbuf_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (!alloc || !alloc->alloc || !alloc->free || !parent)
        return MOQ_ERR_INVAL;
    if (offset + len < offset) return MOQ_ERR_INVAL;
    if (len > 0 && offset + len > parent->len) return MOQ_ERR_INVAL;
    if (len == 0 && offset > parent->len) return MOQ_ERR_INVAL;

    moq_rcbuf_t *root = parent->sliced ? parent->parent : parent;
    const uint8_t *root_data = moq_rcbuf_data(root);
    const uint8_t *parent_data = moq_rcbuf_data(parent);
    size_t root_offset = (root_data && parent_data)
        ? (size_t)(parent_data - root_data) + offset
        : offset;

    moq_rcbuf_t *buf = (moq_rcbuf_t *)alloc->alloc(sizeof(moq_rcbuf_t),
                                                     alloc->ctx);
    if (!buf) return MOQ_ERR_NOMEM;

    buf->alloc       = *alloc;
    buf->refcount    = 1;
    buf->wrapped     = false;
    buf->sliced      = true;
    buf->len         = len;
    buf->external    = root_data ? root_data + root_offset : NULL;
    buf->release     = NULL;
    buf->release_ctx = NULL;
    buf->parent      = moq_rcbuf_incref(root);

    *out = buf;
    return MOQ_OK;
}

moq_result_t moq_rcbuf_clone(const moq_alloc_t *dst_alloc,
                              const moq_rcbuf_t *src,
                              moq_rcbuf_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (!dst_alloc || !dst_alloc->alloc || !dst_alloc->free)
        return MOQ_ERR_INVAL;
    if (!src) return MOQ_ERR_INVAL;

    /*
     * Independent copy of exactly the visible range into storage owned
     * by dst_alloc. Delegating to create keeps behavior aligned: it
     * copies the bytes, retains no parent/root (standalone storage),
     * inherits no wrapped release callback, and handles len == 0
     * (data may be NULL). The caller owns the affinity discipline:
     * dst_alloc must be valid where this runs, and src must stay alive
     * for the duration of the copy.
     */
    return moq_rcbuf_create(dst_alloc, moq_rcbuf_data(src),
                            moq_rcbuf_len(src), out);
}

moq_rcbuf_t *moq_rcbuf_incref(moq_rcbuf_t *buf)
{
    if (buf) {
        /* Intentionally non-recoverable (see rcbuf.h): at UINT32_MAX a
         * further incref would wrap to 0 and let a later decref free a
         * still-referenced buffer (use-after-free / double-free).
         * Reaching this means a refcount leak or memory corruption, not
         * a runtime condition a caller can handle - and incref returns a
         * pointer with no error channel. Aborting is the safe trap. */
        if (buf->refcount == UINT32_MAX)
            abort();
        buf->refcount++;
    }
    return buf;
}

void moq_rcbuf_decref(moq_rcbuf_t *buf)
{
    if (!buf)
        return;
    if (--buf->refcount == 0) {
        if (buf->sliced) {
            moq_rcbuf_t *p = buf->parent;
            buf->alloc.free(buf, sizeof(moq_rcbuf_t), buf->alloc.ctx);
            moq_rcbuf_decref(p);
        } else if (buf->wrapped) {
            if (buf->release)
                buf->release(buf->release_ctx, buf->external, buf->len);
            buf->alloc.free(buf, sizeof(moq_rcbuf_t), buf->alloc.ctx);
        } else {
            size_t total = sizeof(moq_rcbuf_t) + buf->len;
            buf->alloc.free(buf, total, buf->alloc.ctx);
        }
    }
}

const uint8_t *moq_rcbuf_data(const moq_rcbuf_t *buf)
{
    if (!buf) return NULL;
    if (buf->wrapped || buf->sliced) return buf->external;
    return (const uint8_t *)buf + sizeof(moq_rcbuf_t);
}

size_t moq_rcbuf_len(const moq_rcbuf_t *buf)
{
    return buf ? buf->len : 0;
}

uint32_t moq_rcbuf_refcount(const moq_rcbuf_t *buf)
{
    return buf ? buf->refcount : 0;
}
