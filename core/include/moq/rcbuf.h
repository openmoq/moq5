#ifndef MOQ_RCBUF_H
#define MOQ_RCBUF_H

/*
 * Refcounted byte buffer for zero-copy relay forwarding.
 *
 * Shard / threading contract: refcount operations (incref/decref) are
 * NOT atomic. All references to a given rcbuf must stay within one
 * shard — a single executor-affinity domain (a thread, executor, or run
 * loop). Sharing rcbuf ownership across shards via incref/decref is NOT
 * supported by libmoq's engine model. To hand bytes to another shard,
 * make an independent copy with moq_rcbuf_clone() using an allocator
 * valid for the destination shard. (Legacy/manual external
 * synchronization may protect raw calls in single-buffer use, but it is
 * not the shard model and must not be used by engine/adapter/fan-out
 * code to share ownership across shards.)
 *
 * Overflow protection: incref aborts if the refcount reaches
 * UINT32_MAX. This is a hard safety trap, not a recoverable error.
 *
 * The allocator vtable is copied into the rcbuf at creation so the
 * last decref frees with the correct allocator regardless of which
 * session holds the final reference.
 */

#include "export.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_rcbuf moq_rcbuf_t;

/*
 * Create a refcounted buffer by copying `data` (len bytes) into a
 * new allocation via `alloc`. Initial refcount is 1.
 *
 * Zero-length buffers are valid (data may be NULL when len == 0).
 * On failure (alloc returns NULL), sets *out to NULL and returns
 * MOQ_ERR_NOMEM.
 *
 * The allocator vtable is copied into the rcbuf. The allocator's ctx
 * pointer must remain valid until the final moq_rcbuf_decref frees
 * the buffer.
 *
 * Allocates: one block via alloc (header + data together).
 * Ownership: caller owns the initial reference; must decref.
 */
MOQ_API moq_result_t moq_rcbuf_create(const moq_alloc_t *alloc,
                                       const uint8_t *data, size_t len,
                                       moq_rcbuf_t **out);

/*
 * Callback invoked exactly once on final decref of a wrapped buffer.
 * Called before the rcbuf header is freed.
 */
typedef void (*moq_rcbuf_release_fn)(void *ctx,
                                      const uint8_t *data,
                                      size_t len);

/*
 * Create a refcounted buffer wrapping external data WITHOUT copying.
 *
 * Allocates only the rcbuf header via `alloc`. `moq_rcbuf_data()`
 * returns exactly the original `data` pointer. Initial refcount is 1.
 *
 * `release` is called exactly once on final decref, after all refs
 * are gone, before the header is freed. If `release` is NULL, final
 * decref frees only the header and does not touch `data`.
 *
 * For `len > 0`, `data` must be non-NULL.
 * For `len == 0`, `data` may be NULL.
 * On error, sets `*out` to NULL; does not call `release`.
 */
MOQ_API moq_result_t moq_rcbuf_wrap(const moq_alloc_t *alloc,
                                     const uint8_t *data, size_t len,
                                     moq_rcbuf_release_fn release,
                                     void *release_ctx,
                                     moq_rcbuf_t **out);

/*
 * Create a zero-copy slice of an existing rcbuf.
 *
 * Allocates only a slice header. `moq_rcbuf_data(slice)` returns
 * `moq_rcbuf_data(parent) + offset`. Increfs the root parent.
 *
 * Slices are flattened: slicing a slice references the root parent
 * directly with the adjusted offset. The intermediate can be decreffed
 * before the child.
 *
 * Bounds: `offset + len` must not exceed `parent->len`.
 * Zero-length: `offset <= parent->len` is valid for `len == 0`.
 *
 * On final decref of the slice, decrefs the root parent (may trigger
 * the root's release callback for wrapped parents).
 */
MOQ_API moq_result_t moq_rcbuf_slice(const moq_alloc_t *alloc,
                                      moq_rcbuf_t *parent,
                                      size_t offset, size_t len,
                                      moq_rcbuf_t **out);

/*
 * Create an independent copy of `src`'s visible bytes
 * (moq_rcbuf_data / moq_rcbuf_len) into new storage allocated from
 * `dst_alloc`. Initial refcount is 1. The clone shares nothing with
 * `src`: it retains no parent/root, and a wrapped source's release
 * callback is NOT inherited — the clone owns plain copied bytes, freed
 * by `dst_alloc` like any moq_rcbuf_create. A slice source clones only
 * its visible range, not the root's full storage.
 *
 * `dst_alloc` is mandatory (no NULL-as-default). Returns MOQ_ERR_INVAL
 * if `out`, `dst_alloc` (or its alloc/free), or `src` is NULL; on every
 * failure where `out` is non-NULL, *out is set to NULL. A zero-length
 * `src` yields a valid zero-length clone. Returns MOQ_ERR_NOMEM if
 * `dst_alloc` fails.
 *
 * This is an independent COPY, not a thread-safe ownership transfer:
 * the caller is responsible for running it where `dst_alloc` is valid
 * and for keeping `src` alive for the duration of the copy. This is the
 * blessed way to cross a shard boundary (see the threading contract
 * above).
 */
MOQ_API moq_result_t moq_rcbuf_clone(const moq_alloc_t *dst_alloc,
                                      const moq_rcbuf_t *src,
                                      moq_rcbuf_t **out);

/*
 * Increment the reference count. Returns buf for convenience.
 * NULL is a no-op (returns NULL).
 */
MOQ_API moq_rcbuf_t *moq_rcbuf_incref(moq_rcbuf_t *buf);

/*
 * Decrement the reference count. Frees when it reaches zero using
 * the allocator stored at creation time. NULL is a no-op.
 */
MOQ_API void moq_rcbuf_decref(moq_rcbuf_t *buf);

/* Borrowed pointer to the data. NULL if buf is NULL. */
MOQ_API const uint8_t *moq_rcbuf_data(const moq_rcbuf_t *buf);

/* Length in bytes. 0 if buf is NULL. */
MOQ_API size_t moq_rcbuf_len(const moq_rcbuf_t *buf);

/* Current reference count (for diagnostics/testing only). */
MOQ_API uint32_t moq_rcbuf_refcount(const moq_rcbuf_t *buf);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_RCBUF_H */
