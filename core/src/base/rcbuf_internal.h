#ifndef MOQ_BASE_RCBUF_INTERNAL_H
#define MOQ_BASE_RCBUF_INTERNAL_H

/*
 * Core-internal rcbuf constructors. Not part of the public ABI: no MOQ_API,
 * not installed under include/moq. Used across core/src (base + session) only.
 */

#include "moq/rcbuf.h"

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/*
 * Allocate a create-style rcbuf (header + payload in one allocation) with
 * UNINITIALISED payload storage, and hand back a writable pointer to that
 * storage via `*data_out`. The caller fills `len` bytes before the rcbuf is
 * shared with anyone else.
 *
 * This is the zero-copy counterpart to moq_rcbuf_create: instead of copying
 * caller bytes in, the caller writes directly into the rcbuf's inline payload
 * region. Ownership, refcount, and teardown are identical to a create()d
 * rcbuf -- final moq_rcbuf_decref frees the single allocation via the copied
 * allocator vtable, so the buffer stays valid after the originating session is
 * destroyed (the allocator ctx must remain valid until then, exactly as for
 * moq_rcbuf_create).
 *
 * Contract:
 *   - The `*data_out` pointer is writable ONLY until the rcbuf is emitted /
 *     shared. Once another owner can observe it (a second reference, an event
 *     handed to the app), it is immutable like any rcbuf. It is the caller's
 *     job to finish writing before that point.
 *   - `len == 0` is allowed; `*data_out` is then the (zero-length) inline
 *     region and must not be dereferenced.
 *   - On error `*out` is set to NULL, `*data_out` is set to NULL, and nothing
 *     is allocated.
 */
moq_result_t moq_rcbuf_alloc_uninit(const moq_alloc_t *alloc, size_t len,
                                    moq_rcbuf_t **out, uint8_t **data_out);

/*
 * Whether `buf` is a created rcbuf: its storage is its own single allocation
 * (moq_rcbuf_create / moq_rcbuf_alloc_uninit), of exactly
 * moq_rcbuf_allocation_size(moq_rcbuf_len(buf)) bytes. False for a slice
 * (which keeps its root's whole allocation alive) and for a wrapped buffer
 * (whose external storage and release context are not described here), and
 * for NULL.
 */
bool moq_rcbuf_is_created_root(const moq_rcbuf_t *buf);

/*
 * Whether `buf`'s own storage is released through an allocator equal, field
 * for field (ctx included), to `alloc`. For a created root that is the
 * allocator of its whole allocation. False for NULL.
 */
bool moq_rcbuf_allocated_by(const moq_rcbuf_t *buf, const moq_alloc_t *alloc);

#endif /* MOQ_BASE_RCBUF_INTERNAL_H */
