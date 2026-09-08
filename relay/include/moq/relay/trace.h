#ifndef MOQRELAY_TRACE_H
#define MOQRELAY_TRACE_H

/*
 * Deterministic trace ring.
 *
 * Every state transition emits a fixed-size, scalar-only moqr_trace_rec_t
 * (scalar-only rule: never borrowed pointers). Records carry a per-ring monotonic
 * shard_seq — no wall clock — so the same input stream produces a
 * byte-identical trace stream on every replay. An FNV-1a hash folds over
 * records as they are emitted: run-twice hash equality is the determinism
 * gate; the bounded ring retains the recent tail as a flight recorder.
 */

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moqr_trace moqr_trace_t;

/*
 * Create a trace ring retaining `record_cap` recent records (0 = default).
 * The running hash covers EVERY emitted record regardless of ring wrap.
 */
MOQR_CORE_API moqr_result_t moqr_trace_create(const moq_alloc_t *alloc,
                                uint32_t record_cap,
                                moqr_trace_t **out);

MOQR_CORE_API void moqr_trace_destroy(moqr_trace_t *t);

/*
 * Bytes moqr_trace_create allocates for a ring of `record_cap` records
 * (0 = library default): the ring struct plus the record array. This is the
 * trace ring's term in a process capacity accounting, kept here so callers
 * never duplicate the default size or the private struct layout.
 */
MOQR_CORE_API size_t moqr_trace_bytes(uint32_t record_cap);

/*
 * Set the epoch triple stamped on subsequent records. The relay
 * core's control plane calls this when an epoch-bumping input applies.
 */
MOQR_CORE_API void moqr_trace_set_epochs(moqr_trace_t *t, moqr_epochs_t epochs);

/*
 * Emit one record. `rec->shard_seq` and `rec->epochs` are assigned by the
 * ring (caller values ignored); all other fields are the caller's. The
 * record is folded into the running hash and stored in the ring.
 */
MOQR_CORE_API void moqr_trace_emit(moqr_trace_t *t, moqr_trace_rec_t rec);

/* Running FNV-1a hash over all records emitted so far. */
MOQR_CORE_API uint64_t moqr_trace_hash(const moqr_trace_t *t);

/* Total records emitted (== next shard_seq). */
MOQR_CORE_API uint64_t moqr_trace_count(const moqr_trace_t *t);

/*
 * Read retained records, oldest first. Copies up to `cap` records into
 * `out`, returns the number copied. Observing; does not perturb the hash.
 */
MOQR_CORE_API size_t moqr_trace_read(const moqr_trace_t *t,
                       moqr_trace_rec_t *out, size_t cap);

/* Stable lower-snake name for a trace kind (e.g. "cursor_retire"); an
 * unknown kind maps to "unknown". */
MOQR_CORE_API const char *moqr_trace_kind_name(moqr_trace_kind_t kind);

/*
 * Serialize the retained records as JSON Lines (one object per record,
 * oldest first) into `buf`. Pure: no I/O, no allocation, hash untouched.
 * Sets `*written` to the content length (excluding the NUL). Returns
 * MOQR_OK when content + NUL fit; MOQR_ERR_CAPACITY on truncation, with
 * `*written` set to the length the full output needs (grow to *written + 1
 * and retry). Fields are all scalars, so the line is replay-stable.
 */
MOQR_CORE_API moqr_result_t moqr_trace_write_jsonl(const moqr_trace_t *t,
                                     char *buf, size_t cap, size_t *written);

#ifdef __cplusplus
}
#endif

#endif /* MOQRELAY_TRACE_H */
