/*
 * The /api/v1/shards document: one coherent broker generation's copied
 * snapshot rows, projected as JSON. Rendered by the coordinator into the
 * reserved bank's JSON body from the SAME frozen rows the metrics bodies use;
 * never from a live lane.
 *
 * The field set is FINITE and mapped by name from the copied row types:
 * raw counters keep their raw names; the gauges the metrics renderer derives
 * by subtracting internal entities are not exported here, and the renderer
 * refuses (exactly as the metrics renderer does) when a shard-plane row
 * contradicts its core row, so a poisoned generation never reaches a body.
 */
#ifndef MOQR_CLI_SHARDS_DOC_H
#define MOQR_CLI_SHARDS_DOC_H

#include <moq/relay/moqr_obs.h>

#include <stddef.h>
#include <stdint.h>

/*
 * Render `n` views (1..MOQR_SHARDS_MAX) for generation `serial` into `buf`
 * (`cap` bytes, room for the body and one terminating NUL). A view with a
 * NULL `shard` is the single-lane composition ("absent"); a NULL `core` or
 * `bind`, a label that is not complete UTF-8, an unknown transport, or a
 * shard-plane row whose internal entities exceed its core's are refused as
 * MOQR_ERR_INVAL. MOQR_ERR_CAPACITY when the body does not fit. On any
 * refusal the output holds nothing usable.
 */
moqr_result_t moqr_cli_shards_render(const moqr_snapshot_view_t *views,
                                     uint32_t n, uint64_t serial, char *buf,
                                     size_t cap, size_t *out_len);

/*
 * The maximum BODY length (excluding the NUL) for `lanes` views: measured by
 * the renderer over an immutable synthetic maximum (every row valid, every
 * counter at its widest, the widest labels made of six-byte-escaping bytes).
 * Read-only and reentrant. UINT64_MAX if refused or `lanes` is out of range.
 */
uint64_t moqr_cli_shards_bound(uint32_t lanes);

#endif /* MOQR_CLI_SHARDS_DOC_H */
