/*
 * The /api/v1/info document: a FINITE, known-safe projection of the
 * immutable configuration and the capacity model, rendered once before the
 * admin endpoint activates and served as immutable bytes for the endpoint's
 * whole life.
 *
 * Every field is copied by name from the inputs. Nothing is enumerated from a
 * struct, so a field added to the configuration tomorrow is not exported
 * until someone adds it here on purpose. Never exported: certificate and key
 * paths of either listener, anything under the authorization section, and
 * the internal ALPN/subprotocol pointer tables.
 *
 * Rendering is pure: the inputs are read, the document is written through the
 * validating JSON writer, and a refusal (a configuration string that is not
 * complete UTF-8, an unterminated array, or a document that does not fit) is
 * whole-document -- nothing partial is ever presented.
 */
#ifndef MOQR_CLI_INFO_DOC_H
#define MOQR_CLI_INFO_DOC_H

#include "config.h"

#include "../shard/moqr_shards.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct moqr_cli_info_inputs {
    const moqr_cli_config_t     *cfg;
    const moqr_cli_shard_plan_t *plan;      /* the resolved shard plan       */
    const moqr_shards_limits_t  *limits;    /* the resolved budgets, from the
                                             * same pure resolver serve uses */
    const moqr_cli_capacity_t   *capacity;  /* the described ceiling         */
    bool                         dual_listener_build;
    bool                         verify_build;
} moqr_cli_info_inputs_t;

/*
 * Render the document into `buf` (`cap` bytes, room for the body and one
 * terminating NUL). On MOQR_OK `*out_len` is the body length excluding the
 * NUL. MOQR_ERR_INVAL for a NULL input or a value that fails validation;
 * MOQR_ERR_CAPACITY when the body does not fit. `buf` holds nothing usable on
 * any refusal.
 */
moqr_result_t moqr_cli_info_render(const moqr_cli_info_inputs_t *in, char *buf,
                                   size_t cap, size_t *out_len);

/*
 * The maximum BODY length (excluding the NUL) the renderer can produce for
 * any inputs the configuration parser admits: measured by the renderer
 * itself over an IMMUTABLE synthetic input in which every string is as long
 * as its array allows and made of control bytes (six output bytes each),
 * every array is at its bounded maximum, every number prints at its widest,
 * every bool is false and every optional part is present. Read-only and
 * reentrant: no shared scratch, no allocation. Allocate bound + 1.
 * UINT64_MAX if the measurement refused.
 */
uint64_t moqr_cli_info_bound(void);

#endif /* MOQR_CLI_INFO_DOC_H */
