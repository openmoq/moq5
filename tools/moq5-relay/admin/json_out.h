/*
 * A bounded, validating JSON writer.
 *
 * Every document this tier exposes is rendered whole into caller-owned
 * storage, or not at all. The writer never allocates, never reads a clock,
 * and knows nothing about the values it is given beyond their bytes:
 *
 *   - every string is validated as complete UTF-8 before a byte of it is
 *     written (no isolated continuation, no truncated or overlong sequence,
 *     no encoded surrogate, nothing above U+10FFFF); valid multi-byte
 *     sequences are copied unchanged, never re-encoded or replaced;
 *   - `"`, `\`, bytes below 0x20 and 0x7F are escaped; every other byte is
 *     emitted as itself, so one input byte expands to at most six;
 *   - a value that fails validation, or a document that does not fit, makes
 *     the whole document REFUSED: the output's first byte is cleared and
 *     nothing partial is ever presented as a document.
 *
 * Capacity is the room for the BODY plus one terminating NUL: an output that
 * needs N body bytes fits cap = N + 1 and refuses cap = N. The reported length
 * excludes the NUL.
 */
#ifndef MOQR_JSON_OUT_H
#define MOQR_JSON_OUT_H

#include <moq/relay/types.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Containers nest no deeper than this; a deeper document is refused. */
#define MOQR_JSON_MAX_DEPTH 8u

typedef struct moqr_json_w {
    char   *buf;
    size_t  cap;
    size_t  len;
    bool    refused;
    /* Whether the next member in each open container needs a separator. */
    uint8_t depth;
    bool    first[MOQR_JSON_MAX_DEPTH];
} moqr_json_w_t;

/* Begin a document in `buf` (`cap` bytes). A NULL buffer with cap 0 makes a
 * COUNTING writer: nothing is stored, but the length is measured exactly, so
 * a bound can be derived from the renderer itself. */
void moqr_json_begin(moqr_json_w_t *w, char *buf, size_t cap);

/* Close the document. True with `*out_len` (excluding the NUL) when every
 * value validated, every container was closed and the body fitted; false
 * otherwise, with buf[0] cleared when there is a buffer. */
bool moqr_json_end(moqr_json_w_t *w, size_t *out_len);

/* Objects and arrays. A member name is written with `moqr_json_key`; the
 * separator between members and between elements is the writer's business. */
void moqr_json_object_begin(moqr_json_w_t *w);
void moqr_json_object_end(moqr_json_w_t *w);
void moqr_json_array_begin(moqr_json_w_t *w);
void moqr_json_array_end(moqr_json_w_t *w);
void moqr_json_key(moqr_json_w_t *w, const char *key);

/* Values. `moqr_json_str` takes an explicit length; `moqr_json_str_bounded`
 * takes a fixed array and refuses one with no terminator inside `cap`, so a
 * caller never scans past its own storage. */
void moqr_json_str(moqr_json_w_t *w, const char *s, size_t n);
void moqr_json_str_bounded(moqr_json_w_t *w, const char *arr, size_t cap);
void moqr_json_u64(moqr_json_w_t *w, uint64_t v);
void moqr_json_i64(moqr_json_w_t *w, int64_t v);
void moqr_json_bool(moqr_json_w_t *w, bool v);
/* The literal `null`: a value that is not available, never a fabricated one. */
void moqr_json_null(moqr_json_w_t *w);

/* The validation rule on its own, for callers that want to refuse early. */
bool moqr_json_utf8_valid(const char *s, size_t n);

/* The largest number of output bytes `n` input bytes can produce. */
size_t moqr_json_escaped_max(size_t n);

#ifdef __cplusplus
}
#endif

#endif /* MOQR_JSON_OUT_H */
