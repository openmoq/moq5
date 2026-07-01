#ifndef MOQ_TYPES_H
#define MOQ_TYPES_H

#include "export.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- Result codes -------------------------------------------------- */
/*
 * Convention: MOQ_OK == 0, MOQ_DONE > 0 (sentinel, not error),
 * all errors are negative. `if (rc < 0)` is the universal error check.
 */

typedef int moq_result_t;

#define MOQ_OK                 0
#define MOQ_DONE               1

#define MOQ_ERR_NOMEM         (-1)
#define MOQ_ERR_INVAL         (-2)
#define MOQ_ERR_PROTO         (-3)
#define MOQ_ERR_CLOSED        (-4)
#define MOQ_ERR_WRONG_STATE   (-5)
#define MOQ_ERR_STALE_HANDLE  (-6)
#define MOQ_ERR_WRONG_SESSION (-7)
#define MOQ_ERR_WOULD_BLOCK   (-8)
#define MOQ_ERR_BUFFER          (-9)
#define MOQ_ERR_REQUEST_BLOCKED (-10)
#define MOQ_ERR_ABI_MISMATCH    (-11)
#define MOQ_ERR_GOAWAY          (-12)
/* A blocking call returned because the caller's interrupt latch is set
 * (moq_endpoint_set_interrupted, service tier); the object is NOT terminal --
 * the call blocks normally again once the latch clears. */
#define MOQ_ERR_INTERRUPTED     (-13)
/* The requested protocol / backend / version is not compiled into this build
 * or not implemented (e.g. a stubbed transport backend). Distinct from
 * MOQ_ERR_INVAL: the request is well-formed; this build cannot serve it. */
#define MOQ_ERR_UNSUPPORTED     (-14)
#define MOQ_ERR_INTERNAL        (-99)

MOQ_API const char *moq_strerror(moq_result_t rc);

/* -- Byte span ----------------------------------------------------- */
/*
 * Non-owning view of a contiguous byte range. Used for namespace parts,
 * track names, payloads, and all wire data. Byte-safe: not required to
 * be UTF-8 or NUL-terminated.
 */

typedef struct moq_bytes {
    const uint8_t *data;
    size_t         len;
} moq_bytes_t;

/*
 * Compile-time literal helper. Works ONLY on string literals and
 * char arrays. DO NOT pass a char* variable.
 *
 * C uses compound literals; C++ uses brace initialization.
 */
#ifdef __cplusplus
#define MOQ_BYTES_LITERAL(s) \
    moq_bytes_t { reinterpret_cast<const uint8_t *>(s), sizeof(s) - 1 }
#else
#define MOQ_BYTES_LITERAL(s) \
    ((moq_bytes_t){ .data = (const uint8_t *)(s), .len = sizeof(s) - 1 })
#endif

/*
 * Runtime helper for NUL-terminated strings. Declared function so
 * public headers do not require <string.h>.
 */
MOQ_API moq_bytes_t moq_bytes_cstr(const char *s);

/* -- Namespace ----------------------------------------------------- */

typedef struct moq_namespace {
    const moq_bytes_t *parts;
    size_t             count;   /* 0..32; 0 allowed for prefix APIs */
} moq_namespace_t;

#define MOQ_FULL_TRACK_NAME_MAX 4096

/* -- Authorization token ------------------------------------------- */

/* Caller-owned authorization token for outbound requests.
 * token_value is borrowed for the duration of the API call. */
typedef struct moq_auth_token {
    uint64_t    token_type;
    moq_bytes_t token_value;
} moq_auth_token_t;

/* -- Allocator ----------------------------------------------------- */

/*
 * alloc and free are required for any allocator passed to libmoq.
 * realloc is additionally required for session allocators because
 * receive buffering can grow; moq_session_create rejects NULL realloc.
 */
typedef struct moq_alloc {
    void *ctx;
    void *(*alloc)  (size_t size, void *ctx);
    void *(*realloc)(void *ptr, size_t old_size, size_t new_size, void *ctx);
    void  (*free)   (void *ptr, size_t size, void *ctx);
} moq_alloc_t;

/*
 * Returns a default allocator backed by libc malloc/realloc/free.
 * The returned pointer is to a static const, valid for the program's
 * lifetime. Does not allocate.
 */
MOQ_API const moq_alloc_t *moq_alloc_default(void);

/* -- Entity handles ------------------------------------------------ */
/*
 * 64-bit packed handles. Layout:
 *   [ pool_tag : 4 ][ session_tag : 16 ][ generation : 28 ][ slot : 16 ]
 *
 * pool_tag 0 is reserved: MOQ_*_INVALID = {0} is naturally invalid.
 * session_tag catches cross-session misuse (relay safety); must be nonzero.
 * generation catches use-after-free (odd = live, even = free).
 *   28 bits = ~268M reuses per slot before exhaustion.
 * slot is the index into the per-pool array (max 65535).
 *
 * Structural validity (checked by *_is_valid):
 *   - pool_tag matches the expected handle kind
 *   - session_tag is nonzero
 *   - generation is odd (live)
 *
 * See docs/API_DECISIONS.md section 1 for the full rationale.
 */

typedef struct moq_subscription  { uint64_t _opaque; } moq_subscription_t;
typedef struct moq_publication   { uint64_t _opaque; } moq_publication_t;
typedef struct moq_fetch         { uint64_t _opaque; } moq_fetch_t;
typedef struct moq_announcement  { uint64_t _opaque; } moq_announcement_t;
typedef struct moq_ns_sub_handle { uint64_t _opaque; } moq_ns_sub_handle_t;
typedef struct moq_track_status_handle { uint64_t _opaque; } moq_track_status_handle_t;
typedef struct moq_track_sub_handle { uint64_t _opaque; } moq_track_sub_handle_t;

/* C/C++ compatible zero-init sentinels. */
#ifdef __cplusplus
#define MOQ_SUBSCRIPTION_INVALID (moq_subscription_t{0})
#define MOQ_PUBLICATION_INVALID  (moq_publication_t{0})
#define MOQ_FETCH_INVALID        (moq_fetch_t{0})
#define MOQ_ANNOUNCEMENT_INVALID (moq_announcement_t{0})
#define MOQ_NS_SUB_HANDLE_INVALID (moq_ns_sub_handle_t{0})
#define MOQ_TRACK_STATUS_HANDLE_INVALID (moq_track_status_handle_t{0})
#define MOQ_TRACK_SUB_HANDLE_INVALID (moq_track_sub_handle_t{0})
#else
#define MOQ_SUBSCRIPTION_INVALID ((moq_subscription_t){ 0 })
#define MOQ_PUBLICATION_INVALID  ((moq_publication_t){ 0 })
#define MOQ_FETCH_INVALID        ((moq_fetch_t){ 0 })
#define MOQ_ANNOUNCEMENT_INVALID ((moq_announcement_t){ 0 })
#define MOQ_NS_SUB_HANDLE_INVALID ((moq_ns_sub_handle_t){ 0 })
#define MOQ_TRACK_STATUS_HANDLE_INVALID ((moq_track_status_handle_t){ 0 })
#define MOQ_TRACK_SUB_HANDLE_INVALID ((moq_track_sub_handle_t){ 0 })
#endif

MOQ_API bool     moq_subscription_is_valid(moq_subscription_t h);
MOQ_API bool     moq_subscription_eq(moq_subscription_t a, moq_subscription_t b);
MOQ_API uint64_t moq_subscription_hash(moq_subscription_t h);
MOQ_API uint64_t moq_subscription_id_for_trace(moq_subscription_t h);

MOQ_API bool     moq_publication_is_valid(moq_publication_t h);
MOQ_API bool     moq_publication_eq(moq_publication_t a, moq_publication_t b);
MOQ_API uint64_t moq_publication_hash(moq_publication_t h);
MOQ_API uint64_t moq_publication_id_for_trace(moq_publication_t h);

MOQ_API bool     moq_fetch_is_valid(moq_fetch_t h);
MOQ_API bool     moq_fetch_eq(moq_fetch_t a, moq_fetch_t b);
MOQ_API uint64_t moq_fetch_hash(moq_fetch_t h);
MOQ_API uint64_t moq_fetch_id_for_trace(moq_fetch_t h);

MOQ_API bool     moq_announcement_is_valid(moq_announcement_t h);
MOQ_API bool     moq_announcement_eq(moq_announcement_t a, moq_announcement_t b);
MOQ_API uint64_t moq_announcement_hash(moq_announcement_t h);
MOQ_API uint64_t moq_announcement_id_for_trace(moq_announcement_t h);

MOQ_API bool     moq_ns_sub_handle_is_valid(moq_ns_sub_handle_t h);
MOQ_API bool     moq_ns_sub_handle_eq(moq_ns_sub_handle_t a, moq_ns_sub_handle_t b);
MOQ_API uint64_t moq_ns_sub_handle_hash(moq_ns_sub_handle_t h);
MOQ_API uint64_t moq_ns_sub_handle_id_for_trace(moq_ns_sub_handle_t h);

MOQ_API bool     moq_track_status_handle_is_valid(moq_track_status_handle_t h);
MOQ_API bool     moq_track_status_handle_eq(moq_track_status_handle_t a, moq_track_status_handle_t b);
MOQ_API uint64_t moq_track_status_handle_hash(moq_track_status_handle_t h);
MOQ_API uint64_t moq_track_status_handle_id_for_trace(moq_track_status_handle_t h);

MOQ_API bool     moq_track_sub_handle_is_valid(moq_track_sub_handle_t h);
MOQ_API bool     moq_track_sub_handle_eq(moq_track_sub_handle_t a, moq_track_sub_handle_t b);
MOQ_API uint64_t moq_track_sub_handle_hash(moq_track_sub_handle_t h);
MOQ_API uint64_t moq_track_sub_handle_id_for_trace(moq_track_sub_handle_t h);

/* -- Handle packing (internal/tooling use) ------------------------- */

#define MOQ_HANDLE_POOL_SUBSCRIPTION  1u
#define MOQ_HANDLE_POOL_PUBLICATION   2u
#define MOQ_HANDLE_POOL_FETCH         3u
#define MOQ_HANDLE_POOL_ANNOUNCEMENT  4u
#define MOQ_HANDLE_POOL_SUBGROUP      5u
#define MOQ_HANDLE_POOL_NAMESPACE_SUB 6u
#define MOQ_HANDLE_POOL_TRACK_STATUS  7u
#define MOQ_HANDLE_POOL_PLAYBACK_TRACK 8u
#define MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS 9u

/*
 * Pack handle fields into a 64-bit opaque value.
 * Returns 0 if the result would be structurally invalid:
 *   - pool_tag == 0 or > 15
 *   - session_tag == 0
 *   - generation is even (free) or > 0xFFFFFFF (28 bits)
 *   - slot > 0xFFFF (16 bits)
 */
MOQ_API uint64_t moq_handle_pack(uint32_t pool_tag, uint16_t session_tag,
                                  uint32_t generation, uint32_t slot);
MOQ_API uint32_t moq_handle_pool_tag(uint64_t h);
MOQ_API uint16_t moq_handle_session_tag(uint64_t h);
MOQ_API uint32_t moq_handle_generation(uint64_t h);
MOQ_API uint32_t moq_handle_slot(uint64_t h);

/* -- Opaque forward declarations ----------------------------------- */

typedef struct moq_config  moq_config_t;
typedef struct moq_session moq_session_t;

/* -- Stream identity ----------------------------------------------- */

typedef struct moq_stream_ref { uint64_t _v; } moq_stream_ref_t;
typedef struct moq_stream_id  { uint64_t _v; } moq_stream_id_t;

#ifdef __cplusplus
static inline moq_stream_ref_t moq_stream_ref_from_u64(uint64_t v) {
    return moq_stream_ref_t{v};
}
static inline moq_stream_id_t moq_stream_id_from_u64(uint64_t v) {
    return moq_stream_id_t{v};
}
#else
static inline moq_stream_ref_t moq_stream_ref_from_u64(uint64_t v) {
    return (moq_stream_ref_t){ ._v = v };
}
static inline moq_stream_id_t moq_stream_id_from_u64(uint64_t v) {
    return (moq_stream_id_t){ ._v = v };
}
#endif

#ifdef __cplusplus
}
#endif

#endif /* MOQ_TYPES_H */
