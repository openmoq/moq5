#include "moq/types.h"
#include <string.h>
#include <stdlib.h>

/* -- Result codes -------------------------------------------------- */

const char *moq_strerror(moq_result_t rc)
{
    switch (rc) {
    case MOQ_OK:                return "ok";
    case MOQ_DONE:              return "done";
    case MOQ_ERR_NOMEM:         return "out of memory";
    case MOQ_ERR_INVAL:         return "invalid argument";
    case MOQ_ERR_PROTO:         return "protocol violation";
    case MOQ_ERR_CLOSED:        return "session closed";
    case MOQ_ERR_WRONG_STATE:   return "wrong state";
    case MOQ_ERR_STALE_HANDLE:  return "stale handle";
    case MOQ_ERR_WRONG_SESSION: return "wrong session";
    case MOQ_ERR_WOULD_BLOCK:   return "would block";
    case MOQ_ERR_BUFFER:          return "buffer too small";
    case MOQ_ERR_REQUEST_BLOCKED: return "request blocked (no credit)";
    case MOQ_ERR_ABI_MISMATCH:    return "ABI mismatch (element too small for owned resource)";
    case MOQ_ERR_GOAWAY:          return "session draining (GOAWAY)";
    case MOQ_ERR_INTERRUPTED:     return "interrupted (latch set)";
    case MOQ_ERR_UNSUPPORTED:     return "not supported by this build";
    case MOQ_ERR_INTERNAL:        return "internal error";
    default:                    return "unknown error";
    }
}

/* -- Byte span ----------------------------------------------------- */

moq_bytes_t moq_bytes_cstr(const char *s)
{
    moq_bytes_t b;
    if (s) {
        b.data = (const uint8_t *)s;
        b.len  = strlen(s);
    } else {
        b.data = NULL;
        b.len  = 0;
    }
    return b;
}

/* -- Default allocator --------------------------------------------- */

static void *default_alloc(size_t size, void *ctx)
{
    (void)ctx;
    return malloc(size);
}

static void *default_realloc(void *ptr, size_t old_size, size_t new_size,
                             void *ctx)
{
    (void)old_size;
    (void)ctx;
    return realloc(ptr, new_size);
}

static void default_free(void *ptr, size_t size, void *ctx)
{
    (void)size;
    (void)ctx;
    free(ptr);
}

static const moq_alloc_t g_default_alloc = {
    NULL,
    default_alloc,
    default_realloc,
    default_free,
};

const moq_alloc_t *moq_alloc_default(void)
{
    return &g_default_alloc;
}

/* -- Handle packing ------------------------------------------------ */
/*
 * Layout: [ pool_tag : 4 ][ session_tag : 16 ][ generation : 28 ][ slot : 16 ]
 *
 * Bit positions (MSB first):
 *   63..60  pool_tag
 *   59..44  session_tag
 *   43..16  generation
 *   15..0   slot
 */

/*
 * Strict packing: rejects structurally invalid handles.
 * Returns 0 if pool_tag is 0 or > 15, session_tag is 0,
 * generation is even (free), or any field overflows its bit width.
 * Tests that need invalid handles construct them via ._opaque directly.
 */
uint64_t moq_handle_pack(uint32_t pool_tag, uint16_t session_tag,
                          uint32_t generation, uint32_t slot)
{
    if (pool_tag == 0 || pool_tag > 0xFu)  return 0;
    if (session_tag == 0)                  return 0;
    if ((generation & 1u) == 0)            return 0;
    if (generation > 0xFFFFFFFu)           return 0;
    if (slot > 0xFFFFu)                    return 0;

    return ((uint64_t)pool_tag      << 60)
         | ((uint64_t)session_tag   << 44)
         | ((uint64_t)generation    << 16)
         | ((uint64_t)slot);
}

uint32_t moq_handle_pool_tag(uint64_t h)    { return (uint32_t)(h >> 60) & 0xFu; }
uint16_t moq_handle_session_tag(uint64_t h) { return (uint16_t)(h >> 44); }
uint32_t moq_handle_generation(uint64_t h)  { return (uint32_t)(h >> 16) & 0xFFFFFFFu; }
uint32_t moq_handle_slot(uint64_t h)        { return (uint32_t)(h) & 0xFFFFu; }

/* -- Handle structural validity ------------------------------------ */
/*
 * Structural validity checks (no session/pool lookup required):
 *   - pool_tag matches the expected kind
 *   - session_tag is nonzero
 *   - generation is odd (live)
 */

static bool handle_valid(uint64_t h, uint32_t expected_pool)
{
    if (h == 0) return false;
    if (moq_handle_pool_tag(h) != expected_pool) return false;
    if (moq_handle_session_tag(h) == 0) return false;
    if ((moq_handle_generation(h) & 1u) == 0) return false;
    return true;
}

static uint64_t handle_hash(uint64_t h)
{
    return h * 0x9E3779B97F4A7C15ULL;
}

/* -- Subscription helpers ------------------------------------------ */

bool     moq_subscription_is_valid(moq_subscription_t h)    { return handle_valid(h._opaque, MOQ_HANDLE_POOL_SUBSCRIPTION); }
bool     moq_subscription_eq(moq_subscription_t a, moq_subscription_t b) { return a._opaque == b._opaque; }
uint64_t moq_subscription_hash(moq_subscription_t h)        { return handle_hash(h._opaque); }
uint64_t moq_subscription_id_for_trace(moq_subscription_t h){ return h._opaque; }

/* -- Publication helpers ------------------------------------------- */

bool     moq_publication_is_valid(moq_publication_t h)      { return handle_valid(h._opaque, MOQ_HANDLE_POOL_PUBLICATION); }
bool     moq_publication_eq(moq_publication_t a, moq_publication_t b) { return a._opaque == b._opaque; }
uint64_t moq_publication_hash(moq_publication_t h)          { return handle_hash(h._opaque); }
uint64_t moq_publication_id_for_trace(moq_publication_t h)  { return h._opaque; }

/* -- Fetch helpers ------------------------------------------------- */

bool     moq_fetch_is_valid(moq_fetch_t h)                  { return handle_valid(h._opaque, MOQ_HANDLE_POOL_FETCH); }
bool     moq_fetch_eq(moq_fetch_t a, moq_fetch_t b)        { return a._opaque == b._opaque; }
uint64_t moq_fetch_hash(moq_fetch_t h)                      { return handle_hash(h._opaque); }
uint64_t moq_fetch_id_for_trace(moq_fetch_t h)              { return h._opaque; }

/* -- Announcement helpers ------------------------------------------ */

bool     moq_announcement_is_valid(moq_announcement_t h)    { return handle_valid(h._opaque, MOQ_HANDLE_POOL_ANNOUNCEMENT); }
bool     moq_announcement_eq(moq_announcement_t a, moq_announcement_t b) { return a._opaque == b._opaque; }
uint64_t moq_announcement_hash(moq_announcement_t h)        { return handle_hash(h._opaque); }
uint64_t moq_announcement_id_for_trace(moq_announcement_t h){ return h._opaque; }

/* -- Namespace-sub handle helpers ---------------------------------- */

bool     moq_ns_sub_handle_is_valid(moq_ns_sub_handle_t h)    { return handle_valid(h._opaque, MOQ_HANDLE_POOL_NAMESPACE_SUB); }
bool     moq_ns_sub_handle_eq(moq_ns_sub_handle_t a, moq_ns_sub_handle_t b) { return a._opaque == b._opaque; }
uint64_t moq_ns_sub_handle_hash(moq_ns_sub_handle_t h)        { return handle_hash(h._opaque); }
uint64_t moq_ns_sub_handle_id_for_trace(moq_ns_sub_handle_t h){ return h._opaque; }

/* -- Track-status handle helpers ----------------------------------- */

bool     moq_track_status_handle_is_valid(moq_track_status_handle_t h)    { return handle_valid(h._opaque, MOQ_HANDLE_POOL_TRACK_STATUS); }
bool     moq_track_status_handle_eq(moq_track_status_handle_t a, moq_track_status_handle_t b) { return a._opaque == b._opaque; }
uint64_t moq_track_status_handle_hash(moq_track_status_handle_t h)        { return handle_hash(h._opaque); }
uint64_t moq_track_status_handle_id_for_trace(moq_track_status_handle_t h){ return h._opaque; }

/* -- Track-sub (SUBSCRIBE_TRACKS) handle helpers ------------------- */

bool     moq_track_sub_handle_is_valid(moq_track_sub_handle_t h)    { return handle_valid(h._opaque, MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS); }
bool     moq_track_sub_handle_eq(moq_track_sub_handle_t a, moq_track_sub_handle_t b) { return a._opaque == b._opaque; }
uint64_t moq_track_sub_handle_hash(moq_track_sub_handle_t h)        { return handle_hash(h._opaque); }
uint64_t moq_track_sub_handle_id_for_trace(moq_track_sub_handle_t h){ return h._opaque; }
