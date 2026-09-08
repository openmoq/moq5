#include <moq/relay/types.h>

#include <stddef.h>
#include <string.h>

const char *
moqr_strerror(moqr_result_t rc)
{
    switch (rc) {
    case MOQR_OK:               return "ok";
    case MOQR_DONE:             return "done";
    case MOQR_ERR_NOMEM:        return "out of memory";
    case MOQR_ERR_INVAL:        return "invalid argument";
    case MOQR_ERR_WOULD_BLOCK:  return "would block";
    case MOQR_ERR_STALE_HANDLE: return "stale handle";
    case MOQR_ERR_WRONG_STATE:  return "wrong state";
    case MOQR_ERR_CAPACITY:     return "capacity refused";
    case MOQR_ERR_UNSUPPORTED:  return "unsupported";
    case MOQR_ERR_TOO_OLD:      return "too old (past retention)";
    case MOQR_ERR_INTERNAL:     return "internal error";
    default:                    return "unknown error";
    }
}

/* Structural validity mirrors the session core's handle rules via the public
 * unpack helpers: expected pool tag, nonzero shard tag, odd (live)
 * generation. Zero-init sentinels fail all three. */
static bool
handle_valid(uint64_t h, uint32_t pool_tag)
{
    return moq_handle_pool_tag(h) == pool_tag &&
           moq_handle_session_tag(h) != 0 &&
           (moq_handle_generation(h) & 1u) != 0;
}

bool moqr_track_is_valid(moqr_track_t h)
{
    return handle_valid(h._opaque, MOQR_HANDLE_POOL_TRACK);
}

bool moqr_track_eq(moqr_track_t a, moqr_track_t b)
{
    return a._opaque == b._opaque;
}

bool moqr_cursor_is_valid(moqr_cursor_t h)
{
    return handle_valid(h._opaque, MOQR_HANDLE_POOL_CURSOR);
}

bool moqr_cursor_eq(moqr_cursor_t a, moqr_cursor_t b)
{
    return a._opaque == b._opaque;
}

bool moqr_binding_is_valid(moqr_binding_t h)
{
    return handle_valid(h._opaque, MOQR_HANDLE_POOL_BINDING);
}

bool moqr_binding_eq(moqr_binding_t a, moqr_binding_t b)
{
    return a._opaque == b._opaque;
}

bool moqr_parked_is_valid(moqr_parked_t h)
{
    return handle_valid(h._opaque, MOQR_HANDLE_POOL_PARKED);
}

bool moqr_parked_eq(moqr_parked_t a, moqr_parked_t b)
{
    return a._opaque == b._opaque;
}

/* A field may be written only when the caller's declared size contains it
 * entirely — a prefix caller compiled against an older layout must never
 * take a write past its own sizeof. */
#define CFG_FIELD_FITS(field, n) \
    (offsetof(moqr_core_cfg_t, field) + sizeof(((moqr_core_cfg_t *)0)->field) \
        <= (n))

void
moqr_core_cfg_init_sized(moqr_core_cfg_t *cfg, size_t cfg_size,
                         const moq_alloc_t *alloc)
{
    if (cfg == NULL || cfg_size < sizeof(uint32_t)) {
        return;
    }
    size_t n = cfg_size < sizeof(moqr_core_cfg_t)
                   ? cfg_size
                   : sizeof(moqr_core_cfg_t);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (CFG_FIELD_FITS(alloc, n)) {
        cfg->alloc = alloc;
    }
    if (CFG_FIELD_FITS(shard_count, n)) {
        cfg->shard_count = 1;
    }
}
