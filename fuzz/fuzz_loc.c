/*
 * libFuzzer target for the LOC (Low Overhead Container) metadata codec
 * (media/loc/include/moq/loc.h).
 *
 * Invariants under test:
 *   1. Closed return-status set and fail-closed version/profile gating:
 *      - moq_loc_parse() on MOQ_VERSION_DRAFT_16 or MOQ_VERSION_DRAFT_18
 *        with MOQ_LOC_PROFILE_01 returns only MOQ_OK or MOQ_ERR_PROTO.
 *      - Unknown transport_version (0) or reserved profile (MOQ_LOC_PROFILE_02)
 *        fails closed with MOQ_ERR_INVAL.
 *   2. Borrowed-slice and field range bounds on MOQ_OK:
 *      - video_config points within [data, data + size] and has len <= 0xFFFF.
 *      - video_frame_marking.temporal_id <= 7; audio_level.level <= 127.
 *   3. Round-trip and canonical re-encode idempotence:
 *      - Any header set accepted by moq_loc_parse() for a transport version
 *        re-encodes via moq_loc_encode() with MOQ_OK under that same version,
 *        re-parses to an identical moq_loc_headers_t, and re-encodes to
 *        byte-identical canonical KVP output.
 *   4. Exact allocator balance:
 *      - All moq_rcbuf_t allocations are balanced (balance == 0 and
 *        bytes_alloc == bytes_free) at exit.
 */

#include <moq/loc.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FUZZ_MAX_INPUT 16384

typedef struct {
    int64_t balance;
    size_t  bytes_alloc;
    size_t  bytes_free;
} fuzz_alloc_state_t;

static void *fuzz_alloc(size_t size, void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    void *p = malloc(size > 0 ? size : 1);
    if (p) {
        s->balance++;
        s->bytes_alloc += size;
    }
    return p;
}

static void *fuzz_realloc(void *ptr, size_t old_size, size_t new_size,
                          void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    if (new_size == 0) {
        if (ptr) {
            free(ptr);
            s->balance--;
            s->bytes_free += old_size;
        }
        return NULL;
    }
    if (!ptr)
        return fuzz_alloc(new_size, ctx);
    void *np = realloc(ptr, new_size);
    if (np) {
        s->bytes_free += old_size;
        s->bytes_alloc += new_size;
    }
    return np;
}

static void fuzz_free(void *ptr, size_t size, void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    if (ptr) {
        s->balance--;
        s->bytes_free += size;
    }
    free(ptr);
}

static void check_slice_in(const uint8_t *base, size_t len, moq_bytes_t s)
{
    if (s.len == 0)
        return;
    if (!s.data)
        __builtin_trap();
    uintptr_t b = (uintptr_t)base;
    uintptr_t e = b + len;
    uintptr_t p = (uintptr_t)s.data;
    if (p < b || p > e || s.len > (size_t)(e - p))
        __builtin_trap();
}

static bool headers_eq(const moq_loc_headers_t *a, const moq_loc_headers_t *b)
{
    if (a->has_timestamp != b->has_timestamp ||
        (a->has_timestamp && a->timestamp != b->timestamp))
        return false;
    if (a->has_timescale != b->has_timescale ||
        (a->has_timescale && a->timescale != b->timescale))
        return false;
    if (a->has_video_frame_marking != b->has_video_frame_marking)
        return false;
    if (a->has_video_frame_marking) {
        const moq_loc_video_frame_marking_t *ma = &a->video_frame_marking;
        const moq_loc_video_frame_marking_t *mb = &b->video_frame_marking;
        if (ma->start_of_frame != mb->start_of_frame ||
            ma->end_of_frame != mb->end_of_frame ||
            ma->independent != mb->independent ||
            ma->discardable != mb->discardable ||
            ma->base_layer_sync != mb->base_layer_sync ||
            ma->temporal_id != mb->temporal_id ||
            ma->has_layer_id != mb->has_layer_id ||
            (ma->has_layer_id && ma->layer_id != mb->layer_id))
            return false;
    }
    if (a->has_audio_level != b->has_audio_level)
        return false;
    if (a->has_audio_level) {
        if (a->audio_level.voice_activity != b->audio_level.voice_activity ||
            a->audio_level.level != b->audio_level.level)
            return false;
    }
    if (a->has_video_config != b->has_video_config)
        return false;
    if (a->has_video_config) {
        if (a->video_config.len != b->video_config.len)
            return false;
        if (a->video_config.len > 0 &&
            memcmp(a->video_config.data, b->video_config.data,
                   a->video_config.len) != 0)
            return false;
    }
    return true;
}

static void fuzz_version(const moq_alloc_t *alloc, moq_version_t ver,
                         const uint8_t *data, size_t size)
{
    moq_loc_headers_t hdr;
    moq_bytes_t input = { data, size };
    moq_result_t rc = moq_loc_parse(ver, MOQ_LOC_PROFILE_01, input, &hdr);
    if (rc != MOQ_OK && rc != MOQ_ERR_PROTO)
        __builtin_trap();
    if (rc != MOQ_OK)
        return;

    if (hdr.struct_size != sizeof(hdr) || hdr.has_timescale)
        __builtin_trap();
    if (hdr.has_video_frame_marking) {
        if (hdr.video_frame_marking.temporal_id > 7)
            __builtin_trap();
        if (!hdr.video_frame_marking.has_layer_id &&
            hdr.video_frame_marking.layer_id != 0)
            __builtin_trap();
    }
    if (hdr.has_audio_level && hdr.audio_level.level > 127)
        __builtin_trap();
    if (hdr.has_video_config) {
        if (hdr.video_config.len > 0xFFFFu)
            __builtin_trap();
        check_slice_in(data, size, hdr.video_config);
    }

    bool any_field = hdr.has_timestamp || hdr.has_video_frame_marking ||
                     hdr.has_audio_level || hdr.has_video_config;

    moq_rcbuf_t *enc1 = NULL;
    if (moq_loc_encode(alloc, ver, MOQ_LOC_PROFILE_01, &hdr, &enc1) != MOQ_OK)
        __builtin_trap();

    if (!any_field) {
        if (enc1 != NULL)
            __builtin_trap();
        return;
    }
    if (!enc1 || moq_rcbuf_len(enc1) == 0 || !moq_rcbuf_data(enc1))
        __builtin_trap();

    moq_loc_headers_t hdr2;
    moq_bytes_t enc1_bytes = { moq_rcbuf_data(enc1), moq_rcbuf_len(enc1) };
    if (moq_loc_parse(ver, MOQ_LOC_PROFILE_01, enc1_bytes, &hdr2) != MOQ_OK)
        __builtin_trap();
    if (!headers_eq(&hdr, &hdr2))
        __builtin_trap();

    moq_rcbuf_t *enc2 = NULL;
    if (moq_loc_encode(alloc, ver, MOQ_LOC_PROFILE_01, &hdr2, &enc2) != MOQ_OK)
        __builtin_trap();
    if (!enc2 || moq_rcbuf_len(enc2) != moq_rcbuf_len(enc1) ||
        memcmp(moq_rcbuf_data(enc1), moq_rcbuf_data(enc2),
               moq_rcbuf_len(enc1)) != 0)
        __builtin_trap();

    moq_rcbuf_decref(enc2);
    moq_rcbuf_decref(enc1);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size > FUZZ_MAX_INPUT)
        return 0;

    fuzz_alloc_state_t as = { 0, 0, 0 };
    moq_alloc_t alloc = { &as, fuzz_alloc, fuzz_realloc, fuzz_free };

    /* Fail-closed selector checks. */
    moq_loc_headers_t bad;
    if (moq_loc_parse((moq_version_t)0, MOQ_LOC_PROFILE_01,
                      (moq_bytes_t){ data, size }, &bad) != MOQ_ERR_INVAL)
        __builtin_trap();
    if (moq_loc_parse(MOQ_VERSION_DRAFT_18, MOQ_LOC_PROFILE_02,
                      (moq_bytes_t){ data, size }, &bad) != MOQ_ERR_INVAL)
        __builtin_trap();

    fuzz_version(&alloc, MOQ_VERSION_DRAFT_16, data, size);
    fuzz_version(&alloc, MOQ_VERSION_DRAFT_18, data, size);

    if (as.balance != 0 || as.bytes_alloc != as.bytes_free)
        __builtin_trap();

    return 0;
}
