#ifndef MOQ_LOCMAF_LAYOUT_PINS_H
#define MOQ_LOCMAF_LAYOUT_PINS_H

/*
 * Layout pins for <moq/locmaf.h>: sizeof, alignment, offsets and the
 * capability-extent macros of every record, cfg and state, on LP64 and on
 * the two ILP32 data models (uint64_t aligned to 4 on i686, to 8 on ARM).
 * Freestanding: compiled by test_locmaf_abi.c for the host and, syntax only,
 * by scripts/check_ilp32_headers.sh for the 32-bit targets.
 */

#include <moq/locmaf.h>

typedef struct lm_abi_u64_probe { char c; uint64_t v; } lm_abi_u64_probe_t;
#define LM_ABI_U64_ALIGN offsetof(lm_abi_u64_probe_t, v)
#define LM_ABI_PICK(lp64, ilp32_a4, ilp32_a8) \
    (sizeof(void *) == 8 ? (size_t)(lp64) \
     : LM_ABI_U64_ALIGN == 4 ? (size_t)(ilp32_a4) : (size_t)(ilp32_a8))

#define LM_PIN_SIZE(T, a, b, c) \
    _Static_assert(sizeof(T) == LM_ABI_PICK(a, b, c), #T ": sizeof")
#define LM_PIN_AT(T, f, a, b, c) \
    _Static_assert(offsetof(T, f) == LM_ABI_PICK(a, b, c), #T "." #f ": offset")
#define LM_PIN_ALIGN(T, a, b, c) \
    _Static_assert(_Alignof(T) == LM_ABI_PICK(a, b, c), #T ": alignment")

/* Records and cfgs: one extent each, the whole struct. */
LM_PIN_SIZE(moq_locmaf_track_t, 28, 28, 28);
LM_PIN_ALIGN(moq_locmaf_track_t, 4, 4, 4);
LM_PIN_AT(moq_locmaf_track_t, is_protected, 24, 24, 24);
LM_PIN_AT(moq_locmaf_track_t, default_per_sample_iv_size, 25, 25, 25);
_Static_assert(MOQ_LOCMAF_TRACK_V0_SIZE == sizeof(moq_locmaf_track_t),
               "moq_locmaf_track_t: one extent");

LM_PIN_SIZE(moq_locmaf_gen_box_t, 24, 16, 16);
LM_PIN_AT(moq_locmaf_gen_box_t, payload, 8, 8, 8);

LM_PIN_SIZE(moq_locmaf_object_view_t, 64, 36, 36);
LM_PIN_AT(moq_locmaf_object_view_t, gen_box_count, 8, 8, 8);
LM_PIN_AT(moq_locmaf_object_view_t, properties, 16, 12, 12);
LM_PIN_AT(moq_locmaf_object_view_t, raw_boxes, 48, 28, 28);
_Static_assert(MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE == sizeof(moq_locmaf_object_view_t),
               "moq_locmaf_object_view_t: one extent");

LM_PIN_SIZE(moq_locmaf_sample_t, 40, 40, 40);
LM_PIN_AT(moq_locmaf_sample_t, composition_offset, 16, 16, 16);
LM_PIN_AT(moq_locmaf_sample_t, offset, 24, 24, 24);
LM_PIN_AT(moq_locmaf_sample_t, subsample_index, 32, 32, 32);
LM_PIN_SIZE(moq_locmaf_subsample_t, 8, 8, 8);

LM_PIN_SIZE(moq_locmaf_chunk_t, 120, 76, 80);
LM_PIN_AT(moq_locmaf_chunk_t, base_media_decode_time, 8, 8, 8);
LM_PIN_AT(moq_locmaf_chunk_t, samples, 24, 24, 24);
LM_PIN_AT(moq_locmaf_chunk_t, per_sample_iv_size, 32, 28, 28);
LM_PIN_AT(moq_locmaf_chunk_t, ivs, 40, 36, 36);
LM_PIN_AT(moq_locmaf_chunk_t, subsample_total, 64, 48, 48);
LM_PIN_AT(moq_locmaf_chunk_t, mdat_payload, 88, 60, 60);
LM_PIN_AT(moq_locmaf_chunk_t, raw_boxes, 104, 68, 68);
_Static_assert(MOQ_LOCMAF_CHUNK_V0_SIZE == LM_ABI_PICK(120, 76, 76),
               "moq_locmaf_chunk_t: the extent through raw_boxes");

LM_PIN_SIZE(moq_locmaf_decoder_cfg_t, 40, 28, 28);
LM_PIN_AT(moq_locmaf_decoder_cfg_t, track, 16, 16, 16);
LM_PIN_AT(moq_locmaf_decoder_cfg_t, storage_size, 32, 24, 24);
_Static_assert(MOQ_LOCMAF_DECODER_CFG_V0_SIZE == sizeof(moq_locmaf_decoder_cfg_t),
               "moq_locmaf_decoder_cfg_t: one extent");
LM_PIN_SIZE(moq_locmaf_encoder_cfg_t, 40, 28, 28);
_Static_assert(MOQ_LOCMAF_ENCODER_CFG_V0_SIZE == sizeof(moq_locmaf_encoder_cfg_t),
               "moq_locmaf_encoder_cfg_t: one extent");

LM_PIN_SIZE(moq_locmaf_decoder_stats_t, 88, 88, 88);
LM_PIN_AT(moq_locmaf_decoder_stats_t, objects_decoded, 8, 8, 8);
_Static_assert(MOQ_LOCMAF_DECODER_STATS_V0_SIZE == sizeof(moq_locmaf_decoder_stats_t),
               "moq_locmaf_decoder_stats_t: one extent");
LM_PIN_SIZE(moq_locmaf_encoder_stats_t, 64, 64, 64);
_Static_assert(MOQ_LOCMAF_ENCODER_STATS_V0_SIZE == sizeof(moq_locmaf_encoder_stats_t),
               "moq_locmaf_encoder_stats_t: one extent");

/* Caller-owned states: complete types whose private layout may change
 * within the reserved tail; their sizes are what a caller allocates. */
LM_PIN_SIZE(moq_locmaf_ref_state_t, 272, 236, 240);
LM_PIN_SIZE(moq_locmaf_enc_position_t, 40, 36, 40);
LM_PIN_SIZE(moq_locmaf_decoder_t, 592, 544, 552);
LM_PIN_ALIGN(moq_locmaf_decoder_t, 8, 4, 8);
LM_PIN_SIZE(moq_locmaf_encoder_t, 896, 800, 816);
LM_PIN_ALIGN(moq_locmaf_encoder_t, 8, 4, 8);
LM_PIN_AT(moq_locmaf_decoder_t, _reserved, 464, 416, 424);
LM_PIN_AT(moq_locmaf_encoder_t, _reserved, 768, 672, 688);

/* The storage alignment covers every element type carved from it. */
_Static_assert(MOQ_LOCMAF_STORAGE_ALIGN >= _Alignof(uint64_t) &&
               MOQ_LOCMAF_STORAGE_ALIGN >= _Alignof(moq_locmaf_sample_t) &&
               MOQ_LOCMAF_STORAGE_ALIGN >= _Alignof(moq_locmaf_gen_box_t) &&
               MOQ_LOCMAF_STORAGE_ALIGN >= _Alignof(void *),
               "storage alignment");

#endif /* MOQ_LOCMAF_LAYOUT_PINS_H */
