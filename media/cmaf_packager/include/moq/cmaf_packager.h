#ifndef MOQ_CMAF_PACKAGER_H
#define MOQ_CMAF_PACKAGER_H

/*
 * CMAF packager: builds an init segment (ftyp+moov) and one self-contained
 * chunk (moof+mdat) per write. The write-side counterpart of moq::cmaf.
 *
 * One instance is one elementary stream is one track. CMSF 3.3 allows a single
 * traf per moof, so audio and video need separate instances. trun offsets are
 * moof-relative (tfhd default-base-is-moof, no base_data_offset), so a chunk
 * never references bytes outside itself: no sidx, no mfra, no padding.
 *
 * The decoder configuration record handed to create() is the same byte format
 * moq_codec_init_data_build() produces and moq_cmaf_parse_init() returns: the
 * record payload with no box header. Wrapping it in its box, esds descriptor
 * chain included, is this module's job.
 *
 * Not thread-safe: one instance is driven from one thread. Distinct instances
 * share no state.
 */

#include <moq/cmaf.h>
#include <moq/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- Sample byte format ---------------------------------------------- */

/*
 * Layout of the samples handed to moq_cmaf_packager_write(). An mdat holds raw
 * access units, so the caller states what has to be converted or stripped on
 * the way in; nothing is sniffed except the optional ADTS header, whose
 * presence AAC encoders vary on packet by packet.
 */
typedef enum moq_cmaf_sample_format {
    /* AVC/HEVC to ANNEXB, everything else to ELEMENTARY. */
    MOQ_CMAF_SAMPLE_AUTO            = 0,
    /* Start codes rewritten as length prefixes of the avcC/hvcC width.
     * Parameter sets repeated inline on a sync sample are kept: valid, and
     * useful to a decoder joining mid-stream. */
    MOQ_CMAF_SAMPLE_ANNEXB          = 1,
    /* Already length-prefixed at the avcC/hvcC width: copied. */
    MOQ_CMAF_SAMPLE_LENGTH_PREFIXED = 2,
    /* One raw access unit (AV1 OBUs, an AAC frame, an Opus packet), copied.
     * For AAC an ADTS header is detected and stripped. */
    MOQ_CMAF_SAMPLE_ELEMENTARY      = 3,
} moq_cmaf_sample_format_t;

/* -- Configuration --------------------------------------------------- */

typedef struct moq_cmaf_packager_cfg {
    uint32_t                 struct_size;

    moq_cmaf_codec_kind_t    codec_kind;

    /* Decoder configuration record payload, no box header: avcC, hvcC, av1C,
     * AudioSpecificConfig or dOps. Copied at create. */
    moq_bytes_t              codec_config;

    moq_cmaf_sample_format_t sample_format;

    /* mdhd/trun timescale. Required for video. For audio 0 means samplerate,
     * which is what a CMAF audio track uses. */
    uint32_t                 timescale;

    /* tkhd/tfhd track_ID; 0 means 1. Never 0 on the wire. */
    uint32_t                 track_id;

    /*
     * Timebase of the pts/dts on each sample, as num/den seconds. 0/0 means
     * the timestamps are already in timescale units. timescale * num has to
     * fit in int32_t, which every sane pairing does.
     */
    uint32_t                 source_timebase_num;
    uint32_t                 source_timebase_den;

    /* Subtract the first sample's dts from every timestamp, so tfdt starts at
     * zero. Leave false to keep the caller's timeline as-is. */
    bool                     rebase_timestamps;

    /*
     * Sample duration in timescale units for samples that do not carry their
     * own. 0 derives it: video from fps_num/fps_den, AAC from the
     * AudioSpecificConfig frameLengthFlag, Opus per packet from its TOC byte.
     * Create fails if neither a value nor the inputs to derive one are present.
     */
    uint32_t                 default_sample_duration;

    /* Video. width/height are required; fps only feeds the duration above. */
    uint32_t                 width;
    uint32_t                 height;
    uint32_t                 fps_num;
    uint32_t                 fps_den;

    /* Audio. Both required. */
    uint32_t                 samplerate;
    uint32_t                 channel_count;

    /* esds maxBitrate/avgBitrate for AAC, in bits per second. Ignored for
     * every other codec. */
    uint32_t                 avg_bitrate;
} moq_cmaf_packager_cfg_t;

/* Initialize cfg to safe defaults and stamp struct_size; NULL is a no-op. */
MOQ_API void moq_cmaf_packager_cfg_init(moq_cmaf_packager_cfg_t *cfg);

/* -- Sample ---------------------------------------------------------- */

typedef struct moq_cmaf_packager_sample {
    /* One access unit in the configured sample format, borrowed for the
     * duration of the write() call. */
    moq_bytes_t data;

    /* In the configured source timebase. pts - dts becomes the trun
     * composition offset. */
    int64_t     pts;
    int64_t     dts;

    /* In timescale units; 0 uses cfg.default_sample_duration. */
    uint32_t    duration;

    /* Written as sample_depends_on=2, sample_is_non_sync=0. ISO-BMFF flags
     * cannot distinguish SAP type 1 from 2, so a caller that needs the exact
     * type has to state it itself. */
    bool        keyframe;
} moq_cmaf_packager_sample_t;

/* -- Packager -------------------------------------------------------- */

typedef struct moq_cmaf_packager moq_cmaf_packager_t;

/*
 * Create a packager and build its init segment.
 *
 * alloc must provide alloc and free; realloc is not used, so a bump/arena
 * allocator without one is fine. NULL uses moq_alloc_default().
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL for a NULL/undersized cfg or out, a codec_kind of
 *   UNKNOWN, a missing codec_config, a sample_format the codec cannot use, a
 *   missing required dimension/samplerate/timescale, an out-of-range timebase
 *   or fps, or a duration that can neither be given nor derived.
 * Returns MOQ_ERR_PROTO if codec_config is malformed for its codec.
 * Returns MOQ_ERR_NOMEM if the allocator fails.
 */
MOQ_API moq_result_t moq_cmaf_packager_create(const moq_alloc_t *alloc,
                                              const moq_cmaf_packager_cfg_t *cfg,
                                              moq_cmaf_packager_t **out);

/* Releases everything; NULL is a no-op. */
MOQ_API void moq_cmaf_packager_destroy(moq_cmaf_packager_t *p);

/* ftyp+moov, ready for a catalog's init_data. Borrowed from the packager and
 * stable for its lifetime. {NULL,0} if p is NULL. */
MOQ_API moq_bytes_t moq_cmaf_packager_init_segment(const moq_cmaf_packager_t *p);

/* The timescale that landed in mdhd, which for audio is the samplerate
 * whatever cfg.timescale asked for. 0 if p is NULL. */
MOQ_API uint32_t moq_cmaf_packager_timescale(const moq_cmaf_packager_t *p);

/*
 * Package count samples into one CMAF chunk (moof+mdat).
 *
 * On success *out_fragment borrows the packager's internal buffer: it stays
 * valid until the next write() on the same instance, or destroy(). Nothing is
 * retained from samples.
 *
 * The chunk's tfdt is the first sample's dts; every sample lands in one trun.
 * Samples must be in decode order with non-decreasing dts.
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL if p, samples or out_fragment is NULL, if count is 0,
 *   if any sample has no data, or if a timestamp does not fit its field.
 * Returns MOQ_ERR_PROTO if a sample does not hold what sample_format promised
 *   (Annex B with no start code, a length prefix running past the end, an ADTS
 *   header with no frame behind it).
 * Returns MOQ_ERR_NOMEM if the allocator fails.
 */
MOQ_API moq_result_t moq_cmaf_packager_write(moq_cmaf_packager_t *p,
                                             const moq_cmaf_packager_sample_t *samples,
                                             size_t count,
                                             moq_bytes_t *out_fragment);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CMAF_PACKAGER_H */
