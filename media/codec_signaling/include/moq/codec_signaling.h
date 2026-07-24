#ifndef MOQ_CODEC_SIGNALING_H
#define MOQ_CODEC_SIGNALING_H

/*
 * Codec signaling utilities. Two composable helpers producing the artifacts
 * a catalog uses to signal a media track's codec: moq_codec_init_data_build()
 * produces the decoder configuration record (init_data), and
 * moq_codec_string_format() produces the codec string (e.g. "avc1.64001f",
 * "mp4a.40.2") from that same record.
 *
 * Pure, allocation-free, thread-safe. The caller always states the byte
 * format explicitly (no sniffing). Output goes to a caller-provided
 * buffer following a common protocol: call with buf == NULL (or a
 * too-small cap) to learn the required length in *out_len (returns
 * MOQ_ERR_BUFFER); no partial output is written on failure. The codec
 * string is raw ASCII, NOT NUL-terminated.
 */

#include <moq/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- init_data (decoder configuration record) builder --------------- */

/* Byte format of the source handed to moq_codec_init_data_build(). */
typedef enum moq_codec_source_format {
    MOQ_CODEC_SOURCE_AVC_ANNEXB  = 1, /* AVC Annex B SPS/PPS -> build avcC */
    MOQ_CODEC_SOURCE_AVC_AVCC    = 2, /* avcC record -> validate + copy */
    MOQ_CODEC_SOURCE_AAC_ASC     = 3, /* AudioSpecificConfig -> validate + copy */
    MOQ_CODEC_SOURCE_HEVC_ANNEXB = 4, /* HEVC Annex B VPS/SPS/PPS -> build hvcC */
    MOQ_CODEC_SOURCE_HEVC_HVCC   = 5, /* hvcC record -> validate + copy */
    MOQ_CODEC_SOURCE_AV1_OBU     = 6, /* AV1 OBUs -> build av1C */
    MOQ_CODEC_SOURCE_AV1_AV1C    = 7, /* av1C record -> validate + copy */
    MOQ_CODEC_SOURCE_OPUS_HEAD   = 8, /* OpusHead (RFC 7845) -> build dOps */
    MOQ_CODEC_SOURCE_OPUS_DOPS   = 9, /* OpusSpecificBox (dOps) -> validate + copy */
} moq_codec_source_format_t;

/* Parameters shared by the NAL-based families (AVC, HEVC). */
typedef struct moq_nal_params {
    /* NAL length prefix size (1..4) written into a built avcC/hvcC; default
     * 4. Ignored when the source format does not build a NAL-based record. */
    uint8_t length_size;
} moq_nal_params_t;

typedef struct moq_codec_init_data_cfg {
    uint32_t                  struct_size;
    moq_codec_source_format_t source_format;

    /* Source bytes in the format named by source_format. BORROWED. */
    moq_bytes_t               source;

    /* Parameters for building a NAL-based record (AVC/HEVC Annex B). */
    moq_nal_params_t          nal;
} moq_codec_init_data_cfg_t;

/* Initialize cfg to safe defaults and stamp struct_size; NULL is a no-op. */
MOQ_API void moq_codec_init_data_cfg_init(moq_codec_init_data_cfg_t *cfg);

/*
 * Build the decoder configuration record (init_data) into buf. Elemental
 * streams are assembled into a record: AVC/HEVC Annex B into an avcC/hvcC,
 * AV1 OBUs into an av1C, and an OpusHead into an OpusSpecificBox (dOps).
 * Already-formed records (avcC, hvcC, av1C, dOps) and AudioSpecificConfig
 * are validated and copied through unchanged.
 *
 * Notes on the assembled records (all verified byte-for-byte against
 * ffmpeg's muxers except where noted):
 *   - hvcC: min_spatial_segmentation_idc is emitted as 0 rather than parsed
 *     from the SPS VUI, and parallelismType follows as 0. This is an
 *     informative field; a source whose VUI sets it non-zero would differ
 *     there. Profile/tier/level, chroma_format and bit depths are parsed.
 *   - av1C: the four header bytes are parsed from the sequence header, which
 *     is then copied verbatim as the configOBUs. This is conformant but not
 *     byte-identical to muxers that re-serialize (normalize) the OBU.
 *
 * Returns MOQ_OK on success; MOQ_ERR_INVAL for bad arguments;
 * MOQ_ERR_BUFFER if buf/cap is too small (*out_len gets the required
 * length); MOQ_ERR_PROTO if the source is malformed for its format;
 * MOQ_ERR_UNSUPPORTED for a stream this build does not parse (an HEVC SPS
 * with sub-layer profile_tier_level, i.e. sps_max_sub_layers_minus1 > 0, or
 * an AV1 sequence header carrying timing_info / a decoder model) or an input
 * exceeding a fixed internal limit.
 */
MOQ_API moq_result_t moq_codec_init_data_build(const moq_codec_init_data_cfg_t *cfg,
                                               uint8_t *buf, size_t cap,
                                               size_t *out_len);

/* -- Decoder configuration format ----------------------------------- */

/* Byte format of moq_codec_string_cfg::decoder_config. */
typedef enum moq_codec_config_format {
    MOQ_CODEC_CONFIG_AVCC    = 1, /* AVCDecoderConfigurationRecord (avcC) */
    MOQ_CODEC_CONFIG_AAC_ASC = 2, /* AudioSpecificConfig */
    MOQ_CODEC_CONFIG_HVCC    = 3, /* HEVCDecoderConfigurationRecord (hvcC) */
    MOQ_CODEC_CONFIG_AV1C    = 4, /* AV1CodecConfigurationRecord (av1C) */
    MOQ_CODEC_CONFIG_OPUS    = 5, /* OpusSpecificBox (dOps); string is "opus" */
} moq_codec_config_format_t;

/* -- Formatter input ------------------------------------------------ */

typedef struct moq_codec_string_cfg {
    uint32_t                  struct_size;
    moq_codec_config_format_t config_format;

    /* Exact 4-byte sample entry (e.g. "avc1", "mp4a"), echoed as the output
     * prefix. For Opus the whole codec string is just this entry, so pass
     * "opus" (the RFC 6381 name), not the "Opus" box type. BORROWED. */
    moq_bytes_t               sample_entry;

    /* MP4 object type indication. Required for entries that carry an OTI
     * (mp4a): set the flag and the OTI byte. Ignored for avc1/avc3. */
    bool                      has_mp4_object_type_indication;
    uint8_t                   mp4_object_type_indication;

    /* Decoder configuration bytes in the format named by config_format.
     * BORROWED. */
    moq_bytes_t               decoder_config;
} moq_codec_string_cfg_t;

/* Initialize cfg to safe defaults and stamp struct_size; NULL is a no-op. */
MOQ_API void moq_codec_string_cfg_init(moq_codec_string_cfg_t *cfg);

/*
 * Format the codec string described by cfg into buf.
 *
 * Returns MOQ_OK on success; MOQ_ERR_INVAL for bad arguments or an
 * incoherent sample-entry / config-format pair; MOQ_ERR_BUFFER if
 * buf/cap is too small (*out_len gets the required length);
 * MOQ_ERR_PROTO if decoder_config is malformed for its format;
 * MOQ_ERR_UNSUPPORTED for an unimplemented config_format.
 */
MOQ_API moq_result_t moq_codec_string_format(const moq_codec_string_cfg_t *cfg,
                                             uint8_t *buf, size_t cap,
                                             size_t *out_len);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CODEC_SIGNALING_H */
