#ifndef MOQ_TEST_MEDIA_SENDER_CMAF_INIT_H
#define MOQ_TEST_MEDIA_SENDER_CMAF_INIT_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Minimal AVC container fixture from test_media_sender.c: ftyp + moov,
 * tkhd track_ID, mdhd timescale 90000 and avc1/avcC decoder configuration.
 * Caller provides at least 256 bytes. Synthetic config, not decoder input. */
static inline void sender_test_cmaf_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8); p[3] = (uint8_t)v;
}

static inline size_t sender_test_cmaf_box(uint8_t *p, uint32_t n,
                                           const char *type)
{
    sender_test_cmaf_u32(p, n);
    memcpy(p + 4, type, 4);
    return 8;
}

static inline size_t sender_test_cmaf_init(uint8_t *buf, uint32_t track_id)
{
    static const uint8_t avcc[] = { 0x01 };
    size_t p = 0;
    p += sender_test_cmaf_box(buf + p, 20, "ftyp");
    memcpy(buf + p, "isom", 4); p += 4; sender_test_cmaf_u32(buf + p, 0); p += 4;
    memcpy(buf + p, "isom", 4); p += 4;

    size_t avcc_box = 8 + sizeof(avcc);
    size_t avc1 = 8 + 78 + avcc_box;
    size_t stsd = 8 + 8 + avc1;
    size_t stbl = 8 + stsd;
    size_t minf = 8 + stbl;
    size_t mdhd = 8 + 4 + 12;
    size_t mdia = 8 + mdhd + minf;
    size_t tkhd = 24;
    size_t trak = 8 + tkhd + mdia;
    size_t moov = 8 + trak;

    p += sender_test_cmaf_box(buf + p, (uint32_t)moov, "moov");
    p += sender_test_cmaf_box(buf + p, (uint32_t)trak, "trak");
    p += sender_test_cmaf_box(buf + p, (uint32_t)tkhd, "tkhd");
    sender_test_cmaf_u32(buf + p, 0); p += 4;            /* version+flags */
    sender_test_cmaf_u32(buf + p, 0); p += 4;            /* creation */
    sender_test_cmaf_u32(buf + p, 0); p += 4;            /* modification */
    sender_test_cmaf_u32(buf + p, track_id); p += 4;     /* track_ID */
    p += sender_test_cmaf_box(buf + p, (uint32_t)mdia, "mdia");
    p += sender_test_cmaf_box(buf + p, (uint32_t)mdhd, "mdhd");
    sender_test_cmaf_u32(buf + p, 0); p += 4; sender_test_cmaf_u32(buf + p, 0); p += 4;
    sender_test_cmaf_u32(buf + p, 0); p += 4; sender_test_cmaf_u32(buf + p, 90000); p += 4;
    p += sender_test_cmaf_box(buf + p, (uint32_t)minf, "minf");
    p += sender_test_cmaf_box(buf + p, (uint32_t)stbl, "stbl");
    p += sender_test_cmaf_box(buf + p, (uint32_t)stsd, "stsd");
    sender_test_cmaf_u32(buf + p, 0); p += 4; sender_test_cmaf_u32(buf + p, 1); p += 4;
    p += sender_test_cmaf_box(buf + p, (uint32_t)avc1, "avc1");
    memset(buf + p, 0, 78); p += 78;
    p += sender_test_cmaf_box(buf + p, (uint32_t)avcc_box, "avcC");
    memcpy(buf + p, avcc, sizeof(avcc)); p += sizeof(avcc);
    return p;
}

#endif /* MOQ_TEST_MEDIA_SENDER_CMAF_INIT_H */
