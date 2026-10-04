#include <moq/loc.h>
#include <moq/kvp.h>
#include <moq/wire.h>
#include <moq/rcbuf.h>
#include "test_support.h"
#include "test_oom_support.h"
#include <string.h>

#define P01 MOQ_LOC_PROFILE_01
#define D16 MOQ_VERSION_DRAFT_16
#define D18 MOQ_VERSION_DRAFT_18
#define P02 MOQ_LOC_PROFILE_02

int main(void)
{
    int failures = 0;

    /* -- init null-safe ----------------------------------------------- */
    {
        moq_loc_headers_init(NULL);
    }

    /* -- init sets struct_size --------------------------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        MOQ_TEST_CHECK_EQ_SIZE(h.struct_size, sizeof(moq_loc_headers_t));
        MOQ_TEST_CHECK(h.has_timestamp == false);
        MOQ_TEST_CHECK(h.has_timescale == false);
        MOQ_TEST_CHECK(h.has_video_frame_marking == false);
        MOQ_TEST_CHECK(h.has_audio_level == false);
        MOQ_TEST_CHECK(h.has_video_config == false);
    }

    /* -- parse empty ------------------------------------------------- */
    {
        moq_loc_headers_t h;
        moq_bytes_t empty = { NULL, 0 };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, empty, &h) == MOQ_OK);
        MOQ_TEST_CHECK(h.has_timestamp == false);
    }

    /* -- parse NULL out returns INVAL -------------------------------- */
    {
        moq_bytes_t empty = { NULL, 0 };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, empty, NULL) == MOQ_ERR_INVAL);
    }

    /* -- NULL data with nonzero len returns INVAL -------------------- */
    {
        moq_loc_headers_t h;
        moq_bytes_t bad = { NULL, 5 };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, bad, &h) == MOQ_ERR_INVAL);
    }

    /* -- invalid profile parse returns INVAL ------------------------- */
    {
        moq_loc_headers_t h;
        moq_bytes_t empty = { NULL, 0 };
        MOQ_TEST_CHECK(moq_loc_parse(D16, (moq_loc_profile_t)0, empty, &h) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_loc_parse(D16, (moq_loc_profile_t)99, empty, &h) == MOQ_ERR_INVAL);
    }

    /* -- invalid profile encode returns INVAL ------------------------ */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 1;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, (moq_loc_profile_t)0, &h, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == NULL);
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, (moq_loc_profile_t)99, &h, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == NULL);
    }

    /* -- LOC-02 parse returns INVAL (D18 codec not available) --------- */
    {
        moq_loc_headers_t h;
        moq_bytes_t empty = { NULL, 0 };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P02, empty, &h) == MOQ_ERR_INVAL);
    }

    /* -- LOC-02 encode returns INVAL --------------------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 48000;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P02, &h, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == NULL);
    }

    /* -- LOC-01: timestamp encode byte-level oracle ------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 1000000;

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out != NULL);

        /* Expected wire: delta=0x02 (1 byte), value=1000000.
         * 1000000 = 0x0F4240, QUIC varint 4-byte: 0x800F4240.
         * Wire: [0x02] [0x80 0x0F 0x42 0x40] = 5 bytes. */
        const uint8_t *d = moq_rcbuf_data(out);
        size_t len = moq_rcbuf_len(out);
        MOQ_TEST_CHECK_EQ_SIZE(len, 5);
        MOQ_TEST_CHECK_EQ_HEX(d[0], 0x02);
        MOQ_TEST_CHECK_EQ_HEX(d[1], 0x80);
        MOQ_TEST_CHECK_EQ_HEX(d[2], 0x0F);
        MOQ_TEST_CHECK_EQ_HEX(d[3], 0x42);
        MOQ_TEST_CHECK_EQ_HEX(d[4], 0x40);

        moq_loc_headers_t parsed;
        moq_bytes_t props = { d, len };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &parsed) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(parsed.timestamp, 1000000);
        moq_rcbuf_decref(out);
    }

    /* -- LOC-01: audio level byte-level oracle ------------------------ */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_audio_level = true;
        h.audio_level.voice_activity = true;
        h.audio_level.level = 30;

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);

        /* Expected: delta=0x06, value=0x9E (V=1|level=30).
         * Wire: [0x06] [0x40 0x9E] = 3 bytes.
         * (0x9E = 158, QUIC varint 2-byte: 0x409E) */
        const uint8_t *d = moq_rcbuf_data(out);
        size_t len = moq_rcbuf_len(out);
        MOQ_TEST_CHECK_EQ_SIZE(len, 3);
        MOQ_TEST_CHECK_EQ_HEX(d[0], 0x06);
        MOQ_TEST_CHECK_EQ_HEX(d[1], 0x40);
        MOQ_TEST_CHECK_EQ_HEX(d[2], 0x9E);
        moq_rcbuf_decref(out);
    }

    /* -- LOC-01: video frame marking keyframe byte-level oracle ------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_frame_marking = true;
        h.video_frame_marking.start_of_frame = true;
        h.video_frame_marking.end_of_frame = true;
        h.video_frame_marking.independent = true;

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);

        /* Expected: delta=0x04, value=0xE0.
         * Wire: [0x04] [0x40 0xE0] = 3 bytes.
         * (0xE0 = 224, QUIC varint 2-byte: 0x40E0) */
        const uint8_t *d = moq_rcbuf_data(out);
        size_t len = moq_rcbuf_len(out);
        MOQ_TEST_CHECK_EQ_SIZE(len, 3);
        MOQ_TEST_CHECK_EQ_HEX(d[0], 0x04);
        MOQ_TEST_CHECK_EQ_HEX(d[1], 0x40);
        MOQ_TEST_CHECK_EQ_HEX(d[2], 0xE0);
        moq_rcbuf_decref(out);
    }

    /* -- parse video frame marking from wire bytes -------------------- */
    {
        uint8_t wire[16];
        size_t pos = 0;
        pos += moq_quic_varint_encode(4, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(0xE0, wire + pos, sizeof(wire) - pos);

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) == MOQ_OK);
        MOQ_TEST_CHECK(h.has_video_frame_marking == true);
        MOQ_TEST_CHECK(h.video_frame_marking.start_of_frame == true);
        MOQ_TEST_CHECK(h.video_frame_marking.end_of_frame == true);
        MOQ_TEST_CHECK(h.video_frame_marking.independent == true);
        MOQ_TEST_CHECK(h.video_frame_marking.discardable == false);
        MOQ_TEST_CHECK(h.video_frame_marking.base_layer_sync == false);
        MOQ_TEST_CHECK_EQ_INT(h.video_frame_marking.temporal_id, 0);
        MOQ_TEST_CHECK(h.video_frame_marking.has_layer_id == false);
    }

    /* -- parse video frame marking with layer_id --------------------- */
    {
        uint8_t wire[16];
        size_t pos = 0;
        pos += moq_quic_varint_encode(4, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(0xE900, wire + pos, sizeof(wire) - pos);

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) == MOQ_OK);
        MOQ_TEST_CHECK(h.video_frame_marking.has_layer_id == true);
        MOQ_TEST_CHECK_EQ_INT(h.video_frame_marking.layer_id, 0);
        MOQ_TEST_CHECK(h.video_frame_marking.base_layer_sync == true);
        MOQ_TEST_CHECK_EQ_INT(h.video_frame_marking.temporal_id, 1);
    }

    /* -- parse video config: borrowed pointer into input ------------- */
    {
        uint8_t extradata[] = { 0x01, 0x64, 0x00, 0x1e, 0xff };
        uint8_t wire[32];
        size_t pos = 0;
        pos += moq_quic_varint_encode(0x0d, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(sizeof(extradata), wire + pos,
                                       sizeof(wire) - pos);
        memcpy(wire + pos, extradata, sizeof(extradata));
        pos += sizeof(extradata);

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) == MOQ_OK);
        MOQ_TEST_CHECK(h.has_video_config == true);
        MOQ_TEST_CHECK_EQ_SIZE(h.video_config.len, sizeof(extradata));
        MOQ_TEST_CHECK(memcmp(h.video_config.data, extradata,
                               sizeof(extradata)) == 0);
        MOQ_TEST_CHECK(h.video_config.data >= wire);
        MOQ_TEST_CHECK(h.video_config.data < wire + sizeof(wire));
    }

    /* -- LOC-01: parse all four fields ------------------------------- */
    {
        uint8_t wire[64];
        size_t pos = 0;

        pos += moq_quic_varint_encode(2, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(5000, wire + pos, sizeof(wire) - pos);

        pos += moq_quic_varint_encode(2, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(0xE0, wire + pos, sizeof(wire) - pos);

        pos += moq_quic_varint_encode(2, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(0x9E, wire + pos, sizeof(wire) - pos);

        pos += moq_quic_varint_encode(7, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(2, wire + pos, sizeof(wire) - pos);
        wire[pos++] = 0xDE;
        wire[pos++] = 0xAD;

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(h.timestamp, 5000);
        MOQ_TEST_CHECK(h.video_frame_marking.independent == true);
        MOQ_TEST_CHECK(h.audio_level.voice_activity == true);
        MOQ_TEST_CHECK_EQ_INT(h.audio_level.level, 30);
        MOQ_TEST_CHECK_EQ_SIZE(h.video_config.len, 2);
    }

    /* -- video frame marking value > 0xFFFF returns PROTO ------------- */
    {
        uint8_t wire[16];
        size_t pos = 0;
        pos += moq_quic_varint_encode(4, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(0x10000, wire + pos, sizeof(wire) - pos);

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) == MOQ_ERR_PROTO);
    }

    /* -- audio level value > 0xFF returns PROTO ----------------------- */
    {
        uint8_t wire[16];
        size_t pos = 0;
        pos += moq_quic_varint_encode(6, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(0x100, wire + pos, sizeof(wire) - pos);

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) == MOQ_ERR_PROTO);
    }

    /* -- unknown extension ignored ----------------------------------- */
    {
        uint8_t wire[16];
        size_t pos = 0;
        pos += moq_quic_varint_encode(8, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(42, wire + pos, sizeof(wire) - pos);

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) == MOQ_OK);
        MOQ_TEST_CHECK(h.has_timestamp == false);
    }

    /* -- unknown odd extension ignored ------------------------------- */
    {
        uint8_t wire[16];
        size_t pos = 0;
        pos += moq_quic_varint_encode(9, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(3, wire + pos, sizeof(wire) - pos);
        wire[pos++] = 0x01;
        wire[pos++] = 0x02;
        wire[pos++] = 0x03;

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) == MOQ_OK);
    }

    /* -- duplicate known: last wins ---------------------------------- */
    {
        uint8_t wire[32];
        size_t pos = 0;
        pos += moq_quic_varint_encode(2, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(100, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(0, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(200, wire + pos, sizeof(wire) - pos);

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(h.timestamp, 200);
    }

    /* -- malformed/truncated varint returns error -------------------- */
    {
        uint8_t wire[] = { 0x40 };
        moq_loc_headers_t h;
        moq_bytes_t props = { wire, sizeof(wire) };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) < 0);
    }

    /* -- truncated length-prefixed video config ---------------------- */
    {
        uint8_t wire[16];
        size_t pos = 0;
        pos += moq_quic_varint_encode(0x0d, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(10, wire + pos, sizeof(wire) - pos);
        wire[pos++] = 0x01;
        wire[pos++] = 0x02;

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &h) < 0);
    }

    /* -- encode no fields returns OK + NULL -------------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = (moq_rcbuf_t *)1;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out == NULL);
    }

    /* -- encode NULL args returns INVAL ------------------------------ */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(NULL, D16, P01, &h, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, NULL, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, NULL) == MOQ_ERR_INVAL);
    }

    /* -- encode audio level > 127 returns INVAL ---------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_audio_level = true;
        h.audio_level.level = 128;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_ERR_INVAL);
    }

    /* -- encode temporal_id > 7 returns INVAL ------------------------ */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_frame_marking = true;
        h.video_frame_marking.temporal_id = 8;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_ERR_INVAL);
    }

    /* -- LOC-01: timescale encode returns INVAL ----------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timescale = true;
        h.timescale = 48000;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_ERR_INVAL);
    }

    /* -- video frame marking with layer_id roundtrip ------------------ */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_frame_marking = true;
        h.video_frame_marking.start_of_frame = true;
        h.video_frame_marking.end_of_frame = true;
        h.video_frame_marking.base_layer_sync = true;
        h.video_frame_marking.temporal_id = 2;
        h.video_frame_marking.has_layer_id = true;
        h.video_frame_marking.layer_id = 15;

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);

        moq_loc_headers_t parsed;
        moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &parsed) == MOQ_OK);
        MOQ_TEST_CHECK(parsed.video_frame_marking.has_layer_id == true);
        MOQ_TEST_CHECK_EQ_INT(parsed.video_frame_marking.layer_id, 15);
        moq_rcbuf_decref(out);
    }

    /* -- all fields encode -> parse roundtrip ------------------------- */
    {
        uint8_t config_data[] = { 0x01, 0x64, 0x00, 0x1e };

        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 1746104600000000ULL;
        h.has_video_frame_marking = true;
        h.video_frame_marking.start_of_frame = true;
        h.video_frame_marking.end_of_frame = true;
        h.video_frame_marking.independent = true;
        h.has_audio_level = true;
        h.audio_level.voice_activity = true;
        h.audio_level.level = 30;
        h.has_video_config = true;
        h.video_config.data = config_data;
        h.video_config.len = sizeof(config_data);

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);

        moq_loc_headers_t parsed;
        moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &parsed) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(parsed.timestamp, 1746104600000000ULL);
        MOQ_TEST_CHECK(parsed.video_frame_marking.independent == true);
        MOQ_TEST_CHECK(parsed.audio_level.voice_activity == true);
        MOQ_TEST_CHECK_EQ_INT(parsed.audio_level.level, 30);
        MOQ_TEST_CHECK_EQ_SIZE(parsed.video_config.len, sizeof(config_data));
        moq_rcbuf_decref(out);
    }

    /* -- fields encoded in ascending delta order --------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 42;
        h.has_video_frame_marking = true;
        h.video_frame_marking.start_of_frame = true;
        h.video_frame_marking.end_of_frame = true;
        h.video_frame_marking.independent = true;

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);

        const uint8_t *d = moq_rcbuf_data(out);
        uint64_t v = 0;
        size_t n = moq_quic_varint_decode(d, moq_rcbuf_len(out), &v);
        MOQ_TEST_CHECK(n > 0);
        MOQ_TEST_CHECK_EQ_U64(v, 2);
        moq_rcbuf_decref(out);
    }

    /* -- OOM: stack-buffer path (small encode) ------------------------ */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 100;

        oom_alloc_state_t oom = { 0, 0, 1 };
        moq_alloc_t fail_alloc = oom_allocator(&oom);

        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, D16, P01, &h, &out) == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(out == NULL);
        MOQ_TEST_CHECK(oom.balance == 0);
    }

    /* -- OOM: heap staging path (large video_config > 256 bytes) ------ */
    {
        uint8_t big_config[300];
        memset(big_config, 0xAB, sizeof(big_config));

        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_config = true;
        h.video_config.data = big_config;
        h.video_config.len = sizeof(big_config);

        /* Fail the temp heap allocation (alloc #1). */
        {
            oom_alloc_state_t oom = { 0, 0, 1 };
            moq_alloc_t fail_alloc = oom_allocator(&oom);
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, D16, P01, &h, &out) == MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK(out == NULL);
            MOQ_TEST_CHECK(oom.balance == 0);
        }

        /* Fail the rcbuf allocation (alloc #2) after temp succeeds. */
        {
            oom_alloc_state_t oom = { 0, 0, 2 };
            moq_alloc_t fail_alloc = oom_allocator(&oom);
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, D16, P01, &h, &out) == MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK(out == NULL);
            MOQ_TEST_CHECK(oom.balance == 0);
        }

        /* Success path: large config roundtrips correctly. */
        {
            const moq_alloc_t *alloc = moq_alloc_default();
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);
            MOQ_TEST_CHECK(out != NULL);

            moq_loc_headers_t parsed;
            moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
            MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &parsed) == MOQ_OK);
            MOQ_TEST_CHECK(parsed.has_video_config == true);
            MOQ_TEST_CHECK_EQ_SIZE(parsed.video_config.len, sizeof(big_config));
            MOQ_TEST_CHECK(memcmp(parsed.video_config.data, big_config,
                                   sizeof(big_config)) == 0);
            moq_rcbuf_decref(out);
        }
    }

    /* -- audio level silence roundtrip -------------------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_audio_level = true;
        h.audio_level.voice_activity = false;
        h.audio_level.level = 127;

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);

        moq_loc_headers_t parsed;
        moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &parsed) == MOQ_OK);
        MOQ_TEST_CHECK(parsed.audio_level.voice_activity == false);
        MOQ_TEST_CHECK_EQ_INT(parsed.audio_level.level, 127);
        moq_rcbuf_decref(out);
    }

    /* -- video frame marking LID=255 --------------------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_frame_marking = true;
        h.video_frame_marking.base_layer_sync = true;
        h.video_frame_marking.temporal_id = 2;
        h.video_frame_marking.has_layer_id = true;
        h.video_frame_marking.layer_id = 255;

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);

        moq_loc_headers_t parsed;
        moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &parsed) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(parsed.video_frame_marking.layer_id, 255);
        moq_rcbuf_decref(out);
    }

    /* -- video config alone: byte-level oracle ------------------------ */
    {
        uint8_t cfg[] = { 0xAA, 0xBB, 0xCC };

        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_config = true;
        h.video_config.data = cfg;
        h.video_config.len = sizeof(cfg);

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out != NULL);

        /* Wire: [delta 0x0d] [len 0x03] [AA BB CC] = 5 bytes. */
        const uint8_t *d = moq_rcbuf_data(out);
        MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(out), 5);
        MOQ_TEST_CHECK_EQ_HEX(d[0], 0x0d);
        MOQ_TEST_CHECK_EQ_HEX(d[1], 0x03);
        MOQ_TEST_CHECK(memcmp(d + 2, cfg, sizeof(cfg)) == 0);
        moq_rcbuf_decref(out);
    }

    /* -- emitted length equals the independently planned length -------
     * The encoder measures every present field with the KVP length
     * helpers and then writes exactly that many bytes. Recomputing the
     * plan here pins that equality through the public surface, for each
     * field alone and for the combined encoding. */
    {
        uint8_t cfg[] = { 0x01, 0x02, 0x03, 0x04, 0x05 };

        struct {
            const char *name;
            bool ts, vfm, al, cfg_on;
        } cases[] = {
            { "timestamp",       true,  false, false, false },
            { "frame marking",   false, true,  false, false },
            { "audio level",     false, false, true,  false },
            { "video config",    false, false, false, true  },
            { "ts+vfm",          true,  true,  false, false },
            { "ts+config",       true,  false, false, true  },
            { "vfm+al+config",   false, true,  true,  true  },
            { "all four",        true,  true,  true,  true  },
        };

        for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++) {
            moq_loc_headers_t h;
            moq_loc_headers_init(&h);
            if (cases[c].ts) {
                h.has_timestamp = true;
                h.timestamp = 1746104600000000ULL;
            }
            if (cases[c].vfm) {
                h.has_video_frame_marking = true;
                h.video_frame_marking.start_of_frame = true;
                h.video_frame_marking.independent = true;
                h.video_frame_marking.temporal_id = 3;
            }
            if (cases[c].al) {
                h.has_audio_level = true;
                h.audio_level.voice_activity = true;
                h.audio_level.level = 42;
            }
            if (cases[c].cfg_on) {
                h.has_video_config = true;
                h.video_config.data = cfg;
                h.video_config.len = sizeof(cfg);
            }

            /* Plan the same entries in the same ascending order. */
            size_t planned = 0;
            uint64_t prev = 0;
            if (cases[c].ts) {
                planned += moq_kvp_varint_entry_encoded_len(prev, 0x02,
                                                             h.timestamp);
                prev = 0x02;
            }
            if (cases[c].vfm) {
                /* SOF|independent|tid=3 == 0xA3 */
                planned += moq_kvp_varint_entry_encoded_len(prev, 0x04, 0xA3);
                prev = 0x04;
            }
            if (cases[c].al) {
                planned += moq_kvp_varint_entry_encoded_len(prev, 0x06, 0xAA);
                prev = 0x06;
            }
            if (cases[c].cfg_on) {
                moq_kvp_entry_t ce;
                memset(&ce, 0, sizeof(ce));
                ce.type = 0x0d;
                ce.is_varint = false;
                ce.value = cfg;
                ce.value_len = sizeof(cfg);
                planned += moq_kvp_entry_encoded_len(prev, &ce);
            }
            MOQ_TEST_CHECK(planned > 0);

            const moq_alloc_t *alloc = moq_alloc_default();
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);
            MOQ_TEST_CHECK(out != NULL);
            if (out) {
                MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(out), planned);

                /* Every emitted byte is decodable: nothing beyond the
                 * planned span, and no unwritten tail. */
                moq_kvp_decoder_t dec;
                moq_kvp_decoder_init(&dec, moq_rcbuf_data(out),
                                      moq_rcbuf_len(out));
                moq_kvp_entry_t e;
                size_t seen = 0;
                moq_result_t rc;
                while ((rc = moq_kvp_decode_next(&dec, &e)) == MOQ_OK)
                    seen++;
                MOQ_TEST_CHECK(rc == MOQ_DONE);
                MOQ_TEST_CHECK_EQ_SIZE(seen,
                    (size_t)(cases[c].ts + cases[c].vfm +
                             cases[c].al + cases[c].cfg_on));
                moq_rcbuf_decref(out);
            }
        }
    }

    /* -- stack/heap boundary around the 256-byte scratch buffer -------
     * A config entry encodes as [delta 0x0d][len varint][value]; with a
     * 2-byte length varint that is value_len + 3 bytes on the wire. So
     * 253 bytes of config is exactly 256 (the last stack-path size) and
     * 254 is 257 (the first heap-path size). Both must produce the same
     * byte-exact result. */
    {
        static const size_t kValueLens[] = { 252, 253, 254, 300 };
        static const size_t kExpected[]  = { 255, 256, 257, 303 };

        for (size_t c = 0; c < sizeof(kValueLens) / sizeof(kValueLens[0]); c++) {
            uint8_t cfg[320];
            for (size_t i = 0; i < kValueLens[c]; i++)
                cfg[i] = (uint8_t)(i * 7 + c);

            moq_loc_headers_t h;
            moq_loc_headers_init(&h);
            h.has_video_config = true;
            h.video_config.data = cfg;
            h.video_config.len = kValueLens[c];

            const moq_alloc_t *alloc = moq_alloc_default();
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);
            MOQ_TEST_CHECK(out != NULL);
            if (!out) continue;

            MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(out), kExpected[c]);

            moq_loc_headers_t parsed;
            moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
            MOQ_TEST_CHECK(moq_loc_parse(D16, P01, props, &parsed) == MOQ_OK);
            MOQ_TEST_CHECK(parsed.has_video_config == true);
            MOQ_TEST_CHECK_EQ_SIZE(parsed.video_config.len, kValueLens[c]);
            MOQ_TEST_CHECK(memcmp(parsed.video_config.data, cfg,
                                   kValueLens[c]) == 0);
            moq_rcbuf_decref(out);
        }
    }

    /* -- allocator failure at the first heap size (257 bytes) --------- */
    {
        uint8_t cfg[254];
        memset(cfg, 0x5A, sizeof(cfg));

        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_config = true;
        h.video_config.data = cfg;
        h.video_config.len = sizeof(cfg);

        /* The staging allocation is alloc #1 on this path. */
        {
            oom_alloc_state_t oom = { 0, 0, 1 };
            moq_alloc_t fail_alloc = oom_allocator(&oom);
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, D16, P01, &h, &out)
                           == MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK(out == NULL);
            MOQ_TEST_CHECK(oom.balance == 0);
        }

        /* The rcbuf allocation is alloc #2; the staging buffer is freed. */
        {
            oom_alloc_state_t oom = { 0, 0, 2 };
            moq_alloc_t fail_alloc = oom_allocator(&oom);
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, D16, P01, &h, &out)
                           == MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK(out == NULL);
            MOQ_TEST_CHECK(oom.balance == 0);
        }
    }

    /* -- allocator failure on the last stack size (256 bytes) ---------
     * Only the rcbuf allocates here, so alloc #1 is the rcbuf itself. */
    {
        uint8_t cfg[253];
        memset(cfg, 0x33, sizeof(cfg));

        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_config = true;
        h.video_config.data = cfg;
        h.video_config.len = sizeof(cfg);

        oom_alloc_state_t oom = { 0, 0, 1 };
        moq_alloc_t fail_alloc = oom_allocator(&oom);
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, D16, P01, &h, &out)
                       == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(out == NULL);
        MOQ_TEST_CHECK(oom.balance == 0);
    }

    /* -- invalid video config rejected before any buffer is used ------ */
    {
        const moq_alloc_t *alloc = moq_alloc_default();

        /* Non-empty span with a NULL pointer. */
        {
            moq_loc_headers_t h;
            moq_loc_headers_init(&h);
            h.has_video_config = true;
            h.video_config.data = NULL;
            h.video_config.len = 4;
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out)
                           == MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(out == NULL);
        }

        /* Value length beyond the KVP odd-type maximum (2^16 - 1). */
        {
            static uint8_t big[70000];
            moq_loc_headers_t h;
            moq_loc_headers_init(&h);
            h.has_video_config = true;
            h.video_config.data = big;
            h.video_config.len = sizeof(big);
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out)
                           == MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(out == NULL);
        }
    }

    /* -- an empty video config still produces a property -------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_config = true;
        h.video_config.data = NULL;
        h.video_config.len = 0;

        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out != NULL);
        if (out) {
            /* Wire: [delta 0x0d] [len 0x00] = 2 bytes. */
            MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(out), 2);
            MOQ_TEST_CHECK_EQ_HEX(moq_rcbuf_data(out)[0], 0x0d);
            MOQ_TEST_CHECK_EQ_HEX(moq_rcbuf_data(out)[1], 0x00);
            moq_rcbuf_decref(out);
        }
    }

    /* -- Differential vectors: the codec the transport version selects -
     *
     * Every expected byte string below is derived from the DRAFTS, not
     * from the implementation:
     *
     *   draft-ietf-moq-transport-16 section 1.4  -- (i) is the RFC 9000
     *     QUIC variable-length integer: a 2-bit length prefix selecting
     *     1/2/4/8 bytes, maximum 2^62-1;
     *   draft-ietf-moq-transport-18 section 1.4.1 -- (vi64) is MOQT's own
     *     encoding: the count of leading 1 bits of the first byte selects
     *     1..9 bytes, maximum 2^64-1;
     *   both, sections 1.4.2 / 1.4.3 -- Key-Value-Pair { Delta Type,
     *     [Length when the absolute Type is odd,] Value }, where an even
     *     Type's Value is a single integer in that same encoding.
     *
     * A LOC-01 Capture Timestamp is property 0x02, so as the first entry
     * its Delta Type is 2 -- one byte in both drafts. Only the VALUE
     * differs, which isolates the codec.
     */
    {
        const moq_alloc_t *alloc = moq_alloc_default();

        struct ts_vec {
            uint64_t      value;
            const char   *name;
            size_t        d16_len;      /* 0 = draft-16 cannot represent it */
            const uint8_t d16[10];
            size_t        d18_len;
            const uint8_t d18[10];
        };

        /* 63/64 straddle the one-byte boundary the two encodings share;
         * 127/128 straddle vi64's, and 16383/16384 vi64's next one; 33333
         * is the value the closure directions use; then each encoding's
         * own ceiling. */
        static const struct ts_vec vecs[] = {
            { 0,     "0",
              2, { 0x02, 0x00 },
              2, { 0x02, 0x00 } },
            { 63,    "63 (last shared byte)",
              2, { 0x02, 0x3f },
              2, { 0x02, 0x3f } },
            { 64,    "64 (first divergence)",
              3, { 0x02, 0x40, 0x40 },
              2, { 0x02, 0x40 } },
            { 127,   "127",
              3, { 0x02, 0x40, 0x7f },
              2, { 0x02, 0x7f } },
            { 128,   "128",
              3, { 0x02, 0x40, 0x80 },
              3, { 0x02, 0x80, 0x80 } },
            { 16383, "16383",
              3, { 0x02, 0x7f, 0xff },
              3, { 0x02, 0xbf, 0xff } },
            { 16384, "16384",
              5, { 0x02, 0x80, 0x00, 0x40, 0x00 },
              4, { 0x02, 0xc0, 0x40, 0x00 } },
            { 33333, "33333 (the closure vector)",
              5, { 0x02, 0x80, 0x00, 0x82, 0x35 },
              4, { 0x02, 0xc0, 0x82, 0x35 } },
            { MOQ_QUIC_VARINT_MAX, "2^62-1 (draft-16 ceiling)",
              9, { 0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff },
              10, { 0x02, 0xff, 0x3f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff } },
            { UINT64_MAX, "2^64-1 (beyond draft-16 entirely)",
              0, { 0 },
              10, { 0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff } },
        };

        for (size_t i = 0; i < sizeof(vecs) / sizeof(vecs[0]); i++) {
            const struct ts_vec *v = &vecs[i];
            const moq_version_t versions[2] = { D16, D18 };
            for (size_t k = 0; k < 2; k++) {
                moq_loc_headers_t h;
                moq_loc_headers_init(&h);
                h.has_timestamp = true;
                h.timestamp = v->value;

                size_t want_len = k ? v->d18_len : v->d16_len;
                const uint8_t *want = k ? v->d18 : v->d16;

                moq_rcbuf_t *out = NULL;
                moq_result_t rc = moq_loc_encode(alloc, versions[k], P01,
                                                 &h, &out);
                if (want_len == 0) {
                    /* Unrepresentable in the selected codec: refused
                     * outright, never truncated into a wrong value. */
                    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
                    MOQ_TEST_CHECK(out == NULL);
                    continue;
                }
                MOQ_TEST_CHECK(rc == MOQ_OK);
                MOQ_TEST_CHECK(out != NULL);
                if (!out) continue;
                if (moq_rcbuf_len(out) != want_len ||
                    memcmp(moq_rcbuf_data(out), want, want_len) != 0) {
                    fprintf(stderr,
                        "FAIL: timestamp %s under draft-%d: got", v->name,
                        versions[k] == D16 ? 16 : 18);
                    for (size_t b = 0; b < moq_rcbuf_len(out); b++)
                        fprintf(stderr, " %02x", moq_rcbuf_data(out)[b]);
                    fprintf(stderr, ", the draft requires");
                    for (size_t b = 0; b < want_len; b++)
                        fprintf(stderr, " %02x", want[b]);
                    fprintf(stderr, "\n");
                    failures++;
                }

                /* Round trip under the SAME version recovers the value
                 * exactly -- the encoder and the walker agree. */
                moq_loc_headers_t back;
                moq_bytes_t span = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
                MOQ_TEST_CHECK(moq_loc_parse(versions[k], P01, span, &back)
                               == MOQ_OK);
                MOQ_TEST_CHECK(back.has_timestamp);
                MOQ_TEST_CHECK_EQ_U64(back.timestamp, v->value);
                moq_rcbuf_decref(out);
            }
        }
    }

    /* -- Odd-type Length is encoded by the same codec ------------------
     *
     * Video Config is property 0x0d -- ODD -- so section 1.4.2/1.4.3 give
     * the entry a Length field, and that Length is the draft's integer
     * too. A 200-byte value puts the Length above 63, where the two
     * encodings diverge, so this covers the Length field specifically
     * rather than only even values. */
    {
        const moq_alloc_t *alloc = moq_alloc_default();
        static uint8_t cfg[200];
        for (size_t i = 0; i < sizeof(cfg); i++) cfg[i] = (uint8_t)i;

        struct { moq_version_t v; uint8_t len0, len1; } cases[] = {
            { D16, 0x40, 0xc8 },   /* QUIC varint 200: 2-byte form 0x40 0xc8 */
            { D18, 0x80, 0xc8 },   /* vi64 200:        2-byte form 0x80 0xc8 */
        };
        for (size_t k = 0; k < 2; k++) {
            moq_loc_headers_t h;
            moq_loc_headers_init(&h);
            h.has_video_config = true;
            h.video_config.data = cfg;
            h.video_config.len = sizeof(cfg);

            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, cases[k].v, P01, &h, &out)
                           == MOQ_OK);
            if (!out) continue;
            /* [delta 0x0d][Length][200 bytes] */
            MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(out), 3 + sizeof(cfg));
            MOQ_TEST_CHECK_EQ_HEX(moq_rcbuf_data(out)[0], 0x0d);
            MOQ_TEST_CHECK_EQ_HEX(moq_rcbuf_data(out)[1], cases[k].len0);
            MOQ_TEST_CHECK_EQ_HEX(moq_rcbuf_data(out)[2], cases[k].len1);
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(out) + 3, cfg,
                                  sizeof(cfg)) == 0);

            moq_loc_headers_t back;
            moq_bytes_t span = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
            MOQ_TEST_CHECK(moq_loc_parse(cases[k].v, P01, span, &back)
                           == MOQ_OK);
            MOQ_TEST_CHECK(back.has_video_config);
            MOQ_TEST_CHECK_EQ_SIZE(back.video_config.len, sizeof(cfg));
            moq_rcbuf_decref(out);
        }
    }

    /* -- Type deltas are encoded by the same codec --------------------
     *
     * Two properties in ascending order: Capture Timestamp (0x02) then
     * Video Config (0x0d), so the second entry's Delta Type is 11. Both
     * drafts spell 11 in one byte, so the DELTA is held constant here
     * while the timestamp value straddles the divergence -- the encoded
     * blocks must therefore differ only where the value does. */
    {
        const moq_alloc_t *alloc = moq_alloc_default();
        static const uint8_t cfg[3] = { 0xaa, 0xbb, 0xcc };
        /* Timestamp 33333, then the odd Video Config entry. */
        static const uint8_t want_d16[] = {
            0x02, 0x80, 0x00, 0x82, 0x35,
            0x0b, 0x03, 0xaa, 0xbb, 0xcc,
        };
        static const uint8_t want_d18[] = {
            0x02, 0xc0, 0x82, 0x35,
            0x0b, 0x03, 0xaa, 0xbb, 0xcc,
        };
        struct { moq_version_t v; const uint8_t *want; size_t len; } cases[] = {
            { D16, want_d16, sizeof(want_d16) },
            { D18, want_d18, sizeof(want_d18) },
        };
        for (size_t k = 0; k < 2; k++) {
            moq_loc_headers_t h;
            moq_loc_headers_init(&h);
            h.has_timestamp = true;
            h.timestamp = 33333;
            h.has_video_config = true;
            h.video_config.data = cfg;
            h.video_config.len = sizeof(cfg);

            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, cases[k].v, P01, &h, &out)
                           == MOQ_OK);
            if (!out) continue;
            MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(out), cases[k].len);
            MOQ_TEST_CHECK(moq_rcbuf_len(out) == cases[k].len &&
                           memcmp(moq_rcbuf_data(out), cases[k].want,
                                  cases[k].len) == 0);
            moq_rcbuf_decref(out);
        }
    }

    /* -- Cross-family parse: the failure the version exists to prevent -
     *
     * Draft-16 bytes read with the draft-18 codec, and the reverse. Below
     * 64 the two agree and the value survives -- that is the shared-band
     * control, and a difference there would mean the fixture was wrong.
     * At or above 64 the read desynchronizes: a value-level check alone
     * would see nothing, so what is asserted is that the WRONG codec does
     * not silently reproduce the right value. */
    {
        const moq_alloc_t *alloc = moq_alloc_default();

        /* Shared band: identical bytes, so either codec reads it. */
        {
            moq_loc_headers_t h;
            moq_loc_headers_init(&h);
            h.has_timestamp = true;
            h.timestamp = 33;
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);
            if (out) {
                moq_bytes_t span = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
                moq_loc_headers_t a, b;
                MOQ_TEST_CHECK(moq_loc_parse(D16, P01, span, &a) == MOQ_OK);
                MOQ_TEST_CHECK(moq_loc_parse(D18, P01, span, &b) == MOQ_OK);
                MOQ_TEST_CHECK(a.has_timestamp && b.has_timestamp);
                MOQ_TEST_CHECK_EQ_U64(a.timestamp, 33u);
                MOQ_TEST_CHECK_EQ_U64(b.timestamp, 33u);
                moq_rcbuf_decref(out);
            }
        }

        /* Divergent band, both directions. */
        static const uint64_t divergent[] = { 64, 127, 128, 16384, 33333 };
        for (size_t i = 0; i < sizeof(divergent) / sizeof(divergent[0]); i++) {
            const moq_version_t pairs[2][2] = {
                { D16, D18 },   /* written draft-16, read draft-18 */
                { D18, D16 },   /* written draft-18, read draft-16 */
            };
            for (size_t k = 0; k < 2; k++) {
                moq_loc_headers_t h;
                moq_loc_headers_init(&h);
                h.has_timestamp = true;
                h.timestamp = divergent[i];

                moq_rcbuf_t *out = NULL;
                MOQ_TEST_CHECK(moq_loc_encode(alloc, pairs[k][0], P01, &h,
                                              &out) == MOQ_OK);
                if (!out) continue;
                moq_bytes_t span = { moq_rcbuf_data(out), moq_rcbuf_len(out) };

                /* The right codec always recovers the value. */
                moq_loc_headers_t good;
                MOQ_TEST_CHECK(moq_loc_parse(pairs[k][0], P01, span, &good)
                               == MOQ_OK);
                MOQ_TEST_CHECK(good.has_timestamp);
                MOQ_TEST_CHECK_EQ_U64(good.timestamp, divergent[i]);

                /* The wrong one must NOT reproduce it. It may reject the
                 * block or report some other value -- either is a visible
                 * mismatch -- but agreement would mean the codec never
                 * mattered and this whole axis was untested. */
                moq_loc_headers_t bad;
                moq_result_t rc = moq_loc_parse(pairs[k][1], P01, span, &bad);
                bool reproduced = (rc == MOQ_OK) && bad.has_timestamp &&
                                  bad.timestamp == divergent[i];
                if (reproduced) {
                    fprintf(stderr,
                        "FAIL: timestamp %llu written for draft-%d was "
                        "reproduced exactly by the draft-%d codec; the two "
                        "encodings must not agree here\n",
                        (unsigned long long)divergent[i],
                        pairs[k][0] == D16 ? 16 : 18,
                        pairs[k][1] == D16 ? 16 : 18);
                    failures++;
                }
                moq_rcbuf_decref(out);
            }
        }
    }

    /* -- Multiproperty desynchronization ------------------------------
     *
     * A two-property block written for draft-16 with a timestamp in the
     * divergent band. Read with the draft-18 codec the first value
     * consumes the wrong number of bytes, so everything after it is
     * misframed: the SECOND property must not survive intact. This is the
     * case a single-property test cannot show, because a lone entry can
     * desync and still leave the block "well formed" by accident. */
    {
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 33333;
        h.has_video_frame_marking = true;
        h.video_frame_marking.independent = true;

        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P01, &h, &out) == MOQ_OK);
        if (out) {
            moq_bytes_t span = { moq_rcbuf_data(out), moq_rcbuf_len(out) };

            moq_loc_headers_t good;
            MOQ_TEST_CHECK(moq_loc_parse(D16, P01, span, &good) == MOQ_OK);
            MOQ_TEST_CHECK(good.has_timestamp);
            MOQ_TEST_CHECK_EQ_U64(good.timestamp, 33333u);
            MOQ_TEST_CHECK(good.has_video_frame_marking);
            MOQ_TEST_CHECK(good.video_frame_marking.independent);

            moq_loc_headers_t bad;
            moq_result_t rc = moq_loc_parse(D18, P01, span, &bad);
            bool both_survived = (rc == MOQ_OK) &&
                                 bad.has_timestamp &&
                                 bad.timestamp == 33333u &&
                                 bad.has_video_frame_marking &&
                                 bad.video_frame_marking.independent;
            if (both_survived) {
                fprintf(stderr,
                    "FAIL: a draft-16 two-property block was read intact by "
                    "the draft-18 codec; the block cannot be codec-agnostic "
                    "with a value of 33333\n");
                failures++;
            }
            moq_rcbuf_decref(out);
        }
    }

    /* -- Unknown and unregistered versions fail closed ----------------- */
    {
        const moq_alloc_t *alloc = moq_alloc_default();
        static const moq_version_t bad_versions[] = {
            (moq_version_t)0, (moq_version_t)1, (moq_version_t)15,
            (moq_version_t)17, (moq_version_t)19, (moq_version_t)0xffff,
        };
        for (size_t i = 0;
             i < sizeof(bad_versions) / sizeof(bad_versions[0]); i++) {
            moq_loc_headers_t h;
            moq_loc_headers_init(&h);
            h.has_timestamp = true;
            h.timestamp = 33;

            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, bad_versions[i], P01, &h,
                                          &out) == MOQ_ERR_INVAL);
            MOQ_TEST_CHECK(out == NULL);

            /* An EMPTY property block is refused too: a caller that cannot
             * name its codec is refused whether or not there is anything
             * to decode. */
            moq_loc_headers_t parsed;
            moq_bytes_t empty = { NULL, 0 };
            MOQ_TEST_CHECK(moq_loc_parse(bad_versions[i], P01, empty,
                                         &parsed) == MOQ_ERR_INVAL);

            static const uint8_t bytes[] = { 0x02, 0x21 };
            moq_bytes_t span = { bytes, sizeof(bytes) };
            MOQ_TEST_CHECK(moq_loc_parse(bad_versions[i], P01, span,
                                         &parsed) == MOQ_ERR_INVAL);
        }
    }

    /* -- Draft 21 carries LOC-04 ids (MoQT 21 16.8, draft-ietf-moq-loc-04 2.3) ---- */
    {
        const moq_version_t D21 = MOQ_VERSION_DRAFT_21;
        const moq_loc_profile_t P04 = MOQ_LOC_PROFILE_04;
        MOQ_TEST_CHECK(moq_loc_profile_for_transport(D16) == P01);
        MOQ_TEST_CHECK(moq_loc_profile_for_transport(D18) == P01);
        MOQ_TEST_CHECK(moq_loc_profile_for_transport(D21) == P04);
        MOQ_TEST_CHECK((int)moq_loc_profile_for_transport((moq_version_t)19) == 0);

        moq_alloc_t a = *moq_alloc_default();
        const moq_alloc_t *alloc = &a;
        moq_rcbuf_t *out = NULL;
        moq_loc_headers_t h;

        /* Timestamp is id 0x10 (even: no length), a vi64 value. */
        moq_loc_headers_init(&h);
        h.has_timestamp = true; h.timestamp = 5;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D21, P04, &h, &out) == MOQ_OK && out);
        { static const uint8_t want[] = { 0x10, 0x05 };
          MOQ_TEST_CHECK(moq_rcbuf_len(out) == sizeof(want) && memcmp(moq_rcbuf_data(out), want, sizeof(want)) == 0); }
        moq_rcbuf_decref(out); out = NULL;
        h.timestamp = 300;                         /* two-byte vi64 0x812C */
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D21, P04, &h, &out) == MOQ_OK);
        { static const uint8_t want[] = { 0x10, 0x81, 0x2C };
          MOQ_TEST_CHECK(moq_rcbuf_len(out) == sizeof(want) && memcmp(moq_rcbuf_data(out), want, sizeof(want)) == 0); }
        moq_rcbuf_decref(out); out = NULL;

        /* Frame marking is id 0x09 (odd): a length-prefixed RFC 9626 byte string,
         * with the layer id as a second byte. S|I|TID 1 = 0xA1. */
        moq_loc_headers_init(&h);
        h.has_video_frame_marking = true;
        h.video_frame_marking.start_of_frame = true;
        h.video_frame_marking.independent = true;
        h.video_frame_marking.temporal_id = 1;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D21, P04, &h, &out) == MOQ_OK);
        { static const uint8_t want[] = { 0x09, 0x01, 0xA1 };
          MOQ_TEST_CHECK(moq_rcbuf_len(out) == sizeof(want) && memcmp(moq_rcbuf_data(out), want, sizeof(want)) == 0); }
        moq_rcbuf_decref(out); out = NULL;
        h.video_frame_marking.has_layer_id = true; h.video_frame_marking.layer_id = 3;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D21, P04, &h, &out) == MOQ_OK);
        { static const uint8_t want[] = { 0x09, 0x02, 0xA1, 0x03 };
          MOQ_TEST_CHECK(moq_rcbuf_len(out) == sizeof(want) && memcmp(moq_rcbuf_data(out), want, sizeof(want)) == 0); }
        moq_rcbuf_decref(out); out = NULL;

        /* Several fields come out in ascending id order, not field order:
         * Timescale 0x08 (1000 = 0x83E8), Audio Level 0x0C (voice+5 = 0x85, two-byte
         * vi64 0x8085), Timestamp 0x10. Deltas 8, 4, 4. */
        moq_loc_headers_init(&h);
        h.has_timestamp = true; h.timestamp = 5;
        h.has_timescale = true; h.timescale = 1000;
        h.has_audio_level = true; h.audio_level.voice_activity = true; h.audio_level.level = 5;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D21, P04, &h, &out) == MOQ_OK);
        static const uint8_t all[] = { 0x08, 0x83, 0xE8, 0x04, 0x80, 0x85, 0x04, 0x05 };
        MOQ_TEST_CHECK(moq_rcbuf_len(out) == sizeof(all) && memcmp(moq_rcbuf_data(out), all, sizeof(all)) == 0);
        moq_rcbuf_decref(out); out = NULL;

        /* ... and parse reads them back. */
        moq_loc_headers_t p;
        moq_bytes_t span = { all, sizeof(all) };
        MOQ_TEST_CHECK(moq_loc_parse(D21, P04, span, &p) == MOQ_OK);
        MOQ_TEST_CHECK(p.has_timestamp && p.timestamp == 5 && p.has_timescale && p.timescale == 1000 &&
                       p.has_audio_level && p.audio_level.voice_activity && p.audio_level.level == 5);
        static const uint8_t fm[] = { 0x09, 0x02, 0xA1, 0x03 };
        span = (moq_bytes_t){ fm, sizeof(fm) };
        MOQ_TEST_CHECK(moq_loc_parse(D21, P04, span, &p) == MOQ_OK);
        MOQ_TEST_CHECK(p.has_video_frame_marking && p.video_frame_marking.start_of_frame &&
                       p.video_frame_marking.independent && p.video_frame_marking.temporal_id == 1 &&
                       p.video_frame_marking.has_layer_id && p.video_frame_marking.layer_id == 3);
        /* An empty or over-long frame marking is malformed. */
        static const uint8_t fm0[] = { 0x09, 0x00 };
        span = (moq_bytes_t){ fm0, sizeof(fm0) };
        MOQ_TEST_CHECK(moq_loc_parse(D21, P04, span, &p) == MOQ_ERR_PROTO);
        /* LOC-01's timestamp id (0x02) means nothing under LOC-04: skipped. */
        static const uint8_t old_ts[] = { 0x02, 0x05 };
        span = (moq_bytes_t){ old_ts, sizeof(old_ts) };
        MOQ_TEST_CHECK(moq_loc_parse(D21, P04, span, &p) == MOQ_OK && !p.has_timestamp);

        /* A profile is only valid on its own draft: fail closed, never emit another
         * draft's ids. */
        moq_loc_headers_init(&h);
        h.has_timestamp = true; h.timestamp = 5;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D21, P01, &h, &out) == MOQ_ERR_INVAL && !out);
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D18, P04, &h, &out) == MOQ_ERR_INVAL && !out);
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D16, P04, &h, &out) == MOQ_ERR_INVAL && !out);
        MOQ_TEST_CHECK(moq_loc_parse(D21, P01, span, &p) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_loc_parse(D18, P04, span, &p) == MOQ_ERR_INVAL);
        /* LOC-01 under a draft it was reviewed for is unchanged: timestamp id 0x02. */
        MOQ_TEST_CHECK(moq_loc_encode(alloc, D18, P01, &h, &out) == MOQ_OK);
        { static const uint8_t want[] = { 0x02, 0x05 };
          MOQ_TEST_CHECK(moq_rcbuf_len(out) == sizeof(want) && memcmp(moq_rcbuf_data(out), want, sizeof(want)) == 0); }
        moq_rcbuf_decref(out); out = NULL;
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
