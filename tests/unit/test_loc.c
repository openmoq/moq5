#include <moq/loc.h>
#include <moq/kvp.h>
#include <moq/wire.h>
#include <moq/rcbuf.h>
#include "test_support.h"
#include "test_oom_support.h"
#include <string.h>

#define P01 MOQ_LOC_PROFILE_01
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
        MOQ_TEST_CHECK(moq_loc_parse(P01, empty, &h) == MOQ_OK);
        MOQ_TEST_CHECK(h.has_timestamp == false);
    }

    /* -- parse NULL out returns INVAL -------------------------------- */
    {
        moq_bytes_t empty = { NULL, 0 };
        MOQ_TEST_CHECK(moq_loc_parse(P01, empty, NULL) == MOQ_ERR_INVAL);
    }

    /* -- NULL data with nonzero len returns INVAL -------------------- */
    {
        moq_loc_headers_t h;
        moq_bytes_t bad = { NULL, 5 };
        MOQ_TEST_CHECK(moq_loc_parse(P01, bad, &h) == MOQ_ERR_INVAL);
    }

    /* -- invalid profile parse returns INVAL ------------------------- */
    {
        moq_loc_headers_t h;
        moq_bytes_t empty = { NULL, 0 };
        MOQ_TEST_CHECK(moq_loc_parse((moq_loc_profile_t)0, empty, &h) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_loc_parse((moq_loc_profile_t)99, empty, &h) == MOQ_ERR_INVAL);
    }

    /* -- invalid profile encode returns INVAL ------------------------ */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 1;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, (moq_loc_profile_t)0, &h, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == NULL);
        MOQ_TEST_CHECK(moq_loc_encode(alloc, (moq_loc_profile_t)99, &h, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(out == NULL);
    }

    /* -- LOC-02 parse returns INVAL (D18 codec not available) --------- */
    {
        moq_loc_headers_t h;
        moq_bytes_t empty = { NULL, 0 };
        MOQ_TEST_CHECK(moq_loc_parse(P02, empty, &h) == MOQ_ERR_INVAL);
    }

    /* -- LOC-02 encode returns INVAL --------------------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 48000;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P02, &h, &out) == MOQ_ERR_INVAL);
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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &parsed) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);

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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);

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
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) == MOQ_ERR_PROTO);
    }

    /* -- audio level value > 0xFF returns PROTO ----------------------- */
    {
        uint8_t wire[16];
        size_t pos = 0;
        pos += moq_quic_varint_encode(6, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(0x100, wire + pos, sizeof(wire) - pos);

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) == MOQ_ERR_PROTO);
    }

    /* -- unknown extension ignored ----------------------------------- */
    {
        uint8_t wire[16];
        size_t pos = 0;
        pos += moq_quic_varint_encode(8, wire + pos, sizeof(wire) - pos);
        pos += moq_quic_varint_encode(42, wire + pos, sizeof(wire) - pos);

        moq_loc_headers_t h;
        moq_bytes_t props = { wire, pos };
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(h.timestamp, 200);
    }

    /* -- malformed/truncated varint returns error -------------------- */
    {
        uint8_t wire[] = { 0x40 };
        moq_loc_headers_t h;
        moq_bytes_t props = { wire, sizeof(wire) };
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) < 0);
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
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &h) < 0);
    }

    /* -- encode no fields returns OK + NULL -------------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = (moq_rcbuf_t *)1;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out == NULL);
    }

    /* -- encode NULL args returns INVAL ------------------------------ */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(NULL, P01, &h, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, NULL, &out) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, NULL) == MOQ_ERR_INVAL);
    }

    /* -- encode audio level > 127 returns INVAL ---------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_audio_level = true;
        h.audio_level.level = 128;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_ERR_INVAL);
    }

    /* -- encode temporal_id > 7 returns INVAL ------------------------ */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_video_frame_marking = true;
        h.video_frame_marking.temporal_id = 8;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_ERR_INVAL);
    }

    /* -- LOC-01: timescale encode returns INVAL ----------------------- */
    {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timescale = true;
        h.timescale = 48000;
        const moq_alloc_t *alloc = moq_alloc_default();
        moq_rcbuf_t *out = NULL;
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_ERR_INVAL);
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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);

        moq_loc_headers_t parsed;
        moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &parsed) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);

        moq_loc_headers_t parsed;
        moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &parsed) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);

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
        MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, P01, &h, &out) == MOQ_ERR_NOMEM);
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
            MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, P01, &h, &out) == MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK(out == NULL);
            MOQ_TEST_CHECK(oom.balance == 0);
        }

        /* Fail the rcbuf allocation (alloc #2) after temp succeeds. */
        {
            oom_alloc_state_t oom = { 0, 0, 2 };
            moq_alloc_t fail_alloc = oom_allocator(&oom);
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, P01, &h, &out) == MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK(out == NULL);
            MOQ_TEST_CHECK(oom.balance == 0);
        }

        /* Success path: large config roundtrips correctly. */
        {
            const moq_alloc_t *alloc = moq_alloc_default();
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);
            MOQ_TEST_CHECK(out != NULL);

            moq_loc_headers_t parsed;
            moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
            MOQ_TEST_CHECK(moq_loc_parse(P01, props, &parsed) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);

        moq_loc_headers_t parsed;
        moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &parsed) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);

        moq_loc_headers_t parsed;
        moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
        MOQ_TEST_CHECK(moq_loc_parse(P01, props, &parsed) == MOQ_OK);
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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);
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
            MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);
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
            MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);
            MOQ_TEST_CHECK(out != NULL);
            if (!out) continue;

            MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(out), kExpected[c]);

            moq_loc_headers_t parsed;
            moq_bytes_t props = { moq_rcbuf_data(out), moq_rcbuf_len(out) };
            MOQ_TEST_CHECK(moq_loc_parse(P01, props, &parsed) == MOQ_OK);
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
            MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, P01, &h, &out)
                           == MOQ_ERR_NOMEM);
            MOQ_TEST_CHECK(out == NULL);
            MOQ_TEST_CHECK(oom.balance == 0);
        }

        /* The rcbuf allocation is alloc #2; the staging buffer is freed. */
        {
            oom_alloc_state_t oom = { 0, 0, 2 };
            moq_alloc_t fail_alloc = oom_allocator(&oom);
            moq_rcbuf_t *out = NULL;
            MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, P01, &h, &out)
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
        MOQ_TEST_CHECK(moq_loc_encode(&fail_alloc, P01, &h, &out)
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
            MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out)
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
            MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out)
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
        MOQ_TEST_CHECK(moq_loc_encode(alloc, P01, &h, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out != NULL);
        if (out) {
            /* Wire: [delta 0x0d] [len 0x00] = 2 bytes. */
            MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(out), 2);
            MOQ_TEST_CHECK_EQ_HEX(moq_rcbuf_data(out)[0], 0x0d);
            MOQ_TEST_CHECK_EQ_HEX(moq_rcbuf_data(out)[1], 0x00);
            moq_rcbuf_decref(out);
        }
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
