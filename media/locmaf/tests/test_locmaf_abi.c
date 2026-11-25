/*
 * The ABI of <moq/locmaf.h>: layout pins (locmaf_layout_pins.h), and the
 * one-extent rule of every record and cfg at the public boundary: below the
 * extent the call refuses with MOQ_ERR_INVAL before reading or writing any
 * field; at the extent it works; a larger caller record is accepted and its
 * tail is neither read nor written.
 */
#include "locmaf_fixtures.h"
#include "locmaf_layout_pins.h"

#define POISON 0xA5

typedef union {
    unsigned char raw[1024];
    uint64_t      align;
} lm_canvas_t;

static bool all_poison(const unsigned char *p, size_t n)
{
    for (size_t i = 0; i < n; i++)
        if (p[i] != POISON) return false;
    return true;
}

static void test_init_sized(void)
{
    /* track */
    {
        lm_canvas_t c;
        memset(c.raw, POISON, sizeof(c.raw));
        moq_locmaf_track_init_sized((moq_locmaf_track_t *)c.raw, MOQ_LOCMAF_TRACK_V0_SIZE - 1);
        CHECK(all_poison(c.raw, sizeof(c.raw)));
        moq_locmaf_track_init_sized((moq_locmaf_track_t *)c.raw, MOQ_LOCMAF_TRACK_V0_SIZE + 64);
        moq_locmaf_track_t t;
        memcpy(&t, c.raw, sizeof(t));
        CHECK_EQ(t.struct_size, sizeof(moq_locmaf_track_t));
        CHECK(t.track_id == 0 && !t.is_protected && t._reserved[0] == 0);
        CHECK(all_poison(c.raw + sizeof(t), 64));
        moq_locmaf_track_init_sized(NULL, sizeof(t));
    }
    /* decoder and encoder cfgs: limits zero select the defaults */
    {
        lm_canvas_t c;
        memset(c.raw, POISON, sizeof(c.raw));
        moq_locmaf_decoder_cfg_init_sized((moq_locmaf_decoder_cfg_t *)c.raw,
                                          MOQ_LOCMAF_DECODER_CFG_V0_SIZE - 1);
        CHECK(all_poison(c.raw, sizeof(c.raw)));
        moq_locmaf_decoder_cfg_init_sized((moq_locmaf_decoder_cfg_t *)c.raw,
                                          MOQ_LOCMAF_DECODER_CFG_V0_SIZE + 64);
        moq_locmaf_decoder_cfg_t cfg;
        memcpy(&cfg, c.raw, sizeof(cfg));
        CHECK_EQ(cfg.struct_size, sizeof(cfg));
        CHECK(cfg.max_samples == 0 && cfg.track == NULL && cfg.storage == NULL &&
              cfg.storage_size == 0);
        CHECK(all_poison(c.raw + sizeof(cfg), 64));

        memset(c.raw, POISON, sizeof(c.raw));
        moq_locmaf_encoder_cfg_init_sized((moq_locmaf_encoder_cfg_t *)c.raw,
                                          MOQ_LOCMAF_ENCODER_CFG_V0_SIZE - 1);
        CHECK(all_poison(c.raw, sizeof(c.raw)));
        moq_locmaf_encoder_cfg_init_sized((moq_locmaf_encoder_cfg_t *)c.raw,
                                          MOQ_LOCMAF_ENCODER_CFG_V0_SIZE + 64);
        moq_locmaf_encoder_cfg_t ecfg;
        memcpy(&ecfg, c.raw, sizeof(ecfg));
        CHECK_EQ(ecfg.struct_size, sizeof(ecfg));
        CHECK(all_poison(c.raw + sizeof(ecfg), 64));
        moq_locmaf_decoder_cfg_init_sized(NULL, sizeof(cfg));
        moq_locmaf_encoder_cfg_init_sized(NULL, sizeof(ecfg));
    }
}

/* A cfg record in a canvas: struct_size as given, the tail poisoned. */
static void put_decoder_cfg(lm_canvas_t *c, uint32_t struct_size,
                            const moq_locmaf_track_t *track, void *storage,
                            size_t storage_size)
{
    memset(c->raw, POISON, sizeof(c->raw));
    moq_locmaf_decoder_cfg_t cfg;
    moq_locmaf_decoder_cfg_init(&cfg);
    cfg.struct_size = struct_size;
    cfg.track = track;
    cfg.storage = storage;
    cfg.storage_size = storage_size;
    size_t n = struct_size < sizeof(cfg) ? struct_size : sizeof(cfg);
    memcpy(c->raw, &cfg, n);
}

static void put_encoder_cfg(lm_canvas_t *c, uint32_t struct_size,
                            const moq_locmaf_track_t *track, void *storage,
                            size_t storage_size)
{
    memset(c->raw, POISON, sizeof(c->raw));
    moq_locmaf_encoder_cfg_t cfg;
    moq_locmaf_encoder_cfg_init(&cfg);
    cfg.struct_size = struct_size;
    cfg.track = track;
    cfg.storage = storage;
    cfg.storage_size = storage_size;
    size_t n = struct_size < sizeof(cfg) ? struct_size : sizeof(cfg);
    memcpy(c->raw, &cfg, n);
}

static void test_decoder_extents(void)
{
    moq_locmaf_track_t track = lm_track_a();
    lm_canvas_t c;
    size_t need = 0;

    /* storage_size: below the cfg extent refused, at it and above it OK */
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE - 1, &track, NULL, 0);
    CHECK_RC(moq_locmaf_decoder_storage_size((const moq_locmaf_decoder_cfg_t *)c.raw,
                                             &need), MOQ_ERR_INVAL);
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE + 64, &track, NULL, 0);
    CHECK_RC(moq_locmaf_decoder_storage_size((const moq_locmaf_decoder_cfg_t *)c.raw,
                                             &need), MOQ_OK);
    CHECK(need > 0);
    size_t need0 = need;
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE, &track, NULL, 0);
    CHECK_RC(moq_locmaf_decoder_storage_size((const moq_locmaf_decoder_cfg_t *)c.raw,
                                             &need), MOQ_OK);
    CHECK_EQ(need, need0);
    CHECK_RC(moq_locmaf_decoder_storage_size(NULL, &need), MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_decoder_storage_size((const moq_locmaf_decoder_cfg_t *)c.raw, NULL),
             MOQ_ERR_INVAL);
    /* a limit above its ceiling */
    {
        moq_locmaf_decoder_cfg_t cfg;
        moq_locmaf_decoder_cfg_init(&cfg);
        cfg.max_samples = 65537;
        CHECK_RC(moq_locmaf_decoder_storage_size(&cfg, &need), MOQ_ERR_INVAL);
        cfg.max_samples = 65536;
        cfg.max_subsamples = 1048577;
        CHECK_RC(moq_locmaf_decoder_storage_size(&cfg, &need), MOQ_ERR_INVAL);
        cfg.max_subsamples = 1048576;
        cfg.max_gen_boxes = 257;
        CHECK_RC(moq_locmaf_decoder_storage_size(&cfg, &need), MOQ_ERR_INVAL);
        cfg.max_gen_boxes = 256;
        CHECK_RC(moq_locmaf_decoder_storage_size(&cfg, &need), MOQ_OK);
    }

    /* init: every refusal writes nothing into the state */
    uint64_t *mem = malloc(need0 + 16);
    CHECK(mem != NULL);
    if (!mem) return;
    moq_locmaf_decoder_t d;
    memset(&d, POISON, sizeof(d));
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE - 1, &track, mem, need0);
    CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d), (const moq_locmaf_decoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    CHECK(all_poison((const unsigned char *)&d, sizeof(d)));
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE, &track, mem, need0 - 1);
    CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d), (const moq_locmaf_decoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE, &track,
                    (unsigned char *)mem + 4, need0);
    CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d), (const moq_locmaf_decoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE, NULL, mem, need0);
    CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d), (const moq_locmaf_decoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE, &track, NULL, need0);
    CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d), (const moq_locmaf_decoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE, &track, mem, need0);
    CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d) - 1, (const moq_locmaf_decoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_decoder_init(NULL, sizeof(d), (const moq_locmaf_decoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d), NULL), MOQ_ERR_INVAL);
    /* a track below its extent, or with an IV size a track cannot have */
    {
        moq_locmaf_track_t bad = track;
        bad.struct_size = MOQ_LOCMAF_TRACK_V0_SIZE - 1;
        put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE, &bad, mem, need0);
        CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d),
                 (const moq_locmaf_decoder_cfg_t *)c.raw), MOQ_ERR_INVAL);
        bad = track;
        bad.default_per_sample_iv_size = 12;
        put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE, &bad, mem, need0);
        CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d),
                 (const moq_locmaf_decoder_cfg_t *)c.raw), MOQ_ERR_INVAL);
    }
    CHECK(all_poison((const unsigned char *)&d, sizeof(d)));
    /* the cfg's poisoned tail is ignored */
    put_decoder_cfg(&c, MOQ_LOCMAF_DECODER_CFG_V0_SIZE + 64, &track, mem, need0);
    CHECK_RC(moq_locmaf_decoder_init(&d, sizeof(d), (const moq_locmaf_decoder_cfg_t *)c.raw),
             MOQ_OK);
    CHECK_EQ(d.struct_size, sizeof(d));

    /* stats: one extent */
    {
        lm_canvas_t s;
        memset(s.raw, POISON, sizeof(s.raw));
        CHECK_RC(moq_locmaf_decoder_get_stats(&d, (moq_locmaf_decoder_stats_t *)s.raw,
                 MOQ_LOCMAF_DECODER_STATS_V0_SIZE - 1), MOQ_ERR_INVAL);
        CHECK(all_poison(s.raw, sizeof(s.raw)));
        CHECK_RC(moq_locmaf_decoder_get_stats(&d, (moq_locmaf_decoder_stats_t *)s.raw,
                 MOQ_LOCMAF_DECODER_STATS_V0_SIZE + 64), MOQ_OK);
        moq_locmaf_decoder_stats_t st;
        memcpy(&st, s.raw, sizeof(st));
        CHECK_EQ(st.struct_size, MOQ_LOCMAF_DECODER_STATS_V0_SIZE);
        CHECK(st.objects_decoded == 0 && st.last_reason == 0);
        CHECK(all_poison(s.raw + sizeof(st), 64));
        CHECK_RC(moq_locmaf_decoder_get_stats(NULL, &st, sizeof(st)), MOQ_ERR_INVAL);
        CHECK_RC(moq_locmaf_decoder_get_stats(&d, NULL, sizeof(st)), MOQ_ERR_INVAL);
    }

    /* the chunk record: below its extent refused before anything changes;
     * a larger record's tail untouched */
    {
        uint8_t obj[64];
        const lm_fixture_t *f1 = &lm_sequences[0].items[0];
        size_t n = lm_fixture_object(f1, obj, sizeof(obj));
        lm_canvas_t o;
        memset(o.raw, POISON, sizeof(o.raw));
        moq_locmaf_reason_t why;
        CHECK_RC(moq_locmaf_decoder_push(&d, 0, 0, (moq_bytes_t){ obj, n },
                 (moq_locmaf_chunk_t *)o.raw, MOQ_LOCMAF_CHUNK_V0_SIZE - 1, &why),
                 MOQ_ERR_INVAL);
        CHECK(all_poison(o.raw, sizeof(o.raw)));
        CHECK(!moq_locmaf_decoder_expects(&d, 0, 1));
        CHECK_RC(moq_locmaf_decoder_push(&d, 0, 0, (moq_bytes_t){ obj, n },
                 (moq_locmaf_chunk_t *)o.raw, MOQ_LOCMAF_CHUNK_V0_SIZE + 64, &why),
                 MOQ_OK);
        moq_locmaf_chunk_t ch;
        memcpy(&ch, o.raw, sizeof(ch));
        CHECK_EQ(ch.struct_size, MOQ_LOCMAF_CHUNK_V0_SIZE);
        CHECK(all_poison(o.raw + MOQ_LOCMAF_CHUNK_V0_SIZE, 64));
        CHECK(moq_locmaf_decoder_expects(&d, 0, 1));
    }
    free(mem);
}

static void test_encoder_extents(void)
{
    moq_locmaf_track_t track = lm_track_a();
    lm_canvas_t c;
    size_t need = 0;
    put_encoder_cfg(&c, MOQ_LOCMAF_ENCODER_CFG_V0_SIZE - 1, &track, NULL, 0);
    CHECK_RC(moq_locmaf_encoder_storage_size((const moq_locmaf_encoder_cfg_t *)c.raw,
                                             &need), MOQ_ERR_INVAL);
    put_encoder_cfg(&c, MOQ_LOCMAF_ENCODER_CFG_V0_SIZE + 64, &track, NULL, 0);
    CHECK_RC(moq_locmaf_encoder_storage_size((const moq_locmaf_encoder_cfg_t *)c.raw,
                                             &need), MOQ_OK);
    size_t dneed = 0;
    {
        moq_locmaf_decoder_cfg_t dc;
        moq_locmaf_decoder_cfg_init(&dc);
        CHECK_RC(moq_locmaf_decoder_storage_size(&dc, &dneed), MOQ_OK);
    }
    CHECK(need > dneed);   /* the encoder keeps a staged reference too */

    uint64_t *mem = malloc(need + 16);
    CHECK(mem != NULL);
    if (!mem) return;
    moq_locmaf_encoder_t e;
    memset(&e, POISON, sizeof(e));
    put_encoder_cfg(&c, MOQ_LOCMAF_ENCODER_CFG_V0_SIZE - 1, &track, mem, need);
    CHECK_RC(moq_locmaf_encoder_init(&e, sizeof(e), (const moq_locmaf_encoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    put_encoder_cfg(&c, MOQ_LOCMAF_ENCODER_CFG_V0_SIZE, &track,
                    (unsigned char *)mem + 4, need);
    CHECK_RC(moq_locmaf_encoder_init(&e, sizeof(e), (const moq_locmaf_encoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    put_encoder_cfg(&c, MOQ_LOCMAF_ENCODER_CFG_V0_SIZE, &track, mem, need - 1);
    CHECK_RC(moq_locmaf_encoder_init(&e, sizeof(e), (const moq_locmaf_encoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    put_encoder_cfg(&c, MOQ_LOCMAF_ENCODER_CFG_V0_SIZE, &track, mem, need);
    CHECK_RC(moq_locmaf_encoder_init(&e, sizeof(e) - 1, (const moq_locmaf_encoder_cfg_t *)c.raw),
             MOQ_ERR_INVAL);
    CHECK(all_poison((const unsigned char *)&e, sizeof(e)));
    put_encoder_cfg(&c, MOQ_LOCMAF_ENCODER_CFG_V0_SIZE + 64, &track, mem, need);
    CHECK_RC(moq_locmaf_encoder_init(&e, sizeof(e), (const moq_locmaf_encoder_cfg_t *)c.raw),
             MOQ_OK);
    CHECK_EQ(e.struct_size, sizeof(e));
    {
        lm_canvas_t s;
        memset(s.raw, POISON, sizeof(s.raw));
        CHECK_RC(moq_locmaf_encoder_get_stats(&e, (moq_locmaf_encoder_stats_t *)s.raw,
                 MOQ_LOCMAF_ENCODER_STATS_V0_SIZE - 1), MOQ_ERR_INVAL);
        CHECK(all_poison(s.raw, sizeof(s.raw)));
        CHECK_RC(moq_locmaf_encoder_get_stats(&e, (moq_locmaf_encoder_stats_t *)s.raw,
                 MOQ_LOCMAF_ENCODER_STATS_V0_SIZE + 64), MOQ_OK);
        moq_locmaf_encoder_stats_t st;
        memcpy(&st, s.raw, sizeof(st));
        CHECK_EQ(st.struct_size, MOQ_LOCMAF_ENCODER_STATS_V0_SIZE);
        CHECK(all_poison(s.raw + sizeof(st), 64));
    }
    free(mem);
}

static void test_track_from_cmaf_extents(void)
{
    /* track_from_cmaf: an output below its extent, or an init record whose
     * stamp does not cover the header facts, is refused writing nothing. */
    moq_cmaf_init_info_t init;
    memset(&init, 0, sizeof(init));
    init.struct_size = (uint32_t)sizeof(init);
    init.track_id = 1;
    init.trak_count = 1;
    init.has_trex = true;
    lm_canvas_t c;
    memset(c.raw, POISON, sizeof(c.raw));
    CHECK_RC(moq_locmaf_track_from_cmaf(&init, (moq_locmaf_track_t *)c.raw,
                                        MOQ_LOCMAF_TRACK_V0_SIZE - 1), MOQ_ERR_INVAL);
    CHECK(all_poison(c.raw, sizeof(c.raw)));
    init.struct_size = (uint32_t)MOQ_CMAF_INIT_INFO_SIZE_THROUGH_RESERVED;
    CHECK_RC(moq_locmaf_track_from_cmaf(&init, (moq_locmaf_track_t *)c.raw,
                                        MOQ_LOCMAF_TRACK_V0_SIZE), MOQ_ERR_INVAL);
    CHECK(all_poison(c.raw, sizeof(c.raw)));
    init.struct_size = (uint32_t)MOQ_CMAF_INIT_INFO_SIZE_THROUGH_CONSTANT_IV;
    CHECK_RC(moq_locmaf_track_from_cmaf(&init, (moq_locmaf_track_t *)c.raw,
                                        MOQ_LOCMAF_TRACK_V0_SIZE + 64), MOQ_OK);
    CHECK(all_poison(c.raw + MOQ_LOCMAF_TRACK_V0_SIZE, 64));
}

int main(void)
{
    test_init_sized();
    test_decoder_extents();
    test_encoder_extents();
    test_track_from_cmaf_extents();
    return lm_finish("locmaf_abi");
}
