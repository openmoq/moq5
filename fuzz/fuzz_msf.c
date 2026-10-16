#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <moq/msf.h>
#include <moq/rcbuf.h>
#if defined(MOQ_FUZZ_MSF_HAS_MEDIA)
#include <moq/msf_media.h>
#endif

/*
 * LibFuzzer target for the MSF-01 / CMSF-01 JSON catalog, delta update,
 * media-timeline, and Base64 initData codecs (media/msf/src/msf.c and
 * media/msf/src/msf_media.c).
 *
 * Properties enforced on every input:
 *   1. moq_msf_catalog_parse returns only {MOQ_OK, MOQ_ERR_PROTO,
 *      MOQ_ERR_NOMEM} for non-NULL arguments.
 *   2. On MOQ_OK, every string slice in the parsed catalog (tracks,
 *      delta_update ops, initDataList, contentProtections) has non-NULL data
 *      whenever len > 0 and is readable under ASan.
 *   3. moq_msf_catalog_validate_identities returns MOQ_ERR_INVAL on delta
 *      catalogs and only {MOQ_OK, MOQ_ERR_PROTO, MOQ_ERR_NOMEM} on
 *      independent catalogs. Lookup helpers (find_role, find_init_data,
 *      find_content_protection) return in-range pointers into the catalog.
 *   4. When moq_msf_catalog_encode succeeds on a parsed catalog, re-parsing
 *      the emitted JSON MUST succeed (MOQ_OK), preserve every supported
 *      catalog/track/delta/initData/contentProtection field (catalog_eq),
 *      pass identity validation on an independent catalog, and re-encode
 *      byte-for-byte identically (canonical JSON idempotence).
 *   5. moq_msf_catalog_apply_delta_ex with bounded moq_msf_apply_limits_t
 *      returns only {MOQ_OK, MOQ_ERR_INVAL, MOQ_ERR_PROTO, MOQ_ERR_NOMEM},
 *      and MUST refuse applying a delta to a terminal isComplete catalog with
 *      MOQ_ERR_PROTO (MSF §5.1.3).
 *   6. moq_msf_media_timeline_decode (zero-cap count probe vs bounded decode)
 *      and moq_msf_media_timeline_encode round-trip decoded records
 *      identically.
 *   7. moq_msf_decode_init_data and moq_msf_encode_init_data enforce
 *      canonical Base64 round-trip in both directions.
 *   8. Every allocation made through the counting allocator is freed with its
 *      exact byte size before LLVMFuzzerTestOneInput returns.
 */

typedef struct {
    int64_t  balance;
    uint64_t bytes_alloc;
    uint64_t bytes_free;
} fuzz_alloc_state_t;

static void *fuzz_malloc(size_t size, void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    if (size == 0)
        return NULL;
    void *p = malloc(size);
    if (p) {
        s->balance++;
        s->bytes_alloc += size;
    }
    return p;
}

static void fuzz_free(void *ptr, size_t size, void *ctx)
{
    fuzz_alloc_state_t *s = (fuzz_alloc_state_t *)ctx;
    if (!ptr)
        return;
    s->balance--;
    s->bytes_free += size;
    free(ptr);
}

static void check_dom_slice(moq_bytes_t b)
{
    if (b.len == 0)
        return;
    if (!b.data)
        __builtin_trap();
    /* Touch first and last byte so ASan catches any out-of-DOM slice. */
    volatile uint8_t sink = b.data[0] ^ b.data[b.len - 1];
    (void)sink;
}

static void check_track_slices(const moq_msf_track_t *t)
{
    if (t->struct_size != sizeof(*t))
        __builtin_trap();
    check_dom_slice(t->name);
    if (t->has_packaging)
        check_dom_slice(t->packaging);
    if (t->has_namespace)
        check_dom_slice(t->namespace_);
    if (t->has_role)
        check_dom_slice(t->role);
    if (t->has_codec)
        check_dom_slice(t->codec);
    if (t->has_init_data)
        check_dom_slice(t->init_data);
    if (t->has_init_track)
        check_dom_slice(t->init_track);
    if (t->has_channel_config)
        check_dom_slice(t->channel_config);
    if (t->has_lang)
        check_dom_slice(t->lang);
    if (t->has_label)
        check_dom_slice(t->label);
    if (t->has_init_ref)
        check_dom_slice(t->init_ref);
    if (t->has_event_type)
        check_dom_slice(t->event_type);
    if (t->has_mime_type)
        check_dom_slice(t->mime_type);
    if (t->has_parent_name)
        check_dom_slice(t->parent_name);
    if (t->has_parent_namespace)
        check_dom_slice(t->parent_namespace);
    for (size_t i = 0; i < t->depends_count; i++)
        check_dom_slice(t->depends[i]);
    for (size_t i = 0; i < t->cp_ref_id_count; i++)
        check_dom_slice(t->cp_ref_ids[i]);
}

static void check_catalog_invariants(const moq_msf_catalog_t *cat)
{
    if (cat->struct_size != sizeof(*cat))
        __builtin_trap();

    if (cat->delta_update) {
        if (cat->delta_update_count == 0 || cat->track_count != 0 ||
            cat->tracks != NULL || cat->is_complete ||
            cat->init_data_count != 0 || cat->content_protection_count != 0)
            __builtin_trap();
        for (size_t i = 0; i < cat->delta_update_count; i++) {
            const moq_msf_delta_op_t *op = &cat->delta_update[i];
            if (op->track_count == 0 || !op->tracks)
                __builtin_trap();
            for (size_t j = 0; j < op->track_count; j++)
                check_track_slices(&op->tracks[j]);
        }
    } else {
        if (cat->version != MOQ_MSF_VERSION)
            __builtin_trap();
        for (size_t i = 0; i < cat->track_count; i++)
            check_track_slices(&cat->tracks[i]);
        for (size_t i = 0; i < cat->init_data_count; i++) {
            check_dom_slice(cat->init_data_list[i].id);
            check_dom_slice(cat->init_data_list[i].type);
            check_dom_slice(cat->init_data_list[i].data);
        }
        for (size_t i = 0; i < cat->content_protection_count; i++) {
            const moq_cmsf_content_protection_t *cp =
                &cat->content_protections[i];
            check_dom_slice(cp->ref_id);
            check_dom_slice(cp->scheme);
            for (size_t k = 0; k < cp->default_kid_count; k++)
                check_dom_slice(cp->default_kids[k]);
            check_dom_slice(cp->drm_system.system_id);
            if (cp->drm_system.la_url.present) {
                check_dom_slice(cp->drm_system.la_url.url);
                if (cp->drm_system.la_url.has_type)
                    check_dom_slice(cp->drm_system.la_url.type);
            }
            if (cp->drm_system.cert_url.present) {
                check_dom_slice(cp->drm_system.cert_url.url);
                if (cp->drm_system.cert_url.has_type)
                    check_dom_slice(cp->drm_system.cert_url.type);
            }
            if (cp->drm_system.auth_url.present) {
                check_dom_slice(cp->drm_system.auth_url.url);
                if (cp->drm_system.auth_url.has_type)
                    check_dom_slice(cp->drm_system.auth_url.type);
            }
            if (cp->drm_system.has_pssh)
                check_dom_slice(cp->drm_system.pssh);
            if (cp->drm_system.has_robustness)
                check_dom_slice(cp->drm_system.robustness);
        }
    }
}

static bool bytes_eq(moq_bytes_t a, moq_bytes_t b)
{
    if (a.len != b.len)
        return false;
    if (a.len == 0)
        return true;
    return a.data && b.data && memcmp(a.data, b.data, a.len) == 0;
}

static bool url_eq(const moq_cmsf_url_t *a, const moq_cmsf_url_t *b)
{
    if (a->present != b->present)
        return false;
    if (!a->present)
        return true;
    if (!bytes_eq(a->url, b->url) || a->has_type != b->has_type)
        return false;
    if (a->has_type && !bytes_eq(a->type, b->type))
        return false;
    return true;
}

static bool track_eq(const moq_msf_track_t *a, const moq_msf_track_t *b)
{
    if (!bytes_eq(a->name, b->name) ||
        a->has_packaging != b->has_packaging ||
        (a->has_packaging && !bytes_eq(a->packaging, b->packaging)) ||
        a->has_is_live != b->has_is_live ||
        (a->has_is_live && a->is_live != b->is_live) ||
        a->has_namespace != b->has_namespace ||
        (a->has_namespace && !bytes_eq(a->namespace_, b->namespace_)) ||
        a->has_role != b->has_role ||
        (a->has_role && !bytes_eq(a->role, b->role)) ||
        a->has_codec != b->has_codec ||
        (a->has_codec && !bytes_eq(a->codec, b->codec)) ||
        a->has_init_data != b->has_init_data ||
        (a->has_init_data && !bytes_eq(a->init_data, b->init_data)) ||
        a->has_init_track != b->has_init_track ||
        (a->has_init_track && !bytes_eq(a->init_track, b->init_track)) ||
        a->has_width != b->has_width ||
        (a->has_width && a->width != b->width) ||
        a->has_height != b->has_height ||
        (a->has_height && a->height != b->height) ||
        a->has_samplerate != b->has_samplerate ||
        (a->has_samplerate && a->samplerate != b->samplerate) ||
        a->has_bitrate != b->has_bitrate ||
        (a->has_bitrate && a->bitrate != b->bitrate) ||
        a->has_framerate != b->has_framerate ||
        (a->has_framerate && a->framerate_millis != b->framerate_millis) ||
        a->has_render_group != b->has_render_group ||
        (a->has_render_group && a->render_group != b->render_group) ||
        a->has_alt_group != b->has_alt_group ||
        (a->has_alt_group && a->alt_group != b->alt_group) ||
        a->has_timescale != b->has_timescale ||
        (a->has_timescale && a->timescale != b->timescale) ||
        a->has_target_latency != b->has_target_latency ||
        (a->has_target_latency && a->target_latency != b->target_latency) ||
        a->has_channel_config != b->has_channel_config ||
        (a->has_channel_config && !bytes_eq(a->channel_config, b->channel_config)) ||
        a->has_lang != b->has_lang ||
        (a->has_lang && !bytes_eq(a->lang, b->lang)) ||
        a->has_label != b->has_label ||
        (a->has_label && !bytes_eq(a->label, b->label)) ||
        a->has_init_ref != b->has_init_ref ||
        (a->has_init_ref && !bytes_eq(a->init_ref, b->init_ref)) ||
        a->has_event_type != b->has_event_type ||
        (a->has_event_type && !bytes_eq(a->event_type, b->event_type)) ||
        a->has_mime_type != b->has_mime_type ||
        (a->has_mime_type && !bytes_eq(a->mime_type, b->mime_type)) ||
        a->depends_count != b->depends_count ||
        a->cp_ref_id_count != b->cp_ref_id_count ||
        a->has_max_grp_sap != b->has_max_grp_sap ||
        (a->has_max_grp_sap && a->max_grp_sap != b->max_grp_sap) ||
        a->has_max_obj_sap != b->has_max_obj_sap ||
        (a->has_max_obj_sap && a->max_obj_sap != b->max_obj_sap) ||
        a->has_parent_name != b->has_parent_name ||
        (a->has_parent_name && !bytes_eq(a->parent_name, b->parent_name)) ||
        a->has_parent_namespace != b->has_parent_namespace ||
        (a->has_parent_namespace && !bytes_eq(a->parent_namespace, b->parent_namespace)) ||
        a->has_track_duration != b->has_track_duration ||
        (a->has_track_duration && a->track_duration_ms != b->track_duration_ms) ||
        a->has_template != b->has_template)
        return false;

    if (a->has_template &&
        (a->template_.start_media_ms != b->template_.start_media_ms ||
         a->template_.delta_media_ms != b->template_.delta_media_ms ||
         a->template_.start_group != b->template_.start_group ||
         a->template_.start_object != b->template_.start_object ||
         a->template_.delta_group != b->template_.delta_group ||
         a->template_.delta_object != b->template_.delta_object ||
         a->template_.start_wallclock_ms != b->template_.start_wallclock_ms ||
         a->template_.delta_wallclock_ms != b->template_.delta_wallclock_ms))
        return false;

    for (size_t i = 0; i < a->depends_count; i++) {
        if (!bytes_eq(a->depends[i], b->depends[i]))
            return false;
    }
    for (size_t i = 0; i < a->cp_ref_id_count; i++) {
        if (!bytes_eq(a->cp_ref_ids[i], b->cp_ref_ids[i]))
            return false;
    }
    return true;
}

static bool catalog_eq(const moq_msf_catalog_t *a, const moq_msf_catalog_t *b)
{
    if (a->version != b->version ||
        a->has_generated_at != b->has_generated_at ||
        (a->has_generated_at && a->generated_at != b->generated_at) ||
        a->is_complete != b->is_complete ||
        a->track_count != b->track_count ||
        a->init_data_count != b->init_data_count ||
        a->content_protection_count != b->content_protection_count ||
        (a->delta_update != NULL) != (b->delta_update != NULL) ||
        a->delta_update_count != b->delta_update_count)
        return false;

    for (size_t i = 0; i < a->track_count; i++) {
        if (!track_eq(&a->tracks[i], &b->tracks[i]))
            return false;
    }
    for (size_t i = 0; i < a->init_data_count; i++) {
        if (!bytes_eq(a->init_data_list[i].id, b->init_data_list[i].id) ||
            !bytes_eq(a->init_data_list[i].type, b->init_data_list[i].type) ||
            !bytes_eq(a->init_data_list[i].data, b->init_data_list[i].data))
            return false;
    }
    for (size_t i = 0; i < a->content_protection_count; i++) {
        const moq_cmsf_content_protection_t *ca = &a->content_protections[i];
        const moq_cmsf_content_protection_t *cb = &b->content_protections[i];
        if (!bytes_eq(ca->ref_id, cb->ref_id) ||
            !bytes_eq(ca->scheme, cb->scheme) ||
            ca->default_kid_count != cb->default_kid_count ||
            !bytes_eq(ca->drm_system.system_id, cb->drm_system.system_id) ||
            !url_eq(&ca->drm_system.la_url, &cb->drm_system.la_url) ||
            !url_eq(&ca->drm_system.cert_url, &cb->drm_system.cert_url) ||
            !url_eq(&ca->drm_system.auth_url, &cb->drm_system.auth_url) ||
            ca->drm_system.has_pssh != cb->drm_system.has_pssh ||
            (ca->drm_system.has_pssh &&
             !bytes_eq(ca->drm_system.pssh, cb->drm_system.pssh)) ||
            ca->drm_system.has_robustness != cb->drm_system.has_robustness ||
            (ca->drm_system.has_robustness &&
             !bytes_eq(ca->drm_system.robustness, cb->drm_system.robustness)))
            return false;
        for (size_t k = 0; k < ca->default_kid_count; k++) {
            if (!bytes_eq(ca->default_kids[k], cb->default_kids[k]))
                return false;
        }
    }
    for (size_t i = 0; i < a->delta_update_count; i++) {
        const moq_msf_delta_op_t *oa = &a->delta_update[i];
        const moq_msf_delta_op_t *ob = &b->delta_update[i];
        if (oa->op != ob->op || oa->track_count != ob->track_count)
            return false;
        for (size_t j = 0; j < oa->track_count; j++) {
            if (!track_eq(&oa->tracks[j], &ob->tracks[j]))
                return false;
        }
    }
    return true;
}

static const char k_base_catalog_json[] =
    "{\"version\":\"1\",\"generatedAt\":1746104606000,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.64001e\"},"
    "{\"name\":\"slides\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01.0.08M.10.0.110.09\"},"
    "{\"name\":\"video-1080\",\"namespace\":\"example.com/custom\","
    "\"packaging\":\"cmaf\",\"isLive\":true,\"role\":\"video\","
    "\"codec\":\"avc1.64001e\",\"width\":1920,\"height\":1080,"
    "\"bitrate\":1500000,\"timescale\":90000},"
    "{\"name\":\"audio\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"audio\",\"codec\":\"opus\",\"samplerate\":48000}"
    "]}";

static const char k_fixed_delta_json[] =
    "{\"generatedAt\":1746104606044,\"deltaUpdate\":["
    "{\"op\":\"add\",\"tracks\":["
    "{\"name\":\"__fuzz_delta_track\",\"packaging\":\"loc\","
    "\"isLive\":true,\"role\":\"audio\",\"codec\":\"opus\"}"
    "]}]}";

static void fuzz_media_timeline_path(const moq_alloc_t *alloc,
                                     moq_bytes_t slice)
{
    size_t total = 0;
    moq_result_t rc = moq_msf_media_timeline_decode(slice, NULL, 0, &total);
    if (rc != MOQ_OK && rc != MOQ_ERR_BUFFER && rc != MOQ_ERR_PROTO)
        __builtin_trap();
    if (rc < 0 && rc != MOQ_ERR_BUFFER)
        return;

    /* Zero-cap probe must return MOQ_OK iff total == 0, else MOQ_ERR_BUFFER. */
    if ((total == 0 && rc != MOQ_OK) || (total > 0 && rc != MOQ_ERR_BUFFER))
        __builtin_trap();

    moq_msf_media_timeline_record_t recs[16];
    size_t got = 0;
    moq_result_t rc2 = moq_msf_media_timeline_decode(
        slice, recs, sizeof(recs) / sizeof(recs[0]), &got);
    if (got != total)
        __builtin_trap();
    if (total <= 16) {
        if (rc2 != MOQ_OK)
            __builtin_trap();
    } else {
        if (rc2 != MOQ_ERR_BUFFER)
            __builtin_trap();
    }

    size_t n = total <= 16 ? total : 16;
    size_t needed = 0;
    moq_result_t erc = moq_msf_media_timeline_encode(
        n ? recs : NULL, n, NULL, 0, &needed);
    if (erc != MOQ_ERR_BUFFER || needed < 2)
        __builtin_trap();

    uint8_t *ebuf = (uint8_t *)alloc->alloc(needed, alloc->ctx);
    if (!ebuf)
        return;
    size_t written = 0;
    erc = moq_msf_media_timeline_encode(
        n ? recs : NULL, n, ebuf, needed, &written);
    if (erc != MOQ_OK || written != needed)
        __builtin_trap();

    moq_msf_media_timeline_record_t rt[16];
    size_t rt_count = 0;
    moq_result_t drc = moq_msf_media_timeline_decode(
        (moq_bytes_t){ ebuf, written }, rt, 16, &rt_count);
    if (drc != MOQ_OK || rt_count != n)
        __builtin_trap();
    if (n > 0 && memcmp(recs, rt, n * sizeof(recs[0])) != 0)
        __builtin_trap();

    alloc->free(ebuf, needed, alloc->ctx);
}

static void fuzz_base64_path(const moq_alloc_t *alloc, moq_bytes_t slice)
{
    moq_rcbuf_t *dec = NULL;
    moq_result_t rc = moq_msf_decode_init_data(alloc, slice, &dec);
    if (rc != MOQ_OK && rc != MOQ_ERR_INVAL &&
        rc != MOQ_ERR_PROTO && rc != MOQ_ERR_NOMEM)
        __builtin_trap();
    if (rc == MOQ_OK) {
        if (!dec)
            __builtin_trap();
        moq_bytes_t raw = { moq_rcbuf_data(dec), moq_rcbuf_len(dec) };
        moq_rcbuf_t *re_b64 = NULL;
        if (moq_msf_encode_init_data(alloc, raw, &re_b64) != MOQ_OK || !re_b64)
            __builtin_trap();
        if (moq_rcbuf_len(re_b64) != slice.len ||
            (slice.len > 0 &&
             memcmp(moq_rcbuf_data(re_b64), slice.data, slice.len) != 0))
            __builtin_trap();
        moq_rcbuf_decref(re_b64);
        moq_rcbuf_decref(dec);
    } else if (dec != NULL) {
        __builtin_trap();
    }

    if (slice.len <= 256) {
        moq_rcbuf_t *enc_b64 = NULL;
        if (moq_msf_encode_init_data(alloc, slice, &enc_b64) != MOQ_OK ||
            !enc_b64)
            __builtin_trap();
        moq_bytes_t b64_slice = {
            moq_rcbuf_data(enc_b64), moq_rcbuf_len(enc_b64)
        };
        moq_rcbuf_t *rt_raw = NULL;
        if (moq_msf_decode_init_data(alloc, b64_slice, &rt_raw) != MOQ_OK ||
            !rt_raw)
            __builtin_trap();
        if (moq_rcbuf_len(rt_raw) != slice.len ||
            (slice.len > 0 &&
             memcmp(moq_rcbuf_data(rt_raw), slice.data, slice.len) != 0))
            __builtin_trap();
        moq_rcbuf_decref(rt_raw);
        moq_rcbuf_decref(enc_b64);
    }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    uint8_t dummy = 0;
    moq_bytes_t slice = { data ? data : &dummy, size };

    fuzz_alloc_state_t as = { 0, 0, 0 };
    moq_alloc_t alloc = { &as, fuzz_malloc, NULL, fuzz_free };

    /* 1. Media timeline explicit payload codec + Base64 codec. */
    fuzz_media_timeline_path(&alloc, slice);
    fuzz_base64_path(&alloc, slice);

    /* 2. Catalog JSON parser. */
    moq_msf_catalog_t cat;
    moq_result_t rc = moq_msf_catalog_parse(&alloc, slice, &cat);
    if (rc != MOQ_OK && rc != MOQ_ERR_PROTO && rc != MOQ_ERR_NOMEM)
        __builtin_trap();

    if (rc == MOQ_OK) {
        check_catalog_invariants(&cat);

        /* 3. Identity validation and lookup helpers. */
        moq_result_t vrc = moq_msf_catalog_validate_identities(&alloc, &cat);
        if (cat.delta_update) {
            if (vrc != MOQ_ERR_INVAL)
                __builtin_trap();
        } else {
            if (vrc != MOQ_OK && vrc != MOQ_ERR_PROTO && vrc != MOQ_ERR_NOMEM)
                __builtin_trap();
        }

        static const char *const k_roles[] = {
            "video", "audio", "mediatimeline", "eventtimeline", "caption"
        };
        for (size_t r = 0; r < sizeof(k_roles) / sizeof(k_roles[0]); r++) {
            const moq_msf_track_t *found =
                moq_msf_catalog_find_role(&cat, k_roles[r]);
            if (found) {
                if (!cat.tracks || found < cat.tracks ||
                    found >= cat.tracks + cat.track_count)
                    __builtin_trap();
            }
        }

        size_t idl_limit = cat.init_data_count < 8 ? cat.init_data_count : 8;
        for (size_t i = 0; i < idl_limit; i++) {
            moq_bytes_t id = cat.init_data_list[i].id;
            const moq_msf_init_data_entry_t *e =
                moq_msf_catalog_find_init_data(&cat, id);
            if (id.len > 0 && !e)
                __builtin_trap();
        }

        size_t cp_limit = cat.content_protection_count < 8
                        ? cat.content_protection_count : 8;
        for (size_t i = 0; i < cp_limit; i++) {
            moq_bytes_t ref_id = cat.content_protections[i].ref_id;
            const moq_cmsf_content_protection_t *cp =
                moq_msf_catalog_find_content_protection(&cat, ref_id);
            if (ref_id.len > 0 && !cp)
                __builtin_trap();
        }

        /* 4. Encode -> Re-parse -> Re-encode canonical JSON idempotence. */
        moq_rcbuf_t *enc1 = NULL;
        moq_result_t erc = moq_msf_catalog_encode(&alloc, &cat, &enc1);
        if (erc != MOQ_OK && erc != MOQ_ERR_INVAL && erc != MOQ_ERR_NOMEM)
            __builtin_trap();
        if (erc == MOQ_OK) {
            if (!enc1 || moq_rcbuf_len(enc1) == 0)
                __builtin_trap();
            moq_bytes_t enc1_slice = {
                moq_rcbuf_data(enc1), moq_rcbuf_len(enc1)
            };
            moq_msf_catalog_t cat2;
            if (moq_msf_catalog_parse(&alloc, enc1_slice, &cat2) != MOQ_OK)
                __builtin_trap();
            check_catalog_invariants(&cat2);
            if (!catalog_eq(&cat, &cat2))
                __builtin_trap();

            if (!cat2.delta_update) {
                if (moq_msf_catalog_validate_identities(&alloc, &cat2) != MOQ_OK)
                    __builtin_trap();
            }

            moq_rcbuf_t *enc2 = NULL;
            if (moq_msf_catalog_encode(&alloc, &cat2, &enc2) != MOQ_OK || !enc2)
                __builtin_trap();
            if (moq_rcbuf_len(enc1) != moq_rcbuf_len(enc2) ||
                memcmp(moq_rcbuf_data(enc1), moq_rcbuf_data(enc2),
                       moq_rcbuf_len(enc1)) != 0)
                __builtin_trap();

            moq_rcbuf_decref(enc2);
            moq_msf_catalog_cleanup(&alloc, &cat2);
            moq_rcbuf_decref(enc1);
        } else if (enc1 != NULL) {
            __builtin_trap();
        }

        /* 5. Delta application with bounded resource limits. */
        moq_msf_apply_limits_t limits;
        memset(&limits, 0, sizeof(limits));
        limits.struct_size = sizeof(limits);
        limits.max_delta_ops = 32;
        limits.max_effective_tracks = 64;

        if (cat.delta_update) {
            moq_msf_catalog_t base_cat;
            moq_bytes_t base_slice = {
                (const uint8_t *)k_base_catalog_json,
                sizeof(k_base_catalog_json) - 1
            };
            if (moq_msf_catalog_parse(&alloc, base_slice, &base_cat) != MOQ_OK)
                __builtin_trap();

            moq_msf_catalog_t applied;
            moq_result_t arc = moq_msf_catalog_apply_delta_ex(
                &alloc, &base_cat, &cat, &limits, &applied);
            if (arc != MOQ_OK && arc != MOQ_ERR_INVAL &&
                arc != MOQ_ERR_PROTO && arc != MOQ_ERR_NOMEM)
                __builtin_trap();
            if (arc == MOQ_OK) {
                check_catalog_invariants(&applied);
                if (applied.delta_update != NULL)
                    __builtin_trap();
                moq_msf_catalog_cleanup(&alloc, &applied);
            }
            moq_msf_catalog_cleanup(&alloc, &base_cat);
        } else {
            moq_msf_catalog_t delta_cat;
            moq_bytes_t delta_slice = {
                (const uint8_t *)k_fixed_delta_json,
                sizeof(k_fixed_delta_json) - 1
            };
            if (moq_msf_catalog_parse(&alloc, delta_slice, &delta_cat) != MOQ_OK)
                __builtin_trap();

            moq_msf_catalog_t applied;
            moq_result_t arc = moq_msf_catalog_apply_delta_ex(
                &alloc, &cat, &delta_cat, &limits, &applied);
            if (cat.is_complete) {
                /* MSF §5.1.3: terminal isComplete catalog must reject deltas. */
                if (arc != MOQ_ERR_PROTO)
                    __builtin_trap();
            } else if (arc != MOQ_OK && arc != MOQ_ERR_INVAL &&
                       arc != MOQ_ERR_PROTO && arc != MOQ_ERR_NOMEM) {
                __builtin_trap();
            }
            if (arc == MOQ_OK) {
                check_catalog_invariants(&applied);
                if (applied.delta_update != NULL)
                    __builtin_trap();
                moq_msf_catalog_cleanup(&alloc, &applied);
            }
            moq_msf_catalog_cleanup(&alloc, &delta_cat);
        }

#if defined(MOQ_FUZZ_MSF_HAS_MEDIA)
        /* 6. Track-to-media-info and CMAF/RAW initData bridge. */
        size_t trk_limit = cat.track_count < 8 ? cat.track_count : 8;
        static const moq_version_t k_vers[] = {
            MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18
        };
        for (size_t i = 0; i < trk_limit; i++) {
            const moq_msf_track_t *t = &cat.tracks[i];
            for (size_t v = 0; v < 2; v++) {
                moq_media_track_info_t info;
                moq_cmaf_init_info_t cinit;
                moq_rcbuf_t *init_buf = NULL;
                moq_result_t mrc = moq_msf_track_to_media_info_sized(
                    &alloc, t, &info, sizeof(info), k_vers[v],
                    &cinit, &init_buf);
                if (mrc != MOQ_OK && mrc != MOQ_ERR_INVAL &&
                    mrc != MOQ_ERR_PROTO && mrc != MOQ_ERR_UNSUPPORTED &&
                    mrc != MOQ_ERR_NOMEM)
                    __builtin_trap();
                if (mrc == MOQ_OK) {
                    if (info.transport_version != k_vers[v])
                        __builtin_trap();
                    if (init_buf) {
                        const uint8_t *ib = moq_rcbuf_data(init_buf);
                        size_t ilen = moq_rcbuf_len(init_buf);
                        if (cinit.codec_config.len > 0) {
                            if (!cinit.codec_config.data ||
                                cinit.codec_config.data < ib ||
                                cinit.codec_config.data +
                                    cinit.codec_config.len > ib + ilen)
                                __builtin_trap();
                        }
                        moq_rcbuf_decref(init_buf);
                    }
                } else if (init_buf != NULL) {
                    __builtin_trap();
                }
            }

            if (t->has_init_data)
                fuzz_base64_path(&alloc, t->init_data);
        }
#endif

        moq_msf_catalog_cleanup(&alloc, &cat);
    }

    if (as.balance != 0 || as.bytes_alloc != as.bytes_free)
        __builtin_trap();

    return 0;
}
