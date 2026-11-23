/* Merge of one MPEG-TS track received over several paths (delivery
 * redundancy). See <moq/media_merge.h>. The order and the loss accounting
 * are the public MPEG-TS reorder window's; this file adds the paths, the
 * ownership of the polled Objects, and the duplicate and late counts. */
#include <moq/media_merge.h>
#include <moq/media_object.h>
#include <moq/control.h>

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
#include <moq/mpeg2ts.h>
#endif

#include <stddef.h>
#include <string.h>

/* The prefix of a polled Object that carries its Location. */
#define MERGE_OBJ_IDENTITY_SIZE \
    (offsetof(moq_media_object_t, has_subgroup) + sizeof(bool))
/* The prefix that carries its loss report. */
#define MERGE_OBJ_LOSS_SIZE \
    (offsetof(moq_media_object_t, discontinuous) + sizeof(bool))
#define MERGE_CFG_MIN_SIZE \
    (offsetof(moq_media_merge_cfg_t, window_us) + sizeof(uint64_t))
#define MERGE_MAX_SLOTS 4096u

void moq_media_merge_cfg_init_sized(moq_media_merge_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(cfg->struct_size)) return;
    moq_media_merge_cfg_t d;
    memset(&d, 0, sizeof(d));
    size_t n = cfg_size < sizeof(d) ? cfg_size : sizeof(d);
    d.struct_size = (uint32_t)n;
    d.path_count = 2;
    d.packet_size = 188;
    d.slot_count = 128;
    d.window_us = 600000;
    memcpy(cfg, &d, n);
}

#ifdef MOQ_SERVICE_HAVE_MPEG2TS

typedef struct merge_held {
    bool               used;
    uint32_t           path;
    moq_media_object_t obj;
} merge_held_t;

typedef struct merge_loc {
    uint64_t group;
    uint64_t object;
} merge_loc_t;

struct moq_media_merge {
    moq_alloc_t                 alloc;
    uint32_t                    path_count;
    uint32_t                    slot_count;
    moq_mpeg2ts_receiver_t      rx;
    moq_mpeg2ts_reorder_slot_t *slots;
    merge_held_t               *held;
    /* Locations recently handed back or given up, to tell a duplicate from
     * a late Object: two per slot. */
    merge_loc_t                *released;
    size_t                      released_cap;
    size_t                      released_next;
    size_t                      released_count;
    bool                        started;
    uint64_t                    last_group;
    uint64_t                    last_object;
    moq_media_merge_path_stats_t stats[MOQ_MEDIA_MERGE_MAX_PATHS];
};

static bool loc_le(uint64_t ag, uint64_t ao, uint64_t bg, uint64_t bo)
{
    return ag < bg || (ag == bg && ao <= bo);
}

static void obj_release(moq_media_object_t *obj, size_t obj_size)
{
    moq_media_object_cleanup(obj);
    memset(obj, 0, obj_size);
}

static bool held_has(const moq_media_merge_t *m, uint64_t g, uint64_t o)
{
    for (uint32_t i = 0; i < m->slot_count; i++)
        if (m->held[i].used && m->held[i].obj.group_id == g && m->held[i].obj.object_id == o)
            return true;
    return false;
}

static bool released_has(const moq_media_merge_t *m, uint64_t g, uint64_t o)
{
    for (size_t i = 0; i < m->released_count; i++)
        if (m->released[i].group == g && m->released[i].object == o) return true;
    return false;
}

static void released_add(moq_media_merge_t *m, uint64_t g, uint64_t o)
{
    m->released[m->released_next] = (merge_loc_t){ g, o };
    m->released_next = (m->released_next + 1) % m->released_cap;
    if (m->released_count < m->released_cap) m->released_count++;
}

static void merge_free(moq_media_merge_t *m)
{
    const moq_alloc_t a = m->alloc;
    if (m->held) {
        for (uint32_t i = 0; i < m->slot_count; i++)
            if (m->held[i].used) moq_media_object_cleanup(&m->held[i].obj);
        a.free(m->held, m->slot_count * sizeof(*m->held), a.ctx);
    }
    if (m->slots) a.free(m->slots, m->slot_count * sizeof(*m->slots), a.ctx);
    if (m->released) a.free(m->released, m->released_cap * sizeof(*m->released), a.ctx);
    a.free(m, sizeof(*m), a.ctx);
}

moq_result_t moq_media_merge_create(const moq_media_merge_cfg_t *cfg,
                                    const moq_alloc_t *alloc, moq_media_merge_t **out)
{
    if (!out) return MOQ_ERR_INVAL;
    *out = NULL;
    if (!cfg || cfg->struct_size < MERGE_CFG_MIN_SIZE) return MOQ_ERR_INVAL;
    if (cfg->path_count < 2 || cfg->path_count > MOQ_MEDIA_MERGE_MAX_PATHS ||
        (cfg->packet_size != 188 && cfg->packet_size != 192) ||
        cfg->slot_count == 0 || cfg->slot_count > MERGE_MAX_SLOTS || cfg->window_us == 0)
        return MOQ_ERR_INVAL;
    if (!alloc) alloc = moq_alloc_default();
    if (!alloc->alloc || !alloc->free) return MOQ_ERR_INVAL;

    moq_media_merge_t *m = (moq_media_merge_t *)alloc->alloc(sizeof(*m), alloc->ctx);
    if (!m) return MOQ_ERR_NOMEM;
    memset(m, 0, sizeof(*m));
    m->alloc = *alloc;
    m->path_count = cfg->path_count;
    m->slot_count = cfg->slot_count;
    m->released_cap = (size_t)cfg->slot_count * 2u;
    m->slots = (moq_mpeg2ts_reorder_slot_t *)alloc->alloc(m->slot_count * sizeof(*m->slots),
                                                          alloc->ctx);
    m->held = (merge_held_t *)alloc->alloc(m->slot_count * sizeof(*m->held), alloc->ctx);
    m->released = (merge_loc_t *)alloc->alloc(m->released_cap * sizeof(*m->released),
                                              alloc->ctx);
    if (!m->slots || !m->held || !m->released) {
        merge_free(m);
        return MOQ_ERR_NOMEM;
    }
    memset(m->slots, 0, m->slot_count * sizeof(*m->slots));
    memset(m->held, 0, m->slot_count * sizeof(*m->held));

    moq_mpeg2ts_receiver_cfg_t rc;
    moq_mpeg2ts_receiver_cfg_init(&rc);
    rc.packet_size = cfg->packet_size;
    rc.program_number = cfg->program_number;
    rc.reorder_slots = m->slots;
    rc.reorder_slot_count = m->slot_count;
    rc.reorder_window_us = cfg->window_us;
    moq_result_t rc_init = moq_mpeg2ts_receiver_init(&m->rx, sizeof(m->rx), &rc);
    if (rc_init != MOQ_OK) {
        merge_free(m);
        return rc_init;
    }
    for (uint32_t p = 0; p < MOQ_MEDIA_MERGE_MAX_PATHS; p++)
        m->stats[p].struct_size = sizeof(m->stats[p]);
    *out = m;
    return MOQ_OK;
}

void moq_media_merge_destroy(moq_media_merge_t *m)
{
    if (m) merge_free(m);
}

moq_result_t moq_media_merge_offer(moq_media_merge_t *m, uint32_t path, uint64_t now_us,
                                   moq_media_object_t *obj, size_t obj_size)
{
    if (!m || !obj || path >= m->path_count) return MOQ_ERR_INVAL;
    if (obj_size < MERGE_OBJ_IDENTITY_SIZE || obj->struct_size < MERGE_OBJ_IDENTITY_SIZE ||
        obj->struct_size > obj_size || obj->packaging != MOQ_MEDIA_PACKAGING_MPEG2TS)
        return MOQ_ERR_INVAL;
    moq_media_merge_path_stats_t *st = &m->stats[path];
    const uint64_t g = obj->group_id, o = obj->object_id;

    if (obj->status != MOQ_OBJECT_STATUS_NORMAL) {
        /* No payload to merge. An END_OF_GROUP status ends its Group. */
        if (obj->status == MOQ_OBJECT_STATUS_END_OF_GROUP)
            moq_mpeg2ts_receiver_end_group_at(&m->rx, g, o);
        st->offered++;
        st->last_offer_us = now_us;
        obj_release(obj, obj_size);
        return MOQ_OK;
    }
    if (held_has(m, g, o) || released_has(m, g, o)) {
        st->offered++;
        st->duplicates++;
        st->last_offer_us = now_us;
        obj_release(obj, obj_size);
        return MOQ_OK;
    }
    if (m->started && loc_le(g, o, m->last_group, m->last_object)) {
        st->offered++;
        st->late++;
        st->last_offer_us = now_us;
        obj_release(obj, obj_size);
        return MOQ_OK;
    }

    merge_held_t *h = NULL;
    for (uint32_t i = 0; i < m->slot_count && !h; i++)
        if (!m->held[i].used) h = &m->held[i];
    if (!h) return MOQ_ERR_BUFFER;
    const uint32_t flags = obj->end_of_group ? MOQ_MPEG2TS_OBJ_SUBGROUP_ENDS_GROUP : 0u;
    moq_result_t rc = moq_mpeg2ts_receiver_offer(&m->rx, now_us, g, o, flags, obj->payload, h);
    if (rc == MOQ_ERR_BUFFER) return MOQ_ERR_BUFFER;
    st->offered++;
    st->last_offer_us = now_us;
    if (rc != MOQ_OK) {
        /* The window refused it as late or duplicated after all. */
        st->late++;
        obj_release(obj, obj_size);
        return MOQ_OK;
    }
    /* The payload stays where it is, in the Object's buffer: moving the
     * struct moves only the reference the window already holds. */
    h->used = true;
    h->path = path;
    memset(&h->obj, 0, sizeof(h->obj));
    memcpy(&h->obj, obj, obj->struct_size);
    memset(obj, 0, obj_size);
    return MOQ_OK;
}

moq_result_t moq_media_merge_end_group(moq_media_merge_t *m, uint32_t path, uint64_t group)
{
    if (!m || path >= m->path_count) return MOQ_ERR_INVAL;
    moq_mpeg2ts_receiver_end_group(&m->rx, group);
    return MOQ_OK;
}

moq_result_t moq_media_merge_poll(moq_media_merge_t *m, uint64_t now_us,
                                  moq_media_object_t *obj, size_t obj_size, uint32_t *path)
{
    if (!m || !obj) return MOQ_ERR_INVAL;
    /* Nothing can be put back once the window releases it, so check first
     * that the caller's struct takes any held Object whole. */
    for (uint32_t i = 0; i < m->slot_count; i++)
        if (m->held[i].used && m->held[i].obj.struct_size > obj_size) return MOQ_ERR_BUFFER;

    for (;;) {
        moq_mpeg2ts_delivery_t d;
        moq_mpeg2ts_delivery_init(&d);
        moq_result_t rc = moq_mpeg2ts_receiver_poll(&m->rx, now_us, &d, sizeof(d));
        if (rc != MOQ_OK) return rc;
        merge_held_t *h = (merge_held_t *)d.ctx;
        released_add(m, d.group, d.object);
        m->started = true;
        m->last_group = d.group;
        m->last_object = d.object;
        if (!d.result.accepted) {
            /* Not valid MPEG-TS: dropped, and the window counts it missing. */
            m->stats[h->path].invalid++;
            moq_media_object_cleanup(&h->obj);
            memset(&h->obj, 0, sizeof(h->obj));
            h->used = false;
            continue;
        }
        if (h->obj.struct_size >= MERGE_OBJ_LOSS_SIZE) {
            /* The report describes the merged stream, not one path. */
            h->obj.has_loss_report = true;
            h->obj.gap = d.result.gap;
            h->obj.gap_group = d.result.gap_group;
            h->obj.gap_object = d.result.gap_object;
            h->obj.gap_tail_unknown = d.result.gap_tail_unknown;
            h->obj.gap_duration_known = d.result.gap_duration_known;
            h->obj.gap_duration_27mhz = d.result.gap_duration_27mhz;
            h->obj.discontinuous = d.result.discontinuous;
        }
        h->obj.track = NULL;
        memset(obj, 0, obj_size);
        memcpy(obj, &h->obj, h->obj.struct_size);
        if (path) *path = h->path;
        m->stats[h->path].delivered++;
        memset(&h->obj, 0, sizeof(h->obj));
        h->used = false;
        return MOQ_OK;
    }
}

uint64_t moq_media_merge_next_deadline_us(const moq_media_merge_t *m)
{
    return m ? moq_mpeg2ts_receiver_next_deadline_us(&m->rx) : UINT64_MAX;
}

void moq_media_merge_finish(moq_media_merge_t *m)
{
    if (m) moq_mpeg2ts_receiver_finish(&m->rx);
}

moq_result_t moq_media_merge_get_path_stats(const moq_media_merge_t *m, uint32_t path,
                                            moq_media_merge_path_stats_t *stats,
                                            size_t stats_size)
{
    if (!m || !stats || path >= m->path_count || stats_size < sizeof(uint32_t))
        return MOQ_ERR_INVAL;
    size_t n = stats_size < sizeof(m->stats[path]) ? stats_size : sizeof(m->stats[path]);
    memcpy(stats, &m->stats[path], n);
    stats->struct_size = (uint32_t)n;
    return MOQ_OK;
}

#else /* !MOQ_SERVICE_HAVE_MPEG2TS */

struct moq_media_merge {
    int unused;
};

moq_result_t moq_media_merge_create(const moq_media_merge_cfg_t *cfg,
                                    const moq_alloc_t *alloc, moq_media_merge_t **out)
{
    (void)cfg;
    (void)alloc;
    if (out) *out = NULL;
    return out ? MOQ_ERR_UNSUPPORTED : MOQ_ERR_INVAL;
}

void moq_media_merge_destroy(moq_media_merge_t *m) { (void)m; }

moq_result_t moq_media_merge_offer(moq_media_merge_t *m, uint32_t path, uint64_t now_us,
                                   moq_media_object_t *obj, size_t obj_size)
{
    (void)m; (void)path; (void)now_us; (void)obj; (void)obj_size;
    return MOQ_ERR_INVAL;
}

moq_result_t moq_media_merge_end_group(moq_media_merge_t *m, uint32_t path, uint64_t group)
{
    (void)m; (void)path; (void)group;
    return MOQ_ERR_INVAL;
}

moq_result_t moq_media_merge_poll(moq_media_merge_t *m, uint64_t now_us,
                                  moq_media_object_t *obj, size_t obj_size, uint32_t *path)
{
    (void)m; (void)now_us; (void)obj; (void)obj_size; (void)path;
    return MOQ_ERR_INVAL;
}

uint64_t moq_media_merge_next_deadline_us(const moq_media_merge_t *m)
{
    (void)m;
    return UINT64_MAX;
}

void moq_media_merge_finish(moq_media_merge_t *m) { (void)m; }

moq_result_t moq_media_merge_get_path_stats(const moq_media_merge_t *m, uint32_t path,
                                            moq_media_merge_path_stats_t *stats,
                                            size_t stats_size)
{
    (void)m; (void)path; (void)stats; (void)stats_size;
    return MOQ_ERR_INVAL;
}

#endif /* MOQ_SERVICE_HAVE_MPEG2TS */
