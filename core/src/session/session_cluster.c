#include "session_internal.h"
#include "../wire/params_d22_internal.h"
#include "moq/vi64.h"

moq_result_t session_cluster_validate(const moq_cluster_metadata_t *m, bool required)
{
    if (!m || (required && !m->hop_count) || (m->hop_count && !m->hop_ids) ||
        m->hop_count > UINT16_MAX)
        return MOQ_ERR_INVAL;
    size_t bytes = 0;
    for (size_t i = 0; i < m->hop_count; i++) {
        size_t n = moq_vi64_len(m->hop_ids[i]);
        if (n > UINT16_MAX - bytes)
            return MOQ_ERR_INVAL;
        bytes += n;
        if (!m->hop_ids[i])
            continue;
        for (size_t j = 0; j < i; j++)
            if (m->hop_ids[i] == m->hop_ids[j])
                return MOQ_ERR_INVAL;
    }
    return MOQ_OK;
}
void session_cluster_free(moq_session_t *s, moq_cluster_metadata_t *m)
{
    if (m->hop_ids)
        s->alloc.free((void *)m->hop_ids, m->hop_count * sizeof(uint64_t), s->alloc.ctx);
    memset(m, 0, sizeof(*m));
}
moq_result_t session_cluster_copy(moq_session_t *s, const moq_cluster_metadata_t *in,
                                  moq_cluster_metadata_t *out)
{
    moq_cluster_metadata_t v = *in;
    if (in->hop_count) {
        uint64_t *ids = s->alloc.alloc(in->hop_count * sizeof(*ids), s->alloc.ctx);
        if (!ids)
            return MOQ_ERR_NOMEM;
        memcpy(ids, in->hop_ids, in->hop_count * sizeof(*ids));
        v.hop_ids = ids;
    } else
        v.hop_ids = NULL;
    *out = v;
    return MOQ_OK;
}
moq_result_t session_cluster_decode(moq_session_t *s, moq_bytes_t path, bool has_cost,
                                    uint64_t cost, moq_cluster_metadata_t *out)
{
    moq_cluster_metadata_t v = {.has_route_cost = has_cost, .route_cost = has_cost ? cost : 0};
    moq_buf_reader_t r = {path.data, path.len, 0};
    uint64_t id;
    while (r.pos < r.len) {
        if (moq_buf_read_vi64(&r, &id) != MOQ_OK)
            return MOQ_ERR_PROTO;
        ++v.hop_count;
    }
    if (v.hop_count) {
        uint64_t *ids = s->alloc.alloc(v.hop_count * sizeof(*ids), s->alloc.ctx);
        if (!ids)
            return MOQ_ERR_NOMEM;
        r.pos = 0;
        for (size_t i = 0; i < v.hop_count; i++)
            (void)moq_buf_read_vi64(&r, &ids[i]);
        v.hop_ids = ids;
    }
    *out = v;
    return MOQ_OK;
}
moq_result_t session_cluster_event(moq_session_t *s, const moq_cluster_metadata_t *in,
                                   moq_cluster_metadata_t *out)
{
    moq_cluster_metadata_t v = *in;
    if (in->hop_count) {
        uint64_t *ids =
            event_scratch_alloc_aligned(s, in->hop_count * sizeof(*ids), _Alignof(uint64_t));
        if (!ids)
            return MOQ_ERR_WOULD_BLOCK;
        memcpy(ids, in->hop_ids, in->hop_count * sizeof(*ids));
        v.hop_ids = ids;
    }
    *out = v;
    return MOQ_OK;
}
void session_cluster_apply(moq_session_t *s, moq_cluster_metadata_t *current,
                           moq_cluster_metadata_t *update)
{
    if (update->hop_count) {
        if (current->hop_ids)
            s->alloc.free((void *)current->hop_ids, current->hop_count * sizeof(uint64_t),
                          s->alloc.ctx);
        current->hop_ids = update->hop_ids;
        current->hop_count = update->hop_count;
        update->hop_ids = NULL;
        update->hop_count = 0;
    }
    if (update->has_route_cost) {
        current->has_route_cost = true;
        current->route_cost = update->route_cost;
    }
}
moq_result_t moq_session_cluster_info(const moq_session_t *s, moq_cluster_info_t *out,
                                      size_t out_size)
{
    if (!s || !out || out_size < MOQ_CLUSTER_INFO_V0_SIZE)
        return MOQ_ERR_INVAL;
    moq_cluster_info_t v = {.struct_size = sizeof(v),
                            .enabled = s->cluster_enabled,
                            .negotiated = s->cluster_negotiated,
                            .peer_has_relay_cost = s->cluster_peer_has_relay_cost,
                            .local_hop_id = s->cluster_hop_id,
                            .peer_hop_id = s->cluster_peer_hop_id,
                            .peer_relay_cost = s->cluster_peer_relay_cost};
    size_t n = out_size < sizeof(v) ? out_size : sizeof(v);
    v.struct_size = (uint32_t)n;
    memcpy(out, &v, n);
    return MOQ_OK;
}
