#include "relay_internal.h"
#include "delivery_internal.h"
#include "namespace_internal.h"

/* -- route dump (read-only introspection) -------------------------------------------------------
 *
 * A deterministic, entity-detailed snapshot of the routing state: the epoch
 * triple, announced namespaces, namespace watchers, and every live track
 * with its log watermarks and per-subscription cursor lag/state. Pure:
 * renders into a caller buffer, no mutation, no advancing calls, no
 * allocation. Ordering is pool-slot order, so the same state renders
 * identically every time. Same truncation contract as the metrics/trace
 * serializers (MOQR_OK only when content + NUL fit, else MOQR_ERR_CAPACITY
 * with the required length in *written). Unlike metrics, entity identity
 * (namespaces, names) IS included — this is an explicit operator dump, not
 * a bounded label space. */

typedef struct {
    char  *buf;
    size_t cap;
    size_t len;
} rd_writer_t;


static void
rd_addf(rd_writer_t *w, const char *fmt, ...)
{
    char  *dst = (w->len < w->cap) ? w->buf + w->len : NULL;
    size_t avail = (w->len < w->cap) ? w->cap - w->len : 0;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(dst, avail, fmt, ap);
    va_end(ap);
    if (n > 0) {
        w->len += (size_t)n;
    }
}


/* Namespace/track-name bytes are arbitrary; render them as an all-ASCII,
 * always-valid JSON string (non-printables and non-ASCII as \u00XX) so a
 * binary name can never break the document or leak a raw byte. */
static void
rd_json_bytes(rd_writer_t *w, const uint8_t *data, uint32_t len)
{
    rd_addf(w, "\"");
    for (uint32_t i = 0; i < len; i++) {
        uint8_t ch = data[i];
        if (ch == '"' || ch == '\\') {
            rd_addf(w, "\\%c", ch);
        } else if (ch == '\n') {
            rd_addf(w, "\\n");
        } else if (ch == '\t') {
            rd_addf(w, "\\t");
        } else if (ch == '\r') {
            rd_addf(w, "\\r");
        } else if (ch >= 0x20 && ch < 0x7f) {
            rd_addf(w, "%c", ch);
        } else {
            rd_addf(w, "\\u%04x", ch);
        }
    }
    rd_addf(w, "\"");
}


/* Text form: printable ASCII passes; the path separator, backslash, quote,
 * and any non-printable render as \xXX so a part is never mistaken for a
 * boundary or blur the "name" quoting. */
static void
rd_text_bytes(rd_writer_t *w, const uint8_t *data, uint32_t len)
{
    for (uint32_t i = 0; i < len; i++) {
        uint8_t ch = data[i];
        if (ch >= 0x20 && ch < 0x7f && ch != '\\' && ch != '/' &&
            ch != '"') {
            rd_addf(w, "%c", ch);
        } else {
            rd_addf(w, "\\x%02x", ch);
        }
    }
}


/* Walk a trie node's namespace path (root-exclusive) into a bounded stack,
 * then render it leaf-last. json=true → ["a","b"]; json=false → a/b. */
static void
rd_node_ns(rd_writer_t *w, const moqr_core_t *c, uint32_t node, bool json)
{
    uint32_t stack[64];
    uint32_t depth = 0;
    for (uint32_t n = node; n != UINT32_MAX && depth < 64 &&
                            c->nodes[n].part != NULL;
         n = c->nodes[n].parent) {
        stack[depth++] = n;
    }
    if (json) {
        rd_addf(w, "[");
    }
    for (uint32_t i = depth; i > 0; i--) {
        const r_trie_node_t *nd = &c->nodes[stack[i - 1]];
        if (i != depth) {
            rd_addf(w, json ? "," : "/");
        }
        if (json) {
            rd_json_bytes(w, nd->part, nd->part_len);
        } else {
            rd_text_bytes(w, nd->part, nd->part_len);
        }
    }
    if (json) {
        rd_addf(w, "]");
    }
}


/* Render a track key's namespace parts (name excluded). */
static void
rd_key_ns(rd_writer_t *w, const r_key_t *k, bool json)
{
    uint32_t count = key_part_count(k);
    if (json) {
        rd_addf(w, "[");
    }
    for (uint32_t i = 0; i < count; i++) {
        moq_bytes_t p = key_part(k, i);
        if (i != 0) {
            rd_addf(w, json ? "," : "/");
        }
        if (json) {
            rd_json_bytes(w, p.data, (uint32_t)p.len);
        } else {
            rd_text_bytes(w, p.data, (uint32_t)p.len);
        }
    }
    if (json) {
        rd_addf(w, "]");
    }
}


/* Render an ns-sub prefix (key layout without the trailing name slot). */
static void
rd_prefix(rd_writer_t *w, const uint8_t *prefix, bool json)
{
    uint32_t count = key_rd32(prefix);
    uint32_t off = 4u * (1u + count);
    if (json) {
        rd_addf(w, "[");
    }
    for (uint32_t i = 0; i < count; i++) {
        uint32_t plen = key_rd32(prefix + 4u * (1u + i));
        if (i != 0) {
            rd_addf(w, json ? "," : "/");
        }
        if (json) {
            rd_json_bytes(w, prefix + off, plen);
        } else {
            rd_text_bytes(w, prefix + off, plen);
        }
        off += plen;
    }
    if (json) {
        rd_addf(w, "]");
    }
}


/*
 * Read-only cursor backlog: count the retained, in-range groups that still
 * owe this subscription a filter-passing record, and the oldest such group
 * (the delivery frontier). Mirrors sub_best_candidate's deliverability test
 * against the persisted cursor indices WITHOUT mutating them — a local copy
 * of the read position skips below-start records. `frontier` is meaningful
 * only when `lag_groups` is nonzero; UINT64_MAX is a valid owed group.
 */
static void
sub_backlog(const moqr_core_t *c, const r_sub_t *s, const moqr_log_t *log,
            uint64_t *frontier, uint64_t *lag_groups)
{
    (void)c;
    *frontier = UINT64_MAX;
    *lag_groups = 0;
    uint32_t gn = moqr_log_group_count(log);
    for (uint32_t gi = 0; gi < gn; gi++) {
        uint64_t g = moqr_log_group_id_at(log, gi);
        if (g < s->start_group) {
            continue;
        }
        if (s->has_end && g > s->end_group) {
            break;   /* ids ascend */
        }
        const r_gpos_t *e = NULL;
        for (uint32_t i = 0; i < s->gpos_groups; i++) {
            const r_gpos_t *cand = gpos_at(s, i);
            if (cand->occupied && cand->group_id == g) {
                e = cand;
                break;
            }
        }
        uint32_t lists = moqr_log_group_list_count(log, g);
        bool owed = false;
        for (uint32_t slot = 0; slot <= lists && !owed; slot++) {
            bool dg = slot == lists;
            uint32_t pos_slot = dg ? s->gpos_lists - 1 : slot;
            uint32_t list_ref = dg ? MOQR_LOG_LIST_DATAGRAM : slot;
            uint32_t pos = e != NULL ? e->idx[pos_slot] : 0;
            for (;;) {
                moqr_record_view_t v;
                if (moqr_log_read_rec(log, g, list_ref, pos, &v) != MOQR_OK) {
                    break;
                }
                if (rec_passes(s, &v)) {
                    owed = true;
                    break;
                }
                pos++;   /* local only: below filter start */
            }
        }
        if (owed) {
            (*lag_groups)++;
            if (*lag_groups == 1) {
                *frontier = g;
            }
        }
    }
}


static const char *
track_state_name(r_track_state_t st)
{
    switch (st) {
    case R_TRACK_PENDING: return "pending";
    case R_TRACK_ACTIVE:  return "active";
    case R_TRACK_WARM:    return "warm";
    case R_TRACK_RENDEZVOUS: return "rendezvous";
    default:              return "unknown";
    }
}


static const char *
sub_state_name(r_sub_state_t st)
{
    switch (st) {
    case R_SUB_PARKED: return "parked";
    case R_SUB_ACTIVE: return "active";
    default:           return "unknown";
    }
}


static void
rd_group_id(rd_writer_t *w, uint64_t id, bool present)
{
    if (!present) {
        rd_addf(w, "null");
    } else {
        rd_addf(w, "%llu", (unsigned long long)id);
    }
}


/* The bind-layer connection identity for a binding slot (the operator-
 * meaningful id, not the internal pool slot). Live announces/subs/ns-subs
 * imply a live binding, so the slot's current cookie is the right one. */
static uint64_t
rd_binding_cookie(const moqr_core_t *c, uint32_t slot)
{
    return c->bindings[slot].cookie;
}


moqr_result_t
moqr_core_route_dump_json(const moqr_core_t *c, char *buf, size_t cap,
                          size_t *written)
{
    if (written != NULL) {
        *written = 0;
    }
    if (c == NULL || buf == NULL) {
        return MOQR_ERR_INVAL;
    }
    rd_writer_t w = { buf, cap, 0 };
    rd_addf(&w, "{\"epochs\":{\"node\":%llu,\"shard\":%llu,\"route\":%llu}",
            (unsigned long long)c->node_epoch,
            (unsigned long long)c->shard_epoch,
            (unsigned long long)c->route_epoch);

    rd_addf(&w, ",\"announces\":[");
    bool first = true;
    for (uint32_t n = 0; n < c->node_count; n++) {
        if (!c->nodes[n].has_announce) {
            continue;
        }
        rd_addf(&w, first ? "{\"namespace\":" : ",{\"namespace\":");
        first = false;
        rd_node_ns(&w, c, n, true);
        rd_addf(&w, ",\"binding\":%llu}",
                (unsigned long long)rd_binding_cookie(c, c->nodes[n].ann_binding));
    }
    rd_addf(&w, "]");

    if (c->cluster_enabled) {
        rd_addf(&w, ",\"cluster_routes\":[");
        bool route_first = true;
        for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++) {
            const r_cluster_route_t *r = &c->cluster_routes[ri];
            if (!r->used) continue;
            rd_addf(&w, route_first ? "{\"namespace\":" : ",{\"namespace\":");
            route_first = false;
            rd_node_ns(&w, c, r->node, true);
            rd_addf(&w, ",\"binding\":%llu,\"generation\":%llu,\"arrival\":%llu,\"cost\":%llu,\"advertised_cost\":%llu,\"mirrored\":%s,\"hop_path\":[",
                (unsigned long long)c->bindings[r->binding].cookie,
                (unsigned long long)r->lifetime,
                (unsigned long long)r->arrival,
                (unsigned long long)r->cost,
                (unsigned long long)(cluster_route_warm(c, ri) ? 0 : r->cost),
                r->mirrored ? "true" : "false");
            for (uint32_t hi = 0; hi < r->hop_count; hi++)
                rd_addf(&w, hi == 0 ? "%llu" : ",%llu", (unsigned long long)cluster_path(c, ri)[hi]);
            rd_addf(&w, "]}");
        }
        rd_addf(&w, "]");
    }

    rd_addf(&w, ",\"namespace_subscriptions\":[");
    first = true;
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        if (!c->ns_subs[i].used) {
            continue;
        }
        rd_addf(&w, first ? "{\"prefix\":" : ",{\"prefix\":");
        first = false;
        rd_prefix(&w, c->ns_subs[i].prefix, true);
        rd_addf(&w, ",\"binding\":%llu}",
                (unsigned long long)rd_binding_cookie(c, c->ns_subs[i].binding));
    }
    rd_addf(&w, "]");

    rd_addf(&w, ",\"tracks\":[");
    first = true;
    for (uint32_t t = 0; t < c->max_tracks; t++) {
        const r_track_t *tr = &c->tracks[t];
        if ((tr->gen & 1u) == 0) {
            continue;
        }
        rd_addf(&w, first ? "{\"namespace\":" : ",{\"namespace\":");
        first = false;
        rd_key_ns(&w, &tr->key, true);
        rd_addf(&w, ",\"name\":");
        moq_bytes_t nm = key_name(&tr->key);
        rd_json_bytes(&w, nm.data, (uint32_t)nm.len);
        rd_addf(&w, ",\"state\":\"%s\",\"upstream_binding\":",
                track_state_name(tr->state));
        if (tr->has_upstream_binding) {
            rd_addf(&w, "%llu",
                    (unsigned long long)rd_binding_cookie(c, tr->up_binding));
        } else {
            rd_addf(&w, "null");
        }
        moqr_log_stats_t ls;
        moqr_log_get_stats(tr->log, &ls);
        rd_addf(&w, ",\"log\":{\"oldest_group\":");
        rd_group_id(&w, ls.oldest_group_id, ls.group_count != 0);
        rd_addf(&w, ",\"newest_group\":");
        rd_group_id(&w, ls.newest_group_id, ls.group_count != 0);
        rd_addf(&w,
                ",\"groups\":%u,\"records\":%llu,\"retained_bytes\":%llu,"
                "\"end_of_track\":%s}",
                ls.group_count, (unsigned long long)ls.record_count,
                (unsigned long long)ls.retained_bytes,
                ls.end_of_track ? "true" : "false");

        rd_addf(&w, ",\"subscriptions\":[");
        bool sfirst = true;
        for (uint32_t si = 0; si < c->max_subs; si++) {
            const r_sub_t *s = &c->subs[si];
            if ((s->gen & 1u) == 0 || s->track != t ||
                s->track_gen_slot != tr->gen) {
                continue;
            }
            rd_addf(&w, sfirst ? "{" : ",{");
            sfirst = false;
            rd_addf(&w,
                    "\"binding\":%llu,\"state\":\"%s\",\"start_group\":%llu,"
                    "\"start_object\":%llu,\"end_group\":",
                    (unsigned long long)rd_binding_cookie(c, s->binding),
                    sub_state_name(s->state),
                    (unsigned long long)s->start_group,
                    (unsigned long long)s->start_object);
            if (s->has_end) {
                rd_addf(&w, "%llu", (unsigned long long)s->end_group);
            } else {
                rd_addf(&w, "null");
            }
            uint64_t frontier = 0;
            uint64_t lag = 0;
            sub_backlog(c, s, tr->log, &frontier, &lag);
            bool before_ret = s->state == R_SUB_ACTIVE &&
                              ls.group_count != 0 &&
                              s->start_group < ls.oldest_group_id;
            rd_addf(&w, ",\"lag_groups\":%llu,\"frontier_group\":",
                    (unsigned long long)lag);
            rd_group_id(&w, frontier, lag != 0);
            rd_addf(&w,
                    ",\"skip_pending\":%s,\"start_before_retention\":%s}",
                    s->pending_skip ? "true" : "false",
                    before_ret ? "true" : "false");
        }
        rd_addf(&w, "]}");
    }
    rd_addf(&w, "]}");

    if (cap > 0) {
        buf[w.len < cap ? w.len : cap - 1] = '\0';
    }
    if (written != NULL) {
        *written = w.len;
    }
    return w.len < cap ? MOQR_OK : MOQR_ERR_CAPACITY;
}


moqr_result_t
moqr_core_route_dump_text(const moqr_core_t *c, char *buf, size_t cap,
                          size_t *written)
{
    if (written != NULL) {
        *written = 0;
    }
    if (c == NULL || buf == NULL) {
        return MOQR_ERR_INVAL;
    }
    rd_writer_t w = { buf, cap, 0 };
    rd_addf(&w, "epochs: node=%llu shard=%llu route=%llu\n",
            (unsigned long long)c->node_epoch,
            (unsigned long long)c->shard_epoch,
            (unsigned long long)c->route_epoch);

    rd_addf(&w, "announces:\n");
    bool any = false;
    for (uint32_t n = 0; n < c->node_count; n++) {
        if (!c->nodes[n].has_announce) {
            continue;
        }
        any = true;
        rd_addf(&w, "  ");
        rd_node_ns(&w, c, n, false);
        rd_addf(&w, " -> binding %llu\n",
                (unsigned long long)rd_binding_cookie(c, c->nodes[n].ann_binding));
    }
    if (!any) {
        rd_addf(&w, "  (none)\n");
    }

    if (c->cluster_enabled) {
        rd_addf(&w, "cluster_routes:\n");
        for (uint32_t ri = 0; ri < c->max_cluster_routes; ri++) {
            const r_cluster_route_t *r = &c->cluster_routes[ri];
            if (!r->used) continue;
            rd_addf(&w, "  ");
            rd_node_ns(&w, c, r->node, false);
            rd_addf(&w, " -> binding %llu generation=%llu cost=%llu advertised_cost=%llu path=[",
                (unsigned long long)c->bindings[r->binding].cookie,
                (unsigned long long)r->lifetime,
                (unsigned long long)r->cost,
                (unsigned long long)(cluster_route_warm(c, ri) ? 0 : r->cost));
            for (uint32_t hi = 0; hi < r->hop_count; hi++)
                rd_addf(&w, hi == 0 ? "%llu" : ",%llu", (unsigned long long)cluster_path(c, ri)[hi]);
            rd_addf(&w, "]\n");
        }
    }

    rd_addf(&w, "namespace_subscriptions:\n");
    any = false;
    for (uint32_t i = 0; i < c->max_ns_subs; i++) {
        if (!c->ns_subs[i].used) {
            continue;
        }
        any = true;
        rd_addf(&w, "  ");
        rd_prefix(&w, c->ns_subs[i].prefix, false);
        rd_addf(&w, " -> binding %llu\n",
                (unsigned long long)rd_binding_cookie(c, c->ns_subs[i].binding));
    }
    if (!any) {
        rd_addf(&w, "  (none)\n");
    }

    rd_addf(&w, "tracks:\n");
    any = false;
    for (uint32_t t = 0; t < c->max_tracks; t++) {
        const r_track_t *tr = &c->tracks[t];
        if ((tr->gen & 1u) == 0) {
            continue;
        }
        any = true;
        rd_addf(&w, "  ");
        rd_key_ns(&w, &tr->key, false);
        rd_addf(&w, " \"");
        moq_bytes_t nm = key_name(&tr->key);
        rd_text_bytes(&w, nm.data, (uint32_t)nm.len);
        rd_addf(&w, "\" state=%s upstream_binding=", track_state_name(tr->state));
        if (tr->has_upstream_binding) {
            rd_addf(&w, "%llu\n",
                    (unsigned long long)rd_binding_cookie(c, tr->up_binding));
        } else {
            rd_addf(&w, "-\n");
        }
        moqr_log_stats_t ls;
        moqr_log_get_stats(tr->log, &ls);
        rd_addf(&w, "    log: groups=%u records=%llu bytes=%llu oldest=",
                ls.group_count, (unsigned long long)ls.record_count,
                (unsigned long long)ls.retained_bytes);
        rd_group_id(&w, ls.oldest_group_id, ls.group_count != 0);
        rd_addf(&w, " newest=");
        rd_group_id(&w, ls.newest_group_id, ls.group_count != 0);
        rd_addf(&w, " eot=%d\n", ls.end_of_track ? 1 : 0);

        rd_addf(&w, "    subs:\n");
        bool anysub = false;
        for (uint32_t si = 0; si < c->max_subs; si++) {
            const r_sub_t *s = &c->subs[si];
            if ((s->gen & 1u) == 0 || s->track != t ||
                s->track_gen_slot != tr->gen) {
                continue;
            }
            anysub = true;
            uint64_t frontier = 0;
            uint64_t lag = 0;
            sub_backlog(c, s, tr->log, &frontier, &lag);
            bool before_ret = s->state == R_SUB_ACTIVE &&
                              ls.group_count != 0 &&
                              s->start_group < ls.oldest_group_id;
            rd_addf(&w,
                    "      binding=%llu state=%s start=%llu/%llu end=",
                    (unsigned long long)rd_binding_cookie(c, s->binding),
                    sub_state_name(s->state),
                    (unsigned long long)s->start_group,
                    (unsigned long long)s->start_object);
            if (s->has_end) {
                rd_addf(&w, "%llu", (unsigned long long)s->end_group);
            } else {
                rd_addf(&w, "-");
            }
            rd_addf(&w, " lag_groups=%llu frontier=", (unsigned long long)lag);
            rd_group_id(&w, frontier, lag != 0);
            rd_addf(&w, " skip_pending=%d start_before_retention=%d\n",
                    s->pending_skip ? 1 : 0, before_ret ? 1 : 0);
        }
        if (!anysub) {
            rd_addf(&w, "      (none)\n");
        }
    }
    if (!any) {
        rd_addf(&w, "  (none)\n");
    }

    if (cap > 0) {
        buf[w.len < cap ? w.len : cap - 1] = '\0';
    }
    if (written != NULL) {
        *written = w.len;
    }
    return w.len < cap ? MOQR_OK : MOQR_ERR_CAPACITY;
}
