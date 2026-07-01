#include "session_internal.h"
#include "../internal/validate.h"

static moq_result_t handle_subscriber_response(moq_session_t *s,
                                                int32_t slot);

/* Accumulate a tail byte length with overflow detection: *acc += add, returning
 * false if the sum would wrap. A wrapped tail total would look falsely small to
 * event_scratch_fits_aligned, so callers route a false here through the same
 * "does not fit" path. */
static bool tail_len_add(size_t *acc, size_t add)
{
    if (*acc > SIZE_MAX - add) return false;
    *acc += add;
    return true;
}

/* -- Prefix helpers ------------------------------------------------ */

/* -- Suffix key helpers -------------------------------------------- */

typedef struct {
    uint8_t *data;
    size_t   len;
} ns_suffix_key_t;

static size_t ns_suffix_key_len(const moq_namespace_t *ns)
{
    size_t total = 1;
    for (size_t i = 0; i < ns->count; i++)
        total += 2 + ns->parts[i].len;
    return total;
}

static void ns_suffix_key_build(const moq_namespace_t *ns,
                                 uint8_t *buf)
{
    size_t off = 0;
    buf[off++] = (uint8_t)ns->count;
    for (size_t i = 0; i < ns->count; i++) {
        uint16_t plen = (uint16_t)ns->parts[i].len;
        buf[off++] = (uint8_t)(plen >> 8);
        buf[off++] = (uint8_t)(plen & 0xFF);
        if (plen > 0)
            memcpy(buf + off, ns->parts[i].data, plen);
        off += plen;
    }
}

static bool ns_suffix_key_eq(const ns_suffix_key_t *a,
                               const uint8_t *b, size_t b_len)
{
    return a->len == b_len && memcmp(a->data, b, b_len) == 0;
}

/* -- Announced suffix tracker ------------------------------------- */

#define NS_SUF_INIT_CAP 4

typedef struct {
    ns_suffix_key_t *keys;
    size_t           count;
    size_t           cap;
    /* Bytes charged to the session receive budget (s->recv_payload_bytes) for
     * this set: copied key storage plus key-array capacity. Only ever nonzero
     * for the inbound, peer-controlled set; the outbound publisher-side set
     * leaves this 0. Capacity growth is charged once and not released on key
     * removal (the array is not shrunk), so this tracks live allocation. */
    size_t           counted_bytes;
} ns_suffix_set_t;

/* a + b with overflow detection; a wrapped sum must refuse the insert. */
static bool ns_size_add(size_t a, size_t b, size_t *out)
{
    if (a > SIZE_MAX - b) return false;
    *out = a + b;
    return true;
}

static bool ns_suffix_set_contains(const ns_suffix_set_t *set,
                                    const uint8_t *key, size_t key_len)
{
    for (size_t i = 0; i < set->count; i++)
        if (ns_suffix_key_eq(&set->keys[i], key, key_len))
            return true;
    return false;
}

static bool ns_suffix_set_add(ns_suffix_set_t *set,
                               const moq_alloc_t *alloc,
                               const uint8_t *key, size_t key_len,
                               bool *out_inserted)
{
    if (out_inserted) *out_inserted = false;
    if (ns_suffix_set_contains(set, key, key_len)) return true;
    if (set->count >= set->cap) {
        size_t new_cap = set->cap ? set->cap * 2 : NS_SUF_INIT_CAP;
        size_t new_size = new_cap * sizeof(ns_suffix_key_t);
        ns_suffix_key_t *nk;
        if (set->keys) {
            nk = (ns_suffix_key_t *)alloc->realloc(
                set->keys, set->cap * sizeof(ns_suffix_key_t),
                new_size, alloc->ctx);
        } else {
            nk = (ns_suffix_key_t *)alloc->alloc(new_size, alloc->ctx);
        }
        if (!nk) return false;
        set->keys = nk;
        set->cap = new_cap;
    }
    uint8_t *copy = (uint8_t *)alloc->alloc(key_len, alloc->ctx);
    if (!copy) return false;
    memcpy(copy, key, key_len);
    set->keys[set->count].data = copy;
    set->keys[set->count].len = key_len;
    set->count++;
    if (out_inserted) *out_inserted = true;
    return true;
}

static bool ns_suffix_set_remove(ns_suffix_set_t *set,
                                  const moq_alloc_t *alloc,
                                  const uint8_t *key, size_t key_len)
{
    for (size_t i = 0; i < set->count; i++) {
        if (ns_suffix_key_eq(&set->keys[i], key, key_len)) {
            alloc->free(set->keys[i].data, set->keys[i].len, alloc->ctx);
            set->keys[i] = set->keys[--set->count];
            return true;
        }
    }
    return false;
}

static void ns_suffix_set_free(ns_suffix_set_t *set,
                                const moq_alloc_t *alloc)
{
    for (size_t i = 0; i < set->count; i++)
        alloc->free(set->keys[i].data, set->keys[i].len, alloc->ctx);
    if (set->keys)
        alloc->free(set->keys, set->cap * sizeof(ns_suffix_key_t),
                     alloc->ctx);
    set->keys = NULL;
    set->count = 0;
    set->cap = 0;
    set->counted_bytes = 0;
}

/* -- Inbound (receive-budget counted) suffix tracking -------------- *
 * The inbound, peer-controlled suffix set is charged against the session
 * receive budget so a peer cannot park unbounded NAMESPACE suffixes. These
 * variants are used only by handle_subscriber_response(); the outbound
 * publisher-side set keeps the uncounted helpers above. */

typedef enum {
    NS_SUF_DUP        = 0,   /* already present; no change, no charge */
    NS_SUF_INSERTED   = 1,   /* inserted; receive budget charged */
    NS_SUF_NOMEM      = -1,  /* allocation failure (retryable WOULD_BLOCK) */
    NS_SUF_OVER_BUDGET = -2, /* would exceed receive budget (caller must close) */
} ns_suffix_add_status_t;

/* Release n bytes back to the session receive budget (saturating). */
static void ns_recv_budget_release(moq_session_t *s, size_t n)
{
    s->recv_payload_bytes = n <= s->recv_payload_bytes
                                ? s->recv_payload_bytes - n : 0;
}

static ns_suffix_add_status_t ns_suffix_set_add_counted(
    moq_session_t *s, ns_suffix_set_t *set,
    const uint8_t *key, size_t key_len)
{
    if (ns_suffix_set_contains(set, key, key_len)) return NS_SUF_DUP;

    /* Grow the key array if needed, charging the capacity delta to the receive
     * budget before committing the realloc. */
    if (set->count >= set->cap) {
        size_t new_cap = set->cap ? set->cap * 2 : NS_SUF_INIT_CAP;
        if (new_cap < set->cap ||
            new_cap > SIZE_MAX / sizeof(ns_suffix_key_t))
            return NS_SUF_OVER_BUDGET;
        size_t grow = (new_cap - set->cap) * sizeof(ns_suffix_key_t);
        size_t projected;
        if (!ns_size_add(s->recv_payload_bytes, grow, &projected) ||
            projected > s->max_recv_buf)
            return NS_SUF_OVER_BUDGET;
        size_t new_size = new_cap * sizeof(ns_suffix_key_t);
        ns_suffix_key_t *nk = set->keys
            ? (ns_suffix_key_t *)s->alloc.realloc(set->keys,
                  set->cap * sizeof(ns_suffix_key_t), new_size, s->alloc.ctx)
            : (ns_suffix_key_t *)s->alloc.alloc(new_size, s->alloc.ctx);
        if (!nk) return NS_SUF_NOMEM;
        set->keys = nk;
        set->cap = new_cap;
        set->counted_bytes += grow;
        s->recv_payload_bytes += grow;
    }

    /* Charge the copied key bytes. */
    {
        size_t projected;
        if (!ns_size_add(s->recv_payload_bytes, key_len, &projected) ||
            projected > s->max_recv_buf)
            return NS_SUF_OVER_BUDGET;
    }
    uint8_t *copy = (uint8_t *)s->alloc.alloc(key_len, s->alloc.ctx);
    if (!copy) return NS_SUF_NOMEM;
    memcpy(copy, key, key_len);
    set->keys[set->count].data = copy;
    set->keys[set->count].len = key_len;
    set->count++;
    set->counted_bytes += key_len;
    s->recv_payload_bytes += key_len;
    return NS_SUF_INSERTED;
}

/* Remove a counted key, releasing its copied-key bytes from the receive budget.
 * Array capacity stays allocated (and counted) until the set is freed. */
static bool ns_suffix_set_remove_counted(moq_session_t *s, ns_suffix_set_t *set,
                                         const uint8_t *key, size_t key_len)
{
    for (size_t i = 0; i < set->count; i++) {
        if (ns_suffix_key_eq(&set->keys[i], key, key_len)) {
            size_t klen = set->keys[i].len;
            s->alloc.free(set->keys[i].data, klen, s->alloc.ctx);
            set->keys[i] = set->keys[--set->count];
            set->counted_bytes = klen <= set->counted_bytes
                                     ? set->counted_bytes - klen : 0;
            ns_recv_budget_release(s, klen);
            return true;
        }
    }
    return false;
}

/* -- Prefix helpers (shared with the track-sub pool) --------------- */

bool moq_prefix_overlaps(const moq_bytes_t *a_parts, size_t a_count,
                         const moq_bytes_t *b_parts, size_t b_count)
{
    if (a_count == 0 || b_count == 0) return true;
    size_t min_count = a_count < b_count ? a_count : b_count;
    for (size_t i = 0; i < min_count; i++) {
        if (a_parts[i].len != b_parts[i].len) return false;
        if (a_parts[i].len > 0 &&
            memcmp(a_parts[i].data, b_parts[i].data, a_parts[i].len) != 0)
            return false;
    }
    return true;
}

bool moq_prefix_store(moq_session_t *s, const moq_namespace_t *ns,
                      uint8_t **out_buf, size_t *out_buf_len,
                      moq_bytes_t **out_parts, size_t *out_count)
{
    if (ns->count == 0) {
        *out_count = 0;
        *out_parts = NULL;
        *out_buf = NULL;
        *out_buf_len = 0;
        return true;
    }

    size_t total = 0;
    for (size_t i = 0; i < ns->count; i++)
        total += ns->parts[i].len;

    moq_bytes_t *parts = (moq_bytes_t *)s->alloc.alloc(
        ns->count * sizeof(moq_bytes_t), s->alloc.ctx);
    if (!parts) return false;

    uint8_t *buf = NULL;
    if (total > 0) {
        buf = (uint8_t *)s->alloc.alloc(total, s->alloc.ctx);
        if (!buf) {
            s->alloc.free(parts, ns->count * sizeof(moq_bytes_t), s->alloc.ctx);
            return false;
        }
    }

    size_t off = 0;
    for (size_t i = 0; i < ns->count; i++) {
        parts[i].data = buf + off;
        parts[i].len = ns->parts[i].len;
        if (ns->parts[i].len > 0)
            memcpy(buf + off, ns->parts[i].data, ns->parts[i].len);
        off += ns->parts[i].len;
    }

    *out_parts = parts;
    *out_count = ns->count;
    *out_buf = buf;
    *out_buf_len = total;
    return true;
}

void moq_prefix_free(moq_session_t *s, uint8_t **buf, size_t *buf_len,
                     moq_bytes_t **parts, size_t *count)
{
    if (*buf) {
        s->alloc.free(*buf, *buf_len, s->alloc.ctx);
        *buf = NULL;
        *buf_len = 0;
    }
    if (*parts) {
        s->alloc.free(*parts, *count * sizeof(moq_bytes_t), s->alloc.ctx);
        *parts = NULL;
        *count = 0;
    }
}

static bool ns_sub_prefix_conflicts(moq_session_t *s,
                                     const moq_bytes_t *parts, size_t count,
                                     int exclude_slot)
{
    for (size_t i = 0; i < s->ns_sub_cap; i++) {
        if (s->ns_subs[i].state == MOQ_NS_SUB_FREE) continue;
        if ((int)i == exclude_slot) continue;
        /* Skip entries whose prefix has not been parsed/stored yet (e.g. an
         * inbound stream still in RECVING_PUBLISHER after only a partial
         * fragment). Their prefix_count==0 is not a real empty "match-all"
         * prefix, so it must not be treated as overlapping every namespace. */
        if (!s->ns_subs[i].prefix_valid) continue;
        if (moq_prefix_overlaps(s->ns_subs[i].prefix_parts,
                s->ns_subs[i].prefix_count, parts, count))
            return true;
    }
    return false;
}

static bool ns_sub_store_prefix(moq_session_t *s, moq_ns_sub_entry_t *e,
                                 const moq_namespace_t *ns)
{
    bool ok = moq_prefix_store(s, ns, &e->prefix_buf, &e->prefix_buf_len,
                               &e->prefix_parts, &e->prefix_count);
    /* A successfully stored prefix (including a genuine empty "match-all"
     * prefix with count==0) is now a real prefix the overlap scan must honor. */
    if (ok) e->prefix_valid = true;
    return ok;
}

static void ns_sub_free_prefix(moq_session_t *s, moq_ns_sub_entry_t *e)
{
    moq_prefix_free(s, &e->prefix_buf, &e->prefix_buf_len,
                    &e->prefix_parts, &e->prefix_count);
    e->prefix_valid = false;
}

/* ns_sub_free_entry is declared in session_internal.h (shared with the request
 * GOAWAY handler). */

static moq_result_t ns_sub_send_request_error(moq_session_t *s,
                                                size_t ns_slot,
                                                uint64_t error_code)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[ns_slot];

    /* Stream-correlated profiles (draft-18) free the request bidi here, so
     * reserve a drain slot up front: a request the peer sent before seeing our
     * REQUEST_ERROR + FIN is then discarded rather than mistaken for a fresh
     * request (the generic request path checks the drain ring first). Draft-16
     * keeps the ns_sub index mapped until teardown and never consults the ring,
     * so it does not drain. */
    bool drain = moq_session_uses_request_streams(s);
    if (drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    uint8_t ebuf[256];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, ebuf, sizeof(ebuf));
    moq_request_error_encode_args_t err_args = {
        .request_id = e->request_id,
        .error_code = error_code,
    };
    moq_result_t rc = s->profile->encode_request_error(s, &ew, &err_args);
    if (rc < 0) return rc;
    /* Stack-encode then queue on the bidi: a temporary send-buffer shortfall is
     * WOULD_BLOCK (retryable), not a hard BUFFER. Nothing is committed until the
     * response is queued. */
    rc = queue_send_bidi(s, e->stream_ref, ebuf, moq_buf_writer_offset(&ew), true);
    if (rc < 0) return rc;

    s->profile->commit_inbound_request(s, &e->request_ep);
    process_auth_tokens_commit_txn(s, &e->auth_txn);
    e->auth_committed = true;
    process_auth_tokens_free_staging(s, e->resolved_tokens,
        e->token_staged, e->token_count);
    if (drain && e->stream_ref._v != 0)
        (void)drain_ref_add(s, e->stream_ref);   /* slot reserved above */
    ns_sub_free_entry(s, ns_slot);
    return MOQ_OK;
}

/* -- Pool helpers -------------------------------------------------- */

static int ns_sub_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->ns_sub_cap; i++)
        if (s->ns_subs[i].state == MOQ_NS_SUB_FREE) return (int)i;
    return -1;
}

static moq_ns_sub_handle_t ns_sub_make_handle(moq_session_t *s, size_t slot)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                       s->session_tag,
                                       e->generation, (uint32_t)slot);
    moq_ns_sub_handle_t h = { packed };
    return h;
}

static int ns_sub_resolve_handle(moq_session_t *s, moq_ns_sub_handle_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_NAMESPACE_SUB) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->ns_sub_cap) return -1;
    if (s->ns_subs[slot].generation != gen) return -1;
    if (s->ns_subs[slot].state == MOQ_NS_SUB_FREE) return -1;
    return (int)slot;
}

void ns_sub_free_entry(moq_session_t *s, size_t slot)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    if (e->state == MOQ_NS_SUB_PENDING_SUBSCRIBER ||
        e->state == MOQ_NS_SUB_PENDING_PUBLISHER)
        request_registry_remove_by_id(s, e->request_id);
    moq_index_remove(s->idx_ns_by_ref, s->idx_ns_mask, e->stream_ref._v);
    process_auth_tokens_free_staging(s, e->resolved_tokens,
        e->token_staged, e->token_count);
    if (!e->auth_committed)
        process_auth_tokens_abort_txn(s, &e->auth_txn);
    ns_sub_free_prefix(s, e);
    if (e->announced_suffixes) {
        ns_suffix_set_t *set = (ns_suffix_set_t *)e->announced_suffixes;
        /* Return inbound-counted bytes to the receive budget before freeing. */
        if (e->announced_suffixes_inbound)
            ns_recv_budget_release(s, set->counted_bytes);
        ns_suffix_set_free(set, &s->alloc);
        s->alloc.free(e->announced_suffixes, sizeof(ns_suffix_set_t),
                       s->alloc.ctx);
        e->announced_suffixes = NULL;
    }
    e->announced_suffixes_inbound = false;
    e->goaway_sent = false;   /* selective free: clear the migration marker so a
                               * reused slot is never seen as already migrated */
    e->state = MOQ_NS_SUB_FREE;
    e->stream_kind = MOQ_STREAM_KIND_UNKNOWN;
    memset(&e->request_ep, 0, sizeof(e->request_ep));
    e->recv_len = 0;
    e->parse_complete = false;
    e->got_response = false;
    e->pending_fin = false;
    e->closing_remote_error = false;
    e->forward = false;
    e->auth_processed = false;
    e->auth_committed = false;
    e->auth_reject_code = 0;
    e->token_count = 0;
    e->generation++;
}

void ns_sub_destroy_all(moq_session_t *s)
{
    for (size_t i = 0; i < s->ns_sub_cap; i++) {
        moq_ns_sub_entry_t *e = &s->ns_subs[i];
        ns_sub_free_prefix(s, e);
        if (e->announced_suffixes) {
            ns_suffix_set_t *set = (ns_suffix_set_t *)e->announced_suffixes;
            if (e->announced_suffixes_inbound)
                ns_recv_budget_release(s, set->counted_bytes);
            ns_suffix_set_free(set, &s->alloc);
            s->alloc.free(e->announced_suffixes, sizeof(ns_suffix_set_t),
                           s->alloc.ctx);
            e->announced_suffixes = NULL;
            e->announced_suffixes_inbound = false;
        }
    }
}

/* -- Config init --------------------------------------------------- */

void moq_subscribe_namespace_cfg_init(moq_subscribe_namespace_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

void moq_accept_ns_sub_cfg_init(moq_accept_ns_sub_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

void moq_reject_ns_sub_cfg_init(moq_reject_ns_sub_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

/* -- Subscriber: subscribe_namespace ------------------------------- */

moq_result_t moq_session_subscribe_namespace(
    moq_session_t *s,
    const moq_subscribe_namespace_cfg_t *cfg,
    uint64_t now_us,
    moq_ns_sub_handle_t *out_handle)
{
    if (!s || !cfg || !out_handle) return MOQ_ERR_INVAL;
#define NS_CFG_MIN offsetof(moq_subscribe_namespace_cfg_t, auth_tokens)
    if (cfg->struct_size < NS_CFG_MIN) return MOQ_ERR_INVAL;
#define NS_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_subscribe_namespace_cfg_t, f) + sizeof(cfg->f))
    *out_handle = MOQ_NS_SUB_HANDLE_INVALID;

    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (NS_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count) < 0)
        return MOQ_ERR_INVAL;
#undef NS_CFG_HAS
#undef NS_CFG_MIN

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    if (cfg->namespace_interest > 2) return MOQ_ERR_INVAL;
    {
        const moq_namespace_t *pfx = &cfg->track_namespace_prefix;
        if (pfx->count > 32) return MOQ_ERR_INVAL;
        if (pfx->count > 0 && !pfx->parts) return MOQ_ERR_INVAL;
        size_t pfx_total = 0;
        for (size_t i = 0; i < pfx->count; i++) {
            if (pfx->parts[i].len == 0) return MOQ_ERR_INVAL;
            if (!pfx->parts[i].data) return MOQ_ERR_INVAL;
            if (pfx->parts[i].len > MOQ_FULL_TRACK_NAME_MAX - pfx_total)
                return MOQ_ERR_INVAL;
            pfx_total += pfx->parts[i].len;
        }
    }
    if (ns_sub_prefix_conflicts(s, cfg->track_namespace_prefix.parts,
            cfg->track_namespace_prefix.count, -1))
        return MOQ_ERR_INVAL;

    /* Check request ID credit via profile. */
    moq_request_endpoint_t req_ep;
    {
        moq_result_t prc = s->profile->prepare_request(s, &req_ep);
        if (prc < 0) return prc;
    }

    int slot = ns_sub_find_free(s);
    if (slot < 0) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_stream_ref_t ref = moq_stream_ref_from_u64(s->next_stream_ref);

    if (action_queue_full(s)) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    if (!ns_sub_store_prefix(s, e, &cfg->track_namespace_prefix)) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_NOMEM;
    }

    /* Encode directly into send_buf via profile op. */
    size_t avail = s->send_cap - s->send_len;
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len, avail);
    moq_subscribe_namespace_encode_args_t enc_args = {
        .request_id = req_ep.request_id,
        .prefix = cfg->track_namespace_prefix,
        .namespace_interest = (uint64_t)cfg->namespace_interest,
        .auth_tokens = auth_tokens,
        .auth_token_count = auth_token_count,
    };
    moq_result_t rc = s->profile->encode_subscribe_namespace(s, &w, &enc_args);
    if (rc < 0) {
        ns_sub_free_prefix(s, e);
        s->profile->abort_request(s, &req_ep);
        return rc;
    }
    size_t enc_len = moq_buf_writer_offset(&w);

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_OPEN_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_open_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.open_bidi_stream.stream_ref = ref;
    a.u.open_bidi_stream.data = s->send_buf + s->send_len;
    a.u.open_bidi_stream.len  = enc_len;
    rc = push_action(s, &a);
    if (rc < 0) {
        ns_sub_free_prefix(s, e);
        s->profile->abort_request(s, &req_ep);
        return rc;
    }

    s->send_len += enc_len;

    e->generation |= 1;
    e->state = MOQ_NS_SUB_PENDING_SUBSCRIBER;
    e->stream_kind = MOQ_STREAM_KIND_NAMESPACE_SUB;
    e->request_id = req_ep.request_id;
    e->stream_ref = ref;
    e->namespace_interest = cfg->namespace_interest;
    e->handle = ns_sub_make_handle(s, (size_t)slot);
    e->recv_len = 0;
    e->parse_complete = false;
    e->got_response = false;
    e->pending_fin = false;
    e->closing_remote_error = false;

    req_ep.kind = MOQ_REQ_NAMESPACE_SUB;
    req_ep.slot = slot;
    req_ep.has_stream_ref = true;
    req_ep.stream_ref = ref;
    request_registry_insert_by_id(s, req_ep.request_id, req_ep);
    moq_index_insert(s->idx_ns_by_ref, s->idx_ns_mask,
                     ref._v, slot);
    s->profile->commit_request(s, &req_ep);
    s->next_stream_ref++;

    *out_handle = e->handle;
    return MOQ_OK;
}

/* Stream-correlated local teardown of a namespace-sub request bidi (§10.9.1): an
 * unexpected message (e.g. a REQUEST_UPDATE, which is not modelled here) on an
 * established/pending publisher-side bidi closes that bidi, not the session.
 * Cancel both directions with STOP + RESET, retire the ref via the drain ring so
 * a late in-flight message is discarded rather than mistaken for a fresh request,
 * and free the entry. Reserves all capacity before mutating (retryable). */
static moq_result_t ns_sub_local_teardown(moq_session_t *s, size_t slot)
{
    if (action_queue_avail(s) < 2) return MOQ_ERR_WOULD_BLOCK;
    if (s->drain_ref_count >= s->drain_ref_cap) return MOQ_ERR_WOULD_BLOCK;
    moq_stream_ref_t ref = s->ns_subs[slot].stream_ref;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_STOP_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_stop_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.stop_bidi_stream.stream_ref = ref;
    a.u.stop_bidi_stream.error_code = 0x1;   /* CANCELLED (§3.3.3) */
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_reset_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_bidi_stream.stream_ref = ref;
    a.u.reset_bidi_stream.error_code = 0x1;
    rc = push_action(s, &a);
    if (rc < 0) return rc;
    if (ref._v != 0) (void)drain_ref_add(s, ref);   /* slot reserved above */
    ns_sub_free_entry(s, slot);
    return MOQ_OK;
}

/* Decode the buffered SUBSCRIBE_NAMESPACE on an ns_sub entry in RECVING_PUBLISHER,
 * validate the request id, resolve auth, emit NS_SUB_REQUEST, and commit the
 * entry to PENDING_PUBLISHER. The draft-16 bidi router and the draft-18
 * request-stream handoff both drive this, so the ns_sub state machine has a
 * single implementation. Retryable: a WOULD_BLOCK (full event queue / output
 * scratch) keeps the entry and its staged auth/parse for a re-feed. */
moq_result_t ns_sub_process_recving_publisher(moq_session_t *s,
                                              int32_t ns_slot, bool fin)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[ns_slot];

    /* Only RECVING_PUBLISHER proceeds to parse/emit. */
    if (e->state != MOQ_NS_SUB_RECVING_PUBLISHER)
        return MOQ_OK;

    /* If parse already complete, skip to event push (retry path). */
    if (!e->parse_complete) {
        /* Profile decode: envelope + msg_type + payload + validation. */
        moq_decoded_ns_sub_request_t decoded;
        moq_result_t rc = s->profile->decode_ns_sub_request(
            s, e->recv_buf, e->recv_len, &decoded);
        if (rc == MOQ_ERR_BUFFER) {
            if (fin)
                return close_with_error(s, 0x3,
                    "incomplete SUBSCRIBE_NAMESPACE at stream FIN");
            return MOQ_OK;
        }
        if (rc < 0) {
            /* A profile decode may close with a specific code (e.g. a malformed
             * authorization token closes with KEY_VALUE_FORMATTING_ERROR 0x6);
             * respect that rather than re-closing with PROTOCOL_VIOLATION. */
            if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
            return close_with_error(s, 0x3,
                "malformed SUBSCRIBE_NAMESPACE on bidi stream");
        }

        if (decoded.has_trailing_bytes)
            return close_with_error(s, 0x3,
                "trailing bytes after SUBSCRIBE_NAMESPACE");

        /* Request ID validation via profile. Store on entry for WB retry. A
         * stream-correlated profile binds the request to its bidi during
         * validation; a non-stream profile validates the id and we set the ref. */
        {
            moq_result_t vrc;
            if (moq_session_uses_request_streams(s)) {
                /* msg_type is unused by the stream-correlated validator (it keys
                 * on id parity/sequence); the binding is to the bidi ref. */
                vrc = s->profile->validate_inbound_request_stream(
                    s, e->stream_ref, 0, decoded.request_id, &e->request_ep);
            } else {
                vrc = s->profile->validate_inbound_request(
                    s, decoded.request_id, &e->request_ep);
            }
            if (vrc < 0) return vrc;
            if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
            e->request_ep.kind = MOQ_REQ_NAMESPACE_SUB;
            e->request_ep.slot = ns_slot;
            e->request_ep.has_stream_ref = true;
            e->request_ep.stream_ref = e->stream_ref;
            e->request_id = e->request_ep.request_id;
        }

        /* Auth token processing — staged, committed after event/action. */
        if (!e->auth_processed) {
            uint64_t auth_reject_code = 0;
            moq_result_t arc = process_auth_tokens(s,
                decoded.auth_tokens, decoded.auth_token_count,
                e->resolved_tokens, &e->token_count, MOQ_DECODED_MAX_TOKENS,
                &auth_reject_code, e->token_staged, &e->auth_txn);
            if (arc < 0) return arc;
            if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
            e->auth_processed = true;
            e->auth_reject_code = auth_reject_code;
        }

        if (e->auth_reject_code) {
            moq_result_t erc = ns_sub_send_request_error(s,
                (size_t)ns_slot, e->auth_reject_code);
            return erc;
        }

        e->forward = decoded.forward;
        e->namespace_interest = decoded.namespace_interest;

        if (!ns_sub_store_prefix(s, e, &decoded.prefix))
            return close_with_error(s, 0x1,
                "allocation failure storing NS_SUB prefix");
        if (ns_sub_prefix_conflicts(s, e->prefix_parts,
                e->prefix_count, (int)ns_slot)) {
            moq_result_t erc = ns_sub_send_request_error(s,
                (size_t)ns_slot, MOQ_REQUEST_ERROR_PREFIX_OVERLAP);
            if (erc != MOQ_OK) ns_sub_free_prefix(s, e);
            return erc;
        }

        e->parse_complete = true;
    }

    /* Event push (may be retried after WOULD_BLOCK). */
    if (event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;

    /* Re-decode prefix from recv_buf (idempotent). */
    moq_decoded_ns_sub_request_t decoded;
    {
        moq_result_t drc = s->profile->decode_ns_sub_request(
            s, e->recv_buf, e->recv_len, &decoded);
        if (drc < 0)
            return close_with_error(s, 0x3,
                "SUBSCRIBE_NAMESPACE re-decode failed");
    }

    /* Copy prefix to output scratch. */
    size_t scratch_save = s->event_scratch_len;
    moq_namespace_t prefix_copy = { NULL, 0 };

    if (decoded.prefix.count > 0) {
        size_t prefix_tail = 0;
        bool prefix_tail_ok = true;
        for (size_t i = 0; i < decoded.prefix.count && prefix_tail_ok; i++)
            prefix_tail_ok = tail_len_add(&prefix_tail,
                                          decoded.prefix.parts[i].len);

        /* Alignment-aware: the array is allocated aligned (consuming padding),
         * then the parts bytes are memcpy'd manually -- so the fit check must
         * include that padding, not just array+tail at the unaligned length. A
         * wrapped tail sum is treated as "does not fit". */
        if (!prefix_tail_ok ||
            !event_scratch_fits_aligned(s, decoded.prefix.count,
                                        sizeof(moq_bytes_t), prefix_tail,
                                        _Alignof(moq_bytes_t))) {
            if (s->event_scratch_len == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small for NS_SUB_REQUEST");
            s->event_scratch_len = scratch_save;
            return MOQ_ERR_WOULD_BLOCK;
        }

        moq_bytes_t *dst_parts = (moq_bytes_t *)event_scratch_alloc_aligned(
            s, decoded.prefix.count * sizeof(moq_bytes_t),
            _Alignof(moq_bytes_t));
        if (!dst_parts) {
            s->event_scratch_len = scratch_save;
            return MOQ_ERR_WOULD_BLOCK;
        }

        for (size_t i = 0; i < decoded.prefix.count; i++) {
            size_t blen = decoded.prefix.parts[i].len;
            uint8_t *dst = s->event_scratch + s->event_scratch_len;
            memcpy(dst, decoded.prefix.parts[i].data, blen);
            dst_parts[i].data = dst;
            dst_parts[i].len = blen;
            s->event_scratch_len += blen;
        }
        prefix_copy.parts = dst_parts;
        prefix_copy.count = decoded.prefix.count;
    }

    /* Scratch-copy resolved tokens from entry (staged, commit after push). */
    const moq_resolved_token_t *tokens_ptr = NULL;
    size_t tokens_count = 0;
    if (e->token_count > 0) {
        size_t tok_tail = 0;
        bool tok_tail_ok = true;
        for (size_t i = 0; i < e->token_count && tok_tail_ok; i++)
            tok_tail_ok = tail_len_add(&tok_tail,
                                       e->resolved_tokens[i].token_value.len);

        /* Same alignment-aware fit as the prefix: the token array is aligned
         * (padding), then each token value is memcpy'd manually. A wrapped tail
         * sum is treated as "does not fit". */
        if (!tok_tail_ok ||
            !event_scratch_fits_aligned(s, e->token_count,
                                        sizeof(moq_resolved_token_t), tok_tail,
                                        _Alignof(moq_resolved_token_t))) {
            if (s->event_scratch_len == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small for NS_SUB_REQUEST tokens");
            s->event_scratch_len = scratch_save;
            return MOQ_ERR_WOULD_BLOCK;
        }

        moq_resolved_token_t *dst_toks =
            (moq_resolved_token_t *)event_scratch_alloc_aligned(
                s, e->token_count * sizeof(moq_resolved_token_t),
                _Alignof(moq_resolved_token_t));
        if (!dst_toks) {
            s->event_scratch_len = scratch_save;
            return MOQ_ERR_WOULD_BLOCK;
        }
        for (size_t i = 0; i < e->token_count; i++) {
            dst_toks[i].token_type = e->resolved_tokens[i].token_type;
            size_t vlen = e->resolved_tokens[i].token_value.len;
            if (vlen > 0) {
                uint8_t *vdst = s->event_scratch + s->event_scratch_len;
                memcpy(vdst, e->resolved_tokens[i].token_value.data, vlen);
                dst_toks[i].token_value.data = vdst;
                dst_toks[i].token_value.len  = vlen;
                s->event_scratch_len += vlen;
            } else {
                dst_toks[i].token_value.data = NULL;
                dst_toks[i].token_value.len  = 0;
            }
        }
        tokens_ptr   = dst_toks;
        tokens_count = e->token_count;
    }

    uint32_t live_gen = e->generation | 1;
    moq_ns_sub_handle_t handle;
    {
        uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_NAMESPACE_SUB,
                                           s->session_tag,
                                           live_gen, (uint32_t)ns_slot);
        handle._opaque = packed;
    }

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_NS_SUB_REQUEST;
    ev.detail_size = (uint32_t)sizeof(moq_ns_sub_request_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.ns_sub_request.handle = handle;
    ev.u.ns_sub_request.track_namespace_prefix = prefix_copy;
    ev.u.ns_sub_request.namespace_interest = e->namespace_interest;
    ev.u.ns_sub_request.forward = e->forward;
    ev.u.ns_sub_request.tokens  = tokens_ptr;
    ev.u.ns_sub_request.token_count = tokens_count;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) {
        s->event_scratch_len = scratch_save;
        return rc;
    }

    /* Commit: auth txn, free staging, registry insert, state transition. */
    if (!e->auth_committed) {
        process_auth_tokens_commit_txn(s, &e->auth_txn);
        e->auth_committed = true;
    }
    process_auth_tokens_free_staging(s, e->resolved_tokens,
        e->token_staged, e->token_count);
    e->generation = live_gen;
    e->state = MOQ_NS_SUB_PENDING_PUBLISHER;
    e->handle = handle;
    request_registry_insert_by_id(s, e->request_ep.request_id, e->request_ep);
    s->profile->commit_inbound_request(s, &e->request_ep);

    return MOQ_OK;
}

/* -- Publisher: handle inbound bidi stream bytes ------------------- */

moq_result_t ns_sub_on_new_bidi(moq_session_t *s,
                                        moq_stream_ref_t stream_ref,
                                        const uint8_t *buf, size_t len)
{
    int slot = ns_sub_find_free(s);
    if (slot < 0) return MOQ_ERR_WOULD_BLOCK;
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];

    if (len > e->recv_cap) return MOQ_ERR_BUFFER;
    memcpy(e->recv_buf, buf, len);
    e->recv_len = len;

    e->generation |= 1;
    e->state = MOQ_NS_SUB_RECVING_PUBLISHER;
    e->stream_kind = MOQ_STREAM_KIND_NAMESPACE_SUB;
    e->stream_ref = stream_ref;
    e->parse_complete = false;
    e->prefix_valid = false;   /* no prefix decoded yet; do not let a partial
                                * fragment poison overlap scans (count==0 here
                                * is not a real empty prefix). */
    e->got_response = false;
    e->pending_fin = false;
    e->closing_remote_error = false;
    moq_index_insert(s->idx_ns_by_ref, s->idx_ns_mask,
                     stream_ref._v, slot);
    return MOQ_OK;
}

moq_result_t handle_bidi_stream_bytes(moq_session_t *s,
                                       moq_stream_ref_t stream_ref,
                                       const uint8_t *buf, size_t len,
                                       bool fin)
{
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int32_t ns_slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      stream_ref._v);

    /* Stream-correlated profiles (draft-18) route request bidis through the
     * request stream-ref registry, EXCEPT a bidi already mapped to a namespace-
     * subscription entry, which stays on the ns_sub path below: its outbound
     * response (REQUEST_OK then NAMESPACE / NAMESPACE_DONE), established traffic,
     * and reset/stop/FIN teardown. A bidi not in the ns_sub index goes to the
     * generic request path; a fresh inbound SUBSCRIBE_NAMESPACE is handed off
     * into the ns_sub pool from there. */
    if (ns_slot < 0 && moq_session_uses_request_streams(s))
        return handle_request_stream_bytes(s, stream_ref, buf, len, fin);

    /* Unknown stream + empty non-FIN retry → no-op. */
    if (ns_slot < 0 && len == 0 && !fin) return MOQ_OK;

    /* Unknown stream + FIN without data → incomplete request. */
    if (ns_slot < 0 && len == 0 && fin)
        return close_with_error(s, 0x3,
            "empty FIN on bidi stream without request");

    moq_ns_sub_entry_t *e;

    if (ns_slot < 0) {
        /* A fresh bidi opening a NEW namespace subscription. After our GOAWAY
         * (§10.4) the peer must not open new requests. (Bytes on an already-
         * classified ns_sub bidi take the ns_slot >= 0 branch below and stay
         * legal during draining; stream-correlated profiles route fresh request
         * bidis through handle_request_stream_bytes above, gated there.) */
        if (session_refuses_new_requests(s))
            return close_with_error(s, 0x3,
                "SUBSCRIBE_NAMESPACE after local GOAWAY");

        /* New bidi stream: classify by profile.  D16 only supports
         * SUBSCRIBE_NAMESPACE on bidi streams; future profiles may
         * dispatch other request types here. */
        moq_result_t rc = s->profile->classify_bidi_stream(
            s, stream_ref, buf, len);
        if (rc < 0) return rc;

        ns_slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                  stream_ref._v);
        if (ns_slot < 0) return MOQ_OK;
        e = &s->ns_subs[ns_slot];
    } else {
        e = &s->ns_subs[ns_slot];

        /* A namespace-sub we migrated with an outbound GOAWAY (entry kept live so
         * the app keeps producing, §10.4): the peer's empty-FIN old-stream close
         * silently retires it (no event). On the subscriber side, in-flight
         * NAMESPACE and a duplicate GOAWAY fall through to the response parser (a
         * duplicate GOAWAY closes 0x3 via request_goaway_already_sent); on the
         * publisher side the subscriber only ever closes with a FIN, so any further
         * bytes are a second GOAWAY / stray -> PROTOCOL_VIOLATION. */
        if (e->goaway_sent) {
            if (len == 0) {
                if (fin) ns_sub_free_entry(s, (size_t)ns_slot);
                return MOQ_OK;
            }
            bool subscriber_side =
                e->state == MOQ_NS_SUB_PENDING_SUBSCRIBER ||
                (e->state == MOQ_NS_SUB_ESTABLISHED && e->got_response);
            if (!subscriber_side)
                return close_with_error(s, 0x3,
                    "bytes on a namespace-sub already migrated by GOAWAY");
            /* subscriber-side: fall through to the response parser */
        }

        /* Subscriber-side states: route to response parser. */
        if (e->state == MOQ_NS_SUB_PENDING_SUBSCRIBER ||
            (e->state == MOQ_NS_SUB_ESTABLISHED && e->got_response)) {
            if (len > 0) {
                if (len > e->recv_cap - e->recv_len)
                    return MOQ_ERR_BUFFER;
                memcpy(e->recv_buf + e->recv_len, buf, len);
                e->recv_len += len;
            }
            if (fin) e->pending_fin = true;
            return handle_subscriber_response(s, ns_slot);
        }

        /* CLOSING state. */
        if (e->state == MOQ_NS_SUB_CLOSING) {
            if (e->closing_remote_error && len > 0)
                return close_with_error(s, 0x3,
                    "extra bytes after REQUEST_ERROR on bidi stream");
            if (fin) ns_sub_free_entry(s, (size_t)ns_slot);
            return MOQ_OK;
        }

        /* Publisher post-parse states: extra inbound bytes. For a stream-
         * correlated profile (draft-18) the only defined post-request message is
         * a REQUEST_UPDATE, which is not modelled here; §10.9.1 makes a failed
         * update close the *bidi*, not the session, so terminate just this
         * request bidi and keep the session up. A non-stream profile (draft-16)
         * has no post-request message, so extra bytes are a protocol violation. */
        if (e->state == MOQ_NS_SUB_PENDING_PUBLISHER ||
            e->state == MOQ_NS_SUB_ESTABLISHED) {
            if (len > 0) {
                if (moq_session_uses_request_streams(s))
                    return ns_sub_local_teardown(s, (size_t)ns_slot);
                return close_with_error(s, 0x3,
                    "extra bytes on bidi stream after request");
            }
            /* §2249: the subscriber cancels SUBSCRIBE_NAMESPACE by closing its
             * send half with a FIN or RESET. A stream-correlated profile
             * (draft-18) delivers the FIN inline here; reciprocate by closing our
             * send half (so the peer can retire the bidi -- the subscriber's cancel
             * is a graceful FIN, not a reset), then tear the publisher-side entry
             * down. Reserve the close action first (retryable on re-feed). */
            if (fin && moq_session_uses_request_streams(s)) {
                if (e->stream_ref._v != 0) {
                    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
                    (void)queue_close_bidi(s, e->stream_ref);
                }
                ns_sub_free_entry(s, (size_t)ns_slot);
            }
            return MOQ_OK;
        }

        /* Full-parse pending (RECVING with parse_complete): retry only. */
        if (e->parse_complete) {
            if (len > 0)
                return close_with_error(s, 0x3,
                    "extra bytes on bidi stream with pending request");
            /* Fall through to retry event push. */
        } else {
            /* Append new bytes. */
            if (len > 0) {
                if (len > e->recv_cap - e->recv_len)
                    return MOQ_ERR_BUFFER;
                memcpy(e->recv_buf + e->recv_len, buf, len);
                e->recv_len += len;
            }
        }
    }

    /* Decode, validate, auth, emit NS_SUB_REQUEST, commit -- shared with the
     * draft-18 request-stream handoff so the ns_sub state machine has one
     * implementation. */
    return ns_sub_process_recving_publisher(s, ns_slot, fin);
}

/* -- Publisher: accept namespace subscription ---------------------- */

moq_result_t moq_session_accept_ns_sub(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    const moq_accept_ns_sub_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_accept_ns_sub_cfg_t))
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ns_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    if (e->state != MOQ_NS_SUB_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_request_ok(s, &w, e->request_id);
    if (rc < 0) return rc;
    size_t enc_len = moq_buf_writer_offset(&w);

    /* Stack-encode then queue on the bidi: a temporary send-buffer shortfall is
     * WOULD_BLOCK (retryable), distinct from a never-fits BUFFER. */
    rc = queue_send_bidi(s, e->stream_ref, buf, enc_len, false);
    if (rc < 0) return rc;

    /* Remove from request registry (pending-phase only). */
    request_registry_remove_by_id(s, e->request_id);
    e->state = MOQ_NS_SUB_ESTABLISHED;

    return MOQ_OK;
}

/* -- Publisher: reject namespace subscription ---------------------- */

moq_result_t moq_session_reject_ns_sub(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    const moq_reject_ns_sub_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_reject_ns_sub_cfg_t, can_retry))
        return MOQ_ERR_INVAL;   /* pre-redirect minimum; older callers still work */
    if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;
#define NS_REJ_HAS(f) \
    (cfg->struct_size >= offsetof(moq_reject_ns_sub_cfg_t, f) + sizeof(cfg->f))
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ns_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    if (e->state != MOQ_NS_SUB_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    moq_request_error_encode_args_t err_args = {
        .request_id = e->request_id,
        .error_code = cfg->error_code,
        .reason = cfg->reason.data,
        .reason_len = cfg->reason.len,
    };
    if (NS_REJ_HAS(retry_after_ms)) {
        if (cfg->can_retry && cfg->retry_after_ms >= MOQ_QUIC_VARINT_MAX)
            return MOQ_ERR_INVAL;
        err_args.can_retry = cfg->can_retry;
        err_args.retry_after_ms = cfg->retry_after_ms;
    }
    moq_result_t vrc = reject_apply_redirect(
        s, &err_args, NS_REJ_HAS(redirect) ? &cfg->redirect : NULL,
        true /* namespace-scoped: track name must be empty */);
    if (vrc < 0) return vrc;
#undef NS_REJ_HAS

    /* Stream-correlated profiles (draft-18) free the request bidi here; reserve a
     * drain slot up front so a request the peer sent before seeing our
     * REQUEST_ERROR + FIN is discarded rather than mistaken for a fresh request
     * (the generic request path checks the drain ring first). Draft-16 keeps the
     * ns_sub index mapped until teardown and never consults the ring. */
    bool drain = moq_session_uses_request_streams(s);
    if (drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    moq_result_t rc;
    if (err_args.has_redirect) {
        /* Redirect may exceed a fixed reason buffer: sized scratch + queue. */
        rc = queue_request_error_bidi(s, e->stream_ref, &err_args);
    } else {
        /* Stack-encode then queue on the bidi (fin=true): a temporary send-buffer
         * shortfall is WOULD_BLOCK (retryable), not a hard BUFFER. The buffer holds
         * the protocol-max reason (1024) plus envelope overhead. */
        uint8_t err_buf[1152];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &w, &err_args);
        if (rc < 0) return rc;
        rc = queue_send_bidi(s, e->stream_ref, err_buf,
                             moq_buf_writer_offset(&w), true);
    }
    if (rc < 0) return rc;

    if (drain && e->stream_ref._v != 0)
        (void)drain_ref_add(s, e->stream_ref);   /* slot reserved above */
    ns_sub_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* -- Publisher: send NAMESPACE / NAMESPACE_DONE -------------------- */

static moq_result_t send_ns_msg(moq_session_t *s,
                                 moq_ns_sub_handle_t handle,
                                 const moq_namespace_t *suffix,
                                 bool is_done,
                                 uint64_t now_us)
{
    if (!s || !suffix) return MOQ_ERR_INVAL;
    if (suffix->count > 32) return MOQ_ERR_INVAL;
    if (suffix->count > 0 && !suffix->parts) return MOQ_ERR_INVAL;
    {
        size_t total = 0;
        for (size_t i = 0; i < suffix->count; i++) {
            if (suffix->parts[i].len == 0) return MOQ_ERR_INVAL;
            if (!suffix->parts[i].data) return MOQ_ERR_INVAL;
            if (suffix->parts[i].len > MOQ_FULL_TRACK_NAME_MAX - total)
                return MOQ_ERR_INVAL;
            total += suffix->parts[i].len;
        }
    }
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ns_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    if (e->state != MOQ_NS_SUB_ESTABLISHED || e->got_response)
        return MOQ_ERR_WRONG_STATE;
    /* Migrated by an outbound GOAWAY: no further NAMESPACE/NAMESPACE_DONE. Check
     * before the key allocation + capacity work so the contract is WRONG_STATE. */
    if (e->goaway_sent) return MOQ_ERR_WRONG_STATE;

    /* Build suffix key for tracking. */
    size_t key_len = ns_suffix_key_len(suffix);
    uint8_t *key_buf = (uint8_t *)s->alloc.alloc(key_len, s->alloc.ctx);
    if (!key_buf) return MOQ_ERR_NOMEM;
    ns_suffix_key_build(suffix, key_buf);

    if (is_done) {
        ns_suffix_set_t *set = (ns_suffix_set_t *)e->announced_suffixes;
        if (!set || !ns_suffix_set_contains(set, key_buf, key_len)) {
            s->alloc.free(key_buf, key_len, s->alloc.ctx);
            return MOQ_ERR_WRONG_STATE;
        }
    }

    /* For NAMESPACE (not done): pre-allocate suffix tracking so we
     * don't queue an action then fail to record the suffix. */
    bool inserted = false;
    if (!is_done) {
        if (!e->announced_suffixes) {
            e->announced_suffixes = s->alloc.alloc(
                sizeof(ns_suffix_set_t), s->alloc.ctx);
            if (!e->announced_suffixes) {
                s->alloc.free(key_buf, key_len, s->alloc.ctx);
                return MOQ_ERR_NOMEM;
            }
            memset(e->announced_suffixes, 0, sizeof(ns_suffix_set_t));
        }
        if (!ns_suffix_set_add((ns_suffix_set_t *)e->announced_suffixes,
                                &s->alloc, key_buf, key_len, &inserted)) {
            s->alloc.free(key_buf, key_len, s->alloc.ctx);
            return MOQ_ERR_NOMEM;
        }
    }

    /* Stack-encode then queue on the bidi: a temporary send-buffer shortfall is
     * WOULD_BLOCK (retryable), not a hard BUFFER. The buffer holds a protocol-max
     * namespace suffix plus envelope overhead; the bounded writer prevents
     * overflow regardless. On any failure the speculative suffix insert is rolled
     * back so a re-feed retries cleanly. */
    uint8_t nbuf[MOQ_FULL_TRACK_NAME_MAX + 128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, nbuf, sizeof(nbuf));

    moq_namespace_msg_encode_args_t enc_args = {
        .is_done = is_done,
        .suffix = *suffix,
    };
    moq_result_t rc = s->profile->encode_namespace_msg(s, &w, &enc_args);
    if (rc < 0) {
        if (!is_done && inserted)
            ns_suffix_set_remove((ns_suffix_set_t *)e->announced_suffixes,
                                  &s->alloc, key_buf, key_len);
        s->alloc.free(key_buf, key_len, s->alloc.ctx);
        return rc;
    }
    rc = queue_send_bidi(s, e->stream_ref, nbuf, moq_buf_writer_offset(&w), false);
    if (rc < 0) {
        if (!is_done && inserted)
            ns_suffix_set_remove((ns_suffix_set_t *)e->announced_suffixes,
                                  &s->alloc, key_buf, key_len);
        s->alloc.free(key_buf, key_len, s->alloc.ctx);
        return rc;
    }

    if (is_done) {
        ns_suffix_set_t *set = (ns_suffix_set_t *)e->announced_suffixes;
        ns_suffix_set_remove(set, &s->alloc, key_buf, key_len);
    }

    s->alloc.free(key_buf, key_len, s->alloc.ctx);
    return MOQ_OK;
}

moq_result_t moq_session_send_namespace(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    const moq_namespace_t *suffix,
    uint64_t now_us)
{
    return send_ns_msg(s, handle, suffix, false, now_us);
}

moq_result_t moq_session_send_namespace_done(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    const moq_namespace_t *suffix,
    uint64_t now_us)
{
    return send_ns_msg(s, handle, suffix, true, now_us);
}

/* -- Subscriber: response parsing ---------------------------------- */

static moq_result_t copy_suffix_to_event_scratch(moq_session_t *s,
                                            const moq_namespace_t *suffix,
                                            moq_namespace_t *out,
                                            size_t *scratch_save)
{
    *scratch_save = s->event_scratch_len;
    out->parts = NULL;
    out->count = 0;

    if (suffix->count == 0) return MOQ_OK;

    size_t suffix_tail = 0;
    bool suffix_tail_ok = true;
    for (size_t i = 0; i < suffix->count && suffix_tail_ok; i++)
        suffix_tail_ok = tail_len_add(&suffix_tail, suffix->parts[i].len);

    /* Alignment-aware: the parts array is aligned (padding), then the parts
     * bytes are memcpy'd manually onto the tail -- include the padding. A
     * wrapped tail sum is treated as "does not fit". */
    if (!suffix_tail_ok ||
        !event_scratch_fits_aligned(s, suffix->count, sizeof(moq_bytes_t),
                                    suffix_tail, _Alignof(moq_bytes_t))) {
        if (s->event_scratch_len == 0)
            return close_with_error(s, 0x1,
                "event scratch permanently too small");
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_bytes_t *dst = (moq_bytes_t *)event_scratch_alloc_aligned(
        s, suffix->count * sizeof(moq_bytes_t), _Alignof(moq_bytes_t));
    if (!dst) return MOQ_ERR_WOULD_BLOCK;
    for (size_t i = 0; i < suffix->count; i++) {
        uint8_t *p = s->event_scratch + s->event_scratch_len;
        memcpy(p, suffix->parts[i].data, suffix->parts[i].len);
        dst[i].data = p;
        dst[i].len = suffix->parts[i].len;
        s->event_scratch_len += suffix->parts[i].len;
    }
    out->parts = dst;
    out->count = suffix->count;
    return MOQ_OK;
}

static moq_result_t handle_subscriber_response(moq_session_t *s,
                                                int32_t slot)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];

    while (e->recv_len > 0) {
        moq_decoded_ns_sub_response_t resp;
        moq_result_t rc = s->profile->decode_ns_sub_response(s,
            e->recv_buf, e->recv_len, e->got_response, e->request_id, &resp);
        if (rc == MOQ_ERR_BUFFER) {
            if (e->pending_fin && e->recv_len > 0)
                return close_with_error(s, 0x3,
                    "incomplete message at bidi response stream FIN");
            return MOQ_OK;
        }
        if (rc == MOQ_ERR_PROTO)
            return close_with_error(s, 0x3,
                resp.close_reason ? resp.close_reason : "malformed response");

        switch (resp.kind) {
        case MOQ_NS_RESP_GOAWAY: {
            /* Request-stream GOAWAY (§10.4): migrate the namespace subscription.
             * Terminal on this bidi -- reject trailing bytes -- then the
             * draft-neutral core surfaces MOQ_EVENT_REQUEST_GOAWAY, frees the
             * entry, closes our half, and strict-drains the ref. */
            if (resp.has_trailing_bytes)
                return close_with_error(s, 0x3, "extra bytes after GOAWAY");
            if (s->perspective == MOQ_PERSPECTIVE_SERVER &&
                resp.goaway_uri_len > 0)
                return close_with_error(s, 0x3,
                    "server received GOAWAY with non-zero New Session URI");
            return session_core_on_request_goaway(s, MOQ_REQUEST_FAMILY_NS_SUB,
                slot, e->stream_ref, resp.goaway_uri, resp.goaway_uri_len,
                resp.goaway_timeout_ms);
        }
        case MOQ_NS_RESP_OK: {
            if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

            moq_event_t ev;
            memset(&ev, 0, sizeof(ev));
            ev.kind = MOQ_EVENT_NS_SUB_OK;
            ev.detail_size = (uint32_t)sizeof(moq_ns_sub_ok_event_t);
            ev.borrow_epoch = s->borrow_epoch;
            ev.u.ns_sub_ok.handle = e->handle;
            rc = push_event(s, &ev);
            if (rc < 0) return rc;

            e->got_response = true;
            request_registry_remove_by_id(s, e->request_id);
            e->state = MOQ_NS_SUB_ESTABLISHED;
            break;
        }
        case MOQ_NS_RESP_ERROR: {
            /* Check for trailing bytes after terminal REQUEST_ERROR. */
            if (resp.has_trailing_bytes)
                return close_with_error(s, 0x3,
                    "trailing bytes after REQUEST_ERROR on bidi stream");

            /* REDIRECT (§10.6): namespace-scoped, so Track Name MUST be empty;
             * a server receiving a non-zero Connect URI is a violation. */
            if (resp.has_redirect) {
                if (resp.redirect.track_name_len > 0)
                    return close_with_error(s, 0x3,
                        "REDIRECT Track Name on namespace-scoped request");
                if (s->perspective == MOQ_PERSPECTIVE_SERVER &&
                    resp.redirect.connect_uri_len > 0)
                    return close_with_error(s, 0x3,
                        "server received REDIRECT with non-zero Connect URI");
            }

            if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
            /* Stream-correlated profiles close the subscriber's send half after the
             * terminal error so the publisher can retire the request bidi; reserve
             * that action up front (retryable). */
            bool close_half = moq_session_uses_request_streams(s) &&
                              e->stream_ref._v != 0;
            if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

            size_t scratch_save = s->event_scratch_len;
            if (resp.has_redirect) {
                rc = session_core_emit_request_redirect(s,
                    MOQ_REQUEST_FAMILY_NS_SUB, e->handle._opaque,
                    &resp.redirect, resp.error_code,
                    resp.retry_interval != 0, resp.retry_interval,
                    resp.reason, resp.reason_len);
                if (rc < 0) return rc;
            } else {
                moq_bytes_t reason_copy = { NULL, 0 };
                if (resp.reason_len > 0 && resp.reason) {
                    if (resp.reason_len > s->event_scratch_cap - s->event_scratch_len) {
                        if (s->event_scratch_len == 0)
                            return close_with_error(s, 0x1,
                                "event scratch permanently too small");
                        return MOQ_ERR_WOULD_BLOCK;
                    }
                    uint8_t *dst = s->event_scratch + s->event_scratch_len;
                    memcpy(dst, resp.reason, resp.reason_len);
                    reason_copy.data = dst;
                    reason_copy.len = resp.reason_len;
                    s->event_scratch_len += resp.reason_len;
                }

                moq_event_t ev;
                memset(&ev, 0, sizeof(ev));
                ev.kind = MOQ_EVENT_NS_SUB_ERROR;
                ev.detail_size = (uint32_t)sizeof(moq_ns_sub_error_event_t);
                ev.borrow_epoch = s->borrow_epoch;
                ev.u.ns_sub_error.handle = e->handle;
                ev.u.ns_sub_error.error_code = (moq_request_error_t)resp.error_code;
                ev.u.ns_sub_error.reason = reason_copy;
                rc = push_event(s, &ev);
                if (rc < 0) {
                    s->event_scratch_len = scratch_save;
                    return rc;
                }
            }

            /* Transition to CLOSING. The stream index stays alive to
             * absorb a split FIN. Bump generation to invalidate the
             * public handle immediately. Close our send half (reserved above) so
             * the publisher can retire the request bidi. */
            if (close_half)
                (void)queue_close_bidi(s, e->stream_ref);
            request_registry_remove_by_id(s, e->request_id);
            e->closing_remote_error = true;
            e->state = MOQ_NS_SUB_CLOSING;
            e->recv_len = 0;
            e->generation++;

            if (e->pending_fin) ns_sub_free_entry(s, (size_t)slot);
            return MOQ_OK;
        }
        case MOQ_NS_RESP_NAMESPACE:
        case MOQ_NS_RESP_NAMESPACE_DONE: {
            size_t skey_len = ns_suffix_key_len(&resp.suffix);
            uint8_t *skey_buf = (uint8_t *)s->alloc.alloc(
                skey_len, s->alloc.ctx);
            if (!skey_buf) return MOQ_ERR_WOULD_BLOCK;
            ns_suffix_key_build(&resp.suffix, skey_buf);

            if (resp.kind == MOQ_NS_RESP_NAMESPACE_DONE) {
                ns_suffix_set_t *set =
                    (ns_suffix_set_t *)e->announced_suffixes;
                if (!set || !ns_suffix_set_contains(set, skey_buf,
                        skey_len)) {
                    s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                    return close_with_error(s, 0x3,
                        "NAMESPACE_DONE before NAMESPACE");
                }
            }

            if (event_queue_full(s)) {
                s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                return MOQ_ERR_WOULD_BLOCK;
            }

            size_t scratch_save;
            moq_namespace_t suffix_copy;
            rc = copy_suffix_to_event_scratch(s, &resp.suffix, &suffix_copy, &scratch_save);
            if (rc < 0) {
                s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                return rc;
            }

            /* Record suffix tracking before emitting the event. The inbound,
             * peer-controlled set is charged against the receive budget so a
             * flood of unique NAMESPACE suffixes cannot retain unbounded memory;
             * exceeding the budget closes the session rather than looping on a
             * retryable WOULD_BLOCK. */
            bool inbound_inserted = false;
            if (resp.kind == MOQ_NS_RESP_NAMESPACE) {
                if (!e->announced_suffixes) {
                    e->announced_suffixes = s->alloc.alloc(
                        sizeof(ns_suffix_set_t), s->alloc.ctx);
                    if (!e->announced_suffixes) {
                        s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                        return MOQ_ERR_WOULD_BLOCK;
                    }
                    memset(e->announced_suffixes, 0,
                        sizeof(ns_suffix_set_t));
                    e->announced_suffixes_inbound = true;
                }
                ns_suffix_add_status_t st = ns_suffix_set_add_counted(
                    s, (ns_suffix_set_t *)e->announced_suffixes,
                    skey_buf, skey_len);
                if (st == NS_SUF_NOMEM) {
                    s->event_scratch_len = scratch_save;
                    s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                    return MOQ_ERR_WOULD_BLOCK;
                }
                if (st == NS_SUF_OVER_BUDGET) {
                    s->event_scratch_len = scratch_save;
                    s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                    return close_with_error(s, 0x1,
                        "inbound namespace suffix tracking exceeds "
                        "receive budget");
                }
                inbound_inserted = (st == NS_SUF_INSERTED);
            }

            moq_event_t ev;
            memset(&ev, 0, sizeof(ev));
            ev.borrow_epoch = s->borrow_epoch;
            if (resp.kind == MOQ_NS_RESP_NAMESPACE) {
                ev.kind = MOQ_EVENT_NAMESPACE_FOUND;
                ev.detail_size = (uint32_t)sizeof(moq_namespace_found_event_t);
                ev.u.namespace_found.handle = e->handle;
                ev.u.namespace_found.track_namespace_suffix = suffix_copy;
            } else {
                ev.kind = MOQ_EVENT_NAMESPACE_GONE;
                ev.detail_size = (uint32_t)sizeof(moq_namespace_gone_event_t);
                ev.u.namespace_gone.handle = e->handle;
                ev.u.namespace_gone.track_namespace_suffix = suffix_copy;
            }
            rc = push_event(s, &ev);
            if (rc < 0) {
                s->event_scratch_len = scratch_save;
                if (resp.kind == MOQ_NS_RESP_NAMESPACE && inbound_inserted)
                    ns_suffix_set_remove_counted(s,
                        (ns_suffix_set_t *)e->announced_suffixes,
                        skey_buf, skey_len);
                s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                return rc;
            }
            if (resp.kind == MOQ_NS_RESP_NAMESPACE_DONE) {
                ns_suffix_set_t *set =
                    (ns_suffix_set_t *)e->announced_suffixes;
                if (set)
                    ns_suffix_set_remove_counted(s, set, skey_buf,
                        skey_len);
            }
            s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
            break;
        }
        }

        /* Consume processed bytes. */
        if (resp.consumed < e->recv_len)
            memmove(e->recv_buf, e->recv_buf + resp.consumed,
                    e->recv_len - resp.consumed);
        e->recv_len -= resp.consumed;
    }

    return MOQ_OK;
}

/* -- Subscriber: cancel namespace subscription --------------------- */

moq_result_t moq_session_cancel_namespace_sub(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ns_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    if (e->state != MOQ_NS_SUB_PENDING_SUBSCRIBER &&
        e->state != MOQ_NS_SUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_CLOSE_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_close_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.close_bidi_stream.stream_ref = e->stream_ref;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    if (e->state == MOQ_NS_SUB_PENDING_SUBSCRIBER)
        request_registry_remove_by_id(s, e->request_id);
    e->state = MOQ_NS_SUB_CLOSING;
    e->closing_remote_error = false;
    return MOQ_OK;
}

/* -- Bidi stream reset / stop handlers ----------------------------- *
 * RESET_STREAM and STOP_SENDING are distinct peer signals (kept on separate
 * session inputs), but both terminate the request the bidi carried, so they
 * share the same local cleanup: a namespace-sub entry, or a stream-correlated
 * subscription/fetch via request_stream_teardown. An unknown ref is a no-op
 * (the stream may already be gone). */

static moq_result_t bidi_stream_teardown(moq_session_t *s,
                                         moq_stream_ref_t stream_ref)
{
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;

    /* A bidi we locally cancelled and are draining: the peer's reset/stop is the
     * terminal we were waiting for, so retire the drain ref and we are done. */
    if (drain_ref_remove(s, stream_ref))
        return MOQ_OK;

    /* A request we migrated with an outbound GOAWAY (entry kept live, §10.4): the
     * peer's reset/stop is its old-stream close, so silently retire the request. */
    if (request_goaway_free_on_teardown(s, stream_ref))
        return MOQ_OK;

    int32_t ns_slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      stream_ref._v);
    if (ns_slot >= 0) {
        ns_sub_free_entry(s, (size_t)ns_slot);
        return MOQ_OK;
    }
    return request_stream_teardown(s, stream_ref);
}

moq_result_t handle_bidi_stream_reset(moq_session_t *s,
                                       moq_stream_ref_t stream_ref)
{
    return bidi_stream_teardown(s, stream_ref);
}

moq_result_t handle_bidi_stream_stop(moq_session_t *s,
                                      moq_stream_ref_t stream_ref)
{
    return bidi_stream_teardown(s, stream_ref);
}

moq_result_t moq_session_request_goaway_ns_sub(
    moq_session_t *s, moq_ns_sub_handle_t handle,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_request_goaway_cfg_t)) return MOQ_ERR_INVAL;
    if (cfg->new_session_uri.len > 0 && !cfg->new_session_uri.data)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = ns_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    return session_core_send_request_goaway(s, MOQ_REQUEST_FAMILY_NS_SUB, slot,
        cfg->new_session_uri.data, cfg->new_session_uri.len, cfg->timeout_ms);
}
