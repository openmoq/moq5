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

#if defined(MOQ_SESSION_SWEEP_TESTING)
/* Non-exported membership-work probe (see session_transport.h). Bumped for the
 * inbound and outbound sets alike; tests read deltas around an inbound
 * workload. Compiled only into moq-core-test-internals; absent from shipping. */
uint64_t session_ns_suffix_probe_count;
#endif

/* Total order over canonical suffix keys: lexicographic on the raw bytes, then
 * length. Bumps the membership-work probe once per key comparison (inbound and
 * outbound sets alike) so the complexity oracle counts comparisons, not tree
 * operations. */
static int ns_suffix_key_cmp(const uint8_t *a, size_t alen,
                             const uint8_t *b, size_t blen)
{
#if defined(MOQ_SESSION_SWEEP_TESTING)
    session_ns_suffix_probe_count++;
#endif
    size_t m = alen < blen ? alen : blen;
    int c = m ? memcmp(a, b, m) : 0;
    if (c != 0) return c < 0 ? -1 : 1;
    if (alen != blen) return alen < blen ? -1 : 1;
    return 0;
}

/* -- Announced suffix tracker: private height-balanced (AVL) tree over the
 * canonical suffix key bytes. O(log N) membership/insert/remove replaces the
 * former linear scan, so a peer enumerating N namespaces costs O(N log N), not
 * O(N^2). No process-secret entropy exists in this layer, so an ordered tree
 * (not a keyed hash) is the right structure and keeps the complexity oracle
 * representation-neutral for any balanced tree. ------------------- */

typedef struct ns_suffix_node {
    uint8_t               *data;   /* owned copy of the canonical key bytes */
    size_t                 len;
    struct ns_suffix_node *left;
    struct ns_suffix_node *right;
    int                    height;
} ns_suffix_node_t;

typedef struct {
    ns_suffix_node_t *root;
    size_t            count;
    /* Bytes charged to the session receive budget (s->recv_payload_bytes) for
     * this set: the peer-controlled copied key bytes of every node (recomputed
     * exactly from each removed node's own key length). Only ever nonzero for
     * the inbound, peer-controlled set; the outbound publisher-side set leaves
     * this 0. This is a two-axis bound: the receive-byte budget caps the
     * peer-controlled key bytes, and the per-subscription suffix cap
     * (s->max_ns_suffixes) caps the node COUNT -- so the fixed per-node
     * overhead is bounded by the cap, not the byte budget. */
    size_t            counted_bytes;
} ns_suffix_set_t;

/* a + b with overflow detection; a wrapped sum must refuse the insert. */
static bool ns_size_add(size_t a, size_t b, size_t *out)
{
    if (a > SIZE_MAX - b) return false;
    *out = a + b;
    return true;
}

static int ns_node_height(const ns_suffix_node_t *n)
{
    return n ? n->height : 0;
}

static void ns_node_fix_height(ns_suffix_node_t *n)
{
    int hl = ns_node_height(n->left), hr = ns_node_height(n->right);
    n->height = 1 + (hl > hr ? hl : hr);
}

static int ns_node_balance(const ns_suffix_node_t *n)
{
    return n ? ns_node_height(n->left) - ns_node_height(n->right) : 0;
}

static ns_suffix_node_t *ns_rotate_right(ns_suffix_node_t *y)
{
    ns_suffix_node_t *x = y->left;
    y->left = x->right;
    x->right = y;
    ns_node_fix_height(y);
    ns_node_fix_height(x);
    return x;
}

static ns_suffix_node_t *ns_rotate_left(ns_suffix_node_t *x)
{
    ns_suffix_node_t *y = x->right;
    x->right = y->left;
    y->left = x;
    ns_node_fix_height(x);
    ns_node_fix_height(y);
    return y;
}

static ns_suffix_node_t *ns_rebalance(ns_suffix_node_t *n)
{
    ns_node_fix_height(n);
    int b = ns_node_balance(n);
    if (b > 1) {
        if (ns_node_balance(n->left) < 0) n->left = ns_rotate_left(n->left);
        return ns_rotate_right(n);
    }
    if (b < -1) {
        if (ns_node_balance(n->right) > 0) n->right = ns_rotate_right(n->right);
        return ns_rotate_left(n);
    }
    return n;
}

/* Allocate a node owning a copy of the key, all-or-nothing: either a fully
 * initialized node or NULL with nothing allocated. */
static ns_suffix_node_t *ns_node_create(const moq_alloc_t *alloc,
                                        const uint8_t *key, size_t key_len)
{
    ns_suffix_node_t *n =
        (ns_suffix_node_t *)alloc->alloc(sizeof(*n), alloc->ctx);
    if (!n) return NULL;
    uint8_t *copy = (uint8_t *)alloc->alloc(key_len, alloc->ctx);
    if (!copy) {
        alloc->free(n, sizeof(*n), alloc->ctx);
        return NULL;
    }
    memcpy(copy, key, key_len);
    n->data = copy;
    n->len = key_len;
    n->left = n->right = NULL;
    n->height = 1;
    return n;
}

static void ns_node_destroy(const moq_alloc_t *alloc, ns_suffix_node_t *n)
{
    alloc->free(n->data, n->len, alloc->ctx);
    alloc->free(n, sizeof(*n), alloc->ctx);
}

static void ns_node_free_all(const moq_alloc_t *alloc, ns_suffix_node_t *n)
{
    if (!n) return;
    ns_node_free_all(alloc, n->left);
    ns_node_free_all(alloc, n->right);
    ns_node_destroy(alloc, n);
}

static bool ns_suffix_set_contains(const ns_suffix_set_t *set,
                                    const uint8_t *key, size_t key_len)
{
    const ns_suffix_node_t *n = set->root;
    while (n) {
        int c = ns_suffix_key_cmp(key, key_len, n->data, n->len);
        if (c == 0) return true;
        n = c < 0 ? n->left : n->right;
    }
    return false;
}

/* Link a pre-created node whose key is known absent (caller checked via
 * contains). Rebalancing only reshapes existing nodes, so this never allocates
 * and cannot fail. */
static ns_suffix_node_t *ns_node_insert(ns_suffix_node_t *root,
                                        ns_suffix_node_t *node)
{
    if (!root) return node;
    int c = ns_suffix_key_cmp(node->data, node->len, root->data, root->len);
    if (c < 0) root->left = ns_node_insert(root->left, node);
    else       root->right = ns_node_insert(root->right, node);
    return ns_rebalance(root);
}

static ns_suffix_node_t *ns_node_min(ns_suffix_node_t *n)
{
    while (n && n->left) n = n->left;
    return n;
}

/* Detach the node matching (key,key_len) from the subtree; sets *removed to the
 * detached node (caller frees) and returns the rebalanced subtree root. No
 * allocation. */
static ns_suffix_node_t *ns_node_remove(ns_suffix_node_t *root,
                                        const uint8_t *key, size_t key_len,
                                        ns_suffix_node_t **removed)
{
    if (!root) return NULL;
    int c = ns_suffix_key_cmp(key, key_len, root->data, root->len);
    if (c < 0) {
        root->left = ns_node_remove(root->left, key, key_len, removed);
    } else if (c > 0) {
        root->right = ns_node_remove(root->right, key, key_len, removed);
    } else {
        if (!root->left || !root->right) {
            ns_suffix_node_t *child = root->left ? root->left : root->right;
            *removed = root;      /* detach; caller frees */
            root = child;
        } else {
            /* Two children: swap key ownership with the in-order successor
             * (min of the right subtree), then delete the successor node -- so
             * the detached node carries the original target's key bytes. */
            ns_suffix_node_t *succ = ns_node_min(root->right);
            uint8_t *td = root->data; size_t tl = root->len;
            root->data = succ->data; root->len = succ->len;
            succ->data = td;         succ->len = tl;
            root->right = ns_node_remove(root->right, td, tl, removed);
        }
    }
    if (!root) return NULL;
    return ns_rebalance(root);
}

/* The minimum key node (deterministic cursor for terminal synthesis), or NULL
 * on an empty set. */
static const ns_suffix_node_t *ns_suffix_set_min(const ns_suffix_set_t *set)
{
    return ns_node_min(set->root);
}

static bool ns_suffix_set_add(ns_suffix_set_t *set,
                               const moq_alloc_t *alloc,
                               const uint8_t *key, size_t key_len,
                               bool *out_inserted)
{
    if (out_inserted) *out_inserted = false;
    if (ns_suffix_set_contains(set, key, key_len)) return true;  /* dup: no change */
    ns_suffix_node_t *node = ns_node_create(alloc, key, key_len);
    if (!node) return false;                                     /* all-or-nothing */
    set->root = ns_node_insert(set->root, node);
    set->count++;
    if (out_inserted) *out_inserted = true;
    return true;
}

static bool ns_suffix_set_remove(ns_suffix_set_t *set,
                                  const moq_alloc_t *alloc,
                                  const uint8_t *key, size_t key_len)
{
    ns_suffix_node_t *removed = NULL;
    set->root = ns_node_remove(set->root, key, key_len, &removed);
    if (!removed) return false;
    set->count--;
    ns_node_destroy(alloc, removed);
    return true;
}

static void ns_suffix_set_free(ns_suffix_set_t *set,
                                const moq_alloc_t *alloc)
{
    ns_node_free_all(alloc, set->root);
    set->root = NULL;
    set->count = 0;
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
    /* Allocation failure. The set, its counters and the receive budget are
     * exactly as they were on entry, but nothing the session can do makes
     * memory appear, so this is an error rather than a wait. */
    NS_SUF_NOMEM      = -1,
    NS_SUF_OVER_BUDGET = -2, /* would exceed receive budget (caller must close) */
} ns_suffix_add_status_t;

/* Release n bytes back to the session receive budget (saturating). */
static void ns_recv_budget_release(moq_session_t *s, size_t n)
{
    s->recv_payload_bytes = n <= s->recv_payload_bytes
                                ? s->recv_payload_bytes - n : 0;
}

/* Insert one counted key, all-or-nothing: either the key is present and its
 * copied key bytes are charged to the receive budget (the node count is bounded
 * separately by the per-subscription suffix cap), or the set, its counters and
 * the receive budget are exactly as they were on entry. The charge is weighed
 * against the budget once, BEFORE the single node allocation, so an insert can
 * never allocate and then refuse the key it allocated for. */
static ns_suffix_add_status_t ns_suffix_set_add_counted(
    moq_session_t *s, ns_suffix_set_t *set,
    const uint8_t *key, size_t key_len)
{
    if (ns_suffix_set_contains(set, key, key_len)) return NS_SUF_DUP;

    /* Charge the copied key bytes to the receive budget (the node count is
     * bounded separately by the per-subscription suffix cap). */
    size_t need = key_len, projected;
    if (!ns_size_add(s->recv_payload_bytes, need, &projected) ||
        projected > s->max_recv_buf)
        return NS_SUF_OVER_BUDGET;

    ns_suffix_node_t *node = ns_node_create(&s->alloc, key, key_len);
    if (!node) return NS_SUF_NOMEM;   /* set and counters untouched */

    set->root = ns_node_insert(set->root, node);
    set->count++;
    set->counted_bytes += need;
    s->recv_payload_bytes += need;
    return NS_SUF_INSERTED;
}

/* Remove a counted key, releasing exactly its charged key bytes from the receive
 * budget (recomputed from the removed node's own key length). */
static bool ns_suffix_set_remove_counted(moq_session_t *s, ns_suffix_set_t *set,
                                         const uint8_t *key, size_t key_len)
{
    ns_suffix_node_t *removed = NULL;
    set->root = ns_node_remove(set->root, key, key_len, &removed);
    if (!removed) return false;
    size_t charge = removed->len;      /* same amount charged on insert */
    set->count--;
    set->counted_bytes = charge <= set->counted_bytes
                             ? set->counted_bytes - charge : 0;
    ns_recv_budget_release(s, charge);
    ns_node_destroy(&s->alloc, removed);
    return true;
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

/* `peer_fin_observed` is the caller's cumulative fact for this owner --
 * `ns_sub_fin_observed()`, which takes the dispatcher's current FIN, the durable
 * `pending_fin` latch and the transitional `handoff_fin_pending` marker
 * together, so a FIN that rode the creating SUBSCRIBE_NAMESPACE still counts on
 * an internal retry. */
static moq_result_t ns_sub_send_request_error(moq_session_t *s,
                                                size_t ns_slot,
                                                uint64_t error_code,
                                                bool peer_fin_observed)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[ns_slot];

    /* Stream-correlated profiles (draft-18) free the request bidi here, so
     * reserve a drain slot up front -- but only while the peer's send half is
     * still open: a request it sent before seeing our REQUEST_ERROR + FIN is
     * then discarded rather than mistaken for a fresh request (the generic
     * request path checks the drain ring first), while an already-observed FIN
     * leaves nothing to absorb. Draft-16 keeps the ns_sub index mapped until
     * teardown and never consults the ring, so it does not drain. One decision
     * for the preflight and the insertion below; the REQUEST_ERROR + FIN, the
     * request/auth commit and the entry's retirement are unaffected. */
    bool drain = moq_session_uses_request_streams(s) &&
                 e->stream_ref._v != 0 && !peer_fin_observed;
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
    if (drain)
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
    e->suffix_cap_terminating = false;  /* selective free: the cap-terminal marker
                                         * must never follow the slot to its next
                                         * owner (proves no stale cap state on
                                         * reuse) */
    e->goaway_sent = false;   /* selective free: clear the migration marker so a
                               * reused slot is never seen as already migrated */
    e->handoff_fin_pending = false;   /* same reason: this pool resets named
                                       * fields, so the transitional FIN fact
                                       * must not follow the slot to its next
                                       * owner */
    e->local_teardown_pending = false;   /* selective free: a completed or
                                          * reused slot must not carry a stale
                                          * teardown obligation (#245c) */
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
 * Cancel the request stream with ONE whole-stream abort, retire the ref via the
 * drain ring -- but only while the peer's send half is still open, so a late
 * in-flight message is discarded rather than mistaken for a fresh request; an
 * already-observed FIN leaves nothing to absorb -- and free the entry.
 * `peer_fin_observed` is the caller's cumulative fact for this owner.
 *
 * This helper mutates nothing before its capacity reservations succeed, so a
 * WOULD_BLOCK leaves the session exactly as it was. The obligation to retry is
 * durable in the OWNER, not here: the caller latches `local_teardown_pending`
 * (and, for a same-call FIN, the cumulative `pending_fin`) before this helper's
 * capacity refusal, so the documented empty re-feed re-enters and completes the
 * teardown with no peer bytes re-delivered (#245c). The extra bytes that reached
 * this terminal are deliberately NOT buffered -- the marker is the carrier. */
static moq_result_t ns_sub_local_teardown(moq_session_t *s, size_t slot,
                                          bool peer_fin_observed)
{
    moq_stream_ref_t ref = s->ns_subs[slot].stream_ref;
    /* One decision for the preflight and the insertion. The abort and the
     * entry's retirement are unconditional. */
    bool need_drain = ref._v != 0 && !peer_fin_observed;
    if (action_queue_avail(s) < 1) return MOQ_ERR_WOULD_BLOCK;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_ABORT_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_abort_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.abort_bidi_stream.stream_ref = ref;
    a.u.abort_bidi_stream.error_code = 0x1;   /* CANCELLED (§3.3.3) */
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;
    if (need_drain) (void)drain_ref_add(s, ref);   /* slot reserved above */
    ns_sub_free_entry(s, slot);
    return MOQ_OK;
}

/* Reciprocate the subscriber's graceful cancel: close our own send half so the
 * peer can retire the bidi, then tear the publisher-side entry down. The close
 * needs an action slot, so a refusal leaves the entry AND its transitional FIN
 * ownership intact for an empty re-feed. One body, driven both by a later wire
 * FIN and by a handed-over one. */
static moq_result_t ns_sub_publisher_graceful_close(moq_session_t *s,
                                                    int32_t ns_slot)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[ns_slot];
    if (e->stream_ref._v != 0) {
        if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        (void)queue_close_bidi(s, e->stream_ref);
    }
    ns_sub_free_entry(s, (size_t)ns_slot);
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
                (size_t)ns_slot, e->auth_reject_code,
                ns_sub_fin_observed(e, fin));
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
                (size_t)ns_slot, MOQ_REQUEST_ERROR_PREFIX_OVERLAP,
                ns_sub_fin_observed(e, fin));
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
    e->handoff_fin_pending = false;
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

    int32_t ns_slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      stream_ref._v);

    /* Stream-correlated profiles (draft-18) route request bidis through the
     * request stream-ref registry, EXCEPT a bidi already mapped to a namespace-
     * subscription entry, which stays on the ns_sub path below: its outbound
     * response (REQUEST_OK then NAMESPACE / NAMESPACE_DONE), established traffic,
     * and reset/stop/FIN teardown. A bidi not in the ns_sub index goes to the
     * generic request path; a fresh inbound SUBSCRIBE_NAMESPACE is handed off
     * into the ns_sub pool from there.
     *
     * This routing runs BEFORE the active-session gate: a request bidi may
     * legally arrive before the peer's SETUP has been processed (§3.3 -- QUIC
     * gives no cross-stream ordering), and the request path buffers such early
     * bytes until establishment instead of rejecting them. */
    if (ns_slot < 0 && moq_session_uses_request_streams(s))
        return handle_request_stream_bytes(s, stream_ref, buf, len, fin);

    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

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
            /* Extra bytes -- or a re-feed of a teardown already owed. The
             * teardown closes just this bidi (§10.9.1), so the bytes are NOT
             * buffered; instead the OBLIGATION is made durable before the
             * attempt, and the documented empty re-feed re-drives it with no
             * peer-byte redelivery (#245c). The cumulative FIN is part of that
             * obligation: it drives the teardown's drain selection (#249), so it
             * too is latched in session-owned state before any capacity refusal
             * -- otherwise the empty re-feed (fin=false) would recompute a drain
             * the peer's already-closed send half made unnecessary and stall it
             * against a full ring forever. */
            if (len > 0 || e->local_teardown_pending) {
                if (moq_session_uses_request_streams(s)) {
                    e->local_teardown_pending = true;
                    if (fin) e->pending_fin = true;
                    return ns_sub_local_teardown(s, (size_t)ns_slot,
                                                 ns_sub_fin_observed(e, fin));
                }
                if (len > 0)
                    return close_with_error(s, 0x3,
                        "extra bytes on bidi stream after request");
                return MOQ_OK;
            }
            /* §2249: the subscriber cancels SUBSCRIBE_NAMESPACE by closing its
             * send half with a FIN or RESET. A stream-correlated profile
             * (draft-18) delivers the FIN inline here; reciprocate by closing our
             * send half (so the peer can retire the bidi -- the subscriber's cancel
             * is a graceful FIN, not a reset), then tear the publisher-side entry
             * down. Reserve the close action first (retryable on re-feed). */
            if (ns_sub_fin_observed(e, fin) &&
                moq_session_uses_request_streams(s))
                return ns_sub_publisher_graceful_close(s, ns_slot);
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
    {
        moq_result_t prc = ns_sub_process_recving_publisher(s, ns_slot, fin);
        if (prc < 0) return prc;
        /* The commit that just landed may be the retry of a request whose FIN
         * was handed over at the ownership transition; that close is owed here,
         * on the index-routed path, exactly as on the original dispatch. */
        e = &s->ns_subs[ns_slot];
        if (e->state == MOQ_NS_SUB_PENDING_PUBLISHER &&
            ns_sub_fin_observed(e, fin) &&
            moq_session_uses_request_streams(s))
            return ns_sub_publisher_graceful_close(s, ns_slot);
        return prc;
    }
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
    /* The code must be representable in THIS profile's wire encoding; refuse
     * before any mutation rather than truncate (draft-16 encodes a QUIC
     * varint, draft-18 a vi64 spanning the full 64-bit range). */
    if (cfg->error_code > s->profile->request_error_wire_max)
        return MOQ_ERR_INVAL;
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
     * ns_sub index mapped until teardown and never consults the ring. With the
     * peer's close already observed -- on the wire or handed over by the commit
     * -- the bidi needs no reference, so a full ring cannot refuse this. */
    bool drain = moq_session_uses_request_streams(s) &&
                 !ns_sub_fin_observed(e, false);
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

/* Rebuild the namespace a canonical suffix key encodes: a one-byte part count,
 * then each part as a two-byte big-endian length followed by its bytes. Every
 * length is checked against the bytes that remain and the encoding must be
 * consumed exactly, so a corrupt key fails closed instead of being read past.
 * The parts borrow the key's own storage, which outlives the copy into event
 * scratch that follows. */
static bool ns_suffix_key_decode(const ns_suffix_key_t *key,
                                 moq_bytes_t *parts, size_t parts_cap,
                                 moq_namespace_t *out)
{
    if (!key->data || key->len < 1) return false;
    size_t count = key->data[0];
    if (count > parts_cap) return false;
    size_t off = 1;
    for (size_t i = 0; i < count; i++) {
        if (key->len - off < 2) return false;
        size_t plen = ((size_t)key->data[off] << 8) | (size_t)key->data[off + 1];
        off += 2;
        if (key->len - off < plen) return false;
        parts[i].data = key->data + off;
        parts[i].len = plen;
        off += plen;
    }
    if (off != key->len) return false;
    out->parts = count ? parts : NULL;
    out->count = count;
    return true;
}

/* Terminal synthesis for the inbound SUBSCRIBE_NAMESPACE response stream: every
 * still-active suffix receives a NAMESPACE_GONE, then the owner is retired. Two
 * triggers share this deterministic cursor, with different physical-stream
 * obligations:
 *
 *  - §10.18 FIN/reset: the peer's send half is already gone, so no drain carrier
 *    is owed (`install_drain` false). `close_half` is true only for FIN -- a
 *    reset's ingress owns the physical teardown.
 *  - the per-subscription suffix-cap terminal: WE proactively retire the bidi
 *    while the peer's send half may still be live. Unless the over-cap message
 *    itself carried FIN, the caller passes `install_drain` true so a normal drain
 *    reference on the freed ref absorbs late peer bytes (the generic request path
 *    consults the drain ring first) and a late FIN releases it -- the session
 *    stays ESTABLISHED. If the over-cap message carried FIN, the peer half is
 *    already gone and no carrier is owed.
 *
 * The active suffix set is the durable cursor: each outcome is queued first and
 * the suffix deactivated only then, so an event-capacity refusal resumes exactly
 * where it stopped without repeating or losing one. Both the close action slot
 * and the drain slot are reserved BEFORE the first outcome, so a refusal (full
 * action queue or full drain ring) emits nothing. */
static moq_result_t ns_sub_response_terminal(moq_session_t *s, size_t slot,
                                             bool close_half, bool install_drain)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];

    bool req_streams = moq_session_uses_request_streams(s) &&
                       e->stream_ref._v != 0;
    /* The close and the drain carrier are owed as soon as the terminal
     * completes, so both slots are reserved before the first outcome: the bidi
     * must never close (or leave a live peer half uncarried) while outcomes are
     * still owed, and a refusal here must not have emitted any. */
    bool want_close = close_half && req_streams;
    bool want_drain = install_drain && req_streams;
    if (want_close && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (want_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    /* Only the inbound, peer-announced set is synthesized. The publisher-side
     * set records what we announced to a subscriber and owes nothing here. */
    ns_suffix_set_t *set = e->announced_suffixes_inbound
        ? (ns_suffix_set_t *)e->announced_suffixes : NULL;

    while (set && set->count > 0) {
        if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

        /* Deterministic cursor: the minimum key. Each round queues its GONE and
         * removes exactly that key, so an event-capacity refusal resumes at the
         * next minimum without repeating or losing one. */
        const ns_suffix_node_t *m = ns_suffix_set_min(set);
        ns_suffix_key_t k0 = { m->data, m->len };

        moq_bytes_t parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
        moq_namespace_t suffix;
        if (!ns_suffix_key_decode(&k0, parts,
                                  MOQ_DECODED_MAX_NAMESPACE_PARTS, &suffix))
            return close_with_error(s, 0x1, "corrupt namespace suffix key");

        size_t scratch_save;
        moq_namespace_t suffix_copy;
        moq_result_t rc = copy_suffix_to_event_scratch(s, &suffix, &suffix_copy,
                                                       &scratch_save);
        if (rc < 0) return rc;
        /* A permanently undersized arena closes the session and reports it as
         * MOQ_OK, so stop rather than pushing onto a closed session. */
        if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;

        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_NAMESPACE_GONE;
        ev.detail_size = (uint32_t)sizeof(moq_namespace_gone_event_t);
        ev.borrow_epoch = s->borrow_epoch;
        ev.u.namespace_gone.handle = e->handle;
        ev.u.namespace_gone.track_namespace_suffix = suffix_copy;
        rc = push_event(s, &ev);
        if (rc < 0) {
            s->event_scratch_len = scratch_save;
            return rc;
        }
        /* Remove exactly the key just surfaced; the next round recomputes the
         * minimum of the live tree. m stays valid until this call frees it. */
        ns_suffix_set_remove_counted(s, set, m->data, m->len);
    }

    if (want_close) (void)queue_close_bidi(s, e->stream_ref);
    if (want_drain) (void)drain_ref_add(s, e->stream_ref);   /* slot reserved above */
    ns_sub_free_entry(s, slot);
    return MOQ_OK;
}

/* Recovery contract for the results this returns.
 *
 * MOQ_ERR_WOULD_BLOCK is the retryable one the public API documents: the
 * subscription and its buffered response bytes are both retained, and an
 * ordinary empty re-feed of the same bidi resumes where this left off.
 *
 * MOQ_ERR_NOMEM is a hard error, not a wait -- no drain makes memory appear.
 * The subscription and its buffered bytes are nonetheless left exactly as they
 * were, so a caller driving the session directly may recover by the same empty
 * re-feed once memory is available. That is a property of this path, not a
 * general promise: the request-stream families free a still-receiving staging
 * slot on any hard failure (session_subscribe.c), so their bytes are gone and
 * only re-delivery can recover. A caller driving the session through the
 * transport bridge gets neither: the bridge escalates every negative other than
 * MOQ_ERR_WOULD_BLOCK to a connection fatal, deliberately, because a session
 * that cannot allocate has no unblock edge to wait for. */
static moq_result_t handle_subscriber_response(moq_session_t *s,
                                                int32_t slot)
{
    moq_ns_sub_entry_t *e = &s->ns_subs[slot];

    /* A per-subscription suffix-cap terminal in progress owns this entry: resume
     * synthesizing NAMESPACE_GONE from the tree cursor rather than re-decoding
     * the buffered over-cap NAMESPACE (whose bytes are abandoned with the
     * entry). Set before the first GONE, so an event/action-capacity block that
     * returned WOULD_BLOCK resumes here on the empty re-feed. */
    if (e->suffix_cap_terminating)
        return ns_sub_response_terminal(s, (size_t)slot, true,
                                        !e->pending_fin);

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
             * entry, closes our half, and strict-drains the ref only when the
             * peer's FIN has not already been observed. */
            if (resp.has_trailing_bytes)
                return close_with_error(s, 0x3, "extra bytes after GOAWAY");
            if (s->perspective == MOQ_PERSPECTIVE_SERVER &&
                resp.goaway_uri_len > 0)
                return close_with_error(s, 0x3,
                    "server received GOAWAY with non-zero New Session URI");
            /* This route's durable FIN fact is `pending_fin`, latched by the
             * dispatcher before the response parser runs and retained across a
             * refusal -- so a GOAWAY that rode the FIN still knows it on the
             * empty re-feed that completes it. */
            return session_core_on_request_goaway(s, MOQ_REQUEST_FAMILY_NS_SUB,
                slot, e->stream_ref, resp.goaway_uri, resp.goaway_uri_len,
                resp.goaway_timeout_ms, e->pending_fin);
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
                /* The emitter reports a terminal close as MOQ_OK, so the
                 * teardown below runs only while the session is still open. */
                if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
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
                ev.u.ns_sub_error.error_code =
                    s->profile->semantic_request_error(resp.error_code);
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
            /* No suffix scratch or tracking state has been touched yet, and no
             * drain frees memory, so this is an error rather than a wait. See
             * the recovery contract above the response handler. */
            if (!skey_buf) return MOQ_ERR_NOMEM;
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

            /* Per-subscription active-suffix cap (in addition to the receive
             * budget). A NEW unique NAMESPACE that would exceed the cap is not
             * admitted and surfaces no FOUND; instead the offending bidi is
             * retired by synthesizing NAMESPACE_GONE for the active suffixes,
             * with the session staying ESTABLISHED. A duplicate NAMESPACE (or a
             * NAMESPACE_DONE) never grows the set, so it is unaffected. */
            if (resp.kind == MOQ_NS_RESP_NAMESPACE) {
                ns_suffix_set_t *set =
                    (ns_suffix_set_t *)e->announced_suffixes;
                if (set && set->count >= s->max_ns_suffixes &&
                    !ns_suffix_set_contains(set, skey_buf, skey_len)) {
                    s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                    e->suffix_cap_terminating = true;
                    /* No drain if the over-cap message itself carried FIN (peer
                     * send half already gone); otherwise a drain carrier absorbs
                     * the still-live peer half. */
                    return ns_sub_response_terminal(s, (size_t)slot, true,
                                                    !e->pending_fin);
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
                /* The first tracked suffix needs a set. Build it privately and
                 * publish it onto the entry only once the insert has succeeded,
                 * so a failed insert cannot leave an empty tracker behind that a
                 * later NAMESPACE_DONE would treat as an established set. */
                ns_suffix_set_t *set = (ns_suffix_set_t *)e->announced_suffixes;
                ns_suffix_set_t *fresh = NULL;
                if (!set) {
                    fresh = (ns_suffix_set_t *)s->alloc.alloc(
                        sizeof(ns_suffix_set_t), s->alloc.ctx);
                    if (!fresh) {
                        s->event_scratch_len = scratch_save;
                        s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                        return MOQ_ERR_NOMEM;
                    }
                    memset(fresh, 0, sizeof(*fresh));
                    set = fresh;
                }
                ns_suffix_add_status_t st = ns_suffix_set_add_counted(
                    s, set, skey_buf, skey_len);
                if (st < 0) {
                    if (fresh) {
                        ns_suffix_set_free(fresh, &s->alloc);
                        s->alloc.free(fresh, sizeof(*fresh), s->alloc.ctx);
                    }
                    s->event_scratch_len = scratch_save;
                    s->alloc.free(skey_buf, skey_len, s->alloc.ctx);
                    if (st == NS_SUF_OVER_BUDGET)
                        return close_with_error(s, 0x1,
                            "inbound namespace suffix tracking exceeds "
                            "receive budget");
                    return MOQ_ERR_NOMEM;
                }
                if (fresh) {
                    e->announced_suffixes = fresh;
                    e->announced_suffixes_inbound = true;
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

    /* The buffer is drained. A FIN latched by the dispatcher terminates the
     * subscription (§10.18); the latch survives every refusal, so an ordinary
     * empty re-feed resumes the terminal. */
    if (e->pending_fin)
        return ns_sub_response_terminal(s, (size_t)slot, true, false);
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
                                         moq_stream_ref_t stream_ref,
                                         bool reset)
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
        /* §10.18 names the reset, not STOP_SENDING: a reset of the response
         * stream is a NAMESPACE_DONE for every active namespace, and the reset
         * ingress owns the physical teardown, so no local close is queued. A
         * refusal here is retried through the same reset input. STOP_SENDING
         * keeps retiring the owner outright. */
        if (reset)
            return ns_sub_response_terminal(s, (size_t)ns_slot, false, false);
        ns_sub_free_entry(s, (size_t)ns_slot);
        return MOQ_OK;
    }
    /* A no-owner no-slot admission carrier (#245b): the request had no request
     * registry owner, only a bounded carrier holding its retry identity. The
     * peer's RESET/STOP terminates that request (§3.3.2, §11.4.1), so retire the
     * exact carrier and send no now-obsolete REQUEST_ERROR/STOP+RESET terminal.
     * Only this ref's carrier is touched; an unrelated carrier is left intact. */
    if (noslot_carrier_find(s, stream_ref) >= 0) {
        noslot_carrier_remove(s, stream_ref);
        return MOQ_OK;
    }
    return request_stream_teardown(s, stream_ref);
}

moq_result_t handle_bidi_stream_reset(moq_session_t *s,
                                       moq_stream_ref_t stream_ref)
{
    return bidi_stream_teardown(s, stream_ref, true);
}

moq_result_t handle_bidi_stream_stop(moq_session_t *s,
                                      moq_stream_ref_t stream_ref)
{
    return bidi_stream_teardown(s, stream_ref, false);
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
