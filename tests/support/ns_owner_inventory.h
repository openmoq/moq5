#ifndef MOQ_TEST_NS_OWNER_INVENTORY_H
#define MOQ_TEST_NS_OWNER_INVENTORY_H
/*
 * Test-only namespace-subscription owner inventory and outcome oracles,
 * shared by the direct-session and transport-bridge #250 suites so both hold
 * ONE field contract. Header-only and static, like tests/support/
 * ownership_graph.h: nothing here is exported and no product seam is added.
 *
 * Include AFTER moq/moq.h, moq/control_d18.h, test_support.h,
 * ../support/ownership_graph.h, ../support/txn_snapshot.h and the private
 * core/src/session/session_internal.h.
 */

#if defined(__GNUC__) || defined(__clang__)
#define NF_UNUSED __attribute__((unused))
#else
#define NF_UNUSED
#endif

#define NF_EV_MAX    8
#define NF_PART_MAX  24
/* At least the configured drain-ring capacity, so a live row never falls back
 * to a weaker comparison. Overflow stays reachable only as a quiet self-check. */
#define NF_DRAIN_MAX 512
#define NF_INV_BYTES 512

/* The receive-budget charge of one tracked suffix is its canonical key
 * length: one count byte, then a two-byte length plus the bytes of each part
 * (session_namespace_sub.c:27-33). The membership structure is a private
 * ordered tree; its per-node overhead is bounded by the per-subscription
 * suffix cap, not the byte budget, so the budget delta per suffix is exactly
 * this key length -- calculable without reading the private set. */
NF_UNUSED static size_t nf_suffix_charge(const char *field2)
{
    return 1u + (2u + 4u) + (2u + strlen(field2));   /* "room" + field2 */
}

/* -- wire helpers ----------------------------------------------------- */

/* Returns 0 on any encode failure rather than an offset from a failed write,
 * so a truncated fixture message can never be fed as if it were whole. */
NF_UNUSED static size_t nf_encode_ns(uint8_t *buf, size_t cap, uint64_t type,
                           const char *a, const char *b)
{
    uint8_t payload[128];
    moq_buf_writer_t pw;
    moq_buf_writer_init(&pw, payload, sizeof(payload));
    moq_bytes_t parts[2] = {
        { (const uint8_t *)a, strlen(a) },
        { (const uint8_t *)b, strlen(b) },
    };
    moq_namespace_t ns = { parts, 2 };
    if (moq_buf_write_namespace_prefix(&pw, &ns) != MOQ_OK) {
        fprintf(stderr, "NF: namespace prefix encode failed\n");
        return 0;
    }
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    if (moq_control_encode_envelope(&w, type, payload,
                                    (uint16_t)moq_buf_writer_offset(&pw))
        != MOQ_OK) {
        fprintf(stderr, "NF: envelope encode failed\n");
        return 0;
    }
    return moq_buf_writer_offset(&w);
}

/* Set while a self-check drives an expected mismatch, so the negative cases
 * prove the comparator without printing diagnostics on a passing run. */
NF_UNUSED static int nf_quiet;

#define NF_DIAG(...) do { if (!nf_quiet) fprintf(stderr, __VA_ARGS__); } while (0)

/* -- surfaced event images -------------------------------------------- */

typedef struct nf_ev {
    int    kind;
    int    handle_match;      /* -1: this kind carries no handle */
    size_t count;
    size_t len[2];
    char   part[2][NF_PART_MAX];
} nf_ev_t;

NF_UNUSED static int nf_ev_record(const moq_event_t *ev, uint64_t h_opaque, nf_ev_t *out)
{
    memset(out, 0, sizeof(*out));
    out->kind = (int)ev->kind;
    out->handle_match = -1;
    const moq_namespace_t *ns = NULL;
    switch (ev->kind) {
    case MOQ_EVENT_NAMESPACE_FOUND:
        ns = &ev->u.namespace_found.track_namespace_suffix;
        out->handle_match = ev->u.namespace_found.handle._opaque == h_opaque;
        break;
    case MOQ_EVENT_NAMESPACE_GONE:
        ns = &ev->u.namespace_gone.track_namespace_suffix;
        out->handle_match = ev->u.namespace_gone.handle._opaque == h_opaque;
        break;
    case MOQ_EVENT_NS_SUB_OK:
        out->handle_match = ev->u.ns_sub_ok.handle._opaque == h_opaque;
        return 0;
    default:
        return 0;
    }
    if (ns->count > 2) {
        fprintf(stderr, "NF: suffix part count %zu exceeds the declared 2\n",
                ns->count);
        return 1;
    }
    if (ns->count > 0 && !ns->parts) {
        fprintf(stderr, "NF: non-empty suffix with NULL parts array\n");
        return 1;
    }
    out->count = ns->count;
    for (size_t i = 0; i < ns->count; i++) {
        if (ns->parts[i].len >= NF_PART_MAX) {
            fprintf(stderr, "NF: suffix part %zu length %zu exceeds %d\n", i,
                    ns->parts[i].len, NF_PART_MAX);
            return 1;
        }
        if (ns->parts[i].len > 0 && !ns->parts[i].data) {
            fprintf(stderr, "NF: non-empty suffix part %zu has NULL data\n", i);
            return 1;
        }
        out->len[i] = ns->parts[i].len;
        if (out->len[i]) memcpy(out->part[i], ns->parts[i].data, out->len[i]);
    }
    return 0;
}

NF_UNUSED static nf_ev_t nf_ev_want(int kind, const char *a, const char *b)
{
    nf_ev_t w;
    memset(&w, 0, sizeof(w));
    w.kind = kind;
    w.handle_match = 1;
    if (!a) return w;
    w.count = 2;
    w.len[0] = strlen(a);
    w.len[1] = strlen(b);
    memcpy(w.part[0], a, w.len[0]);
    memcpy(w.part[1], b, w.len[1]);
    return w;
}

/* Silent predicate: used while probing multiset candidates, where a
 * non-match is an ordinary outcome and must not print. */
NF_UNUSED static int nf_ev_same(const nf_ev_t *a, const nf_ev_t *b)
{
    if (a->kind != b->kind || a->handle_match != b->handle_match ||
        a->count != b->count)
        return 0;
    for (size_t i = 0; i < a->count; i++) {
        if (a->len[i] != b->len[i]) return 0;
        if (memcmp(a->part[i], b->part[i], a->len[i]) != 0) return 0;
    }
    return 1;
}

NF_UNUSED static int nf_ev_equals(const nf_ev_t *got, const nf_ev_t *want,
                        const char *what)
{
    if (nf_ev_same(got, want)) return 0;
    NF_DIAG("NF %s: event kind %d handle %d suffix", what, got->kind,
            got->handle_match);
    for (size_t i = 0; i < got->count; i++)
        NF_DIAG(" '%.*s'", (int)got->len[i], got->part[i]);
    NF_DIAG(", expected kind %d handle %d suffix", want->kind,
            want->handle_match);
    for (size_t i = 0; i < want->count; i++)
        NF_DIAG(" '%.*s'", (int)want->len[i], want->part[i]);
    NF_DIAG("\n");
    return 1;
}

/* Exactly this set, in any order: the terminal's iteration order over the
 * active set is not a contract. Diagnostics are emitted only for a declared
 * expectation that nothing matches -- probing is silent. */
NF_UNUSED static int nf_multiset(const nf_ev_t *got, size_t got_n, const nf_ev_t *want,
                       size_t want_n, const char *what)
{
    if (got_n != want_n) {
        NF_DIAG("NF %s: %zu events, expected %zu\n", what, got_n, want_n);
        return 1;
    }
    int used[NF_EV_MAX];
    memset(used, 0, sizeof(used));
    int bad = 0;
    for (size_t i = 0; i < want_n; i++) {
        size_t j;
        for (j = 0; j < got_n; j++) {
            if (!used[j] && nf_ev_same(&got[j], &want[i])) break;
        }
        if (j == got_n) {
            NF_DIAG("NF %s: no event matches declared", what);
            for (size_t k = 0; k < want[i].count; k++)
                NF_DIAG(" '%.*s'", (int)want[i].len[k], want[i].part[k]);
            NF_DIAG(" (kind %d)\n", want[i].kind);
            bad++;
        } else {
            used[j] = 1;
        }
    }
    return bad;
}

NF_UNUSED static int nf_collect(moq_session_t *c, uint64_t h, nf_ev_t *out, size_t cap,
                      size_t *n, const char *what)
{
    int bad = 0;
    *n = 0;
    moq_event_t ev;
    while (moq_session_poll_events(c, &ev, 1) > 0) {
        if (*n >= cap) {
            fprintf(stderr, "NF %s: more than %zu events surfaced\n", what,
                    cap);
            bad++;
        } else {
            bad += nf_ev_record(&ev, h, &out[(*n)++]);
        }
        moq_event_cleanup(&ev);
    }
    return bad;
}

/* -- action classification -------------------------------------------- */

typedef struct nf_acts {
    size_t closes_on_ref;
    size_t other;
} nf_acts_t;

NF_UNUSED static void nf_take_actions(moq_session_t *c, uint64_t ref, nf_acts_t *acc)
{
    moq_action_t a;
    while (moq_session_poll_actions(c, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_CLOSE_BIDI_STREAM &&
            a.u.close_bidi_stream.stream_ref._v == ref)
            acc->closes_on_ref++;
        else
            acc->other++;
        moq_action_cleanup(&a);
    }
}

NF_UNUSED static int nf_acts_equals(const nf_acts_t *got, size_t closes,
                          const char *what)
{
    int bad = 0;
    if (got->closes_on_ref != closes) {
        NF_DIAG("NF %s: %zu close-bidi actions on the response stream,"
                " expected %zu\n", what, got->closes_on_ref, closes);
        bad++;
    }
    if (got->other != 0) {
        NF_DIAG("NF %s: %zu unexpected actions\n", what, got->other);
        bad++;
    }
    return bad;
}

/* -- drain multiset ---------------------------------------------------- */

typedef struct nf_drain {
    int      overflow;
    size_t   count;
    uint64_t ref[NF_DRAIN_MAX];
    uint8_t  reason[NF_DRAIN_MAX];
} nf_drain_t;

NF_UNUSED static int nf_drain_snap(const moq_session_t *s, nf_drain_t *d)
{
    memset(d, 0, sizeof(*d));
    if (s->drain_ref_count > NF_DRAIN_MAX) {
        d->overflow = 1;
        NF_DIAG("NF: %zu drain refs exceed the snapshot bound %d\n",
                s->drain_ref_count, NF_DRAIN_MAX);
        return 1;
    }
    d->count = s->drain_ref_count;
    for (size_t i = 0; i < d->count; i++) {
        d->ref[i] = s->drain_refs[i];
        d->reason[i] = s->drain_ref_reasons[i];
    }
    return 0;
}

NF_UNUSED static int nf_drain_equals(const nf_drain_t *got,
                                     const nf_drain_t *want, const char *what)
{
    if (got->overflow || want->overflow) return 1;
    if (got->count != want->count) {
        NF_DIAG("NF %s: %zu drain refs, expected %zu\n", what, got->count,
                want->count);
        return 1;
    }
    int used[NF_DRAIN_MAX];
    memset(used, 0, sizeof(used));
    int bad = 0;
    for (size_t i = 0; i < want->count; i++) {
        size_t j;
        for (j = 0; j < got->count; j++) {
            if (!used[j] && got->ref[j] == want->ref[i] &&
                got->reason[j] == want->reason[i]) break;
        }
        if (j == got->count) {
            NF_DIAG("NF %s: missing drain (ref %llu, reason %u)\n", what,
                    (unsigned long long)want->ref[i],
                    (unsigned)want->reason[i]);
            bad++;
        } else {
            used[j] = 1;
        }
    }
    /* Name the UNMATCHED ACTUAL entries too: a substitution has both a
     * missing expected pair and an extra observed one, and reporting only
     * half of that leaves the reader guessing what replaced it. */
    for (size_t j = 0; j < got->count; j++) {
        if (used[j]) continue;
        NF_DIAG("NF %s: extra drain (ref %llu, reason %u)\n", what,
                (unsigned long long)got->ref[j], (unsigned)got->reason[j]);
        bad++;
    }
    return bad;
}

NF_UNUSED static int nf_head_action_is_close(const moq_session_t *s, uint64_t ref)
{
    if (s->action_head == s->action_tail) return 0;
    const moq_action_t *a = &s->actions[s->action_head % s->action_cap];
    return a->kind == MOQ_ACTION_CLOSE_BIDI_STREAM &&
           a->u.close_bidi_stream.stream_ref._v == ref;
}

/* -- complete owner inventory ------------------------------------------
 *
 * Every field of moq_ns_sub_entry_t, by name, plus a deep copy of every byte
 * span whose contents are part of the contract. Spans are bounds-checked and
 * non-NULL-guarded before they are read; a violation makes the record
 * INCOMPARABLE, which the comparator fails rather than silently accepting.
 *
 * Explicitly NOT covered: heap storage reachable only through the private
 * auth-overlay (`auth_txn.ov.entries[i]` preowned values). The overlay's own
 * bytes ARE covered by an opaque same-run image, so any change to a pointer,
 * count or cache binding is caught; what a surviving pointer points AT is not.
 */
typedef struct nf_inv {
    int valid;
    /* 0 while the tracker's ADDRESS is unpredictable -- the creating
     * transition can only be expressed semantically ("one non-NULL inbound
     * tracker now exists"), never by copying the observed pointer into its
     * own expectation. Every later comparison uses the validated identity. */
    int cmp_suffix_ptr;

    int      state, stream_kind;
    uint32_t generation;
    uint64_t handle, request_id, stream_ref;

    int      ep_kind, ep_slot, ep_has_rid, ep_has_ref;
    uint64_t ep_rid, ep_ref;

    int      interest;
    const void *prefix_buf, *prefix_parts;
    size_t   prefix_buf_len, prefix_count;
    int      prefix_valid;

    int      got_response, parse_complete, pending_fin, handoff_fin_pending,
             local_teardown_pending,
             closing_remote_error, forward, auth_processed, auth_committed;
    uint64_t auth_reject_code;

    size_t   token_count;
    const void *recv_buf;
    size_t   recv_len, recv_cap;
    const void *suffixes;
    int      suffixes_inbound, goaway_sent;

    uint8_t  auth_txn_img[sizeof(moq_auth_txn_t)];
    uint8_t  tokens_img[sizeof(moq_resolved_token_t) * MOQ_DECODED_MAX_TOKENS];
    uint8_t  staged_img[MOQ_DECODED_MAX_TOKENS];

    size_t   pfx_bytes_len;  uint8_t pfx_bytes[NF_INV_BYTES];
    size_t   parts_bytes_len; uint8_t parts_bytes[NF_INV_BYTES];
    /* Per-part boundaries and identities: concatenated bytes alone cannot
     * distinguish {"ab","c"} from {"a","bc"}. */
    size_t      part_len[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    const void *part_ptr[MOQ_DECODED_MAX_NAMESPACE_PARTS];
    size_t   recv_bytes_len; uint8_t recv_bytes[NF_INV_BYTES];
    size_t   tok_bytes_len;  uint8_t tok_bytes[NF_INV_BYTES];
} nf_inv_t;

NF_UNUSED static int nf_inv_copy(uint8_t *dst, size_t cap, size_t *dst_len,
                       const uint8_t *src, size_t len, const char *field)
{
    if (len == 0) return 1;
    if (!src) {
        NF_DIAG("NF inventory: %s has length %zu with a NULL pointer\n",
                field, len);
        return 0;
    }
    /* Subtraction only: `*dst_len + len` can wrap on a corrupt length. */
    if (*dst_len > cap || len > cap - *dst_len) {
        NF_DIAG("NF inventory: %s exceeds the %zu-byte bound\n", field, cap);
        return 0;
    }
    memcpy(dst + *dst_len, src, len);
    *dst_len += len;
    return 1;
}

NF_UNUSED static void nf_inv_read(const moq_session_t *s, int slot, nf_inv_t *o)
{
    memset(o, 0, sizeof(*o));
    o->cmp_suffix_ptr = 1;
    /* Validate the slot BEFORE any dereference: an out-of-range record is
     * incomparable, never a read past the pool. */
    if (slot < 0 || (size_t)slot >= s->ns_sub_cap) {
        NF_DIAG("NF inventory: slot %d is outside the pool of %zu\n", slot,
                s->ns_sub_cap);
        o->valid = 0;
        return;
    }
    const moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    o->valid = 1;

    o->state       = (int)e->state;
    o->stream_kind = (int)e->stream_kind;
    o->generation  = e->generation;
    o->handle      = e->handle._opaque;
    o->request_id  = e->request_id;
    o->stream_ref  = e->stream_ref._v;

    o->ep_kind    = (int)e->request_ep.kind;
    o->ep_slot    = e->request_ep.slot;
    o->ep_has_rid = e->request_ep.has_request_id;
    o->ep_rid     = e->request_ep.request_id;
    o->ep_has_ref = e->request_ep.has_stream_ref;
    o->ep_ref     = e->request_ep.stream_ref._v;

    o->interest       = (int)e->namespace_interest;
    o->prefix_buf     = e->prefix_buf;
    o->prefix_parts   = e->prefix_parts;
    o->prefix_buf_len = e->prefix_buf_len;
    o->prefix_count   = e->prefix_count;
    o->prefix_valid   = e->prefix_valid;

    o->got_response         = e->got_response;
    o->parse_complete       = e->parse_complete;
    o->pending_fin          = e->pending_fin;
    o->handoff_fin_pending  = e->handoff_fin_pending;
    o->local_teardown_pending = e->local_teardown_pending;
    o->closing_remote_error = e->closing_remote_error;
    o->forward              = e->forward;
    o->auth_processed       = e->auth_processed;
    o->auth_committed       = e->auth_committed;
    o->auth_reject_code     = e->auth_reject_code;

    o->token_count      = e->token_count;
    o->recv_buf         = e->recv_buf;
    o->recv_len         = e->recv_len;
    o->recv_cap         = e->recv_cap;
    o->suffixes         = e->announced_suffixes;
    o->suffixes_inbound = e->announced_suffixes_inbound;
    o->goaway_sent      = e->goaway_sent;

    memcpy(o->auth_txn_img, &e->auth_txn, sizeof(o->auth_txn_img));
    memcpy(o->tokens_img, e->resolved_tokens, sizeof(o->tokens_img));
    memcpy(o->staged_img, e->token_staged, sizeof(o->staged_img));

    if (!nf_inv_copy(o->pfx_bytes, NF_INV_BYTES, &o->pfx_bytes_len,
                     e->prefix_buf, e->prefix_buf_len, "prefix_buf"))
        o->valid = 0;
    if (e->prefix_count > MOQ_DECODED_MAX_NAMESPACE_PARTS) {
        NF_DIAG("NF inventory: prefix_count %zu exceeds %d\n", e->prefix_count,
                MOQ_DECODED_MAX_NAMESPACE_PARTS);
        o->valid = 0;
    } else if (e->prefix_count > 0 && !e->prefix_parts) {
        NF_DIAG("NF inventory: prefix_count %zu with NULL parts\n",
                e->prefix_count);
        o->valid = 0;
    } else {
        /* A part must lie wholly inside the known prefix_buf allocation
         * before its bytes are touched. Range arithmetic on uintptr_t, not a
         * relational comparison of unrelated pointers, and the span is
         * checked BEFORE the copy so a corrupted pointer is incomparable
         * rather than an out-of-bounds read. */
        const uintptr_t base = (uintptr_t)e->prefix_buf;
        uintptr_t end = base;
        int span_ok = 1;
        if ((uintptr_t)e->prefix_buf_len > UINTPTR_MAX - base) {
            NF_DIAG("NF inventory: prefix_buf span wraps the address space\n");
            o->valid = 0;
            span_ok = 0;
        } else {
            end = base + (uintptr_t)e->prefix_buf_len;
        }
        for (size_t i = 0; i < e->prefix_count && span_ok; i++) {
            const size_t plen = e->prefix_parts[i].len;
            const uint8_t *pdata = e->prefix_parts[i].data;
            o->part_len[i] = plen;
            o->part_ptr[i] = pdata;
            if (plen == 0) continue;
            if (!pdata) {
                NF_DIAG("NF inventory: part %zu is non-empty with NULL data\n",
                        i);
                o->valid = 0;
                continue;
            }
            const uintptr_t pb = (uintptr_t)pdata;
            if (!e->prefix_buf || pb < base || pb > end ||
                (uintptr_t)plen > end - pb) {
                NF_DIAG("NF inventory: part %zu span lies outside prefix_buf"
                        " -- incomparable, not read\n", i);
                o->valid = 0;
                continue;
            }
            if (!nf_inv_copy(o->parts_bytes, NF_INV_BYTES, &o->parts_bytes_len,
                             pdata, plen, "prefix part"))
                o->valid = 0;
        }
    }
    if (e->recv_len > e->recv_cap) {
        NF_DIAG("NF inventory: recv_len %zu exceeds recv_cap %zu\n",
                e->recv_len, e->recv_cap);
        o->valid = 0;
    } else if (!nf_inv_copy(o->recv_bytes, NF_INV_BYTES, &o->recv_bytes_len,
                            e->recv_buf, e->recv_len, "recv_buf"))
        o->valid = 0;
    if (e->token_count > MOQ_DECODED_MAX_TOKENS) {
        NF_DIAG("NF inventory: token_count %zu exceeds %d\n",
                e->token_count, MOQ_DECODED_MAX_TOKENS);
        o->valid = 0;
    } else {
        for (size_t i = 0; i < e->token_count; i++) {
            if (!nf_inv_copy(o->tok_bytes, NF_INV_BYTES, &o->tok_bytes_len,
                             e->resolved_tokens[i].token_value.data,
                             e->resolved_tokens[i].token_value.len,
                             "resolved token value"))
                o->valid = 0;
        }
    }
}

NF_UNUSED static int nf_inv_equals(const nf_inv_t *got, const nf_inv_t *want,
                         const char *what)
{
    if (!got->valid || !want->valid) {
        NF_DIAG("NF %s: owner inventory is INCOMPARABLE\n", what);
        return 1;
    }
    int bad = 0;
#define NF_F(f, fmt) do { \
        if (got->f != want->f) { \
            NF_DIAG("NF %s: owner " #f " " fmt ", expected " fmt "\n", \
                    what, got->f, want->f); \
            bad++; \
        } \
    } while (0)
#define NF_U64(f) do { \
        if (got->f != want->f) { \
            NF_DIAG("NF %s: owner " #f " %llu, expected %llu\n", what, \
                    (unsigned long long)got->f, \
                    (unsigned long long)want->f); \
            bad++; \
        } \
    } while (0)
    NF_F(state, "%d");            NF_F(stream_kind, "%d");
    NF_F(generation, "%u");       NF_U64(handle);
    NF_U64(request_id);           NF_U64(stream_ref);
    NF_F(ep_kind, "%d");          NF_F(ep_slot, "%d");
    NF_F(ep_has_rid, "%d");       NF_U64(ep_rid);
    NF_F(ep_has_ref, "%d");       NF_U64(ep_ref);
    NF_F(interest, "%d");
    NF_F(prefix_buf, "%p");       NF_F(prefix_parts, "%p");
    NF_F(prefix_buf_len, "%zu");  NF_F(prefix_count, "%zu");
    NF_F(prefix_valid, "%d");
    NF_F(got_response, "%d");     NF_F(parse_complete, "%d");
    NF_F(pending_fin, "%d");      NF_F(handoff_fin_pending, "%d");
    NF_F(local_teardown_pending, "%d");
    NF_F(closing_remote_error, "%d"); NF_F(forward, "%d");
    NF_F(auth_processed, "%d");   NF_F(auth_committed, "%d");
    NF_U64(auth_reject_code);
    NF_F(token_count, "%zu");
    NF_F(recv_buf, "%p");         NF_F(recv_len, "%zu");
    NF_F(recv_cap, "%zu");
    if (got->cmp_suffix_ptr && want->cmp_suffix_ptr) {
        NF_F(suffixes, "%p");
    } else if ((got->suffixes != NULL) != (want->suffixes != NULL)) {
        NF_DIAG("NF %s: owner tracker presence %d, expected %d\n", what,
                got->suffixes != NULL, want->suffixes != NULL);
        bad++;
    }
    NF_F(suffixes_inbound, "%d"); NF_F(goaway_sent, "%d");
    NF_F(pfx_bytes_len, "%zu");   NF_F(parts_bytes_len, "%zu");
    NF_F(recv_bytes_len, "%zu");  NF_F(tok_bytes_len, "%zu");
#undef NF_U64
#undef NF_F
    for (size_t i = 0; i < MOQ_DECODED_MAX_NAMESPACE_PARTS; i++) {
        if (got->part_len[i] != want->part_len[i]) {
            NF_DIAG("NF %s: owner prefix part %zu length %zu, expected %zu\n",
                    what, i, got->part_len[i], want->part_len[i]);
            bad++;
        }
        if (got->part_ptr[i] != want->part_ptr[i]) {
            NF_DIAG("NF %s: owner prefix part %zu pointer %p, expected %p\n",
                    what, i, got->part_ptr[i], want->part_ptr[i]);
            bad++;
        }
    }
#define NF_IMG(f, n) do { \
        if (memcmp(got->f, want->f, (n)) != 0) { \
            NF_DIAG("NF %s: owner " #f " differs\n", what); \
            bad++; \
        } \
    } while (0)
    NF_IMG(auth_txn_img, sizeof(got->auth_txn_img));
    NF_IMG(tokens_img, sizeof(got->tokens_img));
    NF_IMG(staged_img, sizeof(got->staged_img));
    if (got->pfx_bytes_len == want->pfx_bytes_len)
        NF_IMG(pfx_bytes, got->pfx_bytes_len);
    if (got->parts_bytes_len == want->parts_bytes_len)
        NF_IMG(parts_bytes, got->parts_bytes_len);
    if (got->recv_bytes_len == want->recv_bytes_len)
        NF_IMG(recv_bytes, got->recv_bytes_len);
    if (got->tok_bytes_len == want->tok_bytes_len)
        NF_IMG(tok_bytes, got->tok_bytes_len);
#undef NF_IMG
    return bad;
}

NF_UNUSED static int nf_inv_check(const moq_session_t *s, int slot,
                        const nf_inv_t *want, const char *what)
{
    nf_inv_t now;
    nf_inv_read(s, slot, &now);
    return nf_inv_equals(&now, want, what);
}

/* An advancing call resets the event-scratch cursor when the event queue is
 * empty (session_call_prepare). Normalizing that -- only that, and only when
 * the captured queue was empty -- keeps real scratch mutation detectable. The
 * same rule the signed FIN and P7 suites apply. */
NF_UNUSED static void nf_expect_after_call_prepare(txs_snapshot_t *snap)
{
    if (snap->event_depth == 0) snap->event_scratch_len = 0;
}

NF_UNUSED static int nf_expect_no_events(moq_session_t *c, const char *what)
{
    int bad = 0;
    moq_event_t ev;
    while (moq_session_poll_events(c, &ev, 1) > 0) {
        fprintf(stderr, "NF %s: unexpected event kind %d\n", what,
                (int)ev.kind);
        moq_event_cleanup(&ev);
        bad++;
    }
    return bad;
}

NF_UNUSED static int nf_expect_no_actions(moq_session_t *c, const char *what)
{
    int bad = 0;
    moq_action_t a;
    while (moq_session_poll_actions(c, &a, 1) > 0) {
        fprintf(stderr, "NF %s: unexpected action kind %d\n", what,
                (int)a.kind);
        moq_action_cleanup(&a);
        bad++;
    }
    return bad;
}

/* -- retirement --------------------------------------------------------
 *
 * ns_sub_free_entry is a SELECTIVE free (session_namespace_sub.c:450-493), so
 * the declared post-free record is not "everything zero": it is every field
 * either CLEARED or PRESERVED according to that contract.
 *
 * CLEARED: state -> FREE, stream_kind -> UNKNOWN, the whole request_ep,
 * recv_len, parse_complete, got_response, pending_fin, closing_remote_error,
 * forward, auth_processed, auth_committed, auth_reject_code, token_count,
 * goaway_sent, handoff_fin_pending, local_teardown_pending, the suffix tracker
 * and its inbound flag, and all prefix storage; generation advances by EXACTLY
 * one.
 *
 * PRESERVED: handle, request_id, stream_ref, namespace_interest, and the
 * receive buffer's identity and capacity -- the pooled storage the selective
 * free deliberately keeps for the next owner.
 *
 * Scope note: this fixture never populates auth tokens, so token_count is 0
 * throughout and the resolved-token / staging images are all-zero on both
 * sides. Their cleared-versus-preserved behaviour is therefore NOT exercised
 * here; they are declared preserved because the free does not touch the arrays
 * themselves.
 */
NF_UNUSED static void nf_inv_apply_free(nf_inv_t *w)
{
    w->state       = MOQ_NS_SUB_FREE;
    w->stream_kind = MOQ_STREAM_KIND_UNKNOWN;
    w->generation += 1;

    w->ep_kind = 0; w->ep_slot = 0; w->ep_has_rid = 0; w->ep_rid = 0;
    w->ep_has_ref = 0; w->ep_ref = 0;

    w->prefix_buf = NULL;  w->prefix_parts = NULL;
    w->prefix_buf_len = 0; w->prefix_count = 0; w->prefix_valid = 0;
    w->pfx_bytes_len = 0;  w->parts_bytes_len = 0;
    memset(w->pfx_bytes, 0, sizeof(w->pfx_bytes));
    memset(w->parts_bytes, 0, sizeof(w->parts_bytes));
    memset(w->part_len, 0, sizeof(w->part_len));
    memset(w->part_ptr, 0, sizeof(w->part_ptr));

    w->got_response = 0; w->parse_complete = 0; w->pending_fin = 0;
    w->handoff_fin_pending = 0; w->local_teardown_pending = 0;
    w->closing_remote_error = 0; w->forward = 0;
    w->auth_processed = 0; w->auth_committed = 0; w->auth_reject_code = 0;

    w->token_count = 0; w->tok_bytes_len = 0;
    memset(w->tok_bytes, 0, sizeof(w->tok_bytes));

    w->recv_len = 0; w->recv_bytes_len = 0;
    memset(w->recv_bytes, 0, sizeof(w->recv_bytes));

    w->suffixes = NULL; w->suffixes_inbound = 0;
    w->goaway_sent = 0;
}


/* The DECLARED inventory of a locally issued, pending-subscriber namespace
 * owner. Built from the free-slot record plus fixture constants -- never read
 * back. The local path (session_namespace_sub.c:597-665) stores the request id
 * and stream ref directly on the owner and inserts a STACK-LOCAL endpoint into
 * the by-id registry; it never copies that endpoint into `e->request_ep`, so
 * the embedded endpoint stays exactly the free-slot zero record. The two
 * registry edges are real and are required separately. */
NF_UNUSED static nf_inv_t nf_local_pending_want(const nf_inv_t *free_rec,
                                      uint32_t generation, uint64_t handle,
                                      uint64_t request_id, uint64_t stream_ref,
                                      const char *const *parts,
                                      size_t part_count,
                                      moq_namespace_interest_t interest)
{
    nf_inv_t w = *free_rec;
    w.state       = MOQ_NS_SUB_PENDING_SUBSCRIBER;
    w.stream_kind = MOQ_STREAM_KIND_NAMESPACE_SUB;
    w.generation  = generation;
    w.handle      = handle;
    w.request_id  = request_id;
    w.stream_ref  = stream_ref;
    w.interest    = (int)interest;
    w.prefix_valid = 1;
    w.prefix_count = part_count;
    w.pfx_bytes_len = 0;
    w.parts_bytes_len = 0;

    /* Validate the WHOLE declaration before a single byte is copied: an
     * oversized declaration must come back explicitly incomparable, never as
     * a partial or out-of-bounds write into the returned record. */
    if (part_count > MOQ_DECODED_MAX_NAMESPACE_PARTS) {
        NF_DIAG("NF declaration: part_count %zu exceeds %d\n", part_count,
                MOQ_DECODED_MAX_NAMESPACE_PARTS);
        w.valid = 0;
        return w;
    }
    if (part_count > 0 && !parts) {
        NF_DIAG("NF declaration: part_count %zu with a NULL parts array\n",
                part_count);
        w.valid = 0;
        return w;
    }
    {
        size_t total = 0;
        for (size_t i = 0; i < part_count; i++) {
            if (!parts[i]) {
                NF_DIAG("NF declaration: part %zu is a NULL string\n", i);
                w.valid = 0;
                return w;
            }
            size_t n = strlen(parts[i]);
            /* subtraction only -- `total + n` could wrap */
            if (total > NF_INV_BYTES || n > NF_INV_BYTES - total) {
                NF_DIAG("NF declaration: parts exceed the %d-byte bound at"
                        " part %zu\n", NF_INV_BYTES, i);
                w.valid = 0;
                return w;
            }
            total += n;
        }
    }

    for (size_t i = 0; i < part_count; i++) {
        size_t n = strlen(parts[i]);
        w.part_len[i] = n;
        memcpy(w.pfx_bytes + w.pfx_bytes_len, parts[i], n);
        w.pfx_bytes_len += n;
        memcpy(w.parts_bytes + w.parts_bytes_len, parts[i], n);
        w.parts_bytes_len += n;
    }
    /* The embedded endpoint is NOT populated on this path. */
    w.ep_kind = 0; w.ep_slot = 0; w.ep_has_rid = 0; w.ep_rid = 0;
    w.ep_has_ref = 0; w.ep_ref = 0;
    /* Addresses of the freshly allocated prefix storage are unknowable; the
     * caller validates presence and bytes, then fills these in. */
    w.prefix_buf = NULL; w.prefix_parts = NULL;
    for (size_t i = 0; i < MOQ_DECODED_MAX_NAMESPACE_PARTS; i++)
        w.part_ptr[i] = NULL;
    w.prefix_buf_len = w.pfx_bytes_len;
    return w;
}

/* Validate the freshly allocated prefix storage COMPLETELY before adopting
 * any address: exact count, exact buffer length, exact per-part lengths,
 * non-NULL storage for every non-empty span, each part pointer at its
 * declared offset inside prefix_buf with the final offset exactly equal to
 * prefix_buf_len, and exact bytes. Only then are the unknowable addresses
 * copied into the expectation -- so the later inventory comparison is not
 * comparing observed pointers against themselves. */
NF_UNUSED static int nf_adopt_prefix_addrs(const moq_session_t *s, int slot,
                                           nf_inv_t *want, const char *what)
{
    /* An incomparable declaration is refused HERE, before any other
     * declaration field is read and before the live owner is inspected --
     * otherwise a structural mismatch could return first and the validity
     * guard would never be reached. */
    if (!want || !want->valid) {
        NF_DIAG("NF %s: the declaration is INCOMPARABLE\n", what);
        return 1;
    }
    if (slot < 0 || (size_t)slot >= s->ns_sub_cap) {
        NF_DIAG("NF %s: slot %d is outside the pool\n", what, slot);
        return 1;
    }
    const moq_ns_sub_entry_t *e = &s->ns_subs[slot];
    int failures = 0;
    if (want->prefix_count > MOQ_DECODED_MAX_NAMESPACE_PARTS) {
        NF_DIAG("NF %s: declared prefix_count %zu exceeds %d\n", what,
                want->prefix_count, MOQ_DECODED_MAX_NAMESPACE_PARTS);
        return 1;
    }
    if (e->prefix_count != want->prefix_count) {
        NF_DIAG("NF %s: prefix_count %zu, expected %zu\n", what,
                e->prefix_count, want->prefix_count);
        failures++;
    }
    if (e->prefix_buf_len != want->prefix_buf_len) {
        NF_DIAG("NF %s: prefix_buf_len %zu, expected %zu\n", what,
                e->prefix_buf_len, want->prefix_buf_len);
        failures++;
    }
    if (!e->prefix_buf || !e->prefix_parts) {
        NF_DIAG("NF %s: prefix storage is absent\n", what);
        return failures + 1;
    }
    if (failures) return failures;   /* topology below would be unfounded */

    /* Declared byte lengths must fit the fixed inventory arrays, and must
     * agree with each other, before any of them is indexed. */
    if (want->pfx_bytes_len > NF_INV_BYTES ||
        want->parts_bytes_len > NF_INV_BYTES ||
        want->prefix_buf_len > NF_INV_BYTES) {
        NF_DIAG("NF %s: declared prefix byte lengths exceed the %d-byte"
                " inventory bound\n", what, NF_INV_BYTES);
        return failures + 1;
    }
    if (want->pfx_bytes_len != want->parts_bytes_len ||
        want->pfx_bytes_len != want->prefix_buf_len) {
        NF_DIAG("NF %s: declared lengths disagree (pfx %zu, parts %zu,"
                " buf %zu)\n", what, want->pfx_bytes_len,
                want->parts_bytes_len, want->prefix_buf_len);
        return failures + 1;
    }
    {   /* the declared part lengths must sum to exactly that length */
        size_t sum = 0;
        for (size_t i = 0; i < want->prefix_count; i++) {
            if (sum > want->parts_bytes_len ||
                want->part_len[i] > want->parts_bytes_len - sum) {
                NF_DIAG("NF %s: declared part lengths exceed %zu at part"
                        " %zu\n", what, want->parts_bytes_len, i);
                return failures + 1;
            }
            sum += want->part_len[i];
        }
        if (sum != want->parts_bytes_len) {
            NF_DIAG("NF %s: declared part lengths sum to %zu, expected %zu\n",
                    what, sum, want->parts_bytes_len);
            return failures + 1;
        }
    }

    const uintptr_t base = (uintptr_t)e->prefix_buf;
    if ((uintptr_t)e->prefix_buf_len > UINTPTR_MAX - base) {
        NF_DIAG("NF %s: prefix_buf span wraps the address space\n", what);
        return failures + 1;
    }
    const uintptr_t end = base + (uintptr_t)e->prefix_buf_len;
    size_t off = 0;
    for (size_t i = 0; i < want->prefix_count; i++) {
        const size_t plen = e->prefix_parts[i].len;
        const uint8_t *pdata = e->prefix_parts[i].data;
        if (plen != want->part_len[i]) {
            NF_DIAG("NF %s: part %zu length %zu, expected %zu\n", what, i,
                    plen, want->part_len[i]);
            failures++;
            continue;
        }
        if (want->part_len[i] > 0 && !pdata) {
            NF_DIAG("NF %s: part %zu is non-empty with NULL data\n", what, i);
            failures++;
            continue;
        }
        /* Topology, by RANGE ARITHMETIC -- and the rejected pointer is never
         * dereferenced. The bytes are compared through the canonical
         * prefix_buf + off instead, which is already known to be in range. */
        if (plen > 0) {
            const uintptr_t pb = (uintptr_t)pdata;
            if (pb < base || pb > end || (uintptr_t)plen > end - pb) {
                NF_DIAG("NF %s: part %zu span lies outside prefix_buf\n",
                        what, i);
                failures++;
                off += want->part_len[i];
                continue;
            }
            if (pb != base + (uintptr_t)off) {
                NF_DIAG("NF %s: part %zu is not at declared offset %zu\n",
                        what, i, off);
                failures++;
                off += want->part_len[i];
                continue;
            }
            if (off > e->prefix_buf_len || plen > e->prefix_buf_len - off ||
                memcmp(e->prefix_buf + off, want->parts_bytes + off,
                       plen) != 0) {
                NF_DIAG("NF %s: part %zu bytes differ\n", what, i);
                failures++;
            }
        }
        off += want->part_len[i];
    }
    if (off != e->prefix_buf_len) {
        NF_DIAG("NF %s: parts span %zu bytes, buffer holds %zu\n", what, off,
                e->prefix_buf_len);
        failures++;
    }
    if (want->pfx_bytes_len == e->prefix_buf_len &&
        memcmp(e->prefix_buf, want->pfx_bytes, want->pfx_bytes_len) != 0) {
        NF_DIAG("NF %s: aggregate prefix bytes differ\n", what);
        failures++;
    }
    if (failures) return failures;

    want->prefix_buf   = e->prefix_buf;
    want->prefix_parts = e->prefix_parts;
    for (size_t i = 0; i < want->prefix_count; i++)
        want->part_ptr[i] = e->prefix_parts[i].data;
    return 0;
}


#endif /* MOQ_TEST_NS_OWNER_INVENTORY_H */
