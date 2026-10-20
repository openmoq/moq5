/*
 * Draft-22 REQUEST_UPDATE of the three namespace requests (§9.5, §9.5.2):
 * PUBLISH_NAMESPACE, SUBSCRIBE_NAMESPACE and SUBSCRIBE_TRACKS.
 *
 * The family files own their entries, states and stream lifecycles. This
 * module owns what the three share: the per-request update record
 * (moq_ns_update_t), its owned storage, the admission of a peer update
 * (decode, Request ID, authorization tokens), the observation record the
 * application sees, the result event, and this side's own outbound update.
 * Every function here is reserve-before-mutate: a refusal leaves the record,
 * the token cache and the Request ID sequence unchanged.
 */

#include "session_internal.h"
#include "request_stream_internal.h"
#include "../internal/validate.h"

/* -- Owned storage ------------------------------------------------- */

static void ns_update_free_prefix(moq_session_t *s, uint8_t **buf, size_t *buf_len,
                                  moq_bytes_t **parts, size_t *count)
{
    moq_prefix_free(s, buf, buf_len, parts, count);
}

void ns_update_clear_owed(moq_session_t *s, moq_ns_update_t *u)
{
    ns_update_free_prefix(s, &u->owed_prefix_buf, &u->owed_prefix_buf_len,
                          &u->owed_prefix_parts, &u->owed_prefix_count);
    if (u->owed_token_bytes)
        s->alloc.free(u->owed_token_bytes, u->owed_token_bytes_len, s->alloc.ctx);
    u->owed_token_bytes = NULL;
    u->owed_token_bytes_len = 0;
    memset(u->owed_tokens, 0, sizeof(u->owed_tokens));
    u->owed_token_count = 0;
    u->owed = false;
    u->owed_flags = 0;
    u->owed_request_id = 0;
    u->owed_forward = false;
}

void ns_update_clear_outstanding(moq_session_t *s, moq_ns_update_t *u)
{
    ns_update_free_prefix(s, &u->out_prefix_buf, &u->out_prefix_buf_len,
                          &u->out_prefix_parts, &u->out_prefix_count);
    u->outstanding = false;
    u->out_flags = 0;
    u->out_request_id = 0;
    u->out_forward = false;
}

void ns_update_release(moq_session_t *s, moq_ns_update_t *u)
{
    ns_update_clear_owed(s, u);
    ns_update_clear_outstanding(s, u);
    memset(u, 0, sizeof(*u));
}

/* Copy the owed update's prefix and resolved tokens into storage the record
 * owns. All-or-nothing: NOMEM leaves the record untouched. */
static moq_result_t ns_update_store_owed(moq_session_t *s, moq_ns_update_t *u,
    const moq_decoded_namespace_update_t *d, const moq_resolved_token_t *tokens,
    size_t token_count)
{
    uint8_t *pbuf = NULL;
    size_t pbuf_len = 0;
    moq_bytes_t *pparts = NULL;
    size_t pcount = 0;
    if (d->has_prefix &&
        !moq_prefix_store(s, &d->prefix, &pbuf, &pbuf_len, &pparts, &pcount))
        return MOQ_ERR_NOMEM;
    size_t total = 0;
    for (size_t i = 0; i < token_count; i++) total += tokens[i].token_value.len;
    uint8_t *tbuf = NULL;
    if (total > 0) {
        tbuf = (uint8_t *)s->alloc.alloc(total, s->alloc.ctx);
        if (!tbuf) {
            moq_prefix_free(s, &pbuf, &pbuf_len, &pparts, &pcount);
            return MOQ_ERR_NOMEM;
        }
    }
    u->owed = true;
    u->owed_request_id = d->request_id;
    u->owed_flags = (d->has_prefix ? MOQ_NAMESPACE_UPDATE_PREFIX : 0) |
                    (d->has_forward ? MOQ_NAMESPACE_UPDATE_FORWARD : 0) |
                    (token_count ? MOQ_NAMESPACE_UPDATE_AUTHORIZATION : 0);
    u->owed_forward = d->has_forward && d->forward;
    u->owed_prefix_buf = pbuf;
    u->owed_prefix_buf_len = pbuf_len;
    u->owed_prefix_parts = pparts;
    u->owed_prefix_count = pcount;
    u->owed_token_bytes = tbuf;
    u->owed_token_bytes_len = total;
    size_t off = 0;
    for (size_t i = 0; i < token_count; i++) {
        size_t len = tokens[i].token_value.len;
        u->owed_tokens[i].token_type = tokens[i].token_type;
        u->owed_tokens[i].token_value.len = len;
        u->owed_tokens[i].token_value.data = len ? tbuf + off : NULL;
        if (len) memcpy(tbuf + off, tokens[i].token_value.data, len);
        off += len;
    }
    u->owed_token_count = token_count;
    return MOQ_OK;
}

/* -- Observation records -------------------------------------------- */

void ns_update_fill_record(const moq_ns_update_t *u,
                           moq_namespace_update_record_t *rec)
{
    memset(rec, 0, sizeof(*rec));
    rec->struct_size = (uint32_t)sizeof(*rec);
    rec->flags = u->owed_flags;
    rec->update_request_id = u->owed_request_id;
    if (u->owed_flags & MOQ_NAMESPACE_UPDATE_PREFIX) {
        rec->track_namespace_prefix.parts = u->owed_prefix_parts;
        rec->track_namespace_prefix.count = u->owed_prefix_count;
    }
    rec->forward = u->owed_forward;
    rec->tokens = u->owed_token_count ? u->owed_tokens : NULL;
    rec->token_count = u->owed_token_count;
}

/* The record, its prefix and its tokens copied into event scratch, borrowed
 * for the event's epoch. The whole projection is measured first so a short
 * scratch refuses before anything is written. */
static moq_result_t ns_update_stage_record(moq_session_t *s, const moq_ns_update_t *u,
    const moq_namespace_update_record_t **out)
{
    size_t saved = s->event_scratch_len;
    moq_namespace_update_record_t *rec = (moq_namespace_update_record_t *)
        event_scratch_alloc_aligned(s, sizeof(*rec),
                                    _Alignof(moq_namespace_update_record_t));
    if (!rec) goto short_scratch;
    ns_update_fill_record(u, rec);
    if (u->owed_flags & MOQ_NAMESPACE_UPDATE_PREFIX) {
        moq_namespace_t copy;
        if (!event_scratch_copy_namespace(s, &rec->track_namespace_prefix, &copy))
            goto short_scratch;
        rec->track_namespace_prefix = copy;
    }
    if (u->owed_token_count) {
        moq_resolved_token_t *toks = (moq_resolved_token_t *)
            event_scratch_alloc_aligned(s,
                u->owed_token_count * sizeof(moq_resolved_token_t),
                _Alignof(moq_resolved_token_t));
        if (!toks) goto short_scratch;
        for (size_t i = 0; i < u->owed_token_count; i++) {
            toks[i].token_type = u->owed_tokens[i].token_type;
            size_t len = u->owed_tokens[i].token_value.len;
            toks[i].token_value.len = len;
            toks[i].token_value.data = NULL;
            if (len) {
                uint8_t *v = event_scratch_copy(s, u->owed_tokens[i].token_value.data, len);
                if (!v) goto short_scratch;
                toks[i].token_value.data = v;
            }
        }
        rec->tokens = toks;
    }
    *out = rec;
    return MOQ_OK;
short_scratch:
    s->event_scratch_len = saved;
    if (saved == 0)
        return close_with_error(s, 0x1, "event scratch permanently too small");
    return MOQ_ERR_WOULD_BLOCK;
}

moq_result_t ns_update_push_request_event(moq_session_t *s,
    moq_request_family_t family, uint64_t handle_raw, const moq_ns_update_t *u)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    size_t saved = s->event_scratch_len;
    const moq_namespace_update_record_t *rec = NULL;
    moq_result_t rc = ns_update_stage_record(s, u, &rec);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.borrow_epoch = s->borrow_epoch;
    switch (family) {
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT:
        ev.kind = MOQ_EVENT_NAMESPACE_UPDATE_REQUEST;
        ev.detail_size = (uint32_t)sizeof(moq_namespace_update_request_event_t);
        ev.u.namespace_update_request.ann._opaque = handle_raw;
        ev.u.namespace_update_request.update_request_id = u->owed_request_id;
        ev.u.namespace_update_request.update = rec;
        break;
    case MOQ_REQUEST_FAMILY_NS_SUB:
        ev.kind = MOQ_EVENT_NS_SUB_UPDATE_REQUEST;
        ev.detail_size = (uint32_t)sizeof(moq_ns_sub_update_request_event_t);
        ev.u.ns_sub_update_request.handle._opaque = handle_raw;
        ev.u.ns_sub_update_request.update_request_id = u->owed_request_id;
        ev.u.ns_sub_update_request.update = rec;
        break;
    default:
        ev.kind = MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST;
        ev.detail_size = (uint32_t)sizeof(moq_subscribe_tracks_update_request_event_t);
        ev.u.subscribe_tracks_update_request.handle._opaque = handle_raw;
        ev.u.subscribe_tracks_update_request.update_request_id = u->owed_request_id;
        ev.u.subscribe_tracks_update_request.update = rec;
        break;
    }
    rc = push_event(s, &ev);
    if (rc < 0) s->event_scratch_len = saved;
    return rc;
}

moq_result_t ns_update_push_result_event(moq_session_t *s,
    moq_request_family_t family, uint64_t handle_raw, uint64_t update_request_id,
    moq_namespace_update_outcome_t outcome, moq_request_role_t role,
    uint64_t error_code, const uint8_t *reason, size_t reason_len,
    bool has_expires, uint64_t expires_ms)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    size_t saved = s->event_scratch_len;
    moq_bytes_t r = { NULL, 0 };
    if (reason_len) {
        r.data = event_scratch_copy(s, reason, reason_len);
        if (!r.data) {
            s->event_scratch_len = saved;
            if (saved == 0)
                return close_with_error(s, 0x1, "event scratch permanently too small");
            return MOQ_ERR_WOULD_BLOCK;
        }
        r.len = reason_len;
    }
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.borrow_epoch = s->borrow_epoch;
    /* The three result details share one layout after the typed handle. */
    moq_namespace_update_result_event_t *x = &ev.u.namespace_update_result;
    x->ann._opaque = handle_raw;
    x->update_request_id = update_request_id;
    x->outcome = outcome;
    x->local_role = role;
    x->error_code = error_code;
    x->reason = r;
    x->has_expires = has_expires;
    x->expires_ms = has_expires ? expires_ms : 0;
    switch (family) {
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT:
        ev.kind = MOQ_EVENT_NAMESPACE_UPDATE_RESULT;
        ev.detail_size = (uint32_t)sizeof(moq_namespace_update_result_event_t);
        break;
    case MOQ_REQUEST_FAMILY_NS_SUB:
        ev.kind = MOQ_EVENT_NS_SUB_UPDATE_RESULT;
        ev.detail_size = (uint32_t)sizeof(moq_ns_sub_update_result_event_t);
        break;
    default:
        ev.kind = MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT;
        ev.detail_size = (uint32_t)sizeof(moq_subscribe_tracks_update_result_event_t);
        break;
    }
    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) s->event_scratch_len = saved;
    return rc;
}

_Static_assert(offsetof(moq_namespace_update_result_event_t, update_request_id) ==
               offsetof(moq_ns_sub_update_result_event_t, update_request_id) &&
               offsetof(moq_namespace_update_result_event_t, expires_ms) ==
               offsetof(moq_ns_sub_update_result_event_t, expires_ms) &&
               sizeof(moq_namespace_update_result_event_t) ==
               sizeof(moq_ns_sub_update_result_event_t) &&
               offsetof(moq_namespace_update_result_event_t, expires_ms) ==
               offsetof(moq_subscribe_tracks_update_result_event_t, expires_ms) &&
               sizeof(moq_namespace_update_result_event_t) ==
               sizeof(moq_subscribe_tracks_update_result_event_t),
               "the namespace update results share one layout");

moq_result_t ns_update_owed_query(const moq_ns_update_t *u,
    moq_namespace_update_record_t *out, size_t out_size)
{
    if (!u->owed) return MOQ_ERR_WRONG_STATE;
    moq_namespace_update_record_t rec;
    ns_update_fill_record(u, &rec);
    size_t n = out_size < sizeof(rec) ? out_size : sizeof(rec);
    rec.struct_size = (uint32_t)n;
    memcpy(out, &rec, n);
    return MOQ_OK;
}

/* -- Peer update admission (responder) ----------------------------- */

moq_result_t ns_update_admit(moq_session_t *s, moq_request_family_t family,
    moq_stream_ref_t ref, const moq_request_envelope_t *env,
    ns_update_admission_t *a)
{
    memset(a, 0, sizeof(*a));
    const moq_request_codec_t *c = s->profile->request_codec;
    moq_request_tokens_t tk;
    moq_result_t rc = c->decode_namespace_update(env->payload, env->payload_len,
                                                 family, &a->d, &tk);
    bool invalid_filter = rc == MOQ_REQUEST_DECODE_INVALID_FILTER;
    if (rc < 0 && !invalid_filter) {
        if (rc == MOQ_REQUEST_DECODE_BAD_PARAMS)
            return close_with_error(s, 0x6, "malformed auth token structure");
        return close_with_error(s, 0x3, "malformed namespace REQUEST_UPDATE");
    }
    rc = moq_request_stream_validate_inbound(s, ref, env->msg_type,
                                             a->d.request_id, &a->endpoint);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    uint64_t reject = 0;
    rc = process_auth_tokens(s, tk.in, tk.count,
        &(moq_auth_resolve_out_t){
            .tokens = a->tokens, .token_count = &a->token_count,
            .max_tokens = MOQ_DECODED_MAX_TOKENS, .reject_code = &reject,
            .staged = a->token_staged, .txn = &a->auth_txn });
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) {
        process_auth_tokens_free_staging(s, a->tokens, a->token_staged,
                                         a->token_count);
        process_auth_tokens_abort_txn(s, &a->auth_txn);
        return rc;
    }
    a->staged = true;
    a->refuse_code = reject ? reject
                   : invalid_filter ? MOQ_REQUEST_ERROR_INVALID_FILTER : 0;
    return MOQ_OK;
}

void ns_update_admission_commit(moq_session_t *s, ns_update_admission_t *a)
{
    s->profile->commit_inbound_request(s, &a->endpoint);
    process_auth_tokens_commit_txn(s, &a->auth_txn);
    process_auth_tokens_free_staging(s, a->tokens, a->token_staged, a->token_count);
    a->staged = false;
}

void ns_update_admission_abort(moq_session_t *s, ns_update_admission_t *a)
{
    if (!a->staged) return;
    process_auth_tokens_free_staging(s, a->tokens, a->token_staged, a->token_count);
    process_auth_tokens_abort_txn(s, &a->auth_txn);
    a->staged = false;
}

/* Record the owed update and surface it. On any refusal the record and the
 * admission are unchanged, so the caller may retry or abort. */
moq_result_t ns_update_defer(moq_session_t *s, moq_request_family_t family,
    uint64_t handle_raw, moq_ns_update_t *u, ns_update_admission_t *a)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_result_t rc = ns_update_store_owed(s, u, &a->d, a->tokens, a->token_count);
    if (rc < 0) return rc;
    rc = ns_update_push_request_event(s, family, handle_raw, u);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) {
        ns_update_clear_owed(s, u);
        return rc;
    }
    ns_update_admission_commit(s, a);
    return MOQ_OK;
}

/* -- Answers -------------------------------------------------------- */

moq_result_t ns_update_check_accept(const moq_session_t *s, const moq_ns_update_t *u,
    const moq_accept_subscription_update_cfg_t *cfg)
{
    (void)s;
    if (!cfg || cfg->struct_size < MOQ_ACCEPT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE)
        return MOQ_ERR_INVAL;
    if (!u->owed) return MOQ_ERR_WRONG_STATE;
    if (cfg->update_request_id != u->owed_request_id) return MOQ_ERR_STALE_HANDLE;
    return MOQ_OK;
}

moq_result_t ns_update_check_reject(const moq_session_t *s, const moq_ns_update_t *u,
    const moq_reject_subscription_update_cfg_t *cfg)
{
    moq_result_t rc = session_update_reject_cfg_check(s, cfg);
    if (rc < 0) return rc;
    if (!u->owed) return MOQ_ERR_WRONG_STATE;
    if (cfg->update_request_id != u->owed_request_id) return MOQ_ERR_STALE_HANDLE;
    return MOQ_OK;
}

moq_result_t ns_update_encode_ok(moq_session_t *s, uint64_t request_id,
                                 uint8_t *buf, size_t cap, size_t *out_len)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_result_t rc = s->profile->encode_request_update_ok(s, &w,
        &(moq_request_update_ok_encode_args_t){ .request_id = request_id });
    if (rc < 0) return rc;
    *out_len = moq_buf_writer_offset(&w);
    return MOQ_OK;
}

moq_result_t ns_update_encode_error(moq_session_t *s, uint64_t request_id,
    uint64_t code, const uint8_t *reason, size_t reason_len,
    uint8_t *buf, size_t cap, size_t *out_len)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_result_t rc = s->profile->encode_request_error(s, &w,
        &(moq_request_error_encode_args_t){
            .request_id = request_id, .error_code = code,
            .reason = reason, .reason_len = reason_len });
    if (rc < 0) return rc;
    *out_len = moq_buf_writer_offset(&w);
    return MOQ_OK;
}

/* -- This side's update (requester) --------------------------------- */

moq_result_t ns_update_cfg_read(const moq_session_t *s,
    const moq_namespace_update_cfg_t *cfg, moq_request_family_t family,
    moq_namespace_update_encode_args_t *out)
{
    if (!cfg || cfg->struct_size < MOQ_NAMESPACE_UPDATE_CFG_V0_SIZE)
        return MOQ_ERR_INVAL;
    uint32_t legal = family == MOQ_REQUEST_FAMILY_NS_SUB
                         ? MOQ_NAMESPACE_UPDATE_PREFIX
                   : family == MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS
                         ? (MOQ_NAMESPACE_UPDATE_PREFIX | MOQ_NAMESPACE_UPDATE_FORWARD)
                         : 0;
    if (cfg->flags & ~legal) return MOQ_ERR_INVAL;
    if (moq_validate_auth_tokens(cfg->auth_tokens, cfg->auth_token_count,
                                 s->profile->auth_token_type_max) < 0 ||
        cfg->auth_token_count > MOQ_DECODED_MAX_TOKENS)
        return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    out->family = family;
    (void)s;
    if (cfg->flags & MOQ_NAMESPACE_UPDATE_PREFIX) {
        /* A prefix may be empty: it then matches every namespace (§2.4.2). */
        if (moq_validate_namespace_min_fields(&cfg->track_namespace_prefix, 0) < 0)
            return MOQ_ERR_INVAL;
        out->has_prefix = true;
        out->prefix = cfg->track_namespace_prefix;
    }
    if (cfg->flags & MOQ_NAMESPACE_UPDATE_FORWARD) {
        out->has_forward = true;
        out->forward = cfg->forward;
    }
    out->auth_tokens = cfg->auth_token_count ? cfg->auth_tokens : NULL;
    out->auth_token_count = cfg->auth_token_count;
    return MOQ_OK;
}

moq_result_t ns_update_send(moq_session_t *s, moq_stream_ref_t ref,
    moq_ns_update_t *u, moq_namespace_update_encode_args_t *args,
    uint64_t *out_update_request_id)
{
    if (u->outstanding) return MOQ_ERR_REQUEST_BLOCKED;
    moq_request_endpoint_t ep;
    moq_result_t rc = s->profile->prepare_request(s, &ep);
    if (rc < 0) return rc;
    args->request_id = ep.request_id;
    size_t saved = s->output_scratch_len;
    size_t bound = 64 + 9 * MOQ_DECODED_MAX_TOKENS;
    if (args->has_prefix) {
        bound += 9;
        for (size_t i = 0; i < args->prefix.count; i++)
            bound += 9 + args->prefix.parts[i].len;
    }
    for (size_t i = 0; i < args->auth_token_count; i++)
        bound += 32 + args->auth_tokens[i].token_value.len;
    uint8_t *buf = (uint8_t *)scratch_alloc_aligned(s, bound, 1);
    if (!buf) {
        s->profile->abort_request(s, &ep);
        return MOQ_ERR_BUFFER;
    }
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, bound);
    rc = s->profile->encode_namespace_update(s, &w, args);
    if (rc < 0) {
        s->output_scratch_len = saved;
        s->profile->abort_request(s, &ep);
        return rc == MOQ_ERR_BUFFER ? MOQ_ERR_INVAL : rc;
    }
    uint8_t *pbuf = NULL;
    size_t pbuf_len = 0;
    moq_bytes_t *pparts = NULL;
    size_t pcount = 0;
    if (args->has_prefix &&
        !moq_prefix_store(s, &args->prefix, &pbuf, &pbuf_len, &pparts, &pcount)) {
        s->output_scratch_len = saved;
        s->profile->abort_request(s, &ep);
        return MOQ_ERR_NOMEM;
    }
    rc = queue_send_bidi(s, ref, buf, moq_buf_writer_offset(&w), false);
    s->output_scratch_len = saved;
    if (rc < 0) {
        moq_prefix_free(s, &pbuf, &pbuf_len, &pparts, &pcount);
        s->profile->abort_request(s, &ep);
        return rc;
    }
    s->profile->commit_request(s, &ep);
    u->outstanding = true;
    u->out_request_id = ep.request_id;
    u->out_flags = (args->has_prefix ? MOQ_NAMESPACE_UPDATE_PREFIX : 0) |
                   (args->has_forward ? MOQ_NAMESPACE_UPDATE_FORWARD : 0);
    u->out_forward = args->has_forward && args->forward;
    u->out_prefix_buf = pbuf;
    u->out_prefix_buf_len = pbuf_len;
    u->out_prefix_parts = pparts;
    u->out_prefix_count = pcount;
    if (out_update_request_id) *out_update_request_id = ep.request_id;
    return MOQ_OK;
}

/* Take the outstanding update's prefix storage, leaving the record without
 * it (the caller installs it as the request's prefix). */
void ns_update_take_out_prefix(moq_ns_update_t *u, uint8_t **buf, size_t *buf_len,
                               moq_bytes_t **parts, size_t *count)
{
    *buf = u->out_prefix_buf;
    *buf_len = u->out_prefix_buf_len;
    *parts = u->out_prefix_parts;
    *count = u->out_prefix_count;
    u->out_prefix_buf = NULL;
    u->out_prefix_buf_len = 0;
    u->out_prefix_parts = NULL;
    u->out_prefix_count = 0;
}

void ns_update_take_owed_prefix(moq_ns_update_t *u, uint8_t **buf, size_t *buf_len,
                                moq_bytes_t **parts, size_t *count)
{
    *buf = u->owed_prefix_buf;
    *buf_len = u->owed_prefix_buf_len;
    *parts = u->owed_prefix_parts;
    *count = u->owed_prefix_count;
    u->owed_prefix_buf = NULL;
    u->owed_prefix_buf_len = 0;
    u->owed_prefix_parts = NULL;
    u->owed_prefix_count = 0;
}

/* -- Namespace prefix arithmetic ------------------------------------ */

bool ns_update_has_prefix(const moq_bytes_t *prefix, size_t prefix_count,
                          const moq_bytes_t *ns, size_t ns_count)
{
    if (prefix_count > ns_count) return false;
    for (size_t i = 0; i < prefix_count; i++) {
        if (prefix[i].len != ns[i].len) return false;
        if (prefix[i].len && memcmp(prefix[i].data, ns[i].data, prefix[i].len) != 0)
            return false;
    }
    return true;
}

/* -- Public configuration ------------------------------------------- */

void (moq_namespace_update_cfg_init)(moq_namespace_update_cfg_t *cfg)
{
    moq_namespace_update_cfg_init_sized(cfg, MOQ_NAMESPACE_UPDATE_CFG_V0_SIZE);
}

void moq_namespace_update_cfg_init_sized(moq_namespace_update_cfg_t *cfg,
                                         size_t cfg_size)
{
    if (!cfg || cfg_size < sizeof(cfg->struct_size)) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

moq_result_t moq_session_set_defer_namespace_updates(moq_session_t *s, bool defer)
{
    if (!s) return MOQ_ERR_INVAL;
    s->defer_namespace_updates = defer;
    return MOQ_OK;
}
