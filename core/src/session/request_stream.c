/*
 * Request-stream engine shared by the wire profiles that carry one request per
 * bidirectional stream.
 *
 * The session core owns request slots, buffering and lifecycle; this module
 * owns what those profiles have in common above the codec: parity-allocated
 * request IDs with reordered-arrival tracking, the dispatch of buffered
 * request and response messages into the core handlers, per-request GOAWAY
 * migration, and the first-message rules on both directions of a request
 * stream. A profile embeds moq_request_stream_state_t at the start of its
 * profile state and points its vtable at the entry points declared in
 * request_stream_internal.h.
 */

#include "request_stream_internal.h"

static inline moq_request_stream_state_t *rs_state(moq_session_t *s)
{
    return (moq_request_stream_state_t *)s->profile_state;
}

static inline const moq_request_stream_state_t *rs_state_const(const moq_session_t *s)
{
    return (const moq_request_stream_state_t *)s->profile_state;
}

void moq_request_stream_state_init(moq_request_stream_state_t *rs,
                                   moq_perspective_t perspective,
                                   const moq_alloc_t *alloc)
{
    memset(rs, 0, sizeof(*rs));
    rs->next_local_request_id = (perspective == MOQ_PERSPECTIVE_CLIENT) ? 0 : 1;
    rs->peer_next_request_id  = (perspective == MOQ_PERSPECTIVE_CLIENT) ? 1 : 0;
    rs->alloc = *alloc;
    rs->next_track_alias = 1;
}

void moq_request_stream_state_destroy(moq_request_stream_state_t *rs)
{
    if (rs->peer_seen_later)
        rs->alloc.free(rs->peer_seen_later,
                       rs->peer_seen_cap * sizeof(moq_request_id_range_t),
                       rs->alloc.ctx);
}

uint64_t moq_request_stream_peer_next_id(const moq_session_t *s)
{
    return rs_state_const(s)->peer_next_request_id;
}

/* -- Request admission --------------------------------------------- *
 * Request IDs are parity-allocated; there is no MAX_REQUEST_ID gate (QUIC
 * stream limits provide flow control). Each outbound request travels on its own
 * bidi stream; the session core mints the stream_ref and registers it. */

moq_result_t moq_request_stream_prepare_request(moq_session_t *s,
                                        struct moq_request_endpoint *out)
{
    moq_request_stream_state_t *rs = rs_state(s);
    memset(out, 0, sizeof(*out));
    out->has_request_id = true;
    out->request_id = rs->next_local_request_id;
    return MOQ_OK;
}

void moq_request_stream_commit_request(moq_session_t *s,
                               const struct moq_request_endpoint *ep)
{
    (void)ep;
    moq_request_stream_state_t *rs = rs_state(s);
    rs->next_local_request_id += 2;   /* keep parity */
}

void moq_request_stream_abort_request(moq_session_t *s,
                              const struct moq_request_endpoint *ep)
{
    (void)s; (void)ep;   /* request ID not consumed until commit */
}

void moq_request_stream_release_request(moq_session_t *s,
                                const struct moq_request_endpoint *ep)
{
    (void)s; (void)ep;   /* request IDs are not recycled */
}

uint64_t moq_request_stream_next_track_alias(const moq_session_t *s)
{
    return rs_state_const(s)->next_track_alias;
}

void moq_request_stream_advance_track_alias(moq_session_t *s, uint64_t next_after)
{
    moq_request_stream_state_t *rs = rs_state(s);
    if (next_after >= rs->next_track_alias)
        rs->next_track_alias = next_after + 1;
}

/* Later request bidis can arrive first. Keep merged ranges above the lowest
 * missing peer ID; resource use follows disjoint gaps, not ID distance. */
static size_t rs_peer_seen_position(const moq_request_stream_state_t *rs,
                                     uint64_t request_id)
{
    size_t lo = 0, hi = rs->peer_seen_count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (rs->peer_seen_later[mid].last < request_id)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

static moq_result_t rs_check_inbound_id(moq_session_t *s, uint64_t wire_request_id)
{
    moq_request_stream_state_t *rs = rs_state(s);
    bool peer_is_client = (s->perspective == MOQ_PERSPECTIVE_SERVER);
    uint64_t expected_parity = peer_is_client ? 0 : 1;
    if ((wire_request_id & 1) != expected_parity)
        return close_with_error(s, 0x4, "wrong request ID parity");
    if (rs->peer_ids_exhausted ||
        wire_request_id < rs->peer_next_request_id)
        return close_with_error(s, 0x4, "duplicate request ID");
    if (wire_request_id > rs->peer_next_request_id) {
        size_t pos = rs_peer_seen_position(rs, wire_request_id);
        if (pos < rs->peer_seen_count &&
            rs->peer_seen_later[pos].first <= wire_request_id)
            return close_with_error(s, 0x4, "duplicate request ID");
    }
    return MOQ_OK;
}

static moq_result_t rs_reserve_peer_ranges(moq_session_t *s, size_t required)
{
    moq_request_stream_state_t *rs = rs_state(s);
    if (required <= rs->peer_seen_cap) return MOQ_OK;
    if (required > MOQ_REQUEST_PEER_ID_MAX_RANGES)
        return close_with_error(s, 0x1, "request ID ranges exhausted");
    size_t cap = rs->peer_seen_cap ? rs->peer_seen_cap : 8;
    while (cap < required) {
        cap *= 2;
        if (cap > MOQ_REQUEST_PEER_ID_MAX_RANGES) cap = MOQ_REQUEST_PEER_ID_MAX_RANGES;
    }
    moq_request_id_range_t *ranges = rs->peer_seen_later
        ? rs->alloc.realloc(rs->peer_seen_later,
            rs->peer_seen_cap * sizeof(moq_request_id_range_t),
            cap * sizeof(moq_request_id_range_t), rs->alloc.ctx)
        : rs->alloc.alloc(cap * sizeof(moq_request_id_range_t), rs->alloc.ctx);
    if (!ranges) return MOQ_ERR_NOMEM;
    rs->peer_seen_later = ranges;
    rs->peer_seen_cap = cap;
    return MOQ_OK;
}

moq_result_t moq_request_stream_validate_inbound(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t msg_type,
    uint64_t wire_request_id, struct moq_request_endpoint *out)
{
    (void)msg_type;
    moq_result_t rc = rs_check_inbound_id(s, wire_request_id);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    moq_request_stream_state_t *rs = rs_state(s);
    if (wire_request_id > rs->peer_next_request_id) {
        size_t pos = rs_peer_seen_position(rs, wire_request_id);
        bool join_before = pos > 0 &&
            rs->peer_seen_later[pos - 1].last == wire_request_id - 2;
        bool join_after = pos < rs->peer_seen_count &&
            wire_request_id <= UINT64_MAX - 2 &&
            rs->peer_seen_later[pos].first == wire_request_id + 2;
        /* Commit is void and must remain allocation-free after the request
         * handler has performed its other fallible work. */
        if (!join_before && !join_after) {
            rc = rs_reserve_peer_ranges(s, rs->peer_seen_count + 1);
            if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
        }
    }
    memset(out, 0, sizeof(*out));
    out->has_request_id = true;
    out->request_id = wire_request_id;
    out->has_stream_ref = true;
    out->stream_ref = ref;
    return MOQ_OK;
}

/* Count the virtual union with at most two prepared IDs, dropping its contiguous
 * floor. No range copy or semantic mutation; reserve the peak of both commits. */
static size_t rs_prepared_range_count(const moq_request_stream_state_t *rs,
    uint64_t first_id, const uint64_t *second_id)
{
    uint64_t ids[2] = { first_id, second_id ? *second_id : first_id };
    size_t id_count = second_id ? 2 : 1;
    if (ids[0] > ids[1]) {
        uint64_t tmp = ids[0]; ids[0] = ids[1]; ids[1] = tmp;
    }
    size_t id = 0, range = 0, count = 0;
    uint64_t next = rs->peer_next_request_id, last = 0;
    bool beyond_floor = false;
    while (id < id_count || range < rs->peer_seen_count) {
        uint64_t begin, end;
        if (id < id_count && (range == rs->peer_seen_count ||
                             ids[id] < rs->peer_seen_later[range].first)) {
            begin = end = ids[id++];
        } else {
            begin = rs->peer_seen_later[range].first;
            end = rs->peer_seen_later[range++].last;
        }
        if (!beyond_floor && begin == next) {
            if (end > UINT64_MAX - 2) break;
            next = end + 2;
        } else {
            if (!beyond_floor || last > UINT64_MAX - 2 || begin != last + 2) count++;
            beyond_floor = true;
            last = end;
        }
    }
    return count;
}

static moq_result_t rs_validate_inbound_after(moq_session_t *s,
    moq_stream_ref_t ref, uint64_t msg_type, uint64_t id,
    const moq_request_endpoint_t *prefix, moq_request_endpoint_t *out)
{
    if (!prefix) return moq_request_stream_validate_inbound(s, ref, msg_type, id, out);
    moq_result_t rc = rs_check_inbound_id(s, id);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    if (id == prefix->request_id)
        return close_with_error(s, 0x4, "duplicate prepared request ID");
    const moq_request_stream_state_t *rs = rs_state_const(s);
    size_t peak = rs->peer_seen_count;
    size_t count = rs_prepared_range_count(rs, prefix->request_id, NULL);
    if (count > peak) peak = count;
    count = rs_prepared_range_count(rs, prefix->request_id, &id);
    if (count > peak) peak = count;
    rc = rs_reserve_peer_ranges(s, peak);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    memset(out, 0, sizeof(*out));
    out->has_request_id = true;
    out->request_id = id;
    out->has_stream_ref = true;
    out->stream_ref = ref;
    return MOQ_OK;
}

void moq_request_stream_commit_inbound(moq_session_t *s,
                                       const struct moq_request_endpoint *ep)
{
    moq_request_stream_state_t *rs = rs_state(s);
    if (ep->request_id != rs->peer_next_request_id) {
        size_t pos = rs_peer_seen_position(rs, ep->request_id);
        bool join_before = pos > 0 &&
            rs->peer_seen_later[pos - 1].last == ep->request_id - 2;
        bool join_after = pos < rs->peer_seen_count &&
            ep->request_id <= UINT64_MAX - 2 &&
            rs->peer_seen_later[pos].first == ep->request_id + 2;
        if (join_before && join_after) {
            rs->peer_seen_later[pos - 1].last = rs->peer_seen_later[pos].last;
            memmove(&rs->peer_seen_later[pos],
                    &rs->peer_seen_later[pos + 1],
                    (rs->peer_seen_count - pos - 1) * sizeof(moq_request_id_range_t));
            rs->peer_seen_count--;
        } else if (join_before) {
            rs->peer_seen_later[pos - 1].last = ep->request_id;
        } else if (join_after) {
            rs->peer_seen_later[pos].first = ep->request_id;
        } else {
            memmove(&rs->peer_seen_later[pos + 1],
                    &rs->peer_seen_later[pos],
                    (rs->peer_seen_count - pos) * sizeof(moq_request_id_range_t));
            rs->peer_seen_later[pos] =
                (moq_request_id_range_t){ ep->request_id, ep->request_id };
            rs->peer_seen_count++;
        }
        return;
    }
    if (rs->peer_next_request_id > UINT64_MAX - 2) {
        rs->peer_ids_exhausted = true;
        return;
    }
    rs->peer_next_request_id += 2;
    while (rs->peer_seen_count &&
           rs->peer_seen_later[0].first == rs->peer_next_request_id) {
        uint64_t last = rs->peer_seen_later[0].last;
        rs->peer_seen_count--;
        memmove(&rs->peer_seen_later[0], &rs->peer_seen_later[1],
                rs->peer_seen_count * sizeof(moq_request_id_range_t));
        if (last > UINT64_MAX - 2) {
            rs->peer_ids_exhausted = true;
            return;
        }
        rs->peer_next_request_id = last + 2;
    }
}

/* Map the representable SUBSCRIBE/FETCH settings onto draft-18 Message
 * Parameters, emitting a parameter only when it differs from the protocol
 * default (so the omitted form yields the default on the peer). */

/* -- Request-stream dispatch ---------------------------------------- */

static moq_request_family_t rs_family_from_req_kind(uint32_t kind)
{
    switch (kind) {
    case MOQ_REQ_SUBSCRIPTION:     return MOQ_REQUEST_FAMILY_SUBSCRIBE;
    case MOQ_REQ_FETCH:            return MOQ_REQUEST_FAMILY_FETCH;
    case MOQ_REQ_TRACK_STATUS:     return MOQ_REQUEST_FAMILY_TRACK_STATUS;
    case MOQ_REQ_ANNOUNCEMENT:     return MOQ_REQUEST_FAMILY_ANNOUNCEMENT;
    case MOQ_REQ_NAMESPACE_SUB:    return MOQ_REQUEST_FAMILY_NS_SUB;
    case MOQ_REQ_PUBLISH:          return MOQ_REQUEST_FAMILY_PUBLISH;
    case MOQ_REQ_SUBSCRIBE_TRACKS: return MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS;
    default:                       return (moq_request_family_t)0;
    }
}

typedef struct rs_output_owner {
    moq_request_stream_t *stream;
    uint32_t generation;
} rs_output_owner_t;

static rs_output_owner_t rs_output_owner(moq_session_t *s,
    moq_request_family_t family, int slot)
{
    switch (family) {
    case MOQ_REQUEST_FAMILY_SUBSCRIBE:
        if (s->subs[slot].state != MOQ_SUB_FREE)
            return (rs_output_owner_t){&s->subs[slot].rs, s->subs[slot].generation};
        break;
    case MOQ_REQUEST_FAMILY_FETCH:
        if (s->fetches[slot].state != MOQ_FETCH_FREE)
            return (rs_output_owner_t){&s->fetches[slot].rs, s->fetches[slot].generation};
        break;
    case MOQ_REQUEST_FAMILY_TRACK_STATUS:
        if (s->track_statuses[slot].state != MOQ_TS_FREE)
            return (rs_output_owner_t){&s->track_statuses[slot].rs, s->track_statuses[slot].generation};
        break;
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT:
        if (s->announcements[slot].state != MOQ_ANN_FREE)
            return (rs_output_owner_t){&s->announcements[slot].rs, s->announcements[slot].generation};
        break;
    case MOQ_REQUEST_FAMILY_PUBLISH:
        if (s->publishes[slot].state != MOQ_PUB_FREE)
            return (rs_output_owner_t){&s->publishes[slot].rs, s->publishes[slot].generation};
        break;
    case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS:
        if (s->track_subs[slot].state != MOQ_TRACK_SUB_FREE)
            return (rs_output_owner_t){&s->track_subs[slot].rs, s->track_subs[slot].generation};
        break;
    default:
        break;
    }
    return (rs_output_owner_t){NULL, 0};
}

static void rs_output_transferred(moq_session_t *s, moq_request_family_t family,
    int slot, uint32_t generation)
{
    /* Dispatch can retire the logical owner. Re-resolve it before clearing. */
    rs_output_owner_t owner = rs_output_owner(s, family, slot);
    if (owner.stream && owner.generation == generation)
        owner.stream->retained_output = MOQ_RETAINED_OUTPUT_NONE;
}

/* Handle a GOAWAY (§10.4) decoded on an established request bidi: validate
 * (a server must not receive a non-zero New Session URI), then migrate + free
 * via the draft-neutral core, which drains the bidi unless the peer's FIN has
 * already been observed. The GOAWAY does not end the request, so messages the
 * peer still owes may follow it -- in this read or later ones; they are
 * discarded, and a second GOAWAY among them closes the session. `off` is the
 * end of the GOAWAY message within the buffered bytes; every byte after it is
 * consumed here. `peer_fin_observed` is the owner's cumulative FIN fact as its
 * dispatcher received it -- a FIN latched on an earlier call counts, not only
 * one riding this chunk. */
moq_result_t moq_request_stream_process_goaway(moq_session_t *s, moq_stream_ref_t ref,
    moq_request_family_t family, int slot,
    const moq_request_envelope_t *env, const uint8_t *buf, size_t off,
    size_t len, bool peer_fin_observed, size_t *out_consumed)
{
    const moq_request_codec_t *c = s->profile->request_codec;
    moq_request_goaway_msg_t ga;
    moq_result_t rc = c->decode_goaway_request(env->payload, env->payload_len, &ga);
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed request-stream GOAWAY");
    if (s->perspective == MOQ_PERSPECTIVE_SERVER && ga.uri.len > 0)
        return close_with_error(s, 0x3,
            "server received GOAWAY with non-zero New Session URI");
    /* What followed the GOAWAY in this read is scanned before the request is
     * retired (the bytes live in its buffer); the framing state carries over
     * to the drain that absorbs later reads. */
    moq_drain_scan_t scan;
    memset(&scan, 0, sizeof(scan));
    if (off < len) {
        rc = drain_goaway_scan(s, &scan, buf + off, len - off);
        if (rc < 0 || s->state == MOQ_SESS_CLOSED)
            return rc;
    }
    if (family == MOQ_REQUEST_FAMILY_TRACK_STATUS &&
        s->profile->premature_fin_fails_request)
        s->track_statuses[slot].response_validated = true;
    if (family == MOQ_REQUEST_FAMILY_ANNOUNCEMENT &&
        s->profile->premature_fin_fails_request)
        ann_validate_response(s, slot);
    rs_output_owner_t owner = rs_output_owner(s, family, slot);
    if (owner.stream) {
        if (s->profile->premature_fin_fails_request &&
            (family == MOQ_REQUEST_FAMILY_ANNOUNCEMENT ||
             family == MOQ_REQUEST_FAMILY_TRACK_STATUS))
            owner.stream->retained_output = MOQ_RETAINED_OUTPUT_CANCEL;
        else if (family != MOQ_REQUEST_FAMILY_TRACK_STATUS)
            owner.stream->retained_output = MOQ_RETAINED_OUTPUT_FIN;
    }
    rc = session_core_on_request_goaway(s, family, slot, ref, ga.uri.data,
                                        ga.uri.len, ga.timeout_ms,
                                        peer_fin_observed);
    if (rc < 0)
        return rc;
    if (s->state == MOQ_SESS_CLOSED)
        return MOQ_OK;
    rs_output_transferred(s, family, slot, owner.generation);
    moq_drain_scan_t *kept = drain_ref_scan(s, ref);
    if (kept) *kept = scan;
    if (family == MOQ_REQUEST_FAMILY_TRACK_STATUS &&
        s->profile->premature_fin_fails_request)
        s->track_statuses[slot].migration_scan = scan;
    if (family == MOQ_REQUEST_FAMILY_ANNOUNCEMENT &&
        s->profile->premature_fin_fails_request)
        s->announcements[slot].migration_scan = scan;
    *out_consumed = len;
    return MOQ_OK;
}


/* Resolve a decoded token list against the session token cache, staging into
 * the record's resolved-token array. Fatal errors close; a message-level
 * reject is carried in *out_reject_code for the core to turn into REQUEST_ERROR. */
static moq_result_t rs_resolve_tokens(moq_session_t *s, const moq_request_tokens_t *tk,
                                      moq_resolved_token_t *out_tokens,
                                      size_t *out_token_count, bool *out_staged,
                                      moq_auth_txn_t *txn, uint64_t *out_reject_code)
{
    /* A rejection already decided by the decoder (a refused filter) stands
     * unless a token fails too; the token's code then takes precedence. */
    uint64_t preset = *out_reject_code;
    moq_result_t rc = process_auth_tokens(s, tk->in, tk->count,
        &(moq_auth_resolve_out_t){
            .tokens = out_tokens, .token_count = out_token_count,
            .max_tokens = MOQ_DECODED_MAX_TOKENS, .reject_code = out_reject_code,
            .staged = out_staged, .txn = txn });
    if (*out_reject_code == 0) *out_reject_code = preset;
    return rc;
}

/* A new request the session refuses itself (session_refuses_request) fails
 * through the handler's message-level reject path with that code, surfacing
 * no event; the code stands over any token or filter refusal. */
static void rs_refuse_request(moq_session_t *s, const moq_namespace_t *ns,
                              uint64_t *reject_code)
{
    uint64_t code;
    if (session_refuses_request(s, ns, &code)) *reject_code = code;
}

/* Close for a request whose body the codec refused: a malformed parameter
 * block has its own close code; anything else is a malformed message. */
static moq_result_t rs_request_decode_failed(moq_session_t *s, moq_result_t rc,
                                             const char *malformed)
{
    if (rc == MOQ_REQUEST_DECODE_BAD_PARAMS)
        return close_with_error(s, 0x6, "malformed auth token structure");
    return close_with_error(s, 0x3, malformed);
}

/* Decode a REQUEST_UPDATE and admit its request ID; shared by every
 * established-stream update branch. On success *d carries the body and
 * *ep the admitted endpoint. */
static moq_result_t rs_decode_admit_update_after(moq_session_t *s, moq_stream_ref_t ref,
                                           const moq_request_envelope_t *env,
                                           moq_request_family_t target_family,
                                           bool from_subscriber,
                                           moq_decoded_request_update_t *d,
                                           moq_request_tokens_t *tk,
                                           moq_request_endpoint_t *ep,
                                           const moq_request_endpoint_t *prefix)
{
    const moq_request_codec_t *c = s->profile->request_codec;
    moq_result_t rc = c->decode_request_update(env->payload, env->payload_len,
                                               target_family, from_subscriber, d, tk);
    const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
    if (rc < 0 && !invalid_filter)
        return rs_request_decode_failed(s, rc, "malformed REQUEST_UPDATE");
    rc = rs_validate_inbound_after(s, ref, env->msg_type,
                                   d->request_id, prefix, ep);
    if (rc < 0)
        return rc;
    /* The update is admitted and then refused: the family handler answers
     * REQUEST_ERROR with this code and applies nothing. */
    if (invalid_filter) d->auth_reject_code = MOQ_REQUEST_ERROR_INVALID_FILTER;
    return MOQ_OK;
}

static moq_result_t rs_decode_admit_update(moq_session_t *s, moq_stream_ref_t ref,
    const moq_request_envelope_t *env, moq_request_family_t target_family,
    bool from_subscriber, moq_decoded_request_update_t *d,
    moq_request_tokens_t *tk, moq_request_endpoint_t *ep)
{
    return rs_decode_admit_update_after(s, ref, env, target_family,
        from_subscriber, d, tk, ep, NULL);
}

/* Retain the bounded span until the application answers its initial request.
 * Framing/credit checks do not admit an ID or stage authorization effects. */
static moq_result_t rs_defer_pending_update(moq_session_t *s,
                                            moq_request_family_t family,
                                            const uint8_t *buf, size_t len,
                                            bool fin)
{
    const moq_request_codec_t *c = s->profile->request_codec;
    size_t off = 0;
    uint32_t updates = 0;
    while (off < len) {
        moq_request_envelope_t env;
        size_t consumed = 0;
        moq_result_t rc = c->decode_envelope(buf + off, len - off,
                                            &env, &consumed);
        if (rc == MOQ_ERR_BUFFER) {
            if (fin)
                return close_with_error(s, 0x3,
                    "truncated message after pending REQUEST_UPDATE");
            return MOQ_ERR_WOULD_BLOCK;
        }
        if (rc < 0 || consumed == 0)
            return close_with_error(s, 0x3, "malformed request envelope");
        if (env.msg_type == c->type.request_update) {
            updates++;
            if (s->profile->request_updates_max != 0 &&
                updates > s->profile->request_updates_max)
                return close_with_error(s,
                    s->profile->request_updates_exceeded_error,
                    "too many outstanding REQUEST_UPDATEs");
        } else if (family == MOQ_REQUEST_FAMILY_PUBLISH &&
                   env.msg_type == c->type.publish_done) {
            moq_request_publish_done_msg_t done;
            rc = c->decode_publish_done(env.payload, env.payload_len, &done);
            if (rc < 0)
                return close_with_error(s, 0x3, "malformed PUBLISH_DONE");
            if (consumed < len - off)
                return close_with_error(s, 0x3,
                    "extra bytes after PUBLISH_DONE");
        }
        off += consumed;
    }
    return MOQ_ERR_WOULD_BLOCK;
}

/* Prepare the complete update that explicit initial rejection cancels. The
 * caller owns commit/abort after reserving the terminal output batch. Partial
 * input has no decoded ID or token effects; a trailing PUBLISH_DONE is framed
 * and deliberately discarded by cancellation, not dispatched as completion. */
moq_result_t moq_request_stream_prepare_pending_reject(moq_session_t *s,
    const moq_request_stream_t *rs, moq_request_family_t family, bool peer_fin,
    moq_decoded_request_update_t *out)
{
    return moq_request_stream_prepare_pending_reject_after(s, rs, family,
        peer_fin, NULL, NULL, out);
}

moq_result_t moq_request_stream_prepare_pending_reject_after(moq_session_t *s,
    const moq_request_stream_t *rs, moq_request_family_t family, bool peer_fin,
    const moq_request_endpoint_t *prefix_id, const moq_auth_txn_t *prefix_auth,
    moq_decoded_request_update_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!s->profile->request_retains_pending_original_input || rs->recv_len == 0)
        return MOQ_OK;
    const moq_request_codec_t *c = s->profile->request_codec;
    moq_request_envelope_t env;
    size_t consumed = 0;
    moq_result_t rc = c->decode_envelope(rs->recv_buf, rs->recv_len, &env, &consumed);
    if (rc == MOQ_ERR_BUFFER)
        return MOQ_OK;
    if (rc < 0 || consumed == 0)
        return close_with_error(s, 0x3, "malformed request envelope");
    if (env.msg_type != c->type.request_update)
        return MOQ_OK;
    rc = rs_defer_pending_update(s, family, rs->recv_buf, rs->recv_len, peer_fin);
    if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
    if (rc != MOQ_ERR_WOULD_BLOCK) return rc;
    moq_request_tokens_t tk;
    moq_request_endpoint_t ep;
    rc = rs_decode_admit_update_after(s, rs->ref, &env, family,
        family == MOQ_REQUEST_FAMILY_SUBSCRIBE, out, &tk, &ep, prefix_id);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    out->endpoint = ep;
    uint64_t preset = out->auth_reject_code;
    rc = process_auth_tokens_after(s, tk.in, tk.count, out->tokens,
        &out->token_count, MOQ_DECODED_MAX_TOKENS, &out->auth_reject_code,
        out->token_staged, &out->auth_txn, prefix_auth);
    if (!out->auth_reject_code) out->auth_reject_code = preset;
    return rc;
}

moq_result_t moq_request_stream_process_request(
    moq_session_t *s, moq_stream_ref_t ref, int slot,
    const uint8_t *buf, size_t len, bool fin, size_t *out_consumed)
{
    *out_consumed = 0;
    const moq_request_codec_t *c = s->profile->request_codec;
    moq_request_envelope_t env;
    size_t off = 0;
    moq_result_t rc = c->decode_envelope(buf, len, &env, &off);
    if (rc == MOQ_ERR_BUFFER)
        return MOQ_OK;            /* incomplete; the core waits / handles FIN */
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed request envelope");

    /* A GOAWAY on a *committed* request bidi migrates that single request (§10.4).
     * It is valid only once the request owns the bidi; as a first message on a
     * fresh staging stream (no committed request) it is a protocol violation. */
    if (env.msg_type == c->type.goaway) {
        moq_request_endpoint_t gep = request_registry_find_by_streamref(s, ref);
        moq_request_family_t fam = rs_family_from_req_kind((uint32_t)gep.kind);
        bool committed = (fam != (moq_request_family_t)0) &&
            !(gep.kind == MOQ_REQ_SUBSCRIPTION &&
              s->subs[gep.slot].state == MOQ_SUB_RECVING_REQUEST);
        if (!committed)
            return close_with_error(s, 0x3,
                "GOAWAY as first message on a request stream");
        return moq_request_stream_process_goaway(s, ref, fam, gep.slot, &env, buf, off, len, fin,
                                 out_consumed);
    }

    if (s->profile->request_retains_pending_original_input) {
        moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
        if (ep.kind == MOQ_REQ_SUBSCRIPTION &&
            s->subs[ep.slot].state == MOQ_SUB_PENDING_PUBLISHER &&
            s->subs[ep.slot].role == MOQ_SUB_ROLE_PUBLISHER) {
            if (env.msg_type != c->type.request_update)
                return close_with_error(s, 0x3,
                    "unexpected message on pending subscribe request bidi");
            return rs_defer_pending_update(s, MOQ_REQUEST_FAMILY_SUBSCRIBE,
                                           buf, len, fin);
        }
    }

    /* An established announce bidi (keyed ANNOUNCEMENT after the PUBLISH_NAMESPACE
     * handoff) carries only a REQUEST_UPDATE. Handle that kind definitively here,
     * before the first-message branches, so a peer cannot send SUBSCRIBE / FETCH /
     * PUBLISH_NAMESPACE on it and have the announce slot used as a subscription or
     * fetch slot (cross-pool corruption / out-of-bounds when ann_cap > sub_cap).
     * §10.9.1: a failed announce REQUEST_UPDATE closes the bidi; real
     * announce-update is unsupported, so reject + terminate. */
    {
        moq_request_endpoint_t aep = request_registry_find_by_streamref(s, ref);
        if (aep.kind == MOQ_REQ_ANNOUNCEMENT) {
            if (env.msg_type != c->type.request_update)
                return close_with_error(s, 0x3,
                    "unexpected message on announce request bidi");
            /* §9.5: the announcer may pipeline its update behind the request.
             * Retain it until the application answers the announcement. */
            if (c->decode_namespace_update &&
                s->announcements[aep.slot].state == MOQ_ANN_PENDING_RECEIVER &&
                s->announcements[aep.slot].role == MOQ_ANN_ROLE_RECEIVER)
                return MOQ_ERR_WOULD_BLOCK;
            if (s->announcements[aep.slot].state != MOQ_ANN_ESTABLISHED ||
                s->announcements[aep.slot].role != MOQ_ANN_ROLE_RECEIVER)
                return close_with_error(s, 0x3,
                    "REQUEST_UPDATE on non-established announcement");
            if (c->decode_namespace_update) {
                if (s->profile->premature_fin_fails_request)
                    ann_validate_response(s, aep.slot);
                rs_output_owner_t owner = rs_output_owner(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT, aep.slot);
                owner.stream->retained_output = MOQ_RETAINED_OUTPUT_RESPONSE;
                rc = s->defer_namespace_updates
                    ? ann_on_namespace_update(s, aep.slot, &env)
                    : ann_refuse_namespace_update(s, aep.slot, &env);
                if (rc < 0)
                    return rc;
                if (s->state == MOQ_SESS_CLOSED)
                    return MOQ_OK;
                rs_output_transferred(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT, aep.slot, owner.generation);
                *out_consumed = off;
                return MOQ_OK;
            }
            moq_decoded_request_update_t u;
            moq_request_tokens_t tk;
            moq_request_endpoint_t uep;
            rc = rs_decode_admit_update(s, ref, &env, MOQ_REQUEST_FAMILY_ANNOUNCEMENT, true, &u, &tk, &uep);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            if (s->profile->premature_fin_fails_request)
                ann_validate_response(s, aep.slot);
            rs_output_owner_t owner = rs_output_owner(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT, aep.slot);
            owner.stream->retained_output = MOQ_RETAINED_OUTPUT_RESPONSE;
            rc = session_core_on_announce_update_rejected(s, aep.slot, &uep);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            rs_output_transferred(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT, aep.slot, owner.generation);
            *out_consumed = off;
            return MOQ_OK;
        }
    }

    /* An established/pending SUBSCRIBE_TRACKS publisher-side bidi (keyed
     * MOQ_REQ_SUBSCRIBE_TRACKS after the handoff) carries only a REQUEST_UPDATE
     * (re-prefix), which is deferred (§10.9.1): reject and close the bidi, not the
     * session. Handle it definitively here, before the first-message branches, so
     * a peer cannot reuse the established bidi as a fresh request. */
    {
        moq_request_endpoint_t tep = request_registry_find_by_streamref(s, ref);
        if (tep.kind == MOQ_REQ_SUBSCRIBE_TRACKS) {
            if (env.msg_type != c->type.request_update)
                return close_with_error(s, 0x3,
                    "unexpected message on subscribe-tracks request bidi");
            /* §9.5: an update pipelined behind the request waits for the
             * application's answer to the request itself. */
            if (c->decode_namespace_update &&
                s->track_subs[tep.slot].state == MOQ_TRACK_SUB_PENDING_PUBLISHER &&
                s->track_subs[tep.slot].role == MOQ_TRACK_SUB_ROLE_PUBLISHER)
                return MOQ_ERR_WOULD_BLOCK;
            /* A REQUEST_UPDATE is only valid once the subscription is established
             * on the publisher side; one before the app accepts/rejects (or on
             * the subscriber side) is a protocol violation -- close, no mutation
             * (mirrors the announcement-update guard). */
            if (s->track_subs[tep.slot].state != MOQ_TRACK_SUB_ESTABLISHED ||
                s->track_subs[tep.slot].role != MOQ_TRACK_SUB_ROLE_PUBLISHER)
                return close_with_error(s, 0x3,
                    "REQUEST_UPDATE on non-established subscribe-tracks");
            if (c->decode_namespace_update) {
                rs_output_owner_t owner = rs_output_owner(s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, tep.slot);
                owner.stream->retained_output = MOQ_RETAINED_OUTPUT_RESPONSE;
                rc = s->defer_namespace_updates
                    ? track_sub_on_namespace_update(s, tep.slot, &env)
                    : track_sub_refuse_namespace_update(s, tep.slot, &env);
                if (rc < 0)
                    return rc;
                if (s->state == MOQ_SESS_CLOSED)
                    return MOQ_OK;
                rs_output_transferred(s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, tep.slot, owner.generation);
                *out_consumed = off;
                return MOQ_OK;
            }
            moq_decoded_request_update_t u;
            moq_request_tokens_t tk;
            moq_request_endpoint_t uep;
            rc = rs_decode_admit_update(s, ref, &env, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, true, &u, &tk, &uep);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            rs_output_owner_t owner = rs_output_owner(s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, tep.slot);
            owner.stream->retained_output = MOQ_RETAINED_OUTPUT_RESPONSE;
            rc = session_core_on_track_sub_update_rejected(s, tep.slot, &uep);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            rs_output_transferred(s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, tep.slot, owner.generation);
            *out_consumed = off;
            return MOQ_OK;
        }
    }

    /* An established PUBLISH bidi (keyed MOQ_REQ_PUBLISH after the handoff) is
     * read here only on the subscriber (responder) side: the publisher (opener)
     * sends the terminal PUBLISH_DONE, or a REQUEST_OK acknowledging our pending
     * REQUEST_UPDATE. Handle these definitively before the first-message branches
     * so a peer cannot reuse the established bidi as a fresh request. */
    {
        moq_request_endpoint_t pep = request_registry_find_by_streamref(s, ref);
        if (pep.kind == MOQ_REQ_PUBLISH) {
            moq_pub_entry_t *pe = &s->publishes[pep.slot];
            if (s->profile->request_retains_pending_original_input &&
                env.msg_type == c->type.request_update &&
                pe->state == MOQ_PUB_PENDING_SUBSCRIBER &&
                pe->role == MOQ_PUB_ROLE_SUBSCRIBER)
                return rs_defer_pending_update(s, MOQ_REQUEST_FAMILY_PUBLISH,
                                               buf, len, fin);
            if (env.msg_type == c->type.publish_done) {
                /* §5.1: the publisher may terminate a PUBLISH-initiated
                 * subscription from Pending (Publisher) -- before our
                 * PUBLISH_OK -- as well as from Established. */
                if (pe->state != MOQ_PUB_ESTABLISHED &&
                    pe->state != MOQ_PUB_PENDING_SUBSCRIBER)
                    return close_with_error(s, 0x3,
                        "PUBLISH_DONE on non-active publish");
                moq_request_publish_done_msg_t pd;
                rc = c->decode_publish_done(env.payload, env.payload_len, &pd);
                if (rc < 0)
                    return close_with_error(s, 0x3, "malformed PUBLISH_DONE");
                /* PUBLISH_DONE is terminal: nothing but the FIN may follow. */
                if (off < len)
                    return close_with_error(s, 0x3,
                        "extra bytes after PUBLISH_DONE");
                /* A failed update mandates termination via PUBLISH_DONE(UPDATE_FAILED);
                 * any other status after an update failure is a violation. */
                if (pe->update_failed && pd.status_code != 0x8)
                    return close_with_error(s, 0x3,
                        "PUBLISH_DONE after update failure must be UPDATE_FAILED");
                moq_decoded_publish_done_t d;
                memset(&d, 0, sizeof(d));
                d.target_slot = pep.slot;
                d.status_code = pd.status_code;
                d.stream_count = pd.stream_count;
                d.reason = pd.reason.data;
                d.reason_len = pd.reason.len;
                rc = session_core_on_publish_done(s, &d, false /* drain FIN */);
                if (rc < 0)
                    return rc;
                if (s->state == MOQ_SESS_CLOSED)
                    return MOQ_OK;
                *out_consumed = off;
                return MOQ_OK;
            }
            if (env.msg_type == c->type.request_ok &&
                pe->state == MOQ_PUB_ESTABLISHED && pe->update_pending) {
                /* The publisher acknowledged our REQUEST_UPDATE (stream-correlated,
                 * so no request id); surface PUBLICATION_UPDATE_OK and clear the
                 * pending update. */
                moq_request_update_ok_msg_t up;
                rc = c->decode_update_ok(env.payload, env.payload_len, &up);
                if (rc < 0)
                    return close_with_error(s, 0x3, "malformed REQUEST_OK");
                rc = session_core_on_publish_update_ok(s,
                        (int)(pe - s->publishes),
                        up.has_largest, up.largest_group, up.largest_object,
                        up.has_expires, up.expires_ms);
                if (rc < 0)
                    return rc;   /* WOULD_BLOCK: not consumed, retried */
                if (s->state == MOQ_SESS_CLOSED)
                    return MOQ_OK;
                *out_consumed = off;
                return MOQ_OK;
            }
            if (env.msg_type == c->type.request_error &&
                pe->state == MOQ_PUB_ESTABLISHED && pe->update_pending) {
                /* The publisher rejected our REQUEST_UPDATE (§10.9): clear the
                 * pending update and require the terminating PUBLISH_DONE to carry
                 * UPDATE_FAILED. Further updates are blocked until then. */
                moq_bytes_t rd_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
                moq_request_error_msg_t er;
                moq_decoded_redirect_t rd;
                rc = c->decode_request_error(env.payload, env.payload_len,
                                             rd_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS,
                                             &er, &rd);
                if (rc < 0)
                    return close_with_error(s, 0x3, "malformed REQUEST_ERROR");
                uint64_t failed_update_id = pe->update_request_id;
                pe->update_pending = false;
                pe->update_request_id = 0;
                pe->update_has_forward = false;   /* never acknowledged */
                pe->dt_upd_has_object = false;
                pe->dt_upd_has_subgroup = false;
                pe->update_failed = true;
                /* A fill the refused update carried ends with the refusal. */
                fill_control_resolved(s, fill_parent_pub(s, (size_t)(pe - s->publishes)),
                                      failed_update_id, false, er.error_code,
                                      false, 0, 0, false);
                *out_consumed = off;
                return MOQ_OK;
            }
            /* The publisher's state notification: informative, no response,
             * no request id. Accepted once the publication is established. */
            if (c->type.publish_state_notify != 0 &&
                env.msg_type == c->type.publish_state_notify &&
                pe->state == MOQ_PUB_ESTABLISHED &&
                pe->role == MOQ_PUB_ROLE_SUBSCRIBER) {
                moq_decoded_state_notify_t d;
                rc = c->decode_publish_state_notify(env.payload, env.payload_len, &d);
                if (rc < 0)
                    return rs_request_decode_failed(s, rc, "malformed PUBLISH_STATE_NOTIFY");
                rc = session_core_on_publish_state_notify(s, MOQ_REQ_PUBLISH, pep.slot, &d);
                if (rc < 0)
                    return rc;
                if (s->state == MOQ_SESS_CLOSED)
                    return MOQ_OK;
                *out_consumed = off;
                return MOQ_OK;
            }
            /* The publisher updates its own PUBLISH (the sender of a request
             * may update it); the subscriber answers REQUEST_OK or
             * REQUEST_ERROR and learns the change. Accepted once the
             * publication is established on this side. */
            if (env.msg_type == c->type.request_update &&
                pe->state == MOQ_PUB_ESTABLISHED &&
                pe->role == MOQ_PUB_ROLE_SUBSCRIBER) {
                moq_decoded_request_update_t d;
                moq_request_tokens_t tk;
                moq_request_endpoint_t uep;
                rc = rs_decode_admit_update(s, ref, &env, MOQ_REQUEST_FAMILY_PUBLISH,
                                            false, &d, &tk, &uep);
                if (rc < 0)
                    return rc;
                if (s->state == MOQ_SESS_CLOSED)
                    return MOQ_OK;
                d.endpoint = uep;
                d.target_kind = MOQ_REQ_PUBLISH;
                d.target_slot = pep.slot;
                rc = rs_resolve_tokens(s, &tk, d.tokens, &d.token_count,
                                       d.token_staged, &d.auth_txn, &d.auth_reject_code);
                if (rc < 0)
                    return rc;            /* NOMEM */
                if (s->state == MOQ_SESS_CLOSED)
                    return MOQ_OK;        /* cache overflow / duplicate alias */
                rc = session_core_on_publication_update_by_publisher(s, &d);
                if (rc < 0)
                    return rc;
                if (s->state == MOQ_SESS_CLOSED)
                    return MOQ_OK;
                *out_consumed = off;
                return MOQ_OK;
            }
            return close_with_error(s, 0x3,
                "unexpected message on publish request bidi");
        }
    }

    /* A FETCH request bidi on the publisher side (keyed MOQ_REQ_FETCH after the
     * handoff) carries only the fetcher's REQUEST_UPDATE: AUTHORIZATION_TOKEN and
     * SUBSCRIBER_PRIORITY. Handled definitively here so a peer cannot reuse the
     * bidi as a fresh request. */
    {
        moq_request_endpoint_t fep = request_registry_find_by_streamref(s, ref);
        if (fep.kind == MOQ_REQ_FETCH) {
            if (env.msg_type != c->type.request_update)
                return close_with_error(s, 0x3,
                    "unexpected message on fetch request bidi");
            if (s->fetches[fep.slot].role != MOQ_FETCH_ROLE_PUBLISHER)
                return close_with_error(s, 0x3,
                    "REQUEST_UPDATE from the publisher of a fetch");
            moq_decoded_request_update_t d;
            moq_request_tokens_t tk;
            moq_request_endpoint_t uep;
            rc = rs_decode_admit_update(s, ref, &env, MOQ_REQUEST_FAMILY_FETCH,
                                        true, &d, &tk, &uep);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            d.endpoint = uep;
            d.target_kind = MOQ_REQ_FETCH;
            d.target_slot = fep.slot;
            rc = rs_resolve_tokens(s, &tk, d.tokens, &d.token_count,
                                   d.token_staged, &d.auth_txn, &d.auth_reject_code);
            if (rc < 0)
                return rc;            /* NOMEM */
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            rc = session_core_on_fetch_request_update(s, &d);
            if (rc < 0)
                return rc;
            if (s->state == MOQ_SESS_CLOSED)
                return MOQ_OK;
            *out_consumed = off;
            return MOQ_OK;
        }
    }

    /* A SUBSCRIBE request stream that already carries its request (pending
     * or established) is that request's alone: an initial request is valid
     * only as the first message on a new stream (draft-18 §10, draft-22
     * §9.1), and a fresh Request ID does not make an owned stream new. Only
     * the subscription's REQUEST_UPDATE (below) may follow; anything else is
     * a protocol violation that leaves the subscription as it was. */
    {
        moq_request_endpoint_t sep = request_registry_find_by_streamref(s, ref);
        if (sep.kind == MOQ_REQ_SUBSCRIPTION &&
            s->subs[sep.slot].state != MOQ_SUB_RECVING_REQUEST &&
            env.msg_type != c->type.request_update)
            return close_with_error(s, 0x3,
                "unexpected message on subscribe request bidi");
    }

    /* The first message on a request stream is the request itself. Each
     * branch: decode into the core's record, admit the wire request ID, resolve
     * tokens against the cache, hand to the core. */
    if (env.msg_type == c->type.subscribe) {
        moq_decoded_subscribe_t d;
        moq_request_tokens_t tk;
        uint64_t request_id;
        rc = c->decode_subscribe(env.payload, env.payload_len, &d, &request_id, &tk);
        const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
        if (rc < 0 && !invalid_filter)
            return rs_request_decode_failed(s, rc, "malformed SUBSCRIBE");
        rc = moq_request_stream_validate_inbound(s, ref, env.msg_type,
                                                 request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* validate failed closed (bad request id) */
        /* A refused filter fails the request with INVALID_FILTER through the
         * handler's message-level reject path, surfacing no event. */
        if (invalid_filter) d.auth_reject_code = MOQ_REQUEST_ERROR_INVALID_FILTER;
        rc = rs_resolve_tokens(s, &tk, d.tokens, &d.token_count,
                               d.token_staged, &d.auth_txn, &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */
        rs_refuse_request(s, &d.track_namespace, &d.auth_reject_code);
        rc = session_core_on_subscribe(s, &d, slot, fin);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* core failed/closed (e.g. duplicate) */
        *out_consumed = off;
        return MOQ_OK;
    }

    /* PUBLISH (§10.10): a publisher-initiated subscription. The first message on
     * the bidi; the subscriber (us) replies PUBLISH_OK / REQUEST_ERROR. The core
     * hands the request off from the staging slot to the publishes pool, re-keying
     * the bidi to MOQ_REQ_PUBLISH. */
    if (env.msg_type == c->type.publish) {
        moq_decoded_publish_t d;
        moq_request_tokens_t tk;
        uint64_t request_id;
        rc = c->decode_publish(env.payload, env.payload_len, &d, &request_id, &tk);
        const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
        if (rc < 0 && !invalid_filter)
            return rs_request_decode_failed(s, rc, "malformed PUBLISH");
        rc = moq_request_stream_validate_inbound(s, ref, env.msg_type,
                                                 request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        if (invalid_filter) d.auth_reject_code = MOQ_REQUEST_ERROR_INVALID_FILTER;
        rc = rs_resolve_tokens(s, &tk, d.tokens, &d.token_count,
                               d.token_staged, &d.auth_txn, &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */
        /* `fin` here is the staging owner's observed request FIN, which
         * `handle_request_stream_bytes()` supplies from its retained latch --
         * not merely this transport call's flag. */
        rs_refuse_request(s, &d.track_namespace, &d.auth_reject_code);
        rc = session_core_on_publish(s, &d, fin);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* core failed closed (e.g. duplicate alias) */
        *out_consumed = off;
        return MOQ_OK;
    }

    if (env.msg_type == c->type.fetch) {
        moq_decoded_fetch_t fd;
        moq_request_tokens_t tk;
        uint64_t request_id;
        rc = c->decode_fetch(env.payload, env.payload_len, &fd, &request_id, &tk);
        const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
        if (rc < 0 && !invalid_filter)
            return rs_request_decode_failed(s, rc, "malformed FETCH");
        rc = moq_request_stream_validate_inbound(s, ref, env.msg_type,
                                                 request_id, &fd.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        if (invalid_filter) fd.auth_reject_code = MOQ_REQUEST_ERROR_INVALID_FILTER;
        rc = rs_resolve_tokens(s, &tk, fd.tokens, &fd.token_count,
                               fd.token_staged, &fd.auth_txn, &fd.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */
        fd.request_fin = fin;     /* request bidi FIN'd in this chunk? */
        rs_refuse_request(s, &fd.track_namespace, &fd.auth_reject_code);
        rc = session_core_on_fetch(s, &fd);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* core failed closed (e.g. joining/pool-full) */
        *out_consumed = off;
        return MOQ_OK;
    }

    if (env.msg_type == c->type.publish_namespace) {
        moq_decoded_publish_namespace_t d;
        moq_request_tokens_t tk;
        uint64_t request_id;
        rc = c->decode_publish_namespace(env.payload, env.payload_len, &d,
                                         &request_id, &tk);
        if (rc < 0)
            return rs_request_decode_failed(s, rc, "malformed PUBLISH_NAMESPACE");
        rc = moq_request_stream_validate_inbound(s, ref, env.msg_type,
                                                 request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        rc = rs_resolve_tokens(s, &tk, d.tokens, &d.token_count,
                               d.token_staged, &d.auth_txn, &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */
        /* The staging owner's observed request FIN (see PUBLISH above). */
        rs_refuse_request(s, &d.track_namespace, &d.auth_reject_code);
        rc = session_core_on_publish_namespace(s, &d, fin);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    if (env.msg_type == c->type.track_status) {
        moq_decoded_track_status_request_t d;
        moq_request_tokens_t tk;
        uint64_t request_id;
        rc = c->decode_track_status(env.payload, env.payload_len, &d,
                                    &request_id, &tk);
        if (rc < 0)
            return rs_request_decode_failed(s, rc, "malformed TRACK_STATUS");
        rc = moq_request_stream_validate_inbound(s, ref, env.msg_type,
                                                 request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        rc = rs_resolve_tokens(s, &tk, d.tokens, &d.token_count,
                               d.token_staged, &d.auth_txn, &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */
        rs_refuse_request(s, &d.track_namespace, &d.auth_reject_code);
        rc = session_core_on_track_status_request(s, &d, fin);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        /* TRACK_STATUS is the first and only message, so the requester's FIN
         * rides this chunk. The commit re-keyed the bidi to the new owner; the
         * FIN is handed over and driven through this family's own FIN handling
         * by the request-stream handoff, not recorded here. */
        *out_consumed = off;
        return MOQ_OK;
    }

    if (env.msg_type == c->type.subscribe_tracks) {
        moq_decoded_subscribe_tracks_request_t d;
        moq_request_tokens_t tk;
        uint64_t request_id;
        rc = c->decode_subscribe_tracks(env.payload, env.payload_len, &d,
                                        &request_id, &tk);
        const bool invalid_filter = (rc == MOQ_REQUEST_DECODE_INVALID_FILTER);
        if (rc < 0 && !invalid_filter)
            return rs_request_decode_failed(s, rc, "malformed SUBSCRIBE_TRACKS");
        rc = moq_request_stream_validate_inbound(s, ref, env.msg_type,
                                                 request_id, &d.endpoint);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        if (invalid_filter) d.auth_reject_code = MOQ_REQUEST_ERROR_INVALID_FILTER;
        rc = rs_resolve_tokens(s, &tk, d.tokens, &d.token_count,
                               d.token_staged, &d.auth_txn, &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */
        rs_refuse_request(s, &d.track_namespace_prefix, &d.auth_reject_code);
        rc = session_core_on_subscribe_tracks(s, &d, fin);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    if (env.msg_type == c->type.subscribe_namespace) {
        /* Hand the request off to the namespace-sub pool, then drive the shared
         * ns_sub commit. Buffer the full message into a fresh ns_sub entry (keyed
         * in idx_ns_by_ref, which owns the bidi from here) and process it; the
         * core releases this generic staging slot once the ns_sub entry owns the
         * bidi -- including on a WOULD_BLOCK, which the index-first router sends
         * to the ns_sub path, never back here. The retry is therefore carried by
         * the ns_sub entry, not this staging slot; the idx_ns_by_ref lookup makes
         * the create idempotent. The decode (and any malformed-token close) lives
         * in the ns_sub profile op driven by the shared handler. */
        /* The two pool buffers have the same constructor-bound capacity.
         * D22 transfers the complete retained span, including a pipelined
         * UPDATE; legacy profiles retain their first-message-only contract. */
        size_t msg_len = c->decode_namespace_update ? len : off;
        int32_t ns_slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        if (ns_slot < 0) {
            rc = ns_sub_on_new_bidi(s, ref, buf, msg_len);
            if (rc == MOQ_ERR_BUFFER)
                return close_with_error(s, 0x3,
                    "SUBSCRIBE_NAMESPACE too large for buffer");
            if (rc < 0)
                return rc;            /* WOULD_BLOCK: ns_sub pool full; nothing
                                       * consumed, staging retained for re-feed */
            ns_slot = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        }
        /* Report all transferred bytes even when initial event delivery blocks.
         * The namespace owner, not the freed staging slot, retries them. */
        *out_consumed = msg_len;
        /* The ownership transition is HERE: this entry now owns the bidi and the
         * request bytes, and the core will release the generic staging slot even
         * if processing refuses below. So a FIN that rode the creating
         * SUBSCRIBE_NAMESPACE becomes this entry's transitional ownership NOW --
         * before any event-, scratch- or action-capacity refusal can strand it.
         * It is never written into `pending_fin`, and it is installed only on
         * this request-stream handoff: a control-stream request reaches the
         * shared commit by another route and must not acquire it. */
        if (fin) s->ns_subs[ns_slot].handoff_fin_pending = true;
        rc = ns_sub_process_recving_publisher(s, ns_slot, fin);
        if (rc < 0)
            return rc;                /* WOULD_BLOCK keeps the entry for a re-feed */
        return MOQ_OK;
    }

    /* REQUEST_UPDATE arrives on an established subscription's request bidi (the
     * core passes the committed slot). It consumes a fresh peer request id; the
     * response correlates by the stream, so the target is the slot, not a
     * by-id lookup. It is not a valid first message on a fresh request bidi (a
     * staging slot is still RECVING) -- accept it only once the subscription is
     * established on the publisher side; otherwise it falls through to the
     * unexpected-first-message close below. */
    if (env.msg_type == c->type.request_update &&
        s->subs[slot].state == MOQ_SUB_ESTABLISHED &&
        s->subs[slot].role == MOQ_SUB_ROLE_PUBLISHER) {
        moq_decoded_request_update_t d;
        moq_request_tokens_t tk;
        moq_request_endpoint_t ep;
        rc = rs_decode_admit_update(s, ref, &env, MOQ_REQUEST_FAMILY_SUBSCRIBE, true, &d, &tk, &ep);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        d.endpoint = ep;
        d.target_kind = MOQ_REQ_SUBSCRIPTION;
        d.target_slot = slot;
        rc = rs_resolve_tokens(s, &tk, d.tokens, &d.token_count,
                               d.token_staged, &d.auth_txn, &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */
        rc = session_core_on_request_update(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    return close_with_error(s, 0x3, "unexpected first request message");
}

/* -- Response-stream dispatch --------------------------------------- */

moq_result_t moq_request_stream_process_response(
    moq_session_t *s, moq_stream_ref_t ref, int slot, uint32_t kind,
    const uint8_t *buf, size_t len, bool fin, size_t *out_consumed)
{
    *out_consumed = 0;
    const moq_request_codec_t *c = s->profile->request_codec;
    moq_request_envelope_t env;
    size_t off = 0;
    moq_result_t rc = c->decode_envelope(buf, len, &env, &off);
    if (rc == MOQ_ERR_BUFFER)
        return MOQ_OK;            /* incomplete; the core waits / handles FIN */
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed response envelope");

    /* A GOAWAY on this (committed) request bidi migrates the single request
     * (§10.4). The opener side already has the committed (kind, slot). */
    if (env.msg_type == c->type.goaway) {
        moq_request_family_t fam = rs_family_from_req_kind(kind);
        if (fam == (moq_request_family_t)0)
            return close_with_error(s, 0x3, "GOAWAY on unsupported request kind");
        return moq_request_stream_process_goaway(s, ref, fam, slot, &env, buf, off, len, fin,
                                 out_consumed);
    }

    /* The responder closes a request whose update it refused (§9.5.1):
     * nothing but its FIN may follow that REQUEST_ERROR. */
    if ((kind == (uint32_t)MOQ_REQ_ANNOUNCEMENT &&
         s->announcements[slot].update.failed) ||
        (kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS &&
         s->track_subs[slot].update.failed))
        return close_with_error(s, 0x3,
            "message after a refused namespace update");

    /* REQUEST_ERROR is valid for either request kind; route by kind and state. */
    if (env.msg_type == c->type.request_error) {
        moq_bytes_t rd_parts[MOQ_DECODED_MAX_NAMESPACE_PARTS];
        moq_request_error_msg_t er;
        moq_decoded_redirect_t rd_core;
        rc = c->decode_request_error(env.payload, env.payload_len,
                                     rd_parts, MOQ_DECODED_MAX_NAMESPACE_PARTS,
                                     &er, &rd_core);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_ERROR");
        bool can_retry = er.retry_interval != 0;
        uint64_t retry_ms = retry_interval_to_ms(er.retry_interval);
        /* REDIRECT carries a Redirect tail. Every profile allows it on
         * SUBSCRIBE, FETCH, TRACK_STATUS and PUBLISH_NAMESPACE (SUBSCRIBE_NAMESPACE
         * is on its own response path); PUBLISH and SUBSCRIBE_TRACKS only where
         * the profile says so. Elsewhere, and on an established subscription's
         * update, it is a protocol violation. */
        bool is_redirect = er.is_redirect;
        const moq_decoded_redirect_t *redirect = NULL;
        if (is_redirect) {
            if ((kind == (uint32_t)MOQ_REQ_PUBLISH ||
                 kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS) &&
                !s->profile->redirect_on_any_request)
                return close_with_error(s, 0x3,
                    "REDIRECT on a family that does not permit it");
            if (s->perspective == MOQ_PERSPECTIVE_SERVER &&
                rd_core.connect_uri_len > 0)
                return close_with_error(s, 0x3,
                    "server received REDIRECT with non-zero Connect URI");
            if ((kind == (uint32_t)MOQ_REQ_ANNOUNCEMENT ||
                 kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS) &&
                rd_core.track_name_len > 0)
                return close_with_error(s, 0x3,
                    "REDIRECT Track Name on namespace-scoped request");
            redirect = &rd_core;
        }
        moq_request_family_t family = rs_family_from_req_kind(kind);
        rs_output_owner_t owner = rs_output_owner(s, family, slot);
        if (kind == (uint32_t)MOQ_REQ_FETCH &&
            s->fetches[slot].control_response_seen &&
            s->fetches[slot].own_update_pending) {
            /* The publisher refused this side's update of the fetch. */
            if (is_redirect)
                return close_with_error(s, 0x3, "REDIRECT on a fetch update");
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            rc = session_core_on_fetch_update_error(s, slot, er.error_code,
                er.reason.data, er.reason.len);
        } else if (kind == (uint32_t)MOQ_REQ_FETCH) {
            /* Terminal for a fetch: nothing follows on the bidi. */
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            /* Defer the free: the core drains the request stream's FIN. */
            if (!s->fetches[slot].control_response_seen)
                owner.stream->retained_output = MOQ_RETAINED_OUTPUT_FIN;
            rc = session_core_on_fetch_error(s, slot, er.error_code, can_retry,
                retry_ms, er.reason.data, er.reason.len, false, redirect);
        } else if (kind == (uint32_t)MOQ_REQ_ANNOUNCEMENT &&
                   s->announcements[slot].state == MOQ_ANN_ESTABLISHED &&
                   s->announcements[slot].role == MOQ_ANN_ROLE_ANNOUNCER &&
                   s->announcements[slot].update.outstanding) {
            /* The receiver refused this side's update (§9.5.1): it closes the
             * request, so only the FIN may follow. */
            if (is_redirect)
                return close_with_error(s, 0x3, "REDIRECT on a namespace update");
            if (s->profile->premature_fin_fails_request)
                ann_validate_response(s, slot);
            rc = ann_on_update_response(s, slot, false, er.error_code,
                er.reason.data, er.reason.len, false, 0);
        } else if (kind == (uint32_t)MOQ_REQ_ANNOUNCEMENT) {
            /* Terminal rejection of a pending PUBLISH_NAMESPACE. A REQUEST_ERROR
             * after the announcement is established is not a valid response. */
            if (s->announcements[slot].state != MOQ_ANN_PENDING_ANNOUNCER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR on non-pending announcement");
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            moq_decoded_announcement_error_t d;
            memset(&d, 0, sizeof(d));
            d.target_slot = slot;
            d.error_code = er.error_code;
            d.can_retry = can_retry;
            d.retry_after_ms = retry_ms;
            d.reason = er.reason.data;
            d.reason_len = er.reason.len;
            if (s->profile->premature_fin_fails_request)
                ann_validate_response(s, slot);
            owner.stream->retained_output = MOQ_RETAINED_OUTPUT_FIN;
            rc = session_core_on_announcement_error(s, &d, redirect);
        } else if (kind == (uint32_t)MOQ_REQ_TRACK_STATUS) {
            /* Terminal: a failed TRACK_STATUS; nothing follows but the FIN. */
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            if (s->profile->premature_fin_fails_request) {
                moq_ts_entry_t *ts = &s->track_statuses[slot];
                if (moq_request_lifetime_check_initial_response(&ts->rs.lifetime, false) != MOQ_OK)
                    return close_with_error(s, 0x3, "REQUEST_ERROR on non-pending track-status");
                ts->response_validated = true;
            }
            rc = session_core_on_track_status_error(s, slot, er.error_code,
                can_retry, retry_ms, er.reason.data, er.reason.len,
                redirect);
        } else if (kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS &&
                   s->track_subs[slot].state == MOQ_TRACK_SUB_ESTABLISHED &&
                   s->track_subs[slot].update.outstanding) {
            /* The publisher refused this side's update (§9.5.1): it closes
             * the request, so only the FIN may follow. */
            if (is_redirect)
                return close_with_error(s, 0x3, "REDIRECT on a namespace update");
            rc = track_sub_on_update_response(s, slot, false, er.error_code,
                er.reason.data, er.reason.len, false, 0);
        } else if (kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS) {
            /* Terminal first-response rejection of a pending SUBSCRIBE_TRACKS. */
            if (s->track_subs[slot].state != MOQ_TRACK_SUB_PENDING_SUBSCRIBER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR on non-pending subscribe-tracks");
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            owner.stream->retained_output = MOQ_RETAINED_OUTPUT_FIN;
            rc = session_core_on_subscribe_tracks_error(s, slot, er.error_code,
                can_retry, retry_ms, er.reason.data, er.reason.len,
                redirect);
        } else if (kind == (uint32_t)MOQ_REQ_PUBLISH &&
                   s->publishes[slot].state == MOQ_PUB_ESTABLISHED &&
                   s->publishes[slot].role == MOQ_PUB_ROLE_PUBLISHER &&
                   s->publishes[slot].update_pending) {
            /* The subscriber refused this side's update of its own PUBLISH
             * (§9.5.1): the publisher now owes PUBLISH_DONE(UPDATE_FAILED). */
            if (is_redirect)
                return close_with_error(s, 0x3, "REDIRECT on a publish update");
            rc = session_core_on_own_publish_update_error(s, slot, er.error_code,
                er.reason.data, er.reason.len);
        } else if (kind == (uint32_t)MOQ_REQ_PUBLISH) {
            /* Terminal rejection of a pending outbound PUBLISH. */
            if (s->publishes[slot].state != MOQ_PUB_PENDING_PUBLISHER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR on non-pending publish");
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            /* Defer the free: the core drains the request stream's FIN and closes
             * our send half reciprocally. */
            owner.stream->retained_output = MOQ_RETAINED_OUTPUT_FIN;
            rc = session_core_on_publish_error(s, slot, er.error_code, can_retry,
                retry_ms, er.reason.data, er.reason.len, false, redirect);
        } else if (s->subs[slot].state == MOQ_SUB_ESTABLISHED) {
            /* A REQUEST_ERROR on an established subscription is an update
             * failure: clear the pending update. The publisher must follow with
             * PUBLISH_DONE(UPDATE_FAILED) to terminate the subscription, so more
             * messages may follow -- do not reject trailing bytes here. REDIRECT
             * is not a valid update response. */
            if (is_redirect)
                return close_with_error(s, 0x3,
                    "REDIRECT on a subscription update");
            rc = session_core_on_subscribe_update_error(s, slot, er.error_code);
        } else {
            /* First-response rejection of a pending subscription: terminal. */
            if (off < len)
                return close_with_error(s, 0x3, "extra bytes after REQUEST_ERROR");
            moq_decoded_subscribe_error_t d;
            memset(&d, 0, sizeof(d));
            d.target_slot = slot;
            d.error_code = er.error_code;
            d.can_retry = can_retry;
            d.retry_after_ms = retry_ms;
            d.reason = er.reason.data;
            d.reason_len = er.reason.len;
            owner.stream->retained_output = MOQ_RETAINED_OUTPUT_FIN;
            rc = session_core_on_subscribe_error(s, &d, false, redirect);
        }
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        rs_output_transferred(s, family, slot, owner.generation);
        *out_consumed = off;
        return MOQ_OK;
    }

    /* The publisher's state notification on an established subscription:
     * informative, no response, no request id. */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIPTION &&
        c->type.publish_state_notify != 0 &&
        env.msg_type == c->type.publish_state_notify &&
        s->subs[slot].state == MOQ_SUB_ESTABLISHED) {
        moq_decoded_state_notify_t d;
        rc = c->decode_publish_state_notify(env.payload, env.payload_len, &d);
        if (rc < 0)
            return rs_request_decode_failed(s, rc, "malformed PUBLISH_STATE_NOTIFY");
        rc = session_core_on_publish_state_notify(s, MOQ_REQ_SUBSCRIPTION, slot, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* PUBLISH_DONE terminates an established subscription (publisher -> the
     * subscriber); it is the final message before FIN. §5.1: the publisher
     * terminates only Established or Pending (Publisher) -- PUBLISH-initiated
     * -- subscriptions this way; a pending SUBSCRIBE must receive exactly one
     * SUBSCRIBE_OK or REQUEST_ERROR first, so PUBLISH_DONE is accepted only
     * once the subscription is established. Surface SUBSCRIBE_DONE and keep
     * the entry to drain the FIN. */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIPTION &&
        env.msg_type == c->type.publish_done &&
        s->subs[slot].state == MOQ_SUB_ESTABLISHED) {
        moq_request_publish_done_msg_t pd;
        rc = c->decode_publish_done(env.payload, env.payload_len, &pd);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed PUBLISH_DONE");
        /* A failed update mandates termination via PUBLISH_DONE(UPDATE_FAILED);
         * any other status after an update failure is a protocol violation. */
        if (s->subs[slot].update_failed && pd.status_code != 0x8)
            return close_with_error(s, 0x3,
                "PUBLISH_DONE after update failure must be UPDATE_FAILED");
        moq_decoded_publish_done_t d;
        memset(&d, 0, sizeof(d));
        d.target_slot = slot;
        d.status_code = pd.status_code;
        d.stream_count = pd.stream_count;
        d.reason = pd.reason.data;
        d.reason_len = pd.reason.len;
        rc = session_core_on_subscribe_done(s, slot, &d, false /* drain FIN */);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    if (kind == (uint32_t)MOQ_REQ_SUBSCRIPTION &&
        env.msg_type == c->type.subscribe_ok) {
        moq_decoded_subscribe_ok_t d;
        rc = c->decode_subscribe_ok(env.payload, env.payload_len, &d);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed SUBSCRIBE_OK");
        rc = session_core_on_subscribe_ok(s, &d, slot);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* core failed closed (e.g. duplicate alias) */
        *out_consumed = off;
        return MOQ_OK;
    }

    if (kind == (uint32_t)MOQ_REQ_FETCH &&
        env.msg_type == c->type.fetch_ok) {
        moq_decoded_fetch_ok_t d;
        rc = c->decode_fetch_ok(env.payload, env.payload_len, &d);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed FETCH_OK");
        d.target_slot = slot;
        rc = session_core_on_fetch_ok(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* REQUEST_OK after FETCH_OK acknowledges this side's update of the fetch
     * (REQUEST_UPDATE_OK); it correlates by the stream. */
    if (kind == (uint32_t)MOQ_REQ_FETCH &&
        env.msg_type == c->type.request_ok &&
        s->fetches[slot].control_response_seen &&
        s->fetches[slot].own_update_pending) {
        moq_request_update_ok_msg_t up;
        rc = c->decode_update_ok(env.payload, env.payload_len, &up);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        rc = session_core_on_fetch_update_ok(s, slot);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* REQUEST_OK accepting a pending PUBLISH_NAMESPACE (PUBLISH_NAMESPACE_OK):
     * carries no parameters and empty Track Properties. The announce bidi stays
     * open (ESTABLISHED) until teardown. */
    if (kind == (uint32_t)MOQ_REQ_ANNOUNCEMENT &&
        env.msg_type == c->type.request_ok &&
        s->announcements[slot].state == MOQ_ANN_ESTABLISHED &&
        s->announcements[slot].role == MOQ_ANN_ROLE_ANNOUNCER &&
        s->announcements[slot].update.outstanding) {
        /* REQUEST_UPDATE_OK for this side's update: exactly one per update. */
        moq_request_update_ok_msg_t up;
        rc = c->decode_update_ok(env.payload, env.payload_len, &up);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        if (s->profile->premature_fin_fails_request)
            ann_validate_response(s, slot);
        rc = ann_on_update_response(s, slot, true, 0, NULL, 0,
                                    up.has_expires, up.expires_ms);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    if (kind == (uint32_t)MOQ_REQ_ANNOUNCEMENT &&
        env.msg_type == c->type.request_ok) {
        /* PUBLISH_NAMESPACE_OK is the single first response; a second REQUEST_OK
         * after establishment is a protocol violation. */
        if (s->announcements[slot].state != MOQ_ANN_PENDING_ANNOUNCER)
            return close_with_error(s, 0x3,
                "REQUEST_OK on non-pending announcement");
        rc = c->decode_request_ok(env.payload, env.payload_len);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        if (off < len && !s->profile->premature_fin_fails_request)
            return close_with_error(s, 0x3, "extra bytes after REQUEST_OK");
        moq_decoded_announcement_ok_t d;
        memset(&d, 0, sizeof(d));
        d.target_slot = slot;
        if (s->profile->premature_fin_fails_request)
            ann_validate_response(s, slot);
        rc = session_core_on_announcement_ok(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* TRACK_STATUS_OK: a REQUEST_OK carrying LARGEST_OBJECT / EXPIRES params and
     * a Track Properties tail. Terminal (FIN follows); the core surfaces the
     * status and drains the request bidi. */
    if (kind == (uint32_t)MOQ_REQ_TRACK_STATUS &&
        env.msg_type == c->type.request_ok) {
        if (s->track_statuses[slot].state != MOQ_TS_PENDING_REQUESTER)
            return close_with_error(s, 0x3,
                "REQUEST_OK on non-pending track-status");
        moq_decoded_track_status_ok_t d;
        rc = c->decode_track_status_ok(env.payload, env.payload_len, &d);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed TRACK_STATUS_OK");
        if (off < len)
            return close_with_error(s, 0x3, "extra bytes after TRACK_STATUS_OK");
        d.target_slot = slot;
            if (s->profile->premature_fin_fails_request) {
            moq_ts_entry_t *ts = &s->track_statuses[slot];
            if (moq_request_lifetime_check_initial_response(&ts->rs.lifetime, false) != MOQ_OK)
                return close_with_error(s, 0x3, "REQUEST_OK on ended track-status");
            ts->response_validated = true;
        }
        rc = session_core_on_track_status_ok(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* REQUEST_OK on an established subscription's request bidi acknowledges a
     * pending REQUEST_UPDATE (REQUEST_UPDATE_OK). It correlates by the stream;
     * the core clears the pending-update state. */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIPTION &&
        env.msg_type == c->type.request_ok) {
        moq_request_update_ok_msg_t up;
        rc = c->decode_update_ok(env.payload, env.payload_len, &up);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        rc = session_core_on_subscribe_update_ok(s, slot,
                up.has_largest, up.largest_group, up.largest_object,
                up.has_expires, up.expires_ms);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* REQUEST_UPDATE_OK for this side's update of an established
     * SUBSCRIBE_TRACKS: the barrier after which PUBLISH_SKIPPED names
     * suffixes of the new prefix. */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS &&
        env.msg_type == c->type.request_ok &&
        s->track_subs[slot].state == MOQ_TRACK_SUB_ESTABLISHED &&
        s->track_subs[slot].update.outstanding) {
        moq_request_update_ok_msg_t up;
        rc = c->decode_update_ok(env.payload, env.payload_len, &up);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        rc = track_sub_on_update_response(s, slot, true, 0, NULL, 0,
                                          up.has_expires, up.expires_ms);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* REQUEST_OK accepting a pending SUBSCRIBE_TRACKS: the bidi stays established
     * for PUBLISH_BLOCKED (and future PUBLISH on separate bidis). */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS &&
        env.msg_type == c->type.request_ok) {
        if (s->track_subs[slot].state != MOQ_TRACK_SUB_PENDING_SUBSCRIBER)
            return close_with_error(s, 0x3,
                "REQUEST_OK on non-pending subscribe-tracks");
        rc = c->decode_subscribe_tracks_ok
            ? c->decode_subscribe_tracks_ok(env.payload, env.payload_len)
            : c->decode_request_ok(env.payload, env.payload_len);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        rc = session_core_on_subscribe_tracks_ok(s, slot);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* PUBLISH_BLOCKED on an established SUBSCRIBE_TRACKS response stream. */
    if (kind == (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS &&
        env.msg_type == c->type.publish_blocked) {
        if (s->track_subs[slot].state != MOQ_TRACK_SUB_ESTABLISHED)
            return close_with_error(s, 0x3, "PUBLISH_BLOCKED before REQUEST_OK");
        moq_decoded_publish_blocked_t d;
        rc = c->decode_publish_blocked(env.payload, env.payload_len, &d);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed PUBLISH_BLOCKED");
        d.target_slot = slot;
        rc = session_core_on_publish_blocked(s, slot, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* REQUEST_OK acknowledging this side's update of its own established
     * PUBLISH (REQUEST_UPDATE_OK); it correlates by the stream. */
    if (kind == (uint32_t)MOQ_REQ_PUBLISH &&
        env.msg_type == c->type.request_ok &&
        s->publishes[slot].state == MOQ_PUB_ESTABLISHED &&
        s->publishes[slot].role == MOQ_PUB_ROLE_PUBLISHER &&
        s->publishes[slot].update_pending) {
        moq_request_update_ok_msg_t up;
        rc = c->decode_update_ok(env.payload, env.payload_len, &up);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");
        rc = session_core_on_publish_update_ok(s, slot,
                up.has_largest, up.largest_group, up.largest_object,
                up.has_expires, up.expires_ms);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* PUBLISH_OK: a REQUEST_OK carrying the subscriber's delivery parameters
     * (and empty Track Properties). The single first response to a pending
     * outbound PUBLISH; it establishes the publication. */
    if (kind == (uint32_t)MOQ_REQ_PUBLISH &&
        env.msg_type == c->type.request_ok) {
        if (s->publishes[slot].state != MOQ_PUB_PENDING_PUBLISHER)
            return close_with_error(s, 0x3, "PUBLISH_OK on non-pending publish");
        moq_decoded_publish_ok_t d;
        rc = c->decode_publish_ok(env.payload, env.payload_len, &d);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed PUBLISH_OK");
        d.target_slot = slot;
        rc = session_core_on_publish_ok(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* REQUEST_UPDATE inbound on an established PUBLISH (the subscriber updates
     * the forward state); the publisher (us, the opener) reads it on the bidi's
     * response stream and acknowledges with REQUEST_OK. It consumes a fresh peer
     * request id and correlates by the stream. */
    if (kind == (uint32_t)MOQ_REQ_PUBLISH &&
        env.msg_type == c->type.request_update &&
        s->publishes[slot].state == MOQ_PUB_ESTABLISHED &&
        s->publishes[slot].role == MOQ_PUB_ROLE_PUBLISHER) {
        moq_decoded_request_update_t d;
        moq_request_tokens_t tk;
        moq_request_endpoint_t ep;
        rc = rs_decode_admit_update(s, ref, &env, MOQ_REQUEST_FAMILY_PUBLISH, true, &d, &tk, &ep);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        d.endpoint = ep;
        d.target_kind = MOQ_REQ_PUBLISH;
        d.target_slot = slot;
        rc = rs_resolve_tokens(s, &tk, d.tokens, &d.token_count,
                               d.token_staged, &d.auth_txn, &d.auth_reject_code);
        if (rc < 0)
            return rc;            /* NOMEM */
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;        /* cache overflow / duplicate alias */
        rc = session_core_on_publish_request_update(s, &d);
        if (rc < 0)
            return rc;
        if (s->state == MOQ_SESS_CLOSED)
            return MOQ_OK;
        *out_consumed = off;
        return MOQ_OK;
    }

    /* First-response enforcement: only the response valid for this request
     * kind (or REQUEST_ERROR) may appear. */
    return close_with_error(s, 0x3, "unexpected first response message");
}
