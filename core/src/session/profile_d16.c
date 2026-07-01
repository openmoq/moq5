/*
 * profile_d16.c - Media-over-QUIC draft-16 profile (session<->wire adapter).
 *
 * Implements the moq_profile_ops_t vtable (d16_ops, bottom of file). It is
 * NOT the byte codec (that is core/src/wire/control_d16.c) and NOT the
 * session domain logic (that is core/src/session/session_*.c). It adapts a
 * moq_session_t to the draft-16 wire format.
 *
 * ---- Future split map ----
 * The vtable (d16_ops) is the draft boundary: a second draft (D18/RFC)
 * arrives as sibling profile_d18*.c files and must NOT alter D16 ops
 * semantics. moq_profile_lookup() currently lives at the bottom of this
 * file; once more than one profile exists it may move to a small neutral
 * profile registry/assembly file (so adding D18 does not mean editing D16
 * logic, only the registry).
 *
 * A physical split of THIS file is deliberately deferred until that work
 * begins (the split is internal maintainability, not an evolution
 * blocker). When it happens, keep each profile's ops table as its draft
 * boundary and cut along the regions below. Note the current layout is
 * banded by direction (inbound decode early, outbound encode late) and
 * interleaved by domain, so a physical split must regroup, not just slice:
 *
 *   profile_d16_setup.c      profile state/init, setup/version, GOAWAY
 *   profile_d16_kernel.c     shared message-param validation + auth-token
 *     + profile_d16_internal.h   helpers (the cross-region kernel)
 *   profile_d16_subscribe.c  subscribe / request_update / unsubscribe /
 *                            track_status
 *   profile_d16_namespace.c  publish-namespace + subscribe-namespace (bidi)
 *   profile_d16_fetch.c      fetch family
 *   profile_d16_publish.c    publish family
 *   profile_d16_data.c       subgroup/object/datagram + stream classify
 *   profile_d16_request.c    request admission / track-alias ops
 *   profile_d16_control.c    control dispatch + d16_ops + moq_profile_lookup
 *
 * The "REGION:" banners below mark the major structural bands; the finer
 * "-- D16 ... --" banners label each message/domain section within a band.
 */

#include "session_internal.h"
#include "moq/control.h"

/* ==================== REGION: PROFILE STATE / INIT ==================== */

/* -- D16 profile state --------------------------------------------- */

typedef struct moq_d16_profile_state {
    moq_version_t version;
    uint64_t      next_local_request_id;
    uint64_t      peer_next_request_id;
    uint64_t      requests_blocked_at;
    uint64_t      next_track_alias;
    bool          request_was_blocked;
    bool          send_request_capacity;
    uint64_t      initial_request_capacity;
} moq_d16_profile_state_t;

static void d16_init_in_place(void *state, const moq_session_cfg_t *cfg)
{
    moq_d16_profile_state_t *d16 = (moq_d16_profile_state_t *)state;
    d16->version = MOQ_VERSION_DRAFT_16;

    bool send_req_cap = false;
    uint64_t init_req_cap = 0;
    if (cfg->struct_size >=
        offsetof(moq_session_cfg_t, send_request_capacity) +
        sizeof(cfg->send_request_capacity))
        send_req_cap = cfg->send_request_capacity;
    if (send_req_cap &&
        cfg->struct_size >=
        offsetof(moq_session_cfg_t, initial_request_capacity) +
        sizeof(cfg->initial_request_capacity))
        init_req_cap = cfg->initial_request_capacity;

    d16->send_request_capacity = send_req_cap;
    d16->initial_request_capacity = init_req_cap;

    d16->next_local_request_id =
        (cfg->perspective == MOQ_PERSPECTIVE_CLIENT) ? 0 : 1;
    d16->peer_next_request_id =
        (cfg->perspective == MOQ_PERSPECTIVE_CLIENT) ? 1 : 0;
    d16->requests_blocked_at = UINT64_MAX;
    d16->request_was_blocked = false;
    d16->next_track_alias = 1;
}

static void d16_destroy(void *state)
{
    (void)state;
}

/* ==================== REGION: SETUP / VERSION / GOAWAY ==================== */

/* -- D16 setup parameter processing -------------------------------- */

static uint64_t d16_validate_peer_setup_params(moq_setup_params_t *out,
                                                const moq_kvp_entry_t *params,
                                                size_t count,
                                                bool peer_is_client)
{
    bool seen[8] = {0};

    for (size_t i = 0; i < count; i++) {
        uint64_t type = params[i].type;

        bool is_known = (type == MOQ_SETUP_PARAM_PATH ||
                         type == MOQ_SETUP_PARAM_MAX_REQUEST_ID ||
                         type == MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN ||
                         type == MOQ_SETUP_PARAM_MAX_AUTH_TOKEN_CACHE_SIZE ||
                         type == MOQ_SETUP_PARAM_AUTHORITY ||
                         type == MOQ_SETUP_PARAM_MOQT_IMPLEMENTATION);

        if (is_known) {
            size_t idx = 0;
            if (type == MOQ_SETUP_PARAM_PATH) idx = 0;
            else if (type == MOQ_SETUP_PARAM_MAX_REQUEST_ID) idx = 1;
            else if (type == MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN) idx = 2;
            else if (type == MOQ_SETUP_PARAM_MAX_AUTH_TOKEN_CACHE_SIZE) idx = 3;
            else if (type == MOQ_SETUP_PARAM_AUTHORITY) idx = 4;
            else if (type == MOQ_SETUP_PARAM_MOQT_IMPLEMENTATION) idx = 5;

            if (type != MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN) {
                if (seen[idx]) return 0x3;
                seen[idx] = true;
            }
        }

        switch (type) {
        case MOQ_SETUP_PARAM_PATH:
            if (!peer_is_client) return 0x8;
            out->has_path = true;
            break;
        case MOQ_SETUP_PARAM_AUTHORITY:
            if (!peer_is_client) return 0x19;
            out->has_authority = true;
            break;
        case MOQ_SETUP_PARAM_MAX_REQUEST_ID: {
            if (!params[i].is_varint) return 0x3;
            uint64_t v = 0;
            if (moq_quic_varint_decode(params[i].value,
                                        params[i].value_len, &v) == 0)
                return 0x3;
            out->has_max_request_id = true;
            out->max_request_id = v;
            break;
        }
        case MOQ_SETUP_PARAM_MAX_AUTH_TOKEN_CACHE_SIZE: {
            if (!params[i].is_varint) return 0x3;
            uint64_t v = 0;
            if (moq_quic_varint_decode(params[i].value,
                                        params[i].value_len, &v) == 0)
                return 0x3;
            out->has_max_auth_token_cache_size = true;
            out->max_auth_token_cache_size = v;
            break;
        }
        default:
            break;
        }
    }
    return 0;
}

/* -- D16 setup encode helpers -------------------------------------- */

static void d16_build_local_setup(const moq_session_t *s,
                                   moq_setup_params_t *out)
{
    const moq_d16_profile_state_t *d16 =
        (const moq_d16_profile_state_t *)s->profile_state;
    memset(out, 0, sizeof(*out));
    if (d16->send_request_capacity) {
        out->has_max_request_id = true;
        out->max_request_id = d16->initial_request_capacity;
    }
    if (s->send_auth_token_cache_size) {
        out->has_max_auth_token_cache_size = true;
        out->max_auth_token_cache_size = s->auth_token_cache_size;
    }
}

static moq_result_t d16_encode_and_queue_setup(moq_session_t *s,
                                                const moq_setup_params_t *local,
                                                bool is_client)
{
    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));

    moq_kvp_entry_t params[4];
    size_t param_count = 0;

    uint8_t vbuf[8];
    if (local->has_max_request_id) {
        size_t vlen = moq_quic_varint_encode(local->max_request_id,
                                              vbuf, sizeof(vbuf));
        params[param_count].type      = MOQ_SETUP_PARAM_MAX_REQUEST_ID;
        params[param_count].value     = vbuf;
        params[param_count].value_len = vlen;
        params[param_count].is_varint = true;
        params[param_count].raw       = NULL;
        params[param_count].raw_len   = 0;
        param_count++;
    }

    uint8_t vbuf2[8];
    if (local->has_max_auth_token_cache_size) {
        size_t vlen = moq_quic_varint_encode(local->max_auth_token_cache_size,
                                              vbuf2, sizeof(vbuf2));
        params[param_count].type      = MOQ_SETUP_PARAM_MAX_AUTH_TOKEN_CACHE_SIZE;
        params[param_count].value     = vbuf2;
        params[param_count].value_len = vlen;
        params[param_count].is_varint = true;
        params[param_count].raw       = NULL;
        params[param_count].raw_len   = 0;
        param_count++;
    }

    moq_result_t rc;
    if (is_client)
        rc = moq_d16_encode_client_setup(&w, params, param_count);
    else
        rc = moq_d16_encode_server_setup(&w, params, param_count);
    if (rc < 0) return rc;

    return queue_send_control(s, buf, moq_buf_writer_offset(&w));
}

/* -- D16 setup complete event -------------------------------------- */

static void d16_fill_setup_complete_event(moq_session_t *s, moq_event_t *e)
{
    memset(e, 0, sizeof(*e));
    e->kind = MOQ_EVENT_SETUP_COMPLETE;
    e->detail_size = (uint32_t)sizeof(moq_setup_complete_event_t);
    e->borrow_epoch = s->borrow_epoch;
    e->u.setup_complete.local_perspective = s->perspective;
    e->u.setup_complete.peer_perspective =
        (s->perspective == MOQ_PERSPECTIVE_CLIENT)
            ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
}

/* Cache overlay types and helpers are in session_internal.h / session_auth.c */

/* -- D16 CLIENT_SETUP handler -------------------------------------- */

/*
 * Advisory diagnostic: does a CLIENT_SETUP payload that ALREADY FAILED
 * D16 decode look like a legacy (draft <= 14) version-list SETUP?
 *
 * Draft <= 14 CLIENT_SETUP began with
 *   [Number of Supported Versions (i)][Supported Versions (i)...]
 * where a MoQT draft version identifier is 0xff000000 | draft_number.
 * D16 (draft >= 15) removed the version list and negotiates the version
 * via ALPN, so its parser reads the first varint as a parameter count.
 * A small leading count followed by a 0xff0000xx varint is not a known
 * D16 setup-parameter type and, after the decode has already failed, is
 * a strong legacy version-list signal — it almost always means the peer
 * used a legacy ALPN (e.g. moq-00) instead of moqt-16. (Unknown setup
 * parameters are otherwise ignored by D16, which is why this runs only
 * as a post-failure diagnostic, never as a pre-decode rejection.)
 *
 * Read-only and strictly bounds-checked (moq_buf_* never read past
 * payload_len). Used ONLY to choose a more helpful close reason after
 * the decode has already been rejected; it never changes the
 * accept/reject decision.
 */
static bool d16_setup_looks_like_version_list(const uint8_t *payload,
                                              size_t payload_len)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    uint64_t count = 0;
    if (moq_buf_read_varint(&r, &count) < 0)
        return false;
    /* A plausible Supported-Versions count is small and nonzero. */
    if (count == 0 || count > 16)
        return false;

    uint64_t first = 0;
    if (moq_buf_read_varint(&r, &first) < 0)
        return false;
    /* MoQT draft version identifiers are 0xff000000 | draft_number. */
    return first >= 0xff000000u && first <= 0xff0000ffu;
}

static moq_result_t d16_handle_setup_client(moq_session_t *s,
                                             const moq_control_envelope_t *env)
{
    if (s->state != MOQ_SESS_IDLE)
        return close_with_error(s, 0x3, "unexpected CLIENT_SETUP");
    if (s->perspective != MOQ_PERSPECTIVE_SERVER)
        return close_with_error(s, 0x3, "received CLIENT_SETUP as client");

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_kvp_entry_t params[16];
    moq_d16_setup_t setup = {
        .params = params, .params_count = 0, .params_cap = 16
    };
    moq_result_t rc = moq_d16_decode_client_setup(env->payload,
                                                   env->payload_len, &setup);
    if (rc < 0) {
        if (d16_setup_looks_like_version_list(env->payload,
                                              env->payload_len))
            return close_with_error(s, 0x3,
                "CLIENT_SETUP looks like a legacy version-list SETUP; "
                "D16 negotiates version via ALPN (expected moqt-16)");
        return close_with_error(s, 0x3, "malformed CLIENT_SETUP");
    }

    moq_setup_params_t peer_tmp = {0};
    uint64_t err = d16_validate_peer_setup_params(&peer_tmp, setup.params,
                                                   setup.params_count, true);
    if (err) return close_with_error(s, err, "invalid setup parameter");

    /*
     * Phase 1: Validate and collect auth tokens WITHOUT mutating cache.
     * Cache mutations are deferred to Phase 3 (after all output is
     * committed) to preserve the commit-last invariant.
     */
    moq_cache_overlay_t ov;
    moq_ov_init(&ov, &s->peer_token_cache);
    moq_resolved_token_t resolved[16];
    size_t token_count = 0;

    for (size_t i = 0; i < setup.params_count; i++) {
        if (setup.params[i].type != MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN)
            continue;

        moq_d16_auth_token_t tok;
        moq_result_t trc = moq_d16_auth_token_decode(
            setup.params[i].value, setup.params[i].value_len, &tok);
        if (trc < 0)
            return close_with_error(s, 0x6, "malformed auth token structure");

        if (tok.alias_type == MOQ_AUTH_TOKEN_DELETE ||
            tok.alias_type == MOQ_AUTH_TOKEN_USE_ALIAS)
            return close_with_error(s, 0x3, "DELETE/USE_ALIAS in CLIENT_SETUP");

        /* Well-formed structure but semantically invalid value: there is no
         * request to reject at SETUP time, so this is the
         * MALFORMED_AUTH_TOKEN session error (0x16). */
        if (!moq_auth_token_value_semantically_valid(tok.token_value,
                                                     tok.token_value_len))
            return close_with_error(s, 0x16, "malformed auth token in SETUP");

        if (tok.alias_type == MOQ_AUTH_TOKEN_REGISTER) {
            if (moq_ov_alias_live(&ov, tok.alias, NULL, NULL, NULL))
                return close_with_error(s, 0x14, "duplicate auth token alias");

            if (ov.count < MOQ_CACHE_OVERLAY_CAP) {
                ov.entries[ov.count].kind = MOQ_OV_REGISTER;
                ov.entries[ov.count].alias = tok.alias;
                ov.entries[ov.count].token_type = tok.token_type;
                ov.entries[ov.count].value = tok.token_value;
                ov.entries[ov.count].value_len = tok.token_value_len;
                ov.count++;
            }
        }

        if (token_count < 16) {
            resolved[token_count].token_type = tok.token_type;
            resolved[token_count].token_value.data = tok.token_value;
            resolved[token_count].token_value.len = tok.token_value_len;
            token_count++;
        }
    }

    /*
     * Phase 2: All retryable allocation, WITHOUT observable output. Any
     * failure here returns retryably with no SERVER_SETUP queued, no event
     * pushed, and no cache mutation -- the input stays re-decodable, so a
     * retry produces exactly one SERVER_SETUP / SETUP_COMPLETE (rather than
     * a duplicate stale SERVER_SETUP, which is what queuing it before these
     * allocations risked).
     */
    for (size_t i = 0; i < token_count; i++) {
        if (resolved[i].token_value.len > 0) {
            uint8_t *copy = event_scratch_copy(s, resolved[i].token_value.data,
                                          resolved[i].token_value.len);
            if (!copy) return MOQ_ERR_BUFFER;
            resolved[i].token_value.data = copy;
        }
    }

    moq_event_t e;
    d16_fill_setup_complete_event(s, &e);

    if (token_count > 0) {
        moq_resolved_token_t *tok_copy = (moq_resolved_token_t *)event_scratch_alloc_aligned(
            s, token_count * sizeof(moq_resolved_token_t), _Alignof(moq_resolved_token_t));
        if (!tok_copy) return MOQ_ERR_BUFFER;
        memcpy(tok_copy, resolved, token_count * sizeof(moq_resolved_token_t));
        e.u.setup_complete.tokens = tok_copy;
        e.u.setup_complete.token_count = token_count;
    }

    for (size_t i = 0; i < ov.count; i++) {
        if (ov.entries[i].kind != MOQ_OV_REGISTER) continue;
        if (ov.entries[i].value_len > 0) {
            uint8_t *p = (uint8_t *)s->alloc.alloc(ov.entries[i].value_len,
                                                     s->alloc.ctx);
            if (!p) {
                moq_ov_free_preowned(&ov, &s->alloc);
                return MOQ_ERR_NOMEM;
            }
            memcpy(p, ov.entries[i].value, ov.entries[i].value_len);
            ov.entries[i].preowned = p;
        }
    }

    /*
     * Phase 3: Commit. All retryable allocation has succeeded and the event
     * slot was reserved at entry (the event_queue_full check above, with
     * nothing pushing an event before here). The only operation that can
     * still fail is queuing SERVER_SETUP on a full action queue, and it is
     * the FIRST observable mutation -- a WOULD_BLOCK there leaves nothing
     * queued/pushed/committed, so the retry is clean. push_event cannot
     * fail (slot reserved), and the cache commit allocates nothing.
     */
    moq_setup_params_t local_tmp;
    d16_build_local_setup(s, &local_tmp);

    rc = d16_encode_and_queue_setup(s, &local_tmp, false);
    if (rc < 0) {
        moq_ov_free_preowned(&ov, &s->alloc);
        return rc;
    }

    rc = push_event(s, &e);
    if (rc < 0) {
        /* Unreachable while the entry event_queue_full check remains a true
         * reservation (nothing pushes an event before here). If this ever trips,
         * the session is already in an internal inconsistency after SERVER_SETUP
         * was queued; return the error rather than committing cache/state. */
        moq_ov_free_preowned(&ov, &s->alloc);
        return rc;
    }

    /*
     * OVERFLOW is silently downgraded per D16 CLIENT_SETUP spec.
     * DUPLICATE at commit is unreachable (validated in Phase 1) but
     * handled defensively rather than crashing a production server.
     */
    for (size_t i = 0; i < ov.count; i++) {
        if (ov.entries[i].kind != MOQ_OV_REGISTER) continue;
        int rrc = moq_token_cache_register_preowned(&s->peer_token_cache,
            ov.entries[i].alias, ov.entries[i].token_type,
            ov.entries[i].preowned, ov.entries[i].value_len);
        if (rrc == MOQ_TOKEN_OK) {
            ov.entries[i].preowned = NULL;
        } else if (rrc == MOQ_TOKEN_ERR_OVERFLOW) {
            s->alloc.free(ov.entries[i].preowned, ov.entries[i].value_len,
                          s->alloc.ctx);
            ov.entries[i].preowned = NULL;
        } else {
            moq_ov_free_preowned(&ov, &s->alloc);
            return close_with_error(s, 0x1, "internal: overlay/cache mismatch");
        }
    }

    s->peer_setup = peer_tmp;
    s->local_setup = local_tmp;
    s->state = MOQ_SESS_ESTABLISHED;
    return MOQ_OK;
}

/* -- D16 SERVER_SETUP handler -------------------------------------- */

static moq_result_t d16_handle_setup_server(moq_session_t *s,
                                             const moq_control_envelope_t *env)
{
    if (s->state != MOQ_SESS_SETUP_SENT)
        return close_with_error(s, 0x3, "unexpected SERVER_SETUP");
    if (s->perspective != MOQ_PERSPECTIVE_CLIENT)
        return close_with_error(s, 0x3, "received SERVER_SETUP as server");

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_kvp_entry_t params[16];
    moq_d16_setup_t setup = {
        .params = params, .params_count = 0, .params_cap = 16
    };
    moq_result_t rc = moq_d16_decode_server_setup(env->payload,
                                                   env->payload_len, &setup);
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed SERVER_SETUP");

    moq_setup_params_t peer_tmp = {0};
    uint64_t err = d16_validate_peer_setup_params(&peer_tmp, setup.params,
                                                   setup.params_count, false);
    if (err) {
        const char *reason = (err == 0x8)  ? "server sent PATH" :
                             (err == 0x19) ? "server sent AUTHORITY" :
                                             "invalid setup parameter";
        return close_with_error(s, err, reason);
    }

    /*
     * Phase 1: Validate and collect auth tokens WITHOUT mutating cache.
     * REGISTER/DELETE are staged in the overlay; USE_ALIAS resolves
     * from the overlay (which models sequential cache effects).
     */
    moq_cache_overlay_t ov;
    moq_ov_init(&ov, &s->peer_token_cache);
    moq_resolved_token_t resolved[16];
    size_t token_count = 0;

    for (size_t i = 0; i < setup.params_count; i++) {
        if (setup.params[i].type != MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN)
            continue;

        moq_d16_auth_token_t tok;
        moq_result_t trc = moq_d16_auth_token_decode(
            setup.params[i].value, setup.params[i].value_len, &tok);
        if (trc < 0)
            return close_with_error(s, 0x6, "malformed auth token structure");

        if (tok.alias_type == MOQ_AUTH_TOKEN_REGISTER) {
            if (moq_ov_alias_live(&ov, tok.alias, NULL, NULL, NULL))
                return close_with_error(s, 0x14, "duplicate auth token alias");

            size_t projected_bytes, projected_active;
            moq_ov_project(&ov, &projected_bytes, &projected_active);
            size_t entry_size = MOQ_TOKEN_ENTRY_OVERHEAD + tok.token_value_len;
            if (entry_size < MOQ_TOKEN_ENTRY_OVERHEAD ||
                projected_bytes > s->peer_token_cache.max_bytes ||
                entry_size > s->peer_token_cache.max_bytes - projected_bytes ||
                projected_active >= s->peer_token_cache.cap)
                return close_with_error(s, 0x13, "auth token cache overflow");

            if (ov.count < MOQ_CACHE_OVERLAY_CAP) {
                ov.entries[ov.count].kind = MOQ_OV_REGISTER;
                ov.entries[ov.count].alias = tok.alias;
                ov.entries[ov.count].token_type = tok.token_type;
                ov.entries[ov.count].value = tok.token_value;
                ov.entries[ov.count].value_len = tok.token_value_len;
                ov.count++;
            }
        } else if (tok.alias_type == MOQ_AUTH_TOKEN_DELETE) {
            if (!moq_ov_alias_live(&ov, tok.alias, NULL, NULL, NULL))
                return close_with_error(s, 0x3, "DELETE unknown auth token alias");
            if (ov.count < MOQ_CACHE_OVERLAY_CAP) {
                ov.entries[ov.count].kind = MOQ_OV_DELETE;
                ov.entries[ov.count].alias = tok.alias;
                ov.entries[ov.count].token_type = 0;
                ov.entries[ov.count].value = NULL;
                ov.entries[ov.count].value_len = 0;
                ov.count++;
            }
        } else if (tok.alias_type == MOQ_AUTH_TOKEN_USE_ALIAS) {
            uint64_t rtype = 0;
            const uint8_t *rval = NULL;
            size_t rlen = 0;
            if (!moq_ov_alias_live(&ov, tok.alias, &rtype, &rval, &rlen))
                return close_with_error(s, 0x3,
                    "USE_ALIAS unknown auth token alias");

            if (!moq_auth_token_value_semantically_valid(rval, rlen))
                return close_with_error(s, 0x16,
                    "malformed auth token in SETUP");

            if (token_count < 16) {
                resolved[token_count].token_type = rtype;
                resolved[token_count].token_value.data = rval;
                resolved[token_count].token_value.len = rlen;
                token_count++;
            }
            continue;
        }

        if (tok.alias_type == MOQ_AUTH_TOKEN_DELETE) continue;

        /* REGISTER / USE_VALUE carry a literal value: well-formed but
         * semantically invalid closes with MALFORMED_AUTH_TOKEN (0x16) --
         * there is no request to reject at SETUP time. */
        if (!moq_auth_token_value_semantically_valid(tok.token_value,
                                                     tok.token_value_len))
            return close_with_error(s, 0x16, "malformed auth token in SETUP");

        if (token_count < 16) {
            resolved[token_count].token_type = tok.token_type;
            resolved[token_count].token_value.data = tok.token_value;
            resolved[token_count].token_value.len = tok.token_value_len;
            token_count++;
        }
    }

    /*
     * Phase 2: All output and allocation. DELETE is deferred, so cache
     * pointers from USE_ALIAS are still valid during scratch_copy.
     * Pre-allocate cache-value copies for pending REGISTERs so Phase 3
     * commits without allocation.
     */
    for (size_t i = 0; i < token_count; i++) {
        if (resolved[i].token_value.len > 0) {
            uint8_t *copy = event_scratch_copy(s, resolved[i].token_value.data,
                                          resolved[i].token_value.len);
            if (!copy) return MOQ_ERR_BUFFER;
            resolved[i].token_value.data = copy;
        }
    }

    moq_event_t e;
    d16_fill_setup_complete_event(s, &e);

    if (token_count > 0) {
        moq_resolved_token_t *tok_copy = (moq_resolved_token_t *)event_scratch_alloc_aligned(
            s, token_count * sizeof(moq_resolved_token_t), _Alignof(moq_resolved_token_t));
        if (!tok_copy) return MOQ_ERR_BUFFER;
        memcpy(tok_copy, resolved, token_count * sizeof(moq_resolved_token_t));
        e.u.setup_complete.tokens = tok_copy;
        e.u.setup_complete.token_count = token_count;
    }

    for (size_t i = 0; i < ov.count; i++) {
        if (ov.entries[i].kind != MOQ_OV_REGISTER) continue;
        if (ov.entries[i].value_len > 0) {
            uint8_t *p = (uint8_t *)s->alloc.alloc(ov.entries[i].value_len,
                                                     s->alloc.ctx);
            if (!p) {
                moq_ov_free_preowned(&ov, &s->alloc);
                return MOQ_ERR_NOMEM;
            }
            memcpy(p, ov.entries[i].value, ov.entries[i].value_len);
            ov.entries[i].preowned = p;
        }
    }

    rc = push_event(s, &e);
    if (rc < 0) {
        moq_ov_free_preowned(&ov, &s->alloc);
        return rc;
    }

    /*
     * Phase 3: Commit. All output is queued and all cache-value buffers
     * are pre-allocated. No allocation can fail here.
     * DUPLICATE/OVERFLOW/UNKNOWN at commit is an internal invariant failure.
     */
    for (size_t i = 0; i < ov.count; i++) {
        if (ov.entries[i].kind == MOQ_OV_REGISTER) {
            int rrc = moq_token_cache_register_preowned(&s->peer_token_cache,
                ov.entries[i].alias, ov.entries[i].token_type,
                ov.entries[i].preowned, ov.entries[i].value_len);
            if (rrc == MOQ_TOKEN_OK) {
                ov.entries[i].preowned = NULL;
            } else {
                moq_ov_free_preowned(&ov, &s->alloc);
                return close_with_error(s, 0x1, "internal: overlay/cache mismatch");
            }
        } else {
            int drc = moq_token_cache_delete(&s->peer_token_cache,
                                              ov.entries[i].alias);
            if (drc == MOQ_TOKEN_ERR_UNKNOWN) {
                moq_ov_free_preowned(&ov, &s->alloc);
                return close_with_error(s, 0x1, "internal: overlay/cache mismatch");
            }
        }
    }

    s->peer_setup = peer_tmp;
    s->state = MOQ_SESS_ESTABLISHED;
    return MOQ_OK;
}

/* -- D16 profile ops: start ---------------------------------------- */

static moq_result_t d16_start(moq_session_t *s)
{
    if (s->perspective != MOQ_PERSPECTIVE_CLIENT)
        return MOQ_ERR_WRONG_STATE;
    if (s->state != MOQ_SESS_IDLE)
        return MOQ_ERR_WRONG_STATE;

    moq_setup_params_t local_tmp;
    d16_build_local_setup(s, &local_tmp);

    moq_result_t rc = d16_encode_and_queue_setup(s, &local_tmp, true);
    if (rc < 0) return rc;

    s->local_setup = local_tmp;
    s->state = MOQ_SESS_SETUP_SENT;
    return MOQ_OK;
}

/* -- D16 GOAWAY handler -------------------------------------------- */

static moq_result_t d16_handle_goaway(moq_session_t *s,
                                       const moq_control_envelope_t *env)
{
    if (!session_is_active(s))
        return close_with_error(s, 0x3, "GOAWAY before ESTABLISHED");

    if (s->goaway_received)
        return close_with_error(s, 0x3, "duplicate GOAWAY");

    moq_d16_goaway_t goaway;
    moq_result_t rc = moq_d16_decode_goaway(env->payload,
                                             env->payload_len, &goaway);
    if (rc < 0)
        return close_with_error(s, 0x3, "malformed GOAWAY");

    return session_core_on_goaway(s, goaway.uri, goaway.uri_len);
}

/* ==================== REGION: SHARED MESSAGE-PARAM VALIDATION KERNEL ==================== */
/*
 * Cross-region D16 kernel: d16_validate_msg_params / d16_param_is_allowed
 * / d16_is_defined_msg_param are called by nearly every control decoder
 * (subscribe, fetch, publish, namespace, track_status, ...). On a future
 * physical split these move to profile_d16_kernel.c with declarations in
 * profile_d16_internal.h (and become non-static). Keep them dependency-free
 * of any single domain so that move stays mechanical.
 */

/* -- D16 message parameter validation ------------------------------ */

static bool d16_is_defined_msg_param(uint64_t type)
{
    switch (type) {
    case MOQ_MSG_PARAM_AUTHORIZATION_TOKEN:
    case MOQ_MSG_PARAM_DELIVERY_TIMEOUT:
    case MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY:
    case MOQ_MSG_PARAM_GROUP_ORDER:
    case MOQ_MSG_PARAM_SUBSCRIPTION_FILTER:
    case MOQ_MSG_PARAM_FORWARD:
    case MOQ_MSG_PARAM_NEW_GROUP_REQUEST:
    case MOQ_MSG_PARAM_EXPIRES:
    case MOQ_MSG_PARAM_LARGEST_OBJECT:
        return true;
    default:
        return false;
    }
}

static bool d16_param_is_allowed(uint64_t type, const uint64_t *allowed,
                                  size_t num_allowed)
{
    for (size_t i = 0; i < num_allowed; i++)
        if (type == allowed[i]) return true;
    return false;
}

/*
 * D16 message parameter validation.
 *
 * Draft-16 rules:
 * - Unknown param type (not defined in D16) → close with 0x3.
 * - Defined but not allowed for this message → ignore (skip).
 * - Allowed for this message → check duplicates and values.
 *
 * Returns 0 on success, or a session-close error code.
 * After this call, params that are defined but not in allowed_types
 * are still in the array but should be skipped during value decoding.
 */
static uint64_t d16_validate_msg_params(const moq_kvp_entry_t *params,
                                         size_t count,
                                         const uint64_t *allowed_types,
                                         size_t num_allowed)
{
    bool seen[16] = {0};

    for (size_t i = 0; i < count; i++) {
        uint64_t type = params[i].type;

        if (!d16_is_defined_msg_param(type))
            return 0x3;

        bool allowed = false;
        size_t idx = 0;
        for (size_t j = 0; j < num_allowed; j++) {
            if (type == allowed_types[j]) {
                allowed = true;
                idx = j;
                break;
            }
        }

        if (!allowed) continue;

        if (type != MOQ_MSG_PARAM_AUTHORIZATION_TOKEN) {
            if (idx < 16 && seen[idx])
                return 0x3;
            if (idx < 16) seen[idx] = true;
        }
    }
    return 0;
}

/* ==================== REGION: INBOUND CONTROL DECODE ==================== */
/*
 * Inbound decode band. Domains are interleaved here (subscribe,
 * request_update, publish-namespace, fetch, publish, unsubscribe,
 * track_status, ...) and each is labelled by its own "-- D16 inbound X
 * decode --" banner. The matching OUTBOUND ENCODE ops live in the encode
 * band further down; a physical split regroups decode+encode per domain.
 */

/* -- D16 inbound SUBSCRIBE decode ---------------------------------- */

/* Allowed message params for SUBSCRIBE. */
static const uint64_t d16_subscribe_allowed_params[] = {
    MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
    MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
    MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
    MOQ_MSG_PARAM_GROUP_ORDER,
    MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
    MOQ_MSG_PARAM_FORWARD,
    MOQ_MSG_PARAM_NEW_GROUP_REQUEST,
};

/*
 * Decode an inbound SUBSCRIBE wire message into a semantic struct.
 * Handles: wire decode, request ID validation, param validation,
 * auth token processing (including reject path), and param value decode.
 *
 * On success: returns MOQ_OK with *out_consumed == false.  The decoded
 *   struct is fully populated and ready for session_core_on_subscribe.
 * On auth reject: sends REQUEST_ERROR, commits request+auth, returns
 *   MOQ_OK with *out_consumed == true.  Session core must NOT be called.
 * On session close: closes the session, returns MOQ_OK with
 *   *out_consumed == true (close_with_error returns MOQ_OK).
 * On internal error: returns < 0, *out_consumed == true.
 *
 * The dispatch must check both the return value and *out_consumed.
 */
/*
 * Auth-token helper kernel (decode side): d16_extract_auth_tokens is
 * shared across the subscribe/fetch/publish/namespace decoders. With the
 * outbound auth-token helpers below, these form the auth-token kernel that
 * moves to profile_d16_kernel.c on a future split.
 */
/* -- D16 auth token extraction from KVP ---------------------------- */

static moq_result_t d16_extract_auth_tokens(
    const moq_kvp_entry_t *params, size_t params_count,
    moq_decoded_auth_token_t *out, size_t out_cap, size_t *out_count)
{
    size_t count = 0;
    for (size_t i = 0; i < params_count; i++) {
        if (params[i].type != MOQ_MSG_PARAM_AUTHORIZATION_TOKEN)
            continue;
        moq_d16_auth_token_t tok;
        moq_result_t rc = moq_d16_auth_token_decode(
            params[i].value, params[i].value_len, &tok);
        if (rc < 0) return rc;
        if (count < out_cap) {
            switch (tok.alias_type) {
            case MOQ_AUTH_TOKEN_DELETE:    out[count].op = MOQ_AUTH_OP_DELETE; break;
            case MOQ_AUTH_TOKEN_REGISTER:  out[count].op = MOQ_AUTH_OP_REGISTER; break;
            case MOQ_AUTH_TOKEN_USE_ALIAS: out[count].op = MOQ_AUTH_OP_USE_ALIAS; break;
            case MOQ_AUTH_TOKEN_USE_VALUE: out[count].op = MOQ_AUTH_OP_USE_VALUE; break;
            default: return MOQ_ERR_PROTO;
            }
            out[count].alias = tok.alias;
            out[count].token_type = tok.token_type;
            out[count].token_value = tok.token_value;
            out[count].token_value_len = tok.token_value_len;
            count++;
        }
    }
    *out_count = count;
    return MOQ_OK;
}

static moq_result_t d16_decode_subscribe(moq_session_t *s,
                                          const moq_control_envelope_t *env,
                                          moq_decoded_subscribe_t *out,
                                          bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->subscriber_priority = 128;
    out->group_order = MOQ_GROUP_ORDER_DEFAULT;
    out->forward = true;

    /*
     * close_with_error() returns MOQ_OK, so we must set *out_consumed
     * before any close path to prevent the dispatch from falling through
     * to session_core_on_subscribe on a closed session.
     */
    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "SUBSCRIBE before ESTABLISHED");
    }
    /* After our GOAWAY (§10.4) the peer must not open new requests. */
    if (session_refuses_new_requests(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "SUBSCRIBE after local GOAWAY");
    }

    /* Wire decode. */
    moq_bytes_t ns_parts[32];
    moq_kvp_entry_t params[16];
    moq_d16_subscribe_t sub = {
        .params = params, .params_cap = 16,
    };
    moq_result_t rc = moq_d16_decode_subscribe(env->payload, env->payload_len,
                                                ns_parts, 32, &sub);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed SUBSCRIBE");
    }

    if (sub.track_namespace.count > 0)
        memcpy(out->track_namespace_parts, sub.track_namespace.parts,
            sub.track_namespace.count * sizeof(moq_bytes_t));
    out->track_namespace.parts = out->track_namespace_parts;
    out->track_namespace.count = sub.track_namespace.count;
    out->track_name = sub.track_name;
    out->request_id = sub.request_id;

    {
        moq_result_t vrc = s->profile->validate_inbound_request(
            s, sub.request_id, &out->endpoint);
        if (vrc < 0 || s->state == MOQ_SESS_CLOSED) {
            *out_consumed = true;
            return vrc;
        }
    }

    /* Message parameter validation. */
    uint64_t perr = d16_validate_msg_params(sub.params, sub.params_count,
        d16_subscribe_allowed_params,
        sizeof(d16_subscribe_allowed_params) / sizeof(d16_subscribe_allowed_params[0]));
    if (perr) {
        *out_consumed = true;
        return close_with_error(s, perr, "invalid SUBSCRIBE parameter");
    }

    /* Extract and decode AUTH_TOKEN params, then process via staged txn. */
    moq_decoded_auth_token_t auth_in[MOQ_DECODED_MAX_TOKENS];
    size_t auth_in_count = 0;
    rc = d16_extract_auth_tokens(sub.params, sub.params_count,
        auth_in, MOQ_DECODED_MAX_TOKENS, &auth_in_count);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x6, "malformed auth token structure");
    }

    out->token_count = 0;
    uint64_t auth_reject_code = 0;

    rc = process_auth_tokens(s, auth_in, auth_in_count,
        out->tokens, &out->token_count, MOQ_DECODED_MAX_TOKENS,
        &auth_reject_code, out->token_staged,
        &out->auth_txn);
    if (rc < 0) {
        *out_consumed = true;
        return rc;
    }
    /* process_auth_tokens may close the session (returning MOQ_OK) for
     * cache overflow, duplicate alias, or malformed token structure.
     * In those cases the session is already closed; signal consumed. */
    if (s->state == MOQ_SESS_CLOSED) {
        *out_consumed = true;
        return MOQ_OK;
    }

    if (auth_reject_code) {
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = moq_d16_encode_request_error(&ew, sub.request_id,
            auth_reject_code, 0, NULL, 0);
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            return rc;
        }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            return rc;
        }
        s->profile->commit_inbound_request(s, &out->endpoint);
        process_auth_tokens_commit_txn(s, &out->auth_txn);
        *out_consumed = true;
        return MOQ_OK;
    }

    /* Parse parameter values. Malformed param → session close.
     * close_with_error returns MOQ_OK, so set consumed to signal
     * the dispatch that the subscribe was handled (by closing). */
#define D16_PARAM_CLOSE(reason) do { \
    process_auth_tokens_free_staging(s, out->tokens, out->token_staged, \
        out->token_count); \
    process_auth_tokens_abort_txn(s, &out->auth_txn); \
    *out_consumed = true; \
    return close_with_error(s, 0x3, (reason)); \
} while (0)

    for (size_t i = 0; i < sub.params_count; i++) {
        switch (sub.params[i].type) {
        case MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY: {
            uint8_t v;
            if (moq_d16_decode_param_subscriber_priority(
                    sub.params[i].value, sub.params[i].value_len, &v) < 0)
                D16_PARAM_CLOSE("malformed SUBSCRIBER_PRIORITY");
            out->subscriber_priority = v;
            break;
        }
        case MOQ_MSG_PARAM_GROUP_ORDER: {
            uint8_t v;
            if (moq_d16_decode_param_group_order(
                    sub.params[i].value, sub.params[i].value_len, &v) < 0)
                D16_PARAM_CLOSE("malformed GROUP_ORDER");
            out->group_order = v;
            break;
        }
        case MOQ_MSG_PARAM_FORWARD: {
            bool v;
            if (moq_d16_decode_param_forward(
                    sub.params[i].value, sub.params[i].value_len, &v) < 0)
                D16_PARAM_CLOSE("malformed FORWARD");
            out->has_forward = true;
            out->forward = v;
            break;
        }
        case MOQ_MSG_PARAM_SUBSCRIPTION_FILTER: {
            moq_d16_subscription_filter_t f;
            if (moq_d16_decode_subscription_filter(
                    sub.params[i].value, sub.params[i].value_len, &f) < 0)
                D16_PARAM_CLOSE("malformed subscription filter");
            out->has_filter = true;
            out->filter_type = f.filter_type;
            out->start_group = f.start_group;
            out->start_object = f.start_object;
            out->end_group = f.end_group;
            break;
        }
        case MOQ_MSG_PARAM_DELIVERY_TIMEOUT: {
            uint64_t v;
            if (moq_d16_decode_param_expires(
                    sub.params[i].value, sub.params[i].value_len, &v) < 0)
                D16_PARAM_CLOSE("malformed DELIVERY_TIMEOUT");
            if (v == 0)
                D16_PARAM_CLOSE("DELIVERY_TIMEOUT must be > 0");
            out->has_delivery_timeout = true;
            out->delivery_timeout_us = v * 1000;
            break;
        }
        case MOQ_MSG_PARAM_NEW_GROUP_REQUEST: {
            uint64_t v = 0;
            size_t consumed = moq_quic_varint_decode(sub.params[i].value,
                sub.params[i].value_len, &v);
            if (consumed == 0 || consumed != sub.params[i].value_len)
                D16_PARAM_CLOSE("malformed NEW_GROUP_REQUEST");
            out->has_new_group_request = true;
            out->new_group_request = v;
            break;
        }
        default:
            break;
        }
    }

#undef D16_PARAM_CLOSE

    return MOQ_OK;
}

/* Lenient Track Extensions scan for DYNAMIC_GROUPS (0x30, §11.1.1.3):
 * extract the value when the blob is readable, enforce the 0/1 rule
 * (value > 1 MUST close PROTOCOL_VIOLATION), and bail out silently on any
 * structural inconsistency -- this is NOT full extension validation; the
 * blob stays opaque to the application either way. Returns MOQ_ERR_PROTO
 * only for the >1 violation. */
#define D16_EXT_DYNAMIC_GROUPS 0x30u
static moq_result_t d16_scan_extensions_dynamic_groups(
    const uint8_t *data, size_t len, bool *out_dynamic_groups)
{
    *out_dynamic_groups = false;
    if (!data || len == 0) return MOQ_OK;
    moq_kvp_decoder_t dec;
    moq_kvp_decoder_init(&dec, data, len);
    for (;;) {
        moq_kvp_entry_t e;
        moq_result_t rc = moq_kvp_decode_next(&dec, &e);
        if (rc == MOQ_DONE) return MOQ_OK;
        if (rc < 0) return MOQ_OK;             /* lenient: stop, stay opaque */
        if (e.type != D16_EXT_DYNAMIC_GROUPS) continue;
        uint64_t v = 0;
        {
            size_t n = moq_quic_varint_decode(e.value, e.value_len, &v);
            if (n == 0 || n != e.value_len) return MOQ_OK;   /* lenient */
        }
        if (v > 1) return MOQ_ERR_PROTO;       /* §11.1.1.3: 0/1 only */
        if (v == 1) *out_dynamic_groups = true;
    }
}

/* -- D16 inbound SUBSCRIBE_OK decode ------------------------------- */

static const uint64_t d16_subscribe_ok_allowed_params[] = {
    MOQ_MSG_PARAM_LARGEST_OBJECT,
    MOQ_MSG_PARAM_EXPIRES,
};

/*
 * Decode SUBSCRIBE_OK wire message. Validates structure and params.
 * Param VALUES are decoded eagerly, but malformed-param errors are
 * deferred (has_deferred_param_error) so the session handler can
 * check event_queue_full before closing — preserving current ordering
 * where WOULD_BLOCK takes precedence over malformed-param close.
 */
static moq_result_t d16_decode_subscribe_ok(moq_session_t *s,
                                             const moq_control_envelope_t *env,
                                             moq_decoded_subscribe_ok_t *out,
                                             bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "SUBSCRIBE_OK before ESTABLISHED");
    }

    moq_kvp_entry_t params[8];
    moq_d16_subscribe_ok_t ok = {
        .params = params, .params_cap = 8,
    };
    moq_result_t rc = moq_d16_decode_subscribe_ok(env->payload,
                                                    env->payload_len, &ok);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed SUBSCRIBE_OK");
    }

    out->request_id = ok.request_id;
    out->track_alias = ok.track_alias;
    out->track_properties = ok.track_extensions;
    out->track_properties_len = ok.track_extensions_len;
    if (d16_scan_extensions_dynamic_groups(ok.track_extensions,
            ok.track_extensions_len, &out->dynamic_groups) < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "DYNAMIC_GROUPS value above 1");
    }

    if (unsub_tomb_consume(s, ok.request_id)) {
        *out_consumed = true;
        return MOQ_OK;
    }

    int slot = sub_find_by_request_id(s, ok.request_id);
    if (slot < 0 || s->subs[slot].state != MOQ_SUB_PENDING_SUBSCRIBER) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "SUBSCRIBE_OK for unknown request");
    }

    uint64_t perr = d16_validate_msg_params(ok.params, ok.params_count,
        d16_subscribe_ok_allowed_params,
        sizeof(d16_subscribe_ok_allowed_params) /
        sizeof(d16_subscribe_ok_allowed_params[0]));
    if (perr) {
        *out_consumed = true;
        return close_with_error(s, perr, "invalid SUBSCRIBE_OK parameter");
    }

    if (sub_track_alias_in_use(s, ok.track_alias) ||
        pub_track_alias_in_use(s, ok.track_alias)) {
        *out_consumed = true;
        return close_with_error(s, 0x5, "duplicate track alias");
    }

    /* Decode param values eagerly but defer errors. */
    for (size_t i = 0; i < ok.params_count; i++) {
        if (ok.params[i].type == MOQ_MSG_PARAM_LARGEST_OBJECT) {
            moq_d16_location_t loc;
            if (moq_d16_decode_location(ok.params[i].value,
                                         ok.params[i].value_len, &loc) < 0) {
                out->has_deferred_param_error = true;
                out->deferred_param_reason = "malformed LARGEST_OBJECT";
                break;
            }
            out->has_largest = true;
            out->largest_group = loc.group;
            out->largest_object = loc.object;
        } else if (ok.params[i].type == MOQ_MSG_PARAM_EXPIRES) {
            uint64_t v;
            if (moq_d16_decode_param_expires(ok.params[i].value,
                                              ok.params[i].value_len, &v) < 0) {
                out->has_deferred_param_error = true;
                out->deferred_param_reason = "malformed EXPIRES";
                break;
            }
            out->has_expires = (v > 0);
            out->expires_ms = v;
        }
    }

    return MOQ_OK;
}


/* -- D16 inbound REQUEST_UPDATE decode ----------------------------- */

static const uint64_t d16_request_update_all_params[] = {
    MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
    MOQ_MSG_PARAM_FORWARD,
    MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
    MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
    MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
    MOQ_MSG_PARAM_NEW_GROUP_REQUEST,
};

static const uint64_t d16_request_update_supported[] = {
    MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
    MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
    MOQ_MSG_PARAM_FORWARD,
    MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
    MOQ_MSG_PARAM_NEW_GROUP_REQUEST,
};

static moq_result_t d16_decode_request_update(moq_session_t *s,
                                               const moq_control_envelope_t *env,
                                               moq_decoded_request_update_t *out,
                                               bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "REQUEST_UPDATE before ESTABLISHED");
    }

    moq_kvp_entry_t params[16];
    moq_d16_request_update_t upd = {
        .params = params, .params_count = 0, .params_cap = 16
    };
    moq_result_t rc = moq_d16_decode_request_update(env->payload,
                                                     env->payload_len, &upd);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed REQUEST_UPDATE");
    }

    out->request_id = upd.request_id;

    {
        moq_result_t vrc = s->profile->validate_inbound_request(
            s, upd.request_id, &out->endpoint);
        if (vrc < 0 || s->state == MOQ_SESS_CLOSED) {
            *out_consumed = true;
            return vrc;
        }
    }

    moq_request_endpoint_t target_ep = request_registry_find_by_id(
        s, upd.existing_request_id);
    if (target_ep.kind == MOQ_REQ_NONE) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "REQUEST_UPDATE unknown existing ID");
    }
    out->target_kind = target_ep.kind;
    out->target_slot = target_ep.slot;

    if (target_ep.kind == MOQ_REQ_SUBSCRIPTION) {
        moq_sub_entry_t *e = &s->subs[target_ep.slot];
        if (e->role != MOQ_SUB_ROLE_PUBLISHER || e->state != MOQ_SUB_ESTABLISHED) {
            *out_consumed = true;
            return close_with_error(s, 0x3,
                "REQUEST_UPDATE for non-established publisher sub");
        }
    } else if (target_ep.kind == MOQ_REQ_PUBLISH) {
        moq_pub_entry_t *e = &s->publishes[target_ep.slot];
        if (e->role != MOQ_PUB_ROLE_PUBLISHER || e->state != MOQ_PUB_ESTABLISHED) {
            *out_consumed = true;
            return close_with_error(s, 0x3,
                "REQUEST_UPDATE for non-established publisher pub");
        }
    } else {
        /* Non-subscription/publish targets (FETCH, TRACK_STATUS,
         * NAMESPACE_SUB, ANNOUNCEMENT) are not yet supported for
         * REQUEST_UPDATE. Mark as unsupported so the session handler
         * responds with REQUEST_ERROR(NOT_SUPPORTED). */
        out->has_unsupported = true;
    }

    /* Select allowed params based on target kind per D16 Section 9.2.2.
     * Subscription: full set. FETCH: AUTH_TOKEN + SUBSCRIBER_PRIORITY.
     * Others: AUTH_TOKEN only. */
    static const uint64_t d16_request_update_fetch_params[] = {
        MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
        MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
    };
    static const uint64_t d16_request_update_generic_params[] = {
        MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
    };
    const uint64_t *allowed = d16_request_update_all_params;
    size_t num_allowed = sizeof(d16_request_update_all_params) /
                         sizeof(d16_request_update_all_params[0]);
    if (target_ep.kind == MOQ_REQ_FETCH) {
        allowed = d16_request_update_fetch_params;
        num_allowed = sizeof(d16_request_update_fetch_params) /
                      sizeof(d16_request_update_fetch_params[0]);
    } else if (target_ep.kind != MOQ_REQ_SUBSCRIPTION &&
               target_ep.kind != MOQ_REQ_PUBLISH) {
        allowed = d16_request_update_generic_params;
        num_allowed = sizeof(d16_request_update_generic_params) /
                      sizeof(d16_request_update_generic_params[0]);
    }

    uint64_t perr = d16_validate_msg_params(upd.params, upd.params_count,
        allowed, num_allowed);
    if (perr) {
        *out_consumed = true;
        return close_with_error(s, perr, "invalid REQUEST_UPDATE parameter");
    }

    /* Check for unsupported-but-valid params (skip wrong-scope). */
    for (size_t i = 0; i < upd.params_count; i++) {
        if (!d16_param_is_allowed(upd.params[i].type, allowed, num_allowed))
            continue;
        if (!d16_param_is_allowed(upd.params[i].type,
                d16_request_update_supported,
                sizeof(d16_request_update_supported) /
                sizeof(d16_request_update_supported[0])))
            { out->has_unsupported = true; break; }
    }

    /* Decode supported param values (skip wrong-scope). */
    for (size_t i = 0; i < upd.params_count; i++) {
        if (!d16_param_is_allowed(upd.params[i].type, allowed, num_allowed))
            continue;
        if (upd.params[i].type == MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY) {
            uint8_t v;
            if (moq_d16_decode_param_subscriber_priority(
                    upd.params[i].value, upd.params[i].value_len, &v) < 0) {
                *out_consumed = true;
                return close_with_error(s, 0x3,
                    "malformed SUBSCRIBER_PRIORITY in REQUEST_UPDATE");
            }
            out->has_subscriber_priority = true;
            out->subscriber_priority = v;
        } else if (upd.params[i].type == MOQ_MSG_PARAM_FORWARD) {
            bool v;
            if (moq_d16_decode_param_forward(
                    upd.params[i].value, upd.params[i].value_len, &v) < 0) {
                *out_consumed = true;
                return close_with_error(s, 0x3,
                    "malformed FORWARD in REQUEST_UPDATE");
            }
            out->has_forward = true;
            out->forward = v;
        } else if (upd.params[i].type == MOQ_MSG_PARAM_DELIVERY_TIMEOUT) {
            uint64_t v;
            if (moq_d16_decode_param_expires(
                    upd.params[i].value, upd.params[i].value_len, &v) < 0) {
                *out_consumed = true;
                return close_with_error(s, 0x3,
                    "malformed DELIVERY_TIMEOUT in REQUEST_UPDATE");
            }
            if (v == 0) {
                *out_consumed = true;
                return close_with_error(s, 0x3,
                    "DELIVERY_TIMEOUT must be > 0 in REQUEST_UPDATE");
            }
            out->has_delivery_timeout = true;
            out->delivery_timeout_us = v * 1000;
        } else if (upd.params[i].type == MOQ_MSG_PARAM_NEW_GROUP_REQUEST) {
            uint64_t v;
            size_t consumed = moq_quic_varint_decode(
                upd.params[i].value, upd.params[i].value_len, &v);
            if (consumed == 0 || consumed != upd.params[i].value_len) {
                *out_consumed = true;
                return close_with_error(s, 0x3,
                    "malformed NEW_GROUP_REQUEST in REQUEST_UPDATE");
            }
            out->has_new_group_request = true;
            out->new_group_request = v;
        }
    }

    /* Extract and resolve AUTHORIZATION_TOKEN params through the shared
     * staged-transaction path (the same machinery as SUBSCRIBE); the
     * draft-neutral update handler commits/aborts the txn and surfaces the
     * resolved tokens (or rejects on auth_reject_code). Wrong-scope token
     * params were already screened by the allowed-set validation above. */
    {
        moq_decoded_auth_token_t auth_in[MOQ_DECODED_MAX_TOKENS];
        size_t auth_in_count = 0;
        rc = d16_extract_auth_tokens(upd.params, upd.params_count,
            auth_in, MOQ_DECODED_MAX_TOKENS, &auth_in_count);
        if (rc < 0) {
            *out_consumed = true;
            return close_with_error(s, 0x6, "malformed auth token structure");
        }
        rc = process_auth_tokens(s, auth_in, auth_in_count,
            out->tokens, &out->token_count, MOQ_DECODED_MAX_TOKENS,
            &out->auth_reject_code, out->token_staged,
            &out->auth_txn);
        if (rc < 0) {
            *out_consumed = true;
            return rc;
        }
        if (s->state == MOQ_SESS_CLOSED) {
            *out_consumed = true;
            return MOQ_OK;
        }
    }

    return MOQ_OK;
}

/* -- D16 inbound PUBLISH_NAMESPACE decode -------------------------- */

static const uint64_t d16_publish_namespace_allowed_params[] = {
    MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
};

static moq_result_t d16_decode_publish_namespace(
    moq_session_t *s,
    const moq_control_envelope_t *env,
    moq_decoded_publish_namespace_t *out,
    bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_NAMESPACE before ESTABLISHED");
    }
    /* After our GOAWAY (§10.4) the peer must not open new requests. */
    if (session_refuses_new_requests(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_NAMESPACE after local GOAWAY");
    }

    moq_bytes_t ns_parts[32];
    moq_kvp_entry_t params[16];
    moq_d16_publish_namespace_t pn = {
        .params = params, .params_cap = 16,
    };
    moq_result_t rc = moq_d16_decode_publish_namespace(env->payload,
                                                        env->payload_len,
                                                        ns_parts, 32, &pn);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed PUBLISH_NAMESPACE");
    }

    if (pn.track_namespace.count == 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "empty PUBLISH_NAMESPACE namespace");
    }

    {
        moq_result_t vrc = s->profile->validate_inbound_request(
            s, pn.request_id, &out->endpoint);
        if (vrc < 0 || s->state == MOQ_SESS_CLOSED) {
            *out_consumed = true;
            return vrc;
        }
    }

    /* Param validation. */
    uint64_t perr = d16_validate_msg_params(pn.params, pn.params_count,
        d16_publish_namespace_allowed_params,
        sizeof(d16_publish_namespace_allowed_params) /
        sizeof(d16_publish_namespace_allowed_params[0]));
    if (perr) {
        *out_consumed = true;
        return close_with_error(s, perr, "unknown PUBLISH_NAMESPACE parameter");
    }

    if (pn.track_namespace.count > 0)
        memcpy(out->track_namespace_parts, pn.track_namespace.parts,
            pn.track_namespace.count * sizeof(moq_bytes_t));
    out->track_namespace.parts = out->track_namespace_parts;
    out->track_namespace.count = pn.track_namespace.count;
    out->request_id = pn.request_id;

    /* Extract and decode AUTH_TOKEN, then process via staged txn. */
    moq_decoded_auth_token_t auth_in[MOQ_DECODED_MAX_TOKENS];
    size_t auth_in_count = 0;
    rc = d16_extract_auth_tokens(pn.params, pn.params_count,
        auth_in, MOQ_DECODED_MAX_TOKENS, &auth_in_count);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x6, "malformed auth token structure");
    }

    uint64_t auth_reject_code = 0;
    rc = process_auth_tokens(s, auth_in, auth_in_count,
        out->tokens, &out->token_count, MOQ_DECODED_MAX_TOKENS,
        &auth_reject_code, out->token_staged, &out->auth_txn);
    if (rc < 0) { *out_consumed = true; return rc; }

    if (s->state == MOQ_SESS_CLOSED) {
        *out_consumed = true;
        return MOQ_OK;
    }

    if (auth_reject_code) {
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = moq_d16_encode_request_error(&ew, pn.request_id,
            auth_reject_code, 0, NULL, 0);
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            *out_consumed = true;
            return rc;
        }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            *out_consumed = true;
            return rc;
        }
        s->profile->commit_inbound_request(s, &out->endpoint);
        process_auth_tokens_commit_txn(s, &out->auth_txn);
        *out_consumed = true;
        return MOQ_OK;
    }

    return MOQ_OK;
}

/* -- D16 inbound FETCH decode ------------------------------------- */

static const uint64_t d16_fetch_allowed_params[] = {
    MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
    MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
    MOQ_MSG_PARAM_GROUP_ORDER,
};

static moq_result_t d16_decode_fetch(moq_session_t *s,
                                      const moq_control_envelope_t *env,
                                      moq_decoded_fetch_t *out,
                                      bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->subscriber_priority = 128;
    out->group_order = MOQ_GROUP_ORDER_ASCENDING;
    out->joining_sub_slot = -1;

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "FETCH before ESTABLISHED");
    }
    /* After our GOAWAY (§10.4) the peer must not open new requests. */
    if (session_refuses_new_requests(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "FETCH after local GOAWAY");
    }

    moq_bytes_t ns_parts[32];
    moq_kvp_entry_t params[16];
    moq_d16_fetch_t fetch = { .params = params, .params_cap = 16 };

    moq_result_t rc = moq_d16_decode_fetch(env->payload, env->payload_len,
                                            ns_parts, 32, &fetch);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed FETCH");
    }

    out->request_id = fetch.request_id;
    out->fetch_type = fetch.fetch_type;
    if (fetch.track_namespace.count > 0)
        memcpy(out->track_namespace_parts, fetch.track_namespace.parts,
            fetch.track_namespace.count * sizeof(moq_bytes_t));
    out->track_namespace.parts = out->track_namespace_parts;
    out->track_namespace.count = fetch.track_namespace.count;
    out->track_name = fetch.track_name;
    out->start_group = fetch.start_group;
    out->start_object = fetch.start_object;
    out->end_group = fetch.end_group;
    out->end_object = fetch.end_object;
    out->joining_request_id = fetch.joining_request_id;
    out->joining_start = fetch.joining_start;

    {
        moq_result_t vrc = s->profile->validate_inbound_request(
            s, fetch.request_id, &out->endpoint);
        if (vrc < 0 || s->state == MOQ_SESS_CLOSED) {
            *out_consumed = true;
            return vrc;
        }
    }

    uint64_t perr = d16_validate_msg_params(fetch.params, fetch.params_count,
        d16_fetch_allowed_params,
        sizeof(d16_fetch_allowed_params) / sizeof(d16_fetch_allowed_params[0]));
    if (perr) {
        *out_consumed = true;
        return close_with_error(s, perr, "invalid FETCH parameter");
    }

    moq_decoded_auth_token_t auth_in[MOQ_DECODED_MAX_TOKENS];
    size_t auth_in_count = 0;
    rc = d16_extract_auth_tokens(fetch.params, fetch.params_count,
        auth_in, MOQ_DECODED_MAX_TOKENS, &auth_in_count);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x6, "malformed auth token structure");
    }

    out->token_count = 0;
    uint64_t auth_reject_code = 0;
    rc = process_auth_tokens(s, auth_in, auth_in_count,
        out->tokens, &out->token_count, MOQ_DECODED_MAX_TOKENS,
        &auth_reject_code, out->token_staged, &out->auth_txn);
    if (rc < 0) { *out_consumed = true; return rc; }
    if (s->state == MOQ_SESS_CLOSED) { *out_consumed = true; return MOQ_OK; }

    if (auth_reject_code) {
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = moq_d16_encode_request_error(&ew, fetch.request_id,
            auth_reject_code, 0, NULL, 0);
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            *out_consumed = true;
            return rc;
        }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            *out_consumed = true;
            return rc;
        }
        s->profile->commit_inbound_request(s, &out->endpoint);
        process_auth_tokens_commit_txn(s, &out->auth_txn);
        *out_consumed = true;
        return MOQ_OK;
    }

#define D16_FETCH_PARAM_CLOSE(reason) do { \
    process_auth_tokens_free_staging(s, out->tokens, out->token_staged, \
        out->token_count); \
    process_auth_tokens_abort_txn(s, &out->auth_txn); \
    *out_consumed = true; \
    return close_with_error(s, 0x3, (reason)); \
} while (0)

    for (size_t i = 0; i < fetch.params_count; i++) {
        switch (fetch.params[i].type) {
        case MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY: {
            uint8_t pri = 0;
            if (moq_d16_decode_param_subscriber_priority(
                    fetch.params[i].value, fetch.params[i].value_len,
                    &pri) < 0)
                D16_FETCH_PARAM_CLOSE("malformed SUBSCRIBER_PRIORITY");
            out->subscriber_priority = pri;
            break;
        }
        case MOQ_MSG_PARAM_GROUP_ORDER: {
            uint8_t go = 0;
            if (moq_d16_decode_param_group_order(
                    fetch.params[i].value, fetch.params[i].value_len,
                    &go) < 0)
                D16_FETCH_PARAM_CLOSE("malformed GROUP_ORDER");
            out->group_order = go;
            break;
        }
        case MOQ_MSG_PARAM_AUTHORIZATION_TOKEN:
            break;
        default:
            break;
        }
    }
#undef D16_FETCH_PARAM_CLOSE

    return MOQ_OK;
}

/* -- D16 inbound FETCH_CANCEL decode ------------------------------ */

static moq_result_t d16_decode_fetch_cancel(
    moq_session_t *s,
    const moq_control_envelope_t *env,
    moq_decoded_fetch_cancel_t *out,
    bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->target_slot = -1;

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "FETCH_CANCEL before ESTABLISHED");
    }

    uint64_t request_id = 0;
    moq_result_t rc = moq_d16_decode_varint_msg(env->payload,
                                                 env->payload_len,
                                                 &request_id);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed FETCH_CANCEL");
    }

    bool peer_is_client = (s->perspective == MOQ_PERSPECTIVE_SERVER);
    uint64_t expected_parity = peer_is_client ? 0 : 1;
    if ((request_id & 1) != expected_parity) {
        *out_consumed = true;
        return close_with_error(s, 0x4, "FETCH_CANCEL wrong parity");
    }

    moq_request_endpoint_t ep = request_registry_find_by_id(s, request_id);
    if (ep.kind != MOQ_REQ_FETCH) {
        *out_consumed = true;
        return close_with_error(s, 0x4, "FETCH_CANCEL unknown request ID");
    }

    moq_fetch_entry_t *e = &s->fetches[ep.slot];
    if (e->role != MOQ_FETCH_ROLE_PUBLISHER) {
        *out_consumed = true;
        return close_with_error(s, 0x4,
            "FETCH_CANCEL for non-publisher fetch");
    }
    if (e->state != MOQ_FETCH_PENDING_PUBLISHER &&
        e->state != MOQ_FETCH_ACCEPTED) {
        *out_consumed = true;
        return close_with_error(s, 0x4,
            "FETCH_CANCEL for inactive fetch");
    }

    out->request_id = request_id;
    out->target_slot = ep.slot;
    return MOQ_OK;
}

/* -- D16 inbound PUBLISH decode ----------------------------------- */

static const uint64_t d16_publish_allowed_params[] = {
    MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
    MOQ_MSG_PARAM_EXPIRES,
    MOQ_MSG_PARAM_LARGEST_OBJECT,
    MOQ_MSG_PARAM_FORWARD,
};

static moq_result_t d16_decode_publish(moq_session_t *s,
                                        const moq_control_envelope_t *env,
                                        moq_decoded_publish_t *out,
                                        bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->forward = true;

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH before ESTABLISHED");
    }
    /* After our GOAWAY (§10.4) the peer must not open new requests. */
    if (session_refuses_new_requests(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH after local GOAWAY");
    }

    /* Wire decode. */
    moq_bytes_t ns_parts[32];
    moq_kvp_entry_t params[16];
    moq_d16_publish_t pub = {
        .params = params, .params_cap = 16,
    };
    moq_result_t rc = moq_d16_decode_publish(env->payload, env->payload_len,
                                              ns_parts, 32, &pub);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed PUBLISH");
    }

    if (pub.track_namespace.count > 0)
        memcpy(out->track_namespace_parts, pub.track_namespace.parts,
            pub.track_namespace.count * sizeof(moq_bytes_t));
    out->track_namespace.parts = out->track_namespace_parts;
    out->track_namespace.count = pub.track_namespace.count;
    out->track_name = pub.track_name;
    out->track_alias = pub.track_alias;
    out->request_id = pub.request_id;
    out->track_properties = pub.track_extensions;
    out->track_properties_len = pub.track_extensions_len;
    if (d16_scan_extensions_dynamic_groups(pub.track_extensions,
            pub.track_extensions_len, &out->dynamic_groups) < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "DYNAMIC_GROUPS value above 1");
    }

    {
        moq_result_t vrc = s->profile->validate_inbound_request(
            s, pub.request_id, &out->endpoint);
        if (vrc < 0 || s->state == MOQ_SESS_CLOSED) {
            *out_consumed = true;
            return vrc;
        }
    }

    /* Message parameter validation. */
    uint64_t perr = d16_validate_msg_params(pub.params, pub.params_count,
        d16_publish_allowed_params,
        sizeof(d16_publish_allowed_params) / sizeof(d16_publish_allowed_params[0]));
    if (perr) {
        *out_consumed = true;
        return close_with_error(s, perr, "invalid PUBLISH parameter");
    }

    /* Extract and decode AUTH_TOKEN params, then process via staged txn. */
    moq_decoded_auth_token_t auth_in[MOQ_DECODED_MAX_TOKENS];
    size_t auth_in_count = 0;
    rc = d16_extract_auth_tokens(pub.params, pub.params_count,
        auth_in, MOQ_DECODED_MAX_TOKENS, &auth_in_count);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x6, "malformed auth token structure");
    }

    out->token_count = 0;
    uint64_t auth_reject_code = 0;

    rc = process_auth_tokens(s, auth_in, auth_in_count,
        out->tokens, &out->token_count, MOQ_DECODED_MAX_TOKENS,
        &auth_reject_code, out->token_staged,
        &out->auth_txn);
    if (rc < 0) {
        *out_consumed = true;
        return rc;
    }
    if (s->state == MOQ_SESS_CLOSED) {
        *out_consumed = true;
        return MOQ_OK;
    }

    if (auth_reject_code) {
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = moq_d16_encode_request_error(&ew, pub.request_id,
            auth_reject_code, 0, NULL, 0);
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            return rc;
        }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            return rc;
        }
        s->profile->commit_inbound_request(s, &out->endpoint);
        process_auth_tokens_commit_txn(s, &out->auth_txn);
        *out_consumed = true;
        return MOQ_OK;
    }

#define D16_PUB_PARAM_CLOSE(reason) do { \
    process_auth_tokens_free_staging(s, out->tokens, out->token_staged, \
        out->token_count); \
    process_auth_tokens_abort_txn(s, &out->auth_txn); \
    *out_consumed = true; \
    return close_with_error(s, 0x3, (reason)); \
} while (0)

    for (size_t i = 0; i < pub.params_count; i++) {
        switch (pub.params[i].type) {
        case MOQ_MSG_PARAM_FORWARD: {
            bool v;
            if (moq_d16_decode_param_forward(
                    pub.params[i].value, pub.params[i].value_len, &v) < 0)
                D16_PUB_PARAM_CLOSE("malformed FORWARD");
            out->has_forward = true;
            out->forward = v;
            break;
        }
        case MOQ_MSG_PARAM_EXPIRES: {
            uint64_t exp = 0;
            if (moq_d16_decode_param_expires(
                    pub.params[i].value, pub.params[i].value_len, &exp) < 0)
                D16_PUB_PARAM_CLOSE("malformed EXPIRES");
            break;
        }
        case MOQ_MSG_PARAM_LARGEST_OBJECT: {
            moq_d16_location_t loc;
            if (moq_d16_decode_location(
                    pub.params[i].value, pub.params[i].value_len, &loc) < 0)
                D16_PUB_PARAM_CLOSE("malformed LARGEST_OBJECT");
            break;
        }
        case MOQ_MSG_PARAM_AUTHORIZATION_TOKEN:
            /* Handled via d16_extract_auth_tokens above. */
            break;
        default:
            break;
        }
    }

#undef D16_PUB_PARAM_CLOSE

    return MOQ_OK;
}

/* -- D16 inbound PUBLISH_OK decode -------------------------------- */

static moq_result_t d16_decode_publish_ok_inbound(
    moq_session_t *s,
    const moq_control_envelope_t *env,
    moq_decoded_publish_ok_t *out,
    bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->target_slot = -1;
    out->forward = true;
    out->subscriber_priority = 128;

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_OK before ESTABLISHED");
    }

    static const uint64_t d16_publish_ok_allowed_params[] = {
        MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
        MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
        MOQ_MSG_PARAM_GROUP_ORDER,
        MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
        MOQ_MSG_PARAM_EXPIRES,
        MOQ_MSG_PARAM_FORWARD,
        MOQ_MSG_PARAM_NEW_GROUP_REQUEST,
    };

    moq_kvp_entry_t ok_params[8];
    moq_d16_publish_ok_t ok = { .params = ok_params, .params_cap = 8 };
    moq_result_t rc = moq_d16_decode_publish_ok(env->payload,
                                                  env->payload_len, &ok);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed PUBLISH_OK");
    }

    moq_request_endpoint_t ep = request_registry_find_by_id(s, ok.request_id);
    if (ep.kind != MOQ_REQ_PUBLISH) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_OK for unknown request");
    }

    moq_pub_entry_t *pe = &s->publishes[ep.slot];
    if (pe->role != MOQ_PUB_ROLE_PUBLISHER) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_OK for non-publisher publish");
    }
    if (pe->state != MOQ_PUB_PENDING_PUBLISHER) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_OK for non-pending publish");
    }

    uint64_t perr = d16_validate_msg_params(ok.params, ok.params_count,
        d16_publish_ok_allowed_params,
        sizeof(d16_publish_ok_allowed_params) /
        sizeof(d16_publish_ok_allowed_params[0]));
    if (perr) {
        *out_consumed = true;
        return close_with_error(s, perr, "invalid PUBLISH_OK parameter");
    }

    for (size_t i = 0; i < ok.params_count; i++) {
        switch (ok.params[i].type) {
        case MOQ_MSG_PARAM_DELIVERY_TIMEOUT: {
            uint64_t v = 0;
            size_t consumed = moq_quic_varint_decode(ok.params[i].value,
                ok.params[i].value_len, &v);
            if (consumed == 0 || consumed != ok.params[i].value_len || v == 0) {
                out->has_deferred_param_error = true;
                out->deferred_param_reason = "malformed DELIVERY_TIMEOUT";
            } else {
                out->has_delivery_timeout = true;
                out->delivery_timeout_ms = v;
            }
            break;
        }
        case MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY: {
            uint8_t v;
            if (moq_d16_decode_param_subscriber_priority(ok.params[i].value,
                    ok.params[i].value_len, &v) < 0) {
                out->has_deferred_param_error = true;
                out->deferred_param_reason = "malformed SUBSCRIBER_PRIORITY";
            } else {
                out->subscriber_priority = v;
            }
            break;
        }
        case MOQ_MSG_PARAM_GROUP_ORDER: {
            uint8_t v;
            if (moq_d16_decode_param_group_order(ok.params[i].value,
                    ok.params[i].value_len, &v) < 0) {
                out->has_deferred_param_error = true;
                out->deferred_param_reason = "malformed GROUP_ORDER";
            } else {
                out->group_order = v;
            }
            break;
        }
        case MOQ_MSG_PARAM_SUBSCRIPTION_FILTER: {
            moq_d16_subscription_filter_t f;
            if (moq_d16_decode_subscription_filter(ok.params[i].value,
                    ok.params[i].value_len, &f) < 0) {
                out->has_deferred_param_error = true;
                out->deferred_param_reason = "malformed SUBSCRIPTION_FILTER";
            }
            break;
        }
        case MOQ_MSG_PARAM_EXPIRES: {
            uint64_t v;
            if (moq_d16_decode_param_expires(ok.params[i].value,
                    ok.params[i].value_len, &v) < 0) {
                out->has_deferred_param_error = true;
                out->deferred_param_reason = "malformed EXPIRES";
            } else {
                out->has_expires = (v > 0);
                out->expires_ms = v;
            }
            break;
        }
        case MOQ_MSG_PARAM_FORWARD: {
            bool v;
            if (moq_d16_decode_param_forward(ok.params[i].value,
                    ok.params[i].value_len, &v) < 0) {
                out->has_deferred_param_error = true;
                out->deferred_param_reason = "malformed FORWARD";
            } else {
                out->forward = v;
            }
            break;
        }
        case MOQ_MSG_PARAM_NEW_GROUP_REQUEST: {
            uint64_t v = 0;
            size_t consumed = moq_quic_varint_decode(ok.params[i].value,
                ok.params[i].value_len, &v);
            if (consumed == 0 || consumed != ok.params[i].value_len) {
                out->has_deferred_param_error = true;
                out->deferred_param_reason = "malformed NEW_GROUP_REQUEST";
            } else {
                out->has_new_group_request = true;
                out->new_group_request = v;
            }
            break;
        }
        default:
            break;
        }
        if (out->has_deferred_param_error) break;
    }

    out->request_id = ok.request_id;
    out->target_slot = ep.slot;
    return MOQ_OK;
}

/* -- D16 inbound PUBLISH_DONE decode ------------------------------ */

static moq_result_t d16_decode_publish_done_inbound(
    moq_session_t *s,
    const moq_control_envelope_t *env,
    moq_decoded_publish_done_t *out,
    bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->target_slot = -1;

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_DONE before ESTABLISHED");
    }

    moq_d16_publish_done_t done;
    moq_result_t rc = moq_d16_decode_publish_done(env->payload,
                                                    env->payload_len, &done);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed PUBLISH_DONE");
    }

    moq_request_endpoint_t ep = request_registry_find_by_id(s, done.request_id);
    if (ep.kind == MOQ_REQ_SUBSCRIPTION) {
        int slot = ep.slot;
        if (slot < 0 || (size_t)slot >= s->sub_cap ||
            (s->subs[slot].state != MOQ_SUB_PENDING_SUBSCRIBER &&
             s->subs[slot].state != MOQ_SUB_ESTABLISHED)) {
            *out_consumed = true;
            return close_with_error(s, 0x3,
                "PUBLISH_DONE for non-active subscription");
        }
        if (s->subs[slot].role != MOQ_SUB_ROLE_SUBSCRIBER) {
            *out_consumed = true;
            return close_with_error(s, 0x3,
                "PUBLISH_DONE for publisher-role subscription");
        }
        *out_consumed = true;
        {
            moq_decoded_publish_done_t decoded = {
                .request_id = done.request_id,
                .target_slot = slot,
                .status_code = done.status_code,
                .stream_count = done.stream_count,
                .reason = done.reason,
                .reason_len = done.reason_len,
            };
            return session_core_on_subscribe_done(s, slot, &decoded,
                                                  true /* free_now */);
        }
    }
    if (ep.kind != MOQ_REQ_PUBLISH) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_DONE for unknown request");
    }

    moq_pub_entry_t *pe = &s->publishes[ep.slot];
    if (pe->role != MOQ_PUB_ROLE_SUBSCRIBER) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_DONE for non-subscriber publish");
    }
    if (pe->state != MOQ_PUB_ESTABLISHED) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "PUBLISH_DONE for non-established publish");
    }

    out->request_id  = done.request_id;
    out->target_slot = ep.slot;
    out->status_code = done.status_code;
    out->stream_count = done.stream_count;
    out->reason      = done.reason;
    out->reason_len  = done.reason_len;
    return MOQ_OK;
}

/* -- D16 inbound UNSUBSCRIBE decode ------------------------------- */

static moq_result_t d16_decode_unsubscribe(
    moq_session_t *s,
    const moq_control_envelope_t *env,
    moq_decoded_unsubscribe_t *out,
    bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->target_slot = -1;

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "UNSUBSCRIBE before ESTABLISHED");
    }

    uint64_t request_id = 0;
    moq_result_t rc = moq_d16_decode_varint_msg(env->payload,
                                                 env->payload_len,
                                                 &request_id);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed UNSUBSCRIBE");
    }

    /* Check publish entries first (local-parity request_id for
     * PUBLISH-initiated subscriptions). */
    moq_request_endpoint_t ep = request_registry_find_by_id(s, request_id);
    if (ep.kind == MOQ_REQ_PUBLISH) {
        moq_pub_entry_t *pe = &s->publishes[ep.slot];
        if (pe->role != MOQ_PUB_ROLE_PUBLISHER) {
            *out_consumed = true;
            return close_with_error(s, 0x4,
                "UNSUBSCRIBE for non-publisher publish");
        }
        if (pe->state != MOQ_PUB_ESTABLISHED) {
            *out_consumed = true;
            return close_with_error(s, 0x4,
                "UNSUBSCRIBE for non-established publish");
        }
        *out_consumed = true;
        return session_core_on_publish_unsubscribed(s, ep.slot);
    }

    /* Subscription path: validate peer parity. */
    bool peer_is_client = (s->perspective == MOQ_PERSPECTIVE_SERVER);
    uint64_t expected_parity = peer_is_client ? 0 : 1;
    if ((request_id & 1) != expected_parity) {
        *out_consumed = true;
        return close_with_error(s, 0x4, "UNSUBSCRIBE wrong parity");
    }

    int slot = sub_find_by_request_id(s, request_id);
    if (slot < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x4, "UNSUBSCRIBE unknown request ID");
    }

    moq_sub_entry_t *e = &s->subs[slot];
    if (e->role != MOQ_SUB_ROLE_PUBLISHER) {
        *out_consumed = true;
        return close_with_error(s, 0x4,
            "UNSUBSCRIBE for non-publisher subscription");
    }
    if (e->state != MOQ_SUB_PENDING_PUBLISHER &&
        e->state != MOQ_SUB_ESTABLISHED) {
        *out_consumed = true;
        return close_with_error(s, 0x4,
            "UNSUBSCRIBE for inactive subscription");
    }

    out->target_slot = slot;
    return MOQ_OK;
}

/* -- D16 inbound PUBLISH_NAMESPACE_DONE decode -------------------- */

static moq_result_t d16_decode_publish_namespace_done(
    moq_session_t *s,
    const moq_control_envelope_t *env,
    moq_decoded_publish_namespace_done_t *out,
    bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->target_slot = -1;

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3,
            "PUBLISH_NAMESPACE_DONE before ESTABLISHED");
    }

    uint64_t request_id = 0;
    moq_result_t rc = moq_d16_decode_varint_msg(env->payload,
                                                  env->payload_len,
                                                  &request_id);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed PUBLISH_NAMESPACE_DONE");
    }

    out->request_id = request_id;

    int slot = ann_find_by_request_id(s, request_id);
    if (slot < 0 || s->announcements[slot].state != MOQ_ANN_ESTABLISHED ||
        s->announcements[slot].role != MOQ_ANN_ROLE_RECEIVER) {
        *out_consumed = true;
        return close_with_error(s, 0x3,
            "PUBLISH_NAMESPACE_DONE for unknown announcement");
    }
    out->target_slot = slot;

    return MOQ_OK;
}

/* -- D16 inbound PUBLISH_NAMESPACE_CANCEL decode ------------------ */

static moq_result_t d16_decode_publish_namespace_cancel(
    moq_session_t *s,
    const moq_control_envelope_t *env,
    moq_decoded_publish_namespace_cancel_t *out,
    bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->target_slot = -1;

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3,
            "PUBLISH_NAMESPACE_CANCEL before ESTABLISHED");
    }

    moq_d16_publish_namespace_cancel_t cancel;
    moq_result_t rc = moq_d16_decode_publish_namespace_cancel(
        env->payload, env->payload_len, &cancel);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed PUBLISH_NAMESPACE_CANCEL");
    }

    int slot = ann_find_by_request_id(s, cancel.request_id);
    if (slot < 0 || s->announcements[slot].state != MOQ_ANN_ESTABLISHED ||
        s->announcements[slot].role != MOQ_ANN_ROLE_ANNOUNCER) {
        *out_consumed = true;
        return close_with_error(s, 0x3,
            "PUBLISH_NAMESPACE_CANCEL for unknown announcement");
    }

    out->request_id = cancel.request_id;
    out->target_slot = slot;
    out->error_code = cancel.error_code;
    out->reason = cancel.reason;
    out->reason_len = cancel.reason_len;
    return MOQ_OK;
}

/* -- D16 inbound FETCH_OK decode ---------------------------------- */

/* FETCH_OK wire format includes a params field (Section 9.17), but no
 * D16 param definition in Section 9.2.2 lists FETCH_OK as an allowed
 * message. Defined params are wrong-scope and silently ignored per
 * Section 9.2; truly unknown params close. */
static const uint64_t d16_fetch_ok_allowed_params[1] = { 0 };

static moq_result_t d16_decode_fetch_ok_inbound(
    moq_session_t *s,
    const moq_control_envelope_t *env,
    moq_decoded_fetch_ok_t *out,
    bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));
    out->target_slot = -1;

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "FETCH_OK before ESTABLISHED");
    }

    moq_kvp_entry_t params[8];
    moq_d16_fetch_ok_t ok = { .params = params, .params_cap = 8 };
    moq_result_t rc = moq_d16_decode_fetch_ok(env->payload,
                                                env->payload_len, &ok);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed FETCH_OK");
    }

    moq_request_endpoint_t ep = request_registry_find_by_id(s, ok.request_id);
    if (ep.kind != MOQ_REQ_FETCH) {
        /* Late FETCH_OK for a locally-cancelled fetch (its slot was freed at
         * cancel): ignore it but keep the tombstone so a following
         * FETCH_HEADER/data stream is still absorbed (stopped) rather than
         * closing the session. */
        if (fetch_cancel_tomb_contains(s, ok.request_id)) {
            *out_consumed = true;
            return MOQ_OK;
        }
        *out_consumed = true;
        return close_with_error(s, 0x3, "FETCH_OK for unknown request");
    }

    moq_fetch_entry_t *fe = &s->fetches[ep.slot];
    if (fe->role != MOQ_FETCH_ROLE_FETCHER) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "FETCH_OK for non-fetcher");
    }

    /* Validate params: empty allowed list ignores defined wrong-scope params,
     * closes on truly unknown params. */
    uint64_t perr = d16_validate_msg_params(ok.params, ok.params_count,
        d16_fetch_ok_allowed_params, 0);
    if (perr) {
        *out_consumed = true;
        return close_with_error(s, perr, "invalid FETCH_OK parameter");
    }

    out->request_id = ok.request_id;
    out->target_slot = ep.slot;
    out->end_of_track = (ok.end_of_track != 0);
    out->end_group = ok.end_group;
    out->end_object = ok.end_object;
    out->track_properties = ok.track_extensions;
    out->track_properties_len = ok.track_extensions_len;
    return MOQ_OK;
}

/* -- D16 control framing + dispatch -------------------------------- */

/* -- D16 inbound TRACK_STATUS decode ------------------------------- */

static const uint64_t d16_track_status_allowed_params[] = {
    MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
};

static moq_result_t d16_decode_track_status_request(moq_session_t *s,
                                                     const moq_control_envelope_t *env,
                                                     moq_decoded_track_status_request_t *out,
                                                     bool *out_consumed)
{
    *out_consumed = false;
    memset(out, 0, sizeof(*out));

    if (!session_is_active(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "TRACK_STATUS before ESTABLISHED");
    }
    /* After our GOAWAY (§10.4) the peer must not open new requests. */
    if (session_refuses_new_requests(s)) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "TRACK_STATUS after local GOAWAY");
    }

    moq_bytes_t ns_parts[32];
    moq_kvp_entry_t params[16];
    moq_d16_track_status_t ts = {
        .params = params, .params_cap = 16,
    };
    moq_result_t rc = moq_d16_decode_track_status(env->payload, env->payload_len,
                                                   ns_parts, 32, &ts);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x3, "malformed TRACK_STATUS");
    }

    if (ts.track_namespace.count > 0)
        memcpy(out->track_namespace_parts, ts.track_namespace.parts,
            ts.track_namespace.count * sizeof(moq_bytes_t));
    out->track_namespace.parts = out->track_namespace_parts;
    out->track_namespace.count = ts.track_namespace.count;
    out->track_name = ts.track_name;

    {
        moq_result_t vrc = s->profile->validate_inbound_request(
            s, ts.request_id, &out->endpoint);
        if (vrc < 0 || s->state == MOQ_SESS_CLOSED) {
            *out_consumed = true;
            return vrc;
        }
    }

    uint64_t perr = d16_validate_msg_params(ts.params, ts.params_count,
        d16_track_status_allowed_params,
        sizeof(d16_track_status_allowed_params) / sizeof(d16_track_status_allowed_params[0]));
    if (perr) {
        *out_consumed = true;
        return close_with_error(s, perr, "invalid TRACK_STATUS parameter");
    }

    moq_decoded_auth_token_t auth_in[MOQ_DECODED_MAX_TOKENS];
    size_t auth_in_count = 0;
    rc = d16_extract_auth_tokens(ts.params, ts.params_count,
        auth_in, MOQ_DECODED_MAX_TOKENS, &auth_in_count);
    if (rc < 0) {
        *out_consumed = true;
        return close_with_error(s, 0x6, "malformed auth token structure");
    }

    out->token_count = 0;
    uint64_t auth_reject_code = 0;

    rc = process_auth_tokens(s, auth_in, auth_in_count,
        out->tokens, &out->token_count, MOQ_DECODED_MAX_TOKENS,
        &auth_reject_code, out->token_staged,
        &out->auth_txn);
    if (rc < 0) {
        *out_consumed = true;
        return rc;
    }
    if (s->state == MOQ_SESS_CLOSED) {
        *out_consumed = true;
        return MOQ_OK;
    }

    if (auth_reject_code) {
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = moq_d16_encode_request_error(&ew, ts.request_id,
            auth_reject_code, 0, NULL, 0);
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            return rc;
        }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) {
            process_auth_tokens_abort_txn(s, &out->auth_txn);
            return rc;
        }
        s->profile->commit_inbound_request(s, &out->endpoint);
        process_auth_tokens_commit_txn(s, &out->auth_txn);
        *out_consumed = true;
        return MOQ_OK;
    }

    return MOQ_OK;
}

/* ==================== REGION: CONTROL DISPATCH ==================== */

static moq_result_t d16_handle_control_message(moq_session_t *s,
                                                const moq_control_envelope_t *env)
{
    switch (env->msg_type) {
    case MOQ_D16_CLIENT_SETUP:
        return d16_handle_setup_client(s, env);

    case MOQ_D16_SERVER_SETUP:
        return d16_handle_setup_server(s, env);

    case MOQ_D16_SUBSCRIBE: {
        moq_decoded_subscribe_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_subscribe(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;  /* auth reject handled internally */
        return session_core_on_subscribe(s, &decoded, -1);
    }

    case MOQ_D16_SUBSCRIBE_OK: {
        moq_decoded_subscribe_ok_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_subscribe_ok(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_subscribe_ok(s, &decoded, -1);
    }

    case MOQ_D16_REQUEST_OK: {
        if (!session_is_active(s))
            return close_with_error(s, 0x3, "REQUEST_OK before ESTABLISHED");

        moq_kvp_entry_t ok_params[8];
        moq_d16_request_ok_t ok = { .params = ok_params, .params_cap = 8 };
        moq_result_t rc = moq_d16_decode_request_ok(env->payload,
                                                      env->payload_len, &ok);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_OK");

        if (unsub_tomb_consume(s, ok.request_id))
            return MOQ_OK;

        moq_request_endpoint_t ep = request_registry_find_by_id(s, ok.request_id);
        if (ep.kind == MOQ_REQ_NONE)
            return close_with_error(s, 0x3, "REQUEST_OK for unknown request");
        switch (ep.kind) {
        case MOQ_REQ_SUBSCRIPTION:
            return close_with_error(s, 0x3,
                "REQUEST_OK for subscription (expected SUBSCRIBE_OK)");
        case MOQ_REQ_ANNOUNCEMENT: {
            if (s->announcements[ep.slot].state != MOQ_ANN_PENDING_ANNOUNCER)
                return close_with_error(s, 0x3,
                    "REQUEST_OK for non-pending announcement");
            if (ok.params_count > 0)
                return close_with_error(s, 0x3,
                    "REQUEST_OK for announcement must have zero params");
            moq_decoded_announcement_ok_t aok = { .target_slot = ep.slot };
            return session_core_on_announcement_ok(s, &aok);
        }
        case MOQ_REQ_FETCH:
            return close_with_error(s, 0x3,
                "REQUEST_OK for fetch (expected FETCH_OK)");
        case MOQ_REQ_PUBLISH:
            return close_with_error(s, 0x3,
                "REQUEST_OK for publish (expected PUBLISH_OK)");
        case MOQ_REQ_NAMESPACE_SUB:
            return close_with_error(s, 0x3,
                "REQUEST_OK for namespace-sub on control channel");
        case MOQ_REQ_TRACK_STATUS: {
            if (s->track_statuses[ep.slot].state != MOQ_TS_PENDING_REQUESTER)
                return close_with_error(s, 0x3,
                    "REQUEST_OK for non-pending track-status");
            /* §9.19: REQUEST_OK carries the same params as SUBSCRIBE_OK. */
            uint64_t perr = d16_validate_msg_params(ok.params, ok.params_count,
                d16_subscribe_ok_allowed_params,
                sizeof(d16_subscribe_ok_allowed_params) /
                sizeof(d16_subscribe_ok_allowed_params[0]));
            if (perr)
                return close_with_error(s, perr,
                    "invalid param in REQUEST_OK for track-status");
            moq_decoded_track_status_ok_t tsok = { .target_slot = ep.slot };
            for (size_t pi = 0; pi < ok.params_count; pi++) {
                if (!d16_param_is_allowed(ok.params[pi].type,
                        d16_subscribe_ok_allowed_params,
                        sizeof(d16_subscribe_ok_allowed_params) /
                        sizeof(d16_subscribe_ok_allowed_params[0])))
                    continue;
                if (ok.params[pi].type == MOQ_MSG_PARAM_LARGEST_OBJECT) {
                    moq_d16_location_t loc;
                    if (moq_d16_decode_location(ok.params[pi].value,
                            ok.params[pi].value_len, &loc) < 0)
                        return close_with_error(s, 0x3,
                            "malformed LARGEST_OBJECT in REQUEST_OK");
                    tsok.has_largest = true;
                    tsok.largest_group = loc.group;
                    tsok.largest_object = loc.object;
                } else if (ok.params[pi].type == MOQ_MSG_PARAM_EXPIRES) {
                    uint64_t v;
                    if (moq_d16_decode_param_expires(ok.params[pi].value,
                            ok.params[pi].value_len, &v) < 0)
                        return close_with_error(s, 0x3,
                            "malformed EXPIRES in REQUEST_OK");
                    tsok.has_expires = true;
                    tsok.expires_ms = v;
                }
            }
            return session_core_on_track_status_ok(s, &tsok);
        }
        case MOQ_REQ_SUBSCRIPTION_UPDATE: {
            uint64_t perr = d16_validate_msg_params(ok.params, ok.params_count,
                NULL, 0);
            if (perr)
                return close_with_error(s, perr,
                    "invalid param in REQUEST_OK for subscription update");
            if (ep.slot >= 0 && (size_t)ep.slot < s->sub_cap &&
                s->subs[ep.slot].update_pending &&
                s->subs[ep.slot].update_request_id == ok.request_id) {
                s->subs[ep.slot].update_pending = false;
            }
            request_registry_remove_by_id(s, ok.request_id);
            return MOQ_OK;
        }
        case MOQ_REQ_PUBLICATION_UPDATE: {
            uint64_t perr = d16_validate_msg_params(ok.params, ok.params_count,
                NULL, 0);
            if (perr)
                return close_with_error(s, perr,
                    "invalid param in REQUEST_OK for publication update");
            if (ep.slot >= 0 && (size_t)ep.slot < s->pub_cap &&
                s->publishes[ep.slot].update_pending &&
                s->publishes[ep.slot].update_request_id == ok.request_id) {
                s->publishes[ep.slot].update_pending = false;
            }
            request_registry_remove_by_id(s, ok.request_id);
            return MOQ_OK;
        }
        default:
            return close_with_error(s, 0x3, "REQUEST_OK for unknown kind");
        }
    }

    case MOQ_D16_REQUEST_ERROR: {
        if (!session_is_active(s))
            return close_with_error(s, 0x3, "REQUEST_ERROR before ESTABLISHED");

        moq_d16_request_error_t err;
        moq_result_t rc = moq_d16_decode_request_error(env->payload,
                                                         env->payload_len, &err);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUEST_ERROR");

        if (unsub_tomb_consume(s, err.request_id))
            return MOQ_OK;
        /* Late REQUEST_ERROR for a locally-cancelled fetch: absorb it (the slot
         * was freed at cancel), no event, no close. */
        if (fetch_cancel_tomb_consume(s, err.request_id))
            return MOQ_OK;

        moq_request_endpoint_t ep = request_registry_find_by_id(s, err.request_id);
        if (ep.kind == MOQ_REQ_NONE)
            return close_with_error(s, 0x3, "REQUEST_ERROR for unknown request");
        switch (ep.kind) {
        case MOQ_REQ_SUBSCRIPTION: {
            if (s->subs[ep.slot].state != MOQ_SUB_PENDING_SUBSCRIBER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR for unknown request");
            bool can_retry = (err.retry_interval > 0);
            moq_decoded_subscribe_error_t serr = {
                .target_slot = ep.slot,
                .error_code = err.error_code,
                .can_retry = can_retry,
                .retry_after_ms = can_retry ? (err.retry_interval - 1) : 0,
                .reason = err.reason,
                .reason_len = err.reason_len,
            };
            return session_core_on_subscribe_error(s, &serr, true, NULL);
        }
        case MOQ_REQ_ANNOUNCEMENT: {
            if (s->announcements[ep.slot].state != MOQ_ANN_PENDING_ANNOUNCER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR for non-pending announcement");
            bool can_retry = (err.retry_interval > 0);
            moq_decoded_announcement_error_t aerr = {
                .target_slot = ep.slot,
                .error_code = err.error_code,
                .can_retry = can_retry,
                .retry_after_ms = can_retry ? (err.retry_interval - 1) : 0,
                .reason = err.reason,
                .reason_len = err.reason_len,
            };
            return session_core_on_announcement_error(s, &aerr, NULL);
        }
        case MOQ_REQ_FETCH: {
            if (s->fetches[ep.slot].role != MOQ_FETCH_ROLE_FETCHER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR for non-fetcher fetch");
            bool can_retry_f = (err.retry_interval > 0);
            return session_core_on_fetch_error(s, ep.slot, err.error_code,
                can_retry_f, can_retry_f ? (err.retry_interval - 1) : 0,
                err.reason, err.reason_len, true, NULL);
        }
        case MOQ_REQ_PUBLISH: {
            if (s->publishes[ep.slot].role != MOQ_PUB_ROLE_PUBLISHER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR for non-publisher publish");
            /* REQUEST_ERROR is a terminal response to a still-pending PUBLISH;
             * one for an already-established publication is a duplicate/late
             * terminal -- a protocol violation, not a reason to tear down live
             * state (matches the SUBSCRIPTION case above). */
            if (s->publishes[ep.slot].state != MOQ_PUB_PENDING_PUBLISHER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR for non-pending publish");
            bool can_retry_p = (err.retry_interval > 0);
            return session_core_on_publish_error(s, ep.slot, err.error_code,
                can_retry_p, can_retry_p ? (err.retry_interval - 1) : 0,
                err.reason, err.reason_len, true);
        }
        case MOQ_REQ_NAMESPACE_SUB:
            return close_with_error(s, 0x3,
                "REQUEST_ERROR for namespace-sub on control channel");
        case MOQ_REQ_TRACK_STATUS: {
            if (s->track_statuses[ep.slot].role != MOQ_TS_ROLE_REQUESTER)
                return close_with_error(s, 0x3,
                    "REQUEST_ERROR for non-requester track-status");
            bool can_retry_ts = (err.retry_interval > 0);
            return session_core_on_track_status_error(s, ep.slot,
                err.error_code, can_retry_ts,
                can_retry_ts ? (err.retry_interval - 1) : 0,
                err.reason, err.reason_len, NULL);
        }
        case MOQ_REQ_SUBSCRIPTION_UPDATE: {
            /* TODO: surface update failure via event when facade needs it */
            if (ep.slot >= 0 && (size_t)ep.slot < s->sub_cap &&
                s->subs[ep.slot].update_pending &&
                s->subs[ep.slot].update_request_id == err.request_id) {
                s->subs[ep.slot].update_pending = false;
            }
            request_registry_remove_by_id(s, err.request_id);
            return MOQ_OK;
        }
        case MOQ_REQ_PUBLICATION_UPDATE: {
            /* TODO: surface update failure via event when facade needs it */
            if (ep.slot >= 0 && (size_t)ep.slot < s->pub_cap &&
                s->publishes[ep.slot].update_pending &&
                s->publishes[ep.slot].update_request_id == err.request_id) {
                s->publishes[ep.slot].update_pending = false;
            }
            request_registry_remove_by_id(s, err.request_id);
            return MOQ_OK;
        }
        default:
            return close_with_error(s, 0x3, "REQUEST_ERROR for unknown kind");
        }
    }

    case MOQ_D16_PUBLISH_NAMESPACE: {
        moq_decoded_publish_namespace_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_publish_namespace(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_publish_namespace(s, &decoded);
    }

    case MOQ_D16_PUBLISH_NAMESPACE_DONE: {
        moq_decoded_publish_namespace_done_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_publish_namespace_done(
            s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_publish_namespace_done(s, &decoded);
    }

    case MOQ_D16_PUBLISH_NAMESPACE_CANCEL: {
        moq_decoded_publish_namespace_cancel_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_publish_namespace_cancel(
            s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_publish_namespace_cancel(s, &decoded);
    }

    case MOQ_D16_MAX_REQUEST_ID: {
        if (!session_is_active(s))
            return close_with_error(s, 0x3, "MAX_REQUEST_ID before ESTABLISHED");

        uint64_t new_max = 0;
        moq_result_t rc = moq_d16_decode_varint_msg(env->payload,
                                                     env->payload_len,
                                                     &new_max);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed MAX_REQUEST_ID");

        uint64_t old_max = s->peer_setup.has_max_request_id ?
            s->peer_setup.max_request_id : 0;
        if (new_max <= old_max)
            return close_with_error(s, 0x3, "MAX_REQUEST_ID did not increase");

        moq_d16_profile_state_t *d16 =
            (moq_d16_profile_state_t *)s->profile_state;

        bool should_emit = d16->request_was_blocked &&
                           d16->next_local_request_id < new_max;

        if (should_emit && event_queue_full(s))
            return MOQ_ERR_WOULD_BLOCK;

        s->peer_setup.has_max_request_id = true;
        s->peer_setup.max_request_id = new_max;
        d16->requests_blocked_at = UINT64_MAX;

        if (should_emit) {
            d16->request_was_blocked = false;
            moq_event_t e;
            memset(&e, 0, sizeof(e));
            e.kind = MOQ_EVENT_REQUEST_READY;
            e.detail_size = (uint32_t)sizeof(moq_request_ready_event_t);
            e.borrow_epoch = s->borrow_epoch;
            e.u.request_ready.available_requests =
                (new_max - d16->next_local_request_id + 1) / 2;
            return push_event(s, &e);
        }
        return MOQ_OK;
    }

    case MOQ_D16_SUBSCRIBE_NAMESPACE:
    case MOQ_D16_NAMESPACE:
    case MOQ_D16_NAMESPACE_DONE:
        return close_with_error(s, 0x3,
            "bidi-stream-only message on control stream");

    case MOQ_D16_REQUESTS_BLOCKED: {
        if (!session_is_active(s))
            return close_with_error(s, 0x3, "REQUESTS_BLOCKED before ESTABLISHED");
        uint64_t blocked_at;
        moq_result_t rc = moq_d16_decode_varint_msg(env->payload,
                                                      env->payload_len, &blocked_at);
        if (rc < 0)
            return close_with_error(s, 0x3, "malformed REQUESTS_BLOCKED");
        return MOQ_OK;
    }

    case MOQ_D16_GOAWAY:
        return d16_handle_goaway(s, env);

    case MOQ_D16_PUBLISH: {
        moq_decoded_publish_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_publish(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_publish(s, &decoded);
    }

    case MOQ_D16_PUBLISH_OK: {
        moq_decoded_publish_ok_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_publish_ok_inbound(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_publish_ok(s, &decoded);
    }

    case MOQ_D16_PUBLISH_DONE: {
        moq_decoded_publish_done_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_publish_done_inbound(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_publish_done(s, &decoded, true);
    }

    case MOQ_D16_FETCH: {
        moq_decoded_fetch_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_fetch(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_fetch(s, &decoded);
    }

    case MOQ_D16_FETCH_OK: {
        moq_decoded_fetch_ok_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_fetch_ok_inbound(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_fetch_ok(s, &decoded);
    }

    case MOQ_D16_FETCH_CANCEL: {
        moq_decoded_fetch_cancel_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_fetch_cancel(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_fetch_cancel(s, &decoded);
    }

    case MOQ_D16_UNSUBSCRIBE: {
        moq_decoded_unsubscribe_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_unsubscribe(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_unsubscribe(s, &decoded);
    }

    case MOQ_D16_REQUEST_UPDATE:
    {
        moq_decoded_request_update_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_request_update(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_request_update(s, &decoded);
    }

    case MOQ_D16_TRACK_STATUS: {
        moq_decoded_track_status_request_t decoded;
        bool consumed = false;
        moq_result_t drc = d16_decode_track_status_request(s, env, &decoded, &consumed);
        if (drc != MOQ_OK) return drc;
        if (consumed) return MOQ_OK;
        return session_core_on_track_status_request(s, &decoded, false);
    }

    default:
        return close_with_error(s, 0x3, "unknown control message type");
    }
}

/* ==================== REGION: REQUEST ADMISSION / TRACK ALIAS ==================== */

/* -- D16 request admission ops ------------------------------------- */

static bool d16_has_request_capacity(const moq_session_t *s)
{
    const moq_d16_profile_state_t *d16 =
        (const moq_d16_profile_state_t *)s->profile_state;
    return s->peer_setup.has_max_request_id &&
           d16->next_local_request_id < s->peer_setup.max_request_id;
}

static moq_result_t d16_prepare_request(moq_session_t *s,
                                         moq_request_endpoint_t *out)
{
    moq_d16_profile_state_t *d16 =
        (moq_d16_profile_state_t *)s->profile_state;

    if (!s->peer_setup.has_max_request_id ||
        d16->next_local_request_id >= s->peer_setup.max_request_id) {
        /* Best-effort queue REQUESTS_BLOCKED. */
        uint64_t peer_max = s->peer_setup.has_max_request_id ?
            s->peer_setup.max_request_id : 0;
        if (d16->requests_blocked_at != peer_max) {
            uint8_t rb_buf[32];
            moq_buf_writer_t rw;
            moq_buf_writer_init(&rw, rb_buf, sizeof(rb_buf));
            if (moq_d16_encode_varint_msg(&rw, MOQ_D16_REQUESTS_BLOCKED,
                                           peer_max) == MOQ_OK) {
                if (queue_send_control(s, rb_buf,
                                       moq_buf_writer_offset(&rw)) == MOQ_OK)
                    d16->requests_blocked_at = peer_max;
            }
        }
        d16->request_was_blocked = true;
        return MOQ_ERR_REQUEST_BLOCKED;
    }

    memset(out, 0, sizeof(*out));
    out->has_request_id = true;
    out->request_id = d16->next_local_request_id;
    return MOQ_OK;
}

static void d16_commit_request(moq_session_t *s,
                                const moq_request_endpoint_t *ep)
{
    moq_d16_profile_state_t *d16 =
        (moq_d16_profile_state_t *)s->profile_state;
    (void)ep;
    d16->next_local_request_id += 2;
}

static void d16_abort_request(moq_session_t *s,
                               const moq_request_endpoint_t *ep)
{
    (void)s;
    (void)ep;
    /* No-op for D16: request ID was not consumed. */
}

static moq_result_t d16_validate_inbound_request(moq_session_t *s,
                                                  uint64_t wire_request_id,
                                                  moq_request_endpoint_t *out)
{
    const moq_d16_profile_state_t *d16 =
        (const moq_d16_profile_state_t *)s->profile_state;

    bool peer_is_client = (s->perspective == MOQ_PERSPECTIVE_SERVER);
    uint64_t expected_parity = peer_is_client ? 0 : 1;
    if ((wire_request_id & 1) != expected_parity)
        return close_with_error(s, 0x4, "wrong request ID parity");
    if (wire_request_id != d16->peer_next_request_id)
        return close_with_error(s, 0x4, "request ID not next in sequence");
    uint64_t local_max = s->local_setup.has_max_request_id ?
        s->local_setup.max_request_id : 0;
    if (wire_request_id >= local_max)
        return close_with_error(s, 0x7, "request ID exceeds MAX_REQUEST_ID");

    memset(out, 0, sizeof(*out));
    out->has_request_id = true;
    out->request_id = wire_request_id;
    return MOQ_OK;
}

static void d16_commit_inbound_request(moq_session_t *s,
                                        const moq_request_endpoint_t *ep)
{
    moq_d16_profile_state_t *d16 =
        (moq_d16_profile_state_t *)s->profile_state;
    (void)ep;
    d16->peer_next_request_id += 2;
}

static void d16_release_request(moq_session_t *s,
                                 const moq_request_endpoint_t *ep)
{
    (void)s;
    (void)ep;
    /* No-op for D16: request IDs are not recycled. */
}

static moq_result_t d16_grant_capacity(moq_session_t *s,
                                        uint64_t new_capacity,
                                        uint64_t now_us)
{
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    uint64_t old_max = s->local_setup.has_max_request_id ?
        s->local_setup.max_request_id : 0;
    if (new_capacity <= old_max) return MOQ_ERR_INVAL;

    uint8_t buf[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = moq_d16_encode_varint_msg(&w,
        MOQ_D16_MAX_REQUEST_ID, new_capacity);
    if (rc < 0) return rc;

    rc = queue_send_control(s, buf, moq_buf_writer_offset(&w));
    if (rc < 0) return rc;

    s->local_setup.has_max_request_id = true;
    s->local_setup.max_request_id = new_capacity;
    return MOQ_OK;
}

static uint64_t d16_peer_request_capacity(const moq_session_t *s)
{
    return s->peer_setup.max_request_id;
}

static uint64_t d16_local_request_capacity(const moq_session_t *s)
{
    if (!s->local_setup.has_max_request_id) return 0;
    return s->local_setup.max_request_id;
}

/* -- D16 track alias ops ------------------------------------------- */

static uint64_t d16_next_track_alias(const moq_session_t *s)
{
    const moq_d16_profile_state_t *d16 =
        (const moq_d16_profile_state_t *)s->profile_state;
    return d16->next_track_alias;
}

static void d16_advance_track_alias(moq_session_t *s, uint64_t next_after)
{
    moq_d16_profile_state_t *d16 =
        (moq_d16_profile_state_t *)s->profile_state;
    d16->next_track_alias = next_after;
}

/*
 * Auth-token helper kernel (encode side): with d16_extract_auth_tokens
 * above, these belong to profile_d16_kernel.c on a future split.
 */
/* -- D16 outbound auth-token param helper --------------------------- */

/* D16 inbound decoders use 16-entry KVP arrays. Outbound messages
 * must not exceed this or a libmoq peer will reject them. */
#define D16_MAX_MSG_PARAMS 16

/*
 * Encode auth tokens into KVP entries. AUTH_TOKEN type 0x03 must
 * precede FORWARD 0x10, SUBSCRIBER_PRIORITY 0x20, etc. in ascending
 * KVP order. Callers emit these entries before other param types.
 *
 * tok_entries[]: caller-provided array, at least token_count entries.
 * tok_buf/tok_buf_cap: caller-provided buffer for encoded token values.
 * Returns the number of entries written, or -1 on error (buffer overflow).
 */
static int d16_encode_auth_token_params(
    const moq_auth_token_t *tokens, size_t token_count,
    moq_kvp_entry_t *tok_entries, uint8_t *tok_buf, size_t tok_buf_cap)
{
    moq_buf_writer_t tw;
    moq_buf_writer_init(&tw, tok_buf, tok_buf_cap);

    for (size_t i = 0; i < token_count; i++) {
        size_t val_start = tw.pos;
        moq_d16_auth_token_t d16tok = {
            .alias_type      = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type      = tokens[i].token_type,
            .token_value     = tokens[i].token_value.data,
            .token_value_len = tokens[i].token_value.len,
        };
        moq_result_t rc = moq_d16_auth_token_encode(&tw, &d16tok);
        if (rc < 0) return -1;
        tok_entries[i] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_buf + val_start,
            .value_len = tw.pos - val_start,
            .is_varint = false, .raw = NULL, .raw_len = 0 };
    }
    return (int)token_count;
}

/*
 * Compute the total encoded size of auth token USE_VALUE structures.
 * Used to preflight whether a caller-provided buffer can hold all
 * token values before attempting the actual encode.
 */
static size_t d16_auth_tokens_encoded_size(const moq_auth_token_t *tokens,
                                            size_t count)
{
    size_t total = 0;
    for (size_t i = 0; i < count; i++) {
        total += moq_quic_varint_len(MOQ_AUTH_TOKEN_USE_VALUE);
        total += moq_quic_varint_len(tokens[i].token_type);
        total += tokens[i].token_value.len;
    }
    return total;
}

/*
 * Build a combined param array with auth tokens first, then other
 * params appended by the caller. Uses scratch_alloc_aligned from the
 * session for variable-count KVP entry + value storage. Returns NULL
 * if scratch is exhausted (caller should return MOQ_ERR_BUFFER).
 *
 * On success:
 *   *out_params points to the merged array (auth entries + extra_count slots)
 *   *out_count is set to token_count
 *   The caller appends its own params at out_params[*out_count] onwards.
 */
static moq_kvp_entry_t *d16_alloc_auth_params(
    moq_session_t *s,
    const moq_auth_token_t *tokens, size_t token_count,
    size_t extra_cap, size_t *out_count)
{
    size_t total_entries = token_count + extra_cap;
    moq_kvp_entry_t *entries = (moq_kvp_entry_t *)scratch_alloc_aligned(
        s, total_entries * sizeof(moq_kvp_entry_t), _Alignof(moq_kvp_entry_t));
    if (!entries) return NULL;

    size_t val_size = d16_auth_tokens_encoded_size(tokens, token_count);
    uint8_t *val_buf = (uint8_t *)scratch_alloc_aligned(s, val_size, 1);
    if (!val_buf && val_size > 0) return NULL;

    int n = d16_encode_auth_token_params(tokens, token_count,
                                          entries, val_buf, val_size);
    if (n < 0) return NULL;
    *out_count = (size_t)n;
    return entries;
}

/* ==================== REGION: OUTBOUND CONTROL ENCODE ==================== */
/*
 * Outbound encode band. As with the decode band, domains are interleaved
 * (subscribe, publish, namespace, fetch, ...) and labelled by their own
 * "-- D16 ... encode --" banners; a physical split regroups each domain's
 * encode with its decode.
 */

/* -- D16 outbound subscribe-family encode ops ----------------------- */

static moq_result_t d16_encode_subscribe(moq_session_t *s,
                                          moq_buf_writer_t *w,
                                          const moq_subscribe_encode_args_t *args)
{
    uint8_t local_param_bufs[8][16];
    moq_kvp_entry_t local_params[8];
    size_t param_count = 0;

    /* Auth tokens precede other params in ascending KVP order. */
    moq_kvp_entry_t *params = local_params;
    size_t scratch_saved = s->output_scratch_len;

    if (args->auth_token_count > 0) {
        params = d16_alloc_auth_params(s, args->auth_tokens,
            args->auth_token_count, 8, &param_count);
        if (!params) {
            s->output_scratch_len = scratch_saved;
            return MOQ_ERR_BUFFER;
        }
    }

    /* Params must be in ascending type order:
     * AUTH_TOKEN 0x03, FORWARD 0x10, SUBSCRIBER_PRIORITY 0x20,
     * SUBSCRIPTION_FILTER 0x21, GROUP_ORDER 0x22. */
    size_t pbi = 0;
    if (args->has_forward && !args->forward) {
        size_t n = moq_quic_varint_encode(0, local_param_bufs[pbi], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = local_param_bufs[pbi], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++; pbi++;
    }
    if (args->subscriber_priority != 128) {
        size_t n = moq_quic_varint_encode(args->subscriber_priority,
                                           local_param_bufs[pbi], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
            .value = local_param_bufs[pbi], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++; pbi++;
    }
    if (args->filter != MOQ_SUBSCRIBE_FILTER_NONE) {
        moq_d16_subscription_filter_t f = {
            .filter_type = args->filter,
            .start_group = args->start_group,
            .start_object = args->start_object,
            .end_group = args->end_group,
        };
        size_t flen = 0;
        moq_result_t frc = moq_d16_encode_subscription_filter(
            local_param_bufs[pbi], 16, &flen, &f);
        if (frc < 0) {
            s->output_scratch_len = scratch_saved;
            return MOQ_ERR_INVAL;
        }
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_SUBSCRIPTION_FILTER,
            .value = local_param_bufs[pbi], .value_len = flen,
            .is_varint = false, .raw = NULL, .raw_len = 0 };
        param_count++; pbi++;
    }
    if (args->group_order != MOQ_GROUP_ORDER_DEFAULT) {
        size_t n = moq_quic_varint_encode(args->group_order,
                                           local_param_bufs[pbi], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_GROUP_ORDER,
            .value = local_param_bufs[pbi], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++; pbi++;
    }
    if (args->has_new_group_request) {               /* 0x32, last in order */
        size_t n = moq_quic_varint_encode(args->new_group_request,
                                           local_param_bufs[pbi], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_NEW_GROUP_REQUEST,
            .value = local_param_bufs[pbi], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++; pbi++;
    }

    if (param_count > D16_MAX_MSG_PARAMS) {
        s->output_scratch_len = scratch_saved;
        return MOQ_ERR_INVAL;
    }
    moq_result_t rc = moq_d16_encode_subscribe(w, args->request_id,
        &args->track_namespace, args->track_name, params, param_count);
    s->output_scratch_len = scratch_saved;
    return rc;
}

static moq_result_t d16_encode_subscribe_ok(moq_session_t *s,
                                              moq_buf_writer_t *w,
                                              const moq_subscribe_ok_encode_args_t *args)
{
    (void)s;
    moq_kvp_entry_t params[4];
    size_t param_count = 0;
    uint8_t param_bufs[4][16];

    if (args->has_largest) {
        moq_buf_writer_t lw;
        moq_buf_writer_init(&lw, param_bufs[param_count], 16);
        moq_buf_write_varint(&lw, args->largest_group);
        moq_buf_write_varint(&lw, args->largest_object);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
            .value = param_bufs[param_count],
            .value_len = moq_buf_writer_offset(&lw),
            .is_varint = false, .raw = NULL, .raw_len = 0 };
        param_count++;
    }
    if (args->has_expires && args->expires_ms > 0) {
        size_t n = moq_quic_varint_encode(args->expires_ms,
                                           param_bufs[param_count], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_EXPIRES,
            .value = param_bufs[param_count], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++;
    }

    return moq_d16_encode_subscribe_ok(w, args->request_id, args->track_alias,
        params, param_count,
        args->track_properties, args->track_properties_len);
}

static moq_result_t d16_encode_request_ok(moq_session_t *s,
                                            moq_buf_writer_t *w,
                                            uint64_t request_id)
{
    (void)s;
    return moq_d16_encode_request_ok(w, request_id, NULL, 0);
}

static moq_result_t d16_encode_request_error(moq_session_t *s,
                                               moq_buf_writer_t *w,
                                               const moq_request_error_encode_args_t *args)
{
    (void)s;
    /* Draft-16 has no REQUEST_ERROR Redirect structure; refuse a redirect tail. */
    if (args->has_redirect) return MOQ_ERR_INVAL;
    uint64_t wire_retry = args->can_retry ? (args->retry_after_ms + 1) : 0;
    return moq_d16_encode_request_error(w, args->request_id, args->error_code,
        wire_retry, args->reason, args->reason_len);
}

static moq_result_t d16_encode_unsubscribe(moq_session_t *s,
                                             moq_buf_writer_t *w,
                                             uint64_t request_id)
{
    (void)s;
    return moq_d16_encode_varint_msg(w, MOQ_D16_UNSUBSCRIBE, request_id);
}

static moq_result_t d16_encode_request_update_op(moq_session_t *s,
    moq_buf_writer_t *w,
    const moq_request_update_encode_args_t *args)
{
    /* Params must be in ascending KVP type order: DELIVERY_TIMEOUT 0x02,
     * AUTH_TOKEN 0x03 (repeatable), FORWARD 0x10, SUBSCRIBER_PRIORITY
     * 0x20. With auth tokens the entry array and encoded token values
     * come from output scratch (the same pattern as d16_encode_subscribe);
     * the budget stays within the inbound decoder's KVP cap. */
    moq_kvp_entry_t local_params[4];
    moq_kvp_entry_t *params = local_params;
    size_t params_cap = 4;
    uint8_t *tok_buf = NULL;
    size_t tok_buf_len = 0;
    size_t scratch_saved = s->output_scratch_len;

    if (args->auth_token_count > 0) {
        if (args->auth_token_count > D16_MAX_MSG_PARAMS - 4)
            return MOQ_ERR_INVAL;
        params_cap = args->auth_token_count + 4;
        params = (moq_kvp_entry_t *)scratch_alloc_aligned(
            s, params_cap * sizeof(moq_kvp_entry_t),
            _Alignof(moq_kvp_entry_t));
        if (!params) {
            s->output_scratch_len = scratch_saved;
            return MOQ_ERR_BUFFER;
        }
        tok_buf_len = d16_auth_tokens_encoded_size(args->auth_tokens,
                                                   args->auth_token_count);
        tok_buf = (uint8_t *)scratch_alloc_aligned(s, tok_buf_len, 1);
        if (!tok_buf && tok_buf_len > 0) {
            s->output_scratch_len = scratch_saved;
            return MOQ_ERR_BUFFER;
        }
    }

    size_t pc = 0;
    uint8_t param_bufs[4][8];
    size_t pbi = 0;

    if (args->has_delivery_timeout) {
        uint64_t ms = args->delivery_timeout_us / 1000;
        if (ms == 0) goto inval;
        size_t n = moq_quic_varint_encode(ms, param_bufs[pbi], 8);
        if (n == 0) goto inval;
        params[pc] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
            .value = param_bufs[pbi], .value_len = n, .is_varint = true };
        pc++; pbi++;
    }
    if (args->auth_token_count > 0) {
        int n = d16_encode_auth_token_params(args->auth_tokens,
            args->auth_token_count, params + pc, tok_buf, tok_buf_len);
        if (n < 0) goto inval;
        pc += (size_t)n;
    }
    if (args->has_forward) {
        size_t n = moq_quic_varint_encode(args->forward ? 1 : 0,
            param_bufs[pbi], 8);
        params[pc] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = param_bufs[pbi], .value_len = n, .is_varint = true };
        pc++; pbi++;
    }
    if (args->has_subscriber_priority) {
        size_t n = moq_quic_varint_encode(args->subscriber_priority,
            param_bufs[pbi], 8);
        params[pc] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
            .value = param_bufs[pbi], .value_len = n, .is_varint = true };
        pc++; pbi++;
    }
    if (args->has_new_group_request) {               /* 0x32, last in order */
        size_t n = moq_quic_varint_encode(args->new_group_request,
            param_bufs[pbi], 8);
        params[pc] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_NEW_GROUP_REQUEST,
            .value = param_bufs[pbi], .value_len = n, .is_varint = true };
        pc++; pbi++;
    }

    {
        moq_result_t rc = moq_d16_encode_request_update(w, args->request_id,
            args->existing_request_id, params, pc);
        /* The KVP entries/token bytes are fully serialized into w; the
         * scratch mark restores on success AND failure (the same contract
         * as every other D16 auth encoder -- a leak here would consume
         * scratch on every update-with-auth). */
        s->output_scratch_len = scratch_saved;
        return rc;
    }

inval:
    s->output_scratch_len = scratch_saved;
    return MOQ_ERR_INVAL;
}

static moq_result_t d16_encode_goaway(moq_session_t *s,
                                       moq_buf_writer_t *w,
                                       const moq_goaway_encode_args_t *args)
{
    (void)s;
    return moq_d16_encode_goaway(w, args->uri, args->uri_len);
}

static moq_result_t d16_encode_publish_namespace_op(moq_session_t *s,
                                                      moq_buf_writer_t *w,
                                                      const moq_publish_namespace_encode_args_t *args)
{
    if (args->auth_token_count == 0)
        return moq_d16_encode_publish_namespace(w, args->request_id,
            &args->track_namespace, NULL, 0);

    size_t scratch_saved = s->output_scratch_len;
    size_t tok_count = 0;
    moq_kvp_entry_t *params = d16_alloc_auth_params(s, args->auth_tokens,
        args->auth_token_count, 0, &tok_count);
    if (!params) {
        s->output_scratch_len = scratch_saved;
        return MOQ_ERR_BUFFER;
    }
    if (tok_count > D16_MAX_MSG_PARAMS) {
        s->output_scratch_len = scratch_saved;
        return MOQ_ERR_INVAL;
    }
    moq_result_t rc = moq_d16_encode_publish_namespace(w, args->request_id,
        &args->track_namespace, params, tok_count);
    s->output_scratch_len = scratch_saved;
    return rc;
}

static moq_result_t d16_encode_publish_namespace_done_op(moq_session_t *s,
                                                          moq_buf_writer_t *w,
                                                          uint64_t request_id)
{
    (void)s;
    return moq_d16_encode_varint_msg(w, MOQ_D16_PUBLISH_NAMESPACE_DONE,
                                      request_id);
}

static moq_result_t d16_encode_publish_namespace_cancel_op(
    moq_session_t *s, moq_buf_writer_t *w,
    const moq_publish_namespace_cancel_encode_args_t *args)
{
    (void)s;
    return moq_d16_encode_publish_namespace_cancel(w, args->request_id,
        args->error_code, args->reason, args->reason_len);
}

/* -- D16 PUBLISH encode ops --------------------------------------- */

static moq_result_t d16_encode_publish_op(moq_session_t *s,
                                            moq_buf_writer_t *w,
                                            const moq_publish_encode_args_t *args)
{
    uint8_t local_param_bufs[2][16];
    moq_kvp_entry_t local_params[2];
    size_t param_count = 0;

    moq_kvp_entry_t *params = local_params;
    size_t scratch_saved = s->output_scratch_len;

    if (args->auth_token_count > 0) {
        params = d16_alloc_auth_params(s, args->auth_tokens,
            args->auth_token_count, 2, &param_count);
        if (!params) {
            s->output_scratch_len = scratch_saved;
            return MOQ_ERR_BUFFER;
        }
    }

    if (args->has_forward && !args->forward) {
        size_t n = moq_quic_varint_encode(0, local_param_bufs[0], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_FORWARD,
            .value = local_param_bufs[0], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++;
    }

    if (param_count > D16_MAX_MSG_PARAMS) {
        s->output_scratch_len = scratch_saved;
        return MOQ_ERR_INVAL;
    }

    moq_d16_publish_t pub = {
        .request_id = args->request_id,
        .track_namespace = args->track_namespace,
        .track_name = args->track_name,
        .track_alias = args->track_alias,
        .params = params,
        .params_count = param_count,
        .params_cap = param_count,
        .track_extensions = args->track_properties,
        .track_extensions_len = args->track_properties_len,
    };
    moq_result_t rc = moq_d16_encode_publish(w, &pub);
    s->output_scratch_len = scratch_saved;
    return rc;
}

/* Lenient outbound-side extraction of dynamic-group support (mirrors the
 * inbound lenient walk; a >1 value reads as unsupported here -- the peer
 * will close on receipt). */
static bool d16_track_properties_dynamic_groups(const uint8_t *data,
                                                size_t len)
{
    bool dyn = false;
    if (d16_scan_extensions_dynamic_groups(data, len, &dyn) < 0)
        return false;
    return dyn;
}

static moq_result_t d16_encode_publish_ok_op(moq_session_t *s,
                                               moq_buf_writer_t *w,
                                               uint64_t request_id,
                                               uint8_t subscriber_priority,
                                               uint8_t group_order,
                                               bool has_new_group_request,
                                               uint64_t new_group_request)
{
    (void)s;
    moq_kvp_entry_t params[4];
    size_t param_count = 0;
    uint8_t param_bufs[4][16];

    if (subscriber_priority != 128) {
        size_t n = moq_quic_varint_encode(subscriber_priority,
                                           param_bufs[param_count], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
            .value = param_bufs[param_count], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++;
    }
    if (group_order != MOQ_GROUP_ORDER_DEFAULT) {
        size_t n = moq_quic_varint_encode(group_order,
                                           param_bufs[param_count], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_GROUP_ORDER,
            .value = param_bufs[param_count], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++;
    }
    if (has_new_group_request) {                     /* 0x32, last in order */
        size_t n = moq_quic_varint_encode(new_group_request,
                                           param_bufs[param_count], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_NEW_GROUP_REQUEST,
            .value = param_bufs[param_count], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++;
    }
    return moq_d16_encode_publish_ok(w, request_id, params, param_count);
}

static moq_result_t d16_encode_publish_done_op(moq_session_t *s,
                                                 moq_buf_writer_t *w,
                                                 const moq_finish_publish_encode_args_t *args)
{
    (void)s;
    return moq_d16_encode_publish_done(w, args->request_id, args->status_code,
        args->stream_count, args->reason, args->reason_len);
}

static moq_result_t d16_encode_track_status_op(moq_session_t *s,
                                                moq_buf_writer_t *w,
                                                const moq_track_status_encode_args_t *args)
{
    if (args->auth_token_count == 0)
        return moq_d16_encode_track_status(w, args->request_id,
            &args->track_namespace, args->track_name, NULL, 0);

    size_t scratch_saved = s->output_scratch_len;
    size_t tok_count = 0;
    moq_kvp_entry_t *params = d16_alloc_auth_params(s, args->auth_tokens,
        args->auth_token_count, 0, &tok_count);
    if (!params) {
        s->output_scratch_len = scratch_saved;
        return MOQ_ERR_BUFFER;
    }
    if (tok_count > D16_MAX_MSG_PARAMS) {
        s->output_scratch_len = scratch_saved;
        return MOQ_ERR_INVAL;
    }
    moq_result_t rc = moq_d16_encode_track_status(w, args->request_id,
        &args->track_namespace, args->track_name, params, tok_count);
    s->output_scratch_len = scratch_saved;
    return rc;
}

/* TRACK_STATUS_OK: a control-channel REQUEST_OK carrying LARGEST_OBJECT /
 * EXPIRES (no Track Properties tail under draft-16). */
static moq_result_t d16_encode_track_status_ok_op(moq_session_t *s,
    moq_buf_writer_t *w, const moq_track_status_ok_encode_args_t *args)
{
    (void)s;
    moq_kvp_entry_t params[2];
    size_t param_count = 0;
    uint8_t param_bufs[2][16];

    /* Parameters MUST be emitted in ascending Type order (the KVP encoder rejects
     * a type smaller than the previous): EXPIRES (0x08) before LARGEST_OBJECT
     * (0x09). */
    if (args->has_expires && args->expires_ms > 0) {
        size_t n = moq_quic_varint_encode(args->expires_ms,
                                           param_bufs[param_count], 16);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_EXPIRES,
            .value = param_bufs[param_count], .value_len = n,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++;
    }
    if (args->has_largest) {
        moq_buf_writer_t lw;
        moq_buf_writer_init(&lw, param_bufs[param_count], 16);
        moq_buf_write_varint(&lw, args->largest_group);
        moq_buf_write_varint(&lw, args->largest_object);
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_LARGEST_OBJECT,
            .value = param_bufs[param_count],
            .value_len = moq_buf_writer_offset(&lw),
            .is_varint = false, .raw = NULL, .raw_len = 0 };
        param_count++;
    }
    return moq_d16_encode_request_ok(w, args->request_id, params, param_count);
}

/* -- D16 inbound SUBSCRIBE_NAMESPACE decode ------------------------ */

static const uint64_t d16_ns_sub_allowed_params[] = {
    MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
    MOQ_MSG_PARAM_FORWARD,
};

static moq_result_t d16_decode_ns_sub_request(
    moq_session_t *s,
    const uint8_t *data, size_t len,
    moq_decoded_ns_sub_request_t *out)
{
    (void)s;
    memset(out, 0, sizeof(*out));
    out->forward = true;  /* default */

    moq_control_envelope_t env;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_result_t rc = moq_control_decode_envelope(&r, &env);
    if (rc == MOQ_ERR_BUFFER) return MOQ_ERR_BUFFER;
    if (rc < 0) return MOQ_ERR_PROTO;

    if (env.msg_type != MOQ_D16_SUBSCRIBE_NAMESPACE)
        return MOQ_ERR_PROTO;

    size_t consumed = moq_buf_reader_offset(&r);
    out->has_trailing_bytes = (consumed != len);

    moq_kvp_entry_t params[16];
    moq_d16_subscribe_namespace_t sub_ns = {
        .track_namespace_prefix = { out->parts_buf, 0 },
        .params = params,
        .params_count = 0,
        .params_cap = 16,
    };
    rc = moq_d16_decode_subscribe_namespace(env.payload, env.payload_len,
                                             out->parts_buf,
                                             MOQ_DECODED_NS_SUB_MAX_PARTS,
                                             &sub_ns);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (sub_ns.subscribe_options > 2)
        return MOQ_ERR_PROTO;

    if (sub_ns.track_namespace_prefix.count > MOQ_DECODED_NS_SUB_MAX_PARTS)
        return MOQ_ERR_PROTO;

    /* Parameter validation: unknown → ERR_PROTO; defined wrong-scope → ignored. */
    uint64_t perr = d16_validate_msg_params(sub_ns.params, sub_ns.params_count,
        d16_ns_sub_allowed_params,
        sizeof(d16_ns_sub_allowed_params) / sizeof(d16_ns_sub_allowed_params[0]));
    if (perr)
        return MOQ_ERR_PROTO;

    /* Extract auth tokens. */
    rc = d16_extract_auth_tokens(sub_ns.params, sub_ns.params_count,
        out->auth_tokens, MOQ_DECODED_MAX_TOKENS, &out->auth_token_count);
    if (rc < 0) return MOQ_ERR_PROTO;

    /* Decode FORWARD param (default true). */
    for (size_t i = 0; i < sub_ns.params_count; i++) {
        if (sub_ns.params[i].type == MOQ_MSG_PARAM_FORWARD) {
            bool v;
            if (moq_d16_decode_param_forward(
                    sub_ns.params[i].value, sub_ns.params[i].value_len, &v) < 0)
                return MOQ_ERR_PROTO;
            out->forward = v;
            break;
        }
    }

    out->request_id = sub_ns.request_id;
    out->namespace_interest = (moq_namespace_interest_t)sub_ns.subscribe_options;
    out->prefix = sub_ns.track_namespace_prefix;
    return MOQ_OK;
}

/* -- D16 inbound namespace-sub response decode -------------------- */

static moq_result_t d16_decode_ns_sub_response(
    moq_session_t *s,
    const uint8_t *data, size_t len,
    bool got_response,
    uint64_t expected_request_id,
    moq_decoded_ns_sub_response_t *out)
{
    (void)s;
    memset(out, 0, sizeof(*out));

    moq_control_envelope_t env;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    moq_result_t rc = moq_control_decode_envelope(&r, &env);
    if (rc == MOQ_ERR_BUFFER) return MOQ_ERR_BUFFER;
    if (rc < 0) {
        out->close_reason = "malformed bidi response stream envelope";
        return MOQ_ERR_PROTO;
    }

    size_t consumed = moq_buf_reader_offset(&r);

    if (!got_response) {
        /* First response: must be REQUEST_OK or REQUEST_ERROR. */
        switch (env.msg_type) {
        case MOQ_D16_REQUEST_OK: {
            moq_kvp_entry_t ok_params[4];
            moq_d16_request_ok_t ok = { .params = ok_params, .params_cap = 4 };
            rc = moq_d16_decode_request_ok(env.payload, env.payload_len, &ok);
            if (rc < 0) {
                out->close_reason = "malformed REQUEST_OK on bidi stream";
                return MOQ_ERR_PROTO;
            }
            if (ok.request_id != expected_request_id) {
                out->close_reason = "REQUEST_OK request_id mismatch on bidi stream";
                return MOQ_ERR_PROTO;
            }
            out->kind = MOQ_NS_RESP_OK;
            out->consumed = consumed;
            return MOQ_OK;
        }
        case MOQ_D16_REQUEST_ERROR: {
            /* Check for trailing bytes after terminal REQUEST_ERROR. */
            out->has_trailing_bytes = (consumed < len);

            moq_d16_request_error_t err;
            rc = moq_d16_decode_request_error(env.payload, env.payload_len, &err);
            if (rc < 0) {
                out->close_reason = "malformed REQUEST_ERROR on bidi stream";
                return MOQ_ERR_PROTO;
            }
            if (err.request_id != expected_request_id) {
                out->close_reason = "REQUEST_ERROR request_id mismatch on bidi stream";
                return MOQ_ERR_PROTO;
            }
            out->kind = MOQ_NS_RESP_ERROR;
            out->consumed = consumed;
            out->error_code = err.error_code;
            out->reason = err.reason;
            out->reason_len = err.reason_len;
            return MOQ_OK;
        }
        case MOQ_D16_NAMESPACE:
        case MOQ_D16_NAMESPACE_DONE:
            out->close_reason = "NAMESPACE/NAMESPACE_DONE before REQUEST_OK";
            return MOQ_ERR_PROTO;
        default:
            out->close_reason = "unexpected message type on bidi response stream";
            return MOQ_ERR_PROTO;
        }
    } else {
        /* Post-OK: NAMESPACE or NAMESPACE_DONE. */
        switch (env.msg_type) {
        case MOQ_D16_NAMESPACE:
        case MOQ_D16_NAMESPACE_DONE: {
            moq_buf_reader_t sr;
            moq_buf_reader_init(&sr, env.payload, env.payload_len);
            out->suffix.parts = out->parts_buf;
            out->suffix.count = 0;
            rc = moq_buf_read_namespace_prefix(&sr, out->parts_buf,
                                                MOQ_DECODED_NS_SUB_MAX_PARTS,
                                                &out->suffix);
            if (rc < 0) {
                out->close_reason = "malformed namespace suffix on bidi stream";
                return MOQ_ERR_PROTO;
            }
            if (moq_buf_reader_remaining(&sr) != 0) {
                out->close_reason = "trailing bytes in namespace suffix payload";
                return MOQ_ERR_PROTO;
            }
            out->kind = (env.msg_type == MOQ_D16_NAMESPACE)
                ? MOQ_NS_RESP_NAMESPACE : MOQ_NS_RESP_NAMESPACE_DONE;
            out->consumed = consumed;
            return MOQ_OK;
        }
        case MOQ_D16_REQUEST_OK:
        case MOQ_D16_REQUEST_ERROR:
            out->close_reason = "duplicate response on bidi stream";
            return MOQ_ERR_PROTO;
        default:
            out->close_reason = "unexpected message type on bidi response stream";
            return MOQ_ERR_PROTO;
        }
    }
}

/* -- D16 outbound namespace-sub encode ops ------------------------- */

static moq_result_t d16_encode_subscribe_namespace(
    moq_session_t *s, moq_buf_writer_t *w,
    const moq_subscribe_namespace_encode_args_t *args)
{
    if (args->auth_token_count == 0)
        return moq_d16_encode_subscribe_namespace(
            w, args->request_id, &args->prefix,
            args->namespace_interest, NULL, 0);

    size_t scratch_saved = s->output_scratch_len;
    size_t tok_count = 0;
    moq_kvp_entry_t *params = d16_alloc_auth_params(s, args->auth_tokens,
        args->auth_token_count, 0, &tok_count);
    if (!params) {
        s->output_scratch_len = scratch_saved;
        return MOQ_ERR_BUFFER;
    }
    if (tok_count > D16_MAX_MSG_PARAMS) {
        s->output_scratch_len = scratch_saved;
        return MOQ_ERR_INVAL;
    }
    moq_result_t rc = moq_d16_encode_subscribe_namespace(
        w, args->request_id, &args->prefix,
        args->namespace_interest, params, tok_count);
    s->output_scratch_len = scratch_saved;
    return rc;
}

static moq_result_t d16_encode_namespace_msg(
    moq_session_t *s, moq_buf_writer_t *w,
    const moq_namespace_msg_encode_args_t *args)
{
    (void)s;
    uint64_t msg_type = args->is_done ? MOQ_D16_NAMESPACE_DONE
                                      : MOQ_D16_NAMESPACE;
    size_t len_offset;
    moq_result_t rc = moq_control_write_header(w, msg_type, &len_offset);
    if (rc < 0) return MOQ_ERR_BUFFER;
    size_t payload_start = moq_buf_writer_offset(w);
    rc = moq_buf_write_namespace_prefix(w, &args->suffix);
    if (rc < 0) return rc;
    size_t plen = moq_buf_writer_offset(w) - payload_start;
    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) return MOQ_ERR_BUFFER;
    return MOQ_OK;
}

/* ==================== REGION: DATA PLANE / STREAM CLASSIFICATION / DATAGRAM ==================== */
/*
 * Object/subgroup/datagram encode+decode and stream classification. This
 * is the most self-contained region (touches only decode-scratch fields,
 * no session_core_on_* / push_action) - the natural first cut for a split,
 * to profile_d16_data.c. Fetch object/header encode below is data-plane-
 * adjacent and would join it. process_control_data() interleaved here
 * belongs with control dispatch on a split.
 */

/* -- D16 inbound data-plane decode ops ----------------------------- */

static moq_result_t d16_decode_subgroup_header_op(moq_session_t *s,
                                                   moq_buf_reader_t *r,
                                                   moq_decoded_subgroup_header_t *out)
{
    (void)s;
    moq_d16_subgroup_header_t hdr;
    moq_result_t rc = moq_d16_decode_subgroup_header(r, &hdr);
    if (rc < 0) return rc;

    out->track_alias = hdr.track_alias;
    out->group_id = hdr.group_id;
    out->publisher_priority = hdr.default_priority ? 128 : hdr.publisher_priority;
    out->has_extensions = hdr.has_extensions;
    out->end_of_group = hdr.end_of_group;
    switch (hdr.subgroup_id_mode) {
    case MOQ_SUBGROUP_ID_MODE_ZERO:
        out->subgroup_id = 0;
        out->subgroup_id_resolved = true;
        out->subgroup_id_from_first_object = false;
        break;
    case MOQ_SUBGROUP_ID_MODE_FIRST_OBJ:
        out->subgroup_id = 0;
        out->subgroup_id_resolved = false;
        out->subgroup_id_from_first_object = true;
        break;
    case MOQ_SUBGROUP_ID_MODE_PRESENT:
        out->subgroup_id = hdr.subgroup_id;
        out->subgroup_id_resolved = true;
        out->subgroup_id_from_first_object = false;
        break;
    }

    return MOQ_OK;
}

static moq_object_status_t d16_wire_status_to_semantic(uint8_t wire_status)
{
    switch (wire_status) {
    case 0x0: return MOQ_OBJECT_NORMAL;
    case 0x3: return MOQ_OBJECT_END_OF_GROUP;
    case 0x4: return MOQ_OBJECT_END_OF_TRACK;
    default:  return 0xFF;
    }
}

/* -- D16 outbound data-plane encode -------------------------------- */

static moq_result_t d16_encode_subgroup_header_op(moq_session_t *s,
                                                    moq_buf_writer_t *w,
                                                    const moq_subgroup_header_encode_args_t *args)
{
    (void)s;
    moq_d16_subgroup_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    uint8_t type = 0x14;   /* base: mode=PRESENT, no EOG, no default_prio */
    if (args->has_extensions)
        type |= MOQ_SUBGROUP_BIT_EXTENSIONS;
    if (args->end_of_group)
        type |= MOQ_SUBGROUP_BIT_END_OF_GROUP;
    hdr.type = type;
    hdr.has_extensions = args->has_extensions;
    hdr.end_of_group = args->end_of_group;
    hdr.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
    hdr.track_alias = args->track_alias;
    hdr.group_id = args->group_id;
    hdr.subgroup_id = args->subgroup_id;
    hdr.publisher_priority = args->publisher_priority;
    return moq_d16_encode_subgroup_header(w, &hdr);
}

static moq_result_t d16_encode_object_header_op(moq_session_t *s,
                                                  moq_buf_writer_t *w,
                                                  const moq_object_header_encode_args_t *args)
{
    (void)s;
    uint64_t delta;
    if (!args->has_prev_object) {
        delta = args->object_id;
    } else {
        delta = args->object_id - args->prev_object_id - 1;
    }

    if (!args->has_extensions) {
        /* delta + payload_len [+ object_status when len == 0]. Inlined (rather
         * than moq_d16_encode_object_header, which hardcodes NORMAL) so a
         * zero-length terminal object can carry args->object_status. */
        size_t saved0 = w->pos;
        moq_result_t rc0 = moq_buf_write_varint(w, delta);
        if (rc0 < 0) return rc0;
        rc0 = moq_buf_write_varint(w, args->payload_len);
        if (rc0 < 0) { w->pos = saved0; return rc0; }
        if (args->payload_len == 0) {
            rc0 = moq_buf_write_varint(w, args->object_status);
            if (rc0 < 0) { w->pos = saved0; return rc0; }
        }
        return MOQ_OK;
    }

    /* Extensions enabled: delta + extensions_len [+ payload_len]. */
    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_varint(w, delta);
    if (rc < 0) return rc;

    rc = moq_buf_write_varint(w, args->properties_len);
    if (rc < 0) { w->pos = saved; return rc; }

    if (!args->header_only) {
        rc = moq_buf_write_varint(w, args->payload_len);
        if (rc < 0) { w->pos = saved; return rc; }

        if (args->payload_len == 0 && args->properties_len == 0) {
            rc = moq_buf_write_varint(w, args->object_status);
            if (rc < 0) { w->pos = saved; return rc; }
        }
    }

    return MOQ_OK;
}

static moq_result_t d16_encode_object_payload_prefix_op(moq_session_t *s,
                                                         moq_buf_writer_t *w,
                                                         uint64_t payload_len,
                                                         bool with_status)
{
    (void)s;
    moq_result_t rc = moq_buf_write_varint(w, payload_len);
    if (rc < 0) return rc;
    if (with_status && payload_len == 0) {
        rc = moq_buf_write_varint(w, MOQ_OBJECT_STATUS_NORMAL);
        if (rc < 0) return rc;
    }
    return MOQ_OK;
}

/* -- D16 inbound data-plane decode --------------------------------- */

static moq_result_t d16_decode_object_header(moq_session_t *s,
                                              moq_buf_reader_t *r,
                                              bool has_extensions,
                                              uint64_t prev_object_id,
                                              bool has_prev_object,
                                              moq_decoded_object_header_t *out)
{
    (void)s;
    memset(out, 0, sizeof(*out));

    uint64_t delta;
    if (moq_buf_read_varint(r, &delta) < 0) return MOQ_ERR_BUFFER;

    if (has_extensions) {
        uint64_t ext_len;
        if (moq_buf_read_varint(r, &ext_len) < 0) return MOQ_ERR_BUFFER;
        if ((size_t)ext_len > moq_buf_reader_remaining(r))
            return MOQ_ERR_BUFFER;
        if (ext_len > 0) {
            const uint8_t *ext_ptr = moq_buf_reader_ptr(r);
            moq_kvp_decoder_t kvp_dec;
            moq_kvp_decoder_init(&kvp_dec, ext_ptr, (size_t)ext_len);
            moq_kvp_entry_t kvp_entry;
            moq_result_t kvp_rc;
            while ((kvp_rc = moq_kvp_decode_next(&kvp_dec, &kvp_entry)) == MOQ_OK)
                ;
            if (kvp_rc != MOQ_DONE) return MOQ_ERR_PROTO;
            out->has_properties = true;
            out->properties = ext_ptr;
            out->properties_len = (size_t)ext_len;
        }
        r->pos += (size_t)ext_len;
    }

    uint64_t payload_len;
    if (moq_buf_read_varint(r, &payload_len) < 0) return MOQ_ERR_BUFFER;
    out->payload_len = payload_len;

    uint8_t wire_status = 0;
    bool has_status = (payload_len == 0);
    if (has_status) {
        uint64_t st;
        if (moq_buf_read_varint(r, &st) < 0) return MOQ_ERR_BUFFER;
        wire_status = (uint8_t)st;
    }

    moq_object_status_t semantic = d16_wire_status_to_semantic(
        has_status ? wire_status : 0);
    if (semantic == 0xFF) return MOQ_ERR_PROTO;

    if (out->has_properties && semantic != MOQ_OBJECT_NORMAL)
        return MOQ_ERR_PROTO;

    out->status = semantic;

    if (!has_prev_object) {
        out->object_id = delta;
    } else {
        out->object_id = prev_object_id + 1 + delta;
        if (out->object_id <= prev_object_id)
            return MOQ_ERR_PROTO;
    }

    return MOQ_OK;
}

/*
 * Process buffered control data: decode D16 envelopes (varint type +
 * uint16 length) and dispatch each complete message. Returns the number
 * of bytes consumed in *out_consumed. Partial messages remain for the
 * next call.
 */
static moq_result_t d16_process_control_data(moq_session_t *s,
                                              const uint8_t *data, size_t len,
                                              size_t *out_consumed)
{
    size_t total_consumed = 0;

    while (total_consumed < len && s->state != MOQ_SESS_CLOSED) {
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, data + total_consumed, len - total_consumed);

        moq_control_envelope_t env;
        moq_result_t rc = moq_control_decode_envelope(&r, &env);
        if (rc == MOQ_ERR_BUFFER) break;
        if (rc < 0) {
            *out_consumed = total_consumed;
            return close_with_error(s, 0x3, "malformed control envelope");
        }

        rc = d16_handle_control_message(s, &env);
        if (rc < 0) {
            *out_consumed = total_consumed;
            return rc;
        }

        total_consumed += moq_buf_reader_offset(&r);
    }

    *out_consumed = total_consumed;
    return MOQ_OK;
}

/* -- D16 FETCH encode ops ----------------------------------------- */

static moq_result_t d16_encode_fetch(moq_session_t *s,
                                      moq_buf_writer_t *w,
                                      const moq_fetch_encode_args_t *args)
{
    uint8_t local_param_bufs[4][16];
    moq_kvp_entry_t local_params[4];
    size_t param_count = 0;

    moq_kvp_entry_t *params = local_params;
    size_t scratch_saved = s->output_scratch_len;

    if (args->auth_token_count > 0) {
        params = d16_alloc_auth_params(s, args->auth_tokens,
            args->auth_token_count, 4, &param_count);
        if (!params) {
            s->output_scratch_len = scratch_saved;
            return MOQ_ERR_BUFFER;
        }
    }

    size_t pbi = 0;
    if (args->subscriber_priority != 128) {
        size_t plen = moq_quic_varint_encode(args->subscriber_priority,
            local_param_bufs[pbi], sizeof(local_param_bufs[0]));
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_SUBSCRIBER_PRIORITY,
            .value = local_param_bufs[pbi], .value_len = plen,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++; pbi++;
    }

    if (args->group_order != MOQ_GROUP_ORDER_DEFAULT) {
        size_t plen = moq_quic_varint_encode(args->group_order,
            local_param_bufs[pbi], sizeof(local_param_bufs[0]));
        params[param_count] = (moq_kvp_entry_t){
            .type = MOQ_MSG_PARAM_GROUP_ORDER,
            .value = local_param_bufs[pbi], .value_len = plen,
            .is_varint = true, .raw = NULL, .raw_len = 0 };
        param_count++; pbi++;
    }

    moq_d16_fetch_t fetch = {
        .request_id = args->request_id,
        .fetch_type = args->fetch_type ? args->fetch_type : MOQ_D16_FETCH_TYPE_STANDALONE,
        .track_namespace = args->track_namespace,
        .track_name = args->track_name,
        .start_group = args->start_group,
        .start_object = args->start_object,
        .end_group = args->end_group,
        .end_object = args->end_object,
        .joining_request_id = args->joining_request_id,
        .joining_start = args->joining_start,
    };
    if (param_count > D16_MAX_MSG_PARAMS) {
        s->output_scratch_len = scratch_saved;
        return MOQ_ERR_INVAL;
    }
    moq_result_t rc = moq_d16_encode_fetch(w, &fetch, params, param_count);
    s->output_scratch_len = scratch_saved;
    return rc;
}

static moq_result_t d16_encode_fetch_cancel_op(moq_session_t *s,
                                                 moq_buf_writer_t *w,
                                                 uint64_t request_id)
{
    (void)s;
    return moq_d16_encode_varint_msg(w, MOQ_D16_FETCH_CANCEL, request_id);
}

/* -- D16 FETCH_OK / FETCH_HEADER encode + decode -------------------- */

static moq_result_t d16_encode_fetch_ok_op(moq_session_t *s,
                                             moq_buf_writer_t *w,
                                             const moq_accept_fetch_encode_args_t *args)
{
    (void)s;
    moq_d16_fetch_ok_t ok = {
        .request_id = args->request_id,
        .end_of_track = args->end_of_track ? 1 : 0,
        .end_group = args->end_group,
        .end_object = args->end_object,
        .params = NULL, .params_count = 0, .params_cap = 0,
        .track_extensions = NULL, .track_extensions_len = 0,
    };
    return moq_d16_encode_fetch_ok(w, &ok);
}

static moq_result_t d16_encode_fetch_header_op(moq_session_t *s,
                                                 moq_buf_writer_t *w,
                                                 uint64_t request_id)
{
    (void)s;
    return moq_d16_encode_fetch_header(w, request_id);
}

static moq_result_t d16_decode_fetch_header_op(moq_session_t *s,
                                                 moq_buf_reader_t *r,
                                                 moq_decoded_fetch_stream_header_t *out)
{
    (void)s;
    moq_d16_fetch_header_t hdr;
    moq_result_t rc = moq_d16_decode_fetch_header(r, &hdr);
    if (rc < 0) return rc;
    out->request_id = hdr.request_id;
    return MOQ_OK;
}

static uint32_t d16_classify_data_stream(const uint8_t *data, size_t len)
{
    if (len == 0) return MOQ_STREAM_KIND_UNKNOWN;
    uint8_t first = data[0];
    if (first == MOQ_D16_FETCH_HEADER_TYPE) return MOQ_STREAM_KIND_FETCH;
    if (moq_d16_subgroup_type_valid(first)) return MOQ_STREAM_KIND_SUBGROUP;
    return MOQ_STREAM_KIND_UNKNOWN;
}

/* -- D16 fetch data-plane decode/encode ----------------------------- */

static moq_result_t d16_decode_fetch_object_op(moq_session_t *s,
                                                 moq_buf_reader_t *r,
                                                 const moq_fetch_prior_object_t *prev,
                                                 moq_decoded_fetch_object_t *out)
{
    (void)s;
    const moq_d16_fetch_object_t *wire_prev = NULL;
    moq_d16_fetch_object_t prev_wire;
    /* Build a prior object only from an actual prior object (has_actual), not
     * from a bare End-of-Range location (has_prev) -- preserves draft-16's
     * leading-marker behavior now that the two are tracked separately. */
    if (prev && prev->has_actual) {
        memset(&prev_wire, 0, sizeof(prev_wire));
        prev_wire.group_id = prev->group_id;
        prev_wire.subgroup_id = prev->subgroup_id;
        prev_wire.object_id = prev->object_id;
        prev_wire.publisher_priority = prev->publisher_priority;
        wire_prev = &prev_wire;
    }

    moq_d16_fetch_object_t wire_out;
    moq_result_t rc = moq_d16_decode_fetch_object(r, wire_prev, &wire_out);
    if (rc < 0) return rc;

    memset(out, 0, sizeof(*out));
    out->is_range_marker = wire_out.is_end_of_range;
    if (wire_out.is_end_of_range) {
        out->range_kind = (wire_out.range_kind == MOQ_D16_FETCH_END_NON_EXISTENT)
            ? MOQ_FETCH_RANGE_NON_EXISTENT : MOQ_FETCH_RANGE_UNKNOWN;
    }
    out->group_id = wire_out.group_id;
    out->subgroup_id = wire_out.subgroup_id;
    out->object_id = wire_out.object_id;
    out->publisher_priority = wire_out.publisher_priority;
    out->payload_len = wire_out.payload_len;
    out->properties = wire_out.extensions;
    out->properties_len = wire_out.extensions_len;
    return MOQ_OK;
}

static moq_result_t d16_encode_fetch_object_op(moq_session_t *s,
                                                 moq_buf_writer_t *w,
                                                 const moq_fetch_object_encode_args_t *args,
                                                 const moq_fetch_prior_object_t *prev)
{
    (void)s;
    /* draft-16 fetch-object serialization has no Datagram forwarding-preference
     * bit; refuse rather than silently dropping the requested preference. */
    if (args->datagram) return MOQ_ERR_INVAL;
    const moq_d16_fetch_object_t *wire_prev = NULL;
    moq_d16_fetch_object_t prev_wire;
    if (prev && prev->has_actual) {
        memset(&prev_wire, 0, sizeof(prev_wire));
        prev_wire.group_id = prev->group_id;
        prev_wire.subgroup_id = prev->subgroup_id;
        prev_wire.object_id = prev->object_id;
        prev_wire.publisher_priority = prev->publisher_priority;
        wire_prev = &prev_wire;
    }

    if (args->header_only) {
        /* Encode flags + fields + extensions_len only. Extension bytes
         * and payload_len are sent in separate SEND_DATA actions. */
        uint64_t flags = 0;
        if (!wire_prev) {
            flags |= MOQ_D16_FETCH_FLAG_GROUP_ID | MOQ_D16_FETCH_FLAG_OBJECT_ID |
                     MOQ_D16_FETCH_FLAG_PRIORITY;
            if (args->subgroup_id != 0) flags |= 0x03;
        } else {
            if (args->group_id != wire_prev->group_id)
                flags |= MOQ_D16_FETCH_FLAG_GROUP_ID;
            if (args->object_id != wire_prev->object_id + 1)
                flags |= MOQ_D16_FETCH_FLAG_OBJECT_ID;
            if (args->publisher_priority != wire_prev->publisher_priority)
                flags |= MOQ_D16_FETCH_FLAG_PRIORITY;
            if (args->subgroup_id == 0) { /* mode 0 */ }
            else if (args->subgroup_id == wire_prev->subgroup_id) flags |= 0x01;
            else if (args->subgroup_id == wire_prev->subgroup_id + 1) flags |= 0x02;
            else flags |= 0x03;
        }
        if (args->properties_len > 0)
            flags |= MOQ_D16_FETCH_FLAG_EXTENSIONS;

        size_t saved = w->pos;
        moq_result_t rc = moq_buf_write_varint(w, flags);
        if (rc < 0) return rc;
        if (flags & MOQ_D16_FETCH_FLAG_GROUP_ID) {
            rc = moq_buf_write_varint(w, args->group_id);
            if (rc < 0) { w->pos = saved; return rc; }
        }
        if ((flags & MOQ_D16_FETCH_FLAG_SUBGROUP_MASK) == 0x03) {
            rc = moq_buf_write_varint(w, args->subgroup_id);
            if (rc < 0) { w->pos = saved; return rc; }
        }
        if (flags & MOQ_D16_FETCH_FLAG_OBJECT_ID) {
            rc = moq_buf_write_varint(w, args->object_id);
            if (rc < 0) { w->pos = saved; return rc; }
        }
        if (flags & MOQ_D16_FETCH_FLAG_PRIORITY) {
            rc = moq_buf_write_raw(w, &args->publisher_priority, 1);
            if (rc < 0) { w->pos = saved; return rc; }
        }
        if (flags & MOQ_D16_FETCH_FLAG_EXTENSIONS) {
            rc = moq_buf_write_varint(w, (uint64_t)args->properties_len);
            if (rc < 0) { w->pos = saved; return rc; }
        }
        return MOQ_OK;
    }

    /* No extensions: encode flags + fields, rewrite payload_len. */
    moq_d16_fetch_object_t obj;
    memset(&obj, 0, sizeof(obj));
    obj.group_id = args->group_id;
    obj.subgroup_id = args->subgroup_id;
    obj.object_id = args->object_id;
    obj.publisher_priority = args->publisher_priority;
    obj.payload_len = 0;
    obj.payload = NULL;
    moq_result_t rc = moq_d16_encode_fetch_object(w, &obj, wire_prev);
    if (rc < 0) return rc;

    w->pos--;
    return moq_buf_write_varint(w, args->payload_len);
}

static moq_result_t d16_encode_fetch_range_op(moq_session_t *s,
                                                moq_buf_writer_t *w,
                                                uint32_t range_kind,
                                                uint64_t group_id,
                                                uint64_t object_id)
{
    (void)s;
    uint32_t wire_kind;
    if (range_kind == MOQ_FETCH_RANGE_NON_EXISTENT)
        wire_kind = MOQ_D16_FETCH_END_NON_EXISTENT;
    else if (range_kind == MOQ_FETCH_RANGE_UNKNOWN)
        wire_kind = MOQ_D16_FETCH_END_UNKNOWN;
    else
        return MOQ_ERR_INVAL;
    return moq_d16_encode_fetch_end_of_range(w, wire_kind, group_id, object_id);
}

/* -- D16 datagram ops ---------------------------------------------- */

static moq_result_t d16_encode_object_datagram_op(moq_session_t *s,
                                                    moq_buf_writer_t *w,
                                                    const moq_datagram_encode_args_t *args)
{
    (void)s;
    moq_d16_object_datagram_t dg;
    memset(&dg, 0, sizeof(dg));
    dg.track_alias        = args->track_alias;
    dg.group_id           = args->group_id;
    dg.object_id          = args->object_id;
    dg.publisher_priority = args->publisher_priority;
    dg.default_priority   = args->default_priority;
    dg.end_of_group       = args->end_of_group;
    dg.is_status          = args->is_status;
    dg.object_status      = args->object_status;
    if (args->properties_len > 0) {
        dg.has_extensions = true;
        dg.extensions     = args->properties;
        dg.extensions_len = args->properties_len;
    }
    dg.payload     = args->payload;
    dg.payload_len = args->payload_len;
    return moq_d16_encode_object_datagram(w, &dg);
}

static moq_result_t d16_decode_object_datagram_op(moq_session_t *s,
                                                    const uint8_t *data,
                                                    size_t len,
                                                    moq_decoded_object_datagram_t *out)
{
    memset(out, 0, sizeof(*out));

    moq_d16_object_datagram_t dg;
    moq_result_t rc = moq_d16_decode_object_datagram(data, len, &dg);
    if (rc < 0) return rc;

    int sub_slot = sub_find_by_alias_subscriber(s, dg.track_alias);
    int pub_slot = -1;
    if (sub_slot < 0)
        pub_slot = pub_find_by_alias_subscriber(s, dg.track_alias);
    if (sub_slot < 0 && pub_slot < 0) {
        /* Parsed cleanly but no established alias: surface the alias so the
         * caller can hold the datagram for control/data reordering. */
        out->track_alias   = dg.track_alias;
        out->unknown_alias = true;
        return MOQ_DONE;
    }

    out->sub_slot          = sub_slot;
    out->pub_slot          = pub_slot;
    out->group_id          = dg.group_id;
    out->object_id         = dg.object_id;
    out->publisher_priority = dg.publisher_priority;
    out->default_priority  = dg.default_priority;
    out->end_of_group      = dg.end_of_group;
    out->is_status         = dg.is_status;
    if (dg.is_status) {
        if (dg.object_status == MOQ_OBJECT_STATUS_END_OF_GROUP)
            out->status = MOQ_OBJECT_END_OF_GROUP;
        else if (dg.object_status == MOQ_OBJECT_STATUS_END_OF_TRACK)
            out->status = MOQ_OBJECT_END_OF_TRACK;
        else
            out->status = MOQ_OBJECT_NORMAL;
    } else {
        out->status = MOQ_OBJECT_NORMAL;
    }
    if (dg.has_extensions) {
        out->properties     = dg.extensions;
        out->properties_len = dg.extensions_len;
    }
    out->payload     = dg.payload;
    out->payload_len = dg.payload_len;
    return MOQ_OK;
}

/* ==================== REGION: D16 PROFILE OPS TABLE ==================== */
/*
 * d16_ops is the D16 draft boundary - the seam the session core binds to
 * (via moq_profile_lookup). On a future physical split, d16_ops stays the
 * D16 assembly point: the per-region .c files supply the ops and this
 * table wires them up. A second draft is added as a sibling d18_ops and
 * must not alter D16 ops semantics. moq_profile_lookup() (just below) may
 * move to a neutral profile registry file once more than one profile
 * exists; today it lives here because D16 is the only profile.
 */

/* -- D16 profile ops table ----------------------------------------- */

static moq_result_t d16_classify_bidi_stream(moq_session_t *s,
                                               moq_stream_ref_t ref,
                                               const uint8_t *data,
                                               size_t len)
{
    if (len == 0) return MOQ_OK;

    uint64_t msg_type;
    size_t n = moq_quic_varint_decode(data, len, &msg_type);
    if (n == 0)
        return close_with_error(s, 0x3,
            "incomplete varint on bidi request stream");

    if (msg_type == MOQ_D16_SUBSCRIBE_NAMESPACE)
        return ns_sub_on_new_bidi(s, ref, data, len);

    return close_with_error(s, 0x3,
        "unsupported bidi request stream type");
}

static const moq_profile_ops_t d16_ops = {
    .state_size             = sizeof(moq_d16_profile_state_t),
    .state_align            = _Alignof(moq_d16_profile_state_t),
    .init_in_place          = d16_init_in_place,
    .destroy                = d16_destroy,
    .start                  = d16_start,
    .process_control_data   = d16_process_control_data,
    .has_request_capacity   = d16_has_request_capacity,
    .prepare_request        = d16_prepare_request,
    .commit_request         = d16_commit_request,
    .abort_request          = d16_abort_request,
    .validate_inbound_request  = d16_validate_inbound_request,
    .commit_inbound_request    = d16_commit_inbound_request,
    .release_request        = d16_release_request,
    .grant_capacity         = d16_grant_capacity,
    .peer_request_capacity  = d16_peer_request_capacity,
    .local_request_capacity = d16_local_request_capacity,
    .next_track_alias       = d16_next_track_alias,
    .advance_track_alias    = d16_advance_track_alias,
    .encode_subscribe       = d16_encode_subscribe,
    .encode_subscribe_ok    = d16_encode_subscribe_ok,
    .encode_request_ok      = d16_encode_request_ok,
    .encode_request_error   = d16_encode_request_error,
    .encode_request_update  = d16_encode_request_update_op,
    .encode_unsubscribe     = d16_encode_unsubscribe,
    .encode_fetch           = d16_encode_fetch,
    .encode_fetch_cancel    = d16_encode_fetch_cancel_op,
    .encode_fetch_ok        = d16_encode_fetch_ok_op,
    .encode_fetch_header    = d16_encode_fetch_header_op,
    .decode_fetch_header    = d16_decode_fetch_header_op,
    .classify_data_stream   = d16_classify_data_stream,
    .classify_bidi_stream   = d16_classify_bidi_stream,
    .decode_fetch_object    = d16_decode_fetch_object_op,
    .encode_fetch_object    = d16_encode_fetch_object_op,
    .encode_fetch_range     = d16_encode_fetch_range_op,
    .encode_goaway          = d16_encode_goaway,
    .encode_publish         = d16_encode_publish_op,
    .track_properties_dynamic_groups = d16_track_properties_dynamic_groups,
    .encode_publish_ok      = d16_encode_publish_ok_op,
    .encode_publish_done    = d16_encode_publish_done_op,
    .encode_publish_namespace       = d16_encode_publish_namespace_op,
    .encode_publish_namespace_done  = d16_encode_publish_namespace_done_op,
    .encode_publish_namespace_cancel = d16_encode_publish_namespace_cancel_op,
    .encode_subscribe_namespace = d16_encode_subscribe_namespace,
    .encode_namespace_msg       = d16_encode_namespace_msg,
    .decode_ns_sub_request      = d16_decode_ns_sub_request,
    .decode_ns_sub_response     = d16_decode_ns_sub_response,
    .decode_subgroup_header     = d16_decode_subgroup_header_op,
    .encode_subgroup_header     = d16_encode_subgroup_header_op,
    .encode_object_header       = d16_encode_object_header_op,
    .encode_object_payload_prefix = d16_encode_object_payload_prefix_op,
    /* .validate_object_properties left NULL: draft-16 has no forbidden range. */
    .decode_object_header       = d16_decode_object_header,
    .encode_track_status        = d16_encode_track_status_op,
    .encode_track_status_ok     = d16_encode_track_status_ok_op,
    .encode_object_datagram     = d16_encode_object_datagram_op,
    .decode_object_datagram     = d16_decode_object_datagram_op,
    /* Draft-16 correlates requests by wire request-id on a bidirectional
     * control channel, so it does not use per-request bidi streams and has no
     * unidirectional control channel. The capability stays false and both
     * stream-correlation hooks stay NULL (the core then treats every peer
     * unidirectional stream as data and never invokes the request-stream
     * validator). */
    .uses_request_streams            = false,
    .fetch_descending_supported      = true,   /* absolute Group IDs on the wire */
    .uses_uni_control_channel        = false,
    .classify_uni_stream             = NULL,
    .validate_inbound_request_stream = NULL,
};

const moq_profile_ops_t *moq_profile_lookup(moq_version_t version)
{
    switch (version) {
    case MOQ_VERSION_DRAFT_16:
        return &d16_ops;
    case MOQ_VERSION_DRAFT_18:
        return moq_d18_profile_ops();
    default:
        return NULL;
    }
}
