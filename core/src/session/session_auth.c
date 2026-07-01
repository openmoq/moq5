#include "session_internal.h"

static void free_staging_internal(moq_session_t *s,
    moq_resolved_token_t *tokens, const bool *staged, size_t count);

moq_result_t moq_token_cache_init(moq_token_cache_t *cache,
                                   const moq_alloc_t *alloc,
                                   size_t max_bytes,
                                   size_t max_entries)
{
    if (!cache || !alloc) return MOQ_ERR_INVAL;
    memset(cache, 0, sizeof(*cache));
    cache->alloc = *alloc;
    cache->max_bytes = max_bytes;

    if (max_entries == 0)
        max_entries = MOQ_DEFAULT_MAX_TOKEN_CACHE_ENTRIES;

    /* Overflow guard: reject an entry count whose byte size would wrap
     * size_t. The cache was already zeroed above, so it stays safe/empty
     * (entries NULL, cap 0) and a later free is a no-op. */
    if (max_entries > SIZE_MAX / sizeof(moq_token_cache_entry_t))
        return MOQ_ERR_INVAL;
    size_t entries_bytes = max_entries * sizeof(moq_token_cache_entry_t);

    cache->entries = (moq_token_cache_entry_t *)alloc->alloc(
        entries_bytes, alloc->ctx);
    if (!cache->entries) return MOQ_ERR_NOMEM;

    memset(cache->entries, 0, entries_bytes);
    cache->cap = max_entries;
    return MOQ_OK;
}

void moq_token_cache_free(moq_token_cache_t *cache)
{
    if (!cache || !cache->entries) return;
    for (size_t i = 0; i < cache->cap; i++) {
        moq_token_cache_entry_t *e = &cache->entries[i];
        if (e->active && e->token_value) {
            cache->alloc.free(e->token_value, e->token_value_len,
                              cache->alloc.ctx);
        }
    }
    cache->alloc.free(cache->entries,
                       cache->cap * sizeof(moq_token_cache_entry_t),
                       cache->alloc.ctx);
    cache->entries = NULL;
    cache->cap = 0;
    cache->used_bytes = 0;
}

int moq_token_cache_register(moq_token_cache_t *cache,
                              uint64_t alias, uint64_t token_type,
                              const uint8_t *value, size_t value_len)
{
    if (!cache || !cache->entries) return MOQ_TOKEN_ERR_NOMEM;

    for (size_t i = 0; i < cache->cap; i++) {
        if (cache->entries[i].active && cache->entries[i].alias == alias)
            return MOQ_TOKEN_ERR_DUPLICATE;
    }

    size_t entry_size = MOQ_TOKEN_ENTRY_OVERHEAD + value_len;
    if (entry_size < MOQ_TOKEN_ENTRY_OVERHEAD) return MOQ_TOKEN_ERR_OVERFLOW;
    if (cache->used_bytes > cache->max_bytes ||
        entry_size > cache->max_bytes - cache->used_bytes)
        return MOQ_TOKEN_ERR_OVERFLOW;

    int slot = -1;
    for (size_t i = 0; i < cache->cap; i++) {
        if (!cache->entries[i].active) { slot = (int)i; break; }
    }
    if (slot < 0) return MOQ_TOKEN_ERR_OVERFLOW;

    uint8_t *val_copy = NULL;
    if (value_len > 0) {
        if (!value) return MOQ_TOKEN_ERR_NOMEM;
        val_copy = (uint8_t *)cache->alloc.alloc(value_len, cache->alloc.ctx);
        if (!val_copy) return MOQ_TOKEN_ERR_NOMEM;
        memcpy(val_copy, value, value_len);
    }

    moq_token_cache_entry_t *e = &cache->entries[slot];
    e->active = true;
    e->alias = alias;
    e->token_type = token_type;
    e->token_value = val_copy;
    e->token_value_len = value_len;
    cache->used_bytes += entry_size;
    return MOQ_TOKEN_OK;
}

int moq_token_cache_register_preowned(moq_token_cache_t *cache,
                                      uint64_t alias, uint64_t token_type,
                                      uint8_t *value, size_t value_len)
{
    if (!cache || !cache->entries) return MOQ_TOKEN_ERR_NOMEM;

    for (size_t i = 0; i < cache->cap; i++) {
        if (cache->entries[i].active && cache->entries[i].alias == alias)
            return MOQ_TOKEN_ERR_DUPLICATE;
    }

    size_t entry_size = MOQ_TOKEN_ENTRY_OVERHEAD + value_len;
    if (entry_size < MOQ_TOKEN_ENTRY_OVERHEAD) return MOQ_TOKEN_ERR_OVERFLOW;
    if (cache->used_bytes > cache->max_bytes ||
        entry_size > cache->max_bytes - cache->used_bytes)
        return MOQ_TOKEN_ERR_OVERFLOW;

    int slot = -1;
    for (size_t i = 0; i < cache->cap; i++) {
        if (!cache->entries[i].active) { slot = (int)i; break; }
    }
    if (slot < 0) return MOQ_TOKEN_ERR_OVERFLOW;

    moq_token_cache_entry_t *e = &cache->entries[slot];
    e->active = true;
    e->alias = alias;
    e->token_type = token_type;
    e->token_value = value;
    e->token_value_len = value_len;
    cache->used_bytes += entry_size;
    return MOQ_TOKEN_OK;
}

int moq_token_cache_delete(moq_token_cache_t *cache, uint64_t alias)
{
    if (!cache || !cache->entries) return MOQ_TOKEN_ERR_UNKNOWN;

    for (size_t i = 0; i < cache->cap; i++) {
        moq_token_cache_entry_t *e = &cache->entries[i];
        if (e->active && e->alias == alias) {
            size_t entry_size = MOQ_TOKEN_ENTRY_OVERHEAD + e->token_value_len;
            if (entry_size <= cache->used_bytes)
                cache->used_bytes -= entry_size;
            else
                cache->used_bytes = 0;
            if (e->token_value)
                cache->alloc.free(e->token_value, e->token_value_len,
                                  cache->alloc.ctx);
            memset(e, 0, sizeof(*e));
            return MOQ_TOKEN_OK;
        }
    }
    return MOQ_TOKEN_ERR_UNKNOWN;
}

int moq_token_cache_lookup(const moq_token_cache_t *cache,
                            uint64_t alias,
                            uint64_t *out_type,
                            const uint8_t **out_value,
                            size_t *out_value_len)
{
    if (!cache || !cache->entries) return MOQ_TOKEN_ERR_UNKNOWN;

    for (size_t i = 0; i < cache->cap; i++) {
        const moq_token_cache_entry_t *e = &cache->entries[i];
        if (e->active && e->alias == alias) {
            if (out_type) *out_type = e->token_type;
            if (out_value) *out_value = e->token_value;
            if (out_value_len) *out_value_len = e->token_value_len;
            return MOQ_TOKEN_OK;
        }
    }
    return MOQ_TOKEN_ERR_UNKNOWN;
}

/* -- Cache overlay ------------------------------------------------- */

void moq_ov_init(moq_cache_overlay_t *ov, const moq_token_cache_t *cache)
{
    memset(ov->entries, 0, sizeof(ov->entries));
    ov->count = 0;
    ov->cache = cache;
}

void moq_ov_free_preowned(moq_cache_overlay_t *ov, const moq_alloc_t *alloc)
{
    for (size_t i = 0; i < ov->count; i++) {
        if (ov->entries[i].preowned) {
            alloc->free(ov->entries[i].preowned, ov->entries[i].value_len,
                        alloc->ctx);
            ov->entries[i].preowned = NULL;
        }
    }
}

bool moq_ov_alias_live(const moq_cache_overlay_t *ov, uint64_t alias,
                        uint64_t *out_type, const uint8_t **out_val,
                        size_t *out_len)
{
    for (size_t i = ov->count; i > 0; i--) {
        const moq_ov_entry_t *e = &ov->entries[i - 1];
        if (e->alias != alias) continue;
        if (e->kind == MOQ_OV_DELETE) return false;
        if (out_type) *out_type = e->token_type;
        if (out_val)  *out_val  = e->value;
        if (out_len)  *out_len  = e->value_len;
        return true;
    }
    return moq_token_cache_lookup(ov->cache, alias, out_type, out_val, out_len)
           == MOQ_TOKEN_OK;
}

void moq_ov_project(const moq_cache_overlay_t *ov,
                     size_t *out_bytes, size_t *out_active)
{
    size_t bytes = ov->cache->used_bytes;
    size_t active = 0;
    for (size_t i = 0; i < ov->cache->cap; i++)
        if (ov->cache->entries[i].active) active++;

    for (size_t i = 0; i < ov->count; i++) {
        const moq_ov_entry_t *e = &ov->entries[i];
        if (e->kind == MOQ_OV_REGISTER) {
            size_t entry_size = MOQ_TOKEN_ENTRY_OVERHEAD + e->value_len;
            if (entry_size < MOQ_TOKEN_ENTRY_OVERHEAD) {
                bytes = SIZE_MAX;
                active++;
                continue;
            }
            if (bytes > SIZE_MAX - entry_size)
                bytes = SIZE_MAX;
            else
                bytes += entry_size;
            active++;
        } else {
            size_t del_size = MOQ_TOKEN_ENTRY_OVERHEAD;
            bool found = false;
            for (size_t j = i; j > 0; j--) {
                if (ov->entries[j - 1].alias == e->alias &&
                    ov->entries[j - 1].kind == MOQ_OV_REGISTER) {
                    del_size += ov->entries[j - 1].value_len;
                    found = true;
                    break;
                }
            }
            if (!found) {
                size_t vlen;
                if (moq_token_cache_lookup(ov->cache, e->alias,
                        NULL, NULL, &vlen) == MOQ_TOKEN_OK)
                    del_size += vlen;
            }
            if (del_size <= bytes) bytes -= del_size;
            else bytes = 0;
            if (active > 0) active--;
        }
    }
    *out_bytes = bytes;
    *out_active = active;
}

/* -- Semantic token-value validation -------------------------------- */

/*
 * See session_internal.h. The rule is intentionally minimal (zero-length
 * or NUL-containing resolved values are malformed) and deterministic on
 * the value alone: a registered alias whose value fails here fails
 * identically on every future USE_ALIAS, which is what lets callers keep
 * the spec's REGISTER-even-on-reject behavior without re-validating
 * cache state.
 */
bool moq_auth_token_value_semantically_valid(const uint8_t *value,
                                             size_t value_len)
{
    if (value_len == 0)
        return false;
    return memchr(value, 0x00, value_len) == NULL;
}

/* -- Shared request-level AUTH_TOKEN processing -------------------- */

/*
 * Staging: USE_ALIAS resolves a token value from the cache. That pointer
 * borrows cache memory which a later DELETE in the same message could
 * free. To avoid use-after-free, we copy the resolved value into a
 * temporary allocator-owned buffer ("staged copy"). Callers copy staged
 * values into output scratch when building the event, then call
 * process_auth_tokens_free_staging() to release all staging buffers.
 *
 * Staged copies are tracked via a flag in the out_tokens array: if
 * token_value.data was staged (USE_ALIAS), the caller must free it.
 * USE_VALUE borrows from the decode buffer (no staging needed, valid
 * for the duration of the call).
 */

moq_result_t process_auth_tokens(moq_session_t *s,
    const moq_decoded_auth_token_t *tokens_in, size_t tokens_in_count,
    moq_resolved_token_t *out_tokens, size_t *out_token_count,
    size_t max_tokens,
    uint64_t *out_reject_code,
    bool *out_staged,
    moq_auth_txn_t *txn)
{
    size_t token_count = 0;
    uint64_t reject_code = 0;

    if (out_staged)
        memset(out_staged, 0, max_tokens * sizeof(bool));

    bool use_txn = (txn != NULL);
    if (use_txn)
        moq_ov_init(&txn->ov, &s->peer_token_cache);

    for (size_t i = 0; i < tokens_in_count; i++) {
        const moq_decoded_auth_token_t *tok_in = &tokens_in[i];

        /* Local mutable copy for resolved value tracking. */
        uint64_t tok_type = tok_in->token_type;
        const uint8_t *tok_value = tok_in->token_value;
        size_t tok_value_len = tok_in->token_value_len;
        moq_auth_op_t tok_op = tok_in->op;

        switch (tok_op) {
        case MOQ_AUTH_OP_DELETE: {
            if (use_txn) {
                if (!moq_ov_alias_live(&txn->ov, tok_in->alias, NULL, NULL, NULL)) {
                    if (!reject_code) reject_code = 0x17;
                } else if (txn->ov.count < MOQ_CACHE_OVERLAY_CAP) {
                    txn->ov.entries[txn->ov.count].kind = MOQ_OV_DELETE;
                    txn->ov.entries[txn->ov.count].alias = tok_in->alias;
                    txn->ov.entries[txn->ov.count].token_type = 0;
                    txn->ov.entries[txn->ov.count].value = NULL;
                    txn->ov.entries[txn->ov.count].value_len = 0;
                    txn->ov.entries[txn->ov.count].preowned = NULL;
                    txn->ov.count++;
                }
            } else {
                int drc = moq_token_cache_delete(&s->peer_token_cache, tok_in->alias);
                if (drc == MOQ_TOKEN_ERR_UNKNOWN && !reject_code)
                    reject_code = 0x17;
            }
            break;
        }
        case MOQ_AUTH_OP_REGISTER: {
            if (use_txn) {
                if (moq_ov_alias_live(&txn->ov, tok_in->alias, NULL, NULL, NULL)) {
                    free_staging_internal(s, out_tokens, out_staged, token_count);
                    moq_ov_free_preowned(&txn->ov, &s->alloc);
                    return close_with_error(s, 0x14, "duplicate auth token alias");
                }
                size_t projected_bytes, projected_active;
                moq_ov_project(&txn->ov, &projected_bytes, &projected_active);
                size_t entry_size = MOQ_TOKEN_ENTRY_OVERHEAD + tok_value_len;
                if (entry_size < MOQ_TOKEN_ENTRY_OVERHEAD ||
                    projected_bytes > s->peer_token_cache.max_bytes ||
                    entry_size > s->peer_token_cache.max_bytes - projected_bytes ||
                    projected_active >= s->peer_token_cache.cap) {
                    free_staging_internal(s, out_tokens, out_staged, token_count);
                    moq_ov_free_preowned(&txn->ov, &s->alloc);
                    return close_with_error(s, 0x13, "auth token cache overflow");
                }
                if (txn->ov.count < MOQ_CACHE_OVERLAY_CAP) {
                    txn->ov.entries[txn->ov.count].kind = MOQ_OV_REGISTER;
                    txn->ov.entries[txn->ov.count].alias = tok_in->alias;
                    txn->ov.entries[txn->ov.count].token_type = tok_type;
                    txn->ov.entries[txn->ov.count].value = tok_value;
                    txn->ov.entries[txn->ov.count].value_len = tok_value_len;
                    txn->ov.entries[txn->ov.count].preowned = NULL;
                    txn->ov.count++;
                }
            } else {
                int rrc = moq_token_cache_register(&s->peer_token_cache,
                    tok_in->alias, tok_type, tok_value, tok_value_len);
                if (rrc == MOQ_TOKEN_ERR_DUPLICATE) {
                    free_staging_internal(s, out_tokens, out_staged, token_count);
                    return close_with_error(s, 0x14, "duplicate auth token alias");
                }
                if (rrc == MOQ_TOKEN_ERR_OVERFLOW) {
                    free_staging_internal(s, out_tokens, out_staged, token_count);
                    return close_with_error(s, 0x13, "auth token cache overflow");
                }
                if (rrc == MOQ_TOKEN_ERR_NOMEM) {
                    free_staging_internal(s, out_tokens, out_staged, token_count);
                    return MOQ_ERR_NOMEM;
                }
            }
            break;
        }
        case MOQ_AUTH_OP_USE_ALIAS: {
            if (reject_code) break;
            uint64_t rtype;
            const uint8_t *rval;
            size_t rlen;
            bool found;
            if (use_txn) {
                found = moq_ov_alias_live(&txn->ov, tok_in->alias,
                    &rtype, &rval, &rlen);
            } else {
                found = (moq_token_cache_lookup(&s->peer_token_cache, tok_in->alias,
                    &rtype, &rval, &rlen) == MOQ_TOKEN_OK);
            }
            if (!found) {
                reject_code = 0x17;
            } else {
                tok_type = rtype;
                tok_value_len = rlen;
                if (rlen > 0 && rval) {
                    uint8_t *staged_val = (uint8_t *)s->alloc.alloc(rlen,
                                                                     s->alloc.ctx);
                    if (!staged_val) {
                        free_staging_internal(s, out_tokens, out_staged,
                                              token_count);
                        if (use_txn) moq_ov_free_preowned(&txn->ov, &s->alloc);
                        return MOQ_ERR_NOMEM;
                    }
                    memcpy(staged_val, rval, rlen);
                    tok_value = staged_val;
                } else {
                    tok_value = NULL;
                }
            }
            break;
        }
        case MOQ_AUTH_OP_USE_VALUE:
            break;
        }

        if (reject_code) continue;
        if (tok_op == MOQ_AUTH_OP_DELETE) continue;

        /* Semantic validation of the RESOLVED value (well-formed structure,
         * otherwise invalid => MALFORMED_AUTH_TOKEN, a request-level reject).
         * A REGISTER's overlay entry above is intentionally kept: the spec
         * requires the alias to register even when the message is rejected,
         * and the reject path commits the txn. A future USE_ALIAS of that
         * alias resolves to the same value and fails here again. */
        if (!moq_auth_token_value_semantically_valid(tok_value,
                                                     tok_value_len)) {
            if (tok_op == MOQ_AUTH_OP_USE_ALIAS &&
                tok_value && tok_value_len > 0)
                s->alloc.free((void *)(uintptr_t)tok_value,
                              tok_value_len, s->alloc.ctx);
            reject_code = 0x4;     /* MALFORMED_AUTH_TOKEN (request error) */
            continue;
        }

        /* Check duplicate resolved (type, value). */
        for (size_t j = 0; j < token_count; j++) {
            if (out_tokens[j].token_type == tok_type &&
                out_tokens[j].token_value.len == tok_value_len &&
                (tok_value_len == 0 ||
                 memcmp(out_tokens[j].token_value.data,
                        tok_value, tok_value_len) == 0)) {
                if (tok_op == MOQ_AUTH_OP_USE_ALIAS &&
                    tok_value && tok_value_len > 0)
                    s->alloc.free((void *)(uintptr_t)tok_value,
                                  tok_value_len, s->alloc.ctx);
                reject_code = 0x4;
                break;
            }
        }
        if (reject_code) continue;

        if (token_count < max_tokens) {
            out_tokens[token_count].token_type = tok_type;
            out_tokens[token_count].token_value.data = tok_value;
            out_tokens[token_count].token_value.len = tok_value_len;
            if (out_staged)
                out_staged[token_count] =
                    (tok_op == MOQ_AUTH_OP_USE_ALIAS &&
                     tok_value != NULL && tok_value_len > 0);
            token_count++;
        }
    }

    /* Pre-allocate cache value copies for staged REGISTERs. */
    if (use_txn) {
        for (size_t i = 0; i < txn->ov.count; i++) {
            if (txn->ov.entries[i].kind != MOQ_OV_REGISTER) continue;
            if (txn->ov.entries[i].value_len > 0) {
                uint8_t *p = (uint8_t *)s->alloc.alloc(
                    txn->ov.entries[i].value_len, s->alloc.ctx);
                if (!p) {
                    free_staging_internal(s, out_tokens, out_staged, token_count);
                    moq_ov_free_preowned(&txn->ov, &s->alloc);
                    return MOQ_ERR_NOMEM;
                }
                memcpy(p, txn->ov.entries[i].value, txn->ov.entries[i].value_len);
                txn->ov.entries[i].preowned = p;
            }
        }
    }

    *out_token_count = token_count;
    *out_reject_code = reject_code;

    if (reject_code) {
        free_staging_internal(s, out_tokens, out_staged, token_count);
    }

    return MOQ_OK;
}

void process_auth_tokens_commit_txn(moq_session_t *s, moq_auth_txn_t *txn)
{
    for (size_t i = 0; i < txn->ov.count; i++) {
        if (txn->ov.entries[i].kind == MOQ_OV_REGISTER) {
            int rrc = moq_token_cache_register_preowned(&s->peer_token_cache,
                txn->ov.entries[i].alias, txn->ov.entries[i].token_type,
                txn->ov.entries[i].preowned, txn->ov.entries[i].value_len);
            if (rrc == MOQ_TOKEN_OK) {
                txn->ov.entries[i].preowned = NULL;
            } else {
                if (txn->ov.entries[i].preowned)
                    s->alloc.free(txn->ov.entries[i].preowned,
                                  txn->ov.entries[i].value_len, s->alloc.ctx);
                txn->ov.entries[i].preowned = NULL;
            }
        } else {
            moq_token_cache_delete(&s->peer_token_cache,
                                    txn->ov.entries[i].alias);
        }
    }
}

void process_auth_tokens_abort_txn(moq_session_t *s, moq_auth_txn_t *txn)
{
    moq_ov_free_preowned(&txn->ov, &s->alloc);
}

static void free_staging_internal(moq_session_t *s,
    moq_resolved_token_t *tokens, const bool *staged, size_t count)
{
    if (!staged) return;
    for (size_t i = 0; i < count; i++) {
        if (staged[i] && tokens[i].token_value.data &&
            tokens[i].token_value.len > 0) {
            s->alloc.free((void *)(uintptr_t)tokens[i].token_value.data,
                          tokens[i].token_value.len, s->alloc.ctx);
            tokens[i].token_value.data = NULL;
        }
    }
}

void process_auth_tokens_free_staging(moq_session_t *s,
    moq_resolved_token_t *tokens, const bool *staged,
    size_t count)
{
    free_staging_internal(s, tokens, staged, count);
}
