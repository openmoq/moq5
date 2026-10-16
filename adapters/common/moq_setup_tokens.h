/*
 * An owned copy of the authorization tokens a host presents in SETUP
 * (moq_session_cfg_t.setup_auth_tokens), for the managed facades and the
 * service endpoint, which create their sessions after the call that
 * configured them returned. One block holds the token array and the values.
 */
#ifndef MOQ_ADAPTER_SETUP_TOKENS_H
#define MOQ_ADAPTER_SETUP_TOKENS_H

#include <moq/types.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

typedef struct moq_setup_tokens {
    moq_auth_token_t *tokens;
    size_t            count;
    size_t            block_len;
} moq_setup_tokens_t;

/* Copy `n` tokens. The session validates them at create; this refuses only
 * what it cannot copy (NULL with a count, a NULL value with a length, a
 * count past the session's 16) with MOQ_ERR_INVAL, and an allocation failure
 * with MOQ_ERR_NOMEM. `out` is untouched on failure. */
static inline moq_result_t moq_setup_tokens_copy(const moq_alloc_t *alloc,
    const moq_auth_token_t *in, size_t n, moq_setup_tokens_t *out)
{
    if (n == 0) { memset(out, 0, sizeof(*out)); return MOQ_OK; }
    if (!in || n > 16) return MOQ_ERR_INVAL;
    size_t bytes = n * sizeof(moq_auth_token_t);
    for (size_t i = 0; i < n; i++) {
        if (in[i].token_value.len && !in[i].token_value.data) return MOQ_ERR_INVAL;
        if (in[i].token_value.len > UINT16_MAX) return MOQ_ERR_INVAL;
        bytes += in[i].token_value.len;
    }
    uint8_t *b = (uint8_t *)alloc->alloc(bytes, alloc->ctx);
    if (!b) return MOQ_ERR_NOMEM;
    moq_auth_token_t *t = (moq_auth_token_t *)(void *)b;
    uint8_t *p = b + n * sizeof(moq_auth_token_t);
    for (size_t i = 0; i < n; i++) {
        t[i].token_type = in[i].token_type;
        t[i].token_value.len = in[i].token_value.len;
        t[i].token_value.data = in[i].token_value.len ? p : NULL;
        if (in[i].token_value.len) memcpy(p, in[i].token_value.data, in[i].token_value.len);
        p += in[i].token_value.len;
    }
    out->tokens = t;
    out->count = n;
    out->block_len = bytes;
    return MOQ_OK;
}

/* The send arena a session's SETUP is encoded into when its owner leaves
 * moq_session_cfg_t.send_buffer_size at 0 (its documented default), and the
 * room a SETUP needs besides its tokens: the message type and length and
 * every other option this library sends. */
#define MOQ_SETUP_TOKENS_DEFAULT_ARENA 4096u
#define MOQ_SETUP_TOKENS_OTHER_OPTIONS 128u

/* Whether a SETUP carrying these tokens and `target_len` further bytes of
 * connection target fits a send arena of `arena` bytes -- and so a peer
 * receive buffer of that size. Each token adds at most its value and 32
 * bytes (option type and length, alias type, token type: the bound
 * moq_session_create applies). A set that does not fit would be accepted
 * here and then fail when the session starts, so the facades and the
 * service refuse it when they are configured. */
static inline bool moq_setup_tokens_fit(const moq_auth_token_t *in, size_t n,
                                        size_t target_len, size_t arena)
{
    if (n == 0) return true;
    if (!in || n > 16 || arena < MOQ_SETUP_TOKENS_OTHER_OPTIONS) return false;
    size_t room = arena - MOQ_SETUP_TOKENS_OTHER_OPTIONS;
    if (target_len > room) return false;
    room -= target_len;
    for (size_t i = 0; i < n; i++) {
        if (in[i].token_value.len > UINT16_MAX) return false;
        size_t need = in[i].token_value.len + 32u;
        if (need > room) return false;
        room -= need;
    }
    return true;
}

/* Release the copy. The block held credentials, so it is overwritten first;
 * the stores go through a volatile pointer so they are not elided as dead
 * before the free. */
static inline void moq_setup_tokens_free(const moq_alloc_t *alloc,
                                         moq_setup_tokens_t *c)
{
    if (c->tokens) {
        volatile uint8_t *b = (volatile uint8_t *)(void *)c->tokens;
        for (size_t i = 0; i < c->block_len; i++) b[i] = 0;
        alloc->free(c->tokens, c->block_len, alloc->ctx);
    }
    memset(c, 0, sizeof(*c));
}

#endif /* MOQ_ADAPTER_SETUP_TOKENS_H */
