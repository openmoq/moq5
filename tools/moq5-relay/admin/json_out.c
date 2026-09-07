#include "json_out.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

/* Every write goes through here. Once refused, the writer stays refused and
 * stores nothing more; a counting writer keeps measuring. */
static void
put(moqr_json_w_t *w, const char *p, size_t n)
{
    if (w->refused) {
        return;
    }
    if (w->buf != NULL) {
        /* Room for the body AND its terminating NUL. */
        if (n > w->cap || w->len > w->cap - n || w->len + n >= w->cap) {
            w->refused = true;
            return;
        }
        memcpy(w->buf + w->len, p, n);
    } else if (w->len > SIZE_MAX - n) {
        w->refused = true;   /* a measurement that wrapped is not a bound */
        return;
    }
    w->len += n;
}

static void
put_c(moqr_json_w_t *w, char c)
{
    put(w, &c, 1);
}

/* A value is about to be written into the current container. */
static void
separate(moqr_json_w_t *w)
{
    if (w->depth == 0u) {
        return;
    }
    if (w->first[w->depth - 1u]) {
        w->first[w->depth - 1u] = false;
    } else {
        put_c(w, ',');
    }
}

static void
open_container(moqr_json_w_t *w, char c)
{
    separate(w);
    if (w->depth >= MOQR_JSON_MAX_DEPTH) {
        w->refused = true;
        return;
    }
    w->first[w->depth] = true;
    w->depth++;
    put_c(w, c);
}

static void
close_container(moqr_json_w_t *w, char c)
{
    if (w->depth == 0u) {
        w->refused = true;
        return;
    }
    w->depth--;
    put_c(w, c);
}

void
moqr_json_begin(moqr_json_w_t *w, char *buf, size_t cap)
{
    memset(w, 0, sizeof(*w));
    w->buf = buf;
    w->cap = buf != NULL ? cap : 0u;
    if (buf != NULL && cap == 0u) {
        w->refused = true;   /* no room even for the terminator */
    }
}

bool
moqr_json_end(moqr_json_w_t *w, size_t *out_len)
{
    if (w->depth != 0u) {
        w->refused = true;
    }
    if (w->refused) {
        if (w->buf != NULL && w->cap > 0u) {
            w->buf[0] = '\0';
        }
        if (out_len != NULL) {
            *out_len = 0;
        }
        return false;
    }
    if (w->buf != NULL) {
        w->buf[w->len] = '\0';   /* len < cap is guaranteed by put() */
    }
    if (out_len != NULL) {
        *out_len = w->len;
    }
    return true;
}

void moqr_json_object_begin(moqr_json_w_t *w) { open_container(w, '{'); }
void moqr_json_object_end(moqr_json_w_t *w)   { close_container(w, '}'); }
void moqr_json_array_begin(moqr_json_w_t *w)  { open_container(w, '['); }
void moqr_json_array_end(moqr_json_w_t *w)    { close_container(w, ']'); }

/* -- UTF-8 ---------------------------------------------------------------- */

/* The length of the well-formed sequence starting at s[0], or 0 when it is
 * not one: RFC 3629 table, so overlong forms, surrogates and anything above
 * U+10FFFF are rejected by the second-byte ranges. */
static size_t
utf8_seq(const unsigned char *s, size_t n)
{
    unsigned char b0 = s[0];
    unsigned char lo = 0x80u;
    unsigned char hi = 0xBFu;
    size_t need;

    if (b0 < 0x80u) {
        return 1;
    }
    if (b0 >= 0xC2u && b0 <= 0xDFu) {
        need = 2;
    } else if (b0 == 0xE0u) {
        need = 3; lo = 0xA0u;
    } else if (b0 >= 0xE1u && b0 <= 0xECu) {
        need = 3;
    } else if (b0 == 0xEDu) {
        need = 3; hi = 0x9Fu;          /* excludes the surrogate block */
    } else if (b0 >= 0xEEu && b0 <= 0xEFu) {
        need = 3;
    } else if (b0 == 0xF0u) {
        need = 4; lo = 0x90u;
    } else if (b0 >= 0xF1u && b0 <= 0xF3u) {
        need = 4;
    } else if (b0 == 0xF4u) {
        need = 4; hi = 0x8Fu;          /* excludes anything above U+10FFFF */
    } else {
        return 0;                      /* 0x80..0xC1, 0xF5..0xFF */
    }
    if (n < need) {
        return 0;                      /* truncated */
    }
    if (s[1] < lo || s[1] > hi) {
        return 0;
    }
    for (size_t i = 2; i < need; i++) {
        if (s[i] < 0x80u || s[i] > 0xBFu) {
            return 0;
        }
    }
    return need;
}

bool
moqr_json_utf8_valid(const char *s, size_t n)
{
    const unsigned char *p = (const unsigned char *)s;
    size_t i = 0;

    if (s == NULL && n != 0u) {
        return false;
    }
    while (i < n) {
        size_t k = utf8_seq(p + i, n - i);
        if (k == 0u) {
            return false;
        }
        i += k;
    }
    return true;
}

size_t
moqr_json_escaped_max(size_t n)
{
    return n > SIZE_MAX / 6u ? SIZE_MAX : n * 6u;
}

/* -- values --------------------------------------------------------------- */

static void
put_string(moqr_json_w_t *w, const char *s, size_t n)
{
    static const char hex[] = "0123456789abcdef";
    const unsigned char *p = (const unsigned char *)s;
    size_t i = 0;

    if (!moqr_json_utf8_valid(s, n)) {
        w->refused = true;
        return;
    }
    put_c(w, '"');
    while (i < n) {
        unsigned char c = p[i];
        size_t k = utf8_seq(p + i, n - i);
        if (k == 0u) {
            /* Unreachable after validation; refusing here means no input
             * can ever make this loop fail to advance. */
            w->refused = true;
            return;
        }
        if (c == '"' || c == '\\') {
            char esc[2] = { '\\', (char)c };
            put(w, esc, 2);
        } else if (c < 0x20u || c == 0x7Fu) {
            char esc[6] = { '\\', 'u', '0', '0', hex[c >> 4], hex[c & 0xFu] };
            put(w, esc, 6);
        } else {
            put(w, (const char *)p + i, k);
        }
        i += k;
    }
    put_c(w, '"');
}

void
moqr_json_key(moqr_json_w_t *w, const char *key)
{
    separate(w);
    put_string(w, key, strlen(key));
    put_c(w, ':');
    /* The value that follows is part of this member, not a new member. */
    if (w->depth > 0u) {
        w->first[w->depth - 1u] = true;
    }
}

void
moqr_json_str(moqr_json_w_t *w, const char *s, size_t n)
{
    separate(w);
    put_string(w, s, n);
}

void
moqr_json_str_bounded(moqr_json_w_t *w, const char *arr, size_t cap)
{
    const char *end = arr != NULL ? memchr(arr, '\0', cap) : NULL;

    if (end == NULL) {
        separate(w);
        w->refused = true;   /* no terminator inside the array */
        return;
    }
    moqr_json_str(w, arr, (size_t)(end - arr));
}

void
moqr_json_u64(moqr_json_w_t *w, uint64_t v)
{
    char d[32];
    int n = snprintf(d, sizeof(d), "%" PRIu64, v);
    separate(w);
    if (n <= 0 || (size_t)n >= sizeof(d)) {
        w->refused = true;
        return;
    }
    put(w, d, (size_t)n);
}

void
moqr_json_i64(moqr_json_w_t *w, int64_t v)
{
    char d[32];
    int n = snprintf(d, sizeof(d), "%" PRId64, v);
    separate(w);
    if (n <= 0 || (size_t)n >= sizeof(d)) {
        w->refused = true;
        return;
    }
    put(w, d, (size_t)n);
}

void
moqr_json_null(moqr_json_w_t *w)
{
    separate(w);
    put(w, "null", 4);
}

void
moqr_json_bool(moqr_json_w_t *w, bool v)
{
    separate(w);
    if (v) {
        put(w, "true", 4);
    } else {
        put(w, "false", 5);
    }
}
