#include <moq/relay/trace.h>

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

/* Fixed-size, padding-free record is what makes hashing raw bytes
 * deterministic; growth requires a conscious layout decision. */
_Static_assert(sizeof(moqr_trace_rec_t) ==
                   sizeof(uint32_t) * 2 + sizeof(uint64_t) +
                       sizeof(moqr_epochs_t) + sizeof(uint64_t) * 4,
               "moqr_trace_rec_t must stay packed scalars");

#define TRACE_DEF_RECORDS 1024u

#define FNV64_OFFSET UINT64_C(0xCBF29CE484222325)
#define FNV64_PRIME  UINT64_C(0x100000001B3)

struct moqr_trace {
    moq_alloc_t       alloc;
    moqr_trace_rec_t *ring;
    uint32_t          cap;
    uint64_t          count;   /* total emitted == next shard_seq          */
    uint64_t          hash;
    moqr_epochs_t     epochs;
};

moqr_result_t
moqr_trace_create(const moq_alloc_t *alloc, uint32_t record_cap,
                  moqr_trace_t **out)
{
    if (out == NULL) {
        return MOQR_ERR_INVAL;
    }
    *out = NULL;
    if (alloc == NULL || alloc->alloc == NULL || alloc->free == NULL) {
        return MOQR_ERR_INVAL;
    }
    uint32_t cap = record_cap != 0 ? record_cap : TRACE_DEF_RECORDS;

    moqr_trace_t *t = alloc->alloc(sizeof(*t), alloc->ctx);
    if (t == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(t, 0, sizeof(*t));
    t->alloc = *alloc;
    t->cap = cap;
    t->hash = FNV64_OFFSET;

    t->ring = t->alloc.alloc((size_t)cap * sizeof(*t->ring), t->alloc.ctx);
    if (t->ring == NULL) {
        t->alloc.free(t, sizeof(*t), t->alloc.ctx);
        return MOQR_ERR_NOMEM;
    }
    memset(t->ring, 0, (size_t)cap * sizeof(*t->ring));

    *out = t;
    return MOQR_OK;
}

size_t
moqr_trace_bytes(uint32_t record_cap)
{
    uint32_t cap = record_cap != 0 ? record_cap : TRACE_DEF_RECORDS;
    return sizeof(moqr_trace_t) + (size_t)cap * sizeof(moqr_trace_rec_t);
}

void
moqr_trace_destroy(moqr_trace_t *t)
{
    if (t == NULL) {
        return;
    }
    moq_alloc_t a = t->alloc;
    a.free(t->ring, (size_t)t->cap * sizeof(*t->ring), a.ctx);
    a.free(t, sizeof(*t), a.ctx);
}

void
moqr_trace_set_epochs(moqr_trace_t *t, moqr_epochs_t epochs)
{
    if (t != NULL) {
        t->epochs = epochs;
    }
}

void
moqr_trace_emit(moqr_trace_t *t, moqr_trace_rec_t rec)
{
    if (t == NULL) {
        return;
    }
    rec.shard_seq = t->count;
    rec.epochs = t->epochs;

    const unsigned char *bytes = (const unsigned char *)&rec;
    uint64_t h = t->hash;
    for (size_t i = 0; i < sizeof(rec); i++) {
        h ^= bytes[i];
        h *= FNV64_PRIME;
    }
    t->hash = h;

    t->ring[t->count % t->cap] = rec;
    t->count++;
}

uint64_t
moqr_trace_hash(const moqr_trace_t *t)
{
    return t != NULL ? t->hash : 0;
}

uint64_t
moqr_trace_count(const moqr_trace_t *t)
{
    return t != NULL ? t->count : 0;
}

size_t
moqr_trace_read(const moqr_trace_t *t, moqr_trace_rec_t *out, size_t cap)
{
    if (t == NULL || out == NULL || cap == 0) {
        return 0;
    }
    uint64_t retained = t->count < t->cap ? t->count : t->cap;
    uint64_t n = retained < cap ? retained : cap;
    uint64_t first = t->count - retained;   /* oldest retained seq */
    for (uint64_t i = 0; i < n; i++) {
        out[i] = t->ring[(first + i) % t->cap];
    }
    return (size_t)n;
}

const char *
moqr_trace_kind_name(moqr_trace_kind_t kind)
{
    switch (kind) {
    case MOQR_TRACE_INGEST:              return "ingest";
    case MOQR_TRACE_APPEND:              return "append";
    case MOQR_TRACE_GROUP_CLOSE:         return "group_close";
    case MOQR_TRACE_EVICT:               return "evict";
    case MOQR_TRACE_CURSOR_CREATE:       return "cursor_create";
    case MOQR_TRACE_CURSOR_ADVANCE:      return "cursor_advance";
    case MOQR_TRACE_CURSOR_SKIP:         return "cursor_skip";
    case MOQR_TRACE_CURSOR_RETIRE:       return "cursor_retire";
    case MOQR_TRACE_DELIVERY:            return "delivery";
    case MOQR_TRACE_BLOCKED:             return "blocked";
    case MOQR_TRACE_RESUMED:             return "resumed";
    case MOQR_TRACE_ROUTE_ADD:           return "route_add";
    case MOQR_TRACE_ROUTE_REMOVE:        return "route_remove";
    case MOQR_TRACE_UPSTREAM_SUBSCRIBE:  return "upstream_subscribe";
    case MOQR_TRACE_UPSTREAM_RESOLVE:    return "upstream_resolve";
    case MOQR_TRACE_AUTH_DECISION:       return "auth_decision";
    case MOQR_TRACE_REDIRECT:            return "redirect";
    case MOQR_TRACE_GOAWAY:              return "goaway";
    case MOQR_TRACE_MAILBOX_PUSH:        return "mailbox_push";
    case MOQR_TRACE_MAILBOX_POP:         return "mailbox_pop";
    case MOQR_TRACE_CAPACITY:            return "capacity";
    default:                             return "unknown";
    }
}

/* Bounded append writer: snprintf-style, counts the full required length in
 * `len` even past `cap`, so callers learn the needed size on truncation. */
typedef struct {
    char  *buf;
    size_t cap;
    size_t len;
} trace_writer_t;

static void
tw_addf(trace_writer_t *w, const char *fmt, ...)
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

moqr_result_t
moqr_trace_write_jsonl(const moqr_trace_t *t, char *buf, size_t cap,
                       size_t *written)
{
    if (written != NULL) {
        *written = 0;
    }
    if (t == NULL || buf == NULL) {
        return MOQR_ERR_INVAL;
    }
    trace_writer_t w = { buf, cap, 0 };
    uint64_t retained = t->count < t->cap ? t->count : t->cap;
    uint64_t first = t->count - retained;
    for (uint64_t i = 0; i < retained; i++) {
        const moqr_trace_rec_t *r = &t->ring[(first + i) % t->cap];
        tw_addf(&w,
                "{\"seq\":%llu,\"kind\":\"%s\",\"detail\":%u,"
                "\"node_epoch\":%llu,\"shard_epoch\":%llu,\"route_epoch\":%llu,"
                "\"e0\":%llu,\"e1\":%llu,\"e2\":%llu,\"e3\":%llu}\n",
                (unsigned long long)r->shard_seq,
                moqr_trace_kind_name(r->kind), r->detail,
                (unsigned long long)r->epochs.node_epoch,
                (unsigned long long)r->epochs.shard_epoch,
                (unsigned long long)r->epochs.route_epoch,
                (unsigned long long)r->e0, (unsigned long long)r->e1,
                (unsigned long long)r->e2, (unsigned long long)r->e3);
    }
    if (cap > 0) {
        buf[w.len < cap ? w.len : cap - 1] = '\0';
    }
    if (written != NULL) {
        *written = w.len;   /* content length, excluding the NUL */
    }
    /* Success requires room for content + NUL; on truncation the caller
     * grows to *written + 1 and retries. */
    return w.len < cap ? MOQR_OK : MOQR_ERR_CAPACITY;
}
