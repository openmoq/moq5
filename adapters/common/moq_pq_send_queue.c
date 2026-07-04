/*
 * moq_pq_send_queue.c -- see moq_pq_send_queue.h.
 *
 * Per-stream FIFO of chunks with O(1) append and pop. Streams are held in a
 * small dynamically grown table looked up by stream id (bounded by the bridge's
 * stream count, so a linear probe is cheap). An aggregate byte counter enforces
 * the cap.
 */

#include "moq_pq_send_queue.h"
#include <stdlib.h>
#include <string.h>

typedef struct pq_chunk {
    struct pq_chunk *next;
    moq_rcbuf_t     *rcbuf;   /* retained payload ref, or NULL */
    uint8_t         *copied;  /* owned copy buffer, or NULL */
    const uint8_t   *data;    /* base pointer (rcbuf data or copied) */
    size_t           len;
    size_t           offset;  /* bytes already provided from this chunk */
    bool             fin;     /* FIN after this chunk's last byte */
} pq_chunk_t;

typedef struct {
    bool        in_use;
    uint64_t    sid;
    pq_chunk_t *head;
    pq_chunk_t *tail;
    uint64_t    remaining;    /* data bytes not yet provided */
} pq_stream_t;

struct moq_pq_send_queue {
    moq_alloc_t  alloc;
    uint64_t     cap;
    uint64_t     queued;      /* aggregate remaining bytes across streams */
    pq_stream_t *streams;
    size_t       nstreams;    /* allocated slots */
};

static uint64_t pq_env_cap(uint64_t fallback)
{
    const char *e = getenv("MOQ_PQ_STREAM_QUEUE_BYTES");
    if (e && *e) {
        char *end = NULL;
        unsigned long long v = strtoull(e, &end, 10);
        if (end && *end == '\0' && v > 0)
            return (uint64_t)v;
    }
    return fallback;
}

moq_pq_send_queue_t *moq_pq_send_queue_create(const moq_alloc_t *alloc,
                                              uint64_t cap)
{
    if (!alloc) return NULL;
    moq_pq_send_queue_t *q = (moq_pq_send_queue_t *)alloc->alloc(
        sizeof(*q), alloc->ctx);
    if (!q) return NULL;
    memset(q, 0, sizeof(*q));
    q->alloc = *alloc;
    q->cap = pq_env_cap(cap ? cap : MOQ_PQ_SEND_QUEUE_CAP_DEFAULT);
    return q;
}

static void pq_chunk_free(moq_pq_send_queue_t *q, pq_chunk_t *c)
{
    if (c->rcbuf) moq_rcbuf_decref(c->rcbuf);
    if (c->copied) q->alloc.free(c->copied, c->len, q->alloc.ctx);
    q->alloc.free(c, sizeof(*c), q->alloc.ctx);
}

static void pq_stream_clear(moq_pq_send_queue_t *q, pq_stream_t *s)
{
    pq_chunk_t *c = s->head;
    while (c) {
        pq_chunk_t *n = c->next;
        if (q->queued >= (c->len - c->offset))
            q->queued -= (c->len - c->offset);
        else
            q->queued = 0;
        pq_chunk_free(q, c);
        c = n;
    }
    s->head = s->tail = NULL;
    s->remaining = 0;
    s->in_use = false;
    s->sid = 0;
}

void moq_pq_send_queue_destroy(moq_pq_send_queue_t *q)
{
    if (!q) return;
    for (size_t i = 0; i < q->nstreams; i++)
        if (q->streams[i].in_use)
            pq_stream_clear(q, &q->streams[i]);
    if (q->streams)
        q->alloc.free(q->streams, q->nstreams * sizeof(pq_stream_t),
                      q->alloc.ctx);
    q->alloc.free(q, sizeof(*q), q->alloc.ctx);
}

static pq_stream_t *pq_find(moq_pq_send_queue_t *q, uint64_t sid)
{
    for (size_t i = 0; i < q->nstreams; i++)
        if (q->streams[i].in_use && q->streams[i].sid == sid)
            return &q->streams[i];
    return NULL;
}

static pq_stream_t *pq_find_or_create(moq_pq_send_queue_t *q, uint64_t sid)
{
    pq_stream_t *s = pq_find(q, sid);
    if (s) return s;
    for (size_t i = 0; i < q->nstreams; i++)
        if (!q->streams[i].in_use) {
            s = &q->streams[i];
            memset(s, 0, sizeof(*s));
            s->in_use = true;
            s->sid = sid;
            return s;
        }
    /* Grow the table. */
    size_t old = q->nstreams;
    size_t nn = old ? old * 2 : 8;
    pq_stream_t *ns = (pq_stream_t *)q->alloc.realloc(
        q->streams, old * sizeof(pq_stream_t), nn * sizeof(pq_stream_t),
        q->alloc.ctx);
    if (!ns) return NULL;
    memset(ns + old, 0, (nn - old) * sizeof(pq_stream_t));
    q->streams = ns;
    q->nstreams = nn;
    s = &q->streams[old];
    s->in_use = true;
    s->sid = sid;
    return s;
}

/* Aggregate cap check: reject a non-empty-backlog push that would exceed the
 * cap, but always accept onto an empty aggregate (a single object larger than
 * the cap must not be permanently blocked). Overflow-safe. */
static bool pq_would_exceed(const moq_pq_send_queue_t *q, size_t len)
{
    if (len == 0 || q->queued == 0) return false;
    if (q->queued >= q->cap) return true;
    return (uint64_t)len > q->cap - q->queued;
}

static void pq_append(pq_stream_t *s, pq_chunk_t *c)
{
    c->next = NULL;
    if (s->tail) s->tail->next = c;
    else s->head = c;
    s->tail = c;
}

/* Apply a bare FIN (no bytes): collapse onto the tail if present, else add a
 * zero-length FIN chunk. Returns 1 on success, -1 on allocation failure. */
static int pq_push_bare_fin(moq_pq_send_queue_t *q, pq_stream_t *s)
{
    if (s->tail) { s->tail->fin = true; return 1; }
    pq_chunk_t *c = (pq_chunk_t *)q->alloc.alloc(sizeof(*c), q->alloc.ctx);
    if (!c) return -1;
    memset(c, 0, sizeof(*c));
    c->fin = true;
    pq_append(s, c);
    return 1;
}

int moq_pq_send_queue_push_copy(moq_pq_send_queue_t *q, uint64_t sid,
                                const uint8_t *data, size_t len, bool fin)
{
    if (!q) return -1;
    /* A zero-length non-FIN write carries nothing; accept it without touching
     * a stream slot (creating one would leak an empty in_use entry). */
    if (len == 0 && !fin) return 1;
    if (pq_would_exceed(q, len)) return 0;

    pq_stream_t *s = pq_find_or_create(q, sid);
    if (!s) return -1;

    if (len == 0)
        return pq_push_bare_fin(q, s);   /* len == 0 && fin */

    pq_chunk_t *c = (pq_chunk_t *)q->alloc.alloc(sizeof(*c), q->alloc.ctx);
    if (!c) { if (!s->head) s->in_use = false; return -1; }
    memset(c, 0, sizeof(*c));
    c->copied = (uint8_t *)q->alloc.alloc(len, q->alloc.ctx);
    if (!c->copied) {
        q->alloc.free(c, sizeof(*c), q->alloc.ctx);
        if (!s->head) s->in_use = false;
        return -1;
    }
    memcpy(c->copied, data, len);
    c->data = c->copied;
    c->len = len;
    c->fin = fin;
    pq_append(s, c);
    s->remaining += len;
    q->queued += len;
    return 1;
}

int moq_pq_send_queue_push_rcbuf(moq_pq_send_queue_t *q, uint64_t sid,
                                 moq_rcbuf_t *buf, bool fin)
{
    if (!q) return -1;
    size_t len = buf ? moq_rcbuf_len(buf) : 0;

    /* A zero-length payload carries no send_queue node; treat as a bare FIN (or
     * a no-op). Do not retain a ref for zero bytes. */
    if (len == 0)
        return moq_pq_send_queue_push_copy(q, sid, NULL, 0, fin);

    if (pq_would_exceed(q, len)) return 0;

    pq_stream_t *s = pq_find_or_create(q, sid);
    if (!s) return -1;

    pq_chunk_t *c = (pq_chunk_t *)q->alloc.alloc(sizeof(*c), q->alloc.ctx);
    if (!c) { if (!s->head) s->in_use = false; return -1; }
    memset(c, 0, sizeof(*c));
    moq_rcbuf_incref(buf);
    c->rcbuf = buf;
    c->data = moq_rcbuf_data(buf);
    c->len = len;
    c->fin = fin;
    pq_append(s, c);
    s->remaining += len;
    q->queued += len;
    return 1;
}

bool moq_pq_send_queue_has_data(moq_pq_send_queue_t *q, uint64_t sid)
{
    if (!q) return false;
    pq_stream_t *s = pq_find(q, sid);
    return s && s->head != NULL;
}

bool moq_pq_send_queue_plan(moq_pq_send_queue_t *q, uint64_t sid, size_t max,
                            size_t *nbytes, bool *is_fin, bool *still_active)
{
    *nbytes = 0; *is_fin = false; *still_active = false;
    if (!q) return false;
    pq_stream_t *s = pq_find(q, sid);
    if (!s || !s->head) return false;

    uint64_t avail = s->remaining;
    size_t n = (avail < (uint64_t)max) ? (size_t)avail : max;
    bool all = ((uint64_t)n == avail);
    /* FIN lives on the tail chunk; it is delivered only when every data byte
     * up to and including it has been consumed. */
    *nbytes = n;
    *is_fin = all && s->tail && s->tail->fin;
    *still_active = ((uint64_t)n < avail);   /* more bytes remain */
    return true;
}

void moq_pq_send_queue_commit(moq_pq_send_queue_t *q, uint64_t sid,
                              uint8_t *dst, size_t nbytes)
{
    if (!q) return;
    pq_stream_t *s = pq_find(q, sid);
    if (!s) return;

    size_t need = nbytes;
    /* Consume `need` data bytes, popping each chunk as it is fully drained.
     * Continue past need==0 only to pop a trailing fully-consumed chunk (the
     * zero-length FIN marker), never a partially-consumed data chunk. */
    while (s->head && (need > 0 || s->head->offset == s->head->len)) {
        pq_chunk_t *c = s->head;
        size_t rem = c->len - c->offset;
        if (rem > 0 && need > 0) {
            size_t take = rem < need ? rem : need;
            if (dst) { memcpy(dst, c->data + c->offset, take); dst += take; }
            c->offset += take;
            need -= take;
            rem -= take;
            s->remaining -= take;
            q->queued = (q->queued >= take) ? q->queued - take : 0;
        }
        if (c->offset == c->len) {
            s->head = c->next;
            if (!s->head) s->tail = NULL;
            pq_chunk_free(q, c);
        } else {
            break;   /* partial data chunk; stop */
        }
    }
    if (!s->head)
        pq_stream_clear(q, s);   /* drop the now-empty stream slot */
}

void moq_pq_send_queue_drop(moq_pq_send_queue_t *q, uint64_t sid)
{
    if (!q) return;
    pq_stream_t *s = pq_find(q, sid);
    if (s) pq_stream_clear(q, s);
}

uint64_t moq_pq_send_queue_queued_bytes(const moq_pq_send_queue_t *q)
{
    return q ? q->queued : 0;
}
