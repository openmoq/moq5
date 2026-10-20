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
    size_t           copy_len;/* allocated size of `copied` */
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
    uint32_t    priority_key;
    uint8_t     level;        /* transport priority last applied */
    bool        level_applied;
    bool        unstarted;    /* opened, no byte accepted yet */
    bool        counted;      /* in keyed_holding */
    size_t      next_free;
} pq_stream_t;

/* Bytes handed to the provider so far on a stream whose FIN has not yet been
 * handed over, and that has not been dropped. Counted at commit, so bytes a
 * truncate discards are never mistaken for bytes provided. Outlives the
 * stream's queue slot, which is released whenever the queue runs empty, so a
 * reset can tell how much of a prefix was provided, including a reset after
 * the FIN was pushed but before it was provided. Created by the first push
 * that carries bytes; forgotten when the FIN is handed over (commit), on
 * drop, on a truncate that leaves nothing, and on destroy. */
typedef struct {
    uint64_t sid;
    uint64_t provided;
} pq_offset_t;
/* sid -> slot, open addressing; slot < 0 marks an empty entry. */
typedef struct {
    uint64_t sid;
    int64_t  slot;
} pq_index_t;

typedef struct {
    uint32_t key;
    size_t   slot;
} pq_rank_t;

/* Highest data rank: picoquic level 2 + 2 * 126 = 254. */
#define PQ_RANK_MAX 126u

struct moq_pq_send_queue {
    moq_alloc_t  alloc;
    pq_offset_t *offs;
    size_t       noffs;
    size_t       offs_cap;
    uint64_t     cap;
    uint64_t     queued;      /* aggregate remaining bytes across streams */
    pq_stream_t *streams;
    size_t       nstreams;    /* allocated slots */
    /* Telemetry (characterization): peak aggregate backlog and the number of
     * pushes refused for hitting the cap (bridge WOULD_BLOCK). */
    uint64_t     high_water;
    uint64_t     would_block;
    bool         priorities_dirty;
    pq_rank_t   *order;       /* ranking scratch, order_cap entries */
    size_t       order_cap;
    pq_index_t  *index;       /* 2 * nstreams entries */
    size_t       free_head;   /* free slot list; nstreams when empty */
    size_t       keyed_holding; /* keyed streams holding bytes */
};

/* Bump the high-water mark after `queued` grows. */
static void pq_note_high_water(moq_pq_send_queue_t *q)
{
    if (q->queued > q->high_water) q->high_water = q->queued;
}

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

static pq_offset_t *pq_offset_find(moq_pq_send_queue_t *q, uint64_t sid)
{
    for (size_t i = 0; i < q->noffs; i++)
        if (q->offs[i].sid == sid) return &q->offs[i];
    return NULL;
}

static void pq_offset_forget(moq_pq_send_queue_t *q, uint64_t sid)
{
    pq_offset_t *o = pq_offset_find(q, sid);
    if (o) *o = q->offs[--q->noffs];
}

/* Make sure `sid` has a record before `len` bytes are queued on it,
 * including bytes carried by the push that also carries the FIN: the record
 * stays until that FIN is handed to the provider. A zero-byte push needs no
 * record and never allocates. Returns -1 on allocation failure, before
 * anything is queued. */
static int pq_offset_note(moq_pq_send_queue_t *q, uint64_t sid, size_t len)
{
    if (len == 0) return 0;
    pq_offset_t *o = pq_offset_find(q, sid);
    if (!o) {
        if (q->noffs == q->offs_cap) {
            size_t ncap = q->offs_cap ? q->offs_cap * 2 : 8;
            pq_offset_t *n = (pq_offset_t *)q->alloc.realloc(
                q->offs, q->offs_cap * sizeof(*n), ncap * sizeof(*n), q->alloc.ctx);
            if (!n) return -1;
            q->offs = n;
            q->offs_cap = ncap;
        }
        o = &q->offs[q->noffs++];
        o->sid = sid;
        o->provided = 0;
    }
    return 0;
}

static void pq_chunk_free(moq_pq_send_queue_t *q, pq_chunk_t *c)
{
    if (c->rcbuf) moq_rcbuf_decref(c->rcbuf);
    if (c->copied) q->alloc.free(c->copied, c->copy_len, q->alloc.ctx);
    q->alloc.free(c, sizeof(*c), q->alloc.ctx);
}

static void pq_index_remove(moq_pq_send_queue_t *q, uint64_t sid);

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
    q->keyed_holding -= s->counted;
    pq_index_remove(q, s->sid);
    memset(s, 0, sizeof(*s));
    s->next_free = q->free_head;
    q->free_head = (size_t)(s - q->streams);
    /* The others keep their levels: one fewer never reverses their order. */
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
    if (q->offs)
        q->alloc.free(q->offs, q->offs_cap * sizeof(pq_offset_t), q->alloc.ctx);
    if (q->order)
        q->alloc.free(q->order, q->order_cap * sizeof(pq_rank_t),
                      q->alloc.ctx);
    if (q->index)
        q->alloc.free(q->index, 2 * q->nstreams * sizeof(pq_index_t),
                      q->alloc.ctx);
    q->alloc.free(q, sizeof(*q), q->alloc.ctx);
}

static size_t pq_index_mask(const moq_pq_send_queue_t *q)
{
    return 2 * q->nstreams - 1;
}

static size_t pq_index_home(const moq_pq_send_queue_t *q, uint64_t sid)
{
    return (size_t)((sid * 0x9E3779B97F4A7C15ULL) >> 32) & pq_index_mask(q);
}

static void pq_index_insert(moq_pq_send_queue_t *q, uint64_t sid, size_t slot)
{
    size_t i = pq_index_home(q, sid);
    while (q->index[i].slot >= 0) i = (i + 1) & pq_index_mask(q);
    q->index[i].sid = sid;
    q->index[i].slot = (int64_t)slot;
}

static void pq_index_remove(moq_pq_send_queue_t *q, uint64_t sid)
{
    size_t mask = pq_index_mask(q);
    size_t i = pq_index_home(q, sid);
    for (;; i = (i + 1) & mask) {
        if (q->index[i].slot < 0) return;
        if (q->index[i].sid == sid) break;
    }
    q->index[i].slot = -1;
    /* Backshift entries whose probe chain crossed the hole. */
    for (size_t j = (i + 1) & mask; q->index[j].slot >= 0; j = (j + 1) & mask) {
        size_t home = pq_index_home(q, q->index[j].sid);
        if (((j - home) & mask) >= ((j - i) & mask)) {
            q->index[i] = q->index[j];
            q->index[j].slot = -1;
            i = j;
        }
    }
}

static pq_stream_t *pq_find(moq_pq_send_queue_t *q, uint64_t sid)
{
    if (!q->nstreams) return NULL;
    for (size_t i = pq_index_home(q, sid); q->index[i].slot >= 0;
         i = (i + 1) & pq_index_mask(q))
        if (q->index[i].sid == sid) return &q->streams[q->index[i].slot];
    return NULL;
}

/* Double the slot table and rebuild the index around it. */
static int pq_grow(moq_pq_send_queue_t *q)
{
    size_t old = q->nstreams;
    size_t nn = old ? old * 2 : 8;
    pq_index_t *ni = (pq_index_t *)q->alloc.alloc(2 * nn * sizeof(*ni),
                                                  q->alloc.ctx);
    if (!ni) return -1;
    pq_stream_t *ns = (pq_stream_t *)q->alloc.realloc(
        q->streams, old * sizeof(*ns), nn * sizeof(*ns), q->alloc.ctx);
    if (!ns) {
        q->alloc.free(ni, 2 * nn * sizeof(*ni), q->alloc.ctx);
        return -1;
    }
    q->streams = ns;
    if (q->index)
        q->alloc.free(q->index, 2 * old * sizeof(*ni), q->alloc.ctx);
    q->index = ni;
    q->nstreams = nn;
    for (size_t i = 0; i < 2 * nn; i++) ni[i].slot = -1;
    for (size_t i = 0; i < old; i++)
        if (ns[i].in_use) pq_index_insert(q, ns[i].sid, i);
    memset(ns + old, 0, (nn - old) * sizeof(*ns));
    for (size_t i = old; i < nn; i++) ns[i].next_free = i + 1;
    q->free_head = old;   /* only grown when the free list is empty */
    return 0;
}

static pq_stream_t *pq_find_or_create(moq_pq_send_queue_t *q, uint64_t sid)
{
    pq_stream_t *s = pq_find(q, sid);
    if (s) return s;
    if (q->free_head >= q->nstreams && pq_grow(q) != 0) return NULL;
    size_t slot = q->free_head;
    s = &q->streams[slot];
    q->free_head = s->next_free;
    memset(s, 0, sizeof(*s));
    s->in_use = true;
    s->sid = sid;
    pq_index_insert(q, sid, slot);
    q->priorities_dirty = true;
    return s;
}

int moq_pq_send_queue_opened(moq_pq_send_queue_t *q, uint64_t sid)
{
    if (!q) return -1;
    pq_stream_t *s = pq_find_or_create(q, sid);
    if (!s) return -1;
    if (!s->head) s->unstarted = true;
    return 0;
}

int moq_pq_send_queue_set_key(moq_pq_send_queue_t *q, uint64_t sid,
                              uint32_t key)
{
    if (!q) return -1;
    pq_stream_t *s = pq_find_or_create(q, sid);
    if (!s) return -1;
    if (s->priority_key != key) {
        s->priority_key = key;
        q->priorities_dirty = true;
        bool count = key && s->head;
        q->keyed_holding += (size_t)count - (size_t)s->counted;
        s->counted = count;
    }
    return 0;
}

static int pq_rank_cmp(const void *a, const void *b)
{
    const pq_rank_t *x = a;
    const pq_rank_t *y = b;
    return (x->key > y->key) - (x->key < y->key);
}

static int pq_order_reserve(moq_pq_send_queue_t *q)
{
    if (q->order_cap >= q->nstreams) return 0;
    pq_rank_t *o = (pq_rank_t *)q->alloc.realloc(
        q->order, q->order_cap * sizeof(*o), q->nstreams * sizeof(*o),
        q->alloc.ctx);
    if (!o) return -1;
    q->order = o;
    q->order_cap = q->nstreams;
    return 0;
}

static int pq_apply_level(pq_stream_t *s, uint8_t level,
                          int (*apply)(void *, uint64_t, uint8_t), void *ctx)
{
    if (s->level_applied && s->level == level) return 0;
    if (apply(ctx, s->sid, level) != 0) return -1;
    s->level = level;
    s->level_applied = true;
    return 0;
}

int moq_pq_send_queue_apply_priorities(moq_pq_send_queue_t *q,
    int (*apply)(void *, uint64_t, uint8_t), void *ctx)
{
    if (!q) return -1;
    if (!q->priorities_dirty) return 0;
    if (pq_order_reserve(q) != 0) return -1;
    /* Retain ranks alongside bytes, not bridge mappings: FIN can retire the
     * latter before picoquic asks for the last queued packet. */
    size_t n = 0;
    for (size_t i = 0; i < q->nstreams; ++i) {
        pq_stream_t *s = &q->streams[i];
        if (!s->in_use || s->unstarted) continue;
        if (!s->priority_key) {
            if (pq_apply_level(s, 0, apply, ctx) != 0) return -1;
            continue;
        }
        q->order[n].key = s->priority_key;
        q->order[n].slot = i;
        ++n;
    }
    if (n > 1) qsort(q->order, n, sizeof(*q->order), pq_rank_cmp);
    /* Dense: one rank per distinct key. */
    size_t rank = 0;
    for (size_t i = 0; i < n; ++i) {
        if (i > 0 && q->order[i].key != q->order[i - 1].key) rank++;
        const size_t r = rank < PQ_RANK_MAX ? rank : PQ_RANK_MAX;
        if (pq_apply_level(&q->streams[q->order[i].slot], (uint8_t)(2 + 2 * r),
                           apply, ctx) != 0)
            return -1;
    }
    q->priorities_dirty = false;
    return 0;
}

int moq_pq_send_queue_priority(moq_pq_send_queue_t *q, uint64_t sid,
    uint32_t key, int (*apply)(void *, uint64_t, uint8_t), void *ctx)
{
    if (moq_pq_send_queue_set_key(q, sid, key) != 0) return -1;
    return moq_pq_send_queue_apply_priorities(q, apply, ctx);
}

/* Aggregate cap check, a low-water mark: a push is refused only once the
 * backlog has reached the cap. The first write offered when room frees goes
 * in whole whatever its size -- the bridge offers them best priority first,
 * so a large object is never passed over by smaller, less urgent ones -- and
 * the backlog overshoots the cap by at most that one write. */
static bool pq_would_exceed(const moq_pq_send_queue_t *q, size_t len)
{
    return len > 0 && q->queued >= q->cap;
}

/* A keyed stream starts only while at most PQ_RANK_MAX keyed streams hold
 * bytes, so those -- ranks 0 through PQ_RANK_MAX -- keep distinct levels; the
 * bridge holds the rest in priority order. Started streams are never refused,
 * so they always finish (and may share the last level). */
static bool pq_ranks_full(const moq_pq_send_queue_t *q, const pq_stream_t *s)
{
    return s && s->unstarted && s->priority_key &&
           q->keyed_holding > PQ_RANK_MAX;
}

/* Enqueue `c`; a stream's first bytes give it a rank. */
static void pq_append(moq_pq_send_queue_t *q, pq_stream_t *s, pq_chunk_t *c)
{
    if (s->unstarted) {
        s->unstarted = false;
        q->priorities_dirty = true;
    }
    if (!s->head && s->priority_key) {
        s->counted = true;
        q->keyed_holding++;
    }
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
    pq_append(q, s, c);
    return 1;
}

int moq_pq_send_queue_push_copy(moq_pq_send_queue_t *q, uint64_t sid,
                                const uint8_t *data, size_t len, bool fin)
{
    if (!q) return -1;
    /* A zero-length non-FIN write carries nothing; accept it without touching
     * a stream slot (creating one would leak an empty in_use entry). */
    if (len == 0 && !fin) return 1;
    pq_stream_t *s = pq_find(q, sid);
    if (pq_would_exceed(q, len) || pq_ranks_full(q, s)) {
        q->would_block++;
        return 0;
    }
    if (!s && !(s = pq_find_or_create(q, sid))) return -1;

    if (len == 0)                         /* len == 0 && fin */
        return pq_push_bare_fin(q, s);

    pq_chunk_t *c = (pq_chunk_t *)q->alloc.alloc(sizeof(*c), q->alloc.ctx);
    if (!c) { if (!s->head) pq_stream_clear(q, s); return -1; }
    memset(c, 0, sizeof(*c));
    c->copied = (uint8_t *)q->alloc.alloc(len, q->alloc.ctx);
    if (!c->copied) {
        q->alloc.free(c, sizeof(*c), q->alloc.ctx);
        if (!s->head) pq_stream_clear(q, s);
        return -1;
    }
    if (pq_offset_note(q, sid, len) != 0) {
        q->alloc.free(c->copied, len, q->alloc.ctx);
        q->alloc.free(c, sizeof(*c), q->alloc.ctx);
        if (!s->head) pq_stream_clear(q, s);
        return -1;
    }
    memcpy(c->copied, data, len);
    c->copy_len = len;
    c->data = c->copied;
    c->len = len;
    c->fin = fin;
    pq_append(q, s, c);
    s->remaining += len;
    q->queued += len;
    pq_note_high_water(q);
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

    pq_stream_t *s = pq_find(q, sid);
    if (pq_would_exceed(q, len) || pq_ranks_full(q, s)) {
        q->would_block++;
        return 0;
    }
    if (!s && !(s = pq_find_or_create(q, sid))) return -1;

    pq_chunk_t *c = (pq_chunk_t *)q->alloc.alloc(sizeof(*c), q->alloc.ctx);
    if (!c) { if (!s->head) pq_stream_clear(q, s); return -1; }
    memset(c, 0, sizeof(*c));
    if (pq_offset_note(q, sid, len) != 0) {
        q->alloc.free(c, sizeof(*c), q->alloc.ctx);
        if (!s->head) pq_stream_clear(q, s);
        return -1;
    }
    moq_rcbuf_incref(buf);
    c->rcbuf = buf;
    c->data = moq_rcbuf_data(buf);
    c->len = len;
    c->fin = fin;
    pq_append(q, s, c);
    s->remaining += len;
    q->queued += len;
    pq_note_high_water(q);
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
    bool fin_handed = false;
    pq_offset_t *o = pq_offset_find(q, sid);
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
            if (o) o->provided += take;
        }
        if (c->offset == c->len) {
            /* The FIN rides on the last chunk: popping it hands the FIN to
             * the provider (the matching plan reported is_fin). */
            if (c->fin) fin_handed = true;
            s->head = c->next;
            if (!s->head) s->tail = NULL;
            pq_chunk_free(q, c);
        } else {
            break;   /* partial data chunk; stop */
        }
    }
    if (!s->head)
        pq_stream_clear(q, s);   /* drop the now-empty stream slot */
    /* Nothing of the stream remains to be provided: its offset record ends
     * here. Before the FIN is handed over the record is kept, even while the
     * queue is momentarily empty, because later pushes continue the count. */
    if (fin_handed)
        pq_offset_forget(q, sid);
}

void moq_pq_send_queue_drop(moq_pq_send_queue_t *q, uint64_t sid)
{
    if (!q) return;
    pq_stream_t *s = pq_find(q, sid);
    if (s) pq_stream_clear(q, s);
    pq_offset_forget(q, sid);
}

uint64_t moq_pq_send_queue_truncate(moq_pq_send_queue_t *q, uint64_t sid,
                                    uint64_t reliable)
{
    if (!q) return 0;
    pq_stream_t *s = pq_find(q, sid);
    const pq_offset_t *o = pq_offset_find(q, sid);
    if (!s) {                             /* everything pushed was provided */
        pq_offset_forget(q, sid);
        return 0;
    }
    /* Provided so far: bytes handed over at commit. Bytes an earlier
     * truncate discarded were never handed over and do not count, so a
     * repeated or tighter truncate still keeps what is owed. */
    uint64_t provided = o ? o->provided : 0;
    uint64_t keep = reliable > provided ? reliable - provided : 0;
    if (keep == 0) {
        pq_stream_clear(q, s);
        pq_offset_forget(q, sid);
        return 0;
    }

    uint64_t left = keep;
    pq_chunk_t *prev = NULL, *c = s->head;
    while (c && left > 0) {
        size_t rem = c->len - c->offset;
        if ((uint64_t)rem > left) {
            /* Shorten this chunk; its tail stays allocated but is never
             * provided (len bounds every read). */
            size_t cut = rem - (size_t)left;
            c->len -= cut;
            s->remaining -= cut;
            q->queued = q->queued >= cut ? q->queued - cut : 0;
            left = 0;
        } else {
            left -= rem;
        }
        c->fin = false;
        prev = c;
        c = c->next;
    }
    /* Release everything after the kept prefix. */
    while (c) {
        pq_chunk_t *n = c->next;
        size_t rem = c->len - c->offset;
        s->remaining -= rem;
        q->queued = q->queued >= rem ? q->queued - rem : 0;
        pq_chunk_free(q, c);
        c = n;
    }
    if (prev) prev->next = NULL;
    s->tail = prev;
    if (!prev) s->head = NULL;
    if (s->remaining == 0) {
        pq_stream_clear(q, s);
        pq_offset_forget(q, sid);
        return 0;
    }
    return s->remaining;
}

uint64_t moq_pq_send_queue_queued_bytes(const moq_pq_send_queue_t *q)
{
    return q ? q->queued : 0;
}

uint64_t moq_pq_send_queue_high_water(const moq_pq_send_queue_t *q)
{
    return q ? q->high_water : 0;
}

uint64_t moq_pq_send_queue_would_block_count(const moq_pq_send_queue_t *q)
{
    return q ? q->would_block : 0;
}
