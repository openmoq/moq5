/*
 * Send-queue reliable-prefix accounting across the stream's FIN.
 *
 * moq_pq_send_queue_truncate keeps only the part of a reliable prefix the
 * provider has not yet been handed. The count of bytes handed over must
 * survive the FIN being pushed: a reset requested after that (a subgroup
 * delivery timeout that runs from the FIN, draft-ietf-moq-transport-22 §5.2)
 * still needs it. The count ends when the FIN itself is handed over, on drop,
 * on a truncate that leaves nothing, and on destroy; it is never retired by a
 * momentarily empty queue before the FIN, since later pushes continue it.
 *
 * "Handed over" is the provide-data commit; nothing here models peer receipt
 * or acknowledgement.
 */

#include "moq_pq_send_queue.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* Size-aware accounting allocator. Every live block is recorded with its
 * size; a free or realloc that names a different size than the block was
 * allocated with is counted as a mismatch. g_live is the live block count,
 * g_live_bytes their total size, g_largest the largest block ever requested,
 * so a record that is never released shows up as growth of the record
 * table. g_refuse_realloc > 0 refuses that many next reallocs (leaving the
 * old block untouched, as realloc does); g_reallocs counts realloc calls and
 * g_refused_old / g_refused_new record the sizes of the last refused one, so
 * a row can show which allocation it refused. */
#define BLOCKS_MAX 8192
static struct { void *p; size_t n; } g_blocks[BLOCKS_MAX];
static size_t g_nblocks = 0;
static long g_live = 0;
static size_t g_live_bytes = 0;
static size_t g_largest = 0;
static int g_size_mismatch = 0;
static int g_refuse_realloc = 0;
static int g_reallocs = 0;
static size_t g_refused_old = 0, g_refused_new = 0;
static void note(size_t n) { if (n > g_largest) g_largest = n; }
static void track_add(void *p, size_t n)
{
    if (!p) return;
    CHECK(g_nblocks < BLOCKS_MAX);
    if (g_nblocks >= BLOCKS_MAX) return;
    g_blocks[g_nblocks].p = p; g_blocks[g_nblocks].n = n; g_nblocks++;
    g_live++; g_live_bytes += n;
}
static void track_del(void *p, size_t n)
{
    if (!p) return;
    for (size_t i = 0; i < g_nblocks; i++) {
        if (g_blocks[i].p != p) continue;
        if (g_blocks[i].n != n) g_size_mismatch++;
        g_live--; g_live_bytes -= g_blocks[i].n;
        g_blocks[i] = g_blocks[--g_nblocks];
        return;
    }
    g_size_mismatch++;   /* freeing a block this allocator never handed out */
}
static void *ca(size_t n, void *c)
{ (void)c; note(n); void *p = malloc(n ? n : 1); track_add(p, n); return p; }
/* The old block's record is found while its pointer is still valid; a
 * successful realloc moves that record to the new block, a failed one
 * leaves it as it was. */
static void *cr(void *p, size_t o, size_t n, void *c)
{
    (void)c;
    note(n);
    g_reallocs++;
    if (g_refuse_realloc > 0) {
        g_refuse_realloc--;
        g_refused_old = o; g_refused_new = n;
        return NULL;
    }
    size_t rec = g_nblocks;
    for (size_t i = 0; p && i < g_nblocks; i++)
        if (g_blocks[i].p == p) { rec = i; break; }
    void *r = realloc(p, n ? n : 1);
    if (!r) return NULL;
    if (!p) {
        track_add(r, n);
    } else if (rec == g_nblocks) {
        g_size_mismatch++;   /* resizing a block this allocator never handed out */
        track_add(r, n);
    } else {
        if (g_blocks[rec].n != o) g_size_mismatch++;
        g_live_bytes -= g_blocks[rec].n;
        g_blocks[rec].p = r; g_blocks[rec].n = n;
        g_live_bytes += n;
    }
    return r;
}
static void cf(void *p, size_t n, void *c) { (void)c; track_del(p, n); free(p); }
static const moq_alloc_t AL = { .alloc = ca, .realloc = cr, .free = cf, .ctx = NULL };

/* Releases of the external storage behind wrapped rcbufs. */
static int g_released = 0;
static void on_release(void *ctx, const uint8_t *data, size_t len)
{ (void)ctx; (void)data; (void)len; g_released++; }
static int noop_apply(void *ctx, uint64_t sid, uint8_t priority)
{ (void)ctx; (void)sid; (void)priority; return 0; }

/* The send queue's per-stream offset record is {sid, provided}: two uint64_t.
 * Its table starts at eight records and doubles. The sizes below identify a
 * refused realloc as that table's, not the stream table's. */
#define RECORD_BYTES (2 * sizeof(uint64_t))

static const uint8_t HDR[5] = { 1, 2, 3, 4, 5 };
static const uint8_t OBJ[7] = { 9, 9, 9, 9, 9, 9, 9 };

/* The bytes the last give() handed over. */
static uint8_t g_out[64];

/* Hand over up to `max` bytes of `sid`, as a provide-data callback would,
 * copying them to g_out; returns the byte count and reports whether the FIN
 * went with them. */
static size_t give(moq_pq_send_queue_t *q, uint64_t sid, size_t max, bool *fin)
{
    size_t nb = 0; bool is_fin = false, still = false;
    memset(g_out, 0, sizeof(g_out));
    if (!moq_pq_send_queue_plan(q, sid, max, &nb, &is_fin, &still)) {
        if (fin) *fin = false;
        return 0;
    }
    CHECK(nb <= sizeof(g_out));
    moq_pq_send_queue_commit(q, sid, nb <= sizeof(g_out) ? g_out : NULL, nb);
    if (fin) *fin = is_fin;
    return nb;
}

/* Header handed over, object queued, then a FIN: whether bare or carried by
 * the object's write, truncate(header) keeps nothing. */
static void row_prefix_provided(bool fin_with_bytes)
{
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&AL, 0);
    CHECK(q != NULL);
    CHECK(moq_pq_send_queue_push_copy(q, 4, HDR, 5, false) == 1);
    CHECK(give(q, 4, 64, NULL) == 5);
    CHECK(moq_pq_send_queue_push_copy(q, 4, OBJ, 7, fin_with_bytes) == 1);
    if (!fin_with_bytes)
        CHECK(moq_pq_send_queue_push_copy(q, 4, NULL, 0, true) == 1);
    CHECK(moq_pq_send_queue_truncate(q, 4, 5) == 0);
    CHECK(!moq_pq_send_queue_has_data(q, 4));
    CHECK(moq_pq_send_queue_queued_bytes(q) == 0);
    moq_pq_send_queue_destroy(q);
}

/* Three header bytes handed over, then the object with the FIN: exactly the
 * two owed header bytes stay, without the FIN. */
static void row_prefix_partial(void)
{
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&AL, 0);
    CHECK(moq_pq_send_queue_push_copy(q, 8, HDR, 5, false) == 1);
    CHECK(give(q, 8, 3, NULL) == 3);
    CHECK(memcmp(g_out, HDR, 3) == 0);
    CHECK(moq_pq_send_queue_push_copy(q, 8, OBJ, 7, true) == 1);
    CHECK(moq_pq_send_queue_truncate(q, 8, 5) == 2);
    bool fin = true;
    size_t nb = 0; bool is_fin = false, still = false;
    CHECK(moq_pq_send_queue_plan(q, 8, 64, &nb, &is_fin, &still));
    CHECK(nb == 2 && !is_fin && !still);
    CHECK(give(q, 8, 64, &fin) == 2);
    CHECK(memcmp(g_out, HDR + 3, 2) == 0);   /* the owed header bytes, exactly */
    CHECK(!fin);
    CHECK(!moq_pq_send_queue_has_data(q, 8));
    moq_pq_send_queue_destroy(q);
}

/* A second reset of the same size before anything more is handed over keeps
 * the same owed bytes: discarded bytes were never provided. A tighter prefix
 * then keeps only what it still owes. */
static void row_repeat_and_tighter(void)
{
    uint8_t twelve[12];
    for (size_t i = 0; i < sizeof(twelve); i++) twelve[i] = (uint8_t)(0x40 + i);
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&AL, 0);
    CHECK(moq_pq_send_queue_push_copy(q, 28, twelve, 12, false) == 1);
    CHECK(give(q, 28, 3, NULL) == 3);
    CHECK(memcmp(g_out, twelve, 3) == 0);
    CHECK(moq_pq_send_queue_truncate(q, 28, 5) == 2);
    CHECK(moq_pq_send_queue_truncate(q, 28, 5) == 2);   /* same size again */
    CHECK(moq_pq_send_queue_queued_bytes(q) == 2);
    CHECK(moq_pq_send_queue_truncate(q, 28, 4) == 1);   /* tighter: one owed */
    CHECK(give(q, 28, 64, NULL) == 1);
    CHECK(g_out[0] == twelve[3]);
    CHECK(!moq_pq_send_queue_has_data(q, 28));
    moq_pq_send_queue_destroy(q);

    /* The same with the FIN already pushed behind the bytes. */
    q = moq_pq_send_queue_create(&AL, 0);
    CHECK(moq_pq_send_queue_push_copy(q, 32, twelve, 12, true) == 1);
    CHECK(give(q, 32, 3, NULL) == 3);
    CHECK(moq_pq_send_queue_truncate(q, 32, 5) == 2);
    CHECK(moq_pq_send_queue_truncate(q, 32, 5) == 2);
    bool fin = true;
    CHECK(give(q, 32, 64, &fin) == 2);
    CHECK(memcmp(g_out, twelve + 3, 2) == 0);
    CHECK(!fin);
    moq_pq_send_queue_destroy(q);
}

/* The queue runs empty between writes; the count continues across it, and
 * across a FIN pushed after the last write. */
static void row_empty_then_append(void)
{
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&AL, 0);
    CHECK(moq_pq_send_queue_push_copy(q, 12, HDR, 5, false) == 1);
    CHECK(give(q, 12, 64, NULL) == 5);
    CHECK(!moq_pq_send_queue_has_data(q, 12));
    CHECK(moq_pq_send_queue_push_copy(q, 12, OBJ, 4, false) == 1);
    CHECK(give(q, 12, 64, NULL) == 4);
    CHECK(moq_pq_send_queue_push_copy(q, 12, OBJ, 7, true) == 1);
    /* Nine bytes handed over: a nine-byte prefix keeps nothing, a twelve-byte
     * prefix keeps three. */
    CHECK(moq_pq_send_queue_truncate(q, 12, 12) == 3);
    moq_pq_send_queue_destroy(q);

    q = moq_pq_send_queue_create(&AL, 0);
    CHECK(moq_pq_send_queue_push_copy(q, 12, HDR, 5, false) == 1);
    CHECK(give(q, 12, 64, NULL) == 5);
    CHECK(moq_pq_send_queue_push_copy(q, 12, OBJ, 4, false) == 1);
    CHECK(give(q, 12, 64, NULL) == 4);
    CHECK(moq_pq_send_queue_push_copy(q, 12, OBJ, 7, true) == 1);
    CHECK(moq_pq_send_queue_truncate(q, 12, 9) == 0);
    moq_pq_send_queue_destroy(q);
}

/* A FIN handed over ends the stream's count; dropping a stream ends it too;
 * a sibling stream's count is untouched by either. */
static void row_handover_drop_sibling(void)
{
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&AL, 0);
    /* Sibling 20: header handed over, object and FIN still queued. */
    CHECK(moq_pq_send_queue_push_copy(q, 20, HDR, 5, false) == 1);
    CHECK(give(q, 20, 64, NULL) == 5);
    CHECK(moq_pq_send_queue_push_copy(q, 20, OBJ, 7, true) == 1);
    /* Stream 16 runs to its FIN; stream 24 is dropped. */
    bool fin = false;
    CHECK(moq_pq_send_queue_push_copy(q, 16, HDR, 5, true) == 1);
    CHECK(give(q, 16, 64, &fin) == 5);
    CHECK(fin);
    CHECK(moq_pq_send_queue_push_copy(q, 24, HDR, 5, false) == 1);
    CHECK(give(q, 24, 2, NULL) == 2);
    moq_pq_send_queue_drop(q, 24);
    CHECK(!moq_pq_send_queue_has_data(q, 24));
    /* The sibling still knows its header went out. */
    CHECK(moq_pq_send_queue_truncate(q, 20, 5) == 0);
    moq_pq_send_queue_destroy(q);
}

/* Many streams, each run to its FIN or dropped: the count table does not
 * grow with the number of streams that have ended. */
static void row_bounded(void)
{
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&AL, 0);
    for (uint64_t i = 0; i < 2000; i++) {
        uint64_t sid = 2 + 4 * i;
        CHECK(moq_pq_send_queue_push_copy(q, sid, HDR, 5, false) == 1);
        CHECK(give(q, sid, 64, NULL) == 5);
        if (i % 2) {
            CHECK(moq_pq_send_queue_push_copy(q, sid, NULL, 0, true) == 1);
            bool fin = false;
            (void)give(q, sid, 64, &fin);
            CHECK(fin);
        } else {
            moq_pq_send_queue_drop(q, sid);
        }
    }
    /* No block here exceeds a few hundred bytes; 2000 live records would
     * need a table of well over 16 KiB. */
    CHECK(g_largest < 1024);
    moq_pq_send_queue_destroy(q);
}

/* rcbuf storage: a partial header handover, then repeated and tighter
 * truncation with one more byte handed over in between. Exact bytes at each
 * handover; the queue holds one reference while a chunk is queued and
 * releases it when the chunk is discarded or drained. */
static void row_rcbuf_partial(void)
{
    static uint8_t twelve[12];
    for (size_t i = 0; i < sizeof(twelve); i++) twelve[i] = (uint8_t)(0x60 + i);
    g_released = 0;
    moq_rcbuf_t *buf = NULL;
    CHECK(moq_rcbuf_wrap(&AL, twelve, sizeof(twelve), on_release, NULL, &buf) == MOQ_OK);
    if (!buf) return;
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&AL, 0);
    CHECK(moq_pq_send_queue_push_rcbuf(q, 44, buf, true) == 1);
    CHECK(moq_rcbuf_refcount(buf) == 2);
    CHECK(give(q, 44, 3, NULL) == 3);
    CHECK(memcmp(g_out, twelve, 3) == 0);
    CHECK(moq_pq_send_queue_truncate(q, 44, 5) == 2);
    CHECK(moq_pq_send_queue_truncate(q, 44, 5) == 2);   /* repeated */
    CHECK(moq_rcbuf_refcount(buf) == 2);                /* kept chunk holds it */
    CHECK(give(q, 44, 1, NULL) == 1);                   /* one more handed over */
    CHECK(g_out[0] == twelve[3]);
    CHECK(moq_pq_send_queue_truncate(q, 44, 5) == 1);
    CHECK(moq_pq_send_queue_truncate(q, 44, 4) == 0);   /* tighter: nothing owed */
    CHECK(!moq_pq_send_queue_has_data(q, 44));
    CHECK(moq_rcbuf_refcount(buf) == 1);                /* queue's ref released */
    CHECK(g_released == 0);
    moq_rcbuf_decref(buf);
    CHECK(g_released == 1);
    moq_pq_send_queue_destroy(q);

    /* Header and object as separate rcbufs: truncation releases the object's
     * reference at once and keeps the header's until it drains. */
    g_released = 0;
    moq_rcbuf_t *hb = NULL, *ob = NULL;
    CHECK(moq_rcbuf_wrap(&AL, HDR, sizeof(HDR), on_release, NULL, &hb) == MOQ_OK);
    CHECK(moq_rcbuf_wrap(&AL, OBJ, sizeof(OBJ), on_release, NULL, &ob) == MOQ_OK);
    if (!hb || !ob) return;
    q = moq_pq_send_queue_create(&AL, 0);
    CHECK(moq_pq_send_queue_push_rcbuf(q, 48, hb, false) == 1);
    CHECK(moq_pq_send_queue_push_rcbuf(q, 48, ob, true) == 1);
    CHECK(give(q, 48, 3, NULL) == 3);
    CHECK(moq_pq_send_queue_truncate(q, 48, 5) == 2);
    CHECK(moq_rcbuf_refcount(ob) == 1);
    CHECK(moq_rcbuf_refcount(hb) == 2);
    bool fin = true;
    CHECK(give(q, 48, 64, &fin) == 2);
    CHECK(memcmp(g_out, HDR + 3, 2) == 0 && !fin);
    CHECK(moq_rcbuf_refcount(hb) == 1);
    moq_rcbuf_decref(hb);
    moq_rcbuf_decref(ob);
    CHECK(g_released == 2);
    moq_pq_send_queue_destroy(q);
}

/* The first offset record is refused: the push fails, nothing is accepted or
 * retained (bytes, reference, allocation), and a retry succeeds with a
 * correct count. The stream slot is created beforehand so the record table is
 * the only realloc the push performs. */
static void row_record_refused(bool rcbuf)
{
    g_released = 0;
    moq_rcbuf_t *buf = NULL;
    if (rcbuf)
        CHECK(moq_rcbuf_wrap(&AL, HDR, sizeof(HDR), on_release, NULL, &buf) == MOQ_OK);
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&AL, 0);
    CHECK(moq_pq_send_queue_priority(q, 52, 0, noop_apply, NULL) == 0);
    long blocks = g_live; size_t bytes = g_live_bytes;
    int reallocs = g_reallocs;
    g_refused_old = g_refused_new = 0;
    g_refuse_realloc = 1;
    int r = rcbuf ? moq_pq_send_queue_push_rcbuf(q, 52, buf, false)
                  : moq_pq_send_queue_push_copy(q, 52, HDR, sizeof(HDR), false);
    CHECK(r == -1);
    /* Exactly one realloc, refused, and it was the first record table. */
    CHECK(g_refuse_realloc == 0);
    CHECK(g_reallocs == reallocs + 1);
    CHECK(g_refused_old == 0 && g_refused_new == 8 * RECORD_BYTES);
    g_refuse_realloc = 0;
    CHECK(!moq_pq_send_queue_has_data(q, 52));
    CHECK(moq_pq_send_queue_queued_bytes(q) == 0);
    CHECK(g_live == blocks && g_live_bytes == bytes);
    if (rcbuf) CHECK(moq_rcbuf_refcount(buf) == 1);
    r = rcbuf ? moq_pq_send_queue_push_rcbuf(q, 52, buf, true)
              : moq_pq_send_queue_push_copy(q, 52, HDR, sizeof(HDR), true);
    CHECK(r == 1);
    CHECK(give(q, 52, 3, NULL) == 3);
    CHECK(moq_pq_send_queue_truncate(q, 52, 5) == 2);
    CHECK(give(q, 52, 64, NULL) == 2);
    CHECK(memcmp(g_out, HDR + 3, 2) == 0);
    moq_pq_send_queue_destroy(q);
    if (rcbuf) {
        CHECK(moq_rcbuf_refcount(buf) == 1);
        moq_rcbuf_decref(buf);
        CHECK(g_released == 1);
    }
}

/* Growth of the record table is refused at the ninth live record: the push
 * fails with nothing accepted, the eight existing records are intact, and a
 * retry succeeds. */
static void row_record_growth_refused(bool rcbuf)
{
    g_released = 0;
    moq_pq_send_queue_t *q = moq_pq_send_queue_create(&AL, 0);
    for (uint64_t i = 0; i < 8; i++)
        CHECK(moq_pq_send_queue_push_copy(q, 100 + 4 * i, HDR, sizeof(HDR), false) == 1);
    CHECK(give(q, 100, 2, NULL) == 2);
    CHECK(moq_pq_send_queue_priority(q, 200, 0, noop_apply, NULL) == 0);
    moq_rcbuf_t *buf = NULL;
    if (rcbuf)
        CHECK(moq_rcbuf_wrap(&AL, OBJ, sizeof(OBJ), on_release, NULL, &buf) == MOQ_OK);
    long blocks = g_live; size_t bytes = g_live_bytes;
    uint64_t queued = moq_pq_send_queue_queued_bytes(q);
    int reallocs = g_reallocs;
    g_refused_old = g_refused_new = 0;
    g_refuse_realloc = 1;
    int r = rcbuf ? moq_pq_send_queue_push_rcbuf(q, 200, buf, true)
                  : moq_pq_send_queue_push_copy(q, 200, OBJ, sizeof(OBJ), true);
    CHECK(r == -1);
    /* Exactly one realloc, refused, and it was the record table's growth
     * from eight to sixteen records. */
    CHECK(g_refuse_realloc == 0);
    CHECK(g_reallocs == reallocs + 1);
    CHECK(g_refused_old == 8 * RECORD_BYTES && g_refused_new == 16 * RECORD_BYTES);
    g_refuse_realloc = 0;
    CHECK(!moq_pq_send_queue_has_data(q, 200));
    CHECK(moq_pq_send_queue_queued_bytes(q) == queued);
    CHECK(g_live == blocks && g_live_bytes == bytes);
    if (rcbuf) CHECK(moq_rcbuf_refcount(buf) == 1);
    /* An existing record still counts its handover exactly. */
    CHECK(moq_pq_send_queue_truncate(q, 100, 5) == 3);
    CHECK(give(q, 100, 64, NULL) == 3);
    CHECK(memcmp(g_out, HDR + 2, 3) == 0);
    r = rcbuf ? moq_pq_send_queue_push_rcbuf(q, 200, buf, true)
              : moq_pq_send_queue_push_copy(q, 200, OBJ, sizeof(OBJ), true);
    CHECK(r == 1);
    CHECK(give(q, 200, 4, NULL) == 4);
    CHECK(moq_pq_send_queue_truncate(q, 200, 6) == 2);
    moq_pq_send_queue_destroy(q);
    if (rcbuf) {
        CHECK(moq_rcbuf_refcount(buf) == 1);
        moq_rcbuf_decref(buf);
        CHECK(g_released == 1);
    }
}

/* The accounting allocator itself: a successful realloc moves the block's
 * record to the new size, a refused one leaves it, a realloc or free naming
 * the wrong old size is a mismatch, and a free ends the record. */
static void row_allocator_accounting(void)
{
    long blocks = g_live; size_t bytes = g_live_bytes;
    int mismatches = g_size_mismatch;
    void *a = AL.alloc(16, NULL);
    CHECK(a != NULL && g_live == blocks + 1 && g_live_bytes == bytes + 16);
    void *b = AL.realloc(a, 16, 64, NULL);
    CHECK(b != NULL && g_live == blocks + 1 && g_live_bytes == bytes + 64);
    g_refuse_realloc = 1;
    CHECK(AL.realloc(b, 64, 128, NULL) == NULL);
    CHECK(g_live == blocks + 1 && g_live_bytes == bytes + 64);
    void *d = AL.realloc(b, 63, 32, NULL);
    CHECK(d != NULL && g_size_mismatch == mismatches + 1);
    CHECK(g_live == blocks + 1 && g_live_bytes == bytes + 32);
    void *e = AL.realloc(NULL, 0, 8, NULL);
    CHECK(e != NULL && g_live == blocks + 2 && g_live_bytes == bytes + 40);
    AL.free(e, 8, NULL);
    AL.free(d, 32, NULL);
    CHECK(g_live == blocks && g_live_bytes == bytes);
    CHECK(g_size_mismatch == mismatches + 1);
    g_size_mismatch = mismatches;
}

int main(void)
{
    row_allocator_accounting();
    row_prefix_provided(false);
    row_prefix_provided(true);
    row_prefix_partial();
    row_repeat_and_tighter();
    row_empty_then_append();
    row_handover_drop_sibling();
    row_bounded();
    row_rcbuf_partial();
    row_record_refused(false);
    row_record_refused(true);
    row_record_growth_refused(false);
    row_record_growth_refused(true);
    CHECK(g_live == 0);
    CHECK(g_live_bytes == 0);
    CHECK(g_size_mismatch == 0);
    if (failures) {
        fprintf(stderr, "test_pq_send_queue_offsets: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_pq_send_queue_offsets: PASS\n");
    return 0;
}
