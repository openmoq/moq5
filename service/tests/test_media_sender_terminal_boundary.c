/*
 * Measured boundaries of the media sender, no network:
 *
 * t1  Terminal endpoint: every app-thread mutator on a sender whose endpoint
 *     is closed (stopped) refuses with MOQ_ERR_CLOSED, including
 *     moq_media_sender_add_track, which must make NO allocation request and
 *     register nothing. A live add beforehand is the positive control.
 * t2  Terminal transition INSIDE add_track: the endpoint closes after the
 *     initial state checks and before registration (test-only gap hook);
 *     the prepared track is refused, registered nowhere, and every block it
 *     allocated is freed with its exact size.
 * d1  Extreme BLOCK_TIMEOUT deadline (UINT64_MAX): a blocked write still
 *     observes the sender's terminal transition and returns MOQ_ERR_CLOSED
 *     without ownership transfer.
 * d2  Minimal BLOCK_TIMEOUT deadline (1 us) on a full queue returns
 *     MOQ_ERR_WOULD_BLOCK, no transfer, one stall counted.
 *
 * Allocation oracle (t1/t2): the sender is constructed through the test
 * allocator seam, so one LEDGER lives from construction through destruction
 * and records every request (alloc and realloc) with its size, validates
 * every free against the block it supplied (pointer AND size), and reports
 * outstanding blocks. Media-track boundary only: the generated SAP/media
 * timeline siblings are not configured here, so their refusal-path cleanup
 * rests on source review, not on this ledger.
 *
 * Hang bound: the registered CTest row carries a finite TIMEOUT; the fixture
 * itself never cancels or frees state a worker may still touch. The
 * wait-entry handshake proves the write reached the BLOCKING PATH of the
 * BLOCK_TIMEOUT policy (the test-only hook fires with s->mu held, before the
 * deadline is constructed); it does not observe the kernel wait itself.
 * Every write's payload ownership is settled per the transfer-on-OK contract.
 *
 * White-box: links moq-service-sender-test-internals (MOQ_MEDIA_SENDER_TESTING).
 */
#define _POSIX_C_SOURCE 200809L
#include <moq/media_sender.h>
#include "test_support.h"
#include <pthread.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>

extern char **environ;

/* Worker-join boundary. A failed join does not prove the worker retired, so
 * the fixture never continues past it: it prints the named diagnostic and
 * ends the PROCESS without destroying the handshake, clearing the hook, or
 * releasing anything the worker may still touch. The join call is routed
 * through this pointer so the join-failure control can inject EINVAL while a
 * created worker is still unjoined. */
static int (*g_join)(pthread_t, void **) = pthread_join;
static int refusing_join(pthread_t th, void **ret) { (void)th; (void)ret; return EINVAL; }
static const char JOIN_FAILED_EXIT[] = "media_sender_terminal_boundary: worker join failed; ending the process without cleanup";

static int failures = 0;

moq_media_sender_t *moq_media_sender_test_new_cfg(const moq_media_sender_cfg_t *cfg);
moq_result_t moq_media_sender_test_construct(const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_sender_test_set_ep(moq_media_sender_t *s, moq_endpoint_t *ep);
void moq_media_sender_test_set_fatal(moq_media_sender_t *s, bool fatal);
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
void moq_media_sender_test_set_wait_entry_hook(void (*fn)(void *), void *ctx);
void moq_media_sender_test_set_add_track_gap_hook(void (*fn)(void *), void *ctx);
size_t moq_media_sender_test_track_count(moq_media_sender_t *s);

static void row(const char *what, const char *name, bool ok, int line)
{
    if (ok) return;
    fprintf(stderr, "FAIL[%s.%s]: %s:%d\n", what, name, __FILE__, line);
    failures++;
}
#define ROW(what, name, cond) row((what), (name), (cond), __LINE__)

/* -- allocation ledger (alloc + realloc, exact-size frees) ------------------ */
#define MAX_REQ 64
typedef struct { void *ptr; size_t size; bool live; int request; } block_t;
typedef struct {
    size_t  requested[MAX_REQ];
    int     requests;
    block_t blocks[MAX_REQ];
    int     supplied;
    int     freed;
    int     overflow;
    int     bad_free;             /* unknown pointer or wrong size */
} ledger_t;
static ledger_t g_ledger;
static moq_alloc_t g_alloc;

static void *ledger_alloc(size_t size, void *ctx)
{
    ledger_t *l = (ledger_t *)ctx;
    int i = l->requests++;
    if (i >= MAX_REQ) { l->overflow++; return NULL; }
    l->requested[i] = size;
    void *p = malloc(size ? size : 1);
    if (!p) return NULL;
    l->blocks[l->supplied++] = (block_t){ p, size, true, i };
    return p;
}
static bool ledger_release(ledger_t *l, void *p, size_t size)
{
    for (int i = 0; i < l->supplied; i++) {
        block_t *b = &l->blocks[i];
        if (b->live && b->ptr == p) {
            if (b->size != size) l->bad_free++;
            b->live = false;
            l->freed++;
            free(p);
            return true;
        }
    }
    l->bad_free++;
    return false;
}
static void ledger_free(void *p, size_t size, void *ctx)
{
    if (!p) return;
    (void)ledger_release((ledger_t *)ctx, p, size);
}
static void *ledger_realloc(void *p, size_t old, size_t nw, void *ctx)
{
    ledger_t *l = (ledger_t *)ctx;
    void *np = ledger_alloc(nw, ctx);
    if (!np) return NULL;
    if (p) {
        memcpy(np, p, old < nw ? old : nw);
        (void)ledger_release(l, p, old);
    }
    return np;
}
static void arm(void)
{
    memset(&g_ledger, 0, sizeof(g_ledger));
    g_alloc.alloc = ledger_alloc; g_alloc.free = ledger_free;
    g_alloc.realloc = ledger_realloc; g_alloc.ctx = &g_ledger;
    moq_media_sender_test_set_alloc(&g_alloc);
}
static void disarm(void) { moq_media_sender_test_set_alloc(NULL); }
static int outstanding(void) { return g_ledger.supplied - g_ledger.freed; }
typedef struct { int requests, supplied, freed, bad_free; } snap_t;
static snap_t snap(void)
{
    return (snap_t){ g_ledger.requests, g_ledger.supplied, g_ledger.freed, g_ledger.bad_free };
}
static void dump(const char *what)
{
    fprintf(stderr, "  ledger[%s]: requests=%d supplied=%d freed=%d outstanding=%d bad_free=%d overflow=%d sizes=",
            what, g_ledger.requests, g_ledger.supplied, g_ledger.freed, outstanding(),
            g_ledger.bad_free, g_ledger.overflow);
    int n = g_ledger.requests < MAX_REQ ? g_ledger.requests : MAX_REQ;
    for (int i = 0; i < n; i++) fprintf(stderr, "%s%zu", i ? "," : "", g_ledger.requested[i]);
    fprintf(stderr, "\n");
}

static const uint8_t NS0[] = "live";
static const uint8_t NAME[] = "video";
static const uint8_t CODEC[] = "avc1.64001f";
#define TRACK_STRINGS (5 + 11)   /* name + codec bytes copied into one buffer */

static void track_cfg(moq_media_track_cfg_t *tc)
{
    moq_media_track_cfg_init(tc);
    tc->name = (moq_bytes_t){ NAME, 5 };
    tc->media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc->packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc->codec = (moq_bytes_t){ CODEC, 11 };
    tc->bitrate = 1000000;
}

static moq_rcbuf_t *payload(size_t n)
{
    static const uint8_t bytes[64] = { 1, 2, 3, 4 };
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), bytes, n, &b) != MOQ_OK) return NULL;
    return b;
}

static void send_obj(moq_media_send_object_t *o, moq_rcbuf_t *p, bool sync, bool starts)
{
    memset(o, 0, sizeof(*o));
    o->struct_size = sizeof(*o);
    o->payload = p;
    o->is_sync = sync;
    o->starts_group = starts;
    o->presentation_time_us = 1000;
}

static void count_gap(void *ctx) { (*(int *)ctx)++; }

/* Sender under the ledger, pointed at a bare endpoint (NOT hook-attached:
 * test_set_ep bypasses attachment, which is what lets the public
 * moq_endpoint_stop succeed; these rows measure MUTATOR behaviour on a closed
 * endpoint, not endpoint lifecycle). */
static bool make_ledger_sender(const char *what, moq_media_sender_t **s, moq_endpoint_t **ep)
{
    static const moq_bytes_t parts[1] = { { NS0, 4 } };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_.parts = parts; cfg.namespace_.count = 1;
    arm();
    *s = NULL;
    moq_result_t rc = moq_media_sender_test_construct(&cfg, s);
    disarm();                     /* the sender keeps freeing through the ledger (alloc copied) */
    *ep = moq_endpoint_test_make_bare();
    ROW(what, "fixture", rc == MOQ_OK && *s != NULL && *ep != NULL);
    if (rc != MOQ_OK || !*s || !*ep) {
        if (*s) moq_media_sender_test_free(*s);
        if (*ep) moq_endpoint_test_free_bare(*ep);
        return false;
    }
    moq_media_sender_test_set_ep(*s, *ep);
    return true;
}

static void finish_ledger_sender(const char *what, moq_media_sender_t *s, moq_endpoint_t *ep)
{
    moq_media_sender_test_set_ep(s, NULL);
    moq_media_sender_test_free(s);
    moq_endpoint_test_free_bare(ep);
    ROW(what, "final.zero_outstanding", outstanding() == 0);
    ROW(what, "final.every_free_exact_size", g_ledger.bad_free == 0 && g_ledger.overflow == 0);
    if (outstanding() != 0 || g_ledger.bad_free || g_ledger.overflow) dump(what);
}

/* -- t1 ------------------------------------------------------------------- */
static void test_t1_closed_endpoint_refuses_every_mutator(void)
{
    const char *what = "t1.closed_endpoint";
    moq_media_sender_t *s; moq_endpoint_t *ep;
    if (!make_ledger_sender(what, &s, &ep)) return;

    /* Live control: one add registers one track with exactly three requests
     * (the track struct, its copied strings, the registry vector) and frees
     * nothing. */
    moq_media_track_cfg_t tc; track_cfg(&tc);
    moq_media_track_t *t = NULL;
    snap_t a = snap();
    ROW(what, "live.add_track_ok", moq_media_sender_add_track(s, &tc, &t) == MOQ_OK && t != NULL);
    snap_t b = snap();
    ROW(what, "live.three_requests", b.requests - a.requests == 3 && b.supplied - a.supplied == 3);
    ROW(what, "live.strings_request", b.requests - a.requests == 3 && g_ledger.requested[a.requests + 1] == TRACK_STRINGS);
    ROW(what, "live.nothing_freed", b.freed == a.freed);
    ROW(what, "live.registered_one", moq_media_sender_test_track_count(s) == 1);
    ROW(what, "live.not_closed", !moq_media_sender_is_closed(s));
    if (!t) { finish_ledger_sender(what, s, ep); return; }

    ROW(what, "stop_ok", moq_endpoint_stop(ep) == MOQ_OK);
    ROW(what, "endpoint_closed", moq_endpoint_is_closed(ep));
    ROW(what, "endpoint_not_fatal", !moq_endpoint_is_fatal(ep));
    ROW(what, "sender_is_closed", moq_media_sender_is_closed(s));
    ROW(what, "sender_not_fatal", !moq_media_sender_is_fatal(s));

    moq_rcbuf_t *p = payload(8);
    ROW(what, "fixture.payload", p != NULL);
    if (p) {
        moq_media_send_object_t o; send_obj(&o, p, true, true);
        moq_result_t wr = moq_media_sender_write(s, t, &o);
        ROW(what, "write_is_closed", wr == MOQ_ERR_CLOSED);
        if (wr != MOQ_OK) moq_rcbuf_decref(p);   /* no transfer occurred */
    }
    ROW(what, "end_track_is_closed", moq_media_sender_end_track(s, t) == MOQ_ERR_CLOSED);
    ROW(what, "remove_track_is_closed", moq_media_sender_remove_track(s, t) == MOQ_ERR_CLOSED);
    ROW(what, "complete_is_closed", moq_media_sender_complete(s) == MOQ_ERR_CLOSED);
    moq_media_vod_track_t item = { t, 1000 };
    ROW(what, "convert_to_vod_is_closed", moq_media_sender_convert_to_vod(s, &item, 1) == MOQ_ERR_CLOSED);

    /* add_track on the closed endpoint: refused by the INITIAL check -- no
     * allocation request, no registration, the gap never reached. */
    static const uint8_t NAME2[] = "audio2";
    moq_media_track_cfg_t tc2; track_cfg(&tc2);
    tc2.name = (moq_bytes_t){ NAME2, 6 };
    moq_media_track_t *t2 = NULL;
    size_t before = moq_media_sender_test_track_count(s);
    int gap_calls = 0;
    snap_t c = snap();
    moq_media_sender_test_set_add_track_gap_hook(count_gap, &gap_calls);
    moq_result_t ar = moq_media_sender_add_track(s, &tc2, &t2);
    moq_media_sender_test_set_add_track_gap_hook(NULL, NULL);
    snap_t d = snap();
    size_t after = moq_media_sender_test_track_count(s);
    if (ar != MOQ_ERR_CLOSED || after != before || d.requests != c.requests)
        fprintf(stderr, "  %s: add_track on a closed endpoint measured %d, handle %s, registry %zu -> %zu, requests %d -> %d\n",
                what, (int)ar, t2 ? "REGISTERED" : "null", before, after, c.requests, d.requests);
    ROW(what, "add_track_is_closed", ar == MOQ_ERR_CLOSED);
    ROW(what, "add_track_returns_no_handle", t2 == NULL);
    ROW(what, "add_track_registers_nothing", after == before);
    ROW(what, "add_track_refused_before_gap", gap_calls == 0);
    ROW(what, "add_track_zero_requests", d.requests == c.requests && d.freed == c.freed);

    finish_ledger_sender(what, s, ep);
}

/* -- t2: terminal transition between the initial check and registration ---- */
typedef struct { moq_endpoint_t *ep; int calls; moq_result_t stop_rc; } gap_t;
static void stop_in_gap(void *ctx)
{
    gap_t *g = (gap_t *)ctx;
    g->calls++;
    g->stop_rc = moq_endpoint_stop(g->ep);   /* the endpoint closes while add_track is in flight */
}

static void test_t2_terminal_transition_in_add_track_gap(void)
{
    const char *what = "t2.closed_in_gap";
    moq_media_sender_t *s; moq_endpoint_t *ep;
    if (!make_ledger_sender(what, &s, &ep)) return;
    ROW(what, "live.not_closed", !moq_media_sender_is_closed(s));

    gap_t gap = { ep, 0, MOQ_OK };
    moq_media_sender_test_set_add_track_gap_hook(stop_in_gap, &gap);
    moq_media_track_cfg_t tc; track_cfg(&tc);
    moq_media_track_t *t = NULL;
    size_t before = moq_media_sender_test_track_count(s);
    snap_t a = snap();
    moq_result_t ar = moq_media_sender_add_track(s, &tc, &t);
    snap_t b = snap();
    size_t after = moq_media_sender_test_track_count(s);
    moq_media_sender_test_set_add_track_gap_hook(NULL, NULL);
    ROW(what, "gap_reached_once", gap.calls == 1);
    ROW(what, "stop_in_gap_ok", gap.stop_rc == MOQ_OK);
    ROW(what, "endpoint_closed_after", moq_endpoint_is_closed(ep) && moq_media_sender_is_closed(s));
    if (ar != MOQ_ERR_CLOSED || after != before || b.supplied - b.freed != a.supplied - a.freed)
        fprintf(stderr, "  %s: add_track with the endpoint closing in the gap measured %d, handle %s, registry %zu -> %zu, requests +%d supplied +%d freed +%d bad_free +%d\n",
                what, (int)ar, t ? "REGISTERED" : "null", before, after,
                b.requests - a.requests, b.supplied - a.supplied, b.freed - a.freed, b.bad_free - a.bad_free);
    ROW(what, "add_track_is_closed", ar == MOQ_ERR_CLOSED);
    ROW(what, "add_track_returns_no_handle", t == NULL);
    ROW(what, "add_track_registers_nothing", after == before);
    /* The prepared media track (struct + strings) was requested before the
     * in-lock refusal and must be freed with exact sizes: two requests, two
     * frees, no wrong size, outstanding unchanged relative to the live
     * sender baseline. */
    ROW(what, "gap_refusal_two_requests", b.requests - a.requests == 2 && b.supplied - a.supplied == 2);
    ROW(what, "gap_refusal_strings_request", b.requests - a.requests == 2 && g_ledger.requested[a.requests + 1] == TRACK_STRINGS);
    ROW(what, "gap_refusal_freed_exactly", b.freed - a.freed == b.supplied - a.supplied);
    ROW(what, "gap_refusal_exact_sizes", b.bad_free == a.bad_free);
    ROW(what, "gap_refusal_outstanding_unchanged", (b.supplied - b.freed) == (a.supplied - a.freed));

    finish_ledger_sender(what, s, ep);
}

/* -- d1 / d2 --------------------------------------------------------------- */
typedef struct {
    moq_media_sender_t *s;
    moq_media_track_t *t;
    moq_rcbuf_t *p;
    moq_result_t rc;
} writer_t;

static void *blocked_writer(void *arg)
{
    writer_t *w = (writer_t *)arg;
    moq_media_send_object_t o; send_obj(&o, w->p, false, false);   /* delta of the open group */
    w->rc = moq_media_sender_write(w->s, w->t, &o);
    return NULL;
}

static moq_media_sender_t *lossless_sender(uint64_t block_timeout_us, moq_media_track_t **t)
{
    static const moq_bytes_t parts[1] = { { NS0, 4 } };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_lossless_sized(&cfg, sizeof(cfg));
    cfg.namespace_.parts = parts; cfg.namespace_.count = 1;
    cfg.block_timeout_us = block_timeout_us;
    cfg.queue_max_objects = 1;
    cfg.pre_ready_max_objects = 1;
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(&cfg);
    if (!s) return NULL;
    moq_media_track_cfg_t tc; track_cfg(&tc);
    if (moq_media_sender_add_track(s, &tc, t) != MOQ_OK) { moq_media_sender_test_free(s); return NULL; }
    return s;
}

/* Wait-entry handshake: the production BLOCK_TIMEOUT path signals through the
 * test-only hook (s->mu held, before its deadline is constructed) that it has
 * entered the blocking path; the test waits for that signal with a finite
 * deadline. Any condvar error other than a timeout ends the wait as a failure
 * rather than spinning. */
typedef struct { pthread_mutex_t mu; pthread_cond_t cv; int entered; bool ready; } handshake_t;
static bool handshake_init(handshake_t *h)
{
    h->entered = 0; h->ready = false;
    if (pthread_mutex_init(&h->mu, NULL) != 0) return false;
    if (pthread_cond_init(&h->cv, NULL) != 0) { pthread_mutex_destroy(&h->mu); return false; }
    h->ready = true;
    return true;
}
static void handshake_destroy(handshake_t *h)
{
    if (!h->ready) return;
    pthread_cond_destroy(&h->cv);
    pthread_mutex_destroy(&h->mu);
    h->ready = false;
}
static void on_wait_entry(void *ctx)
{
    handshake_t *h = (handshake_t *)ctx;
    pthread_mutex_lock(&h->mu);
    h->entered++;
    pthread_cond_broadcast(&h->cv);
    pthread_mutex_unlock(&h->mu);
}
static bool await_entry(handshake_t *h, unsigned seconds)
{
    struct timespec until;
    if (clock_gettime(CLOCK_REALTIME, &until) != 0) return false;
    until.tv_sec += (time_t)seconds;
    pthread_mutex_lock(&h->mu);
    int err = 0;
    while (h->entered == 0 && err == 0)
        err = pthread_cond_timedwait(&h->cv, &h->mu, &until);
    bool ok = h->entered > 0;
    pthread_mutex_unlock(&h->mu);
    if (err != 0 && err != ETIMEDOUT) {
        fprintf(stderr, "  handshake: pthread_cond_timedwait error %d\n", err);
        return false;   /* an unexpected condvar error is a failure, whatever was observed */
    }
    return ok;
}

static void test_d1_extreme_block_timeout_still_observes_terminal(void)
{
    const char *what = "d1.block_timeout_uint64_max";
    moq_media_track_t *t = NULL;
    moq_media_sender_t *s = lossless_sender(UINT64_MAX, &t);
    ROW(what, "fixture", s != NULL);
    if (!s) return;
    moq_rcbuf_t *first = payload(8);
    moq_rcbuf_t *second = payload(8);
    ROW(what, "fixture.payloads", first != NULL && second != NULL);
    if (!first || !second) {
        if (first) moq_rcbuf_decref(first);
        if (second) moq_rcbuf_decref(second);
        moq_media_sender_test_free(s);
        return;
    }
    moq_media_send_object_t o; send_obj(&o, first, true, true);
    moq_result_t wr1 = moq_media_sender_write(s, t, &o);
    ROW(what, "first_write_ok", wr1 == MOQ_OK);
    if (wr1 != MOQ_OK) {                         /* refused: still ours */
        moq_rcbuf_decref(first);
        moq_rcbuf_decref(second);
        moq_media_sender_test_free(s);
        return;
    }

    handshake_t h;
    ROW(what, "fixture.handshake", handshake_init(&h));
    if (!h.ready) { moq_rcbuf_decref(second); moq_media_sender_test_free(s); return; }
    moq_media_sender_test_set_wait_entry_hook(on_wait_entry, &h);
    writer_t w = { s, t, second, MOQ_OK };
    pthread_t th;
    int created = pthread_create(&th, NULL, blocked_writer, &w);
    ROW(what, "fixture.thread", created == 0);
    if (created != 0) {
        moq_media_sender_test_set_wait_entry_hook(NULL, NULL);
        handshake_destroy(&h);
        moq_rcbuf_decref(second);
        moq_media_sender_test_free(s);
        return;
    }
    /* Prove the writer entered the blocking path BEFORE terminalizing.
     * Terminalization is issued either way so a blocked writer is released;
     * a writer that never returns is bounded only by the registered CTest
     * TIMEOUT (a hang is a failure, never cancelled or freed from here). */
    bool entered = await_entry(&h, 5);
    ROW(what, "writer_entered_blocking_path", entered);
    moq_media_sender_test_set_fatal(s, true);    /* the terminal transition */
    int joined = g_join(th, NULL);
    ROW(what, "writer_joined", joined == 0);
    if (joined != 0) {
        /* The worker may still run: no cleanup, no continuation. */
        fprintf(stderr, "%s\n", JOIN_FAILED_EXIT);
        fflush(stderr);
        _Exit(EXIT_FAILURE);
    }
    moq_media_sender_test_set_wait_entry_hook(NULL, NULL);
    handshake_destroy(&h);
    if (w.rc != MOQ_ERR_CLOSED)
        fprintf(stderr, "  %s: blocked write measured %d\n", what, (int)w.rc);
    ROW(what, "blocked_write_is_closed", w.rc == MOQ_ERR_CLOSED);
    moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
    ROW(what, "stats_ok", moq_media_sender_get_stats(s, &st, sizeof(st)) == MOQ_OK);
    ROW(what, "one_stall", st.backpressure_stalls == 1);
    ROW(what, "one_written", st.objects_written == 1 && st.objects_queued == 1);
    ROW(what, "last_error_closed", st.last_error == MOQ_ERR_CLOSED);
    if (w.rc != MOQ_OK) moq_rcbuf_decref(second); /* refused: still ours */
    moq_media_sender_test_free(s);               /* releases the transferred first payload */
}

static void test_d2_minimal_block_timeout_returns_would_block(void)
{
    const char *what = "d2.block_timeout_1us";
    moq_media_track_t *t = NULL;
    moq_media_sender_t *s = lossless_sender(1, &t);
    ROW(what, "fixture", s != NULL);
    if (!s) return;
    moq_rcbuf_t *first = payload(8);
    moq_rcbuf_t *second = payload(8);
    ROW(what, "fixture.payloads", first != NULL && second != NULL);
    if (!first || !second) {
        if (first) moq_rcbuf_decref(first);
        if (second) moq_rcbuf_decref(second);
        moq_media_sender_test_free(s);
        return;
    }
    moq_media_send_object_t o; send_obj(&o, first, true, true);
    moq_result_t wr1 = moq_media_sender_write(s, t, &o);
    ROW(what, "first_write_ok", wr1 == MOQ_OK);
    if (wr1 != MOQ_OK) {
        moq_rcbuf_decref(first);
        moq_rcbuf_decref(second);
        moq_media_sender_test_free(s);
        return;
    }
    send_obj(&o, second, false, false);
    moq_result_t wr = moq_media_sender_write(s, t, &o);
    ROW(what, "second_write_would_block", wr == MOQ_ERR_WOULD_BLOCK);
    moq_media_sender_stats_t st; memset(&st, 0, sizeof(st));
    ROW(what, "stats_ok", moq_media_sender_get_stats(s, &st, sizeof(st)) == MOQ_OK);
    ROW(what, "one_stall", st.backpressure_stalls == 1);
    ROW(what, "one_written", st.objects_written == 1 && st.objects_queued == 1 && st.objects_dropped == 0);
    ROW(what, "last_error_would_block", st.last_error == MOQ_ERR_WOULD_BLOCK);
    if (wr != MOQ_OK) moq_rcbuf_decref(second);
    moq_media_sender_test_free(s);
}

/* -- controls: the join-failure policy, proven in a child process ----------- */
/* Spawns this binary with `--control join-failure`, which runs ONLY d1 with a
 * refusing join while the real worker is created and unjoined, and requires:
 * exit status exactly EXIT_FAILURE (no crash/signal), the named failed-join
 * row and the process-exit marker on stderr, and NO later output (no d2 row,
 * no summary line) -- i.e. no post-failure cleanup or continuation. The
 * registered outer TIMEOUT bounds the child. */
static int run_controls(const char *self)
{
    const char *what = "control.join_failure";
    int fds[2];
    if (pipe(fds) != 0) { fprintf(stderr, "FAIL[%s.pipe]\n", what); return 1; }
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_adddup2(&fa, fds[1], 2);
    posix_spawn_file_actions_addclose(&fa, fds[0]);
    posix_spawn_file_actions_addclose(&fa, fds[1]);
    char *argv[] = { (char *)self, (char *)"--control", (char *)"join-failure", NULL };
    pid_t pid = 0;
    int rc = posix_spawn(&pid, self, &fa, NULL, argv, environ);
    posix_spawn_file_actions_destroy(&fa);
    close(fds[1]);
    if (rc != 0) { fprintf(stderr, "FAIL[%s.spawn]: %d\n", what, rc); close(fds[0]); return 1; }
    char buf[8192]; size_t n = 0;
    for (;;) {
        ssize_t got = read(fds[0], buf + n, sizeof(buf) - 1 - n);
        if (got <= 0) break;
        n += (size_t)got;
        if (n >= sizeof(buf) - 1) break;
    }
    close(fds[0]);
    buf[n] = '\0';
    int status = 0;
    if (waitpid(pid, &status, 0) != pid) { fprintf(stderr, "FAIL[%s.waitpid]\n", what); return 1; }
    int f = 0;
#define CROW(name, cond) do { if (!(cond)) { fprintf(stderr, "FAIL[%s.%s]\n", what, name); f++; } } while (0)
    CROW("exited_not_signalled", WIFEXITED(status));
    CROW("exit_status_is_failure", WIFEXITED(status) && WEXITSTATUS(status) == EXIT_FAILURE);
    CROW("named_failed_join_row", strstr(buf, "FAIL[d1.block_timeout_uint64_max.writer_joined]") != NULL);
    CROW("process_exit_marker", strstr(buf, JOIN_FAILED_EXIT) != NULL);
    CROW("no_continuation_to_d2", strstr(buf, "d2.block_timeout_1us") == NULL);
    CROW("no_summary_line", strstr(buf, "failure(s)") == NULL && strstr(buf, "PASS") == NULL);
    CROW("no_row_after_exit_marker", strstr(buf, JOIN_FAILED_EXIT) != NULL &&
         strstr(strstr(buf, JOIN_FAILED_EXIT) + sizeof(JOIN_FAILED_EXIT) - 1, "FAIL[") == NULL);
#undef CROW
    if (f) { fprintf(stderr, "child stderr:\n%s", buf); return 1; }
    fprintf(stderr, "control.join_failure: child exit %d, %zu stderr bytes, verdict named, no continuation\n",
            WEXITSTATUS(status), n);
    MOQ_TEST_PASS("media_sender_terminal_boundary_controls");
    return 0;
}

int main(int argc, char **argv)
{
    if (argc == 3 && strcmp(argv[1], "--control") == 0 && strcmp(argv[2], "join-failure") == 0) {
        g_join = refusing_join;
        test_d1_extreme_block_timeout_still_observes_terminal();
        /* Unreachable when the policy holds: d1 ends the process. */
        fprintf(stderr, "media_sender_terminal_boundary: join-failure control CONTINUED past the failed join\n");
        return 2;
    }
    if (argc == 2 && strcmp(argv[1], "--controls") == 0) return run_controls(argv[0]);
    test_t1_closed_endpoint_refuses_every_mutator();
    test_t2_terminal_transition_in_add_track_gap();
    test_d1_extreme_block_timeout_still_observes_terminal();
    test_d2_minimal_block_timeout_returns_would_block();
    if (failures) {
        fprintf(stderr, "media_sender_terminal_boundary: %d failure(s)\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("media_sender_terminal_boundary");
    return 0;
}
