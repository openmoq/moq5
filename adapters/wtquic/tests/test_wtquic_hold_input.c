/* HOLD_INPUT contract: real bridge, callback-borrowed provider
 * buffers. Like test_wtquic_keys, include
 * the adapter TU and supply a provider that never uses a network. */
#include <moq/transport_bridge.h>
#include <moq/session.h>
#include <wtquic/wtquic.h>
#include "../../../core/src/session/session_internal.h"
#include <stdio.h>
#include <string.h>

static unsigned feeds, refused, exact_replays, accepted_replays;
static uint64_t watched_key;
static uint8_t expected[5] = {0x04, 0, 0, 0, 0};
static size_t expected_len = sizeof(expected);
static bool expected_fin;
static moq_result_t observe_uni(moq_transport_bridge_t *b, uint64_t id,
                                const uint8_t *data, size_t len, bool fin,
                                uint64_t now)
{
    bool replay = id == watched_key && feeds && len == expected_len &&
        fin == expected_fin && (len == 0 || memcmp(data, expected, len) == 0);
    if (id == watched_key) {
        if (replay) exact_replays++;
        feeds++;
    }
    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
        b, id, data, len, fin, now);
    if (id == watched_key && rc == MOQ_ERR_INPUT_NOT_CONSUMED) refused++;
    if (replay && rc == MOQ_OK) accepted_replays++;
    return rc;
}

/* The provider reference expires after the terminal callback. Instrument
 * retention separately from the fake's static backing storage. */
static wtq_stream_t *expired;
static unsigned retained, stale_calls;
static unsigned query_calls, abort_calls;
static unsigned session_refs;
static wtq_result_t (*service_probe)(wtq_session_t *);
void wtq_session_add_ref(wtq_session_t *s) { (void)s; session_refs++; }
void wtq_session_release(wtq_session_t *s) { (void)s; session_refs--; }
wtq_result_t wtq_session_service_stream_admission(wtq_session_t *s)
{ return service_probe ? service_probe(s) : s ? WTQ_OK : WTQ_ERR_INVALID_ARG; }
static wtq_result_t contract_rc = WTQ_OK;
static size_t contract_quantum = 65535;
static wtq_receive_pause_mode_t contract_mode = WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED;
wtq_result_t wtq_session_receive_contract(const wtq_session_t *s,
    size_t *quantum, wtq_receive_pause_mode_t *mode)
{
    query_calls++;
    *quantum = s ? contract_quantum : 0;
    *mode = s ? contract_mode : WTQ_RECEIVE_PAUSE_UNSUPPORTED;
    return s ? contract_rc : WTQ_ERR_INVALID_ARG;
}
struct wtq_stream { bool bidi; unsigned pauses, resumes; };
static struct wtq_stream fake_stream;
static wtq_result_t g_pause_rc, g_resume_rc;
static bool close_on_resume, close_on_pause, replace_on_resume;
static bool replacement_paused;
static void resume_terminal(wtq_stream_t *st);
static void check_handle(const wtq_stream_t *st)
{
    if (st == expired && !retained) stale_calls++;
}
static struct wtq_stream *new_stream(bool bidi)
{
    memset(&fake_stream, 0, sizeof(fake_stream));
    fake_stream.bidi = bidi;
    return &fake_stream;
}
wtq_result_t wtq_session_open_uni(wtq_session_t *s, wtq_stream_t **out)
{ (void)s; *out = new_stream(false); return WTQ_OK; }
wtq_result_t wtq_session_open_bidi(wtq_session_t *s, wtq_stream_t **out)
{ (void)s; *out = new_stream(true); return WTQ_OK; }
uint64_t wtq_stream_id(const wtq_stream_t *st)
{ check_handle(st); return WTQ_STREAM_ID_UNKNOWN; }
bool wtq_stream_is_bidi(const wtq_stream_t *st)
{ check_handle(st); return st->bidi; }
wtq_result_t wtq_stream_abort(wtq_stream_t *st, uint32_t code)
{ check_handle(st); (void)code; abort_calls++; return WTQ_OK; }
wtq_result_t wtq_stream_reset(wtq_stream_t *st, uint32_t code)
{ check_handle(st); (void)code; return WTQ_OK; }
wtq_result_t wtq_stream_stop_sending(wtq_stream_t *st, uint32_t code)
{ check_handle(st); (void)code; return WTQ_OK; }
wtq_result_t wtq_stream_send(wtq_stream_t *st, const wtq_span_t *sp,
                            size_t n, uint32_t flags, void *ctx)
{ check_handle(st); (void)sp; (void)n; (void)flags; (void)ctx; return WTQ_OK; }
wtq_result_t wtq_stream_pause_receive(wtq_stream_t *st)
{
    check_handle(st);
    st->pauses++;
    if (close_on_pause) { close_on_pause = false; resume_terminal(st); }
    return g_pause_rc;
}
wtq_result_t wtq_stream_resume_receive(wtq_stream_t *st)
{
    check_handle(st);
    st->resumes++;
    if (close_on_resume) { close_on_resume = false; resume_terminal(st); }
    return g_resume_rc;
}
void wtq_session_events_init(wtq_session_events_t *e)
{ memset(e, 0, sizeof(*e)); }
wtq_result_t wtq_session_close(wtq_session_t *s, uint32_t code,
                              const uint8_t *r, size_t n)
{ (void)s; (void)code; (void)r; (void)n; return WTQ_OK; }
void wtq_stream_add_ref(wtq_stream_t *st)
{
    if (st == expired && !retained) stale_calls++;
    retained++;
}
void wtq_stream_release(wtq_stream_t *st)
{
    (void)st;
    if (retained) retained--;
    else stale_calls++;
}
wtq_receive_pause_mode_t wtq_stream_receive_pause_mode(const wtq_stream_t *st)
{
    if (st == expired && !retained) stale_calls++;
    return WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED;
}

#define moq_transport_bridge_on_peer_uni_bytes observe_uni
#include "../wtquic_adapter.c"
#undef moq_transport_bridge_on_peer_uni_bytes

static moq_wtquic_conn_t *resume_conn;
static uint64_t resume_replacement;
static void resume_terminal(wtq_stream_t *st)
{
    const wtq_session_events_t *events = moq_wtquic_conn_events();
    events->on_stream_closed(resume_conn->ws, st, resume_conn);
    expired = st; /* provider's automatic reference is gone now */
    if (replace_on_resume) {
        expired = NULL; /* provider reuses its handle storage */
        events->on_stream_opened(resume_conn->ws, st, false, resume_conn);
        struct moq_wtq_stream *ms = stream_find_by_handle(resume_conn, st);
        resume_replacement = ms->id;
        /* Model another pending receive in the new generation. Inspect
         * before a second service pass, which would reconcile its pause. */
        ms->paused = replacement_paused;
    }
}

#define CHECK(expr) do { if (!(expr)) { \
    printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr); failures++; \
} } while (0)

int main(int argc, char **argv)
{
    int failures = 0;
    if (argc != 2 && argc != 3) return 2;
    bool resume_close = strcmp(argv[1], "resume_close") == 0;
    bool resume_reuse = strcmp(argv[1], "resume_reuse") == 0;
    bool pause_close = strcmp(argv[1], "pause_close") == 0;
    bool pause_reuse = strcmp(argv[1], "pause_reuse") == 0;
    bool generation_case = resume_close || resume_reuse || pause_close || pause_reuse;
    wtq_result_t provider_rc = WTQ_OK;
    if (argc == 3) {
        if (!generation_case) return 2;
        if (strcmp(argv[2], "state") == 0) provider_rc = WTQ_ERR_STATE;
        else if (strcmp(argv[2], "closed") == 0) provider_rc = WTQ_ERR_CLOSED;
        else if (strcmp(argv[2], "error") == 0) provider_rc = WTQ_ERR_UNSUPPORTED;
        else return 2;
    }
    bool terminal = strcmp(argv[1], "terminal") == 0;
    bool reuse = strcmp(argv[1], "reuse") == 0;
    bool control = strcmp(argv[1], "direct") == 0;
    bool fin_only = strcmp(argv[1], "fin_only") == 0;
    bool paused = strcmp(argv[1], "paused") == 0;
    if (!terminal && !reuse && !control && !fin_only && !paused &&
        !generation_case &&
        strcmp(argv[1], "replay") != 0)
        return 2;

    moq_session_cfg_t scfg;
    moq_session_t *sess = NULL;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.version = MOQ_VERSION_DRAFT_16;
    CHECK(moq_session_create(&scfg, 0, &sess) == MOQ_OK);
    if (!sess) return 2;
    moq_wtquic_conn_cfg_t cfg;
    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.session = sess;
    cfg.wt_session = (wtq_session_t *)(void *)0x1;
    moq_wtquic_conn_t *c = NULL;
    CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_OK);
    if (!c) { moq_session_destroy(sess); return 2; }
    CHECK(c->ops.capabilities & MOQ_TRANSPORT_CAP_HOLD_INPUT);
    wtq_session_t *ws = (wtq_session_t *)(void *)0x1;
    struct wtq_stream *st = new_stream(false);
    ev_stream_opened(ws, st, false, c);
    watched_key = stream_find_by_handle(c, st)->id;
    if (generation_case) {
        CHECK(c->ops.capabilities == (MOQ_TRANSPORT_CAP_WRITE_PAYLOAD | MOQ_TRANSPORT_CAP_HOLD_INPUT));
        resume_conn = c;
        close_on_resume = resume_close || resume_reuse;
        close_on_pause = pause_close || pause_reuse;
        replace_on_resume = resume_reuse || pause_reuse;
        replacement_paused = close_on_resume;
        g_pause_rc = g_resume_rc = provider_rc;
        struct moq_wtq_stream *original = stream_find(c, watched_key);
        original->paused = close_on_resume;
        c->in_service = true;
        if (close_on_resume) resume_paused_streams(c);
        else pause_stream_for_backpressure(c, original, st);
        CHECK(c->term_count == 1);
        CHECK(c->term_keys[0] == watched_key);
        CHECK(stream_find(c, watched_key) == NULL);
        CHECK(st->pauses == (unsigned)(pause_close || pause_reuse));
        CHECK(st->resumes == (unsigned)(resume_close || resume_reuse));
        CHECK(moq_transport_bridge_is_fatal(c->bridge) ==
              (provider_rc == WTQ_ERR_UNSUPPORTED));
        if (replace_on_resume) {
            CHECK(resume_replacement != watched_key);
            CHECK(stream_find(c, resume_replacement) == original);
            CHECK(original->st == st);
            CHECK(original->paused == replacement_paused);
        } else {
            CHECK(!original->in_use);
            CHECK(original->st == NULL);
            CHECK(!original->paused);
        }
        CHECK(stale_calls == 0);
        c->in_service = false;
        conn_service(c);
        CHECK(c->term_count == 0);
        moq_wtquic_conn_destroy(c);
        moq_session_destroy(sess);
        CHECK(retained == 0);
        printf("case=%s provider_rc=%d stale_calls=%u failures=%d\n",
               argv[1], (int)provider_rc, stale_calls, failures);
        return failures ? 1 : 0;
    }
    /* White-box admission pressure without timing or malformed live slots.
     * Restore the allocated pool size before destruction. */
    size_t saved_rx_cap = sess->rx_cap;
    sess->state = MOQ_SESS_ESTABLISHED;
    sess->rx_cap = 0;
    CHECK(!moq_session_can_admit_data_stream(sess));
    expected_fin = !paused;
    if (fin_only) expected_len = 0;
    uint8_t borrowed[sizeof(expected)];
    memcpy(borrowed, expected, sizeof(borrowed));
    /* FIN may already have finished the receive direction at pause time. */
    g_pause_rc = paused ? WTQ_OK : WTQ_ERR_STATE;
    ev_stream_data(ws, st, borrowed, expected_len, expected_fin, c);
    CHECK(feeds == 1);
    CHECK(refused == 1);
    CHECK(moq_transport_bridge_stream_has_pending(c->bridge, watched_key));
    memset(borrowed, 0xa5, sizeof(borrowed));
    conn_service(c);
    CHECK(exact_replays == 0); /* still no capacity */
    if (paused) CHECK(st->pauses == 1 && st->resumes == 0);
    if (control) {
        /* Capacity is advisory: repeated refusal consumes nothing. */
        CHECK(observe_uni(c->bridge, watched_key, expected, expected_len,
                          expected_fin, now_us()) == MOQ_ERR_INPUT_NOT_CONSUMED);
        CHECK(refused == 2);
        exact_replays = 0;
    }

    uint64_t replacement_key = 0;
    if (terminal || reuse) {
        /* Synchronous terminal while service/endpoint work is on-stack. */
        c->in_service = true;
        ev_stream_closed(ws, st, c);
        expired = st;
        CHECK(c->term_count == 1);
        if (reuse) {
            /* Same provider address and same adapter slot, new identity. */
            expired = NULL;
            ev_stream_opened(ws, st, false, c);
            replacement_key = stream_find_by_handle(c, st)->id;
            CHECK(replacement_key != watched_key);
        }
        c->in_service = false;
        conn_service(c); /* terminal must not cancel a refused FIN chunk */
        CHECK(moq_transport_bridge_find_ref(c->bridge, watched_key)._v != 0);
    }

    sess->rx_cap = saved_rx_cap;
    CHECK(moq_session_can_admit_data_stream(sess));
    if (control) {
        /* Positive oracle: an owner explicitly replaying the saved chunk
         * reaches the real bridge with exactly the original FIN and bytes. */
        CHECK(observe_uni(c->bridge, watched_key, expected, expected_len,
                          expected_fin, now_us()) == MOQ_OK);
    } else {
        conn_service(c);
    }
    CHECK(exact_replays == 1);
    CHECK(accepted_replays == 1);
    unsigned replay_snapshot = exact_replays;
    conn_service(c);
    CHECK(exact_replays == replay_snapshot);
    if (paused) CHECK(st->resumes == 1);
    if (reuse) CHECK(stream_find(c, replacement_key) != NULL);
    CHECK(stale_calls == 0);
    CHECK(!moq_transport_bridge_is_fatal(c->bridge));
    printf("case=%s feeds=%u refused=%u exact_replays=%u accepted_replays=%u terminal_queue=%zu "
           "retained=%u stale_calls=%u failures=%d\n", argv[1], feeds,
           refused, exact_replays, accepted_replays, c->term_count, retained, stale_calls, failures);
    moq_wtquic_conn_destroy(c);
    moq_session_destroy(sess);
    CHECK(retained == 0);
    return failures ? 1 : 0;
}
