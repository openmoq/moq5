#include <moq/subscriber.h>
#include <moq/sim.h>
#include "test_support.h"
#include "test_alloc_support.h"
#include <string.h>

static int failures;

static bool expect(bool ok, const char *expression, int line)
{
    if (!ok) fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, line, expression);
    return ok;
}

#define EXPECT(expression) expect((expression), #expression, __LINE__)

/* A token's instance tag and sequence, as the facade packs them. */
static uint64_t tag_of(moq_sub_fill_t fill) { return fill._opaque >> 48; }
static uint64_t seq_of(moq_sub_fill_t fill) { return fill._opaque & ((UINT64_C(1) << 48) - 1); }

typedef struct {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_subscriber_t *sub;
} fixture_t;

static void drain_events(moq_session_t *session)
{
    moq_event_t event;
    while (moq_session_poll_events(session, &event, 1) == 1)
        moq_event_cleanup(&event);
}

static bool close_fixture(fixture_t *f)
{
    if (f->sub) moq_sub_destroy(f->sub);
    if (f->pair) {
        drain_events(moq_simpair_client(f->pair));
        drain_events(moq_simpair_server(f->pair));
        moq_simpair_destroy(f->pair);
    }
    f->sub = NULL;
    f->pair = NULL;
    return f->memory.balance == 0;
}

/* The pair's creation clock; a session's tag hashes it. */
static uint64_t g_initial_now_us = 1000;

static bool open_fixture(fixture_t *f, moq_version_t version,
                         const moq_alloc_t *sub_alloc)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->memory);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = version;
    cfg.seed = 7;
    cfg.initial_now_us = g_initial_now_us;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    if (moq_simpair_create(&cfg, &f->pair) != MOQ_OK ||
        moq_simpair_start(f->pair) != MOQ_OK) return false;
    moq_simpair_run_until_quiescent(f->pair, 8, NULL);
    drain_events(moq_simpair_client(f->pair));
    drain_events(moq_simpair_server(f->pair));
    moq_sub_cfg_t sub_cfg;
    moq_sub_cfg_init_sized(&sub_cfg, sizeof(sub_cfg));
    sub_cfg.max_tracks = 2;
    sub_cfg.max_fills = 1;
    sub_cfg.max_fill_items = 2;
    sub_cfg.max_fetch_items = 2;
    sub_cfg.max_objects = 2;
    return moq_sub_create(moq_simpair_client(f->pair),
        sub_alloc ? sub_alloc : &f->alloc, &sub_cfg, &f->sub) == MOQ_OK;
}

static uint64_t now(const fixture_t *f)
{
    return moq_simpair_now_us(f->pair);
}

static moq_result_t subscribe(fixture_t *f, const moq_fill_cfg_t *fill,
                              moq_sub_track_t **track)
{
    moq_bytes_t ns = MOQ_BYTES_LITERAL("identity");
    moq_sub_track_cfg_t cfg;
    moq_sub_track_cfg_init(&cfg);
    cfg.track_namespace.parts = &ns;
    cfg.track_namespace.count = 1;
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    cfg.fill = fill;
    return moq_sub_subscribe(f->sub, &cfg, now(f), track);
}

static void init_fill(moq_fill_cfg_t *fill, moq_location_filter_cfg_t *location)
{
    moq_location_filter_cfg_init(location);
    location->kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_init(fill);
    fill->location = location;
}

/* The peer is local and deterministic. Leave the child's data stream open. */
static bool accept_request(fixture_t *f, bool want_parent, bool want_fill)
{
    moq_session_t *server = moq_simpair_server(f->pair);
    bool parent = false, child = false, ok = true;
    for (unsigned i = 0; i < 8; ++i) {
        moq_simpair_run_until_quiescent(f->pair, 8, NULL);
        moq_event_t event;
        while (moq_session_poll_events(server, &event, 1) == 1) {
            if (event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                moq_accept_subscribe_cfg_t cfg;
                moq_accept_subscribe_cfg_init(&cfg);
                cfg.has_largest = true;
                cfg.largest_group = 2;
                cfg.largest_object = 0;
                ok = moq_session_accept_subscribe(server,
                    event.u.subscribe_request.sub, &cfg, now(f)) == MOQ_OK && ok;
                parent = true;
            } else if (event.kind == MOQ_EVENT_FILL_REQUEST) {
                ok = moq_session_open_fill(server, event.u.fill_request.fill,
                                          now(f)) == MOQ_OK && ok;
                child = true;
            }
            moq_event_cleanup(&event);
        }
        moq_simpair_run_until_quiescent(f->pair, 8, NULL);
        ok = moq_sub_tick(f->sub, now(f)) == MOQ_OK && ok;
    }
    return ok && parent == want_parent && child == want_fill;
}

static bool cancel_and_retire(fixture_t *f, moq_sub_fill_t child)
{
    if (moq_sub_cancel_fill(f->sub, child, now(f)) != MOQ_OK) return false;
    bool ok = moq_sub_cancel_fill(f->sub, child, now(f)) == MOQ_ERR_WRONG_STATE;
    for (unsigned i = 0; i < 8; ++i) {
        moq_simpair_run_until_quiescent(f->pair, 8, NULL);
        ok = moq_sub_tick(f->sub, now(f)) == MOQ_OK && ok;
    }
    unsigned terminals = 0, created = 0;
    moq_sub_fill_item_t item;
    while (moq_sub_poll_fill(f->sub, &item, sizeof(item)) == MOQ_OK) {
        ok = moq_sub_fill_eq(item.request, child) && ok;
        if (item.kind == MOQ_SUB_FILL_CREATED) {
            ok = terminals == 0 && ok;
            ++created;
        } else if (item.kind == MOQ_SUB_FILL_TERMINAL) {
            ok = created == 1 && ok;
            ++terminals;
            ok = item.u.terminal.reason == MOQ_FILL_LOCAL_CANCELLED && ok;
        } else {
            ok = false;
        }
        moq_sub_fill_item_cleanup(&item);
    }
    return ok && created == 1 && terminals == 1 &&
        moq_sub_cancel_fill(f->sub, child, now(f)) == MOQ_ERR_STALE_HANDLE;
}

static bool no_actions(fixture_t *f)
{
    moq_action_t action;
    bool empty = true;
    while (moq_session_poll_actions(moq_simpair_client(f->pair), &action, 1) == 1) {
        empty = false;
        moq_action_cleanup(&action);
    }
    return empty;
}

typedef struct {
    struct { void *ptr; size_t size; bool busy; } blocks[32];
    size_t count;
    bool bad_free;
} reuse_memory_t;

static void *reuse_alloc(size_t size, void *ctx)
{
    reuse_memory_t *m = ctx;
    for (size_t i = 0; i < m->count; ++i) {
        if (!m->blocks[i].busy && m->blocks[i].size == size) {
            m->blocks[i].busy = true;
            return m->blocks[i].ptr;
        }
    }
    if (m->count == sizeof(m->blocks) / sizeof(m->blocks[0])) return NULL;
    void *ptr = malloc(size);
    if (!ptr) return NULL;
    m->blocks[m->count].ptr = ptr;
    m->blocks[m->count].size = size;
    m->blocks[m->count++].busy = true;
    return ptr;
}

static void reuse_free(void *ptr, size_t size, void *ctx)
{
    reuse_memory_t *m = ctx;
    if (!ptr) return;
    for (size_t i = 0; i < m->count; ++i) {
        if (m->blocks[i].ptr != ptr) continue;
        if (!m->blocks[i].busy || m->blocks[i].size != size) m->bad_free = true;
        m->blocks[i].busy = false;
        return;
    }
    m->bad_free = true;
}

static void *reuse_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    void *next = reuse_alloc(new_size, ctx);
    if (!next) return NULL;
    if (ptr) memcpy(next, ptr, old_size < new_size ? old_size : new_size);
    reuse_free(ptr, old_size, ctx);
    return next;
}

/* Session and subscriber destroyed and created again at the exact same
 * addresses. An exact replay of every tag input (address, the session's own
 * tag, the facade serial) repeats the tag, as a core handle's would; a real
 * host's clock differs, so each owner's session is created at a later clock
 * (bounded retries, since the tag is a 15-bit hash). The new owner's sequence
 * restarts at 1 and refuses the earlier owners' tokens by tag. */
static bool owner_address_reuse(void)
{
    reuse_memory_t memory = {0};
    moq_alloc_t alloc = { &memory, reuse_alloc, reuse_realloc, reuse_free };
    moq_sub_fill_t children[3] = {{0}, {0}, {0}};
    uintptr_t owner_address = 0;
    uint64_t clock = 1000;
    bool ok = true;
    for (unsigned i = 0; i < 3 && ok; ++i) {
        fixture_t f;
        moq_sub_track_t *track = NULL;
        moq_location_filter_cfg_t location;
        moq_fill_cfg_t fill;
        init_fill(&fill, &location);
        for (unsigned attempt = 0; ok; ++attempt) {
            g_initial_now_us = clock;
            clock += 1000;
            ok = open_fixture(&f, MOQ_VERSION_DRAFT_22, &alloc);
            if (ok && i == 0) owner_address = (uintptr_t)f.sub;
            if (ok) ok = (uintptr_t)f.sub == owner_address;
            if (ok) ok = subscribe(&f, &fill, &track) == MOQ_OK && track != NULL;
            if (ok) {
                children[i] = moq_sub_track_fill(track);
                ok = moq_sub_fill_is_valid(children[i]) && accept_request(&f, true, true);
            }
            bool distinct = true;
            for (unsigned j = 0; j < i; ++j)
                if (tag_of(children[i]) == tag_of(children[j])) distinct = false;
            if (!ok || distinct || attempt >= 8) break;
            ok = cancel_and_retire(&f, children[i]) && close_fixture(&f);
        }
        if (ok) ok = EXPECT(seq_of(children[i]) == 1);
        for (unsigned j = 0; j < i && ok; ++j) {
            moq_result_t foreign = moq_sub_cancel_fill(f.sub, children[j], now(&f));
            printf("ORACLE exact_owner_address_reuse old=%llu new=%llu result=%d\n",
                   (unsigned long long)children[j]._opaque,
                   (unsigned long long)children[i]._opaque, (int)foreign);
            ok = EXPECT(tag_of(children[i]) != tag_of(children[j]) &&
                        foreign == MOQ_ERR_STALE_HANDLE);
        }
        /* Cover both an unretired child at destruction and a polled terminal. */
        if (ok && i != 0) ok = cancel_and_retire(&f, children[i]);
        ok = close_fixture(&f) && ok;
        for (size_t j = 0; j < memory.count; ++j)
            ok = !memory.blocks[j].busy && ok;
    }
    g_initial_now_us = 1000;
    for (size_t i = 0; i < memory.count; ++i) free(memory.blocks[i].ptr);
    return ok && !memory.bad_free;
}

static bool reservation_rollback(void)
{
    fixture_t f;
    bool ok = EXPECT(open_fixture(&f, MOQ_VERSION_DRAFT_22, NULL));
    moq_fill_cfg_t invalid = {0}, fill;
    moq_location_filter_cfg_t location;
    init_fill(&fill, &location);
    moq_sub_track_t *track = NULL;
    if (ok) ok = EXPECT(subscribe(&f, &invalid, &track) == MOQ_ERR_ABI_MISMATCH && !track &&
                        no_actions(&f));
    if (ok) ok = EXPECT(subscribe(&f, &fill, &track) == MOQ_OK && track &&
                        accept_request(&f, true, true));
    moq_sub_fill_t first = {0};
    if (ok) {
        first = moq_sub_track_fill(track);
        ok = EXPECT(moq_sub_fill_is_valid(first));
        moq_sub_track_t *refused = NULL;
        if (ok) ok = EXPECT(subscribe(&f, &fill, &refused) == MOQ_ERR_WOULD_BLOCK &&
                            refused == NULL && no_actions(&f));
        moq_sub_update_cfg_t blocked;
        moq_sub_update_cfg_init_sized(&blocked, sizeof(blocked));
        blocked.fill = &fill;
        if (ok) ok = EXPECT(moq_sub_update_subscription(f.sub, track, &blocked, now(&f)) ==
                            MOQ_ERR_WOULD_BLOCK && no_actions(&f) &&
                            moq_sub_fill_eq(moq_sub_track_fill(track), first));
        if (ok) ok = EXPECT(cancel_and_retire(&f, first));
    }
    moq_sub_update_cfg_t update;
    moq_sub_update_cfg_init_sized(&update, sizeof(update));
    update.fill = &invalid;
    if (ok) ok = EXPECT(moq_sub_update_subscription(f.sub, track, &update, now(&f)) ==
                        MOQ_ERR_ABI_MISMATCH && no_actions(&f) &&
                        !moq_sub_fill_is_valid(moq_sub_track_fill(track)));
    update.fill = &fill;
    if (ok) ok = EXPECT(moq_sub_update_subscription(f.sub, track, &update, now(&f)) ==
                        MOQ_OK && accept_request(&f, false, true));
    if (ok) {
        moq_sub_fill_t next = moq_sub_track_fill(track);
        ok = EXPECT(next._opaque > first._opaque &&
                    moq_sub_cancel_fill(f.sub, first, now(&f)) == MOQ_ERR_STALE_HANDLE &&
                    cancel_and_retire(&f, next) && moq_sub_track_is_active(track));
    }
    return close_fixture(&f) && ok;
}

static bool legacy_no_identity(moq_version_t version)
{
    fixture_t f;
    bool ok = open_fixture(&f, version, NULL);
    moq_fill_cfg_t fill;
    moq_location_filter_cfg_t location;
    init_fill(&fill, &location);
    moq_sub_track_t *track = NULL;
    if (ok) ok = subscribe(&f, &fill, &track) == MOQ_ERR_UNSUPPORTED && !track &&
                 no_actions(&f);
    if (ok) ok = subscribe(&f, NULL, &track) == MOQ_OK && track &&
                 accept_request(&f, true, false) && moq_sub_track_is_active(track);
    moq_sub_update_cfg_t update;
    moq_sub_update_cfg_init_sized(&update, sizeof(update));
    update.fill = &fill;
    if (ok) ok = moq_sub_update_subscription(f.sub, track, &update, now(&f)) ==
                 MOQ_ERR_UNSUPPORTED && no_actions(&f) &&
                 !moq_sub_fill_is_valid(moq_sub_track_fill(track));
    return close_fixture(&f) && ok;
}

/* One fill-bearing request on a fresh track, then two through updates of
 * that track; each is cancelled and retired before the next. Returns the
 * three tokens. */
static bool issue_three(fixture_t *f, moq_sub_fill_t out[3])
{
    moq_location_filter_cfg_t location;
    moq_fill_cfg_t fill;
    init_fill(&fill, &location);
    moq_sub_track_t *track = NULL;
    bool ok = subscribe(f, &fill, &track) == MOQ_OK && track &&
              accept_request(f, true, true);
    if (ok) {
        out[0] = moq_sub_track_fill(track);
        ok = moq_sub_fill_is_valid(out[0]) && cancel_and_retire(f, out[0]);
    }
    for (unsigned i = 1; i < 3 && ok; ++i) {
        moq_sub_update_cfg_t update;
        moq_sub_update_cfg_init_sized(&update, sizeof(update));
        update.fill = &fill;
        ok = moq_sub_update_subscription(f->sub, track, &update, now(f)) == MOQ_OK &&
             accept_request(f, false, true);
        if (ok) {
            out[i] = moq_sub_track_fill(track);
            ok = moq_sub_fill_is_valid(out[i]) && cancel_and_retire(f, out[i]);
        }
    }
    return ok;
}

/* Two live subscribers each issue the sequence 1, 2, 3 under their own tag;
 * neither sees the other's tokens. */
static bool independent_instances(void)
{
    fixture_t f[2];
    moq_sub_fill_t tokens[2][3] = {{{0}, {0}, {0}}, {{0}, {0}, {0}}};
    bool ok = open_fixture(&f[0], MOQ_VERSION_DRAFT_22, NULL);
    ok = open_fixture(&f[1], MOQ_VERSION_DRAFT_22, NULL) && ok;
    for (unsigned i = 0; i < 2 && ok; ++i) ok = EXPECT(issue_three(&f[i], tokens[i]));
    if (ok) {
        printf("ORACLE independent_instances a=%llu,%llu,%llu b=%llu,%llu,%llu\n",
               (unsigned long long)tokens[0][0]._opaque, (unsigned long long)tokens[0][1]._opaque,
               (unsigned long long)tokens[0][2]._opaque, (unsigned long long)tokens[1][0]._opaque,
               (unsigned long long)tokens[1][1]._opaque, (unsigned long long)tokens[1][2]._opaque);
        for (unsigned i = 0; i < 2 && ok; ++i)
            for (unsigned k = 0; k < 3 && ok; ++k)
                ok = EXPECT(seq_of(tokens[i][k]) == k + 1 &&
                            tag_of(tokens[i][k]) == tag_of(tokens[i][0]));
        ok = EXPECT(tag_of(tokens[0][0]) != tag_of(tokens[1][0])) && ok;
        /* A retired token is stale to its owner and foreign to the other. */
        for (unsigned i = 0; i < 2 && ok; ++i)
            ok = EXPECT(moq_sub_cancel_fill(f[i].sub, tokens[i][2], now(&f[i])) == MOQ_ERR_STALE_HANDLE &&
                        moq_sub_cancel_fill(f[1 - i].sub, tokens[i][2], now(&f[1 - i])) == MOQ_ERR_STALE_HANDLE);
    }
    ok = close_fixture(&f[1]) && ok;
    return close_fixture(&f[0]) && ok;
}

/* The same session, a subscriber destroyed and created again at the exact
 * same address: its live token is refused by the new subscriber, whose own
 * sequence restarts at 1. The tag hashes the session's facade serial, so a
 * recreation has a new input; a 15-bit collision is possible, so the fixture
 * recreates until the tags differ (bounded) rather than relying on one draw. */
static bool stale_after_recreate(void)
{
    reuse_memory_t memory = {0};
    moq_alloc_t alloc = { &memory, reuse_alloc, reuse_realloc, reuse_free };
    fixture_t f;
    bool ok = open_fixture(&f, MOQ_VERSION_DRAFT_22, &alloc);
    moq_subscriber_t *first = f.sub;
    moq_location_filter_cfg_t location;
    moq_fill_cfg_t fill;
    init_fill(&fill, &location);
    moq_sub_track_t *track = NULL;
    moq_sub_fill_t stale = {0};
    if (ok) ok = subscribe(&f, &fill, &track) == MOQ_OK && track &&
                 accept_request(&f, true, true);
    if (ok) {
        stale = moq_sub_track_fill(track);
        ok = moq_sub_fill_is_valid(stale) && seq_of(stale) == 1;
    }
    moq_sub_cfg_t sub_cfg;
    moq_sub_cfg_init_sized(&sub_cfg, sizeof(sub_cfg));
    sub_cfg.max_tracks = 2;
    sub_cfg.max_fills = 1;
    sub_cfg.max_fill_items = 2;
    sub_cfg.max_fetch_items = 2;
    sub_cfg.max_objects = 2;
    unsigned recreations = 0;
    moq_sub_fill_t fresh = {0};
    while (ok) {
        moq_sub_destroy(f.sub);
        f.sub = NULL;
        drain_events(moq_simpair_client(f.pair));
        drain_events(moq_simpair_server(f.pair));
        ok = moq_sub_create(moq_simpair_client(f.pair), &alloc, &sub_cfg, &f.sub) == MOQ_OK &&
             (uintptr_t)f.sub == (uintptr_t)first;
        ++recreations;
        if (!ok) break;
        track = NULL;
        ok = subscribe(&f, &fill, &track) == MOQ_OK && track && accept_request(&f, true, true);
        if (!ok) break;
        fresh = moq_sub_track_fill(track);
        ok = moq_sub_fill_is_valid(fresh) && seq_of(fresh) == 1;
        if (!ok || tag_of(fresh) != tag_of(stale) || recreations >= 8) break;
    }
    if (ok) {
        moq_result_t refused = moq_sub_cancel_fill(f.sub, stale, now(&f));
        printf("ORACLE stale_after_recreate stale=%llu fresh=%llu recreations=%u result=%d\n",
               (unsigned long long)stale._opaque, (unsigned long long)fresh._opaque,
               recreations, (int)refused);
        ok = EXPECT(tag_of(fresh) != tag_of(stale) && refused == MOQ_ERR_STALE_HANDLE &&
                    cancel_and_retire(&f, fresh));
    }
    ok = close_fixture(&f) && ok;
    for (size_t i = 0; i < memory.count; ++i) free(memory.blocks[i].ptr);
    return ok && !memory.bad_free;
}

static void check_case(const char *name, bool ok)
{
    if (ok) printf("PASS: %s\n", name);
    else { ++failures; fprintf(stderr, "FAIL: %s\n", name); }
}

int main(void)
{
    check_case("owner_destruction_and_exact_address_reuse", owner_address_reuse());
    check_case("invalid_reservations_and_single_slot_reuse", reservation_rollback());
    check_case("d16_fill_refused_without_effects", legacy_no_identity(MOQ_VERSION_DRAFT_16));
    check_case("d18_fill_refused_without_effects", legacy_no_identity(MOQ_VERSION_DRAFT_18));
    check_case("independent_instances_issue_1_2_3", independent_instances());
    check_case("stale_token_after_recreate_refused", stale_after_recreate());
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
