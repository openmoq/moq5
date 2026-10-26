#include <moq/publisher.h>

#ifdef __cplusplus
#define PIN static_assert
#else
#define PIN _Static_assert
#endif

/* The complete pre-V1 config, including max_fills and its trailing padding. */
typedef struct old_pub_cfg {
    uint32_t struct_size;
    moq_pub_accept_mode_t accept_mode;
    uint8_t default_publisher_priority;
    moq_pub_subscribe_cb on_subscribe;
    void *on_subscribe_ctx;
    moq_pub_callbacks_t callbacks;
    uint32_t max_fills;
} old_pub_cfg_t;

/* Frozen V1 allocation, independent of the current config's fill tail. */
typedef struct old_v1_pub_cfg {
    uint32_t struct_size;
    moq_pub_accept_mode_t accept_mode;
    uint8_t default_publisher_priority;
    moq_pub_subscribe_cb on_subscribe;
    void *on_subscribe_ctx;
    moq_pub_callbacks_t callbacks;
    uint32_t max_fills;
    moq_pub_subscribe_v1_cb on_subscribe_v1;
    void *on_subscribe_v1_ctx;
} old_v1_pub_cfg_t;

#if UINTPTR_MAX == UINT64_MAX
#define ABI_SIZE(lp64, i686, arm) (lp64)
#elif defined(__i386__)
#define ABI_SIZE(lp64, i686, arm) (i686)
#elif defined(__arm__)
#define ABI_SIZE(lp64, i686, arm) (arm)
#else
#error Unsupported publisher layout target
#endif
#define CFG_OFFSET(normal64, short64, normal32, short32) \
    (sizeof(void *) == 8 ? \
        (sizeof(moq_pub_accept_mode_t) == 1 ? (short64) : (normal64)) : \
        (sizeof(moq_pub_accept_mode_t) == 1 ? (short32) : (normal32)))
#define PIN_CFG_FIELD(field, normal64, short64, normal32, short32, width) \
    PIN(offsetof(old_v1_pub_cfg_t, field) == \
        CFG_OFFSET(normal64, short64, normal32, short32), "old V1 " #field); \
    PIN(offsetof(moq_pub_cfg_t, field) == offsetof(old_v1_pub_cfg_t, field), \
        "unchanged config " #field); \
    PIN(sizeof(((moq_pub_cfg_t *)0)->field) == (width), "config width " #field)
#define PIN_FIELD(type, field, lp64, i686, arm, width) \
    PIN(offsetof(type, field) == ABI_SIZE(lp64, i686, arm), #type " offset " #field); \
    PIN(offsetof(type, field) + sizeof(((type *)0)->field) == \
        ABI_SIZE(lp64, i686, arm) + (width), #type " field end " #field)

PIN(sizeof(moq_pub_accept_mode_t) == 1 || sizeof(moq_pub_accept_mode_t) == 4,
    "measured enum modes");
PIN(MOQ_PUB_CALLBACK_V1 == 3, "old producers must refuse the v0 selector");
PIN(sizeof(old_pub_cfg_t) == CFG_OFFSET(136, 128, 72, 68), "pre-V1 allocation");
PIN(sizeof(old_v1_pub_cfg_t) == CFG_OFFSET(152, 144, 80, 76), "old V1 allocation");
PIN_CFG_FIELD(struct_size, 0, 0, 0, 0, 4);
PIN_CFG_FIELD(accept_mode, 4, 4, 4, 4, sizeof(moq_pub_accept_mode_t));
PIN_CFG_FIELD(default_publisher_priority, 8, 5, 8, 5, 1);
PIN_CFG_FIELD(on_subscribe, 16, 8, 12, 8, sizeof(void *));
PIN_CFG_FIELD(on_subscribe_ctx, 24, 16, 16, 12, sizeof(void *));
PIN_CFG_FIELD(callbacks, 32, 24, 20, 16, ABI_SIZE(96, 48, 48));
PIN_CFG_FIELD(max_fills, 128, 120, 68, 64, 4);
PIN_CFG_FIELD(on_subscribe_v1, 136, 128, 72, 68, sizeof(void *));
PIN_CFG_FIELD(on_subscribe_v1_ctx, 144, 136, 76, 72, sizeof(void *));
PIN(offsetof(moq_pub_cfg_t, on_subscribe_v1) == sizeof(old_pub_cfg_t),
    "V1 starts after the complete old config");
PIN(offsetof(moq_pub_cfg_t, max_fills) == offsetof(old_pub_cfg_t, max_fills),
    "max_fills stays at its old offset");
PIN(offsetof(moq_pub_cfg_t, callbacks) == offsetof(old_pub_cfg_t, callbacks),
    "embedded callbacks stay at their old offset");
PIN(offsetof(moq_pub_subscribe_info_v1_t, struct_size) == 0, "size comes first");
PIN(MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE == 72 &&
    offsetof(moq_subscription_admission_options_t, rendezvous_timeout_ms) == 72,
    "borrowed admission-options record keeps its v0 prefix");
PIN(offsetof(moq_pub_cfg_t, on_subscribe_v1_ctx) ==
    offsetof(moq_pub_cfg_t, on_subscribe_v1) + sizeof(moq_pub_subscribe_v1_cb),
    "complete V1 callback/context block");
PIN(offsetof(moq_pub_cfg_t, max_fill_snapshot_bytes) >= sizeof(old_v1_pub_cfg_t),
    "fill tail does not reuse any old V1 allocation or padding");
PIN(sizeof(moq_pub_subscribe_info_v1_t) == (sizeof(void *) == 8 ? 136 :
#if defined(__arm__)
    104
#else
    96
#endif
    ), "V1 observation dimensions");
PIN(sizeof(moq_pub_subscribe_info_t) == (sizeof(void *) == 8 ? 120 :
#if defined(__arm__)
    96
#else
    88
#endif
    ), "legacy observation dimensions unchanged");
PIN(sizeof(moq_pub_callbacks_t) == (sizeof(void *) == 8 ? 96 : 48),
    "embedded callbacks unchanged");
PIN_FIELD(moq_pub_callbacks_t, struct_size, 0, 0, 0, 4);
PIN_FIELD(moq_pub_callbacks_t, ctx, 8, 4, 4, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_subscriber_joined, 16, 8, 8, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_subscriber_left, 24, 12, 12, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_draining, 32, 16, 16, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_closed, 40, 20, 20, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_subscriber_updated, 48, 24, 24, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_publish_ok, 56, 28, 28, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_publish_error, 64, 32, 32, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_publish_forward_changed, 72, 36, 36, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_publish_finished, 80, 40, 40, sizeof(void *));
PIN_FIELD(moq_pub_callbacks_t, on_namespace_terminal, 88, 44, 44, sizeof(void *));

#define PIN_OBSERVATION(field, v0_64, v0_x86, v0_arm, v1_64, v1_x86, v1_arm, width) \
    PIN_FIELD(moq_pub_subscribe_info_t, field, v0_64, v0_x86, v0_arm, width); \
    PIN_FIELD(moq_pub_subscribe_info_v1_t, field, v1_64, v1_x86, v1_arm, width)
PIN_FIELD(moq_pub_subscribe_info_v1_t, struct_size, 0, 0, 0, 4);
PIN_OBSERVATION(track, 0, 0, 0, 8, 4, 4, sizeof(void *));
PIN_OBSERVATION(track_namespace, 8, 4, 4, 16, 8, 8, ABI_SIZE(16, 8, 8));
PIN_OBSERVATION(track_name, 24, 12, 12, 32, 16, 16, ABI_SIZE(16, 8, 8));
PIN_OBSERVATION(filter, 40, 20, 20, 48, 24, 24, 4);
PIN_OBSERVATION(subscriber_priority, 44, 24, 24, 52, 28, 28, 1);
PIN_OBSERVATION(group_order, 48, 28, 28, 56, 32, 32, 4);
PIN_OBSERVATION(forward, 52, 32, 32, 60, 36, 36, 1);
PIN_OBSERVATION(start_group, 56, 36, 40, 64, 40, 40, 8);
PIN_OBSERVATION(start_object, 64, 44, 48, 72, 48, 48, 8);
PIN_OBSERVATION(end_group, 72, 52, 56, 80, 56, 56, 8);
PIN_OBSERVATION(delivery_timeout_us, 80, 60, 64, 88, 64, 64, 8);
PIN_OBSERVATION(tokens, 88, 68, 72, 96, 72, 72, sizeof(void *));
PIN_OBSERVATION(token_count, 96, 72, 76, 104, 76, 76, sizeof(size_t));
PIN_OBSERVATION(deferred, 104, 76, 80, 112, 80, 80, sizeof(void *));
PIN_OBSERVATION(deferred_id, 112, 80, 88, 120, 84, 88, 8);
PIN_FIELD(moq_pub_subscribe_info_v1_t, options, 128, 92, 96, sizeof(void *));

#undef PIN_OBSERVATION
#undef PIN_FIELD
#undef PIN_CFG_FIELD
#undef CFG_OFFSET
#undef ABI_SIZE

#ifndef MOQ_PUB_ADMISSION_LAYOUT_ONLY
#include <moq/sim.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned checks, failures, cases;
#define CHECK(expr) do { ++checks; if (!(expr)) { \
    fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #expr); ++failures; \
} } while (0)
#define NEED(expr) do { ++checks; if (!(expr)) { \
    fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #expr); ++failures; goto done; \
} } while (0)

typedef struct memory {
    size_t calls, live;
} memory_t;

static void *allocate(size_t size, void *ctx)
{
    memory_t *m = ctx;
    ++m->calls;
    void *p = malloc(size);
    if (p) ++m->live;
    return p;
}

static void *resize(void *p, size_t old_size, size_t size, void *ctx)
{
    (void)old_size;
    if (!p) return allocate(size, ctx);
    memory_t *m = ctx;
    ++m->calls;
    return realloc(p, size);
}

static void release(void *p, size_t size, void *ctx)
{
    (void)size;
    memory_t *m = ctx;
    if (p) { CHECK(m->live > 0); --m->live; }
    free(p);
}

static moq_alloc_t allocator(memory_t *m)
{
    moq_alloc_t a = {.ctx = m, .alloc = allocate, .realloc = resize, .free = release};
    return a;
}

typedef struct observation {
    unsigned v0_calls, v1_calls, joined;
    moq_pub_accept_decision_t decision;
    moq_request_error_t error;
    moq_pub_track_t *track;
    moq_pub_deferred_t *deferred;
    uint64_t deferred_id;
    moq_subscribe_request_event_t expected;
    bool has_options;
    moq_subscription_admission_options_t options;
    uint8_t namespace_byte, name_byte, token[3];
} observation_t;

static void capture_legacy(observation_t *o, const moq_pub_subscribe_info_t *info)
{
    const moq_subscribe_request_event_t *e = &o->expected;
    CHECK(info->track == o->track);
    CHECK(info->track_namespace.count == 1 && info->track_namespace.parts[0].len == 1);
    CHECK(info->track_name.len == 1);
    o->namespace_byte = info->track_namespace.parts[0].data[0];
    o->name_byte = info->track_name.data[0];
    CHECK(o->namespace_byte == 'n' && o->name_byte == 'v');
    CHECK(info->filter == e->filter);
    CHECK(info->subscriber_priority == e->subscriber_priority);
    CHECK(info->group_order == e->group_order && info->forward == e->forward);
    CHECK(info->start_group == e->start_group && info->start_object == e->start_object);
    CHECK(info->end_group == e->end_group && info->delivery_timeout_us == e->delivery_timeout_us);
    CHECK(info->token_count == e->token_count);
    if (info->token_count) {
        CHECK(info->token_count == 1 && info->tokens[0].token_type == 42);
        CHECK(info->tokens[0].token_value.len == sizeof(o->token));
        memcpy(o->token, info->tokens[0].token_value.data, sizeof(o->token));
        CHECK(memcmp(o->token, "tok", sizeof(o->token)) == 0);
    }
    o->deferred = info->deferred;
    o->deferred_id = info->deferred_id;
    CHECK(o->deferred != NULL && o->deferred_id != 0);
    CHECK(o->joined == 0);
}

static moq_pub_accept_decision_t legacy_callback(
    void *ctx, const moq_pub_subscribe_info_t *info, moq_request_error_t *error)
{
    if (!ctx) return MOQ_PUB_DECISION_REJECT;
    observation_t *o = ctx;
    ++o->v0_calls;
    capture_legacy(o, info);
    *error = o->error;
    return o->decision;
}

static moq_pub_accept_decision_t callback(
    void *ctx, const moq_pub_subscribe_info_v1_t *info, moq_request_error_t *error)
{
    if (!ctx) return MOQ_PUB_DECISION_REJECT;
    observation_t *o = ctx;
    ++o->v1_calls;
    CHECK(info->struct_size == sizeof(*info));
    moq_pub_subscribe_info_t legacy = {
        .track = info->track, .track_namespace = info->track_namespace,
        .track_name = info->track_name, .filter = info->filter,
        .subscriber_priority = info->subscriber_priority, .group_order = info->group_order,
        .forward = info->forward, .start_group = info->start_group,
        .start_object = info->start_object, .end_group = info->end_group,
        .delivery_timeout_us = info->delivery_timeout_us, .tokens = info->tokens,
        .token_count = info->token_count, .deferred = info->deferred,
        .deferred_id = info->deferred_id
    };
    capture_legacy(o, &legacy);
    o->has_options = info->options != NULL;
    if (info->options) {
        CHECK(info->options->struct_size >= MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE);
        memcpy(&o->options, info->options, MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE);
    }
    *error = o->error;
    return o->decision;
}

static void joined(void *ctx, moq_pub_track_t *track)
{
    observation_t *o = ctx;
    CHECK(track == o->track);
    ++o->joined;
}

static void drain_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1)) moq_event_cleanup(&e);
}

static void drain_actions(moq_session_t *s)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1)) moq_action_cleanup(&a);
}

static bool pair_create(const moq_alloc_t *a, moq_version_t version, moq_simpair_t **pair)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = a;
    cfg.version = version;
    cfg.max_actions = 4;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 16;
    if (moq_simpair_create(&cfg, pair) != MOQ_OK) return false;
    if (moq_simpair_start(*pair) != MOQ_OK ||
        moq_simpair_run_until_quiescent(*pair, 32, NULL) != MOQ_OK) return false;
    moq_session_t *c = moq_simpair_client(*pair), *s = moq_simpair_server(*pair);
    if (moq_session_state(c) != MOQ_SESS_ESTABLISHED ||
        moq_session_state(s) != MOQ_SESS_ESTABLISHED) return false;
    drain_events(c);
    drain_events(s);
    return true;
}

static bool publisher_create(moq_simpair_t *pair, const moq_alloc_t *a,
                             bool legacy, observation_t *o, moq_publisher_t **pub)
{
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = legacy ? MOQ_PUB_CALLBACK : MOQ_PUB_CALLBACK_V1;
    if (legacy) { cfg.on_subscribe = legacy_callback; cfg.on_subscribe_ctx = o; }
    else { cfg.on_subscribe_v1 = callback; cfg.on_subscribe_v1_ctx = o; }
    cfg.callbacks.ctx = o;
    cfg.callbacks.on_subscriber_joined = joined;
    if (moq_pub_create(moq_simpair_server(pair), a, &cfg, pub) != MOQ_OK) return false;
    moq_pub_track_cfg_t track;
    moq_pub_track_cfg_init_sized(&track, sizeof(track));
    moq_bytes_t ns = MOQ_BYTES_LITERAL("n");
    track.track_namespace = (moq_namespace_t){&ns, 1};
    track.track_name = MOQ_BYTES_LITERAL("v");
    return moq_pub_add_track(*pub, &track, moq_simpair_now_us(pair), &o->track) == MOQ_OK;
}

static void constructor_case(moq_session_t *session, const moq_alloc_t *a,
                             memory_t *memory, size_t extent, unsigned mode, unsigned mask)
{
    ++cases;
    moq_pub_cfg_t template;
    memset(&template, 0, sizeof(template));
    template.accept_mode = (moq_pub_accept_mode_t)mode;
    template.default_publisher_priority = 128;
    template.callbacks.struct_size = sizeof(template.callbacks);
    if (mask & 1) template.on_subscribe = legacy_callback;
    if (mask & 2) template.on_subscribe_v1 = callback;
    if (mask & 4) template.on_subscribe_v1_ctx = memory;
    size_t physical = extent < sizeof(uint32_t) ? sizeof(uint32_t) : extent;
    uint8_t *bytes = malloc(physical);
    if (!bytes) abort();
    memset(bytes, 0xa5, physical);
    memcpy(bytes, &template, extent < sizeof(template) ? extent : sizeof(template));
    uint32_t stamp = (uint32_t)extent;
    memcpy(bytes, &stamp, sizeof(stamp));
    const size_t minimum = offsetof(moq_pub_cfg_t, default_publisher_priority) + 1;
    const size_t tail = offsetof(moq_pub_cfg_t, on_subscribe_v1);
    const size_t end = offsetof(moq_pub_cfg_t, on_subscribe_v1_ctx) + sizeof(void *);
    const size_t fill_request = offsetof(moq_pub_cfg_t, on_fill_request);
    const size_t fill_request_end = offsetof(moq_pub_cfg_t, on_fill_request_ctx) + sizeof(void *);
    const size_t fill_terminal = offsetof(moq_pub_cfg_t, on_fill_terminal);
    const size_t fill_terminal_end = offsetof(moq_pub_cfg_t, on_fill_terminal_ctx) + sizeof(void *);
    const size_t new_group = offsetof(moq_pub_cfg_t, on_new_group_request);
    const size_t new_group_end = offsetof(moq_pub_cfg_t, on_new_group_request_ctx) +
                                 sizeof(void *);
    bool legacy_hook = (mask & 1) && extent >=
        offsetof(moq_pub_cfg_t, on_subscribe) + sizeof(template.on_subscribe);
    bool valid = mode <= 3 && extent >= minimum && !(extent > tail && extent < end);
    valid = valid && !(extent > fill_request && extent < fill_request_end) &&
                    !(extent > fill_terminal && extent < fill_terminal_end) &&
                    !(extent > new_group && extent < new_group_end);
    if (mode == 3) valid = valid && extent >= end && (mask & 2) && !legacy_hook;
    else {
        valid = valid && !(extent >= end && (mask & 6));
        if (mode == 2) valid = valid && legacy_hook;
    }
    const size_t before = memory->calls, live = memory->live;
    moq_publisher_t *pub = (moq_publisher_t *)(uintptr_t)1;
    moq_result_t rc = moq_pub_create(session, a, (const moq_pub_cfg_t *)bytes, &pub);
    CHECK(rc == (valid ? MOQ_OK : MOQ_ERR_INVAL));
    CHECK((pub != NULL) == valid);
    if (!valid) CHECK(memory->calls == before);
    if (pub && pub != (moq_publisher_t *)(uintptr_t)1) moq_pub_destroy(pub);
    CHECK(memory->live == live);
    free(bytes);
}

static void old_v1_allocation(moq_session_t *session, const moq_alloc_t *a,
                               memory_t *memory, unsigned mode, unsigned mask,
                               bool guarded)
{
    ++cases;
    /* Exact allocation exposes overreads to ASan; guards also detect writes. */
    const size_t guard = guarded ? sizeof(uint64_t) : 0;
    old_v1_pub_cfg_t cfg;
    uint8_t *bytes = malloc(guard + sizeof(cfg) + guard);
    if (!bytes) abort();
    memset(bytes, 0xa5, guard + sizeof(cfg) + guard);
    moq_pub_cfg_init_sized((moq_pub_cfg_t *)(bytes + guard), sizeof(cfg));
    memcpy(&cfg, bytes + guard, sizeof(cfg));
    CHECK(cfg.struct_size == sizeof(cfg));
    CHECK(cfg.accept_mode == MOQ_PUB_REJECT_ALL && cfg.default_publisher_priority == 128);
    CHECK(cfg.callbacks.struct_size == sizeof(cfg.callbacks));
    CHECK(cfg.max_fills == 0 && cfg.on_subscribe == NULL && cfg.on_subscribe_ctx == NULL);
    CHECK(cfg.on_subscribe_v1 == NULL && cfg.on_subscribe_v1_ctx == NULL);
    cfg.accept_mode = (moq_pub_accept_mode_t)mode;
    if (mask & 1) cfg.on_subscribe = legacy_callback;
    if (mask & 2) cfg.on_subscribe_v1 = callback;
    if (mask & 4) cfg.on_subscribe_v1_ctx = memory;
    memcpy(bytes + guard, &cfg, sizeof(cfg));
    bool valid = mode == 3 ? (mask & 2) && !(mask & 1) :
        mode <= 2 && !(mask & 6) && (mode != 2 || (mask & 1));
    const size_t before = memory->calls, live = memory->live;
    moq_publisher_t *pub = (moq_publisher_t *)(uintptr_t)1;
    moq_result_t rc = moq_pub_create(session, a, (const moq_pub_cfg_t *)(bytes + guard), &pub);
    CHECK(rc == (valid ? MOQ_OK : MOQ_ERR_INVAL));
    CHECK((pub != NULL) == valid);
    if (!valid) CHECK(memory->calls == before);
    if (pub && pub != (moq_publisher_t *)(uintptr_t)1) moq_pub_destroy(pub);
    CHECK(memory->live == live);
    CHECK(memcmp(bytes + guard, &cfg, sizeof(cfg)) == 0);
    for (size_t i = 0; i < guard; ++i) {
        CHECK(bytes[i] == 0xa5);
        CHECK(bytes[guard + sizeof(cfg) + i] == 0xa5);
    }
    free(bytes);
}

static void constructors(bool subset)
{
    memory_t memory = {0};
    moq_alloc_t a = allocator(&memory);
    moq_session_cfg_t cfg = MOQ_SESSION_CFG_INIT;
    cfg.alloc = &a;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    moq_session_t *session = NULL;
    NEED(moq_session_create(&cfg, 0, &session) == MOQ_OK);
    if (subset) {
        /* Two full-size configurations from the complete sweep below. */
        constructor_case(session, &a, &memory, sizeof(moq_pub_cfg_t), 3, 2);
        constructor_case(session, &a, &memory, sizeof(moq_pub_cfg_t), 1, 2);
    } else {
        for (size_t extent = 0; extent <= sizeof(moq_pub_cfg_t) + 8; ++extent)
            for (unsigned mode = 0; mode <= 4; ++mode)
                for (unsigned mask = 0; mask < 8; ++mask)
                    constructor_case(session, &a, &memory, extent, mode, mask);
        for (unsigned mode = 0; mode <= 4; ++mode)
            for (unsigned mask = 0; mask < 8; ++mask) {
                old_v1_allocation(session, &a, &memory, mode, mask, false);
                old_v1_allocation(session, &a, &memory, mode, mask, true);
            }
    }
done:
    if (session) moq_session_destroy(session);
    CHECK(memory.live == 0);
}

static void initializers(void)
{
    moq_pub_cfg_t expected;
    moq_pub_cfg_init_sized(&expected, sizeof(expected));
    CHECK(expected.struct_size == sizeof(expected));
    CHECK(expected.accept_mode == MOQ_PUB_REJECT_ALL);
    CHECK(expected.default_publisher_priority == 128);
    CHECK(expected.on_subscribe_v1 == NULL && expected.on_subscribe_v1_ctx == NULL);
    CHECK(expected.max_fill_snapshot_bytes == 0);
    CHECK(expected.on_fill_request == NULL && expected.on_fill_request_ctx == NULL);
    CHECK(expected.on_fill_terminal == NULL && expected.on_fill_terminal_ctx == NULL);
    for (size_t extent = 0; extent <= sizeof(expected) + 8; ++extent) {
        ++cases;
        uint8_t *bytes = malloc(extent + 8);
        if (!bytes) abort();
        memset(bytes, 0xa5, extent + 8);
        moq_pub_cfg_init_sized((moq_pub_cfg_t *)bytes, extent);
        size_t written = extent < sizeof(expected) ? extent : sizeof(expected);
        if (written < sizeof(uint32_t)) written = 0;
        if (written) {
            uint32_t stamp;
            memcpy(&stamp, bytes, sizeof(stamp));
            CHECK(stamp == written);
        }
        for (size_t i = written; i < extent + 8; ++i) CHECK(bytes[i] == 0xa5);
        free(bytes);
    }
    ++cases;
    size_t frozen = offsetof(moq_pub_cfg_t, default_publisher_priority) + 1;
    uint8_t *bytes = malloc(frozen);
    if (!bytes) abort();
    moq_pub_cfg_init((moq_pub_cfg_t *)bytes);
    uint32_t stamp;
    memcpy(&stamp, bytes, sizeof(stamp));
    CHECK(stamp == frozen);
    free(bytes);
}

static void integer(uint8_t *wire, size_t *n, uint64_t value)
{
    if (value == UINT64_MAX) { memset(wire + *n, 0xff, 9); *n += 9; }
    else wire[(*n)++] = (uint8_t)value;
}

/* Literal D22 SUBSCRIBE, request0, namespace n, track v. No native encoder. */
static size_t request(uint8_t wire[128], int kind, bool fill, unsigned timeouts,
                      moq_subscription_admission_options_t *expected)
{
    const uint8_t header[] = {3, 0, 0, 0, 1, 1, 'n', 1, 'v', 0};
    memcpy(wire, header, sizeof(header));
    size_t n = sizeof(header);
    memset(expected, 0, sizeof(*expected));
    expected->struct_size = sizeof(*expected);
    moq_location_filter_cfg_init(&expected->location);
    bool object = timeouts == 1 || timeouts == 3 || timeouts == 4;
    bool subgroup = timeouts == 2 || timeouts == 3 || timeouts == 4;
    uint64_t object_ms = timeouts == 4 ? UINT64_MAX : 0;
    uint64_t subgroup_ms = timeouts == 4 ? 0 : UINT64_MAX;
    wire[9] = (uint8_t)((kind >= 0) + fill + object + subgroup);
    unsigned previous = 0;
    if (object) {
        wire[n++] = 2;
        integer(wire, &n, object_ms);
        expected->flags |= MOQ_SUBSCRIPTION_ADMISSION_OBJECT_DELIVERY_TIMEOUT;
        expected->object_delivery_timeout_ms = object_ms;
        previous = 2;
    }
    if (subgroup) {
        wire[n++] = (uint8_t)(6 - previous);
        integer(wire, &n, subgroup_ms);
        expected->flags |= MOQ_SUBSCRIPTION_ADMISSION_SUBGROUP_DELIVERY_TIMEOUT;
        expected->subgroup_delivery_timeout_ms = subgroup_ms;
        previous = 6;
    }
    if (kind >= 0) {
        wire[n++] = (uint8_t)(0x21 - previous);
        wire[n++] = (uint8_t)kind;
        expected->flags |= MOQ_SUBSCRIPTION_ADMISSION_LOCATION;
        expected->location.kind = (uint32_t)kind;
        if (kind == 1) {
            integer(wire, &n, UINT64_MAX);
            expected->location.relative_group_offset = UINT64_MAX;
        } else if (kind >= 2 && kind <= 4) {
            integer(wire, &n, UINT64_MAX);
            integer(wire, &n, 7);
            expected->location.start_group = UINT64_MAX;
            expected->location.start_object = 7;
            if (kind >= 3) { integer(wire, &n, 0); expected->location.end_group = UINT64_MAX; }
            if (kind == 4) {
                integer(wire, &n, UINT64_MAX);
                expected->location.end_object = UINT64_MAX;
            }
        }
        previous = 0x21;
    }
    if (fill) {
        wire[n++] = (uint8_t)(0x23 - previous);
        wire[n++] = 1;   /* Length 1: Number of Parameters 0 */
        wire[n++] = 0;
        expected->flags |= MOQ_SUBSCRIPTION_ADMISSION_FILL;
    }
    wire[1] = (uint8_t)((n - 3) >> 8);
    wire[2] = (uint8_t)(n - 3);
    return n;
}

static void metadata(int kind, bool fill, unsigned timeouts, int retry)
{
    ++cases;
    memory_t memory = {0};
    moq_alloc_t a = allocator(&memory);
    moq_simpair_t *pair = NULL;
    moq_publisher_t *pub = NULL;
    observation_t o = {0};
    o.decision = retry == 2 ? MOQ_PUB_DECISION_DEFER :
                 retry == 1 ? MOQ_PUB_DECISION_ACCEPT : MOQ_PUB_DECISION_REJECT;
    moq_event_t event;
    bool owned = false;
    moq_subscription_admission_options_t expected;
    uint8_t wire[128];
    size_t n = request(wire, kind, fill, timeouts, &expected);
    NEED(pair_create(&a, MOQ_VERSION_DRAFT_22, &pair));
    NEED(publisher_create(pair, &a, false, &o, &pub));
    moq_session_t *server = moq_simpair_server(pair);
    uint64_t now = moq_simpair_now_us(pair);
    if (retry) {
        /* Each request-stream publication queues one OPEN_BIDI_STREAM action. */
        for (unsigned i = 0; i < 4; ++i) {
            uint8_t name = (uint8_t)('a' + i);
            moq_bytes_t part = {&name, 1};
            moq_publish_namespace_cfg_t cfg;
            moq_publish_namespace_cfg_init(&cfg);
            cfg.track_namespace = (moq_namespace_t){&part, 1};
            moq_announcement_t announcement;
            NEED(moq_session_publish_namespace(server, &cfg, now, &announcement) == MOQ_OK);
        }
    }
    NEED(moq_session_on_bidi_stream_bytes(server, moq_stream_ref_from_u64(0x7100),
                                         wire, n, false, now) == MOQ_OK);
    owned = moq_session_poll_events(server, &event, 1) == 1;
    NEED(owned && event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    o.expected = event.u.subscribe_request;
    moq_pub_event_result_t result;
    moq_result_t rc = moq_pub_handle_event(pub, &event, now, &result);
    NEED(result == MOQ_PUB_EVENT_CONSUMED);
    NEED(rc == (retry == 1 ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
    CHECK(o.v1_calls == 1 && o.v0_calls == 0 && o.joined == 0);
    CHECK(o.has_options && memcmp(&o.options, &expected, sizeof(expected)) == 0);
    moq_event_cleanup(&event);
    owned = false;
    memset(wire, 0xa5, sizeof(wire));
    if (retry == 2) {
        NEED(moq_pub_resolve_deferred(pub, o.deferred, o.deferred_id, true, 0, now)
             == MOQ_ERR_WOULD_BLOCK);
    }
    drain_actions(server);
    NEED(moq_pub_flush(pub, now + 1) == MOQ_OK);
    NEED(moq_pub_flush(pub, now + 2) == MOQ_OK);
    CHECK(o.v1_calls == 1 && o.joined == (retry ? 1u : 0u));
    CHECK(moq_pub_active_subscriptions(pub, o.track) == (retry ? 1u : 0u));
    if (retry == 2)
        CHECK(moq_pub_resolve_deferred(pub, o.deferred, o.deferred_id, true, 0, now + 2)
               == MOQ_ERR_STALE_HANDLE);
done:
    if (owned) moq_event_cleanup(&event);
    if (pub) moq_pub_destroy(pub);
    if (pair) moq_simpair_destroy(pair);
    CHECK(memory.live == 0);
    if (o.v1_calls) {
        CHECK(memcmp(&o.options, &expected, sizeof(expected)) == 0);
        CHECK(o.namespace_byte == 'n' && o.name_byte == 'v');
    }
}

static void decision(moq_version_t version, bool legacy, moq_pub_accept_decision_t choice,
                      bool deferred_accept, moq_request_error_t error)
{
    ++cases;
    memory_t memory = {0};
    moq_alloc_t a = allocator(&memory);
    moq_simpair_t *pair = NULL;
    moq_publisher_t *pub = NULL;
    observation_t o = {0};
    o.decision = choice;
    o.error = error;
    moq_event_t event;
    bool owned = false;
    NEED(pair_create(&a, version, &pair));
    NEED(publisher_create(pair, &a, legacy, &o, &pub));
    moq_session_t *client = moq_simpair_client(pair), *server = moq_simpair_server(pair);
    uint64_t now = moq_simpair_now_us(pair);
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    moq_bytes_t ns = MOQ_BYTES_LITERAL("n");
    cfg.track_namespace = (moq_namespace_t){&ns, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    cfg.start_group = 2;
    cfg.start_object = 3;
    cfg.end_group = 7;
    cfg.has_subscriber_priority = true;
    cfg.subscriber_priority = 17;
    cfg.group_order = MOQ_GROUP_ORDER_DESCENDING;
    cfg.has_forward = true;
    cfg.forward = false;
    moq_auth_token_t token = {.token_type = 42, .token_value = MOQ_BYTES_LITERAL("tok")};
    cfg.auth_tokens = &token;
    cfg.auth_token_count = 1;
    moq_subscription_t sub;
    NEED(moq_session_subscribe(client, &cfg, now, &sub) == MOQ_OK);
    NEED(moq_simpair_run_until_quiescent(pair, 32, NULL) == MOQ_OK);
    owned = moq_session_poll_events(server, &event, 1) == 1;
    NEED(owned && event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    o.expected = event.u.subscribe_request;
    moq_pub_event_result_t result;
    NEED(moq_pub_handle_event(pub, &event, now, &result) == MOQ_OK);
    CHECK(result == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&event);
    owned = false;
    CHECK(o.v0_calls == (legacy ? 1u : 0u) && o.v1_calls == (legacy ? 0u : 1u));
    if (!legacy) CHECK(o.has_options == (version == MOQ_VERSION_DRAFT_22));
    bool accepted = choice == MOQ_PUB_DECISION_ACCEPT ||
                    (choice == MOQ_PUB_DECISION_DEFER && deferred_accept);
    if (choice == MOQ_PUB_DECISION_DEFER) {
        CHECK(moq_pub_active_subscriptions(pub, o.track) == 0 && o.joined == 0);
        /* An unresolved decision remains staged on every profile. */
        NEED(moq_pub_flush(pub, now + 1) == MOQ_ERR_WOULD_BLOCK);
        NEED(moq_pub_resolve_deferred(pub, o.deferred, o.deferred_id,
                                     deferred_accept, MOQ_REQUEST_ERROR_UNAUTHORIZED, now + 2) == MOQ_OK);
        CHECK(moq_pub_resolve_deferred(pub, o.deferred, o.deferred_id,
                                      deferred_accept, 0, now + 2) == MOQ_ERR_STALE_HANDLE);
    }
    NEED(moq_pub_flush(pub, now + 3) == MOQ_OK);
    CHECK(o.v0_calls + o.v1_calls == 1);
    CHECK(o.joined == (accepted ? 1u : 0u));
    CHECK(moq_pub_active_subscriptions(pub, o.track) == (accepted ? 1u : 0u));
    NEED(moq_simpair_run_until_quiescent(pair, 32, NULL) == MOQ_OK);
    unsigned ok = 0, refused = 0;
    while (moq_session_poll_events(client, &event, 1)) {
        if (event.kind == MOQ_EVENT_SUBSCRIBE_OK) { ++ok; CHECK(moq_subscription_eq(event.u.subscribe_ok.sub, sub)); }
        if (event.kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
            ++refused;
            CHECK(moq_subscription_eq(event.u.subscribe_error.sub, sub));
            moq_request_error_t expected_error = choice == MOQ_PUB_DECISION_REJECT ?
                (error ? error : MOQ_REQUEST_ERROR_UNAUTHORIZED) : MOQ_REQUEST_ERROR_UNAUTHORIZED;
            CHECK(event.u.subscribe_error.error_code == expected_error);
        }
        moq_event_cleanup(&event);
    }
    CHECK(ok == (accepted ? 1u : 0u) && refused == (accepted ? 0u : 1u));
done:
    if (owned) moq_event_cleanup(&event);
    if (pub) moq_pub_destroy(pub);
    if (pair) moq_simpair_destroy(pair);
    CHECK(memory.live == 0);
    if (o.v0_calls + o.v1_calls) CHECK(memcmp(o.token, "tok", sizeof(o.token)) == 0);
}

static void event_prefix(size_t detail_size, bool null_options)
{
    ++cases;
    memory_t memory = {0};
    moq_alloc_t a = allocator(&memory);
    moq_simpair_t *pair = NULL;
    moq_publisher_t *pub = NULL;
    observation_t o = {0};
    o.decision = MOQ_PUB_DECISION_REJECT;
    moq_event_t event;
    bool owned = false;
    uint8_t wire[128];
    moq_subscription_admission_options_t expected;
    size_t n = request(wire, 0, false, 0, &expected);
    NEED(pair_create(&a, MOQ_VERSION_DRAFT_22, &pair));
    NEED(publisher_create(pair, &a, false, &o, &pub));
    moq_session_t *server = moq_simpair_server(pair);
    uint64_t now = moq_simpair_now_us(pair);
    NEED(moq_session_on_bidi_stream_bytes(server, moq_stream_ref_from_u64(0x7200),
                                         wire, n, false, now) == MOQ_OK);
    owned = moq_session_poll_events(server, &event, 1) == 1;
    NEED(owned && event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    o.expected = event.u.subscribe_request;
    bool covered = detail_size >= offsetof(moq_subscribe_request_event_t, options) +
                                  sizeof(event.u.subscribe_request.options);
    event.detail_size = (uint32_t)detail_size;
    if (!covered)
        event.u.subscribe_request.options = (const moq_subscription_admission_options_t *)(uintptr_t)1;
    else if (null_options)
        event.u.subscribe_request.options = NULL;
    moq_pub_event_result_t result;
    NEED(moq_pub_handle_event(pub, &event, now, &result) == MOQ_OK);
    CHECK(result == MOQ_PUB_EVENT_CONSUMED && o.v1_calls == 1);
    CHECK(o.has_options == (covered && !null_options));
    if (o.has_options) CHECK(memcmp(&o.options, &expected, sizeof(expected)) == 0);
done:
    if (owned) moq_event_cleanup(&event);
    if (pub) moq_pub_destroy(pub);
    if (pair) moq_simpair_destroy(pair);
    CHECK(memory.live == 0);
}

int main(int argc, char **argv)
{
    /* --constructor-subset runs only two full-size constructor cases; the
     * default run includes them in the complete sweep. */
    bool constructor_subset = argc == 2 && strcmp(argv[1], "--constructor-subset") == 0;
    constructors(constructor_subset);
    if (!constructor_subset) {
        initializers();
        for (size_t n = offsetof(moq_subscribe_request_event_t, options);
             n <= sizeof(moq_subscribe_request_event_t) + 8; ++n)
            event_prefix(n, false);
        event_prefix(sizeof(moq_subscribe_request_event_t), true);
        for (int kind = -1; kind <= 5; ++kind)
            for (unsigned fill = 0; fill <= 1; ++fill)
                for (unsigned timeout = 0; timeout < 5; ++timeout)
                    metadata(kind, fill != 0, timeout, 0);
        metadata(1, false, 3, 1);
        metadata(1, false, 4, 2);
        const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
        for (unsigned i = 0; i < sizeof(versions) / sizeof(versions[0]); ++i)
            for (unsigned legacy = 0; legacy <= 1; ++legacy) {
                decision(versions[i], legacy != 0, MOQ_PUB_DECISION_ACCEPT, false, 0);
                decision(versions[i], legacy != 0, MOQ_PUB_DECISION_REJECT, false, 0);
                decision(versions[i], legacy != 0, MOQ_PUB_DECISION_REJECT, false, MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
                decision(versions[i], legacy != 0, MOQ_PUB_DECISION_DEFER, false, 0);
                decision(versions[i], legacy != 0, MOQ_PUB_DECISION_DEFER, true, 0);
            }
    }
    printf("publisher admission: %u cases, %u checks, %u failures\n", cases, checks, failures);
    return failures ? 1 : 0;
}
#endif
