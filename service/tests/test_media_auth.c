#include "media_auth_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #expr); \
        exit(1); \
    } \
} while (0)

typedef struct allocation {
    void *ptr;
    size_t size;
} allocation_t;

typedef struct fixture {
    allocation_t allocations[8];
    size_t live;
    size_t alloc_calls;
    bool fail_alloc;
    size_t selections;
    size_t notifications;
    bool terminal_latched;
    moq_result_t selector_result;
    moq_media_auth_decision_t decision;
    moq_auth_token_t tokens[MOQ_MEDIA_AUTH_MAX_TOKENS];
    size_t count;
    const moq_media_auth_request_t *request;
    moq_media_auth_operation_t *op;
    moq_media_auth_outcome_info_t observed;
} fixture_t;

static void *test_alloc(size_t size, void *ctx)
{
    fixture_t *f = ctx;
    ++f->alloc_calls;
    CHECK(size != 0);
    if (f->fail_alloc) return NULL;
    CHECK(f->live < 8);
    void *p = malloc(size);
    CHECK(p != NULL);
    f->allocations[f->live++] = (allocation_t){p, size};
    return p;
}

static void test_free(void *ptr, size_t size, void *ctx)
{
    fixture_t *f = ctx;
    size_t i = 0;
    while (i < f->live && f->allocations[i].ptr != ptr) ++i;
    CHECK(i < f->live);
    CHECK(f->allocations[i].size == size);
    free(ptr);
    f->allocations[i] = f->allocations[--f->live];
}

static moq_alloc_t allocator(fixture_t *f)
{
    return (moq_alloc_t){.ctx = f, .alloc = test_alloc, .free = test_free};
}

static moq_result_t select_tokens(void *ctx,
    const moq_media_auth_request_t *request, moq_auth_token_t *tokens,
    size_t capacity, size_t *count, moq_media_auth_decision_t *decision)
{
    fixture_t *f = ctx;
    CHECK(request == f->request);
    CHECK(request->struct_size == sizeof(*request));
    CHECK(capacity == MOQ_MEDIA_AUTH_MAX_TOKENS);
    CHECK(*count == 0);
    CHECK(*decision == MOQ_MEDIA_AUTH_DENY);
    for (size_t i = 0; i < capacity; ++i) {
        CHECK(tokens[i].token_type == 0);
        CHECK(tokens[i].token_value.data == NULL);
        CHECK(tokens[i].token_value.len == 0);
    }
    ++f->selections;
    size_t n = f->count < capacity ? f->count : capacity;
    memcpy(tokens, f->tokens, n * sizeof(*tokens));
    *count = f->count;
    *decision = f->decision;
    return f->selector_result;
}

static void on_outcome(void *ctx, const moq_media_auth_outcome_info_t *info)
{
    fixture_t *f = ctx;
    CHECK(f->terminal_latched);
    CHECK(f->op->notified);
    CHECK(info->struct_size == sizeof(*info));
    CHECK(info->request == f->request);
    ++f->notifications;
    f->observed = *info;
}

static moq_media_auth_request_t request_for(moq_media_auth_action_t action)
{
    static const moq_bytes_t parts[] = {{(const uint8_t *)"namespace", 9}};
    return (moq_media_auth_request_t){.struct_size = sizeof(moq_media_auth_request_t),
        .action = action, .track_namespace = {parts, 1},
        .track_name = action == MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE
                          ? (moq_bytes_t){NULL, 0} : MOQ_BYTES_LITERAL("track")};
}

static moq_media_auth_cfg_t config_for(fixture_t *f, bool dynamic)
{
    moq_media_auth_cfg_t cfg;
    moq_media_auth_cfg_init(&cfg);
    cfg.ctx = f;
    cfg.on_outcome = on_outcome;
    if (dynamic) cfg.select = select_tokens;
    else {
        cfg.tokens = f->tokens;
        cfg.token_count = f->count;
    }
    return cfg;
}

static void test_sizing_and_sources(void)
{
    struct extended { moq_media_auth_cfg_t cfg; uint8_t tail[16]; } e;
    for (size_t n = 0; n <= sizeof(e); ++n) {
        memset(&e, 0xa5, sizeof(e));
        moq_media_auth_cfg_init_sized(&e.cfg, n);
        const uint8_t *bytes = (const uint8_t *)&e;
        size_t written = n < sizeof(uint32_t) ? 0 : n;
        if (written > sizeof(e.cfg)) written = sizeof(e.cfg);
        if (written) CHECK(e.cfg.struct_size == written);
        for (size_t i = sizeof(uint32_t); i < written; ++i) CHECK(bytes[i] == 0);
        for (size_t i = written; i < sizeof(e); ++i) CHECK(bytes[i] == 0xa5);
    }
    moq_media_auth_cfg_init_sized(NULL, sizeof(e));

    fixture_t f = {0};
    moq_alloc_t a = allocator(&f);
    moq_media_auth_source_t source;
    memset(&source, 0, sizeof(source));
    uint8_t original[sizeof(source)];
    memcpy(original, &source, sizeof(source));
    moq_media_auth_cfg_t good = config_for(&f, true);
    for (size_t n = sizeof(uint32_t); n < MOQ_MEDIA_AUTH_CFG_V0_SIZE; ++n) {
        moq_media_auth_cfg_t short_cfg = good;
        short_cfg.struct_size = (uint32_t)n;
        CHECK(moq_media_auth_source_copy(&source, &short_cfg, &a) == MOQ_ERR_INVAL);
        CHECK(memcmp(&source, original, sizeof(source)) == 0);
    }
    for (unsigned row = 0; row < 7; ++row) {
        moq_media_auth_cfg_t cfg = good;
        switch (row) {
        case 0: cfg.on_outcome = NULL; break;
        case 1: cfg.tokens = f.tokens; break;
        case 2: cfg.token_count = 1; break;
        case 3: cfg.select = NULL; break;
        case 4: cfg.select = NULL; cfg.tokens = f.tokens; break;
        case 5: cfg.select = NULL; cfg.token_count = 1; break;
        case 6: cfg.select = NULL; cfg.tokens = f.tokens; cfg.token_count = 17; break;
        }
        CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == MOQ_ERR_INVAL);
        CHECK(memcmp(&source, original, sizeof(source)) == 0);
    }
    CHECK(f.notifications == 0 && f.selections == 0 && f.alloc_calls == 0);
    CHECK(moq_media_auth_source_copy(&source, &good, &a) == MOQ_OK);
    CHECK(source.configured && source.select == select_tokens);
    moq_media_auth_source_clear(&source, &a);
    CHECK(moq_media_auth_source_copy(&source, NULL, &a) == MOQ_OK);
    moq_media_auth_operation_t op = {0};
    moq_media_auth_request_t req = request_for(MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE);
    CHECK(moq_media_auth_operation_prepare(&op, &source, &req,
          MOQ_VERSION_DRAFT_16, &a) == MOQ_OK);
    CHECK(op.list.count == 0 && op.outcome == MOQ_MEDIA_AUTH_OUTCOME_NONE);
    moq_media_auth_operation_notify(&op, &source, &req,
                                   MOQ_MEDIA_AUTH_SCOPE_TRACK, NULL);
    CHECK(f.notifications == 0 && f.selections == 0);
    moq_media_auth_operation_clear(&op, &a);
}

static void test_static_copy_and_empty(void)
{
    uint8_t bytes[] = {0x41, 0, 0xff};
    fixture_t f = {.count = 3};
    f.tokens[0] = (moq_auth_token_t){7, {bytes, sizeof(bytes)}};
    f.tokens[1] = (moq_auth_token_t){8, {NULL, 0}};
    f.tokens[2] = (moq_auth_token_t){9, {bytes, 0}};
    moq_alloc_t a = allocator(&f);
    moq_media_auth_cfg_t cfg = config_for(&f, false);
    moq_media_auth_source_t source = {0};
    CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == MOQ_OK);
    CHECK(f.live == 1 && source.list.storage_size == sizeof(bytes));
    memset(&cfg, 0, sizeof(cfg));
    memset(f.tokens, 0, sizeof(f.tokens));
    memset(bytes, 0, sizeof(bytes));
    CHECK(source.list.tokens[0].token_type == 7);
    CHECK(source.list.tokens[0].token_value.len == 3);
    CHECK(source.list.tokens[0].token_value.data[0] == 0x41);
    CHECK(source.list.tokens[0].token_value.data[2] == 0xff);
    CHECK(source.list.tokens[1].token_type == 8);
    CHECK(source.list.tokens[2].token_type == 9);
    CHECK(source.list.tokens[1].token_value.data == NULL);
    CHECK(source.list.tokens[2].token_value.len == 0);
    moq_media_auth_operation_t op = {0};
    moq_media_auth_request_t req = request_for(MOQ_MEDIA_AUTH_ACTION_PUBLISH_NAMESPACE);
    CHECK(moq_media_auth_operation_prepare(&op, &source, &req,
          MOQ_VERSION_DRAFT_18, &a) == MOQ_OK);
    CHECK(f.live == 2 && op.list.storage != source.list.storage);
    moq_media_auth_source_clear(&source, &a);
    CHECK(f.live == 1 && op.list.tokens[0].token_value.data[2] == 0xff);
    moq_media_auth_operation_clear(&op, &a);
    moq_media_auth_operation_clear(&op, &a);
    CHECK(f.live == 0);

    f.count = 16;
    for (size_t i = 0; i < f.count; ++i) f.tokens[i].token_type = i;
    cfg = config_for(&f, false);
    size_t calls = f.alloc_calls;
    f.fail_alloc = true;
    CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == MOQ_OK);
    CHECK(source.list.count == 16 && source.list.storage == NULL);
    CHECK(moq_media_auth_operation_prepare(&op, &source, &req,
          MOQ_VERSION_DRAFT_16, &a) == MOQ_OK);
    CHECK(op.list.count == 16 && f.alloc_calls == calls);
    moq_media_auth_operation_clear(&op, &a);
    moq_media_auth_source_clear(&source, &a);
}

static void test_duplicates_and_budgets(void)
{
    uint8_t big[MOQ_MEDIA_AUTH_MAX_TOKEN_BYTES] = {0};
    uint8_t other[] = {0, 1};
    fixture_t f = {.count = 2};
    moq_alloc_t a = allocator(&f);
    for (unsigned row = 0; row < 8; ++row) {
        moq_media_auth_source_t source = {0};
        f.count = 2;
        f.tokens[0] = (moq_auth_token_t){1, {big, 2}};
        f.tokens[1] = f.tokens[0];
        moq_result_t expected = MOQ_ERR_INVAL;
        switch (row) {
        case 0: break; /* same binary pair */
        case 1: f.tokens[0].token_value.len = 0;
                f.tokens[1].token_value = (moq_bytes_t){NULL, 0}; break;
        case 2: f.tokens[1].token_type = 2; expected = MOQ_OK; break;
        case 3: f.tokens[1].token_value.data = other; expected = MOQ_OK; break;
        case 4: f.tokens[1].token_value.data = NULL; break;
        case 5: f.tokens[0].token_value.len = SIZE_MAX; break;
        case 6: f.tokens[0].token_value.len = sizeof(big);
                f.tokens[1].token_value.len = sizeof(big);
                f.tokens[1].token_type = 2; expected = MOQ_OK; break;
        case 7: /* Aggregate rejection precedes any duplicate/content read. */
                f.tokens[0].token_value = (moq_bytes_t){(const uint8_t *)(uintptr_t)1,
                                                       sizeof(big)};
                f.tokens[1] = f.tokens[0];
                f.tokens[2] = (moq_auth_token_t){3, {big, 1}};
                f.count = 3; break;
        }
        moq_media_auth_cfg_t cfg = config_for(&f, false);
        CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == expected);
        if (expected != MOQ_OK) CHECK(!source.configured && !source.list.storage);
        moq_media_auth_source_clear(&source, &a);
        CHECK(f.live == 0 && f.notifications == 0);
    }
}

static void test_profile_widths(void)
{
    static const moq_version_t versions[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22
    };
    static const uint64_t types[] = {
        UINT64_C(0x3fffffffffffffff), UINT64_C(0x4000000000000000), UINT64_MAX
    };
    fixture_t f = {.count = 1, .decision = MOQ_MEDIA_AUTH_ALLOW};
    moq_alloc_t a = allocator(&f);
    for (unsigned dynamic = 0; dynamic < 2; ++dynamic) {
        for (size_t i = 0; i < sizeof(types) / sizeof(types[0]); ++i) {
            f.tokens[0] = (moq_auth_token_t){types[i], {NULL, 0}};
            moq_media_auth_cfg_t cfg = config_for(&f, dynamic != 0);
            moq_media_auth_source_t source = {0};
            CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == MOQ_OK);
            for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v) {
                moq_version_t draft = versions[v];
                moq_media_auth_operation_t op = {0};
                moq_media_auth_request_t req = request_for(MOQ_MEDIA_AUTH_ACTION_FETCH);
                f.request = &req;
                moq_result_t expected = draft == 16 && i != 0 ? MOQ_ERR_INVAL : MOQ_OK;
                CHECK(moq_media_auth_operation_prepare(&op, &source, &req,
                      (moq_version_t)draft, &a) == expected);
                if (expected == MOQ_OK) CHECK(op.list.tokens[0].token_type == types[i]);
                else CHECK(op.outcome == MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION);
                moq_media_auth_operation_clear(&op, &a);
            }
            moq_media_auth_source_clear(&source, &a);
        }
    }
    CHECK(f.live == 0 && f.alloc_calls == 0);
}

static void test_selection_failures(void)
{
    static const struct row {
        moq_result_t rc;
        moq_media_auth_decision_t decision;
        size_t count;
        moq_media_auth_outcome_t outcome;
        moq_result_t result;
    } rows[] = {
        {MOQ_OK, MOQ_MEDIA_AUTH_DENY, SIZE_MAX, MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED, MOQ_OK},
        {MOQ_ERR_CLOSED, MOQ_MEDIA_AUTH_ALLOW, SIZE_MAX, MOQ_MEDIA_AUTH_OUTCOME_SELECTOR_ERROR, MOQ_ERR_CLOSED},
        {MOQ_ERR_NOMEM, MOQ_MEDIA_AUTH_ALLOW, SIZE_MAX, MOQ_MEDIA_AUTH_OUTCOME_SELECTOR_ERROR, MOQ_ERR_NOMEM},
        {MOQ_ERR_WOULD_BLOCK, MOQ_MEDIA_AUTH_DENY, SIZE_MAX, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION, MOQ_ERR_INVAL},
        {MOQ_DONE, MOQ_MEDIA_AUTH_DENY, 0, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION, MOQ_ERR_INVAL},
        {7, MOQ_MEDIA_AUTH_ALLOW, 1, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION, MOQ_ERR_INVAL},
        {MOQ_OK, (moq_media_auth_decision_t)2, 1, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION, MOQ_ERR_INVAL},
        {MOQ_OK, MOQ_MEDIA_AUTH_ALLOW, 0, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION, MOQ_ERR_INVAL},
        {MOQ_OK, MOQ_MEDIA_AUTH_ALLOW, 17, MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION, MOQ_ERR_INVAL},
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); ++i) {
        fixture_t f = {.selector_result = rows[i].rc, .decision = rows[i].decision,
                       .count = rows[i].count};
        f.tokens[0] = (moq_auth_token_t){0, {(const uint8_t *)(uintptr_t)1, SIZE_MAX}};
        moq_alloc_t a = allocator(&f);
        moq_media_auth_cfg_t cfg = config_for(&f, true);
        moq_media_auth_source_t source = {0};
        CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == MOQ_OK);
        moq_media_auth_operation_t op = {0};
        moq_media_auth_request_t req = request_for(MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE);
        f.request = &req;
        f.op = &op;
        moq_result_t expected = rows[i].outcome == MOQ_MEDIA_AUTH_OUTCOME_POLICY_DENIED
                                  ? MOQ_DONE : rows[i].result;
        CHECK(moq_media_auth_operation_prepare(&op, &source, &req,
              MOQ_VERSION_DRAFT_18, &a) == expected);
        CHECK(op.outcome == rows[i].outcome && op.result == rows[i].result);
        CHECK(f.notifications == 0 && f.selections == 1 && f.alloc_calls == 0);
        CHECK(moq_media_auth_operation_prepare(&op, NULL, NULL,
              MOQ_VERSION_DRAFT_18, NULL) == expected);
        f.terminal_latched = true;
        moq_media_auth_scope_t scope = (moq_media_auth_scope_t)(1 + i % 3);
        moq_media_auth_operation_notify(&op, &source, &req, scope, NULL);
        moq_media_auth_operation_notify(&op, &source, &req, scope, NULL);
        CHECK(f.notifications == 1 && f.selections == 1);
        CHECK(f.observed.scope == scope && f.observed.track == NULL);
        CHECK(f.observed.outcome == rows[i].outcome && f.observed.result == rows[i].result);
        moq_media_auth_operation_clear(&op, &a);
        moq_media_auth_source_clear(&source, &a);
        CHECK(f.notifications == 1 && f.live == 0);
    }
}

static void test_selection_validation_and_oom(void)
{
    uint8_t bytes[] = {1, 0, 2};
    for (unsigned row = 0; row < 7; ++row) {
        fixture_t f = {.count = 1, .decision = MOQ_MEDIA_AUTH_ALLOW};
        f.tokens[0] = (moq_auth_token_t){1, {bytes, sizeof(bytes)}};
        moq_media_auth_outcome_t expected = MOQ_MEDIA_AUTH_OUTCOME_INVALID_SELECTION;
        moq_result_t result = MOQ_ERR_INVAL;
        moq_version_t version = MOQ_VERSION_DRAFT_18;
        switch (row) {
        case 0: f.tokens[0].token_value.data = NULL; break;
        case 1: f.tokens[1] = f.tokens[0]; f.count = 2; break;
        case 2: f.tokens[0].token_value.len = MOQ_MEDIA_AUTH_MAX_TOKEN_BYTES + 1;
                expected = MOQ_MEDIA_AUTH_OUTCOME_TOKEN_LIMIT; break;
        case 3: f.tokens[0].token_value.len = MOQ_MEDIA_AUTH_MAX_TOKEN_BYTES;
                f.tokens[1] = f.tokens[0]; f.tokens[2] = f.tokens[0]; f.count = 3;
                expected = MOQ_MEDIA_AUTH_OUTCOME_TOKEN_LIMIT; break;
        case 4: f.fail_alloc = true; expected = MOQ_MEDIA_AUTH_OUTCOME_TOKEN_COPY_FAILED;
                result = MOQ_ERR_NOMEM; break;
        case 5: version = (moq_version_t)99;
                expected = MOQ_MEDIA_AUTH_OUTCOME_UNSUPPORTED_PROFILE;
                result = MOQ_ERR_UNSUPPORTED; break;
        case 6: break;
        }
        moq_alloc_t a = allocator(&f);
        moq_media_auth_cfg_t cfg = config_for(&f, true);
        moq_media_auth_source_t source = {0};
        CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == MOQ_OK);
        moq_media_auth_operation_t op = {0};
        moq_media_auth_request_t req = request_for(row == 6 ? 99 : MOQ_MEDIA_AUTH_ACTION_PUBLISH);
        f.request = &req;
        f.op = &op;
        CHECK(moq_media_auth_operation_prepare(&op, &source, &req, version, &a) == result);
        CHECK(op.outcome == expected && op.list.count == 0 && !op.list.storage);
        CHECK(f.notifications == 0);
        CHECK(f.selections == (row >= 5 ? 0u : 1u));
        f.terminal_latched = true;
        moq_media_auth_operation_notify(&op, &source, &req, MOQ_MEDIA_AUTH_SCOPE_SERVICE, NULL);
        CHECK(f.notifications == 1 && f.observed.outcome == expected);
        CHECK(f.observed.result == result && f.live == 0);
        moq_media_auth_operation_clear(&op, &a);
        moq_media_auth_source_clear(&source, &a);
    }
    fixture_t f = {.count = 1, .fail_alloc = true};
    f.tokens[0] = (moq_auth_token_t){1, {bytes, sizeof(bytes)}};
    moq_alloc_t a = allocator(&f);
    moq_media_auth_cfg_t cfg = config_for(&f, false);
    moq_media_auth_source_t source = {0};
    CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == MOQ_ERR_NOMEM);
    CHECK(!source.configured && !source.list.storage && f.live == 0);
    CHECK(f.notifications == 0 && f.alloc_calls == 1);

    f.fail_alloc = false;
    CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == MOQ_OK);
    memset(&cfg, 0, sizeof(cfg));
    f.fail_alloc = true;
    moq_media_auth_operation_t op = {0};
    moq_media_auth_request_t req = request_for(MOQ_MEDIA_AUTH_ACTION_PUBLISH);
    f.op = &op;
    f.request = &req;
    CHECK(moq_media_auth_operation_prepare(&op, &source, &req,
          MOQ_VERSION_DRAFT_16, &a) == MOQ_ERR_NOMEM);
    CHECK(op.outcome == MOQ_MEDIA_AUTH_OUTCOME_TOKEN_COPY_FAILED);
    CHECK(f.notifications == 0 && f.live == 1 && op.list.count == 0);
    f.terminal_latched = true;
    moq_media_auth_operation_notify(&op, &source, &req, MOQ_MEDIA_AUTH_SCOPE_TRACK, NULL);
    CHECK(f.notifications == 1 && f.observed.result == MOQ_ERR_NOMEM);
    moq_media_auth_operation_clear(&op, &a);
    moq_media_auth_source_clear(&source, &a);
    CHECK(f.live == 0);
}

static void test_retained_selection(moq_version_t version)
{
    uint8_t bytes[] = {0x80, 0, 0xff};
    fixture_t f = {.count = 1, .decision = MOQ_MEDIA_AUTH_ALLOW};
    f.tokens[0] = (moq_auth_token_t){17, {bytes, sizeof(bytes)}};
    moq_alloc_t a = allocator(&f);
    moq_media_auth_cfg_t cfg = config_for(&f, true);
    moq_media_auth_source_t source = {0};
    CHECK(moq_media_auth_source_copy(&source, &cfg, &a) == MOQ_OK);
    memset(&cfg, 0, sizeof(cfg));
    moq_media_auth_operation_t op = {0};
    moq_media_auth_request_t req = request_for(MOQ_MEDIA_AUTH_ACTION_SUBSCRIBE);
    f.request = &req;
    CHECK(moq_media_auth_operation_prepare(&op, &source, &req,
          version, &a) == MOQ_OK);
    CHECK(f.selections == 1 && f.live == 1);
    const uint8_t *copy = op.list.tokens[0].token_value.data;
    CHECK(copy != bytes && memcmp(copy, bytes, sizeof(bytes)) == 0);
    memset(bytes, 0, sizeof(bytes));
    f.fail_alloc = true;
    for (unsigned retry = 0; retry < 3; ++retry) {
        CHECK(moq_media_auth_operation_prepare(&op, &source, &req,
              version, &a) == MOQ_OK);
        CHECK(op.list.tokens[0].token_value.data == copy);
        CHECK(copy[0] == 0x80 && copy[2] == 0xff);
    }
    moq_media_auth_operation_notify(&op, &source, &req, MOQ_MEDIA_AUTH_SCOPE_TRACK, NULL);
    CHECK(f.selections == 1 && f.alloc_calls == 1 && f.notifications == 0);
    moq_media_auth_operation_clear(&op, &a);
    CHECK(f.live == 0);
    f.fail_alloc = false;
    req.action = MOQ_MEDIA_AUTH_ACTION_REQUEST_UPDATE;
    CHECK(moq_media_auth_operation_prepare(&op, &source, &req,
          version, &a) == MOQ_OK);
    CHECK(f.selections == 2 && op.list.tokens[0].token_value.data[0] == 0);
    moq_media_auth_operation_clear(&op, &a);
    moq_media_auth_source_clear(&source, &a);
    CHECK(f.live == 0 && f.notifications == 0);
}

int main(void)
{
    test_sizing_and_sources();
    test_static_copy_and_empty();
    test_duplicates_and_budgets();
    test_profile_widths();
    test_selection_failures();
    test_selection_validation_and_oom();
    test_retained_selection(MOQ_VERSION_DRAFT_16);
    test_retained_selection(MOQ_VERSION_DRAFT_18);
    test_retained_selection(MOQ_VERSION_DRAFT_22);
    puts("media_auth: 7 groups PASS");
    return 0;
}
