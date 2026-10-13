#include "auth_source.h"
#include "test_support.h"
#include <stdlib.h>
#include <string.h>

static int failures;
static size_t outstanding;
static bool fail_alloc;
static void *test_alloc(size_t n, void *ctx)
{
    (void)ctx;
    if (fail_alloc) return NULL;
    void *p = malloc(n);
    if (p) ++outstanding;
    return p;
}
static void test_free(void *p, size_t n, void *ctx)
{
    (void)n; (void)ctx;
    if (p) { --outstanding; free(p); }
}
static const moq_alloc_t alloc = { NULL, test_alloc, NULL, test_free };
static uint8_t bytes[] = { 0xd2, 0, 0xff, 0x80 };
static unsigned calls;
static moq_result_t callback_result;
static size_t callback_count = 1;
static moq_result_t select_token(void *ctx, const moq_auth_request_t *r,
    moq_auth_token_t *tokens, size_t capacity, size_t *count)
{
    MOQ_TEST_CHECK(ctx == &calls);
    MOQ_TEST_CHECK(r->action == MOQ_AUTH_FETCH);
    MOQ_TEST_CHECK(r->ns.count == 1 && r->ns.parts[0].len == 3);
    MOQ_TEST_CHECK(r->name.len == 7 && memcmp(r->name.data, "catalog", 7) == 0);
    MOQ_TEST_CHECK(capacity == MOQ_AUTH_SOURCE_MAX_TOKENS);
    ++calls;
    tokens[0] = (moq_auth_token_t){1, {bytes, sizeof(bytes)}};
    *count = callback_count;
    return callback_result;
}

int main(void)
{
    moq_auth_source_t source;
    moq_auth_source_init_sized(&source, sizeof(source));
    MOQ_TEST_CHECK(source.struct_size == sizeof(source));
    moq_auth_source_owned_t owned = {0};
    moq_auth_owned_list_t selected = {0};
    moq_bytes_t part = MOQ_BYTES_LITERAL("a/b");
    moq_auth_request_t request = {MOQ_AUTH_FETCH, {&part, 1}, MOQ_BYTES_LITERAL("catalog")};
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, NULL, &alloc) == MOQ_OK);
    MOQ_TEST_CHECK(moq_auth_source_select(&owned, &request, &alloc, &selected) == MOQ_OK);
    MOQ_TEST_CHECK(selected.count == 0);
    moq_auth_source_clear(&owned, &alloc);
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_INVAL);

    moq_auth_token_t token = {1, {bytes, sizeof(bytes)}};
    source.tokens = &token; source.token_count = 1;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_OK);
    bytes[0] = 0;
    MOQ_TEST_CHECK(moq_auth_source_select(&owned, &request, &alloc, &selected) == MOQ_OK);
    if (selected.count == 1) {
        MOQ_TEST_CHECK(selected.tokens[0].token_type == 1);
        MOQ_TEST_CHECK(selected.tokens[0].token_value.len == sizeof(bytes));
        MOQ_TEST_CHECK(selected.tokens[0].token_value.data[0] == 0xd2);
        MOQ_TEST_CHECK(selected.tokens[0].token_value.data[1] == 0);
    } else MOQ_TEST_CHECK(false);
    moq_auth_source_clear(&owned, &alloc);
    if (selected.count == 1) MOQ_TEST_CHECK(selected.tokens[0].token_value.data[0] == 0xd2);
    moq_auth_list_clear(&selected, &alloc);
    moq_auth_list_clear(&selected, &alloc);
    bytes[0] = 0xd2;

    source.select = select_token; source.ctx = &calls;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_INVAL);
    source.tokens = NULL; source.token_count = 0;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_OK);
    MOQ_TEST_CHECK(moq_auth_source_select(&owned, &request, &alloc, &selected) == MOQ_OK);
    MOQ_TEST_CHECK(calls == 1 && selected.count == 1);
    bytes[0] = 0;
    if (selected.count == 1) MOQ_TEST_CHECK(selected.tokens[0].token_value.data[0] == 0xd2);
    moq_auth_list_clear(&selected, &alloc);
    callback_result = MOQ_ERR_CLOSED;
    MOQ_TEST_CHECK(moq_auth_source_select(&owned, &request, &alloc, &selected) == MOQ_ERR_CLOSED);
    callback_result = MOQ_DONE;
    MOQ_TEST_CHECK(moq_auth_source_select(&owned, &request, &alloc, &selected) == MOQ_ERR_INVAL);
    callback_result = MOQ_OK; callback_count = 0;
    MOQ_TEST_CHECK(moq_auth_source_select(&owned, &request, &alloc, &selected) == MOQ_ERR_INVAL);
    callback_count = 17;
    MOQ_TEST_CHECK(moq_auth_source_select(&owned, &request, &alloc, &selected) == MOQ_ERR_INVAL);
    callback_count = 1; fail_alloc = true;
    MOQ_TEST_CHECK(moq_auth_source_select(&owned, &request, &alloc, &selected) == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(selected.count == 0 && selected.storage == NULL);
    fail_alloc = false;
    moq_auth_source_clear(&owned, &alloc);

    moq_auth_source_init_sized(&source, sizeof(source));
    source.tokens = &token; source.token_count = 1;
    fail_alloc = true;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_NOMEM);
    fail_alloc = false;
    MOQ_TEST_CHECK(!owned.configured && owned.list.storage == NULL);
    source.token_count = 17;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_INVAL);
    source.token_count = 1; token.token_type = UINT64_MAX;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_INVAL);
    token.token_type = 1; token.token_value.data = NULL;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_INVAL);
    token.token_value.data = bytes; token.token_value.len = 0;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_INVAL);
    token.token_value.len = MOQ_AUTH_SOURCE_MAX_TOKEN_BYTES + 1;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_INVAL);
    uint8_t big[MOQ_AUTH_SOURCE_MAX_TOKEN_BYTES] = {0};
    moq_auth_token_t three[3] = {{1,{big,sizeof(big)}},{16,{big,sizeof(big)}},{1,{big,1}}};
    source.tokens = three; source.token_count = 3;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_INVAL);
    source.token_count = 2;
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_OK);
    MOQ_TEST_CHECK(owned.list.storage_size == MOQ_AUTH_SOURCE_MAX_TOTAL_BYTES);
    moq_auth_source_clear(&owned, &alloc);
    source.struct_size = offsetof(moq_auth_source_t, ctx);
    MOQ_TEST_CHECK(moq_auth_source_copy(&owned, &source, &alloc) == MOQ_ERR_INVAL);
    struct { moq_auth_source_t value; uint8_t canary[8]; } oversized;
    memset(&oversized, 0xa5, sizeof(oversized));
    moq_auth_source_init_sized(&oversized.value, sizeof(oversized));
    MOQ_TEST_CHECK(oversized.value.struct_size == sizeof(source));
    for (size_t i=0;i<sizeof(oversized.canary);++i) MOQ_TEST_CHECK(oversized.canary[i] == 0xa5);
    uint8_t tiny[3] = {0xa5,0xa5,0xa5};
    moq_auth_source_init_sized((moq_auth_source_t *)(void *)tiny, sizeof(tiny));
    MOQ_TEST_CHECK(tiny[0] == 0 && tiny[1] == 0 && tiny[2] == 0);
    MOQ_TEST_CHECK(outstanding == 0);
    MOQ_TEST_PASS("auth_source");
    return failures ? 1 : 0;
}
