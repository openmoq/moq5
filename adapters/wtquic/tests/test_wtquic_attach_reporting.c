#include <moq/wtquic.h>
#include <stdio.h>
#include <string.h>

static moq_result_t next_result;
static unsigned notifications, closes, forwarded, creates;
static int failures;
static moq_result_t fake_create(const moq_wtquic_conn_cfg_t *cfg, moq_wtquic_conn_t **out)
{ (void)cfg; creates++; *out = next_result == MOQ_OK ? (void *)&creates : NULL; return next_result; }
static void established(wtq_session_t *s, wtq_str_t sub, void *user)
{ (void)s; (void)sub; (void)user; forwarded++; }
static const wtq_session_events_t *fake_events(void)
{ static const wtq_session_events_t ev = { .on_established = established }; return &ev; }
static wtq_result_t fake_close(wtq_session_t *s, uint32_t code, const uint8_t *p, size_t n)
{ (void)s; (void)code; (void)p; (void)n; closes++; return WTQ_OK; }
#define moq_wtquic_conn_create fake_create
#define moq_wtquic_conn_events fake_events
#define wtq_session_close fake_close
#include "wtquic_attach_bootstrap.h"
#undef moq_wtquic_conn_create
#undef moq_wtquic_conn_events
#undef wtq_session_close
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL %d: %s\n", __LINE__, #x); failures++; } } while (0)
static void failed(void *ctx) { (void)ctx; notifications++; }
static void failed_reentrant(void *ctx)
{
    wtq_test_attach_t *b = ctx;
    CHECK(b->result == MOQ_ERR_NOMEM);
    notifications++;
    wtq_test_attach_events()->on_closed(NULL, 0, NULL, 0, false, b);
}

int main(void)
{
    const wtq_session_events_t *ev = wtq_test_attach_events();
    const moq_result_t results[] = { MOQ_ERR_UNSUPPORTED, MOQ_ERR_NOMEM };
    for (size_t i = 0; i < sizeof(results) / sizeof(results[0]); i++) {
        moq_wtquic_conn_t *out = NULL;
        wtq_test_attach_t b = { .out = &out, .failed = failed };
        notifications = closes = forwarded = creates = 0;
        next_result = results[i];
        ev->on_established(NULL, (wtq_str_t){0}, &b);
        ev->on_closed(NULL, 0, NULL, 0, false, &b);
        ev->on_failed(NULL, (wtq_connect_failure_t)0, &b);
        CHECK(b.result == results[i]);
        CHECK(notifications == 1 && closes == 1 && forwarded == 0);
        CHECK(out == NULL);
    }
    for (int kind = 0; kind < 3; kind++) {
        moq_wtquic_conn_t *out = NULL;
        wtq_test_attach_t b = { .out = &out, .failed = failed };
        notifications = closes = creates = 0;
        if (kind == 0) ev->on_refused(NULL, 403, &b);
        if (kind == 1) ev->on_failed(NULL, (wtq_connect_failure_t)0, &b);
        ev->on_closed(NULL, 0, NULL, 0, false, &b);
        ev->on_closed(NULL, 0, NULL, 0, false, &b);
        CHECK(b.result == MOQ_ERR_CLOSED);
        CHECK(notifications == 1 && creates == 0);
    }
    moq_wtquic_conn_t *out = NULL;
    wtq_test_attach_t b = { .out = &out, .failed = failed };
    notifications = forwarded = creates = 0;
    next_result = MOQ_OK;
    ev->on_established(NULL, (wtq_str_t){0}, &b);
    CHECK(b.result == MOQ_OK && out != NULL && creates == 1);
    CHECK(forwarded == 1 && notifications == 0);
    ev->on_established(NULL, (wtq_str_t){0}, &b);
    CHECK(creates == 1 && forwarded == 1);
    out = NULL;
    memset(&b, 0, sizeof(b));
    b.out = &out;
    b.failed = failed_reentrant;
    b.cfg.hook_user = &b;
    next_result = MOQ_ERR_NOMEM;
    notifications = closes = creates = 0;
    ev->on_established(NULL, (wtq_str_t){0}, &b);
    ev->on_established(NULL, (wtq_str_t){0}, &b);
    CHECK(notifications == 1 && closes == 1 && creates == 1);
    CHECK(b.result == MOQ_ERR_NOMEM);
    return failures ? 1 : 0;
}
