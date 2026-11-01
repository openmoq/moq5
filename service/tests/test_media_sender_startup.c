/* Real executor + sender attach/add/detach/hook control, without a transport.
 * This complements Rust ownership tests; it does not execute the Rust wrapper. */
#include "test_support.h"
#include <moq/media_sender.h>
#include <moq/sim.h>
#include <string.h>

moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *);
void moq_endpoint_test_set_facade_version(moq_endpoint_t *, moq_version_t);
void moq_endpoint_test_pump_cycle(moq_endpoint_t *, moq_session_t *, uint64_t);
void moq_endpoint_test_drain_terminal(moq_endpoint_t *);
int moq_endpoint_test_attachments(moq_endpoint_t *);
size_t moq_endpoint_test_task_count(moq_endpoint_t *);
void moq_media_sender_test_startup_hook_reset(void);
size_t moq_media_sender_test_startup_hook_calls(void);
size_t moq_media_sender_test_startup_first_tracks(void);

static int failures;
typedef struct {
    bool cancelled, fail_second;
    unsigned calls;
    moq_result_t result;
    moq_media_sender_t *sender;
} task_t;

static moq_result_t register_tracks(moq_endpoint_t *ep, moq_session_t *session,
                                    uint64_t now, void *ctx)
{
    (void)now;
    task_t *t = ctx;
    t->calls++;
    if (t->cancelled || !session) return MOQ_OK;
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    moq_bytes_t part = { (const uint8_t *)"startup", 7 };
    cfg.namespace_ = (moq_namespace_t){ &part, 1 };
    t->result = moq_media_sender_attach(ep, &cfg, &t->sender);
    if (t->result < 0) return MOQ_OK;
    for (unsigned i = 0; i < 2; ++i) {
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init(&tc);
        const char *name = i == 0 ? "video" : "audio";
        tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_RAW;
        tc.codec = (moq_bytes_t){ (const uint8_t *)"av01", 4 };
        tc.bitrate = 1000000;
        tc.is_live = true;
        if (i == 1 && t->fail_second) tc.name = (moq_bytes_t){ NULL, 0 };
        moq_media_track_t *track = NULL;
        t->result = moq_media_sender_add_track(t->sender, &tc, &track);
        if (t->result < 0) {
            moq_media_sender_destroy(t->sender);
            t->sender = NULL;
            break;
        }
    }
    return MOQ_OK;
}

int main(void)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = MOQ_VERSION_DRAFT_16;
    cfg.initial_now_us = 1000;
    moq_simpair_t *pair = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_create(&cfg, &pair), MOQ_OK);
    if (!pair) return 1;
    moq_simpair_start(pair);
    MOQ_TEST_CHECK_EQ_INT(moq_simpair_run_until_quiescent(pair, 64, NULL), MOQ_OK);
    moq_session_t *session = moq_simpair_client(pair);
    for (unsigned row = 0; row < 4; ++row) {
        moq_endpoint_t *ep = moq_endpoint_test_make_bare();
        MOQ_TEST_CHECK(ep != NULL);
        if (!ep) return 1;
        moq_endpoint_test_set_facade_version(ep, moq_session_version(session));
        moq_media_sender_test_startup_hook_reset();
        task_t task = { .cancelled = row == 2, .fail_second = row == 1 };
        MOQ_TEST_CHECK_EQ_INT(moq_endpoint_post(ep, register_tracks, &task), MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(moq_endpoint_test_task_count(ep), 1);
        MOQ_TEST_CHECK_EQ_SIZE(moq_media_sender_test_startup_hook_calls(), 0);
        if (row == 3) moq_endpoint_test_drain_terminal(ep);
        else moq_endpoint_test_pump_cycle(ep, session, 1000);
        MOQ_TEST_CHECK_EQ_INT(task.calls, 1);
        MOQ_TEST_CHECK_EQ_SIZE(moq_endpoint_test_task_count(ep), 0);
        if (row == 0) {
            MOQ_TEST_CHECK_EQ_INT(task.result, MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(moq_media_sender_test_startup_hook_calls(), 1);
            MOQ_TEST_CHECK_EQ_SIZE(moq_media_sender_test_startup_first_tracks(), 2);
            MOQ_TEST_CHECK_EQ_INT(moq_endpoint_test_attachments(ep), 1);
        } else {
            if (row == 1) MOQ_TEST_CHECK(task.result < 0);
            MOQ_TEST_CHECK_EQ_SIZE(moq_media_sender_test_startup_hook_calls(), 0);
            MOQ_TEST_CHECK_EQ_INT(moq_endpoint_test_attachments(ep), 0);
        }
        if (task.sender) moq_media_sender_destroy(task.sender);
        MOQ_TEST_CHECK_EQ_INT(moq_endpoint_test_attachments(ep), 0);
        moq_endpoint_test_free_bare(ep);
    }
    moq_simpair_destroy(pair);
    return failures ? 1 : 0;
}
