/* Independent old caller: deliberately includes NO current MoQ header.
 * Captures the released input layout through has_expires, including sizeof
 * tail padding, rather than deriving an old layout from today's offsets. */
#include "accept_publish_historical.h"
#include <stdlib.h>
#include <string.h>
typedef struct moq_session moq_session_t;
typedef struct moq_publication { uint64_t _opaque; } moq_publication_t;
typedef struct moq_accept_publish_cfg {
    uint32_t struct_size;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
    uint32_t group_order;
    bool has_new_group_request;
    uint64_t new_group_request;
    uint32_t filter;
    uint64_t start_group, start_object, end_group;
    bool has_forward, forward;
    uint64_t expires_ms;
    bool has_expires;
} moq_accept_publish_cfg_t;
extern int moq_session_accept_publish(moq_session_t *, moq_publication_t,
    const moq_accept_publish_cfg_t *, uint64_t);

static void init_old(moq_accept_publish_cfg_t *cfg, unsigned variant)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
    if (variant == 1) { cfg->has_forward = true; cfg->forward = false; }
    if (variant == 2) { cfg->has_expires = true; cfg->expires_ms = 1; }
    size_t end = offsetof(moq_accept_publish_cfg_t, has_expires) + sizeof(bool);
    memset((unsigned char *)cfg + end, 0xA5, sizeof(*cfg) - end);
}

int historical_accept_call(void *session, uint64_t publication, uint64_t now,
    unsigned variant, bool exact, historical_accept_observation_t *out)
{
    struct { moq_accept_publish_cfg_t cfg; unsigned char guard[16]; } old;
    unsigned char before[sizeof(old.cfg)];
    memset(out, 0, sizeof(*out));
    out->size = sizeof(old.cfg);
    out->floor = offsetof(moq_accept_publish_cfg_t, has_new_group_request);
    out->expiry_end = offsetof(moq_accept_publish_cfg_t, has_expires) + sizeof(bool);
    init_old(&old.cfg, variant);
    memset(old.guard, 0xB6, sizeof(old.guard));
    memcpy(before, &old.cfg, sizeof(before));
    /* Exact allocation supplies ASan redzones immediately past old sizeof;
     * the companion form checks writes against an adjacent canary too. */
    moq_accept_publish_cfg_t *input = exact ? malloc(sizeof(old.cfg)) : &old.cfg;
    if (!input) return -10000; /* test allocation failure, not a library result */
    if (exact) memcpy(input, before, sizeof(before));
    int rc = moq_session_accept_publish(session, (moq_publication_t){ ._opaque = publication }, input, now);
    out->unchanged = memcmp(input, before, sizeof(before)) == 0;
    out->canary_ok = true;
    for (size_t i = 0; i < sizeof(old.guard); ++i)
        out->canary_ok &= old.guard[i] == 0xB6;
    if (exact) free(input);
    return rc;
}
