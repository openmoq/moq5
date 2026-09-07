/*
 * The ONE closed list of targets the admin endpoint serves, in the order the
 * /api/v1/info document and the JSON readiness event advertise them. The
 * router's accepted target set is pinned against this list by test.
 */
#ifndef MOQR_CLI_ADMIN_TARGETS_H
#define MOQR_CLI_ADMIN_TARGETS_H

#include <stddef.h>

#define MOQR_CLI_ADMIN_TARGET_COUNT 3u

static inline const char *
moqr_cli_admin_target(size_t i)
{
    static const char *const targets[MOQR_CLI_ADMIN_TARGET_COUNT] = {
        "/metrics", "/api/v1/info", "/api/v1/shards",
    };
    return i < MOQR_CLI_ADMIN_TARGET_COUNT ? targets[i] : NULL;
}

#endif /* MOQR_CLI_ADMIN_TARGETS_H */
