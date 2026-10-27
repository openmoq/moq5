#ifndef PR62_ACCEPT_PUBLISH_HISTORICAL_H
#define PR62_ACCEPT_PUBLISH_HISTORICAL_H
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
typedef struct historical_accept_observation {
    size_t size, floor, expiry_end;
    bool canary_ok, unchanged;
} historical_accept_observation_t;
int historical_accept_call(void *session, uint64_t publication, uint64_t now,
    unsigned variant, bool exact, historical_accept_observation_t *out);
#endif
