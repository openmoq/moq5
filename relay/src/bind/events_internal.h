#ifndef MOQR_BIND_EVENTS_INTERNAL_H
#define MOQR_BIND_EVENTS_INTERNAL_H

#include "bind_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

void
bind_on_event(moqr_bind_t *b, b_conn_t *cn, moq_event_t *ev,
              uint64_t now_us);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* MOQR_BIND_EVENTS_INTERNAL_H */
