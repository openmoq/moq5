#ifndef MOQ_TRANSPORT_BINDING_INTERNAL_H
#define MOQ_TRANSPORT_BINDING_INTERNAL_H

#include "moq/transport_binding.h"
#include "../base/uri_internal.h"

/* The session's copy of its transport binding. value.authority and
 * value.path_query point into owned_target (one allocation for both). */
typedef struct moq_binding_state {
    moq_transport_binding_t value;
    uint8_t *owned_target;
    size_t   owned_size;
    bool     bound;
} moq_binding_state_t;

/* True when the profile requires a binding and none has been supplied: every
 * input path refuses with MOQ_ERR_WRONG_STATE until the host binds. */
bool moq_binding_required_unbound(const moq_session_t *s);
void moq_binding_destroy(moq_session_t *s);

/* True when this side is a client bound natively over QUIC: its SETUP names
 * the connection target, PATH and AUTHORITY taken from the binding. Never
 * true for a server, a WebTransport binding, or an unbound session. */
bool moq_binding_sends_target(const moq_session_t *s);

/*
 * Judge the PATH and AUTHORITY a peer's SETUP carried (presence plus value).
 * Both travel client to server, natively over QUIC only: from a server or
 * over WebTransport each is INVALID_*, a value outside RFC 3986 syntax is
 * MALFORMED_*, and a raw-QUIC server's URI policy decides whether it serves
 * the named target. Returns 0 to accept, otherwise the session close code
 * with *reason set. Side-effect free apart from the policy call, which may
 * run again if the SETUP has to be retried before it commits.
 */
typedef struct moq_binding_peer_target {
    bool        has_path;
    bool        has_authority;
    moq_bytes_t path;        /* read only when has_path */
    moq_bytes_t authority;   /* read only when has_authority */
} moq_binding_peer_target_t;

uint64_t moq_binding_judge_peer_target(const moq_session_t *s,
                                       const moq_binding_peer_target_t *target,
                                       const char **reason);

#endif
