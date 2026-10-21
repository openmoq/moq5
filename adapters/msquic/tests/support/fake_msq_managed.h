#ifndef FAKE_MSQ_MANAGED_H
#define FAKE_MSQ_MANAGED_H

/*
 * A facade-level fake MsQuic API table: everything moq_msquic_managed_create
 * touches (registration, configuration, listener, accepted-connection ops),
 * layered over the per-connection fake in fake_msq_table.h, whose stream
 * behavior it reuses unchanged. No sockets, no threads, no library: the test
 * fabricates accepts and delivers connection events itself, on its own
 * thread, so every transport "callback" is a plain deterministic call.
 */

#include <stdbool.h>
#include <stdint.h>

#include "fake_msq_table.h"

#define FAKE_MGD_MAX_CONNS 8 /* LIVE conns; a slot frees on
                               * ConnectionClose and is reused, so long
                               * scripted runs stay bounded by the live
                               * population, not history */

typedef struct fake_mgd_conn {
    fake_msq_t fake;      /* the conn handle IS &this->fake                */
    bool       in_use;
    bool       closed;    /* ConnectionClose observed                      */
    int        shutdowns; /* ConnectionShutdown calls                      */
    bool       configured; /* ConnectionSetConfiguration observed          */
    /* the handler the facade installed for this connection */
    QUIC_CONNECTION_CALLBACK_HANDLER cb;
    void       *ctx;
} fake_mgd_conn_t;

typedef struct fake_mgd {
    QUIC_API_TABLE api;
    /* dummy top-level handles (their addresses are the HQUICs) */
    int reg_obj, cfg_obj, listener_obj;
    bool reg_open, cfg_open, listener_open, listener_started;
    bool cred_loaded;
    /* captured listener callback */
    QUIC_LISTENER_CALLBACK_HANDLER listener_cb;
    void *listener_ctx;
    HQUIC listener_handle;
    fake_mgd_conn_t conns[FAKE_MGD_MAX_CONNS];
    int conn_count;   /* live (in_use) conns                              */
    int conn_hwm;     /* highest slot ever used: the iteration bound      */
} fake_mgd_t;

void fake_mgd_init(fake_mgd_t *g);
const QUIC_API_TABLE *fake_mgd_table(fake_mgd_t *g);

/* Fabricate one accepted connection: invokes the captured listener callback
 * with a NEW_CONNECTION event for a fresh fake connection. Returns the child
 * (its handler captured via SetCallbackHandler) or NULL if the facade refused
 * the accept. `alpn` is the negotiated ALPN the event advertises. */
fake_mgd_conn_t *fake_mgd_accept(fake_mgd_t *g, const char *alpn);

/* Deliver CONNECTED to an accepted child (handshake completion). */
void fake_mgd_deliver_connected(fake_mgd_conn_t *c, const char *alpn);

/* Fabricate a peer-initiated stream on an accepted child (as MsQuic would
 * via PEER_STREAM_STARTED) and return the fake stream to deliver receives
 * on. After this the stream's callback routes to the adapter's handler. */
fake_msq_stream_t *fake_mgd_peer_stream_started(fake_mgd_conn_t *c,
                                                uint64_t id, bool uni);

/* Deliver peer stream aborts to an accepted child's stream. */
void fake_mgd_deliver_peer_send_aborted(fake_mgd_conn_t *c,
                                        fake_msq_stream_t *st, uint64_t code);
void fake_mgd_deliver_peer_recv_aborted(fake_mgd_conn_t *c,
                                        fake_msq_stream_t *st, uint64_t code);

/* Deliver the transport terminal: SHUTDOWN_INITIATED_BY_PEER (optional,
 * orderly close with `code`) then SHUTDOWN_COMPLETE. This is exactly the
 * worker batch that makes terminal/reapable state and the queued
 * SESSION_CLOSED visible — delivered synchronously on the caller's thread.
 * The final helper cancels every accepted send/datagram to quiescence first;
 * the peer-close helper alone deliberately leaves those operations pending. */
void fake_mgd_deliver_peer_close(fake_mgd_conn_t *c, uint64_t code);
void fake_mgd_deliver_shutdown_complete(fake_mgd_conn_t *c);

#endif /* FAKE_MSQ_MANAGED_H */
