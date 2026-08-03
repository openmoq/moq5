#ifndef FAKE_MSQ_TABLE_H
#define FAKE_MSQ_TABLE_H

/*
 * Fake QUIC_API_TABLE for the msquic adapter tests.
 *
 * MsQuic's entire API is a function-pointer table the adapter receives
 * at create time, so the fake needs no link-time stubbing at all: it
 * hands the adapter a table whose entries record every downcall and
 * queue state the test then feeds back through the adapter's public
 * callback handlers, deterministically and with no real transport.
 *
 * One fake_msq_t models one endpoint's transport. Stream handles are
 * pointers to fake_msq_stream_t. The test delivers connection events
 * by invoking moq_msquic_conn_callback() directly and stream events
 * via the helpers below.
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <msquic.h>

#define FAKE_MSQ_MAX_STREAMS 64
#define FAKE_MSQ_MAX_SENDS 256
#define FAKE_MSQ_MAX_DGRAMS 128
#define FAKE_MSQ_HELD_MAX 8192

typedef struct fake_msq fake_msq_t;

typedef struct fake_msq_stream {
    fake_msq_t *owner;
    QUIC_STREAM_CALLBACK_HANDLER cb;
    void *ctx;
    uint64_t id;
    bool in_use;
    bool uni;
    bool started;
    QUIC_STREAM_START_FLAGS start_flags;
    bool closed;               /* StreamClose seen */
    int shutdown_calls;        /* StreamShutdown count */
    QUIC_STREAM_SHUTDOWN_FLAGS last_shutdown_flags;
    uint64_t last_shutdown_code;
    int receive_set_enabled;   /* +1 per TRUE, -1 per FALSE */
    bool receive_enabled;      /* last StreamReceiveSetEnabled value */

    /* Receive partial-acceptance model (default, non-multi-receive mode).
     * When a RECEIVE callback returns having consumed fewer bytes than
     * were indicated (RECEIVE.TotalBufferLength lowered), MsQuic stops
     * indicating RECEIVE and holds the unconsumed tail until a
     * StreamReceiveSetEnabled(TRUE); the tail is then re-indicated. The
     * fake mirrors that: it stashes the unconsumed bytes here and only
     * re-indicates them when the test calls fake_msq_redeliver_held after
     * the adapter has re-enabled the stream. */
    bool recv_disabled;        /* delivery halted by a partial accept */
    uint8_t held[FAKE_MSQ_HELD_MAX];
    uint32_t held_len;
    bool held_fin;
} fake_msq_stream_t;

typedef struct fake_msq_dgram {
    const uint8_t *data;       /* first buffer's bytes (borrowed) */
    uint32_t len;
    void *client_ctx;
    bool completed;
} fake_msq_dgram_t;

#define FAKE_MSQ_SEND_CAP 1024

typedef struct fake_msq_send {
    fake_msq_stream_t *stream;
    const uint8_t *data;       /* first buffer's bytes (borrowed) */
    uint32_t len;              /* first buffer's length */
    uint32_t buf_count;        /* buffers in this send */
    uint64_t total;            /* sum over all buffers */
    QUIC_SEND_FLAGS flags;
    void *client_ctx;
    bool completed;
    /* all buffers concatenated (capped) — a stable copy for relaying a
     * send's bytes into a peer's receive path in paired tests */
    uint8_t bytes[FAKE_MSQ_SEND_CAP];
    uint32_t bytes_len;
} fake_msq_send_t;

struct fake_msq {
    QUIC_API_TABLE api;
    bool is_client;            /* drives fake stream-id numbering */
    /* forced id override for the next StreamStart (UINT64_MAX = off):
     * exercises the adapter's id-verification fatal path */
    uint64_t force_next_id;

    fake_msq_stream_t streams[FAKE_MSQ_MAX_STREAMS];
    int stream_count;
    uint64_t bidi_count;       /* locally started, for id numbering */
    uint64_t uni_count;

    fake_msq_send_t sends[FAKE_MSQ_MAX_SENDS];
    int send_count;

    fake_msq_dgram_t dgrams[FAKE_MSQ_MAX_DGRAMS];
    int dgram_count;

    int stream_send_fails;     /* make the next N StreamSend calls fail */
    int dgram_send_fails;      /* make the next N DatagramSend calls fail */
    int recv_set_enabled_fails;/* make the next N StreamReceiveSetEnabled
                                  calls fail (exercises the resume-fatal
                                  path) */
    int conn_shutdowns;
    uint64_t last_conn_shutdown_code;
};

void fake_msq_init(fake_msq_t *f, bool is_client);
const QUIC_API_TABLE *fake_msq_table(fake_msq_t *f);

/* The connection handle to bind (identifies the fake). */
HQUIC fake_msq_conn_handle(fake_msq_t *f);

/* Local streams in creation order (NULL past the end). */
fake_msq_stream_t *fake_msq_stream_at(fake_msq_t *f, int index);

/* A peer-initiated stream the test conjures (assigning a real-QUIC-
 * numbered id is the caller's business); returns the handle to put in
 * a PEER_STREAM_STARTED event. */
fake_msq_stream_t *fake_msq_peer_stream(fake_msq_t *f, uint64_t id,
                                        bool uni);

/* Deliver START_COMPLETE for a started local stream, with the fake's
 * assigned id (or the forced one) and the given status. */
void fake_msq_deliver_start_complete(fake_msq_stream_t *st,
                                     QUIC_STATUS status);

/* Deliver SEND_COMPLETE for the oldest uncompleted send (FIFO);
 * returns false when none is pending. */
bool fake_msq_deliver_send_complete(fake_msq_t *f, bool canceled);

/* Deliver stream bytes / FIN into the adapter's stream callback as one
 * single-buffer RECEIVE event. Honors the partial-acceptance model: if
 * the adapter consumes fewer than `len` bytes, the unconsumed tail is
 * stashed for redelivery and the stream's delivery is marked disabled. */
void fake_msq_deliver_receive(fake_msq_stream_t *st, const uint8_t *data,
                              size_t len, bool fin);

/* Deliver a multi-buffer RECEIVE event (count contiguous buffers) — the
 * shape MsQuic uses when data spans internal segments. Same partial-
 * acceptance handling as the single-buffer form. */
void fake_msq_deliver_receive_multi(fake_msq_stream_t *st,
                                    const uint8_t *const *bufs,
                                    const size_t *lens, uint32_t count,
                                    bool fin);

/* True once a RECEIVE was partially accepted (delivery halted); cleared
 * when the held bytes are redelivered. */
bool fake_msq_stream_recv_disabled(fake_msq_stream_t *st);

/* Re-indicate the bytes MsQuic held after a partial accept, as MsQuic
 * would after the app re-enables the stream. Returns false if the stream
 * still has delivery disabled (no StreamReceiveSetEnabled(TRUE) seen) or
 * has nothing held. */
bool fake_msq_redeliver_held(fake_msq_stream_t *st);

/* Count of sends not yet completed. */
int fake_msq_pending_sends(fake_msq_t *f);

/* Recorded datagram sends, in order (NULL past the end). */
fake_msq_dgram_t *fake_msq_dgram_at(fake_msq_t *f, int index);

/* Mark the oldest uncompleted datagram completed and return its
 * ClientContext for a DATAGRAM_SEND_STATE_CHANGED event the test
 * delivers (NULL when none is pending). */
void *fake_msq_next_dgram_ctx(fake_msq_t *f);

/* Count of datagram sends not yet completed. */
int fake_msq_pending_dgrams(fake_msq_t *f);

#endif /* FAKE_MSQ_TABLE_H */
