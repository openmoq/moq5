/*
 * The admin surface: a bounded HTTP/1.1 request parser, content negotiation,
 * and a deterministic multi-client response state machine.
 *
 * Sans-I/O by construction. Nothing here opens a socket, starts a thread, or
 * reads a clock: bytes arrive through `moqr_admin_on_bytes`, time arrives as an
 * explicit `now_us`, and output is handed back as a span the caller writes.
 *
 * THREADING CONTRACT. This module takes no lock of its own. Step 4 must hold
 * ONE lock across each of these transactions, because each spans a broker call
 * and an admin call that must not interleave with another:
 *
 *   - request/bind:      moqr_broker_request + moqr_admin_bind_serial
 *   - retirement:        moqr_admin_claim_abandon + broker complete/take
 *                        + moqr_admin_record_take, then
 *                        moqr_admin_settle_abandon (which performs the broker
 *                        release and settles in one non-yielding call)
 *   - bank transition:   moqr_broker_take_serial + moqr_admin_reserve
 *   - release:           moqr_admin_peek_release + moqr_broker_release
 *                        + moqr_admin_ack_release
 *
 * A rejoin arriving between a CLAIM and its SETTLEMENT is refused rather than
 * raced: once a serial is RETIRING the caller has already begun retiring it in
 * the broker, so a new request opens a new generation instead.
 * Every verdict is therefore reproducible from an input transcript alone, which
 * is what lets slow-client and overload behaviour be tested without sleeping.
 *
 * FIXED MEMORY. Clients, request buffers and body banks are all preallocated
 * arrays inside `moqr_admin_t`. There is no allocation on the request path, so
 * a scrape storm changes latency, never footprint. A request that would exceed
 * a bound is refused with a status, not grown into.
 *
 * TWO BODY BANKS. Each bank holds one rendered Prometheus body, one rendered
 * OpenMetrics body, one rendered /api/v1/shards JSON body (the body slots,
 * MOQR_ADMIN_BODY__COUNT of them, all from one frozen row set), and a pin
 * count. A client pins the bank it is writing from
 * for as long as it is writing, so a generation being sent can never be
 * overwritten underneath it. A third generation arriving while both banks are
 * pinned is refused with 503 rather than truncated or queued unboundedly.
 *
 * NO ERROR RESPONSE CARRIES A METRICS BODY, not even a partial one. Error
 * bodies are fixed, short, and generated from the status alone.
 */
#ifndef MOQR_ADMIN_H
#define MOQR_ADMIN_H

#include <moqr_obs.h>
#include <moqrelay/types.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* -- bounds ---------------------------------------------------------------
 * All fixed at compile time in v1. Promoting any of these to configuration
 * requires wiring it into the capacity model in the same change, which is why
 * they are constants here and not fields. */
#define MOQR_ADMIN_MAX_CLIENTS      8u
#define MOQR_ADMIN_MAX_REQUEST     2048u  /* whole request head, bytes        */
#define MOQR_ADMIN_MAX_LINE         512u  /* request line or one header line  */
#define MOQR_ADMIN_MAX_HEADERS       32u
#define MOQR_ADMIN_MAX_RANGES         8u  /* media ranges parsed from Accept  */
#define MOQR_ADMIN_BANKS              2u  /* must equal MOQR_BROKER_BANKS     */

/* Fixed deadlines, in microseconds of the caller's clock. */
#define MOQR_ADMIN_READ_DEADLINE_US   (5u * 1000u * 1000u)
#define MOQR_ADMIN_WRITE_DEADLINE_US (10u * 1000u * 1000u)
/* Bounded wait for a generation before answering 503. */
#define MOQR_ADMIN_EPOCH_DEADLINE_US  (2u * 1000u * 1000u)
#define MOQR_ADMIN_RETRY_AFTER_S      1u  /* rendered into 503 */
/* The largest immutable document the machine will agree to serve. A length
 * above this is refused at construction, before any byte of it is read: an
 * alleged length is not a payload, and the write accounting stays finite for
 * every admitted document. The owner's rendered bound must fit under it. */
#define MOQR_ADMIN_MAX_STATIC_DOC     (256u * 1024u)

/* -- the finite response table -------------------------------------------
 * Exactly these codes, and no others, may leave this module. Anything a future
 * branch cannot classify must map onto one of them deliberately. */
typedef uint32_t moqr_http_status_t;

#define MOQR_HTTP_200 200u  /* a complete, valid exposition                  */
#define MOQR_HTTP_400 400u  /* malformed request                             */
#define MOQR_HTTP_404 404u  /* unknown target                                */
#define MOQR_HTTP_405 405u  /* any method other than GET; carries Allow: GET */
#define MOQR_HTTP_406 406u  /* nothing acceptable could be produced          */
#define MOQR_HTTP_431 431u  /* a request or header bound was exceeded        */
#define MOQR_HTTP_500 500u  /* poisoned/unknown capability, invariant broken  */
#define MOQR_HTTP_503 503u  /* incomplete epoch, bank exhaustion, overload   */

/* Media types emitted verbatim. Prometheus 3.0+ fails a scrape that arrives
 * without a precise Content-Type, so exactly one is always sent. */
#define MOQR_ADMIN_CT_OPENMETRICS \
    "application/openmetrics-text; version=1.0.0; charset=utf-8"
#define MOQR_ADMIN_CT_PROMETHEUS \
    "text/plain; version=0.0.4; charset=utf-8"
#define MOQR_ADMIN_CT_JSON \
    "application/json; charset=utf-8"

/* -- content negotiation --------------------------------------------------
 *
 * Decided rules, in force order:
 *
 *   - a missing Accept, or the full wildcard range, selects OpenMetrics 1.0;
 *   - the highest q-value wins;
 *   - on equal preference OpenMetrics wins;
 *   - `q=0` marks that range NOT acceptable;
 *   - a media range that parses but names something unknown is SKIPPED, not
 *     an error -- a client listing formats we do not serve alongside one we do
 *     must still be served;
 *   - a media parameter we do not recognise NARROWS the representation and
 *     makes that range unavailable, WHEREVER it appears: RFC 9110 removed accept-ext.
 *     Recipients recognise `q` regardless of ordering, so position does not
 *     decide whether a parameter is a media parameter;
 *   - a duplicate q, version or charset is malformed: last-one-wins is not a
 *     decision;
 *   - malformed syntax REFUSES with 400, because a request we cannot parse is
 *     not a request whose intent we may guess;
 *   - nothing acceptable gives 406.
 *
 * Duplicate Accept headers are combined in order, exactly as a single
 * comma-separated field, per the HTTP field-combining rule.
 *
 * A MISSING Accept field selects OpenMetrics. A PRESENT field whose combined
 * value is empty or whitespace-only is MALFORMED, not equivalent to missing:
 * they are different assertions by the client. That distinction is enforced by
 * moqr_http_parse, which knows whether the field was present; this function
 * takes NULL/0 to mean absent.
 *
 * `accept` may be NULL (header absent). Returns 200 with *out set, or 400/406/
 * 431. *out is untouched on any non-200.
 */
moqr_http_status_t moqr_http_negotiate(const char *accept, size_t len,
                                       moqr_obs_format_t *out);

/* -- the request ---------------------------------------------------------- */
typedef uint32_t moqr_http_target_t;

#define MOQR_HTTP_TARGET_UNKNOWN 0u
#define MOQR_HTTP_TARGET_METRICS 1u   /* /metrics                           */
#define MOQR_HTTP_TARGET_INFO    2u   /* /api/v1/info                       */
#define MOQR_HTTP_TARGET_SHARDS  3u   /* /api/v1/shards                     */

/*
 * The REPRESENTATION a request negotiated. It selects the content type; the
 * target selects where the body comes from. The two are validated as a pair
 * before either indexes anything: JSON is never an index into a metrics
 * body array, and a metrics format never labels a JSON document.
 *
 * Each target has its own served-representation table, and the one accepted
 * negotiation grammar (specificity, q, exclusions, refusals) runs over
 * whichever table the target selects.
 */
typedef uint32_t moqr_http_rep_t;

#define MOQR_HTTP_REP_METRICS 0u  /* body = bank body[fmt]; fmt is valid    */
#define MOQR_HTTP_REP_JSON    1u  /* body = an immutable JSON document; fmt
                                   * is the canonical MOQR_OBS_FMT__COUNT   */

typedef struct moqr_http_request {
    moqr_http_target_t target;
    moqr_http_rep_t    rep;
    moqr_obs_format_t  fmt;      /* valid only when the status was 200; for a
                                  * JSON representation it is the canonical
                                  * MOQR_OBS_FMT__COUNT, never a bank index */
} moqr_http_request_t;

/*
 * Parse one complete request head.
 *
 * PRECEDENCE, fixed and tested: bounds (431) before syntax (400) before method
 * (405) before target (404) before negotiation (406). A request can violate
 * several rules at once, so the order in which they are reported is part of the
 * contract rather than an accident of the code.
 *
 * An HTTP/1.1 request must carry exactly one valid, non-empty Host field
 * (RFC 9112 3.2); missing, repeated or empty is 400. HTTP/1.0 predates that
 * requirement and is deliberately exempt.
 *
 * v1 accepts no request body: a Content-Length or Transfer-Encoding header is
 * malformed here (400), not ignored. Ignoring it would leave undrained bytes
 * that the next parse would read as a new request.
 *
 * `len` is the exact length of the head including its terminating CRLFCRLF.
 */
moqr_http_status_t moqr_http_parse(const char *buf, size_t len,
                                   moqr_http_request_t *out);

/* Render the fixed response head (and fixed error body) for `status` into
 * `buf`. Never emits a metrics body. Returns the byte count, or 0 if `cap` is
 * too small or, for 200, if `(rep, fmt)` is not a valid pair: a metrics
 * representation needs a real format, a JSON one needs the canonical
 * MOQR_OBS_FMT__COUNT. `content_len` is used only for 200; `rep`/`fmt` only
 * for 200. */
size_t moqr_http_write_head(moqr_http_status_t status, moqr_http_rep_t rep,
                            moqr_obs_format_t fmt, size_t content_len,
                            char *buf, size_t cap);

/* -- the state machine ---------------------------------------------------- */

typedef uint32_t moqr_admin_cstate_t;

#define MOQR_ADMIN_CS_FREE    0u
#define MOQR_ADMIN_CS_READING 1u  /* accumulating the request head           */
#define MOQR_ADMIN_CS_PARSED  2u  /* head valid; needs a generation assigned */
#define MOQR_ADMIN_CS_WAITING 3u  /* bound to an exact serial, awaiting it   */
#define MOQR_ADMIN_CS_WRITING 4u  /* pinning a bank, writing head+body       */
#define MOQR_ADMIN_CS_DONE    5u  /* fully written or dropped; caller closes */

/* Bank lifecycle. A bank is never written into except while RESERVED, and
 * never delivered except while READY, so a renderer and a reader can never be
 * looking at the same storage. */
typedef uint32_t moqr_admin_bstate_t;

#define MOQR_ADMIN_BS_FREE     0u
#define MOQR_ADMIN_BS_RESERVED 1u  /* a renderer owns the storage            */
#define MOQR_ADMIN_BS_READY    2u  /* committed; deliverable                 */
#define MOQR_ADMIN_BS_RELEASING 3u /* owes a release the broker has not yet
                                    * acknowledged; NOT reusable             */

/* The release token a bank owes.
 *
 * This is HELD IN THE BANK, not queued. A queue can fill, and a token dropped
 * from a full queue is a broker bank leaked forever -- so the carrier is the
 * bank itself, whose capacity is exactly the number of banks and therefore
 * cannot be exceeded by construction.
 *
 * The protocol is peek / release / acknowledge:
 *
 *     while (moqr_admin_peek_release(a, &tok)) {
 *         rc = moqr_broker_release(b, tok.serial, tok.bank, &wake);
 *         if (rc != MOQR_OK) break;              // retryable: see below
 *         moqr_admin_ack_release(a, tok.serial, tok.bank);   // SAME transaction
 *     }
 *
 * THE BOUNDARY, stated exactly. A REFUSED broker release is retryable: neither
 * the broker nor the bank changed, so re-peeking returns the same token. A
 * SUCCESSFUL broker release is NOT: the broker has let the bank go, and the
 * acknowledgement must follow in the same non-yielding owner transaction. An
 * interruption between a successful release and its acknowledgement would
 * re-peek the same token and release it a second time. There is no
 * re-enterable "released" phase here, and this module does not pretend to
 * offer one.
 *
 * The bank stays unavailable until the broker has authenticated the release, so
 * storage the broker still considers SENDING can never be re-rendered. A
 * duplicate or stale acknowledgement fails closed and changes nothing. */
typedef struct moqr_admin_release {
    uint64_t serial;
    uint32_t bank;
} moqr_admin_release_t;

/* An in-flight generation, as an explicit state machine.
 *
 * Two independent booleans admitted stale combinations that no transition
 * should ever produce -- abandoned-and-rejoined, abandoned-and-banked -- and
 * every such pair had to be interpreted somewhere. One state cannot be in two
 * of these at once.
 *
 *   FREE      -> JOINED     a request binds this serial
 *   JOINED    -> BANKED     a bank is reserved for it; the bank owes release
 *   JOINED    -> ABANDONED  its last waiter left before any bank took it
 *   ABANDONED -> JOINED     a waiter rejoined before the retirement was claimed
 *   ABANDONED -> BANKED     a bank was reserved before it was claimed
 *   ABANDONED -> RETIRING   claimed by the caller for broker retirement
 *   RETIRING  -> FREE       the coupled settlement retired it in the broker
 *   BANKED    -> FREE       the bank's own release settles it
 */
typedef uint32_t moqr_admin_gstate_t;

#define MOQR_ADMIN_GS_FREE      0u
#define MOQR_ADMIN_GS_JOINED    1u
#define MOQR_ADMIN_GS_BANKED    2u
#define MOQR_ADMIN_GS_ABANDONED 3u
#define MOQR_ADMIN_GS_RETIRING  4u

typedef struct moqr_admin_gen {
    uint64_t            serial;
    uint32_t            waiters;
    moqr_admin_gstate_t state;
    /* The broker token, once taken. `moqr_broker_take_serial` is DESTRUCTIVE:
     * it moves the slot from READY to SENDING, so a second take of the same
     * serial fails. The token is therefore retained here, because a caller
     * that dropped it on a local variable could not finish the retirement. */
    bool                token_taken;
    uint32_t            token_demand;
    uint32_t            token_bank;
} moqr_admin_gen_t;

/* What a retirement claim hands back. */
typedef struct moqr_admin_retire {
    uint64_t serial;
    /* True when the broker token has ALREADY been taken for this serial, in
     * which case `demand` and `bank` are the retained token and the caller must
     * NOT take it again. */
    bool     taken;
    uint32_t demand;
    uint32_t bank;
} moqr_admin_retire_t;

/*
 * BODY SLOTS. Each bank holds one rendered body per slot: the metrics
 * formats (indexed by moqr_obs_format_t, MOQR_OBS_FMT__COUNT of them) and ONE
 * JSON API body for /api/v1/shards. The slot space is a separate index type:
 * the JSON body is never a metrics format, and MOQR_OBS_FMT__COUNT stays the
 * metrics-format count. A generation renders every slot from one frozen row
 * set, so metrics and shards served from one bank describe one epoch.
 */
typedef uint32_t moqr_admin_body_t;

#define MOQR_ADMIN_BODY_SHARDS  ((moqr_admin_body_t)MOQR_OBS_FMT__COUNT)
#define MOQR_ADMIN_BODY__COUNT  (MOQR_OBS_FMT__COUNT + 1u)

typedef uint32_t moqr_admin_carrier_t;

#define MOQR_ADMIN_CARRIER_NONE   0u  /* no body, or an error body in head  */
#define MOQR_ADMIN_CARRIER_BANK   1u  /* a pinned bank body (metrics)       */
#define MOQR_ADMIN_CARRIER_STATIC 2u  /* the immutable document the machine
                                       * was constructed with (/api/v1/info);
                                       * no generation, no pin, no release  */

typedef struct moqr_admin_client {
    moqr_admin_cstate_t state;
    char                req[MOQR_ADMIN_MAX_REQUEST];
    size_t              req_len;
    moqr_http_request_t parsed;
    moqr_http_status_t  status;
    uint64_t            serial;     /* the EXACT generation this request
                                     * joined; a client is never attached to
                                     * any other */
    uint32_t            bank;       /* pinned bank, valid when WRITING  */
    bool                pinned;
    /* Where the body BYTES live. This is body storage, not generation
     * membership: a metrics waiter joins its generation at bind time with no
     * bank, and leaves it by serial on every exit path regardless of this. */
    moqr_admin_carrier_t carrier;
    /* Deadlines are start + budget, never an absolute instant. An absolute
     * deadline saturated at the top of the range can never be exceeded, which
     * makes the client immortal once the caller's clock reaches the edge;
     * elapsed time computed in unsigned arithmetic is correct across the whole
     * range including wrap. */
    uint64_t            dl_start_us;
    uint64_t            dl_budget_us;
    /* Bytes of THIS response actually handed out. Cancellation is classified by
     * this, not by the WRITING label: a selected-but-unsent 200 can still
     * become a 503, while one byte on the wire cannot be rewound. */
    uint64_t            bytes_out;
    /* Response assembly: the head is fixed-size; the body is a borrowed span
     * into the pinned bank, so no metrics bytes are ever copied per client. */
    char                head[512];
    size_t              head_len;
    size_t              head_sent;
    const char         *body;
    size_t              body_len;
    size_t              body_sent;
} moqr_admin_client_t;

typedef struct moqr_admin_bank {
    uint64_t            serial;
    uint32_t            pins;
    moqr_admin_bstate_t state;
    char               *body[MOQR_ADMIN_BODY__COUNT];
    size_t              body_len[MOQR_ADMIN_BODY__COUNT];
    size_t              body_cap[MOQR_ADMIN_BODY__COUNT];
} moqr_admin_bank_t;

typedef struct moqr_admin {
    moqr_admin_client_t client[MOQR_ADMIN_MAX_CLIENTS];
    moqr_admin_bank_t   bank[MOQR_ADMIN_BANKS];
    uint32_t            clients_used;
    bool                cancelled;
    /* One entry per distinct in-flight generation. A bind either joins an
     * existing serial or creates one, and creating one requires a client slot,
     * so this can never need more entries than there are clients. */
    moqr_admin_gen_t    gen[MOQR_ADMIN_MAX_CLIENTS];
    /* Monotonic publication counter. See the sample/recheck contract below --
     * on its own this is a number, not a notification. */
    uint64_t            publish_gen;
    /* The immutable /api/v1/info document, BORROWED from the constructor's
     * caller, who keeps it valid for the machine's whole life. Served as a
     * borrowed span like a bank body, but through no generation and no pin. */
    const char         *info;
    size_t              info_len;
} moqr_admin_t;

/*
 * `bodies` supplies the preallocated storage for every body slot in both
 * banks: bodies[bank][slot], each of at least `caps[slot]` bytes -- the two
 * metrics formats and the shards JSON body. `info` is the
 * complete, immutable /api/v1/info document (`info_len` bytes, no NUL
 * required), frozen before construction and owned by the caller for the
 * machine's whole life; a NULL or empty document, or one longer than
 * MOQR_ADMIN_MAX_STATIC_DOC, is refused without reading a byte of it, because
 * a machine that advertises the target must be able to serve it. The state
 * machine never allocates.
 */
moqr_result_t moqr_admin_init(moqr_admin_t *a,
                              char *bodies[MOQR_ADMIN_BANKS][MOQR_ADMIN_BODY__COUNT],
                              const size_t caps[MOQR_ADMIN_BODY__COUNT],
                              const char *info, size_t info_len);
void          moqr_admin_destroy(moqr_admin_t *a);

/* Admit a connection. Returns a client index, or MOQR_ERR_CAPACITY when all
 * slots are in use -- the caller answers 503 and closes. */
moqr_result_t moqr_admin_accept(moqr_admin_t *a, uint64_t now_us,
                                uint32_t *out_client);

/* Feed received bytes. Partial heads are accumulated; the request is parsed
 * only once CRLFCRLF is seen. Bounds are enforced as bytes arrive, so an
 * oversized head is refused before it is buffered.
 *
 * Bytes arriving after a complete head end this request, whatever state it has
 * reached: an unstarted response is replaced with 400 (releasing any bank it
 * pinned), and a started one is dropped. Returns MOQR_OK whenever a response
 * was constructed or dropped -- the caller keeps draining its pending output --
 * and MOQR_ERR_WRONG_STATE only when there was nothing left to act on. */
moqr_result_t moqr_admin_on_bytes(moqr_admin_t *a, uint32_t client,
                                  const char *data, size_t len,
                                  uint64_t now_us);

/* -- the generation handshake --------------------------------------------
 *
 * A parsed request does NOT pick its own generation. It is parked in PARSED
 * until the caller assigns it the exact serial the broker handed back, because
 * a waiter attached to "whichever bank is ready" receives whatever generation
 * happens to be sitting there -- which, with two banks and one slow reader, is
 * routinely an older one than the request joined.
 *
 * The Step 4 loop is:
 *
 *     while (moqr_admin_next_demand(a, &c)) {
 *         rc = moqr_broker_request(b, MOQR_BROKER_DEMAND_HTTP, &serial, &wake);
 *         if (rc == MOQR_OK)  moqr_admin_bind_serial(a, c, serial, now);
 *         else                moqr_admin_refuse(a, c, MOQR_HTTP_503, now);
 *     }
 *
 * The two refusals are deliberately different, and the difference is the
 * Step 2 contract: a broker that has no bank to open answers THAT request 503
 * at once, because nothing is coming; a request admitted onto a collecting
 * epoch waits its bounded epoch deadline and only then answers 503.
 */

/* The next client that has a valid request and no generation yet. */
bool moqr_admin_next_demand(const moqr_admin_t *a, uint32_t *out_client);

/* Bind `client` to the exact `serial` the broker assigned. `serial` must be
 * nonzero -- zero is the broker's "no generation" value and must never reach a
 * waiter as an identity. MOQR_ERR_CAPACITY if no generation slot is free, which
 * the caller answers 503. */
moqr_result_t moqr_admin_bind_serial(moqr_admin_t *a, uint32_t client,
                                     uint64_t serial, uint64_t now_us);

/* Fail this request closed now, with no generation and no pin. `status` must be
 * one of the finite table's ERROR statuses: a refusal is never 200, and never a
 * code the table does not contain. Validated before anything is mutated. */
moqr_result_t moqr_admin_refuse(moqr_admin_t *a, uint32_t client,
                                moqr_http_status_t status, uint64_t now_us);

/* -- reserve / render / commit -------------------------------------------
 *
 * A renderer writes into caller-owned storage, so `publish`-after-the-fact
 * cannot protect anything: by the time it could refuse, the bytes are already
 * overwritten. The bank is therefore claimed BEFORE rendering and committed
 * after, and the storage is only reachable while the claim is held.
 */

/* Claim a FREE bank for `serial` (nonzero).
 *
 * `bank` MUST be the bank index the broker returned for this serial. The two
 * allocators are not independent: the admin's body bank IS the broker's bank,
 * so a mismatched pairing would have the broker reject the eventual release.
 *
 * MOQR_ERR_WOULD_BLOCK when that bank is reserved, ready, releasing or pinned.
 * MOQR_ERR_INVAL when the serial is zero, or is already held by ANY bank --
 * one broker generation must never occupy two banks, or it would owe two
 * release tokens. */
moqr_result_t moqr_admin_reserve(moqr_admin_t *a, uint32_t bank,
                                 uint64_t serial);

/* The storage to render into. NULL unless `bank` is RESERVED for `serial`. */
char *moqr_admin_bank_storage(moqr_admin_t *a, uint32_t bank, uint64_t serial,
                              moqr_admin_body_t slot, size_t *out_cap);

/* Commit the rendered lengths. Every length is validated BEFORE anything is
 * stored, so a refused commit leaves the bank byte-for-byte unchanged.
 *
 * A commit requires a JOINED or BANKED generation for `serial`: a bank rendered
 * for demand that is not an HTTP request has no reader here and would sit READY
 * forever. Signal-only output is read synchronously from the RESERVED bank and
 * then aborted -- that is the closed owner transition for that path.
 *
 * If the last waiter left while the renderer held the bank, the commit succeeds
 * and the bank immediately owes its release: it is never left READY, unpinned
 * and unreachable. */
moqr_result_t moqr_admin_commit(moqr_admin_t *a, uint32_t bank, uint64_t serial,
                                const size_t len[MOQR_ADMIN_BODY__COUNT],
                                uint64_t now_us);

/* Give up a reservation. The bank moves to RELEASING and CARRIES its own
 * release until the broker acknowledges it; nothing is queued anywhere. */
moqr_result_t moqr_admin_abort(moqr_admin_t *a, uint32_t bank, uint64_t serial);

/* The release a bank owes, WITHOUT consuming it. Repeated peeks return the same
 * token until it is acknowledged, so a REFUSED broker release is retried
 * unchanged. A SUCCESSFUL one must be acknowledged in the same non-yielding
 * transaction -- see the boundary stated on moqr_admin_release_t. */
bool moqr_admin_peek_release(const moqr_admin_t *a, moqr_admin_release_t *out);

/* Acknowledge that the broker accepted the release of exactly this
 * {serial, bank}. Only then does the bank become reusable. A duplicate, stale
 * or mismatched acknowledgement is MOQR_ERR_INVAL and changes nothing --
 * accepting one would free storage the broker still calls SENDING. */
moqr_result_t moqr_admin_ack_release(moqr_admin_t *a, uint64_t serial,
                                     uint32_t bank);

/* -- abandoned generations ------------------------------------------------
 *
 * A generation can be opened in the broker and joined by a request that then
 * times out, closes or is cancelled BEFORE anything was ever rendered into a
 * bank. No bank exists, so no release token can exist, and the broker would
 * collect that serial forever -- every later request joins the dead epoch and
 * times out too. That is not a slow scrape; it is permanent starvation.
 *
 * When the last HTTP waiter for such a generation is gone, its serial is
 * surfaced here. Step 4 retires it with the ordinary broker vocabulary --
 * complete it, take its token, honour any SIGNAL demand the token carries, then
 * release it -- so a demand that joined the same serial is still served. A
 * serial that has reached a bank is never abandoned: the bank owes its release.
 */
/* CLAIM a generation awaiting broker retirement: it moves ABANDONED ->
 * RETIRING, which is what excludes a rejoin, so this MUTATES and is not a peek.
 * Re-claiming an already-RETIRING generation returns the same serial and makes
 * no further transition.
 *
 * A BANKED serial is never surfaced here: its bank owes the release.
 *
 * THE PHASE CONTRACT, stated honestly. `moqr_broker_take_serial` is
 * destructive -- it moves the slot READY -> SENDING -- so the whole sequence is
 * NOT restartable once take has succeeded:
 *
 *   phase 1, before take:   re-entering from the claim is safe. Nothing has
 *                           been consumed; `out->taken` is false.
 *   phase 2, after take:    the exact {serial, demand, bank} token must be
 *                           carried through to release. A second take of the
 *                           same serial FAILS. Record it with
 *                           moqr_admin_record_take so a later claim hands it
 *                           back in `out->taken`/`demand`/`bank` -- a caller
 *                           that kept it only in a local would have lost it.
 *   phase 3, release+settle: ONE non-yielding transaction. There is no
 *                           re-enterable "released" phase, because there could
 *                           be no honest way to record one: any separate
 *                           acknowledgement leaves exactly the same gap between
 *                           a successful broker release and the record of it,
 *                           and a re-entry landing in that gap would release a
 *                           second time. moqr_admin_settle_abandon therefore
 *                           performs the broker release through a callback and
 *                           settles the admin record in the same stack frame.
 *
 * Post-release re-entry is NOT supported, and this module does not pretend to
 * support it.
 *
 * ONE policy for a failing callback: it is an INVARIANT FAILURE, not a retry.
 * With the retained token and the single owner lock documented above, the
 * broker is being handed exactly the token it issued for a bank it still holds,
 * so a refusal means the two have already disagreed. The admin record is left
 * untouched -- so the state is intact for reporting and for an orderly teardown,
 * NOT so the owner can loop on it. Do not build a retry around this return. */
bool moqr_admin_claim_abandon(moqr_admin_t *a, moqr_admin_retire_t *out);

/* Record the broker token taken for a claimed serial, so the retirement can be
 * finished even if the caller's loop is interrupted after the take. The serial
 * must be RETIRING; a second record for the same serial is MOQR_ERR_INVAL. */
moqr_result_t moqr_admin_record_take(moqr_admin_t *a, uint64_t serial,
                                     uint32_t demand, uint32_t bank);

/* Perform the broker release for a retired generation and settle the admin
 * record, as ONE transaction with no yield point between them.
 *
 * `release` is the caller's broker release, invoked with the retained token.
 * It must not re-enter this module. On MOQR_OK the admin record is settled
 * here, in the same call, so no interruption can fall between the two. On any
 * other result nothing is mutated: the retained token stays readable for
 * DIAGNOSIS and orderly teardown. Do not settle again -- see the single policy
 * stated on moqr_admin_claim_abandon.
 *
 * Requires a recorded take: phase 3 cannot be reached from phase 1. A stale,
 * duplicate or unknown serial is MOQR_ERR_INVAL and changes nothing. */
typedef moqr_result_t (*moqr_admin_release_fn)(void *ctx, uint64_t serial,
                                               uint32_t demand, uint32_t bank);

moqr_result_t moqr_admin_settle_abandon(moqr_admin_t *a, uint64_t serial,
                                        moqr_admin_release_fn release,
                                        void *ctx);

/* -- generation-wide failure --------------------------------------------
 *
 * A poisoned capability or an impossible renderer failure is a property of the
 * GENERATION, not of any one request. Letting its waiters reach the epoch
 * deadline answers 503 -- "try again shortly" -- which is a false statement
 * about a generation that is broken rather than late.
 *
 * Every still-unstarted waiter for `serial` is answered `status` (500 for a
 * poisoned or invariant-broken generation). A waiter whose response has already
 * put a byte on the wire is dropped, by the same byte rule cancellation uses.
 * Any bank holding the serial owes its release; a generation that never reached
 * a bank becomes retirable. No metrics body is ever attached.
 */
moqr_result_t moqr_admin_fail_serial(moqr_admin_t *a, uint64_t serial,
                                     moqr_http_status_t status,
                                     uint64_t now_us);

/* -- driving ------------------------------------------------------------- */

/* Advance every client: attach waiters to the bank carrying THEIR serial,
 * expire deadlines, release pins. Pure function of state and `now_us`. */
moqr_result_t moqr_admin_tick(moqr_admin_t *a, uint64_t now_us);

/* The next span this client wants written. False when it has nothing pending.
 * The span stays valid until the next call that advances this client. */
bool moqr_admin_pending(const moqr_admin_t *a, uint32_t client,
                        const char **out, size_t *out_len);

/* Report how many bytes of the pending span were actually written. A short
 * write is normal and is resumed on the next call. Only POSITIVE progress
 * refreshes the write deadline: refreshing on a zero-byte report would let a
 * client hold a bank open forever by reporting nothing, repeatedly. */
moqr_result_t moqr_admin_on_written(moqr_admin_t *a, uint32_t client,
                                    size_t n, uint64_t now_us);

/* Release the client slot and any pin it holds. */
moqr_result_t moqr_admin_close(moqr_admin_t *a, uint32_t client);

/*
 * Cancel every in-flight request and refuse new work.
 *
 * A request that has not begun writing is failed closed with 503. A response
 * already partway onto the wire is NOT switched to 503 -- the status line has
 * been sent and the protocol cannot be rewound -- so it is dropped
 * deterministically and its pin and token released immediately.
 */
void moqr_admin_cancel(moqr_admin_t *a, uint64_t now_us);

/* True when at least one bank is FREE, so a new generation has somewhere to be
 * rendered. */
bool moqr_admin_bank_available(const moqr_admin_t *a);

/* -- publication predicate ------------------------------------------------
 *
 * A LEVEL-TRIGGERED predicate, not a notification. This module has no mutex and
 * no condition variable, and does not claim to synchronise anything: it exports
 * a monotonic epoch so that Step 4's own wait loop cannot lose a wake.
 *
 * The contract Step 4 must follow, holding its own lock across the sample:
 *
 *     lock();
 *     epoch = moqr_admin_publish_epoch(a);
 *     while (!moqr_admin_publish_since(a, epoch) && !done) {
 *         cond_wait(&cv, &mu);      // a commit between sample and wait is
 *     }                             // still visible, because the epoch moved
 *     unlock();
 *
 * The property that makes this safe is that a commit ADVANCES a counter rather
 * than SETTING a flag: a flag consumed between the check and the wait is gone,
 * whereas an advanced epoch remains observable no matter when it is read. What
 * this does NOT do is provide the mutual exclusion -- passing an epoch sampled
 * outside the caller's lock is a caller bug this module cannot detect.
 */
uint64_t moqr_admin_publish_epoch(const moqr_admin_t *a);
bool     moqr_admin_publish_since(const moqr_admin_t *a, uint64_t epoch);

#endif /* MOQR_ADMIN_H */
