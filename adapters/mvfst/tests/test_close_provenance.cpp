/*
 * Close provenance on the MoQ control stream, at the socket's callback level.
 *
 * mvfst calls a stream's readError for a RESET_STREAM from the peer, and also
 * for every stream that still has a read callback when the connection closes.
 * The attach adapter registers a socket observer to tell the two apart and
 * refuses a socket that cannot take one. The socket double below reproduces
 * the consumed provider's own order for each way a connection ends
 * (QuicTransportBaseLite::close / closeImpl, QuicTransportBase::closeGracefully):
 *
 *   peer or transport close: close starts (observers told), the connection
 *     is marked closed so writes fail, the close is reflected onto every read
 *     callback, then the connection callback runs;
 *   close():   the connection callbacks are removed first, then the same
 *     close -- no connection callback reaches the adapter;
 *   closeGracefully(): the connection callbacks are removed, then
 *     LocalErrorCode::NO_ERROR is reflected onto every read callback before
 *     any close starts; the close that follows reflects nothing more.
 *
 * Rows, on attached clients of draft-16 (control on the first bidi, stream 0)
 * and of drafts 18 and 22 (control on a pair of unidirectional streams):
 *
 *   - the peer resets the control stream, application code zero or not, no
 *     close begun: the control stream has ended, the open session closes as
 *     PROTOCOL_VIOLATION (0x3) (draft-ietf-moq-transport-16 section 3.3, -18
 *     section 3.3, -22 section 6.3);
 *   - a peer close: the reflected errors decide nothing, on the bidi control
 *     stream and on the peer's unidirectional control stream alike, the
 *     adapter writes nothing while the close is in progress even with output
 *     pending, and the connection callback decides;
 *   - the session closes first and the adapter closes the socket: that close
 *     stands, and no connection callback is needed;
 *   - the caller closes the socket with close() or closeGracefully(): an
 *     orderly close with the code it carried, without a connection callback,
 *     and without replacing a close the session decided first;
 *   - a socket with no observer container: attach refused with
 *     MOQ_ERR_UNSUPPORTED, the socket left as it was given;
 *   - destruction, adapter first and socket last.
 *
 * The socket is mvfst's own MockQuicSocket with a real observer container,
 * owned by the double as a full transport owns its own. No network, no
 * EventBase.
 */

#include <moq/mvfst.hpp>
#include <moq/session.h>

#include "../../../tests/unit/test_binding_support.h"

#include <folly/io/IOBuf.h>
#include <quic/api/test/MockQuicSocket.h>
#include <quic/observer/SocketObserverContainer.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <map>
#include <memory>
#include <string>
#include <vector>

/* Allocation can be made to fail for one window: a close notice must not
 * allocate (it is noexcept, so a failure there would end the process). */
static bool g_fail_alloc = false;
void *operator new(std::size_t n)
{
    if (g_fail_alloc) throw std::bad_alloc();
    if (void *p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }

using ::testing::_;
using ::testing::Invoke;
using ::testing::NiceMock;
using ::testing::Return;

static int failures = 0;
#define MVFST_CHECK(expr) do { \
    if (!(expr)) { \
        std::fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static size_t g_observers_at_destruction = SIZE_MAX;
static int g_sockets_destroyed = 0;

/* A socket double that keeps the provider's close order, records what the
 * adapter asks of it, and owns an observer container for itself, or has
 * none. */
struct observed_socket : public NiceMock<quic::MockQuicSocket> {
    explicit observed_socket(bool observable) : container(this) {
        quic::SocketObserverContainer *ctr = observable ? &container : nullptr;
        ON_CALL(*this, getSocketObserverContainer())
            .WillByDefault(Return(ctr));
        ON_CALL(*this, setConnectionSetupCallback(_))
            .WillByDefault(Invoke(
                [this](folly::MaybeManagedPtr<
                       quic::QuicSocket::ConnectionSetupCallback> p) {
                    setup_cb_sets++;
                    setup_cb = p.get();
                }));
        ON_CALL(*this, setConnectionCallback(_))
            .WillByDefault(Invoke(
                [this](folly::MaybeManagedPtr<
                       quic::QuicSocket::ConnectionCallback> p) {
                    conn_cb_sets++;
                    conn_cb = p.get();
                }));
        ON_CALL(*this, setDatagramCallback(_))
            .WillByDefault(Invoke([this](quic::QuicSocket::DatagramCallback *) {
                dgram_cb_sets++;
                return quic::Expected<void, quic::LocalErrorCode>{};
            }));
        ON_CALL(*this, setReadCallback(_, _, _))
            .WillByDefault(Invoke(
                [this](quic::StreamId id, quic::QuicSocket::ReadCallback *cb,
                       quic::Optional<quic::ApplicationErrorCode>) {
                    read_cb_sets++;
                    if (closed)
                        return quic::Expected<void, quic::LocalErrorCode>(
                            quic::make_unexpected(
                                quic::LocalErrorCode::CONNECTION_CLOSED));
                    if (cb) read_cbs[id] = cb;
                    else read_cbs.erase(id);
                    return quic::Expected<void, quic::LocalErrorCode>{};
                }));
        ON_CALL(*this, createBidirectionalStream(_))
            .WillByDefault(Return(
                quic::Expected<quic::StreamId, quic::LocalErrorCode>(
                    quic::StreamId{0})));
        ON_CALL(*this, createUnidirectionalStream(_))
            .WillByDefault(Return(
                quic::Expected<quic::StreamId, quic::LocalErrorCode>(
                    quic::StreamId{2})));
        ON_CALL(*this, getNumOpenableBidirectionalStreams())
            .WillByDefault(Return(uint64_t{16}));
        ON_CALL(*this, getNumOpenableUnidirectionalStreams())
            .WillByDefault(Return(uint64_t{16}));
        ON_CALL(*this, writeChain(_, _, _, _))
            .WillByDefault(Invoke(
                [this](quic::StreamId id, SharedBuf buf, bool,
                       quic::ByteEventCallback *) {
                    if (closed) {
                        writes_after_close++;
                        return quic::QuicSocket::WriteResult(
                            quic::make_unexpected(
                                quic::LocalErrorCode::CONNECTION_CLOSED));
                    }
                    writes++;
                    if (buf) {
                        auto &out = outbound[id];
                        for (const auto &range : *buf)
                            out.insert(out.end(), range.begin(), range.end());
                    }
                    return quic::QuicSocket::WriteResult{};
                }));
        ON_CALL(*this, readNaked(_, _))
            .WillByDefault(Invoke([this](quic::StreamId id, size_t) {
                auto it = inbound.find(id);
                if (it == inbound.end() || it->second.empty())
                    return ReadResult(std::pair<folly::IOBuf *, bool>(
                        nullptr, false));
                auto buf = folly::IOBuf::copyBuffer(it->second.data(),
                                                    it->second.size());
                it->second.clear();
                return ReadResult(std::pair<folly::IOBuf *, bool>(
                    buf.release(), false));
            }));
        ON_CALL(*this, close(_))
            .WillByDefault(Invoke([this](quic::Optional<quic::QuicError> e) {
                closes++;
                if (e && e->code.asApplicationErrorCode())
                    close_code = *e->code.asApplicationErrorCode();
                /* QuicTransportBaseLite::close: the caller no longer wants
                 * connection callbacks, then the close itself */
                conn_cb = nullptr;
                begin_close(e);
            }));
        ON_CALL(*this, closeGracefully())
            .WillByDefault(Invoke([this]() {
                graceful_closes++;
                if (closed) return;
                /* QuicTransportBase::closeGracefully: callbacks removed,
                 * NO_ERROR reflected, then the close (nothing left to
                 * reflect) */
                conn_cb = nullptr;
                reflect(quic::QuicError(
                    quic::QuicErrorCode(quic::LocalErrorCode::NO_ERROR),
                    "Graceful Close"));
                begin_close(std::nullopt);
            }));
    }

    ~observed_socket() override {
        g_observers_at_destruction = container.numObservers();
        g_sockets_destroyed++;
    }

    /* closeImpl up to the connection callback: observers told, connection
     * marked closed, the close reflected onto every read callback (each
     * removed as it is called). */
    void begin_close(const quic::Optional<quic::QuicError> &reason) {
        quic::SocketObserverInterface::CloseStartedEvent ev;
        ev.maybeCloseReason = reason;
        container.invokeInterfaceMethodAllObservers(
            [&](auto observer, auto observed) {
                observer->closeStarted(observed, ev);
            });
        closed = true;
        reflect(reason ? *reason
                       : quic::QuicError(quic::QuicErrorCode(
                                             quic::LocalErrorCode::NO_ERROR),
                                         "no error"));
    }

    void reflect(const quic::QuicError &err) {
        auto copy = read_cbs;
        for (auto &kv : copy) {
            auto it = read_cbs.find(kv.first);
            if (it == read_cbs.end()) continue;
            auto *cb = it->second;
            read_cbs.erase(it);
            cb->readError(kv.first, err);
        }
    }

    /* A peer CONNECTION_CLOSE, in two halves so a row can look between
     * them: the close up to the connection callback, then the callback. */
    void peer_close_begin(const quic::QuicError &err) { begin_close(err); }
    void peer_close_finish(bool clean, const quic::QuicError &err) {
        if (!conn_cb) return;
        if (clean) conn_cb->onConnectionEnd();
        else conn_cb->onConnectionError(err);
    }

    quic::SocketObserverContainer container;
    quic::QuicSocket::ConnectionSetupCallback *setup_cb = nullptr;
    quic::QuicSocket::ConnectionCallback *conn_cb = nullptr;
    std::map<quic::StreamId, quic::QuicSocket::ReadCallback *> read_cbs;
    std::map<quic::StreamId, std::vector<uint8_t>> inbound;
    std::map<quic::StreamId, std::vector<uint8_t>> outbound;
    bool closed = false;
    int setup_cb_sets = 0, conn_cb_sets = 0, dgram_cb_sets = 0;
    int read_cb_sets = 0, closes = 0, close_nows = 0, graceful_closes = 0;
    int writes = 0, writes_after_close = 0;
    uint64_t close_code = UINT64_MAX;
};

struct terminal {
    int closed_events = 0;
    uint64_t code = UINT64_MAX;
    std::string reason;
};

static terminal drain(moq_session_t *s)
{
    terminal t;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            t.closed_events++;
            t.code = ev.u.closed.code;
            t.reason.assign(reinterpret_cast<const char *>(
                                ev.u.closed.reason.data),
                            ev.u.closed.reason.len);
        }
        moq_event_cleanup(&ev);
    }
    return t;
}

static moq_session_t *new_session(moq_perspective_t persp, moq_version_t v)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = v;
    if (persp == MOQ_PERSPECTIVE_SERVER) {
        /* A server grants request IDs in its setup, so a draft-16 client
         * can queue a request. */
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
    }
    moq_session_t *s = nullptr;
    if (moq_session_create(&cfg, 0, &s) != MOQ_OK) return nullptr;
    if (test_bind_if_required(s) != MOQ_OK) {
        moq_session_destroy(s);
        return nullptr;
    }
    return s;
}

struct rig {
    moq_version_t version = MOQ_VERSION_DRAFT_16;
    std::shared_ptr<observed_socket> sock;
    moq_session_t *session = nullptr;
    std::unique_ptr<moq::mvfst::adapter> adp;
    quic::StreamId peer_ctrl = 3;      /* drafts 18/22: the peer's control uni */
};

/* The peer's control-stream bytes for a uni-control draft, as a real server
 * session of that draft opens its control stream. */
static std::vector<uint8_t> peer_uni_control_bytes(moq_version_t v)
{
    std::vector<uint8_t> out;
    moq_session_t *srv = new_session(MOQ_PERSPECTIVE_SERVER, v);
    if (!srv) return out;
    if (moq_session_start(srv, 0) == MOQ_OK) {
        moq_action_t a;
        while (moq_session_poll_actions(srv, &a, 1) == 1) {
            if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL)
                out.insert(out.end(), a.u.open_uni_control.data,
                           a.u.open_uni_control.data +
                               a.u.open_uni_control.len);
            else if (a.kind == MOQ_ACTION_SEND_UNI_CONTROL)
                out.insert(out.end(), a.u.send_uni_control.data,
                           a.u.send_uni_control.data +
                               a.u.send_uni_control.len);
            moq_action_cleanup(&a);
        }
    }
    moq_session_destroy(srv);
    return out;
}

/* Draft-16: the server's answer to the client's control bytes, as a real
 * server session of that draft writes it. */
static std::vector<uint8_t> d16_server_answer(const std::vector<uint8_t> &in)
{
    std::vector<uint8_t> out;
    moq_session_t *srv = new_session(MOQ_PERSPECTIVE_SERVER,
                                     MOQ_VERSION_DRAFT_16);
    if (!srv) return out;
    if (!in.empty() &&
        moq_session_on_control_bytes(srv, in.data(), in.size(), 0) >= 0) {
        moq_action_t a;
        while (moq_session_poll_actions(srv, &a, 1) == 1) {
            if (a.kind == MOQ_ACTION_SEND_CONTROL)
                out.insert(out.end(), a.u.send_control.data,
                           a.u.send_control.data + a.u.send_control.len);
            moq_action_cleanup(&a);
        }
    }
    moq_session_destroy(srv);
    return out;
}

/* An established, attached client of `v` whose control stream has a read
 * callback: stream 0 on draft-16, after the server's real answer to its
 * setup; on drafts 18/22 the peer's control uni, opened and fed the peer's
 * real control bytes, so the bridge knows it as control. */
static bool rig_up(rig &r, moq_version_t v = MOQ_VERSION_DRAFT_16)
{
    r.version = v;
    r.sock = std::make_shared<observed_socket>(true);
    r.session = new_session(MOQ_PERSPECTIVE_CLIENT, v);
    if (!r.session) return false;
    r.adp = std::make_unique<moq::mvfst::adapter>(
        moq::mvfst::adapter::config::client(), r.session, r.sock);
    if (moq_session_start(r.session, 0) != MOQ_OK) return false;
    (void)r.adp->service(0);
    if (r.sock->conn_cb == nullptr || r.sock->container.numObservers() != 1)
        return false;
    if (v == MOQ_VERSION_DRAFT_16) {
        if (r.sock->read_cbs.count(0) != 1) return false;
        r.sock->inbound[0] = d16_server_answer(r.sock->outbound[0]);
        if (r.sock->inbound[0].empty()) return false;
        r.sock->read_cbs[0]->readAvailable(0);
        (void)r.adp->service(0);
        (void)drain(r.session);
        return moq_session_state(r.session) == MOQ_SESS_ESTABLISHED &&
               r.sock->read_cbs.count(0) == 1;
    }
    r.sock->inbound[r.peer_ctrl] = peer_uni_control_bytes(v);
    if (r.sock->inbound[r.peer_ctrl].empty()) return false;
    r.sock->conn_cb->onNewUnidirectionalStream(r.peer_ctrl);
    if (r.sock->read_cbs.count(r.peer_ctrl) != 1) return false;
    r.sock->read_cbs[r.peer_ctrl]->readAvailable(r.peer_ctrl);
    (void)r.adp->service(0);
    (void)drain(r.session);
    return moq_session_state(r.session) == MOQ_SESS_ESTABLISHED &&
           r.sock->read_cbs.count(r.peer_ctrl) == 1;
}

static void rig_down(rig &r)
{
    r.adp.reset();
    r.sock.reset();
    if (r.session) moq_session_destroy(r.session);
    r.session = nullptr;
}

static quic::QuicError app_error(uint64_t code)
{
    return quic::QuicError(quic::ApplicationErrorCode(code));
}

static const char *draft_name(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_22 ? "draft-22"
         : v == MOQ_VERSION_DRAFT_18 ? "draft-18" : "draft-16";
}

/* 1. The peer resets the control stream, no close begun. */
static void t_isolated_control_reset(uint64_t peer_code)
{
    int before = failures;
    rig r;
    MVFST_CHECK(rig_up(r));
    if (r.sock && r.sock->read_cbs.count(0)) {
        r.sock->read_cbs[0]->readError(0, app_error(peer_code));
        (void)r.adp->service(0);
        terminal t = drain(r.session);
        MVFST_CHECK(t.closed_events == 1);
        MVFST_CHECK(t.code == 0x3);
        MVFST_CHECK(r.adp->is_closed());
        MVFST_CHECK(!r.adp->is_fatal());
        /* the adapter's own close of the socket, in the provider's order */
        MVFST_CHECK(r.sock->closes == 1);
        MVFST_CHECK(r.sock->close_code == 0x3);
        (void)r.adp->service(0);
        MVFST_CHECK(drain(r.session).closed_events == 0);
        MVFST_CHECK(r.sock->closes == 1);
    }
    rig_down(r);
    if (failures == before)
        std::printf("PASS: isolated control reset, application code 0x%llx\n",
                    (unsigned long long)peer_code);
}

/* 2. A peer close, clean or with an error, on each draft; optionally with
 * control output pending when it arrives. */
static void t_peer_close(moq_version_t v, bool clean, bool pending_output)
{
    int before = failures;
    const uint64_t code = clean ? 0 : 7;
    rig r;
    MVFST_CHECK(rig_up(r, v));
    if (r.sock && r.sock->conn_cb) {
        if (pending_output) {
            /* a request queued and not yet serviced: output the adapter
             * would write on its next pass */
            moq_subscribe_cfg_t sc;
            moq_subscribe_cfg_init(&sc);
            moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("close") };
            sc.track_namespace = (moq_namespace_t){ ns, 1 };
            sc.track_name = MOQ_BYTES_LITERAL("t");
            sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
            moq_subscription_t sub;
            MVFST_CHECK(moq_session_subscribe(r.session, &sc, 0, &sub) == MOQ_OK);
        }
        const int writes_before = r.sock->writes;
        r.sock->peer_close_begin(app_error(code));
        /* the reflected errors decided nothing and asked nothing */
        MVFST_CHECK(drain(r.session).closed_events == 0);
        MVFST_CHECK(!r.adp->is_closed());
        MVFST_CHECK(!r.adp->is_fatal());
        MVFST_CHECK(r.sock->writes_after_close == 0);
        MVFST_CHECK(r.sock->writes == writes_before);
        MVFST_CHECK(r.sock->closes == 0);

        r.sock->peer_close_finish(clean, app_error(code));
        (void)r.adp->service(0);
        terminal t = drain(r.session);
        MVFST_CHECK(t.closed_events == 1);
        MVFST_CHECK(t.code == code);
        if (clean) {
            MVFST_CHECK(r.adp->is_closed());
            MVFST_CHECK(!r.adp->is_fatal());
        } else {
            MVFST_CHECK(r.adp->is_fatal());
            MVFST_CHECK(r.adp->fatal_code() == code);
        }
        MVFST_CHECK(r.sock->writes_after_close == 0);
        MVFST_CHECK(r.sock->closes == 0);
    }
    rig_down(r);
    if (failures == before)
        std::printf("PASS: %s peer close, %s%s\n", draft_name(v),
                    clean ? "clean" : "with an error",
                    pending_output ? ", output pending" : "");
}

/* 3. The session closes first. Either the adapter closes the socket for it,
 * in the provider's order (no connection callback), or a peer reset of the
 * control stream arrives while that close is still queued. */
static void t_session_close_first(bool reset_while_queued)
{
    int before = failures;
    rig r;
    MVFST_CHECK(rig_up(r));
    if (r.sock && r.sock->read_cbs.count(0)) {
        MVFST_CHECK(moq_session_close(r.session, 0x2, "first", 0) == MOQ_OK);
        if (reset_while_queued)
            r.sock->read_cbs[0]->readError(0, app_error(0x55));
        (void)r.adp->service(0);
        MVFST_CHECK(r.sock->closes == 1);
        MVFST_CHECK(r.sock->close_code == 0x2);
        MVFST_CHECK(r.sock->conn_cb == nullptr);   /* removed by close() */
        (void)r.adp->service(0);
        terminal t = drain(r.session);
        MVFST_CHECK(t.closed_events == 1);
        MVFST_CHECK(t.code == 0x2);
        MVFST_CHECK(t.reason == "first");
        MVFST_CHECK(r.adp->is_closed());
        MVFST_CHECK(!r.adp->is_fatal());
        MVFST_CHECK(r.sock->closes == 1);
        MVFST_CHECK(r.sock->writes_after_close == 0);
    }
    rig_down(r);
    if (failures == before)
        std::printf("PASS: session close first%s\n",
                    reset_while_queued ? ", peer reset while it is queued"
                                       : "");
}

/* 4. The caller closes the socket itself. */
static void t_caller_close(bool graceful, bool session_closed_first)
{
    int before = failures;
    rig r;
    MVFST_CHECK(rig_up(r));
    if (r.sock && r.sock->conn_cb) {
        if (session_closed_first)
            MVFST_CHECK(moq_session_close(r.session, 0x2, "first", 0) == MOQ_OK);
        if (graceful) r.sock->closeGracefully();
        else r.sock->close(app_error(0x9));
        MVFST_CHECK(r.sock->conn_cb == nullptr);   /* the provider removed it */
        MVFST_CHECK(r.sock->writes_after_close == 0);
        (void)r.adp->service(0);
        terminal t = drain(r.session);
        MVFST_CHECK(t.closed_events == 1);
        if (session_closed_first) {
            MVFST_CHECK(t.code == 0x2);
            MVFST_CHECK(t.reason == "first");
        } else {
            MVFST_CHECK(t.code == (graceful ? 0u : 0x9u));
        }
        MVFST_CHECK(r.adp->is_closed());
        MVFST_CHECK(!r.adp->is_fatal());
        /* only the caller's own close reached the socket */
        MVFST_CHECK(r.sock->closes == (graceful ? 0 : 1));
        MVFST_CHECK(r.sock->writes_after_close == 0);
        (void)r.adp->service(0);
        MVFST_CHECK(drain(r.session).closed_events == 0);
    }
    rig_down(r);
    if (failures == before)
        std::printf("PASS: caller %s%s\n",
                    graceful ? "closeGracefully()" : "close()",
                    session_closed_first ? " after the session's own close"
                                         : "");
}

/* 4b. The observer's close notice keeps only what settlement needs --
 * whether the close carried an application error, and its code -- and
 * allocates nothing doing so: a close reason whose message is far longer
 * than any small-string buffer reaches the observer's closeStarted while
 * every scalar operator new fails, and the code still reaches the session
 * when the caller's close is settled.
 *
 * Scope: the observer body only. The reason, the event and the list of
 * attached observers are all built before the failure window, and the
 * observer is called directly through its interface. Folly's own dispatch
 * (which wraps the callable in folly::Function and may allocate) and the
 * managed wake scheduler are outside the window and not claimed here. */
static void t_close_notice_allocates_nothing()
{
    int before = failures;
    rig r;
    MVFST_CHECK(rig_up(r));
    if (r.sock && r.sock->conn_cb) {
        quic::SocketObserverInterface::CloseStartedEvent ev;
        ev.maybeCloseReason = quic::QuicError(
            quic::QuicErrorCode(quic::ApplicationErrorCode(7)),
            std::string(256, 'x'));
        /* the adapter's observer, found before the window */
        std::vector<quic::SocketObserverContainer::Observer *> observers =
            r.sock->container.getObservers();
        MVFST_CHECK(observers.size() == 1);
        r.sock->conn_cb = nullptr;          /* as close() does first */
        if (observers.size() == 1) {
            quic::SocketObserverInterface *obs = observers[0];
            quic::QuicSocketLite *observed = r.sock.get();
            g_fail_alloc = true;
            obs->closeStarted(observed, ev);
            g_fail_alloc = false;
        }
        (void)r.adp->service(0);
        terminal t = drain(r.session);
        MVFST_CHECK(t.closed_events == 1);
        MVFST_CHECK(t.code == 7);
        MVFST_CHECK(r.adp->is_closed());
        MVFST_CHECK(!r.adp->is_fatal());
    }
    g_fail_alloc = false;
    rig_down(r);
    if (failures == before)
        std::printf("PASS: close notice allocates nothing\n");
}

/* 5. A socket with no observer container is refused untouched. */
static void t_refused_without_observer()
{
    int before = failures;
    auto sock = std::make_shared<observed_socket>(false);
    moq_session_t *s = new_session(MOQ_PERSPECTIVE_CLIENT,
                                   MOQ_VERSION_DRAFT_16);
    MVFST_CHECK(s != nullptr);
    bool refused = false, other = false;
    moq_result_t code = MOQ_OK;
    if (s) {
        try {
            moq::mvfst::adapter a(moq::mvfst::adapter::config::client(), s,
                                  sock);
        } catch (const moq::mvfst::unsupported_socket &e) {
            refused = true;
            code = e.code();
        } catch (...) {
            other = true;
        }
    }
    MVFST_CHECK(refused);
    MVFST_CHECK(!other);
    MVFST_CHECK(code == MOQ_ERR_UNSUPPORTED);
    MVFST_CHECK(sock->setup_cb_sets == 0);
    MVFST_CHECK(sock->conn_cb_sets == 0);
    MVFST_CHECK(sock->dgram_cb_sets == 0);
    MVFST_CHECK(sock->read_cb_sets == 0);
    MVFST_CHECK(sock->closes == 0);
    MVFST_CHECK(sock->close_nows == 0);
    MVFST_CHECK(sock->graceful_closes == 0);
    MVFST_CHECK(sock.use_count() == 1);          /* nothing kept a reference */
    MVFST_CHECK(sock->container.numObservers() == 0);
    if (s) moq_session_destroy(s);
    sock.reset();
    if (failures == before)
        std::printf("PASS: socket without an observer container refused\n");
}

/* 6. Destruction in both orders. */
static void t_destruction(bool adapter_first)
{
    int before = failures;
    rig r;
    MVFST_CHECK(rig_up(r));
    g_observers_at_destruction = SIZE_MAX;
    int destroyed_before = g_sockets_destroyed;
    if (adapter_first) {
        /* the caller keeps its socket and destroys the adapter */
        r.adp.reset();
        MVFST_CHECK(r.sock->container.numObservers() == 0);
        MVFST_CHECK(r.sock->conn_cb == nullptr);
        MVFST_CHECK(r.sock->setup_cb == nullptr);
        r.sock->peer_close_begin(app_error(0));   /* reaches no observer */
        MVFST_CHECK(g_sockets_destroyed == destroyed_before);
        r.sock.reset();
    } else {
        /* the caller lets go of its socket first; the adapter holds the
         * last reference and is destroyed after */
        std::weak_ptr<observed_socket> w = r.sock;
        r.sock.reset();
        MVFST_CHECK(!w.expired());
        r.adp.reset();
        MVFST_CHECK(w.expired());
    }
    MVFST_CHECK(g_sockets_destroyed == destroyed_before + 1);
    MVFST_CHECK(g_observers_at_destruction == 0);
    rig_down(r);
    if (failures == before)
        std::printf("PASS: destruction, %s first\n",
                    adapter_first ? "adapter" : "caller's socket reference");
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleMock(&argc, argv);
    t_isolated_control_reset(0);
    t_isolated_control_reset(0x55);
    for (moq_version_t v : { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                             MOQ_VERSION_DRAFT_22 }) {
        t_peer_close(v, true, false);
        t_peer_close(v, false, false);
        t_peer_close(v, true, true);
    }
    t_session_close_first(false);
    t_session_close_first(true);
    t_caller_close(false, false);
    t_caller_close(true, false);
    t_caller_close(false, true);
    t_caller_close(true, true);
    t_close_notice_allocates_nothing();
    t_refused_without_observer();
    t_destruction(true);
    t_destruction(false);
    if (failures == 0) std::printf("PASS: mvfst_close_provenance\n");
    else std::fprintf(stderr, "FAIL: mvfst_close_provenance (%d)\n", failures);
    return failures ? 1 : 0;
}
