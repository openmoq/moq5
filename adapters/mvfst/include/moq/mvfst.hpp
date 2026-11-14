#ifndef MOQ_MVFST_HPP
#define MOQ_MVFST_HPP

/*
 * Stability: pre-1.0, transport-specific adapter API. Mature enough for
 * integration pilots, but may change before 1.0.
 *
 * moq-adapter-mvfst: C++ attach API.
 *
 * Native C++ adapter that bridges an existing quic::QuicSocket to a
 * moq_session_t. The caller owns both the session and the socket;
 * the adapter registers mvfst callbacks and translates between the
 * libmoq action/input model and mvfst stream/datagram operations.
 *
 * C++ users get direct mvfst access with no C ABI indirection.
 * The adapter still calls the libmoq C core (moq_session_*) directly.
 *
 * See <moq/mvfst.h> for the managed-mode C API (adapter owns
 * transport, session, and network thread).
 *
 * Packaging: installed as CMake component adapter-mvfst
 *   (find_package(libmoq COMPONENTS adapter-mvfst); link
 *   moq::adapter-mvfst). CMake-only — no pkg-config (.pc). Consuming this
 *   attach header additionally requires mvfst's own headers (for
 *   quic::QuicSocket) and a C++ toolchain. See <moq/mvfst.h> packaging
 *   note and docs/transport-integration-guide.md §9.
 */

#include <moq/mvfst.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>

namespace quic {
class QuicSocket;
}

namespace moq::mvfst {

/*
 * Attach adapter: bridges an existing QuicSocket to an existing
 * moq_session_t. The caller owns both; the adapter installs mvfst
 * callbacks and owns the bridge that translates between actions
 * and QuicSocket operations.
 *
 * Callback ownership: the adapter installs itself as the socket's
 * ConnectionSetupCallback, ConnectionCallback, ReadCallback, and
 * DatagramCallback. Those callback slots are owned by the adapter
 * for its lifetime. The caller must not replace them while the
 * adapter is alive. The adapter uninstalls all callbacks on
 * destruction to prevent use-after-free if the socket outlives
 * the adapter.
 *
 * Thread confinement: the adapter, moq_session_t, and QuicSocket
 * must all be used from the same EventBase thread. This includes
 * construction, destruction, service(), and any session API calls.
 *
 * Zero-copy send: IOBuf::takeOwnership wraps moq_rcbuf_t payloads
 * so the transport reads directly from session-owned buffers.
 *
 * Receive: inbound uni-stream IOBufs are wrapped in moq_rcbuf_t
 * and fed to the session via on_data_rcbuf, preserving transport-
 * buffer lifetime. This is zero-copy when mvfst supplies a single
 * contiguous IOBuf. Chained IOBufs are coalesced (copied) before
 * wrapping; chain-aware receive is a future optimization.
 * Control/bidi/datagram receive always use the copy path.
 *
 * Close provenance: the socket must accept a socket observer
 * (quic::QuicSocket::addObserver returns true). mvfst's full transports,
 * quic::QuicClientTransport and quic::QuicServerTransport, do; the Lite
 * transports have no observer container and do not. The adapter registers
 * one observer, before it keeps the socket or installs any callback, to
 * learn when the connection's close begins: a read error on the MoQ control
 * stream before that notice is a peer RESET_STREAM and ends the control
 * stream (a protocol violation while the session is open); one after it is
 * the connection's close and decides nothing. While a close is in progress
 * the adapter asks nothing of the socket: no output, replay or resumed reads.
 *
 * Closing the socket yourself: after closeNow(), as after a peer or transport
 * close, the connection callback reports the close to the session. close()
 * and closeGracefully() remove the adapter's connection callbacks first, so
 * the adapter reports those itself, as an orderly close: closeGracefully()
 * when mvfst reflects its LocalErrorCode::NO_ERROR onto the streams, and
 * otherwise -- close(), or closeGracefully() with no stream to reflect onto
 * -- at the next service(), with the application error code the close
 * carried (0 if none). A close the session had already decided keeps its own
 * event.
 *
 * A socket without an observer
 * container is refused: the constructor throws unsupported_socket, whose
 * code() is MOQ_ERR_UNSUPPORTED, and leaves the socket as it was -- no
 * callback installed, no observer left registered, nothing sent, not
 * closed. The observer is detached when the adapter is destroyed.
 *
 * Peer STOP_SENDING on the MoQ control stream ends that stream (a protocol
 * violation while the session is open). Uni-stream STOP_SENDING is handled
 * via moq_session_on_data_stop.
 *
 * moq_rcbuf_t refcounts are intentionally non-atomic, so all
 * incref/decref must be confined to a single thread.
 *
 * Transport binding: a session whose wire profile requires one
 * (moq_session_binding_requirements() != 0, e.g. draft-22) must be bound
 * by the attaching caller with moq_session_bind_transport() before
 * moq_session_start(): kind MOQ_BINDING_RAW_QUIC, the handshake-complete
 * fact once the socket reports the peer's transport parameters
 * (getPeerTransportParams()), QUIC DATAGRAM support from the local
 * transport settings (receive) and the peer's max_datagram_frame_size
 * (send), and the connection target -- the client's authority and
 * path/query, the server's accept_uri policy. A profile without
 * requirements (draft-16, draft-18) accepts the same binding optionally,
 * with no facts needed: a client connecting to a moqt URI binds its target
 * before moq_session_start() so its setup names it. The adapter itself never
 * binds or starts the session; the managed facade in <moq/mvfst.h> does.
 */
/*
 * Thrown by the adapter constructor for a socket that cannot report the start
 * of its close (see "Close provenance" above). code() is MOQ_ERR_UNSUPPORTED.
 * The destructor is defined in the library, so the class's vtable and type
 * information are emitted there once and exported with it.
 */
class MOQ_API unsupported_socket : public std::runtime_error {
public:
    unsupported_socket()
        : std::runtime_error(
              "moq::mvfst::adapter: the socket cannot report the start of "
              "its close (no socket observer container)") {}
    ~unsupported_socket() override;
    moq_result_t code() const noexcept { return MOQ_ERR_UNSUPPORTED; }
};

class MOQ_API adapter {
public:
    /*
     * local_perspective must match the session's perspective.
     * Use the static factories to construct — no default perspective.
     */
    struct config {
        moq_perspective_t local_perspective;

        static config client() noexcept {
            return config{MOQ_PERSPECTIVE_CLIENT};
        }
        static config server() noexcept {
            return config{MOQ_PERSPECTIVE_SERVER};
        }

    private:
        explicit config(moq_perspective_t p) noexcept
            : local_perspective(p) {}
        friend class adapter;
    };

    adapter(config cfg,
            moq_session_t *session,
            std::shared_ptr<quic::QuicSocket> socket);
    ~adapter();

    adapter(const adapter&) = delete;
    adapter& operator=(const adapter&) = delete;

    moq_session_t *raw_session() const noexcept;
    quic::QuicSocket &socket() const noexcept;

    moq_result_t service(uint64_t now_us);

    bool is_fatal() const noexcept;
    bool is_closed() const noexcept;
    uint64_t fatal_code() const noexcept;
    /*
     * Reports bridge-retained pending work (outbound backpressure
     * retries and inbound pending state). Callers must still call
     * service() after any moq_session_t API that may queue actions.
     */
    bool has_pending() const noexcept;

    /*
     * Runs `fn` on the EventBase thread when the socket reports its
     * handshake complete (onTransportReady), before the adapter services
     * the session or delivers any inbound stream. This is where an attaching
     * caller binds a session whose profile requires a transport binding
     * (moq_session_bind_transport) and starts it: the peer's transport
     * parameters are known here and no byte has reached the session yet.
     * At most one hook; a later call replaces it. Not invoked if the
     * handshake already completed before it was set.
     */
    void on_transport_ready(std::function<void()> fn);

private:
    struct impl;
    std::unique_ptr<impl> impl_;

    /* The managed C facade (mvfst_adapter.cpp) reaches impl to enable
     * managed-only outbound stream-credit gating and wire the credit-wake
     * to its pump. This adds no public callable surface; attach-mode
     * consumers use only the methods above. */
    friend struct ::moq_mvfst_managed;
};

} // namespace moq::mvfst

#endif // MOQ_MVFST_HPP
