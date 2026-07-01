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
#include <memory>

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
 * v1 limitation: bidi STOP_SENDING from the peer is ignored.
 * Uni-stream STOP_SENDING is handled via moq_session_on_data_stop.
 *
 * moq_rcbuf_t refcounts are intentionally non-atomic, so all
 * incref/decref must be confined to a single thread.
 */
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
