#ifndef MOQ_FAKE_WEBTRANSPORT_H
#define MOQ_FAKE_WEBTRANSPORT_H

/*
 * Minimal fake proxygen::WebTransport for testing wt_endpoint_ops
 * and the adapter read loop.
 *
 * Records outbound calls, allows configurable return values, and
 * supports queued read data per stream for deterministic read-loop
 * testing.
 *
 * Not installed. Test infrastructure only.
 */

#include <proxygen/lib/http/webtransport/WebTransport.h>

#include <folly/Expected.h>
#include <folly/futures/Future.h>
#include <folly/io/IOBuf.h>

#include <deque>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace moq::wt::testing {

using WT = proxygen::WebTransport;

class FakeWriteHandle : public WT::StreamWriteHandle {
 public:
    explicit FakeWriteHandle(uint64_t id) : StreamWriteHandle(id) {}

    folly::Expected<WT::FCState, WT::ErrorCode> writeStreamData(
        std::unique_ptr<folly::IOBuf>, bool, WT::ByteEventCallback*) override {
        return WT::FCState::UNBLOCKED;
    }

    folly::Expected<folly::Unit, WT::ErrorCode> resetStream(uint32_t) override {
        return folly::unit;
    }

    folly::Expected<folly::SemiFuture<uint64_t>, WT::ErrorCode>
    awaitWritable() override {
        return folly::makeSemiFuture(uint64_t(0));
    }
};

class FakeReadHandle : public WT::StreamReadHandle {
 public:
    explicit FakeReadHandle(uint64_t id) : StreamReadHandle(id) {}

    folly::SemiFuture<WT::StreamData> readStreamData() override {
        return folly::makeSemiFuture(WT::StreamData{});
    }

    folly::Expected<folly::Unit, WT::ErrorCode> stopSending(uint32_t) override {
        return folly::unit;
    }
};

struct RecordedWrite {
    uint64_t stream_id;
    std::unique_ptr<folly::IOBuf> data;
    size_t data_len;
    bool fin;
};

struct RecordedReset {
    uint64_t stream_id;
    uint32_t error;
};

struct RecordedStop {
    uint64_t stream_id;
    uint32_t error;
};

struct RecordedClose {
    folly::Optional<uint32_t> error;
};

/* -- Queued read entry for deterministic read-loop tests ------------- */

struct QueuedRead {
    std::unique_ptr<folly::IOBuf> data;
    bool fin = false;
    bool is_exception = false;
    uint32_t exception_code = 0;
};

class FakeWebTransport : public proxygen::WebTransport {
 public:
    /* -- Config: what the next call returns -- */
    folly::Optional<ErrorCode> next_create_uni_error;
    folly::Optional<ErrorCode> next_create_bidi_error;
    folly::Optional<ErrorCode> next_write_error;
    FCState next_write_fc_state = FCState::UNBLOCKED;
    folly::Optional<ErrorCode> next_reset_error;
    folly::Optional<ErrorCode> next_stop_error;
    folly::Optional<ErrorCode> next_datagram_error;
    folly::Optional<ErrorCode> next_close_error;
    bool throw_on_next_call = false;
    bool block_all_writes = false;
    bool drop_all_datagrams = false;

    /* -- Read queue config -- */
    folly::Optional<ErrorCode> next_read_error;
    bool throw_on_next_read = false;
    std::unordered_map<uint64_t, std::deque<QueuedRead>> read_queues;
    int read_call_count = 0;

    /* -- Recorded calls -- */
    std::vector<RecordedWrite> writes;
    std::vector<RecordedReset> resets;
    std::vector<RecordedStop> stops;
    std::vector<RecordedClose> closes;
    struct RecordedDatagram {
        std::unique_ptr<folly::IOBuf> data;
        size_t len;
    };
    std::vector<RecordedDatagram> datagrams;
    int create_uni_count = 0;
    int create_bidi_count = 0;

    /* -- Stream ID generation and type tracking -- */
    uint64_t next_stream_id = 100;
    std::unordered_set<uint64_t> uni_stream_ids;
    std::unordered_set<uint64_t> bidi_stream_ids;

    /* -- Helper: queue read data for a stream -- */

    void queueRead(uint64_t id, const void *data, size_t len, bool fin) {
        QueuedRead qr;
        if (data && len > 0)
            qr.data = folly::IOBuf::copyBuffer(data, len);
        else if (data)
            qr.data = folly::IOBuf::create(0);
        qr.fin = fin;
        read_queues[id].push_back(std::move(qr));
    }

    void queueReadException(uint64_t id, uint32_t error_code) {
        QueuedRead qr;
        qr.is_exception = true;
        qr.exception_code = error_code;
        read_queues[id].push_back(std::move(qr));
    }

    /* -- Outbound stream creation -- */

    folly::Expected<StreamWriteHandle*, ErrorCode> createUniStream() override {
        if (throw_on_next_call) {
            throw_on_next_call = false;
            throw std::runtime_error("fake exception");
        }
        if (next_create_uni_error) {
            auto ec = *next_create_uni_error;
            next_create_uni_error.reset();
            return folly::makeUnexpected(ec);
        }
        create_uni_count++;
        auto id = next_stream_id++;
        uni_stream_ids.insert(id);
        write_handles_.push_back(std::make_unique<FakeWriteHandle>(id));
        return write_handles_.back().get();
    }

    folly::Expected<BidiStreamHandle, ErrorCode> createBidiStream() override {
        if (throw_on_next_call) {
            throw_on_next_call = false;
            throw std::runtime_error("fake exception");
        }
        if (next_create_bidi_error) {
            auto ec = *next_create_bidi_error;
            next_create_bidi_error.reset();
            return folly::makeUnexpected(ec);
        }
        create_bidi_count++;
        auto id = next_stream_id++;
        bidi_stream_ids.insert(id);
        write_handles_.push_back(std::make_unique<FakeWriteHandle>(id));
        read_handles_.push_back(std::make_unique<FakeReadHandle>(id));
        return BidiStreamHandle{
            read_handles_.back().get(), write_handles_.back().get()};
    }

    /* -- Stream-ID-based API (used by endpoint ops + read loop) ------- */

    folly::Expected<folly::SemiFuture<StreamData>, ErrorCode>
    readStreamData(uint64_t id) override {
        read_call_count++;
        if (throw_on_next_read) {
            throw_on_next_read = false;
            throw std::runtime_error("fake read exception");
        }
        if (next_read_error) {
            auto ec = *next_read_error;
            next_read_error.reset();
            return folly::makeUnexpected(ec);
        }
        auto it = read_queues.find(id);
        if (it == read_queues.end() || it->second.empty()) {
            return folly::makeSemiFuture(StreamData{});
        }
        auto &qr = it->second.front();
        if (qr.is_exception) {
            auto code = qr.exception_code;
            it->second.pop_front();
            return folly::makeSemiFuture<StreamData>(
                folly::make_exception_wrapper<Exception>(code));
        }
        StreamData sd;
        sd.data = std::move(qr.data);
        sd.fin = qr.fin;
        it->second.pop_front();
        return folly::makeSemiFuture(std::move(sd));
    }

    folly::Expected<FCState, ErrorCode> writeStreamData(
        uint64_t id,
        std::unique_ptr<folly::IOBuf> data,
        bool fin,
        ByteEventCallback*) override {
        if (throw_on_next_call) {
            throw_on_next_call = false;
            throw std::runtime_error("fake exception");
        }
        if (block_all_writes)
            return folly::makeUnexpected(ErrorCode::BLOCKED);
        if (next_write_error) {
            auto ec = *next_write_error;
            next_write_error.reset();
            return folly::makeUnexpected(ec);
        }
        size_t len = data ? data->computeChainDataLength() : 0;
        writes.push_back({id, data ? data->clone() : nullptr, len, fin});
        auto fc = next_write_fc_state;
        next_write_fc_state = FCState::UNBLOCKED;
        return fc;
    }

    folly::Expected<folly::Unit, ErrorCode> resetStream(
        uint64_t streamId, uint32_t error) override {
        if (throw_on_next_call) {
            throw_on_next_call = false;
            throw std::runtime_error("fake exception");
        }
        if (next_reset_error) {
            auto ec = *next_reset_error;
            next_reset_error.reset();
            return folly::makeUnexpected(ec);
        }
        resets.push_back({streamId, error});
        return folly::unit;
    }

    folly::Expected<folly::Unit, ErrorCode> stopSending(
        uint64_t streamId, uint32_t error) override {
        if (throw_on_next_call) {
            throw_on_next_call = false;
            throw std::runtime_error("fake exception");
        }
        if (next_stop_error) {
            auto ec = *next_stop_error;
            next_stop_error.reset();
            return folly::makeUnexpected(ec);
        }
        stops.push_back({streamId, error});
        return folly::unit;
    }

    folly::Expected<folly::Unit, ErrorCode> sendDatagram(
        std::unique_ptr<folly::IOBuf> datagram) override {
        if (throw_on_next_call) {
            throw_on_next_call = false;
            throw std::runtime_error("fake exception");
        }
        if (drop_all_datagrams)
            return folly::makeUnexpected(ErrorCode::BLOCKED);
        if (next_datagram_error) {
            auto ec = *next_datagram_error;
            next_datagram_error.reset();
            return folly::makeUnexpected(ec);
        }
        size_t len = datagram ? datagram->computeChainDataLength() : 0;
        datagrams.push_back({datagram ? datagram->cloneCoalesced()
                                      : nullptr, len});
        return folly::unit;
    }

    folly::Expected<folly::Unit, ErrorCode> closeSession(
        folly::Optional<uint32_t> error) override {
        if (throw_on_next_call) {
            throw_on_next_call = false;
            throw std::runtime_error("fake exception");
        }
        if (next_close_error) {
            auto ec = *next_close_error;
            next_close_error.reset();
            return folly::makeUnexpected(ec);
        }
        closes.push_back({error});
        return folly::unit;
    }

    /* -- Stubs for unused pure virtuals -- */

    folly::SemiFuture<folly::Unit> awaitUniStreamCredit() override {
        return folly::makeSemiFuture();
    }

    folly::SemiFuture<folly::Unit> awaitBidiStreamCredit() override {
        return folly::makeSemiFuture();
    }

    folly::Expected<folly::Unit, ErrorCode> setPriority(
        uint64_t, quic::PriorityQueue::Priority) override {
        return folly::unit;
    }

    folly::Expected<folly::Unit, ErrorCode> setPriorityQueue(
        std::unique_ptr<quic::PriorityQueue>) noexcept override {
        return folly::unit;
    }

    folly::Expected<folly::SemiFuture<uint64_t>, ErrorCode> awaitWritable(
        uint64_t) override {
        return folly::makeSemiFuture(uint64_t(0));
    }

    const folly::SocketAddress& getLocalAddress() const override {
        return local_addr_;
    }

    const folly::SocketAddress& getPeerAddress() const override {
        return peer_addr_;
    }

    quic::TransportInfo getTransportInfo() const override {
        return quic::TransportInfo();
    }

 private:
    std::vector<std::unique_ptr<FakeWriteHandle>> write_handles_;
    std::vector<std::unique_ptr<FakeReadHandle>> read_handles_;
    folly::SocketAddress local_addr_;
    folly::SocketAddress peer_addr_;
};

} // namespace moq::wt::testing

#endif // MOQ_FAKE_WEBTRANSPORT_H
