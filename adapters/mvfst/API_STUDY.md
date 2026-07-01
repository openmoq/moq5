# mvfst API Study

Findings from inspecting the installed mvfst headers at
`local-prefix/mvfst/include/quic/api/` and cross-checked against
homebrew mvfst 2026.05.18.00 (headers are identical).

## 1. Which QuicSocket callbacks expose stream data?

**StreamReadCallback** (alias: `ReadCallback`)

```cpp
class StreamReadCallback {
  virtual void readAvailable(StreamId id) noexcept = 0;
  virtual void readError(StreamId id, QuicError error) noexcept = 0;
};
```

Installed per-stream via `QuicSocket::setReadCallback(StreamId, ReadCallback*)`.
`readAvailable` fires when data arrives; the adapter then calls
`QuicSocket::read()` to retrieve the bytes.

## 2. Which callbacks report connection close/reset?

**ConnectionSetupCallback:**

```cpp
virtual void onConnectionSetupError(QuicError code) noexcept = 0;
virtual void onTransportReady() noexcept {}
virtual void onReplaySafe() noexcept {}
virtual void onFullHandshakeDone() noexcept {}
```

**ConnectionCallback:**

```cpp
virtual void onConnectionEnd() noexcept = 0;
virtual void onConnectionError(QuicError code) noexcept = 0;
virtual void onConnectionEnd(QuicError error) noexcept {}  // combined
virtual void onStopSending(StreamId id, ApplicationErrorCode) noexcept = 0;
```

`onConnectionEnd()` fires for clean close; `onConnectionError()` for
error close. Both signal connection death.

## 3. How are bidi streams opened?

```cpp
virtual Expected<StreamId, LocalErrorCode>
createBidirectionalStream(bool replaySafe = true) = 0;
```

Returns a `StreamId` on success. The caller then calls
`setReadCallback()` on the stream and writes with `writeChain()`.

## 4. How are uni streams opened?

```cpp
virtual Expected<StreamId, LocalErrorCode>
createUnidirectionalStream(bool replaySafe = true) = 0;
```

Same pattern. The peer receives the stream via
`ConnectionCallback::onNewUnidirectionalStream(StreamId)`.

## 5. How is write backpressure represented?

Three levels:

**Query available capacity:**
```cpp
virtual Expected<uint64_t, LocalErrorCode>
getMaxWritableOnStream(StreamId id) const = 0;
```

**Stream write readiness callback:**
```cpp
class StreamWriteCallback {
  virtual void onStreamWriteReady(StreamId id, uint64_t maxToSend) noexcept {}
  virtual void onStreamWriteError(StreamId id, QuicError error) noexcept {}
};
```

Registered via:
```cpp
virtual Expected<void, LocalErrorCode>
notifyPendingWriteOnStream(StreamId id, StreamWriteCallback* wcb) = 0;
```

**Connection write readiness callback:**
```cpp
class ConnectionWriteCallback {
  virtual void onConnectionWriteReady(uint64_t maxToSend) noexcept {}
  virtual void onConnectionWriteError(QuicError error) noexcept {}
};
```

The adapter should call `notifyPendingWriteOnStream()` when
`writeChain()` cannot accept more data, and retry writes in
`onStreamWriteReady()`.

## 6. How is readable stream data delivered?

```cpp
virtual Expected<std::pair<BufPtr, bool>, LocalErrorCode>
read(StreamId id, size_t maxLen) = 0;
```

Returns `std::pair<BufPtr, bool>`:
- `BufPtr` = `std::unique_ptr<folly::IOBuf>` — the data as an IOBuf chain
- `bool` = EOF flag

The IOBuf chain may not be contiguous. For control stream parsing,
the adapter must either walk the chain or coalesce it. For data
streams, each `IOBuf` in the chain can be passed to
`moq_session_on_data_bytes()` individually.

## 7. What is the folly::IOBuf write lifetime rule?

```cpp
using BufPtr = std::unique_ptr<folly::IOBuf>;

virtual WriteResult writeChain(
    StreamId id, BufPtr data, bool eof,
    ByteEventCallback* cb = nullptr) = 0;
```

`writeChain()` takes ownership of the `BufPtr` via move.
After the call, the caller no longer owns the IOBuf chain.
The transport manages the buffer lifetime internally.

## 8. Can the send path wrap moq_rcbuf_t in IOBuf with a custom decref callback?

Yes. `folly::IOBuf::takeOwnership()` accepts a custom free function:

```cpp
static std::unique_ptr<IOBuf> takeOwnership(
    void* buf, std::size_t capacity,
    FreeFunction freeFn = nullptr,
    void* userData = nullptr,
    bool freeOnError = true);
```

The adapter can wrap `moq_rcbuf_data()` as:

```cpp
auto iobuf = folly::IOBuf::takeOwnership(
    const_cast<void*>(static_cast<const void*>(moq_rcbuf_data(rcbuf))),
    moq_rcbuf_len(rcbuf),
    [](void* /*buf*/, void* ud) {
        moq_rcbuf_decref(static_cast<moq_rcbuf_t*>(ud));
    },
    rcbuf);
moq_rcbuf_incref(rcbuf);  // ref for the IOBuf
```

This gives zero-copy send: the IOBuf wraps the rcbuf data directly,
and decrefs the rcbuf when the transport is done with the buffer.

The adapter must incref before handing to `writeChain()` and call
`moq_action_cleanup()` separately — the action's ref and the IOBuf's
ref are independent.

## Additional notes

**Datagram API:**

```cpp
virtual WriteResult writeDatagram(BufPtr buf) = 0;
virtual Expected<std::vector<ReadDatagram>, LocalErrorCode>
readDatagrams(size_t atMost = 0) = 0;
```

`DatagramCallback::onDatagramsAvailable()` signals datagrams are
ready to read. The adapter calls `readDatagrams()` and passes each
to `moq_session_on_datagram()`.

**Installed CMake target:** `mvfst::mvfst` (monolithic static library).
Granular targets like `mvfst::mvfst_client_client` also exist but the
monolithic target is the verified installed interface.

**Stream reset / stop sending:**

```cpp
virtual void resetStream(StreamId id, ApplicationErrorCode) = 0;
virtual void stopSending(StreamId id, ApplicationErrorCode) = 0;
```

Maps directly to libmoq's `RESET_DATA` and `STOP_DATA` actions.

**New stream notification:**

```cpp
virtual void onNewBidirectionalStream(StreamId id) noexcept = 0;
virtual void onNewUnidirectionalStream(StreamId id) noexcept = 0;
```

The adapter must call `setReadCallback()` on new streams to begin
receiving data. Stream classification (control vs data) happens on
first bytes received.
