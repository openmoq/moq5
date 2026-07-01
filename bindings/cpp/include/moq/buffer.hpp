#ifndef MOQ_BUFFER_HPP
#define MOQ_BUFFER_HPP

#include <moq/rcbuf.h>
#include <moq/result.hpp>

#include <cstddef>
#include <cstdint>
#include <span>

namespace moq {

class buffer {
public:
    using release_fn = moq_rcbuf_release_fn;

    buffer() noexcept : raw_(nullptr) {}

    static buffer adopt(moq_rcbuf_t *raw) noexcept { return buffer(raw); }

    static buffer retain(moq_rcbuf_t *raw) noexcept
    {
        return buffer(moq_rcbuf_incref(raw));
    }

    static result<buffer> create(const uint8_t *data, size_t len,
                                 const moq_alloc_t *alloc = moq_alloc_default())
    {
        moq_rcbuf_t *raw = nullptr;
        moq_result_t rc  = moq_rcbuf_create(alloc, data, len, &raw);
        if (rc < 0)
            return errc_from_result(rc);
        return buffer(raw);
    }

    // Wrap externally-owned data without copying. The release callback
    // fires exactly once when the last buffer/rcbuf reference is dropped.
    //
    // Use this when the external owner (transport, capture API, etc.)
    // can keep the bytes alive until the release callback. The callback
    // should release the external packet or decrement the external
    // refcount. If the external API cannot retain ownership, use
    // buffer::create to copy instead.
    static result<buffer> wrap(const uint8_t *data, size_t len,
                               release_fn release, void *release_ctx,
                               const moq_alloc_t *alloc = moq_alloc_default())
    {
        moq_rcbuf_t *raw = nullptr;
        moq_result_t rc  = moq_rcbuf_wrap(alloc, data, len,
                                           release, release_ctx, &raw);
        if (rc < 0)
            return errc_from_result(rc);
        return buffer(raw);
    }

    buffer(const buffer &other) noexcept : raw_(moq_rcbuf_incref(other.raw_)) {}

    buffer &operator=(const buffer &other) noexcept
    {
        if (this != &other) {
            moq_rcbuf_decref(raw_);
            raw_ = moq_rcbuf_incref(other.raw_);
        }
        return *this;
    }

    buffer(buffer &&other) noexcept : raw_(other.raw_) { other.raw_ = nullptr; }

    buffer &operator=(buffer &&other) noexcept
    {
        if (this != &other) {
            moq_rcbuf_decref(raw_);
            raw_       = other.raw_;
            other.raw_ = nullptr;
        }
        return *this;
    }

    ~buffer() { moq_rcbuf_decref(raw_); }

    const uint8_t              *data() const noexcept { return moq_rcbuf_data(raw_); }
    size_t                      size() const noexcept { return moq_rcbuf_len(raw_); }
    std::span<const uint8_t>    span() const noexcept { return {data(), size()}; }
    bool                        empty() const noexcept { return size() == 0; }

    // Create an independent copy of this buffer's visible bytes into
    // storage owned by `dst_alloc` — the explicit, blessed way to hand
    // bytes to another shard (executor-affinity domain).
    //
    // The copy constructor / copy assignment above are *same-shard*
    // refcount bumps (non-atomic incref): cheap, but valid only within a
    // single shard. clone_for_shard instead allocates fresh storage from
    // `dst_alloc` and copies the bytes, so the result shares nothing with
    // this buffer (no shared refcount, no retained parent, no inherited
    // wrap release callback).
    //
    // This is NOT a thread-safe ownership transfer: call it where
    // `dst_alloc` is valid (typically the destination shard) and keep
    // this buffer alive for the duration of the copy. `dst_alloc`
    // defaults to moq_alloc_default() — a concrete allocator, not a
    // NULL-as-default sentinel. A default-constructed (null) buffer
    // returns the underlying invalid-argument error, not an empty clone.
    result<buffer> clone_for_shard(
        const moq_alloc_t *dst_alloc = moq_alloc_default()) const
    {
        moq_rcbuf_t *out = nullptr;
        moq_result_t rc  = moq_rcbuf_clone(dst_alloc, raw_, &out);
        if (rc < 0)
            return errc_from_result(rc);
        return buffer(out);
    }

    moq_rcbuf_t *raw() const noexcept { return raw_; }

    moq_rcbuf_t *release() noexcept
    {
        auto *p = raw_;
        raw_    = nullptr;
        return p;
    }

private:
    explicit buffer(moq_rcbuf_t *raw) noexcept : raw_(raw) {}

    moq_rcbuf_t *raw_;
};

} // namespace moq

#endif // MOQ_BUFFER_HPP
