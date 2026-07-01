#include <moq/buffer.hpp>
#include "test_support.hpp"

#include <cstdint>
#include <cstdlib>
#include <cstring>

int main()
{
    int failures = 0;

    // Default buffer is empty
    {
        moq::buffer buf;
        MOQ_CHECK(buf.empty());
        MOQ_CHECK(buf.size() == 0);
        MOQ_CHECK(buf.data() == nullptr);
        MOQ_CHECK(buf.raw() == nullptr);
    }

    // Create from bytes
    {
        const uint8_t data[] = {1, 2, 3, 4, 5};
        auto          r      = moq::buffer::create(data, 5);
        MOQ_CHECK(r.ok());
        auto buf = std::move(*r);
        MOQ_CHECK(!buf.empty());
        MOQ_CHECK(buf.size() == 5);
        MOQ_CHECK(buf.data() != nullptr);
        MOQ_CHECK(std::memcmp(buf.data(), data, 5) == 0);
    }

    // Copy increfs
    {
        const uint8_t data[] = {10, 20};
        auto          r      = moq::buffer::create(data, 2);
        MOQ_CHECK(r.ok());
        auto original = std::move(*r);

        {
            auto copy = original;
            MOQ_CHECK(copy.size() == 2);
            MOQ_CHECK(copy.data()[0] == 10);
            MOQ_CHECK(moq_rcbuf_refcount(original.raw()) == 2);
        }
        MOQ_CHECK(moq_rcbuf_refcount(original.raw()) == 1);
    }

    // Move transfers ownership
    {
        const uint8_t data[] = {42};
        auto          r      = moq::buffer::create(data, 1);
        MOQ_CHECK(r.ok());
        auto buf1 = std::move(*r);
        auto raw  = buf1.raw();

        auto buf2 = std::move(buf1);
        MOQ_CHECK(buf1.raw() == nullptr);
        MOQ_CHECK(buf1.empty());
        MOQ_CHECK(buf2.raw() == raw);
        MOQ_CHECK(buf2.size() == 1);
    }

    // Release
    {
        const uint8_t data[] = {1};
        auto          r      = moq::buffer::create(data, 1);
        MOQ_CHECK(r.ok());
        auto  buf = std::move(*r);
        auto *raw = buf.release();
        MOQ_CHECK(buf.raw() == nullptr);
        MOQ_CHECK(raw != nullptr);
        moq_rcbuf_decref(raw);
    }

    // Span access
    {
        const uint8_t data[] = {1, 2, 3};
        auto          r      = moq::buffer::create(data, 3);
        MOQ_CHECK(r.ok());
        auto buf = std::move(*r);
        auto sp  = buf.span();
        MOQ_CHECK(sp.size() == 3);
        MOQ_CHECK(sp[0] == 1);
        MOQ_CHECK(sp[2] == 3);
    }

    // Create empty buffer
    {
        auto r = moq::buffer::create(nullptr, 0);
        MOQ_CHECK(r.ok());
        auto buf = std::move(*r);
        MOQ_CHECK(buf.size() == 0);
        MOQ_CHECK(buf.raw() != nullptr);
    }

    // Copy assignment
    {
        const uint8_t d1[] = {1};
        const uint8_t d2[] = {2};
        auto          r1   = moq::buffer::create(d1, 1);
        auto          r2   = moq::buffer::create(d2, 1);
        MOQ_CHECK(r1.ok());
        MOQ_CHECK(r2.ok());
        auto buf1 = std::move(*r1);
        auto buf2 = std::move(*r2);

        buf1 = buf2;
        MOQ_CHECK(buf1.data()[0] == 2);
        MOQ_CHECK(moq_rcbuf_refcount(buf1.raw()) == 2);
    }

    // Move assignment
    {
        const uint8_t d1[] = {1};
        const uint8_t d2[] = {2};
        auto          r1   = moq::buffer::create(d1, 1);
        auto          r2   = moq::buffer::create(d2, 1);
        MOQ_CHECK(r1.ok());
        MOQ_CHECK(r2.ok());
        auto buf1 = std::move(*r1);
        auto buf2 = std::move(*r2);

        buf1 = std::move(buf2);
        MOQ_CHECK(buf1.data()[0] == 2);
        MOQ_CHECK(buf2.raw() == nullptr);
    }

    // Self-assignment
    {
        const uint8_t data[] = {7};
        auto          r      = moq::buffer::create(data, 1);
        MOQ_CHECK(r.ok());
        auto buf = std::move(*r);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wself-assign-overloaded"
        buf = buf;
#pragma GCC diagnostic pop

        MOQ_CHECK(buf.data()[0] == 7);
        MOQ_CHECK(moq_rcbuf_refcount(buf.raw()) == 1);
    }

    // adopt() takes ownership without incref
    {
        moq_rcbuf_t  *raw = nullptr;
        const uint8_t data[] = {9};
        moq_result_t  rc  = moq_rcbuf_create(moq_alloc_default(), data, 1, &raw);
        MOQ_CHECK(rc == MOQ_OK);
        MOQ_CHECK(moq_rcbuf_refcount(raw) == 1);

        auto buf = moq::buffer::adopt(raw);
        MOQ_CHECK(buf.size() == 1);
        MOQ_CHECK(buf.data()[0] == 9);
        MOQ_CHECK(moq_rcbuf_refcount(buf.raw()) == 1);
    }

    // retain() increfs
    {
        moq_rcbuf_t  *raw = nullptr;
        const uint8_t data[] = {8};
        moq_result_t  rc  = moq_rcbuf_create(moq_alloc_default(), data, 1, &raw);
        MOQ_CHECK(rc == MOQ_OK);
        MOQ_CHECK(moq_rcbuf_refcount(raw) == 1);

        {
            auto buf = moq::buffer::retain(raw);
            MOQ_CHECK(buf.size() == 1);
            MOQ_CHECK(buf.data()[0] == 8);
            MOQ_CHECK(moq_rcbuf_refcount(raw) == 2);
        }
        MOQ_CHECK(moq_rcbuf_refcount(raw) == 1);
        moq_rcbuf_decref(raw);
    }

    // wrap: data pointer preserved, no copy
    {
        static uint8_t ext[] = {0xAA, 0xBB, 0xCC};
        int released = 0;
        auto r = moq::buffer::wrap(ext, 3,
            [](void *ctx, const uint8_t *, size_t) {
                ++*static_cast<int *>(ctx);
            }, &released);
        MOQ_CHECK(r.ok());
        auto buf = std::move(*r);
        MOQ_CHECK(buf.data() == ext);
        MOQ_CHECK(buf.size() == 3);
        MOQ_CHECK(moq_rcbuf_refcount(buf.raw()) == 1);
        MOQ_CHECK(released == 0);
    }

    // wrap: release fires exactly once on final drop
    {
        static uint8_t ext[] = {0x01};
        int released = 0;
        {
            auto buf = moq::buffer::wrap(ext, 1,
                [](void *ctx, const uint8_t *, size_t) {
                    ++*static_cast<int *>(ctx);
                }, &released).value();
            MOQ_CHECK(released == 0);
        }
        MOQ_CHECK(released == 1);
    }

    // wrap: copy increfs, release deferred until last drop
    {
        static uint8_t ext[] = {0x42};
        int released = 0;
        moq::buffer copy;
        {
            auto buf = moq::buffer::wrap(ext, 1,
                [](void *ctx, const uint8_t *, size_t) {
                    ++*static_cast<int *>(ctx);
                }, &released).value();

            copy = buf;
            MOQ_CHECK(moq_rcbuf_refcount(buf.raw()) == 2);
            MOQ_CHECK(released == 0);
        }
        MOQ_CHECK(released == 0);
        MOQ_CHECK(copy.data() == ext);

        copy = moq::buffer();
        MOQ_CHECK(released == 1);
    }

    // wrap: move does not incref or fire release
    {
        static uint8_t ext[] = {0xFF};
        int released = 0;
        auto buf1 = moq::buffer::wrap(ext, 1,
            [](void *ctx, const uint8_t *, size_t) {
                ++*static_cast<int *>(ctx);
            }, &released).value();
        auto raw = buf1.raw();

        auto buf2 = std::move(buf1);
        MOQ_CHECK(buf1.raw() == nullptr);
        MOQ_CHECK(buf2.raw() == raw);
        MOQ_CHECK(moq_rcbuf_refcount(raw) == 1);
        MOQ_CHECK(released == 0);
    }

    // wrap: nullptr with len=0 succeeds, release fires on drop
    {
        int released = 0;
        {
            auto r = moq::buffer::wrap(nullptr, 0,
                [](void *ctx, const uint8_t *, size_t) {
                    ++*static_cast<int *>(ctx);
                }, &released);
            MOQ_CHECK(r.ok());
            MOQ_CHECK(r->size() == 0);
            MOQ_CHECK(released == 0);
        }
        MOQ_CHECK(released == 1);
    }

    // wrap: nullptr with nonzero len returns error
    {
        auto r = moq::buffer::wrap(nullptr, 5, nullptr, nullptr);
        MOQ_CHECK(!r.ok());
    }

    // clone_for_shard: independent copy, distinct storage, independent lifetime
    {
        const uint8_t data[] = {1, 2, 3, 4};
        auto src = moq::buffer::create(data, 4).value();

        auto cr = src.clone_for_shard();
        MOQ_CHECK(cr.ok());
        auto clone = std::move(*cr);

        MOQ_CHECK(clone.size() == 4);
        MOQ_CHECK(std::memcmp(clone.data(), data, 4) == 0);
        MOQ_CHECK(clone.data() != src.data());        // distinct storage
        MOQ_CHECK(clone.raw() != src.raw());
        MOQ_CHECK(moq_rcbuf_refcount(clone.raw()) == 1);
        MOQ_CHECK(moq_rcbuf_refcount(src.raw()) == 1); // not a refcount bump

        // Independent lifetime: drop src, clone stays valid.
        src = moq::buffer();
        MOQ_CHECK(clone.size() == 4);
        MOQ_CHECK(std::memcmp(clone.data(), data, 4) == 0);
    }

    // clone_for_shard: explicit destination allocator is used for the clone
    {
        struct counts { int64_t bal = 0; };
        static counts dst;
        dst.bal = 0;
        moq_alloc_t dst_alloc = {
            &dst,
            [](size_t n, void *c) -> void * {
                void *p = std::malloc(n);
                if (p) static_cast<counts *>(c)->bal++;
                return p;
            },
            [](void *p, size_t, size_t n, void *) { return std::realloc(p, n); },
            [](void *p, size_t, void *c) {
                if (p) static_cast<counts *>(c)->bal--;
                std::free(p);
            },
        };

        const uint8_t data[] = {9, 8, 7};
        auto src = moq::buffer::create(data, 3).value();  // default allocator

        auto clone = src.clone_for_shard(&dst_alloc).value();
        MOQ_CHECK(dst.bal == 1);                           // clone used dst_alloc
        MOQ_CHECK(std::memcmp(clone.data(), data, 3) == 0);

        clone = moq::buffer();                             // free clone
        MOQ_CHECK(dst.bal == 0);                           // freed via dst_alloc
        // src still valid, freed via its own (default) allocator on scope exit.
        MOQ_CHECK(src.size() == 3);
    }

    // clone_for_shard: null (default-constructed) buffer returns error
    {
        moq::buffer null_buf;
        auto r = null_buf.clone_for_shard();
        MOQ_CHECK(!r.ok());   // mirrors moq_rcbuf_clone(src == NULL) -> INVAL
    }

    MOQ_PASS("test_cpp_buffer");
    return failures;
}
