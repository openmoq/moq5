#include <moq/result.hpp>
#include "test_support.hpp"

#include <memory>
#include <type_traits>

static moq::result<void> try_test(bool fail)
{
    auto inner = fail ? moq::result<void>(moq::errc::invalid)
                      : moq::result<void>();
    MOQ_TRY(inner);
    return {};
}

static moq::result<int> try_value_test(bool fail)
{
    auto inner = fail ? moq::result<int>(moq::errc::would_block)
                      : moq::result<int>(42);
    MOQ_TRY(inner);
    return *inner;
}

int main()
{
    int failures = 0;

    // result<int> success
    {
        moq::result<int> r(42);
        MOQ_CHECK(r.ok());
        MOQ_CHECK(static_cast<bool>(r));
        MOQ_CHECK(r.value() == 42);
        MOQ_CHECK(*r == 42);
    }

    // result<int> error
    {
        moq::result<int> r(moq::errc::nomem);
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(!static_cast<bool>(r));
        MOQ_CHECK(r.error().code() == moq::errc::nomem);
    }

    // value() throws on error
    {
        moq::result<int> r(moq::errc::invalid);
        bool caught = false;
        try {
            (void)r.value();
        } catch (const moq::error &e) {
            caught = true;
            MOQ_CHECK(e.code() == moq::errc::invalid);
        }
        MOQ_CHECK(caught);
    }

    // operator->
    {
        struct point {
            int x, y;
        };
        moq::result<point> r(point{3, 4});
        MOQ_CHECK(r->x == 3);
        MOQ_CHECK(r->y == 4);
    }

    // result<void> success
    {
        moq::result<void> r;
        MOQ_CHECK(r.ok());
    }

    // result<void> error
    {
        moq::result<void> r(moq::errc::closed);
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::closed);
    }

    // result<void> value() throws on error
    {
        moq::result<void> r(moq::errc::protocol);
        bool caught = false;
        try {
            r.value();
        } catch (const moq::error &e) {
            caught = true;
            MOQ_CHECK(e.code() == moq::errc::protocol);
        }
        MOQ_CHECK(caught);
    }

    // result_from_rc
    {
        auto ok = moq::result_from_rc(MOQ_OK);
        MOQ_CHECK(ok.ok());

        auto done = moq::result_from_rc(MOQ_DONE);
        MOQ_CHECK(done.ok());

        auto err = moq::result_from_rc(MOQ_ERR_NOMEM);
        MOQ_CHECK(!err.ok());
        MOQ_CHECK(err.error().code() == moq::errc::nomem);
    }

    // error message is non-null
    {
        moq::error e(moq::errc::nomem);
        MOQ_CHECK(e.message() != nullptr);
        MOQ_CHECK(e.what() != nullptr);
    }

    // MOQ_TRY propagates error
    {
        auto ok = try_test(false);
        MOQ_CHECK(ok.ok());

        auto err = try_test(true);
        MOQ_CHECK(!err.ok());
        MOQ_CHECK(err.error().code() == moq::errc::invalid);
    }

    // MOQ_TRY with value-bearing result
    {
        auto ok = try_value_test(false);
        MOQ_CHECK(ok.ok());
        MOQ_CHECK(ok.value() == 42);

        auto err = try_value_test(true);
        MOQ_CHECK(!err.ok());
        MOQ_CHECK(err.error().code() == moq::errc::would_block);
    }

    // Move semantics
    {
        moq::result<int> r1(99);
        moq::result<int> r2(std::move(r1));
        MOQ_CHECK(r2.ok());
        MOQ_CHECK(r2.value() == 99);
    }

    // Construct from moq::error
    {
        moq::error e(moq::errc::goaway);
        moq::result<int> r(e);
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::goaway);
    }

    // All errc values map correctly
    {
        MOQ_CHECK(static_cast<int>(moq::errc::nomem) == MOQ_ERR_NOMEM);
        MOQ_CHECK(static_cast<int>(moq::errc::invalid) == MOQ_ERR_INVAL);
        MOQ_CHECK(static_cast<int>(moq::errc::protocol) == MOQ_ERR_PROTO);
        MOQ_CHECK(static_cast<int>(moq::errc::closed) == MOQ_ERR_CLOSED);
        MOQ_CHECK(static_cast<int>(moq::errc::wrong_state) == MOQ_ERR_WRONG_STATE);
        MOQ_CHECK(static_cast<int>(moq::errc::stale_handle) == MOQ_ERR_STALE_HANDLE);
        MOQ_CHECK(static_cast<int>(moq::errc::wrong_session) == MOQ_ERR_WRONG_SESSION);
        MOQ_CHECK(static_cast<int>(moq::errc::would_block) == MOQ_ERR_WOULD_BLOCK);
        MOQ_CHECK(static_cast<int>(moq::errc::buffer) == MOQ_ERR_BUFFER);
        MOQ_CHECK(static_cast<int>(moq::errc::request_blocked) == MOQ_ERR_REQUEST_BLOCKED);
        MOQ_CHECK(static_cast<int>(moq::errc::abi_mismatch) == MOQ_ERR_ABI_MISMATCH);
        MOQ_CHECK(static_cast<int>(moq::errc::goaway) == MOQ_ERR_GOAWAY);
        MOQ_CHECK(static_cast<int>(moq::errc::internal) == MOQ_ERR_INTERNAL);
    }

    // result<MoveOnly> — copy traits propagate correctly
    {
        using move_only = std::unique_ptr<int>;
        static_assert(!std::is_copy_constructible_v<moq::result<move_only>>,
                      "result<MoveOnly> must not be copyable");
        static_assert(std::is_move_constructible_v<moq::result<move_only>>,
                      "result<MoveOnly> must be movable");

        moq::result<move_only> r(std::make_unique<int>(77));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(**r == 77);

        auto r2 = std::move(r);
        MOQ_CHECK(r2.ok());
        MOQ_CHECK(**r2 == 77);

        moq::result<move_only> err(moq::errc::nomem);
        MOQ_CHECK(!err.ok());
    }

    // result<int> is copyable
    {
        static_assert(std::is_copy_constructible_v<moq::result<int>>,
                      "result<int> must be copyable");

        moq::result<int> r1(42);
        auto r2 = r1;
        MOQ_CHECK(r2.ok());
        MOQ_CHECK(r2.value() == 42);
    }

    MOQ_PASS("test_cpp_result");
    return failures;
}
