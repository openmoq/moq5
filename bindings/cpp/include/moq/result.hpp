#ifndef MOQ_RESULT_HPP
#define MOQ_RESULT_HPP

#include <moq/types.h>

#include <optional>
#include <stdexcept>
#include <utility>

namespace moq {

enum class errc : int {
    nomem           = MOQ_ERR_NOMEM,
    invalid         = MOQ_ERR_INVAL,
    protocol        = MOQ_ERR_PROTO,
    closed          = MOQ_ERR_CLOSED,
    wrong_state     = MOQ_ERR_WRONG_STATE,
    stale_handle    = MOQ_ERR_STALE_HANDLE,
    wrong_session   = MOQ_ERR_WRONG_SESSION,
    would_block     = MOQ_ERR_WOULD_BLOCK,
    buffer          = MOQ_ERR_BUFFER,
    request_blocked = MOQ_ERR_REQUEST_BLOCKED,
    abi_mismatch    = MOQ_ERR_ABI_MISMATCH,
    goaway          = MOQ_ERR_GOAWAY,
    interrupted     = MOQ_ERR_INTERRUPTED,
    unsupported     = MOQ_ERR_UNSUPPORTED,
    internal        = MOQ_ERR_INTERNAL,
};

class error : public std::exception {
public:
    explicit error(errc code) noexcept : code_(code) {}

    errc code() const noexcept { return code_; }
    const char *message() const noexcept { return moq_strerror(static_cast<int>(code_)); }
    const char *what() const noexcept override { return message(); }

private:
    errc code_;
};

inline errc errc_from_result(moq_result_t rc) noexcept
{
    return static_cast<errc>(rc);
}

template<typename T>
class result {
public:
    result(T &&val) : val_(std::move(val)) {}
    result(const T &val) : val_(val) {}

    result(errc e) noexcept : err_(e) {}
    result(moq::error e) noexcept : err_(e.code()) {}

    bool     ok() const noexcept { return val_.has_value(); }
    explicit operator bool() const noexcept { return ok(); }

    T       &value() &
    {
        if (!ok())
            throw moq::error(err_);
        return *val_;
    }
    const T &value() const &
    {
        if (!ok())
            throw moq::error(err_);
        return *val_;
    }
    T      &&value() &&
    {
        if (!ok())
            throw moq::error(err_);
        return std::move(*val_);
    }

    T       *operator->() noexcept { return &*val_; }
    const T *operator->() const noexcept { return &*val_; }

    T       &operator*() & noexcept { return *val_; }
    const T &operator*() const & noexcept { return *val_; }
    T      &&operator*() && noexcept { return std::move(*val_); }

    moq::error error() const noexcept { return moq::error(err_); }

private:
    std::optional<T> val_;
    errc             err_{};
};

template<>
class result<void> {
public:
    result() noexcept : err_{}, ok_(true) {}
    result(errc e) noexcept : err_(e), ok_(false) {}
    result(moq::error e) noexcept : err_(e.code()), ok_(false) {}

    bool ok() const noexcept { return ok_; }
    explicit operator bool() const noexcept { return ok_; }

    void value() const
    {
        if (!ok())
            throw moq::error(err_);
    }

    moq::error error() const noexcept { return moq::error(err_); }

private:
    errc err_;
    bool ok_;
};

inline result<void> result_from_rc(moq_result_t rc) noexcept
{
    if (rc < 0)
        return errc_from_result(rc);
    return {};
}

} // namespace moq

#define MOQ_TRY(expr)                                  \
    do {                                               \
        auto _moq_try_r_ = (expr);                     \
        if (!_moq_try_r_)                               \
            return _moq_try_r_.error().code();          \
    } while (0)

#endif // MOQ_RESULT_HPP
