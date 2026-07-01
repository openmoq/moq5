#ifndef MOQ_ACTION_HPP
#define MOQ_ACTION_HPP

#include <moq/buffer.hpp>
#include <moq/types.hpp>
#include <moq/visit.hpp>

#include <cstring>
#include <span>
#include <string_view>
#include <variant>

namespace moq {

/* Lifetime contract for action value alternatives: send_data is SELF-OWNING for
 * its payload -- it holds a retained moq::buffer (payload_buf), so payload,
 * payload_rcbuf, and payload_owned() stay valid for as long as the value (or any
 * copy) is alive, even after the polled_action is destroyed and
 * moq_action_cleanup() decrefs the C action's ref. Copy increfs; move steals.
 * All other action fields remain borrowed from polled_action-owned scratch. */
namespace action {

struct send_control {
    std::span<const uint8_t> data;
};

struct close_session {
    uint64_t         code;
    std::string_view reason;
};

struct send_data {
    stream_ref               ref;
    std::span<const uint8_t> header;
    std::span<const uint8_t> payload;
    bool                     fin;
    moq_rcbuf_t             *payload_rcbuf;
    /* Owner retained at variant() construction so the payload buffer (and the
     * span + raw pointer above, which alias it) stays valid after the source
     * polled_action is destroyed and moq_action_cleanup() decrefs the C action's
     * ref. Copying the variant increfs; moving steals -- no double-free. */
    buffer payload_buf;

    buffer payload_owned() const noexcept { return payload_buf; }
};

struct reset_data {
    stream_ref ref;
    uint64_t   error_code;
};

struct stop_data {
    stream_ref ref;
    uint64_t   error_code;
};

struct open_bidi_stream {
    stream_ref               ref;
    std::span<const uint8_t> data;
    bool                     fin;  // close the local send half after the data
};

struct send_bidi_stream {
    stream_ref               ref;
    std::span<const uint8_t> data;
    bool                     fin;
};

struct close_bidi_stream {
    stream_ref ref;
};

struct send_datagram {
    std::span<const uint8_t> data;
};

struct unknown {
    moq_action_kind_t kind;
};

} // namespace action

using action_variant = std::variant<action::send_control,
                                    action::close_session,
                                    action::send_data,
                                    action::reset_data,
                                    action::stop_data,
                                    action::open_bidi_stream,
                                    action::send_bidi_stream,
                                    action::close_bidi_stream,
                                    action::send_datagram,
                                    action::unknown>;

class polled_action {
    moq_action_t action_{};

public:
    explicit polled_action(moq_action_t a) noexcept : action_(a) {}

    ~polled_action() { moq_action_cleanup(&action_); }

    polled_action(polled_action &&o) noexcept : action_(o.action_)
    {
        std::memset(&o.action_, 0, sizeof(o.action_));
    }

    polled_action &operator=(polled_action &&o) noexcept
    {
        if (this != &o) {
            moq_action_cleanup(&action_);
            action_ = o.action_;
            std::memset(&o.action_, 0, sizeof(o.action_));
        }
        return *this;
    }

    polled_action(const polled_action &)            = delete;
    polled_action &operator=(const polled_action &) = delete;

    moq_action_kind_t kind() const noexcept { return action_.kind; }

    action_variant variant() const noexcept
    {
        auto &a = action_;
        switch (a.kind) {
        case MOQ_ACTION_SEND_CONTROL:
            return action::send_control{
                {a.u.send_control.data, a.u.send_control.len}};

        case MOQ_ACTION_CLOSE_SESSION:
            return action::close_session{
                a.u.close_session.code,
                {reinterpret_cast<const char *>(a.u.close_session.reason.data),
                 a.u.close_session.reason.len}};

        case MOQ_ACTION_SEND_DATA:
            return action::send_data{
                stream_ref(a.u.send_data.stream_ref),
                {a.u.send_data.header, a.u.send_data.header_len},
                {moq_rcbuf_data(a.u.send_data.payload),
                 moq_rcbuf_len(a.u.send_data.payload)},
                a.u.send_data.fin,
                a.u.send_data.payload,
                buffer::retain(a.u.send_data.payload)};

        case MOQ_ACTION_RESET_DATA:
            return action::reset_data{stream_ref(a.u.reset_data.stream_ref),
                                      a.u.reset_data.error_code};

        case MOQ_ACTION_STOP_DATA:
            return action::stop_data{stream_ref(a.u.stop_data.stream_ref),
                                     a.u.stop_data.error_code};

        case MOQ_ACTION_OPEN_BIDI_STREAM:
            return action::open_bidi_stream{
                stream_ref(a.u.open_bidi_stream.stream_ref),
                {a.u.open_bidi_stream.data, a.u.open_bidi_stream.len},
                a.u.open_bidi_stream.fin};

        case MOQ_ACTION_SEND_BIDI_STREAM:
            return action::send_bidi_stream{
                stream_ref(a.u.send_bidi_stream.stream_ref),
                {a.u.send_bidi_stream.data, a.u.send_bidi_stream.len},
                a.u.send_bidi_stream.fin};

        case MOQ_ACTION_CLOSE_BIDI_STREAM:
            return action::close_bidi_stream{
                stream_ref(a.u.close_bidi_stream.stream_ref)};

        case MOQ_ACTION_SEND_DATAGRAM:
            return action::send_datagram{
                {a.u.send_datagram.data, a.u.send_datagram.len}};

        default:
            return action::unknown{a.kind};
        }
    }

    template<typename... Visitors>
    decltype(auto) visit(Visitors &&...visitors) const
    {
        return std::visit(moq::overloaded{std::forward<Visitors>(visitors)...},
                          variant());
    }

    const moq_action_t &raw() const noexcept { return action_; }
};

} // namespace moq

#endif // MOQ_ACTION_HPP
