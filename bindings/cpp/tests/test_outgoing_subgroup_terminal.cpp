#include <moq/moq.hpp>
#include "test_support.hpp"

#include <array>
#include <optional>
#include <utility>

static moq_event_t event(bool publication, uint32_t cause, uint64_t code)
{
    moq_event_t out{};
    out.kind = MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED;
    out.detail_size = MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE;
    auto &d = out.u.outgoing_subgroup_terminated;
    d.subgroup._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP, 17, 3, 9);
    if (publication)
        d.pub._opaque = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION, 17, 5, 11);
    else
        d.sub._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION, 17, 7, 13);
    d.group_id = UINT64_MAX;
    d.subgroup_id = UINT64_MAX - 1;
    d.stream_ref = moq_stream_ref_from_u64(UINT64_MAX - 2);
    d.error_code = code;
    d.cause = cause;
    return out;
}

int main()
{
    int failures = 0;
    using terminal_cause = moq::outgoing_subgroup_terminal_cause;
    const std::array<std::pair<uint32_t, terminal_cause>, 3> causes{{
        {MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING,
         terminal_cause::peer_stop_sending},
        {MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
         terminal_cause::delivery_timeout},
        {UINT32_MAX, static_cast<terminal_cause>(UINT32_MAX)}}};
    const std::array<uint64_t, 2> codes{0, UINT64_MAX};
    for (bool publication : std::array<bool, 2>{false, true}) {
        for (const auto &[cause, expected_cause] : causes) {
            for (uint64_t code : codes) {
                auto raw = event(publication, cause, code);
                const auto native = raw.u.outgoing_subgroup_terminated;
                std::optional<moq::extended_event::outgoing_subgroup_terminated> kept;
                {
                    moq::polled_event polled(raw);
                    const auto legacy = polled.variant();
                    const auto *unknown = std::get_if<moq::event::unknown>(&legacy);
                    MOQ_CHECK(unknown != nullptr);
                    if (unknown) MOQ_CHECK(unknown->kind == raw.kind);
                    const auto extended = polled.variant_extended();
                    const auto *observed = std::get_if<
                        moq::extended_event::outgoing_subgroup_terminated>(&extended);
                    MOQ_CHECK(observed != nullptr);
                    if (!observed) continue;
                    kept = *observed;
                    raw = {};
                }
                MOQ_CHECK(kept->subgroup.raw()._opaque == native.subgroup._opaque);
                MOQ_CHECK(kept->sub.raw()._opaque == native.sub._opaque);
                MOQ_CHECK(kept->pub.raw()._opaque == native.pub._opaque);
                MOQ_CHECK(kept->sub.valid() != kept->pub.valid());
                MOQ_CHECK(kept->group_id == UINT64_MAX);
                MOQ_CHECK(kept->subgroup_id == UINT64_MAX - 1);
                MOQ_CHECK(kept->stream.value() == UINT64_MAX - 2);
                MOQ_CHECK(kept->error_code == code);
                MOQ_CHECK(kept->cause == expected_cause);
                MOQ_CHECK(static_cast<uint32_t>(kept->cause) == cause);
            }
        }
    }
    for (uint32_t prefix = 0; prefix < MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE; ++prefix) {
        auto raw = event(false, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING, 0);
        raw.detail_size = prefix;
        moq::polled_event polled(raw);
        const auto extended = polled.variant_extended();
        const auto *unknown = std::get_if<moq::event::unknown>(&extended);
        MOQ_CHECK(unknown != nullptr);
        if (unknown) MOQ_CHECK(unknown->kind == raw.kind);
    }
    auto raw = event(false, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT, 0);
    raw.detail_size = UINT32_MAX;
    MOQ_CHECK(std::holds_alternative<moq::extended_event::outgoing_subgroup_terminated>(
        moq::polled_event(raw).variant_extended()));
    raw.kind = UINT32_MAX;
    const auto extended = moq::polled_event(raw).variant_extended();
    const auto *unknown = std::get_if<moq::event::unknown>(&extended);
    MOQ_CHECK(unknown != nullptr);
    if (unknown) MOQ_CHECK(unknown->kind == UINT32_MAX);
    return failures ? 1 : 0;
}
