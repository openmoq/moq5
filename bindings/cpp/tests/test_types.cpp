#include <moq/types.hpp>
#include <moq/visit.hpp>
#include "test_support.hpp"

#include <string_view>
#include <unordered_map>
#include <variant>

int main()
{
    int failures = 0;

    // -- stream_ref ----------------------------------------------------

    {
        moq::stream_ref ref;
        MOQ_CHECK(ref.value() == 0);

        moq::stream_ref ref2(uint64_t(42));
        MOQ_CHECK(ref2.value() == 42);
        MOQ_CHECK(ref != ref2);

        moq::stream_ref ref3(uint64_t(42));
        MOQ_CHECK(ref2 == ref3);
    }

    // stream_ref from C type
    {
        moq_stream_ref_t raw = moq_stream_ref_from_u64(99);
        moq::stream_ref   ref(raw);
        MOQ_CHECK(ref.value() == 99);
        MOQ_CHECK(ref.raw()._v == 99);
    }

    // stream_ref std::hash
    {
        moq::stream_ref                   a(uint64_t(10));
        moq::stream_ref                   b(uint64_t(20));
        std::hash<moq::stream_ref>        h;
        MOQ_CHECK(h(a) == h(a));
        MOQ_CHECK(h(a) != h(b));
    }

    // -- bytes_view ----------------------------------------------------

    {
        moq::bytes_view bv("hello");
        MOQ_CHECK(bv.size() == 5);
        MOQ_CHECK(bv.string_view() == "hello");
        MOQ_CHECK(!bv.empty());

        moq::bytes_view empty;
        MOQ_CHECK(empty.empty());
        MOQ_CHECK(empty.size() == 0);
    }

    // bytes_view API/type safety: it must NOT accept a bare const char*
    // (that would strlen-scan a possibly non-NUL-terminated, attacker-influenced
    // buffer past its end). A string literal stays ergonomic via the
    // compile-time-sized array overload; runtime C strings go through
    // std::string_view; arbitrary bytes through (data, len).
    {
        // Neither implicit conversion NOR direct construction from a bare
        // const char* is allowed (direct construction could otherwise route
        // const char* -> std::string_view -> bytes_view and strlen-scan).
        static_assert(!std::is_convertible_v<const char *, moq::bytes_view>,
                      "bytes_view must not implicitly accept const char*");
        static_assert(!std::is_constructible_v<moq::bytes_view, const char *>,
                      "bytes_view must not be constructible from const char*");
        static_assert(!std::is_constructible_v<moq::bytes_view, std::nullptr_t>,
                      "bytes_view must not be constructible from nullptr");
        static_assert(std::is_convertible_v<std::string_view, moq::bytes_view>,
                      "explicit string_view path must remain");

        // The literal overload is sized at compile time and never scans for a
        // NUL: an embedded-NUL literal keeps its full length (5), where a strlen
        // scan would have stopped at the first NUL (2).
        moq::bytes_view bv("ab\0cd");
        MOQ_CHECK(bv.size() == 5);
        MOQ_CHECK(bv.data()[2] == 0x00);
        MOQ_CHECK(bv.data()[4] == 'd');

        // A non-NUL-terminated char array keeps its FULL length -- the trailing
        // byte is dropped only when it is actually '\0'.
        const char raw[2] = {'A', 'B'};
        moq::bytes_view rv(raw);
        MOQ_CHECK(rv.size() == 2);
        MOQ_CHECK(rv.data()[1] == 'B');
    }

    // bytes_view from raw
    {
        const uint8_t   data[] = {0x41, 0x42};
        moq::bytes_view bv(data, 2);
        MOQ_CHECK(bv.size() == 2);
        MOQ_CHECK(bv.data()[0] == 0x41);
    }

    // bytes_view from moq_bytes_t
    {
        moq_bytes_t     raw = MOQ_BYTES_LITERAL("test");
        moq::bytes_view bv(raw);
        MOQ_CHECK(bv.size() == 4);
        MOQ_CHECK(bv.string_view() == "test");
    }

    // bytes_view span
    {
        moq::bytes_view bv("abc");
        auto            sp = bv.span();
        MOQ_CHECK(sp.size() == 3);
    }

    // -- namespace_name ------------------------------------------------

    // From initializer list
    {
        moq::namespace_name ns({"live", "camera-1"});
        MOQ_CHECK(ns.count() == 2);
        MOQ_CHECK(ns[0] == "live");
        MOQ_CHECK(ns[1] == "camera-1");
        MOQ_CHECK(!ns.overflowed());
    }

    // Empty
    {
        moq::namespace_name ns;
        MOQ_CHECK(ns.count() == 0);
        MOQ_CHECK(!ns.overflowed());
    }

    // Single part
    {
        moq::namespace_name ns({"chat"});
        MOQ_CHECK(ns.count() == 1);
        MOQ_CHECK(ns[0] == "chat");
    }

    // Out-of-bounds returns empty
    {
        moq::namespace_name ns({"a"});
        MOQ_CHECK(ns[1] == "");
        MOQ_CHECK(ns[99] == "");
    }

    // Overflow
    {
        std::string_view parts[33];
        for (int i = 0; i < 33; ++i)
            parts[i] = "x";
        moq::namespace_name ns(
            std::span<const std::string_view>(parts, 33));
        MOQ_CHECK(ns.count() == 32);
        MOQ_CHECK(ns.overflowed());
    }

    // Exactly max_parts
    {
        std::string_view parts[32];
        for (int i = 0; i < 32; ++i)
            parts[i] = "y";
        moq::namespace_name ns(
            std::span<const std::string_view>(parts, 32));
        MOQ_CHECK(ns.count() == 32);
        MOQ_CHECK(!ns.overflowed());
    }

    // c_namespace round-trip
    {
        moq::namespace_name ns({"a", "bb", "ccc"});
        moq_namespace_t     c_ns = ns.c_namespace();
        MOQ_CHECK(c_ns.count == 3);
        MOQ_CHECK(c_ns.parts[0].len == 1);
        MOQ_CHECK(c_ns.parts[1].len == 2);
        MOQ_CHECK(c_ns.parts[2].len == 3);
    }

    // Construct from C moq_namespace_t
    {
        moq_bytes_t     c_parts[] = {MOQ_BYTES_LITERAL("x"),
                                     MOQ_BYTES_LITERAL("yy")};
        moq_namespace_t c_ns      = {c_parts, 2};
        moq::namespace_name ns(c_ns);
        MOQ_CHECK(ns.count() == 2);
        MOQ_CHECK(ns[0] == "x");
        MOQ_CHECK(ns[1] == "yy");
    }

    // -- Handle wrappers -----------------------------------------------

    // -- Enum wrappers --------------------------------------------------

    {
        MOQ_CHECK(moq::to_c(moq::sub_fetch_item_kind::ok) ==
                  MOQ_SUB_FETCH_OK);
        MOQ_CHECK(moq::to_c(moq::sub_fetch_item_kind::error) ==
                  MOQ_SUB_FETCH_ERROR);
        MOQ_CHECK(moq::to_c(moq::sub_fetch_item_kind::object) ==
                  MOQ_SUB_FETCH_OBJECT);
        MOQ_CHECK(moq::to_c(moq::sub_fetch_item_kind::gap) ==
                  MOQ_SUB_FETCH_GAP);
        MOQ_CHECK(moq::to_c(moq::sub_fetch_item_kind::complete) ==
                  MOQ_SUB_FETCH_COMPLETE);
        MOQ_CHECK(moq::sub_fetch_item_kind_from_c(MOQ_SUB_FETCH_OBJECT) ==
                  moq::sub_fetch_item_kind::object);

        MOQ_CHECK(moq::to_c(moq::sub_status_result_kind::ok) ==
                  MOQ_SUB_STATUS_OK);
        MOQ_CHECK(moq::to_c(moq::sub_status_result_kind::error) ==
                  MOQ_SUB_STATUS_ERROR);
        MOQ_CHECK(moq::sub_status_result_kind_from_c(MOQ_SUB_STATUS_ERROR) ==
                  moq::sub_status_result_kind::error);
    }

    // subscription: invalid default
    {
        moq::subscription inv;
        MOQ_CHECK(!inv.valid());
    }

    // subscription: valid packed handle
    {
        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_SUBSCRIPTION, 1, 1, 0);
        MOQ_CHECK(packed != 0);
        moq_subscription_t raw = {packed};
        moq::subscription  sub(raw);
        MOQ_CHECK(sub.valid());
        MOQ_CHECK(sub.raw()._opaque == packed);
        MOQ_CHECK(sub.hash() != 0);
        MOQ_CHECK(sub.id_for_trace() != 0);
    }

    // subscription: equality
    {
        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_SUBSCRIPTION, 1, 1, 0);
        moq_subscription_t raw = {packed};
        moq::subscription  a(raw);
        moq::subscription  b(raw);
        MOQ_CHECK(a == b);

        moq::subscription inv;
        MOQ_CHECK(a != inv);
    }

    // subscription: std::hash
    {
        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_SUBSCRIPTION, 1, 1, 0);
        moq_subscription_t          raw = {packed};
        moq::subscription           sub(raw);
        std::hash<moq::subscription> h;
        MOQ_CHECK(h(sub) == static_cast<size_t>(sub.hash()));
    }

    // subscription: usable in unordered_map
    {
        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_SUBSCRIPTION, 1, 1, 0);
        moq_subscription_t raw = {packed};
        moq::subscription  sub(raw);

        std::unordered_map<moq::subscription, int> map;
        map[sub] = 42;
        MOQ_CHECK(map[sub] == 42);
    }

    // publication
    {
        moq::publication inv;
        MOQ_CHECK(!inv.valid());

        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_PUBLICATION, 1, 1, 0);
        moq_publication_t raw = {packed};
        moq::publication  pub(raw);
        MOQ_CHECK(pub.valid());
        MOQ_CHECK(pub != inv);
    }

    // fetch_handle
    {
        moq::fetch_handle inv;
        MOQ_CHECK(!inv.valid());

        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_FETCH, 1, 1, 0);
        moq_fetch_t       raw = {packed};
        moq::fetch_handle fh(raw);
        MOQ_CHECK(fh.valid());
    }

    // announcement
    {
        moq::announcement inv;
        MOQ_CHECK(!inv.valid());

        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_ANNOUNCEMENT, 1, 1, 0);
        moq_announcement_t raw = {packed};
        moq::announcement  ann(raw);
        MOQ_CHECK(ann.valid());
    }

    // ns_sub_handle
    {
        moq::ns_sub_handle inv;
        MOQ_CHECK(!inv.valid());

        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_NAMESPACE_SUB, 1, 1, 0);
        moq_ns_sub_handle_t raw = {packed};
        moq::ns_sub_handle  nsh(raw);
        MOQ_CHECK(nsh.valid());
    }

    // track_status_handle
    {
        moq::track_status_handle inv;
        MOQ_CHECK(!inv.valid());

        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_TRACK_STATUS, 1, 1, 0);
        moq_track_status_handle_t raw = {packed};
        moq::track_status_handle  tsh(raw);
        MOQ_CHECK(tsh.valid());
    }

    // subgroup_handle
    {
        moq::subgroup_handle inv;
        MOQ_CHECK(!inv.valid());

        uint64_t packed = moq_handle_pack(
            MOQ_HANDLE_POOL_SUBGROUP, 1, 1, 0);
        moq_subgroup_handle_t raw = {packed};
        moq::subgroup_handle  sg(raw);
        MOQ_CHECK(sg.valid());
    }

    // -- moq::visit ----------------------------------------------------

    {
        using var_t = std::variant<int, double, std::string_view>;
        var_t v     = 42;

        int visited = 0;
        moq::visit(v,
                    [&](int i) { visited = i; },
                    [&](double) { visited = -1; },
                    [&](std::string_view) { visited = -2; });
        MOQ_CHECK(visited == 42);

        v = 3.14;
        moq::visit(v,
                    [&](int) { visited = -1; },
                    [&](double d) { visited = static_cast<int>(d * 100); },
                    [&](std::string_view) { visited = -2; });
        MOQ_CHECK(visited == 314);

        v = std::string_view("test");
        moq::visit(v,
                    [&](int) { visited = -1; },
                    [&](double) { visited = -2; },
                    [&](std::string_view sv) {
                        visited = static_cast<int>(sv.size());
                    });
        MOQ_CHECK(visited == 4);
    }

    // visit with auto catch-all
    {
        using var_t = std::variant<int, double>;
        var_t v     = 3.14;

        bool was_double = false;
        moq::visit(v,
                    [&](int) {},
                    [&](double) { was_double = true; });
        MOQ_CHECK(was_double);
    }

    MOQ_PASS("test_cpp_types");
    return failures;
}
