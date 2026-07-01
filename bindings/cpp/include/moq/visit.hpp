#ifndef MOQ_VISIT_HPP
#define MOQ_VISIT_HPP

#include <utility>
#include <variant>

namespace moq {

template<typename... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};

template<typename... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

template<typename Variant, typename... Visitors>
auto visit(Variant &&v, Visitors &&...visitors)
{
    return std::visit(overloaded{std::forward<Visitors>(visitors)...},
                      std::forward<Variant>(v));
}

} // namespace moq

#endif // MOQ_VISIT_HPP
