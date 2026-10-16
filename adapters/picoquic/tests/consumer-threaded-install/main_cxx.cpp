#include <moq/picoquic_verify.h>

#include <cstdio>
#include <type_traits>

static_assert(std::is_same<decltype(&moq_picoquic_supports_cert_verify_callback),
                           bool (*)(void)>::value, "capability returns bool");
static_assert(std::is_same<decltype(&moq_picoquic_set_cert_verify_callback),
                           moq_result_t (*)(picoquic_quic_t *,
                                           moq_picoquic_cert_verify_fn, void *)>::value,
              "setter returns moq_result_t");

int main()
{
    if (moq_picoquic_set_cert_verify_callback(nullptr, nullptr, nullptr) != MOQ_ERR_INVAL)
        return 1;
    std::printf("delegated trust supported: %d\n",
                static_cast<int>(moq_picoquic_supports_cert_verify_callback()));
    return 0;
}
