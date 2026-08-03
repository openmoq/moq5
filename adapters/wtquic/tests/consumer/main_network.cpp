/* C++17 parity for the managed facade header: it must compile and link
 * from C++ unchanged (extern "C" guards, no keyword collisions),
 * including the unified lane interface. */

#include <moq/wtquic_network_managed.h>

int main()
{
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_t *m = nullptr;

    moq_wtquic_network_managed_cfg_init(&cfg);
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    moq_wtquic_network_managed_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    if (moq_wtquic_network_managed_create(&cfg, &m) != MOQ_ERR_INVAL)
        return 1;
    if (moq_wtquic_network_managed_lane_count(nullptr) != 0 ||
        moq_wtquic_network_managed_lane(nullptr, 0) != nullptr ||
        moq_wtquic_network_lane_next_conn(nullptr, nullptr) != nullptr ||
        moq_wtquic_network_lane_wake(nullptr) != MOQ_ERR_INVAL ||
        moq_wtquic_network_managed_conn_session(nullptr) != nullptr)
        return 1;
    return m == nullptr ? 0 : 1;
}
