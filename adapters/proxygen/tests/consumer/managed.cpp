#include <moq/proxygen_wt_managed.h>
#include <cstdio>

int main()
{
    moq_proxygen_wt_managed_cfg_t cfg;
    moq_proxygen_wt_managed_cfg_init(&cfg);
    moq_proxygen_wt_managed_t *client = nullptr;
    // No host or callback: exercise linkage without starting a network thread.
    const auto rc = moq_proxygen_wt_managed_create(&cfg, &client);
    if (client) moq_proxygen_wt_managed_destroy(client);
    if (rc != MOQ_ERR_INVAL || client != nullptr) return 1;
    std::puts("proxygen managed consumer: invalid config rejected");
    return 0;
}
