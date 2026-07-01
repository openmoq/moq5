#include <moq/sim.h>

#include <cstdint>
#include <cstdlib>

static void *cpp_alloc(std::size_t size, void *ctx)
{
    (void)ctx;
    return std::malloc(size);
}

static void *cpp_realloc(void *ptr, std::size_t old_size,
                         std::size_t new_size, void *ctx)
{
    (void)old_size;
    (void)ctx;
    return std::realloc(ptr, new_size);
}

static void cpp_free(void *ptr, std::size_t size, void *ctx)
{
    (void)size;
    (void)ctx;
    std::free(ptr);
}

int main()
{
    moq_alloc_t alloc = { nullptr, cpp_alloc, cpp_realloc, cpp_free };

    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &alloc;
    cfg.seed = 7;
    cfg.initial_now_us = 11;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 3;

    moq_simpair_t *sp = nullptr;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK)
        return 1;
    if (moq_simpair_seed(sp) != 7)
        return 2;
    if (moq_simpair_now_us(sp) != 11)
        return 3;
    if (moq_simpair_start(sp) != MOQ_OK)
        return 4;
    if (moq_simpair_run_until_quiescent(sp, 4, nullptr) != MOQ_OK)
        return 5;

    moq_simpair_destroy(sp);
    return 0;
}
