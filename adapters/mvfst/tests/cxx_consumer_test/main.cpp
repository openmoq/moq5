#include <moq/mvfst.h>
#include <moq/mvfst.hpp>
#include <cstdio>

int main()
{
    moq_mvfst_managed_cfg_t cfg;
    moq_mvfst_managed_cfg_init(&cfg);
    std::printf("moq_mvfst C++ consumer: cfg_init OK\n");
    return 0;
}
