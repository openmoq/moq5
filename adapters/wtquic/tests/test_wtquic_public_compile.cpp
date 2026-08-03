/*
 * C++ compile/link smoke: <moq/wtquic.h> is includable and linkable
 * from C++ (extern "C" guards intact across moq and wtquic headers).
 */
#include <moq/wtquic.h>

int main()
{
    moq_wtquic_conn_cfg_t cfg;

    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(moq_wtquic_conn_cfg_t))
        return 1;
    return moq_wtquic_conn_events() != nullptr ? 0 : 1;
}
