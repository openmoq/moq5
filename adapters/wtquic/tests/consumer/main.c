/*
 * Installed-consumer smoke: find_package(libmoq COMPONENTS
 * adapter-wtquic) must resolve moq::adapter-wtquic AND its transitive
 * wtquic dependency (the imported target links wtq::msquic). The
 * references below force real symbol resolution against the installed
 * libraries.
 */

#include <moq/wtquic.h>

int main(void)
{
    moq_wtquic_conn_cfg_t cfg;

    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(cfg))
        return 1;
    wtq_stream_budget_t budget = WTQ_STREAM_BUDGET_INIT;
    if (wtq_session_stream_budget(NULL, &budget) != WTQ_ERR_INVALID_ARG)
        return 1;
    return moq_wtquic_conn_events() != NULL ? 0 : 1;
}
