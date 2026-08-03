/*
 * Compile/link smoke test: proves <moq/wtquic.h> is usable on its own
 * by a consumer linking only moq::adapter-wtquic.
 */
#include <moq/wtquic.h>

#include <stddef.h>
#include <stdint.h>

int main(void)
{
    moq_wtquic_conn_cfg_t cfg;

    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    if (cfg.struct_size != sizeof(moq_wtquic_conn_cfg_t))
        return 1;

    /* the event table is a real linked symbol, not a macro */
    if (moq_wtquic_conn_events() == NULL)
        return 1;

    /* opaque handle: only the pointer type is available here */
    moq_wtquic_conn_t *conn = (moq_wtquic_conn_t *)0;
    (void)conn;

    /*
     * Negative: a too-small struct_size must be rejected BEFORE any
     * field beyond struct_size is read — the fake non-null pointers
     * prove validation order (they are never dereferenced).
     */
    moq_wtquic_conn_cfg_t bad;
    bad.struct_size = (uint32_t)sizeof(bad.struct_size);
    bad.alloc = (const moq_alloc_t *)0x1;
    bad.session = (moq_session_t *)0x1;
    bad.hook = 0;
    bad.hook_user = 0;
    moq_wtquic_conn_t *out = (moq_wtquic_conn_t *)0x1;
    if (moq_wtquic_conn_create(&bad, &out) >= 0)
        return 1;
    if (out != NULL)
        return 1;

    /* NULL-tolerant teardown and queries */
    moq_wtquic_conn_destroy(NULL);
    if (moq_wtquic_conn_session(NULL) != NULL)
        return 1;
    if (moq_wtquic_conn_wtq_session(NULL) != NULL)
        return 1;
    if (moq_wtquic_conn_is_fatal(NULL) || moq_wtquic_conn_is_closed(NULL))
        return 1;
    return 0;
}
