/*
 * Compile/link smoke test: proves <moq/pico_wt.h> is usable on its
 * own — without including the internal pico_wt_adapter.h — by a
 * consumer linking only moq::adapter-pico-wt.
 *
 * Must NOT include pico_wt_adapter.h or touch the conn struct layout.
 */
#include <moq/pico_wt.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>

int main(void)
{
    moq_pico_wt_conn_cfg_t cfg;
    moq_pico_wt_conn_cfg_init_sized(&cfg, sizeof(cfg));

    /* The handle is opaque here: only a pointer type is available. */
    moq_pico_wt_conn_t *conn = (moq_pico_wt_conn_t *)0;
    (void)conn;

    /* cfg_init must set the ABI guard. */
    if (cfg.struct_size != sizeof(moq_pico_wt_conn_cfg_t))
        return 1;

    /*
     * Negative: a too-small struct_size must be rejected BEFORE any
     * config field beyond struct_size is read. We pass fake non-null
     * pointers to prove validation order — create() must fail on the
     * size check alone, so these are never dereferenced. (struct_size
     * = sizeof(uint32_t) is far below offsetof(alloc)+sizeof(alloc).)
     */
    moq_pico_wt_conn_cfg_t bad;
    bad.struct_size = (uint32_t)sizeof(bad.struct_size);
    bad.session  = (moq_session_t *)0x1;
    bad.cnx      = (picoquic_cnx_t *)0x1;
    bad.h3_ctx   = (h3zero_callback_ctx_t *)0x1;
    bad.ctrl_ctx = (h3zero_stream_ctx_t *)0x1;
    bad.alloc    = (const moq_alloc_t *)0x1;
    bad.user_ctx = (void *)0x1;

    moq_pico_wt_conn_t *bad_conn = (moq_pico_wt_conn_t *)0x1;
    if (moq_pico_wt_conn_create(&bad, &bad_conn) != -1)
        return 2;
    if (bad_conn != NULL)   /* create() must NULL *out on failure */
        return 3;

    /* Old-ABI canary: a binary compiled against the v0 header allocated a
     * moq_pico_wt_conn_cfg_t ending at `alloc`. The legacy pointer-only
     * initializer must touch ONLY that v0 prefix, never the trailing canary.
     * (RED: a memset(sizeof(*cfg)) zeroes a pointer-width into the canary.) */
    {
        struct old_cfg_v0 {
            uint32_t                struct_size;
            moq_session_t          *session;
            picoquic_cnx_t         *cnx;
            h3zero_callback_ctx_t  *h3_ctx;
            h3zero_stream_ctx_t    *ctrl_ctx;
            const moq_alloc_t      *alloc;
        };
        struct {
            struct old_cfg_v0 cfg;
            uint64_t          canary;
        } holder;
        memset(&holder, 0, sizeof(holder));
        holder.canary = 0xC0FFEE5AA5C0FFEEull;

        moq_pico_wt_conn_cfg_init((moq_pico_wt_conn_cfg_t *)&holder.cfg);

        if (holder.canary != 0xC0FFEE5AA5C0FFEEull)
            return 4;                                  /* overflowed the canary */
        if (holder.cfg.struct_size != (uint32_t)sizeof(struct old_cfg_v0))
            return 5;
        if (sizeof(struct old_cfg_v0) !=
            offsetof(moq_pico_wt_conn_cfg_t, user_ctx))
            return 6;                                  /* v0 prefix mismatch */
    }

    /* Sized initializer fully clears the current struct and stamps its size. */
    {
        moq_pico_wt_conn_cfg_t full;
        memset(&full, 0xAB, sizeof(full));
        moq_pico_wt_conn_cfg_init_sized(&full, sizeof(full));
        if (full.struct_size != (uint32_t)sizeof(full))
            return 7;
        if (full.session != NULL || full.user_ctx != NULL)
            return 8;
    }

    return 0;
}
