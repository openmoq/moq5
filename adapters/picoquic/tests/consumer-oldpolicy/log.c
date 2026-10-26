#include <picoquic.h>
#include <stdio.h>

/* picoquic_set_qlog lives in picoquic-log, and its object depends on
 * picoquic-core. It is declared here from its upstream prototype (autoqlog.h)
 * because an installed PicoQUIC ships autoqlog.h without the picoquic_qlog.h
 * it includes; this probe is about the link, not that header. */
int picoquic_set_qlog(picoquic_quic_t *quic, char const *qlog_dir);

static int (*volatile keep_set_qlog)(picoquic_quic_t *, char const *) =
    picoquic_set_qlog;

int main(void)
{
    if (keep_set_qlog == NULL)
        return 1;
    printf("PASS: oldpolicy_log\n");
    return 0;
}
