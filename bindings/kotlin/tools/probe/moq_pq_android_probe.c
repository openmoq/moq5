/*
 * MoQ5 Android Phase 0.2 -- the cross-build link probe.
 *
 * This is NOT a synthetic "does the compiler run" check. It compiles the
 * repository's OWN private-header consumer, adapters/common/moq_pq_stream_backlog.h,
 * which reads picoquic's internal stream state (picoquic_stream_head_t fields
 * such as reset_sent / is_active) through <picoquic_internal.h>. That header is
 * exactly why source-tree picoquic at the pinned revision is mandatory:
 * adapters/picoquic/CMakeLists.txt:159-171 makes the installed CONFIG package a
 * FATAL_ERROR, and cmake/FindPicoquic.cmake:152-171 ignores picoquic_DIR
 * entirely when a drain-capable adapter is enabled.
 *
 * The probe then takes the address of ONE symbol that is defined in exactly one
 * of the eight required archives, so every archive is semantically load-bearing:
 * replacing any of them with a valid same-ABI archive that lacks its anchor
 * makes the closed link fail with that anchor undefined. Type-only references
 * (a declared pointer, an included header) create no symbol reference and would
 * prove nothing -- an earlier revision of this probe made that mistake.
 *
 * Uniqueness of each anchor was verified with llvm-nm --defined-only across all
 * eight staged archives: each name is defined exactly once, in its own archive.
 */
#include <picoquic.h>
#include <picoquic_internal.h>
#include <autoqlog.h>
#include <h3zero_common.h>
#include <openssl/crypto.h>
#include <openssl/ssl.h>
#include <picotls.h>
#include <picotls/openssl.h>
#include <picotls/minicrypto.h>

#include "moq_pq_stream_backlog.h"

/* Link anchors. Declared as generic function pointers so the link -- not a
 * matching prototype -- is what has to resolve them. picoquic_set_qlog is a
 * second picoquic-core anchor: the qlog streaming functions live in core, so
 * no anchor here selects anything from libpicoquic-log.a. */
const void *moq_pq_android_probe_anchors[8];

int moq_pq_android_probe(picoquic_cnx_t *cnx);

int moq_pq_android_probe(picoquic_cnx_t *cnx)
{
    /* our private-header use: reads picoquic_internal.h stream state */
    int backlog_empty = moq_pq_cnx_stream_backlog_empty(cnx);

    moq_pq_android_probe_anchors[0] = (const void *)&OpenSSL_version;                    /* libcrypto.a */
    moq_pq_android_probe_anchors[1] = (const void *)&TLS_method;                         /* libssl.a */
    moq_pq_android_probe_anchors[2] = (const void *)&ptls_hkdf_extract;                  /* libpicotls-core.a */
    moq_pq_android_probe_anchors[3] = (const void *)&ptls_openssl_init_sign_certificate; /* libpicotls-openssl.a */
    moq_pq_android_probe_anchors[4] = (const void *)&ptls_minicrypto_random_bytes;       /* libpicotls-minicrypto.a */
    moq_pq_android_probe_anchors[5] = (const void *)&picoquic_create;                    /* libpicoquic-core.a */
    moq_pq_android_probe_anchors[6] = (const void *)&picoquic_set_qlog;                  /* libpicoquic-core.a (qlog) */
    moq_pq_android_probe_anchors[7] = (const void *)&h3zero_callback;                    /* libpicohttp-core.a */

    return backlog_empty;
}
