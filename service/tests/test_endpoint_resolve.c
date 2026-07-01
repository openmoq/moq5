/*
 * Endpoint configuration resolution (pure, no network).
 * URL × protocol × backend × version matrix, precedence rules, SNI/WT-path
 * defaults, version-offer validation, and the v0 UNSUPPORTED rejects.
 */
#include "endpoint_internal.h"
#include "test_support.h"

#include <string.h>

static int failures = 0;

static moq_bytes_t B(const char *s)
{
    moq_bytes_t b = { (const uint8_t *)s, strlen(s) };
    return b;
}

static bool bytes_eq(moq_bytes_t b, const char *s)
{
    return b.len == strlen(s) && (b.len == 0 || memcmp(b.data, s, b.len) == 0);
}

static moq_endpoint_cfg_t mkcfg(const char *url)
{
    moq_endpoint_cfg_t c;
    moq_endpoint_cfg_init(&c);
    c.url = B(url);
    return c;
}

int main(void)
{
    /* == cfg_init defaults ============================================ */
    {
        moq_endpoint_cfg_t c;
        moq_endpoint_cfg_init(&c);
        MOQ_TEST_CHECK_EQ_U64(c.struct_size, sizeof(moq_endpoint_cfg_t));
        MOQ_TEST_CHECK_EQ_INT((int)c.perspective, (int)MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK_EQ_INT((int)c.protocol, (int)MOQ_TRANSPORT_PROTOCOL_AUTO);
        MOQ_TEST_CHECK_EQ_INT((int)c.backend, (int)MOQ_TRANSPORT_BACKEND_AUTO);
        MOQ_TEST_CHECK(!c.insecure_skip_verify);
    }

    /* == Malformed configs ============================================ */
    {
        moq_endpoint_resolved_t r;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(NULL, &r),
                              (int)MOQ_ERR_INVAL);
        moq_endpoint_cfg_t c = mkcfg("moqt://relay.example");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, NULL),
                              (int)MOQ_ERR_INVAL);
        c.struct_size = 4;                              /* undersized */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
        c = mkcfg("");                                  /* empty URL */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
        c = mkcfg("ftp://relay.example");               /* unknown scheme */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
        c = mkcfg("moqt://relay.example");
        c.protocol = (moq_transport_protocol_t)99;      /* out-of-range enum */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
        c = mkcfg("moqt://relay.example");
        c.backend = (moq_transport_backend_t)99;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
        /* Incoherent optional byte spans: non-zero length with NULL data. */
        c = mkcfg("moqt://relay.example");
        c.sni = (moq_bytes_t){ NULL, 5 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
        c = mkcfg("moqt://relay.example");
        c.ca_file = (moq_bytes_t){ NULL, 5 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
        c = mkcfg("https://relay.example");
        c.wt_path = (moq_bytes_t){ NULL, 5 };
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
    }

    /* == URL scheme -> protocol (AUTO) ================================ */
    {
        moq_endpoint_resolved_t r;
        moq_endpoint_cfg_t c = mkcfg("moqt://relay.example");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
        MOQ_TEST_CHECK_EQ_INT((int)r.backend,
                              (int)MOQ_TRANSPORT_BACKEND_PICOQUIC);
        MOQ_TEST_CHECK_EQ_U64(r.url.port, 4433);        /* moqt default */

        c = mkcfg("moq://relay.example");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);

        c = mkcfg("https://relay.example/live");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);
        MOQ_TEST_CHECK_EQ_U64(r.url.port, 443);         /* https default */
    }

    /* == Explicit protocol WINS over the scheme (override knob) ====== */
    {
        moq_endpoint_resolved_t r;
        moq_endpoint_cfg_t c = mkcfg("https://relay.example");
        c.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);

        c = mkcfg("moqt://relay.example");
        c.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);

        /* The override knob re-interprets a KNOWN URL only: an unknown scheme
         * stays MOQ_ERR_INVAL even with an explicit protocol (documented in
         * endpoint.h -- host/port/path defaults derive from the parse). */
        c = mkcfg("ws://relay.example");
        c.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
    }

    /* == Backend matrix =============================================== */
    {
        moq_endpoint_resolved_t r;
        /* AUTO resolves to picoquic for RAW_QUIC -- picoquic is first in the
         * registry, so AUTO stays picoquic even when mvfst is also built. */
        moq_endpoint_cfg_t c = mkcfg("moqt://relay.example");  /* backend AUTO */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.backend,
                              (int)MOQ_TRANSPORT_BACKEND_PICOQUIC);
        /* AUTO and explicit PICOQUIC resolve to picoquic for both protocols. */
        c = mkcfg("moqt://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_PICOQUIC;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.backend,
                              (int)MOQ_TRANSPORT_BACKEND_PICOQUIC);
        c = mkcfg("https://relay.example");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.backend,
                              (int)MOQ_TRANSPORT_BACKEND_PICOQUIC);
        /* mvfst: RAW_QUIC only, and only when compiled into the service.
         * When built, explicit MVFST + moqt:// resolves to RAW_QUIC/MVFST;
         * otherwise it is UNSUPPORTED. (The exact-version constraint is a
         * connect-time check, not a resolve-time one.) */
        c = mkcfg("moqt://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_MVFST;
#ifdef MOQ_SERVICE_HAVE_MVFST_MANAGED
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
        MOQ_TEST_CHECK_EQ_INT((int)r.backend,
                              (int)MOQ_TRANSPORT_BACKEND_MVFST);
#else
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
#endif
        /* mvfst has no WebTransport facade: MVFST + WEBTRANSPORT is always
         * UNSUPPORTED, whether or not mvfst is built. */
        c = mkcfg("https://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_MVFST;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
        c = mkcfg("moqt://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_MVFST;
        c.protocol = MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);

        /* proxygen: WEBTRANSPORT only, and only when compiled into the
         * service. When built, explicit PROXYGEN + https:// resolves to
         * WEBTRANSPORT/PROXYGEN; otherwise it is UNSUPPORTED. */
        c = mkcfg("https://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_PROXYGEN;
#ifdef MOQ_SERVICE_HAVE_PROXYGEN_WT_MANAGED
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);
        MOQ_TEST_CHECK_EQ_INT((int)r.backend,
                              (int)MOQ_TRANSPORT_BACKEND_PROXYGEN);
#else
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
#endif
        /* proxygen has no raw-QUIC facade: PROXYGEN + RAW_QUIC is always
         * UNSUPPORTED, whether or not proxygen is built. */
        c = mkcfg("moqt://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_PROXYGEN;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
        c = mkcfg("https://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_PROXYGEN;
        c.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
    }

    /* == v0 rejects server perspective ================================ */
    {
        moq_endpoint_resolved_t r;
        moq_endpoint_cfg_t c = mkcfg("moqt://relay.example");
        c.perspective = MOQ_PERSPECTIVE_SERVER;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
    }

    /* == SNI default from URL host; explicit value wins =============== */
    {
        moq_endpoint_resolved_t r;
        moq_endpoint_cfg_t c = mkcfg("moqt://relay.example:9000");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK(bytes_eq(r.sni, "relay.example"));
        MOQ_TEST_CHECK_EQ_U64(r.url.port, 9000);
        c.sni = B("override.example");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK(bytes_eq(r.sni, "override.example"));
    }

    /* == WT path default: explicit > URL path > "/moq"; RAW empty ===== */
    {
        moq_endpoint_resolved_t r;
        moq_endpoint_cfg_t c = mkcfg("https://relay.example/live/room1");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK(bytes_eq(r.wt_path, "/live/room1"));

        c = mkcfg("https://relay.example");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK(bytes_eq(r.wt_path, "/moq"));

        c = mkcfg("https://relay.example/live");
        c.wt_path = B("/custom");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK(bytes_eq(r.wt_path, "/custom"));

        c = mkcfg("moqt://relay.example/live");        /* RAW: wt_path empty */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(r.wt_path.len, 0);
    }

    /* == Version offers =============================================== */
    {
        moq_endpoint_resolved_t r;
        /* Zero-init (struct_size 0) == AUTO == the full supported set. */
        moq_endpoint_cfg_t c = mkcfg("moqt://relay.example");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.policy, (int)MOQ_VERSION_POLICY_AUTO);
        /* Newest first: the client's offer order is the preference order on
         * both transports (TLS ALPN and the WT protocol field). */
        MOQ_TEST_CHECK_EQ_SIZE(r.version_count, 2);
        MOQ_TEST_CHECK_EQ_INT((int)r.versions[0], (int)MOQ_VERSION_DRAFT_18);
        MOQ_TEST_CHECK_EQ_INT((int)r.versions[1], (int)MOQ_VERSION_DRAFT_16);

        /* LIST with supported versions passes through verbatim. */
        moq_version_t both[] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_16 };
        c.versions.struct_size = sizeof(moq_version_offer_t);
        c.versions.policy = MOQ_VERSION_POLICY_LIST;
        c.versions.versions = both;
        c.versions.version_count = 2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(r.version_count, 2);
        MOQ_TEST_CHECK_EQ_INT((int)r.versions[0], (int)MOQ_VERSION_DRAFT_18);

        /* EXACT with one version. */
        c.versions.policy = MOQ_VERSION_POLICY_EXACT;
        c.versions.version_count = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(r.version_count, 1);

        /* EXACT with two versions is malformed. */
        c.versions.version_count = 2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);

        /* LIST with zero versions / NULL array is malformed. */
        c.versions.policy = MOQ_VERSION_POLICY_LIST;
        c.versions.version_count = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
        c.versions.versions = NULL;
        c.versions.version_count = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);

        /* A version this build has no profile for is UNSUPPORTED, never
         * silently downgraded (§5.2 rule 4). */
        moq_version_t fourteen = (moq_version_t)14;
        c.versions.versions = &fourteen;
        c.versions.version_count = 1;
        c.versions.policy = MOQ_VERSION_POLICY_EXACT;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
        moq_version_t mixed[] = { MOQ_VERSION_DRAFT_16, (moq_version_t)17 };
        c.versions.versions = mixed;
        c.versions.version_count = 2;
        c.versions.policy = MOQ_VERSION_POLICY_LIST;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);

        /* Undersized (but nonzero) offer struct is malformed. */
        c.versions.struct_size = 4;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);
    }

    /* == Public connect: invalid cfg / unsupported version rejected before
     * any facade or network thread exists. (Live-connect behavior is the
     * lifecycle test's territory.) == */
    {
        moq_endpoint_t *ep = (moq_endpoint_t *)0x1;
        moq_endpoint_cfg_t c = mkcfg("ftp://bad");
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&c, &ep),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(ep == NULL);

        /* A version this build has no profile for: refused at resolve, no
         * thread started. */
        static const moq_version_t v14 = (moq_version_t)14;
        ep = (moq_endpoint_t *)0x1;
        c = mkcfg("moqt://relay.example");
        c.versions.struct_size = sizeof(moq_version_offer_t);
        c.versions.policy = MOQ_VERSION_POLICY_EXACT;
        c.versions.versions = &v14;
        c.versions.version_count = 1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_connect(&c, &ep),
                              (int)MOQ_ERR_UNSUPPORTED);
        MOQ_TEST_CHECK(ep == NULL);
    }

    /* == NULL-instance defensive surface ============================== */
    {
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_state(NULL),
                              (int)MOQ_ENDPOINT_CLOSED);
        MOQ_TEST_CHECK(moq_endpoint_is_closed(NULL));
        MOQ_TEST_CHECK(!moq_endpoint_is_fatal(NULL));
        MOQ_TEST_CHECK_EQ_U64(moq_endpoint_fatal_code(NULL), 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_negotiated_version(NULL), 0);
        moq_endpoint_destroy(NULL);                     /* no-op */
        moq_endpoint_set_interrupted(NULL, true);       /* no-op */
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_stop(NULL), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_wait(NULL, 0), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_post(NULL, NULL, NULL),
                              (int)MOQ_ERR_INVAL);
    }

    /* == New result codes are wired ================================== */
    {
        MOQ_TEST_CHECK_EQ_INT(MOQ_ERR_INTERRUPTED, -13);
        MOQ_TEST_CHECK_EQ_INT(MOQ_ERR_UNSUPPORTED, -14);
        MOQ_TEST_CHECK(strcmp(moq_strerror(MOQ_ERR_INTERRUPTED),
                              "unknown error") != 0);
        MOQ_TEST_CHECK(strcmp(moq_strerror(MOQ_ERR_UNSUPPORTED),
                              "unknown error") != 0);
    }

    MOQ_TEST_PASS("endpoint_resolve");
    return failures != 0;
}
