/*
 * Endpoint configuration resolution (pure, no network).
 * URL × protocol × backend × version matrix, precedence rules, SNI/WT-path
 * defaults, version-offer validation, and the v0 UNSUPPORTED rejects.
 */
#include "endpoint_internal.h"
#include "test_support.h"

#include <stdlib.h>
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
        /* Pointer-only init clears/stamps ONLY the frozen v0 prefix (it cannot
         * know the caller's size), so struct_size is the v0 floor, not the
         * current sizeof -- this is what makes it overflow-safe for an old
         * caller. Every v0 field still gets its default. */
        moq_endpoint_cfg_t c;
        moq_endpoint_cfg_init(&c);
        MOQ_TEST_CHECK_EQ_U64(c.struct_size, MOQ_ENDPOINT_CFG_V0_SIZE);
        MOQ_TEST_CHECK_EQ_INT((int)c.perspective, (int)MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK_EQ_INT((int)c.protocol, (int)MOQ_TRANSPORT_PROTOCOL_AUTO);
        MOQ_TEST_CHECK_EQ_INT((int)c.backend, (int)MOQ_TRANSPORT_BACKEND_AUTO);
        MOQ_TEST_CHECK(!c.insecure_skip_verify);

        /* The sized init stamps the full current size and zeroes the tail, so
         * wt_profile is reachable and defaults to BACKEND_DEFAULT (0 = select
         * nothing), NOT an explicit CURRENT. */
        moq_endpoint_cfg_t s;
        moq_endpoint_cfg_init_sized(&s, sizeof(s));
        MOQ_TEST_CHECK_EQ_U64(s.struct_size, sizeof(moq_endpoint_cfg_t));
        MOQ_TEST_CHECK_EQ_INT((int)s.perspective, (int)MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK_EQ_U64(s.wt_profile,
                              (uint64_t)MOQ_WT_PROFILE_BACKEND_DEFAULT);
        /* Same for the handshake bound: 0 = the backend's own default. */
        MOQ_TEST_CHECK_EQ_U64(s.handshake_timeout_us, 0);
        /* The v0 floor must sit exactly at wt_profile (the first tail field). */
        MOQ_TEST_CHECK_EQ_U64(MOQ_ENDPOINT_CFG_V0_SIZE,
                              offsetof(moq_endpoint_cfg_t, wt_profile));
    }

    /* == wt_profile resolve: gate + range + backend rejection ========== *
     * An EXPLICIT profile is honored only by the wtquic-msquic backend; on any
     * other backend it is MOQ_ERR_UNSUPPORTED (not silently ignored).
     * BACKEND_DEFAULT is accepted everywhere. These use the always-compiled
     * AUTO/picoquic backend (a fixed-dialect WebTransport backend), so they run
     * regardless of whether the wtquic-msquic facade is built into this tree. */
    {
        moq_endpoint_resolved_t r;

        /* v0-sized caller (no wt_profile field): resolve defaults to
         * BACKEND_DEFAULT, never reads past the buffer. */
        moq_endpoint_cfg_t v0 = mkcfg("https://relay.example/moq");
        v0.struct_size = MOQ_ENDPOINT_CFG_V0_SIZE;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&v0, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.wt_profile,
                              (uint64_t)MOQ_WT_PROFILE_BACKEND_DEFAULT);

        /* Full-size caller, BACKEND_DEFAULT explicitly: accepted on picoquic. */
        moq_endpoint_cfg_t c = mkcfg("https://relay.example/moq");
        c.struct_size = sizeof(c);
        c.wt_profile = (uint32_t)MOQ_WT_PROFILE_BACKEND_DEFAULT;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.wt_profile,
                              (uint64_t)MOQ_WT_PROFILE_BACKEND_DEFAULT);

        /* An EXPLICIT selection on picoquic (fixed dialect) is rejected -- the
         * public contract must not accept a profile it will not apply. */
        c.wt_profile = (uint32_t)MOQ_WT_PROFILE_CURRENT;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
        c.wt_profile = (uint32_t)MOQ_WT_PROFILE_D13_14_COMPAT;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);

        /* Out-of-range profile is a clean INVAL (checked before the backend
         * rejection: malformed input is INVAL, not UNSUPPORTED). */
        c.wt_profile = 7;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);

        /* A struct_size that only PARTIALLY covers wt_profile must not read it
         * (complete-presence gate): treated as absent -> BACKEND_DEFAULT, so a
         * poisoned explicit value is neither applied nor rejected. */
        moq_endpoint_cfg_t part = mkcfg("https://relay.example/moq");
        part.wt_profile = (uint32_t)MOQ_WT_PROFILE_D13_14_COMPAT;
        part.struct_size =
            (uint32_t)(offsetof(moq_endpoint_cfg_t, wt_profile) + 1);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&part, &r),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.wt_profile,
                              (uint64_t)MOQ_WT_PROFILE_BACKEND_DEFAULT);

#ifdef MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED
        /* The selectable backend accepts an explicit selection and resolves it
         * verbatim (built only where the facade is compiled in). */
        moq_endpoint_cfg_t w = mkcfg("https://relay.example/moq");
        w.struct_size = sizeof(w);
        w.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC;
        w.wt_profile = (uint32_t)MOQ_WT_PROFILE_D13_14_COMPAT;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&w, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.wt_profile,
                              (uint64_t)MOQ_WT_PROFILE_D13_14_COMPAT);
        w.wt_profile = (uint32_t)MOQ_WT_PROFILE_BACKEND_DEFAULT;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&w, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.wt_profile,
                              (uint64_t)MOQ_WT_PROFILE_BACKEND_DEFAULT);
#endif
    }

    /* == handshake_timeout_us resolve: gate + backend rejection ======= *
     * The bound is applied on the QUIC context by the picoquic backend and by
     * no other. AUTO resolves to picoquic here unconditionally -- that is the
     * resolver's stable selection, NOT a claim that the picoquic facade is
     * compiled into every build; connect() reports UNSUPPORTED when it is not.
     * 0 means "the backend's own default" and is accepted everywhere. */
    {
        moq_endpoint_resolved_t r;

        /* v0-sized caller (no field): 0, and resolve never reads past it. */
        moq_endpoint_cfg_t v0 = mkcfg("moqt://relay.example:4433");
        v0.struct_size = MOQ_ENDPOINT_CFG_V0_SIZE;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&v0, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.handshake_timeout_us, 0);

        /* Full-size caller on picoquic: carried through verbatim. */
        moq_endpoint_cfg_t c;
        moq_endpoint_cfg_init_sized(&c, sizeof(c));
        c.url = B("moqt://relay.example:4433");
        c.handshake_timeout_us = 5000000ull;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.handshake_timeout_us, 5000000ull);

        /* Explicit 0 stays 0 (backend default), not a smuggled value. */
        c.handshake_timeout_us = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.handshake_timeout_us, 0);

        /* A struct_size that only PARTIALLY covers the field must not read it
         * (complete-presence gate): treated as absent -> backend default. */
        moq_endpoint_cfg_t part;
        moq_endpoint_cfg_init_sized(&part, sizeof(part));
        part.url = B("moqt://relay.example:4433");
        part.handshake_timeout_us = 5000000ull;
        part.struct_size = (uint32_t)(
            offsetof(moq_endpoint_cfg_t, handshake_timeout_us) + 1);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&part, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.handshake_timeout_us, 0);

#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
        /* A backend with no handshake bound REJECTS a non-zero value rather
         * than silently ignoring it; zero is still fine there. */
        moq_endpoint_cfg_t m;
        moq_endpoint_cfg_init_sized(&m, sizeof(m));
        m.url = B("moqt://relay.example:4433");
        m.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
        m.handshake_timeout_us = 5000000ull;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&m, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
        m.handshake_timeout_us = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&m, &r), (int)MOQ_OK);
#endif
    }

    /* == handshake_timeout_us upper boundary ========================== *
     * The bound becomes an ABSOLUTE deadline (connection start + duration) in
     * the backend, so a value with no headroom above that clock would wrap the
     * unsigned sum and mean the opposite of a long wait. The accepted range is
     * closed at MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US; past it is MOQ_ERR_INVAL,
     * never a clamp. */
    {
        moq_endpoint_resolved_t r;
        moq_endpoint_cfg_t c;
        moq_endpoint_cfg_init_sized(&c, sizeof(c));
        c.url = B("moqt://relay.example:4433");

        /* The named maximum itself is INSIDE the range and survives verbatim --
         * this is what makes the reject a boundary rather than a cap. */
        c.handshake_timeout_us = MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.handshake_timeout_us,
                              MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US);

        /* One past it. The addition is exact in uint64_t (the maximum is
         * INT64_MAX, so +1 is 2^63), not an expression that itself overflows. */
        c.handshake_timeout_us = MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US + 1ull;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);

        /* And the value a caller would most naturally reach for as "no bound",
         * which is precisely the one that wraps. */
        c.handshake_timeout_us = UINT64_MAX;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_INVAL);

        /* Range is checked BEFORE the backend capability answer: out of range
         * reads as INVAL everywhere, not UNSUPPORTED on some backends. Proven
         * on a non-picoquic backend, which rejects even an IN-range non-zero
         * value with UNSUPPORTED -- so the two answers are distinguishable. */
#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
        moq_endpoint_cfg_t m2;
        moq_endpoint_cfg_init_sized(&m2, sizeof(m2));
        m2.url = B("moqt://relay.example:4433");
        m2.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
        m2.handshake_timeout_us = UINT64_MAX;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&m2, &r),
                              (int)MOQ_ERR_INVAL);
#endif

        /* An ABSENT field is never range-read: a v0-sized caller carrying
         * out-of-range bytes in memory it does not declare still resolves. */
        moq_endpoint_cfg_t v0b;
        moq_endpoint_cfg_init_sized(&v0b, sizeof(v0b));
        v0b.url = B("moqt://relay.example:4433");
        v0b.handshake_timeout_us = UINT64_MAX;
        v0b.struct_size = MOQ_ENDPOINT_CFG_V0_SIZE;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&v0b, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.handshake_timeout_us, 0);

        /* Same for a PARTIALLY covered field: not read, so not ranged. */
        v0b.struct_size = (uint32_t)(
            offsetof(moq_endpoint_cfg_t, handshake_timeout_us) + 1);
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&v0b, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(r.handshake_timeout_us, 0);
    }

    /* == old-caller overflow guard (v0-sized HEAP buffer) ============= *
     * Simulate a binary compiled against the pre-wt_profile header: a heap
     * allocation of exactly the v0 size. The pointer-only init must not write
     * past it, and resolve must not read past it. Under ASan either violation is
     * a hard heap-buffer-overflow, so this is the load-bearing forward-compat
     * check, not just an equality assertion. */
    {
        void *raw = malloc(MOQ_ENDPOINT_CFG_V0_SIZE);
        MOQ_TEST_CHECK(raw != NULL);
        if (raw != NULL) {
            moq_endpoint_cfg_t *old = (moq_endpoint_cfg_t *)raw;
            moq_endpoint_cfg_init(old);      /* writes exactly V0_SIZE bytes */
            MOQ_TEST_CHECK_EQ_U64(old->struct_size, MOQ_ENDPOINT_CFG_V0_SIZE);
            old->url = B("https://relay.example/moq");
            moq_endpoint_resolved_t r;
            MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(old, &r),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(r.wt_profile,
                                  (uint64_t)MOQ_WT_PROFILE_BACKEND_DEFAULT);
            MOQ_TEST_CHECK_EQ_U64(r.handshake_timeout_us, 0);
            free(raw);
        }
    }

    /* == old-caller overflow guard (PRECEDING-layout HEAP buffer) ===== *
     * The generation directly before handshake_timeout_us: v0 PLUS wt_profile,
     * i.e. a heap allocation of exactly offsetof(..., handshake_timeout_us)
     * bytes, so the buffer ENDS where the new field begins. A shortened
     * struct_size on a full-size struct would not prove this -- the bytes would
     * still be there to read. Under ASan a read of the new field here is a hard
     * heap-buffer-overflow, which is what makes this the load-bearing check.
     * Kept beside the frozen-v0 canary above, which covers the older
     * generation; both must hold. */
    {
        const size_t old_size = offsetof(moq_endpoint_cfg_t,
                                         handshake_timeout_us);
        /* The preceding layout really is shorter than the current one. */
        MOQ_TEST_CHECK(old_size < sizeof(moq_endpoint_cfg_t));
        void *raw = malloc(old_size);
        MOQ_TEST_CHECK(raw != NULL);
        if (raw != NULL) {
            moq_endpoint_cfg_t *old = (moq_endpoint_cfg_t *)raw;
            /* Sized init writes exactly old_size bytes -- never the new field. */
            moq_endpoint_cfg_init_sized(old, old_size);
            MOQ_TEST_CHECK_EQ_U64(old->struct_size, (uint64_t)old_size);
            old->url = B("https://relay.example/moq");

            moq_endpoint_resolved_t r;
            /* wt_profile behavior is unchanged at this size: it is fully
             * covered, so an EXPLICIT profile is still READ and still rejected
             * on a backend that cannot select a dialect (AUTO -> picoquic).
             * That reject is also what proves the field is read here. */
            old->wt_profile = (uint32_t)MOQ_WT_PROFILE_D13_14_COMPAT;
            MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(old, &r),
                                  (int)MOQ_ERR_UNSUPPORTED);

            /* And with no explicit selection it resolves, with the new field
             * absent -> the backend's own default. */
            old->wt_profile = (uint32_t)MOQ_WT_PROFILE_BACKEND_DEFAULT;
            MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(old, &r),
                                  (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_U64(r.wt_profile,
                                  (uint64_t)MOQ_WT_PROFILE_BACKEND_DEFAULT);
            MOQ_TEST_CHECK_EQ_U64(r.handshake_timeout_us, 0);
            free(raw);
        }
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
        /* AUTO resolves DIRECTLY to picoquic for RAW_QUIC -- a stable
         * default, never a first-available scan, so AUTO stays picoquic
         * no matter which opt-in backends are also built. */
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

        /* MsQuic: RAW_QUIC only, and only when compiled into the service.
         * It is opt-in -- reached only by an explicit backend = MSQUIC; AUTO
         * stays picoquic (pinned above). When built, explicit MSQUIC + moqt://
         * resolves to RAW_QUIC/MSQUIC; otherwise it is UNSUPPORTED. (The
         * exact-version constraint is a connect-time check, not resolve-time.) */
        c = mkcfg("moqt://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
#ifdef MOQ_SERVICE_HAVE_MSQUIC_MANAGED
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_RAW_QUIC);
        MOQ_TEST_CHECK_EQ_INT((int)r.backend,
                              (int)MOQ_TRANSPORT_BACKEND_MSQUIC);
#else
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
#endif
        /* MsQuic has no WebTransport facade: MSQUIC + WEBTRANSPORT is always
         * UNSUPPORTED, whether or not MsQuic is built. */
        c = mkcfg("https://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
        c = mkcfg("moqt://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_MSQUIC;
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

        /* wtquic-Network: WEBTRANSPORT only, and only when compiled into the
         * service (Apple-only). Opt-in -- reached only by an explicit
         * backend = WTQUIC_NETWORK; AUTO WEBTRANSPORT stays pico_wt. When
         * built, explicit WTQUIC_NETWORK + https:// resolves to
         * WEBTRANSPORT/WTQUIC_NETWORK; otherwise it is UNSUPPORTED. */
        c = mkcfg("https://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
#ifdef MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);
        MOQ_TEST_CHECK_EQ_INT((int)r.backend,
                              (int)MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK);
#else
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
#endif
        /* The wtquic Network client has no raw-QUIC facade: WTQUIC_NETWORK
         * + RAW_QUIC is always UNSUPPORTED, whether or not it is built. */
        c = mkcfg("moqt://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
        c = mkcfg("https://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
        c.protocol = MOQ_TRANSPORT_PROTOCOL_RAW_QUIC;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);

        /* wtquic-MsQuic: WEBTRANSPORT only, and only when compiled into the
         * service (cross-platform). Opt-in -- reached only by an explicit
         * backend = WTQUIC_MSQUIC; AUTO WEBTRANSPORT stays pico_wt (pinned
         * above). When built, explicit WTQUIC_MSQUIC + https:// resolves to
         * WEBTRANSPORT/WTQUIC_MSQUIC; otherwise it is UNSUPPORTED. */
        c = mkcfg("https://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC;
#ifdef MOQ_SERVICE_HAVE_WTQUIC_MSQUIC_MANAGED
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)r.protocol,
                              (int)MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT);
        MOQ_TEST_CHECK_EQ_INT((int)r.backend,
                              (int)MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC);
#else
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
#endif
        /* The wtquic MsQuic client has no raw-QUIC facade: WTQUIC_MSQUIC +
         * RAW_QUIC is always UNSUPPORTED, whether or not it is built. */
        c = mkcfg("moqt://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC;
        MOQ_TEST_CHECK_EQ_INT((int)moq_endpoint_resolve_cfg(&c, &r),
                              (int)MOQ_ERR_UNSUPPORTED);
        c = mkcfg("https://relay.example");
        c.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC;
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

#ifdef MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED
    /* == wtquic-Network terminal classification (pure, synthetic) ====== *
     * Provenance mapping with the native detail preserved for EVERY
     * captured record -- no OSStatus allowlist, and no codeless
     * TRANSPORT collapse for POSIX/DNS/BACKEND records. */
    {
        struct {
            uint32_t domain;
            int64_t code;
            moq_endpoint_terminal_reason_t want;
        } cases[] = {
            { (uint32_t)WTQ_ERRDOM_NW_TRUST, -67843,
              MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE },
            { (uint32_t)WTQ_ERRDOM_NW_TLS, -9836,
              MOQ_ENDPOINT_TERMINAL_TLS },
            { (uint32_t)WTQ_ERRDOM_NW_POSIX, 54 /* ECONNRESET */,
              MOQ_ENDPOINT_TERMINAL_TRANSPORT },
            { (uint32_t)WTQ_ERRDOM_NW_DNS, 8,
              MOQ_ENDPOINT_TERMINAL_TRANSPORT },
            { (uint32_t)WTQ_ERRDOM_BACKEND, -1,
              MOQ_ENDPOINT_TERMINAL_TRANSPORT },
        };
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            wtq_transport_error_t rec;
            memset(&rec, 0, sizeof(rec));
            rec.struct_size = (uint32_t)sizeof(rec);
            rec.kind = 3; /* WTQ_ERR_KIND_LOCAL */
            rec.native_domain = cases[i].domain;
            rec.native_code = cases[i].code;
            moq_endpoint_terminal_reason_t reason;
            uint64_t detail = 0;
            moq_endpoint_classify_wtquic_network(true, 0, false, true, &rec,
                                        &reason, &detail);
            MOQ_TEST_CHECK_EQ_INT((int)reason, (int)cases[i].want);
            /* the raw native bits survive, sign included */
            MOQ_TEST_CHECK((int64_t)detail == cases[i].code);
        }
        /* no captured record: the generic fallback rules hold */
        moq_endpoint_terminal_reason_t reason;
        uint64_t detail = 0xAA;
        moq_endpoint_classify_wtquic_network(true, 7, false, false, NULL,
                                    &reason, &detail);
        MOQ_TEST_CHECK_EQ_INT((int)reason,
                              (int)MOQ_ENDPOINT_TERMINAL_PROTOCOL);
        MOQ_TEST_CHECK_EQ_U64(detail, 7);
        moq_endpoint_classify_wtquic_network(true, 0, false, false, NULL,
                                    &reason, &detail);
        MOQ_TEST_CHECK_EQ_INT((int)reason,
                              (int)MOQ_ENDPOINT_TERMINAL_TRANSPORT);
        MOQ_TEST_CHECK_EQ_U64(detail, 0);
        moq_endpoint_classify_wtquic_network(false, 0, true, false, NULL,
                                    &reason, &detail);
        MOQ_TEST_CHECK_EQ_INT((int)reason,
                              (int)MOQ_ENDPOINT_TERMINAL_CLEAN);
        /* a NONE-domain record never claims a native detail */
        wtq_transport_error_t none;
        memset(&none, 0, sizeof(none));
        none.struct_size = (uint32_t)sizeof(none);
        moq_endpoint_classify_wtquic_network(true, 0, false, true, &none,
                                    &reason, &detail);
        MOQ_TEST_CHECK_EQ_INT((int)reason,
                              (int)MOQ_ENDPOINT_TERMINAL_TRANSPORT);
        MOQ_TEST_CHECK_EQ_U64(detail, 0);
    }
#endif /* MOQ_SERVICE_HAVE_WTQUIC_NETWORK_MANAGED */

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
