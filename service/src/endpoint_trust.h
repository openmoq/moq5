/*
 * endpoint_trust.h -- the INTERNAL delegated-trust connect route.
 *
 * NOT INSTALLED, and no MOQ_API. `service/CMakeLists.txt` installs only
 * `include/moq`, so this header reaches the JNI bridge (which compiles into the
 * same one-DSO image) and nothing else. A trust callback in the public
 * `moq_endpoint_cfg_t` would instead be an application-supplied native
 * certificate callback; here an application cannot supply one -- it is not in a
 * header they receive and not an exported symbol.
 *
 * `moq_endpoint_connect()` is this route with `fn == NULL`, so there is one
 * implementation and one lifecycle. Everything below is the contract it
 * provides.
 */
#ifndef MOQ_SERVICE_ENDPOINT_TRUST_H
#define MOQ_SERVICE_ENDPOINT_TRUST_H

#include <moq/endpoint.h>
#include <moq/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * The trust decision, in SERVICE-TIER vocabulary.
 *
 * Deliberately not the adapter's `moq_picoquic_cert_verify_fn`: the service
 * tier is transport-agnostic, and naming a picoquic type here would leak a
 * backend into it even internally. The two have the same shape, and the one
 * translation site is the picoquic create path.
 *
 * Zero is REJECT so a default-initialised or unrecognised verdict fails closed.
 * `server_name` and `certificates` are BORROWED for the call only, and
 * `certificates` is leaf-first DER.
 */
typedef enum moq_endpoint_trust_verdict {
    MOQ_ENDPOINT_TRUST_REJECT = 0,
    MOQ_ENDPOINT_TRUST_ACCEPT = 1
} moq_endpoint_trust_verdict_t;

typedef moq_endpoint_trust_verdict_t (*moq_endpoint_trust_fn)(
    void *ctx,
    moq_bytes_t server_name,
    const moq_bytes_t *certificates,
    size_t certificate_count);

/*
 * Connect with a library-owned trust delegate.
 *
 * `moq_endpoint_connect()` IS this with `fn == NULL`: one implementation, no
 * second lifecycle, and observably identical behaviour at every supported
 * `struct_size`.
 *
 * `ctx` is opaque and MAY be NULL: a stateless delegate needs no context.
 *
 * Validation, all fail-closed and all before an endpoint exists:
 *
 *   fn && cfg->insecure_skip_verify   -> MOQ_ERR_INVAL       (insecure must not
 *                                        win silently over requested trust)
 *   fn && cfg->ca_file.len            -> MOQ_ERR_INVAL       (two trust sources)
 *   fn && provider cannot apply it    -> MOQ_ERR_UNSUPPORTED (never ignored)
 *
 * The delegate must not become a hostname escape hatch: reference-identity
 * checking is performed inside MOQ5's own implementation of the delegate,
 * before any platform bridge is consulted. The auth type that implementation
 * hands the platform is a protocol-derived constant (the TLS 1.3 key-exchange
 * value), never something read off the certificates -- nothing about it
 * crosses this boundary.
 */
moq_result_t moq_endpoint_connect_with_trust(const moq_endpoint_cfg_t *cfg,
                                             moq_endpoint_trust_fn fn,
                                             void *ctx,
                                             moq_endpoint_t **out);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_SERVICE_ENDPOINT_TRUST_H */
