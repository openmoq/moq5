/*
 * moq5_trust_jni_fixture.c -- TEST-ONLY substrate for the Android trust bridge.
 *
 * This is not shipped and is not reachable from any product image: it is built
 * only by tools/build-bridge-testdsos.sh into its own host shared object, and
 * the Android topology links neither this file nor its directory.
 *
 * What it exists to pin, ahead of any public API (chats/libmoq/1301):
 *
 *  * the TRUST DECISION ORDER, fail-closed at every step;
 *  * that HOSTNAME VERIFICATION IS NATIVE AND NON-DELEGABLE -- the reference
 *    identity is checked against the leaf BEFORE the Java trust bridge is
 *    consulted at all. MOQ5's raw QUIC/picotls path has no SSLSocket or
 *    SSLEngine hostname-verifier layer beside the platform trust manager, so
 *    nothing else would perform that check. Platform chain, pinning and CT
 *    authority remain the trust manager's and are separate from this
 *    host-side substrate;
 *  * that a delegate which is absent, refuses, or throws ANYTHING is a REJECT,
 *    never an accept;
 *  * that the Android `authType` handed to the platform is the PROTOCOL-DERIVED
 *    constant for this route (see MOQ5_TRUST_AUTH_TYPE), identical for RSA and
 *    EC leaves, and never a value guessed from the leaf key;
 *  * that a foreign (non-JVM) thread attaches ONCE, not per call.
 *
 * TWO LAYERS live here, and they are tested separately:
 *
 *  1. the DECISION CORE, which takes a DER chain and a server name as plain
 *     data. It needs no TLS at all, so its rows are pure decision tests;
 *  2. the PICOTLS WRAPPER, a real `ptls_verify_certificate_t` whose callback is
 *     driven DIRECTLY -- still no handshake, no socket, no peer. It owns the
 *     decision and consults picotls' own OpenSSL verifier only after accepting,
 *     solely so picotls installs `verify_sign` / `verify_data`, which are
 *     file-static there and cannot be reimplemented.
 *
 * Deliberately NOT here: picoquic, any real handshake, and any network. Every
 * row is deterministic and needs no relay, no localhost and no timing.
 */

#include <jni.h>

#include <picotls.h>
#include <picotls/openssl.h>

#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* The verdicts. Each rejection has its OWN value: a row that could only see
 * "not accepted" could not tell a hostname mismatch from a thrown exception,
 * and those are different defects. */
#define MOQ5_TRUST_OK                  0
#define MOQ5_TRUST_ERR_NO_DELEGATE   (-1)
#define MOQ5_TRUST_ERR_EMPTY_CHAIN   (-2)
#define MOQ5_TRUST_ERR_NO_NAME       (-3)
#define MOQ5_TRUST_ERR_BAD_DER       (-4)
#define MOQ5_TRUST_ERR_HOST          (-5)
#define MOQ5_TRUST_ERR_REJECTED      (-7)
#define MOQ5_TRUST_ERR_CERT_THROW    (-8)
#define MOQ5_TRUST_ERR_OTHER_THROW   (-9)
#define MOQ5_TRUST_ERR_ATTACH       (-10)
#define MOQ5_TRUST_ERR_JNI_LIMIT    (-11)

/* The largest value a jsize can hold. Every size_t crossing into a JNI array
 * API is checked against it. */
#define JSIZE_MAX 0x7FFFFFFF

/* Resolved ONCE in JNI_OnLoad, on the loading thread. A native thread attached
 * later cannot FindClass an application class -- it gets the system loader --
 * so caching here is required, not an optimisation. The class is a global ref
 * because a local one does not survive the call that made it. */
static JavaVM *g_vm;
static jclass g_bridge;             /* global ref */
static jmethodID g_check;           /* static (chain, name, authType) -> int */
static jclass g_cert_exception;     /* global ref */

/* Attachment accounting. The contract is one attach per foreign THREAD, not per
 * call, so the counter is what a row asserts. */
static int g_attach_count;
static pthread_mutex_t g_attach_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_key_t g_attach_key;
static int g_attach_key_ready;

/* What the delegate was handed, so a row can assert the values rather than
 * infer them from the verdict. Single-threaded per row by construction. */
static char g_seen_name[256];
static char g_seen_auth[64];

static void moq5_trust_detach(void *unused)
{
    (void)unused;
    /* The thread is ending: give the JVM its attachment back. Detaching per
     * CALL instead would be the churn this fixture exists to forbid. */
    (void)(*g_vm)->DetachCurrentThread(g_vm);
}

/* Returns the env for this thread, attaching once if it is foreign. */
static JNIEnv *moq5_trust_env(int *attached_now)
{
    JNIEnv *env = NULL;
    *attached_now = 0;
    if ((*g_vm)->GetEnv(g_vm, (void **)&env, JNI_VERSION_1_6) == JNI_OK && env != NULL) {
        return env;
    }
    if ((*g_vm)->AttachCurrentThreadAsDaemon(g_vm, (void **)&env, NULL) != JNI_OK) {
        return NULL;
    }
    *attached_now = 1;
    pthread_mutex_lock(&g_attach_lock);
    g_attach_count++;
    pthread_mutex_unlock(&g_attach_lock);
    if (g_attach_key_ready) (void)pthread_setspecific(g_attach_key, (void *)1);
    return env;
}

/* The DELEGATE. Everything it can do other than returning zero is a reject. */
static int moq5_trust_call_delegate(const uint8_t *const *chain, const size_t *lens,
                                    size_t count, const char *name, const char *auth)
{
    JNIEnv *env = NULL;
    jobjectArray jchain = NULL;
    jstring jname = NULL;
    jstring jauth = NULL;
    jthrowable thrown = NULL;
    jclass byte_array_class = NULL;
    int attached_now = 0;
    int verdict;
    size_t i;

    env = moq5_trust_env(&attached_now);
    if (env == NULL) return MOQ5_TRUST_ERR_ATTACH;

    /* A picotls chain arrives with size_t lengths and a size_t count, and every
     * JNI array API takes a SIGNED jsize. Narrowing without a bound is how a
     * hostile or corrupt length becomes a negative size -- so the bound is
     * checked before any allocation, and a chain that cannot be represented is
     * refused rather than truncated. */
    if (count > (size_t)JSIZE_MAX) return MOQ5_TRUST_ERR_JNI_LIMIT;
    for (i = 0; i < count; i++) {
        if (lens[i] > (size_t)JSIZE_MAX) return MOQ5_TRUST_ERR_JNI_LIMIT;
    }

    byte_array_class = (*env)->FindClass(env, "[B");
    if (byte_array_class == NULL) {
        (*env)->ExceptionClear(env);
        return MOQ5_TRUST_ERR_ATTACH;
    }
    jchain = (*env)->NewObjectArray(env, (jsize)count, byte_array_class, NULL);
    (*env)->DeleteLocalRef(env, byte_array_class);
    if (jchain == NULL) {
        (*env)->ExceptionClear(env);
        return MOQ5_TRUST_ERR_ATTACH;
    }
    for (i = 0; i < count; i++) {
        jbyteArray der = (*env)->NewByteArray(env, (jsize)lens[i]);
        if (der == NULL) {
            (*env)->ExceptionClear(env);
            (*env)->DeleteLocalRef(env, jchain);
            return MOQ5_TRUST_ERR_ATTACH;
        }
        (*env)->SetByteArrayRegion(env, der, 0, (jsize)lens[i], (const jbyte *)chain[i]);
        (*env)->SetObjectArrayElement(env, jchain, (jsize)i, der);
        /* Both can raise -- a bad index or an array-store violation -- and a
         * pending exception makes every later JNI call undefined. */
        if ((*env)->ExceptionCheck(env)) {
            (*env)->ExceptionClear(env);
            (*env)->DeleteLocalRef(env, der);
            (*env)->DeleteLocalRef(env, jchain);
            return MOQ5_TRUST_ERR_ATTACH;
        }
        /* Released immediately: a chain of any length must not grow the local
         * reference table, which has a small guaranteed capacity. */
        (*env)->DeleteLocalRef(env, der);
    }
    jname = (*env)->NewStringUTF(env, name);
    jauth = (*env)->NewStringUTF(env, auth);
    if (jname == NULL || jauth == NULL) {
        (*env)->ExceptionClear(env);
        if (jname != NULL) (*env)->DeleteLocalRef(env, jname);
        if (jauth != NULL) (*env)->DeleteLocalRef(env, jauth);
        (*env)->DeleteLocalRef(env, jchain);
        return MOQ5_TRUST_ERR_ATTACH;
    }

    verdict = (int)(*env)->CallStaticIntMethod(env, g_bridge, g_check, jchain, jname, jauth);

    /* An exception is the EXPECTED refusal shape, so it is classified before
     * the return value is trusted -- a pending throwable makes that value
     * meaningless. Nothing is allowed to cross back into C. */
    if ((*env)->ExceptionCheck(env)) {
        thrown = (*env)->ExceptionOccurred(env);
        (*env)->ExceptionClear(env);
        verdict = MOQ5_TRUST_ERR_OTHER_THROW;
        if (thrown != NULL) {
            if ((*env)->IsInstanceOf(env, thrown, g_cert_exception)) {
                verdict = MOQ5_TRUST_ERR_CERT_THROW;
            }
            (*env)->DeleteLocalRef(env, thrown);
        }
    } else if (verdict != 0) {
        verdict = MOQ5_TRUST_ERR_REJECTED;
    }

    (*env)->DeleteLocalRef(env, jauth);
    (*env)->DeleteLocalRef(env, jname);
    (*env)->DeleteLocalRef(env, jchain);
    return verdict;
}

/* The AUTH TYPE handed to the platform: a PROTOCOL-DERIVED constant, not a
 * property of the leaf.
 *
 * Android's server-side `X509TrustManager.checkServerTrusted` parameter is the
 * KEY-EXCHANGE algorithm of the negotiated cipher suite, not the certificate's
 * public-key algorithm. A QUIC client never negotiates anything older than TLS
 * 1.3 (RFC 9001 section 4.2), and TLS 1.3 cipher suites imply no specific
 * key-exchange method, which is why AOSP/Conscrypt names `GENERIC` as the
 * server authType for every TLS 1.3 suite. Deriving `RSA` / `EC` / `Ed25519`
 * from the leaf key or the CertificateVerify signature would therefore ask the
 * platform to evaluate a different policy from the one it applies to a real
 * TLS 1.3 connection -- and it would be a guess, because the callback receives
 * no cipher-suite input. A transport that cannot prove this contract must
 * expose an authoritative value at its own boundary or be refused. */
#define MOQ5_TRUST_AUTH_TYPE "GENERIC"

/*
 * The decision, in the ONE order this design permits.
 *
 * The native name check precedes the delegate deliberately: it is the step that
 * may never be delegated, so it is placed where no delegate outcome can skip
 * it. Every step below fails closed.
 */
static int moq5_trust_decide(const uint8_t *const *chain, const size_t *lens, size_t count,
                             const char *name, int use_delegate)
{
    X509 *leaf = NULL;
    const unsigned char *p;
    const char *auth;
    int rc;

    if (!use_delegate)                 return MOQ5_TRUST_ERR_NO_DELEGATE;
    if (count == 0)                    return MOQ5_TRUST_ERR_EMPTY_CHAIN;
    if (name == NULL || name[0] == 0)  return MOQ5_TRUST_ERR_NO_NAME;

    p = (const unsigned char *)chain[0];
    leaf = d2i_X509(NULL, &p, (long)lens[0]);
    if (leaf == NULL) return MOQ5_TRUST_ERR_BAD_DER;

    /* The reference identity is checked with the SAME distinction picotls'
     * own OpenSSL verifier makes: an IP literal is matched against iPAddress
     * SANs, a DNS name against dNSName SANs. Checking an IP with the host path
     * would compare it as a string against dNSName entries, which is not the
     * same question and would reject -- or, with a certificate carrying a
     * matching dNSName text, wrongly accept. `ptls_server_name_is_ipaddr` is
     * picotls' own public detector, so the two layers cannot drift. */
    if (ptls_server_name_is_ipaddr(name)) {
        if (X509_check_ip_asc(leaf, name, 0) != 1) {
            X509_free(leaf);
            return MOQ5_TRUST_ERR_HOST;
        }
    } else if (X509_check_host(leaf, name, strlen(name), 0, NULL) != 1) {
        X509_free(leaf);
        return MOQ5_TRUST_ERR_HOST;
    }

    /* Chosen AFTER the leaf parsed and the identity matched, and chosen the
     * same way for every key family: the platform's question is about the TLS
     * 1.3 key exchange, which the leaf cannot answer. */
    auth = MOQ5_TRUST_AUTH_TYPE;

    (void)snprintf(g_seen_name, sizeof g_seen_name, "%s", name);
    (void)snprintf(g_seen_auth, sizeof g_seen_auth, "%s", auth);

    rc = moq5_trust_call_delegate(chain, lens, count, name, auth);
    X509_free(leaf);
    return rc;
}

/* -- the picotls wrapper --------------------------------------------------- *
 *
 * The shape signed off in chats/libmoq/1303: OUR verifier owns the decision,
 * and picotls' OpenSSL verifier is a private delegate consulted ONLY after that
 * decision accepts -- solely so picotls installs its own `verify_sign` /
 * `verify_data`. That step cannot be reimplemented: `verify_sign` is file-static
 * in picotls' openssl.c and is not exported, so a fully custom verifier could
 * not complete a handshake at all.
 *
 * Two properties fall out of taking `server_name` from the CALLBACK parameter
 * rather than from `ptls_get_server_name(tls)`:
 *
 *  * the ECH divergence picotls documents at picotls.h:800-801 -- where the two
 *    can differ after a rejected ECH offer -- cannot arise here;
 *  * the delegate's accept-override is reachable ONLY from a path that has
 *    already accepted, which is what makes its unconditional success safe
 *    rather than a blanket "trust everything".
 */

typedef struct moq5_trust_verifier {
    ptls_verify_certificate_t super;           /* what picoquic installs */
    ptls_openssl_verify_certificate_t ossl;    /* PRIVATE delegate */
    ptls_openssl_override_verify_certificate_t accept_override;
    int use_delegate;                          /* is the Java bridge present */
    int ossl_calls;                            /* how often the delegate ran */
    int last_verdict;
    int last_alert;                            /* what picotls was told */
    int accept_installed;                      /* verify_sign AND verify_data */
    int outputs_untouched;                     /* the seeds survived a reject */
} moq5_trust_verifier_t;

static moq5_trust_verifier_t g_verifier;

/* A never-called stand-in used only as a recognisable output-parameter seed. */
static int moq5_trust_seed_sign(void *ctx, uint16_t algo, ptls_iovec_t data, ptls_iovec_t sign)
{
    (void)ctx; (void)algo; (void)data; (void)sign;
    return PTLS_ALERT_BAD_CERTIFICATE;
}

static int moq5_trust_accept_override(ptls_openssl_override_verify_certificate_t *self,
                                      ptls_t *tls, int ret, int ossl_ret,
                                      X509 *cert, STACK_OF(X509) * chain)
{
    (void)self; (void)tls; (void)ret; (void)ossl_ret; (void)cert; (void)chain;
    /* Unconditional, and safe only because of WHERE it is reachable from: the
     * wrapper calls the delegate exclusively after its own accept. The OpenSSL
     * store is empty on Android, so this is also what stops a missing trust
     * store from vetoing a decision the platform already made. */
    return 0;
}

static int moq5_trust_verify_cb(ptls_verify_certificate_t *self, ptls_t *tls,
                                const char *server_name,
                                int (**verify_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t),
                                void **verify_data, ptls_iovec_t *certs, size_t num_certs)
{
    moq5_trust_verifier_t *v = (moq5_trust_verifier_t *)self;
    const uint8_t **bufs = NULL;
    size_t *lens = NULL;
    size_t i;
    int verdict;

    if (num_certs > 0) {
        bufs = (const uint8_t **)calloc(num_certs, sizeof *bufs);
        lens = (size_t *)calloc(num_certs, sizeof *lens);
        if (bufs == NULL || lens == NULL) {
            free((void *)bufs); free(lens);
            v->last_verdict = MOQ5_TRUST_ERR_ATTACH;
            return PTLS_ALERT_BAD_CERTIFICATE;
        }
        for (i = 0; i < num_certs; i++) {
            bufs[i] = (const uint8_t *)certs[i].base;
            lens[i] = certs[i].len;
        }
    }

    /* The decision, on the callback's OWN server_name. */
    verdict = moq5_trust_decide(bufs, lens, num_certs, server_name, v->use_delegate);
    free((void *)bufs);
    free(lens);
    v->last_verdict = verdict;
    if (verdict != MOQ5_TRUST_OK) {
        /* A rejection returns WITHOUT consulting picotls' OPENSSL VERIFIER, so
         * no accept override can run and no verify_sign is installed. Note the
         * distinction: a NAME mismatch is refused before the JAVA TRUST BRIDGE
         * too, while a Java refusal or throwable necessarily means the bridge
         * WAS consulted -- but in neither case may the OpenSSL verifier run. */
        return PTLS_ALERT_BAD_CERTIFICATE;
    }

    v->ossl_calls++;
    return v->ossl.super.cb(&v->ossl.super, tls, server_name, verify_sign, verify_data,
                            certs, num_certs);
}

/* -- certificate material, generated rather than checked in ---------------- */

static jbyteArray moq5_trust_self_signed(JNIEnv *env, const char *algo,
                                         const char *san_kind, const char *san_text)
{
    EVP_PKEY *key = NULL;
    X509 *cert = NULL;
    X509_NAME *nm = NULL;
    X509_EXTENSION *san = NULL;
    unsigned char *der = NULL;
    char san_value[300];
    jbyteArray out = NULL;
    int len;

    if (strcmp(algo, "RSA") == 0)      key = EVP_RSA_gen(2048);
    else if (strcmp(algo, "EC") == 0)  key = EVP_EC_gen("P-256");
    if (key == NULL) return NULL;

    cert = X509_new();
    if (cert == NULL) { EVP_PKEY_free(key); return NULL; }

    if (X509_set_version(cert, 2) != 1) goto done;
    if (ASN1_INTEGER_set(X509_get_serialNumber(cert), 1) != 1) goto done;
    if (X509_gmtime_adj(X509_getm_notBefore(cert), 0) == NULL) goto done;
    if (X509_gmtime_adj(X509_getm_notAfter(cert), 60 * 60 * 24) == NULL) goto done;
    if (X509_set_pubkey(cert, key) != 1) goto done;

    /* The CN is deliberately a FIXED value that matches no reference identity a
     * row uses. Putting the SAN text in the CN as well would let OpenSSL's
     * host path fall back to the CN and match an IP literal as a string --
     * which would make the DNS/IP distinction untestable, because both paths
     * would accept. Modern verification uses SANs only when present anyway, so
     * this is also the more faithful certificate. */
    nm = X509_get_subject_name(cert);
    if (X509_NAME_add_entry_by_txt(nm, "CN", MBSTRING_ASC,
                                   (const unsigned char *)"moq5-test-leaf", -1, -1, 0) != 1)
        goto done;
    if (X509_set_issuer_name(cert, nm) != 1) goto done;

    /* The SAN KIND is the caller's, never inferred from the text. Inferring it
     * would make one case unconstructible: a dNSName whose text is an IP
     * literal, which is exactly what proves an IP reference identity cannot be
     * satisfied by a DNS entry. */
    (void)snprintf(san_value, sizeof san_value, "%s:%s", san_kind, san_text);
    san = X509V3_EXT_conf_nid(NULL, NULL, NID_subject_alt_name, san_value);
    if (san == NULL) goto done;
    if (X509_add_ext(cert, san, -1) != 1) goto done;

    if (X509_sign(cert, key, EVP_sha256()) == 0) goto done;

    len = i2d_X509(cert, &der);
    if (len <= 0) goto done;
    out = (*env)->NewByteArray(env, (jsize)len);
    if (out != NULL) (*env)->SetByteArrayRegion(env, out, 0, (jsize)len, (const jbyte *)der);

done:
    if (san != NULL) X509_EXTENSION_free(san);
    if (der != NULL) OPENSSL_free(der);
    if (cert != NULL) X509_free(cert);
    EVP_PKEY_free(key);
    return out;
}

/* -- the bound methods ----------------------------------------------------- */

static jbyteArray JNICALL moq5_trust_impl_makeCert(JNIEnv *env, jclass self,
                                                   jstring algo, jstring kind, jstring san)
{
    const char *a = NULL;
    const char *k = NULL;
    const char *v = NULL;
    jbyteArray out = NULL;

    (void)self;
    if (algo == NULL || kind == NULL || san == NULL) return NULL;
    a = (*env)->GetStringUTFChars(env, algo, NULL);
    if (a == NULL) return NULL;
    k = (*env)->GetStringUTFChars(env, kind, NULL);
    if (k == NULL) { (*env)->ReleaseStringUTFChars(env, algo, a); return NULL; }
    v = (*env)->GetStringUTFChars(env, san, NULL);
    if (v == NULL) {
        (*env)->ReleaseStringUTFChars(env, kind, k);
        (*env)->ReleaseStringUTFChars(env, algo, a);
        return NULL;
    }
    out = moq5_trust_self_signed(env, a, k, v);
    (*env)->ReleaseStringUTFChars(env, san, v);
    (*env)->ReleaseStringUTFChars(env, kind, k);
    (*env)->ReleaseStringUTFChars(env, algo, a);
    return out;
}

/* Copies the chain out of the JVM before any decision runs, so the decision
 * core sees plain C data and the row can also drive it from a foreign thread. */
static int moq5_trust_run(JNIEnv *env, jobjectArray chain, jstring name, jboolean use_delegate)
{
    uint8_t **bufs = NULL;
    size_t *lens = NULL;
    const char *utf = NULL;
    jsize count;
    jsize i;
    int rc;

    count = (chain == NULL) ? 0 : (*env)->GetArrayLength(env, chain);
    if (count > 0) {
        bufs = (uint8_t **)calloc((size_t)count, sizeof *bufs);
        lens = (size_t *)calloc((size_t)count, sizeof *lens);
        if (bufs == NULL || lens == NULL) { free(bufs); free(lens); return MOQ5_TRUST_ERR_ATTACH; }
        for (i = 0; i < count; i++) {
            jbyteArray der = (jbyteArray)(*env)->GetObjectArrayElement(env, chain, i);
            jsize n = (der == NULL) ? 0 : (*env)->GetArrayLength(env, der);
            bufs[i] = (uint8_t *)malloc((size_t)n + 1u);
            if (bufs[i] == NULL) { if (der) (*env)->DeleteLocalRef(env, der); rc = MOQ5_TRUST_ERR_ATTACH; goto out; }
            if (n > 0) (*env)->GetByteArrayRegion(env, der, 0, n, (jbyte *)bufs[i]);
            lens[i] = (size_t)n;
            if (der != NULL) (*env)->DeleteLocalRef(env, der);
        }
    }
    if (name != NULL) utf = (*env)->GetStringUTFChars(env, name, NULL);

    rc = moq5_trust_decide((const uint8_t *const *)bufs, lens, (size_t)count,
                           utf, use_delegate == JNI_TRUE);

    if (utf != NULL) (*env)->ReleaseStringUTFChars(env, name, utf);
out:
    for (i = 0; i < count && bufs != NULL; i++) free(bufs[i]);
    free(bufs);
    free(lens);
    return rc;
}

static jint JNICALL moq5_trust_impl_decide(JNIEnv *env, jclass self, jobjectArray chain,
                                           jstring name, jboolean use_delegate)
{
    (void)self;
    return (jint)moq5_trust_run(env, chain, name, use_delegate);
}

/* The foreign-thread driver: the decision runs on a thread the JVM has never
 * seen, exactly as the picoquic network thread would, [repeat] times. */
struct moq5_trust_foreign {
    uint8_t **bufs; size_t *lens; size_t count; char name[256]; int repeat; int rc;
};

static void *moq5_trust_foreign_main(void *arg)
{
    struct moq5_trust_foreign *f = (struct moq5_trust_foreign *)arg;
    int i;
    for (i = 0; i < f->repeat; i++) {
        f->rc = moq5_trust_decide((const uint8_t *const *)f->bufs, f->lens, f->count, f->name, 1);
    }
    return NULL;
}

static jint JNICALL moq5_trust_impl_decideForeign(JNIEnv *env, jclass self, jobjectArray chain,
                                                  jstring name, jint repeat)
{
    struct moq5_trust_foreign f;
    pthread_t th;
    const char *utf = NULL;
    jsize count;
    jsize i;

    (void)self;
    memset(&f, 0, sizeof f);
    f.repeat = (repeat < 1) ? 1 : (int)repeat;
    count = (chain == NULL) ? 0 : (*env)->GetArrayLength(env, chain);
    f.count = (size_t)count;
    if (count > 0) {
        f.bufs = (uint8_t **)calloc((size_t)count, sizeof *f.bufs);
        f.lens = (size_t *)calloc((size_t)count, sizeof *f.lens);
        if (f.bufs == NULL || f.lens == NULL) { free(f.bufs); free(f.lens); return MOQ5_TRUST_ERR_ATTACH; }
        for (i = 0; i < count; i++) {
            jbyteArray der = (jbyteArray)(*env)->GetObjectArrayElement(env, chain, i);
            jsize n = (der == NULL) ? 0 : (*env)->GetArrayLength(env, der);
            f.bufs[i] = (uint8_t *)malloc((size_t)n + 1u);
            if (f.bufs[i] == NULL) { if (der) (*env)->DeleteLocalRef(env, der); return MOQ5_TRUST_ERR_ATTACH; }
            if (n > 0) (*env)->GetByteArrayRegion(env, der, 0, n, (jbyte *)f.bufs[i]);
            f.lens[i] = (size_t)n;
            if (der != NULL) (*env)->DeleteLocalRef(env, der);
        }
    }
    if (name != NULL) {
        utf = (*env)->GetStringUTFChars(env, name, NULL);
        if (utf != NULL) {
            (void)snprintf(f.name, sizeof f.name, "%s", utf);
            (*env)->ReleaseStringUTFChars(env, name, utf);
        }
    }
    if (pthread_create(&th, NULL, moq5_trust_foreign_main, &f) != 0) return MOQ5_TRUST_ERR_ATTACH;
    (void)pthread_join(th, NULL);
    for (i = 0; i < count && f.bufs != NULL; i++) free(f.bufs[i]);
    free(f.bufs);
    free(f.lens);
    return (jint)f.rc;
}

static jint JNICALL moq5_trust_impl_attachCount(JNIEnv *env, jclass self)
{
    int n;
    (void)env; (void)self;
    pthread_mutex_lock(&g_attach_lock);
    n = g_attach_count;
    pthread_mutex_unlock(&g_attach_lock);
    return (jint)n;
}

static jstring JNICALL moq5_trust_impl_seenAuthType(JNIEnv *env, jclass self)
{
    (void)self;
    return (*env)->NewStringUTF(env, g_seen_auth);
}

static jstring JNICALL moq5_trust_impl_seenServerName(JNIEnv *env, jclass self)
{
    (void)self;
    return (*env)->NewStringUTF(env, g_seen_name);
}

/*
 * Drives the wrapper's callback DIRECTLY, with no handshake.
 *
 * `tls_name` is set on the ptls_t as its OWN server name and is deliberately
 * DIFFERENT from `server_name`, which is what the callback is handed. A wrapper
 * that consulted `ptls_get_server_name(tls)` instead of its parameter would
 * therefore decide against the wrong identity, and the row would see it. This
 * is the ECH divergence picotls documents, reproduced deterministically.
 *
 * Returns the wrapper's own verdict; the picotls alert is reported separately
 * through the accessors below, so a row can tell "rejected, and no delegate
 * ran" from "accepted, and verify_sign was installed".
 */
static jint JNICALL moq5_trust_impl_verifyCb(JNIEnv *env, jclass self, jobjectArray chain,
                                             jstring server_name, jstring tls_name,
                                             jboolean use_delegate, jboolean seed_outputs)
{
    static ptls_context_t ctx;
    ptls_t *tls = NULL;
    ptls_iovec_t certs[8];
    uint8_t *bufs[8];
    const char *sname = NULL;
    const char *tname = NULL;
    int (*verify_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t) = NULL;
    void *verify_data = NULL;
    /* SENTINELS. Seeding the output parameters before the call is what turns
     * "verify_sign is NULL" into the stronger "the wrapper wrote nothing": on a
     * reject the caller's own values must survive untouched, so the wrapper
     * cannot have left picotls stale success data of its own making. */
    int (*seed_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t) =
        (int (*)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t))(void *)moq5_trust_seed_sign;
    void *seed_data = (void *)&g_verifier;
    int (*initial_sign)(void *, uint16_t, ptls_iovec_t, ptls_iovec_t) = NULL;
    void *initial_data = NULL;
    jsize count;
    jsize i;
    int alert;

    (void)self;
    memset(bufs, 0, sizeof bufs);
    count = (chain == NULL) ? 0 : (*env)->GetArrayLength(env, chain);
    if (count > (jsize)(sizeof certs / sizeof certs[0])) return MOQ5_TRUST_ERR_JNI_LIMIT;
    for (i = 0; i < count; i++) {
        jbyteArray der = (jbyteArray)(*env)->GetObjectArrayElement(env, chain, i);
        jsize n = (der == NULL) ? 0 : (*env)->GetArrayLength(env, der);
        bufs[i] = (uint8_t *)malloc((size_t)n + 1u);
        if (bufs[i] == NULL) { if (der) (*env)->DeleteLocalRef(env, der); goto fail; }
        if (n > 0) (*env)->GetByteArrayRegion(env, der, 0, n, (jbyte *)bufs[i]);
        certs[i] = ptls_iovec_init(bufs[i], (size_t)n);
        if (der != NULL) (*env)->DeleteLocalRef(env, der);
    }

    ctx.random_bytes = ptls_openssl_random_bytes;
    ctx.get_time = &ptls_get_time;
    ctx.key_exchanges = ptls_openssl_key_exchanges;
    ctx.cipher_suites = ptls_openssl_cipher_suites;
    ctx.verify_certificate = &g_verifier.super;
    tls = ptls_new(&ctx, 0);
    if (tls == NULL) goto fail;

    if (tls_name != NULL) {
        tname = (*env)->GetStringUTFChars(env, tls_name, NULL);
        if (tname != NULL) (void)ptls_set_server_name(tls, tname, strlen(tname));
    }
    if (server_name != NULL) sname = (*env)->GetStringUTFChars(env, server_name, NULL);

    if (seed_outputs == JNI_TRUE) { verify_sign = seed_sign; verify_data = seed_data; }
    /* Whatever the CALLER left in the output parameters, seeded or not. */
    initial_sign = verify_sign;
    initial_data = verify_data;
    g_verifier.use_delegate = (use_delegate == JNI_TRUE);
    g_verifier.ossl_calls = 0;
    g_verifier.last_verdict = MOQ5_TRUST_OK;
    alert = g_verifier.super.cb(&g_verifier.super, tls, sname, &verify_sign, &verify_data,
                                certs, (size_t)count);
    /* INSTALLED means picotls REPLACED what the caller left -- not merely that
     * two pointers are non-NULL. Under seeding the old non-NULL test reported
     * "installed" for a rejection that installed nothing, which made the
     * accessor's name false on exactly the path it mattered most. */
    g_verifier.accept_installed =
        (verify_sign != NULL && verify_data != NULL &&
         verify_sign != initial_sign && verify_data != initial_data) ? 1 : 0;
    g_verifier.outputs_untouched =
        (verify_sign == initial_sign && verify_data == initial_data) ? 1 : 0;
    g_verifier.last_alert = alert;
    /* Only a pubkey picotls actually installed is ours to free; a surviving
     * sentinel is not an EVP_PKEY at all. */
    if (verify_data != NULL && verify_data != initial_data) EVP_PKEY_free((EVP_PKEY *)verify_data);

    if (sname != NULL) (*env)->ReleaseStringUTFChars(env, server_name, sname);
    if (tname != NULL) (*env)->ReleaseStringUTFChars(env, tls_name, tname);
    ptls_free(tls);
    for (i = 0; i < count; i++) free(bufs[i]);
    return (jint)g_verifier.last_verdict;

fail:
    if (tls != NULL) ptls_free(tls);
    for (i = 0; i < count; i++) free(bufs[i]);
    return MOQ5_TRUST_ERR_ATTACH;
}

static jint JNICALL moq5_trust_impl_osslCalls(JNIEnv *env, jclass self)
{
    (void)env; (void)self;
    return (jint)g_verifier.ossl_calls;
}

static jint JNICALL moq5_trust_impl_lastAlert(JNIEnv *env, jclass self)
{
    (void)env; (void)self;
    return (jint)g_verifier.last_alert;
}

static jboolean JNICALL moq5_trust_impl_outputsUntouched(JNIEnv *env, jclass self)
{
    (void)env; (void)self;
    return g_verifier.outputs_untouched ? JNI_TRUE : JNI_FALSE;
}

static jboolean JNICALL moq5_trust_impl_signInstalled(JNIEnv *env, jclass self)
{
    (void)env; (void)self;
    return g_verifier.accept_installed ? JNI_TRUE : JNI_FALSE;
}

static const JNINativeMethod fixture_methods[] = {
    { "nativeMakeSelfSigned",  "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)[B",
      (void *)moq5_trust_impl_makeCert },
    { "nativeDecide",          "([[BLjava/lang/String;Z)I",
      (void *)moq5_trust_impl_decide },
    { "nativeDecideForeign",   "([[BLjava/lang/String;I)I",
      (void *)moq5_trust_impl_decideForeign },
    { "nativeAttachCount",     "()I", (void *)moq5_trust_impl_attachCount },
    { "nativeSeenAuthType",    "()Ljava/lang/String;", (void *)moq5_trust_impl_seenAuthType },
    { "nativeSeenServerName",  "()Ljava/lang/String;", (void *)moq5_trust_impl_seenServerName },
    { "nativeVerifyCb",        "([[BLjava/lang/String;Ljava/lang/String;ZZ)I",
      (void *)moq5_trust_impl_verifyCb },
    { "nativeOsslDelegateCalls", "()I", (void *)moq5_trust_impl_osslCalls },
    { "nativeVerifySignInstalled", "()Z", (void *)moq5_trust_impl_signInstalled },
    { "nativeLastAlert",       "()I", (void *)moq5_trust_impl_lastAlert },
    { "nativeOutputsUntouched", "()Z", (void *)moq5_trust_impl_outputsUntouched },
};

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved)
{
    JNIEnv *env = NULL;
    jclass klass = NULL;
    jclass cex = NULL;

    (void)reserved;
    g_vm = vm;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_6) != JNI_OK || env == NULL) {
        return JNI_ERR;
    }
    if (pthread_key_create(&g_attach_key, moq5_trust_detach) == 0) g_attach_key_ready = 1;

    /* The wrapper is initialised ONCE: its private OpenSSL delegate is built
     * with a NULL store -- picotls then uses OpenSSL's default, which on this
     * host and on Android is effectively empty -- and the accept override is
     * what makes that irrelevant, because the wrapper has already decided. */
    if (ptls_openssl_init_verify_certificate(&g_verifier.ossl, NULL) != 0) return JNI_ERR;
    g_verifier.accept_override.cb = moq5_trust_accept_override;
    g_verifier.ossl.override_callback = &g_verifier.accept_override;
    g_verifier.super.cb = moq5_trust_verify_cb;
    g_verifier.super.algos = g_verifier.ossl.super.algos;

    klass = (*env)->FindClass(env, "org/openmoq/moq5/jni/TrustDelegateBridge");
    if (klass == NULL) return JNI_ERR;
    /* GLOBAL: a native thread attached later cannot resolve an application
     * class, and a local ref would not survive this function in any case. */
    g_bridge = (*env)->NewGlobalRef(env, klass);
    g_check = (*env)->GetStaticMethodID(env, klass, "checkServerTrusted",
                                        "([[BLjava/lang/String;Ljava/lang/String;)I");
    if ((*env)->RegisterNatives(env, klass, fixture_methods,
                                (jsize)(sizeof fixture_methods / sizeof fixture_methods[0]))
        != JNI_OK) {
        (*env)->DeleteLocalRef(env, klass);
        return JNI_ERR;
    }
    (*env)->DeleteLocalRef(env, klass);
    if (g_bridge == NULL || g_check == NULL) return JNI_ERR;

    cex = (*env)->FindClass(env, "java/security/cert/CertificateException");
    if (cex == NULL) return JNI_ERR;
    g_cert_exception = (*env)->NewGlobalRef(env, cex);
    (*env)->DeleteLocalRef(env, cex);
    if (g_cert_exception == NULL) return JNI_ERR;

    return JNI_VERSION_1_6;
}
