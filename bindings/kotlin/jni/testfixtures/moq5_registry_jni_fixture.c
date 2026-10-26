/* A test-only bridge that carries a REAL registry token across JNI.
 *
 * The full-width rule cannot be proven by a C-side cast: the claim is that a
 * token survives C `uint64_t` -> JNI `jlong` -> Kotlin `Long` -> JNI `jlong` ->
 * C `uint64_t` unchanged, and only an actual crossing tests that. The tokens
 * come from the PRODUCTION registry source, which this fixture links, so what
 * crosses is what the product would mint -- including the set high bit that
 * makes an ordinary token negative as a `jlong`.
 *
 * This is test-only. It is never compiled into the Android image, and no symbol
 * of it appears in any shipped object.
 */
#include <jni.h>

#include "../moq5_handle_jni.h"
#include "../moq5_registry.h"

static void fixture_cleanup(void *p)
{
    (void)p;
}

/* Mint a real root token and return its bits. */
static jlong JNICALL fx_mintToken(JNIEnv *env, jobject self)
{
    static int resource;
    moq5_handle_t h = MOQ5_HANDLE_INVALID;

    (void)env;
    (void)self;
    if (moq5_reg_insert_root(&resource, fixture_cleanup, &h) != MOQ5_REG_OK) {
        return (jlong)0;
    }
    return moq5_handle_to_jlong(h);
}

/* Take the token back and report whether the registry still resolves it to a
 * live entry: proof that the bits survived the round trip, judged by the
 * registry itself rather than by comparing two numbers in Kotlin. */
static jboolean JNICALL fx_resolves(JNIEnv *env, jobject self, jlong token)
{
    void *p = NULL;
    moq5_handle_t h = moq5_handle_from_jlong(token);

    (void)env;
    (void)self;
    if (moq5_reg_acquire(h, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, &p) != MOQ5_REG_OK) {
        return JNI_FALSE;
    }
    moq5_reg_release(h);
    return JNI_TRUE;
}

/* Echo the bits straight back, so Kotlin can compare byte-for-byte. */
static jlong JNICALL fx_echo(JNIEnv *env, jobject self, jlong token)
{
    (void)env;
    (void)self;
    return moq5_handle_to_jlong(moq5_handle_from_jlong(token));
}

static jlong JNICALL fx_retire(JNIEnv *env, jobject self, jlong token)
{
    (void)env;
    (void)self;
    return (jlong)moq5_reg_retire_root(moq5_handle_from_jlong(token));
}

static const JNINativeMethod fixture_methods[] = {
    { "mintToken", "()J", (void *)(uintptr_t)fx_mintToken },
    { "resolves", "(J)Z", (void *)(uintptr_t)fx_resolves },
    { "echo", "(J)J", (void *)(uintptr_t)fx_echo },
    { "retire", "(J)J", (void *)(uintptr_t)fx_retire },
};

__attribute__((visibility("default")))
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved)
{
    JNIEnv *env = NULL;
    jclass klass = NULL;

    (void)reserved;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_6) != JNI_OK || env == NULL) {
        return JNI_ERR;
    }
    klass = (*env)->FindClass(env, "org/openmoq/moq5/jni/RegistryRoundTripBridge");
    if (klass == NULL) {
        return JNI_ERR;
    }
    if ((*env)->RegisterNatives(env, klass, fixture_methods,
                                (jsize)(sizeof fixture_methods / sizeof fixture_methods[0]))
        != JNI_OK) {
        (*env)->DeleteLocalRef(env, klass);
        return JNI_ERR;
    }
    (*env)->DeleteLocalRef(env, klass);
    return JNI_VERSION_1_6;
}
