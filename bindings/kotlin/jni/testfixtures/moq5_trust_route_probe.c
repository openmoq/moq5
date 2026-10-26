/*
 * moq5_trust_route_probe.c -- HOST-ONLY recorder around the product bridge.
 *
 * Not shipped and not reachable from any product image: it is linked only into
 * the host product images by tools/build-bridge-testdsos.sh, and the Android
 * topology links neither this file nor its directory.
 *
 * The product translation unit (`moq5_jni.c`) is compiled UNCHANGED, with
 * SIX names redirected at compile time so this file can stand between the
 * product and the things it calls, observe, and forward:
 *
 *   JNI_OnLoad                  -> moq5_product_JNI_OnLoad
 *   pthread_key_create          -> moq5_test_pthread_key_create
 *   pthread_setspecific         -> moq5_test_pthread_setspecific
 *   pthread_key_delete          -> moq5_test_pthread_key_delete
 *   moq5_reg_reserve_root       -> moq5_test_reg_reserve_root
 *   moq_endpoint_connect        -> moq5_test_endpoint_connect
 *   moq_endpoint_test_make_bare -> moq5_test_endpoint_make_bare
 *
 * and ONE name redirected in the service substrate (`endpoint.c`), so this
 * file can own the product name the JNI calls:
 *
 *   moq_endpoint_connect_with_trust -> moq5_test_real_connect_with_trust
 *
 * What is observed, all as CAUSAL counts at the real boundary rather than as
 * inferences from the JVM's later state:
 *
 *   * the internal trust route: what the JNI handed it (delegate, context,
 *     configuration, URL);
 *   * the JavaVM invocation table the product captured in JNI_OnLoad is a
 *     FORWARDING copy, so every GetEnv / AttachCurrentThread(AsDaemon) /
 *     DetachCurrentThread the product makes is counted per thread and then
 *     forwarded to the real VM. A detach attempted from a thread-exit
 *     destructor is counted the same way, whatever the host VM then does with
 *     it;
 *   * on every thread the product ATTACHES (and on a thread a case pre-attaches
 *     through the real VM), the JNIEnv FUNCTION TABLE is swapped for a
 *     forwarding copy so
 *     local-reference creation, deletion and frame use are counted: the
 *     high-water mark of simultaneously live local references is what bounds
 *     the callback, independently of any checked-JNI text;
 *   * with the probe ARMED (MOQ5_TRUST_PROBE_ENV) the recorder also INVOKES
 *     the delegate it was handed, on a pthread the JVM has never seen, once
 *     per scripted case and as many times per case as the case asks, and
 *     reports every verdict and every count to the Kotlin ledger in
 *     `TrustRouteProbe`;
 *   * with a FAULT ARMED (MOQ5_TRUST_FAULT_ENV) the forwarded pthread calls
 *     fail, so the product's fail-closed handling of a cleanup-registration
 *     failure is exercised; with a CONSTRUCTOR FAULT ARMED (MOQ5_CTOR_FAULT_ENV)
 *     one construction errors, or answers success with no endpoint, exactly
 *     once, so the transaction's abort branches are exercised;
 *   * every registry reservation and every constructor attempt the product
 *     makes is counted, and the last reservation's handle is kept, so a row can
 *     require exact reserve/construct multiplicities and exact slot/generation
 *     preservation across an aborted reservation;
 *   * during the product's JNI_OnLoad the LOADING thread's JNIEnv table is
 *     swapped too, so the global references the load creates and deletes are
 *     counted; on a refused load the recorder prints those counts beside the
 *     thread-key creations and deletions, which is how a row proves the
 *     product released what it had initialised;
 *   * the recorder registers a few natives on the Kotlin ledger class so the
 *     platform seam can report each consultation the instant it happens, and
 *     so a scenario can read the counters and any latched instrumentation
 *     fault. The recorder FAILS CLOSED: an instrumentation defect is latched
 *     by name and every affected scenario fails on it, never undercounts.
 *
 * The delegate under observation is the PRODUCT's. This file supplies inputs
 * and records outputs; it performs no trust decision of its own.
 *
 * Absent the arming tokens the recorder is a pure pass-through with counting:
 * every other host row sees the product exactly as it was written.
 */
#define _GNU_SOURCE 1
#define _DARWIN_C_SOURCE 1

#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <jni.h>

#include <moq/codec_signaling.h>
#include <moq/endpoint.h>
#include <moq/loc.h>
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include "endpoint_trust.h"
#include "../moq5_receiver_child.h"
#include "../moq5_registry.h"

/* The service's own testing seam (service/src/media_receiver.c, compiled on
 * the host with MOQ_MEDIA_RECEIVER_TESTING): a catalog object fed through the
 * real catalog path, with no peer. Declared here as the service tests do. */
void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group, uint64_t object,
                                    const char *json, size_t len);
moq_media_track_t *moq_media_receiver_test_track_at(moq_media_receiver_t *r, size_t index);
void moq_media_receiver_test_set_transport_version(moq_media_receiver_t *r, moq_version_t v);
void moq_media_receiver_test_mark_desired(moq_media_receiver_t *r, moq_media_track_t *track);
void moq_media_receiver_test_push_object(moq_media_receiver_t *r, moq_media_track_t *track,
                                         uint64_t group, uint64_t object,
                                         moq_object_status_t status, bool end_of_group,
                                         bool datagram, const uint8_t *payload, size_t len,
                                         const uint8_t *properties, size_t plen);
void moq_media_receiver_test_push_object_ex(moq_media_receiver_t *r, moq_media_track_t *track,
                                            uint64_t group, uint64_t subgroup, uint64_t object,
                                            uint8_t publisher_priority, moq_object_status_t status,
                                            bool end_of_group, bool datagram,
                                            const uint8_t *payload, size_t len,
                                            const uint8_t *properties, size_t plen);

/* The product's object cleanup, redirected here at compile time: counted, and
 * the object's media bytes are POISONED before the real release, so a row
 * that copied after cleanup fails its exact-bytes assertion. */
static pthread_mutex_t g_obj_mu = PTHREAD_MUTEX_INITIALIZER;
static long g_obj_cleanups;
void moq5_test_media_object_cleanup(moq_media_object_t *obj);
void moq5_test_media_object_cleanup(moq_media_object_t *obj)
{
    if (obj != NULL) {
        if (obj->payload.data && obj->payload.len)
            memset((void *)(uintptr_t)obj->payload.data, 0xAA, obj->payload.len);
        if (obj->fragment.data && obj->fragment.len)
            memset((void *)(uintptr_t)obj->fragment.data, 0xAA, obj->fragment.len);
    }
    pthread_mutex_lock(&g_obj_mu);
    g_obj_cleanups++;
    pthread_mutex_unlock(&g_obj_mu);
    moq_media_object_cleanup(obj);
}

/* A receiver's track by the token discovery minted for it (a borrowed child
 * of the receiver), or the receiver's FIRST handle for the sentinel -- the
 * one row that must reach a handle no token exists for yet. */
#define MOQ5_PROBE_FIRST_TRACK ((jlong)(int64_t)0x80000000000000FFULL)
static moq_media_track_t *probe_track(moq_media_receiver_t *r, jlong rx, jlong track,
                                      int *pinned)
{
    void *tp = NULL;
    *pinned = 0;
    if (track == MOQ5_PROBE_FIRST_TRACK) return moq_media_receiver_test_track_at(r, 0);
    if (moq5_reg_acquire((moq5_handle_t)track, MOQ5_KIND_CHILD_BORROWED, MOQ5_TYPE_RECEIVER_TRACK,
                         (moq5_handle_t)rx, &tp) != MOQ5_REG_OK)
        return NULL;
    *pinned = 1;
    return (moq_media_track_t *)tp;
}

/* The REAL route and the product's load entry, as renamed by the substrate
 * build. Declared here rather than in a header: they exist only in the host
 * image. */
moq_result_t moq5_test_real_connect_with_trust(const moq_endpoint_cfg_t *cfg,
                                               moq_endpoint_trust_fn fn,
                                               void *ctx,
                                               moq_endpoint_t **out);
jint moq5_product_JNI_OnLoad(JavaVM *vm, void *reserved);
moq_endpoint_t *moq_endpoint_test_make_bare(void);

/* Constructor faults are ARMED BY THE SCENARIO through a ledger native, right
 * before the construction they are meant for: an environment arm would fire on
 * whatever construction came first. */
#define MOQ5_CTOR_FAULT_ERROR_ONCE "error-once-v1"
#define MOQ5_CTOR_FAULT_NULL_ONCE  "null-once-v1"

#define MOQ5_TRUST_PROBE_ENV   "MOQ5_JNI_HOST_TEST_TRUST_PROBE"
#define MOQ5_TRUST_PROBE_TOKEN "probe-v1"

#define MOQ5_TRUST_FAULT_ENV        "MOQ5_JNI_HOST_TEST_TRUST_FAULT"
#define MOQ5_TRUST_FAULT_KEY_CREATE "key-create-v1"
#define MOQ5_TRUST_FAULT_SETSPECIFIC "setspecific-v1"

#define MOQ5_PROBE_CLASS "org/openmoq/moq5/jni/TrustRouteProbe"

static void moq5_probe_say(const char *what)
{
    (void)fprintf(stderr, "MOQ5-TRUST-PROBE: %s\n", what);
    (void)fflush(stderr);
}

/* -- fail-closed instrumentation --------------------------------------------- *
 * The first instrumentation defect is latched by name. It is reported to the
 * Kotlin ledger, where every scenario requires it to be absent: a recorder
 * that could silently undercount would turn "bounded" into "unobserved". */
static char g_fault[200];
static pthread_mutex_t g_fault_mu = PTHREAD_MUTEX_INITIALIZER;

static void moq5_latch(const char *what)
{
    pthread_mutex_lock(&g_fault_mu);
    if (g_fault[0] == '\0') (void)snprintf(g_fault, sizeof g_fault, "%s", what);
    pthread_mutex_unlock(&g_fault_mu);
    moq5_probe_say(what);
}

/* Causal counters at the product's own call sites. */
static pthread_mutex_t g_cnt_mu = PTHREAD_MUTEX_INITIALIZER;
static long g_consulted;           /* platform consultations, reported by the Kotlin seam */
static long g_reserve_attempts;
static long g_reserve_failures;
static long g_construct_attempts;  /* ordinary real, trusted real, scripted */
static long g_last_reserve_handle; /* the handle of the last SUCCESSFUL reservation */
static char g_ctor_fault[32];      /* the armed one-shot token, or empty */

/* One-shot: the next child insertion is refused by the registry with
 * MOQ5_REG_ERR_CAPACITY without inserting -- the input a row needs to prove a
 * receiver attached but unowned is destroyed at once. */
#define MOQ5_CTOR_FAULT_CHILD_INSERT_ONCE "child-insert-refuse-once-v1"

static long moq5_read_consulted(void)
{
    long n;
    pthread_mutex_lock(&g_cnt_mu);
    n = g_consulted;
    pthread_mutex_unlock(&g_cnt_mu);
    return n;
}

/* One-shot: the first construction after arming fires the fault and disarms
 * it; every later one is ordinary, so the same child can prove recovery on the
 * exact next slot. */
static int moq5_ctor_fault_take(const char *token)
{
    int fire = 0;
    pthread_mutex_lock(&g_cnt_mu);
    if (strcmp(g_ctor_fault, token) == 0) { g_ctor_fault[0] = '\0'; fire = 1; }
    pthread_mutex_unlock(&g_cnt_mu);
    return fire;
}

/* -- the forwarded registry reservation and constructors -------------------- */
/* Forwarding wrapper the product's child insertion is redirected to. */
moq5_reg_result_t moq5_test_reg_insert_child(moq5_handle_t owner, void *ptr, moq5_kind_t kind,
                                             moq5_type_t type, moq5_cleanup_fn cleanup,
                                             moq5_handle_t *out_handle)
{
    if (moq5_ctor_fault_take(MOQ5_CTOR_FAULT_CHILD_INSERT_ONCE)) {
        if (out_handle != NULL) *out_handle = MOQ5_HANDLE_INVALID;
        return MOQ5_REG_ERR_CAPACITY;
    }
    return moq5_reg_insert_child(owner, ptr, kind, type, cleanup, out_handle);
}

/* The REAL service call, counted.
 *
 * "No track was added" must be an observation, not an inference from a name
 * still being free: this forwards to the shipped function and counts the
 * crossings, so a row can state exactly how many times the product reached it.
 */
static long g_add_track_calls;

moq_result_t moq5_test_media_sender_add_track(moq_media_sender_t *s,
                                              const moq_media_track_cfg_t *cfg,
                                              moq_media_track_t **out)
{
    pthread_mutex_lock(&g_cnt_mu);
    g_add_track_calls++;
    pthread_mutex_unlock(&g_cnt_mu);
    return moq_media_sender_add_track(s, cfg, out);
}

/* The same one-shot refusal for the CHILD RESERVATION the product now takes
 * before it calls the service. Same token, because it stands for the same
 * thing: the registry declines to own another child. */
moq5_reg_result_t moq5_test_reg_reserve_child(moq5_handle_t owner, moq5_kind_t kind,
                                              moq5_type_t type, moq5_handle_t *out_handle)
{
    if (moq5_ctor_fault_take(MOQ5_CTOR_FAULT_CHILD_INSERT_ONCE)) {
        if (out_handle != NULL) *out_handle = MOQ5_HANDLE_INVALID;
        return MOQ5_REG_ERR_CAPACITY;
    }
    return moq5_reg_reserve_child(owner, kind, type, out_handle);
}

moq5_reg_result_t moq5_test_reg_reserve_root(moq5_handle_t *out)
{
    moq5_reg_result_t rr = moq5_reg_reserve_root(out);
    pthread_mutex_lock(&g_cnt_mu);
    g_reserve_attempts++;
    if (rr != MOQ5_REG_OK) g_reserve_failures++;
    else if (out != NULL) g_last_reserve_handle = (long)*out;
    pthread_mutex_unlock(&g_cnt_mu);
    return rr;
}

moq_result_t moq5_test_endpoint_connect(const moq_endpoint_cfg_t *cfg, moq_endpoint_t **out)
{
    pthread_mutex_lock(&g_cnt_mu);
    g_construct_attempts++;
    pthread_mutex_unlock(&g_cnt_mu);
    return moq_endpoint_connect(cfg, out);
}

moq_endpoint_t *moq5_test_endpoint_make_bare(void)
{
    pthread_mutex_lock(&g_cnt_mu);
    g_construct_attempts++;
    pthread_mutex_unlock(&g_cnt_mu);
    /* The scripted constructor's one choke point: a NULL here is the one
     * error it can report (MOQ_ERR_NOMEM), which is the scripted constructor
     * error the transaction rows need. */
    if (moq5_ctor_fault_take(MOQ5_CTOR_FAULT_ERROR_ONCE)) return NULL;
    return moq_endpoint_test_make_bare();
}

/* -- the forwarded pthread calls ------------------------------------------- */
static int moq5_fault_armed(const char *token)
{
    const char *v = getenv(MOQ5_TRUST_FAULT_ENV);
    return v != NULL && strcmp(v, token) == 0;
}

static pthread_mutex_t g_key_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_keys_created;   /* successful creations */
static int g_keys_deleted;

int moq5_test_pthread_key_create(pthread_key_t *key, void (*destructor)(void *))
{
    int rc;
    if (moq5_fault_armed(MOQ5_TRUST_FAULT_KEY_CREATE)) return EAGAIN;
    rc = pthread_key_create(key, destructor);
    if (rc == 0) {
        pthread_mutex_lock(&g_key_mu);
        g_keys_created++;
        pthread_mutex_unlock(&g_key_mu);
    }
    return rc;
}

int moq5_test_pthread_key_delete(pthread_key_t key)
{
    int rc = pthread_key_delete(key);
    if (rc == 0) {
        pthread_mutex_lock(&g_key_mu);
        g_keys_deleted++;
        pthread_mutex_unlock(&g_key_mu);
    }
    return rc;
}

int moq5_test_pthread_setspecific(pthread_key_t key, const void *value)
{
    if (moq5_fault_armed(MOQ5_TRUST_FAULT_SETSPECIFIC)) return EAGAIN;
    return pthread_setspecific(key, value);
}

/* -- per-thread accounting --------------------------------------------------- */
#define MOQ5_PROBE_THREADS 64
#define MOQ5_PROBE_FRAMES  32

typedef struct moq5_probe_thread {
    int used;
    pthread_t tid;
    JNIEnv *env;            /* the real env once known; its table is swapped */
    int getenv;
    int attach;
    int attach_daemon;
    int detach;
    int created;            /* local references handed out */
    int deleted;            /* DeleteLocalRef of a non-NULL reference */
    int globals_new;        /* NewGlobalRef results, product callers only */
    int globals_deleted;    /* DeleteGlobalRef of a non-NULL reference */
    int live;               /* created - deleted, frames respected */
    int high;               /* the high-water mark of live */
    int depth;
    int saved[MOQ5_PROBE_FRAMES];
} moq5_probe_thread_t;

static moq5_probe_thread_t g_threads[MOQ5_PROBE_THREADS];
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;

static JavaVM *g_real_vm;
static const struct JNIInvokeInterface_ *g_real_invoke;
static struct JNIInvokeInterface_ g_fwd_invoke;
static const struct JNIInvokeInterface_ *g_fwd_vm_ptr;   /* &g_fwd_invoke; its address is the JavaVM */

static const struct JNINativeInterface_ *g_real_fns;
static struct JNINativeInterface_ g_fwd_fns;

/* The image this recorder -- and the product translation unit -- live in. A
 * local reference is attributed to the product only when the wrapper's CALLER
 * is inside this image: while the product's Java call runs, JDK natives on the
 * same thread reach the same table, and their references are the VM's own,
 * released when their frames return. */
static void *g_self_base;

static int moq5_from_our_image(void *return_address)
{
    Dl_info di;
    if (g_self_base == NULL) { moq5_latch("recorder: this image's base is unresolved; a local reference cannot be attributed"); return 0; }
    /* A caller with no image -- JIT-compiled Java code reaching a JDK native's
     * table entry -- is by definition not the product. */
    if (return_address == NULL || dladdr(return_address, &di) == 0) return 0;
    return di.dli_fbase == g_self_base;
}

/* lock held */
static moq5_probe_thread_t *moq5_thread_by_tid(pthread_t tid, int create)
{
    int i;
    for (i = 0; i < MOQ5_PROBE_THREADS; i++) {
        if (g_threads[i].used && pthread_equal(g_threads[i].tid, tid)) return &g_threads[i];
    }
    if (!create) return NULL;
    for (i = 0; i < MOQ5_PROBE_THREADS; i++) {
        if (!g_threads[i].used) {
            memset(&g_threads[i], 0, sizeof g_threads[i]);
            g_threads[i].used = 1;
            g_threads[i].tid = tid;
            return &g_threads[i];
        }
    }
    return NULL;
}

/* lock held */
static moq5_probe_thread_t *moq5_thread_by_env(JNIEnv *env)
{
    int i;
    for (i = 0; i < MOQ5_PROBE_THREADS; i++) {
        if (g_threads[i].used && g_threads[i].env == env) return &g_threads[i];
    }
    return NULL;
}

static void moq5_build_fwd_fns(const struct JNINativeInterface_ *real);

/* Swap the JNIEnv function table of [env] for the forwarding copy. The env
 * pointer itself stays the VM's own -- the VM derives its thread from that
 * address -- only the table it points at changes, and every forwarded entry
 * receives the real env. */
static void moq5_env_swap(moq5_probe_thread_t *t, JNIEnv *env)
{
    const struct JNINativeInterface_ *cur = *env;
    if (cur == &g_fwd_fns) { t->env = env; return; }
    if (g_real_fns == NULL) {
        g_real_fns = cur;
        moq5_build_fwd_fns(cur);
    } else if (cur != g_real_fns) {
        moq5_latch("recorder: a thread carries a JNI function table this recorder did not capture; its references cannot be counted");
        t->env = env;
        return;
    }
    *(const struct JNINativeInterface_ **)env = &g_fwd_fns;
    t->env = env;
}

static void moq5_env_unswap(moq5_probe_thread_t *t)
{
    if (t->env != NULL && *t->env == &g_fwd_fns) {
        *(const struct JNINativeInterface_ **)t->env = g_real_fns;
    }
    t->env = NULL;
}

/* -- the forwarding JavaVM ----------------------------------------------------- */
static jint JNICALL fwd_DestroyJavaVM(JavaVM *vm)
{
    (void)vm;
    return g_real_invoke->DestroyJavaVM(g_real_vm);
}

static jint JNICALL fwd_GetEnv(JavaVM *vm, void **penv, jint version)
{
    jint r;
    moq5_probe_thread_t *t;
    (void)vm;
    r = g_real_invoke->GetEnv(g_real_vm, penv, version);
    /* Counted only. A thread is given the forwarding table when the PRODUCT
     * attaches it (or when a case pre-attaches it), never on discovery: the
     * JVM's own threads -- the loading thread in particular -- keep their own
     * table, and an image the VM unloads after a refused load leaves no
     * dangling table behind. */
    pthread_mutex_lock(&g_mu);
    t = moq5_thread_by_tid(pthread_self(), 1);
    if (t == NULL) moq5_latch("recorder: thread table full; a GetEnv went uncounted");
    else t->getenv++;
    pthread_mutex_unlock(&g_mu);
    return r;
}

static jint JNICALL fwd_AttachCurrentThread(JavaVM *vm, void **penv, void *args)
{
    jint r;
    moq5_probe_thread_t *t;
    (void)vm;
    r = g_real_invoke->AttachCurrentThread(g_real_vm, penv, args);
    pthread_mutex_lock(&g_mu);
    t = moq5_thread_by_tid(pthread_self(), 1);
    if (t == NULL) moq5_latch("recorder: thread table full; an attach went uncounted");
    if (t != NULL) {
        t->attach++;
        if (r == JNI_OK && penv != NULL && *penv != NULL) moq5_env_swap(t, (JNIEnv *)*penv);
    }
    pthread_mutex_unlock(&g_mu);
    return r;
}

static jint JNICALL fwd_AttachCurrentThreadAsDaemon(JavaVM *vm, void **penv, void *args)
{
    jint r;
    moq5_probe_thread_t *t;
    (void)vm;
    r = g_real_invoke->AttachCurrentThreadAsDaemon(g_real_vm, penv, args);
    pthread_mutex_lock(&g_mu);
    t = moq5_thread_by_tid(pthread_self(), 1);
    if (t == NULL) moq5_latch("recorder: thread table full; a daemon attach went uncounted");
    if (t != NULL) {
        t->attach_daemon++;
        if (r == JNI_OK && penv != NULL && *penv != NULL) moq5_env_swap(t, (JNIEnv *)*penv);
    }
    pthread_mutex_unlock(&g_mu);
    return r;
}

static jint JNICALL fwd_DetachCurrentThread(JavaVM *vm)
{
    moq5_probe_thread_t *t;
    (void)vm;
    pthread_mutex_lock(&g_mu);
    t = moq5_thread_by_tid(pthread_self(), 1);
    if (t == NULL) moq5_latch("recorder: thread table full; a detach went uncounted");
    if (t != NULL) {
        t->detach++;
        /* The table is restored BEFORE the real detach: the VM must see its own
         * table when it tears the thread down. */
        moq5_env_unswap(t);
    }
    pthread_mutex_unlock(&g_mu);
    return g_real_invoke->DetachCurrentThread(g_real_vm);
}

/* -- the forwarding JNIEnv table --------------------------------------------- *
 * Only the entries that CREATE or RELEASE local references, and the frame
 * calls, are wrapped; everything else is the VM's own entry, which receives the
 * real env because the env pointer was never replaced. The variadic Call*Method
 * family cannot be wrapped portably and is not counted: a callback that made
 * local references only through those would be under-counted, and the rows
 * state the counted set. */
#define MOQ5_COUNT_CREATED(envp, obj)                                          \
    do {                                                                       \
        if ((obj) != NULL && moq5_from_our_image(__builtin_return_address(0))) { \
            moq5_probe_thread_t *_t;                                           \
            pthread_mutex_lock(&g_mu);                                         \
            _t = moq5_thread_by_env(envp);                                     \
            if (_t != NULL) {                                                  \
                _t->created++;                                                 \
                _t->live++;                                                    \
                if (_t->live > _t->high) _t->high = _t->live;                  \
            }                                                                  \
            pthread_mutex_unlock(&g_mu);                                       \
            if (_t == NULL) moq5_latch("recorder: a product local reference was created on a thread with no record"); \
        }                                                                      \
    } while (0)

static jclass JNICALL fwd_FindClass(JNIEnv *env, const char *name)
{
    jclass r = g_real_fns->FindClass(env, name);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jclass JNICALL fwd_GetObjectClass(JNIEnv *env, jobject o)
{
    jclass r = g_real_fns->GetObjectClass(env, o);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jclass JNICALL fwd_GetSuperclass(JNIEnv *env, jclass c)
{
    jclass r = g_real_fns->GetSuperclass(env, c);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jthrowable JNICALL fwd_ExceptionOccurred(JNIEnv *env)
{
    jthrowable r = g_real_fns->ExceptionOccurred(env);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jobject JNICALL fwd_NewLocalRef(JNIEnv *env, jobject o)
{
    jobject r = g_real_fns->NewLocalRef(env, o);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jobject JNICALL fwd_AllocObject(JNIEnv *env, jclass c)
{
    jobject r = g_real_fns->AllocObject(env, c);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jobject JNICALL fwd_GetObjectField(JNIEnv *env, jobject o, jfieldID f)
{
    jobject r = g_real_fns->GetObjectField(env, o, f);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jobject JNICALL fwd_GetStaticObjectField(JNIEnv *env, jclass c, jfieldID f)
{
    jobject r = g_real_fns->GetStaticObjectField(env, c, f);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jobject JNICALL fwd_GetObjectArrayElement(JNIEnv *env, jobjectArray a, jsize i)
{
    jobject r = g_real_fns->GetObjectArrayElement(env, a, i);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jobjectArray JNICALL fwd_NewObjectArray(JNIEnv *env, jsize n, jclass c, jobject init)
{
    jobjectArray r = g_real_fns->NewObjectArray(env, n, c, init);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
/* -- the codec rows' JNI operations, observed and faulted -------------------- *
 * One thread at a time is ARMED through the ledger: its table is swapped for
 * the forwarding copy, and every byte-array operation, exception check and
 * local deletion that this image makes on it is logged in order. At most one
 * site is faulted, exactly once: the operation is NOT performed and the
 * declared Throwable is left pending (or, for the NULL arm, NewByteArray
 * answers NULL with nothing pending). Nothing here is reachable unless a
 * scenario arms it. */
#define MOQ5_CODEC_OP_GET_REGION     1
#define MOQ5_CODEC_OP_NEW_ARRAY      2
#define MOQ5_CODEC_OP_SET_REGION     3
#define MOQ5_CODEC_OP_SET_ELEMENT    4
#define MOQ5_CODEC_OP_EXCEPTION_CHECK 5
#define MOQ5_CODEC_OP_DELETE_LOCAL   6
#define MOQ5_CODEC_SITE_NEW_ARRAY_NULL 7   /* a site, never a logged op */
#define MOQ5_CODEC_OPS 64

static pthread_mutex_t g_codec_env_mu = PTHREAD_MUTEX_INITIALIZER;
static JNIEnv *g_codec_env;
static int g_codec_site;
static int g_codec_skip;   /* occurrences of the site to let through before the fault */
static int g_codec_fired;
static jthrowable g_codec_throwable;
static int g_codec_ops[MOQ5_CODEC_OPS];
static int g_codec_nops;

/* Logs [op] if [env] is the armed thread and the caller is this image; returns
 * 1 when this operation is the armed fault site and has not fired yet. */
static int moq5_codec_env_op(JNIEnv *env, int op, int site, void *caller)
{
    int fault = 0;
    if (!moq5_from_our_image(caller)) return 0;
    pthread_mutex_lock(&g_codec_env_mu);
    if (env == g_codec_env) {
        if (g_codec_nops < MOQ5_CODEC_OPS) g_codec_ops[g_codec_nops] = op;
        g_codec_nops++;
        if (!g_codec_fired && site != 0 && g_codec_site == site) {
            if (g_codec_skip > 0) g_codec_skip--;
            else { g_codec_fired = 1; fault = 1; }
        }
    }
    pthread_mutex_unlock(&g_codec_env_mu);
    return fault;
}

static void moq5_codec_env_raise(JNIEnv *env)
{
    if (g_codec_throwable == NULL) { moq5_latch("recorder: a codec fault fired with no declared Throwable"); return; }
    if (g_real_fns->Throw(env, g_codec_throwable) != JNI_OK) moq5_latch("recorder: the declared Throwable could not be raised");
}

static void JNICALL fwd_GetByteArrayRegion(JNIEnv *env, jbyteArray a, jsize start, jsize len, jbyte *buf)
{
    if (moq5_codec_env_op(env, MOQ5_CODEC_OP_GET_REGION, MOQ5_CODEC_OP_GET_REGION, __builtin_return_address(0))) {
        moq5_codec_env_raise(env);
        return;
    }
    g_real_fns->GetByteArrayRegion(env, a, start, len, buf);
}

static void JNICALL fwd_SetByteArrayRegion(JNIEnv *env, jbyteArray a, jsize start, jsize len, const jbyte *buf)
{
    if (moq5_codec_env_op(env, MOQ5_CODEC_OP_SET_REGION, MOQ5_CODEC_OP_SET_REGION, __builtin_return_address(0))) {
        moq5_codec_env_raise(env);
        return;
    }
    g_real_fns->SetByteArrayRegion(env, a, start, len, buf);
}

static void JNICALL fwd_SetObjectArrayElement(JNIEnv *env, jobjectArray a, jsize i, jobject v)
{
    if (moq5_codec_env_op(env, MOQ5_CODEC_OP_SET_ELEMENT, MOQ5_CODEC_OP_SET_ELEMENT, __builtin_return_address(0))) {
        moq5_codec_env_raise(env);
        return;
    }
    g_real_fns->SetObjectArrayElement(env, a, i, v);
}

static jboolean JNICALL fwd_ExceptionCheck(JNIEnv *env)
{
    (void)moq5_codec_env_op(env, MOQ5_CODEC_OP_EXCEPTION_CHECK, 0, __builtin_return_address(0));
    return g_real_fns->ExceptionCheck(env);
}

static jbyteArray JNICALL fwd_NewByteArray(JNIEnv *env, jsize n)
{
    jbyteArray r;
    void *caller = __builtin_return_address(0);
    if (moq5_from_our_image(caller)) {
        /* One logged operation, counted against whichever arm is set: the NULL
         * arm and the throwing arm share the site's occurrence count, so a
         * skipped occurrence runs normally either way. */
        int null_arm;
        pthread_mutex_lock(&g_codec_env_mu);
        null_arm = (env == g_codec_env && g_codec_site == MOQ5_CODEC_SITE_NEW_ARRAY_NULL);
        pthread_mutex_unlock(&g_codec_env_mu);
        if (moq5_codec_env_op(env, MOQ5_CODEC_OP_NEW_ARRAY,
                              null_arm ? MOQ5_CODEC_SITE_NEW_ARRAY_NULL : MOQ5_CODEC_OP_NEW_ARRAY, caller)) {
            if (!null_arm) moq5_codec_env_raise(env);
            return NULL;
        }
    }
    r = g_real_fns->NewByteArray(env, n);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}

#define MOQ5_FWD_NEW_ARRAY(T, name)                                            \
    static T JNICALL fwd_##name(JNIEnv *env, jsize n)                           \
    {                                                                          \
        T r = g_real_fns->name(env, n);                                        \
        MOQ5_COUNT_CREATED(env, r);                                            \
        return r;                                                              \
    }
MOQ5_FWD_NEW_ARRAY(jbooleanArray, NewBooleanArray)
MOQ5_FWD_NEW_ARRAY(jcharArray,    NewCharArray)
MOQ5_FWD_NEW_ARRAY(jshortArray,   NewShortArray)
MOQ5_FWD_NEW_ARRAY(jintArray,     NewIntArray)
MOQ5_FWD_NEW_ARRAY(jlongArray,    NewLongArray)
MOQ5_FWD_NEW_ARRAY(jfloatArray,   NewFloatArray)
MOQ5_FWD_NEW_ARRAY(jdoubleArray,  NewDoubleArray)
#undef MOQ5_FWD_NEW_ARRAY
static jstring JNICALL fwd_NewString(JNIEnv *env, const jchar *s, jsize n)
{
    jstring r = g_real_fns->NewString(env, s, n);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jstring JNICALL fwd_NewStringUTF(JNIEnv *env, const char *s)
{
    jstring r = g_real_fns->NewStringUTF(env, s);
    MOQ5_COUNT_CREATED(env, r);
    return r;
}
static jobject JNICALL fwd_NewGlobalRef(JNIEnv *env, jobject o)
{
    jobject r = g_real_fns->NewGlobalRef(env, o);
    if (r != NULL && moq5_from_our_image(__builtin_return_address(0))) {
        moq5_probe_thread_t *t;
        pthread_mutex_lock(&g_mu);
        t = moq5_thread_by_env(env);
        if (t != NULL) t->globals_new++;
        pthread_mutex_unlock(&g_mu);
        if (t == NULL) moq5_latch("recorder: a product NewGlobalRef on a thread with no record");
    }
    return r;
}
static void JNICALL fwd_DeleteGlobalRef(JNIEnv *env, jobject o)
{
    if (o != NULL && moq5_from_our_image(__builtin_return_address(0))) {
        moq5_probe_thread_t *t;
        pthread_mutex_lock(&g_mu);
        t = moq5_thread_by_env(env);
        if (t != NULL) t->globals_deleted++;
        pthread_mutex_unlock(&g_mu);
        if (t == NULL) moq5_latch("recorder: a product DeleteGlobalRef on a thread with no record");
    }
    g_real_fns->DeleteGlobalRef(env, o);
}
static void JNICALL fwd_DeleteLocalRef(JNIEnv *env, jobject o)
{
    (void)moq5_codec_env_op(env, MOQ5_CODEC_OP_DELETE_LOCAL, 0, __builtin_return_address(0));
    if (o != NULL && moq5_from_our_image(__builtin_return_address(0))) {
        moq5_probe_thread_t *t;
        pthread_mutex_lock(&g_mu);
        t = moq5_thread_by_env(env);
        if (t != NULL) { t->deleted++; t->live--; }
        pthread_mutex_unlock(&g_mu);
        if (t == NULL) moq5_latch("recorder: a product DeleteLocalRef on a thread with no record");
    }
    g_real_fns->DeleteLocalRef(env, o);
}
static jint JNICALL fwd_PushLocalFrame(JNIEnv *env, jint capacity)
{
    jint r = g_real_fns->PushLocalFrame(env, capacity);
    if (r == 0 && moq5_from_our_image(__builtin_return_address(0))) {
        moq5_probe_thread_t *t;
        int overflow = 0;
        pthread_mutex_lock(&g_mu);
        t = moq5_thread_by_env(env);
        if (t != NULL) {
            /* Overflow means THIS push cannot be represented: a push into the
             * last slot is representable, so exactly MOQ5_PROBE_FRAMES nested
             * frames are followed and the next one is refused. */
            if (t->depth < MOQ5_PROBE_FRAMES) t->saved[t->depth++] = t->live;
            else overflow = 1;
        }
        pthread_mutex_unlock(&g_mu);
        if (t == NULL) moq5_latch("recorder: a product PushLocalFrame on a thread with no record");
        else if (overflow) moq5_latch("recorder: local-frame depth overflow; frames can no longer be followed");
    }
    return r;
}
static jobject JNICALL fwd_PopLocalFrame(JNIEnv *env, jobject result)
{
    jobject r = g_real_fns->PopLocalFrame(env, result);
    if (moq5_from_our_image(__builtin_return_address(0))) {
        moq5_probe_thread_t *t;
        int underflow = 0;
        pthread_mutex_lock(&g_mu);
        t = moq5_thread_by_env(env);
        if (t != NULL) {
            if (t->depth > 0) t->live = t->saved[--t->depth];
            else underflow = 1;
            if (r != NULL) { t->created++; t->live++; if (t->live > t->high) t->high = t->live; }
        }
        pthread_mutex_unlock(&g_mu);
        if (t == NULL) moq5_latch("recorder: a product PopLocalFrame on a thread with no record");
        else if (underflow) moq5_latch("recorder: PopLocalFrame without a matching push; frames can no longer be followed");
    }
    return r;
}

static void moq5_build_fwd_fns(const struct JNINativeInterface_ *real)
{
    memcpy(&g_fwd_fns, real, sizeof g_fwd_fns);
    g_fwd_fns.FindClass = fwd_FindClass;
    g_fwd_fns.GetObjectClass = fwd_GetObjectClass;
    g_fwd_fns.GetSuperclass = fwd_GetSuperclass;
    g_fwd_fns.ExceptionOccurred = fwd_ExceptionOccurred;
    g_fwd_fns.NewLocalRef = fwd_NewLocalRef;
    g_fwd_fns.AllocObject = fwd_AllocObject;
    g_fwd_fns.GetObjectField = fwd_GetObjectField;
    g_fwd_fns.GetStaticObjectField = fwd_GetStaticObjectField;
    g_fwd_fns.GetObjectArrayElement = fwd_GetObjectArrayElement;
    g_fwd_fns.NewObjectArray = fwd_NewObjectArray;
    g_fwd_fns.NewBooleanArray = fwd_NewBooleanArray;
    g_fwd_fns.NewByteArray = fwd_NewByteArray;
    g_fwd_fns.NewCharArray = fwd_NewCharArray;
    g_fwd_fns.NewShortArray = fwd_NewShortArray;
    g_fwd_fns.NewIntArray = fwd_NewIntArray;
    g_fwd_fns.NewLongArray = fwd_NewLongArray;
    g_fwd_fns.NewFloatArray = fwd_NewFloatArray;
    g_fwd_fns.NewDoubleArray = fwd_NewDoubleArray;
    g_fwd_fns.NewString = fwd_NewString;
    g_fwd_fns.NewStringUTF = fwd_NewStringUTF;
    g_fwd_fns.NewGlobalRef = fwd_NewGlobalRef;
    g_fwd_fns.DeleteGlobalRef = fwd_DeleteGlobalRef;
    g_fwd_fns.DeleteLocalRef = fwd_DeleteLocalRef;
    g_fwd_fns.GetByteArrayRegion = fwd_GetByteArrayRegion;
    g_fwd_fns.SetByteArrayRegion = fwd_SetByteArrayRegion;
    g_fwd_fns.SetObjectArrayElement = fwd_SetObjectArrayElement;
    g_fwd_fns.ExceptionCheck = fwd_ExceptionCheck;
    g_fwd_fns.PushLocalFrame = fwd_PushLocalFrame;
    g_fwd_fns.PopLocalFrame = fwd_PopLocalFrame;
}

/* -- natives on the Kotlin ledger ------------------------------------------- *
 * Registered by THIS file's JNI_OnLoad, on the test class only; the product
 * table is untouched. */
static void JNICALL ledger_nativeConsulted(JNIEnv *env, jclass self)
{
    (void)env; (void)self;
    pthread_mutex_lock(&g_cnt_mu);
    g_consulted++;
    pthread_mutex_unlock(&g_cnt_mu);
}

/* [reserveAttempts, reserveFailures, constructAttempts, lastReserveHandle, consulted] */
static jlongArray JNICALL ledger_nativeCounters(JNIEnv *env, jclass self)
{
    jlong v[5];
    jlongArray out;
    (void)self;
    pthread_mutex_lock(&g_cnt_mu);
    v[0] = (jlong)g_reserve_attempts; v[1] = (jlong)g_reserve_failures;
    v[2] = (jlong)g_construct_attempts; v[3] = (jlong)g_last_reserve_handle;
    v[4] = (jlong)g_consulted;
    pthread_mutex_unlock(&g_cnt_mu);
    out = (*env)->NewLongArray(env, 5);
    if (out != NULL) (*env)->SetLongArrayRegion(env, out, 0, 5, v);
    return out;
}

static jstring JNICALL ledger_nativeFault(JNIEnv *env, jclass self)
{
    char copy[sizeof g_fault];
    (void)self;
    pthread_mutex_lock(&g_fault_mu);
    memcpy(copy, g_fault, sizeof copy);
    pthread_mutex_unlock(&g_fault_mu);
    if (copy[0] == '\0') return NULL;
    return (*env)->NewStringUTF(env, copy);
}

static void JNICALL ledger_nativeArmCtorFault(JNIEnv *env, jclass self, jstring token)
{
    const char *utf;
    (void)self;
    if (token == NULL) return;
    utf = (*env)->GetStringUTFChars(env, token, NULL);
    if (utf == NULL) return;
    pthread_mutex_lock(&g_cnt_mu);
    (void)snprintf(g_ctor_fault, sizeof g_ctor_fault, "%s", utf);
    pthread_mutex_unlock(&g_cnt_mu);
    (*env)->ReleaseStringUTFChars(env, token, utf);
}

/* Feed one catalog object to the receiver behind a child token, under an
 * ordinary acquire of the child (owned, under `root`, this thread), so the
 * receiver's REAL poll_track has real discovery to answer. The registry's
 * refusal is returned as the bridge would return it. */
static jint JNICALL ledger_nativeIngestCatalog(JNIEnv *env, jclass self, jlong root, jlong rx,
                                               jbyteArray catalog)
{
    void *ptr = NULL;
    jbyte *bytes;
    jsize len;
    moq5_reg_result_t rr;

    (void)self;
    if (catalog == NULL) return (jint)MOQ_ERR_INVAL;
    rr = moq5_reg_acquire((moq5_handle_t)rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER,
                          (moq5_handle_t)root, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(-1000 + (int)rr);
    len = (*env)->GetArrayLength(env, catalog);
    bytes = (*env)->GetByteArrayElements(env, catalog, NULL);
    if (bytes != NULL) {
        moq_media_receiver_test_ingest(((moq5_receiver_child_t *)ptr)->r, 0, 0,
                                       (const char *)bytes, (size_t)len);
        (*env)->ReleaseByteArrayElements(env, catalog, bytes, JNI_ABORT);
    }
    (void)moq5_reg_release((moq5_handle_t)rx);
    return bytes != NULL ? (jint)MOQ_OK : (jint)MOQ_ERR_NOMEM;
}

static jint JNICALL ledger_nativeSetTransportVersion(JNIEnv *env, jclass self, jlong root,
                                                     jlong rx, jint version)
{
    void *ptr = NULL;
    moq5_reg_result_t rr;
    (void)env; (void)self;
    rr = moq5_reg_acquire((moq5_handle_t)rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER,
                          (moq5_handle_t)root, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(-1000 + (int)rr);
    moq_media_receiver_test_set_transport_version(((moq5_receiver_child_t *)ptr)->r, (moq_version_t)version);
    (void)moq5_reg_release((moq5_handle_t)rx);
    return (jint)MOQ_OK;
}

static jint JNICALL ledger_nativeMarkTrackDesired(JNIEnv *env, jclass self, jlong root,
                                                  jlong rx, jlong track)
{
    void *ptr = NULL;
    moq_media_track_t *t;
    moq5_reg_result_t rr;
    int pinned;
    (void)env; (void)self;
    rr = moq5_reg_acquire((moq5_handle_t)rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER,
                          (moq5_handle_t)root, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(-1000 + (int)rr);
    t = probe_track(((moq5_receiver_child_t *)ptr)->r, rx, track, &pinned);
    if (t == NULL) {
        (void)moq5_reg_release((moq5_handle_t)rx);
        return (jint)MOQ_ERR_INVAL;
    }
    moq_media_receiver_test_mark_desired(((moq5_receiver_child_t *)ptr)->r, t);
    if (pinned) (void)moq5_reg_release((moq5_handle_t)track);
    (void)moq5_reg_release((moq5_handle_t)rx);
    return (jint)MOQ_OK;
}

/* One NORMAL media object through the real intake carrying its wire identity
 * as the facade would deliver it: the ids as raw 64-bit patterns, the
 * priority byte, and the datagram flag (a datagram has no subgroup). */
static jint JNICALL ledger_nativeIngestObjectIdentity(JNIEnv *env, jclass self, jlong root, jlong rx,
                                                      jlong track, jlong group, jlong subgroup, jlong object,
                                                      jint priority, jboolean datagram, jbyteArray payload)
{
    void *ptr = NULL;
    moq_media_receiver_t *r;
    moq_media_track_t *t;
    jbyte *bytes;
    jsize len;
    moq5_reg_result_t rr;
    int pinned;
    (void)self;
    if (payload == NULL || priority < 0 || priority > 255) return (jint)MOQ_ERR_INVAL;
    rr = moq5_reg_acquire((moq5_handle_t)rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER,
                          (moq5_handle_t)root, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(-1000 + (int)rr);
    r = ((moq5_receiver_child_t *)ptr)->r;
    t = probe_track(r, rx, track, &pinned);
    if (t == NULL) {
        (void)moq5_reg_release((moq5_handle_t)rx);
        return (jint)MOQ_ERR_INVAL;
    }
    len = (*env)->GetArrayLength(env, payload);
    bytes = (*env)->GetByteArrayElements(env, payload, NULL);
    if (bytes != NULL) {
        moq_media_receiver_test_push_object_ex(r, t, (uint64_t)group, (uint64_t)subgroup, (uint64_t)object,
                                               (uint8_t)priority, MOQ_OBJECT_NORMAL, false,
                                               datagram != JNI_FALSE, (const uint8_t *)bytes, (size_t)len, NULL, 0);
        (*env)->ReleaseByteArrayElements(env, payload, bytes, JNI_ABORT);
    }
    if (pinned) (void)moq5_reg_release((moq5_handle_t)track);
    (void)moq5_reg_release((moq5_handle_t)rx);
    return (jint)MOQ_OK;
}

/* One media object through the real intake. For a RAW/LOC track with a
 * capture time, the LOC-01 timestamp header is encoded as a sender would. */
static jint JNICALL ledger_nativeIngestObject(JNIEnv *env, jclass self, jlong root, jlong rx,
                                              jlong track, jlong group, jlong object,
                                              jint status, jboolean end_of_group,
                                              jlong capture_us, jbyteArray payload)
{
    void *ptr = NULL;
    moq_media_receiver_t *r;
    moq_media_track_t *t;
    moq_rcbuf_t *props = NULL;
    jbyte *bytes;
    jsize len;
    moq5_reg_result_t rr;
    int pinned;
    (void)self;
    if (payload == NULL) return (jint)MOQ_ERR_INVAL;
    rr = moq5_reg_acquire((moq5_handle_t)rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER,
                          (moq5_handle_t)root, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(-1000 + (int)rr);
    r = ((moq5_receiver_child_t *)ptr)->r;
    t = probe_track(r, rx, track, &pinned);
    if (t == NULL) {
        (void)moq5_reg_release((moq5_handle_t)rx);
        return (jint)MOQ_ERR_INVAL;
    }
    if (capture_us != 0) {
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = (uint64_t)capture_us;
        if (moq_loc_encode(moq_alloc_default(), MOQ_VERSION_DRAFT_16, MOQ_LOC_PROFILE_01, &h, &props) != MOQ_OK) {
            if (pinned) (void)moq5_reg_release((moq5_handle_t)track);
            (void)moq5_reg_release((moq5_handle_t)rx);
            return (jint)MOQ_ERR_NOMEM;
        }
    }
    len = (*env)->GetArrayLength(env, payload);
    bytes = (*env)->GetByteArrayElements(env, payload, NULL);
    if (bytes != NULL) {
        moq_media_receiver_test_push_object(r, t, (uint64_t)group, (uint64_t)object,
                                            (moq_object_status_t)status, end_of_group != JNI_FALSE,
                                            false, (const uint8_t *)bytes, (size_t)len,
                                            props ? moq_rcbuf_data(props) : NULL,
                                            props ? moq_rcbuf_len(props) : 0);
        (*env)->ReleaseByteArrayElements(env, payload, bytes, JNI_ABORT);
    }
    if (props) moq_rcbuf_decref(props);
    if (pinned) (void)moq5_reg_release((moq5_handle_t)track);
    (void)moq5_reg_release((moq5_handle_t)rx);
    return bytes != NULL ? (jint)MOQ_OK : (jint)MOQ_ERR_NOMEM;
}

/* A minimal CMAF chunk the CMAF parser accepts, as the media tests build it:
 * moof(mfhd, traf(tfhd, tfdt, trun[1 sample, first-sample-flags])) + mdat(1). */
static void probe_wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16); p[2] = (uint8_t)(v >> 8); p[3] = (uint8_t)v;
}
static size_t probe_box_hdr(uint8_t *p, uint32_t size, const char *type)
{
    probe_wr32(p, size);
    memcpy(p + 4, type, 4);
    return 8;
}
static size_t probe_box_u32(uint8_t *b, const char *type, uint32_t v)
{
    size_t p = probe_box_hdr(b, 16, type);
    probe_wr32(b + p, 0); p += 4;
    probe_wr32(b + p, v); p += 4;
    return p;
}
static size_t probe_cmaf_chunk(uint8_t *out)
{
    uint8_t traf_inner[64]; size_t ti = 0;
    uint8_t traf[80]; size_t tp = 0;
    uint8_t moofc[160]; size_t mc = 0;
    size_t p = 0;
    ti += probe_box_u32(traf_inner + ti, "tfhd", 1);
    ti += probe_box_u32(traf_inner + ti, "tfdt", 0);
    ti += probe_box_hdr(traf_inner + ti, 20, "trun");
    probe_wr32(traf_inner + ti, 0x00000400); ti += 4;
    probe_wr32(traf_inner + ti, 1); ti += 4;
    probe_wr32(traf_inner + ti, 0x02000000); ti += 4;
    tp += probe_box_hdr(traf + tp, (uint32_t)(8 + ti), "traf");
    memcpy(traf + tp, traf_inner, ti); tp += ti;
    mc += probe_box_u32(moofc + mc, "mfhd", 1);
    memcpy(moofc + mc, traf, tp); mc += tp;
    p += probe_box_hdr(out + p, (uint32_t)(8 + mc), "moof");
    memcpy(out + p, moofc, mc); p += mc;
    p += probe_box_hdr(out + p, 9, "mdat");
    out[p++] = 0xAB;
    return p;
}

static jbyteArray JNICALL ledger_nativeCmafFragmentFixture(JNIEnv *env, jclass self)
{
    uint8_t buf[256];
    size_t n = probe_cmaf_chunk(buf);
    jbyteArray out;
    (void)self;
    out = (*env)->NewByteArray(env, (jsize)n);
    if (out != NULL) (*env)->SetByteArrayRegion(env, out, 0, (jsize)n, (const jbyte *)buf);
    return out;
}

static jlong JNICALL ledger_nativeObjectCleanups(JNIEnv *env, jclass self)
{
    long v;
    (void)env; (void)self;
    pthread_mutex_lock(&g_obj_mu);
    v = g_obj_cleanups;
    pthread_mutex_unlock(&g_obj_mu);
    return (jlong)v;
}

/* A catalog object at a caller-chosen (group, object): a LATER group is a new
 * catalog generation, which is how a track gets REMOVED with no peer. */
static jint JNICALL ledger_nativeIngestCatalogAt(JNIEnv *env, jclass self, jlong root, jlong rx,
                                                 jlong group, jlong object, jbyteArray catalog)
{
    void *ptr = NULL;
    jbyte *bytes;
    jsize len;
    moq5_reg_result_t rr;

    (void)self;
    if (catalog == NULL) return (jint)MOQ_ERR_INVAL;
    rr = moq5_reg_acquire((moq5_handle_t)rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER,
                          (moq5_handle_t)root, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(-1000 + (int)rr);
    len = (*env)->GetArrayLength(env, catalog);
    bytes = (*env)->GetByteArrayElements(env, catalog, NULL);
    if (bytes != NULL) {
        moq_media_receiver_test_ingest(((moq5_receiver_child_t *)ptr)->r, (uint64_t)group,
                                       (uint64_t)object, (const char *)bytes, (size_t)len);
        (*env)->ReleaseByteArrayElements(env, catalog, bytes, JNI_ABORT);
    }
    (void)moq5_reg_release((moq5_handle_t)rx);
    return bytes != NULL ? (jint)MOQ_OK : (jint)MOQ_ERR_NOMEM;
}

/* Arm the product's one-shot result-allocation failure, immediately before the
 * row it is meant for. Host image only; the product defines it. */
extern void moq5_test_arm_alloc_fault(int pending, int at_index);
extern int moq5_test_alloc_fault_pending(void);

static void JNICALL ledger_nativeArmAllocFault(JNIEnv *env, jclass self, jboolean pending, jint at)
{
    (void)env; (void)self;
    moq5_test_arm_alloc_fault(pending == JNI_TRUE, (int)at);
}

/* Whether the armed allocation failure is still waiting to fire. */
static jboolean JNICALL ledger_nativeAllocFaultPending(JNIEnv *env, jclass self)
{
    (void)env; (void)self;
    return moq5_test_alloc_fault_pending() ? JNI_TRUE : JNI_FALSE;
}

/* How many times the product has reached the real add-track service call. */
static jlong JNICALL ledger_nativeAddTrackCalls(JNIEnv *env, jclass self)
{
    jlong n;
    (void)env; (void)self;
    pthread_mutex_lock(&g_cnt_mu);
    n = (jlong)g_add_track_calls;
    pthread_mutex_unlock(&g_cnt_mu);
    return n;
}

/* -- the codec utility, forwarded and recorded --------------------------------- *
 * The product's two codec calls are redirected here at compile time. Each call
 * is recorded -- its arguments, its input bytes and what it returned -- and
 * then FORWARDED to the real utility, unless a one-shot SYNTHETIC result is
 * armed for that call index, in which case the recorded row says so and the
 * utility is not called. The utility's own translation unit is compiled
 * without any redirect, so forwarding can never recurse. */
#define MOQ5_CODEC_CALLS 16
#define MOQ5_CODEC_BYTES 512
#define MOQ5_CODEC_FIELDS 11

typedef struct {
    long kind;          /* 1 init data build, 2 string format */
    long format;
    long length_size;   /* build only; -1 for a string call */
    long has_oti, oti;  /* string only */
    long cap;           /* 0 on the size query */
    long rc;
    long out_len;
    long input_len;
    long entry_len;
    long injected;
    uint8_t input[MOQ5_CODEC_BYTES];
    uint8_t entry[8];
} moq5_codec_call_t;

static pthread_mutex_t g_codec_mu = PTHREAD_MUTEX_INITIALIZER;
static moq5_codec_call_t g_codec_calls[MOQ5_CODEC_CALLS];
static int g_codec_ncalls;
static int g_codec_inject_at = -1;
static int g_codec_inject_rc;
static long g_codec_inject_len;

/* Records a call's arguments; returns its index, or -1 when the log is full
 * (latched, never silently dropped). Sets *inject when a synthetic result is
 * armed for this index. */
static int moq5_codec_begin(long kind, long format, long length_size, long has_oti, long oti,
                            const uint8_t *input, size_t input_len, const uint8_t *entry, size_t entry_len,
                            size_t cap, int *inject, int *inject_rc, long *inject_len)
{
    int i;
    moq5_codec_call_t *c;
    pthread_mutex_lock(&g_codec_mu);
    i = g_codec_ncalls++;
    *inject = (i == g_codec_inject_at);
    *inject_rc = g_codec_inject_rc;
    *inject_len = g_codec_inject_len;
    if (*inject) g_codec_inject_at = -1;
    if (i >= MOQ5_CODEC_CALLS) {
        pthread_mutex_unlock(&g_codec_mu);
        moq5_latch("recorder: more codec calls than the log holds");
        return -1;
    }
    c = &g_codec_calls[i];
    memset(c, 0, sizeof *c);
    c->kind = kind; c->format = format; c->length_size = length_size;
    c->has_oti = has_oti; c->oti = oti; c->cap = (long)cap; c->injected = *inject;
    c->input_len = (long)input_len;
    if (input != NULL && input_len > 0) memcpy(c->input, input, input_len < MOQ5_CODEC_BYTES ? input_len : MOQ5_CODEC_BYTES);
    c->entry_len = (long)entry_len;
    if (entry != NULL && entry_len > 0) memcpy(c->entry, entry, entry_len < sizeof c->entry ? entry_len : sizeof c->entry);
    pthread_mutex_unlock(&g_codec_mu);
    if (input_len > MOQ5_CODEC_BYTES) moq5_latch("recorder: a codec input is longer than the log records");
    return i;
}

static void moq5_codec_end(int i, moq_result_t rc, size_t out_len)
{
    if (i < 0) return;
    pthread_mutex_lock(&g_codec_mu);
    g_codec_calls[i].rc = (long)rc;
    g_codec_calls[i].out_len = (long)out_len;
    pthread_mutex_unlock(&g_codec_mu);
}

moq_result_t moq5_test_codec_init_data_build(const moq_codec_init_data_cfg_t *cfg, uint8_t *buf, size_t cap, size_t *out_len);
moq_result_t moq5_test_codec_init_data_build(const moq_codec_init_data_cfg_t *cfg, uint8_t *buf, size_t cap, size_t *out_len)
{
    int inject, inject_rc, i;
    long inject_len;
    moq_result_t rc;
    i = moq5_codec_begin(1, cfg != NULL ? (long)cfg->source_format : -1, cfg != NULL ? (long)cfg->nal.length_size : -1, 0, 0,
                         cfg != NULL ? cfg->source.data : NULL, cfg != NULL ? cfg->source.len : 0, NULL, 0, cap,
                         &inject, &inject_rc, &inject_len);
    if (inject) {
        if (out_len != NULL) *out_len = (size_t)inject_len;
        moq5_codec_end(i, (moq_result_t)inject_rc, (size_t)inject_len);
        return (moq_result_t)inject_rc;
    }
    rc = moq_codec_init_data_build(cfg, buf, cap, out_len);
    moq5_codec_end(i, rc, out_len != NULL ? *out_len : 0);
    return rc;
}

moq_result_t moq5_test_codec_string_format(const moq_codec_string_cfg_t *cfg, uint8_t *buf, size_t cap, size_t *out_len);
moq_result_t moq5_test_codec_string_format(const moq_codec_string_cfg_t *cfg, uint8_t *buf, size_t cap, size_t *out_len)
{
    int inject, inject_rc, i;
    long inject_len;
    moq_result_t rc;
    i = moq5_codec_begin(2, cfg != NULL ? (long)cfg->config_format : -1, -1,
                         cfg != NULL ? (long)cfg->has_mp4_object_type_indication : 0,
                         cfg != NULL ? (long)cfg->mp4_object_type_indication : 0,
                         cfg != NULL ? cfg->decoder_config.data : NULL, cfg != NULL ? cfg->decoder_config.len : 0,
                         cfg != NULL ? cfg->sample_entry.data : NULL, cfg != NULL ? cfg->sample_entry.len : 0, cap,
                         &inject, &inject_rc, &inject_len);
    if (inject) {
        if (out_len != NULL) *out_len = (size_t)inject_len;
        moq5_codec_end(i, (moq_result_t)inject_rc, (size_t)inject_len);
        return (moq_result_t)inject_rc;
    }
    rc = moq_codec_string_format(cfg, buf, cap, out_len);
    moq5_codec_end(i, rc, out_len != NULL ? *out_len : 0);
    return rc;
}

static void JNICALL ledger_nativeCodecReset(JNIEnv *env, jclass self)
{
    (void)env; (void)self;
    pthread_mutex_lock(&g_codec_mu);
    g_codec_ncalls = 0;
    g_codec_inject_at = -1;
    memset(g_codec_calls, 0, sizeof g_codec_calls);
    pthread_mutex_unlock(&g_codec_mu);
}

/* One synthetic result for call [at]: the utility is not called for it. */
static void JNICALL ledger_nativeCodecInject(JNIEnv *env, jclass self, jint at, jint rc, jlong out_len)
{
    (void)env; (void)self;
    pthread_mutex_lock(&g_codec_mu);
    g_codec_inject_at = (int)at;
    g_codec_inject_rc = (int)rc;
    g_codec_inject_len = (long)out_len;
    pthread_mutex_unlock(&g_codec_mu);
}

/* [kind, format, length_size, has_oti, oti, cap, rc, out_len, input_len, entry_len, injected] per call. */
static jlongArray JNICALL ledger_nativeCodecCalls(JNIEnv *env, jclass self)
{
    jlong v[MOQ5_CODEC_CALLS * MOQ5_CODEC_FIELDS];
    jlongArray out;
    int n, i;
    (void)self;
    pthread_mutex_lock(&g_codec_mu);
    n = g_codec_ncalls < MOQ5_CODEC_CALLS ? g_codec_ncalls : MOQ5_CODEC_CALLS;
    for (i = 0; i < n; i++) {
        const moq5_codec_call_t *c = &g_codec_calls[i];
        jlong *r = &v[i * MOQ5_CODEC_FIELDS];
        r[0] = c->kind; r[1] = c->format; r[2] = c->length_size; r[3] = c->has_oti; r[4] = c->oti;
        r[5] = c->cap; r[6] = c->rc; r[7] = c->out_len; r[8] = c->input_len; r[9] = c->entry_len; r[10] = c->injected;
    }
    pthread_mutex_unlock(&g_codec_mu);
    out = (*env)->NewLongArray(env, (jsize)(n * MOQ5_CODEC_FIELDS));
    if (out != NULL) (*env)->SetLongArrayRegion(env, out, 0, (jsize)(n * MOQ5_CODEC_FIELDS), v);
    return out;
}

/* The recorded input (or, when [entry], the sample entry) of call [i]. */
static jbyteArray JNICALL ledger_nativeCodecBytes(JNIEnv *env, jclass self, jint i, jboolean entry)
{
    uint8_t copy[MOQ5_CODEC_BYTES];
    jsize n = 0;
    jbyteArray out;
    (void)self;
    pthread_mutex_lock(&g_codec_mu);
    if (i >= 0 && i < g_codec_ncalls && i < MOQ5_CODEC_CALLS) {
        const moq5_codec_call_t *c = &g_codec_calls[i];
        long len = entry ? c->entry_len : c->input_len;
        long cap = entry ? (long)sizeof c->entry : MOQ5_CODEC_BYTES;
        n = (jsize)(len < cap ? len : cap);
        memcpy(copy, entry ? c->entry : c->input, (size_t)n);
    }
    pthread_mutex_unlock(&g_codec_mu);
    out = (*env)->NewByteArray(env, n);
    if (out != NULL && n > 0) (*env)->SetByteArrayRegion(env, out, 0, n, (const jbyte *)copy);
    return out;
}

/* A CONTROL through the same forwarding names the product is redirected to:
 * the size query, then the call into an exact buffer, returned as bytes or,
 * on any status other than OK, as NULL with the status in status[0]. It
 * proves the forwarding reaches the real utility and that an injection
 * replaces exactly one call; it is not the product row. */
static jbyteArray JNICALL ledger_nativeCodecControl(JNIEnv *env, jclass self, jint kind, jint format, jbyteArray input,
                                                    jint length_size, jbyteArray entry, jboolean has_oti, jint oti,
                                                    jintArray status)
{
    jsize n = (*env)->GetArrayLength(env, input), en = entry != NULL ? (*env)->GetArrayLength(env, entry) : 0;
    uint8_t in[MOQ5_CODEC_BYTES], ent[8], *buf = NULL;
    size_t need = 0, got = 0;
    moq_result_t rc;
    jint st;
    jbyteArray out = NULL;
    (void)self;
    if (n > MOQ5_CODEC_BYTES || en > 8) { st = MOQ_ERR_INVAL; (*env)->SetIntArrayRegion(env, status, 0, 1, &st); return NULL; }
    (*env)->GetByteArrayRegion(env, input, 0, n, (jbyte *)in);
    if (en > 0) (*env)->GetByteArrayRegion(env, entry, 0, en, (jbyte *)ent);
    if (kind == 1) {
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = (moq_codec_source_format_t)format;
        cfg.source.data = in; cfg.source.len = (size_t)n;
        cfg.nal.length_size = (uint8_t)length_size;
        rc = moq5_test_codec_init_data_build(&cfg, NULL, 0, &need);
        if (rc == MOQ_ERR_BUFFER && need > 0 && (buf = (uint8_t *)malloc(need)) != NULL)
            rc = moq5_test_codec_init_data_build(&cfg, buf, need, &got);
    } else {
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = (moq_codec_config_format_t)format;
        cfg.sample_entry.data = ent; cfg.sample_entry.len = (size_t)en;
        cfg.has_mp4_object_type_indication = has_oti != JNI_FALSE;
        cfg.mp4_object_type_indication = (uint8_t)oti;
        cfg.decoder_config.data = in; cfg.decoder_config.len = (size_t)n;
        rc = moq5_test_codec_string_format(&cfg, NULL, 0, &need);
        if (rc == MOQ_ERR_BUFFER && need > 0 && (buf = (uint8_t *)malloc(need)) != NULL)
            rc = moq5_test_codec_string_format(&cfg, buf, need, &got);
    }
    st = (jint)rc;
    if (rc == MOQ_OK && buf != NULL) {
        out = (*env)->NewByteArray(env, (jsize)got);
        if (out != NULL) (*env)->SetByteArrayRegion(env, out, 0, (jsize)got, (const jbyte *)buf);
    }
    free(buf);
    (*env)->SetIntArrayRegion(env, status, 0, 1, &st);
    return out;
}

/* Arms the CALLING thread: its table is swapped for the forwarding copy, the
 * operation log is cleared, and the ([skip] + 1)th occurrence of [site] (0 for
 * none) is faulted once with [throwable] left pending. */
static void JNICALL ledger_nativeCodecEnvArm(JNIEnv *env, jclass self, jint site, jint skip, jthrowable throwable)
{
    moq5_probe_thread_t *t;
    jthrowable g = throwable != NULL ? (jthrowable)(*env)->NewGlobalRef(env, throwable) : NULL;
    (void)self;
    pthread_mutex_lock(&g_mu);
    t = moq5_thread_by_tid(pthread_self(), 1);
    if (t != NULL) moq5_env_swap(t, env);
    pthread_mutex_unlock(&g_mu);
    if (t == NULL) { moq5_latch("recorder: no thread record is free to arm a codec fault"); return; }
    pthread_mutex_lock(&g_codec_env_mu);
    if (g_codec_throwable != NULL) moq5_latch("recorder: a codec fault was armed twice without a disarm");
    g_codec_env = env;
    g_codec_site = (int)site;
    g_codec_skip = (int)skip;
    g_codec_fired = 0;
    g_codec_throwable = g;
    g_codec_nops = 0;
    pthread_mutex_unlock(&g_codec_env_mu);
}

/* Disarms and restores the calling thread's table; returns [fired, op...]. */
static jintArray JNICALL ledger_nativeCodecEnvDisarm(JNIEnv *env, jclass self)
{
    jint v[MOQ5_CODEC_OPS + 1];
    jintArray out;
    jthrowable g;
    jsize n, i;
    moq5_probe_thread_t *t;
    (void)self;
    pthread_mutex_lock(&g_codec_env_mu);
    v[0] = (jint)g_codec_fired;
    n = (jsize)(g_codec_nops < MOQ5_CODEC_OPS ? g_codec_nops : MOQ5_CODEC_OPS);
    for (i = 0; i < n; i++) v[i + 1] = (jint)g_codec_ops[i];
    if (g_codec_nops > MOQ5_CODEC_OPS) moq5_latch("recorder: more codec JNI operations than the log holds");
    g = g_codec_throwable;
    g_codec_throwable = NULL;
    g_codec_env = NULL;
    g_codec_site = 0;
    pthread_mutex_unlock(&g_codec_env_mu);
    pthread_mutex_lock(&g_mu);
    t = moq5_thread_by_tid(pthread_self(), 0);
    if (t != NULL) moq5_env_unswap(t);
    pthread_mutex_unlock(&g_mu);
    if (g != NULL) g_real_fns->DeleteGlobalRef(env, g);
    out = g_real_fns->NewIntArray(env, n + 1);
    if (out != NULL) g_real_fns->SetIntArrayRegion(env, out, 0, n + 1, v);
    return out;
}

/* A CONTROL that performs the four byte-array operations a codec row owes, in
 * the product's order, with an exception check after each and a local
 * deletion on the paths that hold one. It exists to prove each armed site is
 * REACHED and that nothing after it runs; it is not the product row.
 * Returns 0 when all four completed, or the number of the operation it left at. */
static jint JNICALL ledger_nativeCodecEnvControl(JNIEnv *env, jclass self, jbyteArray src, jobjectArray out)
{
    jsize n = (*env)->GetArrayLength(env, src);
    jbyte buf[64];
    jbyteArray arr;
    (void)self;
    if (n > (jsize)sizeof buf) return -1;
    (*env)->GetByteArrayRegion(env, src, 0, n, buf);
    if ((*env)->ExceptionCheck(env)) return MOQ5_CODEC_OP_GET_REGION;
    arr = (*env)->NewByteArray(env, n);
    if (arr == NULL) return (*env)->ExceptionCheck(env) ? MOQ5_CODEC_OP_NEW_ARRAY : MOQ5_CODEC_SITE_NEW_ARRAY_NULL;
    (*env)->SetByteArrayRegion(env, arr, 0, n, buf);
    if ((*env)->ExceptionCheck(env)) { (*env)->DeleteLocalRef(env, arr); return MOQ5_CODEC_OP_SET_REGION; }
    (*env)->SetObjectArrayElement(env, out, 0, arr);
    if ((*env)->ExceptionCheck(env)) { (*env)->DeleteLocalRef(env, arr); return MOQ5_CODEC_OP_SET_ELEMENT; }
    (*env)->DeleteLocalRef(env, arr);
    return 0;
}

/* -- the codec rows' native allocations, observed and faulted ---------------- *
 * The product translation unit's malloc/free are redirected here at compile
 * time. Outside the ARMED owner thread they forward untouched. On it, every
 * attempt is numbered; attempt [fail_at] answers NULL without calling the
 * allocator; every successful block is owned by this ledger until a free
 * retires it. A free is matched against the CURRENT live owner of its address
 * first, so an address the allocator reused after an earlier retirement is an
 * ordinary free of the newer block. Only an address with no live owner is
 * classified from history: retired before (a double free) or never owned (a
 * foreign free). Neither is forwarded, so a defect cannot corrupt the host
 * heap. The pointer is untagged: a stale alias to an address that has since
 * been reused cannot be told apart from the current block, and nothing here
 * claims otherwise.
 *
 * Arming belongs to one thread until that thread disarms. A second arm, or a
 * disarm from another thread, is refused and leaves the inventory intact. On
 * disarm every block still live is reported, then freed here, and counted as
 * reclaimed. */
#define MOQ5_ALLOC_SLOTS 32
#define MOQ5_ALLOC_LIVE    0
#define MOQ5_ALLOC_RETIRED 1
#define MOQ5_ALLOC_FAILED  2

static pthread_mutex_t g_alloc_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_alloc_armed;
static pthread_t g_alloc_thread;
static int g_alloc_fail_at = -1;
static int g_alloc_attempts;
static void *g_alloc_ptr[MOQ5_ALLOC_SLOTS];
static long g_alloc_size[MOQ5_ALLOC_SLOTS];
static int g_alloc_state[MOQ5_ALLOC_SLOTS];
static long g_alloc_double_frees;
static long g_alloc_foreign_frees;

/* lock held; the armed owner only. Records one attempt; returns its index or -1 when full. */
static int moq5_alloc_note_attempt_locked(void *p, size_t n, int failed)
{
    int i = g_alloc_attempts++;
    if (i >= MOQ5_ALLOC_SLOTS) return -1;
    g_alloc_size[i] = (long)n;
    g_alloc_ptr[i] = failed ? NULL : p;
    g_alloc_state[i] = (failed || p == NULL) ? MOQ5_ALLOC_FAILED : MOQ5_ALLOC_LIVE;
    return i;
}

/* lock held; the armed owner only. Classifies a free of [p]; returns 1 when it
 * retired a live block (the caller forwards it), 0 when it is a double or
 * foreign free (the caller does not). */
static int moq5_alloc_note_free_locked(void *p)
{
    int i, n = g_alloc_attempts < MOQ5_ALLOC_SLOTS ? g_alloc_attempts : MOQ5_ALLOC_SLOTS;
    for (i = n - 1; i >= 0; i--) {
        if (g_alloc_ptr[i] == p && g_alloc_state[i] == MOQ5_ALLOC_LIVE) {
            g_alloc_state[i] = MOQ5_ALLOC_RETIRED;
            return 1;
        }
    }
    for (i = 0; i < n; i++) {
        if (g_alloc_ptr[i] == p) { g_alloc_double_frees++; return 0; }
    }
    g_alloc_foreign_frees++;
    return 0;
}

static int moq5_alloc_is_owner_locked(void)
{
    return g_alloc_armed && pthread_equal(g_alloc_thread, pthread_self());
}

void *moq5_test_malloc(size_t n);
void *moq5_test_malloc(size_t n)
{
    int i, fail;
    void *p;
    pthread_mutex_lock(&g_alloc_mu);
    if (!moq5_alloc_is_owner_locked()) {
        pthread_mutex_unlock(&g_alloc_mu);
        return malloc(n);
    }
    fail = (g_alloc_attempts == g_alloc_fail_at);
    p = fail ? NULL : malloc(n);
    i = moq5_alloc_note_attempt_locked(p, n, fail);
    pthread_mutex_unlock(&g_alloc_mu);
    if (i < 0) {
        /* Over the ledger's bound: the block would be unowned, so it is not handed out. */
        free(p);
        moq5_latch("recorder: more codec allocation attempts than the ledger holds");
        return NULL;
    }
    return p;
}

void moq5_test_free(void *p);
void moq5_test_free(void *p)
{
    int forward = 1;
    if (p == NULL) return;
    pthread_mutex_lock(&g_alloc_mu);
    if (moq5_alloc_is_owner_locked()) forward = moq5_alloc_note_free_locked(p);
    pthread_mutex_unlock(&g_alloc_mu);
    if (forward) free(p);
}

/* Arms the CALLING thread; attempt [fail_at] (-1 for none) answers NULL.
 * Returns false, changing nothing, when an inventory is already armed. */
static jboolean JNICALL ledger_nativeCodecAllocArm(JNIEnv *env, jclass self, jint fail_at)
{
    (void)env; (void)self;
    pthread_mutex_lock(&g_alloc_mu);
    if (g_alloc_armed) {
        pthread_mutex_unlock(&g_alloc_mu);
        return JNI_FALSE;
    }
    g_alloc_armed = 1;
    g_alloc_thread = pthread_self();
    g_alloc_fail_at = (int)fail_at;
    g_alloc_attempts = 0;
    g_alloc_double_frees = 0;
    g_alloc_foreign_frees = 0;
    memset(g_alloc_ptr, 0, sizeof g_alloc_ptr);
    memset(g_alloc_size, 0, sizeof g_alloc_size);
    memset(g_alloc_state, 0, sizeof g_alloc_state);
    pthread_mutex_unlock(&g_alloc_mu);
    return JNI_TRUE;
}

/* Disarms, from the OWNER thread only; returns [attempts, double frees,
 * foreign frees, reclaimed, (size, state) per attempt], state 0 live,
 * 1 retired, 2 failed, as they stood BEFORE reclamation. Every block still
 * live is then freed here; [reclaimed] counts the frees actually executed,
 * not the live records, so a skipped release shows. Returns null, changing
 * nothing, when nothing is armed or another thread owns the inventory. */
static jlongArray JNICALL ledger_nativeCodecAllocDisarm(JNIEnv *env, jclass self)
{
    jlong v[4 + 2 * MOQ5_ALLOC_SLOTS];
    void *live[MOQ5_ALLOC_SLOTS];
    int n, i, nl = 0;
    jlongArray out;
    (void)self;
    pthread_mutex_lock(&g_alloc_mu);
    if (!moq5_alloc_is_owner_locked()) {
        pthread_mutex_unlock(&g_alloc_mu);
        return NULL;
    }
    n = g_alloc_attempts < MOQ5_ALLOC_SLOTS ? g_alloc_attempts : MOQ5_ALLOC_SLOTS;
    for (i = 0; i < n; i++) {
        v[4 + 2 * i] = (jlong)g_alloc_size[i];
        v[5 + 2 * i] = (jlong)g_alloc_state[i];
        if (g_alloc_state[i] == MOQ5_ALLOC_LIVE && g_alloc_ptr[i] != NULL) live[nl++] = g_alloc_ptr[i];
    }
    v[0] = (jlong)g_alloc_attempts;
    v[1] = (jlong)g_alloc_double_frees;
    v[2] = (jlong)g_alloc_foreign_frees;
    g_alloc_armed = 0;
    g_alloc_fail_at = -1;
    memset(g_alloc_ptr, 0, sizeof g_alloc_ptr);
    pthread_mutex_unlock(&g_alloc_mu);
    v[3] = 0;
    for (i = 0; i < nl; i++) {
        free(live[i]);
        v[3]++;
    }
    out = (*env)->NewLongArray(env, (jsize)(4 + 2 * n));
    if (out != NULL) (*env)->SetLongArrayRegion(env, out, 0, (jsize)(4 + 2 * n), v);
    return out;
}

/* A CONTROL with a formatter's allocation shape: two inputs, then the output,
 * each checked, earlier blocks retired on a later failure, all retired on
 * success. [omit_first_free] skips retiring the first input on every path,
 * so the leak report can be seen to fail by name; the ledger's disarm then
 * reclaims it. It is not the product row. Returns MOQ_OK or MOQ_ERR_NOMEM. */
static jint JNICALL ledger_nativeCodecAllocControl(JNIEnv *env, jclass self, jboolean omit_first_free)
{
    uint8_t *a, *b, *c;
    (void)env; (void)self;
    a = (uint8_t *)moq5_test_malloc(8);
    if (a == NULL) return MOQ_ERR_NOMEM;
    b = (uint8_t *)moq5_test_malloc(8);
    if (b == NULL) { if (!omit_first_free) moq5_test_free(a); return MOQ_ERR_NOMEM; }
    c = (uint8_t *)moq5_test_malloc(16);
    if (c == NULL) { if (!omit_first_free) moq5_test_free(a); moq5_test_free(b); return MOQ_ERR_NOMEM; }
    memset(a, 1, 8); memset(b, 2, 8); memset(c, 3, 16);
    if (!omit_first_free) moq5_test_free(a);
    moq5_test_free(b);
    moq5_test_free(c);
    return MOQ_OK;
}

/* A CONTROL of the free classification itself, deterministic: one block this
 * control owns (allocated and released with the real allocator, outside the
 * ledger) is NOTED as allocated, freed, noted again at the SAME address, and
 * freed again -- the reuse the allocator may or may not choose to make, here
 * made certain. Then a true double free of that address and a foreign free
 * are noted. It returns [forwarded decisions..., records disowned] and
 * releases its block on every path: a record of that block the
 * classification left live is disowned before the release, counted in the
 * last element, so disarm never frees the block a second time. Requires the
 * calling thread to own the armed inventory. */
static jintArray JNICALL ledger_nativeCodecAllocReuseControl(JNIEnv *env, jclass self)
{
    static int foreign_target;
    jint d[5] = { -1, -1, -1, -1, -1 };
    jintArray out;
    int first, second;
    void *block = malloc(16);
    (void)self;
    if (block == NULL) return NULL;
    pthread_mutex_lock(&g_alloc_mu);
    if (moq5_alloc_is_owner_locked()) {
        first = moq5_alloc_note_attempt_locked(block, 16, 0);
        d[0] = moq5_alloc_note_free_locked(block);          /* 1: retires the first owner */
        second = moq5_alloc_note_attempt_locked(block, 16, 0);
        d[1] = moq5_alloc_note_free_locked(block);          /* 1: retires the REUSED owner, not a double free */
        d[2] = moq5_alloc_note_free_locked(block);          /* 0: now a true double free */
        d[3] = moq5_alloc_note_free_locked(&foreign_target); /* 0: never owned */
        d[4] = 0;
        if (first >= 0 && g_alloc_state[first] == MOQ5_ALLOC_LIVE) { g_alloc_ptr[first] = NULL; d[4]++; }
        if (second >= 0 && g_alloc_state[second] == MOQ5_ALLOC_LIVE) { g_alloc_ptr[second] = NULL; d[4]++; }
    }
    pthread_mutex_unlock(&g_alloc_mu);
    free(block);
    out = (*env)->NewIntArray(env, 5);
    if (out != NULL) (*env)->SetIntArrayRegion(env, out, 0, 5, d);
    return out;
}

/* A CONTROL that allocates one 32-byte block through the ledger and does not
 * free it, so a row can fail after an owned allocation and prove the scoped
 * disarm reclaims it. Returns MOQ_OK or MOQ_ERR_NOMEM. */
static jint JNICALL ledger_nativeCodecAllocHold(JNIEnv *env, jclass self)
{
    (void)env; (void)self;
    return moq5_test_malloc(32) != NULL ? MOQ_OK : MOQ_ERR_NOMEM;
}

static const JNINativeMethod ledger_methods[] = {
    { "nativeArmAllocFault", "(ZI)V",               (void *)ledger_nativeArmAllocFault },
    { "nativeAllocFaultPending", "()Z",             (void *)ledger_nativeAllocFaultPending },
    { "nativeAddTrackCalls", "()J",                 (void *)ledger_nativeAddTrackCalls },
    { "nativeIngestCatalogAt", "(JJJJ[B)I",         (void *)ledger_nativeIngestCatalogAt },
    { "nativeSetTransportVersion", "(JJI)I",        (void *)ledger_nativeSetTransportVersion },
    { "nativeMarkTrackDesired", "(JJJ)I",           (void *)ledger_nativeMarkTrackDesired },
    { "nativeIngestObject", "(JJJJJIZJ[B)I",        (void *)ledger_nativeIngestObject },
    { "nativeIngestObjectIdentity", "(JJJJJJIZ[B)I", (void *)ledger_nativeIngestObjectIdentity },
    { "nativeCmafFragmentFixture", "()[B",          (void *)ledger_nativeCmafFragmentFixture },
    { "nativeObjectCleanups", "()J",                (void *)ledger_nativeObjectCleanups },
    { "nativeIngestCatalog", "(JJ[B)I",             (void *)ledger_nativeIngestCatalog },
    { "nativeArmCtorFault", "(Ljava/lang/String;)V", (void *)ledger_nativeArmCtorFault },
    { "nativeConsulted", "()V",                  (void *)ledger_nativeConsulted },
    { "nativeCounters",  "()[J",                 (void *)ledger_nativeCounters },
    { "nativeFault",     "()Ljava/lang/String;", (void *)ledger_nativeFault },
    { "nativeCodecReset", "()V",                 (void *)ledger_nativeCodecReset },
    { "nativeCodecInject", "(IIJ)V",             (void *)ledger_nativeCodecInject },
    { "nativeCodecCalls", "()[J",                (void *)ledger_nativeCodecCalls },
    { "nativeCodecBytes", "(IZ)[B",              (void *)ledger_nativeCodecBytes },
    { "nativeCodecControl", "(II[BI[BZI[I)[B",   (void *)ledger_nativeCodecControl },
    { "nativeCodecEnvArm", "(IILjava/lang/Throwable;)V", (void *)ledger_nativeCodecEnvArm },
    { "nativeCodecEnvDisarm", "()[I",            (void *)ledger_nativeCodecEnvDisarm },
    { "nativeCodecEnvControl", "([B[[B)I",       (void *)ledger_nativeCodecEnvControl },
    { "nativeCodecAllocArm", "(I)Z",             (void *)ledger_nativeCodecAllocArm },
    { "nativeCodecAllocDisarm", "()[J",          (void *)ledger_nativeCodecAllocDisarm },
    { "nativeCodecAllocControl", "(Z)I",         (void *)ledger_nativeCodecAllocControl },
    { "nativeCodecAllocReuseControl", "()[I",    (void *)ledger_nativeCodecAllocReuseControl },
    { "nativeCodecAllocHold", "()I",             (void *)ledger_nativeCodecAllocHold },
};

static void moq5_register_ledger(JavaVM *vm)
{
    JNIEnv *env = NULL;
    jclass k;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_6) != JNI_OK || env == NULL) return;
    k = (*env)->FindClass(env, MOQ5_PROBE_CLASS);
    if (k == NULL) { (*env)->ExceptionClear(env); return; }   /* no ledger in this process */
    if ((*env)->RegisterNatives(env, k, ledger_methods,
                                (jint)(sizeof ledger_methods / sizeof ledger_methods[0])) != JNI_OK) {
        (*env)->ExceptionClear(env);
        moq5_latch("recorder: the ledger natives could not be registered");
    }
    (*env)->DeleteLocalRef(env, k);
}

/* -- the load entry: the product receives the forwarding VM ------------------- */
__attribute__((visibility("default")))
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved)
{
    {
        Dl_info di;
        g_self_base = (dladdr((void *)(uintptr_t)&JNI_OnLoad, &di) != 0) ? di.dli_fbase : NULL;
        if (g_self_base == NULL) moq5_latch("recorder: cannot resolve this image's base; local references cannot be attributed");
    }
    g_real_vm = vm;
    g_real_invoke = *vm;
    memcpy(&g_fwd_invoke, g_real_invoke, sizeof g_fwd_invoke);
    g_fwd_invoke.DestroyJavaVM = fwd_DestroyJavaVM;
    g_fwd_invoke.AttachCurrentThread = fwd_AttachCurrentThread;
    g_fwd_invoke.DetachCurrentThread = fwd_DetachCurrentThread;
    g_fwd_invoke.GetEnv = fwd_GetEnv;
    g_fwd_invoke.AttachCurrentThreadAsDaemon = fwd_AttachCurrentThreadAsDaemon;
    g_fwd_vm_ptr = &g_fwd_invoke;
    {
        jint r;
        JNIEnv *lenv = NULL;
        moq5_probe_thread_t *lt = NULL;
        int gnew = 0, gdel = 0, kc, kd;

        /* The LOADING thread's table is swapped for the duration of the
         * product's load only, so the global references the load creates and
         * deletes are counted, and restored before the VM could unload this
         * image on a refusal. */
        if (g_real_invoke->GetEnv(g_real_vm, (void **)&lenv, JNI_VERSION_1_6) == JNI_OK && lenv != NULL) {
            pthread_mutex_lock(&g_mu);
            lt = moq5_thread_by_tid(pthread_self(), 1);
            if (lt != NULL) moq5_env_swap(lt, lenv);
            pthread_mutex_unlock(&g_mu);
        }
        r = moq5_product_JNI_OnLoad((JavaVM *)&g_fwd_vm_ptr, reserved);
        pthread_mutex_lock(&g_mu);
        if (lt != NULL) {
            gnew = lt->globals_new;
            gdel = lt->globals_deleted;
            moq5_env_unswap(lt);
            memset(lt, 0, sizeof *lt);
        }
        pthread_mutex_unlock(&g_mu);
        if (r == JNI_ERR) {
            /* A refused load is followed by the VM unloading this image, so no
             * thread may keep a table that lives in it. */
            int i;
            pthread_mutex_lock(&g_mu);
            for (i = 0; i < MOQ5_PROBE_THREADS; i++) {
                if (g_threads[i].used) moq5_env_unswap(&g_threads[i]);
                memset(&g_threads[i], 0, sizeof g_threads[i]);
            }
            pthread_mutex_unlock(&g_mu);
            pthread_mutex_lock(&g_key_mu);
            kc = g_keys_created; kd = g_keys_deleted;
            pthread_mutex_unlock(&g_key_mu);
            /* The causal ledger of a refused load, for the row that judges
             * whether the product released what it had initialised. */
            (void)fprintf(stderr, "MOQ5-TRUST-PROBE: load refused: globals new=%d deleted=%d, "
                          "keys created=%d deleted=%d\n", gnew, gdel, kc, kd);
            (void)fflush(stderr);
        } else {
            moq5_register_ledger(g_real_vm);
        }
        return r;
    }
}

/* -- the Kotlin ledger -------------------------------------------------------- */
typedef struct moq5_probe_java {
    JNIEnv *env;
    jclass klass;
    jmethodID route;        /* (ZZZILjava/lang/String;I[IJZILjava/lang/String;)V */
    jmethodID case_count;   /* ()I */
    jmethodID case_chain;   /* (I)[[B */
    jmethodID case_name;    /* (I)Ljava/lang/String; */
    jmethodID case_repeats; /* (I)I */
    jmethodID case_preatt;  /* (I)Z */
    jmethodID record;       /* (IIIZZIIIIIIIJ)V */
    jmethodID thread_done;  /* (IIIIIII)V */
    jmethodID case_done;    /* (I)V */
} moq5_probe_java_t;

static int moq5_probe_bind(moq5_probe_java_t *j)
{
    memset(j, 0, sizeof *j);
    if (g_real_vm == NULL) { moq5_probe_say("no JavaVM captured at load"); return 0; }
    if (g_real_invoke->GetEnv(g_real_vm, (void **)&j->env, JNI_VERSION_1_6) != JNI_OK || j->env == NULL) {
        moq5_probe_say("the route was entered off a JVM thread");
        return 0;
    }
    if ((*j->env)->PushLocalFrame(j->env, 64) != 0) {
        (*j->env)->ExceptionClear(j->env);
        moq5_probe_say("cannot push a local frame");
        return 0;
    }
    j->klass = (*j->env)->FindClass(j->env, MOQ5_PROBE_CLASS);
    if (j->klass == NULL) {
        (*j->env)->ExceptionClear(j->env);
        moq5_probe_say("the probe class " MOQ5_PROBE_CLASS " is not loadable");
        (*j->env)->PopLocalFrame(j->env, NULL);
        return 0;
    }
#define MOQ5_PROBE_STATIC(field, name, sig)                                     \
    do {                                                                       \
        j->field = (*j->env)->GetStaticMethodID(j->env, j->klass, name, sig);   \
        if (j->field == NULL) {                                                \
            (*j->env)->ExceptionClear(j->env);                                 \
            moq5_probe_say("the probe class lacks " name sig);                 \
            (*j->env)->PopLocalFrame(j->env, NULL);                            \
            return 0;                                                          \
        }                                                                      \
    } while (0)
    MOQ5_PROBE_STATIC(route,        "route",          "(ZZZILjava/lang/String;I[IJZILjava/lang/String;)V");
    MOQ5_PROBE_STATIC(case_count,   "caseCount",      "()I");
    MOQ5_PROBE_STATIC(case_chain,   "caseChain",      "(I)[[B");
    MOQ5_PROBE_STATIC(case_name,    "caseServerName", "(I)Ljava/lang/String;");
    MOQ5_PROBE_STATIC(case_repeats, "caseRepeats",    "(I)I");
    MOQ5_PROBE_STATIC(case_preatt,  "casePreAttached", "(I)Z");
    MOQ5_PROBE_STATIC(record,       "record",         "(IIIZZIIIIIIIJ)V");
    MOQ5_PROBE_STATIC(thread_done,  "threadDone",     "(IIIIIII)V");
    MOQ5_PROBE_STATIC(case_done,    "caseDone",       "(I)V");
#undef MOQ5_PROBE_STATIC
    return 1;
}

static void moq5_probe_unbind(moq5_probe_java_t *j)
{
    if (j->env != NULL) (*j->env)->PopLocalFrame(j->env, NULL);
}

/* -- one scripted case, driven on a foreign thread -------------------------- */
typedef struct moq5_probe_snapshot {
    int verdict;
    int attached;
    int pending;
    int getenv, attach_daemon, attach, detach, high, live, created;
    long consulted_prefix;   /* consultations so far, read right after THIS call */
} moq5_probe_snapshot_t;

typedef struct moq5_probe_case {
    moq_endpoint_trust_fn fn;
    void *ctx;
    moq_bytes_t name;
    char *name_buf;
    moq_bytes_t *certs;
    uint8_t **bufs;
    size_t count;
    int repeats;
    int pre_attached;
    moq5_probe_snapshot_t *snap;
    moq5_probe_thread_t final_counts;
    int harness_failed;
} moq5_probe_case_t;

static void moq5_snapshot_counts(moq5_probe_snapshot_t *s)
{
    moq5_probe_thread_t *t;
    pthread_mutex_lock(&g_mu);
    t = moq5_thread_by_tid(pthread_self(), 0);
    if (t != NULL) {
        s->getenv = t->getenv; s->attach_daemon = t->attach_daemon; s->attach = t->attach;
        s->detach = t->detach; s->high = t->high; s->live = t->live; s->created = t->created;
    }
    pthread_mutex_unlock(&g_mu);
    s->consulted_prefix = moq5_read_consulted();
}

static void *moq5_probe_case_main(void *arg)
{
    moq5_probe_case_t *c = (moq5_probe_case_t *)arg;
    JNIEnv *own = NULL;
    int i;

    if (c->pre_attached) {
        /* A thread the LIBRARY did not attach: attached here, through the real
         * VM, and given the forwarding table so the product's calls on it are
         * still counted. The product must attach nothing and detach nothing. */
        moq5_probe_thread_t *t;
        if (g_real_invoke->AttachCurrentThreadAsDaemon(g_real_vm, (void **)&own, NULL) != JNI_OK || own == NULL) {
            c->harness_failed = 1;
            return NULL;
        }
        pthread_mutex_lock(&g_mu);
        t = moq5_thread_by_tid(pthread_self(), 1);
        if (t != NULL) moq5_env_swap(t, own);
        pthread_mutex_unlock(&g_mu);
    }
    for (i = 0; i < c->repeats; i++) {
        JNIEnv *te = NULL;
        c->snap[i].verdict = (int)c->fn(c->ctx, c->name, c->certs, c->count);
        /* Observed AFTER the call, on the same thread, through the REAL VM: is
         * it still attached, and did the delegate leave a Java exception
         * pending? Nothing is cleared here. */
        c->snap[i].attached =
            (g_real_invoke->GetEnv(g_real_vm, (void **)&te, JNI_VERSION_1_6) == JNI_OK && te != NULL) ? 1 : 0;
        c->snap[i].pending = (c->snap[i].attached && g_real_fns != NULL)
                                 ? (g_real_fns->ExceptionCheck(te) ? 1 : 0)
                                 : (c->snap[i].attached ? ((*te)->ExceptionCheck(te) ? 1 : 0) : 0);
        /* The consultation prefix is read HERE, on the foreign thread, before the
         * next call can start: it is what makes call k's verdict causally
         * follow call k's own consultation. */
        moq5_snapshot_counts(&c->snap[i]);
    }
    if (c->pre_attached) {
        moq5_probe_thread_t *t;
        pthread_mutex_lock(&g_mu);
        t = moq5_thread_by_tid(pthread_self(), 0);
        if (t != NULL) moq5_env_unswap(t);
        pthread_mutex_unlock(&g_mu);
        (void)g_real_invoke->DetachCurrentThread(g_real_vm);
    }
    return NULL;
}

static void moq5_probe_case_free(moq5_probe_case_t *c)
{
    size_t i;
    if (c->bufs != NULL) {
        for (i = 0; i < c->count; i++) free(c->bufs[i]);
    }
    free(c->bufs);
    free(c->certs);
    free(c->name_buf);
    free(c->snap);
    memset(c, 0, sizeof *c);
}

static int moq5_probe_case_load(moq5_probe_java_t *j, jint index, moq5_probe_case_t *c,
                                moq_endpoint_trust_fn fn, void *ctx)
{
    jobjectArray chain;
    jstring name;
    jint n, i, repeats;
    jboolean pre;

    memset(c, 0, sizeof *c);
    c->fn = fn;
    c->ctx = ctx;

    if ((*j->env)->PushLocalFrame(j->env, 16) != 0) {
        (*j->env)->ExceptionClear(j->env);
        moq5_probe_say("cannot push a per-case local frame");
        return 0;
    }
    chain = (jobjectArray)(*j->env)->CallStaticObjectMethod(j->env, j->klass, j->case_chain, index);
    if ((*j->env)->ExceptionCheck(j->env) || chain == NULL) {
        (*j->env)->ExceptionClear(j->env);
        moq5_probe_say("caseChain threw or returned null");
        (*j->env)->PopLocalFrame(j->env, NULL);
        return 0;
    }
    n = (*j->env)->GetArrayLength(j->env, chain);
    c->count = (size_t)n;
    if (n > 0) {
        c->certs = (moq_bytes_t *)calloc((size_t)n, sizeof *c->certs);
        c->bufs = (uint8_t **)calloc((size_t)n, sizeof *c->bufs);
        if (c->certs == NULL || c->bufs == NULL) {
            moq5_probe_say("out of memory copying a chain");
            (*j->env)->PopLocalFrame(j->env, NULL);
            moq5_probe_case_free(c);
            return 0;
        }
        for (i = 0; i < n; i++) {
            jbyteArray der = (jbyteArray)(*j->env)->GetObjectArrayElement(j->env, chain, i);
            jsize len = (der == NULL) ? 0 : (*j->env)->GetArrayLength(j->env, der);
            c->bufs[i] = (uint8_t *)malloc((size_t)len + 1u);
            if (c->bufs[i] == NULL) {
                if (der != NULL) (*j->env)->DeleteLocalRef(j->env, der);
                moq5_probe_say("out of memory copying a certificate");
                (*j->env)->PopLocalFrame(j->env, NULL);
                moq5_probe_case_free(c);
                return 0;
            }
            if (len > 0) (*j->env)->GetByteArrayRegion(j->env, der, 0, len, (jbyte *)c->bufs[i]);
            c->certs[i].data = c->bufs[i];
            c->certs[i].len = (size_t)len;
            if (der != NULL) (*j->env)->DeleteLocalRef(j->env, der);
        }
    }
    name = (jstring)(*j->env)->CallStaticObjectMethod(j->env, j->klass, j->case_name, index);
    if ((*j->env)->ExceptionCheck(j->env)) {
        (*j->env)->ExceptionClear(j->env);
        moq5_probe_say("caseServerName threw");
        (*j->env)->PopLocalFrame(j->env, NULL);
        moq5_probe_case_free(c);
        return 0;
    }
    if (name != NULL) {
        const char *utf = (*j->env)->GetStringUTFChars(j->env, name, NULL);
        if (utf == NULL) {
            (*j->env)->ExceptionClear(j->env);
            moq5_probe_say("cannot read the case server name");
            (*j->env)->PopLocalFrame(j->env, NULL);
            moq5_probe_case_free(c);
            return 0;
        }
        {
            size_t n_len = strlen(utf);
            c->name_buf = (char *)malloc(n_len + 1u);
            if (c->name_buf != NULL) memcpy(c->name_buf, utf, n_len + 1u);
        }
        (*j->env)->ReleaseStringUTFChars(j->env, name, utf);
        if (c->name_buf == NULL) {
            moq5_probe_say("out of memory copying the server name");
            (*j->env)->PopLocalFrame(j->env, NULL);
            moq5_probe_case_free(c);
            return 0;
        }
        c->name.data = (const uint8_t *)c->name_buf;
        c->name.len = strlen(c->name_buf);
    }
    repeats = (*j->env)->CallStaticIntMethod(j->env, j->klass, j->case_repeats, index);
    if ((*j->env)->ExceptionCheck(j->env)) {
        (*j->env)->ExceptionClear(j->env);
        moq5_probe_say("caseRepeats threw");
        (*j->env)->PopLocalFrame(j->env, NULL);
        moq5_probe_case_free(c);
        return 0;
    }
    pre = (*j->env)->CallStaticBooleanMethod(j->env, j->klass, j->case_preatt, index);
    if ((*j->env)->ExceptionCheck(j->env)) {
        (*j->env)->ExceptionClear(j->env);
        moq5_probe_say("casePreAttached threw");
        (*j->env)->PopLocalFrame(j->env, NULL);
        moq5_probe_case_free(c);
        return 0;
    }
    (*j->env)->PopLocalFrame(j->env, NULL);
    c->repeats = (repeats < 1) ? 1 : (int)repeats;
    c->pre_attached = (pre == JNI_TRUE);
    c->snap = (moq5_probe_snapshot_t *)calloc((size_t)c->repeats, sizeof *c->snap);
    if (c->snap == NULL) {
        moq5_probe_say("out of memory recording a case");
        moq5_probe_case_free(c);
        return 0;
    }
    return 1;
}

static void moq5_probe_invoke_cases(moq5_probe_java_t *j, moq_endpoint_trust_fn fn, void *ctx)
{
    jint count, index;

    count = (*j->env)->CallStaticIntMethod(j->env, j->klass, j->case_count);
    if ((*j->env)->ExceptionCheck(j->env)) {
        (*j->env)->ExceptionClear(j->env);
        moq5_probe_say("caseCount threw");
        return;
    }
    for (index = 0; index < count; index++) {
        moq5_probe_case_t c;
        pthread_t th;
        moq5_probe_thread_t *t;
        int i;

        if (!moq5_probe_case_load(j, index, &c, fn, ctx)) return;
        if (pthread_create(&th, NULL, moq5_probe_case_main, &c) != 0) {
            moq5_probe_say("cannot start the foreign thread");
            moq5_probe_case_free(&c);
            return;
        }
        (void)pthread_join(th, NULL);
        if (c.harness_failed) {
            moq5_probe_say("the foreign thread could not be pre-attached");
            moq5_probe_case_free(&c);
            return;
        }
        /* The thread has ENDED: whatever it attempted from its exit destructor
         * has been counted. Read the record, then release it -- pthread ids
         * are reused, and the next case's thread must start from zero. */
        pthread_mutex_lock(&g_mu);
        t = moq5_thread_by_tid(th, 0);
        if (t != NULL) {
            c.final_counts = *t;
            /* A thread that ended still swapped is unswapped here so a reused
             * env cannot inherit the forwarding table without a record. */
            moq5_env_unswap(t);
            memset(t, 0, sizeof *t);
        }
        pthread_mutex_unlock(&g_mu);

        for (i = 0; i < c.repeats; i++) {
            (*j->env)->CallStaticVoidMethod(j->env, j->klass, j->record, index, (jint)i,
                                            (jint)c.snap[i].verdict,
                                            (jboolean)(c.snap[i].attached ? JNI_TRUE : JNI_FALSE),
                                            (jboolean)(c.snap[i].pending ? JNI_TRUE : JNI_FALSE),
                                            (jint)c.snap[i].getenv, (jint)c.snap[i].attach_daemon,
                                            (jint)c.snap[i].attach, (jint)c.snap[i].detach,
                                            (jint)c.snap[i].high, (jint)c.snap[i].live,
                                            (jint)c.snap[i].created, (jlong)c.snap[i].consulted_prefix);
            if ((*j->env)->ExceptionCheck(j->env)) {
                (*j->env)->ExceptionClear(j->env);
                moq5_probe_say("record threw");
                moq5_probe_case_free(&c);
                return;
            }
        }
        (*j->env)->CallStaticVoidMethod(j->env, j->klass, j->thread_done, index,
                                        (jint)c.final_counts.getenv, (jint)c.final_counts.attach_daemon,
                                        (jint)c.final_counts.attach, (jint)c.final_counts.detach,
                                        (jint)c.final_counts.high, (jint)c.final_counts.live);
        if ((*j->env)->ExceptionCheck(j->env)) {
            (*j->env)->ExceptionClear(j->env);
            moq5_probe_say("threadDone threw");
            moq5_probe_case_free(&c);
            return;
        }
        (*j->env)->CallStaticVoidMethod(j->env, j->klass, j->case_done, index);
        if ((*j->env)->ExceptionCheck(j->env)) {
            (*j->env)->ExceptionClear(j->env);
            moq5_probe_say("caseDone threw");
        }
        moq5_probe_case_free(&c);
    }
}

/* The product name. Whatever the JNI hands the route is recorded first; the
 * REAL route then answers exactly as it would without this file. */
moq_result_t moq_endpoint_connect_with_trust(const moq_endpoint_cfg_t *cfg,
                                             moq_endpoint_trust_fn fn,
                                             void *ctx,
                                             moq_endpoint_t **out)
{
    const char *armed = getenv(MOQ5_TRUST_PROBE_ENV);
    moq5_probe_java_t j;

    pthread_mutex_lock(&g_cnt_mu);
    g_construct_attempts++;
    pthread_mutex_unlock(&g_cnt_mu);
    /* The contract violation a constructor may commit: success with no
     * endpoint. Injected here so the product's own handling of that branch is
     * what a row observes. */
    if (moq5_ctor_fault_take(MOQ5_CTOR_FAULT_NULL_ONCE)) {
        if (out != NULL) *out = NULL;
        return MOQ_OK;
    }
    if (armed == NULL) return moq5_test_real_connect_with_trust(cfg, fn, ctx, out);
    if (strcmp(armed, MOQ5_TRUST_PROBE_TOKEN) != 0) {
        (void)fprintf(stderr, "MOQ5-TRUST-PROBE: unrecognised " MOQ5_TRUST_PROBE_ENV
                      "=\"%s\"; the only accepted value is \"" MOQ5_TRUST_PROBE_TOKEN "\"\n", armed);
        (void)fflush(stderr);
        return MOQ_ERR_INTERNAL;
    }
    if (!moq5_probe_bind(&j)) return MOQ_ERR_INTERNAL;

    {
        jstring url = NULL;
        char *url_buf = NULL;
        size_t url_len = (cfg != NULL) ? cfg->url.len : 0u;

        url_buf = (char *)malloc(url_len + 1u);
        if (url_buf == NULL) {
            moq5_probe_say("out of memory copying the URL");
            moq5_probe_unbind(&j);
            return MOQ_ERR_INTERNAL;
        }
        if (url_len > 0) memcpy(url_buf, cfg->url.data, url_len);
        url_buf[url_len] = '\0';
        url = (*j.env)->NewStringUTF(j.env, url_buf);
        free(url_buf);
        if (url == NULL) {
            (*j.env)->ExceptionClear(j.env);
            moq5_probe_say("cannot build the URL string");
            moq5_probe_unbind(&j);
            return MOQ_ERR_INTERNAL;
        }
        /* The version offer and the handshake timeout, as the route received
         * them. The timeout lives past the v0 floor, so it is reported only
         * when struct_size covers the tail (else -1, tail_covered false):
         * a config initialised with the pointer-only init has NO tail. */
        jint policy = -1;
        jlong handshake_us = -1;
        jboolean tail_covered = JNI_FALSE;
        jintArray versions = NULL;
        {
            size_t n = 0;
            if (cfg != NULL) {
                policy = (jint)cfg->versions.policy;
                n = (cfg->versions.versions != NULL) ? cfg->versions.version_count : 0u;
                if (cfg->struct_size >= sizeof(moq_endpoint_cfg_t)) {
                    tail_covered = JNI_TRUE;
                    handshake_us = (jlong)cfg->handshake_timeout_us;
                }
            }
            versions = (*j.env)->NewIntArray(j.env, (jsize)n);
            if (versions == NULL) {
                (*j.env)->ExceptionClear(j.env);
                moq5_probe_say("cannot build the versions array");
                moq5_probe_unbind(&j);
                return MOQ_ERR_INTERNAL;
            }
            if (n > 0) {
                jint *tmp = (jint *)malloc(n * sizeof(jint));
                size_t i;
                if (tmp == NULL) {
                    moq5_probe_say("out of memory copying the version offer");
                    moq5_probe_unbind(&j);
                    return MOQ_ERR_INTERNAL;
                }
                for (i = 0; i < n; i++) tmp[i] = (jint)cfg->versions.versions[i];
                (*j.env)->SetIntArrayRegion(j.env, versions, 0, (jsize)n, tmp);
                free(tmp);
            }
        }
        /* The transport selection as received: the protocol ordinal and the
         * WebTransport path override (NULL when the config carries none). */
        jint protocol = (cfg != NULL) ? (jint)cfg->protocol : -1;
        jstring wt_path = NULL;
        if (cfg != NULL && cfg->wt_path.len > 0 && cfg->wt_path.data != NULL) {
            char *pbuf = (char *)malloc(cfg->wt_path.len + 1u);
            if (pbuf == NULL) {
                moq5_probe_say("out of memory copying the WebTransport path");
                moq5_probe_unbind(&j);
                return MOQ_ERR_INTERNAL;
            }
            memcpy(pbuf, cfg->wt_path.data, cfg->wt_path.len);
            pbuf[cfg->wt_path.len] = '\0';
            wt_path = (*j.env)->NewStringUTF(j.env, pbuf);
            free(pbuf);
            if (wt_path == NULL) {
                (*j.env)->ExceptionClear(j.env);
                moq5_probe_say("cannot build the WebTransport path string");
                moq5_probe_unbind(&j);
                return MOQ_ERR_INTERNAL;
            }
        }
        (*j.env)->CallStaticVoidMethod(j.env, j.klass, j.route,
                                       (jboolean)(fn != NULL ? JNI_TRUE : JNI_FALSE),
                                       (jboolean)(ctx != NULL ? JNI_TRUE : JNI_FALSE),
                                       (jboolean)((cfg != NULL && cfg->insecure_skip_verify)
                                                  ? JNI_TRUE : JNI_FALSE),
                                       (jint)((cfg != NULL) ? cfg->ca_file.len : 0u),
                                       url, policy, versions, handshake_us, tail_covered,
                                       protocol, wt_path);
        if ((*j.env)->ExceptionCheck(j.env)) {
            (*j.env)->ExceptionClear(j.env);
            moq5_probe_say("route threw");
            moq5_probe_unbind(&j);
            return MOQ_ERR_INTERNAL;
        }
    }

    if (fn != NULL) moq5_probe_invoke_cases(&j, fn, ctx);

    moq5_probe_unbind(&j);
    return moq5_test_real_connect_with_trust(cfg, fn, ctx, out);
}
