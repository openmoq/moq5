/* The Android JNI substrate for LibMoQ.
 *
 * The library ships as one self-contained shared object per ABI, so this
 * translation unit is what a JNI loader enters and what makes every LibMoQ
 * archive part of the linked image. JNI_OnLoad is the only symbol the object
 * exports; the version script enforces that.
 *
 * The anchor table names one public entry point per LibMoQ archive and one
 * picoquic qlog entry point. Referencing their addresses is what selects a
 * member from each archive, so the closure is fixed by this file rather than
 * by link order. Nothing here is called: the table's purpose is reachability,
 * not behaviour, and the Kotlin binding's own entry points replace it as they
 * land.
 */
/* Registration atomicity.
 *
 * The test fixture registers row by row deliberately: doing so is how it
 * produces a diagnostic that names the FIRST row a JVM refuses. That is
 * adversarial test machinery and it is deliberately NOT the product design.
 *
 * Here the table is registered AS ONE UNIT. Either every row named by
 * `MOQ5_ABI_METHODS` is bound, or none is: a preflight resolves every row
 * before anything is bound, and a registration that fails part-way is undone
 * before returning `JNI_ERR`. A partially registered table is the one outcome
 * that must never be reachable -- it leaves a class whose methods are
 * individually callable and collectively wrong, which no caller can detect.
 */

#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <jni.h>

/* The library-owned trust delegate verifies the reference identity NATIVELY,
 * with the same OpenSSL calls and the same IP-literal detector picotls' own
 * verifier uses, before any platform bridge is consulted. */
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <picotls.h>

/* The semantic LibMoQ version is DIAGNOSTIC INPUT ONLY: it never decides
 * compatibility, which is the private epoch's and the feature mask's job.
 * `MOQ_VERSION_NUMBER` is a compile-time macro, so reporting the real version
 * costs an include root and no link -- the host build of this same file adds
 * that root rather than substituting a test-only version. */
#include <moq/version.h>

#include "moq5_abi.h"
#include "moq5_handle_jni.h"
#include "moq5_receiver_child.h"
#include "moq5_registry.h"

/* The endpoint surface is needed by BOTH builds: the host image links this
 * worktree's real service endpoint so the constructor boundary below executes
 * the shipped C, and the Android image links the same functions from the
 * packaged archives. */
#include <moq/endpoint.h>

/* The INTERNAL delegated-trust route. Its header is deliberately not installed:
 * it reaches this translation unit through the service source root, and an
 * application receives neither the header nor an exported symbol. */
#include "endpoint_trust.h"

/* The media receiver: attached under an endpoint root as an OWNED child of
 * the registry, and needed by both builds -- the host image compiles the
 * service receiver so the rows below execute the shipped C. */
#include <moq/media_receiver.h>
#include <moq/media_sender.h>
#include <moq/rcbuf.h>

/* The codec signaling utility: two pure value operations, needed by both
 * builds. They take no endpoint, session or worker. */
#include <moq/codec_signaling.h>

/* The anchor table pulls in every LibMoQ archive and therefore needs the whole
 * dependency closure. A host build of THIS SAME FILE -- used to execute the
 * registration/handshake boundary in an ordinary JVM, where no Android
 * cross-build exists -- defines MOQ5_JNI_HOST_TEST and omits only that table.
 *
 * The registration, preflight and native method code below is therefore the
 * SAME SOURCE in both builds; nothing about it is test-specific. The two
 * OBJECTS are not byte-identical and no such claim is made -- they are built by
 * different compilers for different targets, and the host object omits the
 * anchor table -- but the behaviour under test is compiled from the shipped
 * lines rather than from a stand-in. */
#ifndef MOQ5_JNI_HOST_TEST
#include <moq/msf.h>
#include <moq/media_object.h>
#include <moq/loc.h>
#include <moq/cmaf.h>
#include <moq/picoquic.h>
#include <moq/picoquic_threaded.h>
#include <moq/pico_wt.h>
#include <moq/pico_wt_managed.h>

#include <autoqlog.h>

static void *const moq5_anchors[] = {
    (void *)&moq_version_number,
    (void *)&moq_endpoint_cfg_init,
    (void *)&moq_msf_catalog_apply_delta,
    (void *)&moq_media_object_input_from_sub_object,
    (void *)&moq_loc_encode,
    (void *)&moq_cmaf_fragment_info_init,
    (void *)&moq_pq_callback,
    (void *)&moq_pq_threaded_cfg_init,
    (void *)&moq_pico_wt_conn_cfg_init,
    (void *)&moq_pico_wt_managed_cfg_init,
    (void *)&picoquic_set_qlog,
};
#endif /* MOQ5_JNI_HOST_TEST */

/* -- the endpoint constructor boundary -------------------------------------- *
 *
 * ONE create/register/quarantine body, compiled identically into both images.
 * Only the (construct, destroy) PAIR is selectable, and only in a host test
 * build: the ordering, the transactional publication rule and the refusal
 * handling are the same lines on Android.
 *
 * Why a pair rather than a constructor alone: the failure path has to undo a
 * construction it did not make itself, and a scripted root is torn down by the
 * same lifecycle it was built with. Selecting only the constructor would leave
 * the body guessing which destroy is correct. */
typedef moq_result_t (*moq5_construct_fn)(const moq_endpoint_cfg_t *cfg,
                                          moq_endpoint_t **out);
typedef moq_result_t (*moq5_construct_trusted_fn)(const moq_endpoint_cfg_t *cfg,
                                                  moq_endpoint_trust_fn fn, void *ctx,
                                                  moq_endpoint_t **out);
typedef void (*moq5_destroy_fn)(moq_endpoint_t *ep);

typedef moq_result_t (*moq5_stop_fn)(moq_endpoint_t *ep);

/* The two READ calls.
 *
 * The ANDROID image calls the public functions DIRECTLY: there is no indirect
 * dispatch and no target variable, because writable dispatch that exists only
 * to select test inputs is not product state. A host image swaps the call for a
 * fixture that supplies a result or a record the current product cannot mint;
 * everything around the call -- token acquisition and release, Java-array
 * validation, the copy and the widening -- is the same code either way, and is
 * what the rows assert. */
#if defined(MOQ5_JNI_HOST_TEST)
typedef moq_result_t (*moq5_wait_fn)(moq_endpoint_t *ep, uint64_t timeout_us);
typedef moq_result_t (*moq5_snapshot_fn)(const moq_endpoint_t *ep,
                                         moq_endpoint_snapshot_t *out, size_t size);

static moq5_wait_fn     moq5_wait_target     = moq_endpoint_wait;
static moq5_snapshot_fn moq5_snapshot_target = moq_endpoint_get_snapshot;

#define MOQ5_CALL_WAIT(ep, t)          moq5_wait_target((ep), (t))
#define MOQ5_CALL_SNAPSHOT(ep, o, sz)  moq5_snapshot_target((ep), (o), (sz))
#else
#define MOQ5_CALL_WAIT(ep, t)          moq_endpoint_wait((ep), (t))
#define MOQ5_CALL_SNAPSHOT(ep, o, sz)  moq_endpoint_get_snapshot((ep), (o), (sz))
#endif

/* A LIFECYCLE tuple, not a constructor: the failure paths have to stop and
 * destroy an endpoint they did not build, and a host test has to be able to
 * observe that they did so exactly once and in the right order. The production
 * entries are the exact public functions. */
typedef struct moq5_ctor_target {
    moq5_construct_fn         construct;          /* the ordinary row */
    moq5_construct_trusted_fn construct_trusted;  /* the trusted row: the same route, with a delegate */
    moq5_stop_fn              stop;
    moq5_destroy_fn           destroy;
} moq5_ctor_target_t;

/* The default IS the real function: no wrapper, no indirection that could
 * drift from it. A gate can read this initializer, and the real row proves it
 * behaviourally through a discriminator the scripted target cannot produce. */
static const moq5_ctor_target_t MOQ5_REAL_TARGET = {
    moq_endpoint_connect,
    moq_endpoint_connect_with_trust,
    moq_endpoint_stop,
    moq_endpoint_destroy,
};

static const moq5_ctor_target_t *moq5_ctor_target = &MOQ5_REAL_TARGET;

#if defined(MOQ5_JNI_HOST_TEST)
/* -- the host-only scripted target ------------------------------------------
 *
 * Compiled ONLY into a host test image. It builds a real moq_endpoint_t through
 * the service's own bare-endpoint and probe seam -- itself absent from every
 * shipped artifact -- so publication, ownership and retirement exercise the
 * real registry and the real endpoint lifecycle with no socket, thread or peer.
 *
 * It deliberately IGNORES the configuration. That is what makes the real row's
 * MOQ_ERR_UNSUPPORTED / MOQ_ERR_INVAL discriminator evidence: this target
 * cannot produce either value. */

/* The service's bare-endpoint and probe seam has NO public header: it is
 * compiled only under the service's own testing macros and is absent from every
 * shipped artifact. The declarations it needs are therefore stated here, in the
 * host-only region, rather than by widening a product header for a test. */
size_t moq_endpoint_test_probe_size(void);
void moq_endpoint_test_probe_set_negotiated(void *probe, moq_version_t v);
void moq_endpoint_test_probe_set_terminal(void *probe, bool fatal, uint64_t code);
void moq_endpoint_test_probe_set_service(void *probe, void (*fn)(void *), void *ctx);

/* The scripted facade's "service" step. It performs nothing: its only purpose
 * is to make a recorded wake answerable, so a wait can report activity. */
static void moq5_scripted_service(void *ctx) { (void)ctx; }
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
moq_result_t moq_endpoint_test_install_probe(moq_endpoint_t *ep, void *storage,
                                             size_t size, int report);

/* The probe storage is caller-owned by the seam's contract, so the scripted
 * lifecycle has to remember it across the two calls. A bounded side table is
 * enough: this exists only in a host test image, whose rows are single-threaded
 * and hold a handful of roots at a time, and a full table is a NAMED refusal
 * rather than a leak. */
static struct moq5_scripted_slot {
    moq_endpoint_t *ep;
    void *probe;
    int stopped;
} *moq5_scripted;
static size_t moq5_scripted_cap;

static int moq5_scripted_record(moq_endpoint_t *ep, void *probe)
{
    size_t i;
    for (i = 0; i < moq5_scripted_cap; i++) {
        if (moq5_scripted[i].ep == NULL) {
            moq5_scripted[i].ep = ep;
            moq5_scripted[i].probe = probe;
            moq5_scripted[i].stopped = 0;
            return 1;
        }
    }
    {
        size_t want = moq5_scripted_cap ? moq5_scripted_cap * 2u : 16u;
        struct moq5_scripted_slot *grown =
            (struct moq5_scripted_slot *)realloc(moq5_scripted,
                                                 want * sizeof(*grown));
        if (grown == NULL) return 0;
        memset(grown + moq5_scripted_cap, 0,
               (want - moq5_scripted_cap) * sizeof(*grown));
        moq5_scripted = grown;
        i = moq5_scripted_cap;
        moq5_scripted_cap = want;
        moq5_scripted[i].ep = ep;
        moq5_scripted[i].probe = probe;
        moq5_scripted[i].stopped = 0;
        return 1;
    }
}

static void *moq5_scripted_take(moq_endpoint_t *ep)
{
    size_t i;
    for (i = 0; i < moq5_scripted_cap; i++) {
        if (moq5_scripted[i].ep == ep) {
            void *p = moq5_scripted[i].probe;
            moq5_scripted[i].ep = NULL;
            moq5_scripted[i].probe = NULL;
            moq5_scripted[i].stopped = 0;
            return p;
        }
    }
    return NULL;
}

/* One consolidated host-only MODE knob for the inputs a row needs but the
 * product cannot mint: a refused stop, an armed interrupt, and the wake/
 * retirement handshake. Read once at load beside the selector; absent means
 * none; anything unrecognised refuses the load by name. The MODE supplies
 * INPUTS -- the transformation and the ownership path under test are the
 * shipped ones. */
#define MOQ5_MODE_ENV        "MOQ5_JNI_HOST_TEST_MODE"
#define MOQ5_MODE_STOP_REFUSE "stop-refuse-v1"
#define MOQ5_MODE_INTERRUPT   "wait-interrupt-v1"
#define MOQ5_MODE_WAKE_HOLD   "wake-hold-v1"
#define MOQ5_MODE_WAIT_CLOSED "wait-closed-v1"
#define MOQ5_MODE_WAIT_ODD    "wait-odd-v1"
#define MOQ5_MODE_SNAP_FUTURE "snapshot-future-v1"
/* The transport drain state the scripted facade REPORTS (it never decides the
 * drain outcome, which is the product's own loop): idle, or unprovable. Absent
 * either, the facade reports a pending transport. */
#define MOQ5_MODE_DRAIN_PEND  "drain-pending-v1"
#define MOQ5_MODE_DRAIN_DONE  "drain-done-v1"
#define MOQ5_MODE_DRAIN_UNSUP "drain-unsupported-v1"

enum { MOQ5_MODE_NONE = 0, MOQ5_MODE_STOP, MOQ5_MODE_INT, MOQ5_MODE_WAKE,
       MOQ5_MODE_CLOSED, MOQ5_MODE_ODD, MOQ5_MODE_SNAP, MOQ5_MODE_DRAIN_PENDING,
       MOQ5_MODE_DRAIN_IDLE, MOQ5_MODE_DRAIN_UNSUPPORTED };


/* A value no documented endpoint or registry result uses, so a row asserting it
 * cannot be satisfied by anything the bridge already produces. */
#define MOQ5_ODD_WAIT_RESULT (-777)

/* A future service record: a version this build cannot negotiate and a detail
 * carrying bit 63. Supplied as an INPUT to prove JNI TRANSPORT of a future
 * record -- never as a claim that an unregistered profile is supported. */
#define MOQ5_FUTURE_VERSION  ((moq_version_t)0x7FFFFFFFu)
#define MOQ5_FUTURE_DETAIL   ((uint64_t)0x8000000000000001ULL)
static int moq5_mode;
/* The facade's reported drain state, in the service tier's own vocabulary
 * (endpoint.c: unsupported -1, pending 0, done 1). */
static int moq5_scripted_drain_report(void)
{
    if (moq5_mode == MOQ5_MODE_DRAIN_IDLE)        return 1;
    if (moq5_mode == MOQ5_MODE_DRAIN_UNSUPPORTED) return -1;
    return 0;
}

/* The drain modes model a LIVE endpoint: the scripted terminal input is not
 * armed for them, so the product's own closed check does not pre-empt the
 * probe. Every other mode keeps the terminal input the snapshot rows assert. */
static int moq5_scripted_drain_mode(void)
{
    return moq5_mode == MOQ5_MODE_DRAIN_PENDING || moq5_mode == MOQ5_MODE_DRAIN_IDLE ||
           moq5_mode == MOQ5_MODE_DRAIN_UNSUPPORTED;
}

/* -- the host-only lifecycle ledger -----------------------------------------
 *
 * It exists because registry conservation cannot see a LEAK: a branch that
 * merely aborts its reservation leaves every registry observation identical
 * while the endpoint and its probe are never released. The ledger records what
 * actually happened to each scripted endpoint and latches a NAMED violation,
 * which the next construction reports through the ordinary result channel -- so
 * a lifecycle defect is diagnosed by name rather than by a crash.
 *
 * Compiled only into a host test image; absent from both Android ABIs. */
static unsigned moq5_lc_constructs;
static unsigned moq5_lc_stops;
static unsigned moq5_lc_destroys;
static int moq5_lc_violation;          /* the first named violation, sticky */

#define MOQ5_LC_OK                 0
#define MOQ5_LC_DESTROY_BEFORE_STOP 1  /* destroy ran while the ep was unstopped */
#define MOQ5_LC_DOUBLE_DESTROY      2  /* an ep was destroyed twice */
#define MOQ5_LC_LEAKED              3  /* an ep was never destroyed */
#define MOQ5_LC_STOP_UNKNOWN        4  /* a stop for an ep the ledger never saw */
#define MOQ5_LC_CLEANUP_RACED       5  /* cleanup ran while a lease was held */
#define MOQ5_LC_RETIRING_ACCEPTED   6  /* a RETIRING root accepted a new lease */
#define MOQ5_LC_MILESTONE_REPEATED  7  /* the one-shot rendezvous fired twice */

static const char *moq5_lc_name(int code)
{
    switch (code) {
        case MOQ5_LC_DESTROY_BEFORE_STOP: return "destroy before stop";
        case MOQ5_LC_DOUBLE_DESTROY:      return "double destroy";
        case MOQ5_LC_LEAKED:              return "endpoint leaked";
        case MOQ5_LC_STOP_UNKNOWN:        return "stop of an unknown endpoint";
        case MOQ5_LC_CLEANUP_RACED:       return "cleanup ran while a lease was held";
        case MOQ5_LC_RETIRING_ACCEPTED:   return "a retiring root accepted a new lease";
        case MOQ5_LC_MILESTONE_REPEATED:  return "the one-shot rendezvous fired twice";
        default:                          return "unknown";
    }
}

/* Named the INSTANT it happens, not only at the next call: an ordering defect
 * can be followed by the library's own debug assertion, and a verdict that
 * could only be read later would be lost to that abort. The latch still holds
 * it for the result channel; this is what makes the ledger -- not the crash --
 * the primary evidence. */
static void moq5_lc_latch(int code)
{
    if (moq5_lc_violation == MOQ5_LC_OK) {
        moq5_lc_violation = code;
        (void)fprintf(stderr, "MOQ5-LIFECYCLE: %s\n", moq5_lc_name(code));
        (void)fflush(stderr);
    }
}

/* live scripted endpoints, and whether each has been stopped */
static size_t moq5_lc_live(void)
{
    size_t i, n = 0;
    for (i = 0; i < moq5_scripted_cap; i++) {
        if (moq5_scripted[i].ep != NULL) n++;
    }
    return n;
}

static moq_result_t moq5_scripted_stop(moq_endpoint_t *ep)
{
    size_t i;
    int seen = 0;

    if (moq5_mode == MOQ5_MODE_STOP) {
        /* A refusal the product cannot mint here: the endpoint's own refusals
         * need live attachments or an on-pump caller, neither of which this
         * bridge can reach. Supplying the INPUT lets the rows prove what the
         * JNI does with it -- carry it exactly, mark nothing ready, retire
         * nothing -- which is the behaviour under test. */
        return MOQ_ERR_WRONG_STATE;
    }
    for (i = 0; i < moq5_scripted_cap; i++) {
        if (moq5_scripted[i].ep == ep) { moq5_scripted[i].stopped = 1; seen = 1; break; }
    }
    if (!seen) moq5_lc_latch(MOQ5_LC_STOP_UNKNOWN);
    moq5_lc_stops++;
    return moq_endpoint_stop(ep);
}

static moq_result_t moq5_scripted_construct(const moq_endpoint_cfg_t *cfg,
                                            moq_endpoint_t **out)
{
    moq_endpoint_t *ep = NULL;
    void *probe = NULL;
    moq_result_t rc;

    (void)cfg;
    if (out == NULL) return MOQ_ERR_INVAL;
    *out = NULL;

    probe = calloc(1, moq_endpoint_test_probe_size());
    if (probe == NULL) return MOQ_ERR_NOMEM;

    ep = moq_endpoint_test_make_bare();
    if (ep == NULL) {
        free(probe);
        return MOQ_ERR_NOMEM;
    }
    rc = moq_endpoint_test_install_probe(ep, probe,
                                         moq_endpoint_test_probe_size(),
                                         moq5_scripted_drain_report());
    if (rc == MOQ_OK) {
        /* INPUTS the scripted facade supplies so the JNI's own behaviour can be
         * asserted: a FUTURE nonzero negotiated version, a terminal whose detail
         * carries bit 63 and bit 0, and a service function so a recorded wake is
         * answered by the next wait. The facade supplies inputs; it never
         * chooses the result a row asserts. */
        moq_endpoint_test_probe_set_negotiated(probe, (moq_version_t)0x7FFFFFFFu);
        if (!moq5_scripted_drain_mode()) {
            moq_endpoint_test_probe_set_terminal(probe, true,
                                                 (uint64_t)0x8000000000000001ULL);
        }
        moq_endpoint_test_probe_set_service(probe, moq5_scripted_service, NULL);
        if (moq5_mode == MOQ5_MODE_INT) {
            /* The interrupt latch is a PUBLIC endpoint API; arming it here is a
             * fixture input, and what the row asserts is that the JNI carries
             * MOQ_ERR_INTERRUPTED exactly rather than folding it into a
             * timeout or a generic failure. */
            moq_endpoint_set_interrupted(ep, true);
        }
    }
    if (rc != MOQ_OK) {
        moq_endpoint_test_free_bare(ep);
        free(probe);
        return rc;
    }
    if (!moq5_scripted_record(ep, probe)) {
        moq_endpoint_test_free_bare(ep);
        free(probe);
        return MOQ_ERR_WOULD_BLOCK;   /* the host table is full: a named refusal */
    }
    moq5_lc_constructs++;
    *out = ep;
    return MOQ_OK;
}

static void moq5_scripted_destroy(moq_endpoint_t *ep)
{
    void *probe;

    if (ep == NULL) return;
    /* The probe storage is caller-owned by contract, so it is read BEFORE the
     * endpoint is destroyed and freed after -- the endpoint's own destroy runs
     * the probe's no-op destroy hook and must not see freed storage. */
    {
        size_t i;
        int found = 0;
        for (i = 0; i < moq5_scripted_cap; i++) {
            if (moq5_scripted[i].ep == ep) {
                found = 1;
                /* ORDER, observed directly rather than inferred from the
                 * library's own debug assertion. */
                if (!moq5_scripted[i].stopped) moq5_lc_latch(MOQ5_LC_DESTROY_BEFORE_STOP);
                break;
            }
        }
        if (!found) moq5_lc_latch(MOQ5_LC_DOUBLE_DESTROY);
    }
    moq5_lc_destroys++;
    probe = moq5_scripted_take(ep);
    moq_endpoint_destroy(ep);
    free(probe);
}

/* The scripted target ignores the delegate exactly as it ignores the
 * configuration: what the trusted-row ownership rows assert is the transaction,
 * and the delegate's own behaviour is proven against the REAL target. */
static moq_result_t moq5_scripted_construct_trusted(const moq_endpoint_cfg_t *cfg,
                                                    moq_endpoint_trust_fn fn, void *ctx,
                                                    moq_endpoint_t **out)
{
    (void)fn;
    (void)ctx;
    return moq5_scripted_construct(cfg, out);
}

static const moq5_ctor_target_t MOQ5_SCRIPTED_TARGET = {
    moq5_scripted_construct,
    moq5_scripted_construct_trusted,
    moq5_scripted_stop,
    moq5_scripted_destroy,
};

/* The selector.
 *
 * Read exactly ONCE during JNI_OnLoad, before registration exposes any native
 * method, so no method can ever observe a half-selected target and no pointer
 * is mutated lazily on first use. Absent means the real function. One exact
 * versioned token means the scripted target. Anything else -- including an
 * empty value -- refuses the LOAD with a named diagnostic, because a value the
 * bridge does not recognise is a caller error, not a default. */
#define MOQ5_CTOR_ENV   "MOQ5_JNI_HOST_TEST_CONSTRUCTOR"
#define MOQ5_CTOR_TOKEN "scripted-endpoint-v1"

/* The publish-failure injection, armed once at load beside the selector so no
 * method can observe it changing. Absent means off; the only accepted value is
 * the exact token, and anything else refuses the load exactly as an
 * unrecognised selector does. */
#include <pthread.h>

/* The wake/retirement handshake, armed by MOQ5_MODE_WAKE_HOLD.
 *
 * It exists because the composition under test is CROSS-LAYER: the JNI holds a
 * registry lease across `moq_endpoint_wake()`, and the claim is that a
 * concurrent authority-thread retirement can invalidate the token but cannot
 * clean the endpoint until that lease drops. Ordering it with sleeps would
 * prove nothing, so the two sides rendezvous here and the ledger -- not a
 * timing window -- decides whether cleanup raced the lease. */
/* The registry's already-private testing hooks. The host image compiles the
 * registry with MOQ5_REGISTRY_TESTING; the Android build never defines it, so
 * this declaration resolves to nothing there -- and this whole region is
 * host-only in any case. */
#define MOQ5_REGISTRY_TESTING 1
#include "moq5_registry_testing.h"

static jint JNICALL moq5_impl_nativeEndpointWake(JNIEnv *env, jobject self, jlong token);

static jlong moq5_hold_token;
static pthread_mutex_t moq5_hold_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  moq5_hold_cv = PTHREAD_COND_INITIALIZER;
static int moq5_hold_claimed;        /* the first arrival owns the held lease */
static int moq5_hold_arrivals;       /* both workers must be classified before retiring */
static int moq5_hold_wake_entered;
static int moq5_hold_retiring;       /* the milestone has been reached */
static int moq5_hold_probe_done;
static int moq5_hold_may_release;
static int moq5_hold_armed;          /* ONE-SHOT: cleared by the destroy that fired it */
static unsigned moq5_hold_milestones;

#define MOQ5_FAILPUB_ENV   "MOQ5_JNI_HOST_TEST_FAIL_PUBLISH"
#define MOQ5_FAILPUB_TOKEN "fail-publish-v1"

/* The output-acquisition injection, in the same one-shot shape.
 *
 * `GetLongArrayElements` may return NULL, and it may do so with or without a
 * Java exception pending. Both are real answers the specification permits and
 * neither can be provoked on demand from a healthy VM, so the two branches
 * behind them would otherwise be unproven. Host image only. */
/* Global-reference exhaustion during the load, armed at load because that is
 * the only time moq5_trust_bind runs. The value is how many of the next global
 * references are refused, so either the singleton or the byte-array class can
 * be the one that fails. */
#define MOQ5_FAILGLOBAL_ENV     "MOQ5_JNI_HOST_TEST_FAIL_GLOBAL"

#define MOQ5_FAILGET_ENV        "MOQ5_JNI_HOST_TEST_FAIL_ACQUIRE"
#define MOQ5_FAILGET_BARE       "fail-acquire-bare-v1"
#define MOQ5_FAILGET_PENDING    "fail-acquire-pending-v1"

static int moq5_fail_publish;

/* 0 none; 1 NULL with no exception; 2 NULL with a genuine pending exception. */
static int moq5_fail_acquire;

/* How many of the next global-reference requests answer NULL; a negative value
 * lets exactly one succeed and refuses the one after it. */
static int moq5_fail_global;

/* One-shot result-allocation failure, armed by the host recorder immediately
 * before the row it is meant for -- the same way the constructor faults are, so
 * setup rows cannot consume it.
 *
 * 1 answers NULL with NOTHING pending, which the JNI contract permits; 2
 * answers NULL with the runtime's OWN exception pending. They are different
 * answers and the rows must treat them differently, so both are injectable.
 * `moq5_fail_alloc_at` selects WHICH allocation fails: 0 the result array
 * itself, or the ordinal of a byte field, so a partially built result reaches
 * the same cleanup path. */
static int moq5_fail_alloc;
static int moq5_fail_alloc_at;

void moq5_test_arm_alloc_fault(int pending, int at_index)
{
    moq5_fail_alloc = pending ? 2 : 1;
    moq5_fail_alloc_at = at_index;
}

int moq5_test_alloc_fault_pending(void)
{
    return moq5_fail_alloc;
}

static int moq5_select_constructor(char *detail, size_t cap)
{
    const char *v = getenv(MOQ5_CTOR_ENV);

    if (v == NULL) {
        moq5_ctor_target = &MOQ5_REAL_TARGET;
        return 1;
    }
    if (v[0] == '\0') {
        (void)snprintf(detail, cap,
                       "moq5-bridge: the constructor selector " MOQ5_CTOR_ENV
                       " is present but empty; unset it for the real "
                       "constructor or set it to \"" MOQ5_CTOR_TOKEN "\"");
        return 0;
    }
    if (strcmp(v, MOQ5_CTOR_TOKEN) == 0) {
        moq5_ctor_target = &MOQ5_SCRIPTED_TARGET;
        return 1;
    }
    (void)snprintf(detail, cap,
                   "moq5-bridge: unrecognised constructor selector "
                   MOQ5_CTOR_ENV "=\"%s\"; the only accepted value is \""
                   MOQ5_CTOR_TOKEN "\"", v);
    return 0;
}

static int moq5_select_mode(char *detail, size_t cap)
{
    const char *v = getenv(MOQ5_MODE_ENV);

    if (v == NULL)                                  { moq5_mode = MOQ5_MODE_NONE; return 1; }
    if (strcmp(v, MOQ5_MODE_STOP_REFUSE) == 0)      { moq5_mode = MOQ5_MODE_STOP; return 1; }
    if (strcmp(v, MOQ5_MODE_INTERRUPT) == 0)        { moq5_mode = MOQ5_MODE_INT;  return 1; }
    if (strcmp(v, MOQ5_MODE_WAKE_HOLD) == 0)        { moq5_mode = MOQ5_MODE_WAKE; return 1; }
    if (strcmp(v, MOQ5_MODE_WAIT_CLOSED) == 0)      { moq5_mode = MOQ5_MODE_CLOSED; return 1; }
    if (strcmp(v, MOQ5_MODE_WAIT_ODD) == 0)         { moq5_mode = MOQ5_MODE_ODD; return 1; }
    if (strcmp(v, MOQ5_MODE_SNAP_FUTURE) == 0)      { moq5_mode = MOQ5_MODE_SNAP; return 1; }
    if (strcmp(v, MOQ5_MODE_DRAIN_PEND) == 0)       { moq5_mode = MOQ5_MODE_DRAIN_PENDING; return 1; }
    if (strcmp(v, MOQ5_MODE_DRAIN_DONE) == 0)       { moq5_mode = MOQ5_MODE_DRAIN_IDLE; return 1; }
    if (strcmp(v, MOQ5_MODE_DRAIN_UNSUP) == 0)      { moq5_mode = MOQ5_MODE_DRAIN_UNSUPPORTED; return 1; }
    (void)snprintf(detail, cap,
                   "moq5-bridge: unrecognised host test mode " MOQ5_MODE_ENV
                   "=\"%s\"", v);
    return 0;
}

/* Fixture read targets. Each still runs behind the real token acquisition and,
 * for the snapshot, the real output validation and widening. */
static moq_result_t moq5_wait_closed(moq_endpoint_t *ep, uint64_t t)
{
    (void)ep; (void)t;
    return MOQ_ERR_CLOSED;
}

static moq_result_t moq5_wait_odd(moq_endpoint_t *ep, uint64_t t)
{
    (void)ep; (void)t;
    return (moq_result_t)MOQ5_ODD_WAIT_RESULT;
}

static moq_result_t moq5_snapshot_future(const moq_endpoint_t *ep,
                                         moq_endpoint_snapshot_t *out, size_t size)
{
    if (out == NULL || size < sizeof(*out)) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    out->struct_size = (uint32_t)sizeof(*out);
    /* COHERENT, per the record's own rules: a non-NONE reason implies CLOSED,
     * a nonzero detail belongs to a non-clean reason, and a negotiated version
     * is retained unchanged through close. An ESTABLISHED record carrying a
     * terminal detail -- which an earlier revision of this fixture built -- is
     * not a service record the product could ever produce. */
    out->state = MOQ_ENDPOINT_CLOSED;
    out->negotiated_version = MOQ5_FUTURE_VERSION;
    out->terminal_reason = MOQ_ENDPOINT_TERMINAL_PROTOCOL;
    out->terminal_detail_code = MOQ5_FUTURE_DETAIL;
    (void)ep;
    return MOQ_OK;
}

/* Installed ONCE at load, after the mode is known and before registration makes
 * any method callable -- the same discipline the constructor selector follows. */
static void moq5_retiring_milestone(void *arg);

static int moq5_install_read_targets(char *detail, size_t cap)
{
    (void)detail;
    (void)cap;
    if (moq5_mode == MOQ5_MODE_CLOSED) moq5_wait_target = moq5_wait_closed;
    if (moq5_mode == MOQ5_MODE_ODD)    moq5_wait_target = moq5_wait_odd;
    if (moq5_mode == MOQ5_MODE_SNAP)   moq5_snapshot_target = moq5_snapshot_future;
    if (moq5_mode == MOQ5_MODE_WAKE) {
        /* Armed at LOAD, before any method is callable. Arming it inside the
         * teardown instead left a window in which a worker could reach the wake
         * first, skip the protocol entirely, and leave the retirement waiting
         * for a rendezvous nobody would ever enter. */
        moq5_hold_armed = 1;
        moq5_reg_test_on_retire_wait(NULL, moq5_retiring_milestone);
    }
    return 1;
}

static int moq5_select_failglobal(char *detail, size_t cap)
{
    const char *v = getenv(MOQ5_FAILGLOBAL_ENV);

    if (v == NULL) {
        moq5_fail_global = 0;
        return 1;
    }
    if (strcmp(v, "first") == 0) {
        moq5_fail_global = 1;
        return 1;
    }
    if (strcmp(v, "second") == 0) {
        /* The singleton succeeds and the byte-array class does not. */
        moq5_fail_global = -1;
        return 1;
    }
    (void)snprintf(detail, cap,
                   "moq5-bridge: unrecognised global-reference injection "
                   MOQ5_FAILGLOBAL_ENV "=\"%s\"; the accepted values are "
                   "\"first\" and \"second\"", v);
    return 0;
}

static int moq5_select_failget(char *detail, size_t cap)
{
    const char *v = getenv(MOQ5_FAILGET_ENV);

    if (v == NULL) {
        moq5_fail_acquire = 0;
        return 1;
    }
    if (strcmp(v, MOQ5_FAILGET_BARE) == 0) {
        moq5_fail_acquire = 1;
        return 1;
    }
    if (strcmp(v, MOQ5_FAILGET_PENDING) == 0) {
        moq5_fail_acquire = 2;
        return 1;
    }
    (void)snprintf(detail, cap,
                   "moq5-bridge: unrecognised acquisition-failure injection "
                   MOQ5_FAILGET_ENV "=\"%s\"; the accepted values are \""
                   MOQ5_FAILGET_BARE "\" and \"" MOQ5_FAILGET_PENDING "\"", v);
    return 0;
}

static int moq5_select_failpub(char *detail, size_t cap)
{
    const char *v = getenv(MOQ5_FAILPUB_ENV);

    if (v == NULL) {
        moq5_fail_publish = 0;
        return 1;
    }
    if (strcmp(v, MOQ5_FAILPUB_TOKEN) == 0) {
        moq5_fail_publish = 1;
        return 1;
    }
    (void)snprintf(detail, cap,
                   "moq5-bridge: unrecognised publish-failure injection "
                   MOQ5_FAILPUB_ENV "=\"%s\"; the only accepted value is \""
                   MOQ5_FAILPUB_TOKEN "\"", v);
    return 0;
}
#endif /* MOQ5_JNI_HOST_TEST */

/* The registry cleans a retired root through this, so destruction routes to
 * whichever lifecycle constructed it. */
static void moq5_root_cleanup(void *ptr)
{
    moq5_ctor_target->destroy((moq_endpoint_t *)ptr);
}

/* Registry refusals are reported in their own space so they can never be
 * mistaken for a moq_result_t. Nothing maps them to Kotlin yet; they exist so
 * a failure that is not the endpoint's own is attributable. */
#define MOQ5_ERR_REGISTRY_BASE (-1000)

#if defined(MOQ5_JNI_HOST_TEST)
/* A latched lifecycle violation is reported through the ordinary result channel
 * so a host row can name it. Host-only, like the ledger itself. */
#define MOQ5_ERR_LIFECYCLE_BASE (-2000)

/* CONSERVATION: every live scripted endpoint must be owned by a live registry
 * entry. A branch that abandons an endpoint -- the classic leak -- leaves every
 * registry observation identical and is invisible without this. */
static void moq5_lc_audit(void)
{
    if (moq5_lc_live() > moq5_reg_live_count()) moq5_lc_latch(MOQ5_LC_LEAKED);
}
#endif

/* -- the library-owned trust delegate --------------------------------------- *
 *
 * The delegate the trusted row hands the internal route. It decides in ONE
 * order, failing closed at every step:
 *
 *   1. native validity -- a non-empty chain, a usable server name, a leaf that
 *      parses, and the REFERENCE IDENTITY matched against that leaf (an IP
 *      literal against iPAddress SANs, a DNS name against dNSName SANs, with
 *      picotls' own detector deciding which). All of this happens before any
 *      JNI call: a chain the transport should never have offered costs the JVM
 *      nothing. MOQ5's raw QUIC/picotls path has no SSLSocket hostname-verifier
 *      layer beside the platform trust manager, so this step is not delegable;
 *   2. the platform -- `Moq5PlatformTrust.checkServerTrusted`, resolved once in
 *      JNI_OnLoad on the loading thread, called with the leaf-first DER chain,
 *      the exact server name and the auth type below. Only an explicit accept
 *      is trust; a refusal, and any throwable, is a rejection with the
 *      throwable cleared before control returns to C.
 *
 * THE AUTH TYPE is a protocol-derived constant, not a property of the leaf.
 * Android's server-side `X509TrustManager` parameter is the negotiated
 * key-exchange algorithm; a QUIC client never negotiates anything older than
 * TLS 1.3 (RFC 9001 section 4.2), and TLS 1.3 suites imply no key-exchange
 * method, which is why AOSP/Conscrypt names `GENERIC` for every TLS 1.3 suite.
 * A value read off the leaf key would ask the platform a different question
 * from the one it answers for a real TLS 1.3 connection.
 *
 * THREADS. The callback runs on the transport's own thread, which the JVM has
 * never seen. Attachment is discovered with GetEnv on every call; a foreign
 * thread is attached with AttachCurrentThreadAsDaemon at most once, its exit
 * cleanup is registered immediately through a pthread key whose destructor
 * detaches it, and if that registration fails the attachment is returned at
 * once and the call rejects. A thread the library did not attach is never
 * detached. Every call runs inside one local frame, releasing each
 * certificate's reference as it goes, so the number of live local references
 * never depends on the chain length. */
#define MOQ5_TRUST_CLASS       "org/openmoq/moq5/trust/Moq5PlatformTrust"
#define MOQ5_TRUST_INSTANCE    "INSTANCE"
#define MOQ5_TRUST_INSTANCE_SIG "Lorg/openmoq/moq5/trust/Moq5PlatformTrust;"
#define MOQ5_TRUST_METHOD      "checkServerTrusted"
#define MOQ5_TRUST_DESCRIPTOR  "([[BLjava/lang/String;Ljava/lang/String;)I"
#define MOQ5_TRUST_AUTH_TYPE   "GENERIC"
#define MOQ5_TRUST_NAME_MAX    255
#define MOQ5_TRUST_LOCAL_FRAME 8
#define MOQ5_JSIZE_MAX         0x7FFFFFFF

/* The private trust-policy vocabulary. An ordinal, never a callback. */
#define MOQ5_TRUST_POLICY_PLATFORM 0

typedef struct moq5_trust_state {
    JavaVM       *vm;
    jobject       bridge;      /* global ref: the Moq5PlatformTrust singleton */
    jmethodID     check;
    jclass        byte_array;  /* global ref: [B, for the chain array */
    pthread_key_t key;         /* its destructor detaches a thread this library attached */
    int           key_created;
} moq5_trust_state_t;

static moq5_trust_state_t moq5_trust;

/* Release whatever the trust state holds. A REFUSED load is followed by the VM
 * unloading this image, so the global references and the thread key taken on
 * the way to the refusal are returned here; a successful load keeps them for
 * the life of the process. Safe on a partially initialised state. */
static void moq5_trust_unbind(JNIEnv *env)
{
    if (moq5_trust.bridge != NULL) {
        (*env)->DeleteGlobalRef(env, moq5_trust.bridge);
        moq5_trust.bridge = NULL;
    }
    if (moq5_trust.byte_array != NULL) {
        (*env)->DeleteGlobalRef(env, moq5_trust.byte_array);
        moq5_trust.byte_array = NULL;
    }
    if (moq5_trust.key_created) {
        (void)pthread_key_delete(moq5_trust.key);
        moq5_trust.key_created = 0;
    }
    moq5_trust.check = NULL;
    moq5_trust.vm = NULL;
}

static void moq5_trust_thread_exit(void *value)
{
    (void)value;
    (void)(*moq5_trust.vm)->DetachCurrentThread(moq5_trust.vm);
}

/* The env for this thread: discovered first, attached once if foreign, and
 * only kept if the exit cleanup could be registered. */
static JNIEnv *moq5_trust_env(void)
{
    JNIEnv *env = NULL;

    /* `(void *)&env`: the invocation table spells this parameter `void **` on
     * one platform's jni.h and `JNIEnv **` on another's, and a `void *` converts
     * to either without a cast that could hide a real mismatch. */
    if ((*moq5_trust.vm)->GetEnv(moq5_trust.vm, (void *)&env, MOQ5_JNI_VERSION) == JNI_OK &&
        env != NULL) {
        return env;
    }
    if ((*moq5_trust.vm)->AttachCurrentThreadAsDaemon(moq5_trust.vm, (void *)&env, NULL) != JNI_OK ||
        env == NULL) {
        return NULL;
    }
    if (pthread_setspecific(moq5_trust.key, (void *)1) != 0) {
        /* No cleanup path could be registered: the attachment is returned
         * right away rather than left for nobody to release. */
        (void)(*moq5_trust.vm)->DetachCurrentThread(moq5_trust.vm);
        return NULL;
    }
    return env;
}

static int moq5_trust_identity_ok(const char *name, size_t name_len, moq_bytes_t leaf)
{
    const unsigned char *p = leaf.data;
    X509 *cert;
    int ok;

    if (leaf.data == NULL || leaf.len == 0 || leaf.len > (size_t)LONG_MAX) return 0;
    cert = d2i_X509(NULL, &p, (long)leaf.len);
    if (cert == NULL) return 0;
    if (ptls_server_name_is_ipaddr(name)) {
        ok = X509_check_ip_asc(cert, name, 0) == 1;
    } else {
        ok = X509_check_host(cert, name, name_len, 0, NULL) == 1;
    }
    X509_free(cert);
    return ok;
}

static moq_endpoint_trust_verdict_t moq5_trust_delegate(void *ctx, moq_bytes_t server_name,
                                                        const moq_bytes_t *certificates,
                                                        size_t certificate_count)
{
    char name[MOQ5_TRUST_NAME_MAX + 1];
    JNIEnv *env;
    jobjectArray chain;
    jstring jname;
    jstring jauth;
    jint answer;
    size_t i;
    moq_endpoint_trust_verdict_t verdict = MOQ_ENDPOINT_TRUST_REJECT;

    (void)ctx;
    /* Native validity first; nothing below costs the JVM anything. */
    if (certificates == NULL || certificate_count == 0 ||
        certificate_count > (size_t)MOQ5_JSIZE_MAX) {
        return MOQ_ENDPOINT_TRUST_REJECT;
    }
    if (server_name.data == NULL || server_name.len == 0 ||
        server_name.len > MOQ5_TRUST_NAME_MAX ||
        memchr(server_name.data, '\0', server_name.len) != NULL) {
        return MOQ_ENDPOINT_TRUST_REJECT;
    }
    for (i = 0; i < certificate_count; i++) {
        if (certificates[i].len > (size_t)MOQ5_JSIZE_MAX) return MOQ_ENDPOINT_TRUST_REJECT;
    }
    memcpy(name, server_name.data, server_name.len);
    name[server_name.len] = '\0';
    if (!moq5_trust_identity_ok(name, server_name.len, certificates[0])) {
        return MOQ_ENDPOINT_TRUST_REJECT;
    }

    env = moq5_trust_env();
    if (env == NULL) return MOQ_ENDPOINT_TRUST_REJECT;
    if ((*env)->PushLocalFrame(env, MOQ5_TRUST_LOCAL_FRAME) != 0) {
        (*env)->ExceptionClear(env);
        return MOQ_ENDPOINT_TRUST_REJECT;
    }
    chain = (*env)->NewObjectArray(env, (jsize)certificate_count, moq5_trust.byte_array, NULL);
    if (chain == NULL) goto out;
    for (i = 0; i < certificate_count; i++) {
        jbyteArray der = (*env)->NewByteArray(env, (jsize)certificates[i].len);
        if (der == NULL) goto out;
        if (certificates[i].len > 0) {
            (*env)->SetByteArrayRegion(env, der, 0, (jsize)certificates[i].len,
                                       (const jbyte *)certificates[i].data);
        }
        (*env)->SetObjectArrayElement(env, chain, (jsize)i, der);
        /* Released per element: the chain's length must not grow the frame. */
        (*env)->DeleteLocalRef(env, der);
        if ((*env)->ExceptionCheck(env)) goto out;
    }
    jname = (*env)->NewStringUTF(env, name);
    if (jname == NULL) goto out;
    jauth = (*env)->NewStringUTF(env, MOQ5_TRUST_AUTH_TYPE);
    if (jauth == NULL) goto out;

    answer = (*env)->CallIntMethod(env, moq5_trust.bridge, moq5_trust.check, chain, jname, jauth);
    /* A throwable makes the answer meaningless and is itself a refusal. */
    if ((*env)->ExceptionCheck(env)) goto out;
    if (answer == 0) verdict = MOQ_ENDPOINT_TRUST_ACCEPT;

out:
    if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
    (void)(*env)->PopLocalFrame(env, NULL);
    return verdict;
}

/* A global reference, or NULL when the reference table refuses.
 *
 * Exhaustion cannot be provoked on demand, so it is injected here -- one-shot,
 * host image only, and distinct from an actual VM out-of-memory, which this
 * does not simulate. */
static jobject moq5_new_global(JNIEnv *env, jobject local)
{
#if defined(MOQ5_JNI_HOST_TEST)
    if (moq5_fail_global < 0) {
        moq5_fail_global = 1;        /* this one succeeds; the next is refused */
    } else if (moq5_fail_global > 0) {
        moq5_fail_global--;
        return NULL;
    }
#endif
    return (*env)->NewGlobalRef(env, local);
}

/* Resolved on the LOADING thread: a native thread attached later resolves
 * classes through the system loader and cannot see an application class, so a
 * bridge that cannot be resolved here refuses the load rather than failing on
 * the transport thread. */
static int moq5_trust_bind(JNIEnv *env, char *detail, size_t cap)
{
    jclass klass;
    jclass bytes;
    jfieldID instance_field;
    jobject instance;

    klass = (*env)->FindClass(env, MOQ5_TRUST_CLASS);
    if (klass == NULL) {
        (void)snprintf(detail, cap, "moq5-bridge: trust bridge class %s not found", MOQ5_TRUST_CLASS);
        return 0;
    }
    /* EVERY LOOKUP IS ANSWERED BEFORE THE NEXT ONE IS MADE.
     *
     * A failed lookup leaves its own exception pending, and with one pending
     * only a restricted set of JNI functions may be called -- another lookup is
     * not among them. So each step below refuses immediately rather than
     * carrying a pending exception into the next call. The refusal itself is
     * safe: JNI_OnLoad reports it through moq5_refuse, which clears whatever is
     * pending on purpose so its own UnsatisfiedLinkError is what the caller
     * sees, and moq5_trust_unbind returns whatever was taken on the way. */
    instance_field = (*env)->GetStaticFieldID(env, klass, MOQ5_TRUST_INSTANCE, MOQ5_TRUST_INSTANCE_SIG);
    if (instance_field == NULL) {
        (void)snprintf(detail, cap, "moq5-bridge: %s has no %s field of type %s", MOQ5_TRUST_CLASS,
                       MOQ5_TRUST_INSTANCE, MOQ5_TRUST_INSTANCE_SIG);
        (*env)->DeleteLocalRef(env, klass);
        return 0;
    }
    /* A resolved field that reads NULL throws nothing: the class exists and
     * simply has no singleton, and that is refused on its own terms. */
    instance = (*env)->GetStaticObjectField(env, klass, instance_field);
    if (instance == NULL) {
        (void)snprintf(detail, cap, "moq5-bridge: %s.%s is null", MOQ5_TRUST_CLASS, MOQ5_TRUST_INSTANCE);
        (*env)->DeleteLocalRef(env, klass);
        return 0;
    }
    moq5_trust.check = (*env)->GetMethodID(env, klass, MOQ5_TRUST_METHOD, MOQ5_TRUST_DESCRIPTOR);
    if (moq5_trust.check == NULL) {
        (void)snprintf(detail, cap, "moq5-bridge: %s has no %s%s", MOQ5_TRUST_CLASS,
                       MOQ5_TRUST_METHOD, MOQ5_TRUST_DESCRIPTOR);
        (*env)->DeleteLocalRef(env, instance);
        (*env)->DeleteLocalRef(env, klass);
        return 0;
    }
    moq5_trust.bridge = moq5_new_global(env, instance);
    (*env)->DeleteLocalRef(env, instance);
    (*env)->DeleteLocalRef(env, klass);
    if (moq5_trust.bridge == NULL) {
        /* The reference table refused. Answered here, before the next lookup:
         * it may have left an exception pending, and FindClass is not one of
         * the functions that may be called then. */
        (void)snprintf(detail, cap, "moq5-bridge: cannot hold the trust bridge across the load");
        return 0;
    }
    bytes = (*env)->FindClass(env, "[B");
    if (bytes == NULL) {
        (void)snprintf(detail, cap, "moq5-bridge: the byte-array class could not be resolved");
        return 0;
    }
    moq5_trust.byte_array = (jclass)moq5_new_global(env, bytes);
    (*env)->DeleteLocalRef(env, bytes);
    if (moq5_trust.byte_array == NULL) {
        (void)snprintf(detail, cap, "moq5-bridge: cannot hold the byte-array class across the load");
        return 0;
    }
    return 1;
}

/* -- the bound methods ------------------------------------------------------ *
 * One implementation per authority row, named after its row. They are static:
 * the object's only dynamic export is JNI_OnLoad, and these are reached through
 * the registration table, never by symbol name. Each is a pure read of a
 * compile-time constant -- no state, no allocation, no failure mode. */
static jint JNICALL moq5_impl_nativeAbiEpoch(JNIEnv *env, jobject self)
{
    (void)env;
    (void)self;
    return (jint)MOQ5_ABI_EPOCH;
}

static jlong JNICALL moq5_impl_nativeFeatureBits(JNIEnv *env, jobject self)
{
    (void)env;
    (void)self;
    return (jlong)MOQ5_FEATURES_PROVIDED;
}

static jint JNICALL moq5_impl_nativeLibmoqVersion(JNIEnv *env, jobject self)
{
    (void)env;
    (void)self;
    return (jint)MOQ_VERSION_NUMBER;
}

/* An internal invariant that cannot be honoured without losing ownership.
 *
 * Only reachable if the private registry breaks its own contract. Returning a
 * result code here would drop the only pointer to a live endpoint, which is
 * exactly the shape this file exists to remove -- so it does not return. */
static void moq5_invariant_failed(const char *what)
{
    (void)fprintf(stderr, "moq5-bridge: internal invariant violated: %s\n", what);
    (void)fflush(stderr);
    abort();
}

/* Publishing the token is its own step so a host test can force it to fail.
 *
 * With the array length pre-checked this write cannot throw in practice, so the
 * unwinding branch behind it would otherwise be unreachable and therefore
 * unproven. The injection is compiled ONLY into a host test image and is armed
 * once at load, alongside the constructor selector. */
static void moq5_publish_token(JNIEnv *env, jlongArray out, const jlong *value)
{
#if defined(MOQ5_JNI_HOST_TEST)
    if (moq5_fail_publish) {
        /* ONE-SHOT: the injection must not make every later construction fail
         * too, or the RETIRED root's slot could never be observed being reused. */
        moq5_fail_publish = 0;
        /* Provoke the runtime's own ArrayIndexOutOfBoundsException rather than
         * throwing a synthetic one: the branch under test must see exactly the
         * pending-exception state a real failure produces. */
        (*env)->SetLongArrayRegion(env, out, (*env)->GetArrayLength(env, out), 1, value);
        return;
    }
#endif
    (*env)->SetLongArrayRegion(env, out, 0, 1, value);
}

/* Acquire the caller's output storage BEFORE anything is owed.
 *
 * `GetLongArrayElements` is the fallible, possibly allocating step, and it is
 * taken before the reservation and before the service call, so its failure
 * costs nothing: no registry entry, no C mutation, nothing to unwind. NULL is
 * failure whether or not a Java exception is pending, and a NULL result is
 * never released. */
static jlong *moq5_acquire_out(JNIEnv *env, jlongArray out)
{
#if defined(MOQ5_JNI_HOST_TEST)
    if (moq5_fail_acquire != 0) {
        /* ONE-SHOT, like the publication injection: a permanent one would make
         * every later row in the same image fail too. */
        int mode = moq5_fail_acquire;
        moq5_fail_acquire = 0;
        if (mode == 2) {
            /* A GENUINE pending exception from the runtime, so the branch
             * under test sees exactly the state a real failure produces. */
            jlong scratch = 0;
            (*env)->SetLongArrayRegion(env, out, (*env)->GetArrayLength(env, out), 1, &scratch);
        }
        return NULL;
    }
#endif
    return (*env)->GetLongArrayElements(env, out, NULL);
}

/* CONSTRUCTION IS TRANSACTIONAL, AND ITS ONLY FALLIBLE REGISTRY STEP RUNS
 * BEFORE THE ENDPOINT EXISTS.
 *
 * The order is reserve -> construct -> commit -> publish, and each failure
 * unwinds exactly the steps that succeeded:
 *
 *   reserve fails   -> nothing was constructed and nothing was consumed;
 *   construct fails -> abort the UNUSED reservation; the slot and its
 *                      generation are returned exactly as they were found;
 *   commit          -> the REGISTRY owns the endpoint from here on, and its
 *                      cleanup destroys it exactly once;
 *   publish fails   -> the pending Java exception is left pending, the
 *                      registry-owned endpoint is stopped, and the root is
 *                      RETIRED -- registry cleanup destroys it, once.
 *
 * Committing before publication is what removes the last window: no step after
 * it can leave an endpoint that only this frame knows about, so there is no
 * branch on which an endpoint exists without exactly one durable owner. An
 * earlier revision inserted AFTER constructing and, when the insert refused and
 * the stop then refused, dropped the only pointer to a live endpoint. */
/* ONE transaction for both creation rows. `fn` is NULL for the ordinary row and
 * the library-owned delegate for the trusted row; nothing else differs. */
/* The configuration a create row carries beyond the URL: the version offer
 * (policy and codes, already validated by the row), the transport handshake
 * bound in microseconds (0 = the backend default), the transport protocol
 * (AUTO derives from the URL scheme; an explicit value overrides it), and
 * the optional WebTransport CONNECT request target (NULL = no override).
 * NULL means the ordinary row's defaults: AUTO offer, no bound, AUTO
 * protocol, no path. */
typedef struct moq5_create_cfg {
    moq_version_policy_t     policy;
    const moq_version_t     *versions;   /* borrowed for the duration of the create */
    size_t                   count;
    uint64_t                 handshake_us;
    moq_transport_protocol_t protocol;   /* AUTO derives from the URL scheme */
    const char              *wt_path;    /* the WebTransport CONNECT request target, or NULL */
    size_t                   wt_path_len;
} moq5_create_cfg_t;

/* A WebTransport CONNECT request target, judged on the bytes the row was
 * given: RFC 3986 `path-abempty [ "?" query ]`, non-empty and rooted. Before
 * the first '?' every byte is `pchar` or '/'; after it, `pchar`, '/' or '?'.
 * `pchar` = ALPHA / DIGIT / "-" / "." / "_" / "~" / pct-encoded / sub-delims
 * / ":" / "@", sub-delims = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" /
 * "," / ";" / "=". Every '%' must be a complete %HH. Anything else -- '#',
 * control, space, DEL, non-ASCII (JNI modified UTF-8 encodes an embedded NUL
 * as 0xC0 0x80), and printable bytes outside the set such as '"', '<', '>',
 * '\\', '[', ']', '^', '`', '{', '|', '}' -- is refused. Explicit ASCII
 * predicates, never the locale-sensitive <ctype.h> helpers. The public Kotlin
 * surface applies the same grammar; this is the direct-call boundary. */
static int moq5_ascii_hex(unsigned char c)
{
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

static int moq5_pchar_literal(unsigned char c)
{
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) return 1;
    switch (c) {
        case '-': case '.': case '_': case '~':
        case '!': case '$': case '&': case '\'': case '(': case ')':
        case '*': case '+': case ',': case ';': case '=':
        case ':': case '@':
            return 1;
        default:
            return 0;
    }
}

static int moq5_wt_path_ok(const char *s, size_t len)
{
    size_t i;
    int in_query = 0;
    if (s == NULL || len == 0 || s[0] != '/') return 0;
    for (i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '%') {
            if (i + 2 >= len) return 0;
            if (!moq5_ascii_hex((unsigned char)s[i + 1]) ||
                !moq5_ascii_hex((unsigned char)s[i + 2])) return 0;
            i += 2;
        } else if (c == '?') {
            in_query = 1;   /* a later '?' is query data */
        } else if (c == '/') {
            /* path and query both admit it */
        } else if (!moq5_pchar_literal(c)) {
            return 0;
        }
    }
    (void)in_query;
    return 1;
}

static jint moq5_create_root(JNIEnv *env, jstring url, jlongArray out,
                             moq_endpoint_trust_fn fn, void *ctx,
                             const moq5_create_cfg_t *ccfg)
{
    moq_endpoint_cfg_t cfg;
    moq_endpoint_t *ep = NULL;
    moq5_handle_t handle = MOQ5_HANDLE_INVALID;
    const char *utf = NULL;
    moq_result_t rc;
    moq5_reg_result_t rr;
    jlong published;

#if defined(MOQ5_JNI_HOST_TEST)
    /* The ledger is consulted BEFORE any work: a violation latched by an
     * earlier operation is reported by name instead of being overwritten by
     * whatever this call would have returned. */
    moq5_lc_audit();
    if (moq5_lc_violation != MOQ5_LC_OK) {
        return (jint)(MOQ5_ERR_LIFECYCLE_BASE - moq5_lc_violation);
    }
#endif
    if (out == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, out) < 1) return (jint)MOQ_ERR_INVAL;

    rr = moq5_reg_reserve_root(&handle);
    if (rr != MOQ5_REG_OK) {
        /* Nothing has been constructed. This is the ONLY place a registry
         * refusal can be reported, and it costs the caller nothing. */
        return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    }

    /* An absent URL is passed through as an empty one so the REAL constructor
     * performs the validation. Pre-judging it here would let the bridge answer
     * a question only the endpoint may answer. */
    if (url != NULL) {
        utf = (*env)->GetStringUTFChars(env, url, NULL);
        if (utf == NULL) {                       /* OOM already pending */
            if (moq5_reg_abort_root(handle) != MOQ5_REG_OK) {
                moq5_invariant_failed("abort of this thread's own unused reservation refused");
            }
            return (jint)MOQ_ERR_NOMEM;
        }
    }

    /* SIZED: the handshake bound lives past the v0 floor and connect() reads
     * it only when struct_size covers it. A zeroed tail is the backend default
     * on every field, so the ordinary row is unchanged by this. */
    moq_endpoint_cfg_init_sized(&cfg, sizeof cfg);
    cfg.url.data = (const uint8_t *)(utf ? utf : "");
    cfg.url.len = utf ? strlen(utf) : 0u;
    if (ccfg != NULL) {
        cfg.versions.struct_size = (uint32_t)sizeof(moq_version_offer_t);
        cfg.versions.policy = ccfg->policy;
        cfg.versions.versions = ccfg->versions;
        cfg.versions.version_count = ccfg->count;
        cfg.handshake_timeout_us = ccfg->handshake_us;
        cfg.protocol = ccfg->protocol;
        if (ccfg->wt_path != NULL) {
            cfg.wt_path.data = (const uint8_t *)ccfg->wt_path;
            cfg.wt_path.len = ccfg->wt_path_len;
        }
    }

    rc = (fn != NULL) ? moq5_ctor_target->construct_trusted(&cfg, fn, ctx, &ep)
                      : moq5_ctor_target->construct(&cfg, &ep);

    if (utf != NULL) (*env)->ReleaseStringUTFChars(env, url, utf);

    if (rc != MOQ_OK || ep == NULL) {
        /* A target that reports success without an endpoint is a contract
         * violation, reported rather than published. No endpoint exists on this
         * branch, so the abort cannot lose one -- but a refusal would still mean
         * the private registry broke its own contract, and an unchecked result
         * is exactly what this file no longer contains. */
        if (moq5_reg_abort_root(handle) != MOQ5_REG_OK) {
            moq5_invariant_failed("abort of this thread's own unused reservation refused");
        }
        return (jint)(rc != MOQ_OK ? rc : MOQ_ERR_INVAL);
    }

    /* OWNERSHIP TRANSFERS BEFORE ANYTHING ELSE CAN FAIL.
     *
     * Committing here means no later step can leave an endpoint that only this
     * frame knows about: from this line on the registry owns it, and every
     * failure below is an ORDERED TEARDOWN of an owned root rather than a
     * disposal of a pointer. */
    rr = moq5_reg_commit_root(handle, ep, moq5_root_cleanup);
    if (rr != MOQ5_REG_OK) {
        /* Commit refuses only for a token that is not this thread's own live
         * reservation, which this frame is holding: reaching here means the
         * registry's own contract is broken, so there is no correct value to
         * return. Failing closed is the only thing that cannot lose `ep`. */
        moq5_invariant_failed("commit of this thread's own reservation refused");
    }

    published = (jlong)handle;
    moq5_publish_token(env, out, &published);
    if ((*env)->ExceptionCheck(env)) {
        /* The caller can never learn this token, so the root must be torn down
         * -- in the canonical order, on the endpoint the REGISTRY owns. The
         * pending Java exception is deliberately left pending: clearing it
         * would manufacture a success. */
        moq_result_t src = moq5_ctor_target->stop(ep);
        if (src != MOQ_OK) {
            /* A refused stop leaves the root live, UNREADY and registry-owned;
             * nothing may clean it. The refusal is carried exactly. */
            return (jint)src;
        }
        /* The same stop-success -> ready -> ready-checked-retire rule the
         * public teardown uses; this path does not get a private shortcut. */
        rr = moq5_reg_mark_ready(handle);
        if (rr != MOQ5_REG_OK) {
            moq5_invariant_failed("marking this thread's own live root ready refused");
        }
        rr = moq5_reg_retire_root_ready(handle);  /* its cleanup destroys, once */
        if (rr != MOQ5_REG_OK) {
            moq5_invariant_failed("ready-checked retirement of this thread's own root refused");
        }
        return (jint)MOQ_ERR_INTERNAL;
    }

    return (jint)MOQ_OK;
}

static jint JNICALL moq5_impl_nativeEndpointCreate(JNIEnv *env, jobject self,
                                                   jstring url, jlongArray out)
{
    (void)self;
    return moq5_create_root(env, url, out, NULL, NULL, NULL);
}

/* The trusted row: the same transaction, entered with the library-owned
 * delegate. The policy ordinal is validated BEFORE the transaction can reserve
 * anything -- an unknown ordinal is incoherent input, not a capability answer
 * the route could give -- and the output is checked first for the same reason
 * (the transaction checks it again). */
/* The version offer's local capacity, mirroring the service tier's own cap
 * (MOQ_ENDPOINT_MAX_VERSIONS in endpoint_internal.h): the offer is borrowed
 * from a bounded local for exactly the duration of the create call. */
#define MOQ5_MAX_VERSIONS 8

static jint JNICALL moq5_impl_nativeEndpointCreateTrusted(JNIEnv *env, jobject self,
                                                          jstring url, jint trust_policy,
                                                          jint version_policy,
                                                          jintArray versions,
                                                          jlong handshake_us,
                                                          jint protocol,
                                                          jstring wt_path,
                                                          jlongArray out)
{
    moq_version_t local[MOQ5_MAX_VERSIONS];
    jint codes[MOQ5_MAX_VERSIONS];
    moq5_create_cfg_t ccfg;
    jsize n, i, k;
    const char *path_utf = NULL;
    jint rc;

    (void)self;
    /* Every malformed direct call fails CLOSED here: before a reservation and
     * before the trust route. Positive but unknown version codes are NOT
     * malformed -- they cross to the service tier, whose resolution answers
     * MOQ_ERR_UNSUPPORTED rather than downgrading. */
    if (out == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, out) < 1) return (jint)MOQ_ERR_INVAL;
    if (trust_policy != MOQ5_TRUST_POLICY_PLATFORM) return (jint)MOQ_ERR_INVAL;
    if (handshake_us < 0) return (jint)MOQ_ERR_INVAL;   /* the C parameter is unsigned */
    if (version_policy != (jint)MOQ_VERSION_POLICY_AUTO &&
        version_policy != (jint)MOQ_VERSION_POLICY_LIST &&
        version_policy != (jint)MOQ_VERSION_POLICY_EXACT) {
        return (jint)MOQ_ERR_INVAL;
    }
    if (protocol != (jint)MOQ_TRANSPORT_PROTOCOL_AUTO &&
        protocol != (jint)MOQ_TRANSPORT_PROTOCOL_RAW_QUIC &&
        protocol != (jint)MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT) {
        return (jint)MOQ_ERR_INVAL;
    }
    if (versions == NULL) return (jint)MOQ_ERR_INVAL;
    n = (*env)->GetArrayLength(env, versions);
    if (n > (jsize)MOQ5_MAX_VERSIONS) return (jint)MOQ_ERR_INVAL;
    if (version_policy == (jint)MOQ_VERSION_POLICY_AUTO && n != 0) return (jint)MOQ_ERR_INVAL;
    if (version_policy == (jint)MOQ_VERSION_POLICY_LIST && n == 0) return (jint)MOQ_ERR_INVAL;
    if (version_policy == (jint)MOQ_VERSION_POLICY_EXACT && n != 1) return (jint)MOQ_ERR_INVAL;
    if (n > 0) (*env)->GetIntArrayRegion(env, versions, 0, n, codes);
    if ((*env)->ExceptionCheck(env)) return (jint)MOQ_ERR_INVAL;
    for (i = 0; i < n; i++) {
        if (codes[i] <= 0) return (jint)MOQ_ERR_INVAL;
        for (k = 0; k < i; k++) {
            if (codes[k] == codes[i]) return (jint)MOQ_ERR_INVAL;
        }
        local[i] = (moq_version_t)codes[i];
    }
    ccfg.policy = (moq_version_policy_t)version_policy;
    ccfg.versions = (n > 0) ? local : NULL;
    ccfg.count = (size_t)n;
    ccfg.handshake_us = (uint64_t)handshake_us;
    ccfg.protocol = (moq_transport_protocol_t)protocol;
    ccfg.wt_path = NULL;
    ccfg.wt_path_len = 0;
    if (wt_path != NULL) {
        /* NULL is the only spelling of "no override": a non-null path must be
         * a well-formed RFC 3986 path/query request target, judged BEFORE any
         * reservation. It is carried whatever the protocol says -- the
         * service tier decides what a path means for the transport it
         * resolves. */
        path_utf = (*env)->GetStringUTFChars(env, wt_path, NULL);
        if (path_utf == NULL) return (jint)MOQ_ERR_NOMEM;   /* OOM already pending */
        ccfg.wt_path_len = strlen(path_utf);
        if (!moq5_wt_path_ok(path_utf, ccfg.wt_path_len)) {
            (*env)->ReleaseStringUTFChars(env, wt_path, path_utf);
            return (jint)MOQ_ERR_INVAL;
        }
        ccfg.wt_path = path_utf;
    }
    rc = moq5_create_root(env, url, out, moq5_trust_delegate, &moq5_trust, &ccfg);
    if (path_utf != NULL) (*env)->ReleaseStringUTFChars(env, wt_path, path_utf);
    return rc;
}

/* -- the stateful boundary ---------------------------------------------------
 *
 * Every operation here addresses the endpoint through its TOKEN. No native
 * pointer is stored, returned, or reconstructed: each call re-acquires the root
 * -- which validates the token, its kind, its owner and the authority thread in
 * one step -- pins it for exactly the duration of the C call, and releases that
 * pin once. A stale, forged, wrong-kind, foreign-owner, wrong-thread, reserved
 * or retiring token is refused by the registry BEFORE any endpoint pointer is
 * touched.
 *
 * `wake`, `set_interrupted` and `drain` are the exceptions, and structural
 * ones: they LEASE rather than acquire, because their C contracts are
 * any-thread for the application. The lease still pins, so retirement cannot
 * cross the call. */
static moq5_reg_result_t moq5_root_acquire(moq5_handle_t h, void **out)
{
    return moq5_reg_acquire(h, MOQ5_KIND_ROOT, MOQ5_TYPE_ENDPOINT, MOQ5_HANDLE_INVALID, out);
}

static jint JNICALL moq5_impl_nativeEndpointWake(JNIEnv *env, jobject self,
                                                 jlong token)
{
    void *ptr = NULL;
    moq5_reg_result_t rr;

    (void)env;
    (void)self;
#if defined(MOQ5_JNI_HOST_TEST)
    if (moq5_mode == MOQ5_MODE_WAKE && moq5_hold_armed) {
        /* WHICH worker holds the lease is left to the scheduler; the PROTOCOL
         * is not. The first arrival claims the held lease; any later arrival
         * waits until the registry milestone says the tree is RETIRING, so its
         * lease attempt necessarily happens while the first is still held. */
        int probe;
        pthread_mutex_lock(&moq5_hold_mu);
        probe = moq5_hold_claimed;
        moq5_hold_claimed = 1;
        moq5_hold_arrivals++;
        pthread_cond_broadcast(&moq5_hold_cv);
        if (probe) {
            while (!moq5_hold_retiring) {
                pthread_cond_wait(&moq5_hold_cv, &moq5_hold_mu);
            }
        }
        pthread_mutex_unlock(&moq5_hold_mu);
        if (probe) {
            /* The probe path: attempt the lease against a RETIRING root and
             * report what the registry says, then release the milestone. */
            rr = moq5_reg_lease_root_any_thread((moq5_handle_t)token, &ptr);
            if (rr == MOQ5_REG_OK) {
                moq5_lc_latch(MOQ5_LC_RETIRING_ACCEPTED);
                (void)moq5_reg_release((moq5_handle_t)token);
            }
            pthread_mutex_lock(&moq5_hold_mu);
            moq5_hold_probe_done = 1;
            pthread_cond_broadcast(&moq5_hold_cv);
            pthread_mutex_unlock(&moq5_hold_mu);
            return (jint)(rr == MOQ5_REG_OK ? MOQ_OK
                                            : (MOQ5_ERR_REGISTRY_BASE + (int)rr));
        }
    }
#endif
    /* ANY THREAD: the lease deliberately skips the authority check and still
     * pins, so a concurrent retirement cannot free the endpoint under the
     * wake. */
    rr = moq5_reg_lease_root_any_thread((moq5_handle_t)token, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);

    moq_endpoint_wake((moq_endpoint_t *)ptr);   /* infallible by contract */

#if defined(MOQ5_JNI_HOST_TEST)
    if (moq5_mode == MOQ5_MODE_WAKE && moq5_hold_armed) {
        unsigned destroys_at_entry;
        pthread_mutex_lock(&moq5_hold_mu);
        destroys_at_entry = moq5_lc_destroys;
        moq5_hold_wake_entered = 1;
        pthread_cond_broadcast(&moq5_hold_cv);
        /* The lease is STILL HELD here. Retirement may begin, invalidate the
         * token and start awaiting pins -- but it must not be able to clean. */
        while (!moq5_hold_may_release) {
            pthread_cond_wait(&moq5_hold_cv, &moq5_hold_mu);
        }
        if (moq5_lc_destroys != destroys_at_entry) moq5_lc_latch(MOQ5_LC_CLEANUP_RACED);
        pthread_mutex_unlock(&moq5_hold_mu);
    }
#endif

    rr = moq5_reg_release((moq5_handle_t)token);
    if (rr != MOQ5_REG_OK) {
        moq5_invariant_failed("release of this thread's own lease refused");
    }
    return (jint)MOQ_OK;
}

/* `moq_endpoint_set_interrupted()`: the sticky, non-terminal interrupt latch
 * (endpoint.h §5.3). Any-thread by contract and void, so like `wake` the row
 * LEASES the root -- no authority check, pinned for the call -- and its only
 * answers are MOQ_OK or the registry's exact refusal of the token. Setting
 * wakes the facade inside the C tier; clearing wakes nothing, by design. */
static jint JNICALL moq5_impl_nativeEndpointSetInterrupted(JNIEnv *env, jobject self,
                                                           jlong token,
                                                           jboolean interrupted)
{
    void *ptr = NULL;
    moq5_reg_result_t rr;

    (void)env;
    (void)self;
    rr = moq5_reg_lease_root_any_thread((moq5_handle_t)token, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);

    moq_endpoint_set_interrupted((moq_endpoint_t *)ptr, interrupted != JNI_FALSE);

    rr = moq5_reg_release((moq5_handle_t)token);
    if (rr != MOQ5_REG_OK) {
        moq5_invariant_failed("release of this thread's own lease refused");
    }
    return (jint)MOQ_OK;
}

/* `moq_endpoint_drain()`: the application-thread graceful flush. Any-thread
 * for the application and blocking for up to `timeout_us`, so the row LEASES
 * the root for the whole call -- retirement awaits the pin, and a stop during
 * the drain makes the C loop answer MOQ_ERR_CLOSED within one of its slices.
 * The C result crosses VERBATIM (MOQ_OK, MOQ_DONE, MOQ_ERR_INTERRUPTED,
 * MOQ_ERR_CLOSED, MOQ_ERR_UNSUPPORTED, or MOQ_ERR_WRONG_STATE, which no
 * Kotlin thread can earn); a refused token is the registry's exact refusal.
 * The row is a signed/unsigned boundary like `wait`: a negative timeout is
 * refused with MOQ_ERR_INVAL before any lease, never widened. */
static jint JNICALL moq5_impl_nativeEndpointDrain(JNIEnv *env, jobject self,
                                                  jlong token, jlong timeout_us)
{
    void *ptr = NULL;
    moq5_reg_result_t rr;
    moq_result_t rc;

    (void)env;
    (void)self;
    if (timeout_us < 0) return (jint)MOQ_ERR_INVAL;   /* the C parameter is unsigned */

    rr = moq5_reg_lease_root_any_thread((moq5_handle_t)token, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);

    rc = moq_endpoint_drain((moq_endpoint_t *)ptr, (uint64_t)timeout_us);

    rr = moq5_reg_release((moq5_handle_t)token);
    if (rr != MOQ5_REG_OK) {
        moq5_invariant_failed("release of this thread's own lease refused");
    }
    return (jint)rc;
}

/* -- the media receiver rows --------------------------------------------------
 *
 * A receiver is an OWNED CHILD of its endpoint root in the registry: attached
 * on the root's authority thread, published as a full-width child token, and
 * destroyed either by the close row (one child retired, its owner left live)
 * or by root retirement (child before parent). The C stop refuses while a
 * receiver is attached, so the engine closes receivers before it stops. */
static void moq5_receiver_cleanup(void *ptr)
{
    moq5_receiver_child_t *child = (moq5_receiver_child_t *)ptr;

    moq_media_receiver_destroy(child->r);
    free(child->tracks);
    free(child);
}

/* moq_namespace_t: 0..32 parts (the receiver requires at least one); a full
 * track name is at most MOQ_FULL_TRACK_NAME_MAX bytes. */
#define MOQ5_RX_MAX_PARTS 32

static jint JNICALL moq5_impl_nativeReceiverAttach(JNIEnv *env, jobject self, jlong root,
                                                   jobjectArray parts, jint overflow,
                                                   jint max_objects, jlong max_bytes,
                                                   jint max_track_events,
                                                   jboolean auto_subscribe,
                                                   jbyteArray catalog, jint time_mode,
                                                   jlongArray out)
{
    moq_bytes_t bp[MOQ5_RX_MAX_PARTS];
    jsize lens[MOQ5_RX_MAX_PARTS];
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_t *r = NULL;
    moq5_receiver_child_t *wrap = NULL;
    void *ptr = NULL;
    uint8_t *buf = NULL;
    size_t total = 0;
    size_t off = 0;
    jsize n, i, catalog_len = 0;
    moq5_reg_result_t rr;
    moq_result_t rc;
    moq5_handle_t child = MOQ5_HANDLE_INVALID;
    jlong published;

    (void)self;
    /* Every malformed direct call fails CLOSED here: before the root is
     * pinned, before any receiver exists, before anything is published. */
    if (out == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, out) < 1) return (jint)MOQ_ERR_INVAL;
    if (parts == NULL) return (jint)MOQ_ERR_INVAL;
    n = (*env)->GetArrayLength(env, parts);
    if (n < 1 || n > (jsize)MOQ5_RX_MAX_PARTS) return (jint)MOQ_ERR_INVAL;
    if (overflow != (jint)MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME &&
        overflow != (jint)MOQ_MEDIA_OVERFLOW_DROP_GROUP &&
        overflow != (jint)MOQ_MEDIA_OVERFLOW_FLOW_CONTROL) {
        return (jint)MOQ_ERR_INVAL;
    }
    if (time_mode != (jint)MOQ_MEDIA_TIME_RAW && time_mode != (jint)MOQ_MEDIA_TIME_SHARED_EPOCH) {
        return (jint)MOQ_ERR_INVAL;
    }
    if (max_objects < 0 || max_bytes < 0 || max_track_events < 0) return (jint)MOQ_ERR_INVAL;
    for (i = 0; i < n; i++) {
        jobject part = (*env)->GetObjectArrayElement(env, parts, i);
        if (part == NULL) return (jint)MOQ_ERR_INVAL;
        lens[i] = (*env)->GetArrayLength(env, (jbyteArray)part);
        (*env)->DeleteLocalRef(env, part);
        if (lens[i] < 1) return (jint)MOQ_ERR_INVAL;   /* every present part is non-empty */
        total += (size_t)lens[i];
        if (total > (size_t)MOQ_FULL_TRACK_NAME_MAX) return (jint)MOQ_ERR_INVAL;
    }
    if (catalog != NULL) {
        catalog_len = (*env)->GetArrayLength(env, catalog);
        if (catalog_len < 1) return (jint)MOQ_ERR_INVAL;   /* null is the only spelling of the default */
    }

    rr = moq5_root_acquire((moq5_handle_t)root, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);

    /* The bytes cross VERBATIM into one borrowed buffer for the attach call. */
    buf = (uint8_t *)malloc(total + (size_t)catalog_len);
    if (buf == NULL) {
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)MOQ_ERR_NOMEM;
    }
    for (i = 0; i < n; i++) {
        jobject part = (*env)->GetObjectArrayElement(env, parts, i);
        (*env)->GetByteArrayRegion(env, (jbyteArray)part, 0, lens[i], (jbyte *)(buf + off));
        (*env)->DeleteLocalRef(env, part);
        bp[i].data = buf + off;
        bp[i].len = (size_t)lens[i];
        off += (size_t)lens[i];
    }
    if (catalog != NULL) {
        (*env)->GetByteArrayRegion(env, catalog, 0, catalog_len, (jbyte *)(buf + off));
    }
    if ((*env)->ExceptionCheck(env)) {
        free(buf);
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)MOQ_ERR_INVAL;
    }

    moq_media_receiver_cfg_init(&cfg);
    cfg.namespace_.parts = bp;
    cfg.namespace_.count = (size_t)n;
    if (catalog != NULL) {
        cfg.catalog_track.data = buf + off;
        cfg.catalog_track.len = (size_t)catalog_len;
    }
    cfg.auto_subscribe = (auto_subscribe != JNI_FALSE);
    cfg.time_mode = (moq_media_time_mode_t)time_mode;
    cfg.overflow.policy = (moq_media_overflow_policy_t)overflow;
    cfg.overflow.max_objects = (uint32_t)max_objects;
    cfg.overflow.max_bytes = (uint64_t)max_bytes;
    cfg.max_track_events = (uint32_t)max_track_events;

    rc = moq_media_receiver_attach((moq_endpoint_t *)ptr, &cfg, &r);
    free(buf);
    if (rc != MOQ_OK || r == NULL) {
        /* The endpoint's own answer, carried EXACTLY: nothing was published. */
        rr = moq5_reg_release((moq5_handle_t)root);
        if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
        return (jint)(rc != MOQ_OK ? rc : MOQ_ERR_INVAL);
    }

    wrap = (moq5_receiver_child_t *)calloc(1, sizeof *wrap);
    if (wrap == NULL) {
        moq_media_receiver_destroy(r);
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)MOQ_ERR_NOMEM;
    }
    wrap->r = r;
    rr = moq5_reg_insert_child((moq5_handle_t)root, wrap, MOQ5_KIND_CHILD_OWNED,
                               MOQ5_TYPE_RECEIVER, moq5_receiver_cleanup, &child);
    if (rr != MOQ5_REG_OK) {
        /* Attached but unowned: destroy it NOW, so the endpoint's attachment
         * count is restored and nothing leaks behind a registry refusal. */
        moq_media_receiver_destroy(r);
        free(wrap);
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    }

    published = (jlong)child;
    /* A child token is published directly: the root transaction's publish
     * (and its host-only fault arm) belongs to root construction alone. */
    (*env)->SetLongArrayRegion(env, out, 0, 1, &published);
    if ((*env)->ExceptionCheck(env)) {
        /* The caller can never learn this token: retire the child (its
         * cleanup destroys the receiver) and leave the exception pending. */
        (void)moq5_reg_retire_child((moq5_handle_t)root, child, MOQ5_TYPE_RECEIVER);
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)MOQ_ERR_INVAL;
    }
    rr = moq5_reg_release((moq5_handle_t)root);
    if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
    return (jint)MOQ_OK;
}

/* Acquire the receiver child under its root, and a track as a BORROWED leaf
 * under that receiver child.
 *
 * Every receiver row crosses one of these two boundaries, so the kind, the
 * concrete family and the expected owner are stated ONCE. The alternative --
 * the same triple written out at each row -- is where a single mistyped
 * expectation would reintroduce exactly the confusion the type exists to
 * prevent. Both return the registry's answer verbatim. */
static moq5_reg_result_t moq5_receiver_acquire(jlong root, jlong rx, void **out)
{
    return moq5_reg_acquire((moq5_handle_t)rx, MOQ5_KIND_CHILD_OWNED, MOQ5_TYPE_RECEIVER,
                            (moq5_handle_t)root, out);
}

static moq5_reg_result_t moq5_receiver_track_acquire(jlong rx, jlong track, void **out)
{
    return moq5_reg_acquire((moq5_handle_t)track, MOQ5_KIND_CHILD_BORROWED,
                            MOQ5_TYPE_RECEIVER_TRACK, (moq5_handle_t)rx, out);
}

/* Close a receiver: retire that one child under its expected owner. The
 * registry runs the receiver's destroy on this (authority) thread; the root
 * stays live and may attach another receiver. Every refusal is the registry's
 * exact answer. */
static jint JNICALL moq5_impl_nativeReceiverClose(JNIEnv *env, jobject self, jlong root,
                                                  jlong child)
{
    moq5_reg_result_t rr;

    (void)env;
    (void)self;
    rr = moq5_reg_retire_child((moq5_handle_t)root, (moq5_handle_t)child, MOQ5_TYPE_RECEIVER);
    return (jint)(rr == MOQ5_REG_OK ? MOQ_OK : (MOQ5_ERR_REGISTRY_BASE + (int)rr));
}

/* -- the receiver EVENT rows --------------------------------------------------
 *
 * One event per call, on the root's authority thread, into a fixed scalar
 * block; a description as byte arrays plus its own scalar block. A track
 * handle crosses as a BORROWED child token of the receiver child, minted on
 * the handle's first sight and reused for every later event on that handle
 * (see moq5_receiver_child.h; the token is minted on the bridge's first sight
 * of the handle, event or object). The slot orders below are the ones the
 * Kotlin carrier tables name. */
#define MOQ5_RX_EV_SCALARS 12
#define MOQ5_TD_SCALARS 35
#define MOQ5_TD_FIXED_BYTES 13

/* The borrowed token for `track` under the receiver child `rx`, minting one on
 * the bridge's FIRST SIGHT of the handle -- from a track event or from an
 * object, whichever comes first; the token is then reused for every later
 * sight. A registry refusal is returned as is; the map's own growth failure
 * is the registry's NOMEM category. */
static moq5_reg_result_t moq5_track_token(moq5_receiver_child_t *child, moq5_handle_t rx,
                                          const moq_media_track_t *track, moq5_handle_t *out)
{
    moq5_handle_t h = MOQ5_HANDLE_INVALID;
    moq5_reg_result_t rr;
    size_t i;

    for (i = 0; i < child->count; i++) {
        if (child->tracks[i].track == track) {
            *out = child->tracks[i].token;
            return MOQ5_REG_OK;
        }
    }
    if (child->count == child->cap) {
        size_t ncap = child->cap ? child->cap * 2 : 8;
        moq5_track_slot_t *grown = (moq5_track_slot_t *)realloc(child->tracks, ncap * sizeof *grown);
        if (grown == NULL) return MOQ5_REG_ERR_NOMEM;
        child->tracks = grown;
        child->cap = ncap;
    }
    rr = moq5_reg_insert_child(rx, (void *)(uintptr_t)track, MOQ5_KIND_CHILD_BORROWED,
                               MOQ5_TYPE_RECEIVER_TRACK, NULL, &h);
    if (rr != MOQ5_REG_OK) return rr;
    child->tracks[child->count].track = track;
    child->tracks[child->count].token = h;
    child->count++;
    *out = h;
    return MOQ5_REG_OK;
}

static jint JNICALL moq5_impl_nativeReceiverPollTrack(JNIEnv *env, jobject self, jlong root,
                                                      jlong rx, jlongArray scalars)
{
    moq_media_track_event_t ev;
    moq5_receiver_child_t *child;
    jlong out[MOQ5_RX_EV_SCALARS];
    void *ptr = NULL;
    moq5_reg_result_t rr;
    moq_result_t rc;

    (void)self;
    if (scalars == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, scalars) < (jsize)MOQ5_RX_EV_SCALARS) return (jint)MOQ_ERR_INVAL;

    rr = moq5_receiver_acquire(root, rx, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    child = (moq5_receiver_child_t *)ptr;

    memset(&ev, 0, sizeof ev);
    ev.struct_size = (uint32_t)sizeof ev;
    rc = moq_media_receiver_poll_track(child->r, &ev, sizeof ev);
    memset(out, 0, sizeof out);
    if (rc == MOQ_OK) {
        moq5_handle_t tok = MOQ5_HANDLE_INVALID;
        if (ev.track != NULL) {
            rr = moq5_track_token(child, (moq5_handle_t)rx, ev.track, &tok);
            if (rr != MOQ5_REG_OK) {
                /* The event is consumed and cannot be delivered without its
                 * token: the registry's refusal, and nothing published. */
                (void)moq5_reg_release((moq5_handle_t)rx);
                return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
            }
        }
        out[0] = (jlong)ev.kind;
        out[1] = (jlong)tok;
        out[2] = ev.has_largest ? 1 : 0;
        out[3] = (jlong)ev.largest_group;
        out[4] = (jlong)ev.largest_object;
        out[5] = ev.has_expires ? 1 : 0;
        out[6] = (jlong)ev.expires_ms;
        out[7] = (jlong)ev.parse_drop_class;
        out[8] = (jlong)ev.parse_drops_total;
        out[9] = (jlong)ev.parse_drops_delta;
        (*env)->SetLongArrayRegion(env, scalars, 0, (jsize)MOQ5_RX_EV_SCALARS, out);
    } else if (rc == MOQ_ERR_CLOSED) {
        out[10] = moq_media_receiver_is_fatal(child->r) ? 1 : 0;
        out[11] = (jlong)moq_media_receiver_fatal_code(child->r);
        (*env)->SetLongArrayRegion(env, scalars, 0, (jsize)MOQ5_RX_EV_SCALARS, out);
    }
    rr = moq5_reg_release((moq5_handle_t)rx);
    if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
    return (jint)rc;
}

/* The row's result array.
 *
 * A NULL answer means the JVM could not allocate it and its exception is
 * pending -- which is what the unwinding path behind every caller has to cope
 * with. `scalars` is used only by the host injection, to provoke a GENUINE
 * runtime exception rather than a synthetic one. */
static jobjectArray moq5_new_result_array(JNIEnv *env, jlongArray scalars, jsize count, jclass cls)
{
#if defined(MOQ5_JNI_HOST_TEST)
    if (moq5_fail_alloc != 0 && moq5_fail_alloc_at == 0) {
        int mode = moq5_fail_alloc;
        /* ONE-SHOT: a permanent injection would make every later row in the
         * same image fail too. */
        moq5_fail_alloc = 0;
        if (mode == 2) {
            /* A GENUINE pending exception from the runtime, so the branch
             * under test sees exactly the state a real failure produces. */
            jlong scratch = 0;
            (*env)->SetLongArrayRegion(env, scalars, (*env)->GetArrayLength(env, scalars), 1, &scratch);
        }
        return NULL;
    }
#else
    (void)scalars;
#endif
    return (*env)->NewObjectArray(env, count, cls, NULL);
}

/* One byte field into the result array; 0 when the JVM could not allocate.
 *
 * The three calls after the allocation cannot throw between themselves: the
 * region written is exactly the length just allocated, the element type is the
 * `[B` the array was created with, and the index is the caller's own bound. The
 * final check is what the callers branch on. */
static int moq5_put_bytes(JNIEnv *env, jobjectArray arr, jsize index, moq_bytes_t b)
{
    jbyteArray a;
#if defined(MOQ5_JNI_HOST_TEST)
    /* A field allocation that answers NULL with nothing pending, AFTER the
     * result array already exists: a partially built result then reaches the
     * same cleanup path. Only the bare answer is injected here; the pending
     * one is injected at the result array, where provoking a genuine runtime
     * exception needs no already-built element. */
    if (moq5_fail_alloc == 1 && moq5_fail_alloc_at == (int)index + 1) {
        moq5_fail_alloc = 0;
        return 0;
    }
#endif
    a = (*env)->NewByteArray(env, (jsize)b.len);
    if (a == NULL) return 0;
    if (b.len > 0) (*env)->SetByteArrayRegion(env, a, 0, (jsize)b.len, (const jbyte *)b.data);
    (*env)->SetObjectArrayElement(env, arr, index, a);
    (*env)->DeleteLocalRef(env, a);
    return (*env)->ExceptionCheck(env) ? 0 : 1;
}

static void moq5_td_refuse(JNIEnv *env, jlongArray scalars, jint code)
{
    jlong v = (jlong)code;
    (*env)->SetLongArrayRegion(env, scalars, 0, 1, &v);
}

static jobjectArray JNICALL moq5_impl_nativeTrackDescribe(JNIEnv *env, jobject self, jlong root,
                                                          jlong rx, jlong track, jlongArray scalars)
{
    const moq_media_track_desc_t *d;
    const moq_bytes_t empty = { NULL, 0 };
    jlong out[MOQ5_TD_SCALARS];
    jobjectArray arr = NULL;
    jclass bytes_class;
    void *rptr = NULL;
    void *tptr = NULL;
    moq5_reg_result_t rr;
    jsize count, i;
    size_t k;

    (void)self;
    if (scalars == NULL) return NULL;
    if ((*env)->GetArrayLength(env, scalars) < (jsize)MOQ5_TD_SCALARS) {
        if ((*env)->GetArrayLength(env, scalars) >= 1) moq5_td_refuse(env, scalars, (jint)MOQ_ERR_INVAL);
        return NULL;
    }
    rr = moq5_receiver_acquire(root, rx, &rptr);
    if (rr != MOQ5_REG_OK) {
        moq5_td_refuse(env, scalars, (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr));
        return NULL;
    }
    rr = moq5_receiver_track_acquire(rx, track, &tptr);
    if (rr != MOQ5_REG_OK) {
        (void)moq5_reg_release((moq5_handle_t)rx);
        moq5_td_refuse(env, scalars, (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr));
        return NULL;
    }
    d = moq_media_track_desc_get((const moq_media_track_t *)tptr);

    count = (jsize)(MOQ5_TD_FIXED_BYTES + d->depends_count + d->content_protection_ref_id_count);
    bytes_class = (*env)->FindClass(env, "[B");
    if (bytes_class != NULL) arr = moq5_new_result_array(env, scalars, count, bytes_class);
    if (arr == NULL) goto nomem;
    (*env)->DeleteLocalRef(env, bytes_class);
    {
        const bool init = d->has_init;
        const bool cenc = init && d->init.has_cenc;
        const moq_bytes_t fields[MOQ5_TD_FIXED_BYTES] = {
            d->name, d->role, d->codec, d->lang, d->label, d->packaging, d->mime_type,
            d->event_type, d->channel_config,
            init ? d->init_data : empty,
            init ? d->init.codec_config : empty,
            cenc ? d->init.scheme : empty,
            cenc ? d->init.default_kid : empty,
        };
        for (i = 0; i < (jsize)MOQ5_TD_FIXED_BYTES; i++) {
            if (!moq5_put_bytes(env, arr, i, fields[i])) goto nomem;
        }
        for (k = 0; k < d->depends_count; k++, i++) {
            if (!moq5_put_bytes(env, arr, i, d->depends[k])) goto nomem;
        }
        for (k = 0; k < d->content_protection_ref_id_count; k++, i++) {
            if (!moq5_put_bytes(env, arr, i, d->content_protection_ref_ids[k])) goto nomem;
        }
        memset(out, 0, sizeof out);
        out[0] = (jlong)MOQ_OK;
        out[1] = (jlong)d->info.media_type;
        out[2] = (jlong)d->info.packaging;
        out[3] = (jlong)d->info.timescale;
        out[4] = (jlong)d->info.transport_version;
        out[5] = init ? 1 : 0;
        if (init) {
            out[6] = (jlong)d->init.codec_kind;
            out[7] = (jlong)d->init.timescale;
            out[8] = (jlong)d->init.width;
            out[9] = (jlong)d->init.height;
            out[10] = (jlong)d->init.samplerate;
            out[11] = (jlong)d->init.channel_count;
            out[12] = (jlong)d->init.track_id;
            out[13] = cenc ? 1 : 0;
            out[14] = cenc ? (jlong)d->init.default_is_protected : 0;
            out[15] = cenc ? (jlong)d->init.default_per_sample_iv_size : 0;
        }
        out[16] = d->has_width ? 1 : 0;
        out[17] = (jlong)d->width;
        out[18] = d->has_height ? 1 : 0;
        out[19] = (jlong)d->height;
        out[20] = d->has_samplerate ? 1 : 0;
        out[21] = (jlong)d->samplerate;
        out[22] = d->has_framerate ? 1 : 0;
        out[23] = (jlong)d->framerate_millis;
        out[24] = d->has_bitrate ? 1 : 0;
        out[25] = (jlong)d->bitrate;
        out[26] = d->has_max_grp_sap ? 1 : 0;
        out[27] = (jlong)d->max_grp_sap;
        out[28] = d->has_max_obj_sap ? 1 : 0;
        out[29] = (jlong)d->max_obj_sap;
        out[30] = (jlong)d->depends_count;
        out[31] = (jlong)d->content_protection_ref_id_count;
        out[32] = d->has_track_duration ? 1 : 0;
        out[33] = (jlong)d->track_duration_ms;
        out[34] = d->is_live ? 1 : 0;
        (*env)->SetLongArrayRegion(env, scalars, 0, (jsize)MOQ5_TD_SCALARS, out);
    }
    (void)moq5_reg_release((moq5_handle_t)track);
    rr = moq5_reg_release((moq5_handle_t)rx);
    if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
    return arr;

nomem:
    /* TWO OUTCOMES REACH HERE, and they are answered differently.
     *
     * A JNI function that can both return NULL and throw is permitted to
     * report failure with NULL ALONE, so "the allocation failed" does not
     * imply "an exception is pending". This row also reaches this label
     * natively, without JNI at all, at the sample-count bound.
     *
     * With an exception PENDING, only a restricted set of JNI functions may be
     * called and a scalar writer is not among them: the exception is the
     * answer, the block is left exactly as the caller handed it over, and the
     * caller never reads it. With NOTHING pending, the block is the caller's
     * only way to learn anything, so the refusal is published there.
     *
     * Either way the native cleanup is the same, and none of it is JNI. */
    (void)moq5_reg_release((moq5_handle_t)track);
    (void)moq5_reg_release((moq5_handle_t)rx);
    if (arr != NULL) (*env)->DeleteLocalRef(env, arr);
    if (!(*env)->ExceptionCheck(env)) {
        moq5_td_refuse(env, scalars, (jint)MOQ_ERR_NOMEM);
    }
    return NULL;
}

/* -- the receiver OBJECT row --------------------------------------------------
 *
 * ONE moq_media_receiver_poll_object attempt, only ever on a consumer's
 * demand. A successful poll transfers the object's buffers to this row,
 * which copies them into JVM arrays and calls moq_media_object_cleanup
 * EXACTLY ONCE before returning, on every path -- delivered, a refused token
 * mint, or a JVM allocation failure. The track crosses as its BORROWED child
 * token from the receiver's first-sight map (moq5_receiver_child.h): the C
 * queues a track's TRACK_ADDED before any object for it, but the Kotlin side
 * may not have drained that event yet, so a handle first seen here is minted
 * here and the later track poll reuses the same token. */
#define MOQ5_RX_OBJ_SCALARS 23
#define MOQ5_RX_OBJ_BYTES 3
#define MOQ5_RX_SAMPLE_STRIDE 16

static void moq5_obj_refuse(JNIEnv *env, jlongArray scalars, jlong code)
{
    (*env)->SetLongArrayRegion(env, scalars, 0, 1, &code);
}

static jobjectArray JNICALL moq5_impl_nativeReceiverPollObject(JNIEnv *env, jobject self,
                                                               jlong root, jlong rx,
                                                               jlongArray scalars)
{
    moq_media_object_t obj;
    moq5_receiver_child_t *child;
    jobjectArray arr = NULL;
    jbyteArray samples = NULL;
    jclass bytes_class;
    jlong out[MOQ5_RX_OBJ_SCALARS];
    moq5_handle_t tok = MOQ5_HANDLE_INVALID;
    void *ptr = NULL;
    moq5_reg_result_t rr;
    moq_result_t rc;
    size_t i;

    (void)self;
    if (scalars == NULL) return NULL;
    if ((*env)->GetArrayLength(env, scalars) < (jsize)MOQ5_RX_OBJ_SCALARS) {
        if ((*env)->GetArrayLength(env, scalars) >= 1) moq5_obj_refuse(env, scalars, (jlong)MOQ_ERR_INVAL);
        return NULL;
    }
    rr = moq5_receiver_acquire(root, rx, &ptr);
    if (rr != MOQ5_REG_OK) {
        moq5_obj_refuse(env, scalars, (jlong)(MOQ5_ERR_REGISTRY_BASE + (int)rr));
        return NULL;
    }
    child = (moq5_receiver_child_t *)ptr;

    memset(&obj, 0, sizeof obj);
    obj.struct_size = (uint32_t)sizeof obj;
    memset(out, 0, sizeof out);
    rc = moq_media_receiver_poll_object(child->r, &obj, sizeof obj);
    if (rc != MOQ_OK) {
        if (rc == MOQ_ERR_CLOSED) {
            out[15] = moq_media_receiver_is_fatal(child->r) ? 1 : 0;
            out[16] = (jlong)moq_media_receiver_fatal_code(child->r);
        }
        out[0] = (jlong)rc;
        (*env)->SetLongArrayRegion(env, scalars, 0, (jsize)MOQ5_RX_OBJ_SCALARS, out);
        (void)moq5_reg_release((moq5_handle_t)rx);
        return NULL;
    }

    /* Ownership is ours from here: every exit below cleans exactly once. */
    rr = moq5_track_token(child, (moq5_handle_t)rx, obj.track, &tok);
    if (rr != MOQ5_REG_OK) {
        moq_media_object_cleanup(&obj);
        moq5_obj_refuse(env, scalars, (jlong)(MOQ5_ERR_REGISTRY_BASE + (int)rr));
        (void)moq5_reg_release((moq5_handle_t)rx);
        return NULL;
    }

    bytes_class = (*env)->FindClass(env, "[B");
    if (bytes_class != NULL) arr = moq5_new_result_array(env, scalars, (jsize)MOQ5_RX_OBJ_BYTES, bytes_class);
    if (arr == NULL) goto nomem;
    (*env)->DeleteLocalRef(env, bytes_class);
    if (!moq5_put_bytes(env, arr, 0, obj.payload)) goto nomem;
    if (!moq5_put_bytes(env, arr, 1, obj.fragment)) goto nomem;
    if (obj.sample_count > (size_t)(INT32_MAX / MOQ5_RX_SAMPLE_STRIDE)) goto nomem;
    samples = (*env)->NewByteArray(env, (jsize)(obj.sample_count * MOQ5_RX_SAMPLE_STRIDE));
    if (samples == NULL) goto nomem;
    for (i = 0; i < obj.sample_count; i++) {
        const moq_cmaf_sample_t *sm = &obj.samples[i];
        const uint32_t words[4] = { sm->duration, sm->size, sm->flags, (uint32_t)sm->composition_offset };
        jbyte packed[MOQ5_RX_SAMPLE_STRIDE];
        size_t w;
        for (w = 0; w < 4; w++) {
            packed[w * 4 + 0] = (jbyte)(words[w] & 0xFFu);
            packed[w * 4 + 1] = (jbyte)((words[w] >> 8) & 0xFFu);
            packed[w * 4 + 2] = (jbyte)((words[w] >> 16) & 0xFFu);
            packed[w * 4 + 3] = (jbyte)((words[w] >> 24) & 0xFFu);
        }
        (*env)->SetByteArrayRegion(env, samples, (jsize)(i * MOQ5_RX_SAMPLE_STRIDE), (jsize)MOQ5_RX_SAMPLE_STRIDE, packed);
    }
    (*env)->SetObjectArrayElement(env, arr, 2, samples);
    (*env)->DeleteLocalRef(env, samples);
    samples = NULL;
    if ((*env)->ExceptionCheck(env)) goto nomem;

    out[0] = (jlong)MOQ_OK;
    out[1] = (jlong)tok;
    out[2] = (jlong)obj.packaging;
    out[3] = (jlong)obj.status;
    out[4] = obj.end_of_group ? 1 : 0;
    out[5] = obj.datagram ? 1 : 0;
    out[6] = obj.keyframe ? 1 : 0;
    out[7] = obj.has_capture_time ? 1 : 0;
    out[8] = (jlong)obj.capture_time_us;
    out[9] = (jlong)obj.decode_time_us;
    out[10] = (jlong)obj.composition_offset_us;
    out[11] = (jlong)obj.presentation_time_us;
    out[12] = (jlong)obj.mdat_offset;
    out[13] = (jlong)obj.mdat_len;
    out[14] = (jlong)obj.sample_count;
    /* The identity tail crosses only when the stamped prefix covers all of it;
     * a shorter stamp leaves the six slots zero and HAS_IDENTITY clear. */
    if ((size_t)obj.struct_size >= offsetof(moq_media_object_t, has_subgroup) + sizeof(bool)) {
        out[17] = 1;
        out[18] = (jlong)obj.group_id;
        out[19] = (jlong)obj.subgroup_id;
        out[20] = (jlong)obj.object_id;
        out[21] = (jlong)obj.publisher_priority;
        out[22] = obj.has_subgroup ? 1 : 0;
    }
    (*env)->SetLongArrayRegion(env, scalars, 0, (jsize)MOQ5_RX_OBJ_SCALARS, out);
    moq_media_object_cleanup(&obj);
    rr = moq5_reg_release((moq5_handle_t)rx);
    if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
    return arr;

nomem:
    /* The same two outcomes as the describe row, answered the same way: with an
     * exception pending the block is untouched because a writer may not be
     * called then, and with nothing pending the refusal is published there
     * because it is the caller's only answer. The polled object is released
     * either way -- native work, not JNI -- and this label is also reached
     * natively at the sample-count bound, with nothing pending at all. */
    moq_media_object_cleanup(&obj);
    if (samples != NULL) (*env)->DeleteLocalRef(env, samples);
    if (arr != NULL) (*env)->DeleteLocalRef(env, arr);
    (void)moq5_reg_release((moq5_handle_t)rx);
    if (!(*env)->ExceptionCheck(env)) {
        moq5_obj_refuse(env, scalars, (jlong)MOQ_ERR_NOMEM);
    }
    return NULL;
}

/* -- the receiver CONTROL rows -------------------------------------------------
 *
 * subscribe / unsubscribe / track_state for one track (its BORROWED token
 * under the receiver child, itself OWNED under the root), on the authority
 * thread. Malformed direct arguments are refused before anything is
 * acquired; the endpoint's own moq_result_t crosses exactly; a registry
 * refusal is the registry's. */
static jint moq5_track_rows_acquire(jlong root, jlong rx, jlong track, void **rptr, void **tptr)
{
    moq5_reg_result_t rr;

    rr = moq5_receiver_acquire(root, rx, rptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    rr = moq5_receiver_track_acquire(rx, track, tptr);
    if (rr != MOQ5_REG_OK) {
        (void)moq5_reg_release((moq5_handle_t)rx);
        return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    }
    return (jint)MOQ_OK;
}

static void moq5_track_rows_release(jlong rx, jlong track)
{
    (void)moq5_reg_release((moq5_handle_t)track);
    if (moq5_reg_release((moq5_handle_t)rx) != MOQ5_REG_OK) {
        moq5_invariant_failed("release of this thread's own pin refused");
    }
}

static jint JNICALL moq5_impl_nativeTrackSubscribe(JNIEnv *env, jobject self, jlong root, jlong rx,
                                                   jlong track, jint start, jboolean has_priority,
                                                   jint priority)
{
    moq_media_receiver_track_subscribe_cfg_t cfg;
    void *rptr = NULL;
    void *tptr = NULL;
    jint rc;

    (void)env;
    (void)self;
    if (start != (jint)MOQ_MEDIA_START_CURRENT && start != (jint)MOQ_MEDIA_START_NEXT_GROUP) return (jint)MOQ_ERR_INVAL;
    if (has_priority != JNI_FALSE && (priority < 0 || priority > 255)) return (jint)MOQ_ERR_INVAL;

    rc = moq5_track_rows_acquire(root, rx, track, &rptr, &tptr);
    if (rc != (jint)MOQ_OK) return rc;
    moq_media_receiver_track_subscribe_cfg_init(&cfg);
    cfg.start = (moq_media_start_mode_t)start;
    cfg.has_priority = (has_priority != JNI_FALSE);
    cfg.priority = (uint8_t)priority;
    rc = (jint)moq_media_receiver_subscribe_track(((moq5_receiver_child_t *)rptr)->r,
                                                  (moq_media_track_t *)tptr, &cfg);
    moq5_track_rows_release(rx, track);
    return rc;
}

static jint JNICALL moq5_impl_nativeTrackUnsubscribe(JNIEnv *env, jobject self, jlong root, jlong rx,
                                                     jlong track)
{
    void *rptr = NULL;
    void *tptr = NULL;
    jint rc;

    (void)env;
    (void)self;
    rc = moq5_track_rows_acquire(root, rx, track, &rptr, &tptr);
    if (rc != (jint)MOQ_OK) return rc;
    rc = (jint)moq_media_receiver_unsubscribe_track(((moq5_receiver_child_t *)rptr)->r,
                                                    (moq_media_track_t *)tptr);
    moq5_track_rows_release(rx, track);
    return rc;
}

static jint JNICALL moq5_impl_nativeTrackState(JNIEnv *env, jobject self, jlong root, jlong rx,
                                               jlong track, jlongArray out)
{
    moq_media_track_state_t st = MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    void *rptr = NULL;
    void *tptr = NULL;
    jint rc;

    (void)self;
    if (out == NULL || (*env)->GetArrayLength(env, out) < 1) return (jint)MOQ_ERR_INVAL;
    rc = moq5_track_rows_acquire(root, rx, track, &rptr, &tptr);
    if (rc != (jint)MOQ_OK) return rc;
    rc = (jint)moq_media_receiver_track_state(((moq5_receiver_child_t *)rptr)->r,
                                              (const moq_media_track_t *)tptr, &st);
    moq5_track_rows_release(rx, track);
    if (rc == (jint)MOQ_OK) {
        jlong v = (jlong)st;
        (*env)->SetLongArrayRegion(env, out, 0, 1, &v);
    }
    return rc;
}

/* -- the receiver RECORD rows --------------------------------------------------
 *
 * ONE poll_sap / poll_media_timeline attempt, only on a consumer's demand,
 * into a fixed scalar block. After the poll, still under the same receiver
 * pin and on the same authority thread, the family's deferral counter is
 * read (moq_media_receiver_get_stats) into the block. The two C calls take
 * the receiver's mutex separately, so this is not one receiver critical
 * section; it IS coherent for the stream's witness: the intake is the only
 * writer of the counter, it only ever grows, and it is read strictly after
 * the queue was polled -- so the counter is never older than the answer it
 * accompanies, and a stream comparing it against what it last witnessed on
 * an empty answer can neither miss an advance nor see a phantom one. The
 * track slot carries the handle's borrowed token from the first-sight map. */
#define MOQ5_RX_REC_SCALARS 8

static jint moq5_record_rows_acquire(JNIEnv *env, jlong root, jlong rx, jlongArray scalars,
                                     moq5_receiver_child_t **child)
{
    void *ptr = NULL;
    moq5_reg_result_t rr;

    if (scalars == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, scalars) < (jsize)MOQ5_RX_REC_SCALARS) return (jint)MOQ_ERR_INVAL;
    rr = moq5_receiver_acquire(root, rx, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    *child = (moq5_receiver_child_t *)ptr;
    return (jint)MOQ_OK;
}

/* The tail of both rows: the counter, the terminal slots, publish, release. */
static jint moq5_record_rows_finish(JNIEnv *env, jlong rx, jlongArray scalars, jlong *out,
                                    moq5_receiver_child_t *child, moq_result_t rc, int sap)
{
    moq_media_receiver_stats_t st;
    moq5_reg_result_t rr;

    memset(&st, 0, sizeof st);
    if (moq_media_receiver_get_stats(child->r, &st, sizeof st) == MOQ_OK) {
        out[5] = (jlong)(sap ? st.sap_record_deferrals : st.media_timeline_record_deferrals);
    }
    if (rc == MOQ_ERR_CLOSED) {
        out[6] = moq_media_receiver_is_fatal(child->r) ? 1 : 0;
        out[7] = (jlong)moq_media_receiver_fatal_code(child->r);
    }
    if (rc == MOQ_OK || rc == MOQ_DONE || rc == MOQ_ERR_CLOSED) {
        (*env)->SetLongArrayRegion(env, scalars, 0, (jsize)MOQ5_RX_REC_SCALARS, out);
    }
    rr = moq5_reg_release((moq5_handle_t)rx);
    if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
    return (jint)rc;
}

static jint JNICALL moq5_impl_nativeReceiverPollSap(JNIEnv *env, jobject self, jlong root, jlong rx,
                                                    jlongArray scalars)
{
    moq_media_sap_record_t rec;
    moq5_receiver_child_t *child = NULL;
    jlong out[MOQ5_RX_REC_SCALARS];
    moq5_handle_t tok = MOQ5_HANDLE_INVALID;
    moq_result_t rc;
    jint arc;

    (void)self;
    arc = moq5_record_rows_acquire(env, root, rx, scalars, &child);
    if (arc != (jint)MOQ_OK) return arc;
    memset(&rec, 0, sizeof rec);
    memset(out, 0, sizeof out);
    rc = moq_media_receiver_poll_sap(child->r, &rec, sizeof rec);
    if (rc == MOQ_OK) {
        moq5_reg_result_t rr = moq5_track_token(child, (moq5_handle_t)rx, rec.track, &tok);
        if (rr != MOQ5_REG_OK) {
            /* The record is consumed and cannot be delivered without its token. */
            (void)moq5_reg_release((moq5_handle_t)rx);
            return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
        }
        out[0] = (jlong)tok;
        out[1] = (jlong)rec.group;
        out[2] = (jlong)rec.object;
        out[3] = (jlong)rec.sap_type;
        out[4] = (jlong)rec.ept_ms;
    }
    return moq5_record_rows_finish(env, rx, scalars, out, child, rc, 1);
}

static jint JNICALL moq5_impl_nativeReceiverPollMediaTimeline(JNIEnv *env, jobject self, jlong root,
                                                              jlong rx, jlongArray scalars)
{
    moq_media_timeline_record_t rec;
    moq5_receiver_child_t *child = NULL;
    jlong out[MOQ5_RX_REC_SCALARS];
    moq5_handle_t tok = MOQ5_HANDLE_INVALID;
    moq_result_t rc;
    jint arc;

    (void)self;
    arc = moq5_record_rows_acquire(env, root, rx, scalars, &child);
    if (arc != (jint)MOQ_OK) return arc;
    memset(&rec, 0, sizeof rec);
    memset(out, 0, sizeof out);
    rc = moq_media_receiver_poll_media_timeline(child->r, &rec, sizeof rec);
    if (rc == MOQ_OK) {
        moq5_reg_result_t rr = moq5_track_token(child, (moq5_handle_t)rx, rec.track, &tok);
        if (rr != MOQ5_REG_OK) {
            (void)moq5_reg_release((moq5_handle_t)rx);
            return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
        }
        out[0] = (jlong)tok;
        out[1] = (jlong)rec.media_time_ms;
        out[2] = (jlong)rec.group;
        out[3] = (jlong)rec.object;
        out[4] = (jlong)rec.wallclock_ms;
    }
    return moq5_record_rows_finish(env, rx, scalars, out, child, rc, 0);
}

/* -- the receiver STATS row ---------------------------------------------------
 *
 * ONE moq_media_receiver_get_stats read under the receiver pin, every field
 * of moq_media_receiver_stats_t published exactly once into the block, in
 * the struct's order; the two booleans as 0/1. */
#define MOQ5_RX_STATS_SCALARS 14

static jint JNICALL moq5_impl_nativeReceiverStats(JNIEnv *env, jobject self, jlong root, jlong rx,
                                                  jlongArray scalars)
{
    moq_media_receiver_stats_t st;
    moq5_receiver_child_t *child = NULL;
    jlong out[MOQ5_RX_STATS_SCALARS];
    void *ptr = NULL;
    moq5_reg_result_t rr;
    moq_result_t rc;

    (void)self;
    if (scalars == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, scalars) < (jsize)MOQ5_RX_STATS_SCALARS) return (jint)MOQ_ERR_INVAL;
    rr = moq5_receiver_acquire(root, rx, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    child = (moq5_receiver_child_t *)ptr;

    memset(&st, 0, sizeof st);
    rc = moq_media_receiver_get_stats(child->r, &st, sizeof st);
    if (rc == MOQ_OK) {
        out[0] = (jlong)st.objects_received;
        out[1] = (jlong)st.objects_queued;
        out[2] = (jlong)st.bytes_queued;
        out[3] = (jlong)st.objects_dropped;
        out[4] = (jlong)st.groups_dropped;
        out[5] = (jlong)st.keyframes_dropped;
        out[6] = (jlong)st.parse_drops;
        out[7] = (jlong)st.overflow_events;
        out[8] = (jlong)st.pause_transitions;
        out[9] = st.paused ? 1 : 0;
        out[10] = (jlong)st.catalog_drops;
        out[11] = st.catalog_complete ? 1 : 0;
        out[12] = (jlong)st.sap_record_deferrals;
        out[13] = (jlong)st.media_timeline_record_deferrals;
        (*env)->SetLongArrayRegion(env, scalars, 0, (jsize)MOQ5_RX_STATS_SCALARS, out);
    }
    rr = moq5_reg_release((moq5_handle_t)rx);
    if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
    return (jint)rc;
}


/* -- the SENDER rows -------------------------------------------------------------
 *
 * A sender is an OWNED registry child of its endpoint root; every track it
 * adds is a BORROWED child of the sender child, minted by the add row from the
 * handle add_track returned (stable for the sender's life). Retiring the
 * sender invalidates every track token through the registry's subtree rule
 * and destroys the sender exactly once. The C demand/lifecycle callbacks are
 * installed WAKE-ONLY: each calls moq_endpoint_wake on the endpoint (an
 * any-thread, non-allocating, infallible nudge the endpoint outlives), so a
 * subscriber change or a readiness flip produces a service pass promptly;
 * the state itself is always read by the probe row from the C queries. */
#define MOQ5_TX_ATTACH_SCALARS 9
#define MOQ5_TX_TRACK_BYTES 6
#define MOQ5_TX_TRACK_SCALARS 21
#define MOQ5_TX_OBJ_SCALARS 9
#define MOQ5_TX_PROBE_SCALARS 5
#define MOQ5_TX_STATS_SCALARS 11
#define MOQ5_TX_VOD_MAX 64

typedef struct moq5_sender_child {
    moq_media_sender_t *s;
} moq5_sender_child_t;

static void moq5_sender_cleanup(void *ptr)
{
    moq5_sender_child_t *child = (moq5_sender_child_t *)ptr;

    moq_media_sender_destroy(child->s);
    free(child);
}

static void moq5_sender_wake_subscriber(void *ctx, moq_media_sender_t *sender,
                                        moq_media_track_t *track, size_t active)
{
    (void)sender; (void)track; (void)active;
    moq_endpoint_wake((moq_endpoint_t *)ctx);
}

static void moq5_sender_wake_ready(void *ctx, moq_media_sender_t *sender)
{
    (void)sender;
    moq_endpoint_wake((moq_endpoint_t *)ctx);
}

static void moq5_sender_wake_closed(void *ctx, moq_media_sender_t *sender, bool is_fatal,
                                    uint64_t fatal_code)
{
    (void)sender; (void)is_fatal; (void)fatal_code;
    moq_endpoint_wake((moq_endpoint_t *)ctx);
}

static void moq5_sender_wake_track_closed(void *ctx, moq_media_sender_t *sender,
                                          moq_media_track_t *track)
{
    (void)sender; (void)track;
    moq_endpoint_wake((moq_endpoint_t *)ctx);
}

/* Reads a scalar block of exactly `n` slots into `out`; a null or short block
 * is INVAL before anything is acquired. */
static bool moq5_read_scalars(JNIEnv *env, jlongArray scalars, jsize n, jlong *out)
{
    if (scalars == NULL) return false;
    if ((*env)->GetArrayLength(env, scalars) < n) return false;
    (*env)->GetLongArrayRegion(env, scalars, 0, n, out);
    return !(*env)->ExceptionCheck(env);
}

static bool moq5_is_flag(jlong v)
{
    return v == 0 || v == 1;
}

static bool moq5_is_u32_positive(jlong v)
{
    return v >= 1 && v <= (jlong)UINT32_MAX;
}

static jint JNICALL moq5_impl_nativeSenderAttach(JNIEnv *env, jobject self, jlong root,
                                                 jobjectArray parts, jbyteArray catalog,
                                                 jlongArray scalars, jlongArray out)
{
    moq_bytes_t bp[MOQ5_RX_MAX_PARTS];
    jsize lens[MOQ5_RX_MAX_PARTS];
    jlong sc[MOQ5_TX_ATTACH_SCALARS];
    moq_media_sender_cfg_t cfg;
    moq_media_sender_t *s = NULL;
    moq5_sender_child_t *wrap = NULL;
    void *ptr = NULL;
    uint8_t *buf = NULL;
    size_t total = 0;
    size_t off = 0;
    jsize n, i, catalog_len = 0;
    moq5_reg_result_t rr;
    moq_result_t rc;
    moq5_handle_t child = MOQ5_HANDLE_INVALID;
    jlong published;

    (void)self;
    if (out == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, out) < 1) return (jint)MOQ_ERR_INVAL;
    if (!moq5_read_scalars(env, scalars, (jsize)MOQ5_TX_ATTACH_SCALARS, sc)) return (jint)MOQ_ERR_INVAL;
    if (parts == NULL) return (jint)MOQ_ERR_INVAL;
    n = (*env)->GetArrayLength(env, parts);
    if (n < 1 || n > (jsize)MOQ5_RX_MAX_PARTS) return (jint)MOQ_ERR_INVAL;
    if (sc[0] != (jlong)MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME &&
        sc[0] != (jlong)MOQ_MEDIA_SEND_BP_DROP_GROUP &&
        sc[0] != (jlong)MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK) {
        return (jint)MOQ_ERR_INVAL;   /* UNSET and BLOCK_TIMEOUT have no spelling */
    }
    for (i = 1; i <= 4; i++) {
        if (!moq5_is_u32_positive(sc[i])) return (jint)MOQ_ERR_INVAL;
    }
    if (!moq5_is_flag(sc[5]) || !moq5_is_flag(sc[6]) || !moq5_is_flag(sc[7])) return (jint)MOQ_ERR_INVAL;
    for (i = 0; i < n; i++) {
        jobject part = (*env)->GetObjectArrayElement(env, parts, i);
        if (part == NULL) return (jint)MOQ_ERR_INVAL;
        lens[i] = (*env)->GetArrayLength(env, (jbyteArray)part);
        (*env)->DeleteLocalRef(env, part);
        if (lens[i] < 1) return (jint)MOQ_ERR_INVAL;
        total += (size_t)lens[i];
        if (total > (size_t)MOQ_FULL_TRACK_NAME_MAX) return (jint)MOQ_ERR_INVAL;
    }
    if (catalog != NULL) {
        catalog_len = (*env)->GetArrayLength(env, catalog);
        if (catalog_len < 1) return (jint)MOQ_ERR_INVAL;   /* null is the only spelling of the default */
    }

    rr = moq5_root_acquire((moq5_handle_t)root, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);

    buf = (uint8_t *)malloc(total + (size_t)catalog_len);
    if (buf == NULL) {
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)MOQ_ERR_NOMEM;
    }
    for (i = 0; i < n; i++) {
        jobject part = (*env)->GetObjectArrayElement(env, parts, i);
        (*env)->GetByteArrayRegion(env, (jbyteArray)part, 0, lens[i], (jbyte *)(buf + off));
        (*env)->DeleteLocalRef(env, part);
        bp[i].data = buf + off;
        bp[i].len = (size_t)lens[i];
        off += (size_t)lens[i];
    }
    if (catalog != NULL) {
        (*env)->GetByteArrayRegion(env, catalog, 0, catalog_len, (jbyte *)(buf + off));
    }
    if ((*env)->ExceptionCheck(env)) {
        free(buf);
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)MOQ_ERR_INVAL;
    }

    moq_media_sender_cfg_init_sized(&cfg, sizeof cfg);
    cfg.endpoint = NULL;   /* attach borrows the root's endpoint */
    cfg.namespace_.parts = bp;
    cfg.namespace_.count = (size_t)n;
    if (catalog != NULL) {
        cfg.catalog_track.data = buf + off;
        cfg.catalog_track.len = (size_t)catalog_len;
    }
    cfg.backpressure = (moq_media_send_backpressure_t)sc[0];
    cfg.queue_max_objects = (uint32_t)sc[1];
    cfg.queue_max_bytes = (uint32_t)sc[2];
    cfg.pre_ready_max_objects = (uint32_t)sc[3];
    cfg.pre_ready_max_bytes = (uint32_t)sc[4];
    cfg.validate_cmaf = (sc[5] != 0);
    cfg.publish_tracks = (sc[6] != 0);
    cfg.drop_without_demand = (sc[7] != 0);
    cfg.catalog_refresh_interval_us = moq5_handle_from_jlong(sc[8]);   /* bits: -1 is UINT64_MAX */
    moq_media_sender_callbacks_init_sized(&cfg.callbacks, sizeof cfg.callbacks);
    cfg.callbacks.ctx = ptr;
    cfg.callbacks.on_subscriber_joined = moq5_sender_wake_subscriber;
    cfg.callbacks.on_subscriber_left = moq5_sender_wake_subscriber;
    cfg.callbacks.on_ready = moq5_sender_wake_ready;
    cfg.callbacks.on_closed = moq5_sender_wake_closed;
    cfg.callbacks.on_track_closed = moq5_sender_wake_track_closed;

    rc = moq_media_sender_attach((moq_endpoint_t *)ptr, &cfg, &s);
    free(buf);
    if (rc != MOQ_OK || s == NULL) {
        rr = moq5_reg_release((moq5_handle_t)root);
        if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
        return (jint)(rc != MOQ_OK ? rc : MOQ_ERR_INVAL);
    }

    wrap = (moq5_sender_child_t *)calloc(1, sizeof *wrap);
    if (wrap == NULL) {
        moq_media_sender_destroy(s);
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)MOQ_ERR_NOMEM;
    }
    wrap->s = s;
    rr = moq5_reg_insert_child((moq5_handle_t)root, wrap, MOQ5_KIND_CHILD_OWNED,
                               MOQ5_TYPE_SENDER, moq5_sender_cleanup, &child);
    if (rr != MOQ5_REG_OK) {
        moq_media_sender_destroy(s);
        free(wrap);
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    }

    published = moq5_handle_to_jlong(child);
    (*env)->SetLongArrayRegion(env, out, 0, 1, &published);
    if ((*env)->ExceptionCheck(env)) {
        (void)moq5_reg_retire_child((moq5_handle_t)root, child, MOQ5_TYPE_SENDER);
        (void)moq5_reg_release((moq5_handle_t)root);
        return (jint)MOQ_ERR_INVAL;
    }
    rr = moq5_reg_release((moq5_handle_t)root);
    if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
    return (jint)MOQ_OK;
}

static jint JNICALL moq5_impl_nativeSenderClose(JNIEnv *env, jobject self, jlong root,
                                                jlong child)
{
    moq5_reg_result_t rr;

    (void)env;
    (void)self;
    rr = moq5_reg_retire_child((moq5_handle_t)root, (moq5_handle_t)child, MOQ5_TYPE_SENDER);
    return (jint)(rr == MOQ5_REG_OK ? MOQ_OK : (MOQ5_ERR_REGISTRY_BASE + (int)rr));
}

/* Acquire the sender child under its root; on success `*out` is the wrapper. */
static jint moq5_sender_acquire(jlong root, jlong tx, moq5_sender_child_t **out)
{
    void *ptr = NULL;
    moq5_reg_result_t rr = moq5_reg_acquire((moq5_handle_t)tx, MOQ5_KIND_CHILD_OWNED,
                                            MOQ5_TYPE_SENDER, (moq5_handle_t)root, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    *out = (moq5_sender_child_t *)ptr;
    return (jint)MOQ_OK;
}

/* Acquire a track as a BORROWED child of the sender child `tx`. */
static jint moq5_sender_track_acquire(jlong tx, jlong track, moq_media_track_t **out)
{
    void *ptr = NULL;
    moq5_reg_result_t rr = moq5_reg_acquire((moq5_handle_t)track, MOQ5_KIND_CHILD_BORROWED,
                                            MOQ5_TYPE_SENDER_TRACK, (moq5_handle_t)tx, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    *out = (moq_media_track_t *)ptr;
    return (jint)MOQ_OK;
}

static void moq5_sender_release(jlong tx)
{
    moq5_reg_result_t rr = moq5_reg_release((moq5_handle_t)tx);
    if (rr != MOQ5_REG_OK) moq5_invariant_failed("release of this thread's own pin refused");
}

static jint JNICALL moq5_impl_nativeSenderAddTrack(JNIEnv *env, jobject self, jlong root,
                                                   jlong tx, jobjectArray bytes,
                                                   jlongArray scalars, jlongArray out)
{
    jlong sc[MOQ5_TX_TRACK_SCALARS];
    jsize lens[MOQ5_TX_TRACK_BYTES];
    moq_bytes_t fields[MOQ5_TX_TRACK_BYTES];
    moq_media_track_cfg_t tcfg;
    moq_media_track_t *track = NULL;
    moq5_sender_child_t *child = NULL;
    uint8_t *buf = NULL;
    size_t total = 0;
    size_t off = 0;
    jsize i;
    jint rc;
    moq_result_t crc;
    moq5_reg_result_t rr;
    moq5_handle_t h = MOQ5_HANDLE_INVALID;
    jlong *outv = NULL;
    jlong published;

    (void)self;
    if (out == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, out) < 1) return (jint)MOQ_ERR_INVAL;
    if (!moq5_read_scalars(env, scalars, (jsize)MOQ5_TX_TRACK_SCALARS, sc)) return (jint)MOQ_ERR_INVAL;
    if (bytes == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, bytes) < (jsize)MOQ5_TX_TRACK_BYTES) return (jint)MOQ_ERR_INVAL;
    for (i = 0; i < (jsize)MOQ5_TX_TRACK_BYTES; i++) {
        jobject f = (*env)->GetObjectArrayElement(env, bytes, i);
        lens[i] = (f == NULL) ? -1 : (*env)->GetArrayLength(env, (jbyteArray)f);
        if (f != NULL) (*env)->DeleteLocalRef(env, f);
        if (lens[i] > 0) total += (size_t)lens[i];
    }
    if (lens[0] < 1) return (jint)MOQ_ERR_INVAL;   /* the name is required */
    if (sc[0] != (jlong)MOQ_MEDIA_TYPE_VIDEO && sc[0] != (jlong)MOQ_MEDIA_TYPE_AUDIO) return (jint)MOQ_ERR_INVAL;
    if (sc[1] != (jlong)MOQ_MEDIA_PACKAGING_RAW && sc[1] != (jlong)MOQ_MEDIA_PACKAGING_CMAF) return (jint)MOQ_ERR_INVAL;
    if (!moq5_is_flag(sc[3]) || !moq5_is_flag(sc[9]) || !moq5_is_flag(sc[11]) || !moq5_is_flag(sc[13]) ||
        !moq5_is_flag(sc[15]) || !moq5_is_flag(sc[17]) || !moq5_is_flag(sc[19])) {
        return (jint)MOQ_ERR_INVAL;
    }
    if (sc[2] < 0 || sc[2] > (jlong)UINT32_MAX || sc[4] < 0 || sc[4] > (jlong)UINT32_MAX ||
        sc[5] < 0 || sc[5] > (jlong)UINT32_MAX || sc[7] < 0 || sc[7] > (jlong)UINT32_MAX ||
        sc[10] < 0 || sc[10] > (jlong)UINT32_MAX || sc[12] < 0 || sc[12] > (jlong)UINT32_MAX ||
        sc[14] < 0 || sc[14] > (jlong)UINT32_MAX || sc[18] < 0 || sc[18] > (jlong)UINT32_MAX ||
        sc[20] < (jlong)INT32_MIN || sc[20] > (jlong)INT32_MAX) {
        return (jint)MOQ_ERR_INVAL;
    }

    rc = moq5_sender_acquire(root, tx, &child);
    if (rc != (jint)MOQ_OK) return rc;

    buf = (uint8_t *)malloc(total ? total : 1);
    if (buf == NULL) {
        moq5_sender_release(tx);
        return (jint)MOQ_ERR_NOMEM;
    }
    for (i = 0; i < (jsize)MOQ5_TX_TRACK_BYTES; i++) {
        fields[i].data = NULL;
        fields[i].len = 0;
        if (lens[i] > 0) {
            jobject f = (*env)->GetObjectArrayElement(env, bytes, i);
            (*env)->GetByteArrayRegion(env, (jbyteArray)f, 0, lens[i], (jbyte *)(buf + off));
            (*env)->DeleteLocalRef(env, f);
            fields[i].data = buf + off;
            fields[i].len = (size_t)lens[i];
            off += (size_t)lens[i];
        }
    }
    if ((*env)->ExceptionCheck(env)) {
        free(buf);
        moq5_sender_release(tx);
        return (jint)MOQ_ERR_INVAL;
    }

    moq_media_track_cfg_init(&tcfg);
    tcfg.name = fields[0];
    tcfg.codec = fields[1];
    tcfg.init_data = fields[2];
    tcfg.role = fields[3];
    tcfg.lang = fields[4];
    tcfg.channel_config = fields[5];
    tcfg.media_type = (moq_media_type_t)sc[0];
    tcfg.packaging = (moq_media_packaging_t)sc[1];
    tcfg.timescale = (uint32_t)sc[2];
    tcfg.is_live = (sc[3] != 0);
    tcfg.width = (uint32_t)sc[4];
    tcfg.height = (uint32_t)sc[5];
    tcfg.framerate_millis = moq5_handle_from_jlong(sc[6]);
    tcfg.samplerate = (uint32_t)sc[7];
    tcfg.bitrate = moq5_handle_from_jlong(sc[8]);
    tcfg.has_max_grp_sap = (sc[9] != 0);
    tcfg.max_grp_sap = (uint32_t)sc[10];
    tcfg.has_max_obj_sap = (sc[11] != 0);
    tcfg.max_obj_sap = (uint32_t)sc[12];
    tcfg.emit_sap_timeline = (sc[13] != 0);
    tcfg.sap_timeline_history_groups = (uint32_t)sc[14];
    tcfg.has_track_duration = (sc[15] != 0);
    tcfg.track_duration_ms = moq5_handle_from_jlong(sc[16]);
    tcfg.emit_media_timeline = (sc[17] != 0);
    tcfg.media_timeline_history_groups = (uint32_t)sc[18];
    tcfg.has_alt_group = (sc[19] != 0);
    tcfg.alt_group = (int)sc[20];

    /* ADDING A TRACK IS TRANSACTIONAL, IN THE ROOT'S ORDER.
     *
     * acquire output -> reserve -> add -> abort or commit -> write -> release.
     *
     * The two fallible steps that can be taken before the track exists are
     * taken there: the caller's output storage, then the registry slot. What
     * follows can only be the real service call, whose refusal aborts a
     * reservation that owns nothing, or its success, whose commit cannot fail
     * for this thread's own live reservation. There is deliberately NO removal
     * of an added track anywhere here: `moq_media_sender_remove_track` is
     * itself fallible -- it refuses on the endpoint's interrupt latch -- so a
     * rollback built on it can leave a track the C still holds and no token can
     * name.
     *
     * Every refusal after a successful acquisition writes NOTHING into the
     * caller's storage and releases with JNI_ABORT: the storage may be the
     * array itself rather than a copy, and JNI_ABORT does not undo a direct
     * write. Only the committed path writes, and it releases with mode 0. */
    outv = moq5_acquire_out(env, out);
    if (outv == NULL) {
        /* Nothing was acquired, so nothing is released. Any exception the
         * runtime raised stays pending and is the caller's answer; the code
         * below is what an acquisition failure without one reports. */
        free(buf);
        moq5_sender_release(tx);
        return (jint)MOQ_ERR_NOMEM;
    }

    rr = moq5_reg_reserve_child((moq5_handle_t)tx, MOQ5_KIND_CHILD_BORROWED,
                                MOQ5_TYPE_SENDER_TRACK, &h);
    if (rr != MOQ5_REG_OK) {
        /* The registry refused BEFORE the service was called: no track exists,
         * so the name the caller asked for is still free. */
        (*env)->ReleaseLongArrayElements(env, out, outv, JNI_ABORT);
        free(buf);
        moq5_sender_release(tx);
        return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    }

    crc = moq_media_sender_add_track(child->s, &tcfg, &track);
    free(buf);
    if (crc != MOQ_OK || track == NULL) {
        /* The reservation owns nothing, so returning it consumes nothing --
         * not even the slot's generation. */
        if (moq5_reg_abort_child((moq5_handle_t)tx, h, MOQ5_TYPE_SENDER_TRACK) != MOQ5_REG_OK) {
            moq5_invariant_failed("abort of this thread's own unused child reservation refused");
        }
        (*env)->ReleaseLongArrayElements(env, out, outv, JNI_ABORT);
        moq5_sender_release(tx);
        return (jint)(crc != MOQ_OK ? crc : MOQ_ERR_INVAL);
    }

    /* Nonallocating, on this thread's own live reservation under a live owner:
     * there is nothing left here that can fail. */
    if (moq5_reg_commit_child((moq5_handle_t)tx, h, MOQ5_TYPE_SENDER_TRACK, (void *)track, NULL) != MOQ5_REG_OK) {
        moq5_invariant_failed("commit of this thread's own live child reservation refused");
    }
    published = moq5_handle_to_jlong(h);
    outv[0] = published;
    (*env)->ReleaseLongArrayElements(env, out, outv, 0);
    moq5_sender_release(tx);
    return (jint)MOQ_OK;
}

static jint JNICALL moq5_impl_nativeSenderRemoveTrack(JNIEnv *env, jobject self, jlong root,
                                                      jlong tx, jlong track)
{
    moq5_sender_child_t *child = NULL;
    moq_media_track_t *t = NULL;
    jint rc;
    moq_result_t crc;

    (void)env;
    (void)self;
    rc = moq5_sender_acquire(root, tx, &child);
    if (rc != (jint)MOQ_OK) return rc;
    rc = moq5_sender_track_acquire(tx, track, &t);
    if (rc != (jint)MOQ_OK) {
        moq5_sender_release(tx);
        return rc;
    }
    crc = moq_media_sender_remove_track(child->s, t);
    (void)moq5_reg_release((moq5_handle_t)track);
    moq5_sender_release(tx);
    return (jint)crc;
}

static jint JNICALL moq5_impl_nativeSenderWrite(JNIEnv *env, jobject self, jlong root,
                                                jlong tx, jlong track, jbyteArray payload,
                                                jbyteArray props, jlongArray scalars)
{
    jlong sc[MOQ5_TX_OBJ_SCALARS];
    moq_media_send_object_t obj;
    moq5_sender_child_t *child = NULL;
    moq_media_track_t *t = NULL;
    moq_rcbuf_t *pbuf = NULL;
    moq_rcbuf_t *xbuf = NULL;
    uint8_t *tmp = NULL;
    jsize plen, xlen = 0;
    jint rc;
    moq_result_t crc;

    (void)self;
    if (payload == NULL) return (jint)MOQ_ERR_INVAL;
    if (!moq5_read_scalars(env, scalars, (jsize)MOQ5_TX_OBJ_SCALARS, sc)) return (jint)MOQ_ERR_INVAL;
    if (!moq5_is_flag(sc[0]) || !moq5_is_flag(sc[1]) || !moq5_is_flag(sc[2]) || !moq5_is_flag(sc[5]) ||
        !moq5_is_flag(sc[7])) {
        return (jint)MOQ_ERR_INVAL;
    }
    if (sc[7] != 0 && (sc[8] < 0 || sc[8] > 3)) return (jint)MOQ_ERR_INVAL;   /* a declared SAP type is NONE or TYPE_1..3 */
    plen = (*env)->GetArrayLength(env, payload);
    if (props != NULL) xlen = (*env)->GetArrayLength(env, props);

    rc = moq5_sender_acquire(root, tx, &child);
    if (rc != (jint)MOQ_OK) return rc;
    rc = moq5_sender_track_acquire(tx, track, &t);
    if (rc != (jint)MOQ_OK) {
        moq5_sender_release(tx);
        return rc;
    }

    /* The bytes cross into fresh rcbufs built here; they are transferred to
     * the service on MOQ_OK only and released on every other path. */
    tmp = (uint8_t *)malloc((size_t)plen + (size_t)xlen + 1);
    if (tmp == NULL) {
        crc = MOQ_ERR_NOMEM;
        goto done;
    }
    (*env)->GetByteArrayRegion(env, payload, 0, plen, (jbyte *)tmp);
    if (props != NULL) (*env)->GetByteArrayRegion(env, props, 0, xlen, (jbyte *)(tmp + plen));
    if ((*env)->ExceptionCheck(env)) {
        crc = MOQ_ERR_INVAL;
        goto done;
    }
    crc = moq_rcbuf_create(moq_alloc_default(), tmp, (size_t)plen, &pbuf);
    if (crc != MOQ_OK) goto done;
    if (props != NULL) {
        crc = moq_rcbuf_create(moq_alloc_default(), tmp + plen, (size_t)xlen, &xbuf);
        if (crc != MOQ_OK) goto done;
    }

    memset(&obj, 0, sizeof obj);
    obj.struct_size = (uint32_t)sizeof obj;
    obj.payload = pbuf;
    obj.properties = xbuf;
    obj.is_sync = (sc[0] != 0);
    obj.starts_group = (sc[1] != 0);
    obj.ends_group = (sc[2] != 0);
    obj.decode_time_us = moq5_handle_from_jlong(sc[3]);
    obj.presentation_time_us = moq5_handle_from_jlong(sc[4]);
    obj.has_capture_time = (sc[5] != 0);
    obj.capture_time_us = moq5_handle_from_jlong(sc[6]);
    obj.has_sap_type = (sc[7] != 0);
    obj.sap_type = (moq_sap_type_t)sc[8];

    crc = moq_media_sender_write(child->s, t, &obj);
    if (crc == MOQ_OK) {
        /* Transferred: the service owns both refs now. */
        pbuf = NULL;
        xbuf = NULL;
    }

done:
    if (pbuf != NULL) moq_rcbuf_decref(pbuf);
    if (xbuf != NULL) moq_rcbuf_decref(xbuf);
    free(tmp);
    (void)moq5_reg_release((moq5_handle_t)track);
    moq5_sender_release(tx);
    return (jint)crc;
}

static jint JNICALL moq5_impl_nativeSenderEndTrack(JNIEnv *env, jobject self, jlong root,
                                                   jlong tx, jlong track)
{
    moq5_sender_child_t *child = NULL;
    moq_media_track_t *t = NULL;
    jint rc;
    moq_result_t crc;

    (void)env;
    (void)self;
    rc = moq5_sender_acquire(root, tx, &child);
    if (rc != (jint)MOQ_OK) return rc;
    rc = moq5_sender_track_acquire(tx, track, &t);
    if (rc != (jint)MOQ_OK) {
        moq5_sender_release(tx);
        return rc;
    }
    crc = moq_media_sender_end_track(child->s, t);
    (void)moq5_reg_release((moq5_handle_t)track);
    moq5_sender_release(tx);
    return (jint)crc;
}

static jint JNICALL moq5_impl_nativeSenderConvertToVod(JNIEnv *env, jobject self, jlong root,
                                                       jlong tx, jlongArray tracks,
                                                       jlongArray durations)
{
    jlong tk[MOQ5_TX_VOD_MAX];
    jlong du[MOQ5_TX_VOD_MAX];
    moq_media_vod_track_t items[MOQ5_TX_VOD_MAX];
    moq5_sender_child_t *child = NULL;
    jsize n, i, pinned = 0;
    jint rc;
    moq_result_t crc;

    (void)self;
    if (tracks == NULL || durations == NULL) return (jint)MOQ_ERR_INVAL;
    n = (*env)->GetArrayLength(env, tracks);
    if (n < 1 || n > (jsize)MOQ5_TX_VOD_MAX) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, durations) != n) return (jint)MOQ_ERR_INVAL;
    (*env)->GetLongArrayRegion(env, tracks, 0, n, tk);
    (*env)->GetLongArrayRegion(env, durations, 0, n, du);
    if ((*env)->ExceptionCheck(env)) return (jint)MOQ_ERR_INVAL;

    rc = moq5_sender_acquire(root, tx, &child);
    if (rc != (jint)MOQ_OK) return rc;
    for (i = 0; i < n; i++) {
        moq_media_track_t *t = NULL;
        rc = moq5_sender_track_acquire(tx, tk[i], &t);
        if (rc != (jint)MOQ_OK) goto refuse;
        pinned++;
        items[i].track = t;
        items[i].duration_ms = moq5_handle_from_jlong(du[i]);
    }
    crc = moq_media_sender_convert_to_vod(child->s, items, (size_t)n);
    rc = (jint)crc;
refuse:
    for (i = 0; i < pinned; i++) (void)moq5_reg_release((moq5_handle_t)tk[i]);
    moq5_sender_release(tx);
    return rc;
}

static jint JNICALL moq5_impl_nativeSenderComplete(JNIEnv *env, jobject self, jlong root,
                                                   jlong tx)
{
    moq5_sender_child_t *child = NULL;
    jint rc;
    moq_result_t crc;

    (void)env;
    (void)self;
    rc = moq5_sender_acquire(root, tx, &child);
    if (rc != (jint)MOQ_OK) return rc;
    crc = moq_media_sender_complete(child->s);
    moq5_sender_release(tx);
    return (jint)crc;
}

static jint JNICALL moq5_impl_nativeSenderProbe(JNIEnv *env, jobject self, jlong root,
                                                jlong tx, jlongArray scalars)
{
    jlong out[MOQ5_TX_PROBE_SCALARS];
    moq5_sender_child_t *child = NULL;
    jint rc;

    (void)self;
    if (scalars == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, scalars) < (jsize)MOQ5_TX_PROBE_SCALARS) return (jint)MOQ_ERR_INVAL;
    rc = moq5_sender_acquire(root, tx, &child);
    if (rc != (jint)MOQ_OK) return rc;
    out[0] = moq_media_sender_is_ready(child->s) ? 1 : 0;
    out[1] = moq_media_sender_is_closed(child->s) ? 1 : 0;
    out[2] = moq_media_sender_is_fatal(child->s) ? 1 : 0;
    out[3] = moq5_handle_to_jlong(moq_media_sender_fatal_code(child->s));
    out[4] = moq_media_sender_has_media_subscriber(child->s) ? 1 : 0;
    (*env)->SetLongArrayRegion(env, scalars, 0, (jsize)MOQ5_TX_PROBE_SCALARS, out);
    moq5_sender_release(tx);
    return (jint)MOQ_OK;
}

static jint JNICALL moq5_impl_nativeSenderTrackDemand(JNIEnv *env, jobject self, jlong root,
                                                      jlong tx, jlong track, jlongArray out)
{
    moq5_sender_child_t *child = NULL;
    moq_media_track_t *t = NULL;
    jlong count;
    jint rc;

    (void)self;
    if (out == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, out) < 1) return (jint)MOQ_ERR_INVAL;
    rc = moq5_sender_acquire(root, tx, &child);
    if (rc != (jint)MOQ_OK) return rc;
    rc = moq5_sender_track_acquire(tx, track, &t);
    if (rc != (jint)MOQ_OK) {
        moq5_sender_release(tx);
        return rc;
    }
    count = (jlong)moq_media_sender_track_subscriptions(child->s, t);
    (*env)->SetLongArrayRegion(env, out, 0, 1, &count);
    (void)moq5_reg_release((moq5_handle_t)track);
    moq5_sender_release(tx);
    return (jint)MOQ_OK;
}

static jint JNICALL moq5_impl_nativeSenderStats(JNIEnv *env, jobject self, jlong root,
                                                jlong tx, jlongArray scalars)
{
    moq_media_sender_stats_t st;
    jlong out[MOQ5_TX_STATS_SCALARS];
    moq5_sender_child_t *child = NULL;
    jint rc;
    moq_result_t crc;

    (void)self;
    if (scalars == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, scalars) < (jsize)MOQ5_TX_STATS_SCALARS) return (jint)MOQ_ERR_INVAL;
    rc = moq5_sender_acquire(root, tx, &child);
    if (rc != (jint)MOQ_OK) return rc;
    memset(&st, 0, sizeof st);
    crc = moq_media_sender_get_stats(child->s, &st, sizeof st);
    if (crc == MOQ_OK) {
        out[0] = moq5_handle_to_jlong(st.objects_written);
        out[1] = moq5_handle_to_jlong(st.objects_sent);
        out[2] = moq5_handle_to_jlong(st.objects_queued);
        out[3] = moq5_handle_to_jlong(st.bytes_queued);
        out[4] = moq5_handle_to_jlong(st.objects_dropped);
        out[5] = moq5_handle_to_jlong(st.groups_dropped);
        out[6] = moq5_handle_to_jlong(st.keyframes_dropped);
        out[7] = moq5_handle_to_jlong(st.groups_abandoned);
        out[8] = moq5_handle_to_jlong(st.backpressure_stalls);
        out[9] = (jlong)st.last_error;
        out[10] = moq5_handle_to_jlong(st.sap_records_evicted);
        (*env)->SetLongArrayRegion(env, scalars, 0, (jsize)MOQ5_TX_STATS_SCALARS, out);
    }
    moq5_sender_release(tx);
    return (jint)crc;
}

#if defined(MOQ5_JNI_HOST_TEST)
/* THE POST-INVALIDATION MILESTONE.
 *
 * The registry fires this on the retiring authority thread AFTER the tree is
 * marked RETIRING and BEFORE it waits for pins, with its lock released. Every
 * fact the wake/retire composition claims is established here, in order:
 *
 *   the first wake is inside its lease and still pinned;
 *   retirement has begun and the token is invalidated;
 *   a SECOND any-thread call through the registered wake method is refused with
 *     exact registry STATE -- not STALE, which is what it would be had this run
 *     after retirement completed;
 *   no cleanup has run;
 * and only then is the first wake released, after which pins drain and cleanup
 * runs exactly once on this thread. */
static void moq5_retiring_milestone(void *arg)
{
    unsigned destroys_before;

    (void)arg;
    pthread_mutex_lock(&moq5_hold_mu);
    moq5_hold_milestones++;
    if (moq5_hold_milestones > 1) moq5_lc_latch(MOQ5_LC_MILESTONE_REPEATED);
    while (!moq5_hold_wake_entered) {
        pthread_cond_wait(&moq5_hold_cv, &moq5_hold_mu);
    }
    destroys_before = moq5_lc_destroys;

    /* Publish the milestone: the tree is RETIRING and the token is invalidated,
     * while the first lease is still held. The SECOND worker is waiting on this
     * and will now make its own call across the JVM boundary. */
    moq5_hold_retiring = 1;
    pthread_cond_broadcast(&moq5_hold_cv);
    while (!moq5_hold_probe_done) {
        pthread_cond_wait(&moq5_hold_cv, &moq5_hold_mu);
    }
    if (moq5_lc_destroys != destroys_before) moq5_lc_latch(MOQ5_LC_CLEANUP_RACED);

    moq5_hold_may_release = 1;
    pthread_cond_broadcast(&moq5_hold_cv);
    pthread_mutex_unlock(&moq5_hold_mu);
}
#endif

static jint JNICALL moq5_impl_nativeEndpointWait(JNIEnv *env, jobject self,
                                                 jlong token, jlong timeout_us)
{
    void *ptr = NULL;
    moq5_reg_result_t rr;
    moq_result_t rc;

    (void)env;
    (void)self;
    if (timeout_us < 0) return (jint)MOQ_ERR_INVAL;   /* the C parameter is unsigned */

    rr = moq5_root_acquire((moq5_handle_t)token, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);

    rc = MOQ5_CALL_WAIT((moq_endpoint_t *)ptr, (uint64_t)timeout_us);

    rr = moq5_reg_release((moq5_handle_t)token);
    if (rr != MOQ5_REG_OK) {
        moq5_invariant_failed("release of this thread's own pin refused");
    }
    /* VERBATIM: MOQ_OK, MOQ_DONE, MOQ_ERR_INTERRUPTED, MOQ_ERR_CLOSED and any
     * other value cross unchanged. Nothing is collapsed into a boolean and no
     * sign is narrowed. */
    return (jint)rc;
}

/* The snapshot's four values, in the order the record declares them. */
#define MOQ5_SNAPSHOT_SLOTS 4

static jint JNICALL moq5_impl_nativeEndpointSnapshot(JNIEnv *env, jobject self,
                                                     jlong token, jlongArray out)
{
    moq_endpoint_snapshot_t snap;
    jlong values[MOQ5_SNAPSHOT_SLOTS];
    void *ptr = NULL;
    moq5_reg_result_t rr;
    moq_result_t rc;

    (void)self;
    if (out == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, out) < MOQ5_SNAPSHOT_SLOTS) return (jint)MOQ_ERR_INVAL;

    rr = moq5_root_acquire((moq5_handle_t)token, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);

    /* ONE native record from ONE call. The snapshot is never rebuilt from
     * independent state/version/terminal accessors: composing them separately
     * is exactly the incoherence the C side was corrected to remove. */
    memset(&snap, 0, sizeof snap);
    rc = MOQ5_CALL_SNAPSHOT((const moq_endpoint_t *)ptr, &snap, sizeof snap);

    rr = moq5_reg_release((moq5_handle_t)token);
    if (rr != MOQ5_REG_OK) {
        moq5_invariant_failed("release of this thread's own pin refused");
    }
    if (rc != MOQ_OK) return (jint)rc;

    /* Widened, never narrowed: the version is unsigned and a future nonzero
     * value must survive, and the terminal detail carries all 64 bits as RAW
     * BITS through jlong -- bit 63 included. */
    values[0] = (jlong)(uint32_t)snap.state;
    values[1] = (jlong)(uint64_t)snap.negotiated_version;
    values[2] = (jlong)(uint32_t)snap.terminal_reason;
    values[3] = (jlong)snap.terminal_detail_code;
    (*env)->SetLongArrayRegion(env, out, 0, MOQ5_SNAPSHOT_SLOTS, values);
    if ((*env)->ExceptionCheck(env)) return (jint)MOQ_ERR_INTERNAL;
    return (jint)MOQ_OK;
}

static jint JNICALL moq5_impl_nativeEndpointStop(JNIEnv *env, jobject self,
                                                 jlong token)
{
    void *ptr = NULL;
    moq5_reg_result_t rr;
    moq_result_t rc;

    (void)env;
    (void)self;
    rr = moq5_root_acquire((moq5_handle_t)token, &ptr);
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);

    rc = moq5_ctor_target->stop((moq_endpoint_t *)ptr);

    rr = moq5_reg_release((moq5_handle_t)token);
    if (rr != MOQ5_REG_OK) {
        moq5_invariant_failed("release of this thread's own pin refused");
    }
    if (rc == MOQ_OK) {
        /* The registry, not this frame, records that the owner is quiesced.
         * Only a SUCCESSFUL stop marks it: a refusal leaves the root live and
         * unready, so nothing can clean it. */
        rr = moq5_reg_mark_ready((moq5_handle_t)token);
        if (rr != MOQ5_REG_OK) {
            moq5_invariant_failed("marking this thread's own live root ready refused");
        }
    }
    /* Carried EXACTLY: MOQ_OK means stopped; every other value is the refusal,
     * unnormalised, and the endpoint is still live and still owned. */
    return (jint)rc;
}

static jint JNICALL moq5_impl_nativeEndpointDestroy(JNIEnv *env, jobject self,
                                                    jlong token)
{
    moq5_reg_result_t rr;

    (void)env;
    (void)self;
    /* DESTROY DOES NOT STOP.
     *
     * The engine calls stop() once and, only on Stopped, destroy() once. A
     * destroy that stopped again would make the second stop invisible and a
     * stop-refusal oracle ambiguous, so eligibility is asked of the REGISTRY
     * instead: an unready live root is refused by STATE, stays valid, and is
     * not cleaned. Retirement still invalidates the token before pins drain and
     * before cleanup, and that algorithm is gated here, not duplicated. */
#if defined(MOQ5_JNI_HOST_TEST)
    if (moq5_mode == MOQ5_MODE_WAKE && moq5_hold_armed) {
        /* The rendezvous is armed at the registry's own post-invalidation
         * milestone rather than here: releasing the wake before retirement even
         * started would prove only that cleanup follows a signal. */
        moq5_hold_token = token;
        /* Wait until the first wake is inside its lease BEFORE retirement
         * starts. Without this the retirement could invalidate the token first,
         * the wake would be refused, and the milestone would wait for a
         * rendezvous that can never arrive. The RELEASE still happens at the
         * milestone -- after invalidation -- so the order under test is
         * unchanged; this only fixes which side arrives first. */
        pthread_mutex_lock(&moq5_hold_mu);
        /* BOTH workers must have been classified, not just the holder: a probe
         * that had not yet arrived would otherwise meet an already-retired
         * token and the row would silently test nothing. */
        while (!moq5_hold_wake_entered || moq5_hold_arrivals < 2) {
            pthread_cond_wait(&moq5_hold_cv, &moq5_hold_mu);
        }
        pthread_mutex_unlock(&moq5_hold_mu);
    }
#endif

    rr = moq5_reg_retire_root_ready((moq5_handle_t)token);
#if defined(MOQ5_JNI_HOST_TEST)
    if (moq5_mode == MOQ5_MODE_WAKE && moq5_hold_armed) {
        /* ONE-SHOT: the hook and every handshake flag are cleared on EVERY
         * completed path, so a later root's teardown is an ordinary one and
         * cannot silently re-enter a stale milestone. */
        moq5_reg_test_on_retire_wait(NULL, NULL);
        pthread_mutex_lock(&moq5_hold_mu);
        moq5_hold_armed = 0;
        moq5_hold_claimed = 0;
        moq5_hold_arrivals = 0;
        moq5_hold_wake_entered = 0;
        moq5_hold_retiring = 0;
        moq5_hold_probe_done = 0;
        moq5_hold_may_release = 0;
        moq5_hold_token = 0;
        pthread_mutex_unlock(&moq5_hold_mu);
    }
#endif
    if (rr != MOQ5_REG_OK) return (jint)(MOQ5_ERR_REGISTRY_BASE + (int)rr);
    return (jint)MOQ_OK;
}

/* RELEASE is the bootstrap convenience: ONE stop, readiness marked by that
 * stop's own success, then the SAME ready-checked retirement destroy performs.
 * There is no second teardown implementation here -- it is the two operations
 * in their canonical order. */
static jint JNICALL moq5_impl_nativeEndpointRelease(JNIEnv *env, jobject self,
                                                    jlong token)
{
    jint rc = moq5_impl_nativeEndpointStop(env, self, token);
    if (rc != (jint)MOQ_OK) return rc;
    return moq5_impl_nativeEndpointDestroy(env, self, token);
}

/* The codec utility rows.
 *
 * Status transport is the existing codes only: OK; the utility's own
 * INVAL/PROTO/UNSUPPORTED refusals verbatim; the bridge's INVAL for an
 * argument it cannot represent; NOMEM for a native allocation failure or a
 * NULL JVM array without a pending exception; INTERNAL for any contract
 * failure, including every exit with a pending exception (whose status the
 * JVM never reads, because the exception is thrown on return).
 *
 * After any JNI operation that can throw, ExceptionCheck is the next JNI
 * call. With an exception pending only ExceptionCheck, DeleteLocalRef and
 * free follow; the exception is neither cleared nor replaced, and the utility
 * is not called. Every native block is owned here and freed on every path;
 * no Java or native buffer is retained past the call. */

/* One owned native copy of [arr]. An empty array still gets a non-NULL
 * one-byte block, so the utility never sees NULL for "empty". On failure
 * nothing is left allocated: NOMEM (no block) or INTERNAL (the copy threw;
 * the exception stays pending). */
static jint moq5_codec_copy_in(JNIEnv *env, jbyteArray arr, uint8_t **out, jsize *out_len)
{
    jsize n = (*env)->GetArrayLength(env, arr);
    uint8_t *p = (uint8_t *)malloc(n > 0 ? (size_t)n : 1u);
    *out = NULL;
    *out_len = 0;
    if (p == NULL) return (jint)MOQ_ERR_NOMEM;
    if (n > 0) {
        (*env)->GetByteArrayRegion(env, arr, 0, n, (jbyte *)p);
        if ((*env)->ExceptionCheck(env)) {
            free(p);
            return (jint)MOQ_ERR_INTERNAL;
        }
    }
    *out = p;
    *out_len = n;
    return (jint)MOQ_OK;
}

/* Exactly one of the two utility operations, over caller-owned stable input. */
typedef struct moq5_codec_op {
    const moq_codec_init_data_cfg_t *build;
    const moq_codec_string_cfg_t *format;
} moq5_codec_op_t;

static moq_result_t moq5_codec_call(const moq5_codec_op_t *op, uint8_t *buf, size_t cap, size_t *out_len)
{
    return op->build != NULL ? moq_codec_init_data_build(op->build, buf, cap, out_len)
                             : moq_codec_string_format(op->format, buf, cap, out_len);
}

/* The size query, the exact-capacity second call and the single commit into
 * out[0]. The holder is written only after the whole result is in a fully
 * filled array; every failure leaves its prior element untouched. */
static jint moq5_codec_run(JNIEnv *env, const moq5_codec_op_t *op, jobjectArray out)
{
    size_t need = 0, got = 0;
    moq_result_t rc = moq5_codec_call(op, NULL, 0, &need);
    uint8_t *buf;
    jbyteArray arr;

    if (rc == MOQ_ERR_INVAL || rc == MOQ_ERR_PROTO || rc == MOQ_ERR_UNSUPPORTED) {
        /* An ordinary refusal reports no length; one that does is not one. */
        return need == 0 ? (jint)rc : (jint)MOQ_ERR_INTERNAL;
    }
    if (rc != MOQ_ERR_BUFFER || need == 0 || need > (size_t)INT32_MAX) return (jint)MOQ_ERR_INTERNAL;

    buf = (uint8_t *)malloc(need);
    if (buf == NULL) return (jint)MOQ_ERR_NOMEM;
    rc = moq5_codec_call(op, buf, need, &got);
    if (rc != MOQ_OK || got != need) {
        free(buf);
        return (jint)MOQ_ERR_INTERNAL;
    }

    arr = (*env)->NewByteArray(env, (jsize)need);
    if (arr == NULL) {
        free(buf);
        return (*env)->ExceptionCheck(env) ? (jint)MOQ_ERR_INTERNAL : (jint)MOQ_ERR_NOMEM;
    }
    (*env)->SetByteArrayRegion(env, arr, 0, (jsize)need, (const jbyte *)buf);
    free(buf);
    if ((*env)->ExceptionCheck(env)) {
        (*env)->DeleteLocalRef(env, arr);
        return (jint)MOQ_ERR_INTERNAL;
    }
    (*env)->SetObjectArrayElement(env, out, 0, arr);
    if ((*env)->ExceptionCheck(env)) {
        (*env)->DeleteLocalRef(env, arr);
        return (jint)MOQ_ERR_INTERNAL;
    }
    (*env)->DeleteLocalRef(env, arr);
    return (jint)MOQ_OK;
}

static jint JNICALL moq5_impl_nativeCodecInitDataBuild(JNIEnv *env, jobject self,
                                                       jint source_format, jbyteArray source,
                                                       jint nal_length_size, jobjectArray out)
{
    moq_codec_init_data_cfg_t cfg;
    moq_codec_source_format_t format;
    moq5_codec_op_t op = { NULL, NULL };
    uint8_t *in;
    jsize n;
    jint rc;
    (void)self;

    /* Every argument is checked before any cast, copy or utility call. NULL
     * is not empty. */
    if (source == NULL || out == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, out) < 1) return (jint)MOQ_ERR_INVAL;
    switch (source_format) {
    case 1: format = MOQ_CODEC_SOURCE_AVC_ANNEXB; break;
    case 2: format = MOQ_CODEC_SOURCE_AVC_AVCC; break;
    case 3: format = MOQ_CODEC_SOURCE_AAC_ASC; break;
    default: return (jint)MOQ_ERR_INVAL;
    }
    /* The byte field must hold the request exactly; the utility alone decides
     * what a representable length size means for its source. */
    if (nal_length_size < 0 || nal_length_size > UINT8_MAX) return (jint)MOQ_ERR_INVAL;

    rc = moq5_codec_copy_in(env, source, &in, &n);
    if (rc != (jint)MOQ_OK) return rc;

    moq_codec_init_data_cfg_init(&cfg);
    cfg.source_format = format;
    cfg.source.data = in;
    cfg.source.len = (size_t)n;
    cfg.nal.length_size = (uint8_t)nal_length_size;
    op.build = &cfg;
    rc = moq5_codec_run(env, &op, out);
    free(in);
    return rc;
}

static jint JNICALL moq5_impl_nativeCodecStringFormat(JNIEnv *env, jobject self,
                                                      jint config_format, jbyteArray sample_entry,
                                                      jboolean has_oti, jint oti,
                                                      jbyteArray decoder_config, jobjectArray out)
{
    moq_codec_string_cfg_t cfg;
    moq_codec_config_format_t format;
    moq5_codec_op_t op = { NULL, NULL };
    uint8_t *entry, *config;
    jsize entry_len, config_len;
    jint rc;
    (void)self;

    if (sample_entry == NULL || decoder_config == NULL || out == NULL) return (jint)MOQ_ERR_INVAL;
    if ((*env)->GetArrayLength(env, out) < 1) return (jint)MOQ_ERR_INVAL;
    switch (config_format) {
    case 1: format = MOQ_CODEC_CONFIG_AVCC; break;
    case 2: format = MOQ_CODEC_CONFIG_AAC_ASC; break;
    default: return (jint)MOQ_ERR_INVAL;
    }
    /* A present OTI must fit its byte; an absent one carries the canonical
     * zero, so a value without the flag is a malformed request. */
    if (has_oti ? (oti < 0 || oti > UINT8_MAX) : oti != 0) return (jint)MOQ_ERR_INVAL;

    rc = moq5_codec_copy_in(env, sample_entry, &entry, &entry_len);
    if (rc != (jint)MOQ_OK) return rc;
    rc = moq5_codec_copy_in(env, decoder_config, &config, &config_len);
    if (rc != (jint)MOQ_OK) {
        free(entry);
        return rc;
    }

    moq_codec_string_cfg_init(&cfg);
    cfg.config_format = format;
    cfg.sample_entry.data = entry;
    cfg.sample_entry.len = (size_t)entry_len;
    cfg.has_mp4_object_type_indication = has_oti ? true : false;
    cfg.mp4_object_type_indication = (uint8_t)oti;
    cfg.decoder_config.data = config;
    cfg.decoder_config.len = (size_t)config_len;
    op.format = &cfg;
    rc = moq5_codec_run(env, &op, out);
    free(config);
    free(entry);
    return rc;
}

/* The one table, expanded from the authority. A row added to
 * `MOQ5_ABI_METHODS` with no `moq5_impl_<name>` fails this build by name. */
#define MOQ5_PRODUCT_ROW(name, desc) { #name, desc, (void *)(uintptr_t)moq5_impl_##name },

static const JNINativeMethod moq5_methods[] = {
    MOQ5_ABI_METHODS(MOQ5_PRODUCT_ROW)
};

/* Report a refusal to the loader with its CAUSE.
 *
 * A JNI_ERR return alone tells the caller only that something failed. The
 * detail names the class, or the row's exact name and descriptor, so a
 * mismatch is diagnosable from the Kotlin side without parsing vendor text.
 * Any exception the failed lookup left pending is cleared first, deliberately:
 * it would otherwise be the exception the caller sees instead of this one. */
static void moq5_refuse(JNIEnv *env, const char *what)
{
    jclass err = NULL;

    if ((*env)->ExceptionCheck(env)) {
        (*env)->ExceptionClear(env);
    }
    err = (*env)->FindClass(env, "java/lang/UnsatisfiedLinkError");
    if (err == NULL) {
        /* Nothing further can be reported; the JNI_ERR return still refuses. */
        if ((*env)->ExceptionCheck(env)) {
            (*env)->ExceptionClear(env);
        }
        return;
    }
    (void)(*env)->ThrowNew(env, err, what);
    (*env)->DeleteLocalRef(env, err);
}

__attribute__((visibility("default")))
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved)
{
    JNIEnv *env = NULL;
    jclass klass = NULL;
    jsize count = (jsize)(sizeof moq5_methods / sizeof moq5_methods[0]);
    jsize i;
    char detail[256];

    (void)reserved;
#ifndef MOQ5_JNI_HOST_TEST
    (void)moq5_anchors;
#endif

#if defined(MOQ5_JNI_HOST_TEST)
    /* Selected ONCE, here, before any method is registered and therefore before
     * any method is callable. */
    if (!moq5_select_constructor(detail, sizeof detail) ||
        !moq5_select_mode(detail, sizeof detail) ||
        !moq5_install_read_targets(detail, sizeof detail) ||
        !moq5_select_failpub(detail, sizeof detail) ||
        !moq5_select_failget(detail, sizeof detail) ||
        !moq5_select_failglobal(detail, sizeof detail)) {
        if ((*vm)->GetEnv(vm, (void **)&env, MOQ5_JNI_VERSION) == JNI_OK && env != NULL) {
            moq5_refuse(env, detail);
        }
        return JNI_ERR;
    }
#endif

    if ((*vm)->GetEnv(vm, (void **)&env, MOQ5_JNI_VERSION) != JNI_OK || env == NULL) {
        /* No usable environment: nothing can be thrown, and the version this
         * object requires is exactly what the refusal communicates. */
        return JNI_ERR;
    }

    klass = (*env)->FindClass(env, MOQ5_BRIDGE_CLASS);
    if (klass == NULL) {
        (void)snprintf(detail, sizeof detail,
                       "moq5-bridge: bound class %s not found", MOQ5_BRIDGE_CLASS);
        moq5_refuse(env, detail);
        return JNI_ERR;
    }

    /* The trust delegate's Java side and its thread-exit key, on the loading
     * thread and before any row is bound: a delegate that could not be
     * completed here must never become reachable. */
    moq5_trust.vm = vm;
    if (pthread_key_create(&moq5_trust.key, moq5_trust_thread_exit) != 0) {
        moq5_refuse(env, "moq5-bridge: cannot create the trust delegate's thread-exit key");
        moq5_trust_unbind(env);
        (*env)->DeleteLocalRef(env, klass);
        return JNI_ERR;
    }
    moq5_trust.key_created = 1;
    if (!moq5_trust_bind(env, detail, sizeof detail)) {
        moq5_refuse(env, detail);
        moq5_trust_unbind(env);
        (*env)->DeleteLocalRef(env, klass);
        return JNI_ERR;
    }

    /* PREFLIGHT: every row is resolved against its exact name, descriptor and
     * the authority-selected invocation shape BEFORE anything is bound, so a
     * mismatch refuses with nothing registered at all. */
    for (i = 0; i < count; i++) {
        jmethodID mid;

#if MOQ5_BRIDGE_METHOD_STATIC
        mid = (*env)->GetStaticMethodID(env, klass,
                                        moq5_methods[i].name, moq5_methods[i].signature);
#else
        mid = (*env)->GetMethodID(env, klass,
                                  moq5_methods[i].name, moq5_methods[i].signature);
#endif
        if (mid == NULL) {
            (void)snprintf(detail, sizeof detail,
                           "moq5-bridge: %s has no %s%s",
                           MOQ5_BRIDGE_CLASS, moq5_methods[i].name,
                           moq5_methods[i].signature);
            moq5_refuse(env, detail);
            moq5_trust_unbind(env);
            (*env)->DeleteLocalRef(env, klass);
            return JNI_ERR;
        }
    }

    /* ONE call binds the whole table. */
    if ((*env)->RegisterNatives(env, klass, moq5_methods, count) != JNI_OK) {
        jint undo;

        /* A failed RegisterNatives may have bound earlier rows AND may have left
         * a NoSuchMethodError pending. The pending exception is cleared FIRST:
         * UnregisterNatives is not one of the few JNI calls permitted while an
         * exception is pending, and calling it anyway is outside the contract
         * even where a particular VM tolerates it (chats/libmoq/1222 F1). */
        if ((*env)->ExceptionCheck(env)) {
            (*env)->ExceptionClear(env);
        }

        undo = (*env)->UnregisterNatives(env, klass);
        if (undo != JNI_OK) {
            /* The rollback itself failed, so rows may remain individually
             * callable while the table as a whole was refused. That is the one
             * state this design exists to make unreachable, and it cannot be
             * reported as a recoverable load failure: a caller told "not
             * loaded" could still reach a half-bound method. Fail closed. */
            (void)snprintf(detail, sizeof detail,
                           "moq5-bridge: rollback of a failed %d-row registration on %s "
                           "failed (%d); a partially callable table may remain",
                           (int)count, MOQ5_BRIDGE_CLASS, (int)undo);
            (*env)->FatalError(env, detail);
            return JNI_ERR; /* not reached: FatalError does not return */
        }

        (void)snprintf(detail, sizeof detail,
                       "moq5-bridge: RegisterNatives refused the %d-row table on %s",
                       (int)count, MOQ5_BRIDGE_CLASS);
        moq5_refuse(env, detail);
        moq5_trust_unbind(env);
        (*env)->DeleteLocalRef(env, klass);
        return JNI_ERR;
    }

    (*env)->DeleteLocalRef(env, klass);
    return MOQ5_JNI_VERSION;
}
