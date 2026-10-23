/*
 * moq_posix_compat.h - the POSIX threading/clock subset used by the threaded
 * tiers (service, managed picoquic helper).
 *
 * On POSIX this is just <pthread.h> + <time.h>. MSVC ships neither pthreads
 * nor clock_gettime, so on Windows the exact subset libmoq uses is mapped onto
 * native primitives: SRW locks (mutex), condition variables, thread ids and
 * the UCRT/QPC clocks. Only default attributes (NULL) are supported, which is
 * all libmoq passes. The Windows mutex is non-recursive, like the default
 * pthread mutex.
 *
 * Private header: not installed, not part of any public API.
 */
#ifndef MOQ_POSIX_COMPAT_H
#define MOQ_POSIX_COMPAT_H

#include <time.h>

#ifndef _WIN32

#include <pthread.h>

#else /* _WIN32 */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>   /* must precede windows.h (picoquic includes it too) */
#include <windows.h>
#include <errno.h>
#include <stdint.h>

typedef SRWLOCK            pthread_mutex_t;
typedef CONDITION_VARIABLE pthread_cond_t;
typedef DWORD              pthread_t;

static __inline int pthread_mutex_init(pthread_mutex_t *m, const void *attr)
{
    (void)attr;
    InitializeSRWLock(m);
    return 0;
}
static __inline int pthread_mutex_destroy(pthread_mutex_t *m) { (void)m; return 0; }
static __inline int pthread_mutex_lock(pthread_mutex_t *m)
{
    AcquireSRWLockExclusive(m);
    return 0;
}
static __inline int pthread_mutex_unlock(pthread_mutex_t *m)
{
    ReleaseSRWLockExclusive(m);
    return 0;
}
static __inline int pthread_mutex_trylock(pthread_mutex_t *m)
{
    return TryAcquireSRWLockExclusive(m) ? 0 : EBUSY;
}

static __inline int pthread_cond_init(pthread_cond_t *c, const void *attr)
{
    (void)attr;
    InitializeConditionVariable(c);
    return 0;
}
static __inline int pthread_cond_destroy(pthread_cond_t *c) { (void)c; return 0; }
static __inline int pthread_cond_broadcast(pthread_cond_t *c)
{
    WakeAllConditionVariable(c);
    return 0;
}
static __inline int pthread_cond_wait(pthread_cond_t *c, pthread_mutex_t *m)
{
    return SleepConditionVariableSRW(c, m, INFINITE, 0) ? 0 : EINVAL;
}

/* pthread_cond_timedwait takes an absolute CLOCK_REALTIME deadline (no
 * condattr clock is ever set); convert it to a relative millisecond wait. */
static __inline int pthread_cond_timedwait(pthread_cond_t *c, pthread_mutex_t *m,
                                           const struct timespec *abstime)
{
    struct timespec now;
    timespec_get(&now, TIME_UTC);
    int64_t ms = ((int64_t)abstime->tv_sec - (int64_t)now.tv_sec) * 1000 +
                 ((int64_t)abstime->tv_nsec - (int64_t)now.tv_nsec) / 1000000;
    if (ms < 0)
        ms = 0;
    if (ms >= (int64_t)INFINITE)
        ms = (int64_t)INFINITE - 1;
    if (SleepConditionVariableSRW(c, m, (DWORD)ms, 0))
        return 0;
    return GetLastError() == ERROR_TIMEOUT ? ETIMEDOUT : EINVAL;
}

static __inline pthread_t pthread_self(void) { return GetCurrentThreadId(); }
static __inline int pthread_equal(pthread_t a, pthread_t b) { return a == b; }

#ifndef CLOCK_REALTIME
#define CLOCK_REALTIME 0
#endif
#ifndef CLOCK_MONOTONIC
#define CLOCK_MONOTONIC 1
#endif

static __inline int clock_gettime(int clock_id, struct timespec *ts)
{
    if (clock_id == CLOCK_MONOTONIC) {
        LARGE_INTEGER freq, count;
        QueryPerformanceFrequency(&freq);
        QueryPerformanceCounter(&count);
        ts->tv_sec = (time_t)(count.QuadPart / freq.QuadPart);
        ts->tv_nsec = (long)((count.QuadPart % freq.QuadPart) * 1000000000LL /
                             freq.QuadPart);
        return 0;
    }
    return timespec_get(ts, TIME_UTC) == TIME_UTC ? 0 : -1;
}

#endif /* _WIN32 */

#endif /* MOQ_POSIX_COMPAT_H */
