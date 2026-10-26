/* Typed host fakes for test_threaded_windows_model only. Not a Windows SDK. */
#ifndef MOQ_TEST_WINDOWS_H
#define MOQ_TEST_WINDOWS_H
#ifndef MOQ_TEST_WIN32_FAKES
#error "This header is only for the host Windows model test"
#endif
#include <stdint.h>

typedef uint32_t DWORD;
typedef uint32_t ULONG;
typedef int BOOL;
typedef struct { int initialized; int held; } SRWLOCK;
typedef struct { int initialized; } CONDITION_VARIABLE;
typedef struct { int64_t QuadPart; } LARGE_INTEGER;

#define INFINITE UINT32_C(0xffffffff)
#define ERROR_TIMEOUT UINT32_C(1460)
#define ERROR_INVALID_PARAMETER UINT32_C(87)

void InitializeSRWLock(SRWLOCK *lock);
void AcquireSRWLockExclusive(SRWLOCK *lock);
void ReleaseSRWLockExclusive(SRWLOCK *lock);
BOOL TryAcquireSRWLockExclusive(SRWLOCK *lock);
void InitializeConditionVariable(CONDITION_VARIABLE *cond);
void WakeAllConditionVariable(CONDITION_VARIABLE *cond);
BOOL SleepConditionVariableSRW(CONDITION_VARIABLE *cond, SRWLOCK *lock,
                               DWORD milliseconds, ULONG flags);
DWORD GetLastError(void);
DWORD GetCurrentThreadId(void);
void Sleep(DWORD milliseconds);
BOOL QueryPerformanceFrequency(LARGE_INTEGER *frequency);
BOOL QueryPerformanceCounter(LARGE_INTEGER *counter);
#endif
