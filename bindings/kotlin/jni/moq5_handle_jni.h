/* Carrying a handle across JNI as BITS.
 *
 * A token routinely exceeds `INT64_MAX` -- the first generation sets bit 31, so
 * bit 63 is set -- and converting an out-of-range `uint64_t` to a signed `jlong`
 * with a plain cast is implementation-defined. Today's compilers preserve the
 * bits, which is exactly why the defect would be invisible until it wasn't
 * (chats/libmoq/1232 F5).
 *
 * These helpers copy the representation instead, and assert at COMPILE TIME
 * that the two types are the same width, so a platform where they are not fails
 * the build rather than silently truncating.
 */
#ifndef MOQ5_HANDLE_JNI_H
#define MOQ5_HANDLE_JNI_H

#include <string.h>

#include <jni.h>

#include "moq5_registry.h"

_Static_assert(sizeof(moq5_handle_t) == sizeof(jlong),
               "a handle and a jlong must be the same width to carry bits");
_Static_assert(sizeof(moq5_handle_t) == 8, "a handle is 64 bits");

static inline jlong moq5_handle_to_jlong(moq5_handle_t h)
{
    jlong out;
    memcpy(&out, &h, sizeof out);
    return out;
}

static inline moq5_handle_t moq5_handle_from_jlong(jlong v)
{
    moq5_handle_t out;
    memcpy(&out, &v, sizeof out);
    return out;
}

#endif /* MOQ5_HANDLE_JNI_H */
