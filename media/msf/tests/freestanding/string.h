/* The <string.h> subset the freestanding 32-bit check of msf_names.c
 * compiles against; test_msf_names_ilp32.c defines these. */
#ifndef MOQ_MSF_TEST_FREESTANDING_STRING_H
#define MOQ_MSF_TEST_FREESTANDING_STRING_H

#include <stddef.h>

int memcmp(const void *a, const void *b, size_t n);
void *memcpy(void *dst, const void *src, size_t n);
void *memset(void *dst, int c, size_t n);
size_t strlen(const char *s);

#endif
