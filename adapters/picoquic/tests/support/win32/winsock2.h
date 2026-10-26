/* Only the Winsock ownership calls used by the actual threaded facade. */
#ifndef MOQ_TEST_WINSOCK2_H
#define MOQ_TEST_WINSOCK2_H
#ifndef MOQ_TEST_WIN32_FAKES
#error "This header is only for the host Windows model test"
#endif
#include <stdint.h>

typedef uint16_t WORD;
typedef struct { WORD wVersion; } WSADATA;
#define MAKEWORD(low, high) ((WORD)((uint8_t)(low) | ((uint16_t)(uint8_t)(high) << 8)))

int WSAStartup(WORD version, WSADATA *data);
int WSACleanup(void);
#endif
