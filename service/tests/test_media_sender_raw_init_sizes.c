/* Prints the host sizes of the sender's private track and sender structs. */
#include <stddef.h>
#include <stdio.h>
size_t moq_media_sender_test_sizeof_track(void);
size_t moq_media_sender_test_sizeof_sender(void);
int main(void)
{
    printf("sizeof(moq_media_track_t)=%zu sizeof(moq_media_sender_t)=%zu\n",
           moq_media_sender_test_sizeof_track(), moq_media_sender_test_sizeof_sender());
    return 0;
}
