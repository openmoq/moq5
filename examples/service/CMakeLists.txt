# Consumer-facing service-tier examples.
#
# These build against the PUBLIC service headers only (moq/endpoint.h,
# moq/media_receiver.h, moq/media_sender.h, moq/rcbuf.h) and link ONLY
# moq::service -- no session, subscriber/publisher, or adapter APIs. The
# media helpers (msf -> media-object -> loc/cmaf -> core) are pulled in
# transitively through moq::service's public link interface. They compile
# with just the service tier; running them needs a transport facade compiled
# in (RAW_QUIC via pq-threaded or WebTransport via pico-wt-managed),
# otherwise connect() returns MOQ_ERR_UNSUPPORTED.

add_executable(moq_example_media_receive media_receive.c)
target_link_libraries(moq_example_media_receive PRIVATE moq::service)

add_executable(moq_example_media_send media_send.c)
target_link_libraries(moq_example_media_send PRIVATE moq::service)
