# Media service implementation

| Responsibility | File |
| --- | --- |
| Endpoint lifecycle, tasks and transport selection | `endpoint.c` |
| Endpoint URI validation | `endpoint_uri.c` |
| Owned resource authorization | `media_auth.c` |
| Receiver discovery, selection and object delivery | `media_receiver.c` |
| Player orchestration | `media_player.c` |
| Sender configuration, construction and state queries | `media_sender.c` |
| Sender track configuration, registration and removal | `media_sender_tracks.c` |
| Queue admission, draining, writes and blocking waits | `media_sender_write.c` |
| Catalog authoring, generations and refresh | `media_sender_catalog.c` |
| Completion, conversion and terminal handoff | `media_sender_complete.c` |
| Publisher callbacks, demand and endpoint pumping | `media_sender_pump.c` |
| Generated SAP and media timeline delivery | `media_sender_timeline.c` |
| MPEG-2 TS configuration and byte-stream delivery | `media_sender_mpeg2ts.c` |
| Private sender test seams | `media_sender_testing.c` |

`media_sender_internal.h` owns the sender's private layout. Family-specific
internal headers declare only helpers used by another sender file. The testing
source is compiled only into the existing private test variants. MPEG-2 TS
entry points retain their unsupported behavior when that component is absent.
