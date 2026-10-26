## moq/playback.h

```text
Sans-I/O media playback pipeline.

Accepts MoQ objects, parses LOC/CMAF metadata internally, buffers
in decode order, detects gaps, and emits abstract decoder commands
and playback events. No sockets, no decoder, no threads, no clock.

Link against moq::playback (depends on moq::core, moq::loc, moq::cmaf).

Ownership: push_object borrows payload/properties for the call.
On success, payload is retained with one rcbuf incref (zero-copy).
Properties are parsed and not retained. Commands own payload refs
and require moq_playback_cmd_cleanup. Events are scalar-only.

Every caller-filled struct begins with struct_size; the library reads a
field beyond the struct's frozen v0 floor only when struct_size covers the
whole field. Initialize them with the _init macros, which pass the
caller's sizeof to the exported _init_sized functions.
```

## moq/media_player.h

```text
moq_media_player: decode-order media for an application's decoders.

The player composes a moq_media_receiver (catalog, subscriptions, the
object queue and its stream-terminal lane) with a moq_playback engine
(per-track decode order, knowledge of what exists, gap policy). The
application gets, per selected track, ordered OUTPUT records:

  CONFIGURE      a self-contained decoder configuration snapshot;
  UNIT           one access unit in decode order, with its decode and
                 presentation times (on CMAF the decode time is the
                 decode clock; on RAW/LOC it is a copy of the LOC media
                 time, equal to the presentation time, and not evidence
                 of a known decode clock: decode order is output order,
                 and with frame reordering it can decrease from one unit
                 to the next);
  DISCONTINUITY  the decoder must be reset before the next unit;
  TIMELINE       the presentation mapping of the track's timeline,
                 before its first unit;
  END            after the track's last unit, with why and how it ended.

Decoders, renderers, audio output and the presentation clock stay with
the application. Diagnostics (gaps, skips, drops, loss, selection) are
EVENT records on a separate queue and never interleave with OUTPUT.

This release supports RAW/LOC video and the RAW/LOC audio codecs `opus`
and `mp4a.40.2`, on drafts 16, 18 and 22. CMAF, MPEG-2 TS, dependent
spatial layers and timeline tracks are refused at selection with
MOQ_ERR_UNSUPPORTED; the receiver serves them directly.

Threading: the player owns no thread. Every call on a player, wait and
the observers included, is made by the application on one thread at a
time: none may run concurrently with another, tick included. wait and
the observers do not advance it. The receiver keeps its own network
thread.

Clock: now_us is the application's monotonic clock in microseconds, one
clock for tick, wait, handle_feedback and every deadline the player
reports; the player never reads a clock itself.

Records: OUTPUT and EVENT records are frozen polled-union layouts. Their
sizes and detail reserves are pinned; a poll needs a record of at least
the library's size and never truncates one. The configuration, feedback
and stats records follow the sized-init contract: a caller's struct_size
covers at least the frozen v0 floor, and fields past it read as zero.
```
