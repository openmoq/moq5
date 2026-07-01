# moq-playback v0 Design

## 1. Problem statement

Every downstream integration that consumes MoQ objects for media
playback reimplements the same nontrivial glue:

- Parse LOC object properties (capture timestamp, video frame marking)
- Route objects by payload packaging: raw encoded payload vs CMAF
  fragment — LOC properties may be present on either
- Parse CMAF init segments for timescale/codec config
- Parse CMAF fragments for sample tables and decode time
- Normalize two distinct timestamp domains: capture time (wall-clock
  from LOC) and media/decode time (timeline position from CMAF
  timescale or LOC-derived)
- Buffer objects by group/object for decode-safe ordering
- Handle out-of-order arrival from parallel QUIC streams
- Detect missing groups (gaps) with live-join awareness
- Wait for keyframe after a gap before resuming decode
- Drop stale objects after skip-forward
- Shed old groups under live backlog pressure
- Emit abstract decoder commands without calling any decoder
- Surface playback events for logging, analytics, and policy

Each integration prototype currently hand-rolls this in 200-500
lines of interleaved state management. The bugs are the same each
time: partial group decode, wrong keyframe detection, stale object
after skip, unbounded buffer growth, false gap on live join.

moq-playback centralizes this logic in a sans-I/O, deterministic,
testable library component.

## 2. Non-goals

- Player lifecycle (load/play/pause/seek)
- Subscription management or reconnect
- ABR quality switching policy
- Decoder or platform APIs
- Full MP4 parser
- Audio/video sync in v0 (video-only first)
- Catalog parsing (MSF populates track config externally)
- Automatic wall-clock timing
- Threads, sockets, real clocks

## 3. v0 scope

Video-only first. The API structurally supports multiple track
handles (`max_tracks` in config), but PB1-PB5 implementation and
tests exercise a single video track. Multi-track and audio are
deferred to v1+.

- Explicit track config supplied by caller (not discovered)
- Payload packaging: `RAW` or `CMAF`
- LOC properties parsed internally on either packaging
- CMAF init data parsed at `add_track` when present
- Dual timestamp model: `capture_time_us` + `decode_time_us` / `presentation_time_us`
- Bounded jitter buffer (group/object ordered min-heap)
- Group-ordered release with live-join anchoring
- Gap detection with configurable timeout
- Keyframe wait using LOC video frame marking
- Command output with parsed timing metadata
- Event output for gap/skip/malformed/ended/shed
- Decoder feedback: queue pressure, decode error
- Deterministic: all time via caller-injected `now_us`
- v0 CMAF: one sample per fragment only; multi-sample rejected

## 4. Packaging model

LOC is not a packaging type. LOC is object-level metadata
(properties/extensions) that may be present on any packaging.

```c
typedef enum moq_playback_packaging {
    MOQ_PLAYBACK_PACKAGING_RAW  = 1,  /* raw encoded payload */
    MOQ_PLAYBACK_PACKAGING_CMAF = 2,  /* moof+mdat fragment */
} moq_playback_packaging_t;
```

- **RAW**: payload is a raw encoded video access unit. LOC
  properties carry capture timestamp and keyframe flag.
- **CMAF**: payload is a CMAF fragment (moof+mdat). The pipeline
  parses the fragment at push time to extract sample timing and
  mdat range. LOC properties may also be present.

LOC property parsing happens on both packaging types.

## 5. Top-level playback config

```c
typedef struct moq_playback_cfg {
    uint32_t struct_size;
    uint32_t max_tracks;             /* default 4 */
    uint32_t max_buffered_objects;   /* jitter buffer entry cap; default 256 */
    uint64_t max_buffered_bytes;     /* retained payload byte budget; default 16 MiB */
    uint32_t max_backlog_groups;     /* live shedding threshold; default 3 */
    uint32_t max_commands;           /* command queue capacity; default 64 */
    uint32_t max_events;             /* event queue capacity; default 16 */
    uint64_t gap_timeout_us;         /* gap wait before skip; default 500000 */
    uint32_t max_release_per_tick;   /* bounded drain; 0 = unlimited; default 8 */
    uint32_t max_track_config_bytes; /* per-track codec+init copy cap; default 4096 */
} moq_playback_cfg_t;
```

`max_buffered_bytes` caps the total retained payload bytes across
the jitter buffer plus any internally queued decode commands. When
the byte budget is exceeded, `push_object` returns WOULD_BLOCK
(same as object count overflow). This prevents 256 × 2 MB frames
from silently consuming 512 MB.

`max_track_config_bytes` caps the total bytes copied per track for
codec string + init_data / codec_config at `add_track`. If the
caller's init_data exceeds this, `add_track` returns INVAL.

Every later section that references configuration fields refers
to this struct. The allocator is passed separately to
`moq_playback_create`.

## 6. Track config and CMAF init handling

```c
typedef struct moq_playback_track_cfg {
    uint32_t                     struct_size;
    moq_playback_media_type_t    media_type;
    moq_playback_packaging_t     packaging;
    moq_bytes_t                  codec;      /* borrowed; copied internally */
    moq_bytes_t                  init_data;  /* borrowed; copied internally */
    uint32_t                     timescale;  /* caller hint; 0 = derive from init_data */
    uint32_t                     width;      /* caller hint; 0 = derive from init_data */
    uint32_t                     height;     /* caller hint; 0 = derive from init_data */
    uint32_t                     samplerate; /* reserved for v1 */
    uint32_t                     channel_count; /* reserved for v1 */
    uint64_t                     target_latency_us;
    bool                         is_live;
} moq_playback_track_cfg_t;
```

### CMAF init_data handling at add_track

For CMAF tracks, if `init_data` is non-empty, `add_track` calls
`moq_cmaf_parse_init` internally to derive timescale, codec kind,
width, height, and codec config from the init segment.

**Precedence**: parsed init_data values are authoritative. Caller-
supplied `timescale`, `width`, `height` serve as fallback when
init_data is absent or when the init segment does not contain
the field. If init_data is present and parsed successfully, its
values override the caller hints.

If init_data is present but malformed, `add_track` returns
`MOQ_ERR_PROTO`.

The pipeline stores internal copies of `codec`, `init_data`, and
any derived config. These copies outlive the caller's input.

## 7. Input object and ownership

```c
typedef struct moq_playback_object {
    uint32_t              struct_size;
    moq_playback_track_t *track;
    uint64_t              group_id;
    uint64_t              subgroup_id;
    uint64_t              object_id;
    uint8_t               publisher_priority;
    moq_object_status_t   status;
    bool                  end_of_group;
    bool                  datagram;
    moq_rcbuf_t          *payload;     /* borrowed for call */
    moq_rcbuf_t          *properties;  /* borrowed for call */
} moq_playback_object_t;
```

### Memory discipline

**Zero-copy payload path**:

1. `push_object` borrows `payload` and `properties` for the call.
2. On successful push, the pipeline calls `moq_rcbuf_incref` on
   `payload` exactly once. The payload bytes are never copied.
3. `properties` are parsed during push to extract LOC metadata.
   Properties are **not retained** — no incref, no copy.
4. The jitter buffer entry owns the single retained payload ref.
5. When tick releases an entry into a DECODE command, ownership
   of that ref transfers to the command. No additional incref
   where possible (entry is consumed).
6. `moq_playback_cmd_cleanup` decrefs the payload.
7. On push failure (WOULD_BLOCK, INVAL), no incref happens, no
   buffer state changes.

**Byte budget enforcement**:

The pipeline tracks `retained_bytes` (sum of `moq_rcbuf_len` for
all retained payload refs in jitter entries + queued commands).
`push_object` returns WOULD_BLOCK if accepting the object would
exceed `max_buffered_bytes`.

**Track config memory**:

`add_track` copies `codec` and `init_data` bytes into internal
storage via `moq_rcbuf_create`. The total per-track copy is
bounded by `max_track_config_bytes`. CONFIGURE_VIDEO commands
incref the stored rcbufs — no fresh byte copies per command.

**Destroy cleanup**:

`moq_playback_destroy` drains all jitter entries, queued commands,
and queued events, releasing every retained rcbuf. No leaks.

### push_object behavior

`push_object` does the following synchronously:

1. Parse LOC properties (if present) to extract capture_time_us
   and keyframe flag.
2. For CMAF packaging, parse the fragment to extract decode_time_us,
   composition_offset_us, sample count, mdat range. If
   sample_count != 1, emit OBJECT_DROPPED(reason:
   unsupported_multi_sample) and return OK (not fatal).
3. Insert the object (with parsed metadata) into the jitter buffer.

`push_object` does **not** emit DECODE/CONFIGURE commands. Those
are emitted only by `tick`.

If parsing fails (bad LOC or bad CMAF), `push_object` attempts
to emit a OBJECT_DROPPED event. If the event queue is full,
`push_object` returns `MOQ_ERR_WOULD_BLOCK` with commit-last
semantics (no refs retained, no buffer state changed). Caller
drains events and retries.

### Convenience helper

```c
moq_result_t moq_playback_push_sub_object(
    moq_playback_t *pb,
    const moq_sub_object_t *obj,
    moq_playback_track_t *track,
    uint64_t now_us);
```

## 8. Timestamp model

Two distinct timestamps, never conflated:

### capture_time_us

Wall-clock capture time in microseconds. Source: LOC
CaptureTimestamp. Useful for end-to-end latency measurement and
live catch-up. Present on both RAW and CMAF packaging when LOC
properties include CaptureTimestamp. Set to 0 / `has_capture_time
= false` when absent.

### decode_time_us / presentation_time_us / composition_offset_us

Three timing fields, distinct:

- `decode_time_us`: decode order timestamp (DTS). This is the
  primary ordering timestamp for jitter buffer release and
  monotonicity validation.
- `composition_offset_us`: signed offset from DTS to PTS. Zero
  when no reordering is present.
- `presentation_time_us`: presentation timestamp (PTS).
  `presentation_time_us = decode_time_us + composition_offset_us`.

**RAW packaging**: v0 requires LOC CaptureTimestamp. All three
are set equal:
```
decode_time_us = capture_time_us
composition_offset_us = 0
presentation_time_us = capture_time_us
```
If LOC CaptureTimestamp is absent, the object is dropped
(OBJECT_DROPPED event, reason: missing timestamp).

**CMAF packaging**: derived from the parsed fragment:
```
decode_time_us = (base_decode_time * 1000000) / timescale
composition_offset_us = (sample.composition_offset * 1000000) / timescale
presentation_time_us = decode_time_us + composition_offset_us
```
LOC CaptureTimestamp, if present, populates `capture_time_us`
separately. It is metadata for latency, not the CMAF DTS/PTS.

### Monotonicity

Monotonicity validation applies to `decode_time_us`, not
`presentation_time_us` (PTS may be non-monotonic with B-frames).
If `decode_time_us` goes backwards across consecutive released
objects, the pipeline emits OBJECT_DROPPED(reason: non-monotonic
decode time) and drops the object.

## 9. Commands

```c
typedef enum moq_playback_cmd_kind {
    MOQ_PLAYBACK_CMD_CONFIGURE_VIDEO = 1,
    MOQ_PLAYBACK_CMD_DECODE_VIDEO    = 2,
    MOQ_PLAYBACK_CMD_DECODE_CMAF     = 3,
    MOQ_PLAYBACK_CMD_RESET           = 4,
} moq_playback_cmd_kind_t;
```

### CONFIGURE_VIDEO

Emitted before the first decode command and after a RESET that
requires reconfiguration.

Fields:
- `codec` (OWNED rcbuf): codec string, e.g. `"avc1.42e01e"`.
- `codec_config` (OWNED rcbuf): codec-specific extradata parsed
  from the CMAF init segment via `moq_cmaf_parse_init` — this is
  the avcC, hvcC, av1C, esds DecoderSpecificInfo, or dOps body.
  For RAW tracks without CMAF init, this is the caller-supplied
  `init_data` bytes (opaque codec config).
- `width`, `height`: from parsed init or caller config.

Requires `moq_playback_cmd_cleanup`. This ensures the command
remains valid even if the track is removed while commands are
queued.

### DECODE_VIDEO (RAW packaging)

One command per object. Fields:

```c
struct {
    moq_playback_track_t *track;
    uint64_t    group_id;
    uint64_t    object_id;
    uint64_t    decode_time_us;
    int64_t     composition_offset_us;
    uint64_t    presentation_time_us;
    bool        has_capture_time;
    uint64_t    capture_time_us;
    bool        keyframe;
    moq_rcbuf_t *payload;          /* OWNED */
};
```

Requires cleanup.

### DECODE_CMAF (CMAF packaging)

One command per object (v0: one sample per fragment). Fields:

```c
struct {
    moq_playback_track_t *track;
    uint64_t    group_id;
    uint64_t    object_id;
    uint64_t    decode_time_us;
    int64_t     composition_offset_us;
    uint64_t    presentation_time_us;
    bool        has_capture_time;
    uint64_t    capture_time_us;
    bool        keyframe;
    uint32_t    sample_duration_us;
    moq_rcbuf_t *fragment;         /* OWNED: entire moof+mdat */
    size_t      mdat_offset;       /* offset of mdat body within fragment */
    size_t      mdat_len;          /* length of mdat body */
};
```

The command owns the original fragment rcbuf. `mdat_offset` and
`mdat_len` describe the mdat body range within the fragment
buffer (`fragment->data + mdat_offset`, `mdat_len` bytes).
Integrations can use `mdat_offset/mdat_len` for the raw media
data, or pass the entire fragment to an MP4 consumer.

Requires cleanup.

**v0: one-sample fragments only.** If `moq_cmaf_parse_fragment`
returns `sample_count != 1`, the pipeline emits
OBJECT_DROPPED(reason: unsupported multi-sample CMAF). This is
valid media, not malformed — the pipeline simply does not support
it yet. Multi-sample support is deferred to v1.

### RESET

Fields: `moq_playback_track_t *track`, reason enum (`gap`,
`decode_error`, `track_switch`). No owned refs.

### CONFIGURE_VIDEO track handle

Includes `moq_playback_track_t *track` in addition to the
codec/config/width/height fields described above.

### Midstream config updates (v0: unsupported)

v0 does not support midstream codec reconfiguration. The track
config is fixed at `add_track` time. CONFIGURE_VIDEO is emitted
only at initial configuration and after a RESET.

If a future object implies a config change (e.g. CMAF init data
differs from the track config), v0 emits RESET(reason=config_change)
and requires the caller to remove and re-add the track with new
config. This is explicitly not automatic — the pipeline does not
silently reconfigure a decoder.

## 10. Events

```c
typedef enum moq_playback_event_kind {
    MOQ_PLAYBACK_EVENT_GAP_DETECTED           = 1,
    MOQ_PLAYBACK_EVENT_KEYFRAME_WAITING       = 2,
    MOQ_PLAYBACK_EVENT_SKIP_FORWARD           = 3,
    MOQ_PLAYBACK_EVENT_OBJECT_DROPPED         = 4,
    MOQ_PLAYBACK_EVENT_TRACK_ENDED            = 5,
    MOQ_PLAYBACK_EVENT_BACKLOG_SHED           = 6,
    MOQ_PLAYBACK_EVENT_PARTIAL_GROUP_ABANDONED = 7,
} moq_playback_event_kind_t;

typedef enum moq_playback_drop_reason {
    MOQ_PLAYBACK_DROP_MALFORMED_LOC           = 1,
    MOQ_PLAYBACK_DROP_MALFORMED_CMAF          = 2,
    MOQ_PLAYBACK_DROP_MISSING_TIMESTAMP       = 3,
    MOQ_PLAYBACK_DROP_NON_MONOTONIC_DTS       = 4,
    MOQ_PLAYBACK_DROP_UNSUPPORTED_MULTI_SAMPLE = 5,
    MOQ_PLAYBACK_DROP_STALE                   = 6,
    MOQ_PLAYBACK_DROP_KEYFRAME_WAIT           = 7,
} moq_playback_drop_reason_t;
```

Every event includes `moq_playback_track_t *track`.

OBJECT_DROPPED replaces the previous MALFORMED_OBJECT. It
carries a `moq_playback_drop_reason_t` distinguishing malformed
media (bad LOC/CMAF), missing required metadata (no timestamp),
valid-but-unsupported input (multi-sample CMAF), non-monotonic
DTS, stale objects, and keyframe-wait drops.

Events are value types with fixed-size detail unions (scalars
only). No owned buffers, no cleanup needed.

## 11. Push/tick/backpressure semantics

### Who does what

| Function | Buffers objects | Parses LOC/CMAF | Emits commands | Emits events | Advances gap timers |
|----------|:-:|:-:|:-:|:-:|:-:|
| `push_object` | yes | yes | no | OBJECT_DROPPED only | no  |
| `tick` | no | no | yes | yes | yes |
| `poll_command` | no | no | — | — | no |
| `poll_event` | no | no | — | — | no |

### push_object WOULD_BLOCK

Returns WOULD_BLOCK when:
- Jitter buffer is at `max_buffered_objects` capacity, OR
- A OBJECT_DROPPED event needs to be emitted but the event
  queue is full.

In both cases: commit-last, no refs retained, no buffer state
changed. Caller drains events/commands and retries.

### tick WOULD_BLOCK

Returns WOULD_BLOCK when:
- Command queue is full (release wants to emit but cannot), OR
- Event queue is full (gap/skip/shed event cannot be queued).

In both cases: commit-last. Gap timers, release cursor, and
decoder state are not advanced. Caller drains queues and retries
with the same `now_us`.

### Event determinism

Events are never silently dropped. If the event queue is full,
the producing function returns WOULD_BLOCK. If an integration
wants lossy event behavior, it can drain and discard explicitly.

## 12. Gap/keyframe/live-join semantics

### Live-join anchoring

On the first accepted object for a track, the pipeline anchors
both group and object:

```
next_expected_group  = first.group_id
next_expected_object = first.object_id
```

No gap fires before this anchor. Joining at group 500 / object 3
does not report groups 0-499 or objects 0-2 as missing.

For the first anchored group, END_OF_GROUP and partial-group
handling work normally: the contiguous prefix starts from
`next_expected_object` (the anchor), not from 0. Objects with
`object_id < next_expected_object` in the anchor group are
silently ignored (they precede the join point).

After the anchor group completes, `next_expected_group` advances
and `next_expected_object` resets to 0 for subsequent groups.

### Gap detection

When the next expected group (`G`) is missing and an object from
a later group arrives:

1. Record `gap_first_seen_us = now_us` for group G.
2. On subsequent ticks, if `now_us - gap_first_seen_us >=
   gap_timeout_us`, emit GAP_DETECTED(G).
3. Emit RESET(reason=gap) + KEYFRAME_WAITING.
4. If a buffered group with a keyframe exists: set
   `next_expected_group` to that group. Emit
   SKIP_FORWARD(from=G, to=new_group). Resume decode.
5. If no keyframe group is currently buffered: remain in
   keyframe-waiting state. Drop all non-keyframe objects as
   they arrive. When a future keyframe object arrives, emit
   SKIP_FORWARD to its group and resume decode.

### Intra-group object gaps

v0 releases objects in `object_id` order within a group. If
object N is missing but N+1 is present, the buffer holds N+1
until:
- Object N arrives (normal case), or
- END_OF_GROUP is received (group is closed; see below), or
- The group times out (gap_timeout_us from first object arrival
  for the group).

### END_OF_GROUP

Receiving `end_of_group = true` closes the group: no later
object IDs will arrive. The pipeline then:

1. Releases the **contiguous prefix** of buffered objects
   starting from `next_expected_object`. E.g., if objects
   {0, 1, 3} are buffered, releases 0 and 1.
2. If any objects are missing from the prefix (object 2 in the
   example), emits PARTIAL_GROUP_ABANDONED and drops the
   remaining non-contiguous objects.
3. Advances `next_expected_group`.

This is safe: partial groups with missing dependencies are
abandoned rather than decoded with broken references.

### END_OF_TRACK

Object with `status == END_OF_TRACK` → emit TRACK_ENDED. No
further objects expected.

### Keyframe waiting

After gap/reset, the pipeline enters "needs keyframe" state.
Non-keyframe objects are dropped. When a keyframe arrives (LOC
VideoFrameMarking `independent == true`):

1. Emit CONFIGURE_VIDEO if not yet configured or after a reset.
2. Resume emitting DECODE_VIDEO / DECODE_CMAF commands.
3. Clear "needs keyframe" state.

### Stale rejection

After SKIP_FORWARD sets `next_expected_group = G`, objects with
`group_id < G` are silently dropped.

## 13. Error handling

**Malformed LOC**: bad property parse → OBJECT_DROPPED(reason:
malformed_loc) via push. Object dropped, pipeline continues.

**Missing timestamp**: RAW object without LOC CaptureTimestamp →
OBJECT_DROPPED(reason: missing_timestamp). Object dropped.

**Malformed CMAF**: bad fragment parse → OBJECT_DROPPED(reason:
malformed_cmaf). Object dropped.

**Multi-sample CMAF**: valid but unsupported in v0 →
OBJECT_DROPPED(reason: unsupported_multi_sample). Object dropped.

**Non-monotonic DTS**: → OBJECT_DROPPED(reason: non_monotonic_dts).
Object dropped.

**Decoder feedback**: `handle_feedback(QUEUE_PRESSURE)` → pipeline
pauses jitter buffer release during tick. Objects continue to be
accepted via push. `handle_feedback(DECODE_ERROR)` → pipeline
emits RESET(decode_error) + KEYFRAME_WAITING.

## 14. File/target layout

```
media/playback/
  include/moq/playback.h
  src/playback.c
  src/jitter.c
  src/timestamp.c
  tests/
    test_playback_order.c
    test_playback_gap.c
    test_playback_cmaf.c
    test_playback_backlog.c
    test_playback_oom.c
    test_playback_scenario.c
bindings/cpp/include/moq/playback.hpp
```

CMake target: `moq::playback`. Option: `MOQ_BUILD_PLAYBACK=OFF`
until API settles. Depends on `moq::core`, `moq::loc`, `moq::cmaf`.

## 15. Test plan

### Unit tests (deterministic, no network)

**Ordering**: in-order release, out-of-order arrival reordering,
duplicate ignored, stale after skip dropped.

**Live-join**: first object at group 500 anchors, no false gap.
Second at 502 → gap for 501 only.

**Gap**: missing group → wait → timeout → GAP_DETECTED + RESET +
KEYFRAME_WAITING. Keyframe → CONFIGURE + DECODE. Non-keyframe
during wait → dropped. Gap with no buffered keyframe → stay in
keyframe-waiting, drop non-keyframes, resume when keyframe arrives.

**END_OF_GROUP**: contiguous prefix released, missing objects →
PARTIAL_GROUP_ABANDONED.

**END_OF_TRACK**: TRACK_ENDED event.

**CMAF**: decode_time_us from base_decode_time / timescale.
composition_offset_us from sample. Capture time separate.
Multi-sample → OBJECT_DROPPED(unsupported). Missing init →
OBJECT_DROPPED(malformed).

**RAW**: decode_time_us = capture_time_us from LOC. Missing LOC
CaptureTimestamp → OBJECT_DROPPED(missing_timestamp). Keyframe
from VideoFrameMarking.

**Backlog**: exceed max_backlog_groups → BACKLOG_SHED.

**Refcount** (required from PB1/PB2):
- push retains payload exactly once (refcount before/after)
- WOULD_BLOCK/INVAL leaves refcount unchanged
- poll_command still owns the payload ref
- command cleanup releases the ref (refcount returns to caller-only)
- destroy with buffered objects releases all retained refs

**Capacity** (required from PB2):
- object count full → WOULD_BLOCK
- byte budget full → WOULD_BLOCK
- command queue full during tick → WOULD_BLOCK, no state mutation
- event queue full during push → WOULD_BLOCK, no state mutation
- max_release_per_tick bounds drain count
- backlog shed releases refs for dropped groups

**Commit-last** (required from PB1):
- every WOULD_BLOCK path: no partial state, no leaked refs
- retry after drain succeeds

**Malformed input** (required from PB3/PB4):
- bad LOC → OBJECT_DROPPED(malformed_loc)
- bad CMAF → OBJECT_DROPPED(malformed_cmaf)
- missing RAW timestamp → OBJECT_DROPPED(missing_timestamp)
- multi-sample CMAF → OBJECT_DROPPED(unsupported_multi_sample)
- non-monotonic DTS → OBJECT_DROPPED(non_monotonic_dts)

**OOM sweeps** (required from PB1):
- create allocation failure
- add_track config copy allocation failure
- jitter insert at capacity (not OOM — capacity error)
- destroy cleanup balance (all allocs freed)

**Convenience**: push_sub_object maps correctly.

### Scenario tests (SimPair-fed)

- Live join, no gap: subscriber joins mid-stream, no false events
- Group gap → RESET → keyframe recovery
- Delayed delivery → correct reorder
- Command queue WOULD_BLOCK → drain → retry → success
- Malformed LOC/CMAF → OBJECT_DROPPED with reason, pipeline continues
- CMAF timestamp normalization at known timescale

All seeded deterministic. Same seed → same command/event output.

## 16. Slicing plan

| Slice | Scope | Deliverable |
|-------|-------|-------------|
| PB0 | API header | `playback.h` with all types, enums, function declarations |
| PB1 | Skeleton | create/destroy, push/poll, empty tick, command/event queues |
| PB2 | Jitter buffer | min-heap, bounded capacity, release in order, live-join anchor |
| PB3 | RAW + LOC | parse properties on push, keyframe flag, DECODE_VIDEO with dual timestamps |
| PB4 | CMAF | parse fragment on push, decode_time_us/composition_offset, DECODE_CMAF, init_data at add_track |
| PB5 | Gap + keyframe | gap timeout, KEYFRAME_WAITING, SKIP_FORWARD, END_OF_GROUP/TRACK, partial group |
| PB6 | Backlog + feedback | max_backlog_groups, queue pressure, BACKLOG_SHED, WOULD_BLOCK commit-last |
| PB7 | C++ wrapper | `moq::playback` namespace, RAII, span-based |
| PB8 | Demo integration | local-media client optionally uses playback pipeline |
| PB9 | SimPair scenarios | seeded deterministic scenario tests under transport conditions |
