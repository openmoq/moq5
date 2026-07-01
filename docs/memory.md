# Memory Model

## Allocator

Every allocation goes through a caller-provided `moq_alloc_t` vtable:

```c
typedef struct moq_alloc {
    void *ctx;
    void *(*alloc)  (size_t size, void *ctx);
    void *(*realloc)(void *ptr, size_t old_size, size_t new_size, void *ctx);
    void  (*free)   (void *ptr, size_t size, void *ctx);
} moq_alloc_t;
```

The `old_size` parameter on `realloc` and the `size` parameter on `free`
support arena and slab allocators that need the size to bucket freed blocks.
Implementations backed by libc `malloc`/`free` can ignore these parameters.

The library never calls `malloc`, `calloc`, `realloc`, or `free` directly.
A CI grep enforces this.

`moq_alloc_default()` returns a static const allocator backed by libc for
convenience. The allocator vtable is copied into the session at creation
time; the `ctx` pointer must remain valid for the session's lifetime.

## Session Allocation

Each session is a single trailing allocation:

```
session struct | actions[] | events[] | send_buf | recv_buf
             | subs[] | announcements[] | subgroups[] | rx_streams[]
             | rx_finished[] | output_scratch
```

All sub-arrays are aligned to their element type's natural alignment. The
total size is bounded by a 64 MB hard cap and checked for overflow. Resource
limits (queue sizes, buffer sizes, pool sizes, scratch size) are configurable
through `moq_session_cfg_t`; zero means use the library default.

### Dynamic per-stream allocations

Beyond the single session allocation, the receive path makes per-stream
dynamic allocations:

- **input_buf**: per-rx-stream buffer for incoming data bytes. Allocated
  on first data arrival, grown via realloc on demand, freed when the
  stream completes or is reset. Exact-size allocation (no minimum).
- **payload_buf**: per-object payload accumulation buffer. Allocated when
  an object header is decoded, freed after rcbuf creation.
- **ns_id_buf**: per-announcement canonical namespace blob for duplicate
  detection. Allocated on publish/receive, freed on entry termination.
- **track_id_buf**: per-subscription canonical identity blob. Same pattern.

## Receive Buffer Budget

`max_receive_buffer_bytes` is a retained-byte budget covering:

- `recv_input_bytes`: allocated capacity of per-stream input_buf
  across all rx streams.
- `recv_payload_bytes`: in-progress payload_buf allocations + queued
  OBJECT_RECEIVED rcbuf payloads + queued properties rcbufs.

Budget checks use subtraction-form arithmetic to avoid overflow.
When the budget is exceeded, the session emits STOP_DATA for the
offending stream. Idle input_buf allocations are freed on compact
(when all buffered bytes are consumed).

Note: this is a retained-byte budget, not a strict allocator peak.
Transient rcbuf copies during payload/properties handoff can briefly
exceed the configured limit.

## Borrowed Inputs

When the caller feeds bytes to `moq_session_on_control_bytes`, the `buf`
pointer is borrowed for the duration of that call only. The session copies
incoming bytes into its internal receive buffer. The caller can free or
reuse the input buffer immediately after the call returns.

For `moq_session_on_data_bytes`, all caller bytes are copied into the
rx stream's input_buf before parsing. The caller's buffer is never
referenced after the call returns.

## Borrowed Outputs

Polled actions and events contain pointers into session-owned storage:

- `SEND_CONTROL` action data points into the session's send buffer.
- Event fields like `track_namespace`, `track_name`, `reason`, and
  `track_properties` point into the session's output scratch arena.

These pointers are valid until the next **advancing call** on the same
session. After an advancing call, the borrow epoch increments and all
previously polled pointers become invalid.

## Owned Outputs

Some polled records transfer ownership of refcounted buffers:

- `SEND_DATA` actions own a `payload` rcbuf. Ownership transfers to the
  caller on poll. The caller must call `moq_action_cleanup` to release.
- `OBJECT_RECEIVED` events own `payload` and `properties` rcbufs.
  Ownership transfers on poll. The caller must call `moq_event_cleanup`.

On session close or destroy, the session decrefs all queued owned
resources automatically.

### Borrow Epoch

Every polled action and event carries a `borrow_epoch` field, stamped at
poll time (not creation time). Call `moq_session_borrow_valid(s, epoch)`
to check whether borrowed data is still safe to read:

```c
moq_event_t ev;
moq_session_poll_events(session, &ev, 1);

uint64_t epoch = ev.borrow_epoch;
/* ev.u.subscribe_request.track_name is valid here */

moq_session_tick(session, now);
/* epoch is now stale; track_name pointer is invalid */
assert(!moq_session_borrow_valid(session, epoch));
```

### Output Scratch Arena

Variable-length event data (namespace parts, track names, reason phrases,
track properties) is copied into a per-session output scratch arena during
message handling. The scratch resets at the start of every advancing call,
at the same point the borrow epoch increments.

If the scratch is exhausted during a single advancing call, the handler
returns `MOQ_ERR_BUFFER`. If the scratch was already empty and still
cannot fit the data, the session closes with `INTERNAL_ERROR` to prevent
a permanent wedge.

In debug builds (`NDEBUG` not defined), the scratch is poisoned with
`0xCD` bytes on reset to help detect use-after-invalidation.

## Send Buffer Reclamation

The send buffer holds encoded control messages referenced by queued
`SEND_CONTROL` actions. At the start of each advancing call, if all
actions have been polled (the queue is fully drained), the send buffer
resets to offset zero. This is safe because:

1. All actions have been polled (caller has the data).
2. The borrow epoch just incremented (old pointers are invalid).

If any actions remain queued, the send buffer is not reclaimed.

## Refcounted Buffers (rcbuf)

`moq_rcbuf_t` provides refcounted byte buffers for zero-copy relay
forwarding. A buffer is created with `moq_rcbuf_create`, which copies
the input data into a single allocation (header + payload). Reference
counting via `incref`/`decref` allows multiple sessions to share the
same payload without copying.

The rcbuf allocator vtable is copied at creation time, so the final
`decref` frees with the correct allocator regardless of which session
holds the last reference. `incref(NULL)` returns NULL; `decref(NULL)`
is a no-op.

Ownership transfer points:

| Operation | Direction | Ref change |
|-----------|-----------|------------|
| `write_object(payload)` | App → session | Session increfs after validation |
| `write_object_data(data)` | App → session | Session increfs after validation |
| `poll_actions` (SEND_DATA) | Session → adapter | Ownership transfers; adapter must cleanup |
| `poll_events` (OBJECT_RECEIVED) | Session → app | Ownership transfers; app must cleanup |
| `close_with_error` / `destroy` | — | Session decrefs all queued refs |

### Shard model and same-shard fan-out

`rcbuf` refcounts are non-atomic, so every reference to a buffer must stay
within one **shard** — a single executor-affinity domain (thread, executor,
or run loop). `incref`/`decref` are *not* a cross-shard ownership mechanism.

**Same-shard fan-out is a host pattern, not a libmoq API.** libmoq provides
no fan-out helper, recipient-group object, or cross-session membership
state — the host owns the recipient list and loops over it, calling the
ordinary per-session publish API once per recipient with one shared
payload:

- One `moq_rcbuf_t *payload` is reused across all same-shard recipients;
  `write_object` retains it via a cheap non-atomic incref, so the buffer is
  shared, never copied.
- Each recipient is handled independently. If a recipient's session action
  queue is full, `moq_pub_write_object` returns `MOQ_ERR_WOULD_BLOCK` for
  *that recipient only*; the host records it for retry and continues — one
  blocked recipient must not abort the fan-out or drop the others' data.

**Crossing a shard boundary requires an explicit copy** —
`moq_rcbuf_clone(dst_alloc, src, out)` (C) or
`moq::buffer::clone_for_shard(dst_alloc)` (C++) — into storage owned by an
allocator valid on the destination shard. Never share an `rcbuf` across
shards via `incref`/`decref`.

libmoq owns no recipient membership, routing, cache, namespace policy, or
persistent fan-out groups — those are host concerns. The worked pattern is
`tests/unit/test_shard_fanout_cookbook.c`.

## Wrapped Buffers (Zero-Copy)

`moq_rcbuf_wrap()` creates an rcbuf header that references external
caller-owned memory without copying. The wrapped pointer is returned
verbatim by `moq_rcbuf_data()`.

Release callback (`moq_rcbuf_release_fn`):
- Called exactly once on final decref, before the rcbuf header is freed.
- If NULL, final decref frees only the header; the external data is
  not touched.
- The release callback receives the original `data`, `len`, and the
  caller-provided `release_ctx`.

## Sliced Buffers

`moq_rcbuf_slice()` creates a sub-reference into an existing rcbuf
without copying. The slice increfs the root parent; on final decref
of the slice, the root parent is decreffed (may trigger its release
callback).

Slices are flattened: slicing a slice references the root parent
directly. The intermediate slice can be decreffed before the child.
Bounds are validated against the immediate parent's length.

## Zero-Copy Receive

`moq_session_on_data_rcbuf()` accepts a caller-owned `moq_rcbuf_t`
(typically from `moq_rcbuf_wrap` over a transport buffer). When
eligible (streaming continuation chunks, no buffered data), emitted
OBJECT_CHUNK events carry slices of the input rcbuf instead of
copies. The adapter's transport buffer is released when the last
slice is decreffed via `moq_event_cleanup`.

This is the foundation for zero-copy relay/fanout: a transport adapter
can wrap an incoming QUIC buffer in an rcbuf, pass it through libmoq
for event dispatch, and release the transport buffer when the last
consumer decrefs. libmoq never owns the external transport buffer
except through the caller-provided release callback (sans-I/O).

The raw `on_data_bytes()` API always copies and never exposes caller
memory through events.

**Zero-copy begin chunk emission**: when the entire wire frame
(subgroup header + object header + payload) arrives in a single
`on_data_rcbuf` call with no prior partial header (`hdr_len == 0`)
and the begin chunk consumes all remaining data, the emitted
OBJECT_CHUNK begin event's payload is a slice of the input rcbuf.
The data is still parsed from `input_buf` (a copy), but the emitted
chunk references the original transport buffer via offset mapping.
If any of these conditions fail, the begin chunk falls back to copy.

**Relay forwarding**: a relay can receive an OBJECT_CHUNK slice via
`on_data_rcbuf`, then pass the same `chunk` rcbuf directly to
`write_object_data` on a downstream session. The SEND_DATA action's
payload pointer is the same pointer as the received chunk — true
zero-copy end-to-end. The transport buffer's release callback fires
only after both the receive event and all downstream send actions
are cleaned up.

Wrapped rcbufs flow through the same ownership paths as owned rcbufs:
`write_object` / `write_object_data` incref, `poll_actions` transfers,
`moq_action_cleanup` / session destroy decrefs. The release callback
fires exactly once regardless of which path performs the final decref.

## FFI Binding Guidance

Language bindings (Swift, Kotlin, Python, C++) must deep-copy borrowed
metadata at the C bridge boundary before returning control to the host
event loop. The borrow epoch contract means pointers are only valid within
a single C call sequence. Storing a `track_name.data` pointer across an
`await` or a GC pause will produce dangling reads.

The recommended pattern:

1. Poll events in C.
2. Copy `track_namespace`, `track_name`, `reason`, and track properties
   into host-language-managed memory.
3. Handle the `OBJECT_RECEIVED` `payload` / `properties` rcbufs — **the event
   owns these references and `moq_event_cleanup` decrefs them** (see below).
4. Call `moq_event_cleanup` exactly once before returning.
5. The next advancing call invalidates all borrowed pointers.

### Owning the OBJECT_RECEIVED payload / properties rcbufs

The event owns one reference to each of `payload` and `properties`;
`moq_event_cleanup` decrefs that reference and sets the field NULL. Pick
**one** of these and never mix them:

- **Copy the bytes (recommended for FFI).** Read from `moq_rcbuf_data(rc)` /
  `moq_rcbuf_len(rc)` into host-owned memory, then call `moq_event_cleanup`.
  Do **not** call `moq_rcbuf_decref` on the event's rcbufs yourself — cleanup
  does that. (Manually decref'ing *and* calling cleanup double-decrefs.)

- **Keep the rcbuf pointer past cleanup.** Call `moq_rcbuf_incref(rc)` first to
  take the wrapper's *own* reference, then call `moq_event_cleanup` (which drops
  the event's original reference). Your wrapper later calls `moq_rcbuf_decref`
  on its reference. Without the incref, the pointer dangles the moment cleanup
  runs.

- **Take over the event's original reference without incref (discouraged for
  FFI).** Set the event field to NULL *before* `moq_event_cleanup` so cleanup
  does not decref the reference you now own (`moq_rcbuf_decref(NULL)` is a
  no-op). This hands the single existing reference to your wrapper. It is easy
  to get wrong — a missed NULL frees the buffer out from under you — so prefer
  the incref pattern above.

The same-shard rule still applies: `moq_rcbuf_incref` takes another reference
on the **same** shard. It is *not* a cross-thread / cross-shard ownership
transfer. To move a payload to another shard, deep-copy the bytes (see
"Crossing a shard boundary requires an explicit copy" under *Refcounted
Buffers* above).
