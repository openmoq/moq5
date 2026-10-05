# Publisher Retained Catalog Groups

This document describes libmoq's publisher-side retained group support.
It is an implementation facility for answering an explicit FETCH for the
retained group from an origin-local object cache. Both a Joining
FETCH(offset 0) and a bounded standalone FETCH (matched by explicit
namespace/name) are served, provided the FETCH range covers the whole
retained group and — for a standalone FETCH — the request is authorized
(see the authorization rule under "FETCH Matching And Range Rules"). It is
not a protocol feature named by the MoQT or MSF drafts.

## Spec Boundary

MSF-01 requires catalog subscribers to use `SUBSCRIBE` with a Joining
FETCH offset of 0 to obtain the latest complete catalog and any following
delta objects. A catalog group is shaped as:

- object 0: an independent complete catalog
- objects 1..N: `deltaUpdate` objects that apply to object 0

MoQT defines Joining FETCH and FETCH delivery from available objects or
cache. It does not define a "retained group" object, and it does not
require relays to cache every object. A relay is allowed to be
catalog-blind: a catalog object is just another object unless the relay
chooses otherwise.

In libmoq, a retained group is the publisher facade's origin-local cache
entry for one track's latest dense object group. The cache lets the
origin answer an explicit FETCH for objects 0..N — a Joining FETCH(offset
0), or an authorized standalone FETCH that names the track and whose range
covers the whole group (the shape a relay such as moqx emits when it pulls a
catalog on behalf of a downstream subscriber). It does not solve the
late-join-through-relay problem when the relay has already evicted the
catalog and the origin never sees the downstream late joiner.

## What Retained Groups Do

`moq_pub_set_retained_group()` stores a bounded copy of one complete
group for a publisher track:

- all retained objects share one group id
- object ids must be dense: `0, 1, ... N`
- object 0 is the independent base
- objects 1..N are subsequent objects in that same group
- payload and properties buffers are retained with `moq_rcbuf_incref`
- the previous retained group is replaced atomically on success

For an MSF catalog track, this is the latest catalog generation. For
example, a generation with one remove delta is retained as:

```text
group 7
  object 0: prior independent catalog
  object 1: deltaUpdate remove(...)
```

The retained group is also used to advertise a Largest Location for the
track when it is safe to do so. That lets a subscriber issue a Joining
FETCH whose computed range covers the retained objects.

## What Retained Groups Do Not Do

Setting a retained group is not the same thing as publishing the group to
currently attached subscribers.

To update live subscribers, write the objects normally with
`moq_pub_write_object_ex()`. To make the same generation available for
future Joining FETCH requests, also install it with
`moq_pub_set_retained_group()`.

Do not rely on plain `SUBSCRIBE` to replay retained objects. MSF catalog
consumers should use `SUBSCRIBE` plus Joining FETCH offset 0 — that is what
MSF-01 §5 requires, literally, and a `PUBLISH`-initiated subscription does
not substitute for it. A Joining FETCH keyed on a publication
(`joining_pub`) is a separate transport capability that draft-18 §5.1 and
§10.12.2 require independently; it reaches the same retained group, but it
is not a way to satisfy MSF-01 §5. Any
origin-local compatibility behavior that replays retained objects to a
plain direct subscriber is not a relay-safe protocol guarantee.

Do not describe this as automatic replay behavior. That framing does
not appear in the drafts and has historically blurred two different
operations:

- live publication to active subscribers
- cache population for future FETCH requests

## API

```c
#define MOQ_PUB_RETAINED_MAX_OBJECTS 64u

typedef struct moq_pub_retained_object {
    uint64_t     object_id;
    moq_rcbuf_t *payload;
    moq_rcbuf_t *properties;
    bool         end_of_group;
} moq_pub_retained_object_t;

typedef struct moq_pub_retained_group_cfg {
    uint32_t struct_size;
    uint64_t group_id;
    const moq_pub_retained_object_t *objects;
    size_t object_count;
} moq_pub_retained_group_cfg_t;

void moq_pub_retained_group_cfg_init(moq_pub_retained_group_cfg_t *cfg);

moq_result_t moq_pub_set_retained_group(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    const moq_pub_retained_group_cfg_t *cfg);
```

The call succeeds only if:

- `pub`, `track`, and `cfg` are valid
- `track` belongs to `pub`
- `object_count` is at least 1 and at most `MOQ_PUB_RETAINED_MAX_OBJECTS`
- every payload is non-NULL
- `objects[i].object_id == i` for every object
- total payload plus properties bytes fits the track's retained-byte budget
- the track is still joinable

On success, libmoq owns one reference to each payload and properties
buffer until the group is replaced, cleared by future API changes, or the
track/publisher is destroyed.

## Usage Pattern

For a catalog generation, write the generation to current subscribers and
then retain the exact same object set for Joining FETCH:

```c
moq_pub_retained_object_t retained[2] = {
    {
        .object_id = 0,
        .payload = independent_catalog,
        .properties = NULL,
        .end_of_group = true,
    },
    {
        .object_id = 1,
        .payload = delta_update,
        .properties = NULL,
        .end_of_group = true,
    },
};

for (size_t i = 0; i < 2; i++) {
    moq_pub_object_cfg_t obj;
    moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
    obj.group_id = catalog_group_id;
    obj.object_id = retained[i].object_id;
    obj.payload = retained[i].payload;
    obj.properties = retained[i].properties;
    obj.end_of_group = retained[i].end_of_group;

    moq_result_t rc = moq_pub_write_object_ex(pub, catalog_track, &obj, now_us);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) {
        /* handle fatal publish error */
    }
}

moq_pub_retained_group_cfg_t rg;
moq_pub_retained_group_cfg_init(&rg);
rg.group_id = catalog_group_id;
rg.objects = retained;
rg.object_count = 2;

moq_result_t rc = moq_pub_set_retained_group(pub, catalog_track, &rg);
if (rc != MOQ_OK) {
    /* retain failed; current subscribers may still have received the live write */
}
```

Service-level code normally should not duplicate this sequence. The media
sender already stages catalog generations, writes them to active catalog
subscribers, and installs the same dense group for Joining FETCH.

## FETCH Matching And Range Rules

An inbound FETCH is answered from the retained group when:

- **Joining FETCH** — the track is resolved by the FETCH's joining
  subscription. That subscription may have been initiated either way: by a
  `SUBSCRIBE`, in which case the requester names it with
  `moq_fetch_cfg_t::joining_sub`, or by a `PUBLISH`, in which case it names
  the publication with `joining_pub`. Exactly one of the two is set; both or
  neither is `MOQ_ERR_INVAL`. Reaching `joining_pub` requires
  `moq_fetch_cfg_init_sized()` — the pointer-only `moq_fetch_cfg_init()`
  stops at the frozen v0 layout, so under it the field reads as absent.

  A publication join is supported on **both** draft profiles, under each
  profile's own eligibility rule:

  | | draft-18 | draft-16 |
  |---|---|---|
  | eligibility | Established with Forward State 1 (§10.12.2) | Largest Object subscription filter negotiated at acceptance (§9.16.2) |
  | saved location | Largest from the publish request, or from the acknowledgement of an update raising Forward 0→1 (§5.1) | establishment-time Largest only (§5.1); no later update moves it |
  | ineligible, received | request error `INVALID_RANGE` | protocol violation — the session closes, as §9.16.2 requires |
  | before establishment | request error `INVALID_JOINING_REQUEST_ID`, not buffered | same |

  OR
- **Standalone FETCH** — the track is resolved by the FETCH's explicit
  namespace + track name (the shape a relay emits to pull a catalog),

AND the resolved track has a retained group AND the requested range covers
object 0 through the last retained object. The range check is identical for
both forms: the whole dense group must be inside `[start, end)`.

A **standalone** FETCH must additionally be authorized (a Joining FETCH is
not, since its joining subscription is itself proof of an accepted
subscription — for a `joining_pub` join, of a `PUBLISH` this endpoint sent
and the peer accepted). Because a standalone FETCH names the track directly, it is
served only when:

- the publisher's `accept_mode` is `MOQ_PUB_ACCEPT_ALL`, OR
- the track already has an accepted subscription on this session (e.g. a
  relay accepted a `SUBSCRIBE` via callback, then pulls the retained catalog
  with a standalone FETCH).

Otherwise the standalone FETCH is rejected `UNAUTHORIZED`. Under a protected
policy (`REJECT_ALL` or callback without an accepted subscription) this
rejection is returned **whether or not the track exists**, so a protected
track's existence is not leaked through `DOES_NOT_EXIST`. The authorization
decision consults only publisher state — it never invokes the `on_subscribe`
callback.

These FETCHes are rejected:

- standalone FETCH without authorization (see above) → `UNAUTHORIZED`
- range omits object 0 or any retained object → `NOT_SUPPORTED` (a partial
  range cannot reconstruct the catalog from the independent base + deltas)
- unknown / ended / non-retained track (authorized requests only) →
  `DOES_NOT_EXIST`
- retained location not encodable as a fetch End Location → `INTERNAL_ERROR`

Standalone support is deliberately narrow: it is still origin-local, still
not relay-cache-safe, still only the latest retained dense group, and still
not a general object store. It serves exactly the retained group, nothing
else.

## Error And Backpressure Notes

`moq_pub_set_retained_group()` is a cache update. It does not send data,
so it does not return `MOQ_ERR_WOULD_BLOCK`. It can return validation,
state, or allocation errors.

FETCH serving from the retained group is performed by `moq_pub_tick()`.
If transport backpressure interrupts the FETCH response, the publisher
keeps retry state and resumes on a later tick. The retained group is
snapshotted for the FETCH, so replacing the retained group while a FETCH
is in progress does not change the in-flight response.

## Relay Limitation

Retained groups are origin-local. They help when the origin receives the
FETCH directly (Joining or standalone, including a relay's upstream pull).
They do not force a relay to cache catalog objects, and they do not make
the origin aware of downstream subscribers hidden behind a relay.

For relay topologies the request has to reach the origin: the libmoq relay
binding forwards an ascending Joining FETCH on a SUBSCRIBE-initiated
subscription as one standalone upstream FETCH against the subscription's
pinned source, collects the complete response within a bounded byte/count
budget, and only then accepts the downstream fetch (see
`relay/docs/architecture.md`). The publisher retained group is the
origin-side storage that answers that upstream request; a relay that does
not forward (or caps the response) still leaves the subscriber without a
catalog, which is why the receiver treats a rejected catalog fetch as a
bounded wait rather than an error.
