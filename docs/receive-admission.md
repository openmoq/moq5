# Receive admission ownership and bounds

`MOQ_TRANSPORT_CAP_HOLD_INPUT` is required for the lifetime of every bridge.
Creation rejects a well-formed endpoint missing it with `MOQ_ERR_UNSUPPORTED`
and NULL output, before allocation or endpoint calls. Malformed arguments or
vtables still return `MOQ_ERR_INVAL`. There is no send-only exemption, and
withdrawing the bit after creation cannot restore lossy admission. This does
not change `MOQ_ERR_WOULD_BLOCK`. On a peer unidirectional data input:

- `MOQ_ERR_INPUT_NOT_CONSUMED` means that none of this call's bytes or FIN
  were consumed. The adapter owns that exact chunk until an accepting redelivery,
  cancellation by peer RESET, or bridge close/fatal/destroy cleanup. A retry
  which is refused again preserves ownership. Natural stream closure and
  provider-handle cleanup do not release a still-refused byte/FIN obligation.
- `MOQ_ERR_WOULD_BLOCK` means that the existing retained-input path applies.
  The adapter must not replay that chunk as new input.
- A cleared pending-admission indication is permission to retry, not a
  reserved receive entry. Another stream may take the available entry first.

An adapter must stop delivering later bytes on a held stream. A standalone
FIN cannot replace a held payload, even after capacity becomes available.
Control-stream routing is independent of data-stream admission.

## Bridge identity bound

Let `B` be the bridge's configured `max_streams`. Every refused stream has an
active bridge mapping before the bridge returns `INPUT_NOT_CONSUMED`.
Service does not retire that mapping merely because admission becomes ready,
or because the refused chunk includes FIN. RESET retires it; an accepting
redelivery discharges the held-input obligation. If no mapping is available,
the bridge takes its existing fatal resource-exhaustion path instead of
allocating an unbounded overflow table.

Consequently, adapters which retain one chunk per refused mapping and stop
input after bridge close/fatal have at most `B` held stream identities. Control,
request and already-admitted streams can make the available number smaller.
Do not substitute the transport's **initial** stream credit for `B`: stream
credit can replenish while application-owned input remains outstanding.

`test_bridge_hold_input` pins the table boundary with FIN-bearing refusals,
48 RESET/replacement cycles, preserved identities across service calls, and
exhaustion of a 16-entry table in both drafts. This is a bridge ownership
test, not a measurement of a transport's receive buffering.

## WT runtime qualification and retained input

WT adapter creation requires a `wt_session` config tail and the public
`wtq_session_receive_contract` query. The frozen original config prefix is
40 bytes on LP64 and 20 on ILP32; an absent or partially present binding
returns `MOQ_ERR_UNSUPPORTED` before allocation. The query certifies only a
corrected provider registration, never merely the legacy pause bit. Its
immutable result remains available on a retained terminal session.

The adapter accepts only FLOW_CONTROLLED and a nonzero callback quantum at
most 65535, with the strengthened bounded-admission registration. Corrected
MsQuic qualifies. Network.framework's stream pause remains DELIVERY_ONLY and
does not qualify. For a valid config its managed constructor returns
`MOQ_ERR_UNSUPPORTED` synchronously, before allocation or backend startup;
the endpoint propagates that refusal without choosing another backend.
This does not make standalone WT Network usage invalid. Old, accounted-only,
or unknown providers are unsupported. Raw attach callers create the adapter
in a bootstrap established callback; managed callsites already have the real
session there. Pre-establishment failure is a bootstrap terminal, not a fatal
state of an adapter that was never created.

Before bridge creation, the adapter reserves 16 payload slots of 65535 bytes
and advertises HOLD_INPUT. Thus the reserved payload is 1048560 bytes, plus
metadata and allocation overhead; this is not total provider memory. A refused
chunk and FIN stay associated with the stream key, even after natural terminal
and provider-handle destruction. Replay attempts at most once per service pass;
another refusal retains ownership. Acceptance, including owned WOULD_BLOCK,
discharges debt. RESET and connection teardown cancel it. Held streams are not
resumed, and callbacks cannot replace the creation-time session binding.

Capacity is reserved before a new WT stream is published to the adapter.
The qualified MsQuic provider uses eight peer uni and seven peer bidi credits,
with at most fifteen peer records including sparse implicit opens and retained
terminal leases. Its sixteen engine entries reserve three peer critical slots
and either one client CONNECT plus twelve WT slots, or seven server request
slots plus six WT slots. WT counts local/peer and uni/bidi streams together.
Full pools defer valid stream admission instead of aborting a seventeenth
stream or allocating overflow records. Request parsers retain their existing
storage; these counts do not bound all provider/OS memory or byte windows.

Retained input keeps the API stream lease, and native stream-ID credit follows
safe lease retirement rather than FIN alone. Ready streams join a bounded FIFO;
recycled slots cannot bypass earlier ready streams. Adapter service calls
`wtq_session_service_stream_admission` after bridge/replay/resume work returns.
Each pass considers a bounded snapshot; nested requests defer publication.
Attach callers must continue servicing their serialized lane when application
work releases capacity, even without another incoming packet. No whole-session
pressure close or unbounded retry loop is used to relieve pool pressure.

## C++ adapter payload storage

mvfst and proxygen copy a refused delivery into adapter-owned payload storage.
They do not retain the transport IOBuf's backing allocation. An IOBuf's visible
`capacity()` is not proof of backing-allocation size: trimming writable tail
can reduce the former without releasing the latter.

For held lengths `L_i`, steady adapter-owned storage comprises the
allocator-rounded payload allocations plus rcbuf/IOBuf ownership records and
map nodes. Replay additionally uses an O(number of held streams) ID snapshot.
The incoming transport allocation and the new copy coexist during copying.
This is **not** a claim of exactly `sum(L_i)` allocated bytes.

The adapter does not impose an independent maximum chunk length. mvfst uses
`read(id, 0)`; proxygen's read API has no maximum-length argument. A numeric
total byte limit therefore also requires a verified provider read-buffer
bound `R`, yielding payload bytes at most `B * R`, plus the overhead above.
Attach callers must account for their provider settings, including receive
window changes and autotuning. Initial window values alone are not a proof
of a permanent maximum. Connection flow-control credit can be returned when
bytes leave the provider, so it does not bound bytes now held by the adapter.

These facts do not qualify arbitrary provider buffering or a delivery-only
pause implementation. Such a pause can stop application callbacks while the
provider continues accepting and buffering peer bytes. That behavior needs
its own bounded-storage contract before it can establish end-to-end bounded
receive admission.
