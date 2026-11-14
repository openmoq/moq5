## moq/sim.h

```text
Deterministic simulation helpers for libmoq.

This API is intentionally separate from moq/moq.h. Production embedders
can link only moq-core; tests, conformance harnesses, and tools can link
moq-sim for reproducible back-to-back sessions.
```
