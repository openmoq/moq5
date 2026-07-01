# Sans-I/O Architecture Demo

**Purpose:** Single-file demonstration of everything that makes libmoq's
architecture distinctive. This is the file to hand to another MoQ
implementer (Rust/Go/TypeScript) to show what the C library feels like.

**What it demonstrates:**

1. **SimPair pattern** - two sessions wired back-to-back with zero
   sockets, no async runtime, pure synchronous C with virtual time
2. **Protocol-violation error injection** - unknown message type triggers
   clean deterministic close with structured error info
3. **Borrow-epoch validation** - detecting use-after-invalidation through
   an explicit runtime validation hook for borrowed event/action data
4. **Zero-leak proof** - counting allocator verifies alloc/free balance
   after destroy
5. **Virtual time** - every advancing call takes `uint64_t now_us`, no
   system clock is ever read
6. **SimPair automation** - `moq_simpair_run_until_quiescent` drives the
   same setup exchange without hand-rolled pump code
7. **API safety** - NULL guards, wrong-state rejection, safe destroy

**Compare with:**
- Vista (Rust) - `SimPair`, typestate, `SessionInput`/`SessionAction`
- moqt-go - zero-goroutine, `backendtest.RunContract()`
- Playa (TypeScript) - `.sim.test.ts`, discriminated union events

**Status:** Uses the real `moq_session_*` API. Can be built as a CMake
target.
