# publication_owner

Private test-tool support for
`bindings/kotlin/tools/red-publication-source-binding.sh`. Not an installed
library API, not an Android runtime dependency, and not a second owner protocol
— it packages the reviewed owner and wire mechanisms so the publication driver
runs against a real *retained* owner connection.

The connection is the authority. There is no socket pathname, no listening
directory and no discovery: the launcher creates a `socketpair`, keeps its end
for its whole lifetime, and hands one consumer at a time an explicit alias of
that end as an inherited descriptor. A consumer either inherited the channel or
has none.

## Files

| file | role |
| --- | --- |
| `owner_wire.py` | length-prefixed framing (`<len> <payload>`), non-blocking with `select` deadlines for both directions; an idle server wait is unbounded, a frame in flight is not |
| `owner_endpoint.py` | the owner: forks each worker, keeps it unreaped, settles by `kill`+`waitpid` through its own slot, and serves request/reply on an **inherited** descriptor |
| `owner_launcher.py` | the launcher: creates the channel, retains its end, hands out one alias at a time, and accounts for each consumer before recovering it |
| `owner_call.py` | the short-lived client the shell driver calls over the descriptor it inherited |

## Entry points

```sh
# the endpoint is never started by pathname; it is given a descriptor
python3 owner_endpoint.py <channel-fd> <generation>

# the driver, the capsule and the coordinator call this with the alias
# inherited at the descriptor named by MOQ5_PUBBIND_OWNER_FD
python3 owner_call.py create   <scope> <key>
python3 owner_call.py settle   <generation> <scope> <ref>
python3 owner_call.py state    <ref>
python3 owner_call.py snapshot
```

`create` prints `generation scope ref child` only after validating the complete
reply, including the worker's own acknowledgment. `settle` prints the
authoritative outcome word after validating the ref it names. Every refusal
prints `refused: <code>: <reason>` and exits non-zero, so a missing owner can
never read as a settled child.

In Python, `owner_launcher.RetainedOwner` is the launcher:

```python
owner = RetainedOwner("g1").start()
rc, out, err = owner.run_consumer(argv)   # the alias is inherited at fd 9
owner.release()                           # bounded shutdown, then the reap
```

## Contracts held here

- identity is the full `(generation, scope, ref)` tuple; the ref is an opaque
  owner-minted token that embeds neither generation, scope, pid nor the caller
  key, and is looked up byte-for-byte;
- admission is checked before the fork, against `N_live=8` / `N_total=256`;
- refs are never reused within a generation, and tombstones live for it;
- settlement is exactly once, and the owner retains the child unreaped until it
  consumes the wait status;
- a worker closes the launcher channel and every sibling lifetime writer before
  it runs, and reports readiness on its own acknowledgment pipe;
- one consumer holds the alias at a time, so the endpoint never has two
  request/reply readers. A consumer that cannot be stopped and accounted for
  leaves the authority *unresolved*: the alias is not handed out again, and the
  launcher will not speak on the channel itself.
