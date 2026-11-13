# Temporary Picoquic Development Patches

Owner: LibMoQ maintainers. Base:
[`36f49b73d532369882d97474ac018655b6739a58`](https://github.com/private-octopus/picoquic/commit/36f49b73d532369882d97474ac018655b6739a58),
released as `v2026.11.12.36f49b7`. Apply the ordered `series` through
`scripts/provider_patches.sh` or `scripts/setup_picoquic_deps.sh`. Picotls keeps
its separate pin and series.

These are temporary development requirements, not an unmodified upstream
release suitable for claiming system-package compatibility. Review each
residual at every dependency update. Remove it only when the selected upstream
release provides equivalent behavior and the existing regression coverage
qualifies the replacement; do not merely define a contract macro or disable
the feature it protects. Source applicability alone is not runtime qualification.

## Superseded Behavior

- [Upstream PR2295](https://github.com/private-octopus/picoquic/pull/2295)
  supplies zshenker's dual-codepoint RESET_STREAM_AT support. No downstream
  patch implements it. Upstream `transport_param`, `transport_param_default`
  and `transport_param_log` tests cover the released implementation.
- [Upstream PR2279](https://github.com/private-octopus/picoquic/pull/2279)
  supplies the correct sent-offset ACK check for reset after FIN and the
  classification of absent retired reset IDs. The corresponding replacements
  were removed from 0003/0008. The existing stronger refusal/context contract
  and its tests remain.
- Upstream's suppression of a reset callback after delivered FIN is retained.
  The reset-after-FIN wire regression already permits that behavior while
  checking the received reset's final size and error code.
- [Upstream PR2369](https://github.com/private-octopus/picoquic/pull/2369)
  (in the selected release) keeps an ACK_FREQUENCY request at or above the
  peer's min_ack_delay. No downstream patch implements it; upstream's
  `ackfrq_min_delay` test covers it.
- Upstream 6152f9e6 replaces a NULL connection callback with one that refuses
  every event, so in-order data is no longer queued for a connection without
  a callback. The 0003/0010 direct-receive tests queue data behind a missing
  first byte instead; their assertions are unchanged.

## Remaining Removal Conditions

The following requirements are not all supplied by the selected release.
Each patch's preamble names its focused regression tests. The complete series
still requires native provider/adapter and installed-consumer qualification
after rebasing; no new test result is asserted by this inventory.

| Patch | Required replacement before removal |
| --- | --- |
| 0001 | Package metadata uses the declared project version instead of a git hash version. |
| 0002 | Link inputs preserve the required closure without duplicate libraries. |
| 0003 | Reliable-prefix retirement/delivery, size validation, empty ACK handling, receive credit, direction checks and direct-receive lifetime remain correct; terminal reset refusal preserves application context and repeated reset is idempotent. |
| 0004 | Side-effect-free stateless-reset lookup and reset-secret lifecycle preserve lane routing. |
| 0005 | Accepted close reasons are owned through connection deletion callbacks. |
| 0006 | Exact STOP errors and opt-in local UNI byte+FIN commitment, including HTTP forwarding and reset precedence, preserve `PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION=1`. |
| 0007 | Public-random state is isolated between provider threads. |
| 0008 | Range/direction/terminal classification for reset and STOP preserves the no-mutation `PICOQUIC_TERMINAL_STREAM_ALREADY_CLOSED=1` contract. |
| 0009 | Stateless-reset ingress and sending policy preserve authentication, routing and opt-out behavior. |
| 0010 | STOP refuses a direct-receive stream whose receive half has completed, without changing its context. |
| 0011 | Loading, unloading and resetting the crypto providers are serialized, so contexts created concurrently on different threads each see the providers completely loaded. |

No new provider API is introduced by this rebase. External provider headers and
binaries must still implement the existing declared semantic contracts together.
