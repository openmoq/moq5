"""Observe one verified connection using the installed moq5 foundation."""

from __future__ import annotations

import argparse
import sys
import time
from enum import IntEnum

from moq5 import (
    Endpoint,
    EndpointConfig,
    EndpointState,
    MoqError,
    TerminalReason,
    WaitResult,
    build_info,
)


def _timeout_us(value: str) -> int:
    try:
        result = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "timeout must be integer microseconds"
        ) from error
    if not 0 <= result <= (1 << 63) - 1:
        raise argparse.ArgumentTypeError("timeout must be in 0..2**63-1 microseconds")
    return result


def _label(value: int) -> str:
    return value.name if isinstance(value, IntEnum) else str(value)


def _report_terminal(endpoint: Endpoint) -> int:
    terminal = endpoint.terminal
    print(
        f"terminal={_label(terminal.reason)} "
        f"detail_code=0x{terminal.detail_code:016x}"
    )
    return 0 if terminal.reason == TerminalReason.CLEAN else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("--ca-file", help="PEM trust roots for a supporting backend")
    parser.add_argument(
        "--sni", help="TLS server-name override for a supporting backend"
    )
    parser.add_argument("--wt-path", help="WebTransport path override")
    parser.add_argument("--timeout-us", type=_timeout_us, default=10_000_000)
    args = parser.parse_args()
    try:
        config = EndpointConfig(
            url=args.url, ca_file=args.ca_file, sni=args.sni, wt_path=args.wt_path
        )
        print(build_info())
        deadline_ns = time.monotonic_ns() + args.timeout_us * 1_000
        with Endpoint.connect(config) as endpoint:
            previous_state: int | None = None
            while True:
                state = endpoint.state
                if state != previous_state:
                    print(f"state={_label(state)}")
                    previous_state = state
                if state == EndpointState.CLOSED:
                    return _report_terminal(endpoint)
                if state == EndpointState.ESTABLISHED:
                    print(f"negotiated_version={endpoint.negotiated_version}")
                    return 0
                remaining_ns = deadline_ns - time.monotonic_ns()
                if remaining_ns <= 0:
                    print("connection observation timed out", file=sys.stderr)
                    return 1
                # A fixed deadline prevents activity from extending the budget.
                timeout_us = min((remaining_ns + 999) // 1_000, 250_000)
                result = endpoint.wait(timeout_us)
                if result == WaitResult.CLOSED:
                    return _report_terminal(endpoint)
                if result == WaitResult.INTERRUPTED:
                    print("connection wait interrupted", file=sys.stderr)
                    return 1
    except MoqError as error:
        print(error, file=sys.stderr)
        return 1
    except (TypeError, ValueError) as error:
        print(f"invalid configuration: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
