#!/usr/bin/env python3
"""Compile the pinned upstream WT callbacks and independent negative controls.

Use a GCC compiler, the upstream setup source, its configured build directory,
and the selected provider's include directory. No source tree is modified.
"""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cc", required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--msquic-include", type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    backend = source / "backends/msquic"
    flags = [args.cc, "-std=c11", "-Wall", "-Wextra", "-Wpedantic", "-Werror",
             "-DWTQ_BUILDING", "-DWTQ_STATIC", "-fsyntax-only"]
    for directory in (source / "include", args.build.resolve() / "include",
                      source / "src", backend):
        flags += ["-I", str(directory)]
    flags += ["-isystem", str(args.msquic_include.resolve())]
    cases = (
        ("msq_conn.c", "wtq_msq_stream_cb_ptr(\n                                         wtq_msq_stream_callback)",
         "(void *)wtq_msq_stream_callback"),
        ("msq_listener.c", "wtq_msq_conn_cb_ptr(\n                                        wtq_msq_conn_callback)",
         "(void *)wtq_msq_conn_callback"),
    )
    with tempfile.TemporaryDirectory(prefix="wtq-callback-proof-") as work:
        work = Path(work)

        def compile_case(name, text, diagnostic=None):
            path = work / name
            path.write_text(text)
            result = subprocess.run(flags + [str(path)], capture_output=True,
                                    text=True, timeout=60,
                                    env=dict(os.environ, LC_ALL="C"))
            if diagnostic is None:
                ok = result.returncode == 0 and not result.stderr and not result.stdout
            else:
                ok = (result.returncode != 0 and diagnostic in result.stderr
                      and str(path) in result.stderr)
            if not ok:
                raise RuntimeError(f"{name}: unexpected compiler result "
                                   f"{result.returncode}\n{result.stdout}{result.stderr}")
            print(f"PASS {name}")

        for filename, corrected, original in cases:
            text = (backend / filename).read_text()
            if text.count(corrected) != 1:
                raise RuntimeError(f"{filename}: expected exactly one corrected call")
            compile_case(filename, text)
            compile_case("old_" + filename, text.replace(corrected, original),
                         "ISO C forbids conversion of function pointer to object pointer")

        compile_case("typed.c", '#include "msq_internal.h"\n'
                     'void *c(QUIC_CONNECTION_CALLBACK_HANDLER h) '
                     '{ return wtq_msq_conn_cb_ptr(h); }\n'
                     'void *s(QUIC_STREAM_CALLBACK_HANDLER h) '
                     '{ return wtq_msq_stream_cb_ptr(h); }\n')
        for helper, wrong_type in (
            ("wtq_msq_conn_cb_ptr", "QUIC_STREAM_CALLBACK_HANDLER"),
            ("wtq_msq_stream_cb_ptr", "QUIC_CONNECTION_CALLBACK_HANDLER"),
        ):
            compile_case(helper + "_wrong.c", '#include "msq_internal.h"\n'
                         f'void *test({wrong_type} h) {{ return {helper}(h); }}\n',
                         "incompatible")
    print("7/7 callback compile controls passed")


if __name__ == "__main__":
    main()
