"""Installed SDK contract, with external consumers and no transport traffic."""
import argparse
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--build", type=Path, required=True)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--libdir", required=True)
    p.add_argument("--includedir", required=True)
    p.add_argument("--cmake", required=True)
    p.add_argument("--cc", required=True)
    p.add_argument("--cxx", required=True)
    p.add_argument("--runtime", choices=("0", "1"), required=True)
    p.add_argument("--shared", choices=("0", "1"), required=True)
    p.add_argument("--managed", choices=("0", "1"), default="0")
    p.add_argument("--msquic-dir", default="")
    args = p.parse_args()
    args.source = args.source.resolve()
    args.build = args.build.resolve()
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(("DYLD_", "LD_", "CMAKE_", "PKG_CONFIG_")):
            env.pop(key)
    for key in ("CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "LIBRARY_PATH", "DESTDIR"):
        env.pop(key, None)

    def run(argv, *, ok=True):
        result = subprocess.run([str(x) for x in argv], env=env,
                                capture_output=True, text=True, timeout=120)
        if ok:
            require(result.returncode == 0,
                    f"{argv}\n{result.stdout}\n{result.stderr}")
        return result

    with tempfile.TemporaryDirectory(prefix="relay SDK ") as name:
        work = Path(name)
        prefix = work / "installed"
        run([args.cmake, "--install", args.build, "--prefix", prefix])
        relocated = work / "relocated prefix"
        prefix.rename(relocated)
        inc = relocated / args.includedir
        lib = relocated / args.libdir
        pkg = lib / "cmake/libmoq"
        expected_headers = {"export.h", "types.h", "auth.h", "capacity.h", "log.h",
                            "placement.h", "relay.h", "trace.h", "wire_codes.h"}
        components = ["relay-core"]
        if args.runtime == "1":
            expected_headers.update(("moqr_bind.h", "moqr_shards.h", "moqr_obs.h"))
            components.append("relay")
        require({f.name for f in (inc / "moq/relay").iterdir()} == expected_headers,
                "installed header set differs from public API")
        for header in (inc / "moq/relay").iterdir():
            text = header.read_text()
            require(not re.search(r"MOQR_\w*TESTING|moqr_\w*debug_", text),
                    f"test API in {header}")
            # Each header must stand alone, including in C++.
            for cc, lang in ((args.cc, "c"), (args.cxx, "c++")):
                unit = work / "header.c"
                unit.write_text(f"#include <moq/relay/{header.name}>\nint header_probe;\n")
                run([cc, "-x", lang, "-std=c11" if lang == "c" else "-std=c++11",
                     "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-fsyntax-only",
                     "-I", inc, unit])
        for f in relocated.rglob("*"):
            require(not any(x in f.name for x in
                            ("test-internals", "relay-policy", "relay-inspect", "auth_toy")),
                    f"private library or header installed: {f}")
        for f in pkg.glob("libmoqRelay*.cmake"):
            require(str(args.source) not in f.read_text() and
                    str(args.build) not in f.read_text(), f"unrelocatable export: {f}")

        consumer = work / "consumer"
        shutil.copytree(args.source / "tests/consumer", consumer)
        # Base discovery must remain lazy; repeated component finds must compose.
        (consumer / "finds.cmake").write_text(
            "find_package(libmoq CONFIG REQUIRED)\n"
            "if(TARGET moq::relay OR TARGET moq::relay-core)\n"
            '  message(FATAL_ERROR "base package eagerly imports relay")\n'
            "endif()\nfind_package(libmoq CONFIG REQUIRED COMPONENTS relay-core)\n")
        cmake_source = consumer / "CMakeLists.txt"
        cmake_source.write_text(cmake_source.read_text().replace(
            "set(RELAY_COMPONENT", 'include("${CMAKE_CURRENT_LIST_DIR}/finds.cmake")\nset(RELAY_COMPONENT', 1))
        for component in components:
            build = work / (component + " build")
            run([args.cmake, "-S", consumer, "-B", build, f"-Dlibmoq_DIR={pkg}",
                 f"-DRELAY_COMPONENT={component}", f"-DCMAKE_C_COMPILER={args.cc}",
                 f"-DCMAKE_CXX_COMPILER={args.cxx}",
                 "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF",
                 "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF"])
            run([args.cmake, "--build", build, "-j2"])
            for lang in ("c", "cxx"):
                run([build / ("consumer_" + lang)])

            binary = lib / ("libmoq-" + component +
                            (".a" if args.shared == "0" else
                             ".dylib" if sys.platform == "darwin" else ".so"))
            require(binary.is_file(), f"missing library: {binary}")
            command = ["nm", "-gU"] if sys.platform == "darwin" else ["nm", "-g", "--defined-only"]
            nm = run(command + [binary]).stdout
            symbols = {line.split()[-1].lstrip("_") for line in nm.splitlines()
                       if line.split() and line.split()[-1].lstrip("_").startswith("moqr_")}
            expected = set((args.source / "abi" / (component + ".symbols")).read_text().splitlines())
            require(expected <= symbols, f"missing public symbols: {expected - symbols}")
            if args.shared == "1":
                require(symbols == expected, f"extra shared exports: {symbols - expected}")
            require(not any("_debug" in s or "_test_" in s for s in symbols),
                    f"test symbols in production {binary}")

        # The test removes only its own installed metadata to prove refusal.
        exports = list(pkg.glob("libmoqRelayTargets*.cmake"))
        saved = [(f, f.read_bytes()) for f in exports]
        for f, _ in saved:
            f.unlink()
        missing = run([args.cmake, "-S", consumer, "-B", work / "missing runtime",
                       f"-Dlibmoq_DIR={pkg}", "-DRELAY_COMPONENT=relay"], ok=False)
        require(missing.returncode != 0 and "relay" in missing.stderr,
                "missing runtime was accepted")
        for f, data in saved:
            f.write_bytes(data)

        pkgconfig = shutil.which("pkg-config")
        require(pkgconfig is not None, "pkg-config required for SDK contract test")
        env["PKG_CONFIG_LIBDIR"] = str(lib / "pkgconfig")
        env["PKG_CONFIG_PATH"] = ""
        for component in components:
            for static in (False, True):
                flags = shlex.split(run([pkgconfig] + (["--static"] if static else []) +
                                        ["--cflags", "--libs", "libmoq-" + component]).stdout)
                libraries = [f for f in flags if f.startswith("-l")]
                required = ["-lmoq-relay-core", "-lmoq-core"]
                if component == "relay":
                    required.insert(0, "-lmoq-relay")
                require([f for f in libraries if f != "-lpthread"] == required,
                        f"nonminimal pkg-config closure: {libraries}")
                exe = work / f"pc-{component}-{static}"
                run([args.cc, "-std=c11", "-Wall", "-Wextra", "-Wpedantic", "-Werror",
                     *(["-DTEST_RUNTIME"] if component == "relay" else []),
                     consumer / "main.c", "-o", exe, *flags,
                     "-Wl,-rpath," + str(lib)])
                run([exe])
        examples = list(relocated.rglob("in_memory.c"))
        require(len(examples) == 1, "installed deterministic example missing or duplicated")
        flags = shlex.split(run([pkgconfig, "--static", "--cflags", "--libs",
                                "libmoq-relay-core"]).stdout)
        exe = work / "installed-example"
        run([args.cc, "-std=c11", "-Wall", "-Wextra", "-Wpedantic", "-Werror",
             examples[0], "-o", exe, *flags, "-Wl,-rpath," + str(lib)])
        run([exe])
        if args.runtime == "1":
            sources = list(relocated.rglob("simple-relay/main.c"))
            require(len(sources) == 1, "installed network example missing or duplicated")
            if args.managed == "1":
                # Copy only the installed files: no access to private source headers.
                source = work / "network consumer"
                shutil.copytree(sources[0].parent, source)
                build = work / "network build"
                run([args.cmake, "-S", source, "-B", build,
                     f"-Dlibmoq_DIR={pkg}", f"-Dmsquic_DIR={args.msquic_dir}",
                     f"-DCMAKE_C_COMPILER={args.cc}", "-DMOQ_WARNINGS_AS_ERRORS=ON",
                     "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF",
                     "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF"])
                run([args.cmake, "--build", build, "-j2"])
                help_result = run([build / "moq_simple_relay", "--help"])
                require(help_result.stdout.startswith("Usage: moq_simple_relay"),
                        "installed example help missing")
        print("PASS: relocated SDK C/C++, standalone headers, symbols, lazy/missing components, pkg-config")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
