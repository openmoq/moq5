"""Configure-only isolation/refusal contracts for the optional relay SDK."""
import argparse
from pathlib import Path
import re
import subprocess
import tempfile


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--cmake", required=True)
    parser.add_argument("--cc", default="cc")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="relay-options-") as name:
        root = Path(name)
        modules = root / "modules"
        modules.mkdir()
        (modules / "FindThreads.cmake").write_text(
            'message(FATAL_ERROR "unexpected relay engine thread discovery")\n')
        probe = root / "exports.c"
        probe.write_text("#include <moq/export.h>\n#include <moq/relay/export.h>\n"
                         "MOQ_API int core_api(void);\n"
                         "MOQR_CORE_API int engine_api(void);\n"
                         "MOQR_API int runtime_api(void);\n")
        source = Path(args.source).resolve()
        for flags, expected in [
            ([], ("dllimport", "dllimport", "dllimport")),
            (["MOQ_RELAY_CORE_BUILDING"], ("dllimport", "dllexport", "dllimport")),
            (["MOQ_RELAY_BUILDING"], ("dllimport", "dllimport", "dllexport")),
            (["MOQ_STATIC", "MOQ_RELAY_CORE_STATIC", "MOQ_RELAY_STATIC"], (None, None, None)),
        ]:
            proc = subprocess.run([args.cc, "-E", "-P", "-D_WIN32", "-I", str(source / "core/include"),
                                   "-I", str(source / "relay/include"),
                                   *["-D" + f for f in flags], str(probe)],
                                  capture_output=True, text=True, timeout=30)
            require(proc.returncode == 0, proc.stderr)
            for symbol, decoration in zip(("core", "engine", "runtime"), expected):
                match = re.search(r"([^;\n]*)int " + symbol + r"_api\(void\);", proc.stdout)
                require(match is not None, proc.stdout)
                if decoration is None:
                    require("__declspec" not in match[1], proc.stdout)
                else:
                    require(f"__declspec({decoration})" in match[1], proc.stdout)
        print("PASS: independent DLL export/import and static macro ownership (preprocessor only)")
        common = [args.cmake, "-S", args.source, "-DMOQ_BUILD_TESTS=OFF",
                  "-DMOQ_BUILD_EXAMPLES=OFF", "-DMOQ_BUILD_SIM=OFF",
                  "-DMOQ_BUILD_LOC=OFF", "-DMOQ_BUILD_CMAF=OFF",
                  "-DMOQ_BUILD_CODEC_SIGNALING=OFF", "-DMOQ_BUILD_RELAY=OFF"]
        cases = [
            ("core", ["-DMOQ_BUILD_RELAY_CORE=ON", f"-DCMAKE_MODULE_PATH={modules}"], None),
            ("runtime", ["-DMOQ_BUILD_RELAY_CORE=ON", "-DMOQ_BUILD_RELAY_RUNTIME=ON"], None),
            ("no-core", ["-DMOQ_BUILD_RELAY_RUNTIME=ON"], "requires MOQ_BUILD_RELAY_CORE"),
            ("no-runtime", ["-DMOQ_BUILD_RELAY_CORE=ON", "-DMOQ_BUILD_RELAY_TOOL=ON"],
             "requires MOQ_BUILD_RELAY_RUNTIME"),
            ("base", [], None),
        ]
        for label, flags, refusal in cases:
            proc = subprocess.run(common + ["-B", str(root / label)] + flags,
                                  capture_output=True, text=True, timeout=60)
            output = proc.stdout + proc.stderr
            if refusal is None:
                require(proc.returncode == 0, output)
            else:
                require(proc.returncode != 0 and refusal in output, output)
            print("PASS:", label)
        consumer = root / "consumer"
        consumer.mkdir()
        (consumer / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.20)\nproject(consumer C)\n"
            "find_package(libmoq CONFIG REQUIRED COMPONENTS relay-core)\n")
        proc = subprocess.run([args.cmake, "-S", str(consumer), "-B", str(root / "missing"),
                               f"-Dlibmoq_DIR={root / 'base'}"],
                              capture_output=True, text=True, timeout=60)
        require(proc.returncode != 0 and "relay-core" in proc.stderr, proc.stdout + proc.stderr)
        print("PASS: absent engine component")
        # CMake leaves old export files behind when a component is disabled.
        for label, component, option in (("core", "relay-core", "MOQ_BUILD_RELAY_CORE"),
                                          ("runtime", "relay", "MOQ_BUILD_RELAY_RUNTIME")):
            proc = subprocess.run(common + ["-B", str(root / label), f"-D{option}=OFF"],
                                  capture_output=True, text=True, timeout=60)
            require(proc.returncode == 0, proc.stdout + proc.stderr)
            (consumer / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\nproject(consumer C)\n"
                f"find_package(libmoq CONFIG REQUIRED COMPONENTS {component})\n")
            proc = subprocess.run([args.cmake, "-S", str(consumer), "-B", str(root / ("disabled-" + label)),
                                   f"-Dlibmoq_DIR={root / label}"],
                                  capture_output=True, text=True, timeout=60)
            require(proc.returncode != 0 and component in proc.stderr,
                    "disabled component accepted through stale export files\n" + proc.stdout + proc.stderr)
            print("PASS: disabled", component, "with stale export files")


if __name__ == "__main__":
    main()
