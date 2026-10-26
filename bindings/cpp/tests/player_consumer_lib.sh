# Path and configuration rules of the installed C++ player consumer test
# (run_player_install_consumer.sh), as pure shell functions so their
# controls (test_player_consumer_lib.sh) can exercise them without a build.
# Sourced; defines functions only.

# The picotls tree for a child picoquic build, from the PARENT's resolved
# selection: FindPicoquic resolves both layouts it accepts -- an installed
# prefix (include/, lib/) and a checkout build directory (headers in
# ../include) -- to an include directory <tree>/include, so the tree is that
# directory's parent in both. Never derived from MOQ_PICOTLS_PREFIX itself.
pc_ptls_tree() {   # <resolved PTLS_INCLUDE_DIR>
    (cd "$1/.." && pwd -P)
}

# Arguments that select one configuration for configure, build and install:
# a single-config generator takes CMAKE_BUILD_TYPE; every step names
# --config, which single-config generators ignore.
pc_configure_config_args() {   # <config> <multi-config: 0|1>
    if [ "$2" = 0 ] && [ -n "$1" ]; then
        printf '%s\n' "-DCMAKE_BUILD_TYPE=$1"
    fi
}
pc_step_config_args() {   # <config>
    if [ -n "$1" ]; then printf '%s\n' --config "$1"; fi
}

# The built executable: <dir>/<config>/<name> under a multi-config
# generator, <dir>/<name> otherwise. Prints nothing when it is absent.
pc_find_exe() {   # <build dir> <name> <config>
    if [ -n "$3" ] && [ -x "$1/$3/$2" ]; then printf '%s\n' "$1/$3/$2"
    elif [ -x "$1/$2" ]; then printf '%s\n' "$1/$2"
    fi
}

pc_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# The canonical path of <path>, every symlink resolved -- the final file
# component and any chain included -- through CMake's real-path operation
# (portable; no GNU realpath). Prints nothing when it does not resolve to an
# existing file. PC_CMAKE names the cmake to use (default: cmake on PATH).
pc_realpath() {   # <path>
    "${PC_CMAKE:-cmake}" -DIN="$1" -P "$pc_dir/real_path.cmake" 2>&1
}

# Whether <path> resolves, fully, inside <prefix> (itself resolved). Fails
# closed: a path or prefix that does not resolve is not inside.
pc_under_prefix() {   # <path> <prefix>
    local p r
    p=$(pc_realpath "$1")
    r=$(pc_realpath "$2")
    [ -n "$p" ] && [ -n "$r" ] || return 1
    case "$p" in "$r"/*) return 0 ;; *) return 1 ;; esac
}

# The value <file> sets for <var>, from a preload written as
# set(<var> [==[<value>]==] CACHE ...) (the consumer context form), or from a
# CMakeCache.txt line <var>:<TYPE>=<value>. Prints nothing when absent.
pc_setting() {   # <file> <var>
    local line
    line=$(grep -E "^set\($2 \[=+\[" "$1" 2>/dev/null | tail -1) || true
    if [ -n "$line" ]; then
        # The bracket's "=" run, then the text between the matching delimiters
        # (no back-reference: BSD sed has none in patterns).
        local eqs value
        eqs=$(printf '%s\n' "$line" | sed -E 's/^set\([^ ]+ \[(=+)\[.*$/\1/')
        value=${line#*"[$eqs["}
        value=${value%%"]$eqs]"*}
        printf '%s\n' "$value"
        return
    fi
    # Absent is not an error: the runner sources this under pipefail.
    { grep -E "^$2:[A-Z]+=" "$1" 2>/dev/null || true; } | tail -1 | sed -E 's/^[^=]*=//'
}

# Whether a configured child's cache selected exactly <want> for <var>; an
# empty <want> means the parent selected nothing, so the child must not have
# found one either (unset, or *-NOTFOUND).
pc_selected() {   # <child CMakeCache.txt> <var> <want>
    local got
    got=$(pc_setting "$1" "$2")
    if [ -z "$3" ]; then
        [ -z "$got" ] || [[ "$got" == *-NOTFOUND ]]
    else
        [ "$got" = "$3" ]
    fi
}
