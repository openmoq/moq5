#!/usr/bin/env bash
# The tier graphs of moq5-sys against one installed SDK: each supported
# combination a dependency graph can ask for, and the refusals.
#
#   tier_graphs.sh <sdk-prefix> <work-dir>
#
# Each graph is built with `cargo build -vv --message-format=json`. Both of
# its streams go through the strict diagnostic classifier
# (scripts/diagnostics.sh with the dependency allowlist) before anything
# else is judged, so a successful build that printed a warning (a clang
# warning from the build script, a manifest warning) fails its graph. What
# the graph bound is then read from cargo's own record of moq5-sys's build
# script (its out_dir and linked libraries, also when the run was fresh)
# and the build script's report in that out_dir:
# - core (moq5-sys with no feature, and moq5-core): libmoq-core probed
#   alone, one link library (moq-core);
# - core + sim (moq5-sys `sim`): libmoq-sim, linking moq-sim and moq-core;
# - core + media (moq5-sys `media`, and moq5-media): libmoq-media, linking
#   the five media parsers and moq-core, and nothing of the service;
# - core + sim + media: both packages;
# - core + media + locmaf (moq5-sys `locmaf`, and moq5-media `locmaf`):
#   libmoq-media and libmoq-locmaf, linking the media parsers, moq-locmaf
#   and moq-core;
# - service, service + sim, player and player + sim: libmoq-service (and
#   libmoq-sim);
# - independent crates unified into one graph (moq5-core with moq5-media;
#   moq5-core, moq5-media and moq5; with moq5-sys's `sim` or moq5's player
#   beside them): the union, from one SDK.
# Every positive graph names one pkg-config directory for the whole
# SDK-owned metadata the build checks. A tier the SDK is stated not to
# carry (MOQ5_RUST_SIM=0, MOQ5_RUST_PLAYER=0, MOQ5_RUST_LOCMAF=0, or
# MOQ5_RUST_MPEG2TS=0, without which no libmoq-media.pc is installed) is not
# built; where the SDK lacks its package, its refusal is required instead.
#
# The refusals and their matching controls, on views of the real SDK whose
# pkg-config files are copies still describing its files: a package
# missing from the view (libmoq-core, libmoq-sim, libmoq-media,
# libmoq-locmaf) while
# libmoq.pc resolves; a tier package, the service package, or the
# simulator beside the service resolving from another directory than the
# rest of the SDK-owned metadata (the service's aggregate libmoq.pc
# included); one directory whose packages disagree on their version (the
# service's aggregate libmoq.pc included); and, as the matching controls, a
# view holding all of the SDK's packages, which builds. Every view, the
# matching ones included, is searched with the SDK's external providers
# beside it: copies of the packages of the SDK's pkg-config directory and
# of MOQ5_SDK_PKG_CONFIG_PATH that the SDK does not own (a WTQuic SDK's
# wtquic-msquic.pc), and never an SDK-owned package, so a provider the
# SDK requires still resolves while an SDK-owned package a view leaves out
# stays missing. The same refusals and the matching views (service, and
# service with the simulator) run once more for an SDK whose service
# requires an external provider of its own, configured as
# MOQ5_SDK_PKG_CONFIG_PATH configures one and collected by the same code:
# two configured directories, the first holding the provider (its prefix
# relative to its own directory) beside SDK-owned packages, the second a
# different provider of the same name that would fail if it were taken. The collection must keep the first provider and drop the owned
# packages; and controls without the configured providers fail on the
# provider's requirement alone.
#
# A refusal passes only when the build fails, names its reason (and none
# it must not), and its classifier inventory holds exactly one failing
# occurrence, moq5-sys's build-script failure; the predicate's own controls
# (one occurrence; the same failure twice; another failing identity beside
# it) run first. A control drives one graph with an unknown clang warning
# option: cargo exits 0 and the graph's own assertions hold, and the graph
# must fail on the classifier alone. Accepted upstream warnings stay in
# each inventory, classified.
#
# Environment: MOQ5_RUST_SIM, MOQ5_RUST_MPEG2TS, MOQ5_RUST_PLAYER and
# MOQ5_RUST_LOCMAF (1 or 0: what the SDK is stated to carry), CARGO (default cargo), PKG_CONFIG
# (default pkg-config), PYTHON (default python3), LIBCLANG_PATH as bindgen
# needs it, and the link mode the caller states (scripts/sdk_paths.sh).
set -euo pipefail
shopt -s inherit_errexit 2>/dev/null || true
HERE=$(cd "$(dirname "$0")/.." && pwd)
PREFIX=${1:?sdk prefix}; WORK=${2:?work dir}
CARGO=${CARGO:-cargo}
# shellcheck source=sdk_paths.sh
. "$HERE/scripts/sdk_paths.sh"
rm -rf "$WORK"; mkdir -p "$WORK"
for stated in MOQ5_RUST_SIM MOQ5_RUST_MPEG2TS MOQ5_RUST_PLAYER MOQ5_RUST_LOCMAF; do
    case ${!stated:-} in 1|0) ;; *) echo "tier_graphs.sh: $stated must state 1 or 0" >&2; exit 2 ;; esac
done
pc_dir=$(moq5_sdk_pkgconfig "$PREFIX")
pc_dir=$(cd "${pc_dir%%:*}" && pwd -P)
CLASSIFY=$HERE/scripts/diagnostics.sh
ALLOW=$HERE/scripts/dependency_diagnostics.allow
failures=0
fail() { echo "FAIL $*"; failures=$((failures + 1)); }

# One build: build <name> [VAR=value...] -- <cargo args...>. Without
# settings it builds against the real SDK (MOQ_PREFIX); with them, against
# what they name alone (MOQ_PREFIX and PKG_CONFIG_PATH unset). The graphs
# share one target directory: cargo records each graph's build script also
# when it reruns nothing.
build() {
    local name=$1; shift
    local envs=()
    while [ "$1" != -- ]; do envs+=("$1"); shift; done
    shift
    local status=0
    if [ ${#envs[@]} = 0 ]; then
        MOQ_PREFIX=$PREFIX CARGO_TARGET_DIR=$WORK/target "$CARGO" build --manifest-path "$HERE/Cargo.toml" \
            --locked -vv --message-format=json "$@" > "$WORK/$name.json" 2> "$WORK/$name.err" || status=$?
    else
        env -u MOQ_PREFIX -u PKG_CONFIG_PATH "${envs[@]}" CARGO_TARGET_DIR="$WORK/target" "$CARGO" build \
            --manifest-path "$HERE/Cargo.toml" --locked -vv --message-format=json "$@" \
            > "$WORK/$name.json" 2> "$WORK/$name.err" || status=$?
    fi
    return "$status"
}
# The strict classifier over both streams; its verdict and inventory beside
# the logs. Non-zero when any diagnostic is not an allowed upstream warning.
judged() {
    bash "$CLASSIFY" "$WORK/$1.json" "$WORK/$1.err" "$WORK/$1.inv" "$ALLOW" "tier graph $1" \
        > "$WORK/$1.verdict" 2>&1
}
# The failing entries of an inventory, each with its occurrence count:
# count, verdict, crate and identity.
failing_entries() {
    awk -F'\t' '!/^#/ && $0 != "none" && $2 != "allowed" { print $1 "\t" $2 "\t" $4 "\t" $7 }' "$1"
}
# sole_refusal <inventory>: exactly one failing occurrence in all, and it is
# moq5-sys's build-script failure; any count above one, or any other
# failing identity, refuses.
sole_refusal() {
    awk -F'\t' '
        !/^#/ && $0 != "none" && $2 != "allowed" {
            occurrences += $1
            if ($2 == "error" && $4 == "cargo" &&
                index($7, "message:error: failed to run custom build command for `moq5-sys") == 1)
                expected += $1
        }
        END { exit !(occurrences == 1 && expected == 1) }' "$1"
}
# moq5-sys's build-script record in a build's JSON: its out_dir and the
# libraries it linked, sorted (`out_dir<TAB>lib lib...`). Exactly one.
sys_record() {
    "${PYTHON:-python3}" -I -c '
import json, sys
records = []
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    if not line.startswith("{"):
        continue
    try:
        record = json.loads(line)
    except ValueError:
        continue
    package = record.get("package_id", "")
    if record.get("reason") == "build-script-executed" and (
            "/moq5-sys#" in package or "#moq5-sys@" in package):
        records.append(record)
if len(records) != 1:
    sys.exit(f"{len(records)} build-script records of moq5-sys in {sys.argv[1]}")
libs = sorted({lib.split("=", 1)[-1] for lib in records[0].get("linked_libs", [])})
print(records[0]["out_dir"] + "\t" + " ".join(libs))
' "$1"
}
report_line() { grep -o "^moq5-sys: $2.*" "$1" | head -1 || true; }

# expect_graph <name> <dir> <tiers> <packages> <libraries> [VAR=value...] -- <cargo args...>
# <libraries> is the exact sorted set, or `+lib...` for libraries that must
# be among those linked (the service's closure names the SDK's own set).
expect_graph() {
    local name=$1 dir=$2 tiers=$3 packages=$4 libs=$5; shift 5
    if ! build "$name" "$@"; then
        fail "$name: the build failed (see $WORK/$name.err)"; tail -20 "$WORK/$name.err"; return
    fi
    if ! judged "$name"; then
        fail "$name: diagnostics beside a successful build:"; sed -n '2,$p' "$WORK/$name.verdict"; return
    fi
    local record out got package lib
    if ! record=$(sys_record "$WORK/$name.json"); then fail "$name: no single moq5-sys record"; return; fi
    out=${record%%$'\t'*}; got=${record#*$'\t'}
    got_tiers=$(report_line "$out/provenance.txt" "tiers ")
    [ "$got_tiers" = "moq5-sys: tiers $tiers" ] || { fail "$name: tiers [$got_tiers], expected [$tiers]"; return; }
    got_packages=$(report_line "$out/provenance.txt" "native source: ")
    case $got_packages in
        *"packages for these tiers: $packages") ;;
        *) fail "$name: packages [$got_packages], expected [$packages]"; return ;;
    esac
    # One directory for the whole SDK-owned metadata, every probed package
    # among it.
    sdk=$(report_line "$out/provenance.txt" "one SDK: ")
    for package in ${packages//,/ }; do
        case $sdk in
            *" $package.pc in $dir;"*) ;;
            *) fail "$name: $package.pc is not reported from $dir ($sdk)"; return ;;
        esac
    done
    if grep -o '[^ ]*\.pc in [^;]*;' <<<"$sdk" | grep -v -q -F ".pc in $dir;"; then
        fail "$name: SDK-owned metadata outside $dir ($sdk)"; return
    fi
    case $libs in
        +*)
            for lib in ${libs#+}; do
                [[ " $got " == *" $lib "* ]] || { fail "$name: [$got] links no $lib"; return; }
            done ;;
        *) [ "$got" = "$libs" ] || { fail "$name: link libraries [$got], expected exactly [$libs]"; return; } ;;
    esac
    echo "ok   $name: tiers $tiers; ${sdk#moq5-sys: }; links [$got]"
}
# expect_refusal <name> <phrase...> -- <VAR=value...> -- <cargo args...>:
# the build fails, every phrase is in its output (a phrase written `!text`
# must be absent), and its only failing diagnostic is one occurrence of
# moq5-sys's build-script failure. The settings name the search
# configuration alone (PKG_CONFIG_LIBDIR replaces pkg-config's default
# directories), so no other installed SDK can answer.
expect_refusal() {
    local name=$1; shift
    local phrases=() envs=()
    while [ "$1" != -- ]; do phrases+=("$1"); shift; done
    shift
    while [ "$1" != -- ]; do envs+=("$1"); shift; done
    if build "$name" "${envs[@]}" "$@"; then
        fail "$name: the build was accepted; it must be refused"; return
    fi
    local phrase
    for phrase in "${phrases[@]}"; do
        case $phrase in
            '!'*)
                if grep -q -F -- "${phrase#!}" "$WORK/$name.err"; then
                    fail "$name: refused with [${phrase#!}], which is not its cause (see $WORK/$name.err)"; return
                fi ;;
            *)
                grep -q -F -- "$phrase" "$WORK/$name.err" ||
                    { fail "$name: refused without [$phrase] (see $WORK/$name.err)"; tail -15 "$WORK/$name.err"; return; } ;;
        esac
    done
    if judged "$name"; then
        fail "$name: the classifier found no failure in a refused build"; return
    fi
    if ! sole_refusal "$WORK/$name.inv"; then
        fail "$name: not exactly one occurrence of the build script's refusal:"; failing_entries "$WORK/$name.inv"; return
    fi
    echo "ok   $name: refused with [${phrases[*]}]; one occurrence of the build script's failure, no other failing diagnostic"
}

# -- the refusal predicate's own controls --------------------------------------
# Inventories as the classifier writes them: an allowed upstream warning and
# the build script's failure once (accepted); the same failure counted
# twice (refused); and the failure once beside another failing identity
# (refused).
predicate=$WORK/predicate
mkdir -p "$predicate"
inventory_header=$'# diagnostics of a control\n# count  verdict  origin  crate  version  location  lint'
refusal_line=$'1\terror\tlocal\tcargo\t?\t-\tmessage:error: failed to run custom build command for `moq5-sys v0.1.0 (/x/moq5-sys)`'
upstream_line=$'3\tallowed\tregistry\tnom\t7.1.3\tsrc/str.rs:1\tdeprecated'
printf '%s\n%s\n%s\n' "$inventory_header" "$upstream_line" "$refusal_line" > "$predicate/one.inv"
printf '%s\n%s\n%s\n' "$inventory_header" "$upstream_line" "2${refusal_line#1}" > "$predicate/twice.inv"
printf '%s\n%s\n%s\n' "$inventory_header" "$refusal_line" \
    $'1\town\tlocal\tmoq5-sys\t0.1.0\t-\tbuild-output:warning: a second diagnostic' > "$predicate/distinct.inv"
printf '%s\nnone\n' "$inventory_header" > "$predicate/none.inv"
for control in one:pass twice:fail distinct:fail none:fail; do
    expected=${control#*:}; got=fail
    if sole_refusal "$predicate/${control%%:*}.inv"; then got=pass; fi
    if [ "$got" = "$expected" ]; then
        echo "ok   refusal-predicate-${control%%:*}: $got"
    else
        fail "refusal-predicate-${control%%:*}: $got, expected $expected"
    fi
done

media_libs="moq-cmaf moq-core moq-loc moq-media-object moq-mpeg2ts moq-msf"
locmaf_libs="moq-cmaf moq-core moq-loc moq-locmaf moq-media-object moq-mpeg2ts moq-msf"
SIM=$MOQ5_RUST_SIM MPEG2TS=$MOQ5_RUST_MPEG2TS PLAYER=$MOQ5_RUST_PLAYER LOCMAF=$MOQ5_RUST_LOCMAF

# -- the supported graphs ----------------------------------------------------
expect_graph core "$pc_dir" "core (enabled tier features: none: the default core)" libmoq-core moq-core \
    -- -p moq5-sys
expect_graph core-crate "$pc_dir" "core (enabled tier features: none: the default core)" libmoq-core moq-core \
    -- -p moq5-core
if [ "$SIM" = 1 ]; then
    expect_graph core-sim "$pc_dir" "core + sim (enabled tier features: sim)" libmoq-sim "moq-core moq-sim" \
        -- -p moq5-sys --features sim
fi
if [ "$MPEG2TS" = 1 ]; then
    expect_graph core-media "$pc_dir" "core + media (enabled tier features: media)" libmoq-media "$media_libs" \
        -- -p moq5-sys --features media
    expect_graph media-crate "$pc_dir" "core + media (enabled tier features: media)" libmoq-media "$media_libs" \
        -- -p moq5-media
    # Two independent crates, each asking for its own tier: their union.
    expect_graph union-core-media "$pc_dir" "core + media (enabled tier features: media)" libmoq-media \
        "$media_libs" -- -p moq5-core -p moq5-media
    if [ "$SIM" = 1 ]; then
        expect_graph core-sim-media "$pc_dir" "core + sim + media (enabled tier features: sim, media)" \
            "libmoq-media, libmoq-sim" "$media_libs moq-sim" -- -p moq5-sys --features sim,media
    fi
    if [ "$LOCMAF" = 1 ]; then
        expect_graph core-media-locmaf "$pc_dir" "core + media + locmaf (enabled tier features: media, locmaf)" \
            "libmoq-media, libmoq-locmaf" "$locmaf_libs" -- -p moq5-sys --features locmaf
        expect_graph locmaf-crate "$pc_dir" "core + media + locmaf (enabled tier features: media, locmaf)" \
            "libmoq-media, libmoq-locmaf" "$locmaf_libs" -- -p moq5-media --features locmaf
    fi
fi
if [ "$LOCMAF" = 1 ]; then
    # Beside the service, LOCMAF's package follows the service's closure.
    expect_graph service-locmaf "$pc_dir" \
        "core + media + locmaf + service (enabled tier features: media, locmaf, service)" \
        "libmoq-service, libmoq-locmaf" "+moq-service moq-locmaf moq-cmaf moq-core" \
        -- -p moq5-core -p moq5-media -p moq5 --features moq5-media/locmaf
fi
expect_graph service "$pc_dir" "core + media + service (enabled tier features: media, service)" libmoq-service \
    "+moq-service moq-core" -- -p moq5-sys --features service
expect_graph union-service "$pc_dir" "core + media + service (enabled tier features: media, service)" \
    libmoq-service "+moq-service moq-core" -- -p moq5-core -p moq5-media -p moq5
if [ "$SIM" = 1 ]; then
    expect_graph service-sim "$pc_dir" "core + sim + media + service (enabled tier features: sim, media, service)" \
        "libmoq-service, libmoq-sim" "+moq-service moq-sim moq-core" -- -p moq5-sys --features service,sim
    expect_graph union-service-sim "$pc_dir" \
        "core + sim + media + service (enabled tier features: sim, media, service)" \
        "libmoq-service, libmoq-sim" "+moq-service moq-sim moq-core" \
        -- -p moq5-core -p moq5 -p moq5-sys --features moq5-sys/sim
fi
if [ "$PLAYER" = 1 ]; then
    expect_graph player "$pc_dir" \
        "core + media + service + player (enabled tier features: media, service, player)" libmoq-service \
        "+moq-service moq-playback moq-core" -- -p moq5-sys --features player
    expect_graph union-player "$pc_dir" \
        "core + media + service + player (enabled tier features: media, service, player)" libmoq-service \
        "+moq-service moq-playback moq-core" -- -p moq5-core -p moq5-media -p moq5 --features moq5/player
    if [ "$SIM" = 1 ]; then
        expect_graph player-sim "$pc_dir" \
            "core + sim + media + service + player (enabled tier features: sim, media, service, player)" \
            "libmoq-service, libmoq-sim" "+moq-service moq-sim moq-core" -- -p moq5-sys --features player,sim
    fi
fi

# -- views of the real SDK ---------------------------------------------------
# collect_providers <dir> <search list>: the external providers of a search
# list as moq5_sdk_pkgconfig prints it (the SDK's pkg-config directory,
# then MOQ5_SDK_PKG_CONFIG_PATH), copied into <dir>; prints <dir>. A
# provider is any package the SDK does not own; the first spelling of a
# name wins, as pkg-config searches, and each copy has ${pcfiledir}
# replaced by the directory it came from so it still describes its own
# files. Every view below is searched with the providers of the SDK's own
# configuration beside it, or with those of the configured provider
# fixture.
collect_providers() {
    local out=$1 list=$2 search_dir origin file package dirs
    mkdir -p "$out"
    IFS=: read -r -a dirs <<<"$list"
    for search_dir in "${dirs[@]}"; do
        [ -n "$search_dir" ] && [ -d "$search_dir" ] || continue
        origin=$(cd "$search_dir" && pwd -P)
        for file in "$origin"/*.pc; do
            [ -f "$file" ] || continue
            package=${file##*/}; package=${package%.pc}
            case $package in libmoq|libmoq-*) continue ;; esac
            [ -e "$out/$package.pc" ] && continue
            sed "s#\${pcfiledir}#$origin#g" "$file" > "$out/$package.pc"
        done
    done
    (cd "$out" && pwd -P)
}
providers=$(collect_providers "$WORK/providers" "$(moq5_sdk_pkgconfig "$PREFIX")")
if ls "$providers"/libmoq*.pc >/dev/null 2>&1; then
    fail "the providers' directory holds an SDK-owned package"
fi
echo "note the views are searched with the SDK's external providers: $(cd "$providers" && ls | paste -sd' ' - || true)"

# view <name> <package...>: a pkg-config directory holding copies of the
# named packages, each still describing the real SDK's files.
view() {
    local name=$1; shift
    local dir=$WORK/view-$name
    moq5_sdk_copy_pc "$PREFIX" "$dir" "$@"
    (cd "$dir" && pwd -P)
}
# whole <name>: copies of every SDK-owned package.
whole() {
    local dir=$WORK/view-$1
    moq5_sdk_owned_view "$PREFIX" "$dir"
    (cd "$dir" && pwd -P)
}
# versioned <name> <package>: a whole view in which <package> reports 9.9.9.
versioned() {
    local dir
    dir=$(whole "$1")
    sed -i.orig 's/^Version: .*/Version: 9.9.9/' "$dir/$2.pc"
    rm -f "$dir/$2.pc.orig"
    echo "$dir"
}

expect_refusal missing-core "pkg-config finds no \`libmoq-core.pc\`" "the core tier (features: none: the default core)" \
    "\`libmoq.pc\` resolves on that path, but it is not used in its place" \
    -- "PKG_CONFIG_LIBDIR=$(view no-core libmoq):$providers" -- -p moq5-sys
expect_refusal missing-sim "pkg-config finds no \`libmoq-sim.pc\`" "MOQ_BUILD_SIM=ON" \
    -- "PKG_CONFIG_LIBDIR=$(view no-sim libmoq libmoq-core):$providers" -- -p moq5-sys --features sim
expect_refusal missing-media "pkg-config finds no \`libmoq-media.pc\`" "MOQ_BUILD_MPEG2TS" \
    -- "PKG_CONFIG_LIBDIR=$(view no-media libmoq libmoq-core):$providers" -- -p moq5-sys --features media
if [ "$MPEG2TS" = 1 ]; then
    expect_refusal missing-locmaf "pkg-config finds no \`libmoq-locmaf.pc\`" "MOQ_BUILD_LOCMAF=ON" \
        -- "PKG_CONFIG_LIBDIR=$(view no-locmaf libmoq libmoq-core libmoq-media):$providers" \
        -- -p moq5-sys --features locmaf
fi
# A tier stated not carried: the SDK's own refusal when it lacks the
# package; a package it has anyway is left out by the statement, said so.
if [ "$SIM" = 0 ]; then
    if [ -f "$pc_dir/libmoq-sim.pc" ]; then
        echo "note the SDK carries libmoq-sim.pc; its graphs are left out (MOQ5_RUST_SIM=0)"
    else
        expect_refusal sdk-without-sim "pkg-config finds no \`libmoq-sim.pc\`" \
            -- "PKG_CONFIG_LIBDIR=$pc_dir:$providers" -- -p moq5-sys --features sim
    fi
fi
if [ "$LOCMAF" = 0 ]; then
    if [ -f "$pc_dir/libmoq-locmaf.pc" ]; then
        echo "note the SDK carries libmoq-locmaf.pc; its graphs are left out (MOQ5_RUST_LOCMAF=0)"
    elif [ "$MPEG2TS" = 1 ]; then
        expect_refusal sdk-without-locmaf "pkg-config finds no \`libmoq-locmaf.pc\`" \
            -- "PKG_CONFIG_LIBDIR=$pc_dir:$providers" -- -p moq5-sys --features locmaf
    fi
fi
if [ "$MPEG2TS" = 0 ]; then
    if [ -f "$pc_dir/libmoq-media.pc" ]; then
        echo "note the SDK carries libmoq-media.pc; its graphs are left out (MOQ5_RUST_MPEG2TS=0)"
    else
        expect_refusal sdk-without-media "pkg-config finds no \`libmoq-media.pc\`" \
            -- "PKG_CONFIG_LIBDIR=$pc_dir:$providers" -- -p moq5-sys --features media
    fi
fi

# The service's own metadata: the service package from another directory
# than the aggregate libmoq.pc it requires, and one directory whose
# aggregate disagrees on its version; the whole SDK in one view builds.
split=$(view service-split libmoq-service)
expect_refusal service-split "one dependency graph links one native SDK" "libmoq-service.pc from $split" \
    "libmoq.pc from $pc_dir" -- "PKG_CONFIG_LIBDIR=$split:$pc_dir:$providers" -- -p moq5-sys --features service
aggregate=$(versioned service-version libmoq)
expect_refusal service-version "report different versions" "libmoq 9.9.9" \
    -- "PKG_CONFIG_LIBDIR=$aggregate:$providers" -- -p moq5-sys --features service
matching=$(whole service-matching)
expect_graph service-matching "$matching" "core + media + service (enabled tier features: media, service)" \
    libmoq-service "+moq-service moq-core" "PKG_CONFIG_LIBDIR=$matching:$providers" -- -p moq5-sys --features service

# A tier package from another directory than the core it requires, and
# one directory whose tier package disagrees with the core, with the
# simulator (beside the service, too) or else the media parsers; the whole
# SDK in one view builds the same graphs.
tier_package=""
if [ "$SIM" = 1 ]; then
    tier_package=libmoq-sim; tier_feature=sim
elif [ "$MPEG2TS" = 1 ]; then
    tier_package=libmoq-media; tier_feature=media
fi
if [ -n "$tier_package" ]; then
    split=$(view split "$tier_package")
    expect_refusal mixed-sdks "one dependency graph links one native SDK" "$tier_package.pc from $split" \
        "libmoq-core.pc from $pc_dir" -- "PKG_CONFIG_LIBDIR=$split:$pc_dir:$providers" -- -p moq5-sys --features "$tier_feature"
    versions=$(versioned versions "$tier_package")
    expect_refusal mixed-versions "report different versions" "$tier_package 9.9.9" \
        -- "PKG_CONFIG_LIBDIR=$versions:$providers" -- -p moq5-sys --features "$tier_feature"
else
    echo "note the SDK is stated to carry neither the simulator nor the media tier package: the tier package's two-SDK and version refusals need one"
fi
if [ "$SIM" = 1 ]; then
    split=$(view service-sim-split libmoq-sim)
    expect_refusal service-sim-split "one dependency graph links one native SDK" "libmoq-sim.pc from $split" \
        "libmoq-service.pc from $pc_dir" -- "PKG_CONFIG_LIBDIR=$split:$pc_dir:$providers" -- -p moq5-sys --features service,sim
    versions=$(versioned service-sim-version libmoq-sim)
    expect_refusal service-sim-version "report different versions" "libmoq-sim 9.9.9" \
        -- "PKG_CONFIG_LIBDIR=$versions:$providers" -- -p moq5-sys --features service,sim
    matching=$(whole service-sim-matching)
    expect_graph service-sim-matching "$matching" \
        "core + sim + media + service (enabled tier features: sim, media, service)" \
        "libmoq-service, libmoq-sim" "+moq-service moq-sim moq-core" "PKG_CONFIG_LIBDIR=$matching:$providers" \
        -- -p moq5-sys --features service,sim
fi

# -- an SDK whose service requires an external provider ---------------------
# The provider has its own directory, prefix and version, as wtquic-msquic
# beside a WTQuic SDK, and is configured as MOQ5_SDK_PKG_CONFIG_PATH
# configures one: two directories after any already configured, collected
# by collect_providers from what moq5_sdk_pkgconfig prints. The first holds
# the provider, its prefix relative to its own directory, beside copies of
# the SDK's libmoq.pc and libmoq-core.pc (owned packages, which would
# resolve if they were collected); the second a provider of the same name,
# another version, and a requirement nothing satisfies.
first=$WORK/configured-providers-first
second=$WORK/configured-providers-second
mkdir -p "$first" "$second"
first=$(cd "$first" && pwd -P); second=$(cd "$second" && pwd -P)
cat > "$first/moq5-tier-graph-provider.pc" <<'EOF'
prefix=${pcfiledir}/..
Name: moq5-tier-graph-provider
Description: an external provider of a service SDK
Version: 7.7.7
Libs:
Cflags:
EOF
moq5_sdk_copy_pc "$PREFIX" "$first" libmoq libmoq-core
cat > "$second/moq5-tier-graph-provider.pc" <<'EOF'
Name: moq5-tier-graph-provider
Description: a later provider of the same name, never the one taken
Version: 6.6.6
Requires: moq5-tier-graph-absent
Libs:
Cflags:
EOF
configured="${MOQ5_SDK_PKG_CONFIG_PATH:+$MOQ5_SDK_PKG_CONFIG_PATH:}$first:$second"
backed_providers=$(collect_providers "$WORK/providers-configured" \
    "$(MOQ5_SDK_PKG_CONFIG_PATH=$configured moq5_sdk_pkgconfig "$PREFIX")")
collected=$backed_providers/moq5-tier-graph-provider.pc
if [ ! -f "$collected" ]; then
    fail "configured-providers: the configured provider was not collected"
elif ! grep -q -x 'Version: 7.7.7' "$collected" || grep -q 'moq5-tier-graph-absent' "$collected"; then
    fail "configured-providers: the later directory's provider was taken over the first's"
elif ! grep -q -x -F "prefix=$first/.." "$collected" || grep -q -F '${pcfiledir}' "$collected"; then
    fail "configured-providers: the provider's \${pcfiledir} was not resolved to $first"
elif ls "$backed_providers"/libmoq*.pc >/dev/null 2>&1; then
    fail "configured-providers: an SDK-owned package from a configured directory was collected"
else
    echo "ok   configured-providers: the first directory's provider collected with its prefix resolved; the configured owned packages and the later provider left out"
fi

# provided <view dir>: its service package also requires the provider. It
# runs in a command substitution, so a view it cannot rewrite stops the
# script rather than counting a failure.
provided() {
    sed -i.orig 's/^Requires:\(.*\)$/Requires:\1 moq5-tier-graph-provider/' "$1/libmoq-service.pc"
    rm -f "$1/libmoq-service.pc.orig"
    grep -q '^Requires:.* moq5-tier-graph-provider$' "$1/libmoq-service.pc" ||
        { echo "tier_graphs.sh: the provider-backed view $1 does not require the provider" >&2; return 1; }
    echo "$1"
}
backed=$(provided "$(whole provider-matching)")
expect_graph provider-matching "$backed" "core + media + service (enabled tier features: media, service)" \
    libmoq-service "+moq-service moq-core" "PKG_CONFIG_LIBDIR=$backed:$backed_providers" \
    -- -p moq5-sys --features service
expect_refusal provider-missing "libmoq-service.pc is on the search path" \
    "pkg-config cannot resolve what it requires" \
    "Package 'moq5-tier-graph-provider', required by 'libmoq-service', not found" \
    "!one dependency graph" "!report different versions" \
    -- "PKG_CONFIG_LIBDIR=$backed:$providers" -- -p moq5-sys --features service
if [ "$SIM" = 1 ]; then
    backed_sim=$(provided "$(whole provider-sim-matching)")
    expect_graph provider-sim-matching "$backed_sim" \
        "core + sim + media + service (enabled tier features: sim, media, service)" \
        "libmoq-service, libmoq-sim" "+moq-service moq-sim moq-core" \
        "PKG_CONFIG_LIBDIR=$backed_sim:$backed_providers" -- -p moq5-sys --features service,sim
    expect_refusal provider-sim-missing "libmoq-service.pc is on the search path" \
        "pkg-config cannot resolve what it requires" \
        "Package 'moq5-tier-graph-provider', required by 'libmoq-service', not found" \
        "!one dependency graph" "!report different versions" \
        -- "PKG_CONFIG_LIBDIR=$backed_sim:$providers" -- -p moq5-sys --features service,sim
fi
split=$(provided "$(view provider-split libmoq-service)")
expect_refusal provider-split "one dependency graph links one native SDK" "libmoq-service.pc from $split" \
    "libmoq.pc from $pc_dir" "!cannot resolve what it requires" "!moq5-tier-graph-provider.pc" \
    -- "PKG_CONFIG_LIBDIR=$split:$pc_dir:$backed_providers" -- -p moq5-sys --features service
aggregate=$(provided "$(versioned provider-version libmoq)")
expect_refusal provider-version "report different versions" "libmoq 9.9.9" "!7.7.7" "!6.6.6" \
    "!cannot resolve what it requires" \
    -- "PKG_CONFIG_LIBDIR=$aggregate:$backed_providers" -- -p moq5-sys --features service
# The view lacks libmoq.pc while the configured first directory held one:
# only its exclusion from the collected providers keeps it missing.
missing_owned=$(provided "$(whole provider-missing-owned)")
rm "$missing_owned/libmoq.pc"
expect_refusal provider-missing-owned "pkg-config cannot resolve what it requires" \
    "Package 'libmoq', required by 'libmoq-service', not found" \
    "!Package 'moq5-tier-graph-provider', required by 'libmoq-service', not found" "!one dependency graph" \
    -- "PKG_CONFIG_LIBDIR=$missing_owned:$backed_providers" -- -p moq5-sys --features service

# -- the warning gate's control ----------------------------------------------
# The core graph again with an unknown clang warning option: libclang warns
# from the build script, cargo exits 0 and the graph's own assertions hold,
# so the classifier alone must refuse it, with the warning inventoried.
control=warning-control
option=-Wmoq5-tier-graph-control-option
if ! build "$control" "BINDGEN_EXTRA_CLANG_ARGS=${BINDGEN_EXTRA_CLANG_ARGS:+$BINDGEN_EXTRA_CLANG_ARGS }$option" \
    "MOQ_PREFIX=$PREFIX" -- -p moq5-sys; then
    fail "$control: the build failed; the control needs a successful exit (see $WORK/$control.err)"
elif ! record=$(sys_record "$WORK/$control.json") || [ "${record#*$'\t'}" != moq-core ]; then
    fail "$control: the graph itself did not hold (${record:-no record})"
elif judged "$control"; then
    fail "$control: the classifier passed a build whose build script warned"
elif ! grep -q -F "build-output:warning: unknown warning option '$option'" "$WORK/$control.inv"; then
    fail "$control: refused, but not for the injected warning (see $WORK/$control.inv)"
else
    echo "ok   $control: cargo exit 0 and the graph held, the classifier refused it on the injected warning"
fi

if [ "$failures" != 0 ]; then
    echo "tier_graphs.sh: $failures failure(s) (logs under $WORK)" >&2
    exit 1
fi
echo "tier_graphs.sh: OK ($WORK)"
