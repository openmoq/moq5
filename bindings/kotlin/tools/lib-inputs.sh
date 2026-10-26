# shellcheck shell=sh
# LibMoQ topology input inventory -- sourced, never executed.
#
# The digest audited by check-jni-topology.sh must change when the BYTES of any
# input that can affect the packaged DSO change, with no path renamed. An
# earlier revision hashed the `find` output, so it was a checksum of path names
# and an in-place edit left it unchanged (chats/libmoq/1153 R1).
#
# The roots below are a deliberate SUPERSET: every regular file under them is
# hashed, including CMake inputs, vendored third-party sources and the Android
# recipe. A superset fails closed -- an unrelated edit under a root costs a
# re-declaration, whereas an omitted root silently accepts a changed DSO.
#
#   core/ service/ media/ adapters/   product sources, headers, CMake inputs
#   CMakeLists.txt cmake/             the top-level build graph
#   bindings/kotlin/gradle/           the pinned wrapper, toolchain and wrapper jar
#   bindings/kotlin/*.gradle.kts,     the Android build configuration
#   gradle.properties, gradlew*
#   bindings/kotlin/jni/              the JNI source that enters the DSO
#   bindings/kotlin/CMakeLists.txt    the eventual JNI build graph
#                                     (absent today; its APPEARANCE must move
#                                      the digest, which is why it is listed)
#
# This digest describes the NATIVE DSO and nothing else. Pure Kotlin sources are
# deliberately absent: the Kotlin library module cannot change the packaged
# libmoq5_jni.so, so hashing it here would invalidate the native stage on every
# Kotlin edit and would assert a dependency that does not exist. The AAR, when
# it exists, needs its OWN artifact/provenance digest covering Kotlin sources
# and the native payload together; that boundary is NOT implemented yet.
#
# Every declared root must also be a real directory. A root naming a path that
# does not exist reads as coverage while contributing nothing -- the removed
# `bindings/kotlin/src` was exactly that once the module landed at
# moq5-android/src (chats/libmoq/1185 F7).
#
# local.properties is deliberately EXCLUDED: it is host-local, so hashing it
# would make the digest differ between machines for no build-affecting reason.
# THE TOOLS DIRECTORY IS NOT A ROOT. `bindings/kotlin/tools` was one, so every
# host-only oracle, self-test, RED driver and owner harness under it bound the
# packaged DSO: editing a test tool invalidated the native stage even though no
# build step reads it (chats/libmoq/1733 G1, ruled in 1734). The authority is
# now SPLIT -- a native manifest for what can change libmoq5_jni.so, and a
# separate tooling/evidence manifest for the rest -- and a path under the tools
# directory that matches neither declaration is a hard failure, never a default.
MOQ_INPUT_ROOTS="core service media adapters cmake \
bindings/kotlin/gradle bindings/kotlin/jni"
MOQ_INPUT_FILES="CMakeLists.txt \
bindings/kotlin/build.gradle.kts bindings/kotlin/settings.gradle.kts \
bindings/kotlin/gradle.properties bindings/kotlin/gradlew \
bindings/kotlin/gradlew.bat bindings/kotlin/CMakeLists.txt"

# Every family that must be REPRESENTED in the manifest. An aggregate digest
# alone cannot detect a root silently dropped from the inventory, because the
# declaration would move with it; requiring each family to contribute at least
# one entry makes that mutation visible (chats/libmoq/1155 R3).
MOQ_INPUT_FAMILIES="core service media adapters cmake \
bindings/kotlin/tools bindings/kotlin/gradle"

# --- the tools directory, split by what the native producer CONSUMES --------
MOQ_TOOLS_ROOT="bindings/kotlin/tools"

# Files under the tools directory that the topology producer actually reads, by
# EXACT path: the recipe itself and the two helpers it sources. Changing any of
# them can change the packaged DSO, so they stay in the native manifest and are
# what keeps `bindings/kotlin/tools` a covered family. A helper the recipe
# starts consuming must be added here; until it is, it is only evidence.
MOQ_NATIVE_TOOL_FILES="bindings/kotlin/tools/build-jni-topology.sh \
bindings/kotlin/tools/lib-declaration.sh \
bindings/kotlin/tools/lib-inputs.sh \
bindings/kotlin/tools/lib-ptls-prefix.sh"

# Host-only evidence: checkers, self-tests, RED drivers, probes, the owner
# harness and the packaging/inventory helpers that no topology build runs.
# These are hashed into the TOOLING manifest, which no artifact is bound to.
MOQ_TOOLING_GLOBS="check-*.sh selftest-*.sh red-*.sh probe-*.sh probe/* \
publication_owner/* lib-*.sh mk-*.sh build-*.sh run-*.sh source-manifest.sh \
verify-toolchain.sh gradlew-verified *.java"

# inputs_classify_tool <repo-relative path>
# Echoes `native` or `tooling` and returns 0. Returns non-zero for a path the
# declarations above do not cover: UNDECIDABLE, which every caller must treat
# as a failure. Nothing is classified by default.
inputs_classify_tool() {
    _ct=$1
    for _cn in $MOQ_NATIVE_TOOL_FILES; do
        [ "$_ct" = "$_cn" ] && { printf 'native'; return 0; }
    done
    _cr=${_ct#"$MOQ_TOOLS_ROOT"/}
    [ "$_cr" != "$_ct" ] || return 1
    # Pathname expansion is OFF while the patterns are iterated: unquoted in a
    # `for` list they would be expanded against the CURRENT directory, so
    # `check-*.sh` would silently become the checkers that happen to sit beside
    # this file and would then match nothing else.
    case $- in *f*) _cf=1 ;; *) _cf=0; set -f ;; esac
    _cv=1
    for _cg in $MOQ_TOOLING_GLOBS; do
        # shellcheck disable=SC2254
        case $_cr in
            $_cg) _cv=0; break ;;
        esac
    done
    [ "$_cf" -eq 1 ] || set +f
    [ "$_cv" -eq 0 ] || return 1
    printf 'tooling'
    return 0
}

# inputs_tools_partition <repo-root> <native-list> <tooling-list>
# Writes the classified absolute paths into the two lists. Returns 4, with a
# named diagnostic on stderr, at the FIRST unclassifiable path -- so a new tool
# nobody declared stops both manifests instead of quietly joining one.
inputs_tools_partition() {
    _tp_r=$1; _tp_n=$2; _tp_t=$3
    : > "$_tp_n" || return 1
    : > "$_tp_t" || return 1
    [ -d "$_tp_r/$MOQ_TOOLS_ROOT" ] || return 0
    _tp_all="$_tp_n.all"
    find "$_tp_r/$MOQ_TOOLS_ROOT" -type f -print > "$_tp_all" || { rm -f "$_tp_all"; return 1; }
    _tp_sl=$(find "$_tp_r/$MOQ_TOOLS_ROOT" -type l | sed -n '1p')
    [ -z "$_tp_sl" ] || { rm -f "$_tp_all"; return 3; }
    _tp_bad=""
    while IFS= read -r _tp_f; do
        [ -n "$_tp_f" ] || continue
        _tp_rel=${_tp_f#"$_tp_r"/}
        if _tp_k=$(inputs_classify_tool "$_tp_rel"); then
            case $_tp_k in
                native)  printf '%s\n' "$_tp_f" >> "$_tp_n" || { _tp_bad=$_tp_rel; break; } ;;
                tooling) printf '%s\n' "$_tp_f" >> "$_tp_t" || { _tp_bad=$_tp_rel; break; } ;;
            esac
        else
            _tp_bad=$_tp_rel
            printf 'unclassifiable path under %s: %s\n' "$MOQ_TOOLS_ROOT" "$_tp_rel" >&2
            break
        fi
    done < "$_tp_all"
    rm -f "$_tp_all"
    [ -z "$_tp_bad" ] || return 4
    return 0
}

# _inputs_hash_list <repo-root> <list-file> <out-file>
# "<sha256>  <relative path>", sorted by path. Nothing is written unless every
# listed file hashes.
_inputs_hash_list() {
    _hr=$1; _hl=$2; _ho=$3
    : > "$_ho" || return 1
    _hs="$_ho.sorted"
    sort "$_hl" > "$_hs" || { rm -f "$_hs"; return 1; }
    _hfail=0
    while IFS= read -r _hf; do
        [ -n "$_hf" ] || continue
        if ! _hh=$(decl_sha256 "$_hf"); then _hfail=2; break; fi
        printf '%s  %s\n' "$_hh" "${_hf#"$_hr"/}" >> "$_ho" || { _hfail=1; break; }
    done < "$_hs"
    rm -f "$_hs"
    [ "$_hfail" -eq 0 ] || return "$_hfail"
    return 0
}

# Individually listed build files need their OWN coverage rule: a sentinel
# elsewhere under bindings/kotlin/gradle/ does not make gradlew or
# settings.gradle.kts non-vacuous (chats/libmoq/1157 F4). These must appear in
# the manifest by exact relative path.
#
# bindings/kotlin/CMakeLists.txt is deliberately NOT required: it does not exist
# yet. It is hashed if present, so its APPEARANCE moves the digest.
MOQ_INPUT_REQUIRED_FILES="CMakeLists.txt \
bindings/kotlin/build.gradle.kts bindings/kotlin/settings.gradle.kts \
bindings/kotlin/gradle.properties bindings/kotlin/gradlew \
bindings/kotlin/gradlew.bat"

# inputs_manifest <repo-root> <out-file>
# Writes "<sha256>  <relative path>" sorted by path under LC_ALL=C. Fails, with
# nothing written, if any listed file cannot be hashed.
inputs_manifest() {
    _ir=$1; _io=$2
    [ -d "$_ir" ] || return 1
    : > "$_io" || return 1
    _tmp="$_io.partial"
    : > "$_tmp" || return 1
    for _rt in $MOQ_INPUT_ROOTS; do
        [ -d "$_ir/$_rt" ] || continue
        find "$_ir/$_rt" -type f -print >> "$_tmp" || { rm -f "$_tmp"; return 1; }
    done
    for _fl in $MOQ_INPUT_FILES; do
        [ -f "$_ir/$_fl" ] && printf '%s\n' "$_ir/$_fl" >> "$_tmp"
    done
    # The tools directory contributes only what the producer consumes, and an
    # unclassifiable path there stops the manifest rather than being dropped
    # from it: a tool nobody classified must not silently leave the DSO digest.
    _tn="$_io.tools-native"; _tt="$_io.tools-evidence"
    # Taken in statement form: `if ! cmd` would report the NEGATION's status,
    # so an unclassifiable path would return 0 and leave an empty manifest to
    # be hashed as though the tree held nothing.
    inputs_tools_partition "$_ir" "$_tn" "$_tt"; _prc=$?
    if [ "$_prc" -ne 0 ]; then
        rm -f "$_tmp" "$_tn" "$_tt" "$_io"
        return "$_prc"
    fi
    cat "$_tn" >> "$_tmp" || { rm -f "$_tmp" "$_tn" "$_tt"; return 1; }
    rm -f "$_tn" "$_tt"
    # Reject a symlinked input outright: its bytes are not where its path says.
    for _rt in $MOQ_INPUT_ROOTS; do
        [ -d "$_ir/$_rt" ] || continue
        _sl=$(find "$_ir/$_rt" -type l | sed -n '1p')
        [ -z "$_sl" ] || { rm -f "$_tmp"; return 3; }
    done
    _inputs_hash_list "$_ir" "$_tmp" "$_io" || { _mrc=$?; rm -f "$_tmp"; return "$_mrc"; }
    rm -f "$_tmp"
    [ -s "$_io" ] || return 1
    return 0
}

# tooling_manifest <repo-root> <out-file>
# The EVIDENCE inventory: every host-only tool, in the same format. It exists
# so a checker or harness edit is still recorded somewhere -- just not in the
# manifest an artifact is bound to.
tooling_manifest() {
    _gr=$1; _go=$2
    [ -d "$_gr" ] || return 1
    : > "$_go" || return 1
    _gn="$_go.tools-native"; _gt="$_go.tools-evidence"
    inputs_tools_partition "$_gr" "$_gn" "$_gt"; _grc=$?
    if [ "$_grc" -ne 0 ]; then
        rm -f "$_gn" "$_gt" "$_go"
        return "$_grc"
    fi
    _inputs_hash_list "$_gr" "$_gt" "$_go" || { _grc=$?; rm -f "$_gn" "$_gt"; return "$_grc"; }
    rm -f "$_gn" "$_gt"
    # An EMPTY evidence inventory is a legitimate answer -- a tree may carry no
    # host tools at all -- and it is distinguishable from a failure, which
    # returns non-zero above. The native manifest keeps its own non-empty rule.
    return 0
}

# tooling_digest <repo-root> <scratch-dir>
tooling_digest() {
    _gdr=$1; _gds=$2
    [ -d "$_gds" ] || return 1
    _gmf="$_gds/libmoq-tooling.manifest"
    tooling_manifest "$_gdr" "$_gmf" || return $?
    _gdg=$(decl_sha256 "$_gmf") || return 2
    decl_is_sha256 "$_gdg" || return 2
    printf '%s' "$_gdg"
}

# inputs_digest <repo-root> <scratch-dir>
# Echoes the SHA-256 of the manifest, computed through the accepted helper over
# a REAL regular file -- decl_sha256 rejects /dev/stdin by design.
inputs_digest() {
    _dr=$1; _ds=$2
    [ -d "$_ds" ] || return 1
    _mf="$_ds/libmoq-inputs.manifest"
    inputs_manifest "$_dr" "$_mf" || return $?
    _dg=$(decl_sha256 "$_mf") || return 2
    decl_is_sha256 "$_dg" || return 2
    printf '%s' "$_dg"
}

# inputs_families_covered <manifest>
# Echoes the first required family OR required file with no entry in the
# manifest, or nothing.
inputs_families_covered() {
    _mf=$1
    [ -f "$_mf" ] || { printf 'manifest-absent'; return 0; }
    for _fam in $MOQ_INPUT_FAMILIES; do
        awk -v f="$_fam/" 'index($2, f) == 1 { found = 1; exit } END { exit !found }' "$_mf" \
            || { printf '%s' "$_fam"; return 0; }
    done
    for _fl in $MOQ_INPUT_REQUIRED_FILES; do
        awk -v f="$_fl" '$2 == f { found = 1; exit } END { exit !found }' "$_mf" \
            || { printf '%s' "$_fl"; return 0; }
    done
    printf ''
}
