#!/usr/bin/env bash
#
# provider_patches.sh - apply LibMoQ's tracked provider patches.
#
# Each provider directory under patches/ (patches/picoquic, patches/picotls)
# carries a `series` manifest: the patch file names, one per line, in the
# order they apply (blank lines and #-comments ignored). The series is the
# selection: exactly the listed patches are applied, in that order, and a
# patch file it does not list is not applied (a note says so). Every recipe
# that prepares a provider checkout follows it, either through this script
# or through scripts/setup_picoquic_deps.sh, which sources the functions
# below.
#
#   provider_patches.sh apply <patches/provider-dir> <provider-checkout>
#
# applies the series to the checkout. Already-applied inputs (fully or a
# leading part of the series) are accepted; a series naming a missing patch
# or the same patch twice, or a conflicting checkout, is refused before the
# checkout is modified.
#
# Sourcing this file only defines functions.

provider_patches_log() { printf '[provider_patches] %s\n' "$*" >&2; }
provider_patches_die() { printf '[provider_patches] ERROR: %s\n' "$*" >&2; exit 1; }

# Compare canonical shell paths: Git for Windows can return a drive-letter path
# while Git Bash reports the same checkout as /d/..., so string comparison of
# rev-parse and pwd would reject a valid dependency checkout.
provider_is_checkout_root() {
    local dir=$1 git_root shell_root
    git_root=$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null) || return 1
    git_root=$(cd "$git_root" && pwd -P) || return 1
    shell_root=$(cd "$dir" && pwd -P) || return 1
    [ "$git_root" = "$shell_root" ]
}

# read_patch_series <series-file>: prints the manifest's patch names.
read_patch_series() {
    local series=$1 line
    [ -f "$series" ] || provider_patches_die "missing patch series: $series"
    while IFS= read -r line || [ -n "$line" ]; do
        line=${line%%#*}
        line=${line//[[:space:]]/}
        [ -n "$line" ] && printf '%s\n' "$line"
    done < "$series"
}

# provider_apply_patch <provider> <patch-dir> <checkout> <name>
provider_apply_patch() {
    local provider=$1 patch_dir=$2 dir=$3 name=$4
    local patch="$patch_dir/$name"
    [ -f "$patch" ] || provider_patches_die "missing $provider patch: $patch"
    # A non-repository child of libmoq can silently skip every patch path.
    provider_is_checkout_root "$dir" \
        || provider_patches_die "$provider patch input is not its own git root: $dir"
    if git -C "$dir" apply --check "$patch" >/dev/null 2>&1; then
        git -C "$dir" apply "$patch" \
            || provider_patches_die "$provider patch $name failed to apply"
        provider_patches_log "applied $provider patch $name"
    elif git -C "$dir" apply --reverse --check "$patch" >/dev/null 2>&1; then
        provider_patches_log "$provider patch $name already applied"
    else
        provider_patches_die "$provider patch $name conflicts with $dir"
    fi
}

# provider_check_series <provider> <patch-dir> <declared names...>
# Every declared patch must exist in <patch-dir> and be declared once.
# Patch files the series does not list are reported and left alone.
provider_check_series() {
    local provider=$1 patch_dir=$2 name other i j
    shift 2
    local declared=("$@")
    [ "${#declared[@]}" -gt 0 ] || provider_patches_die "empty $provider patch series"
    for ((i=0; i<${#declared[@]}; ++i)); do
        name=${declared[$i]}
        case "$name" in
            */*) provider_patches_die "$provider patch series entry is not a file name: $name" ;;
        esac
        [ -f "$patch_dir/$name" ] \
            || provider_patches_die "missing $provider patch: $patch_dir/$name"
        for ((j=0; j<i; ++j)); do
            [ "${declared[$j]}" != "$name" ] \
                || provider_patches_die "$provider patch series lists $name twice"
        done
    done
    for other in "$patch_dir/"*.patch; do
        [ -f "$other" ] || continue
        other=${other##*/}
        for name in "${declared[@]}"; do
            [ "$name" = "$other" ] && continue 2
        done
        provider_patches_log "$provider patch $other is not in the series; not applied"
    done
}

# provider_apply_series <provider> <patch-dir> <checkout> <declared names...>
provider_apply_series() {
    local provider=$1 patch_dir=$2 dir=$3 name i
    shift 3
    local declared=("$@")
    provider_check_series "$provider" "$patch_dir" "${declared[@]}"
    provider_is_checkout_root "$dir" \
        || provider_patches_die "$provider patch input is not its own git root: $dir"
    # Later patches may rewrite lines an earlier one introduced, so a patch
    # cannot be judged applied or not on its own once its successors are in.
    # Decide for the series: the longest applied prefix after which the rest
    # still applies in order. Without one, nothing is touched and the first
    # patch that neither applies nor reverts on its own is named.
    local applied status=0
    applied=$(provider_applied_prefix "$patch_dir" "$dir" "${declared[@]}") || status=$?
    [ "$status" -eq 0 ] \
        || provider_patches_die "could not trial the $provider series (status $status); $dir was not modified"
    case "$applied" in
        -1|0|[1-9]|[1-9][0-9]) ;;
        *) provider_patches_die "the $provider series trial returned '$applied'; $dir was not modified" ;;
    esac
    [ "$applied" -le "${#declared[@]}" ] \
        || provider_patches_die "the $provider series trial returned '$applied' for ${#declared[@]} patches; $dir was not modified"
    if [ "$applied" -lt 0 ]; then
        for name in "${declared[@]}"; do
            if ! git -C "$dir" apply --check "$patch_dir/$name" >/dev/null 2>&1 &&
               ! git -C "$dir" apply --reverse --check "$patch_dir/$name" >/dev/null 2>&1; then
                # Preserve Git's hunk/path diagnostic for provider-specific
                # checkout failures (notably Windows line-ending differences).
                git -C "$dir" apply --check --verbose "$patch_dir/$name" >&2 || :
                provider_patches_die "$provider patch $name conflicts with $dir"
            fi
        done
        provider_patches_die "$provider patch ${declared[0]} conflicts with $dir (the series does not apply in order)"
    fi
    for ((i=0; i<${#declared[@]}; ++i)); do
        if [ "$applied" -gt "$i" ]; then
            provider_patches_log "$provider patch ${declared[$i]} already applied"
        else
            provider_apply_patch "$provider" "$patch_dir" "$dir" "${declared[$i]}"
        fi
    done
}

# provider_applied_prefix <patch-dir> <checkout> <patch names...>
# Prints k when the first k patches are applied in <checkout> and the
# remaining ones apply to it in order, preferring the largest k; prints -1
# when no such k exists. The trial runs on a scratch copy of the files the
# patches touch, so <checkout> is never modified here (and nested
# repositories such as picotls' bundled dependencies behave as in the real
# apply).
#
# Runs in its own subshell: its EXIT trap removes only the scratch directory
# this call created and leaves the caller's traps alone. Every preparation
# step is checked explicitly (command substitution does not keep errexit);
# a failure returns nonzero with no output instead of a trial result, and
# nothing outside the verified scratch directory is touched.
provider_applied_prefix() (
    patch_dir=$1
    dir=$2
    shift 2
    names=("$@")
    patches=()
    paths=()
    for ((i=0; i<${#names[@]}; ++i)); do
        patches+=("$patch_dir/${names[$i]}")
    done
    scratch=$(mktemp -d "${TMPDIR:-/tmp}/moq-patch-trial.XXXXXXXX") || exit 2
    # Only a directory this call just created, and owns, is ever a target.
    case "$scratch" in
        /*/moq-patch-trial.*) ;;
        *) exit 2 ;;
    esac
    [ -d "$scratch" ] && [ ! -L "$scratch" ] && [ -O "$scratch" ] || exit 2
    trap 'rm -rf -- "$scratch"' EXIT

    sed -nE -e 's#^--- a/(.*)$#\1#p' -e 's#^\+\+\+ b/(.*)$#\1#p' \
        -e 's#^rename (from|to) (.*)$#\2#p' "${patches[@]}" > "$scratch/paths.raw" || exit 2
    sort -u "$scratch/paths.raw" > "$scratch/paths" || exit 2
    while IFS= read -r path; do
        [ -n "$path" ] || continue
        # A patch path stays inside the tree: relative, without "..".
        case "/$path/" in
            //*|*/../*) exit 2 ;;
        esac
        paths+=("$path")
    done < "$scratch/paths"

    # A fresh copy of the touched files as they are in <dir>.
    trial_copy() {
        rm -rf -- "$scratch/tree" || return 1
        mkdir -- "$scratch/tree" || return 1
        git -C "$scratch/tree" init -q || return 1
        for path in "${paths[@]}"; do
            if [ -e "$dir/$path" ]; then
                mkdir -p -- "$scratch/tree/$(dirname -- "$path")" || return 1
                cp -p -- "$dir/$path" "$scratch/tree/$path" || return 1
            fi
        done
    }
    for ((k=${#names[@]}; k>=0; --k)); do
        # The first k revert in reverse order, and, separately, the rest
        # apply in order to the tree as it is.
        ok=1
        trial_copy || exit 2
        for ((i=k-1; i>=0 && ok; --i)); do
            git -C "$scratch/tree" apply --reverse "${patches[$i]}" >/dev/null 2>&1 || ok=0
        done
        if [ "$ok" = 1 ]; then
            trial_copy || exit 2
            for ((i=k; i<${#names[@]} && ok; ++i)); do
                git -C "$scratch/tree" apply "${patches[$i]}" >/dev/null 2>&1 || ok=0
            done
        fi
        if [ "$ok" = 1 ]; then
            printf '%s\n' "$k"
            exit 0
        fi
    done
    printf '%s\n' -1
)

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    set -euo pipefail
    [ "$#" -eq 3 ] && [ "$1" = apply ] \
        || provider_patches_die "usage: $0 apply <patches/provider-dir> <provider-checkout>"
    command -v git >/dev/null 2>&1 || provider_patches_die "git not found"
    patch_dir=$(cd "$2" && pwd) || provider_patches_die "no patch directory: $2"
    names=()
    while IFS= read -r name; do
        names+=("$name")
    done < <(read_patch_series "$patch_dir/series")
    [ "${#names[@]}" -gt 0 ] || provider_patches_die "empty patch series: $patch_dir/series"
    provider_apply_series "${patch_dir##*/}" "$patch_dir" "$3" "${names[@]}"
fi
