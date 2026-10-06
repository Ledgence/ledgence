#!/bin/sh
# Copyright Ledgence contributors. Licensed under the MIT License.
# Install the complete native release bundle; no Python, Rust or Docker required.
# Parse the complete installer before executing it when downloaded through a pipe.
main() {
set -eu
LC_ALL=C
export LC_ALL

fail() { printf 'ledgence installer: %s\n' "$*" >&2; exit 1; }
usage() {
    cat <<'USAGE'
Install the Ledgence CLI and its bundled resources.

Usage: sh install.sh [--version X.Y.Z] [--prefix DIRECTORY] [--base-url HTTPS_URL]

  --version VERSION   Install a stable version (default: latest stable release).
  --prefix DIRECTORY  Install below DIRECTORY (default: $HOME/.local).
  --base-url URL      Release endpoint (default: https://github.com/Ledgence/ledgence/releases).
  --no-modify-path    Do not configure shell startup files (useful in CI).
  --help              Print this help.

Supported hosts: Linux x86_64 with compatible glibc libraries; macOS Apple silicon.
Bundles are retained in PREFIX/share/ledgence/versions and selected by PREFIX/bin/ledgence.
No sudo is used. Existing installations survive download or verification failures.
Shell setup: Bash, Zsh, and POSIX login shells. Source PREFIX/share/ledgence/env
to make the CLI available in the current shell, too.
USAGE
}
version=
prefix=
base_url=https://github.com/Ledgence/ledgence/releases
modify_path=true
while [ "$#" -gt 0 ]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        --no-modify-path) modify_path=false; shift ;;
        --version|--prefix|--base-url)
            option=$1
            [ "$#" -ge 2 ] && [ -n "$2" ] || fail "$option requires a value"
            case "$option" in
                --version) version=$2 ;;
                --prefix) prefix=$2 ;;
                --base-url) base_url=$2 ;;
            esac
            shift 2 ;;
        *) fail "unknown option: $1 (see --help)" ;;
    esac
done
[ -n "$prefix" ] || prefix=${HOME:?HOME must be set; otherwise supply --prefix}/.local
case "$prefix" in /*) ;; *) fail "--prefix must be an absolute directory" ;; esac
case "$prefix" in *'
'*|*"$(printf '\r')"*) fail "--prefix cannot contain line breaks" ;; esac
# A colon cannot be represented as one directory in PATH.
case "$prefix" in *:*) fail "--prefix cannot contain ':'" ;; esac
case "$base_url" in https://?*) ;; *) fail "--base-url must use HTTPS" ;; esac
case "$base_url" in *[!a-zA-Z0-9:/._~-]*) fail "--base-url contains unsupported characters" ;; esac
base_url=${base_url%/}

for command in curl tar uname awk sed sort find cmp mktemp mkdir mv rm rmdir ln readlink; do
    command -v "$command" >/dev/null 2>&1 || fail "required command not found: $command"
done
if command -v sha256sum >/dev/null 2>&1; then
    checksum_tool=sha256sum
elif command -v shasum >/dev/null 2>&1; then
    checksum_tool=shasum
else
    fail "SHA-256 verification requires sha256sum or shasum"
fi
sha256() {
    if [ "$checksum_tool" = sha256sum ]; then sha256sum < "$1"; else shasum -a 256 < "$1"; fi
}
download() {
    curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
        --connect-timeout 20 --max-time 600 --retry 3 --output "$2" "$1"
}

os=$(uname -s)
arch=$(uname -m)
case "$os/$arch" in
    Darwin/arm64|Darwin/aarch64) target=aarch64-apple-darwin ;;
    Linux/x86_64|Linux/amd64)
        target=x86_64-unknown-linux-gnu
        if ! command -v getconf >/dev/null 2>&1 || ! getconf GNU_LIBC_VERSION >/dev/null 2>&1; then
            fail "Linux requires glibc; musl/Alpine is not supported by the native bundle"
        fi
        ;;
    *) fail "unsupported host: $os/$arch; use the documented container or source installation" ;;
esac

if [ -z "$version" ]; then
    latest=$(curl --fail --silent --show-error --location --head --proto '=https' \
        --proto-redir '=https' --connect-timeout 20 --max-time 60 --retry 3 \
        --output /dev/null --write-out '%{url_effective}' "$base_url/latest") ||
        fail "cannot resolve the latest stable release; try --version X.Y.Z"
    case "$latest" in
        "$base_url"/tag/v*) version=${latest#"$base_url"/tag/v} ;;
        *) fail "unexpected latest-release redirect; specify --version X.Y.Z" ;;
    esac
fi
version=${version#v}
printf '%s\n' "$version" | awk '
    /^[0-9]+\.[0-9]+\.[0-9]+$/ {
        split($0, parts, ".")
        for (i=1; i<=3; i++) if (length(parts[i]) > 1 && substr(parts[i],1,1) == "0") exit 1
        valid=1
    }
    END { if (!valid) exit 1 }
' || fail "--version must be a stable X.Y.Z version"

# Reserve a private staging directory on the same filesystem as the final bundle.
# The lock also serializes two installers choosing different versions for one prefix.
umask 022
requested_bin=$prefix/bin
mkdir -p "$prefix"
prefix=$(cd "$prefix" && pwd -P)
install_root=$prefix/share/ledgence
versions=$install_root/versions
bin_dir=$prefix/bin
mkdir -p "$versions" "$bin_dir"
lock=$install_root/.install-lock
mkdir "$lock" 2>/dev/null || fail "another install is running, or a stale lock exists: $lock"
temporary=
pending_link=
cleanup() {
    [ -z "$pending_link" ] || rm -f "$pending_link"
    [ -z "$temporary" ] || rm -rf "$temporary"
    rmdir "$lock" 2>/dev/null || :
}
trap cleanup 0
trap 'exit 1' HUP INT TERM
temporary=$(mktemp -d "$install_root/.install.XXXXXXXX")
label=ledgence-$version-$target
destination=$versions/$version-$target
executable=$bin_dir/ledgence

# Never replace a user's unrelated executable, directory, or link.
previous=
if [ -L "$executable" ]; then
    previous=$(readlink "$executable")
    case "$previous" in
        "$versions"/*/bin/ledgence)
            previous_bundle=${previous%/bin/ledgence}
            relative_bundle=${previous_bundle#"$versions"/}
            case "$relative_bundle" in */*|.|..) fail "refusing an unmanaged launcher target" ;; esac
            [ ! -L "$previous_bundle" ] && [ -f "$previous_bundle/SHA256SUMS" ] ||
                fail "existing ledgence link does not point to an installed bundle: $executable"
            ;;
        *) fail "refusing to replace an unmanaged link: $executable" ;;
    esac
elif [ -e "$executable" ]; then
    fail "refusing to replace an unmanaged path: $executable"
fi

printf 'Downloading Ledgence %s for %s…\n' "$version" "$target"
archive=$label.tar.gz
download "$base_url/download/v$version/SHA256SUMS" "$temporary/release-SHA256SUMS" ||
    fail "could not download release checksums"
download "$base_url/download/v$version/$archive" "$temporary/$archive" ||
    fail "could not download the native bundle"
expected=$(awk -v name="$archive" '
    ($2 == name || $2 == "*" name) && NF == 2 { count++; hash=$1 }
    END { if (count != 1 || length(hash) != 64 || hash ~ /[^0-9a-f]/) exit 1; print hash }
' "$temporary/release-SHA256SUMS") || fail "expected exactly one checksum for $archive"
actual=$(sha256 "$temporary/$archive" | awk '{ print $1 }')
[ "$actual" = "$expected" ] || fail "downloaded archive checksum mismatch"

# Our release format has one root and only regular files/directories. Validate
# before extraction; links and path traversal are not part of the bundle format.
tar -tzf "$temporary/$archive" > "$temporary/archive-paths" || fail "cannot inspect archive"
awk -v root="$label" '
    {
        path=$0; sub(/\/$/, "", path)
        if (path !~ /^[a-zA-Z0-9_.+@\/-]+$/ || seen[path]++) exit 1
        n=split(path, parts, "/")
        if (parts[1] != root) exit 1
        for (i=1; i<=n; i++) if (parts[i] == "" || parts[i] == "." || parts[i] == "..") exit 1
        count++
    }
    END { if (!count) exit 1 }
' "$temporary/archive-paths" || fail "archive contains invalid paths"
tar -tvzf "$temporary/$archive" > "$temporary/archive-types" || fail "cannot inspect archive types"
awk 'substr($0,1,1) != "-" && substr($0,1,1) != "d" { exit 1 }' \
    "$temporary/archive-types" || fail "archive contains unsupported links or special files"
mkdir "$temporary/extracted"
tar -xzf "$temporary/$archive" -C "$temporary/extracted" || fail "cannot extract archive"
bundle=$temporary/extracted/$label

verify_bundle() (
    cd "$1" || exit 1
    [ -f SHA256SUMS ] && [ ! -L SHA256SUMS ] || exit 1
    # Checking the inventory as well as individual checksums detects missing and
    # unlisted files in an existing installation, including dangling symlinks.
    find . ! -type f ! -type d > "$temporary/invalid-files"
    [ ! -s "$temporary/invalid-files" ] || exit 1
    awk '
        {
            if (length($1) != 64 || $1 ~ /[^0-9a-f]/ || NF != 2) exit 1
            name=$2
            if (name !~ /^[a-zA-Z0-9_.+@\/-]+$/ || name == "SHA256SUMS" || seen[name]++) exit 1
            n=split(name, parts, "/")
            for (i=1; i<=n; i++) if (parts[i] == "" || parts[i] == "." || parts[i] == "..") exit 1
            print name; count++
        }
        END { if (!count) exit 1 }
    ' SHA256SUMS > "$temporary/expected-files" || exit 1
    sort "$temporary/expected-files" > "$temporary/expected-sorted"
    find . -type f ! -path ./SHA256SUMS | sed 's|^./||' | sort > "$temporary/actual-files"
    cmp -s "$temporary/expected-sorted" "$temporary/actual-files" || exit 1
    if [ "$checksum_tool" = sha256sum ]; then
        sha256sum -c SHA256SUMS > "$temporary/verification.log" 2>&1 || exit 1
    else
        shasum -a 256 -c SHA256SUMS > "$temporary/verification.log" 2>&1 || exit 1
    fi
    [ -x bin/ledgence ] && [ -f LICENSE ] && [ -d legal ] &&
        [ -f runtime/ledgence/worker/bootstrap.py ] && [ -f console/index.html ] && [ -f provenance.json ]
)
verify_bundle "$bundle" || fail "bundle checksum inventory or required resources are invalid"
reported_version=$("$bundle/bin/ledgence" --version 2>"$temporary/startup-error") ||
    fail "the downloaded CLI cannot run on this host; check the release's OS and library requirements"
[ "$reported_version" = "ledgence $version" ] || fail "bundle executable reports an unexpected version"

if [ -e "$destination" ] || [ -L "$destination" ]; then
    if [ ! -d "$destination" ] || [ -L "$destination" ] ||
        ! verify_bundle "$destination" || ! cmp -s "$bundle/SHA256SUMS" "$destination/SHA256SUMS"; then
        fail "existing version differs or is damaged; preserved without replacement: $destination"
    fi
else
    mv "$bundle" "$destination"
fi

# This file is stable across upgrades and can also be sourced by the invoking
# shell. Quote paths as shell literals; custom prefixes may contain metacharacters.
quoted_bin=$(printf '%s' "$bin_dir" | sed "s/'/'\\\\''/g")
environment=$install_root/env
quoted_environment=$(printf '%s' "$environment" | sed "s/'/'\\\\''/g")
{
    printf '%s\n' '# Ledgence CLI shell environment. Generated by install.sh.'
    printf "_ledgence_path_next='%s'\n" "$quoted_bin"
    # Parse components without word splitting or glob expansion. Retain other
    # entries, including intentional empty components, and remove our duplicates.
    # shellcheck disable=SC2016
    printf '%s\n' \
        'if [ -n "${PATH-}" ]; then' \
        '    _ledgence_path_remaining=$PATH:' \
        '    while [ -n "$_ledgence_path_remaining" ]; do' \
        '        _ledgence_path_entry=${_ledgence_path_remaining%%:*}' \
        '        _ledgence_path_remaining=${_ledgence_path_remaining#*:}'
    printf "        if [ \"\$_ledgence_path_entry\" != '%s' ]; then\n" "$quoted_bin"
    # shellcheck disable=SC2016
    printf '%s\n' \
        '            _ledgence_path_next=$_ledgence_path_next:$_ledgence_path_entry' \
        '        fi' \
        '    done' \
        'fi' \
        'export PATH="$_ledgence_path_next"' \
        'unset _ledgence_path_next _ledgence_path_remaining _ledgence_path_entry'
} > "$temporary/path-env"
if [ -e "$environment" ] || [ -L "$environment" ]; then
    if [ ! -f "$environment" ] || [ -L "$environment" ] ||
        ! cmp -s "$temporary/path-env" "$environment"; then
        fail "existing shell environment differs; preserved without replacement: $environment"
    fi
else
    # Hard-link creation is exclusive, so a concurrently created file is preserved.
    ln "$temporary/path-env" "$environment" ||
        fail "could not create shell environment; existing path preserved: $environment"
fi

# mv replaces a symlink atomically. Reject symlinks to directories, for which
# portable mv may otherwise follow the link instead of replacing it.
[ ! -d "$executable" ] || fail "refusing to replace a directory: $executable"
if [ -z "$previous" ]; then
    # Initial activation must not overwrite a file created while downloading.
    ln -s "$destination/bin/ledgence" "$executable" ||
        fail "launcher appeared during installation; preserved without replacement: $executable"
else
    candidate_link=$bin_dir/.ledgence-install-$$
    ln -s "$destination/bin/ledgence" "$candidate_link" || fail "cannot create the temporary launcher"
    pending_link=$candidate_link
    # Cooperating installers use the lock. Also detect a user's concurrent edit
    # before replacing the managed symlink.
    [ -L "$executable" ] && [ "$(readlink "$executable")" = "$previous" ] ||
        fail "launcher changed during installation; preserved without replacement: $executable"
    mv -f "$pending_link" "$executable"
    pending_link=
fi
printf 'Installed Ledgence %s: %s\n' "$version" "$executable"
printf 'Bundle resources: %s\n' "$destination"

configure_profile() {
    profile=$1
    # Dotfiles are often symlinks. Append through a valid one without replacing
    # it, but never read a FIFO, dangling link, directory, or unreadable profile.
    if [ -e "$profile" ] || [ -L "$profile" ]; then
        if [ ! -f "$profile" ] || [ ! -r "$profile" ]; then
            printf 'Could not configure PATH in %s; profile preserved. Configure it manually.\n' "$profile" >&2
            return
        fi
        while IFS= read -r profile_line || [ -n "$profile_line" ]; do
            if [ "$profile_line" = "$source_line" ]; then
                printf 'PATH already configured in %s\n' "$profile"
                return
            fi
        done < "$profile"
        if [ ! -w "$profile" ]; then
            printf 'Could not configure PATH in %s; profile preserved. Configure it manually.\n' "$profile" >&2
            return
        fi
    fi
    if ! mkdir -p "${profile%/*}" ||
        ! (printf '\n# Ledgence CLI\n%s\n' "$source_line" >> "$profile"); then
        printf 'Could not configure PATH in %s. Configure it manually.\n' "$profile" >&2
        return
    fi
    printf 'Configured PATH in %s\n' "$profile"
}

configure_shell() {
    source_line="[ ! -r '$quoted_environment' ] || . '$quoted_environment'"
    case "${HOME-}" in
        /*) ;;
        *) printf 'HOME is unavailable; configure your shell PATH manually.\n' >&2; return ;;
    esac
    shell=${SHELL:-sh}
    case "${shell##*/}" in
        bash)
            configure_profile "$HOME/.bashrc"
            # Bash reads only the first existing readable login profile.
            # Do not create .bash_profile and shadow an existing .profile.
            login_profile=$HOME/.profile
            for candidate in "$HOME/.bash_profile" "$HOME/.bash_login" "$HOME/.profile"; do
                if [ -f "$candidate" ] && [ -r "$candidate" ]; then
                    login_profile=$candidate
                    break
                fi
            done
            configure_profile "$login_profile"
            ;;
        zsh)
            zsh_directory=${ZDOTDIR:-$HOME}
            case "$zsh_directory" in
                /*) configure_profile "$zsh_directory/.zshrc" ;;
                *) printf 'ZDOTDIR must be absolute for automatic shell setup; configure PATH manually.\n' >&2 ;;
            esac
            ;;
        sh|dash|ksh) configure_profile "$HOME/.profile" ;;
        *) printf 'Automatic PATH setup is unavailable for %s. Add %s to your shell PATH manually.\n' "$shell" "$bin_dir" >&2 ;;
    esac
}

if [ "$modify_path" = true ]; then
    configure_shell
else
    printf 'Shell startup files left unchanged (--no-modify-path).\n'
fi
selected=$(command -v ledgence || :)
case "$selected" in
    "$bin_dir/ledgence"|"$requested_bin/ledgence") printf 'Run: ledgence --help\n' ;;
    *)
        if [ -n "$selected" ]; then
            printf 'Another ledgence currently takes precedence on PATH: %s\n' "$selected"
        fi
        printf "\nActivate Ledgence in the current Bash, Zsh, or POSIX shell:\n  . '%s'\n" "$quoted_environment"
        printf 'Then run: ledgence --help\n'
        ;;
esac
}

main "$@"
