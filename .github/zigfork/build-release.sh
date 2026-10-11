#!/usr/bin/env bash
# zigfork: build a release tarball of the Zig compiler for one native target.
#
#   build-release.sh <target>                 e.g. aarch64-macos, x86_64-linux, aarch64-linux
#   build-release.sh --bundle-name <target>   only print the zig+llvm bundle name
#
# Follows ci/<target>-release.sh from upstream (same LLVM bundle, same cmake
# flags), runs the fork's focused regressions, and produces a flat install
# tarball in out/. The full upstream test suite is not run here.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

mode=build
if [ "${1:-}" = "--bundle-name" ]; then mode=name; shift; fi
target=${1:?usage: build-release.sh [--bundle-name] <target>}
[ "$#" -eq 1 ] || { echo "zigfork: unexpected build arguments" >&2; exit 1; }
case "$target" in aarch64-macos|x86_64-macos|x86_64-linux|aarch64-linux) ;; *) echo "zigfork: unsupported release target $target" >&2; exit 1 ;; esac

script="ci/$target-release.sh"
[ -f "$script" ] || { echo "zigfork: upstream script $script not found" >&2; exit 1; }

# Take the triple, cpu and prebuilt LLVM bundle from the upstream script so the
# fork always builds with the same LLVM as upstream master.
TRIPLE=$(sed -n 's/^TARGET="\(.*\)"$/\1/p' "$script")
MCPU=$(sed -n 's/^MCPU="\(.*\)"$/\1/p' "$script")
MCPU=${MCPU:-baseline}
BUNDLE=$(sed -n 's/^CACHE_BASENAME="\(.*\)"$/\1/p' "$script")
BUNDLE=${BUNDLE//\$TARGET/$TRIPLE}
[ -n "$TRIPLE" ] && [ -n "$BUNDLE" ] || { echo "zigfork: could not parse $script" >&2; exit 1; }

if [ "$mode" = name ]; then
  echo "$BUNDLE"
  exit 0
fi

# Zig's build runner refuses steps whose declared memory exceeds free RAM.
# The compiler step declares 8 GB; free GitHub macOS runners have 7 GB.
# Tell the runner to assume this much and go ahead.
MAXRSS=${ZIGFORK_MAXRSS:-8000000000}

# Bundle location: ~/deps on GitHub Actions (matches the cache step in release.yml),
# ~/.cache/zigfork/deps on a developer machine.
DEPS=${ZIGFORK_DEPS_DIR:-${GITHUB_ACTIONS:+$HOME/deps}}
DEPS=${DEPS:-$HOME/.cache/zigfork/deps}
PREFIX="$DEPS/$BUNDLE"
ZIG="$PREFIX/bin/zig"
if [ ! -x "$ZIG" ]; then
  mkdir -p "$DEPS"
  echo "zigfork: downloading $BUNDLE.tar.xz"
  curl -fL --retry 3 -o "$DEPS/$BUNDLE.tar.xz" "https://ziglang.org/deps/$BUNDLE.tar.xz"
  tar -xf "$DEPS/$BUNDLE.tar.xz" -C "$DEPS"
  rm -f "$DEPS/$BUNDLE.tar.xz"
fi

# Everything lands under build-release/, which the zig tree's .gitignore ignores.
ROOT=$PWD
VERSION_OVERRIDE=${ZIGFORK_VERSION:-}
manifest="$ROOT/.github/zigfork/manifest.txt"
if [ -f "$manifest" ]; then
  release_version=$(awk '$1 == "release_version" {print $2}' "$manifest")
  if [ -n "$release_version" ]; then
    printf '%s\n' "$release_version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || { echo "zigfork: invalid stable release version" >&2; exit 1; }
    build_sha=$(git rev-parse HEAD)
    expected_version="$release_version+fork.${build_sha:0:9}"
    [ -z "$VERSION_OVERRIDE" ] || [ "$VERSION_OVERRIDE" = "$expected_version" ] || { echo "zigfork: version override disagrees with manifest" >&2; exit 1; }
    VERSION_OVERRIDE=$expected_version
  fi
fi
if [ -n "$VERSION_OVERRIDE" ]; then
  printf '%s\n' "$VERSION_OVERRIDE" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-+][A-Za-z0-9.+-]+)?$' || { echo "zigfork: invalid version override" >&2; exit 1; }
fi
export ZIG_GLOBAL_CACHE_DIR="$ROOT/build-release/zig-global-cache"
export ZIG_LOCAL_CACHE_DIR="$ROOT/build-release/zig-local-cache"
OUT="$ROOT/build-release/out"

echo "zigfork: stage3 for $TRIPLE ($MCPU) with $BUNDLE"
mkdir -p build-release
cd build-release
cmake .. \
  -DCMAKE_INSTALL_PREFIX="stage3-release" \
  -DCMAKE_PREFIX_PATH="$PREFIX" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER="$ZIG;cc;-target;$TRIPLE;-mcpu=$MCPU" \
  -DCMAKE_CXX_COMPILER="$ZIG;c++;-target;$TRIPLE;-mcpu=$MCPU" \
  -DZIG_TARGET_TRIPLE="$TRIPLE" \
  -DZIG_TARGET_MCPU="$MCPU" \
  -DZIG_VERSION="$VERSION_OVERRIDE" \
  -DZIG_STATIC=ON \
  -DZIG_NO_LIB=ON \
  -DZIG_EXTRA_BUILD_ARGS="--maxrss;$MAXRSS" \
  -GNinja
ninja install

# stage3 was built with ZIG_NO_LIB, so point it at the lib/ of this checkout.
export ZIG_LIB_DIR="$ROOT/lib"
VERSION=$(stage3-release/bin/zig version)
NAME="zig-$target-$VERSION"
mkdir -p "$OUT"
rm -rf "${OUT:?}/${NAME:?}"

echo "zigfork: stage4 release $NAME"
stage3-release/bin/zig build \
  --maxrss "$MAXRSS" \
  --prefix "$OUT/$NAME" \
  -Dflat \
  -Denable-llvm \
  -Doptimize=ReleaseFast \
  -Dstrip \
  -Dtarget="$TRIPLE" \
  -Dcpu="$MCPU" \
  -Duse-zig-libcxx \
  -Dversion-string="$VERSION"

# Smoke test: the new compiler must start and compile a program with its own lib/.
unset ZIG_LIB_DIR
"$OUT/$NAME/zig" version
printf 'const std = @import("std");\npub fn main() void { std.debug.print("zigfork smoke ok\\n", .{}); }\n' > "$OUT/smoke.zig"
"$OUT/$NAME/zig" run "$OUT/smoke.zig"
rm -f "$OUT/smoke.zig"

# Exercise the compiler and standard library that will actually be shipped.
# A failure stops this job before packaging or publishing any release.
(
  cd "$ROOT"
  bash "$SCRIPT_DIR/test-feature.sh" "$target" "$OUT/$NAME/zig" "$OUT/$NAME/lib"
)

cd "$OUT"
XZ_OPT=-T0 tar -cJf "$NAME.tar.xz" "$NAME"
rm -rf "$NAME"
echo "$VERSION" > "version-$target.txt"
ls -la "$OUT"
