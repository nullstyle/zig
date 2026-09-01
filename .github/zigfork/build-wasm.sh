#!/usr/bin/env bash
# zigfork: cross-compile the Zig compiler to wasm32-wasi.
#
# Run after build-release.sh in the same checkout: it uses that stage3 compiler.
# The wasm compiler has no LLVM backend inside; it uses Zig's own backends.
# Run it with a WASI runtime, e.g.:  wasmtime --dir . zig.wasm build-exe hello.zig
set -euo pipefail

ROOT=$PWD
STAGE3="$ROOT/build-release/stage3-release/bin/zig"
[ -x "$STAGE3" ] || { echo "zigfork: run build-release.sh first" >&2; exit 1; }

export ZIG_GLOBAL_CACHE_DIR="$ROOT/zig-global-cache"
export ZIG_LOCAL_CACHE_DIR="$ROOT/zig-local-cache"
export ZIG_LIB_DIR="$ROOT/lib"

MAXRSS=${ZIGFORK_MAXRSS:-8000000000}   # see build-release.sh
VERSION=$("$STAGE3" version)
NAME="zig-wasm32-wasi-$VERSION"
OUT="$ROOT/out"
mkdir -p "$OUT"

echo "zigfork: building $NAME"
"$STAGE3" build \
  --maxrss "$MAXRSS" \
  --prefix "$OUT/$NAME" \
  -Dflat \
  -Doptimize=ReleaseSmall \
  -Dstrip \
  -Dtarget=wasm32-wasi \
  -Dcpu=generic \
  -Dversion-string="$VERSION"

ls -la "$OUT/$NAME"
[ -f "$OUT/$NAME/zig.wasm" ] || { echo "zigfork: zig.wasm was not produced" >&2; exit 1; }

cd "$OUT"
XZ_OPT=-T0 tar -cJf "$NAME.tar.xz" "$NAME"
rm -rf "$NAME"
