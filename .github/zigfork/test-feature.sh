#!/usr/bin/env bash
# Run the fork's native regressions from the root of the Zig checkout.
#   test-feature.sh <target> <zig> <zig-lib-dir>
# A release passes its flat install's zig and lib/. A no-lib developer build
# must pass the checkout's lib/ explicitly.
set -euo pipefail

target=${1:?usage: test-feature.sh <target> <zig> <zig-lib-dir>}
compiler=${2:?usage: test-feature.sh <target> <zig> <zig-lib-dir>}
zig_lib=${3:?usage: test-feature.sh <target> <zig> <zig-lib-dir>}
[ "$#" -eq 3 ] || { echo "zigfork: unexpected test-feature.sh arguments" >&2; exit 1; }
[ -x "$compiler" ] || { echo "zigfork: compiler not executable: $compiler" >&2; exit 1; }
[ -f "$zig_lib/std/std.zig" ] || { echo "zigfork: standard library not found: $zig_lib" >&2; exit 1; }
[ -d test/cases ] || { echo "zigfork: run feature tests from the Zig checkout root" >&2; exit 1; }

case "$target" in
  aarch64-macos|x86_64-macos) platform=macos ;;
  aarch64-linux|x86_64-linux) platform=linux ;;
  aarch64-freebsd|x86_64-freebsd|aarch64-openbsd|x86_64-openbsd) platform=bsd ;;
  *) echo "zigfork: unsupported native test target: $target" >&2; exit 1 ;;
esac

# The release manifest identifies the feature even if a case was accidentally
# removed. Source sentinels also support testing a local feature checkout.
feature_present=false
if [ -f .github/zigfork/manifest.txt ]; then
  while read -r kind name rest; do
    if [ "$kind" = branch ] && { [ "$name" = evented-dispatch ] || [ "$name" = evented-0.17 ]; }; then feature_present=true; fi
  done < .github/zigfork/manifest.txt
fi
if [ -f test/cases/evented_dispatch_net.zig ] ||
   [ -f test/cases/evented_kqueue_net.zig ] ||
   [ -f test/cases/evented_uring_net.zig ]; then
  feature_present=true
fi
if [ "$feature_present" = false ]; then
  echo "zigfork: evented-dispatch is absent; no feature regressions to run"
  exit 0
fi

cases=(aarch64_vector_parts dwarf_unwind_null_page udp_reuse_port_load_balance io_operate_timeout_deadline)
case "$platform" in
  macos)
    cases+=(evented_dispatch_net evented_dispatch_wait evented_dispatch_cancel evented_dispatch_dns)
    cases+=(evented_dispatch_contract evented_dispatch_batch_ownership evented_dispatch_process evented_dispatch_group_reuse)
    # Kqueue also runs on macOS, giving the release a native check even though
    # its primary users are the BSD targets.
    cases+=(evented_kqueue_net evented_kqueue_net_io evented_kqueue_adapters evented_kqueue_cancel)
    ;;
  linux) cases+=(evented_uring_net evented_uring_cancel evented_uring_process evented_uring_fiber_reuse) ;;
  bsd) cases+=(evented_kqueue_net evented_kqueue_net_io evented_kqueue_adapters evented_kqueue_cancel) ;;
esac
case "$platform" in
  macos|linux) cases+=(threaded_udp_batch_submit threaded_udp_reuse_port io_unix_address io_unix_ancillary) ;;
esac
for name in "${cases[@]}"; do
  [ -f "test/cases/$name.zig" ] || { echo "zigfork: required regression missing: $name" >&2; exit 1; }
done
cast_root="$PWD/test/standalone/translate_c_signedness"
for name in casts.h main.zig; do
  [ -f "$cast_root/$name" ] || { echo "zigfork: required regression missing: translate_c_signedness/$name" >&2; exit 1; }
done
if [ "$platform" = linux ]; then
  for declaration in \
    'test "Uring batch timeout terminal errors release their request ownership"' \
    'test "Uring batch timeout acknowledgments preserve both completion orders"'; do
    grep -Fq "$declaration" "$zig_lib/std/Io/Uring.zig" || {
      echo "zigfork: required Uring terminal timer unit test missing" >&2; exit 1;
    }
  done
fi
if [ "$platform" = macos ]; then
  grep -Fq 'test "Dispatch socket wait timeout' "$zig_lib/std/Io/Dispatch.zig" || {
    echo "zigfork: required Dispatch deadline/readiness unit test missing" >&2; exit 1;
  }
  grep -Fq 'test "Dispatch futex timer allocation failure' "$zig_lib/std/Io/Dispatch.zig" || {
    echo "zigfork: required Dispatch injected allocation failure test missing" >&2; exit 1;
  }
fi
grep -Fq 'test "cmsg.Iterator rejects invalid advertised lengths"' "$zig_lib/std/Io/net.zig" || {
  echo "zigfork: required ancillary bounds unit test missing" >&2; exit 1;
}

out="$PWD/build-release/feature-tests/$target"
mkdir -p "$out"
export ZIG_LIB_DIR="$zig_lib"
export ZIG_GLOBAL_CACHE_DIR=${ZIG_GLOBAL_CACHE_DIR:-"$PWD/build-release/zig-global-cache"}
export ZIG_LOCAL_CACHE_DIR=${ZIG_LOCAL_CACHE_DIR:-"$out/cache"}
timeout_seconds=${ZIGFORK_TEST_TIMEOUT:-120}
case "$timeout_seconds" in
  ''|*[!0-9]*|0) echo "zigfork: ZIGFORK_TEST_TIMEOUT must be a positive integer" >&2; exit 1 ;;
esac

# macOS has no stock `timeout` command. Keep the watchdog in portable bash,
# and reap its sleep process when a passing test finishes before the limit.
run_test() {
  "$@" &
  local test_pid=$!
  (
    sleep "$timeout_seconds" &
    sleeper=$!
    trap 'kill "$sleeper" 2>/dev/null || true' EXIT
    wait "$sleeper"
    echo "zigfork: test exceeded ${timeout_seconds}s: $*" >&2
    kill -KILL "$test_pid" 2>/dev/null || true
  ) &
  local watchdog_pid=$!
  local result=0
  wait "$test_pid" || result=$?
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  return "$result"
}

echo "zigfork: $target translate C signedness regression"
"$compiler" translate-c -target "$target" "$cast_root/casts.h" > "$out/translated-casts.zig"

for mode in Debug ReleaseSafe; do
  for name in "${cases[@]}"; do
    echo "zigfork: $target $mode $name"
    "$compiler" build-exe "test/cases/$name.zig" --zig-lib-dir "$zig_lib" \
      -target "$target" -fllvm -lc -O "$mode" -femit-bin="$out/$name-$mode"
    run_test "$out/$name-$mode"
    if [ "$name" = io_unix_ancillary ]; then
      if [ "$platform" = macos ]; then
        run_test "$out/$name-$mode" dispatch
        run_test "$out/$name-$mode" kqueue
      else
        run_test "$out/$name-$mode" uring
      fi
    fi
  done
  echo "zigfork: $target $mode translated C signedness casts"
  "$compiler" build-exe --zig-lib-dir "$zig_lib" -fllvm -lc \
    -target "$target" -O "$mode" --dep c "-Mroot=$cast_root/main.zig" \
    -target "$target" -O "$mode" "-Mc=$out/translated-casts.zig" \
    -femit-bin="$out/translated-casts-$mode"
  run_test "$out/translated-casts-$mode"
  case "$target" in
    aarch64-*)
      echo "zigfork: $target $mode self-hosted vector partitions"
      "$compiler" build-obj test/cases/aarch64_vector_parts.zig --zig-lib-dir "$zig_lib" \
        -target "$target" -fno-llvm -lc -O "$mode" -femit-bin="$out/vector-parts-selfhosted-$mode.o"
      # The C driver links the generated object with LLVM runtime support;
      # this also works with self-hosted objects on macOS.
      "$compiler" cc -target "$target" "$out/vector-parts-selfhosted-$mode.o" \
        -o "$out/vector-parts-selfhosted-$mode"
      run_test "$out/vector-parts-selfhosted-$mode"
      ;;
  esac
  if [ "$platform" = linux ]; then
    echo "zigfork: $target $mode Uring terminal timer error unit regressions"
    "$compiler" test "$zig_lib/std/std.zig" --zig-lib-dir "$zig_lib" \
      -target "$target" -fllvm -lc -O "$mode" --test-filter 'Uring batch timeout' \
      --test-no-exec -femit-bin="$out/uring-timeout-$mode"
    run_test "$out/uring-timeout-$mode"
  fi
  if [ "$platform" = macos ]; then
    echo "zigfork: $target $mode Dispatch deadline and readiness unit regression"
    "$compiler" test "$zig_lib/std/std.zig" --zig-lib-dir "$zig_lib" \
      -target "$target" -fllvm -lc -O "$mode" --test-filter 'Dispatch socket wait timeout' \
      --test-no-exec -femit-bin="$out/dispatch-timeout-$mode"
    run_test "$out/dispatch-timeout-$mode"
    echo "zigfork: $target $mode Dispatch injected futex allocation failure"
    "$compiler" test "$zig_lib/std/std.zig" --zig-lib-dir "$zig_lib" \
      -target "$target" -fllvm -lc -O "$mode" --test-filter 'Dispatch futex timer allocation failure' \
      --test-no-exec -femit-bin="$out/dispatch-futex-failure-$mode"
    run_test "$out/dispatch-futex-failure-$mode"
  fi
  echo "zigfork: $target $mode ancillary advertised-length bounds"
  "$compiler" test "$zig_lib/std/std.zig" --zig-lib-dir "$zig_lib" \
    -target "$target" -fllvm -lc -O "$mode" --test-filter 'cmsg.Iterator rejects' \
    --test-no-exec -femit-bin="$out/cmsg-bounds-$mode"
  run_test "$out/cmsg-bounds-$mode"
done
echo "zigfork: $target feature regressions passed in Debug and ReleaseSafe"
