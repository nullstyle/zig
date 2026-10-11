#!/usr/bin/env bash
# Validate all native outputs before creating an immutable fork release.
set -euo pipefail

dist=${1:?usage: publish-release.sh <dist-dir>}
[ "$#" -eq 1 ] || { echo "zigfork: unexpected publish arguments" >&2; exit 1; }
manifest=.github/zigfork/manifest.txt
die() { echo "zigfork: $*" >&2; exit 1; }
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}
[ -f "$manifest" ] || die "$manifest missing"
[ -d "$dist" ] || die "artifact directory missing"

config=$(awk '$1 == "config" { print $2 }' "$manifest")
case "$config" in ''|*[!A-Za-z0-9_-]*) die "invalid config name" ;; esac
base_sha=$(awk '$1 == "base" { print $2 }' "$manifest")
base_desc=$(awk '$1 == "base" { print $3 }' "$manifest")
tooling_sha=$(awk '$1 == "tooling" { print $2 }' "$manifest")
sha=${GITHUB_SHA:?}
repo=${GITHUB_REPOSITORY:?}
for value in "$base_sha" "$tooling_sha" "$sha"; do
  printf '%s\n' "$value" | grep -Eq '^[0-9a-f]{40}$' || die "manifest/build commit is invalid"
done
[ "$(git rev-parse HEAD)" = "$sha" ] || die "checkout differs from the requested build commit"

# Require the complete native matrix, with exactly one common compiler version.
targets=(aarch64-macos x86_64-linux aarch64-linux)
version=""
assets=()
for target in "${targets[@]}"; do
  [ -s "$dist/version-$target.txt" ] || die "missing native version for $target"
  target_version=$(cat "$dist/version-$target.txt")
  printf '%s\n' "$target_version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-+][A-Za-z0-9.+-]+)?$' || die "invalid compiler version for $target"
  [ -n "$version" ] || version=$target_version
  [ "$target_version" = "$version" ] || die "native versions disagree"
  asset="zig-$target-$version.tar.xz"
  [ -s "$dist/$asset" ] || die "missing native archive $asset"
  assets+=("$asset")
done
release_version=$(awk '$1 == "release_version" {print $2}' "$manifest")
[ -z "$release_version" ] || [ "$version" = "$release_version+fork.${sha:0:9}" ] || die "stable compiler version disagrees with manifest"
wasm_status=absent
wasm_asset="zig-wasm32-wasi-$version.tar.xz"
if [ -s "$dist/$wasm_asset" ]; then
  assets+=("$wasm_asset")
  wasm_status=built
fi
# Reject stale or mismatched tarballs instead of silently mixing build outputs.
for file in "$dist"/*.tar.xz; do
  [ -f "$file" ] || die "no archives"
  found=false
  for asset in "${assets[@]}"; do [ "$(basename "$file")" != "$asset" ] || found=true; done
  [ "$found" = true ] || die "unexpected archive $(basename "$file")"
done

cp "$manifest" "$dist/release-manifest.txt"
printf 'build_sha %s\nversion %s\nwasm %s\n' "$sha" "$version" "$wasm_status" >> "$dist/release-manifest.txt"
: > "$dist/SHA256SUMS"
for asset in "${assets[@]}"; do
  digest=$(sha256 "$dist/$asset")
  printf '%s  %s\n' "$digest" "$asset" >> "$dist/SHA256SUMS"
  target=${asset#zig-}; target=${target%-"$version".tar.xz}
  printf 'asset %s %s %s\n' "$target" "$asset" "$digest" >> "$dist/release-manifest.txt"
done
printf '%s  release-manifest.txt\n' "$(sha256 "$dist/release-manifest.txt")" >> "$dist/SHA256SUMS"

tag="$config/$(date -u +%Y%m%d-%H%M)-${sha:0:7}"
title="zig $version [$config]"
notes=$(mktemp)
trap 'rm -f "$notes"' EXIT
{
  echo "Fork build of Zig, configuration **$config**."
  echo
  echo "| | |"
  echo "|---|---|"
  echo "| \`zig version\` | \`$version\` |"
  echo "| build commit | [\`${sha:0:9}\`](https://github.com/$repo/commit/$sha) |"
  echo "| upstream base | [\`${base_sha:0:9}\`](https://codeberg.org/ziglang/zig/commit/$base_sha) (\`$base_desc\`) |"
  echo "| tooling commit | \`$tooling_sha\` |"
  echo "| wasm cross-build | $wasm_status (no native socket tests) |"
  echo
  echo "### Feature branches"
  awk -v repo="$repo" '$1 == "branch" {
    printf "- `feat/%s`: %s commits ([`%s`](https://github.com/%s/commit/%s))\n", $2, $4, substr($3, 1, 9), repo, $3
  }' "$manifest"
  echo
  echo "### Assets"
  for asset in "${assets[@]}"; do echo "- \`$asset\`"; done
  echo
  echo "SHA256SUMS and release-manifest.txt record exact binaries and source provenance."
  echo
  echo "Install this release with \`zigfork install $config --tag $tag\`."
} > "$notes"

uploads=("$dist/SHA256SUMS" "$dist/release-manifest.txt")
for asset in "${assets[@]}"; do uploads+=("$dist/$asset"); done
gh release create "$tag" --repo "$repo" --target "$sha" --title "$title" \
  --notes-file "$notes" --prerelease "${uploads[@]}"
echo "zigfork: released https://github.com/$repo/releases/tag/$tag"
