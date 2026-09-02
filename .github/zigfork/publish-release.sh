#!/usr/bin/env bash
# zigfork: create one GitHub release from the build artifacts (release job).
#
# Tag format: <config>/<yyyymmdd-hhmm>-<sha7>. It has no dots on purpose:
# Zig computes its version with `git describe --match '*.*.*'`, and a tag with
# dots would be picked up as a version tag.
set -euo pipefail

dist=${1:?usage: publish-release.sh <dist-dir>}
manifest=.github/zigfork/manifest.txt
[ -f "$manifest" ] || { echo "zigfork: $manifest missing" >&2; exit 1; }

config=$(awk '$1 == "config" { print $2 }' "$manifest")
base_sha=$(awk '$1 == "base" { print $2 }' "$manifest")
base_desc=$(awk '$1 == "base" { print $3 }' "$manifest")
version=$(cat "$dist"/version-*.txt | sort -u | head -1)
sha=${GITHUB_SHA:?}
repo=${GITHUB_REPOSITORY:?}
tag="$config/$(date -u +%Y%m%d-%H%M)-${sha:0:7}"
title="zig $version [$config]"

notes=$(mktemp)
{
  echo "Fork build of Zig, configuration **$config**."
  echo
  echo "| | |"
  echo "|---|---|"
  echo "| \`zig version\` | \`$version\` |"
  echo "| build commit | [\`${sha:0:9}\`](https://github.com/$repo/commit/$sha) (branch \`build/$config\`) |"
  echo "| upstream base | [\`${base_sha:0:9}\`](https://codeberg.org/ziglang/zig/commit/$base_sha) (\`$base_desc\`) |"
  echo
  echo "### Feature branches"
  if awk '$1 == "branch"' "$manifest" | grep -q .; then
    awk -v repo="$repo" '$1 == "branch" {
      printf "- `feat/%s`: %s commits ([`%s`](https://github.com/%s/commit/%s))\n", $2, $4, substr($3, 1, 9), repo, $3
    }' "$manifest"
  else
    echo "- none: this is pure upstream"
  fi
  echo
  echo "### Assets"
  for f in "$dist"/*.tar.xz; do
    echo "- \`$(basename "$f")\`"
  done
  echo
  echo "Install on this machine with \`zigfork install $config\`."
} > "$notes"
cat "$notes"

gh release create "$tag" \
  --repo "$repo" \
  --target "$sha" \
  --title "$title" \
  --notes-file "$notes" \
  --prerelease \
  "$dist"/*.tar.xz
echo "zigfork: released https://github.com/$repo/releases/tag/$tag"
