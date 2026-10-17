#!/usr/bin/env bash
# Downloads the full Temporal server release (the same server version the CLI's dev server
# embeds) into bin/temporal-server/<os>-<arch>/ and checks it against the release checksums.
# tests/test_temporal_mtls.py needs it: `temporal server start-dev` has no TLS options.
set -euo pipefail
cd "$(dirname "$0")/.."

version=${TEMPORAL_SERVER_VERSION:-1.32.0}
os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case $arch in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; esac
asset="temporal_${version}_${os}_${arch}.tar.gz"
base="https://github.com/temporalio/temporal/releases/download/v${version}"

dir=bin/temporal-server/$os-$arch
mkdir -p "$dir"
curl -fsSL -o "$dir/$asset" "$base/$asset"
curl -fsSL -o "$dir/checksums.txt" "$base/checksums.txt"
sha256=(shasum -a 256)
command -v sha256sum >/dev/null && sha256=(sha256sum)
(cd "$dir" && grep " $asset\$" checksums.txt | "${sha256[@]}" -c -)
tar -xzf "$dir/$asset" -C "$dir" temporal-server
rm "$dir/$asset"
"$dir/temporal-server" --version
