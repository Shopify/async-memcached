#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
prefix="$root/target/test-tools"
mkdir -p "$prefix/downloads" "$prefix/build" "$prefix/bin"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    platform=linux-amd64
    toxi_sha=396d318e4c0b2703904edd112b6decaec5a61643ef172fdaf696fababe09fad3
    ;;
  Linux-aarch64)
    platform=linux-arm64
    toxi_sha=862228f3f4c440e8caad1b174a974698363091aec4f0f7237f61a0f290224962
    ;;
  Darwin-x86_64)
    platform=darwin-amd64
    toxi_sha=dd730397b252243aa196bdf0db8aacf73ed7c35a6386c46677da228c31652549
    ;;
  Darwin-arm64)
    platform=darwin-arm64
    toxi_sha=fb9085e232ffe7bfdae3a0be8da5f041cc66fb75740ec173173b5b3cc1b35750
    ;;
  *) printf 'Unsupported platform. See TESTING.md.\n' >&2; exit 1 ;;
esac

fetch() {
  local url="$1" file="$2" sha="$3"
  if [[ ! -f "$file" ]]; then
    curl --fail --location --retry 3 --connect-timeout 15 --max-time 120 "$url" -o "$file"
  fi
  printf '%s  %s\n' "$sha" "$file" | shasum -a 256 --check -
}

fetch https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz \
  "$prefix/downloads/libevent-2.1.12-stable.tar.gz" \
  92e6de1be9ec176428fd2367677e61ceffc2ee1cb119035037a27d346b0403bb
fetch https://memcached.org/files/memcached-1.6.41.tar.gz \
  "$prefix/downloads/memcached-1.6.41.tar.gz" \
  e097073c156eeff9e12655b054f446d57374cfba5c132dcdbe7fac64e728286a
fetch "https://github.com/Shopify/toxiproxy/releases/download/v2.11.0/toxiproxy-server-$platform" \
  "$prefix/downloads/toxiproxy-server-$platform" "$toxi_sha"

jobs="${TEST_BUILD_JOBS:-4}"
(
  cd "$prefix/build"
  tar -xzf "$prefix/downloads/libevent-2.1.12-stable.tar.gz"
  cd libevent-2.1.12-stable
  ./configure --prefix="$prefix" --disable-shared --disable-openssl \
    --disable-samples --disable-libevent-regress
  make -j "$jobs"
  make install
)
(
  cd "$prefix/build"
  tar -xzf "$prefix/downloads/memcached-1.6.41.tar.gz"
  cd memcached-1.6.41
  ./configure --prefix="$prefix" --with-libevent="$prefix"
  make -j "$jobs" memcached
  install -m 755 memcached "$prefix/bin/memcached"
)
install -m 755 "$prefix/downloads/toxiproxy-server-$platform" "$prefix/bin/toxiproxy-server"
printf '\nTest tools installed in %s/bin\n' "$prefix"
