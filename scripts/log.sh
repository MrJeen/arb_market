#!/usr/bin/env bash
# 跟踪 market-arb journal。可加 journalctl 参数，例如: ./scripts/log.sh --since today
set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
  exec journalctl -u "$SERVICE" -f "$@"
fi
exec sudo journalctl -u "$SERVICE" -f "$@"
