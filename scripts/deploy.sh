#!/usr/bin/env bash
# 本机交叉编译后 scp 到服务器并重启 systemd。
# 默认 DEPLOY_HOST=arb，二进制装到 /var/www/arb_market/dist/market-arb
# 可选: DEPLOY_PATH  SERVICE_USER  SKIP_BUILD=1
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DEPLOY_HOST="${DEPLOY_HOST:-arb}"
DEPLOY_PATH="${DEPLOY_PATH:-/var/www/arb_market/dist}"
SERVICE_USER="${SERVICE_USER:-market-arb}"
BIN_LOCAL="$ROOT/dist/market-arb"
REMOTE_TMP="/tmp/market-arb.new"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  "$ROOT/scripts/build-linux.sh"
fi
if [[ ! -f "$BIN_LOCAL" ]]; then
  echo "找不到 $BIN_LOCAL" >&2
  exit 1
fi

scp "$BIN_LOCAL" "$DEPLOY_HOST:$REMOTE_TMP"
ssh -t "$DEPLOY_HOST" "sudo install -d -m 0755 '$DEPLOY_PATH' && sudo install -m 0755 -o '$SERVICE_USER' -g '$SERVICE_USER' '$REMOTE_TMP' '$DEPLOY_PATH/market-arb' && rm -f '$REMOTE_TMP' && sudo systemctl restart market-arb && sudo systemctl --no-pager --full status market-arb"
