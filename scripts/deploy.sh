#!/usr/bin/env bash
# 本机交叉编译后 scp 为 market-arb.new，再远程 restart.sh 安装并重启。
# 默认 DEPLOY_HOST=arb
# 可选: DEPLOY_PATH  SERVICE_USER  SKIP_BUILD=1
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DEPLOY_HOST="${DEPLOY_HOST:-arb}"
DEPLOY_PATH="${DEPLOY_PATH:-/var/www/arb_market/dist}"
BIN_LOCAL="$ROOT/dist/market-arb"
REMOTE_NEW="$DEPLOY_PATH/market-arb.new"
REMOTE_RESTART="${DEPLOY_RESTART:-/var/www/arb_market/scripts/restart.sh}"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  "$ROOT/scripts/build-linux.sh"
fi
if [[ ! -f "$BIN_LOCAL" ]]; then
  echo "找不到 $BIN_LOCAL" >&2
  exit 1
fi

ssh "$DEPLOY_HOST" "mkdir -p '$DEPLOY_PATH'"
scp "$BIN_LOCAL" "$DEPLOY_HOST:$REMOTE_NEW"
ssh -t "$DEPLOY_HOST" "sudo '$REMOTE_RESTART'"
