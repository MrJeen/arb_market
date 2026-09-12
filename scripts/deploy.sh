#!/usr/bin/env bash
# 本机交叉编译后 scp 为 market-arb.new，再远程 restart.sh 安装并重启。
# 默认 DEPLOY_HOST=arb；--upload-only 仅构建上传，不安装、启动或重启服务。
# 可选: DEPLOY_PATH  SERVICE_USER  SKIP_BUILD=1
set -euo pipefail

UPLOAD_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --upload-only) UPLOAD_ONLY=1 ;;
    *)
      echo "未知参数: $arg" >&2
      echo "用法: $0 [--upload-only]" >&2
      exit 1
      ;;
  esac
done

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
if [[ "$UPLOAD_ONLY" == "1" ]]; then
  echo "已上传至 ${DEPLOY_HOST}:${REMOTE_NEW}，未安装或启动、重启服务。"
else
  ssh -t "$DEPLOY_HOST" "sudo '$REMOTE_RESTART'"
fi
