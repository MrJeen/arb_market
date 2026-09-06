#!/usr/bin/env bash
# 将 dist/market-arb.new 安装为正式二进制后启动。
set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
need_root

if systemctl is-active --quiet "$SERVICE"; then
  echo "$SERVICE 已在运行，请用 sudo ./scripts/restart.sh 换二进制" >&2
  exit 1
fi

install_uploaded_bin
systemctl start "$SERVICE"
systemctl --no-pager --full status "$SERVICE"
