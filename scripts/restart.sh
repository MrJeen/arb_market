#!/usr/bin/env bash
# 停服务、安装 dist/market-arb.new、再启动。
set -euo pipefail
# shellcheck source=common.sh
source "$(cd "$(dirname "$0")" && pwd)/common.sh"
need_root

systemctl stop "$SERVICE" || true
install_uploaded_bin
systemctl start "$SERVICE"
systemctl --no-pager --full status "$SERVICE"
