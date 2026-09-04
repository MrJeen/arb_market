#!/usr/bin/env bash
# 在 Linux 服务器上安装 systemd unit（默认就在 git 工作树 /var/www/arb_market）。
# 用法: sudo ./scripts/install-systemd.sh [二进制路径]
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "请用 root 运行: sudo $0" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="${INSTALL_DIR:-/var/www/arb_market}"
SERVICE_USER="${SERVICE_USER:-market-arb}"
BIN_SRC="${1:-$ROOT/dist/market-arb}"
UNIT_SRC="$ROOT/deploy/market-arb.service"
UNIT_DST="/etc/systemd/system/market-arb.service"
BIN_DST="$INSTALL_DIR/dist/market-arb"

if [[ ! -f "$BIN_SRC" ]]; then
  echo "找不到二进制: $BIN_SRC" >&2
  echo "先在开发机运行 scripts/build-linux.sh，再 scp 到 $INSTALL_DIR/dist/market-arb。" >&2
  exit 1
fi
if [[ ! -f "$UNIT_SRC" ]]; then
  echo "找不到 unit: $UNIT_SRC" >&2
  exit 1
fi

if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --home-dir "$INSTALL_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
fi

install -d -m 0755 "$INSTALL_DIR/dist"
install -m 0755 -o "$SERVICE_USER" -g "$SERVICE_USER" "$BIN_SRC" "$BIN_DST"

if [[ ! -f "$INSTALL_DIR/.env" ]]; then
  if [[ -f "$ROOT/.env.example" ]]; then
    install -m 0640 -o "$SERVICE_USER" -g "$SERVICE_USER" "$ROOT/.env.example" "$INSTALL_DIR/.env"
    echo "已写入 $INSTALL_DIR/.env（来自 .env.example），请填好密钥后再启动。"
  else
    echo "请自行创建 $INSTALL_DIR/.env" >&2
  fi
fi

KEYS_DST="$INSTALL_DIR/polymarket_funders.json"
if [[ ! -f "$KEYS_DST" ]]; then
  if [[ -f "$ROOT/polymarket_funders.json" ]]; then
    install -m 0600 -o "$SERVICE_USER" -g "$SERVICE_USER" "$ROOT/polymarket_funders.json" "$KEYS_DST"
  elif [[ -f "$ROOT/polymarket_funders.json.example" ]]; then
    install -m 0600 -o "$SERVICE_USER" -g "$SERVICE_USER" "$ROOT/polymarket_funders.json.example" "$KEYS_DST"
    echo "已写入 $KEYS_DST，请填入 funder 账户后再启动。"
  fi
fi

sed -e "s|/var/www/arb_market|$INSTALL_DIR|g" \
    -e "s|User=market-arb|User=$SERVICE_USER|g" \
    -e "s|Group=market-arb|Group=$SERVICE_USER|g" \
    "$UNIT_SRC" > "$UNIT_DST"
chmod 644 "$UNIT_DST"

systemctl daemon-reload
systemctl enable market-arb.service
echo "已安装。编辑 $INSTALL_DIR/.env 后执行:"
echo "  sudo systemctl start market-arb"
echo "  sudo systemctl status market-arb"
echo "  sudo journalctl -u market-arb -f"
