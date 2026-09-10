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
  echo "先在开发机运行 scripts/build-linux.sh，再 scp 到 $INSTALL_DIR/dist/market-arb.new。" >&2
  exit 1
fi
if [[ ! -f "$UNIT_SRC" ]]; then
  echo "找不到 unit: $UNIT_SRC" >&2
  exit 1
fi

if systemctl is-active --quiet market-arb.service; then
  echo "请先停止 market-arb.service，再安装并校正状态文件权限。" >&2
  exit 1
fi

# 原子保存需要在根目录创建临时文件；拒绝跟随状态路径中的符号链接。
STATE_FILES=(
  polymarket_api_creds.json
  polymarket_api_creds.json.tmp
  polymarket_funder_cursor
  polymarket_funder_cursor.cursor.tmp
)
for name in "${STATE_FILES[@]}"; do
  path="$INSTALL_DIR/$name"
  if [[ -L "$path" || ( -e "$path" && ! -f "$path" ) ]]; then
    echo "状态路径不是普通文件，拒绝修改权限: $path" >&2
    exit 1
  fi
done

if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --home-dir "$INSTALL_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
fi

install -d -m 0755 "$INSTALL_DIR/dist"
# 只调整根目录的组权限，不递归改变工作树；保留部署用户的目录属主。
chgrp "$SERVICE_USER" "$INSTALL_DIR"
chmod g+rwx "$INSTALL_DIR"
for name in "${STATE_FILES[@]}"; do
  path="$INSTALL_DIR/$name"
  if [[ -f "$path" ]]; then
    chown "$SERVICE_USER:$SERVICE_USER" "$path"
    chmod 0600 "$path"
  fi
done

if [[ "$(readlink -f "$BIN_SRC")" == "$(readlink -f "$BIN_DST")" ]]; then
  # 工作树就是安装目录时，二进制已在目标路径，只校正属主。
  chown "$SERVICE_USER:$SERVICE_USER" "$BIN_DST"
  chmod 0755 "$BIN_DST"
else
  install -m 0755 -o "$SERVICE_USER" -g "$SERVICE_USER" "$BIN_SRC" "$BIN_DST"
fi

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
echo "  sudo $ROOT/scripts/start.sh"
echo "  sudo $ROOT/scripts/log.sh"
