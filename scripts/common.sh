# 由 start/restart/stop/log 引用。不要单独执行。
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVICE="${SERVICE:-market-arb}"
SERVICE_USER="${SERVICE_USER:-market-arb}"
BIN_DST="${BIN_DST:-$ROOT/dist/market-arb}"
BIN_NEW="${BIN_NEW:-$ROOT/dist/market-arb.new}"

need_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "请用 root 运行: sudo $0" >&2
    exit 1
  fi
}

install_uploaded_bin() {
  if [[ -f "$BIN_NEW" ]]; then
    install -d -m 0755 "$(dirname "$BIN_DST")"
    install -m 0755 -o "$SERVICE_USER" -g "$SERVICE_USER" "$BIN_NEW" "$BIN_DST"
    echo "已安装 $BIN_NEW -> $BIN_DST"
    return
  fi
  if [[ -f "$BIN_DST" ]]; then
    echo "没有 $BIN_NEW，使用现有 $BIN_DST"
    return
  fi
  echo "找不到二进制: $BIN_NEW 或 $BIN_DST" >&2
  echo "先 scp 到 $BIN_NEW" >&2
  exit 1
}
