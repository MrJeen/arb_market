#!/usr/bin/env bash
# 在 macOS 上交叉编译 Linux x86_64 发行版二进制，输出到 dist/market-arb。
# 依赖（择一）：
#   1. cargo-zigbuild + zig   brew install zig && cargo install cargo-zigbuild
#   2. cross + Docker         cargo install cross
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TARGET="${TARGET:-x86_64-unknown-linux-gnu}"
OUT_DIR="${OUT_DIR:-$ROOT/dist}"
BIN_NAME="market-arb"

rustup target add "$TARGET" >/dev/null

build_with_zig() {
  cargo zigbuild --release --target "$TARGET" --bin "$BIN_NAME"
}

build_with_cross() {
  cross build --release --target "$TARGET" --bin "$BIN_NAME"
}

HOST="$(rustc -vV | awk '/^host:/ { print $2 }')"
if [[ "$HOST" == "$TARGET" ]]; then
  cargo build --release --bin "$BIN_NAME"
  SRC="$ROOT/target/release/$BIN_NAME"
elif command -v cargo-zigbuild >/dev/null 2>&1 && command -v zig >/dev/null 2>&1; then
  build_with_zig
  SRC="$ROOT/target/$TARGET/release/$BIN_NAME"
elif command -v cross >/dev/null 2>&1; then
  build_with_cross
  SRC="$ROOT/target/$TARGET/release/$BIN_NAME"
else
  echo "本机是 $HOST，无法直接编 $TARGET。" >&2
  echo "请先安装: brew install zig && cargo install cargo-zigbuild" >&2
  echo "或安装 Docker 后: cargo install cross" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
cp "$SRC" "$OUT_DIR/$BIN_NAME"
chmod 755 "$OUT_DIR/$BIN_NAME"
file "$OUT_DIR/$BIN_NAME" || true
ls -lh "$OUT_DIR/$BIN_NAME"
echo "已输出 $OUT_DIR/$BIN_NAME"
echo "上传到服务器后执行: sudo ./scripts/install-systemd.sh"
