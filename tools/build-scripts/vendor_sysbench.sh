#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="$ROOT_DIR/vendor/sysbench"
SYSBENCH_BIN="$(command -v sysbench || true)"

if [ -z "$SYSBENCH_BIN" ]; then
  echo "未找到系统 sysbench，请先安装 sysbench，或直接使用 docker compose up --build。"
  exit 1
fi

mkdir -p "$TARGET_DIR/bin" "$TARGET_DIR/share"
cp "$SYSBENCH_BIN" "$TARGET_DIR/bin/sysbench"

SYSBENCH_SHARE_DIR=""
for candidate in \
  "$(cd "$(dirname "$SYSBENCH_BIN")/.." && pwd)/share/sysbench" \
  "/usr/share/sysbench"
do
  if [ -d "$candidate" ]; then
    SYSBENCH_SHARE_DIR="$candidate"
    break
  fi
done

if [ -n "$SYSBENCH_SHARE_DIR" ]; then
  rm -rf "$TARGET_DIR/share/sysbench"
  cp -R "$SYSBENCH_SHARE_DIR" "$TARGET_DIR/share/sysbench"
else
  echo "警告: 未找到 sysbench Lua 脚本目录，项目中仅同步了 sysbench 二进制。"
fi

chmod +x "$TARGET_DIR/bin/sysbench"
echo "已将 sysbench 内置到: $TARGET_DIR"
