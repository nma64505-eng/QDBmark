#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PATCH_SCRIPT="$ROOT_DIR/scripts/patch_ycsb_mongodb.py"
TARGET_DIR="$ROOT_DIR/vendor/ycsb"
YCSB_VERSION="${YCSB_VERSION:-0.17.0}"
SOURCE_DIR="$ROOT_DIR/.cache/ycsb-src"

if ! command -v java >/dev/null 2>&1 || ! command -v mvn >/dev/null 2>&1 || ! command -v git >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
  echo "未找到 Java / Maven / Git / Python3，请先安装这些依赖，或直接使用 docker compose up --build。"
  exit 1
fi

if [[ ! -f "$PATCH_SCRIPT" ]]; then
  echo "缺少补丁脚本: $PATCH_SCRIPT"
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir -p "$(dirname "$SOURCE_DIR")"
rm -rf "$SOURCE_DIR"

echo "正在拉取 YCSB ${YCSB_VERSION} 源码 ..."
git clone --depth 1 --branch "$YCSB_VERSION" https://github.com/brianfrankcooper/YCSB "$SOURCE_DIR"

python3 "$PATCH_SCRIPT" "$SOURCE_DIR"

echo "正在构建 MongoDB YCSB binding ..."
(
  cd "$SOURCE_DIR"
  mvn -pl site.ycsb:mongodb-binding -am -DskipTests -Dcheckstyle.skip=true clean package
)

rm -rf "$TARGET_DIR"
tar -xzf "$SOURCE_DIR/mongodb/target/ycsb-mongodb-binding-${YCSB_VERSION}.tar.gz" -C "$TMP_DIR"
mv "$TMP_DIR/ycsb-mongodb-binding-${YCSB_VERSION}" "$TARGET_DIR"
chmod +x "$TARGET_DIR/bin/ycsb.sh"

echo "已将 YCSB 内置到: $TARGET_DIR"
