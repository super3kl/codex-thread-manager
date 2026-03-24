#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_NAME="CodexThreadManager"
APP_DIR="${APP_DIR:-$HOME/Applications/${APP_NAME}.app}"
CONTENTS_DIR="$APP_DIR/Contents"
MACOS_DIR="$CONTENTS_DIR/MacOS"
RESOURCES_DIR="$CONTENTS_DIR/Resources"
RUST_CRATE_DIR="$ROOT_DIR/rust/codex_thread_manager"
RUST_BINARY_NAME="codex_thread_manager"
ICON_SCRIPT="$ROOT_DIR/scripts/generate_app_icon.swift"
ICON_WORK_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/codex-app-icon-XXXXXX")"
ICONSET_DIR="$ICON_WORK_ROOT/AppIcon.iconset"
ICNS_PATH="$ICON_WORK_ROOT/AppIcon.icns"

if [[ -n "${SWIFT_BUILD_ROOT:-}" ]]; then
  EFFECTIVE_SWIFT_BUILD_ROOT="$SWIFT_BUILD_ROOT"
  SHOULD_CLEAN_SWIFT_BUILD_ROOT="0"
else
  EFFECTIVE_SWIFT_BUILD_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/codex-swift-build-XXXXXX")"
  SHOULD_CLEAN_SWIFT_BUILD_ROOT="1"
fi

cleanup() {
  rm -rf "$ICON_WORK_ROOT"
  if [[ "$SHOULD_CLEAN_SWIFT_BUILD_ROOT" == "1" ]]; then
    rm -rf "$EFFECTIVE_SWIFT_BUILD_ROOT"
  fi
}
trap cleanup EXIT

mkdir -p "$HOME/Applications"

export CLANG_MODULE_CACHE_PATH="${TMPDIR:-/tmp}/codex-thread-manager-clang-cache"
export SWIFT_MODULECACHE_PATH="${TMPDIR:-/tmp}/codex-thread-manager-swift-cache"

mkdir -p "$CLANG_MODULE_CACHE_PATH" "$SWIFT_MODULECACHE_PATH"
mkdir -p "$EFFECTIVE_SWIFT_BUILD_ROOT"

if [[ "$(id -u)" -eq 0 ]]; then
  echo "不要用 sudo 运行整个安装脚本。" >&2
  echo "先用 sudo 删除旧 app，再切回当前用户运行本脚本。" >&2
  echo "示例：" >&2
  echo "  sudo rm -rf \"$HOME/Applications/${APP_NAME}.app\"" >&2
  echo "  cd \"$ROOT_DIR\" && APP_DIR=\"$HOME/Applications/${APP_NAME}.app\" zsh ./scripts/install_bar_app.sh" >&2
  exit 1
fi

if [[ -n "${CARGO_BIN:-}" ]]; then
  CARGO_CMD="$CARGO_BIN"
elif command -v cargo >/dev/null 2>&1; then
  CARGO_CMD="$(command -v cargo)"
elif [[ -x /tmp/codex-cargo/bin/cargo ]]; then
  export RUSTUP_HOME="${RUSTUP_HOME:-/tmp/codex-rustup}"
  export CARGO_HOME="${CARGO_HOME:-/tmp/codex-cargo}"
  CARGO_CMD="/tmp/codex-cargo/bin/cargo"
else
  echo "未找到 cargo，请先安装 Rust toolchain。" >&2
  exit 1
fi

"$CARGO_CMD" build --release --manifest-path "$RUST_CRATE_DIR/Cargo.toml"

swift "$ICON_SCRIPT" "$ICONSET_DIR"
iconutil -c icns "$ICONSET_DIR" -o "$ICNS_PATH"

swift build \
  --disable-sandbox \
  -c release \
  --package-path "$ROOT_DIR" \
  --scratch-path "$EFFECTIVE_SWIFT_BUILD_ROOT"

SWIFT_BUILD_DIR="$(
  swift build \
    --disable-sandbox \
    -c release \
    --package-path "$ROOT_DIR" \
    --scratch-path "$EFFECTIVE_SWIFT_BUILD_ROOT" \
    --show-bin-path
)"
SWIFT_EXECUTABLE_PATH="$SWIFT_BUILD_DIR/$APP_NAME"
RUST_EXECUTABLE_PATH="$RUST_CRATE_DIR/target/release/$RUST_BINARY_NAME"

if [[ ! -x "$SWIFT_EXECUTABLE_PATH" ]]; then
  echo "找不到 Swift 可执行文件: $SWIFT_EXECUTABLE_PATH" >&2
  exit 1
fi

if [[ ! -x "$RUST_EXECUTABLE_PATH" ]]; then
  echo "找不到 Rust 可执行文件: $RUST_EXECUTABLE_PATH" >&2
  exit 1
fi

if [[ -e "$APP_DIR" ]] && ! rm -rf "$APP_DIR" 2>/dev/null; then
  echo "无法覆盖现有 app: $APP_DIR" >&2
  echo "可能原因：旧版菜单栏应用仍在运行，或该目录不属于当前用户。" >&2
  echo "可以先退出旧版应用后重试，或通过 APP_DIR=/新的/安装路径 另装一份。" >&2
  exit 1
fi

mkdir -p "$MACOS_DIR" "$RESOURCES_DIR"

cp "$SWIFT_EXECUTABLE_PATH" "$MACOS_DIR/$APP_NAME"
cp "$RUST_EXECUTABLE_PATH" "$RESOURCES_DIR/$RUST_BINARY_NAME"
cp "$ICNS_PATH" "$RESOURCES_DIR/AppIcon.icns"
chmod +x "$MACOS_DIR/$APP_NAME" "$RESOURCES_DIR/$RUST_BINARY_NAME"

if command -v strip >/dev/null 2>&1; then
  strip -x "$MACOS_DIR/$APP_NAME" "$RESOURCES_DIR/$RUST_BINARY_NAME" 2>/dev/null || true
fi

cat > "$CONTENTS_DIR/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleExecutable</key>
  <string>CodexThreadManager</string>
  <key>CFBundleIdentifier</key>
  <string>tech.leizhu.codex-thread-manager</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundleIconFile</key>
  <string>AppIcon</string>
  <key>CFBundleName</key>
  <string>CodexThreadManager</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>0.1.0</string>
  <key>CFBundleVersion</key>
  <string>1</string>
  <key>LSMinimumSystemVersion</key>
  <string>13.0</string>
  <key>LSUIElement</key>
  <true/>
</dict>
</plist>
PLIST

echo "Installed: $APP_DIR"
echo "Run with: open \"$APP_DIR\""
