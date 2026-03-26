# Codex Thread Manager

Manage Codex threads across providers with a macOS menu bar app and
Rust CLI.

`Codex Thread Manager` 用来处理本机 `~/.codex` 里的线程数据：

- 同步多个 provider 的线程副本
- 查看活跃 / 归档线程空间占用
- 清理已归档线程
- 修复 rollout 和线程元数据不一致
- 在同步和清理前自动备份

## Features

- 自动发现数据库里实际存在的 provider
- `同步全部 provider`
- `同步指定 provider...`
- 已归档线程清理预览与执行
- 菜单栏查看状态、空间占用和最近一次结果
- Rust CLI 支持 `status-all`、`space`、`sync-all`、`sync-selected`、`cleanup`

## Requirements

- macOS 13+
- Xcode Command Line Tools / Swift 6
- Rust stable toolchain

## Install

```bash
./scripts/install_bar_app.sh
open ~/Applications/CodexThreadManager.app
```

默认安装位置：

```text
~/Applications/CodexThreadManager.app
```

## CLI

查看状态：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- status-all
```

查看空间占用：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- space
```

同步全部 provider：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- sync-all
```

同步指定 provider：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- sync-selected \
  --provider openai \
  --provider cpa
```

预览归档清理：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- cleanup \
  --scope archived \
  --older-than-days 30 \
  --keep-latest 20
```

执行归档清理：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- cleanup \
  --scope archived \
  --older-than-days 30 \
  --keep-latest 20 \
  --apply
```

## Local Data

运行时会使用你本机 `~/.codex` 下的文件：

- `state_5.sqlite`
- `provider_sync_state.json`
- `provider_sync.log`
- `provider_sync_backups/`

这些文件不属于仓库内容，不应该提交。

## Development

运行菜单栏应用：

```bash
swift run CodexThreadManager
```

运行检查：

```bash
swift build --disable-sandbox
cargo fmt --manifest-path rust/codex_thread_manager/Cargo.toml --all --check
cargo test --manifest-path rust/codex_thread_manager/Cargo.toml
```

## License

MIT
