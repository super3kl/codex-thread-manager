# Codex Thread Manager

Sync, inspect, repair, and clean Codex threads across providers from a
macOS menu bar app and a Rust CLI.

`Codex Thread Manager` 是一个本地优先的 macOS 菜单栏工具，配合一套
Rust CLI，用来管理 `~/.codex` 里的线程数据。

它解决的不是“聊天”本身，而是聊天线程在多 provider 场景下的这些运维问题：

- 哪些 provider 现在真的有线程
- 各 provider 的线程副本是否一致
- 活跃 / 归档线程到底占了多少空间
- 已归档线程怎么安全清理
- rollout 文件和数据库元数据不一致时怎么修

## Why

当你同时使用多个 Codex provider 时，本地线程数据很容易出现下面这些情况：

- 一个逻辑线程只存在于部分 provider
- 不同 provider 的线程副本内容已经漂移
- rollout 权限上下文和数据库记录不一致
- 已归档线程持续堆积，占用越来越多磁盘

这个项目的目标很直接：

- 让线程副本保持可见
- 让同步与清理变成可重复执行的本地操作
- 在真正改动数据前自动做备份，降低误操作成本

## Highlights

- 自动发现数据库里实际存在的 provider
- 一键同步全部 provider
- 按需选择一组 provider 执行同步
- 查看 provider 数、逻辑线程数、完整覆盖数
- 查看活跃 / 归档线程空间占用
- 按逻辑线程组清理归档线程，避免只删单边副本
- 同步时自动修复 rollout 中最新 `turn_context` 的权限上下文
- 每次同步或清理前自动备份数据库、状态文件和相关 rollout
- 菜单栏里可直接调整清理规则，并持久化到本机偏好

## Architecture

项目是一个很清晰的双层结构：

- `Swift` 负责 macOS 菜单栏体验
- `Rust` 负责真正的数据扫描、同步、修复、清理和备份

菜单栏应用本身不直接操作数据库，而是调用 Rust CLI 并接收 JSON 结果。

```text
macOS menu bar app (Swift)
        |
        v
codex_thread_manager (Rust CLI)
        |
        v
~/.codex/state_5.sqlite
~/.codex/provider_sync_state.json
~/.codex/provider_sync.log
~/.codex/provider_sync_backups/
```

## Menu Bar Experience

安装后的菜单栏应用会提供这些核心动作：

- `同步全部 provider`
- `同步指定 provider...`
- `清理设置...`
- `清理已归档线程...`
- `刷新状态`
- `打开备份目录`
- `打开日志文件`

其中：

- `同步指定 provider...` 会弹出一个 provider 选择框，至少选择 2 个
- `清理已归档线程...` 先预览，再确认执行
- 菜单顶部会显示 provider 状态、逻辑线程数、空间占用和最近一次执行结果

## Requirements

- macOS 13+
- Xcode Command Line Tools / Swift 6
- Rust stable toolchain

## Install The Menu Bar App

在仓库根目录执行：

```bash
./scripts/install_bar_app.sh
```

默认安装到：

```text
~/Applications/CodexThreadManager.app
```

安装完成后可直接启动：

```bash
open ~/Applications/CodexThreadManager.app
```

如果你想装到别的位置：

```bash
APP_DIR=~/Applications/CodexThreadManager-Preview.app ./scripts/install_bar_app.sh
```

## CLI

### Status

查看所有活跃 provider 的 mesh 状态：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- status-all
```

查看空间占用：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- space
```

### Sync

同步全部 provider：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- sync-all
```

只同步指定 provider 集合：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- sync-selected \
  --provider openai \
  --provider cpa \
  --provider anthropic
```

高级用法：如果你确实需要定向同步，CLI 仍保留这些命令：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- sync \
  --source openai \
  --target cpa
```

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- sync-bidirectional \
  --provider-a openai \
  --provider-b cpa
```

### Cleanup

先预览清理：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- cleanup \
  --scope archived \
  --older-than-days 30 \
  --keep-latest 20
```

确认后执行真实清理：

```bash
cargo run --manifest-path rust/codex_thread_manager/Cargo.toml --bin codex_thread_manager -- cleanup \
  --scope archived \
  --older-than-days 30 \
  --keep-latest 20 \
  --apply
```

如果已经编译出了 release 二进制，也可以直接运行：

```bash
./rust/codex_thread_manager/target/release/codex_thread_manager status-all
```

## Design Notes

- `sync-all` 会自动纳入 `threads.model_provider` 中真实存在的 provider
- `sync-selected` 只在选中的 provider 范围内做 mesh 同步
- 逻辑线程的权威版本优先级：
  - 先比较 `updated_at`
  - 同时间按 `openai > cpa > 其他(字典序)`
- 清理是按“逻辑线程组”执行，不会只删单边 provider 副本
- `cleanup --apply` 必须带保护条件：
  - `--older-than-days`
  - 或 `--keep-latest`
- 菜单栏默认清理规则是：
  - 清理 30 天前的已归档线程
  - 保留最近 20 个逻辑线程

## Local Files

运行时会使用你本机 `~/.codex` 下的这些文件：

- `state_5.sqlite`
- `provider_sync_state.json`
- `provider_sync.log`
- `provider_sync_backups/`

这些文件不属于仓库内容，不应该提交到 GitHub。

## Project Layout

```text
.
├── Package.swift
├── README.md
├── docs/
├── Sources/CodexThreadManager/
│   ├── AppDelegate.swift
│   ├── MenuBarController.swift
│   ├── ScriptLocator.swift
│   ├── StatusIcon.swift
│   └── main.swift
├── rust/codex_thread_manager/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── main.rs
│       └── bin/codex_provider_sync.rs
└── scripts/
    ├── generate_app_icon.swift
    └── install_bar_app.sh
```

## Development

运行菜单栏应用：

```bash
swift run CodexThreadManager
```

运行 Rust 测试：

```bash
cargo test --manifest-path rust/codex_thread_manager/Cargo.toml
```

建议在发布或提交前做一次基本检查：

```bash
swift build --disable-sandbox -c release
cargo fmt --manifest-path rust/codex_thread_manager/Cargo.toml --all --check
cargo test --manifest-path rust/codex_thread_manager/Cargo.toml
```

## Compatibility

- 当前主二进制名是 `codex_thread_manager`
- 旧二进制名 `codex_provider_sync` 仍保留，方便兼容旧入口

## License

MIT
