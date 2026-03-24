# Codex Thread Manager

Sync, inspect, repair, and clean Codex threads across providers from a macOS menu bar app and a Rust CLI.

一个本地可运行的 macOS 菜单栏工具，加上一套 Rust CLI，
用于管理 Codex 聊天线程。

它的目标很简单：

- 同步不同 provider 之间的线程
- 查看线程空间占用
- 清理已归档线程
- 修复线程元数据与 rollout 文件的状态不一致

## GitHub 快速信息

- 推荐仓库名：`codex-thread-manager`
- License：`MIT`
- GitHub 简介：
  `A macOS menu bar app and Rust CLI for syncing, inspecting, repairing, and cleaning Codex threads across providers.`
- 一句话 tagline：
  `让 Codex 线程在不同 provider 之间保持同步、可见、可清理。`
- 推荐 Topics：
  `codex`, `macos`, `menubar`, `swift`, `rust`, `sqlite`, `thread-manager`, `session-sync`

## 功能概览

- 自动发现数据库中实际存在的 provider
- 一键同步全部 provider
- 保留 `openai <-> cpa` 的定向同步能力
- 查看活跃线程 / 已归档线程的空间占用
- 按逻辑线程做清理，避免只删单边副本
- 同步时修复 rollout 中最新 `turn_context` 的权限上下文
- 菜单栏内可直接调整清理规则，并保存到本机偏好
- 每次同步或清理前自动备份数据库、状态文件和相关 rollout

## 项目结构

```text
.
├── CHANGELOG.md
├── Package.swift
├── README.md
├── docs
│   └── github-launch-kit.md
├── Sources/CodexThreadManager
│   ├── AppDelegate.swift
│   ├── MenuBarController.swift
│   ├── ScriptLocator.swift
│   ├── StatusIcon.swift
│   └── main.swift
├── rust/codex_thread_manager
│   ├── Cargo.toml
│   └── src
│       ├── lib.rs
│       ├── main.rs
│       └── bin/codex_provider_sync.rs
└── scripts
    ├── generate_app_icon.swift
    └── install_bar_app.sh
```

## CLI 用法

查看 mesh 状态：

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

清理前先预览：

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

如果已经编译出 release 二进制，也可以直接运行：

```bash
./rust/codex_thread_manager/target/release/codex_thread_manager status-all
```

## 安装菜单栏应用

```bash
./scripts/install_bar_app.sh
```

安装后默认生成：

```text
~/Applications/CodexThreadManager.app
```

如果你想装到其他位置：

```bash
APP_DIR=~/Applications/CodexThreadManager-Preview.app ./scripts/install_bar_app.sh
```

## 设计说明

- `sync-all` 会自动纳入 `threads.model_provider` 中真实存在的 provider
- 冲突规则：
  - `updated_at` 更新的优先
  - 同时间按 `openai > cpa > 其他(字典序)`
- 清理是按“逻辑线程组”执行，不会只删单边 provider 副本
- `cleanup --apply` 必须带保护条件：
  - `--older-than-days`
  - 或 `--keep-latest`
- 菜单栏里的“清理设置...”默认规则是：
  - 只清理 30 天前的已归档线程
  - 保留最近 20 个逻辑线程

## 本地状态文件

运行时会使用你本机 `~/.codex` 下的状态文件：

- `provider_sync_state.json`
- `provider_sync.log`
- `provider_sync_backups/`
- `state_5.sqlite`

这些文件不属于仓库内容，不应该提交到 GitHub。

## 开发

本机运行菜单栏：

```bash
swift run CodexThreadManager
```

运行 Rust 测试：

```bash
cargo test --manifest-path rust/codex_thread_manager/Cargo.toml
```

## 兼容性说明

- 当前主二进制名是 `codex_thread_manager`
- 旧二进制名 `codex_provider_sync` 仍然保留，方便兼容旧入口

## 上传 GitHub 前

仓库已经包含 `.gitignore`，默认会忽略：

- Swift 构建产物
- Rust `target`
- macOS 杂项文件
- 本地备份目录

建议上传前再本地确认一次：

```bash
swift build --disable-sandbox -c release
cargo test --manifest-path rust/codex_thread_manager/Cargo.toml
```

## License

MIT
