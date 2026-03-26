# GitHub Launch Kit

这份文档用来直接填写 GitHub 仓库信息、Release 文案和首页简介。

## 仓库名称

`codex-thread-manager`

## License

`MIT`

## GitHub About

### 推荐短版

`Manage Codex threads across providers with a macOS menu bar app and Rust CLI.`

### 推荐长版

### 推荐英文版

`A macOS menu bar app and Rust CLI for syncing, inspecting, repairing, and cleaning Codex threads across providers.`

### 推荐中文版

`一个用于同步、查看、修复和清理 Codex 线程的 macOS 菜单栏工具与 Rust CLI。`

## Tagline

### 中文

`让 Codex 线程在不同 provider 之间保持同步、可见、可备份、可清理。`

### English

`Keep Codex threads in sync, visible, backed up, and easy to clean across providers.`

## 推荐 Topics

- `codex`
- `macos`
- `menubar`
- `swift`
- `rust`
- `sqlite`
- `desktop-app`
- `local-first`
- `thread-manager`
- `session-sync`

## 推荐置顶卖点

- 自动发现数据库里真实存在的 provider
- 同步全部 provider，或按需选择一组 provider 同步
- 查看活跃 / 归档线程的空间占用
- 预览后再清理已归档线程
- 每次同步和清理前自动备份
- 既可以走菜单栏，也可以走 Rust CLI

## 下一版 Release 标题

`v0.1.1 - Simpler Provider Sync`

## 下一版 Release 文案

```md
## Codex Thread Manager v0.1.1

This release streamlines the sync workflow and makes the app easier to use day to day.

### Highlights

- Sync all discovered providers from the menu bar
- Select a custom provider set with `同步指定 provider...`
- Inspect active and archived storage usage
- Preview archived-thread cleanup before applying it
- Keep automatic backups before sync and cleanup

### What's new

- Added selected-provider mesh sync in both the menu bar app and Rust CLI
- Simplified the sync menu by removing redundant one-off provider shortcuts
- Refreshed project documentation and GitHub-facing copy
- Cleaned up the Swift concurrency warning in the menu bar runtime path

### Notes

- Runtime state lives under `~/.codex`
- Local databases, backups, and build artifacts are excluded from the repository
- Directional sync commands are still available from the CLI for advanced workflows
```

## 仓库首页开头推荐文案

```md
Manage Codex threads across providers with a macOS menu bar app and Rust CLI.
```
