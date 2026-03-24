# GitHub Launch Kit

这份文档用来直接填写 GitHub 仓库信息、Release 文案和首页简介。

## 仓库名称

`codex-thread-manager`

## GitHub 简介

### 推荐英文版

`A macOS menu bar app and Rust CLI for syncing, inspecting, repairing, and cleaning Codex threads across providers.`

### 推荐中文版

`一个用于同步、查看、修复和清理 Codex 线程的 macOS 菜单栏工具与 Rust CLI。`

## Tagline

### 中文

`让 Codex 线程在不同 provider 之间保持同步、可见、可清理。`

### English

`Keep Codex threads in sync, visible, and clean across providers.`

## 推荐 Topics

- `codex`
- `macos`
- `menubar`
- `swift`
- `rust`
- `sqlite`
- `thread-manager`
- `session-sync`

## 首版 Release 标题

`v0.1.0 - Codex Thread Manager`

## 首版 Release 文案

```md
## Codex Thread Manager v0.1.0

First public release of Codex Thread Manager.

### What it does

- Sync Codex threads across providers
- Inspect active and archived thread storage usage
- Clean archived threads with dry-run support
- Repair inconsistent thread metadata and rollout state
- Manage everything from a macOS menu bar app or a Rust CLI

### Highlights

- Mesh sync for all discovered providers
- `status-all`, `sync-all`, `space`, and `cleanup` commands
- Menu bar cleanup settings with local persistence
- Automatic backups before sync and cleanup
- Compatibility binary retained for older entrypoints

### Notes

- Runtime state lives under `~/.codex`
- Local databases, backups, and build artifacts are excluded from the repository
```

## 仓库首页开头推荐文案

```md
Sync, inspect, repair, and clean Codex threads across providers from a macOS menu bar app and a Rust CLI.
```
