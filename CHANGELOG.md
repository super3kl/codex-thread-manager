# Changelog

## Unreleased

### Added

- 菜单栏内支持“同步指定 provider...”，可按勾选的 provider 集合同步
- Rust CLI 新增 `sync-selected` 命令

### Changed

- 菜单栏同步区只保留“同步全部 provider”和“同步指定 provider...”
- 删除 `openai/cpa` 的快捷同步按钮，减少重复入口
- 重写 README，并更新 GitHub Launch Kit 文案
- 清理菜单栏运行路径中的 Swift 并发 warning

## v0.1.0

首个可公开发布版本。

### Added

- 基于 Rust 的 `codex_thread_manager` CLI
- 基于 Swift 的 macOS 菜单栏应用 `CodexThreadManager`
- 多 provider mesh 同步能力
- `status-all` / `sync-all` / `space` / `cleanup` CLI 命令
- 线程空间统计
- 已归档线程清理与 dry-run 预览
- 清理前自动备份数据库、状态文件和 rollout
- 菜单栏内清理规则设置与本机持久化

### Changed

- 项目从“provider sync bar”升级为“thread manager”
- 对外名称统一为 `Codex Thread Manager`
- 保留旧二进制名 `codex_provider_sync` 作为兼容入口

### Notes

- 运行时使用本机 `~/.codex` 下的状态文件
- 仓库不包含本地数据库、备份和构建产物
