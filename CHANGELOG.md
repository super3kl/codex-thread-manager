# Changelog

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
