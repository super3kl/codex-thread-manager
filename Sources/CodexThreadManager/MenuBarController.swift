import AppKit
import Foundation

private enum ManagerDefaults {
    static let cleanupOlderThanDays = 30
    static let cleanupKeepLatest = 20
}

private enum CleanupPreferenceKey {
    static let olderThanDays = "codex.thread-manager.cleanup.older-than-days"
    static let keepLatest = "codex.thread-manager.cleanup.keep-latest"
}

private struct ScriptEnvelope<T: Decodable>: Decodable {
    let ok: Bool
    let result: T?
    let error: String?
}

private final class CompletionRelay<Value>: @unchecked Sendable {
    let completion: (Value) -> Void

    init(_ completion: @escaping (Value) -> Void) {
        self.completion = completion
    }
}

private struct StatusPaths: Decodable {
    let codexHome: String
    let statePath: String
    let backupRoot: String
    let logPath: String

    private enum CodingKeys: String, CodingKey {
        case codexHome = "codex_home"
        case statePath = "state_path"
        case backupRoot = "backup_root"
        case logPath = "log_path"
    }
}

private struct MeshStatusResult: Decodable {
    let providers: [String: Int]
    let providerOrder: [String]
    let linkCount: Int
    let completeLinkCount: Int
    let paths: StatusPaths

    private enum CodingKeys: String, CodingKey {
        case providers
        case providerOrder = "provider_order"
        case linkCount = "link_count"
        case completeLinkCount = "complete_link_count"
        case paths
    }
}

private struct AppliedSummary: Decodable {
    let create: Int
    let adopt: Int
    let update: Int
    let repair: Int
    let skip: Int
}

private struct MeshSyncResult: Decodable {
    let mode: String
    let dryRun: Bool
    let providers: [String]
    let planned: AppliedSummary
    let applied: AppliedSummary?
    let backupDir: String?
    let finalStatus: MeshStatusResult

    private enum CodingKeys: String, CodingKey {
        case mode
        case dryRun = "dry_run"
        case providers
        case planned
        case applied
        case backupDir = "backup_dir"
        case finalStatus = "final_status"
    }
}

private struct SpaceBucket: Decodable {
    let threadCopies: Int
    let bytes: Int64
    let missingRollouts: Int

    private enum CodingKeys: String, CodingKey {
        case threadCopies = "thread_copies"
        case bytes
        case missingRollouts = "missing_rollouts"
    }
}

private struct ProviderSpaceUsage: Decodable {
    let threadCopies: Int
    let bytes: Int64

    private enum CodingKeys: String, CodingKey {
        case threadCopies = "thread_copies"
        case bytes
    }
}

private struct SpaceUsageResult: Decodable {
    let providers: [String: Int]
    let providerOrder: [String]
    let active: SpaceBucket
    let archived: SpaceBucket
    let perProvider: [String: ProviderSpaceUsage]
    let paths: StatusPaths

    private enum CodingKeys: String, CodingKey {
        case providers
        case providerOrder = "provider_order"
        case active
        case archived
        case perProvider = "per_provider"
        case paths
    }
}

private struct CleanupSummary: Decodable {
    let logicalThreads: Int
    let threadCopies: Int
    let bytes: Int64
    let missingRollouts: Int
    let providers: [String: Int]

    private enum CodingKeys: String, CodingKey {
        case logicalThreads = "logical_threads"
        case threadCopies = "thread_copies"
        case bytes
        case missingRollouts = "missing_rollouts"
        case providers
    }
}

private struct CleanupResult: Decodable {
    let mode: String
    let scope: String
    let dryRun: Bool
    let olderThanDays: Int?
    let keepLatest: Int
    let planned: CleanupSummary
    let applied: CleanupSummary?
    let backupDir: String?
    let finalStatus: MeshStatusResult
    let finalSpace: SpaceUsageResult

    private enum CodingKeys: String, CodingKey {
        case mode
        case scope
        case dryRun = "dry_run"
        case olderThanDays = "older_than_days"
        case keepLatest = "keep_latest"
        case planned
        case applied
        case backupDir = "backup_dir"
        case finalStatus = "final_status"
        case finalSpace = "final_space"
    }
}

private enum SyncAction {
    case allProviders

    var title: String {
        switch self {
        case .allProviders:
            return "同步全部 provider"
        }
    }

    var arguments: [String] {
        switch self {
        case .allProviders:
            return ["sync-all"]
        }
    }
}

@MainActor
final class MenuBarController: NSObject {
    private let statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
    private let menu = NSMenu()

    private let summaryItem = NSMenuItem(title: "状态加载中...", action: nil, keyEquivalent: "")
    private let mappingItem = NSMenuItem(title: "映射加载中...", action: nil, keyEquivalent: "")
    private let providerHeaderItem = NSMenuItem(title: "渠道状态", action: nil, keyEquivalent: "")
    private let spaceHeaderItem = NSMenuItem(title: "空间占用", action: nil, keyEquivalent: "")
    private let activeSpaceItem = NSMenuItem(title: "活跃线程空间加载中...", action: nil, keyEquivalent: "")
    private let archivedSpaceItem = NSMenuItem(title: "归档线程空间加载中...", action: nil, keyEquivalent: "")
    private let cleanupHintItem = NSMenuItem(
        title: "清理策略: 30 天前归档线程，保留最近 20 条",
        action: nil,
        keyEquivalent: ""
    )
    private let lastResultItem = NSMenuItem(title: "最近一次结果: 尚未执行", action: nil, keyEquivalent: "")
    private let syncAllItem = NSMenuItem(title: SyncAction.allProviders.title, action: #selector(syncAllProviders), keyEquivalent: "")
    private let syncSelectedProvidersItem = NSMenuItem(title: "同步指定 provider...", action: #selector(syncSelectedProviders), keyEquivalent: "")
    private let cleanupSettingsItem = NSMenuItem(title: "清理设置...", action: #selector(openCleanupSettings), keyEquivalent: "")
    private let cleanupArchivedItem = NSMenuItem(
        title: "清理已归档线程...",
        action: #selector(previewCleanupArchivedThreads),
        keyEquivalent: ""
    )
    private let refreshItem = NSMenuItem(title: "刷新状态", action: #selector(refreshStatus), keyEquivalent: "r")
    private let openBackupItem = NSMenuItem(title: "打开备份目录", action: #selector(openBackupDirectory), keyEquivalent: "")
    private let openLogItem = NSMenuItem(title: "打开日志文件", action: #selector(openLogFile), keyEquivalent: "")
    private let quitItem = NSMenuItem(title: "退出", action: #selector(quitApp), keyEquivalent: "q")

    private var providerItems: [NSMenuItem] = []
    private var latestStatus: MeshStatusResult?
    private var latestSpace: SpaceUsageResult?
    private var isRunningSync = false
    private var timer: Timer?
    private let defaults = UserDefaults.standard

    func start() {
        configureStatusItem()
        configureMenu()
        refreshStatus()
        timer = Timer.scheduledTimer(withTimeInterval: 20, repeats: true) { [weak self] _ in
            Task { @MainActor in
                self?.refreshStatus()
            }
        }
    }

    private func configureStatusItem() {
        if let button = statusItem.button {
            button.title = ""
            button.imagePosition = .imageOnly
            button.image = StatusIcon.make(state: .idle)
            button.toolTip = "Codex Thread Manager"
        }
    }

    private func configureMenu() {
        summaryItem.isEnabled = false
        mappingItem.isEnabled = false
        providerHeaderItem.isEnabled = false
        spaceHeaderItem.isEnabled = false
        activeSpaceItem.isEnabled = false
        archivedSpaceItem.isEnabled = false
        refreshCleanupHint()
        cleanupHintItem.isEnabled = false
        lastResultItem.isEnabled = false

        syncAllItem.target = self
        syncSelectedProvidersItem.target = self
        cleanupSettingsItem.target = self
        cleanupArchivedItem.target = self
        refreshItem.target = self
        openBackupItem.target = self
        openLogItem.target = self
        quitItem.target = self

        menu.addItem(summaryItem)
        menu.addItem(mappingItem)
        menu.addItem(providerHeaderItem)
        menu.addItem(spaceHeaderItem)
        menu.addItem(activeSpaceItem)
        menu.addItem(archivedSpaceItem)
        menu.addItem(cleanupHintItem)
        menu.addItem(lastResultItem)
        menu.addItem(.separator())
        menu.addItem(syncAllItem)
        menu.addItem(syncSelectedProvidersItem)
        menu.addItem(cleanupSettingsItem)
        menu.addItem(cleanupArchivedItem)
        menu.addItem(.separator())
        menu.addItem(refreshItem)
        menu.addItem(openBackupItem)
        menu.addItem(openLogItem)
        menu.addItem(.separator())
        menu.addItem(quitItem)

        statusItem.menu = menu
        rebuildProviderItems(for: nil)
        resetSpaceUsage()
        updateActionAvailability()
    }

    @objc
    func refreshStatus() {
        guard !isRunningSync else { return }
        runSyncBinary(arguments: ["status-all"]) { [weak self] (result: Result<MeshStatusResult, Error>) in
            guard let self else { return }
            switch result {
            case .success(let status):
                self.applyMeshStatus(status)
                self.refreshSpaceStatus(showError: false)
            case .failure(let error):
                self.latestStatus = nil
                self.summaryItem.title = "状态读取失败"
                self.mappingItem.title = "请检查 manager"
                self.lastResultItem.title = "最近一次结果: 状态读取失败"
                self.rebuildProviderItems(for: nil)
                self.resetSpaceUsage(message: "空间状态不可用")
                self.updateActionAvailability()
                self.showError("读取状态失败", error: error.localizedDescription)
            }
        }
    }

    @objc
    private func syncAllProviders() {
        performSync(.allProviders)
    }

    @objc
    private func syncSelectedProviders() {
        guard !isRunningSync else { return }

        let providers = selectableProviders()
        guard providers.count >= 2 else {
            showError("可同步的 provider 不足", error: "至少需要 2 个 provider 才能执行同步。")
            return
        }

        let alert = NSAlert()
        alert.alertStyle = .informational
        alert.messageText = "选择要同步的 provider"
        alert.informativeText = "默认会勾选全部 provider。至少选择 2 个后才能开始同步。"

        let checkboxes = providers.map { provider, count -> NSButton in
            let button = NSButton(checkboxWithTitle: "\(provider) (\(count))", target: nil, action: nil)
            button.state = .on
            return button
        }
        alert.accessoryView = makeProviderSelectionView(checkboxes: checkboxes)
        alert.addButton(withTitle: "开始同步")
        alert.addButton(withTitle: "取消")

        guard alert.runModal() == .alertFirstButtonReturn else { return }

        let selectedProviders = zip(providers, checkboxes).compactMap { entry, checkbox in
            checkbox.state == .on ? entry.0 : nil
        }
        guard selectedProviders.count >= 2 else {
            showError("选择无效", error: "请至少勾选 2 个 provider。")
            return
        }

        performSelectedProviderSync(selectedProviders)
    }

    @objc
    private func previewCleanupArchivedThreads() {
        guard !isRunningSync else { return }
        guard hasValidCleanupRule else {
            showError("清理规则无效", error: "请先打开“清理设置...”配置有效的天数或保留数量。")
            return
        }
        isRunningSync = true
        lastResultItem.title = "最近一次结果: 正在预览归档清理"
        updateActionAvailability()
        setStatusIconSyncing()

        runSyncBinary(arguments: cleanupArguments(apply: false)) { [weak self] (result: Result<CleanupResult, Error>) in
            self?.handleCleanupPreview(result)
        }
    }

    @objc
    private func openCleanupSettings() {
        let alert = NSAlert()
        alert.alertStyle = .informational
        alert.messageText = "清理设置"
        alert.informativeText = "设置“清理已归档线程...”使用的规则。"
        let olderField = NSTextField(string: "\(cleanupOlderThanDays)")
        olderField.placeholderString = "天数"
        let keepLatestField = NSTextField(string: "\(cleanupKeepLatest)")
        keepLatestField.placeholderString = "数量"
        alert.accessoryView = makeCleanupSettingsView(
            olderThanField: olderField,
            keepLatestField: keepLatestField
        )
        alert.addButton(withTitle: "保存")
        alert.addButton(withTitle: "取消")

        guard alert.runModal() == .alertFirstButtonReturn else { return }

        guard let olderThanDays = parseNonNegativeInteger(olderField.stringValue),
              let keepLatest = parseNonNegativeInteger(keepLatestField.stringValue) else {
            showError("清理设置无效", error: "请输入大于等于 0 的整数。")
            return
        }

        guard olderThanDays > 0 || keepLatest > 0 else {
            showError("清理设置无效", error: "“天数”和“保留数量”不能同时为 0。")
            return
        }

        defaults.set(olderThanDays, forKey: CleanupPreferenceKey.olderThanDays)
        defaults.set(keepLatest, forKey: CleanupPreferenceKey.keepLatest)
        refreshCleanupHint()
        updateActionAvailability()
        lastResultItem.title = "最近一次结果: 已更新清理设置"
        showInfo("清理设置已保存", message: cleanupRuleDescription())
    }

    @objc
    private func openBackupDirectory() {
        guard let latestStatus else { return }
        NSWorkspace.shared.open(URL(fileURLWithPath: latestStatus.paths.backupRoot))
    }

    @objc
    private func openLogFile() {
        guard let latestStatus else { return }
        NSWorkspace.shared.open(URL(fileURLWithPath: latestStatus.paths.logPath))
    }

    @objc
    private func quitApp() {
        NSApp.terminate(nil)
    }

    private func applyMeshStatus(_ status: MeshStatusResult) {
        latestStatus = status
        summaryItem.title = "Provider 数: \(status.providerOrder.count) | 逻辑线程: \(status.linkCount)"
        mappingItem.title = "完整覆盖: \(status.completeLinkCount)"
        rebuildProviderItems(for: status)
        updateActionAvailability()
    }

    private func applySpaceUsage(_ space: SpaceUsageResult) {
        latestSpace = space
        activeSpaceItem.title = "活跃: \(formatBytes(space.active.bytes)) | \(space.active.threadCopies) 条副本\(missingSuffix(space.active.missingRollouts))"
        archivedSpaceItem.title = "归档: \(formatBytes(space.archived.bytes)) | \(space.archived.threadCopies) 条副本\(missingSuffix(space.archived.missingRollouts))"
        updateActionAvailability()
    }

    private func resetSpaceUsage(message: String = "空间状态加载中...") {
        latestSpace = nil
        activeSpaceItem.title = "活跃: \(message)"
        archivedSpaceItem.title = "归档: \(message)"
        updateActionAvailability()
    }

    private func refreshSpaceStatus(showError: Bool) {
        runSyncBinary(arguments: ["space"]) { [weak self] (result: Result<SpaceUsageResult, Error>) in
            guard let self else { return }
            switch result {
            case .success(let space):
                self.applySpaceUsage(space)
            case .failure(let error):
                self.resetSpaceUsage(message: "空间读取失败")
                if showError {
                    self.showError("读取空间状态失败", error: error.localizedDescription)
                }
            }
        }
    }

    private func rebuildProviderItems(for status: MeshStatusResult?) {
        for item in providerItems {
            menu.removeItem(item)
        }
        providerItems.removeAll()

        let insertionIndex = menu.index(of: providerHeaderItem) + 1
        let items: [NSMenuItem]

        if let status {
            let order = status.providerOrder.isEmpty ? status.providers.keys.sorted() : status.providerOrder
            items = order.map { provider in
                let item = NSMenuItem(
                    title: "\(provider): \(status.providers[provider, default: 0])",
                    action: nil,
                    keyEquivalent: ""
                )
                item.isEnabled = false
                return item
            }
        } else {
            let item = NSMenuItem(title: "状态尚未加载", action: nil, keyEquivalent: "")
            item.isEnabled = false
            items = [item]
        }

        for (offset, item) in items.enumerated() {
            menu.insertItem(item, at: insertionIndex + offset)
        }
        providerItems = items
    }

    private func performSync(_ action: SyncAction) {
        guard !isRunningSync else { return }
        isRunningSync = true
        lastResultItem.title = "最近一次结果: 正在执行 \(action.title)"
        updateActionAvailability()
        setStatusIconSyncing()

        runSyncBinary(arguments: action.arguments) { [weak self] (result: Result<MeshSyncResult, Error>) in
            self?.handleMeshResult(result)
        }
    }

    private func performSelectedProviderSync(_ providers: [String]) {
        guard !isRunningSync else { return }
        let description = providerListDescription(providers)
        isRunningSync = true
        lastResultItem.title = "最近一次结果: 正在同步 \(description)"
        updateActionAvailability()
        setStatusIconSyncing()

        runSyncBinary(arguments: selectedProviderArguments(providers: providers)) { [weak self] (result: Result<MeshSyncResult, Error>) in
            self?.handleMeshResult(result, actionTitle: "同步指定 provider", failureTitle: "同步指定 provider 失败")
        }
    }

    private func handleMeshResult(
        _ result: Result<MeshSyncResult, Error>,
        actionTitle: String = "同步全部 provider",
        failureTitle: String = "同步全部 provider 失败"
    ) {
        defer { finishSync() }
        switch result {
        case .success(let summary):
            let applied = summary.applied
            let titlePrefix = summary.mode == "mesh-selected"
                ? "指定同步 \(providerListDescription(summary.providers))"
                : "全量同步 p\(summary.providers.count)"
            lastResultItem.title = "最近一次结果: \(titlePrefix) c\(applied?.create ?? 0) a\(applied?.adopt ?? 0) u\(applied?.update ?? 0) r\(applied?.repair ?? 0)"
            applyMeshStatus(summary.finalStatus)
            refreshSpaceStatus(showError: false)
        case .failure(let error):
            lastResultItem.title = "最近一次结果: \(actionTitle) 失败"
            showError(failureTitle, error: error.localizedDescription)
        }
    }

    private func handleCleanupPreview(_ result: Result<CleanupResult, Error>) {
        switch result {
        case .success(let preview):
            applyMeshStatus(preview.finalStatus)
            applySpaceUsage(preview.finalSpace)

            guard preview.planned.logicalThreads > 0 else {
                lastResultItem.title = "最近一次结果: 无可清理归档线程"
                finishSync()
                showInfo(
                    "没有需要清理的已归档线程",
                    message: cleanupRuleDescription() + "\n当前没有命中的逻辑线程。"
                )
                return
            }

            let alert = NSAlert()
            alert.alertStyle = .warning
            alert.messageText = "清理 \(preview.planned.logicalThreads) 个已归档逻辑线程？"
            alert.informativeText = cleanupPreviewDescription(preview)
            alert.addButton(withTitle: "执行清理")
            alert.addButton(withTitle: "取消")

            if alert.runModal() == .alertFirstButtonReturn {
                lastResultItem.title = "最近一次结果: 正在执行归档清理"
                runSyncBinary(arguments: cleanupArguments(apply: true)) { [weak self] (applyResult: Result<CleanupResult, Error>) in
                    self?.handleCleanupApply(applyResult)
                }
            } else {
                lastResultItem.title = "最近一次结果: 已取消归档清理"
                finishSync()
            }
        case .failure(let error):
            lastResultItem.title = "最近一次结果: 归档清理预览失败"
            finishSync()
            showError("预览归档清理失败", error: error.localizedDescription)
        }
    }

    private func handleCleanupApply(_ result: Result<CleanupResult, Error>) {
        defer { finishSync() }
        switch result {
        case .success(let summary):
            let applied = summary.applied
            let freed = formatBytes(applied?.bytes ?? 0)
            lastResultItem.title = "最近一次结果: 归档清理 l\(applied?.logicalThreads ?? 0) t\(applied?.threadCopies ?? 0) 释放\(freed)"
            applyMeshStatus(summary.finalStatus)
            applySpaceUsage(summary.finalSpace)
            if let backupDir = summary.backupDir {
                showInfo(
                    "归档清理完成",
                    message: """
                    已删除 \(applied?.logicalThreads ?? 0) 个逻辑线程 / \(applied?.threadCopies ?? 0) 个线程副本
                    释放空间: \(freed)
                    备份目录: \(backupDir)
                    """
                )
            }
        case .failure(let error):
            lastResultItem.title = "最近一次结果: 归档清理失败"
            showError("归档清理失败", error: error.localizedDescription)
        }
    }

    private func finishSync(refreshStatus shouldRefreshStatus: Bool = false) {
        isRunningSync = false
        updateActionAvailability()
        if let button = statusItem.button {
            button.image = StatusIcon.make(state: .idle)
            button.toolTip = "Codex Thread Manager"
        }
        if shouldRefreshStatus {
            refreshStatus()
        }
    }

    private func setStatusIconSyncing() {
        if let button = statusItem.button {
            button.image = StatusIcon.make(state: .syncing)
            button.toolTip = "Codex Thread Manager - 正在执行"
        }
    }

    private func updateActionAvailability() {
        let enabled = !isRunningSync
        let providers = latestStatus?.providerOrder ?? []
        let hasAtLeastTwoProviders = providers.count >= 2
        let hasArchivedThreads = (latestSpace?.archived.threadCopies ?? 0) > 0
        let hasValidCleanupRule = self.hasValidCleanupRule

        syncAllItem.isEnabled = enabled && hasAtLeastTwoProviders
        syncSelectedProvidersItem.isEnabled = enabled && hasAtLeastTwoProviders
        cleanupSettingsItem.isEnabled = enabled
        cleanupArchivedItem.isEnabled = enabled && hasArchivedThreads && hasValidCleanupRule
        refreshItem.isEnabled = enabled
    }

    private func selectableProviders() -> [(String, Int)] {
        guard let status = latestStatus else { return [] }
        let order = status.providerOrder.isEmpty ? status.providers.keys.sorted() : status.providerOrder
        return order.map { provider in
            (provider, status.providers[provider, default: 0])
        }
    }

    private func selectedProviderArguments(providers: [String]) -> [String] {
        var arguments = ["sync-selected"]
        for provider in providers {
            arguments.append(contentsOf: ["--provider", provider])
        }
        return arguments
    }

    private func providerListDescription(_ providers: [String]) -> String {
        providers.joined(separator: " | ")
    }

    private func cleanupArguments(apply: Bool) -> [String] {
        var arguments = [
            "cleanup",
            "--scope", "archived",
            "--older-than-days", "\(cleanupOlderThanDays)",
            "--keep-latest", "\(cleanupKeepLatest)",
        ]
        if apply {
            arguments.append("--apply")
        }
        return arguments
    }

    private func cleanupPreviewDescription(_ result: CleanupResult) -> String {
        let providers = providerBreakdown(result.planned.providers)
        let missing = result.planned.missingRollouts > 0 ? "\n缺失 rollout: \(result.planned.missingRollouts) 条副本" : ""
        return """
        \(cleanupRuleDescription())

        预计删除:
        - 逻辑线程: \(result.planned.logicalThreads)
        - 线程副本: \(result.planned.threadCopies)
        - 预计释放: \(formatBytes(result.planned.bytes))
        - 涉及渠道: \(providers)\(missing)
        """
    }

    private func cleanupRuleDescription() -> String {
        switch (cleanupOlderThanDays, cleanupKeepLatest) {
        case let (days, keep) where days > 0 && keep > 0:
            return "只清理 \(days) 天前的已归档线程，并保留最近 \(keep) 个逻辑线程。"
        case let (days, _) where days > 0:
            return "只清理 \(days) 天前的已归档线程，不额外保留最近逻辑线程。"
        case let (_, keep) where keep > 0:
            return "清理全部已归档线程，但保留最近 \(keep) 个逻辑线程。"
        default:
            return "清理规则无效，请先设置有效的天数或保留数量。"
        }
    }

    private func providerBreakdown(_ providers: [String: Int]) -> String {
        if providers.isEmpty {
            return "无"
        }
        return providers
            .sorted { left, right in
                if left.key == right.key {
                    return left.value < right.value
                }
                return left.key < right.key
            }
            .map { "\($0.key) \($0.value)" }
            .joined(separator: " | ")
    }

    private func missingSuffix(_ count: Int) -> String {
        count > 0 ? " | 缺 \(count)" : ""
    }

    private func formatBytes(_ bytes: Int64) -> String {
        ByteCountFormatter.string(fromByteCount: bytes, countStyle: .file)
    }

    private var cleanupOlderThanDays: Int {
        loadStoredInteger(
            forKey: CleanupPreferenceKey.olderThanDays,
            defaultValue: ManagerDefaults.cleanupOlderThanDays
        )
    }

    private var cleanupKeepLatest: Int {
        loadStoredInteger(
            forKey: CleanupPreferenceKey.keepLatest,
            defaultValue: ManagerDefaults.cleanupKeepLatest
        )
    }

    private var hasValidCleanupRule: Bool {
        cleanupOlderThanDays > 0 || cleanupKeepLatest > 0
    }

    private func loadStoredInteger(forKey key: String, defaultValue: Int) -> Int {
        guard defaults.object(forKey: key) != nil else { return defaultValue }
        return max(0, defaults.integer(forKey: key))
    }

    private func refreshCleanupHint() {
        cleanupHintItem.title = "清理策略: \(cleanupRuleDescription())"
    }

    private func parseNonNegativeInteger(_ value: String) -> Int? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, let parsed = Int(trimmed), parsed >= 0 else {
            return nil
        }
        return parsed
    }

    private func makeCleanupSettingsView(
        olderThanField: NSTextField,
        keepLatestField: NSTextField
    ) -> NSView {
        olderThanField.translatesAutoresizingMaskIntoConstraints = false
        keepLatestField.translatesAutoresizingMaskIntoConstraints = false
        olderThanField.placeholderString = "例如 30"
        keepLatestField.placeholderString = "例如 20"
        olderThanField.widthAnchor.constraint(equalToConstant: 96).isActive = true
        keepLatestField.widthAnchor.constraint(equalToConstant: 96).isActive = true

        let daysGroup = makeCleanupFieldGroup(
            title: "归档超过多少天",
            helper: "填 0 表示不按天数限制。",
            field: olderThanField
        )
        let keepGroup = makeCleanupFieldGroup(
            title: "至少保留多少个逻辑线程",
            helper: "填 0 表示不额外保留最近线程。",
            field: keepLatestField
        )
        let hint = NSTextField(wrappingLabelWithString: "两个值不能同时为 0，否则就等于没有任何保护条件。")
        hint.textColor = .secondaryLabelColor
        hint.maximumNumberOfLines = 0

        let container = NSStackView(views: [daysGroup, keepGroup, hint])
        container.orientation = .vertical
        container.alignment = .leading
        container.spacing = 12
        container.edgeInsets = NSEdgeInsets(top: 2, left: 0, bottom: 2, right: 0)
        container.translatesAutoresizingMaskIntoConstraints = false

        let wrapper = NSView(frame: NSRect(x: 0, y: 0, width: 320, height: 176))
        wrapper.addSubview(container)
        NSLayoutConstraint.activate([
            container.leadingAnchor.constraint(equalTo: wrapper.leadingAnchor),
            container.trailingAnchor.constraint(equalTo: wrapper.trailingAnchor),
            container.topAnchor.constraint(equalTo: wrapper.topAnchor),
            container.bottomAnchor.constraint(equalTo: wrapper.bottomAnchor),
        ])
        return wrapper
    }

    private func makeCleanupFieldGroup(
        title: String,
        helper: String,
        field: NSTextField
    ) -> NSView {
        let titleLabel = NSTextField(labelWithString: title)
        titleLabel.font = .systemFont(ofSize: NSFont.systemFontSize, weight: .semibold)

        let helperLabel = NSTextField(wrappingLabelWithString: helper)
        helperLabel.textColor = .secondaryLabelColor
        helperLabel.maximumNumberOfLines = 0

        let row = NSStackView(views: [titleLabel, field])
        row.orientation = .horizontal
        row.alignment = .centerY
        row.distribution = .equalSpacing

        let group = NSStackView(views: [row, helperLabel])
        group.orientation = .vertical
        group.alignment = .leading
        group.spacing = 6
        return group
    }

    private func makeProviderSelectionView(checkboxes: [NSButton]) -> NSView {
        let hint = NSTextField(wrappingLabelWithString: "至少选择 2 个 provider。默认会同步当前勾选的全部项。")
        hint.textColor = .secondaryLabelColor
        hint.maximumNumberOfLines = 0

        let contentViews = checkboxes.map { $0 as NSView } + [hint as NSView]
        let container = NSStackView(views: contentViews)
        container.orientation = .vertical
        container.alignment = .leading
        container.spacing = 8
        container.edgeInsets = NSEdgeInsets(top: 2, left: 0, bottom: 2, right: 0)
        container.translatesAutoresizingMaskIntoConstraints = false

        let height = max(CGFloat(140), CGFloat((checkboxes.count * 28) + 48))
        let wrapper = NSView(frame: NSRect(x: 0, y: 0, width: 320, height: height))
        wrapper.addSubview(container)
        NSLayoutConstraint.activate([
            container.leadingAnchor.constraint(equalTo: wrapper.leadingAnchor),
            container.trailingAnchor.constraint(equalTo: wrapper.trailingAnchor),
            container.topAnchor.constraint(equalTo: wrapper.topAnchor),
            container.bottomAnchor.constraint(equalTo: wrapper.bottomAnchor),
        ])
        return wrapper
    }

    private func runSyncBinary<T: Decodable>(arguments: [String], completion: @escaping (Result<T, Error>) -> Void) {
        guard let binaryURL = ScriptLocator.locate() else {
            completion(.failure(NSError(domain: "CodexThreadManager", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "找不到 codex_thread_manager 二进制",
            ])))
            return
        }

        let completionRelay = CompletionRelay<Result<T, Error>>(completion)

        let process = Process()
        process.executableURL = binaryURL
        process.arguments = arguments

        let outputPipe = Pipe()
        let errorPipe = Pipe()
        process.standardOutput = outputPipe
        process.standardError = errorPipe

        DispatchQueue.global(qos: .userInitiated).async {
            do {
                try process.run()
                process.waitUntilExit()

                let output = String(data: outputPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
                let errorOutput = String(data: errorPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""

                guard process.terminationStatus == 0 else {
                    DispatchQueue.main.async {
                        completionRelay.completion(.failure(NSError(domain: "CodexThreadManager", code: Int(process.terminationStatus), userInfo: [
                            NSLocalizedDescriptionKey: errorOutput.isEmpty ? output : errorOutput,
                        ])))
                    }
                    return
                }

                let envelope = try JSONDecoder().decode(ScriptEnvelope<T>.self, from: Data(output.utf8))
                if envelope.ok, let result = envelope.result {
                    DispatchQueue.main.async {
                        completionRelay.completion(.success(result))
                    }
                } else {
                    DispatchQueue.main.async {
                        completionRelay.completion(.failure(NSError(domain: "CodexThreadManager", code: 2, userInfo: [
                            NSLocalizedDescriptionKey: envelope.error ?? "未知错误",
                        ])))
                    }
                }
            } catch {
                DispatchQueue.main.async {
                    completionRelay.completion(.failure(error))
                }
            }
        }
    }

    private func showError(_ title: String, error: String) {
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = title
        alert.informativeText = error
        alert.runModal()
    }

    private func showInfo(_ title: String, message: String) {
        let alert = NSAlert()
        alert.alertStyle = .informational
        alert.messageText = title
        alert.informativeText = message
        alert.runModal()
    }
}
