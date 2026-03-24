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

private struct DirectionalResult: Decodable {
    let mode: String
    let source: String?
    let target: String?
    let planned: AppliedSummary?
    let applied: AppliedSummary?
    let backupDir: String?
    let status: [String: Int]?

    private enum CodingKeys: String, CodingKey {
        case mode
        case source
        case target
        case planned
        case applied
        case backupDir = "backup_dir"
        case status
    }
}

private struct BidirectionalResult: Decodable {
    let mode: String
    let runs: [DirectionalResult]
    let finalStatus: StatusResult

    private enum CodingKeys: String, CodingKey {
        case mode
        case runs
        case finalStatus = "final_status"
    }
}

private struct StatusResult: Decodable {
    let providers: [String: Int]
    let pairLinks: [String: Int]
    let paths: StatusPaths

    private enum CodingKeys: String, CodingKey {
        case providers
        case pairLinks = "pair_links"
        case paths
    }
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
    case bidirectional
    case openAIToCPA
    case cPAToOpenAI

    var title: String {
        switch self {
        case .allProviders:
            return "同步全部 provider"
        case .bidirectional:
            return "双向同步 openai ⇄ cpa"
        case .openAIToCPA:
            return "同步 openai -> cpa"
        case .cPAToOpenAI:
            return "同步 cpa -> openai"
        }
    }

    var arguments: [String] {
        switch self {
        case .allProviders:
            return ["sync-all"]
        case .bidirectional:
            return ["sync-bidirectional", "--provider-a", "openai", "--provider-b", "cpa"]
        case .openAIToCPA:
            return ["sync", "--source", "openai", "--target", "cpa"]
        case .cPAToOpenAI:
            return ["sync", "--source", "cpa", "--target", "openai"]
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
    private let syncBothItem = NSMenuItem(title: SyncAction.bidirectional.title, action: #selector(syncBoth), keyEquivalent: "")
    private let syncOpenAIToCPAItem = NSMenuItem(title: SyncAction.openAIToCPA.title, action: #selector(syncOpenAIToCPA), keyEquivalent: "")
    private let syncCPAToOpenAIItem = NSMenuItem(title: SyncAction.cPAToOpenAI.title, action: #selector(syncCPAToOpenAI), keyEquivalent: "")
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
        syncBothItem.target = self
        syncOpenAIToCPAItem.target = self
        syncCPAToOpenAIItem.target = self
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
        menu.addItem(syncBothItem)
        menu.addItem(syncOpenAIToCPAItem)
        menu.addItem(syncCPAToOpenAIItem)
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
    private func syncBoth() {
        performSync(.bidirectional)
    }

    @objc
    private func syncOpenAIToCPA() {
        performSync(.openAIToCPA)
    }

    @objc
    private func syncCPAToOpenAI() {
        performSync(.cPAToOpenAI)
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

        switch action {
        case .allProviders:
            runSyncBinary(arguments: action.arguments) { [weak self] (result: Result<MeshSyncResult, Error>) in
                self?.handleMeshResult(result)
            }
        case .bidirectional:
            runSyncBinary(arguments: action.arguments) { [weak self] (result: Result<BidirectionalResult, Error>) in
                self?.handleBidirectionalResult(result)
            }
        case .openAIToCPA, .cPAToOpenAI:
            runSyncBinary(arguments: action.arguments) { [weak self] (result: Result<DirectionalResult, Error>) in
                self?.handleDirectionalResult(result, title: action.title)
            }
        }
    }

    private func handleMeshResult(_ result: Result<MeshSyncResult, Error>) {
        defer { finishSync() }
        switch result {
        case .success(let summary):
            let applied = summary.applied
            lastResultItem.title = "最近一次结果: 全量同步 p\(summary.providers.count) c\(applied?.create ?? 0) a\(applied?.adopt ?? 0) u\(applied?.update ?? 0) r\(applied?.repair ?? 0)"
            applyMeshStatus(summary.finalStatus)
            refreshSpaceStatus(showError: false)
        case .failure(let error):
            lastResultItem.title = "最近一次结果: 全量同步失败"
            showError("同步全部 provider 失败", error: error.localizedDescription)
        }
    }

    private func handleDirectionalResult(_ result: Result<DirectionalResult, Error>, title: String) {
        defer { finishSync() }
        switch result {
        case .success(let summary):
            let applied = summary.applied
            lastResultItem.title = "最近一次结果: \(title) c\(applied?.create ?? 0) a\(applied?.adopt ?? 0) u\(applied?.update ?? 0) r\(applied?.repair ?? 0)"
            refreshStatus()
        case .failure(let error):
            lastResultItem.title = "最近一次结果: \(title) 失败"
            showError("同步失败", error: error.localizedDescription)
        }
    }

    private func handleBidirectionalResult(_ result: Result<BidirectionalResult, Error>) {
        defer { finishSync() }
        switch result {
        case .success(let summary):
            let description = summary.runs.compactMap { run -> String? in
                guard let source = run.source, let target = run.target else { return nil }
                let applied = run.applied
                return "\(source)->\(target): c\(applied?.create ?? 0) a\(applied?.adopt ?? 0) u\(applied?.update ?? 0) r\(applied?.repair ?? 0)"
            }.joined(separator: " | ")
            lastResultItem.title = "最近一次结果: \(description)"
            refreshStatus()
        case .failure(let error):
            lastResultItem.title = "最近一次结果: 双向同步失败"
            showError("双向同步失败", error: error.localizedDescription)
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

    private func finishSync() {
        isRunningSync = false
        updateActionAvailability()
        if let button = statusItem.button {
            button.image = StatusIcon.make(state: .idle)
            button.toolTip = "Codex Thread Manager"
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
        let providerSet = Set(providers)
        let hasAtLeastTwoProviders = providers.count >= 2
        let hasOpenAIAndCPA = providerSet.contains("openai") && providerSet.contains("cpa")
        let hasArchivedThreads = (latestSpace?.archived.threadCopies ?? 0) > 0
        let hasValidCleanupRule = self.hasValidCleanupRule

        syncAllItem.isEnabled = enabled && hasAtLeastTwoProviders
        syncBothItem.isEnabled = enabled && hasOpenAIAndCPA
        syncOpenAIToCPAItem.isEnabled = enabled && hasOpenAIAndCPA
        syncCPAToOpenAIItem.isEnabled = enabled && hasOpenAIAndCPA
        cleanupSettingsItem.isEnabled = enabled
        cleanupArchivedItem.isEnabled = enabled && hasArchivedThreads && hasValidCleanupRule
        refreshItem.isEnabled = enabled
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

    private func runSyncBinary<T: Decodable>(arguments: [String], completion: @escaping (Result<T, Error>) -> Void) {
        guard let binaryURL = ScriptLocator.locate() else {
            completion(.failure(NSError(domain: "CodexThreadManager", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "找不到 codex_thread_manager 二进制",
            ])))
            return
        }

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
                        completion(.failure(NSError(domain: "CodexThreadManager", code: Int(process.terminationStatus), userInfo: [
                            NSLocalizedDescriptionKey: errorOutput.isEmpty ? output : errorOutput,
                        ])))
                    }
                    return
                }

                let envelope = try JSONDecoder().decode(ScriptEnvelope<T>.self, from: Data(output.utf8))
                if envelope.ok, let result = envelope.result {
                    DispatchQueue.main.async {
                        completion(.success(result))
                    }
                } else {
                    DispatchQueue.main.async {
                        completion(.failure(NSError(domain: "CodexThreadManager", code: 2, userInfo: [
                            NSLocalizedDescriptionKey: envelope.error ?? "未知错误",
                        ])))
                    }
                }
            } catch {
                DispatchQueue.main.async {
                    completion(.failure(error))
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
