import Foundation

enum ScriptLocator {
    private static let binaryName = "codex_thread_manager"
    private static let relativeCandidates = [
        "rust/codex_thread_manager/target/release/codex_thread_manager",
        "rust/codex_thread_manager/target/debug/codex_thread_manager",
        "target/release/codex_thread_manager",
        "target/debug/codex_thread_manager",
        "rust/codex_thread_manager/target/release/codex_provider_sync",
        "rust/codex_thread_manager/target/debug/codex_provider_sync",
        "target/release/codex_provider_sync",
        "target/debug/codex_provider_sync",
    ]

    static func locate() -> URL? {
        let fileManager = FileManager.default

        let overrides = [
            ProcessInfo.processInfo.environment["CODEX_THREAD_MANAGER_BINARY"],
            ProcessInfo.processInfo.environment["CODEX_SYNC_BINARY"],
        ]
        for override in overrides.compactMap({ $0 }) {
            let url = URL(fileURLWithPath: override)
            if fileManager.isExecutableFile(atPath: url.path) {
                return url
            }
        }

        if let resourceURL = Bundle.main.resourceURL {
            let candidate = resourceURL.appendingPathComponent(binaryName)
            if fileManager.isExecutableFile(atPath: candidate.path) {
                return candidate
            }
        }

        let currentDirectory = URL(fileURLWithPath: fileManager.currentDirectoryPath, isDirectory: true)
        if let candidate = searchUpwards(from: currentDirectory),
           fileManager.isExecutableFile(atPath: candidate.path) {
            return candidate
        }

        if let executableURL = Bundle.main.executableURL?.deletingLastPathComponent(),
           let candidate = searchUpwards(from: executableURL),
           fileManager.isExecutableFile(atPath: candidate.path) {
            return candidate
        }

        return nil
    }

    private static func searchUpwards(from start: URL) -> URL? {
        var current = start
        for _ in 0..<8 {
            for relativePath in relativeCandidates {
                let candidate = current.appendingPathComponent(relativePath)
                if FileManager.default.isExecutableFile(atPath: candidate.path) {
                    return candidate
                }
            }
            current.deleteLastPathComponent()
        }
        return nil
    }
}
