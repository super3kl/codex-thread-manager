// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "CodexThreadManager",
    platforms: [
        .macOS(.v13),
    ],
    products: [
        .executable(
            name: "CodexThreadManager",
            targets: ["CodexThreadManager"]
        ),
    ],
    targets: [
        .executableTarget(
            name: "CodexThreadManager",
            path: "Sources/CodexThreadManager"
        ),
    ]
)
