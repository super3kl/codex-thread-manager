#!/usr/bin/env swift

import AppKit
import Foundation

let outputDirectory = CommandLine.arguments.dropFirst().first.map(URL.init(fileURLWithPath:))

guard let outputDirectory else {
    fputs("用法: generate_app_icon.swift <output-iconset-dir>\n", stderr)
    exit(1)
}

let iconSpecs: [(size: Int, name: String)] = [
    (16, "icon_16x16.png"),
    (32, "icon_16x16@2x.png"),
    (32, "icon_32x32.png"),
    (64, "icon_32x32@2x.png"),
    (128, "icon_128x128.png"),
    (256, "icon_128x128@2x.png"),
    (256, "icon_256x256.png"),
    (512, "icon_256x256@2x.png"),
    (512, "icon_512x512.png"),
    (1024, "icon_512x512@2x.png"),
]

let fileManager = FileManager.default
try fileManager.createDirectory(at: outputDirectory, withIntermediateDirectories: true)

for spec in iconSpecs {
    let image = drawIcon(size: CGFloat(spec.size))
    let targetURL = outputDirectory.appendingPathComponent(spec.name)
    try savePNG(image: image, to: targetURL)
}

func drawIcon(size: CGFloat) -> NSImage {
    let image = NSImage(size: NSSize(width: size, height: size))
    image.lockFocus()
    defer { image.unlockFocus() }

    let canvas = NSRect(x: 0, y: 0, width: size, height: size)
    NSColor.clear.setFill()
    canvas.fill()

    let shellRect = canvas.insetBy(dx: size * 0.07, dy: size * 0.07)
    let shellRadius = size * 0.235
    let shellPath = NSBezierPath(roundedRect: shellRect, xRadius: shellRadius, yRadius: shellRadius)

    let background = NSGradient(colors: [
        NSColor(srgbRed: 0.05, green: 0.09, blue: 0.21, alpha: 1),
        NSColor(srgbRed: 0.11, green: 0.33, blue: 0.93, alpha: 1),
        NSColor(srgbRed: 0.14, green: 0.81, blue: 0.84, alpha: 1),
    ])!
    background.draw(in: shellPath, angle: -58)

    NSGraphicsContext.saveGraphicsState()
    shellPath.addClip()

    let topGlow = NSBezierPath(ovalIn: NSRect(
        x: shellRect.minX - size * 0.06,
        y: shellRect.midY,
        width: size * 0.72,
        height: size * 0.52
    ))
    NSColor(srgbRed: 0.75, green: 0.94, blue: 1.0, alpha: 0.20).setFill()
    topGlow.fill()

    let warmGlow = NSBezierPath(ovalIn: NSRect(
        x: shellRect.midX - size * 0.05,
        y: shellRect.minY - size * 0.08,
        width: size * 0.54,
        height: size * 0.44
    ))
    NSColor(srgbRed: 1.0, green: 0.54, blue: 0.38, alpha: 0.26).setFill()
    warmGlow.fill()

    let streak = NSBezierPath()
    streak.move(to: CGPoint(x: shellRect.minX + size * 0.16, y: shellRect.maxY - size * 0.19))
    streak.line(to: CGPoint(x: shellRect.maxX - size * 0.18, y: shellRect.minY + size * 0.22))
    streak.lineWidth = size * 0.055
    NSColor(srgbRed: 1.0, green: 1.0, blue: 1.0, alpha: 0.05).setStroke()
    streak.stroke()

    NSGraphicsContext.restoreGraphicsState()

    let edgeStroke = NSBezierPath(roundedRect: shellRect, xRadius: shellRadius, yRadius: shellRadius)
    edgeStroke.lineWidth = max(3, size * 0.012)
    NSColor(srgbRed: 1, green: 1, blue: 1, alpha: 0.10).setStroke()
    edgeStroke.stroke()

    let innerRect = shellRect.insetBy(dx: size * 0.15, dy: size * 0.18)
    let strokeWidth = size * 0.085

    drawLoop(
        center: CGPoint(x: innerRect.midX - size * 0.11, y: innerRect.midY + size * 0.01),
        size: CGSize(width: size * 0.36, height: size * 0.225),
        angle: -30,
        color: NSColor(srgbRed: 1.0, green: 0.55, blue: 0.38, alpha: 1.0),
        strokeWidth: strokeWidth
    )
    drawLoop(
        center: CGPoint(x: innerRect.midX + size * 0.11, y: innerRect.midY - size * 0.01),
        size: CGSize(width: size * 0.36, height: size * 0.225),
        angle: 30,
        color: NSColor(srgbRed: 0.58, green: 1.0, blue: 0.94, alpha: 1.0),
        strokeWidth: strokeWidth
    )

    let coreDotRect = NSRect(
        x: canvas.midX - size * 0.045,
        y: canvas.midY - size * 0.045,
        width: size * 0.09,
        height: size * 0.09
    )
    let coreDot = NSBezierPath(ovalIn: coreDotRect)
    NSColor(srgbRed: 0.98, green: 0.99, blue: 1.0, alpha: 0.96).setFill()
    coreDot.fill()

    let sparkPath = NSBezierPath()
    sparkPath.move(to: CGPoint(x: shellRect.maxX - size * 0.26, y: shellRect.maxY - size * 0.25))
    sparkPath.line(to: CGPoint(x: shellRect.maxX - size * 0.20, y: shellRect.maxY - size * 0.19))
    sparkPath.lineWidth = max(4, size * 0.02)
    sparkPath.lineCapStyle = .round
    NSColor(srgbRed: 1.0, green: 1.0, blue: 1.0, alpha: 0.72).setStroke()
    sparkPath.stroke()

    let sparkPath2 = NSBezierPath()
    sparkPath2.move(to: CGPoint(x: shellRect.maxX - size * 0.23, y: shellRect.maxY - size * 0.29))
    sparkPath2.line(to: CGPoint(x: shellRect.maxX - size * 0.23, y: shellRect.maxY - size * 0.15))
    sparkPath2.lineWidth = max(4, size * 0.02)
    sparkPath2.lineCapStyle = .round
    NSColor(srgbRed: 1.0, green: 1.0, blue: 1.0, alpha: 0.72).setStroke()
    sparkPath2.stroke()

    return image
}

func drawLoop(center: CGPoint, size: CGSize, angle: CGFloat, color: NSColor, strokeWidth: CGFloat) {
    let loopRect = NSRect(
        x: center.x - size.width * 0.5,
        y: center.y - size.height * 0.5,
        width: size.width,
        height: size.height
    )
    let path = NSBezierPath(roundedRect: loopRect, xRadius: size.height * 0.5, yRadius: size.height * 0.5)
    path.lineWidth = strokeWidth
    path.lineCapStyle = .round
    path.lineJoinStyle = .round

    let glow = NSShadow()
    glow.shadowBlurRadius = strokeWidth * 0.55
    glow.shadowOffset = .zero
    glow.shadowColor = color.withAlphaComponent(0.28)

    NSGraphicsContext.saveGraphicsState()
    glow.set()
    color.setStroke()

    var transform = AffineTransform(translationByX: center.x, byY: center.y)
    transform.rotate(byDegrees: angle)
    transform.translate(x: -center.x, y: -center.y)
    path.transform(using: transform)
    path.stroke()
    NSGraphicsContext.restoreGraphicsState()
}

func savePNG(image: NSImage, to url: URL) throws {
    guard let tiff = image.tiffRepresentation,
          let bitmap = NSBitmapImageRep(data: tiff),
          let data = bitmap.representation(using: .png, properties: [:]) else {
        throw NSError(domain: "AppIcon", code: 1, userInfo: [NSLocalizedDescriptionKey: "PNG 编码失败"])
    }
    try data.write(to: url)
}
