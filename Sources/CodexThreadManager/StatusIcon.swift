import AppKit

enum StatusIcon {
    enum State {
        case idle
        case syncing
    }

    static func make(state: State, size: CGFloat = 18) -> NSImage {
        let image = NSImage(size: NSSize(width: size, height: size), flipped: false) { rect in
            draw(state: state, in: rect)
            return true
        }
        image.isTemplate = true
        return image
    }

    private static func draw(state: State, in rect: NSRect) {
        let strokeWidth = max(1.65, rect.width * 0.105)
        let loopWidth = rect.width * 0.46
        let loopHeight = rect.height * 0.30
        let radius = loopHeight * 0.5
        let leftCenter = CGPoint(x: rect.minX + rect.width * 0.39, y: rect.midY)
        let rightCenter = CGPoint(x: rect.minX + rect.width * 0.61, y: rect.midY)

        NSColor.black.setStroke()
        drawLoop(
            center: leftCenter,
            angle: -30,
            size: CGSize(width: loopWidth, height: loopHeight),
            radius: radius,
            strokeWidth: strokeWidth
        )
        drawLoop(
            center: rightCenter,
            angle: 30,
            size: CGSize(width: loopWidth, height: loopHeight),
            radius: radius,
            strokeWidth: strokeWidth
        )

        if state == .syncing {
            let dotSize = max(1.9, rect.width * 0.11)
            NSColor.black.setFill()
            let leadingDot = NSRect(
                x: rect.minX + rect.width * 0.16 - dotSize * 0.5,
                y: rect.minY + rect.height * 0.32 - dotSize * 0.5,
                width: dotSize,
                height: dotSize
            )
            let trailingDot = NSRect(
                x: rect.minX + rect.width * 0.84 - dotSize * 0.5,
                y: rect.minY + rect.height * 0.68 - dotSize * 0.5,
                width: dotSize,
                height: dotSize
            )
            NSBezierPath(ovalIn: leadingDot).fill()
            NSBezierPath(ovalIn: trailingDot).fill()
        }
    }

    private static func drawLoop(
        center: CGPoint,
        angle: CGFloat,
        size: CGSize,
        radius: CGFloat,
        strokeWidth: CGFloat
    ) {
        let rect = NSRect(
            x: center.x - size.width * 0.5,
            y: center.y - size.height * 0.5,
            width: size.width,
            height: size.height
        )
        let path = NSBezierPath(roundedRect: rect, xRadius: radius, yRadius: radius)
        path.lineWidth = strokeWidth
        path.lineCapStyle = .round
        path.lineJoinStyle = .round

        var transform = AffineTransform(translationByX: center.x, byY: center.y)
        transform.rotate(byDegrees: angle)
        transform.translate(x: -center.x, y: -center.y)
        path.transform(using: transform)
        path.stroke()
    }
}
