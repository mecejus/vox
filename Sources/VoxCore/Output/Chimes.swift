import Foundation

public enum Chime: Sendable {
    case start, stop, error
}

public protocol ChimePlayer: AnyObject {
    func play(_ chime: Chime)
}

/// What production uses: vox makes no sound at all. The pipeline still
/// reports start / stop / error cues so tests can observe them.
public final class NoChimes: ChimePlayer {
    public init() {}
    public func play(_ chime: Chime) {}
}

public final class SilentChimes: ChimePlayer, @unchecked Sendable {
    private let lock = NSLockLite()
    private var log: [Chime] = []
    public init() {}
    public func play(_ chime: Chime) { lock.locked { log.append(chime) } }
    public var played: [Chime] { lock.locked { log } }
}
