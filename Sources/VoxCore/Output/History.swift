import Foundation

/// Every transcript, appended to a plain-text file before it is pasted, so
/// nothing dictated is lost to a failed paste or a paste into the wrong
/// window. One line per dictation (or per chunk of a long one):
/// `2026-11-21 14:03:22<TAB>text`. Bounded like the log: past `maxBytes` the
/// file moves to `<name>.1.txt`, replacing the previous one.
public final class TranscriptHistory: @unchecked Sendable {
    public let url: URL
    private let maxBytes: Int
    private let lock = NSLockLite()
    private let stamp: DateFormatter = {
        let f = DateFormatter()
        f.locale = Locale(identifier: "en_US_POSIX")
        f.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return f
    }()

    public init(url: URL = Paths.historyFile, maxBytes: Int = 2 << 20) {
        self.url = url
        self.maxBytes = maxBytes
    }

    public var previous: URL {
        url.deletingPathExtension().appendingPathExtension("1").appendingPathExtension(url.pathExtension)
    }

    public func append(_ text: String) {
        let flat = text.replacingOccurrences(of: "\n", with: " ")
        lock.locked {
            let line = "\(stamp.string(from: Date()))\t\(flat)\n"
            do {
                let fm = FileManager.default
                try fm.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
                if let size = (try? fm.attributesOfItem(atPath: url.path))?[.size] as? Int, size >= maxBytes {
                    try? fm.removeItem(at: previous)
                    try fm.moveItem(at: url, to: previous)
                }
                if !fm.fileExists(atPath: url.path) {
                    fm.createFile(atPath: url.path, contents: nil, attributes: [.posixPermissions: 0o600])
                }
                let h = try FileHandle(forWritingTo: url)
                defer { try? h.close() }
                try h.seekToEnd()
                try h.write(contentsOf: Data(line.utf8))
            } catch {
                log.warn("history_write_failed", ["error": error])
            }
        }
    }
}
