import Foundation

/// The dictation pipeline: key press → capture → key release → transcribe →
/// output.
///
/// Threads and what they own:
///   - CoreAudio IO thread: the C input callback, writing into the ring.
///   - `state` (serial queue): press/release, the recording session, the
///     drain timer that moves samples from the ring through the resampler
///     into the session's buffer.
///   - worker (one dedicated thread): the model. Transcribes finished
///     recordings in order.
///   - `output` (serial queue): pastes results in order.
///
/// Bounds and drop policy:
///   - Recordings live in a fixed pool of `queueDepth + 2` buffers (one being
///     recorded, one being transcribed, `queueDepth` waiting). A key press
///     when none is free is refused (an error cue, logged): the newest
///     dictation is the one dropped, before the user speaks it, and nothing
///     grows without bound.
///   - A long recording is split into chunks while the key is held: after
///     `chunkSeconds`, at the next pause, it carries on in the next free
///     buffer and the finished chunk is transcribed and pasted while the
///     user keeps talking. A failure then costs one chunk, not the whole
///     dictation, and releasing the key only waits for the last chunk. A
///     chunk that fills its buffer (`maxRecordSeconds`) without a pause is
///     split there; only when no buffer is free is the recording ended.
///   - A failed transcription is retried, the last time on a reloaded
///     model; a failed paste is retried once.
///   - Dictated audio and text live only in memory and are never written
///     anywhere but the pasteboard, for the paste itself.
///   - The ring between the audio thread and the drain holds seconds of
///     audio; if the drain ever falls that far behind, samples are dropped
///     on the audio thread and counted in the log.
///   - Every model call has a deadline; a stuck call is aborted.
public final class Pipeline: @unchecked Sendable {
    public struct Settings {
        public var maxRecordSeconds: Int = 1200
        /// Once a chunk is this long, it is ended at the next pause.
        public var chunkSeconds: Double = 30
        /// Quiet needed to count as a pause; half that once a chunk is
        /// twice `chunkSeconds`.
        public var pauseSeconds: Double = 0.35
        /// How much of the pause is repeated at the start of the next chunk,
        /// so the model never starts mid-sound.
        public var overlapSeconds: Double = 0.2
        /// Delay before the one retry of a failed paste.
        public var pasteRetryDelay: TimeInterval = 0.4
        public var queueDepth: Int = 2
        /// Shorter recordings (or recordings with less speech than this) are
        /// ignored as stray taps.
        public var minSpeechSeconds: Double = 1.0 / 3.0
        /// Consecutive failed transcriptions before the model is reloaded.
        public var reloadAfterFailures: Int = 2
        public var drainInterval: TimeInterval = 0.02
        /// How often a model that failed to load is tried again.
        public var modelRetryInterval: TimeInterval = 30

        public init() {}
    }

    public enum Outcome: Equatable, Sendable {
        case delivered(chars: Int)
        case tooShort
        case noSpeech
        case failed(String)
    }

    /// One finished dictation, reported after its output (or failure).
    public struct Report: Sendable {
        public var id: Int
        public var outcome: Outcome
        public var audioSeconds: Double
        /// Key release (end of speech) to text handed to the system.
        public var latency: Double?
        /// Time spent in the model alone.
        public var inference: Double?
    }

    public let settings: Settings
    private let source: AudioSource
    private let transcriber: Transcriber
    private let output: TextOutput
    private let chimes: ChimePlayer

    private let state = DispatchQueue(label: "vox.state", qos: .userInteractive)
    private let outputQueue = DispatchQueue(label: "vox.output", qos: .userInitiated)
    private let pool: BufferPool

    /// Called on an arbitrary thread after each dictation completes.
    public var onReport: ((Report) -> Void)?
    /// Called when the audio source cannot recover in-process.
    public var onUnrecoverable: ((String) -> Void)?

    // MARK: state-queue-only

    private final class Session {
        let id: Int
        let buffer: SampleBuffer
        let resampler: Resampler
        var drainTimer: DispatchSourceTimer?
        var capped = false
        /// Resampled samples that arrived after the buffer filled; they open
        /// the next chunk when the recording rolls over.
        var spill: [Float] = []
        var pause = Speech.PauseDetector()
        init(id: Int, buffer: SampleBuffer, resampler: Resampler) {
            self.id = id
            self.buffer = buffer
            self.resampler = resampler
        }
    }

    private var session: Session?
    private var resampler: Resampler?
    private var nextID = 1
    private var scratch = [Float](repeating: 0, count: 8192)

    // MARK: worker

    private struct Job {
        let id: Int
        let buffer: SampleBuffer
        let released: DispatchTime
    }

    private let jobsLock = NSCondition()
    private var jobs: [Job] = []          // never longer than the pool
    private var stopping = false
    private var worker: Thread?
    private var modelReady = false        // guarded by jobsLock
    private var inFlightSamples = 0       // guarded by jobsLock
    private var consecutiveFailures = 0   // worker only
    private let workerDone = DispatchSemaphore(value: 0)

    public init(source: AudioSource, transcriber: Transcriber, output: TextOutput, chimes: ChimePlayer, settings: Settings = Settings()) {
        self.source = source
        self.transcriber = transcriber
        self.output = output
        self.chimes = chimes
        self.settings = settings
        pool = BufferPool(count: settings.queueDepth + 2, capacity: settings.maxRecordSeconds * Speech.rate)
        source.onEvent = { [weak self] e in self?.handle(e) }
    }

    // MARK: - Lifecycle

    /// Starts the worker, which loads the model (once) and warms it up.
    /// Returns immediately; `waitUntilReady` blocks for tests and tools.
    public func start() {
        let t = Thread { [weak self] in self?.workerLoop() }
        t.name = "vox.worker"
        t.qualityOfService = .userInteractive
        t.stackSize = 8 << 20
        worker = t
        t.start()
    }

    public var isModelReady: Bool {
        jobsLock.lock()
        defer { jobsLock.unlock() }
        return modelReady
    }

    @discardableResult
    public func waitUntilReady(timeout: TimeInterval) -> Bool {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if isModelReady { return true }
            Thread.sleep(forTimeInterval: 0.05)
        }
        return isModelReady
    }

    /// Ends any recording, finishes queued dictations, stops the worker and
    /// frees the model. Waits at least `timeout`, and longer while queued
    /// audio remains (about 1 s per 2 s of audio), so a restart does not
    /// throw away a long dictation that is still being transcribed.
    public func shutdown(timeout: TimeInterval = 10) {
        state.sync {
            if let s = session { finish(s, released: .now(), reason: "shutdown") }
        }
        jobsLock.lock()
        let queued = jobs.reduce(0) { $0 + $1.buffer.count } + inFlightSamples
        stopping = true
        jobsLock.broadcast()
        jobsLock.unlock()
        let wait = max(timeout, Double(queued) / Double(Speech.rate) / 2 + 10)
        if worker != nil, workerDone.wait(timeout: .now() + wait) == .timedOut {
            log.warn("shutdown_worker_timeout", ["waited_s": wait])
        }
        outputQueue.sync {}
    }

    private var stoppingNow: Bool {
        jobsLock.lock()
        defer { jobsLock.unlock() }
        return stopping
    }

    // MARK: - Key events (any thread)

    public func press() {
        state.async { self.beginRecording() }
    }

    public func release() {
        state.async {
            guard let s = self.session else { return }
            self.finish(s, released: .now(), reason: "release")
        }
    }

    /// Ends a recording without a key event (sleep, device loss).
    public func cancelRecording(reason: String) {
        state.async {
            guard let s = self.session else { return }
            self.finish(s, released: .now(), reason: reason)
        }
    }

    public var isRecording: Bool { state.sync { session != nil } }

    // MARK: - Recording (state queue)

    private func beginRecording() {
        guard session == nil else { return }
        guard isModelReady else {
            log.warn("press_ignored", ["reason": "model not ready"])
            chimes.play(.error)
            return
        }
        guard let buffer = pool.take() else {
            // Backpressure: every buffer is waiting for or in the model.
            log.warn("press_refused", ["reason": "transcription backlog full", "pool": pool.size])
            chimes.play(.error)
            return
        }
        chimes.play(.start)
        do {
            try source.start()
        } catch {
            pool.give(buffer)
            log.error("recording_start_failed", ["error": error])
            chimes.play(.error)
            source.invalidate()
            return
        }
        let rate = source.sampleRate
        if resampler?.inputRate != rate { resampler = Resampler(inputRate: rate) }
        guard let rs = resampler else {
            source.stop()
            pool.give(buffer)
            log.error("recording_start_failed", ["error": "no resampler for \(rate) Hz"])
            chimes.play(.error)
            return
        }
        rs.reset()
        let s = Session(id: nextID, buffer: buffer, resampler: rs)
        nextID += 1
        session = s

        let timer = DispatchSource.makeTimerSource(queue: state)
        timer.schedule(deadline: .now() + settings.drainInterval, repeating: settings.drainInterval, leeway: .milliseconds(5))
        // The timer follows the recording across rollovers, so it acts on
        // whichever session is current.
        timer.setEventHandler { [weak self] in
            guard let self, let s = self.session else { return }
            self.drain(s)
            if s.buffer.isFull {
                if !s.capped, !self.rollOver(s, overlap: 0, reason: "full") {
                    s.capped = true
                    log.warn("recording_capped", ["seconds": self.settings.maxRecordSeconds, "reason": "no free buffer"])
                    self.finish(s, released: .now(), reason: "max duration")
                }
                return
            }
            let seconds = Double(s.buffer.count) / Double(Speech.rate)
            guard seconds >= self.settings.chunkSeconds else { return }
            let needed = seconds >= self.settings.chunkSeconds * 2 ? self.settings.pauseSeconds / 2 : self.settings.pauseSeconds
            if s.pause.quietSeconds >= needed {
                // No free buffer: keep recording into this one and try again
                // at the next pause.
                _ = self.rollOver(s, overlap: Int(self.settings.overlapSeconds * Double(Speech.rate)), reason: "pause")
            }
        }
        s.drainTimer = timer
        timer.resume()
        log.debug("recording_started", ["id": s.id, "rate": rate])
    }

    /// Moves everything in the ring through the resampler into the buffer.
    private func drain(_ s: Session, final: Bool = false) {
        scratch.withUnsafeMutableBufferPointer { tmp in
            while true {
                let n = source.ring.read(into: tmp)
                if n == 0 { break }
                s.resampler.process(UnsafeBufferPointer(rebasing: tmp[0..<n])) { out in
                    let kept = s.buffer.append(out)
                    if kept < out.count { s.spill.append(contentsOf: out[kept...]) }
                    s.pause.feed(out)
                }
                if s.buffer.isFull { break }
            }
            if final {
                s.resampler.process(UnsafeBufferPointer(start: tmp.baseAddress, count: 0), end: true) { out in
                    s.buffer.append(out)
                }
            }
        }
    }

    /// The key is still held: hand the current buffer to the worker as one
    /// chunk and keep recording into a fresh one, without stopping the
    /// microphone or resetting the resampler. The last `overlap` samples are
    /// repeated at the start of the new chunk. Returns false when no buffer
    /// is free.
    private func rollOver(_ s: Session, overlap: Int, reason: String) -> Bool {
        guard session === s, let buffer = pool.take() else { return false }
        let next = Session(id: nextID, buffer: buffer, resampler: s.resampler)
        nextID += 1
        let tail = min(overlap, s.buffer.count)
        if tail > 0 {
            _ = buffer.append(UnsafeBufferPointer(rebasing: s.buffer.samples[(s.buffer.count - tail)...]))
        }
        s.spill.withUnsafeBufferPointer { _ = buffer.append($0) }
        s.spill.removeAll()
        next.pause = s.pause
        next.pause.resetRun()
        next.drainTimer = s.drainTimer
        s.drainTimer = nil
        session = next
        log.info("recording_continued", ["id": s.id, "next": next.id, "reason": reason,
                                         "seconds": Double(s.buffer.count) / Double(Speech.rate)])
        enqueue(Job(id: s.id, buffer: s.buffer, released: .now()))
        return true
    }

    private func finish(_ s: Session, released: DispatchTime, reason: String) {
        guard session === s else { return }
        s.drainTimer?.cancel()
        s.drainTimer = nil
        source.stop()
        chimes.play(.stop)
        drain(s, final: true)
        session = nil

        let health = source.takeHealth()
        if health.droppedSamples > 0 || health.renderErrors > 0 || health.oversizedCallbacks > 0 {
            log.warn("audio_health", ["dropped_samples": health.droppedSamples, "render_errors": health.renderErrors,
                                      "oversized_callbacks": health.oversizedCallbacks])
        }
        log.debug("recording_finished", ["id": s.id, "reason": reason,
                                         "seconds": Double(s.buffer.count) / Double(Speech.rate)])
        enqueue(Job(id: s.id, buffer: s.buffer, released: released))
    }

    private func enqueue(_ job: Job) {
        jobsLock.lock()
        // Cannot exceed the pool: every job owns one of its buffers.
        jobs.append(job)
        jobsLock.signal()
        jobsLock.unlock()
    }

    private func handle(_ e: AudioSourceEvent) {
        switch e {
        case .deviceLost(let why):
            log.warn("audio_device_lost", ["reason": why])
            state.async {
                if let s = self.session { self.finish(s, released: .now(), reason: "device lost") }
                self.source.invalidate()
            }
        case .deviceChanged:
            break
        case .unrecoverable(let why):
            log.error("audio_unrecoverable", ["reason": why])
            onUnrecoverable?(why)
        }
    }

    // MARK: - Worker thread

    private func workerLoop() {
        defer {
            transcriber.unload()
            workerDone.signal()
        }
        loadModel()
        while true {
            jobsLock.lock()
            while jobs.isEmpty && !stopping { jobsLock.wait() }
            if jobs.isEmpty && stopping {
                jobsLock.unlock()
                return
            }
            let job = jobs.removeFirst()
            inFlightSamples = job.buffer.count
            jobsLock.unlock()
            process(job)
            jobsLock.lock()
            inFlightSamples = 0
            jobsLock.unlock()
        }
    }

    /// Loads the model, retrying periodically if the file is missing or bad,
    /// until it works or shutdown begins. Never exits the process.
    private func loadModel() {
        while true {
            do {
                try transcriber.load()
                warmUp()
                jobsLock.lock()
                modelReady = true
                jobsLock.unlock()
                return
            } catch {
                log.error("model_load_failed", ["error": error, "retry_in_s": settings.modelRetryInterval])
                jobsLock.lock()
                modelReady = false
                let deadline = Date().addingTimeInterval(settings.modelRetryInterval)
                while !stopping, jobsLock.wait(until: deadline) {}
                let stop = stopping
                jobsLock.unlock()
                if stop { return }
            }
        }
    }

    /// One throwaway inference so Metal compiles its pipelines and the
    /// weights are paged in before the first real dictation.
    private func warmUp() {
        let started = DispatchTime.now()
        var silence = [Float](repeating: 0, count: Speech.rate)
        for i in silence.indices { silence[i] = Float(i % 7) * 1e-5 }
        _ = try? silence.withUnsafeBufferPointer { try transcriber.transcribe($0, timeout: 120) }
        log.info("model_warm", ["ms": Self.ms(since: started)])
    }

    private func process(_ job: Job) {
        defer { pool.give(job.buffer) }
        let all = UnsafeMutableBufferPointer(mutating: job.buffer.samples)
        let audioSeconds = Double(all.count) / Double(Speech.rate)
        var report = Report(id: job.id, outcome: .noSpeech, audioSeconds: audioSeconds)

        let fixed = Speech.sanitize(all)
        if fixed > 0 { log.warn("audio_samples_repaired", ["id": job.id, "count": fixed]) }

        let analysis = Speech.analyze(UnsafeBufferPointer(all))
        if analysis.peak == 0 && all.count > 0 {
            // All-zero input is what a revoked microphone permission (or a
            // muted device) delivers.
            log.warn("audio_all_silent", ["id": job.id, "hint": "microphone muted or access revoked?"])
        }
        if Double(all.count) / Double(Speech.rate) < settings.minSpeechSeconds {
            report.outcome = .tooShort
            finishReport(report)
            return
        }
        if analysis.speech.isEmpty {
            report.outcome = .noSpeech
            finishReport(report)
            return
        }
        let speech = UnsafeBufferPointer(rebasing: UnsafeBufferPointer(all)[analysis.speech])
        if Double(speech.count) / Double(Speech.rate) < settings.minSpeechSeconds {
            report.outcome = .tooShort
            finishReport(report)
            return
        }

        if !isModelReady { loadModel() }
        let timeout = max(20, Double(speech.count) / Double(Speech.rate) * 3 + 10)
        let t0 = DispatchTime.now()
        var text: String?
        // What was said exists only in this buffer, so a model error is not
        // the end of it: retry, and the second failure in a row reloads the
        // model so the last attempt runs on a fresh one.
        for attempt in 1...3 {
            do {
                text = try transcriber.transcribe(speech, timeout: timeout)
                consecutiveFailures = 0
                break
            } catch {
                consecutiveFailures += 1
                report.outcome = .failed(String(describing: error))
                log.error("transcription_failed", ["id": job.id, "error": error, "attempt": attempt,
                                                   "consecutive": consecutiveFailures])
                if consecutiveFailures >= settings.reloadAfterFailures {
                    log.warn("model_reload", ["after_failures": consecutiveFailures])
                    jobsLock.lock()
                    modelReady = false
                    jobsLock.unlock()
                    loadModel()
                    consecutiveFailures = 0
                }
                if stoppingNow { break }
            }
        }
        guard let text else {
            report.inference = Self.seconds(since: t0)
            chimes.play(.error)
            finishReport(report)
            return
        }
        report.inference = Self.seconds(since: t0)
        guard !text.isEmpty else {
            report.outcome = .noSpeech
            finishReport(report)
            return
        }

        outputQueue.async {
            for attempt in 1...2 {
                do {
                    let at = try self.output.deliver(text)
                    report.outcome = .delivered(chars: text.count)
                    report.latency = Double(at.uptimeNanoseconds &- job.released.uptimeNanoseconds) / 1e9
                    break
                } catch {
                    report.outcome = .failed(String(describing: error))
                    log.error("output_failed", ["id": job.id, "error": error, "attempt": attempt])
                    if attempt == 1 { Thread.sleep(forTimeInterval: self.settings.pasteRetryDelay) }
                }
            }
            if case .failed = report.outcome { self.chimes.play(.error) }
            self.finishReport(report)
        }
    }

    private func finishReport(_ r: Report) {
        var f: [String: Any] = ["id": r.id, "audio_s": r.audioSeconds]
        switch r.outcome {
        case .delivered(let chars): f["outcome"] = "delivered"; f["chars"] = chars
        case .tooShort: f["outcome"] = "too_short"
        case .noSpeech: f["outcome"] = "no_speech"
        case .failed(let why): f["outcome"] = "failed"; f["error"] = why
        }
        if let l = r.latency { f["latency_ms"] = Int(l * 1000) }
        if let i = r.inference { f["inference_ms"] = Int(i * 1000) }
        log.info("dictation", f)
        onReport?(r)
    }

    static func seconds(since t: DispatchTime) -> Double {
        Double(DispatchTime.now().uptimeNanoseconds - t.uptimeNanoseconds) / 1e9
    }

    static func ms(since t: DispatchTime) -> Int { Int(seconds(since: t) * 1000) }

    // MARK: - Offline use

    /// Feeds a whole recording through the pipeline as if the key were held
    /// for its duration: press, play the file into the ring, release at the
    /// end. Used by the integration test, `vox transcribe` and the
    /// benchmarks. The source must be the FileSource this pipeline was built
    /// with.
    /// Returns false if the press was refused (no recording happened).
    @discardableResult
    public func runFile(_ file: FileSource) -> Bool {
        press()
        guard state.sync(execute: { session != nil }) else { return false }
        file.waitUntilFed()
        // Let the drain pick up the tail before "releasing the key".
        Thread.sleep(forTimeInterval: settings.drainInterval * 2)
        release()
        state.sync {}
        return true
    }
}
