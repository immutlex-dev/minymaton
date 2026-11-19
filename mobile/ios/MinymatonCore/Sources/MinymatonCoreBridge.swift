import Foundation

/// Swift face of the Rust core (crates/minymaton-mobile-core, src/ios.rs).
/// Requests block, so they run in order on one serial queue. Events arrive
/// as JSON strings through the C callback and are forwarded to `onEvent`.
@objc(MinymatonCoreBridge) public final class MinymatonCoreBridge: NSObject {
    @objc public var onEvent: ((String) -> Void)?

    private let queue = DispatchQueue(label: "dev.minymaton.core")
    private var handle: UnsafeMutableRawPointer?
    private var retained = true

    @objc public init(dataDir: String) throws {
        super.init()
        let box = Unmanaged.passRetained(self)
        let raw = dataDir.withCString { minymaton_open($0, minymatonEventTrampoline, box.toOpaque()) }
        guard let raw else {
            box.release()
            retained = false
            throw NSError(domain: "MinymatonCore", code: 1, userInfo: [NSLocalizedDescriptionKey: "the core failed to open"])
        }
        handle = raw
    }

    deinit {
        close()
    }

    @objc public func close() {
        guard let handle else { return }
        self.handle = nil
        minymaton_close(handle)
        if retained {
            retained = false
            Unmanaged.passUnretained(self).release()
        }
    }

    @objc public func request(_ command: String, completion: @escaping (String?, NSError?) -> Void) {
        queue.async { [weak self] in
            guard let self, let handle = self.handle else {
                completion(nil, NSError(domain: "MinymatonCore", code: 2, userInfo: [NSLocalizedDescriptionKey: "the core is closed"]))
                return
            }
            let response = command.withCString { minymaton_request(handle, $0) }
            guard let response else {
                completion(nil, NSError(domain: "MinymatonCore", code: 3, userInfo: [NSLocalizedDescriptionKey: "the core returned no response"]))
                return
            }
            let value = String(cString: response)
            minymaton_free_string(response)
            completion(value, nil)
        }
    }

    fileprivate func receive(event: String) {
        onEvent?(event)
    }
}

private func minymatonEventTrampoline(context: UnsafeMutableRawPointer?, event: UnsafePointer<CChar>?) {
    guard let context, let event else { return }
    let bridge = Unmanaged<MinymatonCoreBridge>.fromOpaque(context).takeUnretainedValue()
    bridge.receive(event: String(cString: event))
}
