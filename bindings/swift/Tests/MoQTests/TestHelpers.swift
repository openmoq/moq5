import Testing
import Foundation
@testable import MoQ

private struct PumpError: Error, CustomStringConvertible {
    let description: String
}

/// Pump all pending sendControl actions from source into destination.
/// Throws on any non-sendControl action.
func pumpControl(from source: Session, to destination: Session, now: UInt64 = 0) throws {
    try source.drainActions { action in
        guard case .sendControl(let data) = action else {
            throw PumpError(description: "pumpControl: expected sendControl, got \(action)")
        }
        try data.withUnsafeBytes { bytes in
            try destination.receiveControlBytes(bytes, now: now)
        }
    }
}

/// Create a pair of established Swift Sessions with setup events drained.
func makeEstablishedPair() throws -> (client: Session, server: Session) {
    var clientCfg = Session.Configuration(perspective: .client)
    clientCfg.sendRequestCapacity = true
    clientCfg.initialRequestCapacity = 16

    var serverCfg = Session.Configuration(perspective: .server)
    serverCfg.sendRequestCapacity = true
    serverCfg.initialRequestCapacity = 16

    let client = try Session(configuration: clientCfg)
    let server = try Session(configuration: serverCfg)

    try client.start()
    try pumpControl(from: client, to: server)
    try pumpControl(from: server, to: client)

    let clientEvents = client.pollEvents()
    let serverEvents = server.pollEvents()
    expectSetupComplete(clientEvents, sourceLabel: "client")
    expectSetupComplete(serverEvents, sourceLabel: "server")

    return (client, server)
}

/// Pump all pending actions (control + data) from source into destination.
/// Throws on action kinds not yet handled (reset, stop, bidi, datagram).
func pumpAll(from source: Session, to destination: Session, now: UInt64 = 0) throws {
    try source.drainActions { action in
        switch action {
        case .sendControl(let data):
            try data.withUnsafeBytes { bytes in
                try destination.receiveControlBytes(bytes, now: now)
            }
        case .sendData(let stream, let header, let payload, let fin):
            var bytes = [UInt8](header)
            if let buf = payload {
                buf.withUnsafeBytes { raw in
                    bytes.append(contentsOf: raw)
                }
            }
            try bytes.withUnsafeBufferPointer { ptr in
                let raw = UnsafeRawBufferPointer(
                    start: ptr.baseAddress, count: ptr.count)
                try destination.receiveDataBytes(
                    on: stream, raw, fin: fin, now: now)
            }
        case .sendDatagram(let data):
            try data.withUnsafeBytes { bytes in
                try destination.receiveDatagram(bytes, now: now)
            }
        default:
            throw PumpError(
                description: "pumpAll: unhandled action \(action)")
        }
    }
}

/// Assert that an event list contains `.setupComplete`.
func expectSetupComplete(_ events: [Event], sourceLabel: String = "session") {
    #expect(events.contains(.setupComplete), "\(sourceLabel) should emit setupComplete")
}
