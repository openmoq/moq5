import CMoQCore
import Foundation

/// Opaque stream identifier used by transport adapters.
public struct StreamRef: Sendable, Hashable {
    public let rawValue: UInt64

    internal init(_ ref: moq_stream_ref_t) {
        self.rawValue = ref._v
    }
}

/// Outbound transport action produced by a session.
// Not Sendable: `sendData` carries `Buffer` (non-atomic, shard-confined rcbuf).
public enum Action {
    case sendControl(Data)
    case closeSession(code: UInt64, reason: String)
    case sendData(stream: StreamRef, header: Data, payload: Buffer?, fin: Bool)
    case resetData(stream: StreamRef, errorCode: UInt64)
    case stopData(stream: StreamRef, errorCode: UInt64)
    case openBidiStream(stream: StreamRef, data: Data, fin: Bool)
    case sendBidiStream(stream: StreamRef, data: Data, fin: Bool)
    case closeBidiStream(stream: StreamRef)
    case sendDatagram(Data)
}

extension Action {
    /// Convert a C action to Swift, taking ownership of any owned refs.
    /// The caller must have already polled this action from the session.
    /// This function calls moq_action_cleanup on the C action.
    internal static func from(_ cAction: inout moq_action_t) -> Action? {
        let kind = cAction.kind
        let result: Action

        switch kind {
        case MOQ_ACTION_SEND_CONTROL:
            let sc = cAction.u.send_control
            let data = Data(
                bytes: sc.data, count: sc.len
            )
            result = .sendControl(data)

        case MOQ_ACTION_CLOSE_SESSION:
            let cs = cAction.u.close_session
            let reason: String
            if cs.reason.data != nil && cs.reason.len > 0 {
                reason = String(
                    bytes: UnsafeBufferPointer(
                        start: cs.reason.data, count: cs.reason.len),
                    encoding: .utf8
                ) ?? ""
            } else {
                reason = ""
            }
            result = .closeSession(code: cs.code, reason: reason)

        case MOQ_ACTION_SEND_DATA:
            let sd = cAction.u.send_data
            let headerLen = Int(sd.header_len)
            let header: Data = withUnsafeBytes(of: sd.header) { buf in
                Data(buf.prefix(headerLen))
            }
            let payload: Buffer?
            if let p = sd.payload {
                // Take ownership of the transferred ref. Null it
                // out so moq_action_cleanup doesn't decref it.
                cAction.u.send_data.payload = nil
                payload = Buffer(adopting: p)
            } else {
                payload = nil
            }
            result = .sendData(
                stream: StreamRef(sd.stream_ref),
                header: header,
                payload: payload,
                fin: sd.fin
            )

        case MOQ_ACTION_RESET_DATA:
            let rd = cAction.u.reset_data
            result = .resetData(
                stream: StreamRef(rd.stream_ref),
                errorCode: rd.error_code
            )

        case MOQ_ACTION_STOP_DATA:
            let sd = cAction.u.stop_data
            result = .stopData(
                stream: StreamRef(sd.stream_ref),
                errorCode: sd.error_code
            )

        case MOQ_ACTION_OPEN_BIDI_STREAM:
            let ob = cAction.u.open_bidi_stream
            let data = Data(
                bytes: ob.data, count: ob.len
            )
            result = .openBidiStream(
                stream: StreamRef(ob.stream_ref), data: data, fin: ob.fin)

        case MOQ_ACTION_SEND_BIDI_STREAM:
            let sb = cAction.u.send_bidi_stream
            let data = Data(
                bytes: sb.data, count: sb.len
            )
            result = .sendBidiStream(
                stream: StreamRef(sb.stream_ref), data: data, fin: sb.fin)

        case MOQ_ACTION_CLOSE_BIDI_STREAM:
            let cb = cAction.u.close_bidi_stream
            result = .closeBidiStream(stream: StreamRef(cb.stream_ref))

        case MOQ_ACTION_SEND_DATAGRAM:
            let dg = cAction.u.send_datagram
            let data = Data(
                bytes: dg.data, count: dg.len
            )
            result = .sendDatagram(data)

        default:
            moq_action_cleanup(&cAction)
            return nil
        }

        moq_action_cleanup(&cAction)
        return result
    }
}
