import Testing
import CMoQService
@testable import MoQService

@Suite("Receiver fill bridge")
struct ReceiverFillBridgeTests {
    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("The production subscribe mapper retains defaults and sizes its tail")
    func defaultsAndSize() throws {
        try withReceiverTrackSubscribeCFG(start: .current, priority: nil, options: .init()) { cfg in
            #expect(cfg.start == MOQ_MEDIA_START_CURRENT)
            #expect(!cfg.has_priority)
            #expect(cfg.location == nil && cfg.fill == nil)
            #expect(cfg.struct_size == MemoryLayout<moq_media_receiver_track_subscribe_cfg_t>.size)
        }
        try withReceiverTrackSubscribeCFG(start: .nextGroup, priority: 0, options: .init()) { cfg in
            #expect(cfg.start == MOQ_MEDIA_START_NEXT_GROUP)
            #expect(cfg.has_priority && cfg.priority == 0)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Explicit NONE and empty fill are not omission")
    func selectedOptions() throws {
        try withReceiverTrackSubscribeCFG(
            start: .current, priority: nil,
            options: .init(location: .some(.none), fill: .init())) { cfg in
                #expect(cfg.location != nil)
                #expect(cfg.fill != nil)
                if let location = cfg.location, let fill = cfg.fill {
                    #expect(location.pointee.kind == MOQ_LOCATION_FILTER_NONE)
                    #expect(location.pointee.struct_size == MemoryLayout<moq_location_filter_cfg_t>.size)
                    #expect(fill.pointee.struct_size == MemoryLayout<moq_fill_cfg_t>.size)
                    #expect(!fill.pointee.has_subscriber_priority)
                    #expect(!fill.pointee.has_group_order && !fill.pointee.has_fill_timeout)
                    #expect(fill.pointee.location == nil)
                }
            }
        try withReceiverTrackSubscribeCFG(
            start: .current, priority: nil, options: .init(location: .some(.none))) { cfg in
                #expect(cfg.location?.pointee.kind == MOQ_LOCATION_FILTER_NONE)
                #expect(cfg.fill == nil)
            }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Production track copying retains a full fill terminal")
    func terminalCopy() {
        var event = moq_media_track_event_t()
        event.struct_size = UInt32(MemoryLayout<moq_media_track_event_t>.size)
        event.kind = MOQ_MEDIA_TRACK_FILL_TERMINAL
        event.initiating_request_id = .max
        event.fill_reason = MOQ_FILL_PEER_CANCELLED_OR_RESET
        event.fill_error_domain = MOQ_FILL_ERROR_STREAM
        event.fill_error_code = 0
        let copy = copyReceiverTrackEvent(event)
        #expect(copy != nil)
        if case .fillTerminal(let terminal)? = copy?.kind {
            #expect(terminal.initiatingRequestID == .max)
            #expect(terminal.reason == .peerCancelledOrReset)
            #expect(terminal.errorDomain == .stream)
            #expect(terminal.errorCode == 0)
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Every location retains full-width values in independent nested records")
    func locations() throws {
        let rows: [(MediaLocationFilter, UInt32, [UInt64])] = [
            (.none, MOQ_LOCATION_FILTER_NONE, [0, 0, 0, 0, 0]),
            (.nextObject, MOQ_LOCATION_FILTER_NEXT_OBJECT, [0, 0, 0, 0, 0]),
            (.relativeGroup(offset: .max), MOQ_LOCATION_FILTER_RELATIVE_GROUP, [.max, 0, 0, 0, 0]),
            (.absoluteStart(group: .max, object: .max), MOQ_LOCATION_FILTER_ABSOLUTE_START,
             [0, .max, .max, 0, 0]),
            (.groupEnd(startGroup: 0, startObject: .max, endGroup: .max), MOQ_LOCATION_FILTER_GROUP_END,
             [0, 0, .max, .max, 0]),
            (.objectEnd(startGroup: 0, startObject: 0, endGroup: .max, endObject: .max),
             MOQ_LOCATION_FILTER_OBJECT_END, [0, 0, 0, .max, .max]),
        ]
        for index in rows.indices {
            let parent = rows[index]
            let child = rows[(index + 1) % rows.count]
            try withReceiverTrackSubscribeCFG(
                start: .nextGroup, priority: .max,
                options: .init(location: parent.0, fill: .init(location: child.0))) { cfg in
                    #expect(cfg.has_priority && cfg.priority == .max)
                    #expect(cfg.location != nil && cfg.fill?.pointee.location != nil)
                    guard let p = cfg.location, let c = cfg.fill?.pointee.location else { return }
                    #expect(p != c)
                    for (pointer, row) in [(p, parent), (c, child)] {
                        let value = pointer.pointee
                        #expect(value.struct_size == MemoryLayout<moq_location_filter_cfg_t>.size)
                        #expect(value.kind == row.1)
                        #expect([value.relative_group_offset, value.start_group, value.start_object,
                                 value.end_group, value.end_object] == row.2)
                    }
                }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Fill overrides have independent presence flags, including explicit zero")
    func independentFlags() throws {
        let priorities: [UInt8?] = [nil, 0, .max]
        let orders: [MediaFillOptions.GroupOrder?] = [nil, .ascending, .descending]
        let timeouts: [UInt64?] = [nil, 0, .max]
        for priority in priorities {
            for order in orders {
                for timeout in timeouts {
                    try withReceiverTrackSubscribeCFG(
                        start: .current, priority: nil,
                        options: .init(fill: .init(subscriberPriority: priority, groupOrder: order,
                                                 timeoutMilliseconds: timeout))) { outer in
                            #expect(outer.location == nil && outer.fill != nil)
                            guard let cfg = outer.fill?.pointee else { return }
                            #expect(cfg.struct_size == MemoryLayout<moq_fill_cfg_t>.size)
                            #expect(cfg.has_subscriber_priority == (priority != nil))
                            #expect(cfg.subscriber_priority == priority ?? 0)
                            #expect(cfg.has_group_order == (order != nil))
                            let expectedOrder: UInt32 = order.map {
                                $0 == .ascending ? MOQ_GROUP_ORDER_ASCENDING : MOQ_GROUP_ORDER_DESCENDING
                            } ?? 0
                            #expect(cfg.group_order == expectedOrder)
                            #expect(cfg.has_fill_timeout == (timeout != nil))
                            #expect(cfg.fill_timeout_ms == timeout ?? 0)
                            #expect(cfg.location == nil)
                        }
                }
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Four terminal facts are independently gated by whole-field extents")
    func terminalExtents() throws {
        func end<T>(_ field: KeyPath<moq_media_track_event_t, T>) -> UInt32 {
            UInt32(MemoryLayout<moq_media_track_event_t>.offset(of: field)! + MemoryLayout<T>.size)
        }
        let idEnd = end(\moq_media_track_event_t.initiating_request_id)
        let reasonEnd = end(\moq_media_track_event_t.fill_reason)
        let domainEnd = end(\moq_media_track_event_t.fill_error_domain)
        let codeEnd = end(\moq_media_track_event_t.fill_error_code)
        let oldSize = UInt32(MemoryLayout<moq_media_track_event_t>.offset(of: \.initiating_request_id)!)
        let stamps = [oldSize, idEnd - 1, idEnd, reasonEnd - 1, reasonEnd,
                      domainEnd - 1, domainEnd, codeEnd - 1, codeEnd,
                      UInt32(MemoryLayout<moq_media_track_event_t>.size), .max]
        for stamp in stamps {
            var event = moq_media_track_event_t()
            event.struct_size = stamp
            event.kind = MOQ_MEDIA_TRACK_FILL_TERMINAL
            event.track = OpaquePointer(bitPattern: 0xABCD)
            event.initiating_request_id = .max
            event.fill_reason = .max
            event.fill_error_domain = .max
            event.fill_error_code = .max
            let copy = try #require(copyReceiverTrackEvent(event))
            #expect(copy.handleID == 0xABCD && copy.trackDescription == nil)
            guard case .fillTerminal(let terminal) = copy.kind else {
                Issue.record("known kind9 was not a terminal"); return
            }
            #expect(terminal.initiatingRequestID == (stamp >= idEnd ? .max : nil))
            #expect(terminal.reason == (stamp >= reasonEnd ? .unknown(.max) : nil))
            #expect(terminal.errorDomain == (stamp >= domainEnd ? .unknown(.max) : nil))
            #expect(terminal.errorCode == (stamp >= codeEnd ? .max : nil))
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Reasons and error domains are independent, preserving future values and zero")
    func terminalValues() throws {
        let reasons: [(UInt32, MediaFillReason)] = [
            (MOQ_FILL_COMPLETE, .complete), (MOQ_FILL_NO_STREAM, .noStream),
            (MOQ_FILL_LOCAL_CANCELLED, .localCancelled),
            (MOQ_FILL_PEER_CANCELLED_OR_RESET, .peerCancelledOrReset),
            (MOQ_FILL_LOCAL_ABORTED, .localAborted),
            (MOQ_FILL_PARENT_REJECTED, .parentRejected),
            (MOQ_FILL_PARENT_TERMINATED, .parentTerminated), (0, .unknown(0)), (.max, .unknown(.max)),
        ]
        let domains: [(UInt32, MediaFillErrorDomain)] = [
            (MOQ_FILL_ERROR_NONE, .none), (MOQ_FILL_ERROR_STREAM, .stream),
            (MOQ_FILL_ERROR_PARENT_REQUEST, .parentRequest), (MOQ_FILL_ERROR_PARENT_DONE, .parentDone),
            (.max, .unknown(.max)),
        ]
        for (rawReason, reason) in reasons {
            for (rawDomain, domain) in domains {
                for value in [UInt64(0), .max] {
                    var event = moq_media_track_event_t()
                    event.struct_size = UInt32(MemoryLayout<moq_media_track_event_t>.size)
                    event.kind = MOQ_MEDIA_TRACK_FILL_TERMINAL
                    event.initiating_request_id = value
                    event.fill_reason = rawReason
                    event.fill_error_domain = rawDomain
                    event.fill_error_code = value
                    let copy = try #require(copyReceiverTrackEvent(event))
                    guard case .fillTerminal(let terminal) = copy.kind else {
                        Issue.record("terminal kind lost"); return
                    }
                    #expect(terminal == MediaFillTerminal(initiatingRequestID: value, reason: reason,
                                                         errorDomain: domain, errorCode: value))
                }
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Cancel forwards exact native arguments and preserves unmapped refusals")
    func cancelForwarding() {
        let receiver = OpaquePointer(bitPattern: 0x1000)!
        for id in [UInt64(0), .max] {
            let refusals: [Int32] = [MOQ_ERR_STALE_HANDLE, MOQ_ERR_ABI_MISMATCH, -12345, MOQ_ERR_LIMIT]
            for raw in refusals {
                var calls = 0
                let result = cancelReceiverFill(receiver: receiver, handleID: 0x2000,
                                                initiatingRequestID: id) { r, track, requestID in
                    calls += 1
                    #expect(r == receiver && track == OpaquePointer(bitPattern: 0x2000))
                    #expect(requestID == id)
                    return raw
                }
                #expect(calls == 1)
                guard case .nativeFailure(let code) = result else {
                    Issue.record("raw refusal lost: \(raw)"); continue
                }
                #expect(code == raw)
            }
        }
        let invalid = cancelReceiverFill(receiver: receiver, handleID: 0, initiatingRequestID: 0) { _, _, _ in
            Issue.record("null track must not call native"); return moq_result_t(MOQ_OK)
        }
        if case .invalidArgument = invalid {} else { Issue.record("missing invalid-argument mapping") }
        let named: [Int32] = [MOQ_OK, MOQ_ERR_INVAL, MOQ_ERR_WRONG_STATE, MOQ_ERR_CLOSED, MOQ_ERR_UNSUPPORTED]
        for raw in named {
            let result = receiverCommandResult(raw)
            switch (raw, result) {
            case (MOQ_OK, .ok), (MOQ_ERR_INVAL, .invalidArgument), (MOQ_ERR_WRONG_STATE, .wrongState),
                 (MOQ_ERR_CLOSED, .closed), (MOQ_ERR_UNSUPPORTED, .unsupported): break
            default: Issue.record("named result mapping changed: \(raw)")
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A running fill forwards one sized config; WOULD_BLOCK is named, other refusals stay raw")
    func fillForwarding() {
        let receiver = OpaquePointer(bitPattern: 0x1000)!
        let options = MediaFillOptions(subscriberPriority: 0, groupOrder: .descending,
                                       timeoutMilliseconds: .max, location: .relativeGroup(offset: .max))
        var calls = 0
        let ok = fillReceiverTrack(receiver: receiver, handleID: 0x2000, options: options) { r, track, cfg in
            calls += 1
            #expect(r == receiver && track == OpaquePointer(bitPattern: 0x2000))
            guard let fill = cfg?.pointee else { Issue.record("fill config missing"); return MOQ_ERR_INVAL }
            #expect(fill.struct_size == MemoryLayout<moq_fill_cfg_t>.size)
            #expect(fill.has_subscriber_priority && fill.subscriber_priority == 0)
            #expect(fill.has_group_order && fill.group_order == MOQ_GROUP_ORDER_DESCENDING)
            #expect(fill.has_fill_timeout && fill.fill_timeout_ms == .max)
            #expect(fill.location != nil)
            return moq_result_t(MOQ_OK)
        }
        if case .ok = ok {} else { Issue.record("fill success lost") }
        _ = fillReceiverTrack(receiver: receiver, handleID: 0x2000, options: .init()) { _, _, cfg in
            calls += 1
            guard let fill = cfg?.pointee else { Issue.record("empty fill is not omission"); return MOQ_OK }
            #expect(!fill.has_subscriber_priority && !fill.has_group_order && !fill.has_fill_timeout)
            #expect(fill.location == nil)
            return moq_result_t(MOQ_OK)
        }
        #expect(calls == 2)
        let invalid = fillReceiverTrack(receiver: receiver, handleID: 0, options: options) { _, _, _ in
            Issue.record("null track must not call native"); return moq_result_t(MOQ_OK)
        }
        if case .invalidArgument = invalid {} else { Issue.record("missing invalid-argument mapping") }
        let blocked = fillReceiverTrack(receiver: receiver, handleID: 0x2000, options: options) { _, _, _ in
            moq_result_t(MOQ_ERR_WOULD_BLOCK)
        }
        if case .wouldBlock = blocked {} else { Issue.record("WOULD_BLOCK was not named") }
        for raw in [MOQ_ERR_LIMIT, MOQ_ERR_ABI_MISMATCH, -12345] as [Int32] {
            let result = fillReceiverTrack(receiver: receiver, handleID: 0x2000, options: options) { _, _, _ in raw }
            guard case .nativeFailure(let code) = result else { Issue.record("raw refusal lost: \(raw)"); continue }
            #expect(code == raw)
        }
        let state = queryReceiverTrackState(receiver: receiver, handleID: 0x2000) { _, _, _ in
            moq_result_t(MOQ_ERR_WOULD_BLOCK)
        }
        if case .failure(.wouldBlock) = state {} else { Issue.record("track-state WOULD_BLOCK was not named") }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("A fill receipt carries its ID only when the whole field is stamped, including zero")
    func fillCreatedExtents() throws {
        let idOffset = MemoryLayout<moq_media_track_event_t>.offset(of: \.initiating_request_id)!
        let idEnd = UInt32(idOffset + MemoryLayout<UInt64>.size)
        let stamps = [UInt32(idOffset), idEnd - 1, idEnd,
                      UInt32(MemoryLayout<moq_media_track_event_t>.size), .max]
        for stamp in stamps {
            for value in [UInt64(0), .max] {
                var event = moq_media_track_event_t()
                event.struct_size = stamp
                event.kind = MOQ_MEDIA_TRACK_FILL_CREATED
                event.track = OpaquePointer(bitPattern: 0xABCD)
                event.initiating_request_id = value
                let copy = try #require(copyReceiverTrackEvent(event))
                #expect(copy.handleID == 0xABCD && copy.trackDescription == nil)
                guard case .fillCreated(let id) = copy.kind else {
                    Issue.record("known kind12 was not a receipt"); return
                }
                #expect(id == (stamp >= idEnd ? value : nil))
            }
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Local failure is extended; UPDATE_OK and unknown events remain unmodeled")
    func unrelatedKinds() {
        var event = moq_media_track_event_t()
        event.struct_size = UInt32(MemoryLayout<moq_media_track_event_t>.size)
        event.kind = MOQ_MEDIA_TRACK_LOCAL_FAILURE
        event.local_result = MOQ_ERR_NOMEM
        if case .localFailure(let result)? = copyReceiverTrackEvent(event)?.kind {
            #expect(result == MOQ_ERR_NOMEM)
        } else {
            Issue.record("local failure was lost")
        }
        event.kind = MOQ_MEDIA_TRACK_UPDATE_OK
        #expect(copyReceiverTrackEvent(event) == nil)
        event.kind = moq_media_track_event_kind_t(rawValue: 0x7FFF_FFFE)
        #expect(copyReceiverTrackEvent(event) == nil)
        event.kind = MOQ_MEDIA_CATALOG_READY
        if case .catalogReady? = copyReceiverTrackEvent(event)?.kind {} else {
            Issue.record("legacy catalog event changed")
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    @Test("Extracted production copier preserves all six legacy event kinds")
    func legacyKinds() {
        let arena = CBytesArena()
        var description = moq_media_track_desc_t()
        description.name = arena.bytes("video")
        description.info.media_type = MOQ_MEDIA_TYPE_VIDEO
        description.info.packaging = MOQ_MEDIA_PACKAGING_RAW
        withExtendedLifetime(arena) {
            withUnsafePointer(to: &description) { description in
                var event = moq_media_track_event_t()
                event.struct_size = UInt32(MemoryLayout<moq_media_track_event_t>.size)
                event.track = OpaquePointer(bitPattern: 0x2000)
                event.desc = description
                event.parse_drop_class = MOQ_MEDIA_PARSE_DROP_SAP
                event.parse_drops_total = .max
                event.parse_drops_delta = 1
                for kind in [MOQ_MEDIA_TRACK_ADDED, MOQ_MEDIA_TRACK_UPDATED, MOQ_MEDIA_TRACK_REMOVED,
                             MOQ_MEDIA_TRACK_ENDED, MOQ_MEDIA_CATALOG_READY, MOQ_MEDIA_TRACK_PARSE_DROP] {
                    event.kind = kind
                    guard let copy = copyReceiverTrackEvent(event) else {
                        Issue.record("legacy kind skipped"); continue
                    }
                    #expect(copy.handleID == (kind == MOQ_MEDIA_CATALOG_READY ? 0 : 0x2000))
                    switch (kind, copy.kind) {
                    case (MOQ_MEDIA_TRACK_ADDED, .added), (MOQ_MEDIA_TRACK_UPDATED, .updated):
                        #expect(copy.trackDescription?.name == "video")
                    case (MOQ_MEDIA_TRACK_REMOVED, .removed), (MOQ_MEDIA_TRACK_ENDED, .ended),
                         (MOQ_MEDIA_CATALOG_READY, .catalogReady): break
                    case (MOQ_MEDIA_TRACK_PARSE_DROP, .parseDrop):
                        #expect(copy.parseDropClass == .sap)
                        #expect(copy.parseDropTotal == .max && copy.parseDropDelta == 1)
                    default: Issue.record("legacy kind changed")
                    }
                }
            }
        }
    }
}
