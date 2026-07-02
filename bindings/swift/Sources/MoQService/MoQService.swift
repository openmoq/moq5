/// MoQService: the installed-mode Media-over-QUIC service SDK.
///
/// The public model (endpoint, receiver, sender, tracks, objects, streams)
/// lives in MoQServiceCore and is re-exported here; this product adds the
/// real backends over the installed C service tier (`libmoq-service`) and
/// the `connect`/`attach` factories.
///
/// BINARY EXCLUSIVITY: one app binary links EITHER this product OR the
/// source-built `MoQ`/`MoQMedia` stack, never both. Both stacks define the
/// same strong `moq_swift_stack_guard` symbol so an accidental dual link
/// fails with a duplicate-symbol error instead of silently binding two
/// mismatched copies of the C core.
@_exported import MoQServiceCore

/// The installed-mode stack's half of the duplicate-symbol canary; see
/// the source stack's `StackGuard.swift` in the `MoQ` target.
@_cdecl("moq_swift_stack_guard")
public func moqSwiftStackGuardService() -> UInt32 { 2 }
