/// The source-built stack's half of the duplicate-symbol canary.
///
/// One app binary must link EITHER the source-built stack (`MoQ` and
/// friends, which compile the C core from source) OR the installed-mode
/// `MoQService` product (which links the installed libmoq archives) — never
/// both, or two copies of the core symbols coexist (at best a link error,
/// at worst silent first-archive-wins binding across mismatched revisions).
///
/// Both stacks therefore define this SAME strong C symbol, each referenced
/// from an always-linked entry point (`Session.init` here;
/// `MoQEndpoint.connect` on the service side), so an accidental dual link
/// fails loudly with `duplicate symbol 'moq_swift_stack_guard'` instead of
/// silently misbinding. See scripts/check_swift_stack_guard.sh.
@_cdecl("moq_swift_stack_guard")
public func moqSwiftStackGuardSource() -> UInt32 { 1 }
