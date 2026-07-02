/// The pure-Swift MSF catalog types (`MSFCatalog`, `MSFTrack`, delta/content-
/// protection structures, …) live in the C-free `MoQCatalog` target so they can
/// be imported beside the installed-mode `MoQService` product without pulling
/// the from-source C stack in. `MoQMedia` re-exports them for source
/// compatibility: existing `import MoQMedia` users keep seeing every MSF type.
@_exported import MoQCatalog
