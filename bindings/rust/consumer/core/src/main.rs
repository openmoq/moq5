//! Links an installed libmoq SDK through moq5-core alone: a graph that asks
//! only for the core tier, so it must link the core library and nothing
//! above it (the runner proves that from the build's link lines and the
//! binary's symbols). It exercises what needs no peer: the version string,
//! the draft set, buffers and namespaces, and keeps the session's create
//! entry point linked. Exit status is the result.

use std::process::ExitCode;

use moq5_core::{Buffer, Error, Namespace, Version};

fn main() -> ExitCode {
    let version = moq5_core::libmoq_version();
    println!("libmoq {version}");
    if version.is_empty() {
        return ExitCode::FAILURE;
    }
    // SAFETY: a pure query of the compiled-in draft set.
    if !unsafe { moq5_core::sys::moq_version_supported(Version::DRAFT_22.as_raw()) } {
        eprintln!("draft 22 is not supported by this core");
        return ExitCode::FAILURE;
    }
    let buffer = match Buffer::copy_from(&[1, 2, 3]) {
        Ok(buffer) => buffer,
        Err(e) => {
            eprintln!("buffer: {e}");
            return ExitCode::FAILURE;
        }
    };
    if buffer.as_slice() != [1, 2, 3] {
        return ExitCode::FAILURE;
    }
    if Namespace::parse("").err() != Some(Error::INVAL) || Namespace::root().parts().count() != 0 {
        eprintln!("the namespace rules do not hold");
        return ExitCode::FAILURE;
    }
    // The session's entry point stays linked from the SDK's core library.
    std::hint::black_box(moq5_core::sys::moq_session_create as *const ());
    println!("moq5-core consumer: OK");
    ExitCode::SUCCESS
}
