//! Links an installed libmoq SDK through moq5 and exercises what needs no
//! peer: the version string, a configuration the service refuses, buffers
//! and the CMAF parser. Exit status is the result.

use std::process::ExitCode;

use moq5::cmaf::CmafInit;
use moq5::{Buffer, Endpoint, EndpointConfig, Error, Namespace};

fn main() -> ExitCode {
    let version = moq5::libmoq_version();
    println!("libmoq {version}");
    if version.is_empty() {
        return ExitCode::FAILURE;
    }

    // A URL with a scheme the endpoint reads in no mode is refused at
    // resolution, before any transport exists.
    match Endpoint::connect(&EndpointConfig::new("ftp://relay.example")) {
        Err(Error::INVAL) => {}
        other => {
            eprintln!("expected INVAL for an unknown scheme, got {other:?}");
            return ExitCode::FAILURE;
        }
    }

    let buffer = match Buffer::from_vec(vec![1, 2, 3]) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("buffer: {e}");
            return ExitCode::FAILURE;
        }
    };
    if buffer.as_slice() != [1, 2, 3] {
        return ExitCode::FAILURE;
    }
    if CmafInit::parse(vec![0u8; 16]).err() != Some(Error::PROTO) {
        eprintln!("expected PROTO for a non-CMAF init segment");
        return ExitCode::FAILURE;
    }
    if Namespace::parse("live/demo").is_err() {
        return ExitCode::FAILURE;
    }
    println!("moq5 consumer: OK");
    ExitCode::SUCCESS
}
