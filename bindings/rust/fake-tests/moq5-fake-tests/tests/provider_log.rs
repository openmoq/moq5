//! The provider's log keeps every line whole, whatever its length: a span the
//! caller passes (a URL here) is recorded to its last byte, never cut at an
//! internal buffer and never read past one. The `rust_fake_asan` row runs this
//! binary with the provider under AddressSanitizer.

use moq5::{Endpoint, EndpointConfig};
use moq5_fake_tests as fake;

#[test]
fn a_long_span_is_logged_whole() {
    let s = fake::suite();
    // Longer than any fixed line buffer a logger would reasonably keep.
    let url = format!("moqt://relay:4433/{}", "p".repeat(6000));
    let endpoint = Endpoint::connect(&EndpointConfig::new(url.as_str()))
        .expect("the provider accepts any URL");
    let connect = fake::line(&s.take_log(), "connect ");
    assert_eq!(fake::field(&connect, "url"), url);
    assert!(
        connect.ends_with(&format!("struct_size={}", sys_struct_size())),
        "the line is complete to its last field: {connect}"
    );
    drop(endpoint);
}

fn sys_struct_size() -> usize {
    std::mem::size_of::<moq5_fake_tests::sys::moq_endpoint_cfg_t>()
}
