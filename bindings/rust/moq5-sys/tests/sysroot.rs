//! The cross-build sysroot rules of the build script (`build_sysroot.rs`):
//! every root operand of the clang arguments counts, and every named root
//! must resolve to one directory.

#[path = "../build_sysroot.rs"]
mod sysroot;

use std::path::{Path, PathBuf};

/// Two distinct directories for one test.
fn two_roots(test: &str) -> (PathBuf, PathBuf) {
    let base = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("sysroot-{test}"));
    let (a, b) = (base.join("a"), base.join("b"));
    std::fs::create_dir_all(&a).expect("root a is created");
    std::fs::create_dir_all(&b).expect("root b is created");
    (a, b)
}

fn named(entries: &[(&str, &Path)]) -> Vec<(String, PathBuf)> {
    entries
        .iter()
        .map(|(var, dir)| ((*var).to_owned(), dir.to_path_buf()))
        .collect()
}

#[test]
fn every_root_operand_counts_in_order() {
    let roots = |args| sysroot::sysroots_in_clang_args(args).expect("the arguments split");
    assert_eq!(
        roots("-isysroot /b --sysroot=/a"),
        [PathBuf::from("/b"), PathBuf::from("/a")]
    );
    assert_eq!(
        roots("--sysroot=/a -isysroot /b"),
        [PathBuf::from("/a"), PathBuf::from("/b")]
    );
    assert_eq!(
        roots("--sysroot /a -I/inc -isysroot/b"),
        [PathBuf::from("/a"), PathBuf::from("/b")]
    );
    assert_eq!(roots("--sysroot=/a"), [PathBuf::from("/a")]);
}

#[test]
fn text_inside_another_option_is_no_root() {
    let roots = sysroot::sysroots_in_clang_args("-DWHY=--sysroot -I/usr/include/-isysroot")
        .expect("the arguments split");
    assert!(roots.is_empty(), "found {roots:?}");
}

#[test]
fn a_root_option_without_its_directory_is_refused() {
    for args in ["--sysroot", "-I/inc -isysroot"] {
        let err = sysroot::sysroots_in_clang_args(args).expect_err("an operand is missing");
        assert!(err.contains("names no directory"), "{args}: {err}");
    }
}

#[test]
fn mixed_options_naming_different_roots_disagree_in_either_order() {
    let (a, b) = two_roots("mixed");
    // MOQ_SYSROOT names A; the clang arguments name B through -isysroot
    // and A through --sysroot, in both orders.
    for order in [[&b, &a], [&a, &b]] {
        let entries = named(&[
            ("MOQ_SYSROOT_t", a.as_path()),
            (
                "BINDGEN_EXTRA_CLANG_ARGS_t (root operand 1)",
                order[0].as_path(),
            ),
            (
                "BINDGEN_EXTRA_CLANG_ARGS_t (root operand 2)",
                order[1].as_path(),
            ),
        ]);
        let err = sysroot::agreeing_root(&entries).expect_err("the roots disagree");
        assert!(err.contains("the sysroots named disagree"), "{err}");
    }
}

#[test]
fn the_same_root_through_every_option_agrees() {
    let (a, _) = two_roots("same");
    let spelled_differently = a.join(".");
    let entries = named(&[
        ("MOQ_SYSROOT_t", a.as_path()),
        (
            "BINDGEN_EXTRA_CLANG_ARGS_t (root operand 1)",
            spelled_differently.as_path(),
        ),
        ("BINDGEN_EXTRA_CLANG_ARGS_t (root operand 2)", a.as_path()),
    ]);
    let root = sysroot::agreeing_root(&entries).expect("one root");
    assert_eq!(root, a.canonicalize().expect("root a resolves"));
}

#[test]
fn a_single_root_is_the_root() {
    let (a, _) = two_roots("single");
    let root = sysroot::agreeing_root(&named(&[("MOQ_SYSROOT_t", a.as_path())])).expect("one root");
    assert_eq!(root, a.canonicalize().expect("root a resolves"));
}

#[test]
fn a_root_that_does_not_exist_is_refused() {
    let (a, _) = two_roots("missing");
    let missing = a.join("not-there");
    let err = sysroot::agreeing_root(&named(&[("MOQ_SYSROOT_t", missing.as_path())]))
        .expect_err("the root does not exist");
    assert!(err.contains("does not exist"), "{err}");
}

#[test]
fn a_root_that_is_a_regular_file_is_refused() {
    let (a, _) = two_roots("file");
    let file = a.join("a-file");
    std::fs::write(&file, b"not a sysroot").expect("the file is written");
    let err = sysroot::agreeing_root(&named(&[("MOQ_SYSROOT_t", file.as_path())]))
        .expect_err("a regular file is no root");
    assert!(err.contains("is not a directory"), "{err}");
}
