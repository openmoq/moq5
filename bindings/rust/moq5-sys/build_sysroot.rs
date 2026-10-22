//! The sysroot inputs of a cross build: every root a clang argument list
//! names, and the one directory all named roots must resolve to. Included by
//! the build script and by `tests/sysroot.rs`.

use std::path::PathBuf;

/// Every sysroot a clang argument list names, in order: `--sysroot=DIR`,
/// `--sysroot DIR`, `-isysroot DIR` and `-isysrootDIR`, each as an argument
/// of its own (a define that merely contains the text is not one). All of
/// them count, not the last: clang takes headers from `-isysroot` over
/// `--sysroot` wherever they appear. `Err` is a list that does not split, or
/// a sysroot option without its directory.
pub fn sysroots_in_clang_args(args: &str) -> Result<Vec<PathBuf>, String> {
    let words =
        shlex::split(args).ok_or_else(|| format!("the clang arguments {args:?} do not split"))?;
    let mut found = Vec::new();
    let mut words = words.into_iter();
    while let Some(word) = words.next() {
        let dir = if let Some(dir) = word.strip_prefix("--sysroot=") {
            dir.to_owned()
        } else if word == "--sysroot" || word == "-isysroot" {
            words
                .next()
                .ok_or_else(|| format!("{word} in the clang arguments names no directory"))?
        } else if let Some(dir) = word.strip_prefix("-isysroot") {
            dir.to_owned()
        } else {
            continue;
        };
        found.push(PathBuf::from(dir));
    }
    Ok(found)
}

/// The directory every named sysroot resolves to; each is `(where it was
/// named, the directory)`. `Err` names a root that does not exist or is not
/// a directory, or lists the roots when they disagree.
pub fn agreeing_root(named: &[(String, PathBuf)]) -> Result<PathBuf, String> {
    let mut resolved: Vec<(String, PathBuf)> = Vec::new();
    for (var, dir) in named {
        let canonical = dir
            .canonicalize()
            .map_err(|e| format!("the sysroot {} ({var}) does not exist: {e}", dir.display()))?;
        if !canonical.is_dir() {
            return Err(format!(
                "the sysroot {} ({var}) is not a directory",
                dir.display()
            ));
        }
        resolved.push((var.clone(), canonical));
    }
    let Some((_, first)) = resolved.first() else {
        return Err("no sysroot is named".to_owned());
    };
    if resolved.iter().any(|(_, dir)| dir != first) {
        let listed: Vec<String> = resolved
            .iter()
            .map(|(var, dir)| format!("{var}={}", dir.display()))
            .collect();
        return Err(format!(
            "the sysroots named disagree: {}",
            listed.join(", ")
        ));
    }
    Ok(first.clone())
}
