use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};
use crate::{ffi, sys};

/// A MoQ track namespace: an ordered list of byte parts, each non-empty.
///
/// The root namespace has zero parts ([`Namespace::root`]). Drafts 18 and 22
/// use it where a namespace may be empty (a root full track name, an empty
/// prefix); draft 16 requires at least one part. That limit belongs to the
/// operation and its negotiated draft, which refuses a root it cannot carry;
/// constructing one is never an error.
///
/// It owns the bytes of the `moq_namespace_t` it lends to libmoq
/// ([`as_raw`](Namespace::as_raw)); libmoq copies what it keeps.
pub struct Namespace {
    parts: Vec<Box<[u8]>>,
    spans: Vec<sys::moq_bytes_t>,
}

// SAFETY: `spans` only points into `parts`, whose heap buffers move with it
// and are never mutated; nothing in it is shared with libmoq beyond a call.
unsafe impl Send for Namespace {}
// SAFETY: as for Send: every method reads only.
unsafe impl Sync for Namespace {}

impl Namespace {
    /// The root namespace: zero parts.
    #[must_use]
    pub fn root() -> Namespace {
        Namespace::from_parts(Vec::new())
    }

    /// A namespace from its parts. No parts is the root, the same value as
    /// [`Namespace::root`].
    ///
    /// # Errors
    /// [`Error::INVAL`] when a part is empty: the root has zero parts, never
    /// one empty part.
    pub fn try_new<Parts, Part>(parts: Parts) -> Result<Namespace>
    where
        Parts: IntoIterator<Item = Part>,
        Part: Into<Vec<u8>>,
    {
        let mut owned: Vec<Box<[u8]>> = Vec::new();
        for part in parts {
            let part = part.into();
            if part.is_empty() {
                return Err(Error::INVAL);
            }
            owned.push(part.into_boxed_slice());
        }
        Ok(Namespace::from_parts(owned))
    }

    /// Split a slash-separated namespace, e.g. `"live/room1"` (also
    /// `str::parse`). The text form has no spelling for the root; use
    /// [`Namespace::root`].
    ///
    /// # Errors
    /// [`Error::INVAL`] for an empty text or an empty part (`"a//b"`,
    /// `"/a"`, `"a/"`).
    pub fn parse(text: &str) -> Result<Namespace> {
        if text.is_empty() {
            return Err(Error::INVAL);
        }
        Namespace::try_new(text.split('/'))
    }

    /// Copy a native namespace: zero parts is the root.
    ///
    /// # Errors
    /// [`Error::INVAL`] when a part is empty, or when `raw` names parts
    /// through a null array.
    ///
    /// # Safety
    /// When `raw.count` is not zero, `raw.parts` must point to `raw.count`
    /// spans, and each span with a non-zero length must point to that many
    /// readable bytes, all valid for the call. With `raw.count` zero the
    /// pointer is not read.
    pub unsafe fn from_raw(raw: sys::moq_namespace_t) -> Result<Namespace> {
        if raw.count == 0 {
            return Ok(Namespace::root());
        }
        if raw.parts.is_null() {
            return Err(Error::INVAL);
        }
        // SAFETY: the caller's contract: `count` spans at `parts`.
        let spans = unsafe { ffi::array(raw.parts, raw.count) };
        let mut parts = Vec::with_capacity(spans.len());
        for span in spans {
            // SAFETY: the caller's contract: each span's bytes are readable.
            parts.push(unsafe { ffi::bytes(*span) }.to_vec());
        }
        Namespace::try_new(parts)
    }

    /// The parts, in order; none for the root.
    pub fn parts(&self) -> impl Iterator<Item = &[u8]> {
        self.parts.iter().map(|part| &part[..])
    }

    /// The native view of this namespace: its spans point into `self`, so
    /// the record is valid while `self` is alive (the namespace never
    /// changes). The root is the null array with a count of zero. Producing
    /// it is safe; passing it to libmoq is the caller's `unsafe` call.
    #[must_use]
    pub fn as_raw(&self) -> sys::moq_namespace_t {
        sys::moq_namespace_t {
            parts: if self.spans.is_empty() {
                std::ptr::null()
            } else {
                self.spans.as_ptr()
            },
            count: self.spans.len(),
        }
    }

    /// A namespace of parts already checked to be non-empty.
    fn from_parts(parts: Vec<Box<[u8]>>) -> Namespace {
        let spans = parts.iter().map(|part| ffi::span(part)).collect();
        Namespace { parts, spans }
    }
}

impl FromStr for Namespace {
    type Err = Error;

    fn from_str(text: &str) -> Result<Namespace> {
        Namespace::parse(text)
    }
}

// The parts are copied as they are, the root's none included; the spans
// are rebuilt to point into the copy.
impl Clone for Namespace {
    fn clone(&self) -> Namespace {
        Namespace::from_parts(self.parts.clone())
    }
}

impl PartialEq for Namespace {
    fn eq(&self, other: &Namespace) -> bool {
        self.parts == other.parts
    }
}

impl Eq for Namespace {}

// The parts as text, not the spans a derive would print.
impl fmt::Debug for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Namespace({self})")
    }
}

// The parts joined by `/`; the root is the empty text.
impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, part) in self.parts.iter().enumerate() {
            if index > 0 {
                f.write_str("/")?;
            }
            f.write_str(&String::from_utf8_lossy(part))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let namespace = Namespace::parse("live/room1").expect("two parts");
        assert_eq!(
            namespace.parts().collect::<Vec<_>>(),
            [&b"live"[..], b"room1"]
        );
        assert_eq!(namespace.to_string(), "live/room1");
        assert_eq!(format!("{namespace:?}"), "Namespace(live/room1)");
        let raw = namespace.as_raw();
        assert_eq!(raw.count, 2);
        // SAFETY: the span points into `namespace`.
        assert_eq!(unsafe { ffi::bytes(*raw.parts.add(1)) }, b"room1");
        assert_eq!(namespace.clone(), namespace);
        let parsed: Namespace = "a/b".parse().expect("FromStr");
        assert_eq!(parsed.to_string(), "a/b");
    }

    #[test]
    fn refused() {
        assert_eq!(Namespace::parse("").expect_err("no text"), Error::INVAL);
        assert_eq!(
            Namespace::parse("a//b").expect_err("an empty part"),
            Error::INVAL
        );
        assert_eq!(
            Namespace::parse("/a").expect_err("an empty first part"),
            Error::INVAL
        );
        assert_eq!(
            Namespace::parse("a/").expect_err("an empty last part"),
            Error::INVAL
        );
        assert_eq!(
            Namespace::try_new([b"a".to_vec(), Vec::new()]).expect_err("an empty part"),
            Error::INVAL
        );
        assert_eq!(
            Namespace::try_new([Vec::<u8>::new()]).expect_err("one empty part is not the root"),
            Error::INVAL
        );
        assert!(Namespace::try_new([b"a".to_vec(), b"b".to_vec()]).is_ok());
        assert!("".parse::<Namespace>().is_err());
    }

    #[test]
    fn the_root_has_zero_parts() {
        let root = Namespace::root();
        assert_eq!(root.parts().count(), 0);
        assert_eq!(
            Namespace::try_new(Vec::<Vec<u8>>::new()).expect("no parts is the root"),
            root
        );
        assert_eq!(root.to_string(), "");
        assert_eq!(format!("{root:?}"), "Namespace()");
        assert_ne!(root, Namespace::parse("a").expect("one part"));
    }

    #[test]
    fn the_root_clones() {
        let root = Namespace::root();
        let copy = root.clone();
        assert_eq!(copy, root);
        assert_eq!(copy.parts().count(), 0);
        assert_eq!(copy.as_raw().count, 0);
    }

    #[test]
    fn a_clone_points_into_its_own_parts() {
        let namespace = Namespace::parse("a/bc").expect("two parts");
        let copy = namespace.clone();
        let (raw, raw_copy) = (namespace.as_raw(), copy.as_raw());
        assert_ne!(raw.parts, raw_copy.parts, "the copy has its own spans");
        // SAFETY: both records borrow live namespaces.
        let (first, first_copy) = unsafe { (*raw.parts, *raw_copy.parts) };
        assert_ne!(first.data, first_copy.data, "and its own bytes");
        drop(namespace);
        // SAFETY: the copy is alive; its spans point into it.
        assert_eq!(unsafe { ffi::bytes(*raw_copy.parts.add(1)) }, b"bc");
    }

    #[test]
    fn the_root_converts_to_and_from_native() {
        let raw = Namespace::root().as_raw();
        assert_eq!(raw.count, 0);
        assert!(raw.parts.is_null());
        // SAFETY: zero parts; the pointer is not read.
        let back = unsafe { Namespace::from_raw(raw) }.expect("zero parts is the root");
        assert_eq!(back, Namespace::root());
        // A count of zero is the root whatever the pointer holds.
        let dangling = sys::moq_namespace_t {
            parts: std::ptr::NonNull::<sys::moq_bytes_t>::dangling()
                .as_ptr()
                .cast_const(),
            count: 0,
        };
        // SAFETY: zero parts; the pointer is not read.
        let back = unsafe { Namespace::from_raw(dangling) }.expect("zero parts");
        assert_eq!(back, Namespace::root());
    }

    #[test]
    fn a_full_name_converts_to_and_from_native() {
        let namespace = Namespace::parse("live/room1").expect("two parts");
        // SAFETY: the record borrows `namespace`, alive for the call.
        let back = unsafe { Namespace::from_raw(namespace.as_raw()) }.expect("a copy");
        assert_eq!(back, namespace);
        drop(namespace);
        assert_eq!(back.to_string(), "live/room1");
    }

    #[test]
    fn a_native_namespace_with_an_empty_part_is_refused() {
        let parts = [ffi::span(b"a"), sys::moq_bytes_t::default()];
        let raw = sys::moq_namespace_t {
            parts: parts.as_ptr(),
            count: parts.len(),
        };
        // SAFETY: two spans, the second the null span.
        let refused = unsafe { Namespace::from_raw(raw) }.expect_err("an empty part");
        assert_eq!(refused, Error::INVAL);
        let null = sys::moq_namespace_t {
            parts: std::ptr::null(),
            count: 1,
        };
        // SAFETY: a null array is refused before it is read.
        let refused = unsafe { Namespace::from_raw(null) }.expect_err("a null array");
        assert_eq!(refused, Error::INVAL);
    }
}
