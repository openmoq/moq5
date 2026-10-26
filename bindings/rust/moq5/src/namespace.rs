use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};
use crate::{ffi, sys};

/// A MoQ track namespace: an ordered list of non-empty byte parts.
///
/// It owns the bytes the `moq_namespace_t` it lends to libmoq points into;
/// the service copies them at attach.
pub struct Namespace {
    parts: Vec<Box<[u8]>>,
    spans: Vec<sys::moq_bytes_t>,
}

// SAFETY: `spans` only points into `parts`, whose heap buffers move with it
// and are never mutated.
unsafe impl Send for Namespace {}
unsafe impl Sync for Namespace {}

impl Namespace {
    /// A namespace from its parts. There must be at least one, and every
    /// part must be non-empty; otherwise [`Error::INVAL`], as libmoq would
    /// answer.
    pub fn try_new<Parts, Part>(parts: Parts) -> Result<Namespace>
    where
        Parts: IntoIterator<Item = Part>,
        Part: Into<Vec<u8>>,
    {
        let mut owned: Vec<Box<[u8]>> = Vec::new();
        for part in parts {
            owned.push(part.into().into_boxed_slice());
        }
        if owned.is_empty() || owned.iter().any(|part| part.is_empty()) {
            return Err(Error::INVAL);
        }
        let spans = owned.iter().map(|part| ffi::span(part)).collect();
        Ok(Namespace {
            parts: owned,
            spans,
        })
    }

    /// Split a slash-separated namespace, e.g. `"live/room1"` (also
    /// `str::parse`).
    pub fn parse(text: &str) -> Result<Namespace> {
        if text.is_empty() {
            return Err(Error::INVAL);
        }
        Namespace::try_new(text.split('/'))
    }

    /// The parts, in order.
    pub fn parts(&self) -> impl Iterator<Item = &[u8]> {
        self.parts.iter().map(|part| &part[..])
    }

    pub(crate) fn raw(&self) -> sys::moq_namespace_t {
        sys::moq_namespace_t {
            parts: self.spans.as_ptr(),
            count: self.spans.len(),
        }
    }
}

impl FromStr for Namespace {
    type Err = Error;

    fn from_str(text: &str) -> Result<Namespace> {
        Namespace::parse(text)
    }
}

impl Clone for Namespace {
    fn clone(&self) -> Namespace {
        Namespace::try_new(self.parts.iter().map(|part| part.to_vec()))
            .expect("the parts were validated when this namespace was built")
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
        let raw = namespace.raw();
        assert_eq!(raw.count, 2);
        // SAFETY: the span points into `namespace`.
        assert_eq!(unsafe { ffi::bytes(*raw.parts.add(1)) }, b"room1");
        assert_eq!(namespace.clone(), namespace);
        let parsed: Namespace = "a/b".parse().expect("FromStr");
        assert_eq!(parsed.to_string(), "a/b");
    }

    #[test]
    fn refused() {
        assert_eq!(Namespace::parse("").unwrap_err(), Error::INVAL);
        assert_eq!(Namespace::parse("a//b").unwrap_err(), Error::INVAL);
        assert_eq!(Namespace::parse("/a").unwrap_err(), Error::INVAL);
        assert_eq!(
            Namespace::try_new(Vec::<Vec<u8>>::new()).unwrap_err(),
            Error::INVAL
        );
        assert!(Namespace::try_new([b"a".to_vec(), b"b".to_vec()]).is_ok());
        assert!("".parse::<Namespace>().is_err());
    }
}
