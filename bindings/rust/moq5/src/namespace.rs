use std::fmt;

use crate::sys;
use crate::types::span;

/// A MoQ track namespace: an ordered list of non-empty byte parts.
///
/// It owns the bytes libmoq's `moq_namespace_t` points into, so a sender or
/// receiver keeps it alive for as long as it may be read.
pub struct Namespace {
    parts: Vec<Box<[u8]>>,
    spans: Vec<sys::moq_bytes_t>,
}

// SAFETY: `spans` only points into `parts`, whose heap buffers move with it
// and are never mutated.
unsafe impl Send for Namespace {}
unsafe impl Sync for Namespace {}

/// Why a namespace was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NamespaceError {
    /// No parts at all.
    Empty,
    /// One of the parts is empty; carries the namespace as given.
    EmptyPart(String),
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NamespaceError::Empty => f.write_str("namespace is empty"),
            NamespaceError::EmptyPart(s) => write!(f, "namespace has an empty part: '{s}'"),
        }
    }
}

impl std::error::Error for NamespaceError {}

impl Namespace {
    /// A namespace from its parts. Every part must be non-empty, and there
    /// must be at least one.
    pub fn new<I, P>(parts: I) -> Result<Namespace, NamespaceError>
    where
        I: IntoIterator<Item = P>,
        P: Into<Vec<u8>>,
    {
        let parts: Vec<Box<[u8]>> = parts.into_iter().map(|p| p.into().into()).collect();
        if parts.is_empty() {
            return Err(NamespaceError::Empty);
        }
        if parts.iter().any(|p| p.is_empty()) {
            let joined: Vec<String> = parts
                .iter()
                .map(|p| String::from_utf8_lossy(p).into_owned())
                .collect();
            return Err(NamespaceError::EmptyPart(joined.join("/")));
        }
        let spans = parts.iter().map(|p| span(p)).collect();
        Ok(Namespace { parts, spans })
    }

    /// Split a slash-separated namespace, e.g. `"live/room1"`.
    pub fn parse(s: &str) -> Result<Namespace, NamespaceError> {
        if s.is_empty() {
            return Err(NamespaceError::Empty);
        }
        Namespace::new(s.split('/')).map_err(|e| match e {
            NamespaceError::EmptyPart(_) => NamespaceError::EmptyPart(s.into()),
            e => e,
        })
    }

    /// The parts, in order.
    pub fn parts(&self) -> impl Iterator<Item = &[u8]> {
        self.parts.iter().map(|p| &p[..])
    }

    pub(crate) fn raw(&self) -> sys::moq_namespace_t {
        sys::moq_namespace_t {
            parts: self.spans.as_ptr(),
            count: self.spans.len(),
        }
    }
}

impl Clone for Namespace {
    fn clone(&self) -> Namespace {
        Namespace::new(self.parts.iter().map(|p| p.to_vec())).expect("already validated")
    }
}

impl PartialEq for Namespace {
    fn eq(&self, other: &Namespace) -> bool {
        self.parts == other.parts
    }
}

impl Eq for Namespace {}

impl fmt::Debug for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Namespace({self})")
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, p) in self.parts.iter().enumerate() {
            if i > 0 {
                f.write_str("/")?;
            }
            f.write_str(&String::from_utf8_lossy(p))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let ns = Namespace::parse("live/room1").unwrap();
        assert_eq!(ns.parts().collect::<Vec<_>>(), [&b"live"[..], b"room1"]);
        assert_eq!(ns.to_string(), "live/room1");
        let raw = ns.raw();
        assert_eq!(raw.count, 2);
        assert_eq!(unsafe { crate::types::bytes(*raw.parts.add(1)) }, b"room1");
        assert_eq!(ns.clone(), ns);
    }

    #[test]
    fn refused() {
        assert_eq!(Namespace::parse(""), Err(NamespaceError::Empty));
        let e = Namespace::parse("a//b").unwrap_err();
        assert_eq!(e.to_string(), "namespace has an empty part: 'a//b'");
        assert_eq!(
            Namespace::parse("/a").unwrap_err(),
            NamespaceError::EmptyPart("/a".into())
        );
        assert_eq!(
            Namespace::new(Vec::<Vec<u8>>::new()),
            Err(NamespaceError::Empty)
        );
    }
}
