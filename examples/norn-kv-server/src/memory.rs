//! In-memory storage for the networking-first server.

use std::collections::HashMap;

/// A value stored by [`MemoryStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    flags: u32,
    value: Vec<u8>,
}

impl Entry {
    /// Return the application-defined Memcached flags.
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// Return the stored value.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// Statistics for an in-memory store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Number of live keys.
    pub items: usize,
    /// Sum of live value lengths.
    pub value_bytes: usize,
}

/// A single-threaded in-memory key/value store.
#[derive(Debug, Default)]
pub struct MemoryStore {
    entries: HashMap<Vec<u8>, Entry>,
    value_bytes: usize,
}

impl MemoryStore {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a stored entry.
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.entries.get(key)
    }

    /// Insert or replace a value.
    pub fn set(&mut self, key: &[u8], flags: u32, value: &[u8]) {
        if let Some(current) = self.entries.get_mut(key) {
            self.value_bytes -= current.value.len();
            current.flags = flags;
            current.value.clear();
            current.value.extend_from_slice(value);
        } else {
            self.entries.insert(
                key.to_vec(),
                Entry {
                    flags,
                    value: value.to_vec(),
                },
            );
        }
        self.value_bytes += value.len();
    }

    /// Delete a key, returning whether it was present.
    pub fn delete(&mut self, key: &[u8]) -> bool {
        if let Some(previous) = self.entries.remove(key) {
            self.value_bytes -= previous.value.len();
            true
        } else {
            false
        }
    }

    /// Return current store statistics.
    pub fn stats(&self) -> Stats {
        Stats {
            items: self.entries.len(),
            value_bytes: self.value_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryStore;

    #[test]
    fn set_get_replace_delete() {
        let mut store = MemoryStore::new();
        store.set(b"key", 7, b"first");
        let first = store.get(b"key").unwrap();
        assert_eq!(first.flags(), 7);
        assert_eq!(first.value(), b"first");
        assert_eq!(store.stats().items, 1);
        assert_eq!(store.stats().value_bytes, 5);

        store.set(b"key", 9, b"replacement");
        let replacement = store.get(b"key").unwrap();
        assert_eq!(replacement.flags(), 9);
        assert_eq!(replacement.value(), b"replacement");
        assert_eq!(store.stats().items, 1);
        assert_eq!(store.stats().value_bytes, 11);

        assert!(store.delete(b"key"));
        assert!(!store.delete(b"key"));
        assert_eq!(store.stats().items, 0);
        assert_eq!(store.stats().value_bytes, 0);
    }
}
