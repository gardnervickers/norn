//! In-memory storage for the networking-first server.

use std::collections::HashMap;

use foldhash::fast::RandomState;
use thiserror::Error;

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

/// An allocation for the store failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("out of memory")]
pub struct OutOfMemory;

/// Copy `bytes` into a vector sized exactly, reporting allocation failure.
fn copy_exact(bytes: &[u8]) -> Result<Vec<u8>, OutOfMemory> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(bytes.len())
        .map_err(|_| OutOfMemory)?;
    copy.extend_from_slice(bytes);
    Ok(copy)
}

/// A single-threaded in-memory key/value store.
#[derive(Debug, Default)]
pub struct MemoryStore {
    // foldhash rather than std's SipHash: hashing was about 18% of the
    // server's user-space instructions per request. foldhash is randomly
    // seeded per process but is not a keyed cryptographic hash, so it is
    // weaker against deliberate collision attacks.
    entries: HashMap<Vec<u8>, Entry, RandomState>,
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
    ///
    /// Every allocation is fallible and happens before the store changes, so
    /// on error the store is exactly as it was: a value too large for the
    /// process's memory limit fails only this request.
    ///
    /// # Errors
    ///
    /// Returns [`OutOfMemory`] if the key, value or table slot cannot be
    /// allocated.
    pub fn set(&mut self, key: &[u8], flags: u32, value: &[u8]) -> Result<(), OutOfMemory> {
        if let Some(current) = self.entries.get_mut(key) {
            // Reuse the existing allocation when it is big enough; otherwise
            // build the replacement before dropping the old value.
            let previous_len = current.value.len();
            if current.value.capacity() >= value.len() {
                current.value.clear();
                current.value.extend_from_slice(value);
            } else {
                current.value = copy_exact(value)?;
            }
            current.flags = flags;
            self.value_bytes = self.value_bytes - previous_len + value.len();
            return Ok(());
        }
        let key = copy_exact(key)?;
        let value_copy = copy_exact(value)?;
        self.entries.try_reserve(1).map_err(|_| OutOfMemory)?;
        self.entries.insert(
            key,
            Entry {
                flags,
                value: value_copy,
            },
        );
        self.value_bytes += value.len();
        Ok(())
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
        store.set(b"key", 7, b"first").unwrap();
        let first = store.get(b"key").unwrap();
        assert_eq!(first.flags(), 7);
        assert_eq!(first.value(), b"first");
        assert_eq!(store.stats().items, 1);
        assert_eq!(store.stats().value_bytes, 5);

        store.set(b"key", 9, b"replacement").unwrap();
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
