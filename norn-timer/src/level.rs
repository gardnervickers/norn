use std::{fmt, ptr};

use crate::entry::Entry;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Expiration {
    level: usize,
    slot: usize,
    deadline: u64,
}

impl fmt::Debug for Expiration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Expiration")
            .field("level", &self.level())
            .field("slot", &self.slot())
            .field("deadline", &self.deadline())
            .finish()
    }
}

impl Expiration {
    fn new(level: usize, slot: usize, deadline: u64) -> Self {
        Self {
            level,
            slot,
            deadline,
        }
    }

    /// The level at which this expiration occurs.
    pub(crate) fn level(&self) -> usize {
        self.level
    }

    /// The slot within the `Expiration::level()` at which this expiration occurs.
    pub(crate) fn slot(&self) -> usize {
        self.slot
    }

    /// The deadline for this expiration.
    pub(crate) fn deadline(&self) -> u64 {
        self.deadline
    }
}

pub(crate) struct Level {
    level: usize,
    bitfield: u64,
    slots: [cordyceps::List<Entry>; Self::LEVEL_MULT],
    slot_deadlines: [Option<u64>; Self::LEVEL_MULT],
}

impl Level {
    pub(crate) const LEVEL_MULT: usize = 64;

    pub(crate) fn new(level: usize) -> Self {
        Self {
            level,
            bitfield: 0,
            slots: std::array::from_fn(|_| cordyceps::List::new()),
            slot_deadlines: std::array::from_fn(|_| None),
        }
    }

    #[cfg(test)]
    pub(crate) fn num_registered(&self) -> usize {
        self.slots.iter().map(|s| s.len()).sum()
    }

    pub(crate) fn next_expiration(&self, now: u64) -> Option<Expiration> {
        let slot = self.next_occupied_slot(now)?;
        let deadline =
            self.slot_deadlines[slot].expect("occupied slot must have a tracked minimum deadline");
        let expiration = Expiration::new(self.level, slot, deadline);
        debug_assert!(
            expiration.deadline() >= now,
            "deadline={:016X}; now={:016X}; level={}; lr={:016X}, sr={:016X}, slot={}; occupied={:b}",
            expiration.deadline(),
            now,
            self.level,
            level_range(self.level),
            slot_range(self.level),
            slot,
            self.bitfield
        );

        Some(expiration)
    }

    fn next_occupied_slot(&self, now: u64) -> Option<usize> {
        if self.bitfield == 0 {
            return None;
        }

        let now_slot = ((now >> level_shift(self.level)) as usize) & (Level::LEVEL_MULT - 1);
        let occupied = self.bitfield.rotate_right(now_slot as u32);
        let zeros = occupied.trailing_zeros() as usize;
        let slot = (zeros + now_slot) & (Level::LEVEL_MULT - 1);
        Some(slot)
    }

    /// Add an entry to the bucket corresponding to the expiration time for `item`.
    pub(crate) fn add_entry(&mut self, item: ptr::NonNull<Entry>) -> Expiration {
        let expiration = unsafe { item.as_ref().expiration() };
        let slot = slot_for(expiration, self.level);
        unsafe { item.as_ref().set_location(self.level, slot) };
        self.slots[slot].push_front(item);
        self.bitfield |= occupied_bit(slot);
        let deadline =
            self.slot_deadlines[slot].map_or(expiration, |current| current.min(expiration));
        self.slot_deadlines[slot] = Some(deadline);
        Expiration::new(self.level, slot, deadline)
    }

    pub(crate) unsafe fn remove_entry(
        &mut self,
        item: ptr::NonNull<Entry>,
    ) -> Option<ptr::NonNull<Entry>> {
        let expiration = unsafe { item.as_ref().expiration() };
        let (_, slot) = unsafe { item.as_ref().location() };
        let removed = unsafe { self.slots[slot].remove(item) };
        if self.slots[slot].is_empty() {
            // The slot is empty, mark the bit.
            debug_assert!(
                self.bitfield & occupied_bit(slot) != 0,
                "slot {slot} already marked as unoccupied",
            );
            self.bitfield ^= occupied_bit(slot);
            self.slot_deadlines[slot] = None;
        } else if removed.is_some() && self.slot_deadlines[slot] == Some(expiration) {
            self.slot_deadlines[slot] = self.slots[slot].iter().map(Entry::expiration).min();
        }
        removed
    }

    pub(crate) fn take_slot(&mut self, slot: usize) -> cordyceps::List<Entry> {
        self.bitfield &= !occupied_bit(slot);
        self.slot_deadlines[slot] = None;
        self.slots[slot].split_off(0)
    }
}

const fn level_shift(level: usize) -> usize {
    level * 6
}

const fn slot_range(level: usize) -> u64 {
    1 << level_shift(level)
}

const fn level_range(level: usize) -> u64 {
    1 << (level_shift(level) + 6)
}

/// Convert a duration (milliseconds) and a level to a slot position
pub(crate) const fn slot_for(duration: u64, level: usize) -> usize {
    ((duration >> level_shift(level)) & (Level::LEVEL_MULT as u64 - 1)) as usize
}

const fn occupied_bit(slot: usize) -> u64 {
    1 << slot
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn empty_wheel() {
        let wheel = Level::new(0);
        assert_eq!(None, wheel.next_expiration(0));
    }

    #[test]
    fn level_ranges() {
        assert_eq!(64, level_range(0));
        assert_eq!(4096, level_range(1));
        assert_eq!(262144, level_range(2));
        assert_eq!(16777216, level_range(3));
        assert_eq!(1073741824, level_range(4));
    }

    #[test]
    fn slot_ranges() {
        assert_eq!(1, slot_range(0));
        assert_eq!(64, slot_range(1));
        assert_eq!(4096, slot_range(2));
        assert_eq!(262144, slot_range(3));
    }
}
