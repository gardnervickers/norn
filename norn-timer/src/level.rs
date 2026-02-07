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
}

impl Level {
    pub(crate) const LEVEL_MULT: usize = 64;

    pub(crate) fn new(level: usize) -> Self {
        Self {
            level,
            bitfield: 0,
            slots: std::array::from_fn(|_| cordyceps::List::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn num_registered(&self) -> usize {
        self.slots.iter().map(|s| s.len()).sum()
    }

    pub(crate) fn next_expiration(&self, now: u64) -> Option<Expiration> {
        let slot = self.next_occupied_slot(now)?;
        let expiration = expiration_for_slot(self.level, slot, now);
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
    pub(crate) fn add_entry(&mut self, item: ptr::NonNull<Entry>) -> usize {
        let expiration = unsafe { item.as_ref().expiration() };
        let slot = slot_for(expiration, self.level);
        self.slots[slot].push_front(item);
        self.bitfield |= occupied_bit(slot);
        slot
    }

    pub(crate) unsafe fn remove_entry(
        &mut self,
        item: ptr::NonNull<Entry>,
    ) -> Option<ptr::NonNull<Entry>> {
        let expiration = unsafe { item.as_ref().expiration() };
        let slot = slot_for(expiration, self.level);
        let removed = unsafe { self.slots[slot].remove(item) };
        if self.slots[slot].is_empty() {
            // The slot is empty, mark the bit.
            debug_assert!(
                self.bitfield & occupied_bit(slot) != 0,
                "slot {slot} already marked as unoccupied",
            );
            self.bitfield ^= occupied_bit(slot);
        }
        removed
    }

    pub(crate) fn take_slot(&mut self, slot: usize) -> cordyceps::List<Entry> {
        self.bitfield &= !occupied_bit(slot);
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

pub(crate) fn expiration_for_slot(level: usize, slot: usize, now: u64) -> Expiration {
    let level_range = level_range(level);
    let level_shift = level_shift(level);
    let level_start = now & !(level_range - 1);
    // Return the deadline corresponding to the target level + slot
    // rather than the exact deadline for the slot. This is because
    // we will either fire the entire slot in one shot, or cascade it down.
    let mut deadline = level_start + ((slot as u64) << level_shift);

    if deadline <= now {
        // From the Tokio implementation:
        //
        // A timer is in a slot "prior" to the current time. This can occur
        // because we do not have an infinite hierarchy of timer levels, and
        // eventually a timer scheduled for a very distant time might end up
        // being placed in a slot that is beyond the end of all of the
        // arrays.
        //
        // To deal with this, we first limit timers to being scheduled no
        // more than MAX_DURATION ticks in the future; that is, they're at
        // most one rotation of the top level away. Then, we force timers
        // that logically would go into the top+1 level, to instead go into
        // the top level's slots.
        //
        // What this means is that the top level's slots act as a
        // pseudo-ring buffer, and we rotate around them indefinitely. If we
        // compute a deadline before now, and it's the top level, it
        // therefore means we're actually looking at a slot in the future.

        debug_assert_eq!(
            level,
            crate::NUM_LEVELS - 1,
            "level {} != {}",
            level,
            crate::NUM_LEVELS
        );
        deadline += level_range;
    }

    Expiration::new(level, slot, deadline)
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
