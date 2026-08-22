//! Tracks ownership of buffers published through a kernel buffer ring.
//!
//! A buffer ID (BID) names a reusable allocation. A publication is one
//! occurrence of making that BID selectable by advancing the shared ring tail.
//! Kernel selection consumes the publication; returning the selected buffer may
//! create a later publication of the same BID.
//!
//! Each publication receives a monotonically increasing ticket. The BID names
//! the allocation; the ticket distinguishes repeated publications and records
//! their ring order.
//!
//! A claim reconciles a CQE with publications selected by the kernel.
//! [`BufferToken`] identifies one claimed publication, [`BundleClaim`] preserves
//! the publications selected by one bundle CQE, and [`PublicationTracker`]
//! records publications not yet claimed by a CQE. Claims begin at the BID
//! reported by each CQE because CQE order may differ from selection order;
//! bundle claims require consecutive tickets.

use smallvec::SmallVec;

use super::Bid;

const NO_BID: Bid = Bid::MAX;

/// An ownership token for one publication of a buffer ID.
///
/// A BID may be returned and published again many times. The ticket prevents a
/// stale owner from returning a later publication of the same BID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BufferToken {
    bid: Bid,
    ticket: u64,
}

impl BufferToken {
    pub(super) fn bid(self) -> Bid {
        self.bid
    }

    #[cfg(test)]
    fn ticket(self) -> u64 {
        self.ticket
    }
}

/// A nonempty ordered sequence of buffer publications claimed by one bundle CQE.
///
/// Tickets in a bundle are consecutive by construction. Keeping only the
/// first ticket lets the common numerically-contiguous BID case remain compact.
#[derive(Debug)]
pub(super) struct BundleClaim {
    bids: ClaimedBids,
    first_ticket: u64,
    buf_count: u16,
}

impl BundleClaim {
    pub(super) fn len(&self) -> usize {
        self.bids.len()
    }

    pub(super) fn iter(&self) -> impl ExactSizeIterator<Item = BufferToken> + '_ {
        (0..self.len()).map(|index| BufferToken {
            bid: self.bids.get(index, self.buf_count),
            ticket: self.first_ticket + index as u64,
        })
    }
}

#[derive(Debug)]
enum ClaimedBids {
    Contiguous { first: Bid, count: u16 },
    Sparse(SmallVec<[Bid; 4]>),
}

impl ClaimedBids {
    fn first(bid: Bid) -> Self {
        Self::Contiguous {
            first: bid,
            count: 1,
        }
    }

    fn push(&mut self, bid: Bid, buf_count: u16) {
        match self {
            Self::Contiguous { first, count }
                if sequential_bid(*first, usize::from(*count), buf_count) == bid =>
            {
                *count += 1;
            }
            Self::Contiguous { first, count } => {
                let mut bids = SmallVec::with_capacity(usize::from(*count) + 1);
                for index in 0..usize::from(*count) {
                    bids.push(sequential_bid(*first, index, buf_count));
                }
                bids.push(bid);
                *self = Self::Sparse(bids);
            }
            Self::Sparse(bids) => bids.push(bid),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Contiguous { count, .. } => usize::from(*count),
            Self::Sparse(bids) => bids.len(),
        }
    }

    fn get(&self, index: usize, buf_count: u16) -> Bid {
        match self {
            Self::Contiguous { first, count } => {
                assert!(index < usize::from(*count));
                sequential_bid(*first, index, buf_count)
            }
            Self::Sparse(bids) => bids[index],
        }
    }
}

fn sequential_bid(first: Bid, offset: usize, buf_count: u16) -> Bid {
    ((usize::from(first) + offset) % usize::from(buf_count)) as Bid
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum SlotState {
    Published,
    Owned,
}

/// The current publication record for one BID.
///
/// The ticket remains while the buffer is owned so its return can be matched
/// to the publication that was claimed. Links contain only published,
/// unclaimed entries and remain ordered by ticket, though removed publications
/// can leave ticket gaps between adjacent links.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct PublicationSlot {
    ticket: u64,
    prev: Bid,
    next: Bid,
    state: SlotState,
}

/// A fixed-size ledger of only the current publication of each BID.
///
/// The tracker has no historical window: claiming removes a publication from
/// the linked set, and returning that owner appends a fresh publication with a
/// new ticket. Any contradiction poisons the tracker permanently.
#[derive(Debug)]
pub(super) struct PublicationTracker {
    slots: Box<[PublicationSlot]>,
    head: Bid,
    tail: Bid,
    poisoned: bool,
}

impl PublicationTracker {
    /// Construct the state corresponding to initially publishing every BID in
    /// numerical order.
    pub(super) fn new(buf_count: u16) -> Self {
        assert!(buf_count > 0);
        assert!(buf_count < NO_BID);

        let slots = (0..buf_count)
            .map(|bid| PublicationSlot {
                ticket: u64::from(bid),
                prev: bid.checked_sub(1).unwrap_or(NO_BID),
                next: if bid + 1 == buf_count {
                    NO_BID
                } else {
                    bid + 1
                },
                state: SlotState::Published,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            slots,
            head: 0,
            tail: buf_count - 1,
            poisoned: false,
        }
    }

    pub(super) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Enter the fail-closed state because accounting outside the tracker
    /// observed a contradiction, such as an impossible CQE length.
    pub(super) fn poison(&mut self) {
        self.poisoned = true;
    }

    /// Claim one selected BID and bind its current publication ticket to the
    /// returned owner.
    pub(super) fn claim_one(&mut self, bid: Bid) -> Result<BufferToken, TrackerError> {
        let claim = self.claim_bundle(bid, 1)?;
        let token = claim
            .iter()
            .next()
            .expect("a one-buffer claim must contain one token");
        Ok(token)
    }

    /// Claim `count` consecutive publications beginning with `first_bid`.
    ///
    /// Consecutive linked nodes are insufficient: a previously claimed scalar
    /// buffer can leave two unrelated publications adjacent in the live list.
    /// Consecutive ticket values prevent a bundle from crossing that gap.
    pub(super) fn claim_bundle(
        &mut self,
        first_bid: Bid,
        count: usize,
    ) -> Result<BundleClaim, TrackerError> {
        self.ensure_healthy()?;
        if count == 0 || count > self.slots.len() {
            return self.fail(TrackerError::InvalidBundleCount {
                count,
                buf_count: self.slots.len(),
            });
        }

        let Some(first_slot) = self.slots.get(usize::from(first_bid)) else {
            return self.fail(TrackerError::InvalidBid {
                bid: first_bid,
                buf_count: self.slots.len(),
            });
        };
        if first_slot.state != SlotState::Published {
            return self.fail(TrackerError::BidNotPublished { bid: first_bid });
        }

        let first_ticket = first_slot.ticket;
        let before = first_slot.prev;
        let mut current = first_bid;
        let mut previous = NO_BID;
        let mut bids = ClaimedBids::first(first_bid);

        for index in 0..count {
            let Some(slot) = self.slots.get(usize::from(current)) else {
                return self.fail(TrackerError::BrokenLink { bid: current });
            };
            if slot.state != SlotState::Published {
                return self.fail(TrackerError::BidNotPublished { bid: current });
            }
            if index > 0 && slot.prev != previous {
                return self.fail(TrackerError::BrokenLink { bid: current });
            }

            let Some(expected_ticket) = first_ticket.checked_add(index as u64) else {
                return self.fail(TrackerError::TicketOverflow);
            };
            if slot.ticket != expected_ticket {
                return self.fail(TrackerError::TicketGap {
                    previous,
                    next: current,
                    expected: expected_ticket,
                    actual: slot.ticket,
                });
            }

            if index > 0 {
                bids.push(current, self.slots.len() as u16);
            }
            previous = current;

            if index + 1 < count {
                if slot.next == NO_BID {
                    return self.fail(TrackerError::BundleExhausted {
                        first: first_bid,
                        count,
                    });
                }
                current = slot.next;
            }
        }

        let last = current;
        let after = self.slots[usize::from(last)].next;
        if before == NO_BID {
            if self.head != first_bid {
                return self.fail(TrackerError::BrokenLink { bid: first_bid });
            }
        } else if self
            .slots
            .get(usize::from(before))
            .is_none_or(|slot| slot.next != first_bid)
        {
            return self.fail(TrackerError::BrokenLink { bid: first_bid });
        }
        if after == NO_BID {
            if self.tail != last {
                return self.fail(TrackerError::BrokenLink { bid: last });
            }
        } else if self
            .slots
            .get(usize::from(after))
            .is_none_or(|slot| slot.prev != last)
        {
            return self.fail(TrackerError::BrokenLink { bid: last });
        }

        // All validation and any SmallVec allocation completed before the first
        // mutation, so committing the range cannot leave a partial claim.
        if before == NO_BID {
            self.head = after;
        } else {
            self.slots[usize::from(before)].next = after;
        }
        if after == NO_BID {
            self.tail = before;
        } else {
            self.slots[usize::from(after)].prev = before;
        }
        for bid in bids.iter(self.slots.len() as u16) {
            let slot = &mut self.slots[usize::from(bid)];
            slot.prev = NO_BID;
            slot.next = NO_BID;
            slot.state = SlotState::Owned;
        }

        Ok(BundleClaim {
            bids,
            first_ticket,
            buf_count: self.slots.len() as u16,
        })
    }

    fn ensure_healthy(&self) -> Result<(), TrackerError> {
        if self.poisoned {
            Err(TrackerError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn fail<T>(&mut self, error: TrackerError) -> Result<T, TrackerError> {
        self.poisoned = true;
        Err(error)
    }
}

impl ClaimedBids {
    fn iter(&self, buf_count: u16) -> impl ExactSizeIterator<Item = Bid> + '_ {
        (0..self.len()).map(move |index| self.get(index, buf_count))
    }
}

/// A contradiction between kernel completion accounting and live publication
/// ownership. Every variant other than `Poisoned` is the first error that
/// causes the tracker to enter its permanent fail-closed state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TrackerError {
    Poisoned,
    InvalidBid {
        bid: Bid,
        buf_count: usize,
    },
    InvalidBundleCount {
        count: usize,
        buf_count: usize,
    },
    BidNotPublished {
        bid: Bid,
    },
    BrokenLink {
        bid: Bid,
    },
    TicketGap {
        previous: Bid,
        next: Bid,
        expected: u64,
        actual: u64,
    },
    BundleExhausted {
        first: Bid,
        count: usize,
    },
    TicketOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bids(claim: &BundleClaim) -> Vec<Bid> {
        claim.iter().map(BufferToken::bid).collect()
    }

    #[test]
    fn slot_is_sixteen_bytes() {
        assert_eq!(std::mem::size_of::<PublicationSlot>(), 16);
    }

    #[test]
    fn scalar_claims_can_arrive_after_later_publications() {
        let mut tracker = PublicationTracker::new(2);

        let later = tracker.claim_one(1).unwrap();
        let earlier = tracker.claim_one(0).unwrap();

        assert_eq!((later.bid(), later.ticket()), (1, 1));
        assert_eq!((earlier.bid(), earlier.ticket()), (0, 0));
        assert!(!tracker.is_poisoned());
    }

    #[test]
    fn delayed_bundle_survives_a_later_scalar_completion() {
        let mut tracker = PublicationTracker::new(4);

        let later = tracker.claim_one(3).unwrap();
        let delayed = tracker.claim_bundle(0, 3).unwrap();

        assert_eq!(later.bid(), 3);
        assert_eq!(bids(&delayed), [0, 1, 2]);
        assert_eq!(
            delayed.iter().map(BufferToken::ticket).collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }

    #[test]
    fn bundle_cannot_cross_a_claimed_publication_gap() {
        let mut tracker = PublicationTracker::new(3);
        let _middle = tracker.claim_one(1).unwrap();

        let error = tracker.claim_bundle(0, 2).unwrap_err();

        assert!(matches!(
            error,
            TrackerError::TicketGap {
                previous: 0,
                next: 2,
                expected: 1,
                actual: 2,
            }
        ));
        assert!(tracker.is_poisoned());
        assert_eq!(tracker.claim_one(0), Err(TrackerError::Poisoned));
    }

    #[test]
    fn external_poison_is_sticky() {
        let mut tracker = PublicationTracker::new(2);
        tracker.poison();

        assert!(tracker.is_poisoned());
        assert_eq!(tracker.claim_one(0), Err(TrackerError::Poisoned));
    }
}
