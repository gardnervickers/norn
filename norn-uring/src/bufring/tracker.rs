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
    pub(super) fn empty() -> Self {
        Self {
            bid: NO_BID,
            ticket: 0,
        }
    }

    pub(super) fn is_empty(self) -> bool {
        self.bid == NO_BID
    }

    pub(super) fn bid(self) -> Bid {
        assert!(!self.is_empty(), "empty buffer token has no BID");
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
    // Assigned to the next buffer returned to the ring. It advances once per
    // publication and is never reused, so an old token cannot match a later
    // availability period for the same BID.
    next_ticket: u64,
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
            next_ticket: u64::from(buf_count),
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

    /// Republish one returned buffer after validating its ownership ticket.
    pub(super) fn return_one(&mut self, token: BufferToken) -> Result<(), TrackerError> {
        self.ensure_healthy()?;
        let Some(next_ticket) = self.next_ticket.checked_add(1) else {
            return self.fail(TrackerError::TicketOverflow);
        };
        if let Some(error) = self.return_error(token) {
            return self.fail(error);
        }

        self.append_owned(token.bid, self.next_ticket);
        self.next_ticket = next_ticket;
        Ok(())
    }

    /// Republish a returned bundle after validating every ownership ticket.
    pub(super) fn return_bundle(&mut self, claim: &BundleClaim) -> Result<(), TrackerError> {
        self.ensure_healthy()?;
        if usize::from(claim.buf_count) != self.slots.len() {
            return self.fail(TrackerError::WrongTracker {
                claim_buf_count: claim.buf_count,
                tracker_buf_count: self.slots.len(),
            });
        }
        let Some(next_ticket) = self.next_ticket.checked_add(claim.len() as u64) else {
            return self.fail(TrackerError::TicketOverflow);
        };

        // Validate the whole bundle before publishing any part of it. Tickets
        // make duplicate BIDs fail this preflight without transient state.
        if let Some(error) = claim.iter().find_map(|token| self.return_error(token)) {
            return self.fail(error);
        }

        let start_ticket = self.next_ticket;
        for (index, token) in claim.iter().enumerate() {
            self.append_owned(token.bid, start_ticket + index as u64);
        }
        self.next_ticket = next_ticket;
        Ok(())
    }

    fn return_error(&self, token: BufferToken) -> Option<TrackerError> {
        match self.slots.get(usize::from(token.bid)) {
            None => Some(TrackerError::InvalidBid {
                bid: token.bid,
                buf_count: self.slots.len(),
            }),
            Some(slot) if slot.state != SlotState::Owned || slot.ticket != token.ticket => {
                Some(TrackerError::StaleReturn {
                    bid: token.bid,
                    ticket: token.ticket,
                })
            }
            Some(_) => None,
        }
    }

    fn append_owned(&mut self, bid: Bid, ticket: u64) {
        let old_tail = self.tail;
        let slot = &mut self.slots[usize::from(bid)];
        debug_assert_eq!(slot.state, SlotState::Owned);
        slot.ticket = ticket;
        slot.prev = old_tail;
        slot.next = NO_BID;
        slot.state = SlotState::Published;

        if old_tail == NO_BID {
            self.head = bid;
        } else {
            self.slots[usize::from(old_tail)].next = bid;
        }
        self.tail = bid;
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
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub(super) enum TrackerError {
    #[error("buffer publication tracker is poisoned")]
    Poisoned,
    #[error("buffer id {bid} is outside ring bounds ({buf_count})")]
    InvalidBid { bid: Bid, buf_count: usize },
    #[error("bundle claims {count} buffers from a {buf_count}-buffer ring")]
    InvalidBundleCount { count: usize, buf_count: usize },
    #[error("buffer id {bid} has no live publication")]
    BidNotPublished { bid: Bid },
    #[error("publication links are inconsistent at buffer id {bid}")]
    BrokenLink { bid: Bid },
    #[error(
        "buffer ids {previous} and {next} are not adjacent publications (expected ticket {expected}, found {actual})"
    )]
    TicketGap {
        previous: Bid,
        next: Bid,
        expected: u64,
        actual: u64,
    },
    #[error("bundle beginning at buffer id {first} has fewer than {count} live publications")]
    BundleExhausted { first: Bid, count: usize },
    #[error("buffer id {bid} return does not own publication ticket {ticket}")]
    StaleReturn { bid: Bid, ticket: u64 },
    #[error(
        "bundle from a {claim_buf_count}-buffer tracker was returned to a {tracker_buf_count}-buffer tracker"
    )]
    WrongTracker {
        claim_buf_count: u16,
        tracker_buf_count: usize,
    },
    #[error("buffer publication ticket space exhausted")]
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
    fn sparse_return_order_becomes_the_next_bundle_order() {
        let mut tracker = PublicationTracker::new(3);
        let initial = tracker.claim_bundle(0, 3).unwrap();
        let tokens = initial.iter().collect::<Vec<_>>();

        tracker.return_one(tokens[2]).unwrap();
        tracker.return_one(tokens[0]).unwrap();
        tracker.return_one(tokens[1]).unwrap();
        let recycled = tracker.claim_bundle(2, 3).unwrap();

        assert_eq!(bids(&recycled), [2, 0, 1]);
        assert_eq!(
            recycled.iter().map(BufferToken::ticket).collect::<Vec<_>>(),
            [3, 4, 5]
        );
    }

    #[test]
    fn stale_return_after_reselection_poisons_without_returning_current_owner() {
        let mut tracker = PublicationTracker::new(1);
        let stale = tracker.claim_one(0).unwrap();
        tracker.return_one(stale).unwrap();
        let current = tracker.claim_one(0).unwrap();

        let error = tracker.return_one(stale).unwrap_err();

        assert_eq!(error, TrackerError::StaleReturn { bid: 0, ticket: 0 });
        assert!(tracker.is_poisoned());
        assert_eq!(tracker.return_one(current), Err(TrackerError::Poisoned));
        assert_eq!(tracker.slots[0].state, SlotState::Owned);
        assert_eq!(tracker.slots[0].ticket, current.ticket());
    }

    #[test]
    fn duplicate_bundle_return_is_fail_closed() {
        let mut tracker = PublicationTracker::new(2);
        let claim = tracker.claim_bundle(0, 2).unwrap();
        tracker.return_bundle(&claim).unwrap();

        let error = tracker.return_bundle(&claim).unwrap_err();

        assert!(matches!(error, TrackerError::StaleReturn { .. }));
        assert!(tracker.is_poisoned());
    }

    #[test]
    fn malformed_batch_is_rejected_before_any_publication() {
        let mut tracker = PublicationTracker::new(2);
        let mut claim = tracker.claim_bundle(0, 2).unwrap();
        claim.bids = ClaimedBids::Sparse(SmallVec::from_slice(&[0, 0]));

        let error = tracker.return_bundle(&claim).unwrap_err();

        assert_eq!(error, TrackerError::StaleReturn { bid: 0, ticket: 1 });
        assert!(tracker.is_poisoned());
        assert_eq!(tracker.slots[0].state, SlotState::Owned);
        assert_eq!(tracker.slots[1].state, SlotState::Owned);
        assert_eq!(tracker.head, NO_BID);
        assert_eq!(tracker.tail, NO_BID);
    }

    #[test]
    fn ticket_overflow_poison_does_not_publish_the_owner() {
        let mut tracker = PublicationTracker::new(1);
        let token = tracker.claim_one(0).unwrap();
        tracker.next_ticket = u64::MAX;

        assert_eq!(tracker.return_one(token), Err(TrackerError::TicketOverflow));
        assert!(tracker.is_poisoned());
        assert_eq!(tracker.slots[0].state, SlotState::Owned);
        assert_eq!(tracker.head, NO_BID);
        assert_eq!(tracker.tail, NO_BID);
    }

    #[test]
    fn external_poison_is_sticky() {
        let mut tracker = PublicationTracker::new(2);
        tracker.poison();

        assert!(tracker.is_poisoned());
        assert_eq!(tracker.claim_one(0), Err(TrackerError::Poisoned));
    }
}
