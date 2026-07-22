use std::collections::VecDeque;
use std::rc::Rc;

use super::Bid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BufferToken {
    pub(super) position: u64,
    pub(super) bid: Bid,
    pub(super) generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PublishAction {
    pub(super) position: u64,
    pub(super) bid: Bid,
}

#[derive(Debug)]
pub(super) struct Claim {
    pub(super) tokens: Vec<BufferToken>,
    pub(super) publish: Vec<PublishAction>,
}

#[derive(Debug)]
pub(super) struct Returned {
    pub(super) publish: Vec<PublishAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RingFault(Rc<str>);

impl RingFault {
    pub(super) fn new(message: impl Into<Rc<str>>) -> Self {
        Self(message.into())
    }

    pub(super) fn message(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotState {
    Published,
    Claimed,
}

#[derive(Debug, Clone, Copy)]
struct Slot {
    token: BufferToken,
    state: SlotState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BidState {
    Published(BufferToken),
    Owned(BufferToken),
    Held(BufferToken),
}

#[derive(Debug)]
pub(super) struct PublicationLedger {
    base: u64,
    next: u64,
    window: usize,
    slots: Vec<Option<Slot>>,
    bids: Vec<BidState>,
    next_generation: Vec<u64>,
    held: VecDeque<Bid>,
    fault: Option<RingFault>,
}

impl PublicationLedger {
    pub(super) fn new(buf_count: u16, window: usize) -> (Self, Vec<PublishAction>) {
        assert!(buf_count > 0);
        assert!(window >= usize::from(buf_count));

        let mut ledger = Self {
            base: 0,
            next: 0,
            window,
            slots: vec![None; window],
            bids: Vec::with_capacity(usize::from(buf_count)),
            next_generation: vec![1; usize::from(buf_count)],
            held: VecDeque::new(),
            fault: None,
        };
        let mut publish = Vec::with_capacity(usize::from(buf_count));
        for bid in 0..buf_count {
            let token = BufferToken {
                position: ledger.next,
                bid,
                generation: 0,
            };
            ledger.insert_publication(token);
            ledger.bids.push(BidState::Published(token));
            ledger.next += 1;
            publish.push(PublishAction {
                position: token.position,
                bid,
            });
        }
        (ledger, publish)
    }

    pub(super) fn claim_range(&mut self, first_bid: Bid, count: usize) -> Result<Claim, RingFault> {
        if let Some(fault) = &self.fault {
            return Err(fault.clone());
        }
        if count == 0 {
            return self.fail("buffer selection claimed an empty range");
        }
        let first = match self.bids.get(usize::from(first_bid)).copied() {
            Some(BidState::Published(token)) => token,
            Some(_) => return self.fail("completion selected a BID that is not published"),
            None => return self.fail("completion selected a BID outside the ring"),
        };
        let end = match first.position.checked_add(count as u64) {
            Some(end) => end,
            None => return self.fail("buffer selection position overflowed"),
        };
        if end > self.next {
            return self.fail("buffer selection extends beyond published positions");
        }

        let mut tokens = Vec::with_capacity(count);
        for offset in 0..count {
            let position = first.position + offset as u64;
            let Some(slot) = self.slot(position) else {
                return self.fail("buffer selection references retired or missing history");
            };
            if slot.state != SlotState::Published {
                return self.fail("buffer selection overlaps an already claimed position");
            }
            if self.bids.get(usize::from(slot.token.bid)).copied()
                != Some(BidState::Published(slot.token))
            {
                return self.fail("buffer selection disagrees with BID publication state");
            }
            tokens.push(slot.token);
        }

        // Validation above is intentionally complete before mutation.
        for token in &tokens {
            self.slot_mut(token.position)
                .expect("validated slot missing")
                .state = SlotState::Claimed;
            self.bids[usize::from(token.bid)] = BidState::Owned(*token);
        }
        self.retire_prefix();
        let publish = self.drain_held();
        Ok(Claim { tokens, publish })
    }

    pub(super) fn return_buffer(&mut self, token: BufferToken) -> Result<Returned, RingFault> {
        if let Some(fault) = &self.fault {
            return Err(fault.clone());
        }
        match self.bids.get(usize::from(token.bid)).copied() {
            Some(BidState::Owned(current)) if current == token => {}
            Some(_) => return self.fail("returned buffer token is stale or already returned"),
            None => return self.fail("returned buffer BID is outside the ring"),
        }

        if self.history_len() < self.window {
            let action = self.republish(token.bid);
            Ok(Returned {
                publish: vec![action],
            })
        } else {
            self.bids[usize::from(token.bid)] = BidState::Held(token);
            self.held.push_back(token.bid);
            Ok(Returned {
                publish: Vec::new(),
            })
        }
    }

    #[cfg(test)]
    pub(super) fn fault(&self) -> Option<&RingFault> {
        self.fault.as_ref()
    }

    pub(super) fn quarantine(&mut self, message: impl Into<Rc<str>>) -> RingFault {
        if let Some(fault) = &self.fault {
            return fault.clone();
        }
        let fault = RingFault::new(message);
        self.fault = Some(fault.clone());
        fault
    }

    fn fail<T>(&mut self, message: &'static str) -> Result<T, RingFault> {
        let fault = self.quarantine(message);
        Err(fault)
    }

    fn history_len(&self) -> usize {
        usize::try_from(self.next - self.base).expect("ledger history exceeds usize")
    }

    fn slot(&self, position: u64) -> Option<&Slot> {
        let slot = self.slots[position as usize % self.window].as_ref()?;
        (slot.token.position == position).then_some(slot)
    }

    fn slot_mut(&mut self, position: u64) -> Option<&mut Slot> {
        let slot = self.slots[position as usize % self.window].as_mut()?;
        (slot.token.position == position).then_some(slot)
    }

    fn insert_publication(&mut self, token: BufferToken) {
        let index = token.position as usize % self.window;
        assert!(
            self.slots[index].is_none(),
            "publication ledger window overlapped"
        );
        self.slots[index] = Some(Slot {
            token,
            state: SlotState::Published,
        });
    }

    fn retire_prefix(&mut self) {
        while self.base < self.next {
            let index = self.base as usize % self.window;
            let Some(slot) = self.slots[index] else {
                break;
            };
            if slot.token.position != self.base || slot.state != SlotState::Claimed {
                break;
            }
            self.slots[index] = None;
            self.base += 1;
        }
    }

    fn drain_held(&mut self) -> Vec<PublishAction> {
        let mut publish = Vec::new();
        while self.history_len() < self.window {
            let Some(bid) = self.held.pop_front() else {
                break;
            };
            assert!(matches!(self.bids[usize::from(bid)], BidState::Held(_)));
            publish.push(self.republish(bid));
        }
        publish
    }

    fn republish(&mut self, bid: Bid) -> PublishAction {
        let generation = self.next_generation[usize::from(bid)];
        self.next_generation[usize::from(bid)] = generation
            .checked_add(1)
            .expect("buffer publication generation overflowed");
        let token = BufferToken {
            position: self.next,
            bid,
            generation,
        };
        self.insert_publication(token);
        self.bids[usize::from(bid)] = BidState::Published(token);
        self.next += 1;
        PublishAction {
            position: token.position,
            bid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(claim: &Claim, index: usize) -> BufferToken {
        claim.tokens[index]
    }

    #[test]
    fn later_bundle_can_complete_before_earlier_bundle() {
        let (mut ledger, initial) = PublicationLedger::new(4, 8);
        assert_eq!(initial.len(), 4);

        let later = ledger.claim_range(2, 2).unwrap();
        assert_eq!(
            later.tokens.iter().map(|t| t.position).collect::<Vec<_>>(),
            [2, 3]
        );
        assert!(later.publish.is_empty());

        let earlier = ledger.claim_range(0, 2).unwrap();
        assert_eq!(
            earlier
                .tokens
                .iter()
                .map(|t| t.position)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert!(ledger.fault().is_none());
    }

    #[test]
    fn mixed_single_and_bundle_claims_are_order_independent() {
        let (mut ledger, _) = PublicationLedger::new(5, 10);
        ledger.claim_range(4, 1).unwrap();
        ledger.claim_range(1, 3).unwrap();
        ledger.claim_range(0, 1).unwrap();
        assert!(ledger.fault().is_none());
    }

    #[test]
    fn returned_bid_gets_a_new_generation_and_absolute_position() {
        let (mut ledger, _) = PublicationLedger::new(2, 4);
        let first = ledger.claim_range(0, 1).unwrap();
        let original = token(&first, 0);
        let returned = ledger.return_buffer(original).unwrap();
        assert_eq!(returned.publish[0].position, 2);

        let next = ledger.claim_range(0, 1).unwrap();
        assert_eq!(next.tokens[0].position, 2);
        assert_eq!(next.tokens[0].generation, original.generation + 1);
    }

    #[test]
    fn full_history_holds_returns_until_the_gap_retires() {
        let (mut ledger, _) = PublicationLedger::new(2, 2);
        let later = ledger.claim_range(1, 1).unwrap();
        let held = ledger.return_buffer(token(&later, 0)).unwrap();
        assert!(held.publish.is_empty());

        let earlier = ledger.claim_range(0, 1).unwrap();
        assert_eq!(earlier.publish.len(), 1);
        assert_eq!(earlier.publish[0].bid, 1);
        assert_eq!(earlier.publish[0].position, 2);
    }

    #[test]
    fn invalid_range_quarantines_without_partial_claim() {
        let (mut ledger, _) = PublicationLedger::new(3, 6);
        let err = ledger.claim_range(2, 2).unwrap_err();
        assert_eq!(ledger.fault(), Some(&err));
        assert_eq!(
            err.message(),
            "buffer selection extends beyond published positions"
        );
        assert_eq!(ledger.claim_range(0, 1).unwrap_err(), err);
    }

    #[test]
    fn overlapping_claim_quarantines_the_ring() {
        let (mut ledger, _) = PublicationLedger::new(3, 6);
        ledger.claim_range(1, 1).unwrap();
        let err = ledger.claim_range(1, 1).unwrap_err();
        assert_eq!(
            err.message(),
            "completion selected a BID that is not published"
        );
    }

    #[test]
    fn stale_or_double_return_quarantines_the_ring() {
        let (mut ledger, _) = PublicationLedger::new(2, 4);
        let claim = ledger.claim_range(0, 1).unwrap();
        let token = token(&claim, 0);
        ledger.return_buffer(token).unwrap();
        let err = ledger.return_buffer(token).unwrap_err();
        assert_eq!(
            err.message(),
            "returned buffer token is stale or already returned"
        );
    }
}
