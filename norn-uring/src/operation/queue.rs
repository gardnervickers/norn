use std::collections::VecDeque;

/// A FIFO completion queue with an allocation-free common path.
///
/// Singleshot operations and multishot operations whose consumer keeps pace
/// store their sole pending completion inline. Once an operation has observed
/// a backlog, the deque retains its high-water capacity so a steady producer
/// and consumer do not repeatedly allocate.
pub(crate) struct CompletionQueue<T> {
    storage: CompletionStorage<T>,
}

enum CompletionStorage<T> {
    Empty,
    One(T),
    Many(VecDeque<T>),
}

impl<T> CompletionQueue<T> {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            storage: CompletionStorage::Empty,
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, value: T) {
        match &mut self.storage {
            CompletionStorage::Empty => self.storage = CompletionStorage::One(value),
            CompletionStorage::One(_) => {
                let CompletionStorage::One(first) =
                    std::mem::replace(&mut self.storage, CompletionStorage::Empty)
                else {
                    unreachable!()
                };
                let mut overflow = VecDeque::with_capacity(2);
                overflow.push_back(first);
                overflow.push_back(value);
                self.storage = CompletionStorage::Many(overflow);
            }
            CompletionStorage::Many(overflow) => overflow.push_back(value),
        }
    }

    #[inline]
    pub(crate) fn pop_front(&mut self) -> Option<T> {
        match &mut self.storage {
            CompletionStorage::Empty => None,
            CompletionStorage::One(_) => {
                let CompletionStorage::One(completion) =
                    std::mem::replace(&mut self.storage, CompletionStorage::Empty)
                else {
                    unreachable!()
                };
                Some(completion)
            }
            CompletionStorage::Many(overflow) => overflow.pop_front(),
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        match &self.storage {
            CompletionStorage::Empty => true,
            CompletionStorage::One(_) => false,
            CompletionStorage::Many(overflow) => overflow.is_empty(),
        }
    }
}

impl<T> Default for CompletionQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_completion_stays_inline() {
        let mut queue = CompletionQueue::new();
        queue.push(7);
        assert!(matches!(queue.storage, CompletionStorage::One(_)));
        assert_eq!(queue.pop_front().unwrap(), 7);
        assert!(matches!(queue.storage, CompletionStorage::Empty));
    }

    #[test]
    fn overflow_is_fifo_and_retains_high_water_storage() {
        let mut queue = CompletionQueue::new();
        for value in 0..128 {
            queue.push(value);
        }
        let capacity = match &queue.storage {
            CompletionStorage::Many(overflow) => overflow.capacity(),
            _ => panic!("completion backlog did not use overflow storage"),
        };

        for expected in 0..128 {
            assert_eq!(queue.pop_front().unwrap(), expected);
        }
        assert!(queue.is_empty());
        assert_eq!(
            match &queue.storage {
                CompletionStorage::Many(overflow) => overflow.capacity(),
                _ => panic!("completion queue discarded overflow storage"),
            },
            capacity
        );
    }
}
