use std::collections::VecDeque;

macro_rules! assert_some {
    ($e:expr) => {
        assert_some!($e,)
    };
    ($e:expr,) => {{
        use std::option::Option::*;
        match $e {
            Some(v) => v,
            None => panic!("assertion failed: None"),
        }
    }};
    ($e:expr, $($arg:tt)+) => {{
        use std::option::Option::*;
        match $e {
            Some(v) => v,
            None => panic!("assertion failed: None: {}", format_args!($($arg)+)),
        }
    }};
}

#[derive(Debug)]
pub struct Slots<T> {
    first: u64,
    capacity: usize,
    slots: VecDeque<Option<T>>,
}

impl<T> Slots<T> {
    pub fn print_size(&self) {
        eprintln!("capacity: {}", self.slots.capacity());
        eprintln!("len: {}", self.slots.len());
    }

    pub fn with_first(first: u64, capacity: usize) -> Self {
        Self {
            first,
            capacity,
            slots: VecDeque::default(),
        }
    }

    /// returns Option for use with ? operator
    fn in_bounds(&self, index: u64) -> Option<()> {
        if (self.first..(self.first + self.capacity as u64)).contains(&index) {
            Some(())
        } else {
            None
        }
    }

    #[track_caller]
    fn vec_index(&self, index: u64) -> usize {
        assert_some!(
            self.in_bounds(index),
            "{} is not in range {} to {}",
            index,
            self.first,
            self.first + self.capacity as u64,
        );
        (index - self.first as u64) as usize
    }

    pub fn get(&self, index: u64) -> Option<&T> {
        self.in_bounds(index)?;
        let index = self.vec_index(index);
        self.slots.get(index)?.as_ref()
    }

    pub fn get_mut(&mut self, index: u64) -> Option<&mut T> {
        self.in_bounds(index)?;
        let index = self.vec_index(index);
        self.slots.get_mut(index)?.as_mut()
    }

    fn contains(&self, index: u64) -> bool {
        self.slots
            .get(self.vec_index(index))
            .map_or(false, Option::is_some)
    }

    pub fn insert(&mut self, index: u64, value: T) -> Option<&mut T> {
        self.in_bounds(index)?;
        let index = self.vec_index(index);

        if let Some(slot) = self.slots.get_mut(index) {
            *slot = Some(value);
        } else {
            while self.slots.len() < index {
                self.slots.push_back(None);
            }

            self.slots.push_back(Some(value));
        }

        self.slots.get_mut(index)?.as_mut()
    }

    pub fn get_or_insert_with<F>(&mut self, index: u64, mut f: F) -> Option<&mut T>
    where
        F: FnMut() -> T,
    {
        self.in_bounds(index)?;
        if self.contains(index) {
            self.get_mut(index)
        } else {
            self.insert(index, f())
        }
    }

    pub fn advance(&mut self, k: u64) {
        self.first += k;
        for _ in 0..k {
            self.slots.pop_front();
        }
    }

    pub fn advance_iter(&mut self, k: u64) -> impl Iterator<Item = T> + '_ {
        self.first += k;
        (0..k).filter_map(move |_| self.slots.pop_front().flatten())
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn first_idx(&self) -> u64 {
        self.first
    }

    pub fn slot_iter(&self) -> impl Iterator<Item = (u64, &'_ T)> {
        (self.first..).zip(self.iter())
    }
}

impl<T> Slots<T> {
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().filter_map(Option::as_ref)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.slots.iter_mut().filter_map(Option::as_mut)
    }
}

#[cfg(test)]
mod test {
    use crate::protocol::slots::Slots;

    #[test]
    fn insert() {
        let mut slots: Slots<usize> = Slots::with_first(1, 10);
        slots.insert(1, 1);
        slots.insert(2, 2);
        slots.insert(3, 3);

        assert_eq!(Some(&1), slots.get(1));
        assert_eq!(Some(&2), slots.get(2));
        assert_eq!(Some(&3), slots.get(3));
    }

    #[test]
    fn insert_holes() {
        let mut slots: Slots<usize> = Slots::with_first(1, 10);
        slots.insert(1, 1);
        slots.insert(4, 2);
        slots.insert(8, 3);

        assert_eq!(Some(&1), slots.get(1));
        assert_eq!(None, slots.get(2));
        assert_eq!(Some(&2), slots.get(4));
        assert_eq!(None, slots.get(5));
        assert_eq!(Some(&3), slots.get(8));
        assert_eq!(slots.len(), 8);
    }

    #[test]
    fn advance() {
        let mut slots: Slots<usize> = Slots::with_first(1, 10);
        slots.insert(1, 1);
        slots.insert(2, 2);
        slots.insert(3, 3);

        assert_eq!(Some(&1), slots.get(1));
        slots.advance(1);
        // this now out of range
        assert_eq!(None, slots.get(1));
        assert_eq!(Some(&2), slots.get(2));
        assert_eq!(Some(&3), slots.get(3));
        assert_eq!(slots.len(), 2);
    }

    #[test]
    fn advance_beyond_cap() {
        let mut slots: Slots<usize> = Slots::with_first(1, 10);
        slots.insert(1, 1);
        slots.insert(2, 2);
        slots.insert(3, 3);

        assert_eq!(Some(&3), slots.get(3));
        slots.advance(10);
        // this now out of range
        assert_eq!(None, slots.get(3));
        assert_eq!(None, slots.get(11));
        assert!(slots.is_empty());
    }
}
