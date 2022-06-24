use serde::{Deserialize, Serialize};

use crate::{app::Request, net::Message};
use std::{cmp, convert::TryFrom, iter::FromIterator, ops::Index};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Proposal<T = Message<Request>> {
    Single(T),
    Batch(Vec<T>),
}

impl<T> Proposal<T> {
    pub fn len(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Batch(b) => b.len(),
        }
    }

    pub fn assert_single(self) -> T {
        match self {
            Proposal::Single(r) => r,
            Proposal::Batch(_) => panic!("proposal must be single"),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ T> {
        self.into_iter()
    }
}

impl<T> Default for Proposal<T> {
    fn default() -> Self {
        Self::Batch(Vec::new())
    }
}

impl<T> TryFrom<Vec<T>> for Proposal<T> {
    type Error = &'static str;

    fn try_from(mut value: Vec<T>) -> Result<Self, Self::Error> {
        match value.len() {
            0 => Err("empty proposal"),
            1 => Ok(Proposal::Single(value.remove(0))),
            _ => Ok(Proposal::Batch(value)),
        }
    }
}

impl<U> FromIterator<U> for Proposal<U> {
    fn from_iter<T: IntoIterator<Item = U>>(iter: T) -> Self {
        let mut iter = iter.into_iter();
        let first = iter.next();
        let first = match first {
            None => return Self::Batch(Vec::default()),
            Some(first) => first,
        };

        let second = match iter.next() {
            None => return Self::Single(first),
            Some(second) => second,
        };

        let mut vec = Vec::with_capacity(cmp::max(iter.size_hint().0, 2));
        vec.push(first);
        vec.push(second);
        vec.extend(iter);
        Self::Batch(vec)
    }
}

impl From<Message<Request>> for Proposal {
    fn from(value: Message<Request>) -> Self {
        Proposal::Single(value)
    }
}

impl<'a, T> IntoIterator for &'a Proposal<T> {
    type Item = &'a T;
    type IntoIter = Iter<std::iter::Once<&'a T>, std::slice::Iter<'a, T>>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            Proposal::Single(request) => Iter::Left(std::iter::once(request)),
            Proposal::Batch(batch) => Iter::Right(batch.iter()),
        }
    }
}

impl<T> IntoIterator for Proposal<T> {
    type Item = T;
    type IntoIter = Iter<std::iter::Once<T>, std::vec::IntoIter<T>>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Single(request) => Iter::Left(std::iter::once(request)),
            Self::Batch(batch) => Iter::Right(batch.into_iter()),
        }
    }
}

pub enum Iter<Left, Right> {
    Left(Left),
    Right(Right),
}

impl<Left, Right> Iterator for Iter<Left, Right>
where
    Left: Iterator,
    Right: Iterator<Item = Left::Item>,
{
    type Item = Left::Item;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Left(i) => i.next(),
            Iter::Right(i) => i.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Iter::Left(i) => i.size_hint(),
            Iter::Right(i) => i.size_hint(),
        }
    }
}

impl<T> Index<usize> for Proposal<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        match (index, self) {
            (0, Proposal::Single(value)) => value,
            (_, Proposal::Single(_)) => {
                panic!("Proposal::Single not indexable with index {}", index)
            }
            (i, Proposal::Batch(batch)) => &batch[i],
        }
    }
}
