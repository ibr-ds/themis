use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

use tracing::trace;

use crate::{
    error::Error as BftError,
    protocol::{
        quorum::State::{Empty, Finished, Pending},
        Match,
    },
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum State {
    Empty,
    Finished(usize),
    Pending(usize),
}

impl Default for State {
    fn default() -> Self {
        State::Empty
    }
}

#[derive(Debug, PartialEq)]
pub struct Error(usize);

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Quorum failed.")
    }
}

impl StdError for Error {}

impl From<Error> for BftError {
    fn from(e: Error) -> Self {
        BftError::protocol(e)
    }
}

#[derive(Debug)]
pub struct Quorum<T> {
    classes: Vec<Vec<T>>,
    state: State,
    quorum: usize,
    capacity: usize,
}

impl<T> Quorum<T> {
    pub fn new(quorum: usize, capacity: usize) -> Self {
        Self {
            classes: Default::default(),
            state: Default::default(),
            quorum,
            capacity,
        }
    }
}

impl<T: Match<T>> Quorum<T> {
    pub fn prototype(&self, class: usize) -> Option<&T> {
        self.classes.get(class).and_then(|class| class.first())
    }

    pub fn messages(&self) -> impl Iterator<Item = &T> {
        self.classes.iter().flat_map(|class| class.iter())
    }

    pub fn winning_messages(&self) -> impl Iterator<Item = &T> {
        self.classes
            .iter()
            .max_by_key(|class| class.len())
            .map_or([].iter(), |class| class.iter())
    }

    pub fn take_class(&mut self, class: usize) -> Vec<T> {
        std::mem::take(&mut self.classes[class])
    }

    pub fn take_and_clear(&mut self) -> Option<T> {
        match self.state {
            State::Empty => None,
            State::Pending(i) | State::Finished(i) => {
                let message = self.classes[i].pop().expect("class cannot be empty");
                self.classes.clear();
                Some(message)
            }
        }
    }

    pub fn destroy(mut self) -> Option<T> {
        self.take_and_clear()
    }

    pub fn state(&self) -> State {
        self.state
    }

    pub fn offer(&mut self, message: T) -> Result<State, Error> {
        if self.messages().any(|m| *m == message) {
            trace!("Quorum already contains message: {:?}", message);
            return self.compute_state();
        }
        let class = self.get_class(&message);

        self.classes[class].push(message);
        trace!("Class {}, new count {}", class, self.classes[class].len());

        self.compute_state()
    }

    pub fn class_count(&self) -> usize {
        self.classes.len()
    }

    fn compute_state(&mut self) -> Result<State, Error> {
        let winning_class = self
            .classes
            .iter()
            .enumerate()
            .max_by_key(|(_, class)| class.len());
        let (i, class) = match winning_class {
            Some(class) => class,
            None => {
                self.state = Empty;
                return Ok(Empty);
            }
        };

        if class.len() >= self.quorum {
            self.state = Finished(i);
        } else if self.classes.len() == self.capacity {
            return Err(Error(i));
        } else {
            self.state = Pending(i);
        }
        Ok(self.state)
    }

    fn get_class(&mut self, message: &T) -> usize {
        if let Some(class) = self
            .classes
            .iter()
            .enumerate()
            .find(|(_i, m)| m[0].matches(message))
            .map(|m| m.0)
        {
            class
        } else {
            let new_class = self.classes.len();
            trace!("Creating new class: {}", new_class);
            self.classes.push(Vec::new());
            new_class
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod test {
    use crate::protocol::{
        quorum::{
            Error, Quorum,
            State::{Finished, Pending},
        },
        Match,
    };

    impl Match for &[u8] {
        fn matches(&self, other: &Self) -> bool {
            self[1..] == other[1..]
        }
    }

    const MSG1: &[u8] = &[0, 2, 3, 4, 5];
    const MSG2: &[u8] = &[1, 2, 3, 4, 5];
    const MSG3: &[u8] = &[2, 2, 3, 4, 5];

    const MSG4: &[u8] = &[3, 3, 3, 4, 5];
    const MSG5: &[u8] = &[4, 4, 3, 4, 5];

    #[test]
    fn test_pending() {
        let mut quorum = Quorum::new(2, 4);
        let state = quorum.offer(MSG1);

        assert_eq!(state.unwrap(), Pending(0));
    }

    #[test]
    fn test_finished() {
        let mut quorum = Quorum::new(2, 4);
        let _state = quorum.offer(MSG1);
        let state = quorum.offer(MSG2);

        assert_eq!(state.unwrap(), Finished(0));
    }

    #[test]
    fn test_different() {
        let mut quorum = Quorum::new(2, 4);
        let _state = quorum.offer(MSG1);
        let state = quorum.offer(MSG4);

        assert_eq!(state.unwrap(), Pending(1));
        assert_eq!(2, quorum.class_count());
    }

    #[test]
    fn test_different_complete() {
        let mut quorum = Quorum::new(2, 4);
        let _state = quorum.offer(MSG1);
        let _state = quorum.offer(MSG4);
        let state = quorum.offer(MSG2);

        assert_eq!(state.unwrap(), Finished(0));
        assert_eq!(2, quorum.class_count());
    }

    #[test]
    fn test_failure() {
        let mut quorum = Quorum::new(2, 3);
        let _state = quorum.offer(MSG1);
        let _state = quorum.offer(MSG4);
        let state = quorum.offer(MSG5);

        assert_eq!(state.unwrap_err(), Error(2));
        assert_eq!(3, quorum.class_count());
    }
}
