//use std::collections::VecDeque;
//use std::fmt;
//use std::fmt::Display;
//use std::fmt::Formatter;
//
//use crate::protocol::quorum::Quorum;
//use crate::protocol::quorum::State;
//use std::error::Error;

//struct Window<T>
//where
//    T: Voteable,
//{
//    low_mark: T::SeqNum,
//    capacity: T::SeqNum,
//    messages: VecDeque<Quorum<T>>,
//}
//
//impl<T> Window<T>
//where
//    T: Voteable,
//{
//    fn new(capacity: T::SeqNum) -> Self {
//        Self {
//            low_mark: zero(),
//            capacity,
//            messages: VecDeque::with_capacity(capacity.as_()),
//        }
//    }
//
//    fn high_mark(&self) -> T::SeqNum {
//        self.low_mark + self.capacity
//    }
//
//    fn offer(&mut self, message: T) -> Result<State, WindowError> {
//        if message.seq_num() >= self.high_mark() || message.seq_num() < self.low_mark {
//            return Err(WindowError {});
//        }
//        let _quorum = &mut self.messages[0];
//        unimplemented!()
//    }
//}
//
//#[derive(Debug, PartialEq)]
//struct WindowError;
//
//impl Display for WindowError {
//    fn fmt(&self, _f: &mut Formatter) -> fmt::Result {
//        unimplemented!()
//    }
//}
//
//impl Error for WindowError {}
