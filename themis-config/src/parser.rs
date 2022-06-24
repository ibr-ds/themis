use std::{
    fmt::{self, Display, Formatter},
    iter::once,
};

use nom::{
    branch::Alt,
    character::complete::{char, digit1},
    combinator::{iterator, map_opt, ParserIterator},
    error::ErrorKind,
    sequence::{preceded, terminated},
    AsChar, IResult, InputTakeAtPosition, ParseTo,
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Expr<'a> {
    Ident(&'a str),
    Index(usize),
}

fn subscript(input: &str) -> IResult<&str, Expr<'_>> {
    const LBRACKET: char = '[';
    const RBRACKET: char = ']';

    map_opt(
        preceded(char(LBRACKET), terminated(digit1, char(RBRACKET))),
        |index: &str| index.parse_to().map(Expr::Index),
    )(input)
}

const DASH: char = '-';
const UNDERSCORE: char = '_';

fn is_dash(input: char) -> bool {
    input == DASH
}

fn is_underscore(input: char) -> bool {
    input == UNDERSCORE
}

fn identifier(input: &str) -> IResult<&str, Expr<'_>> {
    input
        .split_at_position1_complete(
            |input| !(input.is_alphanum() || is_dash(input) || is_underscore(input)),
            ErrorKind::AlphaNumeric,
        )
        .map(|(s, e)| (s, Expr::Ident(e)))
}

fn identifier_child(input: &str) -> IResult<&str, Expr<'_>> {
    const SEPARATOR: char = '.';

    preceded(char(SEPARATOR), identifier)(input)
}

fn expression(input: &str) -> IResult<&str, Expr<'_>> {
    (subscript, identifier_child).choice(input)
}

pub fn parse(input: &str) -> PathIter<'_> {
    match identifier(input) {
        Ok((rest, expr)) => {
            let iter = once(Ok(expr)).chain(Iter(Some(iterator(rest, expression))));
            Either::Left(iter)
        }
        Err(e) => Either::Right(once(Err(e.into()))),
    }
}

pub type PathIter<'a> = impl Iterator<Item = Result<'a, Expr<'a>>>;

struct Iter<'a, F>(Option<ParserIterator<&'a str, nom::error::Error<&'a str>, F>>);

impl<'a, F> Iterator for Iter<'a, F>
where
    F: Fn(&'a str) -> IResult<&'a str, Expr<'a>, nom::error::Error<&'a str>>,
{
    type Item = Result<'a, Expr<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.as_mut().map(|ref mut i| i.next()) {
            Some(Some(x)) => Some(Ok(x)),
            Some(None) => match self.0.take().unwrap().finish() {
                Err(e) => Some(Err(Error::BadPathExpr { source: e })),
                Ok((rest, _)) if !rest.is_empty() => Some(Err(Error::NotConsumed { rest })),
                _ => None,
            },
            None => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Error<'a> {
    BadPathExpr {
        source: nom::Err<nom::error::Error<&'a str>>,
    },
    NotConsumed {
        rest: &'a str,
    },
}

impl Display for Error<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::BadPathExpr { source } => {
                write!(f, "Expression is not a valid path: {:?}", source)
            }
            Error::NotConsumed { rest } => {
                write!(f, "{} could not be parsed as path expression.", rest)
            }
        }
    }
}

impl<'a> From<nom::Err<nom::error::Error<&'a str>>> for Error<'a> {
    fn from(e: nom::Err<nom::error::Error<&'a str>>) -> Self {
        Error::BadPathExpr { source: e }
    }
}

type Result<'a, T> = std::result::Result<T, Error<'a>>;

enum Either<Left, Right> {
    Left(Left),
    Right(Right),
}

impl<Left, Right, T> Iterator for Either<Left, Right>
where
    Left: Iterator<Item = T>,
    Right: Iterator<Item = T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Left(i) => i.next(),
            Self::Right(i) => i.next(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::parser::{identifier, identifier_child, parse, subscript, Expr};

    #[test]
    fn ident() {
        let path = identifier("foo-_");
        assert_eq!(path, Ok(("", Expr::Ident("foo-_"))));
    }

    #[test]
    fn ident_child() {
        let path = identifier_child(".foo-_");
        assert_eq!(path, Ok(("", Expr::Ident("foo-_"))));
    }

    #[test]
    fn subscr() {
        let path = subscript("[123]");
        assert_eq!(path, Ok(("", Expr::Index(123))));
    }

    #[test]
    fn subscr_open() {
        let path = dbg!(subscript("[123"));
        assert!(path.is_err());
    }

    #[test]
    fn subscr_closed() {
        let path = dbg!(subscript("123]"));
        assert!(path.is_err());
    }

    #[test]
    fn ident_iter() {
        let mut path = parse("foo");
        assert_eq!(path.next(), Some(Ok(Expr::Ident("foo"))));
        assert_eq!(path.next(), None);
    }

    #[test]
    fn subscript_iter() {
        let mut path = parse("foo[12]");
        assert_eq!(path.next(), Some(Ok(Expr::Ident("foo"))));
        assert_eq!(path.next(), Some(Ok(Expr::Index(12))));
        assert_eq!(path.next(), None);
    }

    #[test]
    fn subscript_twice_iter() {
        let mut path = parse("foo[12][1]");
        assert_eq!(path.next(), Some(Ok(Expr::Ident("foo"))));
        assert_eq!(path.next(), Some(Ok(Expr::Index(12))));
        assert_eq!(path.next(), Some(Ok(Expr::Index(1))));
        assert_eq!(path.next(), None);
    }

    #[test]
    fn child_iter() {
        let mut path = parse("foo.bar");
        assert_eq!(path.next(), Some(Ok(Expr::Ident("foo"))));
        assert_eq!(path.next(), Some(Ok(Expr::Ident("bar"))));
        assert_eq!(path.next(), None);
    }

    #[test]
    fn child_subscript_iter() {
        let mut path = parse("foo.bar[12]");
        assert_eq!(path.next(), Some(Ok(Expr::Ident("foo"))));
        assert_eq!(path.next(), Some(Ok(Expr::Ident("bar"))));
        assert_eq!(path.next(), Some(Ok(Expr::Index(12))));
        assert_eq!(path.next(), None);
    }

    #[test]
    fn subscript_child_iter() {
        let mut path = parse("foo[12].bar");
        assert_eq!(path.next(), Some(Ok(Expr::Ident("foo"))));
        assert_eq!(path.next(), Some(Ok(Expr::Index(12))));
        assert_eq!(path.next(), Some(Ok(Expr::Ident("bar"))));
        assert_eq!(path.next(), None);
    }

    #[test]
    fn ident_iter_error() {
        let mut path = parse("foo[");
        assert_eq!(path.next(), Some(Ok(Expr::Ident("foo"))));
        assert!(path.next().map_or(false, |r| r.is_err()));
        assert_eq!(path.next(), None);
    }

    #[test]
    fn child_start() {
        let mut path = parse(".foo");
        assert!(path.next().map_or(false, |r| r.is_err()));
        assert_eq!(path.next(), None);
    }
}
