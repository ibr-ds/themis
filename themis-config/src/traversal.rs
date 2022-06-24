use crate::{
    parser::{self, parse, Expr},
    value::Value,
};

pub(crate) fn get<'s, 'v>(mut value: &'v Value, path: &'s str) -> Result<'s, &'v Value> {
    let path_iter = parse(path);

    for segment in path_iter {
        match (value, segment?) {
            (Value::Table(table), Expr::Ident(key)) => {
                value = table.get(key).ok_or(Error::KeyNotFound { path, key })?
            }
            (Value::Array(array), Expr::Index(index)) => {
                value = array.get(index).ok_or(Error::OutOfRange { path, index })?
            }
            (_, Expr::Ident(_ident)) => {
                //TODO spans?
                return Err(Error::ExpectedTable { path });
            }
            (_, Expr::Index(_index)) => {
                //TODO spans?
                return Err(Error::ExpectedArray { path });
            }
        }
    }
    Ok(value)
}

#[allow(unused)]
pub(crate) fn get_mut<'s, 'v>(
    mut value: &'v mut Value,
    path: &'s str,
) -> Result<'s, &'v mut Value> {
    let path_iter = parse(path);

    for segment in path_iter {
        match (value, segment?) {
            (Value::Table(table), Expr::Ident(key)) => {
                value = table.get_mut(key).ok_or(Error::KeyNotFound { path, key })?
            }
            (Value::Array(array), Expr::Index(index)) => {
                value = array
                    .get_mut(index)
                    .ok_or(Error::OutOfRange { path, index })?
            }
            (_, Expr::Ident(_ident)) => {
                //TODO spans?
                return Err(Error::ExpectedTable { path });
            }
            (_, Expr::Index(_index)) => {
                //TODO spans?
                return Err(Error::ExpectedArray { path });
            }
        }
    }
    Ok(value)
}

/// Will create tables and arrays (including Null elements) as necessary
pub(crate) fn insert<'s, 'v>(
    mut target: &'v mut Value,
    path: &'s str,
    to_insert: Value,
) -> Result<'s, ()> {
    let path_iter = parse(path);

    // TODO convert Nulls to Table or Array
    for segment in path_iter {
        match (target, segment?) {
            (Value::Table(table), Expr::Ident(key)) => {
                if !table.contains_key(key) {
                    table.insert(key.to_owned(), Value::default());
                }
                target = table.get_mut(key).unwrap();
            }
            (Value::Array(array), Expr::Index(index)) => {
                while array.len() <= index {
                    array.push(Value::default())
                }
                target = array.get_mut(index).unwrap();
            }
            (value @ Value::Null, Expr::Ident(key)) => {
                let mut map = HashMap::default();
                map.insert(key.to_owned(), Value::Null);
                *value = Value::Table(map);

                target = match value {
                    Value::Table(t) => t.get_mut(key).unwrap(),
                    _ => unreachable!(),
                }
            }
            (value @ Value::Null, Expr::Index(0)) => {
                let array = vec![Value::Null];
                *value = Value::Array(array);

                target = match value {
                    Value::Array(a) => a.first_mut().unwrap(),
                    _ => unreachable!(),
                }
            }
            (_, Expr::Ident(_ident)) => {
                //TODO spans?
                return Err(Error::ExpectedTable { path });
            }
            (_, Expr::Index(_index)) => {
                //TODO spans?
                return Err(Error::ExpectedArray { path });
            }
        }
    }

    *target = to_insert;

    Ok(())
}

#[derive(Debug, PartialEq)]
pub enum Error<'a> {
    BadPath { source: parser::Error<'a> },
    ExpectedTable { path: &'a str },
    ExpectedArray { path: &'a str },
    OutOfRange { path: &'a str, index: usize },
    KeyNotFound { path: &'a str, key: &'a str },
}

impl<'a> From<parser::Error<'a>> for Error<'a> {
    fn from(source: parser::Error<'a>) -> Self {
        Self::BadPath { source }
    }
}

use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
};

impl Display for Error<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::BadPath { source } => Display::fmt(source, f),
            Error::ExpectedTable { path } => write!(f, "Expected table at {}", path),
            Error::ExpectedArray { path } => write!(f, "Expected array at {}", path),
            Error::OutOfRange { path, index } => {
                write!(f, "Index {} out of range for array {}", index, path)
            }
            Error::KeyNotFound { path, key } => {
                write!(f, "Key {} not found  in table {}", key, path)
            }
        }
    }
}

impl std::error::Error for Error<'_> {}

type Result<'a, T> = std::result::Result<T, Error<'a>>;

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{traversal::insert, Value};

    use super::get;

    #[test]
    fn get_value() {
        let mut table = HashMap::<String, Value>::new();
        table.insert("foo".to_owned(), Value::try_from(&1).unwrap_or_default());
        table.insert("bar".to_owned(), Value::try_from(&1.0).unwrap_or_default());
        table.insert(
            "baz".to_owned(),
            Value::try_from(&"baz").unwrap_or_default(),
        );
        table.insert(
            "bool".to_owned(),
            Value::try_from(&true).unwrap_or_default(),
        );

        // copy all of this into a table
        table.insert(
            "foo_table".to_owned(),
            Value::try_from(&table).unwrap_or_default(),
        );

        let items: Value = Value::try_from(&vec![3, 4, 5]).unwrap();
        table.insert(
            "bar-array".to_owned(),
            Value::try_from(&items).unwrap_or_default(),
        );

        let value = Value::try_from(&table).unwrap();

        assert_eq!(get(&value, "foo"), Ok(&Value::Integer(1)));
        assert_eq!(get(&value, "bar"), Ok(&Value::Float(1.0)));
        assert_eq!(get(&value, "baz"), Ok(&Value::String("baz".to_owned())));
        assert_eq!(get(&value, "bool"), Ok(&Value::Boolean(true)));

        assert_eq!(get(&value, "foo_table.foo"), Ok(&Value::Integer(1)));
        assert_eq!(get(&value, "foo_table.bar"), Ok(&Value::Float(1.0)));
        assert_eq!(
            get(&value, "foo_table.baz"),
            Ok(&Value::String("baz".to_owned()))
        );
        assert_eq!(get(&value, "foo_table.bool"), Ok(&Value::Boolean(true)));

        assert_eq!(get(&value, "bar-array[0]"), Ok(&Value::Integer(3)));
        assert_eq!(get(&value, "bar-array[1]"), Ok(&Value::Integer(4)));
        assert_eq!(get(&value, "bar-array[2]"), Ok(&Value::Integer(5)));
    }

    #[test]
    fn insert_value() {
        let mut null = Value::Table(Default::default());

        insert(&mut null, "foo", Value::Integer(1)).unwrap();
    }
}
