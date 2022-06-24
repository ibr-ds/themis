use std::{collections::HashMap, fmt};

use serde::{Deserialize, Deserializer, Serialize};

use crate::{de, ser, traversal, Value};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct Config {
    pub root: Value,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            root: Value::Table(HashMap::default()),
        }
    }
}

impl Config {
    pub fn get<'s, 'p, D>(&'s self, path: &'p str) -> Result<'p, D>
    where
        D: Deserialize<'s>,
    {
        let value = traversal::get(&self.root, path)?;
        let d = D::deserialize(de::Deserializer(value))?;

        Ok(d)
    }

    pub fn set<'p, S>(&mut self, path: &'p str, value: S) -> Result<'p, ()>
    where
        S: Serialize,
    {
        let value = value.serialize(ser::Serializer)?;
        traversal::insert(&mut self.root, path, value)?;

        Ok(())
    }

    pub fn merge_from_deserializer<'de, D>(&mut self, deserializer: D) -> MergeResult<D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer).map_err(MergeError::Deserializer)?;
        match value {
            Value::Table(_) => (),
            _ => return Err(MergeError::ExpectedTable),
        };
        self.root.merge(value);
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum MergeError<DError> {
    Deserializer(DError),
    ExpectedTable,
}

impl<DError> Display for MergeError<DError>
where
    DError: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MergeError::ExpectedTable => {
                write!(f, "Expected Deserializer Output to be Value::Table")
            }
            MergeError::Deserializer(e) => e.fmt(f),
        }
    }
}

type MergeResult<DError> = std::result::Result<(), MergeError<DError>>;

impl<DError> std::error::Error for MergeError<DError> where DError: std::error::Error {}

type Result<'a, T> = std::result::Result<T, Error<'a>>;

#[derive(Debug)]
pub enum Error<'a> {
    Path(traversal::Error<'a>),
    Deserialize(de::Error),
    Serialize(ser::Error),
    External(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl Display for Error<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::Path(e) => write!(f, "Invalid path expression: {}", e),
            Error::Deserialize(e) => write!(f, "Deserialization Error: {}", e),
            Error::Serialize(e) => write!(f, "Serialization Error: {}", e),
            Error::External(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error<'_> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Deserialize(e) => Some(e),
            Error::Serialize(e) => Some(e),
            _ => None,
        }
    }
}

impl<'a> From<traversal::Error<'a>> for Error<'a> {
    fn from(e: traversal::Error<'a>) -> Self {
        Error::Path(e)
    }
}

impl From<de::Error> for Error<'_> {
    fn from(e: de::Error) -> Self {
        Error::Deserialize(e)
    }
}

impl From<ser::Error> for Error<'_> {
    fn from(e: ser::Error) -> Self {
        Error::Serialize(e)
    }
}

#[cfg(test)]
mod test {
    use crate::Config;
    use serde::Deserialize;

    #[test]
    fn toml() {
        let toml = toml::toml! {
            [stuff]
            foo = 1
            bar = 2
        };
        let toml = toml::to_string(&toml).unwrap();
        let mut config = Config::default();
        let mut de = toml::Deserializer::new(&toml);
        config.merge_from_deserializer(&mut de).unwrap();

        #[derive(Debug, PartialEq, Deserialize)]
        struct Stuff {
            foo: u64,
            bar: u64,
        }

        assert_eq!(
            config.get::<Stuff>("stuff").unwrap(),
            Stuff { foo: 1, bar: 2 }
        );
    }
}
