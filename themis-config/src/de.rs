use std::fmt::{self, Display, Formatter};

use serde::{
    de::{
        self,
        value::{MapDeserializer, SeqDeserializer},
        IntoDeserializer, Visitor,
    },
    forward_to_deserialize_any,
};

use crate::value::Value;

#[derive(Debug, Clone, Copy)]
pub struct Deserializer<'de>(pub &'de Value);

impl<'de> de::Deserializer<'de> for Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Value::String(s) => visitor.visit_borrowed_str(s),
            Value::Null => visitor.visit_none(),
            Value::Float(f) => visitor.visit_f64(*f),
            Value::Integer(i) => visitor.visit_i64(*i),
            Value::Boolean(b) => visitor.visit_bool(*b),
            Value::Array(arr) => {
                let de = SeqDeserializer::new(arr.iter());
                visitor.visit_seq(de)
            }
            Value::Table(table) => {
                let de = MapDeserializer::new(table.iter().map(|(key, v)| (key.as_str(), v)));
                visitor.visit_map(de)
            }
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit_struct seq tuple
        tuple_struct map struct identifier ignored_any
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<<V as Visitor<'de>>::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.0 {
            Value::String(s) => visitor.visit_enum(s.as_str().into_deserializer()),
            _ => Err(Error::TypeError("string")),
        }
    }
}

impl<'de> IntoDeserializer<'de, Error> for &'de Value {
    type Deserializer = Deserializer<'de>;

    fn into_deserializer(self) -> Self::Deserializer {
        Deserializer(self)
    }
}

#[derive(Debug, PartialEq)]
pub enum Error {
    Custom(String),
    TypeError(&'static str),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::Custom(s) => f.write_str(s),
            Error::TypeError(s) => write!(f, "TypeError: expected {}", s),
        }
    }
}

impl std::error::Error for Error {}

impl de::Error for Error {
    fn custom<T>(v: T) -> Self
    where
        T: Display,
    {
        use std::fmt::Write;
        let mut s = String::new();
        write!(s, "{}", v).unwrap();
        Self::Custom(s)
    }
}
