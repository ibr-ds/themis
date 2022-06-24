use std::{
    collections::HashMap,
    convert::TryInto,
    fmt::{self, Display, Formatter},
};

use serde::{
    de::{Deserialize, Deserializer, Error, MapAccess, SeqAccess, Unexpected, Visitor},
    ser::{SerializeMap, SerializeSeq},
    Serialize, Serializer,
};

use crate::{de, ser};
use std::collections::hash_map::Entry;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Table(Table),
    Array(Array),
}

pub type Array = Vec<Value>;
pub type Table = HashMap<String, Value>;

impl Value {
    pub fn try_into_deserializable<'de, D>(&'de self) -> Result<D, de::Error>
    where
        D: Deserialize<'de>,
    {
        let de = de::Deserializer(self);
        D::deserialize(de)
    }

    pub fn try_from<S>(value: &S) -> Result<Self, ser::Error>
    where
        S: Serialize,
    {
        value.serialize(ser::Serializer)
    }

    pub fn from_deserializer<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer)
    }

    /// Merges other into self.
    /// Tables and Arrays will be merged by item,
    /// When types differ, self will be overwritten with other
    pub fn merge(&mut self, other: Value) {
        match other {
            Value::Table(t) => self.merge_table(t),
            Value::Array(a) => self.merge_array(a),
            value => *self = value,
        }
    }

    fn merge_array(&mut self, other: Vec<Value>) {
        match self {
            Value::Array(a) => {
                let mut iter = other.into_iter();
                let self_items = a.len();
                for (self_item, other_item) in a.iter_mut().zip(&mut iter).take(self_items) {
                    self_item.merge(other_item);
                }
                for leftover in iter {
                    a.push(leftover)
                }
            }
            value => *value = Value::Array(other),
        }
    }

    fn merge_table(&mut self, other: HashMap<String, Value>) {
        match self {
            Value::Table(t) => {
                for (key, value) in other {
                    match t.entry(key) {
                        Entry::Occupied(mut o) => o.get_mut().merge(value),
                        Entry::Vacant(v) => {
                            v.insert(value);
                        }
                    }
                }
            }
            value => *value = Value::Table(other),
        }
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(m: HashMap<String, Value>) -> Self {
        Self::Table(m)
    }
}

impl From<Vec<Value>> for Value {
    fn from(m: Vec<Value>) -> Self {
        Self::Array(m)
    }
}

impl From<String> for Value {
    fn from(m: String) -> Self {
        Self::String(m)
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::String(ref value) => write!(f, "{}", value),
            Value::Boolean(value) => write!(f, "{}", value),
            Value::Integer(value) => write!(f, "{}", value),
            Value::Float(value) => write!(f, "{}", value),
            Value::Null => write!(f, "nil"),

            // TODO: Figure out a nice Display for these
            Value::Table(ref table) => write!(f, "{:?}", table),
            Value::Array(ref array) => write!(f, "{:?}", array),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                write!(formatter, "a valid config value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Boolean(v))
            }

            fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Integer(v.try_into().unwrap()))
            }

            fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Integer(v.try_into().unwrap()))
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Integer(v.try_into().unwrap()))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Integer(v))
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Integer(v.try_into().unwrap()))
            }

            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Integer(v.try_into().unwrap()))
            }

            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Integer(v.try_into().unwrap()))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Integer(v.try_into().map_err(|_| {
                    E::invalid_value(Unexpected::Unsigned(v), &"i64")
                })?))
            }

            fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Float(v.try_into().unwrap()))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Float(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::String(v.to_owned()))
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::String(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::String(v))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::Value::deserialize(deserializer)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Null)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut vec = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(element) = seq.next_element()? {
                    vec.push(element);
                }
                Ok(Value::Array(vec))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut table = HashMap::with_capacity(map.size_hint().unwrap_or(0));
                while let Some((key, value)) = map.next_entry()? {
                    table.insert(key, value);
                }
                Ok(Value::Table(table))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Value::Null => serializer.serialize_none(),
            Value::Array(array) => {
                let mut seq = serializer.serialize_seq(Some(array.len()))?;
                for e in array {
                    seq.serialize_element(e)?;
                }
                seq.end()
            }
            Value::Table(table) => {
                let mut seq = serializer.serialize_map(Some(table.len()))?;
                for (k, v) in table {
                    seq.serialize_entry(k, v)?;
                }
                seq.end()
            }
            Value::Integer(int) => serializer.serialize_i64(*int),
            Value::Float(float) => serializer.serialize_f64(*float),
            Value::String(string) => serializer.serialize_str(string),
            Value::Boolean(bool) => serializer.serialize_bool(*bool),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use serde::Deserialize;

    use crate::value::Value;
    use std::mem::size_of;

    #[test]
    fn de_int() {
        let t = toml::toml! {
            foo = 1
        };

        let v = Value::deserialize(t).expect("de");

        assert_eq!(
            {
                let mut m = HashMap::new();
                m.insert("foo".to_string(), Value::Integer(1));
                Value::Table(m)
            },
            v
        );
    }

    #[test]
    fn de_float() {
        let t = toml::toml! {
            foo = 1.1
        };

        let v = Value::deserialize(t).expect("de");

        assert_eq!(
            {
                let mut m = HashMap::new();
                m.insert("foo".to_string(), Value::Float(1.1));
                Value::Table(m)
            },
            v
        );
    }

    #[test]
    fn de_string() {
        let t = toml::toml! {
            foo = "bar"
        };

        let v = Value::deserialize(t).expect("de");

        assert_eq!(
            {
                let mut m = HashMap::new();
                m.insert("foo".to_string(), Value::String("bar".to_string()));
                Value::Table(m)
            },
            v
        );
    }

    #[test]
    fn de_array() {
        let t = toml::toml! {
            foo = [1, 2]
        };

        let v = Value::deserialize(t).expect("de");

        assert_eq!(
            {
                let mut m = HashMap::new();
                m.insert(
                    "foo".to_string(),
                    Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
                );
                Value::Table(m)
            },
            v
        );
    }

    #[test]
    fn de_map() {
        let t = toml::toml! {
            [foo]
            bar = 1
            baz = 2
        };

        let v = Value::deserialize(t).expect("de");

        assert_eq!(
            {
                let mut v = HashMap::new();
                let mut foo = HashMap::new();
                foo.insert("bar".to_string(), Value::Integer(1));
                foo.insert("baz".to_string(), Value::Integer(2));
                v.insert("foo".to_string(), Value::Table(foo));
                Value::Table(v)
            },
            v
        );
    }

    #[test]
    fn size() {
        dbg!(size_of::<Value>());
        dbg!(size_of::<super::Array>());
        dbg!(size_of::<super::Table>());
        dbg!(size_of::<Option<Value>>());
    }
}
