#![feature(type_alias_impl_trait)]

mod config;
mod de;
mod parser;
mod ser;
mod traversal;
mod value;

pub use config::{Config, MergeError};
pub use de::Deserializer;
pub use ser::Serializer;
pub use value::Value;

#[cfg(test)]
mod tests {
    use crate::{
        de::Deserializer,
        ser::{self, Serializer},
        value::Value,
    };
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[test]
    fn primitives() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Primitives {
            int: u64,
            float: f64,
            string: String,
            bool: bool,
        }

        let foo = dbg!(Primitives {
            int: 5,
            float: 6.7,
            string: "foo".to_owned(),
            bool: true,
        });

        let value: Value = foo.serialize(Serializer).expect("ser");
        let mut table = HashMap::new();
        table.insert("int".to_string(), Value::Integer(5));
        table.insert("float".to_string(), Value::Float(6.7));
        table.insert("string".to_string(), Value::String("foo".to_owned()));
        table.insert("bool".to_string(), Value::Boolean(true));
        assert_eq!(value, Value::Table(table));

        let foo2 = Primitives::deserialize(Deserializer(&value)).expect("de");

        assert_eq!(foo2, foo);
    }

    #[test]
    fn enum_units() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        enum EnumUnits {
            Bar,
            Baz,
        }

        let foo = EnumUnits::Bar;
        let value: Value = foo.serialize(Serializer).expect("ser");

        assert_eq!(value, Value::String("Bar".to_owned()));

        let foo2 = EnumUnits::deserialize(Deserializer(&value)).expect("de");

        assert_eq!(foo, foo2);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum EnumTupleStruct {
        Bar(i64),
        Baz { foo: i64 },
    }

    #[test]
    fn enum_tuple() {
        let foo = EnumTupleStruct::Bar(1);
        assert_eq!(
            foo.serialize(Serializer),
            Err(ser::Error::UnsupportedType("enum"))
        );
    }

    #[test]
    fn enum_struct() {
        let foo = EnumTupleStruct::Baz { foo: 1 };
        assert_eq!(
            foo.serialize(Serializer),
            Err(ser::Error::UnsupportedType("enum"))
        );
    }

    #[test]
    fn array() {
        let arr: Vec<i64> = vec![1, 2, 3, 4, 5, 6];

        let value = arr.serialize(Serializer).expect("ser");

        assert_eq!(
            value,
            Value::Array(arr.iter().map(|i| Value::Integer(*i)).collect())
        );

        let arr2: Vec<i64> = Vec::deserialize(Deserializer(&value)).expect("de");

        assert_eq!(arr, arr2);
    }

    #[test]
    fn table() {
        let mut table = HashMap::new();
        table.insert("foo".to_owned(), 5);
        table.insert("bar".to_owned(), 7);

        let value = table.serialize(Serializer).expect("ser");

        assert_eq!(
            value,
            Value::Table(
                table
                    .iter()
                    .map(|(key, value)| (key.to_owned(), Value::Integer(*value)))
                    .collect()
            )
        );

        let table2 = HashMap::deserialize(Deserializer(&value)).expect("de");

        assert_eq!(table, table2);
    }

    #[test]
    fn table_not_string_key() {
        let mut table = HashMap::new();
        table.insert(1, 5);
        table.insert(2, 7);

        assert_eq!(
            table.serialize(Serializer),
            Err(ser::Error::UnsupportedMapKey("&i32"))
        );
    }

    #[test]
    fn option_some() {
        let opt = Some("foo");

        let value = opt.serialize(Serializer).expect("ser");

        assert_eq!(value, Value::String("foo".to_string()));

        let opt2 = Option::<&str>::deserialize(Deserializer(&value)).expect("de");

        assert_eq!(opt, opt2);
    }

    #[test]
    fn newtype() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Foo(i64);
        let nt = Foo(1);

        let value = nt.serialize(Serializer).expect("ser");
        assert_eq!(value, Value::Integer(1));

        let nt2 = Foo::deserialize(Deserializer(&value)).expect("de");

        assert_eq!(nt, nt2);
    }

    #[test]
    fn tuple_struct() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Foo(i64, f64);
        let ts = Foo(1, 1.1);

        let value = ts.serialize(Serializer).expect("ser");
        assert_eq!(
            value,
            Value::Array(vec![Value::Integer(1), Value::Float(1.1)])
        );

        let ts2 = Foo::deserialize(Deserializer(&value)).expect("de");

        assert_eq!(ts, ts2);
    }

    #[test]
    fn tuple() {
        let ts = (1, 1.1);

        let value = ts.serialize(Serializer).expect("ser");
        assert_eq!(
            value,
            Value::Array(vec![Value::Integer(1), Value::Float(1.1)])
        );

        let ts2 = <(i32, f64)>::deserialize(Deserializer(&value)).expect("de");

        assert_eq!(ts, ts2);
    }

    #[test]
    fn char() {
        let char = 'x';

        let value = char.serialize(Serializer).expect("ser");
        assert_eq!(value, Value::String(char.to_string()));

        let char2 = char::deserialize(Deserializer(&value)).expect("de");

        assert_eq!(char, char2);
    }

    #[test]
    fn char_from_bad_string() {
        let value = Value::String("xy".to_string());

        let result = char::deserialize(Deserializer(&value));

        assert!(result.is_err())
    }

    #[test]
    fn try_from_into() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Primitives {
            int: u64,
            float: f64,
            string: String,
            bool: bool,
        }

        let foo = dbg!(Primitives {
            int: 5,
            float: 6.7,
            string: "foo".to_owned(),
            bool: true,
        });

        let value: Value = Value::try_from(&foo).expect("ser");
        let mut table = HashMap::new();
        table.insert("int".to_string(), Value::Integer(5));
        table.insert("float".to_string(), Value::Float(6.7));
        table.insert("string".to_string(), Value::String("foo".to_owned()));
        table.insert("bool".to_string(), Value::Boolean(true));
        assert_eq!(value, Value::Table(table));

        let foo2: Primitives = value.try_into_deserializable().expect("de");

        assert_eq!(foo2, foo);
    }
}
