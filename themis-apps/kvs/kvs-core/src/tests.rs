use crate::*;
use bytes::Bytes;

#[test]
fn test_set_get_delete() {
    let mut s = Store::new();
    s.set(
        "key".to_string(),
        Bytes::from("value".to_string().into_bytes()),
    )
    .unwrap();
    assert_eq!(s.get("key").unwrap().unwrap(), "value".as_bytes().to_vec());

    s.set(
        "key".to_string(),
        Bytes::from("different value".to_string().into_bytes()),
    )
    .unwrap();
    assert_eq!(
        s.get("key").unwrap().unwrap(),
        "different value".as_bytes().to_vec()
    );

    s.delete("key").unwrap();
    assert_eq!(s.get("key").unwrap(), None);
}

#[test]
fn test_export_import() {
    let mut s = Store::new();
    s.set(
        "key".to_string(),
        Bytes::from("value".to_string().into_bytes()),
    )
    .unwrap();
    let serialized_state = s.export_state();

    let mut new_s = Store::new();
    new_s.import_state(serialized_state).unwrap();
    assert_eq!(
        new_s.get("key").unwrap().unwrap(),
        "value".as_bytes().to_vec()
    );
}

#[test]
fn test_message_set_get_delete() {
    let mut s = Store::new();

    // set(key,value)
    let mut operation: Operation = Operation::Set {
        key: "key".to_string(),
        value: Bytes::from("value".to_string().into_bytes()),
    };
    let mut result: Bytes = s.execute(Bytes::from(rmp_serde::to_vec_named(&operation).unwrap()));
    let mut result_deserialized: Return = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(result_deserialized.unwrap(), None);

    // get(key)
    operation = Operation::Get {
        key: "key".to_string(),
    };
    result = s.execute(Bytes::from(rmp_serde::to_vec_named(&operation).unwrap()));
    result_deserialized = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(
        result_deserialized.unwrap().unwrap(),
        "value".as_bytes().to_vec()
    );
    // delete(key)
    operation = Operation::Delete {
        key: "key".to_string(),
    };
    result = s.execute(Bytes::from(rmp_serde::to_vec_named(&operation).unwrap()));
    result_deserialized = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(
        result_deserialized.unwrap().unwrap(),
        "value".as_bytes().to_vec()
    );

    // get(key)
    operation = Operation::Get {
        key: "key".to_string(),
    };
    result = s.execute(Bytes::from(rmp_serde::to_vec_named(&operation).unwrap()));
    result_deserialized = rmp_serde::from_slice(&result).unwrap();
    assert_eq!(result_deserialized.unwrap(), None);
}
