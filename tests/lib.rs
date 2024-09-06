use async_memcached::{Client, Error, Status};

async fn setup_client(keys: Vec<&str>) -> Client {
    let mut client = Client::new("tcp://127.0.0.1:11211")
        .await
        .expect("Failed to connect to server");

    for key in keys {
        client
            .delete_no_reply(key)
            .await
            .expect("Failed to delete key");
    }

    client
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_add_with_string_value() {
    let key = "async-memcache-test-key-add";

    let mut client = setup_client(vec![key]).await;

    let result = client.delete_no_reply(key).await;
    assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);

    let result = client.add(key, "value", None, None).await;

    assert!(result.is_ok());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_add_with_u64_value() {
    let key = "async-memcache-test-key-add-u64";

    let mut client = setup_client(vec![key]).await;
    let value: u64 = 10;

    let result = client.delete_no_reply(key).await;
    assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);

    let result = client.add(key, value, None, None).await;

    assert!(result.is_ok());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_add_with_a_key_that_already_exists() {
    let key = "async-memcache-test-key-add";

    let mut client = setup_client(vec![key]).await;

    client
        .set(key, "value", None, None)
        .await
        .expect("failed to set");

    let result = client.add(key, "value", None, None).await;

    assert_eq!(result, Err(Error::Protocol(Status::NotStored)));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_set_with_string_value() {
    let key = "set-key-with-string-value";

    let mut client = setup_client(vec![key]).await;

    let value = "value";
    let result = client.set(key, value, None, None).await;

    assert!(result.is_ok());

    let result = client.get(key).await;

    assert!(result.is_ok());

    let get_result = result.unwrap();

    assert_eq!(String::from_utf8(get_result.unwrap().data).unwrap(), value);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_set_with_string_ref_value() {
    let key = "set-key-with-string-reference-value";

    let mut client = setup_client(vec![key]).await;

    let value = String::from("value");
    let result = client.set(key, &value, None, None).await;

    assert!(result.is_ok());

    let result = client.get(key).await;

    assert!(result.is_ok());

    let get_result = result.unwrap();

    assert_eq!(String::from_utf8(get_result.unwrap().data).unwrap(), value);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_set_with_u64_value() {
    let key = "set-key-with-u64-value";

    let mut client = setup_client(vec![key]).await;

    let value: u64 = 20;

    let result = client.set(key, value, None, None).await;

    println!("result: {:?}", result);
    assert!(result.is_ok());

    let result = client.get(key).await;

    assert!(result.is_ok());

    let get_result = result.unwrap();

    println!("get_result: {:?}", get_result);

    println!(
        "get_result.unwrap().data: {:?}",
        String::from_utf8(get_result.unwrap().data).unwrap()
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_delete() {
    let key = "async-memcache-test-key-delete";

    let mut client = setup_client(vec![key]).await;

    let value = rand::random::<u64>();
    let result = client.set(key, value, None, None).await;

    assert!(result.is_ok(), "failed to set {}, {:?}", key, result);

    let result = client.get(key).await;

    assert!(result.is_ok(), "failed to get {}, {:?}", key, result);

    let get_result = result.unwrap();

    match get_result {
        Some(get_value) => assert_eq!(
            String::from_utf8(get_value.data).expect("failed to parse a string"),
            value.to_string()
        ),
        None => panic!("failed to get {}", key),
    }

    let result = client.delete(key).await;

    assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_delete_no_reply() {
    let key = "async-memcache-test-key-delete-no-reply";

    let mut client = setup_client(vec![key]).await;

    let value = format!("{}", rand::random::<u64>());
    let result = client.set(key, value.as_str(), None, None).await;

    assert!(result.is_ok(), "failed to set {}, {:?}", key, result);

    let result = client.get(key).await;

    assert!(result.is_ok(), "failed to get {}, {:?}", key, result);

    let get_result = result.unwrap();

    match get_result {
        Some(get_value) => assert_eq!(
            String::from_utf8(get_value.data).expect("failed to parse a string"),
            value
        ),
        None => panic!("failed to get {}", key),
    }

    let result = client.delete_no_reply(key).await;

    assert!(result.is_ok(), "failed to delete {}, {:?}", key, result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_set_multi_with_same_number_of_keys_and_values() {
    let keys = vec!["key1", "key2", "key3"];
    let values = vec!["value1", "value2", "value3"];

    let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values.into_iter()).collect();

    let mut client = setup_client(keys).await;

    let response = client.set_multi(kv, None, None).await;

    assert!(response.is_ok());

    let result = client.get("key2").await;

    assert!(result.is_ok());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_increment_raises_error_when_key_doesnt_exist() {
    let key = "key-does-not-exist";

    let mut client = setup_client(vec![key]).await;

    let amount = 1;

    let result = client.increment(key, amount).await;

    assert!(matches!(result, Err(Error::Protocol(Status::NotFound))));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_increments_existing_key() {
    let key = "key-to-increment";

    let mut client = setup_client(vec![key]).await;

    let value: u64 = 1;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.increment(key, amount).await;

    assert_eq!(Ok(2), result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_increment_can_overflow() {
    let key = "key-to-increment-overflow";

    let mut client = setup_client(vec![key]).await;

    let value = u64::MAX; // max value for u64

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    // First increment should overflow
    let result = client.increment(key, amount).await;

    assert_eq!(Ok(0), result);

    // Subsequent increments should work as normal
    let result = client.increment(key, amount).await;

    assert_eq!(Ok(1), result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_increments_existing_key_with_no_reply() {
    let key = "key-to-increment-no-reply";

    let mut client = setup_client(vec![key]).await;

    let value: u64 = 1;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.increment_no_reply(key, amount).await;

    assert_eq!(Ok(()), result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_decrement_raises_error_when_key_doesnt_exist() {
    let key = "fails-to-decrement";

    let mut client = setup_client(vec![key]).await;

    let amount = 1;

    let result = client.decrement(key, amount).await;

    assert!(matches!(result, Err(Error::Protocol(Status::NotFound))));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_decrements_existing_key() {
    let key = "key-to-decrement";

    let mut client = setup_client(vec![key]).await;

    let value: u64 = 10;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.decrement(key, amount).await;

    assert_eq!(Ok(9), result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_decrement_does_not_reduce_value_below_zero() {
    let key = "key-to-decrement-past-zero";

    let mut client = setup_client(vec![key]).await;

    let value: u64 = 0;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.decrement(key, amount).await;

    assert_eq!(Ok(0), result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_decrements_existing_key_with_no_reply() {
    let key = "key-to-decrement-no-reply";

    let mut client = setup_client(vec![key]).await;

    let value: u64 = 1;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.decrement_no_reply(key, amount).await;

    assert_eq!(Ok(()), result);
}
