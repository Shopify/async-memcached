use async_memcached::{Client, Error, Status};
use rand::seq::IteratorRandom;
use serial_test::{parallel, serial};

// Note: Each test should run with keys unique to that test to avoid async conflicts.  Because these tests run concurrently,
// it's possible to delete/overwrite keys created by another test before they're read.

const LARGE_PAYLOAD_SIZE: usize = 1000 * 1024;

async fn setup_client(keys: &[&str]) -> Client {
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
#[parallel]
async fn test_get_with_cached_key() {
    let key = "key-that-exists";
    let value = "value-that-exists";

    let mut client = setup_client(&[key]).await;

    let set_result = client.set(key, value, None, None).await;
    assert!(
        set_result.is_ok(),
        "failed to set {}, {:?}",
        key,
        set_result
    );

    let get_result = client.get(key).await;

    assert!(
        get_result.is_ok(),
        "failed to get {}, {:?}",
        key,
        get_result
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_get_with_nonexistent_key() {
    let key = "nonexistent-key";

    let mut client = setup_client(&[key]).await;

    let get_result = client.get(key).await;

    assert!(matches!(get_result, Ok(None)), "key should not be found");
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_add_with_string_value() {
    let key = "async-memcache-test-key-add-string";

    let mut client = setup_client(&[key]).await;

    let result = client.add(key, "value", None, None).await;

    assert!(result.is_ok(), "failed to add {}, {:?}", key, result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_add_with_u64_value() {
    let key = "async-memcache-test-key-add-u64";

    let mut client = setup_client(&[key]).await;
    let value: u64 = 10;

    let result = client.add(key, value, None, None).await;

    assert!(result.is_ok(), "failed to add {}, {:?}", key, result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_add_with_a_key_that_already_exists() {
    let key = "async-memcache-test-key-add-exists";

    let mut client = setup_client(&[key]).await;

    client
        .set(key, "value", None, None)
        .await
        .expect("failed to set");

    let add_result = client.add(key, "value", None, None).await;

    assert_eq!(add_result, Err(Error::Protocol(Status::NotStored)));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_add_multi() {
    let keys = vec!["am-key1", "am-key2", "afm-key3"];
    let values = vec!["value1", "value2", "value3"];
    let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values.into_iter()).collect();

    let mut client = setup_client(&keys).await;

    let result = client.add_multi(&kv, None, None).await;

    assert!(
        result.is_ok(),
        "failed to add_multi {:?}, {:?}",
        &keys,
        result
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_add_multi_with_a_key_that_already_exists() {
    let unset_key_0 = "am2-key1";
    let preset_key_1 = "am2-key-already-set";
    let unset_key_2 = "am2-key3";
    let keys = vec![unset_key_0, preset_key_1, unset_key_2];
    let values = vec!["original-value", "new_value", "value3"];
    let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values.into_iter()).collect();

    let mut client = setup_client(&keys).await;

    client
        .set(&preset_key_1, "original-value", None, None)
        .await
        .expect("failed to set");

    let add_response = client.add_multi(&kv, None, None).await;

    assert!(
        &add_response.is_ok(),
        "failed to add_multi {:?}, {:?}",
        &keys,
        &add_response
    );

    let results = add_response.expect("expected Ok(HashMap<_>)");

    for key in &keys {
        assert!(results.contains_key(key));
    }

    // the preset key should have an error associated with it in the HashMap of results, the unset keys should not
    assert!(results[&unset_key_0].is_ok());
    assert!(matches!(
        results[&preset_key_1],
        Err(Error::Protocol(Status::NotStored))
    ));
    assert!(results[&unset_key_2].is_ok());

    let get_result = client.get(preset_key_1).await;

    // the get result for the preset key should be the original value that it was set with
    // not the new value from the add_multi call
    assert_eq!(
        std::str::from_utf8(&get_result.unwrap().unwrap().data)
            .expect("failed to parse string from bytes"),
        "original-value"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_with_string_value() {
    let key = "set-key-with-str-value";

    let mut client = setup_client(&[key]).await;

    let value = "value";
    let set_result = client.set(key, value, None, None).await;

    assert!(
        set_result.is_ok(),
        "failed to set {}, {:?}",
        key,
        set_result
    );

    let get_result = client.get(key).await;

    assert!(get_result.is_ok());

    assert_eq!(
        String::from_utf8(
            get_result
                .expect("should have unwrapped a Result")
                .expect("should have unwrapped an Option")
                .data
        )
        .expect("failed to parse String from bytes"),
        value
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_with_string_ref_value() {
    let key = "set-key-with-String-reference-value";

    let mut client = setup_client(&[key]).await;

    let value = String::from("value");
    let set_result = client.set(key, &value, None, None).await;

    assert!(
        set_result.is_ok(),
        "failed to set {}, {:?}",
        key,
        set_result
    );

    let get_result = client.get(key).await;

    assert_eq!(
        String::from_utf8(
            get_result
                .expect("should have unwrapped a Result")
                .expect("should have unwrapped an Option")
                .data
        )
        .expect("failed to parse String from bytes"),
        value
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_with_u64_value() {
    let key = "set-key-with-u64-value";

    let mut client = setup_client(&[key]).await;

    let value: u64 = 20;

    let result = client.set(key, value, None, None).await;

    assert!(result.is_ok(), "failed to set {}, {:?}", key, result);

    let result = client.get(key).await;

    assert_eq!(
        value,
        btoi::btoi::<u64>(
            &result
                .expect("should have unwrapped a Result")
                .expect("should have unwrapped an Option")
                .data
        )
        .expect("couldn't parse data from bytes to integer")
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_fails_with_value_too_large() {
    let key = "too-large-set-key-with-str-value";

    let mut client = setup_client(&[key]).await;

    let value = "a".repeat(LARGE_PAYLOAD_SIZE * 2);
    let set_result = client.set(key, &value, None, None).await;

    println!("set_result: {:?}", set_result);

    assert!(
        set_result.is_err(),
        "failed to set {}, {:?}",
        key,
        set_result
    );

    let get_result = client.get(key).await;

    assert!(matches!(get_result, Ok(None)));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_get_multi() {
    let keys = vec!["mg-key1", "mg-key2", "mg-key3"];
    let values = vec!["value1", "value2", "value3"];

    let mut client = setup_client(&keys).await;

    for (key, value) in keys.iter().zip(values.iter()) {
        let result = client.set(*key, *value, None, None).await;
        assert!(result.is_ok(), "failed to set {}, {:?}", key, result);
    }

    let result = client.get_multi(&keys).await;

    assert!(
        result.is_ok(),
        "failed to get many {:?}, {:?}",
        keys,
        result
    );
    assert_eq!(result.unwrap().len(), keys.len());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_get_multi_with_nonexistent_key() {
    let mut keys = vec!["mgne-key1", "mgne-key2", "mgne-key3"];
    let values = vec!["value1", "value2", "value3"];

    let original_keys_length = keys.len();

    let mut client = setup_client(&keys).await;

    for (key, value) in keys.iter().zip(values.iter()) {
        let set_result = client.set(key, *value, None, None).await;
        assert!(
            set_result.is_ok(),
            "failed to set {}, {:?}",
            key,
            set_result
        );
    }

    let unset_key = "thisisakeythatisnotset";

    keys.push(unset_key);

    let results = client.get_multi(&keys).await.unwrap();

    assert_eq!(original_keys_length, results.len());

    for result in results {
        let key_str = std::str::from_utf8(&result.key)
            .expect("should have been able to parse Value.key as utf8");
        assert!(&keys.contains(&key_str));
    }
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_get_many_aliases_get_multi_properly() {
    let keys = vec!["get-many-key1", "get-many-key2", "get-many-key3"];
    let values = vec!["value1", "value2", "value3"];

    let mut client = setup_client(&keys).await;

    for (key, value) in keys.iter().zip(values.iter()) {
        let result = client.set(*key, *value, None, None).await;
        assert!(result.is_ok(), "failed to set {}, {:?}", key, result);
    }

    #[allow(deprecated)] // specifically testing deprecated method
    let result = client.get_many(&keys).await;

    assert!(
        result.is_ok(),
        "failed to get many {:?}, {:?}",
        keys,
        result
    );
    assert_eq!(result.unwrap().len(), keys.len());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_delete() {
    let key = "async-memcache-test-key-delete";

    let mut client = setup_client(&[key]).await;

    let value = rand::random::<u64>();
    let set_result = client.set(key, value, None, None).await;

    assert!(
        set_result.is_ok(),
        "failed to set {}, {:?}",
        key,
        set_result
    );

    let get_result = client.get(key).await;

    assert!(
        get_result.is_ok(),
        "failed to get {}, {:?}",
        key,
        get_result
    );

    let get_result = get_result.unwrap();

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
#[parallel]
async fn test_delete_no_reply() {
    let key = "async-memcache-test-key-delete-no-reply";

    let mut client = setup_client(&[key]).await;

    let value = format!("{}", rand::random::<u64>());
    let set_result = client.set(key, value.as_str(), None, None).await;

    assert!(
        set_result.is_ok(),
        "failed to set {}, {:?}",
        key,
        set_result
    );

    let get_result = client.get(key).await;

    assert!(
        get_result.is_ok(),
        "failed to get {}, {:?}",
        key,
        get_result
    );

    let get_result = get_result.unwrap();

    match get_result {
        Some(get_value) => assert_eq!(
            String::from_utf8(get_value.data).expect("failed to parse a string"),
            value
        ),
        None => panic!("failed to get {}", key),
    }

    let delete_result = client.delete_no_reply(key).await;

    assert!(
        delete_result.is_ok(),
        "failed to delete {}, {:?}",
        key,
        delete_result
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_delete_multi_no_reply() {
    let keys = vec!["dm-key1", "dm-key2", "dm-key3"];
    let values = vec!["value1", "value2", "value3"];
    let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values.into_iter()).collect();

    let mut client = setup_client(&keys).await;
    let _ = client.set_multi(&kv, None, None).await;

    let result = client.delete_multi_no_reply(&keys).await;

    assert!(
        result.is_ok(),
        "failed to delete_multi_no_reply {:?}, {:?}",
        keys,
        result
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_multi_with_string_values() {
    let keys = vec!["smwsv-key1", "smwsv-key2", "smwsv-key3"];
    let values = vec!["value1", "value2", "value3"];

    let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values.into_iter()).collect();

    let mut client = setup_client(&keys).await;

    let _ = client.set_multi(&kv, None, None).await;

    let result = client.get("smwsv-key2").await;

    assert!(matches!(
        std::str::from_utf8(
            &result
                .expect("should have unwrapped a Result")
                .expect("should have unwrapped an Option")
                .data
        )
        .expect("failed to parse string from bytes"),
        "value2"
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_multi_with_string_values_that_exceed_max_size() {
    const NUM_PAIRS: usize = 100;
    const LARGE_VALUE_SIZE: usize = 2_048_576;
    const NUM_LARGE_KEYS: usize = 5;

    let keys: Vec<String> = (0..NUM_PAIRS).map(|i| format!("multi-key{}", i)).collect();
    let mut values: Vec<String> = (0..NUM_PAIRS).map(|i| format!("value{}", i)).collect();

    let mut rng = rand::thread_rng();
    let large_key_indices: Vec<usize> = (1..NUM_PAIRS)
        .choose_multiple(&mut rng, NUM_LARGE_KEYS)
        .into_iter()
        .collect();

    let mut large_key_strs = Vec::new();
    for &index in &large_key_indices {
        values[index] = "a".repeat(LARGE_VALUE_SIZE);
        large_key_strs.push(keys[index].clone());
    }

    let kv: Vec<(&str, &str)> = keys
        .iter()
        .map(AsRef::as_ref)
        .zip(values.iter().map(AsRef::as_ref))
        .collect();

    let mut client = setup_client(&keys.iter().map(AsRef::as_ref).collect::<Vec<&str>>()).await;

    let set_result = client.set_multi(&kv, None, None).await;
    assert!(
        set_result.is_ok(),
        "Failed to set multiple values: {:?}",
        set_result
    );

    let result_map = set_result.unwrap();

    for (key, value) in &result_map {
        println!("key: {}, value: {:?}", key, value);
    }

    // The randomized large keys should have errors due to large values
    for large_key in large_key_strs.clone() {
        assert!(
            result_map.contains_key(&large_key.as_str()),
            "{} should be in the result map",
            large_key
        );
        assert!(
            result_map[&large_key.as_str()].is_err(),
            "{} should have an error",
            large_key
        );
    }

    // Check a small value to make sure it was cached properly - key0 is never chosen to be a large value
    let small_result = client.get("multi-key0").await;
    assert!(matches!(
        std::str::from_utf8(&small_result.unwrap().unwrap().data)
            .expect("failed to parse string from bytes"),
        "value0"
    ));

    // Check that the large values were not cached(should not exist due to error)
    for large_key in large_key_strs {
        let large_result = client.get(large_key.as_str()).await;
        assert!(
            large_result.unwrap().is_none(),
            "Large value {} should not have been set",
            large_key
        );
    }

    // Check other keys were set successfully
    for i in 0..NUM_PAIRS {
        let key = format!("multi-key{}", i);
        if !large_key_indices.contains(&i) {
            assert!(
                result_map[&key.as_str()].is_ok(),
                "Key {} should be in the hashmap",
                key
            );
            let get_result = client.get(key.as_str()).await.unwrap().unwrap();
            assert_eq!(
                std::str::from_utf8(&get_result.data).unwrap(),
                format!("value{}", i),
                "Mismatch for key {}",
                key
            );
        } else {
            assert!(
                result_map.contains_key(&key.as_str()),
                "Key {} should be in the hashmap",
                key
            );
            assert!(
                result_map[&key.as_str()].is_err(),
                "Key {} should have an error",
                key
            );
            let get_result = client.get(key.as_str()).await.unwrap();
            assert!(
                get_result.is_none(),
                "Large value for key {} should not have been set",
                key
            );
        }
    }
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_multi_with_large_string_values() {
    let large_string = "a".repeat(LARGE_PAYLOAD_SIZE);

    let mut keys = Vec::with_capacity(100);
    let key_strings: Vec<String> = (0..100).map(|i| format!("key{}", i)).collect();
    keys.extend(key_strings.iter().map(|s| s.as_str()));
    let values = vec![large_string.as_str(); 100];

    let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values.into_iter()).collect();

    let mut client = setup_client(&keys).await;

    let _ = client.set_multi(&kv, None, None).await;

    let get_result = client.get("key2").await;

    assert!(get_result.is_ok());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_increment_raises_error_when_key_doesnt_exist() {
    let key = "key-does-not-exist";

    let mut client = setup_client(&[key]).await;

    let amount = 1;

    let result = client.increment(key, amount).await;

    assert!(matches!(result, Err(Error::Protocol(Status::NotFound))));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_increments_existing_key() {
    let key = "u64-key-to-increment";

    let mut client = setup_client(&[key]).await;

    let value: u64 = 1;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.increment(key, amount).await;

    assert_eq!(Ok(2), result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_increment_on_non_numeric_value() {
    let key = "NaN-key-to-increment";

    let mut client = setup_client(&[key]).await;

    let value: &str = "not-a-number";

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.increment(key, amount).await;

    assert!(matches!(
        result,
        Err(Error::Protocol(Status::Error(
            async_memcached::ErrorKind::Client(_)
        )))
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_increment_can_overflow() {
    let key = "key-to-increment-overflow";

    let mut client = setup_client(&[key]).await;

    let value = u64::MAX;

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
#[parallel]
async fn test_increments_existing_key_with_no_reply() {
    let key = "key-to-increment-no-reply";

    let mut client = setup_client(&[key]).await;

    let value: u64 = 1;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.increment_no_reply(key, amount).await;

    assert_eq!(Ok(()), result);

    let result = client.get(key).await;

    assert_eq!(
        value + amount,
        btoi::btoi::<u64>(&result.unwrap().unwrap().data)
            .expect("couldn't parse data from bytes to integer")
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_decrement_raises_error_when_key_doesnt_exist() {
    let key = "fails-to-decrement";

    let mut client = setup_client(&[key]).await;

    let amount = 1;

    let result = client.decrement(key, amount).await;

    assert!(matches!(result, Err(Error::Protocol(Status::NotFound))));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_decrements_existing_key() {
    let key = "key-to-decrement";

    let mut client = setup_client(&[key]).await;

    let value: u64 = 10;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.decrement(key, amount).await;

    assert_eq!(Ok(9), result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_decrement_does_not_reduce_value_below_zero() {
    let key = "key-to-decrement-past-zero";

    let mut client = setup_client(&[key]).await;

    let value: u64 = 0;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.decrement(key, amount).await;

    assert_eq!(Ok(0), result);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_decrements_existing_key_with_no_reply() {
    let key = "key-to-decrement-no-reply";

    let mut client = setup_client(&[key]).await;

    let value: u64 = 1;

    let _ = client.set(key, value, None, None).await;

    let amount = 1;

    let result = client.decrement_no_reply(key, amount).await;

    assert_eq!(Ok(()), result);

    let result = client.get(key).await;

    assert_eq!(
        value - amount,
        btoi::btoi::<u64>(&result.unwrap().unwrap().data)
            .expect("couldn't parse data from bytes to integer")
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[serial]
async fn test_flush_all() {
    let key = "flush-all-key";
    let value: u64 = 1;

    let mut client = setup_client(&[key]).await;

    let _ = client.set(key, value, None, None).await;
    let result = client.get(key).await;
    assert!(result.is_ok());

    let result = client.flush_all().await;
    assert!(result.is_ok());

    let result = client.get(key).await;
    assert!(matches!(result, Ok(None)));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_gat_with_nonexistent_key() {
    let key = "test_gat_with_nonexistent_key";

    let mut client = setup_client(&[key]).await;

    let get_result = client.gat(key, None).await;

    assert!(matches!(get_result, Ok(None)), "key should not be found");
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_gat_with_cached_key() {
    let key = "test_gat_with_cached_key";
    let value = "value-that-exists";

    let mut client = setup_client(&[key]).await;

    let set_result = client.set(key, value, None, None).await;
    assert!(
        set_result.is_ok(),
        "failed to set {}, {:?}",
        key,
        set_result
    );

    let get_result = client.gat(key, Some(-1)).await;

    assert!(
        get_result.is_ok(),
        "failed to gat {}, {:?}",
        key,
        get_result
    );

    let expired_result = client.get(key).await;

    assert!(
        matches!(expired_result, Ok(None)),
        "the key should be expired"
    );
}
