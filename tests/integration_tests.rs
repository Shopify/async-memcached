use async_memcached::{Client, Error, ErrorKind, Status};
use rand::seq::IteratorRandom;
use serial_test::{parallel, serial};

// NOTE: Each test should run with keys unique to that test to avoid async conflicts.  Because these tests run concurrently,
// it's possible to delete/overwrite keys created by another test before they're read.

const MAX_KEY_LENGTH: usize = 250; // 250 bytes, default memcached max key length
const LARGE_PAYLOAD_SIZE: usize = 1024 * 1024 - 310; // ~1 MB, default memcached max value size

async fn setup_client(keys: &[&str]) -> Client {
    let mut client = Client::new("tcp://127.0.0.1:11211")
        .await
        .expect("Failed to connect to server");

    for key in keys {
        if key.len() > MAX_KEY_LENGTH {
            continue; // skip keys that are too long because they'll fail
        }

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
async fn test_get_fails_with_key_too_long() {
    let key = "a".repeat(MAX_KEY_LENGTH + 1);

    let mut client = setup_client(&[&key]).await;

    let get_result = client.get(key).await;

    assert!(get_result.is_err());
    assert!(matches!(
        get_result,
        Err(Error::Protocol(Status::Error(ErrorKind::KeyTooLong)))
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_cache_hit_with_no_flags() {
    let key = "meta-get-test-key-cache-hit-with-no-flags";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    client.set(key, value, None, None).await.unwrap();

    let flags = None;
    let result = client.meta_get(key, flags).await.unwrap();

    assert!(result.is_none());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_with_only_v_flag() {
    let key = "meta-get-test-key-with-only-v-flag";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    client.set(key, value, None, None).await.unwrap();

    let flags = ["v"];
    let result = client.meta_get(key, Some(&flags)).await.unwrap();

    assert!(result.is_some());
    let result_meta_value = result.unwrap();

    assert_eq!(
        String::from_utf8(result_meta_value.data.unwrap()).unwrap(),
        value
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_with_large_key_and_value_item() {
    let key = "a".repeat(MAX_KEY_LENGTH);
    let value = "b".repeat(LARGE_PAYLOAD_SIZE);

    let mut client = setup_client(&[&key]).await;

    client.set(&key, &value, None, None).await.unwrap();

    let flags = ["v"];
    let result = client.meta_get(key, Some(&flags)).await.unwrap();

    assert!(result.is_some());
    let result_meta_value = result.unwrap();

    assert_eq!(
        String::from_utf8(result_meta_value.data.unwrap()).unwrap(),
        value
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_with_many_flags() {
    let key = "meta-get-test-key-with-many-flags";
    let value = "test-value";
    let ttl = 3600; // 1 hour

    let mut client = setup_client(&[key]).await;

    // Set the key with a TTL
    client.set(key, value, Some(ttl), None).await.unwrap();

    // Perform a get to ensure the item has been hit
    let result = client.get(key).await.unwrap();
    let result_value = result.unwrap();
    assert_eq!(
        String::from_utf8(result_value.data.unwrap()).unwrap(),
        value
    );

    let flags = ["v", "h", "l", "t", "O9001"];
    let result = client.meta_get(key, Some(&flags)).await.unwrap();

    assert!(result.is_some());
    let result_meta_value = result.unwrap();

    assert_eq!(
        String::from_utf8(result_meta_value.data.unwrap()).unwrap(),
        value
    );

    let meta_flag_values = result_meta_value.meta_values.unwrap();
    assert!(meta_flag_values.hit_before.unwrap());
    assert_eq!(meta_flag_values.last_accessed.unwrap(), 0);
    assert!(meta_flag_values.ttl_remaining.unwrap() > 0);
    assert_eq!(meta_flag_values.opaque_token.unwrap(), "9001".as_bytes());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_with_many_flags_and_no_value() {
    let key = "meta-get-test-key-with-many-flags-no-value";
    let value = "test-value";
    let ttl = 3600; // 1 hour

    let mut client = setup_client(&[key]).await;

    // Set the key with a TTL
    client.set(key, value, Some(ttl), None).await.unwrap();

    // Perform a get to ensure the item has been hit (for the h flag), and confirm the value exists
    let get_result = client.get(key).await.unwrap();
    let get_result_value = get_result.unwrap();
    assert_eq!(
        String::from_utf8(get_result_value.data.unwrap()).unwrap(),
        value
    );

    let flags = ["h", "l", "t", "O9001"];
    let meta_get_result = client.meta_get(key, Some(&flags)).await.unwrap();

    assert!(meta_get_result.is_some());
    let meta_get_result_value = meta_get_result.unwrap();

    assert_eq!(meta_get_result_value.data, None);

    let meta_flag_values = meta_get_result_value.meta_values.unwrap();
    assert!(meta_flag_values.hit_before.unwrap());
    assert_eq!(meta_flag_values.last_accessed.unwrap(), 0);
    assert!(meta_flag_values.ttl_remaining.unwrap() > 0);
    assert_eq!(meta_flag_values.opaque_token.unwrap(), "9001".as_bytes());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_not_found() {
    let key = "meta-get-test-key-not-found";
    let flags = ["v", "h", "l", "t"];
    let mut client = setup_client(&[key]).await;

    let result = client.meta_get(key, Some(&flags)).await.unwrap();
    assert_eq!(result, None);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_not_found_with_opaque_flag() {
    let key = "meta-get-test-key-not-found";
    let flags = ["v", "O9001"];
    let mut client = setup_client(&[key]).await;

    let result = client.meta_get(key, Some(&flags)).await.unwrap();

    assert_eq!(
        result.unwrap().meta_values.unwrap().opaque_token,
        Some("9001".as_bytes().to_vec())
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_not_found_with_k_flag() {
    let key = "meta-get-test-key-not-found";
    let flags = ["v", "k"];
    let mut client = setup_client(&[key]).await;

    let result = client.meta_get(key, Some(&flags)).await.unwrap();

    assert_eq!(result.unwrap().key, key.as_bytes().to_vec());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_quiet_mode_meta_get_with_k_flag_and_cache_hit() {
    let key = "quiet-mode-meta-get-test-key-cache-hit";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    client.set(key, value, None, None).await.unwrap();

    let flags = ["v", "k", "q"];

    let result = client.meta_get(key, Some(&flags)).await.unwrap().unwrap();

    assert_eq!(result.key, key.as_bytes().to_vec());
    assert_eq!(result.data, Some(value.as_bytes().to_vec()));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_quiet_mode_meta_get_key_too_long() {
    let key = "a".repeat(MAX_KEY_LENGTH + 1);

    let mut client = setup_client(&[&key]).await;

    let flags = ["v", "q"];

    let result = client.meta_get(&key, Some(&flags)).await;

    assert!(matches!(
        result,
        Err(Error::Protocol(Status::Error(ErrorKind::KeyTooLong)))
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_quiet_mode_meta_get_with_k_flag_and_cache_miss() {
    let key = "quiet-mode-meta-get-test-key-cache-miss";

    let mut client = setup_client(&[key]).await;

    let flags = ["v", "k", "q"];

    let result = client.meta_get(key, Some(&flags)).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
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
async fn test_add_fails_with_key_too_long() {
    let key = "b".repeat(MAX_KEY_LENGTH + 1);

    let mut client = setup_client(&[&key]).await;

    let value = "value";
    let add_result = client.add(&key, value, None, None).await;

    assert!(matches!(
        add_result,
        Err(Error::Protocol(Status::Error(ErrorKind::KeyTooLong)))
    ));
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
async fn test_add_multi_inserts_client_error_for_key_too_long() {
    let key_too_long = "e".repeat(MAX_KEY_LENGTH + 1);

    let keys = vec!["short-key-1", &key_too_long, "short-key-3"];
    let values = vec!["value1", "value2", "value3"];

    let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values.into_iter()).collect();

    let mut client = setup_client(&keys).await;

    let add_multi_result = client.add_multi(&kv, None, None).await;

    assert!(add_multi_result.is_ok());

    let result_map = add_multi_result.unwrap();

    assert_eq!(keys.len(), result_map.len());

    assert!(result_map[&keys[0]].is_ok(), "Key {} should be Ok", keys[0]);
    assert!(
        result_map[&key_too_long.as_str()].is_err(),
        "Key {} should have an error",
        key_too_long
    );
    assert!(result_map[&keys[2]].is_ok(), "Key {} should be Ok", keys[2]);
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
        std::str::from_utf8(&get_result.unwrap().unwrap().data.unwrap())
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
                .unwrap()
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
                .unwrap()
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
                .unwrap()
        )
        .expect("couldn't parse data from bytes to integer")
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_succeeds_with_max_length_key() {
    let key = "c".repeat(MAX_KEY_LENGTH);

    let mut client = setup_client(&[&key]).await;

    let value = "value";
    let set_result = client.set(&key, value, None, None).await;

    assert!(set_result.is_ok());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_fails_with_key_too_long() {
    let key = "c".repeat(MAX_KEY_LENGTH + 1);

    let mut client = setup_client(&[&key]).await;

    let value = "value";
    let set_result = client.set(&key, value, None, None).await;

    assert!(matches!(
        set_result,
        Err(Error::Protocol(Status::Error(ErrorKind::KeyTooLong)))
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_fails_with_value_too_large() {
    let key = "key-with-value-that-exceeds-max-payload-size";

    let mut client = setup_client(&[key]).await;

    let value = "a".repeat(LARGE_PAYLOAD_SIZE * 2);
    let set_result = client.set(key, &value, None, None).await;

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
async fn test_meta_set_with_no_flags() {
    let key = "meta-set-test-key";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set
    let result = client.meta_set(key, value, None).await;
    assert!(
        result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        result
    );

    // Retrieve the key using meta_get with v flag to verify that the item was stored
    let get_flags = ["v"];
    let get_result = client.meta_get(key, Some(&get_flags)).await.unwrap();

    assert!(get_result.is_some(), "Key not found after meta_set");
    let result_value = get_result.unwrap();

    // Verify the value
    assert_eq!(
        String::from_utf8(result_value.data.unwrap()).unwrap(),
        value,
        "Retrieved value does not match set value"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_meta_set_with_opaque_token() {
    let key = "meta-set-opaque-token-test-key";
    let value = "test-value";
    let mut client = setup_client(&[key]).await;

    let meta_flags = ["T3600", "F42", "Oopaque-token"];

    // Set the key using meta_set
    let set_result = client.meta_set(key, value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    // Fetch the key with .get() to ensure that the item has been hit before
    let get_result = client.get(key).await.unwrap();
    assert!(get_result.is_some(), "Key not found after meta_set");

    // Retrieve the key using meta_get to verify
    let get_flags = ["v", "h", "l", "t"];
    let get_result = client.meta_get(key, Some(&get_flags)).await.unwrap();

    assert!(get_result.is_some(), "Key not found after meta_set");
    let result_value = get_result.unwrap();

    // Verify the value
    assert_eq!(
        String::from_utf8(result_value.data.unwrap()).unwrap(),
        value,
        "Retrieved value does not match set value"
    );

    // Verify the metaflags
    let meta_flag_values = result_value.meta_values.unwrap();
    assert_eq!(meta_flag_values.hit_before, Some(true));
    assert_eq!(meta_flag_values.last_accessed, Some(0));
    assert_eq!(meta_flag_values.ttl_remaining, Some(3600));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
async fn test_meta_set_with_k_flag() {
    let key = "meta-set-k-flag-test-key";
    let value = "test-value";
    let mut client = setup_client(&[key]).await;

    let meta_flags = ["MS", "T3600", "F42", "k"];

    // Set the key using meta_set
    let set_result = client.meta_set(key, value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    let set_value = set_result.unwrap().unwrap();
    assert_eq!(set_value.key, key.as_bytes());

    // Fetch the key with .get() to ensure that the item has been hit before
    let get_result = client.get(key).await.unwrap();
    assert!(get_result.is_some(), "Key not found after meta_set");

    // Retrieve the key using meta_get to verify
    let get_flags = ["v", "h", "l", "t"];
    let get_result = client.meta_get(key, Some(&get_flags)).await.unwrap();

    assert!(get_result.is_some(), "Key not found after meta_set");
    let result_value = get_result.unwrap();

    // Verify the value
    assert_eq!(
        String::from_utf8(result_value.data.unwrap()).unwrap(),
        value,
        "Retrieved value does not match set value"
    );

    // Verify the metaflags
    let meta_flag_values = result_value.meta_values.unwrap();
    assert_eq!(meta_flag_values.hit_before, Some(true));
    assert_eq!(meta_flag_values.last_accessed, Some(0));
    assert_eq!(meta_flag_values.ttl_remaining, Some(3600));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_with_cas_semantics_on_nonexistent_key() {
    let key = "meta-set-cas-semantics-on-nonexistent-key-test-key";
    let value = "test-value";
    let mut client = setup_client(&[key]).await;

    let meta_flags = ["C12321"];

    let set_result = client.meta_set(key, value, Some(&meta_flags)).await;
    assert!(
        set_result.is_err(),
        "meta_set should have returned an error for a nonexistent key with CAS semantics"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_with_cas_match_on_key_that_exists() {
    let key = "meta-set-cas-match-on-key-that-exists-test-key";
    let value = "test-value";
    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set to prepopulate, use E flag to set CAS value
    let meta_flags = ["MS", "E12321"];
    let set_result = client.meta_set(key, value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    let get_flags = ["v", "c"];
    let get_result = client.meta_get(key, Some(&get_flags)).await.unwrap();
    assert!(get_result.is_some(), "Key not found after meta_set");

    let get_value = get_result.unwrap();

    assert_eq!(
        std::str::from_utf8(&get_value.data.unwrap()).unwrap(),
        value
    );
    assert_eq!(get_value.cas.unwrap(), 12321);

    // Set the key again using the force-set CAS value via C flag
    let meta_flags = ["C12321"];
    let new_value = "new-value";

    let set_result = client.meta_set(key, new_value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "meta_set should set a new value when C flag token matches existing CAS value"
    );

    let get_flags = ["v", "c"];
    let get_result = client.meta_get(key, Some(&get_flags)).await.unwrap();
    assert!(get_result.is_some(), "Key not found after meta_set");

    let get_value = get_result.unwrap();
    assert_eq!(
        std::str::from_utf8(&get_value.data.unwrap()).unwrap(),
        new_value
    );
    // CAS value should be reset to a new atomic counter value
    assert!(get_value.cas.unwrap() != 12321);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_with_cas_mismatch_on_key_that_exists() {
    let key = "meta-set-cas-mismatch-on-key-that-exists-test-key";
    let value = "test-value";
    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set to prepopulate, use E flag to set CAS value
    let meta_flags = ["MS", "E99999"];
    let set_result = client.meta_set(key, value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    let get_flags = ["v", "c"];
    let get_result = client.meta_get(key, Some(&get_flags)).await.unwrap();
    assert!(get_result.is_some(), "Key not found after meta_set");

    let get_value = get_result.unwrap();

    assert_eq!(
        std::str::from_utf8(&get_value.data.unwrap()).unwrap(),
        value
    );
    assert_eq!(get_value.cas.unwrap(), 99999);

    // Try to set the key again with a CAS value that doesn't match
    let meta_flags = ["C12321"];
    let new_value = "new-value";

    let set_result = client.meta_set(key, new_value, Some(&meta_flags)).await;
    assert!(
        set_result.is_err(),
        "meta_set should err when C token doesn't match existing CAS value"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_invalidate_on_expired_cas() {
    let key = "meta-set-invalidate-on-expired-cas-test-key";
    let value = "VALID";
    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set to prepopulate, use E flag to set CAS value
    let meta_flags = ["MS", "E99999"];
    let set_result = client.meta_set(key, value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    // Invalidate the key by combining I flag with a C flag with a token lower than the existing CAS value
    let meta_flags = ["C1", "I"];
    let new_value = "INVALID";

    let set_result = client.meta_set(key, new_value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "meta_set should invalidate the key when I flag is used with a C flag token that is lower than the existing CAS value"
    );

    // Verify that the key was invalidated
    let get_flags = ["v", "c"];
    let get_result = client.meta_get(key, Some(&get_flags)).await;

    let get_value = get_result.unwrap().unwrap();

    assert_eq!(
        std::str::from_utf8(&get_value.data.unwrap()).unwrap(),
        new_value
    );

    // CAS value should be reset to a new atomic counter value when the key is invalidated
    assert!(get_value.cas.unwrap() != 99999);

    let meta_values = get_value.meta_values.unwrap();
    assert!(meta_values.is_recache_winner.unwrap());
    assert!(meta_values.is_stale.unwrap());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_invalidate_on_non_expired_cas() {
    let key = "meta-set-invalidate-on-non-expired-cas-test-key";
    let value = "VALID";
    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set to prepopulate, use E flag to set CAS value
    let meta_flags = ["MS", "E1"];
    let set_result = client.meta_set(key, value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    // Invalidate the key by combining I flag with a C flag with a token lower than the existing CAS value
    let meta_flags = ["C9999999", "I"];
    let new_value = "INVALID";

    let set_result = client.meta_set(key, new_value, Some(&meta_flags)).await;
    assert!(
        set_result.is_err(),
        "meta_set should not invalidate the key when I flag is used with a C flag token that is greater than the existing CAS value"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_in_add_mode() {
    let key = "meta-set-add-if-not-exists-test-key";
    let original_value = "test-value";

    let mut client = setup_client(&[key]).await;

    let mut meta_flags = ["ME", "F24"];

    // Set the key using meta_set to pre-populate (in add mode)
    let set_result = client
        .meta_set(key, original_value, Some(&meta_flags))
        .await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    // Check initial set results
    let get_flags = ["v", "f"];
    let get_result_value = client
        .meta_get(key, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 24);
    assert_eq!(
        std::str::from_utf8(&get_result_value.data.unwrap()).unwrap(),
        original_value
    );

    let new_value = "a-new-value";
    meta_flags = ["ME", "F42"];

    // Set the key using meta_set again, this should fail with Status::NotStored
    let add_result = client.meta_set(key, new_value, Some(&meta_flags)).await;
    let result = add_result.unwrap_err();
    assert_eq!(Error::Protocol(Status::NotStored), result);

    // Verify that the key was not re-added / modified
    let get_flags = ["v", "f"];
    let get_result_value = client
        .meta_get(key, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 24);
    assert_eq!(
        std::str::from_utf8(&get_result_value.data.unwrap()).unwrap(),
        original_value
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_existing_key_in_prepend_mode() {
    let key = "meta-set-prepend-test-key";
    let original_value = "test-value";

    let mut client = setup_client(&[key]).await;

    let meta_flags = ["MS", "F24"];

    // Set the key using meta_set to pre-populate (in set mode)
    let set_result = client
        .meta_set(key, original_value, Some(&meta_flags))
        .await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    // Check initial set results
    let get_flags = ["v", "f"];
    let get_result_value = client
        .meta_get(key, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 24);
    assert_eq!(
        std::str::from_utf8(&get_result_value.data.unwrap()).unwrap(),
        original_value
    );

    let new_value = "prefix-to-prepend-";
    // Assign a new F flag value - this should be ignored and the original flags preserved
    let meta_flags = ["MP", "F42"];

    // Set the key using meta_set again, this should fail with Status::NotStored
    let prepend_result = client.meta_set(key, new_value, Some(&meta_flags)).await;
    assert!(
        prepend_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        prepend_result
    );

    // Verify that the new value was prepended to the existing value and that the original flags are preserved
    let get_flags = ["v", "f"];
    let get_result_value = client
        .meta_get(key, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 24);
    assert_eq!(
        std::str::from_utf8(&get_result_value.data.unwrap()).unwrap(),
        format!("{}{}", new_value, original_value)
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_nonexistent_key_in_prepend_mode() {
    let key = "meta-set-prepend-non-existent-key";
    let original_value = "test-value";

    let mut client = setup_client(&[key]).await;

    let meta_flags = ["MP", "F24"];

    // Set the key using meta_set to pre-populate (in prepend mode)
    let set_result = client
        .meta_set(key, original_value, Some(&meta_flags))
        .await;
    assert!(
        set_result.is_err(),
        "Should have received Err(Protocol(NotStored)) but got: {:?}",
        set_result
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_existing_key_in_append_mode() {
    let key = "meta-set-append-test-key";
    let original_value = "test-value";

    let mut client = setup_client(&[key]).await;

    let meta_flags = ["MS", "F24"];

    // Set the key using meta_set to pre-populate (in set mode)
    let set_result = client
        .meta_set(key, original_value, Some(&meta_flags))
        .await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    // Check initial set results
    let get_flags = ["v", "f"];
    let get_result_value = client
        .meta_get(key, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 24);
    assert_eq!(
        std::str::from_utf8(&get_result_value.data.unwrap()).unwrap(),
        original_value
    );

    let new_value = "-suffix-to-append";
    // Assign a new F flag value - this should be ignored and the original flags preserved
    let meta_flags = ["MA", "F42"];

    // Set the key using meta_set again, this should fail with Status::NotStored
    let append_result = client.meta_set(key, new_value, Some(&meta_flags)).await;
    assert!(
        append_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        append_result
    );

    // Verify that the new value was appended to the existing value and that the original flags are preserved
    let get_flags = ["v", "f"];
    let get_result_value = client
        .meta_get(key, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 24);
    assert_eq!(
        std::str::from_utf8(&get_result_value.data.unwrap()).unwrap(),
        format!("{}{}", original_value, new_value)
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_nonexistent_key_in_append_mode() {
    let key = "meta-set-append-non-existent-key";
    let original_value = "test-value";

    let mut client = setup_client(&[key]).await;

    let meta_flags = ["MA", "F24"];

    // Set the key using meta_set to pre-populate (in prepend mode)
    let set_result = client
        .meta_set(key, original_value, Some(&meta_flags))
        .await;
    assert!(
        set_result.is_err(),
        "Should have received Err(Protocol(NotStored)) but got: {:?}",
        set_result
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_nonexistent_key_in_append_mode_with_autovivify() {
    let key = "meta-set-append-autovivify-test-key";
    let original_value = "test-value";

    let mut client = setup_client(&[key]).await;

    // TODO: This will throw a ("Tag") error if N is provided without a TTL, that error needs to be handled properly (raise to client as ClientError // invalid command)
    // Seems like any flag that wants a token will complain if the token is not provided with the same error, except O which will throw ("CrLf")
    let meta_flags = ["MA", "F24", "N3600"];

    // Set the key using meta_set to pre-populate (in set mode)
    let set_result = client
        .meta_set(key, original_value, Some(&meta_flags))
        .await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    // Check initial set results
    let get_flags = ["v", "f", "t"];
    let get_result_value = client
        .meta_get(key, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 24);
    assert_eq!(
        get_result_value.meta_values.unwrap().ttl_remaining.unwrap(),
        3600
    );
    assert_eq!(
        std::str::from_utf8(&get_result_value.data.unwrap()).unwrap(),
        original_value
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_existing_key_in_replace_mode() {
    let key = "meta-set-replace-test-key";
    let original_value = "original-value";

    let mut client = setup_client(&[key]).await;

    let meta_flags = ["MS", "F24"];

    // Set the key using meta_set to pre-populate (in set mode)
    let set_result = client
        .meta_set(key, original_value, Some(&meta_flags))
        .await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    // Check initial set results
    let get_flags = ["v", "f"];
    let get_result_value = client
        .meta_get(key, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 24);
    assert_eq!(
        std::str::from_utf8(&get_result_value.data.unwrap()).unwrap(),
        original_value
    );

    let new_value = "new-value";
    // Assign a new F flag value - this should be ignored and the flags are also replaced
    let meta_flags = ["MR", "F42"];

    // Set the key using meta_set again, this should fail with Status::NotStored
    let replace_result = client.meta_set(key, new_value, Some(&meta_flags)).await;
    assert!(
        replace_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        replace_result
    );

    // Verify that the new value was replaced and that the original flags are preserved
    let get_flags = ["v", "f"];
    let get_result_value = client
        .meta_get(key, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 42);
    assert_eq!(
        std::str::from_utf8(&get_result_value.data.unwrap()).unwrap(),
        new_value
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_set_nonexistent_key_in_replace_mode() {
    let key = "meta-set-replace-non-existent-key";
    let original_value = "test-value";

    let mut client = setup_client(&[key]).await;

    let meta_flags = ["MR", "F24"];

    // Set the key using meta_set to pre-populate (in replace mode)
    let set_result = client
        .meta_set(key, original_value, Some(&meta_flags))
        .await;
    assert!(
        set_result.is_err(),
        "Should have received Err(Protocol(NotStored)) but got: {:?}",
        set_result
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_get_multi() {
    let keys = ["mg-key1", "mg-key2", "mg-key3"];
    let values = ["value1", "value2", "value3"];

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
    let values = ["value1", "value2", "value3"];

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
async fn test_get_multi_skips_key_too_long() {
    let mut keys = vec!["mgktl-key1", "mgktl-key2", "mgktl-key3"];
    let values = ["value1", "value2", "value3"];

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

    let key_too_long = "d".repeat(MAX_KEY_LENGTH + 1);
    keys = vec!["mgktl-key1", &key_too_long, "mgktl-key3"];

    let get_multi_results = client.get_multi(&keys).await;

    let get_multi_results = get_multi_results.expect("Should have yielded Vec<Value>");

    for item in get_multi_results {
        assert!(keys.contains(
            &String::from_utf8(item.key)
                .expect("Should have been able to parse key as utf8")
                .as_str()
        ));
    }
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_get_many_aliases_get_multi_properly() {
    let keys = vec!["get-many-key1", "get-many-key2", "get-many-key3"];
    let values = ["value1", "value2", "value3"];

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
            String::from_utf8(get_value.data.unwrap()).expect("failed to parse a string"),
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
            String::from_utf8(get_value.data.unwrap()).expect("failed to parse a string"),
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
                .unwrap()
        )
        .expect("failed to parse string from bytes"),
        "value2"
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_set_multi_inserts_client_error_for_key_too_long() {
    let key_too_long = "e".repeat(MAX_KEY_LENGTH + 1);

    let keys = vec!["short-key-1", &key_too_long, "short-key-3"];
    let values = vec!["value1", "value2", "value3"];

    let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values.into_iter()).collect();

    let mut client = setup_client(&keys).await;

    let set_multi_result = client.set_multi(&kv, None, None).await;

    assert!(set_multi_result.is_ok());

    let result_map = set_multi_result.unwrap();

    assert_eq!(keys.len(), result_map.len());

    assert!(result_map[&keys[0]].is_ok(), "Key {} should be Ok", keys[0]);
    assert!(
        result_map[&key_too_long.as_str()].is_err(),
        "Key {} should have an error",
        key_too_long
    );
    assert!(result_map[&keys[2]].is_ok(), "Key {} should be Ok", keys[2]);
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
        std::str::from_utf8(&small_result.unwrap().unwrap().data.unwrap())
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
                std::str::from_utf8(&get_result.data.unwrap()).unwrap(),
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
        btoi::btoi::<u64>(&result.unwrap().unwrap().data.unwrap())
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
        btoi::btoi::<u64>(&result.unwrap().unwrap().data.unwrap())
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
