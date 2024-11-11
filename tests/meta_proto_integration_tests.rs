use async_memcached::{AsciiProtocol, Client, Error, ErrorKind, MetaProtocol, Status};
use serial_test::parallel;

// NOTE: Each test should run with keys unique to that test to avoid async conflicts.  Because these tests run concurrently,
// it's possible to delete/overwrite keys created by another test before they're read.

const MAX_KEY_LENGTH: usize = 250; // 250 bytes, default memcached max key length
const LARGE_PAYLOAD_SIZE: usize = 1024 * 1024 - 310; // Memcached's default maximum payload size ~1MB minus max key length + metadata

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
async fn test_meta_get_cache_hit_with_no_flags() {
    let key = "meta-get-test-key-cache-hit-with-no-flags";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    client.set(key, value, None, None).await.unwrap();

    let flags = None;
    let result = client.meta_get(key, false, None, flags).await.unwrap();

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
    let result = client
        .meta_get(key, false, None, Some(&flags))
        .await
        .unwrap();

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
    let result = client
        .meta_get(key, false, None, Some(&flags))
        .await
        .unwrap();

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
    let result = client
        .meta_get(key, false, None, Some(&flags))
        .await
        .unwrap();

    assert!(result.is_some());
    let result_meta_value = result.unwrap();

    assert_eq!(
        String::from_utf8(result_meta_value.data.unwrap()).unwrap(),
        value
    );

    assert!(result_meta_value.hit_before.unwrap());
    assert_eq!(result_meta_value.last_accessed.unwrap(), 0);
    assert!(result_meta_value.ttl_remaining.unwrap() > 0);
    assert_eq!(result_meta_value.opaque_token.unwrap(), "9001".as_bytes());
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
    let meta_get_result = client
        .meta_get(key, false, None, Some(&flags))
        .await
        .unwrap();

    assert!(meta_get_result.is_some());
    let meta_get_result_value = meta_get_result.unwrap();

    assert_eq!(meta_get_result_value.data, None);

    assert!(meta_get_result_value.hit_before.unwrap());
    assert_eq!(meta_get_result_value.last_accessed.unwrap(), 0);
    assert!(meta_get_result_value.ttl_remaining.unwrap() > 0);
    assert_eq!(
        meta_get_result_value.opaque_token.unwrap(),
        "9001".as_bytes()
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_not_found() {
    let key = "meta-get-test-key-not-found";
    let flags = ["v", "h", "l", "t"];
    let mut client = setup_client(&[key]).await;

    let result = client
        .meta_get(key, false, None, Some(&flags))
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_not_found_with_opaque_flag() {
    let key = "meta-get-test-key-not-found";
    let flags = ["v", "O9001"];
    let mut client = setup_client(&[key]).await;

    let result = client
        .meta_get(key, false, None, Some(&flags))
        .await
        .unwrap();

    assert_eq!(
        result.unwrap().opaque_token.unwrap(),
        "9001".as_bytes().to_vec()
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_get_not_found_with_k_flag() {
    let key = "meta-get-test-key-not-found";
    let flags = ["v", "k"];
    let mut client = setup_client(&[key]).await;

    let result = client
        .meta_get(key, false, None, Some(&flags))
        .await
        .unwrap();

    assert_eq!(result.unwrap().key, Some(key.as_bytes().to_vec()));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_quiet_mode_meta_get_with_k_flag_and_cache_hit() {
    let key = "quiet-mode-meta-get-test-key-cache-hit";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    client.set(key, value, None, None).await.unwrap();

    let flags = ["v", "k"];

    let result = client
        .meta_get(key, true, None, Some(&flags))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.key, Some(key.as_bytes().to_vec()));
    assert_eq!(result.data, Some(value.as_bytes().to_vec()));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_quiet_mode_meta_get_key_too_long() {
    let key = "a".repeat(MAX_KEY_LENGTH + 1);

    let mut client = setup_client(&[&key]).await;

    let flags = ["v"];

    let result = client.meta_get(&key, true, None, Some(&flags)).await;

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

    let flags = ["v", "k"];

    let result = client.meta_get(key, true, None, Some(&flags)).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
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
    let get_result = client
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap();

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
    let get_result = client
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap();

    assert!(get_result.is_some(), "Key not found after meta_set");
    let result_value = get_result.unwrap();

    // Verify the value
    assert_eq!(
        String::from_utf8(result_value.data.unwrap()).unwrap(),
        value,
        "Retrieved value does not match set value"
    );

    // Verify the metaflags
    assert!(result_value.hit_before.unwrap());
    assert_eq!(result_value.last_accessed, Some(0));
    assert_eq!(result_value.ttl_remaining, Some(3600));
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
    assert_eq!(set_value.key, Some(key.as_bytes().to_vec()));

    // Fetch the key with .get() to ensure that the item has been hit before
    let get_result = client.get(key).await.unwrap();
    assert!(get_result.is_some(), "Key not found after meta_set");

    // Retrieve the key using meta_get to verify
    let get_flags = ["v", "h", "l", "t"];
    let get_result = client
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap();

    assert!(get_result.is_some(), "Key not found after meta_set");
    let result_value = get_result.unwrap();

    // Verify the value
    assert_eq!(
        String::from_utf8(result_value.data.unwrap()).unwrap(),
        value,
        "Retrieved value does not match set value"
    );

    // Verify the metaflags
    assert!(result_value.hit_before.unwrap());
    assert_eq!(result_value.last_accessed, Some(0));
    assert_eq!(result_value.ttl_remaining, Some(3600));
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
    let get_result = client
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap();
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
    let get_result = client
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap();
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
    let get_result = client
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap();
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
    let get_result = client.meta_get(key, false, None, Some(&get_flags)).await;

    let get_value = get_result.unwrap().unwrap();

    assert_eq!(
        std::str::from_utf8(&get_value.data.unwrap()).unwrap(),
        new_value
    );

    // CAS value should be reset to a new atomic counter value when the key is invalidated
    assert!(get_value.cas.unwrap() != 99999);
    assert!(get_value.is_recache_winner.unwrap());
    assert!(get_value.is_stale.unwrap());
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
        .meta_get(key, false, None, Some(&get_flags))
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
        .meta_get(key, false, None, Some(&get_flags))
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
        .meta_get(key, false, None, Some(&get_flags))
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
        .meta_get(key, false, None, Some(&get_flags))
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
        .meta_get(key, false, None, Some(&get_flags))
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
        .meta_get(key, false, None, Some(&get_flags))
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
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(get_result_value.flags.unwrap(), 24);
    assert_eq!(get_result_value.ttl_remaining.unwrap(), 3600);
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
        .meta_get(key, false, None, Some(&get_flags))
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
        .meta_get(key, false, None, Some(&get_flags))
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
async fn test_quiet_mode_meta_set() {
    let key = "quiet-mode-meta-set-test-key";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    let flags = ["k", "q"];

    let result = client.meta_set(key, value, Some(&flags)).await;

    assert!(result.is_ok());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_quiet_mode_meta_set_with_cas_match_on_key_that_exists() {
    let key = "quiet-mode-meta-set-cas-match-on-key-that-exists-test-key";
    let value = "test-value";
    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set to prepopulate, use E flag to set CAS value
    let meta_flags = ["MS", "E12321", "q"];
    let set_result = client.meta_set(key, value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "Failed to set key using meta_set: {:?}",
        set_result
    );

    let get_flags = ["v", "c"];
    let get_result = client
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap();
    assert!(get_result.is_some(), "Key not found after meta_set");

    let get_value = get_result.unwrap();

    assert_eq!(
        std::str::from_utf8(&get_value.data.unwrap()).unwrap(),
        value
    );
    assert_eq!(get_value.cas.unwrap(), 12321);

    // Set the key again using the force-set CAS value via C flag
    let meta_flags = ["C12321", "q"];
    let new_value = "new-value";

    let set_result = client.meta_set(key, new_value, Some(&meta_flags)).await;
    assert!(
        set_result.is_ok(),
        "meta_set should set a new value when C flag token matches existing CAS value"
    );

    let get_flags = ["v", "c"];
    let get_result = client
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap();
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
async fn test_quiet_mode_meta_set_with_cas_semantics_on_nonexistent_key() {
    let key = "quiet-mode-meta-set-cas-semantics-on-nonexistent-key-test-key";
    let value = "test-value";
    let mut client = setup_client(&[key]).await;

    let meta_flags = ["C12321", "q"];

    let set_result = client.meta_set(key, value, Some(&meta_flags)).await;
    assert!(
        set_result.is_err(),
        "meta_set should have returned an error for a nonexistent key with CAS semantics"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_quiet_mode_meta_set_with_cas_mismatch_on_key_that_exists() {
    let key = "quiet-mode-meta-set-cas-mismatch-on-key-that-exists-test-key";
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
    let get_result = client
        .meta_get(key, false, None, Some(&get_flags))
        .await
        .unwrap();
    assert!(get_result.is_some(), "Key not found after meta_set");

    let get_value = get_result.unwrap();

    assert_eq!(
        std::str::from_utf8(&get_value.data.unwrap()).unwrap(),
        value
    );
    assert_eq!(get_value.cas.unwrap(), 99999);

    // Try to set the key again with a CAS value that doesn't match
    let meta_flags = ["C12321", "q"];
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
async fn test_quiet_mode_meta_set_nonexistent_key_in_replace_mode() {
    let key = "quiet-mode-meta-set-replace-non-existent-key";
    let original_value = "test-value";

    let mut client = setup_client(&[key]).await;

    let meta_flags = ["MR", "F24", "q"];

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
async fn test_meta_delete_existing_key_no_flags() {
    let key = "meta-delete-test-key-no-flags";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set
    client.meta_set(key, value, None).await.unwrap();

    // Delete the key without any flags
    let delete_result = client
        .meta_delete(key, false, None, None)
        .await
        .unwrap();

    // Expect None as the key and no meta  flags were provided
    assert!(delete_result.is_none());

    // Verify that the key is indeed deleted
    let get_result = client.get(key).await.unwrap();
    assert!(get_result.is_none());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_delete_existing_key_with_quiet_flag() {
    let key = "meta-delete-test-key-with-quiet-flag";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set
    client.meta_set(key, value, None).await.unwrap();

    // Use `is_quiet = true` to write q flag
    let delete_result = client
        .meta_delete(key, true, None, None)
        .await
        .unwrap();

    // Expect None as the key was successfully deleted
    assert!(delete_result.is_none());

    // Verify that the key is indeed deleted
    let get_result = client.get(key).await.unwrap();
    assert!(get_result.is_none());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_delete_nonexistent_key() {
    let key = "meta-delete-test-key-nonexistent";

    let mut client = setup_client(&[key]).await;

    // Attempt to delete a non-existent key
    let delete_result = client
        .meta_delete(key, false, None, None)
        .await;

    // Expect an error as the key does not exist
    assert!(matches!(
        delete_result,
        Err(Error::Protocol(Status::NotFound))
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_delete_key_too_long() {
    let key = "a".repeat(MAX_KEY_LENGTH + 1); // Exceeds 250 bytes

    let mut client = setup_client(&[&key]).await;

    // Attempt to delete the key that is too long
    let delete_result = client
        .meta_delete(&key, false, None, None)
        .await;

    // Expect an error indicating the key is too long
    assert!(matches!(
        delete_result,
        Err(Error::Protocol(Status::Error(ErrorKind::KeyTooLong)))
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_delete_with_matching_cas_flags() {
    let key = "meta-delete-test-key-with-matching-cas";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set with a specific CAS value
    let meta_flags = ["E12345"];
    client
        .meta_set(key, value, Some(&meta_flags))
        .await
        .unwrap();

    // Attempt to delete with the correct CAS value
    let delete_result = client
        .meta_delete(key, false, None, Some(&["C12345"]))
        .await
        .unwrap();

    // Expect None as the key was successfully deleted
    assert!(delete_result.is_none());

    // Verify that the key is indeed deleted
    let get_result = client.get(key).await.unwrap();
    assert!(get_result.is_none());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_delete_with_mismatched_cas_flags() {
    let key = "meta-delete-test-key-with-mismatched-cas";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set with a specific CAS value
    let meta_flags = ["E12345"];
    client
        .meta_set(key, value, Some(&meta_flags))
        .await
        .unwrap();

    // Attempt to delete with the mismatched CAS value
    let delete_result = client
        .meta_delete(key, false, None, Some(&["C54321"]))
        .await;

    // Expect an error due to CAS mismatch
    assert!(matches!(
        delete_result,
        Err(Error::Protocol(Status::Exists))
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_delete_invalidates_key() {
    let key = "meta-delete-invalidate-key";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set
    client
        .meta_set(key, value, None)
        .await
        .expect("Failed to set key with meta_set");

    // Delete the key using the I flag (invalidate)
    let delete_result = client
        .meta_delete(key, false, None, Some(&["I"]))
        .await
        .expect("Failed to delete key with meta_delete");

    // Expect None as the key was marked invalidated
    assert!(
        delete_result.is_none(),
        "Key was not invalidated as expected"
    );

    // Attempt to get the key; depending on implementation, it might still exist but marked as invalid
    let get_result = client
        .meta_get(key, false, None, Some(&["v"]))
        .await
        .unwrap()
        .unwrap();

    assert!(get_result.is_stale.unwrap());
    assert!(get_result.is_recache_winner.unwrap());
    assert_eq!(get_result.data.unwrap(), value.as_bytes());

    // Fetching the key again should show that subsequent gets do not qualify as recache winner
    let get_result = client
        .meta_get(key, false, None, Some(&["v"]))
        .await
        .unwrap()
        .unwrap();

    assert!(get_result.is_stale.unwrap());
    assert!(!get_result.is_recache_winner.unwrap());
    assert_eq!(get_result.data.unwrap(), value.as_bytes());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_delete_invalidates_key_and_updates_ttl() {
    let key = "meta-delete-invalidate-key-and-update-ttl";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set
    client
        .meta_set(key, value, Some(&["T1234"]))
        .await
        .expect("Failed to set key with meta_set");

    let ttl_result = client
        .meta_get(key, false, None, Some(&["t"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ttl_result.ttl_remaining.unwrap(), 1234);

    // Invalidate the key using the I flag and update the TTL to 60 seconds
    let delete_result = client
        .meta_delete(key, false, None, Some(&["I", "T60"]))
        .await
        .expect("Failed to delete key with meta_delete");

    // Expect None as the key was marked invalidated, TTL was updated to 60 seconds and no return data was requested
    assert!(
        delete_result.is_none(),
        "Key was not invalidated as expected"
    );

    // Attempt to get the key; depending on implementation, it might still exist but marked as invalid
    let get_result = client
        .meta_get(key, false, None, Some(&["v", "t"]))
        .await
        .unwrap()
        .unwrap();

    assert!(get_result.is_stale.unwrap());
    assert!(get_result.is_recache_winner.unwrap());
    assert_eq!(get_result.ttl_remaining.unwrap(), 60);
    assert_eq!(get_result.data.unwrap(), value.as_bytes());

    // Fetching the key again should show that subsequent gets do not qualify as recache winner
    let get_result = client
        .meta_get(key, false, None, Some(&["v"]))
        .await
        .unwrap()
        .unwrap();

    assert!(get_result.is_stale.unwrap());
    assert!(!get_result.is_recache_winner.unwrap());
    assert_eq!(get_result.data.unwrap(), value.as_bytes());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_delete_tombstones_key() {
    let key = "meta-delete-tombstones-key";
    let value = "test-value";

    let mut client = setup_client(&[key]).await;

    // Set the key using meta_set
    client
        .meta_set(key, value, Some(&["T1234"]))
        .await
        .expect("Failed to set key with meta_set");

    let ttl_result = client
        .meta_get(key, false, None, Some(&["t"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ttl_result.ttl_remaining.unwrap(), 1234);

    // Invalidate the key using the I flag and update the TTL to 60 seconds
    let delete_result = client
        .meta_delete(key, false, None, Some(&["x"]))
        .await
        .expect("Failed to tombstone key with meta_delete");

    // Expect None as the key was marked invalidated, TTL was updated to 60 seconds and no return data was requested
    assert!(
        delete_result.is_none(),
        "Key was not invalidated as expected"
    );

    // Attempt to get the key; depending on implementation, it might still exist but marked as invalid
    let get_result = client
        .meta_get(key, false, None, Some(&["v", "t"]))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(get_result.ttl_remaining.unwrap(), -1);
    assert!(get_result.data.is_none());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_with_no_flags() {
    let key = "meta-increment-no-flags";
    let initial_value = "9";
    let expected_value = "10";

    let mut client = setup_client(&[key]).await;

    client.set(key, initial_value, None, None).await.unwrap();

    let incr_result = client
        .meta_increment(key, false, None, None, None)
        .await
        .unwrap();

    // Verify the result is none because we didn't request any data back through meta flags
    assert!(incr_result.is_none());

    // Verify that the key was incremented by 1
    let get_result = client
        .meta_get(key, false, None, Some(&["v"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        String::from_utf8(get_result.data.unwrap()).unwrap(),
        expected_value,
        "Value after increment does not match expected value"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_decrement_with_no_flags() {
    let key = "meta-decrement-no-flags";
    let initial_value = "100";
    let expected_value = "9";

    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    let decr_result = client
        .meta_decrement(key, false, None, Some(91), None)
        .await
        .unwrap();

    // Verify the result is none because we didn't request any data back through meta flags
    assert!(decr_result.is_none());

    // Verify that the key was decremented by 1
    let get_result = client
        .meta_get(key, false, None, Some(&["v"]))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        String::from_utf8(get_result.data.unwrap()).unwrap(),
        expected_value,
        "Value after decrement does not match expected value"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_with_in_quiet_mode() {
    let key = "meta-arithmetic-increment-quiet-mode";
    let initial_value = "20";
    let expected_value = "21";

    let mut client = setup_client(&[key]).await;

    client.set(key, initial_value, None, None).await.unwrap();

    let result = client
        .meta_increment(key, true, None, None, None)
        .await
        .unwrap();

    assert!(result.is_none());

    let get_result = client
        .meta_get(key, false, None, Some(&["v"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        String::from_utf8(get_result.data.unwrap()).unwrap(),
        expected_value,
        "Value after increment does not match expected value"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_on_nonexistent_key_in_quiet_mode() {
    let key = "meta-increment-nonexistent-key-in-quiet-mode";

    let mut client = setup_client(&[key]).await;

    let incr_result = client.meta_increment(key, true, None, None, None).await;

    assert!(matches!(
        incr_result,
        Err(Error::Protocol(Status::NotFound))
    ));
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_with_opaque_flag() {
    let key = "meta-increment-opaque-flag";
    let initial_value = "5";

    let mut client = setup_client(&[key]).await;

    client.set(key, initial_value, None, None).await.unwrap();

    let opaque = "1001".as_bytes();
    let result = client
        .meta_increment(key, false, Some(opaque), None, None)
        .await
        .unwrap();

    // Verify the result
    assert!(result.is_some());
    let meta_value = result.unwrap();

    assert!(meta_value.data.is_none(), "Data should be empty");

    assert_eq!(
        meta_value.opaque_token.unwrap(),
        opaque,
        "Opaque token does not match"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_with_matching_cas_flag() {
    let key = "meta-increment-matching-cas-flag";
    let initial_value = "15";
    let expected_value = "16";

    let mut client = setup_client(&[key]).await;

    // Set the initial value with a specific CAS value
    client
        .meta_set(key, initial_value, Some(&["E2002"]))
        .await
        .unwrap();

    client
        .meta_increment(key, false, None, None, Some(&["C2002"]))
        .await
        .unwrap();

    let get_result = client
        .meta_get(key, false, None, Some(&["v"]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        String::from_utf8(get_result.data.unwrap()).unwrap(),
        expected_value,
        "Value after increment does not match expected value"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_with_mismatched_cas_flag() {
    let key = "meta-increment-mismatched-cas-flag";
    let initial_value = "15";

    let mut client = setup_client(&[key]).await;

    // Set the initial value with a specific CAS value
    client
        .meta_set(key, initial_value, Some(&["E2002"]))
        .await
        .unwrap();

    // Perform arithmetic increment with a mismatched CAS flag
    let mismatched_flags = ["C9999"];
    let mismatched_result = client
        .meta_increment(key, false, None, None, Some(&mismatched_flags))
        .await;

    // Expect an error due to CAS mismatch
    assert!(
        matches!(mismatched_result, Err(Error::Protocol(Status::Exists))),
        "Expected CAS mismatch error"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_with_invalid_flags() {
    let key = "meta-increment-invalid-flags";
    let initial_value = "10";

    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    // Perform arithmetic increment with invalid flag
    let flags = ["invalid_flag"];
    let result = client
        .meta_increment(key, false, None, None, Some(&flags))
        .await;

    // Expect an error due to invalid flag
    assert!(
        matches!(result, Err(Error::Protocol(Status::Error(_)))),
        "Expected error due to invalid flag"
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_with_specified_delta() {
    let key = "meta-increment-with-specified-delta";
    let initial_value = 50_u64;
    let delta = 50_u64;
    let expected_value = initial_value + delta;

    let meta_flags = ["v"];
    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    let incr_result = client
        .meta_increment(key, false, None, Some(delta), Some(&meta_flags))
        .await
        .unwrap();

    assert_eq!(
        String::from_utf8(incr_result.unwrap().data.unwrap()).unwrap(),
        expected_value.to_string()
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_decrement_with_specified_delta() {
    let key = "meta-decrement-with-specified-delta";
    let initial_value = 51_u64;
    let delta = 50_u64;
    let expected_value = initial_value - delta;

    let meta_flags = ["v"];
    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    let decr_result = client
        .meta_decrement(key, false, None, Some(delta), Some(&meta_flags))
        .await
        .unwrap();

    assert_eq!(
        String::from_utf8(decr_result.unwrap().data.unwrap()).unwrap(),
        expected_value.to_string()
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_overflows_with_proper_delta_when_incrementing_past_max_u64() {
    let key = "meta-increment-overflows-with-proper-delta-when-incrementing-past-max-u64";
    let initial_value = u64::MAX;
    let delta = 3_u64;
    let expected_value = 2;

    let meta_flags = ["v"];
    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    let incr_result = client
        .meta_increment(key, false, None, Some(delta), Some(&meta_flags))
        .await
        .unwrap();

    assert_eq!(
        String::from_utf8(incr_result.unwrap().data.unwrap()).unwrap(),
        expected_value.to_string()
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_decrement_does_not_decrement_below_zero() {
    let key = "meta-decrement-does-not-decrement-below-zero";
    let initial_value = 10_u64;
    let delta = 50_u64;
    let expected_value = 0;

    let meta_flags = ["v"];
    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    let decr_result = client
        .meta_decrement(key, false, None, Some(delta), Some(&meta_flags))
        .await
        .unwrap();

    assert_eq!(
        String::from_utf8(decr_result.unwrap().data.unwrap()).unwrap(),
        expected_value.to_string()
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_sets_new_key_with_n_and_j_flags() {
    let key = "meta-increment-n-and-j-flags";

    let mut client = setup_client(&[key]).await;

    let get_result = client
        .meta_get(key, false, None, Some(&["v"]))
        .await
        .unwrap();
    assert!(get_result.is_none());

    let flags = ["N9001", "J99", "v", "t"];
    let increment_result = client
        .meta_increment(key, false, None, None, Some(&flags))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        String::from_utf8(increment_result.data.unwrap()).unwrap(),
        "99"
    );
    assert_eq!(increment_result.ttl_remaining.unwrap(), 9001);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_prefers_explicit_parameters_over_meta_flags() {
    let key = "meta-increment-prefers-explicit-parameters-over-meta-flags";
    let initial_value = 50_u64;
    let delta = 50_u64;
    let expected_value = initial_value + delta;
    let opaque = "1337".as_bytes();

    let meta_flags = ["v", "D9001", "MD", "q", "O1001"];
    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    let incr_result = client
        .meta_increment(key, false, Some(opaque), Some(delta), Some(&meta_flags))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        String::from_utf8(incr_result.data.unwrap()).unwrap(),
        expected_value.to_string()
    );
    assert_eq!(incr_result.opaque_token.unwrap(), opaque);
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_uses_meta_flags_when_no_explicit_parameters_are_provided() {
    let key = "meta-increment-uses-meta-flags-when-no-explicit-parameters-are-provided";
    let initial_value = 50_u64;
    let expected_value = initial_value + 50;

    let meta_flags = ["v", "D50", "O1001"];
    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    let incr_result = client
        .meta_increment(key, false, None, None, Some(&meta_flags))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        String::from_utf8(incr_result.data.unwrap()).unwrap(),
        expected_value.to_string()
    );
    assert_eq!(incr_result.opaque_token.unwrap(), "1001".as_bytes());
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_ignores_mode_switch_flag() {
    let key = "meta-increment-ignores-mode-switch-flag";
    let initial_value = 50_u64;
    let delta = 50_u64;
    let expected_value = initial_value + delta;

    let meta_flags = ["v", "MD"];
    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    let incr_result = client
        .meta_increment(key, false, None, Some(delta), Some(&meta_flags))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        String::from_utf8(incr_result.data.unwrap()).unwrap(),
        expected_value.to_string()
    );
}

#[ignore = "Relies on a running memcached server"]
#[tokio::test]
#[parallel]
async fn test_meta_increment_raises_error_when_opaque_is_too_long() {
    let key = "meta-increment-raises-error-when-opaque-is-too-long";
    let initial_value = 50_u64;
    let opaque = "1".repeat(33);

    let mut client = setup_client(&[key]).await;

    client.meta_set(key, initial_value, None).await.unwrap();

    let incr_result = client
        .meta_increment(key, false, Some(opaque.as_bytes()), None, None)
        .await;

    assert!(matches!(
        incr_result,
        Err(Error::Protocol(Status::Error(ErrorKind::OpaqueTooLong)))
    ));
}
