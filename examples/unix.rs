use async_memcached::{AsciiProtocol, Client};

#[tokio::main]
async fn main() {
    let mut client = Client::new("unix:///tmp/memcached.sock")
        .await
        .expect("failed to create client");

    match client.get("foo").await {
        Ok(value) => println!("got value for 'foo': {:?}", value),
        Err(status) => println!("got status during 'foo' get: {:?}", status),
    }

    match client.set("foo", "might do popeyes", Some(5), None).await {
        Ok(()) => println!("set 'foo' successfully"),
        Err(status) => println!("got status during 'foo' set: {:?}", status),
    }

    match client.set("bar", "barvalue", Some(5), None).await {
        Ok(()) => println!("set 'bar' successfully"),
        Err(status) => println!("got status during 'bar' set: {:?}", status),
    }

    let keys = &["foo", "bar"];

    match client.get_multi(keys).await {
        Ok(values) => println!("got values: {:?}", values),
        Err(status) => println!("got status during get_many: {:?}", status),
    }

    match client.add("add_key", "bar", None, None).await {
        Ok(()) => println!("added 'add_key' successfully"),
        Err(status) => println!("got status during 'add_key' add: {:?}", status),
    }

    match client.add("add_key", "bar", None, None).await {
        Ok(()) => panic!("should not be able to add 'add_key' again"),
        Err(status) => println!(
            "duplicate add of 'add_key' fails as expected with: {:?}",
            status
        ),
    }

    let kv = vec![("multi_key1", "bar"), ("multi_key2", "bar")];

    match client.add_multi(&kv, None, None).await {
        Ok(_) => println!("added 'multi_key1' and 'multi_key2' successfully"),
        Err(status) => println!("got status during add_multi: {:?}", status),
    }

    match client.set_multi(&kv, None, None).await {
        Ok(_) => println!("set_multi 'multi_key1' and 'multi_key2' successfully"),
        Err(status) => println!("got status during set_multi: {:?}", status),
    }

    match client.add_multi(&kv, None, None).await {
        Ok(results) => match results.get(&"multi_key1") {
            Some(result) => match result {
                Ok(_) => panic!("should not be able to add 'multi_key1' again"),
                Err(status) => println!(
                    "got status during 'multi_key1' add: {:?}, as expected",
                    status
                ),
            },
            None => panic!("Something else went wrong with add_multi"),
        },
        Err(status) => println!(
            "duplicate add of 'multi_key1' or 'multi_key2' fails as expected with: {:?}",
            status
        ),
    }

    match client.delete("foo").await {
        Ok(()) => println!("deleted 'foo' successfully"),
        Err(status) => println!("got status during 'foo' delete: {:?}", status),
    }

    match client.delete_no_reply("add_key").await {
        Ok(()) => println!("deleted_no_reply 'add_key' successfully"),
        Err(status) => println!("got status during 'add_key' deleted_no_reply: {:?}", status),
    }

    let amount = 1;
    let increment_key = "increment_key";

    client
        .set(increment_key, "0", None, None)
        .await
        .unwrap_or_else(|_| panic!("failed to set {}", increment_key));

    match client.increment(increment_key, amount).await {
        Ok(value) => println!(
            "incremented {} by {} successfully, to {}",
            increment_key, amount, value
        ),
        Err(status) => println!(
            "got status during increment on key {}: {:?}",
            increment_key, status
        ),
    }

    match client.increment_no_reply(increment_key, amount).await {
        Ok(()) => println!(
            "incremented_no_reply {} by {} successfully",
            increment_key, amount
        ),
        Err(status) => println!(
            "got status during increment_no_reply on {}: {:?}",
            increment_key, status
        ),
    }

    let decrement_key = "decrement_key";

    client
        .set(decrement_key, "10", None, None)
        .await
        .unwrap_or_else(|_| panic!("failed to set {}", decrement_key));

    match client.decrement(decrement_key, amount).await {
        Ok(value) => println!(
            "decremented {} by {} successfully, to {}",
            decrement_key, amount, value
        ),
        Err(status) => println!(
            "got status during decrement on {}: {:?}",
            decrement_key, status
        ),
    }

    match client.decrement_no_reply(decrement_key, amount).await {
        Ok(()) => println!(
            "decremented_no_reply {} by {} successfully",
            decrement_key, amount
        ),
        Err(status) => println!(
            "got status during decrement_no_reply on {}: {:?}",
            decrement_key, status
        ),
    }

    match client.version().await {
        Ok(version) => println!("version {}", version),
        Err(status) => println!("got status during 'foo' delete: {:?}", status),
    }

    match client.stats().await {
        Ok(stats) => {
            println!("got {} stat entries!", stats.len());
            println!("curr items: {:?}", stats.get("curr_items"));
        }
        Err(e) => println!("error while getting stats: {:?}", e),
    }

    match client.flush_all().await {
        Ok(_) => println!("flushed all successfully"),
        Err(status) => println!("got status during flush_all: {:?}", status),
    }

    println!("✅ All examples completed successfully.");
}
