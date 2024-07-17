use async_memcached::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::new(vec!["unix:///tmp/memcached.sock"])
        .await
        .expect("failed to create client");

    match client.set("foo", "might do popeyes", Some(5), None).await {
        Ok(()) => println!("set 'foo' successfully"),
        Err(status) => println!("got status during 'foo' set: {:?}", status),
    }

    match client.get("foo").await {
        Ok(value) => println!("got value for 'foo': {:?}", value),
        Err(status) => println!("got status during 'foo' get: {:?}", status),
    }
}
