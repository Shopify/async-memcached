use async_memcached::Client;

#[tokio::main]
async fn main() {
    let mut client = Client::new("localhost:11211")
        .await
        .expect("failed to create client");

    match client.get("foo").await {
        Ok(value) => println!("got value for 'foo': {:?}", value),
        Err(status) => println!("got status during 'foo' get: {:?}", status),
    }

    match client
        .set("foo", "fo might do popeyes ovalue", 5, None)
        .await
    {
        Ok(()) => println!("set 'foo' successfully"),
        Err(status) => println!("got status during 'foo' set: {:?}", status),
    }

    match client.set("bar", "barvalue", 5, None).await {
        Ok(()) => println!("set 'bar' successfully"),
        Err(status) => println!("got status during 'bar' set: {:?}", status),
    }

    let keys = &["foo", "bar"];
    match client.get_many(keys).await {
        Ok(values) => println!("got values: {:?}", values),
        Err(status) => println!("got status during get_many: {:?}", status),
    }
}
