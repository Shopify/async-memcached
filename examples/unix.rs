use async_memcached::Client;

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
        match client.get_many(keys).await {
            Ok(values) => println!("got values: {:?}", values),
            Err(status) => println!("got status during get_many: {:?}", status),
        }
    
        match client.add("add_key", "bar", None, None).await {
            Ok(()) => println!("added 'add_key' successfully"),
            Err(status) => println!("got status during 'add_key' add: {:?}", status),
        }
    
        match client.add("add_key", "bar", None, None).await {
            Ok(()) => panic!("should not be able to add 'add_key' again"),
            Err(status) => println!("duplicate add of 'add_key' fails as expected with: {:?}", status),
        }
    
        match client.delete("foo").await {
            Ok(()) => println!("deleted 'foo' successfully"),
            Err(status) => println!("got status during 'foo' delete: {:?}", status),
        }
    
        match client.delete_no_reply("add_key").await {
            Ok(()) => println!("deleted_no_reply 'add_key' successfully"),
            Err(status) => println!("got status during 'add_key' deleted_no_reply: {:?}", status),
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
}
