use toxiproxy_rust::{
    client::Client as ToxiproxyClient,
    proxy::{Proxy, ProxyPack},
};

// use async_memcached::Client;

use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::Deref;
use std::sync::{atomic::AtomicUsize, Once, OnceLock};

// use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

static TOXIPROXY_INIT: Once = Once::new();
static TOXI_ADDR: OnceLock<SocketAddr> = OnceLock::new();
static PROXY_PORT: AtomicUsize = AtomicUsize::new(40000);

struct ProxyDrop {
    proxy: Proxy,
}

impl Deref for ProxyDrop {
    type Target = Proxy;

    fn deref(&self) -> &Self::Target {
        &self.proxy
    }
}

impl Drop for ProxyDrop {
    fn drop(&mut self) {
        self.proxy.delete().unwrap();
    }
}

// async fn create_clients() -> (Vec<ProxyDrop>, Client) {
// 	let (proxy, local_url) = create_proxies_and_configs();

// 	let client = Client::new(local_url).await.unwrap();

// 	(proxy, client)
// }

fn create_proxies_and_configs() -> (Vec<ProxyDrop>, String) {
    TOXIPROXY_INIT.call_once(|| {
        let mut toxiproxy_url = match std::env::var_os("TOXIPROXY_URL") {
            Some(v) => v.into_string().unwrap(),
            None => "http://127.0.0.1:8474".to_string(),
        };
        toxiproxy_url = toxiproxy_url.strip_prefix("http://").unwrap().to_string();
        // Create toxiproxy client and populate proxies
        let toxi_addr = toxiproxy_url.to_socket_addrs().unwrap().next().unwrap();
        let toxiproxy_client = ToxiproxyClient::new(toxi_addr);
        toxiproxy_client
            .all()
            .unwrap()
            .iter()
            .for_each(|(_, proxy)| proxy.delete().unwrap());
        TOXI_ADDR.get_or_init(|| toxi_addr);
    });

    let local_url = match std::env::var_os("NODE_LOCAL_CACHE") {
        Some(v) => v.into_string().unwrap(),
        None => "127.0.0.1:11211".to_string(), // use IPV4 so that it resolves to a single Server
    };
    let toxi_addr = TOXI_ADDR.get().unwrap();
    let local_port = PROXY_PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let toxic_local_addr = format!("{}:{}", toxi_addr.ip(), local_port);
    let proxies = vec![ProxyPack::new(
        format!("local-memcached-{}", local_port),
        toxic_local_addr.clone(),
        local_url.clone(),
    )];
    let toxiproxy_client = ToxiproxyClient::new(toxi_addr);
    assert!(toxiproxy_client.is_running());
    let proxies = toxiproxy_client.populate(proxies).unwrap();
    let proxies = proxies
        .into_iter()
        .map(|proxy| ProxyDrop { proxy })
        .collect();

    (proxies, toxic_local_addr)
}

// fn assert_connection_error(err: lib::errors::Error) {
//     assert!(matches!(
//         err.source(),
//         Source::Memcached(lib::proto::Error::Connection(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused))
// }

// fn assert_timeout_error(err: lib::errors::Error) {
//     assert!(matches!(
//         err.source(),
//         Source::Memcached(lib::proto::Error::IO(e)) if e.kind() == std::io::ErrorKind::TimedOut))
// }

// fn assert_io_error(err: lib::errors::Error) {
//     assert!(matches!(
//         err.source(),
//         Source::Memcached(lib::proto::Error::IO(_))
//     ))
// }

// fn assert_tko_error(err: lib::errors::Error) {
//     assert!(matches!(
//         err.source(),
//         Source::Memcached(lib::proto::Error::TKO(_))
//     ))
// }

#[cfg(test)]
mod tests {
    use super::*;

    // #[ignore = "Relies on a running memcached server and toxiproxy service"]
    #[test]
    fn test_set_multi_errors_with_toxiproxy_via_with_down() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let (proxies, toxic_local_addr) = create_proxies_and_configs();

        let toxic_local_url = "tcp://".to_string() + &toxic_local_addr;
        let toxic_proxy = &proxies[0];
        let keys = vec!["key1", "key2", "key3"];
        let values = vec!["value1", "value2", "value3"];

        let mut clean_client = rt.block_on(async {
            async_memcached::Client::new("tcp://127.0.0.1:11211".to_string())
                .await
                .unwrap()
        });

        let mut toxic_client =
            rt.block_on(async { async_memcached::Client::new(toxic_local_url).await.unwrap() });

        let result = rt.block_on(async {
            clean_client
                .set_multi(keys.clone(), values.clone(), None, None)
                .await
        });

        assert!(result.is_ok());

        let _ = toxic_proxy.with_down(|| {
            rt.block_on(async {
                let result = toxic_client
                    .set_multi(keys.clone(), values.clone(), None, None)
                    .await;
                println!("result: {:?}", result);
                assert_eq!(
                    result,
                    Err(async_memcached::Error::Io(
                        std::io::ErrorKind::UnexpectedEof.into()
                    ))
                );
            });
        });
    }

    // #[test]
    // fn test_set_multi_errors_with_toxiproxy_via_limit_data() {
    // 	let rt = tokio::runtime::Builder::new_multi_thread()
    // 		.enable_all()
    // 		.build()
    // 		.unwrap();

    // 	let (proxies, toxic_local_addr) = create_proxies_and_configs();

    // 	let toxic_local_url = "tcp://".to_string() + &toxic_local_addr;
    // 	let toxic_proxy = &proxies[0];
    // 	let keys = vec!["newkey1", "newkey2", "newkey3"];
    // 	let values = vec!["value1", "value2", "value3"];

    // 	let mut clean_client = rt.block_on(async {
    // 		async_memcached::Client::new("tcp://127.0.0.1:11211".to_string()).await.unwrap()
    // 	});

    // 	let mut toxic_client = rt.block_on(async {
    // 		async_memcached::Client::new(toxic_local_url).await.unwrap()
    // 	});

    // 	let result = rt.block_on(async {
    // 		clean_client.set_multi(&keys, &values, None, None).await
    // 	});

    // 	assert!(result.is_ok());

    // 	let downstream = format!("set {} 0 0 1\r\n{}\r\n and then some more stuff", keys.first().unwrap(), values.first().unwrap());
    // 	let byte_limit = format!("set {} 0 0 1\r\n{}\r\n", keys.first().unwrap(), values.first().unwrap()).as_bytes().len() + 1;
    // 	println!("byte_limit: {}", byte_limit);

    // 	let _ = toxic_proxy.with_limit_data(downstream, byte_limit as u32, 1.0).apply (|| {
    // 		rt.block_on(async {
    // 		let result = toxic_client.set_multi(&keys, &values, None, None).await;
    // 		println!("result: {:?}", result);
    // 		assert_eq!(result, Err(async_memcached::Error::Io(std::io::ErrorKind::UnexpectedEof.into())));
    // 		});
    // 	});
    // }
}
