use async_memcached::{AsciiProtocol, Client};

use toxiproxy_rust::{
    client::Client as ToxiproxyClient,
    proxy::{Proxy, ProxyPack},
};

use std::net::{SocketAddr, ToSocketAddrs};
use std::ops::Deref;
use std::sync::{atomic::AtomicUsize, Once, OnceLock};

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

fn create_proxy_and_config() -> (ProxyDrop, String) {
    TOXIPROXY_INIT.call_once(|| {
        let mut toxiproxy_host = match std::env::var_os("TOXIPROXY_HOST") {
            Some(v) => v.into_string().unwrap(),
            None => "http://127.0.0.1".to_string(),
        };
        if let Some(stripped) = toxiproxy_host.strip_prefix("http://") {
            toxiproxy_host = stripped.to_string();
        }

        let toxiproxy_port = match std::env::var_os("TOXIPROXY_PORT") {
            Some(v) => v.into_string().unwrap(),
            None => "8474".to_string(),
        };

        let toxiproxy_url = format!("{}:{}", toxiproxy_host, toxiproxy_port);

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

    let mut local_host = match std::env::var_os("MEMCACHED_HOST") {
        Some(v) => v.into_string().unwrap(),
        None => "http://127.0.0.1".to_string(), // use IPV4 so that it resolves to a single Server
    };
    if let Some(stripped) = local_host.strip_prefix("http://") {
        local_host = stripped.to_string();
    }

    let local_memcached_port = match std::env::var_os("MEMCACHED_PORT") {
        Some(v) => v.into_string().unwrap(),
        None => "11211".to_string(),
    };

    let local_url = format!("{}:{}", local_host, local_memcached_port);

    let local_port = PROXY_PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let toxi_addr = TOXI_ADDR.get().unwrap();
    let toxic_local_addr = format!("{}:{}", toxi_addr.ip(), local_port);

    let proxy = ProxyPack::new(
        format!("local-memcached-{}", local_port),
        toxic_local_addr.clone(),
        local_url.clone(),
    );

    let toxiproxy_client = ToxiproxyClient::new(toxi_addr);
    assert!(toxiproxy_client.is_running());

    let proxy = toxiproxy_client.populate(vec![proxy]).unwrap();
    let proxy = proxy
        .into_iter()
        .map(|proxy| ProxyDrop { proxy })
        .next()
        .unwrap();

    (proxy, toxic_local_addr)
}

async fn setup_clean_client() -> Client {
    let mut local_host = match std::env::var_os("MEMCACHED_HOST") {
        Some(v) => v.into_string().unwrap(),
        None => "http://127.0.0.1".to_string(),
    };
    if let Some(stripped) = local_host.strip_prefix("http://") {
        local_host = stripped.to_string();
    }

    let local_memcached_port = match std::env::var_os("MEMCACHED_PORT") {
        Some(v) => v.into_string().unwrap(),
        None => "11211".to_string(),
    };

    Client::new(format!("tcp://{}:{}", local_host, local_memcached_port,))
        .await
        .unwrap()
}

async fn setup_toxic_client(toxic_local_url: &String) -> Client {
    Client::new(toxic_local_url).await.unwrap()
}

fn setup_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

async fn clear_keys(client: &mut Client, keys: &[&str]) {
    for key in keys {
        let _ = client.delete(key).await;
        let result = client.get(key).await;
        assert_eq!(result, Ok(None));
    }
}

fn setup_runtime_and_clients(
    toxic_local_url: &String,
    keys: &[&str],
) -> (tokio::runtime::Runtime, Client, Client) {
    let rt = setup_runtime();
    let mut clean_client = rt.block_on(setup_clean_client());
    let toxic_client = rt.block_on(setup_toxic_client(toxic_local_url));

    rt.block_on(clear_keys(&mut clean_client, keys));

    (rt, clean_client, toxic_client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore = "Relies on a running memcached server and toxiproxy service"]
    #[test]
    fn test_set_multi_succeeds_with_clean_client() {
        let rt = setup_runtime();

        let keys = vec!["clean-key1", "clean-key2", "clean-key3"];
        let values = vec!["value1", "value2", "value3"];
        let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values).collect();

        let mut clean_client = rt.block_on(setup_clean_client());

        rt.block_on(clear_keys(&mut clean_client, &keys));

        let result = rt.block_on(async { clean_client.set_multi(&kv, None, None).await });

        assert!(result.is_ok());
    }

    #[ignore = "Relies on a running memcached server and toxiproxy service"]
    #[test]
    fn test_set_multi_errors_with_toxic_client_via_with_down() {
        let keys = vec!["with-down-key1", "with-down-key2", "with-down-key3"];
        let values = vec!["value1", "value2", "value3"];

        let (toxic_proxy, toxic_local_addr) = create_proxy_and_config();
        let toxic_local_url = "tcp://".to_string() + &toxic_local_addr;

        let (rt, _, mut toxic_client) = setup_runtime_and_clients(&toxic_local_url, &keys);

        let kv: Vec<(&str, &str)> = keys.clone().into_iter().zip(values).collect();

        let _ = toxic_proxy.with_down(|| {
            rt.block_on(async {
                let result = toxic_client.set_multi(&kv, None, None).await;
                assert_eq!(
                    result,
                    Err(async_memcached::Error::Io(
                        std::io::ErrorKind::UnexpectedEof.into()
                    ))
                );
            });
        });
    }

    #[ignore = "Relies on a running memcached server and toxiproxy service"]
    #[test]
    fn test_set_multi_errors_on_upstream_with_toxic_client_via_limit_data() {
        let keys = vec!["upstream-key1", "upstream-key2", "upstream-key3"];
        let values = vec!["value1", "value2", "value3"];

        let (toxic_proxy, toxic_local_addr) = create_proxy_and_config();
        let toxic_local_url = "tcp://".to_string() + &toxic_local_addr;

        let (rt, mut clean_client, mut toxic_client) =
            setup_runtime_and_clients(&toxic_local_url, &keys);

        let multiset_command =
            keys.iter()
                .zip(values.iter())
                .fold(String::new(), |mut acc, (key, value)| {
                    acc.push_str(&format!("set {} 0 0 {}\r\n{}\r\n", key, value.len(), value));
                    acc
                });

        // Simulate a network error happening when the client makes a request to the server.  Only part of the request is received by the server.
        // In this case, the server can only cache values for the keys with complete commands.

        let byte_limit = multiset_command.len() - 10; // First two commands should be intact, last one cut off

        let _ = toxic_proxy
            .with_limit_data("upstream".into(), byte_limit as u32, 1.0)
            .apply(|| {
                rt.block_on(async {
                    let kv: Vec<(&str, &str)> =
                        keys.clone().into_iter().zip(values.clone()).collect();
                    let result = toxic_client.set_multi(&kv, None, None).await;

                    assert_eq!(
                        result,
                        Err(async_memcached::Error::Io(
                            std::io::ErrorKind::UnexpectedEof.into()
                        ))
                    );
                });
            });

        // Use a clean client to check that the first two keys were stored and last was not
        let get_result = rt.block_on(async { clean_client.get("upstream-key1").await });
        assert!(matches!(
            std::str::from_utf8(
                &get_result
                    .expect("should have unwrapped a Result")
                    .expect("should have unwrapped an Option")
                    .data
                    .unwrap()
            )
            .expect("failed to parse string from bytes"),
            "value1"
        ));

        let get_result = rt.block_on(async { clean_client.get("upstream-key2").await });
        assert!(matches!(
            std::str::from_utf8(
                &get_result
                    .expect("should have unwrapped a Result")
                    .expect("should have unwrapped an Option")
                    .data
                    .unwrap()
            )
            .expect("failed to parse string from bytes"),
            "value2"
        ));

        let get_result = rt.block_on(async { clean_client.get("upstream-key3").await });
        assert_eq!(get_result, Ok(None));
    }

    #[ignore = "Relies on a running memcached server and toxiproxy service"]
    #[test]
    fn test_set_multi_errors_on_downstream_with_toxic_client_via_limit_data() {
        let keys = vec!["downstream-key1", "downstream-key2", "downstream-key3"];
        let values = vec!["value1", "value2", "value3"];

        let (toxic_proxy, toxic_local_addr) = create_proxy_and_config();
        let toxic_local_url = "tcp://".to_string() + &toxic_local_addr;

        let (rt, mut clean_client, mut toxic_client) =
            setup_runtime_and_clients(&toxic_local_url, &keys);

        // Simulate a network error happening when the server responds back to the client.  A complete response is received for the first key but then
        // the connection is closed before the other responses are received.  Regardless, the server should still cache all the data.
        let byte_limit = "STORED\r\n".len() + 1;

        let _ = toxic_proxy
            .with_limit_data("downstream".into(), byte_limit as u32, 1.0)
            .apply(|| {
                rt.block_on(async {
                    let kv: Vec<(&str, &str)> =
                        keys.clone().into_iter().zip(values.clone()).collect();

                    let set_result = toxic_client.set_multi(&kv, None, None).await;

                    assert_eq!(
                        set_result,
                        Err(async_memcached::Error::Io(
                            std::io::ErrorKind::UnexpectedEof.into()
                        ))
                    );
                });
            });

        // Use a clean client to check that all values were cached by the server despite the interrupted server response.
        for (key, _expected_value) in keys.iter().zip(values.iter()) {
            let get_result = rt.block_on(async { clean_client.get(key).await });
            assert!(matches!(
                std::str::from_utf8(
                    &get_result
                        .expect("should have unwrapped a Result")
                        .expect("should have unwrapped an Option")
                        .data
                        .unwrap()
                )
                .expect("failed to parse string from bytes"),
                _expected_value
            ));
        }
    }
}
