use super::{Memcached, TestProcess};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::time::Duration;
use toxiproxy_rust::{
    client::Client,
    proxy::{Proxy, ProxyPack},
};

pub struct ToxicMemcached {
    pub proxy: Proxy,
    pub memcached: Memcached,
    _process: TestProcess,
}

impl ToxicMemcached {
    // The Toxiproxy HTTP client is synchronous. Create this fixture outside a Tokio runtime.
    pub fn new() -> Self {
        let memcached = Memcached::tcp();
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        let mut process = TestProcess::new("toxiproxy");
        let binary = std::env::var_os("TOXIPROXY_BIN").unwrap_or_else(|| "toxiproxy-server".into());
        let mut command = Command::new(binary);
        command.args([
            "-host",
            "127.0.0.1",
            "-port",
            &address.port().to_string(),
            "-seed",
            "1",
        ]);
        drop(reservation);
        process.spawn(&mut command);
        process.wait_for("the Toxiproxy HTTP API", || {
            let timeout = Duration::from_millis(200);
            let mut stream = TcpStream::connect_timeout(&address, timeout).ok()?;
            stream.set_read_timeout(Some(timeout)).ok()?;
            stream.set_write_timeout(Some(timeout)).ok()?;
            stream
                .write_all(b"GET /version HTTP/1.0\r\nHost: localhost\r\n\r\n")
                .ok()?;
            let mut response = String::new();
            stream.read_to_string(&mut response).ok()?;
            response
                .starts_with("HTTP/1.0 200")
                .then_some(())
                .or_else(|| response.starts_with("HTTP/1.1 200").then_some(()))
        });
        let client = Client::new(address);
        let proxy = client
            .populate(vec![ProxyPack::new(
                "memcached".into(),
                "127.0.0.1:0".into(),
                memcached.address(),
            )])
            .expect("Cannot create the test proxy")
            .pop()
            .unwrap();
        assert_ne!(proxy.proxy_pack.listen, "127.0.0.1:0");
        Self {
            proxy,
            memcached,
            _process: process,
        }
    }

    pub fn dsn(&self) -> String {
        format!("tcp://{}", self.proxy.proxy_pack.listen)
    }
}
