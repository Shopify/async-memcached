use super::{Memcached, TestProcess};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::Command;
use std::time::Duration;

const PROXY_NAME: &str = "memcached";
const API_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ToxicMemcached {
    pub proxy: Proxy,
    pub memcached: Memcached,
    _process: TestProcess,
}

impl ToxicMemcached {
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
            request(address, "GET", "/version", None, Duration::from_millis(200)).ok()
        });
        let proxy = Proxy::create(address, &memcached.address());
        assert_ne!(proxy.listen, "127.0.0.1:0");
        Self {
            proxy,
            memcached,
            _process: process,
        }
    }

    pub fn dsn(&self) -> String {
        format!("tcp://{}", self.proxy.listen)
    }
}

/// A minimal client for one proxy on the Toxiproxy HTTP API.
///
/// See <https://github.com/Shopify/toxiproxy#http-api>.
pub struct Proxy {
    api: SocketAddr,
    pub listen: String,
}

impl Proxy {
    fn create(api: SocketAddr, upstream: &str) -> Self {
        let body = json!({
            "name": PROXY_NAME,
            "listen": "127.0.0.1:0",
            "upstream": upstream,
            "enabled": true,
        });
        let response = request(api, "POST", "/proxies", Some(&body), API_TIMEOUT)
            .expect("Cannot create the test proxy");
        let proxy: Value =
            serde_json::from_str(&response).expect("Toxiproxy returned an invalid proxy");
        let listen = proxy["listen"]
            .as_str()
            .expect("Toxiproxy did not return a listen address")
            .to_owned();
        Self { api, listen }
    }

    /// Runs `closure` with the proxy disabled, then enables it again.
    pub fn with_down(&self, closure: impl FnOnce()) -> Result<(), String> {
        self.set_enabled(false)?;
        closure();
        self.set_enabled(true)
    }

    /// Runs `closure` with the registered toxics, then removes all toxics.
    pub fn apply(&self, closure: impl FnOnce()) -> Result<(), String> {
        closure();
        // The fixture owns this Toxiproxy process, so a reset affects only this proxy.
        request(self.api, "POST", "/reset", None, API_TIMEOUT).map(drop)
    }

    pub fn with_limit_data(&self, stream: &str, bytes: u32, toxicity: f32) -> &Self {
        self.add_toxic("limit_data", stream, toxicity, json!({ "bytes": bytes }))
    }

    pub fn with_slicer(
        &self,
        stream: &str,
        average_size: u32,
        size_variation: u32,
        delay: u32,
        toxicity: f32,
    ) -> &Self {
        let attributes = json!({
            "average_size": average_size,
            "size_variation": size_variation,
            "delay": delay,
        });
        self.add_toxic("slicer", stream, toxicity, attributes)
    }

    pub fn with_timeout(&self, stream: &str, timeout: u32, toxicity: f32) -> &Self {
        self.add_toxic("timeout", stream, toxicity, json!({ "timeout": timeout }))
    }

    fn add_toxic(&self, kind: &str, stream: &str, toxicity: f32, attributes: Value) -> &Self {
        let body = json!({
            "name": format!("{kind}_{stream}"),
            "type": kind,
            "stream": stream,
            "toxicity": toxicity,
            "attributes": attributes,
        });
        let path = format!("/proxies/{PROXY_NAME}/toxics");
        if let Err(error) = request(self.api, "POST", &path, Some(&body), API_TIMEOUT) {
            panic!("Cannot create the {} toxic: {}", kind, error);
        }
        self
    }

    fn set_enabled(&self, enabled: bool) -> Result<(), String> {
        let path = format!("/proxies/{PROXY_NAME}");
        let body = json!({ "enabled": enabled });
        request(self.api, "POST", &path, Some(&body), API_TIMEOUT).map(drop)
    }
}

/// Sends one HTTP/1.0 request and returns the body of a 2xx response.
fn request(
    api: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&Value>,
    timeout: Duration,
) -> Result<String, String> {
    let body = body.map(Value::to_string).unwrap_or_default();
    let mut stream = TcpStream::connect_timeout(&api, timeout).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    // HTTP/1.0 makes the server close the connection after the response, so no chunked bodies.
    write!(
        stream,
        "{method} {path} HTTP/1.0\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .map_err(|e| e.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| e.to_string())?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("{method} {path} returned an incomplete response"))?;
    let status = head.lines().next().unwrap_or_default();
    match status.split(' ').nth(1) {
        Some(code) if code.starts_with('2') => Ok(body.to_owned()),
        _ => Err(format!("{method} {path} returned {status:?}: {body}")),
    }
}
