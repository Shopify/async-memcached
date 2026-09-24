#![allow(dead_code)]

use async_memcached::Client;
use std::fs::{self, DirBuilder, File};
use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

pub mod toxiproxy;

pub const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
pub const TEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_KEY_LENGTH: usize = 250;
pub const LARGE_PAYLOAD_SIZE: usize = 1024 * 1024 - 310;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        // Short paths also fit the Unix socket limit on macOS.
        let path = PathBuf::from(format!(
            "/tmp/amc-{}-{:016x}",
            std::process::id(),
            rand::random::<u64>()
        ));
        DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .expect("Cannot create the test directory");
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub struct TestProcess {
    child: Option<Child>,
    directory: TestDirectory,
    name: &'static str,
}

impl TestProcess {
    pub fn new(name: &'static str) -> Self {
        Self {
            child: None,
            directory: TestDirectory::new(),
            name,
        }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.directory.0.join(name)
    }

    pub fn spawn(&mut self, command: &mut Command) {
        assert!(self.child.is_none(), "The test process is already active");
        let log = File::create(self.path("server.log")).expect("Cannot create the server log");
        self.child = Some(
            command
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap_or_else(|error| {
                    panic!("Cannot start {}: {error}. See TESTING.md.", self.name)
                }),
        );
    }

    pub fn wait_for<T>(&mut self, description: &str, mut probe: impl FnMut() -> Option<T>) -> T {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                panic!("{} exited with {status}:\n{}", self.name, self.log());
            }
            if let Some(value) = probe() {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "Timeout: {description}. {} log:\n{}",
                self.name,
                self.log()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn log(&self) -> String {
        fs::read_to_string(self.path("server.log")).unwrap_or_default()
    }
}

impl Drop for TestProcess {
    fn drop(&mut self) {
        self.stop();
        if std::thread::panicking() {
            eprintln!("{} log:\n{}", self.name, self.log());
        }
    }
}

#[derive(Clone)]
enum Endpoint {
    Tcp(u16),
    Unix(PathBuf),
}

pub struct Memcached {
    process: TestProcess,
    endpoint: Endpoint,
}

impl Memcached {
    pub fn tcp() -> Self {
        Self::start(Endpoint::Tcp(0))
    }

    pub fn unix() -> Self {
        let process = TestProcess::new("memcached");
        let endpoint = Endpoint::Unix(process.path("cache.sock"));
        let mut server = Self { process, endpoint };
        server.spawn();
        server
    }

    fn start(endpoint: Endpoint) -> Self {
        let mut server = Self {
            process: TestProcess::new("memcached"),
            endpoint,
        };
        server.spawn();
        server
    }

    fn spawn(&mut self) {
        static USER: OnceLock<String> = OnceLock::new();
        let user = USER.get_or_init(|| {
            let output = Command::new("id").arg("-un").output().unwrap();
            assert!(output.status.success(), "Cannot identify the test user");
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        });
        let binary = std::env::var_os("MEMCACHED_BIN").unwrap_or_else(|| "memcached".into());
        let mut command = Command::new(binary);
        command.args([
            "-u", user, "-U", "0", "-t", "1", "-c", "1024", "-m", "128", "-I", "1m",
        ]);
        let port_file = self.process.path("ports");
        match &self.endpoint {
            Endpoint::Tcp(port) => {
                let port = if *port == 0 {
                    "-1".into()
                } else {
                    port.to_string()
                };
                command.args(["-l", "127.0.0.1", "-p", &port]);
                command.env("MEMCACHED_PORT_FILENAME", &port_file);
                let _ = fs::remove_file(&port_file);
            }
            Endpoint::Unix(path) => {
                let _ = fs::remove_file(path);
                command.arg("-s").arg(path).args(["-a", "700"]);
            }
        }
        self.process.spawn(&mut command);
        if matches!(self.endpoint, Endpoint::Tcp(0)) {
            // Memcached selects the port itself, so no port reservation race exists.
            let port = self.process.wait_for("the memcached port file", || {
                fs::read_to_string(&port_file)
                    .ok()?
                    .lines()
                    .find_map(|line| line.strip_prefix("TCP INET: ")?.trim().parse::<u16>().ok())
            });
            self.endpoint = Endpoint::Tcp(port);
        }
        let endpoint = self.endpoint.clone();
        let version = self
            .process
            .wait_for("a memcached version response", || probe_version(&endpoint));
        let parts: Vec<u32> = version
            .trim()
            .split('.')
            .take(3)
            .map(|part| part.parse().unwrap_or(0))
            .collect();
        assert!(
            parts.as_slice() >= [1, 6, 40].as_slice(),
            "Memcached 1.6.40 or newer is required. Found {}. See TESTING.md.",
            version
        );
    }

    pub fn address(&self) -> String {
        match &self.endpoint {
            Endpoint::Tcp(port) => format!("127.0.0.1:{port}"),
            Endpoint::Unix(_) => panic!("A Unix socket has no TCP address"),
        }
    }

    pub fn dsn(&self) -> String {
        match &self.endpoint {
            Endpoint::Tcp(_) => format!("tcp://{}", self.address()),
            Endpoint::Unix(path) => format!("unix://{}", path.display()),
        }
    }

    pub async fn client(&self) -> Client {
        within(Client::new(self.dsn()))
            .await
            .expect("Cannot connect to the test memcached process")
    }

    pub fn stop(&mut self) {
        self.process.stop();
    }

    pub fn restart(&mut self) {
        self.stop();
        self.spawn();
    }
}

fn probe_version(endpoint: &Endpoint) -> Option<String> {
    fn read_version(mut stream: impl std::io::Read + Write) -> Option<String> {
        stream.write_all(b"version\r\n").ok()?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).ok()?;
        Some(line.strip_prefix("VERSION ")?.trim().to_owned())
    }
    let timeout = Duration::from_millis(200);
    match endpoint {
        Endpoint::Tcp(port) => {
            let address = format!("127.0.0.1:{port}").parse().ok()?;
            let stream = TcpStream::connect_timeout(&address, timeout).ok()?;
            stream.set_read_timeout(Some(timeout)).ok()?;
            stream.set_write_timeout(Some(timeout)).ok()?;
            read_version(stream)
        }
        Endpoint::Unix(path) => {
            let stream = UnixStream::connect(path).ok()?;
            stream.set_read_timeout(Some(timeout)).ok()?;
            stream.set_write_timeout(Some(timeout)).ok()?;
            read_version(stream)
        }
    }
}

pub struct TestClient {
    client: Client,
    _server: Memcached,
}

impl TestClient {
    pub async fn new() -> Self {
        let server = Memcached::tcp();
        let client = server.client().await;
        Self {
            client,
            _server: server,
        }
    }
}

impl Deref for TestClient {
    type Target = Client;

    fn deref(&self) -> &Client {
        &self.client
    }
}

impl DerefMut for TestClient {
    fn deref_mut(&mut self) -> &mut Client {
        &mut self.client
    }
}

pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

pub fn run(future: impl Future<Output = ()>) {
    runtime().block_on(within(future));
}

pub async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .expect("The integration operation exceeded its 30-second deadline")
}

pub async fn wait_for_expiration(client: &mut Client, key: &str) {
    use async_memcached::AsciiProtocol;
    within(async {
        loop {
            if client.get(key).await.unwrap().is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
}
