// tests/integration_test.rs

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use rand::{distributions::Alphanumeric, Rng};
use std::fs;
use std::process::{Child, Command};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn start_server(snapshot_dir: &str) -> Child {
    Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("--snapshot-dir")
        .arg(snapshot_dir)
        .arg("--port")
        .arg("11213")
        .spawn()
        .expect("failed to start server")
}

async fn stop_server(child: &mut Child) {
    let pid = Pid::from_raw(child.id() as i32);
    signal::kill(pid, Signal::SIGTERM).expect("failed to send SIGTERM");
    child.wait().expect("failed to wait on child");
}

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn create_temp_dir() -> String {
    let random_string = generate_random_string(10);
    let dir = format!("/tmp/{}", random_string);
    fs::create_dir(&dir).expect("failed to create temp dir");
    dir
}

fn remove_temp_dir(dir: &str) {
    fs::remove_dir_all(dir).expect("failed to remove temp dir");
}

#[tokio::test]
async fn test_set_and_get() {
    let snapshot_dir = create_temp_dir();

    // Start the server
    let mut server = start_server(&snapshot_dir).await;
    tokio::time::sleep(Duration::from_secs(2)).await; // Wait for the server to start

    let mut socket = TcpStream::connect("127.0.0.1:11213").await.unwrap();

    // Test SET command
    socket
        .write_all(b"set key 0 0 5\r\nvalue\r\n")
        .await
        .unwrap();
    let mut buf = vec![0; 128];
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"STORED\r\n");

    // Test GET command
    socket.write_all(b"get key\r\n").await.unwrap();
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"VALUE key 0 5\r\nvalue\r\nEND\r\n");

    // Stop the server
    stop_server(&mut server).await;

    // Restart the server
    let mut server = start_server(&snapshot_dir).await;
    tokio::time::sleep(Duration::from_secs(2)).await; // Wait for the server to start

    let mut socket = TcpStream::connect("127.0.0.1:11213").await.unwrap();

    // Test GET command again to ensure data is not lost
    socket.write_all(b"get key\r\n").await.unwrap();
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"VALUE key 0 5\r\nvalue\r\nEND\r\n");

    // Clean up
    stop_server(&mut server).await;
    remove_temp_dir(&snapshot_dir);
}
