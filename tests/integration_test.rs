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
        .arg("--shards")
        .arg("2")
        .arg("--port")
        .arg("11213")
        .spawn()
        .expect("failed to start server")
}

async fn stop_server(child: &mut Child, signal: Signal) {
    let pid = Pid::from_raw(child.id() as i32);
    signal::kill(pid, signal).expect("failed to send signal");
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

    // Setup: start the server
    let mut server = start_server(&snapshot_dir).await;
    tokio::time::sleep(Duration::from_secs(2)).await; // Wait for the server to start

    // Setup: connect to the server
    let mut socket = TcpStream::connect("127.0.0.1:11213").await.unwrap();

    // Exercise: set command
    socket
        .write_all(b"set key 0 0 5\r\nvalue\r\n")
        .await
        .unwrap();

    // Verify: check if the response is STORED
    let mut buf = vec![0; 128];
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"STORED\r\n");

    // Exercise: get command
    socket.write_all(b"get key\r\n").await.unwrap();

    // Verify: check if the response is VALUE
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"VALUE key 0 5\r\nvalue\r\nEND\r\n");

    // Setup: restart the server gracefully
    stop_server(&mut server, Signal::SIGTERM).await;
    server = start_server(&snapshot_dir).await;
    tokio::time::sleep(Duration::from_secs(2)).await; // Wait for the server to start
    let mut socket = TcpStream::connect("127.0.0.1:11213").await.unwrap();

    // Exercise: get command
    socket.write_all(b"get key\r\n").await.unwrap();

    // Verify: data should still be available after restart
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"VALUE key 0 5\r\nvalue\r\nEND\r\n");

    // Exercise: set command
    socket
        .write_all(b"set key2 0 0 6\r\nvalue2\r\n")
        .await
        .unwrap();

    // Verify: check if the response is STORED
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"STORED\r\n");

    // Exercise: snapshot command
    socket.write_all(b"snapshot\r\n").await.unwrap();

    // Verify: check if the response is SNAPSHOT FINISHED
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"SNAPSHOT FINISHED\r\n");

    // Setup: stop the server without snapshot
    stop_server(&mut server, Signal::SIGINT).await;

    // Setup: start the server
    server = start_server(&snapshot_dir).await;
    tokio::time::sleep(Duration::from_secs(2)).await; // Wait for the server to start
    let mut socket = TcpStream::connect("127.0.0.1:11213").await.unwrap();

    // Exercise: get command
    socket.write_all(b"get key\r\n").await.unwrap();

    // Verify: data should still be available after restart
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"VALUE key 0 5\r\nvalue\r\nEND\r\n");

    // Exercise: get command
    socket.write_all(b"get key2\r\n").await.unwrap();

    // Verify: data should still be available after restart
    let n = socket.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"VALUE key2 0 6\r\nvalue2\r\nEND\r\n");

    // Clean up
    stop_server(&mut server, Signal::SIGTERM).await;
    remove_temp_dir(&snapshot_dir);
}
