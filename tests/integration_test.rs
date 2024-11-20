// tests/integration_test.rs

use rand::{distributions::Alphanumeric, Rng};
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
        .spawn()
        .expect("failed to start server")
}

async fn stop_server(child: &mut Child) {
    child.kill().expect("failed to kill server");
    child.wait().expect("failed to wait on child");
}

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

#[tokio::test]
async fn test_set_and_get() {
    let random_string = generate_random_string(10);
    let snapshot_dir = format!("/tmp/{}", random_string);

    let mut server = start_server(&snapshot_dir).await;
    tokio::time::sleep(Duration::from_secs(2)).await; // Wait for the server to start

    let mut socket = TcpStream::connect("127.0.0.1:11211").await.unwrap();

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

    stop_server(&mut server).await;
}
