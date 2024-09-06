use bytes::{BufMut, Bytes, BytesMut};
use chrono::Utc;
use nix::sys::wait::waitpid;
use nix::unistd::{fork, getpid, ForkResult};
use std::fs::{rename, File};
use std::io::prelude::*;
use std::io::BufWriter;
use std::sync::Arc;

use super::db::DB;

pub async fn take_snapshot(db: &Arc<DB>, snapshot_dir: &str) {
    match unsafe { fork() } {
        Ok(ForkResult::Parent { child, .. }) => {
            println(&format!(
                "{:?}: Snapshot process started: {}",
                Utc::now().format("%+").to_string(),
                child
            ));
            if waitpid(child, None).is_err() {
                println("Failed to wait for snapshot process");
            }
        }
        Ok(ForkResult::Child) => {
            let dumped = dump(db).await;

            let prefix = format!("{}/snapshot", snapshot_dir);
            let suffix = Utc::now().format("%+").to_string();
            let tmp_file = format!("{}-{}", prefix, suffix);

            let mut f = match File::create(tmp_file.as_str()) {
                Ok(f) => f,
                Err(_) => {
                    println("Failed to create snapshot file");
                    unsafe { libc::_exit(1) };
                }
            };
            f.write_all(&dumped).unwrap();
            if f.sync_all().is_err() {
                println("Failed to sync snapshot file");
            }

            if rename(tmp_file.as_str(), prefix).is_err() {
                println("Failed to rename snapshot file");
                unsafe { libc::_exit(2) };
            }

            let pid = getpid();
            println(&format!(
                "{:?}: Snapshot process finished: {}",
                Utc::now().format("%+").to_string(),
                pid
            ));
            unsafe { libc::_exit(0) };
        }
        Err(_) => {
            println("Failed to fork");
        }
    }
}

// format: <key_len: u8><key><flags: u32><data_len: u32><data>...
async fn dump(db: &Arc<DB>) -> Bytes {
    let db = db.read().await;
    let mut dumped = BytesMut::with_capacity(db.len() * 100);
    for (key, value) in db.iter() {
        dumped.put_u8(key.len() as u8);
        dumped.put(key.as_bytes());
        dumped.put_u32(value.flags);
        dumped.put_u32(value.data.len() as u32);
        dumped.put(value.data.as_bytes());
    }
    return dumped.freeze();
}

// println! is not safe in child process
fn println(msg: &str) {
    let stdout = std::io::stdout();
    let mut output = BufWriter::new(stdout.lock());
    let _ = output.write_all(msg.as_bytes());
    let _ = output.write(b"\n");
}
