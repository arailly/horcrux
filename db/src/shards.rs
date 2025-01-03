use chrono::Utc;
use nix::sys::wait::waitpid;
use nix::unistd::{fork, getpid, ForkResult};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::prelude::*;
use std::io::BufWriter;

use crate::db::{Value, DB};

pub struct Shards {
    dbs: Vec<DB>,
    snapshot_dir: String,
}

impl Shards {
    pub fn new(n: usize, snapshot_dir: String) -> Self {
        let mut dbs = Vec::new();
        for _ in 0..n {
            dbs.push(DB::new());
        }
        Self { dbs, snapshot_dir }
    }

    fn compute_shard_id(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as usize) % self.dbs.len()
    }

    pub fn insert(&mut self, key: String, value: Value) {
        let shard_id = self.compute_shard_id(&key);
        self.dbs[shard_id].insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        let shard_id = self.compute_shard_id(key);
        self.dbs[shard_id].get(key)
    }

    pub fn snapshot(&self, wait: bool) {
        match unsafe { fork() } {
            Ok(ForkResult::Parent { child, .. }) => {
                println(&format!("{:?}: Snapshot process started: {}", now(), child));
                if !wait {
                    return;
                }
                if waitpid(child, None).is_err() {
                    println("Failed to wait for snapshot process");
                }
            }
            Ok(ForkResult::Child) => {
                for (i, db) in self.dbs.iter().enumerate() {
                    let path = format!("{}/snapshot-{}", self.snapshot_dir, i);
                    match db.snapshot(&path) {
                        Ok(_) => {}
                        Err(err) => {
                            println!("Failed to restore DB({}): {}", i, err);
                        }
                    }
                }

                let pid = getpid();
                println(&format!("{:?}: Snapshot process finished: {}", now(), pid));
                unsafe { libc::_exit(0) };
            }
            Err(_) => {
                println("Failed to fork");
            }
        }
    }

    pub fn restore(&mut self) {
        for (i, db) in self.dbs.iter_mut().enumerate() {
            let path = format!("{}/snapshot-{}", self.snapshot_dir, i);
            match db.restore(&path) {
                Ok(_) => {
                    println!("DB({}) restored from snapshot", i);
                }
                Err(err) => {
                    println!("Failed to restore DB({}): {}", i, err);
                }
            }
        }
    }
}

fn now() -> String {
    Utc::now().format("%+").to_string()
}

// println! is not safe in child process
fn println(msg: &str) {
    let stdout = std::io::stdout();
    let mut output = BufWriter::new(stdout.lock());
    let _ = output.write_all(msg.as_bytes());
    let _ = output.write(b"\n");
}
