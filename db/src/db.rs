use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::Utc;
use std::collections::HashMap;
use std::fs::{rename, File};
use std::io::prelude::*;
use std::io::BufWriter;
use types::types::HorcruxError;

#[derive(Debug, Clone)]
pub struct Value {
    pub flags: u32,
    // exptime: u32,
    pub data: String,
}

pub struct DB {
    db: HashMap<String, Value>,
    snapshot_path: String,
}

impl DB {
    pub fn new(snapshot_path: String) -> Self {
        DB {
            db: HashMap::new(),
            snapshot_path: snapshot_path,
        }
    }

    pub fn insert(&mut self, key: String, value: Value) {
        self.db.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.db.get(key)
    }

    pub fn snapshot(&self) -> Result<(), std::io::Error> {
        let dumped = dump(self);

        let tmp_suffix = Utc::now().format("%+").to_string();
        let tmp_path = format!("{}-{}", self.snapshot_path, tmp_suffix);

        let mut f = match File::create(tmp_path.as_str()) {
            Ok(f) => f,
            Err(err) => {
                println("Failed to create snapshot file");
                return Err(err);
            }
        };
        f.write_all(&dumped).unwrap();
        f.sync_all()?;
        rename(tmp_path.as_str(), self.snapshot_path.as_str())?;

        Ok(())
    }

    pub fn restore(&mut self) {
        println!("{:?}: Restoring DB from snapshot", now());
        let data = match std::fs::read(&self.snapshot_path) {
            Ok(data) => data,
            Err(err) => {
                println!("Failed to read snapshot file: {}", err);
                return;
            }
        };
        let mut mem = Bytes::from(data);
        while !mem.is_empty() {
            let (key, value) = match get_key_value_from_bytes(&mut mem) {
                Ok((key, value)) => (key, value),
                Err(err) => {
                    println!("{}", err);
                    return;
                }
            };
            self.insert(key, value);
        }
        println!("{:?}: DB restored from snapshot", now());
    }
}

// format: <key_len: u8><key><flags: u32><data_len: u32><data>...
fn dump(db: &DB) -> Bytes {
    let mut dumped = BytesMut::with_capacity(db.db.len() * 100);
    for (key, value) in db.db.iter() {
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

fn get_key_value_from_bytes(mem: &mut Bytes) -> Result<(String, Value), HorcruxError> {
    let key_len = mem.get_u8() as usize;
    let key = String::from_utf8(mem.split_to(key_len).to_vec())
        .map_err(|_| HorcruxError::RestoreDB("Failed to parse key from snapshot".to_string()))?;
    let flags = mem.get_u32();
    let data_len = mem.get_u32() as usize;
    let data = String::from_utf8(mem.split_to(data_len).to_vec())
        .map_err(|_| HorcruxError::RestoreDB("Failed to parse data from snapshot".to_string()))?;
    Ok((key, Value { flags, data }))
}

fn now() -> String {
    Utc::now().format("%+").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_restore() {
        let path = "/tmp/test_restore";
        let mut db = DB::new(path.to_string());
        db.insert(
            "key1".to_string(),
            Value {
                flags: 0,
                data: "data1".to_string(),
            },
        );
        db.insert(
            "key2".to_string(),
            Value {
                flags: 0,
                data: "data2".to_string(),
            },
        );

        db.snapshot().unwrap();

        let mut new_db = DB::new(path.to_string());
        new_db.restore();

        let actual_1 = new_db.get("key1").unwrap();
        assert_eq!(actual_1.flags, 0);
        assert_eq!(actual_1.data, "data1");

        let actual_2 = new_db.get("key2").unwrap();
        assert_eq!(actual_2.flags, 0);
        assert_eq!(actual_2.data, "data2");
    }
}
