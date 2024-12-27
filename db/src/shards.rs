use crate::db::DB;

pub struct Shards {
    dbs: Vec<DB>,
}

impl Shards {
    pub fn new(dbs: Vec<DB>) -> Self {
        Self { dbs }
    }
}
