use super::worker::{JobQueue, Request, Response};
use db::db::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use types::types::HorcruxError;

// -----------------------------------------------------------------------------
// Handler trait
// -----------------------------------------------------------------------------

pub trait Handler: Clone + SetHandler + GetHandler + SnapshotHandler {}

pub trait SetHandler {
    fn set(&self, key: String, flags: u32, exptime: u32, data: String) -> Result<(), HorcruxError>;
}

pub trait GetHandler {
    fn get(&self, key: &str) -> Option<Value>;
}

pub trait SnapshotHandler {
    fn snapshot(&self) -> Result<(), HorcruxError>;
}

// -----------------------------------------------------------------------------
// BaseHandler
// -----------------------------------------------------------------------------
pub struct BaseHandler {
    job_queue: JobQueue,
}

impl BaseHandler {
    pub fn _new(job_queue: JobQueue) -> Self {
        BaseHandler { job_queue }
    }
}

impl Clone for BaseHandler {
    fn clone(&self) -> Self {
        BaseHandler {
            job_queue: self.job_queue.clone(),
        }
    }
}

impl SetHandler for BaseHandler {
    fn set(
        &self,
        key: String,
        flags: u32,
        _exptime: u32,
        data: String,
    ) -> Result<(), HorcruxError> {
        let value = Value { flags, data };
        let result = self
            .job_queue
            .send_request(Request::Set { key, value })
            .recv();
        match result {
            Ok(Response::Stored) => {}
            _ => return Err(HorcruxError::Internal),
        }
        Ok(())
    }
}

impl GetHandler for BaseHandler {
    fn get(&self, key: &str) -> Option<Value> {
        let result = self
            .job_queue
            .send_request(Request::Get {
                key: key.to_string(),
            })
            .recv();

        match result {
            Ok(Response::Value(val)) => val,
            _ => None,
        }
    }
}

impl SnapshotHandler for BaseHandler {
    fn snapshot(&self) -> Result<(), HorcruxError> {
        match self
            .job_queue
            .send_request(Request::Snapshot { wait: false })
            .recv()
        {
            Ok(Response::SnapshotAccepted) => Ok(()),
            _ => Err(HorcruxError::Internal),
        }
    }
}

impl Handler for BaseHandler {}

// -----------------------------------------------------------------------------
// ShardHandler
// -----------------------------------------------------------------------------

pub struct ShardHandler {
    job_queues: Vec<JobQueue>,
}

impl ShardHandler {
    pub fn new(job_queues: Vec<JobQueue>) -> Self {
        ShardHandler { job_queues }
    }
}

impl Clone for ShardHandler {
    fn clone(&self) -> Self {
        ShardHandler {
            job_queues: self.job_queues.clone(),
        }
    }
}

impl SetHandler for ShardHandler {
    fn set(
        &self,
        key: String,
        flags: u32,
        _exptime: u32,
        data: String,
    ) -> Result<(), HorcruxError> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let shard_id = (hash as usize) % self.job_queues.len();
        let value = Value { flags, data };
        let result = self.job_queues[shard_id]
            .send_request(Request::Set { key, value })
            .recv();
        match result {
            Ok(Response::Stored) => {}
            _ => return Err(HorcruxError::Internal),
        }
        Ok(())
    }
}

impl GetHandler for ShardHandler {
    fn get(&self, key: &str) -> Option<Value> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let shard_id = (hash as usize) % self.job_queues.len();
        let result = self.job_queues[shard_id]
            .send_request(Request::Get {
                key: key.to_string(),
            })
            .recv();

        match result {
            Ok(Response::Value(val)) => val,
            _ => None,
        }
    }
}

impl SnapshotHandler for ShardHandler {
    fn snapshot(&self) -> Result<(), HorcruxError> {
        let mut results = Vec::new();
        for job_queue in &self.job_queues {
            results.push(
                job_queue
                    .send_request(Request::Snapshot { wait: false })
                    .recv(),
            );
        }
        for result in results {
            match result {
                Ok(Response::SnapshotAccepted) => {}
                _ => return Err(HorcruxError::Internal),
            }
        }
        Ok(())
    }
}

impl Handler for ShardHandler {}
