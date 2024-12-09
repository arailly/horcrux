use super::db::Value;
use super::types::HorcruxError;
use super::worker::{JobQueue, Request, Response};

pub trait Handler: SetHandler + GetHandler + SnapshotHandler {}

pub trait SetHandler {
    fn set(
        &self,
        key: String,
        flags: u32,
        exptime: u32,
        data: String,
    ) -> Result<(), HorcruxError>;
}

pub trait GetHandler {
    fn get(&self, key: &str) -> Option<Value>;
}

pub trait SnapshotHandler {
    fn snapshot(&self) -> Result<(), HorcruxError>;
}

pub struct BaseHandler {
    job_queue: JobQueue,
}

impl BaseHandler {
    pub fn new(job_queue: JobQueue) -> Self {
        BaseHandler { job_queue }
    }

    pub fn clone(&self) -> Self {
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
        let value = Value {
            flags: flags,
            data: data,
        };
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
        let result = self.job_queue.send_request(Request::Get {
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
        match self.job_queue.send_request(Request::Snapshot { wait: false }).recv() {
            Ok(Response::SnapshotAccepted) => Ok(()),
            _ => Err(HorcruxError::Internal),
        }
    }
}

impl Handler for BaseHandler {}
