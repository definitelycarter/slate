use std::net::ToSocketAddrs;
use std::ops::{Deref, DerefMut};

use crossbeam::channel::{Receiver, Sender};

use crate::client::{Client, ClientError};

pub struct ClientPool {
    sender: Sender<Client>,
    receiver: Receiver<Client>,
}

impl ClientPool {
    pub fn new(addr: impl ToSocketAddrs + Clone, size: usize) -> Result<Self, ClientError> {
        let (sender, receiver) = crossbeam::channel::bounded(size);
        for _ in 0..size {
            let client = Client::connect(addr.clone())?;
            sender
                .send(client)
                .map_err(|e| ClientError::Io(std::io::Error::other(e.to_string())))?;
        }
        Ok(Self { sender, receiver })
    }

    pub fn get(&self) -> Result<PooledClient<'_>, ClientError> {
        let client = self
            .receiver
            .recv()
            .map_err(|e| ClientError::Io(std::io::Error::other(e.to_string())))?;
        Ok(PooledClient {
            client: Some(client),
            pool: &self.sender,
        })
    }
}

pub struct PooledClient<'a> {
    client: Option<Client>,
    pool: &'a Sender<Client>,
}

impl Deref for PooledClient<'_> {
    type Target = Client;

    fn deref(&self) -> &Client {
        // BUG: client is always Some until Drop runs, and Deref cannot be called after Drop
        self.client.as_ref().expect("BUG: client already consumed")
    }
}

impl DerefMut for PooledClient<'_> {
    fn deref_mut(&mut self) -> &mut Client {
        // BUG: client is always Some until Drop runs, and DerefMut cannot be called after Drop
        self.client.as_mut().expect("BUG: client already consumed")
    }
}

impl Drop for PooledClient<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = self.pool.send(client);
        }
    }
}
