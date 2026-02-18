use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use signal_hook::consts::SIGTERM;
use signal_hook::flag;
use slate_db::Database;
use slate_store::Store;

use crate::protocol::Request;
use crate::session::Session;

pub struct Server<S: Store> {
    db: Arc<Database<S>>,
    addr: String,
}

impl<S: Store + Send + Sync + 'static> Server<S> {
    pub fn new(db: Database<S>, addr: impl Into<String>) -> Self {
        Self {
            db: Arc::new(db),
            addr: addr.into(),
        }
    }

    pub fn serve(&self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(&self.addr)?;
        eprintln!("slate-server listening on {}", self.addr);

        let shutdown = Arc::new(AtomicBool::new(false));
        flag::register(SIGTERM, Arc::clone(&shutdown))?;

        // Poll-based accept: non-blocking listener with short sleeps
        // so we can check the shutdown flag.
        listener.set_nonblocking(true)?;

        let connections: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        while !shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false)?;
                    let read_half = stream.try_clone()?;
                    connections.lock().unwrap().push(read_half);

                    let db = Arc::clone(&self.db);
                    handles.push(thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, db) {
                            eprintln!("connection error: {e}");
                        }
                    }));
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    eprintln!("accept error: {e}");
                }
            }
        }

        eprintln!("shutdown signal received, draining connections");

        // Shut down the read half of every connection.
        // This unblocks any handler stuck on read_exact with UnexpectedEof.
        for stream in connections.lock().unwrap().iter() {
            let _ = stream.shutdown(Shutdown::Read);
        }

        // Wait for all handlers to finish.
        for handle in handles {
            let _ = handle.join();
        }

        eprintln!("shutdown complete");
        Ok(())
    }
}

fn handle_connection<S: Store>(
    stream: TcpStream,
    db: Arc<Database<S>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let session = Session::new(db);
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);

    loop {
        // Read length-prefixed message
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(()); // client disconnected or shutdown
            }
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut msg_buf = vec![0u8; len];
        reader.read_exact(&mut msg_buf)?;

        let request: Request = rmp_serde::from_slice(&msg_buf)?;
        let response = session.handle(request);

        let response_bytes = rmp_serde::to_vec(&response)?;
        let response_len = (response_bytes.len() as u32).to_be_bytes();
        writer.write_all(&response_len)?;
        writer.write_all(&response_bytes)?;
        writer.flush()?;
    }
}
