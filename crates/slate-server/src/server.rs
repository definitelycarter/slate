use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use slate_db::Database;
use slate_store::RocksStore;

use crate::protocol::Request;
use crate::session::Session;

pub struct Server {
    db: Arc<Database<RocksStore>>,
    addr: String,
}

impl Server {
    pub fn new(db: Database<RocksStore>, addr: impl Into<String>) -> Self {
        Self {
            db: Arc::new(db),
            addr: addr.into(),
        }
    }

    pub fn serve(&self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(&self.addr)?;
        eprintln!("slate-server listening on {}", self.addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db = Arc::clone(&self.db);
                    thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, db) {
                            eprintln!("connection error: {e}");
                        }
                    });
                }
                Err(e) => {
                    eprintln!("accept error: {e}");
                }
            }
        }

        Ok(())
    }
}

fn handle_connection(
    stream: TcpStream,
    db: Arc<Database<RocksStore>>,
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
                return Ok(()); // client disconnected
            }
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut msg_buf = vec![0u8; len];
        reader.read_exact(&mut msg_buf)?;

        let request: Request = bincode::deserialize(&msg_buf)?;
        let response = session.handle(request);

        let response_bytes = bincode::serialize(&response)?;
        let response_len = (response_bytes.len() as u32).to_be_bytes();
        writer.write_all(&response_len)?;
        writer.write_all(&response_bytes)?;
        writer.flush()?;
    }
}
