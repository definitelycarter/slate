use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

use slate_lists::Loader;

use slate_loader_fake::FakeLoader;

fn main() {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8081".to_string());
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).unwrap_or_else(|e| {
        eprintln!("failed to bind {addr}: {e}");
        std::process::exit(1);
    });

    eprintln!("fake loader listening on {addr}");

    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept error: {e}");
                continue;
            }
        };

        // Read the HTTP request (consume headers until blank line)
        let reader = BufReader::new(&stream);
        for line in reader.lines() {
            match line {
                Ok(l) if l.is_empty() => break,
                Ok(_) => continue,
                Err(_) => break,
            }
        }

        eprintln!("serving 100 documents");

        let docs = FakeLoader
            .load("fake", &HashMap::new())
            .expect("loader should not fail");

        let mut body = Vec::new();
        for doc in docs {
            let doc = doc.expect("document should not fail");
            serde_json::to_writer(&mut body, &doc).unwrap();
            body.push(b'\n');
        }

        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );

        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(&body);
    }
}
