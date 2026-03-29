//! # websocket_ihm
//!
//! Demonstrates driving a WebSocket heartbeat loop with `NGE_Scheduler_rs`.
//!
//! * A combined HTTP + WebSocket server runs on `127.0.0.1:8080`.
//! * The scheduler fires every 100 ms and broadcasts a JSON heartbeat to all
//!   connected clients — no Tokio, no async executor, plain `std::thread`.
//!
//! Run:
//! ```
//! cargo run --example websocket_ihm
//! ```
//! Then open a browser at `http://127.0.0.1:8080/` or connect any WS client
//! to `ws://127.0.0.1:8080`.

use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use base64::encode;
use chrono::Local;
use sha1::{Digest, Sha1};

use nge_scheduler::{Scheduler, TimedEvent};

const EVT_HEARTBEAT: u32 = 1;
const HEARTBEAT_MS:  u64 = 100;

// ── WebSocket helpers ────────────────────────────────────────────────────────

fn compute_accept_key(key: &str) -> String {
    let mut h = Sha1::new();
    h.update(key);
    h.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    encode(h.finalize())
}

fn ws_text_frame(msg: &str) -> Vec<u8> {
    let b = msg.as_bytes();
    let mut f = vec![0x81u8];
    match b.len() {
        n if n < 126    => f.push(n as u8),
        n if n <= 65535 => { f.push(126); f.push((n >> 8) as u8); f.push(n as u8); }
        n               => { f.push(127); (0..8).rev().for_each(|i| f.push((n >> (i*8)) as u8)); }
    }
    f.extend_from_slice(b);
    f
}

fn send_ws_text(stream: &mut TcpStream, msg: &str) {
    let _ = stream.write_all(&ws_text_frame(msg));
    let _ = stream.flush();
}

// ── Connection handler ───────────────────────────────────────────────────────

fn handle_connection(
    stream:         TcpStream,
    clients:        Arc<Mutex<HashMap<usize, TcpStream>>>,
    client_counter: Arc<AtomicUsize>,
) {
    let mut s = stream;
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();

    let mut buf = [0u8; 4096];
    let n = match s.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _              => return,
    };
    let req = String::from_utf8_lossy(&buf[..n]);

    if req.contains("Sec-WebSocket-Key") {
        // WebSocket upgrade
        let k_start = req.find("Sec-WebSocket-Key: ").unwrap() + 19;
        let k_end   = req[k_start..].find("\r\n").unwrap() + k_start;
        let key     = req[k_start..k_end].trim();

        let resp = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {}\r\n\r\n",
            compute_accept_key(key)
        );
        let _ = s.write_all(resp.as_bytes());
        let _ = s.flush();

        let id = client_counter.fetch_add(1, Ordering::SeqCst);
        clients.lock().unwrap().insert(id, s.try_clone().unwrap());
        println!("[WS] client {} connected", id);

        send_ws_text(&mut s, &format!(
            r#"{{"type":"welcome","client_id":{}}}"#, id
        ));

        // Read loop (detect close / disconnect)
        s.set_nonblocking(true).ok();
        let mut b = [0u8; 256];
        loop {
            match s.read(&mut b) {
                Ok(0) | Err(_) => break,
                _ => {}
            }
            thread::sleep(Duration::from_millis(10));
        }
        clients.lock().unwrap().remove(&id);
        println!("[WS] client {} disconnected", id);

    } else if req.starts_with("GET") {
        // Minimal HTTP — serve an inline page
        let body = r#"<!DOCTYPE html>
<html><head><title>NGE_Scheduler_rs</title></head><body>
<h2>NGE_Scheduler_rs WebSocket demo</h2>
<pre id="log"></pre>
<script>
  const ws = new WebSocket("ws://" + location.host);
  ws.onmessage = e => {
    const pre = document.getElementById("log");
    pre.textContent = e.data + "\n" + pre.textContent.slice(0, 2000);
  };
</script></body></html>"#;
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(), body
        );
        let _ = s.write_all(resp.as_bytes());
    }
}

// ── main ─────────────────────────────────────────────────────────────────────

fn main() {
    println!("=== NGE_Scheduler_rs — WebSocket IHM example ===\n");

    let clients:        Arc<Mutex<HashMap<usize, TcpStream>>> = Arc::new(Mutex::new(HashMap::new()));
    let client_counter: Arc<AtomicUsize>                      = Arc::new(AtomicUsize::new(0));
    let server_ready:   Arc<AtomicBool>                       = Arc::new(AtomicBool::new(false));

    // ── TCP server thread ────────────────────────────────────────────────────
    {
        let clients        = Arc::clone(&clients);
        let client_counter = Arc::clone(&client_counter);
        let server_ready   = Arc::clone(&server_ready);

        thread::spawn(move || {
            let listener = TcpListener::bind("127.0.0.1:8080").expect("bind failed");
            println!("[server] HTTP  → http://127.0.0.1:8080");
            println!("[server] WS   → ws://127.0.0.1:8080");
            server_ready.store(true, Ordering::SeqCst);
            listener.set_nonblocking(true).unwrap();

            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let c  = Arc::clone(&clients);
                        let cc = Arc::clone(&client_counter);
                        thread::spawn(move || handle_connection(stream, c, cc));
                    }
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) => eprintln!("[server] accept error: {}", e),
                }
            }
        });
    }

    // Wait until the TCP server is listening before starting the scheduler.
    while !server_ready.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(5));
    }

    // ── Scheduler: broadcast heartbeat every 100 ms ──────────────────────────
    let clients_sched = Arc::clone(&clients);
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    let s = Scheduler::new(move |_ev| {
        let n   = COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        let msg = format!(
            r#"{{"type":"heartbeat","count":{},"time":"{}"}}"#,
            n,
            Local::now().format("%H:%M:%S%.3f")
        );
        let frame = ws_text_frame(&msg);

        let lock = clients_sched.lock().unwrap();
        for (&id, stream) in lock.iter() {
            if let Ok(mut s) = stream.try_clone() {
                if s.write_all(&frame).is_err() {
                    eprintln!("[scheduler] client {} write failed", id);
                }
            }
        }
        if !lock.is_empty() {
            println!("[scheduler] heartbeat #{} → {} client(s)", n, lock.len());
        }
    });

    let mut dummy = 0i32;
    s.add_event(&Arc::new(Mutex::new(TimedEvent::new(
        EVT_HEARTBEAT,
        HEARTBEAT_MS,
        &mut dummy,
    ))));

    println!("\nPress Ctrl+C to stop.\n");
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}
