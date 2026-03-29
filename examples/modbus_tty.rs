//! # modbus_tty
//!
//! Shows how to use `NGE_Scheduler_rs` to drive a Modbus RTU polling loop
//! over a TTY serial port without any async runtime.
//!
//! The scheduler fires every 1 000 ms and queues a "Read Holding Registers"
//! request. A dedicated TX thread drains the queue and writes to the serial
//! port; an RX thread reads incoming frames and pushes them into an RX queue.
//!
//! Run:
//! ```
//! cargo run --example modbus_tty
//! ```
//! *(Requires /dev/ttyUSB0 — adjust `PORT` as needed.)*

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use nge_scheduler::{Scheduler, TimedEvent};

const PORT:      &str = "/dev/ttyUSB0";
const BAUD:      u32  = 9600;
const EVT_POLL:  u32  = 1;

// ── Modbus helpers ───────────────────────────────────────────────────────────

fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0xFFFF;
    for &b in data {
        crc ^= b as u16;
        for _ in 0..8 {
            if crc & 1 != 0 { crc = (crc >> 1) ^ 0xA001; } else { crc >>= 1; }
        }
    }
    crc
}

#[derive(Clone, Debug)]
struct Frame {
    slave:    u8,
    function: u8,
    data:     Vec<u8>,
}

impl Frame {
    fn read_holding_registers(slave: u8, addr: u16, qty: u16) -> Self {
        Self {
            slave,
            function: 0x03,
            data: vec![
                (addr >> 8) as u8, addr as u8,
                (qty  >> 8) as u8, qty  as u8,
            ],
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut v = vec![self.slave, self.function];
        v.extend_from_slice(&self.data);
        let crc = crc16(&v);
        v.push((crc & 0xFF) as u8);
        v.push((crc >> 8)   as u8);
        v
    }
}

// ── main ─────────────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== NGE_Scheduler_rs — Modbus TTY example ===\n");
    println!("Opening {} @ {} baud", PORT, BAUD);

    // Shared queues (scheduler → TX thread, RX thread → app)
    let tx_queue: Arc<Mutex<VecDeque<Frame>>> = Arc::new(Mutex::new(VecDeque::new()));
    let rx_queue: Arc<Mutex<VecDeque<Frame>>> = Arc::new(Mutex::new(VecDeque::new()));
    let running                               = Arc::new(AtomicBool::new(true));

    // Open serial port (skip gracefully if not available in CI)
    let port_result = serialport::new(PORT, BAUD)
        .timeout(Duration::from_millis(100))
        .open();

    match port_result {
        Ok(port) => {
            // ── RX thread ────────────────────────────────────────────────────
            let mut rx_port  = port.try_clone().unwrap();
            let rx_q         = Arc::clone(&rx_queue);
            let rx_running   = Arc::clone(&running);
            thread::spawn(move || {
                let mut buf   = [0u8; 256];
                let mut frame = Vec::new();
                while rx_running.load(Ordering::SeqCst) {
                    if let Ok(n) = rx_port.read(&mut buf) {
                        frame.extend_from_slice(&buf[..n]);
                        // Minimal framing: flush when inter-character gap
                        if frame.len() >= 4 {
                            println!("[RX] {} bytes received", frame.len());
                            rx_q.lock().unwrap().push_back(Frame {
                                slave:    frame[0],
                                function: frame[1],
                                data:     frame[2..frame.len()-2].to_vec(),
                            });
                            frame.clear();
                        }
                    }
                }
            });

            // ── TX thread ────────────────────────────────────────────────────
            let mut tx_port = port;
            let tx_q        = Arc::clone(&tx_queue);
            let tx_running  = Arc::clone(&running);
            thread::spawn(move || {
                while tx_running.load(Ordering::SeqCst) {
                    let f = tx_q.lock().unwrap().pop_front();
                    if let Some(frame) = f {
                        let bytes = frame.to_bytes();
                        let _     = tx_port.write_all(&bytes);
                        let _     = tx_port.flush();
                        println!("[TX] slave={} func={:#04X} ({} bytes)", frame.slave, frame.function, bytes.len());
                    } else {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            });
        }
        Err(e) => {
            println!("⚠  Could not open {}: {} — running in dry-run mode", PORT, e);
        }
    }

    // ── Scheduler: poll every 1 000 ms ───────────────────────────────────────
    let tx_q_sched = Arc::clone(&tx_queue);
    let s = Scheduler::new(move |ev| {
        println!("[scheduler] event {} fired — queuing Modbus poll", ev.umsg);
        tx_q_sched
            .lock()
            .unwrap()
            .push_back(Frame::read_holding_registers(1, 0, 10));
    });

    let mut dummy = 0i32;
    s.add_event(&Arc::new(Mutex::new(TimedEvent::new(EVT_POLL, 1000, &mut dummy))));

    // Run for 10 seconds then shut down cleanly.
    thread::sleep(Duration::from_secs(10));
    running.store(false, Ordering::SeqCst);
    println!("\nShutdown complete.");
    Ok(())
}
