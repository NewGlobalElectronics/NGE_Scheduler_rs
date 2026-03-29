//! # basic_usage
//!
//! Demonstrates the minimal API surface of NGE_Scheduler_rs:
//!
//! * One periodic event (fires every 500 ms)
//! * One one-shot event (fires once after 1 000 ms)
//! * Runtime lookup of a live event via `Scheduler::get`

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use nge_scheduler::{Scheduler, TimedEvent};

// User-defined message IDs — any u32 constant works.
const EVT_HEARTBEAT: u32 = 1;
const EVT_ONE_SHOT:  u32 = 2;

fn main() {
    println!("=== NGE_Scheduler_rs — basic usage ===\n");

    // ── Create the scheduler ─────────────────────────────────────────────────
    // The closure is called on the background dispatch thread every time an
    // event fires. Dispatch multiple IDs with a simple match.
    let s = Scheduler::new(|ev| match ev.umsg {
        EVT_HEARTBEAT => println!("[{:>8}ms] ♥  heartbeat (period {}ms)",
                                  timestamp_ms(), ev.time),
        EVT_ONE_SHOT  => println!("[{:>8}ms] ✓  one-shot fired — will not repeat",
                                  timestamp_ms()),
        other         => println!("[{:>8}ms] ?  unknown event id={}", timestamp_ms(), other),
    });

    // ── Register events ──────────────────────────────────────────────────────
    // Payload pointer is optional — pass a null or a real &mut variable.
    let mut dummy = 0i32;

    // Periodic: fires every 500 ms indefinitely (time != 0).
    s.add_event(&Arc::new(Mutex::new(
        TimedEvent::new(EVT_HEARTBEAT, 500, &mut dummy),
    )));

    // One-shot: fires once after 1 000 ms (time == 0 means no reschedule).
    // We deliberately pass time=0 so the scheduler does not re-queue it.
    s.add_event(&Arc::new(Mutex::new(
        TimedEvent::new(EVT_ONE_SHOT, 0, std::ptr::null_mut()),
    )));

    // ── Runtime lookup ───────────────────────────────────────────────────────
    // Useful for inspecting or cancelling a live event from application code.
    thread::sleep(Duration::from_millis(1_200));

    if let Some(hb) = s.get(EVT_HEARTBEAT) {
        let ev = hb.lock().unwrap();
        println!("\n[scheduler] heartbeat event still alive — period {}ms", ev.time);
    }

    println!("\nRunning for 3 more seconds…");
    thread::sleep(Duration::from_secs(3));
    println!("Done.");
}

fn timestamp_ms() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_millis() as u128
}
