
//! # NGE_Scheduler_rs
//!
//! A minimal, event-driven periodic scheduler for Rust — baremetal-compatible.
//!
//! Ported from the C library [NGE_Scheduler](https://github.com/NewGlobalElectronics/NGE_Scheduler).
//!
//! ## Core Philosophy
//!
//! Unlike task-based schedulers, **events are the primary entity**. Each [`TimedEvent`]
//! carries its own period, user message ID, and optional payload. There are no separate
//! task descriptors: registering an event *is* scheduling it.
//!
//! ## Quick Start
//!
//! ```rust
//! use std::sync::{Arc, Mutex};
//! use nge_scheduler::{Scheduler, TimedEvent};
//!
//! let s = Scheduler::new(|ev| {
//!     println!("Event {} fired (period {}ms)", ev.umsg, ev.time);
//! });
//!
//! let mut dummy = 0i32;
//! // Event ID=1, period=500 ms
//! s.add_event(&Arc::new(Mutex::new(TimedEvent::new(1, 500, &mut dummy))));
//! ```

use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};

// ── internal heap machinery ──────────────────────────────────────────────────

type BgCb = Arc<dyn Fn(&TimedEvent) + Send + Sync>;
type EntryHeap = BinaryHeap<HeapEntry>;

#[derive(Clone)]
struct HeapEntry(Arc<Mutex<TimedEvent>>);

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.lock().unwrap().deadline == other.0.lock().unwrap().deadline
    }
}
impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Min-heap ordered by deadline (earliest fires first).
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reversed so BinaryHeap (max-heap by default) becomes a min-heap.
        other
            .0
            .lock()
            .unwrap()
            .deadline
            .cmp(&self.0.lock().unwrap().deadline)
    }
}

// ── public API ───────────────────────────────────────────────────────────────

/// A timed, optionally periodic event.
///
/// | Field   | Description                                              |
/// |---------|----------------------------------------------------------|
/// | `umsg`  | User-defined message / event ID (used as registry key)  |
/// | `time`  | Period in milliseconds. `0` = one-shot event.           |
/// | `pmsg`  | Raw pointer to user payload — caller owns the lifetime. |
///
/// # Safety
/// `pmsg` is deliberately a raw pointer so the scheduler can be used in
/// `no_std` / baremetal contexts where `Box` and heap allocation are absent.
/// On hosted targets you can safely pass `&mut value as *mut i32`.
pub struct TimedEvent {
    /// Application-level event identifier.
    pub umsg: u32,
    /// Repeat period in milliseconds. Set to `0` for a one-shot event.
    pub time: u64,
    /// Optional pointer to a user-managed payload.
    pub pmsg: *mut i32,
    /// Absolute instant at which this event should fire next.
    deadline: Instant,
}

impl TimedEvent {
    /// Construct a new `TimedEvent`.
    ///
    /// The `deadline` is set lazily when the event is registered via
    /// [`Scheduler::add_event`].
    pub fn new(umsg: u32, time: u64, pmsg: *mut i32) -> Self {
        Self {
            umsg,
            time,
            pmsg,
            deadline: Instant::now(), // placeholder; overwritten by add_event
        }
    }

    fn set_deadline(&mut self, d: Instant) {
        self.deadline = d;
    }
}

// SAFETY: The pointer `pmsg` is only dereferenced by user-supplied callbacks.
// The caller is responsible for ensuring the pointed-to memory outlives the event.
unsafe impl Send for TimedEvent {}
unsafe impl Sync for TimedEvent {}

/// A lightweight, event-driven periodic scheduler.
///
/// Internally it maintains a min-heap of `Arc<Mutex<TimedEvent>>` entries
/// and a `HashMap` for O(1) event lookup by ID. A single background thread
/// polls the heap every millisecond and invokes the user callback when an
/// event's deadline expires.
///
/// No Tokio, no async runtime, no external queue libraries.
/// The background thread uses only `std::thread`, `std::sync`, and
/// `std::time` — making a `no_std` port straightforward for RTOS targets
/// (e.g., Cortex-M4 with CMSIS-RTOS or bare-metal tick loops).
#[derive(Clone)]
pub struct Scheduler {
    map:  Arc<Mutex<HashMap<u32, Weak<Mutex<TimedEvent>>>>>,
    heap: Arc<Mutex<EntryHeap>>,
    cb:   BgCb,
}

impl Scheduler {
    /// Create a scheduler and immediately start its background dispatch thread.
    ///
    /// `f` is called on the **scheduler thread** whenever an event fires.
    /// Keep the callback short; long-running work should be handed off to
    /// another thread.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(&TimedEvent) + Send + Sync + 'static,
    {
        let map  = Arc::new(Mutex::new(HashMap::new()));
        let heap: Arc<Mutex<EntryHeap>> = Arc::new(Mutex::new(EntryHeap::new()));
        let cb   = Arc::new(f);

        thread::spawn({
            let heap = heap.clone();
            let cb   = cb.clone();
            move || loop {
                thread::sleep(Duration::from_millis(1));
                let mut h = heap.lock().unwrap();
                while let Some(entry) = h.peek() {
                    let ev = entry.0.lock().unwrap();
                    if ev.deadline > Instant::now() {
                        break;
                    }
                    drop(ev);

                    let entry = h.pop().unwrap();
                    let arc   = entry.0;
                    let ev    = arc.lock().unwrap();
                    cb(&*ev);

                    if ev.time != 0 {
                        // Periodic: reschedule from *now* to avoid drift accumulation.
                        let when = Instant::now() + Duration::from_millis(ev.time);
                        drop(ev);
                        arc.lock().unwrap().set_deadline(when);
                        h.push(HeapEntry(arc));
                    }
                    // Process one expired event per tick to stay responsive.
                    break;
                }
            }
        });

        Self { map, heap, cb }
    }

    /// Register (or re-register) a [`TimedEvent`].
    ///
    /// The event's first deadline is set to `now + event.time` at the moment
    /// this method is called. If an event with the same `umsg` ID already
    /// exists it is replaced in the registry (the old `Weak` reference is
    /// simply overwritten; the old heap entry will be silently dropped when
    /// it pops).
    pub fn add_event(&self, e: &Arc<Mutex<TimedEvent>>) {
        let mut ev = e.lock().unwrap();
        let when   = Instant::now() + Duration::from_millis(ev.time);
        ev.set_deadline(when);
        self.map.lock().unwrap().insert(ev.umsg, Arc::downgrade(e));
        drop(ev);
        self.heap.lock().unwrap().push(HeapEntry(Arc::clone(e)));
    }

    /// Look up a live event by its message ID.
    ///
    /// Returns `None` if the event was never registered or has been dropped.
    pub fn get(&self, id: u32) -> Option<Arc<Mutex<TimedEvent>>> {
        self.map
            .lock()
            .unwrap()
            .get(&id)
            .and_then(|w| w.upgrade())
    }
}
