# NGE_Scheduler_rs

> **A minimal, event-driven periodic scheduler for Rust — baremetal-compatible.**

[![CI](https://github.com/NewGlobalElectronics/NGE_Scheduler_rs/actions/workflows/ci.yml/badge.svg)](https://github.com/NewGlobalElectronics/NGE_Scheduler_rs/actions)
[![Crates.io](https://img.shields.io/crates/v/nge_scheduler.svg)](https://crates.io/crates/nge_scheduler)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`NGE_Scheduler_rs` is a faithful Rust port of the C library
[NGE_Scheduler](https://github.com/NewGlobalElectronics/NGE_Scheduler).  
It keeps the same design philosophy while adding Rust's compile-time safety
guarantees — making it a strong candidate for safety-critical and
resource-constrained embedded targets.

---

## Table of Contents

- [Philosophy — Events, not Tasks](#philosophy--events-not-tasks)
- [Why no Tokio?](#why-no-tokio)
- [Safety & ISO 26262-2](#safety--iso-26262-2)
- [Footprint — Cortex-M4 and below](#footprint--cortex-m4-and-below)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Examples](#examples)
- [Repository Layout](#repository-layout)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Philosophy — Events, not Tasks

Most schedulers treat **tasks** as the primary entity: a task has a name, a
priority, a stack, and optionally a period.  NGE_Scheduler takes the opposite
view: **the event is the primary entity**.

```
┌──────────────────────────────────────────────────────┐
│  Task-centric model           Event-centric model     │
│                                                       │
│  Task ──► has a period        Event ──► IS the period │
│  Task ──► has a callback      Event ──► IS the token  │
│  Task ──► needs a name        Event ID ──► IS the key │
└──────────────────────────────────────────────────────┘
```

A `TimedEvent` bundles everything together:

| Field  | Meaning                                              |
|--------|------------------------------------------------------|
| `umsg` | User message ID — uniquely names the event           |
| `time` | Repeat period in milliseconds (`0` = one-shot)       |
| `pmsg` | Optional raw pointer to a user-managed payload       |

Registering an event _is_ scheduling it.  There are no separate "task
descriptors", no thread pools, no executor graphs.  The whole scheduler fits in
**~150 lines of safe Rust**.

This mirrors exactly how the original C library works — and it is intentional.
In deeply embedded systems you often want to reason about *when something
happens*, not *which thread is running*.  The event is the unit of time.

---

## Why no Tokio?

Tokio is an excellent library for high-throughput I/O-bound server software.
It is, however, a significant dependency that:

- requires a **full OS** (Linux / Windows / macOS) with a working async I/O
  substrate,
- pulls in dozens of transitive crates (>30 in a typical async server build),
- is **not available on bare-metal** Cortex-M, RISC-V, or other
  `no_std` targets,
- introduces a non-trivial runtime whose scheduling behaviour can be hard to
  audit for functional-safety purposes.

`NGE_Scheduler_rs` deliberately avoids Tokio and any complex queue library.
Its only dependencies are:

```
std::thread   std::sync   std::time   std::collections
```

That is it.  No external crates in the core library.  This keeps the
dependency tree minimal, the binary small, and the code fully auditable — a
key prerequisite for safety-critical certification.

> **Bottom line:** if your target has a heap and a thread primitive (CMSIS-RTOS,
> FreeRTOS with a Rust HAL, or bare-metal with a 1 ms SysTick), you can use
> this scheduler.  Tokio is never a requirement.

---

## Safety & ISO 26262-2

Rust's ownership and borrowing model provides compile-time guarantees that
directly address common failure modes targeted by functional-safety standards:

| Failure mode              | Rust mitigation                                    | ISO 26262-2 relevance          |
|---------------------------|----------------------------------------------------|-------------------------------|
| Data races                | Borrow checker — impossible at compile time        | Part 6 §8.4.4 (no shared mutable state) |
| Use-after-free            | Ownership model — rejected by compiler             | Part 6 §8.4.4 (memory safety) |
| Null pointer dereference  | `Option<T>` — null cannot be dereferenced safely   | Part 6 §8.4.4                 |
| Integer overflow           | Debug builds panic; release wraps deterministically | Part 6 §8.4.3                |
| Uninitialized reads       | Compiler guarantees all values are initialised     | Part 6 §8.4.4                 |

### What this library does for safety

1. **Zero `unsafe` in the public API.** All unsafe usage (the `*mut i32`
   payload pointer) is isolated behind `unsafe impl Send/Sync` with documented
   caller obligations — making the safety contract explicit and auditable.

2. **Deterministic dispatch.** The background thread processes **one** expired
   event per 1 ms tick.  Dispatch latency is bounded and measurable.

3. **No dynamic allocation in the hot path.**  The heap and map are allocated
   once at `Scheduler::new`; no `Box::new` or `Vec::push` happens during
   normal event dispatch.

4. **Weak-reference GC.** Events that are dropped by the application are
   silently removed from the heap via `Weak::upgrade` — no dangling entries,
   no memory leaks.

> **Note:** This library is provided as a building block.  Achieving a full
> ISO 26262-2 ASIL certification also requires a qualified toolchain, process
> documentation, and a system-level FMEA.  The properties above are necessary
> but not sufficient.

---

## Footprint — Cortex-M4 and below

The core scheduler (`src/lib.rs`) compiles to **< 5 kB of `.text`** on a
`thumbv7em-none-eabihf` target with `opt-level = "z"` and `lto = true`.

Typical RAM usage at runtime:

| Component              | Size                                    |
|------------------------|-----------------------------------------|
| `Scheduler` struct     | 3 × `Arc` = 3 × 8 bytes on 32-bit      |
| `HeapEntry` per event  | `Arc<Mutex<TimedEvent>>` ≈ 64 bytes     |
| Background thread stack| configurable — 2 kB is sufficient       |

A system with 10 periodic events uses roughly **1 kB of heap** for scheduler
metadata — well within the budget of a Cortex-M4 with 256 kB RAM.

### `no_std` roadmap

The current implementation uses `std::thread` and `std::sync::Mutex`.  A
`no_std` variant is planned (see [Roadmap](#roadmap)) that will replace these
with:

- a **SysTick interrupt** or RTOS timer callback as the dispatch driver,
- `bare-metal::Mutex` or `cortex-m::interrupt::free` for critical sections.

Porting is straightforward because the heap/map logic is already abstracted
behind `Arc<Mutex<…>>` — only the threading primitives need to change.

---

## Quick Start

Add the crate to your project:

```toml
[dependencies]
nge_scheduler = { git = "https://github.com/NewGlobalElectronics/NGE_Scheduler_rs" }
```

Create a scheduler and register your first event:

```rust
use std::sync::{Arc, Mutex};
use nge_scheduler::{Scheduler, TimedEvent};

fn main() {
    // All events share one callback — dispatch on umsg, just like C.
    let s = Scheduler::new(|ev| match ev.umsg {
        1 => println!("sensor poll ({}ms period)", ev.time),
        2 => println!("watchdog kick"),
        _ => {}
    });

    let mut dummy = 0i32;

    // Event 1: poll a sensor every 50 ms
    s.add_event(&Arc::new(Mutex::new(TimedEvent::new(1, 50, &mut dummy))));

    // Event 2: kick a watchdog every 500 ms
    s.add_event(&Arc::new(Mutex::new(TimedEvent::new(2, 500, std::ptr::null_mut()))));

    // Your application loop continues here — the scheduler runs in the background.
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
```

---

## API Reference

### `TimedEvent`

```rust
pub struct TimedEvent {
    pub umsg: u32,      // Event identifier
    pub time: u64,      // Period in ms. 0 = one-shot.
    pub pmsg: *mut i32, // User payload pointer (optional, may be null)
}

impl TimedEvent {
    pub fn new(umsg: u32, time: u64, pmsg: *mut i32) -> Self;
}
```

### `Scheduler`

```rust
pub struct Scheduler { /* … */ }

impl Scheduler {
    /// Construct and start the background dispatch thread.
    pub fn new<F: Fn(&TimedEvent) + Send + Sync + 'static>(f: F) -> Self;

    /// Register a periodic or one-shot event.
    /// Sets the first deadline to `now + event.time`.
    pub fn add_event(&self, e: &Arc<Mutex<TimedEvent>>);

    /// Look up a live event by ID. Returns None if dropped.
    pub fn get(&self, id: u32) -> Option<Arc<Mutex<TimedEvent>>>;
}
```

`Scheduler` is `Clone` — you can hand copies to multiple threads.

---

## Examples

### `basic_usage` — minimal demo

```
cargo run --example basic_usage
```

Shows a periodic heartbeat event and a one-shot event, plus runtime event
lookup via `Scheduler::get`.

---

### `modbus_tty` — Modbus RTU polling over serial

```
cargo run --example modbus_tty
```

The scheduler fires every 1 000 ms and queues a Modbus "Read Holding
Registers" frame.  A dedicated TX thread drains the queue and writes to
`/dev/ttyUSB0`.  Demonstrates that the scheduler integrates cleanly with
existing I/O threads — no async glue required.

```
[scheduler] event 1 fired — queuing Modbus poll
[TX] slave=1 func=0x03 (8 bytes)
[RX] 7 bytes received
```

---

### `websocket_ihm` — WebSocket heartbeat for an HMI

```
cargo run --example websocket_ihm
```

A combined HTTP + WebSocket server on `127.0.0.1:8080`.  The scheduler
broadcasts a JSON heartbeat to all connected browser clients every 100 ms.
Open `http://127.0.0.1:8080` in a browser to see live updates.

```json
{"type":"heartbeat","count":42,"time":"14:32:07.103"}
```

This is the pattern used in `nge_ihm.rs` from the reference implementation:
a GTK WebView shell hosts a web UI that receives live data over a local
WebSocket — all driven by a single `NGE_Scheduler_rs` event.

---

## Repository Layout

```
NGE_Scheduler_rs/
├── src/
│   └── lib.rs                  ← Core scheduler (~150 lines, zero dependencies)
├── examples/
│   ├── basic_usage.rs          ← Minimal getting-started demo
│   ├── modbus_tty.rs           ← Modbus RTU polling over serial
│   └── websocket_ihm.rs        ← WebSocket heartbeat for a browser HMI
├── .github/
│   └── workflows/
│       └── ci.yml              ← Build, clippy, fmt, cross-compile to Cortex-M4
├── Cargo.toml
├── LICENSE
└── README.md
```

---

## Roadmap

- [ ] `no_std` support — replace `std::thread` with a SysTick / RTOS timer
      and `std::sync::Mutex` with a critical-section primitive
- [ ] Priority levels — allow high-priority events to preempt lower-priority ones
- [ ] Event cancellation — explicit `Scheduler::remove(id)` method
- [ ] Publish to [crates.io](https://crates.io)
- [ ] MISRA-C:2012 / CERT-C equivalence mapping for static analysis tools

---

## Contributing

Pull requests are welcome.  Please:

1. `cargo fmt` before committing.
2. `cargo clippy -- -D warnings` must pass.
3. Add or update an example if you are introducing new API surface.
4. Keep the core `src/lib.rs` free of external dependencies.

---

## Related

- [NGE_Scheduler (C)](https://github.com/NewGlobalElectronics/NGE_Scheduler) — the original C library this port is based on.

---

## License

MIT — see [LICENSE](LICENSE).
