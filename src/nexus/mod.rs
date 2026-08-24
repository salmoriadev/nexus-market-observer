//! Nexus Exchange integration boundary.
//!
//! [`adapter`] contains the official SDK-facing REST and subscription calls. [`stream`] translates
//! SDK events into the service lifecycle and uses authoritative REST snapshots whenever stream
//! continuity cannot prove a complete local order book.

pub mod adapter;
pub mod stream;

pub use adapter::{NexusAdapter, NexusSnapshot};
pub use stream::{classify_sdk_event, run_nexus_collector, run_stale_detector, CollectorEvent};
