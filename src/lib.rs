#![forbid(unsafe_code)]

//! Nexus Market Observer service library.
//!
//! The crate is intentionally a single service with explicit boundaries for market-data
//! ingestion, in-memory state, analytics, persistence, HTTP delivery, and telemetry. The live
//! and replay collectors converge on the same domain state so reliability behavior can be
//! demonstrated without presenting replayed data as live Nexus data.

pub mod analytics;
pub mod api;
pub mod app;
pub mod config;
pub mod error;
pub mod nexus;
pub mod persistence;
pub mod replay;
pub mod state;
pub mod telemetry;
