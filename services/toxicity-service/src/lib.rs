//! Library half of `toxicity-service`, exists so other crates in this
//! workspace (`toxicity-client-rs`, and anything else that needs to speak
//! this wire format) can depend on the protocol types without duplicating
//! them, a copy-pasted `ServerMessage` in two crates is a drift risk the
//! moment one of them changes.
//!
//! Everything else (`config`, `server`, `worker`, the binary's internal
//! `ServiceError`) stays private to the `toxicity-service` binary target,
//! those are implementation details of running the service, not part of
//! what a client needs.

pub mod protocol;
