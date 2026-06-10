//! CLI command handlers — thin dispatch from parsed arguments to pipeline calls.
//!
//! Each subcommand has a `run_*` function that takes the parsed [`clap`] args plus
//! the wired-up pipeline components and produces output on stdout. These functions
//! are kept free of I/O wiring so they can be tested with mock pipeline
//! implementations (plan §31.3).

pub mod ingest;
pub mod runtime;
pub mod search;
pub mod serve;
