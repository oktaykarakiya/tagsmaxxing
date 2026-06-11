// SPDX-License-Identifier: AGPL-3.0-or-later

//! CLI argument definitions for the `kb` binary (plan §10, §12, P6-T1).
//!
//! Parsed with [`clap`] derive macros. The [`Cli`] struct is the top-level entry point;
//! subcommands are [`Command::Ingest`] and [`Command::Search`]. Pure-logic tests for
//! argument parsing live in the `tests` module at the bottom of this file.

use clap::{Args, Parser, Subcommand};

/// Local File Knowledge Base (experimental pre-release) — ingest and search your files from the command line.
///
/// Both commands load configuration from `config.toml` (overlaid with environment
/// variables, per plan §6 and P0-T5). Run `kb help` for the full flag reference.
#[derive(Parser, Debug)]
#[command(name = "kb", version, about)]
pub struct Cli {
    /// Tenant to operate on (default: 1, the bootstrap tenant).
    #[arg(long, global = true, default_value = "1", value_name = "ID")]
    pub tenant_id: i64,

    /// The subcommand to execute (`ingest` or `search`).
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level subcommands for the `kb` binary.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Ingest one or more files into the knowledge base.
    ///
    /// Each file is read from disk, extracted, tagged, chunked, embedded, and stored.
    /// Use `--as-document` to group multiple files as pages of a single document.
    /// Use `--no-wait` to enqueue and exit immediately (the API server's worker pool
    /// will process the job asynchronously).
    Ingest(IngestArgs),
    /// Search the knowledge base with a natural-language query.
    ///
    /// Results are ranked by relevance (hybrid vector + keyword search with reranking).
    /// Filter by document kind (`--kind image`) or tag (`--tag invoice`).
    Search(SearchArgs),
    /// Start the HTTP API server (plan §10, §12).
    ///
    /// Serves the REST API on port 9999 (configurable via config.toml or the
    /// `--port` flag) plus the Askama-rendered Web UI. The server handles auth,
    /// ingest, search, document detail, file download, job status, Prometheus
    /// metrics, and the admin panel.
    Serve(ServeArgs),
    /// Run a standalone ingest worker (distributed processing, P15-T8).
    ///
    /// Claims queued ingest jobs from the shared Postgres queue and processes
    /// them with this machine's own model backends ([[backend]] entries in its
    /// config.toml; `[worker] use_db_routing = false` by default for exact
    /// per-machine capacity). Multiple heterogeneous workers — each with its
    /// own config and `--concurrency` — drain the same queue. Requires the
    /// shared blob store (`[blob] backend = "s3"`) when running on a machine
    /// other than the API node.
    Worker(WorkerArgs),
}

/// Arguments for `kb ingest`.
#[derive(Args, Clone, Debug)]
pub struct IngestArgs {
    /// One or more file paths to ingest (required).
    #[arg(required = true, value_name = "FILES...")]
    pub files: Vec<String>,

    /// Group all files as ordered pages of a single document.
    ///
    /// Without this flag each file becomes a separate 1-page document.
    #[arg(long)]
    pub as_document: bool,

    /// Optional free-text note attached to the document.
    #[arg(long, value_name = "NOTE")]
    pub note: Option<String>,

    /// Enqueue the job and exit immediately without waiting for completion.
    ///
    /// The job will be processed asynchronously by a running API server worker pool.
    /// Without this flag the CLI blocks until ingestion finishes.
    #[arg(long)]
    pub no_wait: bool,
}

/// Arguments for `kb search`.
#[derive(Args, Clone, Debug)]
pub struct SearchArgs {
    /// Natural-language search query (required).
    #[arg(value_name = "QUERY")]
    pub query: String,

    /// Restrict results to this document kind.
    ///
    /// Valid values: document, image, audio, video, code, archive, binary,
    /// identity_document. Unknown values are ignored (no kind filter is applied).
    #[arg(long, value_name = "KIND")]
    pub kind: Option<String>,

    /// Restrict results to documents carrying this tag.
    #[arg(long, value_name = "TAG")]
    pub tag: Option<String>,

    /// Maximum number of results to return (default: 10).
    #[arg(long, default_value = "10", value_name = "N")]
    pub limit: usize,
}

/// Arguments for `kb serve`.
#[derive(Args, Clone, Debug)]
pub struct ServeArgs {
    /// TCP port to listen on (overrides config.toml `api.port`).
    #[arg(long, value_name = "PORT")]
    pub port: Option<u16>,

    /// Path to the config.toml file.
    #[arg(long, value_name = "PATH", default_value = "config.toml")]
    pub config: String,

    /// Path to a TLS certificate PEM file (enables HTTPS).
    /// Must be used together with `--tls-key`.
    #[arg(long, value_name = "PATH")]
    pub tls_cert: Option<String>,

    /// Path to a TLS private key PEM file (enables HTTPS).
    /// Must be used together with `--tls-cert`.
    #[arg(long, value_name = "PATH")]
    pub tls_key: Option<String>,
}

/// Arguments for `kb worker`.
#[derive(Args, Clone, Debug)]
pub struct WorkerArgs {
    /// Path to this machine's config.toml file.
    #[arg(long, value_name = "PATH", default_value = "config.toml")]
    pub config: String,

    /// Concurrent jobs to process (overrides config.toml `[worker].concurrency`).
    #[arg(long, value_name = "N")]
    pub concurrency: Option<usize>,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_ingest_single_file() {
        let cli = Cli::try_parse_from(["kb", "ingest", "notes.txt"]).unwrap();
        assert_eq!(cli.tenant_id, 1);
        match cli.command {
            Command::Ingest(args) => {
                assert_eq!(args.files, vec!["notes.txt"]);
                assert!(!args.as_document);
                assert!(args.note.is_none());
                assert!(!args.no_wait);
            }
            other => panic!("expected Ingest, got {other:?}"),
        }
    }

    #[test]
    fn parse_ingest_multiple_files() {
        let cli = Cli::try_parse_from(["kb", "ingest", "a.txt", "b.md", "c.html"]).unwrap();
        match cli.command {
            Command::Ingest(args) => {
                assert_eq!(args.files.len(), 3);
                assert_eq!(args.files[1], "b.md");
            }
            other => panic!("expected Ingest, got {other:?}"),
        }
    }

    #[test]
    fn parse_ingest_with_flags() {
        let cli = Cli::try_parse_from([
            "kb",
            "ingest",
            "--as-document",
            "--note",
            "vacation scans",
            "front.jpg",
            "back.jpg",
        ])
        .unwrap();
        match cli.command {
            Command::Ingest(args) => {
                assert!(args.as_document);
                assert_eq!(args.note.as_deref(), Some("vacation scans"));
                assert_eq!(args.files, vec!["front.jpg", "back.jpg"]);
            }
            other => panic!("expected Ingest, got {other:?}"),
        }
    }

    #[test]
    fn parse_ingest_no_wait() {
        let cli = Cli::try_parse_from(["kb", "ingest", "--no-wait", "large_video.mp4"]).unwrap();
        match cli.command {
            Command::Ingest(args) => {
                assert!(args.no_wait);
                assert!(!args.as_document);
            }
            other => panic!("expected Ingest, got {other:?}"),
        }
    }

    #[test]
    fn parse_search_basic() {
        let cli = Cli::try_parse_from(["kb", "search", "quarterly report"]).unwrap();
        match cli.command {
            Command::Search(args) => {
                assert_eq!(args.query, "quarterly report");
                assert_eq!(args.limit, 10);
                assert!(args.kind.is_none());
                assert!(args.tag.is_none());
            }
            other => panic!("expected Search, got {other:?}"),
        }
    }

    #[test]
    fn parse_search_with_filters() {
        let cli = Cli::try_parse_from([
            "kb",
            "search",
            "--kind",
            "image",
            "--tag",
            "screenshot",
            "--limit",
            "5",
            "desktop",
        ])
        .unwrap();
        match cli.command {
            Command::Search(args) => {
                assert_eq!(args.query, "desktop");
                assert_eq!(args.kind.as_deref(), Some("image"));
                assert_eq!(args.tag.as_deref(), Some("screenshot"));
                assert_eq!(args.limit, 5);
            }
            other => panic!("expected Search, got {other:?}"),
        }
    }

    #[test]
    fn parse_tenant_id_global_flag() {
        let cli = Cli::try_parse_from(["kb", "--tenant-id", "42", "search", "test"]).unwrap();
        assert_eq!(cli.tenant_id, 42);
    }

    #[test]
    fn parse_ingest_requires_at_least_one_file() {
        let result = Cli::try_parse_from(["kb", "ingest"]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_search_help_includes_all_flags() {
        // Verify help text mentions the key flags (non-exhaustive smoke-test).
        let cli = Cli::try_parse_from(["kb", "search", "--help"]).unwrap_err();
        let msg = cli.to_string();
        assert!(msg.contains("--kind"));
        assert!(msg.contains("--tag"));
        assert!(msg.contains("--limit"));
    }

    #[test]
    fn parse_ingest_help_includes_all_flags() {
        let cli = Cli::try_parse_from(["kb", "ingest", "--help"]).unwrap_err();
        let msg = cli.to_string();
        assert!(msg.contains("--as-document"));
        assert!(msg.contains("--note"));
        assert!(msg.contains("--no-wait"));
    }

    #[test]
    fn default_tenant_id_is_one() {
        let cli = Cli::try_parse_from(["kb", "ingest", "f.txt"]).unwrap();
        assert_eq!(cli.tenant_id, 1);
    }

    #[test]
    fn search_default_limit_is_ten() {
        let cli = Cli::try_parse_from(["kb", "search", "q"]).unwrap();
        match cli.command {
            Command::Search(args) => assert_eq!(args.limit, 10),
            other => panic!("expected Search, got {other:?}"),
        }
    }

    #[test]
    fn parse_serve_defaults() {
        let cli = Cli::try_parse_from(["kb", "serve"]).unwrap();
        match cli.command {
            Command::Serve(args) => {
                assert!(args.port.is_none());
                assert_eq!(args.config, "config.toml");
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    #[test]
    fn parse_serve_with_port() {
        let cli = Cli::try_parse_from(["kb", "serve", "--port", "8080"]).unwrap();
        match cli.command {
            Command::Serve(args) => {
                assert_eq!(args.port, Some(8080));
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    #[test]
    fn parse_serve_with_config() {
        let cli = Cli::try_parse_from(["kb", "serve", "--config", "/etc/kb/config.toml"]).unwrap();
        match cli.command {
            Command::Serve(args) => {
                assert_eq!(args.config, "/etc/kb/config.toml");
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    #[test]
    fn parse_serve_with_tenant_id() {
        let cli =
            Cli::try_parse_from(["kb", "--tenant-id", "5", "serve", "--port", "3000"]).unwrap();
        assert_eq!(cli.tenant_id, 5);
        match cli.command {
            Command::Serve(args) => {
                assert_eq!(args.port, Some(3000));
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    // ── kb worker (P15-T8) ─────────────────────────────────────────────────

    #[test]
    fn parse_worker_defaults() {
        let cli = Cli::try_parse_from(["kb", "worker"]).unwrap();
        match cli.command {
            Command::Worker(args) => {
                assert_eq!(args.config, "config.toml");
                assert!(args.concurrency.is_none(), "concurrency defaults to config");
            }
            other => panic!("expected Worker, got {other:?}"),
        }
    }

    #[test]
    fn parse_worker_with_concurrency_and_config() {
        let cli = Cli::try_parse_from([
            "kb",
            "worker",
            "--config",
            "/etc/kb/worker.toml",
            "--concurrency",
            "6",
        ])
        .unwrap();
        match cli.command {
            Command::Worker(args) => {
                assert_eq!(args.config, "/etc/kb/worker.toml");
                assert_eq!(args.concurrency, Some(6));
            }
            other => panic!("expected Worker, got {other:?}"),
        }
    }

    #[test]
    fn parse_worker_rejects_zero_is_not_special() {
        // `--concurrency 0` parses (clap level); worker_main clamps it to ≥ 1.
        let cli = Cli::try_parse_from(["kb", "worker", "--concurrency", "0"]).unwrap();
        match cli.command {
            Command::Worker(args) => assert_eq!(args.concurrency, Some(0)),
            other => panic!("expected Worker, got {other:?}"),
        }
    }

    #[test]
    fn parse_worker_rejects_non_numeric_concurrency() {
        assert!(Cli::try_parse_from(["kb", "worker", "--concurrency", "lots"]).is_err());
    }
}
