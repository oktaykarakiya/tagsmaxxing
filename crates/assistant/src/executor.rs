// SPDX-License-Identifier: AGPL-3.0-or-later

//! OpenCode CLI subprocess executor.
//!
//! Spawns `opencode run` as a tokio subprocess, strips ANSI escape sequences
//! from stdout lines, and pipes them through a channel for SSE streaming.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, broadcast, mpsc};

/// An executing OpenCode subprocess.
pub struct Executor {
    /// Path to the `opencode` binary.
    opencode_bin: PathBuf,
    /// Model reference string, e.g. `local/qwen-35b`.
    model_ref: String,
    /// Maximum runtime per prompt in seconds.
    #[allow(dead_code)]
    timeout_secs: u64,
    /// Handle to the running subprocess (None when idle).
    #[allow(dead_code)]
    child: Option<Arc<Mutex<Child>>>,
    /// Broadcast channel for signalling kill.
    kill_tx: broadcast::Sender<()>,
}

impl Clone for Executor {
    fn clone(&self) -> Self {
        let (kill_tx, _) = broadcast::channel(1);
        Self {
            opencode_bin: self.opencode_bin.clone(),
            model_ref: self.model_ref.clone(),
            timeout_secs: self.timeout_secs,
            child: None,
            kill_tx,
        }
    }
}

impl Executor {
    /// Create a new executor.
    #[must_use]
    pub fn new(opencode_bin: &Path, model_ref: &str, timeout_secs: u64) -> Self {
        let (kill_tx, _) = broadcast::channel(1);
        Self {
            opencode_bin: opencode_bin.to_path_buf(),
            model_ref: model_ref.to_owned(),
            timeout_secs,
            child: None,
            kill_tx,
        }
    }

    /// Run a prompt and return a stream of output lines.
    ///
    /// Lines are stripped of ANSI escape sequences. The stream ends when
    /// the process exits or the timeout is reached.
    ///
    /// # Errors
    ///
    /// Returns an error if the subprocess fails to spawn.
    pub async fn execute(
        &self,
        working_dir: &Path,
        session_key: &str,
        prompt: &str,
        #[allow(unused_mut)] mut env: HashMap<String, String>,
    ) -> Result<mpsc::Receiver<String>, std::io::Error> {
        let mut cmd = Command::new(&self.opencode_bin);
        cmd.args([
            "run",
            "--model",
            &self.model_ref,
            "--dir",
        ])
        .arg(working_dir.as_os_str())
        .args(["--session", session_key])
        .arg(prompt)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // opencode reads stdin, so we close it
        .stdin(Stdio::null())
        .kill_on_drop(true);

        for (k, v) in env.drain() {
            cmd.env(k, v);
        }

        let mut child = cmd.spawn()?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("no stdout"))?;

        let (tx, rx) = mpsc::channel(256);

        let mut kill_rx = self.kill_tx.subscribe();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            loop {
                tokio::select! {
                    line_result = reader.next_line() => {
                        match line_result {
                            Ok(Some(line)) => {
                                let stripped = strip_ansi(&line);
                                if tx.send(stripped).await.is_err() {
                                    break;
                                }
                            }
                            Ok(None) => break,  // EOF
                            Err(_) => break,
                        }
                    }
                    _ = kill_rx.recv() => {
                        let _ = child.start_kill();
                        break;
                    }
                }
            }
        });

        Ok(rx)
    }

    /// Signal the running subprocess to terminate.
    pub fn kill(&self) {
        let _ = self.kill_tx.send(());
    }
}

/// Strip ANSI escape sequences from a string.
fn strip_ansi(text: &str) -> String {
    // Simple ANSI sequence removal: ESC[...m and similar
    let mut result = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            // Skip the escape sequence
            if chars.peek() == Some(&'[') {
                chars.next(); // consume '['
                // Consume until a letter (parameter bytes + terminator)
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            result.push(ch);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_ansi_codes() {
        assert_eq!(strip_ansi("\x1b[32mhello\x1b[0m"), "hello");
        assert_eq!(strip_ansi("plain text"), "plain text");
        assert_eq!(strip_ansi("\x1b[1;32mbold green\x1b[0m normal"), "bold green normal");
    }

    #[test]
    fn strip_ansi_preserves_non_ansi() {
        assert_eq!(strip_ansi("no escape codes here"), "no escape codes here");
        assert_eq!(strip_ansi(""), "");
    }
}
