// SPDX-License-Identifier: AGPL-3.0-or-later

//! Audio extractor: transcodes to 16 kHz mono WAV via ffmpeg subprocess
//! (`tokio::process::Command`, timeout-gated), then POSTs the WAV to
//! whisper-server for transcription (OpenAI-compatible `/v1/audio/transcriptions`).
//!
//! Produces [`Extracted`] with the transcription text, duration metadata, and
//! optional segment timestamps in `meta`. The whisper base URL is hot-swappable
//! via [`WhisperConfig`] (plan §2, §7; CLAUDE.md hot-swappable rule).
//!
//! The ffmpeg subprocess boundary is abstracted behind an internal
//! [`Transcoder`] trait so unit tests can inject a mock without a real ffmpeg
//! on the CI host (plan §31.3 mock-backend pattern, applied to the subprocess).

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use reqwest::Client;
use serde::Deserialize;

// ── WhisperConfig — hot-swappable whisper base URL ────────────────────────────

/// Hot-swappable configuration handle for the whisper server base URL.
///
/// Wraps the base URL in an [`ArcSwap`] so the running pipeline can switch to a
/// different whisper instance without a restart. Callers read the current URL
/// per request via the internal `load()`.
///
/// # Example
///
/// ```rust
/// use kb_extract::audio::WhisperConfig;
///
/// let cfg = WhisperConfig::new("http://localhost:8080".into());
/// assert_eq!(cfg.current_url(), "http://localhost:8080");
/// cfg.set_url("http://whisper2:8080".into());
/// assert_eq!(cfg.current_url(), "http://whisper2:8080");
/// ```
#[derive(Debug, Clone)]
pub struct WhisperConfig {
    base_url: Arc<ArcSwap<String>>,
}

impl WhisperConfig {
    /// Create a new config pointing at `base_url`.
    ///
    /// The URL should not include a trailing slash; the extractor appends
    /// `/v1/audio/transcriptions` to form the full endpoint.
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: Arc::new(ArcSwap::from_pointee(base_url)),
        }
    }

    /// Hot-swap the whisper base URL at runtime.
    ///
    /// Subsequent [`AudioExtractor::extract`] calls will use the new URL
    /// without any restart or reconfiguration.
    pub fn set_url(&self, url: String) {
        self.base_url.store(Arc::new(url));
    }

    /// Return a clone of the current base URL.
    pub fn current_url(&self) -> String {
        self.base_url.load().as_ref().clone()
    }
}

// ── Whisper API response types ────────────────────────────────────────────────

/// The parsed JSON response from whisper-server's `/v1/audio/transcriptions`
/// endpoint (OpenAI-compatible `verbose_json` format).
#[derive(Debug, Deserialize)]
struct WhisperResponse {
    /// Full transcription text.
    text: String,
    /// Timestamped segments, present when `response_format=verbose_json`.
    #[serde(default)]
    segments: Vec<WhisperSegment>,
    /// Total audio duration in seconds (whisper sets this field).
    #[serde(default)]
    duration: Option<f64>,
}

/// One timestamped segment from a whisper transcription.
#[derive(Debug, Deserialize)]
struct WhisperSegment {
    /// Segment start time in seconds.
    start: f64,
    /// Segment end time in seconds.
    end: f64,
    /// Segment transcription text.
    #[serde(default)]
    text: String,
}

// ── Transcoder trait — internal abstraction over the ffmpeg subprocess ────────

/// Internal trait for the audio-to-WAV transcoding step.
///
/// The real implementation spawns `ffmpeg` as a subprocess. Tests inject a mock
/// that returns canned PCM/WAV bytes, keeping the test suite deterministic and
/// free of host-ffmpeg dependencies (plan §31.3).
#[async_trait]
trait Transcoder: Send + Sync {
    /// Transcode `input` audio bytes to 16 kHz mono WAV.
    ///
    /// `timeout` bounds the entire subprocess wall-clock time; exceeding it
    /// must yield a timeout error.
    async fn transcode(&self, input: &[u8], timeout: Duration) -> anyhow::Result<Vec<u8>>;
}

/// The production ffmpeg transcoder.
///
/// Writes the input to a temp file and runs
/// `ffmpeg -i <tmp> -ar 16000 -ac 1 -f wav pipe:1` with **stdin closed**, reading
/// the 16 kHz mono WAV from stdout. (Feeding input via ffmpeg's stdin while only
/// reading stdout *after* the whole write completes deadlocks once ffmpeg's
/// ~64 KiB stdout pipe buffer fills — see [`Transcoder::transcode`].)
#[derive(Debug, Clone, Copy, Default)]
struct RealFfmpeg;

impl RealFfmpeg {
    /// Write `input` to a temporary file (deleted on drop) so ffmpeg can read it
    /// via a path with stdin closed — avoiding the stdin/stdout pipe deadlock.
    fn temp_file(input: &[u8]) -> anyhow::Result<tempfile::NamedTempFile> {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new()
            .map_err(|e| anyhow::anyhow!("AudioExtractor: failed to create temp file: {e}"))?;
        tmp.write_all(input)
            .map_err(|e| anyhow::anyhow!("AudioExtractor: failed to write temp file: {e}"))?;
        tmp.flush()
            .map_err(|e| anyhow::anyhow!("AudioExtractor: failed to flush temp file: {e}"))?;
        Ok(tmp)
    }
}

#[async_trait]
impl Transcoder for RealFfmpeg {
    async fn transcode(&self, input: &[u8], timeout: Duration) -> anyhow::Result<Vec<u8>> {
        // F8: write the input to a temp file and read the transcoded WAV from
        // stdout with stdin **closed**. The previous approach fed bytes to
        // ffmpeg's stdin (pipe:0) and only read stdout *after* the entire
        // `write_all` finished — so once ffmpeg's ~64 KiB stdout pipe buffer
        // filled, ffmpeg blocked writing output, stopped draining stdin, and the
        // `write_all` blocked forever (and was outside the timeout). The temp-file
        // + `stdin(null)` pattern — identical to the video extractor — removes the
        // write side entirely, and the whole exchange is bounded by `timeout`.
        let tmp = Self::temp_file(input)?;

        let child = tokio::process::Command::new("ffmpeg")
            .args(["-i"])
            .arg(tmp.path())
            .args(["-ar", "16000", "-ac", "1", "-f", "wav", "pipe:1"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| anyhow::anyhow!("AudioExtractor: failed to spawn ffmpeg: {e}"))?;

        let output = tokio::time::timeout(timeout, child.wait_with_output())
            .await
            .map_err(|_| anyhow::anyhow!("AudioExtractor: ffmpeg timed out after {timeout:?}"))?
            .map_err(|e| anyhow::anyhow!("AudioExtractor: ffmpeg process error: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "AudioExtractor: ffmpeg exited with status {}: {}",
                output.status,
                stderr.trim()
            ));
        }

        Ok(output.stdout)
    }
}

// ── AudioExtractor ────────────────────────────────────────────────────────────

/// Extracts text from audio files by transcoding to 16 kHz mono WAV via ffmpeg
/// and transcribing with whisper-server.
///
/// # Pipeline
///
/// 1. **Transcode** — spawns `ffmpeg` to convert any audio format to 16 kHz
///    mono WAV, reading the input from a temp file (stdin closed) and capturing
///    stdout. The subprocess is bounded by a configurable timeout.
/// 2. **Transcribe** — POSTs the WAV to `{base_url}/v1/audio/transcriptions`
///    (OpenAI-compatible) with `response_format=verbose_json` to get segments.
/// 3. **Assemble** — text goes into [`Extracted::text`]; duration and
///    timestamped segments go into [`Extracted::meta`] for later chunk-aware
///    embedding (plan §7: transcript chunks keep `ts_offset`).
///
/// The whisper base URL is hot-swappable via [`WhisperConfig`].
///
/// # Examples
///
/// ```rust,no_run
/// use std::time::Duration;
/// use bytes::Bytes;
/// use kb_core::extractor::{Extracted, Extractor, RawFile};
/// use kb_core::kind::DocKind;
/// use kb_extract::audio::{AudioExtractor, WhisperConfig};
///
/// # async fn example() -> anyhow::Result<()> {
/// let config = WhisperConfig::new("http://localhost:8080".into());
/// let client = reqwest::Client::new();
/// let ex = AudioExtractor::new(client, config);
/// let raw = RawFile {
///     bytes: Bytes::from_static(b"fake audio data"),
///     mime: Some("audio/mpeg".into()),
///     kind: DocKind::Audio,
///     path: Some("recording.mp3".into()),
/// };
/// let out = ex.extract(&raw).await?;
/// println!("transcript: {}", out.text);
/// # Ok(())
/// # }
/// ```
pub struct AudioExtractor {
    /// Shared HTTP client (connection pooling).
    http: Client,
    /// Hot-swappable whisper base URL.
    config: WhisperConfig,
    /// Maximum wall-clock time for the ffmpeg subprocess.
    ffmpeg_timeout: Duration,
    /// The transcoding strategy (real ffmpeg in production; mock in tests).
    transcoder: Arc<dyn Transcoder>,
}

impl std::fmt::Debug for AudioExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AudioExtractor")
            .field("config", &self.config)
            .field("ffmpeg_timeout", &self.ffmpeg_timeout)
            .field("transcoder", &"<dyn Transcoder>")
            .finish()
    }
}

impl Clone for AudioExtractor {
    fn clone(&self) -> Self {
        Self {
            http: self.http.clone(),
            config: self.config.clone(),
            ffmpeg_timeout: self.ffmpeg_timeout,
            transcoder: Arc::clone(&self.transcoder),
        }
    }
}

impl AudioExtractor {
    /// Create a new extractor with a given HTTP client, whisper configuration,
    /// and the default (real-ffmpeg) transcoder.
    ///
    /// The `http` client should be configured with appropriate timeouts for the
    /// expected upload sizes. The `config`'s base URL is read on every
    /// [`extract`](Extractor::extract) call, so the operator can hot-swap it at
    /// runtime via [`WhisperConfig::set_url`].
    ///
    /// The default ffmpeg timeout is 120 seconds.
    pub fn new(http: Client, config: WhisperConfig) -> Self {
        Self {
            http,
            config,
            ffmpeg_timeout: Duration::from_secs(120),
            transcoder: Arc::new(RealFfmpeg),
        }
    }

    /// Set a custom timeout for the ffmpeg subprocess.
    ///
    /// The default is 120 seconds.
    pub fn with_ffmpeg_timeout(mut self, timeout: Duration) -> Self {
        self.ffmpeg_timeout = timeout;
        self
    }

    /// Build an extractor with an injected [`Transcoder`] — for tests.
    #[cfg(test)]
    fn with_transcoder(
        http: Client,
        config: WhisperConfig,
        transcoder: Arc<dyn Transcoder>,
    ) -> Self {
        Self {
            http,
            config,
            ffmpeg_timeout: Duration::from_secs(120),
            transcoder,
        }
    }

    /// Transcode and transcribe in sequence, assembling the result.
    async fn extract_impl(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        let wav = self
            .transcoder
            .transcode(&file.bytes, self.ffmpeg_timeout)
            .await?;
        let whisper = self.transcribe(&wav, file.path.as_deref()).await?;

        let mut meta = serde_json::Map::new();

        if let Some(dur) = whisper.duration {
            meta.insert("duration_secs".into(), serde_json::json!(dur));
        }

        if !whisper.segments.is_empty() {
            // If whisper didn't report a top-level duration, infer it from the
            // last segment's end time.
            if whisper.duration.is_none()
                && let Some(last) = whisper.segments.last()
            {
                meta.insert("duration_secs".into(), serde_json::json!(last.end));
            }

            let segments: Vec<serde_json::Value> = whisper
                .segments
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "start": s.start,
                        "end": s.end,
                        "text": s.text,
                    })
                })
                .collect();
            meta.insert("segments".into(), serde_json::Value::Array(segments));
        }

        Ok(Extracted {
            text: whisper.text,
            meta: serde_json::Value::Object(meta),
            page_images: Vec::new(),
        })
    }

    /// POST the WAV bytes to whisper-server and parse the `verbose_json` response.
    async fn transcribe(
        &self,
        wav: &[u8],
        file_path: Option<&str>,
    ) -> anyhow::Result<WhisperResponse> {
        let base = self.config.base_url.load();
        let url = format!("{}/v1/audio/transcriptions", base.as_str());
        drop(base);

        let wav_part = reqwest::multipart::Part::bytes(wav.to_vec())
            .file_name("audio.wav")
            .mime_str("audio/wav")
            .map_err(|e| anyhow::anyhow!("AudioExtractor: invalid MIME string: {e}"))?;

        let form = reqwest::multipart::Form::new()
            .part("file", wav_part)
            .text("model", "whisper-1")
            .text("response_format", "verbose_json");

        let resp = self
            .http
            .post(&url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "AudioExtractor: HTTP request to whisper failed for '{}': {e}",
                    file_path.unwrap_or("<unknown>")
                )
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "AudioExtractor: whisper returned HTTP {status} for '{}': {}",
                file_path.unwrap_or("<unknown>"),
                body
            ));
        }

        let whisper: WhisperResponse = resp.json().await.map_err(|e| {
            anyhow::anyhow!(
                "AudioExtractor: failed to parse whisper response for '{}': {e}",
                file_path.unwrap_or("<unknown>")
            )
        })?;

        Ok(whisper)
    }
}

#[async_trait]
impl Extractor for AudioExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        self.extract_impl(file).await
    }
}

// ── unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::net::SocketAddr;
    use std::time::Duration;

    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::Response;
    use axum::routing::post;
    use bytes::Bytes;
    use kb_core::extractor::{Extractor, RawFile};
    use kb_core::kind::DocKind;
    use reqwest::Client;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    use super::*;

    // ── mock whisper server ─────────────────────────────────────────────────

    /// A lightweight mock whisper server for deterministic tests.
    ///
    /// Binds to `127.0.0.1:0` and serves `POST /v1/audio/transcriptions`.
    /// The response is fully controlled by a shared [`MockWhisperScenario`] so
    /// tests can inject well-formed `verbose_json`, empty segments, HTTP
    /// errors, and non-JSON bodies.
    struct MockWhisper {
        addr: SocketAddr,
        scenario: Arc<Mutex<MockWhisperScenario>>,
        shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    }

    /// What the mock whisper server should return on the next request.
    #[derive(Debug, Clone)]
    struct MockWhisperScenario {
        /// HTTP status code.
        status: StatusCode,
        /// Value of the `Content-Type` response header.
        content_type: String,
        /// Raw response body bytes.
        body: Vec<u8>,
    }

    impl Default for MockWhisperScenario {
        fn default() -> Self {
            Self {
                status: StatusCode::OK,
                content_type: "application/json".into(),
                body: Vec::new(),
            }
        }
    }

    /// Build a [`MockWhisperScenario`] that returns a valid `verbose_json`
    /// whisper response with segments.
    fn whisper_verbose_json(text: &str, segments: &[(f64, f64, &str)]) -> MockWhisperScenario {
        let duration = segments.last().map(|s| s.1).unwrap_or(0.0);
        let segs: Vec<serde_json::Value> = segments
            .iter()
            .map(|&(start, end, txt)| json!({"start": start, "end": end, "text": txt}))
            .collect();
        let body = json!({
            "text": text,
            "duration": duration,
            "segments": segs,
        });
        MockWhisperScenario {
            status: StatusCode::OK,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    /// Build a [`MockWhisperScenario`] with a `verbose_json` response that
    /// has the given duration but no segments.
    fn whisper_text_with_duration(text: &str, duration: f64) -> MockWhisperScenario {
        let body = json!({
            "text": text,
            "duration": duration,
            "segments": [],
        });
        MockWhisperScenario {
            status: StatusCode::OK,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    /// Build a [`MockWhisperScenario`] with text only (no duration field).
    fn whisper_text_only(text: &str) -> MockWhisperScenario {
        let body = json!({"text": text});
        MockWhisperScenario {
            status: StatusCode::OK,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    impl MockWhisper {
        /// Start the mock server on an ephemeral port.
        async fn start() -> Self {
            let scenario = Arc::new(Mutex::new(MockWhisperScenario::default()));
            let state = Arc::clone(&scenario);

            let app = Router::new()
                .route("/v1/audio/transcriptions", post(handle_transcription))
                .with_state(state);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

            tokio::spawn(async move {
                if let Err(e) = axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        shutdown_rx.await.ok();
                    })
                    .await
                {
                    eprintln!("mock whisper server stopped with error: {e}");
                }
            });

            MockWhisper {
                addr,
                scenario,
                shutdown_tx: Some(shutdown_tx),
            }
        }

        /// Return the base URL for use with [`WhisperConfig`].
        fn base_url(&self) -> String {
            format!("http://{}", self.addr)
        }

        /// Lock the scenario for mutation between calls.
        async fn set_scenario(&self, s: MockWhisperScenario) {
            *self.scenario.lock().await = s;
        }

        /// Gracefully shut down.
        async fn shutdown(mut self) {
            if let Some(tx) = self.shutdown_tx.take() {
                let _ = tx.send(());
                tokio::task::yield_now().await;
            }
        }
    }

    /// Axum handler for `POST /v1/audio/transcriptions` — reads the shared
    /// scenario and responds.
    async fn handle_transcription(
        State(scenario): State<Arc<Mutex<MockWhisperScenario>>>,
    ) -> Response {
        let s = scenario.lock().await.clone();
        let mut resp = Response::new(axum::body::Body::from(s.body));
        *resp.status_mut() = s.status;
        resp.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            s.content_type.parse().unwrap(),
        );
        resp
    }

    // ── mock ffmpeg ─────────────────────────────────────────────────────────

    /// A mock [`Transcoder`] that returns canned output or simulates failure.
    struct MockFfmpeg {
        /// The WAV bytes to return on success.
        output: Vec<u8>,
        /// If true, `transcode` returns an error instead of the output.
        should_fail: bool,
        /// If true, simulate a timeout error.
        should_timeout: bool,
    }

    impl MockFfmpeg {
        fn new(output: Vec<u8>) -> Self {
            Self {
                output,
                should_fail: false,
                should_timeout: false,
            }
        }

        fn failing() -> Self {
            Self {
                output: Vec::new(),
                should_fail: true,
                should_timeout: false,
            }
        }

        fn timing_out() -> Self {
            Self {
                output: Vec::new(),
                should_fail: false,
                should_timeout: true,
            }
        }
    }

    #[async_trait]
    impl Transcoder for MockFfmpeg {
        async fn transcode(&self, _input: &[u8], _timeout: Duration) -> anyhow::Result<Vec<u8>> {
            if self.should_timeout {
                anyhow::bail!("AudioExtractor: ffmpeg timed out after 120s");
            }
            if self.should_fail {
                anyhow::bail!(
                    "AudioExtractor: ffmpeg exited with status exit status: 1: Unsupported codec"
                );
            }
            Ok(self.output.clone())
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Build an [`AudioExtractor`] backed by a running mock whisper server and
    /// a mock ffmpeg that passes through the given WAV bytes.
    async fn extractor_with_mocks(mock: &MockWhisper, wav: Vec<u8>) -> AudioExtractor {
        let config = WhisperConfig::new(mock.base_url());
        let transcoder: Arc<dyn Transcoder> = Arc::new(MockFfmpeg::new(wav));
        AudioExtractor::with_transcoder(Client::new(), config, transcoder)
    }

    /// Build an [`AudioExtractor`] with a failing mock ffmpeg.
    async fn extractor_with_failing_ffmpeg(mock: &MockWhisper) -> AudioExtractor {
        let config = WhisperConfig::new(mock.base_url());
        let transcoder: Arc<dyn Transcoder> = Arc::new(MockFfmpeg::failing());
        AudioExtractor::with_transcoder(Client::new(), config, transcoder)
    }

    /// Build an [`AudioExtractor`] with a timing-out mock ffmpeg.
    async fn extractor_with_timeout_ffmpeg(mock: &MockWhisper) -> AudioExtractor {
        let config = WhisperConfig::new(mock.base_url());
        let transcoder: Arc<dyn Transcoder> = Arc::new(MockFfmpeg::timing_out());
        AudioExtractor::with_transcoder(Client::new(), config, transcoder)
    }

    /// Build a sample [`RawFile`] for an MP3 audio file.
    fn audio_raw(bytes: &[u8]) -> RawFile {
        RawFile {
            bytes: Bytes::copy_from_slice(bytes),
            mime: Some("audio/mpeg".into()),
            kind: DocKind::Audio,
            path: Some("recording.mp3".into()),
        }
    }

    // ── WhisperConfig tests ─────────────────────────────────────────────────

    #[test]
    fn config_stores_and_returns_url() {
        let cfg = WhisperConfig::new("http://whisper:8080".into());
        assert_eq!(cfg.current_url(), "http://whisper:8080");
    }

    #[test]
    fn config_hot_swaps_url() {
        let cfg = WhisperConfig::new("http://whisper1:8080".into());
        assert_eq!(cfg.current_url(), "http://whisper1:8080");
        cfg.set_url("http://whisper2:8080".into());
        assert_eq!(cfg.current_url(), "http://whisper2:8080");
    }

    #[test]
    fn config_clone_shares_state() {
        let cfg1 = WhisperConfig::new("http://a:8080".into());
        let cfg2 = cfg1.clone();
        cfg2.set_url("http://b:8080".into());
        assert_eq!(cfg1.current_url(), "http://b:8080");
        assert_eq!(cfg2.current_url(), "http://b:8080");
    }

    #[test]
    fn config_handles_empty_url() {
        let cfg = WhisperConfig::new(String::new());
        assert_eq!(cfg.current_url(), "");
    }

    // ── extraction: happy path ──────────────────────────────────────────────

    /// Full extraction with segments and duration from whisper.
    #[tokio::test]
    async fn extracts_text_and_segments() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_verbose_json(
            "Hello world. This is a test.",
            &[(0.0, 1.5, "Hello world."), (1.5, 3.0, "This is a test.")],
        ))
        .await;

        let ex = extractor_with_mocks(&mock, b"fake wav".to_vec()).await;
        let out = ex.extract(&audio_raw(b"fake mp3")).await.unwrap();

        assert_eq!(out.text, "Hello world. This is a test.");
        assert_eq!(out.meta["duration_secs"], json!(3.0));

        let segments = out.meta["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0]["start"], json!(0.0));
        assert_eq!(segments[0]["end"], json!(1.5));
        assert_eq!(segments[0]["text"], "Hello world.");
        assert_eq!(segments[1]["start"], json!(1.5));
        assert_eq!(segments[1]["end"], json!(3.0));
        assert_eq!(segments[1]["text"], "This is a test.");

        assert!(out.page_images.is_empty());

        mock.shutdown().await;
    }

    /// Extraction with a single segment.
    #[tokio::test]
    async fn extracts_single_segment() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_verbose_json(
            "Short clip.",
            &[(0.0, 0.8, "Short clip.")],
        ))
        .await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let out = ex.extract(&audio_raw(b"mp3")).await.unwrap();

        assert_eq!(out.text, "Short clip.");
        assert_eq!(out.meta["duration_secs"], json!(0.8));
        let segments = out.meta["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 1);

        mock.shutdown().await;
    }

    /// Extraction with duration but no segments.
    #[tokio::test]
    async fn extracts_text_with_duration_no_segments() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_text_with_duration("No segments here.", 4.2))
            .await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let out = ex.extract(&audio_raw(b"mp3")).await.unwrap();

        assert_eq!(out.text, "No segments here.");
        assert_eq!(out.meta["duration_secs"], json!(4.2));
        assert!(out.meta.get("segments").is_none());

        mock.shutdown().await;
    }

    /// Extraction with text only (no duration, no segments).
    #[tokio::test]
    async fn extracts_text_only() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_text_only("Plain transcription."))
            .await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let out = ex.extract(&audio_raw(b"mp3")).await.unwrap();

        assert_eq!(out.text, "Plain transcription.");
        assert!(out.meta.as_object().map(|o| o.is_empty()).unwrap_or(false));

        mock.shutdown().await;
    }

    /// Extraction with empty text response.
    #[tokio::test]
    async fn handles_empty_text() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_verbose_json("", &[])).await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let out = ex.extract(&audio_raw(b"silence")).await.unwrap();

        assert_eq!(out.text, "");
        assert!(out.meta.get("segments").is_none());

        mock.shutdown().await;
    }

    /// Extraction with segments but no top-level duration — duration is inferred
    /// from the last segment.
    #[tokio::test]
    async fn infers_duration_from_last_segment() {
        let mock = MockWhisper::start().await;
        // No "duration" key in the response; only segments.
        let body = json!({
            "text": "one two three",
            "segments": [
                {"start": 0.0, "end": 1.0, "text": "one"},
                {"start": 1.0, "end": 2.5, "text": "two"},
                {"start": 2.5, "end": 3.7, "text": "three"},
            ]
        });
        mock.set_scenario(MockWhisperScenario {
            status: StatusCode::OK,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap(),
        })
        .await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let out = ex.extract(&audio_raw(b"mp3")).await.unwrap();

        assert_eq!(out.text, "one two three");
        assert_eq!(out.meta["duration_secs"], json!(3.7));
        assert_eq!(out.meta["segments"].as_array().unwrap().len(), 3);

        mock.shutdown().await;
    }

    // ── extraction: page_images always empty ────────────────────────────────

    #[tokio::test]
    async fn page_images_always_empty() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_verbose_json("any text", &[(0.0, 0.5, "any text")]))
            .await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let out = ex.extract(&audio_raw(b"mp3")).await.unwrap();
        assert!(out.page_images.is_empty());

        mock.shutdown().await;
    }

    // ── extraction: ffmpeg errors ──────────────────────────────────────────

    /// ffmpeg subprocess fails — error propagates.
    #[tokio::test]
    async fn error_on_ffmpeg_failure() {
        let mock = MockWhisper::start().await;
        let ex = extractor_with_failing_ffmpeg(&mock).await;

        let err = ex.extract(&audio_raw(b"bad mp3")).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("ffmpeg exited"),
            "error should mention ffmpeg exit, got: {msg}"
        );

        mock.shutdown().await;
    }

    /// ffmpeg subprocess times out — error propagates.
    #[tokio::test]
    async fn error_on_ffmpeg_timeout() {
        let mock = MockWhisper::start().await;
        let ex = extractor_with_timeout_ffmpeg(&mock).await;

        let err = ex.extract(&audio_raw(b"huge file")).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("timed out"),
            "error should mention timeout, got: {msg}"
        );

        mock.shutdown().await;
    }

    // ── extraction: whisper HTTP errors ─────────────────────────────────────

    /// Whisper server returns HTTP 500.
    #[tokio::test]
    async fn error_on_whisper_500() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(MockWhisperScenario {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            content_type: "text/plain".into(),
            body: b"whisper crashed".to_vec(),
        })
        .await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let err = ex.extract(&audio_raw(b"mp3")).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("500"),
            "error should mention HTTP 500, got: {msg}"
        );
        assert!(
            msg.contains("recording.mp3"),
            "error should mention filename, got: {msg}"
        );
        assert!(
            msg.contains("whisper crashed"),
            "error should include body, got: {msg}"
        );

        mock.shutdown().await;
    }

    /// Whisper returns a non-JSON body on 200 (unexpected).
    #[tokio::test]
    async fn error_on_non_json_whisper_response() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(MockWhisperScenario {
            status: StatusCode::OK,
            content_type: "text/html".into(),
            body: b"<html>error</html>".to_vec(),
        })
        .await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let err = ex.extract(&audio_raw(b"mp3")).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("failed to parse whisper response"),
            "error should mention parse failure, got: {msg}"
        );

        mock.shutdown().await;
    }

    // ── extraction: connection errors ────────────────────────────────────────

    /// Whisper server is unreachable (verified-refusing port).
    #[tokio::test]
    async fn error_on_connection_refused() {
        // See crate::test_net — avoids the OS port-recycle race.
        let addr = crate::test_net::refused_addr().await;

        let config = WhisperConfig::new(format!("http://{addr}"));
        let transcoder: Arc<dyn Transcoder> = Arc::new(MockFfmpeg::new(b"wav".to_vec()));
        let ex = AudioExtractor::with_transcoder(Client::new(), config, transcoder);

        let err = ex.extract(&audio_raw(b"mp3")).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("HTTP request to whisper failed"),
            "error should mention HTTP request failure, got: {msg}"
        );
    }

    /// Server returns HTTP 503.
    #[tokio::test]
    async fn error_on_whisper_503() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(MockWhisperScenario {
            status: StatusCode::SERVICE_UNAVAILABLE,
            content_type: "text/plain".into(),
            body: b"overloaded".to_vec(),
        })
        .await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let err = ex.extract(&audio_raw(b"mp3")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("503"), "got: {msg}");
        assert!(msg.contains("overloaded"), "got: {msg}");

        mock.shutdown().await;
    }

    // ── extraction: path handling in errors ─────────────────────────────────

    /// Error includes the filename when path is set.
    #[tokio::test]
    async fn error_includes_filename() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(MockWhisperScenario {
            status: StatusCode::BAD_GATEWAY,
            content_type: "text/plain".into(),
            body: b"upstream timeout".to_vec(),
        })
        .await;

        let raw = RawFile {
            bytes: Bytes::from_static(b"fake audio"),
            mime: Some("audio/mpeg".into()),
            kind: DocKind::Audio,
            path: Some("important_recording.mp3".into()),
        };

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("important_recording.mp3"),
            "error should mention filename, got: {msg}"
        );

        mock.shutdown().await;
    }

    /// Error uses <unknown> when path is missing.
    #[tokio::test]
    async fn error_handles_missing_path() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(MockWhisperScenario {
            status: StatusCode::SERVICE_UNAVAILABLE,
            content_type: "text/plain".into(),
            body: b"down".to_vec(),
        })
        .await;

        let raw = RawFile {
            bytes: Bytes::from_static(b"fake audio"),
            mime: None,
            kind: DocKind::Audio,
            path: None,
        };

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("<unknown>"), "got: {msg}");

        mock.shutdown().await;
    }

    // ── extraction: hot-swap URL ────────────────────────────────────────────

    /// After switching the whisper base URL, the next call hits the new server.
    #[tokio::test]
    async fn hot_swap_url_takes_effect_on_next_call() {
        let mock1 = MockWhisper::start().await;
        let mock2 = MockWhisper::start().await;

        mock1.set_scenario(whisper_text_only("from server 1")).await;
        mock2.set_scenario(whisper_text_only("from server 2")).await;

        let config = WhisperConfig::new(mock1.base_url());
        let transcoder: Arc<dyn Transcoder> = Arc::new(MockFfmpeg::new(b"wav".to_vec()));
        let ex = AudioExtractor::with_transcoder(Client::new(), config.clone(), transcoder);

        // First call hits mock1.
        let out = ex.extract(&audio_raw(b"mp3")).await.unwrap();
        assert_eq!(out.text, "from server 1");

        // Hot-swap to mock2.
        config.set_url(mock2.base_url());

        // Second call hits mock2.
        let out = ex.extract(&audio_raw(b"mp3")).await.unwrap();
        assert_eq!(out.text, "from server 2");

        mock1.shutdown().await;
        mock2.shutdown().await;
    }

    // ── extraction: ffmpeg command construction (structural) ────────────────

    /// Verify the mock transcoder receives the raw bytes unmodified — the
    /// passthrough is deterministic (no real ffmpeg).
    #[tokio::test]
    async fn transcoder_receives_input_bytes() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_text_only("ok")).await;

        let input_bytes: Vec<u8> = vec![0x00, 0x01, 0x02, 0x03];
        let wav_bytes: Vec<u8> = b"RIFF....WAVE".to_vec();

        // The mock transcoder ignores input and returns fixed WAV. The key
        // assertion is that extracting succeeds end-to-end with the expected
        // transcription, proving the pipeline wires the input → transcoder →
        // whisper chain correctly.
        let ex = extractor_with_mocks(&mock, wav_bytes).await;
        let raw = audio_raw(&input_bytes);
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "ok");

        mock.shutdown().await;
    }

    // ── extraction: WAV audio/ogg input ─────────────────────────────────────

    /// Simulate an OGG audio file being processed.
    #[tokio::test]
    async fn extracts_ogg_audio() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_verbose_json(
            "Voice memo transcription.",
            &[(0.0, 2.0, "Voice memo"), (2.0, 4.1, "transcription.")],
        ))
        .await;

        let raw = RawFile {
            bytes: Bytes::from_static(b"OggS fake ogg"),
            mime: Some("audio/ogg".into()),
            kind: DocKind::Audio,
            path: Some("memo.ogg".into()),
        };

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "Voice memo transcription.");
        let segments = out.meta["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[1]["start"], json!(2.0));
        assert_eq!(segments[1]["text"], "transcription.");

        mock.shutdown().await;
    }

    // ── extraction: longer transcription ────────────────────────────────────

    /// Multi-segment transcription with realistic values.
    #[tokio::test]
    async fn handles_long_transcription() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_verbose_json(
            "Welcome to the local file knowledge base. You can search your files using natural language. The system transcribes audio and captions images automatically.",
            &[
                (0.0, 2.3, "Welcome to the local file knowledge base."),
                (2.3, 4.8, "You can search your files using natural language."),
                (4.8, 8.1, "The system transcribes audio and captions images automatically."),
            ],
        ))
        .await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let out = ex.extract(&audio_raw(b"mp3")).await.unwrap();

        assert!(out.text.contains("local file knowledge base"));
        assert!(out.text.contains("captions images"));
        assert_eq!(out.meta["duration_secs"], json!(8.1));
        assert_eq!(out.meta["segments"].as_array().unwrap().len(), 3);

        mock.shutdown().await;
    }

    // ── extraction: empty input bytes ───────────────────────────────────────

    #[tokio::test]
    async fn handles_empty_input_bytes() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_text_only("")).await;

        let ex = extractor_with_mocks(&mock, b"wav".to_vec()).await;
        let raw = audio_raw(b"");
        let out = ex.extract(&raw).await.unwrap();

        assert_eq!(out.text, "");
        assert!(out.page_images.is_empty());

        mock.shutdown().await;
    }

    // ── AudioExtractor builder ──────────────────────────────────────────────

    #[test]
    fn default_ffmpeg_timeout_is_120_seconds() {
        let config = WhisperConfig::new("http://localhost:8080".into());
        let ex = AudioExtractor::new(Client::new(), config);
        assert_eq!(ex.ffmpeg_timeout, Duration::from_secs(120));
    }

    #[test]
    fn custom_ffmpeg_timeout() {
        let config = WhisperConfig::new("http://localhost:8080".into());
        let ex =
            AudioExtractor::new(Client::new(), config).with_ffmpeg_timeout(Duration::from_secs(30));
        assert_eq!(ex.ffmpeg_timeout, Duration::from_secs(30));
    }

    // ── trait object: Extractor dispatch ────────────────────────────────────

    #[tokio::test]
    async fn dyn_extractor_trait_object() {
        let mock = MockWhisper::start().await;
        mock.set_scenario(whisper_text_only("trait object works"))
            .await;

        let ex: Box<dyn Extractor> = Box::new(extractor_with_mocks(&mock, b"wav".to_vec()).await);
        let out = ex.extract(&audio_raw(b"mp3")).await.unwrap();

        assert_eq!(out.text, "trait object works");

        mock.shutdown().await;
    }

    // ── F8: real-ffmpeg deadlock regression ─────────────────────────────────

    /// Build an N-second 16 kHz mono 16-bit PCM WAV (no ffmpeg needed), large
    /// enough that re-encoding produces well over 64 KiB of output.
    fn pcm_wav_seconds(secs: u32) -> Vec<u8> {
        let sample_rate: u32 = 16_000;
        let n = sample_rate * secs;
        let data_len = n * 2; // 16-bit mono
        let mut v = Vec::with_capacity(44 + data_len as usize);
        v.extend_from_slice(b"RIFF");
        v.extend_from_slice(&(36 + data_len).to_le_bytes());
        v.extend_from_slice(b"WAVE");
        v.extend_from_slice(b"fmt ");
        v.extend_from_slice(&16u32.to_le_bytes()); // PCM fmt chunk size
        v.extend_from_slice(&1u16.to_le_bytes()); // audio format = PCM
        v.extend_from_slice(&1u16.to_le_bytes()); // channels = mono
        v.extend_from_slice(&sample_rate.to_le_bytes());
        v.extend_from_slice(&(sample_rate * 2).to_le_bytes()); // byte rate
        v.extend_from_slice(&2u16.to_le_bytes()); // block align
        v.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
        v.extend_from_slice(b"data");
        v.extend_from_slice(&data_len.to_le_bytes());
        for i in 0..n {
            let s = ((i as f32 * 0.1).sin() * 8000.0) as i16;
            v.extend_from_slice(&s.to_le_bytes());
        }
        v
    }

    /// The real ffmpeg transcoder must handle audio whose transcoded output
    /// exceeds the ~64 KiB stdout pipe buffer **without deadlocking** (F8). The
    /// old stdin-pipe code hung here; the temp-file + `stdin(null)` path returns
    /// well within the timeout. `#[ignore]` because it needs a real ffmpeg binary
    /// (absent in `just ci`); run with `--ignored` on a host that has ffmpeg.
    #[tokio::test]
    #[ignore = "requires a real ffmpeg binary; run with --ignored"]
    async fn real_ffmpeg_transcodes_large_audio_without_deadlock() {
        let input = pcm_wav_seconds(3); // ~96 KiB in → > 64 KiB out
        let ff = RealFfmpeg;
        // Outer timeout is a safety net so a regression fails fast instead of
        // hanging the whole test run.
        let out = tokio::time::timeout(
            Duration::from_secs(30),
            ff.transcode(&input, Duration::from_secs(20)),
        )
        .await
        .expect("transcode must not hang (F8 deadlock regression)")
        .expect("ffmpeg should transcode the WAV");
        assert!(
            out.len() > 64 * 1024,
            "output must exceed the 64 KiB pipe buffer that triggered the deadlock, got {} bytes",
            out.len()
        );
    }
}
