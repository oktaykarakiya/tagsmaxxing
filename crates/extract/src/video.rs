//! Video extractor: ffmpeg scene-change keyframe extraction (capped at ~40 frames,
//! `select='gt(scene,0.3)'`), audio track extraction + whisper transcription, and
//! ffprobe metadata harvest (duration/resolution/codec/fps).
//!
//! Produces [`Extracted`] with the transcript as `text`, ffprobe metadata as `meta`,
//! and keyframe [`PageImage`]s for later VLM captioning. Transcript segments carry
//! `ts_offset` for deep-link retrieval (plan §7.1).
//!
//! The whisper base URL is hot-swappable via [`crate::audio::WhisperConfig`].
//! The ffmpeg/ffprobe subprocess boundary is abstracted behind an internal
//! [`MediaTool`] trait so unit tests can inject mocks (plan §31.3).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use kb_core::extractor::{Extracted, Extractor, PageImage, RawFile};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;

use crate::audio::WhisperConfig;

// ── ffprobe JSON parsing ─────────────────────────────────────────────────────

/// Parse the raw ffprobe JSON output into a structured, namespaced meta object.
///
/// Extracts `duration_secs`, `width`, `height`, `video_codec`, `audio_codec`,
/// `fps`, and `format_name` from the ffprobe JSON. Fields not present in the
/// input are omitted from the output.
///
/// # Example
///
/// ```rust
/// use kb_extract::video::parse_ffprobe_json;
/// let raw = serde_json::json!({
///     "format": {"duration": "120.5", "format_name": "mov,mp4"},
///     "streams": [
///         {"codec_type": "video", "codec_name": "h264", "width": 1920,
///          "height": 1080, "r_frame_rate": "30000/1001"},
///         {"codec_type": "audio", "codec_name": "aac"}
///     ]
/// });
/// let meta = parse_ffprobe_json(&raw);
/// assert_eq!(meta["duration_secs"], serde_json::json!(120.5));
/// assert_eq!(meta["width"], serde_json::json!(1920));
/// assert_eq!(meta["height"], serde_json::json!(1080));
/// assert_eq!(meta["video_codec"], "h264");
/// assert_eq!(meta["audio_codec"], "aac");
/// ```
pub fn parse_ffprobe_json(raw: &Value) -> Value {
    let mut meta = serde_json::Map::new();

    // ── format fields ──────────────────────────────────────────────────────
    if let Some(format) = raw.get("format") {
        if let Some(dur_str) = format.get("duration").and_then(|v| v.as_str())
            && let Ok(dur) = dur_str.parse::<f64>()
            && let Some(n) = serde_json::Number::from_f64(dur)
        {
            meta.insert("duration_secs".into(), Value::Number(n));
        }
        if let Some(fname) = format.get("format_name").and_then(|v| v.as_str()) {
            meta.insert("format_name".into(), Value::String(fname.to_string()));
        }
    }

    // ── stream fields ─────────────────────────────────────────────────────
    let streams = raw.get("streams").and_then(|v| v.as_array());
    let empty = Vec::new();
    let streams = streams.unwrap_or(&empty);

    for stream in streams {
        let codec_type = stream
            .get("codec_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        match codec_type {
            "video" => {
                if let Some(codec) = stream.get("codec_name").and_then(|v| v.as_str()) {
                    meta.insert("video_codec".into(), Value::String(codec.to_string()));
                }
                if let Some(w) = stream.get("width").and_then(|v| v.as_i64()) {
                    meta.insert("width".into(), Value::Number(serde_json::Number::from(w)));
                }
                if let Some(h) = stream.get("height").and_then(|v| v.as_i64()) {
                    meta.insert("height".into(), Value::Number(serde_json::Number::from(h)));
                }
                if let Some(rate_str) = stream.get("r_frame_rate").and_then(|v| v.as_str())
                    && let Some(fps) = parse_r_frame_rate(rate_str)
                    && let Some(n) = serde_json::Number::from_f64(fps)
                {
                    meta.insert("fps".into(), Value::Number(n));
                }
            }
            "audio" => {
                if let Some(codec) = stream.get("codec_name").and_then(|v| v.as_str()) {
                    meta.insert("audio_codec".into(), Value::String(codec.to_string()));
                }
            }
            _ => {}
        }
    }

    Value::Object(meta)
}

/// Parse an ffprobe `r_frame_rate` string (e.g. `"30000/1001"`, `"30/1"`) into
/// a floating-point FPS value.
///
/// Returns `None` if the string cannot be parsed as a rational fraction.
fn parse_r_frame_rate(rate: &str) -> Option<f64> {
    let (num, den) = rate.split_once('/')?;
    let numerator: f64 = num.parse().ok()?;
    let denominator: f64 = den.parse().ok()?;
    if denominator == 0.0 {
        return None;
    }
    Some(numerator / denominator)
}

// ── MediaTool trait ──────────────────────────────────────────────────────────

/// Internal abstraction over ffmpeg/ffprobe subprocess calls.
///
/// The production implementation uses `tokio::process::Command` with temp files.
/// Tests inject a mock that returns canned data, keeping the test suite
/// deterministic and free of host-ffmpeg dependencies (plan §31.3).
#[async_trait]
trait MediaTool: Send + Sync {
    /// Run ffprobe against `input` bytes, returning the raw JSON metadata.
    ///
    /// The returned value is the full ffprobe JSON tree; callers use
    /// [`parse_ffprobe_json`] to extract the structured fields.
    async fn probe(&self, input: &[u8], timeout: Duration) -> anyhow::Result<Value>;

    /// Extract the audio track from `input` video bytes as 16 kHz mono WAV.
    async fn extract_audio(&self, input: &[u8], timeout: Duration) -> anyhow::Result<Vec<u8>>;

    /// Extract scene-change keyframes from `input` video bytes.
    ///
    /// At most `cap` frames are returned, each a JPEG image.
    async fn extract_keyframes(
        &self,
        input: &[u8],
        cap: usize,
        timeout: Duration,
    ) -> anyhow::Result<Vec<Vec<u8>>>;
}

// ── RealMediaTool (production) ───────────────────────────────────────────────

/// The production [`MediaTool`] that spawns real `ffprobe` and `ffmpeg` subprocesses.
///
/// Input bytes are written to a temporary file so the tools can seek (necessary
/// for video containers). The temp file is cleaned up on drop.
#[derive(Debug, Clone, Copy, Default)]
struct RealMediaTool;

impl RealMediaTool {
    /// Write bytes to a temp file and return the handle (which deletes on drop).
    fn temp_file(input: &[u8]) -> anyhow::Result<tempfile::NamedTempFile> {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new()
            .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to create temp file: {e}"))?;
        tmp.write_all(input)
            .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to write temp file: {e}"))?;
        tmp.flush()
            .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to flush temp file: {e}"))?;
        Ok(tmp)
    }

    /// Read a file at `path` into a byte vector.
    fn read_file(path: &Path) -> anyhow::Result<Vec<u8>> {
        std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to read output file: {e}"))
    }
}

#[async_trait]
impl MediaTool for RealMediaTool {
    async fn probe(&self, input: &[u8], timeout: Duration) -> anyhow::Result<Value> {
        let tmp = Self::temp_file(input)?;

        let output = tokio::time::timeout(
            timeout,
            tokio::process::Command::new("ffprobe")
                .args([
                    "-v",
                    "quiet",
                    "-print_format",
                    "json",
                    "-show_format",
                    "-show_streams",
                ])
                .arg(tmp.path())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .output(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("VideoExtractor: ffprobe timed out after {timeout:?}"))?
        .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to spawn ffprobe: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "VideoExtractor: ffprobe exited with status {}: {}",
                output.status,
                stderr.trim()
            ));
        }

        let json: Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to parse ffprobe JSON: {e}"))?;
        Ok(json)
    }

    async fn extract_audio(&self, input: &[u8], timeout: Duration) -> anyhow::Result<Vec<u8>> {
        let tmp = Self::temp_file(input)?;

        let child = tokio::process::Command::new("ffmpeg")
            .args(["-i"])
            .arg(tmp.path())
            .args(["-vn", "-ar", "16000", "-ac", "1", "-f", "wav", "pipe:1"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to spawn ffmpeg (audio): {e}"))?;

        let output = tokio::time::timeout(timeout, child.wait_with_output())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "VideoExtractor: ffmpeg audio extraction timed out after {timeout:?}"
                )
            })?
            .map_err(|e| anyhow::anyhow!("VideoExtractor: ffmpeg audio process error: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "VideoExtractor: ffmpeg audio extraction exited with status {}: {}",
                output.status,
                stderr.trim()
            ));
        }

        Ok(output.stdout)
    }

    async fn extract_keyframes(
        &self,
        input: &[u8],
        cap: usize,
        timeout: Duration,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let tmp = Self::temp_file(input)?;
        let dir = tempfile::tempdir()
            .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to create temp dir: {e}"))?;
        let pattern = dir.path().join("frame_%04d.jpg");

        let output = tokio::time::timeout(
            timeout,
            tokio::process::Command::new("ffmpeg")
                .args(["-i"])
                .arg(tmp.path())
                .args(["-vf", "select='gt(scene,0.3)'", "-vsync", "vfr", "-vframes"])
                .arg(cap.to_string())
                .args(["-f", "image2"])
                .arg(&pattern)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .output(),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "VideoExtractor: ffmpeg keyframe extraction timed out after {timeout:?}"
            )
        })?
        .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to spawn ffmpeg (keyframes): {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "VideoExtractor: ffmpeg keyframe extraction exited with status {}: {}",
                output.status,
                stderr.trim()
            ));
        }

        let mut frames: Vec<(String, Vec<u8>)> = Vec::new();
        for entry in std::fs::read_dir(dir.path())
            .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to read keyframe dir: {e}"))?
        {
            let entry = entry
                .map_err(|e| anyhow::anyhow!("VideoExtractor: failed to read dir entry: {e}"))?;
            let path = entry.path();
            if path.is_file() {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("")
                    .to_string();
                let data = Self::read_file(&path)?;
                frames.push((name, data));
            }
        }

        // Sort by filename for deterministic ordering.
        frames.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(frames.into_iter().map(|(_, data)| data).collect())
    }
}

// ── Whisper API response types ───────────────────────────────────────────────

/// Parsed JSON response from whisper-server `/v1/audio/transcriptions`
/// (OpenAI-compatible `verbose_json` format).
#[derive(Debug, Deserialize)]
struct WhisperResponse {
    /// Full transcription text.
    text: String,
    /// Timestamped segments.
    #[serde(default)]
    segments: Vec<WhisperSegment>,
    /// Total audio duration in seconds.
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

// ── VideoExtractor ───────────────────────────────────────────────────────────

/// Extracts text, metadata, and keyframe images from video files.
///
/// # Pipeline (plan §7.1)
///
/// 1. **ffprobe metadata** — runs `ffprobe` to extract duration, resolution,
///    codec info, and frame rate into structured meta.
/// 2. **Duration check** — if `max_duration_secs` is set, videos exceeding it
///    are rejected early for cost control.
/// 3. **Audio track** — extracts the audio track (16 kHz mono WAV) via ffmpeg
///    and transcribes it with whisper-server. Segments are recorded with
///    `ts_offset` for deep-link retrieval.
/// 4. **Keyframe extraction** — extracts scene-change keyframes (capped at
///    `max_keyframes`, default 40) as JPEG [`PageImage`]s for later VLM
///    captioning.
///
/// The whisper base URL is hot-swappable via [`WhisperConfig`]
/// (CLAUDE.md hot-swappable rule).
///
/// # Examples
///
/// ```rust,no_run
/// use std::time::Duration;
/// use bytes::Bytes;
/// use kb_core::extractor::{Extracted, Extractor, RawFile};
/// use kb_core::kind::DocKind;
/// use kb_extract::audio::WhisperConfig;
/// use kb_extract::video::VideoExtractor;
///
/// # async fn example() -> anyhow::Result<()> {
/// let whisper = WhisperConfig::new("http://localhost:8080".into());
/// let client = reqwest::Client::new();
/// let ex = VideoExtractor::new(client, whisper);
/// let raw = RawFile {
///     bytes: Bytes::from_static(b"fake video data"),
///     mime: Some("video/mp4".into()),
///     kind: DocKind::Video,
///     path: Some("clip.mp4".into()),
/// };
/// let out = ex.extract(&raw).await?;
/// println!("transcript: {}", out.text);
/// println!("meta: {:?}", out.meta);
/// println!("keyframes: {}", out.page_images.len());
/// # Ok(())
/// # }
/// ```
pub struct VideoExtractor {
    /// Shared HTTP client.
    http: Client,
    /// Hot-swappable whisper base URL.
    whisper_config: WhisperConfig,
    /// Maximum wall-clock time for the ffprobe subprocess.
    probe_timeout: Duration,
    /// Maximum wall-clock time for ffmpeg subprocesses (audio + keyframes).
    ffmpeg_timeout: Duration,
    /// Maximum number of keyframes to extract (default 40, plan §7.1).
    max_keyframes: usize,
    /// Reject videos longer than this many seconds (cost control).
    max_duration_secs: Option<f64>,
    /// The media subprocess strategy (real ffmpeg in production; mock in tests).
    media_tool: Arc<dyn MediaTool>,
}

impl std::fmt::Debug for VideoExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VideoExtractor")
            .field("whisper_config", &self.whisper_config)
            .field("probe_timeout", &self.probe_timeout)
            .field("ffmpeg_timeout", &self.ffmpeg_timeout)
            .field("max_keyframes", &self.max_keyframes)
            .field("max_duration_secs", &self.max_duration_secs)
            .field("media_tool", &"<dyn MediaTool>")
            .finish()
    }
}

impl Clone for VideoExtractor {
    fn clone(&self) -> Self {
        Self {
            http: self.http.clone(),
            whisper_config: self.whisper_config.clone(),
            probe_timeout: self.probe_timeout,
            ffmpeg_timeout: self.ffmpeg_timeout,
            max_keyframes: self.max_keyframes,
            max_duration_secs: self.max_duration_secs,
            media_tool: Arc::clone(&self.media_tool),
        }
    }
}

impl VideoExtractor {
    /// Create a new extractor with a configured HTTP client, whisper
    /// configuration, and the default (real-ffmpeg/ffprobe) media tool.
    ///
    /// Defaults: probe timeout = 30 s, ffmpeg timeout = 300 s,
    /// max keyframes = 40, no duration limit.
    pub fn new(http: Client, whisper_config: WhisperConfig) -> Self {
        Self {
            http,
            whisper_config,
            probe_timeout: Duration::from_secs(30),
            ffmpeg_timeout: Duration::from_secs(300),
            max_keyframes: 40,
            max_duration_secs: None,
            media_tool: Arc::new(RealMediaTool),
        }
    }

    /// Set a custom timeout for the ffprobe subprocess. Default is 30 seconds.
    pub fn with_probe_timeout(mut self, timeout: Duration) -> Self {
        self.probe_timeout = timeout;
        self
    }

    /// Set a custom timeout for ffmpeg subprocesses. Default is 300 seconds.
    pub fn with_ffmpeg_timeout(mut self, timeout: Duration) -> Self {
        self.ffmpeg_timeout = timeout;
        self
    }

    /// Set the maximum number of keyframes to extract. Default is 40 (plan §7.1).
    pub fn with_max_keyframes(mut self, cap: usize) -> Self {
        self.max_keyframes = cap;
        self
    }

    /// Reject videos exceeding this duration. `None` (default) means no limit.
    pub fn with_max_duration(mut self, max_secs: Option<f64>) -> Self {
        self.max_duration_secs = max_secs;
        self
    }

    /// Build an extractor with an injected [`MediaTool`] — for tests.
    #[cfg(test)]
    fn with_media_tool(
        http: Client,
        whisper_config: WhisperConfig,
        media_tool: Arc<dyn MediaTool>,
    ) -> Self {
        Self {
            http,
            whisper_config,
            probe_timeout: Duration::from_secs(30),
            ffmpeg_timeout: Duration::from_secs(300),
            max_keyframes: 40,
            max_duration_secs: None,
            media_tool,
        }
    }

    // ── orchestration ───────────────────────────────────────────────────────

    /// Execute the full video extraction pipeline.
    async fn extract_impl(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        // 1. ffprobe metadata
        let raw_probe = self
            .media_tool
            .probe(&file.bytes, self.probe_timeout)
            .await?;
        let meta = parse_ffprobe_json(&raw_probe);

        // 2. Duration check (cost control)
        if let Some(max_dur) = self.max_duration_secs
            && let Some(dur) = meta.get("duration_secs").and_then(|v| v.as_f64())
            && dur > max_dur
        {
            return Err(anyhow::anyhow!(
                "VideoExtractor: video duration {dur}s exceeds maximum {max_dur}s"
            ));
        }

        // 3. Audio extraction + whisper transcription
        let audio_wav = self
            .media_tool
            .extract_audio(&file.bytes, self.ffmpeg_timeout)
            .await?;
        let whisper = self
            .transcribe_audio(&audio_wav, file.path.as_deref())
            .await?;

        // 4. Keyframe extraction
        let frame_data = self
            .media_tool
            .extract_keyframes(&file.bytes, self.max_keyframes, self.ffmpeg_timeout)
            .await?;
        let page_images: Vec<PageImage> = frame_data
            .into_iter()
            .map(|data| PageImage {
                data: Bytes::from(data),
                mime: "image/jpeg".into(),
            })
            .collect();

        // 5. Assemble result
        let mut out_meta = if let Value::Object(map) = meta {
            map
        } else {
            serde_json::Map::new()
        };

        if let Some(dur) = whisper.duration
            && !out_meta.contains_key("duration_secs")
            && let Some(n) = serde_json::Number::from_f64(dur)
        {
            out_meta.insert("duration_secs".into(), Value::Number(n));
        }

        if !whisper.segments.is_empty() {
            if whisper.duration.is_none()
                && let Some(last) = whisper.segments.last()
                && !out_meta.contains_key("duration_secs")
                && let Some(n) = serde_json::Number::from_f64(last.end)
            {
                out_meta.insert("duration_secs".into(), Value::Number(n));
            }

            let segments: Vec<Value> = whisper
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
            out_meta.insert("segments".into(), Value::Array(segments));
        }

        Ok(Extracted {
            text: whisper.text,
            meta: Value::Object(out_meta),
            page_images,
        })
    }

    /// POST the WAV bytes to whisper-server and parse the response.
    async fn transcribe_audio(
        &self,
        wav: &[u8],
        file_path: Option<&str>,
    ) -> anyhow::Result<WhisperResponse> {
        let base = self.whisper_config.current_url();
        let url = format!("{base}/v1/audio/transcriptions");

        let wav_part = reqwest::multipart::Part::bytes(wav.to_vec())
            .file_name("audio.wav")
            .mime_str("audio/wav")
            .map_err(|e| anyhow::anyhow!("VideoExtractor: invalid MIME string: {e}"))?;

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
                    "VideoExtractor: HTTP request to whisper failed for '{}': {e}",
                    file_path.unwrap_or("<unknown>")
                )
            })?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "VideoExtractor: whisper returned HTTP {status} for '{}': {}",
                file_path.unwrap_or("<unknown>"),
                body
            ));
        }

        let whisper: WhisperResponse = resp.json().await.map_err(|e| {
            anyhow::anyhow!(
                "VideoExtractor: failed to parse whisper response for '{}': {e}",
                file_path.unwrap_or("<unknown>")
            )
        })?;

        Ok(whisper)
    }
}

#[async_trait]
impl Extractor for VideoExtractor {
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

    /// A lightweight mock whisper server for deterministic tests (same pattern
    /// as the audio module's mock).
    struct MockWhisper {
        addr: SocketAddr,
        scenario: Arc<Mutex<MockWhisperScenario>>,
        shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    }

    #[derive(Debug, Clone)]
    struct MockWhisperScenario {
        status: StatusCode,
        content_type: String,
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

    /// Build a `verbose_json` whisper response with segments.
    fn whisper_verbose_json(text: &str, segments: &[(f64, f64, &str)]) -> MockWhisperScenario {
        let duration = segments.last().map(|s| s.1).unwrap_or(0.0);
        let segs: Vec<Value> = segments
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

    /// Build a whisper response with text only.
    fn whisper_text_only(text: &str) -> MockWhisperScenario {
        let body = json!({"text": text});
        MockWhisperScenario {
            status: StatusCode::OK,
            content_type: "application/json".into(),
            body: serde_json::to_vec(&body).unwrap(),
        }
    }

    impl MockWhisper {
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

        fn base_url(&self) -> String {
            format!("http://{}", self.addr)
        }

        async fn set_scenario(&self, s: MockWhisperScenario) {
            *self.scenario.lock().await = s;
        }

        async fn shutdown(mut self) {
            if let Some(tx) = self.shutdown_tx.take() {
                let _ = tx.send(());
                tokio::task::yield_now().await;
            }
        }
    }

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

    // ── mock media tool ──────────────────────────────────────────────────────

    /// A mock [`MediaTool`] that returns pre-configured responses.
    struct MockMediaTool {
        probe_json: Value,
        audio_wav: Vec<u8>,
        keyframes: Vec<Vec<u8>>,
        probe_should_fail: bool,
        audio_should_fail: bool,
        keyframes_should_fail: bool,
        probe_should_timeout: bool,
        audio_should_timeout: bool,
    }

    impl MockMediaTool {
        fn new(probe_json: Value, audio_wav: Vec<u8>, keyframes: Vec<Vec<u8>>) -> Self {
            Self {
                probe_json,
                audio_wav,
                keyframes,
                probe_should_fail: false,
                audio_should_fail: false,
                keyframes_should_fail: false,
                probe_should_timeout: false,
                audio_should_timeout: false,
            }
        }

        fn with_failing_probe(mut self) -> Self {
            self.probe_should_fail = true;
            self
        }

        fn with_failing_audio(mut self) -> Self {
            self.audio_should_fail = true;
            self
        }

        fn with_failing_keyframes(mut self) -> Self {
            self.keyframes_should_fail = true;
            self
        }

        fn with_timeout_probe(mut self) -> Self {
            self.probe_should_timeout = true;
            self
        }

        fn with_timeout_audio(mut self) -> Self {
            self.audio_should_timeout = true;
            self
        }
    }

    #[async_trait]
    impl MediaTool for MockMediaTool {
        async fn probe(&self, _input: &[u8], _timeout: Duration) -> anyhow::Result<Value> {
            if self.probe_should_timeout {
                anyhow::bail!("VideoExtractor: ffprobe timed out after 30s");
            }
            if self.probe_should_fail {
                anyhow::bail!(
                    "VideoExtractor: ffprobe exited with status exit status: 1: unsupported format"
                );
            }
            Ok(self.probe_json.clone())
        }

        async fn extract_audio(
            &self,
            _input: &[u8],
            _timeout: Duration,
        ) -> anyhow::Result<Vec<u8>> {
            if self.audio_should_timeout {
                anyhow::bail!("VideoExtractor: ffmpeg audio extraction timed out after 300s");
            }
            if self.audio_should_fail {
                anyhow::bail!(
                    "VideoExtractor: ffmpeg audio extraction exited with status exit status: 1: no audio stream"
                );
            }
            Ok(self.audio_wav.clone())
        }

        async fn extract_keyframes(
            &self,
            _input: &[u8],
            _cap: usize,
            _timeout: Duration,
        ) -> anyhow::Result<Vec<Vec<u8>>> {
            if self.keyframes_should_fail {
                anyhow::bail!(
                    "VideoExtractor: ffmpeg keyframe extraction exited with status exit status: 1: no video stream"
                );
            }
            Ok(self.keyframes.clone())
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Standard ffprobe JSON fixture: 1920x1080 h264 video, aac audio, 30 fps.
    fn standard_ffprobe_json() -> Value {
        json!({
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "width": 1920,
                    "height": 1080,
                    "r_frame_rate": "30/1"
                },
                {
                    "codec_type": "audio",
                    "codec_name": "aac"
                }
            ],
            "format": {
                "duration": "60.0",
                "format_name": "mov,mp4,m4a,3gp,3g2,mj2"
            }
        })
    }

    fn video_raw(bytes: &[u8]) -> RawFile {
        RawFile {
            bytes: Bytes::copy_from_slice(bytes),
            mime: Some("video/mp4".into()),
            kind: DocKind::Video,
            path: Some("clip.mp4".into()),
        }
    }

    async fn extractor_with_mocks(
        whisper: &MockWhisper,
        media_tool: MockMediaTool,
    ) -> VideoExtractor {
        let whisper_config = WhisperConfig::new(whisper.base_url());
        VideoExtractor::with_media_tool(Client::new(), whisper_config, Arc::new(media_tool))
    }

    // ── parse_ffprobe_json tests ────────────────────────────────────────────

    #[test]
    fn parses_full_ffprobe_output() {
        let raw = json!({
            "format": {"duration": "120.5", "format_name": "matroska,webm"},
            "streams": [
                {"codec_type": "video", "codec_name": "vp9", "width": 3840, "height": 2160, "r_frame_rate": "24000/1001"},
                {"codec_type": "audio", "codec_name": "opus"}
            ]
        });
        let meta = parse_ffprobe_json(&raw);
        assert_eq!(meta["duration_secs"], json!(120.5));
        assert_eq!(meta["format_name"], "matroska,webm");
        assert_eq!(meta["video_codec"], "vp9");
        assert_eq!(meta["width"], json!(3840));
        assert_eq!(meta["height"], json!(2160));
        assert_eq!(meta["fps"].as_f64().unwrap(), 24000.0 / 1001.0);
        assert_eq!(meta["audio_codec"], "opus");
    }

    #[test]
    fn parses_minimal_ffprobe_output() {
        let raw = json!({
            "format": {"duration": "5.0"}
        });
        let meta = parse_ffprobe_json(&raw);
        assert_eq!(meta["duration_secs"], json!(5.0));
        assert!(meta.get("video_codec").is_none());
        assert!(meta.get("width").is_none());
    }

    #[test]
    fn parses_ffprobe_with_no_format_section() {
        let raw = json!({
            "streams": [
                {"codec_type": "video", "codec_name": "mpeg4", "width": 640, "height": 480, "r_frame_rate": "25/1"}
            ]
        });
        let meta = parse_ffprobe_json(&raw);
        assert_eq!(meta["video_codec"], "mpeg4");
        assert_eq!(meta["width"], json!(640));
        assert!(meta.get("duration_secs").is_none());
        assert!(meta.get("audio_codec").is_none());
    }

    #[test]
    fn parses_ffprobe_with_empty_streams() {
        let raw = json!({
            "format": {"duration": "0.0", "format_name": "wav"},
            "streams": []
        });
        let meta = parse_ffprobe_json(&raw);
        assert_eq!(meta["duration_secs"], json!(0.0));
        assert_eq!(meta["format_name"], "wav");
    }

    #[test]
    fn parses_ffprobe_with_non_numeric_duration() {
        let raw = json!({
            "format": {"duration": "N/A"}
        });
        let meta = parse_ffprobe_json(&raw);
        assert!(meta.as_object().unwrap().is_empty());
    }

    #[test]
    fn parses_ffprobe_with_subtitle_stream() {
        let raw = json!({
            "streams": [
                {"codec_type": "video", "codec_name": "h264", "width": 1280, "height": 720, "r_frame_rate": "60/1"},
                {"codec_type": "audio", "codec_name": "aac"},
                {"codec_type": "subtitle", "codec_name": "srt"}
            ],
            "format": {"duration": "300.0"}
        });
        let meta = parse_ffprobe_json(&raw);
        assert_eq!(meta["video_codec"], "h264");
        assert_eq!(meta["audio_codec"], "aac");
        assert_eq!(meta["duration_secs"], json!(300.0));
        // Subtitle stream is ignored (not video or audio).
    }

    // ── parse_r_frame_rate tests ────────────────────────────────────────────

    #[test]
    fn parses_simple_frame_rate() {
        assert!((parse_r_frame_rate("30/1").unwrap() - 30.0).abs() < 0.001);
        assert!((parse_r_frame_rate("60/1").unwrap() - 60.0).abs() < 0.001);
    }

    #[test]
    fn parses_ntsc_frame_rate() {
        let fps = parse_r_frame_rate("30000/1001").unwrap();
        assert!((fps - 29.97003).abs() < 0.001);
    }

    #[test]
    fn parses_fractional_frame_rate() {
        let fps = parse_r_frame_rate("24000/1001").unwrap();
        assert!((fps - 23.976024).abs() < 0.001);
    }

    #[test]
    fn frame_rate_rejects_invalid() {
        assert!(parse_r_frame_rate("invalid").is_none());
        assert!(parse_r_frame_rate("30").is_none());
        assert!(parse_r_frame_rate("").is_none());
    }

    #[test]
    fn frame_rate_rejects_zero_denominator() {
        assert!(parse_r_frame_rate("30/0").is_none());
    }

    // ── VideoExtractor: happy path ──────────────────────────────────────────

    /// Full extraction with probe metadata, whisper transcript segments, and keyframes.
    #[tokio::test]
    async fn full_extraction_with_keyframes() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(whisper_verbose_json(
                "Hello and welcome to the video. Today we discuss important topics.",
                &[
                    (0.0, 2.0, "Hello and welcome to the video."),
                    (2.0, 5.0, "Today we discuss important topics."),
                ],
            ))
            .await;

        let keyframes = vec![
            b"\xff\xd8\xff\xe0\x00\x10JFIFfake1".to_vec(),
            b"\xff\xd8\xff\xe0\x00\x10JFIFfake2".to_vec(),
        ];

        let media_tool =
            MockMediaTool::new(standard_ffprobe_json(), b"fake wav".to_vec(), keyframes);
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let out = ex.extract(&video_raw(b"fake video data")).await.unwrap();

        // Verify text (transcript).
        assert!(out.text.contains("Hello and welcome"));
        assert!(out.text.contains("discuss important topics"));

        // Verify meta: ffprobe fields.
        assert_eq!(out.meta["duration_secs"], json!(60.0));
        assert_eq!(out.meta["video_codec"], "h264");
        assert_eq!(out.meta["width"], json!(1920));
        assert_eq!(out.meta["height"], json!(1080));
        assert_eq!(out.meta["audio_codec"], "aac");
        assert_eq!(out.meta["format_name"], "mov,mp4,m4a,3gp,3g2,mj2");

        // Verify meta: transcript segments.
        let segments = out.meta["segments"].as_array().unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0]["start"], json!(0.0));
        assert_eq!(segments[0]["end"], json!(2.0));
        assert_eq!(segments[1]["start"], json!(2.0));
        assert_eq!(segments[1]["end"], json!(5.0));

        // Verify keyframes.
        assert_eq!(out.page_images.len(), 2);
        assert_eq!(out.page_images[0].mime, "image/jpeg");
        assert!(out.page_images[0].data.starts_with(b"\xff\xd8\xff"));

        mock_whisper.shutdown().await;
    }

    /// Extraction with no audio stream: transcript empty, but probe + keyframes succeed.
    #[tokio::test]
    async fn extraction_without_transcript() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper.set_scenario(whisper_text_only("")).await;

        let probe = json!({
            "streams": [
                {"codec_type": "video", "codec_name": "h264", "width": 640, "height": 360, "r_frame_rate": "24/1"}
            ],
            "format": {"duration": "10.0"}
        });
        let keyframes = vec![b"\xff\xd8\xffkey".to_vec()];
        let media_tool = MockMediaTool::new(probe, b"wav".to_vec(), keyframes);
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let out = ex.extract(&video_raw(b"silent video")).await.unwrap();

        assert_eq!(out.text, "");
        assert_eq!(out.meta["duration_secs"], json!(10.0));
        assert_eq!(out.page_images.len(), 1);

        mock_whisper.shutdown().await;
    }

    /// Extraction with no keyframes (e.g., very short video below scene threshold).
    #[tokio::test]
    async fn extraction_with_zero_keyframes() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(whisper_verbose_json(
                "Short clip.",
                &[(0.0, 0.5, "Short clip.")],
            ))
            .await;

        let media_tool = MockMediaTool::new(
            standard_ffprobe_json(),
            b"wav".to_vec(),
            Vec::new(), // no keyframes
        );
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let out = ex.extract(&video_raw(b"short")).await.unwrap();

        assert_eq!(out.text, "Short clip.");
        assert!(out.page_images.is_empty());

        mock_whisper.shutdown().await;
    }

    // ── VideoExtractor: duration check (cost control) ───────────────────────

    #[tokio::test]
    async fn rejects_video_exceeding_max_duration() {
        let mock_whisper = MockWhisper::start().await;
        // whisper not called, but server must be alive for the extractor.
        mock_whisper.set_scenario(whisper_text_only("")).await;

        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![]);
        let ex = VideoExtractor::with_media_tool(
            Client::new(),
            WhisperConfig::new(mock_whisper.base_url()),
            Arc::new(media_tool),
        )
        .with_max_duration(Some(30.0)); // cap at 30s, video reports 60s

        let err = ex.extract(&video_raw(b"too long")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("exceeds maximum"), "got: {msg}");
        assert!(msg.contains("60"), "got: {msg}");
        assert!(msg.contains("30"), "got: {msg}");

        mock_whisper.shutdown().await;
    }

    #[tokio::test]
    async fn accepts_video_within_max_duration() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(whisper_text_only("within bounds"))
            .await;

        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![]);
        let ex = VideoExtractor::with_media_tool(
            Client::new(),
            WhisperConfig::new(mock_whisper.base_url()),
            Arc::new(media_tool),
        )
        .with_max_duration(Some(120.0)); // cap at 120s, video reports 60s

        let out = ex.extract(&video_raw(b"ok")).await.unwrap();
        assert_eq!(out.text, "within bounds");

        mock_whisper.shutdown().await;
    }

    #[tokio::test]
    async fn no_max_duration_accepts_any_length() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(whisper_text_only("very long"))
            .await;

        // 10-hour video, no max duration → accepted.
        let probe = json!({
            "format": {"duration": "36000.0"},
            "streams": [
                {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080, "r_frame_rate": "30/1"}
            ]
        });
        let media_tool = MockMediaTool::new(probe, b"wav".to_vec(), vec![]);
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let out = ex.extract(&video_raw(b"marathon")).await.unwrap();
        assert_eq!(out.text, "very long");

        mock_whisper.shutdown().await;
    }

    // ── VideoExtractor: ffprobe errors ──────────────────────────────────────

    #[tokio::test]
    async fn error_on_ffprobe_failure() {
        let mock_whisper = MockWhisper::start().await;
        let media_tool =
            MockMediaTool::new(Value::Null, b"wav".to_vec(), vec![]).with_failing_probe();
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let err = ex.extract(&video_raw(b"corrupt")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ffprobe exited"), "got: {msg}");
        assert!(msg.contains("unsupported format"), "got: {msg}");

        mock_whisper.shutdown().await;
    }

    #[tokio::test]
    async fn error_on_ffprobe_timeout() {
        let mock_whisper = MockWhisper::start().await;
        let media_tool =
            MockMediaTool::new(Value::Null, b"wav".to_vec(), vec![]).with_timeout_probe();
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let err = ex.extract(&video_raw(b"huge")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("timed out"), "got: {msg}");

        mock_whisper.shutdown().await;
    }

    // ── VideoExtractor: audio extraction errors ─────────────────────────────

    #[tokio::test]
    async fn error_on_audio_extraction_failure() {
        let mock_whisper = MockWhisper::start().await;
        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![])
            .with_failing_audio();
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let err = ex.extract(&video_raw(b"no audio")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("audio extraction"), "got: {msg}");
        assert!(msg.contains("no audio stream"), "got: {msg}");

        mock_whisper.shutdown().await;
    }

    #[tokio::test]
    async fn error_on_audio_extraction_timeout() {
        let mock_whisper = MockWhisper::start().await;
        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![])
            .with_timeout_audio();
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let err = ex.extract(&video_raw(b"long")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("timed out"), "got: {msg}");

        mock_whisper.shutdown().await;
    }

    // ── VideoExtractor: keyframe errors ─────────────────────────────────────

    #[tokio::test]
    async fn error_on_keyframe_extraction_failure() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(whisper_text_only("transcript ok"))
            .await;

        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![])
            .with_failing_keyframes();
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let err = ex.extract(&video_raw(b"corrupt frames")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("keyframe extraction"), "got: {msg}");
        assert!(msg.contains("no video stream"), "got: {msg}");

        mock_whisper.shutdown().await;
    }

    // ── VideoExtractor: whisper HTTP errors ─────────────────────────────────

    #[tokio::test]
    async fn error_on_whisper_500() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(MockWhisperScenario {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                content_type: "text/plain".into(),
                body: b"whisper overloaded".to_vec(),
            })
            .await;

        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![]);
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let err = ex.extract(&video_raw(b"video")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("500"), "got: {msg}");
        assert!(msg.contains("clip.mp4"), "got: {msg}");
        assert!(msg.contains("whisper overloaded"), "got: {msg}");

        mock_whisper.shutdown().await;
    }

    #[tokio::test]
    async fn error_on_non_json_whisper_response() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(MockWhisperScenario {
                status: StatusCode::OK,
                content_type: "text/html".into(),
                body: b"<html>not json</html>".to_vec(),
            })
            .await;

        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![]);
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let err = ex.extract(&video_raw(b"video")).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("failed to parse whisper response"),
            "got: {msg}"
        );

        mock_whisper.shutdown().await;
    }

    // ── VideoExtractor: connection refused ──────────────────────────────────

    #[tokio::test]
    async fn error_on_connection_refused() {
        let mock_whisper = MockWhisper::start().await;
        let addr = mock_whisper.addr;
        mock_whisper.shutdown().await;

        let whisper_config = WhisperConfig::new(format!("http://{addr}"));
        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![]);
        let ex =
            VideoExtractor::with_media_tool(Client::new(), whisper_config, Arc::new(media_tool));

        let err = ex.extract(&video_raw(b"video")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("HTTP request to whisper failed"), "got: {msg}");
    }

    // ── VideoExtractor: hot-swap whisper URL ────────────────────────────────

    #[tokio::test]
    async fn hot_swap_whisper_url_takes_effect() {
        let mock1 = MockWhisper::start().await;
        let mock2 = MockWhisper::start().await;

        mock1.set_scenario(whisper_text_only("server 1")).await;
        mock2.set_scenario(whisper_text_only("server 2")).await;

        let whisper_config = WhisperConfig::new(mock1.base_url());
        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![]);
        let ex = VideoExtractor::with_media_tool(
            Client::new(),
            whisper_config.clone(),
            Arc::new(media_tool),
        );

        // First call hits mock1.
        let out = ex.extract(&video_raw(b"v")).await.unwrap();
        assert_eq!(out.text, "server 1");

        // Hot-swap to mock2.
        whisper_config.set_url(mock2.base_url());

        // Second call hits mock2.
        let out = ex.extract(&video_raw(b"v")).await.unwrap();
        assert_eq!(out.text, "server 2");

        mock1.shutdown().await;
        mock2.shutdown().await;
    }

    // ── VideoExtractor: path handling in errors ─────────────────────────────

    #[tokio::test]
    async fn error_includes_filename() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(MockWhisperScenario {
                status: StatusCode::BAD_GATEWAY,
                content_type: "text/plain".into(),
                body: b"upstream timeout".to_vec(),
            })
            .await;

        let raw = RawFile {
            bytes: Bytes::from_static(b"video data"),
            mime: Some("video/mp4".into()),
            kind: DocKind::Video,
            path: Some("important_clip.mp4".into()),
        };

        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![]);
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("important_clip.mp4"), "got: {msg}");

        mock_whisper.shutdown().await;
    }

    #[tokio::test]
    async fn error_handles_missing_path() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(MockWhisperScenario {
                status: StatusCode::SERVICE_UNAVAILABLE,
                content_type: "text/plain".into(),
                body: b"down".to_vec(),
            })
            .await;

        let raw = RawFile {
            bytes: Bytes::from_static(b"video data"),
            mime: None,
            kind: DocKind::Video,
            path: None,
        };

        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![]);
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;
        let err = ex.extract(&raw).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("<unknown>"), "got: {msg}");

        mock_whisper.shutdown().await;
    }

    // ── VideoExtractor: builder ─────────────────────────────────────────────

    #[test]
    fn default_max_keyframes_is_40() {
        let whisper = WhisperConfig::new("http://localhost:8080".into());
        let ex = VideoExtractor::new(Client::new(), whisper);
        assert_eq!(ex.max_keyframes, 40);
    }

    #[test]
    fn default_probe_timeout_is_30_seconds() {
        let whisper = WhisperConfig::new("http://localhost:8080".into());
        let ex = VideoExtractor::new(Client::new(), whisper);
        assert_eq!(ex.probe_timeout, Duration::from_secs(30));
    }

    #[test]
    fn default_ffmpeg_timeout_is_300_seconds() {
        let whisper = WhisperConfig::new("http://localhost:8080".into());
        let ex = VideoExtractor::new(Client::new(), whisper);
        assert_eq!(ex.ffmpeg_timeout, Duration::from_secs(300));
    }

    #[test]
    fn custom_max_keyframes() {
        let whisper = WhisperConfig::new("http://localhost:8080".into());
        let ex = VideoExtractor::new(Client::new(), whisper).with_max_keyframes(20);
        assert_eq!(ex.max_keyframes, 20);
    }

    #[test]
    fn custom_probe_timeout() {
        let whisper = WhisperConfig::new("http://localhost:8080".into());
        let ex =
            VideoExtractor::new(Client::new(), whisper).with_probe_timeout(Duration::from_secs(60));
        assert_eq!(ex.probe_timeout, Duration::from_secs(60));
    }

    #[test]
    fn custom_ffmpeg_timeout() {
        let whisper = WhisperConfig::new("http://localhost:8080".into());
        let ex = VideoExtractor::new(Client::new(), whisper)
            .with_ffmpeg_timeout(Duration::from_secs(600));
        assert_eq!(ex.ffmpeg_timeout, Duration::from_secs(600));
    }

    // ── Extractor trait object dispatch ─────────────────────────────────────

    #[tokio::test]
    async fn dyn_extractor_trait_object() {
        let mock_whisper = MockWhisper::start().await;
        mock_whisper
            .set_scenario(whisper_text_only("trait object"))
            .await;

        let media_tool = MockMediaTool::new(standard_ffprobe_json(), b"wav".to_vec(), vec![]);
        let whisper_config = WhisperConfig::new(mock_whisper.base_url());
        let ex: Box<dyn Extractor> = Box::new(VideoExtractor::with_media_tool(
            Client::new(),
            whisper_config,
            Arc::new(media_tool),
        ));
        let out = ex.extract(&video_raw(b"v")).await.unwrap();
        assert_eq!(out.text, "trait object");

        mock_whisper.shutdown().await;
    }

    // ── Infer duration from last segment when ffprobe has no duration ───────

    #[tokio::test]
    async fn infers_duration_from_last_segment() {
        let mock_whisper = MockWhisper::start().await;
        // Whisper response with segments but no top-level duration.
        let body = json!({
            "text": "one two three",
            "segments": [
                {"start": 0.0, "end": 1.2, "text": "one"},
                {"start": 1.2, "end": 2.8, "text": "two"},
                {"start": 2.8, "end": 4.1, "text": "three"},
            ]
        });
        mock_whisper
            .set_scenario(MockWhisperScenario {
                status: StatusCode::OK,
                content_type: "application/json".into(),
                body: serde_json::to_vec(&body).unwrap(),
            })
            .await;

        // ffprobe returns no duration field.
        let probe = json!({
            "streams": [
                {"codec_type": "video", "codec_name": "h264", "width": 1280, "height": 720, "r_frame_rate": "30/1"}
            ]
        });
        let media_tool = MockMediaTool::new(probe, b"wav".to_vec(), vec![]);
        let ex = extractor_with_mocks(&mock_whisper, media_tool).await;

        let out = ex.extract(&video_raw(b"video")).await.unwrap();
        assert_eq!(out.text, "one two three");
        assert_eq!(out.meta["duration_secs"], json!(4.1));
        assert_eq!(out.meta["segments"].as_array().unwrap().len(), 3);

        mock_whisper.shutdown().await;
    }
}
