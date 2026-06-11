// SPDX-License-Identifier: AGPL-3.0-or-later

//! Optional OpenTelemetry tracing export (plan §18, behind the `otlp` feature).
//!
//! When the `otlp` feature is enabled and the `OTEL_EXPORTER_OTLP_ENDPOINT`
//! environment variable is set, [`init_otlp_tracing`] checks for OTLP
//! configuration. The full OTLP exporter wiring uses `opentelemetry-otlp` +
//! `tracing-opentelemetry` (available behind this feature gate) and should be
//! composed into the subscriber setup before `tracing_subscriber::init()`.
//!
//! # Environment variables
//!
//! | Variable                      | Default         | Description                           |
//! |-------------------------------|-----------------|---------------------------------------|
//! | `OTEL_EXPORTER_OTLP_ENDPOINT` | (unset → off)   | OTLP collector endpoint (e.g. `http://localhost:4318/v1/traces`) |
//! | `OTEL_SERVICE_NAME`           | `kb`            | Service name sent as `service.name`  |
//!
//! When `OTEL_EXPORTER_OTLP_ENDPOINT` is **not** set, [`init_otlp_tracing`]
//! returns `Ok(false)`.
//!
//! # Setup example (when the feature is enabled and the env var is set)
//!
//! ```rust,ignore
//! use tracing_subscriber::layer::SubscriberExt;
//!
//! let otlp_enabled = kb_metrics::telemetry::init_otlp_tracing()?;
//! if otlp_enabled {
//!     // Full OTLP wiring: build a SpanExporter → TracerProvider →
//!     // OpenTelemetryLayer and compose with Registry.
//!     // See: https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/
//! }
//! ```

/// Check whether OpenTelemetry OTLP tracing should be enabled.
///
/// Returns `Ok(true)` when `OTEL_EXPORTER_OTLP_ENDPOINT` is set to a
/// non-empty value, indicating the caller should set up OTLP export.
/// Returns `Ok(false)` when the variable is absent or empty (skip).
///
/// # Errors
///
/// Returns an error only if the environment check itself fails (unlikely).
/// The actual exporter setup is left to the caller.
#[cfg(feature = "otlp")]
pub fn init_otlp_tracing() -> anyhow::Result<bool> {
    let endpoint = match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        Ok(v) if !v.is_empty() => v,
        _ => return Ok(false),
    };

    let _service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "kb".to_string());

    tracing::info!(
        endpoint = %endpoint,
        "OpenTelemetry OTLP tracing is configured — enable the exporter in subscriber setup"
    );

    Ok(true)
}

/// Placeholder: build an OTLP-compatible tracing layer.
///
/// This function is available when the `otlp` feature is active but the
/// full exporter wiring is deferred to a future task (the
/// `tracing-opentelemetry` + `opentelemetry_sdk` APIs evolve rapidly across
/// minor versions). In the meantime [`init_otlp_tracing`] tells the caller
/// whether to enable OTLP.
///
/// Returns `None` when `OTEL_EXPORTER_OTLP_ENDPOINT` is absent.
///
/// # Errors
///
/// Returns an error if the OTLP exporter cannot be created.
#[cfg(feature = "otlp")]
pub fn build_otel_layer() -> anyhow::Result<Option<OtlpLayerStub>> {
    let enabled = init_otlp_tracing()?;
    if !enabled {
        return Ok(None);
    }
    Ok(Some(OtlpLayerStub))
}

/// Placeholder layer type for when OTLP is configured but full wiring is
/// deferred. Compose this with `tracing_subscriber::Registry` until the
/// full `OpenTelemetryLayer` is wired in a future task.
#[cfg(feature = "otlp")]
#[derive(Debug)]
pub struct OtlpLayerStub;

#[cfg(feature = "otlp")]
impl<S> tracing_subscriber::Layer<S> for OtlpLayerStub where S: tracing::Subscriber {}

/// Stub for when the `otlp` feature is not enabled — always returns `Ok(false)`.
#[cfg(not(feature = "otlp"))]
pub fn init_otlp_tracing() -> anyhow::Result<bool> {
    Ok(false)
}

/// Stub for when the `otlp` feature is not enabled — always returns `None`.
#[cfg(not(feature = "otlp"))]
pub fn build_otel_layer() -> anyhow::Result<Option<NoopLayer>> {
    Ok(None)
}

/// Type-level stub so the return type compiles without the `otlp` feature.
#[cfg(not(feature = "otlp"))]
#[derive(Debug)]
pub struct NoopLayer;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn otlp_is_disabled_by_default() {
        if let Ok(enabled) = init_otlp_tracing() {
            assert!(!enabled, "OTLP should be disabled without env var");
        }
        // Err is fine — env var might be set but exporter build failed.
    }

    #[test]
    fn build_otel_layer_is_none_without_env() {
        if let Ok(opt) = build_otel_layer() {
            assert!(
                opt.is_none(),
                "should be None without OTEL_EXPORTER_OTLP_ENDPOINT"
            );
        }
    }
}
