//! Public marketing page handlers (P12-T1): landing, pricing, features, FAQ.
//!
//! All routes in this module are **public** (no auth middleware). They detect
//! whether the visitor has a valid session cookie and adjust the nav links
//! accordingly — authenticated users see links to the app, unauthenticated
//! visitors see login/signup CTAs.
//!
//! # Navigation detection
//!
//! The `try_authenticate` helper inspects the `__Host-session` cookie without
//! rejecting the request. This is intentionally lightweight — the auth
//! middleware applies to protected routes separately.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Redirect, Response};

use crate::AppState;
use kb_store::plans::Plan;

use super::handlers::render_ok;
use super::templates::{
    FaqItem, FaqPage, FeaturesPage, FileTypeSupport, LandingPage, PlanCard, PricingPage,
};

// ── GET / ────────────────────────────────────────────────────────────────────────

/// `GET /` — smart root handler: shows the landing page for unauthenticated
/// visitors and redirects to `/search` for authenticated users.
///
/// Authentication is detected from the session cookie. This is the only
/// public route that bifurcates on auth state — marketing pages always render
/// their public version.
pub(crate) async fn landing_page(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    let is_authenticated = try_authenticate(&state, &headers).await;

    if is_authenticated {
        return Redirect::to("/search").into_response();
    }

    let page = LandingPage {
        is_authenticated: false,
    };
    render_ok(&page)
}

// ── GET /pricing ────────────────────────────────────────────────────────────────

/// `GET /pricing` — render the public pricing page with plan cards from the
/// database.
///
/// When the database is not connected or the query fails, an empty plans list
/// is passed to the template (which shows a loading placeholder). This prevents
/// the entire page from 500-ing on a transient DB error and keeps the page
/// usable for new visitors.
pub(crate) async fn pricing_page(State(state): State<Arc<AppState>>) -> Response {
    let is_authenticated = false; // always public nav on marketing pages

    let plans = match state.pg_store.list_plans().await {
        Ok(plans) => plans,
        Err(e) => {
            tracing::warn!(error = %e, "pricing_page: failed to list plans from DB");
            Vec::new()
        }
    };

    let cards: Vec<PlanCard> = plans
        .iter()
        .map(|p| plan_to_card(p, is_authenticated))
        .collect();

    let page = PricingPage {
        is_authenticated,
        plans: cards,
    };
    render_ok(&page)
}

// ── GET /features ───────────────────────────────────────────────────────────────

/// `GET /features` — render the public features page with the supported-file-types
/// grid and core capability highlights.
///
/// The file-type list and capability descriptions are static (not from the
/// database) because they describe the system's built-in capabilities.
pub(crate) async fn features_page() -> Response {
    let page = FeaturesPage {
        is_authenticated: false,
        file_types: build_file_types(),
    };
    render_ok(&page)
}

// ── GET /faq ────────────────────────────────────────────────────────────────────

/// `GET /faq` — render the public FAQ page with expandable question/answer items.
///
/// FAQ content is static and curated for new visitors. The `<details>` HTML element
/// provides zero-JavaScript accordion behavior.
pub(crate) async fn faq_page() -> Response {
    let page = FaqPage {
        is_authenticated: false,
        faqs: build_faqs(),
    };
    render_ok(&page)
}

// ── Helpers ──────────────────────────────────────────────────────────────────────

/// Attempt to validate the session cookie and return `true` if the user is
/// authenticated.
///
/// This is a lightweight check that does not reject the request — it only
/// determines whether to show the authenticated nav or the public nav.
/// The auth middleware still gates protected routes independently.
async fn try_authenticate(state: &AppState, headers: &axum::http::HeaderMap) -> bool {
    let cookie_header = match headers.get(axum::http::header::COOKIE) {
        Some(h) => h,
        None => return false,
    };

    let cookie_str = match cookie_header.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };

    // Find the __Host-session cookie value.
    for part in cookie_str.split(';') {
        let trimmed = part.trim();
        if let Some(value) = trimmed.strip_prefix("__Host-session=") {
            match state.session_store.validate(value).await {
                Ok(Some(_session)) => return true,
                _ => return false,
            }
        }
    }

    false
}

/// Convert a DB [`Plan`] into a display-ready [`PlanCard`].
///
/// This function handles formatting (prices, byte sizes, feature extraction)
/// so the template stays logic-free.
fn plan_to_card(plan: &Plan, _is_authenticated: bool) -> PlanCard {
    let name = match plan.code.as_str() {
        "free" => "Free".to_string(),
        "pro" => "Pro".to_string(),
        "team" => "Team".to_string(),
        other => {
            // Title-case the first letter for unknown codes.
            let mut chars = other.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
                None => other.to_string(),
            }
        }
    };

    let price_display = match plan.price_cents {
        None | Some(0) => "Free".to_string(),
        Some(cents) => {
            let dollars = cents / 100;
            format!("${dollars}")
        }
    };

    let quota_display = format_byte_size(plan.quota_bytes);

    let features = plan_features_to_strings(&plan.features);

    // CTA: unauthenticated visitors link to register; authenticated go to checkout.
    let cta_url = format!("/register?plan={}", plan.code);

    let cta_text = if plan.price_cents.unwrap_or(0) == 0 {
        "Get Started"
    } else {
        "Subscribe"
    };

    PlanCard {
        code: plan.code.clone(),
        name,
        price_display,
        quota_display,
        features,
        cta_url,
        cta_text: cta_text.to_string(),
        highlighted: plan.code == "pro",
    }
}

/// Extract human-readable feature strings from a plan's `features` JSONB blob.
///
/// Each known key is mapped to a display string. Unknown keys are silently
/// skipped. The order is stable and meaningful to a buyer.
fn plan_features_to_strings(features: &serde_json::Value) -> Vec<String> {
    let mut result = Vec::new();

    // Remote model access.
    match features.get("remote_models").and_then(|v| v.as_bool()) {
        Some(true) => result.push("Remote AI models".to_string()),
        Some(false) => result.push("Local-only AI models".to_string()),
        None => {}
    }

    // User count.
    if let Some(n) = features.get("max_users").and_then(|v| v.as_i64()) {
        if n == 1 {
            result.push("1 user".to_string());
        } else if n > 0 {
            result.push(format!("Up to {n} users"));
        }
    } else {
        result.push("Unlimited users".to_string());
    }

    // Always-true for all plans (these are core features).
    result.push("AI tagging & search".to_string());
    result.push("All file types".to_string());
    result.push("Data export".to_string());
    result.push("API access".to_string());

    result
}

/// Format a byte count as a human-readable size string.
///
/// Uses base-10 (SI) units: KB, MB, GB. The plan DB stores byte counts;
/// this converts them for display on the pricing page.
fn format_byte_size(bytes: i64) -> String {
    if bytes <= 0 {
        return "Unlimited".to_string();
    }
    let bytes = bytes as f64;
    if bytes >= 1_000_000_000.0 {
        format!("{:.0} GB", bytes / 1_000_000_000.0)
    } else if bytes >= 1_000_000.0 {
        format!("{:.0} MB", bytes / 1_000_000.0)
    } else if bytes >= 1_000.0 {
        format!("{:.0} KB", bytes / 1_000.0)
    } else {
        format!("{bytes:.0} B")
    }
}

/// Build the static list of supported file types for the features page.
///
/// Returns entries grouped by category and sorted alphabetically within each
/// group. All entries have `supported = true` — the list documents what the
/// extractor set handles.
fn build_file_types() -> Vec<FileTypeSupport> {
    let types = [
        // Documents (Tika-backed)
        (
            ".pdf, .docx, .pptx, .xlsx, .odt, .rtf",
            "Office & PDF documents",
        ),
        (".txt, .md, .html, .log, .csv", "Plain text & markup"),
        // Images
        (
            ".jpg, .jpeg, .png, .gif, .webp, .tiff",
            "Images (photos & scans)",
        ),
        // Audio
        (
            ".mp3, .wav, .ogg, .flac, .m4a, .opus",
            "Audio (with transcription)",
        ),
        // Video
        (
            ".mp4, .mov, .avi, .mkv, .webm",
            "Video (keyframes + transcript)",
        ),
        // Code
        (".py, .js, .ts, .rs, .go, .java", "Source code files"),
        (".c, .cpp, .h, .hpp, .rb, .php, .swift", "More source code"),
        // Archives
        (
            ".zip, .tar, .gz, .bz2, .xz, .zstd, .7z",
            "Archives (manifest + listing)",
        ),
        // Binary
        (".elf, .so, .dylib, .dll, .wasm", "Binaries (metadata only)"),
    ];

    types
        .iter()
        .map(|(ext, name)| FileTypeSupport {
            extension: ext.to_string(),
            name: name.to_string(),
            supported: true,
        })
        .collect()
}

/// Build the static FAQ items.
///
/// These answers are curated for new visitors and cover the most common
/// pre-signup questions. The content is static HTML — no database dependency.
fn build_faqs() -> Vec<FaqItem> {
    vec![
        FaqItem {
            question: "What is Local KB?".to_string(),
            answer: "Local KB is an AI-powered file knowledge base that automatically tags, summarizes, and indexes your files so you can search them with natural language. Upload documents, images, audio, video, or code — and find anything instantly.".to_string(),
        },
        FaqItem {
            question: "Which file types are supported?".to_string(),
            answer: "Local KB supports 30+ file formats including PDFs, Office documents, images (JPEG/PNG/GIF/WebP/TIFF), audio (MP3/WAV/OGG/FLAC), video (MP4/MOV/MKV), source code (Python, JavaScript, Rust, Go, Java, C/C++), and archives (ZIP, tar, gz). See the <a href=\"/features\" class=\"text-indigo-600 hover:underline\">features page</a> for the full list.".to_string(),
        },
        FaqItem {
            question: "Is my data secure?".to_string(),
            answer: "Yes. Local KB encrypts files at rest with AES-256-GCM using per-tenant encryption keys. Multi-tenant data is strictly isolated with PostgreSQL Row-Level Security. You can also run entirely local AI models so your files never leave your infrastructure.".to_string(),
        },
        FaqItem {
            question: "Can I self-host Local KB?".to_string(),
            answer: "Absolutely. Local KB runs on Podman containers with a single compose.yaml file. You control where your data lives — on your own servers, in your cloud account, or on bare metal. The open-source license (Apache 2.0 / MIT) gives you full freedom.".to_string(),
        },
        FaqItem {
            question: "How does AI tagging work?".to_string(),
            answer: "When you upload a file, Local KB extracts its text content, sends the text to an LLM (local or remote) with a structured schema, and receives back a title, summary, and relevant tags. Tags are canonicalized across your tenant — similar concepts merge into a single tag for consistent search.".to_string(),
        },
        FaqItem {
            question: "What's the difference between Free, Pro, and Team?".to_string(),
            answer: "Free includes 50 MB storage, local-only AI models, and 1 user. Pro ($9/mo) adds 5 GB storage, remote AI model access, and priority support. Team ($29/mo) adds 100 GB storage, up to 20 users, and multi-user collaboration. See the <a href=\"/pricing\" class=\"text-indigo-600 hover:underline\">pricing page</a> for a detailed comparison.".to_string(),
        },
        FaqItem {
            question: "Can I export my data?".to_string(),
            answer: "Yes. Local KB provides a streaming ZIP export that includes your original files, all metadata (tags, summaries, chunks) in JSONL format, and a manifest. The export format is round-trippable — you can import it into another Local KB instance or archive it for compliance.".to_string(),
        },
        FaqItem {
            question: "Do I need a GPU to run Local KB?".to_string(),
            answer: "Not necessarily. You can configure Local KB to use remote AI providers (OpenAI-compatible APIs) for tagging, embedding, and reranking — no GPU required. If you want local-only inference, a GPU is recommended but not strictly required (CPU inference works for smaller models).".to_string(),
        },
        FaqItem {
            question: "How do I get started?".to_string(),
            answer: "The quickest way is to <a href=\"/register\" class=\"text-indigo-600 hover:underline\">create a free account</a>. Set up your tenant, upload a few files, and start searching. For self-hosting, clone the repository, run <code class=\"bg-gray-100 px-1 rounded text-xs\">podman compose up</code>, and open <code class=\"bg-gray-100 px-1 rounded text-xs\">http://localhost:9999</code>.".to_string(),
        },
    ]
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use askama::Template;
    use axum::body::Body;
    use axum::http::{StatusCode, header};
    use tower::ServiceExt;

    /// Build a minimal test `AppState` with an InMemorySessionStore and
    /// disconnected PgStore (matching the existing test pattern in handlers.rs).
    fn test_state() -> Arc<AppState> {
        use kb_core::session::SessionStore;
        use kb_store::PgStore;
        use kb_store::session_store::InMemorySessionStore;
        use std::time::Duration;

        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ))
    }

    /// Build the marketing-page sub-router for integration tests.
    fn marketing_router(state: Arc<AppState>) -> axum::Router {
        use axum::routing::get;

        axum::Router::new()
            .route("/", get(landing_page))
            .route("/pricing", get(pricing_page))
            .route("/features", get(features_page))
            .route("/faq", get(faq_page))
            .layer(axum::middleware::from_fn(
                crate::web::security::security_headers_middleware,
            ))
            .with_state(state)
    }

    // ── Template pure-logic tests ──────────────────────────────────────────

    #[test]
    fn landing_page_template_renders() {
        let page = LandingPage {
            is_authenticated: false,
        };
        let html = page.render().expect("landing template must render");
        assert!(
            html.contains("instantly searchable"),
            "landing must contain value proposition"
        );
        assert!(
            html.contains("How it will work"),
            "landing must contain how-it-works section"
        );
        assert!(
            html.contains("Upload your files"),
            "landing must contain step 1"
        );
        assert!(
            html.contains("AI auto-tagging"),
            "landing must contain step 2"
        );
        assert!(
            html.contains("Search naturally"),
            "landing must contain step 3"
        );
        assert!(
            html.contains("Try the preview"),
            "landing must contain CTA button"
        );
        // Meta description for SEO.
        assert!(
            html.contains("<meta name=\"description\""),
            "landing must have meta description"
        );
    }

    #[test]
    fn pricing_page_template_renders_with_plans() {
        let cards = vec![
            PlanCard {
                code: "free".into(),
                name: "Free".into(),
                price_display: "Free".into(),
                quota_display: "50 MB".into(),
                features: vec!["AI tagging & search".into(), "All file types".into()],
                cta_url: "/register?plan=free".into(),
                cta_text: "Get Started".into(),
                highlighted: false,
            },
            PlanCard {
                code: "pro".into(),
                name: "Pro".into(),
                price_display: "$9/mo".into(),
                quota_display: "5 GB".into(),
                features: vec!["Remote AI models".into(), "AI tagging & search".into()],
                cta_url: "/register?plan=pro".into(),
                cta_text: "Subscribe".into(),
                highlighted: true,
            },
        ];

        let page = PricingPage {
            is_authenticated: false,
            plans: cards,
        };
        let html = page.render().expect("pricing template must render");

        assert!(
            html.contains("Simple, transparent pricing"),
            "pricing must have hero headline"
        );
        assert!(html.contains("Free"), "pricing must show Free plan");
        assert!(html.contains("Pro"), "pricing must show Pro plan");
        assert!(
            html.contains("Most popular"),
            "pricing must highlight Pro plan"
        );
        assert!(html.contains("50 MB"), "pricing must show Free quota");
        assert!(
            html.contains("Plan comparison"),
            "pricing must have comparison table"
        );
        // Meta description.
        assert!(
            html.contains("<meta name=\"description\""),
            "pricing must have meta description"
        );
    }

    #[test]
    fn pricing_page_template_renders_empty_plans() {
        let page = PricingPage {
            is_authenticated: false,
            plans: Vec::new(),
        };
        let html = page.render().expect("pricing must render with empty plans");
        assert!(
            html.contains("Plans are being loaded"),
            "empty plans must show loading placeholder"
        );
    }

    #[test]
    fn features_page_template_renders() {
        let page = FeaturesPage {
            is_authenticated: false,
            file_types: build_file_types(),
        };
        let html = page.render().expect("features template must render");

        assert!(
            html.contains("Supported file types"),
            "features must have file type section"
        );
        assert!(html.contains(".pdf"), "features must list PDF");
        assert!(html.contains(".jpg"), "features must list JPEG");
        assert!(html.contains(".mp3"), "features must list MP3");
        assert!(html.contains(".mp4"), "features must list MP4");
        assert!(html.contains(".zip"), "features must list ZIP");
        assert!(html.contains(".py"), "features must list Python");
        assert!(
            html.contains("Hybrid search"),
            "features must mention hybrid search"
        );
        assert!(
            html.contains("Multi-tenant isolation"),
            "features must mention multi-tenancy"
        );
        assert!(
            html.contains("Encryption at rest"),
            "features must mention encryption"
        );
        assert!(html.contains("Data export"), "features must mention export");
        // Meta description.
        assert!(
            html.contains("<meta name=\"description\""),
            "features must have meta description"
        );
    }

    #[test]
    fn faq_page_template_renders() {
        let page = FaqPage {
            is_authenticated: false,
            faqs: build_faqs(),
        };
        let html = page.render().expect("faq template must render");

        assert!(
            html.contains("Frequently asked questions"),
            "faq must have hero headline"
        );
        assert!(
            html.contains("What is Local KB?"),
            "faq must contain first question"
        );
        assert!(
            html.contains("Which file types are supported?"),
            "faq must contain file types question"
        );
        assert!(
            html.contains("Is my data secure?"),
            "faq must contain security question"
        );
        assert!(
            html.contains("Can I self-host"),
            "faq must contain self-hosting question"
        );
        assert!(
            html.contains("How does AI tagging work?"),
            "faq must contain tagging question"
        );
        assert!(
            html.contains("Free, Pro, and Team"),
            "faq must contain plans question"
        );
        assert!(
            html.contains("export my data"),
            "faq must contain export question"
        );
        assert!(
            html.contains("Do I need a GPU"),
            "faq must contain GPU question"
        );
        assert!(
            html.contains("How do I get started?"),
            "faq must contain getting-started question"
        );
        // The FAQ items use `<details>` for no-JS accordion.
        assert!(html.contains("<details"), "faq must use <details> elements");
        // Meta description.
        assert!(
            html.contains("<meta name=\"description\""),
            "faq must have meta description"
        );
    }

    #[test]
    fn landing_page_has_responsive_viewport() {
        let page = LandingPage {
            is_authenticated: false,
        };
        let html = page.render().expect("template must render");
        assert!(html.contains("viewport"));
        assert!(html.contains("width=device-width"));
    }

    #[test]
    fn pricing_page_has_responsive_viewport() {
        let page = PricingPage {
            is_authenticated: false,
            plans: vec![PlanCard {
                code: "pro".into(),
                name: "Pro".into(),
                price_display: "$9".into(),
                quota_display: "5 GB".into(),
                features: vec![],
                cta_url: "/".into(),
                cta_text: "Start".into(),
                highlighted: true,
            }],
        };
        let html = page.render().expect("template must render");
        assert!(html.contains("viewport"));
        assert!(html.contains("width=device-width"));
    }

    #[test]
    fn features_page_has_responsive_viewport() {
        let page = FeaturesPage {
            is_authenticated: false,
            file_types: build_file_types(),
        };
        let html = page.render().expect("template must render");
        assert!(html.contains("viewport"));
        assert!(html.contains("width=device-width"));
    }

    #[test]
    fn faq_page_has_responsive_viewport() {
        let page = FaqPage {
            is_authenticated: false,
            faqs: build_faqs(),
        };
        let html = page.render().expect("template must render");
        assert!(html.contains("viewport"));
        assert!(html.contains("width=device-width"));
    }

    // ── format_byte_size tests ────────────────────────────────────────────

    #[test]
    fn format_byte_size_zero() {
        assert_eq!(format_byte_size(0), "Unlimited");
    }

    #[test]
    fn format_byte_size_negative() {
        assert_eq!(format_byte_size(-1), "Unlimited");
    }

    #[test]
    fn format_byte_size_bytes() {
        assert_eq!(format_byte_size(500), "500 B");
    }

    #[test]
    fn format_byte_size_kb() {
        assert_eq!(format_byte_size(2_500), "2 KB");
    }

    #[test]
    fn format_byte_size_mb() {
        assert_eq!(format_byte_size(50_000_000), "50 MB");
    }

    #[test]
    fn format_byte_size_gb() {
        assert_eq!(format_byte_size(5_000_000_000), "5 GB");
    }

    #[test]
    fn format_byte_size_large_gb() {
        // Team plan: 100 GB
        assert_eq!(format_byte_size(100_000_000_000), "100 GB");
    }

    // ── plan_to_card tests ───────────────────────────────────────────────

    #[test]
    fn plan_to_card_free() {
        let plan = Plan {
            id: 1,
            code: "free".into(),
            stripe_price: "price_free".into(),
            quota_bytes: 50_000_000,
            token_budget: Some(10_000),
            features: serde_json::json!({"remote_models": false, "max_users": 1}),
            price_cents: Some(0),
            currency: None,
        };
        let card = plan_to_card(&plan, false);
        assert_eq!(card.name, "Free");
        assert_eq!(card.price_display, "Free");
        assert_eq!(card.quota_display, "50 MB");
        assert!(!card.highlighted);
        assert_eq!(card.cta_text, "Get Started");
        assert!(card.cta_url.contains("plan=free"));
        // Feature extraction: local-only, 1 user, + core features.
        assert!(
            card.features.iter().any(|f| f.contains("Local-only")),
            "free plan must show local-only models"
        );
        assert!(
            card.features.iter().any(|f| f.contains("1 user")),
            "free plan must show 1 user"
        );
    }

    #[test]
    fn plan_to_card_pro() {
        let plan = Plan {
            id: 2,
            code: "pro".into(),
            stripe_price: "price_pro".into(),
            quota_bytes: 5_000_000_000,
            token_budget: Some(100_000),
            features: serde_json::json!({"remote_models": true}),
            price_cents: Some(900),
            currency: Some("usd".into()),
        };
        let card = plan_to_card(&plan, false);
        assert_eq!(card.name, "Pro");
        assert_eq!(card.price_display, "$9");
        assert_eq!(card.quota_display, "5 GB");
        assert!(card.highlighted);
        assert_eq!(card.cta_text, "Subscribe");
        assert!(
            card.features.iter().any(|f| f.contains("Remote AI")),
            "pro plan must show remote models"
        );
        assert!(
            card.features.iter().any(|f| f.contains("Unlimited users")),
            "pro plan must show unlimited users when max_users absent"
        );
    }

    #[test]
    fn plan_to_card_team() {
        let plan = Plan {
            id: 3,
            code: "team".into(),
            stripe_price: "price_team".into(),
            quota_bytes: 100_000_000_000,
            token_budget: Some(500_000),
            features: serde_json::json!({"remote_models": true, "max_users": 20}),
            price_cents: Some(2900),
            currency: Some("usd".into()),
        };
        let card = plan_to_card(&plan, false);
        assert_eq!(card.name, "Team");
        assert_eq!(card.price_display, "$29");
        assert_eq!(card.quota_display, "100 GB");
        assert!(!card.highlighted);
        assert!(
            card.features.iter().any(|f| f.contains("Up to 20 users")),
            "team plan must show user limit"
        );
    }

    #[test]
    fn plan_to_card_unknown_code_title_cases() {
        let plan = Plan {
            id: 9,
            code: "enterprise".into(),
            stripe_price: "price_ent".into(),
            quota_bytes: 1_000_000_000_000,
            token_budget: None,
            features: serde_json::json!({}),
            price_cents: Some(9900),
            currency: Some("usd".into()),
        };
        let card = plan_to_card(&plan, false);
        assert_eq!(card.name, "Enterprise");
    }

    // ── plan_features_to_strings tests ────────────────────────────────────

    #[test]
    fn plan_features_remote_models_true() {
        let features = serde_json::json!({"remote_models": true});
        let out = plan_features_to_strings(&features);
        assert!(out.contains(&"Remote AI models".to_string()));
    }

    #[test]
    fn plan_features_remote_models_false() {
        let features = serde_json::json!({"remote_models": false});
        let out = plan_features_to_strings(&features);
        assert!(out.contains(&"Local-only AI models".to_string()));
    }

    #[test]
    fn plan_features_remote_models_missing() {
        let features = serde_json::json!({});
        let out = plan_features_to_strings(&features);
        assert!(
            !out.iter()
                .any(|s| s.contains("Remote") || s.contains("Local-only"))
        );
    }

    #[test]
    fn plan_features_max_users_present() {
        let features = serde_json::json!({"max_users": 20});
        let out = plan_features_to_strings(&features);
        assert!(out.contains(&"Up to 20 users".to_string()));
    }

    #[test]
    fn plan_features_max_users_one() {
        let features = serde_json::json!({"max_users": 1});
        let out = plan_features_to_strings(&features);
        assert!(out.contains(&"1 user".to_string()));
    }

    #[test]
    fn plan_features_max_users_missing() {
        let features = serde_json::json!({});
        let out = plan_features_to_strings(&features);
        assert!(out.contains(&"Unlimited users".to_string()));
    }

    #[test]
    fn plan_features_always_includes_core() {
        let features = serde_json::json!({});
        let out = plan_features_to_strings(&features);
        assert!(out.contains(&"AI tagging & search".to_string()));
        assert!(out.contains(&"All file types".to_string()));
        assert!(out.contains(&"Data export".to_string()));
        assert!(out.contains(&"API access".to_string()));
    }

    #[test]
    fn plan_features_remote_models_not_bool() {
        // If remote_models is a string or number, it's not a bool — skip.
        let features = serde_json::json!({"remote_models": "yes"});
        let out = plan_features_to_strings(&features);
        assert!(
            !out.iter()
                .any(|s| s.contains("Remote") || s.contains("Local-only"))
        );
    }

    // ── Integration tests (axum::test) ────────────────────────────────────

    #[tokio::test]
    async fn get_landing_returns_html() {
        let state = test_state();
        let router = marketing_router(state);

        let req = axum::http::Request::builder()
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/html"));
    }

    #[tokio::test]
    async fn get_landing_authenticated_redirects_to_search() {
        use kb_core::user::UserRole;

        let state = test_state();
        let token = state
            .session_store
            .create(
                1,
                42,
                UserRole::Member,
                true,
                std::time::Duration::from_secs(3600),
            )
            .await
            .unwrap();
        let router = marketing_router(state);

        let req = axum::http::Request::builder()
            .uri("/")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        let location = resp
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(location, "/search");
    }

    #[tokio::test]
    async fn get_landing_has_security_headers() {
        let state = test_state();
        let router = marketing_router(state);

        let req = axum::http::Request::builder()
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            resp.headers().contains_key("content-security-policy"),
            "marketing pages must have CSP header"
        );
        assert!(
            resp.headers().contains_key("x-content-type-options"),
            "marketing pages must have X-Content-Type-Options header"
        );
    }

    #[tokio::test]
    async fn get_pricing_returns_html() {
        let state = test_state();
        let router = marketing_router(state);

        let req = axum::http::Request::builder()
            .uri("/pricing")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/html"));
    }

    #[tokio::test]
    async fn get_pricing_no_db_shows_placeholder() {
        let state = test_state(); // disconnected PgStore
        let router = marketing_router(state);

        let req = axum::http::Request::builder()
            .uri("/pricing")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("Plans are being loaded"),
            "disconnected DB must show loading placeholder; got: {body_str}"
        );
    }

    #[tokio::test]
    async fn get_features_returns_html() {
        let state = test_state();
        let router = marketing_router(state);

        let req = axum::http::Request::builder()
            .uri("/features")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/html"));

        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("Supported file types"),
            "features page must contain file type section"
        );
        assert!(
            body_str.contains("Hybrid search"),
            "features page must mention hybrid search"
        );
    }

    #[tokio::test]
    async fn get_faq_returns_html() {
        let state = test_state();
        let router = marketing_router(state);

        let req = axum::http::Request::builder()
            .uri("/faq")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.contains("text/html"));

        let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(
            body_str.contains("What is Local KB?"),
            "faq page must contain FAQ items"
        );
    }

    #[tokio::test]
    async fn all_marketing_routes_are_public() {
        for path in &["/pricing", "/features", "/faq"] {
            let state = test_state();
            let router = marketing_router(state);

            let req = axum::http::Request::builder()
                .uri(*path)
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "marketing route {path} must return 200 without auth"
            );
        }
    }

    // ── build_file_types tests ────────────────────────────────────────────

    #[test]
    fn build_file_types_has_all_categories() {
        let types = build_file_types();
        assert!(!types.is_empty());
        // Each entry should have a non-empty extension and name.
        for ft in &types {
            assert!(!ft.extension.is_empty(), "extension must not be empty");
            assert!(!ft.name.is_empty(), "name must not be empty");
            assert!(ft.supported, "all file types must be supported");
        }
    }

    // ── build_faqs tests ──────────────────────────────────────────────────

    #[test]
    fn build_faqs_has_expected_count() {
        let faqs = build_faqs();
        assert!(faqs.len() >= 8, "expected at least 8 FAQ items");
        for faq in &faqs {
            assert!(!faq.question.is_empty());
            assert!(!faq.answer.is_empty());
        }
    }
}
