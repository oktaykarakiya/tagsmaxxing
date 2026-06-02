//! Dashboard page handler (plan §30, ledger P12-T4).
//!
//! `GET /dashboard` renders an authenticated usage dashboard with:
//! - Storage and token quota bars
//! - Monthly spend (from `cost_micros`)
//! - Daily token breakdown chart (Chart.js)
//! - Recent activity feed
//! - Document/file/tag counts
//!
//! All data is scoped to the authenticated tenant — RLS is enforced by the
//! PgStore methods which call [`begin_tenant_tx`].

use std::sync::Arc;

use askama::Template;
use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};

use crate::AppState;
use crate::AuthUser;

use super::handlers_account::{plan_display_name, quota_bar};
use super::templates::{ActivityFeedEntry, DailyChartPoint, DashboardPage};

// ── Helpers ────────────────────────────────────────────────────────────────────

/// Render a template into an HTML response with the given status code.
fn render_template<T: Template>(template: &T, status: StatusCode) -> Response {
    match template.render() {
        Ok(html) => (status, Html(html)).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "Askama template render failure");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html::<&str>("Internal server error"),
            )
                .into_response()
        }
    }
}

/// Render a template with `StatusCode::OK`.
fn render_ok<T: Template>(template: &T) -> Response {
    render_template(template, StatusCode::OK)
}

/// Convert cost_micros to a display string.
///
/// Free (0) or no cost returns `"$0.00"`. Otherwise converts micro-dollars
/// (1e-6 dollars) to a human-readable dollar amount.
fn spend_display(cost_micros: u64) -> String {
    if cost_micros == 0 {
        "$0.00".into()
    } else {
        let dollars = cost_micros as f64 / 1_000_000.0;
        if dollars < 0.01 {
            "< $0.01".into()
        } else if dollars >= 100.0 {
            format!("${dollars:.0}")
        } else {
            format!("${dollars:.2}")
        }
    }
}

/// Choose a Tailwind CSS badge class for a given activity kind.
fn activity_kind_css(kind: &str) -> &str {
    match kind {
        "ingest" => "bg-blue-100 text-blue-800",
        "search" => "bg-green-100 text-green-800",
        _ => "bg-gray-100 text-gray-600",
    }
}

// ── Handler ────────────────────────────────────────────────────────────────────

/// `GET /dashboard` — the usage dashboard page.
///
/// Fetches all dashboard data for the authenticated tenant and renders the
/// page. Every lookup is fallback-safe: if a query fails, the relevant section
/// shows a zero/empty placeholder rather than returning a 500.
///
/// Unauthenticated requests are caught by the auth middleware (returns 401).
pub async fn dashboard_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, StatusCode> {
    let tenant_id = auth_user.tenant_id;

    // ── Tenant info ──────────────────────────────────────────────────────
    let tenant_name = match state.pg_store.get_tenant_info(tenant_id).await {
        Ok(Some(info)) => info.name,
        Ok(None) => {
            tracing::warn!(tenant_id, "tenant not found for dashboard");
            String::from("Unknown Workspace")
        }
        Err(e) => {
            tracing::error!(error = %e, tenant_id, "failed to look up tenant info");
            String::from("Error")
        }
    };

    // ── Billing / plan ───────────────────────────────────────────────────
    let billing = state.pg_store.get_tenant_billing(tenant_id).await;
    let plan_name = match &billing {
        Ok(Some(b)) => plan_display_name(b),
        _ => "Free".into(),
    };
    let storage_limit = billing
        .as_ref()
        .ok()
        .and_then(|b| b.as_ref())
        .and_then(|b| b.plan.as_ref())
        .map(|p| p.quota_bytes);
    let token_limit = billing
        .as_ref()
        .ok()
        .and_then(|b| b.as_ref())
        .and_then(|b| b.plan.as_ref())
        .and_then(|p| p.token_budget);

    // ── Quota bars ───────────────────────────────────────────────────────
    let (storage_used, token_used) = {
        let s = state
            .pg_store
            .get_storage_usage(tenant_id)
            .await
            .unwrap_or(0);
        let t = state
            .pg_store
            .get_monthly_token_usage(tenant_id)
            .await
            .unwrap_or(0);
        (s, t)
    };
    let storage_bar = quota_bar("Storage", storage_used, storage_limit);
    let token_bar = quota_bar("Tokens (this month)", token_used, token_limit);

    // ── Spend ────────────────────────────────────────────────────────────
    let spend = state
        .pg_store
        .get_monthly_cost(tenant_id)
        .await
        .unwrap_or(0);
    let spend_display_str = spend_display(spend);

    // ── Counts ───────────────────────────────────────────────────────────
    let document_count = state
        .pg_store
        .get_document_count(tenant_id)
        .await
        .unwrap_or(0);
    let file_count = state.pg_store.get_file_count(tenant_id).await.unwrap_or(0);
    let tag_count = state.pg_store.get_tag_count(tenant_id).await.unwrap_or(0);

    // ── Daily token breakdown ────────────────────────────────────────────
    let daily_usage = state
        .pg_store
        .get_daily_token_usage(tenant_id)
        .await
        .unwrap_or_default();
    let chart_points: Vec<DailyChartPoint> = daily_usage
        .into_iter()
        .map(|d| DailyChartPoint {
            date: d.date,
            prompt: d.prompt_tokens,
            completion: d.completion_tokens,
        })
        .collect();
    let daily_chart_json = serde_json::to_string(&chart_points).unwrap_or_else(|_| "[]".into());

    // ── Activity feed ────────────────────────────────────────────────────
    let activity = state
        .pg_store
        .get_recent_activity(tenant_id, 20)
        .await
        .unwrap_or_default();
    let activity_feed: Vec<ActivityFeedEntry> = activity
        .into_iter()
        .map(|a| {
            let kind = a.kind.clone();
            let ts = a.occurred_at.format("%Y-%m-%d %H:%M UTC").to_string();
            ActivityFeedEntry {
                timestamp: ts,
                kind_css: activity_kind_css(&kind).into(),
                kind,
                description: a.description,
            }
        })
        .collect();

    let page = DashboardPage {
        email_verified: auth_user.email_verified,
        tenant_name,
        plan_name,
        storage_bar,
        token_bar,
        spend_display: spend_display_str,
        document_count,
        file_count,
        tag_count,
        daily_chart_json,
        activity_feed,
        error: String::new(),
    };

    Ok(render_ok(&page))
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::super::templates::QuotaBar;
    use super::*;
    use kb_store::PgStore;

    // ── spend_display ────────────────────────────────────────────────────────

    #[test]
    fn spend_display_zero() {
        assert_eq!(spend_display(0), "$0.00");
    }

    #[test]
    fn spend_display_cents() {
        // 500_000 micro-dollars = $0.50
        assert_eq!(spend_display(500_000), "$0.50");
    }

    #[test]
    fn spend_display_dollars() {
        // 2_500_000 micro-dollars = $2.50
        assert_eq!(spend_display(2_500_000), "$2.50");
    }

    #[test]
    fn spend_display_less_than_cent() {
        // 500 micro-dollars = $0.0005
        assert_eq!(spend_display(500), "< $0.01");
    }

    #[test]
    fn spend_display_large() {
        // 123_000_000 micro-dollars = $123.00 → formatted as "$123" (≥$100, no cents)
        assert_eq!(spend_display(123_000_000), "$123");
    }

    #[test]
    fn spend_display_whole_dollars() {
        // 5_000_000 micro-dollars = $5.00
        assert_eq!(spend_display(5_000_000), "$5.00");
    }

    #[test]
    fn spend_display_rounds_down() {
        // 1_234_567 micro-dollars = $1.234567 → "$1.23"
        assert_eq!(spend_display(1_234_567), "$1.23");
    }

    // ── activity_kind_css ────────────────────────────────────────────────────

    #[test]
    fn activity_kind_css_ingest() {
        let css = activity_kind_css("ingest");
        assert!(css.contains("blue"));
    }

    #[test]
    fn activity_kind_css_search() {
        let css = activity_kind_css("search");
        assert!(css.contains("green"));
    }

    #[test]
    fn activity_kind_css_other() {
        let css = activity_kind_css("transcribe");
        assert!(css.contains("gray"));
    }

    // ── Template rendering ───────────────────────────────────────────────────

    #[test]
    fn dashboard_page_template_renders() {
        let page = DashboardPage {
            email_verified: true,
            tenant_name: "My Workspace".into(),
            plan_name: "Pro".into(),
            storage_bar: QuotaBar {
                label: "Storage".into(),
                used_display: "2.5 GB".into(),
                limit_display: "5.0 GB".into(),
                percent: 50,
            },
            token_bar: QuotaBar {
                label: "Tokens (this month)".into(),
                used_display: "250.0K".into(),
                limit_display: "500.0K".into(),
                percent: 50,
            },
            spend_display: "$2.50".into(),
            document_count: 42,
            file_count: 87,
            tag_count: 15,
            daily_chart_json: "[]".into(),
            activity_feed: vec![
                ActivityFeedEntry {
                    timestamp: "2026-06-02 14:30 UTC".into(),
                    kind: "ingest".into(),
                    description: "bge-m3".into(),
                    kind_css: "bg-blue-100 text-blue-800".into(),
                },
                ActivityFeedEntry {
                    timestamp: "2026-06-02 14:25 UTC".into(),
                    kind: "search".into(),
                    description: "Qwen3-VL-30B-A3B".into(),
                    kind_css: "bg-green-100 text-green-800".into(),
                },
            ],
            error: String::new(),
        };
        let html = page.render().expect("dashboard template must render");
        assert!(html.contains("My Workspace"));
        assert!(html.contains("Pro"));
        assert!(html.contains("2.5 GB"));
        assert!(html.contains("250.0K"));
        assert!(html.contains("42"));
        assert!(html.contains("87"));
        assert!(html.contains("15"));
        assert!(html.contains("$2.50"));
        assert!(html.contains("2026-06-02 14:30 UTC"));
        assert!(html.contains("bge-m3"));
    }

    #[test]
    fn dashboard_page_template_renders_unverified() {
        let page = DashboardPage {
            email_verified: false,
            tenant_name: "My Workspace".into(),
            plan_name: "Free".into(),
            storage_bar: QuotaBar {
                label: "Storage".into(),
                used_display: "0 B".into(),
                limit_display: "50.0 MB".into(),
                percent: 0,
            },
            token_bar: QuotaBar {
                label: "Tokens (this month)".into(),
                used_display: "0".into(),
                limit_display: "10.0K".into(),
                percent: 0,
            },
            spend_display: "$0.00".into(),
            document_count: 0,
            file_count: 0,
            tag_count: 0,
            daily_chart_json: r#"[{"date":"2026-06-01","prompt":100,"completion":50}]"#.into(),
            activity_feed: vec![],
            error: String::new(),
        };
        let html = page.render().expect("dashboard template must render");
        assert!(html.contains("Verify your email"));
        assert!(html.contains("Free"));
        assert!(html.contains("$0.00"));
    }

    #[test]
    fn dashboard_page_template_renders_with_error() {
        let page = DashboardPage {
            email_verified: true,
            tenant_name: "Tenant".into(),
            plan_name: "Free".into(),
            storage_bar: QuotaBar {
                label: "S".into(),
                used_display: "0 B".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            token_bar: QuotaBar {
                label: "T".into(),
                used_display: "0".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            spend_display: "$0.00".into(),
            document_count: 0,
            file_count: 0,
            tag_count: 0,
            daily_chart_json: "[]".into(),
            activity_feed: vec![],
            error: "Failed to load some data".into(),
        };
        let html = page
            .render()
            .expect("dashboard template must render with error");
        assert!(html.contains("Failed to load some data"));
    }

    #[test]
    fn dashboard_page_template_includes_chartjs_cdn() {
        let page = DashboardPage {
            email_verified: true,
            tenant_name: "T".into(),
            plan_name: "Free".into(),
            storage_bar: QuotaBar {
                label: "S".into(),
                used_display: "0 B".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            token_bar: QuotaBar {
                label: "T".into(),
                used_display: "0".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            spend_display: "$0.00".into(),
            document_count: 0,
            file_count: 0,
            tag_count: 0,
            daily_chart_json: "[]".into(),
            activity_feed: vec![],
            error: String::new(),
        };
        let html = page.render().expect("render");
        assert!(
            html.contains("chart.js") || html.contains("Chart.js") || html.contains("chartjs"),
            "dashboard should include Chart.js CDN reference"
        );
    }

    #[test]
    fn dashboard_page_template_has_document_count_section() {
        let page = DashboardPage {
            email_verified: true,
            tenant_name: "Test".into(),
            plan_name: "Free".into(),
            storage_bar: QuotaBar {
                label: "S".into(),
                used_display: "0 B".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            token_bar: QuotaBar {
                label: "T".into(),
                used_display: "0".into(),
                limit_display: "Unlimited".into(),
                percent: 0,
            },
            spend_display: "$0.00".into(),
            document_count: 3,
            file_count: 7,
            tag_count: 2,
            daily_chart_json: "[]".into(),
            activity_feed: vec![],
            error: String::new(),
        };
        let html = page.render().expect("render");
        assert!(html.contains("3"), "should show document count");
        assert!(html.contains("7"), "should show file count");
        assert!(html.contains("2"), "should show tag count");
    }

    // ── Handler smoke tests ───────────────────────────────────────────────────

    use axum::body::Body;
    use axum::http::{Request, header};
    use kb_core::session::SessionStore;
    use kb_store::InMemorySessionStore;
    use std::time::Duration;
    use tower::ServiceExt;

    fn make_state() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(3600)),
            false,
        ))
    }

    fn dashboard_router(state: Arc<AppState>) -> axum::Router {
        use axum::middleware::from_fn_with_state;
        use axum::routing::get;

        let protected = axum::Router::new()
            .route("/dashboard", get(dashboard_page))
            .layer(from_fn_with_state(
                state.clone(),
                crate::middleware::auth_middleware,
            ));

        protected.with_state(state)
    }

    #[tokio::test]
    async fn dashboard_requires_auth() {
        let state = make_state();
        let router = dashboard_router(state);
        let req = Request::builder()
            .uri("/dashboard")
            .body(Body::empty())
            .expect("build request");
        let resp = router.oneshot(req).await.expect("send request");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn dashboard_renders_with_disconnected_db() {
        let state = make_state();
        let token = state
            .session_store
            .create(
                1,
                1,
                kb_core::user::UserRole::Owner,
                true,
                Duration::from_secs(3600),
            )
            .await
            .expect("create session");

        let req = Request::builder()
            .uri("/dashboard")
            .header(header::COOKIE, format!("__Host-session={token}"))
            .body(Body::empty())
            .expect("build request");

        let router = dashboard_router(state);
        let resp = router.oneshot(req).await.expect("send request");
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp.into_body(), 10 * 1024 * 1024)
            .await
            .unwrap_or_default();
        let body_str = String::from_utf8_lossy(&body_bytes);
        // Fallback-safe: renders even with a disconnected DB.
        assert!(body_str.contains("Dashboard"));
        assert!(!body_str.is_empty());
    }

    #[test]
    fn daily_chart_point_serialization() {
        let point = DailyChartPoint {
            date: "2026-06-01".into(),
            prompt: 100,
            completion: 50,
        };
        let json = serde_json::to_string(&point).expect("serialize");
        assert!(json.contains("2026-06-01"));
        assert!(json.contains("100"));
        assert!(json.contains("50"));
    }

    #[test]
    fn activity_feed_entry_values_accessible() {
        let entry = ActivityFeedEntry {
            timestamp: "2026-06-02 14:30 UTC".into(),
            kind: "ingest".into(),
            description: "bge-m3".into(),
            kind_css: "bg-blue-100 text-blue-800".into(),
        };
        assert_eq!(entry.kind, "ingest");
        assert_eq!(entry.description, "bge-m3");
        assert!(!entry.timestamp.is_empty());
    }
}
