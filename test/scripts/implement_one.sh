#!/usr/bin/env bash
# implement_one.sh <abs-path-to-catalog-file>
#
# Spawns ONE headless Claude Code instance to implement all PENDING tests in a
# single catalog file. The agent may edit ONLY that file. Invoked in parallel by
# implement.sh (one process per file → no concurrent edits to the same file).
#
# Env: IMPLEMENT_MODEL (optional --model), AGENT_TIMEOUT (default 2400s),
#      BASE_URL (from test/.env). Always exits 0 so the pool keeps going.
set -uo pipefail

FILE="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # test/
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

STATUS="$SCRIPT_DIR/scripts/catalog_status.py"
ENV_FILE="$SCRIPT_DIR/.env"; [ -f "$ENV_FILE" ] || ENV_FILE="$SCRIPT_DIR/.env.example"
# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a
BASE_URL="${BASE_URL:-https://localhost:9443}"

stem="$(basename "$FILE" .py)"
rel="tests/catalog/$(basename "$FILE")"
mkdir -p "$SCRIPT_DIR/.implement-logs"
log="$SCRIPT_DIR/.implement-logs/${stem}.log"

mapfile -t pending < <(python3 "$STATUS" pending-in "$FILE")
if [ "${#pending[@]}" -eq 0 ]; then
  echo "skip   $stem (no pending tests)"
  exit 0
fi
pending_csv="$(IFS=,; echo "${pending[*]}")"
echo "START  $stem (${#pending[@]} pending: $pending_csv)"

model_arg=()
[ -n "${IMPLEMENT_MODEL:-}" ] && model_arg=(--model "$IMPLEMENT_MODEL")

PROMPT="$(cat <<EOF
You are implementing end-to-end tests in ONE file of an isolated, black-box test
harness for a Rust web app (the Local File Knowledge Base). Work autonomously
until done.

REPO ROOT: $REPO_ROOT
THE ONLY FILE YOU MAY EDIT: $rel
PENDING TESTS to implement (these still carry @pytest.mark.skip(reason="step2…")):
$pending_csv

READ FIRST (read-only): test/README.md, test/CATALOG.md, and the canonical worked
example test/tests/smoke/test_smoke_pipeline.py. The app's HTTP routes are defined
in crates/api/src/lib.rs (JSON API under /auth and /api) and crates/api/src/web/mod.rs
(server-rendered pages); Askama templates are in crates/api/templates/. Read those
to learn exact paths, form field names, and response shapes before writing asserts.

APP API NOTES (so you drive the app correctly):
  - POST /api/ingest is SYNCHRONOUS: it runs the whole pipeline (extract, tag,
    embed, store) before returning 202 with {job_id: 0, document_id, message}.
    job_id is a sentinel 0 — do NOT poll /api/jobs for an upload; the document is
    already processed. Use lib.flows.ingest_and_wait (returns (marker, doc)) or read
    document_id straight from the response. /api/jobs/:id is only for async
    re-tag / re-embed / retry, not the upload itself.

FOR EACH pending test listed above:
  - Remove its @pytest.mark.skip(...) decorator and write a real body.
  - KEEP every other marker on it (the module-level marker, and @pytest.mark.judge
    or @pytest.mark.perf where present).
  - Use the harness helpers, do not reinvent them:
      * the 'api' fixture -> lib.api_client.ApiClient (login/register/ingest_text/
        wait_for_job/search/get_document/health).
      * lib.flows -> unique_marker(), ingest_and_wait(api, content), browser_login(page, ...).
      * the pytest-playwright 'page' fixture for browser journeys (already pointed
        at the app over HTTPS with TLS verification off).
      * the 'judge' fixture for any @pytest.mark.judge test (DeepSeek LLM-as-judge):
        v = judge.verdict(rubric="...", output="...", context="..."); assert v.passed, v.reason
      * lib.config for base_url(), tenant_slug(), admin_email(), admin_password().
  - Make every test ISOLATED and repeatable: tag ingested docs with a unique marker
    (lib.flows.unique_marker); for cross-tenant / isolation tests register fresh
    tenants via api.register(slug, email, password) with unique slugs and use TWO
    ApiClient instances. Assume nothing pre-exists except the bootstrap admin
    (tenant '$KB_TENANT_SLUG', email '$KB_ADMIN_EMAIL', password '$KB_ADMIN_PASSWORD').
  - Encode the CORRECT, intended behavior: assert what the test name + docstring and
    the spec (local-kb-plan.md / README / the route's documented response) say the app
    SHOULD do. Derive WHAT to assert from that contract, NOT from the current
    implementation. Read the route definitions only to learn HOW to call the app
    (paths, field names, response shapes). Use the judge ONLY for nondeterministic
    content quality. NEVER weaken, relax, or remove an assertion to make a test pass.

HARD CONSTRAINTS:
  - Edit ONLY $rel. Do NOT modify lib/, conftest.py, pyproject.toml, the compose
    files, config.e2e.toml, any other test file, or anything under crates/.
  - If you need a helper that does not exist, define it locally INSIDE $rel.

IMPORTANT — a failing test is an ACCEPTABLE, EXPECTED outcome. These tests assert how
the app SHOULD behave; if the app misbehaves the test SHOULD fail, and that failure is
a real app bug we want to surface. Therefore:
  - DO run each test once, if the stack is up at $BASE_URL, to confirm it EXECUTES
    cleanly (imports resolve, fixtures work, the flow runs end to end):
        ./test/run.sh pytest $rel        (judge: add -m judge ; perf: add -m perf)
  - If an ASSERTION fails: LEAVE IT FAILING. Do NOT modify app code, do NOT touch
    anything under crates/, do NOT weaken or relax the assertion, do NOT skip the test.
  - If the test ERRORS for a HARNESS reason (typo, wrong selector or field name,
    fixture misuse, wrong path) it is NOT finished — fix the test mechanics (never the
    assertion's intent) until it runs and checks the right thing.
  - Mark a test "blocked: <why>" (skip with that reason) ONLY if it genuinely cannot be
    expressed or driven in this harness at all — never merely because it fails.
If the stack is not up, implement carefully from the route definitions and say so.

When finished, print one line per test: implemented | implemented (failing) | blocked: <why>.
EOF
)"

if ! command -v claude >/dev/null 2>&1; then
  echo "ERROR  $stem: 'claude' CLI not on PATH" | tee -a "$log"
  exit 0
fi

timeout "${AGENT_TIMEOUT:-2400}" claude -p "$PROMPT" \
  --dangerously-skip-permissions "${model_arg[@]}" >"$log" 2>&1
rc=$?

remaining="$(python3 "$STATUS" pending-in "$FILE" | wc -l | tr -d ' ')"
echo "DONE   $stem (claude exit $rc, $remaining still pending) — log: ${log#"$REPO_ROOT"/}"
exit 0
