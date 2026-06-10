# 08 — Upload/Ingest Test Campaign — Findings

**Date:** 2026-06-09 (findings) · 2026-06-10 (resolution) · **Branch:**
`feat/limits-metering-observability` · **Stack:** full model-backed, host-llama
(`text/vision/code`→:9080, `embed`→:9081, `rerank`→:9082, `whisper`→:9093; Postgres + Tika + Caddy).

## Resolution (2026-06-10) — fixes applied + verified

All ten findings were addressed (the campaign was originally report-only; the fixes were
implemented in a follow-up at the user's request). `just ci` is green (fmt / build / clippy
`-D warnings` / test / deny / audit / cov). Each fix is its own commit on the branch with unit
tests; the two §31.5 extractor-boundary changes (F1/F10, F8) are flagged for review — committed on
the branch, **NOT** merged to `main`.

| Finding | Resolution | Ledger id |
|---|---|---|
| **F1** office→archive | OOXML zip detected via a bounded `[Content_Types].xml` peek → its OOXML MIME → Document kind (mirrored in `security.rs` + `document_builder.rs`) | `BUG-INGEST-15` |
| **F2** 500 MiB cap unreachable | doc-only clarification, no behaviour change | (doc) |
| **F3** over-limit → 400 | typed `UploadParseError`; over-size body → **413** via axum `MultipartError::status()`, malformed → 400 | `BUG-INGEST-13` |
| **F4** non-media backend-down → 500 | preserve the typed scheduler error; "no healthy backend" → **503 + Retry-After** (media still degrades; genuine errors still 500) | `BUG-INGEST-14` |
| **F5** backend capacity | environmental — out of app scope (F4 + health-gate convert it to clean 503/skips) | — |
| **F6** monthly-cost query | `SUM(BIGINT)`→NUMERIC; cast `::bigint` so sqlx decodes it (both `get_monthly_cost` + `admin_monthly_spend`) | `BUG-OBS-08` |
| **F7** /health unnamed degraded role | `unhealthy_backend_roles(pool)` folds the down role names into `degradation.subsystems` | `BUG-OBS-09` |
| **F8** audio ffmpeg deadlock | temp-file + `stdin(null)` (mirrors `video.rs`), whole exchange under the timeout — **verified live** (ffmpeg 8.1.1, >64 KiB output in 0.14 s) | `BUG-INGEST-16` |
| **F9** pasta connectivity | environmental — out of app scope | — |
| **F10** JAR → accepted | JAR detected via a `META-INF/MANIFEST.MF` peek → `application/java-archive` → **denied 400** | `BUG-INGEST-15` |

**e2e verification (2026-06-10, fixed binary deployed):** the campaign suite is green —
formats + observability **25 passed / 0 failed**, matrix **50 passed / 20 skipped** (F3's 413 test
now passes; the skips are media/tagger cases the health-gate sheds under load), adversarial + limits
+ extraction + concurrency **18 passed / 20 skipped** (three concurrency/archive assertions widened
to accept the new 503 transient), frontend **8 passed / 1 skipped**, and the original ingest suite
**18 / 18** (no regression — audio ingest now passes, F8). The remaining skips are the F5/F9
environmental backend-saturation cases, which the F4 503 + anti-noise health-gate convert into clean
skips rather than failures.

**Broader regression sweep** (auth / account / session / search / UI / RBAC / scheduler — the areas
these fixes do **not** touch): **170 passed / 43 failed / 84 skipped**. Diffed against the pre-fix
**2026-06-08** baseline, **all 42 of that baseline's failures recur unchanged** (the documented
pre-existing findings: tenant-isolation test-defects ×15, Stripe/SMTP-unconfigured, encryption /
data-governance, account / session-security, search-UX, api-platform), and the **only** delta —
`test_account_team.py::test_dashboard_usage_stats` — is a backend-saturation flake that **passes in
isolation** on an idle backend (25 s; it needs two live tagged ingests to populate the dashboard,
which 503'd under the sweep's sustained load). **Net: zero code regressions.**

---

### Original findings (report-only, 2026-06-09)


## Summary

A combinatorial test campaign was run against the file **upload/ingest** surface
(`POST /api/ingest` JSON API and `POST /upload` browser endpoint). It adds a reusable,
catalog-integrated suite (~108 new tests across 5 lanes + a builder/covering-array library)
and triages every failure as `app-bug` / `spec-ambiguity` / `test-only` / `env-limitation`.

Headline: **the upload validation boundary is sound — no input-handling defects.** Executables,
unknown MIME, zero-byte files, path-traversal filenames, malformed multipart, and NUL-laced content
are all cleanly rejected `400` (never `500`); MIME-spoofing is defeated by magic detection; image
ingestion yields real VLM tags; **whisper transcribes correctly** (`jfk.wav` → the JFK quote in
~13 s); concurrency backpressure (`429`) + dedup work; the browser upload UI is correctly wired.

The material findings are **one app bug + operational/robustness issues**: **F8 (HIGH)** the audio
extractor deadlocks on any real audio (an ffmpeg stdin/stdout pipe deadlock) so audio ingest hangs;
**F9** container→host backend connectivity drops under load (rootless podman/pasta), wedging
tagger-dependent ingest; **F4** non-media ingest returns `500` (not `503`/graceful) when the LLM
backend is unavailable; **F5** the 2 GB-iGPU / ~5 tok-s text backend is capacity-bound. Plus office
docs route to Archive not Tika (**F1**), two spec items (**F2**, **F3**), a broken monthly-cost
metrics query (**F6**), and a `/health` observability gap (**F7**). Every non-F3/F8 test "failure"
in the run traced to F9/F5 backend-outage (hang/500 on the tagger call), **not** to input handling.

## Scope & method (mathematically-bounded coverage)

The upload behaviour is governed by ~10 orthogonal factors:
`endpoint(2) × file-kind(11) × size(8) × count(4) × group(2) × user_note(5) × filename(5) ×
account-state(5) × auth/CSRF(3) × concurrency(3)` → **full Cartesian ≈ 1,584,000** combinations
(infeasible and largely nonsensical).

Reduced — per the NIST/Kuhn empirical result that ≤2-factor interactions trigger the large
majority of faults — to **~150 executable cases** via:
- **Equivalence partitioning** (≈30 concrete MIME types → kind representatives);
- a **2-way (pairwise) covering array** over the orthogonal factors (`lib/covering_array.py`,
  deterministic greedy construction; a meta-test asserts full 2-way coverage);
- **boundary-value analysis** on size / count / note-length / filename-length;
- dedicated lanes for account-state, auth/CSRF, and concurrency (special fixtures + serial exec);
- a **targeted adversarial** set for known-risk inputs.

**Anti-noise gating:** media cases consult `/health` and `pytest.skip` when the relevant
backend is degraded, so a genuine backend outage does not masquerade as an app bug. Every
upload mints an `X-Request-Id` (the app adopts + echoes it) for deterministic log correlation.

**Lanes:** `test_ingest_matrix.py` (pairwise + breadth + BVA, ~70), `test_ingest_adversarial.py`
(17), `test_ingest_limits.py` (8), `test_ingest_concurrency.py` (4), `test_upload_frontend.py`
(9, Playwright). Shared library: `test/lib/file_factory.py` (builders + status-preserving
uploader + health/log helpers) and `test/lib/covering_array.py`.

## Environment notes

- **Whisper IS wired** — there is no `Transcribe` scheduler role; whisper is reached via the
  hot-swappable `WHISPER_URL` env var, and `test/compose.e2e.host.yaml` already sets it to
  `http://host.containers.internal:9093`. The whisper-server (whisper.cpp `large-v3-turbo`) was
  started + a `:9093→:8083` relay added; it transcribes correctly (`jfk.wav` →
  "And so, my fellow Americans…" in ~12–14 s).
- **The GPU is a 2 GB iGPU** (`mem_info_vram_total` = 2 GiB) sharing 60 GB system RAM, so the 35B
  text model runs largely on CPU (~5 tok/s). Running GPU-whisper *alongside* the text/vision model
  **maxes VRAM (≈2021/2048 MiB) and stalls** both — so whisper was moved to **CPU** (`--no-gpu`),
  which frees VRAM and transcribes fine. (GPU-whisper is the user's build; the 2 GB VRAM is the
  binding constraint, not whisper itself.)
- The deployed image was a cached build whose final `cargo build` layer was a cache hit, i.e.
  it already contained the current working-tree code (the in-flight branch changes).

## Baseline — past upload failures (from `test/results/history.csv`)

Historical ingest/upload failures (count × nodeid), dominated by media + async-job cases — the
exact cluster expected when a backend is unavailable mid-run:

| count | test |
|---|---|
| 14 | `test_job_status_progression` |
| 10 | `test_ingest_image_extracts_metadata` |
| 9 | `test_failed_extraction_marks_job_failed` |
| 8 | `test_ingest_code_file` |
| 8 | `test_ingest_binary_file` |
| 7 | `test_browser_upload_flow[chromium]` |
| 6 | `test_ingest_pdf_via_tika` |
| 5 | `test_ingest_video_keyframes_and_audio` / `test_ingest_audio_transcribed` |

These motivated the campaign's **health-gated** assertions (so media outages skip, not fail) and
the focus on the freshly-changed media-tagger fallback path.

## Confirmed-correct behaviours (positive results)

Validated directly against the running app:
- **Executables denied:** ELF (`\x7fELF`) and PE (`MZ`) stubs, and a polyglot ELF-under-`.zip`
  name → `400 {"error":"invalid_upload"}` (content-based detection; extension/declared-MIME
  ignored). No `500`.
- **Unknown bytes denied:** random `application/octet-stream` → `400 invalid_upload`.
- **Zero-byte file** → `400`. **Empty/missing file part** → `400 no_files`.
- **Path-traversal filename** (`../../../../etc/passwd`) → **clean `400`** ("contains
  path-traversal or unsafe characters") for text, pdf, and video — a defense-in-depth *rejection*
  (the traversal never reaches an extractor's temp-file handling). No `500`.
- **MIME spoofing handled:** PNG-as-`text/plain` → re-detected `image/png`; text-as-`image/png`
  → re-detected text. Magic bytes win.
- **Image ingestion works end-to-end:** PNG → `202`, `kind=image`, with VLM-generated title and
  tags (e.g. "Blank White Square Image"; tags `blank image`/`png`/`white background`). The
  media-tagger fallback did **not** fire on a healthy backend (no masking observed).
- **X-Request-Id** is adopted from the client and echoed on the response (log correlation works).

## Findings

### F1 — OOXML office docs route to the Archive extractor, not Tika *(medium / needs-review)*
A minimal `.docx`/`.xlsx` (a valid PK-zip) is detected as `application/zip` by `tree_magic_mini`
**regardless of the declared MIME** (verified with the OOXML MIME, `application/zip`, and
`application/octet-stream` — all three route identically) and is ingested as **`kind=archive`**
(the archive/entry path), not the Tika document path. Content is still indexed (the inner
`word/document.xml` text is extracted by the archive path), and the upload returns `202` in
~15 s with no hang. Open question: whether real Word-generated documents (richer structure) are
detected as the OOXML MIME and reach Tika, or whether all office docs degrade to archive
listing. **BUG-INGEST-05** ("normalise PK-zip magic … so Tika gets them") is marked done, but
the routing observed here sends zips to the Archive extractor. *Recommended:* verify with a
real Word/Excel document; if office docs do not reach Tika, this is a partial-fix regression of
office-doc extraction fidelity. (Builder caveat: the campaign's hand-built OOXML is minimal —
this may be a `tree_magic` detection limitation for minimal OOXML rather than a code defect.)

### F2 — The 500 MiB per-file cap is unreachable on the HTTP path *(low / spec-consistency)*
`MAX_INDIVIDUAL_FILE_BYTES` (500 MiB, `crates/extract/src/security.rs`) is enforced *after* the
full body is buffered, but the 100 MiB total-multipart cap (`MAX_PAYLOAD_BYTES`) always trips
first. So the per-file cap is dead code on the ingest HTTP path. Harmless, but the constant
implies a guarantee that cannot be exercised; consider documenting or removing.

### F3 — 100 MiB-over status code: framework 413 vs app-gate 4xx *(captured by BVA; see lane results)*
Two distinct over-limit mechanisms exist: axum's `DefaultBodyLimit` (`MAX_PAYLOAD_BYTES +
64 KiB` headroom) hard-rejects with `413`, while a body over the soft 100 MiB cap but under the
axum limit reaches the handler's running-total check. The handler docstring says the *total*
cap is a `413`, but the gate `bail!`s as a `400 invalid_multipart`. Exact observed codes are
recorded by `test_bva_just_over_axum_limit_413` / `test_bva_just_over_soft_cap_status`. **Observed:**
a body over the axum hard limit returns **`400 invalid_multipart`** ("failed to read file bytes:
Error parsing `multipart/form-data` request"), *not* the documented `413`. `413 Payload Too Large`
is the semantically-correct status for an over-size body; returning `400` mis-classifies it.
Owning code: `crates/api/src/handlers/ingest.rs` (body-limit / `parse_multipart` error mapping).

### F4 — Non-media ingestion returns 500 (not 503/graceful) when the LLM backend is unavailable *(medium-high / app behavior)*
When the scheduler has no healthy backend for the `text` role, the tagger sub-call fails and
**non-media ingestion returns `500 internal_error` ("tagger failed")**. The *media* path handles
the identical condition gracefully — it logs "tagger failed for media document; ingesting with
default metadata" and returns `202` with default metadata (`crates/pipeline/src/ingest.rs:366-388`).
This asymmetry was the single largest source of campaign test failures (~16 uploads across the
adversarial/limits/matrix lanes), every one correlated 1:1 in `/data/logs/kb.log` to
`ingest handler error / error:"tagger failed" / "scheduler: no healthy backend serves role 'text'"`.
*Impact:* a transient backend hiccup turns every text/document/office/archive upload into a hard
`500` with no retry signal. *Recommendation:* either degrade non-media like media (ingest with
default metadata when the tagger is down) or map "no healthy backend" to `503` + `Retry-After`.
Owning code: `crates/pipeline/src/ingest.rs` (the non-media `Err(e) => return Err(e)` tagger branch),
`crates/api/src/handlers/ingest.rs` (`map_ingest_error`). *§31.5-adjacent — recorded, not fixed.*

### F5 — The single text/vision backend is unstable under ingest load *(high operational / environment)*
The host text/vision backend (Qwen3.6-35B-A3B, AMD/Vulkan, ~2 slots, **~5 tok/s, ~22–27 s per
tagging call**) became unavailable under the campaign's ingest load — first as **circuit-breaker
degradation** (`/health backends:false`, recovers when idle) and then as **outright process death**
(`:8080` connection-refused; its own log ends mid-stream after sustained tagging, consistent with
VRAM/OOM). After a restart it re-degraded within a handful of calls. *Impact:* the stack cannot
sustain even modest ingest throughput; combined with F4 this is why tagger-dependent uploads 500
under load. *Recommendation:* a faster/smaller tagging model or more VRAM/slots; queue/rate-limit
tagger calls; add a watchdog to auto-restart a dead backend. (Environment, not app code — but the
dominant operational risk the campaign surfaced, and consistent with the historical media/job 500
cluster in the baseline.)

### F6 — Monthly-cost metrics query fails for every tenant *(medium / app bug — needs root-cause)*
The background metrics collector logs `WARN "metrics collector: failed to read monthly spend"` →
`"failed to query monthly cost"` for **every tenant on every cycle** (`crates/api/src/metrics_collector.rs:183`
→ `crates/store/src/budget.rs:40` `get_monthly_cost`, a `SUM(cost_micros)` over `usage_events` in a
tenant RLS transaction). The top-level `.context` masks the underlying sqlx error (a logging gap —
the source should be chained), so the precise cause needs investigation (RLS/tenant-tx vs query vs
pool pressure under load). *Impact:* monthly cost/spend metrics — and likely the dashboard cost
figure — are broken, and the log is flooded. Owning code: `crates/store/src/budget.rs`,
`crates/api/src/metrics_collector.rs`.

### F7 — `/health` reports `backends:false` without naming the degraded subsystem *(low / observability)*
When a backend is down, `/health` returns `{"backends":false,"status":"degraded","degradation":
{"ok":true,"subsystems":[]}}` — `degradation.subsystems` is **empty** and `degradation.ok` is
**true**, so neither an operator nor a health-gated client can tell *which* role is degraded from
`/health` alone. Owning code: the `/health` handler / degradation reporting.

### Note — NUL/control bytes in content are *rejected* (400), not sanitized *(spec clarification, not a bug)*
Text content with an early NUL byte is detected as `application/octet-stream` and cleanly
**rejected `400 invalid_upload`** at the MIME gate (the BUG-INGEST-11 content-sanitization runs
later in the pipeline and is never reached). The BUG-INGEST-11 contract — "NUL/control bytes never
cause a `500`" — **holds**; the app refuses such content as binary rather than stripping-and-
ingesting. Either is defensible; flagged only so the intended contract is explicit.

## Lane results

Results are corroborated across the initial run and a **thorough re-run** (whisper started + wired,
the F9 connectivity break fixed by recreate, stack healed between lanes). Under sustained/concurrent
ingest load the backend still flaps (F9 connectivity drop + F5 capacity), so tagger-dependent
uploads intermittently hang (`None`) or `500`; the anti-noise health-gate converts those into SKIPs.
Every non-F3 `500`/hang was confirmed via correlated logs to be "tagger failed / no healthy backend
role `text`" — **not** an input-handling defect. The table reports real results vs backend-outage.

| Lane | Result | Notes |
|---|---|---|
| **matrix** (pairwise + breadth + BVA) | 49 passed · 20 skipped · 1 failed | Real passes: text/code/pdf/archive + **12 extra formats** (gif/jpeg/webp/tiff/mp3/flac/ogg/mkv/webm/c/tar/gz) + reject classes + size/count/note/filename BVA. 20 skips = health-gate correctly skipping media + tagger-dependent cases as the backend degraded late under load. 1 fail = **F3** (over-limit → 400 not 413), backend-independent. Pairwise 2-way coverage proven by the meta-test. |
| **adversarial** (17) | 12 passed · reject contracts all hold | exe/PE/polyglot/octet → `400`; malformed multipart / missing / wrong-field → `400`; path-traversal → `400`; MIME-spoof defeated by magic detection; NUL-content → clean `400`. Remaining non-passes were backend-outage `500`s (F4/F5), not input bugs. |
| **frontend** (9, Playwright) | 8 passed · 1 skipped (healthy run) | Wiring **confirmed**: drop-zone, multi-file, group toggle, `user_note`, CSRF auto-inject, **`document_id` redirect** (the dead `job_id=0` poll path is not taken), reject-error rendering, reorder, control labels. 1 intentional skip (100 MiB client-guard live-trigger). Under a backend flap the 3 *text*-upload redirect tests fail ("stayed on /upload") — backend-outage (text→500→no redirect), not a UI bug; the PNG/UI cases still pass. Now health-gated. |
| **concurrency** (4) | 3 passed · 1 failed | Per-tenant backpressure engages (`429`×28 + `Retry-After`, no silent loss); concurrent identical-bytes dedup is consistent (no `500`/constraint crash); recovers after a burst. The two-tenant **fairness** test failed because, under tenant A's flood, **2 of tenant B's requests hung (`None`)** instead of getting a clean `429`/`503` — a real robustness gap tied to F9 (connectivity dropping under heavy concurrent load). |
| **limits** (8) | 413 / 401 / 403 validated; 429 / 202 backend-blocked | Storage-over-quota → **413**, no-session → **401**, bad-CSRF → **403**, suspended (`billing_status='suspended'`) → **403** all hold. The token-budget **429** and accept (`202`) paths need a successful warm-up ingest, which F5 backend instability blocked from completing reliably. |
| **formats** (20) | denied→400 ✓ · accept gated | Every **denied** family (elf/pe/mach-o/octet-stream) → clean `400` (pre-tagger, reliable). Every allow-listed family (avif/heic/bmp, m4a, mov/avi/3gp, 7z/bz2/xz/zstd, pptx/xlsx/odt/rtf/legacy-doc) has a sample; accepted+routed when the magic is recognised (e.g. m4a→audio), else gated/skipped under the backend flap. Minimal `bz2`/`jar` weren't recognised from the stub (fixture note / F10). |
| **extraction** (8) | gated by backend flap | csv/md/html/json/xml + unicode/RTL + multi-chunk → extracted-content-searchable, markup stripped; all need the tagger, so blocked by F9/F5 (csv→500 mid-flight). Coverage present; verification env-limited (now skips on flap). |
| **observability** (4) | 3 pass · F6 gated | F7 `/health` schema + "name the degraded subsystem" invariant ✓, request-id correlation ✓, backend metrics (`kb_backend_*`, `kb_circuit_breaker_open`) ✓. F6 (the `kb_tenant_spend_monthly_micros` gauge after a metered ingest) gated by the backend flap. |

**Net:** the upload boundary (MIME allow/deny, size, path-traversal, multipart, CSRF, storage
quota) is sound — no validation defects, across the whole pairwise + BVA + adversarial matrix.
The one **functional app bug** is **F8** (audio-extractor ffmpeg deadlock — audio ingest hangs on
real audio). The rest are operational/robustness: **F9** (container→host connectivity drop under
load) and **F4/F5** (no graceful `503` + capacity-bound backend), which together account for every
non-F3 hang/`500`; plus office-doc routing (**F1**), two spec items (**F2**, **F3**), a metrics-query
bug (**F6**), and a `/health` observability gap (**F7**). Audio transcription itself **works** —
the blocker is F8, not whisper.

### F8 — Audio extractor ffmpeg pipe deadlock hangs audio ingest *(HIGH / app bug)*
`crates/extract/src/audio.rs` `RealFfmpeg::transcode` (lines 123–160) spawns
`ffmpeg -i pipe:0 -ar 16000 -ac 1 -f wav pipe:1` and **writes the entire input to ffmpeg's stdin
(`write_all(...).await`, line 137) before reading stdout (`wait_with_output()`, line 145)**. Once
ffmpeg's stdout pipe buffer (~64 KiB) fills, ffmpeg blocks writing the transcoded WAV → stops
draining stdin → the `write_all` blocks **forever**. The stdin write is *not* inside
`ffmpeg_timeout` (which only wraps `wait_with_output`), so it hangs indefinitely: the audio ingest
never returns, the client times out, and — critically — the deadlocked request **never releases its
inflight permit**, so repeated hits exhaust per-tenant/global inflight capacity.
- **Repro:** upload any real audio > ~2 s (e.g. `whisper.cpp/samples/jfk.wav`, 352 KB) → hangs
  >200 s, no completion logged. A ~8 KB synthetic clip (output < 64 KiB) does *not* trigger it —
  which is why mock-transcoder unit tests and tiny fixtures pass and the bug stayed latent.
- Whisper itself is fine: a direct POST, via the `:9093` relay, and **from inside the app
  container** all transcribe `jfk.wav` correctly in ~12–14 s. The hang is purely the extractor's
  pipe handling.
- The **video** extractor (`crates/extract/src/video.rs:167,227`) is unaffected — it writes input
  to a **temp file** and runs ffmpeg with `stdin(null)`. audio.rs should adopt the same temp-file
  pattern (or drain stdout concurrently via `tokio::join!`/a spawned writer) and wrap the whole
  exchange in the timeout.
- Severity **high** — audio transcription is broken for all real-world audio, with an inflight-slot
  leak. Owning code: `crates/extract/src/audio.rs:123-160`. conflict_group: `extract`.

### F9 — Container→host backend connectivity can drop mid-session (rootless podman/pasta) *(medium / env + observability)*
The app container lost the ability to reach the host model relays via `host.containers.internal`
(resolves to `169.254.1.2` under rootless podman/pasta): `host.containers.internal:9080 → 000`
while the relays answered fine on the host loopback (`127.0.0.1:9080 → 200`). All three backends
then read `kb_backend_healthy=0` and every tagger-dependent ingest failed/hung — *masquerading as
backend instability*. A `podman compose up -d --force-recreate app caddy` (re-establishing the
container's pasta networking) restored it (`backends:true`, all healthy). firewalld is active on
this Fedora host. *Impact:* a silent connectivity drop wedges all ingest; with F7 it is hard to
diagnose. *Recommendation:* stabilize the host-gateway path (firewalld zone/rule for the podman
bridge) and have backend health surface *why* a backend is unreachable. **This revises F4/F5:** much
of the earlier "backends:false / tagger failed" was this connectivity drop, not chronic crashing.

### F10 — A minimal JAR is detected as zip → accepted, not denied *(low / needs real-jar check)*
`application/java-archive` is in the deny-list, but a minimal JAR (a zip containing
`META-INF/MANIFEST.MF`) is detected by `tree_magic_mini` as **`application/zip`** and **accepted**
as an Archive (it then reaches the tagger), rather than denied as an executable. So the executable
deny-list may be bypassable by JARs that don't trigger java-archive detection. Needs verification
with a *real* `.jar` (does `tree_magic_mini` flag it as java-archive?). If real jars also detect as
zip, harden the deny path (inspect zip structure / honor the `.jar` extension). Owning code:
`crates/extract/src/security.rs`. Severity low — jars are stored/listed as archives, not executed.

## Recommended next steps

1. **F8 (audio deadlock — the one functional app bug)** — fix `crates/extract/src/audio.rs` to drain
   ffmpeg's stdout concurrently with the stdin write (`tokio::join!` / spawned writer) or feed input
   via a temp file like `video.rs`, with the whole exchange under the timeout. Without it, audio
   ingest hangs on any real audio and leaks inflight permits.
2. **F9 (connectivity under load)** — stabilize the rootless-podman/pasta container→host path
   (firewalld zone/rule for the podman bridge); it silently drops under load and is the dominant
   cause of the run's hangs/500s.
3. **F5 (capacity)** — the 35B text backend can't sustain tagging on this 2 GB-iGPU / ~5 tok-s host.
   Add a watchdog to auto-restart a dead `:8080`; consider a smaller/faster tagging model or
   queued/rate-limited tagger calls; set the fan (`ectool fanduty`) under sustained load.
4. **F4 (degradation asymmetry)** — make non-media ingest degrade like media (default metadata) or
   return `503 + Retry-After` instead of `500` when the tagger backend is unavailable.
5. **F6 (metrics)** — chain the source error in `get_monthly_cost`'s `.context` to expose the root
   cause, then fix the failing monthly-cost query.
6. **F1 (office docs)** — verify with a real Word/Excel file whether OOXML reaches Tika; if not,
   restore office-doc text extraction fidelity.
7. **F3 / F7** — return `413` for over-size bodies; name the degraded subsystem in `/health`.

**Promotion to `BUG_LEDGER.toml`:** F4/F6/F3 touch `crates/api/src/handlers/ingest.rs` and
`crates/store/src/budget.rs`, which are **not in any conflict group** — assign them a group (or
extend `api_handlers` / add a `store_budget` group) before adding ledger entries so the parallel
fix-loop's conflict detection stays correct. F1→`extract`, F7→`metrics_defs` map cleanly today. The
failing intent-encoding tests (e.g. `test_bva_just_over_axum_limit_413`) are left red per the suite's
"a failure is a recorded bug" convention.

## Test artifacts added (report-only; test code only)

- `test/lib/file_factory.py` — binary builders for **every** allow-listed + denied MIME family
  (text/pdf/ooxml `docx`/`xlsx`/`pptx`/`odt`/legacy-`doc`/`rtf`; `png`/`jpeg`/`gif`/`webp`/`tiff`/
  `bmp`/`avif`/`heic`; `wav`/`mp3`/`ogg`/`flac`/`m4a`; `mp4`/`mkv`/`webm`/`mov`/`avi`/`3gp`;
  `zip`/`tar`/`gzip`/`7z`/`bz2`/`xz`/`zstd`; code `.py`/`.c`/`.sh`; denied `elf`/`pe`/`mach-o`/`jar`);
  `text_payloads` (csv/md/html/json/xml); status-preserving `upload()` with `/upload` CSRF +
  `X-Request-Id`; streaming large-body builder; `/health` gating + `podman logs` correlation.
- `test/lib/covering_array.py` — deterministic pairwise covering-array generator + verifier.
- Lanes: `test_ingest_matrix.py` (pairwise+BVA+breadth), `test_ingest_adversarial.py`,
  `test_ingest_limits.py`, `test_ingest_concurrency.py`, `test_upload_frontend.py`,
  `test_ingest_formats.py` (all families handled / denied→400), `test_ingest_extraction.py`
  (csv/md/html/json/xml + unicode/RTL + multi-chunk → searchable, markup stripped),
  `test_ingest_observability.py` (F6 monthly-cost gauge, F7 `/health` subsystem naming, request-id
  correlation, backend metrics). ~150 tests total + the existing catalog.
- `test/pyproject.toml` — added the `matrix` marker.

Existing catalog already covers (verified, not duplicated): HTTP methods/405, content-type rejection,
404 routing, unicode/emoji content round-trip, NUL/control bytes, oversized note, XSS/SQLi-in-input,
idempotent re-upload, presigned download, file-visualization metadata, job-status progression.

No application code was modified.
