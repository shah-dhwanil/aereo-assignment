# Bulk Certificate Generator

Async bulk certificate issuance service: submit a job with up to 5,000 recipients,
get a 202 back immediately, and download per-recipient PDFs (with verification QR codes)
once background workers finish. Built with **FastAPI + Taskiq (Redis Streams) +
Postgres + Alembic**, managed with **`uv`**, shipped as a **Docker Compose** stack.

## Setup

Prerequisites: Python 3.14, [`uv`](https://docs.astral.sh/uv/), Docker + Compose plugin.

```bash
git clone <repo-url> && cd <repo>
uv sync                # creates .venv, installs locked deps (uv.lock)
cp .env.example .env 2>/dev/null || cat > .env <<'EOF'
BCG_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/certs
BCG_REDIS_URL=redis://localhost:6379/0
BCG_STORAGE_DIR=./storage
BCG_BASE_URL=http://localhost:8000
EOF
```

`BCG_DATABASE_URL` and `BCG_REDIS_URL` are required; everything else has a default
(see `src/aereo_assignment/config.py`: storage `./storage`, base URL
`http://localhost:8000`, `max_generation_attempts=4`, `retry_backoff_seconds=5`,
`reconcile_grace_seconds=120`, `reconcile_interval_seconds=45`, page size 50/200).

For local Postgres/Redis without Docker:

```bash
# then:
uv run alembic upgrade head
```

## Run

One command (builds the app image, migrates on API boot, starts API + worker + scheduler):

```bash
docker compose up --build -d
# API → http://localhost:8000  (OpenAPI docs at /docs)
docker compose logs -f api worker
docker compose down -v
```

Without Docker (needs local Postgres 16 + Redis 7 running):

```bash
uv run alembic upgrade head
uv run uvicorn aereo_assignment.main:app --host 0.0.0.0 --port 8000 &
uv run taskiq worker aereo_assignment.worker.broker:broker aereo_assignment.worker.tasks &
uv run taskiq scheduler aereo_assignment.worker.broker:scheduler aereo_assignment.worker.tasks &
```

> **Single API replica only.** The `api` container runs `alembic upgrade head` on boot,
> which is safe with one replica. Do not scale `api` past 1 without moving migrations
> to an init container or an advisory-locked entrypoint.

> **Taskiq 0.13 CLI note (verified at build time).** `taskiq worker` / `taskiq scheduler`
> take task modules as **positional** args (`broker [modules ...]`); there is no
> `--modules` flag — passing it exits with code 2 (`unrecognized arguments`).
> The compose commands above use the correct positional form.

## Run tests

```bash
uv run pytest -v
```

Full suite (unit + integration; integration tests spin up throwaway Postgres/Redis via
testcontainers). Expect green before committing.

## Submit a request

`POST /api/v1/jobs` accepts `{event_name, event_date, issuer, description?, recipients[]}`
(`recipients`: 1–5,000 × `{name, email, extra_metadata?}`). Returns **202**:

```bash
curl -X POST localhost:8000/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "event_name": "Python Conf 2026",
  "event_date": "2026-11-01",
  "issuer": "Aereo Academy",
  "description": "Speaker certificates",
  "recipients": [
    {"name": "Ada Lovelace", "email": "ada@example.com"},
    {"name": "Alan Turing", "email": "alan@example.com", "extra_metadata": {"track": "keynote"}}
  ]
}'
# {"job_id":"<uuid>","status":"PENDING","total":2,"valid":2,"invalid":0,"created_at":"..."}
```

Validation is **hybrid**: envelope-level problems (empty recipients, bad date) are rejected
with 4xx + `{"error": {"code", "message", "details"}}`; per-row problems (bad email,
over-long name) do **not** fail the request — the row becomes a FAILED certificate and
is counted in `invalid`. `total` / `valid` / `invalid` in the 202 body describe that split.

Idempotent resubmission: pass `Idempotency-Key: <stable-key>`; replaying the same key
returns the stored submission snapshot with header `Idempotency-Replayed: true` instead
of creating a second job.

Poll progress:

```bash
curl localhost:8000/api/v1/jobs/<job_id>
# {"job_id":"...","status":"COMPLETED","counts":{"total":2,"done":2,...},...}
```

Job statuses: `PENDING → PROCESSING → COMPLETED | COMPLETED_WITH_ERRORS | FAILED`
(`CANCELLED` via `POST /api/v1/jobs/<job_id>/cancel`, which soft-cancels queued work).

## Retrieve

List jobs (filterable / paginated):

```bash
curl 'localhost:8000/api/v1/jobs?status=COMPLETED&page=1&size=50'
# {"items":[{"job_id":"...","event_name":"...","status":"...","counts":{...},...}],
#  "page":1,"size":50,"total_items":N,"total_pages":M}
```

List one job's certificates:

```bash
curl 'localhost:8000/api/v1/jobs/<job_id>/certificates?status=DONE&page=1&size=50'
# items: {certificate_id, recipient_name, recipient_email, status,
#         certificate_number, attempts, error_message, download_url, generated_at}
```

Download a single PDF:

```bash
curl -OJ 'localhost:8000/api/v1/jobs/<job_id>/certificates/<cert_id>/download'
```

Download the whole job as a ZIP (one `<CERT-NUMBER>.pdf` per DONE certificate; if some
PDFs are missing from disk they are skipped and recorded in a `SKIPPED.txt` manifest
inside the ZIP):

```bash
curl -o certs.zip 'localhost:8000/api/v1/jobs/<job_id>/download.zip'
```

Verify a certificate (public, no auth; the QR code on each PDF points here):

```bash
curl localhost:8000/api/v1/verify/CERT-2026-XXXXXXXX
# {"certificate_number":"...","recipient_name":"...","recipient_email":"...",
#  "event_name":"...","event_date":"...","issuer":"...","status":"DONE",
#  "issued_at":"...","job_id":"..."}
```

Health (deep check: Postgres `SELECT 1` + Redis `PING`; 200 `ok`, else 503):

```bash
curl localhost:8000/health
# {"status":"ok","checks":{"database":"ok","redis":"ok"}}
```

## Design decisions

- **Postgres is the source of truth; Redis is a disposable queue.** All job/certificate
  state lives in Postgres. Redis Streams only transports task messages — losing the
  stream loses no truth: the periodic + startup **reconcile** pass re-derives what still
  needs doing from the DB and re-enqueues it, and per-certificate claim CAS
  (`UPDATE ... WHERE status IN (PENDING, RETRYING)`) makes re-enqueue safe.
- **Hybrid validation.** Request-level schema errors → 4xx, no rows written.
  Row-level content errors → FAILED certificate rows, job still accepted. Rationale: a
  single malformed email in a 5,000-row batch must not nuke the other 4,999.
- **CAS idempotency table.** `jobs(idempotency_key)` (unique, nullable) + a stored
  `submission_response` snapshot give exactly-once submission semantics: concurrent
  same-key inserts race on the unique constraint and the loser replays the winner.
- **Derived counts.** Job `counts` (`total/done/failed/pending/processing/retrying/cancelled`)
  are never stored — every read aggregates `certificates GROUP BY status`. No
  counter-drift possible under retries, reconcile, or cancel.
- **Retry taxonomy.** `classify_error` splits failures into retryable (timeouts,
  connection drops, 5xx) vs terminal (bad payload, 4xx): retryable rows go through
  `SmartRetryMiddleware` with exponential backoff + jitter (bounded by
  `max_generation_attempts=4`); terminal rows fail fast. Invariant enforced in config:
  `retry_backoff_seconds × max_generation_attempts < reconcile_grace_seconds`, so the
  worst-case backoff never outruns the reconcile window.
- **Reconcile (periodic + startup).** Every `reconcile_interval_seconds` (45s) and on
  every worker/client boot, stale `PROCESSING` rows older than the grace window are
  `force_fail`ed or re-queued and job statuses recomputed — crash recovery without any
  queue-side truth.
- **Vendored fonts.** `assets/fonts/` ships DejaVu Sans (regular + bold, Latin/accented
  coverage) plus Noto Sans JP (regular + bold, CJK fallback), ~13 MB total. No network
  fetch at build or render time.

### Documented deviations from the original spec

1. **`processing` counts key (user decision).** Spec §8's example shows six count keys;
   this API returns seven — `processing` counts certificates currently in PROCESSING,
   separate from `pending` (PENDING only). The seven-key shape is the contract used by
   every endpoint and test.
2. **`RedisStreamBroker` acks + XAUTOCLAIM instead of the spec's BRPOP diagram
   (user decision).** The broker is `taskiq_redis.RedisStreamBroker` with consumer group
   `bulk-certificate-generator`: unacked messages are redelivered via XAUTOCLAIM after
   `reconcile_grace_seconds`, and `SmartRetryMiddleware` (backed by
   `ListRedisScheduleSource`) schedules retries. This changes the transport drawing, not
   the architecture — Redis still holds no truth; reconcile + claim CAS remain the
   recovery mechanism, exactly as with BRPOP.
3. **Two-font coverage (user decision).** DejaVu contains no CJK glyphs, so the spec's
   "DejaVu renders CJK" premise is factually wrong — CJK names would render as tofu.
   The renderer uses DejaVu Sans for Latin/accented text with automatic fallback to
   vendored Noto Sans JP for CJK codepoints. Provenance note: the font URLs in the
   original brief were stale (upstream removed the `fonts/` tree → 404), so the TTFs
   were taken from the official DejaVu `version_2_37` release zip instead; Noto Sans JP
   is OFL-licensed (license texts vendored alongside).
4. **Alembic on boot: single replica only** (see Run note above).

## Verification note

`uv run pytest -v` (full suite) and `docker compose config -q` (compose validity) are
the pre-commit gates; both were green for this change. The end-to-end smoke run
(POST 3-recipient job → poll to COMPLETED → `download.zip` → cert list → verify →
`/health` → teardown) passed against compose-provided Postgres/Redis with the API,
worker, and scheduler running via `uv run`, because this sandbox blocks all
container-to-container traffic (inter-container ping 100% loss, embedded DNS
`127.0.0.11` unreachable), so the API container cannot reach the `postgres`/`redis`
service hostnames. The committed `Dockerfile`/`docker-compose.yaml` are unchanged by
this limitation: images build cleanly, `alembic`/`uvicorn`/`taskiq` entrypoints resolve
from the runtime venv, and the API container boots as far as migrations (failing only
on the sandbox-blocked DB connection).
