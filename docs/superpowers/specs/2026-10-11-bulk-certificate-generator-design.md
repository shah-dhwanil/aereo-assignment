# Bulk Certificate Generator — Design

**Status:** draft v1.1 (pending approval; implementation plan follows via writing-plans skill)
**Date:** 2026-10-11
**Source assignment:** `Bulk_Certificate_Generator_Backend_Assignment.pdf`

> How to read this doc: prose defines the contract; mermaid diagrams define the
> behavior. If they ever disagree, the diagram wins and the prose gets patched.

---

## 1. Intent & why

A backend API that accepts **one request containing many recipients** and generates
a certificate PDF per recipient from a single predefined template, with per-job
progress tracking and retrieval.

Built as an interview-defensible assignment: every line explainable and modifiable,
every decision justified, tests for the required scenarios, docs for
setup / run / test / submit / retrieve.

---

## 2. Goals / Non-goals / Anti-goals

| Goals | Non-goals | Anti-goals |
|---|---|---|
| Bulk submission in one call | Template editor / multiple designs | One-API-call-per-certificate UX |
| Failure isolation per recipient | Email delivery / notifications | A design where one bad row kills a job |
| Honest, live progress tracking | Multi-tenant auth | Counters that drift from reality |
| Retrieval: list, per-cert PDF, job ZIP | Async streaming renderer rewrite | Redis/queue owning any critical state |
| Idempotent end-to-end | Horizontal scaling benchmarks | Multiple bespoke recovery paths |
| Verification URL + QR per certificate | S3 implementation (documented path only) | Recovery logic that lives in two places |

---

## 3. Constraints & decisions

**Stack (locked):** Python 3.14 · FastAPI + uvicorn · Postgres 16 via SQLAlchemy 2.0
async (asyncpg driver) · pydantic-settings · **Taskiq + Redis 7** (broker kept
swappable — RabbitMQ via `taskiq-aio-pika` is a one-line change) · Alembic ·
reportlab · testcontainers · Docker.

**Decisions (locked):**

- Hybrid validation rule — invalid rows become per-cert `FAILED` rows inside an
  accepted job; `422` only when **zero** rows are valid.
- Hybrid processing — validate + insert inline (the 202 receipt), fan-out in the
  queue (one `dispatch_job` task → N `generate_pdf` tasks).
- Disk storage with a swappable interface; S3 documented as the production path.
- Expanded statuses incl. `RETRYING`; verification URL + QR; cancel; ZIP download.
- Dockerfile + compose for `api` + `worker`.

**Defaults (mine — veto anytime):** `segno` for QR · 5,000 recipients/job cap ·
pagination 50 default / 200 max · CAS (compare-and-swap) status transitions as
*the* idempotency mechanism · certificate numbers `CERT-YYYY-XXXXXXXX` ·
soft-cancel semantics (in-flight certs finish) · optional `Idempotency-Key`
header · counts always derived via `GROUP BY` · DejaVuSans TTF vendored for
Unicode PDF rendering.

---

## 4. Architecture

```mermaid
flowchart TB
    Client["Client<br/>curl / LMS / browser"]

    subgraph API["API — FastAPI + uvicorn"]
        Routers["routers → services → repositories → SQLAlchemy async<br/>validate rows ▸ job + certs in 1 TX ▸ enqueue ONE dispatch_job"]
    end

    Postgres[("Postgres 16<br/>jobs, certificates<br/>STATE OF RECORD")]
    Redis[("Redis 7<br/>Taskiq broker<br/>DISPOSABLE dispatch")]

    subgraph WORKER["WORKER — taskiq worker"]
        Dispatch["dispatch_job job_id<br/>1 per job → enqueue N generate_pdf"]
        Gen["generate_pdf cert_id<br/>guard ▸ PROCESSING ▸ render ▸ store ▸ DONE"]
        Recon["WORKER_STARTUP reconcile<br/>orphan recovery"]
    end

    Disk[("Local disk<br/>storage/{job_id}/{cert_no}.pdf<br/>prod: same interface → S3")]

    Client -->|"1. POST /jobs<br/>2. GET status<br/>3. GET download / zip"| Routers
    Routers -->|"SQL (ACID)"| Postgres
    Routers -->|"kiq dispatch_job"| Redis
    Redis -->|"BRPOP"| WORKER
    WORKER -->|"SQL: claim / commit / recompute"| Postgres
    WORKER -->|"render + write tmp then os.replace"| Disk
    Routers -->|"read file for download / zip"| Disk
```

### Component responsibilities

| Component | Owns | Never owns |
|---|---|---|
| FastAPI routers | HTTP shape, 202/422/404/409 mapping | generation logic |
| job_service | row validation, transactional insert, dispatch enqueue | PDF internals |
| dispatch_job task | PENDING-cert scan → per-cert enqueue | job-level status decisions |
| generate_pdf task | claim → render → store → cert status, retry policy | job-level decisions |
| reconcile (worker startup) | reset stale certs, re-enqueue dispatch + certs | client-visible state |
| storage service | write/read/zip of files (disk impl; S3-swappable) | DB records |
| Postgres | all truth: jobs, certs, statuses | queues |

### Key invariant

> **Postgres decides what is true. Redis decides what runs next.**
> Redis holds no state that isn't reconstructible from Postgres.

---

## 5. Request lifecycle (hybrid processing)

```mermaid
flowchart TD
    A["POST /jobs<br/>event + up to 5000 recipients"] --> B["Parse + validate every row<br/>pure CPU, no IO"]
    B --> C{"valid rows?"}
    C -- "zero valid" --> D["422 VALIDATION_ERROR<br/>details per row<br/>nothing persisted"]
    C -- ">= 1 valid" --> E["ONE TX: INSERT job PENDING<br/>+ INSERT N cert rows<br/>valid → PENDING<br/>invalid → FAILED + reason"]
    E --> F["kiq ONE message<br/>dispatch_job job_id"]
    F --> G["202 Accepted<br/>job_id, total, valid, invalid"]
    G --> H["worker: dispatch_job<br/>CAS job → PROCESSING<br/>enqueue generate_pdf × N"]
    H --> I["worker: generate_pdf per cert<br/>claim → render → store → DONE<br/>retryable → RETRYING → redeliver<br/>permanent/exhausted → FAILED"]
    I --> J["recompute job status<br/>from cert totals"]
```

**Why here, not elsewhere:** the 202 response depends on validation results and
recipient rows, so they are inline (the client's receipt). Rendering is
slow / IO-bound / fallible, so it is queued. One dispatch task decouples Redis
availability from submission. Fully-deferred validation is a documented
escalation path for payloads above a large threshold.

---

## 6. State diagrams

### 6.1 Job

```mermaid
stateDiagram-v2
    [*] --> PENDING : POST /jobs
    PENDING --> PROCESSING : dispatch_job CAS
    PROCESSING --> COMPLETED : all certs DONE
    PROCESSING --> COMPLETED_WITH_ERRORS : >=1 DONE and >=1 FAILED
    PROCESSING --> FAILED : all FAILED
    PENDING --> CANCELLED : POST cancel CAS
    PROCESSING --> CANCELLED : POST cancel CAS
    COMPLETED --> [*]
    COMPLETED_WITH_ERRORS --> [*]
    FAILED --> [*]
    CANCELLED --> [*]

    note right of PROCESSING
        terminal states never re-entered;
        recompute exits immediately
        for terminal jobs
    end note
```

### 6.2 Certificate

```mermaid
stateDiagram-v2
    [*] --> FAILED : invalid row at submit<br/>with reason, NEVER queued
    [*] --> PENDING : valid row
    PENDING --> PROCESSING : claim CAS<br/>PENDING or RETRYING, attempts + 1
    PROCESSING --> DONE : render + store ok<br/>CAS from PROCESSING
    PROCESSING --> RETRYING : retryable error<br/>attempts below max
    RETRYING --> PROCESSING : redeliver with backoff
    RETRYING --> FAILED : attempts exhausted
    PROCESSING --> FAILED : permanent error
    PENDING --> CANCELLED : cancel CAS
    RETRYING --> CANCELLED : cancel CAS
    DONE --> [*]
    FAILED --> [*]
    CANCELLED --> [*]

    note right of PROCESSING
        EVERY transition:
        UPDATE ... WHERE id = ? AND status IN from-states
        rowcount 0 = someone else moved it = no-op
    end note
```

---

## 7. Sequence diagrams

### 7.1 Happy path

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant P as Postgres
    participant R as Redis
    participant W as Worker
    participant D as Disk

    C->>A: POST /api/v1/jobs (3 recipients)
    A->>A: validate rows (3 valid)
    A->>P: TX: INSERT job PENDING + 3 certs PENDING
    P-->>A: COMMIT
    A->>R: kiq dispatch_job(job_id)
    A-->>C: 202 {job_id, total 3, valid 3, invalid 0}
    R->>W: BRPOP dispatch_job
    W->>P: CAS job PENDING → PROCESSING
    W->>R: kiq generate_pdf × 3
    R->>W: BRPOP generate_pdf cert_1
    W->>P: CAS cert_1 → PROCESSING (attempts 1)
    W->>D: render PDF + tmp→replace
    W->>P: CAS cert_1 → DONE + file_path
    W->>P: recompute job (still PROCESSING)
    Note over W,R: cert_2, cert_3 identical
    W->>P: recompute job → COMPLETED
    C->>A: GET /jobs/{id}
    A->>P: GROUP BY counts
    A-->>C: COMPLETED, done 3 fail 0
```

### 7.2 Mixed validity (hybrid validation rule)

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant P as Postgres
    participant R as Redis
    participant W as Worker

    C->>A: POST /jobs (3 recipients, #2 email bad)
    A->>A: validate: #0 ok, #1 ok, #2 invalid email
    A->>P: TX: INSERT job PENDING<br/>+ cert#2 = FAILED {reason}
    P-->>A: COMMIT
    A->>R: kiq dispatch_job (enqueues cert#0, cert#1 only)
    A-->>C: 202 {total 3, valid 2, invalid 1}
    Note over W: later: cert#0 DONE, cert#1 DONE
    W->>P: recompute → COMPLETED_WITH_ERRORS
    C->>A: GET /jobs/{id}
    A-->>C: succeeded 2, failed 1<br/>cert row FAILED invalid email
```

### 7.3 Retry (transient failure, attempts-capped)

```mermaid
sequenceDiagram
    participant W as Worker
    participant P as Postgres

    W->>P: CAS cert_1 → PROCESSING (attempts 1)
    Note over W: render → OSError disk full (retryable)
    W->>P: CAS cert_1 → RETRYING
    Note over W: raise → SmartRetryMiddleware redelivers with backoff
    W->>P: CAS cert_1 → PROCESSING (attempts 2)
    Note over W: still failing → RETRYING, redeliver
    W->>P: CAS cert_1 → PROCESSING (attempts 3)
    Note over W: still failing → RETRYING, redeliver
    W->>P: CAS cert_1 → PROCESSING (attempts 4 = max)
    Note over W: still failing, attempts >= max
    W->>P: CAS cert_1 → FAILED {disk full}, return, NO re-raise
    W->>P: recompute job → COMPLETED_WITH_ERRORS<br/>(sibling certs unaffected)
```

### 7.4 Cancel (soft cancel, idempotent)

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant P as Postgres
    participant W as Worker

    C->>A: POST /jobs/{id}/cancel
    A->>P: TX: CAS job PENDING|PROCESSING → CANCELLED<br/>certs PENDING|RETRYING → CANCELLED
    P-->>A: COMMIT
    A-->>C: 200 CANCELLED + counts
    Note over W: worker holds cert#7 mid-flight PROCESSING
    W->>P: guard: job CANCELLED → no-op<br/>or finishes → DONE, still retrievable
    W->>P: recompute: job terminal → exit immediately<br/>job stays CANCELLED
    C->>A: POST /jobs/{id}/cancel (repeat)
    A-->>C: 200 same state (idempotent)
    Note over C,A: cancel after COMPLETED / FAILED → 409 JOB_ALREADY_FINAL
```

### 7.5 Duplicate delivery & crash recovery (the idempotency story)

```mermaid
sequenceDiagram
    participant W1 as Worker 1
    participant W2 as Worker 2
    participant P as Postgres
    participant R as Redis

    Note over W1,W2: A. Duplicate delivery — two workers get same cert_id
    W1->>P: CAS cert → PROCESSING (wins)
    W2->>P: CAS cert → PROCESSING (rowcount 0)
    Note over W2: status no longer PENDING|RETRYING → no-op return
    Note over W1: W1 dies before DONE → cert stuck PROCESSING

    Note over P,R: B. WORKER_STARTUP reconcile covers everything below
    Note over P,R: API crash pre-enqueue · Redis flush<br/>worker crash mid-render · duplicate dispatch
    R->>P: reset stale: CAS PROCESSING|RETRYING<br/>with updated_at older than grace → PENDING
    R->>P: enqueue dispatch_job for stale PENDING jobs
    R->>R: enqueue generate_pdf for every PENDING cert
    Note over P,R: duplicate enqueues are normal;<br/>claim guard from A makes them no-ops

    Note over P,R: C. Deterministic outputs
    Note over P,R: file path = f job_id, cert_no<br/>write tmp then os.replace = identical bytes<br/>recompute = pure function of cert rows
```

### 7.6 Retrieval & verification

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant P as Postgres
    participant D as Disk

    C->>A: GET /jobs/{id}/certificates?status=DONE
    A->>P: SELECT cert rows
    A-->>C: [{cert_id, number, download_url}]

    C->>A: GET .../certificates/{cert_id}/download
    A->>P: SELECT cert (DONE?)
    alt not DONE
        A-->>C: 409 CERT_NOT_READY
    else DONE, file missing
        A->>D: read file → missing
        A-->>C: 409 FILE_MISSING
    else DONE
        A->>D: read file
        A-->>C: 200 application/pdf
    end

    C->>A: GET /jobs/{id}/download.zip
    A->>P: SELECT DONE paths
    alt zero DONE
        A-->>C: 409 NO_CERTIFICATES_READY
    else
        A->>D: read files
        A-->>C: 200 application/zip stream
    end

    C->>A: GET /api/v1/verify/CERT-2026-XXXX
    A->>P: SELECT by number
    alt unknown
        A-->>C: 404 CERTIFICATE_NOT_FOUND
    else known
        A-->>C: 200 {recipient, event, issuer, issued_at}
    end
```

---

## 8. API spec

**Conventions** — base path `/api/v1`, JSON, timestamps ISO-8601 UTC. All errors
share one envelope:

```json
{ "error": { "code": "STRING_CODE", "message": "human readable", "details": [] } }
```

**Enums:** `JobStatus = PENDING | PROCESSING | COMPLETED | COMPLETED_WITH_ERRORS | FAILED | CANCELLED`
· `CertificateStatus = PENDING | PROCESSING | RETRYING | DONE | FAILED | CANCELLED`

### 8.1 `POST /api/v1/jobs` — submit a bulk job

Header: `Idempotency-Key: <uuid>` (optional).

Request:

```json
{
  "event_name": "Advanced Robotics Workshop 2026",
  "event_date": "2026-03-15",
  "issuer": "Aereo Academy",
  "description": "For successful completion of the 40-hour program.",
  "recipients": [
    { "name": "Dhwanil Shah", "email": "d@x.com",
      "extra_metadata": { "score": 95, "grade": "A+" } },
    { "name": "Bad Row", "email": "not-an-email" }
  ]
}
```

Validation rules:

| Field | Rule |
|---|---|
| `event_name`, `issuer` | required, 1–200 chars, trimmed |
| `event_date` | required, `YYYY-MM-DD` |
| `description` | optional, ≤1000 chars |
| `recipients` | required array, 1–5000 |
| `recipients[].name` | required, 1–200 chars after strip |
| `recipients[].email` | required, valid email format |
| `recipients[].extra_metadata` | optional, ≤10 keys, values string/int/bool ≤200 chars |

Responses:

- `202` — `{job_id, status: "PENDING", total, valid, invalid, created_at}`.
- `422 VALIDATION_ERROR` — zero valid rows; `details[]` carries
  index/field/message. Schema-level parse errors use the same envelope with
  field paths.
- `409 IDEMPOTENCY_KEY_REUSED` — same key, different payload.
- Replay of the same key + same payload → original `202` body verbatim plus
  `Idempotency-Replayed: true` header (see §10.5).

### 8.2 `GET /api/v1/jobs` — list jobs

Query: `status?`, `page=1`, `size=50 (≤200)`. Response `200`:

```json
{ "items": [ { "job_id": "uuid", "event_name": "...", "status": "PROCESSING",
    "counts": { "total": 3, "done": 0, "failed": 1, "pending": 2,
                "retrying": 0, "cancelled": 0 },
    "created_at": "...", "completed_at": null } ],
  "page": 1, "size": 50, "total_items": 12, "total_pages": 1 }
```

### 8.3 `GET /api/v1/jobs/{job_id}` — job detail

`200` — single item of the same shape + `event_date`, `issuer`, `updated_at`.
`404 JOB_NOT_FOUND`.

### 8.4 `POST /api/v1/jobs/{job_id}/cancel`

`200` — `{job_id, status: "CANCELLED", counts: {...}}`, idempotent on
already-cancelled. `404 JOB_NOT_FOUND`. `409 JOB_ALREADY_FINAL` when status ∈
`{COMPLETED, COMPLETED_WITH_ERRORS, FAILED}`.

### 8.5 `GET /api/v1/jobs/{job_id}/certificates` — per-recipient rows

Query: `status?`, `page`, `size`. `200`:

```json
{ "items": [ { "certificate_id": "uuid", "recipient_name": "...",
    "recipient_email": "...", "status": "FAILED",
    "certificate_number": "CERT-2026-7K3D9F2A", "attempts": 3,
    "error_message": "disk full", "download_url": null,
    "generated_at": null } ],
  "page": 1, "size": 50, "total_items": 3, "total_pages": 1 }
```

`download_url` is populated only when status = `DONE`.

### 8.6 `GET /api/v1/jobs/{job_id}/certificates/{cert_id}/download`

`200` `application/pdf`,
`Content-Disposition: attachment; filename="CERT-2026-7K3D9F2A.pdf"`.
`404 CERT_NOT_FOUND` · `409 CERT_NOT_READY` (status ≠ DONE) ·
`409 FILE_MISSING` (DONE, file gone).

### 8.7 `GET /api/v1/jobs/{job_id}/download.zip`

`200` `application/zip` stream of every DONE cert (filenames = certificate
numbers). `404 JOB_NOT_FOUND` · `409 NO_CERTIFICATES_READY` (zero DONE).

### 8.8 `GET /api/v1/verify/{certificate_number}`

`200` — `{certificate_number, recipient_name, recipient_email, event_name,
event_date, issuer, status: "DONE", issued_at, job_id}`.
`404 CERTIFICATE_NOT_FOUND`.

### 8.9 `GET /health`

`200` — `{status: "ok", checks: {database: "ok", redis: "ok"}}`.
`503` with per-check detail on failure.

---

## 9. Data model

```mermaid
erDiagram
    jobs ||--o{ certificates : has

    jobs {
        uuid id PK
        uuid idempotency_key UK "NULL allowed"
        text event_name
        date event_date
        text issuer
        text description "NULL allowed"
        job_status status "DEFAULT PENDING"
        jsonb submission_response "original 202 snapshot"
        timestamptz created_at
        timestamptz updated_at
        timestamptz completed_at "NULL until terminal"
    }

    certificates {
        uuid id PK
        uuid job_id FK "ON DELETE CASCADE"
        text recipient_name
        text recipient_email
        jsonb extra_metadata "DEFAULT '{}'"
        text certificate_number UK "CERT-YYYY-XXXXXXXX"
        cert_status status "DEFAULT PENDING"
        smallint attempts "DEFAULT 0"
        text error_message "NULL allowed"
        text file_path "NULL until DONE"
        timestamptz created_at
        timestamptz updated_at
        timestamptz generated_at "NULL until DONE"
    }
```

DDL sketch (Alembic migration implements this exactly):

- `jobs(id uuid PK DEFAULT gen_random_uuid(), idempotency_key uuid UNIQUE NULL,
  event_name text NOT NULL, event_date date NOT NULL, issuer text NOT NULL,
  description text NULL, status job_status NOT NULL DEFAULT 'PENDING',
  submission_response jsonb NULL, created_at/updated_at timestamptz NOT NULL
  DEFAULT now(), completed_at timestamptz NULL)`
- `certificates(id uuid PK DEFAULT gen_random_uuid(),
  job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  recipient_name text NOT NULL, recipient_email text NOT NULL,
  extra_metadata jsonb NOT NULL DEFAULT '{}',
  certificate_number text NOT NULL UNIQUE, status cert_status NOT NULL DEFAULT
  'PENDING', attempts smallint NOT NULL DEFAULT 0, error_message text NULL,
  file_path text NULL, created_at/updated_at timestamptz NOT NULL DEFAULT now(),
  generated_at timestamptz NULL)`
- Indexes: `certificates(job_id, status)` · `jobs(idempotency_key)` ·
  `jobs(status, created_at)`.

Counts are **never stored as columns** — always
`SELECT status, count(*) ... GROUP BY` per job. One source of truth, zero drift.

---

## 10. Idempotency (every part of the system)

| # | Operation | Mechanism | Guarantee on retry / duplicate |
|---|---|---|---|
| 1 | `POST /jobs` (with key) | UNIQUE `idempotency_key` + stored request hash + stored response snapshot | Replay → identical `202` + body + `Idempotency-Replayed: true`; same key *different* payload → `409` |
| 2 | `POST /jobs` (no key) | — (documented) | Client's choice; duplicate job expected |
| 3 | `dispatch_job` | enqueues only certs in `PENDING`; bounded batch | Re-run re-enqueues safely |
| 4 | `generate_pdf` claim | CAS `UPDATE ... WHERE status IN (PENDING,RETRYING)` | Concurrent duplicate → rowcount 0 → **no-op** |
| 5 | cert `→ DONE` | CAS from `PROCESSING`; tmp→`os.replace` to deterministic path | No double-count; identical bytes on rewrite |
| 6 | Job status recompute | pure function of cert rows + early exit for terminal jobs | Re-runs converge to the same result |
| 7 | Startup `reconcile` | CAS stale `PROCESSING/RETRYING→PENDING`, enqueue; grace window | Re-runs safe; absorbs #4's leftovers |
| 8 | `POST cancel` | CAS from `PENDING\|PROCESSING` | Repeat → `200` same state; racing completion → `409` |
| 9 | All `GET`s, download, zip, verify | read-only; deterministic paths | Naturally idempotent |

### 10.1 Retry mechanics (the state enforces the cap, not the middleware)

```mermaid
flowchart TD
    G["GUARD: CAS PENDING or RETRYING → PROCESSING<br/>attempts + 1<br/>rowcount 0 ⇒ no-op return"]
    R["render + store"]
    S["success ⇒ CAS → DONE"]
    T["RETRYABLE error:<br/>attempts >= max 4 ⇒ CAS → FAILED, return<br/>no re-raise, no wasted redelivery"]
    U["RETRYABLE, attempts below max:<br/>CAS → RETRYING, re-raise<br/>SmartRetryMiddleware redelivers w/ backoff"]
    V["PERMANENT error ⇒ CAS → FAILED + reason"]

    G --> R --> S
    R --> T
    R --> U
    R --> V
```

`SmartRetryMiddleware`'s own retry count is configured ≥ 4 as a second fence,
but the **DB `attempts` column is the authoritative cap**: even if the
middleware gives up first, the next claim still enforces the limit. Exhausted
certs land in `FAILED` (terminal) — reconcile never touches `FAILED`, so stale
`RETRYING→PENDING` recovery is bounded by the persisted counter. No infinite loop.

**Retryable taxonomy:** **transient** — `OSError` (disk full/permissions),
DB/Redis *connection* errors → retry. **Permanent** — everything else
(reportlab render failures, `ValueError` on bad data, unknown bugs): fail fast
and make the failure visible rather than retrying a deterministic bug.

### 10.2 Job recompute rules (explicit, complete)

```
if job.status in (COMPLETED, COMPLETED_WITH_ERRORS, FAILED, CANCELLED): exit
all certs DONE                                → COMPLETED
≥1 DONE AND ≥1 FAILED                         → COMPLETED_WITH_ERRORS
all FAILED (and total > 0)                    → FAILED
otherwise                                     → PROCESSING
completed_at set exactly once, on first terminal entry
```

After a soft cancel, in-flight certs finishing (`DONE`/`FAILED`) update their
own rows but recompute exits immediately — the job stays `CANCELLED`.

### 10.3 Unicode in the PDF

Vendor `DejaVuSans.ttf` + `DejaVuSans-Bold.ttf` into `assets/fonts/`
(OFL-licensed, freely redistributable), register with
`reportlab.pdfbase.ttfonts.TTFont`. Accented and CJK names render correctly;
Dockerfile copies `assets/`. Unit tests assert on accented (`Dhwaníl`) and CJK
names. reportlab's built-in Helvetica is Latin-1 only — this is why the font is
vendored rather than assumed.

### 10.4 Certificate numbers

`CERT-<year-of-job-created_at>-<8 random base32 chars>`; on unique-constraint
collision → regenerate (bounded at 5 attempts, then permanent failure). The
verify endpoint is keyed on this number, so the rule is part of the contract.

### 10.5 Idempotency replay

On job creation, store the response snapshot as `jobs.submission_response`
JSONB — a submit-time fact, immutable (separate from the derived-counts rule,
which governs live status only). Replay with the same key returns the original
`202` body verbatim plus an `Idempotency-Replayed: true` header; current state
is fetched via `GET /jobs/{id}`.

### 10.6 Reconcile staleness

A job is stale when `status = PENDING AND updated_at < now() - grace`;
a cert is stale when `status IN (PROCESSING, RETRYING) AND updated_at < now() - grace`.
Every job-row update (create, dispatch, recompute) bumps `updated_at`, so a
worker restart 30s after job creation does *not* false-positive.

---

## 11. Configuration (pydantic-settings)

`database_url` · `redis_url` · `storage_dir` (default `./storage`, gitignored) ·
`base_url` (verify links + QR) · `max_recipients_per_job=5000` ·
`max_generation_attempts=4` (1 try + 3 retries) · `retry_backoff_seconds=5` ·
`reconcile_grace_seconds=120` · `page_size_default=50` / max 200 · `log_level` ·
`app_name`, `environment`.

---

## 12. Testing strategy

- **Unit tests (no infra):** renderer produces valid PDF bytes containing the
  recipient name (incl. accented + CJK fixtures); row validation (per-row
  isolation, zero-valid case); `generate_pdf` guard/retry logic via Taskiq
  `InMemoryBroker` + `tmp_path` storage; job recompute as a pure function.
- **Integration tests (testcontainers → real Postgres + Redis):** the six
  required scenarios — create job, input validation, generation,
  status/progress, per-cert failure, retrieval — plus cancel, retry→`RETRYING`,
  reconcile after simulated crash/duplicate enqueue, idempotency-key replay,
  zip, verify, `/health`.
- Migrations are applied once per test session against the throwaway Postgres.

---

## 13. Deployment

`docker-compose.yaml`: `postgres:16`, `redis:7`, `api` (uvicorn, migrations on
boot), `worker` (`taskiq worker aereo_assignment.worker.broker:broker`).
Multi-stage `Dockerfile` built on `uv` (pin exact `taskiq`/`taskiq-redis`
versions; confirm CLI + middleware kwargs at build time); `assets/` copied for
fonts; storage dir mounted as a volume. README documents
`docker compose up --build` as the one-command path.

```mermaid
flowchart LR
    subgraph Compose["docker compose"]
        PG[("postgres:16")]
        RD[("redis:7")]
        API["api<br/>uvicorn + alembic on boot"]
        WK["worker<br/>taskiq worker ... broker"]
        VOL[("storage volume")]
    end

    API --> PG
    API --> RD
    WK --> PG
    WK --> RD
    API --> VOL
    WK --> VOL
```

---

## 14. Design rationale (the justification section)

| Decision | Alternative | Why |
|---|---|---|
| Hybrid processing | All-inline (O(N) enqueue) / all-deferred | 202 receipt needs counts+rows inline; queue decouples Redis availability; dispatch task is the seam |
| Hybrid validation | Strict 422 / blind accept | Max resilience *and* a real input contract; invalid rows stay visible as failures |
| Taskiq + Redis | ARQ / Celery+RabbitMQ | Async-native; DI for testable tasks; broker is a swappable config line (RabbitMQ via `taskiq-aio-pika`); Redis never holds state |
| Per-cert fan-out | One task processes whole job | Independent retry per certificate = structural failure isolation |
| Derived counts | Columns updated per cert | One source of truth, no drift, no lost-update races |
| CAS transitions everywhere | Optimistic app-level checks / constraints alone | Every mutation idempotent under duplicate delivery — one mechanism covers retries, crashes, reconciliation |
| Disk storage | Postgres bytea / S3-only | Simple, fast, swappable interface; S3 documented as the production target |
| reportlab + vendored TTF | WeasyPrint / Pillow | Pure-wheel install, programmatic control, Unicode-safe, easy to test and explain |

**Interview one-liner:** *"State lives in Postgres; the queue is disposable
dispatch. Taskiq keeps the broker a config swap — RabbitMQ is one line — and
DI plus InMemoryBroker make every task unit-testable with zero infra. Every
mutation is a CAS transition, so retries, crashes, and reconciliation all
converge through one mechanism."*

---

## 15. Left to the builder

The implementation plan itself (next step: writing-plans skill). Also: pin the
exact `taskiq`/`taskiq-redis` version and confirm CLI + middleware kwargs at
build time; finalize the PDF's visual polish; decide whether `.kiq` result
handling needs any logging hooks.
