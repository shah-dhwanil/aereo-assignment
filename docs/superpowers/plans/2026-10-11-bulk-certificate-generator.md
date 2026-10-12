# Bulk Certificate Generator — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the FastAPI + Taskiq + Postgres bulk certificate generator per the approved design spec: hybrid validation/processing, CAS-idempotent state machine, per-cert PDF generation with retry, reconcile-based crash recovery, and full retrieval (list / download / ZIP / verify).

**Architecture:** Single Python package (`src/aereo_assignment`). Routers → services → repositories → SQLAlchemy async. Postgres is the only state of record; Redis is disposable dispatch (taskiq `RedisStreamBroker` — chosen by decision, adds acks + XAUTOCLAIM redelivery on top of the spec's reconcile story). Every mutation is a CAS transition returning a rowcount; rowcount 0 = no-op. Counts are always `GROUP BY`, never stored.

**Tech Stack:** Python 3.14 · FastAPI 0.143 · uvicorn · Postgres 16 + SQLAlchemy 2.0 async (asyncpg) · Alembic · Taskiq 0.13.0 + taskiq-redis 1.2.4 (`RedisStreamBroker`, `ListRedisScheduleSource`, `SmartRetryMiddleware`, `LabelScheduleSource`) · reportlab 5.0 · segno · pydantic-settings · testcontainers · pytest-asyncio · Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-11-bulk-certificate-generator-design.md` (v1.2) — the plan argues from the spec; executors read both. Design-review doc: `docs/superpowers/specs/2026-10-11-bulk-certificate-generator-design-review.md`.

## Global Constraints

Every task's requirements implicitly include these (exact values from the spec):

- Python 3.14; package at `src/aereo_assignment`; deps pinned in `pyproject.toml` (`taskiq==0.13.0`, `taskiq-redis==1.2.4` exact; others compatible-release).
- Statuses (exact strings): `JobStatus = PENDING|PROCESSING|COMPLETED|COMPLETED_WITH_ERRORS|FAILED|CANCELLED`; `CertificateStatus = PENDING|PROCESSING|RETRYING|DONE|FAILED|CANCELLED`.
- Error codes (exact strings): `VALIDATION_ERROR`, `IDEMPOTENCY_KEY_REUSED`, `JOB_NOT_FOUND`, `JOB_ALREADY_FINAL`, `CERT_NOT_FOUND`, `CERT_NOT_READY`, `FILE_MISSING`, `NO_CERTIFICATES_READY`, `CERTIFICATE_NOT_FOUND`, `INTERNAL_ERROR`. Envelope: `{"error": {"code", "message", "details": []}}`.
- Caps: recipients 1–5000; `name` 1–200 chars after strip; `event_name`/`issuer` 1–200; `description` ≤1000; `extra_metadata` ≤10 keys, values str/int/bool ≤200 chars; page size default 50, max 200.
- Certificate numbers: `CERT-<year>-<8 chars base32 uppercase A–Z2–7>`; collision → regenerate, bounded at 5 attempts, then permanent failure.
- **Decision (user):** counts include a `processing` key — `{"total", "done", "failed", "pending", "processing", "retrying", "cancelled"}` (`pending` = PENDING only). Documented deviation from §8; README must state it.
- **Decision (user):** broker is `RedisStreamBroker` (acks; XAUTOCLAIM redelivery after `idle_timeout`), not the spec diagram's BRPOP list queue. README must state the deviation and that Redis still holds no truth.
- Config invariant (asserted at startup): `retry_backoff_seconds × max_generation_attempts < reconcile_grace_seconds`. All staleness compares use DB `now()` (`func.now()`), never app time.
- Post-commit rule: after the submit transaction commits, ALWAYS return 202; enqueue failure is logged, never a 500 (M1).
- Terminal job states are never re-entered (recompute is an in-UPDATE CAS, H2). DONE certs are never demoted (X2). Every cert outcome transition requires `status = PROCESSING AND claim_token = :mine` (C3). Claim requires `status IN (PENDING, RETRYING) AND attempts < max` (H3).
- `Idempotency-Key` is an optional header stored as `text`; replay returns the stored `submission_response` snapshot verbatim + `Idempotency-Replayed: true`; same key different payload → `409 IDEMPOTENCY_KEY_REUSED` (M2 race branch: IntegrityError → SELECT → compare → replay/409, never 500).
- Fonts are vendored (committed) at `assets/fonts/` with license; renderer must handle accented + CJK names.
- MVP cut-line (from design review): tasks 1–9 + 12 deliver the six required scenarios; cancel/ZIP/verify/reconcile hardening follow without touching the core.

## Review Focus

The five input classes most likely to bite a user of this software, and the test that pins each:

1. **Concurrent same-key submits** (M2) — expected: one 202, the loser replays the same snapshot with the replay header or gets 409, never 5xx → Task 8 `test_concurrent_same_key_one_202`.
2. **Re-claim split-brain** (C3) — expected: a stalled worker whose cert was reset and re-claimed cannot clobber the new owner's row → Task 9 `test_split_brain_stale_owner_cannot_clobber`.
3. **Worker crash between final DONE and recompute** (C2) — expected: the stale PROCESSING job converges to terminal via reconcile, no restart needed → Task 11 `test_stale_processing_job_recomputed_after_crash_window`.
4. **Broker redelivery racing DB claim** — expected: an XAUTOCLAIMed stream message and a reconcile reset both funnel through the claim CAS; duplicates are no-ops, attempts stay capped → Task 9 `test_duplicate_delivery_is_absorbed` + Task 1 `test_settings_invariant_rejected` (config keeps backoff×attempts < grace so the two recovery clocks never fight).
5. **DONE-but-file-missing on retrieval** (X2/M4) — expected: download → `409 FILE_MISSING` with the row staying DONE; ZIP → pre-flight skip + `SKIPPED.txt` manifest, still 200 → Task 12 `test_download_file_missing_409_row_stays_done`, `test_zip_skips_missing_and_writes_manifest`.

Also covered: cancel-vs-recompute race (H2) → Task 10 `test_recompute_after_cancel_is_dropped`.

---

## Task 1: Scaffolding, Settings, Logging

**Files:**
- Modify: `pyproject.toml` (replace placeholder deps/script; add pytest config)
- Create: `src/aereo_assignment/config.py`
- Create: `src/aereo_assignment/logging.py`
- Create: `tests/unit/test_config.py`
- Create: `tests/conftest.py` (imports only — real fixtures land in Task 2)

**Interfaces:**
- Produces: `Settings` (pydantic-settings) with fields `database_url: str`, `redis_url: str`, `storage_dir: Path = Path("./storage")`, `base_url: str = "http://localhost:8000"`, `max_recipients_per_job: int = 5000`, `max_generation_attempts: int = 4`, `retry_backoff_seconds: int = 5`, `reconcile_grace_seconds: int = 120`, `reconcile_interval_seconds: int = 45`, `page_size_default: int = 50`, `page_size_max: int = 200`, `stream_maxlen: int = 10_000`, `log_level: str = "INFO"`, `app_name: str = "bulk-certificate-generator"`, `environment: str = "development"`; module-level `get_settings()` (cached); `setup_logging(settings) -> None`.
- Env prefix: `BCG_`, `.env` file supported.
- Dependencies to add: `fastapi>=0.115,<1`, `uvicorn[standard]>=0.30,<1`, `sqlalchemy[asyncio]>=2.0.30,<3`, `asyncpg>=0.30,<1`, `alembic>=1.14,<2`, `pydantic-settings>=2.6,<3`, `taskiq==0.13.0`, `taskiq-redis==1.2.4`, `reportlab>=4.2,<6`, `segno>=1.6,<2`, `redis>=5,<7`. Dev group: `pytest`, `pytest-asyncio>=0.25`, `httpx>=0.27`, `testcontainers[postgres,redis]>=4.9`, `pypdf>=5`, `ruff`.
- Remove the placeholder `[project.scripts] aereo-assignment = "aereo_assignment:main"`. Set pytest `asyncio_mode = "auto"`.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_config.py`:

```python
from aereo_assignment.config import Settings


def test_settings_defaults_satisfy_invariant():
    s = Settings(database_url="postgresql+asyncpg://x", redis_url="redis://x")
    assert s.retry_backoff_seconds * s.max_generation_attempts < s.reconcile_grace_seconds


def test_settings_invariant_rejected():
    import pytest
    with pytest.raises(ValueError, match="reconcile_grace_seconds"):
        Settings(database_url="postgresql+asyncpg://x", redis_url="redis://x",
                 retry_backoff_seconds=100, max_generation_attempts=4,
                 reconcile_grace_seconds=120)


def test_env_override(monkeypatch):
    monkeypatch.setenv("BCG_PAGE_SIZE_DEFAULT", "25")
    s = Settings(database_url="postgresql+asyncpg://x", redis_url="redis://x")
    assert s.page_size_default == 25
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_config.py -v` — Expected: FAIL (imports/`ValueError` missing).

- [ ] **Step 3: Implement `config.py` + `logging.py`**

`Settings` with `SettingsConfigDict(env_prefix="BCG_", env_file=".env")` and a `model_validator(mode="after")` raising `ValueError("retry_backoff_seconds × max_generation_attempts must be < reconcile_grace_seconds")` when the invariant fails. `get_settings()` wraps it in `functools.lru_cache`. `setup_logging` configures stdlib logging with `settings.log_level` and UTC timestamps.

- [ ] **Step 4: Run tests to verify they pass** — Run: `uv run pytest tests/unit/test_config.py -v` — Expected: 3 passed.

- [ ] **Step 5: Commit** — `git add pyproject.toml uv.lock src tests && git commit -m "feat: project scaffolding, settings with backoff invariant, logging"`

---

## Task 2: Models, Session, Migration

**Files:**
- Create: `src/aereo_assignment/db/base.py`, `db/models.py`, `db/session.py`
- Create: `alembic.ini`, `alembic/env.py`, `alembic/versions/0001_initial.py`
- Modify: `tests/conftest.py`
- Create: `tests/integration/test_db.py`

**Interfaces:**
- Produces: `JobStatus` / `CertificateStatus` (`enum.StrEnum`, values as above, mapped with `sqlalchemy.Enum(..., values_callable=lambda es: [e.value for e in es])` for native PG enums); ORM `Job` and `Certificate` exactly per spec §9 DDL (ids assigned python-side `uuid.uuid4()`; `jobs.submission_response: Mapped[dict | None]` JSONB; `certificates.extra_metadata: Mapped[dict]` JSONB default `{}`); indexes `certificates(job_id, status)`, `jobs(idempotency_key)`, `jobs(status, created_at)`; FK `ON DELETE CASCADE`.
- `db/session.py`: `create_engine_from(settings)` → `AsyncEngine`; `create_session_factory(engine)` → `async_sessionmaker[AsyncSession, AsyncSession]` with `expire_on_commit=False`; `get_session()` async-generator FastAPI dependency reading `request.app.state.session_factory` — no, simpler: module-level `session_factory` set by `main.py` lifespan; `get_session()` yields from it.
- Test fixtures (session-scoped): `pg_container` (`PostgresContainer("postgres:16")`), `engine`, `session_factory`, `migrate` (runs `alembic.command.upgrade(cfg, "head")` **once per session** against the container URL). Function-scoped: `session`.

- [ ] **Step 1: Write the failing integration test**

`tests/integration/test_db.py`:

```python
import uuid
from datetime import date

from sqlalchemy import inspect, text

from aereo_assignment.db.models import Certificate, Job, CertificateStatus, JobStatus


def test_migrations_create_tables_and_indexes(engine):
    insp = inspect(engine.sync_engine)
    assert {"jobs", "certificates"} <= set(insp.get_table_names())
    idx = {i["name"] for i in insp.get_indexes("certificates")}
    assert "ix_certificates_job_id_status" in idx


async def test_job_and_certificate_roundtrip(session_factory):
    async with session_factory() as s:
        job = Job(id=uuid.uuid4(), event_name="E", event_date=date(2026, 3, 15), issuer="I")
        s.add(job)
        cert = Certificate(id=uuid.uuid4(), job_id=job.id, recipient_name="Dhwaníl",
                           recipient_email="d@x.com", certificate_number="CERT-2026-AAAA2222")
        s.add(cert)
        await s.commit()
        rows = (await s.execute(text("SELECT status FROM jobs"))).scalar_one()
        assert rows == JobStatus.PENDING.value
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/integration/test_db.py -v` — Expected: FAIL (no tables).

- [ ] **Step 3: Implement models, session, and `0001_initial`**

Migration DDL verbatim from spec §9 (uuid PKs, `job_status`/`cert_status` native enums with defaults, timestamptz `now()` defaults, the three indexes). `alembic/env.py` reads `settings.database_url` with `render_as_batch=False`, `target_metadata=Base.metadata`.

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/integration/test_db.py -v` — Expected: 2 passed (testcontainers pulls postgres:16 on first run).

- [ ] **Step 5: Commit** — `git add src/aereo_assignment/db alembic* tests && git commit -m "feat: ORM models, session factory, initial migration"`

---

## Task 3: Pure Domain — Validation, Numbering, Recompute Decision, Error Taxonomy, Canonical Hash

**Files:**
- Create: `src/aereo_assignment/services/validation.py`
- Create: `src/aereo_assignment/services/numbering.py`
- Create: `src/aereo_assignment/services/recompute.py` (decision function only in this task)
- Create: `src/aereo_assignment/services/errors.py` (`classify_error`)
- Create: `src/aereo_assignment/services/idempotency.py` (canonical hash)
- Create: `tests/unit/test_validation.py`, `test_numbering.py`, `test_recompute_decision.py`, `test_classify_error.py`, `test_canonical_hash.py`

**Interfaces:**
- Consumes: `JobStatus`/`CertificateStatus` (Task 2).
- Produces:
  - `ValidRecipient` (dataclass: `name: str`, `email: str`, `extra_metadata: dict[str, str | int | bool]`); `RowError` (dataclass: `index: int`, `field: str`, `message: str`); `validate_recipients(rows: list[RecipientIn]) -> tuple[list[ValidRecipient], list[RowError]]` — returns `(valid, errors)` preserving original indices; a valid row's error is absent from errors and vice versa.
  - `generate_certificate_number(*, year: int) -> str`; `generate_unique_numbers(*, year: int, count: int) -> list[str]` (in-batch dedupe via a set).
  - `decide_job_status(counts: Mapping[CertificateStatus, int]) -> JobStatus` — pure, per spec §10.2 truth table; empty/all-zero → `PROCESSING`.
  - `classify_error(exc: BaseException) -> Literal["transient", "permanent"]` — transient: `OSError`, `ConnectionError`, `asyncio.TimeoutError`, `sqlalchemy.exc.OperationalError`, `sqlalchemy.exc.InterfaceError`; permanent: everything else.
  - `canonical_request_hash(payload: Mapping[str, Any]) -> str` — `sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode())`.

- [ ] **Step 1: Write the failing unit tests**

```python
# test_validation.py (key cases)
def test_valid_rows_pass():            # 2 good rows -> 2 valid, 0 errors
def test_invalid_email_row_isolated(): # good, "not-an-email", good -> 2 valid, errors==[RowError(1, "email", ...)]
def test_name_stripped_and_bounded():  # "   " -> invalid (name); 201 chars -> invalid
def test_extra_metadata_rules():       # 11 keys -> invalid; 201-char value -> invalid; 3.14 float value -> invalid
def test_zero_valid_returns_all_errors()

# test_numbering.py
def test_number_format():        # re.match(r"^CERT-2026-[A-Z2-7]{8}$", generate_certificate_number(year=2026))
def test_batch_unique():         # len(set(generate_unique_numbers(year=2026, count=5000))) == 5000

# test_recompute_decision.py — parametrized truth table
# all DONE -> COMPLETED; 2 DONE 1 FAILED -> COMPLETED_WITH_ERRORS; all FAILED -> FAILED;
# 1 DONE 1 PENDING -> PROCESSING; any RETRYING -> PROCESSING; {} -> PROCESSING

# test_classify_error.py
# OSError -> transient; ConnectionError -> transient; sqlalchemy.exc.OperationalError(...) -> transient
# ValueError -> permanent; reportlab-level generic Exception -> permanent

# test_canonical_hash.py
# {"a":1,"b":2} == {"b":2,"a":1}; extra whitespace/irrelevant; {"a":1} != {"a":2}
```

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/unit -v` — Expected: collection errors.

- [ ] **Step 3: Implement the five modules** — signatures only above; each body is a straight loop/case statement. Email rule: single regex (simple, explainable) — no `email-validator` dependency.

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/unit -v`.

- [ ] **Step 5: Commit** — `git commit -am "feat: pure domain — row validation, cert numbering, recompute decision, error taxonomy"`

---

## Task 4: Storage Service

**Files:**
- Create: `src/aereo_assignment/services/storage.py`
- Create: `tests/unit/test_storage.py`

**Interfaces:**
- Consumes: `Settings.storage_dir` (Task 1).
- Produces: `Storage` (`typing.Protocol`): `path_for(job_id: UUID, certificate_number: str) -> str` (returns `"{job_id}/{certificate_number}.pdf"`, posix-style), `async write(key: str, data: bytes) -> None`, `async read(key: str) -> bytes`, `async exists(key: str) -> bool`. `DiskStorage(root: Path)` implementation. `write` runs in `asyncio.to_thread`, writes to `.{name}.{uuid4().hex}.tmp` sibling then `os.replace`, and best-effort-unlinks the tmp on any exception before re-raising.

- [ ] **Step 1: Write failing tests** — `test_write_then_read_roundtrip(tmp_path)`; `test_write_is_atomic_overwrite` (write v1, write v2, read == v2); `test_exists_false_for_missing`; `test_write_cleans_tmp_on_failure` (monkeypatch `os.replace` to raise `OSError`; assert no `*.tmp` remains and the error propagates).
- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/unit/test_storage.py -v`.
- [ ] **Step 3: Implement `DiskStorage`.**
- [ ] **Step 4: Run to verify pass.**
- [ ] **Step 5: Commit** — `git commit -am "feat: storage protocol + atomic disk implementation"`.

---

## Task 5: PDF Renderer, Vendored Fonts, QR

**Files:**
- Create: `assets/fonts/DejaVuSans.ttf`, `assets/fonts/DejaVuSans-Bold.ttf`, `assets/fonts/LICENSE.txt`
- Create: `src/aereo_assignment/pdf/__init__.py`, `pdf/renderer.py`
- Create: `tests/unit/test_renderer.py`

**Interfaces:**
- Produces: `render_certificate(*, recipient_name: str, recipient_email: str, event_name: str, event_date: date, issuer: str, description: str | None, certificate_number: str, verify_url: str, extra_metadata: dict | None) -> bytes` — pure, sync, returns PDF bytes. Registers `DejaVuSans`/`DejaVuSans-Bold` via `TTFont` once at module import (idempotent). Layout: A4 landscape, bordered; issuer as title; recipient name large (Bold); event name + date; description; certificate number bottom-left; QR (`segno.make(verify_url)` → PNG via segno's built-in writer into `BytesIO` → `reportlab.lib.utils.ImageReader`) bottom-right.
- `extra_metadata` is stored/returned by the API but **not** rendered (README decision note).

- [ ] **Step 1: Download fonts (commit them)**

```bash
mkdir -p assets/fonts
curl -fsSL -o assets/fonts/DejaVuSans.ttf https://raw.githubusercontent.com/dejavu-fonts/dejavu-fonts/master/fonts/ttf/DejaVuSans.ttf
curl -fsSL -o assets/fonts/DejaVuSans-Bold.ttf https://raw.githubusercontent.com/dejavu-fonts/dejavu-fonts/master/fonts/ttf/DejaVuSans-Bold.ttf
curl -fsSL -o assets/fonts/LICENSE.txt https://raw.githubusercontent.com/dejavu-fonts/dejavu-fonts/master/LICENSE
```

- [ ] **Step 2: Write failing tests**

```python
def test_pdf_magic_bytes():            # render_certificate(...) starts with b"%PDF"
def test_recipient_name_in_text():     # pypdf extract_text contains "Dhwaníl Shah" (accented fixture)
def test_cjk_name_in_text():           # contains "山田太郎"
def test_cert_number_in_text():        # contains "CERT-2026-AAAA2222"
def test_qr_image_present():           # b"/Image" in pdf_bytes.replace(b" ", b"")
```

- [ ] **Step 3: Run to verify failure** — `uv run pytest tests/unit/test_renderer.py -v`.
- [ ] **Step 4: Implement `renderer.py`** as per interface.
- [ ] **Step 5: Run to verify pass.**
- [ ] **Step 6: Commit** — `git add assets src tests && git commit -m "feat: reportlab renderer with vendored DejaVu fonts and verification QR"`.

---

## Task 6: Repositories — Every CAS Transition

**Files:**
- Create: `src/aereo_assignment/repositories/__init__.py`, `jobs.py`, `certificates.py`
- Create: `tests/integration/test_repositories.py`

**Interfaces:**
- Consumes: models + `session_factory` (Task 2).
- Produces (constructor `JobsRepository(session: AsyncSession)` / `CertificatesRepository(session: AsyncSession)`; all mutating methods return affected rowcount):

```python
class JobsRepository:
    async def insert(self, job: Job) -> Job                      # flush; IntegrityError propagates to caller
    async def get(self, job_id: UUID) -> Job | None
    async def get_by_idempotency_key(self, key: str) -> Job | None
    async def list(self, *, status: JobStatus | None, offset: int, limit: int) -> Sequence[Job]
    async def count(self, *, status: JobStatus | None) -> int
    async def cas_transition(self, job_id: UUID, *, from_statuses: Sequence[JobStatus],
                             to_status: JobStatus, set_completed_at: bool) -> int
    async def recompute_cas(self, job_id: UUID, decision: JobStatus) -> int   # spec §10.2 SQL verbatim
    async def find_stale(self, *, statuses: Sequence[JobStatus], grace_seconds: int) -> Sequence[Job]
        # WHERE status IN :statuses AND updated_at < func.now() - func.make_interval(secs=:grace)

class CertificatesRepository:
    async def insert_many(self, certs: Sequence[Certificate]) -> None
    async def get(self, cert_id: UUID) -> Certificate | None
    async def get_by_number(self, number: str) -> Certificate | None
    async def list_for_job(self, job_id: UUID, *, status: CertificateStatus | None,
                           offset: int, limit: int) -> Sequence[Certificate]
    async def status_counts(self, job_ids: Sequence[UUID]) -> dict[UUID, dict[CertificateStatus, int]]
    async def pending_ids(self, job_id: UUID) -> Sequence[UUID]
    async def claim(self, cert_id: UUID, *, max_attempts: int, token: UUID) -> Certificate | None
        # UPDATE SET status=PROCESSING, claim_token=:token, attempts=attempts+1, updated_at=func.now()
        #   WHERE id=:id AND status IN ('PENDING','RETRYING') AND attempts < :max RETURNING *
    async def cas_outcome(self, cert_id: UUID, *, token: UUID, to_status: CertificateStatus,
                          error_message: str | None = None, file_path: str | None = None,
                          generated_at: datetime | None = None) -> int
        # WHERE id=:id AND status='PROCESSING' AND claim_token=:token
    async def cancel_open(self, job_id: UUID) -> int                 # PENDING|RETRYING -> CANCELLED
    async def find_stale(self, *, statuses: Sequence[CertificateStatus], grace_seconds: int) -> Sequence[Certificate]
    async def reset_to_pending(self, ids: Sequence[UUID]) -> int     # -> PENDING, claim_token=NULL (never checks token)
    async def force_fail_stale(self, cert_id: UUID, message: str) -> int
        # WHERE status IN ('PROCESSING','RETRYING') AND attempts >= :max -> FAILED (H3 sweep)
    async def find_stale_pending(self, *, grace_seconds: int) -> Sequence[UUID]
```

- [ ] **Step 1: Write failing integration tests** (`session_factory` fixtures from Task 2):

```python
async def test_claim_wins_once(session_factory)          # two claims, second returns None; first has attempts==1 and a token
async def test_claim_respects_attempts_cap()             # attempts set to max -> claim returns None
async def test_claim_from_retrying()                     # RETRYING row claims fine
async def test_cas_outcome_requires_matching_token()     # wrong token -> rowcount 0; right token -> 1 and status set
async def test_recompute_cas_dropped_when_terminal()     # job CANCELLED -> recompute_cas rowcount 0, status stays CANCELLED
async def test_cancel_open_skips_processing_and_done()
async def test_force_fail_stale_only_at_or_above_cap()
async def test_reset_to_pending_clears_token()
async def test_find_stale_uses_db_clock()                # old updated_at row found; fresh row not found
async def test_status_counts_group_by()                  # 3 jobs mixed statuses -> per-job dict exact
async def test_unique_certificate_number_violation_raises()  # sqlalchemy.exc.IntegrityError
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement both repositories** — Core `update()`/`select()` statements; `RETURNING *` on claim; `status_counts` is `SELECT job_id, status, count(*) ... WHERE job_id = ANY(:ids) GROUP BY 1, 2`.
- [ ] **Step 4: Run to verify pass.**
- [ ] **Step 5: Commit** — `git commit -am "feat: job and certificate repositories with CAS transitions"`.

---

## Task 7: Broker Wiring, DI, `dispatch_job`

**Files:**
- Create: `src/aereo_assignment/worker/__init__.py`, `worker/broker.py`, `worker/deps.py`
- Create: `src/aereo_assignment/worker/tasks/__init__.py` (imports all three task modules for CLI discovery)
- Create: `src/aereo_assignment/worker/tasks/dispatch.py`
- Create: `tests/unit/test_dispatch.py`

**Interfaces:**
- Consumes: Task 1 settings, Task 6 repos.
- Produces:
  - `worker/broker.py`:
    ```python
    schedule_source = ListRedisScheduleSource(settings.redis_url)
    broker = RedisStreamBroker(
        url=settings.redis_url,
        consumer_group_name="bulk-certificate-generator",
        idle_timeout=settings.reconcile_grace_seconds * 1000,   # ms; XAUTOCLAIM redelivers unacked after grace
        unacknowledged_lock_timeout=settings.reconcile_grace_seconds,
        maxlen=settings.stream_maxlen,
    ).with_middlewares(SmartRetryMiddleware(
        default_retry_count=settings.max_generation_attempts,
        default_delay=settings.retry_backoff_seconds,
        use_jitter=True, use_delay_exponent=True,
        schedule_source=schedule_source,
    ))
    scheduler = TaskiqScheduler(broker=broker, sources=[schedule_source, LabelScheduleSource(broker)])
    ```
  - `worker/deps.py`: frozen dataclasses `DispatchDeps{session_factory, broker}`, `GenerateDeps{session_factory, storage, settings, broker}`, `ReconcileDeps{session_factory, broker, settings}`; provider functions `get_dispatch_deps()` / `get_generate_deps()` / `get_reconcile_deps()` (plain sync functions usable as TaskiqDepends defaults) reading module-level state that tests can monkeypatch.
  - `dispatch.py`:
    ```python
    @broker.task(name="dispatch_job", retry_on_error=True, max_retries=5)
    async def dispatch_job(job_id: str, deps: DispatchDeps = TaskiqDepends(get_dispatch_deps)) -> dict[str, int]

    async def dispatch_job_core(job_id: UUID, deps: DispatchDeps) -> dict[str, int]
    # 1. CAS job PENDING->PROCESSING via cas_transition; rowcount IGNORED (fan-out runs even on miss, X1)
    # 2. ids = pending_ids(job_id)   # single pass, spec M3
    # 3. for each: await generate_pdf.kicker().with_broker(deps.broker).kiq(str(id))
    # 4. return {"enqueued": len(ids)}
    ```
    Task payloads are strings (taskiq's JSON serializer); cores parse `UUID(...)`.
  - `tests/conftest.py` addition reused by later tasks:
    ```python
    class RecordingBroker(InMemoryBroker):
        """Records kicks without executing them; later tests kick for real via .kicker().with_broker(...)."""
        def __init__(self) -> None:
            super().__init__()
            self.kicked: list[BrokerMessage] = []
        async def kick(self, message: BrokerMessage) -> None:
            self.kicked.append(message)
    ```

- [ ] **Step 1: Write failing unit tests** (`tests/unit/test_dispatch.py`, monkeypatched deps, recorder fake for `generate_pdf`):

```python
async def test_dispatch_enqueues_only_pending_certificates()  # 2 PENDING + 1 FAILED -> exactly 2 kiq calls with those ids
async def test_dispatch_survives_job_cas_miss()               # job already PROCESSING -> no exception, still enqueues
async def test_dispatch_rerun_is_idempotent()                 # second run re-enqueues; claim CAS absorbs (asserted in Task 9)
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement broker.py, deps.py, dispatch.py.** Verify import-time wiring: `uv run python -c "from aereo_assignment.worker.broker import broker, scheduler"` succeeds without Redis running (lazy connect).
- [ ] **Step 4: Run to verify pass.**
- [ ] **Step 5: Commit** — `git commit -am "feat: RedisStreamBroker wiring with smart retry and dispatch_job task"`.

---

## Task 8: Submit Path — Schemas, Error Envelope, Idempotency, `POST /jobs`

**Files:**
- Create: `src/aereo_assignment/schemas/common.py`, `schemas/jobs.py`
- Create: `src/aereo_assignment/api/errors.py`
- Create: `src/aereo_assignment/api/deps.py` (pagination helper)
- Create: `src/aereo_assignment/api/routes/__init__.py`, `api/routes/jobs.py` (POST only in this task)
- Create: `src/aereo_assignment/services/job_service.py`
- Create: `src/aereo_assignment/main.py`
- Create: `tests/integration/test_submit.py`

**Interfaces:**
- Consumes: Task 3 (`validate_recipients`, `generate_unique_numbers`, `canonical_request_hash`), Task 6 repos, Task 1 settings, Task 7 (`dispatch_job` task + broker wiring; `RecordingBroker` fixture).
- Produces:
  - Schemas: `JobCreateRequest` (`event_name` 1–200 stripped, `event_date` `YYYY-MM-DD`, `issuer` 1–200 stripped, `description` ≤1000 optional, `recipients` list[1..5000] of `RecipientIn{name: str, email: str, extra_metadata: dict = {}}` — envelope-level pydantic only; all per-row rules live in `validate_recipients`); `JobAcceptedResponse {job_id, status, total, valid, invalid, created_at}`; `Page[T]{items, page, size, total_items, total_pages}`; `Counts{total, done, failed, pending, processing, retrying, cancelled}` (Global Constraint — user decision); `ErrorEnvelope{error: {code, message, details}}`.
  - `ApiError(Exception)` base with `code/message/status_code/details`; subclasses: `ZeroValidRows(422, "VALIDATION_ERROR")`, `IdempotencyKeyReused(409)`, `JobNotFound(404, "JOB_NOT_FOUND")`, `JobAlreadyFinal(409)`, `CertNotFound(404, "CERT_NOT_FOUND")`, `CertNotReady(409)`, `FileMissing(409)`, `NoCertificatesReady(409)`, `CertificateNotFound(404, "CERTIFICATE_NOT_FOUND")`. Registered handlers: `ApiError` → envelope; `RequestValidationError` → 422 envelope, `details` carries field paths; catch-all `Exception` → 500 `INTERNAL_ERROR`.
  - `async def submit_job(session, *, payload: JobCreateRequest, idempotency_key: str | None, broker, settings) -> SubmitResult` (`SubmitResult{job, total, valid, invalid, replayed: bool}`).
  - `main.py`: `create_app()` + lifespan (build engine/factory, `setup_logging`, `storage_dir.mkdir`, dispose engine on shutdown); mounts routers under `/api/v1` + `/health` placeholder.
  - `api/deps.py`: `pagination(status: JobStatus | None = None, page: int = Query(1, ge=1), size: int = Query(settings.page_size_default, ge=1, le=settings.page_size_max))` returning a `PageParams` dataclass.

- [ ] **Step 1: Write failing integration tests** (`app` fixture: `create_app()` + `httpx.AsyncClient(transport=ASGITransport(app), lifespan="on")`; a `RecordingBroker` (Task 7 fixture) set via `app.state.broker` — recording, NOT plain `InMemoryBroker`, which would execute dispatch+generation inline and mutate rows mid-request):

```python
async def test_submit_returns_202_with_counts()          # 3 good -> 202 {total:3, valid:3, invalid:0, status:"PENDING"}
async def test_mixed_validity_invalid_row_becomes_failed_cert()
    # bad email at index 1 -> 202 valid:2 invalid:1; cert row exists status FAILED, attempts 0,
    # error_message mentions the field; exactly 1 kick (dispatch_job), invalid row's id in no kick payload
async def test_zero_valid_422_with_row_details()         # all rows bad -> 422 code VALIDATION_ERROR, details carry index/field/message, nothing persisted (job count 0)
async def test_schema_error_uses_envelope()              # missing event_name -> 422 envelope with field path
async def test_replay_same_key_returns_snapshot_plus_header()
    # same key+payload twice -> second body == first verbatim, header Idempotency-Replayed: true, same job_id
async def test_same_key_different_payload_409()          # code IDEMPOTENCY_KEY_REUSED
async def test_concurrent_same_key_one_202()
    # asyncio.gather two identical submits with one key -> both succeed (202), same job_id, exactly one
    # carries Idempotency-Replayed; assert no 5xx
async def test_no_key_creates_duplicate_jobs()
async def test_dispatch_enqueue_failure_still_202()      # app.state.broker.kick raises -> 202 anyway (M1)
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement in order** — schemas → errors + handlers → `submit_job`:

```
1. rows, row_errors = validate_recipients(payload.recipients)
2. if not rows: raise ZeroValidRows(details=[{index, field, message}...])
3. if idempotency_key: existing = repo.get_by_idempotency_key(key)
   -> compare canonical_request_hash(payload) to existing.request_hash
   -> equal: return SubmitResult(replayed=True, snapshot=existing.submission_response)
   -> differ: raise IdempotencyKeyReused
4. snapshot = JobAcceptedResponse-shaped dict (job_id pre-generated uuid4)
5. begin TX: insert job (PENDING, request_hash, submission_response=snapshot, idempotency_key)
   on IntegrityError (unique key race, M2): rollback -> SELECT existing -> step 3's compare branch
6. numbers = generate_unique_numbers(year=job.created_at.year, count=len(recipients))
   batch-insert cert rows (valid -> PENDING; invalid -> FAILED with error_message = row reason)
   number-collision IntegrityError: rollback to SAVEPOINT, regenerate numbers, retry <=5, then re-raise
7. commit
8. best-effort: await dispatch_job.kicker().with_broker(broker).kiq(str(job.id)); except Exception -> logger.warning (M1)
9. return SubmitResult
```

`POST /jobs` handler: read optional `Idempotency-Key` header → `submit_job` → 202 body (+ replay header when `replayed`).

- [ ] **Step 4: Run to verify pass.**
- [ ] **Step 5: Commit** — `git commit -am "feat: submit endpoint with hybrid validation and idempotency replay"`.

---

## Task 9: `generate_pdf` — Claim, Render, Store, Retry Taxonomy

**Files:**
- Create: `src/aereo_assignment/worker/tasks/generate.py`
- Create: `tests/integration/test_generate.py`

**Interfaces:**
- Consumes: repos (Task 6), `render_certificate` (Task 5), `DiskStorage` (Task 4), `classify_error` (Task 3), `GenerateDeps`/`get_generate_deps` (Task 7), `recompute_job` — **note:** `recompute_job(session, job_id) -> JobStatus | None` is implemented here as a minimal private step and replaced by Task 10's shared version (same signature; Task 10 supersedes the file location `services/recompute.py`).
- Produces:
  ```python
  @broker.task(name="generate_pdf", retry_on_error=True, max_retries=settings.max_generation_attempts,
               delay=settings.retry_backoff_seconds)
  async def generate_pdf(cert_id: str, deps: GenerateDeps = TaskiqDepends(get_generate_deps)) -> str

  async def generate_pdf_core(cert_id: UUID, deps: GenerateDeps) -> str
  ```
  Core flow:
  ```
  1. cert = claim(cert_id, max_attempts=settings.max_generation_attempts, token=uuid4())
     None -> return "skipped:unclaimed"          # duplicate delivery / exhausted
  2. job = get(cert.job_id); verify_url = f"{settings.base_url}/api/v1/verify/{cert.certificate_number}"
  3. try:  pdf = await asyncio.to_thread(render_certificate, ...)   # all fields from cert+job
           key = storage.path_for(job.id, cert.certificate_number)
           await storage.write(key, pdf)
     except Exception as exc:
           kind = classify_error(exc)
           if kind == "permanent": cas_outcome(FAILED, error_message=str(exc)); recompute_job(); return "failed:permanent"
           if cert.attempts >= settings.max_generation_attempts: cas_outcome(FAILED, ...); recompute_job(); return "failed:exhausted"
           cas_outcome(RETRYING, error_message=str(exc)); raise   # SmartRetryMiddleware redelivers w/ backoff; DB cap authoritative (§10.1)
  4. cas_outcome(DONE, file_path=key, generated_at=func.now()); rowcount 0 -> return "skipped:stale" (lost ownership, C3)
  5. recompute_job(session, job_id); return "done"
  ```

- [ ] **Step 1: Write failing integration tests** (real PG via testcontainers, `DiskStorage(tmp_path)`, deps built directly — no broker needed except the wiring test):

```python
async def test_generate_happy_path_marks_done_and_writes_file()
    # status DONE, file_path set, file exists and starts %PDF, generated_at set, job -> COMPLETED
async def test_permanent_error_fails_fast()
    # monkeypatch renderer to raise ValueError -> FAILED + error_message; re-invocation -> "skipped:unclaimed";
    # single-cert job recompute -> FAILED
async def test_transient_error_moves_to_retrying_and_redelivers()
    # monkeypatch storage.write to raise OSError -> RETRYING attempts=1; invoke again -> attempts=2
async def test_attempts_cap_exhaustion_fails()
    # loop max_generation_attempts invocations -> FAILED, attempts == max, no exception raised out of core
async def test_duplicate_delivery_is_absorbed()
    # asyncio.gather two cores, same cert_id -> exactly one "done", other "skipped:unclaimed"
async def test_split_brain_stale_owner_cannot_clobber()
    # claim T1; reset_to_pending; claim T2; cas_outcome(token=T1, FAILED) -> rowcount 0;
    # cas_outcome(token=T2, DONE) -> rowcount 1
async def test_inmemory_broker_wiring()
    # deps monkeypatched; await generate_pdf.kicker().with_broker(InMemoryBroker()).kiq(str(cert_id)) -> DONE
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement `generate.py`** per the flow above.
- [ ] **Step 4: Run to verify pass.**
- [ ] **Step 5: Commit** — `git commit -am "feat: generate_pdf task with claim CAS and retry taxonomy"`.

---

## Task 10: `recompute_job`, Cancel, Job Read Endpoints

**Files:**
- Modify: `src/aereo_assignment/services/recompute.py` (add `recompute_job`)
- Modify: `src/aereo_assignment/services/job_service.py` (add `get_job_view`, `list_jobs_page`, `cancel_job`; add `build_counts(counts_by_status) -> Counts` mapping — `pending` counts PENDING only, `processing` counts PROCESSING, per Global Constraints)
- Modify: `src/aereo_assignment/worker/tasks/generate.py` (import shared `recompute_job`, delete the private copy)
- Modify: `src/aereo_assignment/api/routes/jobs.py` (GET list, GET detail, POST cancel)
- Create: `tests/integration/test_recompute.py`, `tests/integration/test_jobs_api.py`, `tests/integration/test_cancel.py`

**Interfaces:**
- Produces:
  ```python
  async def recompute_job(session: AsyncSession, job_id: UUID) -> JobStatus | None
  # counts = status_counts([job_id]); decision = decide_job_status(counts); rowcount = recompute_cas(job_id, decision)
  # rowcount 0 -> None (terminal won the race, H2) else decision

  async def get_job_view(session, job_id: UUID) -> JobDetail          # + event_date, issuer, updated_at; raises JobNotFound
  async def list_jobs_page(session, *, status, page, size) -> Page[JobSummary]
  async def cancel_job(session, job_id: UUID) -> JobView              # raises JobNotFound / JobAlreadyFinal
  # TX: rowcount = cas_transition(job, PENDING|PROCESSING -> CANCELLED, set_completed_at=True)
  #     rowcount 0 -> re-read: status CANCELLED -> idempotent 200; terminal -> raise JobAlreadyFinal
  #     certs.cancel_open(job_id); commit; return view with counts
  ```
- Consumes: `page_size_max` from settings.

- [ ] **Step 1: Write failing tests**

```python
# test_recompute.py
async def test_recompute_transitions()               # parametrize: all DONE -> COMPLETED; mixed -> COMPLETED_WITH_ERRORS;
                                                     # all FAILED -> FAILED; partial -> PROCESSING; PENDING job w/ finished certs -> PROCESSING (M7 edge)
async def test_recompute_after_cancel_is_dropped()   # cancel, then recompute_job -> returns None, status stays CANCELLED

# test_cancel.py
async def test_cancel_soft_cancels_pending_and_retrying_but_not_processing()
async def test_cancel_idempotent_repeat_200()
async def test_cancel_completed_job_409()            # code JOB_ALREADY_FINAL
async def test_cancel_unknown_404()

# test_jobs_api.py
async def test_list_jobs_pagination_and_filter()     # size clamp >200 -> 200; page math; status filter
async def test_job_detail_shape()                    # includes event_date, issuer, updated_at, counts.processing
async def test_job_detail_404()
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement** per signatures; swap Task 9's private recompute for the shared one.
- [ ] **Step 4: Run to verify pass** — full suite: `uv run pytest -v`.
- [ ] **Step 5: Commit** — `git commit -am "feat: recompute, soft-cancel, and job read endpoints"`.

---

## Task 11: Reconcile — Periodic + Startup Recovery

**Files:**
- Create: `src/aereo_assignment/worker/tasks/reconcile.py`
- Modify: `src/aereo_assignment/worker/broker.py` (`@broker.startup()` hook)
- Modify: `src/aereo_assignment/worker/tasks/__init__.py`
- Create: `tests/integration/test_reconcile.py`

**Interfaces:**
- Consumes: repos (Task 6), `recompute_job` (Task 10), dispatch/generate kickers (Tasks 7 and 9).
- Produces:
  ```python
  ReconcileSummary = dict[str, int]  # keys: stale_certs_reset, stale_certs_failed, certs_reenqueued,
                                     #       jobs_dispatched, jobs_recomputed

  async def run_reconcile(deps: ReconcileDeps) -> ReconcileSummary

  @broker.task(name="reconcile", schedule=[{"interval": settings.reconcile_interval_seconds}])
  async def reconcile(deps: ReconcileDeps = TaskiqDepends(get_reconcile_deps)) -> ReconcileSummary

  # broker.py: @broker.startup() -> await run_reconcile(get_reconcile_deps())   # one impl, two triggers (C1)
  ```
  `run_reconcile` steps (order matters):
  ```
  1. stale_certs = find_stale(statuses=(PROCESSING, RETRYING), grace)
     for each: attempts >= max -> force_fail_stale(...) + recompute_job(job_id); collect for re-enqueue otherwise
  2. reset_to_pending(reset_ids)  ->  then immediately kiq generate_pdf for reset_ids
     (do NOT rely on stale-PENDING detection for ids reset in this same pass — they now carry fresh updated_at)
  3. find_stale_pending(grace) -> kiq generate_pdf for each (covers lost messages for never-claimed certs)
  4. stale_jobs = find_stale(statuses=(PENDING, PROCESSING), grace)
     PENDING  -> kiq dispatch_job (X1: fan-out runs regardless of job CAS)
     PROCESSING -> recompute_job (C2: closes crash-between-DONE-and-recompute)
  ```
  All comparisons use `func.now() - make_interval(secs=:grace)` — DB clock only (M5).

- [ ] **Step 1: Write failing integration tests** (direct `run_reconcile(deps)` calls; InMemoryBroker so kiq executes generate inline):

```python
async def test_resets_stale_processing_cert_and_regenerates()
    # cert stuck PROCESSING w/ old updated_at + orphan token -> after run: DONE and file exists
async def test_exhausted_stale_cert_swept_to_failed()
    # stale PROCESSING, attempts == max -> FAILED, job recompute ran (COMPLETED_WITH_ERRORS with a DONE sibling)
async def test_stale_pending_job_gets_dispatch()
    # job PENDING old, certs PENDING, never dispatched -> after run: certs DONE, job COMPLETED
async def test_stale_processing_job_recomputed_after_crash_window()
    # all certs DONE, job PROCESSING w/ old updated_at -> job COMPLETED, no cert touched
async def test_fresh_rows_untouched()
    # everything updated_at=now() -> summary all zeros, statuses unchanged
async def test_startup_hook_registered()          # the broker.startup callback runs run_reconcile (monkeypatch recorder)
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement `reconcile.py` + startup hook** per the ordered steps.
- [ ] **Step 4: Run to verify pass.**
- [ ] **Step 5: Commit** — `git commit -am "feat: periodic + startup reconcile with exhaustion sweep"`.

---

## Task 12: Retrieval — Certificates, Download, ZIP, Verify, Health

**Files:**
- Create: `src/aereo_assignment/services/certificate_service.py`
- Create: `src/aereo_assignment/schemas/certificates.py`
- Create: `src/aereo_assignment/api/routes/certificates.py`, `api/routes/verify.py`, `api/routes/health.py`
- Modify: `src/aereo_assignment/main.py` (mount routers)
- Create: `tests/integration/test_retrieval.py`, `test_verify.py`, `test_health.py`

**Interfaces:**
- Consumes: repos, `Storage`, pagination dep, error classes.
- Produces:
  ```python
  CertificateView {certificate_id, recipient_name, recipient_email, status, certificate_number,
                   attempts, error_message, download_url: str | None, generated_at}
  # download_url populated only when status == DONE: f"{base_url}/api/v1/jobs/{job_id}/certificates/{cert_id}/download"

  async def list_certificates_page(session, job_id, *, status, page, size) -> Page[CertificateView]
  async def download_certificate(session, job_id, cert_id, storage) -> tuple[Certificate, bytes]
      # status != DONE -> CertNotReady; DONE but missing -> FileMissing (row stays DONE, X2)
  async def build_job_zip(session, job_id, storage) -> tuple[BinaryIO, list[str]]
      # zero DONE -> NoCertificatesReady; pre-flight exists() per spec §8.7; missing -> skipped + logged;
      # zip members named "<CERTIFICATE_NUMBER>.pdf"; skipped -> "SKIPPED.txt" manifest inside the zip;
      # returns SpooledTemporaryFile(max_size=8MB) rewound + skipped numbers
  async def verify_certificate(session, number: str) -> VerifyView
      # {certificate_number, recipient_name, recipient_email, event_name, event_date, issuer,
      #  status, issued_at (None until DONE), job_id}; 200 even when not DONE; unknown -> CertificateNotFound
  ```
  ZIP route: `StreamingResponse(file, media_type="application/zip")` with `BackgroundTask(file.close)`. Download route: `Response(content=bytes, media_type="application/pdf", headers={"Content-Disposition": f'attachment; filename="{number}.pdf"'})`. Health: `SELECT 1` + `redis.asyncio.from_url(settings.redis_url).ping()`; both ok → 200 `{"status": "ok", "checks": {...}}`; any failure → 503 with per-check detail.

- [ ] **Step 1: Write failing integration tests**

```python
# test_retrieval.py
async def test_cert_list_filters_by_status_and_paginates()   # download_url null unless DONE
async def test_download_pdf_200_with_disposition_header()
async def test_download_not_ready_409()                      # code CERT_NOT_READY
async def test_download_file_missing_409_row_stays_done()    # unlink file -> 409 FILE_MISSING; cert still DONE
async def test_zip_contains_all_done_pdfs()
async def test_zip_skips_missing_and_writes_manifest()       # delete one file -> 200; others present; SKIPPED.txt lists its number
async def test_zip_zero_done_409()                           # code NO_CERTIFICATES_READY
async def test_zip_unknown_job_404()

# test_verify.py
async def test_verify_done_200()
async def test_verify_pending_number_200_with_status()       # existing but not DONE -> 200, issued_at null
async def test_verify_unknown_404()                          # code CERTIFICATE_NOT_FOUND

# test_health.py
async def test_health_ok()
async def test_health_degraded_503()                         # monkeypatch redis ping -> raises; 503, checks show detail
```

- [ ] **Step 2: Run to verify failure.**
- [ ] **Step 3: Implement** per signatures.
- [ ] **Step 4: Run to verify pass** — full suite.
- [ ] **Step 5: Commit** — `git commit -am "feat: retrieval endpoints — cert list, download, ZIP with manifest, verify, health"`.

---

## Task 13: Docker, Compose, README

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yaml`
- Modify: `.gitignore` (add `storage/`, `.env`), `README.md`

**Interfaces:**
- Consumes: everything; CLI entrypoints `aereo_assignment.worker.broker:broker` / `:scheduler`.
- Produces: one-command `docker compose up --build`.

- [ ] **Step 1: Write `Dockerfile`** — multi-stage on `ghcr.io/astral-sh/uv:python3.14-bookworm-slim`: build stage `uv sync --frozen`; runtime copies venv + `src` + `assets/` + `alembic*`; non-root user.
- [ ] **Step 2: Write `docker-compose.yaml`** — `postgres:16` (volume), `redis:7`, `api` (command `sh -c "alembic upgrade head && uvicorn aereo_assignment.main:app --host 0.0.0.0 --port 8000"`, port 8000, storage volume), `worker` (command `taskiq worker aereo_assignment.worker.broker:broker --modules aereo_assignment.worker.tasks`, same image + volume), `scheduler` (command `taskiq scheduler aereo_assignment.worker.broker:scheduler --modules aereo_assignment.worker.tasks`, same image). Env: `BCG_DATABASE_URL`, `BCG_REDIS_URL`, `BCG_STORAGE_DIR=/app/storage`, `BCG_BASE_URL=http://localhost:8000`. Note in README: alembic-on-boot is safe only with a single API replica.
- [ ] **Step 3: Smoke-test locally** — `docker compose up --build -d`; `curl -X POST localhost:8000/api/v1/jobs` with a 3-recipient payload → poll `GET /jobs/{id}` until COMPLETED → `GET .../download.zip` succeeds → `GET /health` 200 → `docker compose down -v`. (If Docker unavailable in the dev shell, run the same smoke against `uv run uvicorn` + compose-only postgres/redis and record it in the README "Verification" note.)
- [ ] **Step 4: Write `README.md`** — sections required by the assignment: setup, run, run tests, **submit a request** (curl example), **retrieve** (list / download / ZIP / verify with examples), important design decisions (Postgres-as-truth + disposable queue, hybrid validation/processing, CAS idempotency table, derived counts, retry taxonomy, reconcile, vendored fonts). Explicitly document the two user decisions: `processing` counts key (deviation from spec §8 shape) and `RedisStreamBroker` acks + XAUTOCLAIM (deviation from the spec's BRPOP diagram; Redis still holds no truth — reconcile + claim CAS remain the recovery mechanism).
- [ ] **Step 5: Final verification** — `uv run pytest -v` (full suite green) + `docker compose config -q` (valid).
- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: Dockerfile, compose stack, and README"`.
