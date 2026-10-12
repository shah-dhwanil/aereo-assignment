# Design Review — Bulk Certificate Generator (v1.1)

**Reviewed doc:** `2026-10-11-bulk-certificate-generator-design.md` (draft v1.1)
**Requirement source:** `Bulk_Certificate_Generator_Backend_Assignment.pdf`
**Review focus:** architectural soundness — will the system survive failures, is every operation idempotent, and does it recover from every crash window without human intervention?
**Date:** 2026-10-11

---

## Verdict

The core architecture is sound — Postgres-as-truth + disposable queue + CAS
transitions + derived counts is the right skeleton, and the design covers most
failure modes deliberately. However, the review found **3 genuine
liveness/recovery holes** (the system can hang forever in specific crash
windows), **2 idempotency-spec bugs**, and several smaller gaps.

**Recommendation:** approvable after the six critical/high items (C1–C3,
H1–H3) are amended. M1–M4 should be resolved in the same pass — they are
ambiguities an implementer *will* hit on day one.

---

## 🔴 Critical — system can hang permanently (recovery holes)

### C1. Reconcile runs only at worker startup → most failure modes recover only on restart

§10.6 + §7.5 make `WORKER_STARTUP reconcile` the *single* recovery path for:
API crash pre-enqueue, Redis flush/message loss, worker crash mid-render, lost
`RETRYING` redeliveries, partial dispatch fan-out. But it runs **once, at
worker boot**. In steady state (worker stays up for days), any of those events
leaves a job/cert **stuck indefinitely** with no recovery trigger at all.

This directly contradicts the goal "system doesn't fail / recovers from
failures" — recovery currently *requires* a human restarting a container.

**Fix:** run the same reconcile as a **periodic scheduled task** (Taskiq
scheduler / cron, every ~30–60s) *and* at startup. This preserves the
anti-goals: still **one** reconcile implementation, still one recovery path —
just time-triggered instead of boot-triggered. Startup invocation becomes a
special case (`reconcile(now)` at boot).

### C2. Orphaned job: crash between final cert `→ DONE` and job recompute → job stuck in `PROCESSING` forever

Reconcile's stale-job rule (§10.6) is `status = PENDING AND updated_at <
grace`. Consider: last cert commits `DONE`, worker crashes (or recompute
raises on a DB blip) **before recompute runs**. Result:

- All certs terminal (`DONE`/`FAILED`) ✅
- Job remains `PROCESSING` ❌
- Reconcile ignores it: not `PENDING`, no stale certs to reset → **nothing
  ever enqueues a recompute**

The job hangs in `PROCESSING` forever, even across restarts. This is a hole in
the diagram's own claim that recompute is triggered after every cert event.

**Fix:** broaden stale-job detection to
`status IN (PENDING, PROCESSING) AND updated_at < now() - grace`, and for
stale `PROCESSING` jobs enqueue/run **recompute** (which is already an
idempotent pure function, and early-exits for terminal jobs). One extra
reconcile branch, no new recovery mechanism.

### C3. Split-brain: status-only CAS cannot distinguish two workers holding `PROCESSING`

The claim guard is `UPDATE ... WHERE status IN (PENDING, RETRYING)` and the
completion guard is `WHERE status = PROCESSING`. But after a grace-reset,
**two workers can legitimately both see `PROCESSING`**:

1. W1 claims cert → `PROCESSING` (attempts=1), then stalls >120s (GC pause,
   container freeze, DB network partition, slow render).
2. Reconcile resets stale `PROCESSING → PENDING`; W2 claims → `PROCESSING`
   (attempts=2). Both now hold the same row.
3. W1 hits an error → `CAS PROCESSING → FAILED` **succeeds** — it just marked
   W2's healthy in-flight work `FAILED`.
4. W2 finishes successfully → `CAS PROCESSING → DONE` → rowcount 0 → **W2's
   success is silently discarded**.

The status column encodes *state*, not *ownership*. §7.5 case A only covers
the duplicate-delivery race (loser has rowcount 0 immediately); it doesn't
cover **re-claimed** certs where both parties are in `PROCESSING`.

**Fix:** add an ownership token — `certificates.claim_token uuid NULL`, set on
claim; all outcome transitions (`DONE`/`RETRYING`/`FAILED`) become:

```sql
UPDATE certificates
   SET status = :new, ...
 WHERE id = :id AND status = 'PROCESSING' AND claim_token = :mine
```

Rowcount 0 ⇒ someone else owns it ⇒ no-op. This is ~2 lines per transition and
keeps the "one mechanism (CAS)" philosophy intact. (Alternative:
heartbeat/`locked_at` refresh during render — heavier, and grace alone can
never distinguish slow from dead.)

---

## 🟠 High — idempotency spec bugs

### H1. `request_hash` is required by §10 row 1 but missing from the data model

§10 says the idempotency mechanism is "UNIQUE `idempotency_key` + **stored
request hash** + stored response snapshot", and `409 IDEMPOTENCY_KEY_REUSED`
(same key, different payload) is impossible without it. But the ER diagram
(§9) and DDL contain only `submission_response` — **no `request_hash`
column**. Since the doc says *the diagrams win*, the spec as written cannot
implement its own 409.

**Fix:** add `request_hash text NULL` (canonical JSON hash of the request
body) to `jobs`, or explicitly nest it inside `submission_response` JSONB —
but it must appear in the ER diagram + DDL.

### H2. Recompute's "early exit for terminal jobs" must be a CAS, not read-then-write

§10.2 is phrased as `if job.status in (terminal): exit` — a **read** followed
by a write. Race: recompute reads `PROCESSING` → cancel commits `CANCELLED`
→ recompute writes `COMPLETED`, **silently un-cancelling the job**. The doc's
own principle (§6 note: "EVERY transition = UPDATE … WHERE … rowcount 0 =
no-op") requires:

```sql
UPDATE jobs
   SET status = :new,
       completed_at = COALESCE(completed_at, now()),
       updated_at = now()
 WHERE id = :id
   AND status NOT IN ('COMPLETED', 'COMPLETED_WITH_ERRORS', 'FAILED', 'CANCELLED');
```

Make this explicit in §10.2 — it's the one mutation in the design currently
described without a CAS.

### H3. The claim guard doesn't enforce the attempts cap it claims to own

§10.1 asserts "the **DB `attempts` column is the authoritative cap**", but the
guard is only `status IN (PENDING, RETRYING)` + `attempts + 1`. Path to
violation: worker claims at `attempts=4`, **crashes** (no error path runs),
reconcile resets to `PENDING`, next claim runs attempt **5** — the cap is
exceeded by 1 per crash cycle. Bounded-ish but not what the spec says.

**Fix:** `WHERE status IN (PENDING, RETRYING) AND attempts < :max` in the
claim guard itself. One predicate, makes the doc's central retry claim
literally true.

---

## 🟡 Medium

| # | Issue | Where | Fix |
|---|---|---|---|
| M1 | **Post-commit Redis enqueue failure semantics unspecified.** Job is committed; if `kiq` raises, does the API return 202 (rely on reconcile) or 500? A 500 post-commit invites the client to resubmit *without* a key → duplicate job. "Decouples Redis availability from submission" implies 202, but it's never stated. | §5, §10 | Explicit rule: **never fail the request after commit**; enqueue best-effort, log on failure, reconcile (C1) guarantees pickup. |
| M2 | **Concurrent same-key submit flow unspecified.** Two racing requests with one key: the loser gets a UNIQUE violation — must `catch IntegrityError → SELECT existing row → compare hash → replay snapshot`, *inside the same request*. If unhandled, it's a 500 instead of a 202 replay. | §8.1, §10.5 | Spell out the integrity-error branch as a numbered step. |
| M3 | **`dispatch_job` fan-out termination unclear** ("bounded batch"). If it loops `WHERE status = PENDING`, certs awaiting claim stay `PENDING` → it re-sends duplicates until "bounded" is hit — wasted messages, ambiguous semantics. | §10 row 3 | Specify single-pass: one `SELECT id FROM certificates WHERE job_id=? AND status='PENDING'` (≤5000, fits easily) → enqueue each. Duplicates are safe via claim CAS; simplicity wins. |
| M4 | **ZIP endpoint has no rule for a missing file** (row `DONE`, file gone from disk — the single-download path handles this with `409 FILE_MISSING`, the ZIP path has no `alt`). Mid-stream 500 after bytes are sent = broken response. | §7.6, §8.7 | Pre-check all paths / skip-and-log missing files, or `409` before streaming begins. |
| M5 | **Clock-skew + config invariants unstated.** Staleness compares `updated_at` to "now" — must use DB-side `now()`, not app clocks. Also `retry_backoff_seconds` × attempts must stay < `reconcile_grace_seconds`, else reconcile steals in-flight backoffs (still bounded by attempts, but noisy). | §10.6, §11 | Document both invariants next to the config knobs. |
| M6 | **Doc-internal contradiction in 7.5:** participant `Redis` issues SQL (`R->>P: reset stale…`). Since "the diagram wins" (§ preamble), the literal spec has Redis writing Postgres — violating the key invariant. | §7.5 | Relabel as `W (reconcile)` / worker participant. Cosmetic, but the doc's own reading rule elevates it. |
| M7 | **Recompute can transition `PENDING → PROCESSING`** (reconcile enqueues certs and dispatch concurrently; a cert can finish before the dispatch CAS), but §6.1 only shows `PENDING → PROCESSING : dispatch_job CAS`. | §6.1 | Add the transition or specify recompute's from-states are `{PENDING, PROCESSING}`. |

---

## ⚪ Low / optional

- **Worker liveness invisible to clients:** `/health` checks DB + Redis only;
  a client can't distinguish "worker dead" from "slow" — job shows
  `PROCESSING` either way. Optional: expose worker heartbeat /
  `last_reconcile_at`. (Assignment doesn't require it.)
- **Orphaned tmp files on crash:** tmp→`os.replace` is right, but a crash
  pre-replace leaks the tmp file. Unique tmp suffix + best-effort unlink/GC.
- **`idempotency_key uuid` type:** rejects non-UUID client keys at the DB
  level; either validate + 422 in the header contract or widen to `text`.
- **Alembic-on-boot:** races if API replicas > 1. Fine for compose (note it
  in README); guard for scale.
- **FastAPI's default 422 body** doesn't match the error envelope — needs a
  custom `RequestValidationError` handler (implementation note).
- **Scope risk (assignment explicitly warns):** cancel, ZIP, verify+QR,
  idempotency keys, pagination, `RETRYING`, health are all *optional*
  features. The implementation plan should carry an explicit **MVP cut-line**
  (the 6 required test scenarios + README) before any optional feature, so
  optionals can't cost required functionality.

---

## What's genuinely solid (keep as-is)

- **"Postgres decides what is true; Redis decides what runs next"** —
  correct and consistently applied; Redis loss is survivable *once C1/C2 land*.
- **Derived counts via `GROUP BY`** — eliminates the entire class of
  counter-drift bugs; better than the usual `done_count += 1` design.
- **Deterministic file paths + tmp→`os.replace`** — makes concurrent or
  duplicate writes converge to identical locations with atomic visibility;
  download-while-writing is safe.
- **Per-cert tasks = structural failure isolation** — directly satisfies "one
  bad cert must not kill the job."
- **Persisted `attempts` as the retry cap** (middleware as second fence only)
  — right way to make retries survive broker restarts; just needs H3 to be
  enforced at claim.
- **Soft-cancel consistency** — job+certs cancelled atomically in one TX;
  in-flight certs finishing is coherent with recompute's terminal early-exit;
  repeat-cancel → 200, final → 409 is internally consistent.
- **Hybrid validation rule** — invalid rows visible as `FAILED` with reason;
  `422` only at zero valid; satisfies "job status identifies successful and
  failed generations."
- **All 6 required test scenarios + README items explicitly mapped** in
  §12/§13 — requirement coverage is complete.

---

## Failure-mode coverage matrix

Cross-check of every failure class against the design's recovery mechanism
**after** applying fixes C1–C3 (recovery = periodic reconcile + claim token):

| Failure | Detection | Recovery | Without fix? |
|---|---|---|---|
| API crash after commit, before enqueue | stale `PENDING` job (grace) | periodic reconcile enqueues dispatch | ❌ only on worker restart (C1) |
| Redis flush / message loss (dispatch or cert tasks) | stale `PENDING` certs | periodic reconcile re-enqueues | ❌ only on worker restart (C1) |
| Worker crash mid-render | stale `PROCESSING` cert | reset to `PENDING`, claim again (attempts still capped) | ⚠️ restart-only (C1) |
| Worker crash between final DONE and recompute | stale `PROCESSING` job | recompute re-run → terminal | ❌ permanent hang (C2) |
| Duplicate delivery (same cert to two workers) | claim CAS rowcount 0 | loser no-ops | ✅ already handled |
| Re-claim split-brain (stall > grace, both workers live) | claim_token mismatch | loser's outcome is a no-op | ❌ success discarded / false FAILED (C3) |
| Retry redelivery lost (middleware gives up) | cert stuck `RETRYING` | stale reset → claim; attempts cap bounds it | ⚠️ restart-only (C1) |
| Concurrent submit, same idempotency key | UNIQUE violation | IntegrityError → replay snapshot | ⚠️ flow unspecified (M2) |
| Cancel racing recompute | recompute CAS rowcount 0 | job stays `CANCELLED` | ⚠️ read-then-write race (H2) |
| Crash at `attempts = max` mid-claim | `attempts < max` in claim guard | bounded, cert → `FAILED` | ⚠️ cap can be exceeded (H3) |
| Disk full during render | `OSError` → retryable | RETRYING → redeliver → FAILED after cap | ✅ already handled |
| File gone after DONE | explicit check at download/ZIP | 409 FILE_MISSING / ZIP skip rule | ⚠️ ZIP unspecified (M4) |

---

## Recommended amendments (summary checklist)

- [ ] **C1** — Reconcile → **periodic scheduled task + startup**
- [ ] **C2** — Stale-job rule → include **stale `PROCESSING` jobs → recompute**
- [ ] **C3** — Add **`claim_token`** (or equivalent ownership) to certificate transitions
- [ ] **H1** — Add **`request_hash`** to ER diagram + DDL
- [ ] **H2** — Make recompute's terminal early-exit an **in-UPDATE CAS**
- [ ] **H3** — Add **`attempts < max`** to the claim predicate
- [ ] **M1** — Post-commit enqueue failure = still-202, never 500
- [ ] **M2** — Concurrent-key IntegrityError → replay branch, spelled out
- [ ] **M3** — Dispatch fan-out = single pass over PENDING ids
- [ ] **M4** — ZIP missing-file rule (pre-check or skip-and-log)
- [ ] **M5** — DB-side `now()` for staleness; backoff × attempts < grace invariant
- [ ] **M6** — Fix §7.5 Redis→SQL participant label
- [ ] **M7** — Add `PENDING → PROCESSING` recompute edge to §6.1

With C1–C3 and H1–H3 applied, every failure class the design enumerates —
crash anywhere, duplicate delivery, Redis flush, slow/stalled worker,
concurrent cancel/recompute, repeated client submit — converges through the
single CAS mechanism the doc promises, with no window that requires a restart
or a human.
