"""Pure job-status recompute decision (spec section 10.2).

The SQL ``UPDATE ... WHERE status NOT IN (terminal)`` guard that applies
this decision lives in a later task; this module is only the truth table.
"""

from __future__ import annotations

from collections.abc import Mapping

from aereo_assignment.db.models import CertificateStatus, JobStatus


def decide_job_status(counts: Mapping[CertificateStatus, int]) -> JobStatus:
    """Map per-status certificate counts to the job status.

    - all DONE -> COMPLETED
    - all FAILED (total > 0) -> FAILED
    - only DONE + FAILED present, at least one of each -> COMPLETED_WITH_ERRORS
    - anything else (unfinished rows remain, empty, or all-zero) -> PROCESSING
    """
    done = counts.get(CertificateStatus.DONE, 0)
    failed = counts.get(CertificateStatus.FAILED, 0)
    total = sum(counts.values())
    if total == 0:
        return JobStatus.PROCESSING
    if done == total:
        return JobStatus.COMPLETED
    if failed == total:
        return JobStatus.FAILED
    if done > 0 and failed > 0 and done + failed == total:
        return JobStatus.COMPLETED_WITH_ERRORS
    return JobStatus.PROCESSING
