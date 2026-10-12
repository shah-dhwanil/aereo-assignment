"""Unit tests for the pure recompute decision (spec section 10.2)."""

import pytest

from aereo_assignment.db.models import CertificateStatus as CS
from aereo_assignment.db.models import JobStatus
from aereo_assignment.services.recompute import decide_job_status

D = CS.DONE
F = CS.FAILED
P = CS.PENDING
P2 = CS.PROCESSING
R = CS.RETRYING
C = CS.CANCELLED


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ({D: 3}, JobStatus.COMPLETED),
        ({D: 2, F: 1}, JobStatus.COMPLETED_WITH_ERRORS),
        ({F: 4}, JobStatus.FAILED),
        ({D: 1, P: 1}, JobStatus.PROCESSING),
        ({R: 1}, JobStatus.PROCESSING),
        ({D: 2, R: 1}, JobStatus.PROCESSING),
        ({P: 5}, JobStatus.PROCESSING),
        ({P2: 2}, JobStatus.PROCESSING),
        ({}, JobStatus.PROCESSING),
        ({D: 0, F: 0, P: 0}, JobStatus.PROCESSING),
        ({C: 2}, JobStatus.PROCESSING),
        ({D: 1, F: 1, P: 1}, JobStatus.PROCESSING),
    ],
)
def test_truth_table(counts, expected):
    assert decide_job_status(counts) == expected
