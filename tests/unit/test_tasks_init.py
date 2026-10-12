"""Unit tests for task package exports (Task 9 fix round).

Importing ``aereo_assignment.worker.tasks`` must register BOTH tasks so a
worker started with ``--modules aereo_assignment.worker.tasks`` (the
documented CLI pattern) picks up ``dispatch_job`` and ``generate_pdf``.
"""

from __future__ import annotations


def test_task_package_exports_both_tasks() -> None:
    import aereo_assignment.worker.tasks as tasks_pkg
    from aereo_assignment.worker.tasks import dispatch_job, generate_pdf
    from aereo_assignment.worker.tasks.dispatch import (
        dispatch_job as dispatch_direct,
    )
    from aereo_assignment.worker.tasks.generate import (
        generate_pdf as generate_direct,
    )

    assert dispatch_job is dispatch_direct
    assert generate_pdf is generate_direct
    assert tasks_pkg.dispatch_job is dispatch_direct
    assert tasks_pkg.generate_pdf is generate_direct


def test_both_tasks_registered_on_broker() -> None:
    from aereo_assignment.worker.broker import broker
    from aereo_assignment.worker.tasks import dispatch_job, generate_pdf

    assert dispatch_job.task_name == "dispatch_job"
    assert generate_pdf.task_name == "generate_pdf"
    assert broker.find_task("dispatch_job") is dispatch_job
    assert broker.find_task("generate_pdf") is generate_pdf
