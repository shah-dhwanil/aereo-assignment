"""Unit tests for row validation (pure domain, no IO)."""

from aereo_assignment.services.validation import (
    RecipientIn,
    RowError,
    ValidRecipient,
    validate_recipients,
)


def _good(name="Ada Lovelace", email="ada@x.com", extra_metadata=None):
    return RecipientIn(
        name=name,
        email=email,
        extra_metadata=dict(extra_metadata) if extra_metadata else {},
    )


def test_valid_rows_pass():
    rows = [_good(), _good(name="Alan Turing", email="alan@x.com")]
    valid, errors = validate_recipients(rows)
    assert errors == []
    assert valid == [
        ValidRecipient(name="Ada Lovelace", email="ada@x.com", extra_metadata={}),
        ValidRecipient(name="Alan Turing", email="alan@x.com", extra_metadata={}),
    ]


def test_invalid_email_row_isolated():
    rows = [_good(), _good(email="not-an-email"), _good(email="grace@x.com")]
    valid, errors = validate_recipients(rows)
    assert len(valid) == 2
    assert len(errors) == 1
    assert errors[0].index == 1
    assert errors[0].field == "email"
    assert errors[0].message
    # valid rows keep their original order; invalid row absent from valid
    assert [v.email for v in valid] == ["ada@x.com", "grace@x.com"]


def test_name_stripped_and_bounded():
    rows = [
        _good(name="  Ada  "),  # 0: valid, stripped
        _good(name="   "),  # 1: blank -> invalid
        _good(name="x" * 201),  # 2: too long -> invalid
        _good(name="y" * 200),  # 3: boundary -> valid
    ]
    valid, errors = validate_recipients(rows)
    assert [v.name for v in valid] == ["Ada", "y" * 200]
    assert [(e.index, e.field) for e in errors] == [(1, "name"), (2, "name")]
    assert all(isinstance(e, RowError) and e.message for e in errors)


def test_extra_metadata_rules():
    rows = [
        _good(extra_metadata={"score": 95, "grade": "A", "pass": True}),  # 0: valid
        _good(extra_metadata={f"k{i}": "v" for i in range(11)}),  # 1: 11 keys
        _good(extra_metadata={"note": "z" * 201}),  # 2: 201-char value
        _good(extra_metadata={"ratio": 3.14}),  # 3: float value
    ]
    valid, errors = validate_recipients(rows)
    assert len(valid) == 1
    assert valid[0].extra_metadata == {"score": 95, "grade": "A", "pass": True}
    assert [(e.index, e.field) for e in errors] == [
        (1, "extra_metadata"),
        (2, "extra_metadata"),
        (3, "extra_metadata"),
    ]


def test_zero_valid_returns_all_errors():
    rows = [_good(email="bad"), _good(name="   "), _good(email="also bad")]
    valid, errors = validate_recipients(rows)
    assert valid == []
    assert [e.index for e in errors] == [0, 1, 2]


def test_valid_error_partition_is_disjoint_and_complete():
    rows = [_good(), _good(email="bad"), _good(name=""), _good()]
    valid, errors = validate_recipients(rows)
    error_indices = {e.index for e in errors}
    assert len(valid) + len(errors) == len(rows)
    assert error_indices == {1, 2}
