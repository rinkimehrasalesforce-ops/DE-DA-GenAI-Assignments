"""Quarantine rules (Q-A .. Q-E) for the claims pipeline.

Pure-Python implementations so a developer can import and test them on a
laptop in seconds -- no cluster, no pipeline run. The pipeline applies them
through a UDF (see my_transformation.py); tests call them directly.

Rule semantics mirror the original vectorized Spark expressions exactly,
including their null behaviour: a rule that cannot be fully evaluated
(missing dates, null amounts, unknown status, ...) fails the row.

Values are expected exactly as Spark delivers them after the pipeline's
try_cast step: datetime.date for DATE columns, datetime.datetime for
TIMESTAMP columns, numbers/Decimal for amounts, None for null.
"""

from datetime import date, datetime

DATA_START_DATE = date(2025, 7, 1)
DATA_END_DATE = date(2026, 6, 30)
VALID_STATUSES = ("submitted", "approved", "rejected", "settled")


def _as_date(value):
    """Return a datetime.date for DATE/TIMESTAMP-like values, else None."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _in_window(day):
    return day is not None and DATA_START_DATE <= day <= DATA_END_DATE


def passes_q_a(member_ok, hospital_ok):
    """Q-A referential integrity: member and hospital must both exist."""
    return bool(member_ok) and bool(hospital_ok)


def passes_q_b(admission_date, discharge_date, submitted_ts, settled_ts):
    """Q-B date ordering and data window.

    - admission and discharge present, ordered, both inside the window
    - submitted_ts present, inside the window, not before admission
    - settled_ts absent, or inside the window and not before submitted_ts
    """
    admission = _as_date(admission_date)
    discharge = _as_date(discharge_date)
    if admission is None or discharge is None:
        return False
    if not (_in_window(admission) and _in_window(discharge)):
        return False
    if admission > discharge:
        return False
    if not isinstance(submitted_ts, datetime):
        return False
    if not _in_window(submitted_ts.date()) or submitted_ts.date() < admission:
        return False
    if settled_ts is not None:
        if not isinstance(settled_ts, datetime):
            return False
        if not _in_window(settled_ts.date()) or settled_ts < submitted_ts:
            return False
    return True


def passes_q_c(billed_amount, approved_amount):
    """Q-C amount sanity: billed >= 0; approved (0 when null) <= billed; approved >= 0."""
    if billed_amount is None or billed_amount < 0:
        return False
    approved = 0 if approved_amount is None else approved_amount
    if approved > billed_amount:
        return False
    if approved_amount is not None and approved_amount < 0:
        return False
    return True


def passes_q_d(status, approved_amount, rejection_code, settled_ts):
    """Q-D status consistency."""
    if status not in VALID_STATUSES:
        return False
    if status == "submitted":
        return approved_amount is None and rejection_code is None and settled_ts is None
    if status == "approved":
        return approved_amount is not None and rejection_code is None and settled_ts is None
    if status == "rejected":
        return rejection_code is not None and settled_ts is None
    # settled
    return approved_amount is not None and settled_ts is not None and rejection_code is None


def passes_q_e(admission_date, discharge_date, room_days, room_rent_per_day, billed_amount):
    """Q-E room math; a same-day stay counts as 1 room-day.

    Mirrors Spark's greatest(datediff(discharge, admission), 1): when either
    date is null the datediff is null and greatest(null, 1) == 1, so the
    expected room-day count falls back to 1 (the billed-amount check still
    applies).
    """
    if room_days is None or room_rent_per_day is None or billed_amount is None:
        return False
    admission = _as_date(admission_date)
    discharge = _as_date(discharge_date)
    if admission is None or discharge is None:
        expected_room_days = 1
    else:
        expected_room_days = max((discharge - admission).days, 1)
    if room_days != expected_room_days:
        return False
    return room_days * room_rent_per_day <= billed_amount


def failed_rules_for(row):
    """Evaluate every rule for one claim row (dict or Row-like) and return
    the codes it failed, e.g. ['Q-C', 'Q-E']; empty list means the row is clean."""
    get = row.get if hasattr(row, "get") else (lambda k: row[k])
    failed = []
    if not passes_q_a(get("member_ok"), get("hospital_ok")):
        failed.append("Q-A")
    if not passes_q_b(get("admission_date"), get("discharge_date"),
                      get("submitted_ts"), get("settled_ts")):
        failed.append("Q-B")
    if not passes_q_c(get("billed_amount"), get("approved_amount")):
        failed.append("Q-C")
    if not passes_q_d(get("status"), get("approved_amount"),
                      get("rejection_code"), get("settled_ts")):
        failed.append("Q-D")
    if not passes_q_e(get("admission_date"), get("discharge_date"),
                      get("room_days"), get("room_rent_per_day"),
                      get("billed_amount")):
        failed.append("Q-E")
    return failed


def _sort_key(row):
    """Descending submission-time key with nulls last (mirrors the pipeline's
    desc_nulls_last window order: submitted_ts desc, then _ingest_ts desc)."""
    def ts(value):
        return value if isinstance(value, datetime) else None

    submitted = ts(row.get("submitted_ts"))
    ingest = ts(row.get("_ingest_ts"))
    return (submitted is not None, submitted or datetime.min,
            ingest is not None, ingest or datetime.min)


def dedupe_latest_by_submission(rows):
    """Keep, per claim_id, the row with the latest submitted_ts; ties broken
    by the latest _ingest_ts; nulls lose to non-null values (desc_nulls_last)."""
    best = {}
    for row in rows:
        claim_id = row.get("claim_id")
        if claim_id not in best or _sort_key(row) > _sort_key(best[claim_id]):
            best[claim_id] = row
    return list(best.values())
