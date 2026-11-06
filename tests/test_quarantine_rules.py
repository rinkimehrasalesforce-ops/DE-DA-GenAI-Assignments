"""Unit tests for the quarantine rules (Q-A .. Q-E).

Pure Python -- no cluster, no Spark session: a developer can run these on a
laptop in seconds before anything gets deployed.

Run:  python -m pytest tests -v
"""

from datetime import date, datetime, timedelta

from quarantine_rules import (
    DATA_END_DATE,
    DATA_START_DATE,
    dedupe_latest_by_submission,
    failed_rules_for,
    passes_q_a,
    passes_q_b,
    passes_q_c,
    passes_q_d,
    passes_q_e,
)


def make_row(**overrides):
    """A claim row that passes every rule; tests mutate one field at a time."""
    row = dict(
        claim_id="CLM-0001",
        member_ok=True,
        hospital_ok=True,
        admission_date=date(2025, 8, 1),
        discharge_date=date(2025, 8, 4),
        submitted_ts=datetime(2025, 8, 5, 10, 0, 0),
        settled_ts=datetime(2025, 8, 6, 12, 0, 0),
        status="settled",
        billed_amount=1000.0,
        approved_amount=800.0,
        rejection_code=None,
        room_days=3,
        room_rent_per_day=100.0,
    )
    row.update(overrides)
    return row


# ---------------------------------------------------------------- clean row

def test_clean_row_passes_all_rules():
    assert failed_rules_for(make_row()) == []


# ------------------------------------------------- one row per rule Q-A..Q-E

def test_q_a_unknown_member_fails_only_q_a():
    assert failed_rules_for(make_row(member_ok=False)) == ["Q-A"]
    assert passes_q_a(False, True) is False
    assert passes_q_a(True, False) is False


def test_q_b_admission_after_discharge_fails_only_q_b():
    # room_days=1 keeps Q-E satisfied (datediff is negative, so the expected
    # room-day count falls back to 1); only Q-B flags the bad date order.
    assert failed_rules_for(make_row(
        admission_date=date(2025, 8, 10), discharge_date=date(2025, 8, 4),
        room_days=1)) == ["Q-B"]


def test_q_c_approved_above_billed_fails_only_q_c():
    assert failed_rules_for(make_row(approved_amount=1200.0)) == ["Q-C"]


def test_q_d_approved_with_rejection_code_fails_only_q_d():
    assert failed_rules_for(make_row(
        status="approved", rejection_code="R-01")) == ["Q-D"]


def test_q_e_wrong_room_days_fails_only_q_e():
    assert failed_rules_for(make_row(room_days=5)) == ["Q-E"]


# -------------------------------------------------------- rule-boundary cases

def test_q_b_outside_data_window_fails():
    assert passes_q_b(date(2025, 6, 30), date(2025, 7, 2),
                      datetime(2025, 7, 3), None) is False
    assert passes_q_b(DATA_START_DATE, DATA_START_DATE,
                      datetime(2025, 7, 2), None) is True
    assert passes_q_b(DATA_END_DATE, DATA_END_DATE,
                      datetime(2026, 7, 1), None) is False


def test_q_b_null_settled_ts_is_allowed_but_not_before_submission():
    assert passes_q_b(date(2025, 8, 1), date(2025, 8, 4),
                      datetime(2025, 8, 5, 10), None) is True
    assert passes_q_b(date(2025, 8, 1), date(2025, 8, 4),
                      datetime(2025, 8, 5, 10), datetime(2025, 8, 5, 9)) is False


def test_q_c_null_approved_counts_as_zero():
    assert passes_q_c(500.0, None) is True
    assert passes_q_c(0.0, None) is True
    assert passes_q_c(None, None) is False
    assert passes_q_c(-1.0, None) is False
    assert passes_q_c(100.0, -5.0) is False


def test_q_d_each_status_and_unknown_status():
    assert passes_q_d("submitted", None, None, None) is True
    assert passes_q_d("submitted", 10.0, None, None) is False
    assert passes_q_d("approved", 10.0, None, None) is True
    assert passes_q_d("approved", None, None, None) is False
    assert passes_q_d("rejected", None, "R-01", None) is True
    assert passes_q_d("rejected", None, None, None) is False
    assert passes_q_d("settled", 10.0, None, datetime(2025, 8, 6)) is True
    assert passes_q_d("settled", 10.0, None, None) is False
    assert passes_q_d("bogus", None, None, None) is False
    assert passes_q_d(None, None, None, None) is False


def test_q_e_same_day_stay_counts_as_one_room_day():
    assert passes_q_e(date(2025, 8, 1), date(2025, 8, 1), 1, 100.0, 500.0) is True
    assert passes_q_e(date(2025, 8, 1), date(2025, 8, 1), 2, 100.0, 500.0) is False


def test_q_e_null_dates_fall_back_to_one_expected_room_day():
    # Spark's greatest(datediff(null,...), 1) == 1, so with null dates the
    # expected room-day count is 1 (billed-amount check still applies).
    assert passes_q_e(None, None, 1, 100.0, 500.0) is True
    assert passes_q_e(None, None, 2, 100.0, 500.0) is False
    assert passes_q_e(None, None, 1, 100.0, None) is False


# ---------------------------------------------- dedupe by submission time

def test_dedupe_keeps_latest_submission_per_claim():
    early = make_row(claim_id="CLM-1", submitted_ts=datetime(2025, 8, 5, 10),
                     status="submitted", approved_amount=None, settled_ts=None,
                     billed_amount=100.0, room_days=1, room_rent_per_day=100.0)
    late = make_row(claim_id="CLM-1", submitted_ts=datetime(2025, 8, 6, 10))
    other = make_row(claim_id="CLM-2")

    kept = {r["claim_id"]: r for r in dedupe_latest_by_submission([early, late, other])}

    assert set(kept) == {"CLM-1", "CLM-2"}
    assert kept["CLM-1"] is late
    assert kept["CLM-2"] is other


def test_dedupe_tie_broken_by_latest_ingest_ts_and_nulls_lose():
    a = make_row(claim_id="CLM-1", submitted_ts=datetime(2025, 8, 5, 10),
                 _ingest_ts=datetime(2026, 1, 1, 9), billed_amount=111.0,
                 room_days=1, room_rent_per_day=100.0,
                 approved_amount=None, settled_ts=None, status="submitted")
    b = make_row(claim_id="CLM-1", submitted_ts=datetime(2025, 8, 5, 10),
                 _ingest_ts=datetime(2026, 1, 1, 10))
    no_ts = make_row(claim_id="CLM-1", submitted_ts=None,
                     status="submitted", approved_amount=None, settled_ts=None,
                     billed_amount=100.0, room_days=1, room_rent_per_day=100.0)

    kept = dedupe_latest_by_submission([a, b, no_ts])

    assert len(kept) == 1
    assert kept[0] is b  # same submitted_ts -> later _ingest_ts wins


def test_dedupe_null_submitted_ts_loses_to_any_non_null():
    has_ts = make_row(claim_id="CLM-1", submitted_ts=datetime(2025, 8, 5, 10),
                      _ingest_ts=datetime(2026, 1, 1), room_days=1,
                      room_rent_per_day=100.0, approved_amount=None,
                      settled_ts=None, status="submitted", billed_amount=100.0)
    null_ts = make_row(claim_id="CLM-1", submitted_ts=None,
                       _ingest_ts=datetime(2026, 12, 31), status="submitted",
                       approved_amount=None, settled_ts=None, billed_amount=100.0,
                       room_days=1, room_rent_per_day=100.0)

    kept = dedupe_latest_by_submission([null_ts, has_ts])

    assert len(kept) == 1
    assert kept[0] is has_ts
