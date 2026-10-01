from datetime import date
from decimal import Decimal

from bank_lakehouse.reconcile import compare

TODAY = date(2026, 10, 1)


def test_equal_days_are_not_reported():
    s = {date(2026, 1, 1): (10, Decimal("100.00"))}
    assert compare(s, dict(s), today=TODAY) == []


def test_old_mismatch_is_broken_recent_is_settling():
    source = {date(2026, 1, 1): (10, Decimal("100")), date(2026, 9, 30): (5, Decimal("50"))}
    dwh = {date(2026, 1, 1): (9, Decimal("90")), date(2026, 9, 30): (4, Decimal("40"))}
    diffs = {d.day: d for d in compare(source, dwh, today=TODAY)}
    assert diffs[date(2026, 1, 1)].settling is False
    assert diffs[date(2026, 9, 30)].settling is True


def test_day_missing_on_one_side_is_a_diff():
    diffs = compare({date(2026, 1, 1): (1, Decimal("1"))}, {}, today=TODAY)
    assert diffs[0].dwh == (0, Decimal(0))
