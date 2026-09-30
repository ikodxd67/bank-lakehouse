from datetime import date

from bank_lakehouse.source.generate import GenParams, month_ranges


def test_month_ranges_cover_period_without_gaps():
    r = month_ranges(date(2025, 1, 15), date(2025, 4, 10))
    assert r == [
        (date(2025, 1, 15), date(2025, 2, 1)),
        (date(2025, 2, 1), date(2025, 3, 1)),
        (date(2025, 3, 1), date(2025, 4, 1)),
        (date(2025, 4, 1), date(2025, 4, 10)),
    ]


def test_month_ranges_empty_when_until_before_start():
    assert month_ranges(date(2025, 5, 1), date(2025, 5, 1)) == []


def test_default_history_ends_today():
    assert GenParams().end() == date.today()
    assert GenParams(until=date(2026, 1, 1)).end() == date(2026, 1, 1)
