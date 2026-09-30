import ast
import inspect
import time
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import swiss_grid_lakehouse.reconcile as rc
from swiss_grid_lakehouse.silver.daily_rollup import rollup_rows

ZONE = ZoneInfo("Europe/Zurich")
RECORDED_STAMP = "2026-09-30T02:56:07.812085"
LONG_AGO = "2027-01-15T00:00:00"


def _hours(day, count, gwh, enduser_gwh=None):
    """`count` hourly rows from local midnight, adding up to `gwh` GWh (and `enduser_gwh`).

    `ts_local` is the wall clock, so on the day the clocks go back the hour 02:00 appears twice.
    """
    start = datetime(day.year, day.month, day.day, tzinfo=ZONE).astimezone(UTC)
    stamps = [
        (start + timedelta(hours=i)).astimezone(ZONE).replace(tzinfo=None) for i in range(count)
    ]
    mw = gwh * 1000 / count
    end_mw = None if enduser_gwh is None else enduser_gwh * 1000 / count
    return [(ts, mw, end_mw) for ts in stamps]


def _rollups(days):
    """days: {date: (swissgrid_total, swissgrid_enduser, entsoe_total)} in GWh, 24 hours each."""
    swissgrid, entsoe = [], []
    for day, (total, enduser, entsoe_total) in sorted(days.items()):
        swissgrid += _hours(day, 24, total, enduser)
        entsoe += _hours(day, 24, entsoe_total)
    return {
        "swissgrid": rollup_rows(swissgrid, "swissgrid"),
        "entsoe": rollup_rows(entsoe, "entsoe"),
    }


def _daily(published, stamp=RECORDED_STAMP, batch="aaaaaaaaaaaa"):
    """published: {date: (Landesverbrauch, Endverbrauch)} as Bronze rows of one batch."""
    return [
        {
            "local_date": day.isoformat(),
            "national_gwh": national,
            "final_gwh": final,
            "registry_modified": stamp,
            "batch_id": batch,
        }
        for day, (national, final) in published.items()
    ]


def _by_series(results):
    return {(r.source, r.series): r for r in results}


D30, D31 = date(2026, 8, 30), date(2026, 8, 31)
MAY1 = date(2026, 5, 1)
RECORDED_DAYS = {D30: (149.47, 109.73, 146.69), D31: (160.25, 134.76, 156.39)}
RECORDED_PUBLISHED = {D30: (149, 110), D31: (149, 125)}


def test_constants_carry_the_measured_values():
    assert (rc.TIGHT_PCT, rc.WIDE_PCT, rc.PRELIM_DAYS) == (6.5, 5.0, 30)
    assert "7.55" in rc.__doc__ or "7.55" in inspect.getsource(rc)


def test_age_is_counted_from_the_end_of_the_day_to_the_registry_stamp():
    assert rc.age_days(D31, "2026-09-01T00:00:00") == 0
    assert rc.age_days(D30, RECORDED_STAMP) == 30
    assert rc.age_days(D31, RECORDED_STAMP) == 29
    assert rc.age_days(D30, "2026-09-29T00:00:00") == 29
    assert rc.age_days(D30, LONG_AGO) > rc.PRELIM_DAYS


def test_final_day_within_tolerance_passes():
    results = rc.reconcile(_rollups({MAY1: (149.47, 109.73, 146.69)}), _daily({MAY1: (149, 110)}))
    for result in results:
        (day,) = result.days
        assert day.status == "PASS"
        assert (result.preliminary, result.final, result.passed) == (0, 1, True)
    assert rc.exit_code(results) == 0
    swissgrid_total = _by_series(results)[("swissgrid", "total")].days[0]
    assert swissgrid_total.diff_pct == pytest.approx(100 * (149.47 - 149) / 149)


def test_final_day_beyond_tolerance_fails_with_exit_3(monkeypatch, capsys):
    rollups = _rollups({MAY1: (160.25, 134.76, 146.69)})
    monkeypatch.setattr(rc, "load_rollups", lambda silver: rollups)
    monkeypatch.setattr(rc, "load_daily_rows", lambda daily: _daily({MAY1: (149, 125)}))
    assert rc.main(["--silver", "s", "--daily", "d"]) == 3
    out = capsys.readouterr().out
    assert "status=FAIL" in out
    summary = [ln for ln in out.splitlines() if ln.startswith("RECONCILE ")]
    assert len(summary) == 3
    assert summary[0].endswith(" FAIL") and "preliminary=0 final=1" in summary[0]
    assert summary[2].endswith(" PASS")


def test_preliminary_day_beyond_tolerance_is_printed_counted_and_exits_0(monkeypatch, capsys):
    rollups = _rollups({D31: (160.25, 134.76, 156.39)})
    monkeypatch.setattr(rc, "load_rollups", lambda silver: rollups)
    monkeypatch.setattr(rc, "load_daily_rows", lambda daily: _daily({D31: (149, 125)}))
    assert rc.main(["--silver", "s", "--daily", "d"]) == 0
    out = capsys.readouterr().out
    day_lines = [ln for ln in out.splitlines() if ln.startswith("DAY ")]
    assert len(day_lines) == 3 and all(ln.endswith("status=PRELIMINARY") for ln in day_lines)
    assert "diff_pct=7.55" in day_lines[0]
    summary = [ln for ln in out.splitlines() if ln.startswith("RECONCILE ")]
    assert all("preliminary=1 final=0" in ln and ln.endswith(" PASS") for ln in summary)


def test_recorded_slice_has_two_preliminary_days_on_every_series():
    results = rc.reconcile(_rollups(RECORDED_DAYS), _daily(RECORDED_PUBLISHED))
    assert [r.preliminary for r in results] == [2, 2, 2]
    assert rc.exit_code(results) == 0
    total = _by_series(results)[("swissgrid", "total")]
    assert [round(d.diff_pct, 2) for d in total.days] == [0.32, 7.55]
    assert [d.published_gwh for d in total.days] == [149, 149]


def test_same_rows_give_the_same_result_when_the_clock_says_2030(monkeypatch):
    rollups, daily = _rollups(RECORDED_DAYS), _daily(RECORDED_PUBLISHED)

    def rendered():
        return [ln for r in rc.reconcile(rollups, daily) for ln in r.lines()]

    before = rendered()

    class Date2030(date):
        @classmethod
        def today(cls):
            return cls(2030, 6, 1)

    monkeypatch.setattr(time, "time", lambda: 1_906_000_000.0)
    monkeypatch.setattr(rc, "date", Date2030)
    after = rendered()
    assert after == before
    assert sum("PRELIMINARY" in ln for ln in after if ln.startswith("DAY ")) == 6


def test_the_module_reads_no_clock():
    tree = ast.parse(inspect.getsource(rc))
    called = {
        n.func.attr
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    }
    assert not called & {"now", "today", "utcnow", "time", "monotonic"}


def test_registry_stamp_of_the_latest_batch_decides_not_row_order():
    old = _daily({D30: (149, 110)}, stamp="2026-09-05T00:00:00", batch="bbbbbbbbbbbb")
    new = _daily({D30: (150, 111)}, stamp="2026-11-20T00:00:00", batch="cccccccccccc")
    for rows in (old + new, new + old):
        results = rc.reconcile(_rollups({D30: (149.47, 109.73, 146.69)}), rows)
        total = _by_series(results)[("swissgrid", "total")].days[0]
        assert total.published_gwh == 150
        assert total.status == "PASS"


def test_a_batch_stamped_early_keeps_the_day_preliminary():
    rows = _daily({D30: (149, 110)}, stamp="2026-09-05T08:00:00")
    results = rc.reconcile(_rollups({D30: (149.47, 109.73, 146.69)}), rows)
    assert [r.days[0].status for r in results] == ["PRELIMINARY"] * 3


def test_no_overlap_exits_3(monkeypatch, capsys):
    rollups = _rollups({MAY1: (149.47, 109.73, 146.69)})
    monkeypatch.setattr(rc, "load_rollups", lambda silver: rollups)
    monkeypatch.setattr(rc, "load_daily_rows", lambda daily: _daily({date(2025, 1, 1): (149, 110)}))
    assert rc.main(["--silver", "s", "--daily", "d"]) == 3
    summary = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("RECONCILE ")]
    assert len(summary) == 3
    assert all("days=0 " in ln and ln.endswith(" FAIL") for ln in summary)


def test_incomplete_day_is_reported_never_scaled_and_exits_3(monkeypatch, capsys):
    rows = _hours(MAY1, 23, 143.0)  # 23 hours of 6.217 GWh: a 24-hour day is missing one hour
    rollups = {"swissgrid": rollup_rows(rows, "swissgrid"), "entsoe": rollup_rows(rows, "entsoe")}
    monkeypatch.setattr(rc, "load_rollups", lambda silver: rollups)
    monkeypatch.setattr(rc, "load_daily_rows", lambda daily: _daily({MAY1: (149, 110)}))
    assert rc.main(["--silver", "s", "--daily", "d"]) == 3
    out = capsys.readouterr().out
    day = next(ln for ln in out.splitlines() if ln.startswith("DAY ") and "series=total" in ln)
    assert "hours=23" in day and "status=INCOMPLETE" in day and "pipeline_gwh=143.00" in day
    assert "pipeline_gwh=143.00 published_gwh=149.00 diff_pct=-4.03" in day


def test_missing_enduser_hour_makes_the_day_incomplete():
    rows = _hours(MAY1, 24, 149.0, 110.0)
    rows[5] = (rows[5][0], rows[5][1], None)
    rollups = {"swissgrid": rollup_rows(rows, "swissgrid"), "entsoe": []}
    results = _by_series(rc.reconcile(rollups, _daily({MAY1: (149, 110)})))
    assert results[("swissgrid", "enduser")].days[0].status == "INCOMPLETE"
    assert results[("swissgrid", "enduser")].days[0].pipeline_gwh is None
    assert results[("swissgrid", "total")].days[0].status == "PASS"


def test_three_summary_lines_in_order_each_after_its_day_lines(monkeypatch, capsys):
    rollups = _rollups(RECORDED_DAYS)
    monkeypatch.setattr(rc, "load_rollups", lambda silver: rollups)
    monkeypatch.setattr(rc, "load_daily_rows", lambda daily: _daily(RECORDED_PUBLISHED))
    assert rc.main(["--silver", "s", "--daily", "d"]) == 0
    lines = capsys.readouterr().out.splitlines()
    kinds = [ln.split(" ", 1)[0] for ln in lines]
    assert kinds == ["DAY", "DAY", "RECONCILE"] * 3
    heads = [
        "RECONCILE source=swissgrid series=total published=landesverbrauch days=2 ",
        "RECONCILE source=swissgrid series=enduser published=endverbrauch days=2 ",
        "RECONCILE source=entsoe series=total published=landesverbrauch days=2 ",
    ]
    summary = [ln for ln in lines if ln.startswith("RECONCILE ")]
    for line, head in zip(summary, heads, strict=True):
        assert line.startswith(head)
        tokens = [t.split("=")[0] for t in line.split()[1:-1]]
        assert tokens == [
            "source", "series", "published", "days", "pipeline_gwh", "published_gwh",
            "diff_pct", "tol", "preliminary", "final",
        ]  # fmt: skip
        assert line.endswith(" PASS")
    assert (
        "pipeline_gwh=309.72 published_gwh=298.00 diff_pct=3.93 tol=6.5 preliminary=2 final=0"
        in summary[0]
    )
    assert "tol=6.5" in summary[1] and "tol=5 " in summary[2]


def test_spring_forward_day_of_23_hours_is_complete():
    day = date(2026, 3, 29)
    rows = _hours(day, 23, 143.0, 105.0)
    rollups = {"swissgrid": rollup_rows(rows, "swissgrid"), "entsoe": rollup_rows(rows, "entsoe")}
    results = rc.reconcile(rollups, _daily({day: (143, 105)}, stamp=LONG_AGO))
    assert [r.days[0].hours for r in results] == [23, 23, 23]
    assert [r.days[0].status for r in results] == ["PASS"] * 3


def test_fall_back_day_of_25_hours_is_complete_and_24_hours_is_not():
    day = date(2026, 10, 25)
    full = _hours(day, 25, 155.0, 115.0)
    short = _hours(day, 24, 155.0, 115.0)
    for rows, hours, status in ((full, 25, "PASS"), (short, 24, "INCOMPLETE")):
        rollups = {
            "swissgrid": rollup_rows(rows, "swissgrid"),
            "entsoe": rollup_rows(rows, "entsoe"),
        }
        results = rc.reconcile(rollups, _daily({day: (155, 115)}, stamp="2027-03-01T00:00:00"))
        assert [(r.days[0].hours, r.days[0].status) for r in results] == [(hours, status)] * 3


@pytest.mark.parametrize(
    ("gap_pct", "entsoe_status", "swissgrid_status"),
    [(4.0, "PASS", "PASS"), (5.8, "FAIL", "PASS"), (6.8, "FAIL", "FAIL")],
)
def test_entsoe_uses_the_wide_tolerance_and_swissgrid_the_tight_one(
    gap_pct, entsoe_status, swissgrid_status
):
    value = 150.0 * (1 + gap_pct / 100)
    results = _by_series(
        rc.reconcile(_rollups({MAY1: (value, 100.0, value)}), _daily({MAY1: (150, 100)}))
    )
    assert results[("entsoe", "total")].days[0].status == entsoe_status
    assert results[("swissgrid", "total")].days[0].status == swissgrid_status
    assert results[("entsoe", "total")].tol == 5.0
    assert results[("swissgrid", "total")].tol == 6.5
    assert results[("swissgrid", "enduser")].tol == 6.5


def test_tolerances_can_be_passed_in():
    rollups = _rollups({MAY1: (152.0, 100.0, 152.0)})
    results = rc.reconcile(rollups, _daily({MAY1: (150, 100)}), tight_pct=1.0, wide_pct=2.0)
    statuses = {k: r.days[0].status for k, r in _by_series(results).items()}
    assert statuses[("swissgrid", "total")] == "FAIL"
    assert statuses[("entsoe", "total")] == "PASS"


def test_unreadable_table_exits_2(monkeypatch, capsys):
    def broken(silver):
        raise OSError("no such table")

    monkeypatch.setattr(rc, "load_rollups", broken)
    assert rc.main(["--silver", "missing", "--daily", "d"]) == 2
    assert "cannot read the tables" in capsys.readouterr().err
