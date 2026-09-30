"""The comparison arithmetic and exit codes, in memory (no Spark)."""

from datetime import UTC, date, datetime, timedelta

import pytest

from swiss_grid_lakehouse.compare import __main__ as compare

# 2026-09-01 00:00 local time (CEST) is 2026-08-31 22:00 UTC.
DAY1 = datetime(2026, 8, 31, 22, tzinfo=UTC)


def _rows(factors, entsoe_mw=6000.0, area="CH", start=DAY1, enduser_factor=None):
    """One day (24 hours) per factor: Swissgrid = ENTSO-E x factor, every hour of that day."""
    rows = []
    for d, factor in enumerate(factors):
        for h in range(24):
            ts = start + timedelta(days=d, hours=h)
            rows.append({"source": "entsoe", "area": area, "ts_utc": ts, "load_mw": entsoe_mw})
            row = {"source": "swissgrid", "area": area, "ts_utc": ts, "load_mw": entsoe_mw * factor}
            if enduser_factor is not None:
                row["load_enduser_mw"] = entsoe_mw * enduser_factor
            rows.append(row)
    return rows


def test_pass_just_below_the_tolerance():
    report = compare.build_report(_rows([1.0499]), tol_pct=5.0)
    assert report.passed and report.exit_code == 0
    assert report.days[0].gap_pct == pytest.approx(4.99)
    assert report.line() == "COMPARE days=1 hours=24 daily_max_abs_pct=4.99 tol=5 PASS"


def test_fail_just_above_the_tolerance():
    report = compare.build_report(_rows([1.0501]), tol_pct=5.0)
    assert not report.passed and report.exit_code == 3
    assert report.line() == "COMPARE days=1 hours=24 daily_max_abs_pct=5.01 tol=5 FAIL"


def test_a_negative_gap_counts_by_its_absolute_value():
    assert compare.build_report(_rows([0.96]), tol_pct=5.0).passed
    assert not compare.build_report(_rows([0.94]), tol_pct=5.0).passed


def test_every_day_must_pass():
    report = compare.build_report(_rows([1.01, 1.02, 1.06]), tol_pct=5.0)
    assert [round(d.gap_pct, 2) for d in report.days] == [1.0, 2.0, 6.0]
    assert report.daily_max_abs_pct == pytest.approx(6.0)
    assert not report.passed


def test_hourly_gaps_never_decide_the_exit_code():
    rows = _rows([1.0])
    for r in rows:  # +30 % in hour 0 and -30 % in hour 1 of the local day: the day total is equal
        h = (r["ts_utc"] - DAY1).total_seconds() / 3600
        if r["source"] == "swissgrid" and h == 0:
            r["load_mw"] *= 1.3
        if r["source"] == "swissgrid" and h == 1:
            r["load_mw"] *= 0.7
    report = compare.build_report(rows, tol_pct=1.0)
    assert report.total_pooled.maximum == pytest.approx(30.0)
    assert report.total_pooled.minimum == pytest.approx(-30.0)
    assert report.passed


def test_per_hour_min_median_max_on_a_three_day_toy_series():
    # local hour 5 gets gaps 2, 4, 9 %; every other hour 0 %; the daily totals are ignored here
    rows = _rows([1.0, 1.0, 1.0])
    gaps = {0: 1.02, 1: 1.04, 2: 1.09}
    for r in rows:
        offset = r["ts_utc"] - DAY1
        day, hour = divmod(int(offset.total_seconds() // 3600), 24)
        if r["source"] == "swissgrid" and hour == 5:
            r["load_mw"] = 6000.0 * gaps[day]
    report = compare.build_report(rows, tol_pct=100.0)
    by_hour = {s.hour: s for s in report.total_by_hour}
    assert sorted(by_hour) == list(range(24))
    assert (by_hour[5].n, by_hour[5].minimum, by_hour[5].median, by_hour[5].maximum) == (
        3,
        pytest.approx(2.0),
        pytest.approx(4.0),
        pytest.approx(9.0),
    )
    assert (by_hour[6].minimum, by_hour[6].median, by_hour[6].maximum) == (0.0, 0.0, 0.0)
    assert report.hours == 72
    assert "HOURLY total: min 0.00 median 0.00 max 9.00 (n=72, percent)" in report.lines()


def test_enduser_column_is_reported_beside_the_total():
    report = compare.build_report(_rows([1.02], enduser_factor=0.9))
    assert report.total_pooled.median == pytest.approx(2.0)
    assert report.enduser_pooled.median == pytest.approx(-10.0)
    assert any(
        line.startswith("HOURLY end-user: min -10.00 median -10.00") for line in report.lines()
    )
    assert compare.build_report(_rows([1.02])).lines()[-3] == "HOURLY end-user: n/a"


def test_hour_of_day_is_local_time():
    report = compare.build_report(_rows([1.0]))
    assert [s.hour for s in report.total_by_hour] == list(range(24))
    assert report.total_by_hour[0].n == 1


def test_an_incomplete_day_is_left_out():
    rows = [r for r in _rows([1.01, 1.02]) if not (r["ts_utc"] == DAY1 + timedelta(hours=30))]
    report = compare.build_report(rows)
    assert [d.day for d in report.days] == [date(2026, 9, 1)]
    assert report.hours == 24


def test_a_day_needs_both_sources():
    only_entsoe = [r for r in _rows([1.0]) if r["source"] == "entsoe"]
    assert compare.build_report(only_entsoe).days == []


def test_clock_change_days_have_23_and_25_hours():
    assert compare.hours_in_local_day(date(2026, 3, 29)) == 23
    assert compare.hours_in_local_day(date(2026, 10, 25)) == 25
    assert compare.hours_in_local_day(date(2026, 9, 1)) == 24
    start = datetime(2026, 10, 24, 22, tzinfo=UTC)  # 2026-10-25 00:00 local
    rows = []
    for h in range(25):
        ts = start + timedelta(hours=h)
        rows.append({"source": "entsoe", "area": "CH", "ts_utc": ts, "load_mw": 5000.0})
        rows.append({"source": "swissgrid", "area": "CH", "ts_utc": ts, "load_mw": 5050.0})
    report = compare.build_report(rows)
    assert report.hours == 25 and report.days[0].gap_pct == pytest.approx(1.0)


def test_area_is_part_of_the_join_key():
    mixed = [r for r in _rows([1.0]) if r["source"] == "entsoe"] + [
        r for r in _rows([1.0], area="XX") if r["source"] == "swissgrid"
    ]
    assert compare.build_report(mixed).days == []


def test_other_sources_and_zero_entsoe_hours_are_ignored():
    rows = _rows([1.03]) + [
        {"source": "other", "area": "CH", "ts_utc": DAY1, "load_mw": 1.0e6},
    ]
    assert compare.build_report(rows).days[0].gap_pct == pytest.approx(3.0)
    zero = _rows([1.03])
    zero[0]["load_mw"] = 0.0  # the ENTSO-E value of hour 0 is zero, so that hour is skipped
    assert compare.build_report(zero).days == []


def test_no_overlap_line():
    line = compare.build_report([]).line()
    assert (
        line == "COMPARE days=0 hours=0 daily_max_abs_pct=n/a tol=3 FAIL no complete day overlaps"
    )


def test_default_tolerance_is_whole_percent_above_the_recorded_days():
    assert compare.DEFAULT_TOL_PCT == 3.0
    assert 2.47 < compare.DEFAULT_TOL_PCT < 3.0 + 1.0


def test_exit_code_table(monkeypatch, capsys):
    rows = {
        "pass": _rows([1.02]),
        "fail": _rows([1.06]),
        "none": _rows([1.0])[::2],
        "empty": [],
    }
    current = {}
    monkeypatch.setattr(compare, "_load_rows", lambda silver: rows[current["k"]])

    def run(key, *extra):
        current["k"] = key
        code = compare.main(["--silver", "unused", *extra])
        return code, capsys.readouterr()

    code, cap = run("pass")
    assert code == 0 and cap.out.strip().endswith("tol=3 PASS")
    code, cap = run("fail")
    assert code == 3 and cap.out.strip().endswith("tol=3 FAIL")
    code, cap = run("fail", "--tol-pct", "7")
    assert code == 0 and "tol=7 PASS" in cap.out
    code, cap = run("none")
    assert code == 3 and "no complete day overlaps" in cap.out
    code, cap = run("empty")
    assert code == 3
    code, cap = run("pass", "--tol-pct", "-1")
    assert code == 2 and "must not be negative" in cap.err


def test_unreadable_table_and_bad_flags_exit_2(monkeypatch, capsys):
    def boom(silver):
        raise OSError("no such table")

    monkeypatch.setattr(compare, "_load_rows", boom)
    assert compare.main(["--silver", "nowhere"]) == 2
    assert "cannot read Silver table" in capsys.readouterr().err
    with pytest.raises(SystemExit) as missing:
        compare.main([])
    assert missing.value.code == 2
    with pytest.raises(SystemExit) as bad:
        compare.main(["--silver", "x", "--tol-pct", "abc"])
    assert bad.value.code == 2


def test_help_documents_exit_codes_and_default(capsys):
    with pytest.raises(SystemExit) as done:
        compare.main(["--help"])
    assert done.value.code == 0
    text = " ".join(capsys.readouterr().out.split())
    assert "Exit codes: 0 PASS; 3" in text and "2 usage or input error" in text
    assert "+2.47 %" in text and "default: 3" in text
