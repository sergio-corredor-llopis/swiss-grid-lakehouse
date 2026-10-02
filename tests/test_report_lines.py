"""ReportLines prints text unchanged, keeps its lines and hands them to dbutils.notebook.exit."""

from swiss_grid_lakehouse.report_lines import ReportLines


class FakeNotebook:
    def __init__(self):
        self.exited = []

    def exit(self, value):
        self.exited.append(value)


class FakeDbutils:
    def __init__(self):
        self.notebook = FakeNotebook()


def test_printed_text_is_unchanged(capsys):
    out = ReportLines()
    out.print("GATE: PASS checks=6")
    out.print("DAY a\nDAY b")
    assert capsys.readouterr().out == "GATE: PASS checks=6\nDAY a\nDAY b\n"


def test_lines_are_collected_one_per_line():
    out = ReportLines()
    out.print("RUN spark_version=4.0.0 environment_version=3")
    out.print("DAY a\nDAY b")
    assert out.lines == ["RUN spark_version=4.0.0 environment_version=3", "DAY a", "DAY b"]
    assert out.text() == "RUN spark_version=4.0.0 environment_version=3\nDAY a\nDAY b"


def test_exit_passes_the_lines_to_dbutils():
    out = ReportLines()
    out.print("one")
    out.print("two")
    dbutils = FakeDbutils()
    assert out.exit(dbutils) == "one\ntwo"
    assert dbutils.notebook.exited == ["one\ntwo"]


def test_exit_without_dbutils_only_returns_the_text():
    out = ReportLines()
    out.print("one")
    assert out.exit(None) == "one"
    assert out.exit() == "one"


def test_the_notebook_guard_finds_no_dbutils_offline():
    out = ReportLines()
    out.print("one")
    assert out.exit(globals().get("dbutils")) == "one"
