"""Collect the report lines a notebook prints, so the notebook can return them.

`databricks bundle run` shows the text a notebook passes to `dbutils.notebook.exit`
and none of its `print` output. A notebook builds one `ReportLines`, prints every
report line through it, and ends with `report.exit(...)`; the run output then carries
the same lines that the notebook printed.
"""

from typing import Any


class ReportLines:
    """Print text unchanged and keep it, one entry per line."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def print(self, text: str) -> None:
        """Print `text` exactly as `print(text)` does and remember its lines."""
        print(text)
        self.lines.extend(text.split("\n"))

    def text(self) -> str:
        """The collected lines, joined by a line feed."""
        return "\n".join(self.lines)

    def exit(self, dbutils: Any = None) -> str:
        """Hand the collected lines to `dbutils.notebook.exit`; return them.

        Without a `dbutils` (a test, a shell) nothing is exited and the text is only returned.
        """
        text = self.text()
        if dbutils is not None:
            dbutils.notebook.exit(text)
        return text
