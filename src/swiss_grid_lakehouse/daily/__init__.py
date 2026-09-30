"""Daily layer: the published national consumption, appended to a Bronze Delta table."""

from swiss_grid_lakehouse.daily.ogd_client import (
    COLUMNS,
    TABLE,
    batch_id_for,
    parse_daily_csv,
    read_registry_stamp,
    run,
    to_bronze_rows,
    write_bronze,
)

__all__ = [
    "COLUMNS",
    "TABLE",
    "batch_id_for",
    "parse_daily_csv",
    "read_registry_stamp",
    "run",
    "to_bronze_rows",
    "write_bronze",
]
