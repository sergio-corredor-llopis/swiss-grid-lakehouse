"""Entry point for `python -m swiss_grid_lakehouse.daily`."""

from swiss_grid_lakehouse.daily.ogd_client import main

if __name__ == "__main__":
    raise SystemExit(main())
