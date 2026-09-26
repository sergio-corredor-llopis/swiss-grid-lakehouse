"""Entry point for `python -m swiss_grid_lakehouse.bronze`."""

from swiss_grid_lakehouse.bronze.entsoe_load_bronze import main

if __name__ == "__main__":
    raise SystemExit(main())
