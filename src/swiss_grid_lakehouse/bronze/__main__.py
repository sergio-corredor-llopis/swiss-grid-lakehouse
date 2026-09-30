"""Entry point for `python -m swiss_grid_lakehouse.bronze`.

`--xml FILE` loads an ENTSO-E load response, `--swissgrid FILE` loads a Swissgrid workbook.
"""

import sys

from swiss_grid_lakehouse.bronze.entsoe_load_bronze import main as entsoe_main


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if any(a == "--swissgrid" or a.startswith("--swissgrid=") for a in args):
        from swiss_grid_lakehouse.bronze.swissgrid_energy_bronze import main as swissgrid_main

        return swissgrid_main(args)
    if any(a in ("-h", "--help") for a in args):
        print(
            "usage: python -m swiss_grid_lakehouse.bronze "
            "(--xml FILE | --swissgrid FILE) --target PATH_OR_TABLE\n\n"
            "  --xml FILE        ENTSO-E Swiss actual-load XML to load (also --input)\n"
            "  --swissgrid FILE  Swissgrid energy workbook (.xlsx) or CSV to load\n"
            "  --target DST      Delta path or table name (also --out)"
        )
        return 0
    return entsoe_main(args)


if __name__ == "__main__":
    raise SystemExit(main())
