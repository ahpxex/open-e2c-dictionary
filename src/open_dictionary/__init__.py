from .db.importer import main as _cli_main

def importer() -> None:
    raise SystemExit(_cli_main())
