from pathlib import Path

with Path("VERSION").open("r") as fh:
    __version__ = fh.read()
