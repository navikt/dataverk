from pathlib import Path

version_file_path = Path(__file__).parent.parent.joinpath("dataverk/VERSION")
with version_file_path.open("r") as fh:
    __version__ = fh.read()
