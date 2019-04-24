from dataverk import connectors
from . import utils
from dataverk.dataverk_context import DataverkContext
from dataverk.dataverk import Dataverk
from pathlib import Path

version_file_path = Path(__file__).parent.joinpath("VERSION")
with version_file_path.open("r") as fh:
    __version__ = fh.read()

__all__ = ['connectors',
           'utils',
           'Dataverk',
           'DataverkContext',
           ]
