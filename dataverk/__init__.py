from . import connectors
from . import utils
from dataverk.context.settings import settings_store_factory
from dataverk.api import _current_dir, read_sql, to_sql, write_notebook, read_kafka
from dataverk.datapackage import Datapackage
from pathlib import Path

version_file_path = Path(__file__).parent.joinpath("VERSION")
with version_file_path.open("r") as fh:
    __version__ = fh.read()

__all__ = ['connectors',
           '_current_dir',
           'read_sql',
           'to_sql',
           'utils',
           'write_notebook',
           'Datapackage',
           'settings_store_factory',
           'read_kafka'
           ]
