from . import connectors
from . import utils
from dataverk.context.settings import settings_store_factory
from dataverk.api import _current_dir, read_sql, to_sql, write_notebook, publish_datapackage, publish_datapackage_google_cloud, publish_datapackage_s3_nais, write_datapackage
from dataverk.datapackage import Datapackage
from pathlib import Path

version_file_path = Path(__file__).parent.joinpath("VERSION")
with version_file_path.open("r") as fh:
    __version__ = fh.read()

__all__ = ['connectors',
           '_current_dir',
           'publish_datapackage_google_cloud',
           'publish_datapackage_s3_nais',
           'publish_datapackage',
           'read_sql',
           'to_sql',
           'utils',
           'write_datapackage',
           'write_notebook',
           'Datapackage',
           'settings_store_factory',
           ]
