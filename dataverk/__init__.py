from . import connectors
from . import utils
from dataverk.utils.settings_store import SettingsStore
from dataverk.api import get_path, read_sql, write_notebook, publish_datapackage, publish_datapackage_google_cloud, publish_datapackage_s3_nais, write_datapackage
from dataverk.datapackage import Datapackage
from pathlib import Path

version_file_path = Path(__file__).parent.parent.joinpath("VERSION")
with version_file_path.open("r") as fh:
    __version__ = fh.read()

__all__ = ['connectors',
           'utils',
           'get_path',
           'read_sql',
           'write_notebook',
           'publish_datapackage_google_cloud',
           'publish_datapackage_s3_nais',
           'publish_datapackage',
           'write_datapackage',
           'Datapackage',
           'SettingsStore'
           ]
