from dataverk.utils import resource_discoverer
from dataverk.context import EnvStore
from pathlib import Path


def get_env_store():
    """ Retrives .env file from project folder

        :return EnvStore containing Environment variables and .env variables
    """

    resource_files = resource_discoverer.search_for_files(start_path=Path('.'), file_names=('.env',), levels=1)
    if '.env' not in resource_files:
        raise FileNotFoundError(f'.env fil må finnes i repo for å kunne kjøre dataverk-cli init/schedule/delete')

    return EnvStore(path=Path(resource_files['.env']))