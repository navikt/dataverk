import os

from .datapackage_base import Action, BaseDataPackage, create_settings_dict, get_settings_dict
from .datapackage_create import CreateDataPackage
from .datapackage_update import UpdateDataPackage
from .datapackage_delete import DeleteDataPackage
from dataverk.utils.env_store import EnvStore
from dataverk.utils import resource_discoverer
from pathlib import Path


def get_datapackage_object(action: Action, args) -> type(BaseDataPackage):
    if not os.popen('git rev-parse --is-inside-work-tree').read().strip():
        raise Exception(f'dataverk create/update/delete må kjøres fra et git repository')

    if not os.path.samefile(os.popen('git rev-parse --show-toplevel').read().strip(), os.getcwd()):
        raise Exception(f'dataverk create/update/delete må kjøres fra topp-nivået i git repoet')

    resource_files = resource_discoverer.search_for_files(start_path=Path('.'), file_names=('.env',), levels=3)
    if '.env' not in resource_files:
        raise Exception(f'.env fil må finnes i repo for å kunne kjøre dataverk create/update/delete')

    envs = EnvStore(path=Path(resource_files['.env']))

    if action is Action.CREATE:
        return CreateDataPackage(settings=create_settings_dict(args=args, envs=envs), envs=envs)
    elif action is Action.UPDATE:
        return UpdateDataPackage(settings=get_settings_dict(args.package_name), envs=envs)
    elif action is Action.DELETE:
        return DeleteDataPackage(settings=get_settings_dict(args.package_name), envs=envs)
    else:
        raise Exception(f'Invalid script parameter "action={action}" for get_datapackage_object()')