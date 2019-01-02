import os

from .dataverk_base import Action, DataverkBase, create_settings_dict, get_settings_dict
from .dataverk_init import DataverkInit
from .dataverk_schedule import DataverkSchedule
from .dataverk_delete import DataverkDelete
from dataverk.context.env_store import EnvStore
from dataverk.utils import resource_discoverer
from pathlib import Path


def _is_in_git_repo():
    return os.popen('git rev-parse --is-inside-work-tree').read().strip()


def _is_in_repo_root():
    return os.path.samefile(os.popen('git rev-parse --show-toplevel').read().strip(), os.getcwd())


def get_datapackage_object(action: Action, args) -> type(DataverkBase):
    if not _is_in_git_repo():
        raise EnvironmentError(f'dataverk-cli init/schedule/delete må kjøres fra et git repository')

    if not _is_in_repo_root():
        raise EnvironmentError(f'dataverk-cli init/schedule/delete må kjøres fra topp-nivået i git repoet')

    resource_files = resource_discoverer.search_for_files(start_path=Path('.'), file_names=('.env',), levels=3)
    if '.env' not in resource_files:
        raise FileNotFoundError(f'.env fil må finnes i repo for å kunne kjøre dataverk-cli init/schedule/delete')

    envs = EnvStore(path=Path(resource_files['.env']))

    if action is Action.INIT:
        return DataverkInit(settings=create_settings_dict(args=args, envs=envs), envs=envs)
    elif action is Action.SCHEDULE:
        return DataverkSchedule(args=args, settings=get_settings_dict(args.package_name), envs=envs)
    elif action is Action.DELETE:
        return DataverkDelete(package_name=args.package_name, settings=get_settings_dict(args.package_name), envs=envs)
    else:
        raise ValueError(f'Invalid script parameter "action={action}" for get_datapackage_object()')
