import os

from .dataverk_base import Action, DataverkBase, create_settings_dict, get_settings_dict
from .dataverk_init import DataverkInit
from .dataverk_schedule import DataverkSchedule
from .dataverk_delete import DataverkDelete
from dataverk.context.settings import SettingsStore
from dataverk.context.env_store import EnvStore


def _is_in_git_repo():
    return os.popen('git rev-parse --is-inside-work-tree').read().strip()


def _is_in_repo_root():
    return os.path.samefile(os.popen('git rev-parse --show-toplevel').read().strip(), os.getcwd())


def get_datapackage_object(action: Action, settings: SettingsStore, envs: EnvStore) -> type(DataverkBase):
    if action is Action.INIT:
        return DataverkInit(settings=settings, envs=envs)
    elif action is Action.SCHEDULE:
        return DataverkSchedule(settings=settings, envs=envs)
    elif action is Action.DELETE:
        return DataverkDelete(settings=settings, envs=envs)
    else:
        raise ValueError(f'Invalid script parameter "action={action}" for get_datapackage_object()')
