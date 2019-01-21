from .dataverk_base import Action, DataverkBase
from .dataverk_init import DataverkInit
from .dataverk_schedule import DataverkSchedule
from .dataverk_delete import DataverkDelete
from dataverk.context.settings import SettingsStore
from dataverk.context.env_store import EnvStore


def get_datapackage_object(action: Action, settings: SettingsStore, envs: EnvStore) -> type(DataverkBase):
    if action is Action.INIT:
        return DataverkInit(settings=settings, envs=envs)
    elif action is Action.SCHEDULE:
        return DataverkSchedule(settings=settings, envs=envs)
    elif action is Action.DELETE:
        return DataverkDelete(settings=settings, envs=envs)
    else:
        raise ValueError(f'Invalid script parameter "action={action}" for get_datapackage_object()')
