from dataverk_cli.dataverk_base import Action, DataverkBase
from dataverk_cli.dataverk_init import DataverkInit
from dataverk_cli.dataverk_schedule import DataverkSchedule
from dataverk_cli.dataverk_delete import DataverkDelete
from collections.abc import Mapping


def get_datapackage_object(action: Action, settings: Mapping, env_store: Mapping) -> type(DataverkBase):
    """
    Creates a Datapackage object for the Action

    :param action: The Action the Datapackage object should complete
    :param settings: Mapping object containing project settings
    :param env_store: Mapping object containing environment variables
    :return: Datapackage
    """
    if action is Action.INIT:
        return DataverkInit(settings=settings, envs=env_store)
    elif action is Action.SCHEDULE:
        return DataverkSchedule(settings=settings, envs=env_store)
    elif action is Action.DELETE:
        return DataverkDelete(settings=settings, envs=env_store)
    else:
        raise ValueError(f'Invalid script parameter "action={action}" for get_datapackage_object()')
