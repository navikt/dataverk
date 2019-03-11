from collections import Mapping
from enum import Enum
from .jenkins_job_scheduler import JenkinsJobScheduler
from dataverk_cli.scheduling.deploy_key import DeployKey


class Schedulers(Enum):
    JENKINS = 1


def create_scheduler(settings_store: Mapping, env_store: Mapping, remote_repo_url: str, deploy_key: DeployKey):

    if env_store is None:
        raise TypeError("env_store cannot be None")

    if settings_store is None:
        raise TypeError("settings_store cannot be None")

    scheduler_type = read_scheduler_from_settings(settings_store)

    # Factory
    if scheduler_type is Schedulers.JENKINS:
        return JenkinsJobScheduler(settings_store, env_store, remote_repo_url, deploy_key)
    else:
        raise KeyError(f"Scheduler matching Schedulers({scheduler_type}) could not be found")


def read_scheduler_from_settings(settings_store: Mapping):
    if settings_store.get("jenkins") is not None:
        return Schedulers.JENKINS
    raise LookupError(f"settings_store({settings_store} does not contain valid scheduler configurations)")
