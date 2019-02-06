from collections import Mapping
from enum import Enum
from .jenkins_job_scheduler import JenkinsJobScheduler
from .travis_job_scheduler import TravisJobScheduler


class Schedulers(Enum):
    JENKINS = 1,
    TRAVIS = 2,
    CIRCLE_CI = 3


def create_scheduler(settings_store: Mapping, env_store: Mapping):

    if env_store is None:
        raise TypeError("env_store cannot be None")

    if settings_store is None:
        raise TypeError("settings_store cannot be None")

    scheduler_type = read_scheduler_from_settings(settings_store)

    # Factory
    if scheduler_type is Schedulers.JENKINS:
        return JenkinsJobScheduler(settings_store, env_store)
    if scheduler_type is Schedulers.TRAVIS:
        return TravisJobScheduler(settings_store, env_store)
    else:
        raise KeyError(f"Scheduler matching Schedulers({scheduler_type}) could not be found")


def read_scheduler_from_settings(settings_store: Mapping):
    if settings_store.get("jenkins") is not None:
        return Schedulers.JENKINS
    if settings_store.get("travis") is not None:
        return Schedulers.TRAVIS
    if settings_store.get("circle_ci") is not None:
        return Schedulers.CIRCLE_CI
    raise LookupError(f"settings_store({settings_store} does not contain valid scheduler configurations)")
