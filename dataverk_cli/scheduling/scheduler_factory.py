from collections import Mapping
from enum import Enum
from .jenkins_job_scheduler import JenkinsJobScheduler


class Schedulers(Enum):
    JENKINS = 1,
    TRAVIS = 2,
    CIRCLE_CI = 3


def create_scheduler(scheduler: Schedulers, settings_store: Mapping, env_store: Mapping=None):
    if env_store is None:
        env_store = {}
    if scheduler is Schedulers.JENKINS:
        return JenkinsJobScheduler(settings_store, env_store)
    else:
        raise KeyError(f"Scheduler({scheduler}) could not be found")
