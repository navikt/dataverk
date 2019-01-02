from abc import ABC, abstractmethod
from enum import Enum


class Schedulers(Enum):
    JENKINS = 1,
    TRAVIS = 2,
    CIRCLE_CI = 3


def create_scheduler(scheduler: Schedulers):
    pass


class Scheduler(ABC):
    """ Abstrakt base klasse for CI jobb skedulerings tjenester

        Definerer en kontrakt for alle CI jobb skedulerings tjeneste wrappers.
    """

    @abstractmethod
    def create_job(self, job_name, config):
        raise NotImplementedError()

    def update_job(self, job_name, config):
        raise NotImplementedError()

    def delete_job(self, job_name):
        raise NotImplementedError()


