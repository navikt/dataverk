from abc import ABC, abstractmethod
from collections import Mapping
from enum import Enum


class Scheduler(ABC):
    """ Abstrakt base klasse for CI jobb skedulerings tjenester

        Definerer en kontrakt for alle CI jobb skedulerings tjeneste wrappers.
    """

    def __init__(self, settings_store: Mapping, env_store: Mapping):
        self._env_store = env_store
        self._settings_store = settings_store

    @abstractmethod
    def create_job(self, job_name, config_file):
        raise NotImplementedError()

    @abstractmethod
    def update_job(self, job_name, config_file):
        raise NotImplementedError()

    @abstractmethod
    def delete_job(self, job_name):
        raise NotImplementedError()

    @abstractmethod
    def job_exist(self, job_name):
        raise NotImplementedError()


