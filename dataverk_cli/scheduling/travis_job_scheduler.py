import yaml
from collections.abc import Mapping
from xml.etree import ElementTree
from pathlib import Path
from string import Template
from .scheduler import Scheduler


class TravisJobScheduler(Scheduler):
    """ Implementer metoder for å håndtere skedulering av jobber for dataverk

    """

    def __init__(self, settings_store: Mapping, env_store: Mapping):
        super().__init__(settings_store, env_store)

        self._package_name = settings_store["package_name"]

    def job_exist(self):
        pass

    def configure_job(self):
        pass

    def delete_job(self):
        pass
