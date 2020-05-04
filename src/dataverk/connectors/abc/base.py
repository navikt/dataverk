import os
from abc import ABC
from dataverk.utils import log


class BaseConnector(ABC):
    """Base class for all dataverk connectors
    
    """
    def __init__(self):
        self._logger = log.get_logger(user=self._get_user(),
                                      connector_name=self._get_class_name())

    @property
    def log(self):
        return self._logger

    @staticmethod
    def _get_user():
        try:
            import pwd
            return pwd.getpwuid(os.getuid()).pw_name
        except ImportError:
            return os.getlogin()

    def _get_class_name(self):
        return self.__class__.__name__
