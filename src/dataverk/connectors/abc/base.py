import os
from abc import ABC
from dataverk.utils.logger import Logger


class DataverkBase(ABC):
    """
    Base class for all dataverk classes
    """
    def __init__(self):
        self._logger = Logger(user=self._get_user(),
                              connector_name=self._get_class_name())

    @property
    def log(self):
        return self._logger.log

    @staticmethod
    def _get_user():
        try:
            import pwd
            return pwd.getpwuid(os.getuid()).pw_name
        except ImportError:
            return os.getlogin()

    def _get_class_name(self):
        return self.__class__.__name__
