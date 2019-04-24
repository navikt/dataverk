from abc import ABC, abstractmethod

from dataverk.mixins.logger_mixin import LoggerMixin
from dataverk.mixins.auth_mixin import AuthMixin, AuthError
from prometheus_client import Summary


# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request', None)


class BaseConnector(ABC, AuthMixin, LoggerMixin):
    """Common connection methods
    
    """
    def __init__(self):
        self.user = self.get_user()
        if self._is_authorized() is not True:
            raise AuthError("auth", "not authorized")
