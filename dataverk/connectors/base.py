import json
from dataverk.utils.logger_mixin import LoggerMixin
from dataverk.utils.auth_mixin import AuthMixin, AuthError
from prometheus_client import Summary


# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')


class BaseConnector(AuthMixin, LoggerMixin):
    """Common connection methods
    
    """

    def __init__(self, encrypted = True):
        self.user = AuthMixin.get_user(self)
        self.encrypted = encrypted
        if (self._is_authorized() != True): 
            raise AuthError("auth", "not authorized")

    def log(self, message):
        """Logging util

        Method inherited from BaseConnector
        
        """
        LoggerMixin.log(self, message)


    def _is_authorized(self):
        """Verify authorization
        
        Method inherited from BaseConnector

        """
        return AuthMixin.is_authorized(self)

    def _get_conn(self):
        """Get Conn
        
        Method inherited from BaseConnector
        
        """
        raise NotImplementedError()

    def get_pandas_df(self, query):
        """Get Pandas
        
        Method inherited from BaseConnector

        """
        message =  { 'method': 'get_pandas_df', 'query': query}
        LoggerMixin.log(self, json.dumps(message))




   