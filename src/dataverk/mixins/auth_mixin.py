import datetime
import os

from sys import platform

if platform == "linux" or platform == "linux2":
    import pwd

class AuthMixin:
    """Authenticator"""

    def _is_authorized(self, user = None):
        """Check if is user is authorized to access data source

        Inherited from AuthMixin class 
        
        """
        if (user == None):
            if platform == "linux" or platform == "linux2":
                user = pwd.getpwuid(os.getuid()).pw_name
            else:
                user = os.getlogin()

        # TODO check authentication
        return True

    def get_user(self):
        """Get currently logged in user

        Inherited from AuthMixin class 
        
        """

        if platform == "linux" or platform == "linux2":
            user = pwd.getpwuid(os.getuid()).pw_name
        else:
            user = os.getlogin()
        return user


class AuthError(Exception):
    """Auth Error (Exception)

    Inherited from AuthMixin class 
    
    """

    def __init__(self, message, errors):
        super().__init__(message)

        # TODO implement custom auth error - if required? 
        self.errors = errors