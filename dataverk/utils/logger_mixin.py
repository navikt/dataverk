import datetime

class LoggerMixin:
    """Logger with timestamps
    
    """
    def log(self, message):
        """ logger 
                
        Inherited from LoggerMixin class 

        """

        message = f'{self.user} {message}'
        message = f'{datetime.datetime.now().isoformat()}: {message}'
        print(message)