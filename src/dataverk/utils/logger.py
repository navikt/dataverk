import logging


class Logger:

    def __init__(self, user: str, connector_name: str):
        self._logger = logging.getLogger(connector_name)
        self._configure_logger(user)

    @property
    def log(self):
        return self._logger

    def _configure_logger(self, user) -> None:
        self._logger.propagate = False
        self._logger.setLevel(logging.INFO)

        if not len(self._logger.handlers):
            handler = self._create_handler(user)
            self._logger.addHandler(handler)

    def _create_handler(self, user) -> logging.Handler:
        handler = logging.StreamHandler()
        formatter = self._create_formatter(user)
        handler.setFormatter(formatter)
        return handler

    def _create_formatter(self, user) -> logging.Formatter:
        return logging.Formatter(f"%(levelname)s:{user}:%(name)s:%(asctime)s: %(message)s",
                                 "%Y-%m-%d %H:%M:%S")
