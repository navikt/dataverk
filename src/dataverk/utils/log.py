import logging


def get_logger(user, connector_name):
    logger = logging.getLogger(connector_name)
    logger.propagate = 0
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(f"%(levelname)s:{user}:%(name)s:%(asctime)s: %(message)s",
                                  "%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
