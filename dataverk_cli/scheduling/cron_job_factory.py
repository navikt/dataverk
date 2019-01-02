from enum import Enum


class CronjobType(Enum):
    NAISCronjob = 1,
    UNIXCronjob = 2,

def create_cronjob(type: CronjobType, cronjob_string):
    pass


# TODO implement concrete