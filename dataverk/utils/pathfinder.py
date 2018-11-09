import sys
import os


def get_project_root():
    return os.path.dirname(__file__)


if __name__ == "__main__":
    print(get_project_root())
