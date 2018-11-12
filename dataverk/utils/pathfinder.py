import sys


def get_project_root():
    return sys.path[0]


if __name__ == "__main__":
    print(get_project_root())
