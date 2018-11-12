import sys


def get_calling_script_root():
    return sys.path[0]


if __name__ == "__main__":
    print(get_calling_script_root())
