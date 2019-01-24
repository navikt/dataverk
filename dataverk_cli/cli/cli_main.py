import os


def _is_in_git_repo():
    return os.popen('git rev-parse --is-inside-work-tree').read().strip()


def _is_in_repo_root():
    return os.path.samefile(os.popen('git rev-parse --show-toplevel').read().strip(), os.getcwd())


