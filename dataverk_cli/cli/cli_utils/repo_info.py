from git import Repo, exc
from pathlib import Path


def get_remote_url() -> str:
    ''' Get remote repository https URL

    :return: remote repository url: str
    '''

    remote = Repo().remotes[0]

    return remote.url


def convert_to_ssh_url(https_url: str) -> str:
    ''' Convert https url to ssh

    :param https url: str
    :return: ssh url: str
    '''

    url_list = Path(https_url).parts
    org_name = url_list[2]
    repo_name = url_list[3]

    return f'git@github.com:{org_name}/{repo_name}'


def get_org_name(https_url: str) -> str:
    ''' Get organization name from remote repository url

    :param https url: str
    :return: organization name: str
    '''

    url_list = Path(https_url).parts

    return url_list[2]


def get_repo_name(https_url: str) -> str:
    ''' Get repository name from remote repository url

    :param https_url: str
    :return: repository name: str
    '''

    url_list = Path(https_url).parts

    return url_list[3].split('.')[0]


def is_in_git_repo() -> bool:
    ''' Check if current dir is git repository

    :return: bool
    '''

    try:
        Repo()
    except exc.InvalidGitRepositoryError:
        return False
    else:
        return True
