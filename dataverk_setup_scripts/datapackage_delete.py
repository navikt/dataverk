import os
import json
import jenkins

from shutil import rmtree
from dataverk.utils.env_store import EnvStore
from dataverk.utils import resource_discoverer

from pathlib import Path


class DeleteDatapackage:

    def __init__(self, github_project: str, package_name: str):
        self.package_name = package_name
        self.github_project = github_project

        if not self._is_in_repo_root():
            raise Exception(f'dataverk create må kjøres fra topp-nivået i git repoet')

        if not self._folder_exists_in_repo(self.package_name):
            raise NameError(f'En mappe med navn {self.package_name} '
                            f'finnes ikke i repo {self.github_project}')

        resource_files = resource_discoverer.search_for_files(start_path=Path(self.package_name), file_names=('.env', 'settings.json'), levels=4)

        if '.env' and 'settings.json' not in resource_files:
            Exception(f'.env og settings.json filer må finnes i repo for å kunne kjøre dataverk delete')

        self.envs = EnvStore(path=Path(resource_files['.env']))

        with open(resource_files["settings.json"], 'r') as settings_file:
            self.settings = json.load(settings_file)

        self.jenkins_server = jenkins.Jenkins(self.settings["jenkins"]["url"],
                                              username=self.envs['USER_IDENT'],
                                              password=self.envs['PASSWORD'])

        if self.jenkins_server.job_exists(name=self.settings["package_name"]):
            raise NameError(f'Det finnes ingen jobber med navn {self.package_name} '
                            f'på jenkins serveren.')

    def _is_in_repo_root(self):
        ''' Sjekk på om create_dataverk kjøres fra toppnivå i repo.

        :return: boolean: "True" hvis dataverk_create kjøres fra toppnivå i repo, "False" ellers
        '''

        current_dir = os.getcwd()
        git_root = os.popen('git rev-parse --show-toplevel').read().strip()

        return os.path.samefile(current_dir, git_root)

    def _folder_exists_in_repo(self, name: str):
        ''' Sjekk på om det finnes en mappe i repoet med samme navn som ønsket pakkenavn

        :return: boolean: "True" hvis pakkenavn allerede er tatt i bruk, "False" ellers
        '''

        for filename in os.listdir(os.getcwd()):
            if name == filename:
                print(f'En mappe med navn {name} eksisterer allerede i repo {self.github_project}')
                return True

        return False

    def delete(self):
        self.jenkins_server.delete_job(self.package_name)
        rmtree(self.package_name)


def get_github_url():
    if not os.popen('git rev-parse --is-inside-work-tree').read().strip():
        raise Exception("dataverk create må kjøres fra et git repository")

    return os.popen('git config --get remote.origin.url').read().strip()


def run(args):
    ''' Entrypoint for dataverk delete

    '''

    github_project = get_github_url()
    print(f'Fjerning av datapakke i {github_project}')

    if args.package_name is None:
        package_name = input(f'Skriv inn navn på datapakke som ønskes fjernet: ')
    else:
        package_name = args.package_name

    datapackage = DeleteDatapackage(github_project=github_project, package_name=package_name)

    res = input(f'Er du sikker på at du ønsker å fjerne datapakken {package_name} fra repository {github_project}? [y/n] ')

    if res in {'j', 'ja', 'y', 'yes'}:
        datapackage.delete()
    else:
        print(f'Datapakken ble ikke fjernet')
