from .datapackage_base import BaseDataPackage
from dataverk.utils.env_store import EnvStore
from shutil import rmtree


class DeleteDataPackage(BaseDataPackage):

    def __init__(self, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

    def _delete(self):
        ''' Fjerner datapakken og jenkinsjobben
        '''

        self.jenkins_server.delete_job(self.settings["package_name"])
        rmtree(self.settings["package_name"])

    def run(self):
        ''' Entrypoint for dataverk delete
        '''

        if not self._folder_exists_in_repo(self.settings["package_name"]):
            raise NameError(f'Det finnes ingen datapakke med navn {self.settings["package_name"]} '
                            f'i repo {self.github_project}')

        if not self.jenkins_server.job_exists(name=self.settings["package_name"]):
            raise NameError(f'Det finnes ingen jobber med navn {self.settings["package_name"]} '
                            f'på jenkins serveren.')

        print(f'Fjerning av datapakke {self.settings["package_name"]} i {self.github_project}')

        res = input(f'Er du sikker på at du ønsker å fjerne datapakken {self.settings["package_name"]} '
                    f'fra repository {self.github_project}? [j/n] ')

        if res in {'j', 'ja', 'y', 'yes'}:
            self._delete()
        else:
            print(f'Datapakken {self.settings["package_name"]} ble ikke fjernet')
