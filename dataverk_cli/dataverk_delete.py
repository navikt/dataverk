import jenkins
from .dataverk_base import DataverkBase
from dataverk.context.env_store import EnvStore
from shutil import rmtree


class DataverkDelete(DataverkBase):

    def __init__(self, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

        self.jenkins_server = jenkins.Jenkins(self.settings["jenkins"]["url"],
                                              username=self.envs['USER_IDENT'],
                                              password=self.envs['PASSWORD'])

    def _delete(self):
        ''' Fjerner datapakken og jenkinsjobben
        '''

        self.jenkins_server.delete_job(self.settings["package_name"])
        try:
            rmtree(self.settings["package_name"])
        except OSError:
            print(f'Det finnes ingen datapakke med navn {self.settings["package_name"]} i repo {self.github_project}')


    def run(self):
        ''' Entrypoint for dataverk delete
        '''

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
