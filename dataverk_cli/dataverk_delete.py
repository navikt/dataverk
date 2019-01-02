from .dataverk_base import DataverkBase
from dataverk_cli.scheduling.jenkins_job_scheduler import JenkinsJobScheduler
from dataverk.context.env_store import EnvStore
from shutil import rmtree


class DataverkDelete(DataverkBase):

    def __init__(self, package_name: str, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

        self._package_name = package_name
        self._scheduler = JenkinsJobScheduler(settings_store=settings, env_store=envs)

    def run(self):
        ''' Entrypoint for dataverk delete
        '''

        if self._package_name is None:
            raise ValueError(f'For å kjøre <dataverk-cli delete> må pakkenavn angis (-p, --package-name). '
                             f'F.eks. <dataverk-cli delete --package-name min-pakke')

        if not self._scheduler.jenkins_job_exists():
            raise NameError(f'Det finnes ingen jobber med navn {self._package_name} '
                            f'på jenkins serveren.')

        print(f'Fjerning av datapakke {self._package_name} i {self.github_project}')

        res = input(f'Er du sikker på at du ønsker å fjerne datapakken {self._package_name} '
                    f'fra repository {self.github_project}? [j/n] ')

        if res in {'j', 'ja', 'y', 'yes'}:
            self._delete()
        else:
            print(f'Datapakken {self._package_name} ble ikke fjernet')

    def _delete(self):
        ''' Fjerner datapakken og jenkinsjobben
        '''

        self._scheduler.delete_jenkins_job()
        try:
            rmtree(self._package_name)
        except OSError:
            print(f'Det finnes ingen datapakke med navn {self._package_name} i repo {self.github_project}')
