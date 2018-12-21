import subprocess

from dataverk_cli.dataverk_base import DataverkBase
from dataverk.context.env_store import EnvStore
from dataverk.utils.validators import validate_cronjob_schedule
from .jenkins_job_scheduler import JenkinsJobScheduler
from pathlib import Path


class DataverkSchedule(DataverkBase):

    def __init__(self, args, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

        self._args = args
        self._scheduler = JenkinsJobScheduler(settings_store=settings, env_store=envs)
        self._package_name = settings["package_name"]

    def run(self):
        if not self._datapackage_exists_in_remote_repo():
            raise FileNotFoundError(f'Datapakken må eksistere i remote repositoriet før man kan eksekvere <dataverk-cli'
                                    f' schedule>. Kjør git add->commit->push av datapakken og prøv på nytt.')

        self._set_update_schedule()
        self._print_datapipeline_config()
        res = input(f'Vil du sette opp pipeline for datapakken over? [j/n] ')

        if res in {'j', 'ja', 'y', 'yes'}:
            try:
                self._schedule_job()
            except Exception:
                raise Exception(f'Klarte ikke sette opp pipeline for datapakke {self._package_name}')
        else:
            print(f'Pipeline for datapakke {self._package_name} ble ikke opprettet')

    def _datapackage_exists_in_remote_repo(self):
        try:
            subprocess.check_output(["git", "cat-file", "-e", f'origin/master:{self._package_name}/Jenkinsfile'])
            return True
        except subprocess.CalledProcessError:
            return False

    def _set_update_schedule(self):
        if self._args.update_schedule is None:
            update_schedule = input("Skriv inn ønsket oppdateringsschedule for datapakken "
                                    "(format: \"<minutt> <time> <dag i måned> <måned> <ukedag>\", "
                                    "f.eks. \"0 12 * * 2,4\" vil gi <Hver tirsdag og torsdag kl 12.00 UTC>): ")
            if not update_schedule:
                update_schedule = "* * 31 2 *"  # Default value Feb 31 (i.e. never)
        else:
            update_schedule = self._args.update_schedule

        validate_cronjob_schedule(update_schedule)
        self.settings["update_schedule"] = update_schedule

    def _schedule_job(self):
        ''' Setter opp schedulering av job for datapakken
        '''

        self._edit_cronjob_config()
        self._configure_jenkins_job()

    def _edit_cronjob_config(self):
        ''' Tilpasser cronjob config fil til datapakken
        '''

        cronjob_file_path = Path(self._package_name).joinpath("cronjob.yaml")
        self._scheduler.edit_cronjob_config(cronjob_file_path, self.settings["image_endpoint"])

    def _configure_jenkins_job(self):
        ''' Tilpasser jenkins konfigurasjonsfil og setter opp ny jenkins jobb for datapakken
        '''

        self._edit_jenkins_job_config()

        config_file_path = Path(self._package_name).joinpath("jenkins_config.xml")
        if self._scheduler.jenkins_job_exists():
            self._scheduler.update_jenkins_job(config_file_path=config_file_path)
        else:
            self._scheduler.create_new_jenkins_job(config_file_path=config_file_path)

    def _edit_jenkins_job_config(self):
        config_file_path = Path(self._package_name).joinpath("jenkins_config.xml")
        tag_value = {"scriptPath": self._package_name + '/Jenkinsfile',
                     "projectUrl": self.github_project,
                     "url": self.github_project}
        self._scheduler.edit_jenkins_job_config(config_file_path, tag_val_map=tag_value)