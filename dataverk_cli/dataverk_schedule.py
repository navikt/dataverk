import jenkins
import os
import yaml

from dataverk_cli.dataverk_base import DataverkBase
from dataverk.context.env_store import EnvStore
from xml.etree import ElementTree
from string import Template
from shutil import rmtree
from .jenkins_job_scheduler import JenkinsJobScheduler
from pathlib import Path

class DataverkSchedule(DataverkBase):

    def __init__(self, args, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

        self.args = args
        self._scheduler = JenkinsJobScheduler(settings_store=settings, env_store=envs)
        self._package_name = settings["package_name"]

    def _edit_cronjob_config(self):
        ''' Tilpasser cronjob config fil til datapakken
        '''

        cronjob_file_path = Path(self._package_name).joinpath("cronjob.yaml")
        image_endepunkt = 'repo.adeo.no:5443/'
        self._scheduler.edit_cronjob_config(cronjob_file_path, image_endepunkt)

    def _edit_jenkins_job_config(self):
        config_file_path = Path(self._package_name).joinpath("jenkins_config.xml")
        tag_value = {"scriptPath": self._package_name + '/Jenkinsfile',
                     "projectUrl": self.github_project,
                     "url": self.github_project}
        self._scheduler.edit_jenkins_job_config(config_file_path, tag_val_map=tag_value)

    def _edit_jenkins_file(self):
        ''' Tilpasser Jenkinsfile til datapakken
        '''

        jenkinsfile_path = Path(self._package_name).joinpath("Jenkinsfile")
        tag_value = {"package_name": self._package_name,
                     "package_repo": self.github_project,
                     "package_path": self._package_name}

        self._scheduler.edit_jenkins_file(jenkinsfile_path, tag_val_map=tag_value)


    def _create_jenkins_job(self):
        ''' Tilpasser jenkins konfigurasjonsfil og setter opp ny jenkins jobb for datapakken
        '''

        jenkins_base_config_path = Path(self._package_name).joinpath("jenkins_base_config.xml")
        # Setter in github repo i jenkins_base_config.xml
        self._scheduler.edit_jenkins_file(jenkins_base_config_path, tag_val_map={"github_repo": self.github_project})
        self._scheduler.create_new_jenkins_job(jenkins_base_config_path)

        self._edit_jenkins_job_config()

        updated_jenkins_config_file_path = Path(self._package_name).joinpath("jenkins_config.xml")
        self._scheduler.update_jenkins_job(updated_jenkins_config_file_path)

    def _schedule_job(self):
        self._edit_cronjob_config()
        self._edit_jenkins_file()
        self._create_jenkins_job()

    def run(self):
        if self.args.update_schedule is None:
            update_schedule = input("Skriv inn ønsket oppdateringsschedule for datapakken "
                                                     "(format: \"<minutt> <time> <dag i måned> <måned> <ukedag>\", "
                                                     "f.eks. \"0 12 * * 2,4\" vil gi <Hver tirsdag og torsdag kl 12.00 UTC>): ")
            if not update_schedule:
                update_schedule = "* * 31 2 *"  # Default value Feb 31 (i.e. never)
        else:
            update_schedule = self.args.update_schedule

        self.settings["update_schedule"] = update_schedule

        self._print_datapipeline_config()
        res = input(f'Vil du opprette datapakken sette opp pipeline for datapakken over? [j/n] ')

        if res in {'j', 'ja', 'y', 'yes'}:
            try:
                self._schedule_job()
            except Exception:
                if os.path.exists(self.settings["package_name"]):
                    rmtree(self.settings["package_name"])
                raise Exception(f'Klarte ikke sette opp pipeline for datapakke {self.settings["package_name"]}')
        else:
            print(f'Datapakken {self.settings["package_name"]} ble ikke opprettet')
