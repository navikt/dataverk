import jenkins
import os
import yaml

from dataverk_cli.dataverk_base import DataverkBase
from dataverk.context.env_store import EnvStore
from dataverk.utils.validators import validate_cronjob_schedule
from xml.etree import ElementTree
from shutil import rmtree


class DataverkSchedule(DataverkBase):

    def __init__(self, args, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

        self._args = args
        self._jenkins_server = jenkins.Jenkins(url=self.settings["jenkins"]["url"],
                                               username=self.envs['USER_IDENT'],
                                               password=self.envs['PASSWORD'])

    def run(self):
        self._check_if_datapackage_exists_in_remote_repo()

        self._set_update_schedule()
        self._print_datapipeline_config()
        res = input(f'Vil du sette opp pipeline for datapakken over? [j/n] ')

        if res in {'j', 'ja', 'y', 'yes'}:
            try:
                self._schedule_job()
            except Exception:
                if os.path.exists(self.settings["package_name"]):
                    rmtree(self.settings["package_name"])
                raise Exception(f'Klarte ikke sette opp pipeline for datapakke {self.settings["package_name"]}')
        else:
            print(f'Pipeline for datapakke {self.settings["package_name"]} ble ikke opprettet')

    def _set_update_schedule(self):
        if self._args.update_schedule is None:
            update_schedule = input("Skriv inn ønsket oppdateringsschedule for datapakken "
                                    "(format: \"<minutt> <time> <dag i måned> <måned> <ukedag>\", "
                                    "f.eks. \"0 12 * * 2,4\" vil gi <Hver tirsdag og torsdag kl 12.00 UTC>): ")
            if not update_schedule:
                update_schedule = "* * 31 2 *"  # Default value Feb 31 (i.e. never)
        else:
            update_schedule = self._args.update_schedule

        self.settings["update_schedule"] = update_schedule

        #validate_cronjob_schedule(schedule)

    def _check_if_datapackage_exists_in_remote_repo(self):


    def _schedule_job(self):
        self._edit_cronjob_config()
        self._create_jenkins_job()

    def _edit_cronjob_config(self):
        ''' Tilpasser cronjob config fil til datapakken
        '''

        try:
            with open(os.path.join(self.settings["package_name"], 'cronjob.yaml'), 'r') as yamlfile:
                cronjob_config = yaml.load(yamlfile)
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil')

        cronjob_config['metadata']['name'] = self.settings["package_name"]
        cronjob_config['metadata']['namespace'] = self.settings["nais_namespace"]

        cronjob_config['spec']['schedule'] = self.settings["update_schedule"]
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['name'] = self.settings["package_name"] + '-cronjob'
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['image'] = 'repo.adeo.no:5443/' + self.settings["package_name"]

        try:
            with open(os.path.join(self.settings["package_name"], 'cronjob.yaml'), 'w') as yamlfile:
                yamlfile.write(yaml.dump(cronjob_config, default_flow_style=False))
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil')

    def _create_jenkins_job(self):
        ''' Tilpasser jenkins konfigurasjonsfil og setter opp ny jenkins jobb for datapakken
        '''

        self._edit_jenkins_job_config()

        xml = ElementTree.parse(os.path.join(self.settings["package_name"], 'jenkins_config.xml'))
        xml_root = xml.getroot()
        xml_config = ElementTree.tostring(xml_root, encoding='utf-8', method='xml').decode()

        if self._jenkins_server.job_exists(self.settings["package_name"]):
            self._jenkins_server.reconfig_job(name=self.settings["package_name"], config_xml=xml_config)
        else:
            self._jenkins_server.create_job(name=self.settings["package_name"], config_xml=xml_config)

    def _edit_jenkins_job_config(self):
        xml = ElementTree.parse(os.path.join(self.settings["package_name"], 'jenkins_config.xml'))
        xml_root = xml.getroot()

        for elem in xml_root.getiterator():
            if elem.tag == 'scriptPath':
                elem.text = self.settings["package_name"] + '/Jenkinsfile'
            elif elem.tag == 'projectUrl':
                elem.text = self.github_project
            elif elem.tag == 'url':
                elem.text = self.github_project

        xml.write(os.path.join(self.settings["package_name"], 'jenkins_config.xml'))
