import jenkins
import os
import json
import yaml

from dataverk_cli.dataverk_base import DataverkBase
from dataverk.utils.env_store import EnvStore
from xml.etree import ElementTree
from string import Template
from shutil import rmtree


class DataverkSchedule(DataverkBase):

    def __init__(self, args, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

        self.args = args
        self.jenkins_server = jenkins.Jenkins(self.settings["jenkins"]["url"],
                                              username=self.envs['USER_IDENT'],
                                              password=self.envs['PASSWORD'])

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

    def _edit_jenkins_file(self):
        ''' Tilpasser Jenkinsfile til datapakken
        '''

        try:
            with open(os.path.join(self.settings["package_name"], 'Jenkinsfile'), 'r') as jenkinsfile:
                jenkins_config = jenkinsfile.read()
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile')

        template = Template(jenkins_config)
        jenkins_config = template.safe_substitute(package_name=self.settings["package_name"],
                                                  package_repo=self.github_project,
                                                  package_path=self.settings["package_name"])

        try:
            with open(os.path.join(self.settings["package_name"], 'Jenkinsfile'), 'w') as jenkinsfile:
                jenkinsfile.write(jenkins_config)
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile')

    def _create_jenkins_job(self):
        ''' Tilpasser jenkins konfigurasjonsfil og setter opp ny jenkins jobb for datapakken
        '''

        try:
            with open(os.path.join(self.settings["package_name"], 'jenkins_base_config.xml'), 'r') as jenkins_config:
                config = jenkins_config.read()
        except OSError:
            raise OSError(f'Finner ikke jenkins_base_config.xml')

        template = Template(config)
        jenkins_config = template.safe_substitute(github_repo=self.github_project)

        try:
            with open(os.path.join(self.settings["package_name"], 'jenkins_base_config.xml'), 'w') as jenkins_base_config:
                jenkins_base_config.write(jenkins_config)
        except OSError:
            raise OSError(f'Finner ikke jenkins_base_config.xml')

        xml_base = ElementTree.parse(os.path.join(self.settings["package_name"], 'jenkins_base_config.xml'))
        xml_base_root = xml_base.getroot()
        xml_base_config = ElementTree.tostring(xml_base_root, encoding='utf-8', method='xml').decode()

        try:
            self.jenkins_server.create_job(name=self.settings["package_name"], config_xml=xml_base_config)
        except jenkins.JenkinsException:
            raise jenkins.JenkinsException("Klarte ikke sette opp jenkinsjobb")

        self.jenkins_server.build_job(name=self.settings["package_name"])

        self._edit_jenkins_job_config()

        xml = ElementTree.parse(os.path.join(self.settings["package_name"], 'jenkins_config.xml'))
        xml_root = xml.getroot()
        xml_config = ElementTree.tostring(xml_root, encoding='utf-8', method='xml').decode()

        self.jenkins_server.reconfig_job(self.settings["package_name"], xml_config)

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
