import jenkins
import os
import json
import yaml

from string import Template
from shutil import rmtree
from distutils.dir_util import copy_tree
from xml.etree import ElementTree
from . import settings_loader
from .datapackage import DataPackage
from dataverk.utils.env_store import EnvStore


class CreateDataPackage(DataPackage):
    ''' Klasse for å opprette ny datapakke i et eksisterende repository for datapakker/datasett
    '''

    def __init__(self, settings: dict, envs: EnvStore):
        super().__init__(settings=settings, envs=envs)

    def _create_datapackage_local(self):
        ''' Lager mappestrukturen for datapakken lokalt og henter template filer
        '''

        os.mkdir(self.settings["package_name"])

        templates_path = ""

        try:
            templates_loader = settings_loader.GitSettingsLoader(url=self.envs["TEMPLATES_REPO"])
            templates_path = templates_loader.download_to(".")
            copy_tree(os.path.join(str(templates_path), 'file_templates'), self.settings["package_name"])
        except OSError:
            raise OSError(f'Templates mappe eksisterer ikke.')
        finally:
            if os.path.exists(str(templates_path)):
                rmtree(str(templates_path))

    def _create_settings_file(self, path: str):
        try:
            with open(os.path.join(path, 'settings.json'), 'w') as settings_file:
                json.dump(self.settings, settings_file, indent=2)
        except OSError:
            raise OSError(f'Klarte ikke å generere settings fil for datapakke')

    def _edit_package_metadata(self):
        '''  Tilpasser metadata fil til datapakken

        '''

        try:
            with open(os.path.join(self.settings["package_name"], 'METADATA.json'), 'r') as metadatafile:
                package_metadata = json.load(metadatafile)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil')

        package_metadata['Datapakke_navn'] = self.settings["package_name"]
        package_metadata['Bucket_navn'] = 'nav-opendata'

        try:
            with open(os.path.join(self.settings["package_name"], 'METADATA.json'), 'w') as metadatafile:
                json.dump(package_metadata, metadatafile, indent=2)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil')

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

        xml_config = ElementTree.tostring(xml_root, encoding='utf-8', method='xml').decode()

        self.jenkins_server.reconfig_job(self.settings["package_name"], xml_config)

    def _create(self):
        ''' Oppretter ny datapakke med ønsket konfigurasjon

        '''

        self._create_datapackage_local()
        self._create_settings_file(path=self.settings["package_name"])
        self._edit_package_metadata()
        self._edit_cronjob_config()
        self._edit_jenkins_file()

        self._create_jenkins_job()

        print(f'Datapakken {self.settings["package_name"]} er opprettet')

    def run(self):
        ''' Entrypoint for dataverk create

        '''

        if self._folder_exists_in_repo(self.settings["package_name"]):
            raise NameError(f'En mappe med navn {self.settings["package_name"]} '
                            f'eksisterer allerede i repo {self.github_project}')

        if self.jenkins_server.job_exists(name=self.settings["package_name"]):
            raise NameError(f'En jobb med navn {self.settings["package_name"]} '
                            f'eksisterer allerede på jenkins serveren. Datapakkenavn må være unikt.')

        print(f'Opprettelse av ny datapakke i {self.github_project}')

        self._print_datapackage_config()
        res = input(f'Vil du opprette datapakken med konfigurasjonen over? [j/n] ')

        if res in {'j', 'ja', 'y', 'yes'}:
            try:
                self._create()
            except Exception:
                if os.path.exists(self.settings["package_name"]):
                    rmtree(self.settings["package_name"])
                raise Exception(f'Klarte ikke generere datapakken {self.settings["package_name"]}')
        else:
            print(f'Datapakken {self.settings["package_name"]} ble ikke opprettet')
