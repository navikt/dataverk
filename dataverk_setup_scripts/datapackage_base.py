import jenkins
import os
import json
import yaml

from . import settings_loader, settings_creator
from dataverk.utils.env_store import EnvStore
from abc import ABC
from enum import Enum
from shutil import rmtree
from string import Template
from xml.etree import ElementTree


class Action(Enum):
    CREATE = 1
    UPDATE = 2
    DELETE = 3


class BaseDataPackage(ABC):
    ''' Abstrakt baseklasse for dataverk scripts.
    '''

    def __init__(self, settings: dict, envs: EnvStore):
        self._verify_class_init_arguments(settings, envs)

        self.settings = settings
        self.github_project = self._get_github_url()
        self.envs = envs

        self.jenkins_server = jenkins.Jenkins(self.settings["jenkins"]["url"],
                                              username=self.envs['USER_IDENT'],
                                              password=self.envs['PASSWORD'])

    def _verify_class_init_arguments(self, settings, envs):
        if not isinstance(settings, dict):
            raise TypeError(f'settings parameter must be of type dict')

        if not isinstance(envs, EnvStore):
            raise TypeError(f'envs parameter must be of type EnvStore')

    def _folder_exists_in_repo(self, name: str):
        ''' Sjekk på om det finnes en mappe i repoet med samme navn som ønsket pakkenavn

        :return: boolean: "True" hvis pakkenavn allerede er tatt i bruk, "False" ellers
        '''

        for filename in os.listdir(os.getcwd()):
            if name == filename:
                return True

        return False

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

    def _print_datapackage_config(self):
        print("\n-------------Datapakke-----------------------------" +
              "\nDatapakkenavn: " + self.settings["package_name"] +
              "\ngithub repo: " + self.github_project +
              "\ncronjob schedule: " + self.settings["update_schedule"] +
              "\nNAIS namespace: " + self.settings["nais_namespace"] +
              "\n-------------------------------------------------\n")

    def _get_github_url(self):
        return os.popen('git config --get remote.origin.url').read().strip()


def create_settings_dict(args, envs: EnvStore):
    default_settings_path = ""
    try:
        default_settings_loader = settings_loader.GitSettingsLoader(url=envs["SETTINGS_REPO"])
        default_settings_path = default_settings_loader.download_to('.')

        settings_creator_object = settings_creator.get_settings_creator(args=args,
                                                                        default_settings_path=str(default_settings_path))
        settings = settings_creator_object.create_settings()
    finally:
        if os.path.exists(str(default_settings_path)):
            rmtree(str(default_settings_path))

    return settings


def get_settings_dict(package_name):
    try:
        with open(os.path.join(package_name, "settings.json"), 'r') as settings_file:
            settings = json.load(settings_file)
    except OSError:
        raise OSError(f'Settings file missing in datapackage {package_name}')
    return settings
