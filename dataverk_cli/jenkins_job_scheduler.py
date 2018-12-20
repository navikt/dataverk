import jenkins
from collections.abc import Mapping
from xml.etree import ElementTree
import os
from pathlib import Path
from string import Template
import yaml


class JenkinsJobScheduler:
    """ Implementer methoder for å håndtere skedulering av jobber for dataverk

    """

    def __init__(self, settings_store: Mapping, env_store: Mapping):
        self._settings_store = settings_store
        self._env_store = env_store

        self.jenkins_server = jenkins.Jenkins(self._settings_store["jenkins"]["url"],
                                              username=self._env_store['USER_IDENT'],
                                              password=self._env_store['PASSWORD'])


    def edit_jenkins_job_config(self, file_path: Path, tag_val_map: Mapping) -> None:
        """ Sett inn eller endre json config fil verdier

        :param file_path: Path til jenkins job config filen
        :param tag_val_map: key value mapping av tag som skal finnes og verdien som skal settes
        :return: None
        """
        xml = ElementTree.parse(file_path)
        xml_root = xml.getroot()

        for elem in xml_root.getiterator():
            if elem in tag_val_map:
                elem.text = tag_val_map[elem]

        xml.write(file_path)

    def edit_jenkins_file(self, file_path: Path, tag_val_map: Mapping) -> None:
        """

        :param file_path: Path til Jenkinsfile
        :param tag_val_map: key value mapping av tag som skal finnes og verdien som skal settes
        :return: None
        """

        try:
            with file_path.open('r') as jenkinsfile:
                jenkins_config = jenkinsfile.read()
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile på Path{file_path})')

        template = Template(jenkins_config)
        jenkins_config = template.safe_substitute(**tag_val_map)

        try:
            with file_path.open('w') as jenkinsfile:
                jenkinsfile.write(jenkins_config)
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile på Path{file_path})')

    def create_new_jenkins_job(self, config_file_path: Path):
        ''' Setter opp ny jenkins jobb for datapakken
        '''

        xml_base = ElementTree.parse(config_file_path)
        xml_base_root = xml_base.getroot()
        xml_base_config = ElementTree.tostring(xml_base_root, encoding='utf-8', method='xml').decode()
        package_name = self._settings_store["package_name"]

        try:
            self.jenkins_server.create_job(name=package_name, config_xml=xml_base_config)
        except jenkins.JenkinsException:
            raise jenkins.JenkinsException(f"Klarte ikke sette opp jenkinsjobb for package_name({package_name})")

        self.jenkins_server.build_job(name=package_name)

    def update_jenkins_job(self, config_file_path: Path) -> None:
        """ Oppdaterer eksisterende Jenkins job

        :param config_file_path: Path til config fil
        :return: None
        """

        xml_base = ElementTree.parse(config_file_path)
        xml_base_root = xml_base.getroot()
        xml_base_config = ElementTree.tostring(xml_base_root, encoding='utf-8', method='xml').decode()
        package_name = self._settings_store["package_name"]

        try:
            self.jenkins_server.reconfig_job(name=package_name, config_xml=xml_base_config)
        except jenkins.JenkinsException:
            raise jenkins.JenkinsException(f"Klarte ikke rekonfigurere jenkinsjobb for package_name({package_name})")

        self.jenkins_server.build_job(name=package_name)

    def delete_jenkins_job(self):
        self.jenkins_server.delete_job(self._settings_store["package_name"])

    def edit_cronjob_config(self, file_path: Path, image_endepunkt: str) -> None:
        """

        :param file_path: Path til cronjob YAML fil
        :param image_endepunkt: endepunkt for image
        :return: None
        """

        try:
            with file_path.open('r') as yamlfile:
                cronjob_config = yaml.load(yamlfile)
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil på Path({file_path})')

        cronjob_config['metadata']['name'] = self._settings_store["package_name"]
        cronjob_config['metadata']['namespace'] = self._settings_store["nais_namespace"]

        cronjob_config['spec']['schedule'] = self._settings_store["update_schedule"]
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['name'] = self._settings_store["package_name"] + '-cronjob'
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['image'] = image_endepunkt + self._settings_store["package_name"]

        try:
            with file_path.open('w') as yamlfile:
                yamlfile.write(yaml.dump(cronjob_config, default_flow_style=False))
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil på Path({file_path})')
