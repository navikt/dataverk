import jenkins
from collections.abc import Mapping
from xml.etree import ElementTree
from pathlib import Path


class JenkinsJobScheduler:
    """ Implementer methoder for å håndtere skedulering av jobber for dataverk

    """

    def __init__(self, settings_store: Mapping, env_store: Mapping):
        self._settings_store = settings_store
        self._env_store = env_store

        self._jenkins_server = jenkins.Jenkins(url=self._settings_store["jenkins"]["url"],
                                               username=self._env_store['USER_IDENT'],
                                               password=self._env_store['PASSWORD'])

    def jenkins_job_exists(self):
        return self._jenkins_server.job_exists(name=self._settings_store["package_name"])

    def edit_jenkins_job_config(self, file_path: Path, tag_val_map: Mapping) -> None:
        """ Sett inn eller endre json config fil verdier

        :param file_path: Path til jenkins job config filen
        :param tag_val_map: key value mapping av tag som skal finnes og verdien som skal settes
        :return: None
        """
        xml = ElementTree.parse(file_path)
        xml_root = xml.getroot()

        for elem in xml_root.getiterator():
            if elem.tag in tag_val_map:
                elem.text = tag_val_map[elem.tag]

        xml.write(file_path)

    def create_new_jenkins_job(self, config_file_path: Path):
        ''' Setter opp ny jenkins jobb for datapakken
        '''

        xml_base = ElementTree.parse(config_file_path)
        xml_base_root = xml_base.getroot()
        xml_base_config = ElementTree.tostring(xml_base_root, encoding='utf-8', method='xml').decode()
        package_name = self._settings_store["package_name"]

        try:
            self._jenkins_server.create_job(name=package_name, config_xml=xml_base_config)
        except jenkins.JenkinsException:
            raise jenkins.JenkinsException(f"Klarte ikke sette opp jenkinsjobb for package_name({package_name})")

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
            self._jenkins_server.reconfig_job(name=package_name, config_xml=xml_base_config)
        except jenkins.JenkinsException:
            raise jenkins.JenkinsException(f"Klarte ikke rekonfigurere jenkinsjobb for package_name({package_name})")

    def delete_jenkins_job(self):
        self._jenkins_server.delete_job(self._settings_store["package_name"])
