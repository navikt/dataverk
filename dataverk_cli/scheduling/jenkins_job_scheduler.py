import jenkins
import yaml
from collections.abc import Mapping
from xml.etree import ElementTree
from pathlib import Path
from .scheduler import Scheduler
from dataverk_cli.scheduling.deploy_key import DeployKey


class JenkinsJobScheduler(Scheduler):
    """ Implementer methoder for å håndtere skedulering av jobber for dataverk

    """

    def __init__(self, settings_store: Mapping, env_store: Mapping, remote_repo_url: str, deploy_key: DeployKey):
        super().__init__(settings_store, env_store, remote_repo_url)

        self._jenkins_server = jenkins.Jenkins(url=self._settings_store["jenkins"]["url"],
                                               username=self._env_store['USER_IDENT'],
                                               password=self._env_store['PASSWORD'])
        self._package_name = settings_store["package_name"]
        self._deploy_key = deploy_key

    def job_exist(self):
        return self._jenkins_server.job_exists(name=self._package_name)

    def configure_job(self):
        self._edit_jenkins_job_config()
        self._edit_cronjob_config()

        config_file_path = Path('jenkins_config.xml')
        if self._jenkins_server.job_exists(name=self._package_name):
            self._update_jenkins_job(config_file_path=config_file_path)
        else:
            self._create_new_jenkins_job(config_file_path=config_file_path)

    def delete_job(self):
        self._delete_cronjob()

        try:
            self._jenkins_server.delete_job(self._package_name)
        except jenkins.NotFoundException:
            raise UserWarning(f'No job with name {self._package_name} exists on jenkinsserver')


    def _edit_jenkins_job_config(self, config_file_path: Path=Path("jenkins_config.xml")):
        tag_value = {"scriptPath": 'Jenkinsfile',
                     "projectUrl": self._github_project,
                     "url": self._github_project_ssh,
                     "credentialsId": self._deploy_key.name}

        xml = ElementTree.parse(config_file_path)
        xml_root = xml.getroot()

        for elem in xml_root.getiterator():
            if elem.tag in tag_value:
                elem.text = tag_value[elem.tag]

        xml.write(config_file_path)

    def _create_new_jenkins_job(self, config_file_path: Path):
        ''' Setter opp ny jenkins jobb for datapakken
        '''

        xml_base_config = self._read_xml_file(config_file_path=config_file_path)
        package_name = self._settings_store["package_name"]

        try:
            self._jenkins_server.create_job(name=package_name, config_xml=xml_base_config)
        except jenkins.JenkinsException:
            raise jenkins.JenkinsException(f"Klarte ikke sette opp jenkinsjobb for package_name({package_name})")

    def _update_jenkins_job(self, config_file_path: Path) -> None:
        """ Oppdaterer eksisterende Jenkins job

        :param config_file_path: Path til config fil
        :return: None
        """

        xml_base_config = self._read_xml_file(config_file_path=config_file_path)
        package_name = self._settings_store["package_name"]

        try:
            self._jenkins_server.reconfig_job(name=package_name, config_xml=xml_base_config)
        except jenkins.JenkinsException:
            raise jenkins.JenkinsException(f"Klarte ikke rekonfigurere jenkinsjobb for package_name({package_name})")

    def _read_xml_file(self, config_file_path: Path):
        xml_base = ElementTree.parse(config_file_path)
        xml_base_root = xml_base.getroot()

        return ElementTree.tostring(xml_base_root, encoding='utf-8', method='xml').decode()

    def _edit_cronjob_config(self, yaml_path: Path=Path('cronjob.yaml')) -> None:
        """
        :return: None
        """

        cronjob_file_path = yaml_path

        try:
            with cronjob_file_path.open('r') as yamlfile:
                cronjob_config = yaml.load(yamlfile)
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil på Path({cronjob_file_path})')

        cronjob_config['metadata']['name'] = self._package_name
        cronjob_config['metadata']['namespace'] = self._settings_store["nais_namespace"]
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['name'] = self._package_name + '-cronjob'
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['image'] = self._settings_store["image_endpoint"] + self._package_name
        cronjob_config['spec']['schedule'] = self._settings_store["update_schedule"]
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['initContainers'][0]["env"][1]['value'] = self._settings_store["vault"]["vks_auth_path"]
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['initContainers'][0]["env"][2]['value'] = self._settings_store["vault"]["vks_kv_path"]
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['initContainers'][0]["env"][3]['value'] = self._package_name
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['serviceAccount'] = self._package_name
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['serviceAccountName'] = self._package_name

        try:
            with cronjob_file_path.open('w') as yamlfile:
                yamlfile.write(yaml.dump(cronjob_config, default_flow_style=False))
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil på Path({cronjob_file_path})')

    def _delete_cronjob(self):
        self._jenkins_server.build_job(name="remove-cronjob", parameters={"PACKAGE_NAME": self._package_name})
