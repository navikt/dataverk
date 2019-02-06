import jenkins
import yaml
import requests
import json
from Cryptodome.PublicKey import RSA
from collections.abc import Mapping
from xml.etree import ElementTree
from pathlib import Path
from string import Template
from .scheduler import Scheduler
from dataverk_cli.cli.cli_utils.repo_info import get_org_name, get_repo_name


GITHUB_API_URL = "https://api.github.com/repos"


class JenkinsJobScheduler(Scheduler):
    """ Implementer methoder for å håndtere skedulering av jobber for dataverk

    """

    def __init__(self, settings_store: Mapping, env_store: Mapping, repo_path: str="."):
        super().__init__(settings_store, env_store, repo_path=repo_path)

        self._jenkins_server = jenkins.Jenkins(url=self._settings_store["jenkins"]["url"],
                                               username=self._env_store['USER_IDENT'],
                                               password=self._env_store['PASSWORD'])
        self._package_name = settings_store["package_name"]
        self._deploy_key_name = f'{get_repo_name(self._github_project)}-ci'

    def job_exist(self):
        return self._jenkins_server.job_exists(name=self._package_name)

    def configure_job(self):
        self._edit_jenkins_job_config()
        self._edit_cronjob_config()
        self._setup_deploy_key()

        config_file_path = Path('jenkins_config.xml')
        if self._jenkins_server.job_exists(name=self._package_name):
            self._update_jenkins_job(config_file_path=config_file_path)
        else:
            self._create_new_jenkins_job(config_file_path=config_file_path)

    def delete_job(self):
        self._jenkins_server.delete_job(self._package_name)

    def _edit_jenkins_job_config(self, config_file_path: Path=Path("jenkins_config.xml")):
        tag_value = {"scriptPath": 'Jenkinsfile',
                     "projectUrl": self._github_project,
                     "url": self._github_project_ssh,
                     "credentialsId": self._deploy_key_name}

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

    def _setup_deploy_key(self) -> None:
        ''' Setter opp deploy key mot github for jenkins-server

        :return: None
        '''
        if not self._key_exists():
            key = self._generate_deploy_key()
            self._upload_public_key(key=key)
            self._upload_private_key(key=key)

    def _key_exists(self):
        ''' Sjekker om det allerede eksisterer en deploy key på github for dette repoet

        :return: True hvis nøkkelen eksisterer, False ellers
        '''
        res = requests.get(url=f'{GITHUB_API_URL}/{get_org_name(self._github_project)}/{get_repo_name(self._github_project)}/keys',
                           headers={"Authorization": f'token {self._env_store["GH_TOKEN"]}'})
        if not res.ok:
            res.raise_for_status()

        for key in json.loads(res.content.decode("utf-8")):
            if key["title"] == self._deploy_key_name:
                return True

        return False

    def _generate_deploy_key(self, key_length: int=4096):
        ''' Genererer et SSH nøkkelpar

        :return: SSH public/private nøkkelpar
        '''
        return RSA.generate(key_length)

    def _upload_public_key(self, key) -> None:
        ''' Laster opp public nøkkelen til repositoriet på github

        :param key: ssh nøkkel
        :return: None
        '''
        public_key = key.publickey().exportKey(format='OpenSSH').decode(encoding="utf-8")
        res = requests.post(url=f'{GITHUB_API_URL}/{get_org_name(self._github_project)}/{get_repo_name(self._github_project)}/keys',
                            headers={"Authorization": f'token {self._env_store["GH_TOKEN"]}'},
                            data=json.dumps({"title": f'{self._deploy_key_name}', "key": public_key, "read_only": True}))
        if res.status_code != 201:
            res.raise_for_status()

    def _upload_private_key(self, key) -> None:
        ''' Laster opp private nøkkelen til jenkins-serveren

        :param key: ssh nøkkel
        :return: None
        '''
        jenkins_crumb = requests.get(f'{self._settings_store["jenkins"]["url"]}/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,":",//crumb)',
                                     auth=(self._env_store["USER_IDENT"], self._env_store["PASSWORD"])).text

        payload = self._compose_credential_payload(key=key)

        res = requests.post(f'{self._settings_store["jenkins"]["url"]}/credentials/store/system/domain/_/createCredentials',
                            auth=(self._env_store["USER_IDENT"], self._env_store["PASSWORD"]),
                            headers={jenkins_crumb.split(":")[0]: jenkins_crumb.split(":")[1]},
                            data=payload)

        if not res.ok:
            res.raise_for_status()

    def _compose_credential_payload(self, key):
        priv_key = key.exportKey().decode(encoding="utf-8")

        data = {
            'credentials': {
                'scope': "GLOBAL",
                'username': self._deploy_key_name,
                'id': self._deploy_key_name,
                'privateKeySource': {
                    'privateKey': priv_key,
                    'stapler-class': "com.cloudbees.jenkins.plugins.sshcredentials.impl.BasicSSHUserPrivateKey$DirectEntryPrivateKeySource"
                },
                'stapler-class': "com.cloudbees.jenkins.plugins.sshcredentials.impl.BasicSSHUserPrivateKey"
            }
        }

        return {'json': json.dumps(data), 'Submit': "OK"}
