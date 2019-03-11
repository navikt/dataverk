import requests
import json
from Cryptodome.PublicKey import RSA
from collections import Mapping
from dataverk_cli.cli.cli_utils import repo_info

GITHUB_API_URL = "https://api.github.com/repos"


class DeployKey:
    """ Checks if a deploy key pair has been set up for this datapackage, and creates a new key pair if it does not exist.
    """

    def __init__(self, settings_store: Mapping, env_store: Mapping, remote_repo_url: str):
        self._settings_store = settings_store
        self._env_store = env_store
        self._remote_repo_url = remote_repo_url
        self._remote_repo_name = repo_info.get_repo_name(remote_repo_url)
        self._remote_repo_org = repo_info.get_org_name(remote_repo_url)
        self._deploy_key_name = f'{self._remote_repo_name}-ci'
        self._setup_key()

    @property
    def name(self):
        return self._deploy_key_name

    def _setup_key(self):
        ''' Setting up deploy key. Creates new key pair if it does not exist and uploads public key to github, and
            private key to Jenkins-server

        :return: str: deploy key name
        '''
        if not self._key_exists():
            key = self._generate_deploy_key()
            self._upload_public_key(key)
            self._upload_private_key(key)

    def _key_exists(self):
        ''' Checks if it already exists a deploy key for the datapackage

        :return: bool: True if key exists, False otherwise
        '''
        res = requests.get(url=f'{GITHUB_API_URL}/{self._remote_repo_org}/'
                               f'{self._remote_repo_name}/keys',
                           headers={"Authorization": f'token {self._env_store["GH_TOKEN"]}'})
        if not res.ok:
            res.raise_for_status()

        for key in json.loads(res.content.decode("utf-8")):
            if key["title"] == self._deploy_key_name:
                return True
        return False

    @staticmethod
    def _generate_deploy_key(key_length: int=4096):
        ''' Generates a SSH key pair

        :return: SSH public/private key pair
        '''
        return RSA.generate(key_length)

    def _upload_public_key(self, key):
        ''' Uploads public key to remote github repository

        :param key: ssh key
        '''
        public_key = key.publickey().exportKey(format='OpenSSH').decode(encoding="utf-8")
        res = requests.post(url=f'{GITHUB_API_URL}/{self._remote_repo_org}/'
                                f'{self._remote_repo_name}/keys',
                            headers={"Authorization": f'token {self._env_store["GH_TOKEN"]}'},
                            data=json.dumps({"title": f'{self._deploy_key_name}', "key": public_key, "read_only": True}))
        if res.status_code != 201:
            res.raise_for_status()

    def _upload_private_key(self, key) -> None:
        ''' Uploads private key to Jenkins-server

        :param key: ssh key
        :return: None
        '''
        jenkins_crumb = requests.get(f'{self._settings_store["jenkins"]["url"]}/crumbIssuer/api/xml?xpath=concat(//crumbRequestField,":",//crumb)',
                                     auth=(self._env_store["USER_IDENT"], self._env_store["PASSWORD"])).text

        payload = self._compose_credential_payload(deploy_key_name=self._deploy_key_name, key=key)
        res = requests.post(f'{self._settings_store["jenkins"]["url"]}/credentials/store/system/domain/_/createCredentials',
                            auth=(self._env_store["USER_IDENT"], self._env_store["PASSWORD"]),
                            headers={jenkins_crumb.split(":")[0]: jenkins_crumb.split(":")[1]},
                            data=payload)
        if not res.ok:
            res.raise_for_status()

    @staticmethod
    def _compose_credential_payload(deploy_key_name: str, key):
        priv_key = key.exportKey().decode(encoding="utf-8")
        data = {
            'credentials': {
                'scope': "GLOBAL",
                'username': deploy_key_name,
                'id': deploy_key_name,
                'privateKeySource': {
                    'privateKey': priv_key,
                    'stapler-class': "com.cloudbees.jenkins.plugins.sshcredentials.impl.BasicSSHUserPrivateKey$DirectEntryPrivateKeySource"
                },
                'stapler-class': "com.cloudbees.jenkins.plugins.sshcredentials.impl.BasicSSHUserPrivateKey"
            }
        }
        return {'json': json.dumps(data), 'Submit': "OK"}
