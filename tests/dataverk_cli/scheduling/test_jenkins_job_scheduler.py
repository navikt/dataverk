# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import os
import json
import yaml
from shutil import copyfile
from tempfile import TemporaryDirectory
from pathlib import Path
from dataverk_cli.scheduling.jenkins_job_scheduler import JenkinsJobScheduler
from git import Repo
from importlib_resources import read_text, path

# Common input parameters
# =======================
SETTINGS_TEMPLATE = {
  "package_name": "package_name",
  "nais_namespace": "dataverk",
  "image_endpoint": "mitt.image.endpoint.no:1234/",
  "update_schedule": "* * 31 2 *",

  "vault": {
    "auth_uri": "https://vault.auth.uri.com",
    "secrets_uri": "https://vault.secrets.uri.com",
    "vks_auth_path": "/path/to/auth",
    "vks_kv_path": "/path/to/kv",
    "vks_vault_role": "role_name",
    "service_account": "service_account_name"
  },

  "jenkins": {
      "url": "https//jenkins.server.com:1234"
  }

}

ENV_STORE_TEMPLATE = {
    "USER_IDENT": "my-ident",
    "PASSWORD": "password"
}

JENKINS_CREDENTIAL_WRAPPER_TEMPLATE = {
    "json": "",
    "Submit": "OK"
}

JENKINS_CREDENTIAL_TEMPLATE = {
    'credentials': {
        'scope': "GLOBAL",
        'username': "repo-ci",
        'id': "repo-ci",
        'privateKeySource': {
            'privateKey': "",
            'stapler-class': "com.cloudbees.jenkins.plugins.sshcredentials.impl.BasicSSHUserPrivateKey$DirectEntryPrivateKeySource"
        },
        'stapler-class': "com.cloudbees.jenkins.plugins.sshcredentials.impl.BasicSSHUserPrivateKey"
     }
}



# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self._local_repo_path = self.create_tmp_repo()

        self._scheduler = JenkinsJobScheduler(settings_store=SETTINGS_TEMPLATE,
                                              env_store=ENV_STORE_TEMPLATE,
                                              repo_path=self._local_repo_path.name)

    def tearDown(self):
        self._local_repo_path.cleanup()

    def create_tmp_repo(self) -> TemporaryDirectory:
        tmpdir = TemporaryDirectory()
        repo = Repo.init(tmpdir.name)
        repo.create_remote("origin", url="https://my/remote/repo.git")
        repo.index.commit("initial commit")
        return tmpdir

# Test classes
# ============


class MethodsInput(Base):
    """
    Tests methods which take input parameters

    Tests include: passing invalid input, etc.
    """
    pass


class MethodsReturnType(Base):
    """
    Tests methods' output types
    """
    pass


class MethodsReturnUnits(Base):
    """
    Tests methods' output units where applicable
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """
    def test__edit_cronjob_config(self):
        yamlpath = Path(self._local_repo_path.name).joinpath("cronjob.yaml")
        yaml_reference = yaml.load(read_text('tests.static', 'test_cronjob_reference.yaml'))
        with path('tests.static', 'test_cronjob_template.yaml') as yaml_template:
            copyfile(yaml_template, str(yamlpath))

        self._scheduler._edit_cronjob_config(yaml_path=yamlpath)

        with yamlpath.open(mode="r", encoding="utf-8") as yamlfile:
            edited_yaml = yaml.load(yamlfile.read())

        self.assertEqual(edited_yaml, yaml_reference)

    def test__edit_jenkins_job_config(self):
        configpath = Path(self._local_repo_path.name).joinpath("jenkins_config.xml")
        jenkins_config_ref = read_text('tests.static', 'test_jenkins_config_reference.xml')
        with path('tests.static', 'test_jenkins_config_template.xml') as jenkins_config:
            copyfile(jenkins_config, str(configpath))

        self._scheduler._edit_jenkins_job_config(config_file_path=configpath)

        with configpath.open(mode="r", encoding="utf-8") as jenkins_config:
            edited_jenkins_config = jenkins_config.read()

        self.assertEqual(edited_jenkins_config, jenkins_config_ref)

    def test__compose_credential_payload(self):
        key = self._scheduler._generate_deploy_key(key_length=1024)
        priv_key = key.exportKey().decode(encoding="utf-8")

        expected_payload = JENKINS_CREDENTIAL_WRAPPER_TEMPLATE
        expected_credentials = JENKINS_CREDENTIAL_TEMPLATE
        expected_credentials["credentials"]["privateKeySource"]["privateKey"] = priv_key
        expected_payload["json"] = json.dumps(expected_credentials)

        credential_payload = self._scheduler._compose_credential_payload(key=key)

        self.assertEqual(credential_payload, expected_payload)
