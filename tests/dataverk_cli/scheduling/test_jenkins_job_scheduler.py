# -*- coding: utf-8 -*-
# Import statements
# =================
import os
import unittest
import yaml
from tempfile import TemporaryDirectory
from pathlib import Path
from dataverk_cli.scheduling.jenkins_job_scheduler import JenkinsJobScheduler
from git import Repo
from dataverk_cli.cli.cli_utils.repo_info import get_remote_url


# Common input parameters
# =======================
SETTINGS_TEMPLATE = {
  "package_name": "package_name",
  "nais_namespace": "dataverk",
  "image_endpoint": "mitt.image.endpoint.no:1234/",

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



# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self._local_repo_path = self.create_tmp_repo()

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
        scheduler = JenkinsJobScheduler(settings_store=SETTINGS_TEMPLATE, env_store=ENV_STORE_TEMPLATE, repo_path=self._local_repo_path.name)

        yamlpath_template = Path(os.path.dirname(__file__)).joinpath("static").joinpath("test_cronjob.yaml")

        with yamlpath_template.open(mode="r", encoding="utf-8") as yamlfile:
            yamldata = yamlfile.read()

        yamlpath = Path(self._local_repo_path.name).joinpath("cronjob.yaml")

        with yamlpath.open(mode="w", encoding="utf-8") as yamlfile:
            yamlfile.write(yaml.dump(yamldata, default_flow_style=False))

        scheduler._edit_cronjob_config()
        correctyaml = Path(os.path.dirname(__file__)).joinpath("static").joinpath("test_cronjob2.yaml")

        with correctyaml.open(mode="w", encoding="utf-8") as yamlfile:
            correct = yamlfile.read()



        #self.assertEqual()
        # cronjob_yaml = self.create_temp_file(file_name='cronjob.yaml')
        # with cronjob_yaml.open()