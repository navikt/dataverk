# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import json
from dataverk.utils.windows_safe_tempdir import WindowsSafeTempDirectory
from dataverk_cli.deploy.deploy_key import DeployKey
from dataverk_cli.cli.cli_utils import repo_info
from git import Repo

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
    "PASSWORD": "password",
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
        pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """
    def test__compose_credential_payload(self):
        key = DeployKey._generate_deploy_key(key_length=1024)
        priv_key = key.exportKey().decode(encoding="utf-8")

        expected_payload = JENKINS_CREDENTIAL_WRAPPER_TEMPLATE
        expected_credentials = JENKINS_CREDENTIAL_TEMPLATE
        expected_credentials["credentials"]["privateKeySource"]["privateKey"] = priv_key
        expected_payload["json"] = json.dumps(expected_credentials)

        credential_payload = DeployKey._compose_credential_payload(deploy_key_name="repo-ci", key=key)

        self.assertEqual(credential_payload, expected_payload)
