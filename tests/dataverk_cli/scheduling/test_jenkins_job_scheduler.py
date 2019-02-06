# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import json
import yaml
from xml.etree import ElementTree
from tempfile import TemporaryDirectory
from pathlib import Path
from dataverk_cli.scheduling.jenkins_job_scheduler import JenkinsJobScheduler
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
        self._local_repo, self._local_repo_path = self.create_tmp_repo()

        self._scheduler = JenkinsJobScheduler(settings_store=SETTINGS_TEMPLATE,
                                              env_store=ENV_STORE_TEMPLATE,
                                              repo_path=self._local_repo_path.name)

    def tearDown(self):
        self._local_repo.close()
        self._local_repo_path.cleanup()

    def create_tmp_repo(self) -> (Repo, TemporaryDirectory):
        tmpdir = TemporaryDirectory()
        repo = Repo.init(tmpdir.name)
        repo.create_remote("origin", url="https://my/remote/repo.git")
        repo.index.commit("initial commit")
        return repo, tmpdir

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
        yaml_reference = yaml.load(CRONJOB_YAML_REFERENCE)

        with yamlpath.open('w') as yamlfile:
            yamlfile.write(CRONJOB_YAML_TEMPLATE)

        self._scheduler._edit_cronjob_config(yaml_path=yamlpath)

        with yamlpath.open(mode="r", encoding="utf-8") as yamlfile:
            edited_yaml = yaml.load(yamlfile.read())

        self.assertEqual(edited_yaml, yaml_reference)

    def test__edit_jenkins_job_config(self):
        configpath = Path(self._local_repo_path.name).joinpath("jenkins_config.xml")
        xml_root = ElementTree.fromstring(JENKINS_CONFIG_REFERENCE)
        jenkins_config_ref = ElementTree.tostring(xml_root).decode("utf-8")

        with configpath.open('w') as configfile:
            configfile.write(JENKINS_CONFIG_TEMPLATE)

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

CRONJOB_YAML_TEMPLATE = '''
apiVersion: batch/v1beta1
kind: CronJob
metadata:
  name: dataverk-job
  namespace: namespace
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - image: path
            name: dataverk-cronjob
            volumeMounts:
            - mountPath: /mount/path
              name: vault-secrets
          initContainers:
          - env:
            - name: VKS_VAULT_ADDR
              value: https://vault.auth.uri.com
            - name: VKS_AUTH_PATH
              value: /
            - name: VKS_KV_PATH
              value: /
            - name: VKS_VAULT_ROLE
              value: dataverk
            - name: VKS_SECRET_DEST_PATH
              value: /
            image: my-init-container-image
            name: vks
            volumeMounts:
            - mountPath: /mount/path
              name: vault-secrets
          restartPolicy: Never
          serviceAccount: dataverk
          serviceAccountName: dataverk
          volumes:
          - emptyDir:
              medium: Memory
            name: vault-secrets
  schedule: ''
'''

CRONJOB_YAML_REFERENCE = '''
apiVersion: batch/v1beta1
kind: CronJob
metadata:
  name: package_name
  namespace: dataverk
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - image: mitt.image.endpoint.no:1234/package_name
            name: package_name-cronjob
            volumeMounts:
            - mountPath: /mount/path
              name: vault-secrets
          initContainers:
          - env:
            - name: VKS_VAULT_ADDR
              value: https://vault.auth.uri.com
            - name: VKS_AUTH_PATH
              value: /path/to/auth
            - name: VKS_KV_PATH
              value: /path/to/kv
            - name: VKS_VAULT_ROLE
              value: package_name
            - name: VKS_SECRET_DEST_PATH
              value: /
            image: my-init-container-image
            name: vks
            volumeMounts:
            - mountPath: /mount/path
              name: vault-secrets
          restartPolicy: Never
          serviceAccount: package_name
          serviceAccountName: package_name
          volumes:
          - emptyDir:
              medium: Memory
            name: vault-secrets
  schedule: '* * 31 2 *'
'''

JENKINS_CONFIG_TEMPLATE = '''
<flow-definition plugin="workflow-job@2.24">
  <actions />
  <description />
  <keepDependencies>false</keepDependencies>
  <properties>
    <org.jenkinsci.plugins.workflow.job.properties.DisableConcurrentBuildsJobProperty />
    <com.coravy.hudson.plugins.github.GithubProjectProperty plugin="github@1.29.2">
      <projectUrl></projectUrl>
      <displayName />
    </com.coravy.hudson.plugins.github.GithubProjectProperty>
    <org.jenkinsci.plugins.workflow.job.properties.PipelineTriggersJobProperty>
      <triggers>
        <hudson.triggers.SCMTrigger>
          <spec>* * * * *</spec>
          <ignorePostCommitHooks>false</ignorePostCommitHooks>
        </hudson.triggers.SCMTrigger>
      </triggers>
    </org.jenkinsci.plugins.workflow.job.properties.PipelineTriggersJobProperty>
  </properties>
  <definition class="org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition" plugin="workflow-cps@2.54">
    <scm class="hudson.plugins.git.GitSCM" plugin="git@3.9.1">
      <configVersion>2</configVersion>
      <userRemoteConfigs>
        <hudson.plugins.git.UserRemoteConfig>
          <url></url>
          <credentialsId>github-token</credentialsId>
        </hudson.plugins.git.UserRemoteConfig>
      </userRemoteConfigs>
      <branches>
        <hudson.plugins.git.BranchSpec>
          <name>*/master</name>
        </hudson.plugins.git.BranchSpec>
      </branches>
      <doGenerateSubmoduleConfigurations>false</doGenerateSubmoduleConfigurations>
      <submoduleCfg class="list" />
      <extensions />
    </scm>
    <scriptPath></scriptPath>
    <lightweight>true</lightweight>
  </definition>
  <triggers />
  <disabled>false</disabled>
</flow-definition>
'''

JENKINS_CONFIG_REFERENCE = '''
<flow-definition plugin="workflow-job@2.24">
  <actions />
  <description />
  <keepDependencies>false</keepDependencies>
  <properties>
    <org.jenkinsci.plugins.workflow.job.properties.DisableConcurrentBuildsJobProperty />
    <com.coravy.hudson.plugins.github.GithubProjectProperty plugin="github@1.29.2">
      <projectUrl>https://my/remote/repo.git</projectUrl>
      <displayName />
    </com.coravy.hudson.plugins.github.GithubProjectProperty>
    <org.jenkinsci.plugins.workflow.job.properties.PipelineTriggersJobProperty>
      <triggers>
        <hudson.triggers.SCMTrigger>
          <spec>* * * * *</spec>
          <ignorePostCommitHooks>false</ignorePostCommitHooks>
        </hudson.triggers.SCMTrigger>
      </triggers>
    </org.jenkinsci.plugins.workflow.job.properties.PipelineTriggersJobProperty>
  </properties>
  <definition class="org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition" plugin="workflow-cps@2.54">
    <scm class="hudson.plugins.git.GitSCM" plugin="git@3.9.1">
      <configVersion>2</configVersion>
      <userRemoteConfigs>
        <hudson.plugins.git.UserRemoteConfig>
          <url>git@github.com:remote/repo.git</url>
          <credentialsId>repo-ci</credentialsId>
        </hudson.plugins.git.UserRemoteConfig>
      </userRemoteConfigs>
      <branches>
        <hudson.plugins.git.BranchSpec>
          <name>*/master</name>
        </hudson.plugins.git.BranchSpec>
      </branches>
      <doGenerateSubmoduleConfigurations>false</doGenerateSubmoduleConfigurations>
      <submoduleCfg class="list" />
      <extensions />
    </scm>
    <scriptPath>Jenkinsfile</scriptPath>
    <lightweight>true</lightweight>
  </definition>
  <triggers />
  <disabled>false</disabled>
</flow-definition>
'''