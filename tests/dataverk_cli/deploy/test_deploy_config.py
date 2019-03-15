# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import yaml
from dataverk.utils.windows_safe_tempdir import WindowsSafeTempDirectory
from dataverk_cli.deploy import deploy_config
from dataverk_cli.cli.cli_utils import repo_info
from pathlib import Path
from git import Repo

# Common input parameters
# =======================
SETTINGS_TEMPLATE = {
  "package_name": "package_name",
  "nais_namespace": "dataverk",
  "image_endpoint": "mitt.image.endpoint.no:1234/",
  "update_schedule": "* * 31 2 *",

  "vault": {
    "auth_uri": "/path/to/auth",
    "secrets_uri": "/path/to/kv",
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
        pass
        self._local_repo, self._local_repo_path = self.create_tmp_repo()

    def tearDown(self):
        pass
        self._local_repo.close()
        self._local_repo_path.cleanup()

    def create_tmp_repo(self) -> (Repo, WindowsSafeTempDirectory):
        tmpdir = WindowsSafeTempDirectory()
        repo = Repo.init(tmpdir.name)
        repo.create_remote("origin", url="https://my/remote/repo.git")
        repo.index.commit("initial commit")
        return repo, tmpdir

# Test classes
# ============


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """
    def test__edit_cronjob_config(self):
        yamlpath = Path(self._local_repo_path.name).joinpath("cronjob.yaml")
        yaml_reference = yaml.load(CRONJOB_YAML_REFERENCE)

        with yamlpath.open('w') as yamlfile:
            yamlfile.write(CRONJOB_YAML_TEMPLATE)

        deploy_config.edit_cronjob_config(settings_store=SETTINGS_TEMPLATE, yaml_path=yamlpath)

        with yamlpath.open(mode="r", encoding="utf-8") as yamlfile:
            edited_yaml = yaml.load(yamlfile.read())

        self.assertEqual(yaml_reference, edited_yaml)

    def test__edit_service_account_config(self):
        yamlpath = Path(self._local_repo_path.name).joinpath("cronjob.yaml")
        yaml_reference = yaml.load(SERVICE_ACCOUNT_REFERENCE)

        with yamlpath.open('w') as yamlfile:
            yamlfile.write(SERVICE_ACCOUNT_TEMPLATE)

        deploy_config.edit_service_user_config(settings_store=SETTINGS_TEMPLATE, service_account_file=yamlpath)

        with yamlpath.open(mode="r", encoding="utf-8") as yamlfile:
            edited_yaml = yaml.load(yamlfile.read())

        self.assertEqual(yaml_reference, edited_yaml)

    def test__edit_jenkins_job_config(self):
        configpath = Path(self._local_repo_path.name).joinpath("jenkins_config.xml")

        with configpath.open('w') as configfile:
            configfile.write(JENKINS_CONFIG_TEMPLATE)

        deploy_config.edit_jenkins_job_config(repo_info.get_remote_url(self._local_repo_path.name), credential_id="repo-ci", config_file_path=configpath)

        with configpath.open(mode="r", encoding="utf-8") as jenkins_config:
            edited_jenkins_config = jenkins_config.read()

        self.assertEqual(JENKINS_CONFIG_REFERENCE, edited_jenkins_config)


CRONJOB_YAML_TEMPLATE = '''
apiVersion: batch/v1beta1
kind: CronJob
metadata:
  name: ${package_name}
  namespace: ${namespace}
spec:
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - image: mitt.image.endpoint.no:1234/package_name
            name: ${package_name}-cronjob
            volumeMounts:
            - mountPath: /mount/path
              name: vault-secrets
          initContainers:
          - env:
            - name: VKS_VAULT_ADDR
              value: https://vault.auth.uri.com
            - name: VKS_AUTH_PATH
              value: ${vks_auth_path}
            - name: VKS_KV_PATH
              value: ${vks_kv_path}
            - name: VKS_VAULT_ROLE
              value: ${package_name}
            - name: VKS_SECRET_DEST_PATH
              value: /
            image: my-init-container-image
            name: vks
            volumeMounts:
            - mountPath: /mount/path
              name: vault-secrets
          restartPolicy: Never
          serviceAccount: ${package_name}
          serviceAccountName: ${package_name}
          volumes:
          - emptyDir:
              medium: Memory
            name: vault-secrets
  schedule: '* * 31 2 *'
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

SERVICE_ACCOUNT_TEMPLATE = '''
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${service_account}
  namespace: dataverk
  labels:
    team: dataverk
'''

SERVICE_ACCOUNT_REFERENCE = '''
apiVersion: v1
kind: ServiceAccount
metadata: 
  name: package_name
  namespace: dataverk
  labels:
    team: dataverk
'''

JENKINS_CONFIG_TEMPLATE = '''
<flow-definition plugin="workflow-job@2.24">
  <actions />
  <description />
  <keepDependencies>false</keepDependencies>
  <properties>
    <org.jenkinsci.plugins.workflow.job.properties.DisableConcurrentBuildsJobProperty />
    <com.coravy.hudson.plugins.github.GithubProjectProperty plugin="github@1.29.2">
      <projectUrl>${projectUrl}</projectUrl>
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
          <url>${url}</url>
          <credentialsId>${credentialsId}</credentialsId>
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
    <scriptPath>${scriptPath}</scriptPath>
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
