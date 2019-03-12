# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import jenkins
from pathlib import Path
from dataverk.utils.windows_safe_tempdir import WindowsSafeTempDirectory
from dataverk_cli.deploy.deploy_connector import DeployConnector
from dataverk_cli.cli.cli_utils import repo_info
from git import Repo


# Common input parameters
# =======================
class BuildServerMock:

    def __init__(self, exising_jobs: list=[]):
        self._existing_jobs = exising_jobs

    def job_exists(self, name):
        if name in self._existing_jobs:
            return True
        return False

    def delete_job(self, name):
        if name not in self._existing_jobs:
            raise jenkins.NotFoundException()

    def create_job(self, name, config_xml):
        pass

    def reconfig_job(self, name, config_xml):
        pass

    def build_job(self, name, parameters=None):
        pass


class FaultyBuildServerMock:
    def __init__(self, exising_jobs: list=[]):
        self._existing_jobs = exising_jobs

    def job_exists(self, name):
        if name in self._existing_jobs:
            return True
        return False

    def create_job(self, name, config_xml):
        raise jenkins.JenkinsException()

    def reconfig_job(self, name, config_xml):
        raise jenkins.JenkinsException()

    def build_job(self, name, parameters=None):
        pass


TEST_CONFIG_XML ="""<flow-definition plugin="workflow-job@2.24">
  <actions />
  <description />
  <keepDependencies>false</keepDependencies>
  <properties>
    <org.jenkinsci.plugins.workflow.job.properties.DisableConcurrentBuildsJobProperty />
    <com.coravy.hudson.plugins.github.GithubProjectProperty plugin="github@1.29.2">
      <projectUrl>https://github.com</projectUrl>
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
    <scriptPath>Jenkinsfile</scriptPath>
    <lightweight>true</lightweight>
  </definition>
  <triggers />
  <disabled>false</disabled>
</flow-definition>"""


# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.tmp_dir = self.create_tmp_dir()
        self.configpath = Path(self.tmp_dir.name).joinpath("jenkins_config.xml")
        with self.configpath.open('w') as configfile:
            configfile.write(TEST_CONFIG_XML)

        self.job_name = "package_name"
        self.build_server = BuildServerMock()
        self.build_server._existing_jobs.append(self.job_name)
        self.deploy_conn = DeployConnector(job_name=self.job_name, build_server=self.build_server)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def create_tmp_dir(self) -> WindowsSafeTempDirectory:
        tmpdir = WindowsSafeTempDirectory()
        return tmpdir


class MethodsInput(Base):
    """
    Tests methods which take input parameters

    Tests include: passing invalid input, etc.
    """

    def test_delete_job_job_does_not_exist(self):
        build_server = BuildServerMock(exising_jobs=[])
        deploy_conn = DeployConnector(job_name=self.job_name, build_server=build_server)
        with self.assertRaises(UserWarning):
            deploy_conn.delete_job()

    def test_delete_job_correct(self):
        self.deploy_conn.delete_job()

    def test_configure_job_valid(self):
        configpath = Path(self.tmp_dir.name).joinpath("jenkins_config.xml")
        self.deploy_conn.configure_job(config_path=configpath)

    def test_configure_job_invalid(self):
        configpath = Path(self.tmp_dir.name).joinpath("jenkins_config.xml")
        deploy_conn = DeployConnector(job_name=self.job_name, build_server=FaultyBuildServerMock(exising_jobs=[]))
        with self.assertRaises(jenkins.JenkinsException):
            deploy_conn.configure_job(config_path=configpath)


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test__read_xml_file(self):
        config = self.deploy_conn._read_xml_file(config_file_path=self.configpath)

        self.assertEqual(TEST_CONFIG_XML, config)
