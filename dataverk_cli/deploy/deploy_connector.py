import jenkins
from xml.etree import ElementTree
from pathlib import Path


class DeployConnector:
    """ Connector to build server for setting up CI/CD
    """

    def __init__(self, job_name: str, build_server):
        self._job_name = job_name
        self._build_server = build_server

    def configure_job(self, config_path: Path=Path('jenkins_config.xml')):
        """ Creates or reconfigures the CI/CD job

        :param config_path: Path
        """
        config_file_path = config_path
        xml_base_config = self._read_xml_file(config_file_path=config_file_path)
        if self._build_server.job_exists(name=self._job_name):
            self._update_jenkins_job(xml_base_config=xml_base_config)
        else:
            self._create_new_jenkins_job(xml_base_config=xml_base_config)

    def delete_job(self):
        """ Deletes the cronjob and CI/CD job
        """
        self._delete_cronjob()

        try:
            self._build_server.delete_job(self._job_name)
        except jenkins.NotFoundException:
            raise UserWarning(f'No job with name {self._job_name} exists on build server')

    def _create_new_jenkins_job(self, xml_base_config: str):
        """ Creates a new CI/CD job for datapackage
        """
        try:
            self._build_server.create_job(name=self._job_name, config_xml=xml_base_config)
        except jenkins.JenkinsException:
            raise jenkins.JenkinsException(f"Unable to setup job for {self._job_name}")

    def _update_jenkins_job(self, xml_base_config: str):
        """ Updates existing CI/CD job for datapackage

        :param config_file_path: Path
        """
        try:
            self._build_server.reconfig_job(name=self._job_name, config_xml=xml_base_config)
        except jenkins.JenkinsException:
            raise jenkins.JenkinsException(f"Unable to reconfigure job for {self._job_name}")

    @staticmethod
    def _read_xml_file(config_file_path: Path):
        xml_base = ElementTree.parse(config_file_path)
        xml_base_root = xml_base.getroot()

        return ElementTree.tostring(xml_base_root, encoding='utf-8', method='xml').decode()

    def _delete_cronjob(self):
        self._build_server.build_job(name="remove-cronjob", parameters={"PACKAGE_NAME": self._job_name})
