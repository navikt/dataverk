import os

from .datapackage_base import BaseDataPackage
from . import settings_loader
from shutil import rmtree, copy
from xml.etree import ElementTree


class UpdateDataPackage(BaseDataPackage):

    def __init__(self, settings: dict, envs):
        super().__init__(settings=settings, envs=envs)

    def _update_datapackage_local(self):
        template_files = ["cronjob.yaml", "Dockerfile", "jenkins_config.xml", "Jenkinsfile"]

        templates_path = ""
        try:
            templates_loader = settings_loader.GitSettingsLoader(url=self.envs["TEMPLATES_REPO"])
            templates_path = templates_loader.download_to(".")
            for template_file in template_files:
                copy(os.path.join(str(templates_path), 'file_templates', template_file), self.settings["package_name"])
        except OSError:
            raise OSError(f'Templates mappe eksisterer ikke.')
        finally:
            if os.path.exists(str(templates_path)):
                rmtree(str(templates_path))

    def _update_jenkins_job(self):
        self._edit_jenkins_job_config()

        xml = ElementTree.parse(os.path.join(self.settings["package_name"], 'jenkins_config.xml'))
        xml_root = xml.getroot()
        xml_config = ElementTree.tostring(xml_root, encoding='utf-8', method='xml').decode()

        self.jenkins_server.reconfig_job(self.settings["package_name"], xml_config)

    def _update(self):
        '''
        '''

        self._update_datapackage_local()
        self._edit_package_metadata()
        self._edit_cronjob_config()
        self._edit_jenkins_file()
        self._update_jenkins_job()

    def run(self):
        ''' Entrypoint for dataverk update
        '''

        if not self._folder_exists_in_repo(self.settings["package_name"]):
            raise NameError(f'Det finnes ingen datapakke med navn {self.settings["package_name"]} '
                            f'i repo {self.github_project}')

        if not self.jenkins_server.job_exists(name=self.settings["package_name"]):
            raise NameError(f'Det finnes ingen jobb med navn {self.settings["package_name"]} '
                            f'på jenkins serveren.')

        res = input(f'Vil du oppdatere datapakken {self.settings["package_name"]} i repo {self.github_project}? [j/n] ')

        if res in {'j', 'ja', 'y', 'yes'}:
            try:
                self._update()
            except Exception:
                raise Exception(f'Klarte ikke oppdatere datapakken {self.settings["package_name"]}')
        else:
            print(f'Datapakken {self.settings["package_name"]} ble ikke opprettet')
