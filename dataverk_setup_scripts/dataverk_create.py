import jenkins
import os
import json
import yaml

from string import Template
from shutil import copyfile, rmtree
from xml.etree import ElementTree
from . import settings_loader, settings_creator
from dataverk.utils.env_store import EnvStore
from dataverk.utils import resource_discoverer
from pathlib import Path


class CreateDataPackage:
    ''' Klasse for å opprette ny datapakke i et eksisterende repository for datapakker/datasett
    '''

    def __init__(self, github_project: str, settings, envs: EnvStore):

        self._verify_class_init_arguments(github_project, envs)

        self.settings = settings
        self.github_project = github_project
        self.envs = envs

        if not self._is_in_repo_root():
            raise Exception(f'dataverk create må kjøres fra topp-nivået i git repoet')

        if self._folder_exists_in_repo(self.settings["package_name"]):
            raise NameError(f'En mappe med navn {self.settings["package_name"]} '
                            f'eksisterer allerede i repo {self.github_project}')

        self._create_folder_structure()
        templates_path = settings_loader.GitSettingsLoader(url=self.envs["SETTINGS_REPO"])
        self.templates_folder_name = templates_path.download_to(self.settings["package_name"])
        self._copy_template_files()
        self._create_settings_file(path=self.settings["package_name"])
        self._remove_templates_repo()

        self.jenkins_server = jenkins.Jenkins(self.settings["jenkins"]["url"],
                                              username=self.envs['USER_IDENT'],
                                              password=self.envs['PASSWORD'])

        if self._jenkins_job_exists(self.settings["package_name"]):
            raise NameError(f'En jobb med navn {self.settings["package_name"]} '
                            f'eksisterer allerede på jenkins serveren. Datapakkenavn må være unikt.')

    def _verify_class_init_arguments(self, github_project, envs):
        if not isinstance(github_project, str):
            raise TypeError(f'github_project parameter must be of type string')

        if not isinstance(envs, EnvStore):
            raise TypeError(f'envs parameter must be of type EnvStore')

    def _is_in_repo_root(self):
        ''' Sjekk på om create_dataverk kjøres fra toppnivå i repo.

        :return: boolean: "True" hvis dataverk_create kjøres fra toppnivå i repo, "False" ellers
        '''

        current_dir = os.getcwd()
        git_root = os.popen('git rev-parse --show-toplevel').read().strip()

        return os.path.samefile(current_dir, git_root)

    def _folder_exists_in_repo(self, name: str):
        ''' Sjekk på om det finnes en mappe i repoet med samme navn som ønsket pakkenavn

        :return: boolean: "True" hvis pakkenavn allerede er tatt i bruk, "False" ellers
        '''

        for filename in os.listdir(os.getcwd()):
            if name == filename:
                print(f'En mappe med navn {name} eksisterer allerede i repo {self.github_project}')
                return True

        return False

    def _jenkins_job_exists(self, name):
        ''' Sjekk på om det finnes en jobb på jenkinsserveren med samme navn som ønsket pakkenavn
            Jenkinsjobben får samme navn som pakkenavnet valgt

        :return: boolean: "True" hvis jenkinsjob allerede er tatt i bruk, "False" ellers
        '''

        if self.jenkins_server.job_exists(name):
            print(f'En jobb med navn {name} eksisterer allerede på jenkins serveren')
            return True

        return False

    def _create_folder_structure(self):
        ''' Lager mappestrukturen for datapakken. Oppretter mappene:
            {repo_root}/{pakkenavn}/scripts
            {repo_root}/{pakkenavn}/data
        '''

        os.mkdir(self.settings["package_name"])
        os.mkdir(os.path.join(self.settings["package_name"], 'scripts'))
        os.mkdir(os.path.join(self.settings["package_name"], 'data'))

    def _copy_template_files(self):
        ''' Kopierer template filer til datapakken:
        '''

        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'jenkins_base_config.xml'), os.path.join(self.settings["package_name"], 'jenkins_base_config.xml'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'jenkins_config.xml'), os.path.join(self.settings["package_name"], 'jenkins_config.xml'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'Jenkinsfile'), os.path.join(self.settings["package_name"], 'Jenkinsfile'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'cronjob.yaml'), os.path.join(self.settings["package_name"], 'cronjob.yaml'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'Dockerfile'), os.path.join(self.settings["package_name"], 'Dockerfile'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'LICENSE.md'), os.path.join(self.settings["package_name"], 'LICENSE.md'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'README.md'), os.path.join(self.settings["package_name"], 'README.md'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'METADATA.json'), os.path.join(self.settings["package_name"], 'METADATA.json'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'etl.ipynb'), os.path.join(self.settings["package_name"], 'scripts', 'etl.ipynb'))

    def _create_settings_file(self, path: str):
        with open(os.path.join(path, 'settings.json'), 'w') as settings_file:
            json.dump(self.settings, settings_file, indent=2)

    def _remove_templates_repo(self):
        try:
            rmtree(str(self.templates_folder_name))
        except OSError:
            raise OSError(f'Templates mappe eksisterer ikke.')

    def _edit_package_metadata(self):
        '''  Tilpasser metadata fil til datapakken

        '''

        try:
            with open(os.path.join(self.settings["package_name"], 'METADATA.json'), 'r') as metadatafile:
                package_metadata = json.load(metadatafile)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil')

        package_metadata['Datapakke_navn'] = self.settings["package_name"]
        package_metadata['Bucket_navn'] = 'nav-opendata'

        try:
            with open(os.path.join(self.settings["package_name"], 'METADATA.json'), 'w') as metadatafile:
                json.dump(package_metadata, metadatafile, indent=2)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil')

    def _edit_cronjob_config(self):
        ''' Tilpasser cronjob config fil til datapakken

        '''

        try:
            with open(os.path.join(self.settings["package_name"], 'cronjob.yaml'), 'r') as yamlfile:
                cronjob_config = yaml.load(yamlfile)
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil')

        cronjob_config['metadata']['name'] = self.settings["package_name"]
        cronjob_config['metadata']['namespace'] = self.settings["nais_namespace"]

        cronjob_config['spec']['schedule'] = self.settings["update_schedule"]
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['name'] = self.settings["package_name"] + '-cronjob'
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['image'] = 'repo.adeo.no:5443/' + self.settings["package_name"]

        try:
            with open(os.path.join(self.settings["package_name"], 'cronjob.yaml'), 'w') as yamlfile:
                yamlfile.write(yaml.dump(cronjob_config, default_flow_style=False))
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil')

    def _edit_jenkins_file(self):
        ''' Tilpasser Jenkinsfile til datapakken

        '''

        try:
            with open(os.path.join(self.settings["package_name"], 'Jenkinsfile'), 'r') as jenkinsfile:
                jenkins_config = jenkinsfile.read()
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile')

        template = Template(jenkins_config)
        jenkins_config = template.safe_substitute(package_name=self.settings["package_name"],
                                                  package_repo=self.github_project,
                                                  package_path=self.settings["package_name"])

        try:
            with open(os.path.join(self.settings["package_name"], 'Jenkinsfile'), 'w') as jenkinsfile:
                jenkinsfile.write(jenkins_config)
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile')

    def _create_jenkins_job(self):
        ''' Tilpasser jenkins konfigurasjonsfil og setter opp ny jenkins jobb for datapakken

        '''

        try:
            with open(os.path.join(self.settings["package_name"], 'jenkins_base_config.xml'), 'r') as jenkins_config:
                config = jenkins_config.read()
        except OSError:
            raise OSError(f'Finner ikke jenkins_base_config.xml')

        template = Template(config)
        jenkins_config = template.safe_substitute(github_repo=self.github_project)

        try:
            with open(os.path.join(self.settings["package_name"], 'jenkins_base_config.xml'), 'w') as jenkins_base_config:
                jenkins_base_config.write(jenkins_config)
        except OSError:
            raise OSError(f'Finner ikke jenkins_base_config.xml')

        xml_base = ElementTree.parse(os.path.join(self.settings["package_name"], 'jenkins_base_config.xml'))
        xml_base_root = xml_base.getroot()

        xml_base_config = ElementTree.tostring(xml_base_root, encoding='utf-8', method='xml').decode()

        try:
            self.jenkins_server.create_job(name=self.settings["package_name"], config_xml=xml_base_config)
        except jenkins.JenkinsException:
            rmtree(self.settings["package_name"])
            raise jenkins.JenkinsException

        self.jenkins_server.build_job(name=self.settings["package_name"])

        xml = ElementTree.parse(os.path.join(self.settings["package_name"], 'jenkins_config.xml'))
        xml_root = xml.getroot()

        for elem in xml_root.getiterator():
            if elem.tag == 'scriptPath':
                elem.text = self.settings["package_name"] + '/Jenkinsfile'
            elif elem.tag == 'projectUrl':
                elem.text = self.github_project
            elif elem.tag == 'url':
                elem.text = self.github_project

        xml.write(os.path.join(self.settings["package_name"], 'jenkins_config.xml'))

        xml_config = ElementTree.tostring(xml_root, encoding='utf-8', method='xml').decode()

        self.jenkins_server.reconfig_job(self.settings["package_name"], xml_config)

    def print_datapackage_config(self):
        print("\n-------------Ny datapakke------------------------" +
              "\nDatapakkenavn: " + self.settings["package_name"] +
              "\ngithub repo: " + self.github_project +
              "\ncronjob schedule: " + self.settings["update_schedule"] +
              "\nNAIS namespace: " + self.settings["nais_namespace"] +
              "\n-------------------------------------------------\n")

    def create(self):
        ''' Oppretter ny datapakke med ønsket konfigurasjon

        '''

        self._edit_package_metadata()
        self._edit_cronjob_config()
        self._edit_jenkins_file()

        self._create_jenkins_job()

        print(f'Datapakken {self.settings["package_name"]} er opprettet')

        # TODO: Add more error handling


def get_github_url():
    if not os.popen('git rev-parse --is-inside-work-tree').read().strip():
        raise Exception("dataverk create må kjøres fra et git repository")

    return os.popen('git config --get remote.origin.url').read().strip()


def run(args):
    ''' Entrypoint for dataverk create

    '''

    github_project = get_github_url()
    print(f'Opprettelse av ny datapakke i {github_project}')

    resource_files = resource_discoverer.search_for_files(start_path=Path('.'), file_names=('settings.json', '.env'), levels=3)

    if '.env' not in resource_files:
        Exception(f'.env fil må finnes i repo for å kunne kjøre dataverk create')

    envs = EnvStore(path=Path(resource_files['.env']))

    default_settings_loader = settings_loader.GitSettingsLoader(url=envs["SETTINGS_REPO"])
    default_settings_path = default_settings_loader.download_to('.')

    settings_creator_object = settings_creator.get_settings_creator(args=args,
                                                                    default_settings_path=str(default_settings_path))
    settings = settings_creator_object.create_settings()

    rmtree(str(default_settings_path))

    new_datapackage = CreateDataPackage(github_project=github_project, envs=envs, settings=settings)

    new_datapackage.print_datapackage_config()
    res = input(f'Vil du opprette datapakken med konfigurasjonen over? [j/n] ')

    if res in {'j', 'ja', 'y', 'yes'}:
        new_datapackage.create()
    else:
        print(f'Datapakken ble ikke opprettet')
