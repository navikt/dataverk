import jenkins
import os
import json
import yaml

from string import Template
from shutil import copyfile, rmtree
from xml.etree import ElementTree
from . import settings_loader
from dataverk.utils.settings_store import SettingsStore
from dataverk.utils import resource_discoverer, env_store
from pathlib import Path


class CreateDataPackage:
    ''' Klasse for å opprette ny datapakke i et eksisterende repository for datapakker/datasett
    '''

    def __init__(self, name: str, github_project: str, update_schedule: str, namespace: str):

        self._verify_class_init_arguments(name, github_project, update_schedule, namespace)

        self.package_name = name
        self.github_project = github_project
        self.namespace = namespace
        self.cronjob_schedule = update_schedule

        if not self._is_in_repo_root():
            raise Exception(f'dataverk create må kjøres fra topp-nivået i git repoet')

        if self._folder_exists_in_repo(name):
            raise NameError(f'En mappe med navn {name} eksisterer allerede i repo {self.github_project}')

        self._create_folder_structure()

        self.resources = resource_discoverer.search_for_files(start_path=Path(os.path.join(self.package_name, 'scripts')),
                                                              file_names=('settings.json', '.env'), levels=3)

        if not self._required_files_found(self.resources):
            raise Exception(f'settings.json og .env må finnes i repo for å kunne kjøre dataverk create')

        self.envs = env_store.EnvStore(path=Path(self.resources['.env']))

        self.settings = SettingsStore(settings_json_url=Path(self.resources["settings.json"]), env_store=self.envs)

        templates_path = settings_loader.GitSettingsLoader(url=self.settings["template_repository"])

        self.templates_folder_name = templates_path.download_to(self.package_name)

        self._copy_template_files()
        self._copy_settings_file()
        self._remove_templates_repo()

        self.jenkins_server = jenkins.Jenkins(self.settings["jenkins"]["url"],
                                              username=self.envs['USER_IDENT'],
                                              password=self.envs['PASSWORD'])

        if self._jenkins_job_exists(name):
            raise NameError(f'En jobb med navn {name} eksisterer allerede på jenkins serveren. Datapakkenavn må være unikt.')

    def _verify_class_init_arguments(self, name, github_project, update_schedule, namespace):
        if not isinstance(name, str):
            raise TypeError(f'name parameter must be of type string')

        if not isinstance(github_project, str):
            raise TypeError(f'github_project parameter must be of type string')

        if not isinstance(update_schedule, str):
            raise TypeError(f'update_schedule parameter must be of type string')

        if not isinstance(namespace, str):
            raise TypeError(f'namespace parameter must be of type string')

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

    def _required_files_found(self, required_files: dict):
        ''' Sjekker at settings.json og .env finnes i repository

        :return: boolean: "True" hvis filene eksisterer, "False" ellers
        '''

        if 'settings.json' not in required_files:
            print(f'Fant ikke settings.json')
            return False

        if '.env' not in required_files:
            print(f'Fant ikke .env')
            return False

        return True

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

        os.mkdir(self.package_name)
        os.mkdir(os.path.join(self.package_name, 'scripts'))
        os.mkdir(os.path.join(self.package_name, 'data'))

    def _copy_settings_file(self):
        '''
        @return path to settings json file
        '''

        copyfile(self.resources['settings.json'], os.path.join(self.package_name, 'scripts', 'settings.json'))

        return os.path.join(self.package_name, 'scripts', 'settings.json')

    def _copy_template_files(self):
        ''' Kopierer følgende template filer til datapakken:
            - jenkins_base_config.xml
            - jenkins_config.xml
            - Jenkinsfile
            - cronjob.yaml
            - Dockerfile
            - METADATA.json
            - LICENSE.md
            - README.md
            - etl.ipynb
        '''

        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'jenkins_base_config.xml'), os.path.join(self.package_name, 'jenkins_base_config.xml'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'jenkins_config.xml'), os.path.join(self.package_name, 'jenkins_config.xml'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'Jenkinsfile'), os.path.join(self.package_name, 'Jenkinsfile'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'cronjob.yaml'), os.path.join(self.package_name, 'cronjob.yaml'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'Dockerfile'), os.path.join(self.package_name, 'Dockerfile'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'LICENSE.md'), os.path.join(self.package_name, 'LICENSE.md'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'README.md'), os.path.join(self.package_name, 'README.md'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'METADATA.json'), os.path.join(self.package_name, 'METADATA.json'))
        copyfile(os.path.join(str(self.templates_folder_name), 'file_templates', 'etl.ipynb'), os.path.join(self.package_name, 'scripts', 'etl.ipynb'))

    def _remove_templates_repo(self):
        try:
            rmtree(str(self.templates_folder_name))
        except OSError:
            raise OSError(f'Templates mappe eksisterer ikke.')

    def _edit_package_metadata(self):
        '''  Tilpasser metadata fil til datapakken

        '''

        try:
            with open(os.path.join(self.package_name, 'METADATA.json'), 'r') as metadatafile:
                package_metadata = json.load(metadatafile)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil')

        package_metadata['Datapakke_navn'] = self.package_name
        package_metadata['Bucket_navn'] = 'nav-opendata'

        try:
            with open(os.path.join(self.package_name, 'METADATA.json'), 'w') as metadatafile:
                json.dump(package_metadata, metadatafile, indent=2)
        except OSError:
            raise OSError(f'Finner ikke METADATA.json fil')

    def _edit_cronjob_config(self):
        ''' Tilpasser cronjob config fil til datapakken

        '''

        try:
            with open(os.path.join(self.package_name, 'cronjob.yaml'), 'r') as yamlfile:
                cronjob_config = yaml.load(yamlfile)
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil')

        cronjob_config['metadata']['name'] = self.package_name
        cronjob_config['metadata']['namespace'] = self.namespace

        cronjob_config['spec']['schedule'] = self.cronjob_schedule
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['name'] = self.package_name + '-cronjob'
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['image'] = 'repo.adeo.no:5443/' + self.package_name

        try:
            with open(os.path.join(self.package_name, 'cronjob.yaml'), 'w') as yamlfile:
                yamlfile.write(yaml.dump(cronjob_config, default_flow_style=False))
        except OSError:
            raise OSError(f'Finner ikke cronjob.yaml fil')

    def _edit_jenkins_file(self):
        ''' Tilpasser Jenkinsfile til datapakken

        '''

        try:
            with open(os.path.join(self.package_name, 'Jenkinsfile'), 'r') as jenkinsfile:
                jenkins_config = jenkinsfile.read()
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile')

        template = Template(jenkins_config)
        jenkins_config = template.safe_substitute(package_name=self.package_name,
                                                  package_repo=self.github_project,
                                                  package_path=self.package_name)

        try:
            with open(os.path.join(self.package_name, 'Jenkinsfile'), 'w') as jenkinsfile:
                jenkinsfile.write(jenkins_config)
        except OSError:
            raise OSError(f'Finner ikke Jenkinsfile')

    def _create_jenkins_job(self):
        ''' Tilpasser jenkins konfigurasjonsfil og setter opp ny jenkins jobb for datapakken

        '''

        try:
            with open(os.path.join(self.package_name, 'jenkins_base_config.xml'), 'r') as jenkins_config:
                config = jenkins_config.read()
        except OSError:
            raise OSError(f'Finner ikke jenkins_base_config.xml')

        template = Template(config)
        jenkins_config = template.safe_substitute(github_repo=self.github_project)

        try:
            with open(os.path.join(self.package_name, 'jenkins_base_config.xml'), 'w') as jenkins_base_config:
                jenkins_base_config.write(jenkins_config)
        except OSError:
            raise OSError(f'Finner ikke jenkins_base_config.xml')

        xml_base = ElementTree.parse(os.path.join(self.package_name, 'jenkins_base_config.xml'))
        xml_base_root = xml_base.getroot()

        xml_base_config = ElementTree.tostring(xml_base_root, encoding='utf-8', method='xml').decode()

        try:
            self.jenkins_server.create_job(name=self.package_name, config_xml=xml_base_config)
        except jenkins.JenkinsException:
            rmtree(self.package_name)
            raise jenkins.JenkinsException

        self.jenkins_server.build_job(name=self.package_name)

        xml = ElementTree.parse(os.path.join(self.package_name, 'jenkins_config.xml'))
        xml_root = xml.getroot()

        for elem in xml_root.getiterator():
            if elem.tag == 'scriptPath':
                elem.text = self.package_name + '/Jenkinsfile'
            elif elem.tag == 'projectUrl':
                elem.text = self.github_project
            elif elem.tag == 'url':
                elem.text = self.github_project

        xml.write(os.path.join(self.package_name, 'jenkins_config.xml'))

        xml_config = ElementTree.tostring(xml_root, encoding='utf-8', method='xml').decode()

        self.jenkins_server.reconfig_job(self.package_name, xml_config)

    def print_datapackage_config(self):
        print("\n-------------Ny datapakke------------------------" +
              "\nDatapakkenavn: " + self.package_name +
              "\ngithub repo: " + self.github_project +
              "\ncronjob schedule: " + self.cronjob_schedule +
              "\nNAIS namespace: " + self.namespace +
              "\n-------------------------------------------------\n")

    def create(self):
        ''' Oppretter ny datapakke med ønsket konfigurasjon

        '''

        self._edit_package_metadata()
        self._edit_cronjob_config()
        self._edit_jenkins_file()

        self._create_jenkins_job()

        print(f'Datapakken {self.package_name} er opprettet')

        # TODO: Add more error handling


def validate_cronjob_schedule(schedule):
    ''' Kontrollerer brukerinput for cronjob schedule
            Format på cronjob schedule string: "* * * * *"
            "minutt" "time" "dag i måned" "måned i år" "ukedag"
             (0-59)  (0-23)    (1-31)        (1-12)     (0-6)
    '''

    schedule_list = schedule.split(' ')

    if not len(schedule_list) is 5:
        raise ValueError(
            f'Schedule {schedule} har ikke riktig format. Må ha formatet: \"<minutt> <time> <dag i måned> <måned> <ukedag>\". '
            f'F.eks. \"0 12 * * 2,4\" vil gi: Hver tirsdag og torsdag kl 12.00 UTC')

    if not schedule_list[0] is '*':
        for minute in schedule_list[0].split(','):
            if not int(minute) in range(0, 60):
                raise ValueError(f'I schedule {schedule} er ikke {minute}'
                                 f' en gyldig verdi for minutt innenfor time. Gyldige verdier er 0-59, eller *')

    if not schedule_list[1] is '*':
        for hour in schedule_list[1].split(','):
            if not int(hour) in range(0, 24):
                raise ValueError(f'I schedule {schedule} er ikke {hour}'
                                 f' en gyldig verdi for time. Gyldige verdier er 0-23, eller *')

    if not schedule_list[2] is '*':
        for day in schedule_list[2].split(','):
            if not int(day) in range(1, 32):
                raise ValueError(f'I schedule {schedule} er ikke {day}'
                                 f' en gyldig verdi for dag i måned. Gyldige verdier er 1-31, eller *')

    if not schedule_list[3] is '*':
        for month in schedule_list[3].split(','):
            if not int(month) in range(1, 13):
                raise ValueError(f'I schedule {schedule} er ikke {month}'
                                 f' en gyldig verdi for måned. Gyldige verdier er 1-12, eller *')

    if not schedule_list[4] is '*':
        for weekday in schedule_list[4].split(','):
            if not int(weekday) in range(0, 7):
                raise ValueError(f'I schedule {schedule} er ikke {weekday}'
                                 f' en gyldig verdi for ukedag. Gyldige verdier er 0-6 (søn-lør), eller *')

def get_github_url():
    if not os.popen('git rev-parse --is-inside-work-tree').read().strip():
        raise Exception("dataverk_create må kjøres fra et git repository")

    return os.popen('git config --get remote.origin.url').read().strip()


def run(package_name_in: str=None, update_schedule_in: str=None, nais_namespace_in: str=None):
    ''' Entrypoint for dataverk create

    '''

    github_project = get_github_url()

    print(f'Opprettelse av ny datapakke i {github_project}')

    if package_name_in is None:
        datapackage = input(f'Skriv inn ønsket navn på ny datapakke: ')
    else:
        datapackage = package_name_in

    if update_schedule_in is None:
        update_schedule = input(f'Skriv inn ønsket oppdateringsschedule for datapakken '
                                f'(format: \"<minutt> <time> <dag i måned> <måned> <ukedag>\", '
                                f'f.eks. \"0 12 * * 2,4\" vil gi <Hver tirsdag og torsdag kl 12.00 UTC>): ')
    else:
        update_schedule = update_schedule_in

    validate_cronjob_schedule(update_schedule)

    if nais_namespace_in is None:
        namespace = input(f'Skriv inn ønsket NAIS namespace: ')
    else:
        namespace = nais_namespace_in

    new_datapackage = CreateDataPackage(name=datapackage, github_project=github_project,
                                        update_schedule=update_schedule, namespace=namespace)

    new_datapackage.print_datapackage_config()

    res = input(f'Vil du opprette datapakken med konfigurasjonen over? [j/n] ')

    if res in {'j', 'ja', 'y', 'yes'}:
        new_datapackage.create()
    else:
        print(f'Datapakken ble ikke opprettet')
