import jenkins
import getpass
import os
import json
import yaml

from string import Template
from shutil import copyfile, rmtree
from xml.etree import ElementTree
from . import settings_loader


class CreateDataPackage:
    ''' Klasse for å opprette ny datapakke i et eksisterende repository for datapakker/datasett
    '''

    def __init__(self, name: str,
                 github_project: str, update_schedule: str,
                 namespace: str, settings_repo: str,
                 user: str, password: str):

        self._verify_constructor_arguments(name, github_project, update_schedule, namespace, settings_repo, user, password)

        self.jenkins_server = jenkins.Jenkins('http://a34apvl00117.devillo.no:8080', username=user, password=password)

        # TODO: Hent jenkins url fra settings

        if not self._is_in_repo_root():
            raise Exception("dataverk create må kjøres fra topp-nivået i git repoet")

        if self._is_package_name_in_use(name):
            raise NameError("Datapakkenavn må være unikt.")

        self.package_name = name
        self.github_project = github_project
        self.namespace = namespace
        self.cronjob_schedule = update_schedule

        self._create_folder_structure()

        settings = settings_loader.GitSettingsLoader(url=settings_repo)

        self.settings_folder_name = settings.download_to(self.package_name)

    def _verify_constructor_arguments(self, name, github_project, update_schedule, namespace, settings_repo, user, password):
        if not isinstance(name, str):
            raise TypeError("name parameter must be of type string")

        if not isinstance(github_project, str):
            raise TypeError("github_project parameter must be of type string")

        if not isinstance(update_schedule, str):
            raise TypeError("update_schedule parameter must be of type string")

        if not isinstance(namespace, str):
            raise TypeError("namespace parameter must be of type string")

        if not isinstance(settings_repo, str):
            raise TypeError("settings_repo parameter must be of type string")

        if not isinstance(user, str):
            raise TypeError("user parameter must be of type string")

        if not isinstance(password, str):
            raise TypeError("password parameter must be of type string")


    def _is_in_repo_root(self):
        ''' Sjekk på om create_dataverk kjøres fra toppnivå i repo.

        :return: boolean: "True" hvis dataverk_create kjøres fra toppnivå i repo, "False" ellers
        '''

        current_dir = os.getcwd()
        git_root = os.popen('git rev-parse --show-toplevel').read().strip()

        return os.path.samefile(current_dir, git_root)

    def _is_package_name_in_use(self, name: str):
        ''' Sjekk på om pakkenavnet er i bruk enten i repo eller på jenkinsserver

        :return: boolean: "True" hvis pakkenavn allerede er tatt i bruk, "False" ellers
        '''

        for filename in os.listdir(os.getcwd()):
            if name == filename:
                print("En mappe med navn " + name + " eksisterer allerede i repo " + self.github_project)
                return True

        if self.jenkins_server.job_exists(name):
            print("En jobb med navn " + name + " eksisterer allerede på jenkins serveren")
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

        if not os.path.exists(os.path.join(self.package_name, 'scripts')):
            raise Exception("Unable to create path '" + str(os.path.join(self.package_name, 'scripts')) + "'")

        if not os.path.exists(os.path.join(self.package_name, 'data')):
            raise Exception("Unable to create path '" + str(os.path.join(self.package_name, 'data')) + "'")

    def _copy_setup_file(self):
        '''
        '''

        copyfile(os.path.join(str(self.settings_folder_name), 'settings.json'),
                 os.path.join(self.package_name, 'scripts', 'settings.json'))

    def _copy_template_files(self):
        ''' Kopierer følgende template filer til datapakken:
            - jenkins_config.xml
            - Jenkinsfile
            - cronjob.xml
            - Dockerfile
            - METADATA.json
            - LICENSE.md
            - README.md
            - etl.ipynb
        '''

        copyfile(os.path.join(str(self.settings_folder_name), 'file_templates', 'jenkins_config.xml'), os.path.join(self.package_name, 'jenkins_config.xml'))
        copyfile(os.path.join(str(self.settings_folder_name), 'file_templates', 'Jenkinsfile'), os.path.join(self.package_name, 'Jenkinsfile'))
        copyfile(os.path.join(str(self.settings_folder_name), 'file_templates', 'cronjob.yaml'), os.path.join(self.package_name, 'cronjob.yaml'))
        copyfile(os.path.join(str(self.settings_folder_name), 'file_templates', 'Dockerfile'), os.path.join(self.package_name, 'Dockerfile'))
        copyfile(os.path.join(str(self.settings_folder_name), 'file_templates', 'LICENSE.md'), os.path.join(self.package_name, 'LICENSE.md'))
        copyfile(os.path.join(str(self.settings_folder_name), 'file_templates', 'README.md'), os.path.join(self.package_name, 'README.md'))
        copyfile(os.path.join(str(self.settings_folder_name), 'file_templates', 'METADATA.json'), os.path.join(self.package_name, 'METADATA.json'))
        copyfile(os.path.join(str(self.settings_folder_name), 'file_templates', 'etl.ipynb'), os.path.join(self.package_name, 'scripts', 'etl.ipynb'))

    def _edit_package_metadata(self):
        '''  Tilpasser metadata fil til datapakken

        '''

        with open(os.path.join(self.package_name, 'METADATA.json'), 'r') as metadatafile:
            package_metadata = json.load(metadatafile)

        package_metadata['Datapakke_navn'] = self.package_name
        package_metadata['Bucket_navn'] = 'nav-opendata'

        with open(os.path.join(self.package_name, 'METADATA.json'), 'w') as metadatafile:
            json.dump(package_metadata, metadatafile)

    def _edit_cronjob_config(self):
        ''' Tilpasser cronjob config fil til datapakken

        '''

        with open(os.path.join(self.package_name, 'cronjob.yaml'), 'r') as yamlfile:
            cronjob_config = yaml.load(yamlfile)

        cronjob_config['metadata']['name'] = self.package_name
        cronjob_config['metadata']['namespace'] = self.namespace

        cronjob_config['spec']['schedule'] = self.cronjob_schedule
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['name'] = self.package_name + '-cronjob'
        cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]['image'] = 'repo.adeo.no:5443/' + self.package_name

        with open(os.path.join(self.package_name, 'cronjob.yaml'), 'w') as yamlfile:
            yamlfile.write(yaml.dump(cronjob_config, default_flow_style=False))

    def _edit_jenkins_file(self):
        ''' Tilpasser Jenkinsfile til datapakken

        '''

        with open(os.path.join(self.package_name, 'Jenkinsfile'), 'r') as jenkinsfile:
            jenkins_config = jenkinsfile.read()

        template = Template(jenkins_config)
        jenkins_config = template.safe_substitute(package_name=self.package_name,
                                                  package_repo=self.github_project,
                                                  package_path=self.package_name)

        with open(os.path.join(self.package_name, 'Jenkinsfile'), 'w') as jenkinsfile:
            jenkinsfile.write(jenkins_config)

    def _create_jenkins_job(self):
        ''' Tilpasser jenkins konfigurasjonsfil og setter opp ny jenkins jobb for datapakken

        '''

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

        try:
            self.jenkins_server.create_job(name=self.package_name, config_xml=xml_config)
        except jenkins.JenkinsException:
            rmtree(self.package_name)
            raise Exception("Klarte ikke sette opp jenkins jobb.")

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

        self._copy_setup_file()
        self._copy_template_files()
        self._edit_package_metadata()
        self._edit_cronjob_config()
        self._edit_jenkins_file()

        self._create_jenkins_job()

        print("Datapakken " + self.package_name + " er opprettet")

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
            "Schedule '" + schedule + "' har ikke riktig format. Må ha formatet: \"<minutt> <time> <dag i måned> <måned> <ukedag>\". "
                                      "F.eks. \"0 12 * * 2,4\" vil gi: Hver tirsdag og torsdag kl 12.00 UTC")

    if not schedule_list[0] is '*':
        for minute in schedule_list[0].split(','):
            if not int(minute) in range(0, 60):
                raise ValueError("I schedule " + schedule + " er ikke '" + minute +
                                 "' en gyldig verdi for minutt innenfor time. Gyldige verdier er 0-59', eller '*'")

    if not schedule_list[1] is '*':
        for hour in schedule_list[1].split(','):
            if not int(hour) in range(0, 24):
                raise ValueError("I schedule " + schedule + " er ikke '" + hour +
                                 "' en gyldig verdi for time. Gyldige verdier er 0-23, eller '*'")

    if not schedule_list[2] is '*':
        for day in schedule_list[2].split(','):
            if not int(day) in range(1, 32):
                raise ValueError("I schedule " + schedule + " er ikke '" + day +
                                 "' en gyldig verdi for dag i måned. Gyldige verdier er 1-31, eller '*'")

    if not schedule_list[3] is '*':
        for month in schedule_list[3].split(','):
            if not int(month) in range(1, 13):
                raise ValueError("I schedule " + schedule + " er ikke '" + month +
                                 "' en gyldig verdi for måned. Gyldige verdier er 1-12, eller '*'")

    if not schedule_list[4] is '*':
        for weekday in schedule_list[4].split(','):
            if not int(weekday) in range(0, 7):
                raise ValueError("I schedule " + schedule + " er ikke '" + weekday +
                                 "' en gyldig verdi for ukedag. Gyldige verdier er 0-6 (søn-lør), eller '*'")

def get_github_url():
    if not os.popen('git rev-parse --is-inside-work-tree').read().strip():
        raise Exception("dataverk_create må kjøres fra et git repository")

    return os.popen('git config --get remote.origin.url').read().strip()


def run(package_name_in: str=None, update_schedule_in: str=None, nais_namespace_in: str=None, settings_repo_in: str=None):
    ''' Entrypoint for dataverk create

    '''

    github_project = get_github_url()

    print("Opprettelse av ny datapakke i " + github_project)

    if package_name_in is None:
        datapackage = input("Skriv inn ønsket navn på ny datapakke: ")
    else:
        datapackage = package_name_in

    if update_schedule_in is None:
        update_schedule = input("Skriv inn ønsket oppdateringsschedule for datapakken "
                                "(format: \"<minutt> <time> <dag i måned> <måned> <ukedag>\", "
                                "f.eks. \"0 12 * * 2,4\" vil gi <Hver tirsdag og torsdag kl 12.00 UTC>): ")
    else:
        update_schedule = update_schedule_in

    validate_cronjob_schedule(update_schedule)

    if nais_namespace_in is None:
        namespace = input("Skriv inn ønsket NAIS namespace: ")
    else:
        namespace = nais_namespace_in

    if settings_repo_in is None:
        settings_repo_url = input("Lim inn url til settings repo: ")
    else:
        settings_repo_url = settings_repo_in

    user = input("Brukerident: ")
    password = getpass.getpass("Passord: ")

    new_datapackage = CreateDataPackage(name=datapackage, github_project=github_project,
                                        update_schedule=update_schedule, settings_repo=settings_repo_url,
                                        namespace=namespace, user=user, password=password)

    new_datapackage.print_datapackage_config()

    res = input("Vil du opprette datapakken med konfigurasjonen over? [j/n] ")

    if res in {'j', 'ja', 'y', 'yes'}:
        new_datapackage.create()
    else:
        print("Datapakken ble ikke opprettet")
