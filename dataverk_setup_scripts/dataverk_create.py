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

        self.jenkins_server = jenkins.Jenkins('http://a34apvl00117.devillo.no:8080', username=user, password=password)

        if not self._is_in_repo_root():
            raise Exception("dataverk_create må kjøres fra topp-nivået i git repoet")

        if self._is_package_name_in_use(name):
            raise NameError("Datapakkenavn må være unikt.")

        self.package_name = name
        self.github_project = github_project
        self.namespace = namespace

        self._create_folder_structure()

        settings = settings_loader.GitSettingsLoader(url=settings_repo)

        self.settings_folder_name = settings.download_to(self.package_name)

        if update_schedule is None:
            self.cronjob_schedule = self._set_cronjob_schedule()
        else:
            self.cronjob_schedule = update_schedule

        # TODO: verify cronjob_schedule string

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

    def _set_cronjob_schedule(self):
        ''' Kontrollerer brukerinput og setter cronjob schedule

        :return: String: cronjob-schedule for .yaml fil
        '''

        print("------------------------------------------------Oppdateringsschedule------------------------------------------")
        print("Ukedager - man-søn (0-6)")
        print("MERK: For flere dager, adskill med komma. F.eks. ønsker du at datapakken skal oppdateres mandag,tirsdag og fredag blir dette: \"0,1,4\"")
        days = input("Skriv inn hvilke(n) ukedag(er) datapakken skal oppdateres: ")

        day_list = days.split(',')

        for day in day_list:
            if int(day) not in range(0, 7):
                raise Exception("'" + day + "' er ikke en gyldig dag. Gyldige dager er 0-6 (man-søn)")

        print("\nTime på dagen(e) - 0-23")
        print("MERK: Time oppgis i UTC")
        hour = int(input("Skriv inn hvilken time på dagen(e) du vil at datapakken skal oppdateres (UTC tid): "))

        if hour not in range(0, 23):
            raise Exception("'" + str(hour) + "' er ikke en gyldig time. Gyldig time er 0-23")

        print("\nMinutt innenfor time - 0-59")
        minute = int(input("Skriv inn hvilket minutt innenfor time datapakken skal oppdateres: "))

        if minute not in range(0, 59):
            raise Exception("'" + str(minute) + "' er ikke gyldig minutt innnenfor time. Gyldig minutt er 0-59")

        print("--------------------------------------------------------------------------------------------------------------")

        return str(minute) + " " + str(hour) + " " + "* " + "* " + days


    def print_datapackage_config(self):
        print("\n-------------Ny datapakke------------------------" +
              "\nDatapakkenavn: " + self.package_name +
              "\ngithub repo: " + self.github_project +
              "\ncronjob schedule: " + self.cronjob_schedule +
              "\nNAIS namespace: " + self.namespace +
              "\n-------------------------------------------------\n")

    def _create_folder_structure(self):
        ''' Lager mappestrukturen for datapakken. Oppretter mappene:
            {repo_root}/{pakkenavn}/scripts
            {repo_root}/{pakkenavn}/data
        '''

        os.mkdir(self.package_name)
        os.mkdir(os.path.join(self.package_name, 'scripts'))
        os.mkdir(os.path.join(self.package_name, 'data'))

    def copy_setup_file(self):
        '''
        '''

        copyfile(os.path.join(str(self.settings_folder_name), 'settings.py'),
                 os.path.join(self.package_name, 'settings.py'))

    def copy_template_files(self):
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

    def edit_package_metadata(self):
        '''  Tilpasser metadata fil til datapakken

        '''

        with open(os.path.join(self.package_name, 'METADATA.json'), 'r') as metadatafile:
            package_metadata = json.load(metadatafile)

        package_metadata['Datapakke_navn'] = self.package_name
        package_metadata['Bucket_navn'] = 'nav-opendata'

        with open(os.path.join(self.package_name, 'METADATA.json'), 'w') as metadatafile:
            json.dump(package_metadata, metadatafile)

    def edit_cronjob_config(self):
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

    def edit_jenkins_file(self):
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

    def create_jenkins_job(self):
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

    def create(self):
        ''' Oppretter ny datapakke med ønsket konfigurasjon

        '''

        self.copy_setup_file()
        self.copy_template_files()
        self.edit_package_metadata()
        self.edit_cronjob_config()
        self.edit_jenkins_file()

        self.create_jenkins_job()

        print("Datapakken " + self.package_name + " er opprettet")

        # TODO: Add more error handling


def get_github_url():
    if not os.popen('git rev-parse --is-inside-work-tree').read().strip():
        raise Exception("dataverk_create må kjøres fra et git repository")

    return os.popen('git config --get remote.origin.url').read().strip()


def run(package_name_in: str=None, update_schedule_in: str=None, nais_namespace_in: str=None, settings_repo_in: str=None):
    github_project = get_github_url()

    print("Opprettelse av ny datapakke i " + github_project)

    if package_name_in is None:
        datapackage = input("Skriv inn ønsket navn på ny datapakke: ")
    else:
        datapackage = package_name_in

    if settings_repo_in is None:
        settings_repo_url = input("Lim inn url til settings repo: ")
    else:
        settings_repo_url = settings_repo_in

    if nais_namespace_in is None:
        namespace = input("Skriv inn ønsket NAIS namespace: ")
    else:
        namespace = nais_namespace_in

    user = input("Brukerident: ")
    password = getpass.getpass("Passord: ")

    new_datapackage = CreateDataPackage(name=datapackage, github_project=github_project,
                                        update_schedule=update_schedule_in, settings_repo=settings_repo_url,
                                        namespace=namespace, user=user, password=password)

    new_datapackage.print_datapackage_config()

    res = input("Vil du opprette datapakken med konfigurasjonen over? [j/n] ")

    if res == 'j':
        new_datapackage.create()
    else:
        print("Datapakken ble ikke opprettet")
