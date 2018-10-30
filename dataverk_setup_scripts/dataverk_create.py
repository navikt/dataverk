import jenkins
import getpass
import os
import json
import yaml

from string import Template
from shutil import copyfile, rmtree
from xml.etree import ElementTree


class CreateDataPackage:
    ''' Klasse for å opprette ny datapakke i et eksisterende repository for datapakker/datasett
    '''

    def __init__(self, name: str, github_project: str):
        user = input("Brukerident: ")
        password = getpass.getpass("Passord: ")

        self.package_name = name
        self.github_project = github_project
        self.namespace = 'opendata' # TODO: Konfigurerbart?
        self.jenkins_server = jenkins.Jenkins('http://a34apvl00117.devillo.no:8080', username=user, password=password)

        # TODO: kanskje en sjekk på om jenkins forbindelsen er oppe (merkes ikke om brukernavn/passord er feil før senere sånn som det er nå)

        if not self._is_in_repo_root():
            raise Exception("dataverk_create må kjøres fra topp-nivået i git repoet")

        if self._is_package_name_in_use():
            raise NameError("Datapakkenavn må være unikt.")

        self.cronjob_schedule = self.set_cronjob_schedule()

    def _is_in_repo_root(self):
        ''' Sjekk på om create_dataverk kjøres fra toppnivå i repo.

        :return: boolean: "True" hvis dataverk_create kjøres fra toppnivå i repo, "False" ellers
        '''

        current_dir = os.getcwd()
        git_root = os.popen('git rev-parse --show-toplevel').read().strip()

        return os.path.samefile(current_dir, git_root)

    def _is_package_name_in_use(self):
        ''' Sjekk på om pakkenavnet er i bruk enten i repo eller på jenkinsserver

        :return: boolean: "True" hvis pakkenavn allerede er tatt i bruk, "False" ellers
        '''

        for filename in os.listdir(os.getcwd()):
            if self.package_name == filename:
                print("En mappe med navn " + self.package_name + " eksisterer allerede i repo " + self.github_project)
                return True

        if self.jenkins_server.job_exists(self.package_name):
            print("En jobb med navn " + self.package_name + " eksisterer allerede på jenkins serveren")
            return True

        return False

    def set_cronjob_schedule(self):
        ''' Kontrollerer bruker input og setter cronjob schedule

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


    def get_datapackage_config(self):
        print("\n-------------Ny datapakke------------------------" +
              "\nDatapakkenavn: " + self.package_name +
              "\ngithub repo: " + self.github_project +
              "\ncronjob schedule: " + self.cronjob_schedule +
              "\nNAIS namespace: " + self.namespace +
              "\n-------------------------------------------------\n")

    def create_folder_structure(self):
        ''' Lager mappestrukturen for datapakken. Oppretter mappene:
            {repo_root}/{pakkenavn}/scripts
            {repo_root}/{pakkenavn}/data
        '''

        os.mkdir(self.package_name)
        os.mkdir(os.path.join(self.package_name, 'scripts'))
        os.mkdir(os.path.join(self.package_name, 'data'))

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

        copyfile(os.path.join('file_templates', 'jenkins_config.xml'), os.path.join(self.package_name, 'jenkins_config.xml'))
        copyfile(os.path.join('file_templates', 'Jenkinsfile'), os.path.join(self.package_name, 'Jenkinsfile'))
        copyfile(os.path.join('file_templates', 'cronjob.yaml'), os.path.join(self.package_name, 'cronjob.yaml'))
        copyfile(os.path.join('file_templates', 'Dockerfile'), os.path.join(self.package_name, 'Dockerfile'))
        copyfile(os.path.join('file_templates', 'METADATA.json'), os.path.join(self.package_name, 'METADATA.json'))
        copyfile(os.path.join('file_templates', 'LICENSE.md'), os.path.join(self.package_name, 'LICENSE.md'))
        copyfile(os.path.join('file_templates', 'README.md'), os.path.join(self.package_name, 'README.md'))
        copyfile(os.path.join('file_templates', 'METADATA.json'), os.path.join(self.package_name, 'METADATA.json'))
        copyfile(os.path.join('file_templates', 'etl.ipynb'), os.path.join(self.package_name, 'scripts', 'etl.ipynb'))

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
        #cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['initContainers'][0]['env'][2]['value'] = '/kv/prod/fss/' + self.package_name + '/' + self.namespace
        #cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['initContainers'][0]['env'][3]['value'] = self.package_name
        #cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['serviceAccount'] = self.package_name
        #cronjob_config['spec']['jobTemplate']['spec']['template']['spec']['serviceAccountName'] = self.package_name

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

        self.create_folder_structure()
        self.copy_template_files()
        self.edit_package_metadata()
        self.edit_cronjob_config()
        self.edit_jenkins_file()

        self.create_jenkins_job()

        print("Datapakken " + self.package_name + " er opprettet")

        # TODO: Add more error handling


def get_github_url():
    return os.popen('git config --get remote.origin.url').read().strip()


def _is_in_git_repo():
    return os.popen('git rev-parse --is-inside-work-tree').read().strip()


def main():
    if not _is_in_git_repo():
        raise Exception("dataverk_create må kjøres fra et git repository")

    github_project = get_github_url()

    print("Opprettelse av ny datapakke i " + github_project)
    datapackage = input('Skriv inn ønsket navn på ny datapakke: ')

    new_datapackage = CreateDataPackage(datapackage, github_project)

    new_datapackage.get_datapackage_config()

    res = input("Vil du opprette datapakken med konfigurasjonen over? [j/n] ")

    if res == 'j':
        new_datapackage.create()
    else:
        print("Datapakken ble ikke opprettet")


if __name__ == "__main__":
    main()
