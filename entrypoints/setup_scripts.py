import jenkins
import getpass
import os
import json

from xml.etree import ElementTree


def dataverk_create():
    # Provide user credentials
    user = input("Enter user ident: ")
    password = getpass.getpass("Enter password: ")

    # Ensure datapackage is unique
    datapackage = input('Enter package name: ')

    for filename in os.listdir(os.getcwd()):
        if filename == datapackage:
            print("Datapackage exists already")
            exit(1)

    # Set metadata
    metadata = {}
    metadata['datapackage'] = datapackage
    metadata['github_project'] = os.popen('git config --get remote.origin.url').read().strip()
    metadata['jenkins_url'] = 'http://a34apvl00117.devillo.no:8080'

    # Create folder structure
    os.system('mkdir -p ' + os.path.join(metadata['datapackage'], 'scripts'))
    os.system('mkdir -p ' + os.path.join(metadata['datapackage'], 'data'))

    # Write metadata to file
    with open(os.path.join(metadata['datapackage'], 'metadata.json'), 'w+') as metadata_file:
        json.dump(metadata, metadata_file)

    # Copy template files
    os.system('cp ' + os.path.join('file_templates', 'jenkins_config.xml') + ' ' + metadata['datapackage'])
    os.system('cp ' + os.path.join('file_templates', 'Jenkinsfile') + ' ' + metadata['datapackage'])
    os.system('cp ' + os.path.join('file_templates', 'nais.yaml') + ' ' + metadata['datapackage'])
    os.system('cp ' + os.path.join('file_templates', 'Dockerfile') + ' ' + metadata['datapackage'])
    os.system('cp ' + os.path.join('file_templates', 'DCAT.json') + ' ' + metadata['datapackage'])
    os.system('cp ' + os.path.join('file_templates', 'etl.ipynb') + ' ' + os.path.join(metadata['datapackage'], 'scripts'))

    # Edit DCAT metadata file
    with open(os.path.join(metadata['datapackage'], 'DCAT.json'), 'r') as f:
        package_metadata = json.load(f)
        package_metadata['Datapakke_navn'] = metadata['datapackage']
        package_metadata['Bucket_navn'] = 'nav-opendata'

    with open(os.path.join(metadata['datapackage'], 'DCAT.json'), 'w') as f:
        json.dump(package_metadata, f)

    # Update jenkins config file and setup Jenkins job
    xml = ElementTree.parse(os.path.join(metadata['datapackage'], 'jenkins_config.xml'))
    xml_root = xml.getroot()

    for elem in xml_root.getiterator():
        if elem.tag == 'scriptPath':
            elem.text = metadata['datapackage'] + '/Jenkinsfile'
        elif elem.tag == 'projectUrl':
            elem.text = metadata['github_project']
        elif elem.tag == 'url':
            elem.text = metadata['github_project']

    xml.write(os.path.join(metadata['datapackage'], 'jenkins_config.xml'))

    xml_config = ElementTree.tostring(xml_root, encoding='utf-8', method='xml').decode()

    jenkins_server = jenkins.Jenkins(metadata['jenkins_url'], username=user, password=password)

    try:
        jenkins_server.create_job(name=metadata['datapackage'], config_xml=xml_config)
    except jenkins.JenkinsException:
        print("Unable to setup jenkins job")
        os.system('rm -rvf ' + metadata['datapackage'])
