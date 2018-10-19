from setuptools import setup


#Gjør README.md om til den lange beskrivelsen på PiPy
with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='dataverk',
    version='0.0.1',
    packages=['dataverk', 'dataverk.connectors', 'dataverk.utils'],
    install_requires=[
        'cryptography==2.2.2',
        'requests==2.19.1',
        'prometheus_client==0.4.0',
        'SQLAlchemy==1.2.10',
        'pyjstat==1.0.1',
        'setuptools>=39.0.1',
        'pandas==0.23.3',
        'boto3==1.9.11',
        'numpy==1.15.2',
        'fire==0.1.3',
        'cx_Oracle==7.0.0',
        'Flask==1.0.2',
        'protobuf==3.6.1',
        'pyarrow>=0.10.0',
        'python-snappy==0.5.3',
        'elasticsearch==6.3.0',
        'google-api-core==0.1.4',
        'google-auth==1.5.0',
        'google-cloud-core==0.28.1',
        'google-cloud-storage==1.10.0',
        'google-resumable-media==0.3.1',
        'googleapis-common-protos==1.5.3'
    ],

     # metadata to display on PyPI
    author="NAV",
    author_email="paul.bencze@nav.no",
    description="NAV Åpne Datasett",
    long_description=long_description,
    license="MIT",
    keywords="åpne data datasett",
    url="hhttps://github.com/navikt",
    project_urls={
        "Bug Tracker": "https://github.com/navikt",
        "Documentation": "https://github.com/navikt",
        "Source Code": "https://github.com/navikt",
    }
)