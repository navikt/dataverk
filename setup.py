# -*- coding: utf-8 -*-
from setuptools import setup
from pathlib import Path

#Gjør README.md om til den lange beskrivelsen på PiPy
with open("README.md", "r") as fh:
    long_description = fh.read()

with Path("dataverk/VERSION").open("r") as fh:
    __version__ = fh.read()

setup(
    name='dataverk',
    version=__version__,
    packages=['dataverk', 'dataverk.connectors', 'dataverk.utils', 'dataverk_cli', 'dataverk.context',
              'dataverk_cli.scheduling', 'dataverk_cli.cli_utils'],
    python_requires='>=3.6',
    install_requires=[
        'cryptography==2.3',
        'requests==2.21.0',
        'prometheus_client==0.4.0',
        'SQLAlchemy==1.2.10',
        'pyjstat==1.0.1',
        'setuptools>=39.0.1',
        'pandas==0.23.3',
        'boto3==1.9.11',
        'numpy==1.15.2',
        'fire==0.1.3',
        'cx_Oracle==7.0.0',
        'protobuf==3.6.1',
        'pyarrow>=0.10.0',
        'python-jenkins==1.3.0',
        'pyyaml==4.2b1',
        'elasticsearch==6.3.0',
        'google-api-core==0.1.4',
        'google-auth==1.5.0',
        'google-cloud-core==0.28.1',
        'google-cloud-storage==1.10.0',
        'google-resumable-media==0.3.1',
        'googleapis-common-protos==1.5.3'
    ],
    entry_points={
        'console_scripts': [
            'dataverk-cli = dataverk_cli.dataverk:main'
        ]
    },
    package_data={'dataverk': ['VERSION']},

     # metadata to display on PyPI
    author="NAV IKT",
    author_email="paul.bencze@nav.no",
    description="NAV Dataverk",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    keywords="datapackage datasett etl open-data",
    url="https://github.com/navikt",
    project_urls={
        "Bug Tracker": "https://github.com/navikt",
        "Documentation": "https://github.com/navikt",
        "Source Code": "https://github.com/navikt",
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'

    ],

)
