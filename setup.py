# -*- coding: utf-8 -*-
from setuptools import setup
from pathlib import Path

# Gjør README.md om til den lange beskrivelsen på PiPy
with open("README.md", "r") as fh:
    long_description = fh.read()

with Path("dataverk/VERSION").open("r") as fh:
    __version__ = fh.read()

with open('requirements.txt') as f:
    install_requires = f.read().strip().split('\n')

setup(
    name='dataverk',
    version=__version__,
    packages=['dataverk', 'dataverk.connectors', 'dataverk.utils', 'dataverk_cli', 'dataverk.context',
              'dataverk_cli.deploy', 'dataverk_cli.cli.cli_command_handlers', 'dataverk_cli.cli.cli_utils', 'dataverk_cli.templates'],
    python_requires='>=3.6',
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'dataverk-cli = dataverk_cli.dataverk_cli_entrypoint:main'
        ]
    },
    package_data={'dataverk': ['VERSION'],
                  'dataverk_cli': ['templates/*']},

    # metadata to display on PyPI
    author="NAV IKT",
    author_email="paul.bencze@nav.no",
    description="Serverless ETL framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    keywords="datapackage serverless etl jupyter open-data",
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
