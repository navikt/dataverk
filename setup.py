# -*- coding: utf-8 -*-
from setuptools import setup
from pathlib import Path

# Gjør README.md om til den lange beskrivelsen på PiPy
with open("README.md", "r") as fh:
    long_description = fh.read()

with Path("nav_dataverk/VERSION").open("r") as fh:
    __version__ = fh.read()

with open('requirements.txt') as f:
    install_requires = f.read().strip().split('\n')

setup(
    name='nav-dataverk',
    version=__version__,
    packages=['nav_dataverk', 'nav_dataverk.connectors', 'nav_dataverk.utils', 'nav_dataverk.context'],
    python_requires='>=3.6',
    install_requires=install_requires,
    package_data={'nav_dataverk': ['VERSION']},
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
