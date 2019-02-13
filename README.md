
[![Build Status](https://travis-ci.com/navikt/dataverk.svg?branch=master)](https://travis-ci.com/navikt/dataverk)
[![Maintainability](https://api.codeclimate.com/v1/badges/517723886f838e83ceaa/maintainability)](https://codeclimate.com/github/navikt/dataverk/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/517723886f838e83ceaa/test_coverage)](https://codeclimate.com/github/navikt/dataverk/test_coverage)

# Dataverk 

### Get started

#### Fra scratch - nytt dataverk prosjekt
 1. Opprett repository på github
 2. Klon github repository lokalt på din maskin
 3. ```pip3 install dataverk```
 4. ```dataverk create_settings```
 5. fyll ut den genererte settings.json filen med data
 
#### Lage en ny datapakke i eksisterende repository
 1. Hvis du ikke har en .env fil kjør; ```dataverk create_env_file```
 2. ```dataverk create```
 3. ```jupyter notebook```
 4. åpne datapakke-navn/scripts/etl.ipynb
 5. Implementer data prosesseringen
 6. push prosjekt endringene til github (```git push orgin master```)




## Metoder for tilgang til datasett. 

### Forbindelser (source & sink med kryptering)
* Fil 
* JsonStat
* Oracle
* Google Cloud Storage
* ...

### Formater
* Pandas
* Arrow
* CSV
* Excel
* JsonStat
* Vega & Vega Lite
* Semiotic
* ...


### Dashboards

## Relaterte  prosjekter

url | beskrivelse
----| -----------
[go-jek feast](https://github.com/gojek/feast/) | Feast - Feature Store for Machine Learning
[ContinuumIO/intake](https://github.com/ContinuumIO/intake/) | Intake: A general interface for loading data
[frictionlessdata.io](https://frictionlessdata.io/) | Frictionless data
[git lfs](https://git-lfs.github.com/) | Github Large File Storage
[dvc.org](https://dvc.org) | Data Science Version Control 
[quilt (github)](https://github.com/quiltdata) | Quilt - Version and deploy data
[Python package in S3](https://github.com/novemberfiveco/s3pypi) | CLI tool for creating a Python Package Repo i S3
[pypiserver based on bottle](https://github.com/pypiserver/pypiserver) | Minimal PyPI server
