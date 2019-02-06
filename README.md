[![CircleCI](https://circleci.com/gh/navikt/dataverk.svg?style=svg&circle-token=3e5fd8de41d8dd24ce2546d0e5800ce06926add0)](https://circleci.com/gh/navikt/dataverk)

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
[frictionlessdata.io](https://frictionlessdata.io/) | Frictionless data
[git lfs](https://git-lfs.github.com/) | Github Large File Storage
[dvc.org](https://dvc.org) | Data Science Version Control 
[quilt (github)](https://github.com/quiltdata) | Quilt - Version and deploy data
[Python package in S3](https://github.com/novemberfiveco/s3pypi) | CLI tool for creating a Python Package Repo i S3
[pypiserver based on bottle](https://github.com/pypiserver/pypiserver) | Minimal PyPI server
