## 1. Starte nytt prosjekt for å gjøre en analyse, bygge en ML modell, lage en presentasjon, produsere en datapakke e.l.

### Hva ønsker jeg å gjøre
Starte en notebook
Importere pakker og andre ressurser fra eksterne og interne repo
Hente data fra ulike kilder i eller utenfor NAV på en enkel og sikker måte 
Kjøre notebook i et miljø med tilstrekkelige ressurser
Dokumentere resultatene
Large notebook samt eventuelle produserte artefakter
(Eventuelt dele notebook med flere)
(Eventuelt trekke ut og lagre funksjoner e.a som kan gjennbrukes i andre notebooks)

### Mulige løsninger
Kjøremiljo
- Dask på kubernetes. Egne noder med GPU & mye minne


Tilgang til data. Håndtering av hemmeligheter
- SSO, Automatisk oppslag av teamtilhørighet i AD. 
- Vault pr team (AD)
- Beskyttet område S:/ med tilgangstyring i AD
- Systembruker i AD pr team?

Finne og dele notebooks
- Automatisk indeksering av notebooks ved lagring?
- Søkemuligheter og notebook viewer i datakatalogen?


## 2. Produsere og publisere kjørbare artefakter fra en notebook

### Hva ønsker jeg å gjøre
Minst mulig. Bygging og publisering skal automagisk


### Mulige løsninger

Automagisk bygging som følge av push til github?
    - repo pr notebook. Støtte til å sette opp CI konfigurasjon støttet med Terraform skript?
    - repo pr team med branches for hver notebook. CI konf. er en engangsjobb pr team
    - monorepo pr team med repo pr notebook (trigger sjekker hvilke repo som inneholder oppdaterte filer og bygger disse). CI konf. er en engangsjobb pr team. (https://github.com/slimm609/monorepo-gitwatcher)


Automagisk bygging som følge av lagring av notebook?
    -   Notebook versjoneres og lagres på S3 (eksekverbar med papermill)
    -   Notebook versjoneres og konverteres til python script og lagres på S3


## 2. Skedulere produksjon av datapakker, ML modeller eller annet 

### Hva ønsker jeg å gjøre
Gjennomføre en testkjøring for å se på og teste resultaene
Sette opp skedulert kjøring av notebooken (eller alternativt et avledet kjørbart artefakt: docker image, python fil e.a.)
Bli varslet om det oppstår problemer med kjøringen

