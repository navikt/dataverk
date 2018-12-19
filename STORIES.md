## 1 - Starte nytt prosjekt
### Hva ØNSKER JEG Å GJØRE?  
Opprette mappe med undermapper og nødvendige filer: datapackage.json, LISENCE.md, README.md METADATA.md  
Legge inn navn på prosjektet/datapakken i mappenavn + i filer (?)  
Hente env-fil fra repo dataverk_settings   
Kobling til Vault / credentials - (?)  
Skrive Jenkins eller Travis script ?
    
### HVORDAN FORVENTER JEG Å KUNNE GJØRE DET?
```console
$ dataverk-cli init-datapackage
``` 
Alternativt: klone eksisterende repo, endre navn på mappe, endre navn i filer (?)  
  
## 2 - Hente data fra kilder  
### Hva ØNSKER JEG Å GJØRE?  
Opprette settings-objekt med connection strings og secrets for tilkobling til kilde  
Ett objekt per connection, eller et masterobjekt med all config?  
Connections til både sources og sinks i samme objekt?  
Opprette dataframes fra kilde  
  
### HVORDAN FORVENTER JEG Å KUNNE GJØRE DET?
```python
import dataverk as dv  
con = dv.connect('dvh') # gir større fleksiblitet, mer inituitiv modell og mulighet for feilmeldinger eller annen tilbakemelding til bruker ved etablering av connection  
df = con.execute('select * from table')
#eller?
df = dv.read_sql('select * from table', con=con)
#eller med automagisk etablering av connection?
df = dv.read_sql('select * from table', source='dvh') # enklere kode?
```
  
## 3 - Bearbeide data  
### Hva ØNSKER JEG Å GJØRE?  
Transformere dataframes til ønsket format og modell  
Utvikle views på dataframes  
  
### HVORDAN FORVENTER JEG Å KUNNE GJØRE DET?  
Pandas  
Dask  
+++  
  
## 4 - Pakketere data i frictionless-format  
### Hva ØNSKER JEG Å GJØRE?  
Definere hvilke ressurser som skal inngå i en datapakke  
Definere nødvendig metadata for å beskrive ressursene og pakken  
Persistere dataframes til .csv (eller annet format)  
Legge inn link til filer som ressurser  
Definere views  
  
### HVORDAN FORVENTER JEG Å KUNNE GJØRE DET?  
Manuell redigering av .md og .json-filer  
Datapackage-klassen i dataverk ?  
Fork av datapackage-py ?  
- med støtte for df -> resource: 
```python
add_resource(df,[description=markdown])
```
- med støtte for views
```python
add_view(title=... type=... resource=... columns=... [spec=...]  [description=markdown]) 
```

## 5 - Publisere datapakken  
### Hva ØNSKER JEG Å GJØRE?  
Kopiere datafilene til S3  
Legge inn metadatadokument i en elastic index  
Begge operasjonene bør være underlagt versjonskontroll ved endringer  
### HVORDAN FORVENTER JEG Å KUNNE GJØRE DET? 
I terminalen?  
```console
$ dataverk-cli publish(package=... storage=... index=...)
```
fork av data-cli?  
  
## 6 - Schedulere datapipeline  
### Hva ØNSKER JEG Å GJØRE?  
Sette notebook'en i "produksjon" - sette den til å kjøre på NAIS (eller Travis?) ved faste intervaller  
### HVORDAN FORVENTER JEG Å KUNNE GJØRE DET?  
Eksplisitt kommando i dataverk-scriptet?  
Som en del av publiseringen ?
```console
$ dataverk-cli publish(package=... storage=... index=..., schedule=...)
```

## 7 - Manage pipelines
### HVA ØNSKER JEG Å GJØRE?
Se listen over pipelines og tilhørende schedules
Se status på kjøringer
Se statistikk på kjøringer (datavolum flyttet, kjøretid, etc)
Endre schedules
Dersom noen kjøringer tar veldig lang tid, justere på tildelte ressurser fra k8
Slette pipelines/schedules
### HVORDAN ØNSKER JEG Å GJØRE DET?
TBD
Airflow?
