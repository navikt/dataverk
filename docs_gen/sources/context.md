<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/context/env_store.py#L6)</span>
## EnvStore class

```python
dataverk.context.env_store.EnvStore(path, env_setter=None)
```


Mapping object for storing and easy accessing of Environment variables


---
## EnvStore methods

### get


```python
get(key)
```

---
### items


```python
items()
```

---
### keys


```python
keys()
```

---
### values


```python
values()
```

----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/context/settings_classes.py#L66)</span>
## SettingsStore class

```python
dataverk.context.settings_classes.SettingsStore(settings_dict)
```

Klassen har ansvar for å gjøre settings som eksterne URLer, keys, flagg og andre ressurser tilgjengelige



---
## SettingsStore methods

### get


```python
get(key)
```

---
### items


```python
items()
```

---
### keys


```python
keys()
```

---
### values


```python
values()
```

----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/context/settings_classes.py#L9)</span>
## SettingsBuilder class

```python
dataverk.context.settings_classes.SettingsBuilder(settings, env_store=None)
```

Bygger SettingsStore objektet fra json fil og tilgjenngeliggjør modifikasjon gjennom apply() metoden.



---
## SettingsBuilder methods

### apply


```python
apply(modifier)
```


public metode som gir eksterne funskjoner tilgang til å endre, berike og/eller fjerne felter i settings_store

---
### build


```python
build()
```

----

### singleton_settings_store_factory


```python
dataverk.context.settings.singleton_settings_store_factory(settings_file_path=None, env_store=None)
```


Lager et nytt SettingsStore objekt om et ikke allerede har blitt laget. Hvis et SettingsStore objekt har blitt
laget returnerer den de istedet.

<br>:param settings_file_path: Path til settings.json filen
<br>:param env_store: EnvStore objekt
<br>:return: Ferdig konfigurert SettingsStore Objekt

----

### settings_store_factory


```python
dataverk.context.settings.settings_store_factory(settings_file_path, env_store)
```


Lager et nytt SettingsStore objekt fra en settings.json fil og modifiserer den basert på env variabler.

<br>:param settings_file_path: Path til settings.json filen
<br>:param env_store: EnvStore objekt
<br>:return: Ferdig konfigurert SettingsStore Objekt
