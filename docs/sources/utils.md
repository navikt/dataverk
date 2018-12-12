<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/utils/settings.py#L13)</span>
### create_settings_store

```python
dataverk.utils.create_settings_store()
```

Lager et nytt SettingsStore objekt fra en settings.json fil og modifiserer den basert på env variabler.

:param settings_file_path: Path til settings.json filen
:param env_store: EnvStore objekt
:return: Ferdig konfigurert SettingsStore Objekt

----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/utils/auth_mixin.py#L9)</span>
### AuthMixin

```python
dataverk.utils.AuthMixin()
```

Authenticator
----

<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/utils/logger_mixin.py#L3)</span>
### LoggerMixin

```python
dataverk.utils.LoggerMixin()
```

Logger with timestamps


----

### get_fylke_from_region


```python
dataverk.utils.get_fylke_from_region(region)
```


Get current name for fylke

Sør-, og Nord-Trøndelag er slått sammen til Trøndelag 

