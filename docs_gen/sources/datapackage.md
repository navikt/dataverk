<span style="float:right;">[[source]](https://github.com/navikt/dataverk/blob/master/dataverk/datapackage.py#L13)</span>
## Datapackage class

```python
dataverk.datapackage.Datapackage(resource_files=None, search_start_path='.')
```


---
## Datapackage methods

### add_resource


```python
add_resource(df, dataset_name, dataset_description='')
```

---
### add_view


```python
add_view(name, resource, columns, view_type='Simple', title='', description='')
```

---
### read_sql


```python
read_sql(source, sql, dataset_name=None, connector='Oracle', dataset_description='')
```



Read pandas dataframe from SQL database

---
### to_sql


```python
to_sql(df, table, schema, sink, connector='Oracle')
```


Write records in dataframe to a SQL database table
---
### update_metadata


```python
update_metadata(key, value)
```

---
### write_datapackage


```python
write_datapackage()
```
