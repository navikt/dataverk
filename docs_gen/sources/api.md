### publish_datapackage


```python
dataverk.api.publish_datapackage(datasets, destination='nais')
```

----

### write_datapackage


```python
dataverk.api.write_datapackage(datasets)
```

----

### is_sql_file


```python
dataverk.api.is_sql_file(source)
```

----

### read_sql


```python
dataverk.api.read_sql(source, sql, connector='Oracle')
```



Read pandas dataframe from SQL database 

----

### to_sql


```python
dataverk.api.to_sql(df, table, sink=None, schema=None, connector='Oracle')
```


Write records in dataframe to a SQL database table
----

### notebook2script


```python
dataverk.utils.notebook2script(fname=None)
```

----

### write_notebook


```python
dataverk.api.write_notebook()
```
